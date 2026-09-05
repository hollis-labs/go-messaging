// Package mailbox — subscribe_test.go
//
// Tests for the in-process pubsub that backs streaming subscribers.
// The pubsub has no replay buffer: only messages published AFTER a
// subscription takes effect are delivered to that subscriber, and slow
// subscribers are dropped on a full channel (buffer = 16).
package mailbox

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestSubscribe_ReceivesNewMessage verifies that SubscribeSessionAgent
// returns a channel which receives a matching message published after the
// subscription is established.
func TestSubscribe_ReceivesNewMessage(t *testing.T) {
	svc, _ := newTestService(t, "file-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("SubscribeSessionAgent: %v", err)
	}

	msg := SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   testHumanID,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-a",
		Body:          "hello",
	}

	// Subscription admission is synchronous and the channel is buffered, so
	// there is no need for an unjoined sender goroutine.
	if _, err := svc.SendMessage(context.Background(), msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case received := <-ch:
		if received == nil {
			t.Fatal("received nil message on subscriber channel")
		}
		if received.Body != "hello" {
			t.Errorf("Body = %q, want %q", received.Body, "hello")
		}
		if received.ToAgentID != "file-a" {
			t.Errorf("ToAgentID = %q, want file-a", received.ToAgentID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for pushed message")
	}
}

// TestSubscribe_DeliveriesHaveIndependentOwnership proves the value returned
// to the sender and each live delivery occupy independent mutable storage.
// The concurrent writes are intentionally redundant with the pointer checks:
// under -race they also expose an alias if fan-out ever regresses.
func TestSubscribe_DeliveriesHaveIndependentOwnership(t *testing.T) {
	svc, _ := newTestService(t, "file-a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstCh, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe first: %v", err)
	}
	secondCh, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe second: %v", err)
	}

	sent, err := svc.SendMessage(context.Background(), baseInput(testHumanID, "file-a"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	first := receiveMessage(t, firstCh)
	second := receiveMessage(t, secondCh)

	if sent == first || sent == second || first == second {
		t.Errorf("messages alias: sender=%p first=%p second=%p", sent, first, second)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, item := range []struct {
		msg   *Message
		value string
	}{
		{sent, "sender"},
		{first, "first"},
		{second, "second"},
	} {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 5000; i++ {
				item.msg.Body = item.value
				item.msg.Metadata = item.value
			}
		}()
	}
	close(start)
	wg.Wait()
}

func receiveMessage(t *testing.T, ch <-chan *Message) *Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("subscriber channel closed before delivery")
		}
		if msg == nil {
			t.Fatal("received nil message")
		}
		return msg
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pushed message")
		return nil
	}
}

func TestPubsub_PublishDeepCopiesReferenceFieldsPerSubscriber(t *testing.T) {
	p := newPubsub()
	firstCh, err := p.subscribe(context.Background(), "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe first: %v", err)
	}
	secondCh, err := p.subscribe(context.Background(), "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe second: %v", err)
	}

	const wantReadAt = "2026-09-04T12:00:00Z"
	const wantResolvedAt = "2026-09-04T12:01:00Z"
	readAt := wantReadAt
	resolvedAt := wantResolvedAt
	source := &Message{
		ToSessionID: "sess-1",
		ToAgentID:   "file-a",
		Body:        "original",
		ReadAt:      &readAt,
		ResolvedAt:  &resolvedAt,
	}
	p.publish(source)
	first := receiveMessage(t, firstCh)
	second := receiveMessage(t, secondCh)

	if source == first || source == second || first == second {
		t.Errorf("message pointers alias: source=%p first=%p second=%p", source, first, second)
	}
	if source.ReadAt == first.ReadAt || source.ReadAt == second.ReadAt || first.ReadAt == second.ReadAt {
		t.Errorf("ReadAt pointers alias: source=%p first=%p second=%p", source.ReadAt, first.ReadAt, second.ReadAt)
	}
	if source.ResolvedAt == first.ResolvedAt || source.ResolvedAt == second.ResolvedAt || first.ResolvedAt == second.ResolvedAt {
		t.Errorf("ResolvedAt pointers alias: source=%p first=%p second=%p", source.ResolvedAt, first.ResolvedAt, second.ResolvedAt)
	}

	first.Body = "first-mutated"
	*first.ReadAt = "first-read-mutated"
	*first.ResolvedAt = "first-resolved-mutated"
	if source.Body != "original" || second.Body != "original" {
		t.Errorf("body mutation escaped first delivery: source=%q second=%q", source.Body, second.Body)
	}
	if *source.ReadAt != wantReadAt || *second.ReadAt != wantReadAt {
		t.Errorf("ReadAt mutation escaped first delivery: source=%q second=%q", *source.ReadAt, *second.ReadAt)
	}
	if *source.ResolvedAt != wantResolvedAt || *second.ResolvedAt != wantResolvedAt {
		t.Errorf("ResolvedAt mutation escaped first delivery: source=%q second=%q", *source.ResolvedAt, *second.ResolvedAt)
	}

	p.closeAll()
}

// TestSubscribe_IgnoresNonMatching verifies that a subscription for
// (sessionID, agentID) does not receive messages addressed to a different
// (sessionID, agentID) pair.
func TestSubscribe_IgnoresNonMatching(t *testing.T) {
	svc, _ := newTestService(t, "file-a", "file-b")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("SubscribeSessionAgent: %v", err)
	}

	if _, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   testHumanID,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-b",
		Body:          "not for us",
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case received := <-ch:
		t.Errorf("unexpected push to file-a subscriber: %+v", received)
	case <-time.After(100 * time.Millisecond):
		// good — nothing arrived.
	}
}

// TestSubscribe_NoReplayOnResubscribe verifies there is no replay buffer:
// a message published before any subscription exists is dropped, and a
// subsequent subscriber does not receive it.
func TestSubscribe_NoReplayOnResubscribe(t *testing.T) {
	svc, _ := newTestService(t, "file-a")

	// Publish BEFORE subscribing. There are zero subscribers, so this must
	// be dropped by the pubsub.
	if _, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   testHumanID,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-a",
		Body:          "old",
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("SubscribeSessionAgent: %v", err)
	}

	select {
	case received := <-ch:
		t.Errorf("expected no replay of pre-subscription messages, got: %+v", received)
	case <-time.After(100 * time.Millisecond):
		// good — no replay.
	}
}

// --- F07 Medium: Subscriber cleanup on service shutdown ---

// TestService_Close_DrainsSubscribers covers F07. Opening N subscriptions
// then calling Service.Close unblocks each receive with a closed-channel
// signal and empties the pubsub map — so a shutting-down host
// does not strand goroutines blocked on reads of a channel that will
// never deliver again.
func TestService_Close_DrainsSubscribers(t *testing.T) {
	svc, _ := newTestService(t, "file-a", "file-b", "file-c")

	// Three independent subscriptions across two keys. Use separate
	// ctxs so nothing is canceled when Close fires — only Close should
	// be the thing that closes the channels.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chA, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe a: %v", err)
	}
	chB, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-b")
	if err != nil {
		t.Fatalf("subscribe b: %v", err)
	}
	chC, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-c")
	if err != nil {
		t.Fatalf("subscribe c: %v", err)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Each subscriber should see its channel closed. Reading from a
	// closed channel returns the zero value with ok=false; we use a
	// timeout to prove we don't block.
	for _, tc := range []struct {
		name string
		ch   <-chan *Message
	}{{"a", chA}, {"b", chB}, {"c", chC}} {
		t.Run(tc.name, func(t *testing.T) {
			select {
			case _, ok := <-tc.ch:
				if ok {
					t.Errorf("expected closed channel, got a live message")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("timeout waiting for channel close")
			}
		})
	}

	// Map must be empty so publish is a true no-op after Close.
	svc.pub.mu.RLock()
	size := len(svc.pub.subs)
	svc.pub.mu.RUnlock()
	if size != 0 {
		t.Errorf("subs map size = %d after Close, want 0", size)
	}

	// Close is idempotent — calling again must not panic.
	if err := svc.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func subscriptionJanitorCount() int {
	stack := make([]byte, 8<<20)
	n := runtime.Stack(stack, true)
	return bytes.Count(stack[:n], []byte("mailbox.(*pubsub).subscribe.func1"))
}

// TestService_Close_TerminatesBackgroundSubscriptionJanitors is a leak
// regression for non-cancelable subscriptions. A large cohort makes the stack
// census deterministic while Close's internal join makes the postcondition
// exact for all janitors owned by this service.
func TestService_Close_TerminatesBackgroundSubscriptionJanitors(t *testing.T) {
	const subscriptionCount = 256

	svc, _ := newTestService(t, "file-a")
	baseline := subscriptionJanitorCount()
	channels := make([]<-chan *Message, 0, subscriptionCount)
	for i := 0; i < subscriptionCount; i++ {
		ch, err := svc.SubscribeSessionAgent(context.Background(), "sess-1", "file-a")
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		channels = append(channels, ch)
	}

	deadline := time.Now().Add(5 * time.Second)
	for subscriptionJanitorCount() < baseline+subscriptionCount && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := subscriptionJanitorCount(); got < baseline+subscriptionCount {
		t.Fatalf("started janitors = %d above baseline, want %d", got-baseline, subscriptionCount)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := subscriptionJanitorCount(); got > baseline {
		t.Fatalf("subscription janitors after Close = %d above baseline, want 0", got-baseline)
	}
	for i, ch := range channels {
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatalf("subscription %d remained open", i)
			}
		default:
			t.Fatalf("subscription %d did not close", i)
		}
	}
}

// TestSubscribe_CtxCancelReleasesSlot covers the F06/F07 interaction:
// canceling the subscription ctx must remove the subscriber from the
// map and close its channel, so a handler that loses its transport stream
// doesn't strand a zombie entry.
func TestSubscribe_CtxCancelReleasesSlot(t *testing.T) {
	svc, _ := newTestService(t, "file-a")

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Verify the subscriber is in the map.
	svc.pub.mu.RLock()
	key := subscriptionKey{sessionID: "sess-1", agentID: "file-a"}
	before := len(svc.pub.subs[key])
	svc.pub.mu.RUnlock()
	if before != 1 {
		t.Fatalf("pre-cancel subs len=%d, want 1", before)
	}

	cancel()

	// Wait for the unsubscribe goroutine to do its work. Use a closed-
	// channel observation as the signal — that fires only after the
	// goroutine has taken the lock and done its cleanup.
	select {
	case _, ok := <-ch:
		if ok {
			t.Errorf("expected closed channel, got live message")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for ctx-driven close")
	}

	svc.pub.mu.RLock()
	after := len(svc.pub.subs[key])
	svc.pub.mu.RUnlock()
	if after != 0 {
		t.Errorf("post-cancel subs len=%d, want 0", after)
	}
}

// TestSubscribe_PublishUnsubscribeRace stresses the close-during-publish window.
// Before the fix this reliably panicked with "send on closed channel" under -race
// within a few iterations.
func TestSubscribe_PublishUnsubscribeRace(t *testing.T) {
	svc, _ := newTestService(t, "file-a")

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			defer wg.Done()
			_, _ = svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
			cancel()
		}()
		go func() {
			defer wg.Done()
			_, _ = svc.SendMessage(context.Background(), SendInput{
				FromSessionID: "sess-1",
				FromAgentID:   testHumanID,
				ToSessionID:   "sess-1",
				ToAgentID:     "file-a",
				Body:          "race",
			})
		}()
	}
	wg.Wait()
}

func TestSubscribe_AfterCloseRejected(t *testing.T) {
	svc, _ := newTestService(t, "file-a")
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ch, err := svc.SubscribeSessionAgent(context.Background(), "sess-1", "file-a")
	if ch != nil {
		t.Errorf("channel = %v, want nil", ch)
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrClosed", err)
	}
}

func TestPubsub_CloseLinearizesSubscriptionAdmission(t *testing.T) {
	for i := 0; i < 500; i++ {
		p := newPubsub()
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(2)
		var ch <-chan *Message
		var err error
		go func() {
			defer wg.Done()
			ch, err = p.subscribe(ctx, "sess-1", "file-a")
		}()
		go func() {
			defer wg.Done()
			p.closeAll()
		}()
		wg.Wait()
		cancel()

		if errors.Is(err, ErrClosed) {
			if ch != nil {
				t.Fatalf("iteration %d: rejected subscription returned channel", i)
			}
			continue
		}
		if err != nil {
			t.Fatalf("iteration %d: subscribe: %v", i, err)
		}
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatalf("iteration %d: admitted channel remained open", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: admitted channel was not closed", i)
		}
	}
}

func TestPubsub_ConcurrentPublishAndCloseIsIdempotent(t *testing.T) {
	p := newPubsub()
	for i := 0; i < 64; i++ {
		if _, err := p.subscribe(context.Background(), "sess-1", "file-a"); err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
	}
	msg := &Message{ToSessionID: "sess-1", ToAgentID: "file-a", Body: "race"}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				p.publish(msg)
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.closeAll()
		}()
	}
	wg.Wait()

	if _, err := p.subscribe(context.Background(), "sess-1", "file-a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close subscribe error = %v, want ErrClosed", err)
	}
}
