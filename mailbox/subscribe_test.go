// Package mailbox — subscribe_test.go
//
// Tests for the in-process pubsub that backs MCP streaming subscribers.
// The pubsub has no replay buffer: only messages published AFTER a
// subscription takes effect are delivered to that subscriber, and slow
// subscribers are dropped on a full channel (buffer = 16).
package mailbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestSubscribe_ReceivesNewMessage verifies that SubscribeSessionAgent
// returns a channel which receives a matching message published after the
// subscription is established.
func TestSubscribe_ReceivesNewMessage(t *testing.T) {
	svc, _, _ := newTestService(t, "file-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("SubscribeSessionAgent: %v", err)
	}

	msg := SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-a",
		Body:          "hello",
	}

	// Publish in a goroutine after a small delay so the receive below is
	// already blocking on the channel when the send fires. The delay is
	// deliberate: this test proves live fan-out, not buffered delivery.
	go func() {
		time.Sleep(10 * time.Millisecond)
		if _, err := svc.SendMessage(context.Background(), msg); err != nil {
			t.Errorf("SendMessage: %v", err)
		}
	}()

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

// TestSubscribe_IgnoresNonMatching verifies that a subscription for
// (sessionID, agentID) does not receive messages addressed to a different
// (sessionID, agentID) pair.
func TestSubscribe_IgnoresNonMatching(t *testing.T) {
	svc, _, _ := newTestService(t, "file-a", "file-b")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("SubscribeSessionAgent: %v", err)
	}

	if _, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
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
	svc, _, _ := newTestService(t, "file-a")

	// Publish BEFORE subscribing. There are zero subscribers, so this must
	// be dropped by the pubsub.
	if _, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
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
	svc, _, _ := newTestService(t, "file-a", "file-b", "file-c")

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

// TestSubscribe_CtxCancelReleasesSlot covers the F06/F07 interaction:
// canceling the subscription ctx must remove the subscriber from the
// map and close its channel, so a handler that loses its MCP stream
// doesn't strand a zombie entry.
func TestSubscribe_CtxCancelReleasesSlot(t *testing.T) {
	svc, _, _ := newTestService(t, "file-a")

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := svc.SubscribeSessionAgent(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Verify the subscriber is in the map.
	svc.pub.mu.RLock()
	before := len(svc.pub.subs["sess-1:file-a"])
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
	after := len(svc.pub.subs["sess-1:file-a"])
	svc.pub.mu.RUnlock()
	if after != 0 {
		t.Errorf("post-cancel subs len=%d, want 0", after)
	}
}

// TestSubscribe_PublishUnsubscribeRace stresses the close-during-publish window.
// Before the fix this reliably panicked with "send on closed channel" under -race
// within a few iterations.
func TestSubscribe_PublishUnsubscribeRace(t *testing.T) {
	svc, _, _ := newTestService(t, "file-a")

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
				FromAgentID:   UserSentinel,
				ToSessionID:   "sess-1",
				ToAgentID:     "file-a",
				Body:          "race",
			})
		}()
	}
	wg.Wait()
}
