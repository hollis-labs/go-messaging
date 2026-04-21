package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
)

func newAddr(kind messaging.AddressKind, id string) messaging.Address {
	return messaging.Address{Kind: kind, Authority: "test", ID: id}
}

func TestMemstore_Send_AssignsID(t *testing.T) {
	s := New()
	ctx := context.Background()
	in := messaging.Envelope{
		Kind:    messaging.MsgKindNotice,
		From:    newAddr(messaging.KindAgent, "sender"),
		To:      newAddr(messaging.KindAgent, "recipient"),
		Payload: json.RawMessage(`{"hi":1}`),
	}
	out, err := s.Send(ctx, in)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if out.ID == "" {
		t.Error("Send did not assign ID")
	}
	if out.CreatedAt.IsZero() {
		t.Error("Send did not assign CreatedAt")
	}
}

func TestMemstore_Send_OverwritesCallerID(t *testing.T) {
	s := New()
	in := messaging.Envelope{
		ID:   "caller-chosen-id",
		Kind: messaging.MsgKindNotice,
		From: newAddr(messaging.KindAgent, "sender"),
		To:   newAddr(messaging.KindAgent, "recipient"),
	}
	out, err := s.Send(context.Background(), in)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if out.ID == "caller-chosen-id" {
		t.Error("Send should overwrite caller-provided ID")
	}
}

func TestMemstore_Send_RejectsPresetLifecycle(t *testing.T) {
	s := New()
	now := time.Now()
	bad := messaging.Envelope{
		Kind:        messaging.MsgKindNotice,
		From:        newAddr(messaging.KindAgent, "s"),
		To:          newAddr(messaging.KindAgent, "r"),
		DeliveredAt: &now,
	}
	_, err := s.Send(context.Background(), bad)
	if !errors.Is(err, messaging.ErrPresetLifecycle) {
		t.Errorf("expected ErrPresetLifecycle, got %v", err)
	}
}

func TestMemstore_Get(t *testing.T) {
	s := New()
	ctx := context.Background()
	in := messaging.Envelope{
		Kind: messaging.MsgKindNotice,
		From: newAddr(messaging.KindAgent, "s"),
		To:   newAddr(messaging.KindAgent, "r"),
	}
	sent, _ := s.Send(ctx, in)
	got, err := s.Get(ctx, sent.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != sent.ID {
		t.Errorf("Get returned wrong envelope: %+v", got)
	}
}

func TestMemstore_Get_NotFound(t *testing.T) {
	s := New()
	_, err := s.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemstore_Inbox_ReturnsUndelivered(t *testing.T) {
	s := New()
	ctx := context.Background()
	to := newAddr(messaging.KindAgent, "r")

	// Send three envelopes to the same recipient.
	for i := 0; i < 3; i++ {
		_, err := s.Send(ctx, messaging.Envelope{
			Kind: messaging.MsgKindNotice,
			From: newAddr(messaging.KindAgent, "s"),
			To:   to,
		})
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	got, err := s.Inbox(ctx, to, messaging.Filter{})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("first Inbox: got %d, want 3", len(got))
	}
	for _, e := range got {
		if e.DeliveredAt == nil {
			t.Errorf("Inbox result should have DeliveredAt set: %+v", e)
		}
	}

	// Second Inbox call: all were marked delivered, should return empty.
	got2, err := s.Inbox(ctx, to, messaging.Filter{})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(got2) != 0 {
		t.Errorf("second Inbox: got %d, want 0 (already delivered)", len(got2))
	}
}

func TestMemstore_Inbox_ChronologicalOrder(t *testing.T) {
	s := New()
	ctx := context.Background()
	to := newAddr(messaging.KindAgent, "r")

	var ids []string
	for i := 0; i < 5; i++ {
		sent, _ := s.Send(ctx, messaging.Envelope{
			Kind: messaging.MsgKindNotice,
			From: newAddr(messaging.KindAgent, "s"),
			To:   to,
		})
		ids = append(ids, sent.ID)
		// Brief delay to ensure monotonic timestamps on systems with
		// low time resolution. UUIDv7 is also monotonic.
		time.Sleep(time.Millisecond)
	}

	got, _ := s.Inbox(ctx, to, messaging.Filter{})
	if len(got) != 5 {
		t.Fatalf("expected 5 envelopes, got %d", len(got))
	}
	for i, e := range got {
		if e.ID != ids[i] {
			t.Errorf("position %d: got ID %s, want %s", i, e.ID, ids[i])
		}
	}
}

func TestMemstore_Inbox_FilterByKind(t *testing.T) {
	s := New()
	ctx := context.Background()
	to := newAddr(messaging.KindAgent, "r")

	send := func(kind messaging.Kind) {
		_, _ = s.Send(ctx, messaging.Envelope{
			Kind: kind,
			From: newAddr(messaging.KindAgent, "s"),
			To:   to,
		})
	}
	send(messaging.MsgKindRequest)
	send(messaging.MsgKindNotice)
	send(messaging.MsgKindRequest)

	got, _ := s.Inbox(ctx, to, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindRequest}})
	if len(got) != 2 {
		t.Errorf("filter by Kind=request: got %d, want 2", len(got))
	}
}

func TestMemstore_Inbox_RespectsRecipient(t *testing.T) {
	s := New()
	ctx := context.Background()
	rA := newAddr(messaging.KindAgent, "A")
	rB := newAddr(messaging.KindAgent, "B")
	_, _ = s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: newAddr(messaging.KindAgent, "s"), To: rA})
	_, _ = s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: newAddr(messaging.KindAgent, "s"), To: rB})

	gotA, _ := s.Inbox(ctx, rA, messaging.Filter{})
	if len(gotA) != 1 {
		t.Errorf("recipient A: got %d, want 1", len(gotA))
	}
	gotB, _ := s.Inbox(ctx, rB, messaging.Filter{})
	if len(gotB) != 1 {
		t.Errorf("recipient B: got %d, want 1", len(gotB))
	}
}

func TestMemstore_Consume(t *testing.T) {
	s := New()
	ctx := context.Background()
	to := newAddr(messaging.KindAgent, "r")
	sent, _ := s.Send(ctx, messaging.Envelope{
		Kind: messaging.MsgKindNotice,
		From: newAddr(messaging.KindAgent, "s"),
		To:   to,
	})
	_, _ = s.Inbox(ctx, to, messaging.Filter{}) // deliver

	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got, _ := s.Get(ctx, sent.ID)
	if got.ConsumedAt == nil {
		t.Error("Consume did not set ConsumedAt")
	}
}

func TestMemstore_Consume_Idempotent(t *testing.T) {
	s := New()
	ctx := context.Background()
	to := newAddr(messaging.KindAgent, "r")
	sent, _ := s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: newAddr(messaging.KindAgent, "s"), To: to})
	_, _ = s.Inbox(ctx, to, messaging.Filter{})

	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Fatalf("first Consume: %v", err)
	}
	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Errorf("second Consume should be idempotent, got %v", err)
	}
}

func TestMemstore_Cancel(t *testing.T) {
	s := New()
	ctx := context.Background()
	sent, _ := s.Send(ctx, messaging.Envelope{
		Kind: messaging.MsgKindRequest,
		From: newAddr(messaging.KindAgent, "s"),
		To:   newAddr(messaging.KindAgent, "r"),
	})
	if err := s.Cancel(ctx, sent.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	// Idempotent
	if err := s.Cancel(ctx, sent.ID); err != nil {
		t.Errorf("second Cancel: %v", err)
	}
}

func TestMemstore_Cancel_NotFound(t *testing.T) {
	s := New()
	if err := s.Cancel(context.Background(), "does-not-exist"); !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("Cancel of missing envelope: got %v, want ErrNotFound", err)
	}
}

func TestMemstore_Thread_ReturnsChronological(t *testing.T) {
	s := New()
	ctx := context.Background()
	from := newAddr(messaging.KindAgent, "s")
	to := newAddr(messaging.KindAgent, "r")

	// Two envelopes in thread T1, one in T2.
	in1, _ := s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to, ThreadID: "T1"})
	time.Sleep(time.Millisecond)
	in2, _ := s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to, ThreadID: "T2"})
	time.Sleep(time.Millisecond)
	in3, _ := s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to, ThreadID: "T1"})

	got, err := s.Thread(ctx, "T1", messaging.Filter{})
	if err != nil {
		t.Fatalf("Thread: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
	if got[0].ID != in1.ID || got[1].ID != in3.ID {
		t.Errorf("wrong order: [%s, %s]", got[0].ID, got[1].ID)
	}
	_ = in2 // unused — just confirmed it's in a different thread
}

func TestMemstore_Thread_NoSideEffects(t *testing.T) {
	s := New()
	ctx := context.Background()
	from := newAddr(messaging.KindAgent, "s")
	to := newAddr(messaging.KindAgent, "r")

	_, _ = s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to, ThreadID: "T1"})
	_, _ = s.Thread(ctx, "T1", messaging.Filter{})

	// Inbox should still return the envelope — Thread did not deliver it.
	inbox, _ := s.Inbox(ctx, to, messaging.Filter{})
	if len(inbox) != 1 {
		t.Errorf("Thread should not mutate delivery state; Inbox got %d", len(inbox))
	}
}

func TestMemstore_Subscribe_LiveOnly(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	from := newAddr(messaging.KindAgent, "s")
	to := newAddr(messaging.KindAgent, "r")

	// Historical envelope (pre-subscribe) — should NOT be replayed.
	_, _ = s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to})

	ch, err := s.Subscribe(ctx, messaging.Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Post-subscribe envelope — should arrive.
	sent, _ := s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to})

	select {
	case got := <-ch:
		if got.ID != sent.ID {
			t.Errorf("wrong envelope on channel: %s vs %s", got.ID, sent.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive envelope within 1s")
	}
}

func TestMemstore_Subscribe_FiltersApply(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	from := newAddr(messaging.KindAgent, "s")
	to := newAddr(messaging.KindAgent, "r")

	ch, _ := s.Subscribe(ctx, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindRequest}})
	_, _ = s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to})
	sent, _ := s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindRequest, From: from, To: to})

	select {
	case got := <-ch:
		if got.ID != sent.ID {
			t.Errorf("filter leaked notice envelope or wrong envelope: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("filtered subscribe timeout")
	}
}

func TestMemstore_Subscribe_CtxCancelClosesChannel(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := s.Subscribe(ctx, messaging.Filter{})
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close within 1s of ctx cancel")
	}
}

func TestMemstore_Subscribe_MultipleSubscribers(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	from := newAddr(messaging.KindAgent, "s")
	to := newAddr(messaging.KindAgent, "r")

	ch1, _ := s.Subscribe(ctx, messaging.Filter{})
	ch2, _ := s.Subscribe(ctx, messaging.Filter{})

	_, _ = s.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to})

	for i, ch := range []<-chan messaging.Envelope{ch1, ch2} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Errorf("subscriber %d did not receive", i)
		}
	}
}
