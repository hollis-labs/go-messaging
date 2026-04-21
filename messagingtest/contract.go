// Package messagingtest provides a shared behavioral test suite every
// messaging.Store impl runs through. Each impl calls RunContract from
// its own *_test.go to assert conformance.
package messagingtest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
)

// Factory constructs a fresh Store for one sub-test. Impls that require
// teardown should return a Store whose close is tied to t.Cleanup.
type Factory func(t *testing.T) messaging.Store

// RunContract exercises every guarantee in the messaging.Store contract
// against the factory-provided Store. Run from an impl's own test file:
//
//	func TestMemstoreContract(t *testing.T) {
//	    messagingtest.RunContract(t, func(t *testing.T) messaging.Store {
//	        return memstore.New()
//	    })
//	}
func RunContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("Send assigns ID + CreatedAt", func(t *testing.T) {
		s := factory(t)
		out, err := s.Send(context.Background(), basicEnv())
		must(t, err)
		if out.ID == "" || out.CreatedAt.IsZero() {
			t.Errorf("Send did not assign ID/CreatedAt: %+v", out)
		}
	})

	t.Run("Send rejects preset lifecycle", func(t *testing.T) {
		s := factory(t)
		now := time.Now()
		bad := basicEnv()
		bad.DeliveredAt = &now
		_, err := s.Send(context.Background(), bad)
		if !errors.Is(err, messaging.ErrPresetLifecycle) {
			t.Errorf("got %v, want ErrPresetLifecycle", err)
		}
	})

	t.Run("Get returns ErrNotFound for missing", func(t *testing.T) {
		s := factory(t)
		_, err := s.Get(context.Background(), "no-such")
		if !errors.Is(err, messaging.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("Inbox atomic delivery", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		to := recipient("r1")
		_, _ = s.Send(ctx, withTo(basicEnv(), to))

		first, _ := s.Inbox(ctx, to, messaging.Filter{})
		if len(first) != 1 {
			t.Fatalf("first Inbox: got %d, want 1", len(first))
		}
		second, _ := s.Inbox(ctx, to, messaging.Filter{})
		if len(second) != 0 {
			t.Errorf("second Inbox: got %d, want 0", len(second))
		}
	})

	t.Run("Inbox chronological + tie-break", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		to := recipient("r2")
		for i := 0; i < 3; i++ {
			_, _ = s.Send(ctx, withTo(basicEnv(), to))
			time.Sleep(time.Millisecond)
		}
		got, _ := s.Inbox(ctx, to, messaging.Filter{})
		if len(got) != 3 {
			t.Fatalf("got %d", len(got))
		}
		for i := 1; i < len(got); i++ {
			if got[i].CreatedAt.Before(got[i-1].CreatedAt) {
				t.Errorf("out-of-order at index %d", i)
			}
		}
	})

	t.Run("Consume sets ConsumedAt, idempotent", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		to := recipient("r3")
		sent, _ := s.Send(ctx, withTo(basicEnv(), to))
		_, _ = s.Inbox(ctx, to, messaging.Filter{})
		must(t, s.Consume(ctx, sent.ID, to))
		must(t, s.Consume(ctx, sent.ID, to)) // idempotent
		got, _ := s.Get(ctx, sent.ID)
		if got.ConsumedAt == nil {
			t.Error("ConsumedAt nil after Consume")
		}
	})

	t.Run("Cancel marks dead, idempotent", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		sent, _ := s.Send(ctx, basicEnv())
		must(t, s.Cancel(ctx, sent.ID))
		must(t, s.Cancel(ctx, sent.ID))
	})

	t.Run("Cancel NotFound", func(t *testing.T) {
		s := factory(t)
		if err := s.Cancel(context.Background(), "no-such"); !errors.Is(err, messaging.ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("Thread chronological, no side effects", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		to := recipient("r4")
		env := withTo(basicEnv(), to)
		env.ThreadID = "TT"
		_, _ = s.Send(ctx, env)
		_, _ = s.Send(ctx, env)

		got, _ := s.Thread(ctx, "TT", messaging.Filter{})
		if len(got) != 2 {
			t.Errorf("Thread got %d", len(got))
		}
		// Inbox should still have both (Thread is read-only).
		inbox, _ := s.Inbox(ctx, to, messaging.Filter{})
		if len(inbox) != 2 {
			t.Errorf("Thread mutated delivery state: Inbox got %d", len(inbox))
		}
	})

	t.Run("Subscribe live-only", func(t *testing.T) {
		s := factory(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sub := recipient("sub1")
		historical := basicEnv()
		historical.To = sub
		_, _ = s.Send(ctx, historical) // historical — must not appear

		ch, err := s.Subscribe(ctx, sub, messaging.Filter{})
		must(t, err)

		live := basicEnv()
		live.To = sub
		sent, _ := s.Send(ctx, live)

		select {
		case got := <-ch:
			if got.ID != sent.ID {
				t.Errorf("got %s, want %s (or historical leaked)", got.ID, sent.ID)
			}
		case <-time.After(time.Second):
			t.Fatal("no envelope within 1s")
		}
	})

	t.Run("Subscribe filters to recipient only", func(t *testing.T) {
		s := factory(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		alice := recipient("subAlice")
		bob := recipient("subBob")

		ch, err := s.Subscribe(ctx, alice, messaging.Filter{})
		must(t, err)

		// Send to Bob — should NOT appear on Alice's subscription.
		bobEnv := basicEnv()
		bobEnv.To = bob
		_, _ = s.Send(ctx, bobEnv)

		// Send to Alice — SHOULD appear.
		aliceEnv := basicEnv()
		aliceEnv.To = alice
		sent, _ := s.Send(ctx, aliceEnv)

		select {
		case got := <-ch:
			if got.ID != sent.ID {
				t.Errorf("received %s, want %s (bob's message leaked)", got.ID, sent.ID)
			}
		case <-time.After(time.Second):
			t.Fatal("no envelope within 1s")
		}
	})

	t.Run("Subscribe ctx cancel closes channel", func(t *testing.T) {
		s := factory(t)
		ctx, cancel := context.WithCancel(context.Background())
		ch, _ := s.Subscribe(ctx, recipient("subCtx"), messaging.Filter{})
		cancel()
		select {
		case _, ok := <-ch:
			if ok {
				t.Error("channel should close after ctx cancel")
			}
		case <-time.After(time.Second):
			t.Fatal("channel did not close within 1s")
		}
	})

	t.Run("Dispatcher.Request round-trip", func(t *testing.T) {
		s := factory(t)
		d := messaging.NewDispatcher(s)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		A := recipient("rqA")
		B := recipient("rqB")

		go func() {
			// Bob subscribes to requests addressed to him.
			sub, err := s.Subscribe(ctx, B, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindRequest}})
			if err != nil {
				return
			}
			for r := range sub {
				_, _ = d.Reply(ctx, r, json.RawMessage(`"pong"`))
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)

		resp, err := d.Request(ctx, messaging.Envelope{
			From:    A,
			To:      B,
			Payload: json.RawMessage(`"ping"`),
		})
		must(t, err)
		if string(resp.Payload) != `"pong"` {
			t.Errorf("resp = %s", string(resp.Payload))
		}
	})

	t.Run("Dispatcher.Request times out", func(t *testing.T) {
		s := factory(t)
		d := messaging.NewDispatcher(s)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		_, err := d.Request(ctx, messaging.Envelope{From: recipient("rA"), To: recipient("rB")})
		if !errors.Is(err, messaging.ErrRequestTimeout) {
			t.Errorf("got %v, want ErrRequestTimeout", err)
		}
	})
}

// Helpers (kept in this file so impls don't have to duplicate them).

func basicEnv() messaging.Envelope {
	return messaging.Envelope{
		Kind: messaging.MsgKindNotice,
		From: messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "sender"},
		To:   messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "recipient"},
	}
}

func withTo(e messaging.Envelope, to messaging.Address) messaging.Envelope {
	e.To = to
	return e
}

func recipient(id string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: id}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
