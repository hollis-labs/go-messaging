package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/memstore"
)

func addr(kind messaging.AddressKind, id string) messaging.Address {
	return messaging.Address{Kind: kind, Authority: "test", ID: id}
}

func TestDispatcher_Reply_WiresFields(t *testing.T) {
	s := memstore.New()
	d := messaging.NewDispatcher(s)
	ctx := context.Background()

	parent := messaging.Envelope{
		Kind:     messaging.MsgKindRequest,
		From:     addr(messaging.KindAgent, "A"),
		To:       addr(messaging.KindAgent, "B"),
		ThreadID: "T1",
	}
	parentSent, err := d.Send(ctx, parent)
	if err != nil {
		t.Fatalf("Send parent: %v", err)
	}

	reply, err := d.Reply(ctx, parentSent, json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}

	if reply.Kind != messaging.MsgKindResponse {
		t.Errorf("reply.Kind = %q, want response", reply.Kind)
	}
	if reply.InReplyTo != parentSent.ID {
		t.Errorf("reply.InReplyTo = %q, want %q", reply.InReplyTo, parentSent.ID)
	}
	if reply.ThreadID != "T1" {
		t.Errorf("reply.ThreadID = %q, want T1", reply.ThreadID)
	}
	if reply.From != parent.To || reply.To != parent.From {
		t.Errorf("reply From/To not swapped: from=%v to=%v", reply.From, reply.To)
	}
	if string(reply.Payload) != `{"ok":true}` {
		t.Errorf("reply payload: %s", string(reply.Payload))
	}
}

func TestDispatcher_Request_ReceivesResponse(t *testing.T) {
	s := memstore.New()
	d := messaging.NewDispatcher(s)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	A := addr(messaging.KindAgent, "A")
	B := addr(messaging.KindAgent, "B")

	// Goroutine B: subscribe for requests, respond to each.
	go func() {
		sub, err := s.Subscribe(ctx, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindRequest}})
		if err != nil {
			return
		}
		for req := range sub {
			if req.To != B {
				continue
			}
			_, _ = d.Reply(ctx, req, json.RawMessage(`{"answer":42}`))
			return
		}
	}()
	// Tiny yield to ensure subscribe is established before Request fires.
	time.Sleep(10 * time.Millisecond)

	resp, err := d.Request(ctx, messaging.Envelope{
		From:    A,
		To:      B,
		Payload: json.RawMessage(`{"q":"what"}`),
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if resp.Kind != messaging.MsgKindResponse {
		t.Errorf("resp.Kind = %q", resp.Kind)
	}
	if string(resp.Payload) != `{"answer":42}` {
		t.Errorf("resp.Payload = %s", string(resp.Payload))
	}
}

func TestDispatcher_Request_TimesOut(t *testing.T) {
	s := memstore.New()
	d := messaging.NewDispatcher(s)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := d.Request(ctx, messaging.Envelope{
		From: addr(messaging.KindAgent, "A"),
		To:   addr(messaging.KindAgent, "B"),
	})
	if !errors.Is(err, messaging.ErrRequestTimeout) {
		t.Errorf("got %v, want ErrRequestTimeout", err)
	}
}

func TestDispatcher_Request_OverwritesKindAndID(t *testing.T) {
	s := memstore.New()
	d := messaging.NewDispatcher(s)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, _ = d.Request(ctx, messaging.Envelope{
		ID:   "caller-set",
		Kind: messaging.MsgKindNotice, // wrong — should be overwritten
		From: addr(messaging.KindAgent, "A"),
		To:   addr(messaging.KindAgent, "B"),
	})
	// Fetch via Inbox on To=B to retrieve.
	got, err := s.Inbox(context.Background(), addr(messaging.KindAgent, "B"), messaging.Filter{})
	if err != nil || len(got) == 0 {
		t.Fatalf("Inbox: %v / %d", err, len(got))
	}
	if got[0].Kind != messaging.MsgKindRequest {
		t.Errorf("Request did not force Kind=request, got %q", got[0].Kind)
	}
	if got[0].ID == "caller-set" {
		t.Error("Request did not overwrite caller-provided ID")
	}
}
