package messaging_test

import (
	"context"
	"encoding/json"
	"testing"

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
