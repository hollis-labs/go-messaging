package delivery_test

import (
	"errors"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/delivery"
)

func TestEnvelopeProjectionPreservesRootCompatibilityShape(t *testing.T) {
	env := messaging.Envelope{
		Kind:        messaging.MsgKindRequest,
		Channel:     "control",
		From:        agent("alice"),
		To:          agent("bob"),
		ThreadID:    "thread-1",
		InReplyTo:   "parent-1",
		Payload:     []byte(`{"body":"hello"}`),
		ContentType: "application/json",
		Metadata:    map[string]string{"trace": "root"},
	}
	req, err := delivery.EnvelopeEnqueueRequest(env)
	if err != nil {
		t.Fatalf("EnvelopeEnqueueRequest: %v", err)
	}
	if req.From != env.From || len(req.Recipients) != 1 || req.Recipients[0].Address != env.To || req.ThreadID != env.ThreadID || req.InReplyTo != env.InReplyTo {
		t.Fatalf("projection lost root fields: %+v", req)
	}
	req.Payload[0] = 'X'
	if string(env.Payload)[0] == 'X' {
		t.Fatal("projection aliases payload")
	}
}

func TestEnvelopeProjectionRejectsPresetLifecycle(t *testing.T) {
	now := time.Now().UTC()
	env := messaging.Envelope{Kind: messaging.MsgKindNotice, From: agent("alice"), To: agent("bob"), DeliveredAt: &now}
	_, err := delivery.EnvelopeEnqueueRequest(env)
	if !errors.Is(err, messaging.ErrPresetLifecycle) {
		t.Fatalf("err=%v, want ErrPresetLifecycle", err)
	}
}

func TestEnvelopeFromDeliveryKeepsAttentionSeparateFromReceipts(t *testing.T) {
	msg := delivery.Message{ID: "m1", Kind: messaging.MsgKindNotice, From: agent("alice"), Payload: []byte(`{}`), CreatedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
	del := delivery.RecipientDelivery{ID: "d1", MessageID: "m1", Recipient: agent("bob"), Status: delivery.DeliveryLeased}
	env := delivery.EnvelopeFromDelivery(msg, del, []delivery.Receipt{{Stage: delivery.StageHostAccepted, At: msg.CreatedAt.Add(time.Minute)}})
	if env.DeliveredAt == nil || env.ConsumedAt != nil {
		t.Fatalf("host receipt should project delivered only: %+v", env)
	}
	env = delivery.EnvelopeFromDelivery(msg, del, []delivery.Receipt{{Stage: delivery.StageConsumed, At: msg.CreatedAt.Add(2 * time.Minute)}})
	if env.DeliveredAt == nil || env.ConsumedAt == nil {
		t.Fatalf("consumed receipt should project both compatibility fields: %+v", env)
	}
}

func TestCanonicalTargetsDistinguishStableActorAndExactSession(t *testing.T) {
	actor := delivery.StableActorTarget(agent("planner"), "planner", 7)
	if actor.Binding.ActorID != "planner" || actor.Binding.SessionID != "" || actor.Binding.BindingGeneration != 7 {
		t.Fatalf("bad stable actor target: %+v", actor)
	}
	sessionAddr := messaging.Address{Kind: messaging.KindSession, Authority: "test", ID: "sess-1"}
	session := delivery.ExactSessionTarget(sessionAddr, "sess-1", 8)
	if session.Binding.SessionID != "sess-1" || session.Binding.ActorID != "" || session.Address.Kind != messaging.KindSession {
		t.Fatalf("bad exact session target: %+v", session)
	}
}
