package mailbox

import (
	"context"
	"testing"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/delivery"
)

func TestCompatibilityAddressesPreserveTupleAndStableActor(t *testing.T) {
	legacy := LegacyTupleAddress("nanite", "sess-1", "planner")
	if legacy.URN() != "msg://agent/nanite/sess-1/planner" {
		t.Fatalf("legacy tuple urn = %s", legacy.URN())
	}
	sessionID, agentID, ok := TupleFromAddress(legacy)
	if !ok || sessionID != "sess-1" || agentID != "planner" {
		t.Fatalf("tuple round trip failed: session=%q agent=%q ok=%v", sessionID, agentID, ok)
	}
	stable := StableActorAddress("nanite", "planner")
	if stable.URN() != "msg://agent/nanite/planner" {
		t.Fatalf("stable actor urn = %s", stable.URN())
	}
	if _, _, ok := TupleFromAddress(stable); ok {
		t.Fatal("stable actor address should not fabricate a session tuple")
	}
	exact := ExactSessionAddress("nanite", "sess-1")
	if exact.Kind != messaging.KindSession || exact.URN() != "msg://session/nanite/sess-1" {
		t.Fatalf("exact session address = %+v", exact)
	}
}

func TestDeliveryRequestFromSendInputPreservesMailboxHistory(t *testing.T) {
	req, err := DeliveryRequestFromSendInput(SendInput{
		FromSessionID: "sess-a",
		FromAgentID:   "planner",
		ToSessionID:   "sess-b",
		ToAgentID:     "reviewer",
		ThreadID:      "thread-1",
		ReplyTo:       "parent-1",
		Type:          TypeDirective,
		Subject:       "check",
		Body:          "please check",
		Metadata:      `{"trace":"nanite"}`,
		Priority:      4,
		Channel:       ChannelInbox,
		Kind:          KindRequest,
		PayloadJSON:   `{"task":"check"}`,
	}, "nanite")
	if err != nil {
		t.Fatalf("DeliveryRequestFromSendInput: %v", err)
	}
	if req.From.URN() != "msg://agent/nanite/sess-a/planner" || req.Recipients[0].Address.URN() != "msg://agent/nanite/sess-b/reviewer" {
		t.Fatalf("tuple addresses not preserved: from=%s to=%s", req.From.URN(), req.Recipients[0].Address.URN())
	}
	if req.Recipients[0].Binding.SessionID != "sess-b" || req.Recipients[0].Binding.ActorID != "reviewer" {
		t.Fatalf("recipient binding not preserved: %+v", req.Recipients[0].Binding)
	}
	if req.Kind != messaging.MsgKindRequest {
		t.Fatalf("mailbox request kind projected to %q", req.Kind)
	}
	if req.Metadata["legacy_type"] != TypeDirective || req.Metadata["legacy_priority"] != "4" || req.ThreadID != "thread-1" || req.InReplyTo != "parent-1" {
		t.Fatalf("history metadata not preserved: %+v", req)
	}
}

func TestMailboxAttentionListIsNonDestructiveAndNotDeliveryAck(t *testing.T) {
	store := newTestMessagingStore(t)
	ctx := context.Background()
	msg, err := store.Send(ctx, SendInput{FromSessionID: "sess-1", FromAgentID: "alice", ToSessionID: "sess-1", ToAgentID: "bob", Body: "hello", Channel: ChannelInbox, Kind: KindNotification})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	views, err := AttentionList(ctx, store, "sess-1", "bob", InboxFilter{})
	if err != nil {
		t.Fatalf("AttentionList: %v", err)
	}
	if len(views) != 1 || views[0].State != AttentionUnread || views[0].Message.ID != msg.ID {
		t.Fatalf("bad attention list: %+v", views)
	}
	count, err := store.UnreadCount(ctx, "sess-1", "bob")
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("attention list acknowledged message; unread count=%d", count)
	}
	if err := store.Ack(ctx, msg.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	views, err = AttentionList(ctx, store, "sess-1", "bob", InboxFilter{})
	if err != nil {
		t.Fatalf("AttentionList after ack: %v", err)
	}
	if views[0].State != AttentionRead {
		t.Fatalf("attention state not read: %+v", views[0])
	}
	if err := store.Archive(ctx, msg.ID); err != nil {
		t.Fatalf("archive: %v", err)
	}
	views, err = AttentionList(ctx, store, "sess-1", "bob", InboxFilter{})
	if err != nil {
		t.Fatalf("AttentionList after archive: %v", err)
	}
	if views[0].State != AttentionArchived {
		t.Fatalf("attention state not archived: %+v", views[0])
	}
}

func TestMailboxAttentionMutatorsDoNotTouchDeliveryStore(t *testing.T) {
	store := newTestMessagingStore(t)
	ctx := context.Background()
	msg, err := store.Send(ctx, SendInput{FromSessionID: "sess-1", FromAgentID: "alice", ToSessionID: "sess-1", ToAgentID: "bob", Body: "hello"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	deliveryStore := delivery.NewMemoryStore()
	dres, err := deliveryStore.Enqueue(ctx, delivery.EnqueueRequest{From: StableActorAddress("nanite", "alice"), Recipients: []delivery.RecipientTarget{delivery.StableActorTarget(StableActorAddress("nanite", "bob"), "bob", 1)}, Kind: messaging.MsgKindNotice, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}
	if err := store.MarkUnread(ctx, msg.ID); err != nil {
		t.Fatalf("mark unread: %v", err)
	}
	del, err := deliveryStore.GetDelivery(ctx, dres.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if del.Status != delivery.DeliveryPending || del.AttemptCount != 0 {
		t.Fatalf("mailbox attention mutation touched delivery state: %+v", del)
	}
}
