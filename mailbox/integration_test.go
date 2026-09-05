package mailbox

import (
	"context"
	"testing"
)

// TestIntegration_HostAdaptersAndMailboxFlow exercises message persistence,
// host-owned events, and host-owned handoff delegation in one service flow.
//
// This is the one end-to-end test for the messaging session-scoping work — the
// per-method unit tests in service_test.go and handoff_test.go already cover
// edge cases, so this test focuses on the happy path stitched together.
func TestIntegration_HostAdaptersAndMailboxFlow(t *testing.T) {
	svc, _ := newTestService(t, "file-backend", "file-frontend")
	ctx := context.Background()
	const sessionID = "sess-integration"
	events := &memoryEventStore{}
	handoffs := &fakeHandoffCoordinator{}
	svc.SetEventStore(events)
	svc.SetHandoffCoordinator(handoffs)

	// 1. Host participant -> backend.
	if _, err := svc.SendMessage(ctx, SendInput{
		FromSessionID: sessionID,
		FromAgentID:   testHumanID,
		ToSessionID:   sessionID,
		ToAgentID:     "file-backend",
		Body:          "implement the thing",
	}); err != nil {
		t.Fatalf("SendMessage user->backend: %v", err)
	}

	// 2. Backend -> host participant.
	if _, err := svc.SendMessage(ctx, SendInput{
		FromSessionID: sessionID,
		FromAgentID:   "file-backend",
		ToSessionID:   sessionID,
		ToAgentID:     testHumanID,
		Body:          "done",
	}); err != nil {
		t.Fatalf("SendMessage backend->user: %v", err)
	}

	// 3. Backend requests handoff to frontend.
	handoffID, err := svc.RequestHandoff(ctx, sessionID, "file-backend", "file-frontend", "host:departing")
	if err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if handoffID == "" {
		t.Fatal("empty handoff id")
	}

	// 4. Approve the handoff.
	if err := svc.ApproveHandoff(ctx, handoffID); err != nil {
		t.Fatalf("ApproveHandoff: %v", err)
	}

	// The shared package delegates rather than mutating host session tables.
	if handoffs.request.ToAgentID != "file-frontend" || len(handoffs.approved) != 1 {
		t.Fatalf("handoff delegation = request %+v approved %v", handoffs.request, handoffs.approved)
	}

	// 6. Host participant -> frontend (post-handoff).
	if _, err := svc.SendMessage(ctx, SendInput{
		FromSessionID: sessionID,
		FromAgentID:   testHumanID,
		ToSessionID:   sessionID,
		ToAgentID:     "file-frontend",
		Body:          "now do the UI",
	}); err != nil {
		t.Fatalf("SendMessage user->frontend: %v", err)
	}

	// 7. Frontend inbox should have exactly one message — the post-handoff
	// host prompt. The earlier messages were addressed to backend or the host
	// participant, so none of them should land in the frontend's inbox.
	inbox, err := svc.Inbox(ctx, sessionID, "file-frontend", InboxFilter{}, sessionID, "file-frontend")
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 1 {
		t.Fatalf("frontend inbox len = %d, want 1", len(inbox))
	}
	if inbox[0].Body != "now do the UI" {
		t.Errorf("frontend inbox[0].Body = %q, want %q", inbox[0].Body, "now do the UI")
	}

	// 8. Catch-up returns all 3 messages in chronological order. The store's
	// Recent query has a rowid ASC tiebreak for same-tick inserts, so
	// ordering is deterministic even when CURRENT_TIMESTAMP values collide.
	recent, err := svc.RecentForSession(ctx, sessionID, 10)
	if err != nil {
		t.Fatalf("RecentForSession: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("recent len = %d, want 3", len(recent))
	}
	if recent[0].Body != "implement the thing" {
		t.Errorf("recent[0].Body = %q, want %q", recent[0].Body, "implement the thing")
	}
	if recent[1].Body != "done" {
		t.Errorf("recent[1].Body = %q, want %q", recent[1].Body, "done")
	}
	if recent[2].Body != "now do the UI" {
		t.Errorf("recent[2].Body = %q, want %q", recent[2].Body, "now do the UI")
	}
	eventPage, err := svc.SessionEvents(ctx, sessionID, 10)
	if err != nil {
		t.Fatalf("SessionEvents: %v", err)
	}
	if len(eventPage) != 6 {
		t.Fatalf("event page len = %d, want 6", len(eventPage))
	}
}
