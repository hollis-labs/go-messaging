package mailbox

import (
	"context"
	"testing"
)

// TestIntegration_HandoffFullFlow exercises the full handoff round-trip
// through the Service layer: two agents bound to a session (backend primary,
// frontend inactive), messages flowing both ways, a handoff request +
// approval, primary rebinding, a post-handoff user message, and finally a
// catch-up read that must return every message in chronological order.
//
// This is the one end-to-end test for the messaging session-scoping work — the
// per-method unit tests in service_test.go and handoff_test.go already cover
// edge cases, so this test focuses on the happy path stitched together.
func TestIntegration_HandoffFullFlow(t *testing.T) {
	svc, _, parent := newTestService(t, "file-backend", "file-frontend")
	ctx := context.Background()

	sess := newHandoffTestSession(t, parent)
	if err := parent.EnsureSessionAgent(context.Background(), sess.ID, "file-backend", "default", true); err != nil {
		t.Fatalf("EnsureSessionAgent backend: %v", err)
	}
	if err := parent.EnsureSessionAgent(context.Background(), sess.ID, "file-frontend", "default", false); err != nil {
		t.Fatalf("EnsureSessionAgent frontend: %v", err)
	}

	// 1. User -> backend.
	if _, err := svc.SendMessage(ctx, SendInput{
		FromSessionID: sess.ID,
		FromAgentID:   UserSentinel,
		ToSessionID:   sess.ID,
		ToAgentID:     "file-backend",
		Body:          "implement the thing",
	}); err != nil {
		t.Fatalf("SendMessage user->backend: %v", err)
	}

	// 2. Backend -> user.
	if _, err := svc.SendMessage(ctx, SendInput{
		FromSessionID: sess.ID,
		FromAgentID:   "file-backend",
		ToSessionID:   sess.ID,
		ToAgentID:     UserSentinel,
		Body:          "done",
	}); err != nil {
		t.Fatalf("SendMessage backend->user: %v", err)
	}

	// 3. Backend requests handoff to frontend.
	handoffID, err := svc.RequestHandoff(ctx, sess.ID, "file-backend", "file-frontend", "departing")
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

	// 5. Verify frontend is now primary.
	primary, err := parent.GetSessionPrimaryAgent(context.Background(), sess.ID)
	if err != nil {
		t.Fatalf("GetSessionPrimaryAgent: %v", err)
	}
	if primary.AgentID != "file-frontend" {
		t.Errorf("primary = %q, want file-frontend", primary.AgentID)
	}

	// 6. User -> frontend (post-handoff).
	if _, err := svc.SendMessage(ctx, SendInput{
		FromSessionID: sess.ID,
		FromAgentID:   UserSentinel,
		ToSessionID:   sess.ID,
		ToAgentID:     "file-frontend",
		Body:          "now do the UI",
	}); err != nil {
		t.Fatalf("SendMessage user->frontend: %v", err)
	}

	// 7. Frontend inbox should have exactly one message — the post-handoff
	// user prompt. The earlier messages were addressed to backend or user, so
	// none of them should land in the frontend's inbox.
	inbox, err := svc.Inbox(ctx, sess.ID, "file-frontend", InboxFilter{}, sess.ID, "file-frontend")
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
	recent, err := svc.RecentForSession(ctx, sess.ID, 10)
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
}
