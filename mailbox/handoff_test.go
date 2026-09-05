package mailbox

import (
	"context"
	"strings"
	"testing"
)

// newHandoffTestSession inserts a real sessions row so that the
// session_handoffs(session_id) foreign key is satisfied. The session gets a
// unique short_code via the store's auto-generation, so multiple calls in a
// single test are safe.
func newHandoffTestSession(t *testing.T, s *testDB) *testSession {
	t.Helper()
	return s.createSession(t)
}

func TestHandoff_FullFlow(t *testing.T) {
	svc, _, parent := newTestService(t, "file-backend", "file-frontend")

	sess := newHandoffTestSession(t, parent)
	if err := parent.EnsureSessionAgent(context.Background(), sess.ID, "file-backend", "default", true); err != nil {
		t.Fatalf("EnsureSessionAgent: %v", err)
	}

	handoffID, err := svc.RequestHandoff(context.Background(), sess.ID, "file-backend", "file-frontend", "departing")
	if err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if handoffID == "" {
		t.Error("empty handoff id")
	}

	if err := svc.ApproveHandoff(context.Background(), handoffID); err != nil {
		t.Fatalf("ApproveHandoff: %v", err)
	}

	// Stronger assertion: verify exactly one session_agents row is primary
	// after the handoff, and that it's file-frontend. This catches the case
	// where the "clear primary" UPDATE silently fails to demote the old
	// primary — GetSessionPrimaryAgent alone can't see that bug because it
	// would still return one of the two matching rows.
	agents, err := parent.ListSessionAgents(context.Background(), sess.ID)
	if err != nil {
		t.Fatalf("ListSessionAgents: %v", err)
	}
	primaryCount := 0
	for _, a := range agents {
		if a.IsPrimary {
			primaryCount++
			if a.AgentID != "file-frontend" {
				t.Errorf("primary = %q, want file-frontend", a.AgentID)
			}
		}
	}
	if primaryCount != 1 {
		t.Errorf("expected exactly 1 primary agent, got %d", primaryCount)
	}
}

func TestHandoff_DoubleRequest(t *testing.T) {
	svc, _, parent := newTestService(t, "file-a", "file-b", "file-c")

	sess := newHandoffTestSession(t, parent)
	if err := parent.EnsureSessionAgent(context.Background(), sess.ID, "file-a", "default", true); err != nil {
		t.Fatalf("EnsureSessionAgent: %v", err)
	}

	h1, err := svc.RequestHandoff(context.Background(), sess.ID, "file-a", "file-b", "departing")
	if err != nil {
		t.Fatalf("RequestHandoff h1: %v", err)
	}
	h2, err := svc.RequestHandoff(context.Background(), sess.ID, "file-a", "file-c", "departing")
	if err != nil {
		t.Fatalf("RequestHandoff h2: %v", err)
	}

	if err := svc.ApproveHandoff(context.Background(), h1); err != nil {
		t.Fatalf("ApproveHandoff h1: %v", err)
	}

	status, err := svc.getHandoffStatus(h2)
	if err != nil {
		t.Fatalf("getHandoffStatus h2: %v", err)
	}
	if status != "rejected" {
		t.Errorf("h2 status = %q, want rejected", status)
	}
}

func TestHandoff_ApproveCompleted_Idempotent(t *testing.T) {
	svc, _, parent := newTestService(t, "file-a", "file-b")

	sess := newHandoffTestSession(t, parent)
	if err := parent.EnsureSessionAgent(context.Background(), sess.ID, "file-a", "default", true); err != nil {
		t.Fatalf("EnsureSessionAgent: %v", err)
	}

	h, err := svc.RequestHandoff(context.Background(), sess.ID, "file-a", "file-b", "departing")
	if err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if err := svc.ApproveHandoff(context.Background(), h); err != nil {
		t.Fatalf("ApproveHandoff first: %v", err)
	}
	if err := svc.ApproveHandoff(context.Background(), h); err != nil {
		t.Errorf("second approve errored: %v", err)
	}
}

func TestHandoff_ApproveRejected_Errors(t *testing.T) {
	svc, _, parent := newTestService(t, "file-a", "file-b")

	sess := newHandoffTestSession(t, parent)
	if err := parent.EnsureSessionAgent(context.Background(), sess.ID, "file-a", "default", true); err != nil {
		t.Fatalf("EnsureSessionAgent: %v", err)
	}

	h, err := svc.RequestHandoff(context.Background(), sess.ID, "file-a", "file-b", "departing")
	if err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if err := svc.RejectHandoff(context.Background(), h, "test"); err != nil {
		t.Fatalf("RejectHandoff: %v", err)
	}
	err = svc.ApproveHandoff(context.Background(), h)
	if err == nil {
		t.Fatal("expected error approving rejected handoff")
	}
	if !strings.Contains(err.Error(), "rejected") {
		t.Errorf("expected error to mention rejected, got: %v", err)
	}
}

func TestHandoff_OrphanClaim(t *testing.T) {
	// Orphan-claim flow: a session with no primary hand off "from nobody"
	// to a new primary. The empty fromAgentID short-circuits the from-side
	// validation and gets stored as SQL NULL.
	svc, _, parent := newTestService(t, "file-frontend")
	sess := newHandoffTestSession(t, parent)
	// NOTE: deliberately no EnsureSessionAgent pre-seed — the session has no current primary.

	handoffID, err := svc.RequestHandoff(context.Background(), sess.ID, "", "file-frontend", "user")
	if err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if handoffID == "" {
		t.Fatal("empty handoff id")
	}

	if err := svc.ApproveHandoff(context.Background(), handoffID); err != nil {
		t.Fatalf("ApproveHandoff: %v", err)
	}

	// Verify frontend is now primary.
	primary, err := parent.GetSessionPrimaryAgent(context.Background(), sess.ID)
	if err != nil {
		t.Fatalf("GetSessionPrimaryAgent: %v", err)
	}
	if primary.AgentID != "file-frontend" {
		t.Errorf("primary = %q, want file-frontend", primary.AgentID)
	}
}
