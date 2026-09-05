package mailbox

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type fakeHandoffCoordinator struct {
	mu       sync.Mutex
	request  HandoffRequest
	approved []string
	rejected [][2]string
	err      error
}

func (f *fakeHandoffCoordinator) Request(_ context.Context, request HandoffRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.request = request
	return "handoff-1", f.err
}

func (f *fakeHandoffCoordinator) Approve(_ context.Context, handoffID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.approved = append(f.approved, handoffID)
	return f.err
}

func (f *fakeHandoffCoordinator) Reject(_ context.Context, handoffID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejected = append(f.rejected, [2]string{handoffID, reason})
	return f.err
}

func TestHandoff_DelegatesHostOwnedWorkflow(t *testing.T) {
	svc, _ := newTestService(t, "file-backend", "file-frontend")
	coordinator := &fakeHandoffCoordinator{}
	svc.SetHandoffCoordinator(coordinator)

	id, err := svc.RequestHandoff(context.Background(), "sess-1", "file-backend", "file-frontend", "host:operator/42")
	if err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if id != "handoff-1" {
		t.Fatalf("handoff id = %q, want handoff-1", id)
	}
	want := HandoffRequest{
		SessionID: "sess-1", FromAgentID: "file-backend",
		ToAgentID: "file-frontend", RequestedBy: "host:operator/42",
	}
	if !reflect.DeepEqual(coordinator.request, want) {
		t.Errorf("request = %+v, want %+v", coordinator.request, want)
	}
	if err := svc.ApproveHandoff(context.Background(), id); err != nil {
		t.Fatalf("ApproveHandoff: %v", err)
	}
	if err := svc.RejectHandoff(context.Background(), id, "superseded"); err != nil {
		t.Fatalf("RejectHandoff: %v", err)
	}
	if !reflect.DeepEqual(coordinator.approved, []string{id}) {
		t.Errorf("approved = %v", coordinator.approved)
	}
	if !reflect.DeepEqual(coordinator.rejected, [][2]string{{id, "superseded"}}) {
		t.Errorf("rejected = %v", coordinator.rejected)
	}
}

func TestHandoff_OrphanClaimDelegatesEmptyFromAgent(t *testing.T) {
	svc, _ := newTestService(t, "file-frontend")
	coordinator := &fakeHandoffCoordinator{}
	svc.SetHandoffCoordinator(coordinator)
	if _, err := svc.RequestHandoff(context.Background(), "sess-1", "", "file-frontend", "host-policy"); err != nil {
		t.Fatalf("RequestHandoff: %v", err)
	}
	if coordinator.request.FromAgentID != "" {
		t.Errorf("from agent = %q, want empty", coordinator.request.FromAgentID)
	}
}

func TestHandoff_RequiresCoordinator(t *testing.T) {
	svc, _ := newTestService(t, "file-a", "file-b")
	if _, err := svc.RequestHandoff(context.Background(), "sess-1", "file-a", "file-b", "host"); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("RequestHandoff error = %v, want ErrNotConfigured", err)
	}
	if err := svc.ApproveHandoff(context.Background(), "handoff-1"); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("ApproveHandoff error = %v, want ErrNotConfigured", err)
	}
	if err := svc.RejectHandoff(context.Background(), "handoff-1", "reason"); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("RejectHandoff error = %v, want ErrNotConfigured", err)
	}
}

func TestHandoff_ValidatesBeforeDelegation(t *testing.T) {
	svc, _ := newTestService(t, "file-a", "file-b")
	coordinator := &fakeHandoffCoordinator{}
	svc.SetHandoffCoordinator(coordinator)
	tests := []struct {
		name, sessionID, fromID, toID, requestedBy string
	}{
		{"empty session", "", "file-a", "file-b", "host"},
		{"empty recipient", "sess-1", "file-a", "", "host"},
		{"unknown sender", "sess-1", "missing", "file-b", "host"},
		{"unknown recipient", "sess-1", "file-a", "missing", "host"},
		{"empty requester", "sess-1", "file-a", "file-b", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RequestHandoff(context.Background(), tc.sessionID, tc.fromID, tc.toID, tc.requestedBy)
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("error = %v, want ErrValidation", err)
			}
		})
	}
	if coordinator.request != (HandoffRequest{}) {
		t.Errorf("coordinator called for invalid input: %+v", coordinator.request)
	}
}

func TestHandoff_PropagatesCoordinatorErrors(t *testing.T) {
	svc, _ := newTestService(t, "file-a", "file-b")
	wantErr := errors.New("host rejected transition")
	coordinator := &fakeHandoffCoordinator{err: wantErr}
	svc.SetHandoffCoordinator(coordinator)
	if _, err := svc.RequestHandoff(context.Background(), "sess-1", "file-a", "file-b", "host"); !errors.Is(err, wantErr) {
		t.Fatalf("RequestHandoff error = %v, want host error", err)
	}
}
