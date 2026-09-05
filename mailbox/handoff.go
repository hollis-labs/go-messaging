package mailbox

import (
	"context"
	"fmt"
)

// HandoffRequest describes a host-coordinated transfer between two agents in
// a session. RequestedBy is opaque host vocabulary; the mailbox package does
// not infer authorization or claim that a human approved a transition.
type HandoffRequest struct {
	SessionID   string
	FromAgentID string
	ToAgentID   string
	RequestedBy string
}

// HandoffCoordinator owns handoff persistence, authorization metadata, and
// any atomic mutation of the host's session/primary-agent model. Keeping that
// transaction behind this interface prevents mailbox from imposing table
// names, status columns, agent modes, or approver semantics on consumers.
type HandoffCoordinator interface {
	Request(ctx context.Context, request HandoffRequest) (string, error)
	Approve(ctx context.Context, handoffID string) error
	Reject(ctx context.Context, handoffID, reason string) error
}

// RequestHandoff validates the addressable participants, then delegates the
// host-owned transition to the configured HandoffCoordinator.
func (svc *Service) RequestHandoff(ctx context.Context, sessionID, fromAgentID, toAgentID, requestedBy string) (string, error) {
	if sessionID == "" {
		return "", fmt.Errorf("%w: session_id required", ErrValidation)
	}
	if toAgentID == "" {
		return "", fmt.Errorf("%w: to_agent_id required", ErrValidation)
	}
	if err := ValidateAgentID(ctx, svc.resolver, toAgentID); err != nil {
		return "", fmt.Errorf("%w: to_agent_id: %w", ErrValidation, err)
	}
	if fromAgentID != "" {
		if err := ValidateAgentID(ctx, svc.resolver, fromAgentID); err != nil {
			return "", fmt.Errorf("%w: from_agent_id: %w", ErrValidation, err)
		}
	}
	if requestedBy == "" {
		return "", fmt.Errorf("%w: requested_by required", ErrValidation)
	}
	coordinator := svc.snapshotHooks().handoffs
	if coordinator == nil {
		return "", fmt.Errorf("%w: handoff coordinator", ErrNotConfigured)
	}
	return coordinator.Request(ctx, HandoffRequest{
		SessionID:   sessionID,
		FromAgentID: fromAgentID,
		ToAgentID:   toAgentID,
		RequestedBy: requestedBy,
	})
}

// ApproveHandoff delegates approval to the host-owned coordinator. The host
// supplies caller authorization and audit identity; this package records no
// synthetic "approved by user" claim.
func (svc *Service) ApproveHandoff(ctx context.Context, handoffID string) error {
	if handoffID == "" {
		return fmt.Errorf("%w: handoff_id required", ErrValidation)
	}
	coordinator := svc.snapshotHooks().handoffs
	if coordinator == nil {
		return fmt.Errorf("%w: handoff coordinator", ErrNotConfigured)
	}
	return coordinator.Approve(ctx, handoffID)
}

// RejectHandoff delegates rejection to the host-owned coordinator.
func (svc *Service) RejectHandoff(ctx context.Context, handoffID, reason string) error {
	if handoffID == "" {
		return fmt.Errorf("%w: handoff_id required", ErrValidation)
	}
	coordinator := svc.snapshotHooks().handoffs
	if coordinator == nil {
		return fmt.Errorf("%w: handoff coordinator", ErrNotConfigured)
	}
	return coordinator.Reject(ctx, handoffID, reason)
}
