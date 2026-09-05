package mailbox

// Handoff operations live in the mailbox package because a handoff
// conceptually is the "the primary agent is changing" message-adjacent
// event, but they cross into `session_agents` (session primary flip)
// and `session_handoffs` (their own tracking table) which are not
// part of the messaging Store interface. These methods therefore
// reach through svc.db directly rather than going via svc.store.
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// RequestHandoff creates a pending handoff record binding the
// departing primary agent (fromAgentID) to the proposed incoming
// primary (toAgentID). It does NOT mutate session_agents — the
// binding flip happens in ApproveHandoff, inside a single
// transaction.
//
// fromAgentID may be empty (e.g., an orphaned session where no
// primary currently exists and someone is claiming it). When non-
// empty it is validated against the resolver the same way toAgentID
// is.
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
	switch requestedBy {
	case "departing", "incoming", "user":
	default:
		return "", fmt.Errorf("%w: requested_by must be one of departing|incoming|user", ErrValidation)
	}

	id := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := svc.db.ExecContext(ctx, `
		INSERT INTO session_handoffs (id, session_id, from_agent_id, to_agent_id, requested_by, status, requested_at)
		VALUES (?, ?, ?, ?, ?, 'pending', ?)
	`, id, sessionID, nullableString(fromAgentID), toAgentID, requestedBy, now)
	if err != nil {
		return "", fmt.Errorf("insert handoff: %w", err)
	}
	return id, nil
}

// ApproveHandoff promotes the handoff's toAgentID to primary for the
// session in a single transaction:
//  1. read the handoff row (status must be pending or completed)
//  2. clear is_primary on all session_agents for that session
//  3. upsert the new primary row (or flip an existing one)
//  4. mark this handoff completed
//  5. mark any other pending handoffs for the same session rejected,
//     noting which handoff superseded them
//
// Calling ApproveHandoff on an already-completed handoff is a no-op
// (idempotent). Calling it on a rejected handoff returns an error.
func (svc *Service) ApproveHandoff(ctx context.Context, handoffID string) error {
	if handoffID == "" {
		return fmt.Errorf("%w: handoff_id required", ErrValidation)
	}

	tx, err := svc.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// If the handoff doesn't exist, wrap sql.ErrNoRows with
	// ErrNotFound so the API boundary can return HTTP 404 instead of
	// a generic 500.
	var sessionID, toAgentID, status string
	err = tx.QueryRowContext(ctx, `
		SELECT session_id, to_agent_id, status FROM session_handoffs WHERE id = ?
	`, handoffID).Scan(&sessionID, &toAgentID, &status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: handoff %s", ErrNotFound, handoffID)
		}
		return fmt.Errorf("read handoff: %w", err)
	}
	switch status {
	case "completed":
		// Already done — idempotent no-op. Commit the empty tx so
		// defer Rollback is harmless.
		return tx.Commit()
	case "rejected":
		// Approving an already-rejected handoff is a client-side
		// precondition/conflict error. Wrapping with
		// ErrValidation rather than introducing ErrConflict keeps the
		// sentinel taxonomy minimal; the caller maps this to HTTP
		// 400.
		return fmt.Errorf("%w: handoff %s is rejected", ErrValidation, handoffID)
	case "pending", "approved":
		// ok — proceed
	default:
		return fmt.Errorf("handoff %s has unexpected status %q", handoffID, status)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if _, err := tx.ExecContext(ctx, `
		UPDATE session_agents SET is_primary = 0 WHERE session_id = ? AND is_primary = 1
	`, sessionID); err != nil {
		return fmt.Errorf("clear primary: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO session_agents (session_id, agent_id, mode, joined_at, is_primary)
		VALUES (?, ?, 'default', ?, 1)
		ON CONFLICT(session_id, agent_id) DO UPDATE SET is_primary = 1
	`, sessionID, toAgentID, now); err != nil {
		return fmt.Errorf("set new primary: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE session_handoffs
		   SET status = 'completed', approved_at = ?, approved_by_user = 1
		 WHERE id = ?
	`, now, handoffID); err != nil {
		return fmt.Errorf("update handoff: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE session_handoffs
		   SET status = 'rejected',
		       notes  = 'superseded by handoff ' || ?
		 WHERE session_id = ? AND status = 'pending' AND id != ?
	`, handoffID, sessionID, handoffID); err != nil {
		return fmt.Errorf("reject superseded handoffs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// RejectHandoff marks a pending handoff as rejected with a human-
// readable reason in notes. Rejecting a non-pending handoff is a
// no-op (no rows affected, no error) — the existing status is
// already terminal.
func (svc *Service) RejectHandoff(ctx context.Context, handoffID, reason string) error {
	if handoffID == "" {
		return fmt.Errorf("%w: handoff_id required", ErrValidation)
	}
	_, err := svc.db.ExecContext(ctx, `
		UPDATE session_handoffs
		   SET status = 'rejected', notes = ?
		 WHERE id = ? AND status = 'pending'
	`, reason, handoffID)
	if err != nil {
		return fmt.Errorf("reject handoff: %w", err)
	}
	return nil
}

// getHandoffStatus is a package-private test helper that reads back
// the current status of a handoff by ID. Kept lowercase so it does
// not leak into the public API; tests reach it via the same-package
// compile unit.
func (svc *Service) getHandoffStatus(handoffID string) (string, error) {
	var status string
	err := svc.db.QueryRow(
		`SELECT status FROM session_handoffs WHERE id = ?`, handoffID,
	).Scan(&status)
	if err != nil {
		return "", fmt.Errorf("get handoff status: %w", err)
	}
	return status, nil
}

// nullableString returns nil (→ SQL NULL) for empty strings and the
// string otherwise.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
