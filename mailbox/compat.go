package mailbox

import (
	"context"
	"fmt"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/delivery"
)

// LegacyTupleAddress preserves a historical Nanite-style (session, agent)
// owner tuple inside a canonical messaging address. The session remains the
// primary ID and the agent occupies SubID so old history can be queried without
// inventing a new session identity.
func LegacyTupleAddress(authority, sessionID, agentID string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: authority, ID: sessionID, SubID: agentID}
}

// StableActorAddress names a durable actor independently of any live session.
func StableActorAddress(authority, actorID string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: authority, ID: actorID}
}

// ExactSessionAddress names one concrete session target.
func ExactSessionAddress(authority, sessionID string) messaging.Address {
	return messaging.Address{Kind: messaging.KindSession, Authority: authority, ID: sessionID}
}

// TupleFromAddress reverses LegacyTupleAddress when the address carries a
// preserved tuple. Stable actor addresses intentionally return ok=false.
func TupleFromAddress(a messaging.Address) (sessionID, agentID string, ok bool) {
	if a.Kind != messaging.KindAgent || a.ID == "" || a.SubID == "" {
		return "", "", false
	}
	return a.ID, a.SubID, true
}

// DeliveryRequestFromSendInput projects one mailbox send into the reliable
// delivery core. It does not write to either store; callers choose one
// authoritative storage path and avoid shadow writes.
func DeliveryRequestFromSendInput(input SendInput, authority string) (delivery.EnqueueRequest, error) {
	if authority == "" || input.FromSessionID == "" || input.FromAgentID == "" || input.ToSessionID == "" || input.ToAgentID == "" || input.Body == "" {
		return delivery.EnqueueRequest{}, delivery.ErrInvalidArgument
	}
	from := LegacyTupleAddress(authority, input.FromSessionID, input.FromAgentID)
	to := LegacyTupleAddress(authority, input.ToSessionID, input.ToAgentID)
	kind := messagingKindFromMailbox(input.Kind)
	channel := messaging.Channel(input.Channel)
	metadata := map[string]string{
		"legacy_from_session_id": input.FromSessionID,
		"legacy_from_agent_id":   input.FromAgentID,
		"legacy_to_session_id":   input.ToSessionID,
		"legacy_to_agent_id":     input.ToAgentID,
		"legacy_type":            defaultString(input.Type, TypeMessage),
		"legacy_subject":         input.Subject,
		"legacy_body":            input.Body,
		"legacy_metadata":        defaultString(input.Metadata, "{}"),
		"legacy_priority":        fmt.Sprintf("%d", defaultPriority(input.Priority)),
	}
	payload := []byte(defaultString(input.PayloadJSON, "{}"))
	return delivery.EnqueueRequest{
		From:        from,
		Recipients:  []delivery.RecipientTarget{{Address: to, Binding: delivery.BindingTarget{Address: to, ActorID: input.ToAgentID, SessionID: input.ToSessionID}}},
		Kind:        kind,
		Channel:     channel,
		ThreadID:    input.ThreadID,
		InReplyTo:   input.ReplyTo,
		Payload:     payload,
		ContentType: "application/json",
		Metadata:    metadata,
	}, nil
}

// AttentionState is application-reader state. It is independent of transport
// delivery receipts.
type AttentionState string

const (
	AttentionUnread   AttentionState = AttentionState(StatusUnread)
	AttentionRead     AttentionState = AttentionState(StatusRead)
	AttentionArchived AttentionState = AttentionState(StatusArchived)
	AttentionResolved AttentionState = AttentionState(StatusResolved)
)

// AttentionView is a non-destructive projection of one mailbox message.
type AttentionView struct {
	Message Message
	State   AttentionState
}

// AttentionList returns mailbox attention state without acknowledging transport
// delivery. It delegates to Store.Inbox, whose mailbox contract is already
// non-destructive.
func AttentionList(ctx context.Context, store Store, sessionID, agentID string, filter InboxFilter) ([]AttentionView, error) {
	rows, err := store.Inbox(ctx, sessionID, agentID, filter)
	if err != nil {
		return nil, err
	}
	out := make([]AttentionView, 0, len(rows))
	for _, row := range rows {
		out = append(out, AttentionView{Message: row, State: AttentionState(row.Status)})
	}
	return out, nil
}

// MarkUnread reopens reader attention for a mailbox row. It does not alter any
// delivery attempt or receipt state.
func (s *SQLiteStore) MarkUnread(ctx context.Context, msgID string) error {
	return s.updateStatus(ctx, msgID, StatusUnread, true)
}

// Archive records an application attention decision. Hosts with CHECK
// constraints must admit StatusArchived before using it.
func (s *SQLiteStore) Archive(ctx context.Context, msgID string) error {
	return s.updateStatus(ctx, msgID, StatusArchived, false)
}

func (s *SQLiteStore) updateStatus(ctx context.Context, msgID, status string, clearRead bool) error {
	query := `UPDATE agent_messages SET status = ? WHERE id = ?`
	args := []any{status, msgID}
	if clearRead {
		query = `UPDATE agent_messages SET status = ?, read_at = NULL, resolved_at = NULL WHERE id = ?`
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: message %s", ErrNotFound, msgID)
	}
	return nil
}

func messagingKindFromMailbox(kind string) messaging.Kind {
	switch kind {
	case "", KindNotification:
		return messaging.MsgKindNotice
	case KindRequest:
		return messaging.MsgKindRequest
	case KindReply:
		return messaging.MsgKindResponse
	case KindHandoff:
		return messaging.MsgKindHandoff
	default:
		return messaging.Kind(kind)
	}
}

func defaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func defaultPriority(v int) int {
	if v == 0 {
		return 2
	}
	return v
}
