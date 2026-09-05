package mailbox

// Every messaging mutation produces one or two rows in session_events so
// host replay tooling can reconstruct session activity chronologically.
//
// The mailbox event types are:
//
//	message_sent      — recorded on the sender's session
//	message_received  — recorded on the recipient's session
//	message_acked     — recorded on the recipient's session on ack
//	message_resolved  — recorded on the recipient's session on resolve
//
// Cross-session sends produce two rows (one per session). Same-
// session sends still produce two rows — "you sent" and "you
// received" are distinct events when the same agent ID isn't on
// both ends, and when it is the caller can collapse on read.
import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Session event-type constants. Kept as plain strings rather than a
// typed enum so later event producers (tool calls, user responses)
// can share the column without cross-package dependencies.
const (
	EventMessageSent     = "message_sent"
	EventMessageReceived = "message_received"
	EventMessageAcked    = "message_acked"
	EventMessageResolved = "message_resolved"

	// PTY / CLI turn lifecycle events. Written by the chat-service layer
	// (not the messaging layer) for PTY-provider sessions. These allow
	// session_diagnose to reconstruct what happened in a PTY turn
	// without having to grep logs.
	EventPTYTurnStart    = "pty_turn_start"
	EventPTYTurnComplete = "pty_turn_complete"
	EventPTYTurnFailed   = "pty_turn_failed"

	// Compaction lifecycle events for hosts that record context reduction.
	EventContextPreCompact  = "context_pre_compact"
	EventContextPostCompact = "context_post_compact"

	// EventHarnessTriggeredTurn distinguishes runtime-initiated turns from
	// user-initiated turns in replay and audit streams.
	EventHarnessTriggeredTurn = "harness_triggered_turn"
)

// SessionEvent is a single row in session_events, surfaced for
// replay. Each event's payload lives in EnvelopePointerJSON so
// future event types (tool_call, user_response) can ride the same
// table with different shapes.
type SessionEvent struct {
	ID                  string `json:"id"`
	SessionID           string `json:"session_id"`
	EventType           string `json:"event_type"`
	Channel             string `json:"channel"`
	EnvelopePointerJSON string `json:"envelope_pointer_json"`
	CreatedAt           string `json:"created_at"`
}

// messagePointer is the envelope_pointer_json shape for a messaging
// event. Carries enough context to rehydrate the envelope without
// re-fetching for common cases (unread chip, sidebar summary).
type messagePointer struct {
	MessageID     string `json:"message_id"`
	FromSessionID string `json:"from_session_id"`
	FromAgentID   string `json:"from_agent_id"`
	ToSessionID   string `json:"to_session_id"`
	ToAgentID     string `json:"to_agent_id"`
	ThreadID      string `json:"thread_id"`
	Kind          string `json:"kind,omitempty"`
}

// writeMessageEvent inserts a single session_events row. Failures are
// logged but not propagated — missing an event row is worse than
// failing a successful send, and the messaging row is still the
// authoritative record of the action.
func (svc *Service) writeMessageEvent(ctx context.Context, sessionID, eventType string, m *Message) {
	if svc.db == nil || sessionID == "" || m == nil {
		return
	}
	ptr := messagePointer{
		MessageID:     m.ID,
		FromSessionID: m.FromSessionID,
		FromAgentID:   m.FromAgentID,
		ToSessionID:   m.ToSessionID,
		ToAgentID:     m.ToAgentID,
		ThreadID:      m.ThreadID,
		Kind:          m.Kind,
	}
	payload, err := json.Marshal(ptr)
	if err != nil {
		slog.Warn("messaging: marshal event pointer", "err", err, "message_id", m.ID, "event_type", eventType)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := svc.db.ExecContext(ctx,
		`INSERT INTO session_events (id, session_id, event_type, channel, envelope_pointer_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		uuid.New().String(), sessionID, eventType, m.Channel, string(payload), now,
	); err != nil {
		slog.Warn("messaging: write session event", "err", err,
			"session_id", sessionID, "event_type", eventType, "message_id", m.ID)
	}
}

// writeSendEvents records the bookkeeping for a newly-sent message
// — one row per session involved. The From and To sessions may be
// the same; in that case two rows are still written so "you sent"
// and "the receiving agent got it" stay distinct events in replay.
func (svc *Service) writeSendEvents(ctx context.Context, m *Message) {
	svc.writeMessageEvent(ctx, m.FromSessionID, EventMessageSent, m)
	svc.writeMessageEvent(ctx, m.ToSessionID, EventMessageReceived, m)
}

// WriteSessionEvent inserts a single session_events row with an arbitrary
// event type and payload. This is the public entry point for non-messaging
// producers (e.g., the chat-service PTY turn lifecycle emitter). Callers
// must supply a non-empty sessionID and eventType. payloadJSON may be empty,
// in which case "{}" is stored. Failures are logged but never returned — same
// fire-and-forget contract as writeMessageEvent.
func (svc *Service) WriteSessionEvent(ctx context.Context, sessionID, eventType, channel, payloadJSON string) {
	if svc.db == nil || sessionID == "" || eventType == "" {
		return
	}
	if payloadJSON == "" {
		payloadJSON = "{}"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := svc.db.ExecContext(ctx,
		`INSERT INTO session_events (id, session_id, event_type, channel, envelope_pointer_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		uuid.New().String(), sessionID, eventType, channel, payloadJSON, now,
	); err != nil {
		slog.Warn("messaging: write session event (external)", "err", err,
			"session_id", sessionID, "event_type", eventType)
	}
}

// SessionEvents returns the recent event rows for a session in
// chronological order (oldest first). Hard cap of 500 rows per call
// to bound memory — same defensive pattern as Recent messages.
func (svc *Service) SessionEvents(ctx context.Context, sessionID string, limit int) ([]SessionEvent, error) {
	if svc.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := svc.db.QueryContext(ctx,
		`SELECT id, session_id, event_type, channel, envelope_pointer_json, created_at
		 FROM session_events
		 WHERE session_id = ?
		 ORDER BY created_at ASC, rowid ASC
		 LIMIT ?`, sessionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("session events: %w", err)
	}
	defer func() {
		_ = rows.Close() // Query and iteration errors are surfaced separately; deferred close is cleanup only.
	}()

	out := make([]SessionEvent, 0)
	for rows.Next() {
		var e SessionEvent
		if err := rows.Scan(&e.ID, &e.SessionID, &e.EventType, &e.Channel, &e.EnvelopePointerJSON, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan session event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
