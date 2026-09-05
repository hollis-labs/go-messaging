package mailbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Message-event types describe mailbox mutations only. Runtime, prompt,
// compaction, and provider lifecycle vocabularies belong to the host that
// produces them.
const (
	EventMessageSent     = "message_sent"
	EventMessageReceived = "message_received"
	EventMessageAcked    = "message_acked"
	EventMessageResolved = "message_resolved"
)

const (
	defaultSessionEventLimit = 100
	maxSessionEventLimit     = 500
)

// SessionEvent is a host-persistable record of a mailbox mutation. The
// envelope pointer contains the mailbox message identity and address tuple;
// hosts may store it alongside unrelated event families without teaching this
// package about those families.
type SessionEvent struct {
	ID                  string `json:"id"`
	SessionID           string `json:"session_id"`
	EventType           string `json:"event_type"`
	Channel             string `json:"channel"`
	EnvelopePointerJSON string `json:"envelope_pointer_json"`
	CreatedAt           string `json:"created_at"`
}

// EventStore is the host-owned persistence seam for mailbox events.
//
// Recent must return the most recent limit events for sessionID in
// chronological order (oldest first within that recent page). Implementations
// backed by SQL commonly select DESC with LIMIT and reverse the selected page;
// ASC with LIMIT returns the oldest page and violates this contract.
type EventStore interface {
	Append(ctx context.Context, event SessionEvent) error
	Recent(ctx context.Context, sessionID string, limit int) ([]SessionEvent, error)
}

type messagePointer struct {
	MessageID     string `json:"message_id"`
	FromSessionID string `json:"from_session_id"`
	FromAgentID   string `json:"from_agent_id"`
	ToSessionID   string `json:"to_session_id"`
	ToAgentID     string `json:"to_agent_id"`
	ThreadID      string `json:"thread_id"`
	Kind          string `json:"kind,omitempty"`
}

// writeMessageEvent appends one best-effort mailbox event through the host
// seam. The persisted message remains authoritative, so an event-store failure
// is logged without changing the already-successful mailbox mutation.
func writeMessageEvent(ctx context.Context, events EventStore, sessionID, eventType string, msg *Message) {
	if events == nil || sessionID == "" || msg == nil {
		return
	}
	pointer, err := json.Marshal(messagePointer{
		MessageID:     msg.ID,
		FromSessionID: msg.FromSessionID,
		FromAgentID:   msg.FromAgentID,
		ToSessionID:   msg.ToSessionID,
		ToAgentID:     msg.ToAgentID,
		ThreadID:      msg.ThreadID,
		Kind:          msg.Kind,
	})
	if err != nil {
		slog.Warn("mailbox: marshal event pointer", "err", err, "message_id", msg.ID, "event_type", eventType)
		return
	}
	event := SessionEvent{
		ID:                  uuid.NewString(),
		SessionID:           sessionID,
		EventType:           eventType,
		Channel:             msg.Channel,
		EnvelopePointerJSON: string(pointer),
		CreatedAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := events.Append(ctx, event); err != nil {
		slog.Warn("mailbox: append event", "err", err,
			"session_id", sessionID, "event_type", eventType, "message_id", msg.ID)
	}
}

func writeSendEvents(ctx context.Context, events EventStore, msg *Message) {
	writeMessageEvent(ctx, events, msg.FromSessionID, EventMessageSent, msg)
	writeMessageEvent(ctx, events, msg.ToSessionID, EventMessageReceived, msg)
}

// SessionEvents returns the most recent event page in chronological order.
// The host EventStore owns persistence and must implement the ordering
// contract documented on EventStore.Recent.
func (svc *Service) SessionEvents(ctx context.Context, sessionID string, limit int) ([]SessionEvent, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: session_id required", ErrValidation)
	}
	if limit <= 0 {
		limit = defaultSessionEventLimit
	}
	if limit > maxSessionEventLimit {
		limit = maxSessionEventLimit
	}
	events := svc.snapshotHooks().events
	if events == nil {
		return nil, fmt.Errorf("%w: event store", ErrNotConfigured)
	}
	return events.Recent(ctx, sessionID, limit)
}
