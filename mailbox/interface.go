// Package mailbox provides an optional durable, tuple-addressed mailbox
// service for applications whose delivery model is unread/read/resolved.
//
// It intentionally complements rather than changes the root go-messaging
// Envelope contract. The root package models portable address URNs and
// delivered/consumed dispatch. Mailbox models application inboxes addressed
// by (session ID, agent ID), including acknowledgement, resolution, recent
// history, session events, handoffs, and live in-process fan-out. Conflating
// those lifecycle models would make the root Store contract ambiguous.
//
// SQLiteStore and the optional DB-backed Service features operate on tables
// provisioned by the host application. See README.md in this directory for
// the required schema contract. Agent identity, auto-registration policy,
// notification delivery, wake behavior, and goroutine lifecycle remain
// host-owned behind narrow interfaces.
package mailbox

import "context"

// Store is the persistence interface for durable mailbox messages.
// Implementations persist Message rows keyed by ID, thread, and the
// (session, agent) address tuples.
type Store interface {
	// Send persists a new message. Defaults are populated for missing
	// ID (UUID), Type ("message"), Metadata ("{}"), Priority (2),
	// Status ("unread"), ThreadID (message self-thread), and CreatedAt.
	Send(ctx context.Context, input SendInput) (*Message, error)

	// Get returns a single message by ID. Missing rows wrap ErrNotFound.
	Get(ctx context.Context, msgID string) (*Message, error)

	// Inbox returns messages addressed to (sessionID, agentID), optionally
	// filtered. Results are priority-descending and FIFO within a priority.
	Inbox(ctx context.Context, sessionID, agentID string, filter InboxFilter) ([]Message, error)

	// Thread returns all messages in a thread chronologically.
	Thread(ctx context.Context, threadID string) ([]Message, error)

	// Recent returns up to limit messages touching a session, oldest first.
	Recent(ctx context.Context, sessionID string, limit int) ([]Message, error)

	// Ack marks a message read. Missing rows wrap ErrNotFound.
	Ack(ctx context.Context, msgID string) error

	// Resolve marks a message resolved. Missing rows wrap ErrNotFound.
	Resolve(ctx context.Context, msgID string) error

	// UnreadCount counts unread messages for an address tuple.
	UnreadCount(ctx context.Context, sessionID, agentID string) (int, error)
}
