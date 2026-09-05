package mailbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SQLiteStore is the SQLite-backed implementation of Store. It targets
// the host-provisioned `agent_messages` table documented in README.md.
// That table must be a normal SQLite rowid table: rowid is the stable
// insertion-order tiebreaker for messages with equal timestamps.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore constructs a SQLiteStore against the given DB. The
// caller owns the DB handle's lifecycle; SQLiteStore does not close
// it on shutdown.
func NewSQLiteStore(db *sql.DB) *SQLiteStore {
	return &SQLiteStore{db: db}
}

// selectColumns is the canonical SELECT clause for loading Message
// rows. All read queries use this so scanRows can consume them.
const selectColumns = `id, from_session_id, from_agent_id, to_session_id, to_agent_id,
	COALESCE(thread_id,''), COALESCE(reply_to,''),
	type, COALESCE(subject,''), body, metadata, priority, status, channel, kind, payload_json,
	created_at, read_at, resolved_at`

// Send inserts a new message row, populating defaults, and re-fetches
// the authoritative row so callers see server-assigned values.
func (s *SQLiteStore) Send(ctx context.Context, input SendInput) (*Message, error) {
	id := uuid.New().String()

	msgType := input.Type
	if msgType == "" {
		msgType = TypeMessage
	}
	metadata := input.Metadata
	if metadata == "" {
		metadata = "{}"
	}
	priority := input.Priority
	if priority == 0 {
		priority = 2
	}
	status := StatusUnread
	channel := input.Channel
	if channel == "" {
		channel = ChannelChat
	}
	kind := input.Kind
	if kind == "" {
		kind = KindNotification
	}
	payload := input.PayloadJSON
	if payload == "" {
		payload = "{}"
	}
	threadID := input.ThreadID
	if threadID == "" {
		// Self-thread for top-level messages — a reply adds itself to
		// the parent's thread id explicitly.
		threadID = id
	}
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO agent_messages (id, from_session_id, from_agent_id,
		                           to_session_id, to_agent_id,
		                           thread_id, reply_to, type,
		                           subject, body, metadata, priority, status, channel, kind, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		input.FromSessionID, input.FromAgentID,
		input.ToSessionID, input.ToAgentID,
		nullIfEmpty(threadID), nullIfEmpty(input.ReplyTo),
		msgType, nullIfEmpty(input.Subject), input.Body,
		metadata, priority, status, channel, kind, payload, now,
	)
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}
	return s.Get(ctx, id)
}

// Get returns a single message by ID.
func (s *SQLiteStore) Get(ctx context.Context, msgID string) (*Message, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+selectColumns+`
		 FROM agent_messages WHERE id = ?`, msgID,
	)
	var m Message
	if err := row.Scan(
		&m.ID,
		&m.FromSessionID, &m.FromAgentID, &m.ToSessionID, &m.ToAgentID,
		&m.ThreadID, &m.ReplyTo,
		&m.Type, &m.Subject, &m.Body, &m.Metadata, &m.Priority, &m.Status, &m.Channel, &m.Kind, &m.PayloadJSON,
		&m.CreatedAt, &m.ReadAt, &m.ResolvedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: message %s", ErrNotFound, msgID)
		}
		return nil, fmt.Errorf("get message: %w", err)
	}
	return &m, nil
}

// Inbox returns messages addressed to (sessionID, agentID), narrowed
// by any non-empty fields on the InboxFilter. The WHERE clause is
// composed dynamically from the filter so the query planner picks the
// right index regardless of which dimensions the caller filtered on.
func (s *SQLiteStore) Inbox(ctx context.Context, sessionID, agentID string, filter InboxFilter) ([]Message, error) {
	where := []string{"to_session_id = ?", "to_agent_id = ?"}
	args := []any{sessionID, agentID}
	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}
	if filter.Channel != "" {
		where = append(where, "channel = ?")
		args = append(args, filter.Channel)
	}
	if filter.Kind != "" {
		where = append(where, "kind = ?")
		args = append(args, filter.Kind)
	}

	// Send timestamps have second precision for compatibility with existing
	// host rows, so bursts routinely share created_at. rowid is the explicit
	// insertion-order tiebreaker; without it a valid host index may reorder
	// equal-timestamp messages and violate Store.Inbox's FIFO contract.
	query := `SELECT ` + selectColumns + ` FROM agent_messages WHERE ` +
		strings.Join(where, " AND ") + ` ORDER BY priority DESC, created_at ASC, rowid ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	defer func() {
		_ = rows.Close() // Query and iteration errors are surfaced separately; deferred close is cleanup only.
	}()

	return scanRows(rows)
}

// Thread returns all messages in a thread, ordered chronologically.
// Tiebreak on rowid so same-tick inserts remain deterministic.
func (s *SQLiteStore) Thread(ctx context.Context, threadID string) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectColumns+`
		 FROM agent_messages WHERE thread_id = ?
		 ORDER BY created_at ASC, rowid ASC`, threadID,
	)
	if err != nil {
		return nil, fmt.Errorf("thread: %w", err)
	}
	defer func() {
		_ = rows.Close() // Query and iteration errors are surfaced separately; deferred close is cleanup only.
	}()

	return scanRows(rows)
}

// Recent returns up to limit recent messages involving sessionID (as
// sender or receiver), ordered chronologically (oldest first). A non-
// positive limit defaults to DefaultRecentLimit (20); limits above
// MaxRecentLimit are clamped.
func (s *SQLiteStore) Recent(ctx context.Context, sessionID string, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = DefaultRecentLimit
	}
	if limit > MaxRecentLimit {
		limit = MaxRecentLimit
	}
	// Tiebreak on rowid so sub-second bursts remain deterministic —
	// multiple inserts in the same RFC3339 second share created_at.
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectColumns+`
		 FROM agent_messages
		 WHERE from_session_id = ? OR to_session_id = ?
		 ORDER BY created_at DESC, rowid DESC
		 LIMIT ?`, sessionID, sessionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("recent: %w", err)
	}
	defer func() {
		_ = rows.Close() // Query and iteration errors are surfaced separately; deferred close is cleanup only.
	}()

	msgs, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	// Reverse to chronological order (oldest first).
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs, nil
}

// Ack marks a message as read.
func (s *SQLiteStore) Ack(ctx context.Context, msgID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.db.ExecContext(ctx,
		`UPDATE agent_messages SET status = 'read', read_at = ? WHERE id = ?`,
		now, msgID,
	)
	if err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: message %s", ErrNotFound, msgID)
	}
	return nil
}

// Resolve marks a message as resolved.
func (s *SQLiteStore) Resolve(ctx context.Context, msgID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.db.ExecContext(ctx,
		`UPDATE agent_messages SET status = 'resolved', resolved_at = ? WHERE id = ?`,
		now, msgID,
	)
	if err != nil {
		return fmt.Errorf("resolve message: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: message %s", ErrNotFound, msgID)
	}
	return nil
}

// UnreadCount returns the count of unread messages for (sessionID,
// agentID).
func (s *SQLiteStore) UnreadCount(ctx context.Context, sessionID, agentID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_messages
		 WHERE to_session_id = ? AND to_agent_id = ? AND status = 'unread'`,
		sessionID, agentID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("unread count: %w", err)
	}
	return count, nil
}

// DB returns the underlying *sql.DB. It is exposed for host composition
// where mailbox and session mutations must share a transaction boundary.
func (s *SQLiteStore) DB() *sql.DB {
	return s.db
}

// nullIfEmpty returns a SQL NULL for empty strings and the string
// otherwise, so nullable TEXT columns stay truly NULL rather than
// holding empty strings that confuse downstream readers.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func scanRows(rows *sql.Rows) ([]Message, error) {
	out := make([]Message, 0)
	for rows.Next() {
		var m Message
		if err := rows.Scan(
			&m.ID,
			&m.FromSessionID, &m.FromAgentID, &m.ToSessionID, &m.ToAgentID,
			&m.ThreadID, &m.ReplyTo,
			&m.Type, &m.Subject, &m.Body, &m.Metadata, &m.Priority, &m.Status, &m.Channel, &m.Kind, &m.PayloadJSON,
			&m.CreatedAt, &m.ReadAt, &m.ResolvedAt,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
