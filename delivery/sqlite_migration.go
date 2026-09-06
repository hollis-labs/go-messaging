package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
)

const legacyMailboxAmbiguousReason = "legacy mailbox delivery state ambiguous; authorize redrive to replay"

// LegacyMailboxPolicy controls how unread legacy mailbox rows are imported.
type LegacyMailboxPolicy string

const (
	// LegacyMailboxHoldAmbiguousUnread is the safe default. Unread mailbox rows
	// preserve history and ownership but are dead-lettered until a host explicitly
	// authorizes redrive; mailbox attention state is not treated as delivery proof.
	LegacyMailboxHoldAmbiguousUnread LegacyMailboxPolicy = "hold_ambiguous_unread"
	// LegacyMailboxReplayUnread is an explicit host opt-in to make unread legacy
	// rows pending delivery obligations after import.
	LegacyMailboxReplayUnread LegacyMailboxPolicy = "replay_unread"
)

// LegacyMailboxMigrationOptions configures import from the historical
// agent_messages table. Authority scopes tuple-derived messaging addresses.
type LegacyMailboxMigrationOptions struct {
	Authority string
	Policy    LegacyMailboxPolicy
	Now       time.Time
}

// LegacyMailboxMigrationResult reports idempotent import outcomes.
type LegacyMailboxMigrationResult struct {
	Imported  int
	Skipped   int
	Pending   int
	Delivered int
	Held      int
}

// MigrateLegacyMailbox imports rows from the legacy mailbox agent_messages table
// into the durable delivery schema. It preserves IDs, thread/reply references,
// tuple ownership, and historical read/resolved timestamps in metadata and an
// audit table. The safe default never blindly replays unread rows.
func MigrateLegacyMailbox(ctx context.Context, db *sql.DB, opts LegacyMailboxMigrationOptions) (LegacyMailboxMigrationResult, error) {
	if db == nil {
		return LegacyMailboxMigrationResult{}, ErrInvalidArgument
	}
	if opts.Authority == "" {
		opts.Authority = "legacy"
	}
	if opts.Policy == "" {
		opts.Policy = LegacyMailboxHoldAmbiguousUnread
	}
	if opts.Policy != LegacyMailboxHoldAmbiguousUnread && opts.Policy != LegacyMailboxReplayUnread {
		return LegacyMailboxMigrationResult{}, ErrInvalidArgument
	}
	now := opts.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return LegacyMailboxMigrationResult{}, err
	}
	defer rollback(tx)
	if err := applySQLiteSchemaTx(ctx, tx); err != nil {
		return LegacyMailboxMigrationResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS messaging_legacy_mailbox_imports (
		legacy_message_id TEXT PRIMARY KEY,
		message_id TEXT NOT NULL,
		delivery_id TEXT NOT NULL,
		legacy_status TEXT NOT NULL,
		read_at TEXT,
		resolved_at TEXT,
		from_session_id TEXT NOT NULL,
		from_agent_id TEXT NOT NULL,
		to_session_id TEXT NOT NULL,
		to_agent_id TEXT NOT NULL,
		policy TEXT NOT NULL,
		imported_at TEXT NOT NULL
	)`); err != nil {
		return LegacyMailboxMigrationResult{}, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT id, from_session_id, from_agent_id, to_session_id, to_agent_id,
		COALESCE(thread_id,''), COALESCE(reply_to,''), type, COALESCE(subject,''), body,
		COALESCE(metadata,'{}'), priority, status, channel, kind, payload_json, created_at, read_at, resolved_at
		FROM agent_messages ORDER BY created_at ASC, rowid ASC`)
	if err != nil {
		return LegacyMailboxMigrationResult{}, fmt.Errorf("read legacy mailbox: %w", err)
	}
	defer rows.Close()

	var result LegacyMailboxMigrationResult
	for rows.Next() {
		row, err := scanLegacyMailboxRow(rows)
		if err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		exists, err := sqliteMessageExists(ctx, tx, MessageID(row.ID))
		if err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		if exists {
			result.Skipped++
			continue
		}
		msg, delivery, err := legacyMailboxDelivery(row, opts, now)
		if err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		if err := sqliteInsertMessage(ctx, tx, msg); err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		if err := sqliteInsertDelivery(ctx, tx, delivery); err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		if err := sqliteInsertReceipt(ctx, tx, Receipt{MessageID: msg.ID, DeliveryID: delivery.ID, Stage: StagePersisted, At: msg.CreatedAt, Detail: "legacy mailbox import"}); err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO messaging_legacy_mailbox_imports(legacy_message_id, message_id, delivery_id, legacy_status, read_at, resolved_at, from_session_id, from_agent_id, to_session_id, to_agent_id, policy, imported_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, row.ID, msg.ID, delivery.ID, row.Status, row.ReadAt, row.ResolvedAt, row.FromSessionID, row.FromAgentID, row.ToSessionID, row.ToAgentID, opts.Policy, timeString(now)); err != nil {
			return LegacyMailboxMigrationResult{}, err
		}
		result.Imported++
		switch delivery.Status {
		case DeliveryPending:
			result.Pending++
		case DeliveryDelivered:
			result.Delivered++
		case DeliveryDeadLettered:
			result.Held++
		}
	}
	if err := rows.Err(); err != nil {
		return LegacyMailboxMigrationResult{}, err
	}
	return result, tx.Commit()
}

type legacyMailboxRow struct {
	ID            string
	FromSessionID string
	FromAgentID   string
	ToSessionID   string
	ToAgentID     string
	ThreadID      string
	ReplyTo       string
	Type          string
	Subject       string
	Body          string
	Metadata      string
	Priority      int
	Status        string
	Channel       string
	Kind          string
	PayloadJSON   string
	CreatedAt     string
	ReadAt        sql.NullString
	ResolvedAt    sql.NullString
}

func scanLegacyMailboxRow(rows *sql.Rows) (legacyMailboxRow, error) {
	var r legacyMailboxRow
	if err := rows.Scan(&r.ID, &r.FromSessionID, &r.FromAgentID, &r.ToSessionID, &r.ToAgentID, &r.ThreadID, &r.ReplyTo, &r.Type, &r.Subject, &r.Body, &r.Metadata, &r.Priority, &r.Status, &r.Channel, &r.Kind, &r.PayloadJSON, &r.CreatedAt, &r.ReadAt, &r.ResolvedAt); err != nil {
		return legacyMailboxRow{}, err
	}
	return r, nil
}

func legacyMailboxDelivery(r legacyMailboxRow, opts LegacyMailboxMigrationOptions, now time.Time) (Message, RecipientDelivery, error) {
	createdAt, err := parseLegacyTime(r.CreatedAt, now)
	if err != nil {
		return Message{}, RecipientDelivery{}, err
	}
	from := messaging.Address{Kind: messaging.KindAgent, Authority: opts.Authority, ID: r.FromSessionID, SubID: r.FromAgentID}
	to := messaging.Address{Kind: messaging.KindAgent, Authority: opts.Authority, ID: r.ToSessionID, SubID: r.ToAgentID}
	metadata := map[string]string{}
	_ = json.Unmarshal([]byte(r.Metadata), &metadata)
	metadata["legacy_mailbox_id"] = r.ID
	metadata["legacy_type"] = r.Type
	metadata["legacy_status"] = r.Status
	metadata["legacy_priority"] = fmt.Sprintf("%d", r.Priority)
	metadata["legacy_from_session_id"] = r.FromSessionID
	metadata["legacy_from_agent_id"] = r.FromAgentID
	metadata["legacy_to_session_id"] = r.ToSessionID
	metadata["legacy_to_agent_id"] = r.ToAgentID
	metadata["legacy_subject"] = r.Subject
	metadata["legacy_body"] = r.Body
	if r.ReadAt.Valid {
		metadata["legacy_read_at"] = r.ReadAt.String
	}
	if r.ResolvedAt.Valid {
		metadata["legacy_resolved_at"] = r.ResolvedAt.String
	}
	payload := []byte(r.PayloadJSON)
	contentType := "application/json"
	if len(payload) == 0 {
		payload = []byte(r.Body)
		contentType = "text/plain"
	}
	msgReq := EnqueueRequest{From: from, Recipients: []RecipientTarget{{Address: to, Binding: BindingTarget{Address: to, ActorID: r.ToAgentID, SessionID: r.ToSessionID}}}, Kind: messaging.Kind(r.Kind), Channel: messaging.Channel(r.Channel), ThreadID: r.ThreadID, InReplyTo: r.ReplyTo, Payload: payload, ContentType: contentType, Metadata: metadata}
	digest, err := canonicalDigest(msgReq)
	if err != nil {
		return Message{}, RecipientDelivery{}, err
	}
	msg := Message{ID: MessageID(r.ID), Digest: digest, From: from, Kind: msgReq.Kind, Channel: msgReq.Channel, ThreadID: r.ThreadID, InReplyTo: r.ReplyTo, Payload: payload, ContentType: contentType, Metadata: metadata, CreatedAt: createdAt}
	d := RecipientDelivery{ID: DeliveryID(r.ID), MessageID: msg.ID, Recipient: to, Binding: BindingTarget{Address: to, ActorID: r.ToAgentID, SessionID: r.ToSessionID}, Status: DeliveryDeadLettered, DeadLetterReason: legacyMailboxAmbiguousReason, CreatedAt: createdAt, UpdatedAt: now}
	switch r.Status {
	case "read", "acknowledged", "resolved":
		d.Status = DeliveryDelivered
		d.DeadLetterReason = ""
	case "unread":
		if opts.Policy == LegacyMailboxReplayUnread {
			d.Status = DeliveryPending
			d.DeadLetterReason = ""
		}
	}
	return msg, d, nil
}

func sqliteMessageExists(ctx context.Context, q sqliteQueryable, id MessageID) (bool, error) {
	var v int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM messaging_messages WHERE id = ?`, id).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func parseLegacyTime(raw string, fallback time.Time) (time.Time, error) {
	if raw == "" {
		return fallback.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("parse legacy time %q: %w", raw, ErrInvalidArgument)
}
