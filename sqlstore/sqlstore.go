package sqlstore

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	messaging "github.com/hollis-labs/go-messaging"
)

//go:embed schema/*.sql
var schemaFS embed.FS

// timeLayout is fixed-width so text comparison orders like time comparison.
const timeLayout = "2006-01-02T15:04:05.000000000Z"

func formatTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(timeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("sqlstore: parse time %q: %w", s, err)
	}
	return t, nil
}

// Schema returns the embedded DDL, one idempotent .sql file per revision, at
// the root of the returned file system. Applications that manage migrations
// themselves can read it instead of calling Migrate.
func Schema() fs.FS {
	sub, err := fs.Sub(schemaFS, "schema")
	if err != nil {
		panic(err) // the embedded directory is fixed at build time
	}
	return sub
}

// Migrate applies the reference schema to db. It is idempotent.
func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("sqlstore: nil database")
	}
	fsys := Schema()
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		for _, stmt := range splitStatements(string(body)) {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("sqlstore: migrate %s: %w", name, err)
			}
		}
	}
	return nil
}

// splitStatements splits on semicolons and drops comment-only lines. The
// embedded DDL contains no semicolons inside literals or comments.
func splitStatements(script string) []string {
	var out []string
	for _, part := range strings.Split(script, ";") {
		var kept []string
		for _, line := range strings.Split(part, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			kept = append(kept, line)
		}
		if stmt := strings.TrimSpace(strings.Join(kept, "\n")); stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

// Store is a SQLite-backed messaging.Store. Subscribe is an in-process
// fan-out registry: live envelopes are pushed to subscribers when Send
// commits, and history is served from the database.
type Store struct {
	db *sql.DB

	mu          sync.Mutex
	subscribers []*subscription
}

var _ messaging.Store = (*Store)(nil)

type subscription struct {
	toURN  string
	ch     chan messaging.Envelope
	filter messaging.Filter
	ctx    context.Context
}

// New returns a Store over db. It does not apply the schema; call Migrate
// first or manage the DDL from Schema yourself.
func New(db *sql.DB) *Store { return &Store{db: db} }

// DB returns the underlying database handle.
func (s *Store) DB() *sql.DB { return s.db }

// Send persists an envelope. Store assigns ID (fresh UUIDv7) and CreatedAt;
// caller-set values for those fields are overwritten. DeliveredAt and
// ConsumedAt MUST be nil, otherwise ErrPresetLifecycle is returned.
func (s *Store) Send(ctx context.Context, env messaging.Envelope) (messaging.Envelope, error) {
	if env.DeliveredAt != nil || env.ConsumedAt != nil {
		return messaging.Envelope{}, messaging.ErrPresetLifecycle
	}
	id, err := uuid.NewV7()
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("uuid v7: %w", err)
	}
	env.ID = id.String()
	env.CreatedAt = time.Now().UTC()

	metaJSON, err := encodeMetadata(env.Metadata)
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("encode metadata: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
        INSERT INTO messages (
            id, kind, channel, thread_id, in_reply_to,
            from_kind, from_authority, from_id, from_subid, from_urn,
            to_kind,   to_authority,   to_id,   to_subid,   to_urn,
            payload, content_type, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		env.ID, string(env.Kind), string(env.Channel), env.ThreadID, env.InReplyTo,
		string(env.From.Kind), env.From.Authority, env.From.ID, env.From.SubID, env.From.URN(),
		string(env.To.Kind), env.To.Authority, env.To.ID, env.To.SubID, env.To.URN(),
		[]byte(env.Payload), env.ContentType, metaJSON, formatTime(env.CreatedAt),
	)
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("insert message: %w", err)
	}

	s.fanOut(env)
	return env, nil
}

// Get retrieves a single envelope by ID. Returns ErrNotFound if absent. A
// canceled envelope is still returned.
func (s *Store) Get(ctx context.Context, id string) (messaging.Envelope, error) {
	row := s.db.QueryRowContext(ctx, baseSelect+` WHERE m.id = ?`, id)
	env, err := scanEnvelope(row)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.Envelope{}, messaging.ErrNotFound
	}
	if err != nil {
		return messaging.Envelope{}, err
	}
	return env, nil
}

// Inbox returns undelivered, non-canceled envelopes for `to`,
// chronologically, and atomically marks them DeliveredAt=now for `to`.
//
// The claim is one INSERT ... SELECT ... ON CONFLICT DO NOTHING RETURNING
// statement that opens the transaction, so concurrent Inbox calls (and a
// concurrent Consume) serialise on SQLite's writer lock and each envelope is
// claimed exactly once. No BEGIN IMMEDIATE and no MaxOpenConns=1 are needed.
func (s *Store) Inbox(ctx context.Context, to messaging.Address, f messaging.Filter) ([]messaging.Envelope, error) {
	toURN := to.URN()
	now := time.Now().UTC()

	q := `
        INSERT INTO message_deliveries (message_id, recipient_urn, delivered_at)
        SELECT m.id, ?, ?
          FROM messages m
         WHERE m.to_urn = ?
           AND m.canceled_at IS NULL
           AND NOT EXISTS (SELECT 1 FROM message_deliveries d
                            WHERE d.message_id = m.id AND d.recipient_urn = ?)`
	args := []any{toURN, formatTime(now), toURN, toURN}
	q, args = applyFilter(q, args, f)
	q += ` ORDER BY m.created_at ASC, m.id ASC`
	if f.Limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, f.Limit)
	}
	q += ` ON CONFLICT (message_id, recipient_urn) DO NOTHING RETURNING message_id`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin inbox: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ids, err := claimIDs(ctx, tx, q, args)
	if err != nil {
		return nil, err
	}
	envs, err := loadByIDs(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit inbox: %w", err)
	}
	for i := range envs {
		t := now
		envs[i].DeliveredAt = &t
	}
	return envs, nil
}

func claimIDs(ctx context.Context, tx *sql.Tx, q string, args []any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("claim inbox: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan claimed id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claim: %w", err)
	}
	return ids, nil
}

// loadByIDs loads the envelopes with the given IDs in (created_at, id) order.
func loadByIDs(ctx context.Context, tx *sql.Tx, ids []string) ([]messaging.Envelope, error) {
	const chunk = 500
	var envs []messaging.Envelope
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		part := ids[start:end]
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		rows, err := tx.QueryContext(ctx, baseSelect+` WHERE m.id IN (`+placeholders(len(part))+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("load claimed: %w", err)
		}
		for rows.Next() {
			env, err := scanEnvelope(rows)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan claimed row: %w", err)
			}
			envs = append(envs, env)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, fmt.Errorf("iterate claimed: %w", err)
		}
	}
	sort.Slice(envs, func(i, j int) bool {
		if !envs[i].CreatedAt.Equal(envs[j].CreatedAt) {
			return envs[i].CreatedAt.Before(envs[j].CreatedAt)
		}
		return envs[i].ID < envs[j].ID
	})
	return envs, nil
}

// Thread returns envelopes sharing a ThreadID, chronological. Read-only.
func (s *Store) Thread(ctx context.Context, threadID string, f messaging.Filter) ([]messaging.Envelope, error) {
	q := baseSelect + ` WHERE m.thread_id = ?`
	args := []any{threadID}
	q, args = applyFilter(q, args, f)
	q += ` ORDER BY m.created_at ASC, m.id ASC`
	if f.Limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, f.Limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query thread: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var envs []messaging.Envelope
	for rows.Next() {
		env, err := scanEnvelope(rows)
		if err != nil {
			return nil, fmt.Errorf("scan thread row: %w", err)
		}
		envs = append(envs, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread: %w", err)
	}
	return envs, nil
}

// Consume advances ConsumedAt for (envelope, recipient). It is idempotent:
// a repeated call keeps the first ConsumedAt. If the envelope has not been
// delivered to `recipient`, Consume creates the delivery row with
// delivered_at = now, so it works as a stand-alone acknowledgement without a
// prior Inbox. Returns ErrNotFound for an unknown envelope.
//
// Consume is a single upsert statement. That is deliberate: a separate
// UPDATE followed by a fallback INSERT loses a race against a concurrent
// Inbox that creates the same (message, recipient) row in between, and
// surfaces the resulting primary-key violation instead of succeeding.
func (s *Store) Consume(ctx context.Context, id string, recipient messaging.Address) error {
	now := formatTime(time.Now())
	res, err := s.db.ExecContext(ctx, `
        INSERT INTO message_deliveries (message_id, recipient_urn, delivered_at, consumed_at)
        SELECT m.id, ?, ?, ? FROM messages m WHERE m.id = ?
        ON CONFLICT (message_id, recipient_urn) DO UPDATE
           SET consumed_at = COALESCE(message_deliveries.consumed_at, excluded.consumed_at)`,
		recipient.URN(), now, now, id)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	if n == 0 {
		return messaging.ErrNotFound
	}
	return nil
}

// Cancel marks an envelope as dead. Idempotent. Returns ErrNotFound only
// when the envelope ID has never existed.
func (s *Store) Cancel(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE messages SET canceled_at = COALESCE(canceled_at, ?) WHERE id = ?`,
		formatTime(time.Now()), id)
	if err != nil {
		return fmt.Errorf("cancel: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cancel: %w", err)
	}
	if n == 0 {
		return messaging.ErrNotFound
	}
	return nil
}

// Subscribe streams newly-created envelopes for `to` matching the filter,
// sent through this Store value. The channel closes when ctx is done. It is
// live-only; use Inbox or Thread for history. The zero Address subscribes to
// every recipient.
func (s *Store) Subscribe(ctx context.Context, to messaging.Address, f messaging.Filter) (<-chan messaging.Envelope, error) {
	toURN := ""
	if to != (messaging.Address{}) {
		toURN = to.URN()
	}
	sub := &subscription{
		toURN:  toURN,
		ch:     make(chan messaging.Envelope, 16),
		filter: f,
		ctx:    ctx,
	}
	s.mu.Lock()
	s.subscribers = append(s.subscribers, sub)
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		for i, sv := range s.subscribers {
			if sv == sub {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				break
			}
		}
		close(sub.ch)
		s.mu.Unlock()
	}()

	return sub.ch, nil
}

func (s *Store) fanOut(env messaging.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	envURN := env.To.URN()
	// Sends are nonblocking, and the registry lock is held until they finish
	// so a canceled subscriber cannot be closed under a stale copy.
	for _, sub := range s.subscribers {
		if sub.toURN != "" && sub.toURN != envURN {
			continue
		}
		if !sub.filter.Matches(env) {
			continue
		}
		select {
		case sub.ch <- env:
		case <-sub.ctx.Done():
		default:
			// buffer full: drop. Inbox is the durable path.
		}
	}
}

// baseSelect surfaces the latest per-recipient delivered_at / consumed_at
// across an envelope's recipients. The root Store treats an envelope as
// single-recipient, so MAX equals "this envelope's lifecycle".
const baseSelect = `
    SELECT m.id, m.kind, m.channel, m.thread_id, m.in_reply_to,
           m.from_kind, m.from_authority, m.from_id, m.from_subid,
           m.to_kind,   m.to_authority,   m.to_id,   m.to_subid,
           m.payload, m.content_type, m.metadata_json, m.created_at,
           (SELECT MAX(d.delivered_at) FROM message_deliveries d WHERE d.message_id = m.id),
           (SELECT MAX(d.consumed_at)  FROM message_deliveries d WHERE d.message_id = m.id)
      FROM messages m`

func applyFilter(q string, args []any, f messaging.Filter) (string, []any) {
	if len(f.Kind) > 0 {
		q += ` AND m.kind IN (` + placeholders(len(f.Kind)) + `)`
		for _, k := range f.Kind {
			args = append(args, string(k))
		}
	}
	if len(f.Channel) > 0 {
		q += ` AND m.channel IN (` + placeholders(len(f.Channel)) + `)`
		for _, c := range f.Channel {
			args = append(args, string(c))
		}
	}
	if f.ThreadID != "" {
		q += ` AND m.thread_id = ?`
		args = append(args, f.ThreadID)
	}
	return q, args
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

type scanRow interface {
	Scan(dest ...any) error
}

func scanEnvelope(row scanRow) (messaging.Envelope, error) {
	var (
		env                                   messaging.Envelope
		kind, channel                         string
		fromKind, fromAuth, fromID, fromSubID string
		toKind, toAuth, toID, toSubID         string
		payload                               []byte
		metadataJSON, createdAt               string
		deliveredAt, consumedAt               sql.NullString
	)
	if err := row.Scan(
		&env.ID, &kind, &channel, &env.ThreadID, &env.InReplyTo,
		&fromKind, &fromAuth, &fromID, &fromSubID,
		&toKind, &toAuth, &toID, &toSubID,
		&payload, &env.ContentType, &metadataJSON, &createdAt,
		&deliveredAt, &consumedAt,
	); err != nil {
		return messaging.Envelope{}, err
	}
	env.Kind = messaging.Kind(kind)
	env.Channel = messaging.Channel(channel)
	env.From = messaging.Address{Kind: messaging.AddressKind(fromKind), Authority: fromAuth, ID: fromID, SubID: fromSubID}
	env.To = messaging.Address{Kind: messaging.AddressKind(toKind), Authority: toAuth, ID: toID, SubID: toSubID}
	if len(payload) > 0 {
		env.Payload = json.RawMessage(payload)
	}
	if metadataJSON != "" && metadataJSON != "{}" {
		md, err := decodeMetadata(metadataJSON)
		if err != nil {
			return messaging.Envelope{}, fmt.Errorf("decode metadata: %w", err)
		}
		env.Metadata = md
	}
	var err error
	if env.CreatedAt, err = parseTime(createdAt); err != nil {
		return messaging.Envelope{}, err
	}
	if deliveredAt.Valid {
		t, err := parseTime(deliveredAt.String)
		if err != nil {
			return messaging.Envelope{}, err
		}
		env.DeliveredAt = &t
	}
	if consumedAt.Valid {
		t, err := parseTime(consumedAt.String)
		if err != nil {
			return messaging.Envelope{}, err
		}
		env.ConsumedAt = &t
	}
	return env, nil
}

func encodeMetadata(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeMetadata(s string) (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}
