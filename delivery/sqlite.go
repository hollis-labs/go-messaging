package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	messaging "github.com/hollis-labs/go-messaging"
)

const SQLiteSchemaVersion = 1

// SQLiteMutationStep names durable transaction boundaries. Hooks are intended
// for tests that prove a partially failed transaction leaves no committed
// delivery state.
type SQLiteMutationStep string

const (
	SQLiteStepAfterMessageInsert     SQLiteMutationStep = "after_message_insert"
	SQLiteStepAfterIdempotencyInsert SQLiteMutationStep = "after_idempotency_insert"
	SQLiteStepAfterDeliveryInsert    SQLiteMutationStep = "after_delivery_insert"
	SQLiteStepAfterLeaseUpdate       SQLiteMutationStep = "after_lease_update"
	SQLiteStepAfterAttemptInsert     SQLiteMutationStep = "after_attempt_insert"
	SQLiteStepAfterReceiptInsert     SQLiteMutationStep = "after_receipt_insert"
)

type SQLiteOption func(*SQLiteStore)

// WithSQLiteClock injects deterministic time. Nil clocks are ignored.
func WithSQLiteClock(clock Clock) SQLiteOption {
	return func(s *SQLiteStore) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithSQLiteMutationHook installs a transaction-boundary hook. Returning an
// error aborts the surrounding store operation before commit.
func WithSQLiteMutationHook(hook func(SQLiteMutationStep) error) SQLiteOption {
	return func(s *SQLiteStore) { s.hook = hook }
}

// SQLiteStore is a durable Store implementation over a host-owned SQLite
// database. The caller owns schema rollout, PRAGMA policy, pooling, and Close.
type SQLiteStore struct {
	db    *sql.DB
	clock Clock
	hook  func(SQLiteMutationStep) error
}

// NewSQLiteStore constructs a delivery store over db. The caller retains
// lifecycle ownership; SQLiteStore never closes or globally configures db.
func NewSQLiteStore(db *sql.DB, opts ...SQLiteOption) *SQLiteStore {
	s := &SQLiteStore{db: db, clock: realClock{}}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ Store = (*SQLiteStore)(nil)

// DB returns the underlying DB for host composition and lifecycle ownership.
func (s *SQLiteStore) DB() *sql.DB { return s.db }

// ApplySQLiteSchema creates or upgrades the vNext delivery tables. It only
// mutates the caller-provided test/host database handle.
func ApplySQLiteSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrInvalidArgument
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := applySQLiteSchemaTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func applySQLiteSchemaTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS messaging_delivery_schema (
			component TEXT PRIMARY KEY,
			version INTEGER NOT NULL,
			applied_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS messaging_messages (
			id TEXT PRIMARY KEY,
			digest TEXT NOT NULL,
			idempotency_key TEXT NOT NULL DEFAULT '',
			from_urn TEXT NOT NULL,
			group_urn TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL,
			channel TEXT NOT NULL DEFAULT '',
			thread_id TEXT NOT NULL DEFAULT '',
			in_reply_to TEXT NOT NULL DEFAULT '',
			payload BLOB,
			content_type TEXT NOT NULL DEFAULT '',
			metadata_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS messaging_deliveries (
			id TEXT PRIMARY KEY,
			message_id TEXT NOT NULL REFERENCES messaging_messages(id) ON DELETE CASCADE,
			recipient_urn TEXT NOT NULL,
			binding_json TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			active_attempt_id TEXT NOT NULL DEFAULT '',
			active_lease_token TEXT NOT NULL DEFAULT '',
			lease_holder TEXT NOT NULL DEFAULT '',
			lease_expires_at TEXT,
			next_attempt_at TEXT,
			deadline_at TEXT,
			dead_letter_reason TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS messaging_attempts (
			id TEXT PRIMARY KEY,
			message_id TEXT NOT NULL REFERENCES messaging_messages(id) ON DELETE CASCADE,
			delivery_id TEXT NOT NULL REFERENCES messaging_deliveries(id) ON DELETE CASCADE,
			lease_token TEXT NOT NULL,
			holder TEXT NOT NULL,
			binding_json TEXT NOT NULL DEFAULT '{}',
			binding_generation INTEGER NOT NULL DEFAULT 0,
			acquired_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			stage TEXT NOT NULL,
			host_accepted_at TEXT,
			turn_submitted_at TEXT,
			consumed_at TEXT,
			failed_at TEXT,
			error TEXT NOT NULL DEFAULT '',
			retryable INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS messaging_receipts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id TEXT NOT NULL,
			delivery_id TEXT NOT NULL REFERENCES messaging_deliveries(id) ON DELETE CASCADE,
			attempt_id TEXT NOT NULL DEFAULT '',
			stage TEXT NOT NULL,
			at TEXT NOT NULL,
			detail TEXT NOT NULL DEFAULT '',
			UNIQUE(delivery_id, attempt_id, stage)
		)`,
		`CREATE TABLE IF NOT EXISTS messaging_idempotency (
			scope TEXT PRIMARY KEY,
			sender_urn TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			message_id TEXT NOT NULL REFERENCES messaging_messages(id) ON DELETE CASCADE,
			digest TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messaging_deliveries_message ON messaging_deliveries(message_id, id)`,
		`CREATE INDEX IF NOT EXISTS idx_messaging_deliveries_recipient_status_ready ON messaging_deliveries(recipient_urn, status, next_attempt_at, deadline_at)`,
		`CREATE INDEX IF NOT EXISTS idx_messaging_deliveries_active_lease ON messaging_deliveries(status, lease_expires_at)`,
		`CREATE INDEX IF NOT EXISTS idx_messaging_attempts_delivery ON messaging_attempts(delivery_id, acquired_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_messaging_receipts_delivery ON messaging_receipts(delivery_id, id)`,
		`INSERT INTO messaging_delivery_schema(component, version, applied_at)
		 VALUES ('delivery', ?, ?)
		 ON CONFLICT(component) DO UPDATE SET version=excluded.version, applied_at=excluded.applied_at`,
	}
	for _, stmt := range stmts {
		var err error
		if strings.Contains(stmt, "VALUES ('delivery', ?, ?)") {
			_, err = tx.ExecContext(ctx, stmt, SQLiteSchemaVersion, time.Now().UTC().Format(time.RFC3339Nano))
		} else {
			_, err = tx.ExecContext(ctx, stmt)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if req.From.IsZero() || len(req.Recipients) == 0 || req.Kind == "" {
		return EnqueueResult{}, ErrInvalidArgument
	}
	for _, r := range req.Recipients {
		if r.Address.IsZero() {
			return EnqueueResult{}, ErrInvalidArgument
		}
	}
	digest, err := canonicalDigest(req)
	if err != nil {
		return EnqueueResult{}, err
	}
	scope := idempotencyScope(req.From, req.IdempotencyKey)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EnqueueResult{}, err
	}
	defer rollback(tx)

	if req.IdempotencyKey != "" {
		existing, ok, err := sqliteIdempotency(ctx, tx, scope)
		if err != nil {
			return EnqueueResult{}, err
		}
		if ok {
			if existing.digest != digest {
				return EnqueueResult{}, &DigestConflictError{Sender: req.From, IdempotencyKey: req.IdempotencyKey, ExistingMessageID: existing.messageID, ExistingDigest: existing.digest, NewDigest: digest}
			}
			msg, err := sqliteGetMessage(ctx, tx, existing.messageID)
			if err != nil {
				return EnqueueResult{}, err
			}
			dels, err := sqliteDeliveriesForMessage(ctx, tx, existing.messageID)
			if err != nil {
				return EnqueueResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return EnqueueResult{}, err
			}
			return EnqueueResult{Message: msg, Deliveries: dels, Duplicate: true}, nil
		}
	}

	now := s.now()
	msg := Message{ID: MessageID(newID()), Digest: digest, IdempotencyKey: req.IdempotencyKey, From: req.From, Group: req.Group, Kind: req.Kind, Channel: req.Channel, ThreadID: req.ThreadID, InReplyTo: req.InReplyTo, Payload: cloneBytes(req.Payload), ContentType: req.ContentType, Metadata: cloneMap(req.Metadata), CreatedAt: now}
	if err := sqliteInsertMessage(ctx, tx, msg); err != nil {
		return EnqueueResult{}, err
	}
	if err := s.step(SQLiteStepAfterMessageInsert); err != nil {
		return EnqueueResult{}, err
	}
	if req.IdempotencyKey != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO messaging_idempotency(scope, sender_urn, idempotency_key, message_id, digest) VALUES (?, ?, ?, ?, ?)`, scope, req.From.URN(), req.IdempotencyKey, msg.ID, digest); err != nil {
			return EnqueueResult{}, err
		}
		if err := s.step(SQLiteStepAfterIdempotencyInsert); err != nil {
			return EnqueueResult{}, err
		}
	}
	deliveries := make([]RecipientDelivery, 0, len(req.Recipients))
	for _, target := range req.Recipients {
		binding := target.Binding
		if binding.Address.IsZero() {
			binding.Address = target.Address
		}
		d := RecipientDelivery{ID: DeliveryID(newID()), MessageID: msg.ID, Recipient: target.Address, Binding: binding, Status: DeliveryPending, DeadlineAt: req.DeadlineAt.UTC(), CreatedAt: now, UpdatedAt: now}
		if err := sqliteInsertDelivery(ctx, tx, d); err != nil {
			return EnqueueResult{}, err
		}
		if err := s.step(SQLiteStepAfterDeliveryInsert); err != nil {
			return EnqueueResult{}, err
		}
		if err := sqliteInsertReceipt(ctx, tx, Receipt{MessageID: msg.ID, DeliveryID: d.ID, Stage: StagePersisted, At: now}); err != nil {
			return EnqueueResult{}, err
		}
		deliveries = append(deliveries, cloneDelivery(d))
	}
	if err := tx.Commit(); err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Message: cloneMessage(msg), Deliveries: deliveries}, nil
}

func (s *SQLiteStore) GetMessage(ctx context.Context, id MessageID) (Message, error) {
	return sqliteGetMessage(ctx, s.db, id)
}

func (s *SQLiteStore) GetDelivery(ctx context.Context, id DeliveryID) (RecipientDelivery, error) {
	if err := s.releaseExpired(ctx); err != nil {
		return RecipientDelivery{}, err
	}
	return sqliteGetDelivery(ctx, s.db, id)
}

func (s *SQLiteStore) ListDeliveries(ctx context.Context, f Filter) ([]RecipientDelivery, error) {
	if err := s.releaseExpired(ctx); err != nil {
		return nil, err
	}
	where := []string{"1=1"}
	args := []any{}
	if f.MessageID != "" {
		where = append(where, "message_id = ?")
		args = append(args, f.MessageID)
	}
	if !f.Recipient.IsZero() {
		where = append(where, "recipient_urn = ?")
		args = append(args, f.Recipient.URN())
	}
	if len(f.Status) > 0 {
		marks := make([]string, len(f.Status))
		for i, st := range f.Status {
			marks[i] = "?"
			args = append(args, st)
		}
		where = append(where, "status IN ("+strings.Join(marks, ",")+")")
	}
	if f.ReadyOnly {
		now := s.now()
		where = append(where, "status NOT IN (?, ?, ?)")
		args = append(args, DeliveryDelivered, DeliveryDeadLettered, DeliveryCanceled)
		where = append(where, "status != ?")
		args = append(args, DeliveryLeased)
		where = append(where, "(next_attempt_at IS NULL OR next_attempt_at <= ?)")
		args = append(args, timeString(now))
		where = append(where, "(deadline_at IS NULL OR deadline_at > ?)")
		args = append(args, timeString(now))
	}
	query := `SELECT ` + sqliteDeliveryColumns + ` FROM messaging_deliveries WHERE ` + strings.Join(where, " AND ") + ` ORDER BY id ASC`
	if f.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, f.Limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqliteScanDeliveries(rows)
}

func (s *SQLiteStore) Claim(ctx context.Context, req ClaimRequest) (ClaimResult, error) {
	if req.Holder == "" || req.LeaseDuration <= 0 {
		return ClaimResult{}, ErrInvalidArgument
	}
	if err := s.releaseExpired(ctx); err != nil {
		return ClaimResult{}, sqliteClaimError(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ClaimResult{}, sqliteClaimError(err)
	}
	defer rollback(tx)
	now := s.now()
	id, err := sqliteSelectDelivery(ctx, tx, now, req)
	if err != nil {
		return ClaimResult{}, sqliteClaimError(err)
	}
	d, err := sqliteGetDelivery(ctx, tx, id)
	if err != nil {
		return ClaimResult{}, sqliteClaimError(err)
	}
	if terminal(d.Status) {
		return ClaimResult{}, ErrTerminalDelivery
	}
	if deadlineExpired(now, d) {
		d, err = sqliteDeadLetter(ctx, tx, d, "deadline exceeded", now)
		if err != nil {
			return ClaimResult{}, err
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return ClaimResult{}, commitErr
		}
		return ClaimResult{}, ErrDeadlineExceeded
	}
	if d.Status == DeliveryLeased && d.ActiveLeaseToken != "" && now.Before(d.LeaseExpiresAt) {
		return ClaimResult{}, ErrAlreadyClaimed
	}
	if !d.NextAttemptAt.IsZero() && now.Before(d.NextAttemptAt) {
		return ClaimResult{}, ErrNoDeliveryReady
	}

	attemptID := AttemptID(newID())
	token := LeaseToken(newID())
	binding := d.Binding
	if binding.BindingGeneration == 0 {
		binding.BindingGeneration = req.BindingGeneration
	}
	expires := now.Add(req.LeaseDuration).UTC()
	attempt := Attempt{ID: attemptID, MessageID: d.MessageID, DeliveryID: d.ID, LeaseToken: token, Holder: req.Holder, Binding: binding, BindingGeneration: req.BindingGeneration, AcquiredAt: now, ExpiresAt: expires, Stage: StageLeaseAcquired}
	res, err := tx.ExecContext(ctx, `UPDATE messaging_deliveries
		SET status = ?, attempt_count = attempt_count + 1, active_attempt_id = ?, active_lease_token = ?, lease_holder = ?, lease_expires_at = ?, next_attempt_at = NULL, updated_at = ?
		WHERE id = ? AND status IN (?, ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?) AND (deadline_at IS NULL OR deadline_at > ?)`, DeliveryLeased, attemptID, token, req.Holder, timeString(expires), timeString(now), d.ID, DeliveryPending, DeliveryRetryScheduled, timeString(now), timeString(now))
	if err != nil {
		return ClaimResult{}, sqliteClaimError(err)
	}
	if err := s.step(SQLiteStepAfterLeaseUpdate); err != nil {
		return ClaimResult{}, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ClaimResult{}, ErrAlreadyClaimed
	}
	if err := sqliteInsertAttempt(ctx, tx, attempt); err != nil {
		return ClaimResult{}, err
	}
	if err := s.step(SQLiteStepAfterAttemptInsert); err != nil {
		return ClaimResult{}, err
	}
	if err := sqliteInsertReceipt(ctx, tx, Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: attempt.ID, Stage: StageLeaseAcquired, At: now}); err != nil {
		return ClaimResult{}, err
	}
	if err := s.step(SQLiteStepAfterReceiptInsert); err != nil {
		return ClaimResult{}, err
	}
	d, err = sqliteGetDelivery(ctx, tx, d.ID)
	if err != nil {
		return ClaimResult{}, err
	}
	msg, err := sqliteGetMessage(ctx, tx, d.MessageID)
	if err != nil {
		return ClaimResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{Message: msg, Delivery: d, Attempt: cloneAttempt(attempt)}, nil
}

func (s *SQLiteStore) ExtendLease(ctx context.Context, ref LeaseRef, until time.Time) (Attempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attempt{}, err
	}
	defer rollback(tx)
	d, a, err := s.currentLease(ctx, tx, ref, false)
	if err != nil {
		return Attempt{}, err
	}
	now := s.now()
	until = until.UTC()
	if !until.After(now) || until.Before(a.ExpiresAt) {
		return Attempt{}, ErrInvalidArgument
	}
	a.ExpiresAt = until
	d.LeaseExpiresAt = until
	d.UpdatedAt = now
	if err := sqliteUpdateAttempt(ctx, tx, a); err != nil {
		return Attempt{}, err
	}
	if err := sqliteUpdateDelivery(ctx, tx, d); err != nil {
		return Attempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return Attempt{}, err
	}
	return cloneAttempt(a), nil
}

func (s *SQLiteStore) Ack(ctx context.Context, req AckRequest) (RecipientDelivery, Attempt, error) {
	if req.Stage == "" {
		return RecipientDelivery{}, Attempt{}, ErrInvalidArgument
	}
	if req.Stage != StageHostAccepted && req.Stage != StageTurnSubmitted && req.Stage != StageConsumed {
		return RecipientDelivery{}, Attempt{}, ErrUnsupportedStage
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	defer rollback(tx)
	d, a, err := s.currentLease(ctx, tx, req.Lease, true)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if a.Stage == StageFailed || a.Stage == StageDeadLettered {
		if err := tx.Commit(); err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
		return cloneDelivery(d), cloneAttempt(a), nil
	}
	at := req.At.UTC()
	if at.IsZero() {
		at = s.now()
	}
	if deadlineExpired(at, d) && d.Status != DeliveryDelivered {
		d, err = sqliteDeadLetter(ctx, tx, d, "deadline exceeded", s.now())
		if err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
		a.Stage = StageDeadLettered
		if err := sqliteUpdateAttempt(ctx, tx, a); err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
		if err := tx.Commit(); err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
		return cloneDelivery(d), cloneAttempt(a), ErrDeadlineExceeded
	}
	if stageRank(a.Stage) > stageRank(req.Stage) {
		if err := tx.Commit(); err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
		return cloneDelivery(d), cloneAttempt(a), nil
	}
	switch req.Stage {
	case StageHostAccepted:
		if a.HostAcceptedAt.IsZero() {
			a.HostAcceptedAt = at
		}
	case StageTurnSubmitted:
		if a.HostAcceptedAt.IsZero() {
			a.HostAcceptedAt = at
		}
		if a.TurnSubmittedAt.IsZero() {
			a.TurnSubmittedAt = at
		}
	case StageConsumed:
		if a.HostAcceptedAt.IsZero() {
			a.HostAcceptedAt = at
		}
		if a.TurnSubmittedAt.IsZero() {
			a.TurnSubmittedAt = at
		}
		if a.ConsumedAt.IsZero() {
			a.ConsumedAt = at
		}
		d.Status = DeliveryDelivered
		d.ActiveAttemptID = ""
		d.ActiveLeaseToken = ""
		d.LeaseHolder = ""
		d.LeaseExpiresAt = time.Time{}
	}
	a.Stage = req.Stage
	d.UpdatedAt = at
	if err := sqliteInsertReceipt(ctx, tx, Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: a.ID, Stage: req.Stage, At: at}); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if err := sqliteUpdateAttempt(ctx, tx, a); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if err := sqliteUpdateDelivery(ctx, tx, d); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	return cloneDelivery(d), cloneAttempt(a), nil
}

func (s *SQLiteStore) Nack(ctx context.Context, req NackRequest) (RecipientDelivery, Attempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	defer rollback(tx)
	d, a, err := s.currentLease(ctx, tx, req.Lease, true)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if a.Stage == StageFailed || a.Stage == StageDeadLettered {
		if err := tx.Commit(); err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
		return cloneDelivery(d), cloneAttempt(a), nil
	}
	at := req.At.UTC()
	if at.IsZero() {
		at = s.now()
	}
	a.Stage = StageFailed
	a.FailedAt = at
	a.Error = req.Error
	a.Retryable = req.Retryable
	a.NextAttemptAt = req.NextAttemptAt.UTC()
	d.ActiveAttemptID = ""
	d.ActiveLeaseToken = ""
	d.LeaseHolder = ""
	d.LeaseExpiresAt = time.Time{}
	d.UpdatedAt = at
	if !req.Retryable || deadlineExpired(at, d) || (!req.NextAttemptAt.IsZero() && deadlineExpired(req.NextAttemptAt, d)) {
		d.Status = DeliveryDeadLettered
		d.DeadLetterReason = req.Error
		a.Stage = StageDeadLettered
	} else {
		d.Status = DeliveryRetryScheduled
		if req.NextAttemptAt.IsZero() {
			d.NextAttemptAt = at
		} else {
			d.NextAttemptAt = req.NextAttemptAt.UTC()
		}
	}
	if err := sqliteInsertReceipt(ctx, tx, Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: a.ID, Stage: StageFailed, At: at, Detail: req.Error}); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if a.Stage == StageDeadLettered {
		if err := sqliteInsertReceipt(ctx, tx, Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: a.ID, Stage: StageDeadLettered, At: at, Detail: req.Error}); err != nil {
			return RecipientDelivery{}, Attempt{}, err
		}
	}
	if err := sqliteUpdateAttempt(ctx, tx, a); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if err := sqliteUpdateDelivery(ctx, tx, d); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if d.Status == DeliveryDeadLettered {
		return cloneDelivery(d), cloneAttempt(a), ErrDeadLettered
	}
	return cloneDelivery(d), cloneAttempt(a), nil
}

func (s *SQLiteStore) Redrive(ctx context.Context, req RedriveRequest) (RecipientDelivery, error) {
	if req.AuthorizedBy == "" {
		return RecipientDelivery{}, ErrUnauthorized
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecipientDelivery{}, err
	}
	defer rollback(tx)
	d, err := sqliteGetDelivery(ctx, tx, req.DeliveryID)
	if err != nil {
		return RecipientDelivery{}, err
	}
	if d.Status != DeliveryDeadLettered {
		return RecipientDelivery{}, ErrInvalidArgument
	}
	at := req.At.UTC()
	if at.IsZero() {
		at = s.now()
	}
	d.Status = DeliveryPending
	d.NextAttemptAt = time.Time{}
	d.DeadlineAt = req.NewDeadlineAt.UTC()
	d.DeadLetterReason = ""
	d.UpdatedAt = at
	if err := sqliteUpdateDelivery(ctx, tx, d); err != nil {
		return RecipientDelivery{}, err
	}
	if err := tx.Commit(); err != nil {
		return RecipientDelivery{}, err
	}
	return cloneDelivery(d), nil
}

func (s *SQLiteStore) Attempts(ctx context.Context, deliveryID DeliveryID) ([]Attempt, error) {
	if _, err := sqliteGetDelivery(ctx, s.db, deliveryID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sqliteAttemptColumns+` FROM messaging_attempts WHERE delivery_id = ? ORDER BY acquired_at ASC, id ASC`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqliteScanAttempts(rows)
}

func (s *SQLiteStore) Receipts(ctx context.Context, deliveryID DeliveryID) ([]Receipt, error) {
	if _, err := sqliteGetDelivery(ctx, s.db, deliveryID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT message_id, delivery_id, attempt_id, stage, at, detail FROM messaging_receipts WHERE delivery_id = ? ORDER BY id ASC`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Receipt{}
	for rows.Next() {
		var r Receipt
		var at string
		if err := rows.Scan(&r.MessageID, &r.DeliveryID, &r.AttemptID, &r.Stage, &at, &r.Detail); err != nil {
			return nil, err
		}
		r.At, err = parseTime(at)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) releaseExpired(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := s.now()
	rows, err := tx.QueryContext(ctx, `SELECT active_attempt_id FROM messaging_deliveries WHERE status = ? AND active_attempt_id != '' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`, DeliveryLeased, timeString(now))
	if err != nil {
		return err
	}
	ids := []AttemptID{}
	for rows.Next() {
		var id AttemptID
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE messaging_attempts SET stage = ?, failed_at = ?, error = ?, retryable = 1, next_attempt_at = ? WHERE id = ?`, StageFailed, timeString(now), "lease expired", timeString(now), id); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE messaging_deliveries
		SET status = ?, active_attempt_id = '', active_lease_token = '', lease_holder = '', lease_expires_at = NULL, next_attempt_at = ?, updated_at = ?
		WHERE status = ? AND active_attempt_id != '' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`, DeliveryRetryScheduled, timeString(now), timeString(now), DeliveryLeased, timeString(now))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) currentLease(ctx context.Context, q sqliteQueryable, ref LeaseRef, allowCompletedIDempotent bool) (RecipientDelivery, Attempt, error) {
	d, err := sqliteGetDelivery(ctx, q, ref.DeliveryID)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	a, err := sqliteGetAttempt(ctx, q, ref.AttemptID)
	if err != nil || a.DeliveryID != ref.DeliveryID || a.LeaseToken != ref.LeaseToken {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	if ref.BindingGeneration != a.BindingGeneration {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	if allowCompletedIdempotent(allowCompletedIDempotent, d, a, ref) {
		return d, a, nil
	}
	if terminal(d.Status) {
		return RecipientDelivery{}, Attempt{}, ErrTerminalDelivery
	}
	if d.ActiveAttemptID != ref.AttemptID || d.ActiveLeaseToken != ref.LeaseToken {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	if !s.now().Before(a.ExpiresAt) {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	return d, a, nil
}

func allowCompletedIdempotent(allow bool, d RecipientDelivery, a Attempt, ref LeaseRef) bool {
	if !allow {
		return false
	}
	if a.Stage != StageFailed && a.Stage != StageDeadLettered && a.Stage != StageConsumed {
		return false
	}
	return d.ActiveAttemptID == "" || d.ActiveAttemptID == ref.AttemptID
}

func (s *SQLiteStore) now() time.Time { return s.clock.Now().UTC() }

func (s *SQLiteStore) step(step SQLiteMutationStep) error {
	if s.hook == nil {
		return nil
	}
	return s.hook(step)
}

type sqliteQueryable interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const sqliteMessageColumns = `id, digest, idempotency_key, from_urn, group_urn, kind, channel, thread_id, in_reply_to, payload, content_type, metadata_json, created_at`
const sqliteDeliveryColumns = `id, message_id, recipient_urn, binding_json, status, attempt_count, active_attempt_id, active_lease_token, lease_holder, lease_expires_at, next_attempt_at, deadline_at, dead_letter_reason, created_at, updated_at`
const sqliteAttemptColumns = `id, message_id, delivery_id, lease_token, holder, binding_json, binding_generation, acquired_at, expires_at, stage, host_accepted_at, turn_submitted_at, consumed_at, failed_at, error, retryable, next_attempt_at`

func sqliteIdempotency(ctx context.Context, q sqliteQueryable, scope string) (idempotencyRecord, bool, error) {
	var rec idempotencyRecord
	err := q.QueryRowContext(ctx, `SELECT message_id, digest FROM messaging_idempotency WHERE scope = ?`, scope).Scan(&rec.messageID, &rec.digest)
	if errors.Is(err, sql.ErrNoRows) {
		return idempotencyRecord{}, false, nil
	}
	if err != nil {
		return idempotencyRecord{}, false, err
	}
	return rec, true, nil
}

func sqliteInsertMessage(ctx context.Context, q sqliteQueryable, m Message) error {
	metadata, err := json.Marshal(m.Metadata)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO messaging_messages(id, digest, idempotency_key, from_urn, group_urn, kind, channel, thread_id, in_reply_to, payload, content_type, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, m.ID, m.Digest, m.IdempotencyKey, m.From.URN(), urnOrEmpty(m.Group), m.Kind, m.Channel, m.ThreadID, m.InReplyTo, []byte(m.Payload), m.ContentType, string(metadata), timeString(m.CreatedAt))
	return err
}

func sqliteInsertDelivery(ctx context.Context, q sqliteQueryable, d RecipientDelivery) error {
	binding, err := json.Marshal(d.Binding)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO messaging_deliveries(id, message_id, recipient_urn, binding_json, status, attempt_count, active_attempt_id, active_lease_token, lease_holder, lease_expires_at, next_attempt_at, deadline_at, dead_letter_reason, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, d.ID, d.MessageID, d.Recipient.URN(), string(binding), d.Status, d.AttemptCount, d.ActiveAttemptID, d.ActiveLeaseToken, d.LeaseHolder, nullableTime(d.LeaseExpiresAt), nullableTime(d.NextAttemptAt), nullableTime(d.DeadlineAt), d.DeadLetterReason, timeString(d.CreatedAt), timeString(d.UpdatedAt))
	return err
}

func sqliteInsertAttempt(ctx context.Context, q sqliteQueryable, a Attempt) error {
	binding, err := json.Marshal(a.Binding)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO messaging_attempts(id, message_id, delivery_id, lease_token, holder, binding_json, binding_generation, acquired_at, expires_at, stage, host_accepted_at, turn_submitted_at, consumed_at, failed_at, error, retryable, next_attempt_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, a.ID, a.MessageID, a.DeliveryID, a.LeaseToken, a.Holder, string(binding), a.BindingGeneration, timeString(a.AcquiredAt), timeString(a.ExpiresAt), a.Stage, nullableTime(a.HostAcceptedAt), nullableTime(a.TurnSubmittedAt), nullableTime(a.ConsumedAt), nullableTime(a.FailedAt), a.Error, boolInt(a.Retryable), nullableTime(a.NextAttemptAt))
	return err
}

func sqliteInsertReceipt(ctx context.Context, q sqliteQueryable, r Receipt) error {
	_, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO messaging_receipts(message_id, delivery_id, attempt_id, stage, at, detail) VALUES (?, ?, ?, ?, ?, ?)`, r.MessageID, r.DeliveryID, r.AttemptID, r.Stage, timeString(r.At), r.Detail)
	return err
}

func sqliteUpdateDelivery(ctx context.Context, q sqliteQueryable, d RecipientDelivery) error {
	binding, err := json.Marshal(d.Binding)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE messaging_deliveries SET message_id=?, recipient_urn=?, binding_json=?, status=?, attempt_count=?, active_attempt_id=?, active_lease_token=?, lease_holder=?, lease_expires_at=?, next_attempt_at=?, deadline_at=?, dead_letter_reason=?, created_at=?, updated_at=? WHERE id=?`, d.MessageID, d.Recipient.URN(), string(binding), d.Status, d.AttemptCount, d.ActiveAttemptID, d.ActiveLeaseToken, d.LeaseHolder, nullableTime(d.LeaseExpiresAt), nullableTime(d.NextAttemptAt), nullableTime(d.DeadlineAt), d.DeadLetterReason, timeString(d.CreatedAt), timeString(d.UpdatedAt), d.ID)
	return err
}

func sqliteUpdateAttempt(ctx context.Context, q sqliteQueryable, a Attempt) error {
	binding, err := json.Marshal(a.Binding)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE messaging_attempts SET message_id=?, delivery_id=?, lease_token=?, holder=?, binding_json=?, binding_generation=?, acquired_at=?, expires_at=?, stage=?, host_accepted_at=?, turn_submitted_at=?, consumed_at=?, failed_at=?, error=?, retryable=?, next_attempt_at=? WHERE id=?`, a.MessageID, a.DeliveryID, a.LeaseToken, a.Holder, string(binding), a.BindingGeneration, timeString(a.AcquiredAt), timeString(a.ExpiresAt), a.Stage, nullableTime(a.HostAcceptedAt), nullableTime(a.TurnSubmittedAt), nullableTime(a.ConsumedAt), nullableTime(a.FailedAt), a.Error, boolInt(a.Retryable), nullableTime(a.NextAttemptAt), a.ID)
	return err
}

func sqliteGetMessage(ctx context.Context, q sqliteQueryable, id MessageID) (Message, error) {
	row := q.QueryRowContext(ctx, `SELECT `+sqliteMessageColumns+` FROM messaging_messages WHERE id = ?`, id)
	return sqliteScanMessage(row)
}

func sqliteScanMessage(row interface{ Scan(...any) error }) (Message, error) {
	var m Message
	var fromURN, groupURN, createdAt, metadata string
	if err := row.Scan(&m.ID, &m.Digest, &m.IdempotencyKey, &fromURN, &groupURN, &m.Kind, &m.Channel, &m.ThreadID, &m.InReplyTo, &m.Payload, &m.ContentType, &metadata, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Message{}, ErrNotFound
		}
		return Message{}, err
	}
	from, err := messaging.ParseURN(fromURN)
	if err != nil {
		return Message{}, err
	}
	m.From = from
	if groupURN != "" {
		group, err := messaging.ParseURN(groupURN)
		if err != nil {
			return Message{}, err
		}
		m.Group = group
	}
	if err := json.Unmarshal([]byte(metadata), &m.Metadata); err != nil {
		return Message{}, err
	}
	m.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return Message{}, err
	}
	return cloneMessage(m), nil
}

func sqliteGetDelivery(ctx context.Context, q sqliteQueryable, id DeliveryID) (RecipientDelivery, error) {
	row := q.QueryRowContext(ctx, `SELECT `+sqliteDeliveryColumns+` FROM messaging_deliveries WHERE id = ?`, id)
	return sqliteScanDelivery(row)
}

func sqliteScanDeliveries(rows *sql.Rows) ([]RecipientDelivery, error) {
	out := []RecipientDelivery{}
	for rows.Next() {
		d, err := sqliteScanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func sqliteScanDelivery(row interface{ Scan(...any) error }) (RecipientDelivery, error) {
	var d RecipientDelivery
	var recipientURN, bindingJSON, createdAt, updatedAt string
	var leaseExpires, nextAttempt, deadline sql.NullString
	if err := row.Scan(&d.ID, &d.MessageID, &recipientURN, &bindingJSON, &d.Status, &d.AttemptCount, &d.ActiveAttemptID, &d.ActiveLeaseToken, &d.LeaseHolder, &leaseExpires, &nextAttempt, &deadline, &d.DeadLetterReason, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RecipientDelivery{}, ErrNotFound
		}
		return RecipientDelivery{}, err
	}
	recipient, err := messaging.ParseURN(recipientURN)
	if err != nil {
		return RecipientDelivery{}, err
	}
	d.Recipient = recipient
	if err := json.Unmarshal([]byte(bindingJSON), &d.Binding); err != nil {
		return RecipientDelivery{}, err
	}
	if d.Binding.Address.IsZero() {
		d.Binding.Address = recipient
	}
	if d.LeaseExpiresAt, err = parseNullTime(leaseExpires); err != nil {
		return RecipientDelivery{}, err
	}
	if d.NextAttemptAt, err = parseNullTime(nextAttempt); err != nil {
		return RecipientDelivery{}, err
	}
	if d.DeadlineAt, err = parseNullTime(deadline); err != nil {
		return RecipientDelivery{}, err
	}
	if d.CreatedAt, err = parseTime(createdAt); err != nil {
		return RecipientDelivery{}, err
	}
	if d.UpdatedAt, err = parseTime(updatedAt); err != nil {
		return RecipientDelivery{}, err
	}
	return cloneDelivery(d), nil
}

func sqliteGetAttempt(ctx context.Context, q sqliteQueryable, id AttemptID) (Attempt, error) {
	row := q.QueryRowContext(ctx, `SELECT `+sqliteAttemptColumns+` FROM messaging_attempts WHERE id = ?`, id)
	return sqliteScanAttempt(row)
}

func sqliteScanAttempts(rows *sql.Rows) ([]Attempt, error) {
	out := []Attempt{}
	for rows.Next() {
		a, err := sqliteScanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func sqliteScanAttempt(row interface{ Scan(...any) error }) (Attempt, error) {
	var a Attempt
	var bindingJSON, acquiredAt, expiresAt string
	var hostAccepted, turnSubmitted, consumed, failed, nextAttempt sql.NullString
	var retryable int
	if err := row.Scan(&a.ID, &a.MessageID, &a.DeliveryID, &a.LeaseToken, &a.Holder, &bindingJSON, &a.BindingGeneration, &acquiredAt, &expiresAt, &a.Stage, &hostAccepted, &turnSubmitted, &consumed, &failed, &a.Error, &retryable, &nextAttempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Attempt{}, ErrNotFound
		}
		return Attempt{}, err
	}
	if err := json.Unmarshal([]byte(bindingJSON), &a.Binding); err != nil {
		return Attempt{}, err
	}
	var err error
	a.AcquiredAt, err = parseTime(acquiredAt)
	if err != nil {
		return Attempt{}, err
	}
	a.ExpiresAt, err = parseTime(expiresAt)
	if err != nil {
		return Attempt{}, err
	}
	if a.HostAcceptedAt, err = parseNullTime(hostAccepted); err != nil {
		return Attempt{}, err
	}
	if a.TurnSubmittedAt, err = parseNullTime(turnSubmitted); err != nil {
		return Attempt{}, err
	}
	if a.ConsumedAt, err = parseNullTime(consumed); err != nil {
		return Attempt{}, err
	}
	if a.FailedAt, err = parseNullTime(failed); err != nil {
		return Attempt{}, err
	}
	if a.NextAttemptAt, err = parseNullTime(nextAttempt); err != nil {
		return Attempt{}, err
	}
	a.Retryable = retryable != 0
	return cloneAttempt(a), nil
}

func sqliteDeliveriesForMessage(ctx context.Context, q sqliteQueryable, id MessageID) ([]RecipientDelivery, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+sqliteDeliveryColumns+` FROM messaging_deliveries WHERE message_id = ? ORDER BY id ASC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqliteScanDeliveries(rows)
}

func sqliteSelectDelivery(ctx context.Context, q sqliteQueryable, now time.Time, req ClaimRequest) (DeliveryID, error) {
	if req.DeliveryID != "" {
		d, err := sqliteGetDelivery(ctx, q, req.DeliveryID)
		if err != nil {
			return "", err
		}
		if !req.Recipient.IsZero() && d.Recipient.URN() != req.Recipient.URN() {
			return "", ErrNoDeliveryReady
		}
		return req.DeliveryID, nil
	}
	where := []string{"status NOT IN (?, ?, ?)", "status != ?", "(next_attempt_at IS NULL OR next_attempt_at <= ?)", "(deadline_at IS NULL OR deadline_at > ?)"}
	args := []any{DeliveryDelivered, DeliveryDeadLettered, DeliveryCanceled, DeliveryLeased, timeString(now), timeString(now)}
	if !req.Recipient.IsZero() {
		where = append(where, "recipient_urn = ?")
		args = append(args, req.Recipient.URN())
	}
	row := q.QueryRowContext(ctx, `SELECT id FROM messaging_deliveries WHERE `+strings.Join(where, " AND ")+` ORDER BY id ASC LIMIT 1`, args...)
	var id DeliveryID
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNoDeliveryReady
		}
		return "", err
	}
	return id, nil
}

func sqliteDeadLetter(ctx context.Context, q sqliteQueryable, d RecipientDelivery, reason string, at time.Time) (RecipientDelivery, error) {
	d.Status = DeliveryDeadLettered
	d.ActiveAttemptID = ""
	d.ActiveLeaseToken = ""
	d.LeaseHolder = ""
	d.LeaseExpiresAt = time.Time{}
	d.DeadLetterReason = reason
	d.UpdatedAt = at.UTC()
	if err := sqliteUpdateDelivery(ctx, q, d); err != nil {
		return RecipientDelivery{}, err
	}
	return d, nil
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }

func urnOrEmpty(a messaging.Address) string {
	if a.IsZero() {
		return ""
	}
	return a.URN()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return timeString(t)
}

const sqliteTimeFormat = "2006-01-02T15:04:05.000000000Z07:00"

func timeString(t time.Time) string { return t.UTC().Format(sqliteTimeFormat) }

func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(sqliteTimeFormat, s); err == nil {
		return t.UTC(), nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func sqliteClaimError(err error) error {
	if sqliteBusy(err) {
		return ErrAlreadyClaimed
	}
	return err
}

func sqliteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "locked") || strings.Contains(msg, "busy")
}

func parseNullTime(s sql.NullString) (time.Time, error) {
	if !s.Valid || s.String == "" {
		return time.Time{}, nil
	}
	return parseTime(s.String)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
