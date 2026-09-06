package delivery_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/delivery"
	"github.com/hollis-labs/go-messaging/deliverytest"
)

func TestSQLiteStoreContract(t *testing.T) {
	deliverytest.RunStoreContract(t, func(t *testing.T) deliverytest.Harness {
		clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
		db := openSQLite(t, sqliteMemoryDSN(t))
		t.Cleanup(func() { _ = db.Close() })
		if err := delivery.ApplySQLiteSchema(context.Background(), db); err != nil {
			t.Fatalf("apply schema: %v", err)
		}
		return deliverytest.Harness{Store: delivery.NewSQLiteStore(db, delivery.WithSQLiteClock(clock)), Clock: clock}
	})
}

func TestSQLiteStoreMultiHandleContention(t *testing.T) {
	path := t.TempDir() + "/delivery.db"
	db1 := openSQLite(t, sqliteFileDSN(path, 5000))
	db2 := openSQLite(t, sqliteFileDSN(path, 5000))
	t.Cleanup(func() { _ = db1.Close() })
	t.Cleanup(func() { _ = db2.Close() })
	if err := delivery.ApplySQLiteSchema(context.Background(), db1); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	store1 := delivery.NewSQLiteStore(db1, delivery.WithSQLiteClock(clock))
	store2 := delivery.NewSQLiteStore(db2, delivery.WithSQLiteClock(clock))
	res := mustEnqueue(t, store1, sqliteBasicRequest("multi-handle", agent("alice"), agent("bob")))

	stores := []delivery.Store{store1, store2}
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	errorsSeen := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := stores[i%len(stores)].Claim(context.Background(), sqliteClaimFor(res.Deliveries[0].ID, agent("bob"), fmt.Sprintf("host-%d", i), time.Minute, 1))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
				return
			}
			if errors.Is(err, delivery.ErrAlreadyClaimed) || errors.Is(err, delivery.ErrNoDeliveryReady) || strings.Contains(strings.ToLower(err.Error()), "locked") || strings.Contains(strings.ToLower(err.Error()), "busy") {
				errorsSeen++
				return
			}
			t.Errorf("unexpected claim error: %T %v", err, err)
		}(i)
	}
	wg.Wait()
	if successes != 1 || errorsSeen != 15 {
		t.Fatalf("successes=%d errors=%d", successes, errorsSeen)
	}
	attempts, err := store1.Attempts(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("attempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempts=%d, want 1", len(attempts))
	}
}

func TestSQLiteStorePersistsAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/delivery.db"
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	db := openSQLite(t, sqliteFileDSN(path, 5000))
	if err := delivery.ApplySQLiteSchema(context.Background(), db); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	store := delivery.NewSQLiteStore(db, delivery.WithSQLiteClock(clock))
	res := mustEnqueue(t, store, sqliteBasicRequest("restart", agent("alice"), agent("bob")))
	claim, err := store.Claim(context.Background(), sqliteClaimFor(res.Deliveries[0].ID, agent("bob"), "host", time.Minute, 1))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	_, _, err = store.Ack(context.Background(), delivery.AckRequest{Lease: sqliteLeaseRef(claim.Attempt), Stage: delivery.StageHostAccepted})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	nextAt := clock.Now().Add(5 * time.Minute)
	_, _, err = store.Nack(context.Background(), delivery.NackRequest{Lease: sqliteLeaseRef(claim.Attempt), Retryable: true, Error: "restart me", NextAttemptAt: nextAt})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openSQLite(t, sqliteFileDSN(path, 5000))
	t.Cleanup(func() { _ = reopened.Close() })
	restarted := delivery.NewSQLiteStore(reopened, delivery.WithSQLiteClock(clock))
	msg, err := restarted.GetMessage(context.Background(), res.Message.ID)
	if err != nil {
		t.Fatalf("get message after restart: %v", err)
	}
	if msg.ID != res.Message.ID || string(msg.Payload) != string(res.Message.Payload) {
		t.Fatalf("message changed after restart: %+v", msg)
	}
	del, err := restarted.GetDelivery(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("get delivery after restart: %v", err)
	}
	if del.Status != delivery.DeliveryRetryScheduled || !del.NextAttemptAt.Equal(nextAt) || del.AttemptCount != 1 {
		t.Fatalf("delivery restart state mismatch: %+v", del)
	}
	receipts, err := restarted.Receipts(context.Background(), del.ID)
	if err != nil {
		t.Fatalf("receipts after restart: %v", err)
	}
	if !hasReceipt(receipts, delivery.StagePersisted) || !hasReceipt(receipts, delivery.StageLeaseAcquired) || !hasReceipt(receipts, delivery.StageHostAccepted) || !hasReceipt(receipts, delivery.StageFailed) {
		t.Fatalf("receipts not durable: %+v", receipts)
	}
	dupe := mustEnqueue(t, restarted, sqliteBasicRequest("restart", agent("alice"), agent("bob")))
	if !dupe.Duplicate || dupe.Message.ID != res.Message.ID {
		t.Fatalf("idempotency not durable: %+v", dupe)
	}
}

func TestSQLiteStoreRollsBackCrashWindows(t *testing.T) {
	for _, step := range []delivery.SQLiteMutationStep{delivery.SQLiteStepAfterMessageInsert, delivery.SQLiteStepAfterIdempotencyInsert, delivery.SQLiteStepAfterDeliveryInsert} {
		t.Run(string(step), func(t *testing.T) {
			db := openSQLite(t, sqliteMemoryDSN(t))
			t.Cleanup(func() { _ = db.Close() })
			if err := delivery.ApplySQLiteSchema(context.Background(), db); err != nil {
				t.Fatalf("apply schema: %v", err)
			}
			crash := errors.New("crash")
			store := delivery.NewSQLiteStore(db, delivery.WithSQLiteClock(deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))), delivery.WithSQLiteMutationHook(func(got delivery.SQLiteMutationStep) error {
				if got == step {
					return crash
				}
				return nil
			}))
			_, err := store.Enqueue(context.Background(), sqliteBasicRequest("rollback-"+string(step), agent("alice"), agent("bob"), agent("carol")))
			if !errors.Is(err, crash) {
				t.Fatalf("err=%v, want crash", err)
			}
			for _, table := range []string{"messaging_messages", "messaging_idempotency", "messaging_deliveries", "messaging_receipts"} {
				if got := tableCount(t, db, table); got != 0 {
					t.Fatalf("%s rows=%d, want rollback to 0", table, got)
				}
			}
		})
	}
}

func TestSQLiteStoreRollsBackClaimCrashWindow(t *testing.T) {
	db := openSQLite(t, sqliteMemoryDSN(t))
	t.Cleanup(func() { _ = db.Close() })
	if err := delivery.ApplySQLiteSchema(context.Background(), db); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	crash := errors.New("claim crash")
	store := delivery.NewSQLiteStore(db, delivery.WithSQLiteClock(clock), delivery.WithSQLiteMutationHook(func(step delivery.SQLiteMutationStep) error {
		if step == delivery.SQLiteStepAfterAttemptInsert {
			return crash
		}
		return nil
	}))
	res := mustEnqueue(t, store, sqliteBasicRequest("claim-rollback", agent("alice"), agent("bob")))
	_, err := store.Claim(context.Background(), sqliteClaimFor(res.Deliveries[0].ID, agent("bob"), "host", time.Minute, 1))
	if !errors.Is(err, crash) {
		t.Fatalf("err=%v, want crash", err)
	}
	del, err := store.GetDelivery(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if del.Status != delivery.DeliveryPending || del.AttemptCount != 0 || del.ActiveLeaseToken != "" {
		t.Fatalf("claim rollback left partial lease: %+v", del)
	}
	if got := tableCount(t, db, "messaging_attempts"); got != 0 {
		t.Fatalf("attempts rows=%d, want 0", got)
	}
}

func TestSQLiteStoreContextCancellationAndBusyRollback(t *testing.T) {
	t.Run("canceled context", func(t *testing.T) {
		db := openSQLite(t, sqliteMemoryDSN(t))
		t.Cleanup(func() { _ = db.Close() })
		if err := delivery.ApplySQLiteSchema(context.Background(), db); err != nil {
			t.Fatalf("apply schema: %v", err)
		}
		store := delivery.NewSQLiteStore(db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := store.Enqueue(ctx, sqliteBasicRequest("canceled", agent("alice"), agent("bob")))
		if err == nil {
			t.Fatal("expected cancellation error")
		}
		if got := tableCount(t, db, "messaging_messages"); got != 0 {
			t.Fatalf("messages rows=%d, want 0", got)
		}
	})

	t.Run("busy writer", func(t *testing.T) {
		path := t.TempDir() + "/delivery.db"
		locker := openSQLite(t, sqliteFileDSN(path, 1))
		contender := openSQLite(t, sqliteFileDSN(path, 1))
		t.Cleanup(func() { _ = locker.Close() })
		t.Cleanup(func() { _ = contender.Close() })
		if err := delivery.ApplySQLiteSchema(context.Background(), locker); err != nil {
			t.Fatalf("apply schema: %v", err)
		}
		if _, err := locker.ExecContext(context.Background(), `BEGIN EXCLUSIVE`); err != nil {
			t.Fatalf("begin exclusive: %v", err)
		}
		defer locker.ExecContext(context.Background(), `ROLLBACK`)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := delivery.NewSQLiteStore(contender).Enqueue(ctx, sqliteBasicRequest("busy", agent("alice"), agent("bob")))
		if err == nil {
			t.Fatal("expected busy/cancellation error")
		}
	})
}

func TestSQLiteStoreConnectionOwnershipAndIndexes(t *testing.T) {
	db := openSQLite(t, sqliteMemoryDSN(t))
	t.Cleanup(func() { _ = db.Close() })
	if err := delivery.ApplySQLiteSchema(context.Background(), db); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	store := delivery.NewSQLiteStore(db)
	if store.DB() != db {
		t.Fatal("store did not expose the host-owned DB handle")
	}
	mustEnqueue(t, store, sqliteBasicRequest("owner", agent("alice"), agent("bob")))
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("host DB was not usable after store call: %v", err)
	}
	for _, idx := range []string{"idx_messaging_deliveries_message", "idx_messaging_deliveries_recipient_status_ready", "idx_messaging_deliveries_active_lease", "idx_messaging_attempts_delivery", "idx_messaging_receipts_delivery"} {
		var name string
		err := db.QueryRowContext(context.Background(), `SELECT name FROM sqlite_master WHERE type='index' AND name=?`, idx).Scan(&name)
		if err != nil {
			t.Fatalf("missing index %s: %v", idx, err)
		}
	}
	plan := explainPlan(t, db, `EXPLAIN QUERY PLAN SELECT id FROM messaging_deliveries WHERE recipient_urn=? AND status=? AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY id ASC LIMIT 1`, agent("bob").URN(), delivery.DeliveryPending, time.Now().UTC().Format(time.RFC3339Nano))
	if !strings.Contains(plan, "idx_messaging_deliveries_recipient_status_ready") {
		t.Fatalf("ready query plan did not use ready index: %s", plan)
	}
}

func TestMigrateLegacyMailboxPreservesHistoryAndSafePolicy(t *testing.T) {
	db := openSQLite(t, sqliteMemoryDSN(t))
	t.Cleanup(func() { _ = db.Close() })
	createLegacyMailboxSchema(t, db)
	_, err := db.ExecContext(context.Background(), `INSERT INTO agent_messages(id, from_session_id, from_agent_id, to_session_id, to_agent_id, thread_id, reply_to, type, subject, body, metadata, priority, status, created_at, read_at, resolved_at, channel, kind, payload_json) VALUES
		('legacy-unread', 'sess-a', 'agent-a', 'sess-b', 'agent-b', 'thread-1', '', 'message', 'subject', 'body', '{"trace":"one"}', 2, 'unread', '2026-09-06T12:00:00Z', NULL, NULL, 'inbox', 'notification', '{}'),
		('legacy-resolved', 'sess-a', 'agent-a', 'sess-c', 'agent-c', 'thread-1', 'legacy-unread', 'message', 'done', 'body2', '{}', 3, 'resolved', '2026-09-06T12:01:00Z', '2026-09-06T12:02:00Z', '2026-09-06T12:03:00Z', 'chat', 'request', '{"ok":true}')`)
	if err != nil {
		t.Fatalf("insert legacy rows: %v", err)
	}
	result, err := delivery.MigrateLegacyMailbox(context.Background(), db, delivery.LegacyMailboxMigrationOptions{Authority: "test", Now: time.Date(2026, 9, 6, 13, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("migrate legacy mailbox: %v", err)
	}
	if result.Imported != 2 || result.Held != 1 || result.Delivered != 1 || result.Pending != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	store := delivery.NewSQLiteStore(db)
	msg, err := store.GetMessage(context.Background(), "legacy-unread")
	if err != nil {
		t.Fatalf("get migrated message: %v", err)
	}
	if msg.ID != "legacy-unread" || msg.ThreadID != "thread-1" || msg.From.URN() != "msg://agent/test/sess-a/agent-a" || msg.Metadata["legacy_status"] != "unread" || msg.Metadata["legacy_to_session_id"] != "sess-b" {
		t.Fatalf("legacy identity/history not preserved: %+v", msg)
	}
	del, err := store.GetDelivery(context.Background(), "legacy-unread")
	if err != nil {
		t.Fatalf("get migrated delivery: %v", err)
	}
	if del.Status != delivery.DeliveryDeadLettered || del.DeadLetterReason == "" || del.Recipient.URN() != "msg://agent/test/sess-b/agent-b" {
		t.Fatalf("unread legacy row was not held safely: %+v", del)
	}
	resolved, err := store.GetDelivery(context.Background(), "legacy-resolved")
	if err != nil {
		t.Fatalf("get resolved delivery: %v", err)
	}
	if resolved.Status != delivery.DeliveryDelivered {
		t.Fatalf("resolved legacy status not preserved as historical completion: %+v", resolved)
	}
	receipts, err := store.Receipts(context.Background(), "legacy-resolved")
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	if hasReceipt(receipts, delivery.StageHostAccepted) || hasReceipt(receipts, delivery.StageTurnSubmitted) || hasReceipt(receipts, delivery.StageConsumed) {
		t.Fatalf("mailbox attention state was mapped to delivery handoff proof: %+v", receipts)
	}
	var legacyStatus, readAt, resolvedAt string
	if err := db.QueryRowContext(context.Background(), `SELECT legacy_status, read_at, resolved_at FROM messaging_legacy_mailbox_imports WHERE legacy_message_id='legacy-resolved'`).Scan(&legacyStatus, &readAt, &resolvedAt); err != nil {
		t.Fatalf("legacy audit row: %v", err)
	}
	if legacyStatus != "resolved" || readAt == "" || resolvedAt == "" {
		t.Fatalf("legacy status/timestamps not preserved: status=%q read=%q resolved=%q", legacyStatus, readAt, resolvedAt)
	}
	again, err := delivery.MigrateLegacyMailbox(context.Background(), db, delivery.LegacyMailboxMigrationOptions{Authority: "test"})
	if err != nil {
		t.Fatalf("second migration: %v", err)
	}
	if again.Imported != 0 || again.Skipped != 2 {
		t.Fatalf("migration not idempotent: %+v", again)
	}
}

func openSQLite(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return db
}

func sqliteMemoryDSN(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("file:%s?mode=memory&cache=shared&_pragma=busy_timeout(5000)", strings.ReplaceAll(t.Name(), "/", "_"))
}

func sqliteFileDSN(path string, busyTimeoutMS int) string {
	return fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)", path, busyTimeoutMS)
}

func tableCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func explainPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	parts := []string{}
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan explain: %v", err)
		}
		parts = append(parts, detail)
	}
	return strings.Join(parts, "\n")
}

func createLegacyMailboxSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `CREATE TABLE agent_messages (
		id TEXT PRIMARY KEY,
		from_session_id TEXT NOT NULL,
		from_agent_id TEXT NOT NULL,
		to_session_id TEXT NOT NULL,
		to_agent_id TEXT NOT NULL,
		thread_id TEXT,
		reply_to TEXT,
		type TEXT NOT NULL DEFAULT 'message',
		subject TEXT,
		body TEXT NOT NULL,
		metadata TEXT DEFAULT '{}',
		priority INTEGER DEFAULT 2,
		status TEXT DEFAULT 'unread',
		created_at TEXT NOT NULL,
		read_at TEXT,
		resolved_at TEXT,
		channel TEXT NOT NULL DEFAULT 'chat',
		kind TEXT NOT NULL DEFAULT 'notification',
		payload_json TEXT NOT NULL DEFAULT '{}'
	)`)
	if err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
}

func sqliteBasicRequest(key string, from messaging.Address, recipients ...messaging.Address) delivery.EnqueueRequest {
	targets := make([]delivery.RecipientTarget, 0, len(recipients))
	for i, r := range recipients {
		binding := delivery.BindingTarget{Address: r, BindingGeneration: int64(i + 1), RouteGeneration: 1}
		switch r.Kind {
		case messaging.KindSession:
			binding.SessionID = r.ID
		case messaging.KindAgent:
			binding.ActorID = r.ID
		}
		targets = append(targets, delivery.RecipientTarget{Address: r, Binding: binding})
	}
	return delivery.EnqueueRequest{IdempotencyKey: key, From: from, Recipients: targets, Kind: messaging.MsgKindNotice, Channel: messaging.Channel("control"), Payload: []byte(`{"body":"hello"}`), ContentType: "application/json", Metadata: map[string]string{"trace": "sqlite"}}
}

func sqliteClaimFor(id delivery.DeliveryID, recipient messaging.Address, holder string, lease time.Duration, generation int64) delivery.ClaimRequest {
	return delivery.ClaimRequest{DeliveryID: id, Recipient: recipient, Holder: holder, BindingGeneration: generation, LeaseDuration: lease}
}

func sqliteLeaseRef(a delivery.Attempt) delivery.LeaseRef {
	return delivery.LeaseRef{DeliveryID: a.DeliveryID, AttemptID: a.ID, LeaseToken: a.LeaseToken, BindingGeneration: a.BindingGeneration}
}

func mustEnqueue(t *testing.T, s delivery.Store, req delivery.EnqueueRequest) delivery.EnqueueResult {
	t.Helper()
	res, err := s.Enqueue(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func agent(id string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: id}
}

func hasReceipt(receipts []delivery.Receipt, stage delivery.ReceiptStage) bool {
	for _, r := range receipts {
		if r.Stage == stage {
			return true
		}
	}
	return false
}
