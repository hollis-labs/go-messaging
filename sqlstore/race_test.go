package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/sqlstore"
)

// torqueShapedConsume is an in-test copy of the Consume in Torque's private
// sqlstore.go (apps/torque/internal/messaging), run against this package's
// schema: an existence check, an UPDATE that finds no delivery row, then a
// separate fallback INSERT. The gap between the UPDATE and the INSERT is the
// race window.
func torqueShapedConsume(ctx context.Context, db *sql.DB, id string, recipient messaging.Address) error {
	rURN := recipient.URN()
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z")

	var exists int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM messages WHERE id = ?`, id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ErrNotFound
	}
	if err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `
        UPDATE message_deliveries
           SET consumed_at = COALESCE(consumed_at, ?)
         WHERE message_id = ? AND recipient_urn = ?`, now, id, rURN)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		_, err = db.ExecContext(ctx, `
            INSERT INTO message_deliveries (message_id, recipient_urn, delivered_at, consumed_at)
            VALUES (?, ?, ?, ?)`, id, rURN, now, now)
		if err != nil {
			return err
		}
	}
	return nil
}

// TestConsumeInboxRaceReproducedOnTorqueShape pins the interleaving
// deterministically: Inbox commits its delivery row after the Torque-shaped
// Consume has decided there is no row to update and before its fallback
// INSERT executes. The Consume then fails with a raw primary-key violation
// instead of the idempotent success the Store doc promises.
func TestConsumeInboxRaceReproducedOnTorqueShape(t *testing.T) {
	var s *sqlstore.Store
	var armed atomic.Bool
	var inboxGot []messaging.Envelope
	var inboxErr error
	to := addr("racer")
	db := newHookedDB(t, func(q string) {
		if insertsDelivery(q) && armed.CompareAndSwap(true, false) {
			inboxGot, inboxErr = s.Inbox(context.Background(), to, messaging.Filter{})
		}
	})
	s = sqlstore.New(db)
	ctx := context.Background()
	sent := mustSend(t, s, notice(addr("src"), to))

	armed.Store(true)
	err := torqueShapedConsume(ctx, db, sent.ID, to)
	if inboxErr != nil || len(inboxGot) != 1 {
		t.Fatalf("inbox in the gap = %d, %v", len(inboxGot), inboxErr)
	}
	if err == nil {
		t.Fatal("expected the naive Consume to fail on the primary key, but it succeeded")
	}
	if errors.Is(err, messaging.ErrNotFound) {
		t.Fatalf("expected a raw constraint error, got a sentinel: %v", err)
	}
	t.Logf("naive Consume error (opaque, non-sentinel): %v", err)
}

// TestConsumeInboxRaceDeterministic runs the identical interleaving against
// sqlstore.Store.Consume: Inbox commits its delivery row immediately before
// Consume's write reaches the database. Consume must still succeed and the
// envelope must end up delivered and consumed exactly once. This is the test
// that fails if Consume is rewritten as check-then-insert.
func TestConsumeInboxRaceDeterministic(t *testing.T) {
	var s *sqlstore.Store
	var armed atomic.Bool
	var inboxGot []messaging.Envelope
	var inboxErr error
	to := addr("racer")
	db := newHookedDB(t, func(q string) {
		if insertsDelivery(q) && armed.CompareAndSwap(true, false) {
			inboxGot, inboxErr = s.Inbox(context.Background(), to, messaging.Filter{})
		}
	})
	s = sqlstore.New(db)
	ctx := context.Background()
	sent := mustSend(t, s, notice(addr("src"), to))

	armed.Store(true)
	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Fatalf("Consume racing Inbox: %v", err)
	}
	if inboxErr != nil {
		t.Fatalf("inbox: %v", inboxErr)
	}
	if len(inboxGot) != 1 {
		t.Fatalf("inbox delivered %d, want 1", len(inboxGot))
	}
	got, err := s.Get(ctx, sent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DeliveredAt == nil || got.ConsumedAt == nil {
		t.Fatalf("lifecycle not set: %+v", got)
	}
	if !got.DeliveredAt.Equal(*inboxGot[0].DeliveredAt) {
		t.Errorf("delivered_at rewritten by Consume: %v vs %v", got.DeliveredAt, inboxGot[0].DeliveredAt)
	}
}

// TestConsumeInboxRace hammers Consume-before-Inbox from many goroutines on
// a real file database. Every Consume must succeed, every envelope must end
// up consumed, and Inbox must never return an envelope twice. Run it under
// -race and -count=20.
func TestConsumeInboxRace(t *testing.T) {
	for round := 0; round < 1; round++ {
		consumeInboxRound(t, round)
	}
}

func consumeInboxRound(t *testing.T, round int) {
	s := newStore(t)
	ctx := context.Background()
	const (
		messagesPerRound = 24
		inboxers         = 6
		consumersPerMsg  = 2
	)
	to := addr("racer")
	ids := make([]string, messagesPerRound)
	for i := range ids {
		ids[i] = mustSend(t, s, notice(addr("src"), to)).ID
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	delivered := map[string]int{}
	fail := func(format string, args ...any) {
		t.Errorf(format, args...)
	}
	for _, id := range ids {
		for c := 0; c < consumersPerMsg; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if err := s.Consume(ctx, id, to); err != nil {
					fail("round %d: Consume(%s): %v", round, id, err)
				}
			}()
		}
	}
	for r := 0; r < inboxers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for k := 0; k < 4; k++ {
				envs, err := s.Inbox(ctx, to, messaging.Filter{Limit: 7})
				if err != nil {
					fail("Inbox: %v", err)
					return
				}
				mu.Lock()
				for _, e := range envs {
					delivered[e.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	for id, n := range delivered {
		if n != 1 {
			t.Errorf("round %d: envelope %s returned by Inbox %d times", round, id, n)
		}
	}
	for _, id := range ids {
		got, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.ConsumedAt == nil || got.DeliveredAt == nil {
			t.Errorf("envelope %s not fully marked: delivered=%v consumed=%v", id, got.DeliveredAt, got.ConsumedAt)
		}
	}
	// Whatever Consume claimed first must not be in Inbox afterwards.
	if rest, err := s.Inbox(ctx, to, messaging.Filter{}); err != nil || len(rest) != 0 {
		t.Errorf("leftover inbox = %d, %v", len(rest), err)
	}
}

// TestNaiveConsumeStressFails runs the same stress against the Torque-shaped
// Consume and records how often it fails. It asserts nothing about the count
// (timing dependent); the deterministic reproduction above is the proof.
func TestNaiveConsumeStressFails(t *testing.T) {
	db := newDB(t)
	s := sqlstore.New(db)
	ctx := context.Background()
	to := addr("racer")
	var ids []string
	for i := 0; i < 40; i++ {
		ids = append(ids, mustSend(t, s, notice(addr("src"), to)).ID)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	failures := 0
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := torqueShapedConsume(ctx, db, id, to); err != nil {
				mu.Lock()
				failures++
				mu.Unlock()
			}
		}()
	}
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for k := 0; k < 4; k++ {
				_, _ = s.Inbox(ctx, to, messaging.Filter{Limit: 7})
			}
		}()
	}
	close(start)
	wg.Wait()
	t.Logf("Torque-shaped Consume failed %d/%d times under stress", failures, len(ids))
}
