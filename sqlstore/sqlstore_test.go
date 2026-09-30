package sqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/messagingtest"
	"github.com/hollis-labs/go-messaging/sqlstore"
)

// newDB opens a migrated file database with a busy timeout and WAL, the
// configuration the package documents for concurrent writers.
func newDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "msg.db") +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := sqlstore.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func newStore(t *testing.T) *sqlstore.Store { return sqlstore.New(newDB(t)) }

func addr(id string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: id}
}

func notice(from, to messaging.Address) messaging.Envelope {
	return messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to}
}

func mustSend(t *testing.T, s messaging.Store, env messaging.Envelope) messaging.Envelope {
	t.Helper()
	sent, err := s.Send(context.Background(), env)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	return sent
}

// TestContract runs the shared Store contract suite (acceptance: RunContract
// passes against sqlstore.Store).
func TestContract(t *testing.T) {
	messagingtest.RunContract(t, func(t *testing.T) messaging.Store { return newStore(t) })
}

// TestRouterContract runs the authority-routing suite with this Store as the
// local Store.
func TestRouterContract(t *testing.T) {
	messagingtest.RunRouterContract(t, func(t *testing.T) messaging.Store { return newStore(t) })
}

func TestCancelThenGetSucceeds(t *testing.T) {
	s := newStore(t)
	sent := mustSend(t, s, notice(addr("a"), addr("b")))
	if err := s.Cancel(context.Background(), sent.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := s.Get(context.Background(), sent.ID); err != nil {
		t.Fatalf("get after cancel: %v", err)
	}
}

func TestCancelHidesFromInbox(t *testing.T) {
	s := newStore(t)
	to := addr("victim")
	sent := mustSend(t, s, notice(addr("src"), to))
	if err := s.Cancel(context.Background(), sent.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.Inbox(context.Background(), to, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("canceled envelope leaked into Inbox: %d rows", len(got))
	}
}

func TestCancelIdempotent(t *testing.T) {
	s := newStore(t)
	sent := mustSend(t, s, notice(addr("a"), addr("b")))
	for i := 0; i < 3; i++ {
		if err := s.Cancel(context.Background(), sent.ID); err != nil {
			t.Fatalf("cancel #%d: %v", i, err)
		}
	}
}

func TestCancelMissingReturnsNotFound(t *testing.T) {
	if err := newStore(t).Cancel(context.Background(), "no-such"); !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestConsumeMissingReturnsNotFound(t *testing.T) {
	if err := newStore(t).Consume(context.Background(), "no-such", addr("b")); !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestPayloadAndMetadataRoundTrip(t *testing.T) {
	s := newStore(t)
	want := messaging.Envelope{
		Kind:        messaging.MsgKindRequest,
		Channel:     "ops",
		ThreadID:    "T-1",
		InReplyTo:   "parent-1",
		From:        messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "a", SubID: "x"},
		To:          addr("b"),
		Payload:     json.RawMessage(`{"hello":"world"}`),
		ContentType: "application/json",
		Metadata:    map[string]string{"trace": "abc", "tenant": "main"},
	}
	sent := mustSend(t, s, want)
	got, err := s.Get(context.Background(), sent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != sent.ID || got.Kind != want.Kind || got.Channel != want.Channel ||
		got.ThreadID != want.ThreadID || got.InReplyTo != want.InReplyTo {
		t.Errorf("scalar fields drift: got %+v", got)
	}
	if got.From != want.From || got.To != want.To {
		t.Errorf("address drift: from=%v to=%v", got.From, got.To)
	}
	if string(got.Payload) != string(want.Payload) || got.ContentType != want.ContentType {
		t.Errorf("payload/content type drift: %s %q", got.Payload, got.ContentType)
	}
	if got.Metadata["trace"] != "abc" || got.Metadata["tenant"] != "main" {
		t.Errorf("metadata drift: %v", got.Metadata)
	}
	if !got.CreatedAt.Equal(sent.CreatedAt) {
		t.Errorf("created_at drift: %v vs %v", got.CreatedAt, sent.CreatedAt)
	}
	if got.DeliveredAt != nil || got.ConsumedAt != nil {
		t.Errorf("fresh envelope has lifecycle marks: %+v", got)
	}
}

func TestInboxMarksDeliveredAndConsumeSetsConsumedAt(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	to := addr("b")
	sent := mustSend(t, s, notice(addr("a"), to))
	envs, err := s.Inbox(ctx, to, messaging.Filter{})
	if err != nil || len(envs) != 1 || envs[0].DeliveredAt == nil {
		t.Fatalf("inbox = %+v, %v", envs, err)
	}
	got, _ := s.Get(ctx, sent.ID)
	if got.DeliveredAt == nil || got.ConsumedAt != nil {
		t.Fatalf("after inbox: %+v", got)
	}
	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, sent.ID)
	if got.ConsumedAt == nil {
		t.Fatal("ConsumedAt not set")
	}
}

// TestConsumeIdempotentKeepsFirstTimestamp: the Store doc says Consume is
// idempotent, so a repeat must succeed and must not move ConsumedAt.
func TestConsumeIdempotentKeepsFirstTimestamp(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	to := addr("b")
	sent := mustSend(t, s, notice(addr("a"), to))
	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Fatal(err)
	}
	first, _ := s.Get(ctx, sent.ID)
	time.Sleep(2 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := s.Consume(ctx, sent.ID, to); err != nil {
			t.Fatalf("repeat consume: %v", err)
		}
	}
	again, _ := s.Get(ctx, sent.ID)
	if first.ConsumedAt == nil || again.ConsumedAt == nil || !first.ConsumedAt.Equal(*again.ConsumedAt) {
		t.Errorf("ConsumedAt moved: %v -> %v", first.ConsumedAt, again.ConsumedAt)
	}
}

// TestConsumeThenInboxSequential: Consume without a prior Inbox is a
// stand-alone acknowledgement; the envelope then no longer appears in Inbox.
func TestConsumeThenInboxSequential(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	to := addr("b")
	sent := mustSend(t, s, notice(addr("a"), to))
	if err := s.Consume(ctx, sent.ID, to); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, sent.ID)
	if got.DeliveredAt == nil || got.ConsumedAt == nil {
		t.Fatalf("stand-alone consume left lifecycle unset: %+v", got)
	}
	envs, err := s.Inbox(ctx, to, messaging.Filter{})
	if err != nil || len(envs) != 0 {
		t.Fatalf("inbox after consume = %+v, %v", envs, err)
	}
}

func TestInboxFilterAndLimit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	to := addr("b")
	for i := 0; i < 3; i++ {
		mustSend(t, s, notice(addr("a"), to))
	}
	req := notice(addr("a"), to)
	req.Kind = messaging.MsgKindRequest
	req.ThreadID = "t9"
	mustSend(t, s, req)

	got, err := s.Inbox(ctx, to, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindRequest}})
	if err != nil || len(got) != 1 || got[0].ThreadID != "t9" {
		t.Fatalf("kind filter = %+v, %v", got, err)
	}
	got, err = s.Inbox(ctx, to, messaging.Filter{Limit: 2})
	if err != nil || len(got) != 2 {
		t.Fatalf("limit = %d, %v", len(got), err)
	}
	if !got[0].CreatedAt.Before(got[1].CreatedAt) && got[0].ID >= got[1].ID {
		t.Error("limited inbox not chronological")
	}
	got, err = s.Inbox(ctx, to, messaging.Filter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("remainder = %d, %v", len(got), err)
	}
}

func TestConcurrentSend(t *testing.T) {
	s := newStore(t)
	const writers, perWriter = 8, 16
	var wg sync.WaitGroup
	ids := make(chan string, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				sent, err := s.Send(context.Background(), notice(addr("w"), addr("sink")))
				if err != nil {
					t.Errorf("send: %v", err)
					return
				}
				ids <- sent.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID %s", id)
		}
		seen[id] = true
	}
	if len(seen) != writers*perWriter {
		t.Errorf("got %d IDs, want %d", len(seen), writers*perWriter)
	}
}

func TestConcurrentInboxNoDoubleDeliver(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	to := addr("lone")
	const n, readers = 32, 8
	for i := 0; i < n; i++ {
		mustSend(t, s, notice(addr("src"), to))
	}
	var wg sync.WaitGroup
	results := make(chan []string, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			envs, err := s.Inbox(ctx, to, messaging.Filter{})
			if err != nil {
				t.Errorf("inbox: %v", err)
				return
			}
			ids := make([]string, len(envs))
			for i, e := range envs {
				ids[i] = e.ID
			}
			results <- ids
		}()
	}
	wg.Wait()
	close(results)
	seen := map[string]int{}
	total := 0
	for batch := range results {
		total += len(batch)
		for _, id := range batch {
			seen[id]++
		}
	}
	if total != n {
		t.Errorf("inbox returned %d total, want %d", total, n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("envelope %s delivered %d times", id, c)
		}
	}
}

func TestContextCancelClosesSubscribe(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Subscribe(ctx, addr("sub"), messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected close, got value")
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close within 1s")
	}
}

// TestSystemSubscriptionPreservesRecipientAndKindFilters: the zero Address
// observes every recipient but still honours the Filter.
func TestSystemSubscriptionPreservesRecipientAndKindFilters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newStore(t)
	alice, bob := addr("alice"), addr("bob")
	all, err := s.Subscribe(ctx, messaging.Address{}, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindNotice}})
	if err != nil {
		t.Fatal(err)
	}
	forBob, err := s.Subscribe(ctx, bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	aliceNotice := mustSend(t, s, notice(alice, alice))
	st := notice(alice, bob)
	st.Kind = messaging.MsgKindStatusUpdate
	bobStatus := mustSend(t, s, st)
	bobNotice := mustSend(t, s, notice(alice, bob))
	next := func(ch <-chan messaging.Envelope, want messaging.Envelope) {
		t.Helper()
		select {
		case got := <-ch:
			if got.ID != want.ID {
				t.Fatalf("got %s, want %s", got.ID, want.ID)
			}
		case <-time.After(time.Second):
			t.Fatalf("envelope %s not delivered", want.ID)
		}
	}
	next(all, aliceNotice)
	next(all, bobNotice)
	next(forBob, bobStatus)
	next(forBob, bobNotice)
}

func TestMigrateIdempotentAndSchema(t *testing.T) {
	db := newDB(t)
	for i := 0; i < 2; i++ {
		if err := sqlstore.Migrate(context.Background(), db); err != nil {
			t.Fatalf("migrate #%d: %v", i, err)
		}
	}
	names, err := fs.Glob(sqlstore.Schema(), "*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("Schema() has no .sql files: %v %v", names, err)
	}
	if err := sqlstore.Migrate(context.Background(), nil); err == nil {
		t.Error("Migrate(nil) should fail")
	}
}

func TestDBAccessor(t *testing.T) {
	db := newDB(t)
	if sqlstore.New(db).DB() != db {
		t.Error("DB() did not return the handle")
	}
}
