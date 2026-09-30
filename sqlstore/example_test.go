package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/sqlstore"
)

func exampleDB() (*sql.DB, func()) {
	dir, err := os.MkdirTemp("", "sqlstore-example")
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "m.db")+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		panic(err)
	}
	return db, func() { _ = db.Close(); _ = os.RemoveAll(dir) }
}

func Example() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	if err := sqlstore.Migrate(ctx, db); err != nil {
		panic(err)
	}
	store := sqlstore.New(db)

	alice := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"}
	bob := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"}
	sent, err := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: alice, To: bob})
	if err != nil {
		panic(err)
	}
	inbox, _ := store.Inbox(ctx, bob, messaging.Filter{})
	again, _ := store.Inbox(ctx, bob, messaging.Filter{})
	fmt.Println(len(inbox), len(again)) // Inbox is destructive

	// Consume is idempotent, with or without a prior Inbox.
	fmt.Println(store.Consume(ctx, sent.ID, bob), store.Consume(ctx, sent.ID, bob))
	// Output:
	// 1 0
	// <nil> <nil>
}

func ExampleMigrate() {
	db, cleanup := exampleDB()
	defer cleanup()
	fmt.Println(sqlstore.Migrate(context.Background(), db))
	fmt.Println(sqlstore.Migrate(context.Background(), db)) // idempotent
	// Output:
	// <nil>
	// <nil>
}

func ExampleSchema() {
	names, _ := fs.Glob(sqlstore.Schema(), "*.sql")
	fmt.Println(names)
	// Output: [001_init.sql]
}

func ExampleNew() {
	db, cleanup := exampleDB()
	defer cleanup()
	_ = sqlstore.Migrate(context.Background(), db)
	var store messaging.Store = sqlstore.New(db)
	_, err := store.Get(context.Background(), "missing")
	fmt.Println(err)
	// Output: envelope not found
}

func ExampleStore_DB() {
	db, cleanup := exampleDB()
	defer cleanup()
	fmt.Println(sqlstore.New(db).DB() == db)
	// Output: true
}

func ExampleStore_Send() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	sent, err := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: a, To: a})
	fmt.Println(err, sent.ID != "", sent.CreatedAt.IsZero())
	// Output: <nil> true false
}

func ExampleStore_Get() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	sent, _ := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: a, To: a})
	got, err := store.Get(ctx, sent.ID)
	fmt.Println(err, got.ID == sent.ID)
	// Output: <nil> true
}

func ExampleStore_Inbox() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	_, _ = store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: a, To: a})
	first, _ := store.Inbox(ctx, a, messaging.Filter{})
	second, _ := store.Inbox(ctx, a, messaging.Filter{})
	fmt.Println(len(first), first[0].DeliveredAt != nil, len(second))
	// Output: 1 true 0
}

func ExampleStore_Thread() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	_, _ = store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, ThreadID: "t1", From: a, To: a})
	_, _ = store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, ThreadID: "t1", From: a, To: a})
	thread, err := store.Thread(ctx, "t1", messaging.Filter{})
	fmt.Println(err, len(thread))
	// Output: <nil> 2
}

// Consume works as a stand-alone acknowledgement, before any Inbox, and is
// safe to call concurrently with Inbox.
func ExampleStore_Consume() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	sent, _ := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: a, To: a})
	fmt.Println(store.Consume(ctx, sent.ID, a))
	got, _ := store.Get(ctx, sent.ID)
	fmt.Println(got.ConsumedAt != nil)
	fmt.Println(store.Consume(ctx, "missing", a))
	// Output:
	// <nil>
	// true
	// envelope not found
}

func ExampleStore_Cancel() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx := context.Background()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	sent, _ := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: a, To: a})
	fmt.Println(store.Cancel(ctx, sent.ID), store.Cancel(ctx, sent.ID))
	inbox, _ := store.Inbox(ctx, a, messaging.Filter{})
	fmt.Println(len(inbox))
	// Output:
	// <nil> <nil>
	// 0
}

func ExampleStore_Subscribe() {
	db, cleanup := exampleDB()
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = sqlstore.Migrate(ctx, db)
	store := sqlstore.New(db)
	a := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "a"}
	ch, _ := store.Subscribe(ctx, a, messaging.Filter{})
	_, _ = store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: a, To: a})
	fmt.Println((<-ch).Kind)
	// Output: notice
}
