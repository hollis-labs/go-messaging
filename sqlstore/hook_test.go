package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hollis-labs/go-messaging/sqlstore"
)

// hookConnector wraps the registered SQLite driver so a test can run code
// immediately before a matching Exec statement reaches the database. It makes
// a concurrency interleaving deterministic without any test seam in the
// production code.
type hookConnector struct {
	drv    driver.Driver
	dsn    string
	before func(query string)
}

func (c hookConnector) Connect(context.Context) (driver.Conn, error) {
	inner, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &hookConn{Conn: inner, before: c.before}, nil
}

func (c hookConnector) Driver() driver.Driver { return c.drv }

type hookConn struct {
	driver.Conn
	before func(query string)
}

func (c *hookConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.before(q)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}

func (c *hookConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}

func (c *hookConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}

func (c *hookConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, q)
}

// newHookedDB opens a migrated database whose Exec statements first pass
// through before. Query statements (Inbox's INSERT ... RETURNING) do not.
func newHookedDB(t *testing.T, before func(query string)) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "msg.db") +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	probe, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	drv := probe.Driver()
	_ = probe.Close()
	db := sql.OpenDB(hookConnector{drv: drv, dsn: dsn, before: before})
	t.Cleanup(func() { _ = db.Close() })
	if err := sqlstore.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

// insertsDelivery reports whether q is the fallback/upsert write into
// message_deliveries that Consume issues through Exec.
func insertsDelivery(q string) bool {
	return strings.Contains(q, "INSERT INTO message_deliveries")
}
