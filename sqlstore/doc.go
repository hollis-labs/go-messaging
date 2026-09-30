// Package sqlstore is a reference, standalone SQLite implementation of
// go-messaging's root Store, over a schema owned by this package. It exists
// for new adopters -- a daemon behind httpstore's wire protocol needs a
// durable Store, not memstore -- and does not require an application that
// already carries its own schema (Torque, Tether) to migrate.
//
// sqlstore implements only the root Store contract: a destructive Inbox and
// delivered_at / consumed_at markers per recipient. It does not track
// delivery obligations, leases, attempts or receipts. A host that needs
// at-least-once delivery with those guarantees composes this package with
// go-messaging/delivery rather than asking sqlstore to grow them.
//
// # Concurrency
//
// Every state change is one SQL statement, or a transaction whose first
// statement is the write, so correctness rests on SQLite's statement
// atomicity and not on an application-level lock or on MaxOpenConns=1.
//
//   - Inbox claims undelivered envelopes with a single INSERT ... SELECT ...
//     ON CONFLICT DO NOTHING RETURNING, so two concurrent Inbox calls never
//     return the same envelope.
//   - Consume is a single INSERT ... ON CONFLICT DO UPDATE upsert. Calling it
//     before, after or concurrently with Inbox for the same envelope and
//     recipient always succeeds and is idempotent. (A check-then-insert
//     Consume loses that race to Inbox and fails with a primary-key
//     violation; see TestConsumeInboxRace.)
//
// The package does not import a SQL driver. The caller opens the *sql.DB and
// should enable a busy timeout (and WAL) so concurrent writers wait rather
// than fail with SQLITE_BUSY. With modernc.org/sqlite that is a DSN such as
//
//	file:messages.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)
//
// INSERT ... RETURNING requires SQLite 3.35 or newer. Timestamps are stored as
// fixed-width UTC text with nanosecond precision.
//
// Subscribe is an in-process fan-out: it delivers envelopes sent through the
// same *Store value, not through other processes sharing the database. Use
// Inbox or Thread for durable reads.
package sqlstore
