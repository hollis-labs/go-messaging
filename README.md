# go-messaging

## Moved to substrate

This standalone repository is deprecated. New development lives in the
[`github.com/hollis-labs/substrate/mesh`](https://github.com/hollis-labs/substrate/tree/mesh/v0.1.0/mesh)
module, released as **`mesh/v0.1.0`**.

```sh
go get github.com/hollis-labs/substrate/mesh@v0.1.0
```

Follow the [package and API migration guide](https://github.com/hollis-labs/substrate/blob/mesh/v0.1.0/mesh/messaging/MIGRATION.md) when updating imports;
the consolidation can include API changes. Existing standalone tags and history
are preserved. The documentation below describes the standalone releases and
is retained for historical reference. Applications migrate separately; this
redirect does not deploy or update any consumer.

[![Go Reference](https://pkg.go.dev/badge/github.com/hollis-labs/go-messaging.svg)](https://pkg.go.dev/github.com/hollis-labs/go-messaging)

A shared Go contract for agent-to-agent, agent-to-service, and
agent-to-user messaging.

`go-messaging` defines the types (`Envelope`, `Address`, `Kind`,
`Channel`, `Filter`), interfaces (`Store`, `Dispatcher`), and delivery
semantics so multiple applications can speak the same wire protocol.
It ships an in-memory reference `Store`, a request/reply `Dispatcher`,
a Messaging vNext reliable-delivery reference in `delivery`, and shared
contract test suites that third-party stores can run. A reference SQLite
`Store` ships as the optional [`sqlstore`](./sqlstore/) subpackage, and an
HTTP-backed client for a remote daemon as [`httpstore`](./httpstore/);
applications may keep persistent `Store` implementations of their own.

Applications that need a durable, tuple-addressed inbox with
unread/read/resolved state can use the optional
[`mailbox`](./mailbox/) subpackage. It is separate because its
non-destructive inbox and acknowledgement lifecycle intentionally differs
from the root `Store`'s delivered/consumed contract.

**Status:** pre-1.0 (`v0.x.y`). The contract surface is stable, but
breaking changes may still occur in minor versions; see
[CHANGELOG.md](./CHANGELOG.md).

## Install

```bash
go get github.com/hollis-labs/go-messaging
```

Requires Go 1.26.6 or newer.

## Quick start

```go
import (
    "context"
    "encoding/json"

    "github.com/hollis-labs/go-messaging"
    "github.com/hollis-labs/go-messaging/memstore"
)

store := memstore.New()
disp := messaging.NewDispatcher(store)

resp, err := disp.Request(ctx, messaging.Envelope{
    From:    messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"},
    To:      messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"},
    Payload: json.RawMessage(`{"ask":"health"}`),
})
```

See [`example_test.go`](./example_test.go) for a complete runnable demo
and [`examples/`](./examples) for standalone programs.

## Address model

Addresses are typed structs serialized as URNs on the wire:

    msg://<kind>/<authority>/<id>[/<subid>]

Examples:

    msg://agent/router/sess-abc/primary
    msg://user/app/alice
    msg://service/scheduler/main

`AddressKind` is a closed enum (`agent`, `user`, `service`, `session`,
`workflow`). `Authority` identifies the owning system. `ID` is the
primary identifier within that authority; `SubID` is optional (e.g.,
an agent-within-a-session).

## Message Kinds

`Kind` is a closed enum; the shared package routes on it:

- `request` — expects a response
- `response` — answers a request (must set `InReplyTo`)
- `notice` — one-way informational
- `status_update` — state change broadcast
- `handoff` — transfer of responsibility
- `escalation` — lift to higher authority

`Channel` is an opaque UX-layer pass-through; applications define
their own vocabulary (e.g., `chat`, `inbox`, `alert`). The shared
package never interprets it.

## Delivery semantics

The root `Store` preserves the original delivered/consumed compatibility
lifecycle:

1. `Send` persists an envelope and assigns `ID` + `CreatedAt`.
2. `Inbox(to)` returns undelivered envelopes and atomically marks them
   `DeliveredAt` for that recipient.
3. `Consume(id, recipient)` records the recipient's `ConsumedAt` marker.

That root `Inbox` behavior is intentionally destructive for future
`Inbox` calls by the same recipient. It is useful for lightweight
request/reply and simple local stores, but it is not a durable host-handoff
receipt or proof that a model processed the message.

The Messaging vNext reliability contract is specified in
[`CONTRACTS.md`](./CONTRACTS.md) and implemented by the neutral
[`delivery`](./delivery/) package. It separates immutable message content,
per-recipient delivery obligations, host/runtime attempts, and independent
reader attention state. The portable reliability target is at-least-once
delivery with idempotent effects, not exactly-once model execution. Receipt
stages such as `host_accepted` and `turn_submitted` are observable handoff
facts; they do not assert model understanding or task success.

The `delivery` package includes two contract-conformant stores:

- `NewMemoryStore` is the deterministic in-memory reference implementation.
- `NewSQLiteStore` is a durable implementation over a caller-owned
  `*sql.DB`. Hosts call `ApplySQLiteSchema` explicitly, choose their own
  SQLite DSN/PRAGMA/pooling policy, and remain responsible for closing the
  database handle. Store operations use transactions so message body,
  sender-scoped idempotency, frozen recipient obligations, attempts, and
  receipts commit together or roll back together. SQLite busy/locked claim
  races surface as delivery contention rather than a second successful claim.

`MigrateLegacyMailbox` imports the historical `agent_messages` mailbox table
into the delivery schema for hosts that opt into migration. It preserves row
IDs, thread/reply IDs, sender/recipient session-agent ownership tuples, and
historical `status`, `read_at`, and `resolved_at` values in metadata plus an
audit table. The default policy holds ambiguous unread legacy rows as
dead-lettered delivery obligations requiring authorized redrive, so migration
does not blindly replay old mailbox rows. Historical read/resolved rows are
preserved as completed history without fabricating `host_accepted`,
`turn_submitted`, or `consumed` receipts. See
[`docs/messaging-vnext-compatibility.md`](./docs/messaging-vnext-compatibility.md)
for the migration and rollback guide.

## Federation

The `Authority` segment of every URN is the email-style "domain" that owns
the addressed entity. `Router` is a `Store` decorator that turns it into a
routing seam, so any app gets federated messaging for free:

```go
router := messaging.NewRouter(localStore, "hq")  // localStore serves "hq"
router.Register("branch", branchStore)           // foreign authority → its Store
disp := messaging.NewDispatcher(router)          // federated request/reply
```

- An operation whose authority has a **registered foreign route** is
  dispatched to that route's `Store` — typically an HTTP-backed `Store`
  reaching the host that owns the authority.
- Every other authority **falls through to the local `Store`**.

"Internal vs external" thus collapses to a single routing question —
`router.IsLocal(authority)` — rather than a schema fork. A **standalone
install registers no foreign routes**: every authority resolves to the local
`Store` and messaging works fully locally with zero extra configuration.
Federation is purely additive — the same code path serves both deployments.

Routing is keyed on the **recipient** authority: `Send` on `To`, `Inbox` /
`Subscribe` on the recipient, `Consume` on the recipient. `Get`, `Thread`,
and `Cancel` are keyed by an envelope/thread ID — which carries no
authority — and are served from the local `Store`.

`WithStrictRouting()` makes the `Router` return `ErrNoRoute` for an authority
that is neither local nor registered, instead of falling through. The
`Router` only decides the route: the transport for a foreign hop is the foreign
`Store` itself, for example an [`httpstore`](#http-backed-store) client, and any
cross-host authentication is supplied by that `Store`'s HTTP client. A `Router`
reports the authority it serves with `LocalAuthority()`.

## HTTP-backed Store

The optional `httpstore` subpackage is one `Store` and `Dispatcher` client for
a remote HTTP daemon, in place of a private copy per application:

```bash
go get github.com/hollis-labs/go-messaging/httpstore
```

```go
package main

import (
    "context"
    "fmt"
    "net/http/httptest"

    "github.com/hollis-labs/go-messaging"
    "github.com/hollis-labs/go-messaging/httpstore"
    "github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
    "github.com/hollis-labs/go-messaging/memstore"
)

func main() {
    // A stand-in daemon; in an application this is the real server's URL.
    srv := httptest.NewServer(httpstoretest.Handler(memstore.New(), nil, httpstore.TetherProfile()))
    defer srv.Close()

    me := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"}
    peer := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"}

    store, err := httpstore.New(srv.URL, httpstore.WithIdentity(me))
    if err != nil {
        panic(err)
    }
    ctx := context.Background()
    if _, err := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: me, To: peer}); err != nil {
        panic(err)
    }
    inbox, err := store.Inbox(ctx, peer, messaging.Filter{})
    if err != nil {
        panic(err)
    }
    fmt.Println(len(inbox), "message for", peer.ID)
}
```

The wire is not part of the go-messaging contract. Two dialects that exist
today are selectable as a `Profile` (`TetherProfile`,
`TorqueFederationProfile`); [`docs/http-wire.md`](./docs/http-wire.md)
describes them as they behave, with the Tether and Torque revisions it was
read from. `WithHTTPClient` is the transport seam (a mutual-TLS, pinned
certificate or unix-socket client plugs in there) and `WithRequestHook` the
place for auth headers or trace propagation, so neither dependency enters this
module. `Subscribe` connects before it returns and closes its channel on
context cancel or stream end, with no reconnect.

`httpstore/httpstoretest` holds a reference server (a `Store` behind a
`Profile`'s routes, optionally enforcing Tether's `?as=` rules with
`WithStrictIdentity`) and `RunConformance`, which runs `RunContract` and
`RunRouterContract` against a client of it. `messagingtest.RunContract` takes
options; `messagingtest.Without("Inbox", "Subscribe")` skips the sub-tests for
operations a Store legitimately lacks, such as Torque's federation hop.

## sqlstore

The optional `sqlstore` subpackage is a reference, standalone SQLite
implementation of the root `Store` over a schema it owns. It is for new
adopters, such as the durable `Store` behind an `httpstore` daemon. An
application that already carries its own schema does not have to migrate.

```go
import (
    "context"
    "database/sql"
    "fmt"

    _ "modernc.org/sqlite" // the caller picks the driver

    messaging "github.com/hollis-labs/go-messaging"
    "github.com/hollis-labs/go-messaging/sqlstore"
)

func main() {
    ctx := context.Background()
    db, err := sql.Open("sqlite", "file:messages.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    if err := sqlstore.Migrate(ctx, db); err != nil { // idempotent
        panic(err)
    }
    store := sqlstore.New(db)

    alice := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"}
    bob := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"}
    sent, _ := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: alice, To: bob})
    inbox, _ := store.Inbox(ctx, bob, messaging.Filter{})
    fmt.Println(len(inbox), store.Consume(ctx, sent.ID, bob)) // 1 <nil>
}
```

`Migrate` applies the embedded DDL and `Schema()` returns it as an `fs.FS` for
hosts that run their own migrations. The package imports no SQL driver: open
the `*sql.DB` yourself and set a busy timeout (and WAL) so concurrent writers
wait instead of failing. `INSERT ... RETURNING` needs SQLite 3.35 or newer.
`Subscribe` is an in-process fan-out for envelopes sent through the same
`*Store`; use `Inbox` or `Thread` for durable reads.

It implements only the root contract (a destructive `Inbox` and per-recipient
delivered/consumed markers). Crash-safe delivery obligations, leases and
receipts stay in [`delivery`](./delivery/); compose the two rather than
expecting `sqlstore` to grow them. It passes `messagingtest.RunContract` and
`RunRouterContract`.

`Consume` is a single upsert, so it is idempotent under concurrency, including
when it runs before or alongside `Inbox` for the same envelope. See the
CHANGELOG for the race this avoids.

## Writing a new Store implementation

Any `Store` implementation must pass the shared contract test suite:

```go
package mystore_test

import (
    "testing"

    "github.com/hollis-labs/go-messaging"
    "github.com/hollis-labs/go-messaging/messagingtest"
    "github.com/example/mystore"
)

func TestMystore_Contract(t *testing.T) {
    messagingtest.RunContract(t, func(t *testing.T) messaging.Store {
        return mystore.New(/* ... */)
    })
}
```

All sub-tests must pass for an implementation to be
contract-conformant. A `Store` that will sit behind a `Router` should also
run `messagingtest.RunRouterContract`, which verifies the authority-routing
guarantees on top of the base contract.

## Scope

**In scope:** contract types, interfaces, delivery lifecycle, URN
addressing, in-memory reference Store, reliable `delivery` state machine,
contract test suites, Dispatcher request/reply helper, the authority-routing
`Router` decorator, and an HTTP client `Store`/`Dispatcher` (`httpstore`) with
a reference server for conformance testing (`httpstore/httpstoretest`), and a
reference SQLite `Store` (`sqlstore`).

## Out of scope

For the legacy root `Store` (explicitly): authentication/authorization,
escalation routing, cross-host authentication and trust (mutual TLS,
certificate pinning, peer authorization: `httpstore` only offers the
`WithHTTPClient` and `WithRequestHook` seams), tracing (a hook, not a
dependency), a production HTTP server (`httpstoretest` is a test double),
reconnecting subscriptions, and large-binary payloads. Retry scheduling,
lease fencing, deadline/dead-letter handling, and authorized redrive live in
the `delivery` package instead of silently changing root `Inbox`/`Consume`.

The optional `mailbox` subpackage is such a higher layer. It supplies
service orchestration and a SQLite adapter for its distinct durable-inbox
contract without changing this root interface.

## Compatibility

`RunContract` gained a trailing variadic `ContractOption` parameter; every
existing call compiles unchanged (only a program that stores `RunContract`
in a variable of the old function type needs an adapter). `Router` gained a
`LocalAuthority` method. Both are additive. `httpstore` is part of this
module, so it needs the same Go version as the rest of it (see Install) and
adds no dependency. Its wire profiles describe servers as read at the
revisions named in `docs/http-wire.md`; while the major version is `0.x`,
minor versions may change them. Adopters that pin an older `go-messaging`
must bump it to use `httpstore`.

## Documentation

Full API reference on
[pkg.go.dev/github.com/hollis-labs/go-messaging](https://pkg.go.dev/github.com/hollis-labs/go-messaging).

## License

MIT — see [LICENSE](./LICENSE).
