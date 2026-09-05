# go-messaging

[![Go Reference](https://pkg.go.dev/badge/github.com/hollis-labs/go-messaging.svg)](https://pkg.go.dev/github.com/hollis-labs/go-messaging)

A shared Go contract for agent-to-agent, agent-to-service, and
agent-to-user messaging.

`go-messaging` defines the types (`Envelope`, `Address`, `Kind`,
`Channel`, `Filter`), interfaces (`Store`, `Dispatcher`), and delivery
semantics so multiple applications can speak the same wire protocol.
It ships an in-memory reference `Store`, a request/reply `Dispatcher`,
and a shared contract test suite that any third-party `Store` should
pass. Concrete persistent / networked `Store` implementations are
provided by consumer applications.

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

Requires Go 1.22 or newer.

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

Exactly-once-per-recipient via `DeliveredAt` + `ConsumedAt` tracking:

1. `Send` persists an envelope (CREATED).
2. `Inbox(to)` returns undelivered envelopes AND atomically marks
   them DELIVERED for that recipient.
3. Consumer calls `Consume(id, recipient)` after processing
   (CONSUMED).

Crashing between Inbox and Consume means the envelope stays
DELIVERED (does not re-appear in Inbox). Consumers that need
at-least-once should wrap the Dispatcher in their own retry layer.

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
network transport for a foreign hop, and any cross-host authentication, are
supplied by the foreign `Store` itself — `Router` only decides the route.

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
addressing, in-memory reference Store, contract test suite, Dispatcher
request/reply helper, the authority-routing `Router` decorator.

**Out of scope (explicitly):** authentication/authorization, escalation
routing, cross-host transport (the foreign `Store` behind a `Router` route
is app-supplied — e.g. an HTTP client), federation authentication,
retry/backoff, tracing hooks, large-binary payloads. These belong in higher
layers built on top of `Store`.

The optional `mailbox` subpackage is such a higher layer. It supplies
service orchestration and a SQLite adapter for its distinct durable-inbox
contract without changing this root interface.

## Documentation

Full API reference on
[pkg.go.dev/github.com/hollis-labs/go-messaging](https://pkg.go.dev/github.com/hollis-labs/go-messaging).

## License

MIT — see [LICENSE](./LICENSE).
