# go-messaging

Shared messaging contract for the `hollis-labs` agent portfolio.

`go-messaging` defines the types (`Envelope`, `Address`, `Kind`,
`Channel`), interfaces (`Store`, `Dispatcher`), and delivery semantics
used by every agent-to-agent, agent-to-service, and agent-to-user
message in the portfolio. It ships an in-memory reference Store and a
shared contract test suite; concrete persistent Stores live in
consumer projects (agent-mux for cross-system runtime, Nanite for
app-local chat).

**Status:** v0.1 — see the design spec for the full rationale and the
four-phase rollout plan.

## Install

```bash
go get github.com/hollis-labs/go-messaging@v0.1.0
```

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

See `example_test.go` for a complete runnable demo.

## Address model

Addresses are typed structs serialized as URNs on the wire:

    msg://<kind>/<authority>/<id>[/<subid>]

Examples:

    msg://agent/agent-mux/sess-abc/primary
    msg://user/nanite/chrispian
    msg://service/clockwork/scheduler

`AddressKind` is a closed enum (`agent`, `user`, `service`, `session`,
`workflow`). `Authority` identifies the owning system. `ID` is the primary
identifier within that authority; `SubID` is optional (e.g., agent-within-
session).

## Message Kinds

Kind is a closed enum; the shared package routes on it:

- `request` — expects a response
- `response` — answers a request (must set `InReplyTo`)
- `notice` — one-way informational
- `status_update` — state change broadcast
- `handoff` — transfer of responsibility
- `escalation` — lift to higher authority

`Channel` is an opaque UX-layer pass-through; apps define their own
vocabulary (e.g., `chat`, `inbox`, `alert`). The shared package never
interprets it.

## Delivery semantics

Exactly-once-per-recipient via `DeliveredAt` + `ConsumedAt` tracking:

1. `Send` persists an envelope (CREATED).
2. `Inbox(to)` returns undelivered envelopes AND atomically marks them
   DELIVERED for that recipient.
3. Consumer calls `Consume(id, recipient)` after processing (CONSUMED).

Crashing between Inbox and Consume means the envelope stays DELIVERED
(does not re-appear in Inbox). Consumers that need at-least-once should
wrap the Dispatcher in their own retry layer.

## Writing a new Store impl

Any Store impl must pass the shared contract test suite:

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
        return mystore.New(...)
    })
}
```

All 13 sub-tests must pass for an impl to be contract-conformant.

## Scope

- **In scope v0.1:** contract types, interfaces, delivery lifecycle,
  URN addressing, in-memory reference Store, contract test suite,
  Dispatcher request/reply helper.
- **Out of scope v0.1 (explicitly):** authn/authz, escalation routing,
  cross-daemon federation, retry/backoff, tracing hooks, large-binary
  payloads.

See the design spec for full scope-fence rationale.

## Design reference

`agent-workspaces/docs/superpowers/specs/2026-04-20-messaging-design.md`

## License

MIT — see LICENSE.
