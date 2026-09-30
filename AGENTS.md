# go-messaging

The shared contract for agent-to-agent, agent-to-service and agent-to-user
messaging: the `Envelope` / `Address` / `Kind` / `Channel` / `Filter` types, the
`Store` and `Dispatcher` interfaces, and the delivery semantics that let
several applications speak one wire protocol. It ships an in-memory reference
store and contract suites; durable and networked stores belong to consumers.

## Start Here

- `README.md` orients; `CONTRACTS.md` freezes the library-ownership split and
  the dependency rules, and is the first thing to read before adding anything.
- `urn.go` owns the `msg://<kind>/<authority>/<id>[/<subid>]` address form.
- `messaging.go` and `store.go` declare the envelope types and the `Store`
  contract; `dispatcher.go` owns request/reply.
- `delivery/` is the reliable-delivery reference — obligations, leases,
  receipts, replay cursors.
- `mailbox/` is the optional non-destructive inbox, deliberately separate from
  the root store's delivered/consumed contract.
- `memstore/` is the in-memory reference implementation.
- `sqlstore/` is the reference SQLite implementation of the root `Store`;
  `Consume` there is deliberately one upsert statement (see its doc.go).
- `messagingtest/` and `deliverytest/` are the conformance suites third-party
  stores run.

## Commands

```bash
make vet
make lint
make test
make vuln
```

Run these individually rather than `make check`: `check` includes `make fmt`,
which is `go fmt ./...` and rewrites tracked source in place.

## Boundaries

The dependency rule in `CONTRACTS.md` is the load-bearing one: this module must
not import `agentkit`, `go-agent-wrapper`, `go-providers`, Tether, Nanite or
Torque. It sits below all of them so they can share a wire protocol; one import
upward makes the contract un-shareable.

`mailbox` attention state and delivery acknowledgement are separate lifecycles
that must not be conflated. Listing an inbox is non-destructive and is not a
delivery ack, and mailbox mutators do not touch the delivery store —
`TestMailboxAttentionListIsNonDestructiveAndNotDeliveryAck` and
`TestMailboxAttentionMutatorsDoNotTouchDeliveryStore` exist precisely because
the two look interchangeable and are not.

Delivery is idempotent by design: consuming twice, and replaying duplicate
hints or receipts, must be safe (`TestMemstore_Consume_Idempotent`,
`TestPumpDuplicateHintsAndReceiptsAreIdempotent`). Handoff records before ack
and does not consume on submit.

`Ack` is authorization-checked — a non-recipient cannot acknowledge another
agent's message (`TestService_Ack_ForbidsNonRecipient`).

The address wire form is serialized and stored by consumers. Changing a kind's
meaning or the URN shape is a protocol break regardless of what compiles here.
