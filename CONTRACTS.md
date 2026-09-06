# Messaging vNext contracts

This document freezes the G01 contract allocation for Messaging vNext. It is an implementation input for tasks G02-G08, not release evidence. The current module remains usable without Tether, a daemon, or agentkit.

## Library ownership

| Area | Owner | Dependency rule |
| --- | --- | --- |
| Address URNs, envelope content, sender-scoped idempotency, delivery obligations, leases, receipts, replay cursors, and store conformance | `github.com/hollis-labs/go-messaging` | Must not import `agentkit`, `go-agent-wrapper`, `go-providers`, Tether, Nanite, or Torque. |
| Canonical session/bootstrap identity records, optional actor/definition/runtime refs, and launch-time propagation | `github.com/hollis-labs/agentkit` | May reference neutral strings and addresses from consumers, but `go-messaging` remains independent. |
| Provider/runtime delivery capabilities and observation vocabulary for running sessions | `github.com/hollis-labs/go-agent-wrapper` plus provider seams | May depend on `agentkit` and provider libraries; unsupported runtime operations return typed capability errors rather than fabricated success. |
| Tether registration, authorization, routing, room service, and hosted bridges | Tether/go-tether-client workstream | Uses the library contracts after G09 landing; not required for local-only messaging. |
| Nanite, Torque, Tachyon, and other app adoption | App-specific workstreams | Read-only during this library stream except for disposable compile probes. |

## Stable identities and addresses

`Address` remains the provider-neutral wire address. The canonical serialized form stays:

```text
msg://<kind>/<authority>/<id>[/<subid>]
```

The existing address kinds retain their meaning:

| Address kind | Messaging vNext meaning |
| --- | --- |
| `agent` | Durable actor or stable participant address. It can outlive a concrete runtime session when the host explicitly owns that durability. |
| `session` | Concrete session address. Delivery to this target must not silently redirect to a replacement session. |
| `group` | Group/room/fanout target resolved by a host or service into recipient obligations. |
| `service`, `user`, `workflow` | Existing neutral participant categories. They may send or receive when a host authorizes them. |

Session IDs are canonical identity values supplied by the launcher/host, normally from the standard `SESSION` key. Provider-native IDs are optional namespaced mappings attached by a higher layer; `go-messaging` must never fabricate a provider ID when one is unavailable; the contract is no fake provider ID. Durable `AgentID`, `DefinitionRef`, runtime attempt/binding IDs, and parent-session lineage are host/bootstrap vocabulary and are mapped into `Address`, metadata, or explicit delivery references without requiring `go-messaging` to import `agentkit`.

## Root Store compatibility

The root `Store` and `Dispatcher` contracts stay source-compatible. Their lifecycle fields keep their current meaning:

| Root field/API | Preserved behavior |
| --- | --- |
| `Envelope.ID` | Store-assigned immutable UUIDv7 on `Send`; caller-supplied values are overwritten. |
| `Envelope.CreatedAt` | Store-assigned creation time on `Send`. |
| `Envelope.DeliveredAt` | Root `Inbox` return marker. `Inbox` is destructive for future `Inbox` calls by that recipient. |
| `Envelope.ConsumedAt` | Root `Consume` marker. It means the recipient reported processing; it is not model task success. |
| `Store.Inbox` | Returns not-yet-delivered messages and atomically marks them delivered. This is legacy root behavior. |
| `Store.Subscribe` | Best-effort live hint only; historical replay comes from `Inbox`. |
| `Dispatcher.Request/Reply` | Request/reply convenience over the same root `Store` semantics. |

The reliable delivery work must not silently change these root semantics. If a reliable implementation exposes the root interface, it does so through a named adapter whose destructive `Inbox` behavior is explicit.

## Mailbox compatibility

The `mailbox` subpackage remains the compatibility surface for tuple-addressed application inboxes. It intentionally has different lifecycle semantics from the root `Store`:

| Mailbox field/API | Preserved behavior |
| --- | --- |
| `(session_id, agent_id)` tuple | Current mailbox address key. It remains valid for Nanite/Tether-shaped consumers while canonical addresses are introduced. |
| `Message.Status = unread/read/acknowledged/resolved` | Application attention state, not transport acknowledgement. |
| `Store.Inbox` / `Service.Inbox` | Non-destructive list of messages for an inbox owner. It must not imply delivery acceptance by a runtime host. |
| `Service.Ack` | Marks read. It must continue to mean user/application read acknowledgement, not durable handoff success. |
| `Service.Resolve` | Marks application resolution. It is independent of transport delivery attempts. |
| `Service.SetWakeReactor` | Host-owned wake hint. It is not proof of turn submission or model consumption. |
| `HandoffCoordinator` | Host-owned workflow delegation. `mailbox` does not own agent reassignment policy. |

Reliable delivery may feed or project mailbox rows, but the adapter must preserve these meanings. In particular, `Ack` must not be overloaded as `host_accepted`, `turn_submitted`, or `consumed`.

## Reliable delivery contract

Reliable delivery separates four records that older APIs conflate:

| Record | Meaning | Mutability |
| --- | --- | --- |
| Message | Immutable content, sender, kind, channel, payload, metadata, thread/reply correlation, room/group reference, idempotency digest, and creation trace. | Immutable after accepted write except explicit cancellation metadata. |
| Recipient delivery | One frozen obligation to one resolved recipient address. Group fanout creates one obligation per resolved recipient. | Status advances through delivery lifecycle; recipient selection does not change on retry. |
| Attempt | One host/runtime handoff attempt with lease token, binding generation, timing, result, and error. | Append/update within the attempt lifecycle. |
| Attention | Reader/application state such as unread, read, archived, resolved. | Independent of transport acknowledgements. |

Required delivery stages are vocabulary, not promises of task success:

| Stage | Meaning |
| --- | --- |
| `persisted` | Message and recipient delivery obligation were committed atomically. |
| `lease_acquired` | A host/consumer has an exclusive bounded lease for one delivery obligation. |
| `host_accepted` | The host durably accepted responsibility for the handoff. |
| `turn_submitted` | The host submitted the message to a concrete runtime turn or equivalent queue. |
| `consumed` | A separately supported consumer/runtime observation reported consumption. |
| `failed` | One attempt failed and may be retried according to policy. |
| `dead_lettered` | The delivery reached its terminal failure policy. |
| `canceled` | Sender/host canceled remaining delivery when cancellation is supported and authorized. |

The portable baseline is at-least-once delivery with idempotent effects. Exactly-once model execution is not part of this contract.

## Idempotency and conflict detection

Each accepted send may carry a sender-scoped idempotency key. The durable uniqueness scope is `(sender address, idempotency key)`. Reusing that key with the same canonical digest returns the original message/delivery result. Reusing it with a different digest fails with an explicit conflict error and must include enough conflict metadata for callers to diagnose the mismatch without exposing private payload content.

The digest covers the canonical immutable message content and the resolved recipient set. Retry attempts, leases, runtime observations, and attention state are excluded.

## Durable SQLite delivery store

`delivery.NewSQLiteStore` implements the same `delivery.Store` contract as the
in-memory reference. It accepts a host-owned `*sql.DB`; callers own connection
lifetime, PRAGMA configuration, pooling, and schema rollout. `ApplySQLiteSchema`
creates versioned `messaging_*` delivery tables and indexes. Store mutations are
transactional: message content, idempotency, recipient obligations, attempts,
and receipts are committed as one unit or rolled back as one unit. Multi-handle
claim contention may hit SQLite busy/locked paths, but it must not create a
second successful lease for one delivery.

SQLite timestamps are stored in fixed-width UTC RFC3339 nanosecond form so
ready and lease-expiry scans compare correctly in SQL. Query indexes cover
message fanout lookup, recipient/status ready scans, active leases, attempts,
and receipts.

`MigrateLegacyMailbox` is a host-invoked migration primitive for the historical
`agent_messages` mailbox table. It preserves legacy row IDs, thread/reply
correlation, tuple ownership, `status`, `read_at`, and `resolved_at` in the new
message metadata and `messaging_legacy_mailbox_imports` audit table. The safe
default policy imports ambiguous unread rows as dead-lettered obligations that
require authorized redrive instead of replaying them blindly. Mailbox `Ack` and
`Resolve` remain attention/history state; migration does not synthesize
`host_accepted`, `turn_submitted`, or `consumed` receipts from them. Hosts that
want immediate replay of unread historical rows must choose the explicit
`LegacyMailboxReplayUnread` policy.

## Lease, fencing, retry, and dead-letter rules

A host leases a recipient delivery before handoff. Lease records must include a token, holder identity, binding generation, acquisition time, expiry, and attempt ID. Mutating a delivery from a stale token or stale binding generation fails. Expired leases can be reclaimed according to store policy without changing the recipient address.

Retry state is per recipient delivery. Offline recipients should remain pending or delayed; they should not burn attempts merely because no host is online. Attempts record retryable vs terminal failure, next-at deadlines, and dead-letter disposition.

## Rooms and group fanout

A group or room send first resolves the group address under host/service authorization. The resulting recipient set is frozen with the message. Each recipient receives a distinct delivery obligation and receipt trail linked to the same immutable message body. Later membership changes do not move existing retries to new recipients unless a new message/fanout operation is created.

Room read cursors and membership history belong to the room service. They must not replace per-recipient delivery receipts.

## Authority and trace

Callers are authenticated outside this package and then passed to the library through explicit caller identity or service interfaces. Self-asserted sender URNs are not enough for arbitrary sends, room reads, role rebinding, wake, or hosted-runtime control.

Trace records carry stable correlations only: session ID, optional actor/definition refs, provider/native IDs when actually known, Torque task/run IDs, message/delivery/attempt IDs, route generation, and commit/version references. Trace metadata must not imply provider identity when none was observed and must not expose private transcript content by default.

## Conformance plan

G03-G08 implementations must include conformance cases for:

1. Store-assigned immutable message IDs and frozen recipient delivery IDs.
2. Sender-scoped idempotent resend and explicit digest conflict.
3. Actor-target delivery and session-target delivery, proving a concrete session target is not redirected to a replacement session.
4. Group fanout with one immutable body, frozen recipient set, per-recipient receipts, and membership-change retry stability.
5. Lease token and binding-generation fencing, including stale token rejection.
6. Crash/restart windows after lease, after durable handoff acceptance, and before receipt persistence.
7. Retry, deadline, delayed offline recipient, and dead-letter transitions.
8. Pull-only replay cursors and live notification hints, proving notifications are not delivery proof.
9. Trace correlation across message, delivery, attempt, session, optional actor, route generation, and provider-native IDs only when supplied.
10. Compatibility adapters proving root `Store.Inbox` remains destructive and `mailbox.Service.Ack` remains read/attention state.
11. Negative authorization cases for spoofed sender, mismatched inbox owner, unauthorized room read/fanout, stale route binding, and unsupported wake/control capability.

## Current consumer-shaped references

These references were refreshed after the G00 local landing and are read-only inputs for this library stream:

| Consumer shape | Current usage to preserve |
| --- | --- |
| Nanite | `apps/nanite/go.mod` imports `github.com/hollis-labs/go-messaging v0.4.0`; CLI/background/service code uses `go-messaging/mailbox` `Service`, `SendInput`, `Inbox`, `Ack`, `Resolve`, `SetWakeReactor`, `EventStore`, and `HandoffCoordinator` semantics. |
| go-tether-client | Uses root `go-messaging` `Store`, `Dispatcher`, `Envelope`, `Address`, `Filter`, `memstore`, and `messagingtest.RunContract`. |
| Tether | Has service/store/API/message transport code that should adapt to reliable delivery after G09; no Tether repository changes occur in this stream. |
| Torque | Has broker/federation/session messaging code and a later single adoption task; this stream may use disposable compile probes but does not modify Torque. |

## Versioning boundary

G01 does not publish a version. Candidate version notes belong in G08 after implementation and review. G09 is the user-controlled landing gate before Tether begins.
