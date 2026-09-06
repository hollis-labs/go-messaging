# Messaging vNext compatibility, migration, and rollback guide

This guide covers the local library boundary only. It does not publish a release, deploy a service, migrate a production database, or adopt the APIs in Tether, Torque, Nanite, or Tachyon.

## Authoritative storage

Choose one authoritative write path for a message stream:

- Existing root `messaging.Store` consumers continue to use the root store contract. `Inbox` is destructive for delivered messages, and `Consume` records the root consumed marker.
- Existing `mailbox.Service` consumers continue to use the `agent_messages` mailbox table. `Inbox` and `AttentionList` are non-destructive; `Ack`, `MarkUnread`, `Archive`, and `Resolve` are reader/application attention state.
- New reliable delivery hosts use `delivery.Store` with immutable messages, recipient delivery obligations, attempts, leases, and receipts.

Do not shadow-write the same logical message into both mailbox and reliable delivery as two independent authorities. Use the projection helpers to choose a boundary:

- `delivery.EnvelopeEnqueueRequest` projects a root envelope into one reliable delivery enqueue request.
- `delivery.EnvelopeFromDelivery` projects reliable delivery state back to a root-compatible envelope view.
- `mailbox.DeliveryRequestFromSendInput` projects one mailbox send into a reliable delivery request while preserving legacy tuple metadata.

## Stable actors, exact sessions, and legacy tuples

`delivery.StableActorTarget` records a durable actor binding without requiring a session ID. This supports offline actors: no attempt is consumed until a host claims the delivery.

`delivery.ExactSessionTarget` records a concrete session binding. Hosts must not silently redirect that obligation to a replacement session.

`mailbox.LegacyTupleAddress` preserves a historical Nanite-style `(session_id, agent_id)` tuple as `msg://agent/<authority>/<session_id>/<agent_id>`. `mailbox.TupleFromAddress` only returns a tuple for that legacy shape. `mailbox.StableActorAddress` intentionally has no session component and must not be used to fabricate tuple history.

## Attention is not transport acknowledgement

Mailbox attention operations do not acknowledge reliable delivery transport:

- `AttentionList` reads mailbox rows and leaves unread counts unchanged.
- `Ack` marks `agent_messages.status = 'read'`.
- `MarkUnread` reopens reader attention and clears `read_at` / `resolved_at`.
- `Archive` records an application attention state for hosts whose schema admits `archived`.
- `Resolve` marks application completion.

None of those operations create `host_accepted`, `turn_submitted`, or `consumed` reliable delivery receipts. Hosts that need a transport receipt must call the reliable `delivery.Store` lease/ack API explicitly.

## Legacy mailbox migration

`delivery.MigrateLegacyMailbox` imports historical `agent_messages` rows into the reliable delivery schema when a host opts in. It preserves:

- legacy message ID as the new message ID and delivery ID,
- thread and reply IDs,
- sender and recipient session/agent tuples,
- legacy status, priority, subject, body, metadata, read timestamp, and resolved timestamp,
- an audit row in `messaging_legacy_mailbox_imports`.

The default `LegacyMailboxHoldAmbiguousUnread` policy imports unread rows as dead-lettered obligations with a reason requiring authorized redrive. This avoids blind replay of historical mailbox entries. Hosts that explicitly want unread rows to become pending delivery obligations can pass `LegacyMailboxReplayUnread`.

Historical read, acknowledged, and resolved rows are imported as completed delivery history, but migration only writes a persisted import receipt. It does not synthesize host/runtime handoff receipts from mailbox attention state.

## Rollback

Library rollback is package-level:

1. Stop using the projection helpers or `delivery.NewSQLiteStore` in the host.
2. Continue reading the original root store or `mailbox.Service` path; those APIs remain compatible.
3. Leave `messaging_*` delivery tables in place for audit or drop them under a host-owned migration after backup.
4. If `Archive` was adopted, ensure the host schema migration adding `archived` is reverted only after archived rows are mapped to a host-supported state.

There is no automatic production database mutation in this package. Schema application and legacy import require explicit host calls against a provided `*sql.DB`.

## Deprecated behavior

No existing root or mailbox behavior is deprecated in this local stream. The deprecated pattern is semantic overloading: treating mailbox `Ack` as reliable delivery `host_accepted`, `turn_submitted`, or `consumed`, or treating root/mailbox writes and reliable-delivery writes as two simultaneous authorities for the same logical inbox.

The completed CW-20260905-0013 mailbox extraction remains the mailbox base. This guide adds compatibility projections around it; it does not repeat that extraction or move Nanite adoption into this repository.
