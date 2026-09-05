# Durable mailbox service

`mailbox` is the optional service-level companion to the root
`go-messaging` contract. It preserves a durable inbox model addressed by
`(session_id, agent_id)`, with independent unread/read/resolved state,
priority ordering, thread history, session catch-up, live fan-out, handoffs,
and replay events.

The root package remains the portable wire contract: typed URN addresses,
`Envelope`, delivered/consumed lifecycle, routing, and request/reply. The
mailbox lifecycle is intentionally separate because reading an inbox here is
non-destructive until `Ack`, while root `Store.Inbox` atomically marks
envelopes delivered. Making both behaviors one interface would weaken the
contract.

## Host ownership

`SQLiteStore` accepts an existing `*sql.DB`; it does not create, migrate,
or close the database. Hosts own schema migrations and connection lifetime.
`Service` accepts narrow host interfaces for agent lookup, optional sender
registration, notifications, wake reactions, and tracked asynchronous work.
The package imports no host application.

The expected host tables are:

- `agent_messages` with the fields represented by `Message`. The
  `channel`, `kind`, `type`, and `status` constraints must admit the
  exported constants.
- `session_events(id, session_id, event_type, channel,
  envelope_pointer_json, created_at)` for replay bookkeeping.
- `session_handoffs(id, session_id, from_agent_id, to_agent_id,
  requested_by, status, requested_at, approved_at, approved_by_user,
  context_message_count, notes)`.
- `session_agents(session_id, agent_id, mode, joined_at, is_primary)`
  with a unique key on `(session_id, agent_id)`.

Only `agent_messages` is required for direct `SQLiteStore` use.
`session_events` is required when a DB-backed `Service` sends,
acknowledges, resolves, or records explicit events. The two handoff tables are
required only for handoff methods.

## Service composition

```go
store := mailbox.NewSQLiteStore(db)
service := mailbox.NewService(store, db, agentDirectory, agentRegistrar)
service.SetNotificationSink(notificationSink)
service.SetWakeReactor(wakeReactor)
service.SetAsyncRunner(lifecycleRunner)
```

`AgentResolver` returns existence rather than a host record, and
`AgentRegistrar` receives an opaque `RegisterAs` hint. Record shape and
registration policy therefore remain in the host.

## Concurrency and shutdown

`Service` and `SQLiteStore` are safe for concurrent use when their
collaborators are. Live subscriptions use a bounded buffer of 16 messages.
Publishing is non-blocking; a slow subscriber drops only its own delivery.
Canceling the subscription context removes and closes that channel. Call
`Service.Close` during shutdown to close every remaining subscription.

Wake reactions receive a copy of the persisted message and run asynchronously.
Configure `AsyncRunner` when those goroutines must be tracked and drained;
otherwise a panic-contained untracked goroutine is used.
