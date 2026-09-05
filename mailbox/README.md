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
registration, event persistence, handoff coordination, notifications, wake
reactions, and tracked asynchronous work. The package imports no host
application and does not name or query host-owned session tables.

The only table expected by the included adapter is:

- `agent_messages` with the fields represented by `Message`. The
  `channel`, `kind`, `type`, and `status` constraints must admit the
  exported constants.
Event storage and handoff state are optional host adapters. `EventStore`
receives mailbox mutation events and must return the most recent requested
page in chronological order. `HandoffCoordinator` owns persistence,
authorization/audit metadata, and any atomic update of the host's session
model. The mailbox package does not assume table names, agent modes, or
approval identities.

## Service composition

```go
store := mailbox.NewSQLiteStore(db)
service := mailbox.NewService(store, agentDirectory, agentRegistrar)
service.SetEventStore(eventStore)
service.SetHandoffCoordinator(handoffCoordinator)
service.SetNotificationSink(notificationSink)
service.SetWakeReactor(wakeReactor)
service.SetAsyncRunner(lifecycleRunner)
```

`AgentResolver` returns existence rather than a host record, and
`AgentRegistrar` receives an opaque `RegisterAs` hint. Record shape,
synthetic identities, and registration policy therefore remain in the host.

## Concurrency and shutdown

`Service` and `SQLiteStore` are safe for concurrent use when their
collaborators are. Live subscriptions use a bounded buffer of 16 messages.
Publishing is non-blocking; a slow subscriber drops only its own delivery.
Canceling the subscription context removes and closes that channel. Call
`Service.Close` during shutdown to close every remaining subscription. A
subscription admitted before `Close` is closed by it; one attempted after
`Close` returns `ErrClosed`.

Wake reactions receive a copy of the persisted message and run asynchronously.
Configure `AsyncRunner` when those goroutines must be tracked and drained;
otherwise a panic-contained untracked goroutine is used.
