// Package messaging defines a shared contract for agent-to-agent,
// agent-to-service, and agent-to-user messaging.
//
// The package ships contract types (Envelope, Address, Kind, Channel,
// Filter), interfaces (Store, Dispatcher), error sentinels, and an
// in-memory reference Store implementation under memstore/. It is not
// itself a running service — concrete Store implementations are
// supplied by consumer applications (e.g. an HTTP daemon client for
// cross-system routing, or a SQL-backed Store for app-local messaging).
//
// Addresses are typed structs serialized as URNs on the wire:
//
//	msg://<kind>/<authority>/<id>[/<subid>]
//
// The Dispatcher type wraps any Store with request/reply semantics
// (Request blocks for a correlated response; Reply constructs the
// response envelope). Delivery is exactly-once-per-recipient via
// DeliveredAt and ConsumedAt tracking; see the README for the full
// lifecycle and the messagingtest sub-package for the shared contract
// test suite that every Store implementation should pass.
//
// The Router type is a Store decorator that routes operations by the
// Authority segment of each URN: a registered foreign authority is
// dispatched to that route's Store, every other authority falls through
// to a local Store. This gives any application federated messaging for
// free — a standalone install simply registers no foreign routes and
// runs fully locally. See the Router documentation and
// messagingtest.RunRouterContract.
package messaging
