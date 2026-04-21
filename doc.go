// Package messaging defines the shared contract for agent-to-agent,
// agent-to-service, and agent-to-user messaging across the hollis-labs
// portfolio.
//
// The package ships contract types (Envelope, Address, Kind, Channel),
// interfaces (Store, Dispatcher), and an in-memory reference Store
// implementation under memstore/. It is never itself a running service —
// concrete Store implementations live in consumer projects (agent-mux
// for cross-system routing, Nanite for app-local messages).
//
// See the design document at
// github.com/chrispian/agent-workspaces/docs/superpowers/specs/2026-04-20-messaging-design.md
// for full rationale.
package messaging
