# Changelog

All notable changes to `go-messaging` are documented here. The format
is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).
While the major version is `0.x`, the API is considered pre-1.0 and
breaking changes may occur in minor (`0.y`) versions; they are called
out explicitly below.

## Unreleased

## v0.4.0 — 2026-09-05

### Added
- Optional `mailbox` subpackage for durable tuple-addressed inboxes with
  unread/read/resolved lifecycle, priority and thread queries, bounded recent
  history, in-process live subscriptions, caller identity, mailbox mutation
  events, handoff coordination, notification/wake hooks, and a host-migrated
  SQLite adapter. Host-owned event persistence, handoff transactions, agent
  registration, and asynchronous lifecycle behavior enter through narrow
  interfaces; the root `Store` contract is unchanged.
- Live fan-out gives the sender, each subscriber, and each hook independently
  owned message values (including reference fields), and `Service.Close` joins
  subscription cleanup even when callers use non-cancelable contexts.

## v0.3.0 — 2026-05-21

### Added
- `KindGroup` address kind for addressing a named collection of
  recipients.
- `Router` — an authority-routing `Store` decorator. It dispatches each
  operation by the URN `Authority` segment: a registered foreign authority
  goes to that route's `Store`, every other authority falls through to a
  local `Store`. This promotes federated messaging into the shared library
  so every consuming app gets it for free; a standalone install registers
  no foreign routes and runs fully locally with no extra configuration.
  - `NewRouter(local, localAuthority, opts...)` constructs one.
  - `Register` / `Unregister` / `Authorities` manage foreign routes
    (concurrency-safe; mutable while operations are in flight).
  - `IsLocal` answers the single "internal vs external" routing question.
  - `WithStrictRouting()` makes unknown authorities return `ErrNoRoute`
    instead of falling through to the local `Store`.
  - `Router` is itself a `Store`, so `NewDispatcher(router)` yields a
    federated request/reply `Dispatcher`.
- `ErrNoRoute` sentinel error, returned by a strict-mode `Router`.
- `messagingtest.RunRouterContract` — a shared contract suite covering the
  authority-routing guarantees (local fall-through, foreign dispatch,
  recipient-keyed routing, strict-mode `ErrNoRoute`), which also asserts a
  `Router` is itself a contract-conformant `Store`.

## v0.2.1 — 2026-05-10

### Changed
- Public-release prep. Documentation and metadata polish only — there
  are no public API changes from `v0.2.0`.
- `README.md` rewritten for an external audience (install snippet,
  godoc badge/link, CHANGELOG link, neutralized example URNs).
- Package-level `doc.go` expanded so `pkg.go.dev` renders a useful
  overview.
- Test fixtures use generic authority names (`app`, `router`,
  `scheduler`) instead of internal project names.

### Added
- `CHANGELOG.md` (this file). Entries for `v0.1.0`, `v0.1.1`, and
  `v0.2.0` are backfilled from git history.
- `examples/request-reply/` — standalone runnable program
  demonstrating the canonical `Dispatcher.Request` / `Dispatcher.Reply`
  flow against the in-memory reference Store.

### Removed
- Internal planning artifacts under `docs/` (epic and sprint notes)
  that were never part of the public API surface.

## v0.2.0 — 2026-04-21

### Changed (BREAKING)
- `Store.Subscribe` now takes an explicit recipient address:

  ```go
  // Before (v0.1.x):
  Subscribe(ctx context.Context, f Filter) (<-chan Envelope, error)

  // After (v0.2.0):
  Subscribe(ctx context.Context, to Address, f Filter) (<-chan Envelope, error)
  ```

  The change brings `Subscribe` into symmetry with `Inbox(ctx, to, f)`
  and lets HTTP-backed `Store` implementations scope an SSE stream to a
  single recipient without leaking traffic across agents.

  **Migration:** pass the subscriber's own `Address` as the new `to`
  argument. The `Dispatcher.Request` helper has been updated to do this
  internally; direct `Store.Subscribe` callers must update their call
  sites.

## v0.1.1 — 2026-04-21

CI / build-tooling fixes only. No library code changes.

### Fixed
- `go.mod` `go` directive lowered to `1.22` to match the declared
  minimum Go version (the module had drifted to a higher floor than
  intended).
- `golangci-lint-action` bumped to `v7` for `golangci-lint v2`
  compatibility.
- `actions/setup-go` pinned to `stable` so `govulncheck` runs against a
  patched Go release.

## v0.1.0 — 2026-04-21

Initial release. Phase 1 of the messaging contract: a pure-Go library
with no runtime, no HTTP, no SQL.

### Added
- Core types: `Envelope`, `Address` (+ canonical URN form
  `msg://<kind>/<authority>/<id>[/<subid>]`), `AddressKind`, `Kind`,
  `Channel`, `Filter`.
- Address JSON marshaling round-trips through the URN form.
- `Filter.Matches` with AND-across-fields, OR-within-slice semantics.
- Interfaces: `Store` (`Send`, `Get`, `Inbox`, `Thread`, `Consume`,
  `Cancel`, `Subscribe`) and `Dispatcher` (extends `Store` with
  `Request` and `Reply`).
- Error sentinels: `ErrNotFound`, `ErrRequestTimeout`, `ErrCanceled`,
  `ErrStoreUnavailable`, `ErrPresetLifecycle`, `ErrInvalidAddress`.
- `memstore` — in-memory reference `Store` implementation. Atomic
  per-recipient delivery marking, idempotent `Consume`/`Cancel`,
  read-only `Thread` queries, live-only `Subscribe` fan-out with
  context-driven cleanup.
- `messagingtest.RunContract` — shared contract test suite that any
  third-party `Store` implementation can run against itself.
- `Dispatcher.NewDispatcher` wrapping any `Store` with `Request` and
  `Reply` helpers; `Request` correlates responses by `InReplyTo` and
  honours context deadlines.
- `example_test.go` — runnable end-to-end Request/Reply demo (visible
  on pkg.go.dev).
- GitHub Actions `check` workflow: `gofmt`, `go vet`, `golangci-lint`,
  `go test -race`, `govulncheck`.
