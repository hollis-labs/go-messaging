# Sprint S01 — Foundation

Epic: [Phase 1 — go-messaging v0.1.0](../epic-phase-1-go-messaging.md)

**Goal:** Scaffold the repo and land the four core types (`Address`, `Envelope`,
`Filter`, `Kind` enums) plus the `Store` / `Dispatcher` interfaces and error
sentinels. After this sprint, the package compiles, `go vet` and all unit
tests written here pass, and no `Store` implementation exists yet.

**Branch:** `feat/s01-foundation` (FF-merge + delete at close).
**Plan Tasks covered:** 1, 2, 3, 4, 5 (in the plan doc at
`agent-workspaces/docs/superpowers/plans/2026-04-20-messaging-phase-1-go-messaging.md`).

## Exit criteria

- [ ] `go.mod` initialized at `github.com/hollis-labs/go-messaging` with `google/uuid v1.6+`
- [ ] `LICENSE`, `README.md` (skeleton), `.gitignore`, `Makefile`, `doc.go` committed
- [ ] `messaging.go`: `AddressKind` + `Address` + `IsZero` + message `Kind` + `Channel` + `Envelope` + `Filter` + `Filter.Matches`
- [ ] `urn.go`: `Address.URN()` + `ParseURN(string)` with `ErrInvalidAddress`
- [ ] `store.go`: `Store` + `Dispatcher` interfaces + all error sentinels (`ErrNotFound`, `ErrRequestTimeout`, `ErrCanceled`, `ErrStoreUnavailable`, `ErrPresetLifecycle`)
- [ ] Address JSON (un)marshals as URN string (custom `MarshalJSON` / `UnmarshalJSON`)
- [ ] Envelope JSON round-trips cleanly: Payload inline (not base64), `DeliveredAt`/`ConsumedAt` always present (null when nil), `channel`/`thread_id`/`in_reply_to`/`metadata` `omitempty`
- [ ] `go test -race ./...` green (URN round-trip, Envelope JSON round-trip, Filter matching)
- [ ] `go vet ./...` clean

## Tasks

Each task here maps 1:1 to a numbered Task in the plan. Code blocks, test
bodies, and commit messages are in the plan — **do not improvise; transcribe**.
TDD rhythm throughout: write test → run (red) → implement → run (green) →
commit. Tick the checkbox here after the commit lands.

### T-s01-01 — Repository scaffolding

**Priority:** 1. **Tags:** scaffold, chore. **Plan Task:** 1 (steps 1.1–1.8).

- [ ] `go mod init github.com/hollis-labs/go-messaging`
- [ ] `go get github.com/google/uuid@v1.6.0`
- [ ] `LICENSE` (copy from `framework/libs/go-agentmux-client/LICENSE` verbatim)
- [ ] `.gitignore`, `Makefile` (runs `fmt vet lint test-race vuln`), `doc.go`, skeleton `README.md`
- [ ] Commit: `chore: initialize go-messaging repo`

**Readiness notes:**
- Repo is already `git init`'d. If step 1.1's `git init` errors, skip to `git remote add`.
- Remote repo `hollis-labs/go-messaging` on GitHub may not exist yet; if `git remote add origin …` fails or push later fails, create the GitHub repo and retry. Defer the push to Sprint 04.
- LICENSE source: `~/Projects-apps/framework/libs/go-agentmux-client/LICENSE`. Keep holder line as-is.

**Files (all new):**
- `go.mod`, `go.sum`
- `LICENSE`, `README.md`, `.gitignore`, `Makefile`, `doc.go`

### T-s01-02 — Address + URN round-trip

**Priority:** 1. **Tags:** types, urn. **Plan Task:** 2 (steps 2.1–2.6).

TDD:
- [ ] Write `urn_test.go`: `TestAddress_URN_RoundTrip` (6 cases: agent with/without subid, user, service, session, workflow) + `TestParseURN_Errors` (9 malformed inputs)
- [ ] Run tests → compile error (types undefined)
- [ ] Write `messaging.go` with `AddressKind` const block + `Address` struct + `IsZero()` method
- [ ] Write `urn.go` with `ErrInvalidAddress` + `urnScheme = "msg://"` + `validKinds` map + `URN()` + `ParseURN()`
- [ ] Tests green under `-race`
- [ ] Commit: `feat(messaging): Address type + URN parse/format round-trip`

**Reference details (spec §Core types, §Naming conventions):**
- Canonical form: `msg://<kind>/<authority>/<id>[/<subid>]`
- `AddressKind` values: `agent`, `user`, `service`, `session`, `workflow`
- `ParseURN` rejects: empty input, wrong scheme, unknown kind, empty parts, 0 or >4 path segments after scheme.

**Files:**
- Create: `messaging.go`, `urn.go`, `urn_test.go`

### T-s01-03 — Envelope + JSON round-trip + Address URN marshaling

**Priority:** 1. **Tags:** types, json. **Plan Task:** 3 (steps 3.1–3.5).

TDD:
- [ ] Write `messaging_test.go`: `TestEnvelope_JSONRoundTrip` + `TestEnvelope_JSONOmitEmpty` + `containsAll` helper
- [ ] Run → compile error (Envelope / `MsgKindRequest` undefined)
- [ ] Extend `messaging.go` with message `Kind` const block (`MsgKindRequest`/`MsgKindResponse`/`MsgKindNotice`/`MsgKindStatusUpdate`/`MsgKindHandoff`/`MsgKindEscalation`), `Channel` type, `Envelope` struct with JSON tags
- [ ] Add `Address.MarshalJSON` + `Address.UnmarshalJSON` (serialize as URN string, not as object)
- [ ] Tests green under `-race`
- [ ] Commit: `feat(messaging): Envelope type + JSON round-trip, Address URN marshaling`

**Envelope JSON tags (critical — the spec's wire contract):**
- `id`, `kind`, `from`, `to`, `created_at` — always present
- `channel`, `thread_id`, `in_reply_to`, `payload`, `content_type`, `metadata` — `omitempty`
- `delivered_at`, `consumed_at` — **NO** `omitempty`; always emit (null when nil). This is the spec's "always present, null when unset" promise.
- `Payload` is `json.RawMessage` — serializes inline, NOT as base64. Verify in test with string-contains.

**Files:**
- Modify: `messaging.go`
- Create: `messaging_test.go`

### T-s01-04 — Filter type + matching logic

**Priority:** 2. **Tags:** types. **Plan Task:** 4 (steps 4.1–4.5).

TDD:
- [ ] Append `TestFilter_Matches` to `messaging_test.go` (10 cases covering AND/OR/empty-matches-all)
- [ ] Run → compile error (`Filter` undefined)
- [ ] Append `Filter` struct + `Matches(Envelope) bool` to `messaging.go`
- [ ] Tests green
- [ ] Commit: `feat(messaging): Filter type + Matches helper with AND/OR semantics`

**Semantic rule (spec §Interfaces):**
- Set fields AND-combine. Within a slice field, values OR-combine.
  `Filter{Kind: [request, handoff], ThreadID: "T1"}` → thread T1 envelopes whose Kind is request OR handoff.
- Empty Filter matches everything.
- `Limit` is not enforced by `Matches` itself (that's Store impl territory).

**Files:**
- Modify: `messaging.go`, `messaging_test.go`

### T-s01-05 — Store + Dispatcher interfaces + error sentinels

**Priority:** 1. **Tags:** interfaces, contract. **Plan Task:** 5 (steps 5.1–5.3).

No tests this task (interfaces are exercised via impls in S02/S03).

- [ ] Create `store.go` with error sentinels (`ErrNotFound`, `ErrRequestTimeout`, `ErrCanceled`, `ErrStoreUnavailable`, `ErrPresetLifecycle`)
- [ ] `Store` interface: `Send`, `Get`, `Inbox`, `Thread`, `Consume`, `Cancel`, `Subscribe`
- [ ] `Dispatcher` interface (embeds `Store`): `Request`, `Reply`
- [ ] `go vet ./...` clean
- [ ] Commit: `feat(messaging): Store + Dispatcher interfaces + error sentinels`

**Interface docs are load-bearing — copy verbatim from plan §Task 5.1.** They
are the contract every Store impl in the portfolio must honor, including:
- `Send` assigns ID + CreatedAt; rejects caller-set `DeliveredAt`/`ConsumedAt` with `ErrPresetLifecycle`.
- `Inbox` is mutating (atomic delivery marking). `Thread` is read-only.
- `Subscribe` is live-only (no historical replay). Apps bootstrap via `Inbox`.
- `Consume` + `Cancel` are idempotent.

**Files:**
- Create: `store.go`

## Scope fences

- Do NOT implement any `Store` method in this sprint — interfaces only. S02 builds `memstore`.
- Do NOT write a `NewDispatcher` — S03's T-11 does.
- Do NOT add `net/http`, `database/sql`, or anything beyond stdlib + `google/uuid`.
- Do NOT rename `AddressKind` to `Kind` (spec's collision-resolution is `Kind*` consts on both, different struct fields). Keep `Kind` as the envelope-kind type per spec.

## Readiness checklist before S02 opens

- [ ] All five tasks ticked and committed.
- [ ] `go test -race ./...` green, `go vet ./...` clean.
- [ ] Branch FF-merged into `main`, feature branch deleted.
- [ ] Update epic `## Sprint map` if naming drifted.

## Review / gotchas

- **Envelope `Kind` field vs `Address.Kind` field.** Both use `Kind*` prefixed
  constants. They CANNOT collide at compile time (different const types,
  different field names) but code review should reference by field:
  `env.Kind == MsgKindRequest`, `addr.Kind == KindAgent`. Keep the const
  blocks in separate `const (...)` groups in `messaging.go`.
- **`Payload json.RawMessage` serialization.** `json.RawMessage` is `[]byte`
  under the hood but implements `json.Marshaler`/`Unmarshaler` to serialize
  inline. The `TestEnvelope_JSONRoundTrip` string-contains assertion on
  `"payload":{"action":"review",...}` is the guard — if that fails, you've
  accidentally used `[]byte` instead of `json.RawMessage`.
- **`Address` custom JSON.** Without the custom `MarshalJSON`/`UnmarshalJSON`,
  Go will serialize Address as `{"Kind":"agent",...}` which breaks wire
  interop. The round-trip test catches this.
- **`ParseURN` exact path-segment count check.** 3 or 4 segments only.
  Empty segments in the middle (e.g., `msg://agent//sess`) must also fail.
