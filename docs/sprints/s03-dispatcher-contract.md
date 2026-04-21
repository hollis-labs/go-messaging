# Sprint S03 — Dispatcher + contract suite + example

Epic: [Phase 1 — go-messaging v0.1.0](../epic-phase-1-go-messaging.md)

**Goal:** Land the `NewDispatcher` constructor with `Reply` + `Request`
helpers, publish the shared `messagingtest.RunContract` test suite that every
Store impl (including Phase 2's agent-mux SQLite/HTTP impl and Phase 4's
Nanite impl) will run against, and ship a runnable `example_test.go` demo.

**Branch:** `feat/s03-dispatcher-contract` (FF-merge + delete at close).
**Plan Tasks covered:** 11, 12, 13, 14.
**Depends on:** S01 (types/interfaces) + S02 (memstore as Dispatcher's wrapped Store).

## Exit criteria

- [x] `dispatcher.go`: `NewDispatcher(Store) Dispatcher` + `Reply` + `Request` (no panics, no stubs).
- [x] `dispatcher_test.go`: `Reply` field wiring + `Request` happy path + `Request` timeout + `Request` overwrites caller-set ID/Kind.
- [x] `messagingtest/contract.go`: `RunContract(t *testing.T, factory Factory)` — ~13 sub-tests covering every `Store` guarantee + 2 `Dispatcher` sub-tests.
- [x] `memstore/contract_test.go`: `TestMemstore_Contract` calls `messagingtest.RunContract` with a `memstore.New` factory; passes all sub-tests.
- [x] `example_test.go`: package-level `Example()` demonstrating Dispatcher Request/Reply round-trip with `// Output:` assertion.
- [x] `go test -race -count=1 ./...` green across the whole module.

## Tasks

### T-s03-01 — Dispatcher scaffold + Send pass-through + Reply helper

**Priority:** 1. **Tags:** dispatcher, reply. **Plan Task:** 11 (steps 11.1–11.5).

TDD:
- [x] Create `dispatcher_test.go` with package `messaging_test` (external test package to catch unexported-API drift). Import `memstore` for a concrete Store. Write `TestDispatcher_Reply_WiresFields`: send a parent request, call `Reply`, assert `Kind=response`, `InReplyTo=parent.ID`, `ThreadID` propagated, `From`/`To` swapped, payload preserved.
- [x] Run → compile error (`NewDispatcher` undefined)
- [x] Create `dispatcher.go`:
  - Unexported `dispatcher` struct embedding `Store`.
  - `NewDispatcher(s Store) Dispatcher` returns `&dispatcher{Store: s}`.
  - `Reply(ctx, parent, payload)` constructs response: `Kind=MsgKindResponse`, `From=parent.To`, `To=parent.From`, `ThreadID=parent.ThreadID`, `InReplyTo=parent.ID`, `ContentType="application/json"`, then `d.Send(ctx, resp)`.
  - `Request` method: panic stub — implemented in T-s03-02.
- [x] Test green
- [x] Commit: `feat(messaging): NewDispatcher + Reply helper (Request stubbed)`

**Files:**
- Create: `dispatcher.go`, `dispatcher_test.go`

**Why panic stub and not just missing method:** `Dispatcher` interface
requires `Request`; the package must compile. Panic message says "Request
not yet implemented (Task 12)" so any accidental call surfaces loudly.

### T-s03-02 — Dispatcher.Request with ctx timeout + response correlation

**Priority:** 1. **Tags:** dispatcher, request, timeout. **Plan Task:** 12 (steps 12.1–12.5).

TDD:
- [x] Extend `dispatcher_test.go` imports to include `errors` and `time` (merge into existing import block — do NOT add a second).
- [x] Append tests:
  - `TestDispatcher_Request_ReceivesResponse`: spawn a goroutine that `Subscribe`'s for `MsgKindRequest`, when one arrives auto-`Reply`. Main goroutine calls `Request`, asserts `Kind=response` and payload.
  - `TestDispatcher_Request_TimesOut`: ctx with 200ms deadline, no responder; expect `messaging.ErrRequestTimeout`.
  - `TestDispatcher_Request_OverwritesKindAndID`: call with caller-set `ID=`"caller-set" and `Kind=MsgKindNotice`; fetch via `Inbox(B)` afterward; assert `Kind=request` and `ID != "caller-set"`.
- [x] Replace `Request` panic stub with real impl:
  - Overwrite `env.Kind = MsgKindRequest`; clear `env.ID = ""` (force Store-assigned UUIDv7).
  - Subscribe BEFORE Send (otherwise fast responses race). Derive `subCtx, subCancel := context.WithCancel(ctx)`; defer `subCancel()`.
  - `sub, err := d.Store.Subscribe(subCtx, Filter{Kind: []Kind{MsgKindResponse}})` — filter on response only; match correlation after.
  - `sent, err := d.Store.Send(ctx, env)` — captures Store-assigned ID.
  - Loop: `select { case resp, ok := <-sub: if !ok { ctx deadline → ErrRequestTimeout else ctx.Err() } if resp.InReplyTo == sent.ID return resp, nil ; case <-ctx.Done(): deadline → ErrRequestTimeout else ctx.Err() }`.
- [x] Tests green
- [x] Commit: `feat(messaging): Dispatcher.Request with ctx timeout + response correlation`

**Files:**
- Modify: `dispatcher.go`, `dispatcher_test.go`

**Critical correlation detail (spec §Interfaces):** Subscribe must be
established BEFORE Send to avoid losing a fast responder. The subscription
filter narrows to `Kind=response`; correlation on `InReplyTo==sent.ID`
happens in the select loop. Ignore responses whose `InReplyTo` doesn't
match (some other request in the same Store); keep waiting.

**Timeout mapping:** `ctx.Err() == context.DeadlineExceeded` → `ErrRequestTimeout`.
Other ctx errors (cancel, etc.) pass through as-is. The spec §Failure modes
table is the reference.

### T-s03-03 — Shared contract test suite (`messagingtest.RunContract`)

**Priority:** 1. **Tags:** contract, testing, cross-impl. **Plan Task:** 13 (steps 13.1–13.4).

- [x] Create `messagingtest/contract.go` with package `messagingtest` (NOT `testing` — avoid stdlib collision). Define `Factory func(t *testing.T) messaging.Store` and `RunContract(t, factory)`.
- [x] Copy all sub-tests from plan Task 13.1 verbatim:
  - `Send assigns ID + CreatedAt`
  - `Send rejects preset lifecycle`
  - `Get returns ErrNotFound for missing`
  - `Inbox atomic delivery`
  - `Inbox chronological + tie-break`
  - `Consume sets ConsumedAt, idempotent`
  - `Cancel marks dead, idempotent`
  - `Cancel NotFound`
  - `Thread chronological, no side effects`
  - `Subscribe live-only`
  - `Subscribe ctx cancel closes channel`
  - `Dispatcher.Request round-trip`
  - `Dispatcher.Request times out`
- [x] Copy helpers (`basicEnv`, `withTo`, `recipient`, `must`) into the same file. Keeping them together so impl repos don't need to duplicate them.
- [x] Create `memstore/contract_test.go` with package `memstore_test`: `TestMemstore_Contract(t)` calls `messagingtest.RunContract(t, func(t *testing.T) messaging.Store { return memstore.New() })`.
- [x] `go test -race -count=1 ./...` all green — every memstore behavior verified twice (once via its own unit tests, once via the contract suite). This is intentional; the contract suite is the canonical guarantee.
- [x] Commit: `feat(messagingtest): shared Store contract suite + memstore conformance`

**Files:**
- Create: `messagingtest/contract.go`, `memstore/contract_test.go`

**Why the `messagingtest` package name exists:** Phase 2 (agent-mux's
concrete impl) and Phase 4 (Nanite) will each add `*_test.go` files that
call `messagingtest.RunContract`. That's the load-bearing primitive for
contract conformance across impls. Drift surfaces as sub-test failure in
*those* repos — impossible to miss.

### T-s03-04 — Example test (end-to-end demo)

**Priority:** 2. **Tags:** docs, example. **Plan Task:** 14 (steps 14.1–14.3).

- [x] Create `example_test.go` with package `messaging_test`. Implement `func Example()` per plan Task 14.1 verbatim:
  - Construct `memstore.New()` + `messaging.NewDispatcher(store)`.
  - `ctx` with 2s timeout.
  - Two addresses `A` and `B`.
  - Goroutine: `Subscribe` for requests → `Reply({"status":"ok"})` to first matching.
  - Main: `Request` from A → B; print `string(resp.Payload)`.
  - `// Output: got response: {"status":"ok"}` assertion line.
- [x] `go test -race -run Example ./...` green (godoc example runner asserts Output match).
- [x] Commit: `docs(example): end-to-end Request/Reply demo`

**Files:**
- Create: `example_test.go`

**Why `// Output:` not `// Unordered output:`** — single response, single
print; ordering is deterministic.

## Scope fences

- Do NOT add `Notify` / `Handoff` helper methods to `Dispatcher`. Spec
  §Interface design notes calls these future additions; v0.1 stays minimal.
- Do NOT generalize `RunContract` to take config options (`SkipSubtests`,
  etc.) until a real impl needs to skip one — YAGNI.
- Do NOT split helpers (`basicEnv`, `recipient`, `must`) into a separate
  file — keeping them co-located with `RunContract` was an explicit spec
  decision so consumer impls don't have to duplicate.
- Do NOT polish README or CI yet — that's S04.

## Readiness checklist before S04 opens

- [x] All four tasks ticked and committed on `feat/s03-dispatcher-contract`.
- [x] `go test -race -count=1 ./...` green across the whole module (package `messaging` + `memstore` + `messagingtest`).
- [x] `go test -race -run Example ./...` green (Output assertion holds).
- [x] `go vet ./...` clean.
- [ ] Branch FF-merged into `main`, feature branch deleted.

## Review / gotchas

- **Dispatcher embeds Store via interface, not pointer.** The `dispatcher`
  struct has `Store` (interface) as an embedded field so method promotion
  works. Calling `d.Send(...)` on the Dispatcher returned by `NewDispatcher`
  dispatches to the wrapped Store's `Send`. This is why `TestDispatcher_Reply`
  works without duplicating Store methods.
- **Subscribe race in `Request`.** Plan's ordering (`Subscribe` → `Send`)
  is load-bearing. Reversing it leaks a race: fast responder could publish
  the reply before the subscription channel is registered for fan-out.
- **`Request` ignoring non-matching responses.** If multiple in-flight
  `Request` calls use the same Store, each one's Subscribe sees *all*
  responses and filters on `InReplyTo`. That's correct and deliberate —
  `Store.Subscribe` doesn't have per-Request routing.
- **Two independent contract-suite invocations (memstore tests).**
  `memstore_test.go` has its own focused tests; `memstore/contract_test.go`
  runs `RunContract`. Redundant-by-design. Don't consolidate — the unit
  tests document expected memstore quirks, the contract tests enforce the
  portable guarantee.
- **Goroutine leak in `Request` timeout path.** The `defer subCancel()`
  + the `subCtx` being a child of `ctx` ensures the subscription janitor
  exits when Request returns. Verify via `-race` which catches goroutine
  leaks within a test's timeout; also rely on S02 T-05's tests for
  Subscribe-cleanup coverage.
