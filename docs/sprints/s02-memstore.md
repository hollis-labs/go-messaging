# Sprint S02 — memstore (in-memory reference Store)

Epic: [Phase 1 — go-messaging v0.1.0](../epic-phase-1-go-messaging.md)

**Goal:** Build `memstore.New()` — the in-memory reference implementation of
`messaging.Store`. By sprint close, every method in `Store` is implemented and
unit-tested; the contract test suite comes in S03 and will validate coverage.
Primary usage: tests and embedded (process-local) agents.

**Branch:** `feat/s02-memstore` (FF-merge + delete at close).
**Plan Tasks covered:** 6, 7, 8, 9, 10.
**Depends on:** S01 (Store interface must exist). S02 cannot start until S01 is merged.

## Exit criteria

- [ ] `memstore/memstore.go` implements every `messaging.Store` method (no stubs, no panics).
- [ ] `memstore/memstore_test.go` covers: Send (ID assignment, caller-ID overwrite, ErrPresetLifecycle rejection), Get (found, NotFound), Inbox (undelivered only, chronological, Kind filter, per-recipient isolation), Consume (sets ConsumedAt, idempotent), Cancel (marks dead, idempotent, NotFound), Thread (chronological, no delivery side-effects), Subscribe (live-only, filters apply, ctx-cancel closes channel, multi-subscriber).
- [ ] `go test -race -count=1 ./memstore/...` green (no goroutine leaks from `Subscribe`).
- [ ] `go vet ./...` clean.
- [ ] `memstore.New()` returns `*memstore.Store`, which satisfies `messaging.Store` as a method-set check (compile-time assertion recommended — add `var _ messaging.Store = (*Store)(nil)` in the package).

## Tasks

TDD throughout. Each task adds tests first (red), then implementation, then
commits. Code is in the plan — **transcribe, don't improvise**.

### T-s02-01 — Send + Get scaffolding

**Priority:** 1. **Tags:** memstore, core. **Plan Task:** 6 (steps 6.1–6.5).

TDD:
- [ ] Write `memstore/memstore_test.go` with `TestMemstore_Send_AssignsID`, `TestMemstore_Send_OverwritesCallerID`, `TestMemstore_Send_RejectsPresetLifecycle`, `TestMemstore_Get`, `TestMemstore_Get_NotFound` + `newAddr` helper
- [ ] Run → compile error (`memstore.New` undefined)
- [ ] Write `memstore/memstore.go`:
  - Package comment: in-memory; tests + embedded use; non-durable.
  - `Store` struct: `mu sync.Mutex`, `envelopes map[string]*memEnvelope`, `subscribers []*subscription`, `canceled map[string]bool`
  - `memEnvelope` inner type: `env messaging.Envelope`, `delivered map[string]time.Time` (URN → time), `consumed map[string]time.Time`
  - `New() *Store`
  - `Send` assigns `uuid.NewV7().String()` + `time.Now().UTC()`, rejects preset `DeliveredAt`/`ConsumedAt` with `messaging.ErrPresetLifecycle`, calls `fanOut` (stub for now)
  - `Get` returns defensive copy via `copyEnvelope` helper (deep-copies pointer time fields)
  - Stub out `Inbox`, `Thread`, `Consume`, `Cancel`, `Subscribe` returning `"not yet implemented"` errors so the interface is satisfied as of this task
- [ ] Tests green
- [ ] Commit: `feat(memstore): Send + Get with ID assignment + lifecycle rejection`

**Why the stubs:** later tasks replace each stub one-by-one with real impls.
Keeps `memstore.Store` satisfying `messaging.Store` after every commit — so
downstream S03 work that wires Dispatcher against memstore isn't blocked if
S02 tasks land incrementally.

**Files:**
- Create: `memstore/memstore.go`, `memstore/memstore_test.go`

### T-s02-02 — Inbox with atomic per-recipient delivery

**Priority:** 1. **Tags:** memstore, core, delivery-lifecycle. **Plan Task:** 7 (steps 7.1–7.5).

TDD:
- [ ] Append tests: `TestMemstore_Inbox_ReturnsUndelivered` (second call returns zero), `TestMemstore_Inbox_ChronologicalOrder`, `TestMemstore_Inbox_FilterByKind`, `TestMemstore_Inbox_RespectsRecipient`
- [ ] Replace `Inbox` stub with real impl:
  - Lock `s.mu` for the whole op (atomic "collect + mark delivered").
  - Filter: `m.env.To.URN() == to.URN()` AND not already in `m.delivered[toURN]` AND `f.Matches(m.env)`.
  - Sort by `CreatedAt` ASC; tie-break on `ID` (UUIDv7 is monotonic so IDs tiebreak correctly).
  - Apply `f.Limit` if > 0.
  - For each result: set `m.delivered[toURN] = now`; return copy with `DeliveredAt = &now`.
  - Add helper `sortByCreatedAtAndID(xs []*memEnvelope)` using `sort.Slice`.
- [ ] Add `"sort"` to imports.
- [ ] Tests green
- [ ] Commit: `feat(memstore): Inbox with atomic per-recipient delivery marking`

**Files:**
- Modify: `memstore/memstore.go`, `memstore/memstore_test.go`

**Critical semantic (spec §Delivery lifecycle):** Inbox is the atomic
"collect + mark delivered" operation. Same envelope on second call must NOT
reappear. Per-recipient tracking via URN key — two recipients with same
Address means both get their own tracking slot.

### T-s02-03 — Consume + Cancel (both idempotent)

**Priority:** 1. **Tags:** memstore, core. **Plan Task:** 8 (steps 8.1–8.5).

TDD:
- [ ] Append tests: `TestMemstore_Consume`, `TestMemstore_Consume_Idempotent`, `TestMemstore_Cancel`, `TestMemstore_Cancel_NotFound`
- [ ] Replace `Consume` stub:
  - Return `messaging.ErrNotFound` if envelope ID unknown.
  - If `recipient.URN()` already in `m.consumed`, return nil (idempotent).
  - Set `m.consumed[rURN] = now`, mirror onto `m.env.ConsumedAt = &now` so `Get` reflects consumption.
- [ ] Replace `Cancel` stub:
  - Return `messaging.ErrNotFound` if envelope ID unknown.
  - `s.canceled[id] = true` (idempotent — map overwrite is fine).
- [ ] Tests green
- [ ] Commit: `feat(memstore): Consume + Cancel with idempotency`

**Files:**
- Modify: `memstore/memstore.go`, `memstore/memstore_test.go`

**Cancel semantic (spec §Failure modes):** Cancel marks an envelope dead so
in-flight `Dispatcher.Request` waits resolve with `ErrCanceled`. The actual
wait-resolution lives in S03's Dispatcher impl — memstore just records the
cancel bit. Late responses matching a canceled request get logged-and-dropped
(actually, "not routed to any waiter" — memstore has no waiter registry;
Dispatcher handles correlation).

### T-s02-04 — Thread (read-only chronological query)

**Priority:** 2. **Tags:** memstore, queries. **Plan Task:** 9 (steps 9.1–9.5).

TDD:
- [ ] Append tests: `TestMemstore_Thread_ReturnsChronological`, `TestMemstore_Thread_NoSideEffects` (Inbox after Thread still returns — proving Thread didn't mutate delivery state)
- [ ] Replace `Thread` stub:
  - Filter: `m.env.ThreadID == threadID` AND `f.Matches(m.env)`
  - Sort by `CreatedAt` + tie-break on ID (reuse `sortByCreatedAtAndID`)
  - Apply `f.Limit`
  - Return copies via `copyEnvelope`
  - **NO** mutation of `m.delivered` or `m.consumed`.
- [ ] Tests green
- [ ] Commit: `feat(memstore): Thread read-only queries`

**Files:**
- Modify: `memstore/memstore.go`, `memstore/memstore_test.go`

### T-s02-05 — Subscribe (live stream with ctx-driven cleanup)

**Priority:** 1. **Tags:** memstore, subscribe, goroutines. **Plan Task:** 10 (steps 10.1–10.5).

TDD:
- [ ] Append tests: `TestMemstore_Subscribe_LiveOnly` (historical envelope must not replay), `TestMemstore_Subscribe_FiltersApply`, `TestMemstore_Subscribe_CtxCancelClosesChannel`, `TestMemstore_Subscribe_MultipleSubscribers`
- [ ] Replace `subscription` placeholder struct with `{ch chan messaging.Envelope; filter messaging.Filter; ctx context.Context}`
- [ ] Replace `Subscribe` stub:
  - Make buffered channel (`make(chan messaging.Envelope, 16)`).
  - Append subscription to `s.subscribers` under lock.
  - Spawn janitor goroutine: `<-ctx.Done()` → under lock, remove from `s.subscribers` slice, close `sub.ch`.
- [ ] Replace `fanOut` placeholder with real impl:
  - Snapshot subscribers under lock, release lock before send (don't hold lock across channel ops).
  - For each matching subscriber, non-blocking `select`: send OR `<-sub.ctx.Done()` (skip going-away) OR `default` (drop — `Subscribe` is best-effort; `Inbox` is durable).
- [ ] Tests green under `-race -count=1` (confirms no goroutine leak).
- [ ] Commit: `feat(memstore): Subscribe with live-only fan-out and ctx-driven cleanup`

**Files:**
- Modify: `memstore/memstore.go`, `memstore/memstore_test.go`

**Concurrency notes:**
- Don't hold `s.mu` across channel sends — deadlock risk if a subscriber's
  `Subscribe` janitor also needs the lock.
- `fanOut` drops rather than blocks because memstore's delivery guarantee is
  `Inbox`, not `Subscribe`. This matches the spec: Subscribe is a live hint,
  not a durable queue.

## Scope fences

- Do NOT implement `Dispatcher` or `Reply` / `Request` — that's S03.
- Do NOT add persistent file storage, WAL, or snapshot — memstore is
  explicitly non-durable (process lifetime).
- Do NOT invent new Store methods beyond the interface — extensions belong
  in a superseding ADR, not in v0.1.
- Do NOT share `canceled` state with `Dispatcher` via global/package-level
  anything. Correlation between canceled requests and in-flight waits is
  Dispatcher's job in S03.

## Readiness checklist before S03 opens

- [ ] All five tasks ticked and committed on `feat/s02-memstore`.
- [ ] `go test -race -count=1 ./memstore/...` green.
- [ ] `go vet ./...` clean.
- [ ] Optional: `var _ messaging.Store = (*memstore.Store)(nil)` compile-time
      assertion added to `memstore/memstore.go`. (Belt-and-suspenders for
      interface satisfaction — will prevent S03 surprises.)
- [ ] Branch FF-merged into `main`, feature branch deleted.

## Review / gotchas

- **Defensive copy on `Get`.** Plan uses `copyEnvelope` which deep-copies
  pointer time fields (`DeliveredAt`, `ConsumedAt`) but not `Metadata`.
  Memstore never mutates returned metadata maps; if callers need to mutate,
  they should clone their own. Document this if it comes up.
- **`sort.Slice` stability.** `sort.Slice` is not stable, but the tie-break
  on ID makes ordering deterministic regardless. Don't switch to
  `sort.SliceStable` — the tie-break is the explicit ordering.
- **`time.Now().UTC()`.** Always `.UTC()`. Mixed timezone bugs are a
  classic source of JSON serialization drift.
- **Subscribe channel buffer size (16).** Matches plan. Don't tune without
  a reason — the value is "comfortably buffered for bursts, still small
  enough that slow consumers drop promptly." If a consumer needs more,
  they should build their own reader goroutine that drains into their
  own queue.
- **Two-recipients-same-Address edge case (spec §Failure modes).** Both
  recipients get their own `m.delivered[URN]` slot; whichever calls Inbox
  first gets the envelope. Contract says "use distinct SubIDs to avoid
  if it matters" — no special handling needed in memstore.
