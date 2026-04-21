# go-messaging — execution plan

Shared messaging contract for the `hollis-labs` agent portfolio. This folder is
the execution-planning surface for **Phase 1** of the four-phase rollout.

> Phase 1 ships the shared package itself: types + interfaces + in-memory
> reference `Store` + `Dispatcher` + contract test suite. No HTTP, no SQL, no
> runtime services. Tag `v0.1.0` and go home.

## Read first (authoritative sources)

| Doc | Role |
|---|---|
| [`epic-phase-1-go-messaging.md`](./epic-phase-1-go-messaging.md) | Epic summary, scope fences, exit criteria, sprint index |
| [Spec (`2026-04-20-messaging-design.md`)](../../../../agent-workspaces/docs/superpowers/specs/2026-04-20-messaging-design.md) | **Authoritative design.** Addressing, types, delivery lifecycle, phasing, migration shape |
| [Plan (`2026-04-20-messaging-phase-1-go-messaging.md`)](../../../../agent-workspaces/docs/superpowers/plans/2026-04-20-messaging-phase-1-go-messaging.md) | Task-by-task implementation plan with full code blocks, tests, commit cadence |

The spec and the plan are the source of truth. Sprint files in `sprints/` are
a **decomposition layer** — they point into the plan's numbered Tasks (1–17)
and the spec's sections. They do not re-duplicate the code.

## Repo location

Implementation lives in this repo: **`~/Projects-apps/framework/libs/go-messaging/`**.

Module path: **`github.com/hollis-labs/go-messaging`**.

This is a new, empty-but-initialized Go repo (sibling of `go-agentmux-client`,
`go-providers`, `go-otel`, etc. under `framework/libs/`). Only `.git/` exists
today; Sprint 01 T-01 populates scaffolding.

## Sprint index

| # | File | Tasks (plan §) | Scope |
|---|---|---|---|
| 01 | [`sprints/s01-foundation.md`](./sprints/s01-foundation.md) | Tasks 1–5 | Repo scaffold, Address+URN, Envelope+JSON, Filter, Store/Dispatcher interfaces |
| 02 | [`sprints/s02-memstore.md`](./sprints/s02-memstore.md) | Tasks 6–10 | In-memory `Store` impl: Send/Get, Inbox, Consume/Cancel, Thread, Subscribe |
| 03 | [`sprints/s03-dispatcher-contract.md`](./sprints/s03-dispatcher-contract.md) | Tasks 11–14 | Dispatcher Request/Reply + `messagingtest.RunContract` + example_test.go |
| 04 | [`sprints/s04-polish-ship.md`](./sprints/s04-polish-ship.md) | Tasks 15–17 | README, GitHub Actions, `v0.1.0` tag + smoke test |

Sprints are **strictly sequential**: S02 imports types from S01; S03 wraps the
S02 memstore; S04 documents and ships what S01–S03 built.

## How execution sessions should work

Driven by `superpowers:executing-plans` or `superpowers:subagent-driven-development`.

1. Read the epic. Pick the lowest-numbered sprint with any un-ticked task.
2. Read the sprint file end-to-end.
3. Open the referenced plan Tasks (by number) — those hold the code blocks.
4. Work task-by-task, TDD throughout: write the failing test, run it (red),
   implement, run (green), commit with the message in the plan. Tick the
   checkbox in the sprint file as each task lands.
5. `make check` (or the local equivalent — Sprint 04 installs the target)
   must be green before closing the sprint.
6. At sprint close: ensure all task checkboxes in the sprint file are ticked,
   ensure commits landed on `main` (branch workflow: one branch per sprint,
   FF-merge + delete at close — matches portfolio convention; see
   `feedback_sprint_merge_cleanup` memory).

## Out of scope for Phase 1 (do not silently expand)

The spec pins these scope fences. Phase 1 **does not** ship:

- Any HTTP client or server code.
- Any SQL, SQLite, or persistent storage impl.
- Auth, identity, ACL, caller-trust enforcement.
- Retry/backoff, DLQ, at-least-once semantics.
- Tracing / OTel hooks.
- Escalation or notice routing logic (the `Kind`s exist; routing lives in consumers).
- Cross-daemon federation, global address namespace.
- Schema versioning in envelope.
- Binary / large-payload handling beyond `json.RawMessage`.

If any of the above looks load-bearing during execution, stop and surface
via the `surface-discovery` skill rather than folding it in.

## Next phases (not planned here)

After `v0.1.0` tags, separate writing-plans sessions will plan:

- **Phase 2** — agent-mux unparks `v003-05 Mailbox Semantics` as the
  concrete external-runtime `Store` impl over its daemon HTTP surface.
- **Phase 3** — `go-agentmux-client` adopts the shared types and exposes
  `AsStore() messaging.Store` (breaking `v0.2.0`).
- **Phase 4** — Nanite migrates its `internal/messaging/` to the shared
  contract with a local `Store` impl + UX projection tables.

Phase 1 blocks nothing except downstream adoption; it does not require any
of the consumer changes to exist.
