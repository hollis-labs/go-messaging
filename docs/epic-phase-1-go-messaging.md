# Epic — go-messaging Phase 1 (`v0.1.0`)

**Target tag:** `github.com/hollis-labs/go-messaging@v0.1.0`
**Work dir:** `~/Projects-apps/framework/libs/go-messaging/`
**Module:** `github.com/hollis-labs/go-messaging`
**Status:** not started (repo is `git init`'d; no code yet)
**Source spec:** `~/Projects-apps/agent-workspaces/docs/superpowers/specs/2026-04-20-messaging-design.md`
**Source plan:** `~/Projects-apps/agent-workspaces/docs/superpowers/plans/2026-04-20-messaging-phase-1-go-messaging.md` (17 numbered Tasks — each sprint below references these by number)

## Goal

Ship `go-messaging v0.1.0` — a pure-Go contract library every consumer project
(agent-mux, Nanite, future apps) imports to get identical types, interfaces,
and delivery semantics for agent messaging across the portfolio.

Pure library. No runtime. No HTTP. No SQL. The only executable code it ships
is `memstore` (in-memory reference `Store`) for tests and embedded use.

## What's in the package

| Layer | Files | Purpose |
|---|---|---|
| Core types | `messaging.go`, `urn.go` | `Envelope`, `Address` (+ `URN()` / `ParseURN`), `AddressKind`, `Kind`, `Channel`, `Filter` |
| Interfaces | `store.go` | `Store`, `Dispatcher`, error sentinels |
| Dispatcher impl | `dispatcher.go` | `NewDispatcher(Store)` — wraps any Store with `Request` + `Reply` |
| Reference Store | `memstore/memstore.go` | In-memory Store satisfying the contract |
| Contract suite | `messagingtest/contract.go` | `RunContract(t, factory)` — every Store impl runs it |
| Docs/examples | `README.md`, `doc.go`, `example_test.go` | Quick start + runnable end-to-end demo |
| CI | `Makefile`, `.github/workflows/check.yml`, `.golangci.yml` | fmt + vet + lint + test-race + vuln gate |

**Naming note** (plan §1 preamble): the contract test package is
`messagingtest` (not `testing`) to avoid stdlib name collision at import
sites. The directory is `messagingtest/`, NOT `testing/` as the spec loosely
describes.

**Kind constant naming** (plan §Task 3, spec §Naming conventions): two
concepts share the `Kind*` prefix but hit different fields — `AddressKind`
(on `Address.Kind`) uses `KindAgent`/`KindUser`/etc.; envelope `Kind`
(on `Envelope.Kind`) uses `MsgKindRequest`/`MsgKindResponse`/etc. Keep the
const blocks separate.

## Scope fences (lifted from spec — must not drift)

**In:** contract types, interfaces, delivery lifecycle, URN addressing,
in-memory reference Store, contract test suite, Dispatcher request/reply.

**Out:**
- HTTP, SQL, file storage
- Auth/authz/ACL/caller-trust
- Retry/backoff, DLQ, at-least-once
- Tracing / OTel hooks
- Escalation routing, cross-daemon federation
- Schema versioning field, large-binary payloads
- Any consumer adoption (agent-mux, go-agentmux-client, Nanite migrations) — those are Phases 2–4

Surface scope discoveries via `superpowers:surface-discovery`. Do not absorb.

## Exit criteria (epic-level)

- [ ] `github.com/hollis-labs/go-messaging v0.1.0` tagged and pushed
- [ ] GitHub Actions `check` workflow green on `main`
- [ ] `make check` green locally (fmt + vet + lint + test-race + vuln)
- [ ] `messagingtest.RunContract` passes against `memstore` (all ~13 sub-tests)
- [ ] `example_test.go` runnable demo prints the canonical round-trip output
- [ ] Smoke: external module can `go get …@v0.1.0`, construct a Dispatcher,
      complete a Request/Reply in <30 LoC (Task 17.6)
- [ ] KB delta: `agent-workspaces/knowledge/portfolio/composition-map.md`
      gains a "Messaging" row; `shared-needs.md` cross-tool messaging line
      references this phase as the resolution path (Task 17.5)

## Sprint map

| Sprint | Plan Tasks | Output | Branch |
|---|---|---|---|
| [S01 Foundation](./sprints/s01-foundation.md) | 1, 2, 3, 4, 5 | Scaffold + core types + interfaces compile clean | `feat/s01-foundation` |
| [S02 memstore](./sprints/s02-memstore.md) | 6, 7, 8, 9, 10 | `memstore.New()` satisfies `messaging.Store` | `feat/s02-memstore` |
| [S03 Dispatcher + contract](./sprints/s03-dispatcher-contract.md) | 11, 12, 13, 14 | `NewDispatcher` + `RunContract` + example | `feat/s03-dispatcher-contract` |
| [S04 Polish + ship](./sprints/s04-polish-ship.md) | 15, 16, 17 | README, CI, `v0.1.0` tag | `feat/s04-polish-ship` |

Sprints run strictly in order. FF-merge + delete branch at each sprint close
(portfolio convention — memory `feedback_sprint_merge_cleanup`).

## Dependencies

- Go 1.22+
- `github.com/google/uuid v1.6+` (UUIDv7 support) — only runtime dep
- Dev-time: `golangci-lint`, `govulncheck` (the Makefile handles
  `go install` for the latter)

## Risk / readiness notes

- **Repo remote.** Repo is `git init`'d locally; GitHub repo
  `hollis-labs/go-messaging` may not yet exist. S01 T-01 adds the remote;
  if push fails at S04, create the repo on GitHub, then re-run.
- **UUIDv7 monotonicity.** Plan relies on UUIDv7's monotonic property for
  Inbox tie-breaking. `github.com/google/uuid v1.6.0` supports `uuid.NewV7()`.
  Verify version pin in S01 T-01 `go.mod`.
- **Plan already has full code.** Execution agents should mostly transcribe
  from the plan, not reinvent. Drift = rework. If something in the plan
  looks wrong during execution, surface it — don't silently "fix."
- **Phase 1 is library-only.** The parked `v003-05` sprint in agent-mux is
  NOT unparked by this work. That's Phase 2.

## Pointers

- Portfolio `framework/libs/` conventions: see sibling
  `~/Projects-apps/framework/libs/go-agentmux-client/` for LICENSE shape,
  `Makefile`, `README.md` structure, `.gitignore`.
- Go ecosystem baseline: memory `feedback_go_ecosystem_baseline` —
  `go vet` + `-race` + `golangci-lint` + `govulncheck` is the portfolio gate.
- Compat/shim policy: pre-launch, no aliases, no re-exports, no deprecation
  shims (memory `feedback_no_compat_shims`). If renaming during execution,
  rename wholesale.
