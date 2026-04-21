# Sprint S04 — Polish + Ship v0.1.0

Epic: [Phase 1 — go-messaging v0.1.0](../epic-phase-1-go-messaging.md)

**Goal:** Replace the skeleton README with the full adoption doc, add the
GitHub Actions `check` workflow + `.golangci.yml` baseline, run `make check`
green locally, tag `v0.1.0`, and smoke-test external consumption via
`go get`. Also update the portfolio KB with a Messaging entry.

**Branch:** `feat/s04-polish-ship` (FF-merge + delete at close).
**Plan Tasks covered:** 15, 16, 17.
**Depends on:** S01 + S02 + S03 (everything that ships under `v0.1.0`).

## Exit criteria

- [x] `README.md` rewritten: install / quick start / address model / message Kinds / delivery semantics / how-to-write-a-Store-impl / scope / design-spec link / license.
- [x] `.github/workflows/check.yml` created; workflow name `check`, triggers on `push main` + `pull_request`, job runs fmt-verify + vet + golangci-lint + test-race + govulncheck.
- [x] `.golangci.yml` baseline added (errcheck, govet, ineffassign, staticcheck, unused, gofmt, goimports, misspell, unconvert).
- [x] `make check` green locally.
- [x] `v0.1.0` tag created and pushed (annotated, points at `c52ac47`).
- [x] GitHub Actions `check` workflow green on `main` (run `24738693281` on `7e2f693`; `v0.1.1` cut to bundle three CI fixes — action v6→v7, go.mod 1.26.1→1.22, setup-go→stable). Workflow isn't tag-triggered; green on the commit v0.1.1 points at.
- [x] External-module smoke (Task 17.6): `/tmp/msg-smoke/` + `/tmp/msg-smoke-v011/` both printed `response: {"pong":true}` against `@v0.1.0` and `@v0.1.1` respectively.
- [x] Portfolio KB updated (`agent-workspaces/knowledge/portfolio/composition-map.md` gains Messaging row; `shared-needs.md` cross-tool messaging line references this phase). Commit `d459faf` in agent-workspaces.

## Tasks

### T-s04-01 — README rewrite + doc polish

**Priority:** 1. **Tags:** docs. **Plan Task:** 15 (steps 15.1–15.2).

- [x] Overwrite `README.md` with the full body from plan Task 15.1:
  - Title + one-paragraph summary.
  - Status line.
  - `## Install` with `go get` command (use `@v0.1.0` even though tag lands in T-s04-03; keeps docs consistent with the shipped tag).
  - `## Quick start` with minimal Dispatcher example (5 lines).
  - `## Address model` — URN shape + examples.
  - `## Message Kinds` — all six `MsgKind*` values, one-liner each.
  - `## Delivery semantics` — 3-step CREATED/DELIVERED/CONSUMED walkthrough + at-least-once caveat.
  - `## Writing a new Store impl` — `messagingtest.RunContract` snippet.
  - `## Scope` — in-scope / out-of-scope bullets (mirrors epic exit criteria).
  - `## Design reference` — absolute path to spec.
  - `## License` — MIT.
- [x] `doc.go` from S01 T-01 is already sufficient; confirm (no change needed).
- [x] Commit: `docs: flesh out README with install, quick start, address model, semantics`

**Files:**
- Modify: `README.md`

### T-s04-02 — CI workflow + lint config + `make check` green

**Priority:** 1. **Tags:** ci, quality-gate. **Plan Task:** 16 (steps 16.1–16.4).

- [x] Create `.github/workflows/check.yml`:
  - `on: push: branches:[main]` + `pull_request:`
  - Steps: `actions/checkout@v4`, `actions/setup-go@v5` (Go 1.22), gofmt verify, `go vet`, `golangci/golangci-lint-action@v6` (version v2.1.6), `go test -race -count=1 ./...`, `go install golang.org/x/vuln/cmd/govulncheck@latest` + `govulncheck ./...`.
- [x] Create `.golangci.yml`:
  - `version: "2"`, `run.timeout: 5m`
  - `linters.enable`: errcheck, govet, ineffassign, staticcheck, unused, gofmt, goimports, misspell, unconvert
  - `linters.disable`: typecheck (go vet covers it; double-firing)
  - `issues.max-issues-per-linter: 0`, `max-same-issues: 0`
- [x] Run `make check` locally. Expected output includes `✓ go vet: ok`, golangci `0 issues`, tests pass under `-race`, `No vulnerabilities found`.
  - If `golangci-lint` is not installed locally, run the fallback sequence: `go fmt ./... && go vet ./... && go test -race -count=1 ./... && govulncheck ./...`. CI will still enforce lint.
- [x] Commit: `ci: GitHub Actions check workflow + golangci baseline`

**Files:**
- Create: `.github/workflows/check.yml`, `.golangci.yml`

**Gotchas:**
- `gofmt -l .` must emit zero lines. If fmt differences exist, `go fmt ./...`
  and amend.
- `golangci-lint-action@v6` pins its own Go toolchain; ensure the
  `setup-go@v5` step runs first or linter version drift confuses
  `go test` invocation later in the job.

### T-s04-03 — Tag v0.1.0, push, smoke test, KB update

**Priority:** 1. **Tags:** release, smoke, kb. **Plan Task:** 17 (steps 17.1–17.6).

**Gate:** `git status` clean. All prior commits on `main` after FF-merge of
prior sprints.

- [x] `git push -u origin main` — initial commit `a4aa5fb` pushed at session start; subsequent sprints pushed via FF-merge cycle.
- [x] `git tag v0.1.0 && git push origin v0.1.0` — annotated tag, points at `c52ac47`.
- [x] Verify GitHub Actions — v0.1.0's tag-push surfaced three CI-config bugs. Fixed in three commits on `main` and bundled as `v0.1.1` (library identical, no API change):
  - `fd1c96b` ci: bump `golangci-lint-action` v6→v7 (v2 linter requires v7 action).
  - `13e5da0` chore(go.mod): lower go directive `1.26.1`→`1.22` to match epic floor (unlocks golangci-lint v2.1.6 on module load).
  - `7e2f693` ci: pin `setup-go` to `stable` to dodge `GO-2025-3750` (unfixed on Go 1.22.12 patch branch).
  Run `24738693281` green on `7e2f693`.
- [x] Smoke test — both `/tmp/msg-smoke/` (`@v0.1.0`) and `/tmp/msg-smoke-v011/` (`@v0.1.1`) printed `response: {"pong":true}`.
- [x] Cleanup: `rm -rf /tmp/msg-smoke` + `/tmp/msg-smoke-v011` done.
- [x] **KB updates** (plan Task 17.5 — mandatory this time, not "optional"):
  - `knowledge/portfolio/composition-map.md` — added go-messaging row to shared SDKs with v0.1.0 + v0.1.1 status and consumer-adoption phases.
  - `knowledge/portfolio/shared-needs.md` — cross-tool messaging open-question now marked partially-resolved (addressing + delivery shipped; auth/ACL still open).
- [x] Commit `d459faf` in agent-workspaces (only these two KB files staged; pre-existing inbox / settings baseline left alone per `feedback_multi_session_default_is_baseline`).

**Files:**
- No files in go-messaging repo for the tag itself (tags are refs, not commits).
- Modify (in `agent-workspaces`): `knowledge/portfolio/composition-map.md`, `knowledge/shared-needs.md`.

## Scope fences

- Do NOT mint a `v0.2.0` or plan Phase 2 work during this sprint. That's a
  separate writing-plans session after `v0.1.0` is stable. See epic
  "Next phases" section.
- Do NOT push `go get` version bumps on consumers (agent-mux, nanite). They
  adopt in their own sprints (Phases 2–4).
- Do NOT add `CHANGELOG.md`. The tag + release notes on GitHub cover v0.1.0;
  a CHANGELOG belongs only once there's a v0.1.1 with something to diff.
- Do NOT open PRs against agent-mux or Nanite. Zero downstream changes this phase.

## Readiness checklist before closing the epic

- [x] All three tasks ticked and committed on `feat/s04-polish-ship` (T-01/T-02 on branch; T-03 executes post-merge on main).
- [x] Branch FF-merged into `main`, feature branch deleted (both local + remote).
- [x] `git log --oneline` on `main` shows sequential sprint closes S01 → S04 (S04 followed by three CI-hardening commits rolled into v0.1.1).
- [x] `v0.1.0` + `v0.1.1` tags visible on GitHub and locally (`git tag` lists both).
- [x] External smoke test printed `response: {"pong":true}`.
- [x] Epic exit criteria section checkboxes all tick.
- [x] KB updates committed in `agent-workspaces/` (`d459faf`).

## Review / gotchas

- **README install line ships before tag.** Plan has the README include
  `go get …@v0.1.0` before T-s04-03 actually creates the tag. That's fine;
  the tag lands in the same sprint and the doc lives in git history from
  T-s04-01 onward. Don't `v0.0.x`-placeholder it — the sprint is atomic from
  a release perspective.
- **First push gotcha.** If `origin` was never pushed in S01 (because the
  GitHub repo didn't exist), the push in T-s04-03 will be the first
  publication of any history. That means the entire commit chain (S01–S03)
  becomes public at that moment. Confirm with user if anything sensitive
  snuck in.
- **Smoke test directory.** `/tmp/msg-smoke` is ephemeral and isolated;
  the `go get …@v0.1.0` forces the module-proxy path. If the proxy hasn't
  indexed the tag yet, `GOPROXY=direct go get` bypasses the proxy. Last
  resort: `GOPROXY=off` with a local replace, though that undermines the
  smoke's value.
- **KB updates as a separate commit in a separate repo.** The agent-workspaces
  repo is distinct from the go-messaging repo. Per portfolio convention
  (memory `feedback_multi_session_default`), commit in every root touched.
  The KB commit does NOT belong in the go-messaging history.
- **Post-ship: no follow-ups expected.** If anything surfaces during ship
  that blocks `v0.1.0`, capture via `surface-discovery` — do not silently
  fix and re-tag. Clean v0.1.0 + deferred v0.1.1 is better than a moving tag.
