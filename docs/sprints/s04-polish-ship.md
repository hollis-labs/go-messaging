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

- [ ] `README.md` rewritten: install / quick start / address model / message Kinds / delivery semantics / how-to-write-a-Store-impl / scope / design-spec link / license.
- [ ] `.github/workflows/check.yml` created; workflow name `check`, triggers on `push main` + `pull_request`, job runs fmt-verify + vet + golangci-lint + test-race + govulncheck.
- [ ] `.golangci.yml` baseline added (errcheck, govet, ineffassign, staticcheck, unused, gofmt, goimports, misspell, unconvert).
- [ ] `make check` green locally.
- [ ] `v0.1.0` tag created and pushed.
- [ ] GitHub Actions run for the tag push is green.
- [ ] External-module smoke (Task 17.6): clean `/tmp/msg-smoke/` module `go get`'s `@v0.1.0` and runs a Dispatcher Request/Reply; prints `response: {"pong":true}`.
- [ ] Portfolio KB updated (`agent-workspaces/knowledge/portfolio/composition-map.md` gains Messaging row; `shared-needs.md` cross-tool messaging line references this phase).

## Tasks

### T-s04-01 — README rewrite + doc polish

**Priority:** 1. **Tags:** docs. **Plan Task:** 15 (steps 15.1–15.2).

- [ ] Overwrite `README.md` with the full body from plan Task 15.1:
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
- [ ] `doc.go` from S01 T-01 is already sufficient; confirm (no change needed).
- [ ] Commit: `docs: flesh out README with install, quick start, address model, semantics`

**Files:**
- Modify: `README.md`

### T-s04-02 — CI workflow + lint config + `make check` green

**Priority:** 1. **Tags:** ci, quality-gate. **Plan Task:** 16 (steps 16.1–16.4).

- [ ] Create `.github/workflows/check.yml`:
  - `on: push: branches:[main]` + `pull_request:`
  - Steps: `actions/checkout@v4`, `actions/setup-go@v5` (Go 1.22), gofmt verify, `go vet`, `golangci/golangci-lint-action@v6` (version v1.60), `go test -race -count=1 ./...`, `go install golang.org/x/vuln/cmd/govulncheck@latest` + `govulncheck ./...`.
- [ ] Create `.golangci.yml`:
  - `version: "2"`, `run.timeout: 5m`
  - `linters.enable`: errcheck, govet, ineffassign, staticcheck, unused, gofmt, goimports, misspell, unconvert
  - `linters.disable`: typecheck (go vet covers it; double-firing)
  - `issues.max-issues-per-linter: 0`, `max-same-issues: 0`
- [ ] Run `make check` locally. Expected output includes `✓ go vet: ok`, golangci `0 issues`, tests pass under `-race`, `No vulnerabilities found`.
  - If `golangci-lint` is not installed locally, run the fallback sequence: `go fmt ./... && go vet ./... && go test -race -count=1 ./... && govulncheck ./...`. CI will still enforce lint.
- [ ] Commit: `ci: GitHub Actions check workflow + golangci baseline`

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

- [ ] `git push -u origin main`. If remote doesn't exist, create
  `hollis-labs/go-messaging` on GitHub first. **This is a user-visible
  action** — pushing a new public repo — confirm with user before running
  if this is the first push.
- [ ] `git tag v0.1.0 && git push origin v0.1.0`
- [ ] Verify GitHub Actions: visit `https://github.com/hollis-labs/go-messaging/actions`. Check workflow green on the tag push. Re-run if transient.
- [ ] Smoke test (plan Task 17.6):
  ```bash
  cd /tmp && mkdir msg-smoke && cd msg-smoke
  go mod init smoke
  go get github.com/hollis-labs/go-messaging@v0.1.0
  # (cat plan's main.go into ./main.go)
  go run .
  ```
  Expected stdout: `response: {"pong":true}`. If `go get` fails with
  "no matching versions for query", the tag hasn't propagated — retry
  after a minute, or verify `git ls-remote --tags origin` shows `v0.1.0`.
- [ ] Cleanup: `rm -rf /tmp/msg-smoke`.
- [ ] **KB updates** (plan Task 17.5 — mandatory this time, not "optional"):
  - `~/Projects-apps/agent-workspaces/knowledge/portfolio/composition-map.md` — add a "Messaging" row noting `go-messaging v0.1.0` as the shared contract, with agent-mux (Phase 2) and Nanite (Phase 4) as planned consumers.
  - `~/Projects-apps/agent-workspaces/knowledge/shared-needs.md` — update "shared auth / identity story for cross-tool agent messaging" line to reference this spec + phase.
- [ ] Commit KB updates in the agent-workspaces repo (separate commit in a separate repo from everything else this sprint).

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

- [ ] All three tasks ticked and committed on `feat/s04-polish-ship`.
- [ ] Branch FF-merged into `main`, feature branch deleted.
- [ ] `git log --oneline` on `main` shows sequential sprint closes S01 → S04.
- [ ] `v0.1.0` tag visible on GitHub and locally (`git tag | grep v0.1.0`).
- [ ] External smoke test printed the expected output.
- [ ] Epic exit criteria section checkboxes all tick.
- [ ] KB updates committed in `agent-workspaces/`.

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
