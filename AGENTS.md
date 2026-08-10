# AGENTS.md

## Project

`jcode-go` is the Go SDK for the Jcode harness API.

- Language: Go
- Module: `github.com/ariel-frischer/jcode-go`
- Validation: `go test ./... -count=1`
- Current SDK stability program: `docs/sdk-stability-plan.md`
- Current lifecycle architecture: `docs/architecture.md`

## Scope and design

- Keep public changes minimal, additive, and Go-idiomatic.
- Preserve the protocol v1 raw APIs and documented ownership contracts unless an approved Bead explicitly changes them.
- The current private-runtime stability program is Linux-only. Do not add or expand Windows implementation work.
- A private Jcode agent must run in the explicit `LaunchOptions.WorkingDir` and session working directory supplied by its caller.
- Use Beads from the project-local database in the parent checkout. Do not create markdown task lists.

## Git and worktrees

- Perform implementation in the dedicated branch and worktree assigned by the coordinator.
- Preserve unrelated changes and stage explicit files only.
- Do not use rebase, reset, amend, squash, force-push, `git stash`, `git add .`, or `git add -A`.
- Merge the latest `origin/main` into the worker branch before final validation when the coordinator requests a handoff.

## Explicit orchestrator boundary

When the assignment says a root coordinator or orchestrator owns integration, the worker must stop at the handoff boundary even if general landing instructions normally require additional lifecycle actions.

The worker may:

- inspect source and Bead context
- edit only its assigned scope
- run focused and full validation
- commit its scoped changes on the assigned branch
- report the commit, files, validation, risks, and follow-up

The worker must not:

- merge into `main`
- push any branch or Dolt ref
- close, reopen, claim, release, or otherwise mutate Beads
- create, remove, or prune worktrees
- create or delete branches outside the assigned worktree
- launch nested agents or swarms
- mutate provider configuration or credentials

The root coordinator owns merge, push, Bead mutation, cross-Bead ordering, worktree cleanup, and final completion.

## Validation

- Add focused tests for every behavior and failure path changed.
- Run focused tests first.
- Run `go test ./... -count=1` before handoff.
- Run race-enabled focused packages when changing shared concurrency or lifecycle code.
- Run `git diff --check` and verify the worktree is clean after the scoped commit.
- Real OAuth or provider validation must not print credentials, prompts, private responses, or raw session identifiers.
