# ADR-0009: The mutation floor runs nightly, not per PR (flagged deviation)

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (D-8), s§6.3; DUCKSPOUT.md §8's tier table, §11 (Keep Rule 11)

## Context and Problem Statement

§8's tier table puts coverage, mutation, and instruction counts **per PR**.
cargo-mutants over this workspace is hours-scale; a per-PR mutation run
would make the merge queue unusable. This ADR records the deviation openly —
a silently demoted gate would be exactly the §11 offense.

## Decision Outcome

The mutation floor runs **nightly** (`nightly.yml`, ledger row
`mutation-floor`, cadence `nightly`, arms at v0.1). Compensating mechanism:
**a red nightly mutation run auto-files a `blocked`-labeled issue**, and the
dispatcher surfaces `blocked` issues before assigning new work (s§9.4) — so
a mutation regression halts the loop's forward progress rather than riding
under it. Coverage and instruction-count floors stay per PR as §8 mandates.

## Candidates considered

- Per-PR full mutation run — rejected: hours-scale; queue-breaking.
- Per-PR mutation on changed files only — rejected for now: incremental
  mutation selection that is *sound* (mutants whose kill status a diff can
  change) is not something current tooling provides; an unsound subset
  would be a skipped-green gate wearing a per-PR costume.
- Dropping the mutation floor — rejected: Keep Rule 11 names it.

## Consequences

- Good: the merge queue stays usable; the floor still bites within a day,
  and a regression blocks new dispatch.
- Bad: a PR can merge before the nightly run convicts it — accepted and
  flagged; the blocking issue plus ordinary revert is the repair path.

## Revisit when

Incremental (changed-code-only) mutation testing makes a sound per-PR run
feasible — then this deviation is retired and the gate moves to `cadence =
"pr"` through the gate-proposal process.
