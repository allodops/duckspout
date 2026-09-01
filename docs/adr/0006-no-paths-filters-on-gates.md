# ADR-0006: No `paths:` filters on gates; latency is managed inside the gate

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (interpretive ruling), D-8; DUCKSPOUT.md §8.1–§8.2, §11

## Context and Problem Statement

TLC model checking runs on every push and trace conformance runs per PR with
real backends (§8.1–§8.2). These are the slowest gates. The conventional CI
economy move is a `paths:` filter ("only run TLC when specs/ changes") or a
nightly-only demotion.

## Decision Outcome

Neither, ever. §11 names narrowing a check as the same offense as weakening
it — and a `paths:` filter is exactly a scope-narrowing: a Rust change can
break trace conformance without touching `specs/`, and a skipped-green gate
on such a PR is a lie. Latency is managed **inside** the gate instead: small
bounded models with pinned state counts, tool/result caching, and parallel
trace fan-out (`-workers 1` per trace, parallel across files).

## Candidates considered

- `paths:` filters — rejected: skipped-green on cross-cutting breakage.
- Nightly-only demotion of per-PR gates — rejected: same offense on the time
  axis; the one sanctioned deviation (mutation floor) is flagged in
  ADR-0009 with a compensating mechanism, not silently demoted.

## Consequences

- Good: every green `ci-ok` means every armed gate actually ran.
- Bad: per-PR CI cost is paid on every PR, including doc-only ones — the
  price of the guarantee; bounded models keep it tolerable.

## Revisit when

Never for the principle. If gate latency makes the merge queue unusable, fix
the gate's internals (smaller scopes, better caching) or move it through the
arming ledger with a compensating mechanism, in the open.
