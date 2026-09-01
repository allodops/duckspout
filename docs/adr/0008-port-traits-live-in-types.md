# ADR-0008: All cross-crate-consumed port traits live in `duckspout-types`

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (interpretive ruling), s§4; DUCKSPOUT.md §10.1

## Context and Problem Statement

§10.1 says protocol crates depend on `duckspout-types` "and on each other's
ports". Read literally as crate dependencies, that creates protocol×protocol
edges — and any two protocol crates consuming each other's ports create a
cycle. Where do port traits live?

## Decision Outcome

All cross-crate-consumed port traits — clock, scheduler, transport,
storage/fsync, `AcceptAdapter` v0.1, `LakeCommitter` v0.1 — are **defined in
`duckspout-types`**. Home crates re-export them
(`pub use duckspout_types::...`) and own everything beyond the bare
signature: adapter registration, the conformance suite, outcome helpers.
This is the only acyclic reading of §10.1: with all protocol×protocol crate
edges banned (s§7), a port consumed across crates must live in types.

## Candidates considered

- Ports in their home crates, consumers depend on the home crate —
  rejected: creates protocol×protocol edges and, transitively, cycles.
- A separate `duckspout-ports` crate — rejected (KISS): `duckspout-types`
  already is the no-I/O root crate; a second root adds a boundary with no
  rule attached to it.

## Consequences

- Good: the layering rule reduces to a finite, hand-enumerated
  forbidden-edge list the invariant engine audits mechanically.
- Bad: `duckspout-types` is a wide dependency — every port signature change
  rebuilds the workspace. Acceptable pre-1.0 with lockstep versioning
  (D-13).

## Revisit when

The absorption pass (s§10) re-verifies §10.1's layering table against this
ruling; if the original text meant something else, amend there, in the open.
