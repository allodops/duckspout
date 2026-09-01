# ADR-0004: HRW hashing is in-house (R-third-party-first exception)

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (D-12 recorded exception); DUCKSPOUT.md §5.2, §8.5

## Context and Problem Statement

Placement uses Highest-Random-Weight (rendezvous) hashing (§5.2).
R-third-party-first requires an ADR to build in-house where a third-party
candidate exists.

## Candidates considered

- **`hrw-hash` (crates.io)** — lost on adoption and maintenance: tiny user
  base and low maintenance activity for a dependency that would sit on the
  placement path of every record.
- **In-house** (chosen) — the algorithm is ~50 lines of pure code inside
  `duckspout-replication`.

## Decision Outcome

In-house. The function is small, pure, and — decisively — *verified twice
over*: §8.5 property-tests its minimal-disruption law exactly from v0.1 on,
and as a seed addition it is cross-checked against the TLA+ placement
function once `Replication.tla` lands (v0.2). A third-party crate would not
remove either obligation.

## Consequences

- Good: no supply-chain exposure on the placement path; the verification
  story is ours either way.
- Bad: ~50 lines to maintain in-house; the D-12 exception must be carried
  here forever or retired.

## Revisit when

A well-maintained, widely adopted HRW crate exists whose semantics match the
spec's placement function exactly — then re-run this ADR with it as a
candidate.
