# ADR-0003: There is no separate WAL crate

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (interpretive ruling); DUCKSPOUT.md §4.2

## Context and Problem Statement

Durable ingestion systems conventionally carry a write-ahead-log component.
Should the workspace contain a `duckspout-wal` crate?

## Decision Outcome

No. §4.2 is explicit: DuckDB persistent tables with fsync-on-commit *are*
the durability primitive — "WAL = hot". The staging tables the ack path
writes are the log; there is nothing to write ahead *of*. Fsync discipline
(directory fsync, torn-write detection, group commit off the reactor) lives
behind the storage port (defined in `duckspout-types`, ADR-0008) and its CTK
fault injectors, inside `duckspout-staging`.

## Candidates considered

- `okaywal` — rejected: dormant, and structurally redundant with the hot
  store's own transactional durability (docs/seed.md s§3.3).
- Any bespoke in-house WAL — rejected for the same structural reason: a
  second durability primitive would need its own fsync verification story
  (A1) for zero added guarantee.

## Consequences

- Good: one durability primitive, one fsync discipline to verify (A1 is
  discharged empirically per DuckDB version via the compatibility matrix).
- Bad: durability semantics are coupled to the embedded engine's commit
  behavior — which is why A1 is a *tested* premise, not documentation trust.

## Revisit when

The compatibility matrix certification (§4.2.1) finds a supported DuckDB
version whose fsync-per-commit granularity no longer holds A1.
