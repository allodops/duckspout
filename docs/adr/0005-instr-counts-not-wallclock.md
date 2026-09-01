# ADR-0005: Per-PR performance gating uses instruction counts, never wall-clock

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (interpretive ruling); DUCKSPOUT.md §8.6–§8.7

## Context and Problem Statement

Performance floors are Keep Rule 11 material — checkable numbers CI
recomputes. Wall-clock measurements on shared CI runners are noisy enough to
make a per-PR gate either flaky (tight threshold) or toothless (loose
threshold).

## Decision Outcome

Per-PR performance gating uses **deterministic instruction counts**
(iai-callgrind, compared against `floors/instr-baselines/` with a +15%
ceiling) plus the §8.6 1M-record smoke *bound*. Wall-clock throughput
appears only in the nightly bench card (§8.7, v0.4), published under full
disclosure norms. **Never swap them**: an instruction-count gate in the
bench card would be dishonest marketing, and a wall-clock gate per PR would
be a flaky gate — and a flaky gate is a red gate (s§3.4's `retries = 0`
logic applies to gates too).

## Candidates considered

- Wall-clock thresholds per PR — rejected: nondeterministic on shared
  runners; would poison the merge queue.
- criterion-style statistical benchmarks per PR — rejected: still
  wall-clock under the statistics; belongs in the nightly card.

## Consequences

- Good: the per-PR gate is deterministic and reproducible locally.
- Bad: instruction counts miss cache/IO effects — accepted, because the
  nightly bench card carries the real-world numbers.

## Revisit when

Dedicated bare-metal runners make wall-clock per-PR measurements
reproducible within the gate's noise budget — then the *bench card* may run
more often; the per-PR gate stays deterministic regardless.
