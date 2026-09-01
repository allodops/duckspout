# ADR-0011: Verification toolchain — TLA+ for the proof tier, in-house CTK for the testing tier

- Status: accepted (2026-09-01)
- Deciders: owner + supervising session (raised in owner design review:
  "why TLA+ and not P?")

## Context

The verification doctrine (docs/verification.md, §8) needs two distinct
capabilities: (a) design-level proof that the protocol has no counterexample
at a bounded scope, with drift detection strong enough that CI can insist
the checker still bites; (b) implementation-level exploration of schedules
and faults against the real code. Several tools cover parts of both, and
R-third-party-first requires the candidates and the losses recorded.

## Decision

**Proof tier: TLA+/TLC** (third-party, tla2tools pinned by SHA-256 in
`scripts/tla.mjs`). The doctrine's three load-bearing mechanics are
TLC-shaped:

1. **Exhaustive bounded checking with exact pinned state counts**
   (`specs/state-counts.toml`): the count is a tamper/shape-change detector
   and the feature-toggle discipline is a state-count *identity* — a
   probabilistic explorer has no equivalent number to pin.
2. **Trace refinement** (§3.7, §8.2): constrain `Next` to a recorded
   implementation trace, check behavior membership + `TraceComplete` — the
   mature CCF/etcd/MongoDB lineage.
3. **Broken variants and witnesses as deterministic cfg runs**: 13
   must-fail variants, witness reachability, permanently-red FINDINGS all
   assume deterministic exhaustive runs.

**Testing tier: in-house CTK** (`duckspout-ctk`): seeded deterministic
scheduler, virtual clock, fault-injector families with armed-vs-fired
vacuity discipline, replayable interleavings — running against the **real
Rust protocol code through the real port traits** (D-2/ADR-0008). No third
model to keep in sync; every bug found is a bug in shipping code.

## Candidates considered

- **P (p-org) as the proof tier**: excellent prioritized schedule
  exploration at scales TLC cannot exhaust; used by AWS for
  implementation-adjacent work (ShardStore). Lost because its checker
  samples rather than exhausts — "no counterexample" means *not found*,
  not *proven at scope*; there is no exact state count to pin; and its
  trace-refinement story is thinner than the TLA+ lineage this repo
  copies. The doctrine's stated philosophy — exhaustion at a tiny scope
  beats sampling at a large one — is the TLC trade.
- **P / Coyote as the testing tier**: a polished systematic checker, but
  requires modeling the system a third time in P's language and keeping
  that artifact in sync with both the spec and the Rust — the same
  duplicated-seam argument that excluded turmoil and madsim (D-2). The CTK
  forgoes the polished checker to test the real code directly.
- **turmoil / madsim**: rejected at D-2 (net-aliasing duplicates the port
  seam; whole-runtime patching is invasive and version-locked).

The division of labor mirrors AWS's own published practice: TLA+ for
design verification, a P-shaped harness for implementation testing —
except our harness runs the production code itself.

## Consequences

- The spec is forever a sibling of the code, not generated from it — the
  trace-refinement tier (#42) exists precisely to keep the sibling honest.
- The CTK's v0.1 schedule exploration is seeded-random; P's checker does
  prioritized search (PCT-style). Parity is tracked as a v0.2 enhancement
  so the gap is a decision, not an accident.

## Revisit when

- PChecker (or a successor) gains exhaustive small-scope enumeration with
  stable, pinnable state counts — revisit the proof tier.
- The CTK's schedule exploration repeatedly misses interleaving bugs that
  a prioritized checker finds (evidence: escaped bugs with interleaving
  root causes) — revisit adopting P/Coyote for the testing tier.
- Trace refinement proves too costly in practice (conformance-gate wall
  clock dominating CI) — revisit the binding between tiers.
