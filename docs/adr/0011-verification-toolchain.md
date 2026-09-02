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
- **shuttle (AWS) / stateright — MISSED in this ADR's first pass, added
  by owner-review amendment (2026-09-01)**: Rust-native tools that run
  P-class systematic exploration on REAL Rust code — shuttle implements
  PCT scheduling (used on s2n-quic) with no third model to sync;
  stateright model-checks Rust actor systems. shuttle remains the
  candidate backend for the CTK's ScheduleStrategy (#124).
- **Apalache / TLAPS (added with the owner-review amendments)** — the
  rungs above TLC on the assurance ladder. Apalache (symbolic, SMT):
  handles larger state spaces and can prove *inductive* safety
  invariants for unbounded executions, but yields no exact reachable
  state count (our pinned-count drift detector), has thin liveness
  support (half our suite), and accepts an annotated fragment of TLA+.
  TLAPS (deductive proofs): unbounded assurance at expert proof-
  engineering cost, brittle under spec change, no counterexamples —
  wrong shape for a CI-gated loop. **Escalation trigger**: if v0.2's
  replication scopes make bounded exhaustion feel thin, an Apalache
  inductive proof of NoAckedLoss (safety only, alongside TLC keeping
  liveness/counts/traces) is the pre-planned next rung.
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

## Honest framing (owner-review amendment, 2026-09-01)

"The CTK plays P's role" is true architecturally and aspirational
qualitatively. Recorded so the gap stays visible:

- **Checker sophistication**: PCT carries a *proven* bug-finding bound
  (≥ 1/(n·k^(d-1)) per run for a depth-d bug among n tasks and k steps);
  seeded-random schedules are exponentially blind to depth. P also
  deduplicates explored state; the CTK re-runs schedules blind. #124
  (shuttle as the ScheduleStrategy backend) is the response.
- **No implementation-level liveness checking anywhere in the stack**:
  TLC checks liveness on the design only; a Rust-level livelock,
  starvation, or retry storm that needs a particular schedule escapes
  both tiers. Tracked as #129.
- **The skipped middle model**: protocol decisions first materialize in
  the Rust, and nothing checks the CTK's own schedule quality beyond the
  vacuity ledger — nobody checks the checker. **Resolved for protocol
  components from v0.2 on by ADR-0012** (the TLA+ → P → Rust refinement
  pipeline, with the P model gated by dual trace conformance); this
  ADR's evidence-triggered posture remains the fallback.

## Revisit when

- PChecker (or a successor) gains exhaustive small-scope enumeration with
  stable, pinnable state counts — revisit the proof tier.
- The CTK's schedule exploration repeatedly misses interleaving bugs that
  a prioritized checker finds (evidence: escaped bugs with interleaving
  root causes) — revisit adopting P/Coyote for the testing tier.
- Trace refinement proves too costly in practice (conformance-gate wall
  clock dominating CI) — revisit the binding between tiers.
