# ADR-0012: The layered refinement pipeline — TLA+ → P → Rust, traces closing the loop

- Status: accepted (2026-09-01)
- Deciders: owner (explicit methodology ruling, superseding ADR-0011's
  P-as-evidence-triggered posture for protocol components from v0.2 on)

## Context

ADR-0011 chose TLA+ for the proof tier and the in-house CTK for the
testing tier, leaving P adoption to evidence triggers; its owner-review
amendment recorded the honest gaps (checker sophistication, no
implementation-level liveness checking, the skipped middle model). The
owner's ruling resolves the middle-model question by methodology rather
than by reactive evidence.

## Decision

For protocol components from v0.2 (replication) onward, development runs
as a refinement pipeline:

1. **TLA+ first**: the module is written and validated in TLC — bounded
   exhaustive, pinned state counts, armed broken variants (§8.1).
2. **P model second**: an executable P model of the same protocol,
   validated with P's checker (deep prioritized schedules, liveness) and
   **cross-checked against the TLA+ spec** — same protocol, two
   formalisms; any divergence is itself a finding.
3. **Rust third, built from both**: the spec supplies the invariants; the
   P model supplies implementation-granularity protocol decisions,
   already made and reviewed before the borrow checker meets them.
4. **CTK exercises the real Rust** (schedules, fault families) emitting
   the §3.7 NDJSON traces.
5. **Traces close the loop against BOTH models**: TLC trace refinement
   (§8.2) and P log-conformance (PObserve-style) consume the same trace
   vocabulary — the P model is thereby a *gated* artifact, not folklore;
   "validated once" cannot silently rot.

Scoping and consequences:

- v0.1 is exempt (already mid-implementation from TLA+ + design docs;
  its single-node choreography is largely sequential). No retrofit.
- #130 becomes the v0.2 execution plan for this pipeline (P toolchain in
  CI, model scope, the trace-conformance wiring on the P side), not a
  go/no-go.
- #124 (shuttle) rescopes to the CTK ScheduleStrategy backend question
  only — P's checker existing does not decide how the CTK picks
  schedules against the real Rust.
- #129's liveness gap is partially addressed for replication by step 2;
  the judge-level liveness verdicts remain v0.3 work.
- Cost accepted deliberately: a third artifact per protocol component,
  a dotnet/P toolchain surface in CI (gated + ledgered like everything
  else), and the sync discipline that step 5's dual conformance makes
  mechanical.

## Revisit when

- Step 5's P-side conformance proves impractical (no reliable
  log-conformance path) — the pipeline's honesty depends on it; without
  it, fall back to ADR-0011's evidence-triggered posture.
- The pipeline's wall-clock cost on a protocol component exceeds its
  finding yield two components in a row (measure: findings caught in
  step 2 that steps 1/4 missed).
