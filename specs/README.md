# specs/ — the TLA+ tree

> This README carries the **§3 prose** of `DUCKSPOUT.md` — modeling
> philosophy, module map, liveness/findings doctrine, broken variants and
> witnesses, trace conformance — absorbed per docs/seed.md s§10. The **formal
> content** (§3.2 state space, §3.3 action set, §3.4 invariants, and the
> modules themselves) **lives verbatim in [`formal-core.md`](formal-core.md) until the `.tla` files land
> at v0.1/v0.2** per the arming ledger: every `.tla`/`.cfg` path here is
> reserved-absent (Ⓜ), tracked by ledger rows `tla-mc-core` (v0.1) and
> `tla-mc-replication` (v0.2). Section citations (`§n`) refer to
> `DUCKSPOUT.md` until its absorption completes.

## The formal core (§3)

§3 is DuckSpout's vocabulary and ground truth. Every protocol step named
anywhere in the design is one of the actions defined there, under exactly
the name defined there (§3.1 scopes the short list of operational behaviors
that are deliberately not actions); every guarantee the product claims is
one of the invariants defined there. Implementation subsystems emit
execution traces whose event names are these action names verbatim (§3.7),
so the mapping between model and code is a checked artifact, not a
convention.

## Modeling philosophy (§3.1)

**Bounded and exhaustive, at small, deliberately chosen scopes.** Each
model is checked by TLC over its entire reachable state space at a scope
small enough to exhaust and large enough to contain the hazard it exists
for: 2–3 nodes, 2 origins, 1–2 partitions, 2 windows, ~4 records, RF = 2.
This is the converged industry practice (Amazon's published TLA+
experience with S3 and DynamoDB: protocol bugs appear at tiny scopes, and
exhaustion at a tiny scope beats sampling at a large one). Every scope
choice is justified in its module header against the hazard it must
represent — e.g. the replication model needs two origins and two records
per window so a window's full coverage genuinely requires a peer's bytes,
the only shape in which message loss matters at all. Two scope pins are
normative here because reachability depends on them: the checked ingest
configuration includes **at least two requests with colliding dedup keys**
(`DKey(q1) = DKey(q2)`) — otherwise both `DedupCheck` branches and the
`DemoteDirty` variant are dead code — and dedup-key collisions are
scoped per tenant by construction (`DKey` carries the tenant, §3.3), so
cross-tenant collisions do not exist in any configuration. The drain
configuration likewise pins **divergent coverage between racing drains**,
so `DoubleDrain`'s two candidate parts differ and the `lake` set union
cannot silently merge them.

Four operational behaviors are deliberately *not* §3 actions, with stated
rationale rather than silence: window quarantine/re-fetch (§5.9) is the
already-modeled catch-up path re-run (`PeerApply` over receipted ranges);
trickle inlining (§6.2) is a backend-private encoding of `LakeCommitOk`
(rows instead of a part pointer ride the same atomic commit, so the
`pt \in objects` guard is read as "the commit's payload is durable");
the owner/replica role flip (§5.3) is advisory local metadata no
invariant reads; and the late-arrival hold (§6.3) is window-close timing,
abstracted into `WindowClosed`. Every protocol step a guarantee depends
on is an action here; these four are routing, timing, or encoding.

**One model family, several modules.** Shared definitions (records, keys,
parts, the ladder measure) live in `DuckSpoutCore.tla`; the checked
modules project the state space each needs:

| Module | Actions owned | Properties owned | Lands |
|---|---|---|---|
| `Ingest.tla` | Accept, DedupCheck, StageCommit, Throttle, Refuse, ClientAck, ClientTimeout | DurableAck, LadderMonotone, EveryRequestResolves | v0.1 |
| `Replication.tla` | Forward, PeerApply, Receipt, ClaimAdvertise, Heartbeat, TakeoverDrain, CrashNode, CrashWipe, RecoverNode, FenceBoot, DegradedBoot | NoAckedLoss, GapFreedom, FencedZombie | v0.2 |
| `Drain.tla` | SealPart, PutPart, LakeCommitOk/Abort/IndeterminateLanded/IndeterminateLost, Reconcile, Demote, Evict, DropWindow, SnapshotSeal, Expire, DeclareLoss | WatermarkHonesty, SingleDrainCommit, CacheTransparency, SnapshotCovered, LossLedgerTruthful, LatestViewCorrect, WatermarkEventuallyAdvances | v0.1 |
| `Schema.tla` | EvolveSchema (+ PeerApply's fail-closed guard) | lattice monotonicity, replay convergence | v0.1 |
| `traces/*Trace.tla` | (refinement modules, §3.7) | TraceComplete + behavior membership | v0.1 |

"Actions owned" does **not** mean "actions present": every module
instantiates the full shared `Next` over a projected state space and
constant set, so no property is checked in a configuration missing the
actions it quantifies over (NoAckedLoss is meaningless without ClientAck;
DurableAck without Receipt is vacuous at RF = 2). Ownership means: this
module's configuration is the one whose pinned state count, broken
variants, and witnesses *arm* the property — the place a regression in it
is caught first.

Gated features follow a state-count pinning discipline: the cache class
and the changelog machinery sit behind `CONSTANT` toggles wired so that
the disabled configuration's pinned state count is *checkably* identical
with and without the feature's variables and actions — the claim "this
addition changes nothing when off" is verified by an exact state-count
assertion, never argued in prose.

**External systems are atomic actions with stated semantics — never
modeled internally.** DuckSpout composes with three external systems; the
model gives each one exactly the semantics DuckSpout depends on, as a
named boundary assumption, and nothing more:

| Assumption | External system | Modeled semantics | Discharge |
|---|---|---|---|
| **A1** | Embedded DuckDB (hot store) | A local transaction commit is one atomic, fsynced state transition. `StageCommit` and `PeerApply` are single actions; a crash either sees the whole transaction or none of it. | DuckDB's officially documented WAL and checkpointing semantics are the trusted base (R-trust-official-docs — consistent with A2 trusting Postgres ACID and A3 trusting S3 PUT atomicity; empirical validation only if the docs prove vague for a guarantee we rely on, invoked explicitly); the engine version is pinned in the compatibility matrix, and the CTK's fsync fault family (§8.3) exercises DuckSpout's *own* fsync discipline behind the storage port — our obligations, not the engine's. |
| **A2** | Catalog DB (Postgres) | A catalog transaction is atomic with a **three-way outcome**: `Committed`, `Aborted`, or `Indeterminate` (the connection died mid-commit). Indeterminate is resolved by **exactly one read-back** before any retry — never by resubmitting the same attempt blind. Postgres ACID is trusted. | Postgres's transactional guarantees are the trusted base; the three-way outcome and read-back discipline are DuckSpout's own obligations, modeled and trace-checked. |
| **A3** | Object store (S3 contract) | A PUT is an atomic object appearance: the object exists whole or not at all; no partial objects, no in-place modification. At most one *logical* PUT (byte-identical retries permitted) and one whole-file DELETE per object, ever (§2's immutable-with-expiry rule). | S3's documented PUT atomicity; the one-logical-PUT-one-DELETE half is DuckSpout's own invariant, enforced by the drain (§6). |
| **A4** | Network | Asynchronous, lossy, reordering, duplicating; no delay bound. Modeled as a message *set* — loss is a message never taken, reordering is inherent, duplication is re-taking. | No discharge needed; this is the adversarial assumption. |

**Ground truth versus what a guard consulted.** Every invariant is
written over true model state — monotone ledgers, the catalog's committed
register, the real receipt set — never over the sample a node's own guard
happened to read. In the honest configuration the guard and the
ground-truth predicate are the identical formula, so the invariant holds
by construction; each broken variant (§3.6) perturbs exactly one clause
of the *guard* while the yardstick never changes. That separation is what
makes a violation representable rather than definitionally impossible —
a model whose invariant restates its own guard checks nothing.

## The state space, actions, and invariants (§3.2–§3.4)

Formal content — the `CONSTANTS`/`VARIABLES` declarations, the action
definitions, and the invariant formulas — **lives verbatim in
[`specs/formal-core.md`](formal-core.md)
until the modules land at v0.1** (ledger rows `tla-mc-core`,
`tla-mc-replication`). Do not paraphrase it here; when the `.tla` files
land, they become the authoritative formal statement and this README maps
to them.

## The four-file pattern (per module, CCF/etcd-derived — s§8.1)

1. `<Module>.tla` — the module.
2. `<Module>.cfg` — the bounded clean config (2–3 nodes, exhaustively
   checked; reachable-state count pinned exactly in `state-counts.toml`).
3. `traces/<Module>Trace.tla` — the trace-refinement sibling.
4. `broken/` variants + `fixtures/` NDJSON traces — the 13 broken variants
   of §3.6 must fail, the 11 witness assertions must be reachable, the 5
   FINDINGS configs of §3.5 must stay red; fixtures are 1 conforming + 4
   doctored traces per module.

## Liveness, fairness, and honest findings (§3.5)

Safety without liveness lets a node satisfy every invariant by doing
nothing. Two liveness properties are checked under explicit fairness:
weak fairness on `StageCommit`, `Forward`, `PeerApply`, `Receipt`,
`ClientAck`, `ClientTimeout`, `Throttle`, `Refuse`, `SealPart`,
`PutPart`, `LakeCommitOk`, `TakeoverDrain`, and `Reconcile`, plus the
assumption that a message resent forever is eventually applied (A4
tempered by retry, as the implementation behaves). `TakeoverDrain`'s
fairness is load-bearing: without it, WatermarkEventuallyAdvances fails
at the model's own hands for any partition whose owner died —
a takeover that may forever not happen is no availability story.

**EveryRequestResolves** — never silence.
```tla
EveryRequestResolves ==
  \A q \in Requests :
    (resolved[q] = "pending") ~> (resolved[q] \in {"acked", "throttled", "refused"})
```
Every accepted request terminates in an ack, a retryable throttle, or a
refusal. Fail-closed is a liveness discipline as much as a safety one: a
client left hanging is an undisclosed failure.

**WatermarkEventuallyAdvances** — completeness is not vacuous.
```tla
WatermarkEventuallyAdvances ==
  \A p \in Partitions :
    (DrainEnabled /\ LakeAccepts /\ AckedBehindWm(p)) ~> WmAdvanced(p)
```
When drains are enabled and the lake accepts commits, every partition
with acked data behind its watermark eventually advances it. The
antecedent is honest: during a catalog outage the watermark does *not*
advance — and that is a disclosed pause (§9), not a liveness bug.

**The honest-findings convention.** Properties DuckSpout deliberately
does *not* have are kept in the suite as permanently-failing FINDINGS —
checked on every run, required to fail. **This table is the single
authoritative FINDINGS set — five members, exactly; §8.1 runs it and
cross-references it here:**

| Finding (must fail) | What its failure documents |
|---|---|
| `Finding_BoundedAckLatency` | "Every pending request acks within B steps" is false: DuckSpout sets no ack-latency bound under contention. Throttle is the pressure valve, not a deadline. |
| `Finding_WatermarkThroughCatalogOutage` | The watermark does not advance while the catalog is down (WatermarkEventuallyAdvances without the catalog-recovers fairness assumption). Drains pause and say so (§9); no timer ever escalates a catalog outage into data movement. |
| `Finding_PerOriginFairness` | One origin can be throttled indefinitely while others progress; no cross-origin fairness is promised in v1. |
| `Finding_BoundedThrottleDuration` | No upper bound exists on how long a client is throttled while staging is full and drains are stalled. The alternative is shedding acked-adjacent data, which NoAckedLoss forbids. |
| `Finding_RefuseFreeBelowRF` | Below the replication floor, ingest does not eventually accept: refuse-only is the design (§5.1); "ingest always eventually accepts" is false on purpose. |

A finding that goes green fails CI **on purpose**: either the model
drifted from the protocol or the protocol silently acquired a guarantee
nobody committed to documenting — both demand a human decision, not a
quiet pass.

## The teeth: broken variants and non-vacuity witnesses (§3.6)

An invariant that never produced a counterexample proves either that the
design is sound or that the model cannot represent the bug — TLC cannot
tell you which. So every checked **safety invariant** ships a
**permanently-armed, deliberately-broken variant**: a configuration
perturbing exactly one clause of one action's *guard* (never the
invariant's yardstick) that MUST produce a counterexample on every CI
run. Liveness is armed the same way through `SuppressionNeverExpires`
(and the FINDINGS above, which are permanently-red liveness checks);
Schema.tla's lattice laws are armed in the property-test tier (§8.5). A
model whose broken variant stops failing is a model that stopped
checking; CI fails closed on it.

| Broken variant (armed `.cfg`) | The one perturbed clause | Property that must catch it |
|---|---|---|
| `AckBeforeReceipt` | `ClientAck` drops the ≥ RF receipt conjunct | DurableAck; NoAckedLoss under one wipe |
| `DrainWithoutWatermark` | `LakeCommitOk` no longer advances `wm`; a separate, unguarded advance action exists | WatermarkHonesty — the freestanding advance fires ahead of the commit it should have ridden; no crash is needed (with the honest commit-then-advance coupling, `wm` can only ever *lag*, which is the safe direction) |
| `EvictStaging` | `Evict` enabled on staging-class tables | NoAckedLoss (DurableAck cannot catch it: its evidence ledgers never shrink) |
| `UnfencedZombie` | `PeerApply`/`LakeCommitOk` accept an incarnation below the acceptor's fence | FencedZombie (alone — the intact UNIQUE guard aborts a zombie drain before SingleDrainCommit could see it) |
| `WatermarkPastHole` | `NewWatermark` may pass an uncovered range with no `lossLedger` row | WatermarkHonesty |
| `GapAcceptingPeer` | `PeerApply` drops the contiguity conjunct | GapFreedom |
| `DemoteDirty` | `Demote` drops `dedupRemoved = 0` | CacheTransparency (reachable because the pinned config includes colliding DKeys, §3.1) |
| `DoubleDrain` | `LakeCommitOk` drops the UNIQUE conjunct | SingleDrainCommit (the config pins divergent coverage between the racing drains, §3.1, so the two parts differ and the `lake` set union cannot mask them) |
| `SupplementOverlap` | The supplement path skips the disjoint-coverage proof | SingleDrainCommit |
| `LossOverLiveReplica` | `DeclareLoss` drops the no-live-coverage guard | LossLedgerTruthful (a live replica's coverage falsely confessed away — invisible to NoAckedLoss, whose record never left the replica) |
| `ExpireUncovered` | `Expire` drops the covering-snapshot conjunct | SnapshotCovered |
| `LadderInversion` | `Accept`'s rung guard re-permits admission at rung ≥ 2 | LadderMonotone (the action property — the step itself is the counterexample) |
| `SuppressionNeverExpires` | `SuppressionExpired` pinned FALSE — takeover never fires for a "restarting" node that never returns (§5.10) | WatermarkEventuallyAdvances |

The clean configuration's state count is pinned exactly — silent drift
means the model's shape changed without the baseline moving. A broken
variant's count is asserted only nonzero (it halts at first
counterexample; its count is a race, not a property); the stable signal
is the violated invariant's *name*.

**Non-vacuity witnesses** prove the model genuinely reaches the states
its guards protect — reachability assertions, permanently armed. **This
table is the definitive armed witness set; §8.1 describes the tier and
cross-references it here:**

| Witness | What it proves is genuinely exercised |
|---|---|
| `Witness_TakeoverCommits` | A `TakeoverDrain` actually lands a dead owner's window in the lake — takeover is a reachable behavior, not a declared one. |
| `Witness_LossDeclared` | With the budget raised past RF − 1, `DeclareLoss` actually fires end-to-end: ledger row and watermark advance in one step. |
| `Witness_LossRefusedOverLiveReplica` | A `DeclareLoss` is refused because a live replica still advertises coverage — the ceremony's unreachability *within* budget, checked as its own reachable refusal. |
| `Witness_IndeterminateResolved` | The three-way commit's least trivial branch — `LakeCommitIndeterminateLanded` followed by `Reconcile` adopting the landed write — occurs. |
| `Witness_SupplementCommits` | A supplement part commits beside a winner with proven-disjoint coverage. |
| `Witness_SupplementPending` | The state between winner commit and supplement commit is reached: the residue is staged on a replica, receipted, and `wm` has **not** advanced over the window — `NewWatermark`'s extent criterion is doing work, not decoration. |
| `Witness_ReceiptOutstandingAtAck` | A `Forward`'s Receipt is outstanding at ClientAck-decision time — the RF wait is a real wait. |
| `Witness_ThrottleAndRefuseTaken` | `Throttle` and `Refuse` are each actually taken — the ladder's upper rungs are reachable behaviors. |
| `Witness_DedupReplayAcked` | A colliding retry replays the original's ack through `DedupCheck`, inheriting its evidence — the replay branch is live, not dead code. |
| `Witness_SchemaWidensInFlight` | An `EvolveSchema` lands mid-window and a catching-up peer applies widen-before-data. |
| `Witness_CrashBetweenCommitAndDemote` | The crash window between `LakeCommitOk` and `Demote` is reached and recovered through. |

Witnesses for the dormant cache class (`Witness_EvictDuringCompleteRead`)
are parked with the feature and arm the day its toggle flips.

## Trace conformance: the model checks the code (§3.7)

The models above verify the design. Trace refinement closes the loop to
the implementation: each subsystem emits an execution trace whose event
names are the action names of §3.3, verbatim — `StageCommit`, `Receipt`,
`ClientAck`, `LakeCommitOk`, `FenceBoot` — with the arguments the
corresponding action takes. Three vocabulary rules keep the mapping
exact: a commit journals its outcome name (`LakeCommitOk`,
`LakeCommitAbort`, or `LakeCommitIndeterminate` — the implementation
cannot know which Indeterminate successor it took, so it journals the
one name and the following `Reconcile` names the outcome); `CrashNode`
and `CrashWipe` are environment events, not journaled (a crashed node
cannot journal its own crash) — the trace checker treats them as
unobserved environment steps; and `ClientTimeout` is journaled by the
verifying load generator (§8.4), which is a fleet member, not by any
node. For each module a `*Trace.tla` sibling constrains `Next` to the
recorded step sequence and checks two things:

1. **Every recorded run is a behavior of the model.** A run the model
   cannot take deadlocks at the first impossible step, and the deadlock
   names it — the implementation did something the specification
   forbids, at a specific event.
2. **Every required step was recorded** — the `TraceComplete` invariant.
   A subsystem that performs a modeled transition without emitting its
   event is as broken as one that performs a forbidden transition:
   silent steps are how implementations drift out from under their
   specifications.

Conformance runs are part of the standard suite, executed against runs
from the deterministic harness and from chaos schedules (kills at
arbitrary points, partitions, membership churn). The capture format, the
harness, and the CI wiring — including the rule that a conformance
failure blocks release exactly as a broken-variant regression does — are
specified in §8.

The rest of the design uses these names as defined in §3: §4's "ack
only after RF receipts" is `ClientAck`'s guard, §6's "seal, put, commit"
is `SealPart → PutPart → LakeCommitOk` with WatermarkAdvance inside the
commit, §9's loss ceremony is `DeclareLoss`. One vocabulary; this is it.

## `just tla-*` how-to

- `just tla-install` — fetches `tla2tools.jar` v1.8.0 +
  `CommunityModules-deps.jar` into `specs/.tools/`, SHA-256-verified
  (`scripts/tla.mjs` holds the authoritative pins); needs Temurin 21 (the CI
  setup action provides it).
- `just tla-mc <Module>` — bounded exhaustive check of `<Module>.cfg`;
  fails on any reachable-state-count drift vs `state-counts.toml`; runs the
  `broken/` suite.
- `just tla-sim <Module>` — simulation mode (nightly cadence).
- `just tla-tv <trace.ndjson>` — trace validation against
  `traces/<Module>Trace.tla`; `-workers 1` per trace, parallel across
  files; includes the mutated-trace negative control that must be rejected.

Until the gates arm, these recipes run for real when invoked directly and
exit 78 (`STAGED`) when their inputs don't exist yet — never a fake green
(s§5.1). TLC scratch (`states/`, `*_TTrace_*.tla`, `specs/.tools/`) is
gitignored.
