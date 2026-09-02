# Verification End-to-End (§8)

> Toolchain decision record: **ADR-0011** — TLA+/TLC for the proof
> tier, the in-house CTK for the testing tier; candidates (P, Coyote,
> turmoil, madsim) and revisit triggers recorded there.


Absorbed from DUCKSPOUT.md §8 per docs/seed.md s§10. Section labels (§8.1
… §8.7) are preserved so citations elsewhere — the arming ledger's `spec`
fields, CONSTITUTION.md, the ADRs — keep resolving after the monolith is
deleted. Each tier's enforcing gates are the named rows in
`docs/arming-ledger.toml` (R-armed-or-ledgered: every gate is an armed CI
job or a staged ledger row, never silently absent).

DuckSpout's promises are the kind that fail silently: an ack that wasn't
durable, a watermark that overstates coverage, a drained window that quietly
double-committed. None of these produce a stack trace. The verification
stack therefore runs from an abstract model down to real fleets on real
backends, and every tier carries **teeth** — a mechanism proving the tier
itself can still reject a wrong answer. A checker that has never rejected
anything is indistinguishable from a checker too weak to reject anything;
DuckSpout treats that as a permanent design constraint on the verification
stack, not a slogan.

Verification grows in-milestone, never trailing (§12, `docs/deferred.md` +
the milestone tree): a pillar's mechanism and its verification land in the
same release, because a guarantee that ships before its check is a claim,
not a guarantee. Three postures govern every tier:

1. **Skipped ≠ passed.** A gate that cannot run — missing backend, absent
   endpoint, vanished subject — fails, it does not shrug. Ambiguity fails
   closed (§11, CONSTITUTION.md).
2. **CI recomputes, never trusts.** Every floor, count, and threshold is
   recomputed independently on each change; a PR's own claim about a number
   is never the input to the gate that checks that number.
3. **Every check bites, provably.** Each property ships alongside a
   deliberately broken variant that MUST fail; each judge carries vacuity
   rules that downgrade a run that exercised nothing to "no verdict," which
   is never a pass.

The tiers, top to bottom:

| Tier | Subject | Runs | Catches |
|---|---|---|---|
| TLA+ / TLC (§8.1) | The §3 action system, bounded exhaustive | Every push | Protocol design errors |
| Trace conformance (§8.2) | Real implementation vs. the model, step by step | Every CI run, fresh trace | Implementation drift from the model |
| CTK in-memory (§8.3) | Core library through engine-neutral adapters, injected faults | Every CI run | Fault-path logic errors; port-contract violations |
| CTK distributed (§8.4) | Real fleets, real MinIO + Postgres, chaos windows, post-pass judges | Nightly + release | End-to-end invariant violations under real failures |
| Property tests (§8.5) | Algebraic laws and codecs | Every CI run | Determinism and lattice-law regressions |
| Ratcheted floors (§8.6) | Coverage, mutation kill rate, instruction counts | Every PR | Erosion of the test suite itself; perf regressions |
| Bench card + durability audit (§8.7) | Published performance under durable settings | Nightly + release | Throughput-without-durability dishonesty |

> **Flagged deviation (ADR-0009).** The tier table puts the ratcheted
> floors — coverage, mutation, instruction counts — at "Every PR". The
> **mutation floor runs nightly**, not per PR: cargo-mutants over this
> workspace is hours-scale and would make the merge queue unusable. The
> compensating mechanism (a red nightly mutation run auto-files a
> `blocked`-labeled issue that halts new dispatch) and the revisit trigger
> are recorded in ADR-0009. Coverage and instruction-count floors stay per
> PR as this table mandates. Ledger rows: `mutation-floor` (nightly, v0.1);
> `coverage-floor`, `instr-gate`, `smoke` (pr, v0.1).

## §8.1 The TLA+ tier

Ledger rows: `tla-mc-core` (pr, v0.1), `tla-mc-replication` (pr, v0.2),
`tla-sim` (nightly, v0.1).

The §3 specification (specs/) is not documentation — it is checked. TLC
runs bounded, exhaustive model checking of the action system on **every
push**, so the models are deliberately kept small enough for per-PR CI:
small node counts (2–3), small window counts, small per-partition sequence
bounds, with the module header of each model stating why that grain is
sound rather than asserting it. Each model's reachable state count is
pinned in the repository; an unexplained change in the pinned count fails
CI, because a state space that silently shrank is a model that silently
stopped exploring something. Feature-gated model extensions (e.g. the cache
class, changelog snapshots) are wired so the disabled configuration's state
count is **checkably** identical with and without the extension's variables
and actions — "the extension changes nothing when off" is verified, never
argued.

Checked per push, against every reachable state of the bounded models:

- **Safety**: DurableAck, NoAckedLoss, WatermarkHonesty, CacheTransparency,
  GapFreedom, SingleDrainCommit, FencedZombie, LossLedgerTruthful,
  SnapshotCovered, LatestViewCorrect, and the LadderMonotone action property
  (§3 defines each).
- **Liveness**, under stated fairness assumptions: EveryRequestResolves
  (every Accept eventually reaches ClientAck, Throttle — including the
  client's own timeout — or Refuse) and WatermarkEventuallyAdvances (with
  the catalog reachable and at least one live claim holder, LakeCommit
  eventually fires and the watermark moves).

**The armed-broken-variant convention.** Every checked safety invariant
ships a deliberately broken sibling — a one-clause guard perturbation —
that MUST reproduce its own violation on every run, and liveness is armed
through the suppression variant and the FINDINGS; §3.6's table is the
definitive armed set. Examples: a DurableAck variant where ClientAck may
precede the last Receipt; a SingleDrainCommit variant with the uniqueness
guard on (partition, window_id, part_kind, discriminator) removed; a
FencedZombie variant where a pre-crash incarnation's LakeCommitOk is
accepted after RecoverNode. If a broken variant ever goes green, CI fails:
the check stopped biting, whether because the model drifted, the property
was weakened, or TLC's configuration quietly stopped exploring the states
the violation lives in. The broken variants are the tier's teeth; they
convert "the model passes" from an absence of evidence into evidence.

**Non-vacuity witnesses.** A property over states the model never reaches
is vacuously true. Every quantified property therefore carries witness
assertions — reachability claims TLC must confirm. **The definitive armed
witness set is §3.6's table**; representative members: a Forward's Receipt
outstanding at ClientAck-decision time
(`Witness_ReceiptOutstandingAtAck`); TakeoverDrain actually landing a dead
owner's window (`Witness_TakeoverCommits`); Throttle and Refuse each taken
(`Witness_ThrottleAndRefuseTaken`); a DeclareLoss refused over a live
replica (`Witness_LossRefusedOverLiveReplica`); EvolveSchema interleaved
with an in-flight drain (`Witness_SchemaWidensInFlight`). A witness that
becomes unreachable fails CI even though every property still "passes."

**Liveness FINDINGS — permanently red on purpose.** Some liveness
properties DuckSpout deliberately does not have, and the honest way to keep
that fact visible is to keep checking them. **The authoritative FINDINGS
set is §3.5's table — five members, exactly**; this tier runs each in a
dedicated TLC configuration that MUST fail, with the design rationale in
the model header. A FINDING going green fails CI exactly like a broken
variant doing so: either the model was changed to promise something the
system does not deliver, or the system grew a behavior nobody designed.
Both demand a human decision, not a green tick.

## §8.2 Trace conformance

Ledger row: `conformance` (pr, v0.1). The event-name↔trace-enum mapping is
`docs/trace-mapping.md` (Appendix B transcription).

The models constrain designs; trace conformance constrains the code. Every
subsystem that can emit a trace does, using **exactly the §3 action
vocabulary** as its event names — Accept, DedupCheck, StageCommit,
Throttle, Refuse, ClientAck, ClientTimeout (journaled by the load
generator, §3.7), Forward, PeerApply, Receipt, SealPart, PutPart,
LakeCommitOk, LakeCommitAbort, LakeCommitIndeterminate (the model's
Landed/Lost split is unobservable to the emitter; the following Reconcile
names the outcome), Reconcile, Demote, Evict, DropWindow, SnapshotSeal,
Expire, TakeoverDrain, ClaimAdvertise, Heartbeat, FenceBoot, DegradedBoot,
DeclareLoss, EvolveSchema — CrashNode and CrashWipe are environment events,
not journaled (§3.7) — so a recorded trace is directly a candidate behavior
of the model, no translation layer whose own bugs could mask drift. The
trace-refinement specs (§3) accept a finite recorded trace iff the model
has a matching execution and the trace is complete: every step that must
have happened was recorded, and no recorded step is one the model has no
transition for.

Two checks per traced subsystem, deliberately not collapsed into one:

**Self-test against doctored fixtures — the tier's teeth.** A committed set
of static fixtures: one real recorded trace that must conform, plus
doctorings of it that must not. Each doctoring deletes a recorded step
**four different ways**, and the harness asserts **which mechanism rejects
each** — one tooth going blunt cannot hide behind another:

| Doctoring | Rejecting mechanism |
|---|---|
| Delete a mid-trace StageCommit whose ClientAck was recorded | Refinement deadlock: a recorded step that could not have happened |
| Delete the trailing LakeCommitOk after a recorded SealPart + PutPart | TraceComplete invariant: a step that happened and was not recorded |
| Delete a Receipt the ClientAck's RF accounting depends on | Refinement deadlock, at the DurableAck decision point |
| Delete the trailing ClientAck itself | TraceComplete invariant, resolution clause |

For deadlock rejections the harness reads the counterexample's own halt
cursor and asserts *which* recorded entry the walk halted at; for
TraceComplete rejections it asserts *which* clause fired, since the checker
names the invariant, never the conjunct. The self-test catches regressions
in the **checking mechanism** — the refinement spec, the trace decoder, the
harness — which static fixtures are exactly the right instrument for, and
real-code regressions not at all, which is why the next check exists.

**Live generation on every CI run.** A fresh trace is generated on every
single run from the real implementation: a multi-node in-process harness
(§8.3's adapters) runs a real ingest → replicate → drain → query cycle —
concurrent writers, a forced takeover, at least one EvolveSchema — records
the trace, and checks it against the refinement specs. This is what
actually catches a regression: a static fixture captured once can only ever
certify the code as it was on capture day, while a fresh trace certifies
the code in the PR. A live-generated trace that the model has no execution
for is a red check naming the first unmatched step.

**The real-backend variant.** The same generation-and-check runs against
live MinIO (S3 API) and live Postgres (catalog) in CI containers. A trace
from the in-memory double and a trace from real backends are different
inputs to the same model: different timing, PutPart outcomes the test
process does not decide, and — the part no double reproduces faithfully —
real **Indeterminate** resolution, where a timed-out LakeCommit must be
resolved by reading the catalog back rather than assumed either way. Static
fixtures would go stale the moment an adapter changed, so this variant
doctors the trace it just generated, the same four ways, and requires each
rejection to come from the mechanism it was aimed at. The generator skips
gracefully for a contributor without Docker; **the CI gate does not inherit
that skip** — with the endpoint absent, the gate fails rather than
reporting a green run that checked nothing.

> **Flagged deviation (issue #44's arming PR).** Two scope narrowings, both
> traceable to the same v0.1/RF = 1 boundary `specs/traces/IngestTrace.tla`'s
> module header already argues: (1) the doctoring set is **three** of the
> four table rows, not four — the Receipt row has no armed instrument at
> RF = 1 (no receipts are ever journaled with a single replica), so it stays
> undoctored here and arms with v0.2's receipted traces, the same deferral
> the trace config's own omission of `WatermarkHonesty` already documents.
> (2) the capture this variant validates is the **happy path** over real
> MinIO + Postgres — it demonstrates the real credential/attach/PUT plumbing
> works end to end, but does not itself force a genuine **Indeterminate**
> resolution (that needs fault injection against the real catalog
> connection, which is §8.4's CTK-distributed tier machinery, not yet
> built). Real Indeterminate resolution stays exercised at v0.2/nightly by
> that tier; this gate's job at v0.1 is proving the real-backend path
> conforms and the doctoring teeth bite against it, not injecting faults
> into it.

## §8.3 CTK, in-memory tier

Runs on every CI run inside the armed `test` ledger row (workspace test
suite); the port-conformance suites publish with `duckspout-ctk`.

The Conformance Testing Kit is DuckSpout's own test harness family,
published as a crate so that what verifies DuckSpout also verifies
third-party extensions. The in-memory tier drives the core library through
**engine-neutral adapters** — the kit speaks to a trait surface, never to
concrete internals — so a run certifies the contract, and any conforming
implementation can be substituted under the same suite.

**Fault-injecting backends.** Every external effect the core takes goes
through an injectable seam: fsync faults (fail, torn write,
fsync-reports-success-then-loses on simulated power cut), S3 faults
(PutPart timeout, 500, slow-then-success, Indeterminate), catalog faults
(LakeCommit timeout, serialization failure, outage window), peer faults
(Forward drop, Receipt delay, duplicated PeerApply). Each injector keeps a
ledger of faults armed and faults fired; a schedule that armed faults and
fired none certifies nothing and the run is reported as such (the same
vacuity discipline as §8.4).

**Deterministic seed-sweep property tests.** The kit runs randomized fault
schedules from a seeded PRNG, sweeping seeds in CI; every run is
reproducible from its seed alone. An in-process oracle judges each run: no
journaled ClientAck maps to unreadable data, no gap admitted past
DedupCheck, every ambiguous outcome resolved before retry, LadderMonotone
over the journaled status transitions. A failing seed is captured and
pinned as a permanent regression case — flakes are bugs, fixed at the root,
never hidden behind a retry or a loosened assertion.

**Concurrency exploration.** Where the core's lock-order and atomicity
claims are small enough to model-check at the code level (the
staging-commit path, the dedup-window transaction, the claim-advertise
state), loom-style exhaustive interleaving exploration runs in CI over
miniature configurations. It complements the TLA+ tier: TLC explores the
protocol's interleavings, loom explores the Rust memory-model interleavings
of the code implementing one action.

**Published port-conformance suites.** Every port gets a conformance suite
a third-party implementation runs against itself: the LakeCommitter
contract (atomic {EvolveSchema-state, add-files, watermark} commit; commit
idempotence under retry; refusal of add-before-evolve; SingleDrainCommit
uniqueness semantics — this is how the Iceberg committer proves itself
equal to the DuckLake one, §6) and the accept-adapter contract (canonical
decode, DedupCheck key derivation determinism, partial-success semantics,
§4). "Iceberg by contract" means exactly this: the contract is executable,
and passing it is the definition of conforming.

## §8.4 CTK, distributed tier

Ledger rows: `ctk-distributed` (nightly, v0.2) and its release-gate
promotion `ctk-release-gate` (nightly, v0.3, §12.4).

The in-memory tier can inject any fault but cannot produce real timing,
real networks, or real crash semantics. The distributed tier runs **real
multi-node fleets against real MinIO and Postgres**: a fleet runner
provisions nodes, drives sustained load through real OTLP/Arrow Flight
ingest, executes a fault schedule, and journals everything; a separate
**judge binary** runs as a post-pass over the journals plus the final
backend state and produces the run's verdict. Judging from journals after
the run — rather than asserting in-line — is what lets the fleet misbehave
freely during the run and still be convicted precisely afterward.

**Fault windows** in the standard schedule (each window journaled with
start/end):

- Node kills, including the sharpest one: the partition owner mid-drain,
  between PutPart and LakeCommit — the window where SingleDrainCommit and
  TakeoverDrain are both live.
- Network partitions and asymmetric degradation (drops, delay, bandwidth
  caps).
- Process pauses (SIGSTOP long enough to expire claims, then resume — the
  FencedZombie scenario: the paused node's stale incarnation must be
  rejected).
- Membership churn: join and leave under load, not only crash — the fault
  class most implicated in published acked-loss incidents elsewhere, so it
  is in the v1 schedule, not deferred.
- Flight-server kill mid-stream (a hot query's stream dies; the client's
  typed error, never a silently truncated result — §7).
- Catalog outage windows (ingest must continue undegraded; drains stall
  and disclose — §4, §9).
- Discovery flapping (ClaimAdvertise/Heartbeat oscillation; routing must
  converge without ever serving a `complete` answer it cannot prove).

**Journals.** Every node journals its events durably and locally in the §3
action vocabulary, before the corresponding external call where the
predicate demands it (an attempt with no journaled resolution, or a
resolution with no journaled attempt, is itself a finding). The load
generator is a first-class fleet member: it journals every request sent and
every ClientAck received, with payload identity — the verifying client is
part of the test, not an afterthought.

**Judges.** Each judge is a predicate over journals plus read-back state:

- **Zero-acked-lost (the W-shaped judge, write-side):** every record whose
  ClientAck the load generator journaled must be present in the final
  system — queryable from hot or lake — regardless of what the fault
  schedule did. System-class datasets are excluded by definition: they
  receive no durable acks, so there are no acks to lose (§2).
- **Watermark honesty (the Q-shaped judge, query-side):** claimed vs.
  served — every watermark value any node ever advertised is replayed
  against the journals: no record acked before that watermark may be
  missing from a `complete` read at it, and no `complete` read may have
  been served over coverage the journals show did not exist at serving
  time. Fail-closed refusals are correct outcomes; optimistic answers that
  happened to be right are still violations if coverage was unproven.
- **Per-key order and latest-view correctness (changelog datasets — the §3
  invariant LatestViewCorrect, judged end-to-end):** for every key, the
  served latest view equals the fold of that key's acked changelog in
  (origin, seq) order, across takeover and snapshot rollover; tombstones
  delete.
- **Retention honesty (Keep Rule 10 — SnapshotCovered, §3):** replayed
  from the journaled `Expire` events against read-back state: no expired
  changelog part lacks a committed snapshot covering its arrival range,
  and no acked record's last value became unreachable through expiry.
- **Cache transparency under eviction storms:** with forced Evict/Demote
  churn and DropWindow racing queries, every `complete` answer is a
  function of staging ∪ lake alone — any two cache states, including
  empty, yield the identical row set. This judge is the mechanical
  discharge of §2.4's read-answer equivalence — the half of the
  cache-transparency theorem the §3 lemma deliberately does not carry
  (§3.4), including obligation (c): no Evict-held lock ever blocks a read.

**Vacuity teeth.** A judge that never rejects anything is indistinguishable
from one too weak to reject anything, so the verdict is three-valued and
the exit codes are distinct: **Pass** (0), **Violation** (2), **NoVerdict**
(3) — and NoVerdict is never a pass. NoVerdict rules include: a fault
schedule that armed faults and fired none (measured from each injector's
own ledger, not assumed from the profile); a run with no observed
cross-node contention when contention is what the run exists to certify; an
ambiguous-outcome fraction above the profile's ceiling; a node whose
journals simply stop (a vanished machine is exactly the
under-reported-loss shape, so it accuses nothing and certifies nothing).
Additionally, each judge is periodically run against a **seeded-violation
replay** — a journal set with a known injected violation — and must convict
it; a judge that acquits its own seeded violation fails CI.

The distributed tier runs nightly and gates releases; it is too heavy for
per-PR.

## §8.5 Property tests

Run on every CI run inside the armed `test` ledger row.

Algebraic laws the design leans on are tested as laws, not as examples,
with shrinking on failure and every failing seed pinned as a permanent
regression case:

- **Codec round-trips**: OTLP and Arrow decode→canonicalize→encode
  round-trips; journal and manifest serialization stability across
  versions.
- **Lattice laws**: the schema-widening join is commutative, associative,
  and idempotent — the property that makes EvolveSchema crash-retry and
  concurrent-owner convergence correct (§6); tested over generated type
  pairs, including the JSON terminal.
- **Dedup determinism**: DedupCheck key derivation is a pure function of
  (tenant, content | idempotency token); equal inputs collide, unequal
  inputs don't, across process restarts.
- **Ring exactness**: the HRW placement function's minimal-disruption
  property is tested exactly, not approximately — adding or removing one
  node reassigns only the partitions that node gains or loses, and nothing
  else moves (§5).
- **Per-signal natural-key dedup**: drain-time winner selection is
  deterministic (smallest (origin, seq)) and produces identical sealed
  parts from any arrival permutation of the same acked set (§6).

## §8.6 Measured, ratcheted floors

Ledger rows: `coverage-floor`, `instr-gate`, `smoke` (pr, v0.1);
`mutation-floor` (nightly, v0.1 — ADR-0009's flagged deviation from this
section's per-PR posture). Floors live in `floors/`; the per-PR gating
choice of instruction counts over wall clocks is ADR-0005.

Quality gates are **checkable numbers recomputed by CI on every change** —
never trusted from a PR's description, never carried forward from a cached
value.

- **Coverage floor** and **mutation-testing floor** (pinned tool versions,
  pinned operator sets): CI recomputes each and fails below the recorded
  floor. Raising a floor is an ordinary commit; lowering one is a
  reviewed, named decision in the commit message — never "adjusted
  threshold."
- **Per-PR performance gates**: deterministic instruction-count
  measurements of the hot paths (Accept→ClientAck, DedupCheck, drain
  fold), gated at **baseline +15%**, where the baseline moves only via
  explicit baseline-update commits. Instruction counts are deterministic
  enough to gate per-PR where wall clocks are not; their known blind spot
  — fsync and I/O wait — is documented and covered by the nightly
  wall-clock bench card (§8.7), which blocks release on regression.
- **A 1M-record ingest smoke bound** per PR, catching order-of-magnitude
  regressions cheaply.

**The anti-gaming stance** — the gate philosophy. §11 makes it a Keep Rule,
CONSTITUTION.md's preamble quotes §11's framing sentence, and this section
is its enforcement surface; the text below is the same rule, not a second
one — amendments go through the s§9.6 procedure, never through a divergent
copy: no change may weaken, disable, or self-lower a gate under any framing
— "temporarily," "to unblock." A diff touching a threshold, an allowlist, a
lint suppression, or a timeout is reviewed line by line, never accepted on
its own explanation. Where a mechanical check cannot decide whether
something violates a rule, it treats it as if it does — **ambiguity fails
closed** — and narrowing a check's scope to dodge the ambiguity is the same
offense as weakening the check. A check whose subject vanished **fails, it
does not shrug**: a mutation floor whose crate was renamed, a trace gate
whose generator test was deleted, a broken variant that no longer compiles
— each is a red check demanding a decision, because a gate that silently
stopped measuring anything reports exactly the same green as a gate that
measured everything.

## §8.7 The bench card and the durability audit

Ledger row: `bench-card` (nightly, v0.4). Methodology document tracked at
issue #68 (`docs/bench/methodology.md`); the durability audit is issue #73.

Published performance is a correctness claim about honesty: the field's
recurring failure mode is throughput-without-durability — big numbers
measured with acks that promise nothing. Every DuckSpout headline number is
measured at **RF=2 with durable acks** (fsync + replication receipts before
ClientAck, §4–§5). RF=1 or no-fsync numbers are **never published alone**;
where shown for context they appear beside the durable figure, labeled.

**The nine-metric bench card**, every metric mapped to a pillar or a hard
rule:

| # | Metric | Floor (published only when beaten) |
|---|---|---|
| 1 | Ingest throughput per signal, per node | logs ≥200k rec/s · spans ≥150k/s (provisional — see note below) · datapoints ≥300k/s |
| 2 | Ack latency (durable) | p99 ≤25 ms, concurrency disclosed |
| 3 | Queryable-hot lag (ClientAck → visible to SQL) | p50 ≤100 ms |
| 4 | Hot-query latency under 80% ingest load | reported, trend-gated |
| 5 | Drain write amplification | →1.0 PUTs per sealed part (§6's one-PUT rule, measured) |
| 6 | Owner-kill recovery (kill → TakeoverDrain → coverage restored) | ≤15 s |
| 7 | Catalog-outage continuity | 0% ingest degradation through a 30-min outage |
| 8 | Chaos invariant score | pass/fail: zero acked-lost, zero `complete` violations (system-class datasets excluded by definition) |
| 9 | Resource envelope (CPU/RSS/disk at target load) | reported, trend-gated |

Span-throughput has no published industry comparable; the floor is
re-derived from the first internal measurement before any external
announcement.

**Disclosure norms**, adopted wholesale from ClickBench's: one disclosed
hardware class (16 vCPU / 32 GB, local NVMe — never network gp2-class
volumes, because fsync is the critical path — 3 nodes for cluster runs),
full configuration published, deviations labeled. Every throughput number
states its RF, fsync mode, and active chaos schedule. Chaos runs **inside**
the standard benchmark run, not in a separate friendlier universe: metric 8
is measured on the same run as metrics 1–4.

**The durability audit** is a Jepsen-style self-run harness, and its
defining discipline is that the verifying client is the test: the load
generator journals **every ack it receives**. After the chaos schedule —
node kills including owner-mid-drain, pauses, partitions, and **membership
churn**, which is in the v1 schedule because it is the fault class
real-world acked-loss reports implicate — and a fixed heal window (set in
the methodology document before the first run, never tuned to the result),
every journaled-acked record must be queryable at read concern `complete`.
The changelog scenario additionally asserts per-key order and latest-view
correctness through churn and takeover (§8.4's judges reused verbatim — the
audit is a CTK-distributed profile, not separate machinery). The full
methodology and raw journals are published with each report. An external
Jepsen engagement is planned post-1.0, after the self-run harness and at
least one public report exist — an external audit is worth buying only once
the internal one has stopped finding things.
