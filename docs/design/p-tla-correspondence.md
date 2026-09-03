# P ↔ TLA+ Correspondence Map: Replication

> **Provenance:** new analysis document, issue #133 (ADR-0012 step 2 groundwork)
> — **not** absorbed from `DUCKSPOUT.md`; there is no §n home for it. Built by
> close reading of `specs/DuckSpoutCore.tla`, `specs/Replication.tla`,
> `specs/Replication.cfg`, and `p/Replication/*.p` as they stand today.
> **Owning crate:** none — this is a specs/models cross-reference, not
> implementation-owned. Re-derive it (or at least re-check its tables) any
> time either `specs/Replication.tla` or `p/Replication/*.p` changes; nothing
> currently enforces that mechanically (§4).

## 1. What this document is, and is not

ADR-0012 (`docs/adr/0012-layered-refinement-pipeline.md`) makes the P model
a *gated* artifact: step 2 of the pipeline calls for P's checker-explored
executions to be "cross-checked against the TLA+ spec **mechanically**"
(trace refinement, the same machinery step 5 uses for Rust traces), and it
is explicit that an LLM reading both models and comparing them by eye "is
not independent evidence" of correspondence — the two models are authored
by the same class of agent reading the same design prose, so a second
transcription does not catch what the first one got wrong about the
protocol.

This document is exactly that kind of reading. It is useful as a reference
for anyone (human or agent) touching either model — a place to look up "what
does this P event mean in TLA+ terms" — and it is where a concrete,
non-hypothetical scope gap between the two models surfaces (§4.3). It is
**not** the mechanical refinement check ADR-0012 step 2 calls for; that
machinery does not exist yet (#130 is the tracked v0.2 execution plan for
building it). Treat every claim below as "as read," not as "as proven
equivalent."

Scope: replication only — `specs/Replication.tla` (which `EXTENDS
DuckSpoutCore` and instantiates its full shared `Next`) against
`p/Replication/*.p`. Other checked TLA+ modules (`Ingest.tla`, `Drain.tla`,
`Schema.tla`) have no P counterpart at all yet; this map does not attempt
to cover them.

## 2. The scenario each model checks

Both models check the **same narrative**: `docs/design/replication.md`
§5.6, "Node death, end to end" — an owner accepts and stages a write,
forwards it to its one replica, then dies; the replica either already has
the forwarded record (receipted) or does not, and either way must take over
the orphaned partition and (eventually) drain what it holds. Both explore
every relative ordering of forward-vs-crash the respective checker can
produce, in the smallest scope where takeover is not vacuous: 2 nodes,
RF = 2, one request.

- TLA+: `specs/Replication.tla`'s clean config (`Replication.cfg`) — `n1`
  (owner, `Crashable = {n1}`) accepts and stages `q1`, replicates to `n2`,
  crashes; `n2` observes no live claimant and takes over.
- P: `p/Replication/TestDriver.p`'s `TestTakeoverDrain` — `owner` accepts a
  write from a `Client`, forwards to `replica`; `owner` is sent `eDie`
  (halts for real) and `replica` is sent `eCrashSignal` (told out-of-band)
  in the same `entry`, and the checker explores every scheduling of
  Forward / Receipt / Die / CrashSignal.

The topology match is close and deliberate. What each model does with that
topology once takeover fires is not (§4.3).

## 3. Correspondence map

### 3.1 P events/handlers → TLA+ actions

| P event / handler (`p/Replication/*.p`) | TLA+ counterpart (`specs/DuckSpoutCore.tla` unless noted) | Correspondence |
|---|---|---|
| `eLink` (`Events.p`, wiring) | — | No TLA+ counterpart. Pure P scaffolding: P machines need an explicit peer reference because spawn order is circular; TLA+ nodes are just elements of the `Nodes` set and `RingPeers(p, n) == Nodes \ {n}` needs no wiring step. |
| `eWriteReq` handler (`Node.p`: `staged += key`, `holders[key] = 1`, `announce eAccepted`, `send peer, eForward`) | `Accept(n, q)` **+** `StageCommit(n, q)` **+** the origin's own `Forward(n, m, r)`, fused | TLA+ keeps admission (`Accept`, gated on `Rung(n) < 2`, `WindowClosed`), durable write (`StageCommit`), and the replication send (`Forward`) as three separately-interleavable actions, with `DedupCheck(n, q)` able to intervene between them (replay or throttle a duplicate). P's single scenario has one client, one key, so it collapses all of this into one atomic handler and has **no `DedupCheck` analog at all** — there is no dedup-key concept in the P model. There is also no ladder (`SoftLim`/`ThrottleLim`/`HardLim`/`Rung`) and no `WindowClosed` gating; every P write is unconditionally admitted. |
| `eForward` handler (`Node.p`: `staged += key`, `send origin, eReceipt`, conditionally `committed += key` + `announce eTakeoverDrain`, `announce eForwardHandled`) | `PeerApply(m, g)` **+** `Receipt(m, r)`, fused (plus the late-arrival branch of `TakeoverDrain`, see §3.2) | `PeerApply`'s guards — fencing (`g.inc >= highestSeen[m][g.rec.origin]`), gap-refusal (`g.rec.seq = AppliedThru(...) + 1`), `SchemaKnown`, the hard-rung refusal — have **no P counterpart**. P applies every forwarded record unconditionally. `Receipt` in TLA+ is a separately-schedulable action (a peer can hold an applied record for a while before receipting it); in P, receipting happens inline in the same handler that applies the record — no interleaving between "applied" and "receipted" is representable. |
| `eReceipt` handler (`Node.p`: `holders[key] += 1`; if `holders[key] >= 2` and pending, `send eWriteAck`) | `ClientAck(n, q)` (`Cardinality(H) >= RF`) | Faithful in spirit: P's `>= 2` is `Replication.cfg`'s `RF = 2` baked in as a literal rather than carried as a parameter. Granularity differs slightly — TLA+'s `ClientAck` is a distinct action that re-evaluates the guard whenever scheduled (it can be enabled without immediately firing); P re-checks and acts within the same handler invocation that received the triggering receipt. |
| `eCrashSignal` handler (`Node.p`: `peerDead = true`; sweep `staged \ committed`, `committed += k`, `announce eTakeoverDrain` per key; `announce eCrashSignalHandled`) | **No TLA+ action.** Explicitly an abstraction — see §4.1. | — |
| `eDie` handler (`Node.p`: `raise halt`) | `CrashNode(n)` (`alive' = FALSE`, `inflight' = {}`, `crashBudget' - 1`; staged/dedup/cache survive — "fsynced state survives (A1)") | Good match for the dying node's own transition. P's halted machine simply stops processing (its `staged`/`committed`/`holders` fields are inert, matching "fsynced state survives" — nothing in this scenario reads a dead node's state again). `Replication.cfg`'s `WipeBudget = 0` means `CrashWipe(n)` is unreachable there too, so P having no `CrashWipe` analog is not a scope gap *for this config*. |
| `eAccepted` (announced inside `eWriteReq` handling) | The moment `StageCommit(n, q)` adds `r` to `staged[n]` | **Naming trap**: P's event is called `eAccepted`, which echoes TLA+'s `Accept` action by name, but its own doc comment (`Events.p` lines 25-28) says what it actually tracks is "the point past which the system has made a durability commitment" — that is TLA+'s `StageCommit` moment (what `NoAckedLoss`'s own `staged[n]` disjunct anchors on), not `Accept`'s admission moment. Read `eAccepted` as "staged," not "accepted," when comparing to TLA+. |
| `eTakeoverDrain` (announced from both the late-Forward branch and the `eCrashSignal` sweep) | `TakeoverDrain(n, p)` **by name**, but see §4.3 for scope | Same name, materially different amount of work — flagged as a genuine divergence below, not glossed over here. |
| `eForwardHandled`, `eCrashSignalHandled` (`Events.p`, marker events for `NoAckedLoss.p`'s assert) | — | No TLA+ counterpart. Pure P scaffolding, invented so the hand-written `NoAckedLoss` spec machine (`p/Replication/NoAckedLoss.p`) has a well-defined point to assert at in a *bounded* scenario. Its own header explains why: an earlier `hot state`-liveness formulation of the same check never fired (#132 finding) because P's default bugfinding checker does not treat "terminated while hot" as a violation the way TLA+'s fairness-driven `~>` checking does. TLA+ needs no equivalent marker because TLC checks invariants at *every* reachable state by construction. |
| `eWriteAck` (Node → Client) | Client-visible effect of `resolved[q] = "acked"` becoming true | The `Client` machine (`Client.p`) only prints on receipt; TLA+ has no analogous "client machine," `resolved`/`ackEvidence` are just state components any spec can read directly. |
| `TestDriver.p` / `Spec.p` / `TestDecl.p` (scenario wiring, `test Replication [main = TestTakeoverDrain]: assert NoAckedLoss in {...}`) | `Replication.tla`'s `Init`/`Spec` + `Replication.cfg`'s constant assignment (`ReplicationAcceptorOf`, `ReplicationInitClaims`, `Crashable = {n1}`, `MaxCrashes = 1`) | Structural match for the scenario topology (§2). One instantiation vs. TLC's exhaustive walk over the whole reachable space from that same `Init` is the expected difference in kind between a P test and a TLC config, not a divergence. |

### 3.2 TLA+ actions with no P counterpart at all

`specs/README.md`'s module-ownership table lists `Replication.tla`'s owned
actions as: `Forward, PeerApply, Receipt, ClaimAdvertise, Heartbeat,
TakeoverDrain, CrashNode, CrashWipe, RecoverNode, FenceBoot, DegradedBoot`.
Of these, only `Forward`/`PeerApply`/`Receipt`/`TakeoverDrain`/`CrashNode`
have any P representation (§3.1, with the fusions and guard-drops noted
there). The rest:

| TLA+ action | P representation | Note |
|---|---|---|
| `ClaimAdvertise(n, p)` | None | P has no `claims` registry / `InitClaims` concept at all. The sole authorization for P's takeover is the local boolean `peerDead`; TLA+'s `TakeoverDrain` guard instead reads a global `claims` set and every node's `alive[]`. Quiescent in `Replication.cfg` too (`InitClaims` pre-seeds `n1` directly), so this is a wash for *this* scenario, not a live gap. |
| `Heartbeat(n)` | `eHeartbeat`/`eTick` handlers (`Node.p`), `TestHeartbeatDetection` (`TestDriver.p`) | **Partial, and now ahead of TLA+ here** — see §4.1 (updated). `Replication.cfg` still sets `MaxHb = 0` ("advisory, nothing reads it"), so TLA+ itself gives `Heartbeat` no teeth in this scope; P's round-counted TTL-lapse detection is real but narrower than §5.5/§6.1's full mechanism (no false-positive/flapping modeling, no takeover-suppression window, no interaction with incarnation fencing — §4.1). |
| `CrashWipe(n)` | None | `WipeBudget = 0` in `Replication.cfg` — unreachable in TLA+'s own clean config too. |
| `RecoverNode(n)` / `FenceBoot(n)` / `DegradedBoot(n)` | `eFenceBoot`/`eCatalogOutage`/`eCatalogRestored` handlers (`Node.p`), `TestFenceBootZombie` + `TestDegradedBoot` (`TestDriver.p`) | **Partial, and now wider** — see §4.2 (updated). `FenceBoot`'s incarnation-draw-and-persist has a P analog (`TestFenceBootZombie`); `DegradedBoot`'s catalog-outage boot split now has one too (`TestDegradedBoot`: a persisted incarnation booting into a catalog outage suppresses ownership actions until promotion, checked by `NoOwnershipWhileDegraded`). `RecoverNode`'s remaining surrounding machinery does not — see §4.2 for the precise boundary of what is and is not covered. |

## 4. Known gaps and divergences

### 4.1 `eCrashSignal` is a deliberate per-scenario abstraction; P now models heartbeat-TTL detection directly in a third scenario

*Updated: a third P test scenario, `TestHeartbeatDetection`
(`p/Replication/TestDriver.p`), now models the heartbeat-TTL detection
mechanism `eCrashSignal` previously stood in for unconditionally. What
follows is the corrected picture, not the original finding — see the PR
that added `eHeartbeat`/`eTick` for the verification evidence.*

`Events.p`'s `eCrashSignal` comment still says what it always has:
`eCrashSignal` "stand[s] in for heartbeat-TTL expiry," delivered to the
surviving replica directly rather than derived by it. `TestTakeoverDrain`
and `TestFenceBootZombie` both still use it, deliberately — those two
scenarios are about takeover-drain correctness and incarnation fencing
respectively, and neither needs the detection mechanism itself to be
real to exercise what they're actually testing. `TestHeartbeatDetection`
is the scenario that now makes the mechanism real: `Node.p` tracks
`lastHeartbeatRound`/`heartbeatTTL` per node, `eHeartbeat` carries a
logical `round` counter (standing in for wall-clock ticks — this
project's `R-determinism` bars real time from protocol crates, and the
same principle applies to this model), and a node's own `eTick` handler
derives peer death itself by checking `round - lastHeartbeatRound >=
heartbeatTTL`, rather than being told about it. The orphan-drain sweep
this triggers is the exact same `sweepOrphanedKeys` helper `eCrashSignal`'s
handler calls — one shared implementation, two triggers.

The important point for this map is still *why* TLA+ has nothing
resembling either P mechanism: TLA+'s actions read `alive[n]` as shared
state, synchronously, with no notion of one node learning about another's
death — `TakeoverDrain`'s guard (`~\E n1 \in Nodes : alive[n1] /\
HoldsClaim(n1, p)`) simply reads the global `alive` function. P's actors
do not share state; they only learn things via messages, so *some*
discrete step bridging that paradigm difference was always going to be
necessary — `eCrashSignal` is one such step (an oracle), `eHeartbeat`/
`eTick` are another (a derived one, closer to §5.5/§6.1's actual
mechanism). Neither is a transliteration of an under-specified TLA+
action.

This narrows what remains genuinely unmodeled: `Replication.cfg` sets
`MaxHb = 0` ("advisory, nothing reads it") on the TLA+ side, so TLA+
itself still gives `Heartbeat` no teeth in this scope (§3.2) — P is now
*ahead* of TLA+ here, not behind it, for the narrow slice
`TestHeartbeatDetection` covers (round-counted TTL-lapse detection by a
single replica against a single peer). What P's heartbeat model does
**not** cover, so this is a scope-parity gap in the other direction now:
false-positive/flapping behavior (a node wrongly declared dead under a
still-live peer whose heartbeats are merely delayed), the takeover-
suppression window (`docs/design/replication.md` §10 — planned restarts
must not trigger takeover), and any interaction between heartbeat-TTL
detection and incarnation fencing/reboot (`TestFenceBootZombie` and
`TestHeartbeatDetection` are two separate scenarios; nothing exercises a
node deriving its own peer's death from a heartbeat gap and *then*
observing that peer reboot under a fenced incarnation). None of these
have a TLA+ analog to be behind *or* ahead of — they are simply open
scope on both sides.

### 4.2 P now models `FenceBoot`/recovery, `DegradedBoot`, and checks `FencedZombie`/`NoOwnershipWhileDegraded` — narrower than TLA+'s, not absent

*Updated: a second P test scenario, `TestFenceBootZombie`
(`p/Replication/TestDriver.p`), closed the gap this section used to
describe as unrepresented for `FenceBoot`/`RecoverNode`. A fourth scenario,
`TestDegradedBoot`, now closes the `DegradedBoot` half of it too — see the
PR that added `eCatalogOutage`/`eCatalogRestored`/`NoOwnershipWhileDegraded`
for the verification evidence, and the PR before it (`eFenceBoot`/
`FencedZombie`) for the `FenceBoot` half. What follows is the corrected
boundary of what P covers here, not either original finding.*

`Replication.tla`'s own header is explicit that the recovery path matters
to this exact scenario: "`FenceBoot`'s recovery path is reachable too...
n1 can `FenceBoot` after n2's takeover and must be fenced out of
re-claiming or re-committing anything — `FencedZombie` is checked here for
real, not vacuously, because the crash that makes it interesting... is
exactly this scope's story." `Replication.cfg` checks `FencedZombie` (along
with the other nine state invariants) as a real, non-vacuous check in this
same crash/takeover narrative.

`p/Replication/TestFenceBootZombie` now sends the crashed `owner` a reboot
too, modeled as a **brand-new `Node` machine instance** (`newOwner`) —  P
machines cannot un-halt, so there is no way to resume the halted `owner`
itself; recovery is a fresh instance seeded via a new `eFenceBoot` event
with the old node's persistent logical identity (`nodeId`, a field
`Node.p` carries specifically so a rebooted instance and its crashed
predecessor can be recognized as "the same sender" despite being different
machine references) and a strictly higher `incarnation` than the crashed
instance had. Every `eForward`/`eReceipt` now carries `(originId/holderId,
inc)`, and the receiving `Node.p` handler tracks `highestSeen: map[int,
int]` per logical sender and refuses (no apply, no claim advertisement, no
receipt, no takeover) anything below what it has already seen — mirroring
§5.7's "peers... track the highest incarnation seen per node and reject
anything older." The new `FencedZombie` spec (`Spec.p`) asserts this
directly: no node ever accepts a message whose incarnation is strictly
below the highest it has already accepted from that same logical sender,
checked by recomputing an independent ground truth from the announced
`eFenceDecision` stream rather than reading `Node.p`'s own bookkeeping (so
a broken fence in `Node.p` cannot pass by construction). `just p-check
Replication` plus a direct `p check -tc FenceBootZombie` run both show 0
bugs across 5000 explored schedules on the honest model; a scratch,
uncommitted variant that disables the fence (`accept = true` in both
handlers) is caught in 7 schedules with a genuine zombie-acceptance trace
— see the PR for the exact trace excerpt.

**`DegradedBoot`, now modeled (`TestDegradedBoot`):** §5.7's boot-time
catalog-outage split reads "a node with a persisted incarnation boots into
replica-only degraded mode: it applies and receipts replication under its
existing incarnation but takes no ownership actions... [and] promotes
itself when the catalog returns and FenceBoot completes." `Node.p` now
carries a `degraded: bool` set at `eFenceBoot` time when a persisted
incarnation (`priorIncarnation > 0`) coincides with a catalog outage
(`eCatalogOutage`/`eCatalogRestored`, sent directly by the environment —
see below for what this deliberately does not model), and every
takeover-drain call site (`sweepOrphanedKeys`'s shared helper, plus
`eForward`'s own inline late-arrival check) is gated on `!degraded`. On
promotion (`eCatalogRestored`), the node re-checks for orphaned keys right
then — any takeover suppressed while degraded becomes eligible at that
moment, not merely for future triggers, matching "promotes itself... and
FenceBoot completes." `NoOwnershipWhileDegraded` (`Spec.p`) asserts this
directly: no node ever announces `eTakeoverDrain` while degraded, checked
by recomputing each node's degraded status from the announced
`eDegradedChanged` stream rather than reading `Node.p`'s own `degraded`
field — the same independent-ground-truth convention `FencedZombie`
already established (see its header comment for why). `just p-check
Replication` plus direct `p check -tc FenceBootZombie`, `-tc
HeartbeatDetection`, and `-tc DegradedBoot` runs all show 0 bugs (1000
schedules each; `DegradedBoot` also clean at 5000) on the honest model; a
scratch, uncommitted variant that drops the `!degraded` guard from
`sweepOrphanedKeys` is caught in 5 schedules with a genuine
still-degraded-takeover trace — see that PR for the exact excerpt.

What this deliberately does **not** model, so it is a narrower
representation of §5.7 rather than the real thing: there is no modeled
catalog service at all, only a boolean reachability flag the environment
flips directly (`eCatalogOutage`/`eCatalogRestored`) — no request/timeout
shape, no partial-reachability, nothing resembling an actual catalog-DB
client. §5.7's *other* boot case — "a genuinely new node[,] no persisted
incarnation[,] waits, in a typed startup state" — has no P representation
at all; `TestDegradedBoot` only exercises the persisted-incarnation branch
(`Node.p`'s `eFenceBoot` handler never lets `priorIncarnation = 0` produce
`degraded = true`, matching the spec text, but nothing models the waiting
new-node path itself). And the *combination* of heartbeat-TTL detection
with `DegradedBoot` remains open: `TestDegradedBoot` deliberately reuses
`eCrashSignal`'s oracle (matching `TestFenceBootZombie`'s and
`TestTakeoverDrain`'s convention) rather than `TestHeartbeatDetection`'s
`eTick` path, so nothing exercises a degraded node deriving its own peer's
death from a heartbeat gap while still suppressing ownership actions. Also
still open, unchanged from the original finding: `RecoverNode`'s remaining
surrounding machinery, and — the same gap §4.3 describes in more detail —
any of TLA+'s discrete claim/seal/commit-guard steps that `FencedZombie`
also polices on the drain-commit side (`CommitGuardsHold`'s `pt.inc =
inc[pt.sealer]`). **Candidate follow-up**, unchanged from the original
finding: widen P's takeover handling into discrete claim/seal/commit steps
with guards (tracked the same way as §4.3's).

### 4.3 `eTakeoverDrain` fuses claim-acquisition with commit; TLA+ keeps them apart

This is the sharpest same-name/different-meaning gap in the whole map.

In TLA+, `TakeoverDrain(n, p)` does exactly one thing: it adds `<<n, p>>`
to the advisory `claims` registry, gated on no live node currently holding
that claim. It does **not** seal, put, or commit anything. Actually
draining the window is a separate sequence of `Drain.tla`-owned actions —
`SealPart` (gated on `HoldsClaim(n, p)` and `WindowClosed`), `PutPart`, and
`LakeCommitOk` (gated on `CommitGuardsHold`: `pt.inc = inc[pt.sealer]`
matching the catalog's minted incarnation, `UniqueOk` — one window part per
window ever, spanning `lake \cup expired` — and `DisjointOk` for
supplements) — each independently interleavable, each with its own guard.
`SingleDrainCommit` and `FencedZombie` are invariants precisely *about*
those guards holding.

`p/Replication/Node.p`'s `eForward` and `eCrashSignal` handlers do the
whole thing in one line: `committed += (k); announce eTakeoverDrain, ...`.
There is no seal, no put, no commit-guard, no incarnation check, no
uniqueness check — "committed" in the P model is a bare boolean flip on
detecting an orphaned key. `eTakeoverDrain`'s own doc comment in `Events.p`
even states the fusion outright: "A replica claims an orphaned key and
drains (commits) it" — one event, both meanings.

Consequence: P's `NoAckedLoss` check, however useful for what it does
check, cannot exercise or catch anything in the double-drain hazard family
that `SingleDrainCommit`/`CommitGuardsHold` exist to prevent — that
specific hazard surface (drain-commit uniqueness/disjointness) still has
**zero** P coverage, only TLA+ coverage (and TLA+ only reaches it because
`Replication.tla` turns `TakeoverOn = TRUE` over the full shared `Next`,
which includes `Drain.tla`'s actions). §4.2's update narrows this: P's
`FencedZombie` now covers the *message-fencing* half of the incarnation
hazard family (a stale-incarnation Forward/Receipt from a rebooted node's
former self) but not the *commit-guard* half (`pt.inc = inc[pt.sealer]` at
`LakeCommitOk`, gated on a seal/put/commit sequence P's `eTakeoverDrain`
still fuses into one boolean flip, as above). This is the same underlying
gap as §4.2 (no discrete claim/seal/commit steps), viewed from the
commit-guard side rather than the recovery side, and has the same
disposition: a candidate follow-up issue to widen the P model's takeover
handling into discrete claim/seal/commit steps with guards, not a fix made
in the PR that added `FencedZombie`'s message-fencing coverage.

### 4.4 Property coverage: TLA+ checks ten invariants and two properties here; P checks three, none a transliteration

*Updated alongside §4.2: `FencedZombie` (`Spec.p`) is a new third P
property, added by the same PR that closed §4.2's original gap.*

`Replication.cfg` checks all ten of `DuckSpoutCore.tla`'s state invariants
(`DurableAck`, `NoAckedLoss`, `WatermarkHonesty`, `CacheTransparency`,
`GapFreedom`, `SingleDrainCommit`, `FencedZombie`, `LossLedgerTruthful`,
`SnapshotCovered`, `LatestViewCorrect`) plus the two properties
`LadderMonotone` and `EveryRequestResolves`. It explicitly does **not**
check `WatermarkEventuallyAdvances` in this scope — TLC falsifies it here,
confirmed as a permanently-red finding
(`specs/broken/Finding_TakeoverOrphanedSeal.cfg`), not a silently-dropped
check.

`p/Replication/*.p` checks three properties today, none a 1:1
transliteration of a TLA+ invariant:

- `NoAckedLoss.p`'s own header says what kind of correspondence it is:
  "Mirrors `DuckSpoutCore.tla`'s `NoAckedLoss`/`GapFreedom` in spirit, not
  text — this is a hand-written P analog, not a transliteration." It is a
  hybrid, informal analog of *two* of TLA+'s twelve checked properties, not
  a 1:1 rendering of either.
- `ClaimAdvertiseOnce` (`Spec.p`) has no directly-named TLA+ invariant twin
  at all: `DuckSpoutCore.tla`'s `ClaimAdvertise(n, p)` action's own guard
  prevents redundant advertisement structurally, but nothing in
  `Replication.cfg`'s checked-invariant list separately asserts that the
  way `NoAckedLoss`/`FencedZombie`/etc. are asserted. P's version makes
  that implicit structural guarantee an explicit, independently-checked
  safety property TLA+ does not separately check for.
- `FencedZombie` (`Spec.p`) **is** a named analog of
  `DuckSpoutCore.tla`'s `FencedZombie` invariant (`staleApplied = {}`,
  guarded by `PeerApply`'s `g.inc >= highestSeen[m][origin]`) — matching
  name, matching intent, per §4.2's convention of naming P analogs after
  their TLA+ counterpart without transliterating the text. Its scope is
  narrower than TLA+'s: message-level Forward/Receipt fencing only, not
  the commit-guard half (`CommitGuardsHold`'s `pt.inc = inc[pt.sealer]`) —
  see §4.2 and §4.3 for the exact boundary.

The other nine (`DurableAck`, `WatermarkHonesty`, `CacheTransparency`,
`SingleDrainCommit`, `LossLedgerTruthful`, `SnapshotCovered`,
`LatestViewCorrect`, `LadderMonotone`, `EveryRequestResolves`) have no P
counterpart of any kind today. This is consistent with the P model's own
commit history describing it as an explicitly scoped-down slice, and this
map should not be read as implying broader property coverage than these
three checks — one of which (`FencedZombie`) is itself narrower than its
TLA+ namesake, per §4.2.

## 5. Divergence protocol

ADR-0012 step 2 says plainly: "Any divergence is itself a finding." This
section makes that concrete for replication.

**What "divergence" means here**, in increasing order of severity:

1. **Scope-parity gap** — one model represents a mechanism or checks a
   property the other doesn't yet, but neither *contradicts* the other
   where they overlap. §4.2, §4.3, and §4.4 above are all of this kind
   today: P is behind TLA+ in what it represents, not disagreeing with it.
2. **Checker-vocabulary gap** — TLC explores a reachable state (or an
   armed `broken/` variant confirms a specific finding, e.g.
   `Finding_TakeoverOrphanedSeal`) that the P model's checker structurally
   cannot produce or distinguish, because the P model has no variables or
   transitions in that area. Also §4.3: P cannot find, or rule out, the
   `SingleDrainCommit`/`CommitGuardsHold` double-drain hazard class,
   because it has no representation of the discrete claim/seal/commit
   steps those invariants guard — narrower now than it reads in §4.2's
   history, since P's own `FencedZombie` does cover that invariant's
   message-fencing half (§4.2).
3. **Genuine disagreement** — one model's guard permits a transition the
   other's guard forbids, in an area both models *do* represent (not a
   scope gap). None found in this reading; would be the most serious class
   were one found, since it would mean the two models encode different
   beliefs about the same protocol step.
4. **Mechanical cross-check failure** — once ADR-0012 step 2's actual
   refinement machinery exists (#130), P's checker-explored executions
   fail to refine against TLA+'s state graph, or vice versa, in a way this
   hand-read correspondence map didn't anticipate. This is the failure
   mode ADR-0012's own "Revisit when" section names as grounds to fall
   back to ADR-0011's evidence-triggered posture if it "proves impractical."

**What happens when one is found**, grounded in ADR-0012's actual
purpose — verify *before* implementing, so that "Rust third, built from
both" (step 3) starts from spec-supplied invariants and P-supplied
implementation-granularity decisions that have *already* been reconciled,
not from two silently-diverging sources of truth:

- **Class 1 (scope-parity)** is not a pipeline blocker by itself. It is
  filed as a tracked issue (this document now names one remaining
  candidate: §4.2/§4.3's claim/seal/commit widening — §4.2's original
  `TestFenceBootZombie` candidate is done, landed alongside this update)
  and left as backlog, because ADR-0012's own framing is that the P model
  "earns its
  trust by being executed and refined" over time, not by being complete on
  day one. It **does** become relevant the moment step 3 (Rust) needs to
  rely on a guarantee only in the gap — at that point the gap must close
  before that slice of Rust is built "from both," per the pipeline's
  ordering (each layer starts once the layer below it is validated).
- **Class 2** is filed the same way as Class 1 when it traces back to a
  scope gap (as both instances found here do); it escalates toward Class 3
  treatment if closing the gap surfaces an actual disagreement rather than
  just missing machinery.
- **Class 3 (genuine disagreement)** blocks: per the pipeline's own
  ordering, step 3 does not start (or, if already started, does not merge)
  for the affected protocol slice until one model is corrected to match
  the other's finding, with the correction and its reasoning recorded (a
  `TRANSCRIPTION-NOTES.md`-style note for a TLA+ fix, or an ADR amendment
  via the s§9.6 procedure if the disagreement touches a settled decision).
  Neither model is presumptively right — ADR-0012's whole argument for
  wanting a second, checker-executed model is that an LLM's read of the
  design prose can be wrong in the same way twice, so a genuine
  disagreement is exactly the signal the pipeline exists to surface, and
  resolving it requires going back to the design prose (`specs/formal-core.md`
  / `docs/design/replication.md`) and the mechanism in question, not
  picking whichever model was authored first.
- **Class 4 (mechanical cross-check failure)** is the condition ADR-0012's
  "Revisit when" section already names explicitly: if step 5's P-side
  conformance proves impractical, the pipeline's honesty depends on it, and
  the fallback is ADR-0011's evidence-triggered posture — this is a
  methodology-level decision (owner ruling), not something a single PR
  resolves.
- **Fairness-assumption divergences** (`WF_vars`/`SF_vars` clauses) get
  ADR-0012's own named discipline regardless of which class they fall
  under: every fairness clause must cite the concrete implementation
  mechanism that supplies it, and its armed `broken/` variant (fairness
  removed) must be shown to break the liveness property it was added for.
  `CoreFairness`/`FairnessBase` in `DuckSpoutCore.tla` are the current
  citations on the TLA+ side; P's bugfinding-checker posture (§4's
  `eForwardHandled`/`eCrashSignalHandled` scaffolding, chosen specifically
  because P's default checker does not do fairness-style liveness the way
  TLC's `~>` does) means P currently makes no fairness claims of its own to
  reconcile against TLA+'s.
