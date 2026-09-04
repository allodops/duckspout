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
| `eForward` handler (`Node.p`: fencing gate first (`accept = fwd.inc >= highestSeen[originId]`; `announce eFenceDecision` either way, no further processing if fenced out); when fenced in, a three-way branch on `originSeq` vs. the sender's `appliedThru` watermark — idempotent duplicate (`originSeq <= thru`: `send eReceipt` only if `fwd.key in staged`, no restaging, no re-advertise, no takeover check), genuine next record (`originSeq == thru + 1`: `announce eGapDecision(accepted=true)`, `appliedThru` advances, `staged += key`, claim-advertise once, `send eReceipt`, conditionally `committed += key` + `announce eTakeoverDrain`), or gap refusal (`originSeq > thru + 1`: `announce eGapDecision(accepted=false)`, no apply, no claim, no receipt, no takeover); `announce eForwardHandled` unconditionally at the end regardless of branch) | `PeerApply(m, g)` **+** `Receipt(m, r)`, fused (plus the late-arrival branch of `TakeoverDrain`, see §3.2) | `PeerApply`'s guards: fencing (`g.inc >= highestSeen[m][g.rec.origin]`) **and** gap-refusal (`g.rec.seq = AppliedThru(...) + 1`) now both have a P counterpart (§4.4/§4.5, `GapFreedom`, added alongside `TestGapFreedom`, #132) — `Node.p`'s `eForward` handler tracks a per-sender `appliedThru` watermark and refuses (no apply, no claim, no receipt, no takeover) any `originSeq` that would leave a gap, acknowledging an at-or-below-watermark `originSeq` idempotently without re-applying (and only when the key at that watermark is genuinely the one already staged — #192's ACPR finding 1/2, see `Node.p`'s own comment), matching this section's own `PeerApply` row exactly. `SchemaKnown` and the hard-rung refusal still have **no P counterpart** — P's minimal slice has no schema-lattice or ladder-rung concept at all (§3.2). `Receipt` in TLA+ is a separately-schedulable action (a peer can hold an applied record for a while before receipting it); in P, receipting happens inline in the same handler that applies the record — no interleaving between "applied" and "receipted" is representable. |
| `eReceipt` handler (`Node.p`: `holders[key] += 1`; if `holders[key] >= 2` and pending, `send eWriteAck`) | `ClientAck(n, q)` (`Cardinality(H) >= RF`) | Faithful in spirit: P's `>= 2` is `Replication.cfg`'s `RF = 2` baked in as a literal rather than carried as a parameter. Granularity differs slightly — TLA+'s `ClientAck` is a distinct action that re-evaluates the guard whenever scheduled (it can be enabled without immediately firing); P re-checks and acts within the same handler invocation that received the triggering receipt. |
| `eCrashSignal` handler (`Node.p`: `peerDead = true`; sweep `staged \ committed`, `committed += k`, `announce eTakeoverDrain` per key; `announce eCrashSignalHandled`) | **No TLA+ action.** Explicitly an abstraction — see §4.1. | — |
| `eDie` handler (`Node.p`: `raise halt`) | `CrashNode(n)` (`alive' = FALSE`, `inflight' = {}`, `crashBudget' - 1`; staged/dedup/cache survive — "fsynced state survives (A1)") | Good match for the dying node's own transition. P's halted machine simply stops processing (its `staged`/`committed`/`holders` fields are inert, matching "fsynced state survives" — nothing in this scenario reads a dead node's state again). |
| `eCrashWipe` handler (`Node.p`: `raise halt`, same body as `eDie`) | `CrashWipe(n)` (`wiped' = wiped \cup {n}`, `staged'/cache'/dedup'/inflight' = {}`, `alive' = FALSE` — its own comment: "a wiped node never re-enters") | **New, and only a partial, honestly-caveated match — see §4.2 (updated).** `TestNewNodeBoot` (`TestDriver.p`) sends this to the dying node, but the P handler is behaviorally indistinguishable from `eDie`'s: both just `raise halt`. Clearing `staged`/`cache`/`dedup` first, matching TLA+'s guard, would be dead code in this model (nothing reads a halted instance's fields again) — the real distinguishing behavior lives entirely in what the *environment* does for the reboot afterward, and even there it is not the same thing TLA+'s `CrashWipe` describes (see §4.2: TLA+ permanently retires `n`; P's replacement is a different `nodeId` altogether). `Replication.cfg`'s `WipeBudget = 0` still makes `CrashWipe` unreachable in TLA+'s own clean config, so there is no TLA+ execution to mechanically cross-check this against even in principle. |
| `eAccepted` (announced inside `eWriteReq` handling) | The moment `StageCommit(n, q)` adds `r` to `staged[n]` | **Naming trap**: P's event is called `eAccepted`, which echoes TLA+'s `Accept` action by name, but its own doc comment (`Events.p` lines 25-28) says what it actually tracks is "the point past which the system has made a durability commitment" — that is TLA+'s `StageCommit` moment (what `NoAckedLoss`'s own `staged[n]` disjunct anchors on), not `Accept`'s admission moment. Read `eAccepted` as "staged," not "accepted," when comparing to TLA+. |
| `eTakeoverDrain` (announced from both the late-Forward branch and the `eCrashSignal` sweep) | `TakeoverDrain(n, p)` **by name**, but see §4.3 for scope | Same name, materially different amount of work — flagged as a genuine divergence below, not glossed over here. |
| `eForwardHandled`, `eCrashSignalHandled` (`Events.p`, marker events for `NoAckedLoss`'s assert) | — | No TLA+ counterpart. Pure P scaffolding giving the direct-assert `NoAckedLoss` spec a well-defined point to check at in a *bounded* scenario, kept alongside `NoAckedLossLive` (a genuine `hot state` liveness twin checking the identical property — see `Spec.p`'s header for both). An earlier attempt at the `hot state` formulation never fired for a deliberately-broken variant and wrongly concluded P's checker doesn't flag "terminated while hot" for a bounded scenario; **corrected** — P's own manual documents that rule explicitly (verified via Coyote's docs and a live p-org/P GitHub discussion, then confirmed empirically with a direct retest: `NoAckedLossLive` does fire, 100% of schedules, once wired correctly). The earlier "never fired" result was the same test-declaration wiring bug (`assert SpecName in {...}` never applied) found and fixed elsewhere in this model's history — the monitor was never running, not failing to detect anything. TLA+ needs no equivalent marker because TLC checks invariants at *every* reachable state by construction. |
| `eWriteAck` (Node → Client) | Client-visible effect of `resolved[q] = "acked"` becoming true | The `Client` machine (`Client.p`) only prints on receipt; TLA+ has no analogous "client machine," `resolved`/`ackEvidence` are just state components any spec can read directly. |
| `TestDriver.p` / `Spec.p` / `TestDecl.p` (scenario wiring, `test Replication [main = TestTakeoverDrain]: assert NoAckedLoss in {...}`) | `Replication.tla`'s `Init`/`Spec` + `Replication.cfg`'s constant assignment (`ReplicationAcceptorOf`, `ReplicationInitClaims`, `Crashable = {n1}`, `MaxCrashes = 1`) | Structural match for the scenario topology (§2). One instantiation vs. TLC's exhaustive walk over the whole reachable space from that same `Init` is the expected difference in kind between a P test and a TLC config, not a divergence. |

### 3.2 TLA+ actions with no P counterpart at all

`specs/README.md`'s module-ownership table lists `Replication.tla`'s owned
actions as: `Forward, PeerApply, Receipt, ClaimAdvertise, Heartbeat,
TakeoverDrain, CrashNode, CrashWipe, RecoverNode, FenceBoot, DegradedBoot`.
Of these, `Forward`/`PeerApply`/`Receipt`/`TakeoverDrain`/`CrashNode`/
`CrashWipe` have some P representation (§3.1, with the fusions,
guard-drops, and — for `CrashWipe` — the honest correspondence caveat
noted there). The rest:

| TLA+ action | P representation | Note |
|---|---|---|
| `ClaimAdvertise(n, p)` | None | P has no `claims` registry / `InitClaims` concept at all. The sole authorization for P's takeover is the local boolean `peerDead`; TLA+'s `TakeoverDrain` guard instead reads a global `claims` set and every node's `alive[]`. Quiescent in `Replication.cfg` too (`InitClaims` pre-seeds `n1` directly), so this is a wash for *this* scenario, not a live gap. |
| `Heartbeat(n)` | `eHeartbeat`/`eTick` handlers (`Node.p`), `TestHeartbeatDetection` (`TestDriver.p`) | **Partial, and now ahead of TLA+ here** — see §4.1 (updated). `Replication.cfg` still sets `MaxHb = 0` ("advisory, nothing reads it"), so TLA+ itself gives `Heartbeat` no teeth in this scope; P's round-counted TTL-lapse detection is real but narrower than §5.5/§6.1's full mechanism (no false-positive/flapping modeling, no takeover-suppression window, no interaction with incarnation fencing — §4.1). |
| `CrashWipe(n)` | `eCrashWipe` handler (`Node.p`), `TestNewNodeBoot` (`TestDriver.p`) | **New, but narrower than the name suggests — see §4.2 (updated).** `WipeBudget = 0` in `Replication.cfg` still makes `CrashWipe` unreachable in TLA+'s own clean config, so there is no TLA+ execution to cross-check this against even in principle. What P models under this name is not TLA+'s `CrashWipe` recovering — TLA+'s own comment says a wiped node "never re-enters," full stop, and `Nodes` is a fixed set with no dynamic-membership concept to reissue an identity through. It is a P model of §7's *separate* "genuinely new node" prose instead, which has no TLA+ action of its own at all (see §4.2). |
| `RecoverNode(n)` / `FenceBoot(n)` / `DegradedBoot(n)` | `eFenceBoot`/`eCatalogOutage`/`eCatalogRestored` handlers (`Node.p`), `TestFenceBootZombie` + `TestDegradedBoot` + `TestNewNodeBoot` (`TestDriver.p`) | **Partial, and now wider still** — see §4.2 (updated). `FenceBoot`'s incarnation-draw-and-persist has a P analog (`TestFenceBootZombie`); `DegradedBoot`'s catalog-outage boot split has one too (`TestDegradedBoot`); and `TestNewNodeBoot` now models the boot-time behavior of a node with no persisted incarnation at all (`Node.p`'s `Waiting` state, checked by `NoIdentityWhileWaiting`) — a case §7 describes but that maps to **no TLA+ action whatsoever** (`Init` already assumes every node's first boot succeeded), not a partial rendering of one. `RecoverNode`'s remaining surrounding machinery does not have a P counterpart — see §4.2 for the precise boundary of what is and is not covered. |

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

### 4.2 P now models `FenceBoot`/recovery, `DegradedBoot`, the genuinely-new-node boot case, and checks `FencedZombie`/`NoOwnershipWhileDegraded`/`NoIdentityWhileWaiting` — narrower than TLA+'s, not absent

*Updated: a second P test scenario, `TestFenceBootZombie`
(`p/Replication/TestDriver.p`), closed the gap this section used to
describe as unrepresented for `FenceBoot`/`RecoverNode`. A fourth scenario,
`TestDegradedBoot`, closed the `DegradedBoot` half of it too. A fifth,
`TestNewNodeBoot`, now closes §7's *other* boot case — a genuinely new
node with no persisted incarnation — which the update before this one had
explicitly left as still-open scope (see the paragraph this update
replaces below). See the PR that added `eCrashWipe`/`Node.p`'s `Waiting`
state/`NoIdentityWhileWaiting` for the verification evidence on this
latest addition, the PR before it (`eCatalogOutage`/`eCatalogRestored`/
`NoOwnershipWhileDegraded`) for the `DegradedBoot` half, and the one before
that (`eFenceBoot`/`FencedZombie`) for the `FenceBoot` half. What follows
is the corrected boundary of what P covers here, not any of the three
original findings.*

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

**Known divergence, flagged here (ACPR #197 MEDIUM-5), not fixed in `p/`:**
`Node.p`'s `eFenceBoot` handler bumps the incarnation unconditionally
(`incarnation = fb.priorIncarnation + 1`) BEFORE it even checks
`catalogOutage`/`fb.priorIncarnation` to decide whether to also set
`degraded` — so `Node.p` bumps the incarnation on the `DegradedBoot` branch
too. `specs/DuckSpoutCore.tla`'s own `DegradedBoot(n)` action does the
opposite: `UNCHANGED <<catalogSeq, inc, ...>>` — the incarnation is
explicitly NOT bumped while degraded, only the boolean `degraded` set membership
changes. The Rust implementation (`crates/duckspout-replication/src/boot.rs`)
follows the TLA+ action, not `Node.p`, on this specific branch — `boot.rs`'s
own module doc says so explicitly. This PR does not change `p/` to match
TLA+ here (out of this PR's scope, which touches no `p/` file); it is named
as open scope for whoever next touches `TestDegradedBoot`. Worth carrying
forward for issue #54: an un-bumped degraded reboot resumes under the SAME
incarnation its dead predecessor held, so it cannot fence out that
predecessor as a zombie by incarnation alone if the "dead" node was actually
just partitioned, not crashed.

**The genuinely-new-node boot case, now modeled too (`TestNewNodeBoot`):**
§7's other boot-time split reads "Only a genuinely new node — no persisted
incarnation — waits, in a typed startup state. It has no identity to be
safely partial with." `Node.p` now has a second named state, `Waiting`
(the "typed startup state" itself), entered from `eFenceBoot`'s handler
exactly when a node's very first fence attempt (`priorIncarnation = 0` —
nothing persisted to bump) cannot complete because the catalog is down.
Unlike `DegradedBoot`'s persisted-incarnation branch, this node draws
*no* incarnation at all while waiting, and every handler in `Waiting`
either drops an inbound message outright (`ignore`) or does nothing
observable — no claim advertisement, no receipt, no takeover-drain — until
`eCatalogRestored` lets the deferred fence finally complete and the node
returns to `Active`. `NoIdentityWhileWaiting` (`Spec.p`) asserts this
directly: no node ever announces `eClaimAdvertise` or `eTakeoverDrain`
while it is in `Waiting`, checked the same independent-ground-truth way as
`FencedZombie`/`NoOwnershipWhileDegraded` (recomputed from the announced
`eWaitingChanged` stream, not read off `Node.p`'s own `waitingForFence`
field). `just p-check Replication` plus direct `p check -tc
FenceBootZombie`, `-tc HeartbeatDetection`, `-tc DegradedBoot`, and `-tc
NewNodeBoot` runs all show 0 bugs (1000 schedules each; `NewNodeBoot` also
clean at 5000) on the honest model; a scratch, uncommitted variant that
lets `Waiting` process `eForward` the way `Active` does (i.e., drops the
state gate) is caught in 70 schedules with a genuine
claim-while-waiting trace — see that PR for the exact excerpt.

A necessary correction while adding this: the natural first reading of
"model `CrashWipe`" turns out not to be what `TestNewNodeBoot` does, and
saying so plainly matters more than the scenario itself. `specs/
DuckSpoutCore.tla`'s `CrashWipe(n)` guards both `FenceBoot(n)` and
`DegradedBoot(n)` on `n \notin wiped` forever after — its own comment
reads "a wiped node never re-enters" — and TLA+'s `Nodes` is a *fixed*
set with no dynamic-membership concept at all, so there is no TLA+
notion of "a replacement for `n`" to model in the first place. `eCrashWipe`
(the new P event `TestNewNodeBoot` sends to the dying `owner`) mirrors
`CrashWipe`'s effect on the dying node itself only in name and halting
behavior (its handler is identical to `eDie`'s: nothing in this model
reads a halted instance's fields again, so faithfully clearing
`staged`/`cache`/`dedup` first would be dead code — see the `eCrashWipe`
row in §3.1). What follows it — a brand-new `Node` instance with a
*different* `nodeId` (3, never owner's own 1) — is a P model of §7's
separate "genuinely new node" prose, which corresponds to **no TLA+
action whatsoever**: `Init` already assumes every node's first-ever boot
succeeded (`alive = [n \in Nodes |-> TRUE]`, `inc = [n \in Nodes |-> 0]`
from turn zero), so a fresh node hitting trouble on its very first boot is
not a reachable `DuckSpoutCore.tla` transition to begin with, not merely
one P represents narrowly. This is a stronger claim than the usual
scope-parity gap (§5, Class 1): it is not that TLA+ has the mechanism and
P's is narrower, but that *neither* model has a transition for this
specific real-system moment — the gap is symmetric, not P being behind.

What this deliberately does **not** model, so it is a narrower
representation of §7 rather than the real thing: there is no modeled
catalog service at all, only a boolean reachability flag the environment
flips directly (`eCatalogOutage`/`eCatalogRestored`) — no request/timeout
shape, no partial-reachability, nothing resembling an actual catalog-DB
client (same limitation `DegradedBoot`'s own paragraph above already
notes). And the *combination* of heartbeat-TTL detection with either
`DegradedBoot` or the new waiting state remains open: `TestDegradedBoot`
and `TestNewNodeBoot` both deliberately reuse `TestFenceBootZombie`'s
environment-driven conventions (`eCrashSignal`/`eCatalogOutage`,
`eFenceBoot`) rather than `TestHeartbeatDetection`'s `eTick` path, so
nothing exercises a degraded-or-waiting node deriving its own peer's
death from a heartbeat gap. Also still open, unchanged from the original
finding: `RecoverNode`'s remaining surrounding machinery, and — the same
gap §4.3 describes in more detail — any of TLA+'s discrete claim/seal/
commit-guard steps that `FencedZombie` also polices on the drain-commit
side (`CommitGuardsHold`'s `pt.inc = inc[pt.sealer]`). **Candidate
follow-up**, unchanged from the original finding: widen P's takeover
handling into discrete claim/seal/commit steps with guards (tracked the
same way as §4.3's).

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

### 4.4 Property coverage: TLA+ checks ten invariants and two properties here; P checks six, none a transliteration

*Updated alongside §4.2, in four steps: `FencedZombie` (`Spec.p`) was a
new third P property, added by the PR that closed the `FenceBoot`/
`RecoverNode` half of §4.2's original gap. `NoOwnershipWhileDegraded`
followed as a fourth, added by the PR that closed the `DegradedBoot` half
— this document's own count here was not updated at the time, an
inconsistency corrected now rather than carried forward.
`NoIdentityWhileWaiting` is the fifth, added by the PR that closed §4.2's
genuinely-new-node-boot gap. `GapFreedom` is the sixth, added by the PR
that closed §3.1's own gap-refusal gap (`TestGapFreedom`, #132; that
section's `eForward`/`PeerApply` row previously named gap-refusal as
having no P counterpart while fencing did, and is now corrected in place
rather than left describing the pre-fix model — not §4.2, which is about
`FenceBoot`/`RecoverNode`/`DegradedBoot`/new-node-boot and never mentions
gap-refusal at all; see §4.5 for `GapFreedom`'s own narrative and
teeth-proof evidence).*

`Replication.cfg` checks all ten of `DuckSpoutCore.tla`'s state invariants
(`DurableAck`, `NoAckedLoss`, `WatermarkHonesty`, `CacheTransparency`,
`GapFreedom`, `SingleDrainCommit`, `FencedZombie`, `LossLedgerTruthful`,
`SnapshotCovered`, `LatestViewCorrect`) plus the two properties
`LadderMonotone` and `EveryRequestResolves`. It explicitly does **not**
check `WatermarkEventuallyAdvances` in this scope — TLC falsifies it here,
confirmed as a permanently-red finding
(`specs/broken/Finding_TakeoverOrphanedSeal.cfg`), not a silently-dropped
check.

`p/Replication/*.p` checks six properties today, none a 1:1
transliteration of a TLA+ invariant:

- `NoAckedLoss.p`'s own header says what kind of correspondence it is:
  "Mirrors `DuckSpoutCore.tla`'s `NoAckedLoss` in spirit, not text — this
  is a hand-written P analog, not a transliteration." (Its header used to
  also claim `GapFreedom` "in spirit," back when `GapFreedom` had no
  dedicated P analog of its own — see the next bullet; that claim was
  retired from `NoAckedLoss`'s header once it would have double-counted
  the same property under two spec names.)
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
- `NoOwnershipWhileDegraded` (`Spec.p`) has no directly-named TLA+
  invariant twin either, same shape as `ClaimAdvertiseOnce` above:
  `DegradedBoot(n)`'s own guards (`~alive[n] /\ n \notin wiped /\ inc[n] >
  0`) and `TakeoverDrain(n, p)`'s (`alive[n] /\ n \notin degraded`) already
  prevent a degraded node from taking ownership structurally, but nothing
  in `Replication.cfg`'s checked-invariant list separately names that
  guarantee. P's version makes it an explicit, independently-checked
  safety property, checked by recomputing degraded status from the
  announced `eDegradedChanged` stream rather than reading `Node.p`'s own
  field (§4.2).
- `NoIdentityWhileWaiting` (`Spec.p`), the newest, is likewise unnamed on
  the TLA+ side — not because TLA+ checks it under a different name, but
  because the boot-time case it covers (§7's genuinely-new-node path) maps
  to no TLA+ action at all (§3.2, §4.2). It asserts no node announces
  `eClaimAdvertise`/`eTakeoverDrain` while in `Node.p`'s `Waiting` state,
  checked the same recomputed-ground-truth way as the two properties
  above (from `eWaitingChanged`, not `Node.p`'s own `waitingForFence`).
- `GapFreedom` (`Spec.p`), the newest, **is** a named analog of
  `DuckSpoutCore.tla`'s `GapFreedom` invariant — same relationship
  `FencedZombie` above already has with its own TLA+ namesake: matching
  name, matching intent (`AppliedThru`'s contiguous-prefix property,
  enforced by `PeerApply`'s `g.rec.seq = AppliedThru(...) + 1` gap-refusal
  guard), not a transliteration of TLA+'s set-based `S \cup D =
  1..Cardinality(S \cup D)` formula. Checked the same
  recomputed-ground-truth way as `FencedZombie`/`NoOwnershipWhileDegraded`/
  `NoIdentityWhileWaiting` above: from the announced `eGapDecision` stream
  (`Node.p`'s own `eForward` handler, one per sender per evaluated
  `originSeq`), not from `Node.p`'s internal `appliedThru` map directly.
  Its scope is narrower than TLA+'s in the same two ways `FencedZombie`'s
  already is: no partition dimension (this model has none at all, so it
  is per logical origin only) and message-level only — the `SchemaKnown`
  and hard-rung-refusal guards `PeerApply` also conjoins gap-refusal with
  remain unmodeled (§3.2, this section's `eForward`/`PeerApply` row
  above).

The other nine (`DurableAck`, `WatermarkHonesty`, `CacheTransparency`,
`SingleDrainCommit`, `LossLedgerTruthful`, `SnapshotCovered`,
`LatestViewCorrect`, `LadderMonotone`, `EveryRequestResolves`) have no P
counterpart of any kind today. This is consistent with the P model's own
commit history describing it as an explicitly scoped-down slice, and this
map should not be read as implying broader property coverage than these
six checks — two of which (`FencedZombie`, per §4.2/§4.3; `GapFreedom`,
per the bullet above and §4.5) are themselves narrower than their TLA+
namesakes, and two of which (`NoOwnershipWhileDegraded`,
`NoIdentityWhileWaiting`) check boot-time behavior TLA+ represents only
partially or not at all, per §4.2.

### 4.5 GapFreedom, now modeled: closing §3.1's gap-refusal representation gap

*New, added alongside `TestGapFreedom` (#132), with its teeth-proof
evidence corrected here as part of a subsequent ACPR on the PR that added
it (#192, finding 4): the originally-reported "100% of schedules" catch
came from a broken variant that removed BOTH the idempotent-duplicate
short-circuit AND the `originSeq == thru + 1` gap gate at once, so most of
that catch rate was `GapFreedom`'s own recomputation failing on the
duplicate case (`2 != 2 + 1`) before any real gap was ever exercised, not
gap-refusal specifically. This entry gives the real, isolated number
instead, and records it here rather than only in a PR body, matching this
document's own precedent for the other properties added this way (§4.2).*

§3.1's `eForward`/`PeerApply` row used to name gap-refusal (`g.rec.seq =
AppliedThru(...) + 1`) as the one `PeerApply` guard with **no P
counterpart** while fencing already had one. `Node.p`'s `eForward` handler
now tracks a per-sender `appliedThru` watermark and refuses (no apply, no
claim, no receipt, no takeover) any `originSeq` that would leave a gap —
see that row's corrected left column for the handler's full current
branch structure. `GapFreedom` (`Spec.p`) checks this directly, the same
non-tautological, recomputed-ground-truth way `FencedZombie` already
established (§4.2): it rebuilds the contiguous-watermark fact purely from
the announced `eGapDecision` stream, not from `Node.p`'s own internal
`appliedThru` map, so a `Node.p` that stopped enforcing gap-refusal but
kept announcing `eGapDecision` honestly would still be caught.

`TestGapFreedom` (`TestDriver.p`) races two `Node` instances, `sender1`
and `sender2`, sharing one explicit logical origin (`nodeId = 1`, via
`eFenceBoot` on both — encoded directly rather than left resting on P's
shared zero-default, per #192's ACPR finding 8), plus a directly-injected
retransmit of `sender2`'s write, to force genuinely out-of-order delivery
at `receiver` in some explored schedules — see that scenario's own header
comment for the full three-way schedule breakdown. `just p-check
Replication` plus a direct `p check -tc GapFreedom` run both show 0 bugs
across 5000 explored schedules on the honest model. A second monitor,
`GapFreedomCoverage` (`Spec.p`, a `hot state` liveness twin added
alongside this correction, #192 finding 4), also shows 0 bugs across the
same runs — closing the one blind spot a pure safety assert cannot rule
out on its own: `GapFreedom`'s assert only ever fires `if (gd.accepted)`,
so a `Node.p` that refused every `eForward` unconditionally would pass it
vacuously; `GapFreedomCoverage` demands the scenario actually produce at
least one genuine accepted `eGapDecision` before it terminates.

A scratch, uncommitted variant that breaks gap-refusal **alone** —
widening the applied-next gate from `originSeq == thru + 1` to
`originSeq >= thru + 1`, leaving the idempotent-duplicate branch
untouched — is caught in **821 of 1000** explored schedules (82.1%), run
with `p check`'s `--explore` flag to force full exploration past the
first bug found rather than stopping there. This is the real, isolated
catch rate for gap-refusal specifically: materially lower than the
original combined-variant's reported 100%, and lower than 100% for a
legitimate reason `TestGapFreedom`'s own header comment already
documents — the safety check only has something to catch on schedules
where the race actually produces genuine out-of-order delivery at
`receiver`, which is not every explored schedule (one of the scenario's
three enumerated orderings never lets a gap occur at all).

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
  citations on the TLA+ side; P has no `WF_vars`/`SF_vars`-equivalent
  fairness-assumption syntax at all (its checker instead explores random/
  prioritized schedules directly, and `NoAckedLossLive`'s `hot state`
  liveness monitor — §4's scaffolding — checks termination-in-a-hot-state
  per schedule, not a fairness-conditioned temporal property), so P
  currently makes no fairness *claims* of its own to reconcile against
  TLA+'s — a difference in mechanism, not a gap in what gets checked.

## 6. #135's log-conformance investigation: Class 4 fired

*New section, issue #135 (ADR-0012 step 5's P half). Provenance matches
§1's own convention: a close reading, this time of P's actual public
tooling and manual rather than of the two models, done before writing any
code — per the issue's own instruction to research the mechanism properly
before committing to an implementation approach.*

ADR-0012 step 5 reads: "Traces close the loop against BOTH models: TLC
trace refinement (§8.2) and P log-conformance (PObserve-style) consume the
same trace vocabulary — the P model is thereby a *gated* artifact, not
folklore." Issue #135 asked for exactly that: take one of the §3.7 NDJSON
traces and check it against `Replication.p`, preferring P's native
PObserve support "if it exists and works cleanly," else a documented
translation/replay harness. What follows is why neither route is
practical *today*, with the evidence, not an impression.

### 6.1 PObserve does not exist as usable software

`p-org/PObserve` is a real GitHub repository — but querying it directly
(`https://api.github.com/repos/p-org/PObserve`) shows `created_at` and
`pushed_at` are the same instant, 2025-05-21T22:20:1{3,4}Z: one commit,
ever. `size` is 5 (KB) — a `LICENSE` (Apache-2.0) and a `README.md` whose
entire content is the single line "PObserve: Monitoring P Specifications
on Traces." No source, no examples, no releases, no wiki content despite
`has_wiki: true`, 6 stargazers, 0 forks, last touched over a year before
this investigation (`updated_at: 2026-04-03` reflects GitHub's own
metadata refresh, not a content change). This is a reserved name for
intended future work, not a tool — there is nothing to install, wire, or
even read past the title.

### 6.2 P's checker has no native external-trace-replay mode

Independent of PObserve's absence, P's own manual
(`p-org.github.io/P/manual/monitors/`) documents the `spec`/monitor
mechanism this repo's `p/Replication/Spec.p` already uses: "each time
there is a `send` or `announce` of an event during the execution of a
system, all the monitors... that are observing that event are executed
synchronously." A monitor cannot `send`, `receive`, `new`, or `announce`
anything itself — it only reacts to what the *checker's own schedule
exploration* produces from an author-written `test` scenario (this repo's
`TestDriver.p` machines, all hand-authored literal `send` sequences). The
manual documents no mechanism — and none was found in the wider P/Coyote
documentation or discussion threads — for handing the checker a fixed,
externally-recorded sequence of events to validate instead of exploring
one it generates itself. This is the structural reason `tla.mjs tv`
(`scripts/tla.mjs`) has no P-side analog to imitate: TLC's trace validation
works by walking a *declarative* state relation (`*Trace.tla` constrains
`Next` to the recorded step sequence and checks each step is enabled) —
no interpreter runs, nothing is instantiated. P's model is *operational*:
`Node` machines are real actors passing real messages, and the checker's
entire value proposition is exploring interleavings of a script, not
replaying one fixed history against a relation.

### 6.3 The vocabularies do not line up, concretely

Even setting aside 6.1–6.2, the trace vocabulary and the P model's own
event vocabulary are different by design, not by oversight:

- The §3.7 trace vocabulary journals a bare `PeerApply` per apply — e.g.
  `specs/fixtures/replication-conforming.ndjson` (branch
  `feat/specs-replication-trace-sibling`, PR #195, not yet merged):
  `{"node":"n2","seq":0,"event":"PeerApply"}`. `Node.p`'s `eForward`
  handler — the closest analog — never announces anything named
  `PeerApply`. It deliberately splits that single trace-vocabulary action
  into two separate scaffolding announcements, `eFenceDecision` and
  `eGapDecision` (§3.1's own `eForward` row), built specifically so
  `FencedZombie`/`GapFreedom` can "recompute... an independent ground
  truth" rather than "read `Node.p`'s own internal... map directly"
  (`Spec.p`'s own header comments, quoted in §3.1) — i.e. these events are
  test-instrumentation for the spec's own non-tautological-checking
  discipline, not a mirror of the implementation's journaled action names.
- `Receipt` is a journaled outcome name in the trace vocabulary, but in
  `Node.p` it is a plain point-to-point `send` (`send fwd.origin, eReceipt,
  ...`), never `announce`d — §3.1's own table already notes "no spec reads
  `holders` directly, or observes `eReceipt` itself." Per §6.2, a monitor
  can only ever see an `announce`d event, so `Receipt` as a checkable
  log-conformance point does not exist on the P side today, full stop —
  not narrower, absent.
- §3.2 and §4.4 above already establish that most of `docs/trace-mapping.md`'s
  27-variant vocabulary — `DedupCheck`, `Throttle`, `Refuse`, `SealPart`,
  `PutPart`, all three `LakeCommit*` outcomes, `Reconcile`, `Expire`,
  `Demote`, `Evict`, `DropWindow`, `SnapshotSeal`, `EvolveSchema`,
  `DeclareLoss` — has zero representation anywhere in `p/Replication/*.p`.
  `Ingest.tla`/`Drain.tla`/`Schema.tla` have no P counterpart at all (§1).
  A P-side check could, at absolute best, ever evaluate the
  Forward/Receipt/FenceBoot/Heartbeat/ClaimAdvertise/TakeoverDrain/
  DegradedBoot slice §3.1 already documents as fused, narrowed, or
  partially represented — never "the same trace vocabulary" ADR-0012 step
  5 describes both validators consuming.

### 6.4 No live-captured Replication trace exists to feed it, either

`specs/fixtures/replication-manifest.toml`'s own header (same branch, PR
#195) says plainly: every Replication fixture is "HAND-AUTHORED, not
captured from a real `duckspout-replication` run — no such implementation
exists yet," and calls the fixture set itself "useful only as a self-test
of the refinement checker, not the genuine conformance value a trace
sibling exists to provide." Unlike `Ingest` (`duckspout-daemon`'s
`tests/trace_capture.rs`, exercised live by `trace-conformance.mjs`'s tier
2), Replication has no live-capture path yet even on the TLA+ side. The
traces a P-side check would need are, today, hand-authored placeholders on
the sibling this issue was told to build on — one more reason to not build
a second consumer of them yet.

### 6.5 What a from-scratch harness would actually require

Building the translation/replay route ourselves (option (b) the issue
named) is not impossible, but it is disproportionate to what exists today:

1. New `announce` instrumentation in the checker-validated `Node.p` model
   for events that are today deliberately internal or point-to-point only
   (a unified `PeerApply` name; `Receipt`) — a change to an *already
   checked* model, needing re-verification that it does not perturb the
   six existing specs' semantics (§4.4).
2. A translation/codegen layer mapping the NDJSON's untyped node-id
   strings and (at least in the hand-authored fixture above) argument-free
   event records onto `p/Replication`'s minimal int-keyed, 2-node scenario
   shape — a mechanism with no existing convention to extend (`tla.mjs
   tv`'s declarative walk does not generalize to P's operational model,
   §6.2), effectively a second, independent trace-consumer built from
   nothing.
3. Even after (1) and (2), the result would only ever validate the narrow
   slice named in §6.3 — not "the same trace vocabulary" both validators
   are supposed to consume per ADR-0012's own framing. Shipping that as
   "P log-conformance" would be *less* honest than not shipping it: it
   would look, from the ledger, like step 5's parity promise is met when
   most of any real trace's lines are silently outside what the P side
   could ever reject.

### 6.6 Disposition

This is ADR-0012 §5's own **Class 4** ("mechanical cross-check
failure... if step 5's P-side conformance proves impractical") by its own
definition, and the ADR's "Revisit when" section names the exact condition
met here: "Step 5's P-side conformance proves impractical (no reliable
log-conformance path) — the pipeline's honesty depends on it; without it,
fall back to ADR-0011's evidence-triggered posture." §5 itself is explicit
that Class 4 "is a methodology-level decision (owner ruling), not
something a single PR resolves" — so this section records the finding and
its evidence rather than forcing an implementation whose real coverage
would misrepresent what step 5 promises, and rather than amending
ADR-0012 (protected set, `docs/adr/`) unilaterally.

**Recommendation to the owner**: invoke ADR-0012's revisit clause for step
5's P half specifically (TLC trace refinement, §8.2, is unaffected — it
has no equivalent architectural gap). `docs/arming-ledger.toml`'s
`p-replication` row (`status = "staged"`, issue #131) governs `just
p-check` — the model-*checking* gate — not log-conformance; no
log-conformance ledger row was ever created, so nothing needs un-arming.
If the owner instead wants the harness attempted despite §6.5's cost, that
list is the starting scope for the follow-up issue this section would
otherwise leave unfiled.
