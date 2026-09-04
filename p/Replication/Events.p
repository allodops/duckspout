// Payloads and events for the replication takeover-drain scenario.

// `sq` is the client's own opaque request/ack correlator (matched back on
// `eWriteAck`) -- reused freely across scenarios (see `TestNewNodeBoot`'s
// third write, `sq = 1` again for a brand-new origin) with no ordering
// guarantee of its own. `originSeq` (below, `eForward`) is the *distinct*
// §5.4/GapFreedom sequence number this scenario adds -- checked, not just
// carried; see `eForward`'s own comment for why `sq` cannot faithfully
// double as it. Named `originSeq`, not `seq`, purely because `seq` is a P
// reserved word (the built-in `seq[T]` collection type).
type tWriteReq = (client: Client, key: int, sq: int, originSeq: int);
type tWriteAck = (key: int, sq: int);

// §5.5's two claim roles.
enum tRole {
  OWNER,
  REPLICA
}

event eWriteReq: tWriteReq;
event eWriteAck: tWriteAck;

// Owner -> Replica: forward a staged record for replication. `originId` is
// the sender's *logical* node identity (stable across a reboot -- see
// eFenceBoot below), distinct from `origin`, the sender's current machine
// reference (needed to route the eReceipt reply). `inc` is the sender's
// incarnation at send time (§5.7: "every message ... carries (node_id,
// incarnation)").
// `originSeq` (added for GapFreedom, #132's third named safety property):
// the §5.4 "dense per-(origin, partition) sequence assigned at
// StageCommit" (docs/design/replication.md §4) -- this model has no
// partition dimension, so it is dense per logical *origin* (`originId`)
// only. Named `originSeq`, not TLA+'s own `seq`, purely because `seq` is
// a P reserved word (the built-in `seq[T]` collection type) -- no
// semantic difference intended. Deliberately NOT `sq`: the two are
// different concepts even where their values happen to coincide -- `sq`
// is a *client's* own request/ack correlator, opaque to everything except
// `eWriteAck`'s reply matching, while `originSeq` is an *origin's*
// durably-persisted per-(origin, partition) sequence, checked for gap
// contiguity by GapFreedom and load-bearing to `Node.p`'s apply logic.
// Reusing one field for both would conflate a client-facing identifier
// with a replication-protocol invariant the client has no part in --
// itself a KISS/DRY violation in the other direction, not an
// optimization. (`sq` is, as it happens, asserted on nowhere in this
// model -- only ever printed -- so reusing the field would not have
// crashed anything; that is not why a distinct field exists.) The
// existing scenarios' own `sq` values confirm the concepts were never
// interchangeable in the first place: `TestNewNodeBoot`'s third write
// (`replica`'s own first-ever forward) carries `sq = 1` right after a
// second write carried `sq = 2` -- neither dense nor monotonic per
// origin, because `sq` was never meant to be. `originSeq` is assigned
// explicitly at every call site instead (see `TestDriver.p`), standing in
// for the origin's own durably-persisted `nextSeq[n][p]` counter (§4) --
// the same "environment supplies persisted state a halted P machine
// cannot recover on its own" convention `eFenceBoot`'s `priorIncarnation`
// already establishes for incarnation.
event eForward: (key: int, sq: int, originSeq: int, origin: Node, originId: int, inc: int);
// Replica -> Owner: receipt once the forwarded record is durably applied.
// `holderId`/`inc` mirror `eForward`'s `originId`/`inc` -- same rationale,
// other direction.
event eReceipt: (key: int, sq: int, holder: Node, holderId: int, inc: int);

// Environment: the owner dies. Broken (never sent) is NOT how a real
// crash is observed -- CrashSignal is delivered to the surviving replica
// directly, standing in for heartbeat-TTL expiry. Used by
// `TestTakeoverDrain` and `TestFenceBootZombie`, which only need the
// fact that the replica eventually learns the owner is dead, not the
// detection mechanism itself. `TestHeartbeatDetection` (`TestDriver.p`)
// now models that mechanism directly via `eHeartbeat`/`eTick` below
// instead of this oracle -- a deliberate per-scenario choice of
// abstraction level, not a scope gap left unmodeled.
event eCrashSignal: (dead: Node);

// §5.5/§6.1 (docs/design/replication.md): a live node's periodic
// heartbeat to its peer, carrying a logical `round` counter standing in
// for wall-clock ticks -- R-determinism bars real time (`Instant::now`,
// `SystemTime::now`) from protocol crates, and the same principle
// applies here: model logical rounds, not timestamps. `Node.p`'s `eTick`
// handler sends this to its peer every round it is alive to process one,
// which is what makes "heartbeats simply stop arriving" after a real
// crash (`eDie`) fall out of P's own halted-machine-drops-its-queue
// semantics, not a fact this event's handler has to special-case.
// `TestHeartbeatDetection` (`TestDriver.p`) also injects one heartbeat
// directly from the environment (the same "stand in for the peer's own
// send" convention `TestTakeoverDrain`/`TestFenceBootZombie` already use
// for retransmitted/zombie Forwards) rather than ticking the sender, to
// keep `eTick`'s marker-announcing side effect (below) scoped to only
// the node whose own detection cycle that scenario is testing.
event eHeartbeat: (from: Node, round: int);

// The environment's periodic driver signal, standing in for each node's
// own wall-clock timer firing (§5.5/§6.1's Heartbeat cadence and
// TTL-lapse detection). On every tick a node (a) re-heartbeats its peer
// under the current round, and (b) checks whether its OWN peer's
// heartbeat gap (`round - lastHeartbeatRound`) has reached
// `heartbeatTTL` -- deriving peer-death detection from the gap itself,
// rather than being told about it by `eCrashSignal`'s oracle (see that
// event's comment above). `TestHeartbeatDetection` (`TestDriver.p`) is
// the only scenario that sends this; `TestTakeoverDrain` and
// `TestFenceBootZombie` never send `eTick` at all and keep using
// `eCrashSignal`, unaffected by this event's addition.
event eTick: (round: int);

// Sent to the node that is itself dying: it halts immediately (real
// crash semantics) rather than just being talked about by others.
event eDie;

// §5.7 (Incarnation fencing): "every process boot executes FenceBoot: the
// node draws a fresh incarnation ... and persists it locally." `nodeId` is
// the persistent logical identity (stable across a reboot); `incarnation`
// becomes `priorIncarnation + 1` -- strictly higher than whatever this
// logical node last had, never true global uniqueness (this scenario does
// not need that, only per-node monotonicity across reboots, per the task
// this model was built against).
//
// A real boot reads its own prior incarnation from local persisted state;
// a P machine that has `raise halt`ed cannot come back and read anything
// -- P has no notion of a process identity surviving a restart. A "reboot"
// is therefore modeled as a brand-new `Node` machine instance, and the
// environment (the test driver, standing in for the persisted-local-state
// the real node would read for itself) tells it both who it is (`nodeId`,
// matching the crashed instance's own) and what incarnation it is
// superseding (`priorIncarnation`). This is a deliberate, documented
// modeling choice for a gap P has no native concept for -- see the PR
// description for the alternatives considered.
event eFenceBoot: (nodeId: int, priorIncarnation: int);

// §7 (docs/design/replication.md, DegradedBoot): a boot-time catalog-DB
// outage/restore. Sent directly to the node by the environment -- the
// same "stand in for a real signal" convention this file already uses
// for eCrashSignal/eHeartbeat -- rather than modeling an actual catalog
// service. `catalogOutage` defaults to P's zero-value `false` (catalog
// reachable), so no scenario that never sends either event is affected:
// `TestTakeoverDrain`, `TestFenceBootZombie`, and `TestHeartbeatDetection`
// all boot with the catalog implicitly up, matching their behavior before
// this pair of events existed.
event eCatalogOutage;
event eCatalogRestored;

// `specs/DuckSpoutCore.tla`'s `CrashWipe(n)`, the *other* fault -- "the
// disk dies too": `staged`/`cache`/`dedup` are all cleared, not merely
// `inflight` (`CrashNode`'s own, milder effect -- see `eDie` below).
// Sent to the node that dies via wipe, alongside the existing `eDie` --
// deliberately a SEPARATE event, not a reused `eDie`, even though
// `Node.p`'s handler for it is behaviorally identical (`raise halt`):
// see that handler's own comment for why bothering to clear fields would
// be dead code, and why the real distinguishing behavior lives entirely
// in what identity the environment gives the eventual replacement, not
// in the dying instance's own handler.
//
// IMPORTANT correspondence caveat (see docs/design/p-tla-correspondence.md
// for the full write-up): TLA+'s `CrashWipe(n)` guard (`wiped' = wiped
// \cup {n}`) is checked by both `FenceBoot(n)` and `DegradedBoot(n)`
// (`n \notin wiped`) forever after -- the comment on `CrashWipe` in
// `specs/DuckSpoutCore.tla` says plainly "a wiped node never re-enters."
// TLA+'s `Nodes` is a fixed set with no dynamic-membership concept, so a
// wiped node's identity is retired *permanently*, not reissued to a
// "replacement." What follows this event in `TestNewNodeBoot`
// (`TestDriver.p`) -- a brand-new `Node` instance with a *different*
// `nodeId`, never the wiped one -- is therefore NOT a P model of TLA+'s
// `CrashWipe` recovering; there is no such thing to model, because TLA+
// forbids it by construction. It is a P model of `docs/design/
// replication.md` §7's own separate prose ("a genuinely new node -- no
// persisted incarnation -- waits, in a typed startup state"), which has
// **no TLA+ action at all** to correspond to: TLA+'s `Init` already
// assumes every node's first-ever boot succeeded (`alive = [n \in Nodes
// |-> TRUE]`, `inc = [n \in Nodes |-> 0]`), so a fresh node hitting
// trouble on its very first boot is not a reachable `Next` transition in
// `DuckSpoutCore.tla` at all -- see `eFenceBoot`'s and the `Waiting`
// state's comments in `Node.p` for how this scenario models it instead.
event eCrashWipe;

// §7 (docs/design/replication.md): announced by `Node.p` whenever a
// node's "awaiting its very first fence" status changes -- true the
// moment a genuinely new node's (`priorIncarnation = 0`) first
// `eFenceBoot` attempt cannot complete because the catalog is down (it
// "has no identity to be safely partial with," per §7 -- unlike
// `DegradedBoot`'s persisted-incarnation case, there is nothing to fall
// back to), false once `eCatalogRestored` lets that first fence finally
// complete. Purpose-built scaffolding for `NoIdentityWhileWaiting`
// (Spec.p), the same independent-ground-truth convention
// `eDegradedChanged`/`eFenceDecision` already establish in this file --
// see `FencedZombie`'s header comment (Spec.p) for why the spec needs
// this rather than reading `Node.p`'s own `waitingForFence` field
// directly. `node` identifies which `Node` this transition belongs to.
event eWaitingChanged: (node: Node, waiting: bool);

// A node reports it has durably staged a key (accepted for replication) --
// what NoAckedLoss actually tracks: not the client's raw request (which
// may never even be accepted if its target node dies first), but the
// point past which the system has made a durability commitment.
event eAccepted: (key: int, sq: int);

// A replica claims an orphaned key and drains (commits) it.
event eTakeoverDrain: (key: int, sq: int, newOwner: Node);

// §5.7: announced by `Node.p`'s `eForward`/`eReceipt` handlers every time
// they evaluate the incarnation fence against a message, whether the
// message is accepted or rejected. This is scaffolding purpose-built for
// `FencedZombie` (`Spec.p`) exactly the way `eForwardHandled`/
// `eCrashSignalHandled` are scaffolding for `NoAckedLoss`: `eForward` and
// `eReceipt` themselves are point-to-point `send`s, not `announce`s, so a
// `spec` machine (which can only `observe` announced events) has no other
// way to see them. `senderId` is the sender's logical node identity (see
// `eFenceBoot`), not its current machine reference -- the whole point is
// to recognize the *same logical sender* across a reboot, which a machine
// reference cannot do (a rebooted node is a different machine instance).
event eFenceDecision: (receiver: Node, senderId: int, inc: int, accepted: bool);

// §5.4 PeerApply's gap-refusal guard (docs/design/replication.md §4;
// `specs/DuckSpoutCore.tla`'s `GapFreedom`/`g.rec.seq = AppliedThru(...) +
// 1`): announced by `Node.p`'s `eForward` handler every time it evaluates
// a NEW (not already-applied-and-idempotently-acked) `originSeq` from a
// sender that already passed the incarnation fence -- `accepted = true`
// means this `originSeq` was exactly the receiver's next expected one for
// that sender and was applied, advancing its watermark; `accepted =
// false` means it was strictly ahead of that watermark (a gap) and was
// refused outright. Purpose-built scaffolding for `GapFreedom` (Spec.p),
// the same independent-ground-truth convention `eFenceDecision`/
// `eDegradedChanged`/`eWaitingChanged` already establish in this file --
// see `FencedZombie`'s header comment (Spec.p) for why the spec needs
// this rather than reading `Node.p`'s own internal `appliedThru` map
// directly. Deliberately NOT announced for the idempotent-duplicate case
// (`originSeq` at or below the watermark, acknowledged without
// re-applying, §4) -- that case neither advances nor gaps the watermark,
// so it carries no information this spec's contiguity check needs;
// `GapFreedom`'s independent recomputation would be a no-op either way
// for it. `senderId` is the sender's logical node identity (see
// `eFenceBoot`), matching `eFenceDecision`'s own convention, for the same
// reason: recognizing the same logical sender across a reboot, which a
// machine reference cannot do.
event eGapDecision: (receiver: Node, senderId: int, originSeq: int, accepted: bool);

// §5.5: "published as a side effect of PeerApply -- the first apply for a
// partition the node has no claim row for triggers the insert." Announced
// once per (key, node) -- a second apply for the same key must NOT
// re-advertise (ClaimAdvertiseOnce, Spec.p).
event eClaimAdvertise: (key: int, node: Node, role: tRole);

// §7 DegradedBoot: announced by `Node.p` whenever its own `degraded` flag
// changes -- true at `eFenceBoot` time when a persisted incarnation
// (`priorIncarnation > 0`) boots into a catalog outage, false at
// promotion once the catalog returns. Purpose-built scaffolding for
// `NoOwnershipWhileDegraded` (Spec.p), the same way `eFenceDecision` is
// scaffolding for `FencedZombie`: the spec needs an independently
// recomputed ground truth of "is this node degraded right now" built from
// the announced record, not from reading `Node.p`'s own `degraded` field
// directly -- see `FencedZombie`'s header comment (Spec.p) for why that
// would just check the implementation against itself. `node` identifies
// which `Node` this transition belongs to.
event eDegradedChanged: (node: Node, degraded: bool);

// Announced once a node finishes handling a Forward -- the last
// state-changing event for that key in this bounded scenario (Init sends
// eDie and eCrashSignal unconditionally; nothing else can still act on the
// key afterward). The spec's safety assert fires here rather than via a
// `hot state`: this scenario terminates (one write, one crash, done), and
// P's default bugfinding checker does not treat "quiescent while hot" as a
// violation the way TLA+'s fairness-based `~>` checking does (#132
// finding) -- so eventual-drain must be checked as a direct assert at the
// scenario's own last-relevant-event, not as liveness.
event eForwardHandled: (key: int, sq: int);

// Announced once a node finishes evaluating a peer-death detection -- the
// scenario's other last-relevant-event marker (a Forward can arrive
// either before or after detection settles; whichever of the two happens
// *last* is the true last chance to drain, so the spec must observe
// both, not just one). Originally announced only from `eCrashSignal`'s
// oracle-path handler; `Node.p`'s `eTick` handler now announces this SAME
// event from the heartbeat-TTL-expiry path too (both call through a
// shared `sweepOrphanedKeys` helper) -- deliberately reused, not
// renamed or duplicated, so `NoAckedLoss` (`Spec.p`) needs zero changes
// to also cover `TestHeartbeatDetection`: the property it checks ("every
// accepted key is eventually drained") does not care *how* the peer's
// death was established, only that this marker fires once detection is
// settled one way or the other.
event eCrashSignalHandled;

// Wire a Node's peer after both nodes exist (spawn order is otherwise
// circular: owner needs replica's ref, replica needs owner's ref).
event eLink: Node;
