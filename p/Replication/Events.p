// Payloads and events for the replication takeover-drain scenario.

type tWriteReq = (client: Client, key: int, sq: int);
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
event eForward: (key: int, sq: int, origin: Node, originId: int, inc: int);
// Replica -> Owner: receipt once the forwarded record is durably applied.
// `holderId`/`inc` mirror `eForward`'s `originId`/`inc` -- same rationale,
// other direction.
event eReceipt: (key: int, sq: int, holder: Node, holderId: int, inc: int);

// Environment: the owner dies. Broken (never sent) is NOT how a real
// crash is observed -- CrashSignal is delivered to the surviving replica
// directly, standing in for heartbeat-TTL expiry (the mechanism is out
// of this slice's scope; the fact that the replica eventually learns the
// owner is dead is what this model needs).
event eCrashSignal: (dead: Node);

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

// §5.5: "published as a side effect of PeerApply -- the first apply for a
// partition the node has no claim row for triggers the insert." Announced
// once per (key, node) -- a second apply for the same key must NOT
// re-advertise (ClaimAdvertiseOnce, Spec.p).
event eClaimAdvertise: (key: int, node: Node, role: tRole);

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

// Announced once a node finishes handling eCrashSignal -- the scenario's
// other last-relevant-event marker (a Forward can arrive either before or
// after the crash signal; whichever of the two happens *last* is the true
// last chance to drain, so the spec must observe both, not just one).
event eCrashSignalHandled;

// Wire a Node's peer after both nodes exist (spawn order is otherwise
// circular: owner needs replica's ref, replica needs owner's ref).
event eLink: Node;
