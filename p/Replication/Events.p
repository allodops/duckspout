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

// Owner -> Replica: forward a staged record for replication.
event eForward: (key: int, sq: int, origin: Node);
// Replica -> Owner: receipt once the forwarded record is durably applied.
event eReceipt: (key: int, sq: int, holder: Node);

// Environment: the owner dies. Broken (never sent) is NOT how a real
// crash is observed -- CrashSignal is delivered to the surviving replica
// directly, standing in for heartbeat-TTL expiry (the mechanism is out
// of this slice's scope; the fact that the replica eventually learns the
// owner is dead is what this model needs).
event eCrashSignal: (dead: Node);

// Sent to the node that is itself dying: it halts immediately (real
// crash semantics) rather than just being talked about by others.
event eDie;

// A node reports it has durably staged a key (accepted for replication) --
// what NoAckedLoss actually tracks: not the client's raw request (which
// may never even be accepted if its target node dies first), but the
// point past which the system has made a durability commitment.
event eAccepted: (key: int, sq: int);

// A replica claims an orphaned key and drains (commits) it.
event eTakeoverDrain: (key: int, sq: int, newOwner: Node);

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
