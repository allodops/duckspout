// A replication node. In this minimal slice one Node instance plays the
// acceptor/owner role for the scenario's single request, the other plays
// the replica -- both are the SAME machine type (matching DuckSpoutCore.tla's
// symmetric treatment: any node can accept, any node can replicate).
machine Node {
  var peer: Node;
  // Keys this node has durably staged (owner: from the client; replica:
  // from a Forward), not yet known-committed.
  var staged: set[int];
  // Keys this node has already committed (normal path or takeover).
  var committed: set[int];
  // How many holders (self + receipts) each key currently has.
  var holders: map[int, int];
  var pendingClient: map[int, Client];
  // §5.5: keys this node has published a claim row for (side effect of the
  // first apply). Guards eClaimAdvertise from firing more than once per key.
  var claims: map[int, tRole];
  // Set once this node learns its peer is dead (eCrashSignal); once true,
  // any key that lands here afterward (a late Forward) is orphaned on
  // arrival, not just what was already staged at signal time.
  var peerDead: bool;
  // §5.7: this node's persistent logical identity and current incarnation.
  // Both stay at their P zero-default (0) until `eFenceBoot` runs --
  // `TestTakeoverDrain` never sends it at all, which is harmless there:
  // that scenario never reboots anyone and each node only ever hears from
  // its one peer, so a shared default of 0 on both sides creates no
  // cross-identity collision to worry about (§5.7 is simply unexercised
  // in that scenario, not violated by it).
  var nodeId: int;
  var incarnation: int;
  // §5.7 fencing table: highest incarnation seen so far from each sender,
  // keyed by the sender's *logical* `nodeId` -- not by machine reference,
  // because the whole point is to recognize a rebooted node's stale,
  // lower-incarnation message as coming from the *same* sender a
  // higher-incarnation message already arrived from, and a reboot is a
  // brand-new machine reference (see `eFenceBoot`'s header comment in
  // Events.p). Shared across `eForward` and `eReceipt` -- §5.7 fences
  // every message type from a sender through one incarnation counter, not
  // one counter per message kind.
  var highestSeen: map[int, int];

  start state Active {
    entry {
      peerDead = false;
    }

    on eLink do (p: Node) {
      peer = p;
    }

    // §5.7 FenceBoot: draw this node's (logical identity, incarnation).
    // `priorIncarnation` is supplied by the environment because a P
    // machine cannot read back state from an instance that `raise
    // halt`ed -- see Events.p's `eFenceBoot` comment for the full
    // rationale. Strictly higher than the caller-supplied prior value is
    // all this scenario needs (simple per-node monotonicity), not true
    // global uniqueness.
    on eFenceBoot do (fb: (nodeId: int, priorIncarnation: int)) {
      nodeId = fb.nodeId;
      incarnation = fb.priorIncarnation + 1;
    }

    // A real crash: stop entirely, right now. Any event already in this
    // machine's queue behind eDie is simply never processed (P drops a
    // halted machine's queue) -- matching a process that is actually gone,
    // not one that merely stops being polite.
    on eDie do {
      raise halt;
    }

    on eWriteReq do (req: tWriteReq) {
      staged += (req.key);
      holders[req.key] = 1;
      pendingClient[req.key] = req.client;
      if (!(req.key in claims)) {
        claims[req.key] = OWNER;
        announce eClaimAdvertise, (key = req.key, node = this, role = OWNER);
      }
      announce eAccepted, (key = req.key, sq = req.sq);
      send peer, eForward, (key = req.key, sq = req.sq, origin = this, originId = nodeId, inc = incarnation);
    }

    // §5.7 fencing gate: a Forward carrying an incarnation strictly below
    // the highest this node has already seen from that logical sender is
    // a zombie -- a partitioned former self (or a stale retransmit from
    // before a reboot) -- and is refused everywhere: no apply, no claim
    // advertisement, no receipt, no takeover check. Accepted and rejected
    // paths both still announce `eFenceDecision` (so `FencedZombie` can
    // observe the decision either way) and `eForwardHandled` (so
    // `NoAckedLoss`'s marker bookkeeping, unrelated to fencing, is
    // unaffected by whether this particular Forward was fenced).
    on eForward do (fwd: (key: int, sq: int, origin: Node, originId: int, inc: int)) {
      var seen: int;
      var accept: bool;
      seen = 0;
      if (fwd.originId in highestSeen) {
        seen = highestSeen[fwd.originId];
      }
      accept = fwd.inc >= seen;
      announce eFenceDecision, (receiver = this, senderId = fwd.originId, inc = fwd.inc, accepted = accept);
      if (accept) {
        highestSeen[fwd.originId] = fwd.inc;
        staged += (fwd.key);
        if (!(fwd.key in claims)) {
          claims[fwd.key] = REPLICA;
          announce eClaimAdvertise, (key = fwd.key, node = this, role = REPLICA);
        }
        send fwd.origin, eReceipt, (key = fwd.key, sq = fwd.sq, holder = this, holderId = nodeId, inc = incarnation);
        if (peerDead && !(fwd.key in committed)) {
          committed += (fwd.key);
          announce eTakeoverDrain, (key = fwd.key, sq = fwd.sq, newOwner = this);
        }
      }
      announce eForwardHandled, (key = fwd.key, sq = fwd.sq);
    }

    // Same §5.7 gate, other direction: a Receipt carrying a stale
    // incarnation from its holder is fenced out too -- no holder-count
    // bump, no client ack triggered off it.
    on eReceipt do (rc: (key: int, sq: int, holder: Node, holderId: int, inc: int)) {
      var seen: int;
      var accept: bool;
      seen = 0;
      if (rc.holderId in highestSeen) {
        seen = highestSeen[rc.holderId];
      }
      accept = rc.inc >= seen;
      announce eFenceDecision, (receiver = this, senderId = rc.holderId, inc = rc.inc, accepted = accept);
      if (accept) {
        highestSeen[rc.holderId] = rc.inc;
        holders[rc.key] = holders[rc.key] + 1;
        if (holders[rc.key] >= 2 && rc.key in pendingClient) {
          send pendingClient[rc.key], eWriteAck, (key = rc.key, sq = rc.sq);
        }
      }
    }

    // The environment tells a live node its peer has died. Any key this
    // node already has staged (received via Forward or its own accept)
    // but not yet committed is orphaned right now -- take over and drain
    // it; peerDead also covers anything that arrives from here on.
    on eCrashSignal do (sig: (dead: Node)) {
      var k: int;
      peerDead = true;
      foreach (k in staged) {
        if (!(k in committed)) {
          committed += (k);
          announce eTakeoverDrain, (key = k, sq = 0, newOwner = this);
        }
      }
      announce eCrashSignalHandled;
    }
  }
}
