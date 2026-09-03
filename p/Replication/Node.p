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
  // §5.5/§6.1: the last round this node received an `eHeartbeat` from its
  // peer (0 default -- indistinguishable from "never received one," which
  // is exactly the state a peer that never heartbeats should be in).
  // `heartbeatTTL` is a small fixed constant (kept small deliberately to
  // keep this bounded scenario's state space small, matching this file's
  // existing incarnation-fencing convention of "simple... not true global
  // uniqueness" scoped to what the scenario needs) rather than a
  // configurable value threaded in from the environment.
  var lastHeartbeatRound: int;
  var heartbeatTTL: int;
  // §7 DegradedBoot (docs/design/replication.md): whether the catalog DB
  // is currently reachable, per the environment's `eCatalogOutage`/
  // `eCatalogRestored` signals. Defaults to P's zero-value `false` --
  // i.e. reachable -- deliberately: a scenario that never sends either
  // event (TestTakeoverDrain, TestFenceBootZombie, TestHeartbeatDetection)
  // must see no change in behavior from this field's mere existence.
  var catalogOutage: bool;
  // Set true at `eFenceBoot` time when a persisted incarnation boots into
  // a catalog outage (see that handler below); set false at promotion
  // once the catalog returns. While true, this node applies and receipts
  // replication under its incarnation as normal but takes no ownership
  // action (no takeover-drain) -- `sweepOrphanedKeys` below is the single
  // enforcement point.
  var degraded: bool;

  start state Active {
    entry {
      peerDead = false;
      heartbeatTTL = 3;
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
      // §7 DegradedBoot: a node with a *persisted* incarnation
      // (`priorIncarnation > 0`) booting into a catalog outage comes up
      // replica-only degraded -- it keeps applying and receipting under
      // its new incarnation (below), but takes no ownership action until
      // promotion. A genuinely new node (`priorIncarnation = 0`) never
      // goes degraded here: §7 is explicit it "has no identity to be
      // safely partial with" and instead waits in a typed startup state --
      // a different boot path this model does not represent (see
      // docs/design/p-tla-correspondence.md's DegradedBoot section for
      // the precise boundary).
      if (fb.priorIncarnation > 0 && catalogOutage) {
        degraded = true;
        announce eDegradedChanged, (node = this, degraded = true);
      }
    }

    on eCatalogOutage do {
      catalogOutage = true;
    }

    // §7: "promotes itself when the catalog returns and FenceBoot
    // completes." This model represents "FenceBoot completes" as simply
    // exiting degraded mode the moment the catalog signal arrives -- the
    // node never lost its incarnation while degraded, so there is no
    // further boot ceremony left for it to redo. Exiting degraded also
    // re-checks staged-but-uncommitted keys for orphans right now, not
    // merely from here on: any takeover suppressed while degraded becomes
    // eligible at the moment of promotion, not only for future triggers.
    on eCatalogRestored do {
      catalogOutage = false;
      if (degraded) {
        degraded = false;
        announce eDegradedChanged, (node = this, degraded = false);
        sweepOrphanedKeys();
        // `eCrashSignal`/`eTick` withhold `eCrashSignalHandled` while
        // degraded (see both below) precisely because promotion can still
        // drain a key they could not -- so if a death was already known
        // (`peerDead`) when the catalog returned, promotion is the true
        // last chance, and this is where that marker belongs instead.
        if (peerDead) {
          announce eCrashSignalHandled;
        }
      }
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
        // This inline immediate-takeover check (a late Forward arriving
        // after the peer is already known dead) does not route through
        // `sweepOrphanedKeys` -- it needs its own §7 DegradedBoot guard
        // for the same reason: no ownership action while `degraded`.
        if (peerDead && !degraded && !(fwd.key in committed)) {
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
      peerDead = true;
      sweepOrphanedKeys();
      // §7 DegradedBoot: withhold this "last chance to drain" marker
      // while degraded -- promotion (`eCatalogRestored` above) is the
      // genuine last chance in that case, and announces the marker itself
      // once it re-sweeps. Announcing it here regardless would tell
      // `NoAckedLoss` no further trigger can drain this key, which is
      // false while degraded.
      if (!degraded) {
        announce eCrashSignalHandled;
      }
    }

    // §5.5/§6.1: a live peer's own heartbeat, renewing our record of when
    // we last heard from it. Independent of our own `eTick` cycle below --
    // a heartbeat can arrive at any time, not just when we happen to be
    // ticking ourselves.
    on eHeartbeat do (hb: (from: Node, round: int)) {
      lastHeartbeatRound = hb.round;
    }

    // Our own periodic housekeeping tick (standing in for a wall-clock
    // timer -- see `eTick`'s comment in Events.p). Every tick we (a)
    // re-heartbeat our peer under the current round -- a genuinely dead
    // peer (halted via eDie) simply never processes this event at all,
    // which is what makes its heartbeats stop without any dead-check
    // needed here, and is harmless to still send if our own peer already
    // died (P drops a halted machine's queue) -- and (b) check whether
    // OUR peer's heartbeat gap has reached `heartbeatTTL`. Crossing that
    // gap is this node's own, internally-derived death detection: the
    // same `sweepOrphanedKeys` call `eCrashSignal`'s oracle path makes
    // above, just reached by a different trigger; `!peerDead` guards it
    // from re-sweeping on a later tick once detection has already
    // happened once.
    //
    // `eCrashSignalHandled` is announced UNCONDITIONALLY at the end of
    // every tick, deliberately mirroring `eCrashSignal`'s own handler
    // above (which announces it regardless of whether `staged` had any
    // orphaned keys to sweep): this is the scenario's environment-paced
    // "no further legitimate detection chance remains as of this round"
    // marker, not a report of whether detection actually fired this
    // round. Gating the announce on the `if` below instead would silence
    // `NoAckedLoss` entirely against a TTL check that is broken in the
    // "never fires" direction -- the marker would never arrive, so the
    // spec's final per-key check would never run, and a permanently
    // orphaned key would pass unnoticed. Safe here specifically because
    // `TestDriver.p`'s heartbeat scenario ticks each node at most once;
    // a future scenario ticking the same node repeatedly before its true
    // last chance would need to gate this differently (e.g. only the
    // driver's own last tick carries a "final" flag).
    on eTick do (t: (round: int)) {
      send peer, eHeartbeat, (from = this, round = t.round);
      if (!peerDead && (t.round - lastHeartbeatRound >= heartbeatTTL)) {
        peerDead = true;
        sweepOrphanedKeys();
      }
      // §7 DegradedBoot: same withholding as `eCrashSignal` above, same
      // reason -- promotion, not this tick, is the last chance to drain
      // while degraded.
      if (!degraded) {
        announce eCrashSignalHandled;
      }
    }
  }

  // Shared by `eCrashSignal`'s oracle path, `eTick`'s heartbeat-TTL path,
  // and `eCatalogRestored`'s promotion re-check (see all three above):
  // any key this node already has staged but not yet committed is
  // orphaned once its peer is known dead -- take over and drain it.
  // Callers are responsible for setting `peerDead` and announcing
  // `eCrashSignalHandled` themselves, since each caller's surrounding
  // guard/idempotence needs differ (`eCrashSignal` fires exactly once per
  // scenario; `eTick` must not re-fire once `peerDead` is already true;
  // `eCatalogRestored` only calls this once, at promotion).
  fun sweepOrphanedKeys() {
    var k: int;
    // §7 DegradedBoot: no ownership action while degraded -- this is the
    // single enforcement point `NoOwnershipWhileDegraded` (Spec.p)
    // polices. Centralized here, rather than duplicated at each call
    // site above, so every current and future trigger route gets it for
    // free (the one call site that bypasses this helper -- `eForward`'s
    // own inline immediate-takeover check -- carries the equivalent
    // `!degraded` guard directly, see that handler).
    if (degraded) {
      return;
    }
    foreach (k in staged) {
      if (!(k in committed)) {
        committed += (k);
        announce eTakeoverDrain, (key = k, sq = 0, newOwner = this);
      }
    }
  }
}
