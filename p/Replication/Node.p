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

  start state Active {
    entry {
      peerDead = false;
    }

    on eLink do (p: Node) {
      peer = p;
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
      send peer, eForward, (key = req.key, sq = req.sq, origin = this);
    }

    on eForward do (fwd: (key: int, sq: int, origin: Node)) {
      staged += (fwd.key);
      if (!(fwd.key in claims)) {
        claims[fwd.key] = REPLICA;
        announce eClaimAdvertise, (key = fwd.key, node = this, role = REPLICA);
      }
      send fwd.origin, eReceipt, (key = fwd.key, sq = fwd.sq, holder = this);
      if (peerDead && !(fwd.key in committed)) {
        committed += (fwd.key);
        announce eTakeoverDrain, (key = fwd.key, sq = fwd.sq, newOwner = this);
      }
      announce eForwardHandled, (key = fwd.key, sq = fwd.sq);
    }

    on eReceipt do (rc: (key: int, sq: int, holder: Node)) {
      holders[rc.key] = holders[rc.key] + 1;
      if (holders[rc.key] >= 2 && rc.key in pendingClient) {
        send pendingClient[rc.key], eWriteAck, (key = rc.key, sq = rc.sq);
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
