// Scenario: owner accepts a write, replicates it to the replica, then
// actually dies (eDie -- halts, matching real crash semantics) while the
// replica separately learns of the death out-of-band (eCrashSignal,
// standing in for heartbeat-TTL expiry). The checker explores every
// relative ordering of Forward / Receipt / Die / CrashSignal the P
// scheduler can produce.
machine TestTakeoverDrain {
  start state Init {
    entry {
      var owner: Node;
      var replica: Node;
      owner = new Node();
      replica = new Node();
      send owner, eLink, replica;
      send replica, eLink, owner;
      new Client((owner = owner, key = 1, sq = 1));
      send owner, eDie;
      send replica, eCrashSignal, (dead = owner,);
      // Simulate a retransmitted eForward for the same (key, sq, origin) --
      // real networks retry, and docs/design/replication.md §4 (PeerApply)
      // requires a duplicate/retried apply be acknowledged without
      // re-applying. Sent directly from the environment rather than routed
      // through `owner`'s own eWriteReq handling, since owner may already
      // be dead (eDie above) by the time a real retry would fire -- matches
      // how eCrashSignal above is also delivered directly by the
      // environment rather than through the peer's own logic.
      send replica, eForward, (key = 1, sq = 1, origin = owner);
    }
  }
}
