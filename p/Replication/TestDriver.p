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
      send replica, eForward, (key = 1, sq = 1, origin = owner, originId = 0, inc = 0);
    }
  }
}

// Scenario: §5.7's incarnation-fencing/zombie hazard. `owner` (logical
// nodeId 1) accepts and forwards a write, then actually dies (eDie).
// Unlike `TestTakeoverDrain`, this scenario models the crashed node
// *coming back*: `newOwner` is a brand-new `Node` machine instance -- P
// machines cannot un-halt, so this is the only way to represent "the same
// logical node, rebooted" -- seeded via `eFenceBoot` with the same
// `nodeId` (1) and a strictly higher incarnation than `owner` ever had.
// `replica` is re-linked to `newOwner` (its peer is now the rebooted
// node), matching the real re-election/re-registration a live replica
// would do once it observes the new incarnation.
//
// The zombie: a Forward the OLD `owner` instance (nodeId 1, incarnation 1)
// queued for `replica` before it died -- standing in for a message
// already in flight over the network at crash time, delivered late --
// carries that stale incarnation and must be recognized as such whenever
// it arrives at `replica` *after* `replica` has already accepted a
// Forward from `newOwner` under the higher incarnation (2). The zombie
// Forward is injected directly from the environment (same convention
// TestTakeoverDrain already uses for its own retransmitted Forward,
// since the halted `owner` cannot send anything on its own); newOwner's
// key-2 Forward instead goes through a genuine second `Client` write (not
// a direct injection) specifically so newOwner's own `holders`/
// `pendingClient` bookkeeping is populated the normal way before
// `replica`'s Receipt comes back to it -- an earlier version of this
// scenario injected that Forward directly too and crashed exactly there
// (`KeyNotFoundException` on `holders[key]`), because a directly-injected
// Forward has no corresponding `eWriteReq` to have initialized the
// sender's own bookkeeping for that key. The two Forwards come from
// different senders (newOwner's own handler vs. this environment
// machine), so, unlike TestTakeoverDrain's single injected retransmit,
// their relative arrival order at `replica` is NOT FIFO-fixed -- the
// checker explores both; `FencedZombie` must hold in every ordering it
// finds, and the deliberately-broken scratch variant (PR description) is
// what confirms the "zombie arrives after" ordering is genuinely
// reachable and gets caught.
machine TestFenceBootZombie {
  start state Init {
    entry {
      var owner: Node;
      var replica: Node;
      var newOwner: Node;

      owner = new Node();
      replica = new Node();
      send owner, eFenceBoot, (nodeId = 1, priorIncarnation = 0);
      send replica, eFenceBoot, (nodeId = 2, priorIncarnation = 0);
      send owner, eLink, replica;
      send replica, eLink, owner;

      // owner accepts and forwards key 1 under incarnation 1, then dies
      // for real -- same shape as TestTakeoverDrain's opening.
      new Client((owner = owner, key = 1, sq = 1));
      send owner, eDie;

      // Reboot: a fresh machine instance for the SAME logical node
      // (nodeId 1), fenced to a strictly higher incarnation (2).
      newOwner = new Node();
      send newOwner, eFenceBoot, (nodeId = 1, priorIncarnation = 1);
      send newOwner, eLink, replica;
      send replica, eLink, newOwner;

      // The rebooted owner accepts a genuine second write (key 2) under
      // its new incarnation and forwards it through its own eWriteReq
      // handler, same as owner's key-1 write above.
      new Client((owner = newOwner, key = 2, sq = 2));

      // The zombie (see header comment above).
      send replica, eForward, (key = 3, sq = 1, origin = owner, originId = 1, inc = 1);
    }
  }
}
