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
      new Client((owner = owner, key = 1, sq = 1, originSeq = 1));
      send owner, eDie;
      send replica, eCrashSignal, (dead = owner,);
      // Simulate a retransmitted eForward for the same (key, sq, seq,
      // origin) -- real networks retry, and docs/design/replication.md §4
      // (PeerApply) requires a duplicate/retried apply be acknowledged
      // without re-applying (the same `seq` as the original, since this is
      // the SAME record retried, not a new one). Sent directly from the
      // environment rather than routed through `owner`'s own eWriteReq
      // handling, since owner may already be dead (eDie above) by the time
      // a real retry would fire -- matches how eCrashSignal above is also
      // delivered directly by the environment rather than through the
      // peer's own logic.
      send replica, eForward, (key = 1, sq = 1, originSeq = 1, origin = owner, originId = 0, inc = 0);
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
// Forward from `newOwner` under the higher incarnation (2). It carries
// the SAME key (1), sq, and originSeq as owner's one real forward above
// -- a retransmit, by definition, resends the exact record the origin
// already assigned a seq to, matching TestTakeoverDrain's own retransmit
// convention (same key/sq/seq/origin) exactly; docs/design/replication.md
// §4 describes `seq` as a dense per-(origin, partition) sequence assigned
// once, at StageCommit -- a real origin's own honest bookkeeping can
// never assign that same seq to a second, different record, so a zombie
// standing in for a stale retransmit must name the SAME key the original
// send did, not an arbitrary different one (see the `fwd.key in
// staged`-guarded idempotent-duplicate branch's own comment in `Node.p`
// for what goes wrong if a test scenario violates that invariant). The
// zombie Forward is injected directly from the environment (same
// convention TestTakeoverDrain already uses for its own retransmitted
// Forward, since the halted `owner` cannot send anything on its own);
// newOwner's key-2 Forward instead goes through a genuine second `Client`
// write (not a direct injection) specifically so newOwner's own `holders`/
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
      // for real -- same shape as TestTakeoverDrain's opening, including
      // telling replica out-of-band (this scenario's own oversight, caught
      // once a genuine hot-state liveness monitor -- NoAckedLossLive,
      // Spec.p -- was wired in: without this send, replica's
      // `crashHandled` flag for owner never flips, so the pre-existing
      // direct-assert `NoAckedLoss` was silently vacuous for key 1 in this
      // scenario the whole time -- its check body never ran, reporting a
      // spuriously clean "0 bugs").
      //
      // Fixing that vacuity surfaced a second, real, deeper finding
      // (#190, not fixed here): `owner`'s own `eForward` for key 1 can
      // still be queued behind its not-yet-dequeued `eDie` (P delivers a
      // machine's own earlier-queued messages before a later one, even
      // one that will halt it), so in some explored orderings `owner`
      // sends its Forward *after* `newOwner` has already advanced
      // `replica`'s fencing past incarnation 1 -- a legitimately-accepted
      // write gets correctly fenced out as stale, with no timeout/ring-
      // walk retry modeled anywhere in this P model to recover it
      // (docs/design/replication.md §1's real mechanism for exactly this
      // case). NoAckedLoss/NoAckedLossLive are deliberately NOT asserted
      // against this scenario (TestDecl.p) because of that -- it isn't a
      // vacuous-check problem this time, it's real, unmodeled scope; see
      // #190. seq = 1: nodeId 1's first-ever forward.
      new Client((owner = owner, key = 1, sq = 1, originSeq = 1));
      send owner, eDie;
      send replica, eCrashSignal, (dead = owner,);

      // Reboot: a fresh machine instance for the SAME logical node
      // (nodeId 1), fenced to a strictly higher incarnation (2).
      newOwner = new Node();
      send newOwner, eFenceBoot, (nodeId = 1, priorIncarnation = 1);
      send newOwner, eLink, replica;
      send replica, eLink, newOwner;

      // The rebooted owner accepts a genuine second write (key 2) under
      // its new incarnation and forwards it through its own eWriteReq
      // handler, same as owner's key-1 write above. seq = 2, NOT 1: this
      // is nodeId 1's SECOND-ever forward -- a real node's own
      // per-(origin, partition) `nextSeq` (§4) is durably persisted (the
      // hot staging table itself, "the table is the log") and survives
      // the reboot in between, unlike its volatile P machine instance, so
      // the environment (standing in for that persisted counter, the same
      // convention `eFenceBoot`'s `priorIncarnation` already establishes)
      // continues the count rather than restarting it at 1.
      new Client((owner = newOwner, key = 2, sq = 2, originSeq = 2));

      // The zombie (see header comment above) -- key = 1, sq = 1, seq = 1,
      // matching owner's one-and-only real forward above exactly: this
      // stands in for a stale retransmit of THAT SAME record, not a new
      // one (see the header comment for why a retransmit cannot
      // legitimately carry a different key at the same originSeq).
      send replica, eForward, (key = 1, sq = 1, originSeq = 1, origin = owner, originId = 1, inc = 1);
    }
  }
}

// Scenario: the same owner-accepts-forwards-then-dies narrative as
// `TestTakeoverDrain`, but the replica derives the owner's death from a
// lapsed heartbeat TTL itself -- `Node.p`'s `eTick`/`eHeartbeat` handlers
// -- instead of being told about it by `eCrashSignal`'s oracle. Neither
// node ever receives `eCrashSignal`, `eFenceBoot`, or an `eLink`-only
// setup; this is purely the heartbeat-TTL death-detection path in
// isolation.
//
// `owner`'s round-1 heartbeat is injected directly into `replica` from
// this environment, rather than by ticking `owner` itself -- the same
// "sent directly from the environment, standing in for the peer's own
// send" convention `TestTakeoverDrain`'s retransmitted Forward and
// `TestFenceBootZombie`'s zombie Forward already use above, and required
// here for a sharper reason than convenience: `eTick`'s handler
// (`Node.p`) unconditionally announces `eCrashSignalHandled` once it
// finishes, exactly mirroring `eCrashSignal`'s own always-announce
// handler (see that comment). Ticking `owner` as well as `replica` would
// let `owner`'s tick announce that marker the moment it happens to be
// scheduled -- which can race *before* `owner` has even processed its
// own `eWriteReq`/Forward -- and `NoAckedLoss` would then race-condition
// a false positive on a perfectly correct model, the exact failure shape
// its own header (`Spec.p`) already warns about for a premature marker.
// Restricting `eTick` to the one node whose own detection cycle this
// scenario is actually testing (`replica`) avoids that hazard entirely
// while still exercising `Node.p`'s `eTick` handler faithfully for the
// role it plays here.
//
// Round shape (`Node.p`'s `heartbeatTTL` is 3): `replica` receives
// `owner`'s round-1 heartbeat (bumping `lastHeartbeatRound` to 1) at some
// point -- its relative order against the Forward chain and against
// `replica`'s own later tick is unconstrained (different senders), and
// the checker explores every ordering. `owner` then actually dies
// (`eDie`), so no further heartbeats are ever sent (nothing in this
// environment ticks `owner` again). `replica` is ticked exactly once, at
// round 4: whether the round-1 heartbeat has already been delivered by
// then or not, `4 - lastHeartbeatRound` is >= 3 either way (4 - 1 = 3, or
// 4 - 0 = 4 if the heartbeat hasn't landed yet) -- detection fires under
// both orderings.
//
// `NoAckedLoss` and `ClaimAdvertiseOnce` (`TestDecl.p`) apply unchanged --
// see `eCrashSignalHandled`'s Events.p comment for why `NoAckedLoss`
// needs no modification to cover this scenario.
machine TestHeartbeatDetection {
  start state Init {
    entry {
      var owner: Node;
      var replica: Node;
      owner = new Node();
      replica = new Node();
      send owner, eLink, replica;
      send replica, eLink, owner;

      new Client((owner = owner, key = 1, sq = 1, originSeq = 1));

      // owner's one round-1 heartbeat, then it actually dies.
      send replica, eHeartbeat, (from = owner, round = 1);
      send owner, eDie;

      // replica's own tick cycle advances straight to round 4 (>=
      // heartbeatTTL rounds past the last heartbeat owner ever sent,
      // round 1) -- crossing the gap regardless of whether that round-1
      // heartbeat has already been delivered to replica by this point.
      send replica, eTick, (round = 4,);
    }
  }
}

// Scenario: §7's DegradedBoot -- a rebooted node comes back during a
// catalog outage and must take no ownership action (no takeover-drain)
// until the catalog returns, even though it still applies and receipts
// replication normally throughout. Reuses `TestFenceBootZombie`'s reboot
// mechanism (`eFenceBoot` with the crashed node's own `nodeId` and a
// strictly higher incarnation) but drives the rebooted node into exactly
// the situation that would otherwise trigger a takeover: it holds a
// staged, uncommitted key belonging to a peer that then dies too.
//
// Unlike `TestFenceBootZombie`, `owner` here dies before ever accepting
// anything -- this scenario isolates DegradedBoot's own-node
// ownership-suppression effect and does not need a first write through
// `owner`. (`TestFenceBootZombie`'s own key-1-staged-at-replica-forever
// shape only stays sound there because that scenario never announces
// `eCrashSignalHandled` at all -- see `NoAckedLoss`'s marker comments in
// Node.p; this scenario's promotion path *does* announce it, so
// reproducing that same dangling-key shape here would trip a false
// `NoAckedLoss` positive on an unrelated key. Simpler is also correct
// here: `owner`'s reboot identity is all this scenario needs from it.)
//
// Roles: `owner` (nodeId 1) and `replica` (nodeId 2) both boot clean,
// then `owner` dies immediately (`eDie`). `newOwner` reboots as nodeId
// 1's next incarnation (2) while the catalog is down (`eCatalogOutage`
// sent first, same-sender-to-same-target FIFO with the `eFenceBoot`
// right after it, so the outage is guaranteed observed before FenceBoot
// evaluates it) -- `priorIncarnation = 1 > 0` plus the outage puts
// `newOwner` straight into `degraded` per `Node.p`'s `eFenceBoot`
// handler.
//
// `replica` then accepts a genuine write (key 1) and forwards it to
// `newOwner`, which stages it as a replica -- exactly the
// staged-but-uncommitted shape `sweepOrphanedKeys` looks for. `replica`
// itself then dies, and the environment tells `newOwner` about it
// (`eCrashSignal`, the same oracle convention `TestTakeoverDrain`/
// `TestFenceBootZombie` use, chosen deliberately over the heartbeat-TTL
// path so this scenario isolates DegradedBoot from
// `TestHeartbeatDetection`'s already-covered mechanism -- combining the
// two remains open scope, see docs/design/p-tla-correspondence.md).
// `newOwner` is still degraded at this point: whether the Forward for
// key 1 has already landed or not by the time `eCrashSignal` arrives, no
// `eTakeoverDrain` may be announced yet -- exactly what
// `NoOwnershipWhileDegraded` (Spec.p) checks, across every relative
// ordering of {Forward, eCrashSignal} the checker explores (both are
// sent by different senders -- `replica` and the environment -- so their
// arrival order at `newOwner` is unconstrained).
//
// Finally the catalog returns (`eCatalogRestored`): `newOwner` promotes
// out of degraded and, per §7's "promotes itself... and FenceBoot
// completes," re-checks for orphaned keys right at that moment -- key 1
// becomes eligible for takeover here, not merely from some later trigger
// (`Node.p`'s `eCatalogRestored` handler calls `sweepOrphanedKeys`
// itself). `NoAckedLoss` still holds: the key is drained, just later than
// in the non-degraded scenarios, and `Node.p`'s marker-withholding while
// degraded (see `eCrashSignal`/`eTick`'s comments) is what keeps
// `NoAckedLoss` from false-firing on that legitimate delay.
machine TestDegradedBoot {
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

      // owner dies immediately, before accepting anything -- see header
      // comment above for why this scenario skips TestFenceBootZombie's
      // first-write-through-owner shape.
      send owner, eDie;

      // Reboot into a catalog outage: newOwner (same logical nodeId 1,
      // fenced to incarnation 2) boots while the catalog is unreachable,
      // so it comes up in degraded mode (persisted incarnation + catalog
      // down = DegradedBoot, docs/design/replication.md §7).
      newOwner = new Node();
      send newOwner, eCatalogOutage;
      send newOwner, eFenceBoot, (nodeId = 1, priorIncarnation = 1);
      send newOwner, eLink, replica;
      send replica, eLink, newOwner;

      // replica accepts a genuine write (key 1) and forwards it to
      // newOwner -- newOwner stages key 1 as a replica while degraded.
      // seq = 1: replica's (nodeId 2) first-ever forward.
      new Client((owner = replica, key = 1, sq = 1, originSeq = 1));

      // replica itself now dies. newOwner is (or will be) holding
      // replica's staged key 1, uncommitted -- ordinarily exactly the
      // shape that triggers immediate takeover, but newOwner is still
      // degraded: no ownership action is permitted until the catalog
      // comes back.
      send replica, eDie;
      send newOwner, eCrashSignal, (dead = replica,);

      // The catalog returns: newOwner promotes out of degraded mode and
      // must re-check for orphaned keys that were suppressed while
      // degraded -- key 1 becomes eligible for takeover right here, not
      // merely "from now on."
      send newOwner, eCatalogRestored;
    }
  }
}

// Scenario: §7's OTHER boot case -- "a genuinely new node[,] no
// persisted incarnation[,] waits, in a typed startup state" --
// distinguished here from `TestFenceBootZombie`'s reboot-with-history
// path by starting the dying node's replacement from an `eFenceBoot`
// with `priorIncarnation = 0`, not a nonzero prior.
//
// `owner` (nodeId 1) accepts and forwards a write, same opening as
// `TestFenceBootZombie`, then suffers `eCrashWipe` -- not `eDie` --
// standing in for `specs/DuckSpoutCore.tla`'s `CrashWipe(n)` (the disk
// dies too). Its replacement, `newOwner`, is deliberately given a
// DIFFERENT `nodeId` (3, not 1): TLA+'s `CrashWipe` comment says a wiped
// node "never re-enters" (its `wiped' = wiped \cup {n}` permanently
// blocks both `FenceBoot(n)` and `DegradedBoot(n)` for that same `n`
// forever after), so this is deliberately NOT modeled as nodeId 1 coming
// back -- see `eCrashWipe`'s header comment (Events.p) for the full
// correspondence caveat. `newOwner` boots straight into a catalog outage
// (`eCatalogOutage` sent first, same-sender-to-same-target FIFO with the
// `eFenceBoot` right after it, the same convention `TestDegradedBoot`
// already uses to guarantee the outage is observed first) with
// `priorIncarnation = 0` -- nothing persisted to fall back to -- so it
// cannot complete FenceBoot at all and goes to `Node.p`'s `Waiting`
// state instead of `DegradedBoot`'s replica-only mode.
//
// While `newOwner` waits, `replica` accepts a genuine second write (key
// 2) and forwards it to `newOwner` -- exactly the race
// `NoIdentityWhileWaiting` must hold across: this Forward and the
// `eCatalogRestored` below come from different senders (`replica` vs.
// this environment), so their relative arrival order at `newOwner` is
// unconstrained and the checker explores both. Whichever order, `Waiting`
// drops the Forward outright (see its own comment, Node.p) -- no claim,
// no receipt, no takeover can result from it.
//
// The catalog then returns (`eCatalogRestored`): `newOwner` completes
// its very first fence (nodeId 3, incarnation 1) and leaves `Waiting`.
// A final genuine write routed through `newOwner` (key 3) confirms the
// positive path too -- promotion out of `Waiting` genuinely restores
// full participation, not merely a permanently-stuck node.
machine TestNewNodeBoot {
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

      // owner accepts and forwards key 1 under incarnation 1, then
      // suffers a wipe, not merely a crash -- see eCrashWipe's header
      // comment (Events.p) for the eDie/eCrashWipe distinction. seq = 1:
      // nodeId 1's first-ever forward.
      new Client((owner = owner, key = 1, sq = 1, originSeq = 1));
      send owner, eCrashWipe;

      // The replacement: a genuinely new node (fresh nodeId 3, NOT
      // owner's old nodeId 1 -- see this machine's header comment above
      // for why), booting straight into a catalog outage with no
      // persisted incarnation to bump.
      newOwner = new Node();
      send newOwner, eCatalogOutage;
      send newOwner, eFenceBoot, (nodeId = 3, priorIncarnation = 0);
      send newOwner, eLink, replica;
      send replica, eLink, newOwner;

      // A peer forward races the still-waiting newOwner (see header
      // comment above). seq = 1: replica's (nodeId 2) first-ever forward
      // (distinct from `sq = 2`, the client's own unrelated request
      // counter).
      new Client((owner = replica, key = 2, sq = 2, originSeq = 1));

      // The catalog returns: newOwner completes its first-ever fence and
      // leaves the waiting state.
      send newOwner, eCatalogRestored;

      // Positive path: newOwner can now genuinely participate. seq = 1:
      // newOwner's (nodeId 3) own first-ever forward.
      new Client((owner = newOwner, key = 3, sq = 1, originSeq = 1));
    }
  }
}

// Scenario: §5.4's PeerApply gap-refusal (docs/design/replication.md §4;
// `specs/DuckSpoutCore.tla`'s `GapFreedom`, `PeerApply`'s `g.rec.seq =
// AppliedThru(...) + 1` guard) -- the third named safety property #132's
// own DoD calls out alongside NoAckedLoss and FencedZombie, and the one
// docs/design/p-tla-correspondence.md previously named as having "no P
// counterpart" (see that file's `eForward`/`PeerApply` row, corrected
// alongside this scenario).
//
// `sender1` and `sender2` both call `eFenceBoot` with the SAME `nodeId`
// (1) -- deliberately explicit, not left resting on P's zero-default the
// way `TestTakeoverDrain`/`TestHeartbeatDetection` do for a scenario that
// never exercises §5.7 fencing at all: this scenario's whole premise
// *depends on* both senders sharing one logical origin identity (see the
// race below), so that premise is encoded directly rather than left as an
// implicit consequence of neither sender ever fence-booting, which would
// silently go vacuous if `Node.p`'s zero-value default ever changed.
// `receiver` itself never calls `eFenceBoot` (§5.7 fencing keys off the
// *sender's* logical id from the receiver's point of view -- `receiver`'s
// own identity is never read by anything this scenario checks) and is
// also never `eLink`ed to either sender: `Node.p`'s `peer` field models
// one owner-replica pair, and `receiver` genuinely has two upstream
// senders here, so linking it to just one would misrepresent the
// topology rather than complete it -- `receiver` never itself needs to
// `send peer, ...` in this scenario (it only ever replies via `eReceipt`,
// addressed by `fwd.origin`, not `peer`), so the field is correctly left
// unset rather than forced into a misleading link.
//
// The race: TWO DIFFERENT `Node` machine instances, `sender1` and
// `sender2`, share the SAME logical origin identity (nodeId 1, set
// explicitly above) without any reboot involved -- the same "two
// different senders, so their relative arrival order at the receiver is
// unconstrained" trick `TestFenceBootZombie`'s reboot mechanism already
// establishes for racing two forwards from one logical origin, used here
// with both senders at the same, unchanging incarnation throughout (no
// reboot, no fencing dynamics to exercise). `sender1` accepts and
// forwards the FIRST write (key 1, seq 1); `sender2` accepts and forwards
// the SECOND (key 2, seq 2) -- each through its OWN real `eWriteReq`
// handler, not a direct injection, so each sender's own
// `holders`/`pendingClient` bookkeeping is populated before its own
// Receipt can come back to it. This is deliberate, not incidental:
// `TestFenceBootZombie`'s header comment records that an earlier version
// of THAT scenario injected its second write directly and crashed with a
// `KeyNotFoundException` on `holders[key]`, because a directly-injected
// Forward has no corresponding `eWriteReq` to have initialized the
// sender's own bookkeeping for that key. Routing both writes here through
// real `eWriteReq` handlers is what gives each sender's OWN write correct
// bookkeeping before its own Receipt returns -- but the identical crash
// still reappeared here during authoring, from a DIFFERENT source: the
// retransmit below is itself a direct injection under a still-alive
// origin (`sender2`), so the Receipt it can trigger is not causally
// downstream of `sender2`'s own `eWriteReq` at all, and `sender2` can
// dequeue that Receipt before it dequeues its own `eWriteReq` (different
// senders, no FIFO relationship between them) -- the two-real-senders
// shape narrows *how often* this is reachable but does not, by itself,
// rule it out. `Node.p`'s `eReceipt` handler now guards on `rc.key in
// holders` (see that handler's own comment) precisely for this: that
// guard, not the two-real-senders shape, is what actually fixes the
// crash; the two-real-senders shape is what makes the underlying race
// (and GapFreedom's own out-of-order scenario) reachable and exercised at
// all.
//
// A third message -- a retransmit of `sender2`'s write, claiming `origin
// = sender2` -- stands in for the origin's own retry once its
// receipt-timeout (§4) expires without an ack, or the catch-up query
// (§4, "the table is the log") re-sending it -- the same "sent directly
// from the environment, standing in for the peer's own send" convention
// `TestTakeoverDrain`'s retransmitted Forward already uses. Environment-
// injected, so its arrival is unconstrained relative to BOTH real writes.
// The checker explores every relative order of
// {`sender2`'s real Forward, this retransmit, `sender1`'s real Forward}
// it can produce:
//   - either delivery of `sender2`'s write (key 2, seq 2) arrives at
//     `receiver` before `sender1`'s (key 1, seq 1): GapFreedom's gate
//     refuses it (appliedThru is still 0, so seq 2 != 0 + 1) -- no apply,
//     no claim advertisement, no receipt. Once `sender1`'s write then
//     arrives, it is accepted normally (appliedThru: 0 -> 1). Whichever
//     delivery of `sender2`'s write lands after that is now accepted too
//     (appliedThru: 1 -> 2) -- both writes applied, in the now-correct
//     order, exactly the recovery this scenario is built to demonstrate.
//   - BOTH deliveries of `sender2`'s write race ahead of `sender1`'s:
//     both are refused, and key 2 never applies within this bounded
//     scenario. This is safe, not a bug -- GapFreedom is a safety
//     property (no seq is ever accepted out of order), not a liveness
//     guarantee that a refused write is eventually retried successfully
//     within a fixed, bounded scenario.
//   - `sender1`'s write arrives before either delivery of `sender2`'s: no
//     gap ever occurs; both keys apply in order, appliedThru: 0 -> 1 -> 2.
machine TestGapFreedom {
  start state Init {
    entry {
      var sender1: Node;
      var sender2: Node;
      var receiver: Node;

      sender1 = new Node();
      sender2 = new Node();
      receiver = new Node();
      // Explicit shared logical origin (nodeId 1) for both senders -- see
      // header comment above for why this is spelled out rather than
      // resting on P's zero-default. Sent before either `new Client(...)`
      // below so each sender's own eFenceBoot is already enqueued (this
      // entry runs as one atomic step) ahead of anything a spawned
      // Client's eWriteReq could ever send it.
      send sender1, eFenceBoot, (nodeId = 1, priorIncarnation = 0);
      send sender2, eFenceBoot, (nodeId = 1, priorIncarnation = 0);
      send sender1, eLink, receiver;
      send sender2, eLink, receiver;

      // sender1's genuine first write and sender2's genuine second write
      // -- see header comment above for why both are routed through
      // their own real eWriteReq handlers.
      new Client((owner = sender1, key = 1, sq = 1, originSeq = 1));
      new Client((owner = sender2, key = 2, sq = 2, originSeq = 2));

      // Retransmit of sender2's write (see header comment above).
      // originId = 1, inc = 1: sender2's own logical identity and
      // incarnation, matching the eFenceBoot above (nodeId 1,
      // priorIncarnation 0 -> incarnation 1).
      send receiver, eForward, (key = 2, sq = 2, originSeq = 2, origin = sender2, originId = 1, inc = 1);
    }
  }
}
