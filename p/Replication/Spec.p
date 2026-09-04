// NoAckedLoss (P half, minimal slice): every key a node has durably
// STAGED (accepted for replication -- eAccepted, not the client's raw
// request, which may never even be accepted if its target node dies
// first) must be drained (committed) via takeover by the time the
// scenario's last relevant event for that key has been handled -- this
// scenario's owner always dies before any normal-path commit exists, so
// takeover is the only route to durability; a key that never gets there
// is lost forever.
//
// Checked as a direct `assert`, alongside a genuine `hot state` liveness
// twin (`NoAckedLossLive`, below) checking the identical property. An
// earlier attempt at the `hot state` formulation here (#132 finding)
// never fired even for a deliberately-broken variant, and this file's
// header wrongly concluded from that result that P's checker does not
// flag "terminated while hot" as a violation for a bounded scenario --
// **corrected**: deep research (P's own manual, Coyote's docs, a live
// p-org/P GitHub discussion) found the opposite is explicitly
// documented, and a direct retest confirms it empirically --
// `NoAckedLossLive` below does fire, 100% of schedules, against the same
// class of broken variant, once wired correctly. The earlier "never
// fired" result was actually the exact same test-declaration wiring bug
// (a `test` block whose machine set never actually attached the spec via
// `assert SpecName in {...}`) found and fixed elsewhere in this file's
// history -- the hot-state monitor was never running at all, not failing
// to detect anything. See `NoAckedLossLive`'s own header for the checked
// mechanism and the ordering fix both twins now share.
//
// There are two events that can still drain a key -- `eForward`'s own
// inline check, and `eCrashSignal`'s staged-keys sweep -- and either can
// happen first (a Forward can race the crash signal in both directions).
// So the assert cannot simply fire off of one marker event: it fires once
// *both* `eForwardHandled` and `eCrashSignalHandled` have been observed
// for a key, i.e. once neither remaining trigger can still drain it. An
// earlier version of this spec asserted straight off `eForwardHandled`
// alone and produced a false positive on the correct model precisely
// because a Forward that arrives before the crash signal is still validly
// pending at that point -- the crash-signal sweep is what drains it, a
// moment later.
//
// `pending`/`drained` are tracked as two separately-accumulated monotonic
// sets rather than one add-on-accept/subtract-on-drain set, and the check
// is `accepted subset-of drained`, not `!(key in pending)` -- a second
// #132-class finding, surfaced once TestDriver.p started sending a
// duplicate `eForward` directly from the environment (ClaimAdvertiseOnce
// slice): because that duplicate is injected independently of the owner's
// own `eWriteReq` handling, the checker can schedule the replica's
// takeover-drain for a key *before* the owner has even announced
// `eAccepted` for it (P's scheduler does not owe environment-originated
// sends any happens-before relationship with each other). A
// subtract-then-check-absence set goes permanently wrong under that
// ordering: `eTakeoverDrain` fires first and subtracts a key that was
// never added, then `eAccepted` adds it back with nothing left to ever
// remove it, producing a false NoAckedLoss violation on a trace where the
// real system behaved correctly throughout (Node.p's guards held: no
// re-advertise, no re-commit, and the key was genuinely drained). Tracking
// the two events as independent monotonic sets and checking set
// containment sidesteps the ordering entirely -- this is the same fix
// shape as the earlier `eForwardHandled`-alone false positive above: the
// monitor's bookkeeping, not the modeled system, was wrong.
//
// Mirrors DuckSpoutCore.tla's NoAckedLoss in spirit, not text -- this is a
// hand-written P analog, not a transliteration. (Previously this header
// also claimed to mirror GapFreedom "in spirit" -- true only for as long
// as GapFreedom had no dedicated P analog of its own; now that it does
// (`GapFreedom`, below), that claim here would double-count the same
// property under two different spec names, so it is retired from this
// header.)
spec NoAckedLoss observes eAccepted, eTakeoverDrain, eForwardHandled, eCrashSignalHandled {
  var accepted: set[int];
  var drained: set[int];
  // Keys whose eForwardHandled fired before eCrashSignalHandled did --
  // checked once eCrashSignalHandled finally arrives, since neither
  // trigger can drain a key any further after that point.
  var forwardedBeforeCrash: set[int];
  var crashHandled: bool;

  start state Watching {
    entry {
      crashHandled = false;
    }

    on eAccepted do (acc: (key: int, sq: int)) {
      accepted += (acc.key);
    }

    on eTakeoverDrain do (td: (key: int, sq: int, newOwner: Node)) {
      drained += (td.key);
    }

    on eForwardHandled do (fh: (key: int, sq: int)) {
      if (crashHandled)
        assert !(fh.key in accepted) || (fh.key in drained),
          "NoAckedLoss violated: an accepted key was never drained";
      else
        forwardedBeforeCrash += (fh.key);
    }

    on eCrashSignalHandled do {
      var k: int;
      crashHandled = true;
      foreach (k in forwardedBeforeCrash) {
        assert !(k in accepted) || (k in drained),
          "NoAckedLoss violated: an accepted key was never drained";
      }
    }
  }
}

// NoAckedLossLive: the SAME property as `NoAckedLoss` above ("every
// accepted key is eventually drained"), checked as a genuine `hot state`
// liveness monitor instead of a direct assert at a hand-picked
// last-relevant-event marker. #132's DoD names "liveness monitors" as its
// own deliverable, and this is the idiomatic P mechanism for exactly that
// -- P's own manual (p-org.github.io/P/manual/monitors/) states plainly:
// "If the program terminates and the monitor is in a hot state, then
// there is a liveness bug." An earlier attempt at this (`NoAckedLoss`'s
// header, above) never fired for a deliberately-broken variant and wrongly
// concluded the mechanism itself doesn't work for a bounded scenario; a
// retest confirms it does, once (a) the test declaration actually attaches
// the spec (`assert SpecName in {...}`, not a bare machine set -- the same
// wiring bug found and fixed elsewhere this session) and (b) the same
// ordering fix `NoAckedLoss` needed applies here too: `eTakeoverDrain` for
// a key can legitimately arrive before `eAccepted` for that key
// (environment-originated sends have no happens-before relationship), so
// tracking one mutable `pending` set (add-on-accept, subtract-on-drain)
// loses a drain that happened "too early" -- `accepted`/`drained` as
// independent monotonic sets, checked by subset containment, sidesteps it.
//
// Both `NoAckedLoss` and `NoAckedLossLive` are kept, deliberately: the
// direct assert is cheaper and still correct; this hot-state twin is what
// literally satisfies "liveness monitors" as its own artifact, not a
// stand-in for the other. Verified (see the PR this landed in): 0 bugs
// across 5000 schedules on the correct model; a deliberately-broken
// variant (removing `eForward`'s inline late-arrival takeover check) is
// caught in 100% of schedules, by this spec alone with every other spec
// unwired, via P's documented termination-in-a-hot-state rule.
spec NoAckedLossLive observes eAccepted, eTakeoverDrain {
  var accepted: set[int];
  var drained: set[int];

  start state NoPending {
    on eTakeoverDrain do (td: (key: int, sq: int, newOwner: Node)) {
      drained += (td.key);
    }

    on eAccepted do (acc: (key: int, sq: int)) {
      accepted += (acc.key);
      if (!(acc.key in drained))
        goto Draining;
    }
  }

  hot state Draining {
    on eAccepted do (acc: (key: int, sq: int)) {
      accepted += (acc.key);
    }

    on eTakeoverDrain do (td: (key: int, sq: int, newOwner: Node)) {
      drained += (td.key);
      if (IsSubsetOf(accepted, drained))
        goto NoPending;
    }
  }
}

fun IsSubsetOf(a: set[int], b: set[int]) : bool {
  var x: int;
  foreach (x in a) {
    if (!(x in b))
      return false;
  }
  return true;
}

// ClaimAdvertiseOnce (§5.5): "the first apply for a partition the node has
// no claim row for triggers the insert" -- a *second* apply for a key this
// node already holds a claim for must NOT re-advertise. Modeled as a
// registry-row-count invariant: the (key, node) pair may appear in the
// claims table at most once, ever. A direct assert (not `hot state`) for
// the same reason as NoAckedLoss above -- this is a per-event safety check,
// not an eventually-property.
spec ClaimAdvertiseOnce observes eClaimAdvertise {
  var seen: set[(key: int, node: Node)];

  start state Watching {
    on eClaimAdvertise do (ca: (key: int, node: Node, role: tRole)) {
      assert !((key = ca.key, node = ca.node) in seen),
        "ClaimAdvertiseOnce violated: a node re-advertised a claim it already holds";
      seen += ((key = ca.key, node = ca.node));
    }
  }
}

// FencedZombie (§5.7 -- matches DuckSpoutCore.tla's own invariant name for
// the same property, not its text; the TLA+ analog is `staleApplied = {}`
// checked against PeerApply's `g.inc >= highestSeen[m][origin]` guard --
// see `specs/DuckSpoutCore.tla` lines ~304-316, ~988). Hand-written P
// analog, same convention `NoAckedLoss` and `ClaimAdvertiseOnce` above
// already establish for this file: never a transliteration.
//
// The property: no node ever accepts a message carrying an incarnation
// strictly lower than the highest incarnation it has already accepted
// from that same logical sender. Checked by recomputing, purely from the
// `eFenceDecision` event stream `Node.p` announces, an independent
// ground truth of "the highest incarnation this receiver has legitimately
// accepted from this sender so far" -- and asserting every *accepted*
// decision is consistent with it. This is deliberate: the spec does not
// read `Node.p`'s own internal `highestSeen` map (that would just be
// checking the implementation against itself, tautologically green
// whether or not the implementation's fencing logic is actually correct
// -- the "gamed test" failure mode). Recomputing the yardstick from the
// announced record of decisions is what lets this catch a broken `Node.p`
// that stops enforcing the fence but keeps announcing `eFenceDecision`
// honestly (see the scratch broken variant in the PR this spec shipped
// with): if `Node.p` wrongly accepts a stale message, the *spec's own*
// bookkeeping -- built from nothing but the accept/reject record -- still
// has the true highest incarnation on file for that (receiver, sender)
// pair, and the assert fires on the wrongly-accepted message itself.
//
// A direct `assert`, not `hot state`, for the same reason as `NoAckedLoss`
// above: this is a per-event safety check on a bounded scenario, not an
// eventually-property, and P's default bugfinding checker does not treat
// "terminated while hot" as a violation the way TLA+'s fairness-driven
// `~>` checking does (#132 finding, see `NoAckedLoss`'s header).
spec FencedZombie observes eFenceDecision {
  // Highest incarnation accepted so far, per (receiver, sender) pair.
  var highestAccepted: map[(receiver: Node, sender: int), int];

  start state Watching {
    on eFenceDecision do (fd: (receiver: Node, senderId: int, inc: int, accepted: bool)) {
      var k: (receiver: Node, sender: int);
      var seen: int;
      if (fd.accepted) {
        k = (receiver = fd.receiver, sender = fd.senderId);
        seen = 0;
        if (k in highestAccepted) {
          seen = highestAccepted[k];
        }
        assert fd.inc >= seen,
          "FencedZombie violated: a node accepted a message whose incarnation was strictly lower than the highest incarnation already accepted from that sender";
        if (fd.inc > seen) {
          highestAccepted[k] = fd.inc;
        }
      }
    }
  }
}

// NoOwnershipWhileDegraded (§7 DegradedBoot, docs/design/replication.md):
// no node ever announces `eTakeoverDrain` while it is degraded at the
// moment of the announce -- a degraded node may keep applying and
// receipting replication, but must take no ownership action until it
// promotes.
//
// Checked by recomputing each node's degraded status purely from the
// `eDegradedChanged` event stream `Node.p` announces, the same
// independent-ground-truth convention `FencedZombie` above already
// establishes for this file (see its header comment for why): reading
// `Node.p`'s own `degraded` field directly would just check the
// implementation against itself, tautologically green whether or not the
// guard at every takeover call site is actually wired correctly. This
// spec's own scratch broken variant (PR description) -- which drops the
// `!degraded` guard from one takeover-drain call site -- is what confirms
// this recomputation genuinely catches a node draining while still
// degraded, not merely a model that happens to never exercise the gap.
//
// A direct `assert`, not `hot state`, matching this file's established
// convention for a per-event safety check on a bounded scenario -- see
// `NoAckedLoss`'s header above for why `hot state` liveness is the wrong
// shape here.
spec NoOwnershipWhileDegraded observes eDegradedChanged, eTakeoverDrain {
  var degradedNodes: set[Node];

  start state Watching {
    on eDegradedChanged do (dc: (node: Node, degraded: bool)) {
      if (dc.degraded) {
        degradedNodes += (dc.node);
      } else {
        degradedNodes -= (dc.node);
      }
    }

    on eTakeoverDrain do (td: (key: int, sq: int, newOwner: Node)) {
      assert !(td.newOwner in degradedNodes),
        "NoOwnershipWhileDegraded violated: a node announced a takeover-drain while still degraded";
    }
  }
}

// GapFreedom (§5.4, docs/design/replication.md §4; matches
// `specs/DuckSpoutCore.tla`'s own invariant name for the same property,
// not its text -- the TLA+ analog is a STATE invariant recomputed from
// `staged`/`DrainedSeqs` (`AppliedThru`'s `PrefixLen`, checked at every
// reachable state by construction); the P analog below recomputes the
// same contiguity independently from `Node.p`'s `eGapDecision`
// announcements, checked as each new watermark advance is observed.
// #132's own DoD names this as the third safety property to mirror
// alongside `NoAckedLoss` and `FencedZombie`, and it is the one
// docs/design/p-tla-correspondence.md previously flagged as having "no P
// counterpart" (see that file's `eForward`/`PeerApply` row).
//
// The property: a node's applied `seq` prefix, per sender, is always
// contiguous -- no `seq` is ever genuinely accepted (i.e. applied,
// advancing that sender's watermark at this receiver) out of order.
// Checked by recomputing, purely from the `eGapDecision` event stream
// `Node.p` announces, an independent ground truth of "the highest `seq`
// this receiver has legitimately accepted from this sender so far" --
// the SAME non-tautological, non-gamed-test convention `FencedZombie`
// above already establishes for this file (see its header comment for
// why): the spec does not read `Node.p`'s own internal `appliedThru` map
// (that would just be checking the implementation against itself,
// tautologically green whether or not the gap-refusal guard is actually
// wired correctly). A broken `Node.p` that drops the gap-refusal guard
// entirely -- applying every Forward unconditionally, matching this
// model's behavior before this guard existed -- but still announces
// `eGapDecision` honestly is still caught: the spec's own yardstick, built
// from nothing but the announced accept/refuse record, still has the true
// contiguous watermark on file, and the assert fires on the wrongly
// "accepted" out-of-order seq itself (see the scratch broken variant in
// the PR this spec shipped with).
//
// A direct `assert`, not `hot state`, matching this file's established
// convention for a per-event safety check on a bounded scenario -- see
// `NoAckedLoss`'s header above for why `hot state` liveness is the wrong
// shape here.
spec GapFreedom observes eGapDecision {
  // Highest originSeq genuinely accepted (watermark-advancing) so far, per
  // (receiver, sender) pair. Absent key means 0 -- nothing accepted yet,
  // matching `Node.p`'s own `appliedThru` default convention.
  var highestAccepted: map[(receiver: Node, sender: int), int];

  start state Watching {
    on eGapDecision do (gd: (receiver: Node, senderId: int, originSeq: int, accepted: bool)) {
      var k: (receiver: Node, sender: int);
      var seen: int;
      if (gd.accepted) {
        k = (receiver = gd.receiver, sender = gd.senderId);
        seen = 0;
        if (k in highestAccepted) {
          seen = highestAccepted[k];
        }
        assert gd.originSeq == seen + 1,
          "GapFreedom violated: a seq was accepted that did not contiguously extend the applied prefix";
        highestAccepted[k] = gd.originSeq;
      }
    }
  }
}

// NoIdentityWhileWaiting (§7, docs/design/replication.md): "Only a
// genuinely new node -- no persisted incarnation -- waits, in a typed
// startup state. It has no identity to be safely partial with." The
// property: no node ever announces an identity-bearing action --
// `eClaimAdvertise` (establishing a claim under a role) or
// `eTakeoverDrain` (committing a key via takeover) -- while it is still
// in that waiting state (`Node.p`'s `Waiting`).
//
// Checked by recomputing each node's waiting status purely from the
// `eWaitingChanged` event stream `Node.p` announces -- the same
// independent-ground-truth convention `FencedZombie`/
// `NoOwnershipWhileDegraded` above already establish for this file (see
// `FencedZombie`'s header comment for why: reading `Node.p`'s own
// `waitingForFence` field directly would just check the implementation
// against itself, tautologically green whether or not the `Waiting`
// state's guards are actually wired correctly). This spec's own scratch
// broken variant (PR description) -- which lets `Waiting` process
// `eForward` the way `Active` does instead of dropping it -- is what
// confirms this recomputation genuinely catches a node taking an
// identity action while still waiting, not merely a model that happens
// to never exercise the gap.
//
// A direct `assert`, not `hot state`, matching this file's established
// convention for a per-event safety check on a bounded scenario -- see
// `NoAckedLoss`'s header above for why `hot state` liveness is the wrong
// shape here.
spec NoIdentityWhileWaiting observes eWaitingChanged, eClaimAdvertise, eTakeoverDrain {
  var waitingNodes: set[Node];

  start state Watching {
    on eWaitingChanged do (wc: (node: Node, waiting: bool)) {
      if (wc.waiting) {
        waitingNodes += (wc.node);
      } else {
        waitingNodes -= (wc.node);
      }
    }

    on eClaimAdvertise do (ca: (key: int, node: Node, role: tRole)) {
      assert !(ca.node in waitingNodes),
        "NoIdentityWhileWaiting violated: a node advertised a claim while still in the waiting-for-first-fence state";
    }

    on eTakeoverDrain do (td: (key: int, sq: int, newOwner: Node)) {
      assert !(td.newOwner in waitingNodes),
        "NoIdentityWhileWaiting violated: a node announced a takeover-drain while still in the waiting-for-first-fence state";
    }
  }
}

// GapFreedomCoverage: a coverage/liveness twin for GapFreedom (above),
// guarding the one degenerate case a pure safety assert cannot rule out by
// itself -- GapFreedom's own assert only ever fires `if (gd.accepted)`, so
// a `Node.p` that refused every `eForward` unconditionally (applying
// nothing, ever, an over-refusing implementation) would pass GapFreedom
// cleanly with zero real coverage: vacuously safe, not actually correct.
// #192's ACPR (finding 4): `TestGapFreedom`'s scenario asserted `GapFreedom`
// alone, with no other spec and no direct "something was actually
// accepted" check, so this blind spot was real, not hypothetical.
//
// Checked as a genuine `hot state` liveness monitor, the same P mechanism
// `NoAckedLossLive` (above) already establishes in this file for exactly
// this shape of gap -- P's own manual: "If the program terminates and the
// monitor is in a hot state, then there is a liveness bug."
// `TestGapFreedom`'s scenario is bounded and always terminates, so this
// monitor either sees a genuine accepted `eGapDecision` before the
// scenario ends, or the checker flags the termination-while-hot itself; no
// direct assert needed.
spec GapFreedomCoverage observes eGapDecision {
  start hot state NoneAcceptedYet {
    on eGapDecision do (gd: (receiver: Node, senderId: int, originSeq: int, accepted: bool)) {
      if (gd.accepted) {
        goto SomeAccepted;
      }
    }
  }

  state SomeAccepted {
    ignore eGapDecision;
  }
}
