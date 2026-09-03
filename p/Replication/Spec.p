// NoAckedLoss (P half, minimal slice): every key a node has durably
// STAGED (accepted for replication -- eAccepted, not the client's raw
// request, which may never even be accepted if its target node dies
// first) must be drained (committed) via takeover by the time the
// scenario's last relevant event for that key has been handled -- this
// scenario's owner always dies before any normal-path commit exists, so
// takeover is the only route to durability; a key that never gets there
// is lost forever.
//
// Checked as a direct `assert`, not a `hot state`: an earlier attempt
// (#132 finding) modeled this as `hot state` liveness matching the P
// tutorial's `GuaranteedWithDrawProgress` pattern, and it did not fire
// even for a deliberately-broken variant (0 bugs, 1000 schedules). The
// tutorial's own regression example for that pattern
// (`Liveness_1_WarmState`) drives an infinite self-loop -- hot-state
// checking there is about a cycle that never leaves the hot state, not
// about a bounded scenario reaching quiescence while hot. This scenario
// is bounded (one write, one crash, done) and P's default bugfinding
// checker does not flag "terminated while hot" as a violation, so
// liveness-style checking is the wrong shape here.
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
// Mirrors DuckSpoutCore.tla's NoAckedLoss/GapFreedom in spirit, not text
// -- this is a hand-written P analog, not a transliteration.
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
