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
