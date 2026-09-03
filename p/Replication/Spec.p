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
// Mirrors DuckSpoutCore.tla's NoAckedLoss/GapFreedom in spirit, not text
// -- this is a hand-written P analog, not a transliteration.
spec NoAckedLoss observes eAccepted, eTakeoverDrain, eForwardHandled, eCrashSignalHandled {
  var pending: set[int];
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
      pending += (acc.key);
    }

    on eTakeoverDrain do (td: (key: int, sq: int, newOwner: Node)) {
      pending -= (td.key);
    }

    on eForwardHandled do (fh: (key: int, sq: int)) {
      if (crashHandled)
        assert !(fh.key in pending),
          "NoAckedLoss violated: an accepted key was never drained";
      else
        forwardedBeforeCrash += (fh.key);
    }

    on eCrashSignalHandled do {
      var k: int;
      crashHandled = true;
      foreach (k in forwardedBeforeCrash) {
        assert !(k in pending),
          "NoAckedLoss violated: an accepted key was never drained";
      }
    }
  }
}
