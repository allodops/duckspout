--------------------------- MODULE DrainSnapshot ---------------------------
(***************************************************************************)
(* Drain.tla's changelog scope: the snapshot machinery -- SnapshotSeal,    *)
(* Expire's covering-snapshot guard, SnapshotCovered, LatestViewCorrect -- *)
(* at the scope its hazards need:                                          *)
(*                                                                         *)
(*   2 nodes, RF = 2, 1 changelog partition, ONE window, 2 requests        *)
(*   sharing a client key, the second a tombstone, a single drainer.       *)
(*                                                                         *)
(* Why this shape: snapshot honesty is about the FOLD, not about racing    *)
(* drains -- two origins updating one client key, with a tombstone, make   *)
(* LatestViewCorrect's overlay genuinely delete and genuinely re-fold      *)
(* (TN-34's straggler case reaches here: n2 can seal a snapshot whose      *)
(* as-of has seen its own tombstone but not n1's earlier-ordered upsert).  *)
(*                                                                         *)
(* NO CLEAN .cfg SHIPS AT v0.1 (TN-35): even at this minimal shape the     *)
(* exhaustive space measured past 2.5 million states with TLC's queue      *)
(* still growing -- beyond the per-PR bounded-tier budget (8.1) -- so the  *)
(* exhaustive changelog configuration is deferred to the nightly           *)
(* simulation tier (ledger row tla-sim, issue #48), recorded as issue #41  *)
(* remainder.  This module is live in the must-fail suite today:           *)
(* broken/ExpireUncovered.cfg extends it (SnapshotCovered's armed          *)
(* variant), and TLC's partial exhaustive sweeps of this scope (2.5M       *)
(* states, no violation) are what surfaced and then validated the TN-34    *)
(* and TN-36 findings.                                                     *)
(***************************************************************************)
EXTENDS DuckSpoutCore, TLC

CONSTANTS n1, n2, q1, q4, p1, d1, dkA, dkC, ckA

DSWinOf      == [q \in Requests |-> 1]
DSPartOf     == [q \in Requests |-> p1]
DSDKeyOf     == (q1 :> dkA) @@ (q4 :> dkC)
DSCKeyOf     == [q \in Requests |-> ckA]   \* one client key: folds contend
DSTombOf     == (q1 :> FALSE) @@ (q4 :> TRUE)   \* the tombstone deletes it
DSKindOf     == [p \in Partitions |-> "changelog"]
DSHome       == [d \in Datasets |-> p1]
DSDsOf       == [q \in Requests |-> d1]
DSAcceptorOf == (q1 :> {n1}) @@ (q4 :> {n2})   \* TN-8: two origins
DSInitClaims == {<<n2, p1>>}   \* TN-4: only the replica drains -- snapshot
  \* honesty is about the FOLD and the covering-expiry guard, not racing
  \* drains (Drain.cfg and the broken/ scopes own the races); a single
  \* drainer keeps this space exhaustible while n2, applying n1's log,
  \* still seals every snapshot as-of the straggler case needs

Spec == Init /\ [][Next]_vars /\ CoreFairness

=============================================================================
