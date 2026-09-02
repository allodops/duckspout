------------------------------ MODULE Drain --------------------------------
(***************************************************************************)
(* The drain module: owns SealPart, PutPart, the LakeCommit family,        *)
(* Reconcile, Demote, Evict, DropWindow, SnapshotSeal, Expire, DeclareLoss *)
(* and arms WatermarkHonesty, SingleDrainCommit, CacheTransparency,        *)
(* SnapshotCovered, LossLedgerTruthful, LatestViewCorrect,                 *)
(* WatermarkEventuallyAdvances (specs/README.md module map).  It           *)
(* instantiates the full shared Next of DuckSpoutCore with the drain       *)
(* pipeline ON; the schema lattice is trivial (Columns = {}).              *)
(*                                                                         *)
(* Clean scope (3.1, justified against the hazards it must represent):     *)
(*   3 nodes, RF = 2, 1 event-class partition, ONE window, ONE request --  *)
(*   the largest full-pipeline scope TLC exhausts WITH the liveness        *)
(*   properties.  Measured (TN-35): every 2-origin full-pipeline scope     *)
(*   explodes into the millions of states through the cross product of    *)
(*   seal-time part variants, the four-way commit outcomes and the         *)
(*   replication lattice, so the hazards that need more requests are armed *)
(*   at their own scopes by the broken/ configurations -- which is 3.1's   *)
(*   own pin structure: "divergent coverage between racing drains" is      *)
(*   DoubleDrain's (and SupplementOverlap's) two-origin configuration,     *)
(*   drain-time dedup is DemoteDirty's colliding-key configuration, and    *)
(*   the changelog/snapshot machinery is DrainSnapshot.cfg's.              *)
(*                                                                         *)
(* Why THIS shape still carries the module's core hazards:                 *)
(* - THREE nodes, because the sealed extent only does work when a receipt  *)
(*   can attest a key the SEALER does not hold: q1's record (origin n1)    *)
(*   can be applied and receipted by n3 while n2 -- which never applied it *)
(*   -- seals and commits a winner part; the winner's extent then contains *)
(*   a receipted key it does not cover, the watermark blocks (impossible   *)
(*   at 2 nodes, TN-27), and WatermarkEventuallyAdvances rests on the      *)
(*   fairness chain CloseWindow -> SealSupplement -> PutPart ->            *)
(*   LakeCommitOk(SF) completing the extent -- the load-bearing liveness   *)
(*   story of the supplement path (the v0.1 analog of TakeoverDrain's      *)
(*   load-bearing fairness, 3.5).                                          *)
(* - Racing drains exist even here (holder vs empty-cov candidates from    *)
(*   three claim holders); the UNIQUE fence, the supplement path, the      *)
(*   three-way commit outcome (IndetOn = {n1}, TN-3) and post-drain        *)
(*   residency (Demote/Evict/DropWindow) are all reachable.                *)
(* - Ladder thresholds sit above reach (5): the ladder is Ingest.tla's     *)
(*   armed hazard; here every rung-0 step keeps the drain surface small.   *)
(* - MaxCrashes = 0 here (TN-3): crash schedules multiply the space ~18x   *)
(*   and Ingest owns them; the commit-vs-demote crash window is armed by   *)
(*   Witness_CrashBetweenCommitAndDemote.cfg, which overrides it to 1.     *)
(*                                                                         *)
(* q2 and q4 are declared here (and stay bound in every configuration)     *)
(* because the broken/ scopes use them: q2 collides with q1 on DKey        *)
(* (DemoteDirty), q4 is a third-origin request (DoubleDrain,               *)
(* SupplementOverlap, the loss witnesses).                                 *)
(***************************************************************************)
EXTENDS DuckSpoutCore, TLC

CONSTANTS n1, n2, n3, q1, q2, q4, p1, d1, dkA, dkC, ckA

DrainWinOf      == [q \in Requests |-> 1]
DrainPartOf     == [q \in Requests |-> p1]
DrainDKeyOf     == (q1 :> dkA) @@ (q2 :> dkA) @@ (q4 :> dkC)
DrainCKeyOf     == [q \in Requests |-> ckA]   \* changelog semantics are
DrainTombOf     == [q \in Requests |-> FALSE] \* DrainSnapshot.cfg's scope
DrainKindOf     == [p \in Partitions |-> "event"]
DrainHome       == [d \in Datasets |-> p1]
DrainDsOf       == [q \in Requests |-> d1]
\* Arrival routing pinned (TN-8): one request per origin -- the shape in
\* which racing drains diverge and residues split across replicas.
DrainAcceptorOf == (q1 :> {n1}) @@ (q2 :> {n2}) @@ (q4 :> {n3})

DrainInitClaims == {<<n1, p1>>, <<n2, p1>>}   \* advisory rows pre-seeded
  \* (TN-4): n1 (the origin) and n2 (which need not have applied q1's
  \* record) race as drainers; n3 is the pure receipting replica whose
  \* receipt puts k1 into the sealed extent (TN-27).  A third drainer
  \* multiplies the commit pipeline without adding a hazard this scope
  \* arms (the broken/ scopes race their own claim sets).

Spec == Init /\ [][Next]_vars /\ CoreFairness

\* Finding_WatermarkThroughCatalogOutage runs this spec: the same behaviors
\* WITHOUT the catalog-recovers fairness assumption (no SF on LakeCommitOk).
SpecCatalogOutage == Init /\ [][Next]_vars /\ FairnessBase

=============================================================================
