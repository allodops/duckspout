------------------------------ MODULE Schema -------------------------------
(***************************************************************************)
(* The schema module: owns EvolveSchema (plus PeerApply's fail-closed      *)
(* SchemaKnown guard) and arms lattice monotonicity and replay convergence *)
(* (specs/README.md module map).  It instantiates the full shared Next of  *)
(* DuckSpoutCore with the drain pipeline OFF and a genuine lattice.        *)
(*                                                                         *)
(* Scope (3.1, justified against the hazards this module exists for):      *)
(*   2 nodes, RF = 2, 2 partitions, 1 dataset, Columns = {c1, c2}          *)
(*   (LatticeElem = SUBSET {c1, c2}: a 3-chain plus its join sibling), 1   *)
(*   data request -- the schema records are this module's real traffic.    *)
(* - TWO partitions with the datasets homed on p1 while data rides p2:     *)
(*   gap refusal orders schema-before-data only within one (partition,     *)
(*   origin) log, so only a cross-partition arrival makes SchemaKnown's    *)
(*   fail-closed clause do work a seq comparison would not (TN-28).        *)
(* - Both nodes may evolve d1 concurrently, so join commutativity and      *)
(*   replay convergence are checked across concurrent widenings.           *)
(* - RF = 2 keeps acks honest while forwards carry sAt in-band; a          *)
(*   receiver behind on p1 genuinely "does not know a column"             *)
(*   (Witness_SchemaWidensInFlight).                                       *)
(* - Ladder thresholds sit above reach (9): the ladder is Ingest.tla's     *)
(*   armed hazard.  MaxCrashes = 1 admits replay-after-crash schedules.    *)
(***************************************************************************)
EXTENDS DuckSpoutCore, TLC

CONSTANTS n1, n2, q1, p1, p2, d1, c1, c2, dkA, ckA

SchemaWinOf      == [q \in Requests |-> 1]
SchemaPartOf     == [q \in Requests |-> p2]
SchemaDKeyOf     == (q1 :> dkA)
SchemaCKeyOf     == [q \in Requests |-> ckA]
SchemaTombOf     == [q \in Requests |-> FALSE]
SchemaKindOf     == [p \in Partitions |-> "event"]
SchemaHome       == [d \in Datasets |-> p1]
SchemaDsOf       == [q \in Requests |-> d1]
SchemaAcceptorOf == (q1 :> {n1})   \* TN-8

Spec == Init /\ [][Next]_vars /\ CoreFairness

=============================================================================
