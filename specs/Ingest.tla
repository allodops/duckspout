------------------------------ MODULE Ingest -------------------------------
(***************************************************************************)
(* The ingest module: owns Accept, DedupCheck, StageCommit, Throttle,      *)
(* Refuse, ClientAck, ClientTimeout; arms DurableAck, LadderMonotone,      *)
(* EveryRequestResolves (specs/README.md module map).  It instantiates the *)
(* full shared Next of DuckSpoutCore over a projected state space: the     *)
(* drain pipeline is off (DrainOn = FALSE) and the schema lattice is the   *)
(* trivial one (Columns = {}), so the reachable states are exactly the     *)
(* ingest + replication surface.                                           *)
(*                                                                         *)
(* Scope (3.1, justified against the hazards this module exists for):      *)
(*   2 nodes, RF = 2, 1 partition, 2 windows, 4 requests.                  *)
(* - RF = 2 with 2 nodes makes the RF receipt wait real: ClientAck's       *)
(*   evidence conjunct is load-bearing (AckBeforeReceipt perturbs it), and *)
(*   DedupCheck's pre-RF ELSE branch is reachable.  At RF = 1 both are     *)
(*   dead code -- see TRANSCRIPTION-NOTES TN-25 for why no v0.1 config     *)
(*   runs RF = 1.                                                          *)
(* - DKey(q1) = DKey(q2) (normative reachability pin, 3.1): otherwise both *)
(*   DedupCheck branches are dead code.                                    *)
(* - Ladder thresholds 1/2/3 over 4 requests make every rung reachable     *)
(*   (Witness_ThrottleAndRefuseTaken).                                     *)
(* - MaxCrashes = 1 admits the crash schedules ClientTimeout and           *)
(*   FenceBoot exist for; staged rows survive (A1), so NoAckedLoss is      *)
(*   checked across them.                                                  *)
(***************************************************************************)
EXTENDS DuckSpoutCore, TLC

CONSTANTS n1, n2, q1, q2, q3, q4, p1, d1, dkA, dkB, dkC, ckA

IngestWinOf      == (q1 :> 1) @@ (q2 :> 1) @@ (q3 :> 2) @@ (q4 :> 2)
IngestPartOf     == [q \in Requests |-> p1]
IngestDKeyOf     == (q1 :> dkA) @@ (q2 :> dkA) @@ (q3 :> dkB) @@ (q4 :> dkC)
IngestCKeyOf     == [q \in Requests |-> ckA]
IngestTombOf     == [q \in Requests |-> FALSE]
IngestKindOf     == [p \in Partitions |-> "event"]
IngestHome       == [d \in Datasets |-> p1]
IngestDsOf       == [q \in Requests |-> d1]
\* Arrival routing pinned (TN-8), like WinOf pins arrival timing: q1..q3
\* land on n1 -- q2's collision with q1 exercises BOTH DedupCheck branches
\* at the entry holder, and q1+q3 plus q4's replicated row drive M(n1)
\* through every rung with q2 as the rung-3 refusal target; q4 lands on n2,
\* the origin whose RF wait crosses the wire (Witness_ReceiptOutstandingAtAck).
IngestAcceptorOf == (q1 :> {n1}) @@ (q2 :> {n1}) @@ (q3 :> {n1}) @@ (q4 :> {n2})

Spec == Init /\ [][Next]_vars /\ CoreFairness

=============================================================================
