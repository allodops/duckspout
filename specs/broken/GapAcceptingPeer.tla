-------------------------- MODULE GapAcceptingPeer --------------------------
(***************************************************************************)
(* STAGED to v0.2 (ledger row tla-mc-replication, issue #57): this broken  *)
(* variant drops PeerApply's contiguity conjunct, and GapFreedom must      *)
(* catch it.  Gap refusal is Replication.tla's armed hazard: the           *)
(* counterexample needs the replication module's scope (message loss over  *)
(* competing incarnations, takeover from a partial prefix).  No .cfg       *)
(* exists yet, so the runner's must-fail sweep does not pick this file up; *)
(* the .cfg lands with Replication.tla and arms the variant.               *)
(***************************************************************************)
EXTENDS DuckSpoutCore
=============================================================================
