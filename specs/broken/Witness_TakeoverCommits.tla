------------------------ MODULE Witness_TakeoverCommits ---------------------
(***************************************************************************)
(* STAGED to v0.2 (ledger row tla-mc-replication, issue #57): the witness  *)
(* that a TakeoverDrain actually lands a dead owner's window in the lake.  *)
(* TakeoverDrain (heartbeat staleness, suppression, advertised-coverage    *)
(* election) is Replication.tla's machinery; v0.1's drain module has no    *)
(* owner/takeover distinction to witness.  No .cfg exists yet, so the      *)
(* runner's must-fail sweep does not pick this file up; the .cfg lands     *)
(* with Replication.tla and arms the witness.                              *)
(***************************************************************************)
EXTENDS DuckSpoutCore
=============================================================================
