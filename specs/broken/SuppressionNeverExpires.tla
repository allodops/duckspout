------------------------ MODULE SuppressionNeverExpires ---------------------
(***************************************************************************)
(* STAGED to v0.2 (ledger row tla-mc-replication, issue #57): this variant *)
(* pins SuppressionExpired FALSE -- takeover never fires for a             *)
(* "restarting" node that never returns (5.10) -- and                      *)
(* WatermarkEventuallyAdvances must catch it.  TakeoverDrain and the       *)
(* suppression window are Replication.tla's actions; v0.1 has no takeover  *)
(* to suppress.  No .cfg exists yet, so the runner's must-fail sweep does  *)
(* not pick this file up; the .cfg lands with Replication.tla and arms     *)
(* the variant.                                                            *)
(***************************************************************************)
EXTENDS DuckSpoutCore
=============================================================================
