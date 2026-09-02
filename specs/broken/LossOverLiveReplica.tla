------------------------- MODULE LossOverLiveReplica ------------------------
(***************************************************************************)
(* STAGED to v0.2 (ledger row tla-mc-replication, issue #57): this broken  *)
(* variant drops DeclareLoss's no-live-coverage guard, and                 *)
(* LossLedgerTruthful must catch it (a live replica's coverage falsely     *)
(* confessed away).  The loss ceremony only fires inside the wipe-budget   *)
(* fault schedules Replication.tla arms (WipeBudget = RF - 1); the v0.1    *)
(* clean configurations run WipeBudget = 0, where DeclareLoss is           *)
(* unreachable outside its dedicated witness.  No .cfg exists yet, so the  *)
(* runner's must-fail sweep does not pick this file up; the .cfg lands     *)
(* with Replication.tla and arms the variant.                              *)
(***************************************************************************)
EXTENDS DuckSpoutCore
=============================================================================
