--------------------------- MODULE UnfencedZombie ---------------------------
(***************************************************************************)
(* STAGED to v0.2 (ledger row tla-mc-replication, issue #57): this broken  *)
(* variant perturbs PeerApply/LakeCommitOk to accept an incarnation below  *)
(* the acceptor's fence, and FencedZombie must catch it.  The fence only   *)
(* has work to do under the fault machinery Replication.tla arms --        *)
(* crash-wipe schedules, competing incarnations, TakeoverDrain -- none of  *)
(* which the v0.1 configurations explore (WipeBudget = 0, staleApplied     *)
(* stays empty by construction).  No .cfg exists yet, so the runner's      *)
(* must-fail sweep does not pick this file up; the .cfg lands with         *)
(* Replication.tla and arms the variant.                                   *)
(***************************************************************************)
EXTENDS DuckSpoutCore
=============================================================================
