------------------------ MODULE Witness_ReclaimSealFires --------------------
(***************************************************************************)
(* Non-vacuity witness (3.6, armed): ReclaimSeal (v0.2, #177) actually      *)
(* fires -- a recovering node's own orphaned seal (sealed and PUT before it *)
(* crashed, never committed) genuinely gets re-registered under its new     *)
(* incarnation, not just declared as an action with no reachable            *)
(* transition. This configuration asserts the witness step is UNREACHABLE  *)
(* (NoWitness_ReclaimSealFires) and MUST fail -- the counterexample TLC     *)
(* prints IS the witness: it should mirror                                 *)
(* specs/broken/Finding_TakeoverOrphanedSeal's trace up through FenceBoot,  *)
(* then take the ReclaimSeal step that trace never had (ReclaimOn = FALSE   *)
(* there, TRUE here). Constants identical to the clean Replication.cfg --   *)
(* the smallest scope in which a node's own orphaned seal is reachable.     *)
(***************************************************************************)
EXTENDS Replication
=============================================================================
