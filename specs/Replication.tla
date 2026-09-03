--------------------------- MODULE Replication ------------------------------
(***************************************************************************)
(* The replication module: owns TakeoverDrain (#55, ADR-0012 step 1) -- a  *)
(* live node acquiring an orphaned partition's claim so it can drain the   *)
(* dead owner's undrained windows from its own replicated copy             *)
(* (docs/design/replication.md 5.6, "Node death, end to end"). It          *)
(* instantiates the full shared Next of DuckSpoutCore with both the drain  *)
(* pipeline and takeover dynamics ON.                                      *)
(*                                                                         *)
(* Clean scope (3.1, justified against the hazard it must represent):      *)
(*   2 nodes, RF = 2, 1 event-class partition, ONE window, ONE request,    *)
(*   MaxCrashes = 1, Crashable = {n1} -- the smallest scope in which        *)
(*   takeover is not vacuous: with RF = 2 and exactly 2 nodes, the         *)
(*   surviving node is BY CONSTRUCTION always in the dead owner's replica  *)
(*   set, so a successful takeover-drain requires nothing this scope       *)
(*   doesn't already exercise -- no third node, no colliding keys, no      *)
(*   ladder pressure. q1 originates at n1 (AcceptorOf), which InitClaims   *)
(*   also pre-seeds as p1's owner, so the reachable story matches 5.6's    *)
(*   narrative directly: n1 accepts and stages q1, replicates it to n2     *)
(*   (Forward/PeerApply/Receipt), crashes, n2 observes no live claimant    *)
(*   and takes over (TakeoverDrain), then seals, puts, and commits the     *)
(*   window from its own staged[n2] copy -- SealPart already gates on      *)
(*   HoldsClaim(n, p) and reads WindowRecs(n, p, w) := staged[n], both     *)
(*   already correct for whoever holds the claim, takeover or not.        *)
(*                                                                         *)
(* Why THIS shape still carries the module's core hazard:                  *)
(* - The negative case matters as much as the positive one: if n1's crash  *)
(*   happens BEFORE Receipt (n2 never applied q1's record), n2 has         *)
(*   nothing to seal even after taking over the claim -- the window        *)
(*   simply never closes/commits on this branch, which is correct         *)
(*   (NoAckedLoss only binds what was ACKED, and DurableAck requires RF    *)
(*   receipts before ack -- so an unacked record dying with its only       *)
(*   holder is not a violation). Both interleavings are reachable in this  *)
(*   scope; TLC explores both without needing a second crash or a third    *)
(*   node to force them apart.                                            *)
(* - FenceBoot's recovery path is reachable too (MaxCrashes = 1 bounds     *)
(*   the crash count, not recovery): n1 can FenceBoot after n2's takeover  *)
(*   and must be fenced out of re-claiming or re-committing anything --    *)
(*   FencedZombie is checked here for real, not vacuously, because the     *)
(*   crash that makes it interesting (a claim transferred out from under   *)
(*   the recovering node) is exactly this scope's story.                  *)
(* - IndetOn = {} here: the indeterminate-commit branch during a takeover  *)
(*   drain is a real, separate interleaving (n2's own catalog connection   *)
(*   dying mid-commit after taking over) but is not THIS module's hazard   *)
(*   -- Drain.tla already proves the three-way commit outcome sound in     *)
(*   general; arming it here too would only multiply the state space       *)
(*   without adding a takeover-specific finding.                          *)
(* - Ladder thresholds sit above reach (5): overload is Ingest.tla's       *)
(*   armed hazard, orthogonal to who holds a claim.                       *)
(* - ReclaimOn = TRUE (v0.2, #177): the SAME node recovering from its own  *)
(*   crash, after it had already sealed+PUT a window part but before it    *)
(*   ever committed it, is this scope's OTHER reachable story (distinct    *)
(*   from TakeoverDrain's different-node case above) -- n1's incarnation   *)
(*   bumps on FenceBoot, permanently fencing its own commit of that object  *)
(*   under CommitGuardsHold unless it reclaims the entry first             *)
(*   (DuckSpoutCore's ReclaimSeal). WatermarkEventuallyAdvances is checked  *)
(*   here for real now; the original gap (no reclaim available) stays      *)
(*   permanently red at specs/broken/Finding_TakeoverOrphanedSeal.cfg.     *)
(***************************************************************************)
EXTENDS DuckSpoutCore, TLC

CONSTANTS n1, n2, q1, p1, d1, dkA

ReplicationWinOf      == [q \in Requests |-> 1]
ReplicationPartOf     == [q \in Requests |-> p1]
ReplicationDKeyOf     == (q1 :> dkA)
ReplicationCKeyOf     == [q \in Requests |-> dkA]
ReplicationTombOf     == [q \in Requests |-> FALSE]
ReplicationKindOf     == [p \in Partitions |-> "event"]
ReplicationHome       == [d \in Datasets |-> p1]
ReplicationDsOf       == [q \in Requests |-> d1]
ReplicationAcceptorOf == (q1 :> {n1})   \* q1 originates at n1: the node
                                        \* whose death is this scope's story

ReplicationInitClaims == {<<n1, p1>>}   \* n1 pre-seeded as p1's owner
  \* (TN-4), matching 5.6's narrative: an established owner dies and a
  \* live replica takes over, rather than a partition nobody ever claimed.

Spec == Init /\ [][Next]_vars /\ CoreFairness

=============================================================================
