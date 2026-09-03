------------------- MODULE Finding_TakeoverOrphanedSeal ---------------------
\* FINDING (3.5, permanently red on purpose): this run MUST fail.
\* A node that seals and PUTs a window part, then crashes before
\* committing it, then recovers (FenceBoot), can never have that object
\* committed by anyone: CommitGuardsHold requires pt.inc = inc[pt.sealer],
\* and the sealer's own incarnation bumped on recovery; only the sealer
\* may commit its own object (LakeCommitOk requires pt.sealer = n); and no
\* other node can seal a replacement without first taking the claim, which
\* requires the sealer to have no live holder for long enough -- not
\* guaranteed if it recovers before any other node observes it as dead.
\* Reconcile does not help: it only resolves the Indeterminate-commit
\* case (pendingCommit # None), and this crash happens before any commit
\* attempt is ever made. Constants otherwise identical to the clean
\* Replication.cfg, EXCEPT its .cfg pins ReclaimOn = FALSE: the fix
\* (DuckSpoutCore's ReclaimSeal, armed by ReclaimOn = TRUE in the clean
\* Replication.cfg) closes exactly this gap, so this module keeps the
\* mechanism off on purpose to keep demonstrating the ORIGINAL bug shape.
EXTENDS Replication
=============================================================================
