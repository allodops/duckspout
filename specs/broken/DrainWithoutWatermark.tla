------------------------ MODULE DrainWithoutWatermark ------------------------
\* Broken variant (3.6, armed): LakeCommitOk no longer advances wm; a freestanding advance exists
\* (BrkDrainWithoutWatermark = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Scope: Requests = {q1} -- the smallest request set reaching this
\* counterexample; constants otherwise identical to the clean
\* Drain.cfg.
EXTENDS Drain
=============================================================================
