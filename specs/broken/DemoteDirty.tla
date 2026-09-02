----------------------------- MODULE DemoteDirty -----------------------------
\* Broken variant (3.6, armed): Demote drops dedupRemoved = 0
\* (BrkDemoteDirty = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Scope: Requests = {q1, q2} -- the smallest request set reaching this
\* counterexample; constants otherwise identical to the clean
\* Drain.cfg.
EXTENDS Drain
=============================================================================
