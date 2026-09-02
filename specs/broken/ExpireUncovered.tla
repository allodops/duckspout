--------------------------- MODULE ExpireUncovered ---------------------------
\* Broken variant (3.6, armed): Expire drops the covering-snapshot conjunct
\* (BrkExpireUncovered = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Scope: Requests = {q1} -- the smallest request set reaching this
\* counterexample; constants otherwise identical to the clean
\* DrainSnapshot.cfg.
EXTENDS DrainSnapshot
=============================================================================
