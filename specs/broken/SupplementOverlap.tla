-------------------------- MODULE SupplementOverlap --------------------------
\* Broken variant (3.6, armed): the supplement path skips the disjoint-coverage proof
\* (BrkSupplementOverlap = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Scope: Requests = {q1, q4} -- the smallest request set reaching this
\* counterexample; constants otherwise identical to the clean
\* Drain.cfg.
EXTENDS Drain
=============================================================================
