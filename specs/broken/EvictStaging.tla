---------------------------- MODULE EvictStaging ----------------------------
\* Broken variant (3.6, armed): Evict enabled on staging-class tables
\* (BrkEvictStaging = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Scope: Requests = {q1} -- the smallest request set reaching this
\* counterexample; constants otherwise identical to the clean
\* Drain.cfg.
EXTENDS Drain
=============================================================================
