-------------------------- MODULE WatermarkPastHole --------------------------
\* Broken variant (3.6, armed): NewWatermark may pass an uncovered range with no lossLedger row
\* (BrkWatermarkPastHole = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Scope: Requests = {q1} -- the smallest request set reaching this
\* counterexample; constants otherwise identical to the clean
\* Drain.cfg.
EXTENDS Drain
=============================================================================
