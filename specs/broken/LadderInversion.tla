--------------------------- MODULE LadderInversion ---------------------------
\* Broken variant (3.6, armed): Accept's rung guard re-permits admission at rung >= 2
\* (BrkLadderInversion = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Constants otherwise identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
