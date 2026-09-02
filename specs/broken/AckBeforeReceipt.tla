-------------------------- MODULE AckBeforeReceipt --------------------------
\* Broken variant (3.6, armed): ClientAck drops the >= RF receipt conjunct
\* (BrkAckBeforeReceipt = TRUE).  MUST fail on every run; the
\* perturbation is this configuration -- the .tla adds nothing.
\* Constants otherwise identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
