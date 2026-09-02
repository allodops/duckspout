------------------- MODULE Witness_ReceiptOutstandingAtAck -------------------
\* Non-vacuity witness (3.6, armed): a Forward's Receipt is outstanding at ClientAck-decision time.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Constants identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
