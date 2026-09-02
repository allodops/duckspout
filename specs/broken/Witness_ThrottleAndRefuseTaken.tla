------------------- MODULE Witness_ThrottleAndRefuseTaken -------------------
\* Non-vacuity witness (3.6, armed): Throttle and Refuse are each actually taken.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Constants identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
