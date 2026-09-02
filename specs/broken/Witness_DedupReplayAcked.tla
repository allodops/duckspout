---------------------- MODULE Witness_DedupReplayAcked ----------------------
\* Non-vacuity witness (3.6, armed): a colliding retry replays the original's ack through DedupCheck.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Constants identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
