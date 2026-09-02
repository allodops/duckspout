-------------------- MODULE Witness_SchemaWidensInFlight --------------------
\* Non-vacuity witness (3.6, armed): an EvolveSchema lands mid-flight and a peer applies widen-before-data.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Constants identical to the clean Schema.cfg.
EXTENDS Schema
=============================================================================
