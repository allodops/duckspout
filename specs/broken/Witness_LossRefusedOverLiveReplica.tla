----------------- MODULE Witness_LossRefusedOverLiveReplica -----------------
\* Non-vacuity witness (3.6, armed): a DeclareLoss is refused because a live replica still covers the range.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Overrides vs the clean Drain.cfg: Requests   = {q1}; -- the smallest scope the
\* witness needs; constants otherwise identical.
EXTENDS Drain
=============================================================================
