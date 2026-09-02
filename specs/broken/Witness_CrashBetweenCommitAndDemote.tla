----------------- MODULE Witness_CrashBetweenCommitAndDemote -----------------
\* Non-vacuity witness (3.6, armed): the crash window between LakeCommitOk and Demote, recovered through.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Overrides vs the clean Drain.cfg: MaxCrashes = 1; Requests   = {q1}; -- the smallest scope the
\* witness needs; constants otherwise identical.
EXTENDS Drain
=============================================================================
