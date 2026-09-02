------------------------ MODULE Witness_LossDeclared ------------------------
\* Non-vacuity witness (3.6, armed): with the budget raised past RF - 1, DeclareLoss fires end-to-end.
\* This configuration asserts the witness state/step is UNREACHABLE
\* and MUST fail -- the counterexample TLC prints IS the witness.
\* Overrides vs the clean Drain.cfg: WipeBudget = 2; Requests   = {q1}; -- the smallest scope the
\* witness needs; constants otherwise identical.
EXTENDS Drain
=============================================================================
