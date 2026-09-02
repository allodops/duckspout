---------------------- MODULE Finding_PerOriginFairness ----------------------
\* FINDING (3.5, permanently red on purpose): this run MUST fail.
\* No cross-origin fairness is promised in v1: a throttled client may be
\* throttled indefinitely while others progress.
\* Constants otherwise identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
