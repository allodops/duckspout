------------------- MODULE Finding_BoundedThrottleDuration -------------------
\* FINDING (3.5, permanently red on purpose): this run MUST fail.
\* No upper bound exists on how long a client is throttled while staging
\* is full and drains are stalled (this module IS drains-stalled).
\* Constants otherwise identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
