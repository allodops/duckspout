---------------------- MODULE Finding_BoundedAckLatency ----------------------
\* FINDING (3.5, permanently red on purpose): this run MUST fail.
\* DuckSpout sets no ack-latency bound under contention: throttle is the
\* pressure valve, not a deadline.
\* Constants otherwise identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
