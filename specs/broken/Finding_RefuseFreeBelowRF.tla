---------------------- MODULE Finding_RefuseFreeBelowRF ----------------------
\* FINDING (3.5, permanently red on purpose): this run MUST fail.
\* Below the replication floor, ingest does not eventually accept:
\* refuse-only is the design (5.1).
\* Constants otherwise identical to the clean Ingest.cfg.
EXTENDS Ingest
=============================================================================
