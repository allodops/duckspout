---------------- MODULE Finding_WatermarkThroughCatalogOutage ----------------
\* FINDING (3.5, permanently red on purpose): this run MUST fail.
\* The watermark does not advance while the catalog is down:
\* WatermarkEventuallyAdvances is checked WITHOUT the catalog-recovers
\* fairness assumption (SpecCatalogOutage: no SF on LakeCommitOk) and
\* must fail. Drains pause and say so (9); no timer escalates an outage
\* into data movement.
\* Constants otherwise identical to the clean Drain.cfg.
EXTENDS Drain
=============================================================================
