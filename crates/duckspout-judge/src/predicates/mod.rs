//! Judge predicates built on `crate::journal` (§8.4). #205 ships one:
//! [`zero_acked_lost`]. `docs/verification.md` §8.4 names four more
//! (watermark honesty, per-key order/latest-view correctness, retention
//! honesty, cache transparency under eviction storms) — #206/#207/#208's
//! territory, not implemented here.

pub mod zero_acked_lost;
