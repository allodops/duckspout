//! Judge predicates built on `crate::journal` (§8.4). #205 shipped
//! [`zero_acked_lost`]; #206 adds [`watermark_honesty`] (the Q-shaped judge)
//! and [`latest_view`] (the §3 invariant `LatestViewCorrect`, judged
//! end-to-end). `docs/verification.md` §8.4 names two more — retention
//! honesty and cache transparency under eviction storms — which are #207's
//! and #208's territory, not implemented here.
//!
//! Every predicate is a pure function of its evidence returning
//! `crate::verdict::Verdict` over its own typed findings, so
//! `crate::runner` can run them all and combine their verdicts under one
//! exit contract.

pub mod latest_view;
pub mod watermark_honesty;
pub mod zero_acked_lost;

/// The reserved system-tenant prefix (§2.2): `_self`/`_canary`, and any
/// future `_`-prefixed system tenant.
///
/// Shared by the predicates that exclude system-class datasets by
/// definition — they receive no durable acks, so there is nothing to lose
/// and nothing a read owes them — rather than each spelling the convention
/// out for itself.
pub(crate) const SYSTEM_TENANT_PREFIX: char = '_';
