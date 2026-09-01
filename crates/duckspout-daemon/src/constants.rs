//! Fixed constants (§9.6.3) — not configurable, each with a stated
//! derivation. A constant graduates to a knob only through the §9.6.4
//! ratchet (divergent-workload justification, golden-manifest diff).

// Justification for the allow: §9.6.3 is transcribed complete at bootstrap
// (SEED s§4); consumers arrive with the wiring (v0.1), and dropping unread
// constants would un-transcribe the table.
#![allow(dead_code)]

/// Overload ladder: disclose at 80% of `hot.max_bytes` of **staged** bytes
/// (§4.5 rung 1 — also the only capacity alert, §9.2).
pub const LADDER_DISCLOSE_FRACTION: f64 = 0.80;

/// Overload ladder: throttle at 95% (§4.5 rung 2).
pub const LADDER_THROTTLE_FRACTION: f64 = 0.95;

/// Overload ladder: refuse at 100% — `hot.max_bytes` itself (§4.5 rung 3,
/// the top rung).
pub const LADDER_REFUSE_FRACTION: f64 = 1.00;

/// Heartbeat cadence, seconds (§5.6).
pub const HEARTBEAT_CADENCE_SECS: u64 = 5;

/// Heartbeat TTL = 3× cadence: one missed beat is jitter, three is death
/// (§5.6, §9.6.3).
pub const HEARTBEAT_TTL_SECS: u64 = 3 * HEARTBEAT_CADENCE_SECS;

/// Changelog snapshot rollover trigger: dirty ratio 1.0 — at most 2× space
/// amplification (§6.7).
pub const SNAPSHOT_DIRTY_RATIO: f64 = 1.0;

/// Background-eviction low-water: 5% free (dormant until the cache class is
/// live, §4.5 rung 0).
pub const BACKGROUND_EVICTION_LOW_WATER_FREE_FRACTION: f64 = 0.05;

/// SLRU protected share, percent (probationary is the remainder). Start
/// 80:20, bench-validated before it ships — parked with the cache class
/// (§12.7); listed for the design-of-record.
pub const SLRU_PROTECTED_PERCENT: u32 = 80;

/// Clock-skew epsilon, milliseconds: bounds the heartbeat-staleness /
/// event-time-lateness *skew warning* only — no invariant reads a clock
/// (§3's model has no clock variable).
pub const CLOCK_SKEW_EPSILON_MS: u64 = 500;

/// Takeover suppression, derived: this multiple of the termination grace
/// period (§9.1.2).
pub const TAKEOVER_SUPPRESSION_GRACE_MULTIPLIER: u32 = 2;

/// Outstanding queries per principal: 4× `query.max_concurrent_hot_scans`'
/// default, so one dashboard's fan-out queues rather than starves.
pub const OUTSTANDING_QUERIES_PER_PRINCIPAL: u32 = 32;

/// Shard sanity ceiling — config validation, not a tunable: beyond 64 lies a
/// topology decision, not a knob (§2.2, §9.6.3).
pub const SHARD_SANITY_CEILING: u32 = 64;

/// Retention-class set cap: parts are class-pure (§2.7) and surveyed fleet
/// horizons cluster well under eight distinct values.
pub const RETENTION_CLASS_CAP: u32 = 8;

/// The system tenants' built-in short retention class, hours (§9.3.1).
pub const SYSTEM_TENANT_RETENTION_HOURS: u64 = 72;
