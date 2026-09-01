//! The closed node-status vocabulary (§4.5, §9.3.2).
//!
//! One type, three transports: the same [`NodeStatus`] value is reported
//! identically on the health endpoint, the metrics, and the registry — no
//! channel ever knows more than another (§9.5).

use serde::{Deserialize, Serialize};

/// The closed overload-ladder status enum (§4.5):
/// `normal | staging_pressure | drain_stalled | throttling | refusing_ingest`.
///
/// The rung is a pure function of `M = staged_bytes / hot.max_bytes` — no
/// hysteresis, no rung memory (`LadderMonotone`, §3). A closed enum is what
/// §3's properties and §8's chaos judge can assert over; free-text status is
/// unverifiable status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverloadStatus {
    /// M below the disclosure threshold; nothing to disclose.
    #[default]
    Normal,
    /// M ≥ 80% driven by sheer ingest rate, drains healthy (rung 1).
    StagingPressure,
    /// M ≥ 80% driven by a stalled drain — including catalog outage (rung 1).
    DrainStalled,
    /// M ≥ 95%: no new accepts; UNAVAILABLE + `RetryInfo` with growing delay
    /// (rung 2).
    Throttling,
    /// M ≥ 100%: refuse new writes and new-range replication (rung 3 — the
    /// top rung; nothing above it, ever).
    RefusingIngest,
}

/// The complete disclosed node status: the overload rung plus the orthogonal
/// `replication_degraded` flag (§9.3.2).
///
/// `replication_degraded` is deliberately **not** a sixth enum variant: it is
/// orthogonal to the ladder (§4.5 — a node can be `throttling` *and*
/// replication-degraded), so the honest single status type is this pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct NodeStatus {
    /// The overload-ladder rung's disclosed status.
    pub overload: OverloadStatus,
    /// True while the node holds ranges below the replication floor —
    /// availability preferred over placement, disclosed (§5).
    pub replication_degraded: bool,
}
