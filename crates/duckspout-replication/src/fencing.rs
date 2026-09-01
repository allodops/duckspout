//! Incarnation fencing and the ring protocol (§5). Ⓢ v0.2 stubs.
//!
//! A node boots with a persisted, monotonically increasing incarnation;
//! `FenceBoot` (§5) rejects any pre-crash zombie carrying a stale one. The
//! forward/receipt ring, gap refusal, takeover, and `DeclareLoss` land at
//! v0.2 with `Replication.tla`.

use serde::{Deserialize, Serialize};

use duckspout_types::NodeId;

/// A node's boot incarnation (§5): persisted locally, advanced on every
/// `FenceBoot`, compared to fence stale writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Incarnation(pub u64);

impl Incarnation {
    /// The next incarnation, taken at `FenceBoot`.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A fencing identity: which incarnation of which node performed a write.
/// Ⓢ v0.2 — comparison and rejection rules land with the protocol.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FenceIdentity {
    /// The node.
    pub node: NodeId,
    /// Its incarnation at write time.
    pub incarnation: Incarnation,
}
