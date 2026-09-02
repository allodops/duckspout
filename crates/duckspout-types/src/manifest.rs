//! The window/part manifest (§2.4, §6.2, §6.8).
//!
//! **Frozen for the v1 series (§12.2).** Every `LakeCommit` carries one
//! [`WindowManifest`]; it rides the commit atomically and is stored queryably.
//! The manifest sequence in the lake, together with live hot staging state,
//! is what makes watermark state authoritative-but-reconstructible (§6.8).

use serde::{Deserialize, Serialize};

use crate::ids::{DatasetId, NodeId, PartName, PartitionId, WindowId};

/// The kind slot of a part's deterministic object name
/// `(dataset, partition, window_id, part_kind, discriminator)` (§6.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartKind {
    /// The window's sealed winner part(s) (§6.2).
    Primary,
    /// A supplement part: per-origin seq coverage validated disjoint against
    /// the sealed winner's manifest inside the commit transaction (§4.4.2).
    Supplement,
    /// A changelog snapshot part, sorted by `(key_cols)` (§6.2, §6.7).
    Snapshot,
}

/// Contiguous per-origin sequence coverage, `first_seq..=last_seq` (§6.8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginSeqRange {
    /// The accepting node that assigned this seq range (§4.2.4).
    pub origin: NodeId,
    /// First covered sequence number, inclusive.
    pub first_seq: u64,
    /// Last covered sequence number, inclusive.
    pub last_seq: u64,
}

/// The window manifest carried by every `LakeCommit` (§6.8). Frozen (§12.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowManifest {
    /// The dataset this window belongs to.
    pub dataset: DatasetId,
    /// The partition this window belongs to.
    pub partition: PartitionId,
    /// Dense per-partition window sequence — contiguity must be decidable
    /// (§6.8).
    pub window_id: WindowId,
    /// Per-origin seq coverage of the sealed data (§6.8).
    pub origin_coverage: Vec<OriginSeqRange>,
    /// Row count across the manifest's parts.
    pub rows: u64,
    /// Event-time minimum, Unix milliseconds. Late arrivals widen this
    /// truthfully (§6.3): placement reflects arrival, the column never lies.
    pub event_time_min_ms: i64,
    /// Event-time maximum, Unix milliseconds.
    pub event_time_max_ms: i64,
    /// Rows removed by drain-time dedup (§6.2). Load-bearing for `Demote`
    /// (§6.9) and the `CacheTransparency` proof obligations (§2.4): a drained
    /// window may serve `complete` reads from the cache class only when this
    /// is zero. Part of the v1 manifest format unconditionally (§2.4).
    pub dedup_removed: u64,
    /// The sealed parts' deterministic object names (§6.5).
    pub parts: Vec<PartName>,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_range() -> impl Strategy<Value = OriginSeqRange> {
        (".{0,24}", any::<u64>(), any::<u64>()).prop_map(|(origin, a, b)| OriginSeqRange {
            origin: NodeId::new(origin),
            first_seq: a.min(b),
            last_seq: a.max(b),
        })
    }

    proptest! {
        /// §8.5's manifest serialization-stability law: the frozen (§12.2)
        /// [`WindowManifest`] round-trips losslessly through JSON for ANY
        /// field values — ids with quotes and non-ASCII, extreme seqs,
        /// negative event times. The manifest is what makes the watermark
        /// reconstructible (§6.8); a field silently dropped, renamed, or
        /// truncated by a serde attribute would corrupt every stored
        /// manifest, and this is the test that catches it before the format
        /// drifts.
        #[test]
        fn window_manifest_round_trips_any_values(
            dataset in ".{0,24}",
            partition in ".{0,24}",
            window in any::<u64>(),
            coverage in prop::collection::vec(arb_range(), 0..4),
            rows in any::<u64>(),
            t1 in any::<i64>(),
            t2 in any::<i64>(),
            dedup_removed in any::<u64>(),
            parts in prop::collection::vec(".{0,24}", 0..3),
        ) {
            let manifest = WindowManifest {
                dataset: DatasetId::new(dataset),
                partition: PartitionId::new(partition),
                window_id: WindowId(window),
                origin_coverage: coverage,
                rows,
                event_time_min_ms: t1.min(t2),
                event_time_max_ms: t1.max(t2),
                dedup_removed,
                parts: parts.into_iter().map(PartName::new).collect(),
            };
            let json = serde_json::to_string(&manifest).expect("serialize");
            let back: WindowManifest = serde_json::from_str(&json).expect("deserialize");
            prop_assert_eq!(back, manifest);
        }
    }
}
