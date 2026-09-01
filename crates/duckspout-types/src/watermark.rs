//! Watermark row types (§4.2.4, §6.8, §7).
//!
//! Watermarks are the only registry state that matters (§6.8); everything
//! else is soft state. The rows here are the wire/domain shapes; the ledger
//! logic lives in `duckspout-watermark`, the transactional persistence
//! behind the `LakeCommitter` port.

use serde::{Deserialize, Serialize};

use crate::ids::{DatasetId, NodeId, PartitionId};

/// A per-partition completeness watermark row (`duckspout.watermarks`, §7):
/// `complete_through` advances only via this data's own drain (`LakeCommit`,
/// §6) or the `DeclareLoss` ceremony (§5.8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkRow {
    /// The partition this watermark covers.
    pub partition: PartitionId,
    /// The instant (Unix milliseconds, **inclusive**) through which this
    /// partition's data is complete in the lake: the §7.5 cold branch takes
    /// at-or-below, and `duckspout_absent` proves
    /// `range_end ≤ complete_through` (§7.6). Never lies
    /// (`WatermarkHonesty`, §3).
    pub complete_through_ms: i64,
}

/// A changelog dataset's dimension freshness row: `dimension_as_of` is the
/// dataset-level `complete_through` exposed to enrichment joins (§7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DimensionWatermarkRow {
    /// The changelog dataset.
    pub dataset: DatasetId,
    /// Freshness instant, Unix milliseconds.
    pub dimension_as_of_ms: i64,
}

/// The exactly-once apply row (§4.2.4): one row per `(partition, origin)`
/// holding the highest contiguously applied `seq`, advanced in the same hot
/// `DuckDB` transaction as the rows it accounts for. Shared by `PeerApply` and
/// the origin's own `StageCommit` bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedWatermarkRow {
    /// The partition the applied range belongs to.
    pub partition: PartitionId,
    /// The origin node that assigned the sequence numbers.
    pub origin: NodeId,
    /// Highest contiguously applied sequence number. A range at or below it
    /// is acknowledged without re-insertion; a range beyond `applied_seq + 1`
    /// is refused (gap refusal, §5).
    pub applied_seq: u64,
}
