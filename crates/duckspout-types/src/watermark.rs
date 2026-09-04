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

// ---------------------------------------------------------------------------
// DeclareLoss (§5.8, issue #54)
// ---------------------------------------------------------------------------
//
// These three types were originally sketched, types-only, inside
// `duckspout-watermark::loss` ("the ceremony's logic ... lands at v0.2 with
// replication" — that module's own doc comment). They move here now that the
// ceremony is real (issue #54): `LossLedgerCommitter` below needs
// `LossLedgerRow` in its signature, and ADR-0008 requires every type crossing
// a cross-crate port boundary to live in `duckspout-types`, exactly as
// `StagedCoverage`/`WindowManifest` already do for their own boundaries.
// `duckspout-watermark::loss` re-exports all three verbatim — no shape
// change, so nothing downstream (the existing `WatermarkLedger::record_loss`,
// its tests, `reconstruct.rs`) needed to change.

/// One **exact** lost `(partition, origin, seq-range)` — §5.8: no wildcards,
/// no "whatever is missing". Seqs are 1-based and inclusive on both ends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LostRange {
    /// The partition the lost range belongs to.
    pub partition: PartitionId,
    /// The origin that assigned the lost sequence numbers.
    pub origin: NodeId,
    /// First lost seq, inclusive (1-based).
    pub first_seq: u64,
    /// Last lost seq, inclusive.
    pub last_seq: u64,
}

/// The `DeclareLoss` ceremony's request shape (§5.8). Unwedging a frozen
/// watermark is a deliberate operator act, never automatic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclareLossRequest {
    /// The exact ranges being declared lost.
    pub ranges: Vec<LostRange>,
    /// The literal consent parameter — the name is the consent form (§5.8).
    /// The ceremony refuses any request where this is not `true`; it is a
    /// field, not a default.
    pub accept_data_loss: bool,
}

/// A permanent loss-ledger row (§5.8, §7.3): the first-class queryable
/// confession, written **in the same catalog transaction** as the watermark
/// advance past the lost range. `WatermarkHonesty`'s contract becomes
/// "complete, except the ledgered ranges" — auditable forever.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LossLedgerRow {
    /// The declared-lost range.
    pub range: LostRange,
    /// When the ceremony declared the loss, Unix milliseconds — supplied by
    /// the caller through the `Clock` port (this crate reads no clock, D-2).
    pub declared_at_ms: i64,
}

/// One live replica's advertised coverage of one origin's sequence range
/// within a partition (§5.5's `replicated_through`) — exactly the evidence
/// `DeclareLoss`'s refusal guard reads (§5.8: "refused while any live
/// replica still advertises coverage of the range"). The caller assembles
/// this from whatever it trusts as "live" (a registry read, a heartbeat-
/// filtered membership view, §5.6's detection timeline) — this crate reads
/// no such state itself; determining liveness is `duckspout-replication`'s
/// domain, not this one's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaCoverage {
    /// The live node advertising this coverage.
    pub node: NodeId,
    /// The origin whose sequence range this coverage answers for.
    pub origin: NodeId,
    /// The highest contiguous seq `node` advertises as durably held for
    /// `origin` within the queried partition.
    pub replicated_thru: u64,
}

#[cfg(test)]
mod loss_tests {
    use super::*;

    #[test]
    fn loss_rows_round_trip_through_serde() {
        let row = LossLedgerRow {
            range: LostRange {
                partition: PartitionId::new("t0-s0"),
                origin: NodeId::new("node-a"),
                first_seq: 6,
                last_seq: 7,
            },
            declared_at_ms: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&row).expect("serializes");
        let back: LossLedgerRow = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, row);
    }
}
