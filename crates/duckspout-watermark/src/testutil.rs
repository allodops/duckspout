//! Shared test fixtures.

use duckspout_types::{DatasetId, NodeId, OriginSeqRange, PartitionId, WindowId, WindowManifest};

use crate::loss::{LossLedgerRow, LostRange};

/// A manifest for dataset `ds`, with the given per-origin coverage.
pub(crate) fn manifest(
    partition: &str,
    window: u64,
    coverage: &[(&str, u64, u64)],
    event_time_max_ms: i64,
) -> WindowManifest {
    WindowManifest {
        dataset: DatasetId::new("ds"),
        partition: PartitionId::new(partition),
        window_id: WindowId(window),
        origin_coverage: coverage
            .iter()
            .map(|&(origin, first_seq, last_seq)| OriginSeqRange {
                origin: NodeId::new(origin),
                first_seq,
                last_seq,
            })
            .collect(),
        rows: 10,
        event_time_min_ms: 0,
        event_time_max_ms,
        dedup_removed: 0,
        parts: Vec::new(),
    }
}

/// A loss-ledger row declared at instant 0.
pub(crate) fn loss(partition: &str, origin: &str, first_seq: u64, last_seq: u64) -> LossLedgerRow {
    LossLedgerRow {
        range: LostRange {
            partition: PartitionId::new(partition),
            origin: NodeId::new(origin),
            first_seq,
            last_seq,
        },
        declared_at_ms: 0,
    }
}
