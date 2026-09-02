//! Shared test fixtures.

use duckspout_types::{DatasetId, NodeId, OriginSeqRange, PartitionId, WindowId, WindowManifest};

use crate::loss::{LossLedgerRow, LostRange};

/// One origin's plan for one window: `None` = origin absent; otherwise
/// (chunk length, hole length before the chunk, whether the hole is
/// loss-ledgered).
pub(crate) type OriginPlan = Option<(u8, u8, bool)>;

/// Renders per-window [`OriginPlan`]s into a dense manifest history plus
/// the loss rows excusing the plans' excused holes — the generator behind
/// the ledger/reconstruction law suites.
pub(crate) fn build_history(
    partition: &str,
    plans: &[Vec<OriginPlan>],
    event_maxes: &[i16],
    dedups: &[u8],
) -> (Vec<WindowManifest>, Vec<LossLedgerRow>) {
    let origins = ["o1", "o2", "o3"];
    let mut next_seq = [1_u64; 3];
    let mut manifests = Vec::new();
    let mut losses = Vec::new();
    for (window, plan) in plans.iter().enumerate() {
        let mut coverage = Vec::new();
        for (index, origin_plan) in plan.iter().enumerate() {
            let Some((chunk, hole, excused)) = origin_plan else {
                continue;
            };
            let hole = u64::from(*hole);
            if hole > 0 {
                if *excused {
                    losses.push(loss(
                        partition,
                        origins[index],
                        next_seq[index],
                        next_seq[index] + hole - 1,
                    ));
                }
                next_seq[index] += hole;
            }
            let chunk = u64::from(*chunk).max(1);
            coverage.push(OriginSeqRange {
                origin: NodeId::new(origins[index]),
                first_seq: next_seq[index],
                last_seq: next_seq[index] + chunk - 1,
            });
            next_seq[index] += chunk;
        }
        let window_id = u64::try_from(window).expect("test histories are tiny");
        let mut m = manifest(partition, window_id, &[], i64::from(event_maxes[window]));
        // The fixture pins `event_time_min_ms = 0`; keep min ≤ max when
        // the generated max is negative.
        m.event_time_min_ms = m.event_time_max_ms.min(0);
        m.origin_coverage = coverage;
        m.dedup_removed = u64::from(dedups[window]);
        manifests.push(m);
    }
    (manifests, losses)
}

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
