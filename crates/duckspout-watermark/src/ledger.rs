//! The per-partition watermark ledger (§2.4, §6.8, §7.3).

use std::collections::BTreeMap;

use duckspout_types::{
    DatasetId, NodeId, OriginSeqRange, PartitionId, WatermarkRow, WindowId, WindowManifest,
};

use crate::coverage::{OriginCoverage, unexcused_gaps};
use crate::loss::LossLedgerRow;

/// A rejected ledger mutation. Every variant is a caller bug or a corrupt
/// record — the ledger fails closed rather than guessing (R-3).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdvanceError {
    /// The manifest names a dataset other than the one this partition is
    /// recorded under. A partition belongs to exactly one dataset (§2.2: the
    /// partition key is the dataset's `(tenant_id, shard)` space).
    #[error(
        "dataset mismatch for partition {partition}: ledger has {recorded}, manifest has {got}"
    )]
    DatasetMismatch {
        /// The partition whose pairing was violated.
        partition: PartitionId,
        /// The dataset the ledger recorded for this partition.
        recorded: DatasetId,
        /// The dataset the offered manifest names.
        got: DatasetId,
    },
    /// The manifest's window is not the dense-next window for its partition
    /// (§6.8: contiguity must be decidable). A lower id is a replay of a
    /// commit that already stands; a higher id would tear a hole in the
    /// dense sequence.
    #[error(
        "window {got} is not the dense-next window of partition {partition} (expected {expected})"
    )]
    WindowNotNext {
        /// The partition whose sequence was violated.
        partition: PartitionId,
        /// The dense-next window id the ledger expected.
        expected: WindowId,
        /// The window id the offered manifest carries.
        got: WindowId,
    },
    /// A malformed per-origin seq range: seqs are 1-based (§4.2.4) and
    /// `first_seq ≤ last_seq`.
    #[error(
        "invalid seq range {first_seq}..={last_seq} for origin {origin} (seqs are 1-based, first ≤ last)"
    )]
    InvalidSeqRange {
        /// The origin the range was declared for.
        origin: NodeId,
        /// The offered first seq, inclusive.
        first_seq: u64,
        /// The offered last seq, inclusive.
        last_seq: u64,
    },
    /// A malformed event-time range: `event_time_min_ms ≤ event_time_max_ms`.
    #[error("invalid event-time range: min {min_ms} > max {max_ms}")]
    InvalidEventTimeRange {
        /// The offered event-time minimum, Unix milliseconds.
        min_ms: i64,
        /// The offered event-time maximum, Unix milliseconds.
        max_ms: i64,
    },
    /// The manifest's coverage intersects coverage already committed for the
    /// partition — committed parts are pairwise disjoint per origin
    /// (`SingleDrainCommit`'s second conjunct, §6.6).
    #[error(
        "coverage {first_seq}..={last_seq} for origin {origin} intersects already-committed coverage"
    )]
    CoverageOverlap {
        /// The origin whose coverage would double-commit.
        origin: NodeId,
        /// The offending range's first seq, inclusive.
        first_seq: u64,
        /// The offending range's last seq, inclusive.
        last_seq: u64,
    },
}

/// The ledger's compact projection of one committed window's manifest — the
/// fields later machinery reads back: coverage (the advance rule), the
/// event-time maximum (the watermark instant), and `dedup_removed` (the
/// `Demote` guard, §6.9), passed through verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowRecord {
    /// Per-origin seq coverage, verbatim from the manifest (§6.8).
    pub origin_coverage: Vec<OriginSeqRange>,
    /// The manifest's event-time maximum, Unix milliseconds.
    pub event_time_max_ms: i64,
    /// Rows removed by drain-time dedup, verbatim from the manifest (§6.2).
    /// `Demote` may substitute the hot table for the sealed parts only when
    /// this is zero (§2.4, §6.9).
    pub dedup_removed: u64,
}

/// One partition's bookkeeping. The `done_*` fields are a deterministic
/// function of `windows` + `losses` (the advance rule), cached incrementally.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PartitionState {
    /// The dataset this partition belongs to; `None` until the first
    /// committed manifest names it (a loss can be recorded first, §5.8).
    dataset: Option<DatasetId>,
    /// Committed windows by dense window id.
    windows: BTreeMap<u64, WindowRecord>,
    /// Union of all committed coverage (overlap validation).
    committed: OriginCoverage,
    /// Loss-ledgered coverage (§5.8) — the sanctioned excusal.
    losses: OriginCoverage,
    /// Coverage of windows `0..=done_through` (the advance cursor).
    done_cov: OriginCoverage,
    /// The highest window the watermark has advanced through.
    done_through: Option<u64>,
    /// The partition's `complete_through`, Unix milliseconds, inclusive.
    complete_through_ms: Option<i64>,
}

impl PartitionState {
    /// Re-runs the advance rule from the cached cursor: walk the dense
    /// window sequence, folding coverage; the watermark stands at the
    /// highest window after which every origin's committed coverage is
    /// gap-free from seq 1 (losses excusing, §6.8's committed-or-ledgered).
    /// A hole discovered at window `w` parks the watermark before `w` until
    /// a later window or a loss row covers it — advancing past unproven
    /// coverage would let `complete_through` lie (`WatermarkHonesty`, §3).
    fn extend(&mut self) {
        let mut cov = self.done_cov.clone();
        let mut event_max = self.complete_through_ms;
        let mut next = self.done_through.map_or(0, |done| done + 1);
        while let Some(record) = self.windows.get(&next) {
            for range in &record.origin_coverage {
                cov.entry(range.origin.clone())
                    .or_default()
                    .insert(range.first_seq, range.last_seq);
            }
            event_max = Some(event_max.map_or(record.event_time_max_ms, |current| {
                current.max(record.event_time_max_ms)
            }));
            if unexcused_gaps(&cov, &self.losses).is_empty() {
                self.done_cov.clone_from(&cov);
                self.done_through = Some(next);
                self.complete_through_ms = event_max;
            }
            next += 1;
        }
    }

    fn watermark_row(&self, partition: &PartitionId) -> Option<WatermarkRow> {
        self.complete_through_ms.map(|ms| WatermarkRow {
            partition: partition.clone(),
            complete_through_ms: ms,
        })
    }
}

/// The lake-neutral watermark ledger: per-`(dataset, partition)`
/// `complete_through` bookkeeping above the [`duckspout_types::LakeCommitter`]
/// port (ADR-0010 — the lake stores the state; this crate computes it).
///
/// See the crate docs for the advance rule and the conventions (0-based
/// dense windows, 1-based per-origin seqs, inclusive `complete_through`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatermarkLedger {
    partitions: BTreeMap<PartitionId, PartitionState>,
}

impl WatermarkLedger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The watermark rows `commit_files` must carry for `manifest` (§6.4:
    /// `WatermarkAdvance` rides `LakeCommit` atomically) — the post-commit
    /// state computed **without** mutating the ledger. `Ok(None)` means the
    /// partition still has no provable watermark (its coverage is blocked
    /// from window 0); the commit then carries no watermark row.
    ///
    /// On `CommitOutcome::Committed`, record the same manifest with
    /// [`WatermarkLedger::record_commit`].
    ///
    /// # Errors
    ///
    /// Every [`AdvanceError`] that [`WatermarkLedger::record_commit`] would
    /// return for `manifest`.
    pub fn advance_for(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, AdvanceError> {
        self.clone().record_commit(manifest)
    }

    /// Records a durably committed window manifest and returns the
    /// partition's watermark row after the advance rule re-runs — `None`
    /// while the partition has no provable watermark. The returned row is
    /// unchanged (not an error) when the new window is coverage-blocked:
    /// the commit stands, the watermark honestly does not move.
    ///
    /// # Errors
    ///
    /// - [`AdvanceError::DatasetMismatch`] — the partition is recorded under
    ///   a different dataset;
    /// - [`AdvanceError::WindowNotNext`] — the window id is not the
    ///   dense-next id (replay or gap; §6.8 contiguity);
    /// - [`AdvanceError::InvalidSeqRange`] /
    ///   [`AdvanceError::InvalidEventTimeRange`] — malformed manifest;
    /// - [`AdvanceError::CoverageOverlap`] — coverage intersects a committed
    ///   part (§6.6 disjointness).
    ///
    /// The ledger is unchanged on every error.
    pub fn record_commit(
        &mut self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, AdvanceError> {
        if manifest.event_time_min_ms > manifest.event_time_max_ms {
            return Err(AdvanceError::InvalidEventTimeRange {
                min_ms: manifest.event_time_min_ms,
                max_ms: manifest.event_time_max_ms,
            });
        }
        let state = self.partitions.get(&manifest.partition);
        if let Some(recorded) = state.and_then(|s| s.dataset.as_ref())
            && *recorded != manifest.dataset
        {
            return Err(AdvanceError::DatasetMismatch {
                partition: manifest.partition.clone(),
                recorded: recorded.clone(),
                got: manifest.dataset.clone(),
            });
        }
        let expected = state.map_or(0, PartitionState::next_window_id);
        if manifest.window_id.0 != expected {
            return Err(AdvanceError::WindowNotNext {
                partition: manifest.partition.clone(),
                expected: WindowId(expected),
                got: manifest.window_id,
            });
        }
        // Validate every range before mutating anything: the probe set is the
        // already-committed coverage plus this manifest's earlier ranges, so
        // within-manifest overlap is caught too.
        let mut probe: OriginCoverage = OriginCoverage::new();
        for range in &manifest.origin_coverage {
            if range.first_seq == 0 || range.first_seq > range.last_seq {
                return Err(AdvanceError::InvalidSeqRange {
                    origin: range.origin.clone(),
                    first_seq: range.first_seq,
                    last_seq: range.last_seq,
                });
            }
            let set = probe.entry(range.origin.clone()).or_insert_with(|| {
                state
                    .and_then(|s| s.committed.get(&range.origin))
                    .cloned()
                    .unwrap_or_default()
            });
            if set.overlaps(range.first_seq, range.last_seq) {
                return Err(AdvanceError::CoverageOverlap {
                    origin: range.origin.clone(),
                    first_seq: range.first_seq,
                    last_seq: range.last_seq,
                });
            }
            set.insert(range.first_seq, range.last_seq);
        }
        // All checks passed — mutate.
        let state = self
            .partitions
            .entry(manifest.partition.clone())
            .or_default();
        state
            .dataset
            .get_or_insert_with(|| manifest.dataset.clone());
        state.committed.extend(probe);
        state.windows.insert(
            manifest.window_id.0,
            WindowRecord {
                origin_coverage: manifest.origin_coverage.clone(),
                event_time_max_ms: manifest.event_time_max_ms,
                dedup_removed: manifest.dedup_removed,
            },
        );
        state.extend();
        Ok(state.watermark_row(&manifest.partition))
    }

    /// Records a loss-ledger row (§5.8) and returns the partition's
    /// watermark row after the advance rule re-runs — the ledgered range
    /// excuses coverage holes, so this is the bookkeeping half of the
    /// "watermark advance past the lost range, atomically beside the
    /// confession" contract. The ceremony's guards (the literal
    /// `accept_data_loss` consent, refusal while a live replica still
    /// advertises coverage) live above this call and land at v0.2; see
    /// [`crate::DeclareLossRequest`].
    ///
    /// # Errors
    ///
    /// [`AdvanceError::InvalidSeqRange`] for a malformed range. The ledger
    /// is unchanged on error.
    pub fn record_loss(
        &mut self,
        row: &LossLedgerRow,
    ) -> Result<Option<WatermarkRow>, AdvanceError> {
        let range = &row.range;
        if range.first_seq == 0 || range.first_seq > range.last_seq {
            return Err(AdvanceError::InvalidSeqRange {
                origin: range.origin.clone(),
                first_seq: range.first_seq,
                last_seq: range.last_seq,
            });
        }
        let state = self.partitions.entry(range.partition.clone()).or_default();
        state
            .losses
            .entry(range.origin.clone())
            .or_default()
            .insert(range.first_seq, range.last_seq);
        state.extend();
        Ok(state.watermark_row(&range.partition))
    }

    /// The partition's `complete_through`, Unix milliseconds, **inclusive**:
    /// an instant at or below it is lake-covered (§7.5's cold branch takes
    /// at-or-below).
    #[must_use]
    pub fn complete_through_ms(&self, partition: &PartitionId) -> Option<i64> {
        self.partitions
            .get(partition)
            .and_then(|state| state.complete_through_ms)
    }

    /// Whether a `complete` read through `instant_ms` is lake-covered for
    /// this partition — the §7.6 predicate (`range_end ≤ complete_through`),
    /// boundary inclusive: an instant exactly at `complete_through` is
    /// covered. `false` when the partition has no watermark ("couldn't
    /// check" is never "empty", R-3).
    #[must_use]
    pub fn covers(&self, partition: &PartitionId, instant_ms: i64) -> bool {
        self.complete_through_ms(partition)
            .is_some_and(|complete_through| instant_ms <= complete_through)
    }

    /// The highest window the watermark has advanced through, if any.
    #[must_use]
    pub fn advanced_through(&self, partition: &PartitionId) -> Option<WindowId> {
        self.partitions
            .get(partition)
            .and_then(|state| state.done_through)
            .map(WindowId)
    }

    /// The highest committed window recorded for the partition, if any —
    /// equals [`WatermarkLedger::advanced_through`] exactly when no
    /// coverage hole blocks the advance rule.
    #[must_use]
    pub fn recorded_through(&self, partition: &PartitionId) -> Option<WindowId> {
        self.partitions
            .get(partition)
            .and_then(|state| state.windows.last_key_value())
            .map(|(id, _)| WindowId(*id))
    }

    /// The dense-next window id the ledger will accept for the partition —
    /// `WindowId(0)` for a partition with no committed windows.
    #[must_use]
    pub fn next_window(&self, partition: &PartitionId) -> WindowId {
        WindowId(
            self.partitions
                .get(partition)
                .map_or(0, PartitionState::next_window_id),
        )
    }

    /// The dataset the partition is recorded under, once a manifest has
    /// named it.
    #[must_use]
    pub fn dataset(&self, partition: &PartitionId) -> Option<&DatasetId> {
        self.partitions
            .get(partition)
            .and_then(|state| state.dataset.as_ref())
    }

    /// The ledger's record of one committed window — coverage, event-time
    /// maximum, and the pass-through `dedup_removed` the `Demote` guard
    /// reads (§6.9).
    #[must_use]
    pub fn window_record(
        &self,
        partition: &PartitionId,
        window: WindowId,
    ) -> Option<&WindowRecord> {
        self.partitions
            .get(partition)
            .and_then(|state| state.windows.get(&window.0))
    }

    /// The current watermark rows, sorted by partition — what
    /// `read_watermarks` (§6.4) would return if the lake state matches this
    /// ledger. Partitions without a provable watermark contribute no row.
    #[must_use]
    pub fn rows(&self) -> Vec<WatermarkRow> {
        self.partitions
            .iter()
            .filter_map(|(partition, state)| state.watermark_row(partition))
            .collect()
    }

    /// The unexcused coverage gaps currently blocking the partition's
    /// advance, over the full committed coverage.
    pub(crate) fn blocking_gaps(&self, partition: &PartitionId) -> Vec<(NodeId, u64, u64)> {
        self.partitions
            .get(partition)
            .map(|state| unexcused_gaps(&state.committed, &state.losses))
            .unwrap_or_default()
    }
}

impl PartitionState {
    fn next_window_id(&self) -> u64 {
        self.windows.last_key_value().map_or(0, |(id, _)| id + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{loss, manifest};

    #[test]
    fn advance_from_manifest_is_the_event_time_max() {
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        let row = ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0 commits")
            .expect("watermark exists after window 0");
        assert_eq!(row.complete_through_ms, 1_000);
        let row = ledger
            .record_commit(&manifest("p", 1, &[("o1", 6, 9)], 2_000))
            .expect("window 1 commits")
            .expect("watermark exists");
        assert_eq!(row.complete_through_ms, 2_000);
        assert_eq!(ledger.advanced_through(&p), Some(WindowId(1)));
    }

    #[test]
    fn boundary_row_at_complete_through_is_covered() {
        // §7.5: the cold branch takes at-or-below — the boundary instant
        // itself is lake-covered, one past it is not.
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0 commits");
        assert!(
            ledger.covers(&p, 1_000),
            "the row AT complete_through is covered"
        );
        assert!(
            !ledger.covers(&p, 1_001),
            "one ms past complete_through is not"
        );
        assert!(ledger.covers(&p, 0));
    }

    #[test]
    fn unknown_partition_is_never_covered() {
        let ledger = WatermarkLedger::new();
        assert!(!ledger.covers(&PartitionId::new("nope"), 0));
        assert_eq!(ledger.complete_through_ms(&PartitionId::new("nope")), None);
    }

    #[test]
    fn all_late_window_does_not_regress_the_watermark() {
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 5_000))
            .expect("window 0");
        let row = ledger
            .record_commit(&manifest("p", 1, &[("o1", 6, 9)], 3_000))
            .expect("late window commits")
            .expect("watermark exists");
        assert_eq!(row.complete_through_ms, 5_000, "watermark is monotone");
        assert_eq!(ledger.advanced_through(&p), Some(WindowId(1)));
    }

    #[test]
    fn window_gap_is_rejected() {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        let err = ledger
            .record_commit(&manifest("p", 2, &[("o1", 6, 9)], 2_000))
            .expect_err("window 2 skips window 1");
        assert_eq!(
            err,
            AdvanceError::WindowNotNext {
                partition: PartitionId::new("p"),
                expected: WindowId(1),
                got: WindowId(2),
            }
        );
        // The rejection left no trace.
        assert_eq!(ledger.next_window(&PartitionId::new("p")), WindowId(1));
    }

    #[test]
    fn window_replay_is_rejected() {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        let err = ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect_err("a replayed window is not dense-next");
        assert!(matches!(err, AdvanceError::WindowNotNext { .. }));
    }

    #[test]
    fn first_window_must_be_zero() {
        let mut ledger = WatermarkLedger::new();
        let err = ledger
            .record_commit(&manifest("p", 1, &[("o1", 1, 5)], 1_000))
            .expect_err("the dense sequence starts at 0");
        assert!(matches!(
            err,
            AdvanceError::WindowNotNext {
                expected: WindowId(0),
                ..
            }
        ));
    }

    #[test]
    fn dataset_mismatch_is_rejected() {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        let mut wrong = manifest("p", 1, &[("o1", 6, 9)], 2_000);
        wrong.dataset = DatasetId::new("other");
        let err = ledger
            .record_commit(&wrong)
            .expect_err("partition is bound to ds");
        assert!(matches!(err, AdvanceError::DatasetMismatch { .. }));
    }

    #[test]
    fn coverage_overlap_is_rejected_without_mutation() {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        let err = ledger
            .record_commit(&manifest("p", 1, &[("o2", 1, 3), ("o1", 5, 7)], 2_000))
            .expect_err("o1 seq 5 is already committed");
        assert!(matches!(
            err,
            AdvanceError::CoverageOverlap {
                first_seq: 5,
                last_seq: 7,
                ..
            }
        ));
        // The valid o2 range of the rejected manifest must not have leaked in.
        let retry = manifest("p", 1, &[("o2", 1, 3), ("o1", 6, 7)], 2_000);
        ledger
            .record_commit(&retry)
            .expect("disjoint retry commits");
    }

    #[test]
    fn malformed_ranges_are_rejected() {
        let mut ledger = WatermarkLedger::new();
        assert!(matches!(
            ledger.record_commit(&manifest("p", 0, &[("o1", 0, 3)], 1_000)),
            Err(AdvanceError::InvalidSeqRange { first_seq: 0, .. })
        ));
        assert!(matches!(
            ledger.record_commit(&manifest("p", 0, &[("o1", 5, 3)], 1_000)),
            Err(AdvanceError::InvalidSeqRange {
                first_seq: 5,
                last_seq: 3,
                ..
            })
        ));
        let mut bad_time = manifest("p", 0, &[("o1", 1, 3)], 1_000);
        bad_time.event_time_min_ms = 2_000;
        assert!(matches!(
            ledger.record_commit(&bad_time),
            Err(AdvanceError::InvalidEventTimeRange { .. })
        ));
        assert_eq!(ledger, WatermarkLedger::new(), "no rejection mutated state");
    }

    #[test]
    fn coverage_hole_blocks_the_advance_until_loss_ledgered() {
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        // Window 1 reveals a hole: o1 seqs 6..=7 are neither committed nor
        // ledgered.
        let row = ledger
            .record_commit(&manifest("p", 1, &[("o1", 8, 10)], 2_000))
            .expect("the commit stands");
        assert_eq!(
            row.map(|r| r.complete_through_ms),
            Some(1_000),
            "the watermark honestly does not move over the hole"
        );
        assert_eq!(ledger.advanced_through(&p), Some(WindowId(0)));
        // The DeclareLoss ceremony ledgered the hole: the watermark resumes
        // in the same bookkeeping step (§5.8).
        let row = ledger
            .record_loss(&loss("p", "o1", 6, 7))
            .expect("loss row records")
            .expect("watermark exists");
        assert_eq!(row.complete_through_ms, 2_000);
        assert_eq!(ledger.advanced_through(&p), Some(WindowId(1)));
    }

    #[test]
    fn coverage_hole_filled_by_a_later_window_resumes_the_advance() {
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        ledger
            .record_commit(&manifest("p", 1, &[("o1", 8, 10)], 2_000))
            .expect("window 1 (hole 6..=7)");
        assert_eq!(ledger.advanced_through(&p), Some(WindowId(0)));
        let row = ledger
            .record_commit(&manifest("p", 2, &[("o1", 6, 7)], 1_500))
            .expect("window 2 fills the hole")
            .expect("watermark exists");
        assert_eq!(
            row.complete_through_ms, 2_000,
            "windows 1 and 2 both fold in"
        );
        assert_eq!(ledger.advanced_through(&p), Some(WindowId(2)));
    }

    #[test]
    fn blocked_from_window_zero_yields_no_row() {
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        let row = ledger
            .record_commit(&manifest("p", 0, &[("o1", 3, 5)], 1_000))
            .expect("the commit stands");
        assert_eq!(
            row, None,
            "seqs 1..=2 are unproven; no watermark exists yet"
        );
        assert_eq!(ledger.complete_through_ms(&p), None);
        assert!(ledger.rows().is_empty());
        assert_eq!(ledger.advanced_through(&p), None);
        assert_eq!(ledger.recorded_through(&p), Some(WindowId(0)));
    }

    #[test]
    fn advance_for_previews_without_mutating() {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0");
        let before = ledger.clone();
        let next = manifest("p", 1, &[("o1", 6, 9)], 2_000);
        let row = ledger
            .advance_for(&next)
            .expect("preview computes")
            .expect("watermark exists");
        assert_eq!(row.complete_through_ms, 2_000);
        assert_eq!(ledger, before, "advance_for is pure");
        // Recording after the durable commit yields exactly the previewed row.
        let recorded = ledger.record_commit(&next).expect("commit records");
        assert_eq!(recorded, Some(row));
    }

    #[test]
    fn dedup_removed_passes_through_verbatim() {
        let mut ledger = WatermarkLedger::new();
        let p = PartitionId::new("p");
        let mut m = manifest("p", 0, &[("o1", 1, 5)], 1_000);
        m.dedup_removed = 7;
        ledger.record_commit(&m).expect("window 0");
        let record = ledger
            .window_record(&p, WindowId(0))
            .expect("window 0 is recorded");
        assert_eq!(record.dedup_removed, 7);
        assert_eq!(record.event_time_max_ms, 1_000);
        assert_eq!(record.origin_coverage, m.origin_coverage);
    }

    #[test]
    fn rows_are_sorted_by_partition() {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("pb", 0, &[("o1", 1, 5)], 2_000))
            .expect("pb window 0");
        ledger
            .record_commit(&manifest("pa", 0, &[("o1", 1, 5)], 1_000))
            .expect("pa window 0");
        let rows = ledger.rows();
        assert_eq!(
            rows.iter()
                .map(|r| r.partition.as_str())
                .collect::<Vec<_>>(),
            vec!["pa", "pb"]
        );
        assert_eq!(
            ledger.dataset(&PartitionId::new("pa")),
            Some(&DatasetId::new("ds"))
        );
    }

    // --- §8.5 law suite: advance monotonicity under arbitrary
    // --- manifest/loss interleavings (issue #40; extends the
    // --- reconstruction≡live property in `reconstruct.rs`).

    mod laws {
        use proptest::prelude::*;

        use super::*;
        use crate::testutil::build_history;

        /// Applies one accepted mutation and asserts the two monotone
        /// quantities never move backwards across it.
        fn assert_monotone(
            ledger: &WatermarkLedger,
            partition: &PartitionId,
            before_ms: Option<i64>,
            before_through: Option<WindowId>,
        ) {
            let after_ms = ledger.complete_through_ms(partition);
            let after_through = ledger.advanced_through(partition);
            assert!(
                after_ms >= before_ms,
                "complete_through regressed: {before_ms:?} -> {after_ms:?}"
            );
            assert!(
                after_through >= before_through,
                "advance cursor regressed: {before_through:?} -> {after_through:?}"
            );
        }

        /// Attempts three mutations the ledger must reject — a window-id
        /// gap, overlapping coverage (when any is committed), a malformed
        /// loss range — and asserts each is a TOTAL no-op.
        fn assert_rejections_are_no_ops(ledger: &mut WatermarkLedger, partition: &PartitionId) {
            let before = ledger.clone();
            let gap = ledger.next_window(partition).0 + 5;
            let gapped = manifest(partition.as_str(), gap, &[("o1", 1_000, 1_001)], 0);
            ledger
                .record_commit(&gapped)
                .expect_err("a window-id gap must be rejected");
            if let Some(record) = ledger
                .advanced_through(partition)
                .and_then(|w| ledger.window_record(partition, w).cloned())
                && let Some(range) = record.origin_coverage.first()
            {
                let next = ledger.next_window(partition).0;
                let mut overlap = manifest(partition.as_str(), next, &[], 0);
                overlap.origin_coverage = vec![range.clone()];
                ledger
                    .record_commit(&overlap)
                    .expect_err("re-committing committed coverage must be rejected");
            }
            ledger
                .record_loss(&loss(partition.as_str(), "o1", 0, 3))
                .expect_err("a 0-based loss range must be rejected");
            assert_eq!(*ledger, before, "a rejected mutation must change nothing");
        }

        proptest! {
            /// `WatermarkHonesty`'s monotone half as a law (§6.8, §3): under
            /// ANY interleaving of committed manifests and loss-ledger rows
            /// — holes revealed then excused, all-late windows, losses
            /// landing before their hole is even visible — `complete_through`
            /// and the advance cursor never move backwards, and every
            /// rejected mutation (window gap, overlapping coverage,
            /// malformed range) is a total no-op. Would catch: an advance
            /// rule that re-derives `complete_through` from the newest
            /// window alone (an all-late window would regress it), a loss
            /// row recomputing the cursor from scratch, or a rejection that
            /// leaves partial coverage behind (the probe-set design exists
            /// exactly to prevent that).
            #[test]
            fn watermark_never_regresses_under_any_interleaving(
                plans in prop::collection::vec(
                    prop::collection::vec(
                        prop::option::of((1u8..=4, 0u8..=3, any::<bool>())),
                        3,
                    ),
                    1..=6,
                ),
                event_maxes in prop::collection::vec(-100i16..=1000, 6),
                dedups in prop::collection::vec(0u8..=3, 6),
                picks in prop::collection::vec(any::<bool>(), 24),
            ) {
                let partition = PartitionId::new("pa");
                let (manifests, losses) = build_history("pa", &plans, &event_maxes, &dedups);

                // Interleave: `picks` chooses commit vs. loss at each step;
                // commits keep their dense order (the ledger enforces it),
                // losses keep generation order — the interleaving point is
                // the free variable.
                let mut ledger = WatermarkLedger::new();
                let mut commits = manifests.iter();
                let mut loss_rows = losses.iter();
                let mut picks = picks.iter();
                loop {
                    let take_commit = *picks.next().unwrap_or(&true);
                    let before_ms = ledger.complete_through_ms(&partition);
                    let before_through = ledger.advanced_through(&partition);
                    if take_commit {
                        let Some(m) = commits.next() else { break };
                        ledger.record_commit(m).expect("dense history commits");
                    } else {
                        let Some(row) = loss_rows.next() else { continue };
                        ledger.record_loss(row).expect("loss records");
                    }
                    assert_monotone(&ledger, &partition, before_ms, before_through);
                }
                for row in loss_rows {
                    let before_ms = ledger.complete_through_ms(&partition);
                    let before_through = ledger.advanced_through(&partition);
                    ledger.record_loss(row).expect("loss records");
                    assert_monotone(&ledger, &partition, before_ms, before_through);
                }
                assert_rejections_are_no_ops(&mut ledger, &partition);
            }
        }
    }

    /// ACPR #199 HIGH-4 scratch-repro re-verification: a second manifest
    /// for an ALREADY-COMMITTED window — exactly the shape a churn-boundary
    /// supplement part (§5.6 step 5) would need to commit — is rejected
    /// with `WindowNotNext`, never `CoverageOverlap`, even though its
    /// coverage is genuinely disjoint from what was already committed for
    /// that window. This is the concrete, executable proof behind
    /// `duckspout_replication::takeover`'s corrected module docs: an
    /// earlier revision of this codebase's documentation (that module,
    /// `compute_residue`'s own doc, and PR #199's own description) claimed
    /// this guard "already enforces" disjoint supplement coverage
    /// generically — false, because the dense-next contiguity check below
    /// runs BEFORE any coverage-overlap check and rejects the second
    /// manifest outright. `duckspout_replication::takeover::compute_residue`
    /// computes a value that cannot currently be submitted through any
    /// commit path this ledger offers.
    #[test]
    fn a_second_manifest_for_an_already_committed_window_hits_window_not_next_not_coverage_overlap()
    {
        let mut ledger = WatermarkLedger::new();
        ledger
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0 commits its primary part");

        // A hypothetical supplement for the SAME window, with coverage that
        // is genuinely disjoint from what window 0 already committed
        // (o1 6..=10, no overlap with 1..=5 at all) -- the exact shape
        // `compute_residue` would hand a caller to submit.
        let supplement = manifest("p", 0, &[("o1", 6, 10)], 1_000);
        let err = ledger
            .record_commit(&supplement)
            .expect_err("no code path accepts a second manifest for window 0 today");
        assert!(
            matches!(
                err,
                AdvanceError::WindowNotNext {
                    expected: WindowId(1),
                    got: WindowId(0),
                    ..
                }
            ),
            "expected WindowNotNext (the dense-next fence, reached first), \
             got {err:?} -- if this ever becomes CoverageOverlap instead, \
             the dense-next check has grown a supplement exemption and the \
             module docs claiming no such exemption exists need updating \
             alongside it"
        );
    }

    #[test]
    fn loss_for_an_unknown_partition_records_without_a_watermark() {
        let mut ledger = WatermarkLedger::new();
        let row = ledger
            .record_loss(&loss("p", "o1", 1, 3))
            .expect("loss row records");
        assert_eq!(row, None, "no committed window, no watermark");
        assert!(matches!(
            ledger.record_loss(&loss("p", "o1", 0, 3)),
            Err(AdvanceError::InvalidSeqRange { .. })
        ));
    }
}
