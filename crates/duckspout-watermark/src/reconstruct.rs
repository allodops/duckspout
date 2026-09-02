//! Watermark reconstruction from the lake's manifest record (§6.8).
//!
//! The catalog's watermark rows are the fast path; the ground truth is
//! derivable from (a) the dense manifest sequence in the lake, (b) the
//! loss ledger, and (c) live hot coverage. Reconstruction replays the
//! record through the same [`WatermarkLedger`] advance rule the live path
//! uses — one rule, two entry points — so the recomputed state is the
//! authoritative state, never a parallel approximation. The recomputed
//! watermark is ≤ the true pre-failure watermark, never greater: the cost
//! of a recovery gap is temporary conservatism, never a false `complete`
//! (§6.8's PITR procedure, step 4).

use std::collections::BTreeMap;

use duckspout_types::{AppliedWatermarkRow, NodeId, PartitionId, WindowId, WindowManifest};

use crate::ledger::{AdvanceError, WatermarkLedger};
use crate::loss::LossLedgerRow;

/// A manifest history or loss ledger that no honest lake can produce —
/// reconstruction fails closed rather than guessing (R-3).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReconstructError {
    /// A manifest in the history was rejected by the ledger — a duplicate
    /// window id (the §6.6 fence makes one impossible), overlapping
    /// coverage, a dataset mismatch, or a malformed range.
    #[error("manifest history of partition {partition}, window {window}: {source}")]
    Manifest {
        /// The partition whose history is corrupt.
        partition: PartitionId,
        /// The window whose manifest was rejected.
        window: WindowId,
        /// The ledger's rejection.
        source: AdvanceError,
    },
    /// A loss-ledger row was rejected by the ledger (malformed range).
    #[error("loss ledger row for partition {partition}: {source}")]
    Loss {
        /// The partition whose loss row is corrupt.
        partition: PartitionId,
        /// The ledger's rejection.
        source: AdvanceError,
    },
}

/// One unexcused per-origin coverage hole blocking a partition's watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageHole {
    /// The origin whose committed coverage has the hole.
    pub origin: NodeId,
    /// First missing seq, inclusive.
    pub first_seq: u64,
    /// Last missing seq, inclusive.
    pub last_seq: u64,
    /// Whether live hot coverage still holds the missing range: `true` when
    /// the origin's `applied_seq` is at or past `last_seq` — the range was
    /// durably applied, and acked data leaves staging only by successful
    /// drain (R-5), so it is still staged and a supplement commit will fill
    /// the hole. `false` means no live coverage accounts for it: the range
    /// is a `DeclareLoss` candidate (§5.8).
    pub in_hot: bool,
}

/// Why a partition's reconstructed watermark stands below its recorded
/// windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StallReason {
    /// The dense window sequence has a missing id — a commit lost to the
    /// recovery horizon. Orphan reconcile (§6.8 step 3) either re-registers
    /// the window or proves it never committed; its manifests-after-the-gap
    /// are returned in [`Reconstruction::deferred`].
    MissingWindow {
        /// The absent dense-next window id.
        expected: WindowId,
    },
    /// Committed coverage has unexcused per-origin holes; the watermark
    /// stands before the first window that revealed one.
    CoverageHoles {
        /// Every blocking hole, classified against live hot coverage.
        holes: Vec<CoverageHole>,
    },
}

/// One partition whose watermark could not advance through its full record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stall {
    /// The stalled partition.
    pub partition: PartitionId,
    /// What blocks it.
    pub reason: StallReason,
}

/// The result of a reconstruction: the recomputed ledger plus everything a
/// recovery operator must know about what did **not** advance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconstruction {
    /// The recomputed authoritative watermark state.
    pub ledger: WatermarkLedger,
    /// Partitions whose watermark stands below their recorded windows, with
    /// the blocking reason. Empty means every recorded window advanced.
    pub stalls: Vec<Stall>,
    /// Manifests beyond a window-id gap, not replayed into the ledger —
    /// the orphan-reconcile candidates (§6.8 step 3). Nothing is silently
    /// dropped.
    pub deferred: Vec<WindowManifest>,
}

impl WatermarkLedger {
    /// Recomputes the authoritative watermark state from the lake's record:
    /// the committed [`WindowManifest`] sequence and the loss ledger, with
    /// live hot coverage (`staging`) classifying any blocking hole. Input
    /// order is irrelevant — manifests are replayed per partition in dense
    /// window order.
    ///
    /// # Errors
    ///
    /// [`ReconstructError`] when the record itself is corrupt (duplicate
    /// window, overlapping coverage, malformed row) — fail closed, never
    /// guess (R-3).
    pub fn reconstruct(
        manifests: Vec<WindowManifest>,
        losses: &[LossLedgerRow],
        staging: &[AppliedWatermarkRow],
    ) -> Result<Reconstruction, ReconstructError> {
        let mut ledger = WatermarkLedger::new();
        for row in losses {
            ledger
                .record_loss(row)
                .map_err(|source| ReconstructError::Loss {
                    partition: row.range.partition.clone(),
                    source,
                })?;
        }

        let mut by_partition: BTreeMap<PartitionId, Vec<WindowManifest>> = BTreeMap::new();
        for manifest in manifests {
            by_partition
                .entry(manifest.partition.clone())
                .or_default()
                .push(manifest);
        }

        let mut stalls = Vec::new();
        let mut deferred = Vec::new();
        for (partition, mut wins) in by_partition {
            wins.sort_by_key(|m| m.window_id.0);
            let mut gap_hit = false;
            for manifest in wins {
                if gap_hit {
                    deferred.push(manifest);
                    continue;
                }
                let expected = ledger.next_window(&partition);
                if manifest.window_id.0 > expected.0 {
                    stalls.push(Stall {
                        partition: partition.clone(),
                        reason: StallReason::MissingWindow { expected },
                    });
                    gap_hit = true;
                    deferred.push(manifest);
                    continue;
                }
                let window = manifest.window_id;
                ledger
                    .record_commit(&manifest)
                    .map_err(|source| ReconstructError::Manifest {
                        partition: partition.clone(),
                        window,
                        source,
                    })?;
            }
            if ledger.advanced_through(&partition) < ledger.recorded_through(&partition) {
                let holes = ledger
                    .blocking_gaps(&partition)
                    .into_iter()
                    .map(|(origin, first_seq, last_seq)| {
                        let in_hot = staging.iter().any(|row| {
                            row.partition == partition
                                && row.origin == origin
                                && row.applied_seq >= last_seq
                        });
                        CoverageHole {
                            origin,
                            first_seq,
                            last_seq,
                            in_hot,
                        }
                    })
                    .collect();
                stalls.push(Stall {
                    partition: partition.clone(),
                    reason: StallReason::CoverageHoles { holes },
                });
            }
        }

        Ok(Reconstruction {
            ledger,
            stalls,
            deferred,
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::testutil::{loss, manifest};

    #[test]
    fn reconstruction_matches_the_live_ledger() {
        let history = vec![
            manifest("p", 0, &[("o1", 1, 5)], 1_000),
            manifest("p", 1, &[("o1", 6, 9), ("o2", 1, 4)], 2_000),
            manifest("p", 2, &[("o2", 5, 8)], 1_500),
        ];
        let mut live = WatermarkLedger::new();
        for m in &history {
            live.record_commit(m).expect("dense history commits");
        }
        // Reversed input: reconstruction orders the record itself.
        let mut shuffled = history;
        shuffled.reverse();
        let rebuilt = WatermarkLedger::reconstruct(shuffled, &[], &[]).expect("reconstructs");
        assert_eq!(rebuilt.ledger, live);
        assert!(rebuilt.stalls.is_empty());
        assert!(rebuilt.deferred.is_empty());
        assert_eq!(rebuilt.ledger.rows(), live.rows());
    }

    #[test]
    fn missing_window_stalls_and_defers_the_rest() {
        let p = PartitionId::new("p");
        let history = vec![
            manifest("p", 0, &[("o1", 1, 5)], 1_000),
            // window 1 lost to the recovery horizon
            manifest("p", 2, &[("o1", 10, 12)], 3_000),
            manifest("p", 3, &[("o1", 13, 14)], 4_000),
        ];
        let rebuilt = WatermarkLedger::reconstruct(history, &[], &[]).expect("reconstructs");
        assert_eq!(rebuilt.ledger.complete_through_ms(&p), Some(1_000));
        assert_eq!(
            rebuilt.stalls,
            vec![Stall {
                partition: p,
                reason: StallReason::MissingWindow {
                    expected: WindowId(1)
                },
            }]
        );
        assert_eq!(
            rebuilt
                .deferred
                .iter()
                .map(|m| m.window_id.0)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "nothing beyond the gap is silently dropped"
        );
    }

    #[test]
    fn coverage_hole_is_classified_against_live_hot_coverage() {
        let p = PartitionId::new("p");
        let history = vec![
            manifest("p", 0, &[("o1", 1, 5)], 1_000),
            manifest("p", 1, &[("o1", 8, 10)], 2_000), // hole: o1 6..=7
        ];
        // Hot still holds o1 through seq 10: the hole is supplement-pending.
        let staging = [AppliedWatermarkRow {
            partition: p.clone(),
            origin: NodeId::new("o1"),
            applied_seq: 10,
        }];
        let rebuilt =
            WatermarkLedger::reconstruct(history.clone(), &[], &staging).expect("reconstructs");
        assert_eq!(rebuilt.ledger.complete_through_ms(&p), Some(1_000));
        assert_eq!(
            rebuilt.stalls,
            vec![Stall {
                partition: p.clone(),
                reason: StallReason::CoverageHoles {
                    holes: vec![CoverageHole {
                        origin: NodeId::new("o1"),
                        first_seq: 6,
                        last_seq: 7,
                        in_hot: true,
                    }]
                },
            }]
        );
        // No live coverage: the same hole is a DeclareLoss candidate.
        let rebuilt = WatermarkLedger::reconstruct(history, &[], &[]).expect("reconstructs");
        let Stall {
            reason: StallReason::CoverageHoles { holes },
            ..
        } = &rebuilt.stalls[0]
        else {
            panic!("expected a coverage-hole stall");
        };
        assert!(!holes[0].in_hot);
    }

    #[test]
    fn ledgered_loss_excuses_the_hole_in_reconstruction() {
        let p = PartitionId::new("p");
        let history = vec![
            manifest("p", 0, &[("o1", 1, 5)], 1_000),
            manifest("p", 1, &[("o1", 8, 10)], 2_000),
        ];
        let losses = [loss("p", "o1", 6, 7)];
        let rebuilt = WatermarkLedger::reconstruct(history, &losses, &[]).expect("reconstructs");
        assert!(rebuilt.stalls.is_empty());
        assert_eq!(rebuilt.ledger.complete_through_ms(&p), Some(2_000));
    }

    #[test]
    fn duplicate_window_in_the_record_fails_closed() {
        let history = vec![
            manifest("p", 0, &[("o1", 1, 5)], 1_000),
            manifest("p", 0, &[("o2", 1, 5)], 1_000),
        ];
        let err = WatermarkLedger::reconstruct(history, &[], &[])
            .expect_err("the §6.6 fence makes a duplicate window impossible");
        assert!(matches!(
            err,
            ReconstructError::Manifest {
                window: WindowId(0),
                source: AdvanceError::WindowNotNext { .. },
                ..
            }
        ));
    }

    // --- the §6.8 equivalence property: live bookkeeping and reconstruction
    // --- are the same function of the record.

    use crate::testutil::build_history;

    proptest! {
        #[test]
        fn live_ledger_equals_reconstruction(
            plans in prop::collection::vec(
                prop::collection::vec(
                    prop::option::of((1u8..=4, 0u8..=3, any::<bool>())),
                    3,
                ),
                1..=6,
            ),
            plans_b in prop::collection::vec(
                prop::collection::vec(
                    prop::option::of((1u8..=4, 0u8..=3, any::<bool>())),
                    3,
                ),
                1..=4,
            ),
            event_maxes in prop::collection::vec(-100i16..=1000, 6),
            dedups in prop::collection::vec(0u8..=3, 6),
        ) {
            let (mut manifests, mut losses) =
                build_history("pa", &plans, &event_maxes, &dedups);
            let (manifests_b, losses_b) =
                build_history("pb", &plans_b, &event_maxes, &dedups);
            manifests.extend(manifests_b);
            losses.extend(losses_b);

            // Live: half the losses land before any commit, half after —
            // the advance rule must not care.
            let split = losses.len() / 2;
            let mut live = WatermarkLedger::new();
            for row in &losses[..split] {
                live.record_loss(row).expect("loss records");
            }
            for m in &manifests {
                live.record_commit(m).expect("dense history commits");
            }
            for row in &losses[split..] {
                live.record_loss(row).expect("loss records");
            }

            // Reconstruction: same record, reversed input order.
            let mut shuffled = manifests;
            shuffled.reverse();
            let rebuilt = WatermarkLedger::reconstruct(shuffled, &losses, &[])
                .expect("an honest record reconstructs");

            prop_assert_eq!(&rebuilt.ledger, &live);
            prop_assert_eq!(rebuilt.ledger.rows(), live.rows());
            prop_assert!(rebuilt.deferred.is_empty());
        }
    }
}
