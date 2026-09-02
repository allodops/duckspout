//! The [`WatermarkBookkeeping`] port, implemented over [`WatermarkLedger`]
//! (ADR-0008: the trait lives in `duckspout-types`; this crate owns the
//! implementation and everything beyond the bare signature).
//!
//! The seam exists because `duckspout-drain` and this crate are both
//! protocol crates — a direct edge is banned — yet the drain must carry the
//! computed watermark rows on `commit_files` (§6.4, ADR-0010). The daemon
//! composes the two: it owns a [`SharedLedger`] and hands the drain a
//! `dyn WatermarkBookkeeping`.

use std::sync::Mutex;

use duckspout_types::{
    LedgerRejection, OriginSeqRange, PartitionId, WatermarkBookkeeping, WatermarkRow, WindowId,
    WindowManifest,
};

use crate::ledger::{AdvanceError, WatermarkLedger};

/// A [`WatermarkLedger`] behind the [`WatermarkBookkeeping`] port: interior
/// synchronization so the port can take `&self` (the ledger itself is pure
/// state, no I/O).
#[derive(Debug, Default)]
pub struct SharedLedger {
    inner: Mutex<WatermarkLedger>,
}

impl SharedLedger {
    /// Wraps a ledger — typically one freshly rebuilt from the lake's
    /// manifest record at boot (§6.8), so the bookkeeping resumes exactly
    /// where the last durable commit left it.
    #[must_use]
    pub fn new(ledger: WatermarkLedger) -> Self {
        Self {
            inner: Mutex::new(ledger),
        }
    }

    /// A snapshot of the wrapped ledger.
    ///
    /// # Panics
    ///
    /// If a previous lock holder panicked mid-hold. The ledger mutations
    /// validate everything before mutating (the ledger is unchanged on
    /// every error), so the state itself is still consistent when that
    /// happens — but failing loud beats guessing (R-3).
    #[must_use]
    pub fn snapshot(&self) -> WatermarkLedger {
        self.lock().clone()
    }

    /// Panics on a poisoned lock — see [`SharedLedger::snapshot`]'s
    /// `# Panics` note, which applies to every method here.
    fn lock(&self) -> std::sync::MutexGuard<'_, WatermarkLedger> {
        self.inner.lock().expect("watermark ledger lock poisoned")
    }
}

impl From<WatermarkLedger> for SharedLedger {
    fn from(ledger: WatermarkLedger) -> Self {
        Self::new(ledger)
    }
}

/// Projects the ledger's rich rejection onto the port's two-way split: the
/// dense-next fence (which the drain acts on) versus everything else (a
/// caller bug, rendered).
fn rejection(error: AdvanceError) -> LedgerRejection {
    match error {
        AdvanceError::WindowNotNext {
            partition,
            expected,
            got,
        } => LedgerRejection::WindowNotNext {
            partition,
            expected,
            got,
        },
        other => LedgerRejection::Rejected(other.to_string()),
    }
}

impl WatermarkBookkeeping for SharedLedger {
    fn next_window(&self, partition: &PartitionId) -> WindowId {
        self.lock().next_window(partition)
    }

    fn complete_through_ms(&self, partition: &PartitionId) -> Option<i64> {
        self.lock().complete_through_ms(partition)
    }

    fn advance_for(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, LedgerRejection> {
        self.lock().advance_for(manifest).map_err(rejection)
    }

    fn record_commit(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, LedgerRejection> {
        self.lock().record_commit(manifest).map_err(rejection)
    }

    fn recorded_coverage(
        &self,
        partition: &PartitionId,
        window: WindowId,
    ) -> Option<Vec<OriginSeqRange>> {
        self.lock()
            .window_record(partition, window)
            .map(|record| record.origin_coverage.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::manifest;

    #[test]
    fn port_delegates_to_the_ledger() {
        let shared = SharedLedger::default();
        let p = PartitionId::new("p");
        assert_eq!(shared.next_window(&p), WindowId(0));
        assert_eq!(shared.complete_through_ms(&p), None);

        let m0 = manifest("p", 0, &[("o1", 1, 5)], 1_000);
        let previewed = shared.advance_for(&m0).expect("preview computes");
        assert_eq!(shared.next_window(&p), WindowId(0), "advance_for is pure");
        let recorded = shared.record_commit(&m0).expect("commit records");
        assert_eq!(previewed, recorded);
        assert_eq!(shared.next_window(&p), WindowId(1));
        assert_eq!(shared.complete_through_ms(&p), Some(1_000));
        assert_eq!(shared.snapshot().next_window(&p), WindowId(1));
        assert_eq!(
            shared.recorded_coverage(&p, WindowId(0)),
            Some(m0.origin_coverage),
            "the TN-32 drop guard reads back exactly the recorded coverage"
        );
        assert_eq!(shared.recorded_coverage(&p, WindowId(1)), None);
    }

    #[test]
    fn replay_maps_onto_the_port_fence_variant() {
        let shared = SharedLedger::default();
        shared
            .record_commit(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect("window 0 records");
        let err = shared
            .advance_for(&manifest("p", 0, &[("o1", 1, 5)], 1_000))
            .expect_err("a replay is not dense-next");
        assert_eq!(
            err,
            LedgerRejection::WindowNotNext {
                partition: PartitionId::new("p"),
                expected: WindowId(1),
                got: WindowId(0),
            }
        );
    }

    #[test]
    fn malformed_manifests_map_onto_rejected() {
        let shared = SharedLedger::default();
        let err = shared
            .record_commit(&manifest("p", 0, &[("o1", 0, 5)], 1_000))
            .expect_err("seqs are 1-based");
        assert!(matches!(err, LedgerRejection::Rejected(_)));
    }
}
