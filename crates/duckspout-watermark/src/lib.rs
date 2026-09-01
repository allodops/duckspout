//! Watermark ledger logic (§6.8, §7) over the row types owned by
//! `duckspout-types` ([`WatermarkRow`],
//! [`duckspout_types::AppliedWatermarkRow`]).
//!
//! Watermarks are the only registry state that matters (§6.8):
//! `complete_through` advances only via a window's own drain (riding
//! `LakeCommit` atomically, §6.4) or the `DeclareLoss` ceremony (§5.8), and
//! is authoritative-but-reconstructible from manifest contiguity plus live
//! hot coverage (§6.8).
//!
//! Ⓢ bootstrap stub — advance/reconstruction logic lands at v0.1. This crate
//! is pure ledger logic; persistence is the committer's transaction, never
//! this crate's I/O.
//!
//! Layering (§10.1, ADR-0008): depends on `duckspout-types` only among
//! workspace crates.
//!
//! Design home: `docs/design/drain.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §6.8).

#![forbid(unsafe_code)]

use duckspout_types::{PartitionId, WatermarkRow};

/// An in-memory view of committed watermark state. Ⓢ v0.1: the advance rule
/// (window contiguity), `DeclareLoss` annotations, and reconstruction land
/// with the drain.
#[derive(Debug, Default)]
pub struct WatermarkLedger {
    rows: Vec<WatermarkRow>,
}

impl WatermarkLedger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads the last committed rows, as read back through
    /// `LakeCommitter::read_watermarks` (§6.4).
    #[must_use]
    pub fn from_rows(rows: Vec<WatermarkRow>) -> Self {
        Self { rows }
    }

    /// The committed `complete_through` for a partition, if any (§7: a range
    /// at or below it is lake-served and never touches hot).
    #[must_use]
    pub fn complete_through_ms(&self, partition: &PartitionId) -> Option<i64> {
        self.rows
            .iter()
            .find(|row| &row.partition == partition)
            .map(|row| row.complete_through_ms)
    }

    /// The rows in this view.
    #[must_use]
    pub fn rows(&self) -> &[WatermarkRow] {
        &self.rows
    }
}
