//! Loss-ledger annotation types, in the §5.8 `DeclareLoss` shape.
//!
//! Types only at v0.1: the ceremony's logic — refusal while any live replica
//! still advertises coverage, the atomic ledger-row + watermark-advance
//! catalog transaction — lands at v0.2 with replication. The shapes are
//! fixed now so the loss ledger's wire and storage form never needs a
//! migration when the ceremony arms.

use serde::{Deserialize, Serialize};

use duckspout_types::{NodeId, PartitionId};

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
    /// The v0.2 ceremony refuses any request where this is not `true`; it is
    /// a field, not a default.
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

#[cfg(test)]
mod tests {
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
