//! `Receipt` bookkeeping and total-inclusive RF `ClientAck` gating
//! (§4, §5.1, §5.4).
//!
//! `docs/design/replication.md` §5.1: `cluster.rf` is **total-inclusive** —
//! "RF counts every durable copy of an acked record, including the copy on
//! the node that will own the drain." RF=2 means the origin's own fsynced
//! copy plus **one** receipted peer, not two peers beyond the origin. This
//! module's [`client_ack_ready`] is exactly that arithmetic: the origin's
//! local commit always counts as the first of `rf` copies, so `ClientAck`
//! needs `rf - 1` *additional*, distinct, sufficiently-caught-up peers —
//! never `rf` peers on top of the origin.
//!
//! [`ReceiptTracker`] is the origin-side mirror of
//! `specs/DuckSpoutCore.tla`'s `receipts` variable, generalized to the
//! cumulative-watermark shape §4 itself specifies for the real wire
//! protocol ("Receipts are cumulative acknowledgments — one number, no
//! per-batch bookkeeping, retransmit-safe") rather than TLA+'s
//! easier-to-state-as-an-invariant per-record receipt set.

use std::collections::HashMap;

use duckspout_types::{NodeId, PartitionId};

use crate::fencing::{FenceIdentity, FenceOutcome, FenceTable};
use crate::wire::ReceiptMessage;

/// The result of recording one inbound [`ReceiptMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptOutcome {
    /// The receipt passed fencing; its watermark is recorded (raised, if
    /// higher than what was already on file for this `(holder, origin,
    /// partition)`).
    Recorded,
    /// The receipt's incarnation was strictly below the highest already
    /// accepted from this holder — a zombie (`FencedZombie`); the watermark
    /// is unchanged. A partitioned former self's stale receipt must never
    /// be allowed to (dis)count toward `ClientAck`.
    Fenced,
}

/// Origin-side receipt bookkeeping: per `(holder, origin, partition)`, the
/// highest contiguous seq that holder has reported durably applied, plus
/// the §5.7 fencing this crate applies to every message uniformly.
///
/// Keyed by `origin` (not assumed to always be "this node") so one tracker
/// can, in principle, serve a node acting as the origin for more than one
/// partition, or observing receipts it is not itself the origin of (a
/// catch-up scenario) — `client_ack_ready` is the query that actually
/// matters for this node's own `ClientAck` decisions.
#[derive(Debug, Default)]
pub struct ReceiptTracker {
    fence: FenceTable,
    watermarks: HashMap<(NodeId, NodeId, PartitionId), u64>,
}

impl ReceiptTracker {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `receipt`, fencing it first (§5.7 — every message, `Receipt`
    /// included, carries `(node_id, incarnation)` and is checked against the
    /// highest incarnation already seen from that sender). A receipt whose
    /// watermark is **lower** than what is already on file for the same
    /// `(holder, origin, partition)` (an out-of-order-delivered stale
    /// receipt) never regresses the recorded watermark — cumulative
    /// acknowledgments only ever advance (§4).
    pub fn record(&mut self, receipt: ReceiptMessage) -> ReceiptOutcome {
        let identity = FenceIdentity {
            node: receipt.holder.clone(),
            incarnation: receipt.incarnation,
        };
        if matches!(self.fence.admit(&identity), FenceOutcome::Zombie { .. }) {
            return ReceiptOutcome::Fenced;
        }
        let key = (receipt.holder, receipt.origin, receipt.partition);
        let watermark = self.watermarks.entry(key).or_insert(0);
        *watermark = (*watermark).max(receipt.applied_thru);
        ReceiptOutcome::Recorded
    }

    /// The highest contiguous seq `holder` has reported applied for
    /// `(origin, partition)` — `0` if no receipt has ever been recorded.
    #[must_use]
    pub fn holder_applied_thru(
        &self,
        holder: &NodeId,
        origin: &NodeId,
        partition: &PartitionId,
    ) -> u64 {
        self.watermarks
            .get(&(holder.clone(), origin.clone(), partition.clone()))
            .copied()
            .unwrap_or(0)
    }

    /// The number of **distinct** holders whose recorded watermark for
    /// `(origin, partition)` is at or past `thru_seq` — exactly `AtRF`'s
    /// `H`/`Cardinality({rc.holder : ...})` computation
    /// (`specs/DuckSpoutCore.tla`), minus the origin itself (the origin is
    /// never a "holder" entry in this tracker).
    #[must_use]
    pub fn receipted_peer_count(
        &self,
        origin: &NodeId,
        partition: &PartitionId,
        thru_seq: u64,
    ) -> usize {
        self.watermarks
            .iter()
            .filter(|((_, o, p), watermark)| {
                o == origin && p == partition && **watermark >= thru_seq
            })
            .count()
    }
}

/// Total-inclusive RF `ClientAck` gating (§5.1, §4.3): whether a locally
/// staged range up to `last_seq` for `(origin, partition)` has enough
/// receipted coverage to ack the client, at replication factor `rf`.
///
/// **Total-inclusive**: the origin's own durable commit is the first of
/// `rf` copies, so this needs `rf.saturating_sub(1)` *additional* distinct,
/// sufficiently-caught-up peers — matching `AtRF`/`ClientAck`'s own `H`
/// computation (`{r.origin} \cup {rc.holder : ...} >= RF`,
/// `specs/DuckSpoutCore.tla`) and `docs/design/replication.md` §5.1's own
/// worked example ("RF=2 means the origin's fsynced copy plus one replica
/// receipt"). At `rf = 1` this is vacuously always ready — the origin's own
/// commit already **is** the complete ack evidence, matching
/// `duckspout-staging`'s existing `AT_RF_V01` constant for the
/// pre-replication case.
///
/// This is the exact predicate `StageOutcome::DuplicateInFlight`'s own doc
/// comment anticipates ("unreachable at RF = 1 ... the branch exists
/// because §3's `DedupCheck` has it, and replication (v0.2) makes it live"):
/// wiring `EngineStager`'s `at_rf` parameter to call through to this
/// function for `rf > 1` is daemon-composition work, tracked alongside
/// [`duckspout_types::ReplicaLog`]'s concrete `duckspout-staging`
/// implementation in issue #193 (this crate cannot make that call itself —
/// `duckspout-staging` depending on `duckspout-replication` would be a
/// forbidden protocol×protocol edge, ADR-0008).
#[must_use]
pub fn client_ack_ready(
    receipts: &ReceiptTracker,
    origin: &NodeId,
    partition: &PartitionId,
    last_seq: u64,
    rf: u32,
) -> bool {
    let peers_needed = rf.saturating_sub(1);
    receipts.receipted_peer_count(origin, partition, last_seq) as u64 >= u64::from(peers_needed)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::fencing::Incarnation;

    fn receipt(holder: &str, origin: &str, partition: &str, inc: u64, thru: u64) -> ReceiptMessage {
        ReceiptMessage {
            incarnation: Incarnation(inc),
            holder: NodeId::new(holder),
            origin: NodeId::new(origin),
            partition: PartitionId::new(partition),
            applied_thru: thru,
        }
    }

    /// RF=1 never waits on a peer receipt: the origin's own commit already
    /// is the complete ack evidence, regardless of peer state — this is the
    /// v0.1 behavior `duckspout-staging`'s `AT_RF_V01` constant already
    /// hard-codes, and this crate must not regress it once RF becomes
    /// configurable.
    #[test]
    fn rf_one_is_always_ack_ready() {
        let receipts = ReceiptTracker::new();
        assert!(client_ack_ready(
            &receipts,
            &NodeId::new("origin-1"),
            &PartitionId::new("p0"),
            999,
            1
        ));
    }

    /// RF=2 (total-inclusive, §5.1's worked example): the origin's own copy
    /// plus exactly ONE receipted peer suffices — a second peer receipt is
    /// not required. Would catch an additive (non-total-inclusive)
    /// off-by-one that demanded RF peers on top of the origin.
    #[test]
    fn rf_two_is_ready_after_exactly_one_peer_receipt() {
        let mut receipts = ReceiptTracker::new();
        let origin = NodeId::new("origin-1");
        let partition = PartitionId::new("p0");
        assert!(!client_ack_ready(&receipts, &origin, &partition, 10, 2));

        assert_eq!(
            receipts.record(receipt("replica-a", "origin-1", "p0", 1, 10)),
            ReceiptOutcome::Recorded
        );
        assert!(client_ack_ready(&receipts, &origin, &partition, 10, 2));
    }

    /// RF=3 needs TWO distinct receipted peers, not one repeated twice —
    /// would catch a count that double-counts retransmits from the same
    /// holder instead of counting distinct holders.
    #[test]
    fn rf_three_needs_two_distinct_peers_not_one_peer_twice() {
        let mut receipts = ReceiptTracker::new();
        let origin = NodeId::new("origin-1");
        let partition = PartitionId::new("p0");

        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 10));
        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 10)); // retransmit
        assert!(
            !client_ack_ready(&receipts, &origin, &partition, 10, 3),
            "one peer, receipted twice, must not count as two"
        );

        receipts.record(receipt("replica-b", "origin-1", "p0", 1, 10));
        assert!(client_ack_ready(&receipts, &origin, &partition, 10, 3));
    }

    /// A peer whose watermark has not yet reached `last_seq` does not count
    /// — `ClientAck` needs coverage of the SPECIFIC range being acked, not
    /// merely "some receipt exists."
    #[test]
    fn a_peer_short_of_the_needed_seq_does_not_count() {
        let mut receipts = ReceiptTracker::new();
        let origin = NodeId::new("origin-1");
        let partition = PartitionId::new("p0");
        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 5));
        assert!(!client_ack_ready(&receipts, &origin, &partition, 10, 2));
        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 10));
        assert!(client_ack_ready(&receipts, &origin, &partition, 10, 2));
    }

    /// A zombie (fenced) receipt must not raise the recorded watermark —
    /// `FencedZombie` applied to the receipt side of the protocol: a
    /// partitioned former self's stale receipt cannot manufacture ack
    /// evidence for a range it was never actually caught up on.
    #[test]
    fn a_fenced_receipt_does_not_advance_the_watermark() {
        let mut receipts = ReceiptTracker::new();
        assert_eq!(
            receipts.record(receipt("replica-a", "origin-1", "p0", 2, 10)),
            ReceiptOutcome::Recorded
        );
        // A stale incarnation, even claiming a HIGHER watermark, is fenced.
        assert_eq!(
            receipts.record(receipt("replica-a", "origin-1", "p0", 1, 999)),
            ReceiptOutcome::Fenced
        );
        assert_eq!(
            receipts.holder_applied_thru(
                &NodeId::new("replica-a"),
                &NodeId::new("origin-1"),
                &PartitionId::new("p0")
            ),
            10
        );
    }

    /// An out-of-order-delivered receipt under the SAME (non-stale)
    /// incarnation, reporting a lower watermark than one already recorded,
    /// must not regress it — cumulative acknowledgments only ever advance.
    #[test]
    fn watermark_never_regresses_under_the_same_incarnation() {
        let mut receipts = ReceiptTracker::new();
        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 10));
        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 4));
        assert_eq!(
            receipts.holder_applied_thru(
                &NodeId::new("replica-a"),
                &NodeId::new("origin-1"),
                &PartitionId::new("p0")
            ),
            10
        );
    }

    /// Receipt bookkeeping is scoped per `(origin, partition)`: a peer's
    /// coverage of one partition never counts toward another partition's
    /// (or another origin's) `ClientAck` decision.
    #[test]
    fn receipts_are_scoped_per_origin_and_partition() {
        let mut receipts = ReceiptTracker::new();
        receipts.record(receipt("replica-a", "origin-1", "p0", 1, 10));
        assert!(!client_ack_ready(
            &receipts,
            &NodeId::new("origin-1"),
            &PartitionId::new("p1"),
            10,
            2
        ));
        assert!(!client_ack_ready(
            &receipts,
            &NodeId::new("origin-2"),
            &PartitionId::new("p0"),
            10,
            2
        ));
    }

    proptest! {
        /// §8.5-style law: for ANY RF and ANY number of distinct
        /// sufficiently-caught-up peers, `client_ack_ready` agrees exactly
        /// with the total-inclusive arithmetic (`1 + peers >= rf`) —
        /// checked against a directly-computed ground truth, not by
        /// re-deriving the same formula the implementation uses.
        #[test]
        fn client_ack_ready_matches_total_inclusive_arithmetic(
            rf in 1u32..6,
            caught_up_peers in 0usize..6,
            extra_short_peers in 0usize..4,
        ) {
            let mut receipts = ReceiptTracker::new();
            let origin = NodeId::new("origin-1");
            let partition = PartitionId::new("p0");
            for i in 0..caught_up_peers {
                receipts.record(receipt(&format!("caught-up-{i}"), "origin-1", "p0", 1, 100));
            }
            for i in 0..extra_short_peers {
                receipts.record(receipt(&format!("short-{i}"), "origin-1", "p0", 1, 5));
            }
            let expected = 1 + caught_up_peers >= rf as usize;
            prop_assert_eq!(
                client_ack_ready(&receipts, &origin, &partition, 100, rf),
                expected
            );
        }
    }
}
