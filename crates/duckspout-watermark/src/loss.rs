//! `DeclareLoss` (§5.8): the ceremony's pure guard logic over the loss-ledger
//! types.
//!
//! The three wire/domain types below — [`LostRange`], [`DeclareLossRequest`],
//! [`LossLedgerRow`] — were originally sketched here, types-only ("the
//! ceremony's logic ... lands at v0.2 with replication," this module's own
//! prior doc comment). They now live in `duckspout-types`
//! (`duckspout_types::watermark`) instead, re-exported verbatim below: issue
//! #54 makes the ceremony real, and
//! [`duckspout_types::LossLedgerCommitter::commit_loss`] needs
//! [`LossLedgerRow`] in its signature — ADR-0008 requires every type crossing
//! a cross-crate port boundary to live in `duckspout-types`, exactly as
//! [`duckspout_types::StagedCoverage`]/[`duckspout_types::WindowManifest`]
//! already do for their own boundaries. No shape changed, so nothing
//! downstream of these types (`crate::ledger::WatermarkLedger::record_loss`,
//! its own tests, `crate::reconstruct`) needed to change.
//!
//! What is new here (issue #54): [`check_declare_loss`], the ceremony's own
//! two guards, both pure functions of already-gathered inputs —
//! - the literal `accept_data_loss: true` consent (§5.8: "the name is the
//!   consent form");
//! - refusal while any **live** replica still advertises coverage of a
//!   requested range (§5.8: "the ceremony destroys the claim to
//!   completeness, so it must be impossible while completeness is still
//!   recoverable").
//!
//! This crate determines neither "is `accept_data_loss` well-formed input"
//! beyond the boolean itself nor "which nodes are live" — the caller
//! assembles [`duckspout_types::ReplicaCoverage`] from whatever it trusts as
//! live (a registry read, a heartbeat-filtered membership view — §5.6's
//! detection timeline, `duckspout-replication`'s domain, not this crate's).
//! [`check_declare_loss`] is deliberately a pure, dependency-free function
//! over that already-gathered snapshot: `duckspout-watermark` cannot depend
//! on `duckspout-replication` (both protocol crates, ADR-0008), so the
//! ceremony's actual orchestration — gather live coverage, call this guard,
//! then (only if it passes) durably commit through
//! [`duckspout_types::LossLedgerCommitter`] and record locally through
//! [`crate::ledger::WatermarkLedger::record_loss`] — lives above both crates
//! (`duckspout-daemon`/`duckspout-ctl` composition), deliberately deferred
//! here (this module's own PR names the follow-up).

pub use duckspout_types::{DeclareLossRequest, LossLedgerRow, LostRange, ReplicaCoverage};

/// Why [`check_declare_loss`] refused a `DeclareLoss` request. Every variant
/// means nothing was recorded and the watermark has not moved — the
/// ceremony's own all-or-nothing discipline: a single refused range refuses
/// the whole request, never a partial declaration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LossRefusal {
    /// `accept_data_loss` was not the literal `true` — the consent form was
    /// not signed (§5.8).
    #[error("DeclareLoss requires the literal accept_data_loss: true consent")]
    ConsentNotGiven,
    /// The request named no ranges at all — nothing to declare.
    #[error("DeclareLoss request names no ranges")]
    EmptyRequest,
    /// One of the named ranges is malformed: seqs are 1-based and
    /// `first_seq <= last_seq` (matching
    /// [`crate::ledger::WatermarkLedger::record_loss`]'s own validation, but
    /// checked here first so a malformed range is refused before any live-
    /// coverage lookup or partial commit is even attempted).
    #[error(
        "range {index}: invalid seq range {first_seq}..={last_seq} for origin {origin} \
         of partition {partition} (seqs are 1-based, first <= last)"
    )]
    InvalidRange {
        /// The index of the offending range within the request.
        index: usize,
        /// The partition the malformed range was declared against.
        partition: duckspout_types::PartitionId,
        /// The origin the malformed range was declared against.
        origin: duckspout_types::NodeId,
        /// The offered first seq.
        first_seq: u64,
        /// The offered last seq.
        last_seq: u64,
    },
    /// A live replica still advertises coverage of a range this request
    /// asked to declare lost (§5.8's core refusal: "impossible while
    /// completeness is still recoverable").
    #[error(
        "range {index} ({origin} {first_seq}..={last_seq} of partition {partition}) is still \
         recoverable: live replica {covering_node} advertises coverage through seq \
         {covering_thru}"
    )]
    StillRecoverable {
        /// The index of the still-recoverable range within the request.
        index: usize,
        /// The partition the range belongs to.
        partition: duckspout_types::PartitionId,
        /// The origin the range belongs to.
        origin: duckspout_types::NodeId,
        /// The requested first seq.
        first_seq: u64,
        /// The requested last seq.
        last_seq: u64,
        /// The live node whose advertised coverage blocks the declaration.
        covering_node: duckspout_types::NodeId,
        /// The seq that covering node advertises through, for diagnostics.
        covering_thru: u64,
    },
}

/// Checks `request` against `live_coverage` (§5.8's two guards; module
/// docs). Pure: no I/O, no mutation, no clock (D-2) — the caller runs this
/// **before** attempting the durable
/// [`duckspout_types::LossLedgerCommitter::commit_loss`] write, exactly as
/// `duckspout-drain`'s own coordinator runs
/// [`crate::ledger::WatermarkLedger::advance_for`] as a pure preview ahead of
/// its own commit.
///
/// `live_coverage` is scoped to exactly the ranges' partitions by the
/// caller; a `ReplicaCoverage` entry for an unrelated partition is simply
/// never matched (this function reads no partition field off
/// `ReplicaCoverage` itself — module docs explain why: the caller already
/// queried it per-partition).
///
/// A range is "still recoverable" when some entry in `live_coverage` shares
/// its `origin` and advertises `replicated_thru >= last_seq` — i.e. the live
/// replica's own contiguous coverage already reaches (or exceeds) the
/// requested range's upper end. A replica only partially overlapping the
/// range (covering less than `last_seq`) does not, on its own, prove the
/// FULL range recoverable — but per `PeerApply`'s `GapFreedom`
/// (`docs/design/replication.md` §5.4), a replica's advertised
/// `replicated_thru` is by construction a contiguous prefix from seq 1, so
/// `replicated_thru >= last_seq` implies the replica holds every seq in
/// `first_seq..=last_seq`, not merely `last_seq` itself.
///
/// # Errors
///
/// See [`LossRefusal`]'s variants. On refusal, nothing about the request has
/// been recorded anywhere — the ceremony has not begun.
pub fn check_declare_loss(
    request: &DeclareLossRequest,
    live_coverage: &[ReplicaCoverage],
) -> Result<(), LossRefusal> {
    if !request.accept_data_loss {
        return Err(LossRefusal::ConsentNotGiven);
    }
    if request.ranges.is_empty() {
        return Err(LossRefusal::EmptyRequest);
    }
    for (index, range) in request.ranges.iter().enumerate() {
        if range.first_seq == 0 || range.first_seq > range.last_seq {
            return Err(LossRefusal::InvalidRange {
                index,
                partition: range.partition.clone(),
                origin: range.origin.clone(),
                first_seq: range.first_seq,
                last_seq: range.last_seq,
            });
        }
        if let Some(covering) = live_coverage
            .iter()
            .find(|c| c.origin == range.origin && c.replicated_thru >= range.last_seq)
        {
            return Err(LossRefusal::StillRecoverable {
                index,
                partition: range.partition.clone(),
                origin: range.origin.clone(),
                first_seq: range.first_seq,
                last_seq: range.last_seq,
                covering_node: covering.node.clone(),
                covering_thru: covering.replicated_thru,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use duckspout_types::{NodeId, PartitionId};

    use super::*;

    fn range(partition: &str, origin: &str, first_seq: u64, last_seq: u64) -> LostRange {
        LostRange {
            partition: PartitionId::new(partition),
            origin: NodeId::new(origin),
            first_seq,
            last_seq,
        }
    }

    fn request(ranges: Vec<LostRange>, accept_data_loss: bool) -> DeclareLossRequest {
        DeclareLossRequest {
            ranges,
            accept_data_loss,
        }
    }

    #[test]
    fn loss_rows_round_trip_through_serde() {
        let row = LossLedgerRow {
            range: range("t0-s0", "node-a", 6, 7),
            declared_at_ms: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&row).expect("serializes");
        let back: LossLedgerRow = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, row);
    }

    /// The literal consent parameter is required — a request with every
    /// range perfectly valid and unrecoverable is still refused when
    /// `accept_data_loss` is `false`. Would catch a guard that only checks
    /// coverage and ignores consent.
    #[test]
    fn a_request_without_the_literal_consent_is_refused() {
        let req = request(vec![range("p", "o1", 1, 5)], false);
        assert_eq!(
            check_declare_loss(&req, &[]),
            Err(LossRefusal::ConsentNotGiven)
        );
    }

    /// An empty range list is refused outright — nothing to declare, and a
    /// silent no-op success would be a confusing ceremony outcome.
    #[test]
    fn an_empty_request_is_refused() {
        let req = request(vec![], true);
        assert_eq!(
            check_declare_loss(&req, &[]),
            Err(LossRefusal::EmptyRequest)
        );
    }

    /// With consent given and no live coverage anywhere, an otherwise valid
    /// request is accepted — the ceremony's positive path.
    #[test]
    fn an_unrecoverable_request_with_consent_is_accepted() {
        let req = request(vec![range("p", "o1", 6, 7)], true);
        assert_eq!(check_declare_loss(&req, &[]), Ok(()));
    }

    /// A live replica whose advertised `replicated_thru` reaches the
    /// requested range's upper end blocks the declaration — §5.8's core
    /// refusal. Would catch a guard that never actually consults
    /// `live_coverage`.
    #[test]
    fn a_range_still_covered_by_a_live_replica_is_refused() {
        let req = request(vec![range("p", "o1", 6, 7)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            origin: NodeId::new("o1"),
            replicated_thru: 10,
        }];
        assert_eq!(
            check_declare_loss(&req, &coverage),
            Err(LossRefusal::StillRecoverable {
                index: 0,
                partition: PartitionId::new("p"),
                origin: NodeId::new("o1"),
                first_seq: 6,
                last_seq: 7,
                covering_node: NodeId::new("replica-a"),
                covering_thru: 10,
            })
        );
    }

    /// A live replica that only PARTIALLY covers the requested range (its
    /// `replicated_thru` falls short of `last_seq`) does not block the
    /// declaration — the range genuinely is not fully recoverable from that
    /// replica alone. Would catch an off-by-one or `>` vs `>=` inversion
    /// that either over- or under-refuses.
    #[test]
    fn a_replica_short_of_the_requested_range_does_not_block_it() {
        let req = request(vec![range("p", "o1", 6, 10)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            origin: NodeId::new("o1"),
            replicated_thru: 8,
        }];
        assert_eq!(check_declare_loss(&req, &coverage), Ok(()));
    }

    /// Coverage for a DIFFERENT origin never blocks a declaration — the
    /// refusal check is scoped per origin, matching gap-freedom's own
    /// per-`(origin, partition)` scoping elsewhere in this codebase.
    #[test]
    fn coverage_of_a_different_origin_does_not_block_the_declaration() {
        let req = request(vec![range("p", "o1", 1, 5)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            origin: NodeId::new("o2"),
            replicated_thru: 100,
        }];
        assert_eq!(check_declare_loss(&req, &coverage), Ok(()));
    }

    /// A malformed range (0-based, or inverted) is refused before any live-
    /// coverage lookup — matching
    /// `WatermarkLedger::record_loss`'s own validation, but caught here
    /// first so the ceremony never even reaches the commit attempt.
    #[test]
    fn a_malformed_range_is_refused_before_any_coverage_check() {
        let req = request(vec![range("p", "o1", 0, 5)], true);
        assert_eq!(
            check_declare_loss(&req, &[]),
            Err(LossRefusal::InvalidRange {
                index: 0,
                partition: PartitionId::new("p"),
                origin: NodeId::new("o1"),
                first_seq: 0,
                last_seq: 5,
            })
        );

        let req = request(vec![range("p", "o1", 5, 3)], true);
        assert!(matches!(
            check_declare_loss(&req, &[]),
            Err(LossRefusal::InvalidRange {
                first_seq: 5,
                last_seq: 3,
                ..
            })
        ));
    }

    /// A multi-range request is refused as a whole the moment ANY one range
    /// is still recoverable — the ceremony is all-or-nothing, never a
    /// partial declaration of "the ranges that happened to be safe."
    #[test]
    fn one_still_recoverable_range_refuses_the_whole_multi_range_request() {
        let req = request(vec![range("p", "o1", 1, 5), range("p", "o2", 1, 5)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            origin: NodeId::new("o2"),
            replicated_thru: 5,
        }];
        assert!(matches!(
            check_declare_loss(&req, &coverage),
            Err(LossRefusal::StillRecoverable { index: 1, .. })
        ));
    }
}
