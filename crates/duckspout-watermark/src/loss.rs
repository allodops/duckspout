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
/// `live_coverage` may freely mix entries for partitions other than the
/// ones named in `request.ranges` (a caller's live-coverage snapshot need
/// not be pre-filtered) — this function matches every entry on
/// `(partition, origin)` itself (ACPR #199 HIGH-1: an earlier revision
/// matched on `origin` alone, trusting the caller to have pre-scoped
/// `live_coverage` to the request's partitions; `ReplicaCoverage` then
/// carried no partition field at all, so a multi-partition request whose
/// caller happened to supply coverage for only SOME of its partitions was
/// silently approved for ranges in the unscoped partitions even when a live
/// replica there fully covered them — the guard had nothing to notice the
/// mismatch with).
///
/// A range is "still recoverable" when the UNION of every live replica's
/// advertised coverage for its exact `(partition, origin)` reaches any seq
/// within `first_seq..=last_seq` — not merely when a SINGLE replica's
/// `replicated_thru` covers the range's entire upper end (ACPR #199
/// HIGH-2: an earlier revision refused only on that narrower condition,
/// which under-refused in two ways: (a) a live replica covering a STRICT
/// SUBRANGE of the declared range — e.g. holding seqs 6..=8 of a declared
/// 6..=10 — proves 6..=8 still recoverable, so the whole declaration must
/// be refused rather than silently approved wholesale; the operator must
/// re-submit a range narrowed to the genuinely unrecoverable part, exactly
/// as any other still-recoverable range is refused today; (b) coverage
/// assembled from more than one live replica, each covering only PART of
/// the range, previously blocked nothing at all, since the old guard only
/// ever asked whether one single entry alone reached `last_seq`).
///
/// Computing the union is simplified by an invariant `PeerApply`'s
/// `GapFreedom` (`docs/design/replication.md` §5.4) guarantees: a replica's
/// advertised `replicated_thru` is by construction a contiguous prefix from
/// seq 1 (never a mid-stream range), so any one replica's coverage is
/// exactly `1..=replicated_thru` and the UNION of several replicas'
/// coverage for the same `(partition, origin)` is exactly `1..=` their
/// **maximum** `replicated_thru` — the shorter prefixes contribute nothing
/// a longer one doesn't already include. So refusal reduces to: take the
/// highest `replicated_thru` any live entry advertises for the range's
/// `(partition, origin)`; the range is still recoverable, and the whole
/// request refused, whenever that maximum is `>= first_seq` (i.e. the union
/// prefix reaches into the declared range at all, not necessarily past its
/// end).
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
        // The union of every live replica's prefix coverage for this exact
        // (partition, origin) is the single highest replicated_thru any of
        // them advertises (module docs: each is a prefix from seq 1, so the
        // shorter ones are subsumed). Refuse the moment that union reaches
        // INTO the declared range at all -- not only past its far end.
        if let Some(covering) = live_coverage
            .iter()
            .filter(|c| c.partition == range.partition && c.origin == range.origin)
            .max_by_key(|c| c.replicated_thru)
            && covering.replicated_thru >= range.first_seq
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

    // `loss_rows_round_trip_through_serde` lives in
    // `duckspout_types::watermark` only (ACPR #199 LOW-8(a)): this module
    // re-exports `LossLedgerRow` verbatim with no shape change, so the same
    // serde round-trip test existing here too was a pure duplicate, not a
    // second thing being tested.

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
            partition: PartitionId::new("p"),
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

    /// A live replica whose coverage does not reach INTO the requested range
    /// at all (its `replicated_thru` falls short of `first_seq`, not just
    /// `last_seq`) does not block the declaration — genuinely nothing about
    /// this range is recoverable from that replica. Would catch an
    /// off-by-one or `>` vs `>=` inversion that either over- or
    /// under-refuses at the range's near end.
    #[test]
    fn a_replica_entirely_short_of_the_requested_range_does_not_block_it() {
        let req = request(vec![range("p", "o1", 6, 10)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            partition: PartitionId::new("p"),
            origin: NodeId::new("o1"),
            replicated_thru: 5,
        }];
        assert_eq!(check_declare_loss(&req, &coverage), Ok(()));
    }

    /// ACPR #199 HIGH-2 scratch-repro re-verification: a live replica
    /// covering a STRICT SUBRANGE of the declared range (holds through seq
    /// 8 of a declared 6..=10) must refuse the declaration — seqs 6..=8 are
    /// still genuinely recoverable from that replica, even though it does
    /// not cover the range's full upper end. Before the fix, this exact
    /// scenario (`a_replica_short_of_the_requested_range_does_not_block_it`)
    /// was asserted `Ok(())` — the wrong, permissive behavior this test
    /// replaces.
    #[test]
    fn a_replica_covering_only_a_subrange_still_refuses_the_whole_range() {
        let req = request(vec![range("p", "o1", 6, 10)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            partition: PartitionId::new("p"),
            origin: NodeId::new("o1"),
            replicated_thru: 8,
        }];
        assert_eq!(
            check_declare_loss(&req, &coverage),
            Err(LossRefusal::StillRecoverable {
                index: 0,
                partition: PartitionId::new("p"),
                origin: NodeId::new("o1"),
                first_seq: 6,
                last_seq: 10,
                covering_node: NodeId::new("replica-a"),
                covering_thru: 8,
            })
        );
    }

    /// ACPR #199 HIGH-2: the UNION of two live replicas, each covering only
    /// PART of the declared range on its own, still refuses — the guard
    /// must consider every matching entry, not just the first or a single
    /// one. Replica A's prefix (1..=7) covers the range's near end;
    /// replica B's longer prefix (1..=9) covers nearly all of it; neither
    /// alone reaches `last_seq` (10), but the higher of the two
    /// (`replicated_thru = 9`) still reaches into the range at seq 6, so
    /// the declaration must be refused citing that replica.
    #[test]
    fn the_union_of_multiple_partial_replicas_still_refuses() {
        let req = request(vec![range("p", "o1", 6, 10)], true);
        let coverage = [
            ReplicaCoverage {
                node: NodeId::new("replica-a"),
                partition: PartitionId::new("p"),
                origin: NodeId::new("o1"),
                replicated_thru: 7,
            },
            ReplicaCoverage {
                node: NodeId::new("replica-b"),
                partition: PartitionId::new("p"),
                origin: NodeId::new("o1"),
                replicated_thru: 9,
            },
        ];
        assert_eq!(
            check_declare_loss(&req, &coverage),
            Err(LossRefusal::StillRecoverable {
                index: 0,
                partition: PartitionId::new("p"),
                origin: NodeId::new("o1"),
                first_seq: 6,
                last_seq: 10,
                covering_node: NodeId::new("replica-b"),
                covering_thru: 9,
            })
        );
    }

    /// Coverage for a DIFFERENT origin never blocks a declaration — the
    /// refusal check is scoped per origin, matching gap-freedom's own
    /// per-`(origin, partition)` scoping elsewhere in this codebase.
    #[test]
    fn coverage_of_a_different_origin_does_not_block_the_declaration() {
        let req = request(vec![range("p", "o1", 1, 5)], true);
        let coverage = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            partition: PartitionId::new("p"),
            origin: NodeId::new("o2"),
            replicated_thru: 100,
        }];
        assert_eq!(check_declare_loss(&req, &coverage), Ok(()));
    }

    /// ACPR #199 HIGH-1 scratch-repro re-verification. The same origin
    /// (accepting node `o1`) writes to two DIFFERENT partitions, each with
    /// its OWN independent `(partition, origin)` seq counter (§4.2.4) — so
    /// `o1`'s true `replicated_thru` genuinely differs between `p1` (a low
    /// value) and `p2` (a high one). Before `ReplicaCoverage` carried a
    /// `partition` field, a caller had no way to represent BOTH true values
    /// in one snapshot at all: any assembly keyed on origin alone (the only
    /// key the old type offered) could hold at most one `replicated_thru`
    /// per origin, so gathering live coverage for a multi-partition request
    /// meant one partition's true value silently overwrote the other's.
    /// Reproducing that exact collapse — `live_coverage` holding only `p1`'s
    /// low true value under `o1`, `p2`'s high true value lost — the guard
    /// used to see `thru = 3` for BOTH ranges and wrongly approve declaring
    /// `p2`'s genuinely-still-recoverable `50..=60` lost (`thru = 3` never
    /// reaches anywhere near it). With `partition` on the type, the SAME
    /// live snapshot can now hold both true values distinctly and the guard
    /// refuses both ranges correctly.
    #[test]
    fn a_shared_origin_across_two_partitions_no_longer_collapses_to_one_thru() {
        let req = request(
            vec![range("p1", "o1", 1, 3), range("p2", "o1", 50, 60)],
            true,
        );

        // The pre-fix collapse: only p1's true value survives (whichever
        // partition's coverage a same-origin-keyed assembly happened to
        // write last), p2's is gone from the snapshot entirely.
        let collapsed = [ReplicaCoverage {
            node: NodeId::new("replica-a"),
            partition: PartitionId::new("p1"),
            origin: NodeId::new("o1"),
            replicated_thru: 3,
        }];
        assert_eq!(
            check_declare_loss(&req, &collapsed),
            Err(LossRefusal::StillRecoverable {
                index: 0,
                partition: PartitionId::new("p1"),
                origin: NodeId::new("o1"),
                first_seq: 1,
                last_seq: 3,
                covering_node: NodeId::new("replica-a"),
                covering_thru: 3,
            }),
            "p1's own true coverage must still refuse p1's range"
        );

        // The fix: both partitions' true coverage coexist in one snapshot,
        // each correctly scoped by the new `partition` field.
        let both = [
            ReplicaCoverage {
                node: NodeId::new("replica-a"),
                partition: PartitionId::new("p1"),
                origin: NodeId::new("o1"),
                replicated_thru: 3,
            },
            ReplicaCoverage {
                node: NodeId::new("replica-b"),
                partition: PartitionId::new("p2"),
                origin: NodeId::new("o1"),
                replicated_thru: 100,
            },
        ];
        assert_eq!(
            check_declare_loss(&req, &both),
            Err(LossRefusal::StillRecoverable {
                index: 0,
                partition: PartitionId::new("p1"),
                origin: NodeId::new("o1"),
                first_seq: 1,
                last_seq: 3,
                covering_node: NodeId::new("replica-a"),
                covering_thru: 3,
            }),
            "the request is refused as a whole at the first still-recoverable \
             range (p1, all-or-nothing) -- p2's own refusal is checked next"
        );
        // Isolate p2 alone to prove its now-representable true coverage (a
        // value that literally could not coexist with p1's in the old,
        // origin-only-keyed type) is itself correctly consulted and refuses
        // the declaration -- this is the exact false negative the ACPR
        // scratch test found: before the fix, this value was structurally
        // unrepresentable alongside p1's and was lost, wrongly approving
        // p2's still-fully-recoverable 50..=60.
        let p2_only_request = request(vec![range("p2", "o1", 50, 60)], true);
        assert_eq!(
            check_declare_loss(&p2_only_request, &both),
            Err(LossRefusal::StillRecoverable {
                index: 0,
                partition: PartitionId::new("p2"),
                origin: NodeId::new("o1"),
                first_seq: 50,
                last_seq: 60,
                covering_node: NodeId::new("replica-b"),
                covering_thru: 100,
            })
        );
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
            partition: PartitionId::new("p"),
            origin: NodeId::new("o2"),
            replicated_thru: 5,
        }];
        assert!(matches!(
            check_declare_loss(&req, &coverage),
            Err(LossRefusal::StillRecoverable { index: 1, .. })
        ));
    }
}
