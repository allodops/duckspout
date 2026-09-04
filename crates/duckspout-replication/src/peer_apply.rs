//! `PeerApply` (§4, §5.4): a replica's handling of one inbound `Forward`.
//!
//! Three outcomes, matching `docs/design/replication.md` §4's `PeerApply`
//! row and `specs/DuckSpoutCore.tla`'s `PeerApply` action exactly, in this
//! priority order (a message failing an earlier guard never reaches a
//! later one — mirroring TLA+'s single joint conjunction):
//!
//! 1. **Fencing** (§5.7): `incarnation < highest seen from this origin` is a
//!    zombie — refused outright, no apply, no claim, no receipt
//!    (`FencedZombie`).
//! 2. **Gap-freedom** (§5.4, `GapFreedom`): the forwarded range's
//!    `first_seq` must be exactly one past this peer's current
//!    `applied_thru` for `(origin, partition)`. A range at or below the
//!    watermark is an **idempotent duplicate** — receipted without
//!    re-applying, defensively confirmed against the durable log first
//!    (`ReplicaLog::has_applied`) rather than trusted from the incoming
//!    message — see [`ReplicaLog::has_applied`]'s own doc comment for the
//!    exact P-model ACPR finding (#192) this guards against. A range that
//!    would leave a gap (`first_seq` strictly past `applied_thru + 1`, or
//!    a range straddling the watermark on either side) is refused outright.
//! 3. **Durable apply** (§4.2 A1): a genuinely next-in-line range is
//!    durably applied through [`ReplicaLog::apply`]. A receipt is sent
//!    **only** after the apply durably succeeds — never on a backend
//!    failure (the exact bug PR #192's ACPR pass on the P model's
//!    `Node.p` found and fixed: a receipt for a record that was never
//!    actually staged).
//!
//! Generalized from `p/Replication/Node.p`'s per-origin-only bookkeeping to
//! per-`(origin, partition)`: the real system has a partition dimension the
//! P model deliberately omits (`docs/design/p-tla-correspondence.md` §3.2's
//! `GapFreedom` PR notes), and `docs/trace-mapping.md`'s own `PeerApply` row
//! ("Refuses gaps per (partition, origin)") already documents this as the
//! Rust-level guarantee.
//!
//! **Deliberately out of scope here** (see this crate's module docs / the
//! PR this landed in for the full boundary):
//! - `SchemaKnown`'s fail-closed-on-unknown-columns guard and schema
//!   records riding in-band (§4's "Schema changes ride in-band") — no
//!   schema-evolution machinery exists in this crate yet; a peer here
//!   applies every forwarded range's rows opaquely.
//! - The late-arrival immediate-takeover check (`p/Replication/Node.p`'s
//!   `eForward` handler: `peerDead && !degraded && ...`) and
//!   `ClaimAdvertise`'s registry row — both are issue #54's
//!   (`Node death end-to-end: TakeoverDrain + DeclareLoss`) and #53's
//!   (`Incarnation fencing + registry claims`) scope respectively, not
//!   this issue's.
//! - `Rung(m) < 3`'s hard-overload refusal of NEW ranges (catch-up ranges
//!   still apply) — the overload ladder is `duckspout-staging`'s existing
//!   machinery (§4.5); wiring it into `PeerApply` needs the concrete
//!   `ReplicaLog` backend this port's own doc comment defers.

use duckspout_types::{ForwardedRecord, NodeId, PartitionId, ReplicaLog, TraceEvent, TraceSink};

use crate::fencing::{FenceIdentity, FenceOutcome, FenceTable};
use crate::wire::{ForwardMessage, ReceiptMessage};

/// The result of evaluating one inbound [`ForwardMessage`] (§4's
/// `PeerApply` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerApplyOutcome {
    /// A genuinely next-in-line range was durably applied. Carries the
    /// [`ReceiptMessage`] to send back to the origin.
    Applied(ReceiptMessage),
    /// An idempotent duplicate (`range.last_seq <= applied_thru`),
    /// confirmed genuinely already-staged: receipted without re-applying.
    DuplicateAcked(ReceiptMessage),
    /// The range would leave a gap in this peer's applied prefix (or
    /// straddles its watermark ambiguously) — refused; no apply, no
    /// receipt.
    GapRefused,
    /// The message's incarnation was strictly below the highest already
    /// accepted from this origin — a zombie (`FencedZombie`); no apply, no
    /// receipt, no claim.
    Fenced,
    /// The range claims to be an already-applied duplicate
    /// (`last_seq <= applied_thru`), but the durable log does not actually
    /// hold it — the defensive guard [`ReplicaLog::has_applied`] exists
    /// for. Never fabricates a receipt for a record this peer does not
    /// actually hold; no apply is attempted either (a range below the
    /// watermark is never re-applied, honest duplicate or not).
    SuspectDuplicate,
    /// [`ReplicaLog::apply`] failed: nothing is staged, so nothing is
    /// receipted (§5.4; the record stays gap-refusable on a future retry —
    /// this peer's `applied_thru` did not advance).
    ApplyFailed,
}

/// Evaluates one inbound [`ForwardMessage`] against `fence` and `log`,
/// applying it and producing the [`ReceiptMessage`] to send back when
/// warranted (module docs for the exact guard order). `self_node` /
/// `self_incarnation` are this peer's own identity and current incarnation,
/// stamped on any outgoing receipt (§5.7) — drawn once at `FenceBoot` (issue
/// #53), handed in here as opaque already-known values (this module has no
/// membership or boot-sequencing concept of its own).
///
/// Journals [`TraceEvent::PeerApply`] only on [`PeerApplyOutcome::Applied`]
/// (a genuine durable apply — `docs/trace-mapping.md`'s own row) and
/// [`TraceEvent::Receipt`] whenever a receipt is actually produced
/// (`Applied` or `DuplicateAcked`), matching `EngineStager`'s own
/// journal-only-on-success convention for `StageCommit`.
pub async fn apply_forward(
    fence: &mut FenceTable,
    log: &dyn ReplicaLog,
    self_node: &NodeId,
    self_incarnation: crate::fencing::Incarnation,
    trace: Option<&dyn TraceSink>,
    forward: ForwardMessage,
) -> PeerApplyOutcome {
    let origin = forward.range.origin.clone();
    let partition = forward.partition.clone();

    let identity = FenceIdentity {
        node: origin.clone(),
        incarnation: forward.incarnation,
    };
    if matches!(fence.admit(&identity), FenceOutcome::Zombie { .. }) {
        return PeerApplyOutcome::Fenced;
    }

    let applied_thru = log.applied_thru(&origin, &partition);

    if forward.range.last_seq <= applied_thru {
        // Idempotent duplicate: this entire range is already covered.
        return if log.has_applied(&origin, &partition, forward.range.last_seq) {
            let receipt = build_receipt(
                self_node,
                self_incarnation,
                &origin,
                &partition,
                applied_thru,
            );
            if let Some(trace) = trace {
                trace.record(TraceEvent::Receipt);
            }
            PeerApplyOutcome::DuplicateAcked(receipt)
        } else {
            PeerApplyOutcome::SuspectDuplicate
        };
    }

    if forward.range.first_seq != applied_thru + 1 {
        // Either a genuine gap, or a range straddling the watermark on
        // either side — both are refused outright (module docs: this
        // crate does not guess at a partial apply of a straddling range).
        return PeerApplyOutcome::GapRefused;
    }

    let last_seq = forward.range.last_seq;
    let record = ForwardedRecord {
        partition: forward.partition,
        range: forward.range,
        window: forward.window,
        dataset: forward.dataset,
        records: forward.records,
    };
    match log.apply(record).await {
        Ok(()) => {
            if let Some(trace) = trace {
                trace.record(TraceEvent::PeerApply);
                trace.record(TraceEvent::Receipt);
            }
            PeerApplyOutcome::Applied(build_receipt(
                self_node,
                self_incarnation,
                &origin,
                &partition,
                last_seq,
            ))
        }
        Err(_) => PeerApplyOutcome::ApplyFailed,
    }
}

fn build_receipt(
    self_node: &NodeId,
    incarnation: crate::fencing::Incarnation,
    origin: &NodeId,
    partition: &PartitionId,
    applied_thru: u64,
) -> ReceiptMessage {
    ReceiptMessage {
        incarnation,
        holder: self_node.clone(),
        origin: origin.clone(),
        partition: partition.clone(),
        applied_thru,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use bytes::Bytes;
    use duckspout_types::{BoxFuture, DatasetId, OriginSeqRange, ReplicaApplyError, WindowId};

    use super::*;
    use crate::fencing::Incarnation;

    /// A minimal, dependency-free executor for these tests: this crate has
    /// no tokio runtime (D-2: protocol crates spawn nothing themselves) and
    /// cannot pull `duckspout-ctk` in either (would be a
    /// protocol-crate-depends-on-a-concrete-impl edge, forbidden by
    /// `invariants.toml`) — matching `duckspout-drain/tests/choreography.rs`'s
    /// own `block_on` for exactly the same reason. Every future polled here
    /// resolves on the first poll (the fake below is `std::future::ready`),
    /// so `Waker::noop()` never needs to actually wake anything.
    fn block_on<T>(future: impl Future<Output = T>) -> T {
        let mut future = Box::pin(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    /// A hand-rolled, deterministic [`ReplicaLog`] double — this crate
    /// cannot depend on `duckspout-ctk` (it would be a
    /// protocol-crate-depends-on-a-concrete-impl edge, forbidden by
    /// `invariants.toml`), so its own tests roll a minimal fake instead,
    /// matching how `duckspout-staging`/`duckspout-accept` test their own
    /// ports without it.
    #[derive(Default)]
    struct FakeReplicaLog {
        applied: Mutex<HashMap<(NodeId, PartitionId), Vec<u64>>>,
        fail_next_apply: Mutex<bool>,
    }

    impl FakeReplicaLog {
        fn fail_next(&self) {
            *self.fail_next_apply.lock().expect("lock") = true;
        }
    }

    impl ReplicaLog for FakeReplicaLog {
        fn applied_thru(&self, origin: &NodeId, partition: &PartitionId) -> u64 {
            self.applied
                .lock()
                .expect("lock")
                .get(&(origin.clone(), partition.clone()))
                .and_then(|seqs| seqs.iter().max().copied())
                .unwrap_or(0)
        }

        fn has_applied(&self, origin: &NodeId, partition: &PartitionId, seq: u64) -> bool {
            self.applied
                .lock()
                .expect("lock")
                .get(&(origin.clone(), partition.clone()))
                .is_some_and(|seqs| seqs.contains(&seq))
        }

        fn apply(&self, record: ForwardedRecord) -> BoxFuture<'_, Result<(), ReplicaApplyError>> {
            let mut fail = self.fail_next_apply.lock().expect("lock");
            if *fail {
                *fail = false;
                return Box::pin(std::future::ready(Err(ReplicaApplyError::Backend(
                    "fake backend failure".to_string(),
                ))));
            }
            drop(fail);
            self.applied
                .lock()
                .expect("lock")
                .entry((record.range.origin.clone(), record.partition.clone()))
                .or_default()
                .push(record.range.last_seq);
            Box::pin(std::future::ready(Ok(())))
        }
    }

    fn forward(origin: &str, partition: &str, inc: u64, first: u64, last: u64) -> ForwardMessage {
        ForwardMessage {
            incarnation: Incarnation(inc),
            partition: PartitionId::new(partition),
            range: OriginSeqRange {
                origin: NodeId::new(origin),
                first_seq: first,
                last_seq: last,
            },
            window: WindowId(0),
            dataset: DatasetId::new("otlp_logs"),
            records: Bytes::from_static(b"rows"),
        }
    }

    /// `TestGapFreedom`'s basic shape (mirroring `p/Replication/TestDriver.p`
    /// naming, PR #192): the first-ever range from a fresh origin, starting
    /// at seq 1, is applied and receipted.
    #[test]
    fn first_contiguous_range_is_applied_and_receipted() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 3),
            )
            .await;
            match outcome {
                PeerApplyOutcome::Applied(receipt) => {
                    assert_eq!(receipt.applied_thru, 3);
                    assert_eq!(receipt.origin, NodeId::new("origin-1"));
                }
                other => panic!("expected Applied, got {other:?}"),
            }
            assert_eq!(
                log.applied_thru(&NodeId::new("origin-1"), &PartitionId::new("p0")),
                3
            );
        });
    }

    /// A range whose `first_seq` is past `applied_thru + 1` leaves a gap
    /// and must be refused — `GapFreedom`'s exact contiguity requirement.
    /// Would catch a `PeerApply` that applies out of order.
    #[test]
    fn a_range_past_the_watermark_is_gap_refused() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 5, 7),
            )
            .await;
            assert_eq!(outcome, PeerApplyOutcome::GapRefused);
            assert_eq!(
                log.applied_thru(&NodeId::new("origin-1"), &PartitionId::new("p0")),
                0
            );
        });
    }

    /// A retransmit of an already-fully-applied range is acknowledged
    /// without re-applying (§4's `PeerApply` row, "acknowledged without
    /// re-applying") — the receipt still carries the current watermark, and
    /// `ReplicaLog::apply` is never called a second time (no duplicate
    /// entry in the fake's `applied` vec, or `applied_thru` would advance
    /// past the range's own `last_seq`).
    #[test]
    fn idempotent_duplicate_is_receipted_without_reapplying() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let first = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 3),
            )
            .await;
            assert!(matches!(first, PeerApplyOutcome::Applied(_)));

            let retransmit = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 3),
            )
            .await;
            match retransmit {
                PeerApplyOutcome::DuplicateAcked(receipt) => assert_eq!(receipt.applied_thru, 3),
                other => panic!("expected DuplicateAcked, got {other:?}"),
            }
            assert_eq!(
                log.applied
                    .lock()
                    .expect("lock")
                    .get(&(NodeId::new("origin-1"), PartitionId::new("p0")))
                    .expect("entry")
                    .len(),
                1,
                "a duplicate must not call ReplicaLog::apply a second time"
            );
        });
    }

    /// The ACPR-hardened guard (#192): a duplicate-shaped range
    /// (`last_seq <= applied_thru`) whose exact seq the durable log does
    /// NOT actually hold must never fabricate a receipt. This is the exact
    /// hazard the P model's own fix targets — an earlier revision reused an
    /// already-used seq for a genuinely different record, which would
    /// otherwise both fabricate a receipt for an unstaged record and
    /// silently drop the real new record.
    #[test]
    fn a_claimed_duplicate_the_log_never_actually_applied_is_never_receipted() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            // Fabricate an inconsistent state: applied_thru = 3 (only seq 3
            // was ever individually recorded); a range claiming coverage up
            // to a DIFFERENT, never-recorded seq at or below that watermark
            // must not be receipted.
            log.applied
                .lock()
                .expect("lock")
                .insert((NodeId::new("origin-1"), PartitionId::new("p0")), vec![3]);
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 3),
            )
            .await;
            // seq 3 genuinely is on file, so this one IS a legitimate
            // duplicate:
            assert!(matches!(outcome, PeerApplyOutcome::DuplicateAcked(_)));

            // Now ask about a range claiming coverage up to seq 2, which
            // the log has never individually recorded even though its
            // watermark (3) is above it -- has_applied(2) is false, so
            // this must refuse to fabricate a receipt.
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 2),
            )
            .await;
            assert_eq!(outcome, PeerApplyOutcome::SuspectDuplicate);
        });
    }

    /// `FencedZombie`: a Forward carrying a strictly lower incarnation than
    /// one already accepted from the same origin is refused outright, even
    /// though its range would otherwise be perfectly gap-free.
    #[test]
    fn a_stale_incarnation_is_fenced_even_when_the_range_is_gap_free() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let first = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 2, 1, 3),
            )
            .await;
            assert!(matches!(first, PeerApplyOutcome::Applied(_)));

            let zombie = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 4, 6),
            )
            .await;
            assert_eq!(zombie, PeerApplyOutcome::Fenced);
            assert_eq!(
                log.applied_thru(&NodeId::new("origin-1"), &PartitionId::new("p0")),
                3,
                "a fenced message must not advance the applied watermark"
            );
        });
    }

    /// A backend apply failure produces no receipt and does not advance
    /// the watermark — the exact `Receipt`-for-an-unstaged-record bug PR
    /// #192 caught and fixed, exercised here for the ORIGINAL apply path
    /// (not just the duplicate path the two tests above cover).
    #[test]
    fn a_failed_apply_produces_no_receipt() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            log.fail_next();
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 3),
            )
            .await;
            assert_eq!(outcome, PeerApplyOutcome::ApplyFailed);
            assert_eq!(
                log.applied_thru(&NodeId::new("origin-1"), &PartitionId::new("p0")),
                0
            );
        });
    }

    /// Gap-freedom is scoped per `(origin, partition)`: a fresh origin
    /// starting at seq 1 for a partition that ALREADY has a different
    /// origin's rows applied is unaffected — generalizing the P model's
    /// origin-only bookkeeping (module docs).
    #[test]
    fn gap_freedom_is_scoped_per_origin_and_partition_independently() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let a = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-a", "p0", 1, 1, 5),
            )
            .await;
            assert!(matches!(a, PeerApplyOutcome::Applied(_)));

            // origin-b's first range for the SAME partition still starts at
            // 1.
            let b = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-b", "p0", 1, 1, 2),
            )
            .await;
            assert!(matches!(b, PeerApplyOutcome::Applied(_)));

            // origin-a's next range for a DIFFERENT partition also starts
            // at 1.
            let c = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-a", "p1", 1, 1, 1),
            )
            .await;
            assert!(matches!(c, PeerApplyOutcome::Applied(_)));
        });
    }
}
