//! `PeerApply` (§4, §5.4): a replica's handling of one inbound `Forward`.
//!
//! Five outcomes, in this priority order (a message failing an earlier
//! guard never reaches a later one):
//!
//! 0. **Range validity**: an inverted or degenerate range (`first_seq >
//!    last_seq`) is refused outright — a malformed/buggy peer's message,
//!    not a shape any correct origin produces; evaluated before anything
//!    else touches fencing or gap state. (ACPR #194 MEDIUM-10.)
//! 1. **Self-origin** (§5.7; `specs/DuckSpoutCore.tla`'s `RingPeers(p, n)
//!    == Nodes \ {n}`, line 284, and `Forward`'s own `r.origin = n`
//!    guard, line 287): a Forward whose claimed `origin` is this peer's
//!    own identity is refused outright — a node is never a member of its
//!    own replica set, so a genuine `Forward` can never carry this shape.
//!    Defense-in-depth against a stale/buggy membership view (§5.2) or a
//!    caller that failed to filter self out of the peer list —
//!    [`crate::forward::forward_to_peers`]'s own `self_node` filter is the
//!    matching guard on the sending side. (ACPR #194 HIGH-1.)
//! 2. **Fencing** (§5.7): `incarnation < highest seen from this origin` is a
//!    zombie — refused outright, no apply, no claim, no receipt
//!    (`FencedZombie`).
//! 3. **Gap-freedom** (§5.4, `GapFreedom`): the forwarded range's
//!    `first_seq` must be exactly one past this peer's current
//!    `applied_thru` for `(origin, partition)`. A range at or below the
//!    watermark is an **idempotent duplicate** — receipted without
//!    re-applying, defensively confirmed against the durable log first
//!    (`ReplicaLog::has_applied`) rather than trusted from the incoming
//!    message. This idempotent-duplicate branch is `docs/design/replication.md`
//!    §4's `PeerApply` row, not `specs/DuckSpoutCore.tla`'s `PeerApply`
//!    action — the TLA+ action has no such branch at all (its guard is a
//!    strict `g.rec.seq = AppliedThru(...) + 1`, line 305). The
//!    `has_applied` confirmation is defense-in-depth against a
//!    `ReplicaLog` backend whose `applied_thru` and `has_applied` answers
//!    are mutually inconsistent — a state a **conformant** backend can
//!    never actually produce (`applied_thru`'s own port contract IS the
//!    prefix length of exactly the set `has_applied` answers over); see
//!    [`ReplicaLog::has_applied`]'s own doc comment (ACPR #194 HIGH-3) for
//!    why this is **not** a defense against a same-`seq`-different-record
//!    collision — this port carries no record identity for `has_applied`
//!    to check, and #192 explicitly rejected building seq→key tracking as
//!    unneeded (an honest origin can never legitimately assign the same
//!    `seq` to two different records). A range that would leave a gap
//!    (`first_seq` strictly past `applied_thru + 1`, or a range straddling
//!    the watermark on either side) is refused outright.
//! 4. **Durable apply** (§4.2 A1): a genuinely next-in-line range is
//!    durably applied through [`ReplicaLog::apply`]. A receipt is sent
//!    **only** after the apply durably succeeds — never on a backend
//!    failure (the exact bug PR #192's ACPR pass on the P model's
//!    `Node.p` found and fixed: a receipt for a record that was never
//!    actually staged).
//!
//! Fencing and gap-freedom are **not** one atomic joint conjunction end to
//! end, unlike `specs/DuckSpoutCore.tla`'s `PeerApply` action, which is a
//! true single-step conjunction (fencing, the gap check, and every state
//! update happen together or not at all): [`FenceTable::admit`] mutates
//! `highest_seen` the moment fencing passes — strictly before the gap
//! check below runs — so a message that passes fencing but is then
//! gap-refused still leaves the fencing table advanced. This is the exact
//! same asymmetry PR #192's ACPR pass found in the P model's `eForward`
//! handler (`p/Replication/Node.p`'s own header comment) and judged
//! practically benign there (nothing in this crate depends on
//! `highest_seen` NOT advancing on a gap-refused message); the same
//! judgment holds here, so the behavior is unchanged from the code this
//! comment replaces — only the claim is corrected to disclose the
//! asymmetry honestly instead of implying a false end-to-end atomicity.
//! (ACPR #194 MEDIUM-9.)
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
    /// The forwarded range is inverted or degenerate (`first_seq >
    /// last_seq`) — a malformed/buggy peer's message. Refused outright,
    /// before fencing or gap state is touched. (ACPR #194 MEDIUM-10.)
    InvalidRange,
    /// The Forward's claimed `range.origin` is this peer's own identity —
    /// a node is never a member of its own replica set
    /// (`RingPeers(p, n) == Nodes \ {n}`). Refused outright, no apply, no
    /// claim, no receipt. (ACPR #194 HIGH-1.)
    SelfOriginRefused,
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
    ///
    /// Structurally unreachable against a **conformant** [`ReplicaLog`]:
    /// `applied_thru`'s own port contract makes `last_seq <= applied_thru`
    /// and `has_applied(last_seq)` the same fact, not two facts this
    /// branch has to separately verify against each other. This variant
    /// is defense-in-depth against a backend that violates that contract
    /// (a bug, not a protocol-level attack) — it is **not** a guard
    /// against a same-`seq`-different-record collision (there is no
    /// record identity here to check), and does not defend against
    /// anything issue #192's own resolution addressed (ACPR #194 HIGH-3).
    SuspectDuplicate,
    /// [`ReplicaLog::apply`] failed: nothing is staged, so nothing is
    /// receipted (§5.4; the record stays gap-refusable on a future retry —
    /// this peer's `applied_thru` did not advance).
    ApplyFailed,
}

/// Evaluates one inbound [`ForwardMessage`] against `fence` and `log`,
/// applying it and producing the [`ReceiptMessage`] to send back when
/// warranted (module docs for the exact guard order). `self_node` /
/// `self_incarnation` are this peer's own identity and current incarnation:
/// `self_incarnation` is stamped on any outgoing receipt (§5.7), and
/// `self_node` also gates the self-origin refusal (guard 1 above) — both
/// drawn once at `FenceBoot` (issue #53), handed in here as opaque
/// already-known values (this module has no membership or boot-sequencing
/// concept of its own). `fence` is expected to be the SAME [`FenceTable`]
/// the caller's `Receipt`-handling path uses (ACPR #194 HIGH-2) — see
/// [`crate::receipt::ReceiptTracker`]'s own doc comment for why one table,
/// not two, is required.
///
/// Journals [`TraceEvent::PeerApply`] only on [`PeerApplyOutcome::Applied`]
/// (a genuine durable apply — `docs/trace-mapping.md`'s own row) and
/// [`TraceEvent::Receipt`] whenever a receipt is actually produced
/// (`Applied` or `DuplicateAcked`), matching `EngineStager`'s own
/// journal-only-on-success convention for `StageCommit`. Unlike
/// [`crate::forward::forward_to_peers`]'s `TraceEvent::Forward` (which that
/// module explicitly reasons is journaled at "handed to the transport, not
/// delivered," since a send has no delivery-confirmed moment to gate on),
/// [`TraceEvent::Receipt`] here is journaled at the [`ReceiptMessage`]'s
/// **construction**, not at confirmed send back to the origin — this
/// function returns the message for the caller to actually send; whether
/// that send itself later fails is invisible to this trace point. That is
/// the same implicit choice `Transport::send`'s contract makes explicit for
/// `Forward`, made here without comment before this note (ACPR #194
/// LOW-13); the behavior is unchanged, only now disclosed.
pub async fn apply_forward(
    fence: &mut FenceTable,
    log: &dyn ReplicaLog,
    self_node: &NodeId,
    self_incarnation: crate::fencing::Incarnation,
    trace: Option<&dyn TraceSink>,
    forward: ForwardMessage,
) -> PeerApplyOutcome {
    if forward.range.first_seq > forward.range.last_seq {
        return PeerApplyOutcome::InvalidRange;
    }

    let origin = forward.range.origin.clone();
    let partition = forward.partition.clone();

    if origin == *self_node {
        return PeerApplyOutcome::SelfOriginRefused;
    }

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
    ///
    /// Tracks every individual `seq` durably applied per `(origin,
    /// partition)`, not just the endpoints of applied ranges, so
    /// `applied_thru` can compute the REAL port contract — the longest
    /// 1-based contiguous prefix of that set — rather than merely the
    /// maximum `last_seq` ever passed to `apply`. An earlier revision of
    /// this fake used `seqs.iter().max()`, which is conformant only when
    /// `apply` is never called out of order; that shortcut let
    /// `PeerApplyOutcome::SuspectDuplicate` look reachable in tests only
    /// because the fake itself violated the very port contract it stands
    /// in for — against this conformant fake (and, by the port's own
    /// contract, against any real conformant backend) `last_seq <=
    /// applied_thru` and `has_applied(last_seq)` are always the same fact,
    /// so `SuspectDuplicate` cannot be reached without a deliberately
    /// non-conformant double (ACPR #194 HIGH-3 — see
    /// [`PeerApplyOutcome::SuspectDuplicate`]'s own doc comment).
    #[derive(Default)]
    struct FakeReplicaLog {
        applied: Mutex<HashMap<(NodeId, PartitionId), std::collections::BTreeSet<u64>>>,
        fail_next_apply: Mutex<bool>,
        /// Number of times `apply` actually staged a record (excludes
        /// injected failures) — a call counter distinct from `applied`'s
        /// per-seq coverage set, since one `apply` call can cover more
        /// than one `seq` (a multi-row range) and tests need to assert on
        /// "how many times was apply invoked," not "how many seqs are
        /// covered."
        apply_calls: Mutex<usize>,
    }

    impl FakeReplicaLog {
        fn fail_next(&self) {
            *self.fail_next_apply.lock().expect("lock") = true;
        }

        fn apply_call_count(&self) -> usize {
            *self.apply_calls.lock().expect("lock")
        }
    }

    impl ReplicaLog for FakeReplicaLog {
        fn applied_thru(&self, origin: &NodeId, partition: &PartitionId) -> u64 {
            let applied = self.applied.lock().expect("lock");
            let Some(seqs) = applied.get(&(origin.clone(), partition.clone())) else {
                return 0;
            };
            let mut thru = 0u64;
            while seqs.contains(&(thru + 1)) {
                thru += 1;
            }
            thru
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
            let mut applied = self.applied.lock().expect("lock");
            let seqs = applied
                .entry((record.range.origin.clone(), record.partition.clone()))
                .or_default();
            for seq in record.range.first_seq..=record.range.last_seq {
                seqs.insert(seq);
            }
            drop(applied);
            *self.apply_calls.lock().expect("lock") += 1;
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
                log.apply_call_count(),
                1,
                "a duplicate must not call ReplicaLog::apply a second time"
            );
        });
    }

    // NOTE (ACPR #194 HIGH-3): this file previously had a
    // `a_claimed_duplicate_the_log_never_actually_applied_is_never_receipted`
    // test here, built by manually inserting an inconsistent state into
    // `FakeReplicaLog` (`applied_thru` claiming 3 while only seq 3, not 1
    // or 2, was individually on file). That premise required a
    // NON-conformant `ReplicaLog` fake — `applied_thru`'s real port
    // contract makes it, by definition, the prefix length of exactly the
    // set `has_applied` answers over, so a conformant backend can never
    // have `last_seq <= applied_thru` with `has_applied(last_seq)` false.
    // Once `FakeReplicaLog` was fixed to be conformant (this file, above),
    // that test's premise became impossible to construct, proving the ACPR
    // finding's own point: `PeerApplyOutcome::SuspectDuplicate` is
    // unreachable against a conformant `ReplicaLog`, in tests or in
    // production. The defensive branch stays in `apply_forward` (backend
    // bugs are still worth defending against), but per #192's own
    // precedent for its structurally analogous `fwd.key in staged` guard
    // (`p/Replication/Node.p`'s header comment: fixed at the test-data
    // level, kept as "cheap, correct defense ... regardless," with no
    // dedicated test reaching its false branch), this defensive branch is
    // deliberately left without a unit test that can only be constructed
    // by cheating the fake it would run against. See
    // `PeerApplyOutcome::SuspectDuplicate`'s and
    // `duckspout_types::ReplicaLog::has_applied`'s doc comments for the
    // full corrected claim.

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

    /// ACPR #194 HIGH-1 scratch-repro re-verification: a Forward whose
    /// claimed `range.origin` equals the RECEIVING peer's own identity is
    /// refused outright — `RingPeers(p, n) == Nodes \ {n}`
    /// (`specs/DuckSpoutCore.tla:284`) means a genuine `Forward` can never
    /// carry this shape; this is the defensive guard for a stale/buggy
    /// membership view (or a forwarding bug) that addressed one to itself
    /// anyway. Before this guard existed, `apply_forward` would happily
    /// apply and receipt such a message — the exact hole the ACPR's
    /// scratch test exploited to make `client_ack_ready(rf=2)` report
    /// durability with zero real replicas. No apply attempted, no fencing
    /// state touched.
    #[test]
    fn a_forward_whose_origin_is_the_receiving_peer_itself_is_refused() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let self_node = NodeId::new("node-x");
            let outcome = apply_forward(
                &mut fence,
                &log,
                &self_node,
                Incarnation(1),
                None,
                forward("node-x", "p0", 1, 1, 3),
            )
            .await;
            assert_eq!(outcome, PeerApplyOutcome::SelfOriginRefused);
            assert_eq!(
                log.applied_thru(&self_node, &PartitionId::new("p0")),
                0,
                "a self-origin Forward must never be applied"
            );
            assert_eq!(
                fence.highest_seen(&self_node),
                Incarnation(0),
                "a refused self-origin Forward must not advance fencing state"
            );
        });
    }

    /// ACPR #194 MEDIUM-10(a): an inverted range (`first_seq > last_seq`) —
    /// the shape a malformed/buggy peer could send, e.g. via
    /// `last_seq = u64::MAX` paired with a small `first_seq` — is refused
    /// outright rather than falling through to the gap/duplicate logic,
    /// which was never written to reason about `first_seq > last_seq` and
    /// could otherwise misclassify it.
    #[test]
    fn an_inverted_range_is_refused_outright() {
        block_on(async {
            let mut fence = FenceTable::new();
            let log = FakeReplicaLog::default();
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 5, 2),
            )
            .await;
            assert_eq!(outcome, PeerApplyOutcome::InvalidRange);
            assert_eq!(
                log.applied_thru(&NodeId::new("origin-1"), &PartitionId::new("p0")),
                0
            );

            // The degenerate extreme the finding calls out by name.
            let outcome = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, u64::MAX, 1),
            )
            .await;
            assert_eq!(outcome, PeerApplyOutcome::InvalidRange);
        });
    }

    /// ACPR #194 HIGH-4: a range that STRADDLES the applied watermark
    /// (`first_seq <= applied_thru < last_seq`) must be refused outright,
    /// never silently applied (which would re-stage already-applied seqs
    /// into the hot table as duplicate rows). This is the teeth-proof the
    /// finding specifically demands: mutating the real gap-check condition
    /// in `apply_forward` from `first_seq != applied_thru + 1` to
    /// `first_seq > applied_thru + 1` left every OTHER test in this file
    /// passing while silently admitting exactly this straddling shape —
    /// this test is written to fail under that exact mutation.
    #[test]
    fn a_straddling_range_is_refused_not_silently_applied() {
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

            // applied_thru is now 3. A range [1, 5] straddles it: under
            // `!=`, first_seq (1) != applied_thru + 1 (4), so this is
            // GapRefused, exactly like a genuine gap -- it is NOT treated
            // as the idempotent-duplicate case (which requires
            // `last_seq <= applied_thru`, false here since last_seq = 5).
            let straddling = apply_forward(
                &mut fence,
                &log,
                &NodeId::new("replica-1"),
                Incarnation(1),
                None,
                forward("origin-1", "p0", 1, 1, 5),
            )
            .await;
            assert_eq!(
                straddling,
                PeerApplyOutcome::GapRefused,
                "a straddling range must be refused outright, never partially applied"
            );
            assert_eq!(
                log.applied_thru(&NodeId::new("origin-1"), &PartitionId::new("p0")),
                3,
                "a refused straddling range must not advance the watermark or \
                 re-stage already-applied seqs"
            );
            assert_eq!(
                log.apply_call_count(),
                1,
                "a straddling range must not call ReplicaLog::apply a second time"
            );
        });
    }
}
