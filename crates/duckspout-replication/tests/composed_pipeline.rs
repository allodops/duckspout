//! The full `Forward -> decode -> apply_forward -> record ->
//! client_ack_ready` pipeline, composed as one test (§4, §5.4).
//!
//! ACPR #194 MEDIUM-10(b): every unit in this crate was, before this file
//! existed, tested only against its own hand-made fixture, in isolation.
//! That is exactly why HIGH-1 (no enforcement that a receipt's `holder`
//! differs from the write's `origin`) and HIGH-2 (`ReceiptTracker` owning
//! a PRIVATE `FenceTable` instead of sharing the Forward path's) both went
//! undetected: both are seam defects between units, invisible to any test
//! that only ever exercises one unit at a time. This file drives the real
//! public API end to end, across two logical nodes, with one shared
//! `FenceTable` per node exactly as a real composition root
//! (`duckspout-daemon`, issue #193) would wire it.
//!
//! Doubles live here, locally: this crate cannot depend on `duckspout-ctk`
//! even as a dev-dependency (`invariants.toml` forbids a
//! protocol-crate -> concrete-impl edge), matching
//! `duckspout-drain/tests/choreography.rs`'s own local doubles for the
//! same layering reason.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use duckspout_replication::{
    Envelope, FenceTable, ForwardBatch, PeerApplyOutcome, ReceiptOutcome, ReceiptTracker,
    apply_forward, client_ack_ready, forward_to_peers,
};
use duckspout_types::{
    BoxFuture, DatasetId, ForwardedRecord, NodeId, OriginSeqRange, PartitionId, ReplicaApplyError,
    ReplicaLog, StagedCoverage, Transport, TransportError, WindowId,
};

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

/// A [`Transport`] double that hands every sent payload straight to the
/// named recipient's inbox, decoded lazily by the test driver (no actual
/// async delivery loop needed — this is a synchronous point-to-point
/// handoff, matching the other doubles in this crate).
#[derive(Default)]
struct FakeTransport {
    sent: Mutex<Vec<(NodeId, Bytes)>>,
}

impl Transport for FakeTransport {
    fn send(&self, to: NodeId, payload: Bytes) -> BoxFuture<'_, Result<(), TransportError>> {
        self.sent.lock().expect("lock").push((to, payload));
        Box::pin(std::future::ready(Ok(())))
    }

    fn recv(&self) -> BoxFuture<'_, Result<(NodeId, Bytes), TransportError>> {
        Box::pin(std::future::ready(Err(TransportError::Closed)))
    }
}

/// A conformant [`ReplicaLog`] double: `applied_thru` is the real prefix-
/// length computation over individually tracked seqs (ACPR #194 HIGH-3 —
/// see `peer_apply.rs`'s own `FakeReplicaLog` for the full reasoning on
/// why this must NOT be a max-endpoint shortcut).
#[derive(Default)]
struct FakeReplicaLog {
    applied: Mutex<HashMap<(NodeId, PartitionId), std::collections::BTreeSet<u64>>>,
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
        let mut applied = self.applied.lock().expect("lock");
        let seqs = applied
            .entry((record.range.origin.clone(), record.partition.clone()))
            .or_default();
        for seq in record.range.first_seq..=record.range.last_seq {
            seqs.insert(seq);
        }
        Box::pin(std::future::ready(Ok(())))
    }
}

fn batch(origin: &str, partition: &str, first: u64, last: u64) -> ForwardBatch {
    ForwardBatch {
        coverage: StagedCoverage {
            partition: PartitionId::new(partition),
            range: OriginSeqRange {
                origin: NodeId::new(origin),
                first_seq: first,
                last_seq: last,
            },
        },
        window: WindowId(0),
        dataset: DatasetId::new("otlp_logs"),
        records: Bytes::from_static(b"rows"),
    }
}

/// The happy path, end to end, at RF=2: `origin-1` forwards a fresh range
/// to `replica-b`; `replica-b` decodes, applies, and produces a receipt;
/// `origin-1` records that receipt against its OWN `FenceTable` (shared
/// with nothing else here, since `origin-1` never applies a Forward in
/// this test — see the HIGH-2 test below for the cross-path sharing
/// itself); `client_ack_ready(rf = 2)` then reports true with exactly one
/// genuine replica copy.
#[test]
fn forward_decode_apply_record_and_client_ack_compose_end_to_end() {
    block_on(async {
        let origin = NodeId::new("origin-1");
        let replica = NodeId::new("replica-b");
        let partition = PartitionId::new("p0");

        // Forward, origin-side.
        let transport = FakeTransport::default();
        let results = forward_to_peers(
            &transport,
            &origin,
            duckspout_replication::Incarnation(1),
            std::slice::from_ref(&replica),
            &batch("origin-1", "p0", 1, 3),
            None,
        )
        .await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.is_ok());

        // Decode, wire boundary. Scoped so the transport's lock guard is
        // dropped well before the next `.await` below.
        let forward_msg = {
            let sent = transport.sent.lock().expect("lock");
            assert_eq!(sent.len(), 1);
            let (to, payload) = &sent[0];
            assert_eq!(*to, replica);
            match Envelope::from_bytes(payload).expect("decode") {
                Envelope::Forward(msg) => msg,
                Envelope::Receipt(_) => panic!("expected a Forward envelope"),
            }
        };

        // PeerApply, replica-side, against replica-b's own FenceTable.
        let mut replica_fence = FenceTable::new();
        let log = FakeReplicaLog::default();
        let outcome = apply_forward(
            &mut replica_fence,
            &log,
            &replica,
            duckspout_replication::Incarnation(1),
            None,
            forward_msg,
        )
        .await;
        let receipt = match outcome {
            PeerApplyOutcome::Applied(receipt) => receipt,
            other => panic!("expected Applied, got {other:?}"),
        };
        assert_eq!(receipt.holder, replica);
        assert_eq!(receipt.origin, origin);
        assert_eq!(receipt.applied_thru, 3);

        // Receipt, origin-side, against origin-1's own FenceTable.
        let mut origin_fence = FenceTable::new();
        let mut receipts = ReceiptTracker::new();
        let record_outcome = receipts.record(&mut origin_fence, receipt);
        assert_eq!(record_outcome, ReceiptOutcome::Recorded);

        // ClientAck gating.
        assert!(client_ack_ready(&receipts, &origin, &partition, 3, 2));
        assert!(!client_ack_ready(&receipts, &origin, &partition, 3, 3));
    });
}

/// ACPR #194 HIGH-2 scratch-repro re-verification, driven through the REAL
/// `apply_forward` (not a direct `FenceTable::admit` simulation — see
/// `receipt.rs`'s own unit test for that faster-but-narrower version).
///
/// A physical node `node-y` plays both `PeerApply`-receiver and
/// `Receipt`-receiver roles against the SAME sender, `node-x`, through one
/// shared `FenceTable`:
///
/// 1. `node-x` forwards its own write to `node-y` at incarnation 5;
///    `node-y` applies it (advancing `highest_seen[node-x]` to 5 in the
///    shared table).
/// 2. `node-x` then "receipts" a write `node-y` forwarded to it earlier,
///    but claims incarnation 1 — a zombie, since `node-y` already knows
///    `node-x` is at incarnation 5.
///
/// Before HIGH-2's fix (`ReceiptTracker` owning a private `FenceTable`),
/// step 2 would have been admitted: the private table had never seen
/// `node-x` at all. With the shared table, step 2 is correctly fenced.
#[test]
fn a_sender_fenced_via_apply_forward_is_fenced_on_the_receipt_path_through_the_same_table() {
    block_on(async {
        let node_y = NodeId::new("node-y");
        let node_x = NodeId::new("node-x");
        let partition = PartitionId::new("p0");

        // Step 1: node-y applies a Forward FROM node-x at incarnation 5,
        // through node-y's ONE shared FenceTable.
        let mut node_y_fence = FenceTable::new();
        let log = FakeReplicaLog::default();
        let forward_msg = duckspout_replication::ForwardMessage {
            incarnation: duckspout_replication::Incarnation(5),
            partition: partition.clone(),
            range: OriginSeqRange {
                origin: node_x.clone(),
                first_seq: 1,
                last_seq: 1,
            },
            window: WindowId(0),
            dataset: DatasetId::new("otlp_logs"),
            records: Bytes::from_static(b"rows"),
        };
        let outcome = apply_forward(
            &mut node_y_fence,
            &log,
            &node_y,
            duckspout_replication::Incarnation(1),
            None,
            forward_msg,
        )
        .await;
        assert!(matches!(outcome, PeerApplyOutcome::Applied(_)));

        // Step 2: the SAME table is now handed to node-y's Receipt path.
        // node-x "receipts" node-y at incarnation 1 -- a zombie.
        let mut receipts = ReceiptTracker::new();
        let zombie_receipt = duckspout_replication::ReceiptMessage {
            incarnation: duckspout_replication::Incarnation(1),
            holder: node_x.clone(),
            origin: node_y.clone(),
            partition: partition.clone(),
            applied_thru: 999,
        };
        let record_outcome = receipts.record(&mut node_y_fence, zombie_receipt);
        assert_eq!(record_outcome, ReceiptOutcome::Fenced);

        // The zombie receipt must never count toward ClientAck.
        assert!(!client_ack_ready(&receipts, &node_y, &partition, 999, 2));
        assert_eq!(
            receipts.holder_applied_thru(&node_x, &node_y, &partition),
            0,
            "a zombie receipt must never enter the watermark map"
        );
    });
}
