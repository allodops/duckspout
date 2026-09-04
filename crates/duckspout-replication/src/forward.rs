//! `Forward` (§4, §5.4): the origin's shipment of one newly staged,
//! contiguous seq range to each member of its RF replica set.
//!
//! **Deliberately out of scope here** (see this crate's module docs / the
//! PR this landed in for the full boundary): *which* peers make up a
//! partition's RF set is HRW ring membership plus ownership routing —
//! issue #52's own scope (`HRW ring integration + ownership routing`), not
//! this one's. This module takes an already-decided peer list and sends one
//! `Forward` to each; nothing here computes `hrw_ranked` or reads any
//! membership view. Likewise, the receipt-wait timeout and ring-walk retry
//! to a substitute peer (§1) are not implemented here — that needs the
//! `Scheduler`/`Clock` ports and daemon-level composition, and issue #190
//! already tracks the P model's own still-open finding in the same area.

use bytes::Bytes;
use duckspout_types::{
    DatasetId, NodeId, StagedCoverage, TraceEvent, TraceSink, Transport, TransportError, WindowId,
};

use crate::fencing::Incarnation;
use crate::wire::{Envelope, ForwardMessage};

/// One locally staged, not-yet-forwarded batch (§4.2.3, §4.2.4): exactly
/// the coverage a `StageCommit` just produced, plus the row bytes and the
/// window/dataset bookkeeping a peer's `PeerApply` needs to durably apply
/// it.
#[derive(Debug, Clone)]
pub struct ForwardBatch {
    /// The partition and origin-assigned seq range this batch covers —
    /// typically one of the [`StagedCoverage`] entries a `StageCommitter`
    /// call just returned.
    pub coverage: StagedCoverage,
    /// The dense per-partition window the rows were staged into.
    pub window: WindowId,
    /// The dataset the rows belong to.
    pub dataset: DatasetId,
    /// The rows, as one Arrow IPC stream.
    pub records: Bytes,
}

/// Sends `batch` to every peer in `peers`, over `self_incarnation` (§5.7).
/// Returns one `(peer, send result)` per peer, in `peers`' order — message
/// loss is silent to the sender (`Transport`'s own contract, §5): an `Err`
/// here means the transport itself refused the send (e.g. an unknown peer),
/// never that delivery failed, which this crate learns about only through
/// the peer's own `Receipt` (or its absence).
///
/// Journals one [`TraceEvent::Forward`] per peer sent to, unconditionally on
/// the send attempt (matching `Transport::send`'s own "handed to the
/// transport, not delivered" contract — there is no delivery-confirmed
/// moment to gate this on).
pub async fn forward_to_peers(
    transport: &dyn Transport,
    self_incarnation: Incarnation,
    peers: &[NodeId],
    batch: &ForwardBatch,
    trace: Option<&dyn TraceSink>,
) -> Vec<(NodeId, Result<(), TransportError>)> {
    let mut results = Vec::with_capacity(peers.len());
    for peer in peers {
        let message = ForwardMessage {
            incarnation: self_incarnation,
            partition: batch.coverage.partition.clone(),
            range: batch.coverage.range.clone(),
            window: batch.window,
            dataset: batch.dataset.clone(),
            records: batch.records.clone(),
        };
        let payload = Envelope::Forward(message).to_bytes();
        let result = transport.send(peer.clone(), payload).await;
        if let Some(trace) = trace {
            trace.record(TraceEvent::Forward);
        }
        results.push((peer.clone(), result));
    }
    results
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use duckspout_types::{BoxFuture, OriginSeqRange, PartitionId};

    use super::*;

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

    /// A hand-rolled deterministic [`Transport`] double (this crate cannot
    /// depend on `duckspout-ctk`, see `peer_apply`'s test module for why):
    /// records every send, always succeeds unless `to` is in `unreachable`.
    #[derive(Default)]
    struct FakeTransport {
        sent: Mutex<Vec<(NodeId, Bytes)>>,
        unreachable: Vec<NodeId>,
    }

    impl Transport for FakeTransport {
        fn send(&self, to: NodeId, payload: Bytes) -> BoxFuture<'_, Result<(), TransportError>> {
            if self.unreachable.contains(&to) {
                return Box::pin(std::future::ready(Err(TransportError::UnknownPeer(to))));
            }
            self.sent.lock().expect("lock").push((to, payload));
            Box::pin(std::future::ready(Ok(())))
        }

        fn recv(&self) -> BoxFuture<'_, Result<(NodeId, Bytes), TransportError>> {
            Box::pin(std::future::ready(Err(TransportError::Closed)))
        }
    }

    fn batch() -> ForwardBatch {
        ForwardBatch {
            coverage: StagedCoverage {
                partition: PartitionId::new("p0"),
                range: OriginSeqRange {
                    origin: NodeId::new("origin-1"),
                    first_seq: 1,
                    last_seq: 4,
                },
            },
            window: WindowId(0),
            dataset: DatasetId::new("otlp_logs"),
            records: Bytes::from_static(b"rows"),
        }
    }

    /// A batch forwarded to N peers produces exactly N sends, one Forward
    /// envelope each, decodable back to the same coverage — the basic
    /// shape §4's "ships ... to each member of the RF set" describes.
    /// Would catch a fan-out bug (sending once instead of per-peer, or
    /// mutating the message between sends).
    #[test]
    fn forwards_the_same_batch_to_every_peer() {
        block_on(async {
            let transport = FakeTransport::default();
            let peers = vec![NodeId::new("replica-a"), NodeId::new("replica-b")];
            let results =
                forward_to_peers(&transport, Incarnation(1), &peers, &batch(), None).await;
            assert_eq!(results.len(), 2);
            assert!(results.iter().all(|(_, r)| r.is_ok()));

            let sent = transport.sent.lock().expect("lock");
            assert_eq!(sent.len(), 2);
            for (to, payload) in sent.iter() {
                assert!(peers.contains(to));
                match Envelope::from_bytes(payload).expect("decode") {
                    Envelope::Forward(msg) => {
                        assert_eq!(msg.range, batch().coverage.range);
                        assert_eq!(msg.incarnation, Incarnation(1));
                    }
                    Envelope::Receipt(_) => panic!("expected a Forward envelope"),
                }
            }
        });
    }

    /// A transport failure to one peer does not stop the fan-out to the
    /// others (message loss/refusal is per-peer, §5) — would catch an
    /// implementation that returns early on the first error instead of
    /// attempting every peer.
    #[test]
    fn one_peer_being_unreachable_does_not_stop_the_others() {
        block_on(async {
            let transport = FakeTransport {
                unreachable: vec![NodeId::new("replica-a")],
                ..Default::default()
            };
            let peers = vec![NodeId::new("replica-a"), NodeId::new("replica-b")];
            let results =
                forward_to_peers(&transport, Incarnation(1), &peers, &batch(), None).await;
            assert_eq!(results.len(), 2);
            assert!(results[0].1.is_err());
            assert!(results[1].1.is_ok());
            assert_eq!(transport.sent.lock().expect("lock").len(), 1);
        });
    }

    #[test]
    fn empty_peer_list_sends_nothing() {
        block_on(async {
            let transport = FakeTransport::default();
            let results = forward_to_peers(&transport, Incarnation(1), &[], &batch(), None).await;
            assert!(results.is_empty());
            assert!(transport.sent.lock().expect("lock").is_empty());
        });
    }
}
