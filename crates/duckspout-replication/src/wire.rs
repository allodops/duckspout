//! The `Forward`/`Receipt` wire envelope (§5.4).
//!
//! `docs/design/replication.md` §4: `Forward` and `Receipt` ride the
//! [`Transport`](duckspout_types::Transport) port's opaque `Bytes` payload —
//! this module is the encode/decode boundary each side of that channel
//! shares. The encoding (JSON) is an internal implementation detail of this
//! crate's own peer protocol, not a public format any other crate reads or
//! a wire format a client ever sees (contrast `duckspout-accept`'s OTLP,
//! which is externally specified) — plain `serde_json` keeps this module
//! small and revisitable without an ADR; nothing here is a settled-decision
//! surface (§9.6).
//!
//! Both messages carry `(node_id, incarnation)` per §5.7 ("every message —
//! Forward, `PeerApply`, Receipt, Heartbeat, drain commit — carries `(node_id,
//! incarnation)`"): [`ForwardMessage::incarnation`] /
//! [`ReceiptMessage::incarnation`] are the sender's own, evaluated by the
//! receiver's [`crate::fencing::FenceTable`] before anything else.

use bytes::Bytes;
use duckspout_types::{DatasetId, NodeId, OriginSeqRange, PartitionId, WindowId};
use serde::{Deserialize, Serialize};

use crate::fencing::Incarnation;

/// `Forward` (§4, §5.4): the acceptor's shipment of one contiguous
/// `(origin, seq)` range to one replica, over its own current incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardMessage {
    /// The forwarding origin's incarnation at send time (§5.7).
    pub incarnation: Incarnation,
    /// The partition the range belongs to.
    pub partition: PartitionId,
    /// The origin-assigned, contiguous seq range (§4.2.3).
    pub range: OriginSeqRange,
    /// The dense per-partition window the rows were staged into.
    pub window: WindowId,
    /// The dataset the rows belong to.
    pub dataset: DatasetId,
    /// The rows, as one Arrow IPC stream (opaque to this crate).
    pub records: Bytes,
}

/// `Receipt` (§4, §5.4): the peer's cumulative, retransmit-safe
/// acknowledgment — "one number, no per-batch bookkeeping." `holder` is
/// carried explicitly (redundant with, but never assumed identical to, the
/// `Transport::recv` sender) exactly as §5.7 requires: fencing evaluates the
/// **message's own** claimed identity, not a transport-layer artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptMessage {
    /// The receipting peer (holder)'s incarnation at send time (§5.7).
    pub incarnation: Incarnation,
    /// The receipting peer (holder) itself.
    pub holder: NodeId,
    /// The origin this receipt reports coverage for.
    pub origin: NodeId,
    /// The partition this receipt reports coverage for.
    pub partition: PartitionId,
    /// The highest contiguous seq the peer has durably applied for
    /// `(origin, partition)` — the cumulative watermark, not a per-record
    /// acknowledgment.
    pub applied_thru: u64,
}

/// One wire message, tagged so a single [`Transport`](duckspout_types::Transport)
/// channel between two peers can carry both directions of §4's protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Envelope {
    /// A `Forward` message.
    Forward(ForwardMessage),
    /// A `Receipt` message.
    Receipt(ReceiptMessage),
}

/// A failure decoding an inbound [`Envelope`] — always a peer/transport bug
/// or a version skew, never a client-facing error (§10.1: this crate is
/// leafless of any client-facing error vocabulary).
#[derive(Debug, thiserror::Error)]
#[error("malformed replication envelope: {0}")]
pub struct WireDecodeError(String);

// --- on-the-wire shapes (serde-derivable; `Bytes` has no serde impl in this
// workspace's pinned `bytes` version, so the row payload rides as `Vec<u8>`
// here and converts back to `Bytes` at the `Envelope` boundary) -----------

#[derive(Serialize, Deserialize)]
enum WireEnvelope {
    Forward {
        incarnation: Incarnation,
        partition: PartitionId,
        origin: NodeId,
        first_seq: u64,
        last_seq: u64,
        window: WindowId,
        dataset: DatasetId,
        records: Vec<u8>,
    },
    Receipt {
        incarnation: Incarnation,
        holder: NodeId,
        origin: NodeId,
        partition: PartitionId,
        applied_thru: u64,
    },
}

impl Envelope {
    /// Encodes this message for [`Transport::send`](duckspout_types::Transport::send).
    ///
    /// # Panics
    ///
    /// Never in practice: every field of the internal wire shape is a plain
    /// serde-derived type over owned data (no non-UTF-8 map keys, no NaN
    /// floats, no cyclic structures) — `serde_json` encoding cannot fail
    /// for it. The `expect` exists because `serde_json::to_vec`'s signature
    /// is fallible in general, not because failure is expected here.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        let wire = match self {
            Envelope::Forward(msg) => WireEnvelope::Forward {
                incarnation: msg.incarnation,
                partition: msg.partition.clone(),
                origin: msg.range.origin.clone(),
                first_seq: msg.range.first_seq,
                last_seq: msg.range.last_seq,
                window: msg.window,
                dataset: msg.dataset.clone(),
                records: msg.records.to_vec(),
            },
            Envelope::Receipt(msg) => WireEnvelope::Receipt {
                incarnation: msg.incarnation,
                holder: msg.holder.clone(),
                origin: msg.origin.clone(),
                partition: msg.partition.clone(),
                applied_thru: msg.applied_thru,
            },
        };
        // Infallible: every field here is a plain serde-derived type over
        // owned data — nothing here can fail to serialize (no non-UTF-8
        // map keys, no NaN floats, no cyclic structures).
        Bytes::from(serde_json::to_vec(&wire).expect("replication envelope always encodes"))
    }

    /// Decodes an inbound payload from [`Transport::recv`](duckspout_types::Transport::recv).
    ///
    /// # Errors
    ///
    /// [`WireDecodeError`] if `payload` is not a well-formed envelope.
    pub fn from_bytes(payload: &Bytes) -> Result<Self, WireDecodeError> {
        let wire: WireEnvelope =
            serde_json::from_slice(payload).map_err(|e| WireDecodeError(e.to_string()))?;
        Ok(match wire {
            WireEnvelope::Forward {
                incarnation,
                partition,
                origin,
                first_seq,
                last_seq,
                window,
                dataset,
                records,
            } => Envelope::Forward(ForwardMessage {
                incarnation,
                partition,
                range: OriginSeqRange {
                    origin,
                    first_seq,
                    last_seq,
                },
                window,
                dataset,
                records: Bytes::from(records),
            }),
            WireEnvelope::Receipt {
                incarnation,
                holder,
                origin,
                partition,
                applied_thru,
            } => Envelope::Receipt(ReceiptMessage {
                incarnation,
                holder,
                origin,
                partition,
                applied_thru,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_forward() -> impl Strategy<Value = ForwardMessage> {
        (
            any::<u64>(),
            ".{0,16}",
            ".{0,16}",
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            ".{0,16}",
            prop::collection::vec(any::<u8>(), 0..32),
        )
            .prop_map(|(inc, partition, origin, a, b, window, dataset, records)| {
                ForwardMessage {
                    incarnation: Incarnation(inc),
                    partition: PartitionId::new(partition),
                    range: OriginSeqRange {
                        origin: NodeId::new(origin),
                        first_seq: a.min(b),
                        last_seq: a.max(b),
                    },
                    window: WindowId(window),
                    dataset: DatasetId::new(dataset),
                    records: Bytes::from(records),
                }
            })
    }

    fn arb_receipt() -> impl Strategy<Value = ReceiptMessage> {
        (any::<u64>(), ".{0,16}", ".{0,16}", ".{0,16}", any::<u64>()).prop_map(
            |(inc, holder, origin, partition, thru)| ReceiptMessage {
                incarnation: Incarnation(inc),
                holder: NodeId::new(holder),
                origin: NodeId::new(origin),
                partition: PartitionId::new(partition),
                applied_thru: thru,
            },
        )
    }

    proptest! {
        /// §8.5-style law: ANY [`ForwardMessage`] round-trips losslessly
        /// through the wire encoding — including empty/non-ASCII ids and an
        /// empty or arbitrary row payload. Would catch a field silently
        /// dropped or mis-ordered in `WireEnvelope`'s hand-written
        /// conversion, which would corrupt every Forward on the wire.
        #[test]
        fn forward_round_trips_any_values(msg in arb_forward()) {
            let bytes = Envelope::Forward(msg.clone()).to_bytes();
            let decoded = Envelope::from_bytes(&bytes).expect("decode");
            prop_assert_eq!(decoded, Envelope::Forward(msg));
        }

        /// Same law, for [`ReceiptMessage`] — the cumulative watermark and
        /// sender identity must survive the wire exactly.
        #[test]
        fn receipt_round_trips_any_values(msg in arb_receipt()) {
            let bytes = Envelope::Receipt(msg.clone()).to_bytes();
            let decoded = Envelope::from_bytes(&bytes).expect("decode");
            prop_assert_eq!(decoded, Envelope::Receipt(msg));
        }
    }

    #[test]
    fn malformed_payload_is_a_decode_error_not_a_panic() {
        let garbage = Bytes::from_static(b"not json at all");
        assert!(Envelope::from_bytes(&garbage).is_err());
    }
}
