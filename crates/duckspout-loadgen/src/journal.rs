//! The loadgen's journal: one per-node NDJSON file, in the exact
//! `{node, seq, event}` shape nodes themselves produce (D-6, §3.7), with
//! payload identity riding along as extra JSON fields on the `ClientAck` /
//! `ClientTimeout` lines it journals (§8.4: "journals every `ClientAck`
//! received, with payload identity").
//!
//! Why not just `duckspout_ctk::NdjsonTraceWriter` verbatim: its
//! `TraceSink::record` takes a bare [`TraceEvent`] with nowhere to carry a
//! payload, by design — the frozen §3.3 vocabulary's variants are
//! payload-free (`duckspout_types::trace`'s module docs). This writer
//! mirrors its discipline exactly (dense per-node seq starting at 0, one
//! flushed line per event, one mutex) but serializes a locally-flattened
//! line instead, so the extra fields ride *beside* `node`/`seq`/`event`
//! rather than inside the frozen enum. `TraceRecord::from_ndjson_line`
//! decodes these lines unchanged (`serde`'s default is to ignore unknown
//! fields), and `scripts/trace-conformance.mjs`'s decoder only checks
//! `node`/`event`/`seq` — so this journal is a strict superset of what a
//! node's journal contains, not a divergent format.
//!
//! What is deliberately **not** journaled here: a "request sent" line. §3.3
//! has no such action (the vocabulary is closed at 27 node-journaled
//! variants plus `ClientTimeout`, D-6) — a client-side "sent" marker with no
//! ClientAck/ClientTimeout to follow it (the loadgen crashed mid-run) is
//! exactly the §8.4 vacuity-teeth case "a node whose journals simply stop…
//! accuses nothing and certifies nothing," which already covers a vanished
//! loadgen the same way it covers a vanished node. The identity needed to
//! journal a *resolution* is tracked in memory by the caller (`crate::client`)
//! between send and resolution instead.

use std::io::Write;
use std::sync::Mutex;

use duckspout_types::{NodeId, TraceEvent};
use serde::Serialize;

/// The identity of one sent write request (§8.4's "payload identity"):
/// enough for a future judge (#205) to match a journaled `ClientAck` back to
/// the specific records it covers. `request_id` doubles as the OTLP
/// `x-duckspout-idempotency-key` the loadgen sends with the request
/// (§4.4.1), so it is also the identity the accept-side dedup key would use
/// — the same request, named once.
#[derive(Debug, Clone, Serialize)]
pub struct RequestIdentity {
    /// The idempotency key this request was sent with.
    pub request_id: String,
    /// The tenant the request was sent as.
    pub tenant: String,
    /// Number of log records the request carried.
    pub record_count: usize,
}

/// One journaled line: the frozen `{node, seq, event}` triple plus the
/// sent request's identity, flattened onto the same JSON object.
#[derive(Serialize)]
struct JournalLine<'a> {
    node: &'a str,
    seq: u64,
    event: TraceEvent,
    #[serde(flatten)]
    identity: &'a RequestIdentity,
}

struct Inner<W> {
    next_seq: u64,
    out: W,
}

/// The loadgen's journal writer (module docs).
pub struct LoadgenJournal<W: Write + Send> {
    node: NodeId,
    inner: Mutex<Inner<W>>,
}

impl<W: Write + Send> LoadgenJournal<W> {
    /// A journal for `node`, starting at seq 0, writing to `out`.
    #[must_use]
    pub fn new(node: NodeId, out: W) -> Self {
        Self {
            node,
            inner: Mutex::new(Inner { next_seq: 0, out }),
        }
    }

    /// Journals a `ClientAck` for `identity` (§4.3, §8.4).
    ///
    /// # Panics
    ///
    /// On serialization or write failure — a journal that silently drops an
    /// event certifies a run that never happened as recorded (R-3), so this
    /// fails loud rather than limping on, matching
    /// `duckspout_ctk::NdjsonTraceWriter`'s own contract.
    pub fn record_client_ack(&self, identity: &RequestIdentity) {
        self.record(TraceEvent::ClientAck, identity);
    }

    /// Journals a `ClientTimeout` for `identity` — the one event only
    /// `duckspout-loadgen` may journal (§3.7, §8.4).
    ///
    /// # Panics
    ///
    /// Same as [`Self::record_client_ack`].
    pub fn record_client_timeout(&self, identity: &RequestIdentity) {
        self.record(TraceEvent::ClientTimeout, identity);
    }

    fn record(&self, event: TraceEvent, identity: &RequestIdentity) {
        let mut inner = self.inner.lock().expect("loadgen journal lock poisoned");
        let line = JournalLine {
            node: self.node.as_str(),
            seq: inner.next_seq,
            event,
            identity,
        };
        let text = serde_json::to_string(&line).expect("journal line serializes");
        inner.next_seq += 1;
        writeln!(inner.out, "{text}").expect("journal write");
        inner.out.flush().expect("journal flush");
    }

    /// Unwraps the journal, returning the underlying output.
    ///
    /// # Panics
    ///
    /// If a call panicked mid-record (the journal is then suspect).
    #[must_use]
    pub fn into_inner(self) -> W {
        self.inner
            .into_inner()
            .expect("loadgen journal lock poisoned")
            .out
    }
}

#[cfg(test)]
mod tests {
    use duckspout_types::TraceRecord;

    use super::*;

    fn identity(id: &str) -> RequestIdentity {
        RequestIdentity {
            request_id: id.to_owned(),
            tenant: "tenant-a".to_owned(),
            record_count: 7,
        }
    }

    #[test]
    fn client_ack_line_carries_the_frozen_shape_and_identity() {
        let journal = LoadgenJournal::new(NodeId::new("loadgen-0"), Vec::new());
        journal.record_client_ack(&identity("req-1"));
        let out = String::from_utf8(journal.into_inner()).expect("utf8");
        let value: serde_json::Value = serde_json::from_str(out.trim_end()).unwrap();
        assert_eq!(value["node"], "loadgen-0");
        assert_eq!(value["seq"], 0);
        assert_eq!(value["event"], "ClientAck");
        assert_eq!(value["request_id"], "req-1");
        assert_eq!(value["tenant"], "tenant-a");
        assert_eq!(value["record_count"], 7);
    }

    #[test]
    fn client_timeout_line_carries_the_frozen_shape_and_identity() {
        let journal = LoadgenJournal::new(NodeId::new("loadgen-0"), Vec::new());
        journal.record_client_timeout(&identity("req-2"));
        let out = String::from_utf8(journal.into_inner()).expect("utf8");
        let value: serde_json::Value = serde_json::from_str(out.trim_end()).unwrap();
        assert_eq!(value["event"], "ClientTimeout");
        assert_eq!(value["request_id"], "req-2");
    }

    #[test]
    fn seqs_stay_dense_across_acks_and_timeouts() {
        let journal = LoadgenJournal::new(NodeId::new("loadgen-0"), Vec::new());
        journal.record_client_ack(&identity("req-1"));
        journal.record_client_timeout(&identity("req-2"));
        journal.record_client_ack(&identity("req-3"));
        let out = String::from_utf8(journal.into_inner()).expect("utf8");
        let seqs: Vec<u64> = out
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["seq"].as_u64().unwrap())
            .collect();
        assert_eq!(seqs, vec![0, 1, 2]);
    }

    /// The extra identity fields must not break the frozen decoder — a
    /// future judge or `docs/trace-mapping.md`-paired tool parsing this
    /// journal as a plain `TraceRecord` stream (ignoring what it does not
    /// know about) must still see valid, dense-seq `ClientAck`/`ClientTimeout`
    /// records. Would catch a change to the line shape (e.g. nesting the
    /// identity under a sub-object, or renaming `node`/`seq`/`event`) that
    /// silently stopped being a superset of the frozen format.
    #[test]
    fn a_journal_line_decodes_as_a_plain_trace_record_too() {
        let journal = LoadgenJournal::new(NodeId::new("loadgen-0"), Vec::new());
        journal.record_client_ack(&identity("req-1"));
        let out = String::from_utf8(journal.into_inner()).expect("utf8");
        let record = TraceRecord::from_ndjson_line(out.trim_end()).expect("decodes as TraceRecord");
        assert_eq!(record.node, NodeId::new("loadgen-0"));
        assert_eq!(record.seq, 0);
        assert_eq!(record.event, TraceEvent::ClientAck);
    }
}
