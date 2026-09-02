//! The NDJSON trace writer: the [`TraceSink`] port's capture-side
//! implementation (§3.7, §8.2).
//!
//! The port trait lives in `duckspout-types` (the emitters are protocol
//! crates, which depend on the types crate only — ADR-0008); the writer
//! lives here because it is I/O, and the types crate is I/O-free by
//! charter. One writer per journaling node: it stamps the node id, assigns
//! the dense per-node sequence (D-6), and writes **one flushed NDJSON line
//! per event** — the §3.7 discipline that makes a crash-truncated journal a
//! clean prefix rather than a torn line.

use std::io::Write;
use std::sync::Mutex;

use duckspout_types::{NodeId, TraceEvent, TraceRecord, TraceSink};

/// A [`TraceSink`] writing [`TraceRecord`] NDJSON lines to any [`Write`]
/// (a file for the capture harness, a `Vec<u8>` for tests).
///
/// The sequence counter lives inside the same mutex as the writer, so line
/// order in the journal always equals seq order — two racing tracepoints
/// can never journal out of order. Plain `std::sync` on purpose: the writer
/// is blocking I/O, not one of the deterministic doubles, so it sits
/// outside the loom exploration surface.
pub struct NdjsonTraceWriter<W: Write + Send> {
    node: NodeId,
    inner: Mutex<Inner<W>>,
}

struct Inner<W> {
    next_seq: u64,
    out: W,
}

impl<W: Write + Send> NdjsonTraceWriter<W> {
    /// A writer journaling as `node`, starting at seq 0.
    #[must_use]
    pub fn new(node: NodeId, out: W) -> Self {
        Self {
            node,
            inner: Mutex::new(Inner { next_seq: 0, out }),
        }
    }

    /// The journaling node identity.
    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// Unwraps the writer, returning the underlying output.
    ///
    /// # Panics
    ///
    /// If a tracepoint panicked mid-record (the journal is then suspect;
    /// fail loud, R-3).
    #[must_use]
    pub fn into_inner(self) -> W {
        self.inner
            .into_inner()
            .expect("trace writer lock poisoned")
            .out
    }
}

impl<W: Write + Send> TraceSink for NdjsonTraceWriter<W> {
    /// # Panics
    ///
    /// On serialization or write failure: a tracepoint that silently drops
    /// events would certify runs that never happened as recorded (R-3) —
    /// the harness fails loud instead.
    fn record(&self, event: TraceEvent) {
        let mut inner = self.inner.lock().expect("trace writer lock poisoned");
        let record = TraceRecord {
            node: self.node.clone(),
            seq: inner.next_seq,
            event,
        };
        let line = record.to_ndjson_line().expect("trace record serializes");
        inner.next_seq += 1;
        writeln!(inner.out, "{line}").expect("trace journal write");
        inner.out.flush().expect("trace journal flush");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn writes_one_flushed_line_per_event_with_dense_seqs() {
        let writer = NdjsonTraceWriter::new(NodeId::new("n1"), Vec::new());
        writer.record(TraceEvent::Accept);
        writer.record(TraceEvent::StageCommit);
        writer.record(TraceEvent::ClientAck);
        let out = String::from_utf8(writer.into_inner()).expect("utf8");
        assert_eq!(
            out,
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"ClientAck\"}\n"
        );
    }

    #[test]
    fn shared_across_subsystems_the_seq_stays_dense() {
        // The composition shares ONE writer per node across accept, staging,
        // and drain (D-6: per-NODE seqs, not per-subsystem).
        let writer = Arc::new(NdjsonTraceWriter::new(NodeId::new("n1"), Vec::new()));
        let sink: Arc<dyn TraceSink> = Arc::clone(&writer) as _;
        for _ in 0..3 {
            sink.record(TraceEvent::Heartbeat);
        }
        drop(sink);
        let writer = Arc::into_inner(writer).expect("the sink alias was dropped");
        let out = String::from_utf8(writer.into_inner()).expect("utf8");
        let seqs: Vec<u64> = out
            .lines()
            .map(|l| TraceRecord::from_ndjson_line(l).expect("line decodes").seq)
            .collect();
        assert_eq!(seqs, vec![0, 1, 2]);
    }
}
