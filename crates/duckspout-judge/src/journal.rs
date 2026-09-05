//! NDJSON journal ingestion (§8.4, D-6): the shared plumbing every judge
//! predicate (#205's own zero-acked-lost, and #206/#207/#208's future ones)
//! parses fleet + load-generator journals through.
//!
//! Every journal file — one per fleet node, plus the load generator's own
//! (`duckspout_loadgen::journal`) — is the frozen `{node, seq, event}`
//! triple (`duckspout_types::TraceRecord`) one JSON object per line (D-6).
//! The loadgen's `ClientAck`/`ClientTimeout` lines additionally carry
//! payload identity as extra fields flattened onto the same object
//! (`duckspout_loadgen::journal::RequestIdentity`'s wire shape). This module
//! decodes that shape **structurally** — by field presence, not by trusting
//! which file it came from — without depending on the `duckspout-loadgen`
//! crate at all: a judge parses the wire format the same way a real OTLP
//! client speaks a wire protocol without linking the server it talks to
//! (`duckspout_loadgen::client`'s own module docs make the identical call
//! for the identical reason). `duckspout-accept` also journals `ClientAck`
//! on the node side (`docs/trace-mapping.md`), and those lines carry no
//! identity fields — this module keeps both shapes as one line type with an
//! `Option<RequestIdentity>`, so a plain node-journaled `ClientAck` is not a
//! decode error, just a line with no identity to key on.
//!
//! Malformed input fails the whole ingestion closed
//! (`docs/verification.md` §8.4's "skipped ≠ passed, ambiguity fails
//! closed" posture, echoed in `duckspout_types::trace`'s R-3 fail-loud
//! discipline for the writer side of this same format): a judge that
//! silently drops or ignores an unparseable line could certify a run that
//! never happened as recorded. One bad line anywhere fails the entire
//! ingestion — never a partial, silently-truncated result.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use duckspout_types::{NodeId, TraceEvent};
use serde::Deserialize;

/// The loadgen's payload identity, decoded structurally off a journal
/// line's extra fields (module docs) — field-for-field the wire shape of
/// `duckspout_loadgen::journal::RequestIdentity`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RequestIdentity {
    /// The idempotency key the request was sent with.
    pub request_id: String,
    /// The tenant the request was sent as.
    pub tenant: String,
    /// Number of log records the request carried.
    pub record_count: usize,
    /// The 0-based index of the first record the request carried — together
    /// with `record_count`, the `[first_index, first_index + record_count)`
    /// range this ack covers.
    pub first_index: u64,
}

/// One decoded journal line: the frozen envelope plus, when present, the
/// loadgen's payload identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalLine {
    /// The journal file this line came from (diagnostics only).
    pub source: PathBuf,
    /// 1-based line number within `source` (diagnostics only).
    pub line_no: usize,
    /// The journaling node (the loadgen fleet member's id, for
    /// loadgen-journaled lines).
    pub node: NodeId,
    /// The node-local dense sequence number (D-6).
    pub seq: u64,
    /// The journaled event.
    pub event: TraceEvent,
    /// Present exactly on lines carrying a `request_id` field — in
    /// practice the loadgen's own `ClientAck`/`ClientTimeout` lines (module
    /// docs).
    pub identity: Option<RequestIdentity>,
}

/// A parsed, queryable set of journal lines from every ingested file
/// (§8.4: "reconstruction of the run's event history keyed by node/seq").
#[derive(Debug, Default, Clone)]
pub struct JournalSet {
    /// Every decoded line, in ingestion order (file order, then line order
    /// within each file) — NOT globally seq-sorted, since seqs are only
    /// dense per node, not across nodes.
    pub lines: Vec<JournalLine>,
}

impl JournalSet {
    /// Every line for `event` that carries a payload identity, in ingestion
    /// order — the primary lookup #205's zero-acked-lost predicate needs,
    /// and reusable by any future predicate keying on an identity-bearing
    /// event.
    pub fn identity_events(
        &self,
        event: TraceEvent,
    ) -> impl Iterator<Item = (&JournalLine, &RequestIdentity)> {
        self.lines.iter().filter_map(move |line| {
            if line.event == event {
                line.identity.as_ref().map(|identity| (line, identity))
            } else {
                None
            }
        })
    }
}

/// Ingestion failure — fails the run closed rather than skipping the bad
/// line (module docs).
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// The journal file could not be read at all.
    #[error("reading journal {path}: {source}")]
    Io {
        /// The journal file that failed to open/read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// One line was not a valid `{node, seq, event, ...}` object, or its
    /// identity-shaped extra fields did not decode as [`RequestIdentity`].
    #[error("{path}:{line_no}: not a valid journal line: {source}")]
    Decode {
        /// The journal file the bad line came from.
        path: PathBuf,
        /// The 1-based line number.
        line_no: usize,
        /// The underlying decode error.
        #[source]
        source: serde_json::Error,
    },
    /// A node's seq did not continue the dense, zero-based sequence D-6
    /// requires — a gap (lost lines), a repeat, or an out-of-order line.
    #[error(
        "{path}:{line_no}: node {node} seq {got}, expected {expected} \
         (D-6: dense per-node seqs starting at 0)"
    )]
    NonDenseSeq {
        /// The journal file the bad line came from.
        path: PathBuf,
        /// The 1-based line number.
        line_no: usize,
        /// The offending node.
        node: NodeId,
        /// The seq the writer's own discipline required next.
        expected: u64,
        /// The seq the line actually carried.
        got: u64,
    },
}

/// The frozen envelope every journal line carries, with everything else
/// (the loadgen's identity fields, if present) captured verbatim so it can
/// be decoded a second time, more strictly, only when it is actually there.
#[derive(Deserialize)]
struct Envelope {
    node: NodeId,
    seq: u64,
    event: TraceEvent,
    #[serde(flatten)]
    rest: serde_json::Value,
}

/// Decodes `rest` (whatever is left on the line after `node`/`seq`/`event`)
/// into a [`RequestIdentity`] when it looks like one, by the presence of
/// `request_id` (module docs' "structurally, by field presence" rule). A
/// `rest` with no `request_id` field is a plain envelope line (e.g. a
/// node-journaled `ClientAck`, or any payload-free event) — not an error.
/// A `rest` WITH a `request_id` field that does not fully decode as
/// [`RequestIdentity`] IS an error: a half-formed identity is corruption,
/// not "no identity here."
fn identity_from_rest(
    rest: &serde_json::Value,
) -> Result<Option<RequestIdentity>, serde_json::Error> {
    match rest {
        serde_json::Value::Object(map) if map.contains_key("request_id") => {
            serde_json::from_value(rest.clone()).map(Some)
        }
        _ => Ok(None),
    }
}

/// Parses one journal file's lines, fully — the first malformed line fails
/// the whole file (module docs).
///
/// # Errors
///
/// Returns [`JournalError`] on the first I/O failure, undecodable line, or
/// seq-density violation.
pub fn parse_journal_file(path: &Path) -> Result<Vec<JournalLine>, JournalError> {
    let text = std::fs::read_to_string(path).map_err(|source| JournalError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut next_seq: HashMap<NodeId, u64> = HashMap::new();
    let mut lines = Vec::new();
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        // No special-casing for a blank line: `str::lines` never emits a
        // trailing empty element for a `\n`-terminated string (every writer
        // here flushes exactly one `\n` per line, `duckspout_ctk::trace_writer`
        // / `duckspout_loadgen::journal` module docs), so an empty `raw`
        // here can only mean a genuinely blank line *inside* the file —
        // which `serde_json::from_str` below already rejects on its own
        // (an empty string is not a JSON object), failing this line closed
        // exactly like any other malformed one rather than needing a
        // separate branch.
        let envelope: Envelope =
            serde_json::from_str(raw).map_err(|source| JournalError::Decode {
                path: path.to_owned(),
                line_no,
                source,
            })?;
        let expected = *next_seq.get(&envelope.node).unwrap_or(&0);
        if envelope.seq != expected {
            return Err(JournalError::NonDenseSeq {
                path: path.to_owned(),
                line_no,
                node: envelope.node,
                expected,
                got: envelope.seq,
            });
        }
        next_seq.insert(envelope.node.clone(), expected + 1);
        let identity =
            identity_from_rest(&envelope.rest).map_err(|source| JournalError::Decode {
                path: path.to_owned(),
                line_no,
                source,
            })?;
        lines.push(JournalLine {
            source: path.to_owned(),
            line_no,
            node: envelope.node,
            seq: envelope.seq,
            event: envelope.event,
            identity,
        });
    }
    Ok(lines)
}

/// Ingests every journal file in `paths` into one queryable [`JournalSet`]
/// (§8.4). The first malformed line, in any file, fails the whole
/// ingestion — never a partial or silently-skipped result (module docs).
///
/// # Errors
///
/// Returns the first [`JournalError`] encountered, across all files in
/// `paths` order.
pub fn ingest_journals(paths: &[PathBuf]) -> Result<JournalSet, JournalError> {
    let mut lines = Vec::new();
    for path in paths {
        lines.extend(parse_journal_file(path)?);
    }
    Ok(JournalSet { lines })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn write_journal(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(text.as_bytes()).expect("write");
        file
    }

    #[test]
    fn parses_plain_node_lines_with_no_identity() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"ClientAck\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        assert_eq!(lines.len(), 3);
        assert!(lines.iter().all(|l| l.identity.is_none()));
        assert_eq!(lines[2].event, TraceEvent::ClientAck);
    }

    #[test]
    fn extracts_identity_from_loadgen_client_ack_lines() {
        // Would catch the identity-extraction logic silently treating the
        // loadgen's own richer `ClientAck` line as a plain envelope line —
        // the exact shape `duckspout_loadgen::journal::LoadgenJournal`
        // produces.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"tenant-a\",\
             \"record_count\":7,\"first_index\":42}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let identity = lines[0].identity.as_ref().expect("identity present");
        assert_eq!(identity.request_id, "req-1");
        assert_eq!(identity.tenant, "tenant-a");
        assert_eq!(identity.record_count, 7);
        assert_eq!(identity.first_index, 42);
    }

    #[test]
    fn a_half_formed_identity_is_a_decode_error_not_a_silent_downgrade() {
        // Would catch treating corruption (a `request_id` present but the
        // rest of the identity shape missing/wrong-typed) as "just no
        // identity here" — ambiguity must fail closed, not get quietly
        // reinterpreted as a plain envelope line.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn malformed_json_fails_closed() {
        let file = write_journal("{not json}\n");
        let err = parse_journal_file(file.path()).expect_err("must fail");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_blank_line_fails_closed_rather_than_being_silently_skipped() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail on the blank line");
        assert!(matches!(err, JournalError::Decode { line_no: 2, .. }));
    }

    #[test]
    fn a_seq_gap_fails_closed() {
        // Would catch silently accepting a journal with a missing line
        // (e.g. a torn write that dropped one event) as if it were complete.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"StageCommit\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail on the gap");
        assert!(matches!(
            err,
            JournalError::NonDenseSeq {
                line_no: 2,
                expected: 1,
                got: 2,
                ..
            }
        ));
    }

    #[test]
    fn each_node_keeps_its_own_dense_seq_within_one_file() {
        // Multiple nodes' lines can legitimately interleave inside one
        // captured stream; D-6's density is per-node, not global.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n2\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n\
             {\"node\":\"n2\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        assert_eq!(lines.len(), 4);
    }

    #[test]
    fn ingest_journals_aggregates_multiple_files_in_order() {
        let f1 = write_journal("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let f2 = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"t\",\"record_count\":1,\"first_index\":0}\n",
        );
        let set = ingest_journals(&[f1.path().to_owned(), f2.path().to_owned()]).expect("ingests");
        assert_eq!(set.lines.len(), 2);
        assert_eq!(set.identity_events(TraceEvent::ClientAck).count(), 1);
    }

    #[test]
    fn ingest_journals_fails_closed_if_any_file_is_bad() {
        // Would catch a partial-ingestion bug where a later good file's
        // lines get returned even though an earlier file was corrupt —
        // exactly the "skipped ≠ passed" gap this module must not have.
        let good = write_journal("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let bad = write_journal("not json at all\n");
        let err = ingest_journals(&[good.path().to_owned(), bad.path().to_owned()])
            .expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { .. }));
    }

    #[test]
    fn a_missing_journal_file_is_an_io_error() {
        let missing = PathBuf::from("/nonexistent/duckspout-judge-test-journal.ndjson");
        let err = parse_journal_file(&missing).expect_err("must fail");
        assert!(matches!(err, JournalError::Io { .. }));
    }
}
