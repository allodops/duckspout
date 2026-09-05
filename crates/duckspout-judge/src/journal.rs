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
    /// range this ack covers. ALIASES across loadgen fleet members and
    /// across one member's restart (ACPR finding HIGH-2) — never use this
    /// bare as a global correlation key; combine it with
    /// `source_incarnation` below.
    pub first_index: u64,
    /// The `(node, start_nonce)` pair naming the loadgen process incarnation
    /// that sent this request (ACPR HIGH-2,
    /// `duckspout_loadgen::client::source_incarnation`'s wire shape) —
    /// together with `first_index` this is what makes a record's identity
    /// globally unique across the whole fleet's lifetime, not just within
    /// one process. The predicate keys its `FinalSystemState` lookups on
    /// `{source_incarnation}-{index}`, matching the exact string
    /// `duckspout_loadgen::client::synthetic_batch` embeds in the record's
    /// own `loadgen.index` attribute.
    pub source_incarnation: String,
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
    /// Checked BOTH within one file (`parse_journal_file`) and, since ACPR
    /// finding MEDIUM-HIGH-3(c), across every file ingested together
    /// (`ingest_journals`'s cross-file re-check) — so the same `(node, seq)`
    /// reappearing in a second file (a file passed twice, or a rotated
    /// journal re-fed by mistake) is a repeat, caught the same way a repeat
    /// within one file would be, rather than silently double-counted.
    #[error(
        "{path}:{line_no}: node {node} seq {got}, expected {expected} \
         (D-6: dense per-node seqs starting at 0, tracked across every \
         journal file ingested together)"
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
///
/// Also rejects, as the same kind of corruption (ACPR finding
/// MEDIUM-HIGH-3(b)): a `first_index`/`record_count` pair whose sum
/// overflows `u64`. Left unchecked, a predicate computing
/// `first_index..first_index + record_count` over this identity would
/// either panic (debug) or silently wrap to an empty, vacuously-passing
/// range (release) — neither is the fail-closed contract this module
/// promises, so the check happens once, here, at decode time, rather than
/// trusting every future caller to redo it correctly.
fn identity_from_rest(
    rest: &serde_json::Value,
) -> Result<Option<RequestIdentity>, serde_json::Error> {
    match rest {
        serde_json::Value::Object(map) if map.contains_key("request_id") => {
            let identity: RequestIdentity = serde_json::from_value(rest.clone())?;
            if identity
                .first_index
                .checked_add(identity.record_count as u64)
                .is_none()
            {
                return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "first_index {} + record_count {} overflows u64 — fails closed rather \
                     than panicking or silently wrapping to an empty range",
                    identity.first_index, identity.record_count
                )));
            }
            Ok(Some(identity))
        }
        _ => Ok(None),
    }
}

/// A structural check for a duplicate JSON object key at the TOP LEVEL of
/// one line (ACPR finding MEDIUM-HIGH-3(d)): `serde_json`'s own `Map` (the
/// `preserve_order` feature included) silently keeps "last value wins" on a
/// repeated key, with no built-in opt-in to reject it instead. That is
/// dangerous here specifically: a line with `tenant` duplicated could
/// silently reclassify a real tenant's ack as the system tenant `_self` (or
/// vice versa) and have it wrongly excluded or included.
///
/// Reasonable-effort fix, not a general JSON-validation library: every
/// journal line this crate ever decodes is one FLAT object (`node`/`seq`/
/// `event` plus, at most, the identity fields alongside them — module
/// docs' wire shape) — none of them nest a key one level deeper — so this
/// only walks the top-level object's keys (via a `serde::de::Visitor` that
/// never *builds* a map, so a duplicate key is never lost to the same
/// collapsing a normal deserialize would do) and discards each value with
/// [`serde::de::IgnoredAny`] rather than recursing into it. If the wire
/// shape ever grows a nested object, a duplicate key one level down would
/// not be caught by this check — an intentionally narrow scope matching
/// what this format actually is today, not a claim of full generality.
struct RejectDuplicateKeys;

impl<'de> serde::de::Deserialize<'de> for RejectDuplicateKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct DupKeyVisitor;

        impl<'de> serde::de::Visitor<'de> for DupKeyVisitor {
            type Value = ();

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut seen = std::collections::HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate JSON key {key:?} (ambiguity fails closed — \
                             last-value-wins could silently reclassify identity fields)"
                        )));
                    }
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(())
            }
        }

        deserializer.deserialize_map(DupKeyVisitor).map(|()| Self)
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
        serde_json::from_str::<RejectDuplicateKeys>(raw).map_err(|source| {
            JournalError::Decode {
                path: path.to_owned(),
                line_no,
                source,
            }
        })?;
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
/// Each file's OWN seq density is checked per-file by [`parse_journal_file`]
/// (starting at 0 there, since one file may legitimately be one node's
/// complete, self-contained journal). This function additionally re-checks
/// density ACROSS every file, in ingestion order (ACPR finding
/// MEDIUM-HIGH-3(c)): `parse_journal_file`'s own per-file check cannot catch
/// the same file being passed twice, or a rotated/split journal fed
/// out-of-order or duplicated, because each file looks internally dense
/// starting from 0 on its own — the bug is only visible once seqs are
/// tracked per node across the WHOLE run.
///
/// # Errors
///
/// Returns the first [`JournalError`] encountered: an I/O or decode failure
/// from an individual file (in `paths` order), or a cross-file
/// [`JournalError::NonDenseSeq`] (in ingestion order) if no single file had
/// one.
pub fn ingest_journals(paths: &[PathBuf]) -> Result<JournalSet, JournalError> {
    let mut lines = Vec::new();
    for path in paths {
        lines.extend(parse_journal_file(path)?);
    }
    check_cross_file_density(&lines)?;
    Ok(JournalSet { lines })
}

/// The cross-file half of `ingest_journals`'s seq-density check (module
/// docs): replays every line, in ingestion order, tracking each node's next
/// expected seq across ALL files together rather than per file.
fn check_cross_file_density(lines: &[JournalLine]) -> Result<(), JournalError> {
    let mut next_seq: HashMap<NodeId, u64> = HashMap::new();
    for line in lines {
        let expected = *next_seq.get(&line.node).unwrap_or(&0);
        if line.seq != expected {
            return Err(JournalError::NonDenseSeq {
                path: line.source.clone(),
                line_no: line.line_no,
                node: line.node.clone(),
                expected,
                got: line.seq,
            });
        }
        next_seq.insert(line.node.clone(), expected + 1);
    }
    Ok(())
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
             \"record_count\":7,\"first_index\":42,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let identity = lines[0].identity.as_ref().expect("identity present");
        assert_eq!(identity.request_id, "req-1");
        assert_eq!(identity.tenant, "tenant-a");
        assert_eq!(identity.record_count, 7);
        assert_eq!(identity.first_index, 42);
        assert_eq!(identity.source_incarnation, "loadgen-0-1000");
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
             \"request_id\":\"req-1\",\"tenant\":\"t\",\"record_count\":1,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
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

    #[test]
    fn an_overflowing_index_range_is_a_decode_error_not_a_panic_or_wraparound() {
        // ACPR finding MEDIUM-HIGH-3(b): would catch the predicate's range
        // arithmetic panicking (debug) or silently wrapping to an empty,
        // vacuously-passing range (release) instead of failing closed.
        let file = write_journal(&format!(
            "{{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"tenant-a\",\
             \"record_count\":10,\"first_index\":{},\
             \"source_incarnation\":\"loadgen-0-1000\"}}\n",
            u64::MAX - 1
        ));
        let err = parse_journal_file(file.path()).expect_err("must fail closed on overflow");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_duplicate_json_key_fails_closed() {
        // ACPR finding MEDIUM-HIGH-3(d): a duplicated `tenant` key could
        // silently reclassify a real tenant's ack as the system tenant
        // (last-value-wins) — this must be rejected, not silently resolved.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"tenant-a\",\"tenant\":\"_self\",\
             \"record_count\":1,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed on duplicate key");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_repeated_node_seq_across_two_files_fails_closed() {
        // ACPR finding MEDIUM-HIGH-3(c): the same file passed twice (or a
        // rotated journal re-fed by mistake) must not be silently
        // double-counted just because each file looks dense-from-0 on its
        // own — density must hold across the whole ingested run.
        let f1 = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let f2 = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let err = ingest_journals(&[f1.path().to_owned(), f2.path().to_owned()])
            .expect_err("must fail closed on cross-file repeat");
        assert!(matches!(
            err,
            JournalError::NonDenseSeq {
                expected: 2,
                got: 0,
                ..
            }
        ));
    }

    #[test]
    fn distinct_nodes_across_files_are_unaffected_by_the_cross_file_check() {
        // The non-regression case for the same fix: files covering
        // DIFFERENT nodes must aggregate normally — the cross-file density
        // check must not spuriously conflate unrelated nodes' seq counters.
        let f1 = write_journal("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let f2 = write_journal("{\"node\":\"n2\",\"seq\":0,\"event\":\"Accept\"}\n");
        let set = ingest_journals(&[f1.path().to_owned(), f2.path().to_owned()]).expect("ingests");
        assert_eq!(set.lines.len(), 2);
    }
}
