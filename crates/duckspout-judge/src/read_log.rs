//! The served-read log (§8.4's Q-shaped judge, query side): what the fleet's
//! query client asked for, and what it was actually served.
//!
//! # Why this is a separate file, and not a journal
//!
//! The Q-shaped judge grades READS, and **§3 has no read action** — stated
//! outright in `specs/formal-core.md`'s `CacheTransparency` note ("the theorem
//! quantifies over every `complete` read's *answer*, and §3 has no read
//! action"). The trace vocabulary is exactly the §3.3 action names, verbatim
//! and frozen (`duckspout_types::trace`, `docs/trace-mapping.md`), so there
//! is no journal event a served read could ever ride, and inventing one
//! would be a change to the specification's own action system, not to this
//! judge. The query client therefore writes its own NDJSON log — one object
//! per read it issued — and this module ingests it beside the journals.
//!
//! Ingestion posture is the journals' posture exactly (`crate::journal`):
//! one bad line fails the whole file closed, and a repeated top-level JSON
//! key is rejected rather than silently resolved last-value-wins (here the
//! hazard is a duplicated `concern`, which would silently reclassify a
//! `complete` read — the only kind this judge grades — as an `available`
//! one, or the reverse).
//!
//! # Producer status
//!
//! Nothing in this workspace writes this file yet: the fleet's query client
//! lands with the distributed tier's own wiring (#204/#208), exactly as
//! `crate::final_state`'s read-back does. A judge run given no read log
//! reports `NoVerdict` for the served half of watermark honesty rather than
//! a vacuous pass (§8.4's vacuity teeth).
//!
//! # Honest limits of this shape
//!
//! A served entry lists the record identity of every row the answer
//! contained. That is the right shape for a judge — a missing row is only
//! detectable against what was actually returned — but it is NOT free at
//! fleet scale: an answer spanning millions of rows produces a line of
//! millions of identities. The same honesty `crate::final_state` keeps about
//! its per-record `contains` applies here: a real query client may well need
//! a more compact encoding (a sorted range form, or a per-partition digest
//! the judge can probe), and this module does not claim to have designed it.
//! What it does not do is sample: a subset-checked answer would silently
//! stop being able to convict the rows it dropped.
//!
//! # Wire shape
//!
//! ```json
//! {"tenant":"t","partition":"t0-s0","concern":"complete","outcome":"served",
//!  "complete_through_ms":1700,"record_keys":["loadgen-0-1000-0"]}
//! {"tenant":"t","partition":"t0-s0","concern":"complete","outcome":"refused",
//!  "reason":"holder unreachable"}
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use duckspout_types::PartitionId;
use serde::Deserialize;

/// The read concern a read was issued under (§7.6). `complete` is the
/// default and fails closed; `available` may narrow silently, which is why
/// the judge grades only `complete` reads (a narrowed `available` answer is
/// correct by definition, so treating it as evidence of completeness would
/// manufacture violations out of documented behaviour).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConcern {
    /// `duckspout_read_concern = 'complete'` — the default (§7.6).
    Complete,
    /// `duckspout_read_concern = 'available'` — narrows silently (§7.6).
    Available,
}

/// What the read actually returned.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReadOutcome {
    /// An answer was served, over the coverage the server claimed at
    /// serving time.
    Served {
        /// The `complete_through` the answer was served at — the pinned
        /// watermark the bind resolved against (§7.6's per-transaction
        /// pinning), Unix milliseconds.
        complete_through_ms: i64,
        /// The `loadgen.index` attribute value
        /// (`{source_incarnation}-{index}`,
        /// `duckspout_loadgen::client::synthetic_batch`) of every record
        /// the answer contained — the same identity `crate::final_state`
        /// keys on, so an acked record can be looked for in a served
        /// answer.
        record_keys: BTreeSet<String>,
    },
    /// The read was refused — a typed error naming the uncovered cells
    /// (§7.6). A refusal is a CORRECT outcome for this judge: fail-closed
    /// is the promise, so a refusal can never be a violation.
    Refused {
        /// The refusal reason, verbatim from the client (diagnostics only).
        reason: String,
    },
}

/// One read the query client issued and the answer it got.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReadRecord {
    /// The tenant the read was issued as — matched against the acking
    /// client's own tenant (`crate::journal::RequestIdentity::tenant`).
    pub tenant: String,
    /// The partition the read covered. Watermarks are per-partition (§7.3),
    /// so this is the scope every coverage claim is judged in.
    pub partition: PartitionId,
    /// The concern the read ran under (§7.6).
    pub concern: ReadConcern,
    /// The answer.
    #[serde(flatten)]
    pub outcome: ReadOutcome,
}

/// Read-log ingestion failure — fails the run closed, never skipping a bad
/// line (module docs).
#[derive(Debug, thiserror::Error)]
pub enum ReadLogError {
    /// The read log could not be read at all.
    #[error("reading read log {path}: {source}")]
    Io {
        /// The read log that failed to open/read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// One line was not a valid [`ReadRecord`].
    #[error("{path}:{line_no}: not a valid read-log line: {source}")]
    Decode {
        /// The read log the bad line came from.
        path: PathBuf,
        /// The 1-based line number.
        line_no: usize,
        /// The underlying decode error.
        #[source]
        source: serde_json::Error,
    },
}

/// Parses a whole read log. The first malformed line fails the whole file
/// (module docs).
///
/// # Errors
///
/// Returns [`ReadLogError`] on an I/O failure or the first undecodable line.
pub fn parse_read_log(path: &Path) -> Result<Vec<ReadRecord>, ReadLogError> {
    let text = std::fs::read_to_string(path).map_err(|source| ReadLogError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut records = Vec::new();
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let decode_error = |source| ReadLogError::Decode {
            path: path.to_owned(),
            line_no,
            source,
        };
        crate::journal::reject_duplicate_keys(raw).map_err(decode_error)?;
        records.push(serde_json::from_str(raw).map_err(decode_error)?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn write_log(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(text.as_bytes()).expect("write");
        file
    }

    #[test]
    fn parses_a_served_complete_read() {
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"t0-s0\",\"concern\":\"complete\",\
             \"outcome\":\"served\",\"complete_through_ms\":1700,\
             \"record_keys\":[\"loadgen-0-1000-0\",\"loadgen-0-1000-1\"]}\n",
        );
        let records = parse_read_log(file.path()).expect("parses");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].concern, ReadConcern::Complete);
        assert_eq!(records[0].partition, PartitionId::new("t0-s0"));
        match &records[0].outcome {
            ReadOutcome::Served {
                complete_through_ms,
                record_keys,
            } => {
                assert_eq!(*complete_through_ms, 1700);
                assert_eq!(record_keys.len(), 2);
            }
            other @ ReadOutcome::Refused { .. } => panic!("expected Served, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_refused_read() {
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"refused\",\"reason\":\"holder unreachable\"}\n",
        );
        let records = parse_read_log(file.path()).expect("parses");
        assert!(matches!(records[0].outcome, ReadOutcome::Refused { .. }));
    }

    #[test]
    fn a_served_read_with_no_watermark_fails_closed() {
        // Would catch a served answer whose claimed coverage is simply
        // absent being ingested as if it had claimed something — the
        // predicate would then have nothing to compare and would silently
        // check one fewer read.
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"served\",\"record_keys\":[]}\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn an_unknown_outcome_fails_closed() {
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"probably_fine\"}\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { .. }));
    }

    #[test]
    fn an_unknown_concern_fails_closed() {
        // A concern this judge does not know is not silently graded as
        // `available` (ignored) NOR as `complete` (graded) — either guess
        // would be a verdict about a read whose contract was not understood.
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"eventual\",\
             \"outcome\":\"refused\",\"reason\":\"x\"}\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { .. }));
    }

    #[test]
    fn a_duplicate_concern_key_fails_closed() {
        // Last-value-wins would silently reclassify a `complete` read as an
        // `available` one, which this judge does not grade at all — the
        // exact class of hazard `crate::journal`'s duplicate-key check
        // exists for.
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"concern\":\"available\",\"outcome\":\"refused\",\"reason\":\"x\"}\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_blank_line_fails_closed_rather_than_being_skipped() {
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"refused\",\"reason\":\"x\"}\n\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { line_no: 2, .. }));
    }

    #[test]
    fn a_missing_read_log_is_an_io_error() {
        let err = parse_read_log(Path::new("/nonexistent/duckspout-judge-read-log.ndjson"))
            .expect_err("must fail");
        assert!(matches!(err, ReadLogError::Io { .. }));
    }
}
