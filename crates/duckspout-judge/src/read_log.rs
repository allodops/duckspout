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
//! **A [`ReadRecord`] carries no ordering handle, and that costs a real
//! guarantee (ACPR finding MEDIUM-3).** There is no seq, no position, and no
//! other field placing a read relative to the journals' per-node dense
//! sequences (D-6) — a deliberate consequence of this being a sidecar file
//! rather than a journal (see above: §3 has no read action to ride). §8.4
//! asks that no `complete` read be served over coverage that did not exist
//! **at serving time**, and "serving time" is exactly what this shape cannot
//! express: a read served BEFORE the commit that would justify it is
//! indistinguishable, in this evidence, from one served after. So
//! `crate::predicates::watermark_honesty` compares every read against the
//! RUN-WIDE maximum coverage for its partition — the strongest rule this
//! shape supports, and strictly weaker than §8.4's sentence. Closing that
//! gap is a change to this wire format (the query client stamping an
//! ordering handle the journals can be aligned against), not a change to the
//! predicate; until then the weakening is disclosed rather than papered
//! over. Note the asymmetry: ADVERTISEMENTS do ride journal lines and so do
//! carry D-6's ordering, which that predicate exploits for same-node claims.
//!
//! # The cache probe (#207)
//!
//! §8.4's eviction-storm judge needs one thing this shape did not carry:
//! **which cache state served this answer.** Without it, "any two cache
//! states yield the identical row set" (§2.4) is not a statement about
//! anything a judge can see. [`CacheProbe`] is that observation, decoded by
//! FIELD PRESENCE off the same flat line (`crate::journal`'s rule, applied
//! here), and it is deliberately an OUTSIDE-THE-NODE measurement: the
//! residency-op counter is the number of `Demote`/`Evict`/`DropWindow` lines
//! in the serving node's own D-6 journal at the moment the read was issued
//! and again when it finished. A node self-reporting "my cache did not
//! affect your answer" would be the subject grading itself; counting the
//! §3 actions it journaled is the fleet runner reading the same public
//! evidence the judge does.
//!
//! Two consequences worth stating plainly:
//!
//! - The counter is **monotone per node**, so probed reads against one node
//!   ARE mutually ordered — which is the ordering handle this module's
//!   MEDIUM-3 note above says a bare [`ReadRecord`] lacks. That order is
//!   still not comparable against the journals' per-node seqs, so the
//!   MEDIUM-3 weakening stands for watermark honesty; it is only within the
//!   probed reads themselves that `crate::predicates::cache_transparency`
//!   can say "before" and "after".
//! - A read whose `residency_ops_after` exceeds its `residency_ops_before`
//!   genuinely **raced** a residency action, which is what makes obligation
//!   (c) — "Evict takes no locks the read path depends on" (§2.4) —
//!   checkable at all.
//!
//! # Producer status
//!
//! Nothing in this workspace writes this file yet: the fleet's query client
//! lands with the distributed tier's own wiring (#208), exactly as
//! `crate::final_state`'s read-back does. A judge run given no read log
//! reports `NoVerdict` for the served half of watermark honesty, and for all
//! of cache transparency, rather than a vacuous pass (§8.4's vacuity teeth).
//!
//! `duckspout-fleet`'s `--fault-cache-churn-node` injector (#207) forces
//! real, confirmed residency churn against a real node while driving real
//! Arrow Flight reads through it. It does NOT write this file: the daemon's
//! read surface is hot-only with no read concern and no coverage pinning
//! (`duckspout_daemon::serving`'s own #113 gap note), so no `complete` read
//! exists for it to log, and stamping a `complete_through_ms` sampled from
//! the advisory `/status` row would be exactly the lagging-watermark
//! disclosure hazard `crate::journal::RequestIdentity::max_event_time_ms`
//! already warns makes a judge convict a correct fleet. The injector
//! therefore journals its racing reads' real observed outcomes into the
//! fleet's own `faults.ndjson` window, and this format waits for the real
//! query client (#208, and the Airport read vocabulary behind it).
//!
//! # Wire shape
//!
//! ```json
//! {"tenant":"t","partition":"t0-s0","concern":"complete","outcome":"served",
//!  "complete_through_ms":1700,"record_keys":["loadgen-0-1000-0"]}
//! {"tenant":"t","partition":"t0-s0","concern":"complete","outcome":"refused",
//!  "reason":"holder unreachable"}
//! {"tenant":"t","partition":"t0-s0","concern":"complete","outcome":"served",
//!  "complete_through_ms":1700,"record_keys":["loadgen-0-1000-0"],
//!  "query":"SELECT ...","serving_node":"fleet-0-1/1",
//!  "residency_ops_before":12,"residency_ops_after":13,"latency_ms":4}
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use duckspout_types::{NodeId, PartitionId};
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

/// What cache state served one read, observed from outside the serving node
/// (module docs' "the cache probe").
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CacheProbe {
    /// The exact question this read asked. Two answers are only required to
    /// agree when they are answers to the SAME question: without this,
    /// `crate::predicates::cache_transparency` would compare a `SELECT *`
    /// against a `SELECT count(*)` and convict a correct fleet.
    pub query: String,
    /// The node that served the answer, spelled as that node's own D-6
    /// journal spells it (`node/incarnation`) — the residency counters below
    /// are counts within THAT node's journal, so a probe naming a different
    /// node's counter would be comparing two unrelated clocks.
    pub serving_node: NodeId,
    /// `Demote` + `Evict` + `DropWindow` lines in `serving_node`'s journal
    /// when the read was ISSUED — the cache-state label (module docs).
    pub residency_ops_before: u64,
    /// The same count when the answer was complete. Strictly greater than
    /// `residency_ops_before` exactly when the read RACED a residency
    /// action, which is obligation (c)'s subject.
    pub residency_ops_after: u64,
    /// End-to-end latency of the read, milliseconds — the observable a held
    /// lock would show up in (§2.4 obligation (c)).
    pub latency_ms: u64,
}

impl CacheProbe {
    /// Whether this read overlapped at least one residency action.
    #[must_use]
    pub fn raced_residency_action(&self) -> bool {
        self.residency_ops_after > self.residency_ops_before
    }
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
    /// The cache state that served this read, when the client sampled one
    /// (module docs). Absent on every line written before #207 and on every
    /// read whose client did not probe — which is "no cache evidence," and
    /// `crate::predicates::cache_transparency` treats it as such rather than
    /// guessing a state.
    ///
    /// Not `#[serde(flatten)]`: [`ReadOutcome`] already owns this struct's
    /// one flatten slot, and a second flattened field would have to be a
    /// catch-all that swallowed the outcome's own tag. `parse_read_log`
    /// decodes it by field presence off the line's JSON instead — the same
    /// mechanism, and the exact one `crate::journal` uses for its four
    /// payload shapes.
    #[serde(skip)]
    pub cache: Option<CacheProbe>,
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
        let mut record: ReadRecord = serde_json::from_str(raw).map_err(decode_error)?;
        let line: serde_json::Value = serde_json::from_str(raw).map_err(decode_error)?;
        record.cache = probe_from_line(&line).map_err(decode_error)?;
        records.push(record);
    }
    Ok(records)
}

/// Decodes a [`CacheProbe`] off one read-log line when it carries a
/// `residency_ops_before` field, under exactly the rules
/// `crate::journal::identity_from_rest` applies to its own key field:
/// presence decides that a probe is being made, and a probe that does not
/// fully decode is corruption, not "no probe here" — a half-formed probe
/// silently read as "unprobed" would drop the one read that actually raced
/// an eviction out of obligation (c)'s evidence.
///
/// Also rejects a probe whose `residency_ops_after` is BELOW its
/// `residency_ops_before`: the counter is a count of journaled lines and is
/// monotone by construction, so a decreasing pair means the two samples came
/// from different journals (or a truncated one) and the read cannot be
/// placed in any cache state at all.
fn probe_from_line(line: &serde_json::Value) -> Result<Option<CacheProbe>, serde_json::Error> {
    match line {
        serde_json::Value::Object(map) if map.contains_key("residency_ops_before") => {
            let probe: CacheProbe = serde_json::from_value(line.clone())?;
            if probe.residency_ops_after < probe.residency_ops_before {
                return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "cache probe on node {} went BACKWARDS ({} → {}) — the residency-op counter \
                     is a monotone line count, so this pair cannot label one cache state and \
                     fails closed",
                    probe.serving_node, probe.residency_ops_before, probe.residency_ops_after
                )));
            }
            Ok(Some(probe))
        }
        _ => Ok(None),
    }
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
    fn a_probed_read_carries_its_cache_state_and_its_race() {
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"served\",\"complete_through_ms\":1700,\"record_keys\":[],\
             \"query\":\"SELECT 1\",\"serving_node\":\"fleet-0-1/1\",\
             \"residency_ops_before\":12,\"residency_ops_after\":13,\"latency_ms\":4}\n",
        );
        let records = parse_read_log(file.path()).expect("parses");
        let probe = records[0].cache.as_ref().expect("probe present");
        assert_eq!(probe.serving_node.as_str(), "fleet-0-1/1");
        assert_eq!(probe.latency_ms, 4);
        assert!(probe.raced_residency_action());
    }

    #[test]
    fn an_unprobed_read_still_parses_and_carries_no_cache_state() {
        // Non-regression for every read-log line written before #207: the
        // probe fields are absent, which is "no cache evidence", never a
        // decode failure.
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"refused\",\"reason\":\"x\"}\n",
        );
        let records = parse_read_log(file.path()).expect("parses");
        assert!(records[0].cache.is_none());
    }

    #[test]
    fn a_read_that_did_not_race_a_residency_action_says_so() {
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"served\",\"complete_through_ms\":1,\"record_keys\":[],\
             \"query\":\"SELECT 1\",\"serving_node\":\"n/1\",\
             \"residency_ops_before\":7,\"residency_ops_after\":7,\"latency_ms\":1}\n",
        );
        let records = parse_read_log(file.path()).expect("parses");
        assert!(
            !records[0]
                .cache
                .as_ref()
                .expect("probe")
                .raced_residency_action()
        );
    }

    #[test]
    fn a_half_formed_cache_probe_fails_closed() {
        // Would catch a probe that names a cache state but no query being
        // read as "unprobed" — silently dropping exactly the read that
        // raced an eviction out of obligation (c)'s evidence.
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"refused\",\"reason\":\"x\",\
             \"residency_ops_before\":1,\"residency_ops_after\":2}\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_backwards_cache_probe_fails_closed() {
        // The counter is a monotone line count: a decreasing pair means the
        // two samples were not taken from one journal, so the read belongs
        // to no single cache state and must not be graded as if it did.
        let file = write_log(
            "{\"tenant\":\"t\",\"partition\":\"p\",\"concern\":\"complete\",\
             \"outcome\":\"served\",\"complete_through_ms\":1,\"record_keys\":[],\
             \"query\":\"SELECT 1\",\"serving_node\":\"n/1\",\
             \"residency_ops_before\":9,\"residency_ops_after\":3,\"latency_ms\":1}\n",
        );
        let err = parse_read_log(file.path()).expect_err("must fail closed");
        assert!(matches!(err, ReadLogError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_missing_read_log_is_an_io_error() {
        let err = parse_read_log(Path::new("/nonexistent/duckspout-judge-read-log.ndjson"))
            .expect_err("must fail");
        assert!(matches!(err, ReadLogError::Io { .. }));
    }
}
