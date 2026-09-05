//! Loadgen run-summary sidecar ingestion (§8.4 vacuity teeth, ACPR finding
//! HIGH-1).
//!
//! `duckspout-loadgen` writes a `{journal_path}.summary.json` sidecar
//! alongside its NDJSON journal at clean exit
//! (`duckspout_loadgen::main`'s `RunSummary`/`write_summary`), specifically
//! because the frozen §3.3 trace vocabulary has no "Sent" event
//! (`duckspout_loadgen::journal`'s own module docs explain why): a loadgen
//! killed with requests still in flight produces a perfectly-parseable,
//! dense-seq journal that simply stops early — and looks, from inside the
//! frozen vocabulary alone, identical to a clean run that just happened to
//! send fewer requests. Confirmed by ACPR: a loadgen that acked 1 request
//! then was SIGKILL'd with 999 in flight produces a 1-line journal this
//! crate's ingestion parses without complaint, and the old judge reported
//! `Pass`.
//!
//! This module closes that gap by reading the sidecar for every journal
//! that is STRUCTURALLY a loadgen journal — carries at least one
//! payload-identity line (`crate::journal`'s own "decode structurally, not
//! by which file it came from" rule, rather than trusting a `--node-id`
//! naming convention like a `loadgen-` prefix) — and turning an
//! incomplete, missing, or unreliable summary into an honest `NoVerdict`
//! (§8.4's own vacuity-teeth language: "an ambiguous-outcome fraction above
//! the profile's ceiling," "a node whose journals simply stop... accuses
//! nothing and certifies nothing") rather than letting the judge report
//! `Pass` over a run it never actually observed the end of.
//!
//! # Known gap
//!
//! A loadgen process killed before journaling even ONE identity-bearing
//! line (e.g. SIGKILL before its first request resolves at all) produces a
//! journal file with no identity lines to structurally recognize as
//! "loadgen-shaped" — this module cannot then tell that file apart from an
//! empty accept-node journal, so a missing summary for it is not flagged.
//! This is a narrower gap than the one this module fixes (it requires the
//! loadgen to have resolved literally nothing before dying) and is left as
//! a known limitation rather than relying on a `--node-id` naming
//! convention that is not actually enforced unique
//! (`duckspout_loadgen::main`'s `Cli::node_id` docs).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::journal::JournalSet;

/// Default ceiling on the fraction of a loadgen's RESOLVED requests that
/// came back `Ambiguous` (`duckspout_loadgen::outcome`'s "the local deadline
/// fired but the connection gave no sign of being dead — genuinely don't
/// know" bucket) before its run summary is treated as unreliable evidence
/// (§8.4's own "an ambiguous-outcome fraction above the profile's ceiling"
/// `NoVerdict` rule).
///
/// Reasoning for `0.05` (5%): a handful of `Ambiguous` outcomes is expected
/// background noise from real network races landing close to
/// `--ack-timeout-ms` under load — not every request that times out
/// locally is proof of anything. But `Ambiguous` means the loadgen
/// genuinely could not confirm what happened (module docs, `outcome`'s own
/// "don't know" framing), so once it is a non-trivial share of the run's
/// resolved outcomes, it stops being noise and starts meaning the
/// ack-timeout is likely mis-tuned for the network/load profile being
/// exercised — at which point a `Pass`, if this predicate would otherwise
/// report one, would be certifying a system this loadgen mostly could not
/// actually observe. `5%` is a conservative starting point, not a
/// theoretically derived bound; it is exposed as `--max-ambiguous-fraction`
/// specifically so an operator tuning a profile's timeout/network
/// parameters can override it with a value they can justify for that
/// profile.
pub const DEFAULT_MAX_AMBIGUOUS_FRACTION: f64 = 0.05;

/// The `{journal_path}.summary.json` shape
/// (`duckspout_loadgen::main::RunSummary`/`StatsSnapshot`), decoded
/// independently here rather than shared via a dependency: this judge does
/// not depend on `duckspout-loadgen` (`crate::journal`'s own module docs
/// make the identical call for the NDJSON journal shape, for the identical
/// reason — a judge parses the wire/file format a producer writes, without
/// linking the producer).
#[derive(Debug, Deserialize)]
struct RunSummary {
    sent: u64,
    acked: u64,
    timed_out: u64,
    rejected: u64,
    ambiguous: u64,
}

impl RunSummary {
    fn resolved_total(&self) -> u64 {
        self.acked + self.timed_out + self.rejected + self.ambiguous
    }
}

/// One reason a loadgen's run summary makes this run's evidence
/// untrustworthy — surfaced to the judge's caller as `NoVerdict`, never
/// silently ignored.
#[derive(Debug, Clone, PartialEq)]
pub enum SummaryFinding {
    /// The sidecar could not be read (missing — the SIGKILL-mid-run case,
    /// module docs — or unreadable) or did not decode as a `RunSummary`.
    Unreadable {
        /// The journal file this summary should have accompanied.
        journal_path: PathBuf,
        /// A human-readable reason.
        reason: String,
    },
    /// `sent > resolved` (`acked + timed_out + rejected + ambiguous`):
    /// batches were sent that this run never recorded any resolution for at
    /// all — the run stopped mid-flight (module docs' core repro).
    UnresolvedInFlight {
        /// The journal file the summary came from.
        journal_path: PathBuf,
        /// Batches sent.
        sent: u64,
        /// Batches resolved one way or another.
        resolved: u64,
    },
    /// The `Ambiguous` share of resolved outcomes exceeded the configured
    /// ceiling (`DEFAULT_MAX_AMBIGUOUS_FRACTION` docs).
    AmbiguousFractionExceeded {
        /// The journal file the summary came from.
        journal_path: PathBuf,
        /// How many resolved outcomes were `Ambiguous`.
        ambiguous: u64,
        /// Total resolved outcomes.
        resolved: u64,
        /// The ceiling that was exceeded.
        ceiling: f64,
    },
}

impl std::fmt::Display for SummaryFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SummaryFinding::Unreadable {
                journal_path,
                reason,
            } => write!(
                f,
                "{}: run summary sidecar unreadable ({reason}) — a loadgen killed mid-run \
                 (SIGKILL) never reaches its own summary-writing code, so a missing/malformed \
                 summary is exactly that vacuity signature, not incidental noise",
                journal_path.display()
            ),
            SummaryFinding::UnresolvedInFlight {
                journal_path,
                sent,
                resolved,
            } => write!(
                f,
                "{}: sent {sent} batch(es) but only {resolved} were ever resolved — this run \
                 stopped with requests genuinely in flight",
                journal_path.display()
            ),
            SummaryFinding::AmbiguousFractionExceeded {
                journal_path,
                ambiguous,
                resolved,
                ceiling,
            } => {
                let fraction = fraction_of(*ambiguous, *resolved);
                write!(
                    f,
                    "{}: {ambiguous}/{resolved} resolved outcomes ({:.1}%) were Ambiguous, \
                     above the {:.1}% ceiling",
                    journal_path.display(),
                    fraction * 100.0,
                    ceiling * 100.0
                )
            }
        }
    }
}

/// `numerator / denominator` as a fraction, `0.0` if `denominator` is 0.
/// `u64 -> f64` loses precision above 2^53, which a loadgen run's request
/// counts (a load-test parameter, not an unbounded external input) are
/// nowhere near — a ratio for a human-readable percentage is the only use.
#[allow(clippy::cast_precision_loss)]
fn fraction_of(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn summary_path_for(journal_path: &Path) -> PathBuf {
    let mut path = journal_path.as_os_str().to_owned();
    path.push(".summary.json");
    PathBuf::from(path)
}

/// Every journal source path that is structurally a loadgen journal —
/// carries at least one payload-identity line (module docs) — and
/// therefore should have a `{path}.summary.json` sidecar written by
/// `duckspout-loadgen` at clean exit.
fn loadgen_journal_sources(journals: &JournalSet) -> BTreeSet<PathBuf> {
    journals
        .lines
        .iter()
        .filter(|line| line.identity.is_some())
        .map(|line| line.source.clone())
        .collect()
}

/// Checks every loadgen journal's run-summary sidecar for the vacuity
/// signatures ACPR finding HIGH-1 named (module docs).
///
/// # Errors
///
/// Returns every [`SummaryFinding`] across every loadgen journal in
/// `journals`, if any — never just the first, so a caller can report the
/// whole picture rather than one arbitrary reason.
pub fn check_summaries(
    journals: &JournalSet,
    max_ambiguous_fraction: f64,
) -> Result<(), Vec<SummaryFinding>> {
    let mut findings = Vec::new();
    for journal_path in loadgen_journal_sources(journals) {
        let summary_path = summary_path_for(&journal_path);
        let text = match std::fs::read_to_string(&summary_path) {
            Ok(text) => text,
            Err(err) => {
                findings.push(SummaryFinding::Unreadable {
                    journal_path,
                    reason: format!("reading {}: {err}", summary_path.display()),
                });
                continue;
            }
        };
        let summary: RunSummary = match serde_json::from_str(&text) {
            Ok(summary) => summary,
            Err(err) => {
                findings.push(SummaryFinding::Unreadable {
                    journal_path,
                    reason: format!("decoding {}: {err}", summary_path.display()),
                });
                continue;
            }
        };

        let resolved = summary.resolved_total();
        if summary.sent > resolved {
            findings.push(SummaryFinding::UnresolvedInFlight {
                journal_path: journal_path.clone(),
                sent: summary.sent,
                resolved,
            });
        }
        if resolved > 0 {
            let fraction = fraction_of(summary.ambiguous, resolved);
            if fraction > max_ambiguous_fraction {
                findings.push(SummaryFinding::AmbiguousFractionExceeded {
                    journal_path,
                    ambiguous: summary.ambiguous,
                    resolved,
                    ceiling: max_ambiguous_fraction,
                });
            }
        }
    }
    if findings.is_empty() {
        Ok(())
    } else {
        Err(findings)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use duckspout_types::{NodeId, TraceEvent};

    use super::*;
    use crate::journal::{JournalLine, RequestIdentity};

    fn loadgen_journal_line(source: &Path) -> JournalLine {
        JournalLine {
            source: source.to_owned(),
            line_no: 1,
            node: NodeId::new("loadgen-0"),
            seq: 0,
            event: TraceEvent::ClientAck,
            identity: Some(RequestIdentity {
                request_id: "req-1".to_owned(),
                tenant: "tenant-a".to_owned(),
                record_count: 1,
                first_index: 0,
                source_incarnation: "loadgen-0-1000".to_owned(),
                partition: None,
                max_event_time_ms: None,
            }),
            watermark: None,
            changelog: None,
            part: None,
        }
    }

    fn write_summary(journal_path: &Path, json: &str) {
        let mut summary_path = journal_path.as_os_str().to_owned();
        summary_path.push(".summary.json");
        let mut file = std::fs::File::create(&summary_path).expect("create summary");
        file.write_all(json.as_bytes()).expect("write summary");
    }

    #[test]
    fn a_journal_with_no_identity_lines_needs_no_summary() {
        // A plain node journal (no ClientAck identity lines) is not
        // structurally a loadgen journal — no sidecar is expected for it.
        let journals = JournalSet {
            lines: vec![JournalLine {
                source: PathBuf::from("/nonexistent/node.ndjson"),
                line_no: 1,
                node: NodeId::new("n1"),
                seq: 0,
                event: TraceEvent::Accept,
                identity: None,
                watermark: None,
                changelog: None,
                part: None,
            }],
        };
        assert_eq!(
            check_summaries(&journals, DEFAULT_MAX_AMBIGUOUS_FRACTION),
            Ok(())
        );
    }

    #[test]
    fn a_missing_summary_for_a_loadgen_journal_is_a_finding() {
        // THE ACPR HIGH-1 repro: a loadgen SIGKILL'd mid-run never reaches
        // its own summary-writing code, so the sidecar is simply absent —
        // even though the journal itself parses perfectly (1 acked
        // request, 999 never resolved at all, no trace of them here).
        let journal_file = tempfile::NamedTempFile::new().expect("tempfile");
        let journals = JournalSet {
            lines: vec![loadgen_journal_line(journal_file.path())],
        };
        let findings = check_summaries(&journals, DEFAULT_MAX_AMBIGUOUS_FRACTION)
            .expect_err("missing summary must be a finding, not a silent Pass");
        assert_eq!(findings.len(), 1);
        assert!(matches!(findings[0], SummaryFinding::Unreadable { .. }));
    }

    #[test]
    fn a_complete_clean_summary_has_no_findings() {
        let journal_file = tempfile::NamedTempFile::new().expect("tempfile");
        write_summary(
            journal_file.path(),
            r#"{"node":"loadgen-0","sent":10,"acked":10,"timed_out":0,"rejected":0,"ambiguous":0}"#,
        );
        let journals = JournalSet {
            lines: vec![loadgen_journal_line(journal_file.path())],
        };
        assert_eq!(
            check_summaries(&journals, DEFAULT_MAX_AMBIGUOUS_FRACTION),
            Ok(())
        );
    }

    #[test]
    fn sent_greater_than_resolved_is_a_finding() {
        // A run summary that IS written (a clean shutdown path, or a
        // controlled kill that still flushed it) but honestly records
        // unresolved in-flight batches.
        let journal_file = tempfile::NamedTempFile::new().expect("tempfile");
        write_summary(
            journal_file.path(),
            r#"{"node":"loadgen-0","sent":1000,"acked":1,"timed_out":0,"rejected":0,"ambiguous":0}"#,
        );
        let journals = JournalSet {
            lines: vec![loadgen_journal_line(journal_file.path())],
        };
        let findings = check_summaries(&journals, DEFAULT_MAX_AMBIGUOUS_FRACTION)
            .expect_err("sent > resolved must be a finding");
        assert_eq!(findings.len(), 1);
        assert!(matches!(
            findings[0],
            SummaryFinding::UnresolvedInFlight {
                sent: 1000,
                resolved: 1,
                ..
            }
        ));
    }

    #[test]
    fn an_ambiguous_fraction_above_the_ceiling_is_a_finding() {
        let journal_file = tempfile::NamedTempFile::new().expect("tempfile");
        write_summary(
            journal_file.path(),
            r#"{"node":"loadgen-0","sent":10,"acked":4,"timed_out":0,"rejected":0,"ambiguous":6}"#,
        );
        let journals = JournalSet {
            lines: vec![loadgen_journal_line(journal_file.path())],
        };
        let findings = check_summaries(&journals, DEFAULT_MAX_AMBIGUOUS_FRACTION)
            .expect_err("60% ambiguous must exceed the 5% default ceiling");
        assert!(
            findings
                .iter()
                .any(|f| matches!(f, SummaryFinding::AmbiguousFractionExceeded { .. }))
        );
    }

    #[test]
    fn an_ambiguous_fraction_within_a_relaxed_ceiling_is_not_a_finding() {
        let journal_file = tempfile::NamedTempFile::new().expect("tempfile");
        write_summary(
            journal_file.path(),
            r#"{"node":"loadgen-0","sent":10,"acked":9,"timed_out":0,"rejected":0,"ambiguous":1}"#,
        );
        let journals = JournalSet {
            lines: vec![loadgen_journal_line(journal_file.path())],
        };
        // 10% ambiguous, but with an operator-supplied 20% ceiling.
        assert_eq!(check_summaries(&journals, 0.20), Ok(()));
    }

    #[test]
    fn a_malformed_summary_is_a_finding_not_a_panic() {
        let journal_file = tempfile::NamedTempFile::new().expect("tempfile");
        write_summary(journal_file.path(), "not json");
        let journals = JournalSet {
            lines: vec![loadgen_journal_line(journal_file.path())],
        };
        let findings = check_summaries(&journals, DEFAULT_MAX_AMBIGUOUS_FRACTION)
            .expect_err("malformed summary must be a finding");
        assert!(matches!(findings[0], SummaryFinding::Unreadable { .. }));
    }
}
