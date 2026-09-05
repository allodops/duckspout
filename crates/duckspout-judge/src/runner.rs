//! The judge binary's actual run logic (§8.4), split out of `main.rs` so it
//! is directly testable — ACPR finding MEDIUM-HIGH-4: every existing test
//! before this fix built a `JournalSet` by hand in memory; nothing drove
//! real NDJSON text through `parse_journal_file`/`ingest_journals` into the
//! predicate and out through the judge's own exit-code mapping. This module
//! is that real, callable pipeline: `main.rs` becomes a thin wrapper that
//! parses `clap` args into [`RunArgs`], calls [`run`], prints
//! [`RunOutcome`], and exits with [`RunOutcome::exit_code`] — every step a
//! test in `tests/` can also drive directly, without a subprocess.
//!
//! # Every predicate runs, every time (#206, #207)
//!
//! With all five of `docs/verification.md` §8.4's judges in the crate,
//! "which ones did this run judge?" is a question the exit code must not
//! hide. There is deliberately no `--predicate` selection flag: the judge
//! runs all of them and reports one [`PredicateReport`] each, and the run's
//! exit code is `crate::verdict::combined_exit_code` over the lot. A
//! predicate given no evidence reports `NoVerdict`, so a run that fed only
//! loadgen journals and a final-state fixture exits `3` even when
//! zero-acked-lost passes — honestly, because such a run certifies nothing
//! about watermark honesty, the latest view, retention, or cache
//! transparency. Anything else would be "skipped ≠ passed" (§8.4) smuggled
//! in through a command-line knob.

use std::path::PathBuf;

use crate::final_state::{InMemoryCommittedParts, InMemoryFinalState, InMemoryLatestView};
use crate::journal::{JournalSet, ingest_journals};
use crate::predicates::{
    cache_transparency, latest_view, retention_honesty, watermark_honesty, zero_acked_lost,
};
use crate::read_log::{ReadRecord, parse_read_log};
use crate::summary::{self, SummaryFinding};
use crate::verdict::{Verdict, combined_exit_code};

/// The judge run's inputs — `main.rs`'s `Cli`, stripped of `clap` so it can
/// be constructed directly by a test.
#[derive(Debug, Clone)]
pub struct RunArgs {
    /// Paths of the per-node (plus loadgen) NDJSON journals to grade.
    pub journals: Vec<PathBuf>,
    /// DEV/TEST ONLY: a `FinalSystemState` fixture JSON
    /// (`crate::final_state` module docs). `None` leaves zero-acked-lost
    /// with nothing to check against, which it reports as `NoVerdict`
    /// rather than fabricating a verdict.
    pub final_state_fixture: Option<PathBuf>,
    /// The query client's served-read log (`crate::read_log`). `None`
    /// leaves watermark honesty with no served half — its journal-only
    /// rules still run.
    pub read_log: Option<PathBuf>,
    /// DEV/TEST ONLY: a `LatestView` fixture JSON (`crate::final_state`),
    /// standing in for a `<dataset>_latest` read-back.
    pub latest_view_fixture: Option<PathBuf>,
    /// DEV/TEST ONLY: a `CommittedParts` fixture JSON
    /// (`crate::final_state`), standing in for the lake's own read-back of
    /// which snapshot parts it holds. `None` leaves retention honesty with
    /// no covering set to check the journaled expiries against.
    pub committed_parts_fixture: Option<PathBuf>,
    /// Ceiling on the loadgen run summary's ambiguous-outcome fraction
    /// (`crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION` docs).
    pub max_ambiguous_fraction: f64,
    /// Absolute latency ceiling for a read that raced a residency action
    /// (`crate::predicates::cache_transparency::DEFAULT_MAX_RACING_READ_MS`
    /// docs).
    pub max_racing_read_ms: u64,
}

/// One predicate's verdict, named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateReport {
    /// The predicate's stable name, as printed and as cited in
    /// `docs/verification.md` §8.4.
    pub predicate: &'static str,
    /// Its verdict, with findings already rendered (`Verdict::erase`).
    pub verdict: Verdict<String>,
}

/// The judge's own outcome — one-to-one with `main.rs`'s `EXIT_CONTRACT`
/// (0/2/3 via [`RunOutcome::exit_code`]).
///
/// Not `Eq`: [`SummaryFinding`] carries the ambiguous-fraction ceiling as an
/// `f64`.
#[derive(Debug, Clone, PartialEq)]
pub enum RunOutcome {
    /// Journal (or read-log) ingestion itself failed (malformed input;
    /// fails closed) — no predicate ran, because none of them can be
    /// trusted over evidence that did not parse.
    IngestionFailed(String),
    /// The loadgen run-summary vacuity check (ACPR finding HIGH-1) found a
    /// reason not to trust this run's evidence.
    SummaryVacuous(Vec<SummaryFinding>),
    /// Every predicate ran; here is what each concluded.
    Judged {
        /// How many journal files were ingested.
        journal_count: usize,
        /// How many total lines were ingested across every file.
        line_count: usize,
        /// One report per predicate, in a stable order.
        reports: Vec<PredicateReport>,
    },
}

impl RunOutcome {
    /// Maps this outcome to the judge's exit contract: `0` = Pass,
    /// `2` = Violation, `3` = `NoVerdict` (every other case — inconclusive
    /// or vacuous, never a pass). A judged run combines its predicates'
    /// verdicts through `crate::verdict::combined_exit_code`.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            RunOutcome::IngestionFailed(_) | RunOutcome::SummaryVacuous(_) => {
                crate::verdict::EXIT_NO_VERDICT
            }
            RunOutcome::Judged { reports, .. } => {
                let verdicts: Vec<Verdict<String>> =
                    reports.iter().map(|r| r.verdict.clone()).collect();
                combined_exit_code(&verdicts)
            }
        }
    }
}

/// Runs the real judge pipeline: ingest → summary vacuity check → every
/// predicate (module docs).
#[must_use]
pub fn run(args: &RunArgs) -> RunOutcome {
    let journals = match ingest_journals(&args.journals) {
        Ok(journals) => journals,
        Err(err) => return RunOutcome::IngestionFailed(err.to_string()),
    };

    if let Err(findings) = summary::check_summaries(&journals, args.max_ambiguous_fraction) {
        return RunOutcome::SummaryVacuous(findings);
    }

    let reads = match &args.read_log {
        Some(path) => match parse_read_log(path) {
            Ok(reads) => reads,
            Err(err) => return RunOutcome::IngestionFailed(err.to_string()),
        },
        None => Vec::new(),
    };

    RunOutcome::Judged {
        journal_count: args.journals.len(),
        line_count: journals.lines.len(),
        reports: vec![
            zero_acked_lost_report(args, &journals),
            watermark_honesty_report(&journals, &reads),
            latest_view_report(args, &journals),
            retention_honesty_report(args, &journals),
            cache_transparency_report(args, &journals, &reads),
        ],
    }
}

/// Reads a fixture file into `T`, or the reason it could not be used —
/// shared by the two fixture-backed predicates so "unreadable" and
/// "undecodable" produce the same honest `NoVerdict` wording in both.
fn load_fixture<T>(
    path: Option<&PathBuf>,
    absent: &str,
    decode: impl FnOnce(&str) -> Result<T, serde_json::Error>,
) -> Result<T, String> {
    let Some(path) = path else {
        return Err(absent.to_owned());
    };
    let text = std::fs::read_to_string(path)
        .map_err(|err| format!("reading {}: {err}", path.display()))?;
    decode(&text).map_err(|err| format!("{}: {err}", path.display()))
}

fn zero_acked_lost_report(args: &RunArgs, journals: &JournalSet) -> PredicateReport {
    let verdict = match load_fixture(
        args.final_state_fixture.as_ref(),
        "no --final-state-fixture was given, and no real hot/lake read-back is wired yet \
         (crate::final_state's scope note) — this predicate had nothing to check the acks \
         against",
        InMemoryFinalState::from_fixture_json,
    ) {
        Ok(final_state) => zero_acked_lost::check(journals, &final_state).erase(),
        Err(reason) => Verdict::NoVerdict(reason),
    };
    PredicateReport {
        predicate: "zero-acked-lost",
        verdict,
    }
}

fn watermark_honesty_report(journals: &JournalSet, reads: &[ReadRecord]) -> PredicateReport {
    // No fixture to load, and no special case for a missing `--read-log`:
    // the predicate itself reports `NoVerdict` when nothing was served,
    // whether that is because no read log was given or because the one
    // given contained no served `complete` read. Both mean the same thing —
    // the query-side half of §8.4's sentence went unchecked — so they get
    // the same verdict from the same code path.
    PredicateReport {
        predicate: "watermark-honesty",
        verdict: watermark_honesty::check(journals, reads).erase(),
    }
}

fn latest_view_report(args: &RunArgs, journals: &JournalSet) -> PredicateReport {
    let verdict = match load_fixture(
        args.latest_view_fixture.as_ref(),
        "no --latest-view-fixture was given, and no real `<dataset>_latest` read-back is wired \
         yet (crate::final_state's scope note) — this predicate had no served view to compare \
         the acked changelog's fold against",
        InMemoryLatestView::from_fixture_json,
    ) {
        Ok(view) => latest_view::check(journals, &view).erase(),
        Err(reason) => Verdict::NoVerdict(reason),
    };
    PredicateReport {
        predicate: "latest-view",
        verdict,
    }
}

/// Retention honesty needs BOTH read-backs — the lake's snapshot set for
/// obligation (A) and the served latest view for obligation (B) — so a
/// missing either one is that predicate's own `NoVerdict`, named precisely
/// enough that an operator knows which fixture to supply.
fn retention_honesty_report(args: &RunArgs, journals: &JournalSet) -> PredicateReport {
    let parts = load_fixture(
        args.committed_parts_fixture.as_ref(),
        "no --committed-parts-fixture was given, and no real lake read-back is wired yet \
         (crate::final_state's scope note) — this predicate had no committed snapshot set to \
         check the journaled expiries against",
        InMemoryCommittedParts::from_fixture_json,
    );
    let view = load_fixture(
        args.latest_view_fixture.as_ref(),
        "no --latest-view-fixture was given, and no real `<dataset>_latest` read-back is wired \
         yet (crate::final_state's scope note) — this predicate could not tell whether an \
         expiry made an acked record's last value unreachable",
        InMemoryLatestView::from_fixture_json,
    );
    let verdict = match (parts, view) {
        (Ok(parts), Ok(view)) => retention_honesty::check(journals, &parts, &view).erase(),
        (Err(reason), _) | (Ok(_), Err(reason)) => Verdict::NoVerdict(reason),
    };
    PredicateReport {
        predicate: "retention-honesty",
        verdict,
    }
}

/// Cache transparency reads no fixture: its evidence is the read log's cache
/// probes and the journals' own residency actions, both of which the
/// predicate itself downgrades to `NoVerdict` when absent — the same call
/// `watermark_honesty_report` makes, for the same reason (one code path, one
/// verdict, whether the evidence was never given or was given empty).
fn cache_transparency_report(
    args: &RunArgs,
    journals: &JournalSet,
    reads: &[ReadRecord],
) -> PredicateReport {
    PredicateReport {
        predicate: "cache-transparency",
        verdict: cache_transparency::check(journals, reads, args.max_racing_read_ms).erase(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn write_temp(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(text.as_bytes()).expect("write");
        file
    }

    fn args(journals: Vec<PathBuf>) -> RunArgs {
        RunArgs {
            journals,
            final_state_fixture: None,
            read_log: None,
            latest_view_fixture: None,
            committed_parts_fixture: None,
            max_ambiguous_fraction: summary::DEFAULT_MAX_AMBIGUOUS_FRACTION,
            max_racing_read_ms: crate::predicates::cache_transparency::DEFAULT_MAX_RACING_READ_MS,
        }
    }

    fn verdict_of<'a>(outcome: &'a RunOutcome, predicate: &str) -> &'a Verdict<String> {
        match outcome {
            RunOutcome::Judged { reports, .. } => {
                &reports
                    .iter()
                    .find(|report| report.predicate == predicate)
                    .expect("every predicate is reported")
                    .verdict
            }
            other => panic!("expected a judged run, got {other:?}"),
        }
    }

    #[test]
    fn malformed_journal_is_ingestion_failed_exit_3() {
        let bad = write_temp("not json\n");
        let outcome = run(&args(vec![bad.path().to_owned()]));
        assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
        assert_eq!(outcome.exit_code(), 3);
    }

    #[test]
    fn a_malformed_read_log_fails_the_run_closed() {
        // The read log is evidence like any other: a line that does not
        // parse must stop the run, not be silently dropped from the served
        // half of watermark honesty.
        let journal = write_temp("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let bad_log = write_temp("{\"tenant\":\"t\"}\n");
        let mut args = args(vec![journal.path().to_owned()]);
        args.read_log = Some(bad_log.path().to_owned());
        let outcome = run(&args);
        assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
        assert_eq!(outcome.exit_code(), 3);
    }

    #[test]
    fn every_predicate_is_reported_even_with_no_evidence_for_any_of_them() {
        let journal = write_temp("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let outcome = run(&args(vec![journal.path().to_owned()]));
        match &outcome {
            RunOutcome::Judged {
                reports,
                journal_count,
                line_count,
            } => {
                assert_eq!(*journal_count, 1);
                assert_eq!(*line_count, 1);
                assert_eq!(reports.len(), 5);
                assert!(
                    reports
                        .iter()
                        .all(|r| matches!(r.verdict, Verdict::NoVerdict(_)))
                );
            }
            other => panic!("expected a judged run, got {other:?}"),
        }
        assert_eq!(outcome.exit_code(), 3);
    }

    #[test]
    fn a_missing_fixture_is_that_predicates_no_verdict_not_the_whole_runs_error() {
        // Would catch a pipeline that aborted the entire run because ONE
        // predicate's read-back was absent — the other predicates' evidence
        // is still worth judging, and their verdicts are still reported.
        let journal = write_temp(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"LakeCommitOk\",\
             \"partition\":\"p\",\"complete_through_ms\":10}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"ClaimAdvertise\",\
             \"partition\":\"p\",\"complete_through_ms\":99}\n",
        );
        let outcome = run(&args(vec![journal.path().to_owned()]));
        assert!(matches!(
            verdict_of(&outcome, "watermark-honesty"),
            Verdict::Violation(_)
        ));
        assert!(matches!(
            verdict_of(&outcome, "zero-acked-lost"),
            Verdict::NoVerdict(_)
        ));
        assert_eq!(outcome.exit_code(), 2);
    }

    #[test]
    fn an_unreadable_fixture_is_a_no_verdict_naming_the_file() {
        let journal = write_temp("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let mut args = args(vec![journal.path().to_owned()]);
        args.final_state_fixture = Some(PathBuf::from("/nonexistent/final-state.json"));
        let outcome = run(&args);
        match verdict_of(&outcome, "zero-acked-lost") {
            Verdict::NoVerdict(reason) => {
                assert!(reason.contains("final-state.json"), "reason: {reason}");
            }
            other => panic!("expected NoVerdict, got {other:?}"),
        }
    }
}
