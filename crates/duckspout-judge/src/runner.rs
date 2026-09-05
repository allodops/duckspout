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
//!
//! # …and so does every run-level vacuity rule (#208)
//!
//! `crate::vacuity`'s four §8.4 run-level rules are reported the same way,
//! in the same list, under `vacuity/…` names, and combined by the same
//! `combined_exit_code` call. There is exactly one composition rule for the
//! whole run (that module's docs state it), and no rule anywhere gets to
//! short-circuit the others:
//!
//! **The one behaviour change #208 made here** is that the loadgen
//! run-summary check no longer aborts the run before any predicate has
//! spoken. It used to return a dedicated `SummaryVacuous` outcome, which
//! meant a run with — say — an ambiguous-outcome fraction over the ceiling
//! reported `3` even when the journals contained a *proven* lost ack. That
//! contradicted `combined_exit_code`'s own documented ordering ("a proven
//! violation anywhere outranks an inconclusive predicate elsewhere") for no
//! reason: an acked record missing from the final system is a fact about the
//! code, and an unrelated vacuity signal does not unprove it. The check now
//! runs as `vacuity/loadgen-outcome-quality` alongside everything else, so a
//! vacuous-and-clean run still exits `3` (unchanged) while a
//! vacuous-and-convicted run exits `2` (the fact the judge actually holds).
//! Ingestion failure remains the one true short-circuit: evidence that did
//! not parse cannot be judged at all.

use std::path::PathBuf;

use crate::fault_ledger::{FaultLedger, parse_fault_ledger};
use crate::final_state::{InMemoryCommittedParts, InMemoryFinalState, InMemoryLatestView};
use crate::journal::{JournalSet, ingest_journals};
use crate::predicates::{
    cache_transparency, latest_view, retention_honesty, watermark_honesty, zero_acked_lost,
};
use crate::read_log::{ReadRecord, parse_read_log};
use crate::run_manifest::{RunManifest, parse_run_manifest};
use crate::vacuity::{self, VacuityInputs};
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
    /// The fleet run's fault-injector ledger (`faults.ndjson`,
    /// `crate::fault_ledger`). `None` leaves §8.4's armed-but-unfired rule
    /// with nothing measured — reported as `NoVerdict`, never assumed from
    /// the run's profile.
    pub fault_log: Option<PathBuf>,
    /// The fleet run's manifest (`run.json`, `crate::run_manifest`). `None`
    /// leaves the cross-node-contention and node-continuity rules with no
    /// roster and no clock — both reported as `NoVerdict`.
    pub run_manifest: Option<PathBuf>,
    /// Ceiling on the loadgen run summary's ambiguous-outcome fraction
    /// (`crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION` docs).
    pub max_ambiguous_fraction: f64,
    /// How far a roster node's journal may fall behind the fleet's own last
    /// activity (`crate::vacuity::DEFAULT_MAX_JOURNAL_SILENCE_MS` docs).
    pub max_journal_silence_ms: u64,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    /// Evidence ingestion itself failed — a malformed journal, read log,
    /// fault ledger or run manifest (fails closed). No predicate and no
    /// vacuity rule ran, because none of them can be trusted over evidence
    /// that did not parse. This is the pipeline's ONLY short-circuit
    /// (module docs).
    IngestionFailed(String),
    /// Every predicate and every run-level vacuity rule ran; here is what
    /// each concluded.
    Judged {
        /// How many journal files were ingested.
        journal_count: usize,
        /// How many total lines were ingested across every file.
        line_count: usize,
        /// One report per predicate and per run-level vacuity rule, in a
        /// stable order (predicates first, then `vacuity/…`).
        reports: Vec<PredicateReport>,
    },
}

impl RunOutcome {
    /// Maps this outcome to the judge's exit contract: `0` = Pass,
    /// `2` = Violation, `3` = `NoVerdict` (every other case — inconclusive
    /// or vacuous, never a pass). A judged run combines every report's
    /// verdict — predicates and `vacuity/…` rules alike — through the single
    /// `crate::verdict::combined_exit_code`.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            RunOutcome::IngestionFailed(_) => crate::verdict::EXIT_NO_VERDICT,
            RunOutcome::Judged { reports, .. } => {
                let verdicts: Vec<Verdict<String>> =
                    reports.iter().map(|r| r.verdict.clone()).collect();
                combined_exit_code(&verdicts)
            }
        }
    }
}

/// Runs the real judge pipeline: ingest → every predicate → every run-level
/// vacuity rule (module docs).
#[must_use]
pub fn run(args: &RunArgs) -> RunOutcome {
    let journals = match ingest_journals(&args.journals) {
        Ok(journals) => journals,
        Err(err) => return RunOutcome::IngestionFailed(err.to_string()),
    };

    let reads = match &args.read_log {
        Some(path) => match parse_read_log(path) {
            Ok(reads) => reads,
            Err(err) => return RunOutcome::IngestionFailed(err.to_string()),
        },
        None => Vec::new(),
    };

    // A fault ledger or manifest that was SUPPLIED and did not parse is
    // ingestion failure, exactly like a malformed journal: the operator asked
    // this run to be graded against that file, and silently degrading to
    // "then we have no run-level evidence" would turn a corrupt ledger into a
    // milder verdict than an absent one. Not supplied at all is a different
    // thing, and `crate::vacuity` reports it as that rule's own `NoVerdict`.
    let ledger = match &args.fault_log {
        Some(path) => match parse_fault_ledger(path) {
            Ok(ledger) => Some(ledger),
            Err(err) => return RunOutcome::IngestionFailed(err.to_string()),
        },
        None => None,
    };
    let manifest = match &args.run_manifest {
        Some(path) => match parse_run_manifest(path) {
            Ok(manifest) => Some(manifest),
            Err(err) => return RunOutcome::IngestionFailed(err),
        },
        None => None,
    };

    let mut reports = vec![
        zero_acked_lost_report(args, &journals),
        watermark_honesty_report(&journals, &reads),
        latest_view_report(args, &journals),
        retention_honesty_report(args, &journals),
        cache_transparency_report(args, &journals, &reads),
    ];
    reports.extend(vacuity_reports(
        args,
        &journals,
        ledger.as_ref(),
        manifest.as_ref(),
    ));

    RunOutcome::Judged {
        journal_count: args.journals.len(),
        line_count: journals.lines.len(),
        reports,
    }
}

/// §8.4's four run-level vacuity rules, as [`PredicateReport`]s so they land
/// in the same list — and therefore under the same composition rule — as the
/// five predicates (module docs).
fn vacuity_reports(
    args: &RunArgs,
    journals: &JournalSet,
    ledger: Option<&FaultLedger>,
    manifest: Option<&RunManifest>,
) -> Vec<PredicateReport> {
    vacuity::check_all(&VacuityInputs {
        journals,
        ledger,
        manifest,
        max_ambiguous_fraction: args.max_ambiguous_fraction,
        max_journal_silence_ms: args.max_journal_silence_ms,
    })
    .into_iter()
    .map(|(rule, verdict)| PredicateReport {
        predicate: rule,
        verdict: verdict.erase(),
    })
    .collect()
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
            fault_log: None,
            run_manifest: None,
            max_ambiguous_fraction: crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION,
            max_journal_silence_ms: vacuity::DEFAULT_MAX_JOURNAL_SILENCE_MS,
            max_racing_read_ms: crate::predicates::cache_transparency::DEFAULT_MAX_RACING_READ_MS,
        }
    }

    /// How many reports a full run produces: five predicates plus §8.4's
    /// four run-level vacuity rules.
    const REPORT_COUNT: usize = 9;

    fn verdict_of<'a>(outcome: &'a RunOutcome, predicate: &str) -> &'a Verdict<String> {
        match outcome {
            RunOutcome::Judged { reports, .. } => {
                &reports
                    .iter()
                    .find(|report| report.predicate == predicate)
                    .expect("every predicate is reported")
                    .verdict
            }
            other @ RunOutcome::IngestionFailed(_) => {
                panic!("expected a judged run, got {other:?}")
            }
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
                assert_eq!(reports.len(), REPORT_COUNT);
                assert!(
                    reports
                        .iter()
                        .all(|r| matches!(r.verdict, Verdict::NoVerdict(_)))
                );
            }
            other @ RunOutcome::IngestionFailed(_) => {
                panic!("expected a judged run, got {other:?}")
            }
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

    /// A supplied-but-corrupt fault ledger fails the whole run closed rather
    /// than degrading to "no run-level evidence" — which would make a corrupt
    /// ledger produce a MILDER outcome than an absent one for the predicates
    /// that would otherwise still have been graded.
    #[test]
    fn a_malformed_fault_ledger_fails_the_run_closed() {
        let journal = write_temp("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let bad = write_temp("not json\n");
        let mut args = args(vec![journal.path().to_owned()]);
        args.fault_log = Some(bad.path().to_owned());
        let outcome = run(&args);
        assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
        assert_eq!(outcome.exit_code(), 3);
    }

    #[test]
    fn a_malformed_run_manifest_fails_the_run_closed() {
        let journal = write_temp("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let bad = write_temp("{\"started_at_ms\":1}\n");
        let mut args = args(vec![journal.path().to_owned()]);
        args.run_manifest = Some(bad.path().to_owned());
        let outcome = run(&args);
        assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
        assert_eq!(outcome.exit_code(), 3);
    }

    /// The #208 composition change, stated as a test: a run that is vacuous
    /// by a run-level rule AND holds a proven violation reports the
    /// violation. Would catch a reintroduced pre-predicate short-circuit —
    /// the pre-#208 behaviour, in which a missing loadgen summary made this
    /// exact evidence set report `3` and hid the lost ack entirely.
    #[test]
    fn a_vacuous_run_that_also_convicts_reports_the_conviction() {
        // A loadgen-shaped journal (identity-bearing) with NO `.summary.json`
        // sidecar: `vacuity/loadgen-outcome-quality` is NoVerdict …
        let journal = write_temp(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"r\",\"tenant\":\"t\",\"record_count\":1,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        // … while the final state is missing the very record that ack promised.
        let final_state = write_temp(r#"{"present":[]}"#);
        let mut args = args(vec![journal.path().to_owned()]);
        args.final_state_fixture = Some(final_state.path().to_owned());
        let outcome = run(&args);
        assert!(matches!(
            verdict_of(&outcome, "zero-acked-lost"),
            Verdict::Violation(_)
        ));
        assert!(matches!(
            verdict_of(&outcome, crate::vacuity::RULE_LOADGEN_OUTCOMES),
            Verdict::NoVerdict(_)
        ));
        assert_eq!(
            outcome.exit_code(),
            2,
            "a proven lost ack is a fact the judge holds; an unrelated vacuity signal does not \
             unprove it"
        );
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
