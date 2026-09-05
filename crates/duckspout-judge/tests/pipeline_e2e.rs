//! End-to-end integration tests for the real judge pipeline (ACPR finding
//! MEDIUM-HIGH-4): every test in `src/predicates/*.rs` builds a `JournalSet`
//! directly in memory, never actually parsing real NDJSON text through
//! `parse_journal_file`/`ingest_journals`. This file writes REAL NDJSON text
//! (plus real `.summary.json`, read-log, and fixture files) to temp files
//! and drives them through `duckspout_judge::runner::run` — the exact
//! function `main.rs` calls — asserting on the resulting `RunOutcome` and
//! its `exit_code()`, so the judge binary's own 0/2/3 exit-code mapping has
//! real, automated coverage for at least one of each contract value, and
//! each predicate has at least one end-to-end conviction.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use duckspout_judge::runner::{RunArgs, RunOutcome, run};
use duckspout_judge::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION;
use duckspout_judge::verdict::Verdict;

/// One loadgen `ClientAck` line for a 3-record batch, carrying every #206
/// coverage field: the partition, the batch's event-time edge (1500), and
/// the partition watermark in force when it was acked (1000 — still behind
/// the batch, so the batch is owed by any later `complete` read at or above
/// 1500).
const ACK_LINE: &str = "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
     \"request_id\":\"loadgen-0-1000-0\",\"tenant\":\"tenant-a\",\
     \"record_count\":3,\"first_index\":0,\"source_incarnation\":\"loadgen-0-1000\",\
     \"partition\":\"t0-s0\",\"max_event_time_ms\":1500,\"complete_through_ms\":1000}\n";

/// The drain commit that made coverage through 2000 real for `t0-s0`.
const COMMIT_LINE: &str = "{\"node\":\"n1\",\"seq\":0,\"event\":\"LakeCommitOk\",\
     \"partition\":\"t0-s0\",\"complete_through_ms\":2000}\n";

/// A second loadgen ack, this one carrying a changelog entry.
const CHANGELOG_ACK_LINE: &str = "{\"node\":\"loadgen-0\",\"seq\":1,\"event\":\"ClientAck\",\
     \"dataset\":\"dim_users\",\"changelog_key\":\"u1\",\"origin\":\"n1\",\
     \"changelog_seq\":7,\"value\":\"alice\"}\n";

/// Every record key `ACK_LINE` covers.
const ACKED_KEYS: &str = "[\"loadgen-0-1000-0\",\"loadgen-0-1000-1\",\"loadgen-0-1000-2\"]";

/// The final-state fixture in which all three acked records are present.
const FINAL_STATE_ALL_PRESENT: &str = r#"{"present":[{"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":0,"count":3}]}"#;

/// The latest-view fixture matching `CHANGELOG_ACK_LINE`'s fold.
const LATEST_VIEW_CORRECT: &str = r#"{"views":{"dim_users":{"u1":"alice"}}}"#;

fn write_file(path: &Path, text: &str) {
    let mut file = std::fs::File::create(path).expect("create fixture file");
    file.write_all(text.as_bytes()).expect("write fixture file");
}

/// Writes the loadgen's clean run-summary sidecar beside `journal_path`.
fn write_clean_summary(journal_path: &Path, resolved: u64) {
    let mut summary_path = journal_path.as_os_str().to_owned();
    summary_path.push(".summary.json");
    write_file(
        Path::new(&summary_path),
        &format!(
            r#"{{"node":"loadgen-0","sent":{resolved},"acked":{resolved},"timed_out":0,"rejected":0,"ambiguous":0}}"#
        ),
    );
}

/// A complete fleet-run evidence set on disk, so each test can vary exactly
/// the one file whose defect it is about.
struct Evidence {
    dir: tempfile::TempDir,
    journal: PathBuf,
}

impl Evidence {
    /// A clean run: one commit, one record ack, one changelog ack, and a
    /// matching run summary.
    fn clean() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = dir.path().join("fleet.ndjson");
        write_file(
            &journal,
            &format!("{COMMIT_LINE}{ACK_LINE}{CHANGELOG_ACK_LINE}"),
        );
        write_clean_summary(&journal, 2);
        Self { dir, journal }
    }

    fn with_file(&self, name: &str, text: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        write_file(&path, text);
        path
    }

    /// The judge invocation with every evidence file supplied — the shape a
    /// real nightly `ctk-distributed` run uses.
    fn args(
        &self,
        final_state: Option<PathBuf>,
        read_log: Option<PathBuf>,
        latest_view: Option<PathBuf>,
    ) -> RunArgs {
        RunArgs {
            journals: vec![self.journal.clone()],
            final_state_fixture: final_state,
            read_log,
            latest_view_fixture: latest_view,
            max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
        }
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

/// A clean run with every evidence file present, fed through real NDJSON
/// parsing end to end: every predicate must pass, and the run must exit `0`.
#[test]
fn a_clean_run_with_every_evidence_file_exits_pass() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        Some(evidence.with_file("final_state.json", FINAL_STATE_ALL_PRESENT)),
        Some(evidence.with_file(
            "reads.ndjson",
            &format!(
                "{{\"tenant\":\"tenant-a\",\"partition\":\"t0-s0\",\"concern\":\"complete\",\
                 \"outcome\":\"served\",\"complete_through_ms\":2000,\
                 \"record_keys\":{ACKED_KEYS}}}\n"
            ),
        )),
        Some(evidence.with_file("latest_view.json", LATEST_VIEW_CORRECT)),
    ));
    match &outcome {
        RunOutcome::Judged { reports, .. } => {
            for report in reports {
                assert!(
                    matches!(report.verdict, Verdict::Pass { .. }),
                    "{} did not pass: {:?}",
                    report.predicate,
                    report.verdict
                );
            }
        }
        other => panic!("expected a judged run, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), 0);
}

/// The same clean journals, judged with only the zero-acked-lost evidence:
/// that predicate passes, but the run must still exit `3` — a run that never
/// looked at a served answer or a latest view certifies neither (§8.4:
/// skipped ≠ passed). Would catch an exit-code rule that reported the best
/// predicate's verdict instead of the whole run's.
#[test]
fn one_predicate_passing_with_the_others_unevidenced_is_not_a_passing_run() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        Some(evidence.with_file("final_state.json", FINAL_STATE_ALL_PRESENT)),
        None,
        None,
    ));
    assert!(matches!(
        verdict_of(&outcome, "zero-acked-lost"),
        Verdict::Pass { .. }
    ));
    assert!(matches!(
        verdict_of(&outcome, "watermark-honesty"),
        Verdict::NoVerdict(_)
    ));
    assert!(matches!(
        verdict_of(&outcome, "latest-view"),
        Verdict::NoVerdict(_)
    ));
    assert_eq!(outcome.exit_code(), 3);
}

/// A genuinely lost acked record, through the real pipeline: the
/// final-state fixture is missing index 1 of the 3-record ack. Exit `2`.
#[test]
fn a_genuinely_lost_record_exits_violation() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        Some(evidence.with_file(
            "final_state.json",
            r#"{"present":[
                {"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":0,"count":1},
                {"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":2,"count":1}
            ]}"#,
        )),
        None,
        None,
    ));
    match verdict_of(&outcome, "zero-acked-lost") {
        Verdict::Violation(findings) => {
            assert_eq!(findings.len(), 1);
            assert!(findings[0].contains('1'), "finding: {}", findings[0]);
        }
        other => panic!("expected a real Violation, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), 2);
}

/// A `complete` answer served at a watermark no journaled commit ever
/// established — and containing every acked record anyway. The optimistic
/// answer happened to be right and is still a violation (§8.4). Exit `2`.
#[test]
fn a_complete_read_served_over_unproven_coverage_exits_violation() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        None,
        Some(evidence.with_file(
            "reads.ndjson",
            &format!(
                "{{\"tenant\":\"tenant-a\",\"partition\":\"t0-s0\",\"concern\":\"complete\",\
                 \"outcome\":\"served\",\"complete_through_ms\":9999,\
                 \"record_keys\":{ACKED_KEYS}}}\n"
            ),
        )),
        None,
    ));
    match verdict_of(&outcome, "watermark-honesty") {
        Verdict::Violation(findings) => {
            assert_eq!(findings.len(), 1);
            assert!(findings[0].contains("9999"), "finding: {}", findings[0]);
        }
        other => panic!("expected a real Violation, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), 2);
}

/// A record acked while the watermark was still behind it, missing from a
/// `complete` answer served above it. Exit `2`.
#[test]
fn a_record_missing_from_a_complete_answer_exits_violation() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        None,
        Some(evidence.with_file(
            "reads.ndjson",
            "{\"tenant\":\"tenant-a\",\"partition\":\"t0-s0\",\"concern\":\"complete\",\
             \"outcome\":\"served\",\"complete_through_ms\":2000,\
             \"record_keys\":[\"loadgen-0-1000-0\",\"loadgen-0-1000-2\"]}\n",
        )),
        None,
    ));
    match verdict_of(&outcome, "watermark-honesty") {
        Verdict::Violation(findings) => {
            assert!(
                findings[0].contains("loadgen-0-1000-1"),
                "finding: {}",
                findings[0]
            );
        }
        other => panic!("expected a real Violation, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), 2);
}

/// A latest view that lost the acked changelog's value. Exit `2`.
#[test]
fn a_stale_latest_view_exits_violation() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        None,
        None,
        Some(evidence.with_file(
            "latest_view.json",
            r#"{"views":{"dim_users":{"u1":"STALE"}}}"#,
        )),
    ));
    match verdict_of(&outcome, "latest-view") {
        Verdict::Violation(findings) => {
            assert!(findings[0].contains("STALE"), "finding: {}", findings[0]);
        }
        other => panic!("expected a real Violation, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), 2);
}

/// The HIGH-1 vacuity repro through the real pipeline: a loadgen journal
/// with clean ack lines (perfectly parseable) but NO `.summary.json` sidecar
/// at all — exactly what a SIGKILL mid-run leaves behind, since the summary
/// is only written at clean exit. Must exit `3` even though every other
/// piece of evidence says the run was clean.
#[test]
fn a_killed_mid_run_loadgen_missing_its_summary_exits_no_verdict_not_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("fleet.ndjson");
    write_file(&journal_path, &format!("{COMMIT_LINE}{ACK_LINE}"));
    // Deliberately no `.summary.json` written — the SIGKILL-mid-run case.
    let fixture_path = dir.path().join("final_state.json");
    write_file(&fixture_path, FINAL_STATE_ALL_PRESENT);

    let outcome = run(&RunArgs {
        journals: vec![journal_path],
        final_state_fixture: Some(fixture_path),
        read_log: None,
        latest_view_fixture: None,
        max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
    });
    assert!(
        matches!(outcome, RunOutcome::SummaryVacuous(_)),
        "expected the missing-summary vacuity finding, got {outcome:?}"
    );
    assert_eq!(
        outcome.exit_code(),
        3,
        "a run this predicate never actually observed the end of must NEVER be exit 0"
    );
}

/// A malformed journal line, fed through the real ingestion path, must
/// exit `3` (fails closed) — not panic, not silently skip the bad line.
#[test]
fn a_malformed_journal_line_exits_no_verdict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("n1.ndjson");
    write_file(&journal_path, "not valid json at all\n");

    let outcome = run(&RunArgs {
        journals: vec![journal_path],
        final_state_fixture: None,
        read_log: None,
        latest_view_fixture: None,
        max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
    });
    assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
    assert_eq!(outcome.exit_code(), 3);
}

/// A malformed read-log line fails the whole run closed, exactly as a
/// malformed journal line does: served-read evidence that did not parse is
/// not evidence, and grading the rest of the run over it would certify a
/// query surface nobody actually recorded.
#[test]
fn a_malformed_read_log_line_exits_no_verdict() {
    let evidence = Evidence::clean();
    let outcome = run(&evidence.args(
        None,
        Some(evidence.with_file("reads.ndjson", "{\"tenant\":\"tenant-a\"}\n")),
        None,
    ));
    assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
    assert_eq!(outcome.exit_code(), 3);
}
