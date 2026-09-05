//! End-to-end integration tests for the real judge pipeline (ACPR finding
//! MEDIUM-HIGH-4): every test in `src/predicates/zero_acked_lost.rs` builds
//! a `JournalSet` directly in memory, never actually parsing real NDJSON
//! text through `parse_journal_file`/`ingest_journals`. This file writes
//! REAL NDJSON text (and, where relevant, a real `.summary.json` sidecar)
//! to temp files and drives them through `duckspout_judge::runner::run` —
//! the exact function `main.rs` calls — asserting on the resulting
//! `RunOutcome` and its `exit_code()`, so the judge binary's own 0/2/3
//! exit-code mapping has real, automated coverage for at least one of each
//! contract value.

use std::io::Write as _;
use std::path::Path;

use duckspout_judge::runner::{RunArgs, RunOutcome, run};
use duckspout_judge::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION;

fn write_file(path: &Path, text: &str) {
    let mut file = std::fs::File::create(path).expect("create fixture file");
    file.write_all(text.as_bytes()).expect("write fixture file");
}

/// A clean run, fed through real NDJSON parsing end to end: one loadgen
/// journal acking a full range, a matching complete run summary, and a
/// final-state fixture where every acked index is present. Must exit `0`.
#[test]
fn a_clean_run_through_real_ndjson_parsing_exits_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("loadgen-0.ndjson");
    write_file(
        &journal_path,
        "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
         \"request_id\":\"loadgen-0-1000-0\",\"tenant\":\"tenant-a\",\
         \"record_count\":3,\"first_index\":0,\
         \"source_incarnation\":\"loadgen-0-1000\"}\n",
    );
    let mut summary_path = journal_path.as_os_str().to_owned();
    summary_path.push(".summary.json");
    write_file(
        Path::new(&summary_path),
        r#"{"node":"loadgen-0","sent":1,"acked":1,"timed_out":0,"rejected":0,"ambiguous":0}"#,
    );
    let fixture_path = dir.path().join("final_state.json");
    write_file(
        &fixture_path,
        r#"{"present":[{"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":0,"count":3}]}"#,
    );

    let outcome = run(&RunArgs {
        journals: vec![journal_path],
        final_state_fixture: Some(fixture_path),
        max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
    });
    assert!(
        matches!(
            outcome,
            RunOutcome::Predicate(
                duckspout_judge::predicates::zero_acked_lost::ZeroAckedLostVerdict::Pass {
                    checked: 1
                }
            )
        ),
        "expected a real Pass, got {outcome:?}"
    );
    assert_eq!(outcome.exit_code(), 0);
}

/// A genuinely lost acked record, fed through the same real pipeline: the
/// final-state fixture is missing index 1 of a 3-record ack. Must exit `2`.
#[test]
fn a_genuinely_lost_record_through_real_ndjson_parsing_exits_violation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("loadgen-0.ndjson");
    write_file(
        &journal_path,
        "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
         \"request_id\":\"loadgen-0-1000-0\",\"tenant\":\"tenant-a\",\
         \"record_count\":3,\"first_index\":0,\
         \"source_incarnation\":\"loadgen-0-1000\"}\n",
    );
    let mut summary_path = journal_path.as_os_str().to_owned();
    summary_path.push(".summary.json");
    write_file(
        Path::new(&summary_path),
        r#"{"node":"loadgen-0","sent":1,"acked":1,"timed_out":0,"rejected":0,"ambiguous":0}"#,
    );
    let fixture_path = dir.path().join("final_state.json");
    // Present: index 0 and 2 only — index 1 is genuinely missing.
    write_file(
        &fixture_path,
        r#"{"present":[
            {"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":0,"count":1},
            {"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":2,"count":1}
        ]}"#,
    );

    let outcome = run(&RunArgs {
        journals: vec![journal_path],
        final_state_fixture: Some(fixture_path),
        max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
    });
    match &outcome {
        RunOutcome::Predicate(
            duckspout_judge::predicates::zero_acked_lost::ZeroAckedLostVerdict::Violation(findings),
        ) => {
            assert_eq!(findings.len(), 1);
            assert!(findings[0].missing_indices.contains(&1));
        }
        other => panic!("expected a real Violation, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), 2);
}

/// The HIGH-1 vacuity repro through the real pipeline: a loadgen journal
/// with one clean ack line (perfectly parseable) but NO `.summary.json`
/// sidecar at all — exactly what a SIGKILL mid-run leaves behind, since the
/// summary is only written at clean exit. Before the HIGH-1 fix this
/// reported a false `Pass`; must now exit `3` (`NoVerdict`) even though a
/// final-state fixture proving the one ack is present is supplied.
#[test]
fn a_killed_mid_run_loadgen_missing_its_summary_exits_no_verdict_not_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("loadgen-0.ndjson");
    write_file(
        &journal_path,
        "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
         \"request_id\":\"loadgen-0-1000-0\",\"tenant\":\"tenant-a\",\
         \"record_count\":1,\"first_index\":0,\
         \"source_incarnation\":\"loadgen-0-1000\"}\n",
    );
    // Deliberately no `.summary.json` written — the SIGKILL-mid-run case.
    let fixture_path = dir.path().join("final_state.json");
    write_file(
        &fixture_path,
        r#"{"present":[{"tenant":"tenant-a","source_incarnation":"loadgen-0-1000","first_index":0,"count":1}]}"#,
    );

    let outcome = run(&RunArgs {
        journals: vec![journal_path],
        final_state_fixture: Some(fixture_path),
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
fn a_malformed_journal_line_through_real_ndjson_parsing_exits_no_verdict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("n1.ndjson");
    write_file(&journal_path, "not valid json at all\n");

    let outcome = run(&RunArgs {
        journals: vec![journal_path],
        final_state_fixture: None,
        max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
    });
    assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
    assert_eq!(outcome.exit_code(), 3);
}
