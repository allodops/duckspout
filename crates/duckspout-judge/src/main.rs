//! `duckspout-judge` — the CTK verdict binary (§8.4).
//!
//! Deliberately a **separate binary** from the fleet driver (D-5, §8.4): the
//! process that runs the system must not be the process that grades it.
//! Reads the per-node (plus loadgen) NDJSON journals of a fleet run and
//! delivers exactly one verdict via its exit code — see [`EXIT_CONTRACT`].
//! Seeded-violation replays must convict, and a run whose armed injectors
//! never fired is vacuous — `NoVerdict`, never `Pass` (§8.3).
//!
//! This binary is a thin `clap` wrapper: the actual pipeline
//! (ingest → loadgen run-summary vacuity check → every predicate) lives in
//! `duckspout_judge::runner`, split out specifically so it is directly
//! testable (ACPR finding MEDIUM-HIGH-4) without spawning a subprocess.
//!
//! # What's wired for real here, and what's deferred
//!
//! Journal and read-log ingestion (`duckspout_judge::journal`,
//! `duckspout_judge::read_log`) are real: malformed input fails closed,
//! exactly as the checking logic relies on. All three predicates
//! (`zero_acked_lost`, `watermark_honesty`, `latest_view`) are real and
//! unit-tested. What is NOT wired is a REAL backend behind either read-back
//! surface (`FinalSystemState`, `LatestView`): `duckspout-fleet` has no real
//! multi-node run to judge yet (§8.4's distributed tier lands at v0.2;
//! #204/#208's work is where a real fleet run and a real query client
//! appear), so there is nothing to query for real. The `--*-fixture` flags
//! run the real predicates against JSON test doubles
//! (`duckspout_judge::final_state`'s own module docs) — a DEV/TEST
//! convenience that exercises the real Pass/Violation/NoVerdict contract end
//! to end; omitting one reports `NoVerdict` for that predicate honestly,
//! since a judge with no backend to check against has proven nothing.

#![forbid(unsafe_code)]

use std::path::PathBuf;

use clap::Parser;
use duckspout_judge::runner::{RunArgs, RunOutcome, run};
use duckspout_judge::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION;
use duckspout_judge::verdict::Verdict;

/// The judge's exit contract (§8.4), stable for every caller:
/// `0` = Pass, `2` = Violation, `3` = `NoVerdict` (inconclusive or vacuous).
const EXIT_CONTRACT: &str = "Exit codes: 0 = Pass · 2 = Violation · 3 = NoVerdict\n\
    Every predicate runs on every invocation; the run passes only if all of them do.\n\
    A vacuous run (armed fault injectors that never fired, §8.3) is NoVerdict, never Pass.";

/// CTK judge (§8.4): grades a fleet run's journals.
#[derive(Debug, Parser)]
#[command(name = "duckspout-judge", version, about, after_help = EXIT_CONTRACT)]
struct Cli {
    /// Paths of the per-node (plus loadgen) NDJSON journals to grade.
    #[arg(long)]
    journal: Vec<PathBuf>,

    /// DEV/TEST ONLY: a `FinalSystemState` fixture JSON
    /// (`duckspout_judge::final_state` module docs) standing in for a real
    /// hot/lake read-back — no real backend is wired yet (module docs).
    /// Omitted: zero-acked-lost honestly reports `NoVerdict` rather than
    /// fabricating a verdict with nothing real to check the acks against.
    #[arg(long)]
    final_state_fixture: Option<PathBuf>,

    /// The query client's served-read log (`duckspout_judge::read_log`): one
    /// NDJSON object per read issued, with what it was served and at which
    /// `complete_through`. Omitted: watermark honesty reports `NoVerdict`
    /// for its query-side half rather than passing a run whose answers
    /// nobody recorded.
    #[arg(long)]
    read_log: Option<PathBuf>,

    /// DEV/TEST ONLY: a `LatestView` fixture JSON
    /// (`duckspout_judge::final_state`) standing in for a real
    /// `<dataset>_latest` read-back. Omitted: latest-view reports
    /// `NoVerdict`.
    #[arg(long)]
    latest_view_fixture: Option<PathBuf>,

    /// Ceiling on the fraction of a loadgen's resolved requests that came
    /// back `Ambiguous` before its run summary is treated as unreliable
    /// evidence (ACPR finding HIGH-1; reasoning:
    /// `duckspout_judge::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION` docs).
    #[arg(long, default_value_t = DEFAULT_MAX_AMBIGUOUS_FRACTION)]
    max_ambiguous_fraction: f64,
}

fn main() {
    let cli = Cli::parse();
    let outcome = run(&RunArgs {
        journals: cli.journal,
        final_state_fixture: cli.final_state_fixture,
        read_log: cli.read_log,
        latest_view_fixture: cli.latest_view_fixture,
        max_ambiguous_fraction: cli.max_ambiguous_fraction,
    });
    report(&outcome);
    std::process::exit(outcome.exit_code());
}

/// Prints the same operator-facing detail the judge always has, keyed off
/// [`RunOutcome`]'s variant.
fn report(outcome: &RunOutcome) {
    match outcome {
        RunOutcome::IngestionFailed(err) => {
            eprintln!(
                "duckspout-judge: evidence ingestion failed: {err} — NoVerdict \
                 (ambiguity fails closed, §8.4)"
            );
        }
        RunOutcome::SummaryVacuous(findings) => {
            eprintln!(
                "duckspout-judge: loadgen run-summary check found {} vacuity finding(s) — \
                 NoVerdict (§8.4 vacuity teeth, ACPR finding HIGH-1):",
                findings.len()
            );
            for finding in findings {
                eprintln!("  {finding}");
            }
        }
        RunOutcome::Judged {
            journal_count,
            line_count,
            reports,
        } => {
            eprintln!(
                "duckspout-judge: {journal_count} journal(s) parsed OK ({line_count} record(s)); \
                 {} predicate(s) judged:",
                reports.len()
            );
            for report in reports {
                match &report.verdict {
                    Verdict::Pass { checked } => {
                        eprintln!("  {} PASS ({checked} check(s))", report.predicate);
                    }
                    Verdict::Violation(findings) => {
                        eprintln!(
                            "  {} VIOLATION ({} finding(s)):",
                            report.predicate,
                            findings.len()
                        );
                        for finding in findings {
                            eprintln!("    {finding}");
                        }
                    }
                    Verdict::NoVerdict(reason) => {
                        eprintln!("  {} NoVerdict: {reason}", report.predicate);
                    }
                }
            }
        }
    }
}
