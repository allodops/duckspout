//! `duckspout-judge` — the CTK verdict binary (§8.4).
//!
//! Deliberately a **separate binary** from the fleet driver (D-5, §8.4): the
//! process that runs the system must not be the process that grades it.
//! Reads the per-node (plus loadgen) NDJSON journals of a fleet run and
//! delivers exactly one verdict via its exit code — see [`EXIT_CONTRACT`].
//! Seeded-violation replays must convict, and a run whose armed injectors
//! never fired is vacuous — `NoVerdict`, never `Pass` (§8.3).
//!
//! This binary is now a thin `clap` wrapper: the actual pipeline
//! (ingest → loadgen run-summary vacuity check → predicate) lives in
//! `duckspout_judge::runner`, split out specifically so it is directly
//! testable (ACPR finding MEDIUM-HIGH-4) without spawning a subprocess.
//!
//! # #205: what's wired for real here, and what's deferred
//!
//! Journal ingestion (`duckspout_judge::journal`) is real: malformed input
//! fails closed, exactly as the checking logic below relies on. The
//! zero-acked-lost predicate (`duckspout_judge::predicates::zero_acked_lost`)
//! is real and unit-tested against a `FinalSystemState` test double
//! (`duckspout_judge::final_state`). What is NOT wired is a REAL final-system
//! backend: `duckspout-fleet` has no real multi-node run to judge yet
//! (§8.4's distributed tier lands at v0.2; #203/#204's fault-schedule work
//! hasn't landed), so there is nothing to query for real in this PR. Passing
//! `--final-state-fixture` runs the real predicate against a JSON test
//! double (`duckspout_judge::final_state`'s own module docs) — a DEV/TEST
//! convenience that exercises the real Pass/Violation/NoVerdict contract
//! end-to-end; omitting it reports `NoVerdict` honestly, since a judge with
//! no backend to check against has proven nothing (not "no fault fired",
//! §8.3's vacuity discipline, but the same posture: an unproven claim of
//! passing is never reported as passing).

#![forbid(unsafe_code)]

use std::path::PathBuf;

use clap::Parser;
use duckspout_judge::predicates::zero_acked_lost::ZeroAckedLostVerdict;
use duckspout_judge::runner::{RunArgs, RunOutcome, run};
use duckspout_judge::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION;

/// The judge's exit contract (§8.4), stable for every caller:
/// `0` = Pass, `2` = Violation, `3` = `NoVerdict` (inconclusive or vacuous).
const EXIT_CONTRACT: &str = "Exit codes: 0 = Pass · 2 = Violation · 3 = NoVerdict\n\
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
                "duckspout-judge: journal ingestion failed: {err} — NoVerdict \
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
        RunOutcome::NoBackend {
            journal_count,
            line_count,
        } => {
            eprintln!(
                "duckspout-judge: {journal_count} journal(s) parsed OK ({line_count} record(s)); \
                 no final-system backend wired yet (§8.4's real hot/lake read-back lands with the \
                 distributed tier's fleet wiring, #205's own scope note) — NoVerdict"
            );
        }
        RunOutcome::FixtureInvalid(err) => {
            eprintln!("duckspout-judge: final-state fixture invalid: {err} — NoVerdict");
        }
        RunOutcome::Predicate(ZeroAckedLostVerdict::Pass { checked }) => {
            eprintln!("duckspout-judge: zero-acked-lost PASS ({checked} acked request(s) checked)");
        }
        RunOutcome::Predicate(ZeroAckedLostVerdict::Violation(findings)) => {
            eprintln!(
                "duckspout-judge: zero-acked-lost VIOLATION ({} finding(s)):",
                findings.len()
            );
            for finding in findings {
                eprintln!(
                    "  request {} (tenant {}): missing indices {:?}",
                    finding.request_id, finding.tenant, finding.missing_indices
                );
            }
        }
        RunOutcome::Predicate(ZeroAckedLostVerdict::NoVerdict(reason)) => {
            eprintln!("duckspout-judge: zero-acked-lost NoVerdict: {reason}");
        }
    }
}
