//! The judge binary's actual run logic (§8.4), split out of `main.rs` so it
//! is directly testable — ACPR finding MEDIUM-HIGH-4: every existing test
//! before this fix built a `JournalSet` by hand in memory; nothing drove
//! real NDJSON text through `parse_journal_file`/`ingest_journals` into the
//! predicate and out through the judge's own exit-code mapping. This module
//! is that real, callable pipeline: `main.rs` becomes a thin wrapper that
//! parses `clap` args into [`RunArgs`], calls [`run`], prints
//! [`RunOutcome`], and exits with [`RunOutcome::exit_code`] — every step a
//! test in `tests/` can also drive directly, without a subprocess.

use std::path::PathBuf;

use crate::final_state::InMemoryFinalState;
use crate::journal::ingest_journals;
use crate::predicates::zero_acked_lost::{self, ZeroAckedLostVerdict};
use crate::summary::{self, SummaryFinding};

/// The judge run's inputs — `main.rs`'s `Cli`, stripped of `clap` so it can
/// be constructed directly by a test.
#[derive(Debug, Clone)]
pub struct RunArgs {
    /// Paths of the per-node (plus loadgen) NDJSON journals to grade.
    pub journals: Vec<PathBuf>,
    /// DEV/TEST ONLY: a `FinalSystemState` fixture JSON
    /// (`crate::final_state` module docs). `None` honestly reports
    /// [`RunOutcome::NoBackend`] rather than fabricating a verdict.
    pub final_state_fixture: Option<PathBuf>,
    /// Ceiling on the loadgen run summary's ambiguous-outcome fraction
    /// (`crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION` docs).
    pub max_ambiguous_fraction: f64,
}

/// The judge's own outcome — one-to-one with `main.rs`'s `EXIT_CONTRACT`
/// (0/2/3 via [`RunOutcome::exit_code`]). Carries the same detail `main.rs`
/// prints, so a caller (the bin, or a test) can render or assert on it
/// without re-deriving anything.
#[derive(Debug, Clone, PartialEq)]
pub enum RunOutcome {
    /// Journal ingestion itself failed (malformed input; fails closed).
    IngestionFailed(String),
    /// The loadgen run-summary vacuity check (ACPR finding HIGH-1) found a
    /// reason not to trust this run's evidence.
    SummaryVacuous(Vec<SummaryFinding>),
    /// No `--final-state-fixture` was given — nothing to check against.
    NoBackend {
        /// How many journal files were ingested.
        journal_count: usize,
        /// How many total lines were ingested across every file.
        line_count: usize,
    },
    /// The fixture file could not be read or decoded.
    FixtureInvalid(String),
    /// The zero-acked-lost predicate's own verdict.
    Predicate(ZeroAckedLostVerdict),
}

impl RunOutcome {
    /// Maps this outcome to the judge's exit contract: `0` = Pass,
    /// `2` = Violation, `3` = `NoVerdict` (every other case — inconclusive
    /// or vacuous, never a pass).
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            RunOutcome::Predicate(ZeroAckedLostVerdict::Pass { .. }) => 0,
            RunOutcome::Predicate(ZeroAckedLostVerdict::Violation(_)) => 2,
            RunOutcome::IngestionFailed(_)
            | RunOutcome::SummaryVacuous(_)
            | RunOutcome::NoBackend { .. }
            | RunOutcome::FixtureInvalid(_)
            | RunOutcome::Predicate(ZeroAckedLostVerdict::NoVerdict(_)) => 3,
        }
    }
}

/// Runs the real judge pipeline: ingest → summary vacuity check → predicate
/// (module docs).
#[must_use]
pub fn run(args: &RunArgs) -> RunOutcome {
    let journals = match ingest_journals(&args.journals) {
        Ok(journals) => journals,
        Err(err) => return RunOutcome::IngestionFailed(err.to_string()),
    };

    if let Err(findings) = summary::check_summaries(&journals, args.max_ambiguous_fraction) {
        return RunOutcome::SummaryVacuous(findings);
    }

    let Some(fixture_path) = &args.final_state_fixture else {
        return RunOutcome::NoBackend {
            journal_count: args.journals.len(),
            line_count: journals.lines.len(),
        };
    };

    let fixture_text = match std::fs::read_to_string(fixture_path) {
        Ok(text) => text,
        Err(err) => {
            return RunOutcome::FixtureInvalid(format!(
                "reading {}: {err}",
                fixture_path.display()
            ));
        }
    };
    let final_state = match InMemoryFinalState::from_fixture_json(&fixture_text) {
        Ok(state) => state,
        Err(err) => {
            return RunOutcome::FixtureInvalid(format!("{}: {err}", fixture_path.display()));
        }
    };

    RunOutcome::Predicate(zero_acked_lost::check(&journals, &final_state))
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

    #[test]
    fn malformed_journal_is_ingestion_failed_exit_3() {
        let bad = write_temp("not json\n");
        let outcome = run(&RunArgs {
            journals: vec![bad.path().to_owned()],
            final_state_fixture: None,
            max_ambiguous_fraction: summary::DEFAULT_MAX_AMBIGUOUS_FRACTION,
        });
        assert!(matches!(outcome, RunOutcome::IngestionFailed(_)));
        assert_eq!(outcome.exit_code(), 3);
    }

    #[test]
    fn no_fixture_is_no_backend_exit_3() {
        let journal = write_temp("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let outcome = run(&RunArgs {
            journals: vec![journal.path().to_owned()],
            final_state_fixture: None,
            max_ambiguous_fraction: summary::DEFAULT_MAX_AMBIGUOUS_FRACTION,
        });
        assert!(matches!(outcome, RunOutcome::NoBackend { .. }));
        assert_eq!(outcome.exit_code(), 3);
    }
}
