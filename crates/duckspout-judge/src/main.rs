//! `duckspout-judge` — the CTK verdict binary (§8.4).
//!
//! Deliberately a **separate binary** from the fleet driver (D-5, §8.4): the
//! process that runs the system must not be the process that grades it.
//! Reads the per-node NDJSON journals of a fleet run and delivers exactly
//! one verdict via its exit code — see [`EXIT_CONTRACT`]. Seeded-violation
//! replays must convict, and a run whose armed injectors never fired is
//! vacuous — `NoVerdict`, never `Pass` (§8.3).
//!
//! Ⓢ clap skeleton at bootstrap: with no checking logic yet, the honest
//! default verdict is `NoVerdict` (3).
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.4).

#![forbid(unsafe_code)]

use clap::Parser;

/// The judge's exit contract (§8.4), stable for every caller:
/// `0` = Pass, `2` = Violation, `3` = `NoVerdict` (inconclusive or vacuous).
const EXIT_CONTRACT: &str = "Exit codes: 0 = Pass · 2 = Violation · 3 = NoVerdict\n\
    A vacuous run (armed fault injectors that never fired, §8.3) is NoVerdict, never Pass.";

/// CTK judge (§8.4): grades a fleet run's journals.
#[derive(Debug, Parser)]
#[command(name = "duckspout-judge", version, about, after_help = EXIT_CONTRACT)]
struct Cli {
    /// Paths of the per-node NDJSON journals to grade.
    #[arg(long)]
    journal: Vec<std::path::PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    // No checking logic exists yet (lands at v0.2, ledger row
    // `ctk-distributed`): the only honest verdict is NoVerdict.
    eprintln!(
        "duckspout-judge: checking logic lands at v0.2 (§8.4); {} journal(s) ignored — NoVerdict",
        cli.journal.len()
    );
    std::process::exit(3);
}
