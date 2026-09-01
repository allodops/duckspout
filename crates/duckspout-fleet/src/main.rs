//! `duckspout-fleet` — the CTK distributed-run driver (§8.4, D-5).
//!
//! Boots a multi-node fleet under a seeded schedule (crash/wipe environment
//! events included — `EnvironmentEvent`, never node-journaled), collects the
//! per-node NDJSON journals, and hands them to `duckspout-judge` for the
//! verdict. Ⓢ clap skeleton at bootstrap; fleet logic lands at v0.2
//! (arming-ledger row `ctk-distributed`).
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.4).

#![forbid(unsafe_code)]

use clap::Parser;

/// CTK fleet driver (§8.4): deterministic multi-node runs.
#[derive(Debug, Parser)]
#[command(name = "duckspout-fleet", version, about)]
struct Cli {
    /// Schedule seed; the same seed replays the same run.
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// Number of node processes to boot.
    #[arg(long, default_value_t = 3)]
    nodes: u16,
}

fn main() {
    let cli = Cli::parse();
    eprintln!(
        "duckspout-fleet: not yet implemented (lands at v0.2, §8.4) — seed {}, {} nodes requested",
        cli.seed, cli.nodes
    );
    std::process::exit(2);
}
