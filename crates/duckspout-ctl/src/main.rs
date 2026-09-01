//! `duckspoutctl` — the operator CLI (§9).
//!
//! Ⓢ clap skeleton at bootstrap. First subcommand: `size` (§9.2), which
//! computes per-node volume provisioning, `hot.max_bytes`, the ladder
//! thresholds, and the stall runway from
//! `pvc = (rate × expansion × residency × RF ÷ nodes + staging) ÷ 0.6`.
//!
//! Design home: `docs/operations.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §9.2).

#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};

/// Operator CLI for a `DuckSpout` cluster (§9).
#[derive(Debug, Parser)]
#[command(name = "duckspoutctl", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Size a node volume (§9.2): pvc = (rate × expansion × residency × RF
    /// ÷ nodes + staging) ÷ 0.6, plus the derived `hot.max_bytes` and ladder
    /// thresholds.
    Size {
        /// Cluster-wide wire ingest, bytes/s.
        #[arg(long)]
        rate_bytes_per_sec: u64,
        /// Wire→hot representation factor (measure; start at 1.4).
        #[arg(long, default_value_t = 1.4)]
        expansion: f64,
        /// Drain-stall budget, seconds (floor: `drain.max_age`; production
        /// recommendation ≥ 7200).
        #[arg(long)]
        residency_secs: u64,
        /// Replication factor (`cluster.rf`).
        #[arg(long, default_value_t = 2)]
        rf: u16,
        /// Node count.
        #[arg(long)]
        nodes: u16,
        /// Fixed per-node scratch, bytes (start at 20 GB).
        #[arg(long, default_value_t = 20_000_000_000)]
        staging_bytes: u64,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Size { .. } => {
            eprintln!("duckspoutctl size: not yet implemented (lands at v0.1, §9.2)");
            std::process::exit(2);
        }
    }
}
