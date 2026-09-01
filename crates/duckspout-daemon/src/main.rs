//! The `DuckSpout` node daemon.
//!
//! A thin composition of the protocol crates (§10.4): wiring, signal
//! handling, and the cadence loop that ticks drains and retention — zero
//! protocol logic of its own. Anything the daemon can do, an embedder can do
//! by depending on the crates directly.
//!
//! Complete at bootstrap: the full 32-setting configuration surface
//! ([`config`], §9.6.1), the §9.6.3 fixed constants ([`constants`]), and
//! `--dump-config-manifest` ([`manifest`]) — the golden-manifest mechanism
//! `check-invariants.mjs` diffs against `floors/config-surface.toml`
//! (SEED s§7). Ⓢ v0.1: the wiring itself and the status endpoint.
//!
//! Design home: `docs/operations.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §9).

#![forbid(unsafe_code)]

mod config;
mod constants;
mod manifest;
mod wiring;

use std::path::PathBuf;

use clap::Parser;

/// The `DuckSpout` node daemon (§9).
#[derive(Debug, Parser)]
#[command(name = "duckspout-daemon", version, about)]
struct Cli {
    /// Print the configuration-surface manifest (name, type, default, since)
    /// as TOML to stdout and exit. CI diffs this against
    /// `floors/config-surface.toml` — the §9.6.4 KISS ratchet.
    #[arg(long)]
    dump_config_manifest: bool,

    /// Path to the node's TOML configuration file (§9.6: one TOML file,
    /// environment-variable overrides, secrets by file path).
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.dump_config_manifest {
        print!("{}", manifest::render_toml()?);
        return Ok(());
    }

    tracing_subscriber::fmt::init();

    if let Some(path) = cli.config.as_deref() {
        let loaded = config::load(Some(path))?;
        tracing::info!(data_dir = %loaded.node.data_dir.display(), "configuration loaded");
    }

    tracing::info!("duckspout-daemon: {} — exiting", wiring::STATUS);
    Ok(())
}
