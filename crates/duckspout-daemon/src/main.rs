//! The `DuckSpout` node daemon: the thin CLI wrapper (§10.4) over the
//! `duckspout_daemon` library crate — config parsing, `--dump-config-manifest`,
//! signal wiring, and calling [`duckspout_daemon::wiring::Daemon`]. Zero
//! protocol logic of its own; see the library crate's docs (`src/lib.rs`)
//! for why it is a library first, and `wiring`'s module docs for what is
//! wired at v0.1 (issue #38) and what is deliberately deferred.
//!
//! Design home: `docs/operations.md` (§9).

#![forbid(unsafe_code)]

use std::path::PathBuf;

use clap::Parser;
use duckspout_daemon::{config, constants, manifest, wiring};

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
    /// environment-variable overrides, secrets by file path). Required
    /// unless `--dump-config-manifest` is given.
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

    let Some(path) = cli.config.as_deref() else {
        return Err("--config <path> is required to boot the daemon (or pass \
                     --dump-config-manifest to print the setting surface and exit)"
            .into());
    };
    let loaded = config::load(Some(path))?;
    tracing::info!(
        data_dir = %loaded.node.data_dir.display(),
        otlp_listen = loaded.node.otlp_listen,
        "configuration loaded"
    );

    let daemon = wiring::Daemon::boot(&loaded, constants::OBSERVATION_LISTEN_PORT_DEFAULT).await?;
    tracing::info!(
        otlp_addr = %daemon.otlp_addr(),
        status_addr = %daemon.status_addr(),
        node_id = %daemon.handle().node_id(),
        "daemon booted"
    );

    daemon.serve(shutdown_signal()).await;
    Ok(())
}

/// Resolves once the process receives SIGTERM (§9.1.2's shallow-drain
/// choreography) or SIGINT (`Ctrl-C`, the same choreography — a convenient
/// local-dev equivalent).
async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installing the SIGTERM handler");
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
