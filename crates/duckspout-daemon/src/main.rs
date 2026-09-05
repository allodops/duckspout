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
use std::sync::Arc;

use clap::Parser;
use duckspout_daemon::{config, constants, manifest, system, wiring};
use duckspout_types::TraceSink;

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

    /// The observation listener's port (§9.3.2). Not a §9.6.1 setting
    /// (`constants::OBSERVATION_LISTEN_PORT_DEFAULT`'s own doc comment) —
    /// this CLI-only override exists so co-located node processes on one
    /// host (`duckspout-fleet`, issue #201) can each bind a distinct port;
    /// a single-node deployment never needs it.
    #[arg(long, default_value_t = constants::OBSERVATION_LISTEN_PORT_DEFAULT)]
    status_listen: u16,

    /// When given, journals this node's real §3.3 events as NDJSON to this
    /// path through [`duckspout_ctk::NdjsonTraceWriter`] (§3.7, §8.4, issue
    /// #201) — every trace-capable port wired via [`wiring::Daemon::boot`].
    /// Absent by default: a plain `duckspout-daemon --config …` journals
    /// nothing, unchanged from before this flag existed.
    #[arg(long)]
    trace_out: Option<PathBuf>,

    /// Fault-injection-only (§8.4, issue #203): stalls this many
    /// milliseconds between `PutPart` and `LakeCommit` on every drain,
    /// through [`duckspout_daemon::fault::StallingLakeCommitter`] — widens
    /// the real §8.4 "partition owner mid-drain" window wide enough for
    /// `duckspout-fleet`'s node-kill injector to land a real `SIGKILL`
    /// inside it deterministically. Not a §9.6 setting (same convention as
    /// `--status-listen`/`--trace-out`): a real deployment never passes
    /// this, and `0` (the default) is a measured, tested exact
    /// pass-through — `crate::fault`'s own module docs and tests.
    #[arg(long, default_value_t = 0)]
    fault_drain_commit_delay_ms: u64,
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

    let trace_sink = build_trace_sink(cli.trace_out.as_deref())?;
    let fault_drain_commit_delay =
        std::time::Duration::from_millis(cli.fault_drain_commit_delay_ms);
    let daemon = wiring::Daemon::boot(
        &loaded,
        cli.status_listen,
        trace_sink,
        fault_drain_commit_delay,
    )
    .await?;
    tracing::info!(
        otlp_addr = %daemon.otlp_addr(),
        status_addr = %daemon.status_addr(),
        flight_addr = %daemon.flight_addr(),
        node_id = %daemon.handle().node_id(),
        "daemon booted"
    );

    daemon.serve(shutdown_signal()).await;
    Ok(())
}

/// Builds the §3.7 capture-side [`TraceSink`] `--trace-out` requests, or
/// `None` when the flag is absent (`Cli::trace_out`'s own doc comment).
/// Journals as this process's own [`system::detect_node_id`] — computed a
/// second time here (boot computes it again inside [`wiring::Daemon::boot`])
/// rather than threading the id back out of boot: both calls are pure and
/// deterministic (the same `/proc/sys/kernel/hostname` or
/// [`system::DUCKSPOUT_NODE_HOSTNAME_OVERRIDE`] read), so they always agree.
///
/// # Errors
///
/// If `path` cannot be created (a bad `duckspout-fleet`-supplied journal
/// directory, most likely) — fails closed rather than booting silently
/// unjournaled when the operator explicitly asked for a journal (R-3).
fn build_trace_sink(path: Option<&std::path::Path>) -> Result<Option<Arc<dyn TraceSink>>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let file = std::fs::File::create(path)
        .map_err(|e| format!("opening --trace-out {}: {e}", path.display()))?;
    let node_id = system::detect_node_id(system::V01_FIXED_INCARNATION);
    Ok(Some(
        Arc::new(duckspout_ctk::NdjsonTraceWriter::new(node_id, file)) as Arc<dyn TraceSink>,
    ))
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
