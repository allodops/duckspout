//! `duckspout-fleet` — the CTK distributed-run driver (§8.4, D-5).
//!
//! Issue #201's foundational slice: **node provisioning**, **real
//! `MinIO`/Postgres wiring**, and a **boot/drive-load smoke loop** — no
//! fault injection (issues #203/#204), no judge (#205–#208), no real
//! `duckspout-loadgen` internals (#202). Those follow-ons plug into the
//! seams this binary establishes: [`topology`] provisions real
//! `duckspout-daemon` processes with distinct identities against a shared
//! real Postgres catalog and (by default) a real `MinIO` bucket;
//! [`process`] boots and supervises them, journaling each one's real §3.3
//! events as NDJSON (`duckspout_ctk::trace_writer::NdjsonTraceWriter`, wired
//! through `duckspout-daemon`'s new `--trace-out`); [`load`] is the
//! placeholder drive-load driver §8.4's own text licenses until #202 lands.
//!
//! This binary reports its own smoke-loop status (booted / load accepted /
//! watermarks advanced) — it is explicitly NOT the §8.4 judge: it makes no
//! Pass/Violation/NoVerdict claim, and its exit codes are its own (module
//! docs of [`run`]), never to be confused with `duckspout-judge`'s future
//! ones.
//!
//! Design home: `docs/verification.md` §8.4 (until absorption, `DUCKSPOUT.md`
//! §8.4).

#![forbid(unsafe_code)]

mod backend_check;
mod load;
mod process;
mod topology;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::Parser;

use topology::{FleetPlan, LakeStorage, NodeSpec};

/// CTK fleet driver (§8.4): provisions and drives a real multi-node
/// `duckspout-daemon` fleet against real `MinIO` + Postgres.
#[derive(Debug, Parser)]
#[command(name = "duckspout-fleet", version, about)]
struct Cli {
    /// Schedule seed; the same seed reproduces the same node names/layout.
    /// No fault schedule exists yet to seed randomness for (issues
    /// #203/#204) — at v0.2 this only seeds deterministic node naming
    /// (`topology::node_name`), so two runs with the same seed and node
    /// count lay out identical work directories.
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// Number of `duckspout-daemon` processes to boot.
    #[arg(long, default_value_t = 3)]
    nodes: u16,

    /// Root directory for per-node data/config/journals/logs. Defaults to
    /// a seed-scoped directory under the OS temp dir. Always left on disk
    /// on exit (success or failure) — printed at the end of the run — so
    /// journals and stderr logs stay inspectable.
    #[arg(long)]
    work_dir: Option<PathBuf>,

    /// Path to the `duckspout-daemon` binary. Defaults to a sibling of this
    /// binary's own path, falling back to `PATH`.
    #[arg(long)]
    daemon_bin: Option<PathBuf>,

    /// Optional path to the `duckspout-loadgen` binary (issue #202): when
    /// given, spawned as an additional fleet member pointed at node 0
    /// (§8.4: "the load generator is a first-class fleet member"). Its
    /// stub currently exits 2 immediately ("not yet implemented") — this
    /// fleet run logs that, but never fails on it.
    #[arg(long)]
    loadgen_bin: Option<PathBuf>,

    /// `cluster.rf` for every node.
    #[arg(long, default_value_t = duckspout_daemon::config::defaults::rf())]
    rf: u16,

    /// First node's OTLP/gRPC port; node i binds `otlp_base_port + i`.
    #[arg(long, default_value_t = duckspout_daemon::config::defaults::otlp_listen())]
    otlp_base_port: u16,

    /// First node's Arrow Flight port; node i binds `flight_base_port + i`.
    #[arg(long, default_value_t = duckspout_daemon::config::defaults::flight_listen())]
    flight_base_port: u16,

    /// First node's peer-listen port (§9.6.1; not yet dialed by anything —
    /// module docs of `duckspout-daemon`'s `wiring.rs` — but still assigned
    /// distinctly per node so `cluster.seed_peers` entries are honest).
    #[arg(long, default_value_t = duckspout_daemon::config::defaults::peer_listen())]
    peer_base_port: u16,

    /// First node's `/status` observation port; node i binds
    /// `status_base_port + i` (`duckspout-daemon --status-listen`, issue
    /// #201 — the production default collides across co-located nodes).
    #[arg(long, default_value_t = duckspout_daemon::constants::OBSERVATION_LISTEN_PORT_DEFAULT)]
    status_base_port: u16,

    /// The shared Postgres catalog DSN every node attaches (§7.3's
    /// multi-process answer) — `deploy/compose/compose.yaml`'s default.
    #[arg(
        long,
        default_value = "postgres://duckspout@127.0.0.1:5432/duckspout_catalog"
    )]
    postgres_dsn: String,

    /// The Postgres password (§9.5: secrets are file paths — this fleet
    /// runner writes it to a file under `work_dir` and hands the daemons
    /// that path). `deploy/compose/compose.yaml`'s dev/CI-only default.
    #[arg(long, default_value = "duckspout-dev")]
    postgres_password: String,

    /// Use a local-filesystem lake shared by every node instead of real
    /// `MinIO` — an escape hatch for a box with no `MinIO` running.
    /// **Not the default**: §8.4 calls for real `MinIO`.
    #[arg(long)]
    local_lake: bool,

    /// The `MinIO`/S3-compatible endpoint (`host:port`, no scheme).
    /// `deploy/compose/compose.yaml`'s default.
    #[arg(long, default_value = "127.0.0.1:9000")]
    s3_endpoint: String,

    /// The bucket every node's `lake.uri` resolves against. Must already
    /// exist in `MinIO` (this runner does not create buckets — `mc mb`, or
    /// `deploy/compose/`'s own provisioning, is the documented path).
    #[arg(long, default_value = "duckspout-fleet")]
    s3_bucket: String,

    /// The shared `DATA_PATH` prefix under `s3_bucket` — deliberately
    /// FIXED, not run-scoped: a `DuckLake` catalog pins its `DATA_PATH` for
    /// its whole lifetime (`tests/trace_capture_real_backends.rs`'s own
    /// module docs, proven empirically against real `MinIO` + Postgres), so
    /// a varying prefix breaks the second of two runs against the SAME
    /// persistent backend.
    #[arg(long, default_value = "duckspout-fleet")]
    s3_prefix: String,

    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    #[arg(long, default_value = "duckspout")]
    s3_access_key_id: String,

    /// `deploy/compose/compose.yaml`'s dev/CI-only default.
    #[arg(long, default_value = "duckspout-dev")]
    s3_secret_access_key: String,

    /// Skips the Postgres/`MinIO` TCP reachability probe
    /// ([`backend_check`]). The probe's failure message already explains
    /// how to bring the real backends up; this flag exists for a caller
    /// that already knows they are up under a name this runner's plain TCP
    /// probe cannot resolve.
    #[arg(long)]
    skip_backend_check: bool,

    /// How long the backend-reachability probe waits before failing
    /// closed.
    #[arg(long, default_value_t = 5)]
    backend_check_timeout_secs: u64,

    /// `hot.window` for every node — deliberately short (unlike the
    /// production default of 60s) so the boot/drive-load loop's window
    /// actually rolls, and drains, inside `--settle-timeout-secs`.
    #[arg(long, default_value = "5s")]
    hot_window: String,

    /// `drain.allowed_lateness` for every node — deliberately short (unlike
    /// the production default of 15m), same reasoning as `--hot-window`.
    #[arg(long, default_value = "1s")]
    allowed_lateness: String,

    /// How long to wait for each node's `/status` to report `ready: true`.
    #[arg(long, default_value_t = 30)]
    boot_timeout_secs: u64,

    /// OTLP export batches sent to each node during the drive-load pass.
    #[arg(long, default_value_t = 20)]
    load_batches: u32,

    /// Log records per OTLP export batch.
    #[arg(long, default_value_t = 25)]
    load_batch_size: u32,

    /// Delay between batches — keeps the load a sustained trickle (§8.4)
    /// rather than one instantaneous burst.
    #[arg(long, default_value_t = 200)]
    load_interval_ms: u64,

    /// How long, after the drive-load pass, to wait for at least one
    /// node's watermark to advance past empty — the proof that
    /// accept→stage→drain→lake-commit actually closed the loop, not merely
    /// that ingest was accepted.
    #[arg(long, default_value_t = 60)]
    settle_timeout_secs: u64,

    /// Grace period for each node's SIGTERM shutdown before a hard kill.
    #[arg(long, default_value_t = 10)]
    shutdown_grace_secs: u64,
}

/// This runner's own smoke-loop status codes — **not** `duckspout-judge`'s
/// future Pass(0)/Violation(2)/NoVerdict(3) vocabulary (§8.4's vacuity-teeth
/// section); deliberately a disjoint scheme so nobody mistakes one for the
/// other.
const EXIT_OK: i32 = 0;
const EXIT_HARD_FAILURE: i32 = 1;
const EXIT_DRAIN_UNCONFIRMED: i32 = 2;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => std::process::ExitCode::from(u8::try_from(code).unwrap_or(255)),
        Err(error) => {
            tracing::error!("{error:#}");
            std::process::ExitCode::from(u8::try_from(EXIT_HARD_FAILURE).unwrap_or(255))
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<i32> {
    let daemon_bin = resolve_daemon_bin(cli.daemon_bin.as_deref())?;
    tracing::info!(daemon_bin = %daemon_bin.display(), "resolved duckspout-daemon binary");

    let work_dir = cli
        .work_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("duckspout-fleet-{}", cli.seed)));
    std::fs::create_dir_all(&work_dir)
        .with_context(|| format!("creating work dir {}", work_dir.display()))?;
    tracing::info!(work_dir = %work_dir.display(), "fleet work directory");

    if cli.skip_backend_check {
        tracing::warn!("--skip-backend-check set: proceeding without probing Postgres/MinIO");
    } else {
        check_backends(&cli).await?;
    }

    let plan = build_plan(&cli, &work_dir)?;

    let nodes = topology::provision_nodes(
        &work_dir,
        cli.seed,
        cli.nodes,
        cli.otlp_base_port,
        cli.flight_base_port,
        cli.peer_base_port,
        cli.status_base_port,
    )?;
    if nodes.is_empty() {
        bail!("--nodes 0: nothing to boot");
    }
    for node in &nodes {
        let rendered = topology::render_node_config(&plan, node, &nodes);
        std::fs::write(&node.config_path, rendered)
            .with_context(|| format!("writing {}", node.config_path.display()))?;
    }

    let mut running: Vec<process::RunningNode> = Vec::with_capacity(nodes.len());
    let boot_result = boot_fleet(&daemon_bin, &nodes, &cli, &mut running).await;
    if let Err(error) = boot_result {
        shutdown_all(&mut running, Duration::from_secs(cli.shutdown_grace_secs)).await;
        report_work_dir(&work_dir);
        return Err(error);
    }

    if let Some(loadgen_bin) = cli.loadgen_bin.as_deref() {
        spawn_loadgen_best_effort(loadgen_bin, &nodes[0]).await;
    }

    let load_results = drive_load_all(&nodes, &cli).await;
    let all_accepted = load_results.iter().all(load::LoadResult::fully_accepted);
    for (node, result) in nodes.iter().zip(&load_results) {
        tracing::info!(
            node = %node.name,
            attempted = result.batches_attempted,
            accepted = result.batches_accepted,
            records = result.records_accepted,
            "drive-load pass complete"
        );
    }

    let drain_confirmed =
        wait_for_any_watermark(&nodes, Duration::from_secs(cli.settle_timeout_secs)).await;

    shutdown_all(&mut running, Duration::from_secs(cli.shutdown_grace_secs)).await;

    print_summary(&nodes, &load_results, drain_confirmed, &work_dir);
    report_work_dir(&work_dir);

    if !all_accepted {
        bail!("at least one node did not fully accept its drive-load batches (see summary above)");
    }
    if !drain_confirmed {
        tracing::warn!(
            "no node's watermark advanced within --settle-timeout-secs={}s: ingest was accepted \
             but the accept→drain→lake-commit loop was not confirmed closed within the budget",
            cli.settle_timeout_secs
        );
        return Ok(EXIT_DRAIN_UNCONFIRMED);
    }
    Ok(EXIT_OK)
}

/// Probes Postgres and (when not `--local-lake`) `MinIO` before touching a
/// single node (module docs of [`backend_check`]).
async fn check_backends(cli: &Cli) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(cli.backend_check_timeout_secs);
    let (pg_host, pg_port) = backend_check::postgres_host_port(&cli.postgres_dsn)?;
    backend_check::check_reachable("Postgres", &pg_host, pg_port, timeout).await?;
    if !cli.local_lake {
        let (s3_host, s3_port) = backend_check::s3_host_port(&cli.s3_endpoint)?;
        backend_check::check_reachable("MinIO/S3", &s3_host, s3_port, timeout).await?;
    }
    Ok(())
}

/// Writes the shared Postgres-password and (if used) S3-secret files, then
/// builds the [`FleetPlan`] every node's config renders against.
fn build_plan(cli: &Cli, work_dir: &std::path::Path) -> anyhow::Result<FleetPlan> {
    let postgres_password_file = work_dir.join("postgres-password");
    std::fs::write(&postgres_password_file, &cli.postgres_password)
        .with_context(|| format!("writing {}", postgres_password_file.display()))?;

    let lake = if cli.local_lake {
        LakeStorage::Local {
            dir: work_dir.join("lake"),
        }
    } else {
        let secret_file = work_dir.join("s3-secret");
        std::fs::write(&secret_file, &cli.s3_secret_access_key)
            .with_context(|| format!("writing {}", secret_file.display()))?;
        LakeStorage::S3 {
            endpoint: cli.s3_endpoint.clone(),
            bucket: cli.s3_bucket.clone(),
            prefix: cli.s3_prefix.clone(),
            region: cli.s3_region.clone(),
            access_key_id: cli.s3_access_key_id.clone(),
            secret_access_key_file: secret_file,
        }
    };

    Ok(FleetPlan {
        postgres_dsn: cli.postgres_dsn.clone(),
        postgres_password_file,
        lake,
        rf: cli.rf,
        hot_window: cli.hot_window.clone(),
        allowed_lateness: cli.allowed_lateness.clone(),
    })
}

/// Spawns every node and waits for all of them to report ready, leaving
/// whatever subset already started in `running` even on failure — the
/// caller is responsible for shutting those down.
async fn boot_fleet(
    daemon_bin: &std::path::Path,
    nodes: &[NodeSpec],
    cli: &Cli,
    running: &mut Vec<process::RunningNode>,
) -> anyhow::Result<()> {
    for node in nodes {
        running.push(process::spawn_node(daemon_bin, node)?);
    }
    let timeout = Duration::from_secs(cli.boot_timeout_secs);
    for node in running.iter_mut() {
        process::wait_until_ready(node, timeout).await?;
        tracing::info!(node = %node.spec.name, "node ready");
    }
    Ok(())
}

/// Drives the placeholder smoke load against every node concurrently
/// (module docs of [`load`]).
async fn drive_load_all(nodes: &[NodeSpec], cli: &Cli) -> Vec<load::LoadResult> {
    let interval = Duration::from_millis(cli.load_interval_ms);
    let futures = nodes.iter().map(|node| {
        let addr = node.otlp_addr();
        let label = node.name.clone();
        async move {
            load::drive_load(
                &addr,
                &label,
                cli.load_batches,
                cli.load_batch_size,
                interval,
            )
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(node = %label, %error, "drive-load could not even connect");
                load::LoadResult::default()
            })
        }
    });
    futures::future::join_all(futures).await
}

/// Polls every node's `/status` until at least one reports a non-empty
/// `watermarks` array, or `timeout` elapses — the proof the drain loop
/// actually committed a window, not only that ingest was accepted.
async fn wait_for_any_watermark(nodes: &[NodeSpec], timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for node in nodes {
            if let Ok(snapshot) = process::fetch_status(node.status_addr()).await
                && snapshot
                    .get("watermarks")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|rows| !rows.is_empty())
            {
                return true;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Best-effort loadgen spawn (§8.4: "the load generator is a first-class
/// fleet member" — CLI seam only, module docs of [`main`]). Its current
/// stub exits 2 immediately; that is logged, never treated as a fleet
/// failure.
async fn spawn_loadgen_best_effort(loadgen_bin: &std::path::Path, target_node: &NodeSpec) {
    let target = target_node.otlp_addr();
    let output = tokio::process::Command::new(loadgen_bin)
        .arg("--node-id")
        .arg("fleet-loadgen-0")
        .arg("--target")
        .arg(&target)
        .output()
        .await;
    match output {
        Ok(output) if output.status.success() => {
            tracing::info!("duckspout-loadgen ran to completion");
        }
        Ok(output) => {
            tracing::info!(
                status = %output.status,
                stderr = %String::from_utf8_lossy(&output.stderr),
                "duckspout-loadgen exited non-zero — expected while it is still a stub \
                 (issue #202); not treated as a fleet failure"
            );
        }
        Err(error) => {
            tracing::warn!(%error, "could not spawn duckspout-loadgen (--loadgen-bin path wrong?)");
        }
    }
}

async fn shutdown_all(running: &mut [process::RunningNode], grace: Duration) {
    let futures = running
        .iter_mut()
        .map(|node| process::shutdown(node, grace));
    futures::future::join_all(futures).await;
}

fn print_summary(
    nodes: &[NodeSpec],
    load_results: &[load::LoadResult],
    drain_confirmed: bool,
    work_dir: &std::path::Path,
) {
    println!("\nduckspout-fleet smoke-loop summary");
    println!("==================================");
    for (node, result) in nodes.iter().zip(load_results) {
        println!(
            "  node {:<16} otlp=:{:<6} status=:{:<6} load {}/{} batches accepted ({} records) \
             journal={}",
            node.name,
            node.otlp_port,
            node.status_port,
            result.batches_accepted,
            result.batches_attempted,
            result.records_accepted,
            node.journal_path.display(),
        );
    }
    println!(
        "  drain confirmed (any node's watermark advanced): {}",
        if drain_confirmed { "yes" } else { "NO" }
    );
    println!("  work dir: {}", work_dir.display());
}

fn report_work_dir(work_dir: &std::path::Path) {
    tracing::info!(
        work_dir = %work_dir.display(),
        "fleet artifacts (configs, journals, stdout/stderr logs) left on disk"
    );
}

/// Resolves the `duckspout-daemon` binary: an explicit `--daemon-bin`, else
/// a sibling of this process's own executable (the `cargo build` /
/// `cargo install` layout, where every workspace bin lands in the same
/// `target/<profile>/` directory), else the first `duckspout-daemon` found
/// on `PATH`.
///
/// # Errors
///
/// If none of the three resolve to an existing file.
fn resolve_daemon_bin(explicit: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        if path.is_file() {
            return Ok(path.to_owned());
        }
        bail!("--daemon-bin {} does not exist", path.display());
    }
    let exe_name = if cfg!(windows) {
        "duckspout-daemon.exe"
    } else {
        "duckspout-daemon"
    };
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(dir) = current_exe.parent()
    {
        let sibling = dir.join(exe_name);
        if sibling.is_file() {
            return Ok(sibling);
        }
    }
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join(exe_name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    bail!(
        "could not find the duckspout-daemon binary next to this executable or on PATH; \
         build it first (`cargo build -p duckspout-daemon`) or pass --daemon-bin explicitly"
    )
}
