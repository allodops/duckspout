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
mod fault;
mod faultlog;
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

    // --- Fault injection (§8.4, issue #203) ---
    /// Node index (0-based) to `SIGKILL` as a real fault. Absent by
    /// default: no node-kill fault runs.
    #[arg(long)]
    fault_kill_node: Option<u16>,

    /// Times `--fault-kill-node`'s kill to land inside the real
    /// `PutPart`→`LakeCommit` window — §8.4's sharpest fault ("the
    /// partition owner mid-drain") — rather than firing after a plain
    /// delay. Requires `--fault-kill-node`; boots that one node with
    /// `--fault-drain-commit-delay-ms` set to
    /// `--fault-kill-drain-stall-ms` so the window is wide enough to hit
    /// deterministically (`fault`'s module docs). Enforced by clap
    /// (`requires`), not merely documented (R-3/vacuity-avoidance,
    /// `AGENTS.md`) — an ACPR finding: this flag without
    /// `--fault-kill-node` used to silently arm nothing.
    #[arg(long, requires = "fault_kill_node")]
    fault_kill_mid_drain: bool,

    /// Delay before firing `--fault-kill-node`'s kill, when
    /// `--fault-kill-mid-drain` is NOT set.
    #[arg(long, default_value_t = 5)]
    fault_kill_delay_secs: u64,

    /// The target node's `--fault-drain-commit-delay-ms` stall (only
    /// applied to that one node's boot), used only when
    /// `--fault-kill-mid-drain` is set.
    #[arg(long, default_value_t = 3_000)]
    fault_kill_drain_stall_ms: u64,

    /// How long `--fault-kill-mid-drain`'s journal watch waits for a
    /// `PutPart` line before giving up (e.g. a drive-load pass that never
    /// produces a drainable window).
    #[arg(long, default_value_t = 60)]
    fault_kill_mid_drain_timeout_secs: u64,

    /// Node index (0-based) to `SIGSTOP`/`SIGCONT` as a real pause fault
    /// (§8.4's `FencedZombie` fault). Absent by default: no SIGSTOP fault
    /// runs.
    #[arg(long)]
    fault_sigstop_node: Option<u16>,

    /// Delay before sending `SIGSTOP` to `--fault-sigstop-node`.
    #[arg(long, default_value_t = 5)]
    fault_sigstop_delay_secs: u64,

    /// How long to hold `--fault-sigstop-node` paused before `SIGCONT` —
    /// the default comfortably exceeds
    /// `duckspout_daemon::constants::HEARTBEAT_TTL_SECS`, so the pause is
    /// "long enough to expire claims" per §8.4's own wording.
    #[arg(long, default_value_t = duckspout_daemon::constants::HEARTBEAT_TTL_SECS + 5)]
    fault_sigstop_duration_secs: u64,
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

    // §8.4/issue #203: whichever `--fault-*` faults were armed run
    // CONCURRENTLY with the drive-load/settle passes below — a fault fired
    // only after the smoke loop already finished would prove nothing about
    // the system under load. `fault_log` and `running` are separate
    // bindings from `nodes` (module docs of `run_armed_faults`), so this
    // borrows nothing the drive-load/settle futures also touch.
    let fault_log = faultlog::FaultLog::create(&work_dir.join("faults.ndjson"))
        .with_context(|| format!("creating {}", work_dir.join("faults.ndjson").display()))?;
    let (fault_result, load_results, drain_confirmed) = tokio::join!(
        run_armed_faults(&cli, &mut running, &fault_log),
        drive_load_all(&nodes, &cli),
        wait_for_any_watermark(&nodes, Duration::from_secs(cli.settle_timeout_secs)),
    );
    if let Err(error) = fault_result {
        tracing::error!(%error, "fault injection failed");
        shutdown_all(&mut running, Duration::from_secs(cli.shutdown_grace_secs)).await;
        report_work_dir(&work_dir);
        return Err(error);
    }
    let all_accepted = all_batches_accepted(&cli, &load_results);
    for (node, result) in nodes.iter().zip(&load_results) {
        tracing::info!(
            node = %node.name,
            attempted = result.batches_attempted,
            accepted = result.batches_accepted,
            records = result.records_accepted,
            "drive-load pass complete"
        );
    }

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

/// Whether every node's drive-load pass counts as fully accepted for
/// `run()`'s own exit-code purposes.
///
/// §8.4's own design premise (quoted in this crate's module docs): the
/// fleet must be free to misbehave during the run and still be convicted
/// precisely AFTERWARD by a separate judge (`duckspout-judge`, #205–#208),
/// never by this runner's own exit code. A node named by `cli.fault_kill_node`
/// is exempted from the "every batch accepted" check: by the time `run()`
/// calls this, `run_armed_faults`'s own `Err` has already short-circuited
/// the whole function (module docs of [`run`]), so a still-configured
/// `--fault-kill-node` at this point means that node was successfully,
/// intentionally killed (`fault::run_node_kill` only returns `Ok` once it
/// confirmed a real exit) — its own necessarily-incomplete batch acceptance
/// is the fault working as scheduled, not a fleet-run failure. An ACPR
/// finding: the pre-fix check counted a successful, scheduled kill as an
/// `all_accepted` failure, causing `duckspout-fleet` to `bail!` on exactly
/// the outcome it was told to produce.
fn all_batches_accepted(cli: &Cli, load_results: &[load::LoadResult]) -> bool {
    load_results.iter().enumerate().all(|(index, result)| {
        let intentionally_killed =
            u16::try_from(index).is_ok_and(|index| cli.fault_kill_node == Some(index));
        intentionally_killed || result.fully_accepted()
    })
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
///
/// The node named by `--fault-kill-node`, when `--fault-kill-mid-drain` is
/// also set, is booted with `--fault-drain-commit-delay-ms` set to
/// `--fault-kill-drain-stall-ms` (`fault`'s module docs on why: it is what
/// makes that node's `PutPart`→`LakeCommit` window wide enough for a
/// scheduled kill to land inside it deterministically) — every other node
/// boots exactly as it did before this issue.
async fn boot_fleet(
    daemon_bin: &std::path::Path,
    nodes: &[NodeSpec],
    cli: &Cli,
    running: &mut Vec<process::RunningNode>,
) -> anyhow::Result<()> {
    for node in nodes {
        let fault_drain_commit_delay_ms = (cli.fault_kill_mid_drain
            && cli.fault_kill_node == Some(node.index))
        .then_some(cli.fault_kill_drain_stall_ms);
        running.push(process::spawn_node(
            daemon_bin,
            node,
            fault_drain_commit_delay_ms,
        )?);
    }
    let timeout = Duration::from_secs(cli.boot_timeout_secs);
    for node in running.iter_mut() {
        process::wait_until_ready(node, timeout).await?;
        tracing::info!(node = %node.spec.name, "node ready");
    }
    Ok(())
}

/// Runs whichever faults `cli` armed (§8.4, issue #203), sequentially
/// against `running` (module docs of [`fault`] for why this need not be
/// concurrent-safe across faults: at most one of each kind runs per fleet
/// invocation today, and both take `&mut running[..]` by index, which
/// `tokio::join!`ing this future alongside the unrelated drive-load/settle
/// futures in [`run`] already runs them concurrent with the REST of the
/// smoke loop). A fault whose target index is out of range is a plain
/// `--fault-*-node` misconfiguration, reported as an error rather than
/// silently skipped (R-3).
async fn run_armed_faults(
    cli: &Cli,
    running: &mut [process::RunningNode],
    log: &faultlog::FaultLog,
) -> anyhow::Result<()> {
    if let Some(index) = cli.fault_kill_node {
        let target = running
            .get_mut(index as usize)
            .ok_or_else(|| anyhow::anyhow!("--fault-kill-node {index}: no such node"))?;
        let timing = if cli.fault_kill_mid_drain {
            fault::KillTiming::MidDrainCommit {
                journal_poll_timeout: Duration::from_secs(cli.fault_kill_mid_drain_timeout_secs),
            }
        } else {
            fault::KillTiming::AfterDelay(Duration::from_secs(cli.fault_kill_delay_secs))
        };
        fault::run_node_kill("node-kill-0", target, timing, log).await?;
    }
    if let Some(index) = cli.fault_sigstop_node {
        let target = running
            .get_mut(index as usize)
            .ok_or_else(|| anyhow::anyhow!("--fault-sigstop-node {index}: no such node"))?;
        fault::run_sigstop_pause(
            "sigstop-pause-0",
            target,
            Duration::from_secs(cli.fault_sigstop_delay_secs),
            Duration::from_secs(cli.fault_sigstop_duration_secs),
            log,
        )
        .await?;
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
    println!(
        "  fault-window journal: {} (§8.4, issue #203)",
        work_dir.join("faults.ndjson").display()
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal `Cli` with every field explicit — `Cli::parse_from` isn't
    /// used here since these tests exercise `build_plan`/`resolve_daemon_bin`
    /// directly, not argument parsing itself.
    fn base_cli(work_dir_seed: &str) -> Cli {
        Cli {
            seed: 0,
            nodes: 3,
            work_dir: None,
            daemon_bin: None,
            loadgen_bin: None,
            rf: 2,
            otlp_base_port: 14317,
            flight_base_port: 18815,
            peer_base_port: 17946,
            status_base_port: 19095,
            postgres_dsn: "postgres://duckspout@127.0.0.1:5432/duckspout_catalog".to_owned(),
            postgres_password: "duckspout-dev".to_owned(),
            local_lake: true,
            s3_endpoint: "127.0.0.1:9000".to_owned(),
            s3_bucket: "duckspout-fleet".to_owned(),
            s3_prefix: format!("duckspout-fleet-{work_dir_seed}"),
            s3_region: "us-east-1".to_owned(),
            s3_access_key_id: "duckspout".to_owned(),
            s3_secret_access_key: "duckspout-dev".to_owned(),
            skip_backend_check: true,
            backend_check_timeout_secs: 5,
            hot_window: "5s".to_owned(),
            allowed_lateness: "1s".to_owned(),
            boot_timeout_secs: 30,
            load_batches: 20,
            load_batch_size: 25,
            load_interval_ms: 200,
            settle_timeout_secs: 60,
            shutdown_grace_secs: 10,
            fault_kill_node: None,
            fault_kill_mid_drain: false,
            fault_kill_delay_secs: 5,
            fault_kill_drain_stall_ms: 3_000,
            fault_kill_mid_drain_timeout_secs: 60,
            fault_sigstop_node: None,
            fault_sigstop_delay_secs: 5,
            fault_sigstop_duration_secs: duckspout_daemon::constants::HEARTBEAT_TTL_SECS + 5,
        }
    }

    fn scratch_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-main-test-{}-{label}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn build_plan_writes_the_postgres_password_file_and_picks_local_lake() {
        let work_dir = scratch_dir("local-lake");
        let cli = base_cli("local");
        let plan = build_plan(&cli, &work_dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(&plan.postgres_password_file).unwrap(),
            "duckspout-dev"
        );
        match plan.lake {
            LakeStorage::Local { dir } => assert_eq!(dir, work_dir.join("lake")),
            LakeStorage::S3 { .. } => panic!("--local-lake must select LakeStorage::Local"),
        }
        assert_eq!(plan.rf, cli.rf);
        assert_eq!(plan.hot_window, cli.hot_window);
        assert_eq!(plan.allowed_lateness, cli.allowed_lateness);
    }

    #[test]
    fn build_plan_writes_the_s3_secret_file_and_picks_s3_lake_when_not_local() {
        let work_dir = scratch_dir("s3-lake");
        let mut cli = base_cli("s3");
        cli.local_lake = false;
        let plan = build_plan(&cli, &work_dir).unwrap();

        match plan.lake {
            LakeStorage::S3 {
                endpoint,
                bucket,
                prefix,
                secret_access_key_file,
                ..
            } => {
                assert_eq!(endpoint, cli.s3_endpoint);
                assert_eq!(bucket, cli.s3_bucket);
                assert_eq!(prefix, cli.s3_prefix);
                assert_eq!(
                    std::fs::read_to_string(&secret_access_key_file).unwrap(),
                    cli.s3_secret_access_key
                );
            }
            LakeStorage::Local { .. } => panic!("non---local-lake must select LakeStorage::S3"),
        }
    }

    #[test]
    fn resolve_daemon_bin_accepts_an_existing_explicit_path() {
        let dir = scratch_dir("daemon-bin-explicit");
        let bin = dir.join("duckspout-daemon");
        std::fs::write(&bin, b"").unwrap();
        assert_eq!(resolve_daemon_bin(Some(&bin)).unwrap(), bin);
    }

    #[test]
    fn resolve_daemon_bin_rejects_a_missing_explicit_path() {
        let dir = scratch_dir("daemon-bin-missing");
        let bin = dir.join("does-not-exist");
        assert!(resolve_daemon_bin(Some(&bin)).is_err());
    }

    /// An ACPR finding (LOW-MEDIUM-7): `--fault-kill-mid-drain` without
    /// `--fault-kill-node` used to silently arm nothing, despite its own
    /// doc comment claiming "Requires `--fault-kill-node`" — clap's
    /// `requires` on the field must turn this into a reported CLI error,
    /// not a silent vacuous no-op (R-3, `AGENTS.md`).
    #[test]
    fn fault_kill_mid_drain_without_fault_kill_node_is_a_clap_error() {
        let result = Cli::try_parse_from([
            "duckspout-fleet",
            "--fault-kill-mid-drain",
            "--skip-backend-check",
        ]);
        assert!(
            result.is_err(),
            "--fault-kill-mid-drain without --fault-kill-node must be rejected by clap"
        );
    }

    /// The same flag combination, but WITH `--fault-kill-node`, must parse
    /// fine — `requires` must not have accidentally banned the legitimate
    /// combination it is meant to allow.
    #[test]
    fn fault_kill_mid_drain_with_fault_kill_node_parses() {
        let result = Cli::try_parse_from([
            "duckspout-fleet",
            "--fault-kill-node",
            "0",
            "--fault-kill-mid-drain",
            "--skip-backend-check",
        ]);
        assert!(
            result.is_ok(),
            "--fault-kill-mid-drain with --fault-kill-node must parse: {result:?}"
        );
    }

    fn load_result(attempted: u32, accepted: u32) -> load::LoadResult {
        load::LoadResult {
            batches_attempted: attempted,
            batches_accepted: accepted,
            records_accepted: 0,
        }
    }

    /// The MEDIUM-6 ACPR finding's baseline: with no kill fault configured,
    /// a node that did not fully accept its batches must still fail the
    /// check — this fix must not accidentally turn `all_batches_accepted`
    /// into an always-true rubber stamp.
    #[test]
    fn all_batches_accepted_is_false_for_an_unexplained_partial_acceptance() {
        let cli = base_cli("no-fault");
        let results = vec![load_result(10, 10), load_result(10, 7)];
        assert!(!all_batches_accepted(&cli, &results));
    }

    /// The MEDIUM-6 fix itself: a node named by `--fault-kill-node` is
    /// exempted from the full-acceptance check — a scheduled, successful
    /// kill must not fail `run()`'s own exit code (§8.4: the judge, not the
    /// runner, convicts misbehavior).
    #[test]
    fn all_batches_accepted_exempts_the_intentionally_killed_node() {
        let mut cli = base_cli("kill-exempt");
        cli.fault_kill_node = Some(1);
        let results = vec![load_result(10, 10), load_result(10, 3)];
        assert!(
            all_batches_accepted(&cli, &results),
            "the node named by --fault-kill-node must be exempted from the check"
        );
    }

    /// The exemption is scoped to exactly the named node index — a
    /// DIFFERENT node's own unexplained partial acceptance must still fail
    /// the check even while a kill fault is configured elsewhere.
    #[test]
    fn all_batches_accepted_does_not_exempt_a_different_node() {
        let mut cli = base_cli("kill-elsewhere");
        cli.fault_kill_node = Some(0);
        let results = vec![load_result(10, 10), load_result(10, 3)];
        assert!(
            !all_batches_accepted(&cli, &results),
            "only the node named by --fault-kill-node may be exempted"
        );
    }

    /// A minimal [`NodeSpec`] for the offline (no real daemon) tests below —
    /// only `status_port`/`otlp_port` are ever dialed by the functions under
    /// test here.
    fn test_node_spec(name: &str, status_port: u16, otlp_port: u16) -> NodeSpec {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-main-test-{}-{name}",
            std::process::id()
        ));
        NodeSpec {
            index: 0,
            name: name.to_owned(),
            otlp_port,
            flight_port: 0,
            peer_port: 0,
            status_port,
            data_dir: dir.join("data"),
            config_path: dir.join("config.toml"),
            journal_path: dir.join("journal.ndjson"),
            stdout_path: dir.join("stdout.log"),
            stderr_path: dir.join("stderr.log"),
        }
    }

    /// Binds a real listener that answers every `/status` request with
    /// `body` and returns its port — a stand-in `duckspout-daemon` for
    /// `wait_for_any_watermark`'s own tests below, matching
    /// `process::tests::fetch_status_parses_a_real_http_response_body`'s
    /// own wire-shape convention.
    async fn spawn_fake_status_server(body: &'static str) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
                    let mut buf = [0_u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let _ = stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{body}"
                            )
                            .as_bytes(),
                        )
                        .await;
                });
            }
        });
        port
    }

    /// `wait_for_any_watermark` returns `true` as soon as ANY node's
    /// `/status` reports a non-empty `watermarks` array — the proof the
    /// accept→drain→lake-commit loop actually closed, `run()`'s own reason
    /// for calling this (module docs of [`wait_for_any_watermark`]).
    #[tokio::test]
    async fn wait_for_any_watermark_returns_true_once_a_node_reports_one() {
        let port =
            spawn_fake_status_server(r#"{"ready":true,"watermarks":[{"partition":"p0"}]}"#).await;
        let node = test_node_spec("watermark-yes", port, 0);
        assert!(
            wait_for_any_watermark(&[node], Duration::from_secs(2)).await,
            "a node reporting a non-empty watermarks array must resolve true"
        );
    }

    /// The other side of the same check: every node reporting an EMPTY
    /// `watermarks` array must time out to `false`, not hang or
    /// false-positive on the array's mere presence.
    #[tokio::test]
    async fn wait_for_any_watermark_returns_false_when_none_ever_advances() {
        let port = spawn_fake_status_server(r#"{"ready":true,"watermarks":[]}"#).await;
        let node = test_node_spec("watermark-no", port, 0);
        assert!(
            !wait_for_any_watermark(&[node], Duration::from_millis(300)).await,
            "an always-empty watermarks array must never resolve true"
        );
    }

    /// `spawn_loadgen_best_effort`'s own contract (module docs): a
    /// successfully-run loadgen is logged, never propagated as a failure —
    /// there is nothing to assert on besides "this never panics regardless
    /// of outcome," which IS the actual behavioral contract for a function
    /// with no return value whose whole job is best-effort logging.
    #[tokio::test]
    async fn spawn_loadgen_best_effort_never_panics_on_a_successful_exit() {
        let node = test_node_spec("loadgen-ok", 0, 0);
        spawn_loadgen_best_effort(std::path::Path::new("/bin/true"), &node).await;
    }

    /// The non-zero-exit arm (the loadgen stub's current documented
    /// behavior, issue #202) must also be swallowed, not propagated.
    #[tokio::test]
    async fn spawn_loadgen_best_effort_never_panics_on_a_nonzero_exit() {
        let node = test_node_spec("loadgen-nonzero", 0, 0);
        spawn_loadgen_best_effort(std::path::Path::new("/bin/false"), &node).await;
    }

    /// The spawn-itself-failed arm (a wrong `--loadgen-bin` path) must also
    /// be swallowed, not propagated or panicked on.
    #[tokio::test]
    async fn spawn_loadgen_best_effort_never_panics_when_the_binary_does_not_exist() {
        let node = test_node_spec("loadgen-missing", 0, 0);
        spawn_loadgen_best_effort(
            std::path::Path::new("/no/such/duckspout-loadgen-binary"),
            &node,
        )
        .await;
    }

    /// `check_backends` succeeds once both a real Postgres-shaped listener
    /// and a real S3-shaped listener are reachable — the plain TCP-only
    /// reachability probe `main.rs`'s own module docs describe, proven here
    /// against real (fake) listeners rather than only unit-testing
    /// `backend_check::check_reachable` in isolation.
    #[tokio::test]
    async fn check_backends_succeeds_when_both_backends_are_reachable() {
        let pg_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pg_port = pg_listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                if pg_listener.accept().await.is_err() {
                    return;
                }
            }
        });
        let s3_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s3_port = s3_listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                if s3_listener.accept().await.is_err() {
                    return;
                }
            }
        });

        let mut cli = base_cli("check-backends-ok");
        cli.postgres_dsn = format!("postgres://duckspout@127.0.0.1:{pg_port}/duckspout_catalog");
        cli.local_lake = false;
        cli.s3_endpoint = format!("127.0.0.1:{s3_port}");
        cli.backend_check_timeout_secs = 2;

        assert!(check_backends(&cli).await.is_ok());
    }

    /// `check_backends` fails closed when Postgres is unreachable — proven
    /// against the SAME well-known, never-listening port
    /// `backend_check::tests::check_reachable_fails_closed_against_a_closed_port`
    /// already uses, rather than inventing a second convention for "a port
    /// nothing binds to."
    #[tokio::test]
    async fn check_backends_fails_closed_when_postgres_is_unreachable() {
        let mut cli = base_cli("check-backends-fail");
        cli.postgres_dsn = "postgres://duckspout@127.0.0.1:1/duckspout_catalog".to_owned();
        cli.backend_check_timeout_secs = 1;
        assert!(check_backends(&cli).await.is_err());
    }

    /// With no `--daemon-bin` given, this test binary's own directory
    /// (`target/.../deps/`) never contains a sibling `duckspout-daemon`, and
    /// a real PATH scan of a clean CI runner finds none either — exercises
    /// both the sibling-of-current-exe check and the real PATH scan, both
    /// falling through to the documented error rather than panicking.
    /// (A developer machine with `duckspout-daemon` already on `PATH` from
    /// a prior `cargo install` could make this find one — an accepted,
    /// disclosed limitation of testing a PATH scan without mutating the
    /// process-global `PATH` env var, which this test binary shares with
    /// `process::tests`' own PATH-dependent subprocess tests and must not
    /// disturb.)
    #[test]
    fn resolve_daemon_bin_with_no_explicit_path_falls_through_to_a_reported_error() {
        let result = resolve_daemon_bin(None);
        assert!(
            result.is_err(),
            "expected no duckspout-daemon binary next to the test binary or on PATH, got {result:?}"
        );
    }

    /// `print_summary` and `report_work_dir` are pure disclosure (stdout
    /// `println!`/`tracing::info!`) with no return value — exercising both
    /// branches of `drain_confirmed` and a multi-node summary is the only
    /// thing left to verify: that formatting a real `LoadResult` set never
    /// panics (e.g. on a `nodes`/`load_results` length mismatch the `zip`
    /// would otherwise silently truncate rather than crash on, but a
    /// mismatched summary would itself be a bug worth a loud failure in
    /// this test if one were ever introduced).
    #[test]
    fn print_summary_and_report_work_dir_do_not_panic_for_a_typical_run() {
        let nodes = vec![
            test_node_spec("summary-0", 9095, 4317),
            test_node_spec("summary-1", 9096, 4318),
        ];
        let results = vec![load_result(10, 10), load_result(10, 3)];
        let work_dir = scratch_dir("print-summary");
        print_summary(&nodes, &results, true, &work_dir);
        print_summary(&nodes, &results, false, &work_dir);
        report_work_dir(&work_dir);
    }
}
