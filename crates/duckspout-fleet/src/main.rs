//! `duckspout-fleet` — the CTK distributed-run driver (§8.4, D-5).
//!
//! Issue #201's foundational slice: **node provisioning**, **real
//! `MinIO`/Postgres wiring**, and a **boot/drive-load smoke loop**; issues
//! #203 and #204 added **fault injection** on top of it ([`fault`] for the
//! injectors, [`link`] for the real network faults' mechanism,
//! [`faultlog`] for the `faults.ndjson` window journal). Still absent: the
//! judge (#205–#208) and the real `duckspout-loadgen` internals (#202).
//! Those follow-ons plug into the seams this binary establishes:
//! [`topology`] provisions real
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
mod dsn;
mod fault;
mod faultlog;
mod link;
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
// clippy::struct_excessive_bools: this is a CLI argument struct, one field
// per flag — `--local-lake`, `--skip-backend-check`,
// `--fault-kill-mid-drain`, `--fault-churn-join`. The lint's own suggested
// remedy (group the bools into a state enum) would mean the flag surface no
// longer maps one-to-one onto the struct clap derives from, which is worse,
// not better, for a flag set this flat.
#[allow(clippy::struct_excessive_bools)]
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

    /// The `X-Scope-OrgID` this run's drive load is sent under (§2.2's real
    /// multi-tenant admission header). Absent by default: no header, which
    /// is single-tenant mode's `anonymous` tenant — every fleet run's
    /// behaviour before this flag existed.
    ///
    /// Why a fleet run may want its own: a tenant is what a partition is
    /// keyed by, and a **persistent** catalog outlives any one run. A run
    /// whose node processes are torn down with staged-but-undrained data
    /// leaves that partition with a real coverage hole in the catalog — the
    /// daemon is right to stall its watermark there afterwards (a wiped hot
    /// store IS lost data), but it also means a LATER run against the same
    /// partition inherits the stall and never drains anything of its own.
    /// Giving an independent workload its own tenant gives it its own
    /// partition, which is exactly how a real multi-tenant fleet keeps one
    /// workload's damage out of another's. `tests/fault_injection.rs`
    /// relies on this.
    #[arg(long)]
    tenant: Option<String>,

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

    // --- Fault injection II (§8.4, issue #204) ---
    /// Node index (0-based) to cut off from the network: every real link
    /// the fleet gave it — its ingest link, its catalog link, and (unless
    /// `--local-lake`) its lake link — is dropped for the window
    /// (`fault::run_network_partition`; `link`'s module docs for the
    /// mechanism). Absent by default.
    #[arg(long)]
    fault_partition_node: Option<u16>,

    /// Delay before the partition starts.
    #[arg(long, default_value_t = 5)]
    fault_partition_delay_secs: u64,

    /// How long the partition holds before every link is restored.
    #[arg(long, default_value_t = 10)]
    fault_partition_duration_secs: u64,

    /// Node index (0-based) whose real ingest link is DEGRADED rather than
    /// cut — §8.4's "asymmetric degradation": the request direction is
    /// delayed by `--fault-degrade-request-delay-ms` while the response
    /// direction is capped at `--fault-degrade-response-bytes-per-sec`.
    /// Absent by default.
    #[arg(long)]
    fault_degrade_node: Option<u16>,

    /// Delay before the degradation starts.
    #[arg(long, default_value_t = 5)]
    fault_degrade_delay_secs: u64,

    /// How long the degradation holds before the link is restored.
    #[arg(long, default_value_t = 10)]
    fault_degrade_duration_secs: u64,

    /// Client→node added latency, per forwarded chunk.
    #[arg(long, default_value_t = 250)]
    fault_degrade_request_delay_ms: u64,

    /// Node→client bandwidth cap, bytes per second. The asymmetry is the
    /// point: this direction is capped, the other is delayed.
    #[arg(long, default_value_t = 16 * 1024)]
    fault_degrade_response_bytes_per_sec: u64,

    /// Node index (0-based) that LEAVES the fleet gracefully under load —
    /// a real `SIGTERM` and the daemon's own §9.1.2 shutdown, not a crash
    /// (§8.4: "join and leave under load, not only crash"). Absent by
    /// default.
    #[arg(long)]
    fault_churn_leave_node: Option<u16>,

    /// Delay before the leaving node is asked to shut down.
    #[arg(long, default_value_t = 5)]
    fault_churn_leave_delay_secs: u64,

    /// Boot one EXTRA node mid-run, under load (§8.4's membership-churn
    /// join). The extra node is provisioned alongside the rest — so every
    /// node's `cluster.seed_peers` names it from the start — but is not
    /// booted with them; this fault is what starts it.
    #[arg(long)]
    fault_churn_join: bool,

    /// Delay before the joining node is spawned.
    #[arg(long, default_value_t = 5)]
    fault_churn_join_delay_secs: u64,

    /// Node index (0-based) whose Flight server is `SIGKILL`ed while a real
    /// hot query's stream is in flight (§8.4; `fault::run_flight_kill_mid_stream`
    /// for why this lands mid-stream deterministically). Absent by default.
    #[arg(long)]
    fault_flight_kill_node: Option<u16>,

    /// Delay before the Flight query is opened (and the kill follows as
    /// soon as its first message arrives).
    #[arg(long, default_value_t = 5)]
    fault_flight_kill_delay_secs: u64,

    /// The SQL the killed stream is serving. The default is sized so its
    /// encoded result comfortably exceeds HTTP/2's default 64 KiB
    /// flow-control window, which is what makes the kill provably land
    /// mid-stream rather than racing a result that already finished
    /// (`fault::run_flight_kill_mid_stream`'s module docs). It is a real
    /// ticket through the real §7.8-guarded read path on a real dedicated
    /// read connection; a caller wanting the stream to carry real ingested
    /// rows instead can pass any `SELECT`/`WITH` the hot read surface
    /// accepts — as long as it is comfortably bigger than that window, or
    /// the fault degrades to a `clean_end_of_stream` outcome that proves
    /// nothing.
    #[arg(long, default_value = FLIGHT_KILL_DEFAULT_QUERY)]
    fault_flight_kill_query: String,

    /// How long to wait for the Flight stream's first message before
    /// giving up (a stream that never starts makes the kill vacuous).
    #[arg(long, default_value_t = 30)]
    fault_flight_kill_first_message_timeout_secs: u64,

    /// Node index (0-based) whose real link to the Postgres catalog is cut
    /// for a window (§8.4: "ingest must continue undegraded; drains stall
    /// and disclose"). Absent by default.
    #[arg(long)]
    fault_catalog_outage_node: Option<u16>,

    /// Delay before the catalog outage starts.
    #[arg(long, default_value_t = 5)]
    fault_catalog_outage_delay_secs: u64,

    /// How long the catalog stays unreachable.
    #[arg(long, default_value_t = 10)]
    fault_catalog_outage_duration_secs: u64,

    /// Node index (0-based) whose catalog link OSCILLATES up and down —
    /// §8.4's discovery flapping (`fault::run_discovery_flap`'s own docs
    /// for exactly which half of that predicate is observable today).
    /// Absent by default.
    #[arg(long)]
    fault_discovery_flap_node: Option<u16>,

    /// Delay before the flapping starts.
    #[arg(long, default_value_t = 5)]
    fault_discovery_flap_delay_secs: u64,

    /// How many down/up cycles to run.
    #[arg(long, default_value_t = 5)]
    fault_discovery_flap_cycles: u32,

    /// How long each cycle holds the catalog link down.
    #[arg(long, default_value_t = 1_000)]
    fault_discovery_flap_down_ms: u64,

    /// How long each cycle leaves it up again before the next cycle.
    #[arg(long, default_value_t = 1_000)]
    fault_discovery_flap_up_ms: u64,

    // --- Cache/residency churn (§8.4, issue #207) ---
    /// Node index (0-based) whose post-drain hot residency is CHURNED while
    /// real Arrow Flight reads run through it (§8.4's "forced Evict/Demote
    /// churn and `DropWindow` racing queries"). Boots that one node with
    /// `--fault-cache-churn-hot-window` as its `hot.window`, so under the
    /// drive load its windows seal, drain and `DropWindow` densely instead
    /// of once a minute. Absent by default.
    ///
    /// `fault::run_cache_churn`'s own docs carry the mechanism, and the
    /// disclosure of which third of the Evict/Demote/`DropWindow` vocabulary
    /// can actually fire at v0.2 (only `DropWindow`: v1's cache class is
    /// empty by construction, `docs/design/data-model.md` §2.4).
    #[arg(long)]
    fault_cache_churn_node: Option<u16>,

    /// Delay before the churn window opens.
    #[arg(long, default_value_t = 5)]
    fault_cache_churn_delay_secs: u64,

    /// How long the churn window holds — how long reads keep racing the
    /// residency actions. Long enough by default for several windows to
    /// seal and drain at the shortened `hot.window` below.
    #[arg(long, default_value_t = 20)]
    fault_cache_churn_duration_secs: u64,

    /// The `hot.window` the churn target is booted with, replacing
    /// `--hot-window` for that one node. An existing §9.6.1 setting tuned
    /// down, never a new knob — the fault uses the daemon's own drain
    /// cadence rather than a back door.
    #[arg(long, default_value = "1s")]
    fault_cache_churn_hot_window: String,

    /// The SQL each racing read issues. The default reads the staging
    /// engine's own window registry, which `DropWindow` DELETEs from inside
    /// the same transaction as its `DROP TABLE`
    /// (`duckspout_staging::engine`) — a read of exactly the table the churn
    /// is mutating, which is what makes a held lock observable at all. A
    /// query over synthetic rows would touch nothing the churn touches and
    /// would prove nothing about §2.4's obligation (c).
    #[arg(long, default_value = CACHE_CHURN_DEFAULT_QUERY)]
    fault_cache_churn_query: String,

    /// Pause between racing reads. Small enough that reads and residency
    /// actions actually interleave; a value at or above the churn duration
    /// would issue one read and prove nothing.
    #[arg(long, default_value_t = 50)]
    fault_cache_churn_read_interval_ms: u64,
}

/// `--fault-flight-kill-query`'s default (its own doc comment for the
/// sizing rationale): ~2M `BIGINT`s ≈ 16 MiB of encoded Arrow, orders of
/// magnitude past HTTP/2's 64 KiB default flow-control window.
const FLIGHT_KILL_DEFAULT_QUERY: &str = "SELECT i FROM range(0, 2000000) t(i)";

/// `--fault-cache-churn-query`'s default (its own doc comment for why this
/// table and not a synthetic one): the staging engine's window registry,
/// which `DropWindow`'s own transaction deletes rows from
/// (`duckspout_staging::engine`'s `DropWindow` transaction body).
const CACHE_CHURN_DEFAULT_QUERY: &str =
    "SELECT count(*) FROM duckspout_windows WHERE staged_bytes >= 0";

/// This runner's own smoke-loop status codes — **not** `duckspout-judge`'s
/// future Pass(0)/Violation(2)/NoVerdict(3) vocabulary (§8.4's vacuity-teeth
/// section); deliberately a disjoint scheme so nobody mistakes one for the
/// other.
const EXIT_OK: i32 = 0;
const EXIT_HARD_FAILURE: i32 = 1;
const EXIT_DRAIN_UNCONFIRMED: i32 = 2;

/// [`run`]'s failure message when [`all_batches_accepted`] says no. It names
/// what is actually measured — WHOLE-RUN acceptance, never acceptance scoped
/// to a fault window (an ACPR finding on issue #204, MEDIUM-5; module docs
/// of [`ingest_faulted_nodes`] carry the full reasoning).
const ALL_ACCEPTED_FAILURE: &str = "at least one node did not fully accept its drive-load batches across the WHOLE run (see \
     summary above); this runner measures whole-run acceptance, not acceptance scoped to a fault \
     window — the window-scoped verdict is the judge's, from journals (§8.4)";

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

    if cli.nodes == 0 {
        bail!("--nodes 0: nothing to boot");
    }
    // `--fault-churn-join` provisions ONE extra member (§8.4's membership
    // join): it gets a config, an identity and a slot in every other node's
    // `cluster.seed_peers` up front, but `boot_fleet` below never starts it
    // — `fault::run_membership_join` does, mid-run, under load.
    let nodes = topology::provision_nodes(
        &work_dir,
        cli.seed,
        cli.nodes + u16::from(cli.fault_churn_join),
        cli.otlp_base_port,
        cli.flight_base_port,
        cli.peer_base_port,
        cli.status_base_port,
    )?;
    let links = build_fault_links(&cli, &nodes).await?;
    for node in &nodes {
        let overrides = node_overrides(&cli, node.index, links.get(&node.index))?;
        let rendered = topology::render_node_config(&plan, node, &nodes, &overrides);
        std::fs::write(&node.config_path, rendered)
            .with_context(|| format!("writing {}", node.config_path.display()))?;
    }
    let (booted_nodes, joiner_nodes) = nodes.split_at(cli.nodes as usize);

    let mut running: Vec<process::RunningNode> = Vec::with_capacity(nodes.len());
    let boot_result = boot_fleet(&daemon_bin, booted_nodes, &cli, &mut running).await;
    if let Err(error) = boot_result {
        shutdown_all(&mut running, Duration::from_secs(cli.shutdown_grace_secs)).await;
        report_work_dir(&work_dir);
        return Err(error);
    }

    if let Some(loadgen_bin) = cli.loadgen_bin.as_deref() {
        spawn_loadgen_best_effort(loadgen_bin, &nodes[0]).await;
    }

    // §8.4/issues #203 and #204: whichever `--fault-*` faults were armed run
    // CONCURRENTLY with the drive-load/settle passes below — a fault fired
    // only after the smoke loop already finished would prove nothing about
    // the system under load. `fault_log` and `running` are separate
    // bindings from `nodes` (module docs of `run_armed_faults`), so this
    // borrows nothing the drive-load/settle futures also touch.
    let fault_log = faultlog::FaultLog::create(&work_dir.join("faults.ndjson"))
        .with_context(|| format!("creating {}", work_dir.join("faults.ndjson").display()))?;
    let fault_run = FaultRun {
        cli: &cli,
        daemon_bin: &daemon_bin,
        nodes: booted_nodes,
        joiner: joiner_nodes.first(),
        links: &links,
        log: &fault_log,
    };
    let (fault_result, load_results, drain_confirmed) = tokio::join!(
        run_armed_faults(&fault_run, &mut running),
        drive_load_all(booted_nodes, &cli, &links),
        wait_for_any_watermark(booted_nodes, Duration::from_secs(cli.settle_timeout_secs)),
    );
    let joined = match fault_result {
        Ok(joined) => joined,
        Err(error) => {
            tracing::error!(%error, "fault injection failed");
            shutdown_all(&mut running, Duration::from_secs(cli.shutdown_grace_secs)).await;
            report_work_dir(&work_dir);
            return Err(error);
        }
    };
    // A node that joined mid-run is a fleet member from here on — it must be
    // shut down with the rest, not leaked past the end of the run.
    running.extend(joined);
    let all_accepted = all_batches_accepted(&cli, &load_results);
    for (node, result) in booted_nodes.iter().zip(&load_results) {
        tracing::info!(
            node = %node.name,
            attempted = result.batches_attempted,
            accepted = result.batches_accepted,
            records = result.records_accepted,
            "drive-load pass complete"
        );
    }

    shutdown_all(&mut running, Duration::from_secs(cli.shutdown_grace_secs)).await;

    print_summary(booted_nodes, &load_results, drain_confirmed, &work_dir);
    report_work_dir(&work_dir);

    if !all_accepted {
        bail!(ALL_ACCEPTED_FAILURE);
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
/// never by this runner's own exit code. A node whose own INGEST PATH a
/// scheduled fault deliberately broke is therefore exempted from the "every
/// batch accepted" check ([`ingest_faulted_nodes`]): by the time `run()`
/// calls this, `run_armed_faults`'s own `Err` has already short-circuited
/// the whole function (module docs of [`run`]), so a still-configured
/// `--fault-*-node` at this point means that fault fired as scheduled — the
/// node's necessarily-incomplete batch acceptance is the fault working, not
/// a fleet-run failure. An ACPR finding on #203: the pre-fix check counted
/// a successful, scheduled kill as an `all_accepted` failure, causing
/// `duckspout-fleet` to `bail!` on exactly the outcome it was told to
/// produce.
fn all_batches_accepted(cli: &Cli, load_results: &[load::LoadResult]) -> bool {
    let exempt = ingest_faulted_nodes(cli);
    load_results.iter().enumerate().all(|(index, result)| {
        let ingest_faulted = u16::try_from(index).is_ok_and(|index| exempt.contains(&index));
        ingest_faulted || result.fully_accepted()
    })
}

/// The node indices whose own ingest path an armed fault deliberately broke
/// — a killed process, a paused process, a cut or throttled ingest link, a
/// node asked to leave, a node whose Flight server was killed.
///
/// Deliberately NOT every fault-targeted node: `--fault-catalog-outage-node`
/// and `--fault-discovery-flap-node` only touch a node's CATALOG link, which
/// §8.4 pairs with "ingest must continue undegraded" while "drains stall and
/// disclose". Exempting them here would erase the one runner-level signal
/// related to that predicate — so a catalog-faulted node whose ingest
/// degrades still shows up as a red smoke run.
///
/// # What the resulting check actually measures (an ACPR finding, MEDIUM-5)
///
/// [`all_batches_accepted`] is computed over the WHOLE run's batch counts,
/// so what a catalog-faulted node is held to here is "every batch of the
/// whole drive-load pass was accepted" — a STRICTER bar than §8.4's
/// during-the-window predicate, not the same one. The difference is real
/// and disclosed rather than papered over: [`fault::run_catalog_outage`]'s
/// own docs record that `libpq` does not reconnect a session cut mid-window,
/// so a node can ingest perfectly during the outage and degrade only
/// afterwards — and this check would still fail the run for it.
///
/// That strictness is kept deliberately. Scoping the check to the window
/// would need per-batch timing correlated against the window's own journaled
/// bounds, which is the judge's job (#205–#208, judging from journals after
/// the run) and not this runner's; and doing it here would NARROW a live
/// check, which is exactly the move this repo's constitution names as an
/// offense (§11). So the check stays whole-run and the wording — here, and
/// in [`run`]'s own failure message — says whole-run.
fn ingest_faulted_nodes(cli: &Cli) -> std::collections::BTreeSet<u16> {
    [
        cli.fault_kill_node,
        cli.fault_sigstop_node,
        cli.fault_partition_node,
        cli.fault_degrade_node,
        cli.fault_churn_leave_node,
        cli.fault_flight_kill_node,
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Probes Postgres and (when not `--local-lake`) `MinIO` before touching a
/// single node (module docs of [`backend_check`]).
async fn check_backends(cli: &Cli) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(cli.backend_check_timeout_secs);
    let (pg_host, pg_port) = dsn::postgres_host_port(&cli.postgres_dsn)?;
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

/// Everything the armed faults need that is not the (mutably borrowed) set
/// of running processes — grouped so [`run_armed_faults`] takes two
/// parameters rather than seven.
struct FaultRun<'a> {
    cli: &'a Cli,
    daemon_bin: &'a std::path::Path,
    /// The BOOTED members, index-aligned with `running`.
    nodes: &'a [NodeSpec],
    /// The provisioned-but-unbooted member `--fault-churn-join` starts, if
    /// that fault is armed.
    joiner: Option<&'a NodeSpec>,
    links: &'a FleetLinks,
    log: &'a faultlog::FaultLog,
}

/// Runs whichever faults `cli` armed (§8.4, issues #203 and #204).
///
/// Two groups, run CONCURRENTLY with each other (and, via [`run`]'s own
/// `tokio::join!`, with the drive-load and settle passes — a fault that
/// fired only after the smoke loop finished would prove nothing about the
/// system under load):
///
/// - **Network faults** ([`run_network_faults`]) touch only shared state (a
///   [`link::FaultLink`] handle and a [`NodeSpec`]), so they run
///   concurrently with each other too — a real chaos schedule overlaps its
///   windows, and each one journals its own start/end regardless. Two
///   windows can legitimately land on the SAME link (`--fault-catalog-
///   outage-node N` and `--fault-discovery-flap-node N` share node N's
///   catalog link, which [`link_needs`] builds once): they compose through
///   `crate::link`'s refcounted holds, so neither can lift the other's
///   condition — see that module's own docs.
/// - **Process faults** ([`run_process_faults`]) each need `&mut` on a
///   running child, so they run sequentially, in the fixed order below.
///   Each one's own `--fault-*-delay-secs` is measured from when its turn
///   starts, not from the beginning of the run.
///
/// The two groups are joined with `tokio::join!`, not `try_join!`, on
/// purpose: a process fault that fails must not cancel a network fault
/// mid-window, which would leave that link dropped with no `Ended` line
/// ever journaled for it — an unresolved window is exactly the shape
/// §8.4's own Journals paragraph calls a finding, and manufacturing one
/// through cancellation would be a lie about the run.
///
/// Returns any node that JOINED during the run, for the caller to fold into
/// its own shutdown. A fault whose target index is out of range is a plain
/// `--fault-*-node` misconfiguration, reported as an error rather than
/// silently skipped (R-3).
async fn run_armed_faults(
    ctx: &FaultRun<'_>,
    running: &mut [process::RunningNode],
) -> anyhow::Result<Vec<process::RunningNode>> {
    let (network, churn, process_faults) = tokio::join!(
        run_network_faults(ctx),
        run_cache_churn_fault(ctx),
        run_process_faults(ctx, running)
    );
    network?;
    churn?;
    process_faults
}

/// The cache/residency-churn fault (§8.4, issue #207). Its own group rather
/// than a member of either existing one: it touches no process handle (so it
/// need not serialize with the process faults) and no [`link::FaultLink`]
/// (so it is not a network fault), and it must run CONCURRENTLY with the
/// drive-load pass — a residency storm on an idle node churns nothing.
async fn run_cache_churn_fault(ctx: &FaultRun<'_>) -> anyhow::Result<()> {
    let cli = ctx.cli;
    let Some(index) = cli.fault_cache_churn_node else {
        return Ok(());
    };
    let target = node_spec(ctx.nodes, index, "--fault-cache-churn-node")?;
    fault::run_cache_churn(
        "cache-churn-0",
        target,
        &cli.fault_cache_churn_query,
        Duration::from_secs(cli.fault_cache_churn_delay_secs),
        Duration::from_secs(cli.fault_cache_churn_duration_secs),
        Duration::from_millis(cli.fault_cache_churn_read_interval_ms),
        ctx.log,
    )
    .await
}

/// The link-level faults (§8.4, issue #204): partition, asymmetric
/// degradation, catalog outage, discovery flapping. All concurrent; none of
/// them touches a process handle.
async fn run_network_faults(ctx: &FaultRun<'_>) -> anyhow::Result<()> {
    let cli = ctx.cli;
    let mut windows: Vec<futures::future::BoxFuture<'_, anyhow::Result<()>>> = Vec::new();

    if let Some(index) = cli.fault_partition_node {
        let target = node_spec(ctx.nodes, index, "--fault-partition-node")?;
        let links = ctx
            .links
            .get(&index)
            .map(NodeLinks::all)
            .unwrap_or_default();
        anyhow::ensure!(
            !links.is_empty(),
            "--fault-partition-node {index}: no fault links were built for that node \
             (build_fault_links and the flag set disagree — a partition with nothing to cut \
             would fire vacuously)"
        );
        windows.push(Box::pin(async move {
            fault::run_network_partition(
                "network-partition-0",
                target,
                &links,
                Duration::from_secs(cli.fault_partition_delay_secs),
                Duration::from_secs(cli.fault_partition_duration_secs),
                ctx.log,
            )
            .await
        }));
    }

    if let Some(index) = cli.fault_degrade_node {
        let target = node_spec(ctx.nodes, index, "--fault-degrade-node")?;
        let link = require_link(ctx.links, index, LinkKind::Ingress, "--fault-degrade-node")?;
        let conditions = link::LinkConditions {
            client_to_server: link::LinkCondition::Delay {
                ms: cli.fault_degrade_request_delay_ms,
            },
            server_to_client: link::LinkCondition::BandwidthCap {
                bytes_per_sec: cli.fault_degrade_response_bytes_per_sec,
            },
        };
        windows.push(Box::pin(async move {
            fault::run_network_degradation(
                "network-degradation-0",
                target,
                link,
                conditions,
                Duration::from_secs(cli.fault_degrade_delay_secs),
                Duration::from_secs(cli.fault_degrade_duration_secs),
                ctx.log,
            )
            .await
        }));
    }

    if let Some(index) = cli.fault_catalog_outage_node {
        let target = node_spec(ctx.nodes, index, "--fault-catalog-outage-node")?;
        let link = require_link(
            ctx.links,
            index,
            LinkKind::Catalog,
            "--fault-catalog-outage-node",
        )?;
        windows.push(Box::pin(async move {
            fault::run_catalog_outage(
                "catalog-outage-0",
                target,
                link,
                Duration::from_secs(cli.fault_catalog_outage_delay_secs),
                Duration::from_secs(cli.fault_catalog_outage_duration_secs),
                ctx.log,
            )
            .await
        }));
    }

    if let Some(index) = cli.fault_discovery_flap_node {
        let target = node_spec(ctx.nodes, index, "--fault-discovery-flap-node")?;
        let link = require_link(
            ctx.links,
            index,
            LinkKind::Catalog,
            "--fault-discovery-flap-node",
        )?;
        windows.push(Box::pin(async move {
            fault::run_discovery_flap(
                "discovery-flap-0",
                target,
                link,
                Duration::from_secs(cli.fault_discovery_flap_delay_secs),
                fault::FlapSchedule {
                    cycles: cli.fault_discovery_flap_cycles,
                    down: Duration::from_millis(cli.fault_discovery_flap_down_ms),
                    up: Duration::from_millis(cli.fault_discovery_flap_up_ms),
                },
                ctx.log,
            )
            .await
        }));
    }

    futures::future::try_join_all(windows).await?;
    Ok(())
}

/// The process-level faults (§8.4, issues #203 and #204), sequentially:
/// node kill, `SIGSTOP` pause, membership leave, membership join, Flight
/// kill. Returns whatever joined.
async fn run_process_faults(
    ctx: &FaultRun<'_>,
    running: &mut [process::RunningNode],
) -> anyhow::Result<Vec<process::RunningNode>> {
    let cli = ctx.cli;
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
        fault::run_node_kill("node-kill-0", target, timing, ctx.log).await?;
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
            ctx.log,
        )
        .await?;
    }
    if let Some(index) = cli.fault_churn_leave_node {
        let target = running
            .get_mut(index as usize)
            .ok_or_else(|| anyhow::anyhow!("--fault-churn-leave-node {index}: no such node"))?;
        fault::run_membership_leave(
            "membership-leave-0",
            target,
            Duration::from_secs(cli.fault_churn_leave_delay_secs),
            Duration::from_secs(cli.shutdown_grace_secs),
            ctx.log,
        )
        .await?;
    }
    if let Some(index) = cli.fault_flight_kill_node {
        let target = running
            .get_mut(index as usize)
            .ok_or_else(|| anyhow::anyhow!("--fault-flight-kill-node {index}: no such node"))?;
        fault::run_flight_kill_mid_stream(
            "flight-kill-mid-stream-0",
            target,
            &cli.fault_flight_kill_query,
            Duration::from_secs(cli.fault_flight_kill_delay_secs),
            Duration::from_secs(cli.fault_flight_kill_first_message_timeout_secs),
            ctx.log,
        )
        .await?;
    }
    let mut joined_nodes = Vec::new();
    if cli.fault_churn_join {
        let joiner = ctx.joiner.ok_or_else(|| {
            anyhow::anyhow!("--fault-churn-join: no joining node was provisioned")
        })?;
        joined_nodes.push(
            fault::run_membership_join(
                "membership-join-0",
                ctx.daemon_bin,
                joiner,
                Duration::from_secs(cli.fault_churn_join_delay_secs),
                Duration::from_secs(cli.boot_timeout_secs),
                ctx.log,
            )
            .await?,
        );
    }
    Ok(joined_nodes)
}

/// `nodes[index]`, or a reported `--fault-*-node` misconfiguration (R-3 —
/// never a silently skipped fault).
fn node_spec<'a>(nodes: &'a [NodeSpec], index: u16, flag: &str) -> anyhow::Result<&'a NodeSpec> {
    nodes
        .get(index as usize)
        .ok_or_else(|| anyhow::anyhow!("{flag} {index}: no such node"))
}

/// Which of a node's links a fault needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinkKind {
    Ingress,
    Catalog,
}

/// The named link for `index`, or a reported error — a fault armed against
/// a link the fleet never built would fire vacuously (§8.4's vacuity
/// teeth), so this fails closed rather than skipping.
fn require_link<'a>(
    links: &'a FleetLinks,
    index: u16,
    kind: LinkKind,
    flag: &str,
) -> anyhow::Result<&'a link::FaultLink> {
    links
        .get(&index)
        .and_then(|node_links| match kind {
            LinkKind::Ingress => node_links.ingress.as_ref(),
            LinkKind::Catalog => node_links.catalog.as_ref(),
        })
        .ok_or_else(|| anyhow::anyhow!("{flag} {index}: no {kind:?} fault link was built"))
}

/// Drives the placeholder smoke load against every node concurrently
/// (module docs of [`load`]), through each node's ingest fault link where
/// it has one — a degradation or partition armed against a link the load
/// never traverses would fire against an idle link.
async fn drive_load_all(
    nodes: &[NodeSpec],
    cli: &Cli,
    links: &FleetLinks,
) -> Vec<load::LoadResult> {
    let plan = load::LoadPlan {
        batches: cli.load_batches,
        batch_size: cli.load_batch_size,
        interval: Duration::from_millis(cli.load_interval_ms),
        tenant: cli.tenant.as_deref(),
    };
    let futures = nodes.iter().map(|node| {
        let addr = load_target_addr(node, links);
        let label = node.name.clone();
        async move {
            load::drive_load(&addr, &label, plan)
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(node = %label, %error, "drive-load could not even connect");
                    load::LoadResult::default()
                })
        }
    });
    futures::future::join_all(futures).await
}

/// Every fault link this run created, keyed by node index. Empty for a run
/// with no network fault armed — the links exist ONLY where a fault needs
/// one, so an unfaulted run's byte path is byte-for-byte what it was before
/// issue #204 (no proxy in front of ingest, the catalog or the lake).
type FleetLinks = std::collections::BTreeMap<u16, NodeLinks>;

/// One node's fault links (`crate::link`): whichever of its three real
/// network edges some armed fault needs to be able to condition.
#[derive(Default)]
struct NodeLinks {
    /// Client→node OTLP ingest, dialed by [`drive_load_all`].
    ingress: Option<link::FaultLink>,
    /// Node→Postgres catalog, dialed by the node itself (its rendered
    /// `catalog.dsn` points here).
    catalog: Option<link::FaultLink>,
    /// Node→S3/`MinIO` lake, dialed by the node itself (its rendered
    /// `lake.s3_endpoint` points here). Never built under `--local-lake`,
    /// which has no network path to the lake at all.
    lake: Option<link::FaultLink>,
}

impl NodeLinks {
    /// Every link this node actually has — what a full partition cuts.
    fn all(&self) -> Vec<&link::FaultLink> {
        [
            self.ingress.as_ref(),
            self.catalog.as_ref(),
            self.lake.as_ref(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

/// Which links node `index` needs, derived from the armed `--fault-*`
/// flags. A pure function so the derivation is testable without binding a
/// single socket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LinkNeeds {
    ingress: bool,
    catalog: bool,
    lake: bool,
}

impl LinkNeeds {
    fn none(self) -> bool {
        self == Self::default()
    }
}

/// Which fault links node `index` needs (module docs of [`FleetLinks`]:
/// nothing is proxied unless an armed fault needs it).
fn link_needs(cli: &Cli, index: u16) -> LinkNeeds {
    let targets = |flag: Option<u16>| flag == Some(index);
    let partitioned = targets(cli.fault_partition_node);
    LinkNeeds {
        ingress: partitioned || targets(cli.fault_degrade_node),
        catalog: partitioned
            || targets(cli.fault_catalog_outage_node)
            || targets(cli.fault_discovery_flap_node),
        // §8.4's partition is "this node is cut off"; under `--local-lake`
        // the lake is a shared directory, so there is no link to cut.
        lake: partitioned && !cli.local_lake,
    }
}

/// Binds every fault link the armed flags call for (module docs of
/// [`FleetLinks`]).
///
/// # Errors
///
/// If a link cannot be bound, or the catalog DSN / S3 endpoint a link must
/// forward to cannot be parsed.
async fn build_fault_links(cli: &Cli, nodes: &[NodeSpec]) -> anyhow::Result<FleetLinks> {
    let mut links = FleetLinks::new();
    for node in nodes {
        let needs = link_needs(cli, node.index);
        if needs.none() {
            continue;
        }
        let mut node_links = NodeLinks::default();
        if needs.ingress {
            node_links.ingress = Some(
                link::FaultLink::bind(
                    &format!("{}-ingest", node.name),
                    "127.0.0.1",
                    node.otlp_port,
                )
                .await?,
            );
        }
        if needs.catalog {
            let (host, port) = dsn::postgres_host_port(&cli.postgres_dsn)?;
            node_links.catalog =
                Some(link::FaultLink::bind(&format!("{}-catalog", node.name), &host, port).await?);
        }
        if needs.lake {
            let (host, port) = backend_check::s3_host_port(&cli.s3_endpoint)?;
            node_links.lake =
                Some(link::FaultLink::bind(&format!("{}-lake", node.name), &host, port).await?);
        }
        for one in node_links.all() {
            tracing::info!(
                node = %node.name,
                link = one.label(),
                listen = %one.listen_addr(),
                upstream = one.upstream(),
                "fault link bound (§8.4, issue #204)"
            );
        }
        links.insert(node.index, node_links);
    }
    Ok(links)
}

/// How node `index`'s rendered config must deviate from the fleet plan: the
/// catalog/lake addresses its fault links impose, and the shortened
/// `hot.window` the cache-churn fault needs on its own target
/// (`topology::NodeOverrides`).
///
/// # Errors
///
/// If the catalog DSN cannot be rewritten to point at the link.
fn node_overrides(
    cli: &Cli,
    index: u16,
    links: Option<&NodeLinks>,
) -> anyhow::Result<topology::NodeOverrides> {
    // Applied to the churn target ONLY: a fleet-wide short window would
    // change every node's drain cadence, which is a different experiment
    // from "one node's residency churns while queries race it" — and would
    // silently move the baseline every other fault is measured against.
    let hot_window = (cli.fault_cache_churn_node == Some(index))
        .then(|| cli.fault_cache_churn_hot_window.clone());
    let Some(links) = links else {
        return Ok(topology::NodeOverrides {
            hot_window,
            ..topology::NodeOverrides::default()
        });
    };
    let postgres_dsn = links
        .catalog
        .as_ref()
        .map(|link| {
            dsn::rewrite_postgres_host_port(
                &cli.postgres_dsn,
                "127.0.0.1",
                link.listen_addr().port(),
            )
        })
        .transpose()?;
    let s3_endpoint = links
        .lake
        .as_ref()
        .map(|link| format!("127.0.0.1:{}", link.listen_addr().port()));
    Ok(topology::NodeOverrides {
        postgres_dsn,
        s3_endpoint,
        hot_window,
    })
}

/// Where the drive-load pass sends `node`'s traffic: through its ingest
/// fault link when it has one, straight at the node otherwise.
fn load_target_addr(node: &NodeSpec, links: &FleetLinks) -> String {
    links
        .get(&node.index)
        .and_then(|node_links| node_links.ingress.as_ref())
        .map_or_else(
            || node.otlp_addr(),
            |link| format!("http://{}", link.listen_addr()),
        )
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
        "  fault-window journal: {} (§8.4, issues #203/#204)",
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
            tenant: None,
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
            fault_partition_node: None,
            fault_partition_delay_secs: 5,
            fault_partition_duration_secs: 10,
            fault_degrade_node: None,
            fault_degrade_delay_secs: 5,
            fault_degrade_duration_secs: 10,
            fault_degrade_request_delay_ms: 250,
            fault_degrade_response_bytes_per_sec: 16 * 1024,
            fault_churn_leave_node: None,
            fault_churn_leave_delay_secs: 5,
            fault_churn_join: false,
            fault_churn_join_delay_secs: 5,
            fault_flight_kill_node: None,
            fault_flight_kill_delay_secs: 5,
            fault_flight_kill_query: FLIGHT_KILL_DEFAULT_QUERY.to_owned(),
            fault_flight_kill_first_message_timeout_secs: 30,
            fault_catalog_outage_node: None,
            fault_catalog_outage_delay_secs: 5,
            fault_catalog_outage_duration_secs: 10,
            fault_discovery_flap_node: None,
            fault_discovery_flap_delay_secs: 5,
            fault_discovery_flap_cycles: 5,
            fault_discovery_flap_down_ms: 1_000,
            fault_discovery_flap_up_ms: 1_000,
            fault_cache_churn_node: None,
            fault_cache_churn_delay_secs: 5,
            fault_cache_churn_duration_secs: 20,
            fault_cache_churn_hot_window: "1s".to_owned(),
            fault_cache_churn_query: CACHE_CHURN_DEFAULT_QUERY.to_owned(),
            fault_cache_churn_read_interval_ms: 50,
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

    /// A run with no network fault armed must build NO links at all — the
    /// unfaulted byte path has to stay exactly what it was before issue
    /// #204 (module docs of [`FleetLinks`]), and a proxy silently inserted
    /// in front of every node's ingest would change every existing fleet
    /// run's behaviour.
    #[test]
    fn an_unfaulted_run_needs_no_links_at_all() {
        let cli = base_cli("no-links");
        for index in 0..cli.nodes {
            assert!(link_needs(&cli, index).none(), "node {index} needs no link");
        }
    }

    /// A partition cuts every link the node has — and the lake link only
    /// exists when there IS a lake network path (`--local-lake` shares a
    /// directory, so there is nothing to cut).
    #[test]
    fn a_partition_needs_every_link_except_a_local_lakes() {
        let mut cli = base_cli("partition-links");
        cli.fault_partition_node = Some(1);

        assert_eq!(
            link_needs(&cli, 1),
            LinkNeeds {
                ingress: true,
                catalog: true,
                lake: false, // base_cli sets --local-lake
            }
        );
        assert!(
            link_needs(&cli, 0).none(),
            "only the partitioned node gets links"
        );

        cli.local_lake = false;
        assert!(
            link_needs(&cli, 1).lake,
            "a real S3 lake is a real network edge a partition must cut"
        );
    }

    /// The catalog-only faults touch ONLY the catalog link: a discovery
    /// flap or a catalog outage that also proxied ingest would silently
    /// change the ingest path it is supposed to leave alone (§8.4: "ingest
    /// must continue undegraded").
    #[test]
    fn catalog_faults_need_only_the_catalog_link() {
        let mut cli = base_cli("catalog-links");
        cli.fault_catalog_outage_node = Some(0);
        assert_eq!(
            link_needs(&cli, 0),
            LinkNeeds {
                ingress: false,
                catalog: true,
                lake: false
            }
        );

        let mut cli = base_cli("flap-links");
        cli.fault_discovery_flap_node = Some(2);
        assert_eq!(
            link_needs(&cli, 2),
            LinkNeeds {
                ingress: false,
                catalog: true,
                lake: false
            }
        );
    }

    /// A degradation conditions the INGEST link (that is the traffic
    /// `drive_load_all` actually sends), and nothing else.
    #[test]
    fn a_degradation_needs_only_the_ingress_link() {
        let mut cli = base_cli("degrade-links");
        cli.fault_degrade_node = Some(0);
        assert_eq!(
            link_needs(&cli, 0),
            LinkNeeds {
                ingress: true,
                catalog: false,
                lake: false
            }
        );
    }

    /// The load driver must dial the ingest LINK when one exists — a
    /// degradation or partition armed against a link the load never
    /// traverses would condition an idle socket and prove nothing.
    #[tokio::test]
    async fn drive_load_targets_the_ingest_link_when_one_exists() {
        let node = test_node_spec("link-target", 0, 4317);
        let mut links = FleetLinks::new();
        assert_eq!(
            load_target_addr(&node, &links),
            node.otlp_addr(),
            "with no link, the load must go straight at the node"
        );

        let ingress = link::FaultLink::bind("link-target-ingest", "127.0.0.1", node.otlp_port)
            .await
            .unwrap();
        let expected = format!("http://{}", ingress.listen_addr());
        links.insert(
            node.index,
            NodeLinks {
                ingress: Some(ingress),
                ..NodeLinks::default()
            },
        );
        assert_eq!(load_target_addr(&node, &links), expected);
    }

    /// `node_overrides` must point the node's own config at whatever
    /// links it has — and at nothing when it has none.
    #[tokio::test]
    async fn node_overrides_follow_the_links_a_node_actually_has() {
        let mut cli = base_cli("overrides");
        cli.local_lake = false;
        cli.postgres_dsn = "postgres://duckspout@10.0.0.9:5432/duckspout_catalog".to_owned();

        assert!(
            node_overrides(&cli, 0, None)
                .unwrap()
                .postgres_dsn
                .is_none(),
            "a node with no links keeps the fleet's own backend addresses"
        );

        let catalog = link::FaultLink::bind("n0-catalog", "127.0.0.1", 5432)
            .await
            .unwrap();
        let lake = link::FaultLink::bind("n0-lake", "127.0.0.1", 9000)
            .await
            .unwrap();
        let catalog_port = catalog.listen_addr().port();
        let lake_port = lake.listen_addr().port();
        let links = NodeLinks {
            ingress: None,
            catalog: Some(catalog),
            lake: Some(lake),
        };

        let overrides = node_overrides(&cli, 0, Some(&links)).unwrap();
        assert_eq!(
            overrides.postgres_dsn.as_deref(),
            Some(
                format!("postgres://duckspout@127.0.0.1:{catalog_port}/duckspout_catalog").as_str()
            )
        );
        assert_eq!(
            overrides.s3_endpoint.as_deref(),
            Some(format!("127.0.0.1:{lake_port}").as_str())
        );
    }

    /// `require_link` fails closed on a fault armed against a link that was
    /// never built — a vacuous fault window (§8.4's vacuity teeth) must be
    /// a reported error, never a silent skip (R-3).
    #[test]
    fn require_link_fails_closed_when_no_link_was_built() {
        let links = FleetLinks::new();
        assert!(require_link(&links, 0, LinkKind::Catalog, "--fault-x").is_err());
        assert!(require_link(&links, 0, LinkKind::Ingress, "--fault-x").is_err());
    }

    /// `node_spec` reports an out-of-range `--fault-*-node` rather than
    /// silently arming nothing.
    #[test]
    fn node_spec_reports_an_out_of_range_target() {
        let nodes = vec![test_node_spec("only", 0, 0)];
        assert!(node_spec(&nodes, 0, "--fault-x").is_ok());
        assert!(node_spec(&nodes, 7, "--fault-x").is_err());
    }

    /// The catalog-only faults are deliberately NOT exempted from the
    /// whole-run batch-acceptance check (module docs of
    /// [`ingest_faulted_nodes`], including exactly how that check differs
    /// from §8.4's during-the-window predicate): exempting them would erase
    /// the one runner-level signal related to "ingest must continue
    /// undegraded".
    #[test]
    fn ingest_faulted_nodes_covers_ingest_faults_only() {
        let mut cli = base_cli("exempt-set");
        cli.fault_partition_node = Some(0);
        cli.fault_churn_leave_node = Some(1);
        cli.fault_flight_kill_node = Some(2);
        cli.fault_catalog_outage_node = Some(3);
        cli.fault_discovery_flap_node = Some(4);
        let exempt = ingest_faulted_nodes(&cli);
        assert!(exempt.contains(&0));
        assert!(exempt.contains(&1));
        assert!(exempt.contains(&2));
        assert!(
            !exempt.contains(&3),
            "a catalog outage must not excuse degraded ingest"
        );
        assert!(
            !exempt.contains(&4),
            "discovery flapping must not excuse degraded ingest"
        );
    }

    /// The generalized exemption still behaves the way #203's own tests
    /// pinned it for the kill fault, for every #204 ingest-path fault too.
    #[test]
    fn all_batches_accepted_exempts_every_ingest_faulted_node() {
        let mut cli = base_cli("exempt-partition");
        cli.fault_partition_node = Some(1);
        let results = vec![load_result(10, 10), load_result(10, 0)];
        assert!(all_batches_accepted(&cli, &results));

        let mut cli = base_cli("exempt-none");
        cli.fault_catalog_outage_node = Some(1);
        assert!(
            !all_batches_accepted(&cli, &results),
            "a catalog outage must leave a partial acceptance red"
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
