//! The daemon's composition root (§10.4): `StagingEngine` +
//! `OtlpLogsService` (gRPC listener) + `DrainCoordinator` (ticked off the
//! [`Clock`] port, single-node so no takeover) +
//! `DuckLakeCommitter` + `WatermarkLedger` into one running process, plus
//! the [`crate::status`] disclosure listener.
//!
//! # What is wired here (issue #38)
//!
//! - The full OTLP accept → stage → ack path over the real WAL=hot engine
//!   (`tests/otlp_e2e.rs` proved the chain; this composes it with real
//!   listeners and the production [`crate::system`] ports instead of test
//!   doubles).
//! - The drain loop: a background pass that (a) notes newly-closed windows
//!   (the "ingest roller" `duckspout-staging/src/seal.rs` module docs assign
//!   the composition — see `note_closed_windows` below) and (b) drains every
//!   window the coordinator reports eligible, through the real
//!   `DuckLakeCommitter`.
//! - **Watermark reconstruction at boot** (§6.8, ADR-0010; issue #153):
//!   before the drain coordinator is built, [`Daemon::boot`] reads back
//!   every committed window manifest through
//!   [`duckspout_lake_ducklake::DuckLakeCommitter::read_manifests`] and
//!   replays it through [`duckspout_watermark::WatermarkLedger::reconstruct`]
//!   — the exact function the live drain path uses, run over the stored
//!   record instead of as-it-happens (`reconstruct.rs`'s module docs: "one
//!   rule, two entry points"). A never-drained node's lake has no manifests,
//!   so this reduces to the empty ledger v0.1 used to hardcode; a restarting
//!   node with prior commits gets the true per-partition dense-next window
//!   back, so `DrainCoordinator`'s pre-commit fence does not reject the real
//!   next window as `WindowAhead`. Deliberately **not** a `LakeCommitter`
//!   port method (`read_manifests`'s own doc comment) — it stays a
//!   `DuckLake`-specific boot read on the concrete committer this module
//!   already binds.
//! - The [`crate::status`] endpoint: `NodeId`, the overload rung, watermark
//!   per partition, `drain_stalled` (§9.3, R-9).
//! - **Arrow Flight serving** over the hot store (issue #39, PR #151,
//!   wired here as the #151→#154 follow-up): [`crate::serving::HotFlightService`]
//!   built over the same `StagingEngine` the stager writes through, guarded
//!   per §7.8 with [`crate::serving::ServingConfig`] read from `query.*`
//!   (`hot.max_bytes` supplies the fill-scale denominator already computed
//!   for the stager). Bound on `node.flight_listen`, served alongside the
//!   OTLP and status listeners, and folded into the same SIGTERM
//!   choreography below.
//! - SIGTERM: readiness flips false, in-flight gRPC work finishes
//!   (`tonic`'s own graceful shutdown), the drain loop finishes its current
//!   tick, then the process exits (§9.1.2's shallow drain — v0.1 has no
//!   replica peers to hand staged data to, so "shallow" here is exactly
//!   "finish, do not orphan work mid-tick").
//! - **HRW ring integration + ownership routing** (§5.2–5.3, issue #52):
//!   [`Daemon::boot`] reads `cluster.seed_peers` and this node's own id into
//!   a [`duckspout_replication::routing::MembershipView`]
//!   (`build_membership_view`, private) and stores it, alongside
//!   `cluster.rf`, on the daemon's own core state. [`DaemonHandle::routing_plan`] resolves
//!   [`duckspout_replication::routing::route_write`] against that view —
//!   the ring owner, the RF replica set, and whether this node is the
//!   owner — for any partition, real composed state rather than the bare
//!   `hrw_owner` re-export below on its own. See that re-export's doc
//!   comment for exactly what remains unwired and why.
//!
//! # What is explicitly deferred, and why
//!
//! - **The CTK trace sink** (issue #42, PR #152 — merged after this branch
//!   was cut, then rebased in): every trace-capable port
//!   (`OtlpLogsService`, `EngineStager`, `DrainCoordinator`) now carries an
//!   optional `.with_trace_sink(...)` builder method, defaulting to `None`
//!   — this composition deliberately never calls it (SCOPE confirms the
//!   default: "still optional/None by default"). Journaling turns on only
//!   once the `conformance` ledger row arms (issue #44), which is a CI/CTK
//!   concern, not a boot-wiring one.
//! - **Multi-tenant dataset declarations** (§9.6.2): v0.1 ships exactly one
//!   built-in dataset, `otlp_logs` (the OTLP adapter's fixed target); its
//!   drain plan (`otlp_logs_drain_plan`) is hardcoded rather than read
//!   from a declaration ledger that does not exist yet.
//! - **Actually Forwarding a routed write over the network** (issue #52's
//!   own scope note): [`DaemonHandle::routing_plan`] makes the ownership
//!   decision real and composed, but nothing in the accept path calls it
//!   yet, because there is nothing to Forward *through*: no crate in this
//!   workspace implements `duckspout_types::Transport` over a real
//!   network today (only `duckspout-ctk`'s in-memory `InMemTransport`
//!   exists, a test double), and `duckspout_types::ReplicaLog`'s concrete
//!   `duckspout-staging` backend is issue #193's separate, already-filed
//!   follow-up. Hooking `duckspout_replication::forward::forward_to_peers`
//!   into the OTLP accept path needs both — a peer-transport server bound
//!   on `node.peer_listen` (§9.6.1's already-reserved, currently-unbound
//!   port) is genuinely new scope, not a wiring-only task, so it is left
//!   for a dedicated follow-up rather than invented here as an untested
//!   stub. `cluster.rf > 1` is therefore still not durable beyond the
//!   local node's own copy at v0.1 — exactly the same limitation
//!   `StageOutcome::DuplicateInFlight`'s and `client_ack_ready`'s own doc
//!   comments already disclose ("unreachable at RF = 1 ... replication
//!   (v0.2) makes it live"), unchanged by this issue.

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_flight::flight_service_server::FlightServiceServer;
use duckspout_accept::OtlpLogsService;
use duckspout_accept::otlp::OTLP_LOGS_DATASET;
use duckspout_accept::server::AdmissionConfig as OtlpAdmissionConfig;
use duckspout_drain::{DatasetDrainPlan, DrainConfig as DrainScheduleConfig, DrainCoordinator};
use duckspout_lake_ducklake::{DuckLakeCommitter, DuckLakeConfig};
use duckspout_staging::{
    EngineSealSurface, EngineStager, StagerConfig, StagingConfig, StagingEngine,
};
use duckspout_types::{
    BoxFuture, Clock, DatasetDeclaration, DatasetId, DatasetKind, DecodedBatch, LakeCommitter,
    NodeId, PartitionId, SealSurface, StageCommitter, StageError, StageOutcome, Storage,
};
use duckspout_watermark::{SharedLedger, WatermarkLedger};
use tokio::net::TcpListener;

use crate::config::{self, DaemonConfig};
use crate::constants::DRAIN_TICK_INTERVAL_MS;
use crate::serving::{HotFlightService, ServingConfig};
use crate::status::{self, StatusSnapshot};
use crate::system::{self, FsStorage, SystemClock};

/// Placement (§5): any node accepts, then forwards to the HRW owner. The
/// bare placement function, re-exported so the composition-shape
/// declaration (§10.1's crate graph) stays honest even for callers that
/// only need [`hrw_owner`] directly rather than a resolved
/// [`RoutingPlan`] — [`DaemonHandle::routing_plan`] below is the real,
/// composed seam (§5.2–5.3, issue #52): it resolves [`route_write`]
/// against the [`MembershipView`] built at boot (`build_membership_view`,
/// private), not a bare candidate list a caller has to
/// assemble itself. **Not called from the OTLP accept path yet** — module
/// docs' "What is explicitly deferred" section explains why (no peer
/// `Transport` exists to Forward the routed write through).
pub use duckspout_replication::hrw_owner;
pub use duckspout_replication::routing::{MembershipView, RoutingPlan, route_write};

/// Everything that can go wrong composing or booting the daemon. Every
/// variant is a boot-time failure — nothing here is a runtime data-path
/// error (those are the protocol crates' own typed errors, surfaced through
/// the ports the daemon wires together).
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// The hot volume (or its scratch/lake-data siblings) could not be
    /// prepared.
    #[error("preparing node storage: {0}")]
    Storage(#[from] std::io::Error),
    /// The staging engine failed to open (§4.2), or the Flight service's
    /// pre-created read-connection pool (§7.4, #114) failed to clone.
    #[error("opening the staging engine: {0}")]
    Staging(#[from] duckspout_staging::StagingError),
    /// The lake committer failed to open or bootstrap (§6.4).
    #[error("opening the lake committer: {0}")]
    Lake(#[from] duckspout_types::LakeError),
    /// The local parts object store could not be constructed (§6.1).
    #[error("opening the parts object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// A §9.6.1 duration setting did not parse.
    #[error("configuration: {0}")]
    Config(#[from] system::DurationParseError),
    /// The OTLP, observation, or Flight listener could not bind.
    #[error("binding {what} listener on {addr}: {source}")]
    Bind {
        /// Which listener failed to bind.
        what: &'static str,
        /// The address it tried to bind.
        addr: SocketAddr,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// A built-in dataset's fixed schema uses an Arrow type outside the §2
    /// lattice subset the lake backend accepts (never expected at v0.1 —
    /// `otlp_logs`' schema is a fixed, reviewed constant).
    #[error("dataset schema column has arrow type {0}, outside the supported lake subset")]
    UnsupportedSchemaColumn(String),
    /// The lake's manifest record is corrupt — a duplicate window id (the
    /// §6.6 fence makes one impossible via the normal drain path), an
    /// overlapping-coverage or malformed row (§6.8, issue #153). Boot fails
    /// closed rather than starting the watermark ledger from a guess (R-3):
    /// an honest lake's own record always reconstructs.
    #[error("reconstructing the watermark ledger from the lake's manifest record: {0}")]
    WatermarkReconstruction(#[from] duckspout_watermark::ReconstructError),
}

/// How one background drain tick went — logged, not otherwise consumed
/// (the [`crate::status`] endpoint reads the watermark ledger and the
/// `drain_stalled` flag directly, not this report).
#[derive(Debug, Clone, Copy, Default)]
pub struct DrainTickReport {
    /// Windows the coordinator judged eligible this tick (§6.3).
    pub eligible: usize,
    /// Windows that committed (including "already committed" replays,
    /// §6.9).
    pub committed: usize,
    /// Windows requeued — not an error; see [`duckspout_drain::RequeueReason`].
    pub requeued: usize,
}

/// The concrete v0.1 staging stack: the WAL=hot engine over the production
/// [`FsStorage`] port and [`SystemClock`].
type Stager = EngineStager<FsStorage, SystemClock>;

/// The shared, `Arc`-held state a running daemon exposes to its listeners
/// and its background drain loop. Cheap to clone (one more `Arc` each);
/// exists so [`Daemon::handle`] can hand out a driver independent of the
/// listener sockets themselves (tests drive ticks directly, without waiting
/// on [`DRAIN_TICK_INTERVAL_MS`]).
struct DaemonCore {
    node_id: NodeId,
    stager: Arc<Stager>,
    seal_surface: Arc<EngineSealSurface<FsStorage>>,
    drain: DrainCoordinator,
    ledger: Arc<SharedLedger>,
    clock: SystemClock,
    drain_stalled: AtomicBool,
    ready: AtomicBool,
    /// The ownership-routing membership view (§5.2, issue #52), built once
    /// at boot from `cluster.seed_peers` (module docs of
    /// [`build_membership_view`]). Advisory and static for the lifetime of
    /// the process at v0.1 — there is no live registry (#53) to refresh it
    /// from yet.
    membership: MembershipView,
    /// `cluster.rf` (§5.1, §5.11), read once at boot.
    rf: u16,
}

/// Assembles the [`DaemonCore`] from `config` and every already-opened port
/// ([`Daemon::boot`]'s own locals) — split out of `boot` purely to keep that
/// function under the workspace's line-count ceiling; it composes nothing
/// [`Daemon::boot`] didn't already decide. Builds the ownership-routing
/// [`MembershipView`] (§5.2, issue #52; [`build_membership_view`]'s own doc
/// comment) as its one piece of actual logic.
fn assemble_core(
    config: &DaemonConfig,
    node_id: NodeId,
    stager: Arc<Stager>,
    seal_surface: Arc<EngineSealSurface<FsStorage>>,
    drain: DrainCoordinator,
    ledger: Arc<SharedLedger>,
    clock: SystemClock,
) -> Arc<DaemonCore> {
    let membership = build_membership_view(config, &node_id);
    Arc::new(DaemonCore {
        node_id,
        stager,
        seal_surface,
        drain,
        ledger,
        clock,
        drain_stalled: AtomicBool::new(false),
        ready: AtomicBool::new(false),
        membership,
        rf: config.cluster.rf,
    })
}

impl DaemonCore {
    /// One drain-loop pass (module docs): note newly-closed windows, then
    /// drain everything the coordinator reports eligible.
    async fn drain_once(&self) -> DrainTickReport {
        note_closed_windows(&self.stager, &self.seal_surface, self.clock.wall_unix_ms());

        let Ok(eligible) = self.drain.eligible_windows().await else {
            // The seal surface itself failed to enumerate — treat as a
            // stall (R-9: disclose, never go silent) and try again next
            // tick.
            self.drain_stalled.store(true, Ordering::Relaxed);
            return DrainTickReport::default();
        };

        let mut report = DrainTickReport {
            eligible: eligible.len(),
            ..DrainTickReport::default()
        };
        let mut stalled = false;
        for window in &eligible {
            let Some(plan) = drain_plan_for(&window.dataset) else {
                // No declared plan for this dataset (module docs: v0.1 has
                // exactly one built-in dataset) — leave it for a future
                // tick rather than guess a plan.
                continue;
            };
            match self
                .drain
                .drain_window(&window.dataset, &window.partition, window.window, &plan)
                .await
            {
                Ok(
                    duckspout_drain::DrainOutcome::Committed { .. }
                    | duckspout_drain::DrainOutcome::AlreadyCommitted,
                ) => report.committed += 1,
                Ok(duckspout_drain::DrainOutcome::Requeue(reason)) => {
                    report.requeued += 1;
                    tracing::warn!(?reason, dataset = %window.dataset, partition = %window.partition, window = window.window.0, "drain requeue");
                    if matches!(reason, duckspout_drain::RequeueReason::CatalogUnavailable) {
                        stalled = true;
                    }
                }
                Err(error) => {
                    report.requeued += 1;
                    tracing::warn!(%error, dataset = %window.dataset, partition = %window.partition, window = window.window.0, "drain error");
                }
            }
        }
        self.drain_stalled.store(stalled, Ordering::Relaxed);
        report
    }

    fn status_snapshot(&self) -> StatusSnapshot {
        let drain_stalled = self.drain_stalled.load(Ordering::Relaxed);
        // `EngineStager::status` is infallible as of PR #151 (`staged_bytes`
        // no longer fails), so there is no accounting-unreadable case left
        // to fail closed against.
        let status = self.stager.status(drain_stalled);
        StatusSnapshot {
            node_id: self.node_id.clone(),
            ready: self.ready.load(Ordering::Relaxed),
            status,
            drain_stalled,
            watermarks: self.ledger.snapshot().rows(),
        }
    }
}

/// Wraps [`EngineStager`] behind [`tokio::task::spawn_blocking`]: the port's
/// future already resolves synchronously (an fsynced `DuckDB` commit,
/// `crates/duckspout-staging/src/stager.rs` module docs), so awaiting it
/// inline would block whichever reactor worker thread happens to run the
/// gRPC handler for the whole commit latency. `tests/otlp_e2e.rs`'s module
/// docs name this exact obligation as production's, done once here
/// (ADR-0003's "group commit off the reactor" seam).
struct BlockingStager(Arc<Stager>);

impl StageCommitter for BlockingStager {
    fn stage_commit(&self, batch: DecodedBatch) -> BoxFuture<'_, Result<StageOutcome, StageError>> {
        let inner = Arc::clone(&self.0);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || inner.stage_blocking(&batch))
                .await
                .unwrap_or_else(|_| {
                    Err(StageError::Backend("stage_commit task panicked".to_owned()))
                })
        })
    }
}

/// Runs one [`DaemonCore::drain_once`] pass off the reactor
/// ([`tokio::task::spawn_blocking`]): `SealPart`'s `COPY` and
/// `DuckLakeCommitter`'s commit transaction are both blocking `DuckDB` work
/// (module docs of [`BlockingStager`] — the same discipline applies here).
/// `drain_once`'s own `.await` points all resolve synchronously already
/// (every port method underneath is `Box::pin(async move { result })`), so
/// driving it via [`tokio::runtime::Handle::block_on`] on a blocking-pool
/// thread costs nothing beyond the one thread hop.
async fn run_drain_tick_blocking(core: Arc<DaemonCore>) -> DrainTickReport {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || handle.block_on(core.drain_once()))
        .await
        .unwrap_or_default()
}

/// A cheap, `Clone`-able handle onto a running [`Daemon`]'s state — the seam
/// tests use to drive drain ticks and read status without waiting on the
/// production timers.
#[derive(Clone)]
pub struct DaemonHandle(Arc<DaemonCore>);

impl DaemonHandle {
    /// Runs one drain-loop pass immediately, off the reactor (module docs of
    /// `run_drain_tick_blocking` below).
    pub async fn drain_once(&self) -> DrainTickReport {
        run_drain_tick_blocking(Arc::clone(&self.0)).await
    }

    /// The current disclosed status (§9.3, R-9).
    #[must_use]
    pub fn status(&self) -> StatusSnapshot {
        self.0.status_snapshot()
    }

    /// This node's identity (§5).
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.0.node_id
    }

    /// Resolves the ownership-routing decision for `partition` (§5.2–5.3,
    /// issue #52): the ring owner, the RF replica set, and whether this
    /// node is that owner — [`route_write`] against the [`MembershipView`]
    /// built at boot (`build_membership_view`, private) and `cluster.rf`.
    ///
    /// `None` only when the membership view is somehow empty — unreachable
    /// past a real [`Daemon::boot`], which always seeds it with this node's
    /// own id (`build_membership_view`'s own doc comment).
    #[must_use]
    pub fn routing_plan(&self, partition: &PartitionId) -> Option<RoutingPlan> {
        route_write(partition, &self.0.node_id, &self.0.membership, self.0.rf)
    }
}

/// A booted-but-not-yet-serving daemon: every port is wired and both
/// listeners are bound (so their addresses are known — the test seam), but
/// nothing is accepting connections until [`Daemon::serve`] runs.
pub struct Daemon {
    core: Arc<DaemonCore>,
    otlp_listener: TcpListener,
    otlp_addr: SocketAddr,
    status_listener: TcpListener,
    status_addr: SocketAddr,
    flight_listener: TcpListener,
    flight_addr: SocketAddr,
    flight_service: FlightServiceServer<HotFlightService<FsStorage>>,
    admission: OtlpAdmissionConfig,
}

impl Daemon {
    /// Composes every port from `config` (§9.6) and binds the OTLP,
    /// observation, and Flight listeners. Blocking work (opening the
    /// staging engine and the lake committer) runs on the calling task —
    /// call this during startup, off any request path.
    ///
    /// `status_port` is not a §9.6.1 setting (module docs of
    /// [`crate::constants::OBSERVATION_LISTEN_PORT_DEFAULT`]); pass `0` to
    /// let the OS choose (tests), or the constant in production.
    ///
    /// # Errors
    ///
    /// See [`BootError`].
    pub async fn boot(config: &DaemonConfig, status_port: u16) -> Result<Self, BootError> {
        let node_id = system::detect_node_id(system::V01_FIXED_INCARNATION);
        let clock = SystemClock::new();

        // --- Staging (§4.2) ---
        let hot_storage = FsStorage::create(config.node.data_dir.clone())?;
        let engine = Arc::new(StagingEngine::open(
            StagingConfig {
                hot_dir: config.node.data_dir.clone(),
                origin: node_id.clone(),
            },
            hot_storage,
        )?);
        let hot_max_bytes = match config.hot.max_bytes {
            Some(bytes) => bytes,
            None => system::detect_hot_max_bytes(&config.node.data_dir)?,
        };
        let stager = Arc::new(EngineStager::new(
            Arc::clone(&engine),
            clock,
            StagerConfig {
                window_nanos: system::duration_nanos_saturating(system::parse_duration(
                    &config.hot.window,
                )?),
                dedup_ttl_ms: system::duration_millis_saturating(system::parse_duration(
                    &config.dedup.window_ttl,
                )?),
                dedup_max_entries: config.dedup.window_max_entries,
                hot_max_bytes,
            },
        ));
        let seal_surface = Arc::new(EngineSealSurface::new(Arc::clone(&engine)));

        let (committer, parts_store) = open_lake(config).await?;

        // --- Watermark ledger: reconstructed from the lake's manifest
        // --- record, never booted empty (§6.8, ADR-0010; issue #153) — see
        // --- `reconstruct_watermark_ledger`.
        let ledger = Arc::new(SharedLedger::new(reconstruct_watermark_ledger(&committer)?));

        let scratch_storage: Arc<dyn Storage> =
            Arc::new(FsStorage::create(config.node.data_dir.clone())?);

        let drain = DrainCoordinator::new(
            Arc::clone(&seal_surface) as Arc<dyn SealSurface>,
            Arc::clone(&ledger) as Arc<dyn duckspout_types::WatermarkBookkeeping>,
            Arc::clone(&committer) as Arc<dyn LakeCommitter>,
            parts_store,
            scratch_storage,
            Arc::new(clock) as Arc<dyn duckspout_types::Clock>,
            DrainScheduleConfig {
                allowed_lateness_ms: system::duration_millis_saturating(system::parse_duration(
                    &config.drain.allowed_lateness,
                )?),
            },
        );

        // --- Flight serving over the hot store (§7.4, §7.8) ---
        let serving_config = ServingConfig {
            max_hot_bytes_per_query: config
                .query
                .max_hot_bytes_per_query
                .unwrap_or_else(config::defaults::max_hot_bytes_per_query),
            hot_scan_deadline_ms: system::duration_nanos_saturating(system::parse_duration(
                &config.query.hot_scan_deadline,
            )?) / 1_000_000,
            max_concurrent_hot_scans: usize::try_from(config.query.max_concurrent_hot_scans)
                .unwrap_or(usize::MAX),
            hot_max_bytes,
        };
        let flight_service = HotFlightService::new(
            Arc::clone(&engine),
            Arc::new(clock) as Arc<dyn Clock>,
            serving_config,
        )?
        .into_server();

        let core = assemble_core(
            config,
            node_id,
            Arc::clone(&stager),
            seal_surface,
            drain,
            ledger,
            clock,
        );

        // --- Listeners ---
        let (otlp_listener, otlp_addr) = bind_listener("otlp", config.node.otlp_listen).await?;
        let (status_listener, status_addr) = bind_listener("status", status_port).await?;
        let (flight_listener, flight_addr) =
            bind_listener("flight", config.node.flight_listen).await?;

        let admission = OtlpAdmissionConfig {
            max_payload_bytes: usize::try_from(config.max_payload_bytes).unwrap_or(usize::MAX),
            max_inflight_bytes: match config.admission.max_inflight_bytes {
                Some(bytes) => bytes,
                None => system::detect_memory_budget()?,
            },
        };

        Ok(Self {
            core,
            otlp_listener,
            otlp_addr,
            status_listener,
            status_addr,
            flight_listener,
            flight_addr,
            flight_service,
            admission,
        })
    }

    /// The bound OTLP/gRPC listener's address (its port, when
    /// `node.otlp_listen = 0`, is only known after [`Daemon::boot`]).
    #[must_use]
    pub fn otlp_addr(&self) -> SocketAddr {
        self.otlp_addr
    }

    /// The bound observation listener's address.
    #[must_use]
    pub fn status_addr(&self) -> SocketAddr {
        self.status_addr
    }

    /// The bound Arrow Flight listener's address (§7.4) — its port, when
    /// `node.flight_listen = 0`, is only known after [`Daemon::boot`].
    #[must_use]
    pub fn flight_addr(&self) -> SocketAddr {
        self.flight_addr
    }

    /// A cheap handle onto the running daemon's state, independent of the
    /// listener sockets — the test seam (module docs).
    #[must_use]
    pub fn handle(&self) -> DaemonHandle {
        DaemonHandle(Arc::clone(&self.core))
    }

    /// Runs the daemon: the OTLP listener, the observation listener, the
    /// Flight listener, and the background drain loop, all until `shutdown`
    /// resolves — then readiness flips false, in-flight gRPC work finishes,
    /// the drain loop finishes its current tick, and this returns (§9.1.2's
    /// SIGTERM choreography, shallow — module docs).
    pub async fn serve(self, shutdown: impl Future<Output = ()> + Send + 'static) {
        self.core.ready.store(true, Ordering::Relaxed);

        let stager = Arc::new(BlockingStager(Arc::clone(&self.core.stager)));
        let otlp_service = OtlpLogsService::new(stager, self.admission).into_server();

        let (otlp_shutdown_tx, otlp_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (status_shutdown_tx, status_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (flight_shutdown_tx, flight_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (drain_shutdown_tx, mut drain_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let otlp_task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(otlp_service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(self.otlp_listener),
                    async {
                        let _ = otlp_shutdown_rx.await;
                    },
                ),
        );

        let flight_task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(self.flight_service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(self.flight_listener),
                    async {
                        let _ = flight_shutdown_rx.await;
                    },
                ),
        );

        let core_for_status = Arc::clone(&self.core);
        let status_task = tokio::spawn(status::serve(
            self.status_listener,
            Arc::new(move || core_for_status.status_snapshot()),
            async {
                let _ = status_shutdown_rx.await;
            },
        ));

        let core_for_drain = Arc::clone(&self.core);
        let drain_task = tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(DRAIN_TICK_INTERVAL_MS));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let report = run_drain_tick_blocking(Arc::clone(&core_for_drain)).await;
                        tracing::debug!(
                            eligible = report.eligible,
                            committed = report.committed,
                            requeued = report.requeued,
                            "drain tick"
                        );
                    }
                    _ = &mut drain_shutdown_rx => return,
                }
            }
        });

        shutdown.await;
        self.core.ready.store(false, Ordering::Relaxed);
        tracing::info!("SIGTERM: readiness false, draining in-flight work");

        let _ = otlp_shutdown_tx.send(());
        let _ = status_shutdown_tx.send(());
        let _ = flight_shutdown_tx.send(());
        let _ = drain_shutdown_tx.send(());
        let _ = otlp_task.await;
        let _ = status_task.await;
        let _ = flight_task.await;
        let _ = drain_task.await;
        tracing::info!("shutdown complete");
    }
}

/// Evolves the lake's `otlp_logs` table into existence from the OTLP
/// adapter's fixed schema (module docs at the call site), **plus** the two
/// system columns every sealed part carries (`origin VARCHAR`, `seq
/// UBIGINT` — `crates/duckspout-staging/src/engine.rs`'s window-table DDL,
/// §2.3): `SealPart`'s `COPY` selects the whole staging table, system
/// columns included, so the lake table `ducklake_add_data_files` registers
/// against must carry the identical column set or the file/table schemas
/// mismatch. `evolve_schema` is idempotent (`CREATE TABLE IF NOT EXISTS` /
/// `ADD COLUMN IF NOT EXISTS`, §6.4) — safe to call on every boot.
async fn ensure_otlp_logs_table(committer: &Arc<dyn LakeCommitter>) -> Result<(), BootError> {
    let mut columns = duckspout_accept::otlp::logs_schema()
        .fields()
        .iter()
        .map(|field| {
            Ok(duckspout_types::ColumnSpec {
                name: field.name().clone(),
                logical_type: arrow_logical_type(field.data_type())?.to_owned(),
            })
        })
        .collect::<Result<Vec<_>, BootError>>()?;
    columns.push(duckspout_types::ColumnSpec {
        name: "origin".to_owned(),
        logical_type: "utf8".to_owned(),
    });
    columns.push(duckspout_types::ColumnSpec {
        name: "seq".to_owned(),
        logical_type: "uint64".to_owned(),
    });
    committer
        .evolve_schema(duckspout_types::SchemaEvolution {
            dataset: DatasetId::new(OTLP_LOGS_DATASET),
            columns,
        })
        .await?;
    Ok(())
}

/// Maps an Arrow [`DataType`](arrow::datatypes::DataType) onto the §2
/// logical-type lattice's name, as `duckspout-lake-ducklake`'s backend
/// accepts it — the same closed subset `duckspout-staging`'s engine stages
/// (`crates/duckspout-staging/src/engine.rs::staging_sql_type`), since a
/// column that can reach the hot store must be able to reach the lake too.
fn arrow_logical_type(data_type: &arrow::datatypes::DataType) -> Result<&'static str, BootError> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Utf8 => "utf8",
        DataType::Binary => "binary",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_micros",
        other => {
            return Err(BootError::UnsupportedSchemaColumn(other.to_string()));
        }
    })
}

/// The [`DatasetDrainPlan`] for v0.1's one built-in dataset (module docs).
/// Routed through [`DatasetDrainPlan::from_declaration`] (#93) so the
/// kind-based branching it implements is exercised by real wiring, not only
/// by its own unit tests — v0.1 still declares exactly one dataset, so
/// there is no declaration ledger to read a second one from yet.
fn drain_plan_for(dataset: &DatasetId) -> Option<DatasetDrainPlan> {
    (dataset.as_str() == OTLP_LOGS_DATASET).then(otlp_logs_drain_plan)
}

/// `otlp_logs`' declaration (`event`-kind, no dedup key) and its
/// event-time/statistics column (`ts`, `duckspout-accept/src/otlp.rs`'s
/// fixed schema — schema-level, so it is supplied here rather than read off
/// the declaration; see [`DatasetDrainPlan::from_declaration`]).
fn otlp_logs_drain_plan() -> DatasetDrainPlan {
    let declaration = DatasetDeclaration {
        dataset: DatasetId::new(OTLP_LOGS_DATASET),
        kind: DatasetKind::Event,
        key_cols: Vec::new(),
        sort_key: None,
    };
    DatasetDrainPlan::from_declaration(&declaration, "ts")
}

/// Builds this node's [`MembershipView`] (§5.2, issue #52) from
/// `cluster.seed_peers` plus `self_node` — the only membership source at
/// v0.1, per `docs/design/replication.md` §5.2 ("seeded at bootstrap by
/// `cluster.seed_peers`... superseded by the registry once reachable") and
/// [`duckspout_replication::routing`]'s own module docs (the registry does
/// not exist yet, issue #53). `self_node` is always included, so
/// [`route_write`] never sees an empty view — even a lone, unconfigured
/// node routes every partition to itself.
///
/// A duplicate peer entry (repeated in config, or one that happens to
/// resolve to `self_node`'s own id) is folded rather than kept twice —
/// `MembershipView` is a set of distinct candidates, and `hrw_ranked`'s own
/// score is per-node, so a repeated entry would silently double that node's
/// odds of nothing (HRW's score doesn't accumulate across duplicate rows,
/// but a duplicate WOULD show up twice in `RoutingPlan::replicas`, corrupting
/// its "distinct RF holders" meaning) — folding here keeps that invariant
/// true from construction rather than relying on every caller to dedup.
fn build_membership_view(config: &DaemonConfig, self_node: &NodeId) -> MembershipView {
    let mut candidates = vec![self_node.clone()];
    for raw in &config.cluster.seed_peers {
        let peer = seed_peer_node_id(raw);
        if !candidates.contains(&peer) {
            candidates.push(peer);
        }
    }
    MembershipView::new(candidates)
}

/// Renders one `cluster.seed_peers` entry as the [`NodeId`] its own boot
/// will present as — matching [`system::detect_node_id`]'s
/// `<hostname>/<incarnation>` convention. `cluster.seed_peers` entries are
/// dial addresses (`host` or `host:port`, §9.1.3), so a trailing `:<port>`
/// (all-ASCII-digit, so a bare IPv6 literal is never mistaken for one) is
/// stripped before appending the fixed v0.1 incarnation
/// ([`system::V01_FIXED_INCARNATION`] — no real per-peer incarnation is
/// knowable from config alone; `FenceBoot`'s real draw is issue #53's).
/// This is advisory, like the whole membership view (§5.2): a seed entry
/// whose host does not match what that peer's own `detect_node_id` produces
/// costs one avoidable forward hop once ownership resolves to a
/// name nothing answers to, never correctness.
fn seed_peer_node_id(raw: &str) -> NodeId {
    let host = raw
        .rsplit_once(':')
        .filter(|(_, port)| !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
        .map_or(raw, |(host, _)| host);
    NodeId::new(format!("{host}/{}", system::V01_FIXED_INCARNATION))
}

/// The ingest roller's obligation (`duckspout-staging/src/seal.rs` module
/// docs): note every window that is closed but not yet marked so. A live
/// window strictly below its `(dataset, partition)`'s persistent high-water
/// is closed by construction — [`EngineStager`]'s window roll only ever
/// allocates a new id once `hot.window` has elapsed on the old one — so this
/// needs no separate "just rolled" event from the stager. Idempotent
/// ([`EngineSealSurface::note_closed`] keeps the first-noted instant) and
/// safe to re-derive every tick, including after a restart.
fn note_closed_windows(stager: &Stager, seal_surface: &EngineSealSurface<FsStorage>, now_ms: i64) {
    let Ok(windows) = stager.engine().list_windows() else {
        return;
    };
    for window in windows {
        let Ok(Some(high_water)) = stager
            .engine()
            .highest_window_id(&window.dataset, &window.partition)
        else {
            continue;
        };
        if window.window.0 < high_water.0 {
            seal_surface.note_closed(&window.dataset, &window.partition, window.window, now_ms);
        }
    }
}

/// Reconstructs the boot-time `WatermarkLedger` from the lake's manifest
/// record (§6.8, ADR-0010; issue #153) — the module docs' "Watermark
/// reconstruction at boot" bullet explains why this must run before the
/// drain coordinator starts. A never-drained node's lake has no manifests,
/// so this reduces to the empty ledger v0.1 used to hardcode — a strict
/// generalization, not a behavior change for that case.
///
/// # Errors
///
/// [`BootError::Lake`] if the manifest read fails;
/// [`BootError::WatermarkReconstruction`] if the lake's own record is
/// corrupt — boot fails closed rather than guessing (R-3).
fn reconstruct_watermark_ledger(
    committer: &DuckLakeCommitter,
) -> Result<WatermarkLedger, BootError> {
    let manifests = committer.read_manifests()?;
    let reconstruction = WatermarkLedger::reconstruct(manifests, &[], &[])?;
    for stall in &reconstruction.stalls {
        // Not fatal (module docs: temporary conservatism, never a false
        // `complete`, R-9 disclosure) — the watermark simply stands below
        // the stalled window until whatever it is waiting on resolves (a
        // later window, a loss-ledger row at v0.2).
        tracing::warn!(?stall, "watermark stalled below a recorded window at boot");
    }
    for manifest in &reconstruction.deferred {
        tracing::warn!(
            dataset = %manifest.dataset,
            partition = %manifest.partition,
            window = manifest.window_id.0,
            "manifest deferred past a window-id gap at boot (orphan-reconcile candidate, §6.8)"
        );
    }
    Ok(reconstruction.ledger)
}

/// Opens the lake committer and the parts object store (§6.1, §6.4,
/// ADR-0010), and evolves the fixed `otlp_logs` table into existence
/// (module docs of [`ensure_otlp_logs_table`]).
///
/// `catalog.dsn` addresses the metadata catalog `DuckLake` attaches
/// (a `postgres:` DSN, a `sqlite:` path, or a bare path, per
/// `duckspout-lake-ducklake`'s own kind detection); `lake.uri` is the
/// `DATA_PATH` the drain PUTs sealed parts into (§9.6.1 — this module's
/// `lake.*` mapping).
async fn open_lake(
    config: &DaemonConfig,
) -> Result<(Arc<DuckLakeCommitter>, Arc<dyn object_store::ObjectStore>), BootError> {
    std::fs::create_dir_all(&config.lake.uri)?;
    let catalog_uri = catalog_uri_with_secret(&config.catalog.dsn, &config.catalog.password_file)?;
    let committer = Arc::new(DuckLakeCommitter::open(DuckLakeConfig {
        catalog_uri,
        data_path: config.lake.uri.clone(),
        // v0.1 is single-node (SCOPE, issue #38): exactly one process ever
        // commits through this catalog, so the multi-process guard (issue
        // #119) never needs to reject a DuckDB-file catalog — that
        // restriction is replication's (v0.2) concern.
        multi_process: false,
        // v0.1 always drains to local NVMe (§9.1); no daemon config knob
        // requests S3 yet, so the metadata connection never needs one.
        s3: None,
    })?);
    ensure_otlp_logs_table(&(Arc::clone(&committer) as Arc<dyn LakeCommitter>)).await?;
    let parts_store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&config.lake.uri)?,
    );
    Ok((committer, parts_store))
}

/// Binds one TCP listener, wrapping a bind failure with which listener it
/// was (`otlp` or `status`) and the address it tried.
async fn bind_listener(
    what: &'static str,
    port: u16,
) -> Result<(TcpListener, SocketAddr), BootError> {
    let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| BootError::Bind { what, addr, source })?;
    let bound = listener.local_addr().map_err(BootError::Storage)?;
    Ok((listener, bound))
}

/// Builds the `DuckLake` `catalog_uri` from `catalog.dsn`, injecting
/// `catalog.password_file`'s content as `password=…` when the DSN is a
/// libpq-style Postgres DSN (§9.5: secrets are file paths, never inline
/// TOML values) — file-backed and SQLite catalogs read no password
/// (`duckspout-lake-ducklake`'s own `attach_info` reports as much).
fn catalog_uri_with_secret(
    dsn: &str,
    password_file: &std::path::Path,
) -> Result<String, BootError> {
    let is_postgres = dsn.starts_with("postgres:") || dsn.starts_with("postgresql:");
    if !is_postgres {
        return Ok(dsn.to_owned());
    }
    let password = std::fs::read_to_string(password_file)?;
    let password = password.trim();
    if password.is_empty() {
        return Ok(dsn.to_owned());
    }
    Ok(format!("{dsn} password={password}"))
}
