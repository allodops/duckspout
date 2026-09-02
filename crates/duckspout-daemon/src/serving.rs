//! The node's Arrow Flight server over the hot store — the **server half**
//! of remote hot (§7.4), guarded per §7.8.
//!
//! Placement, per the design of record: `docs/design/query.md`'s
//! provenance line assigns the Flight server to **the daemon**; the
//! protocol-grade pieces live below it — the guarded scan and dedicated
//! read connections in `duckspout-staging` (`StagingReader`, #114), the
//! fill-scaled budget and ladder vocabulary in `duckspout-types`. This
//! module is the wiring: admission (concurrency), per-query guard
//! computation, error mapping, and the Flight surface itself.
//!
//! # The three §7.8 guards — typed errors, never truncation
//!
//! | Guard | Source | Tripped as |
//! |---|---|---|
//! | Byte budget | `query.max_hot_bytes_per_query`, fill-scaled by the ladder measure (`fill_scaled_budget`) | `RESOURCE_EXHAUSTED` |
//! | Deadline | `query.hot_scan_deadline`, cooperative per-batch check **plus** a hard watchdog on the reader's engine interrupt | `DEADLINE_EXCEEDED` |
//! | Concurrency | `query.max_concurrent_hot_scans`, gated at scan admission | `RESOURCE_EXHAUSTED` |
//!
//! A tripped guard discards the partial result and returns the error —
//! a silently truncated result would be a completeness lie (§7.8).
//! Per-principal queueing (the fixed depth-32 constant) arrives with the
//! §7.9 auth era — v0.1 has no principals, so over-cap scans are refused
//! outright with the retry-shaped error.
//!
//! # The #113 gap, stated (Airport's protocol vocabulary)
//!
//! This surface is **generic Flight**: `get_flight_info`/`get_schema` over
//! a CMD descriptor carrying SQL, `do_get` over a ticket carrying the same
//! SQL. The Airport client (`ATTACH (TYPE AIRPORT)`) does **not** speak
//! this — per spike #26 (issue #113) it expects:
//!
//! - a msgpack(+zstd)-serialized **catalog inventory** (schemas → tables,
//!   each with an Arrow IPC schema), discovered up front;
//! - a fixed **`do_action` vocabulary**: `list_schemas`,
//!   `create_transaction`, `catalog_version`, `endpoints`, …;
//! - **PATH**-shaped `FlightDescriptor`s (`catalog/schema/table`), never
//!   SQL commands; msgpack-encoded tickets;
//! - filter pushdown as a **JSON expression tree** plus column projection
//!   in scan options; `airport-*` gRPC headers; scalar/table functions
//!   over `do_exchange`.
//!
//! What this module delivers is the transport, execution, and guard layer
//! all of that rides on — the IPC encode path, the guarded read
//! connections, the blocking-engine-off-the-reactor shape. The Airport
//! vocabulary grows on top of exactly this skeleton in the #67 (extension
//! era) build-out; each unimplemented endpoint below names its verb.
//!
//! # Read surface honesty
//!
//! Tickets are restricted to `SELECT`/`WITH` shapes: the read connections
//! must never write (#114), and until the Airport vocabulary replaces free
//! SQL with PATH descriptors, the restriction is the server-side guard.
//! §7.9 auth (principal identity, server-side tenant predicates) is the
//! auth-era work and deliberately absent here: v0.1 serving is for the
//! trusted single-tenant perimeter, and the handshake endpoint says so.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_descriptor::DescriptorType,
};
use duckspout_staging::arrow::datatypes::SchemaRef;
use duckspout_staging::arrow::ipc::writer::IpcWriteOptions;
use duckspout_staging::arrow::record_batch::RecordBatch;
use duckspout_staging::{ScanGuards, StagingEngine, StagingError, StagingReader};
use duckspout_types::{Clock, Storage, fill_scaled_budget};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status, Streaming};

/// The serving knobs (§7.8, §9.6) — read from config by the composition
/// layer and passed as values.
#[derive(Debug, Clone, Copy)]
pub struct ServingConfig {
    /// `query.max_hot_bytes_per_query` (default 2 GiB) — the steady-state
    /// per-query byte budget, fill-scaled per §7.8.
    pub max_hot_bytes_per_query: u64,
    /// `query.hot_scan_deadline` (default 30 s), in milliseconds.
    pub hot_scan_deadline_ms: u64,
    /// `query.max_concurrent_hot_scans` (default 8).
    pub max_concurrent_hot_scans: usize,
    /// `hot.max_bytes` — the fill ratio's denominator (§4.5).
    pub hot_max_bytes: u64,
}

/// The hot store's Flight server (module docs). Constructed by the daemon
/// (or a test harness); the listener stays with whoever serves
/// [`Self::into_server`].
pub struct HotFlightService<S: Storage + 'static> {
    engine: Arc<StagingEngine<S>>,
    clock: Arc<dyn Clock>,
    config: ServingConfig,
    scan_permits: Arc<Semaphore>,
    /// Pre-created dedicated read connections (#114), one per permitted
    /// concurrent scan. Created **up front** because cloning a reader takes
    /// the write mutex briefly — done per scan, an open `StageCommit`
    /// transaction would stall new scans behind the write path, exactly the
    /// contention #114 exists to rule out. With the pool, a scan touches
    /// only its checked-out connection; the semaphore guarantees one is
    /// always available to a permit holder.
    readers: Arc<std::sync::Mutex<Vec<StagingReader>>>,
}

impl<S: Storage + 'static> HotFlightService<S> {
    /// Builds the service over the engine's read path, pre-creating one
    /// dedicated read connection per permitted concurrent scan (#114 —
    /// field docs).
    ///
    /// # Errors
    ///
    /// [`StagingError`] if a read connection cannot be cloned.
    pub fn new(
        engine: Arc<StagingEngine<S>>,
        clock: Arc<dyn Clock>,
        config: ServingConfig,
    ) -> Result<Self, StagingError> {
        let readers = (0..config.max_concurrent_hot_scans)
            .map(|_| engine.reader())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            engine,
            clock,
            config,
            scan_permits: Arc::new(Semaphore::new(config.max_concurrent_hot_scans)),
            readers: Arc::new(std::sync::Mutex::new(readers)),
        })
    }

    /// Wraps the service into the tonic server type.
    #[must_use]
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Runs one guarded hot scan (module docs): concurrency admission →
    /// fill-scaled budget + deadline guards → execution on a dedicated
    /// read connection, off the reactor, with the hard-interrupt watchdog
    /// armed.
    async fn run_guarded(&self, sql: String) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        require_select_shape(&sql)?;

        // Guard 3: concurrency, at admission. No principals exist yet
        // (§7.9), so there is no per-principal queue to join — over-cap is
        // the typed refusal.
        let Ok(_permit) = self.scan_permits.try_acquire() else {
            return Err(Status::resource_exhausted(format!(
                "hot-scan concurrency guard tripped: {} scans in flight \
                 (query.max_concurrent_hot_scans; §7.8 — retry, or read the lake)",
                self.config.max_concurrent_hot_scans
            )));
        };

        // Guard 1: the byte budget, fill-scaled by the ladder measure —
        // read lock-free, so guard computation never waits on the write
        // path (#114).
        let guards = ScanGuards {
            max_bytes: fill_scaled_budget(
                self.config.max_hot_bytes_per_query,
                self.engine.staged_bytes(),
                self.config.hot_max_bytes,
            ),
            deadline_nanos: self.config.hot_scan_deadline_ms.saturating_mul(1_000_000),
        };

        // Check out a pre-created dedicated read connection (#114): MVCC
        // snapshot, never the write mutex. The create-on-demand fallback
        // exists only for the rare replacement path below (it can wait
        // briefly on the write mutex; the pre-created pool is the normal,
        // contention-free case).
        let pooled = self
            .readers
            .lock()
            .map_err(|_| Status::internal("reader pool poisoned"))?
            .pop();
        let reader = match pooled {
            Some(reader) => reader,
            None => self
                .engine
                .reader()
                .map_err(|e| Status::internal(format!("read connection: {e}")))?,
        };

        // Guard 2's hard half: the engine interrupt watchdog, for a scan
        // stuck inside a single engine call where the cooperative per-batch
        // check cannot run.
        let interrupt = reader.interrupt_handle();
        let fired = Arc::new(AtomicBool::new(false));
        let watchdog_fired = Arc::clone(&fired);
        let deadline_ms = self.config.hot_scan_deadline_ms;
        let watchdog = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(deadline_ms)).await;
            watchdog_fired.store(true, Ordering::SeqCst);
            interrupt.interrupt();
        });

        let clock = Arc::clone(&self.clock);
        let (reader, result) = tokio::task::spawn_blocking(move || {
            let result = reader.query_arrow_guarded(&sql, clock.as_ref(), &guards);
            (reader, result)
        })
        .await
        .map_err(|e| Status::internal(format!("scan task: {e}")))?;
        watchdog.abort();
        // Return the connection before releasing the permit — unless the
        // watchdog fired: an interrupted connection may hold a latched
        // interrupt that would kill its next statement, so it is replaced
        // (best-effort; a failed replacement falls back to the checkout's
        // create-on-demand path).
        let returned = if fired.load(Ordering::SeqCst) {
            drop(reader);
            self.engine.reader().ok()
        } else {
            Some(reader)
        };
        if let Some(reader) = returned {
            self.readers
                .lock()
                .map_err(|_| Status::internal("reader pool poisoned"))?
                .push(reader);
        }

        match result {
            Ok(out) => Ok(out),
            Err(error @ StagingError::ScanBudgetExceeded { .. }) => {
                Err(Status::resource_exhausted(error.to_string()))
            }
            Err(error @ StagingError::ScanDeadlineExceeded { .. }) => {
                Err(Status::deadline_exceeded(error.to_string()))
            }
            // The watchdog killed the statement mid-call: same guard, same
            // typed shape as the cooperative check.
            Err(_) if fired.load(Ordering::SeqCst) => Err(Status::deadline_exceeded(format!(
                "scan aborted: deadline of {deadline_ms} ms exceeded via engine interrupt \
                 (§7.8 — narrow the range, raise query.hot_scan_deadline, or read the lake)"
            ))),
            Err(error) => Err(Status::invalid_argument(format!("query failed: {error}"))),
        }
    }

    /// Result schema without materializing rows — the LIMIT-0 bind (#114's
    /// duckdb-rs finding: the result schema exists only after execution).
    async fn schema_of(&self, sql: &str) -> Result<SchemaRef, Status> {
        let (schema, _) = self
            .run_guarded(format!("SELECT * FROM ({sql}) AS q LIMIT 0"))
            .await?;
        Ok(schema)
    }
}

/// The read-surface shape guard (module docs): tickets execute on read
/// connections that must never write (#114), so only `SELECT`/`WITH`
/// shapes are served until the Airport PATH vocabulary replaces free SQL.
fn require_select_shape(sql: &str) -> Result<(), Status> {
    let head = sql.trim_start().get(..6).unwrap_or("").to_ascii_uppercase();
    if head.starts_with("SELECT") || head.starts_with("WITH") {
        return Ok(());
    }
    Err(Status::invalid_argument(
        "only SELECT/WITH queries are served on the hot read surface (#114; \
         the Airport catalog vocabulary replaces free SQL — issue #113)",
    ))
}

/// Extract the SQL string a CMD descriptor / ticket carries.
fn sql_utf8(bytes: &[u8]) -> Result<String, Status> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|e| Status::invalid_argument(format!("ticket/cmd is not UTF-8 SQL: {e}")))
}

#[tonic::async_trait]
impl<S: Storage + 'static> FlightService for HotFlightService<S> {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    /// CMD descriptor carrying SQL → `FlightInfo`: IPC result schema + one
    /// endpoint whose ticket is that SQL (single node; §7.2's
    /// one-holder-per-partition rule keeps fan-out out even later).
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        if descriptor.r#type != DescriptorType::Cmd as i32 {
            return Err(Status::invalid_argument(
                "only CMD descriptors (SQL) are served; PATH descriptors are \
                 Airport-vocabulary work (issue #113)",
            ));
        }
        let sql = sql_utf8(&descriptor.cmd)?;
        let schema = self.schema_of(&sql).await?;
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(sql.into_bytes())))
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let sql = sql_utf8(&descriptor.cmd)?;
        let schema = self.schema_of(&sql).await?;
        let result: SchemaResult = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e: duckspout_staging::arrow::error::ArrowError| {
                Status::internal(format!("schema encode: {e}"))
            })?;
        Ok(Response::new(result))
    }

    /// Ticket (SQL) → guarded execution → encoded record-batch stream.
    /// Collect-then-stream: the guards bound the collected size (budget)
    /// and time (deadline), and collecting releases the scan permit before
    /// network streaming begins — the guards protect ingest from scans,
    /// not from slow clients.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = sql_utf8(&ticket.ticket)?;
        let (schema, batches) = self.run_guarded(sql).await?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema) // explicit: an empty result still carries its schema
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(format!("flight encode: {e}")));
        Ok(Response::new(stream.boxed()))
    }

    // ---- the #113 Airport-vocabulary gap, endpoint by endpoint ----------

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "no authentication yet: §7.9 (principal identity, server-side tenant \
             predicates) is the auth-era work; v0.1 serving is the trusted perimeter",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "catalog enumeration is Airport do_action vocabulary (list_schemas, \
             catalog_version — issue #113)",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "no long-running queries: the §7.8 deadline bounds every hot scan",
        ))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "ingest is OTLP-only (§4.1); Flight is a read surface",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "Airport scalar/table functions ride do_exchange (issue #113)",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "Airport's do_action vocabulary (list_schemas, create_transaction, \
             catalog_version, endpoints, …) is issue #113's build-out",
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "no action vocabulary yet (issue #113)",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::require_select_shape;

    #[test]
    fn only_select_shapes_pass_the_read_surface_guard() {
        assert!(require_select_shape("SELECT 1").is_ok());
        assert!(require_select_shape("  with x as (select 1) select * from x").is_ok());
        for rejected in [
            "DROP TABLE s_logs__t_2e0__w0",
            "INSERT INTO duckspout_dedup VALUES (1)",
            "UPDATE duckspout_applied SET applied_seq = 0",
            "CHECKPOINT",
            "",
        ] {
            assert!(require_select_shape(rejected).is_err(), "{rejected:?}");
        }
    }
}
