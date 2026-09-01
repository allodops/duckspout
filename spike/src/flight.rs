//! Spike Flight serve leg (§7.4) — issue #26.
//!
//! DuckSpout builds only the **server half** of remote hot: every node
//! serves its hot tables over Arrow Flight; the client half is the Airport
//! DuckDB extension (`ATTACH (TYPE AIRPORT)`), driven internally by the
//! catalog extension at bind (docs/design/query.md §7.4). This module
//! prototypes the server: a `FlightService` whose ticket carries a SQL
//! string, executed against the spike's hot DuckDB, the result streamed
//! back as Arrow record batches — duckdb's native Arrow output feeding
//! arrow-flight's IPC encoder directly, no transposition layer.
//!
//! What round-trips here (the seam under test): `get_flight_info` with a
//! CMD descriptor holding SQL → `FlightInfo` (IPC-encoded result schema +
//! one endpoint whose ticket is that SQL) → `do_get` with the ticket →
//! encoded record-batch stream. `get_schema` serves the schema alone.
//!
//! **The Airport gap** (a finding, not an oversight — Airport itself is
//! deliberately out of scope for the spike): Airport does not speak
//! "ticket = SQL". Per its protocol, `ATTACH (TYPE AIRPORT)` expects a
//! msgpack(+zstd)-serialized catalog inventory (schemas → tables, each
//! with an Arrow IPC schema) discovered up front, `do_action` verbs in a
//! fixed vocabulary (`list_schemas`, `create_transaction`,
//! `catalog_version`, `endpoints`, ...), PATH-shaped `FlightDescriptor`s
//! (`catalog/schema/table`) rather than SQL commands, msgpack-encoded
//! tickets, filter pushdown as a JSON expression tree plus column
//! projection in scan options, and `airport-*` gRPC headers. None of that
//! exists here. What DOES carry over unchanged: the transport (gRPC/
//! Flight), the IPC encoding path (`FlightDataEncoderBuilder` over
//! engine-produced batches), and the blocking-engine-off-the-reactor
//! shape. The real server grows Airport's vocabulary on top of exactly
//! this skeleton.
//!
//! Throwaway spike code — instructive, not production (spike/README.md).

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_descriptor::DescriptorType,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::ingest::IngestCore;

/// Flight server over the hot database. Shares the daemon's single
/// connection (DuckDB is single-writer per file — the ingesting process
/// owns the lock, so serving MUST go through it; §7.4 "local hot is also
/// served via Flight" exists precisely because of this lock).
pub struct HotFlightService {
    core: Arc<Mutex<IngestCore>>,
}

impl HotFlightService {
    pub fn new(core: Arc<Mutex<IngestCore>>) -> Self {
        Self { core }
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Run `sql` off the async reactor (engine calls are blocking) and hand
    /// back duckdb's Arrow output as-is.
    async fn run_query(&self, sql: String) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        let core = Arc::clone(&self.core);
        tokio::task::spawn_blocking(move || {
            let core = core.lock().expect("core poisoned");
            core.query_arrow(&sql)
        })
        .await
        .map_err(|e| Status::internal(format!("query task: {e}")))?
        .map_err(|e| Status::invalid_argument(format!("query failed: {e:#}")))
    }

    /// Result schema without materializing rows: bind the query wrapped in
    /// `LIMIT 0` (spike-grade — restricts tickets to SELECT shapes, fine
    /// here; the real server binds via the engine's prepare surface).
    async fn schema_of(&self, sql: &str) -> Result<SchemaRef, Status> {
        let (schema, _) = self
            .run_query(format!("SELECT * FROM ({sql}) AS q LIMIT 0"))
            .await?;
        Ok(schema)
    }
}

/// Extract the SQL string a CMD descriptor / ticket carries.
fn sql_utf8(bytes: &[u8]) -> Result<String, Status> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|e| Status::invalid_argument(format!("ticket/cmd is not UTF-8 SQL: {e}")))
}

#[tonic::async_trait]
impl FlightService for HotFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    /// CMD descriptor carrying SQL → FlightInfo: IPC result schema + one
    /// endpoint whose ticket is the same SQL (single node — no fan-out; the
    /// one-holder-per-partition rule of §7.2 keeps it that way even later).
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        if descriptor.r#type != DescriptorType::Cmd as i32 {
            return Err(Status::invalid_argument(
                "only CMD descriptors (SQL) are served",
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
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?;
        Ok(Response::new(result))
    }

    /// Ticket (SQL) → executed result as an encoded record-batch stream.
    /// Collect-then-stream: the spike materializes the full result before
    /// encoding, because incremental streaming would pin the single shared
    /// connection for the stream's lifetime (a daemon-design question —
    /// dedicated read connection/cursor — recorded as a finding, not solved
    /// here).
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = sql_utf8(&ticket.ticket)?;
        let (schema, batches) = self.run_query(sql).await?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema) // explicit: an empty result still carries its schema
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(format!("flight encode: {e}")));
        Ok(Response::new(stream.boxed()))
    }

    // ---- everything below is deliberately unserved in the spike ----------
    // Airport's ATTACH drives its catalog through do_action verbs and its
    // pushdown through do_exchange (module doc); those are the named gap.

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "spike: no handshake/auth (§7.9 later)",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "spike: no catalog enumeration (Airport lists via do_action)",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("spike: no long-running queries"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "spike: ingest is OTLP-only (§4.1); Flight is a read surface",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "spike: no do_exchange (Airport scalar/table functions ride here)",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "spike: no action vocabulary (Airport's list_schemas etc. — the gap)",
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("spike: no action vocabulary"))
    }
}
