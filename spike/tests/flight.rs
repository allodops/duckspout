//! End-to-end Flight serve-leg tests (§7.4, issue #26): rows land through
//! the ingest core, a real `FlightClient` does the get_flight_info → do_get
//! round trip for a SQL ticket, and the streamed Arrow record batches match
//! the hot table exactly — the DuckDB-Arrow-to-Flight seam, both directions
//! of the wire, no mocks.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_flight::{FlightClient, FlightDescriptor};
use futures::TryStreamExt;
use spike::flight::HotFlightService;
use spike::ingest::{IngestCore, LogRow};

const TABLE: &str = "hot_w0";

/// Open a hot db in `dir`, insert `rows` synthetic rows through the ingest
/// core (one transaction), and return the shared handle the server needs.
fn seeded_core(dir: &std::path::Path, rows: i64) -> Arc<Mutex<IngestCore>> {
    let mut core = IngestCore::open(&dir.join("hot.db")).unwrap();
    core.create_window(TABLE).unwrap();
    let batch: Vec<_> = (0..rows).map(LogRow::synthetic).collect();
    core.insert_batch(TABLE, &batch).unwrap();
    Arc::new(Mutex::new(core))
}

/// In-process Flight server on an ephemeral loopback port; returns a
/// connected client (same shape as the OTLP e2e test).
async fn client_for(core: Arc<Mutex<IngestCore>>) -> FlightClient {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(HotFlightService::new(core).into_server())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightClient::new(channel)
}

/// Full protocol round trip for an aggregate: get_flight_info yields the
/// real result schema and a servable ticket; do_get returns the count the
/// ingest path committed. Would catch a broken descriptor/ticket path, a
/// schema that doesn't survive IPC, or an executor wired to the wrong db.
#[tokio::test]
async fn count_star_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client_for(seeded_core(dir.path(), 1101)).await;

    let sql = format!("SELECT count(*) AS n FROM {TABLE}");
    let t0 = Instant::now();
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(sql))
        .await
        .unwrap();

    // The advertised schema is the query's real output schema.
    let schema = info.clone().try_decode_schema().unwrap();
    assert_eq!(schema.field(0).name(), "n");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);

    // One endpoint, one ticket — serve it.
    assert_eq!(info.endpoint.len(), 1);
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 1101, "count over Flight != rows committed");
    // Aggregate round-trip ballpark: bind (get_flight_info) + execute+stream
    // (do_get), both legs over the real socket (visible with --nocapture).
    eprintln!(
        "flight aggregate ballpark: get_flight_info + do_get in {:.1?}",
        t0.elapsed()
    );
}

/// A full scan streams every committed row with engine-typed columns
/// (BIGINT seq, TIMESTAMP ts, VARCHAR body) intact across the wire. Would
/// catch row loss across batch boundaries, type mangling in the
/// duckdb→arrow→IPC→arrow chain, and ordering-destroying re-encodes.
#[tokio::test]
async fn full_scan_streams_all_rows_typed() {
    const ROWS: i64 = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let mut client = client_for(seeded_core(dir.path(), ROWS)).await;

    let sql = format!("SELECT seq, ts, body FROM {TABLE} ORDER BY seq");
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(sql))
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    let t0 = Instant::now();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let elapsed = t0.elapsed();

    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, ROWS as usize, "rows lost or duplicated in flight");

    // Types survived: engine-side TIMESTAMP arrives as Timestamp(Microsecond).
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(
        matches!(
            schema.field(1).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
        ),
        "ts arrived as {:?}",
        schema.field(1).data_type()
    );

    // Content survived, in order: first row of the first batch and last row
    // of the last batch are the synthetic rows 0 and ROWS-1.
    let first = &batches[0];
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        first
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        LogRow::synthetic(0).ts_micros
    );
    assert_eq!(
        first
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        LogRow::synthetic(0).body
    );
    let last = batches.last().unwrap();
    assert_eq!(
        last.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(last.num_rows() - 1),
        ROWS - 1
    );

    // Latency ballpark for the findings comment (visible with --nocapture).
    eprintln!(
        "flight scan ballpark: {ROWS} rows / {} batches in {elapsed:.1?} ({:.0} rows/s)",
        batches.len(),
        ROWS as f64 / elapsed.as_secs_f64()
    );
}

/// The failure surface is typed gRPC errors, not hangs or empty streams:
/// bad SQL in a ticket and a non-CMD descriptor both come back as
/// InvalidArgument. Would catch a server that panics the connection or
/// silently serves an empty result for garbage input.
#[tokio::test]
async fn bad_inputs_fail_typed() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client_for(seeded_core(dir.path(), 10)).await;

    let err = client
        .do_get(arrow_flight::Ticket::new("SELECT nope FROM nowhere"))
        .await
        .err()
        .expect("bad SQL must error");
    assert!(
        err.to_string().contains("query failed"),
        "unexpected error: {err}"
    );

    let err = client
        .get_flight_info(FlightDescriptor::new_path(vec!["hot_w0".into()]))
        .await
        .err()
        .expect("path descriptor must be rejected");
    assert!(err.to_string().contains("CMD"), "unexpected error: {err}");
}
