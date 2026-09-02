//! End-to-end Flight serving (§7.4, §7.8, issue #39): a real
//! `FlightServiceClient` against the real WAL=hot engine — schema bind,
//! guarded execution, each §7.8 guard tripping as its typed error, and the
//! #114 non-contention proof at the serving layer (a scan completing while
//! a write transaction is open).

use std::path::PathBuf;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use bytes::Bytes;
use duckspout_daemon::{HotFlightService, ServingConfig, StdClock};
use duckspout_staging::arrow::array::AsArray as _;
use duckspout_staging::arrow::record_batch::RecordBatch;
use duckspout_staging::{StagingConfig, StagingEngine};
use duckspout_types::{
    BoxFuture, DatasetId, NodeId, PartitionId, Storage, StorageError, StoragePath, WindowId,
};
use futures::TryStreamExt as _;

/// A real-filesystem Storage rooted at the hot dir (test-local; the CTK
/// double is out of reach only for protocol crates — here it is simply the
/// right fidelity).
struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    fn resolve(&self, path: &StoragePath) -> PathBuf {
        if path.as_str().is_empty() {
            self.root.clone()
        } else {
            self.root.join(path.as_str())
        }
    }

    fn ready<T: Send + 'static>(
        result: Result<T, StorageError>,
    ) -> BoxFuture<'static, Result<T, StorageError>> {
        Box::pin(async move { result })
    }
}

impl Storage for FsStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::write(self.resolve(&path), &data)
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        Self::ready(
            std::fs::read(self.resolve(&path))
                .map(Bytes::from)
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::remove_file(self.resolve(&path))
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::File::open(self.resolve(&path))
                .and_then(|f| f.sync_all())
                .map_err(|_| StorageError::FsyncFailed(path.clone())),
        )
    }

    fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::File::open(self.resolve(&dir))
                .and_then(|f| f.sync_all())
                .map_err(|_| StorageError::FsyncFailed(dir.clone())),
        )
    }
}

fn generous() -> ServingConfig {
    ServingConfig {
        max_hot_bytes_per_query: 2 * 1024 * 1024 * 1024,
        hot_scan_deadline_ms: 30_000,
        max_concurrent_hot_scans: 8,
        hot_max_bytes: u64::MAX,
    }
}

/// A synthetic log-shaped batch (ts, severity, body).
fn log_batch(rows: usize) -> RecordBatch {
    use duckspout_staging::arrow::array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use duckspout_staging::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("severity", DataType::Int32, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ts: TimestampMicrosecondArray = (0..rows)
        .map(|i| Some(1_000_000 + i64::try_from(i).unwrap()))
        .collect();
    let severity: Int32Array = (0..rows)
        .map(|i| Some(i32::try_from(i % 24).unwrap()))
        .collect();
    let body: StringArray = (0..rows)
        .map(|i| Some(format!("flight line {i}")))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(ts), Arc::new(severity), Arc::new(body)],
    )
    .unwrap()
}

struct Harness {
    engine: Arc<StagingEngine<FsStorage>>,
    client: FlightServiceClient<tonic::transport::Channel>,
    table: String,
}

/// Opens an engine with `rows` committed into window 0 and serves it over
/// Flight on a loopback port.
async fn harness(dir: &std::path::Path, rows: usize, config: ServingConfig) -> Harness {
    let engine = Arc::new(
        StagingEngine::open(
            StagingConfig {
                hot_dir: dir.to_path_buf(),
                origin: NodeId::new("node-a/1"),
            },
            FsStorage {
                root: dir.to_path_buf(),
            },
        )
        .unwrap(),
    );
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::new("t.0");
    if rows > 0 {
        let mut txn = engine.begin().unwrap();
        txn.append(&dataset, &partition, WindowId(0), &log_batch(rows))
            .unwrap();
        txn.commit().unwrap();
    }
    let table = duckspout_staging::naming::window_table_name(&dataset, &partition, WindowId(0));

    let service = HotFlightService::new(Arc::clone(&engine), Arc::new(StdClock::new()), config)
        .unwrap()
        .into_server();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = FlightServiceClient::new(channel);
    Harness {
        engine,
        client,
        table,
    }
}

async fn do_get_rows(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let stream = client
        .do_get(Ticket::new(sql.to_owned().into_bytes()))
        .await?
        .into_inner();
    FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|s| FlightError::Tonic(Box::new(s))),
    )
    .try_collect::<Vec<_>>()
    .await
    .map_err(|e| match e {
        FlightError::Tonic(status) => *status,
        other => tonic::Status::internal(other.to_string()),
    })
}

/// The full §7.4 round-trip: `get_flight_info` binds the result schema
/// without materializing rows and hands back the ticket; `do_get` streams
/// the batches; the decoded rows match what was staged. Would catch a
/// broken LIMIT-0 bind, IPC encode drift, or a ticket that does not
/// round-trip.
#[tokio::test(flavor = "multi_thread")]
async fn flight_round_trip_serves_the_hot_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(dir.path(), 100, generous()).await;
    let sql = format!("SELECT body, seq FROM {} ORDER BY seq", h.table);

    let info = h
        .client
        .get_flight_info(FlightDescriptor::new_cmd(sql.clone().into_bytes()))
        .await
        .unwrap()
        .into_inner();
    let ticket = info.endpoint[0]
        .ticket
        .clone()
        .expect("one endpoint ticket");
    let schema = info.try_decode_schema().unwrap();
    assert_eq!(schema.fields().len(), 2, "bound schema: body, seq");
    assert_eq!(ticket.ticket, Bytes::from(sql.clone().into_bytes()));

    let batches = do_get_rows(&mut h.client, &sql).await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 100);
    let first = &batches[0];
    assert_eq!(first.column(0).as_string::<i32>().value(0), "flight line 0");
}

/// #114 at the serving layer: with a write transaction OPEN (the write
/// mutex held, rows appended but uncommitted), a Flight scan completes and
/// sees only committed state; after commit, a new scan sees the new rows.
/// Would catch a serving path that touches the write connection or its
/// mutex (this test would hang), or a dirty read.
#[tokio::test(flavor = "multi_thread")]
async fn scans_do_not_contend_with_an_open_write_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(dir.path(), 10, generous()).await;
    let sql = format!("SELECT count(*) AS n FROM {}", h.table);

    // Hold a write transaction with uncommitted rows across the scan.
    let engine = Arc::clone(&h.engine);
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::new("t.0");
    let mut txn = engine.begin().unwrap();
    txn.append(&dataset, &partition, WindowId(0), &log_batch(5))
        .unwrap();

    let batches = do_get_rows(&mut h.client, &sql).await.unwrap();
    let n = batches[0]
        .column(0)
        .as_primitive::<duckspout_staging::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(n, 10, "the scan sees only committed state (MVCC)");

    txn.commit().unwrap();
    let batches = do_get_rows(&mut h.client, &sql).await.unwrap();
    let n = batches[0]
        .column(0)
        .as_primitive::<duckspout_staging::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(n, 15, "after commit the new rows are served");
}

/// Guard 1 (§7.8): the fill-scaled byte budget trips as `RESOURCE_EXHAUSTED`
/// naming the budget — never a truncated result.
#[tokio::test(flavor = "multi_thread")]
async fn byte_budget_guard_trips_as_resource_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(
        dir.path(),
        500,
        ServingConfig {
            max_hot_bytes_per_query: 1,
            ..generous()
        },
    )
    .await;
    let sql = format!("SELECT * FROM {}", h.table);
    let status = do_get_rows(&mut h.client, &sql).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(
        status.message().contains("budget"),
        "the error names the guard: {status:?}"
    );
}

/// Guard 2 (§7.8): the deadline trips as `DEADLINE_EXCEEDED` (cooperative
/// per-batch check and/or the engine-interrupt watchdog — both map to the
/// same typed shape).
#[tokio::test(flavor = "multi_thread")]
async fn deadline_guard_trips_as_deadline_exceeded() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(
        dir.path(),
        500,
        ServingConfig {
            hot_scan_deadline_ms: 0,
            ..generous()
        },
    )
    .await;
    let sql = format!("SELECT * FROM {}", h.table);
    let status = do_get_rows(&mut h.client, &sql).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
}

/// Guard 3 (§7.8): the concurrency cap trips as `RESOURCE_EXHAUSTED` naming
/// the guard (zero permits makes the trip deterministic).
#[tokio::test(flavor = "multi_thread")]
async fn concurrency_guard_trips_as_resource_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(
        dir.path(),
        10,
        ServingConfig {
            max_concurrent_hot_scans: 0,
            ..generous()
        },
    )
    .await;
    let sql = format!("SELECT * FROM {}", h.table);
    let status = do_get_rows(&mut h.client, &sql).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("concurrency"), "{status:?}");
}

/// The read-surface shape guard: non-SELECT tickets are rejected before
/// touching the engine (#114's read-only discipline; the Airport PATH
/// vocabulary replaces free SQL — #113).
#[tokio::test(flavor = "multi_thread")]
async fn write_shaped_tickets_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(dir.path(), 10, generous()).await;
    let status = do_get_rows(&mut h.client, &format!("DROP TABLE {}", h.table))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    // The table is still there.
    let batches = do_get_rows(&mut h.client, &format!("SELECT count(*) FROM {}", h.table))
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<duckspout_staging::arrow::datatypes::Int64Type>()
            .value(0),
        10
    );
}

/// The #113 gap is explicit on the wire: Airport's verbs answer
/// `UNIMPLEMENTED` naming the issue, not `UNKNOWN`.
#[tokio::test(flavor = "multi_thread")]
async fn airport_vocabulary_is_named_unimplemented() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = harness(dir.path(), 0, generous()).await;
    let Err(status) = h
        .client
        .do_action(arrow_flight::Action::new("list_schemas", Bytes::new()))
        .await
    else {
        panic!("do_action is the #113 gap and must be unimplemented");
    };
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert!(status.message().contains("113"), "{status:?}");
}
