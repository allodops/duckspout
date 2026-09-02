//! The full-daemon smoke test (issue #38): boot [`duckspout_daemon::wiring::Daemon`]
//! through its public API (never the binary), send OTLP logs over a real
//! gRPC client, force a real drain cycle, and verify the watermark advanced,
//! the lake actually holds the rows, and the disclosed status endpoint
//! agrees — end to end, over every piece #38 wires together.
//!
//! This is the first test that runs staging, drain, `DuckLake`, the
//! watermark ledger, **and** Flight serving together as the daemon composes
//! them (rather than each protocol crate's own unit/property suite,
//! `tests/otlp_e2e.rs`'s staging-only composition, or `tests/flight_e2e.rs`'s
//! `HotFlightService`-in-isolation harness) — treated accordingly: thorough,
//! not a smoke puff.
//!
//! # Querying the drained data back: two independent proofs
//!
//! This test's original cut (issue #38, before #151 merged) could not query
//! Flight — there was no `FlightService` on `main` yet — so it substituted
//! reading the drained rows straight out of the `DuckLake` catalog with a
//! raw `DuckDB` connection. That substitute stays: it is still the only
//! proof that data survived the full accept→stage→drain→commit path into
//! the lake, one layer further down the pipeline than Flight reaches (hot
//! data is dropped from staging once its window drains — `DropWindow`,
//! `duckspout-drain`'s coordinator docs — so Flight cannot see window 0
//! after this test's drain tick commits it).
//!
//! What is new: a **real Flight round-trip** (issue #39/#151, wired into
//! `Daemon::boot`/`serve` by this branch) against window 1 — the second
//! window this test opens by rolling past `hot.window = "1s"`, which stays
//! open (undrained, still in the hot tier) for the whole test. A real
//! `FlightServiceClient` binds the window's schema through `get_flight_info`
//! and streams its rows through `do_get`, proving the daemon's bound Flight
//! listener serves the exact rows the OTLP client just ingested — the proof
//! the lake read-back cannot give, since it never sees hot-only data.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use duckspout_daemon::wiring::Daemon;
use duckspout_staging::arrow::array::{Array as _, AsArray as _};
use duckspout_staging::arrow::datatypes::DataType;
use duckspout_types::{DatasetId, PartitionId, TenantId, WindowId};
use futures::TryStreamExt as _;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// Writes a minimal §9.6 config file rooted at `root` (a fresh temp dir) and
/// loads it through the real [`duckspout_daemon::config::load`] path — the
/// test exercises config loading too, not just the composition it feeds.
/// `hot.window = "1s"` and `drain.allowed_lateness = "0s"` are deliberately
/// far below their §9.6.1 production defaults so a real window roll and a
/// real drain-eligibility check both happen inside a test's patience, over
/// the real [`std::time::SystemTime`]/[`std::time::Instant`]-backed
/// [`duckspout_daemon::system::SystemClock`] the daemon actually runs on
/// (module docs: no clock is injected — this is the real one).
fn write_config(root: &std::path::Path) -> PathBuf {
    let hot_dir = root.join("hot");
    let catalog_path = root.join("catalog.ducklake");
    let data_path = root.join("lake-data");
    std::fs::create_dir_all(&hot_dir).unwrap();

    let toml = format!(
        r#"
[node]
data_dir = "{hot_dir}"
otlp_listen = 0
flight_listen = 0
peer_listen = 0

[catalog]
dsn = "{catalog_path}"
password_file = "{unused}"

[tls]
mode = "disabled"
cert = "{unused}"
key = "{unused}"
ca = "{unused}"

[lake]
uri = "{data_path}"

[hot]
window = "1s"
max_bytes = 1000000000

[drain]
allowed_lateness = "0s"

[admission]
max_inflight_bytes = 67108864
"#,
        hot_dir = hot_dir.display(),
        catalog_path = catalog_path.display(),
        data_path = data_path.display(),
        unused = root.join("unused").display(),
    );
    let config_path = root.join("daemon.toml");
    std::fs::write(&config_path, toml).unwrap();
    config_path
}

fn now_unix_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

/// Builds one OTLP export request of `n` log records, `base_nanos +
/// 0..n` apart — matches `tests/otlp_e2e.rs`'s synthetic-request shape.
fn synthetic_request(n: u64, base_nanos: u64) -> ExportLogsServiceRequest {
    let str_attr = |k: &str, v: &str| KeyValue {
        key: k.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(v.to_owned())),
        }),
        ..Default::default()
    };
    let records = (0..n)
        .map(|i| LogRecord {
            time_unix_nano: base_nanos + i,
            severity_number: 9,
            severity_text: "INFO".to_owned(),
            body: Some(AnyValue {
                value: Some(PbValue::StringValue(format!("e2e-boot log {i}"))),
            }),
            attributes: vec![str_attr("k8s.pod.name", &format!("pod-{}", i % 2))],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "e2e-boot")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// One GET against the daemon's `/status` endpoint, over a plain blocking
/// TCP request — the exact shape `duckspoutctl status` uses
/// (`crates/duckspout-ctl/src/main.rs`), exercised here as raw bytes so the
/// test proves the wire transport, not just the in-process
/// [`duckspout_daemon::wiring::DaemonHandle::status`] computation.
async fn fetch_status_json(addr: std::net::SocketAddr) -> serde_json::Value {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /status HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let body = raw.split_once("\r\n\r\n").unwrap().1;
    serde_json::from_str(body).unwrap()
}

/// Row count of the drained `otlp_logs` table, read with a fresh `DuckDB`
/// connection attached to the same `DuckLake` catalog — the "query it back"
/// substitute (module docs).
fn drained_row_count(catalog_path: &std::path::Path, data_path: &std::path::Path) -> i64 {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .unwrap();
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:{}' AS verify (DATA_PATH '{}');",
        catalog_path.display(),
        data_path.display()
    ))
    .unwrap();
    // `duckspout-lake-ducklake`'s private `dataset_table` encoding: `ds_` +
    // every byte outside `[a-z0-9]` (including `_`) hex-escaped — so
    // "otlp_logs" becomes "ds_otlp_5flogs" (`_` is `0x5f`), not the more
    // readable "ds_otlp_logs" a naive reading of the encoding might expect.
    conn.query_row("SELECT count(*) FROM verify.ds_otlp_5flogs", [], |row| {
        row.get(0)
    })
    .unwrap()
}

/// One `do_get` round-trip: ticket in, decoded `RecordBatch`es out — the
/// same helper shape `tests/flight_e2e.rs` uses against `HotFlightService`
/// directly, exercised here over the real daemon-bound listener instead.
async fn flight_query_rows(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<duckspout_staging::arrow::record_batch::RecordBatch> {
    let stream = client
        .do_get(Ticket::new(sql.to_owned().into_bytes()))
        .await
        .unwrap()
        .into_inner();
    FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|s| FlightError::Tonic(Box::new(s))),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap()
}

/// The Flight round-trip proof (module docs): `get_flight_info` binds the
/// result schema and hands back a ticket carrying the same SQL, `do_get`
/// streams the rows, and the decoded rows match what the OTLP client
/// ingested into `window1_table` — the still-hot window Flight can see that
/// the lake read-back cannot.
async fn assert_flight_serves_hot_window(flight_addr: std::net::SocketAddr, window1_table: &str) {
    let flight_channel = tonic::transport::Endpoint::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect to the real Flight listener");
    let mut flight_client = FlightServiceClient::new(flight_channel);

    let sql = format!("SELECT body FROM {window1_table} ORDER BY body");
    let info = flight_client
        .get_flight_info(FlightDescriptor::new_cmd(sql.clone().into_bytes()))
        .await
        .expect("get_flight_info against the hot window")
        .into_inner();
    let ticket = info.endpoint[0]
        .ticket
        .clone()
        .expect("one endpoint ticket");
    assert_eq!(ticket.ticket, bytes::Bytes::from(sql.clone().into_bytes()));
    let schema = info.try_decode_schema().unwrap();
    assert_eq!(schema.fields().len(), 1, "bound schema: body only");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);

    let batches = flight_query_rows(&mut flight_client, &sql).await;
    let bodies: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch.column(0).as_string::<i32>();
            (0..col.len()).map(move |i| col.value(i).to_owned())
        })
        .collect();
    assert_eq!(
        bodies,
        vec!["e2e-boot log 0", "e2e-boot log 1"],
        "Flight served exactly window 1's 2 still-hot rows, matching what was ingested"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn otlp_to_lake_end_to_end_through_the_public_daemon_api() {
    let root = tempfile::tempdir().unwrap();
    let config_path = write_config(root.path());
    let config = duckspout_daemon::config::load(Some(&config_path)).unwrap();
    let catalog_path = root.path().join("catalog.ducklake");
    let data_path = root.path().join("lake-data");

    let _ = tracing_subscriber::fmt::try_init();

    // --- Boot through the public API (never the binary, per the task) ---
    let daemon = Daemon::boot(&config, 0).await.expect("daemon boots");
    let handle = daemon.handle();
    let otlp_addr = daemon.otlp_addr();
    let status_addr = daemon.status_addr();
    let flight_addr = daemon.flight_addr();
    assert_eq!(handle.node_id().as_str().rsplit('/').next(), Some("1"));

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(daemon.serve(async {
        let _ = shutdown_rx.await;
    }));

    // Booted, not yet serving readiness=false until `serve` flips it — poll
    // status until ready (serve starts async; a handful of retries is
    // plenty on a loopback bind).
    let mut ready = false;
    for _ in 0..50 {
        if handle.status().ready {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ready, "daemon did not report ready in time");

    // --- Send OTLP logs via a real gRPC client (§4.1) ---
    let mut client = LogsServiceClient::connect(format!("http://{otlp_addr}"))
        .await
        .expect("connect to the real OTLP listener");
    let window0_base = now_unix_nanos();
    let mut request = tonic::Request::new(synthetic_request(7, window0_base));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());
    let resp = client.export(request).await.unwrap().into_inner();
    assert!(resp.partial_success.is_none(), "full-batch ack");

    // Nothing eligible yet: window 0 is still open (hasn't rolled).
    let tick = handle.drain_once().await;
    assert_eq!(tick.eligible, 0, "the open window is never offered");

    // Roll the window: `hot.window = 1s`, so waiting past it and appending
    // again allocates window 1, which is what makes window 0 *closed*
    // (`EngineStager`'s roll-on-append rule — `wiring.rs`'s
    // `note_closed_windows` docs).
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let mut request = tonic::Request::new(synthetic_request(2, now_unix_nanos()));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());
    client.export(request).await.unwrap();

    // --- Drain: window 0 should now be eligible and commit ---
    let tick = handle.drain_once().await;
    assert_eq!(
        tick.eligible, 1,
        "window 0 closed and its lateness hold is 0s"
    );
    assert_eq!(tick.committed, 1, "the one eligible window committed");
    assert_eq!(tick.requeued, 0);

    // --- Watermark advanced (§6.8) ---
    let status = handle.status();
    assert_eq!(status.watermarks.len(), 1, "one partition drained");
    let watermark = &status.watermarks[0];
    let expected_min_ms = i64::try_from(window0_base / 1_000_000).unwrap();
    assert!(
        watermark.complete_through_ms >= expected_min_ms,
        "watermark {} should cover window 0's event time {expected_min_ms}",
        watermark.complete_through_ms
    );
    assert!(!status.drain_stalled, "a local file catalog never stalls");
    assert_eq!(
        status.status.overload,
        duckspout_types::OverloadStatus::Normal,
        "7 rows is nowhere near hot.max_bytes"
    );

    // --- The disclosed HTTP endpoint agrees with the in-process view (§9.3, R-9) ---
    let wire_status = fetch_status_json(status_addr).await;
    assert_eq!(wire_status["ready"], true);
    assert_eq!(wire_status["drain_stalled"], false);
    assert_eq!(wire_status["status"]["overload"], "normal");
    assert_eq!(
        wire_status["watermarks"][0]["partition"],
        watermark.partition.as_str()
    );
    assert_eq!(
        wire_status["watermarks"][0]["complete_through_ms"],
        watermark.complete_through_ms
    );

    // --- Query the still-hot window back over a real Flight client (§7.4) ---
    // Window 0 (7 rows) just drained and is gone from staging (`DropWindow`,
    // module docs); window 1 (the 2 rows that rolled it) is still open and
    // hot, and is exactly what remote-hot querying is for — read the data a
    // node is currently ingesting, not yet in the lake.
    let dataset = DatasetId::new(duckspout_accept::otlp::OTLP_LOGS_DATASET);
    let partition = PartitionId::from_tenant_shard(&TenantId::new("tenant-a"), 0);
    let window1_table =
        duckspout_staging::naming::window_table_name(&dataset, &partition, WindowId(1));
    assert_flight_serves_hot_window(flight_addr, &window1_table).await;

    // --- SIGTERM choreography: readiness false, finish, exit (§9.1.2) ---
    shutdown_tx.send(()).unwrap();
    serve_task.await.unwrap();
    // Release every remaining `Arc<DaemonCore>` (and with it the
    // `DuckLakeCommitter`'s file-backed catalog connection) before opening
    // a second connection to the same catalog file below.
    drop(handle);

    // --- Query the drained data back (substituted step, module docs) ---
    let rows = drained_row_count(&catalog_path, &data_path);
    assert_eq!(rows, 7, "exactly window 0's 7 rows landed in the lake");
}
