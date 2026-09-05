//! The per-PR 1M-record ingest smoke test (§8.6; ledger row `smoke`, issue
//! #47): pushes 1,000,000 synthetic OTLP log records through the real
//! daemon — the same public-API boot `tests/e2e_boot.rs` uses, at volume —
//! drains every window the ingest opened, and verifies the lake holds
//! exactly the row count sent. `scripts/smoke.mjs` runs this test via
//! `cargo test --release`, times the whole run, and compares the elapsed
//! time against `floors/smoke-bound.toml`'s measured bound — the "order of
//! magnitude regressions" check §8.6 asks for (never wall-clock as a *per-PR
//! gate* threshold on its own — that's ADR-0005's instr-gate territory —
//! this is the one §8.6 explicitly carves out as a coarse volume bound, not
//! a latency claim).
//!
//! `hot.window = "5s"` (vs. `e2e_boot.rs`'s `"1s"`): a 1M-record ingest
//! takes tens of seconds, and a 1-second window would multiply the number
//! of `SealPart → PutPart → LakeCommit` cycles (and therefore wall time)
//! well past what this test needs to prove — real window rolling under
//! real load, not the maximum number of drains a test patience can afford.
//! `drain.allowed_lateness = "0s"` keeps every closed window immediately
//! eligible, as in `e2e_boot.rs`.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use duckspout_daemon::wiring::Daemon;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// Total synthetic records pushed — §8.6's "1M-record" bound, exactly.
const TOTAL_ROWS: u64 = 1_000_000;
/// Rows per OTLP export request — keeps each request's encoded size well
/// under tonic's default 4 MiB decode limit (a few hundred KB here) while
/// keeping the request count (`TOTAL_ROWS / BATCH_ROWS`) modest.
const BATCH_ROWS: u64 = 2_000;
/// Concurrent in-flight requests — `LogsServiceClient` is `Clone` and
/// multiplexes over one HTTP/2 connection, so this overlaps encode/decode
/// and network round-trips across many requests instead of paying them
/// serially; bounded so the test does not itself become a thundering herd
/// (`admission.max_inflight_bytes` below is generous enough for this
/// concurrency at this batch size).
const CONCURRENCY: usize = 32;

/// Same shape as `e2e_boot.rs`'s `write_config`, differing only in
/// `hot.window` (module docs) and `admission.max_inflight_bytes` (raised
/// for this test's concurrent send pattern).
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
window = "5s"
max_bytes = 4000000000

[drain]
allowed_lateness = "0s"

[admission]
max_inflight_bytes = 268435456
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

/// One OTLP export request of `n` log records, `base_nanos + 0..n` apart —
/// same shape as `e2e_boot.rs`'s `synthetic_request`, with a shorter body
/// (this test cares about row-count volume, not payload-byte volume).
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
                value: Some(PbValue::StringValue(format!("smoke {i}"))),
            }),
            attributes: vec![str_attr("k8s.pod.name", "pod-0")],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "smoke-volume")],
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

/// Row count of the drained `otlp_logs` table — same encoding note as
/// `e2e_boot.rs`'s `drained_row_count` (`duckspout-lake-ducklake`'s private
/// `dataset_table` hex-escaping: `otlp_logs` → `ds_otlp_5flogs`).
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
    conn.query_row("SELECT count(*) FROM verify.ds_otlp_5flogs", [], |row| {
        row.get(0)
    })
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "the per-PR smoke gate (§8.6, issue #47); excluded from `just test`'s \
            fast nextest run (same convention as tests/latency.rs's \
            commit_latency_distribution) — scripts/smoke.mjs runs it explicitly, \
            release-mode, via `cargo nextest run --release --run-ignored ignored-only`"]
async fn one_million_records_ingest_and_drain() {
    let root = tempfile::tempdir().unwrap();
    let config_path = write_config(root.path());
    let config = duckspout_daemon::config::load(Some(&config_path)).unwrap();
    let catalog_path = root.path().join("catalog.ducklake");
    let data_path = root.path().join("lake-data");

    let _ = tracing_subscriber::fmt::try_init();

    // --- Boot through the public API (never the binary) ---
    let daemon = Daemon::boot(&config, 0, None, std::time::Duration::ZERO)
        .await
        .expect("daemon boots");
    let handle = daemon.handle();
    let otlp_addr = daemon.otlp_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(daemon.serve(async {
        let _ = shutdown_rx.await;
    }));

    let mut ready = false;
    for _ in 0..50 {
        if handle.status().ready {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ready, "daemon did not report ready in time");

    // --- Push 1,000,000 records over CONCURRENCY concurrent OTLP clients ---
    let client = LogsServiceClient::connect(format!("http://{otlp_addr}"))
        .await
        .expect("connect to the real OTLP listener");
    let base_nanos = now_unix_nanos();
    assert_eq!(
        TOTAL_ROWS % BATCH_ROWS,
        0,
        "TOTAL_ROWS must divide evenly by BATCH_ROWS"
    );
    let batches = TOTAL_ROWS / BATCH_ROWS;
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(CONCURRENCY));
    let mut tasks = tokio::task::JoinSet::new();
    for batch in 0..batches {
        let mut client = client.clone();
        let sem = std::sync::Arc::clone(&sem);
        let batch_base = base_nanos + batch * BATCH_ROWS;
        tasks.spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let mut request = tonic::Request::new(synthetic_request(BATCH_ROWS, batch_base));
            request
                .metadata_mut()
                .insert("x-scope-orgid", "tenant-smoke".parse().unwrap());
            let resp = client.export(request).await.unwrap().into_inner();
            assert!(resp.partial_success.is_none(), "full-batch ack");
        });
    }
    while let Some(res) = tasks.join_next().await {
        res.expect("send task panicked");
    }

    // --- Close the last window: sleep past `hot.window = 5s`, then append
    // one more record so the roll-on-append rule actually rolls it
    // (`EngineStager`'s roll-on-append rule — `e2e_boot.rs`'s comment on
    // `note_closed_windows`). This tail record itself lands in the *new*
    // window that rolling opens, which stays hot/undrained for the rest of
    // this test (same as `e2e_boot.rs`'s window 1) — it is deliberately
    // excluded from the row-count assertion below, not a bug.
    tokio::time::sleep(Duration::from_millis(5_200)).await;
    let mut client = client;
    let mut tail = tonic::Request::new(synthetic_request(1, now_unix_nanos()));
    tail.metadata_mut()
        .insert("x-scope-orgid", "tenant-smoke".parse().unwrap());
    client.export(tail).await.unwrap();

    // --- Drain every window the ingest opened, to exhaustion ---
    let mut total_committed = 0usize;
    loop {
        let tick = handle.drain_once().await;
        assert_eq!(tick.requeued, 0, "no window should need a requeue here");
        total_committed += tick.committed;
        if tick.eligible == 0 {
            break;
        }
    }
    assert!(total_committed > 0, "at least one window must have drained");

    // --- Watermark advanced, status discloses no overload ---
    let status = handle.status();
    assert!(
        !status.watermarks.is_empty(),
        "at least one partition drained"
    );
    assert!(!status.drain_stalled, "a local file catalog never stalls");

    // --- Shut down cleanly (§9.1.2 choreography) ---
    shutdown_tx.send(()).unwrap();
    serve_task.await.unwrap();
    drop(handle);

    // --- The lake holds exactly the rows sent — the smoke bound's proof ---
    // Exactly TOTAL_ROWS, not TOTAL_ROWS + 1: the tail record's own window
    // never closes within this test (module docs) and is still hot,
    // undrained — DropWindow never ran for it, so it cannot be in the lake.
    let rows = drained_row_count(&catalog_path, &data_path);
    assert_eq!(
        rows,
        i64::try_from(TOTAL_ROWS).unwrap(),
        "the lake should hold every closed-window record (not the still-hot tail record)"
    );
}
