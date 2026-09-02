//! Boot-time watermark reconstruction (§6.8, ADR-0010; issue #153).
//!
//! `docs/design/drain.md` §6.8 makes the watermark state
//! "authoritative-but-reconstructible": the lake's committed manifest
//! record is ground truth, even when the catalog's own watermark row is
//! unavailable. Before this issue, `Daemon::boot`
//! (`crates/duckspout-daemon/src/wiring.rs`) hardcoded a fresh **empty**
//! `WatermarkLedger` on every boot — correct for a node that has never
//! drained, but unsafe on restart: `DrainCoordinator::drain_window`'s
//! pre-commit fence (`crates/duckspout-drain/src/coordinator.rs`) rejects
//! any window above the ledger's `next_window` as `WindowAhead`, so a
//! restarted node with prior commits would refuse to drain its true next
//! window until the fence's `expected` (wrongly `WindowId(0)`) caught up —
//! it never would, because window 0 no longer exists to redrain (it was
//! already dropped from staging on the prior boot).
//!
//! This test proves the fix end to end: boot a daemon (A), ingest window 0
//! and drain it (leaving window 1 open with a couple of A's own rows,
//! never drained), shut A down cleanly, boot a **second** daemon (B) —
//! through the same public API, against the identical hot/catalog/lake-data
//! directories, simulating a process restart — and show that (a) B's
//! watermark reads back exactly what A committed **before B ever drains
//! anything** (pure reconstruction, not a lucky re-derivation); (b) B's
//! first two drain ticks commit window 1 (A's own leftover, still-open
//! window at shutdown) and then window 2 (entirely B's own), densely and in
//! order, rather than either being rejected or requeued; and (c) nothing
//! was double-registered: the lake holds exactly windows 0, 1, and 2's rows,
//! once each, with window 3 correctly left undrained.
//!
//! Boot/ingest/drain choreography mirrors `tests/e2e_boot.rs`; helpers are
//! deliberately local to this file rather than factored into
//! `tests/common/mod.rs`, matching this crate's existing convention of
//! per-scenario test helpers (`tests/otlp_e2e.rs` and `tests/e2e_boot.rs`
//! each carry their own `synthetic_request`, not a shared one) — `common`
//! is reserved for genuinely identical port doubles (`FsStorage`,
//! `SettableClock`), which this test does not need (it drives the real
//! public API, not the ports directly).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use duckspout_daemon::wiring::{Daemon, DaemonHandle};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// Writes a minimal §9.6 config file rooted at `root`, with a short window
/// and zero lateness hold — see `tests/e2e_boot.rs`'s `write_config` for why
/// (a real window roll and a real drain-eligibility check inside a test's
/// patience). Called twice against the **same** `root` in this test: the
/// second call reloads byte-identical settings, so daemon B attaches the
/// exact same hot dir, catalog, and lake data daemon A used — the "restart"
/// this test exercises.
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

/// Builds one OTLP export request of `n` log records, `base_nanos + 0..n`
/// apart — matches `tests/e2e_boot.rs` and `tests/otlp_e2e.rs`'s synthetic
/// request shape.
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
                value: Some(PbValue::StringValue(format!("watermark-boot log {i}"))),
            }),
            attributes: vec![str_attr("k8s.pod.name", &format!("pod-{}", i % 2))],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "watermark-boot")],
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

/// Sends one export request tagged for `tenant-a`, over an already-connected
/// client.
async fn export(
    client: &mut LogsServiceClient<tonic::transport::Channel>,
    n: u64,
    base_nanos: u64,
) {
    let mut request = tonic::Request::new(synthetic_request(n, base_nanos));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());
    let resp = client.export(request).await.unwrap().into_inner();
    assert!(resp.partial_success.is_none(), "full-batch ack");
}

/// Polls `/status` (in-process, via the handle) until `ready` — boot returns
/// before `serve` flips readiness, matching `tests/e2e_boot.rs`.
async fn wait_ready(handle: &DaemonHandle) {
    for _ in 0..50 {
        if handle.status().ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not report ready in time");
}

/// A booted, serving daemon under test: the shared boot/serve/wait-ready
/// choreography, factored out so the test body reads as "boot A, do things,
/// stop A, boot B against the same paths" without repeating the plumbing —
/// and so daemon A's and daemon B's shutdown channels never appear as
/// separate `_tx`/`_rx` locals in the test body (they'd otherwise collide
/// with clippy's `similar_names`, which flags `shutdown_tx_a` against
/// `shutdown_rx_a`).
struct RunningDaemon {
    handle: DaemonHandle,
    otlp_addr: SocketAddr,
    stop_tx: tokio::sync::oneshot::Sender<()>,
    served: tokio::task::JoinHandle<()>,
}

impl RunningDaemon {
    /// Boots a daemon from `config_path`, spawns `serve`, and waits for
    /// readiness.
    async fn start(config_path: &std::path::Path) -> Self {
        let config = duckspout_daemon::config::load(Some(config_path)).unwrap();
        let daemon = Daemon::boot(&config, 0).await.expect("daemon boots");
        let handle = daemon.handle();
        let otlp_addr = daemon.otlp_addr();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let served = tokio::spawn(daemon.serve(async {
            let _ = stop_rx.await;
        }));
        wait_ready(&handle).await;
        Self {
            handle,
            otlp_addr,
            stop_tx,
            served,
        }
    }

    /// Clean shutdown: releases every DuckDB-file handle (the hot engine's
    /// and the `DuckLake` catalog's) — required before another daemon opens
    /// the same paths, a DuckDB-file catalog is single-process
    /// (`duckspout-lake-ducklake`'s module docs, issue #119).
    async fn stop(self) {
        let _ = self.stop_tx.send(());
        self.served.await.unwrap();
        drop(self.handle);
    }
}

/// Row count of the drained `otlp_logs` table, read with a fresh `DuckDB`
/// connection attached to the same `DuckLake` catalog (`tests/e2e_boot.rs`'s
/// `drained_row_count`).
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
async fn restarted_daemon_reconstructs_the_watermark_and_drains_the_true_next_window() {
    let root = tempfile::tempdir().unwrap();
    let catalog_path = root.path().join("catalog.ducklake");
    let data_path = root.path().join("lake-data");
    let _ = tracing_subscriber::fmt::try_init();
    let config_path = write_config(root.path());

    // ===================== Daemon A: ingest + drain window 0 =====================
    let daemon_a = RunningDaemon::start(&config_path).await;
    let mut client_a = LogsServiceClient::connect(format!("http://{}", daemon_a.otlp_addr))
        .await
        .expect("connect to daemon A's OTLP listener");
    let window0_base = now_unix_nanos();
    export(&mut client_a, 7, window0_base).await;

    // Window 0 is still open — nothing eligible yet.
    let tick = daemon_a.handle.drain_once().await;
    assert_eq!(tick.eligible, 0, "the open window is never offered");

    // Roll the window (hot.window = 1s): appending again after it elapses
    // allocates window 1, which closes window 0.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    export(&mut client_a, 2, now_unix_nanos()).await;

    let tick = daemon_a.handle.drain_once().await;
    assert_eq!(
        tick.eligible, 1,
        "window 0 closed and its lateness hold is 0s"
    );
    assert_eq!(tick.committed, 1, "window 0 committed");
    assert_eq!(tick.requeued, 0);

    let status_a = daemon_a.handle.status();
    assert_eq!(status_a.watermarks.len(), 1, "one partition drained");
    let watermark_after_a = status_a.watermarks[0].complete_through_ms;
    let partition_a = status_a.watermarks[0].partition.clone();
    let expected_min_ms = i64::try_from(window0_base / 1_000_000).unwrap();
    assert!(
        watermark_after_a >= expected_min_ms,
        "watermark {watermark_after_a} should cover window 0's event time {expected_min_ms}"
    );

    daemon_a.stop().await;

    // ============ Daemon B: boot against the SAME hot/catalog/lake dirs ============
    // This is the "restart": a fresh process, a fresh in-memory
    // WatermarkLedger, the identical durable state on disk.
    let daemon_b = RunningDaemon::start(&config_path).await;

    // --- (a) The watermark is back BEFORE B ever drains anything: pure
    // --- boot-time reconstruction from the lake's manifest record, not a
    // --- coincidence of a subsequent drain re-deriving the same number.
    let status_b_at_boot = daemon_b.handle.status();
    assert_eq!(
        status_b_at_boot.watermarks.len(),
        1,
        "boot-time reconstruction found the partition's prior commit"
    );
    assert_eq!(
        status_b_at_boot.watermarks[0].partition, partition_a,
        "same partition"
    );
    assert_eq!(
        status_b_at_boot.watermarks[0].complete_through_ms, watermark_after_a,
        "B's reconstructed watermark exactly matches what A had committed \
         before restart — this fails if boot falls back to an empty ledger"
    );

    // --- (b) The dense-next fence: window 1 must commit, never be
    // --- rejected/requeued as WindowAhead. Window 1 is A's own leftover:
    // --- A's second export (above) rolled window 0 closed and opened
    // --- window 1 with its 2 rows, but A shut down before draining it — so
    // --- window 1 is still *open* right after B boots (`note_closed_windows`
    // --- only closes a window once a newer one is allocated). Force it
    // --- stale before B's first append, so that append deterministically
    // --- rolls it closed instead of racing the boot choreography's own
    // --- elapsed time.
    let mut client_b = LogsServiceClient::connect(format!("http://{}", daemon_b.otlp_addr))
        .await
        .expect("connect to daemon B's OTLP listener");
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    export(&mut client_b, 4, now_unix_nanos()).await; // rolls window 1 closed, opens window 2

    let tick = daemon_b.handle.drain_once().await;
    assert_eq!(
        tick.eligible, 1,
        "window 1 — A's own still-open window at restart — is the dense-next \
         window offered"
    );
    assert_eq!(
        tick.committed, 1,
        "window 1 committed on B's first post-restart drain tick — a broken \
         reconstruction would leave it requeued (WindowAhead), never committed"
    );
    assert_eq!(tick.requeued, 0, "no window was rejected or retried");

    // --- Continuity holds past the first post-restart window too: window 2
    // --- (entirely B's own) is dense-next after window 1.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    export(&mut client_b, 1, now_unix_nanos()).await; // rolls window 2 closed, opens window 3 (left undrained)

    let tick = daemon_b.handle.drain_once().await;
    assert_eq!(tick.eligible, 1, "window 2 is dense-next after window 1");
    assert_eq!(tick.committed, 1, "window 2 committed");
    assert_eq!(tick.requeued, 0);

    let status_b_after_drain = daemon_b.handle.status();
    assert!(
        status_b_after_drain.watermarks[0].complete_through_ms >= watermark_after_a,
        "the watermark only advances from where A left it, never regresses"
    );

    daemon_b.stop().await;

    // --- (c) No double registration: window 0's 7 rows (A) + window 1's 2
    // --- rows (A's leftover, drained by B) + window 2's 4 rows (B), each
    // --- counted exactly once. Window 3's 1 row is still undrained in hot
    // --- staging and correctly absent from the lake.
    let rows = drained_row_count(&catalog_path, &data_path);
    assert_eq!(
        rows,
        7 + 2 + 4,
        "windows 0, 1, and 2 each landed exactly once; nothing was re-registered"
    );
}
