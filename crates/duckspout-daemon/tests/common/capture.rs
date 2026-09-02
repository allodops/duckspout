//! The real accept → staging → drain composition that produces a §8.2
//! trace, factored out so both the local-double capture
//! (`tests/trace_capture.rs`) and the real-backend capture
//! (`tests/trace_capture_real_backends.rs`, issue #44) drive the exact
//! same choreography — the trace format is what this script asserts, so it
//! must exist exactly once (DRY).
//!
//! Mirrors `otlp_e2e.rs` (accept over real gRPC into the real WAL=hot
//! engine) and extends it with a live duplicate replay (`DedupCheck`,
//! §4.4.1 — issue #146's window) and a real drain cycle — single node
//! `n1`, the v0.1 topology (RF = 1: local durable is the whole replication
//! floor).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{FsStorage, SettableClock};
use duckspout_accept::OtlpLogsService;
use duckspout_accept::server::AdmissionConfig;
use duckspout_ctk::NdjsonTraceWriter;
use duckspout_drain::{DatasetDrainPlan, DrainConfig, DrainCoordinator, DrainOutcome};
use duckspout_lake_ducklake::DuckLakeCommitter;
use duckspout_staging::{
    EngineSealSurface, EngineStager, StagerConfig, StagingConfig, StagingEngine,
};
use duckspout_types::{
    ColumnSpec, DatasetId, LakeCommitter as _, NodeId, PartitionId, TenantId, TraceSink, WindowId,
};
use duckspout_watermark::{SharedLedger, WatermarkLedger};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

/// One-second arrival windows, so the test rolls the window by advancing
/// the clock — no waiting.
const WINDOW_NANOS: u64 = 1_000_000_000;

pub fn dataset_for_capture() -> DatasetId {
    DatasetId::new("otlp_logs")
}

/// The fixed OTLP logs schema plus the two §2.3 system columns, as the
/// closed §2 logical-type vocabulary — what the daemon's declaration wiring
/// will evolve before the first drain.
pub fn logs_columns() -> Vec<ColumnSpec> {
    let col = |name: &str, logical_type: &str| ColumnSpec {
        name: name.to_owned(),
        logical_type: logical_type.to_owned(),
    };
    vec![
        col("ts", "timestamp_micros"),
        col("observed_ts", "timestamp_micros"),
        col("severity_number", "int32"),
        col("severity_text", "utf8"),
        col("body", "utf8"),
        col("attrs", "utf8"),
        col("resource_attrs", "utf8"),
        col("scope_name", "utf8"),
        col("scope_version", "utf8"),
        col("trace_id", "utf8"),
        col("span_id", "utf8"),
        col("flags", "uint32"),
        col("event_name", "utf8"),
        col("dropped_attributes_count", "uint32"),
        col("origin", "utf8"),
        col("seq", "uint64"),
    ]
}

fn export_request(lines: u64, base_nano: u64) -> ExportLogsServiceRequest {
    let records = (0..lines)
        .map(|i| LogRecord {
            time_unix_nano: base_nano + i,
            severity_number: 9,
            body: Some(AnyValue {
                value: Some(PbValue::StringValue(format!("trace capture line {i}"))),
            }),
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Drives the real composition through two acked exports plus a duplicate
/// replay, one full drain cycle (seal → put → commit → coverage-guarded
/// drop), and a final export into the rolled window, and returns the
/// journaled trace text. Would catch: a tracepoint dropped, doubled, or
/// reordered against the real choreography; an event journaled on a
/// failure or replay path that must stay silent (a duplicate must journal
/// `DedupCheck`, never a second `StageCommit`/`ClientAck`); a drain step
/// whose journaled name drifts from its §3.3 action.
///
/// `committer` and `parts_store` are the lake backend: local doubles for
/// the committed-fixture test, real `MinIO` + Postgres for the real-backend
/// conformance capture (§8.2, issue #44) — the choreography itself does
/// not know or care which.
#[expect(
    clippy::too_many_lines,
    reason = "one linear composition script: the full accept → staging → drain wiring, driven once — splitting it would scatter the choreography the trace asserts"
)]
pub async fn capture_ingest_drain_trace(
    trace_path: &Path,
    hot_dir: PathBuf,
    committer: DuckLakeCommitter,
    parts_store: Arc<dyn object_store::ObjectStore>,
) -> String {
    // One writer, node "n1" (the trace config's node vocabulary): every
    // subsystem journals through the same sink, so per-node seqs are dense
    // across the whole composition (D-6).
    let sink: Arc<dyn TraceSink> = Arc::new(NdjsonTraceWriter::new(
        NodeId::new("n1"),
        std::fs::File::create(trace_path).unwrap(),
    ));

    let clock = SettableClock::new();
    let engine = Arc::new(
        StagingEngine::open(
            StagingConfig {
                hot_dir: hot_dir.clone(),
                origin: NodeId::new("n1"),
            },
            FsStorage {
                root: hot_dir.clone(),
            },
        )
        .unwrap(),
    );
    let stager = Arc::new(
        EngineStager::new(
            Arc::clone(&engine),
            clock.clone(),
            StagerConfig {
                window_nanos: WINDOW_NANOS,
                dedup_ttl_ms: 24 * 60 * 60 * 1000,
                dedup_max_entries: 100_000,
                hot_max_bytes: u64::MAX,
            },
        )
        .with_trace_sink(Arc::clone(&sink)),
    );
    let seal = Arc::new(EngineSealSurface::new(Arc::clone(&engine)));

    // Evolve-before-add (§6.4): the lake table must exist before the drain
    // registers parts into it. This is the composition's lake-side DDL
    // setup — not the §3.3 EvolveSchema staging record (that pipeline is
    // future work), so it journals nothing and the trace config's lattice
    // is the trivial one (Columns = {}).
    committer
        .evolve_schema(duckspout_types::SchemaEvolution {
            dataset: dataset_for_capture(),
            columns: logs_columns(),
        })
        .await
        .unwrap();
    let coordinator = DrainCoordinator::new(
        Arc::clone(&seal) as _,
        Arc::new(SharedLedger::new(WatermarkLedger::new())),
        Arc::new(committer),
        parts_store,
        Arc::new(FsStorage { root: hot_dir }),
        Arc::new(clock.clone()),
        DrainConfig {
            allowed_lateness_ms: 0,
        },
    )
    .with_trace_sink(Arc::clone(&sink));

    // The daemon's serving shape (otlp_e2e.rs): real gRPC in-process.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = OtlpLogsService::new(
        Arc::clone(&stager),
        AdmissionConfig {
            max_payload_bytes: 4 * 1024 * 1024,
            max_inflight_bytes: 64 * 1024 * 1024,
        },
    )
    .with_trace_sink(Arc::clone(&sink))
    .into_server();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    // Export A into window 0: Accept, StageCommit, ClientAck.
    let a = export_request(2, 1_756_600_000_000_000_000);
    client.export(tonic::Request::new(a.clone())).await.unwrap();
    // The SAME payload again: a §4.4.1 duplicate — Accept, then DedupCheck
    // replays the original's ack (no StageCommit, no second ClientAck
    // journal; the wire response is still a success).
    client.export(tonic::Request::new(a)).await.unwrap();
    // Export B, distinct content: Accept, StageCommit, ClientAck.
    client
        .export(tonic::Request::new(export_request(
            1,
            1_756_600_001_000_000_000,
        )))
        .await
        .unwrap();

    // Roll the arrival window (window 0 closes to ordinary ingest); the
    // §6.3 lateness hold is 0 ms here, so the window is drain-eligible.
    clock.set_nanos(WINDOW_NANOS + 1);
    clock.set_wall_ms(1);
    let dataset = dataset_for_capture();
    let partition = PartitionId::from_tenant_shard(&TenantId::new("anonymous"), 0);
    seal.note_closed(&dataset, &partition, WindowId(0), 0);

    // One real drain cycle: SealPart, PutPart, LakeCommitOk, DropWindow.
    let outcome = coordinator
        .drain_window(
            &dataset,
            &partition,
            WindowId(0),
            &DatasetDrainPlan {
                order_by: vec!["ts".to_owned()],
                event_time_column: "ts".to_owned(),
                dedup_key: None,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, DrainOutcome::Committed { watermark: Some(_) }),
        "the drain must commit with a provable watermark, got {outcome:?}"
    );

    // A final export lands in the rolled window 1.
    client
        .export(tonic::Request::new(export_request(
            1,
            1_756_600_010_000_000_000,
        )))
        .await
        .unwrap();

    std::fs::read_to_string(trace_path).unwrap()
}
