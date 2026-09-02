//! End-to-end OTLP accept path (§4.1, §4.3, issue #32), composed the way
//! the daemon composes it: a real gRPC client → `OtlpLogsService` →
//! `StageCommitter` port → the real WAL=hot `StagingEngine` — and the ack
//! observed only with the rows durably present.
//!
//! This test lives in the daemon deliberately: accept and staging may never
//! see each other (§10.1, dev-deps included), and the daemon is the one
//! crate whose job is exactly this composition.

mod common;

use std::sync::Arc;

use common::{FsStorage, SettableClock};
use duckspout_accept::OtlpLogsService;
use duckspout_accept::server::AdmissionConfig;
use duckspout_staging::arrow::array::AsArray as _;
use duckspout_staging::{EngineStager, StagerConfig, StagingConfig, StagingEngine};
use duckspout_types::{DatasetId, NodeId, PartitionId, TenantId, WindowId};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

fn synthetic_request(n: u64) -> ExportLogsServiceRequest {
    let str_attr = |k: &str, v: &str| KeyValue {
        key: k.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(v.to_owned())),
        }),
        ..Default::default()
    };
    let records = (0..n)
        .map(|i| LogRecord {
            time_unix_nano: 1_756_600_000_000_000_000 + i,
            severity_number: 9,
            severity_text: "INFO".to_owned(),
            body: Some(AnyValue {
                value: Some(PbValue::StringValue(format!("e2e log line {i}"))),
            }),
            attributes: vec![str_attr("k8s.pod.name", &format!("pod-{}", i % 4))],
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "e2e")],
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

/// Send a batch through real gRPC; the ack comes back only after the real
/// engine committed, and the hot table then holds exactly the flattened
/// rows with dense engine-stamped seqs. A second export appends (seq
/// resumes). Would catch any break in the composed chain: adapter schema vs
/// engine type subset, IPC encode/decode drift, coverage that disagrees
/// with the landed rows, or an ack that outruns durability.
#[tokio::test(flavor = "multi_thread")]
async fn otlp_export_acked_then_rows_durably_present() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        StagingEngine::open(
            StagingConfig {
                hot_dir: dir.path().to_path_buf(),
                origin: NodeId::new("node-a/1"),
            },
            FsStorage {
                root: dir.path().to_path_buf(),
            },
        )
        .unwrap(),
    );
    let stager = Arc::new(EngineStager::new(
        Arc::clone(&engine),
        SettableClock::new(),
        StagerConfig {
            window_nanos: 60_000_000_000,
            dedup_ttl_ms: 24 * 60 * 60 * 1000,
            dedup_max_entries: 100_000,
            hot_max_bytes: u64::MAX,
        },
    ));

    // The daemon's serving shape: the blocking engine commit runs inside
    // tonic's handler here, which a multi-thread runtime absorbs in-test
    // (production wraps the port in spawn_blocking at composition).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = OtlpLogsService::new(
        Arc::clone(&stager),
        AdmissionConfig {
            max_payload_bytes: 4 * 1024 * 1024,
            max_inflight_bytes: u64::MAX,
        },
    )
    .into_server();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let mut request = tonic::Request::new(synthetic_request(37));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());
    let resp = client.export(request).await.unwrap().into_inner();
    assert!(resp.partial_success.is_none(), "full-batch ack (§4.1.2)");

    // Ack received ⇒ rows durably committed: count, dense seq, payload.
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::from_tenant_shard(&TenantId::new("tenant-a"), 0);
    let reader = engine.reader().unwrap();
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .unwrap(),
        37
    );
    assert_eq!(engine.applied_seq(&partition).unwrap(), Some(37));

    let table = duckspout_staging::naming::window_table_name(&dataset, &partition, WindowId(0));
    let (_, batches) = reader
        .query_arrow(&format!(
            "SELECT body, attrs, resource_attrs, seq FROM {table} WHERE seq = 6"
        ))
        .unwrap();
    let row = &batches[0];
    let string_at = |i: usize| row.column(i).as_string::<i32>().value(0).to_owned();
    assert_eq!(string_at(0), "e2e log line 5"); // seq is 1-based: seq 6 = line 5
    assert!(string_at(1).contains("pod-1"));
    assert!(string_at(2).contains("\"service.name\":\"e2e\""));

    // Second export appends; seq stays dense across acked batches.
    let mut request = tonic::Request::new(synthetic_request(3));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());
    client.export(request).await.unwrap();
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .unwrap(),
        40
    );
    assert_eq!(engine.applied_seq(&partition).unwrap(), Some(40));
}
