//! In-process gRPC tests of the ack sequence (§4.3): a real tonic client
//! over a loopback listener, with a scripted `StageCommitter` double so
//! every branch of the sequence is drivable — including the one production
//! must never take (ack before durability).
//!
//! The double is test-local by necessity, not preference: the invariant
//! engine audits *all* direct dependency edges, dev-deps included, and
//! accept → ctk is a forbidden edge (§10.1) — so the CTK doubles are out of
//! reach here, exactly as they are for staging's own tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use duckspout_accept::OtlpLogsService;
use duckspout_types::{
    BoxFuture, DecodedBatch, OriginSeqRange, StageCommitter, StageError, StagedCoverage,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use tonic::Request;

/// What the scripted stager should do with a batch.
enum Script {
    /// Commit succeeds: record the batch, return coverage evidence.
    Commit,
    /// The commit fails (a crash before commit durably lands, §4.3): the
    /// batch is NOT durable and must NOT be acked.
    FailCommit,
    /// The commit blocks until its gate is notified — for proving
    /// the ack cannot outrun the commit.
    GateCommit(Arc<tokio::sync::Notify>),
}

/// A scripted [`StageCommitter`] double.
struct ScriptedStager {
    script: Script,
    staged: Mutex<Vec<DecodedBatch>>,
}

impl ScriptedStager {
    fn new(script: Script) -> Arc<Self> {
        Arc::new(Self {
            script,
            staged: Mutex::new(Vec::new()),
        })
    }

    fn staged_count(&self) -> usize {
        self.staged.lock().unwrap().len()
    }

    fn coverage_for(batch: &DecodedBatch) -> Vec<StagedCoverage> {
        vec![StagedCoverage {
            partition: duckspout_types::PartitionId::from_tenant_shard(&batch.tenant, 0),
            range: OriginSeqRange {
                origin: duckspout_types::NodeId::new("test-node/1"),
                first_seq: 1,
                last_seq: 1,
            },
        }]
    }
}

impl StageCommitter for ScriptedStager {
    fn stage_commit(
        &self,
        batch: DecodedBatch,
    ) -> BoxFuture<'_, Result<Vec<StagedCoverage>, StageError>> {
        Box::pin(async move {
            match &self.script {
                Script::Commit => {
                    let coverage = Self::coverage_for(&batch);
                    self.staged.lock().unwrap().push(batch);
                    Ok(coverage)
                }
                Script::FailCommit => Err(StageError::Backend(
                    "injected: crashed before commit".to_owned(),
                )),
                Script::GateCommit(gate) => {
                    gate.notified().await;
                    let coverage = Self::coverage_for(&batch);
                    self.staged.lock().unwrap().push(batch);
                    Ok(coverage)
                }
            }
        })
    }
}

/// Serves the logs service on an ephemeral loopback port; returns a client.
async fn client_for(stager: Arc<ScriptedStager>) -> LogsServiceClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = OtlpLogsService::new(stager).into_server();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

fn str_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(value.to_owned())),
        }),
        ..Default::default()
    }
}

fn logs_request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
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

fn log_record(i: u64) -> LogRecord {
    LogRecord {
        time_unix_nano: 1_756_600_000_000_000_000 + i,
        severity_number: 9,
        body: Some(AnyValue {
            value: Some(PbValue::StringValue(format!("line {i}"))),
        }),
        attributes: vec![str_attr("k", "v")],
        ..Default::default()
    }
}

/// Happy path: the ack arrives, carries no `partial_success`, and the batch
/// reached the stager exactly once. Would catch an ack path that skips the
/// port or a service that answers per-resource-group instead of per-batch.
#[tokio::test]
async fn export_is_acked_after_the_port_commits() {
    let stager = ScriptedStager::new(Script::Commit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let resp = client
        .export(logs_request((0..5).map(log_record).collect()))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.partial_success.is_none(), "unexpected: {resp:?}");
    assert_eq!(stager.staged_count(), 1, "batch must stage exactly once");
}

/// §4.3's core promise, falsified from the client side: when the commit
/// fails, the client gets the retryable UNAVAILABLE vocabulary — never an
/// ack. Would catch a service that acks first and stages after, or maps a
/// storage failure onto a non-retryable code (silent loss at the edge,
/// §4.1.1).
#[tokio::test]
async fn no_ack_when_the_commit_fails() {
    let stager = ScriptedStager::new(Script::FailCommit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let status = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("a failed commit must not ack");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(stager.staged_count(), 0, "nothing durable, nothing staged");
}

/// Ack-after-commit ordering, driven: while the commit is gated open, the
/// client's call must still be pending; releasing the gate releases the
/// ack. Would catch an ack issued concurrently with (rather than after)
/// `StageCommit` — the §4.3 ordering, not just the §4.3 outcome.
#[tokio::test]
async fn ack_waits_for_the_commit_to_finish() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let stager = ScriptedStager::new(Script::GateCommit(Arc::clone(&gate)));
    let mut client = client_for(Arc::clone(&stager)).await;

    let call = tokio::spawn(async move {
        client
            .export(logs_request(vec![log_record(0)]))
            .await
            .unwrap()
            .into_inner()
    });
    // Give the request ample time to reach the gated commit: the ack must
    // not have been produced while the commit is still in flight.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !call.is_finished(),
        "ack arrived while StageCommit was still running (§4.3 ordering broken)"
    );

    gate.notify_one();
    let resp = call.await.unwrap();
    assert!(resp.partial_success.is_none());
    assert_eq!(stager.staged_count(), 1);
}

/// An empty export succeeds without touching the staging port (nothing to
/// stage; OTLP says an empty export is a success).
#[tokio::test]
async fn empty_export_acks_without_staging() {
    let stager = ScriptedStager::new(Script::Commit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let resp = client
        .export(ExportLogsServiceRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert!(resp.partial_success.is_none());
    assert_eq!(stager.staged_count(), 0);
}

/// §2.2 identity validation fails closed at the wire: a reserved system
/// tenant in `X-Scope-OrgID` is a permanent `INVALID_ARGUMENT` and nothing
/// reaches staging.
#[tokio::test]
async fn reserved_tenant_header_is_rejected_before_staging() {
    let stager = ScriptedStager::new(Script::Commit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let mut request = Request::new(logs_request(vec![log_record(0)]));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "_self".parse().unwrap());
    let status = client.export(request).await.expect_err("must reject");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(stager.staged_count(), 0);
}

/// The tenant header rides into the decoded batch (multi-tenant identity is
/// extracted, not defaulted, when present) and the idempotency key rides
/// alongside (§4.4.1's precedence input).
#[tokio::test]
async fn tenant_and_idempotency_headers_reach_the_batch() {
    let stager = ScriptedStager::new(Script::Commit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let mut request = Request::new(logs_request(vec![log_record(0)]));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());
    request.metadata_mut().insert(
        "x-duckspout-idempotency-key",
        "retry-token-1".parse().unwrap(),
    );
    client.export(request).await.unwrap();

    let batches = stager.staged.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].tenant.as_str(), "tenant-a");
    assert_eq!(batches[0].idempotency_key.as_deref(), Some("retry-token-1"));
}

/// A profiling-only string-interning reference (issue #110) is non-fatal:
/// the batch is acked in full, the ignored reference is disclosed via the
/// OTLP `rejected_log_records = 0` warning shape, and the record itself
/// still stages. Would catch a decoder that rejects (or silently drops)
/// interned attributes.
#[tokio::test]
async fn strindex_references_ack_with_a_zero_rejected_warning() {
    let stager = ScriptedStager::new(Script::Commit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let mut record = log_record(0);
    record.attributes.push(KeyValue {
        key: String::new(),
        key_strindex: 7, // profiling-only reference; logs have no table
        value: Some(AnyValue {
            value: Some(PbValue::StringValue("unreachable".to_owned())),
        }),
    });
    let resp = client
        .export(logs_request(vec![record]))
        .await
        .unwrap()
        .into_inner();

    let partial = resp.partial_success.expect("warning must be disclosed");
    assert_eq!(
        partial.rejected_log_records, 0,
        "a warning is not a rejection: the batch is fully accepted"
    );
    assert!(partial.error_message.contains("string-interning"));
    assert_eq!(stager.staged_count(), 1, "the record still stages");
}
