//! In-process gRPC tests of the ack sequence (§4.3), the admission caps
//! (§4.6), and the ladder's client-visible half (§4.5): a real tonic
//! client over a loopback listener, with a scripted `StageCommitter`
//! double so every branch is drivable — including the one production must
//! never take (ack before durability).
//!
//! The double is test-local by necessity, not preference: the invariant
//! engine audits *all* direct dependency edges, dev-deps included, and
//! accept → ctk is a forbidden edge (§10.1) — so the CTK doubles are out of
//! reach here, exactly as they are for staging's own tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use duckspout_accept::server::{AdmissionConfig, DEFAULT_RETRY_DELAY_MS, OtlpLogsService};
use duckspout_types::{
    BoxFuture, DecodedBatch, OriginSeqRange, StageCommitter, StageError, StageOutcome,
    StagedCoverage,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use tonic::Request;
use tonic_types::StatusExt as _;

/// What the scripted stager should do with a batch.
enum Script {
    /// Commit succeeds: record the batch, return coverage evidence.
    Commit,
    /// The commit fails (a crash before commit durably lands, §4.3): the
    /// batch is NOT durable and must NOT be acked.
    FailCommit,
    /// The commit blocks until its gate is notified — for proving the ack
    /// cannot outrun the commit.
    GateCommit(Arc<tokio::sync::Notify>),
    /// `DedupCheck` resolves the batch as an already-acked duplicate
    /// (§4.4.1): replay, no staging.
    DuplicateAcked,
    /// `DedupCheck` resolves the batch as a pre-RF duplicate (§4.4.1).
    DuplicateInFlight,
    /// Ladder rung 2 (§4.5): admission throttled with a growing delay.
    Throttle(u64),
    /// Ladder rung 3 (§4.5): admission refused.
    Refuse(u64),
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
    fn stage_commit(&self, batch: DecodedBatch) -> BoxFuture<'_, Result<StageOutcome, StageError>> {
        Box::pin(async move {
            match &self.script {
                Script::Commit => {
                    let coverage = Self::coverage_for(&batch);
                    self.staged.lock().unwrap().push(batch);
                    Ok(StageOutcome::Committed(coverage))
                }
                Script::FailCommit => Err(StageError::Backend(
                    "injected: crashed before commit".to_owned(),
                )),
                Script::GateCommit(gate) => {
                    gate.notified().await;
                    let coverage = Self::coverage_for(&batch);
                    self.staged.lock().unwrap().push(batch);
                    Ok(StageOutcome::Committed(coverage))
                }
                Script::DuplicateAcked => {
                    Ok(StageOutcome::DuplicateAcked(Self::coverage_for(&batch)))
                }
                Script::DuplicateInFlight => Ok(StageOutcome::DuplicateInFlight),
                Script::Throttle(ms) => Err(StageError::Throttled {
                    retry_after_ms: *ms,
                }),
                Script::Refuse(ms) => Err(StageError::RefusingIngest {
                    retry_after_ms: *ms,
                }),
            }
        })
    }
}

/// A permissive §4.6 posture for tests not about the caps.
fn open_admission() -> AdmissionConfig {
    AdmissionConfig {
        max_payload_bytes: 4 * 1024 * 1024,
        max_inflight_bytes: u64::MAX,
    }
}

/// Serves the logs service on an ephemeral loopback port; returns a client.
async fn client_with(
    stager: Arc<ScriptedStager>,
    admission: AdmissionConfig,
) -> LogsServiceClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = OtlpLogsService::new(stager, admission).into_server();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

async fn client_for(stager: Arc<ScriptedStager>) -> LogsServiceClient<tonic::transport::Channel> {
    client_with(stager, open_admission()).await
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

fn retry_delay(status: &tonic::Status) -> Option<Duration> {
    status.get_details_retry_info().and_then(|r| r.retry_delay)
}

// ---------------------------------------------------------------------------
// §4.3: the ack sequence
// ---------------------------------------------------------------------------

/// Happy path: the ack arrives, carries no `partial_success`, and the batch
/// reached the stager exactly once.
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
/// fails, the client gets the retryable UNAVAILABLE vocabulary — with a
/// `RetryInfo` detail — never an ack.
#[tokio::test]
async fn no_ack_when_the_commit_fails() {
    let stager = ScriptedStager::new(Script::FailCommit);
    let mut client = client_for(Arc::clone(&stager)).await;

    let status = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("a failed commit must not ack");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(
        retry_delay(&status),
        Some(Duration::from_millis(DEFAULT_RETRY_DELAY_MS)),
        "retryable-by-right outcomes say how to retry (§4.1.2)"
    );
    assert_eq!(stager.staged_count(), 0, "nothing durable, nothing staged");
}

/// Ack-after-commit ordering, driven: while the commit is gated open, the
/// client's call must still be pending; releasing the gate releases the
/// ack.
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

/// An empty export succeeds without touching the staging port.
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

/// §2.2 identity validation fails closed at the wire.
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
    assert!(
        retry_delay(&status).is_none(),
        "permanent rejects carry no RetryInfo"
    );
    assert_eq!(stager.staged_count(), 0);
}

/// The tenant and idempotency headers ride into the decoded batch.
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
/// full ack, `rejected_log_records = 0` warning, record still staged.
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

// ---------------------------------------------------------------------------
// §4.4.1: duplicate semantics on the wire
// ---------------------------------------------------------------------------

/// A replayed duplicate is an ack, indistinguishable from the original's
/// success shape (R-2) — and nothing new is staged.
#[tokio::test]
async fn duplicate_acked_replays_success() {
    let stager = ScriptedStager::new(Script::DuplicateAcked);
    let mut client = client_for(Arc::clone(&stager)).await;

    let resp = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.partial_success.is_none());
    assert_eq!(stager.staged_count(), 0, "a replay never re-stages");
}

/// A pre-RF duplicate resolves UNAVAILABLE + `RetryInfo` (§4.4.1): the
/// client in that window is a retrying OTLP client that already handles
/// retry signaling.
#[tokio::test]
async fn duplicate_in_flight_is_retryable() {
    let stager = ScriptedStager::new(Script::DuplicateInFlight);
    let mut client = client_for(Arc::clone(&stager)).await;

    let status = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("pre-RF duplicate must not ack");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(retry_delay(&status).is_some());
}

// ---------------------------------------------------------------------------
// §4.5: the ladder's client-visible half
// ---------------------------------------------------------------------------

/// Rung 2 on the wire: UNAVAILABLE with the stager-computed growing delay
/// in a spec-exact `google.rpc.RetryInfo` detail.
#[tokio::test]
async fn throttle_carries_the_growing_delay() {
    let stager = ScriptedStager::new(Script::Throttle(7_500));
    let mut client = client_for(Arc::clone(&stager)).await;

    let status = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("rung 2 must not ack");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(retry_delay(&status), Some(Duration::from_millis(7_500)));
}

/// Rung 3 on the wire: still the retryable vocabulary (§4.5 — refusal is a
/// refusal of new promises, not a permanent failure).
#[tokio::test]
async fn refusal_is_still_retryable_vocabulary() {
    let stager = ScriptedStager::new(Script::Refuse(30_000));
    let mut client = client_for(Arc::clone(&stager)).await;

    let status = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("rung 3 must not ack");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(retry_delay(&status), Some(Duration::from_secs(30)));
}

// ---------------------------------------------------------------------------
// §4.6: admission constants
// ---------------------------------------------------------------------------

/// Over-cap payload: `RESOURCE_EXHAUSTED` with **no** `RetryInfo` — retrying an
/// over-sized payload can never succeed, and instructing a retry would
/// manufacture a loop (§4.6). Nothing is decoded or staged.
#[tokio::test]
async fn oversized_payload_is_non_retryable() {
    let stager = ScriptedStager::new(Script::Commit);
    let mut client = client_with(
        Arc::clone(&stager),
        AdmissionConfig {
            max_payload_bytes: 16, // far under one record's encoding
            max_inflight_bytes: u64::MAX,
        },
    )
    .await;

    let status = client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("over-cap must reject");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(
        retry_delay(&status).is_none(),
        "over-cap is non-retryable: no RetryInfo, ever (§4.6)"
    );
    assert_eq!(stager.staged_count(), 0);
}

/// The in-flight decoded-bytes cap (§4.6): while one request's decoded
/// bytes are held in flight (gated commit), a second request over the
/// remaining budget throttles — UNAVAILABLE + `RetryInfo`, nothing staged —
/// and once the first completes (bytes released), the same request
/// succeeds. Would catch a leaked in-flight accounting (bytes never
/// released) or a cap that is a per-request rather than an in-flight
/// bound.
#[tokio::test]
async fn inflight_cap_throttles_while_held_and_recovers() {
    // Size one decoded batch exactly as the service will.
    let decoded_size = duckspout_accept::OtlpGrpcAdapter
        .decode_logs(logs_request(vec![log_record(0)]), None, None)
        .unwrap()
        .batch
        .records
        .len() as u64;

    let gate = Arc::new(tokio::sync::Notify::new());
    let stager = ScriptedStager::new(Script::GateCommit(Arc::clone(&gate)));
    let client = client_with(
        Arc::clone(&stager),
        AdmissionConfig {
            max_payload_bytes: 4 * 1024 * 1024,
            // Budget for one batch in flight, not two.
            max_inflight_bytes: decoded_size + decoded_size / 2,
        },
    )
    .await;

    // First request enters and parks at the gated commit, holding its
    // decoded bytes in flight.
    let mut held_client = client.clone();
    let held = tokio::spawn(async move {
        held_client
            .export(logs_request(vec![log_record(0)]))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !held.is_finished(),
        "first request must be parked in-flight"
    );

    // Second request: over the in-flight budget → throttle.
    let mut second_client = client.clone();
    let status = second_client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect_err("second batch exceeds the in-flight budget");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(retry_delay(&status).is_some());
    assert_eq!(stager.staged_count(), 0, "throttled batch never staged");

    // Release the held commit; its bytes leave the account and the retry
    // is admitted.
    gate.notify_one();
    held.await.unwrap();
    gate.notify_one(); // the retry's own gated commit
    let mut retry_client = client.clone();
    retry_client
        .export(logs_request(vec![log_record(0)]))
        .await
        .expect("after release the same request is admitted");
    assert_eq!(stager.staged_count(), 2);
}
