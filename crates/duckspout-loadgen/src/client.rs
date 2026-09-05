//! The real OTLP/gRPC write client (§8.4's "drives sustained load through
//! real OTLP … ingest").
//!
//! Talks the exact wire contract `duckspout-accept`'s `OtlpLogsService`
//! serves — the tenant and idempotency-key metadata header names below are
//! that contract's, not a dependency on the accept crate itself: a real
//! OTLP client speaks the wire protocol, it does not link the server (the
//! same reason `duckspout-accept`'s own dev-dependency on this pairing is
//! test-only, never a production edge). `opentelemetry-proto`'s `gen-tonic`
//! feature is the workspace's one pinned OTLP wire crate (no
//! `opentelemetry-otlp` SDK is pinned — R-third-party-first: the exporter
//! SDK bundles far more than one gRPC call needs, and the protocol crates
//! already use the bare proto + tonic pairing directly).

use std::time::Duration;

use duckspout_types::NodeId;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use tonic::transport::Channel;

use crate::journal::{LoadgenJournal, RequestIdentity};
use crate::outcome::{RaceOutcome, RequestResolution, resolve};

/// Mirrors `duckspout_accept::server::TENANT_METADATA_KEY` (§2.2, §4.1.2):
/// the wire contract, not a crate dependency (module docs).
pub const TENANT_METADATA_KEY: &str = "x-scope-orgid";

/// Mirrors `duckspout_accept::server::IDEMPOTENCY_METADATA_KEY` (§4.4.1).
pub const IDEMPOTENCY_METADATA_KEY: &str = "x-duckspout-idempotency-key";

/// Connects a real gRPC channel to `target` (e.g. `http://127.0.0.1:4317`).
///
/// # Errors
///
/// Whatever tonic reports for a malformed endpoint or an unreachable target.
pub async fn connect(target: &str) -> anyhow::Result<LogsServiceClient<Channel>> {
    Ok(LogsServiceClient::connect(target.to_owned()).await?)
}

/// Builds one synthetic OTLP export request of `record_count` log records —
/// deterministic content (a counter, not randomness: no `rand` dependency is
/// pinned, and reproducible load is easier to reason about in a fleet run's
/// journals than random load would be).
#[must_use]
pub fn synthetic_batch(record_count: usize, first_index: u64) -> ExportLogsServiceRequest {
    let records = (0..record_count as u64)
        .map(|i| {
            let index = first_index + i;
            LogRecord {
                time_unix_nano: index,
                severity_number: 9,
                severity_text: "INFO".to_owned(),
                body: Some(AnyValue {
                    value: Some(PbValue::StringValue(format!("loadgen record {index}"))),
                }),
                attributes: vec![KeyValue {
                    key: "loadgen.index".to_owned(),
                    value: Some(AnyValue {
                        value: Some(PbValue::StringValue(index.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }
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

/// Sends one batch, races the ack against `ack_timeout`, and journals
/// exactly the resolution §8.4 calls for (module docs, `crate::journal`,
/// `crate::outcome`). Returns the resolution for the caller's own run
/// statistics — the journal is the durable record; this is just so `main`
/// can print a live summary.
///
/// # Panics
///
/// If `tenant` or `request_id` are not valid gRPC metadata ASCII — both are
/// operator/loadgen-controlled inputs, never data from the wire.
pub async fn send_and_journal<W: std::io::Write + Send>(
    client: &mut LogsServiceClient<Channel>,
    journal: &LoadgenJournal<W>,
    tenant: &str,
    request_id: String,
    record_count: usize,
    first_index: u64,
    ack_timeout: Duration,
) -> RequestResolution {
    let mut request = tonic::Request::new(synthetic_batch(record_count, first_index));
    request.metadata_mut().insert(
        TENANT_METADATA_KEY,
        tenant.parse().expect("tenant is valid metadata ASCII"),
    );
    request.metadata_mut().insert(
        IDEMPOTENCY_METADATA_KEY,
        request_id
            .parse()
            .expect("request id is valid metadata ASCII"),
    );

    let raced = match tokio::time::timeout(ack_timeout, client.export(request)).await {
        Ok(result) => RaceOutcome::Settled(result.map(tonic::Response::into_inner)),
        Err(_elapsed) => RaceOutcome::DeadlineFirst,
    };

    let identity = RequestIdentity {
        request_id,
        tenant: tenant.to_owned(),
        record_count,
        first_index,
    };
    let resolution = resolve(&raced);
    match resolution {
        RequestResolution::Acked => journal.record_client_ack(&identity),
        // The one positively-confirmed case (`outcome`'s module docs,
        // HIGH-1): a transport-level failure vacuously satisfies the
        // model's `ClientTimeout` precondition.
        RequestResolution::TimedOut => journal.record_client_timeout(&identity),
        // Rejected: §3.3 has no client-journaled action for an explicit,
        // prompt rejection (`outcome`'s module docs) — accept already
        // journaled its own `Throttle`/`Refuse`. Ambiguous: journaling
        // either `ClientAck` or `ClientTimeout` here would assert something
        // the loadgen cannot confirm (`outcome`'s module docs, HIGH-1).
        // Neither is journaled to the frozen §3.3 vocabulary; `main`'s run
        // summary counts both so they stay visible.
        RequestResolution::Rejected | RequestResolution::Ambiguous => {}
    }
    resolution
}

/// A request id doubling as the OTLP idempotency key (§4.4.1): unique
/// across loadgen invocations that reuse the same `--node-id`, not just
/// within one process's lifetime (ACPR finding HIGH-2's second half —
/// `--journal-path` must be fresh per invocation, `main.rs`'s `Cli` docs,
/// but a restarted fleet member conventionally keeps the *same* `--node-id`
/// for its D-6 slot; a bare `{node}-{sequence}` would then reuse ids
/// starting from `-0` again, silently colliding with a prior run's dedup
/// key). `start_nonce` is one value captured once per process start
/// (`main.rs`, not per request, and never written anywhere durable — no
/// recovery state needed, matching the journal's own fresh-file choice).
#[must_use]
pub fn request_id(node: &NodeId, start_nonce: u128, sequence: u64) -> String {
    format!("{node}-{start_nonce}-{sequence}")
}
