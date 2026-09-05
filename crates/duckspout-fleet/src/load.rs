//! The placeholder drive-load driver (§8.4's own scoping language: "drive
//! load via the loadgen once its own issue lands, or a placeholder driver
//! until then" — issue #201; full load-generation semantics, retries, and
//! `ClientAck`/`ClientTimeout` journaling are `duckspout-loadgen`'s own
//! scope, issue #202, deliberately NOT reimplemented here).
//!
//! Sends real OTLP/gRPC `ExportLogsServiceRequest` batches — the identical
//! client shape `duckspout-daemon/tests/otlp_e2e.rs` and
//! `duckspout-daemon/tests/common/capture.rs` already exercise — at a
//! sustained cadence (`--load-interval-ms` between batches, not one burst)
//! against every fleet member, proving the fleet's real accept path is
//! actually live end to end. This is a **smoke** driver: it counts
//! attempted vs. accepted batches and returns that count, nothing else — no
//! per-record identity tracking, no dedup exercise, no acked-loss
//! bookkeeping (§8.4's real judge, issues #205–#208, is a post-pass over
//! journals the real loadgen writes, not this).

use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// How one node's drive-load pass went.
#[derive(Debug, Clone, Copy, Default)]
pub struct LoadResult {
    pub batches_attempted: u32,
    pub batches_accepted: u32,
    pub records_accepted: u64,
}

impl LoadResult {
    #[must_use]
    pub fn fully_accepted(&self) -> bool {
        self.batches_attempted > 0 && self.batches_attempted == self.batches_accepted
    }
}

/// Connects to `otlp_addr` and sends `batches` exports of `batch_size`
/// synthetic log records each, `interval` apart (a sustained trickle, not a
/// burst — §8.4's "drives sustained load"). Never returns
/// [`Err`](anyhow::Error) for a single failed export: a partial batch
/// failure is exactly the kind of fact the fleet's summary needs to
/// disclose, not turn into a hard stop mid-run.
///
/// # Errors
///
/// Only if the initial gRPC channel cannot even be established (the node is
/// not listening at all) — a per-export failure is counted, not
/// propagated.
pub async fn drive_load(
    otlp_addr: &str,
    node_label: &str,
    batches: u32,
    batch_size: u32,
    interval: Duration,
) -> anyhow::Result<LoadResult> {
    let mut client = LogsServiceClient::connect(otlp_addr.to_owned()).await?;
    let mut result = LoadResult::default();

    for batch_index in 0..batches {
        result.batches_attempted += 1;
        let request = synthetic_request(node_label, batch_index, batch_size);
        match client.export(tonic::Request::new(request)).await {
            Ok(response) => {
                let full_success = response.into_inner().partial_success.is_none();
                if full_success {
                    result.batches_accepted += 1;
                    result.records_accepted += u64::from(batch_size);
                } else {
                    tracing::warn!(
                        node = node_label,
                        batch_index,
                        "OTLP export reported a partial success"
                    );
                }
            }
            Err(status) => {
                tracing::warn!(node = node_label, batch_index, %status, "OTLP export failed");
            }
        }
        if batch_index + 1 < batches {
            tokio::time::sleep(interval).await;
        }
    }

    Ok(result)
}

/// One synthetic batch, tagged with `node_label` and `batch_index` so a
/// journal reader can tell fleet-smoke traffic apart from anything else —
/// not a real workload shape, just enough distinct content per record to
/// exercise real encode/decode/dedup-key derivation.
fn synthetic_request(node_label: &str, batch_index: u32, count: u32) -> ExportLogsServiceRequest {
    let str_attr = |k: &str, v: String| KeyValue {
        key: k.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(v)),
        }),
        ..Default::default()
    };
    let base_nanos = 1_756_600_000_000_000_000_u64 + u64::from(batch_index) * 1_000_000_000;
    let records = (0..count)
        .map(|i| LogRecord {
            time_unix_nano: base_nanos + u64::from(i),
            severity_number: 9,
            severity_text: "INFO".to_owned(),
            body: Some(AnyValue {
                value: Some(PbValue::StringValue(format!(
                    "duckspout-fleet smoke load: {node_label} batch {batch_index} record {i}"
                ))),
            }),
            attributes: vec![str_attr("duckspout.fleet.node", node_label.to_owned())],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "duckspout-fleet-smoke".to_owned())],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_accepted_is_true_only_when_every_attempted_batch_landed() {
        let none_attempted = LoadResult::default();
        assert!(
            !none_attempted.fully_accepted(),
            "zero attempted batches is not the same as full acceptance"
        );

        let all_accepted = LoadResult {
            batches_attempted: 3,
            batches_accepted: 3,
            records_accepted: 75,
        };
        assert!(all_accepted.fully_accepted());

        let partial = LoadResult {
            batches_attempted: 3,
            batches_accepted: 2,
            records_accepted: 50,
        };
        assert!(!partial.fully_accepted());
    }

    /// `synthetic_request`'s own doc comment claims "enough distinct
    /// content per record to exercise real ... dedup-key derivation" — this
    /// is what would catch a regression collapsing every record onto the
    /// same body/timestamp (silently defeating that claim).
    #[test]
    fn synthetic_request_produces_the_requested_record_count_with_distinct_content() {
        let request = synthetic_request("fleet-0-0", 2, 4);
        let resource_logs = &request.resource_logs[0];
        assert_eq!(
            resource_logs.resource.as_ref().unwrap().attributes[0]
                .value
                .as_ref()
                .unwrap()
                .value,
            Some(PbValue::StringValue("duckspout-fleet-smoke".to_owned()))
        );

        let records = &resource_logs.scope_logs[0].log_records;
        assert_eq!(records.len(), 4);

        let bodies: std::collections::HashSet<String> = records
            .iter()
            .map(|record| match &record.body.as_ref().unwrap().value {
                Some(PbValue::StringValue(s)) => s.clone(),
                other => panic!("expected a string body, got {other:?}"),
            })
            .collect();
        assert_eq!(bodies.len(), 4, "every record in a batch must be distinct");

        let timestamps: std::collections::HashSet<u64> =
            records.iter().map(|r| r.time_unix_nano).collect();
        assert_eq!(
            timestamps.len(),
            4,
            "every record in a batch must carry a distinct timestamp"
        );

        // A different batch_index must not collide with batch 2's records.
        let other_batch = synthetic_request("fleet-0-0", 3, 4);
        let other_timestamps: std::collections::HashSet<u64> = other_batch.resource_logs[0]
            .scope_logs[0]
            .log_records
            .iter()
            .map(|r| r.time_unix_nano)
            .collect();
        assert!(
            timestamps.is_disjoint(&other_timestamps),
            "different batches must not share timestamps"
        );
    }
}
