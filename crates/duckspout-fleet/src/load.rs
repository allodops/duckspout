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

/// One drive-load pass's shape.
#[derive(Debug, Clone, Copy)]
pub struct LoadPlan<'a> {
    /// How many OTLP export batches to send.
    pub batches: u32,
    /// Log records per batch.
    pub batch_size: u32,
    /// Wall-clock gap between batches (a sustained trickle, §8.4).
    pub interval: Duration,
    /// The `X-Scope-OrgID` this load is sent under, if any (§2.2's real
    /// multi-tenant admission header — `duckspout-fleet --tenant`'s own doc
    /// comment for why a fleet run may want its own tenant). `None` sends
    /// no header at all, which is single-tenant mode's `anonymous` — the
    /// behaviour every fleet run had before this flag existed.
    pub tenant: Option<&'a str>,
}

/// Connects to `otlp_addr` and sends `plan.batches` exports of
/// `plan.batch_size` synthetic log records each, `plan.interval` apart (a
/// sustained trickle, not a burst — §8.4's "drives sustained load"). Never returns
/// [`Err`](anyhow::Error) for a single failed export: a partial batch
/// failure is exactly the kind of fact the fleet's summary needs to
/// disclose, not turn into a hard stop mid-run.
///
/// # Errors
///
/// If the initial gRPC channel cannot even be established (the node is not
/// listening at all), or `plan.tenant` is not a valid gRPC metadata value —
/// a per-export failure is counted, not propagated.
pub async fn drive_load(
    otlp_addr: &str,
    node_label: &str,
    plan: LoadPlan<'_>,
) -> anyhow::Result<LoadResult> {
    let LoadPlan {
        batches,
        batch_size,
        interval,
        tenant,
    } = plan;
    let mut client = LogsServiceClient::connect(otlp_addr.to_owned()).await?;
    let mut result = LoadResult::default();
    let base_nanos = run_base_nanos(batches);

    for batch_index in 0..batches {
        result.batches_attempted += 1;
        let payload = synthetic_request(node_label, base_nanos, batch_index, batch_size);
        let request = tenanted_request(payload, tenant)?;
        match client.export(request).await {
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

/// Wraps `payload` in a `tonic::Request`, carrying the real
/// `X-Scope-OrgID` admission header when a tenant is configured — the same
/// key the real server reads
/// ([`duckspout_accept::server::TENANT_METADATA_KEY`]), taken from that
/// crate rather than hand-copied here.
///
/// # Errors
///
/// If `tenant` is not a valid ASCII metadata value (the server's own
/// `[A-Za-z0-9._-]` tenant charset is a strict subset of what this
/// accepts, so a tenant that passes here can still be refused there — with
/// the real typed error, which is the point of sending the real header
/// rather than pre-validating it locally).
fn tenanted_request(
    payload: ExportLogsServiceRequest,
    tenant: Option<&str>,
) -> anyhow::Result<tonic::Request<ExportLogsServiceRequest>> {
    let mut request = tonic::Request::new(payload);
    if let Some(tenant) = tenant {
        let value: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = tenant
            .parse()
            .map_err(|e| anyhow::anyhow!("--tenant {tenant:?} is not a valid header value: {e}"))?;
        request
            .metadata_mut()
            .insert(duckspout_accept::server::TENANT_METADATA_KEY, value);
    }
    Ok(request)
}

/// The event-time base this run's synthetic records start from: far enough
/// in the past that `batches` seconds of them still end at roughly **now**,
/// so every window this load produces closes inside the run.
///
/// # Why this is wall-clock and not a fixed constant (a real failure this
/// caught, issue #204)
///
/// This used to be a hardcoded 2026-ish nanosecond constant, which meant
/// EVERY fleet run — with any seed, on any day — produced the exact same
/// event-time windows. Against a **persistent** catalog + lake (which is
/// the documented deployment model: `main.rs`'s own `--s3-prefix` doc
/// comment pins `DATA_PATH` for the catalog's whole lifetime), the second
/// and every later run therefore had nothing new to drain: the watermark
/// ledger reconstructed at boot already covered those windows, so the drain
/// dropped them (`DropWindow`) instead of committing anything, and no
/// `PutPart` was ever journaled again. Empirically that turned
/// `tests/fault_injection.rs`'s mid-drain-kill scenario — whose whole
/// premise is a real `PutPart`→`LakeCommit` window to land a kill inside —
/// into a 40-second timeout as soon as any other scenario had run against
/// the same catalog first. A run-varying base fixes it at the source: each
/// run drains its own fresh windows, exactly as a real ingest workload
/// would.
///
/// Saturates rather than panicking on a pre-epoch or absurd clock — a load
/// generator must not itself kill the fleet run over a clock oddity (R-5).
fn run_base_nanos(batches: u32) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    // One second of event time per batch (below), plus a second of slack.
    let span_nanos = u64::from(batches).saturating_add(1) * 1_000_000_000;
    now.saturating_sub(span_nanos)
}

/// One synthetic batch, tagged with `node_label` and `batch_index` so a
/// journal reader can tell fleet-smoke traffic apart from anything else —
/// not a real workload shape, just enough distinct content per record to
/// exercise real encode/decode/dedup-key derivation. `base_nanos` is this
/// RUN's own event-time origin ([`run_base_nanos`]).
fn synthetic_request(
    node_label: &str,
    base_nanos: u64,
    batch_index: u32,
    count: u32,
) -> ExportLogsServiceRequest {
    let str_attr = |k: &str, v: String| KeyValue {
        key: k.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(v)),
        }),
        ..Default::default()
    };
    let batch_nanos = base_nanos.saturating_add(u64::from(batch_index) * 1_000_000_000);
    let records = (0..count)
        .map(|i| LogRecord {
            time_unix_nano: batch_nanos + u64::from(i),
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

    /// `--tenant` must reach the wire as the REAL admission header the real
    /// server reads, under the real key — a request that carried the tenant
    /// anywhere else (or nowhere) would silently land every fleet run in
    /// the `anonymous` tenant, which is exactly the partition collision
    /// `tests/fault_injection.rs` depends on avoiding.
    #[test]
    fn a_configured_tenant_rides_the_real_x_scope_orgid_header() {
        let payload = synthetic_request("fleet-0-0", 0, 0, 1);
        let request = tenanted_request(payload, Some("fleet204-abc")).unwrap();
        assert_eq!(
            request
                .metadata()
                .get(duckspout_accept::server::TENANT_METADATA_KEY)
                .map(|value| value.to_str().unwrap()),
            Some("fleet204-abc")
        );
    }

    /// No `--tenant` must send NO header at all — single-tenant mode's
    /// `anonymous`, i.e. every fleet run's behaviour before the flag
    /// existed. An empty-string header would instead be a rejected tenant.
    #[test]
    fn no_tenant_sends_no_header_at_all() {
        let payload = synthetic_request("fleet-0-0", 0, 0, 1);
        let request = tenanted_request(payload, None).unwrap();
        assert!(
            request
                .metadata()
                .get(duckspout_accept::server::TENANT_METADATA_KEY)
                .is_none()
        );
    }

    /// A tenant that cannot be a header value at all fails closed, rather
    /// than being silently dropped into an untenanted run.
    #[test]
    fn a_tenant_that_is_not_a_valid_header_value_is_rejected() {
        let payload = synthetic_request("fleet-0-0", 0, 0, 1);
        assert!(tenanted_request(payload, Some("bad\nvalue")).is_err());
    }

    /// The exact regression `run_base_nanos`'s own doc comment records: two
    /// runs must NOT replay the same event-time windows, or a fleet run
    /// against a catalog that already holds those windows has nothing left
    /// to drain and never journals another `PutPart`. Pinned two ways —
    /// the base really moves between runs, and it really lands in the
    /// recent past (a future base would produce windows that never close
    /// inside the run, which is the same failure from the other side).
    #[test]
    fn each_run_gets_its_own_event_time_window_set_in_the_recent_past() {
        let first = run_base_nanos(20);
        std::thread::sleep(Duration::from_millis(5));
        let second = run_base_nanos(20);
        assert!(
            second > first,
            "a later run must start from a later event-time base ({first} → {second})"
        );

        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap();
        assert!(first < now, "the base must be in the past");
        // 20 batches at one second of event time each, plus a second of
        // slack: the run's LAST batch must still land at or before now.
        assert!(
            first + 21 * 1_000_000_000 <= now + 1_000_000_000,
            "the run's whole event-time span must end at roughly now, not in the future"
        );
    }

    /// `synthetic_request`'s own doc comment claims "enough distinct
    /// content per record to exercise real ... dedup-key derivation" — this
    /// is what would catch a regression collapsing every record onto the
    /// same body/timestamp (silently defeating that claim).
    #[test]
    fn synthetic_request_produces_the_requested_record_count_with_distinct_content() {
        let base = run_base_nanos(10);
        let request = synthetic_request("fleet-0-0", base, 2, 4);
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
        let other_batch = synthetic_request("fleet-0-0", base, 3, 4);
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
