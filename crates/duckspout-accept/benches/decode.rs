//! Per-PR instruction-count gate (§8.6, ADR-0005; issue #46): the Accept
//! decode step (§4.3's `Accept` stage) — [`OtlpGrpcAdapter::decode_logs`]
//! flattening one OTLP `ExportLogsServiceRequest` into the fixed logs
//! schema. `scripts/instr-gate.mjs` runs this via `just instr-gate`,
//! comparing the callgrind instruction count against
//! `floors/instr-baselines/duckspout-accept-decode.json` at a +15% ceiling.
//!
//! The request is a fixed, deterministic synthetic payload (same shape as
//! `tests/otlp_grpc.rs`'s `logs_request` and `e2e_boot.rs`'s
//! `synthetic_request`): no randomness, so the instruction count this
//! measures is reproducible byte-for-byte across runs — the property the
//! per-PR gate needs (never wall-clock, ADR-0005).
//!
//! Crate-wide `missing_docs` allow: iai-callgrind's `#[library_benchmark]`
//! and `library_benchmark_group!` generate undocumented items (a wrapper
//! module, a harness fn, a group const). Workspace-wide `missing_docs` is
//! `warn` (root `Cargo.toml` `[workspace.lints]`) but CI's
//! `RUSTFLAGS=-D warnings` promotes it to a hard error; this bench binary
//! has no public surface of its own to document, so the blanket allow costs
//! nothing real.
#![allow(missing_docs)]

use std::hint::black_box;

use duckspout_accept::otlp::OtlpGrpcAdapter;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// Rows in the fixed synthetic request — large enough that the per-row loop
/// dominates the fixed decode overhead, small enough that the instrumented
/// run stays fast in CI.
const ROWS: u64 = 500;

fn str_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(value.to_owned())),
        }),
        ..Default::default()
    }
}

/// Builds the fixed request benchmarked below — same shape as
/// `tests/otlp_grpc.rs`'s fixture, deterministic timestamps and bodies.
fn synthetic_request() -> ExportLogsServiceRequest {
    let records = (0..ROWS)
        .map(|i| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + i,
            severity_number: 9,
            severity_text: "INFO".to_owned(),
            body: Some(AnyValue {
                value: Some(PbValue::StringValue(format!(
                    "instr-gate decode benchmark log line {i}"
                ))),
            }),
            attributes: vec![str_attr("k8s.pod.name", "pod-0")],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "instr-gate")],
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

// The benchmarked function: one `decode_logs` call over the fixed request
// above. NOT a doc comment (`///`) — `#[library_benchmark]` rejects any
// attribute on its function other than `#[bench::...]`/`#[benches::...]`
// (including a `#[doc = ...]` a `///` comment would desugar to).
#[library_benchmark]
fn decode_logs() {
    let adapter = OtlpGrpcAdapter;
    let request = black_box(synthetic_request());
    black_box(
        adapter
            .decode_logs(request, Some("tenant-a"), None)
            .expect("decode"),
    );
}

library_benchmark_group!(name = accept_decode; benchmarks = decode_logs);

main!(library_benchmark_groups = accept_decode);
