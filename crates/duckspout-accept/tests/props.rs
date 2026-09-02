//! §8.5 codec laws for the accept surface, as properties over arbitrary
//! inputs (issue #40). The example-based decode tests live in `decode.rs`;
//! these quantify the two laws the ack path leans on: the decode → IPC
//! encoding is structure-preserving for ANY request shape, and §2.2 tenant
//! validation is an exact charset/length predicate, not an approximation.
//!
//! Scoping note, stated honestly: §8.5 also names `DedupCheck` key-derivation
//! determinism and the overload ladder's rung monotonicity; neither surface
//! exists yet (the dedup-window table and the ladder's rung computation are
//! issue #33's work), so their laws land with that implementation — a
//! property over absent code would test nothing.

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use duckspout_accept::otlp::{OtlpGrpcAdapter, logs_schema, tenant_from_header};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use proptest::prelude::*;

fn arb_attr() -> impl Strategy<Value = KeyValue> {
    (".{0,12}", prop::option::of(".{0,12}")).prop_map(|(key, value)| KeyValue {
        key,
        value: value.map(|s| AnyValue {
            value: Some(PbValue::StringValue(s)),
        }),
        ..Default::default()
    })
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        any::<u64>(),
        any::<i32>(),
        prop::option::of(".{0,20}"),
        prop::collection::vec(arb_attr(), 0..3),
        prop::collection::vec(any::<u8>(), 0..16),
        any::<u32>(),
    )
        .prop_map(
            |(time_unix_nano, severity_number, body, attributes, trace_id, flags)| LogRecord {
                time_unix_nano,
                severity_number,
                body: body.map(|s| AnyValue {
                    value: Some(PbValue::StringValue(s)),
                }),
                attributes,
                trace_id,
                flags,
                ..Default::default()
            },
        )
}

/// An arbitrary resource → scope → records nesting, plus its flattened row
/// count computed independently of the decoder.
fn arb_request() -> impl Strategy<Value = (ExportLogsServiceRequest, usize)> {
    prop::collection::vec(
        prop::collection::vec(prop::collection::vec(arb_record(), 0..4), 0..3),
        0..3,
    )
    .prop_map(|nests| {
        let expected: usize = nests.iter().flatten().map(Vec::len).sum();
        let request = ExportLogsServiceRequest {
            resource_logs: nests
                .into_iter()
                .map(|scopes| ResourceLogs {
                    scope_logs: scopes
                        .into_iter()
                        .map(|log_records| ScopeLogs {
                            log_records,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
        };
        (request, expected)
    })
}

fn ipc_batches(records: &Bytes) -> Vec<RecordBatch> {
    arrow::ipc::reader::StreamReader::try_new(records.as_ref(), None)
        .expect("records must be a decodable IPC stream")
        .collect::<Result<_, _>>()
        .expect("every IPC batch must decode")
}

proptest! {
    /// The decode → IPC codec law (§8.5 "OTLP and Arrow
    /// decode→canonicalize→encode round-trips"): for ANY nesting shape —
    /// empty resources, empty scopes, records at any depth — the flattened
    /// row count equals the independent count of `log_records`, and the
    /// emitted IPC stream decodes back to exactly that many rows under
    /// exactly [`logs_schema`]. Every batch acked is a batch the staging
    /// engine can read back whole. Would catch: a column builder skipped on
    /// one code path (the columns would disagree in length and the batch
    /// would fail or truncate), a nesting level silently dropped, or an IPC
    /// writer change that emits a schema the port's reader no longer
    /// round-trips.
    #[test]
    fn decode_preserves_every_record_through_the_ipc_stream(
        (request, expected) in arb_request(),
    ) {
        let decoded = OtlpGrpcAdapter
            .decode_logs(request, None, None)
            .expect("well-formed request decodes");
        prop_assert_eq!(decoded.rows, expected);
        let batches = ipc_batches(&decoded.batch.records);
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        prop_assert_eq!(total, expected);
        for batch in &batches {
            prop_assert_eq!(batch.schema(), logs_schema());
        }
    }

    /// §2.2 tenant validation as an exact predicate over ANY header value:
    /// accepted iff nonempty, ≤ 150 bytes, every char in `[A-Za-z0-9._-]`,
    /// and no leading `_` — and an accepted tenant rides through VERBATIM
    /// (identity is never canonicalized: two spellings must not collapse
    /// into one tenant's dedup/partition space). Would catch a charset
    /// drift (e.g. admitting `:` or UTF-8 lookalikes into tenant identity)
    /// or a lossy normalization (lowercasing, trimming) — either would move
    /// data between tenants.
    #[test]
    fn tenant_header_is_an_exact_charset_predicate(raw in ".{0,160}") {
        let valid = !raw.is_empty()
            && raw.len() <= 150
            && raw.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
            && !raw.starts_with('_');
        match tenant_from_header(Some(&raw)) {
            Ok(tenant) => {
                prop_assert!(valid, "invalid header {raw:?} was accepted");
                prop_assert_eq!(tenant.as_str(), raw, "tenant identity must ride verbatim");
            }
            Err(_) => prop_assert!(!valid, "valid header {raw:?} was rejected"),
        }
    }
}
