//! Decode-obligation tests (§4.1.2 obligations 1–3): flattening into the
//! fixed OTLP logs schema, §2.2 tenant validation, string-interning
//! handling (issue #110), and the error-vocabulary mapping.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int32Type, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use duckspout_accept::otlp::{
    ANONYMOUS_TENANT, OTLP_LOGS_DATASET, OtlpGrpcAdapter, logs_schema, tenant_from_header,
};
use duckspout_types::{AcceptAdapter, AcceptError, OtlpErrorClass, WireRequest};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value as PbValue,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;

fn str_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(value.to_owned())),
        }),
        ..Default::default()
    }
}

fn one_record_request(record: LogRecord) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "svc-a")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "lib".to_owned(),
                    version: "1.2".to_owned(),
                    ..Default::default()
                }),
                log_records: vec![record],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn ipc_batches(records: &Bytes) -> Vec<RecordBatch> {
    arrow::ipc::reader::StreamReader::try_new(records.as_ref(), None)
        .expect("records must be a decodable IPC stream")
        .collect::<Result<_, _>>()
        .unwrap()
}

/// The full flattening: every `LogRecord` field lands in its typed column of
/// the fixed schema, attribute trees become JSON, ids become hex. Would
/// catch any column drift between the decoder and [`logs_schema`], or a
/// lossy flattening.
#[test]
fn log_record_flattens_into_the_fixed_schema() {
    let record = LogRecord {
        time_unix_nano: 1_756_600_000_123_456_789,
        observed_time_unix_nano: 1_756_600_001_000_000_000,
        severity_number: 13,
        severity_text: "WARN".to_owned(),
        body: Some(AnyValue {
            value: Some(PbValue::StringValue("the line".to_owned())),
        }),
        attributes: vec![
            str_attr("k8s.pod.name", "pod-1"),
            KeyValue {
                key: "n".to_owned(),
                value: Some(AnyValue {
                    value: Some(PbValue::IntValue(42)),
                }),
                ..Default::default()
            },
        ],
        dropped_attributes_count: 3,
        flags: 1,
        trace_id: vec![0xab; 16],
        span_id: vec![0xcd; 8],
        event_name: "my.event".to_owned(),
    };
    let decoded = OtlpGrpcAdapter
        .decode_logs(one_record_request(record), Some("tenant-a"), None)
        .unwrap();

    assert_eq!(decoded.rows, 1);
    assert_eq!(decoded.strindex_ignored, 0);
    assert_eq!(decoded.batch.dataset.as_str(), OTLP_LOGS_DATASET);
    assert_eq!(decoded.batch.tenant.as_str(), "tenant-a");

    let batches = ipc_batches(&decoded.batch.records);
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.schema(), logs_schema());
    assert_eq!(batch.num_rows(), 1);

    let col = |name: &str| batch.column_by_name(name).unwrap();
    let string_at = |name: &str| col(name).as_string::<i32>().value(0).to_owned();
    assert_eq!(
        col("ts")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_756_600_000_123_456 // ns → µs
    );
    assert_eq!(
        col("observed_ts")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_756_600_001_000_000
    );
    assert_eq!(
        col("severity_number").as_primitive::<Int32Type>().value(0),
        13
    );
    assert_eq!(string_at("severity_text"), "WARN");
    assert_eq!(string_at("body"), "the line");
    let attrs: serde_json::Value = serde_json::from_str(&string_at("attrs")).unwrap();
    assert_eq!(attrs["k8s.pod.name"], "pod-1");
    assert_eq!(attrs["n"], 42);
    let resource: serde_json::Value = serde_json::from_str(&string_at("resource_attrs")).unwrap();
    assert_eq!(resource["service.name"], "svc-a");
    assert_eq!(string_at("scope_name"), "lib");
    assert_eq!(string_at("scope_version"), "1.2");
    assert_eq!(string_at("trace_id"), "ab".repeat(16));
    assert_eq!(string_at("span_id"), "cd".repeat(8));
    assert_eq!(string_at("event_name"), "my.event");
}

/// The producer-timestamp fallback: a record without `time_unix_nano` takes
/// the collector's observed time as its event time (the OTLP-recommended
/// substitute), never a zero timestamp.
#[test]
fn missing_time_falls_back_to_observed_time() {
    let record = LogRecord {
        observed_time_unix_nano: 2_000_000_000_000_000_000,
        ..Default::default()
    };
    let decoded = OtlpGrpcAdapter
        .decode_logs(one_record_request(record), None, None)
        .unwrap();
    let batches = ipc_batches(&decoded.batch.records);
    assert_eq!(
        batches[0]
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        2_000_000_000_000_000
    );
}

/// Issue #110, at the decoder: interning references in attribute keys,
/// attribute values, and the body are each counted and treated as absent —
/// the record survives, nothing else about it is disturbed, and resolution
/// is never invented (logs carry no string table to resolve against).
#[test]
fn strindex_references_are_counted_and_treated_as_absent() {
    let record = LogRecord {
        time_unix_nano: 1,
        body: Some(AnyValue {
            value: Some(PbValue::StringValueStrindex(3)),
        }),
        attributes: vec![
            str_attr("kept", "yes"),
            // Interned KEY: the whole pair is unresolvable.
            KeyValue {
                key: String::new(),
                key_strindex: 7,
                value: Some(AnyValue {
                    value: Some(PbValue::StringValue("x".to_owned())),
                }),
            },
            // Interned VALUE under a plain key.
            KeyValue {
                key: "interned-value".to_owned(),
                value: Some(AnyValue {
                    value: Some(PbValue::StringValueStrindex(9)),
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let decoded = OtlpGrpcAdapter
        .decode_logs(one_record_request(record), None, None)
        .unwrap();

    assert_eq!(decoded.rows, 1, "the record itself is never dropped");
    assert_eq!(decoded.strindex_ignored, 3, "body + key + value references");
    let batches = ipc_batches(&decoded.batch.records);
    let body = batches[0].column_by_name("body").unwrap();
    assert!(body.is_null(0), "interned body is absent, not fabricated");
    let attrs_col = batches[0].column_by_name("attrs").unwrap();
    let attrs: serde_json::Value =
        serde_json::from_str(attrs_col.as_string::<i32>().value(0)).unwrap();
    assert_eq!(attrs["kept"], "yes");
    assert_eq!(attrs["interned-value"], serde_json::Value::Null);
    assert_eq!(
        attrs.as_object().unwrap().len(),
        2,
        "the interned-key pair is absent, everything else intact"
    );
}

/// §2.2 tenant validation, all four rules: absent → anonymous; over-long,
/// bad charset, and reserved `_` prefix each fail closed with the reason.
#[test]
fn tenant_validation_follows_the_data_model() {
    assert_eq!(tenant_from_header(None).unwrap().as_str(), ANONYMOUS_TENANT);
    assert_eq!(
        tenant_from_header(Some("Tenant-1.prod")).unwrap().as_str(),
        "Tenant-1.prod"
    );
    for bad in [
        String::new(),
        "x".repeat(151),
        "tenant/with/slash".to_owned(),
        "_self".to_owned(),
    ] {
        assert!(
            matches!(
                tenant_from_header(Some(&bad)),
                Err(AcceptError::InvalidTenant(_))
            ),
            "{bad:?} must be rejected"
        );
    }
}

/// The port-shaped decode path (obligation 1 over raw wire bytes): a valid
/// protobuf round-trips, garbage is a permanent `Malformed` — the class
/// whose wire mapping is `INVALID_ARGUMENT` without `RetryInfo`.
#[test]
fn wire_decode_accepts_protobuf_and_rejects_garbage() {
    let payload = one_record_request(LogRecord {
        time_unix_nano: 1,
        ..Default::default()
    })
    .encode_to_vec();
    let batch = OtlpGrpcAdapter
        .decode(WireRequest {
            payload: Bytes::from(payload),
            tenant_header: None,
            idempotency_key: None,
        })
        .unwrap();
    assert_eq!(batch.tenant.as_str(), ANONYMOUS_TENANT);

    let err = OtlpGrpcAdapter
        .decode(WireRequest {
            payload: Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]),
            tenant_header: None,
            idempotency_key: None,
        })
        .expect_err("garbage must not decode");
    assert!(matches!(err, AcceptError::Malformed(_)));
}

/// Obligation 3: the adapter's wire mapping is exactly the types-crate OTLP
/// error table — spec-exact codes, `RetryInfo` on precisely the UNAVAILABLE
/// rows. Would catch an adapter that invents its own vocabulary.
#[test]
fn error_mapping_is_the_types_table_verbatim() {
    let all = [
        OtlpErrorClass::MalformedPermanent,
        OtlpErrorClass::PayloadTooLarge,
        OtlpErrorClass::InflightOverCap,
        OtlpErrorClass::Throttled,
        OtlpErrorClass::RefusingIngest,
        OtlpErrorClass::ReceiptShortfall,
        OtlpErrorClass::DuplicateInFlight,
        OtlpErrorClass::StorageFailure,
    ];
    for class in all {
        let wire = OtlpGrpcAdapter.map_error(class);
        assert_eq!(wire.grpc_code, class.grpc_code());
        assert_eq!(wire.retry_info, class.carries_retry_info());
        let status = OtlpGrpcAdapter::to_tonic_status(class, "detail");
        // The numeric gRPC code on the wire equals the table's code.
        assert_eq!(status.code() as u32, class.grpc_code().code());
    }
}
