//! The OTLP/gRPC logs adapter (§4.1.2) — v1's adapter, the first
//! [`AcceptAdapter`] implementation.
//!
//! The three adapter obligations, as implemented here:
//!
//! 1. **Decode** ([`OtlpGrpcAdapter::decode_logs`]): flatten the
//!    resource → scope → `log_record` nesting into the fixed, spec-derived
//!    OTLP logs schema ([`logs_schema`]) — typed columns, zero inference on
//!    the ack path (§4.8's default path) — encoded as one Arrow IPC stream
//!    for the [`duckspout_types::StageCommitter`] port.
//! 2. **Identity** ([`tenant_from_header`]): `X-Scope-OrgID` validated per
//!    §2.2 (charset, length ≤ 150, leading `_` reserved); an absent header
//!    is single-tenant mode's fixed [`ANONYMOUS_TENANT`] — same code path,
//!    §2.2. The optional `x-duckspout-idempotency-key` rides through.
//! 3. **Error mapping** ([`AcceptAdapter::map_error`]): the OTLP error
//!    table from `duckspout-types`, spec-exact, no invented extensions.
//!
//! # String-interning (`*_strindex`) references — issue #110
//!
//! opentelemetry-proto 0.32 carries string-interning fields on the common
//! types (`KeyValue::key_strindex`, `AnyValue::StringValueStrindex`). Their
//! referent — `ProfilesDictionary.string_table` — exists **only in the
//! Profiling signal**; a logs request carries no string table to resolve
//! against. The proto's own normative instruction for non-profiling
//! receivers (trusted as published, R-trust-official-docs) is: treat the
//! presence as a non-fatal issue, warn, and process the data as if the
//! value were absent or empty. This decoder does exactly that — the
//! affected attribute key or value is treated as absent, counted in
//! [`DecodedLogs::strindex_ignored`], and the server layer disclosed it via
//! an OTLP `partial_success` warning with `rejected_log_records = 0`
//! (the spec's sanctioned warning shape; never a partial durability
//! outcome, §4.1.2).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use duckspout_types::{
    AcceptAdapter, AcceptError, DatasetId, DatasetKind, DecodedBatch, OtlpErrorClass, TenantId,
    WireError, WireRequest,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use prost::Message as _;

/// The fixed dataset OTLP log records land in (v0.1: the OTLP adapter's
/// datasets are fixed per signal; declaration wiring is the daemon's).
pub const OTLP_LOGS_DATASET: &str = "otlp_logs";

/// The fixed single-tenant-mode tenant used when no `X-Scope-OrgID` header
/// is present (§2.2 — same code path as multi-tenant, nothing retrofitted).
pub const ANONYMOUS_TENANT: &str = "anonymous";

/// §2.2's tenant-header length cap.
const TENANT_MAX_LEN: usize = 150;

/// A decoded OTLP logs request: the port-shaped batch plus the decode
/// bookkeeping the OTLP response discloses.
#[derive(Debug, Clone)]
pub struct DecodedLogs {
    /// The decoded batch, ready for the `StageCommitter` port.
    pub batch: DecodedBatch,
    /// Flattened row count (log records across all resource/scope groups).
    pub rows: usize,
    /// Profiling-only string-interning references encountered and treated
    /// as absent, per the proto's instruction (module docs; issue #110).
    pub strindex_ignored: u64,
}

/// The OTLP/gRPC adapter. Stateless: the fixed schema and the fixed
/// dataset are constants, and the error mapping is the types-crate table.
#[derive(Debug, Default, Clone, Copy)]
pub struct OtlpGrpcAdapter;

impl OtlpGrpcAdapter {
    /// Maps an OTLP error class onto a [`tonic::Status`] with the
    /// spec-exact code and a diagnostic message. Exactly the
    /// `carries_retry_info` rows get a `google.rpc.RetryInfo` detail with
    /// `retry_after_ms` (§4.5's growing delay for the ladder rows; the
    /// caller's fixed default for the rest); non-retryable rows carry no
    /// detail — nothing non-retryable pretends to be (§4.6).
    #[must_use]
    pub fn to_tonic_status(
        class: OtlpErrorClass,
        detail: &str,
        retry_after_ms: u64,
    ) -> tonic::Status {
        let code = match class.grpc_code() {
            duckspout_types::GrpcCode::InvalidArgument => tonic::Code::InvalidArgument,
            duckspout_types::GrpcCode::ResourceExhausted => tonic::Code::ResourceExhausted,
            duckspout_types::GrpcCode::Unavailable => tonic::Code::Unavailable,
        };
        let message = format!("duckspout: {detail}");
        if class.carries_retry_info() {
            let mut details = tonic_types::ErrorDetails::new();
            details.set_retry_info(Some(std::time::Duration::from_millis(retry_after_ms)));
            <tonic::Status as tonic_types::StatusExt>::with_error_details(code, message, details)
        } else {
            tonic::Status::new(code, message)
        }
    }

    /// Decodes one already-prost-decoded logs export into the fixed OTLP
    /// logs schema (obligations 1–2; module docs). `tenant_header` is the
    /// raw `X-Scope-OrgID` value when the request carried one.
    ///
    /// # Errors
    ///
    /// [`AcceptError::InvalidTenant`] per §2.2; [`AcceptError::Malformed`]
    /// if the flattened rows cannot be assembled into the fixed schema.
    pub fn decode_logs(
        &self,
        request: ExportLogsServiceRequest,
        tenant_header: Option<&str>,
        idempotency_key: Option<String>,
    ) -> Result<DecodedLogs, AcceptError> {
        let tenant = tenant_from_header(tenant_header)?;
        let mut columns = LogColumns::default();
        let mut strindex_ignored: u64 = 0;

        for resource_logs in request.resource_logs {
            let resource_attrs = resource_logs.resource.map_or_else(
                || "{}".to_owned(),
                |r| kvs_to_json(r.attributes, &mut strindex_ignored).to_string(),
            );
            for scope_logs in resource_logs.scope_logs {
                let (scope_name, scope_version) = scope_logs.scope.map_or((None, None), |s| {
                    (some_nonempty(s.name), some_nonempty(s.version))
                });
                for record in scope_logs.log_records {
                    columns.push(
                        record,
                        &resource_attrs,
                        scope_name.as_deref(),
                        scope_version.as_deref(),
                        &mut strindex_ignored,
                    );
                }
            }
        }

        let rows = columns.len();
        let records = columns.into_ipc_stream()?;
        Ok(DecodedLogs {
            batch: DecodedBatch {
                dataset: DatasetId::new(OTLP_LOGS_DATASET),
                kind: DatasetKind::Event,
                tenant,
                idempotency_key,
                records,
            },
            rows,
            strindex_ignored,
        })
    }
}

impl AcceptAdapter for OtlpGrpcAdapter {
    fn protocol(&self) -> &'static str {
        "otlp-grpc"
    }

    fn decode(&self, request: WireRequest) -> Result<DecodedBatch, AcceptError> {
        let message = ExportLogsServiceRequest::decode(request.payload)
            .map_err(|error| AcceptError::Malformed(error.to_string()))?;
        let decoded = self.decode_logs(
            message,
            request.tenant_header.as_deref(),
            request.idempotency_key,
        )?;
        Ok(decoded.batch)
    }

    fn map_error(&self, class: OtlpErrorClass) -> WireError {
        WireError {
            grpc_code: class.grpc_code(),
            retry_info: class.carries_retry_info(),
        }
    }
}

/// Validates the `X-Scope-OrgID` header per §2.2: length 1..=150, charset
/// `[A-Za-z0-9._-]`, leading `_` reserved for system tenants. An absent
/// header is single-tenant mode's [`ANONYMOUS_TENANT`].
///
/// # Errors
///
/// [`AcceptError::InvalidTenant`] naming the violated rule.
pub fn tenant_from_header(header: Option<&str>) -> Result<TenantId, AcceptError> {
    let Some(raw) = header else {
        return Ok(TenantId::new(ANONYMOUS_TENANT));
    };
    if raw.is_empty() || raw.len() > TENANT_MAX_LEN {
        return Err(AcceptError::InvalidTenant(format!(
            "length {} outside 1..={TENANT_MAX_LEN}",
            raw.len()
        )));
    }
    if let Some(bad) = raw
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(AcceptError::InvalidTenant(format!(
            "character {bad:?} outside [A-Za-z0-9._-]"
        )));
    }
    if raw.starts_with('_') {
        return Err(AcceptError::InvalidTenant(
            "leading '_' is reserved for system tenants (§2.2)".to_owned(),
        ));
    }
    Ok(TenantId::new(raw))
}

/// The fixed, spec-derived OTLP logs schema (§4.8's default path): one
/// typed column per `LogRecord` field, attributes as JSON text. Columns are
/// within the staging engine's supported payload subset.
#[must_use]
pub fn logs_schema() -> SchemaRef {
    let ts = |name: &str, nullable| {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable,
        )
    };
    Arc::new(Schema::new(vec![
        ts("ts", false),
        ts("observed_ts", true),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("attrs", DataType::Utf8, false),
        Field::new("resource_attrs", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("flags", DataType::UInt32, false),
        Field::new("event_name", DataType::Utf8, true),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
    ]))
}

/// Column builders for the fixed logs schema, filled row by row.
#[derive(Default)]
struct LogColumns {
    ts: Vec<i64>,
    observed_ts: Vec<Option<i64>>,
    severity_number: Vec<i32>,
    severity_text: Vec<Option<String>>,
    body: Vec<Option<String>>,
    attrs: Vec<String>,
    resource_attrs: Vec<String>,
    scope_name: Vec<Option<String>>,
    scope_version: Vec<Option<String>>,
    trace_id: Vec<Option<String>>,
    span_id: Vec<Option<String>>,
    flags: Vec<u32>,
    event_name: Vec<Option<String>>,
    dropped_attributes_count: Vec<u32>,
}

impl LogColumns {
    fn len(&self) -> usize {
        self.ts.len()
    }

    fn push(
        &mut self,
        record: opentelemetry_proto::tonic::logs::v1::LogRecord,
        resource_attrs: &str,
        scope_name: Option<&str>,
        scope_version: Option<&str>,
        strindex_ignored: &mut u64,
    ) {
        // Event time: `time_unix_nano`, falling back to the collector's
        // `observed_time_unix_nano` (the OTLP-recommended substitute when
        // the producer had no timestamp).
        let ts_nanos = if record.time_unix_nano != 0 {
            record.time_unix_nano
        } else {
            record.observed_time_unix_nano
        };
        self.ts.push(nanos_to_micros(ts_nanos));
        self.observed_ts.push(match record.observed_time_unix_nano {
            0 => None,
            nanos => Some(nanos_to_micros(nanos)),
        });
        self.severity_number.push(record.severity_number);
        self.severity_text.push(some_nonempty(record.severity_text));
        self.body.push(
            record
                .body
                .and_then(|b| body_to_string(b, strindex_ignored)),
        );
        self.attrs
            .push(kvs_to_json(record.attributes, strindex_ignored).to_string());
        self.resource_attrs.push(resource_attrs.to_owned());
        self.scope_name.push(scope_name.map(str::to_owned));
        self.scope_version.push(scope_version.map(str::to_owned));
        self.trace_id.push(some_hex(&record.trace_id));
        self.span_id.push(some_hex(&record.span_id));
        self.flags.push(record.flags);
        self.event_name.push(some_nonempty(record.event_name));
        self.dropped_attributes_count
            .push(record.dropped_attributes_count);
    }

    /// Finishes the columns into one Arrow IPC stream over [`logs_schema`].
    fn into_ipc_stream(self) -> Result<Bytes, AcceptError> {
        let malformed = |error: arrow::error::ArrowError| AcceptError::Malformed(error.to_string());
        let schema = logs_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(self.ts)),
            Arc::new(TimestampMicrosecondArray::from(self.observed_ts)),
            Arc::new(Int32Array::from(self.severity_number)),
            Arc::new(StringArray::from(self.severity_text)),
            Arc::new(StringArray::from(self.body)),
            Arc::new(StringArray::from(self.attrs)),
            Arc::new(StringArray::from(self.resource_attrs)),
            Arc::new(StringArray::from(self.scope_name)),
            Arc::new(StringArray::from(self.scope_version)),
            Arc::new(StringArray::from(self.trace_id)),
            Arc::new(StringArray::from(self.span_id)),
            Arc::new(UInt32Array::from(self.flags)),
            Arc::new(StringArray::from(self.event_name)),
            Arc::new(UInt32Array::from(self.dropped_attributes_count)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(malformed)?;
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema).map_err(malformed)?;
        writer.write(&batch).map_err(malformed)?;
        Ok(Bytes::from(writer.into_inner().map_err(malformed)?))
    }
}

/// Attribute list → one JSON object. A `key_strindex` reference (profiling
/// signal only — module docs) makes the pair count as ignored and the
/// attribute is processed as absent, per the proto's instruction.
fn kvs_to_json(kvs: Vec<KeyValue>, strindex_ignored: &mut u64) -> serde_json::Value {
    serde_json::Value::Object(
        kvs.into_iter()
            .filter_map(|kv| {
                if kv.key_strindex != 0 {
                    *strindex_ignored += 1;
                    return None;
                }
                Some((
                    kv.key,
                    kv.value.map_or(serde_json::Value::Null, |v| {
                        any_value_to_json(v, strindex_ignored)
                    }),
                ))
            })
            .collect(),
    )
}

fn any_value_to_json(value: AnyValue, strindex_ignored: &mut u64) -> serde_json::Value {
    match value.value {
        Some(PbValue::StringValue(s)) => serde_json::Value::String(s),
        Some(PbValue::BoolValue(b)) => serde_json::Value::Bool(b),
        Some(PbValue::IntValue(i)) => serde_json::Value::from(i),
        Some(PbValue::DoubleValue(d)) => serde_json::Number::from_f64(d)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Some(PbValue::ArrayValue(array)) => serde_json::Value::Array(
            array
                .values
                .into_iter()
                .map(|v| any_value_to_json(v, strindex_ignored))
                .collect(),
        ),
        Some(PbValue::KvlistValue(kvlist)) => kvs_to_json(kvlist.values, strindex_ignored),
        Some(PbValue::BytesValue(bytes)) => serde_json::Value::String(hex(&bytes)),
        Some(PbValue::StringValueStrindex(_)) => {
            // Profiling-only interning reference: processed as absent, per
            // the proto's instruction (module docs; issue #110).
            *strindex_ignored += 1;
            serde_json::Value::Null
        }
        None => serde_json::Value::Null,
    }
}

/// Body stringification policy: strings stay strings; anything else is its
/// JSON encoding; an empty `AnyValue` (or an interning reference) is null.
fn body_to_string(body: AnyValue, strindex_ignored: &mut u64) -> Option<String> {
    match body.value {
        Some(PbValue::StringValue(s)) => Some(s),
        Some(PbValue::StringValueStrindex(_)) => {
            *strindex_ignored += 1;
            None
        }
        Some(other) => {
            Some(any_value_to_json(AnyValue { value: Some(other) }, strindex_ignored).to_string())
        }
        None => None,
    }
}

fn nanos_to_micros(nanos: u64) -> i64 {
    i64::try_from(nanos / 1000).unwrap_or(i64::MAX)
}

fn some_nonempty(raw: String) -> Option<String> {
    if raw.is_empty() { None } else { Some(raw) }
}

fn some_hex(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        None
    } else {
        Some(hex(bytes))
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Infallible: writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}
