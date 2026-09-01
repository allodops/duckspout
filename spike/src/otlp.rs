//! Spike OTLP accept path (§4.1) — issue #24.
//!
//! A tonic gRPC `LogsService` that decodes `ExportLogsServiceRequest` into
//! rows and writes them through the Task-1 ingest core: one transaction per
//! export batch, ack only after COMMIT returns (the `StageCommit`-then-ack
//! shape of §4.3, minus replication — the spike has no peers).
//!
//! Throwaway spike code — instructive, not production (spike/README.md).

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as PbValue};
use tonic::{Request, Response, Status};

use crate::ingest::{IngestCore, LogRow};

/// Single-writer wrapper over the ingest core: owns the connection and the
/// dense per-(partition, origin) `seq` counter (§4.2.3). DuckDB is a
/// single-writer-process engine, so every export batch funnels through here.
pub struct HotWriter {
    core: IngestCore,
    table: String,
    next_seq: i64,
}

impl HotWriter {
    pub fn open(path: &std::path::Path, table: &str) -> Result<Self> {
        let core = IngestCore::open(path)?;
        core.create_window(table)?;
        // Resume the dense seq after the last durable row (spike-grade
        // stand-in for the applied-watermark row of §4.2.4).
        let next_seq: i64 = core
            .conn_query_row(&format!("SELECT coalesce(max(seq) + 1, 0) FROM {table}"))
            .context("resume seq")?;
        Ok(Self {
            core,
            table: table.to_string(),
            next_seq,
        })
    }

    /// Assign dense seqs, insert in ONE transaction, return committed count.
    pub fn stage_commit(&mut self, mut rows: Vec<LogRow>) -> Result<usize> {
        for (i, r) in rows.iter_mut().enumerate() {
            r.seq = self.next_seq + i as i64;
        }
        let n = rows.len();
        self.core.insert_batch(&self.table, &rows)?;
        // Advance only after COMMIT returned: a failed commit must not burn
        // seqs the table never got.
        self.next_seq += n as i64;
        Ok(n)
    }

    pub fn count(&self) -> Result<i64> {
        self.core.count(&self.table)
    }

    pub fn core(&self) -> &IngestCore {
        &self.core
    }
}

/// The spike's OTLP logs adapter: flatten + stage-commit + ack-after-commit.
pub struct OtlpLogsService {
    writer: Arc<Mutex<HotWriter>>,
}

impl OtlpLogsService {
    pub fn new(writer: Arc<Mutex<HotWriter>>) -> Self {
        Self { writer }
    }

    pub fn into_server(self) -> LogsServiceServer<Self> {
        LogsServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<ExportLogsServiceResponse>, Status> {
        let rows = flatten_request(request.into_inner());
        if rows.is_empty() {
            // Nothing to stage; OTLP says an empty export succeeds.
            return Ok(Response::new(ExportLogsServiceResponse {
                partial_success: None,
            }));
        }
        let writer = Arc::clone(&self.writer);
        // The commit is a blocking fsync-bound call — off the async reactor.
        let committed = tokio::task::spawn_blocking(move || {
            let mut w = writer.lock().expect("writer poisoned");
            w.stage_commit(rows)
        })
        .await
        .map_err(|e| Status::internal(format!("writer task: {e}")))?
        .map_err(|e| Status::internal(format!("stage-commit: {e:#}")))?;
        // Ack strictly after COMMIT returned (§4.3; batch acked in its
        // entirety — no partial durability outcome, per §4.1).
        let _ = committed;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

/// Flatten resource → scope → log_record nesting into flat rows. Resource
/// and scope identity ride in the `attrs` JSON alongside record attributes
/// and the trace correlation ids (spike-grade; the real adapter gets typed
/// columns per §4.8's fixed OTLP schema).
pub fn flatten_request(msg: ExportLogsServiceRequest) -> Vec<LogRow> {
    let mut rows = Vec::new();
    for rl in msg.resource_logs {
        let resource_attrs: serde_json::Value = rl
            .resource
            .map(|r| kvs_to_json(r.attributes))
            .unwrap_or(serde_json::Value::Null);
        for sl in rl.scope_logs {
            let scope_name = sl
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            for rec in sl.log_records {
                let ts_nanos = if rec.time_unix_nano != 0 {
                    rec.time_unix_nano
                } else {
                    rec.observed_time_unix_nano
                };
                let attrs = serde_json::json!({
                    "resource": resource_attrs,
                    "scope": scope_name,
                    "attrs": kvs_to_json(rec.attributes),
                    "trace_id": hex(&rec.trace_id),
                    "span_id": hex(&rec.span_id),
                    "severity_text": rec.severity_text,
                });
                rows.push(LogRow {
                    origin: "node-a/1".to_string(),
                    seq: 0, // assigned by HotWriter::stage_commit
                    ts_micros: (ts_nanos / 1000) as i64,
                    severity: rec.severity_number,
                    body: rec.body.map(any_value_to_string).unwrap_or_default(),
                    attrs: attrs.to_string(),
                });
            }
        }
    }
    rows
}

fn kvs_to_json(kvs: Vec<KeyValue>) -> serde_json::Value {
    serde_json::Value::Object(
        kvs.into_iter()
            .map(|kv| {
                (
                    kv.key,
                    kv.value
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null),
                )
            })
            .collect(),
    )
}

fn any_value_to_json(v: AnyValue) -> serde_json::Value {
    match v.value {
        Some(PbValue::StringValue(s)) => serde_json::Value::String(s),
        Some(PbValue::BoolValue(b)) => serde_json::Value::Bool(b),
        Some(PbValue::IntValue(i)) => serde_json::Value::from(i),
        Some(PbValue::DoubleValue(d)) => serde_json::Number::from_f64(d)
            .map(serde_json::Value::Number)
            .unwrap_or_default(),
        Some(PbValue::ArrayValue(a)) => {
            serde_json::Value::Array(a.values.into_iter().map(any_value_to_json).collect())
        }
        Some(PbValue::KvlistValue(kvl)) => kvs_to_json(kvl.values),
        Some(PbValue::BytesValue(b)) => serde_json::Value::String(hex(&b)),
        // OTLP 0.32 grew string-interning (`*_strindex`) variants; resolving
        // them needs the enclosing message's string table. The spike does not
        // implement interning — surface, don't silently drop (lesson for the
        // real adapter: the §4.1 decode obligation includes the string table).
        Some(other @ PbValue::StringValueStrindex(_)) => {
            serde_json::Value::String(format!("<unresolved strindex: {other:?}>"))
        }
        None => serde_json::Value::Null,
    }
}

fn any_value_to_string(v: AnyValue) -> String {
    match v.value {
        Some(PbValue::StringValue(s)) => s,
        other => any_value_to_json(AnyValue { value: other }).to_string(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A synthetic OTLP export request with `n` log records under one resource
/// and one scope — used by the bench and the integration test.
pub fn synthetic_request(n: usize) -> ExportLogsServiceRequest {
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    let str_attr = |k: &str, v: &str| KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(PbValue::StringValue(v.to_string())),
        }),
        ..Default::default()
    };
    let records = (0..n as u64)
        .map(|i| LogRecord {
            time_unix_nano: 1_756_600_000_000_000_000 + i,
            severity_number: (i % 24) as i32,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(PbValue::StringValue(format!("synthetic otlp log line {i}"))),
            }),
            attributes: vec![
                str_attr("k8s.pod.name", &format!("pod-{}", i % 16)),
                KeyValue {
                    key: "i".to_string(),
                    value: Some(AnyValue {
                        value: Some(PbValue::IntValue(i as i64)),
                    }),
                    ..Default::default()
                },
            ],
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![str_attr("service.name", "spike-bench")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "spike".to_string(),
                    ..Default::default()
                }),
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}
