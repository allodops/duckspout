//! The OTLP/gRPC logs service: admission (§4.6), the ack sequence over the
//! [`StageCommitter`] port (§4.3), and the ladder's client-visible half
//! (§4.5).
//!
//! ```text
//! Accept (payload cap → decode → in-flight cap, §4.6) →
//! stage_commit (ladder gate + DedupCheck + StageCommit, via the port) →
//! ClientAck
//! ```
//!
//! The response is produced **only after the port's future resolves** —
//! `Committed` and `DuplicateAcked` (the §4.4.1 replay: same ack a second
//! time, never a second copy) ack; everything else maps onto the
//! spec-exact OTLP error table, with `google.rpc.RetryInfo` on exactly the
//! retryable rows. v0.1 is single-node (RF = 1): local durable is the
//! whole replication floor, so the ack follows `StageCommit` directly; the
//! RF−1 `Receipt` wait of §4.3 slots into the one `await` when replication
//! lands (v0.2).
//!
//! Admission constants (§4.6): over-`max_payload_bytes` is
//! `RESOURCE_EXHAUSTED` **without** `RetryInfo` (retrying an over-sized
//! payload can never succeed); decoded-but-uncommitted bytes over
//! `admission.max_inflight_bytes` throttle. Both limits arrive as
//! [`AdmissionConfig`] values from the composition layer — the daemon owns
//! config reading and the memory-budget autodetection; this crate never
//! reads an environment.
//!
//! This crate builds the service; it never binds a socket — the daemon (or
//! a test harness) owns the listener and serves
//! [`OtlpLogsService::into_server`] on it (R-determinism: network I/O stays
//! out of protocol crates; the tonic service is pure request → response).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use duckspout_types::{
    AcceptError, OtlpErrorClass, StageCommitter, StageError, StageOutcome, TraceEvent, TraceSink,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use prost::Message as _;
use tonic::{Request, Response, Status};

use crate::otlp::OtlpGrpcAdapter;

/// The gRPC metadata key carrying tenant identity (§2.2, §4.1.2).
pub const TENANT_METADATA_KEY: &str = "x-scope-orgid";

/// The gRPC metadata key carrying the optional idempotency token (§4.4.1).
pub const IDEMPOTENCY_METADATA_KEY: &str = "x-duckspout-idempotency-key";

/// The `RetryInfo` delay for retryable outcomes that carry no
/// ladder-computed delay (`StorageFailure`, `DuplicateInFlight`): a retry
/// may succeed as soon as the fault clears, so the floor of §4.5's delay
/// band is the honest instruction. A constant, not a knob (R-12).
pub const DEFAULT_RETRY_DELAY_MS: u64 = duckspout_types::status::THROTTLE_RETRY_MIN_MS;

/// The §4.6 admission limits, as values: `max_payload_bytes` (default
/// 4 MiB — gRPC's and the collector's shared default) and
/// `admission.max_inflight_bytes` (default 10% of the memory budget,
/// autodetected **by the daemon** at startup). No `Default` impl on
/// purpose: an admission posture is stated by the composition layer or the
/// service is not constructed — a silently unlimited default would be a
/// missing cap wearing a type.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionConfig {
    /// `max_payload_bytes` (§4.6): over-cap is non-retryable.
    pub max_payload_bytes: usize,
    /// `admission.max_inflight_bytes` (§4.6): decoded-but-uncommitted
    /// bytes in flight; beyond it, throttle.
    pub max_inflight_bytes: u64,
}

/// The OTLP logs export service over any [`StageCommitter`] (module docs).
pub struct OtlpLogsService<P> {
    adapter: OtlpGrpcAdapter,
    stager: Arc<P>,
    admission: AdmissionConfig,
    /// Decoded-but-uncommitted bytes currently in flight (§4.6).
    inflight_bytes: AtomicU64,
    /// The §3.7 capture seam: `ClientAck`, `Throttle`, and `Refuse` journal
    /// here (docs/trace-mapping.md's attributions; `Accept` rides the
    /// staging-side admission gate). `None` — the production default until
    /// the `conformance` row arms — journals nothing.
    trace: Option<Arc<dyn TraceSink>>,
}

impl<P> OtlpLogsService<P> {
    /// Builds the service over the staging port with the §4.6 admission
    /// posture.
    #[must_use]
    pub fn new(stager: Arc<P>, admission: AdmissionConfig) -> Self {
        Self {
            adapter: OtlpGrpcAdapter,
            stager,
            admission,
            inflight_bytes: AtomicU64::new(0),
            trace: None,
        }
    }

    /// Journals this service's §3.3 events (`ClientAck`, `Throttle`,
    /// `Refuse`) through `sink` (§3.7; the trace-conformance harness's
    /// capture side).
    #[must_use]
    pub fn with_trace_sink(mut self, sink: Arc<dyn TraceSink>) -> Self {
        self.trace = Some(sink);
        self
    }

    /// Wraps the service into the tonic server type for the composition
    /// layer (the daemon binds it; this crate never does).
    #[must_use]
    pub fn into_server(self) -> LogsServiceServer<Self>
    where
        P: StageCommitter + 'static,
    {
        LogsServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl<P: StageCommitter + 'static> LogsService for OtlpLogsService<P> {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let (metadata, _, message) = request.into_parts();
        let tenant_header = metadata_str(&metadata, TENANT_METADATA_KEY)?;
        let idempotency_key = metadata_str(&metadata, IDEMPOTENCY_METADATA_KEY)?;

        // §4.6 payload cap, before any decode work: over-cap is
        // RESOURCE_EXHAUSTED without RetryInfo — instructing a retry would
        // manufacture a loop.
        let payload_len = message.encoded_len();
        if payload_len > self.admission.max_payload_bytes {
            return Err(OtlpGrpcAdapter::to_tonic_status(
                OtlpErrorClass::PayloadTooLarge,
                &format!(
                    "payload {payload_len} B over max_payload_bytes {}",
                    self.admission.max_payload_bytes
                ),
                0,
            ));
        }

        // Accept: decode + identity (§4.1.2 obligations 1–2). Malformed and
        // invalid-tenant rejections are permanent — protocol-native reject.
        let decoded = self
            .adapter
            .decode_logs(message, tenant_header, idempotency_key.map(str::to_owned))
            .map_err(|e| reject(&e))?;

        let partial_success = strindex_warning(decoded.strindex_ignored);
        // An empty export succeeds without touching the port (OTLP: empty
        // exports are successes; nothing to stage, nothing to guard).
        if decoded.rows > 0 {
            // §4.6 in-flight cap over the decoded bytes; the guard releases
            // them once the stage resolves either way.
            let _guard = InflightGuard::admit(
                &self.inflight_bytes,
                decoded.batch.records.len() as u64,
                self.admission.max_inflight_bytes,
            )?;

            // DedupCheck + StageCommit via the port (§4.3–§4.5). The whole
            // batch stages durably or nothing does; nothing is acked until
            // this resolves.
            let outcome = match self.stager.stage_commit(decoded.batch).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    // §3.3's ladder resolutions journal their action name
                    // (§3.7): rung 2 is Throttle, rung 3 is Refuse. Other
                    // failures (storage, malformed IPC) are not §3.3
                    // actions and journal nothing — the model has no
                    // failed-StageCommit transition.
                    if let Some(trace) = &self.trace {
                        match &error {
                            StageError::Throttled { .. } => trace.record(TraceEvent::Throttle),
                            StageError::RefusingIngest { .. } => trace.record(TraceEvent::Refuse),
                            StageError::MalformedRecords(_) | StageError::Backend(_) => {}
                        }
                    }
                    return Err(stage_status(&error));
                }
            };
            match outcome {
                // ClientAck (§4.3): v0.1 single-node — local durable is RF.
                // The coverage is the ack evidence; the v0.2 Receipt wait
                // consumes it here before this response is built. The
                // journal precedes the wire response (§3.7: journal before
                // the corresponding external effect).
                StageOutcome::Committed(_coverage) => {
                    if let Some(trace) = &self.trace {
                        trace.record(TraceEvent::ClientAck);
                    }
                }
                // §4.4.1: a duplicate of an ack-complete entry replays the
                // original success — same ack, no second staged copy (R-2).
                // The warning body is recomputed from the identical payload,
                // so replayed counts match the original's by construction.
                // No journal: the §3.3 resolution IS the DedupCheck the
                // staging side journaled — a second ClientAck would be a
                // step the model cannot take.
                StageOutcome::DuplicateAcked(_coverage) => {}
                StageOutcome::DuplicateInFlight => {
                    return Err(OtlpGrpcAdapter::to_tonic_status(
                        OtlpErrorClass::DuplicateInFlight,
                        "duplicate of an in-flight request (§4.4.1)",
                        DEFAULT_RETRY_DELAY_MS,
                    ));
                }
            }
        }

        Ok(Response::new(ExportLogsServiceResponse { partial_success }))
    }
}

/// RAII accounting of decoded-but-uncommitted bytes (§4.6): admitted on
/// construction, released on drop — success, error, and panic paths all
/// release exactly once.
struct InflightGuard<'a> {
    counter: &'a AtomicU64,
    bytes: u64,
}

impl<'a> InflightGuard<'a> {
    fn admit(counter: &'a AtomicU64, bytes: u64, max: u64) -> Result<Self, Status> {
        let previous = counter.fetch_add(bytes, Ordering::SeqCst);
        if previous.saturating_add(bytes) > max {
            counter.fetch_sub(bytes, Ordering::SeqCst);
            return Err(OtlpGrpcAdapter::to_tonic_status(
                OtlpErrorClass::InflightOverCap,
                &format!("decoded bytes in flight over admission.max_inflight_bytes {max}"),
                DEFAULT_RETRY_DELAY_MS,
            ));
        }
        Ok(Self { counter, bytes })
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// The `rejected_log_records = 0` warning for ignored profiling-interning
/// references (the OTLP-sanctioned fully-accepted warning shape; see
/// [`crate::otlp`] module docs). `None` when there is nothing to disclose.
fn strindex_warning(strindex_ignored: u64) -> Option<ExportLogsPartialSuccess> {
    (strindex_ignored > 0).then(|| ExportLogsPartialSuccess {
        rejected_log_records: 0,
        error_message: format!(
            "ignored {strindex_ignored} profiling-only string-interning reference(s) \
             (*_strindex); logs carry no string table — values treated as absent"
        ),
    })
}

/// A permanent decode rejection → its spec-exact status (§4.1.2).
fn reject(error: &AcceptError) -> Status {
    OtlpGrpcAdapter::to_tonic_status(OtlpErrorClass::MalformedPermanent, &error.to_string(), 0)
}

/// A port error → its spec-exact status: the ladder rows carry §4.5's
/// growing delay computed by the stager; everything else is the retryable
/// storage-failure vocabulary with the fixed default delay.
fn stage_status(error: &StageError) -> Status {
    match &error {
        StageError::Throttled { retry_after_ms } => OtlpGrpcAdapter::to_tonic_status(
            OtlpErrorClass::Throttled,
            &error.to_string(),
            *retry_after_ms,
        ),
        StageError::RefusingIngest { retry_after_ms } => OtlpGrpcAdapter::to_tonic_status(
            OtlpErrorClass::RefusingIngest,
            &error.to_string(),
            *retry_after_ms,
        ),
        StageError::MalformedRecords(_) | StageError::Backend(_) => {
            OtlpGrpcAdapter::to_tonic_status(
                OtlpErrorClass::StorageFailure,
                &error.to_string(),
                DEFAULT_RETRY_DELAY_MS,
            )
        }
    }
}

/// Reads one metadata value as a string; a non-UTF-8 value in an identity
/// header is a permanent reject, not a silent skip (§2.2 fails closed).
fn metadata_str<'m>(
    metadata: &'m tonic::metadata::MetadataMap,
    key: &'static str,
) -> Result<Option<&'m str>, Status> {
    metadata.get(key).map_or(Ok(None), |value| {
        value.to_str().map(Some).map_err(|_| {
            OtlpGrpcAdapter::to_tonic_status(
                OtlpErrorClass::MalformedPermanent,
                &format!("metadata {key} is not valid UTF-8"),
                0,
            )
        })
    })
}
