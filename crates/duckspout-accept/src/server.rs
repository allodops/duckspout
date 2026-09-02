//! The OTLP/gRPC logs service: the ack sequence over the
//! [`StageCommitter`] port (§4.3).
//!
//! ```text
//! Accept (decode, §4.1) → stage_commit (§4.3 DedupCheck+StageCommit, via
//! the port) → ClientAck
//! ```
//!
//! The response is produced **only after the port's future resolves Ok** —
//! and the port's contract is that `Ok` means the whole batch is
//! fsynced-durable (§4.3). v0.1 is single-node (RF = 1): local durable is
//! the whole replication floor, so the ack follows `StageCommit` directly.
//! The RF−1 `Receipt` wait of §4.3 slots between the port call and the ack
//! when replication lands (v0.2) — the seam is this one `await`.
//!
//! A failed stage resolves to the retryable wire vocabulary
//! (`StorageFailure` — UNAVAILABLE): the batch is not acked and a retry may
//! land on a healthy node (§4.1.2). No partial durability outcome exists:
//! the batch acks in its entirety or errors in its entirety, and
//! `partial_success` carries only the `rejected_log_records = 0` warning
//! shape for ignored profiling-interning references (module docs of
//! [`crate::otlp`]; issue #110).
//!
//! This crate builds the service; it never binds a socket — the daemon (or
//! a test harness) owns the listener and serves
//! [`OtlpLogsService::into_server`] on it (R-determinism: network I/O stays
//! out of protocol crates; the tonic service is pure request → response).

use std::sync::Arc;

use duckspout_types::{AcceptError, OtlpErrorClass, StageCommitter};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use tonic::{Request, Response, Status};

use crate::otlp::OtlpGrpcAdapter;

/// The gRPC metadata key carrying tenant identity (§2.2, §4.1.2).
pub const TENANT_METADATA_KEY: &str = "x-scope-orgid";

/// The gRPC metadata key carrying the optional idempotency token (§4.4.1).
pub const IDEMPOTENCY_METADATA_KEY: &str = "x-duckspout-idempotency-key";

/// The OTLP logs export service over any [`StageCommitter`] (module docs).
pub struct OtlpLogsService<P> {
    adapter: OtlpGrpcAdapter,
    stager: Arc<P>,
}

impl<P> OtlpLogsService<P> {
    /// Builds the service over the staging port.
    #[must_use]
    pub fn new(stager: Arc<P>) -> Self {
        Self {
            adapter: OtlpGrpcAdapter,
            stager,
        }
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

        // Accept: decode + identity (§4.1.2 obligations 1–2). Malformed and
        // invalid-tenant rejections are permanent — protocol-native reject.
        let decoded = self
            .adapter
            .decode_logs(message, tenant_header, idempotency_key.map(str::to_owned))
            .map_err(|e| reject(&e))?;

        // StageCommit via the port (§4.3). The whole batch stages durably
        // or nothing does; nothing is acked until this resolves Ok.
        if decoded.rows > 0 {
            self.stager
                .stage_commit(decoded.batch)
                .await
                .map_err(|error| {
                    OtlpGrpcAdapter::to_tonic_status(
                        OtlpErrorClass::StorageFailure,
                        &error.to_string(),
                    )
                })?;
        }

        // ClientAck (§4.3): v0.1 single-node — local durable is RF. The
        // per-partition coverage the port returned is the ack evidence; the
        // v0.2 Receipt wait consumes it here before this response is built.
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: strindex_warning(decoded.strindex_ignored),
        }))
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
    OtlpGrpcAdapter::to_tonic_status(OtlpErrorClass::MalformedPermanent, &error.to_string())
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
            )
        })
    })
}
