//! The OTLP adapter (gRPC and HTTP/protobuf), v1's adapter set (§4.1.2).
//!
//! Ⓢ bootstrap stub: the wire types compile (tonic + `opentelemetry-proto`,
//! pre-generated — no proto vendoring, no build-time protoc, SEED s§3.2) and
//! the outcome mapping is real; the decoder lands at v0.1.

use duckspout_types::{
    AcceptAdapter, AcceptError, DecodedBatch, OtlpErrorClass, WireError, WireRequest,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

/// The three OTLP export request types this adapter will decode (v0.1). The
/// aliases exist at bootstrap so the pinned wire dependencies compile as one
/// unit with the workspace (compat-matrix row 1).
pub type OtlpTraceRequest = ExportTraceServiceRequest;
/// See [`OtlpTraceRequest`].
pub type OtlpMetricsRequest = ExportMetricsServiceRequest;
/// See [`OtlpTraceRequest`].
pub type OtlpLogsRequest = ExportLogsServiceRequest;

/// The OTLP/gRPC adapter. Decoding is a v0.1 stub; the error-vocabulary
/// mapping (obligation 3, §4.1.2) is complete — it is the OTLP error table
/// from `duckspout-types`, spec-exact, no invented extensions.
#[derive(Debug, Default, Clone, Copy)]
pub struct OtlpGrpcAdapter;

impl OtlpGrpcAdapter {
    /// Maps an OTLP error class onto a [`tonic::Status`] with the spec-exact
    /// code. (The `RetryInfo` detail payload is attached by the server layer
    /// that knows the current backoff — v0.1.)
    #[must_use]
    pub fn to_tonic_status(class: OtlpErrorClass) -> tonic::Status {
        let code = match class.grpc_code() {
            duckspout_types::GrpcCode::InvalidArgument => tonic::Code::InvalidArgument,
            duckspout_types::GrpcCode::ResourceExhausted => tonic::Code::ResourceExhausted,
            duckspout_types::GrpcCode::Unavailable => tonic::Code::Unavailable,
        };
        tonic::Status::new(code, format!("duckspout: {class:?}"))
    }
}

impl AcceptAdapter for OtlpGrpcAdapter {
    fn protocol(&self) -> &'static str {
        "otlp-grpc"
    }

    fn decode(&self, _request: WireRequest) -> Result<DecodedBatch, AcceptError> {
        Err(AcceptError::NotImplemented(
            "OTLP decode lands at v0.1 (§4.1.2)",
        ))
    }

    fn map_error(&self, class: OtlpErrorClass) -> WireError {
        WireError {
            grpc_code: class.grpc_code(),
            retry_info: class.carries_retry_info(),
        }
    }
}
