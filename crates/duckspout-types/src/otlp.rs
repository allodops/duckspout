//! The OTLP error table (§4.1.2, §4.5, §4.6), homed here by §10.1.
//!
//! An accept adapter's third obligation is to map `DuckSpout`'s
//! admission/overload outcomes onto its protocol's native error vocabulary —
//! for OTLP, the spec's own retryable status table, with no invented
//! extensions (§4.1.2). This module is that table as a closed enum: every
//! non-ack outcome class, each with its spec-exact gRPC code and whether a
//! `RetryInfo` detail accompanies it.

use serde::{Deserialize, Serialize};

/// The gRPC status codes the OTLP error table uses. A closed subset — the
/// table never invents codes (§4.1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GrpcCode {
    /// `INVALID_ARGUMENT` (3): permanently rejected malformed input.
    InvalidArgument,
    /// `UNAVAILABLE` (14): the retryable signal; conformant OTLP clients
    /// already back off correctly (§4.5).
    Unavailable,
    /// `RESOURCE_EXHAUSTED` (8): over-cap payload; non-retryable (§4.6).
    ResourceExhausted,
}

impl GrpcCode {
    /// The numeric gRPC status code.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            Self::InvalidArgument => 3,
            Self::ResourceExhausted => 8,
            Self::Unavailable => 14,
        }
    }
}

/// The OTLP error table (§4): every non-ack admission/overload outcome class.
///
/// `partial_success` is used only for permanently rejected malformed items
/// within an otherwise-acked batch; it is never used to smuggle a partial
/// durability outcome — a batch is acked durable in its entirety or it is
/// refused/throttled in its entirety (§4.1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpErrorClass {
    /// A whole-batch permanent rejection of malformed input. Never retryable:
    /// retrying malformed bytes can never succeed.
    MalformedPermanent,
    /// Payload over `max_payload_bytes` (§4.6): `RESOURCE_EXHAUSTED`
    /// **without** `RetryInfo` — instructing a retry would manufacture a loop.
    PayloadTooLarge,
    /// Decoded-but-uncommitted bytes over `admission.max_inflight_bytes`
    /// (§4.6): throttle.
    InflightOverCap,
    /// Overload-ladder rung 2, `M ≥ 95%` (§4.5): `UNAVAILABLE` + `RetryInfo`
    /// with growing delay.
    Throttled,
    /// Overload-ladder rung 3, `M ≥ 100%` (§4.5): new writes refused; still
    /// the retryable OTLP vocabulary on the wire.
    RefusingIngest,
    /// Receipt shortfall after the ring walk-down (§4.3): the batch is staged
    /// and durable, so the signal is retryable by right; the retry replays
    /// success once receipts reach RF (§4.4.1).
    ReceiptShortfall,
    /// A duplicate arriving while the original is still pre-RF (§4.4.1).
    DuplicateInFlight,
    /// A typed storage error inside `StageCommit` (§4.3): the batch is not
    /// acked, so the client must retry — a retry may land on a healthy node.
    /// A non-retryable code here would convert a node-local fault into
    /// silent loss at the edge (§4.1.1), so the wire signal is the
    /// retryable vocabulary.
    StorageFailure,
}

impl OtlpErrorClass {
    /// The gRPC status code this outcome maps to.
    #[must_use]
    pub fn grpc_code(self) -> GrpcCode {
        match self {
            Self::MalformedPermanent => GrpcCode::InvalidArgument,
            Self::PayloadTooLarge => GrpcCode::ResourceExhausted,
            Self::InflightOverCap
            | Self::Throttled
            | Self::RefusingIngest
            | Self::ReceiptShortfall
            | Self::DuplicateInFlight
            | Self::StorageFailure => GrpcCode::Unavailable,
        }
    }

    /// Whether the response carries a `RetryInfo` detail. Exactly the
    /// `UNAVAILABLE` rows: everything retryable says how to retry, and
    /// nothing non-retryable pretends to be (§4.5, §4.6).
    #[must_use]
    pub fn carries_retry_info(self) -> bool {
        self.grpc_code() == GrpcCode::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_info_accompanies_exactly_the_unavailable_rows() {
        // §4.6: over-cap is non-retryable; §4.1.2: malformed is permanent.
        assert!(!OtlpErrorClass::PayloadTooLarge.carries_retry_info());
        assert!(!OtlpErrorClass::MalformedPermanent.carries_retry_info());
        // Unacked outcomes are retryable by right (§4.3, §4.4.1).
        assert!(OtlpErrorClass::StorageFailure.carries_retry_info());
        // §4.5: throttle and refusal are spec-exact retryable UNAVAILABLE.
        assert!(OtlpErrorClass::Throttled.carries_retry_info());
        assert!(OtlpErrorClass::RefusingIngest.carries_retry_info());
        assert!(OtlpErrorClass::ReceiptShortfall.carries_retry_info());
        assert!(OtlpErrorClass::DuplicateInFlight.carries_retry_info());
        assert!(OtlpErrorClass::InflightOverCap.carries_retry_info());
    }
}
