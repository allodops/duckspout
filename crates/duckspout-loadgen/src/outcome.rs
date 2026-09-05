//! The ack/timeout race, as pure logic (§3.7, §8.4).
//!
//! # The `ClientTimeout` boundary (ACPR finding HIGH-1)
//!
//! `specs/DuckSpoutCore.tla`'s `ClientTimeout(q)` action is enabled only
//! when `resolved[q] = "pending"` **and** no live node still holds `q` in
//! its `inflight` set. `inflight` is cleared by exactly four actions:
//! `DedupCheck`, `StageCommit` (both leave `resolved` untouched — the
//! request may still be waiting on RF receipts), and `CrashNode` /
//! `CrashWipe` (which also flip `alive[n]` to `FALSE`). A real client
//! cannot observe the first two: whether the accepting node has moved a
//! request from "being decided" to "staged, awaiting quorum" is invisible
//! over the wire — both states look identical to the client (no response
//! yet, connection healthy). The only state change a real client *can*
//! positively observe is the fourth: the node's connection dies, which
//! tonic surfaces as the RPC settling (often quickly) with a transport-level
//! `Status` rather than the request hanging forever.
//!
//! So a bare local deadline expiring against a **still-open** connection
//! gives no information about whether `inflight` has actually been vacated
//! — the model may or may not permit `ClientTimeout` in that exact state,
//! and the client cannot tell which. Journaling `ClientTimeout` there would
//! sometimes assert a transition the model forbids (this is exactly what
//! the pre-fix flagship smoke test did: a server that hangs in
//! `stage_commit` forever never leaves `inflight`, so `ClientTimeout` is
//! disabled in that state by the model's own definition, yet the old test
//! asserted it was the correct journal entry).
//!
//! The fix: `ClientTimeout` is now journaled only when the RPC **settles**
//! with a transport-level failure (connection refused/reset, the channel
//! going away mid-call) — the one case a real client can positively
//! confirm, and the one case that unconditionally satisfies the model's
//! precondition (the node is no longer alive, so the "no live node holds
//! it" existential is vacuously true regardless of which of `StageCommit`'s
//! or `CrashNode`'s clearing of `inflight` actually happened first). A bare
//! local deadline against a still-open connection is instead
//! [`RequestResolution::Ambiguous`] — an honest "don't know," matching
//! §8.4's own "ambiguous-outcome fraction" vacuity-teeth language in
//! `docs/verification.md`, and never confused with a positively-confirmed
//! `ClientAck` or `ClientTimeout`.
//!
//! An explicit, prompt application-level rejection (`Throttle`/`Refuse`/
//! storage-failure — all of which `duckspout-accept` attaches
//! `google.rpc.RetryInfo` to, per its OTLP error table) is
//! [`RequestResolution::Rejected`], distinct from both: the accept side
//! already journals its own event for it (§3.3 has no client-journaled
//! action for a prompt rejection), and — unlike `Ambiguous` — there is
//! nothing left uncertain about it (ACPR finding MEDIUM-5).

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use tonic_types::StatusExt as _;

/// What the real I/O race observed: either the RPC settled (successfully or
/// not) before the deadline, or the deadline fired first.
#[derive(Debug, Clone)]
pub enum RaceOutcome {
    /// The RPC settled before the deadline fired.
    Settled(Result<ExportLogsServiceResponse, tonic::Status>),
    /// The deadline fired before the RPC settled — the connection's true
    /// state (still open vs. already dead) is unknown (module docs).
    DeadlineFirst,
}

/// The resolution of one sent request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestResolution {
    /// The ack arrived before the deadline, with no rejected records:
    /// journal `ClientAck`.
    Acked,
    /// A positively-confirmed transport-level failure (connection
    /// refused/reset — the acceptor is gone): journal `ClientTimeout` — the
    /// one event only `duckspout-loadgen` may journal (§3.7, §8.4). Named
    /// for the model's action, not for "our local timer fired" (module
    /// docs' HIGH-1 discussion).
    TimedOut,
    /// The RPC settled with an explicit, prompt application-level
    /// rejection (or a partial-success response reporting rejected
    /// records) — not ambiguous, just not an ack. §3.3 has no
    /// client-journaled action for this (the accept side already journals
    /// its own `Throttle`/`Refuse`): the caller journals nothing for it,
    /// though it still counts against the run's observed outcomes.
    Rejected,
    /// The local deadline fired while the RPC was still pending and the
    /// connection gave no sign of being dead: genuinely unresolved. Neither
    /// `ClientAck` nor `ClientTimeout` would be honest here (module docs) —
    /// nothing is journaled to the frozen §3.3 vocabulary, but the run
    /// summary counts it so a vanished-mid-run loadgen is visible rather
    /// than looking like a clean completion (ACPR finding MEDIUM-HIGH-4).
    Ambiguous,
}

/// Resolves the ack/deadline race into exactly one [`RequestResolution`].
#[must_use]
pub fn resolve(raced: &RaceOutcome) -> RequestResolution {
    match raced {
        RaceOutcome::Settled(Ok(response)) => {
            let rejected = response
                .partial_success
                .as_ref()
                .is_some_and(|p| p.rejected_log_records > 0);
            if rejected {
                // §4.1.2's sanctioned partial-success shape carries a count,
                // never per-record identity, so there is no honest range
                // left to ack (ACPR finding LOW-MEDIUM-6): treat the whole
                // batch as Rejected rather than asserting full coverage.
                RequestResolution::Rejected
            } else {
                RequestResolution::Acked
            }
        }
        RaceOutcome::Settled(Err(status)) => classify_error(status),
        RaceOutcome::DeadlineFirst => RequestResolution::Ambiguous,
    }
}

/// Distinguishes an explicit, prompt application-level rejection from a
/// transport-level failure (module docs).
///
/// `duckspout-accept`'s OTLP error table (`duckspout_types::OtlpErrorClass`)
/// attaches `google.rpc.RetryInfo` to every retryable row
/// (`Throttled`/`RefusingIngest`/storage-failure/...) and uses
/// `InvalidArgument`/`ResourceExhausted` — codes tonic's transport layer
/// never manufactures on its own — for the two permanent-reject rows.
/// Anything else (typically `Unavailable`/`Cancelled`/`Unknown` with no
/// `RetryInfo`) is tonic's own signal that the connection died, never a
/// deliberate answer from `duckspout-accept`.
fn classify_error(status: &tonic::Status) -> RequestResolution {
    let explicit_rejection = status.get_details_retry_info().is_some()
        || matches!(
            status.code(),
            tonic::Code::InvalidArgument | tonic::Code::ResourceExhausted
        );
    if explicit_rejection {
        RequestResolution::Rejected
    } else {
        RequestResolution::TimedOut
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess;
    use tonic_types::{ErrorDetails, StatusExt};

    use super::*;

    fn ok(partial_success: Option<ExportLogsPartialSuccess>) -> RaceOutcome {
        RaceOutcome::Settled(Ok(ExportLogsServiceResponse { partial_success }))
    }

    fn explicit_rejection_status() -> tonic::Status {
        // Mirrors `OtlpGrpcAdapter::to_tonic_status` for a retryable row:
        // UNAVAILABLE + RetryInfo (Throttle/Refuse/storage-failure all look
        // like this on the wire).
        let mut details = ErrorDetails::new();
        details.set_retry_info(Some(Duration::from_millis(50)));
        <tonic::Status as StatusExt>::with_error_details(
            tonic::Code::Unavailable,
            "duckspout: throttled",
            details,
        )
    }

    fn transport_failure_status() -> tonic::Status {
        // What tonic manufactures itself on a dead/refused connection: same
        // family of code, no RetryInfo attached by anyone.
        tonic::Status::new(tonic::Code::Unavailable, "error trying to connect: refused")
    }

    #[test]
    fn a_clean_ack_is_acked() {
        assert_eq!(resolve(&ok(None)), RequestResolution::Acked);
    }

    #[test]
    fn a_partial_success_with_rejected_records_is_rejected_not_acked() {
        // Would catch asserting `ClientAck` (and the full record_count) over
        // a batch the server itself said it partially rejected.
        let partial = ExportLogsPartialSuccess {
            rejected_log_records: 3,
            error_message: "malformed".to_owned(),
        };
        assert_eq!(resolve(&ok(Some(partial))), RequestResolution::Rejected);
    }

    #[test]
    fn a_partial_success_with_zero_rejected_is_still_acked() {
        // The OTLP-sanctioned all-accepted warning shape (§4.1.2's
        // `rejected_log_records = 0`, e.g. `duckspout-accept`'s strindex
        // warning) must not be treated as a rejection.
        let partial = ExportLogsPartialSuccess {
            rejected_log_records: 0,
            error_message: "informational only".to_owned(),
        };
        assert_eq!(resolve(&ok(Some(partial))), RequestResolution::Acked);
    }

    #[test]
    fn an_explicit_application_rejection_is_rejected_not_timed_out() {
        // Would catch conflating "the server said no" with "the connection
        // died" — the loadgen must not invent a `ClientTimeout` (evidence
        // of a vanished acceptor) for a request the server promptly and
        // explicitly answered.
        let raced = RaceOutcome::Settled(Err(explicit_rejection_status()));
        assert_eq!(resolve(&raced), RequestResolution::Rejected);
    }

    #[test]
    fn a_transport_failure_is_timed_out() {
        // The one case a real client can positively confirm matches the
        // model's `ClientTimeout` precondition (module docs): the
        // connection itself is gone, which vacuously satisfies "no live
        // node holds this request in flight."
        let raced = RaceOutcome::Settled(Err(transport_failure_status()));
        assert_eq!(resolve(&raced), RequestResolution::TimedOut);
    }

    #[test]
    fn a_bare_local_deadline_is_ambiguous_not_timed_out() {
        // ACPR finding HIGH-1's core fix: a hung-but-alive connection must
        // NOT be journaled as `ClientTimeout` — the model's precondition
        // (no live node holds the request) cannot be confirmed from a bare
        // local deadline against a still-open connection.
        assert_eq!(
            resolve(&RaceOutcome::DeadlineFirst),
            RequestResolution::Ambiguous
        );
    }
}
