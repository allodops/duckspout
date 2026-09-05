//! The ack/timeout race, as pure logic (§3.7, §8.4).
//!
//! A timeout is a client-side observation (§3.7: `TraceEvent::ClientTimeout`
//! is journaled only by `duckspout-loadgen`), so the loadgen — not the wire —
//! decides which resolution a sent request gets. That decision is factored
//! out of the real I/O race (`tokio::time::timeout` racing the RPC future
//! against a deadline sleep, in [`crate::client`]) into this pure module so
//! it is testable without a network, a clock, or an async runtime: build a
//! [`RaceOutcome`] by hand and read back the verdict.

/// What the real I/O race observed: either the RPC settled (successfully or
/// not) before the deadline, or the deadline fired first. Generic over the
/// RPC's actual success/error types because the resolution below never
/// inspects their content — only which of the two branches happened.
#[derive(Debug, Clone, Copy)]
pub enum RaceOutcome<T, E> {
    /// The RPC settled before the deadline fired.
    Settled(Result<T, E>),
    /// The deadline fired before the RPC settled.
    DeadlineFirst,
}

/// The exactly-one resolution of one sent request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestResolution {
    /// The ack arrived before the deadline: journal `ClientAck`.
    Acked,
    /// The deadline fired before the RPC settled: journal `ClientTimeout` —
    /// the one event only `duckspout-loadgen` may journal (§3.7, §8.4).
    TimedOut,
    /// The RPC settled before the deadline, but with an explicit failure —
    /// not a timeout: the server (or the transport) answered promptly, it
    /// just did not ack. §3.3 has no client-journaled action for this
    /// (compare `duckspout-accept`'s own server-side convention that not
    /// every failure is a §3.3 action — `StageError::MalformedRecords` /
    /// `Backend` journal nothing either): the caller journals nothing for
    /// it, though it still counts against the run's observed failure rate.
    Failed,
}

/// Resolves the ack/deadline race into exactly one [`RequestResolution`].
#[must_use]
pub fn resolve<T, E>(raced: &RaceOutcome<T, E>) -> RequestResolution {
    match raced {
        RaceOutcome::Settled(Ok(_)) => RequestResolution::Acked,
        RaceOutcome::Settled(Err(_)) => RequestResolution::Failed,
        RaceOutcome::DeadlineFirst => RequestResolution::TimedOut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_settled_success_is_acked() {
        let raced: RaceOutcome<(), ()> = RaceOutcome::Settled(Ok(()));
        assert_eq!(resolve(&raced), RequestResolution::Acked);
    }

    #[test]
    fn a_settled_failure_is_failed_not_timed_out() {
        // Would catch collapsing "explicit rejection" and "deadline expired"
        // into the same journaled event — the loadgen would then invent a
        // ClientTimeout for a request that was, in fact, answered.
        let raced: RaceOutcome<(), &str> = RaceOutcome::Settled(Err("rejected"));
        assert_eq!(resolve(&raced), RequestResolution::Failed);
    }

    #[test]
    fn deadline_first_times_out() {
        let raced: RaceOutcome<(), ()> = RaceOutcome::DeadlineFirst;
        assert_eq!(resolve(&raced), RequestResolution::TimedOut);
    }
}
