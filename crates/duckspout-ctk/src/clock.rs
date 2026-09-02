//! The virtual clock: the [`Clock`] port's deterministic double.

use duckspout_types::Clock;

use crate::sync::atomic::{AtomicU64, Ordering};

/// A clock that advances only when told to. Monotonic time starts at 0;
/// wall time is derived from it (the CTK's schedules need one totally
/// ordered notion of time, not two).
#[derive(Debug)]
pub struct VirtualClock {
    nanos: AtomicU64,
}

impl Default for VirtualClock {
    // Spelled out (not derived) so the loom builds need no `Default` on
    // loom's atomics — see `crate::sync`.
    fn default() -> Self {
        Self {
            nanos: AtomicU64::new(0),
        }
    }
}

impl VirtualClock {
    /// A clock at time zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Advances virtual time by `delta` nanoseconds.
    pub fn advance_nanos(&self, delta: u64) {
        self.nanos.fetch_add(delta, Ordering::SeqCst);
    }

    /// Advances virtual time **to** `deadline` if it is in the future; time
    /// never moves backward.
    pub fn advance_to_nanos(&self, deadline: u64) {
        self.nanos.fetch_max(deadline, Ordering::SeqCst);
    }
}

impl Clock for VirtualClock {
    fn monotonic_nanos(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }

    fn wall_unix_ms(&self) -> i64 {
        i64::try_from(self.nanos.load(Ordering::SeqCst) / 1_000_000).unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_zero_and_advances_deterministically() {
        let clock = VirtualClock::new();
        assert_eq!(clock.monotonic_nanos(), 0);
        assert_eq!(clock.wall_unix_ms(), 0);
        clock.advance_nanos(1_500_000);
        assert_eq!(clock.monotonic_nanos(), 1_500_000);
        assert_eq!(clock.wall_unix_ms(), 1);
    }

    #[test]
    fn never_moves_backward() {
        let clock = VirtualClock::new();
        clock.advance_to_nanos(10);
        clock.advance_to_nanos(5);
        assert_eq!(clock.monotonic_nanos(), 10);
    }
}
