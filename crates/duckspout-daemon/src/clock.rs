//! The daemon's production [`Clock`]: real time behind the D-2 port. Only
//! composition code touches `std::time` — protocol crates receive this
//! through the port and stay deterministic under the CTK doubles.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use duckspout_types::Clock;

/// Real monotonic + wall time (the composition-side Clock).
pub struct StdClock {
    /// Process epoch for the monotonic reading.
    started: Instant,
}

impl StdClock {
    /// A clock whose monotonic epoch is its construction instant.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Default for StdClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for StdClock {
    fn monotonic_nanos(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn wall_unix_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }
}
