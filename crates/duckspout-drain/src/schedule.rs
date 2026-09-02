//! Drain-window scheduling (§6.3): which closed micro-windows are
//! drain-eligible right now.
//!
//! The rule is the lateness hold: a window remains eligible to absorb rows
//! whose event time falls inside it for `drain.allowed_lateness` past
//! window close, so ordinary network-delayed data drains into its home
//! window. A window is therefore eligible only once that hold has elapsed.
//! Time comes exclusively from the [`duckspout_types::Clock`] port (D-2):
//! the hold is a freshness decision, never a correctness one — sealing
//! later than the hold is always safe, sealing earlier turns holdable rows
//! into arrival-placed stragglers (§6.3's stated cost).

use duckspout_types::DrainableWindow;

/// The drain's scheduling knobs, mirroring the `drain.*` config settings
/// (`floors/config-surface.toml`; the daemon maps its config surface onto
/// this).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainConfig {
    /// `drain.allowed_lateness` in milliseconds — the §6.3 hold. Default
    /// 15 minutes.
    pub allowed_lateness_ms: i64,
}

impl DrainConfig {
    /// The `drain.allowed_lateness` default: 15 minutes (§6.3).
    pub const DEFAULT_ALLOWED_LATENESS_MS: i64 = 15 * 60 * 1_000;
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            allowed_lateness_ms: Self::DEFAULT_ALLOWED_LATENESS_MS,
        }
    }
}

/// Whether the §6.3 lateness hold on a window closed at `closed_at_ms` has
/// elapsed at `now_ms` (both Unix milliseconds). Boundary inclusive: the
/// hold is "for that long past window close", so the window becomes
/// eligible exactly at `closed_at + allowed_lateness`.
#[must_use]
pub fn hold_elapsed(now_ms: i64, allowed_lateness_ms: i64, closed_at_ms: i64) -> bool {
    now_ms >= closed_at_ms.saturating_add(allowed_lateness_ms)
}

/// Filters the offered closed windows down to the drain-eligible ones:
/// closed (everything offered is, by the `SealSurface` contract) **and**
/// past the lateness hold. Order is preserved.
#[must_use]
pub fn eligible(
    now_ms: i64,
    config: DrainConfig,
    windows: Vec<DrainableWindow>,
) -> Vec<DrainableWindow> {
    windows
        .into_iter()
        .filter(|w| hold_elapsed(now_ms, config.allowed_lateness_ms, w.closed_at_ms))
        .collect()
}

#[cfg(test)]
mod tests {
    use duckspout_types::{DatasetId, PartitionId, WindowId};

    use super::*;

    fn window(id: u64, closed_at_ms: i64) -> DrainableWindow {
        DrainableWindow {
            dataset: DatasetId::new("ds"),
            partition: PartitionId::new("p"),
            window: WindowId(id),
            closed_at_ms,
        }
    }

    #[test]
    fn hold_boundary_is_inclusive() {
        assert!(!hold_elapsed(1_899, 900, 1_000));
        assert!(hold_elapsed(1_900, 900, 1_000));
    }

    #[test]
    fn eligible_filters_only_held_windows() {
        let config = DrainConfig {
            allowed_lateness_ms: 900,
        };
        let windows = vec![window(0, 100), window(1, 200), window(2, 150)];
        let picked = eligible(1_050, config, windows);
        assert_eq!(
            picked.iter().map(|w| w.window.0).collect::<Vec<_>>(),
            vec![0, 2],
            "only windows whose hold elapsed are eligible, order preserved"
        );
    }

    #[test]
    fn saturating_hold_never_wraps() {
        assert!(!hold_elapsed(i64::MAX - 1, i64::MAX, 1));
    }
}
