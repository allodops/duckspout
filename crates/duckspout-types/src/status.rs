//! The closed node-status vocabulary (§4.5, §9.3.2).
//!
//! One type, three transports: the same [`NodeStatus`] value is reported
//! identically on the health endpoint, the metrics, and the registry — no
//! channel ever knows more than another (§9.5).

use serde::{Deserialize, Serialize};

/// Overload ladder rung 1: disclose at 80% of `hot.max_bytes` of **staged**
/// bytes (§4.5 — also the only capacity alert, §9.2). A constant, not a
/// knob (R-12): every threshold is a fixed function of `hot.max_bytes`,
/// the *only* configured byte number.
pub const LADDER_DISCLOSE_PERCENT: u64 = 80;

/// Overload ladder rung 2: throttle at 95% (§4.5).
pub const LADDER_THROTTLE_PERCENT: u64 = 95;

/// Overload ladder rung 3: refuse at 100% — `hot.max_bytes` itself (§4.5,
/// the top rung; nothing above it, ever).
pub const LADDER_REFUSE_PERCENT: u64 = 100;

/// The throttle `RetryInfo` delay at the rung-2 boundary (M = 95%) — the
/// floor of §4.5's growing delay. A constant, not a knob (R-12).
pub const THROTTLE_RETRY_MIN_MS: u64 = 1_000;

/// The throttle `RetryInfo` delay at and beyond M = 100% — the ceiling of
/// §4.5's growing delay, also used for rung-3 refusals (still the
/// retryable OTLP vocabulary on the wire). A constant, not a knob (R-12).
pub const THROTTLE_RETRY_MAX_MS: u64 = 30_000;

/// The closed overload-ladder status enum (§4.5):
/// `normal | staging_pressure | drain_stalled | throttling | refusing_ingest`.
///
/// The rung is a pure function of `M = staged_bytes / hot.max_bytes` — no
/// hysteresis, no rung memory (`LadderMonotone`, §3). A closed enum is what
/// §3's properties and §8's chaos judge can assert over; free-text status is
/// unverifiable status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverloadStatus {
    /// M below the disclosure threshold; nothing to disclose.
    #[default]
    Normal,
    /// M ≥ 80% driven by sheer ingest rate, drains healthy (rung 1).
    StagingPressure,
    /// M ≥ 80% driven by a stalled drain — including catalog outage (rung 1).
    DrainStalled,
    /// M ≥ 95%: no new accepts; UNAVAILABLE + `RetryInfo` with growing delay
    /// (rung 2).
    Throttling,
    /// M ≥ 100%: refuse new writes and new-range replication (rung 3 — the
    /// top rung; nothing above it, ever).
    RefusingIngest,
}

impl OverloadStatus {
    /// The rung as a pure function of the measure (§4.5, `LadderMonotone`):
    /// `M = staged_bytes / max_bytes`, thresholds
    /// [`LADDER_DISCLOSE_PERCENT`] / [`LADDER_THROTTLE_PERCENT`] /
    /// [`LADDER_REFUSE_PERCENT`]. Integer arithmetic, so the boundaries are
    /// exact: `M ≥ p%` iff `staged_bytes · 100 ≥ max_bytes · p`.
    ///
    /// `drain_stalled` selects rung 1's label (§4.5: `drain_stalled` when a
    /// stalled drain drove M there, else `staging_pressure`); it never
    /// changes the rung. `max_bytes = 0` means no staging capacity exists at
    /// all and is the top rung (ambiguity fails closed, §11).
    #[must_use]
    pub fn from_measure(staged_bytes: u64, max_bytes: u64, drain_stalled: bool) -> Self {
        if max_bytes == 0 {
            return Self::RefusingIngest;
        }
        let at = |percent: u64| {
            u128::from(staged_bytes) * 100 >= u128::from(max_bytes) * u128::from(percent)
        };
        if at(LADDER_REFUSE_PERCENT) {
            Self::RefusingIngest
        } else if at(LADDER_THROTTLE_PERCENT) {
            Self::Throttling
        } else if at(LADDER_DISCLOSE_PERCENT) {
            if drain_stalled {
                Self::DrainStalled
            } else {
                Self::StagingPressure
            }
        } else {
            Self::Normal
        }
    }
}

/// §4.5's growing throttle delay as a pure function of the measure — no
/// per-client state, consistent with the stateless rung. Linear from
/// [`THROTTLE_RETRY_MIN_MS`] at the rung-2 boundary (M = 95%) to
/// [`THROTTLE_RETRY_MAX_MS`] at M ≥ 100%; below rung 2 (where nothing is
/// throttled) it is the floor.
#[must_use]
pub fn throttle_retry_delay_ms(staged_bytes: u64, max_bytes: u64) -> u64 {
    if max_bytes == 0 {
        return THROTTLE_RETRY_MAX_MS;
    }
    // Permille of max, saturating — enough resolution for the 95..100 band.
    let permille =
        u64::try_from(u128::from(staged_bytes) * 1000 / u128::from(max_bytes)).unwrap_or(u64::MAX);
    let floor = LADDER_THROTTLE_PERCENT * 10;
    let ceiling = LADDER_REFUSE_PERCENT * 10;
    if permille >= ceiling {
        return THROTTLE_RETRY_MAX_MS;
    }
    let position = permille.saturating_sub(floor); // 0..(ceiling-floor)
    let span = ceiling - floor;
    THROTTLE_RETRY_MIN_MS + (THROTTLE_RETRY_MAX_MS - THROTTLE_RETRY_MIN_MS) * position / span
}

/// The fill-scaled per-query byte budget's maximum multiplier (§7.8): at a
/// completely full hot store the budget is twice its steady-state value. A
/// constant, not a knob (R-12) — the base is the one configured number
/// (`query.max_hot_bytes_per_query`).
pub const QUERY_BUDGET_MAX_SCALE: u64 = 2;

/// §7.8's fill-scaled per-query byte budget: `base_bytes` steady-state,
/// scaling **up** linearly with the hot fill ratio once fill crosses the
/// ladder's disclosure threshold ([`LADDER_DISCLOSE_PERCENT`]) — because
/// when drains stall, hot is the *only* coverage and steady-state limits
/// would kill querying exactly when it matters — reaching
/// [`QUERY_BUDGET_MAX_SCALE`] × at 100% fill and clamping there. Scaling
/// starts precisely where the pressure becomes a disclosed status: below
/// it, steady state; a pure function, like the rung itself.
///
/// `max_bytes = 0` (no capacity configured — the fail-closed top rung,
/// [`OverloadStatus::from_measure`]) yields the unscaled base: with no
/// meaningful fill ratio the budget must not inflate.
#[must_use]
pub fn fill_scaled_budget(base_bytes: u64, staged_bytes: u64, max_bytes: u64) -> u64 {
    if max_bytes == 0 {
        return base_bytes;
    }
    let permille =
        u64::try_from(u128::from(staged_bytes) * 1000 / u128::from(max_bytes)).unwrap_or(u64::MAX);
    let onset = LADDER_DISCLOSE_PERCENT * 10;
    if permille <= onset {
        return base_bytes;
    }
    let span = LADDER_REFUSE_PERCENT * 10 - onset;
    let position = (permille - onset).min(span);
    let extra = u64::try_from(
        u128::from(base_bytes) * u128::from(QUERY_BUDGET_MAX_SCALE - 1) * u128::from(position)
            / u128::from(span),
    )
    .unwrap_or(u64::MAX);
    base_bytes.saturating_add(extra)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rung transitions at exactly the §4.6 thresholds — one byte below
    /// each boundary stays on the lower rung, the boundary itself is the
    /// higher rung (`M ≥ p%`, inclusive).
    #[test]
    fn rungs_transition_at_exact_thresholds() {
        let max = 100_000;
        let cases = [
            (0, OverloadStatus::Normal),
            (79_999, OverloadStatus::Normal),
            (80_000, OverloadStatus::StagingPressure),
            (94_999, OverloadStatus::StagingPressure),
            (95_000, OverloadStatus::Throttling),
            (99_999, OverloadStatus::Throttling),
            (100_000, OverloadStatus::RefusingIngest),
            (u64::MAX, OverloadStatus::RefusingIngest),
        ];
        for (staged, expected) in cases {
            assert_eq!(
                OverloadStatus::from_measure(staged, max, false),
                expected,
                "staged={staged}"
            );
        }
    }

    /// `drain_stalled` selects rung 1's label and nothing else (§4.5).
    #[test]
    fn drain_stalled_labels_rung_one_only() {
        assert_eq!(
            OverloadStatus::from_measure(80_000, 100_000, true),
            OverloadStatus::DrainStalled
        );
        assert_eq!(
            OverloadStatus::from_measure(0, 100_000, true),
            OverloadStatus::Normal
        );
        assert_eq!(
            OverloadStatus::from_measure(95_000, 100_000, true),
            OverloadStatus::Throttling
        );
        assert_eq!(
            OverloadStatus::from_measure(100_000, 100_000, true),
            OverloadStatus::RefusingIngest
        );
    }

    /// No capacity fails closed to the top rung (§11: ambiguity fails
    /// closed) — a zero denominator can never read as headroom.
    #[test]
    fn zero_capacity_is_the_top_rung() {
        assert_eq!(
            OverloadStatus::from_measure(0, 0, false),
            OverloadStatus::RefusingIngest
        );
    }

    /// The numeric rung a status sits on — the yardstick the ladder laws
    /// below compare with. The two rung-1 labels are the same rung (§4.5).
    fn rung_level(status: OverloadStatus) -> u8 {
        match status {
            OverloadStatus::Normal => 0,
            OverloadStatus::StagingPressure | OverloadStatus::DrainStalled => 1,
            OverloadStatus::Throttling => 2,
            OverloadStatus::RefusingIngest => 3,
        }
    }

    proptest::proptest! {
        /// `LadderMonotone`'s fill half as a law (§4.5, §8.5; issue #40):
        /// for ANY capacity and ANY pair of fills, more staged bytes never
        /// yields a lower rung — over the full u64 domain, where the
        /// boundary-sweep unit test above cannot reach. Would catch an
        /// inverted threshold comparison or an overflow in the widened
        /// `M ≥ p%` arithmetic (exactly where `staged · 100` exceeds u64).
        #[test]
        fn rung_is_monotone_in_fill(
            fill_a in proptest::prelude::any::<u64>(),
            fill_b in proptest::prelude::any::<u64>(),
            max in proptest::prelude::any::<u64>(),
            stalled in proptest::prelude::any::<bool>(),
        ) {
            let (low, high) = (fill_a.min(fill_b), fill_a.max(fill_b));
            proptest::prop_assert!(
                rung_level(OverloadStatus::from_measure(low, max, stalled))
                    <= rung_level(OverloadStatus::from_measure(high, max, stalled))
            );
        }

        /// `drain_stalled` selects rung 1's LABEL only — for ANY measure it
        /// never moves the rung itself (§4.5). Would catch the flag leaking
        /// into the throttle/refuse decisions.
        #[test]
        fn stalled_flag_never_changes_the_rung(
            staged in proptest::prelude::any::<u64>(),
            max in proptest::prelude::any::<u64>(),
        ) {
            proptest::prop_assert_eq!(
                rung_level(OverloadStatus::from_measure(staged, max, true)),
                rung_level(OverloadStatus::from_measure(staged, max, false))
            );
        }

        /// §4.5's growing delay as a law: for ANY measure the delay stays in
        /// `[THROTTLE_RETRY_MIN_MS, THROTTLE_RETRY_MAX_MS]` and never
        /// shrinks as fill grows — a delay that dips under pressure would
        /// invite retries exactly when the node most needs them not to come.
        #[test]
        fn throttle_delay_is_bounded_and_monotone(
            fill_a in proptest::prelude::any::<u64>(),
            fill_b in proptest::prelude::any::<u64>(),
            max in proptest::prelude::any::<u64>(),
        ) {
            let (low, high) = (fill_a.min(fill_b), fill_a.max(fill_b));
            let (d_low, d_high) =
                (throttle_retry_delay_ms(low, max), throttle_retry_delay_ms(high, max));
            proptest::prop_assert!((THROTTLE_RETRY_MIN_MS..=THROTTLE_RETRY_MAX_MS).contains(&d_low));
            proptest::prop_assert!((THROTTLE_RETRY_MIN_MS..=THROTTLE_RETRY_MAX_MS).contains(&d_high));
            proptest::prop_assert!(d_low <= d_high);
        }
    }

    /// §7.8's fill scaling: steady-state base at and below the disclosure
    /// threshold, linear growth above it, exactly 2× at full, clamped
    /// beyond — and no inflation when no capacity is configured.
    #[test]
    fn query_budget_scales_with_fill_from_disclose_to_full() {
        let base = 2 * 1024 * 1024 * 1024_u64; // the 2 GiB default
        let max = 100_000;
        assert_eq!(fill_scaled_budget(base, 0, max), base);
        assert_eq!(
            fill_scaled_budget(base, 80_000, max),
            base,
            "onset is exclusive"
        );
        let mid = fill_scaled_budget(base, 90_000, max);
        assert!(mid > base && mid < QUERY_BUDGET_MAX_SCALE * base);
        assert_eq!(
            fill_scaled_budget(base, 100_000, max),
            QUERY_BUDGET_MAX_SCALE * base
        );
        assert_eq!(
            fill_scaled_budget(base, 250_000, max),
            QUERY_BUDGET_MAX_SCALE * base,
            "clamped past full"
        );
        assert_eq!(
            fill_scaled_budget(base, 123, 0),
            base,
            "no capacity, no inflation"
        );
        let mut last = 0;
        for staged in (80_000..=100_000).step_by(500) {
            let budget = fill_scaled_budget(base, staged, max);
            assert!(budget >= last, "not monotone at staged={staged}");
            last = budget;
        }
    }

    /// The throttle delay grows monotonically across the 95..100 band and
    /// clamps to its documented floor and ceiling.
    #[test]
    fn throttle_delay_grows_from_floor_to_ceiling() {
        let max = 100_000;
        assert_eq!(throttle_retry_delay_ms(95_000, max), THROTTLE_RETRY_MIN_MS);
        assert_eq!(throttle_retry_delay_ms(100_000, max), THROTTLE_RETRY_MAX_MS);
        assert_eq!(
            throttle_retry_delay_ms(u64::MAX, max),
            THROTTLE_RETRY_MAX_MS
        );
        let mid = throttle_retry_delay_ms(97_500, max);
        assert!(mid > THROTTLE_RETRY_MIN_MS && mid < THROTTLE_RETRY_MAX_MS);
        let mut last = 0;
        for staged in (95_000..=100_000).step_by(100) {
            let delay = throttle_retry_delay_ms(staged, max);
            assert!(delay >= last, "delay not monotone at staged={staged}");
            last = delay;
        }
    }
}

/// The complete disclosed node status: the overload rung plus the orthogonal
/// `replication_degraded` flag (§9.3.2).
///
/// `replication_degraded` is deliberately **not** a sixth enum variant: it is
/// orthogonal to the ladder (§4.5 — a node can be `throttling` *and*
/// replication-degraded), so the honest single status type is this pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct NodeStatus {
    /// The overload-ladder rung's disclosed status.
    pub overload: OverloadStatus,
    /// True while the node holds ranges below the replication floor —
    /// availability preferred over placement, disclosed (§5).
    pub replication_degraded: bool,
}
