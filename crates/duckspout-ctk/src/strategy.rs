//! The schedule-exploration seam (ADR-0011).
//!
//! The CTK explores interleavings by choosing which pending task to poll
//! next; **how** that choice is made is a strategy, kept behind
//! [`ScheduleStrategy`] so exploration modes can be added without touching
//! the executor. v0.1 ships one strategy, [`SeededRandom`]; the distributed
//! tier adds a PCT-style prioritized strategy alongside it (#124), and the
//! judge's seeded-violation replays must convict under both.

/// How the executor picks the next task to poll from the current round.
///
/// Contract:
///
/// - `pending` is the number of not-yet-polled tasks in the round, always
///   ≥ 1; the returned index must be `< pending`.
/// - The choice sequence must be a pure function of the strategy's
///   construction inputs and its call history — no ambient randomness or
///   time (R-determinism). Replaying the same construction against the same
///   call sequence must reproduce the same schedule bit-for-bit; that is
///   the reproduction handle every CTK failure ships with (§8.3).
///
/// The signature is deliberately minimal for the uniform-random v0.1
/// strategy; when the prioritized strategy lands (#124) it widens — PCT
/// needs task identity across rounds — rather than carrying speculative
/// parameters nothing reads today.
pub trait ScheduleStrategy: Send {
    /// Picks which of the `pending` tasks to poll next.
    fn next_index(&mut self, pending: usize) -> usize;
}

/// The v0.1 strategy: uniform choice from a `SplitMix64` stream, so the
/// whole schedule is a pure function of the seed.
#[derive(Debug, Clone)]
pub struct SeededRandom {
    state: u64,
}

impl SeededRandom {
    /// A strategy driven by `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_random(&mut self) -> u64 {
        // SplitMix64 (public domain), inlined: `thread_rng` is banned (D-2)
        // and a seeded stream is the whole point.
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

impl ScheduleStrategy for SeededRandom {
    fn next_index(&mut self, pending: usize) -> usize {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "index is reduced modulo the (small) round length"
        )]
        let index = (self.next_random() % pending as u64) as usize;
        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_choices() {
        let picks = |seed| {
            let mut strategy = SeededRandom::new(seed);
            (1..=32)
                .map(|pending| strategy.next_index(pending))
                .collect::<Vec<_>>()
        };
        assert_eq!(picks(42), picks(42));
    }

    #[test]
    fn index_is_always_in_range() {
        let mut strategy = SeededRandom::new(7);
        for pending in 1..=64 {
            assert!(strategy.next_index(pending) < pending);
        }
    }

    proptest::proptest! {
        /// The [`ScheduleStrategy`] contract as a law, over any seed and any
        /// pending-size call history: the index stays `< pending`, and the
        /// same construction replayed against the same call sequence makes
        /// the same choices (R-determinism — the reproduction handle).
        /// Would catch a modulo/cast bug at extreme pending sizes and any
        /// hidden state or ambient randomness in the stream.
        #[test]
        fn contract_holds_for_any_seed_and_call_history(
            seed in proptest::prelude::any::<u64>(),
            pendings in proptest::collection::vec(1usize..=1 << 20, 1..64),
        ) {
            let mut first = SeededRandom::new(seed);
            let mut second = SeededRandom::new(seed);
            for pending in pendings {
                let index = first.next_index(pending);
                proptest::prop_assert!(index < pending);
                proptest::prop_assert_eq!(index, second.next_index(pending));
            }
        }
    }
}
