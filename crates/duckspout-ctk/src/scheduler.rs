//! The seeded scheduler: the [`Scheduler`] port's deterministic double.
//!
//! A single-threaded executor whose interleavings are decided by a
//! [`ScheduleStrategy`] (ADR-0011's exploration seam): each polling round
//! removes tasks in the order the strategy picks, so with the v0.1
//! [`SeededRandom`] strategy the same seed
//! replays the same schedule bit-for-bit — the reproduction handle every
//! CTK failure ships with (§8.3).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use duckspout_types::{BoxFuture, Clock, Scheduler};

use crate::clock::VirtualClock;
use crate::strategy::{ScheduleStrategy, SeededRandom};

/// A deterministic, virtual-time executor implementing the [`Scheduler`]
/// port.
///
/// Pending tasks are re-polled every round (determinism beats efficiency in
/// a test double); a round counts as progress when a task completed, a task
/// was spawned, or any polled future invoked its waker. When a whole round
/// makes no progress, virtual time advances to the earliest registered
/// sleep deadline; if no deadline exists either, the remaining tasks are
/// deadlocked and [`SeededScheduler::run_until_idle`] returns them as stuck.
pub struct SeededScheduler {
    clock: Arc<VirtualClock>,
    inner: Mutex<Inner>,
}

struct Inner {
    strategy: Box<dyn ScheduleStrategy>,
    tasks: Vec<BoxFuture<'static, ()>>,
    deadlines: BTreeSet<u64>,
}

impl SeededScheduler {
    /// A scheduler driven by `seed` (the v0.1 seeded-random strategy),
    /// advancing `clock`.
    #[must_use]
    pub fn new(seed: u64, clock: Arc<VirtualClock>) -> Self {
        Self::with_strategy(Box::new(SeededRandom::new(seed)), clock)
    }

    /// A scheduler exploring interleavings with `strategy` (ADR-0011's
    /// seam; #124 adds a PCT-style prioritized strategy here).
    #[must_use]
    pub fn with_strategy(strategy: Box<dyn ScheduleStrategy>, clock: Arc<VirtualClock>) -> Self {
        Self {
            clock,
            inner: Mutex::new(Inner {
                strategy,
                tasks: Vec::new(),
                deadlines: BTreeSet::new(),
            }),
        }
    }

    /// The virtual clock this scheduler advances.
    #[must_use]
    pub fn clock(&self) -> &Arc<VirtualClock> {
        &self.clock
    }

    /// Runs until every task completes, or until the remainder is stuck
    /// (pending with no future deadline). Returns the number of stuck tasks
    /// — 0 is the healthy outcome, and callers should assert it.
    pub fn run_until_idle(&self) -> usize {
        loop {
            let mut round = {
                let mut inner = self.inner.lock().expect("scheduler lock");
                std::mem::take(&mut inner.tasks)
            };
            if round.is_empty() {
                return 0;
            }

            let round_waker = Arc::new(RoundWaker {
                woke: AtomicBool::new(false),
            });
            let waker = Waker::from(Arc::clone(&round_waker));
            let mut progressed = false;
            let mut still_pending = Vec::new();
            while !round.is_empty() {
                let index = {
                    let mut inner = self.inner.lock().expect("scheduler lock");
                    inner.strategy.next_index(round.len())
                };
                assert!(index < round.len(), "strategy broke its index contract");
                let mut task = round.swap_remove(index);
                let mut context = Context::from_waker(&waker);
                match task.as_mut().poll(&mut context) {
                    Poll::Ready(()) => progressed = true,
                    Poll::Pending => still_pending.push(task),
                }
            }
            if round_waker.woke.load(Ordering::SeqCst) {
                // A pending future signaled readiness to make progress.
                progressed = true;
            }

            let now = self.clock.monotonic_nanos();
            let next_deadline = {
                let mut inner = self.inner.lock().expect("scheduler lock");
                if !inner.tasks.is_empty() {
                    // Tasks spawned during the round count as progress.
                    progressed = true;
                }
                inner.tasks.append(&mut still_pending);
                // Expired deadlines have done their job.
                inner.deadlines = inner.deadlines.split_off(&(now + 1));
                inner.deadlines.first().copied()
            };

            if progressed {
                continue;
            }
            let Some(deadline) = next_deadline else {
                // Stuck: pending tasks, no timer to advance to.
                return self.inner.lock().expect("scheduler lock").tasks.len();
            };
            self.clock.advance_to_nanos(deadline);
        }
    }
}

impl Scheduler for SeededScheduler {
    fn spawn(&self, task: BoxFuture<'static, ()>) {
        self.inner.lock().expect("scheduler lock").tasks.push(task);
    }

    fn sleep(&self, nanos: u64) -> BoxFuture<'static, ()> {
        let deadline = self.clock.monotonic_nanos().saturating_add(nanos);
        {
            let mut inner = self.inner.lock().expect("scheduler lock");
            inner.deadlines.insert(deadline);
        }
        let clock = Arc::clone(&self.clock);
        Box::pin(SleepFuture { clock, deadline })
    }
}

/// One polling round's wake witness: any waker invocation marks the round
/// as having made progress.
struct RoundWaker {
    woke: AtomicBool,
}

impl Wake for RoundWaker {
    fn wake(self: Arc<Self>) {
        self.woke.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woke.store(true, Ordering::SeqCst);
    }
}

struct SleepFuture {
    clock: Arc<VirtualClock>,
    deadline: u64,
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.clock.monotonic_nanos() >= self.deadline {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A future that yields once before completing, to force interleaving.
    struct YieldOnce {
        yielded: bool,
    }

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                // The futures contract: a Pending that wants re-polling
                // wakes its waker.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// Always polls index 0 — with `swap_remove`, two rounds of this leave
    /// the tasks completing in spawn order (0,7,6,…,1 polled first round,
    /// then 0,1,2,…,7), which no seeded-random schedule is pinned to.
    struct PickFirst;

    impl ScheduleStrategy for PickFirst {
        fn next_index(&mut self, _pending: usize) -> usize {
            0
        }
    }

    fn record_order_with(scheduler: &SeededScheduler) -> Vec<u32> {
        let order = Arc::new(Mutex::new(Vec::new()));
        for label in 0..8_u32 {
            let order = Arc::clone(&order);
            scheduler.spawn(Box::pin(async move {
                YieldOnce { yielded: false }.await;
                order.lock().expect("order lock").push(label);
            }));
        }
        assert_eq!(scheduler.run_until_idle(), 0);
        let order = order.lock().expect("order lock").clone();
        assert_eq!(order.len(), 8, "every task must complete");
        order
    }

    fn record_order(seed: u64) -> Vec<u32> {
        let scheduler = SeededScheduler::new(seed, Arc::new(VirtualClock::new()));
        record_order_with(&scheduler)
    }

    #[test]
    fn same_seed_same_schedule() {
        let first = record_order(42);
        let second = record_order(42);
        assert_eq!(first, second);
    }

    proptest::proptest! {
        /// The §8.3 reproduction handle as a law: for ANY seed — not a
        /// sampled example — the schedule replays bit-for-bit, and every
        /// task completes. Would catch ambient nondeterminism leaking into
        /// the executor (hash-map iteration order, real time, thread
        /// scheduling) that a single pinned seed can easily miss.
        #[test]
        fn any_seed_replays_bit_for_bit(seed in proptest::prelude::any::<u64>()) {
            proptest::prop_assert_eq!(record_order(seed), record_order(seed));
        }
    }

    #[test]
    fn custom_strategy_steers_the_schedule() {
        let scheduler =
            SeededScheduler::with_strategy(Box::new(PickFirst), Arc::new(VirtualClock::new()));
        assert_eq!(
            record_order_with(&scheduler),
            (0..8).collect::<Vec<u32>>(),
            "the executor must poll exactly where the strategy points"
        );
    }

    #[test]
    fn sleep_completes_by_advancing_virtual_time() {
        let clock = Arc::new(VirtualClock::new());
        let scheduler = Arc::new(SeededScheduler::new(7, Arc::clone(&clock)));
        let done = Arc::new(Mutex::new(false));
        {
            let done = Arc::clone(&done);
            let timer = scheduler.sleep(5_000);
            scheduler.spawn(Box::pin(async move {
                timer.await;
                *done.lock().expect("done lock") = true;
            }));
        }
        assert_eq!(scheduler.run_until_idle(), 0);
        assert!(*done.lock().expect("done lock"));
        assert!(
            clock.monotonic_nanos() >= 5_000,
            "time auto-advanced to the deadline"
        );
    }
}
