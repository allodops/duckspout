//! Loom interleaving exploration of the CTK's own concurrency primitives
//! (§8.3: "loom explores the Rust memory-model interleavings of the code
//! implementing one action").
//!
//! Run with `just test-loom` (or
//! `cargo nextest run -p duckspout-ctk --features loom --test loom`); the
//! `loom` feature swaps the lib's sync primitives for loom's checked types
//! (`duckspout_ctk::sync`), so these models drive the **real**
//! `VirtualClock` and `InjectorLedger` — never copies.
//!
//! Scope, stated honestly: the [`duckspout_ctk::SeededScheduler`] is a
//! deliberately single-threaded executor (its interleaving exploration IS
//! the `ScheduleStrategy` seam, and its determinism laws are property-tested
//! in `src/scheduler.rs`); loom targets the two primitives that are
//! genuinely shared across OS threads by harness code — the clock and the
//! injector ledger. Protocol crates are deterministic-by-ports and carry no
//! loom of their own.

#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::thread;

use duckspout_ctk::{FaultCount, InjectorLedger, VirtualClock};
use duckspout_types::Clock as _;

/// `advance_to_nanos` under a race keeps time monotone and converges on the
/// maximum deadline. Would catch: a load-then-store advance (instead of the
/// atomic `fetch_max`), where the smaller deadline's late store rolls time
/// backwards — exactly the interleaving loom forces and a sampled test
/// almost never hits.
#[test]
fn concurrent_advance_to_converges_on_the_maximum() {
    loom::model(|| {
        let clock = Arc::new(VirtualClock::new());
        let a = {
            let clock = Arc::clone(&clock);
            thread::spawn(move || clock.advance_to_nanos(5))
        };
        let b = {
            let clock = Arc::clone(&clock);
            thread::spawn(move || clock.advance_to_nanos(3))
        };
        // A concurrent reader must never see time move backwards.
        let first = clock.monotonic_nanos();
        let second = clock.monotonic_nanos();
        assert!(second >= first, "time went backwards: {first} -> {second}");
        a.join().expect("advance(5) thread");
        b.join().expect("advance(3) thread");
        assert_eq!(clock.monotonic_nanos(), 5, "the maximum deadline wins");
    });
}

/// Concurrent `advance_nanos` deltas all land. Would catch: a lost update
/// from a non-atomic read-modify-write (final time 2 or 3 instead of 5).
#[test]
fn concurrent_advance_deltas_are_never_lost() {
    loom::model(|| {
        let clock = Arc::new(VirtualClock::new());
        let a = {
            let clock = Arc::clone(&clock);
            thread::spawn(move || clock.advance_nanos(2))
        };
        let b = {
            let clock = Arc::clone(&clock);
            thread::spawn(move || clock.advance_nanos(3))
        };
        a.join().expect("advance(2) thread");
        b.join().expect("advance(3) thread");
        assert_eq!(clock.monotonic_nanos(), 5, "a delta was lost");
    });
}

/// Concurrent arm/fire accounting is exact. The ledger is the §8.3 vacuity
/// verdict's evidence — an undercount on `fired` convicts an honest run as
/// vacuous, an undercount on `armed` lets a vacuous run pass. Would catch:
/// racy compound updates on the counts (e.g. a get-then-insert outside the
/// lock) losing one thread's increment.
#[test]
fn concurrent_arm_and_fire_counts_are_exact() {
    loom::model(|| {
        let ledger = Arc::new(InjectorLedger::new());
        let a = {
            let ledger = Arc::clone(&ledger);
            thread::spawn(move || {
                ledger.arm("storage:fsync-fail");
                ledger.fired("storage:fsync-fail");
            })
        };
        let b = {
            let ledger = Arc::clone(&ledger);
            thread::spawn(move || ledger.arm("storage:fsync-fail"))
        };
        a.join().expect("arm+fire thread");
        b.join().expect("arm thread");
        assert_eq!(
            ledger.count("storage:fsync-fail"),
            FaultCount { armed: 2, fired: 1 }
        );
        assert_eq!(
            ledger.vacuously_armed(),
            Vec::<String>::new(),
            "the fault fired; the run is not vacuous"
        );
    });
}
