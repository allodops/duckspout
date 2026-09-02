//! The consistency toolkit (CTK): deterministic doubles for the four runtime
//! ports (D-2) plus armed-vs-fired fault accounting (§8.3).
//!
//! Protocol crates reach time, tasks, the network, and disk only through the
//! port traits defined in `duckspout-types` (ADR-0008); this crate provides
//! the deterministic side of each port:
//!
//! - [`VirtualClock`] — time advances only when told to;
//! - [`SeededScheduler`] — a single-threaded executor whose interleavings
//!   are decided by a pluggable [`ScheduleStrategy`] (ADR-0011's
//!   exploration seam); the v0.1 strategy, [`SeededRandom`], makes the
//!   schedule a pure function of its seed, and #124 adds a PCT-style
//!   prioritized strategy alongside it;
//! - [`InMemNetwork`] / [`InMemTransport`] — in-memory peer messaging with
//!   fault-injection points;
//! - [`InMemStorage`] — an in-memory store modeling the fsync discipline
//!   (content vs. directory-entry durability, torn writes, crash loss).
//!
//! Every injected fault is accounted by the [`InjectorLedger`]: **armed vs.
//! fired** (§8.3's vacuity discipline) — a schedule that arms faults which
//! never fire proves nothing, and the judge treats it as vacuous, not green.
//!
//! Library only (D-5): the distributed runner lives in `duckspout-fleet`.
//! No turmoil/madsim anywhere in production crates (D-2).
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.3).

#![forbid(unsafe_code)]
// Justification for the allow: the doubles guard shared state with std
// mutexes and deliberately propagate lock poisoning as a panic — a poisoned
// double means a test already panicked, and limping on would hide it.
// Repeating a "# Panics: lock poisoned" section on every accessor documents
// the mechanism, not the API.
#![allow(clippy::missing_panics_doc)]

pub mod clock;
pub mod ledger;
pub mod scheduler;
pub mod storage;
pub mod strategy;
mod sync;
pub mod transport;

pub use clock::VirtualClock;
pub use ledger::{FaultCount, InjectorLedger};
pub use scheduler::SeededScheduler;
pub use storage::InMemStorage;
pub use strategy::{ScheduleStrategy, SeededRandom};
pub use transport::{InMemNetwork, InMemTransport};
