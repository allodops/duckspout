//! `duckspout-loadgen` library surface (§8.4, D-5): the OTLP client, the
//! ack/timeout race, and the journal — split out of `main.rs` so the pure
//! logic is unit-testable and the wire behavior is testable against a real
//! in-process accept endpoint (`tests/`), without a fleet.
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.4 and §3.7).

#![forbid(unsafe_code)]

pub mod client;
pub mod journal;
pub mod outcome;
