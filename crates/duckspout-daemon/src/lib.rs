//! The `DuckSpout` node daemon, as a library (§10.4).
//!
//! `main.rs` is a thin CLI wrapper over this crate: config parsing, signal
//! wiring, and calling [`wiring::Daemon::boot`] /
//! [`wiring::Daemon::serve`] — "anything the daemon can do, an embedder can
//! do by depending on the crates directly" (§10) applies to the daemon's own
//! composition too, which is why it is a library first. This is also the
//! seam `tests/e2e_boot.rs` boots the daemon through: its own public API,
//! never the binary.
//!
//! See [`wiring`]'s module docs for what is wired at v0.1 (issue #38) and
//! what is deliberately deferred. The §7.4 Flight server over the hot store
//! ([`serving`]) and the production [`Clock`] implementation ([`clock`])
//! are constructed directly by embedders and by this crate's own
//! integration tests, per the design of record
//! (`docs/design/query.md`: "the daemon's Flight server").
//!
//! [`Clock`]: duckspout_types::Clock

#![forbid(unsafe_code)]

pub mod clock;
pub mod config;
pub mod constants;
pub mod manifest;
pub mod serving;
pub mod status;
pub mod system;
pub mod wiring;

pub use clock::StdClock;
pub use serving::{HotFlightService, ServingConfig};
