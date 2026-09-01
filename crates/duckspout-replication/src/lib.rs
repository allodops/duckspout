//! Replication (§5): HRW ownership placement, the forward/receipt ring, and
//! incarnation fencing.
//!
//! The HRW placement function is **implemented** here (ADR-0004: in-house,
//! ~50 pure lines; the `hrw-hash` crate lost on adoption/maintenance). The
//! ring protocol and fencing are Ⓢ stubs landing at v0.2. §8.5
//! property-tests HRW's minimal-disruption law exactly from v0.1 on, and it
//! is cross-checked against the TLA+ placement function once
//! `Replication.tla` lands (ADR-0004).
//!
//! Layering (§10.1, ADR-0008): depends on `duckspout-types` only among
//! workspace crates; the runtime is reached exclusively through the
//! types-defined ports (D-2).
//!
//! Design home: `docs/design/replication.md` (lands at absorption; until
//! then see `DUCKSPOUT.md` §5).

#![forbid(unsafe_code)]

pub mod fencing;
pub mod hrw;

pub use hrw::{hrw_owner, hrw_ranked};
