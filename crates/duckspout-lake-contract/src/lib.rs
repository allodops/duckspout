//! The `LakeCommitter` contract's home crate (§6.4, §10.3).
//!
//! The trait itself is defined in `duckspout-types` (ADR-0008) and
//! re-exported here; this crate owns everything beyond the bare signature —
//! above all the published **conformance suite** ([`conformance`]) that lets
//! backend #2 (and #3) be a community contribution validated by the same
//! harness, not a fork (§6.4).
//!
//! Neutrality rule (Keep Rule, §11): nothing on the critical path may depend
//! on a backend-exclusive feature; the six-operation contract is expressible
//! in both `DuckLake` and Iceberg.
//!
//! Design home: `docs/design/drain.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §6.4–§6.5).

#![forbid(unsafe_code)]

pub use duckspout_types::{
    AttachInfo, ColumnSpec, CommitOutcome, LakeCommitter, LakeError, SchemaEvolution,
};

pub mod conformance;
