//! The staging engine: **WAL = hot** (§4.2, ADR-0003).
//!
//! There is no separate WAL crate, deliberately: `DuckDB` persistent tables
//! with fsync-on-commit *are* the durability primitive. One store, not two —
//! `StageCommit` is one `DuckDB` transaction and crash replay is the
//! engine's own. The concrete engine lives in [`engine`] (the `duckdb`
//! cargo feature, in the default set since the engine landed — issue #31):
//!
//! - [`StagingEngine`] — open/create the persistent hot database, one
//!   micro-window table per (dataset, partition, window) (§2.3), a write
//!   connection serialized behind a mutex, dedicated read connections that
//!   never contend with it (#114), and checkpointing kept off the ack path
//!   (#109).
//! - [`StageTxn`] — one transactional batch append: rows + system columns
//!   (`origin`, `seq`) + the applied-watermark rows (§4.2.4), fsynced at
//!   [`StageTxn::commit`], returning the per-origin seq coverage
//!   ([`StagedCoverage`]) that `ClientAck` evidence needs (§4.3).
//! - [`StagingReader`] — the dedicated read path (Arrow out, §7.4).
//! - [`EngineStager`] — the [`StageCommitter`] port (defined in
//!   `duckspout-types`, ADR-0008) over the engine: partition assignment
//!   (§2.2), arrival-time window rolling on the [`Clock`] port (§2.3), and
//!   the `StageCommit` transaction the accept path acks on (§4.3). The
//!   accept path reaches staging only through that port; the daemon
//!   composes the two crates (issue #32).
//!
//! What this crate does **not** do yet, by design: the dedup-window table
//! and the overload ladder are the §4.4–§4.6 work (issue #33) and will ride
//! the same `StageTxn` through the same port; replica-side `PeerApply`
//! sharing the applied-watermark mechanism is the replication work (§5).
//!
//! Layering (§10.1, ADR-0008): this crate depends on `duckspout-types` only
//! among workspace crates; the runtime side channels go through the
//! types-defined ports (D-2). The [`Storage`] port's exact boundary here —
//! `DuckDB` is the content-durability primitive, the port owns
//! directory-entry durability — is documented in [`engine`].
//!
//! Design home: `docs/design/ingest.md` (§4.2–§4.3) and
//! `docs/design/data-model.md` (§2.3).

#![forbid(unsafe_code)]

pub use duckspout_types::{
    Clock, StageCommitter, StageError, StagedCoverage, Storage, StorageError,
};

pub mod naming;

#[cfg(feature = "duckdb")]
pub mod engine;
#[cfg(feature = "duckdb")]
pub mod stager;

#[cfg(feature = "duckdb")]
pub use engine::{
    CHECKPOINT_THRESHOLD_DEFERRED, HOT_DB_FILE, StageTxn, StagingConfig, StagingEngine,
    StagingError, StagingReader, WindowRef,
};
#[cfg(feature = "duckdb")]
pub use stager::EngineStager;

/// The arrow pin the engine's append/scan surfaces speak (compat-matrix
/// row 1) — re-exported so embedders build batches against the same version.
#[cfg(feature = "duckdb")]
pub use arrow;
