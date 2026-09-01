//! The staging engine: **WAL = hot** (§4.2, ADR-0003).
//!
//! There is no separate WAL crate, deliberately: `DuckDB` persistent tables
//! with fsync-on-commit *are* the durability primitive. One store, not two —
//! `StageCommit` is one `DuckDB` transaction (rows + dedup-window entry +
//! applied-watermark row, §4.2.4, §4.3), and crash replay is the engine's
//! own. Fsync discipline — directory fsync, torn-write detection, group
//! commit off the reactor — lives behind the [`Storage`] port and its CTK
//! fault injectors (ADR-0003).
//!
//! Layering (§10.1, ADR-0008): this crate depends on `duckspout-types` only
//! among workspace crates; it consumes the runtime exclusively through the
//! types-defined ports (D-2 — no direct network, wall-clock, randomness,
//! or process-spawning APIs; the invariants engine enforces the ban).
//!
//! Ⓢ bootstrap stub — the engine lands at v0.1. The `duckdb` cargo feature
//! (the bundled engine, ADR-0002) is **off by default at bootstrap** —
//! compiling the vendored C++ costs >10 minutes with no engine code to use
//! it yet — and joins the default set when the engine lands.
//!
//! Design home: `docs/design/ingest.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §4.2 and ADR-0003).

#![forbid(unsafe_code)]

use duckspout_types::{Storage, StorageError};

/// The embedded hot-store connection type (ADR-0002: bundled DuckDB C++ is
/// upstream's code, not first-party C++).
#[cfg(feature = "duckdb")]
pub type HotConnection = duckdb::Connection;

/// The staging engine over a [`Storage`] port. Ⓢ v0.1: `StageCommit`,
/// micro-window tables (§2.3), the dedup window (§4.4.1), and the
/// applied-watermark rows (§4.2.4) land together with the engine.
pub struct StagingEngine<S: Storage> {
    storage: S,
}

impl<S: Storage> StagingEngine<S> {
    /// Wraps a storage port. No I/O happens until the engine lands (v0.1).
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// The storage port this engine commits through.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// `StageCommit` (§4.3): one transaction — rows + dedup-window entry +
    /// applied-watermark row — fsync on commit. Ⓢ v0.1.
    ///
    /// # Errors
    ///
    /// Always [`StorageError::Backend`] with a not-implemented notice until
    /// the engine lands.
    pub fn stage_commit_stub(&self) -> Result<(), StorageError> {
        Err(StorageError::Backend(
            "StageCommit lands at v0.1 (§4.3, ADR-0003)".to_owned(),
        ))
    }
}
