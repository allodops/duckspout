//! The first `LakeCommitter` backend: `DuckLake` (§6.4).
//!
//! The committer embeds a `DuckDB` instance used purely as a metadata-commit
//! executor — rows never transit it. `commit_files` executes
//! `CALL ducklake_add_data_files(...)` and inserts the watermark sidecar row
//! in the **same Postgres transaction** as `DuckLake`'s own catalog writes:
//! one transaction, one atomicity domain — the whole mechanism behind
//! `WatermarkHonesty` on this backend (§6.4).
//!
//! This is the only crate that knows `DuckLake` (§10.1): everything above the
//! port is lake-neutral, and `DuckLake`-exclusive optimizations (inlining,
//! §6.2) never become critical-path requirements (Keep Rule, §11).
//!
//! Ⓢ bootstrap stub — every operation returns
//! [`LakeError::NotImplemented`] honestly (never a panic, never a fake
//! outcome); the embedded-`DuckDB` executor lands at v0.1 and brings the
//! `duckdb` dependency with it.
//!
//! Design home: `docs/design/drain.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §6.4–§6.5).

#![forbid(unsafe_code)]

use duckspout_types::{
    AttachInfo, BoxFuture, CommitOutcome, LakeCommitter, LakeError, PartName, PartitionId,
    SchemaEvolution, WatermarkRow, WindowManifest,
};

/// The `DuckLake` committer. Ⓢ v0.1.
#[derive(Debug, Clone)]
pub struct DuckLakeCommitter {
    catalog_dsn: String,
}

impl DuckLakeCommitter {
    /// Configures a committer against a Postgres-backed `DuckLake` catalog.
    /// No connection is opened until the executor lands (v0.1).
    #[must_use]
    pub fn new(catalog_dsn: impl Into<String>) -> Self {
        Self {
            catalog_dsn: catalog_dsn.into(),
        }
    }

    /// The configured catalog DSN.
    #[must_use]
    pub fn catalog_dsn(&self) -> &str {
        &self.catalog_dsn
    }

    fn not_implemented<T>(op: &'static str) -> BoxFuture<'static, Result<T, LakeError>>
    where
        T: Send + 'static,
    {
        Box::pin(async move { Err(LakeError::NotImplemented(op)) })
    }
}

impl LakeCommitter for DuckLakeCommitter {
    fn commit_files(
        &self,
        _manifest: WindowManifest,
        _watermarks: Vec<WatermarkRow>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        Self::not_implemented("ducklake commit_files (v0.1)")
    }

    fn replace_files(
        &self,
        _remove: Vec<PartName>,
        _add: Vec<PartName>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        Self::not_implemented("ducklake replace_files (v0.1)")
    }

    fn evolve_schema(&self, _change: SchemaEvolution) -> BoxFuture<'_, Result<(), LakeError>> {
        Self::not_implemented("ducklake evolve_schema (v0.1)")
    }

    fn expire(&self, _parts: Vec<PartName>) -> BoxFuture<'_, Result<(), LakeError>> {
        Self::not_implemented("ducklake expire (v0.1)")
    }

    fn read_watermarks(
        &self,
        _partitions: Vec<PartitionId>,
    ) -> BoxFuture<'_, Result<Vec<WatermarkRow>, LakeError>> {
        Self::not_implemented("ducklake read_watermarks (v0.1)")
    }

    fn attach_info(&self) -> BoxFuture<'_, Result<AttachInfo, LakeError>> {
        Self::not_implemented("ducklake attach_info (v0.1)")
    }
}
