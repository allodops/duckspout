//! Shared fixtures for the staging engine's integration tests.
//!
//! The engine's own tests run against real `DuckDB` files — inside this
//! crate `DuckDB` *is* the storage (ADR-0003) — so the [`Storage`] port is
//! implemented here over the real filesystem ([`FsStorage`]): the port's
//! one engine-side duty, directory fsync, really happens. The CTK's
//! in-memory double is out of reach by layering (invariants.toml forbids
//! staging → ctk even as a dev-dependency), and would be the wrong fidelity
//! here anyway.

// Justification for the allow: every integration-test binary compiles this
// module independently and none uses all of it; the per-binary unused
// remainder is expected, not dead design.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use duckspout_staging::arrow::array::{Int32Array, StringArray, TimestampMicrosecondArray};
use duckspout_staging::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckspout_staging::arrow::record_batch::RecordBatch;
use duckspout_staging::{StagingConfig, StagingEngine};
use duckspout_types::{BoxFuture, NodeId, Storage, StorageError, StoragePath};

/// A real-filesystem [`Storage`] rooted at one directory. Only the calls the
/// engine actually makes get full fidelity; everything is implemented
/// honestly enough for reuse.
pub struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, path: &StoragePath) -> PathBuf {
        if path.as_str().is_empty() {
            self.root.clone()
        } else {
            self.root.join(path.as_str())
        }
    }

    fn ready<T: Send + 'static>(
        result: Result<T, StorageError>,
    ) -> BoxFuture<'static, Result<T, StorageError>> {
        Box::pin(async move { result })
    }

    fn backend(err: &std::io::Error, path: &StoragePath) -> StorageError {
        if err.kind() == std::io::ErrorKind::NotFound {
            StorageError::NotFound(path.clone())
        } else {
            StorageError::Backend(err.to_string())
        }
    }
}

impl Storage for FsStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        let result =
            std::fs::write(self.resolve(&path), &data).map_err(|e| Self::backend(&e, &path));
        Self::ready(result)
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        let result = std::fs::read(self.resolve(&path))
            .map(Bytes::from)
            .map_err(|e| Self::backend(&e, &path));
        Self::ready(result)
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        let result =
            std::fs::remove_file(self.resolve(&path)).map_err(|e| Self::backend(&e, &path));
        Self::ready(result)
    }

    fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        let result = std::fs::File::open(self.resolve(&path))
            .and_then(|f| f.sync_all())
            .map_err(|_| StorageError::FsyncFailed(path.clone()));
        Self::ready(result)
    }

    fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        let result = std::fs::File::open(self.resolve(&dir))
            .and_then(|f| f.sync_all())
            .map_err(|_| StorageError::FsyncFailed(dir.clone()));
        Self::ready(result)
    }
}

/// Opens an engine on `hot_dir` with an [`FsStorage`] rooted there.
pub fn open_engine(hot_dir: &Path, origin: &str) -> StagingEngine<FsStorage> {
    StagingEngine::open(
        StagingConfig {
            hot_dir: hot_dir.to_path_buf(),
            origin: NodeId::new(origin),
        },
        FsStorage::new(hot_dir),
    )
    .expect("open staging engine")
}

/// A synthetic OTLP-log-shaped payload batch: `ts` (µs timestamp, NOT NULL),
/// `severity` (Int32), `body` (Utf8). `body_pad` controls the per-row byte
/// weight for WAL-size-driven tests.
pub fn log_batch(rows: usize, first_ts_micros: i64, body_pad: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("severity", DataType::Int32, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ts: TimestampMicrosecondArray = (0..rows)
        .map(|i| Some(first_ts_micros + i64::try_from(i).expect("row index")))
        .collect();
    let severity: Int32Array = (0..rows)
        .map(|i| Some(i32::try_from(i % 24).expect("bounded")))
        .collect();
    let pad = "x".repeat(body_pad);
    let body: StringArray = (0..rows)
        .map(|i| Some(format!("synthetic log line {i} {pad}")))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(ts), Arc::new(severity), Arc::new(body)],
    )
    .expect("build log batch")
}
