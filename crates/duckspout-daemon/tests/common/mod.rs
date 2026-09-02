//! Shared port doubles for the daemon's composition tests: the
//! real-filesystem [`Storage`] and a settable test [`Clock`]. Test-local by
//! design — production wiring lives in `src/wiring.rs`, and the CTK's
//! doubles model faults these tests do not inject.

// Justification for the allow: every integration-test binary compiles this
// module independently and none uses all of it.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use duckspout_types::{BoxFuture, Clock, Storage, StorageError, StoragePath};

/// A real-filesystem [`Storage`] rooted at a directory — the engine's one
/// port duty here (directory fsync) really happens.
pub struct FsStorage {
    pub root: PathBuf,
}

impl FsStorage {
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
}

impl Storage for FsStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::write(self.resolve(&path), &data)
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        Self::ready(
            std::fs::read(self.resolve(&path))
                .map(Bytes::from)
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::remove_file(self.resolve(&path))
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::File::open(self.resolve(&path))
                .and_then(|f| f.sync_all())
                .map_err(|_| StorageError::FsyncFailed(path.clone())),
        )
    }

    fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::File::open(self.resolve(&dir))
                .and_then(|f| f.sync_all())
                .map_err(|_| StorageError::FsyncFailed(dir.clone())),
        )
    }
}

/// A settable test [`Clock`]: cloneable (shared inner), advanced explicitly
/// by the test — arrival-window rolling and the drain's lateness hold are
/// driven, never awaited.
#[derive(Clone, Default)]
pub struct SettableClock {
    inner: Arc<ClockInner>,
}

#[derive(Default)]
struct ClockInner {
    nanos: AtomicU64,
    wall_ms: AtomicI64,
}

impl SettableClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_nanos(&self, nanos: u64) {
        self.inner.nanos.store(nanos, Ordering::SeqCst);
    }

    pub fn set_wall_ms(&self, ms: i64) {
        self.inner.wall_ms.store(ms, Ordering::SeqCst);
    }
}

impl Clock for SettableClock {
    fn monotonic_nanos(&self) -> u64 {
        self.inner.nanos.load(Ordering::SeqCst)
    }

    fn wall_unix_ms(&self) -> i64 {
        self.inner.wall_ms.load(Ordering::SeqCst)
    }
}
