//! The in-memory store: the [`Storage`] port's deterministic double,
//! modeling the fsync discipline ADR-0003 puts behind the port.
//!
//! The model distinguishes what real filesystems distinguish:
//!
//! - **content durability** — bytes survive a crash only after
//!   `fsync_file`;
//! - **name durability** — a created file's directory entry survives a
//!   crash only after `fsync_dir` on its parent;
//! - **torn writes** — a crash mid-write leaves a truncated file, detected
//!   on read-back.
//!
//! [`InMemStorage::crash`] applies the model: everything not durable on both
//! axes is gone. Faults are accounted armed-vs-fired (§8.3).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use duckspout_types::{BoxFuture, Storage, StorageError, StoragePath};

use crate::ledger::InjectorLedger;

const FSYNC_FAIL_FAULT: &str = "storage:fail-next-fsync";
const TORN_WRITE_FAULT: &str = "storage:tear-next-put";

#[derive(Debug, Clone)]
struct FileState {
    content: Bytes,
    content_durable: bool,
    name_durable: bool,
    torn: bool,
}

#[derive(Debug, Default)]
struct StoreInner {
    files: HashMap<StoragePath, FileState>,
    fail_next_fsync: bool,
    tear_next_put: bool,
}

/// The in-memory storage double with fault-injection points.
pub struct InMemStorage {
    inner: Mutex<StoreInner>,
    ledger: Arc<InjectorLedger>,
}

fn parent_dir(path: &StoragePath) -> String {
    path.as_str()
        .rsplit_once('/')
        .map_or_else(String::new, |(dir, _)| dir.to_owned())
}

impl InMemStorage {
    /// An empty store accounting its faults in `ledger`.
    #[must_use]
    pub fn new(ledger: Arc<InjectorLedger>) -> Self {
        Self {
            inner: Mutex::new(StoreInner::default()),
            ledger,
        }
    }

    /// Arms `storage:fail-next-fsync`: the next `fsync_file` fails with
    /// [`StorageError::FsyncFailed`], leaving durability unknown.
    pub fn arm_fsync_failure(&self) {
        self.ledger.arm(FSYNC_FAIL_FAULT);
        self.inner.lock().expect("storage lock").fail_next_fsync = true;
    }

    /// Arms `storage:tear-next-put`: the next `put` stores only a prefix of
    /// its bytes, detected as [`StorageError::TornWrite`] on read-back.
    pub fn arm_torn_write(&self) {
        self.ledger.arm(TORN_WRITE_FAULT);
        self.inner.lock().expect("storage lock").tear_next_put = true;
    }

    /// Simulates a crash: files survive only with durable content **and** a
    /// durable name; everything else is gone (ADR-0003's discipline, made
    /// checkable).
    pub fn crash(&self) {
        let mut inner = self.inner.lock().expect("storage lock");
        inner
            .files
            .retain(|_, file| file.content_durable && file.name_durable);
        inner.fail_next_fsync = false;
        inner.tear_next_put = false;
    }

    /// The paths currently visible (test convenience; sorted, so assertions
    /// are deterministic).
    #[must_use]
    pub fn paths(&self) -> Vec<StoragePath> {
        let inner = self.inner.lock().expect("storage lock");
        let mut paths: Vec<StoragePath> = inner.files.keys().cloned().collect();
        paths.sort();
        paths
    }

    fn ready<T>(result: Result<T, StorageError>) -> BoxFuture<'static, Result<T, StorageError>>
    where
        T: Send + 'static,
    {
        Box::pin(async move { result })
    }
}

impl Storage for InMemStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        let mut inner = self.inner.lock().expect("storage lock");
        let torn = inner.tear_next_put;
        if torn {
            inner.tear_next_put = false;
            self.ledger.fired(TORN_WRITE_FAULT);
        }
        let stored = if torn {
            data.slice(0..data.len() / 2)
        } else {
            data
        };
        // An overwrite keeps name durability (the entry already exists) but
        // the new content is not durable until the next fsync_file.
        let name_durable = inner
            .files
            .get(&path)
            .is_some_and(|existing| existing.name_durable);
        inner.files.insert(
            path,
            FileState {
                content: stored,
                content_durable: false,
                name_durable,
                torn,
            },
        );
        Self::ready(Ok(()))
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        let inner = self.inner.lock().expect("storage lock");
        let result = match inner.files.get(&path) {
            Some(file) if file.torn => Err(StorageError::TornWrite(path)),
            Some(file) => Ok(file.content.clone()),
            None => Err(StorageError::NotFound(path)),
        };
        Self::ready(result)
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        let mut inner = self.inner.lock().expect("storage lock");
        let result = match inner.files.remove(&path) {
            Some(_) => Ok(()),
            None => Err(StorageError::NotFound(path)),
        };
        Self::ready(result)
    }

    fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        let mut inner = self.inner.lock().expect("storage lock");
        if inner.fail_next_fsync {
            inner.fail_next_fsync = false;
            self.ledger.fired(FSYNC_FAIL_FAULT);
            return Self::ready(Err(StorageError::FsyncFailed(path)));
        }
        let result = match inner.files.get_mut(&path) {
            Some(file) => {
                file.content_durable = true;
                Ok(())
            }
            None => Err(StorageError::NotFound(path)),
        };
        Self::ready(result)
    }

    fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        let mut inner = self.inner.lock().expect("storage lock");
        for (path, file) in &mut inner.files {
            if parent_dir(path) == dir.as_str() {
                file.name_durable = true;
            }
        }
        Self::ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, Waker};

    fn run<T>(mut future: BoxFuture<'_, T>) -> T {
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("storage futures resolve immediately"),
        }
    }

    #[test]
    fn crash_keeps_only_fully_durable_files() {
        let ledger = Arc::new(InjectorLedger::new());
        let store = InMemStorage::new(Arc::clone(&ledger));
        let durable = StoragePath::new("wal/000001");
        let volatile = StoragePath::new("wal/000002");

        run(store.put(durable.clone(), Bytes::from_static(b"committed"))).expect("put");
        run(store.fsync_file(durable.clone())).expect("fsync");
        run(store.fsync_dir(StoragePath::new("wal"))).expect("fsync dir");
        run(store.put(volatile.clone(), Bytes::from_static(b"unsynced"))).expect("put");

        store.crash();
        assert_eq!(store.paths(), vec![durable.clone()]);
        assert_eq!(
            run(store.get(durable)).expect("get"),
            Bytes::from_static(b"committed")
        );
    }

    #[test]
    fn file_fsync_without_dir_fsync_does_not_survive() {
        let ledger = Arc::new(InjectorLedger::new());
        let store = InMemStorage::new(ledger);
        let path = StoragePath::new("wal/000003");
        run(store.put(path.clone(), Bytes::from_static(b"x"))).expect("put");
        run(store.fsync_file(path)).expect("fsync");
        store.crash();
        assert!(store.paths().is_empty(), "name was never durable");
    }

    #[test]
    fn torn_write_is_detected_on_read_back() {
        let ledger = Arc::new(InjectorLedger::new());
        let store = InMemStorage::new(Arc::clone(&ledger));
        store.arm_torn_write();
        let path = StoragePath::new("part");
        run(store.put(path.clone(), Bytes::from_static(b"0123456789"))).expect("put");
        assert!(matches!(
            run(store.get(path)),
            Err(StorageError::TornWrite(_))
        ));
        assert!(ledger.vacuously_armed().is_empty());
    }

    #[test]
    fn armed_fsync_failure_fires_once() {
        let ledger = Arc::new(InjectorLedger::new());
        let store = InMemStorage::new(Arc::clone(&ledger));
        let path = StoragePath::new("wal/000004");
        run(store.put(path.clone(), Bytes::from_static(b"x"))).expect("put");
        store.arm_fsync_failure();
        assert!(matches!(
            run(store.fsync_file(path.clone())),
            Err(StorageError::FsyncFailed(_))
        ));
        run(store.fsync_file(path)).expect("second fsync succeeds");
        assert!(ledger.vacuously_armed().is_empty());
    }
}
