//! `EngineStager` tests: the `StageCommitter` port over the real engine —
//! partition assignment, arrival-time window rolling on the Clock port,
//! dense never-reused window ids across drop and reopen, and the IPC seam.
//!
//! The clock double is test-local (like `common::FsStorage`): the invariant
//! engine audits dev-dependency edges too, and staging → ctk is forbidden.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use common::{log_batch, open_engine};
use duckspout_staging::{EngineStager, StageError};
use duckspout_types::{Clock, DatasetId, DecodedBatch, PartitionId, StageCommitter, WindowId};

const WINDOW_NANOS: u64 = 60_000_000_000; // hot.window default, 60 s

/// A hand-cranked Clock: monotonic time advances only when told to.
#[derive(Default)]
struct ManualClock {
    nanos: Arc<AtomicU64>,
}

impl ManualClock {
    fn handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.nanos)
    }
}

impl Clock for ManualClock {
    fn monotonic_nanos(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }

    fn wall_unix_ms(&self) -> i64 {
        0
    }
}

fn ipc_bytes(batch: &duckspout_staging::arrow::record_batch::RecordBatch) -> Bytes {
    let mut writer =
        duckspout_staging::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &batch.schema())
            .unwrap();
    writer.write(batch).unwrap();
    Bytes::from(writer.into_inner().unwrap())
}

fn decoded(tenant: &str, rows: usize, first_ts: i64) -> DecodedBatch {
    DecodedBatch {
        dataset: DatasetId::new("otlp_logs"),
        kind: duckspout_types::DatasetKind::Event,
        tenant: duckspout_types::TenantId::new(tenant),
        idempotency_key: None,
        records: ipc_bytes(&log_batch(rows, first_ts, 0)),
    }
}

fn stager_over(
    hot_dir: &std::path::Path,
) -> (EngineStager<common::FsStorage, ManualClock>, Arc<AtomicU64>) {
    let clock = ManualClock::default();
    let handle = clock.handle();
    let engine = Arc::new(open_engine(hot_dir, "node-a/1"));
    (EngineStager::new(engine, clock, WINDOW_NANOS), handle)
}

/// The port's core contract: staging returns per-partition coverage that is
/// dense across successive commits, and the rows are really there —
/// tenant-derived partition, window 0, exact counts. Would catch coverage
/// that disagrees with the landed rows or a partition derived differently
/// from §2.2's key shape.
#[test]
fn stage_commit_returns_dense_coverage_and_lands_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _) = stager_over(dir.path());

    let coverage = stager
        .stage_blocking(&decoded("tenant-a", 5, 1_000))
        .unwrap();
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("tenant-a"), 0);
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].partition, partition);
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (1, 5)
    );

    // A second batch of the same tenant, same window: seq continues densely.
    let coverage = stager
        .stage_blocking(&decoded("tenant-a", 3, 2_000))
        .unwrap();
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (6, 8)
    );

    let reader = stager.engine().reader().unwrap();
    let dataset = DatasetId::new("otlp_logs");
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .unwrap(),
        8
    );
}

/// Two tenants are two partitions (§2.2): separate windows, separate dense
/// seq sequences — byte-identical payloads never share a partition.
#[test]
fn tenants_map_to_disjoint_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _) = stager_over(dir.path());

    let a = stager.stage_blocking(&decoded("tenant-a", 2, 0)).unwrap();
    let b = stager.stage_blocking(&decoded("tenant-b", 2, 0)).unwrap();
    assert_ne!(a[0].partition, b[0].partition);
    assert_eq!((a[0].range.first_seq, a[0].range.last_seq), (1, 2));
    assert_eq!((b[0].range.first_seq, b[0].range.last_seq), (1, 2));
}

/// Window rolling is a pure function of the Clock port (§2.3): within
/// `hot.window` the same window receives writes; past it a new dense id
/// opens. Would catch a roller that reads a real clock (undrivable) or
/// rolls per batch.
#[test]
fn windows_roll_on_arrival_time() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, clock) = stager_over(dir.path());
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);

    stager.stage_blocking(&decoded("t", 1, 0)).unwrap();
    clock.store(WINDOW_NANOS - 1, Ordering::SeqCst); // 1 ns before the roll
    stager.stage_blocking(&decoded("t", 1, 1)).unwrap();
    let reader = stager.engine().reader().unwrap();
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .unwrap(),
        2
    );

    clock.store(WINDOW_NANOS, Ordering::SeqCst); // exactly hot.window later
    stager.stage_blocking(&decoded("t", 1, 2)).unwrap();
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(1))
            .unwrap(),
        1
    );
    assert_eq!(
        stager.engine().list_windows().unwrap().len(),
        2,
        "exactly the two windows exist"
    );
}

/// Window ids stay dense and are never reused, across both `DropWindow`
/// (the drain's cleanup) and an engine reopen (restart). Would catch an
/// allocator seeded from the live registry — the bug where dropping every
/// window resets the sequence and a recycled id collides with a drained
/// window's committed identity (§2.3).
#[test]
fn window_ids_survive_drop_and_reopen_without_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);

    {
        let (stager, clock) = stager_over(dir.path());
        stager.stage_blocking(&decoded("t", 1, 0)).unwrap(); // window 0
        clock.store(WINDOW_NANOS, Ordering::SeqCst);
        stager.stage_blocking(&decoded("t", 1, 1)).unwrap(); // window 1
        // Drain both windows away: the live registry is now empty.
        assert!(
            stager
                .engine()
                .drop_window(&dataset, &partition, WindowId(0))
                .unwrap()
        );
        assert!(
            stager
                .engine()
                .drop_window(&dataset, &partition, WindowId(1))
                .unwrap()
        );
        assert!(stager.engine().list_windows().unwrap().is_empty());
        // Same process: the next roll must go to 2, not back to 0.
        clock.store(2 * WINDOW_NANOS, Ordering::SeqCst);
        stager.stage_blocking(&decoded("t", 1, 2)).unwrap();
        assert_eq!(
            stager.engine().list_windows().unwrap()[0].window,
            WindowId(2)
        );
        assert!(
            stager
                .engine()
                .drop_window(&dataset, &partition, WindowId(2))
                .unwrap()
        );
    }

    // Restart with every window drained away: allocation resumes past the
    // persistent high-water, never at 0.
    let (stager, _) = stager_over(dir.path());
    stager.stage_blocking(&decoded("t", 1, 3)).unwrap();
    assert_eq!(
        stager.engine().list_windows().unwrap()[0].window,
        WindowId(3)
    );
    assert_eq!(
        stager
            .engine()
            .highest_window_id(&dataset, &partition)
            .unwrap(),
        Some(WindowId(3))
    );
}

/// The port trait itself: `stage_commit` resolves with the same result as
/// the blocking body (the future is the seam the accept path awaits).
#[test]
fn port_future_resolves_synchronously_with_the_result() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _) = stager_over(dir.path());
    let future = stager.stage_commit(decoded("t", 2, 0));
    let coverage = pollster_block_on(future).unwrap();
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (1, 2)
    );
}

/// Non-IPC bytes fail typed and stage nothing — the adapter↔stager contract
/// breach is `MalformedRecords`, and no window or seq is consumed.
#[test]
fn malformed_records_stage_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _) = stager_over(dir.path());
    let mut batch = decoded("t", 1, 0);
    batch.records = Bytes::from_static(b"not an arrow ipc stream");
    let err = stager.stage_blocking(&batch).unwrap_err();
    assert!(matches!(err, StageError::MalformedRecords(_)));
    assert!(stager.engine().list_windows().unwrap().is_empty());
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);
    assert_eq!(stager.engine().applied_seq(&partition).unwrap(), None);
}

/// Drives a ready-or-not future to completion on this thread (the engine's
/// own `block_on` shape, test-local).
fn pollster_block_on<T>(mut future: duckspout_types::BoxFuture<'_, T>) -> T {
    use std::task::{Context, Poll, Wake, Waker};
    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match std::future::Future::poll(future.as_mut(), &mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
