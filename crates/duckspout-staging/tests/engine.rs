//! Engine behavior: micro-window lifecycle, seq-coverage bookkeeping,
//! read/write separation (#114), and typed refusals. Real `DuckDB` files
//! throughout — inside this crate the engine is the storage (ADR-0003).

mod common;

use duckspout_staging::{StagingEngine, StagingError};
use duckspout_types::{
    BoxFuture, DatasetId, NodeId, PartitionId, Storage, StorageError, StoragePath, WindowId,
};

use common::{FsStorage, log_batch, open_engine};

fn ids(partition: &str) -> (DatasetId, PartitionId) {
    (DatasetId::new("logs"), PartitionId::new(partition))
}

/// Would catch: coverage ranges that are not dense, not contiguous across
/// appends, not per-partition, or that disagree with what actually landed.
#[test]
fn stage_commit_returns_dense_per_partition_coverage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");
    let p2 = PartitionId::new("t2.0");
    let w0 = WindowId(0);

    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w0, &log_batch(3, 1_000, 0))
        .expect("append p1 a");
    txn.append(&dataset, &p1, w0, &log_batch(2, 2_000, 0))
        .expect("append p1 b");
    txn.append(&dataset, &p2, w0, &log_batch(4, 3_000, 0))
        .expect("append p2");
    let coverage = txn.commit().expect("commit");

    assert_eq!(coverage.len(), 2);
    assert_eq!(coverage[0].partition, p1);
    assert_eq!(coverage[0].range.origin, NodeId::new("node-a/1"));
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (1, 5)
    );
    assert_eq!(coverage[1].partition, p2);
    assert_eq!(
        (coverage[1].range.first_seq, coverage[1].range.last_seq),
        (1, 4)
    );

    let reader = engine.reader().expect("reader");
    assert_eq!(reader.count_window(&dataset, &p1, w0).expect("count"), 5);
    assert_eq!(reader.count_window(&dataset, &p2, w0).expect("count"), 4);
    assert_eq!(engine.applied_seq(&p1).expect("applied"), Some(5));
    assert_eq!(engine.applied_seq(&p2).expect("applied"), Some(4));
}

/// Would catch: an applied-watermark row that does not ride the commit, or
/// seq assignment that restarts (or gaps) across transactions and reopens.
#[test]
fn seq_is_dense_across_transactions_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (dataset, p1) = ids("t1.0");
    {
        let engine = open_engine(dir.path(), "node-a/1");
        let mut txn = engine.begin().expect("begin");
        txn.append(&dataset, &p1, WindowId(0), &log_batch(5, 0, 0))
            .expect("append");
        txn.commit().expect("commit");
        let mut txn = engine.begin().expect("begin");
        txn.append(&dataset, &p1, WindowId(0), &log_batch(3, 0, 0))
            .expect("append");
        let coverage = txn.commit().expect("commit");
        assert_eq!(
            (coverage[0].range.first_seq, coverage[0].range.last_seq),
            (6, 8)
        );
    }
    // Clean reopen: bookkeeping reloads from the applied rows (§4.2.4).
    let engine = open_engine(dir.path(), "node-a/1");
    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, WindowId(1), &log_batch(2, 0, 0))
        .expect("append");
    let coverage = txn.commit().expect("commit");
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (9, 10)
    );
}

/// Would catch: a rollback that leaks assigned seqs (gapping the dense
/// sequence), or DDL that survives the rolled-back transaction.
#[test]
fn rollback_releases_seqs_and_created_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");
    let w1 = WindowId(1);

    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w1, &log_batch(2, 0, 0))
        .expect("append");
    txn.rollback().expect("rollback");
    assert_eq!(engine.applied_seq(&p1).expect("applied"), None);
    assert!(engine.list_windows().expect("list").is_empty());

    // An implicit rollback (drop) behaves identically.
    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w1, &log_batch(2, 0, 0))
        .expect("append");
    drop(txn);

    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w1, &log_batch(2, 0, 0))
        .expect("append");
    let coverage = txn.commit().expect("commit");
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (1, 2),
        "rolled-back transactions must release their seqs"
    );
    let reader = engine.reader().expect("reader");
    assert_eq!(reader.count_window(&dataset, &p1, w1).expect("count"), 2);
    assert_eq!(engine.list_windows().expect("list").len(), 1);
}

/// The #114 constraint: a dedicated read connection answers while the write
/// connection holds an open transaction — a blocked read would hang this
/// test (single-threaded by design: blocking equals failure), and a read
/// that saw uncommitted rows would break snapshot isolation.
#[test]
fn reads_do_not_contend_with_an_open_write_transaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");
    let w0 = WindowId(0);

    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w0, &log_batch(10, 0, 0))
        .expect("append");
    txn.commit().expect("commit");

    let reader = engine.reader().expect("reader");
    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w0, &log_batch(5, 0, 0))
        .expect("append");
    // The write transaction is open and holds the write lock right now.
    assert_eq!(
        reader
            .count_window(&dataset, &p1, w0)
            .expect("read during open write txn"),
        10,
        "reader must see the committed snapshot, not uncommitted appends"
    );
    txn.commit().expect("commit");
    assert_eq!(reader.count_window(&dataset, &p1, w0).expect("count"), 15);

    // The serve seam returns engine-native Arrow (§7.4).
    let table = duckspout_staging::naming::window_table_name(&dataset, &p1, w0);
    let (schema, batches) = reader
        .query_arrow(&format!(
            "SELECT ts, severity, body, origin, seq FROM {table} ORDER BY seq"
        ))
        .expect("query_arrow");
    assert_eq!(schema.fields().len(), 5);
    let rows: usize = batches
        .iter()
        .map(duckspout_staging::arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 15);
}

/// Would catch: a registry that drifts from the actual tables, a
/// non-idempotent `DropWindow`, or state that does not survive reopen.
#[test]
fn window_lifecycle_create_list_drop_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (dataset, p1) = ids("t1.0");
    {
        let engine = open_engine(dir.path(), "node-a/1");
        let mut txn = engine.begin().expect("begin");
        txn.append(&dataset, &p1, WindowId(0), &log_batch(1, 0, 0))
            .expect("append");
        txn.append(&dataset, &p1, WindowId(1), &log_batch(1, 0, 0))
            .expect("append");
        txn.commit().expect("commit");

        let windows = engine.list_windows().expect("list");
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].window, WindowId(0));
        assert_eq!(windows[1].window, WindowId(1));
        assert_eq!(
            windows[0].table_name,
            duckspout_staging::naming::window_table_name(&dataset, &p1, WindowId(0))
        );

        // DropWindow is O(1) cleanup and idempotent (§2.3).
        assert!(
            engine
                .drop_window(&dataset, &p1, WindowId(0))
                .expect("drop")
        );
        assert!(
            !engine
                .drop_window(&dataset, &p1, WindowId(0))
                .expect("second drop")
        );
        let reader = engine.reader().expect("reader");
        assert!(
            reader.count_window(&dataset, &p1, WindowId(0)).is_err(),
            "dropped window table must be gone"
        );
    }
    let engine = open_engine(dir.path(), "node-a/1");
    let windows = engine.list_windows().expect("list");
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].window, WindowId(1));
}

/// Would catch: payload columns silently shadowing the system columns the
/// replication log depends on (§4.2.3).
#[test]
fn payload_columns_may_not_shadow_system_columns() {
    use duckspout_staging::arrow::array::StringArray;
    use duckspout_staging::arrow::datatypes::{DataType, Field, Schema};
    use duckspout_staging::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "Origin",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec!["x"]))],
    )
    .expect("batch");

    let mut txn = engine.begin().expect("begin");
    let err = txn
        .append(&dataset, &p1, WindowId(0), &batch)
        .expect_err("reserved column must be refused");
    assert!(matches!(err, StagingError::ReservedColumn { .. }), "{err}");
}

/// Would catch: an arrow type outside the supported subset reaching DDL
/// generation and failing somewhere less typed.
#[test]
fn unsupported_arrow_types_are_refused_typed() {
    use duckspout_staging::arrow::array::TimestampNanosecondArray;
    use duckspout_staging::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use duckspout_staging::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");
    let nanos: TimestampNanosecondArray = vec![Some(1)].into();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )])),
        vec![Arc::new(nanos)],
    )
    .expect("batch");

    let mut txn = engine.begin().expect("begin");
    let err = txn
        .append(&dataset, &p1, WindowId(0), &batch)
        .expect_err("unsupported type must be refused");
    assert!(
        matches!(err, StagingError::UnsupportedColumnType { .. }),
        "{err}"
    );
}

/// Would catch: empty batches consuming sequence numbers or failing to
/// create the window table they address.
#[test]
fn empty_batch_creates_the_window_and_assigns_no_seqs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");

    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, WindowId(0), &log_batch(0, 0, 0))
        .expect("append empty");
    let coverage = txn.commit().expect("commit");
    assert!(coverage.is_empty());
    assert_eq!(engine.applied_seq(&p1).expect("applied"), None);
    let reader = engine.reader().expect("reader");
    assert_eq!(
        reader
            .count_window(&dataset, &p1, WindowId(0))
            .expect("count"),
        0
    );
}

/// Would catch: a second append silently landing with a different payload
/// schema (the per-table schema is fixed at creation).
#[test]
fn schema_mismatch_on_later_append_is_an_error() {
    use duckspout_staging::arrow::array::Int64Array;
    use duckspout_staging::arrow::datatypes::{DataType, Field, Schema};
    use duckspout_staging::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, p1) = ids("t1.0");
    let w0 = WindowId(0);

    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &p1, w0, &log_batch(1, 0, 0))
        .expect("append");
    txn.commit().expect("commit");

    let other = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1i64]))],
    )
    .expect("batch");
    let mut txn = engine.begin().expect("begin");
    assert!(
        txn.append(&dataset, &p1, w0, &other).is_err(),
        "mismatched payload schema must not land"
    );
}

/// The open path fails closed when the storage port refuses the directory
/// fsync — would catch an engine that reports itself open with the hot
/// files' names not yet durable (ADR-0003).
#[test]
fn open_fails_closed_when_directory_fsync_is_refused() {
    struct RefusingStorage(FsStorage);
    impl Storage for RefusingStorage {
        fn put(
            &self,
            path: StoragePath,
            data: bytes::Bytes,
        ) -> BoxFuture<'_, Result<(), StorageError>> {
            self.0.put(path, data)
        }
        fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<bytes::Bytes, StorageError>> {
            self.0.get(path)
        }
        fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
            self.0.delete(path)
        }
        fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
            self.0.fsync_file(path)
        }
        fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
            Box::pin(async move { Err(StorageError::FsyncFailed(dir)) })
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let result = StagingEngine::open(
        duckspout_staging::StagingConfig {
            hot_dir: dir.path().to_path_buf(),
            origin: NodeId::new("node-a/1"),
        },
        RefusingStorage(FsStorage::new(dir.path())),
    );
    assert!(
        matches!(
            result,
            Err(StagingError::Storage(StorageError::FsyncFailed(_)))
        ),
        "open must fail closed on a refused directory fsync"
    );
}

/// Pins the open discipline's mechanism: the epoch bump forces the WAL file
/// into existence before the directory fsync (would catch a `DuckDB` bump
/// that changes lazy-WAL-creation semantics out from under the fsync).
#[test]
fn wal_file_exists_after_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    assert!(
        engine.wal_size().is_some(),
        "hot.db.wal must exist once open returns"
    );
}
