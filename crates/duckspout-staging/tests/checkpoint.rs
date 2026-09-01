//! The #109 checkpoint scheme, behaviorally: the engine's write connection
//! never auto-checkpoints inside a commit (the WAL grows straight through
//! `DuckDB`'s default 16 MiB trigger), and the explicit
//! [`StagingEngine::checkpoint`] — the drain-window call — truncates the
//! WAL, recreates it, and leaves the data intact. A raw connection with
//! default settings is the control: it proves the default *would* have
//! checkpointed inside the same write pattern, so the deferral assertion
//! stays meaningful if `DuckDB`'s semantics ever move.
//!
//! (Latency evidence — the outlier table in the PR — is the ignored
//! `commit_latency_distribution` harness in `tests/latency.rs`; this file
//! pins behavior, which is what CI can assert deterministically.)

mod common;

use duckspout_types::{DatasetId, PartitionId, WindowId};

use common::{log_batch, open_engine};

/// `DuckDB`'s documented default `checkpoint_threshold` (16 MiB) — the size
/// the engine's WAL must sail past to prove automatic checkpointing is
/// really deferred.
const DEFAULT_CHECKPOINT_THRESHOLD: u64 = 16 * 1024 * 1024;

const ROWS_PER_BATCH: usize = 10_000;
const BODY_PAD: usize = 160;

/// Would catch: a lost or ineffective `checkpoint_threshold` setting (the
/// ack path would re-inherit 219–620 ms checkpoint pauses, #109), a
/// `checkpoint()` that does not truncate the WAL, one that loses rows, or a
/// post-checkpoint WAL left without a durable-name discipline (the file
/// must be recreated before `checkpoint()` returns).
#[test]
fn auto_checkpoint_is_deferred_and_manual_checkpoint_truncates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let dataset = DatasetId::new("logs");
    let partition = PartitionId::new("t1.0");
    let w0 = WindowId(0);
    let reader = engine.reader().expect("reader");

    let mut staged_rows = 0u64;
    let mut last_wal = engine.wal_size().expect("wal exists after open");
    let mut batches = 0i64;
    while last_wal <= DEFAULT_CHECKPOINT_THRESHOLD + DEFAULT_CHECKPOINT_THRESHOLD / 4 {
        let mut txn = engine.begin().expect("begin");
        txn.append(
            &dataset,
            &partition,
            w0,
            &log_batch(ROWS_PER_BATCH, batches * 1_000_000, BODY_PAD),
        )
        .expect("append");
        txn.commit().expect("commit");
        staged_rows += ROWS_PER_BATCH as u64;
        batches += 1;
        let wal = engine.wal_size().expect("wal present");
        assert!(
            wal >= last_wal,
            "WAL shrank mid-ingest ({last_wal} -> {wal} bytes): an automatic \
             checkpoint fired on the ack path despite the deferral (#109)"
        );
        last_wal = wal;
        assert!(batches < 200, "WAL never grew past the default threshold");
    }

    // The drain-window pause: explicit checkpoint, with a reader open.
    engine.checkpoint().expect("checkpoint");
    let wal_after = engine.wal_size().expect("WAL recreated after checkpoint");
    assert!(
        wal_after < DEFAULT_CHECKPOINT_THRESHOLD / 4,
        "checkpoint must truncate the WAL (got {wal_after} bytes)"
    );
    assert_eq!(
        reader
            .count_window(&dataset, &partition, w0)
            .expect("count after checkpoint"),
        staged_rows
    );

    // Commits keep working after the checkpoint, and the store reopens with
    // everything intact.
    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &partition, w0, &log_batch(10, 0, 0))
        .expect("append");
    txn.commit().expect("commit");
    drop(reader);
    drop(engine);

    let engine = open_engine(dir.path(), "node-a/1");
    let reader = engine.reader().expect("reader");
    assert_eq!(
        reader
            .count_window(&dataset, &partition, w0)
            .expect("count"),
        staged_rows + 10
    );
}

/// The control leg: the same write pattern on a raw connection with default
/// settings *does* auto-checkpoint (its WAL shrinks mid-ingest). Would
/// catch a `DuckDB` bump that changed the default trigger semantics — which
/// would silently hollow out the deferral test above.
#[test]
fn default_connection_does_auto_checkpoint_under_the_same_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("control.db");
    let conn = duckdb::Connection::open(&db).expect("open");
    conn.execute_batch(
        "CREATE TABLE hot_w0 (ts TIMESTAMP NOT NULL, severity INTEGER, body VARCHAR)",
    )
    .expect("ddl");

    let wal_size = || {
        let mut wal = db.clone().into_os_string();
        wal.push(".wal");
        std::fs::metadata(std::path::PathBuf::from(wal))
            .ok()
            .map_or(0, |m| m.len())
    };

    let mut shrank = false;
    let mut last_wal = 0u64;
    for batch in 0..200i64 {
        conn.execute_batch("BEGIN TRANSACTION").expect("begin");
        let mut appender = conn.appender("hot_w0").expect("appender");
        appender
            .append_record_batch(log_batch(ROWS_PER_BATCH, batch * 1_000_000, BODY_PAD))
            .expect("append");
        appender.flush().expect("flush");
        drop(appender);
        conn.execute_batch("COMMIT").expect("commit");
        let wal = wal_size();
        if wal < last_wal {
            shrank = true;
            break;
        }
        last_wal = wal;
    }
    assert!(
        shrank,
        "the default configuration never auto-checkpointed; the deferral \
         test's premise (and #109's cost model) needs re-examination"
    );
}
