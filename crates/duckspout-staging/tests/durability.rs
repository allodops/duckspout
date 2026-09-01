//! Commit durability across `kill -9` (§4.2, ADR-0003): a child process
//! stages batches through the real engine, is `SIGKILL`ed with a transaction
//! deliberately left open, and the parent verifies recovery — committed
//! rows survive exactly, uncommitted work vanishes, and the dense seq
//! bookkeeping continues where the applied-watermark rows say it should.
//!
//! The child is this same test binary re-invoked with the libtest filter
//! for [`child_writer_process`] (ignored, so ordinary runs skip it).

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use duckspout_types::{DatasetId, PartitionId, WindowId};

use common::{log_batch, open_engine};

const CHILD_DIR_ENV: &str = "DUCKSPOUT_STAGING_KILL9_DIR";
const BATCHES: usize = 5;
const ROWS_PER_BATCH: usize = 100;

fn ids() -> (DatasetId, PartitionId) {
    (DatasetId::new("logs"), PartitionId::new("t1.0"))
}

/// The child body: commit [`BATCHES`] batches (announcing each), then leave
/// a sixth transaction open with rows appended but never committed, and
/// wait to be killed.
#[test]
#[ignore = "child half of committed_batches_survive_kill_nine; runs only re-invoked"]
fn child_writer_process() {
    let Ok(dir) = std::env::var(CHILD_DIR_ENV) else {
        panic!("{CHILD_DIR_ENV} not set; this test only runs as a re-invoked child");
    };
    let engine = open_engine(std::path::Path::new(&dir), "node-a/1");
    let (dataset, partition) = ids();
    for batch in 0..BATCHES {
        let mut txn = engine.begin().expect("begin");
        txn.append(
            &dataset,
            &partition,
            WindowId(0),
            &log_batch(
                ROWS_PER_BATCH,
                i64::try_from(batch * ROWS_PER_BATCH).expect("bounded"),
                0,
            ),
        )
        .expect("append");
        let coverage = txn.commit().expect("commit");
        println!("committed={}", coverage[0].range.last_seq);
        // Stdout is a pipe here: flush, or the parent never sees the line.
        std::io::Write::flush(&mut std::io::stdout()).expect("flush");
    }
    let mut txn = engine.begin().expect("begin");
    txn.append(
        &dataset,
        &partition,
        WindowId(0),
        &log_batch(ROWS_PER_BATCH, 0, 0),
    )
    .expect("append uncommitted");
    println!("uncommitted-open");
    std::io::Write::flush(&mut std::io::stdout()).expect("flush");
    // Hold the transaction open until the parent SIGKILLs this process. The
    // sleep bound only matters if the kill never arrives (test bug).
    std::thread::sleep(std::time::Duration::from_secs(300));
    drop(txn);
}

/// Would catch: an engine whose "commit" is not actually durable at return
/// (rows lost on SIGKILL), a WAL replay that resurrects uncommitted
/// appends, or applied-watermark bookkeeping that desyncs from the data on
/// crash recovery.
#[test]
fn committed_batches_survive_kill_nine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let exe = std::env::current_exe().expect("current test binary");
    let mut child = Command::new(exe)
        .args([
            "child_writer_process",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_DIR_ENV, dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child writer");

    let stdout = child.stdout.take().expect("child stdout");
    let mut committed = 0u64;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("read child line");
        if let Some(seq) = line.strip_prefix("committed=") {
            committed = seq.parse().expect("committed seq");
        } else if line == "uncommitted-open" {
            break;
        }
    }
    // SIGKILL with five commits durable and a sixth transaction open.
    child.kill().expect("kill -9 child");
    let _ = child.wait();
    assert_eq!(committed, (BATCHES * ROWS_PER_BATCH) as u64);

    // Recovery: reopen the same hot database (WAL replay is DuckDB's own).
    let engine = open_engine(dir.path(), "node-a/1");
    let (dataset, partition) = ids();
    let reader = engine.reader().expect("reader");
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .expect("count after recovery"),
        committed,
        "exactly the committed rows survive; the open transaction vanishes"
    );
    assert_eq!(
        engine.applied_seq(&partition).expect("applied"),
        Some(committed),
        "the applied-watermark row rode the same transactions as the rows"
    );

    // The dense sequence continues from the recovered watermark (§4.2.4).
    let mut txn = engine.begin().expect("begin");
    txn.append(&dataset, &partition, WindowId(1), &log_batch(10, 0, 0))
        .expect("append");
    let coverage = txn.commit().expect("commit");
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (committed + 1, committed + 10),
        "no gap and no reuse after crash recovery"
    );
}
