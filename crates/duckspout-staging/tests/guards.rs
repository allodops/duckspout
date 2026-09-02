//! Guarded-scan tests (§7.8, the mechanism half in `StagingReader`): the
//! byte budget and deadline trip as typed aborts — never truncation — and
//! a generous guard changes nothing about the result.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::{log_batch, open_engine};
use duckspout_staging::arrow::record_batch::RecordBatch;
use duckspout_staging::{ScanGuards, StagingError};
use duckspout_types::{Clock, DatasetId, PartitionId, WindowId};

/// A clock whose monotonic reading advances by a fixed step on every read —
/// so the per-batch deadline check observes elapsing time deterministically.
struct SteppingClock {
    nanos: AtomicU64,
    step: u64,
}

impl Clock for SteppingClock {
    fn monotonic_nanos(&self) -> u64 {
        self.nanos.fetch_add(self.step, Ordering::SeqCst)
    }

    fn wall_unix_ms(&self) -> i64 {
        0
    }
}

fn staged_engine(
    dir: &std::path::Path,
    rows: usize,
) -> (
    Arc<duckspout_staging::StagingEngine<common::FsStorage>>,
    String,
) {
    let engine = Arc::new(open_engine(dir, "node-a/1"));
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::new("t.0");
    let mut txn = engine.begin().unwrap();
    txn.append(&dataset, &partition, WindowId(0), &log_batch(rows, 0, 32))
        .unwrap();
    txn.commit().unwrap();
    let table = duckspout_staging::naming::window_table_name(&dataset, &partition, WindowId(0));
    (engine, format!("SELECT * FROM {table}"))
}

/// Generous guards change nothing: the guarded scan returns exactly the
/// unguarded scan's rows.
#[test]
fn generous_guards_return_the_full_result() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, sql) = staged_engine(dir.path(), 500);
    let reader = engine.reader().unwrap();
    let clock = SteppingClock {
        nanos: AtomicU64::new(0),
        step: 1,
    };
    let guards = ScanGuards {
        max_bytes: u64::MAX,
        deadline_nanos: u64::MAX,
    };
    let (_, batches) = reader.query_arrow_guarded(&sql, &clock, &guards).unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 500);
}

/// The byte budget trips as its typed abort with nothing returned (§7.8:
/// never truncation) — and the error names the budget in force.
#[test]
fn byte_budget_trips_typed() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, sql) = staged_engine(dir.path(), 500);
    let reader = engine.reader().unwrap();
    let clock = SteppingClock {
        nanos: AtomicU64::new(0),
        step: 1,
    };
    let guards = ScanGuards {
        max_bytes: 1,
        deadline_nanos: u64::MAX,
    };
    let err = reader
        .query_arrow_guarded(&sql, &clock, &guards)
        .unwrap_err();
    match err {
        StagingError::ScanBudgetExceeded {
            scanned_bytes,
            budget_bytes,
        } => {
            assert_eq!(budget_bytes, 1);
            assert!(scanned_bytes > 1);
        }
        other => panic!("expected ScanBudgetExceeded, got {other:?}"),
    }
}

/// The deadline trips as its typed abort once monotonic time crosses the
/// span — driven entirely by the Clock port, no real time involved.
#[test]
fn deadline_trips_typed_on_the_clock_port() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, sql) = staged_engine(dir.path(), 500);
    let reader = engine.reader().unwrap();
    // Every clock read advances 1 s; the deadline is 1 ns — the first
    // per-batch check must trip.
    let clock = SteppingClock {
        nanos: AtomicU64::new(0),
        step: 1_000_000_000,
    };
    let guards = ScanGuards {
        max_bytes: u64::MAX,
        deadline_nanos: 1,
    };
    let err = reader
        .query_arrow_guarded(&sql, &clock, &guards)
        .unwrap_err();
    assert!(
        matches!(err, StagingError::ScanDeadlineExceeded { .. }),
        "expected ScanDeadlineExceeded, got {err:?}"
    );
}
