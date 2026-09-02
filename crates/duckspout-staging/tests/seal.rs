//! The seal-side read surface over a real hot database (§6.2): the sorted
//! deduplicating `COPY`, the manifest bookkeeping, and the close-gated
//! enumeration.

mod common;

use std::sync::Arc;

use common::FsStorage;
use duckspout_staging::{EngineSealSurface, StagingConfig, StagingEngine};
use duckspout_types::{
    DatasetId, DropOutcome, NodeId, OriginSeqRange, PartitionId, SealError, SealRequest,
    SealSurface, WindowId,
};

fn ds() -> DatasetId {
    DatasetId::new("logs")
}

fn p() -> PartitionId {
    PartitionId::new("tenant1.0")
}

fn open_surface(dir: &std::path::Path) -> EngineSealSurface<FsStorage> {
    let engine = StagingEngine::open(
        StagingConfig {
            hot_dir: dir.to_path_buf(),
            origin: NodeId::new("n1"),
        },
        FsStorage::new(dir),
    )
    .expect("engine opens");
    EngineSealSurface::new(Arc::new(engine))
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    use std::task::{Context, Poll, Waker};
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn request(dedup_key: Option<Vec<String>>) -> SealRequest {
    SealRequest {
        dataset: ds(),
        partition: p(),
        window: WindowId(0),
        order_by: vec!["ts".into()],
        event_time_column: "ts".into(),
        dedup_key,
    }
}

/// Stages `bodies` as one committed batch into window 0 (ts spaced 1 s
/// apart starting at `t0_micros`).
fn stage(surface: &EngineSealSurface<FsStorage>, t0_micros: i64, bodies: &[&str]) {
    let engine = surface.engine();
    let batch = common::bodies_batch(t0_micros, bodies);
    let mut txn = engine.begin().expect("txn begins");
    txn.append(&ds(), &p(), WindowId(0), &batch)
        .expect("append");
    txn.commit().expect("commit");
}

#[test]
fn drainable_windows_are_close_gated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let surface = open_surface(dir.path());
    stage(&surface, 1_000_000, &["a"]);

    assert!(
        block_on(surface.drainable_windows())
            .expect("enumerates")
            .is_empty(),
        "an un-noted window is never offered"
    );

    surface.note_closed(&ds(), &p(), WindowId(0), 60_000);
    surface.note_closed(&ds(), &p(), WindowId(0), 99_000);
    let offered = block_on(surface.drainable_windows()).expect("enumerates");
    assert_eq!(offered.len(), 1);
    assert_eq!(offered[0].window, WindowId(0));
    assert_eq!(offered[0].closed_at_ms, 60_000, "first noted close wins");

    // A close noted for a window the registry does not hold is not offered.
    surface.note_closed(&ds(), &p(), WindowId(9), 60_000);
    assert_eq!(
        block_on(surface.drainable_windows())
            .expect("enumerates")
            .len(),
        1
    );
}

#[test]
fn seal_produces_a_sorted_part_with_manifest_bookkeeping() {
    let dir = tempfile::tempdir().expect("tempdir");
    let surface = open_surface(dir.path());
    // Two commits into the same window: seqs 1..=3 then 4..=5, one origin.
    stage(&surface, 5_000_000, &["e", "d", "c"]);
    stage(&surface, 2_000_000, &["b", "a"]);

    let sealed = block_on(surface.seal_window(request(None))).expect("seals");
    assert_eq!(sealed.rows, 5);
    assert_eq!(sealed.dedup_removed, 0);
    assert_eq!(sealed.event_time_min_ms, 2_000, "min over both commits");
    assert_eq!(sealed.event_time_max_ms, 7_000, "max over both commits");
    assert_eq!(
        sealed.origin_coverage,
        vec![OriginSeqRange {
            origin: NodeId::new("n1"),
            first_seq: 1,
            last_seq: 5,
        }],
        "contiguous seqs collapse into one run"
    );

    // The part exists at the returned scratch path, is readable Parquet,
    // and is sorted by the requested order.
    let absolute = dir.path().join(sealed.path.as_str());
    assert!(absolute.exists(), "sealed parquet written");
    let reader = surface.engine().reader().expect("reader");
    let (_, batches) = reader
        .query_arrow(&format!(
            "SELECT body FROM '{}' ORDER BY ALL",
            absolute.display()
        ))
        .expect("parquet readable");
    let rows: usize = batches
        .iter()
        .map(duckspout_staging::arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 5);
    let (_, ordered) = reader
        .query_arrow(&format!(
            "SELECT (SELECT list(ts ORDER BY file_row_number)
                     FROM read_parquet('{0}', file_row_number = true)) =
                    (SELECT list(ts ORDER BY ts) FROM '{0}')",
            absolute.display()
        ))
        .expect("order check runs");
    let flag = ordered[0]
        .column(0)
        .as_any()
        .downcast_ref::<duckspout_staging::arrow::array::BooleanArray>()
        .expect("boolean")
        .value(0);
    assert!(flag, "part rows are in the requested sort order (§6.2)");
}

#[test]
fn seal_dedups_keeping_the_smallest_origin_seq_winner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let surface = open_surface(dir.path());
    // "a" appears three times (seqs 1, 3, 4), "b" once (seq 2).
    stage(&surface, 1_000_000, &["a", "b", "a", "a"]);

    let sealed = block_on(surface.seal_window(request(Some(vec!["body".into()])))).expect("seals");
    assert_eq!(sealed.rows, 2, "one row per distinct key");
    assert_eq!(
        sealed.dedup_removed, 2,
        "the removed duplicates are counted"
    );
    assert_eq!(
        sealed.origin_coverage,
        vec![OriginSeqRange {
            origin: NodeId::new("n1"),
            first_seq: 1,
            last_seq: 4,
        }],
        "coverage is pre-dedup: removed rows stay covered"
    );

    let absolute = dir.path().join(sealed.path.as_str());
    let reader = surface.engine().reader().expect("reader");
    let (_, batches) = reader
        .query_arrow(&format!(
            "SELECT body, seq FROM '{}' ORDER BY body",
            absolute.display()
        ))
        .expect("parquet readable");
    let batch = &batches[0];
    let seqs = batch
        .column(1)
        .as_any()
        .downcast_ref::<duckspout_staging::arrow::array::UInt64Array>()
        .expect("seq column");
    assert_eq!(
        seqs.values(),
        &[1, 2],
        "the deterministic smallest-(origin, seq) winner survives (§6.2)"
    );
}

#[test]
fn empty_window_seals_honestly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let surface = open_surface(dir.path());
    // A committed 0-row append creates the table with no rows.
    let engine = surface.engine();
    let batch = common::bodies_batch(1_000_000, &[]);
    let mut txn = engine.begin().expect("txn begins");
    txn.append(&ds(), &p(), WindowId(0), &batch)
        .expect("append");
    txn.commit().expect("commit");

    let sealed = block_on(surface.seal_window(request(None))).expect("seals");
    assert_eq!(sealed.rows, 0);
    assert_eq!(sealed.dedup_removed, 0);
    assert_eq!((sealed.event_time_min_ms, sealed.event_time_max_ms), (0, 0));
    assert!(sealed.origin_coverage.is_empty());
    assert!(dir.path().join(sealed.path.as_str()).exists());
}

#[test]
fn unknown_window_is_a_typed_error_and_drop_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let surface = open_surface(dir.path());
    let mut unknown = request(None);
    unknown.window = WindowId(7);
    let err = block_on(surface.seal_window(unknown)).expect_err("unknown window");
    assert!(matches!(
        err,
        SealError::UnknownWindow {
            window: WindowId(7),
            ..
        }
    ));

    stage(&surface, 1_000_000, &["a"]);
    surface.note_closed(&ds(), &p(), WindowId(0), 60_000);
    let full = vec![OriginSeqRange {
        origin: NodeId::new("n1"),
        first_seq: 1,
        last_seq: 1,
    }];
    assert_eq!(
        block_on(surface.drop_window(ds(), p(), WindowId(0), full.clone())).expect("drops"),
        DropOutcome::Dropped
    );
    assert_eq!(
        block_on(surface.drop_window(ds(), p(), WindowId(0), full)).expect("second drop resolves"),
        DropOutcome::AlreadyGone,
        "DropWindow is idempotent (§6.9)"
    );
    assert!(
        block_on(surface.drainable_windows())
            .expect("enumerates")
            .is_empty(),
        "a dropped window is no longer offered"
    );
}

#[test]
fn drop_is_coverage_guarded_and_keeps_uncovered_residue() {
    // TN-32 (PR #137): rows outside the committed coverage — e.g. a late
    // arrival landing between the seal COPY and the drop — are durable
    // acked data and must survive the drop as supplement input.
    let dir = tempfile::tempdir().expect("tempdir");
    let surface = open_surface(dir.path());
    stage(&surface, 1_000_000, &["a", "b"]); // seqs 1..=2, "the sealed rows"
    stage(&surface, 9_000_000, &["late"]); // seq 3, "landed after the seal"
    surface.note_closed(&ds(), &p(), WindowId(0), 60_000);

    let sealed_coverage = vec![OriginSeqRange {
        origin: NodeId::new("n1"),
        first_seq: 1,
        last_seq: 2,
    }];
    assert_eq!(
        block_on(surface.drop_window(ds(), p(), WindowId(0), sealed_coverage)).expect("drops"),
        DropOutcome::ResidueKept { rows: 1 },
        "the uncovered late row survives"
    );

    // The window is still enumerable and holds exactly the residue row.
    let offered = block_on(surface.drainable_windows()).expect("enumerates");
    assert_eq!(offered.len(), 1, "a residue window stays offered (§6.6)");
    let reader = surface.engine().reader().expect("reader");
    assert_eq!(
        reader
            .count_window(&ds(), &p(), WindowId(0))
            .expect("counts"),
        1
    );

    // A later (supplement) commit covering the residue completes the drop.
    let residue_coverage = vec![OriginSeqRange {
        origin: NodeId::new("n1"),
        first_seq: 3,
        last_seq: 3,
    }];
    assert_eq!(
        block_on(surface.drop_window(ds(), p(), WindowId(0), residue_coverage)).expect("drops"),
        DropOutcome::Dropped,
        "once everything is covered, the whole-table drop completes"
    );
    assert!(
        block_on(surface.drainable_windows())
            .expect("enumerates")
            .is_empty()
    );
}
