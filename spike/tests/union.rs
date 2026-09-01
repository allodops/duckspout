//! Hot∪cold union tests (§7.5/§12.1, issue #27): the CONVERGENCE leg. One
//! SQL query unions the DuckLake-attached cold table and the hot windows,
//! bounded by `complete_through`, with the watermark visible in the output.
//!
//! The physical setup makes the seam sharp: drained rows stay in the hot
//! window tables (no truncation in the spike — and in the product, not
//! until HotTruncate), so every drained row exists on BOTH sides and only
//! the watermark bound prevents double-counting.
//!
//! First run needs network once: `INSTALL ducklake` fetches the extension
//! into ~/.duckdb, cached thereafter.

use std::path::{Path, PathBuf};
use std::time::Instant;

use spike::drain::{CommitRequest, DrainCore};
use spike::ingest::{IngestCore, LogRow};
use spike::union_query::{audit, pinned_union_sql, read_complete_through, split_bounds_union_sql};

/// Two micro-windows: w0 = seqs [0, W0_ROWS), w1 = [W0_ROWS, TOTAL).
/// Synthetic ts = BASE + seq, so timestamps straddle the drain boundary
/// deliberately: the last w0 row sits exactly AT `complete_through`.
const TOTAL: i64 = 1_000;
const W0_ROWS: i64 = 600;
const BASE: i64 = 1_756_600_000_000_000; // LogRow::synthetic ts of seq 0
/// complete_through after draining w0 / after draining w1.
const CT_W0: i64 = BASE + W0_ROWS - 1;
const CT_W1: i64 = BASE + TOTAL - 1;

/// The hot branch's FROM-source: all not-yet-truncated windows.
const HOT: &str = "(SELECT * FROM hot_w0 UNION ALL SELECT * FROM hot_w1)";

fn build_hot(db: &Path) {
    let mut core = IngestCore::open(db).unwrap();
    core.create_window("hot_w0").unwrap();
    core.create_window("hot_w1").unwrap();
    let w0: Vec<_> = (0..W0_ROWS).map(LogRow::synthetic).collect();
    let w1: Vec<_> = (W0_ROWS..TOTAL).map(LogRow::synthetic).collect();
    core.insert_batch("hot_w0", &w0).unwrap();
    core.insert_batch("hot_w1", &w1).unwrap();
}

/// Seal `table` into the lake and run the #25 atomic {add files + watermark}
/// commit, advancing `complete_through` to `ct_micros`.
fn drain_window(core: &DrainCore, lake: &Path, table: &str, window_id: i64, ct_micros: i64) {
    let part = lake
        .join("data")
        .join(format!("w{window_id}-part0.parquet"));
    std::fs::create_dir_all(part.parent().unwrap()).unwrap();
    let stats = core.seal_part(table, &part).unwrap();
    core.lake_commit(&CommitRequest {
        partition: "tenant-a/logs/p0".to_string(),
        window_id,
        part,
        complete_through_micros: ct_micros,
        rows: stats.rows,
    })
    .unwrap();
}

fn paths(dir: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    (dir.path().join("hot.db"), dir.path().join("lake"))
}

/// The §12.1 finale: ingest → drain w0 → ONE pinned union statement tiles
/// exactly (no row lost, none double-counted, boundary rows land on the
/// right sides) and `complete_through` is SELECTable alongside every row.
/// Would catch an off-by-one bound (≤/> swapped or both inclusive), a
/// union that drops the watermark column, or branches reading different
/// watermark values within one statement.
#[test]
fn union_tiles_exactly_with_watermark_visible() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);

    let core = DrainCore::open(&hot, &lake).unwrap();
    drain_window(&core, &lake, "hot_w0", 0, CT_W0);
    assert_eq!(read_complete_through(core.conn()).unwrap(), Some(CT_W0));

    let sql = pinned_union_sql(HOT);
    let t0 = Instant::now();
    let a = audit(core.conn(), &sql).unwrap();
    let audit_elapsed = t0.elapsed();

    // Exact tiling: w0's 600 rows are in the lake AND still in hot_w0, yet
    // each identity appears exactly once.
    assert_eq!(a.total, TOTAL, "row lost or double-counted: {a:?}");
    assert_eq!(a.distinct_rows, TOTAL, "duplicate identities: {a:?}");
    assert_eq!(a.cold_rows, W0_ROWS);
    assert_eq!(a.hot_rows, TOTAL - W0_ROWS);
    // The visible watermark: one value, and it is the drained boundary.
    assert_eq!(a.ct_values, 1);
    assert_eq!(a.ct_max_micros, Some(CT_W0));

    // Boundary rows straddle deliberately: the row AT complete_through is
    // cold (≤), the next one is hot (>).
    let branches: Vec<String> = {
        let mut stmt = core
            .conn()
            .prepare(&format!(
                "SELECT branch FROM ({sql}) u WHERE seq IN (?, ?) ORDER BY seq"
            ))
            .unwrap();
        stmt.query_map(duckdb::params![W0_ROWS - 1, W0_ROWS], |r| {
            r.get::<_, String>(0)
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    };
    assert_eq!(branches, ["cold", "hot"]);

    // Full materialization latency ballpark (visible with --nocapture).
    let t1 = Instant::now();
    let mut stmt = core.conn().prepare(&sql).unwrap();
    let rows: usize = stmt.query_arrow([]).unwrap().map(|b| b.num_rows()).sum();
    assert_eq!(rows, TOTAL as usize);
    eprintln!(
        "union ballpark ({TOTAL} rows): audit {audit_elapsed:.1?}, full materialize {:.1?}",
        t1.elapsed()
    );
}

/// THE SEAM (§7.5's "pinned to its bind-time watermark snapshot"): a reader
/// whose two branch bounds come from watermark reads at DIFFERENT times,
/// with a LakeCommit landing in between, double-counts exactly the rows the
/// commit moved — and the same data under any SINGLE consistent bound tiles
/// exactly. This is the exhibit for what bind-time pinning must guarantee:
/// one watermark read per query, both branches bounded by it.
#[test]
fn split_bounds_across_a_commit_double_count() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);
    let core = DrainCore::open(&hot, &lake).unwrap();
    drain_window(&core, &lake, "hot_w0", 0, CT_W0);

    // Reader reads the watermark for its HOT bound... (t1)
    let hot_bound = read_complete_through(core.conn()).unwrap().unwrap();
    // ...a drain commits w1 in between... (the race)
    drain_window(&core, &lake, "hot_w1", 1, CT_W1);
    // ...and the reader reads again for its COLD bound. (t2)
    let cold_bound = read_complete_through(core.conn()).unwrap().unwrap();
    assert_ne!(hot_bound, cold_bound, "setup: the commit must land between");

    // The torn read: w1's rows are ≤ cold_bound (in the lake) AND
    // > hot_bound (still hot) — counted twice, precisely all 400 of them.
    let torn = audit(
        core.conn(),
        &split_bounds_union_sql(HOT, hot_bound, cold_bound),
    )
    .unwrap();
    assert_eq!(
        torn.total,
        TOTAL + (TOTAL - W0_ROWS),
        "expected the double count: {torn:?}"
    );
    assert_eq!(torn.distinct_rows, TOTAL);
    assert_eq!(torn.cold_rows, TOTAL);
    assert_eq!(torn.hot_rows, TOTAL - W0_ROWS);

    // ANY single consistent bound tiles exactly — the stale one and the
    // fresh one alike. Pinning is about consistency, not freshness.
    for bound in [hot_bound, cold_bound] {
        let a = audit(core.conn(), &split_bounds_union_sql(HOT, bound, bound)).unwrap();
        assert_eq!(
            (a.total, a.distinct_rows),
            (TOTAL, TOTAL),
            "bound {bound}: {a:?}"
        );
    }

    // And the pinned single-statement shape, post-commit: exact.
    let pinned = audit(core.conn(), &pinned_union_sql(HOT)).unwrap();
    assert_eq!(pinned.total, TOTAL);
    assert_eq!(pinned.cold_rows, TOTAL, "everything drained → all cold");
    assert_eq!(pinned.ct_max_micros, Some(CT_W1));
}

/// What DuckDB itself contributes to pinning: a reader connection holding
/// an open transaction keeps ONE watermark-and-data snapshot across a
/// concurrent LakeCommit from the writer connection — data read and
/// coverage claimed stay evaluated against the same instant (§7.6's
/// per-transaction pinning), then the next transaction sees the new world.
/// Would catch the attached DuckLake catalog NOT participating in the
/// reader's snapshot (watermark advancing mid-transaction).
#[test]
fn reader_transaction_pins_watermark_and_data_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);
    let core = DrainCore::open(&hot, &lake).unwrap();
    drain_window(&core, &lake, "hot_w0", 0, CT_W0);

    let reader = core.reader().unwrap();
    reader.execute_batch("BEGIN TRANSACTION").unwrap();
    assert_eq!(read_complete_through(&reader).unwrap(), Some(CT_W0));
    let before = audit(&reader, &pinned_union_sql(HOT)).unwrap();
    assert_eq!((before.total, before.hot_rows), (TOTAL, TOTAL - W0_ROWS));

    // Writer commits w1 while the reader's transaction is open.
    drain_window(&core, &lake, "hot_w1", 1, CT_W1);
    assert_eq!(
        read_complete_through(core.conn()).unwrap(),
        Some(CT_W1),
        "writer sees its own commit"
    );

    // Same transaction → same snapshot: watermark unmoved, union unchanged,
    // still exact.
    assert_eq!(
        read_complete_through(&reader).unwrap(),
        Some(CT_W0),
        "watermark advanced mid-transaction — snapshot not pinned"
    );
    let during = audit(&reader, &pinned_union_sql(HOT)).unwrap();
    assert_eq!(during, before, "union changed mid-transaction");

    // Next transaction → the new snapshot, still tiling exactly.
    reader.execute_batch("COMMIT").unwrap();
    assert_eq!(read_complete_through(&reader).unwrap(), Some(CT_W1));
    let after = audit(&reader, &pinned_union_sql(HOT)).unwrap();
    assert_eq!(
        (after.total, after.cold_rows, after.hot_rows),
        (TOTAL, TOTAL, 0)
    );
}
