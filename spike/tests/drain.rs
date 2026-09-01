//! Atomicity tests for the drain leg (issue #25): the {add files +
//! watermark} pair commits or vanishes AS A UNIT. These are the
//! load-bearing tests of the seam — commit-then-verify-both,
//! abort-then-verify-neither, crash-then-verify-neither.
//!
//! First run needs network once: `INSTALL ducklake` fetches the extension
//! into ~/.duckdb, cached thereafter.

use std::path::{Path, PathBuf};

use spike::drain::{CommitRequest, DrainCore};
use spike::ingest::{IngestCore, LogRow};

const HOT_TABLE: &str = "hot_w0";
const ROWS: i64 = 500;

/// Build a hot db with one closed micro-window of ROWS rows, then close it
/// (the drain opens its own connection; DuckDB is single-writer per file).
fn build_hot(db: &Path) {
    let mut core = IngestCore::open(db).unwrap();
    core.create_window(HOT_TABLE).unwrap();
    let rows: Vec<_> = (0..ROWS).map(LogRow::synthetic).collect();
    core.insert_batch(HOT_TABLE, &rows).unwrap();
}

/// Seal the hot window into the lake's data dir (product shape: the part is
/// PUT to cold storage first; LakeCommit only registers it).
fn seal(core: &DrainCore, lake_dir: &Path) -> CommitRequest {
    let part = lake_dir.join("data").join("w0-part0.parquet");
    std::fs::create_dir_all(part.parent().unwrap()).unwrap();
    let stats = core.seal_part(HOT_TABLE, &part).unwrap();
    assert_eq!(stats.rows, ROWS);
    CommitRequest {
        partition: "tenant-a/logs/p0".to_string(),
        window_id: 0,
        part,
        complete_through_micros: 1_756_600_000_000_000 + ROWS,
        rows: ROWS,
    }
}

fn paths(dir: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    (dir.path().join("hot.db"), dir.path().join("lake"))
}

/// Commit → BOTH sides visible: rows queryable through the lake, the
/// watermark row present, and the registered data file is the very Parquet
/// file the seal produced (zero-copy add, §6.1) — including from a fresh
/// process-shaped reopen. Would catch a commit that registers files without
/// the watermark (or vice versa), or one that rewrites data instead of
/// adding the sealed file.
#[test]
fn commit_then_both_visible() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);

    let req = {
        let core = DrainCore::open(&hot, &lake).unwrap();
        let req = seal(&core, &lake);
        core.lake_commit(&req).unwrap();
        assert_eq!(core.lake_row_count().unwrap(), ROWS);
        let wm = core.read_watermark(&req.partition, 0).unwrap().unwrap();
        assert_eq!(wm.complete_through_micros, req.complete_through_micros);
        assert_eq!(wm.rows, ROWS);
        assert_eq!(wm.part_name, "w0-part0.parquet");
        req
    }; // drop → clean close

    // Fresh attach: the commit is durable in the catalog, and the
    // registered file is our sealed part, not a rewrite.
    let core = DrainCore::open(&hot, &lake).unwrap();
    assert_eq!(core.lake_row_count().unwrap(), ROWS);
    assert!(core.read_watermark(&req.partition, 0).unwrap().is_some());
    let files = core.registered_files().unwrap();
    assert_eq!(files.len(), 1);
    assert!(
        files[0].ends_with("w0-part0.parquet"),
        "registered file {} is not the sealed part",
        files[0]
    );
}

/// Abort mid-commit (explicit ROLLBACK after BOTH writes) → NEITHER side
/// visible: no rows, no file registration, no watermark advance — the
/// atomic-unit half of `WatermarkHonesty`. Would catch add_data_files
/// escaping the transaction (e.g. autocommitting) or a watermark write in a
/// separate atomicity domain.
#[test]
fn abort_then_neither_visible() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);

    let req = {
        let core = DrainCore::open(&hot, &lake).unwrap();
        let req = seal(&core, &lake);
        core.begin().unwrap();
        core.add_files_and_watermark(&req).unwrap();
        core.rollback().unwrap();
        assert_eq!(core.lake_row_count().unwrap(), 0);
        assert!(core.read_watermark(&req.partition, 0).unwrap().is_none());
        assert!(core.registered_files().unwrap().is_empty());
        req
    };

    // And from a fresh attach — nothing leaked into the catalog.
    let core = DrainCore::open(&hot, &lake).unwrap();
    assert_eq!(core.lake_row_count().unwrap(), 0);
    assert!(core.read_watermark(&req.partition, 0).unwrap().is_none());
    assert!(core.registered_files().unwrap().is_empty());
}

/// Crash mid-commit (connection dropped after BOTH writes, before COMMIT)
/// → NEITHER side visible on reopen. The crash half of the crash-or-abort
/// obligation: an in-flight LakeCommit that dies leaves the catalog exactly
/// as it was.
#[test]
fn crash_mid_commit_then_neither_visible() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);

    let req = {
        let core = DrainCore::open(&hot, &lake).unwrap();
        let req = seal(&core, &lake);
        core.begin().unwrap();
        core.add_files_and_watermark(&req).unwrap();
        req
        // core dropped here with the transaction open — simulated crash.
    };

    let core = DrainCore::open(&hot, &lake).unwrap();
    assert_eq!(core.lake_row_count().unwrap(), 0);
    assert!(core.read_watermark(&req.partition, 0).unwrap().is_none());
    assert!(core.registered_files().unwrap().is_empty());
}

/// A second commit of the same sealed part registers the file AGAIN —
/// DuckLake itself does not fence duplicate registration, and DuckLake
/// tables take no UNIQUE constraint, so the §6.6 SingleDrainCommit guard
/// cannot live in the lake: it belongs to the catalog database (Postgres
/// UNIQUE in production) or above the port. This test pins that finding.
#[test]
fn duplicate_registration_is_not_fenced_by_ducklake() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, lake) = paths(&dir);
    build_hot(&hot);

    let core = DrainCore::open(&hot, &lake).unwrap();
    let req = seal(&core, &lake);
    core.lake_commit(&req).unwrap();
    // Same window, same part, committed again: DuckLake happily double-
    // registers — rows double, two file entries for one physical file.
    core.lake_commit(&req).unwrap();
    assert_eq!(core.lake_row_count().unwrap(), 2 * ROWS);
    assert_eq!(core.registered_files().unwrap().len(), 2);
}
