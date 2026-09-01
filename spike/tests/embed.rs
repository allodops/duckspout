//! Smoke tests for the spike embed: the ingest core works — transactional
//! batch inserts land, and committed data is visible across a clean
//! close/reopen. Durability re-verification (kill -9, fsync behavior) is
//! deliberately out of scope: owner ruling 2026-09-01 — DuckDB's documented
//! WAL/checkpoint behavior is trusted.

use std::path::Path;

fn count(db: &Path) -> i64 {
    let conn = duckdb::Connection::open(db).expect("open");
    conn.query_row("SELECT count(*) FROM hot_w0", [], |r| r.get(0))
        .expect("count")
}

/// Batches inserted through the core's transaction-per-batch path are all
/// present, with exact counts — would catch a broken appender/commit path.
#[test]
fn batch_inserts_land_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hot.db");
    let mut core = spike::ingest::IngestCore::open(&db).unwrap();
    core.create_window("hot_w0").unwrap();
    let mut seq = 0i64;
    for batch in [1usize, 100, 1000] {
        let rows: Vec<_> = (0..batch)
            .map(|i| spike::ingest::LogRow::synthetic(seq + i as i64))
            .collect();
        seq += batch as i64;
        core.insert_batch("hot_w0", &rows).unwrap();
    }
    assert_eq!(core.count("hot_w0").unwrap(), 1101);
}

/// Committed rows are visible from a fresh connection after a clean close —
/// would catch a core that only ever wrote to an in-memory or temp database.
#[test]
fn committed_rows_visible_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hot.db");
    {
        let mut core = spike::ingest::IngestCore::open(&db).unwrap();
        core.create_window("hot_w0").unwrap();
        let rows: Vec<_> = (0..250i64).map(spike::ingest::LogRow::synthetic).collect();
        core.insert_batch("hot_w0", &rows).unwrap();
    } // clean close
    assert_eq!(count(&db), 250);
}
