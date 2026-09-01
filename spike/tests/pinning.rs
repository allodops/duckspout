//! Issue #28 — catalog-snapshot pinning under concurrent drain commits.
//!
//! Every contested behavior here was MEASURED before it was asserted; a test
//! failing after a DuckDB/DuckLake upgrade means the observed semantics
//! moved, which is exactly what the spike exists to catch. Client = a
//! second, independent DuckDB instance (and, where it matters, a real second
//! OS process) attached read-only to the same DuckLake catalog the drain
//! writes.

use spike::pinning::{CatalogKind, ClientReader, DrainWriter, Lake, ROWS_PER_WINDOW, watermark_of};

fn sqlite_lake(dir: &tempfile::TempDir) -> Lake {
    Lake::new(dir.path(), CatalogKind::Sqlite).unwrap()
}

/// THE seam test — BEGIN … read … drain-commits … read-again … COMMIT.
///
/// Observed (DuckDB 1.5.5 + ducklake extension): the open client transaction
/// PINS the DuckLake catalog snapshot. Mid-transaction, the table scan, the
/// watermark table, `ducklake_snapshots` and `ducklake_table_info` all keep
/// reporting the snapshot read first — the drain's commit is invisible until
/// the client COMMITs. Because the watermark lives in the same catalog, the
/// pinned snapshot yields a mutually consistent {file set, watermark} pair
/// with no extension work at all. The one exception, asserted below: the raw
/// `__ducklake_metadata_*` passthrough is NOT snapshot-pinned.
#[test]
fn open_transaction_pins_cold_view_under_concurrent_commits() {
    let dir = tempfile::tempdir().unwrap();
    let lake = sqlite_lake(&dir);
    let drain = DrainWriter::open(&lake).unwrap();
    drain.commit_window(0).unwrap();

    let client = ClientReader::open(&lake).unwrap();
    client.begin().unwrap();
    let before = client.observe().unwrap();
    assert_eq!(before.rows, ROWS_PER_WINDOW);
    assert_eq!(before.watermark, watermark_of(0));
    assert_eq!(before.visible_files, 1);

    // The drain commits window 1 while the client transaction is open.
    drain.commit_window(1).unwrap();

    let during = client.observe().unwrap();
    // PINNED: every query-surface observable is unchanged mid-transaction.
    assert_eq!(during.rows, before.rows, "file set moved mid-transaction");
    assert_eq!(during.watermark, before.watermark, "watermark moved");
    assert_eq!(during.snapshot, before.snapshot, "snapshot id moved");
    assert_eq!(during.visible_files, before.visible_files);
    // NOT pinned: the raw metadata-catalog passthrough already shows the new
    // data file. An extension reading coverage through this surface would
    // reintroduce the data-vs-coverage TOCTOU — footgun, measured.
    assert_eq!(during.raw_catalog_files, before.raw_catalog_files + 1);

    client.commit().unwrap();
    let after = client.observe().unwrap();
    // Unpinned at COMMIT: the next read sees window 1, as one new snapshot,
    // with rows and watermark advancing TOGETHER.
    assert_eq!(after.rows, 2 * ROWS_PER_WINDOW);
    assert_eq!(after.watermark, watermark_of(1));
    assert_eq!(after.visible_files, 2);
    assert_eq!(after.snapshot, before.snapshot + 1);
}

/// The same sequence with the drain in a REAL second OS process (the spike
/// binary's `pin-commit`), which is the actual client topology: querying
/// DuckDB and draining daemon never share a process. Also proves a reader
/// transaction does not block the drain's commit (sqlite catalog, WAL mode
/// — with the default rollback journal the drain's COMMIT fails with
/// "database is locked"; measured, and why `Lake` forces WAL).
#[test]
fn pin_holds_across_a_real_process_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let lake = sqlite_lake(&dir);
    DrainWriter::open(&lake).unwrap().commit_window(0).unwrap();

    let client = ClientReader::open(&lake).unwrap();
    client.begin().unwrap();
    let before = client.observe().unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_spike"))
        .args(["pin-commit", dir.path().to_str().unwrap(), "1"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "drain process failed under open reader txn: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let during = client.observe().unwrap();
    assert_eq!(
        (during.rows, during.watermark, during.snapshot),
        (before.rows, before.watermark, before.snapshot)
    );
    client.commit().unwrap();
    let after = client.observe().unwrap();
    assert_eq!(after.rows, 2 * ROWS_PER_WINDOW);
    assert_eq!(after.watermark, watermark_of(1));
}

/// WHEN the pin is taken. Observed: at the transaction's first read of the
/// lake catalog, not at BEGIN — a commit landing between BEGIN and the first
/// read IS visible to the whole transaction. For the extension this is the
/// right shape: §7.6 says pinning happens at bind, and bind is the first
/// catalog read; nothing needs the pin to predate it.
#[test]
fn pin_is_taken_at_first_catalog_read_not_at_begin() {
    let dir = tempfile::tempdir().unwrap();
    let lake = sqlite_lake(&dir);
    let drain = DrainWriter::open(&lake).unwrap();
    drain.commit_window(0).unwrap();

    let client = ClientReader::open(&lake).unwrap();
    client.begin().unwrap();
    // No read yet; the drain commits window 1 after BEGIN.
    drain.commit_window(1).unwrap();
    let first = client.observe().unwrap();
    assert_eq!(first.rows, 2 * ROWS_PER_WINDOW, "pin predated first read");
    assert_eq!(first.watermark, watermark_of(1));
    // …and from that first read on, the transaction is pinned as usual.
    drain.commit_window(2).unwrap();
    let second = client.observe().unwrap();
    assert_eq!(second.rows, first.rows);
    assert_eq!(second.watermark, first.watermark);
    client.commit().unwrap();
}

/// Explicit re-pinning outside any transaction: `AT (VERSION => v)` reads a
/// named catalog snapshot no matter how far the drain has advanced. This is
/// the extension's recovery lever — if a client connection is lost and the
/// hot ticket must be re-validated, the cold branch can be re-pinned to the
/// snapshot the ticket was minted against.
#[test]
fn at_version_re_pins_a_named_snapshot_outside_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let lake = sqlite_lake(&dir);
    let drain = DrainWriter::open(&lake).unwrap();
    drain.commit_window(0).unwrap();

    let client = ClientReader::open(&lake).unwrap();
    let pinned = client.observe().unwrap();
    drain.commit_window(1).unwrap();
    drain.commit_window(2).unwrap();
    // Autocommit reads track the head…
    assert_eq!(client.observe().unwrap().rows, 3 * ROWS_PER_WINDOW);
    // …but the named snapshot still serves exactly window 0's file set.
    assert_eq!(
        client.rows_at_version(pinned.snapshot).unwrap(),
        ROWS_PER_WINDOW
    );
}

/// Run `attempts` second-process read-only attaches while the drain in THIS
/// process commits windows in a loop; returns (ok, failed, first error).
fn attach_under_active_drain(
    lake: Lake,
    kind_arg: &str,
    dir: &std::path::Path,
    attempts: usize,
) -> (usize, usize, String) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let drain = DrainWriter::open(&lake).unwrap();
    drain.commit_window(0).unwrap();
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let s2 = stop.clone();
    let writer = std::thread::spawn(move || {
        let mut id = 1;
        while !s2.load(Ordering::Relaxed) {
            drain.commit_window(id).expect("drain commit under readers");
            id += 1;
        }
    });
    let (mut ok, mut fail, mut first_err) = (0, 0, String::new());
    for _ in 0..attempts {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_spike"))
            .args(["pin-attach", dir.to_str().unwrap(), kind_arg])
            .output()
            .unwrap();
        if out.status.success() {
            ok += 1;
        } else {
            fail += 1;
            if first_err.is_empty() {
                first_err = String::from_utf8_lossy(&out.stderr).into_owned();
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    (ok, fail, first_err)
}

/// Topology guard, measured with real second OS processes. A DuckLake
/// catalog stored in a plain DuckDB FILE is not multi-process: DuckLake
/// opens the metadata connection transiently, so a second process CAN slip
/// an attach in while the drain is idle — but while the drain is actively
/// committing, the attach fails with DuckDB's "Conflicting lock is held"
/// (single-writer file lock). Same-process attaches always succeed (POSIX
/// locks don't conflict within one process), which is exactly why an
/// in-process test would report a false pass. The sqlite catalog in WAL
/// mode — the spike's Postgres stand-in — serves second-process readers
/// even under an active drain (control below).
#[test]
fn duckdb_file_catalog_is_not_multi_process_under_active_drain() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::new(dir.path(), CatalogKind::DuckdbFile).unwrap();
    {
        let drain = DrainWriter::open(&lake).unwrap();
        drain.commit_window(0).unwrap();
        // Same process: misleadingly fine, even with the drain attached.
        ClientReader::open(&lake).unwrap();
    }
    let fdir = tempfile::tempdir().unwrap();
    let flake = Lake::new(fdir.path(), CatalogKind::DuckdbFile).unwrap();
    let (_ok, fail, err) = attach_under_active_drain(flake, "file", fdir.path(), 5);
    assert!(
        fail > 0,
        "no duckdb-file attach conflicts under an active drain — \
         lock semantics changed, re-measure the topology claim"
    );
    assert!(
        err.contains("lock"),
        "expected a file-lock conflict, got: {err}"
    );

    // Control: sqlite catalog (WAL) under the SAME active-drain load.
    let sdir = tempfile::tempdir().unwrap();
    let slake = Lake::new(sdir.path(), CatalogKind::Sqlite).unwrap();
    let (ok, fail, err) = attach_under_active_drain(slake, "sqlite", sdir.path(), 5);
    assert_eq!(
        (ok, fail),
        (5, 0),
        "sqlite-catalog reader failed under active drain: {err}"
    );
}
