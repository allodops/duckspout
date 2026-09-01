//! Spike pinning leg (issue #28) — riskiest seam #1: transaction-lifecycle
//! pinning in the extension (§7.6 per-transaction pinning, §10.2 seam 2).
//!
//! The question under test is the **cold half** of that seam: when a client
//! DuckDB ATTACHes the DuckSpout lake catalog read-only and holds an open
//! transaction, what does it see while the drain keeps committing
//! {add files + watermark} snapshots? If DuckLake pins the catalog snapshot
//! for the transaction's lifetime, the extension gets cold-side
//! {file set, watermark} consistency from the engine; if it does not, the
//! extension must pin explicitly (e.g. `AT (VERSION => v)` on every cold
//! branch). Measured, not assumed — the tests assert observed behavior.
//!
//! Topology note (measured in `tests/pinning.rs`): a **duckdb-file** DuckLake
//! catalog is not multi-process. DuckLake opens the metadata connection
//! transiently, so a second OS process can slip an attach in while the drain
//! is idle — but under an active drain the attach fails with DuckDB's
//! "Conflicting lock is held" (and the lock is per-process, so an in-process
//! test reports a false pass). This spike therefore uses DuckLake's
//! **SQLite catalog backend in WAL mode** as the smallest local stand-in for
//! the production Postgres catalog (query.md section 3): multi-process, and
//! readers don't block the drain's commits.
//!
//! Throwaway spike code — instructive, not production (spike/README.md).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;

/// Catalog alias the lake is attached under.
const LAKE: &str = "lake";

/// Rows every spike window carries (each commit registers one part file).
pub const ROWS_PER_WINDOW: i64 = 10;

/// Which database hosts the DuckLake catalog (the metadata store).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CatalogKind {
    /// `ducklake:sqlite:` — WAL mode; the spike's Postgres stand-in.
    Sqlite,
    /// `ducklake:` on a plain DuckDB file — single-process only (measured).
    DuckdbFile,
}

/// One on-disk DuckLake lake: metadata store + data path + sealed parts.
#[derive(Debug, Clone)]
pub struct Lake {
    pub kind: CatalogKind,
    pub meta: PathBuf,
    pub data: PathBuf,
    pub parts: PathBuf,
}

impl Lake {
    pub fn new(dir: &Path, kind: CatalogKind) -> Result<Self> {
        let meta = match kind {
            CatalogKind::Sqlite => dir.join("metadata.sqlite"),
            CatalogKind::DuckdbFile => dir.join("metadata.ducklake"),
        };
        let lake = Self {
            kind,
            meta,
            data: dir.join("data"),
            parts: dir.join("parts"),
        };
        std::fs::create_dir_all(&lake.data)?;
        std::fs::create_dir_all(&lake.parts)?;
        Ok(lake)
    }

    /// The ATTACH statement for this lake. SQLite gets WAL + busy-timeout so
    /// open reader transactions do not block drain commits (measured: with
    /// the default rollback journal, the drain's COMMIT fails with "database
    /// is locked" while any reader transaction is open).
    fn attach_sql(&self, read_only: bool) -> String {
        let ro = if read_only { ", READ_ONLY" } else { "" };
        match self.kind {
            CatalogKind::Sqlite => format!(
                "ATTACH 'ducklake:sqlite:{}' AS {LAKE} \
                 (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000{ro});",
                self.meta.display(),
                self.data.display()
            ),
            CatalogKind::DuckdbFile => format!(
                "ATTACH 'ducklake:{}' AS {LAKE} (DATA_PATH '{}'{ro});",
                self.meta.display(),
                self.data.display()
            ),
        }
    }

    fn open(&self, read_only: bool) -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
            .context("install/load ducklake + sqlite extensions")?;
        conn.execute_batch(&self.attach_sql(read_only))
            .with_context(|| format!("attach lake (read_only={read_only})"))?;
        Ok(conn)
    }
}

/// The drain stand-in: owns a read-write attach and commits
/// {add data file + watermark} snapshots, same shape as issue #25's leg.
pub struct DrainWriter {
    conn: Connection,
    lake: Lake,
}

impl DrainWriter {
    pub fn open(lake: &Lake) -> Result<Self> {
        let conn = lake.open(false)?;
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {LAKE}.logs (
                origin VARCHAR, seq BIGINT, ts TIMESTAMP
             );
             CREATE TABLE IF NOT EXISTS {LAKE}.watermarks (
                window_id BIGINT, complete_through_micros BIGINT
             );"
        ))
        .context("create lake tables")?;
        Ok(Self {
            conn,
            lake: lake.clone(),
        })
    }

    /// Seal a synthetic window part and commit {register file + advance
    /// watermark} in ONE DuckLake transaction. Window `id` carries seqs
    /// `[id*ROWS, (id+1)*ROWS)` and advances the watermark to `(id+1)*1000`.
    pub fn commit_window(&self, id: i64) -> Result<()> {
        let part = self.lake.parts.join(format!("w{id}.parquet"));
        self.conn
            .execute_batch(&format!(
                "COPY (SELECT 'node-a/1' AS origin, {id}*{ROWS_PER_WINDOW} + i AS seq,
                              (TIMESTAMP '2026-08-31 00:00:00') AS ts
                       FROM range({ROWS_PER_WINDOW}) t(i))
                 TO '{}' (FORMAT parquet);",
                part.display()
            ))
            .context("seal part")?;
        self.conn
            .execute_batch(&format!(
                "BEGIN;
                 CALL ducklake_add_data_files('{LAKE}', 'logs', '{}');
                 INSERT INTO {LAKE}.watermarks VALUES ({id}, {});
                 COMMIT;",
                part.display(),
                watermark_of(id)
            ))
            .context("lake commit {add files + watermark}")?;
        Ok(())
    }
}

/// The watermark value window `id`'s commit advances to.
pub fn watermark_of(id: i64) -> i64 {
    (id + 1) * 1000
}

/// Everything the client's query surface reports at one instant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    /// `count(*)` over the cold table — the table-scan surface.
    pub rows: i64,
    /// `max(complete_through_micros)` — the watermark, read through the same
    /// pinned surface (it is a DuckLake table in the same catalog).
    pub watermark: i64,
    /// `max(snapshot_id)` from `ducklake_snapshots('lake')` — the snapshot
    /// version this transaction is reading.
    pub snapshot: i64,
    /// `file_count` from `ducklake_table_info('lake')` — the visible file
    /// set, sized.
    pub visible_files: i64,
    /// Data-file rows in the RAW metadata catalog passthrough
    /// (`__ducklake_metadata_lake.ducklake_data_file`) — NOT part of the
    /// query surface; measured to show it bypasses snapshot pinning.
    pub raw_catalog_files: i64,
}

/// The client stand-in: a second, independent DuckDB instance holding a
/// read-only attach of the same lake catalog.
pub struct ClientReader {
    conn: Connection,
}

impl ClientReader {
    pub fn open(lake: &Lake) -> Result<Self> {
        Ok(Self {
            conn: lake.open(true)?,
        })
    }

    pub fn begin(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    pub fn observe(&self) -> Result<Observation> {
        let one = |sql: &str| -> Result<i64> { Ok(self.conn.query_row(sql, [], |r| r.get(0))?) };
        Ok(Observation {
            rows: one(&format!("SELECT count(*) FROM {LAKE}.logs"))?,
            watermark: one(&format!(
                "SELECT coalesce(max(complete_through_micros), -1) FROM {LAKE}.watermarks"
            ))?,
            snapshot: one(&format!(
                "SELECT max(snapshot_id) FROM ducklake_snapshots('{LAKE}')"
            ))?,
            visible_files: one(&format!(
                "SELECT coalesce(file_count, 0) FROM ducklake_table_info('{LAKE}')
                 WHERE table_name = 'logs'"
            ))?,
            raw_catalog_files: one(&format!(
                "SELECT count(*) FROM __ducklake_metadata_{LAKE}.ducklake_data_file"
            ))?,
        })
    }

    /// Explicit snapshot pinning outside any transaction: read the cold
    /// table AT a named catalog version — the extension's re-pin lever.
    pub fn rows_at_version(&self, snapshot: i64) -> Result<i64> {
        Ok(self.conn.query_row(
            &format!("SELECT count(*) FROM {LAKE}.logs AT (VERSION => {snapshot})"),
            [],
            |r| r.get(0),
        )?)
    }
}
