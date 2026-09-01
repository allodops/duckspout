//! Spike drain leg (§6.2 SealPart + §6.4 LakeCommit) — issue #25.
//!
//! The seam under test: the atomic {add files + watermark} commit. One
//! DuckDB connection opens the hot database, seals a closed micro-window
//! with a single sorted `COPY ... TO 'part.parquet'`, then — in ONE
//! explicit transaction against an attached DuckLake catalog — registers
//! the file (`ducklake_add_data_files`) and inserts the watermark row into
//! a table living inside the same lake catalog. Commit → both visible;
//! abort/crash → neither.
//!
//! Deliberate delta from the §6.4 production shape, reported as a finding:
//! drain.md has the watermark sidecar riding DuckLake's own catalog writes
//! *in the catalog database's transaction* (Postgres). Through the ducklake
//! extension's public surface there is no way to piggyback an arbitrary
//! catalog-DB write onto DuckLake's metadata transaction — so the closest
//! achievable single-transaction shape is a watermark table that is itself
//! a DuckLake table in the same attached catalog, committed in the same
//! DuckLake snapshot. That is what this module demonstrates.
//!
//! Throwaway spike code — instructive, not production (spike/README.md).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use duckdb::{Connection, params};

/// Catalog alias the lake is attached under.
const LAKE: &str = "lake";
/// Cold table sealed parts are registered into.
const LAKE_TABLE: &str = "logs";

/// What one `LakeCommit` carries (spike-sized `WindowManifest`, §6.8).
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub partition: String,
    pub window_id: i64,
    /// Absolute path of the sealed Parquet part.
    pub part: PathBuf,
    /// Watermark the commit advances to (epoch micros of window close).
    pub complete_through_micros: i64,
    pub rows: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct SealStats {
    pub rows: i64,
    pub copy: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitStats {
    /// The two writes (add_data_files + watermark INSERT), before COMMIT.
    pub writes: Duration,
    /// The COMMIT itself — DuckLake's snapshot commit into the catalog.
    pub commit: Duration,
}

/// One watermark row as read back (§6.5 `read_watermarks`).
#[derive(Debug, Clone, PartialEq)]
pub struct Watermark {
    pub complete_through_micros: i64,
    pub rows: i64,
    pub part_name: String,
}

pub struct DrainCore {
    conn: Connection,
}

impl DrainCore {
    /// Open the hot database and attach a local-file DuckLake catalog under
    /// `lake_dir` (`metadata.ducklake` + `data/`). Requires the `ducklake`
    /// extension (INSTALL fetches it on first use; cached in ~/.duckdb).
    pub fn open(hot_db: &Path, lake_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(lake_dir)?;
        let conn = Connection::open(hot_db)
            .with_context(|| format!("open hot duckdb at {}", hot_db.display()))?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("install/load ducklake extension")?;
        let meta = lake_dir.join("metadata.ducklake");
        let data = lake_dir.join("data");
        conn.execute_batch(&format!(
            "ATTACH IF NOT EXISTS 'ducklake:{}' AS {LAKE} (DATA_PATH '{}');",
            meta.display(),
            data.display()
        ))
        .context("attach ducklake catalog")?;
        // Cold table (same shape as the hot micro-window) + watermark table,
        // both inside the lake catalog — one atomicity domain.
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {LAKE}.{LAKE_TABLE} (
                origin   VARCHAR,
                seq      BIGINT,
                ts       TIMESTAMP,
                severity INTEGER,
                body     VARCHAR,
                attrs    VARCHAR
             );
             CREATE TABLE IF NOT EXISTS {LAKE}.watermarks (
                partition               VARCHAR,
                window_id               BIGINT,
                complete_through_micros BIGINT,
                rows                    BIGINT,
                part_name               VARCHAR
             );"
        ))
        .context("create lake tables")?;
        Ok(Self { conn })
    }

    /// The underlying connection (hot db + attached lake) — the union spike
    /// (#27) runs its one-statement hot∪cold query on it.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// A second connection onto the SAME database instance (hot + lake),
    /// with its own transaction scope — the #27 pinning experiment reads
    /// through this while `self.conn` commits a drain.
    pub fn reader(&self) -> Result<Connection> {
        Ok(self.conn.try_clone()?)
    }

    /// §6.2 SealPart: ONE sorted COPY of a closed window to a Parquet file.
    pub fn seal_part(&self, table: &str, out: &Path) -> Result<SealStats> {
        let rows: i64 = self
            .conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        let t0 = Instant::now();
        self.conn.execute_batch(&format!(
            "COPY (SELECT * FROM {table} ORDER BY ts, origin, seq)
             TO '{}' (FORMAT parquet);",
            out.display()
        ))?;
        Ok(SealStats {
            rows,
            copy: t0.elapsed(),
        })
    }

    /// §6.4 LakeCommit, the whole seam: begin, both writes, commit.
    pub fn lake_commit(&self, req: &CommitRequest) -> Result<CommitStats> {
        self.begin()?;
        let t0 = Instant::now();
        if let Err(e) = self.add_files_and_watermark(req) {
            let _ = self.rollback();
            return Err(e);
        }
        let writes = t0.elapsed();
        let t1 = Instant::now();
        self.commit()?;
        Ok(CommitStats {
            writes,
            commit: t1.elapsed(),
        })
    }

    // The transaction surface is public in pieces so the atomicity tests can
    // abort (ROLLBACK) or crash (drop the connection) between the writes and
    // the COMMIT — that hole in the middle is exactly what the spike probes.

    pub fn begin(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        Ok(())
    }

    /// The two writes of the atomic unit: register the sealed part AND
    /// advance the watermark. No commit here.
    pub fn add_files_and_watermark(&self, req: &CommitRequest) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CALL ducklake_add_data_files('{LAKE}', '{LAKE_TABLE}', '{}')",
                req.part.display()
            ))
            .context("ducklake_add_data_files")?;
        self.conn
            .execute(
                &format!("INSERT INTO {LAKE}.watermarks VALUES (?, ?, ?, ?, ?)"),
                params![
                    req.partition,
                    req.window_id,
                    req.complete_through_micros,
                    req.rows,
                    req.part.file_name().unwrap().to_string_lossy()
                ],
            )
            .context("insert watermark row")?;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
        Ok(())
    }

    // --- read-back surface (§6.5 Indeterminate resolution shape) ---

    /// Rows visible in the cold table through the lake catalog.
    pub fn lake_row_count(&self) -> Result<i64> {
        Ok(self.conn.query_row(
            &format!("SELECT count(*) FROM {LAKE}.{LAKE_TABLE}"),
            [],
            |r| r.get(0),
        )?)
    }

    /// §6.5 `read_watermarks` for one (partition, window): None = the commit
    /// never landed.
    pub fn read_watermark(&self, partition: &str, window_id: i64) -> Result<Option<Watermark>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT complete_through_micros, rows, part_name
             FROM {LAKE}.watermarks WHERE partition = ? AND window_id = ?"
        ))?;
        let mut rows = stmt.query(params![partition, window_id])?;
        match rows.next()? {
            Some(r) => Ok(Some(Watermark {
                complete_through_micros: r.get(0)?,
                rows: r.get(1)?,
                part_name: r.get(2)?,
            })),
            None => Ok(None),
        }
    }

    /// Data-file paths DuckLake has registered for the cold table, read from
    /// the catalog's own metadata — proves add-file (zero rewrite) vs a data
    /// rewrite, and is the registration half of the §6.5 read-back.
    pub fn registered_files(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT data_file.path FROM __ducklake_metadata_lake.ducklake_data_file data_file
             ORDER BY data_file.path",
        )?;
        let paths = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(paths)
    }
}
