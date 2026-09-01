//! Spike ingest core: WAL=hot (§4.2, ADR-0003).
//!
//! One persistent DuckDB file is the durability primitive. A micro-window
//! table holds the rows; every batch lands in one explicit transaction and
//! DuckDB's own fsync-on-commit WAL is what makes `StageCommit` durable.
//! Throwaway spike code — instructive, not production (spike/README.md).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use duckdb::{Connection, params};

/// One synthetic log row, shaped like the §4.2.3 staging table: the two
/// system columns (`origin`, `seq`) plus a minimal OTLP-log-ish payload.
#[derive(Debug, Clone)]
pub struct LogRow {
    pub origin: String,
    pub seq: i64,
    pub ts_micros: i64,
    pub severity: i32,
    pub body: String,
    pub attrs: String,
}

impl LogRow {
    /// Deterministic synthetic row `i` (no rand: spike doesn't need it).
    pub fn synthetic(i: i64) -> Self {
        Self {
            origin: "node-a/1".to_string(),
            seq: i,
            ts_micros: 1_756_600_000_000_000 + i,
            severity: (i % 24) as i32,
            body: format!("synthetic log line {i} — the quick brown fox jumps over the lazy duck"),
            attrs: format!("{{\"k8s.pod\":\"pod-{}\",\"i\":{i}}}", i % 16),
        }
    }
}

/// Timings for one `insert_batch` call.
#[derive(Debug, Clone, Copy)]
pub struct BatchTiming {
    /// BEGIN + all inserts (appender rows + flush), before COMMIT.
    pub insert: Duration,
    /// The COMMIT statement alone — this is where the WAL fsync lives.
    pub commit: Duration,
}

pub struct IngestCore {
    conn: Connection,
    path: PathBuf,
}

impl IngestCore {
    /// Open (or create) the persistent hot database file.
    pub fn open(path: &Path) -> Result<Self> {
        let conn =
            Connection::open(path).with_context(|| format!("open duckdb at {}", path.display()))?;
        Ok(Self {
            conn,
            path: path.to_path_buf(),
        })
    }

    /// Create one micro-window staging table (§4.2.2: one table per window).
    pub fn create_window(&self, table: &str) -> Result<()> {
        self.conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                origin   VARCHAR NOT NULL,
                seq      BIGINT  NOT NULL,
                ts       TIMESTAMP NOT NULL,
                severity INTEGER,
                body     VARCHAR,
                attrs    VARCHAR
            )"
        ))?;
        Ok(())
    }

    /// Insert one batch inside one explicit transaction; ack-worthy only
    /// after COMMIT returns (StageCommit shape). Returns split timings.
    pub fn insert_batch(&mut self, table: &str, rows: &[LogRow]) -> Result<BatchTiming> {
        let t0 = Instant::now();
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        {
            let mut app = self.conn.appender(table)?;
            for r in rows {
                app.append_row(params![
                    r.origin,
                    r.seq,
                    // TIMESTAMP from epoch micros, computed engine-side.
                    duckdb::types::Value::Timestamp(
                        duckdb::types::TimeUnit::Microsecond,
                        r.ts_micros
                    ),
                    r.severity,
                    r.body,
                    r.attrs
                ])?;
            }
            app.flush()?;
        }
        let insert = t0.elapsed();
        let t1 = Instant::now();
        self.conn.execute_batch("COMMIT")?;
        let commit = t1.elapsed();
        Ok(BatchTiming { insert, commit })
    }

    /// One-value scalar query (spike convenience).
    pub fn conn_query_row<T: duckdb::types::FromSql>(&self, sql: &str) -> Result<T> {
        Ok(self.conn.query_row(sql, [], |r| r.get(0))?)
    }

    pub fn count(&self, table: &str) -> Result<i64> {
        let n: i64 = self
            .conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        Ok(n)
    }

    /// Size of the engine's own WAL file (`<db>.wal`), if present.
    pub fn wal_size(&self) -> Option<u64> {
        let mut wal = self.path.as_os_str().to_owned();
        wal.push(".wal");
        std::fs::metadata(PathBuf::from(wal)).ok().map(|m| m.len())
    }

    pub fn db_size(&self) -> Option<u64> {
        std::fs::metadata(&self.path).ok().map(|m| m.len())
    }
}
