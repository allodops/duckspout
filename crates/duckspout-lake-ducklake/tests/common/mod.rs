//! Shared fixtures for the `DuckLake` backend's integration tests.
//!
//! First run needs network once: `INSTALL ducklake` fetches the extension
//! into `~/.duckdb`, cached thereafter (same discipline as the spike).

// Justification for the allow: every integration-test binary compiles this
// module independently and none uses all of it.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use duckdb::Connection;
use duckspout_lake_ducklake::{DuckLakeCommitter, DuckLakeConfig};
use duckspout_types::PartName;

/// The catalog and data locations for one test lake.
pub struct LakePaths {
    pub catalog: PathBuf,
    pub data: PathBuf,
}

pub fn lake_paths(dir: &Path) -> LakePaths {
    LakePaths {
        catalog: dir.join("meta.ducklake"),
        data: dir.join("data"),
    }
}

/// Opens one committer instance (its own embedded `DuckDB`) on a
/// `DuckDB`-file catalog — the single-instance topology.
pub fn open_committer(paths: &LakePaths) -> DuckLakeCommitter {
    DuckLakeCommitter::open(DuckLakeConfig {
        catalog_uri: paths.catalog.display().to_string(),
        data_path: paths.data.display().to_string(),
        multi_process: false,
    })
    .expect("committer opens")
}

/// Opens one committer instance on a **SQLite** catalog (`WAL` forced at
/// attach, issue #119) — the topology the racing tests use: SQLite does
/// real cross-connection locking through the catalog file itself, so two
/// independent committer instances contend with the same fidelity as two
/// processes, which a `DuckDB`-file catalog cannot provide in-process
/// (#119's false-pass zone).
pub fn open_committer_sqlite(dir: &Path, paths: &LakePaths) -> DuckLakeCommitter {
    DuckLakeCommitter::open(DuckLakeConfig {
        catalog_uri: format!("sqlite:{}", dir.join("catalog.sqlite").display()),
        data_path: paths.data.display().to_string(),
        multi_process: false,
    })
    .expect("sqlite-catalog committer opens")
}

/// Materializes a suite part: a Parquet file with the conformance schema
/// (`ts TIMESTAMP`, `body VARCHAR`) and 10 rows, written through a plain
/// `DuckDB` connection. Idempotent: an existing file is left untouched
/// (deterministic naming makes re-materialization byte-equivalent anyway;
/// not rewriting a registered object mirrors §6.1).
pub fn materialize_part(data: &Path, part: &PartName) -> Result<(), String> {
    let path = data.join(part.as_str());
    if path.exists() {
        return Ok(());
    }
    let parent = path.parent().ok_or("part path has no parent")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
    conn.execute_batch(&format!(
        "COPY (SELECT TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i) SECOND AS ts,
                      'row-' || i AS body
               FROM range(10) t(i))
         TO '{}' (FORMAT parquet)",
        path.display()
    ))
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// A raw inspection connection onto a lake — row-level assertions the
/// port cannot express (duplicate registration, expired-row removal).
/// `catalog_uri` is the same `ATTACH` body the committer used.
pub fn inspect_uri(catalog_uri: &str, data: &Path) -> Connection {
    let conn = Connection::open_in_memory().expect("inspection connection");
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .expect("ducklake loads");
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:{catalog_uri}' AS lake (DATA_PATH '{}');",
        data.display()
    ))
    .expect("inspection attach");
    conn
}

/// Inspection over the `DuckDB`-file catalog of [`lake_paths`].
pub fn inspect(paths: &LakePaths) -> Connection {
    inspect_uri(&paths.catalog.display().to_string(), &paths.data)
}

pub fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0))
        .expect("count query")
}
