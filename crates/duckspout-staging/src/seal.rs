//! The [`SealSurface`] port, implemented over the staging engine (§6.2) —
//! the seal-side read surface the drain consumes (ADR-0008: the trait lives
//! in `duckspout-types` because drain and staging are both protocol crates;
//! this crate owns the implementation).
//!
//! `SealPart` is a single sorted, deduplicating `COPY … TO` over the
//! window's staging table, executed on a dedicated read connection (#114):
//! the seal never contends with the ack path's write lock. The sealed
//! Parquet file lands in a scratch directory under the hot volume
//! ([`SEAL_SCRATCH_DIR`]); the drain PUTs it to object storage and deletes
//! it — it is never served from.
//!
//! # Window closes are reported, not discovered
//!
//! The engine's registry does not know ingest cadence: windows close on the
//! arrival axis (`hot.window`, `docs/design/ingest.md` §2.2), which the
//! composition (the daemon's ingest roller) owns. The surface therefore
//! keeps an in-memory close log fed by [`EngineSealSurface::note_closed`];
//! the roller calls it when it advances past a window, and **re-derives the
//! closes at boot** (every window below the partition's current arrival
//! window is closed — a deterministic recomputation, so nothing durable is
//! needed here). A window never noted closed is simply not offered yet —
//! failing toward "not drained" is always safe (R-5).
//!
//! # Blocking discipline
//!
//! Like the engine itself, every method does blocking `DuckDB` work; the
//! port futures complete synchronously. Callers embed the surface off their
//! reactor (module docs of [`crate::engine`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use duckspout_types::{
    BoxFuture, DatasetId, DrainableWindow, DropOutcome, OriginSeqRange, PartitionId, SealError,
    SealRequest, SealSurface, SealedPart, Storage, StoragePath, WindowId,
};

use crate::engine::{StagingEngine, StagingError, quote_ident};

/// The scratch directory for sealed parts, under the engine's hot
/// directory. Relative — it doubles as the [`StoragePath`] prefix of every
/// [`SealedPart::path`] this surface returns.
pub const SEAL_SCRATCH_DIR: &str = "seal";

type WindowKey = (DatasetId, PartitionId, u64);

/// The staging engine behind the [`SealSurface`] port. Composition: the
/// daemon owns the engine `Arc`, wraps it once, feeds closes from its
/// ingest roller, and hands the drain a `dyn SealSurface`.
pub struct EngineSealSurface<S: Storage> {
    engine: Arc<StagingEngine<S>>,
    /// (dataset, partition, window) → close instant, Unix ms. First close
    /// wins: re-noting is idempotent.
    closes: Mutex<HashMap<WindowKey, i64>>,
}

impl<S: Storage> EngineSealSurface<S> {
    /// Wraps an engine. The close log starts empty; see the module docs for
    /// who feeds it and the boot-time re-derivation obligation.
    #[must_use]
    pub fn new(engine: Arc<StagingEngine<S>>) -> Self {
        Self {
            engine,
            closes: Mutex::new(HashMap::new()),
        }
    }

    /// Notes that a window closed to ordinary ingest at `at_ms` (Unix
    /// milliseconds). Idempotent: the first noted instant wins, so a
    /// boot-time re-derivation cannot move a close later and shorten the
    /// §6.3 lateness hold.
    ///
    /// # Panics
    ///
    /// If a previous holder of the close log panicked mid-update (the log
    /// is then suspect; fail loud, R-3).
    pub fn note_closed(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
        at_ms: i64,
    ) {
        self.closes
            .lock()
            .expect("seal close log lock poisoned")
            .entry((dataset.clone(), partition.clone(), window.0))
            .or_insert(at_ms);
    }

    /// The wrapped engine.
    #[must_use]
    pub fn engine(&self) -> &Arc<StagingEngine<S>> {
        &self.engine
    }

    fn closed_at(&self, key: &WindowKey) -> Option<i64> {
        self.closes
            .lock()
            .expect("seal close log lock poisoned")
            .get(key)
            .copied()
    }

    fn forget_close(&self, key: &WindowKey) {
        self.closes
            .lock()
            .expect("seal close log lock poisoned")
            .remove(key);
    }

    fn drainable_blocking(&self) -> Result<Vec<DrainableWindow>, SealError> {
        // Intersect the close log with the live registry: a dropped window
        // must never be offered, whatever the log still holds.
        let windows = self.engine.list_windows().map_err(|e| backend(&e))?;
        Ok(windows
            .into_iter()
            .filter_map(|w| {
                let key = (w.dataset.clone(), w.partition.clone(), w.window.0);
                self.closed_at(&key).map(|closed_at_ms| DrainableWindow {
                    dataset: w.dataset,
                    partition: w.partition,
                    window: w.window,
                    closed_at_ms,
                })
            })
            .collect())
    }

    fn seal_blocking(&self, request: &SealRequest) -> Result<SealedPart, SealError> {
        let table = self
            .engine
            .window_table(&request.dataset, &request.partition, request.window)
            .map_err(|e| backend(&e))?
            .ok_or_else(|| SealError::UnknownWindow {
                dataset: request.dataset.clone(),
                partition: request.partition.clone(),
                window: request.window,
            })?;
        let reader = self.engine.reader().map_err(|e| backend(&e))?;
        let conn = reader.conn();

        let total: u64 = conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .map_err(|e| engine_backend(&e))?;
        let origin_coverage = coverage_runs(conn, &table)?;
        let (event_time_min_ms, event_time_max_ms) =
            event_time_bounds(conn, &table, &request.event_time_column)?;

        // The sealed row set: drain-time dedup keeps, per distinct key, the
        // deterministic smallest-(origin, seq) winner (§6.2).
        let sealed_rows_sql = match &request.dedup_key {
            Some(key_cols) => {
                let keys = column_list(key_cols);
                format!(
                    "SELECT * EXCLUDE (__ds_dedup_rn) FROM (
                         SELECT *, row_number() OVER (
                             PARTITION BY {keys} ORDER BY origin, seq
                         ) AS __ds_dedup_rn FROM {table}
                     ) WHERE __ds_dedup_rn = 1"
                )
            }
            None => format!("SELECT * FROM {table}"),
        };
        let rows: u64 = conn
            .query_row(
                &format!("SELECT count(*) FROM ({sealed_rows_sql})"),
                [],
                |row| row.get(0),
            )
            .map_err(|e| engine_backend(&e))?;

        let scratch_dir = self.engine.hot_dir().join(SEAL_SCRATCH_DIR);
        std::fs::create_dir_all(&scratch_dir)
            .map_err(|e| SealError::Backend(format!("seal scratch dir: {e}")))?;
        // The table name is already a pure, injective function of the
        // window identity (crate::naming), so it names the scratch file too.
        let relative = format!("{SEAL_SCRATCH_DIR}/{table}.parquet");
        let absolute = self.engine.hot_dir().join(&relative);

        let order = column_list(&request.order_by);
        conn.execute_batch(&format!(
            "COPY ({sealed_rows_sql} ORDER BY {order})
             TO '{}' (FORMAT parquet)",
            absolute.display()
        ))
        .map_err(|e| engine_backend(&e))?;

        Ok(SealedPart {
            path: StoragePath::new(relative),
            rows,
            event_time_min_ms,
            event_time_max_ms,
            dedup_removed: total - rows,
            origin_coverage,
        })
    }

    fn drop_blocking(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
        covered: &[OriginSeqRange],
    ) -> Result<DropOutcome, SealError> {
        // TN-32 (PR #137): only rows covered by the committed coverage may
        // leave staging. An uncovered row — e.g. a late arrival that landed
        // between the seal COPY and this drop — is durable acked data that
        // will drain later as a supplement; it must survive the drop.
        //
        // The residue count and the subsequent drop are two statements, not
        // one transaction: sound because a window being dropped is past its
        // lateness hold, and post-hold stragglers take arrival-window
        // placement into the *current* window (§6.3) — nothing appends
        // here anymore.
        let Some(table) = self
            .engine
            .window_table(dataset, partition, window)
            .map_err(|e| backend(&e))?
        else {
            return Ok(DropOutcome::AlreadyGone);
        };
        let reader = self.engine.reader().map_err(|e| backend(&e))?;
        let predicate = coverage_predicate(covered);
        let residue: u64 = reader
            .conn()
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE NOT ({predicate})"),
                [],
                |row| row.get(0),
            )
            .map_err(|e| engine_backend(&e))?;
        if residue == 0 {
            // The common case: everything is covered — the O(1) whole-table
            // drop (§6.9), no vacuum debt.
            self.engine
                .drop_window(dataset, partition, window)
                .map_err(|e| backend(&e))?;
            self.forget_close(&(dataset.clone(), partition.clone(), window.0));
            return Ok(DropOutcome::Dropped);
        }
        // Residue exists: delete only the covered rows and keep the window
        // (still closed, still enumerable) for the supplement path (§6.6).
        self.engine
            .delete_covered_rows(&table, &predicate)
            .map_err(|e| backend(&e))?;
        Ok(DropOutcome::ResidueKept { rows: residue })
    }
}

/// The SQL predicate "this row's `(origin, seq)` is inside `covered`".
/// Origins are escaped as SQL string literals; an empty coverage yields
/// `FALSE` (nothing covered — nothing may be dropped).
fn coverage_predicate(covered: &[OriginSeqRange]) -> String {
    if covered.is_empty() {
        return "FALSE".to_owned();
    }
    covered
        .iter()
        .map(|range| {
            format!(
                "(origin = '{}' AND seq BETWEEN {} AND {})",
                range.origin.as_str().replace('\'', "''"),
                range.first_seq,
                range.last_seq
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

impl<S: Storage> SealSurface for EngineSealSurface<S> {
    fn drainable_windows(&self) -> BoxFuture<'_, Result<Vec<DrainableWindow>, SealError>> {
        let result = self.drainable_blocking();
        Box::pin(async move { result })
    }

    fn seal_window(&self, request: SealRequest) -> BoxFuture<'_, Result<SealedPart, SealError>> {
        let result = self.seal_blocking(&request);
        Box::pin(async move { result })
    }

    fn drop_window(
        &self,
        dataset: DatasetId,
        partition: PartitionId,
        window: WindowId,
        covered: Vec<OriginSeqRange>,
    ) -> BoxFuture<'_, Result<DropOutcome, SealError>> {
        let result = self.drop_blocking(&dataset, &partition, window, &covered);
        Box::pin(async move { result })
    }
}

/// Renders a quoted, comma-separated column list.
fn column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

fn backend(error: &StagingError) -> SealError {
    SealError::Backend(error.to_string())
}

/// The window's per-origin seq coverage as maximal contiguous runs
/// (gaps-and-islands over the system columns), sorted by
/// `(origin, first_seq)`. Computed **pre-dedup**: a removed duplicate is
/// still a covered row.
fn coverage_runs(conn: &Connection, table: &str) -> Result<Vec<OriginSeqRange>, SealError> {
    let sql = format!(
        "SELECT origin, min(seq) AS first_seq, max(seq) AS last_seq FROM (
             SELECT origin, seq,
                    CAST(seq AS HUGEINT)
                        - row_number() OVER (PARTITION BY origin ORDER BY seq) AS run
             FROM {table}
         ) GROUP BY origin, run ORDER BY origin, first_seq"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| engine_backend(&e))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(OriginSeqRange {
                origin: duckspout_types::NodeId::new(row.get::<_, String>(0)?),
                first_seq: row.get(1)?,
                last_seq: row.get(2)?,
            })
        })
        .map_err(|e| engine_backend(&e))?;
    let mut runs = Vec::new();
    for row in rows {
        runs.push(row.map_err(|e| engine_backend(&e))?);
    }
    Ok(runs)
}

/// Event-time min/max over the window, Unix milliseconds; `(0, 0)` for an
/// empty window (the manifest still needs a well-formed range, and `0`
/// never advances a running-maximum watermark).
fn event_time_bounds(
    conn: &Connection,
    table: &str,
    event_time_column: &str,
) -> Result<(i64, i64), SealError> {
    let col = quote_ident(event_time_column);
    let (min_ms, max_ms): (Option<i64>, Option<i64>) = conn
        .query_row(
            &format!("SELECT epoch_ms(min({col})), epoch_ms(max({col})) FROM {table}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| engine_backend(&e))?;
    Ok((min_ms.unwrap_or(0), max_ms.unwrap_or(0)))
}

fn engine_backend(error: &duckdb::Error) -> SealError {
    SealError::Backend(error.to_string())
}
