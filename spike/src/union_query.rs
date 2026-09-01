//! Spike hot∪cold union (§7.5) — issue #27, the §12.1 finale.
//!
//! ONE SQL query unions the DuckLake-attached cold table and the hot
//! micro-window tables, each branch bounded by `complete_through`, with the
//! watermark itself SELECTable alongside every row (§12.1 "visible"):
//!
//! ```text
//! dataset = cold_branch(lake files ≤ complete_through)
//!           UNION ALL
//!           hot_branch(hot windows, > complete_through)
//! ```
//!
//! The seam under test is the *watermark join* (§7.5): drained rows are
//! physically present on BOTH sides (the sealed part in the lake AND the
//! not-yet-truncated hot window), so the `complete_through` bound is the
//! only thing standing between exact tiling and a double count. Two SQL
//! shapes are provided:
//!
//! - [`pinned_union_sql`] — both branch bounds come from ONE watermark read
//!   inside the statement (a CTE), i.e. one snapshot: the §7.5 shape.
//! - [`split_bounds_union_sql`] — each branch takes a caller-supplied bound,
//!   simulating a reader that read the watermark twice at different times
//!   with a LakeCommit landing in between. This is the TOCTOU the §7.6
//!   bind-time pinning exists to forbid; the tests use it to *exhibit* the
//!   double count.
//!
//! Throwaway spike code — instructive, not production (spike/README.md).

use anyhow::{Context, Result};
use arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use duckdb::{Connection, params};

/// Column name the union exposes the watermark under (§12.1 "visible").
pub const CT_COL: &str = "complete_through";

/// The dataset's `complete_through`: max over the lake-resident watermark
/// table (spike scope: one partition, monotone windows). `None` = nothing
/// drained yet.
pub fn read_complete_through(conn: &Connection) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT max(complete_through_micros) FROM lake.watermarks",
        [],
        |r| r.get(0),
    )
    .context("read complete_through")
}

/// The §7.5 union, single statement: both branch bounds AND the visible
/// `complete_through` column come from ONE read of the watermark table (the
/// `pinned` CTE) — within one statement DuckDB evaluates everything against
/// one transaction snapshot, so the two branches tile by construction.
///
/// `hot_source` is the FROM-source of the hot branch (a table name, or a
/// parenthesized UNION ALL over the open windows). Before the first drain
/// the watermark is absent; COALESCE to -1 degrades the union to all-hot
/// (synthetic timestamps are far above epoch 0 — spike-grade).
pub fn pinned_union_sql(hot_source: &str) -> String {
    format!(
        "WITH pinned AS (
            SELECT coalesce(max(complete_through_micros), -1) AS ct
            FROM lake.watermarks
         )
         SELECT 'cold' AS branch, l.*, make_timestamp(p.ct) AS {CT_COL}
         FROM lake.logs l CROSS JOIN pinned p
         WHERE l.ts <= make_timestamp(p.ct)
         UNION ALL
         SELECT 'hot' AS branch, h.*, make_timestamp(p.ct) AS {CT_COL}
         FROM {hot_source} h CROSS JOIN pinned p
         WHERE h.ts > make_timestamp(p.ct)"
    )
}

/// The hazard shape: cold and hot bounds supplied separately, as a reader
/// that read the watermark at two different instants would hold them. When
/// a LakeCommit lands between the two reads (`hot_bound` stale, `cold_bound`
/// fresh), rows in `(hot_bound, cold_bound]` satisfy BOTH branch predicates
/// and double-count. Never a production shape — the exhibit.
pub fn split_bounds_union_sql(
    hot_source: &str,
    hot_bound_micros: i64,
    cold_bound_micros: i64,
) -> String {
    format!(
        "SELECT 'cold' AS branch, l.*, make_timestamp({cold_bound_micros}) AS {CT_COL}
         FROM lake.logs l
         WHERE l.ts <= make_timestamp({cold_bound_micros})
         UNION ALL
         SELECT 'hot' AS branch, h.*, make_timestamp({hot_bound_micros}) AS {CT_COL}
         FROM {hot_source} h
         WHERE h.ts > make_timestamp({hot_bound_micros})"
    )
}

/// What one execution of a union query looked like, measured in SQL over
/// the union itself — the tiling verdict in numbers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnionAudit {
    pub total: i64,
    pub cold_rows: i64,
    pub hot_rows: i64,
    /// Distinct row identities (origin, seq): `total > distinct_rows` IS the
    /// double count; `total == distinct_rows == ingested` is exact tiling.
    pub distinct_rows: i64,
    /// Distinct values of the visible `complete_through` column (a pinned
    /// union carries exactly one).
    pub ct_values: i64,
    /// Max visible `complete_through`, epoch micros (None = zero rows).
    pub ct_max_micros: Option<i64>,
}

/// Run a union query and audit it: totals per branch, identity distinctness
/// (the double-count detector), and the visible watermark column.
pub fn audit(conn: &Connection, union_sql: &str) -> Result<UnionAudit> {
    conn.query_row(
        &format!(
            "SELECT count(*),
                    count(*) FILTER (WHERE branch = 'cold'),
                    count(*) FILTER (WHERE branch = 'hot'),
                    count(DISTINCT (origin, seq)),
                    count(DISTINCT {CT_COL}),
                    max(epoch_us({CT_COL}))
             FROM ({union_sql}) u"
        ),
        [],
        |r| {
            Ok(UnionAudit {
                total: r.get(0)?,
                cold_rows: r.get(1)?,
                hot_rows: r.get(2)?,
                distinct_rows: r.get(3)?,
                ct_values: r.get(4)?,
                ct_max_micros: r.get(5)?,
            })
        },
    )
    .context("audit union")
}

/// Land Flight-fetched hot rows (engine-shaped Arrow batches: origin, seq,
/// ts, severity, body, attrs) into a local table, so the executor's ONE SQL
/// union can reference the remote hot branch — the spike's stand-in for
/// Airport's table function surface (§7.4; the real extension binds the
/// remote scan into the plan instead of materializing).
pub fn materialize_hot(conn: &Connection, table: &str, batches: &[RecordBatch]) -> Result<i64> {
    conn.execute_batch(&format!(
        "CREATE TABLE {table} (
            origin   VARCHAR,
            seq      BIGINT,
            ts       TIMESTAMP,
            severity INTEGER,
            body     VARCHAR,
            attrs    VARCHAR
         )"
    ))?;
    let mut n = 0i64;
    let mut app = conn.appender(table)?;
    for batch in batches {
        let col = |i: usize| batch.column(i).as_any();
        let origin = col(0).downcast_ref::<StringArray>().context("origin")?;
        let seq = col(1).downcast_ref::<Int64Array>().context("seq")?;
        let ts = col(2)
            .downcast_ref::<TimestampMicrosecondArray>()
            .context("ts")?;
        let severity = col(3).downcast_ref::<Int32Array>().context("severity")?;
        let body = col(4).downcast_ref::<StringArray>().context("body")?;
        let attrs = col(5).downcast_ref::<StringArray>().context("attrs")?;
        for i in 0..batch.num_rows() {
            app.append_row(params![
                origin.value(i),
                seq.value(i),
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Microsecond, ts.value(i)),
                severity.value(i),
                body.value(i),
                attrs.value(i)
            ])?;
            n += 1;
        }
    }
    app.flush()?;
    Ok(n)
}
