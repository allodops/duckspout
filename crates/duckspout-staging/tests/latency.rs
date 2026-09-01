//! Measurement harness for the #109 evidence table: the commit-latency
//! distribution with the engine's deferred-checkpoint scheme vs. `DuckDB`'s
//! default auto-checkpoint-inside-commit. Not a CI assertion — latency is
//! substrate-sensitive — so it is `#[ignore]`d and run explicitly:
//!
//! ```text
//! cargo nextest run -p duckspout-staging --release \
//!     --run-ignored ignored-only -E 'test(commit_latency_distribution)' \
//!     --no-capture
//! ```
//!
//! Both legs run the identical write pattern (the spike's outlier shape:
//! 10 k-row batches, #23): same payload, same table shape (system columns
//! included), one applied-watermark upsert per transaction, and only the
//! `COMMIT` (leg B) / `commit()` (leg A) is timed. The one difference is
//! the checkpoint scheme — leg A defers (and pays the pause in one explicit
//! `checkpoint()` afterwards, timed separately), leg B leaves the 16 MiB
//! default in place.

mod common;

use std::time::{Duration, Instant};

use duckspout_types::{DatasetId, PartitionId, WindowId};

use common::{log_batch, open_engine};

const BATCHES: usize = 120;
const ROWS_PER_BATCH: usize = 10_000;
const BODY_PAD: usize = 160;

struct Distribution {
    label: &'static str,
    samples: Vec<Duration>,
}

impl Distribution {
    fn quantile(&self, q: f64) -> Duration {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let last = sorted.len() - 1;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "quantile index arithmetic on a ~100-element vector"
        )]
        let index = ((last as f64) * q).round() as usize;
        sorted[index.min(last)]
    }

    fn row(&self) -> String {
        let over_50ms = self
            .samples
            .iter()
            .filter(|d| **d > Duration::from_millis(50))
            .count();
        format!(
            "| {} | {:.1?} | {:.1?} | {:.1?} | {:.1?} | {over_50ms} |",
            self.label,
            self.quantile(0.50),
            self.quantile(0.95),
            self.quantile(0.99),
            self.quantile(1.0),
        )
    }
}

/// Prints the #109 evidence table. Ignored: a measurement, not an assertion
/// (the behavioral guarantees live in `tests/checkpoint.rs`).
#[test]
#[ignore = "measurement harness for the #109 PR evidence; run explicitly with --release"]
fn commit_latency_distribution() {
    // --- Leg A: the engine, checkpoints deferred (#109 scheme). ---
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "node-a/1");
    let dataset = DatasetId::new("logs");
    let partition = PartitionId::new("t1.0");
    let mut engine_leg = Distribution {
        label: "engine (deferred checkpoints)",
        samples: Vec::with_capacity(BATCHES),
    };
    for batch in 0..BATCHES {
        let mut txn = engine.begin().expect("begin");
        txn.append(
            &dataset,
            &partition,
            WindowId(0),
            &log_batch(ROWS_PER_BATCH, batch_ts(batch), BODY_PAD),
        )
        .expect("append");
        let start = Instant::now();
        txn.commit().expect("commit");
        engine_leg.samples.push(start.elapsed());
    }
    let start = Instant::now();
    engine.checkpoint().expect("checkpoint");
    let checkpoint_pause = start.elapsed();

    // --- Leg B: raw connection, DuckDB defaults (auto-checkpoint on). ---
    let control_dir = tempfile::tempdir().expect("tempdir");
    let db = control_dir.path().join("control.db");
    let conn = duckdb::Connection::open(&db).expect("open");
    conn.execute_batch(
        "CREATE TABLE hot_w0 (
             ts TIMESTAMP NOT NULL, severity INTEGER, body VARCHAR,
             origin VARCHAR NOT NULL, seq UBIGINT NOT NULL);
         CREATE TABLE applied (
             partition VARCHAR NOT NULL, origin VARCHAR NOT NULL,
             applied_seq UBIGINT NOT NULL, PRIMARY KEY (partition, origin));",
    )
    .expect("ddl");
    let mut default_leg = Distribution {
        label: "default (auto-checkpoint in commit)",
        samples: Vec::with_capacity(BATCHES),
    };
    let mut seq = 0u64;
    for batch in 0..BATCHES {
        conn.execute_batch("BEGIN TRANSACTION").expect("begin");
        let payload = log_batch(ROWS_PER_BATCH, batch_ts(batch), BODY_PAD);
        let augmented = augment(&payload, seq + 1);
        seq += ROWS_PER_BATCH as u64;
        let mut appender = conn.appender("hot_w0").expect("appender");
        appender.append_record_batch(augmented).expect("append");
        appender.flush().expect("flush");
        drop(appender);
        conn.execute(
            "INSERT INTO applied (partition, origin, applied_seq) VALUES (?, ?, ?)
             ON CONFLICT (partition, origin) DO UPDATE SET applied_seq = excluded.applied_seq",
            duckdb::params!["t1.0", "node-a/1", seq],
        )
        .expect("upsert");
        let start = Instant::now();
        conn.execute_batch("COMMIT").expect("commit");
        default_leg.samples.push(start.elapsed());
    }

    println!();
    println!(
        "#109 commit-latency table — {BATCHES} batches x {ROWS_PER_BATCH} rows, \
         ~{BODY_PAD}B body pad, identical table shape both legs; commit step only"
    );
    println!("| leg | p50 | p95 | p99 | max | commits >50ms |");
    println!("|---|---:|---:|---:|---:|---:|");
    println!("{}", default_leg.row());
    println!("{}", engine_leg.row());
    println!(
        "engine leg's one explicit checkpoint() afterwards (the drain-window \
         pause the ack path no longer pays): {checkpoint_pause:.1?}"
    );
}

fn batch_ts(batch: usize) -> i64 {
    i64::try_from(batch).expect("batch index") * 1_000_000
}

/// Payload batch + (origin, seq) columns, mirroring the engine's augmented
/// shape so both legs commit identical bytes.
fn augment(
    payload: &duckspout_staging::arrow::record_batch::RecordBatch,
    first_seq: u64,
) -> duckspout_staging::arrow::record_batch::RecordBatch {
    use duckspout_staging::arrow::array::{ArrayRef, StringArray, UInt64Array};
    use duckspout_staging::arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use duckspout_staging::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let rows = payload.num_rows();
    let mut fields: Vec<FieldRef> = payload.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new("origin", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("seq", DataType::UInt64, false)));
    let mut columns: Vec<ArrayRef> = payload.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec!["node-a/1"; rows])));
    columns.push(Arc::new(UInt64Array::from_iter_values(
        first_seq..first_seq + rows as u64,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("augment")
}
