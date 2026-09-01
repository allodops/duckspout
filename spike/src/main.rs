//! Spike binary — issue #23: DuckDB embed + commit-latency ballpark (§4.2,
//! ADR-0002/0003). Throwaway by design; deleted at v0.1 (spike/README.md).
//!
//! Scope note (owner ruling 2026-09-01): durability re-verification (kill -9,
//! fsync granularity probing) is descoped — DuckDB's documented WAL/checkpoint
//! behavior is trusted. What remains: the working embed and capacity data.
//!
//! Subcommands:
//!   bench <db-dir>        commit-latency distribution per batch size
//!   count <db> <table>    print row count (reopen path)

use std::path::Path;
use std::process::ExitCode;

use spike::ingest::{IngestCore, LogRow};

const TABLE: &str = "hot_w0";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let strs: Vec<&str> = args.iter().map(String::as_str).collect();
    let r = match strs.as_slice() {
        ["bench", dir] => bench(Path::new(dir)),
        ["count", db, table] => count(Path::new(db), table),
        _ => {
            eprintln!("usage: spike bench <db-dir> | count <db> <table>");
            return ExitCode::from(2);
        }
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

/// Commit-latency distribution across batch sizes — capacity data for the
/// ack budget (§4.2: the local commit is on the ClientAck critical path).
fn bench(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    println!(
        "batch_rows  reps  insert_p50  commit_p50  commit_p95  commit_p99  commit_max  rows/s(txn)"
    );
    for (batch, reps) in [(1usize, 200usize), (100, 100), (10_000, 20)] {
        let db = dir.join(format!("bench_{batch}.db"));
        let _ = std::fs::remove_file(&db);
        let mut core = IngestCore::open(&db)?;
        core.create_window(TABLE)?;
        let mut inserts = Vec::with_capacity(reps);
        let mut commits = Vec::with_capacity(reps);
        let mut seq = 0i64;
        let t_all = std::time::Instant::now();
        for _ in 0..reps {
            let rows: Vec<LogRow> = (0..batch)
                .map(|i| LogRow::synthetic(seq + i as i64))
                .collect();
            seq += batch as i64;
            let t = core.insert_batch(TABLE, &rows)?;
            inserts.push(t.insert);
            commits.push(t.commit);
        }
        let elapsed = t_all.elapsed();
        inserts.sort();
        commits.sort();
        let p = |v: &[std::time::Duration], q: f64| v[((v.len() - 1) as f64 * q) as usize];
        println!(
            "{batch:>10}  {reps:>4}  {:>10.1?}  {:>10.1?}  {:>10.1?}  {:>10.1?}  {:>10.1?}  {:>10.0}",
            p(&inserts, 0.5),
            p(&commits, 0.5),
            p(&commits, 0.95),
            p(&commits, 0.99),
            commits[commits.len() - 1],
            (batch * reps) as f64 / elapsed.as_secs_f64(),
        );
        println!(
            "            post-run: rows={} wal={:?}B db={:?}B",
            core.count(TABLE)?,
            core.wal_size(),
            core.db_size()
        );
    }
    Ok(())
}

fn count(db: &Path, table: &str) -> anyhow::Result<()> {
    let core = IngestCore::open(db)?;
    println!("{}", core.count(table)?);
    Ok(())
}
