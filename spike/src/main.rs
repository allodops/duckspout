//! Spike binary — issue #23: DuckDB embed + commit-latency ballpark (§4.2,
//! ADR-0002/0003). Throwaway by design; deleted at v0.1 (spike/README.md).
//!
//! Scope note (owner ruling 2026-09-01): durability re-verification (kill -9,
//! fsync granularity probing) is descoped — DuckDB's documented WAL/checkpoint
//! behavior is trusted. What remains: the working embed and capacity data.
//!
//! Issue #24 adds the OTLP accept path (§4.1): a tonic gRPC LogsService
//! writing through the ingest core, ack after commit.
//! Issue #25 adds the drain leg (§6.2/§6.4): seal → atomic DuckLake commit.
//! Issue #28 adds the pinning probes (§7.6): drain commits and read-only
//! attaches runnable as a REAL second OS process, for the cross-process
//! catalog-snapshot tests in `tests/pinning.rs`.
//!
//! Subcommands:
//!   bench <db-dir>        commit-latency distribution per batch size
//!   count <db> <table>    print row count (reopen path)
//!   serve <db> <addr>     OTLP/gRPC logs endpoint into the hot table
//!   otlp-bench <db-dir> <batches> <batch-rows>
//!                         in-process end-to-end throughput ballpark
//!   drain <dir> <rows>    seal a synthetic hot window, commit it to a local
//!                         DuckLake with the watermark riding the same txn
//!   pin-commit <lake-dir> <window-id>
//!                         one drain-shaped {add files + watermark} commit
//!                         against the sqlite-catalog lake in <lake-dir>
//!   pin-attach <lake-dir> <sqlite|file>
//!                         read-only attach + one observation; prints
//!                         "rows=N watermark=W" or fails

use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use spike::ingest::{IngestCore, LogRow};
use spike::otlp::{HotWriter, OtlpLogsService};

const TABLE: &str = "hot_w0";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let strs: Vec<&str> = args.iter().map(String::as_str).collect();
    let r = match strs.as_slice() {
        ["bench", dir] => bench(Path::new(dir)),
        ["count", db, table] => count(Path::new(db), table),
        ["serve", db, addr] => serve(Path::new(db), addr),
        ["flight", db, addr] => flight(Path::new(db), addr),
        ["otlp-bench", dir, batches, rows] => otlp_bench(
            Path::new(dir),
            batches.parse().unwrap(),
            rows.parse().unwrap(),
        ),
        ["drain", dir, rows] => drain(Path::new(dir), rows.parse().unwrap()),
        ["pin-commit", dir, window] => pin_commit(Path::new(dir), window.parse().unwrap()),
        ["pin-attach", dir, kind] => pin_attach(Path::new(dir), kind),
        _ => {
            eprintln!(
                "usage: spike bench <db-dir> | count <db> <table> | serve <db> <addr> | \
                 flight <db> <addr> | otlp-bench <db-dir> <batches> <batch-rows> | \
                 drain <dir> <rows> | pin-commit <lake-dir> <window-id> | \
                 pin-attach <lake-dir> <sqlite|file>"
            );
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

/// OTLP/gRPC logs endpoint into the hot table (§4.1 accept path, spike-grade).
fn serve(db: &Path, addr: &str) -> anyhow::Result<()> {
    let writer = Arc::new(Mutex::new(HotWriter::open(db, TABLE)?));
    let addr: std::net::SocketAddr = addr.parse()?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        eprintln!("spike otlp: listening on {addr}, db {}", db.display());
        tonic::transport::Server::builder()
            .add_service(OtlpLogsService::new(writer).into_server())
            .serve(addr)
            .await
    })?;
    Ok(())
}

/// Arrow Flight endpoint over the hot table (§7.4 serve leg, issue #26):
/// ticket = SQL, result streamed back as Arrow record batches.
fn flight(db: &Path, addr: &str) -> anyhow::Result<()> {
    let core = Arc::new(Mutex::new(IngestCore::open(db)?));
    let addr: std::net::SocketAddr = addr.parse()?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        eprintln!("spike flight: listening on {addr}, db {}", db.display());
        tonic::transport::Server::builder()
            .add_service(spike::flight::HotFlightService::new(core).into_server())
            .serve(addr)
            .await
    })?;
    Ok(())
}

/// One drain-shaped commit from THIS process against the sqlite-catalog lake
/// in `dir` — the "drain keeps committing" half of the cross-process pinning
/// test (issue #28), run as a genuinely separate OS process.
fn pin_commit(dir: &Path, window: i64) -> anyhow::Result<()> {
    let lake = spike::pinning::Lake::new(dir, spike::pinning::CatalogKind::Sqlite)?;
    let writer = spike::pinning::DrainWriter::open(&lake)?;
    writer.commit_window(window)?;
    println!("pin-commit: window {window} committed");
    Ok(())
}

/// Read-only attach + one observation from THIS process — probes whether a
/// second OS process can attach the given catalog kind at all (issue #28).
fn pin_attach(dir: &Path, kind: &str) -> anyhow::Result<()> {
    let kind = match kind {
        "sqlite" => spike::pinning::CatalogKind::Sqlite,
        "file" => spike::pinning::CatalogKind::DuckdbFile,
        other => anyhow::bail!("unknown catalog kind {other:?}"),
    };
    let lake = spike::pinning::Lake::new(dir, kind)?;
    let reader = spike::pinning::ClientReader::open(&lake)?;
    let obs = reader.observe()?;
    println!("pin-attach: rows={} watermark={}", obs.rows, obs.watermark);
    Ok(())
}

/// End-to-end throughput ballpark: in-process server on an ephemeral port,
/// a real gRPC client sending `batches` × `batch_rows` log records, ack
/// awaited per batch (sequential — one in-flight export, the collector's
/// default posture), then a count check against the table.
fn otlp_bench(dir: &Path, batches: usize, batch_rows: usize) -> anyhow::Result<()> {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    std::fs::create_dir_all(dir)?;
    let db = dir.join("otlp_bench.db");
    let _ = std::fs::remove_file(&db);
    let writer = Arc::new(Mutex::new(HotWriter::open(&db, TABLE)?));
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let svc = OtlpLogsService::new(Arc::clone(&writer)).into_server();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let mut client = LogsServiceClient::connect(format!("http://{addr}")).await?;
        // Warm-up batch (connection setup, first-table costs) — not counted.
        client.export(spike::otlp::synthetic_request(8)).await?;
        let t0 = std::time::Instant::now();
        for _ in 0..batches {
            let resp = client
                .export(spike::otlp::synthetic_request(batch_rows))
                .await?;
            anyhow::ensure!(
                resp.into_inner().partial_success.is_none(),
                "unexpected partial"
            );
        }
        let elapsed = t0.elapsed();
        let total = batches * batch_rows;
        let committed = {
            let w = writer.lock().unwrap();
            w.count()?
        };
        println!(
            "otlp-bench: {batches} batches x {batch_rows} rows in {elapsed:.2?} \
             => {:.0} records/s (acked); table rows={committed} (incl. warm-up 8)",
            total as f64 / elapsed.as_secs_f64()
        );
        anyhow::ensure!(committed == (total + 8) as i64, "count mismatch");
        Ok(())
    })
}

/// Drain-leg latency ballpark (issue #25): synthetic hot window of `rows`,
/// one sorted seal COPY, one atomic {add files + watermark} DuckLake commit.
fn drain(dir: &Path, rows: usize) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    let hot = dir.join("hot.db");
    let lake = dir.join("lake");
    let _ = std::fs::remove_file(&hot);
    let _ = std::fs::remove_dir_all(&lake);
    {
        let mut core = IngestCore::open(&hot)?;
        core.create_window(TABLE)?;
        let batch: Vec<LogRow> = (0..rows as i64).map(LogRow::synthetic).collect();
        core.insert_batch(TABLE, &batch)?;
    } // close hot before the drain opens it
    let core = spike::drain::DrainCore::open(&hot, &lake)?;
    let part = lake.join("data").join("w0-part0.parquet");
    std::fs::create_dir_all(part.parent().unwrap())?;
    let seal = core.seal_part(TABLE, &part)?;
    let req = spike::drain::CommitRequest {
        partition: "tenant-a/logs/p0".to_string(),
        window_id: 0,
        part: part.clone(),
        complete_through_micros: 1_756_600_000_000_000 + rows as i64,
        rows: rows as i64,
    };
    let commit = core.lake_commit(&req)?;
    println!(
        "drain: rows={} seal_copy={:.1?} part={}B commit_writes={:.1?} commit_txn={:.1?}",
        seal.rows,
        seal.copy,
        std::fs::metadata(&part)?.len(),
        commit.writes,
        commit.commit
    );
    println!(
        "lake rows={} watermark={:?} files={:?}",
        core.lake_row_count()?,
        core.read_watermark(&req.partition, 0)?,
        core.registered_files()?
    );
    Ok(())
}
