//! `duckspout-loadgen` — the journaling load generator (§8.4, D-5).
//!
//! A fleet member, not a bystander: it keeps the same per-node NDJSON
//! journal shape the nodes keep, and it is the **only** process that
//! journals `TraceEvent::ClientTimeout` (§3.7) — a timeout is a client-side
//! observation, so only the client may witness it; a node journaling one
//! would be inventing evidence. Its journal joins the fleet's in the
//! judge's verdict (§8.4), which is how client-visible loss or a broken ack
//! promise is convicted rather than averaged away.
//!
//! Sends real OTLP/gRPC `Export` batches to `--target` (`client`), races
//! each ack against `--ack-timeout-ms` (`outcome`), and journals the
//! resolution with payload identity (`journal`).
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.4 and §3.7).

#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use duckspout_loadgen::client::{connect, request_id, send_and_journal};
use duckspout_loadgen::journal::LoadgenJournal;
use duckspout_loadgen::outcome::RequestResolution;
use duckspout_types::NodeId;

/// CTK load generator (§8.4): journaling OTLP client fleet member.
#[derive(Debug, Parser)]
#[command(name = "duckspout-loadgen", version, about)]
struct Cli {
    /// This fleet member's node id in the journals.
    #[arg(long, default_value = "loadgen-0")]
    node_id: String,

    /// Target accept endpoint (OTLP/gRPC), e.g. `http://127.0.0.1:4317`.
    #[arg(long, default_value = "http://127.0.0.1:4317")]
    target: String,

    /// Tenant to send every batch as.
    #[arg(long, default_value = "loadgen")]
    tenant: String,

    /// Log records per batch.
    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    /// Concurrent in-flight batches.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Ack deadline in milliseconds; a batch with no ack inside it is
    /// journaled `ClientTimeout` (§3.7, §8.4).
    #[arg(long, default_value_t = 5_000)]
    ack_timeout_ms: u64,

    /// Stop after sending this many batches. Unset: bounded only by
    /// `--duration-secs`.
    #[arg(long)]
    requests: Option<u64>,

    /// Stop after this many seconds. Defaults to 60s when `--requests` is
    /// also unset, so a bare invocation is a bounded smoke run rather than
    /// an unbounded one needing a signal to stop.
    #[arg(long)]
    duration_secs: Option<u64>,

    /// NDJSON journal file to append to (created if absent).
    #[arg(long)]
    journal_path: PathBuf,
}

/// Counts each [`RequestResolution`] the run produced, for the summary line
/// printed at exit — the journal file is the durable record; this is only
/// an operator-facing tally.
#[derive(Default)]
struct Stats {
    acked: AtomicU64,
    timed_out: AtomicU64,
    failed: AtomicU64,
}

impl Stats {
    fn record(&self, resolution: RequestResolution) {
        let counter = match resolution {
            RequestResolution::Acked => &self.acked,
            RequestResolution::TimedOut => &self.timed_out,
            RequestResolution::Failed => &self.failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self) -> String {
        format!(
            "acked {}, timed_out {}, failed {}",
            self.acked.load(Ordering::Relaxed),
            self.timed_out.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?
        .block_on(run(cli))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let node = NodeId::new(cli.node_id.clone());
    let concurrency = cli.concurrency.max(1);
    let duration_secs = cli.duration_secs.or(if cli.requests.is_none() {
        Some(60)
    } else {
        None
    });

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cli.journal_path)
        .with_context(|| format!("opening journal path {}", cli.journal_path.display()))?;
    let journal = Arc::new(LoadgenJournal::new(node.clone(), file));

    let client = connect(&cli.target)
        .await
        .with_context(|| format!("connecting to {}", cli.target))?;

    let ack_timeout = Duration::from_millis(cli.ack_timeout_ms);
    let deadline = duration_secs.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let stats = Arc::new(Stats::default());

    let mut tasks = tokio::task::JoinSet::new();
    let mut sent: u64 = 0;
    loop {
        let hit_request_limit = cli.requests.is_some_and(|limit| sent >= limit);
        let hit_deadline = deadline.is_some_and(|d| tokio::time::Instant::now() >= d);
        if hit_request_limit || hit_deadline {
            break;
        }

        if tasks.len() >= concurrency {
            tasks.join_next().await;
        }

        let mut client = client.clone();
        let journal = Arc::clone(&journal);
        let stats = Arc::clone(&stats);
        let tenant = cli.tenant.clone();
        let batch_size = cli.batch_size;
        let first_index = sent * batch_size as u64;
        let id = request_id(&node, sent);
        tasks.spawn(async move {
            let resolution = send_and_journal(
                &mut client,
                &journal,
                &tenant,
                id,
                batch_size,
                first_index,
                ack_timeout,
            )
            .await;
            stats.record(resolution);
        });
        sent += 1;
    }
    while tasks.join_next().await.is_some() {}

    eprintln!(
        "duckspout-loadgen ({node}): sent {sent} batch(es), {}",
        stats.summary()
    );
    Ok(())
}
