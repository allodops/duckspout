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
use serde::Serialize;

/// CTK load generator (§8.4): journaling OTLP client fleet member.
#[derive(Debug, Parser)]
#[command(name = "duckspout-loadgen", version, about)]
struct Cli {
    /// This fleet member's node id in the journals. MUST be unique across
    /// every loadgen instance in a fleet run — two members left at the
    /// default would collide on D-6's per-node keying (their journal lines
    /// and request ids would be indistinguishable). The default is a
    /// single-instance convenience, not a safe multi-member default.
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

    /// NDJSON journal file to create. MUST NOT already exist (ACPR finding
    /// HIGH-2): recovering the per-node seq counter and the request-id
    /// sequence safely across two invocations sharing one path would need a
    /// durable record of every request *sent* (not just resolved) — which
    /// the frozen §3.3 trace vocabulary has no room for today
    /// (`duckspout_loadgen::journal` module docs) — so instead of a recovery
    /// path that can silently under-recover and reuse a request id, this
    /// loadgen refuses to start against an existing journal path. A fleet
    /// runner restarting a crashed loadgen member should pass a fresh path
    /// per attempt (e.g. suffix the attempt number).
    #[arg(long)]
    journal_path: PathBuf,
}

/// Counts each [`RequestResolution`] the run produced. Printed at exit for
/// an operator, and also written out as the durable `{journal_path}.summary
/// .json` artifact (`write_summary`) — the journal file only carries
/// `ClientAck`/`ClientTimeout`, so `Rejected` and `Ambiguous` outcomes
/// (`outcome` module docs) would otherwise leave no durable trace at all,
/// which is exactly the vacuity gap ACPR finding MEDIUM-HIGH-4 raised.
#[derive(Default)]
struct Stats {
    acked: AtomicU64,
    timed_out: AtomicU64,
    rejected: AtomicU64,
    ambiguous: AtomicU64,
}

impl Stats {
    fn record(&self, resolution: RequestResolution) {
        let counter = match resolution {
            RequestResolution::Acked => &self.acked,
            RequestResolution::TimedOut => &self.timed_out,
            RequestResolution::Rejected => &self.rejected,
            RequestResolution::Ambiguous => &self.ambiguous,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            acked: self.acked.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            ambiguous: self.ambiguous.load(Ordering::Relaxed),
        }
    }
}

/// A `Stats` snapshot, also the `{journal_path}.summary.json` shape
/// (`write_summary`).
#[derive(Serialize)]
struct StatsSnapshot {
    acked: u64,
    timed_out: u64,
    rejected: u64,
    ambiguous: u64,
}

impl StatsSnapshot {
    fn total(&self) -> u64 {
        self.acked + self.timed_out + self.rejected + self.ambiguous
    }
}

/// The durable, non-journal run summary (module docs, `Stats`): every
/// [`RequestResolution`] this run produced, plus `sent` so a reader can see
/// at a glance whether the run completed cleanly (`sent == total`) or ended
/// with sent batches this loadgen never resolved at all (`sent > total` — a
/// spawned send task panicked without recording an outcome; caught here
/// rather than silently undercounting). A process killed outright (SIGKILL)
/// never reaches `write_summary` at all — this file's plain *absence*, or
/// staleness, is that case's signal (§8.4's vacuity-teeth case), the same
/// way a node's journal simply stopping is. Deliberately a sibling file, not
/// a new line shape inside the frozen NDJSON journal
/// (`duckspout_loadgen::journal` module docs).
#[derive(Serialize)]
struct RunSummary<'a> {
    node: &'a str,
    sent: u64,
    #[serde(flatten)]
    resolved: StatsSnapshot,
}

fn write_summary(journal_path: &std::path::Path, node: &str, sent: u64, resolved: StatsSnapshot) {
    let mut summary_path = journal_path.as_os_str().to_owned();
    summary_path.push(".summary.json");
    let summary = RunSummary {
        node,
        sent,
        resolved,
    };
    let text = serde_json::to_string_pretty(&summary).expect("run summary serializes");
    if let Err(err) = std::fs::write(&summary_path, text) {
        // Advisory artifact: losing it does not corrupt the journal or the
        // run itself, so this warns rather than failing the process (unlike
        // the journal writer's own fail-loud contract, R-3, which protects
        // the frozen §3.3 evidence stream this is deliberately not part of).
        eprintln!(
            "duckspout-loadgen: warning: could not write run summary {}: {err}",
            PathBuf::from(summary_path).display()
        );
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
        .create_new(true)
        .append(true)
        .open(&cli.journal_path)
        .with_context(|| {
            format!(
                "opening journal path {} (must not already exist — Cli::journal_path docs)",
                cli.journal_path.display()
            )
        })?;
    let journal = Arc::new(LoadgenJournal::new(node.clone(), file));

    let client = connect(&cli.target)
        .await
        .with_context(|| format!("connecting to {}", cli.target))?;

    let ack_timeout = Duration::from_millis(cli.ack_timeout_ms);
    let deadline = duration_secs.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let stats = Arc::new(Stats::default());
    // Captured once, not per request (`client::request_id` docs, HIGH-2):
    // makes a restart under the same `--node-id` mint fresh request ids
    // instead of reusing `{node}-0`, `{node}-1`, ... from scratch.
    let start_nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let mut tasks = tokio::task::JoinSet::new();
    let mut sent: u64 = 0;
    loop {
        let hit_request_limit = cli.requests.is_some_and(|limit| sent >= limit);
        let hit_deadline = deadline.is_some_and(|d| tokio::time::Instant::now() >= d);
        if hit_request_limit || hit_deadline {
            break;
        }

        if tasks.len() >= concurrency {
            // Race the wait for a free slot against the deadline too
            // (rather than an unconditional `tasks.join_next().await`) so a
            // slow/hung batch cannot make `--duration-secs` overshoot by up
            // to `--ack-timeout-ms` merely because the deadline check below
            // never got a turn to run.
            match deadline {
                Some(d) => {
                    tokio::select! {
                        _ = tasks.join_next() => {}
                        () = tokio::time::sleep_until(d) => {}
                    }
                }
                None => {
                    tasks.join_next().await;
                }
            }
            continue;
        }

        let mut client = client.clone();
        let journal = Arc::clone(&journal);
        let stats = Arc::clone(&stats);
        let tenant = cli.tenant.clone();
        let batch_size = cli.batch_size;
        let first_index = sent * batch_size as u64;
        let id = request_id(&node, start_nonce, sent);
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

    let snapshot = stats.snapshot();
    let unresolved = sent.saturating_sub(snapshot.total());
    let warning = if unresolved == 0 {
        String::new()
    } else {
        format!(" (WARNING: {unresolved} sent batch(es) never resolved at all)")
    };
    eprintln!(
        "duckspout-loadgen ({node}): sent {sent} batch(es), acked {}, timed_out {}, \
         rejected {}, ambiguous {}{warning}",
        snapshot.acked, snapshot.timed_out, snapshot.rejected, snapshot.ambiguous,
    );
    write_summary(&cli.journal_path, node.as_str(), sent, snapshot);
    Ok(())
}
