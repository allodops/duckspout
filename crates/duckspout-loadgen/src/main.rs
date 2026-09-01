//! `duckspout-loadgen` — the journaling load generator (§8.4, D-5).
//!
//! A fleet member, not a bystander: it keeps the same per-node NDJSON
//! journal the nodes keep, and it is the **only** process that journals
//! [`TraceEvent::ClientTimeout`] (§3.7) — a timeout is a client-side
//! observation, so only the client may witness it; a node journaling one
//! would be inventing evidence. Its journal joins the fleet's in the judge's
//! verdict (§8.4), which is how client-visible loss or a broken ack promise
//! is convicted rather than averaged away.
//!
//! Ⓢ clap skeleton at bootstrap; generation lands at v0.2 (arming-ledger
//! row `ctk-distributed`).
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.4 and §3.7).

#![forbid(unsafe_code)]

use clap::Parser;
use duckspout_types::TraceEvent;

/// CTK load generator (§8.4): journaling OTLP client fleet member.
#[derive(Debug, Parser)]
#[command(name = "duckspout-loadgen", version, about)]
struct Cli {
    /// This fleet member's node id in the journals.
    #[arg(long, default_value = "loadgen-0")]
    node_id: String,

    /// Target accept endpoint (OTLP/gRPC).
    #[arg(long)]
    target: Option<String>,
}

fn main() {
    let cli = Cli::parse();
    eprintln!(
        "duckspout-loadgen ({}): not yet implemented (lands at v0.2, §8.4); \
         will journal {:?} on client-observed timeouts",
        cli.node_id,
        TraceEvent::ClientTimeout
    );
    std::process::exit(2);
}
