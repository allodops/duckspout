//! `duckspoutctl` — the operator CLI (§9).
//!
//! Subcommands:
//! - `size` (§9.2, Ⓢ stub): per-node volume provisioning, `hot.max_bytes`,
//!   the ladder thresholds, and the stall runway from
//!   `pvc = (rate × expansion × residency × RF ÷ nodes + staging) ÷ 0.6`.
//! - `status` (§9.3, R-9; issue #38): pretty-prints the disclosed
//!   `NodeStatus` — `NodeId`, the overload rung, watermark per partition,
//!   `drain_stalled` — read from a running daemon's observation listener
//!   over one plain HTTP GET.
//!
//! Design home: `docs/operations.md` (§9.2, §9.3).

#![forbid(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::time::Duration;

use clap::{Parser, Subcommand};

/// The daemon's observation-listener default port
/// (`duckspout-daemon/src/constants.rs::OBSERVATION_LISTEN_PORT_DEFAULT`) —
/// not a §9.6.1 config knob (R-12), so it is duplicated here as a literal
/// rather than a cross-crate dependency (ctl stays a leaf, §10.1); `--addr`
/// overrides it for a non-default bind.
const DEFAULT_STATUS_ADDR: &str = "127.0.0.1:9095";

/// Operator CLI for a `DuckSpout` cluster (§9).
#[derive(Debug, Parser)]
#[command(name = "duckspoutctl", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Size a node volume (§9.2): pvc = (rate × expansion × residency × RF
    /// ÷ nodes + staging) ÷ 0.6, plus the derived `hot.max_bytes` and ladder
    /// thresholds.
    Size {
        /// Cluster-wide wire ingest, bytes/s.
        #[arg(long)]
        rate_bytes_per_sec: u64,
        /// Wire→hot representation factor (measure; start at 1.4).
        #[arg(long, default_value_t = 1.4)]
        expansion: f64,
        /// Drain-stall budget, seconds (floor: `drain.max_age`; production
        /// recommendation ≥ 7200).
        #[arg(long)]
        residency_secs: u64,
        /// Replication factor (`cluster.rf`).
        #[arg(long, default_value_t = 2)]
        rf: u16,
        /// Node count.
        #[arg(long)]
        nodes: u16,
        /// Fixed per-node scratch, bytes (start at 20 GB).
        #[arg(long, default_value_t = 20_000_000_000)]
        staging_bytes: u64,
    },
    /// Read and print a node's disclosed status (§9.3, R-9): `NodeId`, the
    /// overload rung, watermark per partition, `drain_stalled`.
    Status {
        /// The daemon's observation listener, `host:port`.
        #[arg(long, default_value = DEFAULT_STATUS_ADDR)]
        addr: String,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Size { .. } => {
            eprintln!("duckspoutctl size: not yet implemented (lands at v0.1, §9.2)");
            std::process::exit(2);
        }
        Command::Status { addr } => {
            if let Err(error) = print_status(&addr) {
                eprintln!("duckspoutctl status: {error}");
                std::process::exit(1);
            }
        }
    }
}

/// Fetches `GET /status` from `addr` and pretty-prints the JSON body.
///
/// # Errors
///
/// Any connect/I/O failure, or a body that is not valid JSON.
fn print_status(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(
        format!("GET /status HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
    )?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let response = String::from_utf8_lossy(&raw);
    let body = response
        .split_once("\r\n\r\n")
        .map_or(response.as_ref(), |(_, body)| body);

    let value: serde_json::Value = serde_json::from_str(body)?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
