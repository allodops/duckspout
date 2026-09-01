//! The node configuration surface (§9.6.1): 27 rows, **32 settings** — the
//! §9.6.4 ratchet baseline. This module IS the config surface; everything
//! not here is a fixed constant in [`crate::constants`] (§9.6.3).
//!
//! One TOML file, environment-variable overrides (`DUCKSPOUT__…`), secrets
//! by file path. Any new setting requires a divergent-workload justification
//! measured against the 32-setting count, and shows up as a loud diff of
//! `--dump-config-manifest` against `floors/config-surface.toml`.
//!
//! Duration-valued settings are carried as strings (`"60s"`, `"30m"`, `"24h"`)
//! at bootstrap; typed parsing lands with the wiring (v0.1) without changing
//! the surface.

// Justification for the allow: the 32-setting surface is complete at
// bootstrap (SEED s§4) while its reader — the wiring — lands at v0.1;
// deleting unread fields would shrink the ratcheted surface.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// TLS posture (§9.5). **Deliberately no `Default` and no default value in
/// the manifest**: an operator states the security posture explicitly or the
/// daemon refuses to start — a silently defaulted posture is the §9.5
/// anti-pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// Mutual TLS: clients present certificates; tenant identity rides the
    /// verified connection (§4.1.2).
    Mutual,
    /// Server-side TLS only.
    ServerOnly,
    /// No TLS — explicitly stated, never assumed (loopback and test rigs).
    Disabled,
}

/// Zone-aware placement (§9.1.1): auto-detected, with a boolean escape hatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ZoneAware {
    /// Detect failure domains and place replicas across them when possible.
    #[default]
    Auto,
    /// Force zone-aware placement on.
    On,
    /// Force it off.
    Off,
}

/// `node.*` — identity and listeners.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    /// `node.data_dir` — required; deployment-specific.
    pub data_dir: PathBuf,
    /// `node.otlp_listen` — default 4317 (the OTLP/gRPC convention).
    #[serde(default = "defaults::otlp_listen")]
    pub otlp_listen: u16,
    /// `node.flight_listen` — default 8815 (the Arrow Flight convention).
    #[serde(default = "defaults::flight_listen")]
    pub flight_listen: u16,
    /// `node.peer_listen` — default 7946.
    #[serde(default = "defaults::peer_listen")]
    pub peer_listen: u16,
    /// `node.advertise_addr` — default: first non-loopback interface, with
    /// the listen ports (NAT, K8s).
    #[serde(default)]
    pub advertise_addr: Option<String>,
    /// `node.failure_domain` — zone label / config; non-K8s has no downward
    /// API.
    #[serde(default)]
    pub failure_domain: Option<String>,
}

/// `cluster.*` — membership and durability posture.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// `cluster.rf` — default 2; durability vs cost is a real tradeoff.
    pub rf: u16,
    /// `cluster.zone_aware` — default auto (§9.1.1).
    pub zone_aware: ZoneAware,
    /// `cluster.seed_peers` — default `[]`; non-K8s bootstrap (§9.1.3).
    pub seed_peers: Vec<String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            rf: defaults::rf(),
            zone_aware: ZoneAware::Auto,
            seed_peers: Vec::new(),
        }
    }
}

/// `catalog.*` — 2 settings (§9.6.1 bundles them in one row).
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogConfig {
    /// `catalog.dsn` — required; deployment-specific.
    pub dsn: String,
    /// `catalog.password_file` — required; secrets by file path (§9.6).
    pub password_file: PathBuf,
}

/// `tls.*` — 4 settings, all required, none defaulted (§9.5).
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// `tls.mode` — required; see [`TlsMode`].
    pub mode: TlsMode,
    /// `tls.cert` — embedder-supplied PEM path.
    pub cert: PathBuf,
    /// `tls.key` — embedder-supplied PEM path.
    pub key: PathBuf,
    /// `tls.ca` — embedder-supplied PEM path.
    pub ca: PathBuf,
}

/// `lake.*` — 2 settings (§6: Iceberg-by-contract).
#[derive(Debug, Clone, Deserialize)]
pub struct LakeConfig {
    /// `lake.committer` — default `ducklake`.
    #[serde(default = "defaults::lake_committer")]
    pub committer: String,
    /// `lake.uri` — required; deployment-specific.
    pub uri: String,
}

/// `hot.*` — the hot tier's two knobs.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HotConfig {
    /// `hot.window` — default 60s; latency vs table-count tradeoff.
    pub window: String,
    /// `hot.max_bytes` — default: 75% of volume at startup (`None` = that
    /// autodetection). The disk budget; the only configured byte number;
    /// bounds staging+cache combined (§4.5).
    pub max_bytes: Option<u64>,
}

impl Default for HotConfig {
    fn default() -> Self {
        Self {
            window: defaults::hot_window(),
            max_bytes: None,
        }
    }
}

/// `drain.*` — seal and lateness policy (§6).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DrainConfig {
    /// `drain.max_age` — default 30m; seal latency vs part size (§6.2).
    pub max_age: String,
    /// `drain.part_target_bytes` — default 384 MiB (recommended band
    /// 256–512 MiB); object-store economics diverge (§6.2).
    pub part_target_bytes: u64,
    /// `drain.allowed_lateness` — default 15m; workload event-time
    /// discipline diverges (§6.3).
    pub allowed_lateness: String,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            max_age: defaults::drain_max_age(),
            part_target_bytes: defaults::part_target_bytes(),
            allowed_lateness: defaults::allowed_lateness(),
        }
    }
}

/// `replication.*` — the ring's one knob (§5).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// `replication.receipt_timeout` — default 5s (revisit by measurement);
    /// intra-AZ vs WAN latency diverges.
    pub receipt_timeout: String,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            receipt_timeout: defaults::receipt_timeout(),
        }
    }
}

/// `admission.*` (§4.6).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AdmissionConfig {
    /// `admission.max_inflight_bytes` — default: 10% of the memory budget
    /// (cgroup limit, else system RAM — autodetected, §4.6; `None` = that
    /// autodetection).
    pub max_inflight_bytes: Option<u64>,
}

/// `dedup.*` — the accept-node dedup window (§4.4.1).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DedupConfig {
    /// `dedup.window_ttl` — default 24h; must dominate the deployed retry
    /// horizon (§4).
    pub window_ttl: String,
    /// `dedup.window_max_entries` — default 100k; burst-rate divergence.
    pub window_max_entries: u64,
    /// `dedup.log_identity` — default off; false-drop history in the field:
    /// opt-in only (§4.4.2).
    pub log_identity: bool,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            window_ttl: defaults::dedup_window_ttl(),
            window_max_entries: defaults::dedup_window_max_entries(),
            log_identity: false,
        }
    }
}

/// `query.*` — hot-scan governance (§7).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    /// `query.max_hot_bytes_per_query` — default 2 GiB/node, fill-scaled
    /// (`None` = that default); hot sizing is workload-derived.
    pub max_hot_bytes_per_query: Option<u64>,
    /// `query.hot_scan_deadline` — default 30s; the real backstop.
    pub hot_scan_deadline: String,
    /// `query.max_concurrent_hot_scans` — default 8; node sizing diverges.
    pub max_concurrent_hot_scans: u32,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_hot_bytes_per_query: None,
            hot_scan_deadline: defaults::hot_scan_deadline(),
            max_concurrent_hot_scans: defaults::max_concurrent_hot_scans(),
        }
    }
}

/// The complete node configuration: §9.6.1's 27 rows, 32 settings.
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
    /// `node.*`.
    pub node: NodeConfig,
    /// `cluster.*`.
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// `catalog.*`.
    pub catalog: CatalogConfig,
    /// `tls.*`.
    pub tls: TlsConfig,
    /// `lake.*`.
    pub lake: LakeConfig,
    /// `hot.*`.
    #[serde(default)]
    pub hot: HotConfig,
    /// `drain.*`.
    #[serde(default)]
    pub drain: DrainConfig,
    /// `replication.*`.
    #[serde(default)]
    pub replication: ReplicationConfig,
    /// `admission.*`.
    #[serde(default)]
    pub admission: AdmissionConfig,
    /// `dedup.*`.
    #[serde(default)]
    pub dedup: DedupConfig,
    /// `query.*`.
    #[serde(default)]
    pub query: QueryConfig,
    /// `max_payload_bytes` — default 4 MiB; ecosystem default, edge batching
    /// diverges (§4.6). Over-cap is non-retryable.
    #[serde(default = "defaults::max_payload_bytes")]
    pub max_payload_bytes: u64,
    /// `max_auto_columns` — default 1024; curated schemas vs unbounded raw
    /// keys; overflow spills to JSON, never rejects (§4.8).
    #[serde(default = "defaults::max_auto_columns")]
    pub max_auto_columns: u32,
}

/// Default-value functions; the manifest ([`crate::manifest`]) quotes the
/// same values, so a drift between the two is a one-file diff.
pub mod defaults {
    /// 4317, the OTLP/gRPC convention.
    #[must_use]
    pub fn otlp_listen() -> u16 {
        4317
    }
    /// 8815, the Arrow Flight convention.
    #[must_use]
    pub fn flight_listen() -> u16 {
        8815
    }
    /// 7946.
    #[must_use]
    pub fn peer_listen() -> u16 {
        7946
    }
    /// RF 2.
    #[must_use]
    pub fn rf() -> u16 {
        2
    }
    /// `"ducklake"`.
    #[must_use]
    pub fn lake_committer() -> String {
        "ducklake".to_owned()
    }
    /// `"60s"`.
    #[must_use]
    pub fn hot_window() -> String {
        "60s".to_owned()
    }
    /// `"30m"`.
    #[must_use]
    pub fn drain_max_age() -> String {
        "30m".to_owned()
    }
    /// 384 MiB.
    #[must_use]
    pub fn part_target_bytes() -> u64 {
        384 * 1024 * 1024
    }
    /// `"15m"`.
    #[must_use]
    pub fn allowed_lateness() -> String {
        "15m".to_owned()
    }
    /// `"5s"`.
    #[must_use]
    pub fn receipt_timeout() -> String {
        "5s".to_owned()
    }
    /// `"24h"`.
    #[must_use]
    pub fn dedup_window_ttl() -> String {
        "24h".to_owned()
    }
    /// 100k entries.
    #[must_use]
    pub fn dedup_window_max_entries() -> u64 {
        100_000
    }
    /// `"30s"`.
    #[must_use]
    pub fn hot_scan_deadline() -> String {
        "30s".to_owned()
    }
    /// 8 concurrent hot scans.
    #[must_use]
    pub fn max_concurrent_hot_scans() -> u32 {
        8
    }
    /// 4 MiB.
    #[must_use]
    pub fn max_payload_bytes() -> u64 {
        4 * 1024 * 1024
    }
    /// 1024 auto columns.
    #[must_use]
    pub fn max_auto_columns() -> u32 {
        1024
    }
}

/// Loads the configuration: one TOML file plus `DUCKSPOUT__…` environment
/// overrides (§9.6).
///
/// # Errors
///
/// Any [`config`] source or deserialization error — including a missing
/// `tls.mode`, which is required and has no default (§9.5).
pub fn load(path: Option<&Path>) -> Result<DaemonConfig, Box<dyn std::error::Error>> {
    let mut builder = config::Config::builder();
    if let Some(path) = path {
        builder = builder.add_source(config::File::from(path));
    }
    builder = builder.add_source(config::Environment::with_prefix("DUCKSPOUT").separator("__"));
    Ok(builder.build()?.try_deserialize()?)
}
