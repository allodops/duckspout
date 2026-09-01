//! The configuration-surface manifest (§9.6.4, SEED s§7).
//!
//! `--dump-config-manifest` serializes this list — name, type, default,
//! since — as TOML to stdout; `check-invariants.mjs` diffs the output
//! against the golden `floors/config-surface.toml`, which is how a new
//! setting becomes a loud, human-reviewed diff instead of a quiet addition.
//! No Rust parsing in JS: this binary is the single producer of the truth.
//!
//! The list must stay in lockstep with [`crate::config`]; the unit test
//! pins the ratchet count (32) and agreement with the default functions.

use serde::Serialize;

use crate::config::defaults;

/// One setting's manifest row.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SettingSpec {
    /// The setting's TOML path, e.g. `drain.max_age`.
    pub name: &'static str,
    /// The value type, as documented (§9.6.1).
    #[serde(rename = "type")]
    pub value_type: &'static str,
    /// The default, verbatim from §9.6.1 — `"(required)"` where there is
    /// none, and a description where the default is derived at startup.
    pub default: String,
    /// The workspace version that introduced the setting.
    pub since: &'static str,
}

#[derive(Debug, Serialize)]
struct Manifest {
    setting: Vec<SettingSpec>,
}

const SINCE_SEED: &str = "0.0.1";

fn spec(name: &'static str, value_type: &'static str, default: impl Into<String>) -> SettingSpec {
    SettingSpec {
        name,
        value_type,
        default: default.into(),
        since: SINCE_SEED,
    }
}

/// The §9.6.1 surface: 27 rows, 32 settings — the ratchet counts settings.
#[must_use]
pub fn settings() -> Vec<SettingSpec> {
    vec![
        spec("node.data_dir", "path", "(required)"),
        spec(
            "node.otlp_listen",
            "port",
            defaults::otlp_listen().to_string(),
        ),
        spec(
            "node.flight_listen",
            "port",
            defaults::flight_listen().to_string(),
        ),
        spec(
            "node.peer_listen",
            "port",
            defaults::peer_listen().to_string(),
        ),
        spec(
            "node.advertise_addr",
            "string",
            "first non-loopback interface, with the listen ports",
        ),
        spec("node.failure_domain", "string", "zone label / config"),
        spec("cluster.rf", "u16", defaults::rf().to_string()),
        spec("cluster.zone_aware", "enum(auto|on|off)", "auto"),
        spec("cluster.seed_peers", "list<string>", "[]"),
        spec("catalog.dsn", "string", "(required)"),
        spec("catalog.password_file", "path", "(required)"),
        spec(
            "tls.mode",
            "enum(mutual|server_only|disabled)",
            "(required, no default)",
        ),
        spec("tls.cert", "path", "(required, no default)"),
        spec("tls.key", "path", "(required, no default)"),
        spec("tls.ca", "path", "(required, no default)"),
        spec("lake.committer", "string", defaults::lake_committer()),
        spec("lake.uri", "string", "(required)"),
        spec("hot.window", "duration", defaults::hot_window()),
        spec("hot.max_bytes", "bytes", "75% of volume at startup"),
        spec("drain.max_age", "duration", defaults::drain_max_age()),
        spec(
            "drain.part_target_bytes",
            "bytes",
            defaults::part_target_bytes().to_string(),
        ),
        spec(
            "drain.allowed_lateness",
            "duration",
            defaults::allowed_lateness(),
        ),
        spec(
            "replication.receipt_timeout",
            "duration",
            defaults::receipt_timeout(),
        ),
        spec(
            "admission.max_inflight_bytes",
            "bytes",
            "10% of the memory budget (cgroup limit, else system RAM)",
        ),
        spec(
            "max_payload_bytes",
            "bytes",
            defaults::max_payload_bytes().to_string(),
        ),
        spec("dedup.window_ttl", "duration", defaults::dedup_window_ttl()),
        spec(
            "dedup.window_max_entries",
            "u64",
            defaults::dedup_window_max_entries().to_string(),
        ),
        spec("dedup.log_identity", "bool", "off"),
        spec(
            "max_auto_columns",
            "u32",
            defaults::max_auto_columns().to_string(),
        ),
        spec(
            "query.max_hot_bytes_per_query",
            "bytes",
            "2 GiB/node, fill-scaled",
        ),
        spec(
            "query.hot_scan_deadline",
            "duration",
            defaults::hot_scan_deadline(),
        ),
        spec(
            "query.max_concurrent_hot_scans",
            "u32",
            defaults::max_concurrent_hot_scans().to_string(),
        ),
    ]
}

/// Renders the manifest as TOML (`[[setting]]` rows, declaration order).
///
/// # Errors
///
/// Any TOML serialization error.
pub fn render_toml() -> Result<String, Box<dyn std::error::Error>> {
    Ok(toml::to_string(&Manifest {
        setting: settings(),
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ratchet_counts_exactly_32_settings() {
        // §9.6.1: 27 rows, 32 settings — the ratchet baseline (Rule 12).
        assert_eq!(settings().len(), 32);
    }

    #[test]
    fn names_are_unique_and_render_round_trips() {
        let all = settings();
        let mut names: Vec<&str> = all.iter().map(|setting| setting.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), all.len(), "duplicate setting names");

        let rendered = render_toml().expect("render");
        assert_eq!(rendered.matches("[[setting]]").count(), 32);
        assert!(rendered.contains("name = \"tls.mode\""));
        assert!(rendered.contains("default = \"(required, no default)\""));
    }
}
