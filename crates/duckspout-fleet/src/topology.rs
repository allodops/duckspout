//! Node provisioning (§8.4, issue #201): computes each fleet member's
//! identity, ports, and directory layout, and renders its `duckspout-daemon`
//! TOML config (§9.6.1) as plain text.
//!
//! Deliberately plain-text rendering, not `toml::to_string` over
//! [`duckspout_daemon::config::DaemonConfig`]: that type derives
//! `Deserialize` only (the daemon never serializes its own config), and
//! adding a `Serialize` derive purely for this crate's convenience would be
//! a wider, less-obviously-safe change to the config surface than rendering
//! the handful of fields a fleet member actually needs.
//! `tests::rendered_config_round_trips_through_the_real_loader` below is
//! what keeps this text in lockstep with the real parser instead (a plain
//! code span, not a doc link: `#[cfg(test)]` items don't exist in a
//! `cargo doc` build, so an intra-doc link to one never resolves) — it
//! feeds the rendered string through
//! [`duckspout_daemon::config::load`] and asserts the loaded struct, so a
//! future config-surface change that this file's hand-rolled TOML silently
//! drifted from fails a fleet test, not just a production boot.

use std::fmt::Write as _;
use std::path::PathBuf;

use duckspout_daemon::system::DUCKSPOUT_NODE_HOSTNAME_OVERRIDE;

/// Where a fleet member's lake storage lands, chosen from `--local-lake`
/// by [`crate::build_plan`].
#[derive(Debug, Clone)]
pub enum LakeStorage {
    /// `lake.uri` is a local filesystem directory shared by every node in
    /// the fleet — the `--local-lake` escape hatch for a dev box with no
    /// `MinIO` running. Not the default: §8.4 calls for **real** `MinIO`.
    Local { dir: PathBuf },
    /// `lake.uri` is `s3://{bucket}/{prefix}`, resolved against a real
    /// `MinIO` (or other S3-compatible) endpoint — the default topology.
    S3 {
        endpoint: String,
        bucket: String,
        prefix: String,
        region: String,
        access_key_id: String,
        secret_access_key_file: PathBuf,
    },
}

/// Every setting shared by every node in one fleet run (the postgres
/// catalog and the lake are shared — a real cluster has ONE lake, not one
/// per node, `wiring.rs::open_lake`'s module docs on why a shared Postgres
/// catalog is exactly what `multi_process: false` already supports).
#[derive(Debug, Clone)]
pub struct FleetPlan {
    pub postgres_dsn: String,
    pub postgres_password_file: PathBuf,
    pub lake: LakeStorage,
    pub rf: u16,
    pub hot_window: String,
    pub allowed_lateness: String,
}

/// One provisioned fleet member's identity, ports, and files.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    pub index: u16,
    /// This node's `DUCKSPOUT_NODE_HOSTNAME` value — also the host half of
    /// every OTHER node's `cluster.seed_peers` entry naming it
    /// (`wiring.rs::seed_peer_node_id`'s exactness requirement: every peer
    /// must present, character-for-character, the hostname its own seed
    /// entry names it by).
    pub name: String,
    pub otlp_port: u16,
    pub flight_port: u16,
    pub peer_port: u16,
    pub status_port: u16,
    pub data_dir: PathBuf,
    pub config_path: PathBuf,
    pub journal_path: PathBuf,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
}

impl NodeSpec {
    /// The address this node's OTLP/gRPC listener binds — always loopback:
    /// `wiring.rs::bind_listener` always binds `0.0.0.0`, so every
    /// co-located fleet member is reachable on `127.0.0.1` regardless of
    /// `node.advertise_addr` (which v0.1 never reads for binding).
    #[must_use]
    pub fn otlp_addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.otlp_port)
    }

    /// This node's `/status` disclosure address (§9.3.2).
    #[must_use]
    pub fn status_addr(&self) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], self.status_port))
    }
}

/// Computes every node's identity/ports/paths (`node_name` matches
/// `wiring.rs::seed_peer_node_id`'s expectations exactly) and creates the
/// per-node data directory. Does not write config files —
/// [`render_node_config`] does that once [`FleetPlan`] is known.
///
/// # Errors
///
/// Any I/O error creating a node's data directory.
pub fn provision_nodes(
    work_dir: &std::path::Path,
    seed: u64,
    count: u16,
    otlp_base_port: u16,
    flight_base_port: u16,
    peer_base_port: u16,
    status_base_port: u16,
) -> std::io::Result<Vec<NodeSpec>> {
    let mut nodes = Vec::with_capacity(count as usize);
    for index in 0..count {
        let node_dir = work_dir.join(format!("node-{index}"));
        let data_dir = node_dir.join("data");
        std::fs::create_dir_all(&data_dir)?;
        nodes.push(NodeSpec {
            index,
            name: node_name(seed, index),
            otlp_port: otlp_base_port + index,
            flight_port: flight_base_port + index,
            peer_port: peer_base_port + index,
            status_port: status_base_port + index,
            data_dir,
            config_path: node_dir.join("config.toml"),
            journal_path: node_dir.join("journal.ndjson"),
            stdout_path: node_dir.join("stdout.log"),
            stderr_path: node_dir.join("stderr.log"),
        });
    }
    Ok(nodes)
}

/// This fleet run's deterministic node-name scheme: `fleet-{seed}-{index}`.
/// A bare hostname (no colons, no dots) so `wiring.rs::strip_seed_peer_port`
/// -shaped parsing never has to distinguish it from a port suffix — same
/// reasoning as `system::detect_node_id`'s real kernel-hostname case.
#[must_use]
pub fn node_name(seed: u64, index: u16) -> String {
    format!("fleet-{seed}-{index}")
}

/// Per-node deviations from the fleet-wide [`FleetPlan`]: which backend
/// addresses THIS node dials, and which of its own knobs a fault has tuned.
/// Every field `None` means "exactly the plan", which is every node in a run
/// with no fault armed against it — so an unfaulted run's rendered config is
/// bit-for-bit what it was before any of this existed.
///
/// Renamed from `BackendOverrides` by #207: with `hot_window` here it is no
/// longer only about backends, and a struct whose name contradicts one of
/// its fields is how a reader learns to stop trusting the names.
#[derive(Debug, Clone, Default)]
pub struct NodeOverrides {
    /// Replaces [`FleetPlan::postgres_dsn`]'s host/port for this node
    /// (`crate::main`'s `rewrite_postgres_dsn_host_port`) — set when a
    /// [`crate::link::FaultLink`] sits in front of its catalog (#204).
    pub postgres_dsn: Option<String>,
    /// Replaces the `lake.s3_endpoint` this node dials — irrelevant under
    /// [`LakeStorage::Local`], which has no network at all.
    pub s3_endpoint: Option<String>,
    /// Replaces [`FleetPlan::hot_window`] for this node alone (#207's
    /// cache-churn fault). Shortening one node's `hot.window` is what makes
    /// its windows seal, drain and `DropWindow` densely enough for a read to
    /// actually race one — the same "boot exactly the target node with the
    /// knob the fault needs" shape `--fault-kill-mid-drain` already uses for
    /// `--fault-drain-commit-delay-ms` (`crate::fault`'s module docs).
    ///
    /// It is an existing, ratcheted §9.6.1 setting (`hot.window`), not a new
    /// knob: the fault tunes the daemon's real drain cadence rather than
    /// asking it for a back door.
    pub hot_window: Option<String>,
}

/// Renders `node`'s complete `duckspout-daemon` TOML config against `plan`
/// and the fleet's other members (for `cluster.seed_peers`) — module docs
/// on why this is hand-rolled text rather than a `Serialize`d struct.
/// `overrides` applies this one node's deviations from the plan — fault-link
/// catalog/lake addresses, and a fault-shortened `hot.window`
/// ([`NodeOverrides`]).
#[must_use]
#[allow(clippy::unnecessary_debug_formatting)] // `{:?}` quotes/escapes paths for TOML string literals — `.display()` would emit them unquoted and invalid
pub fn render_node_config(
    plan: &FleetPlan,
    node: &NodeSpec,
    all_nodes: &[NodeSpec],
    overrides: &NodeOverrides,
) -> String {
    let seed_peers: Vec<String> = all_nodes
        .iter()
        .filter(|peer| peer.index != node.index)
        .map(|peer| format!("\"{}:{}\"", peer.name, peer.peer_port))
        .collect();

    // tls.mode = "disabled" makes tls.cert/key/ca unread at v0.1
    // (`config.rs`'s `TlsConfig` doc comment; no listener in `wiring.rs`
    // ever consults them) — required struct fields with no default, but
    // never dereferenced, so a placeholder path that need not exist is
    // enough to satisfy the loader.
    let tls_placeholder = node.data_dir.join("tls-unused.pem");

    let mut out = format!(
        "[node]\n\
         data_dir = {data_dir:?}\n\
         otlp_listen = {otlp}\n\
         flight_listen = {flight}\n\
         peer_listen = {peer}\n\
         \n\
         [cluster]\n\
         rf = {rf}\n\
         seed_peers = [{seed_peers}]\n\
         \n\
         [catalog]\n\
         dsn = {dsn:?}\n\
         password_file = {password_file:?}\n\
         \n\
         [tls]\n\
         mode = \"disabled\"\n\
         cert = {tls:?}\n\
         key = {tls:?}\n\
         ca = {tls:?}\n\
         \n\
         [hot]\n\
         window = {hot_window:?}\n\
         \n\
         [drain]\n\
         allowed_lateness = {allowed_lateness:?}\n",
        data_dir = node.data_dir,
        otlp = node.otlp_port,
        flight = node.flight_port,
        peer = node.peer_port,
        rf = plan.rf,
        seed_peers = seed_peers.join(", "),
        dsn = overrides
            .postgres_dsn
            .as_deref()
            .unwrap_or(&plan.postgres_dsn),
        password_file = plan.postgres_password_file,
        tls = tls_placeholder,
        hot_window = overrides.hot_window.as_deref().unwrap_or(&plan.hot_window),
        allowed_lateness = plan.allowed_lateness,
    );

    out.push_str("\n[lake]\n");
    // write! into the same String rather than format!+push_str (avoids the
    // extra intermediate allocation clippy's format_push_string flags).
    match &plan.lake {
        LakeStorage::Local { dir } => {
            let _ = writeln!(out, "uri = {dir:?}");
        }
        LakeStorage::S3 {
            endpoint,
            bucket,
            prefix,
            region,
            access_key_id,
            secret_access_key_file,
        } => {
            let uri = format!("s3://{bucket}/{prefix}");
            let endpoint = overrides.s3_endpoint.as_deref().unwrap_or(endpoint);
            let _ = writeln!(
                out,
                "uri = {uri:?}\n\
                 s3_endpoint = {endpoint:?}\n\
                 s3_region = {region:?}\n\
                 s3_access_key_id = {access_key_id:?}\n\
                 s3_secret_access_key_file = {secret_access_key_file:?}",
            );
        }
    }
    out
}

/// This process's own env-var key for [`std::process::Command::env`] — one
/// source of truth with `duckspout-daemon`'s override, rather than a
/// hand-copied string literal drifting from it.
#[must_use]
pub fn node_hostname_env_key() -> &'static str {
    DUCKSPOUT_NODE_HOSTNAME_OVERRIDE
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_with_local_lake(dir: PathBuf, password_file: PathBuf) -> FleetPlan {
        FleetPlan {
            postgres_dsn: "postgres://duckspout@127.0.0.1:5432/duckspout_catalog".to_owned(),
            postgres_password_file: password_file,
            lake: LakeStorage::Local { dir },
            rf: 2,
            hot_window: "5s".to_owned(),
            allowed_lateness: "1s".to_owned(),
        }
    }

    /// The whole point of hand-rolling TOML text (module docs): it must
    /// load through the REAL parser into the REAL struct with the values
    /// this crate computed, not just "look like TOML."
    #[test]
    fn rendered_config_round_trips_through_the_real_loader() {
        let tmp = tempfile_dir();
        let password_file = tmp.join("pg-password");
        std::fs::write(&password_file, "duckspout-dev").unwrap();
        let nodes = provision_nodes(&tmp, 42, 3, 14317, 18815, 17946, 19095).unwrap();
        let plan = plan_with_local_lake(tmp.join("lake"), password_file);

        for node in &nodes {
            let rendered = render_node_config(&plan, node, &nodes, &NodeOverrides::default());
            std::fs::write(&node.config_path, &rendered).unwrap();
            let loaded = duckspout_daemon::config::load(Some(&node.config_path))
                .unwrap_or_else(|e| panic!("node {}: {e}\n---\n{rendered}", node.index));

            assert_eq!(loaded.node.otlp_listen, node.otlp_port);
            assert_eq!(loaded.node.flight_listen, node.flight_port);
            assert_eq!(loaded.node.peer_listen, node.peer_port);
            assert_eq!(loaded.cluster.rf, 2);
            assert_eq!(loaded.cluster.seed_peers.len(), nodes.len() - 1);
            for peer in &nodes {
                if peer.index == node.index {
                    continue;
                }
                let expected = format!("{}:{}", peer.name, peer.peer_port);
                assert!(
                    loaded.cluster.seed_peers.contains(&expected),
                    "node {} missing peer entry {expected:?} in {:?}",
                    node.index,
                    loaded.cluster.seed_peers
                );
            }
            assert_eq!(loaded.hot.window, "5s");
            assert_eq!(loaded.drain.allowed_lateness, "1s");
            assert!(loaded.lake.s3_endpoint.is_none());
        }
    }

    /// The `s3://` shape the daemon's own `open_lake`/`build_s3_access`
    /// (`wiring.rs`, issue #201) requires: `lake.uri` starting with
    /// `s3://` paired with `lake.s3_endpoint` set.
    #[test]
    fn rendered_s3_lake_config_round_trips_and_agrees_with_the_uri_scheme() {
        let tmp = tempfile_dir();
        let password_file = tmp.join("pg-password");
        std::fs::write(&password_file, "duckspout-dev").unwrap();
        let secret_file = tmp.join("s3-secret");
        std::fs::write(&secret_file, "duckspout-dev").unwrap();
        let nodes = provision_nodes(&tmp, 7, 1, 14317, 18815, 17946, 19095).unwrap();
        let plan = FleetPlan {
            lake: LakeStorage::S3 {
                endpoint: "127.0.0.1:9000".to_owned(),
                bucket: "duckspout-fleet".to_owned(),
                prefix: "duckspout-fleet".to_owned(),
                region: "us-east-1".to_owned(),
                access_key_id: "duckspout".to_owned(),
                secret_access_key_file: secret_file,
            },
            ..plan_with_local_lake(tmp.join("unused"), password_file)
        };
        let rendered = render_node_config(&plan, &nodes[0], &nodes, &NodeOverrides::default());
        std::fs::write(&nodes[0].config_path, &rendered).unwrap();
        let loaded = duckspout_daemon::config::load(Some(&nodes[0].config_path))
            .unwrap_or_else(|e| panic!("{e}\n---\n{rendered}"));
        assert_eq!(loaded.lake.uri, "s3://duckspout-fleet/duckspout-fleet");
        assert_eq!(loaded.lake.s3_endpoint.as_deref(), Some("127.0.0.1:9000"));
        assert_eq!(loaded.lake.s3_access_key_id.as_deref(), Some("duckspout"));
        assert!(loaded.lake.s3_secret_access_key_file.is_some());
    }

    /// The whole point of [`NodeOverrides`] (issue #204): a node whose
    /// catalog and lake are reached through fault links must have THOSE
    /// addresses in its real config — proven through the real loader, so a
    /// rendering that silently kept the direct address (leaving every
    /// network fault a no-op against a node that never traverses the link)
    /// fails here rather than in a fleet run's post-mortem.
    #[test]
    fn backend_overrides_redirect_the_catalog_and_lake_through_the_fault_links() {
        let tmp = tempfile_dir();
        let password_file = tmp.join("pg-password");
        std::fs::write(&password_file, "duckspout-dev").unwrap();
        let secret_file = tmp.join("s3-secret");
        std::fs::write(&secret_file, "duckspout-dev").unwrap();
        let nodes = provision_nodes(&tmp, 11, 1, 14317, 18815, 17946, 19095).unwrap();
        let plan = FleetPlan {
            lake: LakeStorage::S3 {
                endpoint: "127.0.0.1:9000".to_owned(),
                bucket: "duckspout-fleet".to_owned(),
                prefix: "duckspout-fleet".to_owned(),
                region: "us-east-1".to_owned(),
                access_key_id: "duckspout".to_owned(),
                secret_access_key_file: secret_file,
            },
            ..plan_with_local_lake(tmp.join("unused"), password_file)
        };
        let overrides = NodeOverrides {
            postgres_dsn: Some("postgres://duckspout@127.0.0.1:34567/duckspout_catalog".to_owned()),
            s3_endpoint: Some("127.0.0.1:34568".to_owned()),
            hot_window: None,
        };

        let rendered = render_node_config(&plan, &nodes[0], &nodes, &overrides);
        std::fs::write(&nodes[0].config_path, &rendered).unwrap();
        let loaded = duckspout_daemon::config::load(Some(&nodes[0].config_path))
            .unwrap_or_else(|e| panic!("{e}\n---\n{rendered}"));
        assert_eq!(
            loaded.catalog.dsn,
            "postgres://duckspout@127.0.0.1:34567/duckspout_catalog"
        );
        assert_eq!(loaded.lake.s3_endpoint.as_deref(), Some("127.0.0.1:34568"));
        // The plan's own window, unoverridden — the baseline the next test
        // varies exactly one field away from.
        assert_eq!(loaded.hot.window, "5s");
    }

    /// The cache-churn fault's forcing mechanism, checked against the REAL
    /// config loader (#207): a per-node `hot.window` override must actually
    /// reach `hot.window` in the loaded struct, and must not disturb the
    /// backend addresses beside it. Would catch an override that rendered
    /// into the wrong section, or one silently ignored in favour of the
    /// fleet-wide plan — either of which would leave the churn fault firing
    /// at the plan's cadence and, quite possibly, journaling no residency
    /// action at all.
    #[test]
    fn a_per_node_hot_window_override_reaches_the_real_loader() {
        let tmp = tempfile_dir();
        let password_file = tmp.join("pg-password-hot-window");
        std::fs::write(&password_file, "duckspout-dev").unwrap();
        let nodes = provision_nodes(&tmp, 12, 1, 14317, 18815, 17946, 19095).unwrap();
        let plan = plan_with_local_lake(tmp.join("lake"), password_file);
        let overrides = NodeOverrides {
            hot_window: Some("250ms".to_owned()),
            ..NodeOverrides::default()
        };

        let rendered = render_node_config(&plan, &nodes[0], &nodes, &overrides);
        std::fs::write(&nodes[0].config_path, &rendered).unwrap();
        let loaded = duckspout_daemon::config::load(Some(&nodes[0].config_path))
            .unwrap_or_else(|e| panic!("{e}\n---\n{rendered}"));
        assert_eq!(loaded.hot.window, "250ms");
        assert_eq!(loaded.catalog.dsn, plan.postgres_dsn);
    }

    #[test]
    fn node_names_are_deterministic_and_distinct_within_a_seed() {
        let names: Vec<String> = (0..5).map(|i| node_name(9, i)).collect();
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len());
        assert_eq!(node_name(9, 0), "fleet-9-0");
        // Reproducible: the same seed replays the same names (CLI doc
        // comment on `--seed`).
        assert_eq!(node_name(9, 2), node_name(9, 2));
    }

    fn tempfile_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-topology-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
