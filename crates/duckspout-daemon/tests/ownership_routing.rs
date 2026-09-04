//! HRW ring integration + ownership routing, wired end to end through the
//! public daemon API (issue #52): a real [`Daemon::boot`] reads
//! `cluster.seed_peers`/`cluster.rf` from config, builds the
//! [`duckspout_replication::routing::MembershipView`], and
//! [`duckspout_daemon::wiring::DaemonHandle::routing_plan`] resolves real
//! ownership-routing decisions against it — proving the composition (config
//! → membership → routing decision) actually runs, not just the pure
//! `duckspout-replication` unit tests in isolation.
//!
//! Two shapes are exercised:
//! - a single-node deployment (`cluster.seed_peers = []`, v0.1's only fully
//!   supported topology): every partition must route locally, matching
//!   today's pre-#52 behavior exactly (the regression guard: wiring
//!   ownership routing in must never turn a working single-node daemon into
//!   one that thinks it should forward somewhere unreachable);
//! - a declared multi-node membership (`cluster.seed_peers` non-empty, no
//!   peer actually running — `wiring.rs`'s own module docs explain why
//!   actually Forwarding is still out of scope): the daemon's composed
//!   `routing_plan` must agree, partition for partition, with
//!   `duckspout_replication::routing::route_write` computed independently
//!   over the same candidate set — the wiring adds no logic of its own
//!   beyond assembling the [`MembershipView`], so it must not disagree with
//!   the crate that owns the decision.

use std::path::PathBuf;

use duckspout_daemon::wiring::Daemon;
use duckspout_replication::routing::{MembershipView, route_write};
use duckspout_types::{NodeId, PartitionId};

/// Writes a minimal §9.6 config, with an explicit `[cluster]` block, rooted
/// at `root` — a fresh temp dir. `seed_peers` is the caller's own
/// `cluster.seed_peers` TOML array literal (already quoted/bracketed),
/// spliced in verbatim so both tests below can share this one writer.
fn write_config(root: &std::path::Path, rf: u16, seed_peers_toml: &str) -> PathBuf {
    let hot_dir = root.join("hot");
    let catalog_path = root.join("catalog.ducklake");
    let data_path = root.join("lake-data");
    std::fs::create_dir_all(&hot_dir).unwrap();

    let toml = format!(
        r#"
[node]
data_dir = "{hot_dir}"
otlp_listen = 0
flight_listen = 0
peer_listen = 0

[cluster]
rf = {rf}
seed_peers = {seed_peers_toml}

[catalog]
dsn = "{catalog_path}"
password_file = "{unused}"

[tls]
mode = "disabled"
cert = "{unused}"
key = "{unused}"
ca = "{unused}"

[lake]
uri = "{data_path}"

[admission]
max_inflight_bytes = 67108864
"#,
        hot_dir = hot_dir.display(),
        catalog_path = catalog_path.display(),
        data_path = data_path.display(),
        unused = root.join("unused").display(),
    );
    let config_path = root.join("daemon.toml");
    std::fs::write(&config_path, toml).unwrap();
    config_path
}

fn partitions() -> Vec<PartitionId> {
    (0..12).map(|i| PartitionId::new(format!("p{i}"))).collect()
}

/// A single-node deployment (`cluster.seed_peers = []`) always routes every
/// partition to itself — HRW over a one-candidate membership has exactly
/// one possible owner and replica set, so this is both a correctness check
/// and the "ownership routing didn't regress v0.1's only supported
/// topology" guard. Would catch a membership builder that somehow excludes
/// self, or a `routing_plan` that reports non-ownership on a solo node.
#[tokio::test(flavor = "multi_thread")]
async fn single_node_deployment_always_routes_locally() {
    let root = tempfile::tempdir().unwrap();
    let config_path = write_config(root.path(), 2, "[]");
    let config = duckspout_daemon::config::load(Some(&config_path)).unwrap();

    let daemon = Daemon::boot(&config, 0).await.expect("daemon boots");
    let handle = daemon.handle();

    for partition in partitions() {
        let plan = handle
            .routing_plan(&partition)
            .expect("nonempty membership");
        assert!(
            plan.is_local_owner,
            "{partition}: a lone node must own every partition"
        );
        assert_eq!(plan.owner, *handle.node_id());
        assert_eq!(plan.replicas, vec![handle.node_id().clone()]);
        assert!(plan.forward_targets(handle.node_id()).is_empty());
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(daemon.serve(async {
        let _ = shutdown_rx.await;
    }));
    shutdown_tx.send(()).unwrap();
    serve_task.await.unwrap();
}

/// A declared multi-node membership (`cluster.seed_peers` non-empty): the
/// daemon's composed [`duckspout_daemon::wiring::DaemonHandle::routing_plan`]
/// must agree exactly, for every partition, with
/// [`route_write`] computed independently over the same candidate set built
/// by hand here — proving `Daemon::boot` really reads `cluster.seed_peers`
/// into the [`MembershipView`] it resolves against, not a hardcoded or
/// empty one. Also asserts both a local-owner and a forwarded case are
/// actually reached, so the test cannot pass vacuously.
#[tokio::test(flavor = "multi_thread")]
async fn multi_node_membership_matches_an_independently_computed_routing_plan() {
    let root = tempfile::tempdir().unwrap();
    let config_path = write_config(
        root.path(),
        2,
        r#"["peer-a:7946", "peer-b:7946", "peer-a:7946"]"#,
    );
    let config = duckspout_daemon::config::load(Some(&config_path)).unwrap();

    let daemon = Daemon::boot(&config, 0).await.expect("daemon boots");
    let handle = daemon.handle();
    let self_node = handle.node_id().clone();

    // The same candidate set `build_membership_view` should have produced:
    // self plus the two DISTINCT seed peers (module docs: a duplicate seed
    // entry folds rather than doubling a node's odds).
    let expected_candidates = vec![
        self_node.clone(),
        NodeId::new("peer-a/1"),
        NodeId::new("peer-b/1"),
    ];
    let independent_view = MembershipView::new(expected_candidates);

    let mut saw_local_owner = false;
    let mut saw_forward = false;
    for partition in partitions() {
        let plan = handle
            .routing_plan(&partition)
            .expect("nonempty membership");
        let expected =
            route_write(&partition, &self_node, &independent_view, 2).expect("nonempty membership");
        assert_eq!(
            plan, expected,
            "{partition}: daemon-composed plan disagrees with an independently computed one"
        );
        assert_eq!(
            plan.replicas.len(),
            2,
            "{partition}: RF=2 must yield 2 replicas"
        );
        if plan.is_local_owner {
            saw_local_owner = true;
        } else {
            saw_forward = true;
            assert!(plan.forward_targets(&self_node).contains(&plan.owner));
        }
    }
    assert!(
        saw_local_owner,
        "vacuous: self never won ownership of any partition"
    );
    assert!(
        saw_forward,
        "vacuous: self was never a non-owner for any partition"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(daemon.serve(async {
        let _ = shutdown_rx.await;
    }));
    shutdown_tx.send(()).unwrap();
    serve_task.await.unwrap();
}
