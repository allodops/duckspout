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

/// ACPR #196 LOW-8: deterministically picks two seed-peer host names such
/// that, given THIS process's own real `detect_node_id` (the daemon's
/// actual identity, read from the real kernel hostname — no injection seam
/// exists to mock it, and adding one just for this test would be more
/// machinery than the finding warrants), the fixed `partitions()` set below
/// yields both a local-owner and a forwarded case.
///
/// Before this fix, the test hardcoded `["peer-a:7946", "peer-b:7946"]` and
/// asserted `saw_local_owner`/`saw_forward` were both true — true for the
/// overwhelming majority of hostnames, but a real (if rare, ~1-in-6500 by
/// the reviewer's estimate) hostname would make `self` lose (or win) every
/// one of the 12 fixed partitions against those two specific fixed peer
/// names, failing the "vacuous" assertion in a way that reads as "ownership
/// routing is broken" when it is actually just an unlucky hash draw for
/// whichever machine happens to run the suite. Searching over candidate
/// peer names AFTER learning the real `self_node` (using the same
/// `hrw_owner` math production uses, computed ahead of booting the daemon)
/// makes the outcome deterministic regardless of the runtime hostname,
/// while still exercising the real assertion this test cares about: the
/// daemon's composed `routing_plan` must agree with an independently
/// computed one, for a genuinely-reached local-owner AND a genuinely-reached
/// forwarded case.
fn peers_exercising_both_ownership_cases(
    self_node: &NodeId,
    partitions: &[PartitionId],
) -> (String, String) {
    for i in 0..1000u32 {
        let peer_a = format!("peer-search-{i}-a");
        let peer_b = format!("peer-search-{i}-b");
        let candidates = vec![
            self_node.clone(),
            NodeId::new(format!("{peer_a}/1")),
            NodeId::new(format!("{peer_b}/1")),
        ];
        let mut saw_owner = false;
        let mut saw_forward = false;
        for partition in partitions {
            match duckspout_replication::hrw_owner(partition, &candidates) {
                Some(owner) if owner == self_node => saw_owner = true,
                Some(_) => saw_forward = true,
                None => {}
            }
        }
        if saw_owner && saw_forward {
            return (peer_a, peer_b);
        }
    }
    panic!("could not find peer host names exercising both ownership cases in 1000 tries");
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

    let daemon = Daemon::boot(&config, 0, None).await.expect("daemon boots");
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

    // LOW-8: choose peer names deterministically against this process's
    // real identity (module docs of `peers_exercising_both_ownership_cases`)
    // rather than hardcoding names that could, for an unlucky real
    // hostname, make this test's own vacuity guards fail misleadingly.
    let self_node =
        duckspout_daemon::system::detect_node_id(duckspout_daemon::system::V01_FIXED_INCARNATION);
    let parts = partitions();
    let (peer_a, peer_b) = peers_exercising_both_ownership_cases(&self_node, &parts);

    let config_path = write_config(
        root.path(),
        2,
        &format!(r#"["{peer_a}:7946", "{peer_b}:7946", "{peer_a}:7946"]"#),
    );
    let config = duckspout_daemon::config::load(Some(&config_path)).unwrap();

    let daemon = Daemon::boot(&config, 0, None).await.expect("daemon boots");
    let handle = daemon.handle();
    assert_eq!(
        handle.node_id(),
        &self_node,
        "detect_node_id must be deterministic across calls"
    );

    // The same candidate set `build_membership_view` should have produced:
    // self plus the two DISTINCT seed peers (module docs: a duplicate seed
    // entry folds rather than doubling a node's odds).
    let expected_candidates = vec![
        self_node.clone(),
        NodeId::new(format!("{peer_a}/1")),
        NodeId::new(format!("{peer_b}/1")),
    ];
    let independent_view = MembershipView::new(expected_candidates);

    let mut saw_local_owner = false;
    let mut saw_forward = false;
    for partition in &parts {
        let plan = handle.routing_plan(partition).expect("nonempty membership");
        let expected =
            route_write(partition, &self_node, &independent_view, 2).expect("nonempty membership");
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
        "self never won ownership of any partition — peers_exercising_both_ownership_cases \
         guarantees this deterministically, so a failure here is a real regression"
    );
    assert!(
        saw_forward,
        "self was never a non-owner for any partition — peers_exercising_both_ownership_cases \
         guarantees this deterministically, so a failure here is a real regression"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(daemon.serve(async {
        let _ = shutdown_rx.await;
    }));
    shutdown_tx.send(()).unwrap();
    serve_task.await.unwrap();
}

/// ACPR #196 HIGH-2 scratch-repro re-verification: `cluster.rf` is actually
/// read from config in `assemble_core`, not silently hardcoded to the
/// default (2). Confirmed via the reviewer's own executed mutation:
/// replacing `rf: config.cluster.rf` with the literal `rf: 2` in
/// `assemble_core` (`wiring.rs`) left every OTHER test in this file
/// passing, because both used `rf = 2` — exactly the default
/// (`config::defaults::rf()`), so nothing distinguished "read from config"
/// from "hardcoded." This test uses `rf = 3` against 4 candidates: the
/// daemon can only produce a 3-replica plan if it genuinely reads
/// `cluster.rf`, and is written to FAIL under that exact mutation (which
/// would instead produce 2).
#[tokio::test(flavor = "multi_thread")]
async fn a_non_default_rf_is_actually_read_from_config() {
    let root = tempfile::tempdir().unwrap();
    let config_path = write_config(
        root.path(),
        3,
        r#"["peer-rf-a:7946", "peer-rf-b:7946", "peer-rf-c:7946"]"#,
    );
    let config = duckspout_daemon::config::load(Some(&config_path)).unwrap();

    let daemon = Daemon::boot(&config, 0, None).await.expect("daemon boots");
    let handle = daemon.handle();
    let self_node = handle.node_id().clone();

    let independent_view = MembershipView::new(vec![
        self_node.clone(),
        NodeId::new("peer-rf-a/1"),
        NodeId::new("peer-rf-b/1"),
        NodeId::new("peer-rf-c/1"),
    ]);

    for partition in partitions() {
        let plan = handle
            .routing_plan(&partition)
            .expect("nonempty membership");
        assert_eq!(
            plan.rf, 3,
            "{partition}: RoutingPlan.rf must reflect cluster.rf = 3, not the default 2"
        );
        assert_eq!(
            plan.replicas.len(),
            3,
            "{partition}: rf=3 with 4 candidates must yield 3 replicas, not the rf=2 default \
             (this is the mutation's exact teeth: hardcoding rf: 2 makes this 2)"
        );
        let expected =
            route_write(&partition, &self_node, &independent_view, 3).expect("nonempty membership");
        assert_eq!(
            plan, expected,
            "{partition}: daemon-composed plan disagrees with an independently computed one at rf=3"
        );
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(daemon.serve(async {
        let _ = shutdown_rx.await;
    }));
    shutdown_tx.send(()).unwrap();
    serve_task.await.unwrap();
}
