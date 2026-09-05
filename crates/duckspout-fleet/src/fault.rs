//! Fault injectors (§8.4) — every fault class §8.4's own fault-window list
//! names, across two batches:
//!
//! | Fault | §8.4 wording | Injector | Issue |
//! |---|---|---|---|
//! | Node kill (incl. mid-drain) | "node kills, including the sharpest one" | [`run_node_kill`] | #203 |
//! | Process pause | "SIGSTOP long enough to expire claims, then resume" | [`run_sigstop_pause`] | #203 |
//! | Network partition | "network partitions" | [`run_network_partition`] | #204 |
//! | Asymmetric degradation | "drops, delay, bandwidth caps" | [`run_network_degradation`] | #204 |
//! | Membership churn | "join and leave under load, not only crash" | [`run_membership_join`] / [`run_membership_leave`] | #204 |
//! | Flight-server kill mid-stream | "a hot query's stream dies" | [`run_flight_kill_mid_stream`] | #204 |
//! | Catalog outage | "ingest must continue undegraded; drains stall and disclose" | [`run_catalog_outage`] | #204 |
//! | Discovery flapping | "`ClaimAdvertise`/`Heartbeat` oscillation" | [`run_discovery_flap`] | #204 |
//! | Cache/residency churn | "forced Evict/Demote churn and `DropWindow` racing queries" | [`run_cache_churn`] | #207 |
//!
//! Each injector runs against a real `duckspout-daemon` process spawned by
//! [`crate::process`] and/or a real network link owned by [`crate::link`],
//! and journals its own Armed/Started/Ended lifecycle through
//! [`crate::faultlog::FaultLog`] (§8.4: "each injector keeps its own
//! armed/fired ledger").
//!
//! # The network faults' mechanism
//!
//! [`crate::link`]'s module docs carry the whole argument for the userspace
//! TCP proxy — why not `iptables`/`tc netem`, why a drop resets rather than
//! silently blackholes, and what a proxy cannot reproduce. The injectors
//! here are the schedule and the journal on top of it.
//!
//! # Two disclosed gaps, both pre-existing composition gaps rather than
//! anything these injectors chose to skip
//!
//! `FencedZombie` (the SIGSTOP pause, below) and the routing-convergence
//! half of discovery flapping ([`run_discovery_flap`]) both need daemon
//! composition that does not exist yet — a real `FenceBoot` draw and a
//! concrete `duckspout_types::Registry` respectively. Each injector's own
//! docs spell out exactly which half is real today and which is blocked.
//!
//! # The mid-drain kill's timing (`KillTiming::MidDrainCommit`)
//!
//! §8.4's sharpest node-kill fault is "the partition owner mid-drain,
//! between `PutPart` and `LakeCommit`." Landing a real `SIGKILL` exactly
//! there from OUTSIDE the process, with no synchronization primitive shared
//! with it, cannot be made both real and race-free against however fast the
//! real backend's commit happens to complete — so this crate widens the
//! window instead of guessing at timing: the target node is booted with a
//! non-zero `--fault-drain-commit-delay-ms`
//! (`duckspout_daemon::fault::StallingLakeCommitter`'s own module docs), and
//! this injector watches that node's real NDJSON journal for its first
//! `PutPart` line before firing — as long as the configured stall
//! comfortably exceeds this injector's journal-poll latency (milliseconds),
//! the kill is deterministically inside the window, not a race. See
//! `crates/duckspout-daemon/src/fault.rs` for the daemon-side half of this
//! mechanism and the ACPR-facing safety argument for why it is not a
//! production-weakening change.
//!
//! # The SIGSTOP pause's `FencedZombie` verification gap (documented, not
//! papered over)
//!
//! §8.4's SIGSTOP fault is described as exercising `FencedZombie`: "the
//! paused node's stale incarnation must be rejected." This module's
//! [`run_sigstop_pause`] implements the real OS-level half of that fault —
//! a real `SIGSTOP`, held long enough to exceed
//! `duckspout_daemon::constants::HEARTBEAT_TTL_SECS`, then a real
//! `SIGCONT` — completely for real, against a real process. What it does
//! **not**, and cannot yet, verify against the live fleet: that a peer
//! actually rejects the resumed node's incarnation, because
//! `duckspout-daemon`'s own composition root does not yet wire
//! `duckspout_replication::boot::fence_boot` /
//! `duckspout_replication::fencing::FenceTable` at all —
//! `crates/duckspout-daemon/src/system.rs::V01_FIXED_INCARNATION` is a
//! hardcoded placeholder every node boots with today, and
//! `wiring.rs`'s own module docs disclose separately that no crate in this
//! workspace implements `duckspout_types::Transport` over a real network
//! yet, so nodes never actually Forward/receipt across the wire at all. A
//! paused-then-resumed real daemon in today's fleet therefore has no live
//! incarnation-fencing peer to be rejected BY — there is nothing this
//! injector could observe in the running fleet that `FencedZombie` even
//! applies to yet. This is a pre-existing gap in daemon composition, not
//! something introduced or that should be silently patched over by this
//! issue's fault-injection scope; `crates/duckspout-replication/tests/`
//! carries a real (non-P-model), production-`fence_boot`/`FenceTable`
//! integration test — driven through test doubles for storage/registry, not
//! a mock of `fence_boot`/`FenceTable` themselves — proving the guarantee
//! holds for this exact pause/promote/resume scenario shape, which is the
//! most faithful verification available until that wiring lands. See this
//! issue's PR description for the full accounting of what is
//! real-and-verified here vs. deferred.

use std::time::Duration;

use anyhow::Context as _;
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use duckspout_types::TraceEvent;

use crate::faultlog::{FaultKind, FaultLog};
use crate::link::{FaultLink, LinkConditions, LinkStats};
use crate::process::{self, RunningNode};
use crate::topology::NodeSpec;

/// The RENDERED node-id string `target`'s OWN real D-6 journal uses for its
/// `TraceRecord::node` field — `<fleet-node-name>/<incarnation>`, exactly
/// `duckspout_daemon::system::detect_node_id`'s own formula, given
/// `crate::process::spawn_node` sets `DUCKSPOUT_NODE_HOSTNAME` to the
/// fleet's own `NodeSpec::name` and every node boots under
/// `duckspout_daemon::system::V01_FIXED_INCARNATION` (v0.1: no real
/// `FenceBoot`-minted incarnation yet). An ACPR finding: journaling the
/// fleet's bare `NodeSpec::name` in [`crate::faultlog::FaultWindowLine::target_node`]
/// (the pre-fix behavior) does NOT match what the node calls itself in its
/// own journal, so a future judge could never join the two on this field.
fn rendered_node_id(spec: &NodeSpec) -> String {
    format!(
        "{}/{}",
        spec.name,
        duckspout_daemon::system::V01_FIXED_INCARNATION
    )
}

/// The current number of NDJSON lines in `journal_path` — an exact seq
/// anchor a future judge can use to locate a fault-window phase's
/// wall-clock moment within the target's own seq-ordered D-6 journal
/// (`duckspout_types::TraceRecord` carries no timestamp of its own —
/// `crate::faultlog`'s module docs on why this snapshot lives in the fault
/// log instead of a D-6 vocabulary change). `0` for a journal that does not
/// exist yet (the target hasn't booted, or hasn't written anything yet) —
/// never an error: a fault log must never itself fail the fleet run over
/// this best-effort correlation aid.
fn node_journal_line_count(journal_path: &std::path::Path) -> u64 {
    std::fs::read_to_string(journal_path).map_or(0, |contents| {
        u64::try_from(contents.lines().count()).unwrap_or(u64::MAX)
    })
}

/// When a [`run_node_kill`] fault fires.
#[derive(Debug, Clone, Copy)]
pub enum KillTiming {
    /// Fires after waiting `Duration` of wall-clock time from when the
    /// injector starts running.
    AfterDelay(Duration),
    /// Watches the target's own NDJSON journal for its first `PutPart`
    /// line, then fires immediately (module docs above: paired with the
    /// target node having been booted under a non-zero
    /// `--fault-drain-commit-delay-ms` stall so the kill reliably lands
    /// before `LakeCommit` completes). `journal_poll_timeout` bounds how
    /// long to wait for that line to appear at all — a drive-load pass
    /// that never produces a drainable window would otherwise hang the
    /// fleet run forever.
    MidDrainCommit { journal_poll_timeout: Duration },
}

/// Runs one real node-kill fault against `target`, journaling its full
/// Armed/Started/Ended lifecycle to `log`. Always sends a real `SIGKILL`
/// (never `SIGTERM` — module docs of §8.4's "sharpest" framing: a kill, not
/// a graceful shutdown) and waits up to 10 s to confirm the process actually
/// exited before journaling `Ended`.
///
/// # Errors
///
/// If the `MidDrainCommit` journal watch times out, or the `kill` utility
/// itself cannot be invoked (`crate::process::send_signal`).
pub async fn run_node_kill(
    fault_id: &str,
    target: &mut RunningNode,
    timing: KillTiming,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(&target.spec);
    // The journal's line count AT ARM TIME — an ACPR finding's fix
    // (`wait_for_journal_event`'s own module docs): `MidDrainCommit` below
    // must only fire on a `PutPart` line written AFTER this point, never on
    // one already sitting in the file from before this injector armed.
    let journal_anchor = node_journal_line_count(&target.spec.journal_path);
    log.armed(
        fault_id,
        FaultKind::NodeKill,
        &target_node,
        Some(serde_json::json!({ "node_journal_lines": journal_anchor })),
    );

    match timing {
        KillTiming::AfterDelay(delay) => tokio::time::sleep(delay).await,
        KillTiming::MidDrainCommit {
            journal_poll_timeout,
        } => {
            wait_for_journal_event(
                &target.spec.journal_path,
                "PutPart",
                journal_anchor,
                journal_poll_timeout,
            )
            .await?;
        }
    }

    let pid = process::pid(target);
    process::send_signal(target, "-KILL").await?;
    log.started(
        fault_id,
        FaultKind::NodeKill,
        &target_node,
        Some(serde_json::json!({
            "pid": pid,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    let confirmed_exited = process::wait_exited(target, Duration::from_secs(10)).await;
    log.ended(
        fault_id,
        FaultKind::NodeKill,
        &target_node,
        Some(serde_json::json!({
            "confirmed_exited": confirmed_exited,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );
    if !confirmed_exited {
        anyhow::bail!(
            "node {target_node} did not confirm exit within 10s of SIGKILL (fault {fault_id})"
        );
    }
    Ok(())
}

/// Runs one real `SIGSTOP`→(hold)→`SIGCONT` pause fault against `target`,
/// journaling its full Armed/Started/Ended lifecycle to `log`. Module docs
/// above for exactly what this does and does not verify.
///
/// # Errors
///
/// If either signal cannot be sent (`crate::process::send_signal`); if the
/// `SIGSTOP` cannot be confirmed as a genuine pause within 2 s (an ACPR
/// finding: a target that exited on its own — a zombie — makes
/// `kill -STOP` return success with nothing actually paused, and this must
/// **not** journal a `Started` phase at all, let alone go on to fabricate a
/// `confirmed_resumed: true` — see [`process::is_stopped`]'s and
/// [`process::is_live_running`]'s own module docs for the exact false
/// positive this refusal closes); or if the resume cannot be confirmed as
/// genuinely live within 2 s of the `SIGCONT` (mirroring
/// [`run_node_kill`]'s own fail-closed posture on a failed confirmation).
pub async fn run_sigstop_pause(
    fault_id: &str,
    target: &mut RunningNode,
    delay: Duration,
    pause_duration: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(&target.spec);
    log.armed(
        fault_id,
        FaultKind::SigstopPause,
        &target_node,
        Some(serde_json::json!({
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    let pid = process::pid(target);
    process::send_signal(target, "-STOP").await?;
    let confirmed_stopped = confirm(pid, Duration::from_secs(2), process::is_stopped).await;
    if !confirmed_stopped {
        anyhow::bail!(
            "node {target_node} did not confirm a genuine SIGSTOP within 2s (fault {fault_id}) \
             — refusing to journal a Started phase for a pause that may never have taken effect \
             (e.g. the process had already exited on its own before this injector's SIGSTOP)"
        );
    }
    log.started(
        fault_id,
        FaultKind::SigstopPause,
        &target_node,
        Some(serde_json::json!({
            "pid": pid,
            "confirmed_stopped": true,
            "planned_pause_ms": pause_duration.as_millis(),
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    tokio::time::sleep(pause_duration).await;

    process::send_signal(target, "-CONT").await?;
    let confirmed_resumed = confirm(pid, Duration::from_secs(2), process::is_live_running).await;
    log.ended(
        fault_id,
        FaultKind::SigstopPause,
        &target_node,
        Some(serde_json::json!({
            "confirmed_resumed": confirmed_resumed,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );
    if !confirmed_resumed {
        anyhow::bail!(
            "node {target_node} did not confirm a genuinely live resume within 2s of SIGCONT \
             (fault {fault_id})"
        );
    }
    Ok(())
}

/// Runs one real network-partition fault (§8.4, issue #204): every link in
/// `links` — a node's real ingest, catalog and lake links, whichever the
/// fleet created for it — is cut for `duration`, then restored.
/// [`crate::link`]'s module docs carry the mechanism and its honest
/// boundary; this function is the schedule and the journal.
///
/// The `Ended` line journals each link's own traffic delta ACROSS the
/// window: `bytes_client_to_server`/`bytes_server_to_client` of zero is the
/// partition's own proof it really cut traffic, and
/// `conns_refused`/`conns_cut` above zero is the proof something really
/// tried to cross it (the raw material #208's vacuity teeth need — a fault
/// that armed against a link nothing ever used fired vacuously, and this is
/// what makes that visible after the run).
///
/// # Errors
///
/// Never — a link condition is set through an in-process handle, with no
/// signal to fail to deliver and no OS confirmation to fail closed on
/// (unlike [`run_node_kill`]/[`run_sigstop_pause`]). The `Result` is kept
/// for signature symmetry with the other injectors, which
/// `crate::run_armed_faults` composes uniformly.
pub async fn run_network_partition(
    fault_id: &str,
    target: &NodeSpec,
    links: &[&FaultLink],
    delay: Duration,
    duration: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(target);
    log.armed(
        fault_id,
        FaultKind::NetworkPartition,
        &target_node,
        Some(serde_json::json!({
            "links": link_descriptions(links),
            "planned_duration_ms": duration.as_millis(),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    let before: Vec<LinkStats> = links.iter().map(|link| link.stats()).collect();
    // Held, not assigned: this window lifts only its own conditions when
    // `holds` is dropped below, even if another armed window is holding one
    // of these same links (`crate::link`'s module docs).
    let holds: Vec<_> = links
        .iter()
        .map(|link| link.hold(LinkConditions::dropped()))
        .collect();
    log.started(
        fault_id,
        FaultKind::NetworkPartition,
        &target_node,
        Some(serde_json::json!({
            "links": link_descriptions(links),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(duration).await;

    let during = link_deltas(links, &before);
    drop(holds);
    log.ended(
        fault_id,
        FaultKind::NetworkPartition,
        &target_node,
        Some(serde_json::json!({
            "link_traffic_during_window": during,
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );
    Ok(())
}

/// Runs one real asymmetric-degradation fault (§8.4, issue #204):
/// `conditions` (a per-direction delay and/or bandwidth cap — the asymmetry
/// is the two directions differing) is applied to `link` for `duration`,
/// then lifted.
///
/// # Errors
///
/// Never — same reasoning as [`run_network_partition`]'s own `Errors`
/// section.
pub async fn run_network_degradation(
    fault_id: &str,
    target: &NodeSpec,
    link: &FaultLink,
    conditions: LinkConditions,
    delay: Duration,
    duration: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(target);
    log.armed(
        fault_id,
        FaultKind::NetworkDegradation,
        &target_node,
        Some(serde_json::json!({
            "link": link.label(),
            "conditions": conditions,
            "planned_duration_ms": duration.as_millis(),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    let before = link.stats();
    let hold = link.hold(conditions);
    log.started(
        fault_id,
        FaultKind::NetworkDegradation,
        &target_node,
        Some(serde_json::json!({
            "link": link.label(),
            "conditions": conditions,
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(duration).await;

    let during = link.stats().since(before);
    drop(hold);
    log.ended(
        fault_id,
        FaultKind::NetworkDegradation,
        &target_node,
        Some(serde_json::json!({
            "link": link.label(),
            "link_traffic_during_window": during,
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );
    Ok(())
}

/// Runs one real catalog-outage fault (§8.4, issue #204): `catalog_link` —
/// the node's own real link to the shared Postgres catalog — is cut for
/// `duration`, then restored.
///
/// §8.4's own predicate for this fault class is "ingest must continue
/// undegraded; drains stall and disclose (§4, §9)", so this injector
/// samples the target's own `/status` disclosure (§9.3.2) at both ends of
/// the window and journals `drain_stalled` from each sample. That is
/// evidence for a judge (#208), NOT a verdict: this injector never fails
/// the fleet run over what it observed there (§8.4: the fleet misbehaves
/// freely during the run and is convicted afterward from journals).
///
/// # Why the `/status` sample is taken BEFORE the link changes
///
/// [`observed_drain_stalled`] is bounded but real blocking I/O — up to 2 s
/// against a node that accepts the connection and never answers. A phase's
/// journaled `at_ms` is stamped when [`FaultLog::record`] writes it, so
/// sampling between the real link change and the journal line would put up
/// to 2 s of skew between "when the cut really happened" and "when the
/// journal says it happened" (an ACPR finding on issue #204, MEDIUM-2:
/// measured at ~2002 ms against a `SIGSTOP`ped target). Both phases below
/// therefore sample first, into a local, then change the link, then journal
/// immediately — so `at_ms` is the moment of the real network effect. The
/// sampled disclosure is correspondingly the node's status from just before
/// each edge, which is exactly what "did this node's drain stall across the
/// window" wants to compare.
///
/// # The `Ended` phase is the end of the INJECTED CONDITION, not of the
/// system's degradation
///
/// A cut TCP connection to Postgres does not repair itself when the link
/// comes back: `libpq` (under `DuckLake`'s real `ATTACH`) does not silently
/// reconnect a broken session, so a drain that stalled inside this window
/// may well still be stalled after it. That is a real, disclosable property
/// of the system under test — precisely the kind of thing the post-pass
/// judge exists to rule on — and this injector deliberately does not paper
/// over it by, say, restarting the node to make the window "look" closed.
/// `Ended` means "this injector stopped imposing the condition."
///
/// # Errors
///
/// Never — same reasoning as [`run_network_partition`]'s own `Errors`
/// section. A `/status` sample that cannot be fetched is journaled as
/// `null`, never an error: an unreachable node is itself a finding for the
/// judge, not a reason for the injector to abort the run.
pub async fn run_catalog_outage(
    fault_id: &str,
    target: &NodeSpec,
    catalog_link: &FaultLink,
    delay: Duration,
    duration: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(target);
    log.armed(
        fault_id,
        FaultKind::CatalogOutage,
        &target_node,
        Some(serde_json::json!({
            "link": catalog_link.label(),
            "catalog_upstream": catalog_link.upstream(),
            "planned_duration_ms": duration.as_millis(),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    let before = catalog_link.stats();
    // Sample, THEN cut, THEN journal (module docs above on the skew this
    // ordering closes).
    let drain_stalled_at_start = observed_drain_stalled(target).await;
    let hold = catalog_link.hold(LinkConditions::dropped());
    log.started(
        fault_id,
        FaultKind::CatalogOutage,
        &target_node,
        Some(serde_json::json!({
            "link": catalog_link.label(),
            "drain_stalled": drain_stalled_at_start,
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(duration).await;

    let drain_stalled = observed_drain_stalled(target).await;
    // The traffic delta is read immediately before the link is restored, so
    // it covers exactly the interval the two journaled timestamps bound —
    // not a window that ends 2 s before its own `Ended` line.
    let during = catalog_link.stats().since(before);
    drop(hold);
    log.ended(
        fault_id,
        FaultKind::CatalogOutage,
        &target_node,
        Some(serde_json::json!({
            "link": catalog_link.label(),
            "link_traffic_during_window": during,
            "drain_stalled": drain_stalled,
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );
    Ok(())
}

/// One discovery-flapping fault's oscillation schedule.
#[derive(Debug, Clone, Copy)]
pub struct FlapSchedule {
    /// How many down/up cycles to run. Zero is a vacuous flap, journaled
    /// honestly as `cycles_completed: 0` (§8.4's vacuity teeth), never an
    /// error.
    pub cycles: u32,
    /// How long each cycle holds the link down.
    pub down: Duration,
    /// How long each cycle leaves it up again before the next one.
    pub up: Duration,
}

/// Runs one real discovery-flapping fault (§8.4, issue #204):
/// `catalog_link` — the node's link to the catalog DB that holds the
/// `nodes`/`claims` rows §5.5/§5.7 define — is oscillated down/up for
/// `cycles` cycles.
///
/// # What is real here, and what is blocked (disclosed, not papered over)
///
/// §8.4 words this fault class as "discovery flapping
/// (`ClaimAdvertise`/`Heartbeat` oscillation; routing must converge without
/// ever serving a `complete` answer it cannot prove)". The catalog link IS
/// the right link to oscillate: `duckspout_types::Registry` — the port
/// `ClaimAdvertise` and `FenceBoot`'s incarnation draw both go through — is
/// defined against "the catalog DB's `nodes`/`claims` tables" (its own
/// module docs), so a node whose catalog reachability oscillates is exactly
/// a node whose advertisements and heartbeats land, then don't, then do.
///
/// What is NOT observable in today's fleet is the second half of that
/// sentence, and it is blocked on composition that does not exist yet
/// rather than on anything this issue could implement:
/// `duckspout_types::Registry` has **no concrete implementation at all**
/// (its own module docs: "Home crate: none yet"), so no node writes a claim
/// row or a heartbeat to the catalog today; membership comes entirely from
/// static `cluster.seed_peers` config
/// (`duckspout_daemon::wiring::build_membership_view`), so no routing view
/// can converge or diverge in response to this oscillation at all. What
/// this injector therefore delivers today is the REAL oscillation of the
/// real link — real TCP connections really cut and really restored, on the
/// real byte path a real registry would use, journaled with real per-cycle
/// evidence — against a system that does not yet have the registry
/// behaviour to observe on the other end. Exactly the same shape of gap
/// #203 disclosed for `FencedZombie` (this module's own docs above), and
/// the same conclusion: the fault is real now, the predicate becomes
/// checkable when #53 lands.
///
/// # Errors
///
/// Never — same reasoning as [`run_network_partition`]'s own `Errors`
/// section.
pub async fn run_discovery_flap(
    fault_id: &str,
    target: &NodeSpec,
    catalog_link: &FaultLink,
    delay: Duration,
    schedule: FlapSchedule,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let FlapSchedule { cycles, down, up } = schedule;
    let target_node = rendered_node_id(target);
    log.armed(
        fault_id,
        FaultKind::DiscoveryFlap,
        &target_node,
        Some(serde_json::json!({
            "link": catalog_link.label(),
            "planned_cycles": cycles,
            "down_ms": down.as_millis(),
            "up_ms": up.as_millis(),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    let before = catalog_link.stats();
    log.started(
        fault_id,
        FaultKind::DiscoveryFlap,
        &target_node,
        Some(serde_json::json!({
            "link": catalog_link.label(),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    let mut cycles_completed = 0_u32;
    for _ in 0..cycles {
        // Each cycle's down phase is its own hold, released at the end of
        // the phase — so a cycle can never lift a condition some OTHER
        // armed window (a catalog outage over the same link, say) is
        // holding, and cannot leave one of its own behind either
        // (`crate::link`'s module docs; an ACPR finding on issue #204).
        {
            let _down = catalog_link.hold(LinkConditions::dropped());
            tokio::time::sleep(down).await;
        }
        tokio::time::sleep(up).await;
        cycles_completed += 1;
    }

    let during = catalog_link.stats().since(before);
    log.ended(
        fault_id,
        FaultKind::DiscoveryFlap,
        &target_node,
        Some(serde_json::json!({
            "link": catalog_link.label(),
            "cycles_completed": cycles_completed,
            "link_traffic_during_window": during,
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );
    Ok(())
}

/// Runs one real membership-LEAVE fault (§8.4, issue #204): `target` is
/// asked to leave the fleet gracefully under load — a real `SIGTERM`, which
/// the daemon answers with its own §9.1.2 choreography (readiness flips
/// false, in-flight gRPC finishes, the drain tick completes), NOT a
/// `SIGKILL`. §8.4 is explicit that this fault class is "join and leave
/// under load, **not only crash**": [`run_node_kill`] is the crash;
/// this is the orderly departure, and the two exercise different code
/// paths in the node under test.
///
/// # Errors
///
/// If the `SIGTERM` cannot be sent, or the node does not actually exit
/// within `grace` — the same fail-closed posture as [`run_node_kill`]'s own
/// exit confirmation: a "leave" nobody left is not a fault window that
/// fired.
pub async fn run_membership_leave(
    fault_id: &str,
    target: &mut RunningNode,
    delay: Duration,
    grace: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(&target.spec);
    log.armed(
        fault_id,
        FaultKind::MembershipLeave,
        &target_node,
        Some(serde_json::json!({
            "grace_ms": grace.as_millis(),
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    let pid = process::pid(target);
    process::send_signal(target, "-TERM").await?;
    log.started(
        fault_id,
        FaultKind::MembershipLeave,
        &target_node,
        Some(serde_json::json!({
            "pid": pid,
            "signal": "SIGTERM",
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    let confirmed_exited = process::wait_exited(target, grace).await;
    log.ended(
        fault_id,
        FaultKind::MembershipLeave,
        &target_node,
        Some(serde_json::json!({
            "confirmed_exited": confirmed_exited,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );
    if !confirmed_exited {
        anyhow::bail!(
            "node {target_node} did not complete its graceful shutdown within {grace:?} of \
             SIGTERM (fault {fault_id})"
        );
    }
    Ok(())
}

/// Runs one real membership-JOIN fault (§8.4, issue #204): a node process
/// the fleet provisioned but never booted is really spawned, under load,
/// and really waited on until its own `/status` reports `ready: true`.
/// Returns the running node so the caller can fold it into its own
/// shutdown.
///
/// # What "join" means at v0.1 (disclosed)
///
/// Membership is static config today — every node's `cluster.seed_peers`
/// already names every provisioned member, including this one, because
/// there is no registry to learn membership from (#53;
/// [`run_discovery_flap`]'s own docs). So what really happens here is the
/// real half: a real new process appears under load, binds its real ports,
/// attaches the real shared catalog, boots its real staging engine, and
/// starts answering. What does NOT happen is a membership VIEW changing in
/// any already-running node — nothing in today's daemon can observe a join.
/// The fault is real; the convergence predicate §8.4 pairs with it becomes
/// checkable when the registry lands.
///
/// # Errors
///
/// If the node cannot be spawned, or does not become ready within
/// `boot_timeout` — a join that never joined is not a fault window that
/// fired (same fail-closed posture as [`run_membership_leave`]).
pub async fn run_membership_join(
    fault_id: &str,
    daemon_bin: &std::path::Path,
    joiner: &NodeSpec,
    delay: Duration,
    boot_timeout: Duration,
    log: &FaultLog,
) -> anyhow::Result<RunningNode> {
    let target_node = rendered_node_id(joiner);
    log.armed(
        fault_id,
        FaultKind::MembershipJoin,
        &target_node,
        Some(serde_json::json!({
            "boot_timeout_ms": boot_timeout.as_millis(),
            "otlp_port": joiner.otlp_port,
            "status_port": joiner.status_port,
        })),
    );

    tokio::time::sleep(delay).await;

    let mut member = process::spawn_node(daemon_bin, joiner, None)?;
    log.started(
        fault_id,
        FaultKind::MembershipJoin,
        &target_node,
        Some(serde_json::json!({ "pid": process::pid(&member) })),
    );

    let ready = process::wait_until_ready(&mut member, boot_timeout).await;
    log.ended(
        fault_id,
        FaultKind::MembershipJoin,
        &target_node,
        Some(serde_json::json!({
            "confirmed_ready": ready.is_ok(),
            "node_journal_lines": node_journal_line_count(&joiner.journal_path),
        })),
    );
    match ready {
        Ok(()) => Ok(member),
        Err(error) => {
            // The half-joined process is handed back to nobody, so it must
            // not be left running: `RunningNode`'s child is `kill_on_drop`,
            // so dropping it here is the teardown. (`member`, not `joined`
            // — clippy's `similar_names` against `joiner` above.)
            drop(member);
            Err(error.context(format!(
                "membership-join fault {fault_id}: node {target_node} never became ready"
            )))
        }
    }
}

/// Runs one real Flight-server-kill-mid-stream fault (§8.4, issue #204):
/// opens a REAL Arrow Flight `DoGet` against `target`'s real Flight server,
/// reads the stream's FIRST `FlightData` message — which
/// `FlightDataEncoderBuilder` always emits as the Arrow schema, so this is
/// "the server has really begun writing this stream", not "a data batch has
/// arrived" (an ACPR finding on issue #204, LOW-10: the pre-fix wording
/// claimed the latter) — then `SIGKILL`s the node and keeps reading the
/// stream to journal what the client actually observed. Killing after that
/// first message is still a strictly stronger fault than killing before any
/// read: it is what puts the server in the flow-control state the next
/// section relies on.
///
/// # Why this lands mid-stream deterministically
///
/// `duckspout_daemon::serving`'s `do_get` is collect-then-stream: the whole
/// result is materialized under the §7.8 guards first, then encoded onto
/// the wire. So "mid-stream" is a property of the NETWORK phase, and HTTP/2
/// flow control is what makes it deterministic rather than racy: a server
/// may only push up to the connection/stream window (64 KiB by default)
/// ahead of a client that has stopped reading. This injector reads exactly
/// ONE message, then stops reading while it kills the node — so for any
/// query whose encoded result comfortably exceeds that window, the server
/// provably still had unsent data at the moment it died. That is the same
/// "widen the window instead of racing it" discipline #203's mid-drain kill
/// used (module docs above), applied to the wire instead of to a commit.
///
/// The caller therefore owns the query (`--fault-flight-kill-query`), and
/// its default is sized for exactly this: see that flag's own doc comment.
/// A query whose result fits inside the flow-control window is not an
/// error — it simply produces a `clean_end_of_stream` outcome, journaled
/// honestly, which a judge reads as "this fault window proved nothing"
/// rather than as a violation.
///
/// # What is journaled, and why the runner never convicts
///
/// The `Ended` line carries `terminal_outcome`: `typed_error` (with the
/// gRPC status code — §7's required shape: "the client's typed error, never
/// a silently truncated result") or `clean_end_of_stream`, plus how many
/// Flight messages arrived before and after the kill. Whether a given
/// outcome is a violation is a judge's call over the journals (#208), not
/// this injector's: it fails only on things that mean the fault never
/// fired at all (below).
///
/// # Errors
///
/// If the Flight server cannot be reached, the `DoGet` itself is refused
/// (a rejected ticket is a misconfigured query, not a fault window), no
/// first message arrives within `first_message_timeout`, or the `SIGKILL`
/// cannot be confirmed — every one of which means the fault did not fire.
pub async fn run_flight_kill_mid_stream(
    fault_id: &str,
    target: &mut RunningNode,
    query: &str,
    delay: Duration,
    first_message_timeout: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(&target.spec);
    let endpoint = format!("http://127.0.0.1:{}", target.spec.flight_port);
    log.armed(
        fault_id,
        FaultKind::FlightKillMidStream,
        &target_node,
        Some(serde_json::json!({
            "flight_endpoint": endpoint,
            "query": query,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    tokio::time::sleep(delay).await;

    // `Endpoint::from_shared(...).connect()` + `Client::new(channel)` — the
    // same construction `duckspout-daemon/tests/flight_e2e.rs` uses against
    // the real server, rather than the codegen'd `connect` shortcut (which
    // is gated behind a tonic feature this crate does not enable).
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .with_context(|| format!("fault {fault_id}: {endpoint} is not a valid Flight endpoint"))?
        .connect()
        .await
        .with_context(|| format!("fault {fault_id}: connecting to Flight at {endpoint}"))?;
    let mut client = FlightServiceClient::new(channel);
    let mut stream = client
        .do_get(Ticket::new(query.to_owned().into_bytes()))
        .await
        .with_context(|| format!("fault {fault_id}: DoGet {query:?} was refused"))?
        .into_inner();

    // One message, then stop reading: from here the server is blocked on
    // HTTP/2 flow control with data still to send (module docs).
    let first = tokio::time::timeout(first_message_timeout, stream.message())
        .await
        .with_context(|| {
            format!(
                "fault {fault_id}: no Flight message within {first_message_timeout:?} — the \
                 stream never started, so a kill now would prove nothing"
            )
        })?
        .with_context(|| format!("fault {fault_id}: the Flight stream errored before any data"))?;
    anyhow::ensure!(
        first.is_some(),
        "fault {fault_id}: the Flight stream ended before the kill could land mid-stream \
         (query {query:?} produced no data at all)"
    );

    let pid = process::pid(target);
    process::send_signal(target, "-KILL").await?;
    log.started(
        fault_id,
        FaultKind::FlightKillMidStream,
        &target_node,
        Some(serde_json::json!({
            "pid": pid,
            "messages_before_kill": 1,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );

    let mut messages_after_kill = 0_u64;
    let terminal_outcome = loop {
        match stream.message().await {
            Ok(Some(_)) => messages_after_kill += 1,
            Ok(None) => break serde_json::json!({ "terminal_outcome": "clean_end_of_stream" }),
            Err(status) => {
                break serde_json::json!({
                    "terminal_outcome": "typed_error",
                    "grpc_code": format!("{:?}", status.code()),
                    "grpc_message": status.message(),
                });
            }
        }
    };

    let confirmed_exited = process::wait_exited(target, Duration::from_secs(10)).await;
    log.ended(
        fault_id,
        FaultKind::FlightKillMidStream,
        &target_node,
        Some(serde_json::json!({
            "client_outcome": terminal_outcome,
            "messages_after_kill": messages_after_kill,
            "confirmed_exited": confirmed_exited,
            "node_journal_lines": node_journal_line_count(&target.spec.journal_path),
        })),
    );
    if !confirmed_exited {
        anyhow::bail!(
            "node {target_node} did not confirm exit within 10s of the Flight-server SIGKILL \
             (fault {fault_id})"
        );
    }
    Ok(())
}

/// The three §3 post-drain residency actions (§2.4, §6.9) — the set
/// [`run_cache_churn`] counts in a target's own D-6 journal, and exactly the
/// set `duckspout_judge::journal::JournalSet::residency_action_count` counts
/// on the grading side. One vocabulary, two readers.
///
/// The events are named by their [`TraceEvent`] VARIANTS, not by string
/// literals, so this list cannot drift from the frozen §3.3 vocabulary: the
/// journal token each one is written as is `format!("{variant:?}")`, which
/// `duckspout_types::trace`'s own
/// `every_event_serializes_as_its_verbatim_action_name` test pins for all 28
/// variants. The second element is the key the count is journaled under in
/// this crate's `faults.ndjson` detail object — an ordinary `snake_case` JSON
/// key, deliberately not the action name, because the fault log is this
/// crate's informal channel and not a D-6 journal (`crate::faultlog`'s own
/// framing).
const RESIDENCY_EVENTS: [(TraceEvent, &str); 3] = [
    (TraceEvent::Demote, "demote"),
    (TraceEvent::Evict, "evict"),
    (TraceEvent::DropWindow, "drop_window"),
];

/// One cache-churn window's observed residency activity — the real counts,
/// per event, from the target's own journal, positionally aligned with
/// [`RESIDENCY_EVENTS`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ResidencyCounts([u64; RESIDENCY_EVENTS.len()]);

impl ResidencyCounts {
    /// Reads `journal_path` and counts each residency event in it, skipping
    /// the first `skip_lines` lines so a window measures only what happened
    /// inside it (the same anchoring [`wait_for_journal_event`] needs, for
    /// the same reason).
    ///
    /// Never errors: a journal that does not exist yet, or a torn last line
    /// mid-write, both read as "nothing counted" — a fault log must never
    /// fail the fleet run over a best-effort observation.
    fn since(journal_path: &std::path::Path, skip_lines: u64) -> Self {
        let Ok(contents) = std::fs::read_to_string(journal_path) else {
            return Self::default();
        };
        let mut counts = Self::default();
        for line in contents
            .lines()
            .skip(usize::try_from(skip_lines).unwrap_or(usize::MAX))
        {
            let Some(event) = serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("event")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                })
            else {
                continue;
            };
            if let Some(index) = RESIDENCY_EVENTS
                .iter()
                .position(|(variant, _)| event_token(*variant) == event)
            {
                counts.0[index] += 1;
            }
        }
        counts
    }

    /// How many times `event` was journaled — `0` for an event that is not a
    /// residency action at all.
    ///
    /// Test-only: production code reads the whole set through
    /// [`ResidencyCounts::to_json`] and [`ResidencyCounts::total`], and a
    /// per-event accessor with no production caller would be dead weight
    /// in the shipped binary.
    #[cfg(test)]
    fn of(self, event: TraceEvent) -> u64 {
        RESIDENCY_EVENTS
            .iter()
            .position(|(variant, _)| *variant == event)
            .map_or(0, |index| self.0[index])
    }

    /// How many residency actions of any kind — the judge-side cache-state
    /// label's own definition.
    fn total(self) -> u64 {
        self.0.iter().sum()
    }

    fn to_json(self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        for ((_, key), count) in RESIDENCY_EVENTS.iter().zip(self.0) {
            out.insert((*key).to_owned(), serde_json::json!(count));
        }
        out.insert("total".to_owned(), serde_json::json!(self.total()));
        serde_json::Value::Object(out)
    }
}

/// The exact token `event` is written as in a node's own D-6 journal
/// ([`RESIDENCY_EVENTS`]' own note on why `Debug` is the authority here).
fn event_token(event: TraceEvent) -> String {
    format!("{event:?}")
}

/// What the racing reads observed across one cache-churn window.
#[derive(Debug, Clone, Copy, Default)]
struct ReadTally {
    issued: u64,
    served: u64,
    failed: u64,
    max_latency_ms: u64,
    /// Reads whose own residency-op count moved between issue and
    /// completion — the ones that genuinely overlapped a residency action,
    /// which is what obligation (c) is about.
    raced: u64,
}

impl ReadTally {
    fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "issued": self.issued,
            "served": self.served,
            "failed": self.failed,
            "raced_residency_action": self.raced,
            "max_latency_ms": self.max_latency_ms,
        })
    }
}

/// Runs one real cache/residency-churn fault (§8.4, issue #207): the target
/// node's post-drain residency is churned under load while real Arrow Flight
/// reads run through the same engine.
///
/// # The forcing mechanism, and why it is not a back door
///
/// §8.4 asks for "forced Evict/Demote churn and `DropWindow` racing
/// queries". The forcing here is the daemon's OWN drain cadence, tuned
/// through its own ratcheted setting: the target is booted with a shortened
/// `hot.window` (`--fault-cache-churn-hot-window`, applied to that one node
/// via [`crate::topology::NodeOverrides`]), so under sustained drive load its
/// windows seal, drain, commit and `DropWindow` every window period instead
/// of every minute. Nothing is simulated and no debug hook is added: the
/// residency actions this window counts are the real ones
/// `duckspout_drain::coordinator` journals after a real durable
/// `LakeCommit`.
///
/// A daemon-side hook (a `--fault-*` decorator like
/// `duckspout_daemon::fault::StallingLakeCommitter`) was considered and
/// rejected: there is nothing for it to decorate. `DropWindow` already fires
/// on every drain, so the honest lever is the cadence, and shortening a
/// window is a configuration the daemon already supports and a real
/// deployment can legitimately choose.
///
/// # What this can and cannot force at v0.2 (disclosed, not papered over)
///
/// Of the three residency actions, **only `DropWindow` can fire today**, and
/// the reason is a settled design decision rather than a gap this injector
/// declined to close:
///
/// - `docs/design/data-model.md` §2.4: "v1's cache class is **empty by
///   construction** (`DropWindow` at drain commit)". A drained window is
///   dropped, never demoted, so no cache-class table ever exists.
/// - `docs/deferred.md`'s warm-retention row parks the whole residency
///   mechanism — SLRU, the `residency` attribute, rung-0 eviction — behind a
///   v0.4 experiment with a named trigger.
/// - Consequently `duckspout_types::TraceEvent::{Demote, Evict}` have no
///   emitter anywhere in this workspace.
///
/// Making them fire would mean implementing warm retention, which is exactly
/// the settled deferral `AGENTS.md` forbids re-litigating in a PR. So this
/// injector counts all three events and journals all three counts —
/// `demote: 0, evict: 0` is the honest, machine-readable record of that
/// state, and the day the cache class activates the same injector starts
/// reporting non-zero without a line changing. This is the same shape #203
/// disclosed for `FencedZombie` and #204 for discovery flapping: the fault
/// is real now, and the part of the predicate that needs unbuilt composition
/// says so rather than pretending.
///
/// What IS being exercised for real, and is not a lesser thing: `DropWindow`
/// is the transition where a window stops being served from staging and
/// starts being served from the lake — §2.4 obligation (b)'s "exactly one
/// side serves any window — staging XOR lake/cache" boundary, crossed under
/// a live read. That is precisely the read-path interleaving
/// `specs/formal-core.md`'s `CacheTransparency` note hands to §8.4 ("eviction
/// interleavings stress the read-path equivalence, which is §8.4's job, not
/// this formula's").
///
/// # Why the reads are what they are
///
/// The default query (`--fault-cache-churn-query`) reads the staging
/// engine's own window registry, which `DropWindow`'s transaction deletes
/// from in the same transaction as its `DROP TABLE`
/// (`duckspout_staging::engine`). A read of a table the churn is actively
/// writing is the one that would show a held lock; a query over `range()`
/// would touch nothing the churn touches and would prove nothing about
/// obligation (c).
///
/// The reads go through a real Flight `DoGet` on a real dedicated read
/// connection (§7.8, #114) — the same client construction
/// [`run_flight_kill_mid_stream`] uses. Each read is bracketed by a
/// residency-op count read out of the target's own journal, so a read that
/// really overlapped an action is distinguishable from one that did not.
///
/// This injector deliberately does NOT write `duckspout-judge`'s read log:
/// the daemon's read surface is hot-only, with no read concern and no
/// coverage pinning (`duckspout_daemon::serving`'s #113 gap), so there is no
/// `complete` read here to log, and inventing a `complete_through_ms` for
/// one would be manufacturing the exact evidence the judge is supposed to
/// weigh. The observations go into this window's own `Ended` line instead,
/// where they are what they are: a fleet runner's record, not a verdict
/// (§8.4 — this binary never makes one).
///
/// # Errors
///
/// If no residency action is observed inside the window at all. A churn
/// fault that churned nothing fired vacuously (§8.4's vacuity teeth), and
/// journaling an `Ended` line for it would record a storm that never
/// happened — the same fail-closed posture [`run_sigstop_pause`] takes when
/// it cannot confirm its own pause. Flight-side failures are NOT errors:
/// a refused or errored read is an OBSERVATION about the system under churn,
/// journaled and left to the judge.
pub async fn run_cache_churn(
    fault_id: &str,
    target: &NodeSpec,
    query: &str,
    delay: Duration,
    duration: Duration,
    read_interval: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = rendered_node_id(target);
    let endpoint = format!("http://127.0.0.1:{}", target.flight_port);
    let anchor = node_journal_line_count(&target.journal_path);
    log.armed(
        fault_id,
        FaultKind::CacheChurn,
        &target_node,
        Some(serde_json::json!({
            "flight_endpoint": endpoint,
            "query": query,
            "planned_duration_ms": duration.as_millis(),
            "residency_events_watched": RESIDENCY_EVENTS
                .iter()
                .map(|(event, _)| event_token(*event))
                .collect::<Vec<_>>(),
            "node_journal_lines": anchor,
        })),
    );

    tokio::time::sleep(delay).await;

    // The window opens where the reads start, not where the injector armed:
    // everything the drive-load pass drained during `delay` belongs to
    // neither the baseline nor the storm.
    let window_start = node_journal_line_count(&target.journal_path);
    log.started(
        fault_id,
        FaultKind::CacheChurn,
        &target_node,
        Some(serde_json::json!({
            "node_journal_lines": window_start,
            // What the node churned between arming and the window opening —
            // NOT part of the window's own counts below, and journaled so a
            // reader can tell a node that was already churning from one this
            // fault had to get going.
            "residency_actions_before_window": ResidencyCounts::since(
                &target.journal_path,
                anchor,
            )
            .to_json(),
        })),
    );

    let tally = drive_racing_reads(&endpoint, target, query, duration, read_interval).await;
    let observed = ResidencyCounts::since(&target.journal_path, window_start);

    log.ended(
        fault_id,
        FaultKind::CacheChurn,
        &target_node,
        Some(serde_json::json!({
            "residency_actions_during_window": observed.to_json(),
            "reads_during_window": tally.to_json(),
            "node_journal_lines": node_journal_line_count(&target.journal_path),
        })),
    );

    anyhow::ensure!(
        observed.total() > 0,
        "fault {fault_id}: node {target_node} journaled no Demote/Evict/DropWindow line during \
         the whole {duration:?} churn window — the storm never happened, so this fault fired \
         vacuously (§8.4). Check that the drive load is still running and that \
         --fault-cache-churn-hot-window is short enough for a window to seal and drain inside \
         the window's duration."
    );
    Ok(())
}

/// Issues real Flight reads against `endpoint` for `duration`, bracketing
/// each one by the target's own residency-op count (module docs of
/// [`run_cache_churn`]).
///
/// Best-effort by construction: a connection that cannot be established, a
/// refused ticket and a stream that errors mid-read are all counted as
/// FAILED reads rather than propagated, because each of them is exactly the
/// kind of thing §2.4's obligation (c) is about and the runner does not get
/// to decide whether it was a violation.
async fn drive_racing_reads(
    endpoint: &str,
    target: &NodeSpec,
    query: &str,
    duration: Duration,
    read_interval: Duration,
) -> ReadTally {
    let mut tally = ReadTally::default();
    let deadline = tokio::time::Instant::now() + duration;
    let client = match tonic::transport::Endpoint::from_shared(endpoint.to_owned()) {
        Ok(channel) => match channel.connect().await {
            Ok(channel) => Some(FlightServiceClient::new(channel)),
            Err(_) => None,
        },
        Err(_) => None,
    };
    let Some(mut client) = client else {
        // Not reachable at all: every read this window would have issued is
        // a failed one, and the `Ended` line will show `issued == failed`.
        //
        // The window still runs its FULL duration rather than collapsing to
        // zero: the residency storm is the daemon's own drain loop, which is
        // churning whether or not this injector can reach the Flight port,
        // and cutting the window short would report a real storm as a
        // vacuous one purely because the read half could not connect. The
        // two halves are journaled separately for exactly this reason.
        tally.issued = 1;
        tally.failed = 1;
        tokio::time::sleep_until(deadline).await;
        return tally;
    };

    while tokio::time::Instant::now() < deadline {
        let before = ResidencyCounts::since(&target.journal_path, 0).total();
        let started = tokio::time::Instant::now();
        let outcome = read_once(&mut client, query).await;
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let after = ResidencyCounts::since(&target.journal_path, 0).total();

        tally.issued += 1;
        if outcome {
            tally.served += 1;
        } else {
            tally.failed += 1;
        }
        if after > before {
            tally.raced += 1;
        }
        tally.max_latency_ms = tally.max_latency_ms.max(latency_ms);

        tokio::time::sleep(read_interval).await;
    }
    tally
}

/// One real Flight read, drained to completion. `true` iff the whole stream
/// arrived without a typed error.
async fn read_once(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    query: &str,
) -> bool {
    let Ok(response) = client
        .do_get(Ticket::new(query.to_owned().into_bytes()))
        .await
    else {
        return false;
    };
    let mut stream = response.into_inner();
    loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
}

/// `(label, upstream)` for every link a fault window covers — the journal's
/// own record of exactly which real network edges it cut.
fn link_descriptions(links: &[&FaultLink]) -> Vec<serde_json::Value> {
    links
        .iter()
        .map(|link| serde_json::json!({ "label": link.label(), "upstream": link.upstream() }))
        .collect()
}

/// Each link's traffic delta since the matching `before` snapshot, keyed by
/// label (module docs of [`run_network_partition`] on why this is the
/// evidence a judge needs).
fn link_deltas(links: &[&FaultLink], before: &[LinkStats]) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (link, earlier) in links.iter().zip(before) {
        out.insert(
            link.label().to_owned(),
            serde_json::to_value(link.stats().since(*earlier)).unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::Value::Object(out)
}

/// `drain_stalled` from `target`'s own `/status` (§9.3.2) right now, or
/// `null` if the node cannot be reached or does not report the field —
/// best-effort evidence, never an error (module docs of
/// [`run_catalog_outage`]).
async fn observed_drain_stalled(target: &NodeSpec) -> serde_json::Value {
    // Bounded: `process::fetch_status` has no timeout of its own, and a node
    // that accepts the connection but never answers (a `SIGSTOP`ped one, if
    // both faults are armed against the same node) would otherwise hang this
    // fault window open indefinitely — the one thing a start/end-journaled
    // window must never do.
    let fetched = tokio::time::timeout(
        Duration::from_secs(2),
        process::fetch_status(target.status_addr()),
    )
    .await;
    match fetched {
        Ok(Ok(snapshot)) => snapshot
            .get("drain_stalled")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        Ok(Err(_)) | Err(_) => serde_json::Value::Null,
    }
}

/// Polls `check(pid)` until it reports `true`, or `timeout` elapses —
/// best-effort OS-level confirmation that a sent signal actually took
/// observable effect (module docs above: Linux-only, never a hard error — a
/// missing `/proc` read degrades to "unconfirmed," i.e. `false`).
/// `check` is [`process::is_stopped`] for the pause side and
/// [`process::is_live_running`] for the resume side — deliberately two
/// DIFFERENT predicates, not one boolean-flipped check: a zombie (`Z`)
/// satisfies neither, which is exactly the property this fault's HIGH-1
/// ACPR finding needed (module docs of [`run_sigstop_pause`]).
async fn confirm(
    pid: Option<u32>,
    timeout: Duration,
    check: fn(u32) -> anyhow::Result<bool>,
) -> bool {
    let Some(pid) = pid else { return false };
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check(pid).unwrap_or(false) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Polls `journal_path` every 50 ms until an NDJSON line AFTER the first
/// `skip_lines` lines has an `event` field equal to `event`, or `timeout`
/// elapses. `skip_lines` is an ACPR finding's fix: without it, this would
/// match ANY pre-existing matching line already in the file at the moment
/// the injector starts watching — safe only by an unstated, untested
/// precondition (the daemon truncates its journal at boot, and boot happens
/// before arming) that a real sustained-load generator running BEFORE the
/// fault triggers would silently defeat (module docs of
/// [`journal_contains_event_after`]). Callers pass the journal's own line
/// count at the moment they start watching (`crate::fault::run_node_kill`'s
/// own `journal_anchor`) so only a genuinely NEW line counts.
///
/// # Errors
///
/// If `timeout` elapses with no matching NEW line ever observed.
async fn wait_for_journal_event(
    journal_path: &std::path::Path,
    event: &str,
    skip_lines: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if journal_contains_event_after(journal_path, event, skip_lines) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out after {timeout:?} waiting for a NEW {event:?} line (after line \
                 {skip_lines}) in {}",
                journal_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether `journal_path`'s NDJSON contents contain a line, AFTER skipping
/// the first `skip_lines` lines, whose `event` field equals `event` —
/// re-reads the whole (small, per-node) journal each call rather than
/// tracking a byte offset, which is simple and correct for this crate's
/// smoke-scale journals (`crate::faultlog`'s own "informal channel" framing
/// extends to this reader: no shared parsing contract with a real judge is
/// implied here). Never errors: a journal that does not exist yet, or a
/// torn/partial last line mid-write, both read as "not found yet" — the
/// caller's polling loop already tolerates that.
fn journal_contains_event_after(
    journal_path: &std::path::Path,
    event: &str,
    skip_lines: u64,
) -> bool {
    let Ok(contents) = std::fs::read_to_string(journal_path) else {
        return false;
    };
    contents
        .lines()
        .skip(usize::try_from(skip_lines).unwrap_or(usize::MAX))
        .any(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("event")
                        .and_then(serde_json::Value::as_str)
                        .map(|e| e == event)
                })
                .unwrap_or(false)
        })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;
    use crate::faultlog::FaultLog;
    use crate::link;
    use crate::process::test_support;

    fn scratch_faultlog(label: &str) -> (std::path::PathBuf, FaultLog) {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-{label}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("faults.ndjson");
        let log = FaultLog::create(&path).unwrap();
        (path, log)
    }

    fn read_ndjson_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// Every line of one fault window, in journaled order.
    fn window_lines(path: &std::path::Path, fault_id: &str) -> Vec<serde_json::Value> {
        read_ndjson_lines(path)
            .into_iter()
            .filter(|line| line["fault_id"] == fault_id)
            .collect()
    }

    /// The phases one fault window journaled, in order.
    fn phases(lines: &[serde_json::Value]) -> Vec<&str> {
        lines
            .iter()
            .map(|line| line["phase"].as_str().unwrap())
            .collect()
    }

    /// Writes `text` as a node's own D-6 journal and returns its path.
    fn write_node_journal(label: &str, text: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-{label}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.ndjson");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
        path
    }

    /// The residency counter reads exactly the three §3 post-drain actions
    /// out of a real NDJSON journal, and nothing else. Would catch a counter
    /// that swept in every line (making an idle node look like a storm) or
    /// that missed `Demote`/`Evict` — the two that cannot fire yet, and
    /// whose day-one arrival must not need a code change here
    /// (`run_cache_churn`'s own disclosure).
    #[test]
    fn the_residency_counter_counts_exactly_the_three_post_drain_actions() {
        let path = write_node_journal(
            "residency-counts",
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"DropWindow\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"Demote\"}\n\
             {\"node\":\"n1\",\"seq\":3,\"event\":\"Evict\"}\n\
             {\"node\":\"n1\",\"seq\":4,\"event\":\"DropWindow\"}\n\
             {\"node\":\"n1\",\"seq\":5,\"event\":\"LakeCommitOk\"}\n",
        );
        let counts = ResidencyCounts::since(&path, 0);
        assert_eq!(counts.of(TraceEvent::DropWindow), 2);
        assert_eq!(counts.of(TraceEvent::Demote), 1);
        assert_eq!(counts.of(TraceEvent::Evict), 1);
        assert_eq!(counts.total(), 4);
        // A non-residency action is not a residency action — would catch a
        // counter that swept in every journaled line.
        assert_eq!(counts.of(TraceEvent::Accept), 0);
    }

    /// The watched set is derived from the frozen [`TraceEvent`] vocabulary,
    /// not from hand-written strings ([`RESIDENCY_EVENTS`]' own note). Would
    /// catch a token that drifted from what a node actually journals — a
    /// churn window would then silently count nothing and report every real
    /// storm as vacuous.
    #[test]
    fn the_watched_event_tokens_are_the_frozen_action_names() {
        let tokens: Vec<String> = RESIDENCY_EVENTS
            .iter()
            .map(|(event, _)| event_token(*event))
            .collect();
        assert_eq!(tokens, vec!["Demote", "Evict", "DropWindow"]);
    }

    /// The window anchor is load-bearing: a churn window must count only
    /// what happened INSIDE it, never the drains the drive-load pass already
    /// did while the injector was waiting out its delay. Would catch an
    /// injector that inherited a pre-window storm and reported a vacuous
    /// window as a real one.
    #[test]
    fn the_residency_counter_ignores_everything_before_the_windows_anchor() {
        let path = write_node_journal(
            "residency-anchor",
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"DropWindow\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"DropWindow\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"DropWindow\"}\n",
        );
        assert_eq!(ResidencyCounts::since(&path, 0).total(), 3);
        assert_eq!(ResidencyCounts::since(&path, 2).total(), 1);
        assert_eq!(ResidencyCounts::since(&path, 3).total(), 0);
    }

    /// A journal that does not exist yet — or a torn last line mid-write —
    /// counts as nothing rather than erroring: a best-effort observation
    /// must never fail the fleet run (its own docs).
    #[test]
    fn a_missing_or_torn_journal_counts_as_nothing_rather_than_failing() {
        assert_eq!(
            ResidencyCounts::since(
                std::path::Path::new("/nonexistent/duckspout-fleet-churn-journal.ndjson"),
                0
            )
            .total(),
            0
        );
        let torn = write_node_journal(
            "residency-torn",
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"DropWindow\"}\n{\"node\":\"n1\",\"se",
        );
        assert_eq!(ResidencyCounts::since(&torn, 0).total(), 1);
    }

    /// A churn window that observed no residency action at all must FAIL,
    /// not journal a clean `Ended` line: a storm that never happened is
    /// exactly §8.4's vacuous fault, and recording it as a completed window
    /// would tell a judge the run exercised something it did not. The
    /// `Ended` line is still written first, so the run's own journal shows
    /// the zero counts that justify the failure.
    #[tokio::test]
    async fn a_churn_window_that_churned_nothing_fails_closed() {
        let (log_path, log) = scratch_faultlog("cache-churn-vacuous");
        let mut spec = test_support::dummy_spec();
        spec.journal_path = write_node_journal(
            "residency-idle",
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n",
        );
        // Port 1 is not bound by anything here, so every read fails — which
        // is an observation, not an error (the injector's own docs); the
        // failure below must come from the ABSENT residency actions.
        spec.flight_port = 1;

        let error = run_cache_churn(
            "cache-churn-0",
            &spec,
            "SELECT 1",
            Duration::ZERO,
            Duration::from_millis(50),
            Duration::from_millis(10),
            &log,
        )
        .await
        .expect_err("a churn window that churned nothing must fail closed");
        assert!(error.to_string().contains("vacuously"), "error: {error:#}");

        let lines = window_lines(&log_path, "cache-churn-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        let ended = lines.last().unwrap();
        assert_eq!(
            ended["detail"]["residency_actions_during_window"]["total"],
            0
        );
        assert_eq!(ended["detail"]["reads_during_window"]["served"], 0);
    }

    /// The `Ended` line reports the real counts it observed, per event —
    /// including the `demote`/`evict` zeros that are the honest record of
    /// v1's empty-by-construction cache class (`run_cache_churn`'s
    /// disclosure). Would catch an injector that reported only a total, or
    /// that omitted the two events it cannot yet see, either of which would
    /// leave a judge unable to tell "no cache class" from "cache class that
    /// never churned".
    #[tokio::test]
    async fn a_churn_window_journals_every_residency_event_it_watched() {
        let (log_path, log) = scratch_faultlog("cache-churn-counts");
        let mut spec = test_support::dummy_spec();
        spec.flight_port = 1;
        // The journal already holds one pre-window action, and gains two
        // more that the window itself must be the one to count.
        spec.journal_path = write_node_journal(
            "residency-during",
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"DropWindow\"}\n",
        );
        let journal_path = spec.journal_path.clone();
        let appender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&journal_path)
                .unwrap();
            file.write_all(
                b"{\"node\":\"n1\",\"seq\":1,\"event\":\"DropWindow\"}\n\
                  {\"node\":\"n1\",\"seq\":2,\"event\":\"DropWindow\"}\n",
            )
            .unwrap();
        });

        run_cache_churn(
            "cache-churn-0",
            &spec,
            "SELECT 1",
            Duration::ZERO,
            Duration::from_millis(200),
            Duration::from_millis(10),
            &log,
        )
        .await
        .expect("a window that observed real residency actions must succeed");
        appender.await.unwrap();

        let lines = window_lines(&log_path, "cache-churn-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        let observed = &lines.last().unwrap()["detail"]["residency_actions_during_window"];
        assert_eq!(observed["drop_window"], 2);
        assert_eq!(
            observed["demote"], 0,
            "v1's cache class is empty by construction — the zero must be journaled, not omitted"
        );
        assert_eq!(observed["evict"], 0);
        assert_eq!(observed["total"], 2);
    }

    /// A [`NodeSpec`] with a chosen `/status` port — the network-fault
    /// injectors only ever read a target's identity, journal path and
    /// status address, never its process.
    fn spec_with_status_port(name: &str, status_port: u16) -> NodeSpec {
        let mut spec = test_support::dummy_spec();
        spec.name = name.to_owned();
        spec.status_port = status_port;
        // `process::spawn_node` (the join fault) really creates this node's
        // stdout/stderr files, so the directory holding them must exist —
        // `dummy_spec` only computes paths.
        if let Some(parent) = spec.stdout_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        spec
    }

    /// Binds a real listener answering every request with `body` — the same
    /// hand-rolled `/status` stand-in `crate::process`'s and `crate::main`'s
    /// own tests use, so no real daemon is needed to exercise the
    /// disclosure sampling.
    async fn spawn_fake_status_server(body: &'static str) -> u16 {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let _ = stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{body}"
                            )
                            .as_bytes(),
                        )
                        .await;
                });
            }
        });
        port
    }

    /// The partition fault, end to end against a REAL link and a REAL
    /// upstream: traffic crosses before the window, is refused DURING it,
    /// crosses again after it — and the journal's own per-window traffic
    /// delta says exactly that (zero bytes forwarded, a refused connection
    /// counted). Would catch an injector that journaled a window it never
    /// actually imposed, or one that forgot to restore the link.
    #[tokio::test]
    async fn network_partition_really_cuts_the_link_for_exactly_its_window() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-ingest", "127.0.0.1", port)
            .await
            .unwrap();
        let spec = spec_with_status_port("partition-node", 0);
        let (path, log) = scratch_faultlog("partition");

        link::test_support::echo_round_trip(&link, b"before")
            .await
            .expect("the link must carry traffic before the fault");

        let partitioned = [&link];
        let during = tokio::join!(
            run_network_partition(
                "network-partition-0",
                &spec,
                &partitioned,
                Duration::ZERO,
                Duration::from_millis(400),
                &log,
            ),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                link::test_support::echo_round_trip(&link, b"during").await
            }
        );
        during.0.unwrap();
        assert!(
            during.1.is_err(),
            "traffic must not cross a partitioned link"
        );

        link::test_support::echo_round_trip(&link, b"after")
            .await
            .expect("the link must be restored when the window ends");

        let lines = window_lines(&path, "network-partition-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        let traffic = &lines[2]["detail"]["link_traffic_during_window"]["n0-ingest"];
        assert_eq!(
            traffic["bytes_client_to_server"], 0,
            "no byte may cross during the window: {lines:#?}"
        );
        assert_eq!(
            traffic["conns_refused"], 1,
            "the refused connection is the window's own evidence: {lines:#?}"
        );
    }

    /// The asymmetric-degradation fault: the conditions it journals are the
    /// conditions it actually imposed (a real, measurable delay on the
    /// configured direction), and the link is left clean afterwards.
    #[tokio::test]
    async fn network_degradation_imposes_and_then_lifts_the_journaled_conditions() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-ingest", "127.0.0.1", port)
            .await
            .unwrap();
        let spec = spec_with_status_port("degrade-node", 0);
        let (path, log) = scratch_faultlog("degrade");
        let conditions = LinkConditions {
            client_to_server: link::LinkCondition::Delay { ms: 300 },
            server_to_client: link::LinkCondition::Pass,
        };

        let (fault, probe) = tokio::join!(
            run_network_degradation(
                "network-degradation-0",
                &spec,
                &link,
                conditions,
                Duration::ZERO,
                Duration::from_millis(600),
                &log,
            ),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                link::test_support::echo_round_trip(&link, b"slowed").await
            }
        );
        fault.unwrap();
        let degraded = probe.expect("a degraded link still carries traffic, just slowly");
        assert!(
            degraded >= Duration::from_millis(300),
            "the journaled delay must actually be imposed, took {degraded:?}"
        );

        let restored = link::test_support::echo_round_trip(&link, b"fast")
            .await
            .unwrap();
        assert!(
            restored < Duration::from_millis(300),
            "the condition must be lifted when the window ends, took {restored:?}"
        );

        let lines = window_lines(&path, "network-degradation-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        assert_eq!(
            lines[1]["detail"]["conditions"]["client_to_server"]["condition"],
            "delay"
        );
        assert_eq!(
            lines[1]["detail"]["conditions"]["client_to_server"]["ms"],
            300
        );
        assert_eq!(
            lines[1]["detail"]["conditions"]["server_to_client"]["condition"], "pass",
            "the asymmetry must be journaled as asymmetric: {lines:#?}"
        );
    }

    /// The catalog outage journals the target's OWN disclosed
    /// `drain_stalled` (§9.3.2) at both ends of the window — the evidence
    /// §8.4's "drains stall and disclose" predicate is judged from. Would
    /// catch an injector that journaled a hardcoded value instead of
    /// actually reading the node's disclosure.
    #[tokio::test]
    async fn catalog_outage_journals_the_targets_own_disclosed_drain_status() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-catalog", "127.0.0.1", port)
            .await
            .unwrap();
        let status_port =
            spawn_fake_status_server(r#"{"ready":true,"drain_stalled":true,"watermarks":[]}"#)
                .await;
        let spec = spec_with_status_port("catalog-node", status_port);
        let (path, log) = scratch_faultlog("catalog-outage");

        run_catalog_outage(
            "catalog-outage-0",
            &spec,
            &link,
            Duration::ZERO,
            Duration::from_millis(100),
            &log,
        )
        .await
        .unwrap();

        let lines = window_lines(&path, "catalog-outage-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        assert_eq!(lines[1]["detail"]["drain_stalled"], true);
        assert_eq!(lines[2]["detail"]["drain_stalled"], true);
        link::test_support::echo_round_trip(&link, b"after")
            .await
            .expect("the catalog link must be restored when the window ends");
    }

    /// A node that cannot be reached at all journals `null` rather than
    /// failing the run — an unreachable node is a finding for the judge,
    /// never a reason for the injector to abort (its own module docs).
    #[tokio::test]
    async fn catalog_outage_journals_a_null_drain_status_for_an_unreachable_node() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-catalog", "127.0.0.1", port)
            .await
            .unwrap();
        // Port 1: privileged, never listening in this sandbox — the same
        // convention `backend_check`'s own closed-port test uses.
        let spec = spec_with_status_port("unreachable-node", 1);
        let (path, log) = scratch_faultlog("catalog-outage-null");

        run_catalog_outage(
            "catalog-outage-0",
            &spec,
            &link,
            Duration::ZERO,
            Duration::from_millis(50),
            &log,
        )
        .await
        .unwrap();

        let lines = window_lines(&path, "catalog-outage-0");
        assert!(lines[1]["detail"]["drain_stalled"].is_null());
        assert!(lines[2]["detail"]["drain_stalled"].is_null());
    }

    /// A real listener that accepts `/status` connections and NEVER answers
    /// — what [`observed_drain_stalled`]'s own 2 s bound exists for (a
    /// `SIGSTOP`ped node accepts the connection and never replies), and the
    /// precondition the timestamp-skew test below needs.
    async fn spawn_never_answering_status_server() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                // Held open, never written to: the client blocks until its
                // own timeout rather than seeing a closed connection.
                held.push(stream);
            }
        });
        port
    }

    /// Wall clock in Unix milliseconds — the same clock
    /// [`crate::faultlog::FaultLog::record`] stamps `at_ms` from, so the two
    /// are directly comparable.
    fn now_unix_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    }

    /// The HIGH-1 ACPR finding on issue #204, through the REAL injectors:
    /// `--fault-catalog-outage-node N` and `--fault-discovery-flap-node N`
    /// resolve to the SAME catalog link and `crate::run_network_faults` runs
    /// them concurrently. The flap's per-cycle up phase must not lift the
    /// outage's cut — the pre-fix `set`/`restore` pair was last-writer-wins,
    /// so real traffic crossed a link the outage window still journaled as
    /// cut.
    #[tokio::test]
    async fn a_concurrent_flap_cannot_lift_a_catalog_outages_cut() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-catalog", "127.0.0.1", port)
            .await
            .unwrap();
        // Status port 1 is never listening (this file's own convention), so
        // the disclosure samples fail fast and journal `null`.
        let spec = spec_with_status_port("shared-link-node", 1);
        let (path, log) = scratch_faultlog("shared-link");

        let (outage, flap, crossings) = tokio::join!(
            run_catalog_outage(
                "catalog-outage-0",
                &spec,
                &link,
                Duration::ZERO,
                Duration::from_millis(900),
                &log,
            ),
            run_discovery_flap(
                "discovery-flap-0",
                &spec,
                &link,
                Duration::from_millis(100),
                FlapSchedule {
                    cycles: 4,
                    down: Duration::from_millis(50),
                    up: Duration::from_millis(50),
                },
                &log,
            ),
            async {
                let mut crossings = 0_u32;
                for _ in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if link::test_support::echo_round_trip(&link, b"probe")
                        .await
                        .is_ok()
                    {
                        crossings += 1;
                    }
                }
                crossings
            }
        );
        outage.unwrap();
        flap.unwrap();

        assert_eq!(
            crossings,
            0,
            "no byte may cross while the catalog-outage window is open, even as an overlapping \
             discovery flap ends each of its own down phases: {:#?}",
            window_lines(&path, "catalog-outage-0")
        );
        // And the link is genuinely usable once BOTH windows are done.
        link::test_support::echo_round_trip(&link, b"after")
            .await
            .expect("the link must pass once every window has released its hold");
    }

    /// The MEDIUM-2 ACPR finding on issue #204: a fault window's journaled
    /// `at_ms` must be the moment the real network effect changed, not the
    /// moment a bounded blocking `/status` probe finished. Against a target
    /// that accepts but never answers, [`observed_drain_stalled`] blocks for
    /// its full 2 s bound — the pre-fix code cut the link, THEN blocked,
    /// THEN journaled `Started`, putting ~2 s between the real cut and its
    /// own timestamp. Measured here against the real link: the moment
    /// traffic actually stops crossing versus the timestamp the journal
    /// claims for it.
    #[tokio::test]
    async fn catalog_outage_stamps_started_at_the_real_cut_not_after_the_status_probe() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-catalog", "127.0.0.1", port)
            .await
            .unwrap();
        let status_port = spawn_never_answering_status_server().await;
        let spec = spec_with_status_port("slow-status-node", status_port);
        let (path, log) = scratch_faultlog("catalog-outage-timestamp");

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watcher_done = std::sync::Arc::clone(&done);
        let (outage, first_refusal_ms) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(
                async {
                    let result = run_catalog_outage(
                        "catalog-outage-0",
                        &spec,
                        &link,
                        Duration::ZERO,
                        Duration::from_millis(400),
                        &log,
                    )
                    .await;
                    done.store(true, std::sync::atomic::Ordering::Relaxed);
                    result
                },
                async {
                    let mut first_refusal_ms = None;
                    while !watcher_done.load(std::sync::atomic::Ordering::Relaxed) {
                        if link::test_support::echo_round_trip(&link, b"probe")
                            .await
                            .is_err()
                            && first_refusal_ms.is_none()
                        {
                            first_refusal_ms = Some(now_unix_ms());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    first_refusal_ms
                },
            )
        })
        .await
        .expect("the outage window must complete well within 30s");
        outage.unwrap();
        let first_refusal_ms = first_refusal_ms.expect("the outage must really have cut traffic");

        let lines = window_lines(&path, "catalog-outage-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        let started_at = lines[1]["at_ms"].as_u64().unwrap();
        let skew = started_at.abs_diff(first_refusal_ms);
        assert!(
            skew < 1_000,
            "the Started timestamp must correspond to the real cut, not to the end of a 2s \
             blocking status probe: journaled {started_at}, traffic actually stopped at \
             {first_refusal_ms} ({skew}ms apart): {lines:#?}"
        );

        // The mirror half, on the Ended side: the journaled traffic delta
        // must cover the whole imposed window, not stop 2s short of its own
        // `Ended` line. Every refusal this link ever counted happened inside
        // the window (it passes before and after), so the link's own total
        // and the journaled delta must agree — give or take one connection
        // attempt racing the release itself.
        let journaled_refused = lines[2]["detail"]["link_traffic_during_window"]["conns_refused"]
            .as_u64()
            .unwrap();
        let really_refused = link.stats().conns_refused;
        assert!(
            journaled_refused + 1 >= really_refused,
            "the Ended line's traffic delta must cover the whole window: journaled \
             {journaled_refused} refusals, the link really refused {really_refused}: {lines:#?}"
        );
    }

    /// Discovery flapping really oscillates the link — every configured
    /// cycle, journaled as completed — and leaves it UP at the end. Would
    /// catch an off-by-one in the cycle loop, or a flap that left the
    /// catalog link dropped for every later fault in the schedule.
    #[tokio::test]
    async fn discovery_flap_runs_every_cycle_and_leaves_the_link_up() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-catalog", "127.0.0.1", port)
            .await
            .unwrap();
        let spec = spec_with_status_port("flap-node", 0);
        let (path, log) = scratch_faultlog("flap");

        run_discovery_flap(
            "discovery-flap-0",
            &spec,
            &link,
            Duration::ZERO,
            FlapSchedule {
                cycles: 3,
                down: Duration::from_millis(30),
                up: Duration::from_millis(30),
            },
            &log,
        )
        .await
        .unwrap();

        let lines = window_lines(&path, "discovery-flap-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        assert_eq!(lines[2]["detail"]["cycles_completed"], 3);
        link::test_support::echo_round_trip(&link, b"after")
            .await
            .expect("a flap must end with the link up");
    }

    /// Zero cycles is a vacuous flap — journaled honestly as
    /// `cycles_completed: 0` (the shape #208's vacuity teeth read) with the
    /// link untouched, rather than silently dropping it forever.
    #[tokio::test]
    async fn a_zero_cycle_flap_journals_its_own_vacuity_and_leaves_the_link_up() {
        let port = link::test_support::spawn_echo_server().await;
        let link = FaultLink::bind("n0-catalog", "127.0.0.1", port)
            .await
            .unwrap();
        let spec = spec_with_status_port("flap-node", 0);
        let (path, log) = scratch_faultlog("flap-zero");

        run_discovery_flap(
            "discovery-flap-0",
            &spec,
            &link,
            Duration::ZERO,
            FlapSchedule {
                cycles: 0,
                down: Duration::from_millis(30),
                up: Duration::from_millis(30),
            },
            &log,
        )
        .await
        .unwrap();

        let lines = window_lines(&path, "discovery-flap-0");
        assert_eq!(lines[2]["detail"]["cycles_completed"], 0);
        link::test_support::echo_round_trip(&link, b"after")
            .await
            .expect("a zero-cycle flap must leave the link untouched");
    }

    /// The membership-leave fault against a REAL process: a real `SIGTERM`
    /// (not `SIGKILL` — §8.4's own "not only crash" distinction), confirmed
    /// to have actually ended the process before `Ended` claims it did.
    #[tokio::test]
    async fn membership_leave_sends_a_real_sigterm_and_confirms_the_exit() {
        let mut node = test_support::spawn_sleep(30);
        let (path, log) = scratch_faultlog("leave");

        run_membership_leave(
            "membership-leave-0",
            &mut node,
            Duration::ZERO,
            Duration::from_secs(5),
            &log,
        )
        .await
        .unwrap();

        let lines = window_lines(&path, "membership-leave-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        assert_eq!(lines[1]["detail"]["signal"], "SIGTERM");
        assert_eq!(lines[2]["detail"]["confirmed_exited"], true);
    }

    /// The fail-closed half: a node that IGNORES `SIGTERM` must journal
    /// `confirmed_exited: false` and return an error — a "leave" nobody
    /// left is not a fault window that fired. Proven against a real
    /// process that really traps the signal, not a stub.
    #[tokio::test]
    async fn membership_leave_fails_closed_when_the_node_ignores_sigterm() {
        let mut node = test_support::spawn_ignoring_sigterm();
        let (path, log) = scratch_faultlog("leave-ignored");

        let result = run_membership_leave(
            "membership-leave-0",
            &mut node,
            Duration::ZERO,
            Duration::from_millis(300),
            &log,
        )
        .await;

        assert!(
            result.is_err(),
            "a node that never left must not report a clean leave window"
        );
        let lines = window_lines(&path, "membership-leave-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        assert_eq!(lines[2]["detail"]["confirmed_exited"], false);
    }

    /// The membership-join fault's fail-closed path: a "node" that never
    /// reports ready must journal `confirmed_ready: false` and return an
    /// error, rather than handing back a half-joined process the caller
    /// would treat as a fleet member. (The success path needs a real
    /// `duckspout-daemon` binary and a real catalog — it is covered by
    /// `tests/fault_injection.rs`, which spawns both for real.)
    #[tokio::test]
    async fn membership_join_fails_closed_when_the_node_never_becomes_ready() {
        let joiner = spec_with_status_port("joiner", 1);
        let (path, log) = scratch_faultlog("join-never-ready");

        // `/bin/true` "boots" and exits immediately, never serving
        // `/status` — `process::wait_until_ready`'s own early-exit branch.
        let result = run_membership_join(
            "membership-join-0",
            std::path::Path::new("/bin/true"),
            &joiner,
            Duration::ZERO,
            Duration::from_secs(2),
            &log,
        )
        .await;

        assert!(result.is_err(), "a join that never joined must fail closed");
        let lines = window_lines(&path, "membership-join-0");
        assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
        assert_eq!(lines[2]["detail"]["confirmed_ready"], false);
    }

    /// The Flight-kill fault must fail closed BEFORE killing anything when
    /// the stream it is supposed to interrupt never exists — otherwise it
    /// would kill a node and journal a "mid-stream" window with no stream
    /// in it at all. Proven by asserting the target is still alive
    /// afterwards, and that only the `Armed` line was journaled.
    #[tokio::test]
    async fn flight_kill_fails_closed_without_killing_when_no_flight_server_answers() {
        let mut node = test_support::spawn_sleep(30);
        node.spec.flight_port = 1; // privileged, never listening
        let pid = process::pid(&node).unwrap();
        let (path, log) = scratch_faultlog("flight-kill-no-server");

        let result = run_flight_kill_mid_stream(
            "flight-kill-mid-stream-0",
            &mut node,
            "SELECT 1",
            Duration::ZERO,
            Duration::from_millis(500),
            &log,
        )
        .await;

        assert!(
            result.is_err(),
            "an unreachable Flight server must fail the fault, not fire it"
        );
        assert!(
            process::is_live_running(pid).unwrap_or(false),
            "the target must NOT have been killed by a fault that never fired"
        );
        assert_eq!(
            phases(&window_lines(&path, "flight-kill-mid-stream-0")),
            vec!["armed"]
        );
        process::send_signal(&node, "-KILL").await.unwrap();
    }

    /// The exact HIGH-severity ACPR finding, reproduced end to end through
    /// the real [`run_sigstop_pause`] entry point (not just the lower-level
    /// `process::is_stopped`/`is_live_running` primitives, `crate::process`'s
    /// own tests for those): a target that has already exited on its own
    /// (a real zombie, never reaped) before this injector's `SIGSTOP` must
    /// make `run_sigstop_pause` return an error and journal ONLY the
    /// `Armed` phase — never a `Started` line, and never the false
    /// `confirmed_resumed: true` the pre-fix code would have fabricated for
    /// a node that crashed on its own.
    #[tokio::test]
    async fn run_sigstop_pause_refuses_a_false_positive_against_an_already_exited_zombie() {
        let mut node = test_support::spawn_short_lived();
        let pid = process::pid(&node).unwrap();
        // Let the (near-instant) `true` process actually exit — deliberately
        // never `wait`/`try_wait`ed on, so the OS keeps it as a real zombie
        // for the whole test (the exact precondition the ACPR's own
        // empirical repro relies on; `crate::process`'s own
        // `zombie_process_reads_as_neither_stopped_nor_live_running` test
        // proves the OS mechanics in isolation).
        let became_zombie = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if std::fs::read_to_string(format!("/proc/{pid}/stat"))
                    .ok()
                    .and_then(|stat| {
                        stat.rsplit_once(')')
                            .map(|(_, r)| r.trim_start().starts_with('Z'))
                    })
                    .unwrap_or(false)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        assert!(
            became_zombie,
            "the spawned `true` process must become a real zombie before this test proceeds"
        );

        let (path, log) = scratch_faultlog("sigstop-zombie");
        let result = run_sigstop_pause(
            "pause-zombie",
            &mut node,
            Duration::ZERO,
            Duration::from_millis(50),
            &log,
        )
        .await;

        assert!(
            result.is_err(),
            "run_sigstop_pause must refuse a zombie target, not report a clean fault window"
        );

        let lines = read_ndjson_lines(&path);
        let phases: Vec<&str> = lines
            .iter()
            .map(|line| line["phase"].as_str().unwrap())
            .collect();
        assert_eq!(
            phases,
            vec!["armed"],
            "a zombie target must journal ONLY Armed — never a Started phase for a pause that \
             never actually took effect: {lines:#?}"
        );
    }

    /// A journal file with a `PutPart` line among others is detected;
    /// one without it, or with only unrelated events, is not. Would catch
    /// a substring-matching shortcut (e.g. `contents.contains("PutPart")`)
    /// that a payload field or another event's name could false-positive
    /// on, since this test's "decoy" line contains the string `PutPart`
    /// outside the `event` field.
    #[test]
    fn journal_contains_event_matches_only_the_event_field() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-journal-match",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.ndjson");

        std::fs::write(&path, "").unwrap();
        assert!(!journal_contains_event_after(&path, "PutPart", 0));

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, r#"{{"node":"n0","seq":0,"event":"SealPart"}}"#).unwrap();
        writeln!(
            file,
            r#"{{"node":"n0","seq":1,"event":"Reconcile","note":"not a PutPart"}}"#
        )
        .unwrap();
        assert!(
            !journal_contains_event_after(&path, "PutPart", 0),
            "a decoy string in a non-`event` field must not false-positive"
        );

        writeln!(file, r#"{{"node":"n0","seq":2,"event":"PutPart"}}"#).unwrap();
        assert!(journal_contains_event_after(&path, "PutPart", 0));
    }

    /// A missing journal file reads as "not found yet," never a panic or an
    /// error the caller must special-case — the injector may start
    /// watching before the target has written anything at all.
    #[test]
    fn journal_contains_event_tolerates_a_missing_file() {
        let missing =
            std::env::temp_dir().join("duckspout-fleet-fault-test-missing-journal.ndjson");
        assert!(!journal_contains_event_after(&missing, "PutPart", 0));
    }

    /// The exact MEDIUM-HIGH ACPR finding: a `PutPart` line already sitting
    /// in the journal BEFORE the injector starts watching (e.g. a
    /// sustained-load generator that produced load before the fault
    /// triggers) must NOT be treated as the "new" line this injector is
    /// waiting for — only a line appended AFTER `skip_lines` counts.
    #[test]
    fn journal_contains_event_after_ignores_a_stale_pre_existing_line() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-stale-line",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.ndjson");

        std::fs::write(&path, "").unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, r#"{{"node":"n0","seq":0,"event":"PutPart"}}"#).unwrap();
        let anchor = 1; // the injector arms after this one pre-existing line

        assert!(
            !journal_contains_event_after(&path, "PutPart", anchor),
            "a PutPart line already present before the anchor must not count as a NEW one"
        );

        writeln!(file, r#"{{"node":"n0","seq":1,"event":"PutPart"}}"#).unwrap();
        assert!(
            journal_contains_event_after(&path, "PutPart", anchor),
            "a PutPart line appended AFTER the anchor must count as the NEW one"
        );
    }

    /// `wait_for_journal_event` returns as soon as the line appears —
    /// proven by writing it from a background task after a short delay and
    /// asserting the wait resolves well before its own generous timeout.
    #[tokio::test]
    async fn wait_for_journal_event_resolves_once_the_line_appears() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-wait",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.ndjson");
        std::fs::write(&path, "").unwrap();

        let writer_path = path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&writer_path)
                .unwrap();
            writeln!(file, r#"{{"node":"n0","seq":0,"event":"PutPart"}}"#).unwrap();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_journal_event(&path, "PutPart", 0, Duration::from_secs(5)),
        )
        .await;
        assert!(result.is_ok(), "outer timeout must not fire");
        assert!(result.unwrap().is_ok(), "the wait itself must succeed");
    }

    /// `wait_for_journal_event` waits for a NEW `PutPart` line, not a stale
    /// one already sitting in the journal when it starts watching — the
    /// end-to-end version of `journal_contains_event_after_ignores_a_stale_pre_existing_line`
    /// through the actual polling loop `crate::fault::run_node_kill` calls.
    #[tokio::test]
    async fn wait_for_journal_event_waits_for_a_new_line_not_a_preexisting_one() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-wait-anchor",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.ndjson");
        std::fs::write(&path, "").unwrap();
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(file, r#"{{"node":"n0","seq":0,"event":"PutPart"}}"#).unwrap();
        }
        let anchor = 1;

        // With a short timeout and no NEW line ever written, the stale
        // pre-existing line must NOT satisfy the wait.
        let result =
            wait_for_journal_event(&path, "PutPart", anchor, Duration::from_millis(150)).await;
        assert!(
            result.is_err(),
            "a stale pre-existing PutPart line must not satisfy the wait"
        );

        // Once a genuinely NEW line is appended, the wait resolves.
        let writer_path = path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&writer_path)
                .unwrap();
            writeln!(file, r#"{{"node":"n0","seq":1,"event":"PutPart"}}"#).unwrap();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_journal_event(&path, "PutPart", anchor, Duration::from_secs(5)),
        )
        .await;
        assert!(result.is_ok(), "outer timeout must not fire");
        assert!(
            result.unwrap().is_ok(),
            "the wait must succeed once a NEW line is appended"
        );
    }

    /// `wait_for_journal_event` fails (not hangs forever) when the line
    /// never appears — the exact case a drive-load pass that produces no
    /// drainable window would hit (module docs of `KillTiming::MidDrainCommit`).
    #[tokio::test]
    async fn wait_for_journal_event_times_out_when_the_line_never_appears() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-fault-test-{}-timeout",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.ndjson");
        std::fs::write(&path, "").unwrap();

        let result = wait_for_journal_event(&path, "PutPart", 0, Duration::from_millis(150)).await;
        assert!(result.is_err());
    }
}
