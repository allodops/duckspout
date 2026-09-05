//! Real end-to-end fault injection (§8.4, issues #203 and #204): spawns the REAL
//! `duckspout-fleet` binary — which itself spawns a REAL `duckspout-daemon`
//! process — with a fault armed, waits for the run to finish, and asserts
//! on the REAL NDJSON journals it left on disk. This is the "actually
//! launches real processes and verifies real behavior" rigor level this
//! issue's own testing-discipline guidance calls for; `crate::fault`'s and
//! `crate::faultlog`'s own unit tests already cover the injector logic and
//! journal format against test doubles/real subprocesses in isolation —
//! this file is the seam proof that the CLI wiring, the daemon-side
//! `--fault-drain-commit-delay-ms` stall, and the injectors all compose
//! correctly end to end.
//!
//! **Skips gracefully without a reachable Postgres** — the same posture
//! `duckspout-daemon/tests/trace_capture_real_backends.rs` documents for
//! the daemon crate: if `DUCKSPOUT_FLEET_TEST_POSTGRES_DSN` is unset, this
//! test prints why and returns, so a contributor's plain `cargo test` never
//! needs Postgres running. Every scenario here also passes `--local-lake`
//! (§8.4 calls for real `MinIO` in the FULL nightly `ctk-distributed` run,
//! issue #58 — this file only needs a real Postgres catalog to exercise a
//! real drain commit for real fault timing, so it takes the lighter
//! escape hatch `duckspout-fleet --local-lake` already documents for
//! exactly this "no `MinIO` running" situation).
//!
//! **`DUCKSPOUT_FLEET_TEST_POSTGRES_DSN` must be the libpq keyword/value
//! form** (e.g. `postgres:host=127.0.0.1 port=5432 dbname=duckspout_catalog
//! user=duckspout`), **not** the `postgres://user@host/db` URI form
//! `duckspout-fleet --postgres-dsn`'s own CLI default documents — issue
//! #212 (filed while verifying this file): `DuckLake`'s real `ATTACH` does
//! not parse the URI form at all and silently falls through to treating the
//! whole string as a local file path. `host`/`port` must name a real TCP
//! address (not a Unix-socket directory): every scenario here runs the
//! fleet's own backend-reachability probe, which this issue taught to read
//! the keyword form (`crate::dsn`), so the probe now covers this file too
//! instead of being skipped past — an ACPR finding (LOW-9: the
//! `--skip-backend-check` this file used to pass was justified by a
//! limitation issue #204 itself removed).
//! `duckspout-fleet`'s own `--postgres-password` CLI default
//! (`duckspout-dev`) matches `deploy/compose/compose.yaml`'s dev
//! credential, so only the DSN itself needs overriding via env var.
//!
//! **`--nodes 1` deliberately, not a real multi-node fleet** — issue #213
//! (also filed while verifying this file): more than one node cold-booting
//! CONCURRENTLY against a genuinely fresh Postgres catalog races `DuckLake`'s
//! own metadata-table initialization and one node loses. Neither fault
//! this issue implements needs more than one node to exercise for real
//! (the mid-drain kill is about ONE node's own local drain sequence; the
//! `SIGSTOP` pause's OS-level mechanics are equally real against a
//! single-node fleet — module docs of `crate::fault` for exactly what a
//! multi-node fleet would be needed for and is NOT yet verified here: a
//! peer actually rejecting the resumed incarnation). #204's faults hold to
//! the same bound, with one deliberate exception: the membership-JOIN test
//! boots a SECOND node — but only after the first is already up and has
//! initialized the catalog, which is exactly the sequencing #213's race
//! does not apply to (it is about CONCURRENT cold boots).
//!
//! **Every scenario runs under its OWN tenant** (`--tenant`, the real
//! `X-Scope-OrgID` admission header), unique per scenario and per
//! execution. A tenant is what a partition is keyed by, and the catalog is
//! shared and persistent: a scenario that (deliberately!) kills a node with
//! staged-but-undrained data leaves that partition with a real coverage
//! hole, and the daemon is right to stall its watermark there on every
//! later boot — a wiped hot store IS lost data. Without its own tenant, the
//! NEXT scenario would inherit that stall and never drain anything of its
//! own: empirically, the mid-drain-kill scenario's `PutPart` watch timed out
//! after 40s with the node's own stdout reporting exactly that stall
//! (`watermark stalled below a recorded window at boot ... CoverageHoles`).
//! Per-tenant isolation is how a real multi-tenant fleet keeps one
//! workload's damage out of another's, so it is also the honest fix here —
//! not a suppression of the stall, which stays fully live for the tenant
//! that actually lost data.
//!
//! **Every test below shares one [`POSTGRES_CATALOG`] mutex, one
//! `.config/nextest.toml` serialization group, and one fixed work dir.**
//! The mutex serializes them under a plain `cargo test` (one process, many
//! threads); the nextest group serializes them under this repo's actual
//! runner, which gives every test its OWN process and so cannot be
//! serialized by any in-process lock. Both are needed, and each covers what
//! the other cannot. A `DuckLake` catalog pins its `DATA_PATH` for its whole
//! lifetime (`crates/duckspout-fleet/src/main.rs`'s own `--s3-prefix` doc
//! comment makes the same point for the S3 case; it applies identically to
//! `--local-lake`'s local-filesystem path) — two tests attaching the SAME
//! real Postgres catalog from two DIFFERENT work dirs would only ever let
//! the first one through. Running them concurrently would additionally
//! risk issue #213's own race on a fresh catalog — and, plainly, collide
//! on the fleet's own default ports, which every scenario here binds.
//! Sharing one work dir and serializing (mutex + nextest group) sidesteps
//! all of it without this file needing to reset Postgres itself between
//! tests.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

/// Serializes the tests below against the one shared real Postgres
/// catalog (module docs above).
static POSTGRES_CATALOG: Mutex<()> = Mutex::new(());

fn postgres_dsn_from_env() -> Option<String> {
    std::env::var("DUCKSPOUT_FLEET_TEST_POSTGRES_DSN").ok()
}

fn postgres_password_from_env() -> String {
    std::env::var("DUCKSPOUT_FLEET_TEST_POSTGRES_PASSWORD")
        .unwrap_or_else(|_| "duckspout-dev".to_owned())
}

fn fleet_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_duckspout-fleet"))
}

/// Reads `path` as NDJSON, silently skipping any line that fails to parse
/// (a torn last line mid-write, e.g. if the run was still flushing when
/// this reads it — best-effort, matching `crate::fault`'s own journal
/// reader). An unreadable/missing file reads as no lines at all.
fn read_ndjson_lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .map(|contents| {
            contents
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The one work dir every test here shares (module docs above for why it
/// must be fixed and shared, not per-scenario). Per-run artifacts — node
/// directories, the fault journal, the secret files — are wiped fresh by
/// every caller; **`lake/` is deliberately NOT wiped.**
///
/// # Why the lake survives (a real failure this caught)
///
/// The shared Postgres catalog and the lake directory are ONE unit: the
/// catalog holds the file list, `lake/` holds the files, and the catalog
/// outlives any single test (nothing here can drop a Postgres database —
/// this crate has no Postgres client). Wiping `lake/` while leaving the
/// catalog in place therefore leaves the catalog pointing at data files
/// that no longer exist, and a later test's drain never reaches `PutPart`
/// at all. Empirically: with #203's two tests the mid-drain kill happened
/// to run first and never saw it; with #204's five more ahead of it in the
/// same file it failed outright (`["armed"]` and no `PutPart` in the node's
/// own journal), and passed again against a freshly created catalog
/// database — i.e. the wipe, not the fault, was the defect.
fn shared_work_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("duckspout-fleet-fault-injection-test");
    std::fs::create_dir_all(&dir).unwrap();
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        if entry.file_name() == "lake" {
            continue;
        }
        let path = entry.path();
        let removed = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        removed.unwrap_or_else(|e| panic!("wiping {}: {e}", path.display()));
    }
    dir
}

/// The arguments every scenario in this file shares (module docs above for
/// `--nodes 1` and `--local-lake`), plus `extra`. One home for them so a
/// scenario's own arguments are the only thing that differs between tests.
///
/// The backend-reachability probe deliberately runs (no
/// `--skip-backend-check`): it can read the keyword-form DSN this file uses
/// as of issue #204, so leaving it on is real coverage of the probe against
/// the real Postgres these tests already require.
fn fleet_command(work_dir: &Path, postgres_dsn: &str, scenario: &str, extra: &[&str]) -> Command {
    let mut command = Command::new(fleet_bin());
    command.args([
        "--nodes",
        "1",
        "--local-lake",
        "--postgres-dsn",
        postgres_dsn,
        "--postgres-password",
        &postgres_password_from_env(),
        "--work-dir",
        work_dir.to_str().unwrap(),
        "--boot-timeout-secs",
        "30",
        "--tenant",
        &scenario_tenant(scenario),
    ]);
    command.args(extra);
    command
}

/// This execution's own tenant — unique per scenario AND per execution
/// (module docs above for why both halves are needed).
fn scenario_tenant(scenario: &str) -> String {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    // The server's tenant charset is `[A-Za-z0-9._-]` with `_` reserved as
    // a leading character for system tenants
    // (`duckspout_accept::otlp::tenant_from_header`).
    format!("fleet204-{scenario}-{stamp}")
}

/// Every line of one fault window, in journaled order.
fn window_lines(work_dir: &Path, fault_id: &str) -> Vec<serde_json::Value> {
    read_ndjson_lines(&work_dir.join("faults.ndjson"))
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

/// Node 0's own real D-6 NDJSON journal, as parsed lines.
fn node0_journal(work_dir: &Path) -> Vec<serde_json::Value> {
    read_ndjson_lines(&work_dir.join("node-0").join("journal.ndjson"))
}

/// The real §8.4 "sharpest" node-kill fault: a real `SIGKILL`, timed via
/// the real `--fault-drain-commit-delay-ms` stall + real `PutPart`-journal
/// watch, must land strictly between the killed node's `PutPart` and any
/// `LakeCommit*` outcome — proven by reading that node's own real NDJSON
/// journal after the run, not by asserting anything about the fleet
/// binary's own exit code (it is deliberately not the judge, `main.rs`'s
/// own module docs).
///
/// # The load-vs-fault arithmetic (an ACPR finding, MEDIUM-HIGH-2)
///
/// An earlier version of this test used `--load-batches 15
/// --load-interval-ms 150` (total load wall-clock ≈ 14×150ms ≈ 2.1s), while
/// the mid-drain kill only fires once `--hot-window 2s` + `--allowed-lateness
/// 1s` (≈3s of simulated event time, plus daemon-side scheduling latency)
/// has produced a `PutPart` line — i.e. the load pass had ALREADY finished
/// by the time the kill fired, proving nothing about the system under load
/// (exactly the failure mode this module's own doc comment warns against).
/// `LOAD_BATCHES` below is sized so the load pass's wall-clock floor,
/// `(LOAD_BATCHES - 1) * LOAD_INTERVAL_MS`, comfortably EXCEEDS that ≈3s
/// PutPart-arrival estimate — verified after the run, not merely assumed,
/// via the real node-0 journal's own `Accept` count (module docs at the
/// assertion below).
#[test]
fn node_kill_mid_drain_lands_strictly_between_putpart_and_lakecommit() {
    // (LOAD_BATCHES - 1) * LOAD_INTERVAL_MS ≈ 79 * 150ms ≈ 11.85s of load
    // wall-clock — comfortably past the ≈3s PutPart estimate above.
    const LOAD_BATCHES: u32 = 80;
    const LOAD_INTERVAL_MS: u32 = 150;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!(
            "fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping (Docker-optional \
             dev convenience, matching duckspout-daemon/tests/trace_capture_real_backends.rs's \
             own posture; the nightly ctk-distributed job, issue #58, does not inherit this skip)"
        );
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "node-kill-mid-drain",
        &[
            "--hot-window",
            "2s",
            "--allowed-lateness",
            "1s",
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "30",
            "--fault-kill-node",
            "0",
            "--fault-kill-mid-drain",
            "--fault-kill-drain-stall-ms",
            "2000",
            "--fault-kill-mid-drain-timeout-secs",
            "40",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let faults = read_ndjson_lines(&work_dir.join("faults.ndjson"));
    let phases: Vec<&str> = faults
        .iter()
        .filter(|line| line["fault_id"] == "node-kill-0")
        .map(|line| line["phase"].as_str().unwrap())
        .collect();
    assert_eq!(
        phases,
        vec!["armed", "started", "ended"],
        "the node-kill fault must journal its full Armed/Started/Ended lifecycle: {faults:#?}"
    );

    let node0_journal = read_ndjson_lines(&work_dir.join("node-0").join("journal.ndjson"));
    let last_putpart = node0_journal
        .iter()
        .rposition(|line| line["event"] == "PutPart")
        .unwrap_or_else(|| {
            panic!(
                "the killed node must have journaled at least one PutPart before dying: {node0_journal:#?}"
            )
        });
    let a_commit_outcome_follows_the_last_putpart =
        node0_journal[last_putpart + 1..].iter().any(|line| {
            matches!(
                line["event"].as_str(),
                Some("LakeCommitOk" | "LakeCommitAbort" | "LakeCommitIndeterminate")
            )
        });
    assert!(
        !a_commit_outcome_follows_the_last_putpart,
        "the real SIGKILL must have landed strictly between PutPart and any LakeCommit outcome; \
         node-0's journal: {node0_journal:#?}"
    );

    // The MEDIUM-HIGH-2 ACPR finding's own verification ask: prove the kill
    // genuinely fired DURING active load, not after it had already
    // finished. `TraceEvent::Accept` journals once per admitted OTLP export
    // batch (`duckspout-staging/src/stager.rs`'s own `stage_blocking`) — if
    // the real SIGKILL had fired only after the whole drive-load pass
    // finished, node-0's own journal would show all `LOAD_BATCHES` Accepts;
    // strictly fewer is direct evidence the load pass was still in flight,
    // mid-send, when the kill landed.
    let accept_count = node0_journal
        .iter()
        .filter(|line| line["event"] == "Accept")
        .count();
    assert!(
        accept_count < LOAD_BATCHES as usize,
        "the kill must land while load is still active: node-0 accepted {accept_count} of \
         {LOAD_BATCHES} batches — a count equal to (or exceeding) {LOAD_BATCHES} would mean the \
         load pass had already finished before the kill fired"
    );
}

/// The real SIGSTOP-pause fault: a real `SIGSTOP`, then a real `SIGCONT` —
/// both confirmed via `/proc/<pid>/stat` (`crate::process::is_stopped`/
/// `is_live_running`), proven by reading `faults.ndjson` after the run.
/// `crate::fault`'s own module docs disclose exactly what this does NOT
/// verify (a peer actually rejecting the resumed incarnation — blocked on
/// daemon composition not yet wiring `fence_boot`/`FenceTable` at all) and
/// where that IS verified for real:
/// `crates/duckspout-replication/tests/fenced_zombie_pause_and_promote.rs`.
///
/// # The pause duration vs. `HEARTBEAT_TTL_SECS` (an ACPR finding, honesty
/// correction)
///
/// §8.4 calls for the pause to be "long enough to expire claims," i.e. past
/// `duckspout_daemon::constants::HEARTBEAT_TTL_SECS` (15s) — an earlier
/// version of this doc comment claimed the pause was held past that
/// threshold, while `--fault-sigstop-duration-secs` below is actually only
/// 3s. This is a deliberate, disclosed proxy, not the claim this doc
/// comment used to make: a real 15s+ hold is easy to add but slows every
/// `cargo test`/CI run of this file by 15+ seconds for a duration this test
/// does not otherwise need — everything it actually asserts (a real
/// `SIGSTOP`/`SIGCONT` round trip, confirmed via `/proc/<pid>/stat`, and
/// that it lands while load is genuinely still active, module docs below)
/// is exercised identically at 3s as at 15s+. The `HEARTBEAT_TTL_SECS`-scale
/// hold itself is not this file's concern: `duckspout-fleet` has no live
/// heartbeat/claim-expiry peer to observe yet at all (this file's own
/// module docs on the daemon-composition gap), so a longer hold here would
/// not exercise any additional real behavior — only wall-clock time.
///
/// # The load-vs-fault arithmetic (an ACPR finding, MEDIUM-HIGH-2)
///
/// An earlier version of this test used `--load-batches 5
/// --load-interval-ms 100` (total load wall-clock ≈ 4×100ms ≈ 0.4s), firing
/// `SIGSTOP` only after `--fault-sigstop-delay-secs 1` — i.e. the load pass
/// had ALREADY finished by the time the pause even started, proving nothing
/// about the system under load. `LOAD_BATCHES`/`LOAD_INTERVAL_MS` below are
/// sized so the load pass's wall-clock floor, `(LOAD_BATCHES - 1) *
/// LOAD_INTERVAL_MS` ≈ 39×150ms ≈ 5.85s, comfortably exceeds
/// `fault_sigstop_delay_secs + fault_sigstop_duration_secs` = 1+3 = 4s —
/// verified after the run, not merely assumed, via the real node-0
/// journal's own `Accept` count sliced at the fault's own journaled
/// `node_journal_lines` anchor (module docs at the assertion below).
#[test]
fn sigstop_pause_is_a_real_confirmed_stop_and_resume() {
    const LOAD_BATCHES: u32 = 40;
    const LOAD_INTERVAL_MS: u32 = 150;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!(
            "fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping (see the other \
             test in this file for the full rationale)"
        );
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "sigstop-pause",
        &[
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "5",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "15",
            "--fault-sigstop-node",
            "0",
            "--fault-sigstop-delay-secs",
            "1",
            "--fault-sigstop-duration-secs",
            "3",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let faults = read_ndjson_lines(&work_dir.join("faults.ndjson"));
    let this_fault: Vec<&serde_json::Value> = faults
        .iter()
        .filter(|line| line["fault_id"] == "sigstop-pause-0")
        .collect();
    let phases: Vec<&str> = this_fault
        .iter()
        .map(|line| line["phase"].as_str().unwrap())
        .collect();
    assert_eq!(
        phases,
        vec!["armed", "started", "ended"],
        "the sigstop-pause fault must journal its full Armed/Started/Ended lifecycle: {faults:#?}"
    );

    let started = this_fault[1];
    assert_eq!(
        started["detail"]["confirmed_stopped"], true,
        "a real SIGSTOP must be confirmed via /proc/<pid>/stat: {started:#?}"
    );
    let ended = this_fault[2];
    assert_eq!(
        ended["detail"]["confirmed_resumed"], true,
        "a real SIGCONT must be confirmed via /proc/<pid>/stat: {ended:#?}"
    );

    let started_at = started["at_ms"].as_u64().unwrap();
    let ended_at = ended["at_ms"].as_u64().unwrap();
    assert!(
        Duration::from_millis(ended_at - started_at) >= Duration::from_secs(3),
        "the journaled window must span at least the configured 3s pause"
    );

    // The MEDIUM-HIGH-2 ACPR finding's own verification ask: prove the
    // pause genuinely fired DURING active load, not after it had already
    // finished. `Started`'s own journaled `node_journal_lines` (MEDIUM-4's
    // fix) is an exact seq anchor into node-0's own real D-6 journal at the
    // moment the SIGSTOP was confirmed — the journal is append-only, so
    // slicing the FINAL journal at that same line count reproduces exactly
    // what existed at that moment. If the load pass had already finished
    // sending before the pause began, ALL `LOAD_BATCHES` `Accept` lines
    // would already be in that slice; strictly fewer is direct evidence the
    // load pass was still incomplete when the pause began.
    let started_anchor =
        usize::try_from(started["detail"]["node_journal_lines"].as_u64().unwrap()).unwrap();
    let node0_journal = read_ndjson_lines(&work_dir.join("node-0").join("journal.ndjson"));
    assert!(
        started_anchor <= node0_journal.len(),
        "the journal must only ever grow: anchor {started_anchor} exceeds final length {}",
        node0_journal.len()
    );
    let accept_count_before_pause = node0_journal[..started_anchor]
        .iter()
        .filter(|line| line["event"] == "Accept")
        .count();
    assert!(
        accept_count_before_pause < LOAD_BATCHES as usize,
        "the pause must land while load is still active: node-0 had accepted only \
         {accept_count_before_pause} of {LOAD_BATCHES} batches by the time SIGSTOP was \
         confirmed — a count reaching {LOAD_BATCHES} would mean the load pass had already \
         finished before the pause began"
    );
}

/// §8.4's catalog-outage fault, end to end through the real CLI: the node's
/// real link to the real Postgres catalog is cut for a window while load is
/// still flowing, and the journal proves both halves of the evidence §8.4's
/// own predicate needs — that the outage really cut real traffic (zero
/// bytes crossed the link, and a real established connection was cut or a
/// real connection attempt refused), and that ingest kept being accepted
/// while the catalog was gone ("ingest must continue undegraded").
///
/// The `Accept` count is sliced at the outage's own journaled
/// `node_journal_lines` anchor (the same technique the SIGSTOP test above
/// uses): lines after that anchor were journaled after the outage began.
#[test]
fn catalog_outage_cuts_the_real_catalog_link_while_ingest_keeps_being_accepted() {
    const LOAD_BATCHES: u32 = 60;
    const LOAD_INTERVAL_MS: u32 = 200;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    // (60 - 1) * 200ms ≈ 11.8s of load wall clock, so the 3s..9s outage
    // window sits comfortably INSIDE the load pass.
    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "catalog-outage",
        &[
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "20",
            "--fault-catalog-outage-node",
            "0",
            "--fault-catalog-outage-delay-secs",
            "3",
            "--fault-catalog-outage-duration-secs",
            "6",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let lines = window_lines(&work_dir, "catalog-outage-0");
    assert_eq!(
        phases(&lines),
        vec!["armed", "started", "ended"],
        "the catalog-outage fault must journal its full lifecycle: {lines:#?}"
    );

    let traffic = &lines[2]["detail"]["link_traffic_during_window"];
    assert_eq!(
        traffic["bytes_client_to_server"], 0,
        "no catalog byte may cross during the outage: {lines:#?}"
    );
    let cut = traffic["conns_cut"].as_u64().unwrap();
    let refused = traffic["conns_refused"].as_u64().unwrap();
    assert!(
        cut + refused > 0,
        "the outage must have disrupted a real catalog connection (cut {cut}, refused \
         {refused}) — zero of both would mean the node never used this link, making the fault \
         vacuous: {lines:#?}"
    );

    let started_anchor =
        usize::try_from(lines[1]["detail"]["node_journal_lines"].as_u64().unwrap()).unwrap();
    let journal = node0_journal(&work_dir);
    assert!(started_anchor <= journal.len());
    let accepts_during_and_after = journal[started_anchor..]
        .iter()
        .filter(|line| line["event"] == "Accept")
        .count();
    assert!(
        accepts_during_and_after > 0,
        "§8.4: ingest must continue undegraded through a catalog outage — node 0 journaled no \
         Accept at all after the outage began (anchor {started_anchor} of {} lines)",
        journal.len()
    );
}

/// §8.4's discovery-flapping fault, end to end: the node's real catalog
/// link really oscillates for every configured cycle, and the window
/// journals the completed count plus the real traffic disruption it caused.
/// `fault::run_discovery_flap`'s own docs carry what is NOT observable yet
/// (routing convergence — there is no registry to converge).
#[test]
fn discovery_flap_oscillates_the_real_catalog_link_for_every_cycle() {
    const CYCLES: u32 = 4;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "discovery-flap",
        &[
            "--load-batches",
            "60",
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            "200",
            "--settle-timeout-secs",
            "20",
            "--fault-discovery-flap-node",
            "0",
            "--fault-discovery-flap-delay-secs",
            "3",
            "--fault-discovery-flap-cycles",
            &CYCLES.to_string(),
            "--fault-discovery-flap-down-ms",
            "700",
            "--fault-discovery-flap-up-ms",
            "700",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let lines = window_lines(&work_dir, "discovery-flap-0");
    assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
    assert_eq!(
        lines[2]["detail"]["cycles_completed"], CYCLES,
        "every configured flap cycle must actually run: {lines:#?}"
    );
    let traffic = &lines[2]["detail"]["link_traffic_during_window"];
    let cut = traffic["conns_cut"].as_u64().unwrap();
    let refused = traffic["conns_refused"].as_u64().unwrap();
    assert!(
        cut + refused > 0,
        "flapping must really disrupt the real catalog link (cut {cut}, refused {refused}): \
         {lines:#?}"
    );
}

/// §8.4's Flight-server-kill-mid-stream fault, end to end against the REAL
/// Arrow Flight server: a real hot query's stream is opened, the node is
/// `SIGKILL`ed while the stream still has unsent data (the flow-control
/// argument in `fault::run_flight_kill_mid_stream`'s own docs), and the
/// client's observed outcome is journaled. §7's requirement — "the client's
/// typed error, never a silently truncated result" — is what the
/// `terminal_outcome` assertion below pins: a `clean_end_of_stream` here
/// would mean the client was handed a truncated result as if it were
/// complete.
#[test]
fn flight_kill_mid_stream_gives_the_client_a_typed_error_not_a_truncated_result() {
    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "flight-kill",
        &[
            "--load-batches",
            "40",
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            "200",
            "--settle-timeout-secs",
            "15",
            "--fault-flight-kill-node",
            "0",
            "--fault-flight-kill-delay-secs",
            "3",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let lines = window_lines(&work_dir, "flight-kill-mid-stream-0");
    assert_eq!(
        phases(&lines),
        vec!["armed", "started", "ended"],
        "the Flight-kill fault must journal its full lifecycle: {lines:#?}"
    );
    assert_eq!(
        lines[2]["detail"]["client_outcome"]["terminal_outcome"], "typed_error",
        "§7: a stream whose server died mid-stream must end in a typed error, never a clean \
         (silently truncated) end of stream: {lines:#?}"
    );
    assert_eq!(
        lines[2]["detail"]["confirmed_exited"], true,
        "the Flight server's own process must be confirmed dead: {lines:#?}"
    );
}

/// §8.4's membership churn, LEAVE half: a running node leaves gracefully
/// under load — a real `SIGTERM` and the daemon's own §9.1.2 shutdown, not
/// a crash.
///
/// # Why leave and join are two scenarios, not one (an ACPR finding on
/// issue #204, MEDIUM-6)
///
/// They used to share one run, with `--fault-churn-leave-node 0` firing at
/// t≈3s and `--fault-churn-join` at t≈4s (the process faults run
/// SEQUENTIALLY — `crate::run_process_faults` — so the join only starts
/// once the leave has completed). With `--nodes 1` the sole load target is
/// the node that left, so by the time the join fired there was nothing left
/// alive to be "under load": every remaining batch simply failed at the
/// connection, and the join-under-load half was vacuous — the same class of
/// vacuity ACPR already caught in #203 for the kill/`SIGSTOP` scenarios.
/// Splitting them lets each half fire while load is genuinely flowing, and
/// keeps `--nodes 1` (this file's own module docs: a second CONCURRENTLY
/// cold-booting node races `DuckLake`'s metadata init, issue #213).
///
/// # The load-vs-fault arithmetic
///
/// `(LOAD_BATCHES - 1) * LOAD_INTERVAL_MS` ≈ 59×200 ms ≈ 11.8 s of load
/// wall clock, against a leave that fires at `LEAVE_DELAY_SECS` = 3 s —
/// and it is verified after the run rather than assumed, from node-0's own
/// real journal sliced at the leave's own journaled `node_journal_lines`
/// anchor.
#[test]
fn membership_leave_departs_gracefully_while_load_is_still_flowing() {
    const LOAD_BATCHES: u32 = 60;
    const LOAD_INTERVAL_MS: u32 = 200;
    const LEAVE_DELAY_SECS: u32 = 3;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    const {
        assert!(
            (LOAD_BATCHES - 1) * LOAD_INTERVAL_MS > LEAVE_DELAY_SECS * 1_000,
            "the load pass must still be running when the leave fires"
        );
    }
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "membership-leave",
        &[
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "15",
            "--fault-churn-leave-node",
            "0",
            "--fault-churn-leave-delay-secs",
            &LEAVE_DELAY_SECS.to_string(),
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let leave = window_lines(&work_dir, "membership-leave-0");
    assert_eq!(phases(&leave), vec!["armed", "started", "ended"]);
    assert_eq!(
        leave[1]["detail"]["signal"], "SIGTERM",
        "a leave is a graceful departure, not a crash (§8.4): {leave:#?}"
    );
    assert_eq!(
        leave[2]["detail"]["confirmed_exited"], true,
        "the leaving node must actually have left: {leave:#?}"
    );

    // The vacuity teeth: node-0's own `Accept` count at the moment the
    // `SIGTERM` was sent (`Started`'s journaled `node_journal_lines` is an
    // exact seq anchor into the append-only journal, the same technique the
    // SIGSTOP scenario above uses). Some batches must already have landed —
    // load was really flowing — and strictly fewer than all of them, or the
    // load pass had already finished and "under load" would be a fiction.
    let started_anchor =
        usize::try_from(leave[1]["detail"]["node_journal_lines"].as_u64().unwrap()).unwrap();
    let journal = node0_journal(&work_dir);
    assert!(
        started_anchor <= journal.len(),
        "the journal must only ever grow: anchor {started_anchor} exceeds final length {}",
        journal.len()
    );
    let accepts_before_leave = journal[..started_anchor]
        .iter()
        .filter(|line| line["event"] == "Accept")
        .count();
    assert!(
        accepts_before_leave > 0,
        "load must already have been flowing when the leave fired: node-0 journaled no Accept at \
         all before the SIGTERM (anchor {started_anchor} of {} lines)",
        journal.len()
    );
    assert!(
        accepts_before_leave < LOAD_BATCHES as usize,
        "the leave must land while load is STILL flowing: node-0 had accepted \
         {accepts_before_leave} of {LOAD_BATCHES} batches by the time the SIGTERM was sent — \
         reaching {LOAD_BATCHES} would mean the load pass had already finished"
    );
}

/// §8.4's membership churn, JOIN half: a provisioned-but-unbooted node
/// really joins mid-run, under load, and is confirmed ready.
/// `fault::run_membership_join`'s own docs carry what "join" can and cannot
/// mean while membership is static config. The leave scenario above carries
/// why these are two runs.
///
/// # The load-vs-fault arithmetic
///
/// The join fires at `JOIN_DELAY_SECS` = 1 s, while the load pass's own
/// wall-clock floor is `(LOAD_BATCHES - 1) * LOAD_INTERVAL_MS` ≈ 11.8 s —
/// and nothing in this scenario kills the load target, so the proof that
/// load really was in flight at t≈1 s is that node 0 accepted ALL
/// `LOAD_BATCHES` batches: a pass that admitted every one of 60 batches
/// spaced 200 ms apart necessarily spanned ≈11.8 s of wall clock, which
/// strictly contains the join's own 1 s mark. A load pass that had died,
/// stalled, or finished early would show fewer.
#[test]
fn membership_join_boots_a_real_new_node_while_load_is_still_flowing() {
    const LOAD_BATCHES: u32 = 60;
    const LOAD_INTERVAL_MS: u32 = 200;
    const JOIN_DELAY_SECS: u32 = 1;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    const {
        assert!(
            (LOAD_BATCHES - 1) * LOAD_INTERVAL_MS > JOIN_DELAY_SECS * 1_000,
            "the load pass must still be running when the join fires"
        );
    }
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "membership-join",
        &[
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "15",
            "--fault-churn-join",
            "--fault-churn-join-delay-secs",
            &JOIN_DELAY_SECS.to_string(),
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let join = window_lines(&work_dir, "membership-join-0");
    assert_eq!(phases(&join), vec!["armed", "started", "ended"]);
    assert_eq!(
        join[2]["detail"]["confirmed_ready"], true,
        "the joining node must really have booted and reported ready: {join:#?}"
    );
    assert!(
        work_dir.join("node-1").join("journal.ndjson").exists(),
        "the joined node must have written its own real D-6 journal"
    );

    // The vacuity teeth (module docs above for the arithmetic): every batch
    // of a 60×200ms pass was admitted by the node the join happened
    // alongside, so load was genuinely in flight across the join's 1s mark.
    let accepts = node0_journal(&work_dir)
        .iter()
        .filter(|line| line["event"] == "Accept")
        .count();
    assert!(
        accepts >= LOAD_BATCHES as usize,
        "the join must happen under sustained load: node-0 accepted only {accepts} of \
         {LOAD_BATCHES} batches, so the load pass did not span the join at all"
    );
}

/// §8.4's network partition, end to end through the real CLI — the seam
/// proof `link`'s and `fault`'s own unit tests cannot give: that the fleet
/// really routes the node's ingest through its fault link (so a partition
/// really cuts the traffic the load driver is sending) and really points
/// the node's own `catalog.dsn` at its catalog link.
#[test]
fn a_network_partition_cuts_the_real_ingest_the_load_driver_is_sending() {
    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "network-partition",
        &[
            "--load-batches",
            "60",
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            "200",
            "--settle-timeout-secs",
            "20",
            "--fault-partition-node",
            "0",
            "--fault-partition-delay-secs",
            "3",
            "--fault-partition-duration-secs",
            "5",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let lines = window_lines(&work_dir, "network-partition-0");
    assert_eq!(phases(&lines), vec!["armed", "started", "ended"]);
    let armed_links: Vec<&str> = lines[0]["detail"]["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|link| link["label"].as_str().unwrap())
        .collect();
    assert!(
        armed_links.iter().any(|label| label.ends_with("-ingest")),
        "a partition must cut the node's ingest link: {lines:#?}"
    );
    assert!(
        armed_links.iter().any(|label| label.ends_with("-catalog")),
        "a partition must cut the node's catalog link: {lines:#?}"
    );

    let traffic = &lines[2]["detail"]["link_traffic_during_window"];
    for label in &armed_links {
        assert_eq!(
            traffic[*label]["bytes_client_to_server"], 0,
            "no byte may cross the partitioned {label} link: {lines:#?}"
        );
    }
    let ingest_label = armed_links
        .iter()
        .find(|label| label.ends_with("-ingest"))
        .unwrap();
    let disrupted = traffic[*ingest_label]["conns_refused"].as_u64().unwrap()
        + traffic[*ingest_label]["conns_cut"].as_u64().unwrap();
    assert!(
        disrupted > 0,
        "the load driver's own real traffic must have been cut or refused during the window — \
         zero would mean the load never traversed the link at all: {lines:#?}"
    );
}

/// The real §8.4 cache/residency-churn fault (issue #207): a real node,
/// booted with a shortened `hot.window`, must actually journal real
/// `DropWindow` lines under sustained load while real Arrow Flight reads run
/// through it — and the fault window must record both, per event.
///
/// # What this proves, and what it deliberately does not
///
/// It proves the forcing mechanism is real end to end: the per-node
/// `hot.window` override reaches the real daemon, the real drain loop
/// produces real residency actions inside the window, real Flight reads are
/// served through the same engine while they do, and the fault log carries
/// the counts a judge would grade. It does NOT assert that `Demote`/`Evict`
/// fired — nothing in this workspace emits them (`crate::fault`'s
/// `run_cache_churn` disclosure: v1's cache class is empty by construction,
/// `docs/design/data-model.md` §2.4) — and asserting a zero there would pin
/// today's absence as a requirement, so the assertion below is on the
/// residency TOTAL, which starts biting harder the day the cache class
/// activates.
///
/// Nor does it assert on the reads' latencies: whether a read that raced a
/// residency action was blocked is `duckspout-judge`'s call over the
/// evidence (§2.4 obligation (c),
/// `duckspout_judge::predicates::cache_transparency`), never this runner's
/// (`crate::main`'s own module docs — this binary is not the judge).
#[test]
fn cache_churn_produces_real_residency_actions_while_real_reads_run_through_the_node() {
    const LOAD_BATCHES: u32 = 60;
    const LOAD_INTERVAL_MS: u32 = 200;

    let Some(postgres_dsn) = postgres_dsn_from_env() else {
        eprintln!("fault_injection: DUCKSPOUT_FLEET_TEST_POSTGRES_DSN unset — skipping");
        return;
    };
    let _guard = POSTGRES_CATALOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let work_dir = shared_work_dir();

    // (60 - 1) * 200ms ≈ 11.8s of load wall clock, so the 3s..13s churn
    // window overlaps the load pass for its whole length — a residency storm
    // on an idle node would churn nothing.
    let status = fleet_command(
        &work_dir,
        &postgres_dsn,
        "cache-churn",
        &[
            "--allowed-lateness",
            "1s",
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "20",
            "--fault-cache-churn-node",
            "0",
            "--fault-cache-churn-delay-secs",
            "3",
            "--fault-cache-churn-duration-secs",
            "10",
            "--fault-cache-churn-hot-window",
            "1s",
        ],
    )
    .status()
    .expect("spawning duckspout-fleet");
    eprintln!("duckspout-fleet exited with {status} (not asserted on — see module docs)");

    let lines = window_lines(&work_dir, "cache-churn-0");
    assert_eq!(
        phases(&lines),
        vec!["armed", "started", "ended"],
        "the cache-churn fault must journal its full lifecycle: {lines:#?}"
    );

    let observed = &lines[2]["detail"]["residency_actions_during_window"];
    let total = observed["total"].as_u64().unwrap();
    assert!(
        total > 0,
        "the churn window must have observed at least one real post-drain residency action — \
         zero means the shortened hot.window never produced a drain inside the window, and the \
         fault fired vacuously: {lines:#?}"
    );

    let reads = &lines[2]["detail"]["reads_during_window"];
    let issued = reads["issued"].as_u64().unwrap();
    let served = reads["served"].as_u64().unwrap();
    assert!(
        issued > 1,
        "the window must have issued many real Flight reads (got {issued}) — one would mean the \
         Flight endpoint was never reachable, so nothing ever raced the churn: {lines:#?}"
    );
    assert!(
        served > 0,
        "at least one real read must have been served through the churning node — zero served of \
         {issued} issued would mean the reads never reached the read path at all, and the window \
         proved nothing about §2.4's obligation (c): {lines:#?}"
    );

    // The node's OWN journal is the ground truth the window's counts are
    // read from: a count that did not correspond to a real journaled line
    // would be this runner grading itself. Recount the window's OWN slice of
    // that journal — the lines between the `Started` line's anchor and the
    // `Ended` line's — and require exact equality. Comparing the window's
    // count against the whole file instead can never fail for any defect:
    // the file is a superset of every window's slice by construction, so
    // `whole_file >= window` holds even for a count that is pure invention.
    let journal = node0_journal(&work_dir);
    let window_start = lines[1]["detail"]["node_journal_lines"].as_u64().unwrap();
    let window_end = lines[2]["detail"]["node_journal_lines"].as_u64().unwrap();
    assert!(
        window_end >= window_start && window_end <= u64::try_from(journal.len()).unwrap(),
        "the window's own anchors must bracket a real slice of node 0's journal ({window_start} \
         → {window_end}, {} lines): {lines:#?}",
        journal.len()
    );
    let residency_in_window = journal
        .iter()
        .skip(usize::try_from(window_start).unwrap())
        .take(usize::try_from(window_end - window_start).unwrap())
        .filter(|line| {
            matches!(
                line["event"].as_str(),
                Some("Demote" | "Evict" | "DropWindow")
            )
        })
        .count();
    assert_eq!(
        u64::try_from(residency_in_window).unwrap(),
        total,
        "the window counted {total} residency action(s), but its own slice of node 0's journal \
         (lines {window_start}..{window_end}) holds {residency_in_window}"
    );
}
