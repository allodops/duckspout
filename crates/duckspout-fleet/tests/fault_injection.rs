//! Real end-to-end fault injection (§8.4, issue #203): spawns the REAL
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
//! not parse the URI form at all and silently falls through to treating
//! the whole string as a local file path, so this test also passes
//! `--skip-backend-check` (the fleet's own TCP-probe DSN parser only
//! understands the URI form, and the probe is not this test's concern).
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
//! peer actually rejecting the resumed incarnation).
//!
//! **The two tests below share one [`POSTGRES_CATALOG`] mutex and one fixed
//! work dir.** A `DuckLake` catalog pins its `DATA_PATH` for its whole
//! lifetime (`crates/duckspout-fleet/src/main.rs`'s own `--s3-prefix` doc
//! comment makes the same point for the S3 case; it applies identically to
//! `--local-lake`'s local-filesystem path) — two tests attaching the SAME
//! real Postgres catalog from two DIFFERENT work dirs would only ever let
//! the first one through. Running them concurrently (`cargo test`'s
//! default) would additionally risk issue #213's own race on a fresh
//! catalog. Sharing one work dir and serializing via the mutex sidesteps
//! both pre-existing gaps without this file needing to reset Postgres
//! itself between tests.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

/// Serializes the two tests below against the one shared real Postgres
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

/// The one work dir both tests share (module docs above for why it must be
/// fixed and shared, not per-scenario). Contents are wiped fresh by every
/// caller (`remove_dir_all` then recreate) — only the PATH STRING (and so
/// the `DuckLake` `DATA_PATH` it implies) needs to stay stable.
fn shared_work_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("duckspout-fleet-fault-injection-test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

    let status = Command::new(fleet_bin())
        .args([
            "--nodes",
            "1",
            "--local-lake",
            "--skip-backend-check",
            "--postgres-dsn",
            &postgres_dsn,
            "--postgres-password",
            &postgres_password_from_env(),
            "--work-dir",
            work_dir.to_str().unwrap(),
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
            "--boot-timeout-secs",
            "30",
            "--fault-kill-node",
            "0",
            "--fault-kill-mid-drain",
            "--fault-kill-drain-stall-ms",
            "2000",
            "--fault-kill-mid-drain-timeout-secs",
            "40",
        ])
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

    let status = Command::new(fleet_bin())
        .args([
            "--nodes",
            "1",
            "--local-lake",
            "--skip-backend-check",
            "--postgres-dsn",
            &postgres_dsn,
            "--postgres-password",
            &postgres_password_from_env(),
            "--work-dir",
            work_dir.to_str().unwrap(),
            "--load-batches",
            &LOAD_BATCHES.to_string(),
            "--load-batch-size",
            "5",
            "--load-interval-ms",
            &LOAD_INTERVAL_MS.to_string(),
            "--settle-timeout-secs",
            "15",
            "--boot-timeout-secs",
            "30",
            "--fault-sigstop-node",
            "0",
            "--fault-sigstop-delay-secs",
            "1",
            "--fault-sigstop-duration-secs",
            "3",
        ])
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
