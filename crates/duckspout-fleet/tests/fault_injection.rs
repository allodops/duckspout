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
#[test]
fn node_kill_mid_drain_lands_strictly_between_putpart_and_lakecommit() {
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
            "15",
            "--load-batch-size",
            "10",
            "--load-interval-ms",
            "150",
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
}

/// The real SIGSTOP-pause fault: a real `SIGSTOP`, held past
/// `duckspout_daemon::constants::HEARTBEAT_TTL_SECS`, then a real
/// `SIGCONT` — both confirmed via `/proc/<pid>/stat`
/// (`crate::process::is_stopped`), proven by reading `faults.ndjson` after
/// the run. `crate::fault`'s own module docs disclose exactly what this
/// does NOT verify (a peer actually rejecting the resumed incarnation —
/// blocked on daemon composition not yet wiring `fence_boot`/`FenceTable`
/// at all) and where that IS verified for real:
/// `crates/duckspout-replication/tests/fenced_zombie_pause_and_promote.rs`.
#[test]
fn sigstop_pause_is_a_real_confirmed_stop_and_resume() {
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
            "5",
            "--load-batch-size",
            "5",
            "--load-interval-ms",
            "100",
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
}
