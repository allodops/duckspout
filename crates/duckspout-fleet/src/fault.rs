//! Fault injectors (§8.4, issue #203): real node kills — including timed to
//! land inside the real `PutPart`→`LakeCommit` window — and real
//! `SIGSTOP`/`SIGCONT` pauses (the `FencedZombie` fault). Each injector runs
//! against a real `duckspout-daemon` process spawned by [`crate::process`],
//! and journals its own Armed/Started/Ended lifecycle through
//! [`crate::faultlog::FaultLog`] (§8.4: "each injector keeps its own
//! armed/fired ledger").
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

use crate::faultlog::{FaultKind, FaultLog};
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
