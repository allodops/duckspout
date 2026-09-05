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
//! carries a real (non-mocked, non-P-model) integration test of the actual
//! `fence_boot`/`FenceTable` library code proving the guarantee holds for
//! this exact pause/promote/resume scenario shape, which is the most
//! faithful verification available until that wiring lands. See this
//! issue's PR description for the full accounting of what is
//! real-and-verified here vs. deferred.

use std::time::Duration;

use crate::faultlog::{FaultKind, FaultLog};
use crate::process::{self, RunningNode};

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
    let target_node = target.spec.name.clone();
    log.armed(fault_id, FaultKind::NodeKill, &target_node);

    match timing {
        KillTiming::AfterDelay(delay) => tokio::time::sleep(delay).await,
        KillTiming::MidDrainCommit {
            journal_poll_timeout,
        } => {
            wait_for_journal_event(&target.spec.journal_path, "PutPart", journal_poll_timeout)
                .await?;
        }
    }

    let pid = process::pid(target);
    process::send_signal(target, "-KILL").await?;
    log.started(
        fault_id,
        FaultKind::NodeKill,
        &target_node,
        Some(serde_json::json!({ "pid": pid })),
    );

    let confirmed_exited = process::wait_exited(target, Duration::from_secs(10)).await;
    log.ended(
        fault_id,
        FaultKind::NodeKill,
        &target_node,
        Some(serde_json::json!({ "confirmed_exited": confirmed_exited })),
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
/// If either signal cannot be sent (`crate::process::send_signal`).
pub async fn run_sigstop_pause(
    fault_id: &str,
    target: &mut RunningNode,
    delay: Duration,
    pause_duration: Duration,
    log: &FaultLog,
) -> anyhow::Result<()> {
    let target_node = target.spec.name.clone();
    log.armed(fault_id, FaultKind::SigstopPause, &target_node);

    tokio::time::sleep(delay).await;

    let pid = process::pid(target);
    process::send_signal(target, "-STOP").await?;
    let confirmed_stopped = confirm_state(pid, Duration::from_secs(2), true).await;
    log.started(
        fault_id,
        FaultKind::SigstopPause,
        &target_node,
        Some(serde_json::json!({
            "pid": pid,
            "confirmed_stopped": confirmed_stopped,
            "planned_pause_ms": pause_duration.as_millis(),
        })),
    );

    tokio::time::sleep(pause_duration).await;

    process::send_signal(target, "-CONT").await?;
    let confirmed_resumed = confirm_state(pid, Duration::from_secs(2), false).await;
    log.ended(
        fault_id,
        FaultKind::SigstopPause,
        &target_node,
        Some(serde_json::json!({ "confirmed_resumed": confirmed_resumed })),
    );
    Ok(())
}

/// Polls [`process::is_stopped`] for `pid` until it reports `want_stopped`,
/// or `timeout` elapses — best-effort OS-level confirmation that a sent
/// signal actually took observable effect (module docs above: Linux-only,
/// never a hard error — a missing `/proc` read degrades to "unconfirmed",
/// journaled as `false`, not a fleet-run failure).
async fn confirm_state(pid: Option<u32>, timeout: Duration, want_stopped: bool) -> bool {
    let Some(pid) = pid else { return false };
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if process::is_stopped(pid).unwrap_or(!want_stopped) == want_stopped {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Polls `journal_path` every 50 ms until an NDJSON line whose `event`
/// field equals `event` appears, or `timeout` elapses.
///
/// # Errors
///
/// If `timeout` elapses with no matching line ever observed.
async fn wait_for_journal_event(
    journal_path: &std::path::Path,
    event: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if journal_contains_event(journal_path, event) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out after {timeout:?} waiting for a {event:?} line in {}",
                journal_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether `journal_path`'s NDJSON contents contain a line whose `event`
/// field equals `event` — re-reads the whole (small, per-node) journal each
/// call rather than tracking a byte offset, which is simple and correct for
/// this crate's smoke-scale journals (`crate::faultlog`'s own "informal
/// channel" framing extends to this reader: no shared parsing contract with
/// a real judge is implied here). Never errors: a journal that does not
/// exist yet, or a torn/partial last line mid-write, both read as "not
/// found yet" — the caller's polling loop already tolerates that.
fn journal_contains_event(journal_path: &std::path::Path, event: &str) -> bool {
    let Ok(contents) = std::fs::read_to_string(journal_path) else {
        return false;
    };
    contents.lines().any(|line| {
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
        assert!(!journal_contains_event(&path, "PutPart"));

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
            !journal_contains_event(&path, "PutPart"),
            "a decoy string in a non-`event` field must not false-positive"
        );

        writeln!(file, r#"{{"node":"n0","seq":2,"event":"PutPart"}}"#).unwrap();
        assert!(journal_contains_event(&path, "PutPart"));
    }

    /// A missing journal file reads as "not found yet," never a panic or an
    /// error the caller must special-case — the injector may start
    /// watching before the target has written anything at all.
    #[test]
    fn journal_contains_event_tolerates_a_missing_file() {
        let missing =
            std::env::temp_dir().join("duckspout-fleet-fault-test-missing-journal.ndjson");
        assert!(!journal_contains_event(&missing, "PutPart"));
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
            wait_for_journal_event(&path, "PutPart", Duration::from_secs(5)),
        )
        .await;
        assert!(result.is_ok(), "outer timeout must not fire");
        assert!(result.unwrap().is_ok(), "the wait itself must succeed");
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

        let result = wait_for_journal_event(&path, "PutPart", Duration::from_millis(150)).await;
        assert!(result.is_err());
    }
}
