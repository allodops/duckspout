//! Fault-window journaling (§8.4, issue #203): "each window journaled with
//! start/end."
//!
//! # Why this is a separate channel, not a `TraceEvent` (D-6)
//!
//! `duckspout_types::TraceEvent` is the frozen §3.3 node-action vocabulary
//! (`docs/trace-mapping.md`: "the enum itself and this table are the frozen
//! [...]" — 27 node-journaled variants plus `duckspout-loadgen`'s own
//! `ClientTimeout`) — every variant is something a NODE does, journaled BY
//! that node, transcribed verbatim from the formal model's action set. A
//! fault window is the opposite shape on every axis: it is something the
//! FLEET RUNNER does *to* a node from outside, and the node itself has no
//! idea it is being faulted (and must not — a node that could observe its
//! own fault schedule would not be exercising a real, uninstrumented
//! failure mode).
//!
//! `duckspout_types::EnvironmentEvent::CrashNode` DOES have a §3
//! correspondence for a node kill (its own doc comment: "Kill a node
//! process; durable state survives" — `docs/trace-mapping.md` maps it to
//! "§3.3 Crash and recovery"), and that type's module docs say it is
//! "injected by the CTK's schedule stream, never journaled by a node" —
//! which is consistent with, not against, reusing it here. `SigstopPause`,
//! though, has no `EnvironmentEvent` variant at all — a SIGSTOP pause is
//! not a crash. Mixing a `NodeKill` fault-log entry that DOES have a D-6
//! correspondence with a `SigstopPause` entry that does NOT would make this
//! channel inconsistent depending on which fault kind fired, so this module
//! is deliberately ONE small, informal, fleet-runner-owned NDJSON channel
//! (`faults.ndjson`, one file per fleet run, shared across every injector)
//! for BOTH fault kinds uniformly — not a per-kind mix of "amend D-6 where
//! there is a variant, informal channel where there isn't." Amending the
//! frozen D-6 vocabulary to add a `SigstopPause` correspondence would be a
//! settled-decision amendment (`AGENTS.md`: "never re-litigate\[d\] in
//! PRs"), not a mechanical fix available inside this issue's scope, so this
//! module does not attempt it for either fault kind.
//!
//! # Shape
//!
//! Every fault the fleet runs is one logical `FaultWindow` identified by a
//! `fault_id`, journaled as up to three lines as it progresses through
//! [`FaultPhase`]s: `Armed` (scheduled, before anything happens — the
//! "start" of the fault's *lifecycle*, not yet the fault itself), `Started`
//! (the fault is now actually in effect — a signal was sent and, where
//! confirmable, observed to have landed), and `Ended` (the fault's effect is
//! over — the process confirmed dead, or confirmed resumed). This is
//! deliberately the same "attempt, then a separately journaled resolution"
//! shape `docs/verification.md`'s own Journals paragraph describes for the
//! frozen vocabulary ("an attempt with no journaled resolution, or a
//! resolution with no journaled attempt, is itself a finding") — applied
//! here to fault windows instead of protocol actions, feeding the
//! vacuity-teeth judge work (#208, tracked separately, not implemented by
//! this issue): an `Armed` line with no matching `Started` line is exactly
//! "a fault schedule that armed faults and fired none" (§8.4).

use std::io::Write;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// The two fault classes this issue implements (§8.4's own list has five
/// total; #204 covers the other three — partitions, membership churn,
/// catalog outages, discovery flapping, Flight-server kill).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    /// A real `SIGKILL` of a node process, optionally timed to land inside
    /// the real `PutPart`→`LakeCommit` window (`crate::fault`'s module
    /// docs).
    NodeKill,
    /// A real `SIGSTOP`, held for a configured duration, then `SIGCONT`
    /// (the `FencedZombie` fault, `crate::fault`'s module docs).
    SigstopPause,
}

/// Where in its lifecycle one fault window is, at the moment a line is
/// journaled (module docs above for the three-phase shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultPhase {
    /// Scheduled: the injector has decided this fault will run and named
    /// its target, but has not yet sent any signal.
    Armed,
    /// In effect: the fault's signal was sent and, where the OS lets this
    /// be confirmed (`crate::process::is_stopped`), observed to have
    /// landed.
    Started,
    /// Over: the fault's effect has been confirmed resolved — the process
    /// confirmed dead ([`crate::process::wait_exited`]) or confirmed
    /// resumed.
    Ended,
}

/// One journaled fault-window line.
#[derive(Debug, Clone, Serialize)]
pub struct FaultWindowLine<'a> {
    /// Stable identity for this fault window, shared across its `Armed`/
    /// `Started`/`Ended` lines — a judge (or a human) correlates the three
    /// lines on this field.
    pub fault_id: &'a str,
    pub kind: FaultKind,
    /// The fleet node name (`topology::node_name`) this fault targets.
    pub target_node: &'a str,
    pub phase: FaultPhase,
    /// Wall-clock milliseconds since the Unix epoch when this phase was
    /// journaled (module docs: "each window journaled with start/end" —
    /// this is that timestamp). A bin crate, not a protocol crate, so a
    /// direct wall-clock read is not an R-determinism violation (D-2 scopes
    /// that rule to the layered protocol crates, `AGENTS.md`).
    pub at_ms: u64,
    /// Fault-specific extra context (e.g. the configured pause duration,
    /// the observed exit status) — deliberately a free-form JSON object
    /// rather than a fixed schema, matching this module's own "informal
    /// channel" framing (module docs above).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

/// The fleet run's fault-window journal (module docs): one shared,
/// append-only NDJSON file every injector writes `Armed`/`Started`/`Ended`
/// lines to.
pub struct FaultLog {
    out: Mutex<std::fs::File>,
}

impl FaultLog {
    /// Creates (or truncates) the fault-window journal at `path`.
    ///
    /// # Errors
    ///
    /// If the file cannot be created.
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        Ok(Self {
            out: Mutex::new(std::fs::File::create(path)?),
        })
    }

    /// Journals one phase of `fault_id`/`kind` targeting `target_node`.
    ///
    /// # Panics
    ///
    /// On serialization or write failure — a fault log that silently drops
    /// a line would make an armed-but-never-fired fault indistinguishable
    /// from one this journal simply failed to record (R-3), so this fails
    /// loud rather than limping on, matching
    /// `duckspout_ctk::NdjsonTraceWriter`'s and
    /// `duckspout_loadgen::journal::LoadgenJournal`'s own contract.
    pub fn record(
        &self,
        fault_id: &str,
        kind: FaultKind,
        target_node: &str,
        phase: FaultPhase,
        detail: Option<serde_json::Value>,
    ) {
        let line = FaultWindowLine {
            fault_id,
            kind,
            target_node,
            phase,
            at_ms: now_unix_ms(),
            detail,
        };
        let text = serde_json::to_string(&line).expect("fault-window line serializes");
        let mut out = self.out.lock().expect("fault log lock poisoned");
        writeln!(out, "{text}").expect("fault-window journal write");
        out.flush().expect("fault-window journal flush");
    }

    /// Journals `fault_id`'s [`FaultPhase::Armed`] line. `detail` carries
    /// the same kind of correlation aid `started`/`ended` already accept
    /// (`crate::fault`'s own callers snapshot the target's D-6 journal line
    /// count here too, not only at `Started`/`Ended` — a judge locating
    /// "how far had this node's own journal gotten when this fault was
    /// scheduled" needs the Armed-phase anchor as much as the later ones).
    pub fn armed(
        &self,
        fault_id: &str,
        kind: FaultKind,
        target_node: &str,
        detail: Option<serde_json::Value>,
    ) {
        self.record(fault_id, kind, target_node, FaultPhase::Armed, detail);
    }

    /// Journals `fault_id`'s [`FaultPhase::Started`] line.
    pub fn started(
        &self,
        fault_id: &str,
        kind: FaultKind,
        target_node: &str,
        detail: Option<serde_json::Value>,
    ) {
        self.record(fault_id, kind, target_node, FaultPhase::Started, detail);
    }

    /// Journals `fault_id`'s [`FaultPhase::Ended`] line.
    pub fn ended(
        &self,
        fault_id: &str,
        kind: FaultKind,
        target_node: &str,
        detail: Option<serde_json::Value>,
    ) {
        self.record(fault_id, kind, target_node, FaultPhase::Ended, detail);
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch, saturating
/// rather than panicking on a pre-epoch clock (never observed in practice,
/// but a fault log must never itself panic the fleet run over a clock
/// oddity — R-5).
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn scratch(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-faultlog-test-{}-{label}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("faults.ndjson")
    }

    /// The three-phase lifecycle of one fault window round-trips through
    /// NDJSON with the correlating `fault_id` intact and phases in order —
    /// the exact shape a future judge (#208) needs to pair `Armed` against
    /// `Started` (vacuity) and `Started` against `Ended` (an unresolved
    /// fault window).
    #[test]
    fn armed_started_ended_share_the_fault_id_and_appear_in_order() {
        let path = scratch("lifecycle");
        let log = FaultLog::create(&path).unwrap();
        log.armed("kill-0", FaultKind::NodeKill, "fleet-0-1", None);
        log.started(
            "kill-0",
            FaultKind::NodeKill,
            "fleet-0-1",
            Some(serde_json::json!({"pid": 4242})),
        );
        log.ended(
            "kill-0",
            FaultKind::NodeKill,
            "fleet-0-1",
            Some(serde_json::json!({"confirmed_exited": true})),
        );

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 3);
        for line in &lines {
            assert_eq!(line["fault_id"], "kill-0");
            assert_eq!(line["kind"], "node_kill");
            assert_eq!(line["target_node"], "fleet-0-1");
            assert!(line["at_ms"].as_u64().unwrap() > 0);
        }
        assert_eq!(lines[0]["phase"], "armed");
        assert_eq!(lines[1]["phase"], "started");
        assert_eq!(lines[2]["phase"], "ended");
        assert_eq!(lines[1]["detail"]["pid"], 4242);
        assert_eq!(lines[2]["detail"]["confirmed_exited"], true);
    }

    /// An `Armed` line with no follow-up is exactly the vacuity shape §8.4
    /// names ("a fault schedule that armed faults and fired none") — this
    /// journal must not require a `Started`/`Ended` line to exist for the
    /// `Armed` one to be valid on its own, so a future judge can detect the
    /// gap rather than the journal format itself preventing it.
    #[test]
    fn an_armed_only_fault_journals_without_error() {
        let path = scratch("armed-only");
        let log = FaultLog::create(&path).unwrap();
        log.armed("pause-1", FaultKind::SigstopPause, "fleet-0-2", None);
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["phase"], "armed");
    }

    /// `detail` is omitted entirely (not `null`) when absent — keeps a
    /// vacuously-armed line's JSON minimal rather than growing a
    /// meaningless field.
    #[test]
    fn absent_detail_is_omitted_not_null() {
        let path = scratch("no-detail");
        let log = FaultLog::create(&path).unwrap();
        log.armed("kill-2", FaultKind::NodeKill, "fleet-0-0", None);
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(
            !raw.contains("detail"),
            "expected no `detail` key at all, got: {raw}"
        );
    }

    /// Independent fault windows (distinct `fault_id`s) interleave safely
    /// in the shared journal — the injectors run concurrently
    /// (`crate::fault`'s module docs), so the log must not corrupt or drop
    /// lines under concurrent writers.
    #[test]
    fn concurrent_writers_never_corrupt_or_drop_lines() {
        let path = scratch("concurrent");
        let log = std::sync::Arc::new(FaultLog::create(&path).unwrap());
        std::thread::scope(|scope| {
            for i in 0..8 {
                let log = std::sync::Arc::clone(&log);
                scope.spawn(move || {
                    log.armed(&format!("f-{i}"), FaultKind::NodeKill, "fleet-0-0", None);
                });
            }
        });
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 8, "every writer's line must land intact");
        let mut ids: Vec<String> = lines
            .iter()
            .map(|l| l["fault_id"].as_str().unwrap().to_owned())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 8, "no line may be dropped or duplicated");
    }
}
