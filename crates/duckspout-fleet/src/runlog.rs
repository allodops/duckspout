//! The fleet run's own manifest (`run.json`) — issue #208, `docs/verification.md`
//! §8.4's run-level vacuity teeth.
//!
//! # Why this file has to exist at all
//!
//! Two of §8.4's four `NoVerdict` rules — "a run with no observed cross-node
//! contention when contention is what the run exists to certify" and "a node
//! whose journals simply stop" — are statements about the RUN, not about any
//! one journal line, and neither is decidable from the D-6 journals alone:
//!
//! - **The roster.** "No cross-node contention" needs to know how many nodes
//!   the run actually had. Counting the distinct node ids that appear in the
//!   journals cannot answer it: a two-node run whose second node died at boot
//!   and journaled nothing looks exactly like a one-node run, and a
//!   one-node run is the shape that trivially "explains" the absence of
//!   cross-node traffic. The roster must come from whoever provisioned it.
//! - **Time.** `duckspout_types::TraceRecord` carries `{node, seq, event}`
//!   and no timestamp (`crate::faultlog`'s module docs on why that is frozen
//!   and why the fault log carries its own `at_ms` instead), so "this node's
//!   journal stopped while the rest of the fleet kept working" is not a
//!   question the journals can be asked. Something outside them has to have
//!   watched the clock.
//!
//! So the runner samples every node's journal length on a fixed interval and
//! records, per node, the last wall-clock moment at which that length grew.
//! That sample series is the only thing here that is a MEASUREMENT; the rest
//! of the manifest is the runner stating facts about what it provisioned and
//! observed (the roster, the run's wall-clock bounds, whether a child process
//! was already dead when teardown reached it).
//!
//! # This is not the runner grading itself
//!
//! §8.4 splits the distributed tier into a fleet runner that drives the
//! system and a **separate judge binary** that runs as a post-pass over the
//! journals and produces the run's verdict — the process that runs the system
//! is not the process that grades it. This file stays on the runner's side of
//! that line, and contains no verdict at all: no threshold, no pass/fail, no
//! judgement about whether a silent node was a problem. `duckspout-judge`
//! owns every one of those decisions — including the silence budget and the
//! exemption for a node an armed fault deliberately stopped, which it derives
//! from `faults.ndjson` (each injector's own ledger, §8.4's own wording) and
//! never from anything asserted here. The runner is a witness; the judge is
//! the judge. `faults.ndjson` is written under exactly the same split, by the
//! same runner, for the same reason.
//!
//! # Shape
//!
//! ```json
//! {"started_at_ms":1700000000000,"ended_at_ms":1700000090000,
//!  "sample_interval_ms":500,
//!  "nodes":[{"name":"fleet-0-0","journal_path":"/…/journal.ndjson",
//!            "journal_lines":812,"last_progress_at_ms":1700000089500,
//!            "exited_early":false}]}
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;

use crate::topology::NodeSpec;

/// How often the runner re-reads each node's journal length. 500 ms is
/// deliberately much finer than any plausible judge-side silence budget
/// (`duckspout_judge::vacuity::DEFAULT_MAX_JOURNAL_SILENCE_MS` is 30 s), so
/// the sampling grain is never the thing that decides a verdict — the
/// manifest publishes this interval precisely so the judge can widen its own
/// exemption window by it rather than assume a resolution.
pub const DEFAULT_SAMPLE_INTERVAL_MS: u64 = 500;

/// One roster member's record: who it was, and what its journal did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeRun {
    /// The node's `DUCKSPOUT_NODE_HOSTNAME` ([`NodeSpec::name`]) — the HOST
    /// half of the `<hostname>/<incarnation>` [`duckspout_types::NodeId`] the
    /// node itself journals (`duckspout_daemon::system::detect_node_id`).
    /// The judge joins this manifest to the journals on this field and not on
    /// `journal_path`, because an archived artifact set is routinely read
    /// back from a different directory than it was written in, while the node
    /// identity inside the journal travels with the evidence.
    pub name: String,
    /// Where this node's journal was written — diagnostics, so an operator
    /// reading the manifest can go open the file. Never a join key (above).
    pub journal_path: PathBuf,
    /// Lines in that journal at [`RunManifest::ended_at_ms`].
    pub journal_lines: u64,
    /// The last sample at which `journal_lines` was observed to have GROWN,
    /// in Unix milliseconds. `None` for a node that never journaled a line at
    /// all — which is a strictly stronger vacuity signal than a node that
    /// went quiet, and is reported as such rather than collapsed into it.
    pub last_progress_at_ms: Option<u64>,
    /// True iff this node's child process had already exited by the time
    /// teardown reached it — i.e. it was not the runner's SIGTERM that ended
    /// it. Deliberately NOT filtered here by whether a fault explains it: an
    /// intentionally killed node exits early too, and deciding which exits
    /// were legitimate is the judge's job over the fault ledger (module
    /// docs).
    pub exited_early: bool,
}

/// The fleet run's manifest (module docs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunManifest {
    /// When the runner started provisioning, in Unix milliseconds.
    pub started_at_ms: u64,
    /// When the run's load/fault phase finished — measured BEFORE teardown,
    /// so the shutdown tail (during which every node legitimately goes quiet
    /// as it shallow-drains) is outside the window the judge reasons about.
    pub ended_at_ms: u64,
    /// The sampling grain of every `last_progress_at_ms` above
    /// ([`DEFAULT_SAMPLE_INTERVAL_MS`]).
    pub sample_interval_ms: u64,
    /// Every node the runner actually STARTED, in roster order. A node that
    /// was provisioned but never booted (the `--fault-churn-join` member of a
    /// run whose join fault never fired) is absent: it is not a machine that
    /// vanished, it is a machine that never existed, and §8.4's
    /// armed-but-unfired rule over `faults.ndjson` is what covers it.
    pub nodes: Vec<NodeRun>,
}

/// Per-node journal-length sample state, shared between the sampling task and
/// the run's own teardown path.
#[derive(Debug, Default)]
pub struct JournalProgress {
    seen: Mutex<BTreeMap<String, Progress>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Progress {
    lines: u64,
    last_progress_at_ms: Option<u64>,
}

impl JournalProgress {
    /// A sampler that has observed nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One sampling pass: re-reads every node's journal length and, for each
    /// node whose length CHANGED since the previous pass, stamps `at_ms` as
    /// that node's newest progress.
    ///
    /// A node's first-ever non-zero length counts as progress (from the
    /// implicit `0` of a node that had not journaled yet), so a node that
    /// boots late gets a real first-progress stamp rather than inheriting the
    /// run's start.
    ///
    /// # Why CHANGED and not GREW
    ///
    /// `duckspout-daemon` opens its journal with `File::create`, which
    /// TRUNCATES, so a node restarted inside one run (a supervisor restart, a
    /// `membership_join` re-add) starts its line count over at zero. Under a
    /// grew-only rule that node's `last_progress_at_ms` would then freeze at
    /// its pre-restart value until the new journal climbed back past the old
    /// high-water mark — a restart would read to the judge as a machine going
    /// silent, which is the exact signal
    /// `duckspout_judge::vacuity`'s node-continuity rule exists to detect and
    /// which `duckspout_judge::run_manifest::node_host` explicitly claims a
    /// restart survives. So a count that goes DOWN re-baselines this node
    /// rather than being ignored. An ACPR finding (LOW-3).
    ///
    /// The stamp itself is still gated on a NON-ZERO count, which is the
    /// difference between "the process rewrote its journal" and "the journal
    /// is gone": `node_journal_line_count` answers `0` for an unreadable or
    /// absent file, and a vanished journal must never be mistaken for a live
    /// node. A restart caught mid-truncation therefore re-baselines on the
    /// pass that sees `0` and stamps on the next one that sees a line, which
    /// costs one sampling interval and cannot manufacture liveness.
    pub fn sample(&self, nodes: &[NodeSpec], at_ms: u64) {
        let mut seen = self.seen.lock().expect("journal-progress lock poisoned");
        for node in nodes {
            let lines = crate::fault::node_journal_line_count(&node.journal_path);
            let entry = seen.entry(node.name.clone()).or_default();
            if lines != entry.lines {
                entry.lines = lines;
                if lines > 0 {
                    entry.last_progress_at_ms = Some(at_ms);
                }
            }
        }
    }

    /// The roster rows for `nodes`, with `exited_early` supplied by the
    /// caller (only the runner holds the `Child` handles that can answer it).
    ///
    /// `nodes` is the roster: exactly the nodes the runner started
    /// ([`RunManifest::nodes`]'s own contract), so a provisioned-but-unbooted
    /// member is excluded by not being passed here.
    #[must_use]
    pub fn roster(&self, nodes: &[NodeSpec], exited_early: &BTreeSet<String>) -> Vec<NodeRun> {
        let seen = self.seen.lock().expect("journal-progress lock poisoned");
        nodes
            .iter()
            .map(|node| {
                let progress = seen.get(&node.name).copied().unwrap_or_default();
                NodeRun {
                    name: node.name.clone(),
                    journal_path: node.journal_path.clone(),
                    journal_lines: progress.lines,
                    last_progress_at_ms: progress.last_progress_at_ms,
                    exited_early: exited_early.contains(&node.name),
                }
            })
            .collect()
    }
}

/// Samples `nodes` forever, every `interval`. Runs as a detached task for the
/// length of the run and is aborted by the caller at teardown; it deliberately
/// has no completion condition of its own, so it cannot stop early and leave a
/// stale `last_progress_at_ms` behind that would read as a silent node.
pub async fn sample_forever(
    progress: std::sync::Arc<JournalProgress>,
    nodes: Vec<NodeSpec>,
    interval: std::time::Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        progress.sample(&nodes, crate::faultlog::now_unix_ms());
    }
}

/// Writes `manifest` to `path` as pretty JSON.
///
/// # Errors
///
/// Any I/O or serialization failure — surfaced rather than swallowed: a run
/// whose manifest silently failed to land would be judged as one with no
/// roster at all, which the judge reports as `NoVerdict`. That is the safe
/// direction, but an operator should still be told why.
pub fn write(path: &Path, manifest: &RunManifest) -> anyhow::Result<()> {
    let text = serde_json::to_string_pretty(manifest)?;
    std::fs::write(path, text)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-runlog-test-{}-{label}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn spec(dir: &Path, name: &str) -> NodeSpec {
        NodeSpec {
            index: 0,
            name: name.to_owned(),
            otlp_port: 1,
            flight_port: 2,
            peer_port: 3,
            status_port: 4,
            data_dir: dir.to_owned(),
            config_path: dir.join(format!("{name}.toml")),
            journal_path: dir.join(format!("{name}.ndjson")),
            stdout_path: dir.join(format!("{name}.out")),
            stderr_path: dir.join(format!("{name}.err")),
        }
    }

    fn append(path: &Path, lines: usize) {
        let existing = std::fs::read_to_string(path).unwrap_or_default();
        let mut text = existing;
        for _ in 0..lines {
            text.push_str("{\"node\":\"x\",\"seq\":0,\"event\":\"Accept\"}\n");
        }
        std::fs::write(path, text).unwrap();
    }

    /// The core measurement: a node that stops journaling keeps the timestamp
    /// of its LAST growth, while a node that keeps journaling advances — the
    /// exact difference §8.4's "a node whose journals simply stop" rule is
    /// decided on. Would catch a sampler that stamped every node on every
    /// pass (making every node look alive forever).
    #[test]
    fn last_progress_advances_only_for_a_node_whose_journal_grew() {
        let dir = scratch("progress");
        let live = spec(&dir, "live");
        let dead = spec(&dir, "dead");
        let _ = std::fs::remove_file(&live.journal_path);
        let _ = std::fs::remove_file(&dead.journal_path);
        let nodes = vec![live.clone(), dead.clone()];
        let progress = JournalProgress::new();

        append(&live.journal_path, 1);
        append(&dead.journal_path, 1);
        progress.sample(&nodes, 1_000);

        append(&live.journal_path, 1);
        progress.sample(&nodes, 2_000);

        let roster = progress.roster(&nodes, &BTreeSet::new());
        assert_eq!(roster[0].last_progress_at_ms, Some(2_000));
        assert_eq!(roster[0].journal_lines, 2);
        assert_eq!(roster[1].last_progress_at_ms, Some(1_000));
        assert_eq!(roster[1].journal_lines, 1);
    }

    /// A node restarted inside one run truncates its own journal
    /// (`duckspout-daemon` opens it with `File::create`), so its line count
    /// starts over. That is a live process writing a fresh journal, and it
    /// must not read as silence — otherwise a restart looks to the judge
    /// exactly like the vanished machine its node-continuity rule convicts,
    /// contradicting `duckspout_judge::run_manifest`'s own claim that a
    /// restart within one run survives the join. An ACPR finding (LOW-3):
    /// would catch the `lines > entry.lines` grew-only rule, under which
    /// `last_progress_at_ms` froze at 1000 until the new journal climbed back
    /// past three lines.
    #[test]
    fn a_journal_truncated_by_a_restart_is_progress_not_silence() {
        let dir = scratch("restart");
        let node = spec(&dir, "restarted");
        let _ = std::fs::remove_file(&node.journal_path);
        let nodes = vec![node.clone()];
        let progress = JournalProgress::new();

        append(&node.journal_path, 3);
        progress.sample(&nodes, 1_000);

        // The restart: a fresh, truncated journal with one line in it.
        std::fs::write(&node.journal_path, "").unwrap();
        append(&node.journal_path, 1);
        progress.sample(&nodes, 2_000);

        let roster = progress.roster(&nodes, &BTreeSet::new());
        assert_eq!(roster[0].last_progress_at_ms, Some(2_000));
        assert_eq!(roster[0].journal_lines, 1);

        // And the new journal keeps advancing from its own baseline, rather
        // than waiting to climb back past the pre-restart high-water mark.
        append(&node.journal_path, 1);
        progress.sample(&nodes, 3_000);
        let roster = progress.roster(&nodes, &BTreeSet::new());
        assert_eq!(roster[0].last_progress_at_ms, Some(3_000));
        assert_eq!(roster[0].journal_lines, 2);
    }

    /// The other side of that line: a journal that DISAPPEARS is not a
    /// restart. `node_journal_line_count` answers `0` for an absent or
    /// unreadable file, and a zero must never stamp progress — otherwise a
    /// deleted journal would manufacture liveness for a node that is gone.
    #[test]
    fn a_journal_that_vanished_does_not_stamp_progress() {
        let dir = scratch("vanished");
        let node = spec(&dir, "vanished");
        let _ = std::fs::remove_file(&node.journal_path);
        let nodes = vec![node.clone()];
        let progress = JournalProgress::new();

        append(&node.journal_path, 2);
        progress.sample(&nodes, 1_000);

        std::fs::remove_file(&node.journal_path).unwrap();
        progress.sample(&nodes, 2_000);

        let roster = progress.roster(&nodes, &BTreeSet::new());
        assert_eq!(roster[0].last_progress_at_ms, Some(1_000));
    }

    /// A node that never journaled anything reports `None`, not the run's
    /// start time — the stronger vacuity signal must stay distinguishable
    /// from "went quiet late" (`NodeRun::last_progress_at_ms` docs).
    #[test]
    fn a_node_that_never_journaled_has_no_progress_timestamp() {
        let dir = scratch("never");
        let silent = spec(&dir, "silent");
        let _ = std::fs::remove_file(&silent.journal_path);
        let nodes = vec![silent];
        let progress = JournalProgress::new();
        progress.sample(&nodes, 1_000);
        progress.sample(&nodes, 2_000);
        let roster = progress.roster(&nodes, &BTreeSet::new());
        assert_eq!(roster[0].last_progress_at_ms, None);
        assert_eq!(roster[0].journal_lines, 0);
    }

    /// `exited_early` is reported verbatim from the caller and is NOT
    /// suppressed for any node — the runner records the observation, the
    /// judge decides whether a fault explains it (module docs).
    #[test]
    fn exited_early_is_recorded_without_being_excused_here() {
        let dir = scratch("exited");
        let node = spec(&dir, "gone");
        let nodes = vec![node];
        let progress = JournalProgress::new();
        let exited: BTreeSet<String> = ["gone".to_owned()].into_iter().collect();
        let roster = progress.roster(&nodes, &exited);
        assert!(roster[0].exited_early);
    }

    /// The manifest round-trips to the wire shape the judge parses (module
    /// docs' `Shape` block), field names included.
    #[test]
    fn the_manifest_serializes_to_the_documented_shape() {
        let dir = scratch("shape");
        let manifest = RunManifest {
            started_at_ms: 10,
            ended_at_ms: 90,
            sample_interval_ms: DEFAULT_SAMPLE_INTERVAL_MS,
            nodes: vec![NodeRun {
                name: "fleet-0-0".to_owned(),
                journal_path: PathBuf::from("/tmp/j.ndjson"),
                journal_lines: 7,
                last_progress_at_ms: Some(80),
                exited_early: false,
            }],
        };
        let path = dir.join("run.json");
        write(&path, &manifest).unwrap();
        let value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(value["started_at_ms"], 10);
        assert_eq!(value["ended_at_ms"], 90);
        assert_eq!(value["sample_interval_ms"], DEFAULT_SAMPLE_INTERVAL_MS);
        assert_eq!(value["nodes"][0]["name"], "fleet-0-0");
        assert_eq!(value["nodes"][0]["journal_lines"], 7);
        assert_eq!(value["nodes"][0]["last_progress_at_ms"], 80);
        assert_eq!(value["nodes"][0]["exited_early"], false);
    }
}
