//! The fault injectors' own ledger (`faults.ndjson`), read back for §8.4's
//! first run-level vacuity rule: "a fault schedule that armed faults and
//! fired none (**measured from each injector's own ledger, not assumed from
//! the profile**)."
//!
//! That parenthesis is the whole point of this module. The run's CLI profile
//! says which `--fault-*` flags were passed; it does NOT say whether the
//! signal was ever sent, whether the target was still alive to receive it, or
//! whether the injector bailed before touching anything. Only the injector's
//! own `Armed`/`Started`/`Ended` lines say that
//! (`duckspout_fleet::faultlog`'s three-phase shape), so this module reads
//! them and nothing else. A judge that inferred "the fault fired" from "the
//! flag was passed" would be certifying resilience under faults that never
//! happened — which is exactly the vacuity §8.4 forbids.
//!
//! # Wire shape
//!
//! One JSON object per line, field-for-field
//! `duckspout_fleet::faultlog::FaultWindowLine` — decoded here independently
//! rather than by depending on `duckspout-fleet`, for the reason
//! `crate::journal` states for the D-6 journals themselves (a judge parses
//! the format a producer writes without linking the producer; the whole
//! premise of D-5 is that the two are separate processes).
//!
//! ```json
//! {"fault_id":"kill-0","kind":"node_kill","target_node":"fleet-0-1",
//!  "phase":"armed","at_ms":1700000010000}
//! {"fault_id":"kill-0","kind":"node_kill","target_node":"fleet-0-1",
//!  "phase":"started","at_ms":1700000010040,"detail":{"pid":4242}}
//! ```
//!
//! `detail` is deliberately ignored: it is the fault log's own free-form
//! per-injector context (`duckspout_fleet::faultlog::FaultWindowLine::detail`
//! calls it exactly that), and no vacuity rule may depend on a shape no
//! injector is obliged to keep stable.
//!
//! # Ingestion posture
//!
//! `crate::journal`'s posture, unchanged: one malformed line fails the whole
//! file closed, and a repeated top-level JSON key is rejected rather than
//! silently resolved last-value-wins (here the hazard is a duplicated
//! `phase`, which would silently turn an armed-but-unfired fault into a fired
//! one — the precise fact this module exists to establish).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Every fault kind whose effect on its target is PERMANENT: after the
/// window, the node is gone for the rest of the run.
///
/// The strings are `duckspout_fleet::faultlog::FaultKind`'s own `snake_case`
/// serialization. This module deliberately decodes `kind` as a free `String`
/// rather than a mirrored enum, so that an injector added on the fleet side
/// never fails this judge closed over a kind it has not heard of; the price
/// is this one list, which IS coupled to the fleet's vocabulary and is
/// tested against the full set below.
///
/// # Why the classification matters, and which way an unknown kind falls
///
/// It is used for exactly one thing: excusing a node whose journal stopped
/// (`crate::vacuity`'s node-continuity rule). A `node_kill` ends its window
/// as soon as the process is confirmed dead — an `Ended` line early in the
/// run — yet the target stays silent from then to the end of the run, and
/// that silence is entirely explained. A `network_partition`, by contrast,
/// ends when the link is restored, and a target that never journals again
/// afterwards is a genuinely vanished machine that the fault does NOT
/// explain.
///
/// An unrecognised kind is therefore treated as TRANSIENT: it earns its
/// target the overlap-scoped excuse any fault window grants, but never a
/// blanket excuse for the whole rest of the run. That is the fail-closed
/// direction — a judge must not hand a permanent alibi to a fault it does
/// not understand.
const TERMINAL_FAULT_KINDS: &[&str] = &["node_kill", "membership_leave", "flight_kill_mid_stream"];

/// One phase of a fault window (`duckspout_fleet::faultlog::FaultPhase`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultPhase {
    /// Scheduled: the injector named its target but has sent no signal yet.
    Armed,
    /// In effect: the signal was sent and, where confirmable, observed to
    /// have landed.
    Started,
    /// Over: the fault's effect was confirmed resolved.
    Ended,
}

/// One line of the ledger, as written.
#[derive(Debug, Clone, Deserialize)]
struct FaultLine {
    fault_id: String,
    kind: String,
    target_node: String,
    phase: FaultPhase,
    at_ms: u64,
}

/// One logical fault window, reassembled from its up-to-three lines by
/// `fault_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultWindow {
    /// The injector's stable identity for this window.
    pub fault_id: String,
    /// Its kind, verbatim from the ledger
    /// ([`TERMINAL_FAULT_KINDS`] on why this is a `String`).
    pub kind: String,
    /// The roster name of the node it targets
    /// (`duckspout_fleet::topology::node_name`).
    pub target_node: String,
    /// When it was armed, if an `Armed` line was journaled.
    pub armed_at_ms: Option<u64>,
    /// When it took effect, if a `Started` line was journaled. `None` is
    /// exactly §8.4's "armed and never fired".
    pub started_at_ms: Option<u64>,
    /// When its effect was confirmed over, if an `Ended` line was journaled.
    pub ended_at_ms: Option<u64>,
}

impl FaultWindow {
    /// Whether this window's effect is permanent
    /// ([`TERMINAL_FAULT_KINDS`]).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        TERMINAL_FAULT_KINDS.contains(&self.kind.as_str())
    }

    /// Whether this window was in effect across the WHOLE interval
    /// `[from_ms, until_ms]`, given a sampling `slack_ms`.
    ///
    /// This is the alibi test for a target that journaled nothing over that
    /// interval, and it is deliberately a containment test, not an overlap
    /// test: a partition that was LIFTED partway through, after which the
    /// node still never spoke again, explains the first half of the silence
    /// and none of the rest — so it is not an alibi at all. A window with no
    /// `Ended` line was still in effect when the run finished, so it covers
    /// any interval it started before.
    ///
    /// An armed-but-unfired window never covers anything: it did nothing to
    /// anybody, and it is its own vacuity finding under
    /// `crate::vacuity::check_fault_schedule` rather than an excuse for
    /// somebody else's.
    ///
    /// `slack_ms` widens both ends by the run manifest's own sampling
    /// interval, since `last_progress_at_ms` is only that precise
    /// (`crate::run_manifest::RunManifest::sample_interval_ms`).
    ///
    /// Note what is NOT here: terminal-ness. A `node_kill`'s `Ended` line
    /// means "the process is confirmed dead", not "the node is back", so
    /// asking whether such a window *covers* the rest of the run would answer
    /// no for entirely the wrong reason. Terminal faults are handled where
    /// they belong — by shortening the interval a node is expected to journal
    /// over at all (`crate::vacuity::check_node_continuity`'s horizon).
    #[must_use]
    pub fn covers(&self, from_ms: u64, until_ms: u64, slack_ms: u64) -> bool {
        let Some(started) = self.started_at_ms else {
            return false;
        };
        if started > from_ms.saturating_add(slack_ms) {
            // The node had already gone quiet before this fault touched it.
            return false;
        }
        match self.ended_at_ms {
            None => true,
            Some(ended) => ended.saturating_add(slack_ms) >= until_ms,
        }
    }
}

/// Every fault window one fleet run's injectors journaled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultLedger {
    /// The windows, ordered by `fault_id` (stable, so findings are
    /// reproducible run to run).
    pub windows: Vec<FaultWindow>,
}

impl FaultLedger {
    /// Every window targeting `node`.
    pub fn windows_targeting<'a>(
        &'a self,
        node: &'a str,
    ) -> impl Iterator<Item = &'a FaultWindow> + 'a {
        self.windows.iter().filter(move |w| w.target_node == node)
    }

    /// When a TERMINAL fault first took `node` out of the run for good, if
    /// one did ([`TERMINAL_FAULT_KINDS`]) — the earliest such `Started`.
    ///
    /// `crate::vacuity::check_node_continuity` uses this as the node's
    /// horizon: after this instant the node is expected to journal nothing,
    /// so its silence past it is the fault schedule working, while its
    /// silence BEFORE it is still measured on the ordinary budget. A node
    /// that went quiet a minute before it was killed really did vanish a
    /// minute before it was killed, and folding that into the kill's alibi
    /// would hide it.
    #[must_use]
    pub fn terminal_horizon(&self, node: &str) -> Option<u64> {
        self.windows_targeting(node)
            .filter(|window| window.is_terminal())
            .filter_map(|window| window.started_at_ms)
            .min()
    }
}

/// Ingestion failure — fails closed rather than skipping the bad line
/// (module docs).
#[derive(Debug, thiserror::Error)]
pub enum FaultLedgerError {
    /// The ledger file could not be read at all.
    #[error("reading fault ledger {path}: {source}")]
    Io {
        /// The file that failed to open/read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// One line was not a valid fault-window object.
    #[error("{path}:{line_no}: not a valid fault-ledger line: {source}")]
    Decode {
        /// The file the bad line came from.
        path: PathBuf,
        /// The 1-based line number.
        line_no: usize,
        /// The underlying decode error.
        #[source]
        source: serde_json::Error,
    },
    /// One `fault_id` journaled the same phase twice. Each injector writes
    /// each phase at most once (`duckspout_fleet::faultlog`'s three-phase
    /// shape), so a repeat means two different faults collided on one id —
    /// and a collision could hide an armed-but-unfired window behind another
    /// window's `Started` line, which is the exact fact this module
    /// establishes.
    #[error("{path}:{line_no}: fault {fault_id} journaled phase {phase:?} twice")]
    RepeatedPhase {
        /// The file the repeat appeared in.
        path: PathBuf,
        /// The 1-based line number of the repeat.
        line_no: usize,
        /// The colliding fault id.
        fault_id: String,
        /// The phase that repeated.
        phase: FaultPhase,
    },
    /// One `fault_id`'s lines disagreed about its kind or its target — the
    /// same collision hazard as [`FaultLedgerError::RepeatedPhase`], caught
    /// on the other field.
    #[error("{path}:{line_no}: fault {fault_id} is {found} here but {known} on an earlier line")]
    InconsistentWindow {
        /// The file the disagreement appeared in.
        path: PathBuf,
        /// The 1-based line number of the disagreeing line.
        line_no: usize,
        /// The colliding fault id.
        fault_id: String,
        /// This line's `kind/target_node`.
        found: String,
        /// The `kind/target_node` an earlier line established.
        known: String,
    },
}

/// Parses one `faults.ndjson` into its logical fault windows.
///
/// # Errors
///
/// Returns [`FaultLedgerError`] on the first I/O failure, undecodable line,
/// or `fault_id` collision.
pub fn parse_fault_ledger(path: &Path) -> Result<FaultLedger, FaultLedgerError> {
    let text = std::fs::read_to_string(path).map_err(|source| FaultLedgerError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut windows: BTreeMap<String, FaultWindow> = BTreeMap::new();
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        crate::journal::reject_duplicate_keys(raw).map_err(|source| FaultLedgerError::Decode {
            path: path.to_owned(),
            line_no,
            source,
        })?;
        let line: FaultLine =
            serde_json::from_str(raw).map_err(|source| FaultLedgerError::Decode {
                path: path.to_owned(),
                line_no,
                source,
            })?;
        let window = windows
            .entry(line.fault_id.clone())
            .or_insert_with(|| FaultWindow {
                fault_id: line.fault_id.clone(),
                kind: line.kind.clone(),
                target_node: line.target_node.clone(),
                armed_at_ms: None,
                started_at_ms: None,
                ended_at_ms: None,
            });
        if window.kind != line.kind || window.target_node != line.target_node {
            return Err(FaultLedgerError::InconsistentWindow {
                path: path.to_owned(),
                line_no,
                fault_id: line.fault_id,
                found: format!("{}/{}", line.kind, line.target_node),
                known: format!("{}/{}", window.kind, window.target_node),
            });
        }
        let slot = match line.phase {
            FaultPhase::Armed => &mut window.armed_at_ms,
            FaultPhase::Started => &mut window.started_at_ms,
            FaultPhase::Ended => &mut window.ended_at_ms,
        };
        if slot.is_some() {
            return Err(FaultLedgerError::RepeatedPhase {
                path: path.to_owned(),
                line_no,
                fault_id: line.fault_id,
                phase: line.phase,
            });
        }
        *slot = Some(line.at_ms);
    }
    Ok(FaultLedger {
        windows: windows.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn write_temp(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(text.as_bytes()).expect("write");
        file
    }

    fn line(fault_id: &str, kind: &str, target: &str, phase: &str, at_ms: u64) -> String {
        format!(
            "{{\"fault_id\":\"{fault_id}\",\"kind\":\"{kind}\",\"target_node\":\"{target}\",\
             \"phase\":\"{phase}\",\"at_ms\":{at_ms}}}\n"
        )
    }

    fn window(kind: &str, started: Option<u64>, ended: Option<u64>) -> FaultWindow {
        FaultWindow {
            fault_id: "f".to_owned(),
            kind: kind.to_owned(),
            target_node: "n".to_owned(),
            armed_at_ms: Some(0),
            started_at_ms: started,
            ended_at_ms: ended,
        }
    }

    #[test]
    fn the_three_phases_reassemble_into_one_window() {
        let file = write_temp(&format!(
            "{}{}{}",
            line("kill-0", "node_kill", "fleet-0-1", "armed", 10),
            line("kill-0", "node_kill", "fleet-0-1", "started", 20),
            line("kill-0", "node_kill", "fleet-0-1", "ended", 30),
        ));
        let ledger = parse_fault_ledger(file.path()).expect("parses");
        assert_eq!(ledger.windows.len(), 1);
        assert_eq!(ledger.windows[0].armed_at_ms, Some(10));
        assert_eq!(ledger.windows[0].started_at_ms, Some(20));
        assert_eq!(ledger.windows[0].ended_at_ms, Some(30));
    }

    /// The shape §8.4's first rule is about: armed, never started. It must
    /// survive parsing intact rather than being normalised away — would
    /// catch a parser that required a `Started` line to accept the window.
    #[test]
    fn an_armed_only_window_parses_with_no_start() {
        let file = write_temp(&line("pause-1", "sigstop_pause", "fleet-0-2", "armed", 10));
        let ledger = parse_fault_ledger(file.path()).expect("parses");
        assert_eq!(ledger.windows[0].started_at_ms, None);
    }

    /// Interleaved windows from concurrent injectors (the real
    /// `faults.ndjson` shape — `duckspout_fleet::faultlog`'s concurrency
    /// test) correlate on `fault_id`, not on adjacency.
    #[test]
    fn interleaved_windows_correlate_on_fault_id() {
        let file = write_temp(&format!(
            "{}{}{}{}",
            line("a", "node_kill", "n1", "armed", 1),
            line("b", "network_partition", "n2", "armed", 2),
            line("b", "network_partition", "n2", "started", 3),
            line("a", "node_kill", "n1", "started", 4),
        ));
        let ledger = parse_fault_ledger(file.path()).expect("parses");
        assert_eq!(ledger.windows.len(), 2);
        assert_eq!(ledger.windows[0].fault_id, "a");
        assert_eq!(ledger.windows[0].started_at_ms, Some(4));
        assert_eq!(ledger.windows[1].started_at_ms, Some(3));
    }

    #[test]
    fn a_repeated_phase_for_one_fault_id_fails_closed() {
        let file = write_temp(&format!(
            "{}{}",
            line("a", "node_kill", "n1", "started", 1),
            line("a", "node_kill", "n1", "started", 2),
        ));
        assert!(matches!(
            parse_fault_ledger(file.path()),
            Err(FaultLedgerError::RepeatedPhase { .. })
        ));
    }

    #[test]
    fn one_fault_id_with_two_targets_fails_closed() {
        let file = write_temp(&format!(
            "{}{}",
            line("a", "node_kill", "n1", "armed", 1),
            line("a", "node_kill", "n2", "started", 2),
        ));
        assert!(matches!(
            parse_fault_ledger(file.path()),
            Err(FaultLedgerError::InconsistentWindow { .. })
        ));
    }

    /// A duplicated `phase` key would silently turn an armed-but-unfired
    /// fault into a fired one under serde's last-value-wins (module docs).
    #[test]
    fn a_duplicated_phase_key_is_rejected_not_resolved_last_value_wins() {
        let file = write_temp(
            "{\"fault_id\":\"a\",\"kind\":\"node_kill\",\"target_node\":\"n1\",\
             \"phase\":\"armed\",\"phase\":\"started\",\"at_ms\":1}\n",
        );
        assert!(matches!(
            parse_fault_ledger(file.path()),
            Err(FaultLedgerError::Decode { .. })
        ));
    }

    #[test]
    fn a_malformed_line_fails_the_whole_file_closed() {
        let file = write_temp("not json\n");
        assert!(matches!(
            parse_fault_ledger(file.path()),
            Err(FaultLedgerError::Decode { .. })
        ));
    }

    /// Every kind the fleet ships today, classified. This is the coupling
    /// [`TERMINAL_FAULT_KINDS`] admits to, pinned so a fleet-side addition
    /// that belongs in the terminal set is a conscious edit here rather than
    /// a silent default.
    #[test]
    fn the_terminal_classification_covers_every_fleet_fault_kind_today() {
        let terminal = ["node_kill", "membership_leave", "flight_kill_mid_stream"];
        let transient = [
            "sigstop_pause",
            "network_partition",
            "network_degradation",
            "membership_join",
            "catalog_outage",
            "discovery_flap",
            "cache_churn",
        ];
        for kind in terminal {
            assert!(window(kind, Some(0), Some(0)).is_terminal(), "{kind}");
        }
        for kind in transient {
            assert!(!window(kind, Some(0), Some(0)).is_terminal(), "{kind}");
        }
    }

    /// A window that was LIFTED partway through the silence covers none of
    /// it: the node still never spoke again afterwards, which is exactly the
    /// vanished machine. Would catch an OVERLAP test in place of the
    /// containment one — the difference between "a fault touched this node at
    /// some point" and "a fault accounts for this silence."
    #[test]
    fn a_lifted_window_does_not_cover_silence_that_outlasts_it() {
        let partition = window("network_partition", Some(1_000), Some(2_000));
        assert!(partition.covers(1_500, 1_900, 100));
        assert!(!partition.covers(1_500, 5_000, 100));
    }

    /// A window still in effect when the run finished (no `Ended` line)
    /// covers any interval it started before.
    #[test]
    fn an_unresolved_window_covers_everything_after_its_start() {
        let partition = window("network_partition", Some(1_000), None);
        assert!(partition.covers(1_500, 90_000, 0));
        assert!(!partition.covers(500, 90_000, 0));
    }

    /// A node already silent BEFORE the fault touched it is not excused by
    /// it — the fault cannot have caused a silence that predates it.
    #[test]
    fn a_window_that_started_after_the_silence_began_covers_nothing() {
        let kill = window("node_kill", Some(5_000), Some(5_100));
        assert!(!kill.covers(1_000, 5_100, 500));
    }

    /// An armed-but-unfired fault is never an alibi: it did nothing to
    /// anybody, and it is its own vacuity finding.
    #[test]
    fn an_unfired_window_covers_nothing_at_all() {
        let unfired = window("node_kill", None, None);
        assert!(!unfired.covers(1_000, 1_001, 5_000));
    }

    /// The horizon is the earliest TERMINAL start targeting a node, and only
    /// that: a transient fault does not take a node out of the run, and an
    /// armed-but-unfired terminal one never happened.
    #[test]
    fn the_terminal_horizon_is_the_earliest_fired_terminal_start() {
        let ledger = FaultLedger {
            windows: vec![
                FaultWindow {
                    fault_id: "p".to_owned(),
                    kind: "network_partition".to_owned(),
                    target_node: "n1".to_owned(),
                    armed_at_ms: Some(0),
                    started_at_ms: Some(1_000),
                    ended_at_ms: Some(2_000),
                },
                FaultWindow {
                    fault_id: "k2".to_owned(),
                    kind: "node_kill".to_owned(),
                    target_node: "n1".to_owned(),
                    armed_at_ms: Some(0),
                    started_at_ms: Some(9_000),
                    ended_at_ms: Some(9_100),
                },
                FaultWindow {
                    fault_id: "k1".to_owned(),
                    kind: "membership_leave".to_owned(),
                    target_node: "n1".to_owned(),
                    armed_at_ms: Some(0),
                    started_at_ms: Some(5_000),
                    ended_at_ms: Some(5_100),
                },
                FaultWindow {
                    fault_id: "k0".to_owned(),
                    kind: "node_kill".to_owned(),
                    target_node: "n1".to_owned(),
                    armed_at_ms: Some(0),
                    started_at_ms: None,
                    ended_at_ms: None,
                },
                FaultWindow {
                    fault_id: "other".to_owned(),
                    kind: "node_kill".to_owned(),
                    target_node: "n2".to_owned(),
                    armed_at_ms: Some(0),
                    started_at_ms: Some(100),
                    ended_at_ms: Some(200),
                },
            ],
        };
        assert_eq!(ledger.terminal_horizon("n1"), Some(5_000));
        assert_eq!(ledger.terminal_horizon("n2"), Some(100));
        assert_eq!(ledger.terminal_horizon("n3"), None);
    }
}
