//! The fleet run's manifest (`run.json`), read back for the two §8.4
//! run-level vacuity rules that are statements about the RUN rather than
//! about any journal line: "a run with no observed cross-node contention when
//! contention is what the run exists to certify," and "a node whose journals
//! simply stop."
//!
//! `duckspout_fleet::runlog`'s module docs carry the full argument for why
//! this file has to exist — in one line: the D-6 journals name no roster and
//! carry no clock, so neither "how many nodes did this run have" nor "when
//! did this node last say anything" is a question they can be asked. The
//! manifest is the runner's witness statement about both; every threshold and
//! every verdict derived from it lives in `crate::vacuity`, on this side of
//! D-5's line.
//!
//! Decoded here independently rather than by depending on `duckspout-fleet`,
//! for the reason `crate::journal` states for the journals themselves.
//!
//! # What this module deliberately does not trust
//!
//! The manifest names a roster and a sampling series. It does NOT name which
//! faults ran, and this module never asks it to: §8.4 requires fault firing
//! to be "measured from each injector's own ledger, not assumed from the
//! profile," so `crate::fault_ledger` is the only source for that, and
//! `crate::vacuity` joins the two rather than letting either speak for the
//! other.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// One roster member (`duckspout_fleet::runlog::NodeRun`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NodeRun {
    /// The node's roster name — the HOST half of the
    /// `<hostname>/<incarnation>` [`duckspout_types::NodeId`] it journals
    /// under. [`node_host`] is the projection that makes the join.
    pub name: String,
    /// Where the node's journal was written (diagnostics; never a join key —
    /// `duckspout_fleet::runlog::NodeRun::name` carries the reasoning).
    pub journal_path: PathBuf,
    /// Lines in that journal at [`RunManifest::ended_at_ms`].
    pub journal_lines: u64,
    /// The last sampled moment at which `journal_lines` grew. `None` for a
    /// node that never journaled a line at all.
    pub last_progress_at_ms: Option<u64>,
    /// Whether the node's process had already exited when teardown reached
    /// it — recorded unexcused by the runner; `crate::vacuity` decides
    /// whether a fault accounts for it.
    pub exited_early: bool,
}

/// One fleet run's manifest (module docs).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RunManifest {
    /// When the runner started provisioning, in Unix milliseconds.
    pub started_at_ms: u64,
    /// When the run's load/fault phase finished, measured before teardown.
    pub ended_at_ms: u64,
    /// The sampling grain of every `last_progress_at_ms`, which
    /// `crate::vacuity` widens its exemption windows by rather than assuming
    /// a resolution.
    pub sample_interval_ms: u64,
    /// Every node the runner actually started, in roster order.
    pub nodes: Vec<NodeRun>,
}

impl RunManifest {
    /// The last moment ANY roster node was observed to journal something —
    /// the run's own high-water mark of activity.
    ///
    /// This, not [`RunManifest::ended_at_ms`], is what
    /// `crate::vacuity`'s node-continuity rule measures a node's silence
    /// against, and the difference is the whole reason that rule does not
    /// fire on every run: `ended_at_ms` is followed by a teardown during
    /// which every node legitimately goes quiet, and the interval between the
    /// last real work and the runner noticing it is over is neither fixed nor
    /// interesting. Measuring each node against the fleet's own last activity
    /// makes the rule self-normalising — a fleet that all went quiet together
    /// convicts nobody, and a node that went quiet while its peers kept
    /// working stands out exactly as far as it should.
    ///
    /// `None` when no node ever journaled anything.
    #[must_use]
    pub fn last_progress_at_ms(&self) -> Option<u64> {
        self.nodes
            .iter()
            .filter_map(|n| n.last_progress_at_ms)
            .max()
    }

    /// Decodes a manifest from its JSON text.
    ///
    /// Same fail-closed posture as `crate::journal` and
    /// `crate::fault_ledger`: a repeated TOP-LEVEL key is rejected rather
    /// than silently resolved last-value-wins. The hazard here is the same
    /// shape as the fault ledger's duplicated `phase` — a repeated `nodes`
    /// key would let one roster silently replace another, and a repeated
    /// `sample_interval_ms` would change the tolerance every exemption in
    /// `crate::vacuity` is computed with. Consistency across the three
    /// evidence readers is the point: a judge that fails closed on one
    /// evidence file and open on another is only as honest as its weakest
    /// reader.
    ///
    /// # Errors
    ///
    /// Any `serde_json` decode error, duplicate top-level keys included — a
    /// manifest that does not parse is not a manifest, and the caller reports
    /// `NoVerdict` rather than judging the run's roster from nothing.
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        crate::journal::reject_duplicate_keys(text)?;
        serde_json::from_str(text)
    }
}

/// Reads `path` and decodes it.
///
/// # Errors
///
/// The reason the manifest could not be used, already rendered for an
/// operator: an unreadable file and an undecodable one produce the same
/// honest wording, exactly as `crate::runner`'s fixture loader does for the
/// read-back fixtures.
pub fn parse_run_manifest(path: &Path) -> Result<RunManifest, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| format!("reading run manifest {}: {err}", path.display()))?;
    RunManifest::from_json(&text).map_err(|err| format!("{}: {err}", path.display()))
}

/// The HOST half of a journaled [`duckspout_types::NodeId`] — everything
/// before the first `/`.
///
/// A node journals as `<hostname>/<incarnation>`
/// (`duckspout_daemon::system::detect_node_id`), and the incarnation changes
/// when the process restarts, while the roster name does not. Joining on the
/// host half therefore survives a restart within one run (a
/// `membership_join`'s new process, a supervisor-restarted node) instead of
/// reporting the same machine as two, one of which "vanished". An id with no
/// `/` at all — the shape a hand-written fixture or a non-fleet journal uses
/// — is its own host.
///
/// This is also the projection that joins the fault ledger to the roster:
/// `duckspout_fleet::fault`'s `rendered_node_id` writes the RENDERED form
/// into `faults.ndjson`'s `target_node` while `duckspout_fleet::runlog`
/// writes the bare form into the manifest, so `crate::fault_ledger` runs
/// every lookup through here too.
///
/// The other half of "a restart is one machine" lives on the producer side:
/// `duckspout_fleet::runlog::JournalProgress::sample` re-baselines a node
/// whose journal was truncated by a restart, so a restart is progress in the
/// sample series rather than a gap in it.
#[must_use]
pub fn node_host(node_id: &str) -> &str {
    node_id.split('/').next().unwrap_or(node_id)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    const MANIFEST: &str = r#"{
        "started_at_ms": 1000,
        "ended_at_ms": 9000,
        "sample_interval_ms": 500,
        "nodes": [
            {"name":"fleet-0-0","journal_path":"/w/0/journal.ndjson","journal_lines":10,
             "last_progress_at_ms":8500,"exited_early":false},
            {"name":"fleet-0-1","journal_path":"/w/1/journal.ndjson","journal_lines":3,
             "last_progress_at_ms":2000,"exited_early":true},
            {"name":"fleet-0-2","journal_path":"/w/2/journal.ndjson","journal_lines":0,
             "last_progress_at_ms":null,"exited_early":false}
        ]
    }"#;

    #[test]
    fn the_documented_wire_shape_decodes() {
        let manifest = RunManifest::from_json(MANIFEST).expect("decodes");
        assert_eq!(manifest.nodes.len(), 3);
        assert_eq!(manifest.nodes[1].last_progress_at_ms, Some(2000));
        assert!(manifest.nodes[1].exited_early);
        assert_eq!(manifest.nodes[2].last_progress_at_ms, None);
    }

    /// The run's high-water mark is the MAXIMUM across nodes, never the
    /// run's declared end — the distinction the node-continuity rule rests
    /// on ([`RunManifest::last_progress_at_ms`] docs).
    #[test]
    fn the_runs_last_progress_is_the_busiest_nodes_not_the_declared_end() {
        let manifest = RunManifest::from_json(MANIFEST).expect("decodes");
        assert_eq!(manifest.last_progress_at_ms(), Some(8500));
        assert_ne!(manifest.last_progress_at_ms(), Some(manifest.ended_at_ms));
    }

    #[test]
    fn a_run_where_nobody_journaled_has_no_high_water_mark() {
        let manifest = RunManifest::from_json(
            r#"{"started_at_ms":1,"ended_at_ms":2,"sample_interval_ms":500,
                "nodes":[{"name":"n","journal_path":"/j","journal_lines":0,
                          "last_progress_at_ms":null,"exited_early":false}]}"#,
        )
        .expect("decodes");
        assert_eq!(manifest.last_progress_at_ms(), None);
    }

    /// The join key survives an incarnation change: one machine that
    /// restarted mid-run is one roster member, not two — one of which would
    /// otherwise look like it vanished.
    #[test]
    fn the_host_half_of_a_node_id_is_stable_across_incarnations() {
        assert_eq!(node_host("fleet-0-1/1"), "fleet-0-1");
        assert_eq!(node_host("fleet-0-1/2"), "fleet-0-1");
        assert_eq!(node_host("loadgen-0"), "loadgen-0");
        assert_eq!(node_host(""), "");
    }

    #[test]
    fn an_unreadable_manifest_is_a_reason_naming_the_file() {
        let err =
            parse_run_manifest(Path::new("/nonexistent/run.json")).expect_err("must not succeed");
        assert!(err.contains("run.json"), "{err}");
    }

    /// The same fail-closed posture `crate::fault_ledger` documents and
    /// tests, on the file that feeds two of the four run-level rules. Would
    /// catch a bare `serde_json::from_str` here: under last-value-wins a
    /// second `nodes` key silently replaces the roster, and a second
    /// `sample_interval_ms` silently changes every exemption's tolerance.
    #[test]
    fn a_duplicated_top_level_key_is_rejected_not_resolved_last_value_wins() {
        let err = RunManifest::from_json(
            r#"{"started_at_ms":1,"ended_at_ms":2,"sample_interval_ms":500,
                "nodes":[{"name":"n","journal_path":"/j","journal_lines":1,
                          "last_progress_at_ms":1,"exited_early":false}],
                "nodes":[]}"#,
        )
        .expect_err("must not succeed");
        assert!(err.to_string().contains("nodes"), "{err}");
    }

    #[test]
    fn a_malformed_manifest_is_a_reason_not_a_panic() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(b"not json").expect("write");
        let err = parse_run_manifest(file.path()).expect_err("must not succeed");
        assert!(!err.is_empty());
    }
}
