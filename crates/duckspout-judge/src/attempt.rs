//! Attempt/resolution matching (§8.4): "an attempt with no journaled
//! resolution, or a resolution with no journaled attempt, is itself a
//! finding."
//!
//! This module is the generic FIFO-matching primitive only. It makes no
//! claim about which pairs of `duckspout_types::TraceEvent` variants are
//! attempt/resolution pairs for any given predicate — the frozen §3.3
//! vocabulary's variants are payload-free at bootstrap
//! (`duckspout_types::trace` module docs), so correlating events across
//! NODES needs a payload to key on that does not exist yet; correlating
//! events on the SAME node, in seq order, is what this module offers, and
//! callers supply their own [`AttemptResolutionRule`]s naming which pairs
//! matter to their predicate. #205's own zero-acked-lost predicate does not
//! need this (it checks loadgen-journaled `ClientAck` identities against
//! final-system read-back, not node-side attempt/resolution pairing); this
//! is the shared plumbing #206/#207/#208's future predicates are expected
//! to reuse — the "sharpest fault window" §8.4 names as the reason this
//! kind of check matters (the partition owner mid-drain, between `PutPart`
//! and `LakeCommit`) is exactly a `PutPart` → `{LakeCommitOk,
//! LakeCommitAbort, LakeCommitIndeterminate}` rule in this shape.

use std::collections::{HashMap, VecDeque};

use duckspout_types::{NodeId, TraceEvent};

use crate::journal::JournalLine;

/// Pairs one "attempt" event with the resolution events that count as its
/// outcome, on the same node.
#[derive(Debug, Clone, Copy)]
pub struct AttemptResolutionRule {
    /// The attempt event.
    pub attempt: TraceEvent,
    /// Any of these, on the same node, resolves one open `attempt`.
    pub resolutions: &'static [TraceEvent],
}

/// One unmatched half of an attempt/resolution pair (§8.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnmatchedFinding {
    /// An attempt with no resolution anywhere after it, on the same node,
    /// by the end of that node's journal.
    AttemptWithoutResolution {
        /// The node whose journal has the gap.
        node: NodeId,
        /// The attempt's seq.
        seq: u64,
        /// The attempt event left unresolved.
        attempt: TraceEvent,
    },
    /// A resolution with no preceding, still-open attempt on the same node.
    ResolutionWithoutAttempt {
        /// The node whose journal has the gap.
        node: NodeId,
        /// The resolution's seq.
        seq: u64,
        /// The resolution event with nothing open to resolve.
        resolution: TraceEvent,
    },
}

/// Walks `lines`, per node, in seq order, and matches each `rule.attempt`
/// occurrence to the next unmatched `rule.resolutions` occurrence on the
/// SAME node (FIFO per node per rule — the natural order for a request
/// pipeline where attempts of one kind resolve in the order they were
/// made). Returns every unmatched half, across every rule.
///
/// `lines` need not already be seq-sorted; this function sorts a local
/// index by `(node, seq)` before walking.
#[must_use]
pub fn match_attempts(
    lines: &[JournalLine],
    rules: &[AttemptResolutionRule],
) -> Vec<UnmatchedFinding> {
    let mut ordered: Vec<&JournalLine> = lines.iter().collect();
    ordered.sort_by(|a, b| (a.node.as_str(), a.seq).cmp(&(b.node.as_str(), b.seq)));

    let mut findings = Vec::new();
    for rule in rules {
        let mut open: HashMap<NodeId, VecDeque<u64>> = HashMap::new();
        for line in &ordered {
            if line.event == rule.attempt {
                open.entry(line.node.clone())
                    .or_default()
                    .push_back(line.seq);
            } else if rule.resolutions.contains(&line.event) {
                match open.get_mut(&line.node).and_then(VecDeque::pop_front) {
                    Some(_) => {}
                    None => findings.push(UnmatchedFinding::ResolutionWithoutAttempt {
                        node: line.node.clone(),
                        seq: line.seq,
                        resolution: line.event,
                    }),
                }
            }
        }
        for (node, queue) in open {
            for seq in queue {
                findings.push(UnmatchedFinding::AttemptWithoutResolution {
                    node: node.clone(),
                    seq,
                    attempt: rule.attempt,
                });
            }
        }
    }
    findings
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn line(node: &str, seq: u64, event: TraceEvent) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new(node),
            seq,
            event,
            identity: None,
        }
    }

    const PUT_PART_COMMITS: AttemptResolutionRule = AttemptResolutionRule {
        attempt: TraceEvent::PutPart,
        resolutions: &[
            TraceEvent::LakeCommitOk,
            TraceEvent::LakeCommitAbort,
            TraceEvent::LakeCommitIndeterminate,
        ],
    };

    #[test]
    fn a_resolved_attempt_produces_no_finding() {
        let lines = vec![
            line("n1", 0, TraceEvent::PutPart),
            line("n1", 1, TraceEvent::LakeCommitOk),
        ];
        assert!(match_attempts(&lines, &[PUT_PART_COMMITS]).is_empty());
    }

    #[test]
    fn an_attempt_with_no_resolution_is_a_finding() {
        // The exact §8.4 fault window this rule exists for: a node killed
        // between PutPart and LakeCommit.
        let lines = vec![line("n1", 0, TraceEvent::PutPart)];
        let findings = match_attempts(&lines, &[PUT_PART_COMMITS]);
        assert_eq!(
            findings,
            vec![UnmatchedFinding::AttemptWithoutResolution {
                node: NodeId::new("n1"),
                seq: 0,
                attempt: TraceEvent::PutPart,
            }]
        );
    }

    #[test]
    fn a_resolution_with_no_attempt_is_a_finding() {
        let lines = vec![line("n1", 0, TraceEvent::LakeCommitAbort)];
        let findings = match_attempts(&lines, &[PUT_PART_COMMITS]);
        assert_eq!(
            findings,
            vec![UnmatchedFinding::ResolutionWithoutAttempt {
                node: NodeId::new("n1"),
                seq: 0,
                resolution: TraceEvent::LakeCommitAbort,
            }]
        );
    }

    #[test]
    fn matching_is_fifo_per_node_and_never_crosses_nodes() {
        // Two attempts on n1 resolve in order; n2's lone attempt stays
        // unresolved — would catch a global (non-per-node) queue wrongly
        // pairing n2's attempt with one of n1's resolutions.
        let lines = vec![
            line("n1", 0, TraceEvent::PutPart),
            line("n2", 0, TraceEvent::PutPart),
            line("n1", 1, TraceEvent::PutPart),
            line("n1", 2, TraceEvent::LakeCommitOk),
            line("n1", 3, TraceEvent::LakeCommitAbort),
        ];
        let findings = match_attempts(&lines, &[PUT_PART_COMMITS]);
        assert_eq!(
            findings,
            vec![UnmatchedFinding::AttemptWithoutResolution {
                node: NodeId::new("n2"),
                seq: 0,
                attempt: TraceEvent::PutPart,
            }]
        );
    }

    #[test]
    fn unrelated_events_are_ignored() {
        let lines = vec![line("n1", 0, TraceEvent::Heartbeat)];
        assert!(match_attempts(&lines, &[PUT_PART_COMMITS]).is_empty());
    }
}
