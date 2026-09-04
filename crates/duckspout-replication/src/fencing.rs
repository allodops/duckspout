//! Incarnation fencing (§5.7).
//!
//! A node boots with a persisted, monotonically increasing incarnation;
//! every message it sends — `Forward`, `PeerApply`'s implicit accept,
//! `Receipt`, `Heartbeat`, a drain commit — carries `(node_id, incarnation)`.
//! Peers track the highest incarnation seen per logical sender and reject
//! anything older (`FencedZombie`, `specs/DuckSpoutCore.tla` lines ~304,
//! ~988): a partitioned former self that wakes and tries to forward,
//! receipt, or commit is refused everywhere with a token it cannot forge
//! forward.
//!
//! [`FenceTable`] is exactly that receiver-held bookkeeping —
//! `highestSeen: [Nodes -> [Nodes -> Nat]]` in the TLA+ model, `highestSeen:
//! map[int, int]` per logical sender in `p/Replication/Node.p`. What is
//! **not** here, and is `duckspout-replication` issue #53's separate scope
//! (`Incarnation fencing + registry claims`): `FenceBoot`'s own boot-time
//! incarnation draw from the catalog sequence, `DegradedBoot`'s
//! catalog-outage boot split, and `ClaimAdvertise`'s registry rows. This
//! module gives `Forward`/`PeerApply`/`Receipt` (issue #51) exactly the
//! comparison-and-reject primitive those three need to be minimally correct
//! now — a node's own incarnation is simply handed in by the caller (the
//! daemon, once #53 lands `FenceBoot`) as an opaque, already-drawn
//! [`Incarnation`].

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use duckspout_types::NodeId;

/// A node's boot incarnation (§5.7): persisted locally, advanced on every
/// `FenceBoot`, compared to fence stale writers. `0` is never minted by a
/// real boot (`FenceBoot` is always `priorIncarnation + 1`, minimum `1`) —
/// [`FenceTable`] relies on this to give every never-before-seen sender a
/// harmless `Incarnation(0)` floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Incarnation(pub u64);

impl Incarnation {
    /// The next incarnation, taken at `FenceBoot`.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A fencing identity: which incarnation of which node performed a write.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FenceIdentity {
    /// The node.
    pub node: NodeId,
    /// Its incarnation at write time.
    pub incarnation: Incarnation,
}

/// The result of evaluating one incoming message's [`FenceIdentity`] against
/// a [`FenceTable`] (`eFenceDecision`, `p/Replication/Node.p`/`Spec.p`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceOutcome {
    /// `incarnation >= highest seen so far from this sender` — the message
    /// is genuine (or a legitimate retransmit under the same incarnation).
    /// The table's record for this sender has been advanced to
    /// `max(previous, incarnation)`.
    Admitted,
    /// `incarnation < highest seen so far` — a zombie: a partitioned former
    /// self, or a stale retransmit predating a reboot. The table is
    /// unchanged; the caller must apply nothing, advertise nothing, and
    /// receipt nothing on the strength of this message (`FencedZombie`).
    Zombie {
        /// The highest incarnation already on file for this sender — for
        /// diagnostics/logging at the call site.
        highest_seen: Incarnation,
    },
}

/// Receiver-held fencing state: the highest incarnation seen so far from
/// each logical sender (`highestSeen`, `specs/DuckSpoutCore.tla`;
/// `p/Replication/Node.p`'s `highestSeen: map[int, int]`). One instance per
/// receiving node; keyed by the sender's logical [`NodeId`], not by any
/// transport-level connection identity, so a rebooted sender (a new process,
/// same logical node) is still recognized as "the same sender" a
/// higher-incarnation message already arrived from.
#[derive(Debug, Clone, Default)]
pub struct FenceTable {
    highest_seen: HashMap<NodeId, Incarnation>,
}

impl FenceTable {
    /// An empty table: every sender starts at the `Incarnation(0)` floor
    /// (matching TLA+'s `[n \in Nodes |-> 0]` `Init`).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Evaluates `identity` against this table's record for `identity.node`,
    /// advancing the record on [`FenceOutcome::Admitted`]. This is the one
    /// guard `Forward`, `PeerApply`, and `Receipt` all share (§5.7: one
    /// fencing token per sender, not one per message kind) — call it first,
    /// before any gap-freedom or apply/receipt logic, exactly as
    /// `specs/DuckSpoutCore.tla`'s `PeerApply` guard evaluates fencing
    /// before `GapFreedom`'s `AppliedThru` conjunct.
    pub fn admit(&mut self, identity: &FenceIdentity) -> FenceOutcome {
        let highest_seen = self
            .highest_seen
            .get(&identity.node)
            .copied()
            .unwrap_or(Incarnation(0));
        if identity.incarnation < highest_seen {
            return FenceOutcome::Zombie { highest_seen };
        }
        self.highest_seen
            .insert(identity.node.clone(), identity.incarnation);
        FenceOutcome::Admitted
    }

    /// The highest incarnation seen so far from `node` — `Incarnation(0)`
    /// when none has ever been seen. Read-only; does not affect a
    /// subsequent [`FenceTable::admit`].
    #[must_use]
    pub fn highest_seen(&self, node: &NodeId) -> Incarnation {
        self.highest_seen
            .get(node)
            .copied()
            .unwrap_or(Incarnation(0))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn id(node: &str, inc: u64) -> FenceIdentity {
        FenceIdentity {
            node: NodeId::new(node),
            incarnation: Incarnation(inc),
        }
    }

    /// A never-before-seen sender is always admitted, at any incarnation
    /// (including 0) — `highestSeen`'s TLA+ default floor is 0, so nothing
    /// is ever a zombie on a sender's very first message. Would catch an
    /// off-by-one default (e.g. `u64::MAX`) that fenced out every genuine
    /// first contact.
    #[test]
    fn first_contact_from_any_sender_is_always_admitted() {
        let mut table = FenceTable::new();
        assert_eq!(table.admit(&id("n1", 0)), FenceOutcome::Admitted);
        let mut table = FenceTable::new();
        assert_eq!(table.admit(&id("n1", 7)), FenceOutcome::Admitted);
    }

    /// A strictly lower incarnation than the highest already accepted from
    /// the SAME sender is a zombie — this is `FencedZombie`'s exact
    /// yardstick (`staleApplied = {}` iff every accepted message's
    /// incarnation is `>= ` the highest already accepted from that sender).
    /// Would catch a fence that compares against the wrong key (e.g. a
    /// single global counter instead of per-sender), or an inverted
    /// comparison.
    #[test]
    fn strictly_lower_incarnation_from_a_known_sender_is_a_zombie() {
        let mut table = FenceTable::new();
        assert_eq!(table.admit(&id("n1", 2)), FenceOutcome::Admitted);
        assert_eq!(
            table.admit(&id("n1", 1)),
            FenceOutcome::Zombie {
                highest_seen: Incarnation(2)
            }
        );
        // The table is unchanged by a rejected message: a later retry at
        // the ORIGINAL (still-stale) incarnation is rejected identically,
        // not accepted because the rejection "moved the goalposts."
        assert_eq!(
            table.admit(&id("n1", 1)),
            FenceOutcome::Zombie {
                highest_seen: Incarnation(2)
            }
        );
    }

    /// A repeat of the SAME incarnation is admitted (a legitimate
    /// retransmit under an unchanged incarnation is not a zombie — only a
    /// STRICTLY lower one is, per `g.inc >= highestSeen` in
    /// `specs/DuckSpoutCore.tla`'s `PeerApply`, not `>`).
    #[test]
    fn repeat_of_the_same_incarnation_is_admitted() {
        let mut table = FenceTable::new();
        assert_eq!(table.admit(&id("n1", 3)), FenceOutcome::Admitted);
        assert_eq!(table.admit(&id("n1", 3)), FenceOutcome::Admitted);
    }

    /// Fencing state is per-sender: a low incarnation from a DIFFERENT node
    /// is unaffected by another node's high-water mark. Would catch a
    /// table keyed by nothing (a single shared counter) instead of by
    /// sender identity.
    #[test]
    fn fencing_is_scoped_per_sender_not_global() {
        let mut table = FenceTable::new();
        assert_eq!(table.admit(&id("n1", 100)), FenceOutcome::Admitted);
        assert_eq!(table.admit(&id("n2", 1)), FenceOutcome::Admitted);
    }

    proptest! {
        /// §8.5-style law: for ANY sequence of (sender, incarnation) pairs,
        /// a message is admitted if and only if its incarnation is >= the
        /// true maximum incarnation already admitted from that same sender
        /// earlier in the sequence — checked against an independent,
        /// naively-recomputed ground truth (a plain `HashMap` scan), not by
        /// reading `FenceTable`'s own bookkeeping back at itself (the same
        /// non-tautological convention `Spec.p`'s `FencedZombie` uses
        /// against `Node.p`). Would catch any FenceTable bug across
        /// arbitrary interleavings of senders and incarnations, not just
        /// the hand-picked cases above.
        #[test]
        fn admits_iff_at_or_above_the_true_per_sender_maximum(
            events in prop::collection::vec((0u8..4, 0u64..8), 1..40)
        ) {
            let mut table = FenceTable::new();
            let mut ground_truth: HashMap<u8, u64> = HashMap::new();
            for (sender, incarnation) in events {
                let node = NodeId::new(sender.to_string());
                let identity = FenceIdentity { node, incarnation: Incarnation(incarnation) };
                let outcome = table.admit(&identity);
                let highest_before = ground_truth.get(&sender).copied().unwrap_or(0);
                if incarnation >= highest_before {
                    prop_assert_eq!(outcome, FenceOutcome::Admitted);
                    ground_truth.insert(sender, incarnation);
                } else {
                    prop_assert_eq!(outcome, FenceOutcome::Zombie { highest_seen: Incarnation(highest_before) });
                }
            }
        }
    }
}
