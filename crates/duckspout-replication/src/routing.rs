//! Ownership routing (§5.2–5.3, issue #52): resolving a partition's HRW
//! owner and RF replica set against an advisory membership view, and the
//! accept-side routing decision that follows from it — "any node accepts
//! any write ... but the RF set for a partition always includes the ring
//! OWNER" (§5.3).
//!
//! # Membership sourcing at v0.1
//!
//! §5.2's own words: "the candidate set comes from the registry (`nodes`
//! table, section 5), seeded at bootstrap by `cluster.seed_peers` ... and
//! superseded by the registry once reachable." The registry does not exist
//! yet — it is issue #53's scope (`Incarnation fencing + registry claims`).
//! [`MembershipView`] is therefore, at v0.1, permanently in the
//! seed-peers regime: a static snapshot built once at daemon boot from
//! `cluster.seed_peers` plus the local node's own id
//! (`duckspout-daemon/src/wiring.rs`), never refreshed by a live
//! heartbeat/liveness signal. The view is advisory (§5.2: "two nodes
//! briefly holding different views cannot corrupt anything") — every
//! function below treats it as a plain snapshot, never a source of truth.
//!
//! # The ring walk is not separate machinery
//!
//! §5.6 describes dead-node substitution as "the HRW walk over the
//! membership view minus the dead node." That is not new logic to build
//! here: it falls out for free from [`hrw_ranked`] run over whatever
//! candidate set the caller supplies. A liveness-aware caller (post-#53,
//! once a heartbeat/registry feed exists) simply constructs a
//! [`MembershipView`] with the dead node already excluded, and
//! [`route_write`] "ring-walks" automatically — the missing piece #53 adds
//! is the INPUT (who is live), not new ring-walk logic in this module.
//! Full timeout-driven ring-walk *retry* (wait
//! `replication.receipt_timeout`, then re-resolve against a
//! liveness-updated view and re-forward to the substitute — §1, §4) is
//! deliberately not implemented here: issue #190 tracks the identical gap
//! in the P model, and closing it for real needs the `Scheduler`/`Clock`
//! ports plus daemon-level retry composition this module has no business
//! owning.
//!
//! # Zone awareness deferred
//!
//! §5.2's zone-awareness bullet (filtering the candidate walk so the RF set
//! spans `node.failure_domain`s) is not implemented here either: there is no
//! membership source at v0.1 that carries a *peer's* failure domain (the
//! registry that would is #53's), so [`MembershipView`] carries none. Adding
//! the field once a real source exists is additive, not a breaking change to
//! this module's shape.

use duckspout_types::{NodeId, PartitionId};

use crate::hrw::hrw_ranked;

/// The advisory candidate set HRW places over (§5.2) — module docs for how
/// it is sourced at v0.1.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembershipView {
    candidates: Vec<NodeId>,
}

impl MembershipView {
    /// A view over exactly `candidates`. Order is irrelevant — HRW's score
    /// is a pure function of the `(partition, node)` pair, never of
    /// position in this list (`hrw.rs`'s own module docs).
    #[must_use]
    pub fn new(candidates: Vec<NodeId>) -> Self {
        Self { candidates }
    }

    /// The candidate nodes, as HRW sees them.
    #[must_use]
    pub fn candidates(&self) -> &[NodeId] {
        &self.candidates
    }
}

/// Where one write for a partition routes, resolved once against a
/// [`MembershipView`] (§5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPlan {
    /// The ring owner: rank 0 of [`hrw_ranked`] — the node whose disk the
    /// partition's window converges onto before `SealPart` (§5.3's "Only
    /// owners drain").
    pub owner: NodeId,
    /// The full RF replica set, ranks `0..RF` of the SAME [`hrw_ranked`]
    /// call that produced `owner` — `owner` is always `replicas[0]`. Using
    /// one ranked list for both answers is what makes "merge locality by
    /// construction" (§5.3) hold: there is nowhere in this crate that
    /// "who owns this partition" and "who replicates it" can be computed
    /// against different memberships and disagree.
    pub replicas: Vec<NodeId>,
    /// Whether the node that resolved this plan (`self_node` passed to
    /// [`route_write`]) is `owner` itself.
    pub is_local_owner: bool,
}

impl RoutingPlan {
    /// The subset of [`RoutingPlan::replicas`] that is not `self_node` —
    /// the peer list [`crate::forward::forward_to_peers`] Forwards a batch
    /// to. That function also filters `self_node` out defensively on its
    /// own (ACPR #194 HIGH-1), so handing it the unfiltered `replicas` is
    /// equally safe; this accessor exists for callers that want the target
    /// list without going through `forward_to_peers` (e.g. status
    /// disclosure, tests).
    #[must_use]
    pub fn forward_targets(&self, self_node: &NodeId) -> Vec<NodeId> {
        self.replicas
            .iter()
            .filter(|node| *node != self_node)
            .cloned()
            .collect()
    }
}

/// Resolves the ownership-routing decision for `partition` (§5.2's
/// placement, §5.3's routing): the ring owner, the RF replica set, and
/// whether `self_node` is that owner.
///
/// `rf` is `cluster.rf` (§5.11); `0` is treated as `1` — a partition always
/// has at least an owner (§5.1: the RF floor is `cluster.rf` itself, never a
/// knob that can express "no owner exists").
///
/// Returns `None` only when `membership` has no candidates at all —
/// [`hrw_owner`](crate::hrw::hrw_owner)'s own empty-set case, propagated
/// through [`hrw_ranked`]. Unreachable past a real boot: a node's own id is
/// always included in its own [`MembershipView`]
/// (`duckspout-daemon/src/wiring.rs`'s composition).
#[must_use]
pub fn route_write(
    partition: &PartitionId,
    self_node: &NodeId,
    membership: &MembershipView,
    rf: u16,
) -> Option<RoutingPlan> {
    let ranked = hrw_ranked(partition, membership.candidates());
    let owner = *ranked.first()?;
    let take = usize::from(rf.max(1));
    let replicas: Vec<NodeId> = ranked.into_iter().take(take).cloned().collect();
    Some(RoutingPlan {
        owner: owner.clone(),
        is_local_owner: *owner == *self_node,
        replicas,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::hrw::hrw_owner;

    fn nodes(names: &[&str]) -> Vec<NodeId> {
        names.iter().map(|name| NodeId::new(*name)).collect()
    }

    fn view(names: &[&str]) -> MembershipView {
        MembershipView::new(nodes(names))
    }

    /// Empty membership: no owner exists, so [`route_write`] returns
    /// `None` rather than fabricating a plan — mirrors
    /// [`hrw_owner`](crate::hrw::hrw_owner)'s own contract for an empty
    /// node set.
    #[test]
    fn empty_membership_has_no_routing_plan() {
        assert_eq!(
            route_write(
                &PartitionId::new("p0"),
                &NodeId::new("a"),
                &MembershipView::default(),
                2
            ),
            None
        );
    }

    /// A write for a key this node owns is NOT forwarded to itself: `owner
    /// == self_node`, `is_local_owner` is true, and `forward_targets`
    /// excludes `self_node` from the replica set — the "a write for a key
    /// owned locally gets staged [locally], not forwarded to itself" half
    /// of the routing contract. Would catch a plan that fails to identify
    /// local ownership, or a `forward_targets` that forwards to self.
    #[test]
    fn owner_is_never_a_forward_target_of_its_own_plan() {
        let membership = view(&["a", "b", "c"]);
        for partition_name in ["p0", "p1", "p2", "p3", "p4"] {
            let partition = PartitionId::new(partition_name);
            let owner = hrw_owner(&partition, membership.candidates())
                .expect("nonempty")
                .clone();
            let plan = route_write(&partition, &owner, &membership, 2).expect("nonempty");
            assert_eq!(plan.owner, owner);
            assert!(plan.is_local_owner);
            assert!(!plan.forward_targets(&owner).contains(&owner));
        }
    }

    /// A write for a key this node does NOT own is routed toward the real
    /// owner: `is_local_owner` is false, and the owner is always present in
    /// `forward_targets` (the acceptor is not itself a member of its own
    /// replica set at this seq, so nothing filters the owner out) — "a write
    /// for a key not owned locally gets forwarded" half of the contract.
    #[test]
    fn non_owner_forwards_toward_the_real_owner() {
        let membership = view(&["a", "b", "c", "d"]);
        let mut exercised = false;
        for partition_name in ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"] {
            let partition = PartitionId::new(partition_name);
            let owner = hrw_owner(&partition, membership.candidates())
                .expect("nonempty")
                .clone();
            for candidate in membership.candidates() {
                if *candidate == owner {
                    continue;
                }
                exercised = true;
                let plan = route_write(&partition, candidate, &membership, 2).expect("nonempty");
                assert_eq!(plan.owner, owner);
                assert!(!plan.is_local_owner);
                assert!(plan.forward_targets(candidate).contains(&owner));
            }
        }
        assert!(exercised, "vacuous: no non-owner case was exercised");
    }

    /// The replica set is exactly the top-`RF` entries of [`hrw_ranked`] —
    /// the same ranked list [`RoutingPlan::owner`] is drawn from (rank 0).
    /// This is the "merge locality by construction" property (§5.3): owner
    /// and replica-set answers can never come from different computations.
    /// Would catch a `route_write` that re-derives the replica set some
    /// other way (e.g. re-sorting, or reading a stale/second membership
    /// snapshot).
    #[test]
    fn replica_set_matches_hrw_ranked_prefix_exactly() {
        let membership = view(&["a", "b", "c", "d", "e"]);
        for rf in 1u16..=5 {
            for partition_name in ["p0", "p1", "p2", "p3"] {
                let partition = PartitionId::new(partition_name);
                let expected: Vec<NodeId> =
                    crate::hrw::hrw_ranked(&partition, membership.candidates())
                        .into_iter()
                        .take(rf as usize)
                        .cloned()
                        .collect();
                let plan =
                    route_write(&partition, &NodeId::new("a"), &membership, rf).expect("nonempty");
                assert_eq!(plan.replicas, expected);
                assert_eq!(plan.replicas.first(), Some(&plan.owner));
            }
        }
    }

    /// `rf = 0` is treated as `1`: a partition always has at least an
    /// owner (§5.1's RF floor is `cluster.rf` itself, never expressible as
    /// "no owner"). Would catch a plan whose `replicas` is empty at
    /// `rf = 0`, which would make `owner` unreachable from `replicas`.
    #[test]
    fn rf_zero_still_yields_an_owner() {
        let membership = view(&["a", "b"]);
        let plan = route_write(&PartitionId::new("p0"), &NodeId::new("a"), &membership, 0)
            .expect("nonempty");
        assert_eq!(plan.replicas.len(), 1);
        assert_eq!(plan.replicas[0], plan.owner);
    }

    /// A single-node membership (v0.1's only fully-supported deployment,
    /// `cluster.seed_peers = []`) always routes locally, at any RF: the
    /// sole candidate is trivially both owner and the entire replica set,
    /// and `forward_targets` is always empty. This is the regression guard
    /// that ownership routing never turns a single-node deployment's
    /// existing behavior into a silent forward-to-nobody-reachable.
    #[test]
    fn single_node_membership_always_routes_locally() {
        let self_node = NodeId::new("solo");
        let membership = MembershipView::new(vec![self_node.clone()]);
        for rf in 1u16..=3 {
            for partition_name in ["p0", "p1", "p2"] {
                let plan = route_write(
                    &PartitionId::new(partition_name),
                    &self_node,
                    &membership,
                    rf,
                )
                .expect("nonempty");
                assert!(plan.is_local_owner);
                assert_eq!(plan.owner, self_node);
                assert!(plan.forward_targets(&self_node).is_empty());
            }
        }
    }

    proptest! {
        /// §8.5-style law: for ANY membership, ANY partition, and ANY RF,
        /// exactly one of two things holds — either `self_node` is the
        /// owner (and never its own forward target), or `self_node` is not
        /// the owner (and the owner IS a forward target, whenever
        /// `self_node` is itself a replica-set member so it has anything to
        /// forward at all). Checked against `hrw_owner`/`hrw_ranked`
        /// computed independently, not by re-reading `route_write`'s own
        /// output back at itself.
        #[test]
        fn route_write_agrees_with_hrw_for_arbitrary_inputs(
            names in proptest::collection::btree_set("[a-z]{1,6}", 1..8),
            partition_raw in "[a-z0-9/]{1,16}",
            rf in 1u16..6,
        ) {
            let candidates: Vec<NodeId> = names.iter().map(NodeId::new).collect();
            let membership = MembershipView::new(candidates.clone());
            let partition = PartitionId::new(partition_raw);
            let expected_owner = hrw_owner(&partition, &candidates).expect("nonempty").clone();
            let expected_replicas: Vec<NodeId> = crate::hrw::hrw_ranked(&partition, &candidates)
                .into_iter()
                .take(rf as usize)
                .cloned()
                .collect();

            for self_node in &candidates {
                let plan = route_write(&partition, self_node, &membership, rf).expect("nonempty");
                prop_assert_eq!(&plan.owner, &expected_owner);
                prop_assert_eq!(&plan.replicas, &expected_replicas);
                prop_assert_eq!(plan.is_local_owner, *self_node == expected_owner);
                prop_assert!(!plan.forward_targets(self_node).contains(self_node));
            }
        }
    }
}
