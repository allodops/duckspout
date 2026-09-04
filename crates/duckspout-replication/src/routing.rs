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
//! # The ring walk is not separate machinery — for the membership-shrink case
//!
//! §5.6 step 2 describes dead-node substitution as "the HRW walk over the
//! membership view minus the dead node." That specific case is not new
//! logic to build here: it falls out for free from [`hrw_ranked`] run over
//! whatever candidate set the caller supplies. A liveness-aware caller
//! (post-#53, once a heartbeat/registry feed exists) simply constructs a
//! [`MembershipView`] with the dead node already excluded, and
//! [`route_write`] "ring-walks" automatically — the missing piece #53 adds
//! is the INPUT (who is live), not new ring-walk logic in this module.
//!
//! What is genuinely NOT proven to work today, despite the math being sound
//! (ACPR #196 LOW-6 — this claim was previously overstated as unqualified):
//! §4's immediate walk-down-on-refusal case (a live peer refuses a NEW
//! range at its overload threshold and the origin must retry against the
//! *next* candidate) needs the ranked TAIL past the top-RF prefix
//! [`RoutingPlan`] exposes — no caller anywhere can reach that tail through
//! [`RoutingPlan`] today; [`hrw_ranked`] itself has it, but nothing wires it
//! through. No caller anywhere also filters dead/unresponsive nodes before
//! calling [`hrw_ranked`], and [`MembershipView`] is stored immutably with
//! no updater in the daemon's core state — so ring-walk substitution does
//! not functionally work yet regardless of this module's own correctness.
//! Full timeout-driven ring-walk *retry* (wait
//! `replication.receipt_timeout`, then re-resolve against a
//! liveness-updated view and re-forward to the substitute — §1, §4) is
//! deliberately not implemented here: issue #190 tracks the identical gap
//! in the P model, and closing it for real needs the `Scheduler`/`Clock`
//! ports plus daemon-level retry composition this module has no business
//! owning.
//!
//! # `forward_targets` is `RF − 1`, always (ACPR #196 HIGH-1)
//!
//! §5.1 is total-inclusive: "RF counts every durable copy of an acked
//! record, including the copy on the node that will own the drain." §5.3
//! extends that to the acceptor, not just the owner: "any node accepts any
//! write ... the RF set for a partition always includes the ring OWNER.
//! The acceptor Forwards the batch" — the accepting node's own locally
//! staged copy is a real, durable copy from the moment it fsyncs, whether
//! or not that node is the ring owner (it later "demotes to replica
//! standing" once the owner's receipt lands — a role flip, no bytes move).
//! So the accepting node's local commit is unconditionally the first of
//! `rf` copies, exactly as `receipt.rs`'s `client_ack_ready` already
//! encodes (`peers_needed = rf.saturating_sub(1)`): [`forward_targets`]
//! must hand back exactly `rf − 1` distinct peers for `receipt.rs`'s
//! `client_ack_ready` to reach `rf` total copies when they all receipt —
//! never `rf` peers on top of the local copy, which would over-forward to
//! `rf + 1` total copies whenever the accepting node is NOT one of the
//! top-`RF` ranked nodes (a non-owner, non-replica acceptor — the common
//! case once membership exceeds `RF`).
//!
//! [`RoutingPlan::replicas`] stays the top-`RF` ranked prefix (owner-
//! inclusive) — §5.6 step 4's `TakeoverDrain` and any future ring-
//! maintenance caller that needs the full RF set (or the ranked tail past
//! it) should read [`hrw_ranked`] directly rather than through
//! [`RoutingPlan`], since `RoutingPlan`/`forward_targets` is specifically
//! the write-routing/ack-path decision, not a general ring-membership
//! accessor. [`forward_targets`] derives its `RF − 1` from that same
//! `replicas` prefix (filtering `self_node` out where present, then taking
//! the first `rf − 1` of what remains) rather than re-deriving from
//! [`hrw_ranked`] itself, so owner/replica-set/forward-targets all still
//! come from the one ranked call `route_write` made — no second lookup that
//! could disagree.
//!
//! [`forward_targets`]: RoutingPlan::forward_targets
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
    /// A view over `candidates`, deduplicated to distinct node ids
    /// (first-occurrence order preserved, though order is otherwise
    /// irrelevant — HRW's score is a pure function of the `(partition,
    /// node)` pair, never of position in this list, `hrw.rs`'s own module
    /// docs). ACPR #196 MEDIUM-3: a duplicated candidate would otherwise let
    /// `route_write`'s top-`RF` prefix hold `RF` slots but only `RF − 1`
    /// DISTINCT holders — the same double-counting shape ACPR #194 HIGH-1
    /// found one layer up, at the receipt-counting layer, reproduced here at
    /// membership construction. Enforcing distinctness INSIDE this
    /// constructor (rather than only in a private wrapper one caller
    /// happens to use, as it previously was) means every caller gets the
    /// invariant by construction — this file's own tests build a
    /// [`MembershipView`] by hand, and #53's future registry-backed
    /// membership feed will too; neither has to remember to dedup itself.
    #[must_use]
    pub fn new(candidates: Vec<NodeId>) -> Self {
        let mut seen = std::collections::HashSet::with_capacity(candidates.len());
        let candidates = candidates
            .into_iter()
            .filter(|node| seen.insert(node.clone()))
            .collect();
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
    /// The top-`RF` ranked prefix of the SAME [`hrw_ranked`] call that
    /// produced `owner` — `owner` is always `replicas[0]`. Using one ranked
    /// list for both answers is what makes "merge locality by construction"
    /// (§5.3) hold: there is nowhere in this crate that "who owns this
    /// partition" and "who replicates it" can be computed against different
    /// memberships and disagree.
    ///
    /// This is the ring-placement answer, not the ack-path forward list —
    /// use [`RoutingPlan::forward_targets`] for that (module docs, ACPR
    /// #196 HIGH-1). May hold FEWER than `rf` entries when the membership
    /// view has fewer than `rf` candidates (ACPR #196 LOW-7); check
    /// [`RoutingPlan::is_below_floor`] rather than assuming `replicas.len()
    /// == rf`.
    pub replicas: Vec<NodeId>,
    /// Whether the node that resolved this plan (`self_node` passed to
    /// [`route_write`]) is `owner` itself.
    pub is_local_owner: bool,
    /// The `rf` this plan was resolved at (`rf.max(1)` — §5.1's floor: a
    /// partition always has at least an owner). Carried on the plan itself
    /// (ACPR #196 LOW-7) so a caller can tell "RF satisfied" from "below the
    /// floor" via [`RoutingPlan::is_below_floor`] without separately
    /// threading `cluster.rf` through every call site.
    pub rf: u16,
}

impl RoutingPlan {
    /// The peer list [`crate::forward::forward_to_peers`] Forwards a batch
    /// to: exactly `rf − 1` distinct nodes, never `rf` (module docs, ACPR
    /// #196 HIGH-1) — the accepting node's own local copy is unconditionally
    /// the first of `rf` total copies (§5.1, §5.3), so only `rf − 1`
    /// *additional* peers are needed, matching `receipt.rs`'s
    /// `client_ack_ready` exactly (`peers_needed = rf.saturating_sub(1)`).
    ///
    /// Computed by filtering `self_node` out of [`RoutingPlan::replicas`]
    /// (the top-`RF` ranked prefix, in rank order) and taking the first
    /// `rf − 1` of what remains:
    /// - When `self_node` IS one of the top-`RF` nodes (it accepted a write
    ///   for a partition it itself replicates), filtering removes exactly
    ///   one entry, leaving exactly `rf − 1` — every other top-`RF` member.
    /// - When `self_node` is NOT one of the top-`RF` nodes (a non-owner,
    ///   non-replica acceptor forwarding onward), nothing is filtered, so
    ///   taking the first `rf − 1` of the `rf`-long prefix drops the
    ///   lowest-ranked (weakest) replica and keeps the rest — the owner
    ///   (rank 0) is always included either way, satisfying §5.3's "the RF
    ///   set always includes the ring OWNER."
    ///
    /// [`crate::forward::forward_to_peers`] also filters `self_node` out of
    /// whatever list it is handed, defensively (ACPR #194 HIGH-1) — that
    /// guard stays as defense-in-depth against a stale/buggy membership
    /// view, but is no longer this function's own arithmetic bug.
    #[must_use]
    pub fn forward_targets(&self, self_node: &NodeId) -> Vec<NodeId> {
        let needed = usize::from(self.rf.saturating_sub(1));
        self.replicas
            .iter()
            .filter(|node| *node != self_node)
            .take(needed)
            .cloned()
            .collect()
    }

    /// Whether this plan's actual replica coverage is below the configured
    /// `rf` floor — fewer live candidates existed than `rf` requires (§5.1's
    /// "stops promising further durability" ladder; ACPR #196 LOW-7). A
    /// caller cannot tell this from `replicas.len()` alone without also
    /// knowing `rf`, which is why both are carried on the same plan.
    #[must_use]
    pub fn is_below_floor(&self) -> bool {
        self.replicas.len() < usize::from(self.rf)
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
    let rf = rf.max(1);
    let take = usize::from(rf);
    let replicas: Vec<NodeId> = ranked.into_iter().take(take).cloned().collect();
    Some(RoutingPlan {
        owner: owner.clone(),
        is_local_owner: *owner == *self_node,
        rf,
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

    /// ACPR #196 MEDIUM-3 scratch-repro re-verification: a duplicated
    /// candidate is folded to one distinct entry inside `MembershipView::new`
    /// itself — every caller gets the distinctness invariant by
    /// construction, not just the one that happened to dedup first. Before
    /// this fix, constructing a `MembershipView` with a duplicated node
    /// produced a `RoutingPlan` with `replicas.len() == rf` but only
    /// `rf - 1` DISTINCT holders.
    #[test]
    fn membership_view_dedups_a_repeated_candidate_on_construction() {
        let a = NodeId::new("a");
        let membership = MembershipView::new(vec![a.clone(), NodeId::new("b"), a.clone()]);
        assert_eq!(membership.candidates(), &[a, NodeId::new("b")]);

        let plan = route_write(&PartitionId::new("p0"), &NodeId::new("b"), &membership, 2)
            .expect("nonempty");
        let distinct: std::collections::HashSet<_> = plan.replicas.iter().collect();
        assert_eq!(
            distinct.len(),
            plan.replicas.len(),
            "replicas must never repeat a node — MembershipView::new's own dedup guarantees this"
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

    /// ACPR #196 HIGH-1 scratch-repro re-verification: when the accepting
    /// node is OUTSIDE the top-`RF` ranked prefix (a non-owner, non-replica
    /// acceptor — the reviewer's own scratch topology hit this on 72% of
    /// forwarded partitions), `forward_targets` must still yield exactly
    /// `rf - 1` peers, never the full `rf` — which, composed with
    /// `receipt.rs`'s `client_ack_ready`, would durably over-replicate to
    /// `rf + 1` total copies (the acceptor's own local stage, plus a
    /// full-`rf` forward fan-out). Before this fix, `forward_targets`
    /// filtered `self_node` out of `replicas` and returned whatever was
    /// left: for an outside-top-RF acceptor, `self_node` was never IN
    /// `replicas` to begin with, so nothing was filtered and the full
    /// `rf`-long prefix leaked through unchanged.
    #[test]
    fn forward_targets_is_rf_minus_one_even_when_the_acceptor_is_outside_the_top_rf() {
        let membership = view(&["a", "b", "c", "d", "e", "f"]);
        let rf: u16 = 3;
        let mut exercised = false;
        for partition_name in ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"] {
            let partition = PartitionId::new(partition_name);
            let ranked = crate::hrw::hrw_ranked(&partition, membership.candidates());
            let Some(outside) = ranked.get(rf as usize..).and_then(|tail| tail.first()) else {
                continue;
            };
            let outside = (*outside).clone();
            exercised = true;

            let plan = route_write(&partition, &outside, &membership, rf).expect("nonempty");
            assert!(
                !plan.is_local_owner,
                "{partition}: an outside-top-RF acceptor is never the owner"
            );
            assert_eq!(
                plan.replicas.len(),
                rf as usize,
                "{partition}: 6 candidates always satisfy RF=3 in full"
            );

            let targets = plan.forward_targets(&outside);
            assert_eq!(
                targets.len(),
                (rf - 1) as usize,
                "{partition}: forward_targets must be exactly rf - 1, never the full rf \
                 (ACPR #196 HIGH-1: the bug would silently produce rf + 1 total durable copies)"
            );
            assert!(!targets.contains(&outside));
            assert!(
                targets.contains(&plan.owner),
                "{partition}: the forward set must still include the ring owner (§5.3)"
            );
        }
        assert!(
            exercised,
            "vacuous: no outside-top-RF acceptor was exercised"
        );
    }

    /// [`RoutingPlan::rf`] and [`RoutingPlan::is_below_floor`] (ACPR #196
    /// LOW-7): a membership with fewer candidates than `rf` requires is
    /// disclosed as below-floor, distinguishably from a satisfied RF — a
    /// caller reading `replicas.len()` alone cannot tell "RF=1 satisfied at
    /// 1" from "RF=3 short by 2" without also knowing `rf`.
    #[test]
    fn is_below_floor_distinguishes_satisfied_rf_from_a_short_candidate_set() {
        let membership = view(&["a", "b"]);
        let satisfied = route_write(&PartitionId::new("p0"), &NodeId::new("a"), &membership, 2)
            .expect("nonempty");
        assert_eq!(satisfied.rf, 2);
        assert_eq!(satisfied.replicas.len(), 2);
        assert!(!satisfied.is_below_floor());

        let short = route_write(&PartitionId::new("p0"), &NodeId::new("a"), &membership, 5)
            .expect("nonempty");
        assert_eq!(short.rf, 5);
        assert_eq!(short.replicas.len(), 2, "only 2 candidates exist");
        assert!(short.is_below_floor());
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
                prop_assert_eq!(plan.rf, rf);
                let targets = plan.forward_targets(self_node);
                prop_assert!(!targets.contains(self_node));
                // ACPR #196 HIGH-1's own invariant, checked generally: when
                // RF is actually satisfied (not below floor), forwarding
                // plus the acceptor's own local copy totals exactly `rf` —
                // never `rf + 1`.
                if !plan.is_below_floor() {
                    prop_assert_eq!(targets.len(), usize::from(rf) - 1);
                }
            }
        }
    }
}
