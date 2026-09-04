//! `TakeoverDrain`, ownership-transition half (§5.6 steps 2 and 4, issue
//! #54): resolving who the new owner of a dead node's partition is, and
//! whether a caller should raise [`duckspout_types::TakeoverDrainSignal`]
//! for it.
//!
//! # What this module resolves (§5.6 step 2)
//!
//! "Write reroute. The HRW walk over the membership view minus the dead
//! node yields a new owner; acceptors forward there. The new owner was, by
//! the walk's construction, almost always already in the RF set — it holds
//! the partition's receipted prefix and begins accepting with no state
//! transfer." [`resolve_takeover`] is exactly that walk: it is not new ring
//! logic (`crate::routing`'s own module docs already say as much — "the
//! missing piece is the INPUT \[a membership view with the dead node
//! excluded\], not new ring-walk logic") — this module's job is computing
//! BOTH the old and new [`RoutingPlan`] for the same partition and deriving
//! the three questions a caller actually needs answered:
//!
//! - Did the dead node's removal change who owns this partition at all
//!   ([`TakeoverDecision::is_genuine_takeover`])? Removing an ordinary
//!   (non-owner) replica from the candidate set can still shift the RF
//!   set's *tail*, but §5.6 is specifically about "the death of a
//!   partition's OWNER" — a replica-only death is an availability event the
//!   ring-walk substitution (`crate::routing` module docs) already covers
//!   without any drain-side consequence (§5.3: "Only owners drain").
//! - Is the LOCAL node the new owner ([`TakeoverDecision::self_is_new_owner`])?
//!   Only the new owner itself ever raises a
//!   [`duckspout_types::TakeoverDrainSignal`] — a node only ever learns of
//!   its own promotion (that type's own doc comment).
//! - Did the local node already hold a replica copy before the death
//!   ([`TakeoverDecision::self_was_replica_before`])? This is the "almost
//!   always" the design doc names — exposed so a caller (and this module's
//!   own tests) can directly observe it rather than assume it.
//!
//! # What this module does NOT resolve
//!
//! - **Death detection itself.** §5.6 step 1's Heartbeat-TTL machinery (and
//!   the takeover-suppression window of §5.10) is a separate concern this
//!   issue explicitly does not need to build; [`resolve_takeover`] takes
//!   `dead_node` as an already-decided input, exactly as
//!   `crate::routing::route_write` takes a [`MembershipView`] as an
//!   already-decided input rather than computing liveness itself.
//! - **Actually draining anything.** [`duckspout_types::TakeoverDrainTrigger`]
//!   is the seam a caller uses to hand this module's decision to the drain
//!   side (`duckspout-drain`, via daemon composition) — this crate cannot
//!   depend on `duckspout-drain` directly (ADR-0008, both protocol crates).
//! - **The churn-boundary supplement part's actual sealing** (§5.6 step 5).
//!   [`compute_residue`] is the pure coverage-arithmetic half — "validates
//!   disjoint per-(origin, seq) coverage against the winner's manifest" is
//!   ALREADY enforced generically by
//!   `duckspout_watermark::WatermarkLedger::record_commit`'s
//!   `CoverageOverlap` guard (any manifest's coverage, `PartKind::Supplement`
//!   included, is checked against everything already committed) — but
//!   actually sealing a residue-restricted subset of a window's staged rows
//!   needs `duckspout_types::SealSurface`/`SealRequest` machinery this
//!   module has no access to and that does not exist yet (`SealRequest`
//!   today seals a whole window, never an origin/seq-restricted subset).
//!   This PR names the follow-up for that `duckspout-drain`-side work
//!   explicitly (see its own description) rather than inventing an
//!   untested stub here.
//!
//! # Cross-check against the P model
//!
//! `p/Replication/Node.p`'s `eForward` handler fires its inline
//! `eTakeoverDrain` the moment a replica durably applies a genuinely
//! next-in-line Forward AND `peerDead && !degraded && !(fwd.key in
//! committed)` — i.e. gated on (a) the peer being known dead, (b) this
//! node's own boot/fencing state permitting ownership actions
//! (`crate::boot::BootOutcome::permits_ownership_actions`, the Rust analog
//! of `!degraded`), and (c) not having already taken this exact thing over
//! (`crate::claims::ClaimTracker`'s idempotency pattern, mirrored here by
//! [`TakeoverTracker`]). [`TakeoverTracker::should_trigger`] is exactly that
//! three-way gate, generalized from the P model's per-key granularity to
//! this crate's per-partition granularity (the real system's drain
//! machinery operates on windows within a partition, not individual
//! records — `docs/design/p-tla-correspondence.md`'s own convention for
//! where the real system's granularity legitimately differs from the P
//! model's abstraction).

use std::collections::HashMap;

use duckspout_types::{NodeId, OriginSeqRange, PartitionId, TakeoverDrainSignal};

use crate::routing::{MembershipView, RoutingPlan, route_write};

/// The resolved ownership-transition decision for one `(partition,
/// dead_node)` pair (§5.6 step 2). Module docs explain each field's role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeoverDecision {
    /// The partition whose ownership was (re-)resolved.
    pub partition: PartitionId,
    /// The node whose death triggered this resolution.
    pub dead_owner: NodeId,
    /// Who owned the partition BEFORE the dead node was excluded — the
    /// question [`TakeoverDecision::is_genuine_takeover`] answers by
    /// comparing this to `dead_owner`.
    pub old_owner: NodeId,
    /// The freshly resolved plan over the membership view with `dead_owner`
    /// excluded (§5.6 step 2's "HRW walk over the membership view minus the
    /// dead node") — `new_plan.owner` is who Forwards should now route to.
    pub new_plan: RoutingPlan,
    /// Whether the LOCAL node (`self_node` passed to [`resolve_takeover`])
    /// is the new owner.
    pub self_is_new_owner: bool,
    /// Whether the local node was already a member of the partition's RF
    /// replica set BEFORE the dead node was excluded — the "almost always"
    /// the design doc names: "the new owner was, by the walk's
    /// construction, almost always already in the RF set... begins
    /// accepting with no state transfer" (§5.6 step 2).
    pub self_was_replica_before: bool,
}

impl TakeoverDecision {
    /// Whether the dead node was genuinely this partition's OWNER before its
    /// removal — as opposed to an ordinary replica whose death does not, on
    /// its own, trigger `TakeoverDrain` (§5.3: "Only owners drain"; an
    /// ordinary replica's death is availability churn the ring-walk
    /// substitution already covers, `crate::routing`'s module docs).
    #[must_use]
    pub fn is_genuine_takeover(&self) -> bool {
        self.old_owner == self.dead_owner
    }

    /// The [`TakeoverDrainSignal`] this decision authorizes — only
    /// meaningful once a caller has confirmed
    /// [`TakeoverTracker::should_trigger`] (this method performs none of
    /// that gating itself, matching [`crate::claims::ClaimTracker`]'s own
    /// query/mutation split: computing the signal and deciding to raise it
    /// are separate steps).
    #[must_use]
    pub fn signal(&self) -> TakeoverDrainSignal {
        TakeoverDrainSignal {
            partition: self.partition.clone(),
            dead_owner: self.dead_owner.clone(),
            new_owner: self.new_plan.owner.clone(),
        }
    }
}

/// Resolves the ownership-transition decision for `partition` once
/// `dead_node` is excluded from `membership` (§5.6 step 2). `rf` is
/// `cluster.rf`, exactly as [`route_write`] takes it.
///
/// Returns `None` only when no plan can be resolved at all — either the
/// full `membership` is empty ([`route_write`]'s own empty-candidate case,
/// unreachable past a real boot per its own doc comment) or `dead_node` was
/// the sole candidate (removing it leaves nothing to route to at all: every
/// copy of the partition is gone, which is [`crate::takeover`]'s sibling
/// ceremony's territory — `duckspout_watermark::loss` — not
/// `TakeoverDrain`'s).
#[must_use]
pub fn resolve_takeover(
    partition: &PartitionId,
    dead_node: &NodeId,
    self_node: &NodeId,
    membership: &MembershipView,
    rf: u16,
) -> Option<TakeoverDecision> {
    let old_plan = route_write(partition, self_node, membership, rf)?;
    let survivors: Vec<NodeId> = membership
        .candidates()
        .iter()
        .filter(|node| *node != dead_node)
        .cloned()
        .collect();
    let new_membership = MembershipView::new(survivors);
    let new_plan = route_write(partition, self_node, &new_membership, rf)?;
    Some(TakeoverDecision {
        partition: partition.clone(),
        dead_owner: dead_node.clone(),
        old_owner: old_plan.owner,
        self_is_new_owner: new_plan.is_local_owner,
        self_was_replica_before: old_plan.replicas.contains(self_node),
        new_plan,
    })
}

/// Node-local idempotency guard for `TakeoverDrain` (§5.6 step 4): at most
/// one trigger per `(partition, dead_owner)` pair, mirroring
/// [`crate::claims::ClaimTracker`]'s structural role for `ClaimAdvertise`
/// and the P model's inline `!(fwd.key in committed)` check
/// (`p/Replication/Node.p`'s `eForward` handler; module docs above). Keyed
/// by `dead_owner` too, not just `partition`, so a LATER, DIFFERENT node's
/// death for the same partition (a cascading failure — this node was the
/// takeover winner once already, and now the ring walk excludes a second
/// dead node too) still triggers a fresh drain-trigger call rather than
/// being silently absorbed by the first takeover's record.
#[derive(Debug, Clone, Default)]
pub struct TakeoverTracker {
    triggered: HashMap<PartitionId, NodeId>,
}

impl TakeoverTracker {
    /// An empty tracker: no partition has ever had a takeover triggered
    /// through it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `decision` should raise a [`TakeoverDrainSignal`] right now:
    /// a genuine takeover (module docs,
    /// [`TakeoverDecision::is_genuine_takeover`]), the local node is the
    /// winner, `boot_permits_ownership` reports this node's own boot state
    /// allows ownership actions
    /// (`crate::boot::BootOutcome::permits_ownership_actions` — the Rust
    /// analog of the P model's `!degraded` conjunct), and this exact
    /// `(partition, dead_owner)` pair has not already been triggered
    /// through this tracker. A pure query — it does not itself commit to
    /// "triggered"; call [`TakeoverTracker::mark_triggered`] only after the
    /// caller's own [`duckspout_types::TakeoverDrainTrigger::trigger`] call
    /// actually succeeds, matching [`crate::claims::ClaimTracker`]'s own
    /// query/mutation split (a failed trigger call must leave this
    /// retryable, not permanently swallowed).
    #[must_use]
    pub fn should_trigger(
        &self,
        decision: &TakeoverDecision,
        boot_permits_ownership: bool,
    ) -> bool {
        decision.is_genuine_takeover()
            && decision.self_is_new_owner
            && boot_permits_ownership
            && !matches!(
                self.triggered.get(&decision.partition),
                Some(recorded) if *recorded == decision.dead_owner
            )
    }

    /// Commits `(partition, dead_owner)` as triggered — call this ONLY
    /// after the caller's own
    /// [`duckspout_types::TakeoverDrainTrigger::trigger`] call for this
    /// exact signal has actually succeeded (module docs).
    pub fn mark_triggered(&mut self, partition: PartitionId, dead_owner: NodeId) {
        self.triggered.insert(partition, dead_owner);
    }
}

/// The churn-boundary split (§5.6 step 5): the residue of `window_coverage`
/// left over once `winner_coverage` (the dead owner's already-committed
/// part, if any, for the SAME window) is subtracted out, per origin.
///
/// Pure interval arithmetic — general subtraction, not merely the common
/// "winner covers a prefix, residue is the tail" case the design doc
/// describes in prose, since a real `winner_coverage` could in principle
/// carry more than one disjoint sub-range per origin (multiple prior
/// supplement commits) and this function must not assume otherwise. Origins
/// present in `window_coverage` but absent from `winner_coverage` pass
/// through entirely as residue; origins present in `winner_coverage` but
/// absent from `window_coverage` contribute nothing (there is no local
/// coverage to subtract from).
///
/// The actual disjointness-against-the-winner's-manifest VALIDATION this
/// residue must satisfy inside the commit transaction is already enforced
/// generically by `duckspout_watermark::WatermarkLedger::record_commit`'s
/// `CoverageOverlap` guard — this function computes what a caller SHOULD
/// submit as a supplement part's coverage, it is not itself the guard.
#[must_use]
pub fn compute_residue(
    window_coverage: &[OriginSeqRange],
    winner_coverage: &[OriginSeqRange],
) -> Vec<OriginSeqRange> {
    let mut residue = Vec::new();
    for range in window_coverage {
        let mut cuts: Vec<(u64, u64)> = winner_coverage
            .iter()
            .filter(|winner| {
                winner.origin == range.origin
                    && winner.first_seq <= range.last_seq
                    && winner.last_seq >= range.first_seq
            })
            .map(|winner| {
                (
                    winner.first_seq.max(range.first_seq),
                    winner.last_seq.min(range.last_seq),
                )
            })
            .collect();
        cuts.sort_unstable();

        let mut cursor = range.first_seq;
        for (cut_first, cut_last) in cuts.drain(..) {
            if cut_first > cursor {
                residue.push(OriginSeqRange {
                    origin: range.origin.clone(),
                    first_seq: cursor,
                    last_seq: cut_first - 1,
                });
            }
            cursor = cursor.max(cut_last.saturating_add(1));
        }
        if cursor <= range.last_seq {
            residue.push(OriginSeqRange {
                origin: range.origin.clone(),
                first_seq: cursor,
                last_seq: range.last_seq,
            });
        }
    }
    residue
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(names: &[&str]) -> Vec<NodeId> {
        names.iter().map(|name| NodeId::new(*name)).collect()
    }

    fn view(names: &[&str]) -> MembershipView {
        MembershipView::new(nodes(names))
    }

    /// `TestTakeoverDrain`'s own topology (`p/Replication/TestDriver.p`):
    /// one owner, one replica. With the owner excluded, the sole survivor
    /// (the replica) is trivially the new owner at any RF, and it was
    /// already a replica before (RF >= 2 over a 2-candidate membership puts
    /// both in the replica set) — "begins accepting with no state
    /// transfer."
    #[test]
    fn the_sole_surviving_node_becomes_owner_with_no_state_transfer() {
        let membership = view(&["owner", "replica"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("replica"),
            &membership,
            2,
        )
        .expect("two candidates resolve");
        assert_eq!(decision.old_owner, NodeId::new("owner"));
        assert_eq!(decision.new_plan.owner, NodeId::new("replica"));
        assert!(decision.self_is_new_owner);
        assert!(decision.self_was_replica_before);
        assert!(decision.is_genuine_takeover());
    }

    /// A dead node that was NOT the owner (an ordinary replica) is not a
    /// "genuine takeover" even if removing it happens to change the ranked
    /// tail — §5.3's "Only owners drain" means a replica-only death has no
    /// `TakeoverDrain` consequence. Constructed by finding, for a fixed
    /// partition, the actual HRW owner among a candidate set and killing a
    /// DIFFERENT node instead.
    #[test]
    fn a_dead_replica_that_was_not_the_owner_is_not_a_genuine_takeover() {
        let membership = view(&["a", "b", "c"]);
        let partition = PartitionId::new("p0");
        let owner = crate::hrw::hrw_owner(&partition, membership.candidates())
            .expect("nonempty")
            .clone();
        let dead = membership
            .candidates()
            .iter()
            .find(|node| **node != owner)
            .expect("at least one non-owner candidate")
            .clone();
        let decision = resolve_takeover(&partition, &dead, &owner, &membership, 2)
            .expect("three candidates resolve");
        assert_eq!(decision.old_owner, owner);
        assert_ne!(decision.old_owner, dead);
        assert!(!decision.is_genuine_takeover());
    }

    /// Every candidate excluded but the sole survivor: `dead_node` was the
    /// only OTHER candidate, so [`resolve_takeover`] still resolves (the
    /// survivor is both old and new replica-set member, just not owner
    /// before).
    #[test]
    fn losing_every_candidate_but_one_still_resolves() {
        let membership = view(&["only-owner"]);
        // Nothing to remove -- dead_node isn't even a candidate; the walk
        // minus a non-member is a no-op, matching a stale liveness signal
        // (§5.2's advisory-view tolerance).
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("someone-else"),
            &NodeId::new("only-owner"),
            &membership,
            2,
        )
        .expect("single candidate still resolves");
        assert_eq!(decision.new_plan.owner, NodeId::new("only-owner"));
        assert!(!decision.is_genuine_takeover());
    }

    /// Removing the SOLE candidate (the dead node held every copy) leaves
    /// nothing to route to at all: `resolve_takeover` returns `None` rather
    /// than fabricating an owner — this is `DeclareLoss` territory
    /// (`duckspout_watermark::loss`), not `TakeoverDrain`'s.
    #[test]
    fn removing_the_sole_candidate_yields_no_decision() {
        let membership = view(&["only-owner"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("only-owner"),
            &NodeId::new("only-owner"),
            &membership,
            2,
        );
        assert_eq!(decision, None);
    }

    /// A node that was NOT already a replica of the partition before the
    /// death (e.g. it only just joined, or RF=1 meant it held nothing)
    /// still resolves correctly, with `self_was_replica_before` honestly
    /// `false` — the "almost always" the design doc names is a description
    /// of the common case, not an invariant this module enforces.
    #[test]
    fn a_new_owner_that_held_no_prior_replica_copy_is_disclosed_honestly() {
        // RF=1: the old plan's replica set is JUST the owner. A non-owner
        // candidate was never in that set.
        let membership = view(&["owner", "bystander"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("bystander"),
            &membership,
            1,
        )
        .expect("two candidates resolve");
        assert!(!decision.self_was_replica_before);
    }

    /// [`TakeoverTracker`]'s idempotency: the first genuine, self-owning,
    /// boot-permitted decision triggers; a repeat of the IDENTICAL
    /// `(partition, dead_owner)` decision does not.
    #[test]
    fn a_repeat_decision_for_the_same_dead_owner_does_not_retrigger() {
        let membership = view(&["owner", "replica"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("replica"),
            &membership,
            2,
        )
        .expect("resolves");

        let mut tracker = TakeoverTracker::new();
        assert!(tracker.should_trigger(&decision, true));
        tracker.mark_triggered(decision.partition.clone(), decision.dead_owner.clone());
        assert!(!tracker.should_trigger(&decision, true));
    }

    /// A SECOND, different node dying for the same partition re-triggers —
    /// cascading failures are not silently absorbed by the first takeover's
    /// record.
    #[test]
    fn a_different_dead_owner_for_the_same_partition_retriggers() {
        let membership = view(&["owner", "replica", "third"]);
        let first = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("replica"),
            &membership,
            3,
        )
        .expect("resolves");
        let mut tracker = TakeoverTracker::new();
        assert!(tracker.should_trigger(&first, true));
        tracker.mark_triggered(first.partition.clone(), first.dead_owner.clone());

        // A cascading second death: the membership now excludes "owner"
        // (already dead) and "replica" now dies too.
        let after_first = view(&["replica", "third"]);
        let second = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("replica"),
            &NodeId::new("third"),
            &after_first,
            2,
        )
        .expect("resolves");
        assert!(tracker.should_trigger(&second, true));
    }

    /// `TakeoverTracker::should_trigger` refuses to fire while this node's
    /// own boot state does not permit ownership actions — the Rust analog
    /// of the P model's `!degraded` conjunct
    /// (`crate::boot::BootOutcome::permits_ownership_actions`, module
    /// docs). Would catch a gate that ignores boot state entirely.
    #[test]
    fn a_degraded_or_waiting_boot_state_never_triggers() {
        let membership = view(&["owner", "replica"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("replica"),
            &membership,
            2,
        )
        .expect("resolves");
        let tracker = TakeoverTracker::new();
        assert!(!tracker.should_trigger(&decision, false));
    }

    /// A decision where the local node is NOT the new owner never triggers
    /// — only the winner itself ever raises a signal (a node only ever
    /// learns of its own promotion, `TakeoverDrainSignal`'s own doc
    /// comment).
    #[test]
    fn a_decision_where_self_is_not_the_new_owner_never_triggers() {
        let membership = view(&["owner", "replica", "bystander"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("bystander"),
            &membership,
            2,
        )
        .expect("resolves");
        assert!(!decision.self_is_new_owner);
        let tracker = TakeoverTracker::new();
        assert!(!tracker.should_trigger(&decision, true));
    }

    /// `TakeoverDecision::signal` maps the decision onto the wire shape a
    /// [`duckspout_types::TakeoverDrainTrigger`] call carries.
    #[test]
    fn signal_carries_the_partition_dead_and_new_owner() {
        let membership = view(&["owner", "replica"]);
        let decision = resolve_takeover(
            &PartitionId::new("p0"),
            &NodeId::new("owner"),
            &NodeId::new("replica"),
            &membership,
            2,
        )
        .expect("resolves");
        let signal = decision.signal();
        assert_eq!(signal.partition, PartitionId::new("p0"));
        assert_eq!(signal.dead_owner, NodeId::new("owner"));
        assert_eq!(signal.new_owner, NodeId::new("replica"));
    }

    // --- compute_residue (§5.6 step 5) ---

    fn range(origin: &str, first: u64, last: u64) -> OriginSeqRange {
        OriginSeqRange {
            origin: NodeId::new(origin),
            first_seq: first,
            last_seq: last,
        }
    }

    /// The common case the design doc describes in prose: the dead owner
    /// already committed a prefix; the residue is exactly the tail.
    #[test]
    fn residue_is_the_tail_past_a_committed_prefix() {
        let window = vec![range("o1", 1, 10)];
        let winner = vec![range("o1", 1, 6)];
        assert_eq!(compute_residue(&window, &winner), vec![range("o1", 7, 10)]);
    }

    /// No prior winner commit at all: the whole window's coverage is
    /// residue, unchanged.
    #[test]
    fn no_winner_coverage_means_the_whole_window_is_residue() {
        let window = vec![range("o1", 1, 10)];
        assert_eq!(compute_residue(&window, &[]), window);
    }

    /// The winner already covers the ENTIRE window: the residue is empty —
    /// nothing for the supplement to carry (this is the ordinary case where
    /// no supplement is needed at all, distinguishable from "residue exists"
    /// by an empty result).
    #[test]
    fn full_winner_coverage_leaves_no_residue() {
        let window = vec![range("o1", 1, 10)];
        let winner = vec![range("o1", 1, 10)];
        assert_eq!(compute_residue(&window, &winner), Vec::new());
    }

    /// A winner commit covering a MIDDLE chunk (not a clean prefix) splits
    /// the residue into two sub-ranges — general interval subtraction, not
    /// merely the prefix-only case.
    #[test]
    fn a_middle_winner_chunk_splits_the_residue_in_two() {
        let window = vec![range("o1", 1, 10)];
        let winner = vec![range("o1", 4, 6)];
        assert_eq!(
            compute_residue(&window, &winner),
            vec![range("o1", 1, 3), range("o1", 7, 10)]
        );
    }

    /// Multiple disjoint prior winner sub-ranges (more than one earlier
    /// supplement) are all subtracted correctly, in any input order.
    #[test]
    fn multiple_disjoint_winner_ranges_are_all_subtracted() {
        let window = vec![range("o1", 1, 20)];
        let winner = vec![range("o1", 15, 20), range("o1", 1, 3)];
        assert_eq!(compute_residue(&window, &winner), vec![range("o1", 4, 14)]);
    }

    /// Coverage is scoped per origin: a winner's coverage of a DIFFERENT
    /// origin never subtracts from this origin's residue.
    #[test]
    fn residue_is_scoped_per_origin() {
        let window = vec![range("o1", 1, 5), range("o2", 1, 5)];
        let winner = vec![range("o2", 1, 5)];
        assert_eq!(compute_residue(&window, &winner), vec![range("o1", 1, 5)]);
    }

    /// A winner range for an origin the window never mentions contributes
    /// nothing — there is no local coverage to subtract it from.
    #[test]
    fn winner_coverage_for_an_absent_origin_is_ignored() {
        let window = vec![range("o1", 1, 5)];
        let winner = vec![range("o2", 1, 100)];
        assert_eq!(compute_residue(&window, &winner), window);
    }
}
