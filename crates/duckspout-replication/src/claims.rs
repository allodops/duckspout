//! `ClaimAdvertise` (§5.5): the node-local idempotency guard that decides
//! whether a successful accept/apply should publish an advisory registry
//! row at all. §5.5's own words: "the first apply for a partition the node
//! has no claim row for triggers the insert. No separate claim protocol, no
//! claim heartbeat distinct from the node heartbeat."
//!
//! [`ClaimTracker`] is exactly that "does this node already have a claim
//! row for this partition" bookkeeping — the same structural role
//! [`crate::fencing::FenceTable`] plays for incarnation fencing, and modeled
//! after the identical mechanism in `p/Replication/Node.p`'s `claims:
//! map[int, tRole]` field, guarding its `eWriteReq`/`eForward` handlers'
//! `if (!(key in claims))` checks (checker-validated against
//! `ClaimAdvertiseOnce`, `p/Replication/Spec.p`: "a node re-advertised a
//! claim it already holds" is asserted unreachable) — **for the SAME-role
//! case**; see [`ClaimTracker::should_advertise`]'s own doc comment for a
//! deliberate divergence from that spec on the role-CHANGE case.
//!
//! # Two steps, not one call (ACPR #197 MEDIUM-4)
//!
//! [`ClaimTracker::should_advertise`] (a pure, non-mutating query) and
//! [`ClaimTracker::mark_advertised`] (the mutation) are deliberately
//! separate calls, not the one `advertise_if_new` this module used to
//! expose. A single call that decided AND immediately marked a claim
//! advertised — before the caller had actually invoked
//! [`duckspout_types::Registry::advertise_claim`] and learned whether it
//! succeeded — meant a failed registry call left the tracker believing the
//! claim was already advertised forever: [`Registry::advertise_claim`]'s
//! own doc comment calls a failure "a harmless miss... until a future apply
//! re-triggers the advertisement," but nothing could ever re-trigger it
//! once the tracker had already committed to "advertised" on the strength
//! of a call that never actually happened. The caller's obligation is now:
//! check [`ClaimTracker::should_advertise`], make the registry call, and
//! only call [`ClaimTracker::mark_advertised`] once that call actually
//! succeeds — a failure leaves the tracker exactly as it was, so the next
//! apply's [`ClaimTracker::should_advertise`] check retries it, same as a
//! partition this tracker has never seen at all.
//!
//! [`Registry::advertise_claim`]: duckspout_types::Registry::advertise_claim
//!
//! # What this module does NOT do
//!
//! - **Call [`duckspout_types::Registry::advertise_claim`] itself.**
//!   [`ClaimTracker`] only decides WHETHER to advertise — the same
//!   separation [`duckspout_types::ReplicaLog`]'s own doc comment draws
//!   between "this port performs no guard evaluation of its own" and the
//!   caller's guards. The actual catalog write is the caller's, exactly as
//!   [`crate::peer_apply::apply_forward`] returns a
//!   [`crate::wire::ReceiptMessage`] for the caller to send rather than
//!   sending it itself.
//! - **Wire either side end to end.** §5.5's "first apply" trigger fires
//!   from BOTH the accepting node's own write (`OWNER`, `duckspout-accept`'s
//!   territory) and a replica's `PeerApply` (`REPLICA`, this crate's
//!   `peer_apply`). [`ClaimTracker`] itself is role-agnostic — its tests
//!   below exercise both [`duckspout_types::ClaimRole::Owner`] and
//!   [`duckspout_types::ClaimRole::Replica`] — but it lives here, in
//!   `duckspout-replication`, not in `duckspout-types`: `duckspout-accept`
//!   and `duckspout-replication` are both protocol crates and protocol×
//!   protocol edges are banned (ADR-0008), so `duckspout-accept` cannot
//!   reach this exact struct without either duplicating it or promoting it
//!   to `duckspout-types`. **This means only the REPLICA-side call path
//!   (`peer_apply`) can actually wire this tracker into a live apply flow
//!   today** — the OWNER-side call path (`duckspout-accept`'s own writes)
//!   cannot reach it at all from where it currently lives, not because the
//!   tracker's own logic is scoped to replicas (it manifestly is not, per
//!   its Owner-role tests). Whether to promote [`ClaimTracker`] (or just
//!   [`duckspout_types::ClaimRole`], the part `duckspout-accept` would
//!   actually need) to `duckspout-types` once that OWNER-side wiring is
//!   built is deliberately left open — the same daemon-composition
//!   follow-up [`crate::boot`]'s module docs name for `FenceBoot`; neither
//!   side is wired into a live apply flow in THIS crate's tests or exposed
//!   integration today.
//! - **Persist across a restart.** [`ClaimTracker`] is pure in-memory
//!   bookkeeping, matching `Node.p`'s own volatile `claims` field — a
//!   restarted node starts with an empty tracker and will re-advertise its
//!   still-held claims on their next apply, which the catalog's own
//!   upsert semantics (§5.5: an advisory row, never load-bearing, R-8)
//!   tolerate for free; re-advertising after a restart is not the
//!   `ClaimAdvertiseOnce` violation this module's own tests forbid within
//!   ONE tracker's lifetime.

use std::collections::HashMap;

use duckspout_types::{ClaimRole, PartitionId};

/// Node-local idempotency guard for `ClaimAdvertise` (§5.5, module docs):
/// one row per partition this node currently holds a claim for. A second
/// apply for a partition already tracked here — under the SAME role — is a
/// no-op; [`ClaimTracker::should_advertise`]'s own doc comment covers the
/// ROLE-CHANGE case, which §5.5 itself never describes explicitly (a node's
/// role for a partition changing in place).
#[derive(Debug, Clone, Default)]
pub struct ClaimTracker {
    claims: HashMap<PartitionId, ClaimRole>,
}

impl ClaimTracker {
    /// An empty tracker: no partition has an on-file claim yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a successful apply for `partition` under `role` should be
    /// advertised to the registry — `true` exactly the first time this
    /// tracker sees `partition`, or the first time it sees `partition`
    /// under a DIFFERENT role than it last held (a takeover promoting a
    /// former replica to owner, or the reverse). `false` for every other
    /// call — the SAME role repeated.
    ///
    /// **Deliberate divergence from `p/Replication/Spec.p`'s
    /// `ClaimAdvertiseOnce`, disclosed honestly (ACPR #197 MEDIUM-3):**
    /// that checker-validated spec (and `Node.p`'s own guard, `if
    /// (!(fwd.key in claims))`) is role-AGNOSTIC — a key present in
    /// `claims` at all blocks re-advertisement, regardless of role, so the
    /// checker would flag a role-change re-advertisement like this one as
    /// a `ClaimAdvertiseOnce` violation. This is NOT this module
    /// "mirroring" that spec (an earlier revision of this comment claimed
    /// it was, which overclaimed): it is a proposed improvement over the P
    /// model, made explicitly here rather than silently. The reasoning: a
    /// stale `Replica` row left on file after this node becomes `Owner`
    /// (or the reverse) would misinform a query-path resolver reading the
    /// registry's advisory `claims` table (§5.5) — §5.5's own prose does
    /// not explicitly settle the role-change case either way, so this is
    /// this module's own judgment call, not something either the design
    /// doc or the P model mandates. Worth confirming against a future
    /// revision of `docs/design/replication.md` §5's prose if it is ever
    /// extended to address role changes explicitly.
    ///
    /// This is a pure query — it does not itself commit to "advertised";
    /// call [`ClaimTracker::mark_advertised`] only after the registry call
    /// this decision gates actually succeeds (module docs, ACPR #197
    /// MEDIUM-4).
    #[must_use]
    pub fn should_advertise(&self, partition: &PartitionId, role: ClaimRole) -> bool {
        !matches!(self.claims.get(partition), Some(existing) if *existing == role)
    }

    /// Commits `partition` as advertised under `role` — call this ONLY
    /// after the caller's own [`duckspout_types::Registry::advertise_claim`]
    /// call for this exact `(partition, role)` has actually succeeded
    /// (module docs, ACPR #197 MEDIUM-4). A failed registry call must never
    /// reach this method: leaving the tracker unmarked is what lets a
    /// future apply's [`ClaimTracker::should_advertise`] retry it.
    pub fn mark_advertised(&mut self, partition: PartitionId, role: ClaimRole) {
        self.claims.insert(partition, role);
    }

    /// The role this tracker currently has on file for `partition`, `None`
    /// if no claim has ever been marked advertised for it through this
    /// tracker.
    #[must_use]
    pub fn current_role(&self, partition: &PartitionId) -> Option<ClaimRole> {
        self.claims.get(partition).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first apply for a partition this tracker has never seen triggers
    /// an advertisement — §5.5's own trigger condition, "the first apply for
    /// a partition the node has no claim row for."
    #[test]
    fn the_first_apply_for_a_partition_advertises() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");
        assert!(tracker.should_advertise(&p0, ClaimRole::Owner));
        tracker.mark_advertised(p0.clone(), ClaimRole::Owner);
        assert_eq!(tracker.current_role(&p0), Some(ClaimRole::Owner));
    }

    /// A second apply for the SAME partition under the SAME role never
    /// re-advertises — `ClaimAdvertiseOnce`'s exact property
    /// (`p/Replication/Spec.p`): "a node re-advertised a claim it already
    /// holds" is a violation. Would catch a tracker keyed on nothing (always
    /// returns `true`) or one that forgets what it already recorded.
    #[test]
    fn a_repeat_apply_under_the_same_role_never_re_advertises() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");
        assert!(tracker.should_advertise(&p0, ClaimRole::Replica));
        tracker.mark_advertised(p0.clone(), ClaimRole::Replica);
        assert!(!tracker.should_advertise(&p0, ClaimRole::Replica));
        assert!(!tracker.should_advertise(&p0, ClaimRole::Replica));
    }

    /// Claim tracking is scoped per partition: a first apply for a
    /// DIFFERENT partition advertises independently of another partition's
    /// already-on-file claim. Would catch a tracker collapsing every
    /// partition into one global "have I ever advertised anything" flag.
    #[test]
    fn tracking_is_scoped_per_partition() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");
        let p1 = PartitionId::new("p1");
        assert!(tracker.should_advertise(&p0, ClaimRole::Owner));
        tracker.mark_advertised(p0, ClaimRole::Owner);
        assert!(tracker.should_advertise(&p1, ClaimRole::Owner));
    }

    /// A role CHANGE for an already-tracked partition re-advertises — the
    /// deliberate divergence from `ClaimAdvertiseOnce`'s role-agnostic P
    /// model documented on [`ClaimTracker::should_advertise`]. Would catch a
    /// tracker that silently swallows a legitimate takeover promotion
    /// because the partition key alone already matched.
    #[test]
    fn a_role_change_for_an_already_tracked_partition_re_advertises() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");
        assert!(tracker.should_advertise(&p0, ClaimRole::Replica));
        tracker.mark_advertised(p0.clone(), ClaimRole::Replica);

        assert!(
            tracker.should_advertise(&p0, ClaimRole::Owner),
            "a promotion from replica to owner must re-advertise, not stay silent"
        );
        tracker.mark_advertised(p0.clone(), ClaimRole::Owner);
        assert_eq!(tracker.current_role(&p0), Some(ClaimRole::Owner));
    }

    /// ACPR #197 MEDIUM-4 scratch-repro re-verification: a registry call
    /// that FAILS must never leave the tracker believing the claim was
    /// advertised — the caller simply does not call
    /// [`ClaimTracker::mark_advertised`], and the next apply's
    /// [`ClaimTracker::should_advertise`] retries exactly as if nothing had
    /// been attempted. Before the two-step split, a single
    /// `advertise_if_new` call committed to "advertised" the moment it was
    /// called, regardless of whether the caller's subsequent registry call
    /// ever succeeded — a failure was then unrecoverable for the tracker's
    /// entire lifetime.
    #[test]
    fn a_failed_registry_call_leaves_the_claim_retryable() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");

        // First attempt: should_advertise says yes, but the caller's
        // registry call (not modeled here) fails, so mark_advertised is
        // never called.
        assert!(tracker.should_advertise(&p0, ClaimRole::Owner));

        // A later apply for the SAME (partition, role) must still see it as
        // needing advertisement -- nothing was committed.
        assert!(
            tracker.should_advertise(&p0, ClaimRole::Owner),
            "a claim whose registry call failed must remain retryable, not \
             silently treated as already advertised"
        );
        assert_eq!(
            tracker.current_role(&p0),
            None,
            "an unconfirmed advertisement must not appear as the tracker's \
             current-role record"
        );

        // The retry succeeds this time, and the caller confirms it.
        tracker.mark_advertised(p0.clone(), ClaimRole::Owner);
        assert!(!tracker.should_advertise(&p0, ClaimRole::Owner));
        assert_eq!(tracker.current_role(&p0), Some(ClaimRole::Owner));
    }
}
