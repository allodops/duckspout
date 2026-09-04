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
//! claim it already holds" is asserted unreachable).
//!
//! # What this module does NOT do
//!
//! - **Call [`duckspout_types::Registry::advertise_claim`] itself.**
//!   [`ClaimTracker::advertise_if_new`] only decides WHETHER to advertise —
//!   the same separation [`duckspout_types::ReplicaLog`]'s own doc comment
//!   draws between "this port performs no guard evaluation of its own" and
//!   the caller's guards. The actual catalog write is the caller's, exactly
//!   as [`crate::peer_apply::apply_forward`] returns a
//!   [`crate::wire::ReceiptMessage`] for the caller to send rather than
//!   sending it itself.
//! - **Wire the OWNER side.** §5.5's "first apply" trigger fires from BOTH
//!   the accepting node's own write (`OWNER`, `duckspout-accept`'s
//!   territory) and a replica's `PeerApply` (`REPLICA`, this crate's
//!   `peer_apply`). [`ClaimTracker`] lives here, in `duckspout-types`'
//!   sibling protocol crate `duckspout-replication`, because `duckspout-accept`
//!   and `duckspout-replication` are both protocol crates and protocol×
//!   protocol edges are banned (ADR-0008) — `duckspout-accept` wiring an
//!   OWNER-side claim through this exact struct is not possible without
//!   duplicating it or promoting it to `duckspout-types`. This module
//!   therefore covers the REPLICA side only; OWNER-side wiring (and the
//!   question of whether the tracker needs to move to `duckspout-types` to
//!   be shared) is deliberately deferred to the same daemon-composition
//!   follow-up [`crate::boot`]'s module docs name for `FenceBoot` — neither
//!   is implemented in THIS crate's tests or exposed integration today.
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
/// no-op; module docs describe the ROLE-CHANGE case explicitly, since §5.5
/// itself never describes a node's role for a partition changing in place.
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

    /// Records a successful apply for `partition` under `role` and returns
    /// `Some(role)` — the claim to advertise — exactly the first time this
    /// tracker sees `partition`, or the first time it sees `partition` under
    /// a DIFFERENT role than it last held (a takeover promoting a former
    /// replica to owner, or the reverse — §5.5 does not model this
    /// explicitly, but a stale `Replica` row left after this node becomes
    /// `Owner` would misinform a query-path resolver, so a role change
    /// re-advertises rather than staying silently no-op'd by the
    /// partition-only key). Every other call — the SAME role repeated —
    /// returns `None`: `ClaimAdvertiseOnce`'s exact yardstick
    /// (`p/Replication/Spec.p`), "a node re-advertised a claim it already
    /// holds."
    pub fn advertise_if_new(
        &mut self,
        partition: PartitionId,
        role: ClaimRole,
    ) -> Option<ClaimRole> {
        match self.claims.get(&partition) {
            Some(existing) if *existing == role => None,
            _ => {
                self.claims.insert(partition, role);
                Some(role)
            }
        }
    }

    /// The role this tracker currently has on file for `partition`, `None`
    /// if no claim has ever been advertised for it through this tracker.
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
        assert_eq!(
            tracker.advertise_if_new(p0.clone(), ClaimRole::Owner),
            Some(ClaimRole::Owner)
        );
        assert_eq!(tracker.current_role(&p0), Some(ClaimRole::Owner));
    }

    /// A second apply for the SAME partition under the SAME role never
    /// re-advertises — `ClaimAdvertiseOnce`'s exact property
    /// (`p/Replication/Spec.p`): "a node re-advertised a claim it already
    /// holds" is a violation. Would catch a tracker keyed on nothing (always
    /// returns `Some`) or one that forgets what it already recorded.
    #[test]
    fn a_repeat_apply_under_the_same_role_never_re_advertises() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");
        assert_eq!(
            tracker.advertise_if_new(p0.clone(), ClaimRole::Replica),
            Some(ClaimRole::Replica)
        );
        assert_eq!(
            tracker.advertise_if_new(p0.clone(), ClaimRole::Replica),
            None
        );
        assert_eq!(tracker.advertise_if_new(p0, ClaimRole::Replica), None);
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
        assert_eq!(
            tracker.advertise_if_new(p0, ClaimRole::Owner),
            Some(ClaimRole::Owner)
        );
        assert_eq!(
            tracker.advertise_if_new(p1, ClaimRole::Owner),
            Some(ClaimRole::Owner)
        );
    }

    /// A role CHANGE for an already-tracked partition re-advertises — module
    /// docs' own reasoning: a stale row under the old role would misinform a
    /// query-path resolver. Would catch a tracker that silently swallows a
    /// legitimate takeover promotion because the partition key alone already
    /// matched.
    #[test]
    fn a_role_change_for_an_already_tracked_partition_re_advertises() {
        let mut tracker = ClaimTracker::new();
        let p0 = PartitionId::new("p0");
        assert_eq!(
            tracker.advertise_if_new(p0.clone(), ClaimRole::Replica),
            Some(ClaimRole::Replica)
        );
        assert_eq!(
            tracker.advertise_if_new(p0.clone(), ClaimRole::Owner),
            Some(ClaimRole::Owner),
            "a promotion from replica to owner must re-advertise, not stay silent"
        );
        assert_eq!(tracker.current_role(&p0), Some(ClaimRole::Owner));
    }
}
