//! Highest-random-weight (rendezvous) placement (§5, ADR-0004).
//!
//! For each `(partition, node)` pair a stable 64-bit score is computed; the
//! owner is the node with the maximum score. This yields HRW's
//! **minimal-disruption property**:
//!
//! - adding a node moves to it exactly the partitions it now wins, and moves
//!   nothing between pre-existing nodes;
//! - removing a node reassigns only the partitions it owned; every other
//!   partition keeps its owner.
//!
//! Both follow from scores being per-`(partition, node)` and independent of
//! the membership set: changing membership never changes any surviving
//! pair's score, so a partition's argmax changes only when its previous
//! maximum leaves or a new maximum joins.
//!
//! The hash is an in-crate FNV-1a 64 rather than `std`'s `DefaultHasher`:
//! placement must be identical across nodes, builds, and Rust releases, and
//! `DefaultHasher`'s algorithm is explicitly unspecified. Cryptographic
//! strength is not required — placement is a latency concern, never a
//! correctness concern (§9.1: any node accepts and forwards to the owner).

use duckspout_types::{NodeId, PartitionId};

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64 over `bytes`, continuing from `state`.
fn fnv1a(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(FNV_PRIME);
    }
    state
}

/// The stable HRW score of a `(partition, node)` pair. The `0xff` separator
/// domain-separates the two ids (no UTF-8 byte is `0xff`), so
/// `("ab", "c")` and `("a", "bc")` never collide structurally.
#[must_use]
pub fn hrw_score(partition: &PartitionId, node: &NodeId) -> u64 {
    let state = fnv1a(FNV_OFFSET_BASIS, partition.as_str().as_bytes());
    let state = fnv1a(state, &[0xff]);
    fnv1a(state, node.as_str().as_bytes())
}

/// The partition's owner: the node with the highest HRW score. Ties (2^-64
/// per pair) break toward the lexicographically greatest node id, so the
/// function stays a pure function of its inputs. Returns `None` only for an
/// empty node set.
#[must_use]
pub fn hrw_owner<'a>(partition: &PartitionId, nodes: &'a [NodeId]) -> Option<&'a NodeId> {
    nodes
        .iter()
        .max_by_key(|node| (hrw_score(partition, node), *node))
}

/// All nodes ranked by descending HRW score for `partition`: index 0 is the
/// owner, indices `1..RF` are the replica set, and the tail is the ring
/// walk-down order used when a peer refuses new ranges (§4.3, §5).
#[must_use]
pub fn hrw_ranked<'a>(partition: &PartitionId, nodes: &'a [NodeId]) -> Vec<&'a NodeId> {
    let mut ranked: Vec<&NodeId> = nodes.iter().collect();
    ranked.sort_by_key(|node| std::cmp::Reverse((hrw_score(partition, node), *node)));
    ranked
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn nodes(names: &[&str]) -> Vec<NodeId> {
        names.iter().map(|name| NodeId::new(*name)).collect()
    }

    fn partitions(count: usize) -> Vec<PartitionId> {
        (0..count)
            .map(|i| PartitionId::new(format!("tenant-{}/dataset-{}", i % 7, i)))
            .collect()
    }

    #[test]
    fn owner_is_rank_zero() {
        let members = nodes(&["a", "b", "c"]);
        for partition in partitions(50) {
            assert_eq!(
                hrw_owner(&partition, &members),
                hrw_ranked(&partition, &members).first().copied()
            );
        }
    }

    #[test]
    fn adding_a_node_moves_partitions_only_to_it() {
        let before = nodes(&["a", "b", "c"]);
        let after = nodes(&["a", "b", "c", "d"]);
        let added = NodeId::new("d");
        let mut moved = 0_usize;
        for partition in partitions(200) {
            let old = hrw_owner(&partition, &before).expect("nonempty");
            let new = hrw_owner(&partition, &after).expect("nonempty");
            if new != old {
                assert_eq!(*new, added, "{partition}: moved between survivors");
                moved += 1;
            }
        }
        assert!(moved > 0, "vacuous: the added node won nothing");
    }

    #[test]
    fn removing_a_node_moves_only_its_partitions() {
        let before = nodes(&["a", "b", "c"]);
        let after = nodes(&["a", "b"]);
        let removed = NodeId::new("c");
        let mut moved = 0_usize;
        for partition in partitions(200) {
            let old = hrw_owner(&partition, &before).expect("nonempty");
            let new = hrw_owner(&partition, &after).expect("nonempty");
            if *old == removed {
                moved += 1;
            } else {
                assert_eq!(new, old, "{partition}: a survivor's partition moved");
            }
        }
        assert!(moved > 0, "vacuous: the removed node owned nothing");
    }

    #[test]
    fn empty_membership_has_no_owner() {
        assert_eq!(hrw_owner(&PartitionId::new("p"), &[]), None);
    }

    proptest! {
        /// The minimal-disruption law (§8.5) over arbitrary memberships:
        /// adding one node never moves a partition between pre-existing
        /// nodes.
        #[test]
        fn minimal_disruption_on_arbitrary_add(
            names in proptest::collection::btree_set("[a-z]{1,8}", 1..12),
            new_name in "[A-Z][a-z]{1,8}",
            parts in proptest::collection::vec("[a-z0-9/]{1,16}", 1..64),
        ) {
            let before: Vec<NodeId> = names.iter().map(NodeId::new).collect();
            let mut after = before.clone();
            after.push(NodeId::new(new_name.clone()));
            for raw in parts {
                let partition = PartitionId::new(raw);
                let old = hrw_owner(&partition, &before).expect("nonempty");
                let new = hrw_owner(&partition, &after).expect("nonempty");
                prop_assert!(new == old || new.as_str() == new_name);
            }
        }

        /// Minimal disruption on removal (§8.5's "removing one node
        /// reassigns only the partitions that node loses"), over arbitrary
        /// memberships and an arbitrary victim — the exact dual of the add
        /// law, quantified rather than fixed at 3→2 nodes like the unit
        /// test above. Would catch a score that mixes membership into the
        /// hash: a survivor's partition would move.
        #[test]
        fn minimal_disruption_on_arbitrary_remove(
            names in proptest::collection::btree_set("[a-z]{1,8}", 2..12),
            victim_index in proptest::prelude::any::<proptest::sample::Index>(),
            parts in proptest::collection::vec("[a-z0-9/]{1,16}", 1..64),
        ) {
            let before: Vec<NodeId> = names.iter().map(NodeId::new).collect();
            let victim = before[victim_index.index(before.len())].clone();
            let after: Vec<NodeId> =
                before.iter().filter(|n| **n != victim).cloned().collect();
            for raw in parts {
                let partition = PartitionId::new(raw);
                let old = hrw_owner(&partition, &before).expect("nonempty");
                let new = hrw_owner(&partition, &after).expect("nonempty");
                prop_assert!(*old == victim || new == old,
                    "{partition}: a survivor's partition moved from {old} to {new}");
            }
        }

        /// The exactness form of minimal disruption, one level stronger
        /// than the owner laws: removing a node merely DELETES it from the
        /// full HRW ranking — every survivor keeps its relative order. The
        /// ranking is the replica set (indices 1..RF) and the §4.3 ring
        /// walk-down, so this is what makes membership change move only
        /// the departed node's replica load. Implies both owner laws.
        /// Would catch any per-(partition, node) score that is not
        /// membership-independent.
        #[test]
        fn removal_projects_the_ranking_exactly(
            names in proptest::collection::btree_set("[a-z]{1,8}", 2..12),
            victim_index in proptest::prelude::any::<proptest::sample::Index>(),
            parts in proptest::collection::vec("[a-z0-9/]{1,16}", 1..32),
        ) {
            let before: Vec<NodeId> = names.iter().map(NodeId::new).collect();
            let victim = before[victim_index.index(before.len())].clone();
            let after: Vec<NodeId> =
                before.iter().filter(|n| **n != victim).cloned().collect();
            for raw in parts {
                let partition = PartitionId::new(raw);
                let projected: Vec<&NodeId> = hrw_ranked(&partition, &before)
                    .into_iter()
                    .filter(|n| **n != victim)
                    .collect();
                prop_assert_eq!(projected, hrw_ranked(&partition, &after));
            }
        }
    }
}
