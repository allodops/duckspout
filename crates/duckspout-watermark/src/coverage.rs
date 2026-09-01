//! Coalesced per-origin sequence-range sets (crate-internal).
//!
//! Per-`(partition, origin)` sequences are 1-based (§4.2.4: gap refusal
//! admits exactly `applied_seq + 1`, and [`duckspout_types::AppliedWatermarkRow`]
//! encodes "nothing applied" as `applied_seq = 0`), so seq `0` is never a
//! member and coverage completeness is always judged from seq 1.

use std::collections::BTreeMap;

use duckspout_types::NodeId;

/// A sorted, disjoint, coalesced set of inclusive `u64` sequence ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SeqSet {
    /// Sorted by start; pairwise disjoint and non-adjacent.
    ranges: Vec<(u64, u64)>,
}

impl SeqSet {
    /// Whether `[first, last]` intersects any member range.
    pub(crate) fn overlaps(&self, first: u64, last: u64) -> bool {
        self.ranges.iter().any(|&(a, b)| first <= b && a <= last)
    }

    /// Union-inserts `[first, last]`, coalescing overlap and adjacency.
    pub(crate) fn insert(&mut self, first: u64, last: u64) {
        let mut merged = (first, last);
        let mut out = Vec::with_capacity(self.ranges.len() + 1);
        for &(a, b) in &self.ranges {
            if a <= merged.1.saturating_add(1) && merged.0 <= b.saturating_add(1) {
                merged = (merged.0.min(a), merged.1.max(b));
            } else {
                out.push((a, b));
            }
        }
        out.push(merged);
        out.sort_unstable();
        self.ranges = out;
    }

    /// The gaps between seq 1 and the set's maximum that the set does not
    /// cover. Empty for an empty set.
    pub(crate) fn gaps(&self) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        let mut next = 1_u64;
        for &(a, b) in &self.ranges {
            if a > next {
                gaps.push((next, a - 1));
            }
            next = b.saturating_add(1);
        }
        gaps
    }

    /// The sub-ranges of `[first, last]` this set does **not** cover.
    pub(crate) fn uncovered(&self, first: u64, last: u64) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        let mut next = first;
        for &(a, b) in &self.ranges {
            if next > last {
                break;
            }
            if b < next {
                continue;
            }
            if a > next {
                out.push((next, (a - 1).min(last)));
            }
            next = b.saturating_add(1);
        }
        if next <= last {
            out.push((next, last));
        }
        out
    }
}

/// Per-origin coalesced coverage.
pub(crate) type OriginCoverage = BTreeMap<NodeId, SeqSet>;

/// The gaps in `coverage` (from seq 1 up to each origin's committed maximum)
/// that `losses` does not excuse — the coverage-completeness check behind the
/// advance rule (§6.8's `NewWatermark`: committed-or-ledgered).
pub(crate) fn unexcused_gaps(
    coverage: &OriginCoverage,
    losses: &OriginCoverage,
) -> Vec<(NodeId, u64, u64)> {
    let mut out = Vec::new();
    for (origin, set) in coverage {
        for (first, last) in set.gaps() {
            match losses.get(origin) {
                Some(excuse) => out.extend(
                    excuse
                        .uncovered(first, last)
                        .into_iter()
                        .map(|(a, b)| (origin.clone(), a, b)),
                ),
                None => out.push((origin.clone(), first, last)),
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_coalesces_overlap_and_adjacency() {
        let mut set = SeqSet::default();
        set.insert(5, 8);
        set.insert(1, 2);
        set.insert(3, 4); // adjacent to both sides: 1..=8 coalesces
        assert!(set.gaps().is_empty());
        assert!(set.overlaps(8, 20));
        assert!(!set.overlaps(9, 20));
    }

    #[test]
    fn gaps_are_reported_from_seq_one() {
        let mut set = SeqSet::default();
        set.insert(3, 4);
        set.insert(8, 9);
        assert_eq!(set.gaps(), vec![(1, 2), (5, 7)]);
    }

    #[test]
    fn uncovered_clips_to_the_probed_range() {
        let mut set = SeqSet::default();
        set.insert(3, 4);
        assert_eq!(set.uncovered(1, 6), vec![(1, 2), (5, 6)]);
        assert_eq!(set.uncovered(3, 4), Vec::<(u64, u64)>::new());
        assert_eq!(set.uncovered(1, 2), vec![(1, 2)]);
        assert_eq!(set.uncovered(5, 9), vec![(5, 9)]);
    }
}
