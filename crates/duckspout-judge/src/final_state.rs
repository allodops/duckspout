//! Read-back access to the final system state after a fleet run — "present
//! in the final system … queryable from hot or lake" (§8.4's zero-acked-lost
//! wording).
//!
//! # What's real here, and what's deferred (#205 scope note)
//!
//! This module ships the [`FinalSystemState`] interface plus
//! [`InMemoryFinalState`], a test double, and nothing else. Wiring a REAL
//! backend — querying `duckspout-daemon`'s Arrow Flight serving surface
//! (`crates/duckspout-daemon/src/serving.rs`) or attaching directly to the
//! `DuckLake` catalog for the lake half — is deliberately out of scope for
//! this PR: `duckspout-fleet` (the distributed-run driver this judge is
//! meant to grade) is still its own clap skeleton (no real multi-node run
//! exists to judge yet), and #203/#204's fault-schedule work — which is
//! what will actually produce a fleet run worth reading back — hasn't
//! landed. Building the real query against a system that cannot yet run is
//! exactly the kind of empirical validation `AGENTS.md`'s
//! R-trust-official-docs section warns against skipping when a guarantee is
//! unverified; here there is nothing to verify against yet. The predicate
//! logic in `crate::predicates::zero_acked_lost` is written entirely
//! against the [`FinalSystemState`] trait, so wiring a real implementation
//! later is additive — no predicate-logic change needed — tracked as a
//! follow-up once a real fleet run exists to query (see the PR description).

use std::collections::HashSet;

use serde::Deserialize;

/// Whether a given tenant's record — identified by its `loadgen.index`
/// attribute (`duckspout_loadgen::client::synthetic_batch`) — is present,
/// queryable from hot or lake, in the final system (§8.4).
pub trait FinalSystemState {
    /// True iff the record at `index` for `tenant` is queryable (hot or
    /// lake) in the final system.
    fn contains(&self, tenant: &str, index: u64) -> bool;
}

/// A test double: an explicit set of `(tenant, index)` pairs known present.
/// Everything else reads as absent — the honest default for "never proven
/// present," matching the predicate's own fail-closed posture (a double
/// that defaulted to "present" would make every test vacuously pass).
#[derive(Debug, Default, Clone)]
pub struct InMemoryFinalState {
    present: HashSet<(String, u64)>,
}

impl InMemoryFinalState {
    /// An empty final state: nothing is present.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks every index in `[first_index, first_index + count)` present
    /// for `tenant`. Builder-style, for compact test/fixture setup.
    #[must_use]
    pub fn with_present_range(mut self, tenant: &str, first_index: u64, count: usize) -> Self {
        for offset in 0..count as u64 {
            self.present
                .insert((tenant.to_owned(), first_index + offset));
        }
        self
    }
}

impl FinalSystemState for InMemoryFinalState {
    fn contains(&self, tenant: &str, index: u64) -> bool {
        self.present.contains(&(tenant.to_owned(), index))
    }
}

/// One `present` range in a [`InMemoryFinalState`] fixture file
/// (`InMemoryFinalState::from_fixture_json`).
#[derive(Debug, Deserialize)]
struct FixtureRange {
    tenant: String,
    first_index: u64,
    count: usize,
}

/// The on-disk shape `InMemoryFinalState::from_fixture_json` reads: a list
/// of present `(tenant, first_index, count)` ranges. A DEV/TEST convenience
/// standing in for a real hot/lake read-back (module docs) — not a
/// production wire format, so it is deliberately not shared with any other
/// crate.
#[derive(Debug, Deserialize)]
struct FixtureFile {
    present: Vec<FixtureRange>,
}

impl InMemoryFinalState {
    /// Builds a final-state double from a fixture file's JSON text — the
    /// judge binary's `--final-state-fixture` flag (module docs: a
    /// stand-in for real read-back, since no real fleet run exists yet to
    /// query for real).
    ///
    /// # Errors
    ///
    /// Returns any [`serde_json`] decode error.
    pub fn from_fixture_json(text: &str) -> Result<Self, serde_json::Error> {
        let fixture: FixtureFile = serde_json::from_str(text)?;
        let mut state = Self::new();
        for range in fixture.present {
            state = state.with_present_range(&range.tenant, range.first_index, range.count);
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_state_contains_nothing() {
        let state = InMemoryFinalState::new();
        assert!(!state.contains("tenant-a", 0));
    }

    #[test]
    fn with_present_range_marks_exactly_the_declared_half_open_range() {
        // Would catch an off-by-one on either edge of the range.
        let state = InMemoryFinalState::new().with_present_range("tenant-a", 10, 3);
        assert!(!state.contains("tenant-a", 9));
        assert!(state.contains("tenant-a", 10));
        assert!(state.contains("tenant-a", 11));
        assert!(state.contains("tenant-a", 12));
        assert!(!state.contains("tenant-a", 13));
    }

    #[test]
    fn presence_is_tenant_scoped() {
        let state = InMemoryFinalState::new().with_present_range("tenant-a", 0, 5);
        assert!(!state.contains("tenant-b", 0));
    }

    #[test]
    fn from_fixture_json_decodes_present_ranges() {
        let state = InMemoryFinalState::from_fixture_json(
            r#"{"present":[{"tenant":"a","first_index":0,"count":3},
                           {"tenant":"b","first_index":10,"count":1}]}"#,
        )
        .expect("decodes");
        assert!(state.contains("a", 0));
        assert!(state.contains("a", 2));
        assert!(!state.contains("a", 3));
        assert!(state.contains("b", 10));
    }

    #[test]
    fn from_fixture_json_rejects_malformed_input() {
        assert!(InMemoryFinalState::from_fixture_json("not json").is_err());
    }
}
