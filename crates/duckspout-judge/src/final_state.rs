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
//! later is additive at the CALL-SITE level — `check()`'s algorithm does not
//! change — tracked as a follow-up once a real fleet run exists to query
//! (see the PR description).
//!
//! # Honest limits of this shape (ACPR finding MEDIUM-5)
//!
//! `contains` is a per-record boolean query. That is NOT the shape a real
//! backend implementation should aim to keep: issuing one Arrow Flight (or
//! `DuckLake` catalog) round trip per acked record does not scale to a fleet
//! run acking millions of records, and a real implementation will almost
//! certainly want to batch this into range- or set-oriented queries
//! (`WHERE loadgen_index IN (...)`-shaped, or a per-tenant range scan)
//! rather than a call per record — a genuinely different internal shape
//! from what this trait's single-record signature suggests, even though the
//! call site in `check()` would not need to change. This module does not
//! claim otherwise.
//!
//! `contains` also returns `Result<bool, QueryError>`, not a bare `bool`
//! (the fix for MEDIUM-5(b)): a real backend query CAN fail (a timeout, a
//! dropped connection, an unavailable catalog) independently of whether the
//! record is actually present, and collapsing that failure into "absent"
//! would let a transient infra hiccup falsely convict a system that never
//! actually lost anything — exactly backwards for a judge whose entire job
//! is fail-closed conviction. `crate::predicates::zero_acked_lost::check`
//! treats a query error as its own `NoVerdict`, distinct from both `Pass`
//! and a genuine `Violation`.
//!
//! Finally (MEDIUM-5(c)): this module is coupled to
//! `duckspout_loadgen`'s own synthetic record-identity scheme, not
//! genuinely dataset-agnostic. `record_key` is expected to be exactly the
//! string `duckspout_loadgen::client::synthetic_batch` embeds as the
//! record's `loadgen.index` attribute (`{source_incarnation}-{index}`,
//! ACPR HIGH-2) — a real backend implementation would need to know that
//! wire convention to answer `contains` at all. A genuinely dataset-agnostic
//! interface, if one is ever needed for a non-loadgen-generated dataset,
//! is out of scope here.

use std::collections::HashSet;

use serde::Deserialize;

/// A final-system read-back query failed (ACPR finding MEDIUM-5(b)):
/// distinguishable from "genuinely absent" so a transient infra hiccup can
/// never silently read as proof of loss.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("final-system query for tenant {tenant:?} record {record_key:?} failed: {reason}")]
pub struct QueryError {
    /// The tenant the failed query was for.
    pub tenant: String,
    /// The record key the failed query was for (module docs: the
    /// `{source_incarnation}-{index}` composite, matching the wire
    /// attribute value verbatim).
    pub record_key: String,
    /// A human-readable reason (a real backend's own error, stringified).
    pub reason: String,
}

/// Whether a given tenant's record — identified by its `loadgen.index`
/// attribute value (`duckspout_loadgen::client::synthetic_batch`, module
/// docs' honesty note about this coupling) — is present, queryable from hot
/// or lake, in the final system (§8.4).
pub trait FinalSystemState {
    /// True iff the record keyed `record_key` for `tenant` is queryable
    /// (hot or lake) in the final system.
    ///
    /// # Errors
    ///
    /// A [`QueryError`] iff the query itself failed — NEVER returned merely
    /// because the record is absent (module docs' MEDIUM-5(b) fix): absence
    /// is `Ok(false)`, a failed query is `Err`, and a caller must not
    /// conflate the two.
    fn contains(&self, tenant: &str, record_key: &str) -> Result<bool, QueryError>;
}

/// A test double: an explicit set of `(tenant, record_key)` pairs known
/// present. Everything else reads as absent — the honest default for
/// "never proven present," matching the predicate's own fail-closed posture
/// (a double that defaulted to "present" would make every test vacuously
/// pass). Its `contains` never fails (always `Ok`); a real backend
/// implementation is the one that can return `Err`.
#[derive(Debug, Default, Clone)]
pub struct InMemoryFinalState {
    present: HashSet<(String, String)>,
}

impl InMemoryFinalState {
    /// An empty final state: nothing is present.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks every index in `[first_index, first_index + count)` present for
    /// `tenant`, under the composite `{source_incarnation}-{index}` key
    /// (ACPR HIGH-2, module docs) — builder-style, for compact test/fixture
    /// setup.
    #[must_use]
    pub fn with_present_range(
        mut self,
        tenant: &str,
        source_incarnation: &str,
        first_index: u64,
        count: usize,
    ) -> Self {
        for offset in 0..count as u64 {
            let index = first_index + offset;
            self.present
                .insert((tenant.to_owned(), format!("{source_incarnation}-{index}")));
        }
        self
    }
}

impl FinalSystemState for InMemoryFinalState {
    fn contains(&self, tenant: &str, record_key: &str) -> Result<bool, QueryError> {
        Ok(self
            .present
            .contains(&(tenant.to_owned(), record_key.to_owned())))
    }
}

/// One `present` range in a [`InMemoryFinalState`] fixture file
/// (`InMemoryFinalState::from_fixture_json`).
#[derive(Debug, Deserialize)]
struct FixtureRange {
    tenant: String,
    source_incarnation: String,
    first_index: u64,
    count: usize,
}

/// The on-disk shape `InMemoryFinalState::from_fixture_json` reads: a list
/// of present `(tenant, source_incarnation, first_index, count)` ranges. A
/// DEV/TEST convenience standing in for a real hot/lake read-back (module
/// docs) — not a production wire format, so it is deliberately not shared
/// with any other crate.
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
            state = state.with_present_range(
                &range.tenant,
                &range.source_incarnation,
                range.first_index,
                range.count,
            );
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
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-0"), Ok(false));
    }

    #[test]
    fn with_present_range_marks_exactly_the_declared_half_open_range() {
        // Would catch an off-by-one on either edge of the range.
        let state =
            InMemoryFinalState::new().with_present_range("tenant-a", "loadgen-0-1000", 10, 3);
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-9"), Ok(false));
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-10"), Ok(true));
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-11"), Ok(true));
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-12"), Ok(true));
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-13"), Ok(false));
    }

    #[test]
    fn presence_is_tenant_scoped() {
        let state =
            InMemoryFinalState::new().with_present_range("tenant-a", "loadgen-0-1000", 0, 5);
        assert_eq!(state.contains("tenant-b", "loadgen-0-1000-0"), Ok(false));
    }

    #[test]
    fn presence_is_source_incarnation_scoped() {
        // The exact ACPR HIGH-2 aliasing shape: two different incarnations
        // (fleet members, or one member's restart) using the identical
        // numeric index must NOT alias onto the same key.
        let state =
            InMemoryFinalState::new().with_present_range("tenant-a", "loadgen-0-1000", 0, 2);
        assert_eq!(state.contains("tenant-a", "loadgen-0-1000-0"), Ok(true));
        assert_eq!(state.contains("tenant-a", "loadgen-1-2000-0"), Ok(false));
    }

    #[test]
    fn from_fixture_json_decodes_present_ranges() {
        let state = InMemoryFinalState::from_fixture_json(
            r#"{"present":[{"tenant":"a","source_incarnation":"loadgen-0-1000","first_index":0,"count":3},
                           {"tenant":"b","source_incarnation":"loadgen-1-2000","first_index":10,"count":1}]}"#,
        )
        .expect("decodes");
        assert_eq!(state.contains("a", "loadgen-0-1000-0"), Ok(true));
        assert_eq!(state.contains("a", "loadgen-0-1000-2"), Ok(true));
        assert_eq!(state.contains("a", "loadgen-0-1000-3"), Ok(false));
        assert_eq!(state.contains("b", "loadgen-1-2000-10"), Ok(true));
    }

    #[test]
    fn from_fixture_json_rejects_malformed_input() {
        assert!(InMemoryFinalState::from_fixture_json("not json").is_err());
    }
}
