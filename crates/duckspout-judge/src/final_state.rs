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

//! # The changelog latest view (#206)
//!
//! [`LatestView`] is this module's second read-back surface: the
//! `<dataset>_latest` argmax view (§7.7) a changelog dataset serves, which
//! `crate::predicates::latest_view` compares against the fold of the acked
//! changelog. It is deliberately a WHOLE-VIEW query (`dataset` in, the
//! served key→value map out) rather than the per-record probe
//! [`FinalSystemState::contains`] is: a per-key probe could only ever find
//! keys the judge already expected, and `LatestViewCorrect` is violated just
//! as badly by a key the view serves that the fold says was deleted (a
//! resurrected tombstone) as by one it fails to serve. The whole-view shape
//! is also the one a real backend actually has — `SELECT * FROM
//! ds.<dataset>_latest` is one query, where `contains` is one round trip per
//! record (this module's MEDIUM-5 honesty note above).

use std::collections::{BTreeMap, HashSet};

use duckspout_types::DatasetId;
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

/// A `<dataset>_latest` read-back failed (the [`QueryError`] analogue for
/// [`LatestView`]).
///
/// A separate type rather than a reuse of [`QueryError`]: that error names
/// the subject of a failed probe as `(tenant, record_key)`, which a
/// whole-view query simply does not have — bending its fields to mean
/// something else at one call site is how error messages start lying. Both
/// exist for the same reason (module docs' MEDIUM-5(b)): a failed query is
/// not proof of absence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("latest-view query for dataset {dataset} failed: {reason}")]
pub struct ViewQueryError {
    /// The dataset whose view could not be read.
    pub dataset: DatasetId,
    /// A human-readable reason (a real backend's own error, stringified).
    pub reason: String,
}

/// The served `<dataset>_latest` view of a changelog dataset (§7.7).
pub trait LatestView {
    /// Every key the view serves for `dataset`, with its served value —
    /// tombstoned keys are ABSENT from the map, exactly as they are absent
    /// from the view (§7.7: "tombstones make keys absent from the view").
    ///
    /// # Errors
    ///
    /// A [`ViewQueryError`] iff the query itself failed — never merely
    /// because the view is empty (an empty view is `Ok` of an empty map).
    fn view(&self, dataset: &DatasetId) -> Result<BTreeMap<String, String>, ViewQueryError>;
}

/// A test double: the exact served view per dataset. A dataset with no
/// entry serves an EMPTY view rather than failing — "the dataset exists and
/// currently shows nothing" is a real, and violating, state when the fold
/// says it should show something, so defaulting to an error here would let
/// a genuinely empty view escape as `NoVerdict` instead of `Violation`.
#[derive(Debug, Default, Clone)]
pub struct InMemoryLatestView {
    views: BTreeMap<DatasetId, BTreeMap<String, String>>,
}

impl InMemoryLatestView {
    /// An empty view set: every dataset serves nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `dataset`'s served view to `rows` — builder-style, for compact
    /// test/fixture setup.
    #[must_use]
    pub fn with_view(
        mut self,
        dataset: &str,
        rows: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.views
            .insert(DatasetId::new(dataset), rows.into_iter().collect());
        self
    }

    /// Builds a latest-view double from a fixture file's JSON text — the
    /// judge binary's `--latest-view-fixture` flag, the `<dataset>_latest`
    /// counterpart of [`InMemoryFinalState::from_fixture_json`] and, like
    /// it, a DEV/TEST stand-in for a real read-back (module docs).
    ///
    /// Shape: `{"views": {"<dataset>": {"<key>": "<value>", …}, …}}`.
    ///
    /// # Errors
    ///
    /// Returns any [`serde_json`] decode error.
    pub fn from_fixture_json(text: &str) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct FixtureFile {
            views: BTreeMap<DatasetId, BTreeMap<String, String>>,
        }
        let fixture: FixtureFile = serde_json::from_str(text)?;
        Ok(Self {
            views: fixture.views,
        })
    }
}

impl LatestView for InMemoryLatestView {
    fn view(&self, dataset: &DatasetId) -> Result<BTreeMap<String, String>, ViewQueryError> {
        Ok(self.views.get(dataset).cloned().unwrap_or_default())
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

    #[test]
    fn an_unknown_dataset_serves_an_empty_view_rather_than_failing() {
        // Deliberate (this type's own docs): an empty served view is a
        // real, violating state when the fold expects rows, so it must
        // reach the predicate as data, not as a query failure that would
        // downgrade a genuine violation to NoVerdict.
        let view = InMemoryLatestView::new();
        assert_eq!(view.view(&DatasetId::new("nope")), Ok(BTreeMap::new()));
    }

    #[test]
    fn with_view_serves_exactly_the_rows_given() {
        let view = InMemoryLatestView::new().with_view(
            "dim",
            [
                ("k1".to_owned(), "v1".to_owned()),
                ("k2".to_owned(), "v2".to_owned()),
            ],
        );
        let served = view.view(&DatasetId::new("dim")).expect("query");
        assert_eq!(served.get("k1").map(String::as_str), Some("v1"));
        assert_eq!(served.len(), 2);
    }

    #[test]
    fn latest_view_from_fixture_json_decodes_views() {
        let view = InMemoryLatestView::from_fixture_json(
            r#"{"views":{"dim_users":{"u1":"alice","u2":"bob"}}}"#,
        )
        .expect("decodes");
        let served = view.view(&DatasetId::new("dim_users")).expect("query");
        assert_eq!(served.get("u2").map(String::as_str), Some("bob"));
        assert_eq!(served.len(), 2);
    }

    #[test]
    fn latest_view_from_fixture_json_rejects_malformed_input() {
        assert!(InMemoryLatestView::from_fixture_json("not json").is_err());
    }
}
