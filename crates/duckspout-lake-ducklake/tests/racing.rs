//! The §6.6 racing-drains fence proof — ADR-0010's mandatory test, issue
//! #36's definition of done: two committers with **independent catalog connections**
//! (separate embedded `DuckDB` instances) race the same window's commit;
//! exactly one registration stands, and the loser resolves via read-back.
//!
//! Runs on a **SQLite catalog** (`WAL`, issue #119): SQLite does real
//! cross-connection locking through the catalog file itself, so two
//! independent committer instances contend with the same fidelity as two
//! processes. A `DuckDB`-file catalog cannot host this proof — a second
//! process cannot even `ATTACH` it, and two in-process instances sit in
//! #119's false-pass zone (per-process POSIX locks). The Postgres
//! topology carries the same snapshot-commit conflict through the same
//! extension code path.
//!
//! # Known upstream flake (issue #157)
//!
//! `repeated_races_never_double_commit` occasionally `SIGSEGV`s under CI's
//! concurrent test load (reproduced locally at ~1.7–2% per attempt under
//! contention; 0% in isolated runs). Root-caused to a race inside the
//! third-party `ducklake` extension's name-map cache
//! (`DuckLakeCatalog::LoadNameMaps` → `DuckLakeNameMapSet::Add` →
//! `DuckLakeNameMapEntry::GetHash`), several native frames below the `CALL
//! ducklake_add_data_files` this crate issues — not a bug in this crate or
//! in the test's synchronization (full backtrace and analysis in #157). A
//! red run with this exact signature is a known environment/dependency
//! flake to re-run, not a signal to bisect this crate's commits — and the
//! test stays exactly as strict as it is: weakening it would defeat the
//! §6.6 fence proof it exists to establish.

mod common;

use common::{count, inspect_uri, lake_paths, materialize_part, open_committer_sqlite};
use duckspout_lake_contract::conformance;

#[test]
fn racing_drains_admit_exactly_one_registration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let paths = lake_paths(dir.path());
    let a = open_committer_sqlite(dir.path(), &paths);
    let b = open_committer_sqlite(dir.path(), &paths);
    let data = paths.data.clone();
    let mut materialize = |part: &duckspout_types::PartName| materialize_part(&data, part);

    conformance::racing_drains(&a, &b, &mut materialize, "conf.race")
        .expect("the fence admits exactly one standing commit");

    // The backend-level halves the port cannot see (the DoD's teeth):
    let raw = inspect_uri(&a.config().catalog_uri, &paths.data);
    assert_eq!(
        count(
            &raw,
            "SELECT count(*) FROM lake.duckspout_manifests
             WHERE partition = 'conf.race' AND window_id = 0"
        ),
        1,
        "exactly one manifest registration stands — the §6.6 fence held"
    );
    assert_eq!(
        count(&raw, "SELECT count(*) FROM lake.ds_conformance"),
        10,
        "the part's rows registered exactly once (no double add_data_files)"
    );
    assert_eq!(
        count(
            &raw,
            "SELECT count(*) FROM lake.duckspout_watermarks
             WHERE partition = 'conf.race' AND complete_through_ms = 1000"
        ),
        1,
        "one watermark row, advanced exactly once"
    );
}

#[test]
fn repeated_races_never_double_commit() {
    // The race is timing-dependent; repeat it on fresh windows to sweep
    // interleavings (loser-in-flight → Aborted via snapshot conflict;
    // loser-after-winner → Committed via check-before-register — both are
    // single-registration outcomes).
    let dir = tempfile::tempdir().expect("tempdir");
    let paths = lake_paths(dir.path());
    let a = open_committer_sqlite(dir.path(), &paths);
    let b = open_committer_sqlite(dir.path(), &paths);
    let data = paths.data.clone();
    let mut materialize = |part: &duckspout_types::PartName| materialize_part(&data, part);

    for round in 0..4 {
        let partition = format!("conf.race{round}");
        conformance::racing_drains(&a, &b, &mut materialize, &partition)
            .unwrap_or_else(|e| panic!("round {round}: {e}"));
    }
    let raw = inspect_uri(&a.config().catalog_uri, &paths.data);
    assert_eq!(
        count(&raw, "SELECT count(*) FROM lake.duckspout_manifests"),
        4,
        "four races, four windows, four single registrations"
    );
    assert_eq!(
        count(&raw, "SELECT count(*) FROM lake.ds_conformance"),
        40,
        "each race's part registered exactly once"
    );
}
