//! The seeded-violation-replay harness (issue #208, `docs/verification.md`
//! §8.4):
//!
//! > Additionally, each judge is periodically run against a
//! > **seeded-violation replay** — a journal set with a known injected
//! > violation — and must convict it; a judge that acquits its own seeded
//! > violation fails CI.
//!
//! This is the CTK distributed tier's version of the §8.1 armed-broken-variant
//! convention ("every checked safety invariant ships a deliberately broken
//! sibling […] that MUST reproduce its own violation on every run"), and it
//! exists for the same reason: a predicate that stopped biting — because a
//! refactor neutered it, because its evidence accessor started returning
//! nothing, because a guard was inverted — still reports `Pass` on every
//! healthy run, and nothing else in this repository would notice.
//!
//! # The shape: one clean base, five one-file seeds
//!
//! `tests/fixtures/replay/base/` is a COMPLETE, HEALTHY fleet-run evidence
//! set: three journals (two fleet nodes and a load generator), a served-read
//! log, the three read-back fixtures, the injectors' fault ledger and the run
//! manifest. It exits `0` — every one of the five predicates and all four of
//! §8.4's run-level vacuity rules pass over it ([`the_clean_base_exits_pass`]
//! asserts exactly that).
//!
//! Each `fixtures/replay/<predicate>/` directory then contains the ONE file
//! that differs, overlaid on that base. So each seeded violation is literally
//! a one-file diff from a run that passes, and the assertion
//! "predicate X convicts this" cannot be satisfied by a fixture that is
//! broken in some general way — the same fixture minus that one file is a
//! clean pass.
//!
//! # Why the base's own pass is half the harness
//!
//! A must-convict test alone is gameable in the direction that matters least:
//! a predicate hard-wired to `Violation` would pass every conviction test in
//! this file. The differential is what closes that — for each seed, the
//! predicate convicts the seeded set AND passes the base, so its verdict is
//! demonstrably a function of the seeded defect and not of the fixture's
//! existence. [`each_seed_is_convicted_by_its_own_predicate_and_only_it`] adds
//! the third leg: no OTHER predicate may convict a seed either, so a seed
//! cannot be "caught" by a predicate that happens to overlap it, and a
//! predicate cannot borrow another's conviction to look alive.
//!
//! # Why not one seed per §3 invariant
//!
//! §8.4 asks for one seeded violation per JUDGE, and that is what this is:
//! five judges, five seeds. Sharper per-obligation coverage (each predicate's
//! individual findings) already lives in that predicate's own unit tests;
//! this file's job is the crate-level "does each judge still bite at all,
//! through the real pipeline, over real files on disk."

use std::path::{Path, PathBuf};

use duckspout_judge::predicates::cache_transparency::DEFAULT_MAX_RACING_READ_MS;
use duckspout_judge::runner::{RunArgs, RunOutcome, run};
use duckspout_judge::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION;
use duckspout_judge::vacuity::DEFAULT_MAX_JOURNAL_SILENCE_MS;
use duckspout_judge::verdict::Verdict;

/// One seeded-violation replay: the fixture directory overlaid on the base,
/// the predicate that MUST convict it, and a substring its finding must name
/// so a conviction for the wrong reason is not accepted as a conviction.
struct Seed {
    /// Directory under `fixtures/replay/` holding the one differing file.
    dir: &'static str,
    /// The `crate::runner::PredicateReport::predicate` that must convict.
    predicate: &'static str,
    /// What the seeded defect is, for a failure message.
    defect: &'static str,
    /// A substring the convicting finding must contain — the seeded artifact
    /// itself, never a phrase the predicate would print for any violation.
    names: &'static str,
}

/// Every judge in the crate, with its own seeded violation. A predicate added
/// to `duckspout_judge::runner::run` without a row here is a judge with no
/// must-convict test — which
/// [`every_predicate_the_runner_reports_has_a_seed`] fails on.
const SEEDS: &[Seed] = &[
    Seed {
        dir: "zero-acked-lost",
        predicate: "zero-acked-lost",
        defect: "the middle record of a journaled 3-record ClientAck is absent from the final \
                 system",
        names: "at indices {1}",
    },
    Seed {
        dir: "watermark-honesty",
        predicate: "watermark-honesty",
        defect: "n1 advertises complete_through 9999 for t0-s0, while the only journaled commit \
                 for that partition reached 2000",
        names: "9999",
    },
    Seed {
        dir: "latest-view",
        predicate: "latest-view",
        defect: "the served <dataset>_latest row for key u2 is a stale value, not the fold of \
                 the acked changelog. Key u2's winning entry sits OUTSIDE the expired part's \
                 arrival range on purpose: an acked row whose winner was inside it is retention \
                 honesty's obligation (B), and seeding this on such a row would convict two \
                 judges at once, hiding whichever of them had stopped working",
        names: "bob-STALE",
    },
    Seed {
        dir: "retention-honesty",
        predicate: "retention-honesty",
        defect: "the committed snapshot covers arrival range 1..=3, while the expired changelog \
                 part covered 1..=5",
        names: "Keep Rule 10",
    },
    Seed {
        dir: "cache-transparency",
        predicate: "cache-transparency",
        defect: "the same question at the same pinned coverage returns a phantom extra row at \
                 one cache state — a stale cached row surviving the storm. An EXTRA row rather \
                 than a missing one on purpose: a missing row is also a watermark-honesty \
                 violation (a record acked under the served watermark, absent from the answer), \
                 which would convict two judges at once",
        names: "loadgen-0-0900-7",
    },
];

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay")
}

/// Copies every file in `from` into `into`, overwriting.
fn overlay(from: &Path, into: &Path) {
    for entry in std::fs::read_dir(from).unwrap_or_else(|e| panic!("read {}: {e}", from.display()))
    {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        assert!(
            path.is_file(),
            "replay fixtures are flat directories of files; found {}",
            path.display()
        );
        std::fs::copy(&path, into.join(entry.file_name())).expect("copy fixture");
    }
}

/// The base evidence set, with `seed_dir`'s files overlaid on it when given,
/// materialised in a fresh temp directory and judged through the real
/// pipeline.
///
/// Copied to a temp directory rather than judged in place for one specific
/// reason: `duckspout_judge::summary` finds a loadgen journal's run-summary
/// sidecar at `{journal_path}.summary.json`, so the sidecar must sit beside
/// whichever copy of the journal is actually being read. Judging in place
/// would work today and break the moment a test wanted to vary the sidecar.
fn replay(seed_dir: Option<&str>) -> (tempfile::TempDir, RunOutcome) {
    let dir = tempfile::tempdir().expect("tempdir");
    overlay(&fixtures().join("base"), dir.path());
    if let Some(seed_dir) = seed_dir {
        let seed = fixtures().join(seed_dir);
        assert!(
            seed.is_dir(),
            "seed fixture {} does not exist",
            seed.display()
        );
        overlay(&seed, dir.path());
    }
    let at = |name: &str| dir.path().join(name);
    let outcome = run(&RunArgs {
        journals: vec![at("n1.ndjson"), at("n2.ndjson"), at("loadgen.ndjson")],
        final_state_fixture: Some(at("final_state.json")),
        read_log: Some(at("reads.ndjson")),
        latest_view_fixture: Some(at("latest_view.json")),
        committed_parts_fixture: Some(at("committed_parts.json")),
        fault_log: Some(at("faults.ndjson")),
        run_manifest: Some(at("run.json")),
        max_ambiguous_fraction: DEFAULT_MAX_AMBIGUOUS_FRACTION,
        max_journal_silence_ms: DEFAULT_MAX_JOURNAL_SILENCE_MS,
        max_racing_read_ms: DEFAULT_MAX_RACING_READ_MS,
    });
    (dir, outcome)
}

fn reports(outcome: &RunOutcome) -> &[duckspout_judge::runner::PredicateReport] {
    match outcome {
        RunOutcome::Judged { reports, .. } => reports,
        other @ RunOutcome::IngestionFailed(_) => {
            panic!("expected a judged run, got {other:?}")
        }
    }
}

fn verdict_of<'a>(outcome: &'a RunOutcome, predicate: &str) -> &'a Verdict<String> {
    &reports(outcome)
        .iter()
        .find(|report| report.predicate == predicate)
        .unwrap_or_else(|| panic!("no report named {predicate}"))
        .verdict
}

/// The other half of every must-convict assertion: the SAME evidence set
/// without the seeded file passes, entirely — five predicates and four
/// run-level vacuity rules. Without this, "the predicate convicted" would say
/// nothing about whether it convicted the seeded defect or the fixture.
#[test]
fn the_clean_base_exits_pass() {
    let (_dir, outcome) = replay(None);
    for report in reports(&outcome) {
        assert!(
            matches!(report.verdict, Verdict::Pass { .. }),
            "the clean base must pass everything, but {} reported {:?}",
            report.predicate,
            report.verdict
        );
    }
    assert_eq!(outcome.exit_code(), 0);
}

/// §8.4's must-convict rule, for all five judges at once: each seeded
/// violation is convicted BY ITS OWN PREDICATE, for a reason that names the
/// seeded artifact, and by no other predicate.
///
/// Three assertions per seed, each catching a different failure mode:
///
/// - **`Violation` from the named predicate** — the predicate stopped biting
///   (a neutered guard, an evidence accessor that quietly returns nothing, a
///   verdict that got downgraded to `NoVerdict` on a path it should convict).
/// - **the finding names the seeded artifact** — the predicate convicted, but
///   for something else, so the test would keep passing after the seeded
///   defect stopped being detected.
/// - **no other predicate convicts** — the seed is not a general breakage,
///   and no predicate is borrowing another's conviction to look alive.
#[test]
fn each_seed_is_convicted_by_its_own_predicate_and_only_it() {
    for seed in SEEDS {
        let (_dir, outcome) = replay(Some(seed.dir));
        match verdict_of(&outcome, seed.predicate) {
            Verdict::Violation(findings) => {
                assert!(
                    findings.iter().any(|f| f.contains(seed.names)),
                    "{} convicted the {} seed ({}) but no finding names {:?}: {findings:?}",
                    seed.predicate,
                    seed.dir,
                    seed.defect,
                    seed.names,
                );
            }
            other => panic!(
                "{} ACQUITTED its own seeded violation ({}) — §8.4: a judge that acquits its \
                 seeded violation fails CI. Got {other:?}",
                seed.predicate, seed.defect,
            ),
        }
        for report in reports(&outcome) {
            assert!(
                report.predicate == seed.predicate
                    || !matches!(report.verdict, Verdict::Violation(_)),
                "the {} seed ({}) also convicted {} — a seed must isolate the one judge it is \
                 aimed at, or a dead predicate could hide behind a live one",
                seed.dir,
                seed.defect,
                report.predicate,
            );
        }
        assert_eq!(
            outcome.exit_code(),
            2,
            "a convicted run exits 2 (§8.4's exit contract)"
        );
    }
}

/// Every predicate the runner reports has a seed here — so a sixth judge
/// cannot land without its own must-convict fixture. The `vacuity/…` rules
/// are deliberately exempt: they never convict at all
/// (`duckspout_judge::vacuity`'s own contract, asserted in that module), so
/// "must convict" is not a property they can have.
#[test]
fn every_predicate_the_runner_reports_has_a_seed() {
    let (_dir, outcome) = replay(None);
    let seeded: Vec<&str> = SEEDS.iter().map(|s| s.predicate).collect();
    for report in reports(&outcome) {
        if report.predicate.starts_with("vacuity/") {
            continue;
        }
        assert!(
            seeded.contains(&report.predicate),
            "predicate {} has no seeded-violation replay — §8.4 requires one per judge",
            report.predicate
        );
    }
}

/// Each seed is a ONE-FILE diff against the base. A seed that grew a second
/// file would quietly widen what it changes, and the isolation the test above
/// asserts would stop meaning what it says.
#[test]
fn each_seed_changes_exactly_one_file_of_the_base() {
    let base = fixtures().join("base");
    for seed in SEEDS {
        let dir = fixtures().join(seed.dir);
        let files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .expect("read seed dir")
            .map(|e| e.expect("dir entry").path())
            .collect();
        assert_eq!(
            files.len(),
            1,
            "seed {} changes {} files; each seed is one file",
            seed.dir,
            files.len()
        );
        let name = files[0].file_name().expect("file name");
        assert!(
            base.join(name).is_file(),
            "seed {} overlays {:?}, which is not part of the base evidence set",
            seed.dir,
            name
        );
        assert_ne!(
            std::fs::read_to_string(&files[0]).expect("read seed file"),
            std::fs::read_to_string(base.join(name)).expect("read base file"),
            "seed {} overlays {:?} with content identical to the base — it seeds nothing",
            seed.dir,
            name
        );
    }
}
