//! Per-PR instruction-count gate (§8.6, ADR-0005; issue #46): the
//! `StageCommit` step (§4.3) — one [`StageTxn::append`] +
//! [`StageTxn::commit`] transaction against a real, on-disk `DuckDB` engine
//! (WAL = hot, ADR-0003 — there is no in-memory double to substitute here,
//! same fidelity rule `tests/common/mod.rs` documents). `scripts/instr-gate.mjs`
//! runs this via `just instr-gate`, comparing the callgrind instruction
//! count against `floors/instr-baselines/duckspout-staging-commit.json` at
//! a +15% ceiling.
//!
//! `#[path]` pulls in the integration tests' `FsStorage` + fixture helpers
//! (`tests/common/mod.rs`) rather than duplicating them — same real
//! filesystem `Storage` port impl the engine's own tests exercise.
//!
//! One engine is opened in `setup()` — an argument-position function call in
//! the `#[bench::commit(setup())]` attribute, iai-callgrind's documented way
//! to keep setup cost out of the measured region (the alternative to the
//! `setup = fn` attribute parameter, per iai-callgrind's setup-and-teardown
//! guide) — and one fixed batch is appended and committed inside the
//! measured function: deterministic row count, deterministic timestamps, no
//! randomness in this crate's own code. `PRAGMA threads=1` in `setup()`
//! pins `DuckDB`'s own thread pool, without which the identical binary's
//! instruction count varied run-to-run (measured empirically) because the
//! bundled engine's default task scheduler runs part of the commit on a
//! background thread whose scheduling Valgrind does not make deterministic.
//!
//! Crate-wide `missing_docs` allow: iai-callgrind's `#[library_benchmark]`
//! and `library_benchmark_group!` generate undocumented items (a wrapper
//! module, a harness fn, a group const). Workspace-wide `missing_docs` is
//! `warn` (root `Cargo.toml` `[workspace.lints]`) but CI's
//! `RUSTFLAGS=-D warnings` promotes it to a hard error; this bench binary
//! has no public surface of its own to document, so the blanket allow costs
//! nothing real.
#![allow(missing_docs)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::hint::black_box;

use common::{FsStorage, log_batch, open_engine};
use duckspout_staging::StagingEngine;
use duckspout_types::{DatasetId, PartitionId, WindowId};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};

/// Rows in the fixed committed batch — large enough that the transactional
/// append/commit work dominates fixed per-batch overhead.
const ROWS: usize = 500;

struct Fixture {
    _dir: tempfile::TempDir,
    engine: StagingEngine<FsStorage>,
}

fn setup() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = open_engine(dir.path(), "instr-gate/1");
    // Pin DuckDB's thread pool to 1 (module-wide config on the shared
    // DB instance, set via a cloned connection before the measured region
    // starts) — without this, the bundled engine's default parallel task
    // scheduler makes the very same commit()'s instruction count vary
    // run-to-run by up to 2x (measured empirically while building this
    // benchmark), which would make `just instr-gate` a flaky gate — and
    // ADR-0005 treats a flaky gate as a red gate. Single-threaded execution
    // is deterministic; it still exercises the real StageCommit code path.
    engine
        .reader()
        .expect("reader")
        .query_arrow("PRAGMA threads=1")
        .expect("pin single-threaded execution");
    Fixture { _dir: dir, engine }
}

// The benchmarked function: one `append` + `commit()` transaction against
// the `setup()`-built fixture. NOT a doc comment (`///`) —
// `#[library_benchmark]` rejects any attribute on its function other than
// `#[bench::...]`/`#[benches::...]` (including a `#[doc = ...]` a `///`
// comment would desugar to).
#[library_benchmark]
#[bench::commit(setup())]
fn stage_commit(fixture: Fixture) {
    let dataset = DatasetId::new("logs");
    let partition = PartitionId::new("t1.0");
    let batch = log_batch(ROWS, 0, 32);
    let mut txn = fixture.engine.begin().expect("begin");
    txn.append(&dataset, &partition, WindowId(0), &batch)
        .expect("append");
    black_box(txn.commit().expect("commit"));
}

library_benchmark_group!(name = staging_commit; benchmarks = stage_commit);

main!(library_benchmark_groups = staging_commit);
