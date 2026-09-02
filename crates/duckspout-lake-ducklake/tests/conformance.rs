//! The `DuckLake` backend self-certifies against the published
//! `LakeCommitter` conformance suite (§6.4, §10.3), then pins the
//! row-level facts the port cannot observe.

mod common;

use common::{count, inspect, lake_paths, materialize_part, open_committer};
use duckspout_lake_contract::conformance;

#[test]
fn ducklake_passes_the_conformance_suite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let paths = lake_paths(dir.path());
    let committer = open_committer(&paths);
    let data = paths.data.clone();
    let mut materialize = |part: &duckspout_types::PartName| materialize_part(&data, part);

    let report = conformance::run(&committer, &mut materialize).expect("suite passes");
    assert_eq!(
        report.passed,
        vec![
            "evolve_idempotent",
            "commit_advances_watermark",
            "re_registration_short_circuits",
            "watermark_monotone",
            "commit_without_watermark_row",
            "expire_keeps_fence",
            "unknown_partition_has_no_row",
            "attach_info_answers",
        ],
        "every check ran"
    );

    // Row-level facts the port cannot observe:
    let raw = inspect(&paths);
    // Three windows committed 10 rows each; window 0 was expired, so its
    // rows left the current snapshot — a wrong-path DELETE (or a filename
    // mismatch) would leave 30.
    assert_eq!(
        count(&raw, "SELECT count(*) FROM lake.ds_conformance"),
        20,
        "the expired part's rows left the table's current snapshot (§6.7)"
    );
    // The idempotent re-commit and the post-expire re-commit registered
    // nothing: exactly one manifest row per window, w0 marked expired and
    // KEPT (TN-36: the fence spans lake ∪ expired).
    assert_eq!(
        count(&raw, "SELECT count(*) FROM lake.duckspout_manifests"),
        3
    );
    assert_eq!(
        count(
            &raw,
            "SELECT count(*) FROM lake.duckspout_manifests WHERE expired"
        ),
        1
    );
    // One watermark row, at the suite's final value.
    assert_eq!(
        count(
            &raw,
            "SELECT count(*) FROM lake.duckspout_watermarks
             WHERE partition = 'conf.p0' AND complete_through_ms = 2000"
        ),
        1
    );
}

#[test]
fn commits_survive_a_reopen() {
    // The durability half the spike proved (#25), re-pinned through the
    // port: a fresh committer (fresh embedded DuckDB, fresh attach) sees
    // the committed watermark.
    let dir = tempfile::tempdir().expect("tempdir");
    let paths = lake_paths(dir.path());
    let data = paths.data.clone();
    let mut materialize = |part: &duckspout_types::PartName| materialize_part(&data, part);
    {
        let committer = open_committer(&paths);
        conformance::run(&committer, &mut materialize).expect("suite passes");
    } // dropped: clean close
    let reopened = open_committer(&paths);
    let rows = futures_block_on(duckspout_types::LakeCommitter::read_watermarks(
        &reopened,
        vec![duckspout_types::PartitionId::new("conf.p0")],
    ))
    .expect("read-back after reopen");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].complete_through_ms, 2_000);
}

/// Local single-future driver (the committer resolves synchronously).
fn futures_block_on<T>(mut future: duckspout_types::BoxFuture<'_, T>) -> T {
    use std::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
