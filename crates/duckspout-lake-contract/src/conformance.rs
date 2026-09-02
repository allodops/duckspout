//! The `LakeCommitter` conformance suite (§6.4), public so third-party
//! backends can self-certify (§10.3): backend #2 (and #3) is a community
//! contribution validated by the same harness, not a fork.
//!
//! The suite drives a backend exclusively **through the port** — commit
//! atomicity as observed via `read_watermarks`, the three-outcome
//! discipline, idempotent re-registration (a repeat commit short-circuits,
//! §6.5), expire semantics with the TN-36 fence (an expired part's window
//! can never be re-admitted, issue #142), and the racing-drains fence
//! (§6.6, ADR-0010). What the port cannot observe — duplicate physical
//! registration, crash-mid-commit recovery — is the backend's own test
//! obligation on top of this suite (the `DuckLake` backend pins those
//! against its catalog directly).
//!
//! # The materializer
//!
//! The suite names parts; the backend's harness must make each named part
//! exist wherever that backend resolves part names to data files, as a
//! Parquet file matching [`suite_schema`] (`ts TIMESTAMP`, `body VARCHAR`)
//! with at least one row. That is the whole harness contract.

use std::sync::Barrier;

use duckspout_types::{
    BoxFuture, ColumnSpec, CommitOutcome, DatasetId, LakeCommitter, OriginSeqRange, PartName,
    PartitionId, SchemaEvolution, WatermarkRow, WindowId, WindowManifest,
};

/// The dataset every suite commit targets.
pub const DATASET: &str = "conformance";

/// The schema the suite's dataset evolves to and every materialized part
/// must match: `ts TIMESTAMP` (micros) and `body VARCHAR`.
#[must_use]
pub fn suite_schema() -> SchemaEvolution {
    SchemaEvolution {
        dataset: DatasetId::new(DATASET),
        columns: vec![
            ColumnSpec {
                name: "ts".to_owned(),
                logical_type: "timestamp_micros".to_owned(),
            },
            ColumnSpec {
                name: "body".to_owned(),
                logical_type: "utf8".to_owned(),
            },
        ],
    }
}

/// Makes one named part exist as a data file the backend can register —
/// see the module docs for the contract.
pub trait PartMaterializer {
    /// Materializes `part`.
    ///
    /// # Errors
    ///
    /// A description of why the file could not be produced.
    fn materialize(&mut self, part: &PartName) -> Result<(), String>;
}

impl<F: FnMut(&PartName) -> Result<(), String>> PartMaterializer for F {
    fn materialize(&mut self, part: &PartName) -> Result<(), String> {
        self(part)
    }
}

/// What one conformance run verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceReport {
    /// Names of the checks that ran and passed.
    pub passed: Vec<&'static str>,
}

/// A conformance failure.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    /// A check convicted the backend.
    #[error("conformance check {check} failed: {detail}")]
    CheckFailed {
        /// The failing check's name.
        check: &'static str,
        /// What the backend did wrong.
        detail: String,
    },
    /// The harness (not the backend) failed — e.g. a part could not be
    /// materialized.
    #[error("conformance harness: {0}")]
    Harness(String),
}

/// Drives one port future to completion on the calling thread (the suite
/// is a test harness; backends resolve synchronously or on their own
/// executors).
fn block_on<T>(mut future: BoxFuture<'_, T>) -> T {
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

/// The suite's canonical part name for a window.
#[must_use]
pub fn part_name(partition: &str, window: u64) -> PartName {
    PartName::new(format!("{DATASET}/{partition}/w{window}-primary.parquet"))
}

/// The suite's canonical manifest for one window.
#[must_use]
pub fn manifest(partition: &str, window: u64, first_seq: u64, last_seq: u64) -> WindowManifest {
    WindowManifest {
        dataset: DatasetId::new(DATASET),
        partition: PartitionId::new(partition),
        window_id: WindowId(window),
        origin_coverage: vec![OriginSeqRange {
            origin: "conf-o1".into(),
            first_seq,
            last_seq,
        }],
        rows: last_seq - first_seq + 1,
        event_time_min_ms: 1_000 * i64::try_from(window + 1).unwrap_or(i64::MAX),
        event_time_max_ms: 1_000 * i64::try_from(window + 1).unwrap_or(i64::MAX),
        dedup_removed: 0,
        parts: vec![part_name(partition, window)],
    }
}

fn watermark(partition: &str, complete_through_ms: i64) -> WatermarkRow {
    WatermarkRow {
        partition: PartitionId::new(partition),
        complete_through_ms,
    }
}

fn fail(check: &'static str, detail: impl Into<String>) -> ConformanceError {
    ConformanceError::CheckFailed {
        check,
        detail: detail.into(),
    }
}

fn read_one<T: LakeCommitter + ?Sized>(
    committer: &T,
    partition: &str,
    check: &'static str,
) -> Result<Option<i64>, ConformanceError> {
    let rows = block_on(committer.read_watermarks(vec![PartitionId::new(partition)]))
        .map_err(|e| fail(check, format!("read_watermarks failed: {e}")))?;
    Ok(rows
        .iter()
        .find(|r| r.partition.as_str() == partition)
        .map(|r| r.complete_through_ms))
}

fn commit<T: LakeCommitter + ?Sized>(
    committer: &T,
    materialize: &mut dyn PartMaterializer,
    m: &WindowManifest,
    rows: Vec<WatermarkRow>,
    check: &'static str,
) -> Result<CommitOutcome, ConformanceError> {
    for part in &m.parts {
        materialize
            .materialize(part)
            .map_err(ConformanceError::Harness)?;
    }
    block_on(committer.commit_files(m.clone(), rows))
        .map_err(|e| fail(check, format!("commit_files errored: {e}")))
}

/// Runs the single-committer half of the conformance suite. The fence
/// half is [`racing_drains`] (it needs two independently connected
/// committers).
///
/// # Errors
///
/// The first failing check, or a harness failure.
#[allow(clippy::too_many_lines)] // a linear checklist, one check per block
pub fn run<T: LakeCommitter + ?Sized>(
    committer: &T,
    materialize: &mut dyn PartMaterializer,
) -> Result<ConformanceReport, ConformanceError> {
    let mut passed = Vec::new();
    let p = "conf.p0";

    // evolve_schema is idempotent: a repeat application converges (§6.4).
    {
        let check = "evolve_idempotent";
        for _ in 0..2 {
            block_on(committer.evolve_schema(suite_schema()))
                .map_err(|e| fail(check, format!("evolve_schema failed: {e}")))?;
        }
        passed.push(check);
    }

    // A commit atomically registers parts and advances the watermark, as
    // observed through read_watermarks (§6.4: WatermarkAdvance rides
    // LakeCommit).
    {
        let check = "commit_advances_watermark";
        let m0 = manifest(p, 0, 1, 10);
        let outcome = commit(
            committer,
            materialize,
            &m0,
            vec![watermark(p, 1_000)],
            check,
        )?;
        if outcome != CommitOutcome::Committed {
            return Err(fail(check, format!("first commit returned {outcome:?}")));
        }
        if read_one(committer, p, check)? != Some(1_000) {
            return Err(fail(check, "watermark did not advance to 1000"));
        }
        passed.push(check);
    }

    // Idempotent re-registration: the same commit again short-circuits to
    // Committed (§6.5 check-before-register + deterministic naming) and
    // observably changes nothing.
    {
        let check = "re_registration_short_circuits";
        let m0 = manifest(p, 0, 1, 10);
        let outcome = commit(
            committer,
            materialize,
            &m0,
            vec![watermark(p, 1_000)],
            check,
        )?;
        if outcome != CommitOutcome::Committed {
            return Err(fail(check, format!("repeat commit returned {outcome:?}")));
        }
        if read_one(committer, p, check)? != Some(1_000) {
            return Err(fail(check, "repeat commit moved the watermark"));
        }
        passed.push(check);
    }

    // The watermark is monotone across successive commits.
    {
        let check = "watermark_monotone";
        let m1 = manifest(p, 1, 11, 20);
        let outcome = commit(
            committer,
            materialize,
            &m1,
            vec![watermark(p, 2_000)],
            check,
        )?;
        if outcome != CommitOutcome::Committed {
            return Err(fail(check, format!("second window returned {outcome:?}")));
        }
        if read_one(committer, p, check)? != Some(2_000) {
            return Err(fail(check, "watermark did not advance to 2000"));
        }
        passed.push(check);
    }

    // A commit may carry no watermark row (a coverage-blocked partition,
    // §6.4): files register, the watermark stays put.
    {
        let check = "commit_without_watermark_row";
        let m2 = manifest(p, 2, 31, 40);
        let outcome = commit(committer, materialize, &m2, vec![], check)?;
        if outcome != CommitOutcome::Committed {
            return Err(fail(check, format!("rowless commit returned {outcome:?}")));
        }
        if read_one(committer, p, check)? != Some(2_000) {
            return Err(fail(check, "a rowless commit moved the watermark"));
        }
        passed.push(check);
    }

    // Expire semantics with the TN-36 fence: expiring a part neither
    // regresses the watermark nor re-admits the part's window — a repeat
    // commit of the expired window must short-circuit (Committed) or be
    // definitively rejected (Aborted), never freshly register.
    {
        let check = "expire_keeps_fence";
        block_on(committer.expire(vec![part_name(p, 0)]))
            .map_err(|e| fail(check, format!("expire failed: {e}")))?;
        if read_one(committer, p, check)? != Some(2_000) {
            return Err(fail(check, "expire moved the watermark"));
        }
        let m0 = manifest(p, 0, 1, 10);
        let outcome = commit(
            committer,
            materialize,
            &m0,
            vec![watermark(p, 1_000)],
            check,
        )?;
        if outcome == CommitOutcome::Indeterminate {
            return Err(fail(
                check,
                "re-commit of an expired window was Indeterminate",
            ));
        }
        if read_one(committer, p, check)? != Some(2_000) {
            return Err(fail(
                check,
                "re-commit of an expired window changed the watermark (TN-36: the fence \
                 must span lake ∪ expired)",
            ));
        }
        // Idempotent re-expire must also hold.
        block_on(committer.expire(vec![part_name(p, 0)]))
            .map_err(|e| fail(check, format!("re-expire failed: {e}")))?;
        passed.push(check);
    }

    // "Couldn't find" is an empty result, never an invented row (R-3).
    {
        let check = "unknown_partition_has_no_row";
        if read_one(committer, "conf.absent", check)?.is_some() {
            return Err(fail(check, "an unknown partition returned a watermark row"));
        }
        passed.push(check);
    }

    // attach_info answers (§6.4: feeds the catalog extension's bind).
    {
        let check = "attach_info_answers";
        let info = block_on(committer.attach_info())
            .map_err(|e| fail(check, format!("attach_info failed: {e}")))?;
        if info.catalog_uri.is_empty() {
            return Err(fail(check, "attach_info returned an empty catalog uri"));
        }
        passed.push(check);
    }

    Ok(ConformanceReport { passed })
}

/// The §6.6 racing-drains fence check (ADR-0010's mandatory proof shape):
/// two independently connected committers race the **same** window's
/// commit; the port contract requires that this can never double-commit —
/// every attempt resolves to `Committed` (the standing commit, whether won
/// or short-circuited on a late start) or `Aborted` (the fence), never
/// `Indeterminate`, and the watermark lands exactly once.
///
/// Port-level observability ends there: proving the *registration* was not
/// duplicated requires looking inside the backend, which is the backend's
/// own racing-drains test obligation (issue #36's definition of done for `DuckLake`).
///
/// # Errors
///
/// [`ConformanceError`] when any attempt errors, any outcome is
/// `Indeterminate`, no attempt commits, or the watermark is wrong.
///
/// # Panics
///
/// If a racing thread panics (a bug in the backend under test, never a
/// verdict).
pub fn racing_drains<T: LakeCommitter + Sync>(
    a: &T,
    b: &T,
    materialize: &mut dyn PartMaterializer,
    partition: &str,
) -> Result<(), ConformanceError> {
    let check = "racing_drains_single_winner";
    for committer in [&a, &b] {
        block_on(committer.evolve_schema(suite_schema()))
            .map_err(|e| fail(check, format!("evolve_schema failed: {e}")))?;
    }
    let m = manifest(partition, 0, 1, 10);
    for part in &m.parts {
        materialize
            .materialize(part)
            .map_err(ConformanceError::Harness)?;
    }
    let row = watermark(partition, 1_000);
    let barrier = Barrier::new(2);
    let outcomes: Vec<Result<CommitOutcome, String>> = std::thread::scope(|scope| {
        let ja = scope.spawn(|| {
            barrier.wait();
            block_on(a.commit_files(m.clone(), vec![row.clone()])).map_err(|e| e.to_string())
        });
        let jb = scope.spawn(|| {
            barrier.wait();
            block_on(b.commit_files(m.clone(), vec![row.clone()])).map_err(|e| e.to_string())
        });
        vec![ja.join().expect("no panic"), jb.join().expect("no panic")]
    });

    let mut committed = 0usize;
    for outcome in &outcomes {
        match outcome {
            Ok(CommitOutcome::Committed) => committed += 1,
            Ok(CommitOutcome::Aborted) => {}
            Ok(CommitOutcome::Indeterminate) => {
                return Err(fail(
                    check,
                    "a racing commit returned Indeterminate; the fence must resolve races \
                     definitively (§6.6)",
                ));
            }
            Err(e) => return Err(fail(check, format!("a racing commit errored: {e}"))),
        }
    }
    if committed == 0 {
        return Err(fail(check, "no racing attempt committed"));
    }
    // The loser's read-back path (§6.5): the standing watermark is visible
    // through either committer.
    for committer in [&a, &b] {
        let rows = block_on(committer.read_watermarks(vec![PartitionId::new(partition)]))
            .map_err(|e| fail(check, format!("read-back failed: {e}")))?;
        let read = rows
            .iter()
            .find(|r| r.partition.as_str() == partition)
            .map(|r| r.complete_through_ms);
        if read != Some(1_000) {
            return Err(fail(
                check,
                format!("read-back saw {read:?}, expected 1000"),
            ));
        }
    }
    Ok(())
}
