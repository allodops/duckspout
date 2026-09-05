//! Fault-injection-only test seam (§8.4, issue #203): a [`LakeCommitter`]
//! decorator that stalls before delegating `commit_files` to the real
//! committer — nothing else.
//!
//! # Why this exists
//!
//! §8.4's sharpest node-kill fault window is "the partition owner mid-drain,
//! between `PutPart` and `LakeCommit` — the window where `SingleDrainCommit`
//! and `TakeoverDrain` are both live." `duckspout-drain`'s own choreography
//! (`crates/duckspout-drain/src/coordinator.rs::drain_window`) journals
//! `PutPart` and then calls `LakeCommitter::commit_files` on the very next
//! line — with no delay, landing a real `SIGKILL` inside that window from
//! outside the process (`duckspout-fleet`'s injector, watching the node's
//! own NDJSON journal for the `PutPart` line) would be racing however fast
//! the real backend's commit happens to complete, which is not a
//! deterministic fault to schedule.
//!
//! This decorator widens that window on demand: when
//! `--fault-drain-commit-delay-ms` is passed a non-zero value,
//! `duckspout-daemon` wraps its real [`LakeCommitter`] in a
//! [`StallingLakeCommitter`] before handing it to `DrainCoordinator`
//! (`wiring.rs::Daemon::boot`) — every OTHER commit-side call (`SealPart`,
//! `PutPart`'s own PUT, everything the coordinator does before
//! `commit_files`) is untouched, so the fault only ever widens the exact
//! window §8.4 names, never any other step of the choreography.
//!
//! # Why this is safe to ship in production code (not test-only-cfg'd)
//!
//! - **Off by construction in every real deployment.** The daemon's own CLI
//!   default is `0` (`Duration::ZERO`), and [`StallingLakeCommitter::commit_files`]
//!   special-cases a zero delay into a plain pass-through with no
//!   `tokio::time::sleep` call at all — a production boot that never passes
//!   `--fault-drain-commit-delay-ms` behaves identically to one running the
//!   unwrapped committer directly.
//! - **Not a protocol-crate concern.** This lives in `duckspout-daemon`
//!   (a composition-root/bin-adjacent library crate, not one of the
//!   layered protocol crates `AGENTS.md` names) and is wired entirely at
//!   `wiring.rs::Daemon::boot` — `duckspout-drain` itself gains no new
//!   dependency, no new time source, and no awareness that a fault harness
//!   exists (R-determinism's "ports only" rule is about protocol crates;
//!   this crate already owns the one concrete [`Clock`] implementation and
//!   every other real-time/real-network wiring decision for the daemon).
//! - **Scoped to exactly one port method.** [`StallingLakeCommitter`]
//!   delegates every other [`LakeCommitter`] method
//!   (`replace_files`/`evolve_schema`/`expire`/`read_watermarks`/
//!   `attach_info`) straight through, unmodified — it cannot silently widen
//!   any window besides the one it names.
//!
//! [`Clock`]: duckspout_types::Clock

use std::sync::Arc;
use std::time::Duration;

use duckspout_types::{
    AttachInfo, BoxFuture, CommitOutcome, LakeCommitter, LakeError, PartName, PartitionId,
    SchemaEvolution, WatermarkRow, WindowManifest,
};

/// Wraps a real [`LakeCommitter`], stalling `commit_files` for `delay`
/// before delegating — module docs above for why, and why a zero delay is
/// an exact pass-through (module docs' safety argument depends on this).
pub struct StallingLakeCommitter {
    inner: Arc<dyn LakeCommitter>,
    delay: Duration,
}

impl StallingLakeCommitter {
    /// Wraps `inner`, stalling `commit_files` by `delay` before delegating.
    /// `delay = Duration::ZERO` is a legitimate, cheap no-op configuration
    /// (module docs) — callers need not special-case it themselves.
    #[must_use]
    pub fn new(inner: Arc<dyn LakeCommitter>, delay: Duration) -> Self {
        Self { inner, delay }
    }
}

impl LakeCommitter for StallingLakeCommitter {
    /// The one stalled method (module docs): sleeps `delay`, then delegates
    /// unchanged. A zero delay never calls `tokio::time::sleep` at all —
    /// the exact pass-through this decorator's safety argument rests on.
    fn commit_files(
        &self,
        manifest: WindowManifest,
        watermarks: Vec<WatermarkRow>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        let inner = Arc::clone(&self.inner);
        let delay = self.delay;
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            inner.commit_files(manifest, watermarks).await
        })
    }

    fn replace_files(
        &self,
        remove: Vec<PartName>,
        add: Vec<PartName>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        self.inner.replace_files(remove, add)
    }

    fn evolve_schema(&self, change: SchemaEvolution) -> BoxFuture<'_, Result<(), LakeError>> {
        self.inner.evolve_schema(change)
    }

    fn expire(&self, parts: Vec<PartName>) -> BoxFuture<'_, Result<(), LakeError>> {
        self.inner.expire(parts)
    }

    fn read_watermarks(
        &self,
        partitions: Vec<PartitionId>,
    ) -> BoxFuture<'_, Result<Vec<WatermarkRow>, LakeError>> {
        self.inner.read_watermarks(partitions)
    }

    fn attach_info(&self) -> BoxFuture<'_, Result<AttachInfo, LakeError>> {
        self.inner.attach_info()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use duckspout_types::{DatasetId, PartitionId as Partition, WindowId};

    use super::*;

    /// A [`LakeCommitter`] double recording call counts and always
    /// returning [`CommitOutcome::Committed`] — enough to observe whether
    /// [`StallingLakeCommitter`] delegated a call and, for `commit_files`,
    /// how long it took to do so.
    #[derive(Default)]
    #[allow(clippy::struct_field_names)] // the shared `_calls` suffix is the clearest name for a per-method call counter, not accidental repetition
    struct RecordingCommitter {
        commit_files_calls: AtomicUsize,
        replace_files_calls: AtomicUsize,
        evolve_schema_calls: AtomicUsize,
        expire_calls: AtomicUsize,
        read_watermarks_calls: AtomicUsize,
        attach_info_calls: AtomicUsize,
    }

    fn manifest() -> WindowManifest {
        WindowManifest {
            dataset: DatasetId::new("otlp_logs"),
            partition: Partition::new("p0"),
            window_id: WindowId(0),
            origin_coverage: Vec::new(),
            rows: 0,
            event_time_min_ms: 0,
            event_time_max_ms: 0,
            dedup_removed: 0,
            parts: Vec::new(),
        }
    }

    impl LakeCommitter for RecordingCommitter {
        fn commit_files(
            &self,
            _manifest: WindowManifest,
            _watermarks: Vec<WatermarkRow>,
        ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
            self.commit_files_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(CommitOutcome::Committed)))
        }

        fn replace_files(
            &self,
            _remove: Vec<PartName>,
            _add: Vec<PartName>,
        ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
            self.replace_files_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(CommitOutcome::Committed)))
        }

        fn evolve_schema(&self, _change: SchemaEvolution) -> BoxFuture<'_, Result<(), LakeError>> {
            self.evolve_schema_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(())))
        }

        fn expire(&self, _parts: Vec<PartName>) -> BoxFuture<'_, Result<(), LakeError>> {
            self.expire_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(())))
        }

        fn read_watermarks(
            &self,
            _partitions: Vec<PartitionId>,
        ) -> BoxFuture<'_, Result<Vec<WatermarkRow>, LakeError>> {
            self.read_watermarks_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(Vec::new())))
        }

        fn attach_info(&self) -> BoxFuture<'_, Result<AttachInfo, LakeError>> {
            self.attach_info_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(AttachInfo {
                catalog_uri: "postgres://test".to_owned(),
                credentials_shape: "none".to_owned(),
                dialect: "duckdb".to_owned(),
            })))
        }
    }

    /// A zero delay is an exact pass-through: `commit_files` still reaches
    /// the inner committer, and does so without measurably sleeping. Would
    /// catch a refactor that always slept, even at `Duration::ZERO`
    /// (defeating this decorator's whole "off in production" safety
    /// argument — module docs).
    #[tokio::test]
    async fn zero_delay_delegates_commit_files_without_sleeping() {
        let inner = Arc::new(RecordingCommitter::default());
        let stalling = StallingLakeCommitter::new(
            Arc::clone(&inner) as Arc<dyn LakeCommitter>,
            Duration::ZERO,
        );

        let started = Instant::now();
        let outcome = stalling.commit_files(manifest(), Vec::new()).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(outcome, CommitOutcome::Committed);
        assert_eq!(inner.commit_files_calls.load(Ordering::SeqCst), 1);
        assert!(
            elapsed < Duration::from_millis(50),
            "a zero delay must not sleep at all, took {elapsed:?}"
        );
    }

    /// A non-zero delay actually stalls before delegating — the whole point
    /// of this decorator (module docs' "deterministically wide window"
    /// claim). Would catch a delay that was stored but never applied.
    #[tokio::test]
    async fn nonzero_delay_actually_stalls_before_delegating() {
        let inner = Arc::new(RecordingCommitter::default());
        let delay = Duration::from_millis(80);
        let stalling =
            StallingLakeCommitter::new(Arc::clone(&inner) as Arc<dyn LakeCommitter>, delay);

        let started = Instant::now();
        stalling.commit_files(manifest(), Vec::new()).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(inner.commit_files_calls.load(Ordering::SeqCst), 1);
        assert!(
            elapsed >= delay,
            "expected at least {delay:?} elapsed, got {elapsed:?}"
        );
    }

    /// Every OTHER `LakeCommitter` method is a plain, unstalled
    /// pass-through — module docs' "scoped to exactly one port method"
    /// claim. Would catch a copy-paste that accidentally stalled (or
    /// dropped) one of these.
    #[tokio::test]
    async fn every_other_method_is_an_unstalled_pass_through() {
        let inner = Arc::new(RecordingCommitter::default());
        let stalling = StallingLakeCommitter::new(
            Arc::clone(&inner) as Arc<dyn LakeCommitter>,
            Duration::from_secs(999),
        );

        let started = Instant::now();
        stalling
            .replace_files(Vec::new(), Vec::new())
            .await
            .unwrap();
        stalling
            .evolve_schema(SchemaEvolution {
                dataset: DatasetId::new("otlp_logs"),
                columns: vec![duckspout_types::ColumnSpec {
                    name: "extra".to_owned(),
                    logical_type: "string".to_owned(),
                }],
            })
            .await
            .unwrap();
        stalling.expire(Vec::new()).await.unwrap();
        stalling.read_watermarks(Vec::new()).await.unwrap();
        stalling.attach_info().await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(inner.replace_files_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.evolve_schema_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.expire_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.read_watermarks_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.attach_info_calls.load(Ordering::SeqCst), 1);
        assert!(
            elapsed < Duration::from_secs(1),
            "non-commit_files methods must never observe the configured delay, took {elapsed:?}"
        );
    }
}
