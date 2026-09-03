//! The drain choreography (§6.2–§6.6): `SealPart` → `PutPart` →
//! `LakeCommit`, with the three-outcome discipline and the
//! `SingleDrainCommit` fence sequenced above the port.
//!
//! # The seams (ADR-0008, ADR-0010)
//!
//! Drain, staging, and watermark are all protocol crates, so this crate
//! touches its neighbors only through `duckspout-types` ports:
//!
//! - **[`SealSurface`]** — staging's seal-side read surface: enumerate
//!   closed windows, run the one sorted deduplicating `COPY` (§6.2), and
//!   `DropWindow` after a durable commit (§6.9).
//! - **[`WatermarkBookkeeping`]** — the watermark ledger's computation seam
//!   (ADR-0010: the lake *stores* the watermark, the ledger crate
//!   *computes* it): `advance_for` yields the rows `commit_files` carries;
//!   `record_commit` is post-commit bookkeeping; `next_window` is the
//!   dense-next fence check.
//! - **[`LakeCommitter`]** — the lake-agnosticism boundary (§6.4); this
//!   crate never sees a concrete backend.
//! - [`object_store`] — `PutPart`'s one PUT (§6.1); [`Storage`] — the
//!   node-local scratch the sealed bytes travel through; [`Clock`] — the
//!   §6.3 lateness hold (D-2: no direct time).
//!
//! # Retries never replay blindly (R-2; PR #126's ACPR note)
//!
//! The retry path detects "the commit already stands" through evidence,
//! **never** by re-recording or resubmitting on faith:
//!
//! - *Before* committing, the dense-next check
//!   ([`WatermarkBookkeeping::next_window`]) short-circuits a window whose
//!   commit this process already recorded — the §6.9
//!   crash-between-commit-and-cleanup recovery completes the pending
//!   `DropWindow` instead.
//! - *After* an `Aborted` or `Indeterminate` outcome, **exactly one**
//!   read-back ([`LakeCommitter::read_watermarks`]) decides whether the
//!   commit stands (§6.5). A bookkeeping `WindowNotNext` with `got <
//!   expected` during the finish is likewise read as "already recorded",
//!   never re-recorded.
//!
//! # Boot recovery obligation
//!
//! The bookkeeping port must reflect the lake's committed state when drains
//! resume (rebuild-from-manifests, §6.8): a ledger stale about a *landed*
//! commit would let a retry reseal and overwrite a registered object's
//! bytes. The daemon's recovery composition owns that rebuild; this crate
//! assumes it.

use std::sync::Arc;

use duckspout_lake_contract::LakeCommitter;
use duckspout_types::{
    Clock, CommitOutcome, DatasetDeclaration, DatasetId, DatasetKind, DrainableWindow, DropOutcome,
    LakeError, LedgerRejection, PartitionId, SealError, SealRequest, SealSurface, SealedPart,
    Storage, StorageError, StoragePath, TraceEvent, TraceSink, WatermarkBookkeeping, WatermarkRow,
    WindowId, WindowManifest,
};
use object_store::ObjectStoreExt as _;

use crate::naming::{PartDiscriminator, part_name};
use crate::schedule::{self, DrainConfig};

/// How one drain attempt ended. Every variant leaves the system in a state
/// a later attempt (or none) handles correctly — the choreography has no
/// "unknown" terminal state: unknowns are resolved by the one read-back or
/// parked as [`DrainOutcome::Requeue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainOutcome {
    /// This attempt's commit is durable: files registered, watermark
    /// advanced (§6.5), bookkeeping recorded, and the coverage-guarded
    /// `DropWindow` done (§6.9; TN-32) — rows outside the committed
    /// coverage (late arrivals after the seal `COPY`) stay in staging as
    /// supplement input.
    Committed {
        /// The partition's watermark row after the commit — `None` while
        /// the partition has no provable watermark.
        watermark: Option<WatermarkRow>,
    },
    /// The window's commit already stands — recorded earlier by this
    /// process, or committed by another drainer and proven by read-back.
    /// Local completion (`DropWindow`) has been performed; nothing was
    /// double-committed.
    AlreadyCommitted,
    /// The attempt did not commit and the window stays in staging, intact
    /// (R-5). Retry-safe by construction: a retry recomputes the full
    /// choreography (R-2) under the same deterministic part name, and the
    /// port's check-before-register contract (§6.5) plus the §6.6 fence
    /// make the re-attempt idempotent.
    Requeue(RequeueReason),
}

/// Why a window was requeued. All reasons are retryable; they differ in
/// what is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueReason {
    /// The backend definitively rejected the commit and the read-back
    /// confirmed nothing landed (§6.5 Aborted): a transient rejection
    /// (e.g. serialization failure) — nothing changed.
    RejectedByBackend,
    /// The outcome was Indeterminate and the one read-back proved the
    /// commit did not land: the retry may proceed without any risk of
    /// double-registration.
    CommitLost,
    /// The one read-back could not prove the outcome either way (the
    /// commit carried no strict watermark advance to observe). The retry
    /// is still safe — deterministic naming plus check-before-register
    /// resolve it at the backend (§6.5). A *persistently* unprovable
    /// window signals bookkeeping divergence, which the §6.8
    /// rebuild-from-manifests recovery resolves.
    Unproven,
    /// The read-back itself failed: the catalog is unreachable. That is a
    /// drain stall on the overload ladder (§6.5), not a new state — the
    /// window waits.
    CatalogUnavailable,
}

/// A drain-attempt failure. Transient lake outcomes are **not** errors —
/// they fold into [`DrainOutcome`]; every variant here is a broken input,
/// a broken port, or a broken composition.
#[derive(Debug, thiserror::Error)]
pub enum DrainError {
    /// The window is ahead of the bookkeeping's dense-next window — the
    /// scheduler offered windows out of order (§6.8: contiguity is
    /// load-bearing). Fail closed; draining it would tear the sequence.
    #[error("window {got} of {partition} is ahead of the dense-next window {expected}")]
    WindowAhead {
        /// The partition whose ordering was violated.
        partition: PartitionId,
        /// The dense-next window the bookkeeping expects.
        expected: WindowId,
        /// The window that was offered.
        got: WindowId,
    },
    /// The seal surface failed (§6.2). The window is untouched.
    #[error(transparent)]
    Seal(#[from] SealError),
    /// The bookkeeping rejected the manifest as malformed — a bug upstream
    /// of the commit; nothing was committed under it (R-3).
    #[error(transparent)]
    Ledger(#[from] LedgerRejection),
    /// The node-local scratch failed while moving sealed bytes.
    #[error(transparent)]
    Scratch(#[from] StorageError),
    /// The cold object store refused the PUT.
    #[error(transparent)]
    Put(#[from] object_store::Error),
    /// A backend-invariant lake failure (misconfiguration — never a
    /// transient commit outcome, §6.5).
    #[error(transparent)]
    Lake(#[from] LakeError),
}

/// The per-dataset seal parameters, derived from the dataset's declaration
/// (§2, §6.2) by the composition: `order_by` from `sort_key` (default:
/// the event-time column; changelog parts `(key_cols, origin, seq)`),
/// `dedup_key` from the dataset's natural or declared key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetDrainPlan {
    /// The sealed part's sort order (§6.2).
    pub order_by: Vec<String>,
    /// The event-time column feeding the manifest's min/max statistics.
    pub event_time_column: String,
    /// Drain-time dedup key; `None` seals every row (§6.2).
    pub dedup_key: Option<Vec<String>>,
}

impl DatasetDrainPlan {
    /// Derive a plan from a dataset's declaration (§2.1) plus its schema's
    /// event-time column (§2.1's `key_cols`/`sort_key` are the declaration's
    /// closed attribute set; `event_time_column` is schema-level — e.g.
    /// `otlp_logs`' fixed `ts` — not one of the three, so it is supplied by
    /// the caller rather than read off `decl`).
    ///
    /// Implements drain.md §2's per-kind defaults verbatim: `event` sorts by
    /// the event-time column with no dedup; `changelog` sorts
    /// `(key_cols, origin, seq)` key-clustered and dedups keep-latest on
    /// `key_cols`. An explicit `sort_key` overrides the default order for
    /// either kind; dedup stays kind-determined (only `changelog` declares a
    /// key to dedup on).
    pub fn from_declaration(
        decl: &DatasetDeclaration,
        event_time_column: impl Into<String>,
    ) -> Self {
        let event_time_column = event_time_column.into();
        let (default_order_by, dedup_key) = match decl.kind {
            DatasetKind::Event => (vec![event_time_column.clone()], None),
            DatasetKind::Changelog => {
                let mut order_by = decl.key_cols.clone();
                order_by.push("origin".to_owned());
                order_by.push("seq".to_owned());
                (order_by, Some(decl.key_cols.clone()))
            }
        };
        DatasetDrainPlan {
            order_by: decl.sort_key.clone().unwrap_or(default_order_by),
            event_time_column,
            dedup_key,
        }
    }
}

/// The per-node drain driver: schedules eligible windows and runs the
/// `SealPart` → `PutPart` → `LakeCommit` choreography for one window at a
/// time, entirely through ports.
pub struct DrainCoordinator {
    seal: Arc<dyn SealSurface>,
    ledger: Arc<dyn WatermarkBookkeeping>,
    committer: Arc<dyn LakeCommitter>,
    parts_store: Arc<dyn object_store::ObjectStore>,
    scratch: Arc<dyn Storage>,
    clock: Arc<dyn Clock>,
    config: DrainConfig,
    /// The §3.7 capture seam: this crate's §3.3 events (`SealPart`,
    /// `PutPart`, the `LakeCommit*` outcome names, `Reconcile`,
    /// `DropWindow`) journal here (docs/trace-mapping.md's attributions).
    /// `None` — the production default until the `conformance` row arms —
    /// journals nothing.
    trace: Option<Arc<dyn TraceSink>>,
}

impl DrainCoordinator {
    /// Wires the ports. `scratch` must be rooted where the seal surface
    /// writes its scratch files (the composition roots both at the hot
    /// volume).
    #[must_use]
    pub fn new(
        seal: Arc<dyn SealSurface>,
        ledger: Arc<dyn WatermarkBookkeeping>,
        committer: Arc<dyn LakeCommitter>,
        parts_store: Arc<dyn object_store::ObjectStore>,
        scratch: Arc<dyn Storage>,
        clock: Arc<dyn Clock>,
        config: DrainConfig,
    ) -> Self {
        Self {
            seal,
            ledger,
            committer,
            parts_store,
            scratch,
            clock,
            config,
            trace: None,
        }
    }

    /// Journals this coordinator's §3.3 events through `sink` (§3.7; the
    /// trace-conformance harness's capture side).
    #[must_use]
    pub fn with_trace_sink(mut self, sink: Arc<dyn TraceSink>) -> Self {
        self.trace = Some(sink);
        self
    }

    /// Journals `event` when a sink is wired (§3.7).
    fn journal(&self, event: TraceEvent) {
        if let Some(trace) = &self.trace {
            trace.record(event);
        }
    }

    /// The windows that are drain-eligible **now**: closed (the surface
    /// offers nothing else) and past the §6.3 lateness hold, judged
    /// against the [`Clock`] port.
    ///
    /// # Errors
    ///
    /// [`DrainError::Seal`] if the surface cannot enumerate.
    pub async fn eligible_windows(&self) -> Result<Vec<DrainableWindow>, DrainError> {
        let offered = self.seal.drainable_windows().await?;
        Ok(schedule::eligible(
            self.clock.wall_unix_ms(),
            self.config,
            offered,
        ))
    }

    /// Drains one window end to end. See the module docs for the seam map
    /// and the retry discipline; the sequence is:
    ///
    /// 1. **Fence pre-check** — the window must be the bookkeeping's
    ///    dense-next window. Below it: the commit already stands, complete
    ///    the pending `DropWindow` (§6.9 crash recovery). Above it: fail
    ///    closed ([`DrainError::WindowAhead`]).
    /// 2. **`SealPart`** (§6.2) through the seal surface.
    /// 3. **Manifest + watermark rows** —
    ///    [`WatermarkBookkeeping::advance_for`] computes what
    ///    `commit_files` carries (§6.4).
    /// 4. **`PutPart`** (§6.1): one PUT of the sealed bytes at the
    ///    deterministic name (§6.5).
    /// 5. **`LakeCommit`** with the three-outcome discipline (§6.5):
    ///    `Committed` → record + `DropWindow`; `Aborted` / `Indeterminate`
    ///    → exactly one read-back, then reconcile — never a blind retry.
    ///
    /// # Errors
    ///
    /// See [`DrainError`]; transient lake outcomes are returned as
    /// [`DrainOutcome::Requeue`], not errors.
    pub async fn drain_window(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
        plan: &DatasetDrainPlan,
    ) -> Result<DrainOutcome, DrainError> {
        // 1. Fence pre-check (SingleDrainCommit above the port, ADR-0010).
        let expected = self.ledger.next_window(partition);
        if window.0 < expected.0 {
            return self
                .finish_already_committed(dataset, partition, window, None)
                .await;
        }
        if window.0 > expected.0 {
            return Err(DrainError::WindowAhead {
                partition: partition.clone(),
                expected,
                got: window,
            });
        }
        let prior = self.ledger.complete_through_ms(partition);

        // 2. SealPart: one sorted, deduplicating COPY through the surface.
        let sealed = self
            .seal
            .seal_window(SealRequest {
                dataset: dataset.clone(),
                partition: partition.clone(),
                window,
                order_by: plan.order_by.clone(),
                event_time_column: plan.event_time_column.clone(),
                dedup_key: plan.dedup_key.clone(),
            })
            .await?;
        // §3.3 SealPart: the one sorted, deduplicating COPY completed.
        self.journal(TraceEvent::SealPart);

        // 3. Manifest + the watermark rows the commit carries (§6.4).
        let part = part_name(dataset, partition, window, &PartDiscriminator::Window);
        let manifest = WindowManifest {
            dataset: dataset.clone(),
            partition: partition.clone(),
            window_id: window,
            origin_coverage: sealed.origin_coverage.clone(),
            rows: sealed.rows,
            event_time_min_ms: sealed.event_time_min_ms,
            event_time_max_ms: sealed.event_time_max_ms,
            dedup_removed: sealed.dedup_removed,
            parts: vec![part.clone()],
        };
        let previewed = match self.ledger.advance_for(&manifest) {
            Ok(row) => row,
            // Recorded concurrently in this process between the pre-check
            // and here: the commit stands — evidence, not re-record.
            Err(LedgerRejection::WindowNotNext { expected, got, .. }) if got.0 < expected.0 => {
                return self
                    .finish_already_committed(dataset, partition, window, Some(&sealed.path))
                    .await;
            }
            Err(rejection) => return Err(rejection.into()),
        };
        let watermarks: Vec<WatermarkRow> = previewed.clone().into_iter().collect();

        // 4. PutPart: the object's one PUT (§6.1); a retry re-PUTs the same
        // name (idempotent by deterministic naming, §6.5).
        let data = self.scratch.get(sealed.path.clone()).await?;
        let location = object_store::path::Path::from(part.as_str());
        self.parts_store.put(&location, data.into()).await?;
        // §3.3 PutPart: the object's one logical PUT is durable.
        self.journal(TraceEvent::PutPart);

        // 5. LakeCommit, three-valued (§6.5).
        match self
            .committer
            .commit_files(manifest.clone(), watermarks)
            .await?
        {
            CommitOutcome::Committed => {
                // §3.7's outcome-name rule: a commit journals its outcome —
                // LakeCommitOk here, never a bare LakeCommit.
                self.journal(TraceEvent::LakeCommitOk);
                let watermark = self.finish_committed(&manifest, &sealed.path).await?;
                Ok(DrainOutcome::Committed { watermark })
            }
            CommitOutcome::Aborted => {
                self.journal(TraceEvent::LakeCommitAbort);
                self.reconcile(
                    Unsettled::Aborted,
                    &manifest,
                    &sealed,
                    previewed.as_ref(),
                    prior,
                )
                .await
            }
            CommitOutcome::Indeterminate => {
                // One journaled name for both model successors (§3.7); the
                // following Reconcile names the resolution.
                self.journal(TraceEvent::LakeCommitIndeterminate);
                self.reconcile(
                    Unsettled::Indeterminate,
                    &manifest,
                    &sealed,
                    previewed.as_ref(),
                    prior,
                )
                .await
            }
        }
    }

    /// The §6.5 resolution of an unsettled outcome: **exactly one**
    /// read-back, then reconcile. Blind resubmission is forbidden (R-2);
    /// unbounded read-back loops equally — an unreachable catalog is a
    /// drain stall, not a loop.
    async fn reconcile(
        &self,
        unsettled: Unsettled,
        manifest: &WindowManifest,
        sealed: &SealedPart,
        previewed: Option<&WatermarkRow>,
        prior: Option<i64>,
    ) -> Result<DrainOutcome, DrainError> {
        let partition = &manifest.partition;
        let Ok(rows) = self
            .committer
            .read_watermarks(vec![partition.clone()])
            .await
        else {
            self.discard_scratch(&sealed.path).await;
            return Ok(DrainOutcome::Requeue(RequeueReason::CatalogUnavailable));
        };
        let read_back = rows
            .iter()
            .find(|row| row.partition == *partition)
            .map(|row| row.complete_through_ms);
        // §3.3 Reconcile: the ONE read-back resolving an Indeterminate
        // outcome succeeded (whatever it decides below). An Aborted
        // outcome's read-back is not the model's Reconcile — the model's
        // abort is terminal — so it journals nothing; a failed read-back
        // resolved nothing and journals nothing either.
        if matches!(unsettled, Unsettled::Indeterminate) {
            self.journal(TraceEvent::Reconcile);
        }

        // The commit is observable through the watermark only when it
        // carried a strict advance: previewed strictly above the
        // bookkeeping's pre-commit value.
        let strict_advance = previewed
            .map(|row| row.complete_through_ms)
            .filter(|advanced| prior.is_none_or(|p| *advanced > p));
        match strict_advance {
            Some(advanced) if read_back.is_some_and(|v| v >= advanced) => {
                // Landed: the commit stands — ours, or an identical racing
                // commit under the same fence key (the watermark is
                // monotone and only this window's commit reaches
                // `advanced`, §6.8 dense contiguity).
                match unsettled {
                    Unsettled::Indeterminate => {
                        let watermark = self.finish_committed(manifest, &sealed.path).await?;
                        Ok(DrainOutcome::Committed { watermark })
                    }
                    Unsettled::Aborted => {
                        // The fence aborted us because another attempt's
                        // commit stands (§6.6: the loser yields).
                        self.finish_committed(manifest, &sealed.path).await?;
                        Ok(DrainOutcome::AlreadyCommitted)
                    }
                }
            }
            Some(_) => {
                // Lost: the watermark never reached the commit's advance,
                // so the commit did not land anywhere.
                self.discard_scratch(&sealed.path).await;
                Ok(DrainOutcome::Requeue(match unsettled {
                    Unsettled::Aborted => RequeueReason::RejectedByBackend,
                    Unsettled::Indeterminate => RequeueReason::CommitLost,
                }))
            }
            None => {
                self.discard_scratch(&sealed.path).await;
                Ok(DrainOutcome::Requeue(RequeueReason::Unproven))
            }
        }
    }

    /// Completes a durable commit locally: record the manifest (unless the
    /// bookkeeping already holds it — detected via the dense-next evidence,
    /// never blind, PR #126's ACPR note), discard the scratch bytes, and
    /// `DropWindow` (§6.9). Crash-safe: every step is idempotent and a
    /// re-attempt lands back here through the fence pre-check.
    async fn finish_committed(
        &self,
        manifest: &WindowManifest,
        scratch: &StoragePath,
    ) -> Result<Option<WatermarkRow>, DrainError> {
        let watermark = match self.ledger.record_commit(manifest) {
            Ok(row) => row,
            Err(LedgerRejection::WindowNotNext { expected, got, .. }) if got.0 < expected.0 => {
                // Already recorded (an in-process racer won the record):
                // the current bookkeeping row is the answer.
                self.ledger
                    .complete_through_ms(&manifest.partition)
                    .map(|complete_through_ms| WatermarkRow {
                        partition: manifest.partition.clone(),
                        complete_through_ms,
                    })
            }
            Err(rejection) => return Err(rejection.into()),
        };
        self.discard_scratch(scratch).await;
        // TN-32 (PR #137): the drop is coverage-guarded — only rows this
        // commit's coverage accounts for may leave staging; a late arrival
        // that landed after the seal COPY is kept as residue for the
        // supplement path (§6.6).
        let dropped = self
            .seal
            .drop_window(
                manifest.dataset.clone(),
                manifest.partition.clone(),
                manifest.window_id,
                manifest.origin_coverage.clone(),
            )
            .await?;
        // §3.3 DropWindow: journaled when covered rows actually left
        // staging (Dropped, or ResidueKept's covered subset). AlreadyGone
        // dropped nothing, and an empty coverage authorizes no drop —
        // neither is a §3.3 step.
        if !manifest.origin_coverage.is_empty() && !matches!(dropped, DropOutcome::AlreadyGone) {
            self.journal(TraceEvent::DropWindow);
        }
        Ok(watermark)
    }

    /// The §6.9 completion path for a window whose commit already stands:
    /// no seal, no PUT, no commit — just the pending local cleanup, guarded
    /// by the coverage the bookkeeping recorded for the window (TN-32: with
    /// no manifest at hand, the recorded coverage is the drop authority;
    /// when even that is unavailable, nothing is dropped — refusing to drop
    /// is always the safe direction, R-5).
    async fn finish_already_committed(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
        scratch: Option<&StoragePath>,
    ) -> Result<DrainOutcome, DrainError> {
        if let Some(path) = scratch {
            self.discard_scratch(path).await;
        }
        let covered = self
            .ledger
            .recorded_coverage(partition, window)
            .unwrap_or_default();
        let authorized = !covered.is_empty();
        let dropped = self
            .seal
            .drop_window(dataset.clone(), partition.clone(), window, covered)
            .await?;
        if authorized && !matches!(dropped, DropOutcome::AlreadyGone) {
            self.journal(TraceEvent::DropWindow);
        }
        Ok(DrainOutcome::AlreadyCommitted)
    }

    /// Best-effort scratch cleanup: the scratch file is derivable (a retry
    /// reseals), so failure to delete is never worth failing a drain over.
    async fn discard_scratch(&self, path: &StoragePath) {
        let _ = self.scratch.delete(path.clone()).await;
    }
}

/// The two unsettled §6.5 outcomes reconcile resolves.
#[derive(Debug, Clone, Copy)]
enum Unsettled {
    Aborted,
    Indeterminate,
}

#[cfg(test)]
mod drain_plan_tests {
    use super::*;

    fn event_decl() -> DatasetDeclaration {
        DatasetDeclaration {
            dataset: DatasetId::new("otlp_logs"),
            kind: DatasetKind::Event,
            key_cols: Vec::new(),
            sort_key: None,
        }
    }

    fn changelog_decl() -> DatasetDeclaration {
        DatasetDeclaration {
            dataset: DatasetId::new("accounts"),
            kind: DatasetKind::Changelog,
            key_cols: vec!["account_id".to_owned()],
            sort_key: None,
        }
    }

    #[test]
    fn event_default_sorts_by_event_time_and_never_dedups() {
        let plan = DatasetDrainPlan::from_declaration(&event_decl(), "ts");
        assert_eq!(plan.order_by, vec!["ts".to_owned()]);
        assert_eq!(plan.event_time_column, "ts");
        assert_eq!(plan.dedup_key, None);
    }

    #[test]
    fn changelog_default_sorts_key_clustered_and_dedups_on_key_cols() {
        let plan = DatasetDrainPlan::from_declaration(&changelog_decl(), "updated_at");
        assert_eq!(
            plan.order_by,
            vec![
                "account_id".to_owned(),
                "origin".to_owned(),
                "seq".to_owned()
            ]
        );
        assert_eq!(plan.event_time_column, "updated_at");
        assert_eq!(plan.dedup_key, Some(vec!["account_id".to_owned()]));
    }

    #[test]
    fn explicit_sort_key_overrides_the_kind_default_for_either_kind() {
        let mut decl = changelog_decl();
        decl.sort_key = Some(vec!["region".to_owned(), "account_id".to_owned()]);
        let plan = DatasetDrainPlan::from_declaration(&decl, "updated_at");
        assert_eq!(
            plan.order_by,
            vec!["region".to_owned(), "account_id".to_owned()]
        );
        // Dedup stays kind-determined even when sort_key is overridden.
        assert_eq!(plan.dedup_key, Some(vec!["account_id".to_owned()]));
    }
}
