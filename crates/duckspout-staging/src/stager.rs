//! [`EngineStager`]: the [`StageCommitter`] port over the WAL=hot engine
//! (§4.3, ADR-0008).
//!
//! The port trait lives in `duckspout-types` (a port consumed across the
//! accept↔staging boundary must — §10.1's layering); this crate owns the
//! implementation, which is exactly the composition of three staging-side
//! decisions the accept path must never make itself:
//!
//! - **Partitioning** (§2.2): the partition key is `(tenant_id, shard)`;
//!   v1 `event` datasets have a single shard, so every batch of a tenant
//!   lands in [`PartitionId::from_tenant_shard`]`(tenant, 0)`.
//! - **Windowing** (§2.3): ingest lands in the partition's *current*
//!   micro-window; the window rolls when `hot.window` of arrival time has
//!   elapsed, measured on the [`Clock`] port (D-2 — never a direct clock).
//!   Window ids are allocated dense and never reused, backed by the
//!   engine's persistent high-water ([`StagingEngine::highest_window_id`]).
//! - **The `StageCommit` transaction** (§4.3): all record batches of one
//!   decoded batch are appended and committed atomically; the returned
//!   [`StagedCoverage`] is the ack evidence.
//!
//! Blocking discipline: like the engine it wraps, [`EngineStager`] blocks
//! (the commit is an fsync). The port future resolves synchronously;
//! callers embed the port off their reactor (the daemon composes it behind
//! `spawn_blocking` — ADR-0003's seam), exactly as the engine's module docs
//! prescribe.
//!
//! The dedup-window entry of §4.4.1 rides this same transaction when issue
//! #33 lands; this type is where that work plugs in.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::ipc::reader::StreamReader;
use duckspout_types::{
    BoxFuture, Clock, DatasetId, DecodedBatch, PartitionId, StageCommitter, StageError,
    StagedCoverage, Storage, WindowId,
};

use crate::engine::{StagingEngine, StagingError};

/// One partition's currently-open micro-window, by arrival time.
#[derive(Debug, Clone, Copy)]
struct OpenWindow {
    id: WindowId,
    /// [`Clock::monotonic_nanos`] at which this window opened.
    opened_at: u64,
}

/// The [`StageCommitter`] port over a [`StagingEngine`]: partition
/// assignment, arrival-time window rolling, and the durable `StageCommit`
/// transaction (module docs).
pub struct EngineStager<S: Storage, C: Clock> {
    engine: Arc<StagingEngine<S>>,
    clock: C,
    /// `hot.window` in nanoseconds of [`Clock::monotonic_nanos`] time.
    window_nanos: u64,
    /// The open window per (dataset, partition). In-memory only: after a
    /// restart the roller opens a fresh window (dense, past the persistent
    /// high-water) rather than guessing how much of the old one's span
    /// remains — a shorter first window is always legal (§2.3), a reused
    /// id never is.
    open_windows: Mutex<HashMap<(DatasetId, PartitionId), OpenWindow>>,
}

impl<S: Storage, C: Clock> EngineStager<S, C> {
    /// Wraps `engine` as the [`StageCommitter`] port. `window_nanos` is
    /// `hot.window` (default 60 s) rendered in nanoseconds; `clock` is the
    /// D-2 time port the window roller measures arrival time on.
    #[must_use]
    pub fn new(engine: Arc<StagingEngine<S>>, clock: C, window_nanos: u64) -> Self {
        Self {
            engine,
            clock,
            window_nanos,
            open_windows: Mutex::new(HashMap::new()),
        }
    }

    /// The wrapped engine (the daemon reaches readers and the drain seam
    /// through this).
    #[must_use]
    pub fn engine(&self) -> &Arc<StagingEngine<S>> {
        &self.engine
    }

    /// Stages one decoded batch in one durable `StageCommit` transaction —
    /// the blocking body of [`StageCommitter::stage_commit`] (module docs:
    /// callers embed it off the reactor).
    ///
    /// # Errors
    ///
    /// [`StageError::MalformedRecords`] if the batch's records are not a
    /// decodable Arrow IPC stream; [`StageError::Backend`] if the engine
    /// fails the transaction. Either way nothing is staged and nothing may
    /// be acked (§4.3).
    pub fn stage_blocking(&self, batch: &DecodedBatch) -> Result<Vec<StagedCoverage>, StageError> {
        let partition = PartitionId::from_tenant_shard(&batch.tenant, 0);
        let reader = StreamReader::try_new(batch.records.as_ref(), None)
            .map_err(|error| StageError::MalformedRecords(error.to_string()))?;
        let mut record_batches = Vec::new();
        for record_batch in reader {
            record_batches
                .push(record_batch.map_err(|e| StageError::MalformedRecords(e.to_string()))?);
        }

        let window = self
            .current_window(&batch.dataset, &partition)
            .map_err(|e| backend(&e))?;
        let mut txn = self.engine.begin().map_err(|e| backend(&e))?;
        for record_batch in &record_batches {
            txn.append(&batch.dataset, &partition, window, record_batch)
                .map_err(|e| backend(&e))?;
        }
        txn.commit().map_err(|e| backend(&e))
    }

    /// The partition's current window id, rolling to a freshly allocated
    /// dense id when `hot.window` of arrival time has elapsed (§2.3).
    fn current_window(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
    ) -> Result<WindowId, StagingError> {
        let now = self.clock.monotonic_nanos();
        let mut open_windows = self
            .open_windows
            .lock()
            .map_err(|_| StagingError::WriterPoisoned)?;
        let key = (dataset.clone(), partition.clone());
        if let Some(open) = open_windows.get(&key)
            && now.saturating_sub(open.opened_at) < self.window_nanos
        {
            return Ok(open.id);
        }
        // Roll: allocate strictly past the persistent high-water, so a
        // drained-and-dropped window's id is never reused (§2.3). A window
        // that never committed anything never advanced the high-water and
        // its id is legitimately re-opened.
        let id = WindowId(
            self.engine
                .highest_window_id(dataset, partition)?
                .map_or(0, |w| w.0 + 1),
        );
        open_windows.insert(key, OpenWindow { id, opened_at: now });
        Ok(id)
    }
}

impl<S: Storage, C: Clock> StageCommitter for EngineStager<S, C> {
    fn stage_commit(
        &self,
        batch: DecodedBatch,
    ) -> BoxFuture<'_, Result<Vec<StagedCoverage>, StageError>> {
        // Resolved synchronously by design — the engine blocks on fsync, and
        // the caller owns the off-reactor embedding (module docs).
        let result = self.stage_blocking(&batch);
        Box::pin(std::future::ready(result))
    }
}

fn backend(error: &StagingError) -> StageError {
    StageError::Backend(error.to_string())
}
