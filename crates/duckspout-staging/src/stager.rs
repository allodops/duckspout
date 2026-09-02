//! [`EngineStager`]: the [`StageCommitter`] port over the WAL=hot engine
//! (§4.3–§4.5, ADR-0008).
//!
//! The port trait lives in `duckspout-types` (a port consumed across the
//! accept↔staging boundary must — §10.1's layering); this crate owns the
//! implementation, which is exactly the composition of the staging-side
//! decisions the accept path must never make itself:
//!
//! - **The overload ladder gates admission** (§4.5): before anything is
//!   staged, `M = staged_bytes / hot.max_bytes` decides the rung — rung 2
//!   throttles, rung 3 refuses, both with §4.5's growing retry delay. The
//!   gate runs **before** `begin`, never after: a `StageCommit` already in
//!   flight completes whatever the rung ("the ladder gates admission,
//!   never promises made"). The rung is [`OverloadStatus::from_measure`] —
//!   a pure function, no hysteresis (`LadderMonotone`, §3).
//! - **`DedupCheck`** (§4.4.1): key `(tenant, token)` when the client sent
//!   `x-duckspout-idempotency-key`, else `(tenant, content_hash)` — SHA-256
//!   over the decoded records, tenant scoped because two tenants may
//!   legally send byte-identical bodies. The lookup, the entry insert, and
//!   the §4.4.1 TTL/count-cap eviction all ride the same transaction as
//!   the staged rows. A duplicate of an ack-complete entry replays the
//!   stored outcome (R-2); an unacked entry resolves through the `AtRF`
//!   branch (§3.3) — at RF = 1 trivially satisfied, so it is marked acked
//!   and replayed, never re-staged and never poison.
//! - **Partitioning** (§2.2): [`PartitionId::from_tenant_shard`]`(tenant, 0)`
//!   (v1 `event` datasets are single-shard).
//! - **Windowing** (§2.3): arrival-time rolling on the [`Clock`] port;
//!   window ids dense and never reused, backed by the engine's persistent
//!   high-water ([`StagingEngine::highest_window_id`]).
//!
//! v0.1 honesty notes: at RF = 1 the commit itself completes the ack
//! evidence, so the dedup entry is written `acked = true` in the
//! `StageCommit` transaction (`StageCommit` and `ClientAck` coincide — no
//! second fsync on the ack path); the pre-RF unacked state and the
//! [`duckspout_types::StageOutcome::DuplicateInFlight`] answer become live
//! with replication (v0.2). `drain_stalled` is wired `false` until the
//! drain exists to report a stall — rung labels, not rungs, depend on it.
//!
//! Blocking discipline: like the engine it wraps, [`EngineStager`] blocks
//! (the commit is an fsync). The port future resolves synchronously;
//! callers embed the port off their reactor (the daemon composes it behind
//! `spawn_blocking` — ADR-0003's seam).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::ipc::reader::StreamReader;
use duckspout_types::{
    BoxFuture, Clock, DatasetId, DecodedBatch, NodeStatus, OverloadStatus, PartitionId,
    StageCommitter, StageError, StageOutcome, StagedCoverage, Storage, TenantId, TraceEvent,
    TraceSink, WindowId, throttle_retry_delay_ms,
};
use sha2::{Digest as _, Sha256};

use crate::engine::{DedupEntry, StageTxn, StagingEngine, StagingError};

/// How to run an [`EngineStager`] — every field is an existing §9.6 setting
/// or a §4.5 input, handed in by the composition layer (the daemon reads
/// the config; this crate never does).
#[derive(Debug, Clone)]
pub struct StagerConfig {
    /// `hot.window` (default 60 s) in nanoseconds of
    /// [`Clock::monotonic_nanos`] time.
    pub window_nanos: u64,
    /// `dedup.window_ttl` (default 24 h) in wall-clock milliseconds.
    pub dedup_ttl_ms: i64,
    /// `dedup.window_max_entries` (default 100 000).
    pub dedup_max_entries: u64,
    /// `hot.max_bytes` — the ladder measure's denominator, the *only*
    /// configured byte number (§4.5).
    pub hot_max_bytes: u64,
}

/// One partition's currently-open micro-window, by arrival time.
#[derive(Debug, Clone, Copy)]
struct OpenWindow {
    id: WindowId,
    /// [`Clock::monotonic_nanos`] at which this window opened.
    opened_at: u64,
}

/// The [`StageCommitter`] port over a [`StagingEngine`]: ladder admission,
/// `DedupCheck`, partition/window assignment, and the durable `StageCommit`
/// transaction (module docs).
pub struct EngineStager<S: Storage, C: Clock> {
    engine: Arc<StagingEngine<S>>,
    clock: C,
    config: StagerConfig,
    /// The open window per (dataset, partition). In-memory only: after a
    /// restart the roller opens a fresh window (dense, past the persistent
    /// high-water) rather than guessing how much of the old one's span
    /// remains — a shorter first window is always legal (§2.3), a reused
    /// id never is.
    open_windows: Mutex<HashMap<(DatasetId, PartitionId), OpenWindow>>,
    /// Count-cap dedup evictions (§4.4.1): each shortened the effective
    /// window below the documented retry horizon. The operator surface
    /// (daemon) reads and warns on this; the ladder never does.
    dedup_cap_evictions: AtomicU64,
    /// The §3.7 capture seam: `Accept`, `StageCommit`, and `DedupCheck`
    /// journal here (docs/trace-mapping.md's attributions). `None` — the
    /// production default until the `conformance` row arms — journals
    /// nothing.
    trace: Option<Arc<dyn TraceSink>>,
}

impl<S: Storage, C: Clock> EngineStager<S, C> {
    /// Wraps `engine` as the [`StageCommitter`] port; `clock` is the D-2
    /// time port (window rolling on monotonic time, dedup TTL on wall
    /// time).
    #[must_use]
    pub fn new(engine: Arc<StagingEngine<S>>, clock: C, config: StagerConfig) -> Self {
        Self {
            engine,
            clock,
            config,
            open_windows: Mutex::new(HashMap::new()),
            dedup_cap_evictions: AtomicU64::new(0),
            trace: None,
        }
    }

    /// Journals this stager's §3.3 events (`Accept`, `StageCommit`,
    /// `DedupCheck`) through `sink` (§3.7; the trace-conformance harness's
    /// capture side).
    #[must_use]
    pub fn with_trace_sink(mut self, sink: Arc<dyn TraceSink>) -> Self {
        self.trace = Some(sink);
        self
    }

    /// The wrapped engine (the daemon reaches readers and the drain seam
    /// through this).
    #[must_use]
    pub fn engine(&self) -> &Arc<StagingEngine<S>> {
        &self.engine
    }

    /// The disclosed node status (§4.5, §9.3.2): the current rung plus the
    /// orthogonal replication flag. `drain_stalled` is the drain's stall
    /// signal (v0.1: no drain exists, callers pass `false`);
    /// `replication_degraded` is replication's (v0.1: `false`).
    #[must_use]
    pub fn status(&self, drain_stalled: bool) -> NodeStatus {
        NodeStatus {
            overload: OverloadStatus::from_measure(
                self.engine.staged_bytes(),
                self.config.hot_max_bytes,
                drain_stalled,
            ),
            replication_degraded: false,
        }
    }

    /// Count-cap dedup evictions so far (§4.4.1's below-horizon warning
    /// input; the daemon surfaces it).
    #[must_use]
    pub fn dedup_cap_evictions(&self) -> u64 {
        self.dedup_cap_evictions.load(Ordering::Relaxed)
    }

    /// The blocking body of [`StageCommitter::stage_commit`] (module docs:
    /// callers embed it off the reactor): ladder admission → `DedupCheck` →
    /// `StageCommit`, one durable transaction.
    ///
    /// # Errors
    ///
    /// [`StageError::Throttled`] / [`StageError::RefusingIngest`] at rungs
    /// 2/3 (admission only — nothing staged);
    /// [`StageError::MalformedRecords`] for undecodable records;
    /// [`StageError::Backend`] if the engine fails the transaction. In
    /// every error case nothing is staged and nothing may be acked (§4.3).
    pub fn stage_blocking(&self, batch: &DecodedBatch) -> Result<StageOutcome, StageError> {
        self.admit()?;

        let partition = PartitionId::from_tenant_shard(&batch.tenant, 0);
        let reader = StreamReader::try_new(batch.records.as_ref(), None)
            .map_err(|error| StageError::MalformedRecords(error.to_string()))?;
        let mut record_batches = Vec::new();
        for record_batch in reader {
            record_batches
                .push(record_batch.map_err(|e| StageError::MalformedRecords(e.to_string()))?);
        }
        let rows: usize = record_batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum();
        if rows == 0 {
            // Nothing to stage and nothing to guard: an empty batch commits
            // vacuously and leaves no dedup entry (there is no promise a
            // retry could need replayed).
            return Ok(StageOutcome::Committed(Vec::new()));
        }

        // §3.3 Accept: decoded and ADMITTED into volatile memory — the
        // admission gate (`admit`, §4.5's rung check) passed and the batch
        // is nonempty. Journaled here rather than in `duckspout-accept`
        // because the model's Accept bundles the rung guard, which #146
        // placed on this side of the port (docs/trace-mapping.md carries
        // the same attribution); a throttled or refused request journals
        // Throttle/Refuse INSTEAD of Accept, exactly as §3.3's ladder
        // actions resolve an unsent request.
        if let Some(trace) = &self.trace {
            trace.record(TraceEvent::Accept);
        }

        let dedup_key = dedup_key(batch);
        let window = self
            .current_window(&batch.dataset, &partition)
            .map_err(|e| backend(&e))?;

        let wall_now_ms = self.clock.wall_unix_ms();
        let mut txn = self.engine.begin().map_err(|e| backend(&e))?;
        if let Some(entry) = txn
            .dedup_lookup(
                &batch.tenant,
                &dedup_key,
                wall_now_ms.saturating_sub(self.config.dedup_ttl_ms),
            )
            .map_err(|e| backend(&e))?
        {
            let outcome = resolve_duplicate(&mut txn, &batch.tenant, &dedup_key, &entry, AT_RF_V01)
                .map_err(|e| backend(&e))?;
            // The lookup (and any AtRF ack-marking) is all this transaction
            // did; commit releases it. Nothing was appended (R-2: never
            // replay blindly, never re-stage).
            txn.commit().map_err(|e| backend(&e))?;
            // §3.7: the duplicate's resolution IS DedupCheck — both model
            // branches (replay / in-flight) journal the same name, and no
            // StageCommit or second ClientAck may follow it.
            if let Some(trace) = &self.trace {
                trace.record(TraceEvent::DedupCheck);
            }
            return Ok(outcome);
        }

        for record_batch in &record_batches {
            txn.append(&batch.dataset, &partition, window, record_batch)
                .map_err(|e| backend(&e))?;
        }
        // The entry, its stored outcome, and the §4.4.1 bounds ride the
        // same transaction as the rows. Coverage is knowable before COMMIT
        // (seqs are assigned at append), so the stored outcome is exactly
        // what commit() returns below.
        let outcome_json = coverage_json(&txn)?;
        txn.dedup_insert(
            &batch.tenant,
            &dedup_key,
            AT_RF_V01, // RF = 1: the commit completes the ack evidence.
            &outcome_json,
            wall_now_ms,
        )
        .map_err(|e| backend(&e))?;
        let cap_evicted = txn
            .dedup_evict(
                wall_now_ms,
                self.config.dedup_ttl_ms,
                self.config.dedup_max_entries,
            )
            .map_err(|e| backend(&e))?;
        let coverage = txn.commit().map_err(|e| backend(&e))?;
        // §3.7: journal only on a SUCCESSFUL fsynced commit — a failed
        // transaction staged nothing and journals nothing (the model has
        // no failed-StageCommit action).
        if let Some(trace) = &self.trace {
            trace.record(TraceEvent::StageCommit);
        }
        if cap_evicted > 0 {
            self.dedup_cap_evictions
                .fetch_add(cap_evicted, Ordering::Relaxed);
        }
        Ok(StageOutcome::Committed(coverage))
    }

    /// The ladder's admission gate (§4.5): pure function of the measure,
    /// checked before any work — and only here, so in-flight commits are
    /// never gated.
    fn admit(&self) -> Result<(), StageError> {
        let staged = self.engine.staged_bytes();
        let max = self.config.hot_max_bytes;
        match OverloadStatus::from_measure(staged, max, false) {
            OverloadStatus::RefusingIngest => Err(StageError::RefusingIngest {
                retry_after_ms: throttle_retry_delay_ms(staged, max),
            }),
            OverloadStatus::Throttling => Err(StageError::Throttled {
                retry_after_ms: throttle_retry_delay_ms(staged, max),
            }),
            OverloadStatus::Normal
            | OverloadStatus::StagingPressure
            | OverloadStatus::DrainStalled => Ok(()),
        }
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
            && now.saturating_sub(open.opened_at) < self.config.window_nanos
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
    fn stage_commit(&self, batch: DecodedBatch) -> BoxFuture<'_, Result<StageOutcome, StageError>> {
        // Resolved synchronously by design — the engine blocks on fsync, and
        // the caller owns the off-reactor embedding (module docs).
        let result = self.stage_blocking(&batch);
        Box::pin(std::future::ready(result))
    }
}

/// v0.1's replication floor: RF = 1, so local durable commit *is* the
/// complete ack evidence. Replication (v0.2) replaces this constant with
/// the live receipt count reaching RF.
const AT_RF_V01: bool = true;

/// `DedupCheck`'s duplicate resolution (§3.3, §4.4.1), pure over the entry
/// state and the replication floor:
///
/// - acked entry → replay the stored outcome (R-2);
/// - unacked entry, receipts at RF → the `AtRF` branch: mark acked (in the
///   caller's transaction) and replay;
/// - unacked entry, still short of RF → `DuplicateInFlight` (the retryable
///   signal; unreachable at RF = 1).
fn resolve_duplicate(
    txn: &mut StageTxn<'_>,
    tenant: &TenantId,
    dedup_key: &str,
    entry: &DedupEntry,
    at_rf: bool,
) -> Result<StageOutcome, StagingError> {
    if entry.acked {
        return Ok(StageOutcome::DuplicateAcked(parse_outcome(
            &entry.outcome_json,
        )?));
    }
    if at_rf {
        txn.dedup_mark_acked(tenant, dedup_key)?;
        return Ok(StageOutcome::DuplicateAcked(parse_outcome(
            &entry.outcome_json,
        )?));
    }
    Ok(StageOutcome::DuplicateInFlight)
}

/// The dedup key (§4.4.1): the client's idempotency token when present
/// (kind-prefixed `t:`), else the SHA-256 of the decoded records
/// (kind-prefixed `h:`) — the prefixes keep a token from ever colliding
/// with a hash. Tenant scoping lives in the table key's tenant column, not
/// here.
fn dedup_key(batch: &DecodedBatch) -> String {
    if let Some(token) = &batch.idempotency_key {
        return format!("t:{token}");
    }
    let digest = Sha256::digest(batch.records.as_ref());
    let mut out = String::with_capacity(2 + digest.len() * 2);
    out.push_str("h:");
    for byte in digest {
        // Infallible: writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Serializes the coverage this transaction will commit — the stored
/// outcome a duplicate replays (§4.4.1).
fn coverage_json(txn: &StageTxn<'_>) -> Result<String, StageError> {
    serde_json::to_string(&txn.pending_coverage())
        .map_err(|error| StageError::Backend(format!("stored-outcome encoding: {error}")))
}

/// Deserializes a stored outcome.
fn parse_outcome(outcome_json: &str) -> Result<Vec<StagedCoverage>, StagingError> {
    serde_json::from_str(outcome_json).map_err(|error| {
        StagingError::Corrupt(format!(
            "dedup stored outcome is not decodable coverage: {error}"
        ))
    })
}

fn backend(error: &StagingError) -> StageError {
    StageError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use duckspout_types::{DatasetId, DatasetKind, TenantId};
    use proptest::prelude::*;

    use super::*;

    fn batch(records: Vec<u8>, token: Option<String>) -> DecodedBatch {
        DecodedBatch {
            dataset: DatasetId::new("otlp_logs"),
            kind: DatasetKind::Event,
            tenant: TenantId::new("t"),
            idempotency_key: token,
            records: Bytes::from(records),
        }
    }

    proptest! {
        /// §8.5's dedup-determinism laws (issue #40) over the §4.4.1 key
        /// derivation, as pure-function properties — the engine-level replay
        /// behavior is exercised in `tests/stager.rs`; these pin the key
        /// itself for ANY content and token:
        ///
        /// - equal `(content | token)` inputs derive equal keys, and the key
        ///   reads NOTHING else (would catch a nonce, a timestamp, or batch
        ///   metadata leaking into the key — every retry would then miss the
        ///   window and duplicate);
        /// - a token makes the key content-independent (§4.4.1 precedence —
        ///   would catch content sneaking back in, where a client's retry
        ///   with a re-encoded body would double-stage);
        /// - token-derived and content-derived keys never collide, even for
        ///   a token forged to look like a content hash (the `t:`/`h:`
        ///   prefix discrimination — would catch the prefixes being
        ///   dropped);
        /// - distinct contents derive distinct keys (the content hash doing
        ///   its job).
        #[test]
        fn dedup_key_is_a_pure_function_of_content_or_token(
            content_a in prop::collection::vec(any::<u8>(), 0..64),
            content_b in prop::collection::vec(any::<u8>(), 0..64),
            token in ".{0,32}",
        ) {
            // Determinism + content-only dependence.
            prop_assert_eq!(
                dedup_key(&batch(content_a.clone(), None)),
                dedup_key(&batch(content_a.clone(), None))
            );
            // Token precedence: content is invisible under a token.
            prop_assert_eq!(
                dedup_key(&batch(content_a.clone(), Some(token.clone()))),
                dedup_key(&batch(content_b.clone(), Some(token.clone())))
            );
            // Prefix discrimination: a token can never collide with any
            // content-derived key — including a token spelled exactly like
            // one.
            let content_key = dedup_key(&batch(content_a.clone(), None));
            prop_assert_ne!(
                dedup_key(&batch(content_b.clone(), Some(content_key.clone()))),
                content_key
            );
            // Distinct contents → distinct keys.
            if content_a != content_b {
                prop_assert_ne!(
                    dedup_key(&batch(content_a, None)),
                    dedup_key(&batch(content_b, None))
                );
            }
        }
    }
}
