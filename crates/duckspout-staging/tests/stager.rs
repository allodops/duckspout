//! `EngineStager` tests: the `StageCommitter` port over the real engine —
//! ladder admission (§4.5), `DedupCheck` (§4.4.1), partition assignment,
//! arrival-time window rolling, and dense never-reused window ids.
//!
//! The clock double is test-local (like `common::FsStorage`): the invariant
//! engine audits dev-dependency edges too, and staging → ctk is forbidden.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use common::{log_batch, open_engine};
use duckspout_staging::{EngineStager, StageError, StageOutcome, StagerConfig, StagingEngine};
use duckspout_types::{Clock, DatasetId, DecodedBatch, PartitionId, StageCommitter, WindowId};

const WINDOW_NANOS: u64 = 60_000_000_000; // hot.window default, 60 s
const DAY_MS: i64 = 24 * 60 * 60 * 1000; // dedup.window_ttl default, 24 h

/// A hand-cranked Clock: monotonic and wall time advance only when told to.
#[derive(Default)]
struct ManualClock {
    nanos: Arc<AtomicU64>,
    wall_ms: Arc<AtomicU64>,
}

impl ManualClock {
    fn handles(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (Arc::clone(&self.nanos), Arc::clone(&self.wall_ms))
    }
}

impl Clock for ManualClock {
    fn monotonic_nanos(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }

    fn wall_unix_ms(&self) -> i64 {
        i64::try_from(self.wall_ms.load(Ordering::SeqCst)).unwrap_or(i64::MAX)
    }
}

fn ipc_bytes(batch: &duckspout_staging::arrow::record_batch::RecordBatch) -> Bytes {
    let mut writer =
        duckspout_staging::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &batch.schema())
            .unwrap();
    writer.write(batch).unwrap();
    Bytes::from(writer.into_inner().unwrap())
}

fn decoded(tenant: &str, rows: usize, first_ts: i64) -> DecodedBatch {
    decoded_keyed(tenant, rows, first_ts, None)
}

fn decoded_keyed(
    tenant: &str,
    rows: usize,
    first_ts: i64,
    idempotency_key: Option<&str>,
) -> DecodedBatch {
    DecodedBatch {
        dataset: DatasetId::new("otlp_logs"),
        kind: duckspout_types::DatasetKind::Event,
        tenant: duckspout_types::TenantId::new(tenant),
        idempotency_key: idempotency_key.map(str::to_owned),
        records: ipc_bytes(&log_batch(rows, first_ts, 0)),
    }
}

fn config(hot_max_bytes: u64) -> StagerConfig {
    StagerConfig {
        window_nanos: WINDOW_NANOS,
        dedup_ttl_ms: DAY_MS,
        dedup_max_entries: 100_000,
        hot_max_bytes,
    }
}

type TestStager = EngineStager<common::FsStorage, ManualClock>;

fn stager_over(hot_dir: &std::path::Path) -> (TestStager, Arc<AtomicU64>, Arc<AtomicU64>) {
    stager_with(hot_dir, config(u64::MAX))
}

fn stager_with(
    hot_dir: &std::path::Path,
    config: StagerConfig,
) -> (TestStager, Arc<AtomicU64>, Arc<AtomicU64>) {
    let engine = Arc::new(open_engine(hot_dir, "node-a/1"));
    stager_on(engine, config)
}

fn stager_on(
    engine: Arc<StagingEngine<common::FsStorage>>,
    config: StagerConfig,
) -> (TestStager, Arc<AtomicU64>, Arc<AtomicU64>) {
    let clock = ManualClock::default();
    let (nanos, wall) = clock.handles();
    (EngineStager::new(engine, clock, config), nanos, wall)
}

fn committed(outcome: StageOutcome) -> Vec<duckspout_types::StagedCoverage> {
    match outcome {
        StageOutcome::Committed(coverage) => coverage,
        other => panic!("expected Committed, got {other:?}"),
    }
}

fn replayed(outcome: StageOutcome) -> Vec<duckspout_types::StagedCoverage> {
    match outcome {
        StageOutcome::DuplicateAcked(coverage) => coverage,
        other => panic!("expected DuplicateAcked, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// §4.3: coverage evidence, partitions, windows (the #32 seam, port-shaped)
// ---------------------------------------------------------------------------

/// The port's core contract: staging returns per-partition coverage that is
/// dense across successive commits, and the rows are really there.
#[test]
fn stage_commit_returns_dense_coverage_and_lands_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, _) = stager_over(dir.path());

    let coverage = committed(
        stager
            .stage_blocking(&decoded("tenant-a", 5, 1_000))
            .unwrap(),
    );
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("tenant-a"), 0);
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].partition, partition);
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (1, 5)
    );

    // A second, distinct batch of the same tenant: seq continues densely.
    let coverage = committed(
        stager
            .stage_blocking(&decoded("tenant-a", 3, 2_000))
            .unwrap(),
    );
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (6, 8)
    );

    let reader = stager.engine().reader().unwrap();
    let dataset = DatasetId::new("otlp_logs");
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .unwrap(),
        8
    );
}

/// Two tenants are two partitions (§2.2) — and byte-identical payloads in
/// different tenants are both staged: the tenant is in the dedup key
/// because a tenant-blind hit would answer tenant B with tenant A's ack.
#[test]
fn tenants_are_disjoint_in_partitioning_and_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, _) = stager_over(dir.path());

    let a = committed(stager.stage_blocking(&decoded("tenant-a", 2, 0)).unwrap());
    let b = committed(stager.stage_blocking(&decoded("tenant-b", 2, 0)).unwrap());
    assert_ne!(a[0].partition, b[0].partition);
    assert_eq!((a[0].range.first_seq, a[0].range.last_seq), (1, 2));
    assert_eq!((b[0].range.first_seq, b[0].range.last_seq), (1, 2));
}

/// Window rolling is a pure function of the Clock port (§2.3).
#[test]
fn windows_roll_on_arrival_time() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, nanos, _) = stager_over(dir.path());
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);

    stager.stage_blocking(&decoded("t", 1, 0)).unwrap();
    nanos.store(WINDOW_NANOS - 1, Ordering::SeqCst); // 1 ns before the roll
    stager.stage_blocking(&decoded("t", 1, 1)).unwrap();
    let reader = stager.engine().reader().unwrap();
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(0))
            .unwrap(),
        2
    );

    nanos.store(WINDOW_NANOS, Ordering::SeqCst); // exactly hot.window later
    stager.stage_blocking(&decoded("t", 1, 2)).unwrap();
    assert_eq!(
        reader
            .count_window(&dataset, &partition, WindowId(1))
            .unwrap(),
        1
    );
    assert_eq!(stager.engine().list_windows().unwrap().len(), 2);
}

/// Window ids stay dense and are never reused, across both `DropWindow`
/// and an engine reopen (§2.3).
#[test]
fn window_ids_survive_drop_and_reopen_without_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);

    {
        let (stager, nanos, _) = stager_over(dir.path());
        stager.stage_blocking(&decoded("t", 1, 0)).unwrap(); // window 0
        nanos.store(WINDOW_NANOS, Ordering::SeqCst);
        stager.stage_blocking(&decoded("t", 1, 1)).unwrap(); // window 1
        assert!(
            stager
                .engine()
                .drop_window(&dataset, &partition, WindowId(0))
                .unwrap()
        );
        assert!(
            stager
                .engine()
                .drop_window(&dataset, &partition, WindowId(1))
                .unwrap()
        );
        assert!(stager.engine().list_windows().unwrap().is_empty());
        nanos.store(2 * WINDOW_NANOS, Ordering::SeqCst);
        stager.stage_blocking(&decoded("t", 1, 2)).unwrap();
        assert_eq!(
            stager.engine().list_windows().unwrap()[0].window,
            WindowId(2)
        );
        assert!(
            stager
                .engine()
                .drop_window(&dataset, &partition, WindowId(2))
                .unwrap()
        );
    }

    let (stager, _, _) = stager_over(dir.path());
    stager.stage_blocking(&decoded("t", 1, 3)).unwrap();
    assert_eq!(
        stager.engine().list_windows().unwrap()[0].window,
        WindowId(3)
    );
}

/// The port trait itself resolves with the blocking body's result.
#[test]
fn port_future_resolves_synchronously_with_the_result() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, _) = stager_over(dir.path());
    let future = stager.stage_commit(decoded("t", 2, 0));
    let coverage = committed(pollster_block_on(future).unwrap());
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (1, 2)
    );
}

/// Non-IPC bytes fail typed and stage nothing.
#[test]
fn malformed_records_stage_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, _) = stager_over(dir.path());
    let mut batch = decoded("t", 1, 0);
    batch.records = Bytes::from_static(b"not an arrow ipc stream");
    let err = stager.stage_blocking(&batch).unwrap_err();
    assert!(matches!(err, StageError::MalformedRecords(_)));
    assert!(stager.engine().list_windows().unwrap().is_empty());
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);
    assert_eq!(stager.engine().applied_seq(&partition).unwrap(), None);
}

// ---------------------------------------------------------------------------
// §4.4.1: DedupCheck — R-2's "retries resolve to the original outcome"
// ---------------------------------------------------------------------------

/// The R-2 heart: a byte-identical retry replays the original ack evidence
/// verbatim — same coverage, no second staged copy, seq untouched. Would
/// catch a dedup that re-stages, or a stored outcome that drifts from what
/// `ClientAck` returned.
#[test]
fn retry_replays_the_original_ack_without_restaging() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, _) = stager_over(dir.path());
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);

    let original = committed(stager.stage_blocking(&decoded("t", 5, 1_000)).unwrap());
    let replay = replayed(stager.stage_blocking(&decoded("t", 5, 1_000)).unwrap());
    assert_eq!(
        replay, original,
        "replay must be the original outcome (R-2)"
    );

    let reader = stager.engine().reader().unwrap();
    assert_eq!(
        reader
            .count_window(&DatasetId::new("otlp_logs"), &partition, WindowId(0))
            .unwrap(),
        5,
        "no second staged copy"
    );
    assert_eq!(stager.engine().applied_seq(&partition).unwrap(), Some(5));
}

/// The idempotency token takes precedence over the content hash (§4.4.1):
/// same token + different bytes is a duplicate; different token + same
/// bytes is fresh.
#[test]
fn idempotency_token_takes_precedence_over_content() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, _) = stager_over(dir.path());

    let original = committed(
        stager
            .stage_blocking(&decoded_keyed("t", 3, 0, Some("token-1")))
            .unwrap(),
    );
    // Same token, different payload: the token decides — replay, no stage.
    let replay = replayed(
        stager
            .stage_blocking(&decoded_keyed("t", 7, 999, Some("token-1")))
            .unwrap(),
    );
    assert_eq!(replay, original);
    // Different token, byte-identical payload: fresh (token precedence cuts
    // both ways — content is not consulted when a token is present).
    committed(
        stager
            .stage_blocking(&decoded_keyed("t", 3, 0, Some("token-2")))
            .unwrap(),
    );
}

/// The `AtRF` branch (§3.3, §4.4.1): an entry left unacked (the
/// crash-between-commit-and-ack shape) is never poison — at RF = 1 a retry
/// marks it acked and replays the stored outcome. Simulated by flipping the
/// flag on the closed database, exactly what a crash would leave.
#[test]
fn unacked_entry_resolves_to_replay_at_rf() {
    let dir = tempfile::tempdir().unwrap();
    let original;
    {
        let (stager, _, _) = stager_over(dir.path());
        original = committed(stager.stage_blocking(&decoded("t", 4, 0)).unwrap());
    }
    {
        let conn = duckdb::Connection::open(dir.path().join("hot.db")).unwrap();
        let flipped = conn
            .execute("UPDATE duckspout_dedup SET acked = false", [])
            .unwrap();
        assert_eq!(flipped, 1, "exactly the one entry");
    }
    let (stager, _, _) = stager_over(dir.path());
    let replay = replayed(stager.stage_blocking(&decoded("t", 4, 0)).unwrap());
    assert_eq!(
        replay, original,
        "AtRF resolution replays the stored outcome"
    );
    // And the entry is now ack-complete: a further retry replays directly.
    let again = replayed(stager.stage_blocking(&decoded("t", 4, 0)).unwrap());
    assert_eq!(again, original);
}

/// TTL eviction (§4.4.1): once the wall clock passes `dedup.window_ttl`,
/// the entry is gone and a retry stages a second copy — the *disclosed*
/// residual duplicate path (§4.4.3b), not a silent one.
#[test]
fn ttl_eviction_expires_entries_on_wall_time() {
    let dir = tempfile::tempdir().unwrap();
    let (stager, _, wall) = stager_over(dir.path());

    committed(stager.stage_blocking(&decoded("t", 2, 0)).unwrap());
    // Still inside the window: replay.
    wall.store(u64::try_from(DAY_MS).unwrap() - 1, Ordering::SeqCst);
    replayed(stager.stage_blocking(&decoded("t", 2, 0)).unwrap());
    // Past the TTL: eviction runs with the next insert's transaction, so
    // the retry is fresh and stages again (disclosed residual).
    wall.store(u64::try_from(DAY_MS).unwrap() + 1, Ordering::SeqCst);
    let coverage = committed(stager.stage_blocking(&decoded("t", 2, 0)).unwrap());
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (3, 4),
        "a post-TTL retry is a fresh batch"
    );
}

/// Count-cap eviction (§4.4.1): the oldest entries fall out at
/// `dedup.window_max_entries`, and each such eviction is counted for the
/// below-retry-horizon warning.
#[test]
fn count_cap_evicts_oldest_and_counts_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(u64::MAX);
    cfg.dedup_max_entries = 2;
    let (stager, _, wall) = stager_with(dir.path(), cfg);

    for (i, tenant) in ["a", "b", "c"].iter().enumerate() {
        wall.store(u64::try_from(i).unwrap() + 1, Ordering::SeqCst);
        committed(stager.stage_blocking(&decoded(tenant, 1, 0)).unwrap());
    }
    assert_eq!(stager.dedup_cap_evictions(), 1, "one over the cap of 2");
    // The oldest ("a") was evicted: its retry stages a second copy; the
    // newest ("c") still replays.
    committed(stager.stage_blocking(&decoded("a", 1, 0)).unwrap());
    replayed(stager.stage_blocking(&decoded("c", 1, 0)).unwrap());
}

// ---------------------------------------------------------------------------
// §4.5: the overload ladder gates admission, never promises made
// ---------------------------------------------------------------------------

/// Rung 2 and rung 3 refuse admission with the right typed error at the
/// right measure — computed against the engine's real staged-bytes
/// accounting, not a mock.
#[test]
fn ladder_throttles_at_95_and_refuses_at_100() {
    let dir = tempfile::tempdir().unwrap();
    let engine;
    {
        let (stager, _, _) = stager_over(dir.path());
        committed(stager.stage_blocking(&decoded("t", 100, 0)).unwrap());
        engine = Arc::clone(stager.engine());
    }
    let staged = engine.staged_bytes().unwrap();
    assert!(staged > 0, "accounting must see the staged batch");

    // A stager whose capacity puts the current fill at exactly 100%.
    let (refusing, _, _) = stager_on(Arc::clone(&engine), config(staged));
    let err = refusing.stage_blocking(&decoded("t2", 1, 0)).unwrap_err();
    assert!(
        matches!(err, StageError::RefusingIngest { .. }),
        "M = 100% must refuse, got {err:?}"
    );

    // Capacity such that the fill sits inside [95%, 100%): throttle.
    let (throttling, _, _) = stager_on(Arc::clone(&engine), config(staged * 100 / 97));
    let err = throttling.stage_blocking(&decoded("t2", 1, 0)).unwrap_err();
    assert!(
        matches!(err, StageError::Throttled { .. }),
        "M ≈ 97% must throttle, got {err:?}"
    );
    if let StageError::Throttled { retry_after_ms } = err {
        assert!(
            (duckspout_types::status::THROTTLE_RETRY_MIN_MS
                ..=duckspout_types::status::THROTTLE_RETRY_MAX_MS)
                .contains(&retry_after_ms),
            "delay {retry_after_ms} outside the §4.5 band"
        );
    }

    // Capacity with headroom: admission unaffected.
    let (healthy, _, _) = stager_on(engine, config(staged * 100));
    committed(healthy.stage_blocking(&decoded("t2", 1, 0)).unwrap());
}

/// "The ladder gates admission, never promises made": with the measure at
/// the top rung, a `StageCommit` transaction already begun still commits,
/// and an already-acked entry's retry still replays its ack.
#[test]
fn in_flight_work_completes_at_every_rung() {
    let dir = tempfile::tempdir().unwrap();
    let engine;
    {
        let (stager, _, _) = stager_over(dir.path());
        committed(stager.stage_blocking(&decoded("t", 10, 0)).unwrap());
        engine = Arc::clone(stager.engine());
    }
    let staged = engine.staged_bytes().unwrap();

    // An in-flight transaction (begun before the rung would gate it)
    // commits fine — the engine has no ladder, by design.
    let mut txn = engine.begin().unwrap();
    txn.append(
        &DatasetId::new("otlp_logs"),
        &PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0),
        WindowId(0),
        &log_batch(3, 50, 0),
    )
    .unwrap();
    let coverage = txn.commit().unwrap();
    assert_eq!(
        (coverage[0].range.first_seq, coverage[0].range.last_seq),
        (11, 13)
    );

    // At the top rung, the made promise (the acked entry) still replays.
    let (refusing, _, _) = stager_on(engine, config(staged));
    assert!(matches!(
        refusing.stage_blocking(&decoded("fresh", 1, 0)),
        Err(StageError::RefusingIngest { .. })
    ));
    // Deliberate: admission is gated before DedupCheck, so even a replay
    // waits out the refusal — it accepts no new data, but a uniform gate is
    // what keeps the rung a pure function of M. The retryable signal is
    // correct either way; the replay answers once M drops.
    assert!(matches!(
        refusing.stage_blocking(&decoded("t", 10, 0)),
        Err(StageError::RefusingIngest { .. })
    ));
}

/// The disclosed status follows the measure and the drain flag (§4.5,
/// §9.3.2).
#[test]
fn status_discloses_the_rung() {
    let dir = tempfile::tempdir().unwrap();
    let engine;
    {
        let (stager, _, _) = stager_over(dir.path());
        committed(stager.stage_blocking(&decoded("t", 100, 0)).unwrap());
        engine = Arc::clone(stager.engine());
    }
    let staged = engine.staged_bytes().unwrap();

    let (roomy, _, _) = stager_on(Arc::clone(&engine), config(staged * 100));
    assert_eq!(
        roomy.status(false).unwrap().overload,
        duckspout_types::OverloadStatus::Normal
    );
    let (pressured, _, _) = stager_on(Arc::clone(&engine), config(staged * 100 / 90));
    assert_eq!(
        pressured.status(false).unwrap().overload,
        duckspout_types::OverloadStatus::StagingPressure
    );
    assert_eq!(
        pressured.status(true).unwrap().overload,
        duckspout_types::OverloadStatus::DrainStalled
    );
    let (full, _, _) = stager_on(engine, config(staged));
    assert_eq!(
        full.status(false).unwrap().overload,
        duckspout_types::OverloadStatus::RefusingIngest
    );
}

/// `DropWindow` returns its bytes to the measure (§4.5: `staged_bytes` sums
/// over LIVE tables) — and the accounting survives reopen.
#[test]
fn staged_bytes_track_commit_drop_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dataset = DatasetId::new("otlp_logs");
    let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);
    let after_commit;
    {
        let (stager, _, _) = stager_over(dir.path());
        assert_eq!(stager.engine().staged_bytes().unwrap(), 0);
        committed(stager.stage_blocking(&decoded("t", 100, 0)).unwrap());
        after_commit = stager.engine().staged_bytes().unwrap();
        assert!(after_commit > 0);
    }
    {
        let engine = open_engine(dir.path(), "node-a/1");
        assert_eq!(
            engine.staged_bytes().unwrap(),
            after_commit,
            "accounting is durable"
        );
        assert!(
            engine
                .drop_window(&dataset, &partition, WindowId(0))
                .unwrap()
        );
        assert_eq!(engine.staged_bytes().unwrap(), 0, "dropped bytes leave M");
    }
}

// ---------------------------------------------------------------------------
// §8.5 law suite (issue #40): replay-returns-original under arbitrary retry
// interleavings — exhaustive over every arrival order at a tiny scope
// (§3.1's posture), not sampled.
// ---------------------------------------------------------------------------

/// For EVERY arrival order of {A, A-retried, B, C} (all 24 permutations —
/// the 12 distinct multiset arrangements, each reached twice because the
/// duplicate pair is content-identical), the retry replays exactly the
/// original's committed
/// coverage — wherever it lands in the interleaving — and the final staged
/// state holds each batch exactly once with a dense sequence. The
/// example-based tests above pin single orders; this quantifies the §4.4.1
/// law they instantiate. Would catch: a dedup entry written after commit
/// instead of inside it (an interleaving-dependent duplicate), a stored
/// outcome that depends on when the retry arrives, or a replay that
/// consumes sequence numbers.
#[test]
fn replay_returns_the_original_under_every_retry_interleaving() {
    // 0 and 1 are the duplicate pair; 2 and 3 are distinct bystanders.
    let batches = |i: usize| match i {
        0 | 1 => decoded("t", 2, 1_000),
        2 => decoded("t", 1, 2_000),
        _ => decoded("t", 3, 3_000),
    };
    let mut orders: Vec<[usize; 4]> = Vec::new();
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let mut seen = [false; 4];
                    for i in [a, b, c, d] {
                        seen[i] = true;
                    }
                    if seen == [true; 4] {
                        orders.push([a, b, c, d]);
                    }
                }
            }
        }
    }
    assert_eq!(orders.len(), 24);

    for order in orders {
        let dir = tempfile::tempdir().unwrap();
        let (stager, _, _) = stager_over(dir.path());
        let mut original: Option<Vec<duckspout_types::StagedCoverage>> = None;
        for index in order {
            let outcome = stager.stage_blocking(&batches(index)).unwrap();
            match (index, &original) {
                // First arrival of the duplicate pair commits…
                (0 | 1, None) => original = Some(committed(outcome)),
                // …and the other one replays IDENTICAL coverage, whenever
                // it arrives.
                (0 | 1, Some(first)) => {
                    assert_eq!(
                        &replayed(outcome),
                        first,
                        "order {order:?}: the retry must replay the original's ack evidence"
                    );
                }
                _ => {
                    committed(outcome);
                }
            }
        }
        // Exactly one copy of the duplicate pair landed: 2 + 1 + 3 rows,
        // densely sequenced.
        let partition = PartitionId::from_tenant_shard(&duckspout_types::TenantId::new("t"), 0);
        assert_eq!(
            stager.engine().applied_seq(&partition).unwrap(),
            Some(6),
            "order {order:?}: duplicate rows leaked into staging"
        );
    }
}

/// Drives a ready-or-not future to completion on this thread.
fn pollster_block_on<T>(mut future: duckspout_types::BoxFuture<'_, T>) -> T {
    use std::task::{Context, Poll, Wake, Waker};
    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match std::future::Future::poll(future.as_mut(), &mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
