//! The full drain choreography against port doubles (§6.2–§6.6).
//!
//! The doubles live here, locally: the CTK's doubles are out of reach by
//! layering (invariants.toml forbids drain → ctk even as a dev-dependency),
//! and the seams under test are exactly the types-level ports, so local
//! doubles are the right fidelity anyway.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use bytes::Bytes;
use duckspout_drain::{
    DatasetDrainPlan, DrainConfig, DrainCoordinator, DrainError, DrainOutcome, PartDiscriminator,
    RequeueReason, part_name,
};
use duckspout_types::{
    BoxFuture, Clock, CommitOutcome, DatasetId, DrainableWindow, DropOutcome, LakeCommitter,
    LakeError, LedgerRejection, OriginSeqRange, PartitionId, SealError, SealRequest, SealSurface,
    SealedPart, Storage, StorageError, StoragePath, WatermarkBookkeeping, WatermarkRow, WindowId,
    WindowManifest,
};

// ---------------------------------------------------------------------------
// A minimal single-future executor (all double futures are ready-made).
// ---------------------------------------------------------------------------

fn block_on<T>(future: impl Future<Output = T>) -> T {
    use std::task::{Context, Poll, Waker};
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn ready<T: Send + 'static>(value: T) -> BoxFuture<'static, T> {
    Box::pin(async move { value })
}

// ---------------------------------------------------------------------------
// Storage double: an in-memory node-local scratch store.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MemStorage {
    files: Mutex<HashMap<String, Bytes>>,
}

impl MemStorage {
    fn contains(&self, path: &str) -> bool {
        self.files.lock().unwrap().contains_key(path)
    }

    fn insert(&self, path: &str, data: &[u8]) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_owned(), Bytes::copy_from_slice(data));
    }
}

impl Storage for MemStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        self.files
            .lock()
            .unwrap()
            .insert(path.as_str().to_owned(), data);
        ready(Ok(()))
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        let result = self
            .files
            .lock()
            .unwrap()
            .get(path.as_str())
            .cloned()
            .ok_or_else(|| StorageError::NotFound(path.clone()));
        ready(result)
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        self.files.lock().unwrap().remove(path.as_str());
        ready(Ok(()))
    }

    fn fsync_file(&self, _path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        ready(Ok(()))
    }

    fn fsync_dir(&self, _dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// Clock double.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TestClock {
    wall_ms: AtomicI64,
}

impl TestClock {
    fn set_ms(&self, ms: i64) {
        self.wall_ms.store(ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn monotonic_nanos(&self) -> u64 {
        u64::try_from(self.wall_ms.load(Ordering::SeqCst)).unwrap_or(0) * 1_000_000
    }

    fn wall_unix_ms(&self) -> i64 {
        self.wall_ms.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// SealSurface double.
// ---------------------------------------------------------------------------

/// One recorded `drop_window` call: identity plus the TN-32 covered set.
type DroppedCall = (DatasetId, PartitionId, WindowId, Vec<OriginSeqRange>);

struct SealDouble {
    windows: Mutex<Vec<DrainableWindow>>,
    /// Template bookkeeping every seal reports.
    coverage: Vec<OriginSeqRange>,
    rows: u64,
    event_time_min_ms: i64,
    event_time_max_ms: i64,
    dedup_removed: u64,
    scratch: Arc<MemStorage>,
    seal_calls: AtomicUsize,
    dropped: Mutex<Vec<DroppedCall>>,
    requests: Mutex<Vec<SealRequest>>,
}

impl SealDouble {
    fn new(scratch: Arc<MemStorage>) -> Self {
        Self {
            windows: Mutex::new(Vec::new()),
            coverage: vec![OriginSeqRange {
                origin: "o1".into(),
                first_seq: 1,
                last_seq: 5,
            }],
            rows: 5,
            event_time_min_ms: 100,
            event_time_max_ms: 1_000,
            dedup_removed: 0,
            scratch,
            seal_calls: AtomicUsize::new(0),
            dropped: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn offer(&self, window: DrainableWindow) {
        self.windows.lock().unwrap().push(window);
    }

    fn dropped_windows(&self) -> Vec<u64> {
        self.dropped
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, w, _)| w.0)
            .collect()
    }

    /// The covered ranges the drain passed to each drop, in call order
    /// (the TN-32 guard's input).
    fn dropped_coverage(&self) -> Vec<Vec<OriginSeqRange>> {
        self.dropped
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, _, covered)| covered.clone())
            .collect()
    }
}

impl SealSurface for SealDouble {
    fn drainable_windows(&self) -> BoxFuture<'_, Result<Vec<DrainableWindow>, SealError>> {
        ready(Ok(self.windows.lock().unwrap().clone()))
    }

    fn seal_window(&self, request: SealRequest) -> BoxFuture<'_, Result<SealedPart, SealError>> {
        self.seal_calls.fetch_add(1, Ordering::SeqCst);
        let path = format!("seal/w{}.parquet", request.window);
        self.scratch.insert(
            &path,
            format!("parquet-bytes-w{}", request.window).as_bytes(),
        );
        let sealed = SealedPart {
            path: StoragePath::new(path),
            rows: self.rows,
            event_time_min_ms: self.event_time_min_ms,
            event_time_max_ms: self.event_time_max_ms,
            dedup_removed: self.dedup_removed,
            origin_coverage: self.coverage.clone(),
        };
        self.requests.lock().unwrap().push(request);
        ready(Ok(sealed))
    }

    fn drop_window(
        &self,
        dataset: DatasetId,
        partition: PartitionId,
        window: WindowId,
        covered: Vec<OriginSeqRange>,
    ) -> BoxFuture<'_, Result<DropOutcome, SealError>> {
        let mut dropped = self.dropped.lock().unwrap();
        let first = !dropped
            .iter()
            .any(|(d, p, w, _)| *d == dataset && *p == partition && *w == window);
        dropped.push((dataset, partition, window, covered));
        ready(Ok(if first {
            DropOutcome::Dropped
        } else {
            DropOutcome::AlreadyGone
        }))
    }
}

// ---------------------------------------------------------------------------
// WatermarkBookkeeping double: dense-next fence + running-max watermark.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct LedgerState {
    next: u64,
    complete: Option<i64>,
    recorded: Vec<u64>,
    /// Coverage per recorded window id (the TN-32 read-back surface).
    coverage: HashMap<u64, Vec<OriginSeqRange>>,
}

#[derive(Default)]
struct LedgerDouble {
    state: Mutex<LedgerState>,
    /// When set, the partition has no provable watermark: `advance_for`
    /// and `record_commit` yield no row (the blocked-partition branch).
    blocked: bool,
}

impl LedgerDouble {
    fn with_recorded(next: u64, complete: Option<i64>, coverage: &[(u64, OriginSeqRange)]) -> Self {
        Self {
            state: Mutex::new(LedgerState {
                next,
                complete,
                recorded: Vec::new(),
                coverage: coverage
                    .iter()
                    .map(|(w, range)| (*w, vec![range.clone()]))
                    .collect(),
            }),
            blocked: false,
        }
    }

    fn recorded(&self) -> Vec<u64> {
        self.state.lock().unwrap().recorded.clone()
    }

    fn check(state: &LedgerState, manifest: &WindowManifest) -> Result<(), LedgerRejection> {
        if manifest.window_id.0 == state.next {
            Ok(())
        } else {
            Err(LedgerRejection::WindowNotNext {
                partition: manifest.partition.clone(),
                expected: WindowId(state.next),
                got: manifest.window_id,
            })
        }
    }

    fn row(&self, state: &LedgerState, manifest: &WindowManifest) -> Option<WatermarkRow> {
        if self.blocked {
            return None;
        }
        let complete = state.complete.map_or(manifest.event_time_max_ms, |c| {
            c.max(manifest.event_time_max_ms)
        });
        Some(WatermarkRow {
            partition: manifest.partition.clone(),
            complete_through_ms: complete,
        })
    }
}

impl WatermarkBookkeeping for LedgerDouble {
    fn next_window(&self, _partition: &PartitionId) -> WindowId {
        WindowId(self.state.lock().unwrap().next)
    }

    fn complete_through_ms(&self, _partition: &PartitionId) -> Option<i64> {
        self.state.lock().unwrap().complete
    }

    fn advance_for(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, LedgerRejection> {
        let state = self.state.lock().unwrap();
        Self::check(&state, manifest)?;
        Ok(self.row(&state, manifest))
    }

    fn record_commit(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, LedgerRejection> {
        let mut state = self.state.lock().unwrap();
        Self::check(&state, manifest)?;
        let row = self.row(&state, manifest);
        state.next += 1;
        state.complete = row
            .as_ref()
            .map(|r| r.complete_through_ms)
            .or(state.complete);
        state.recorded.push(manifest.window_id.0);
        state
            .coverage
            .insert(manifest.window_id.0, manifest.origin_coverage.clone());
        Ok(row)
    }

    fn recorded_coverage(
        &self,
        _partition: &PartitionId,
        window: WindowId,
    ) -> Option<Vec<OriginSeqRange>> {
        self.state.lock().unwrap().coverage.get(&window.0).cloned()
    }
}

// ---------------------------------------------------------------------------
// LakeCommitter double: a fenced in-memory lake with scriptable outcomes.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Script {
    /// Behave like a real fenced backend: first commit of a fence key wins
    /// (Committed, state applied); a duplicate Aborts.
    Real,
    /// Definitively reject without applying anything (transient abort).
    RejectWithoutApplying,
    /// Drop the connection mid-COMMIT: apply (or not) silently, then
    /// report Indeterminate.
    Indeterminate { landed: bool },
}

#[derive(Default)]
struct LakeState {
    /// Registered fence keys: (partition, window) — the §6.6 UNIQUE fence.
    fenced: BTreeSet<(String, u64)>,
    watermarks: BTreeMap<String, i64>,
    manifests: Vec<WindowManifest>,
}

struct CommitterDouble {
    lake: Mutex<LakeState>,
    script: Mutex<Vec<Script>>,
    read_back_fails: Mutex<bool>,
    commit_calls: AtomicUsize,
    read_back_calls: AtomicUsize,
}

impl CommitterDouble {
    fn new() -> Self {
        Self {
            lake: Mutex::new(LakeState::default()),
            script: Mutex::new(Vec::new()),
            read_back_fails: Mutex::new(false),
            commit_calls: AtomicUsize::new(0),
            read_back_calls: AtomicUsize::new(0),
        }
    }

    /// Queues outcomes for the next `commit_files` calls (drained in
    /// order); when the queue is empty the double behaves like
    /// [`Script::Real`].
    fn push_script(&self, script: Script) {
        self.script.lock().unwrap().push(script);
    }

    fn fail_next_read_back(&self) {
        *self.read_back_fails.lock().unwrap() = true;
    }

    fn apply(lake: &mut LakeState, manifest: &WindowManifest, watermarks: &[WatermarkRow]) {
        lake.fenced
            .insert((manifest.partition.as_str().to_owned(), manifest.window_id.0));
        for row in watermarks {
            lake.watermarks
                .insert(row.partition.as_str().to_owned(), row.complete_through_ms);
        }
        lake.manifests.push(manifest.clone());
    }

    fn committed_manifests(&self) -> Vec<WindowManifest> {
        self.lake.lock().unwrap().manifests.clone()
    }
}

impl LakeCommitter for CommitterDouble {
    fn commit_files(
        &self,
        manifest: WindowManifest,
        watermarks: Vec<WatermarkRow>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        self.commit_calls.fetch_add(1, Ordering::SeqCst);
        let script = {
            let mut queue = self.script.lock().unwrap();
            if queue.is_empty() {
                Script::Real
            } else {
                queue.remove(0)
            }
        };
        let outcome = match script {
            Script::Real => {
                let mut lake = self.lake.lock().unwrap();
                let key = (manifest.partition.as_str().to_owned(), manifest.window_id.0);
                if lake.fenced.contains(&key) {
                    CommitOutcome::Aborted
                } else {
                    Self::apply(&mut lake, &manifest, &watermarks);
                    CommitOutcome::Committed
                }
            }
            Script::RejectWithoutApplying => CommitOutcome::Aborted,
            Script::Indeterminate { landed } => {
                if landed {
                    Self::apply(&mut self.lake.lock().unwrap(), &manifest, &watermarks);
                }
                CommitOutcome::Indeterminate
            }
        };
        ready(Ok(outcome))
    }

    fn replace_files(
        &self,
        _remove: Vec<duckspout_types::PartName>,
        _add: Vec<duckspout_types::PartName>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        ready(Err(LakeError::NotImplemented("replace_files")))
    }

    fn evolve_schema(
        &self,
        _change: duckspout_types::SchemaEvolution,
    ) -> BoxFuture<'_, Result<(), LakeError>> {
        ready(Err(LakeError::NotImplemented("evolve_schema")))
    }

    fn expire(
        &self,
        _parts: Vec<duckspout_types::PartName>,
    ) -> BoxFuture<'_, Result<(), LakeError>> {
        ready(Err(LakeError::NotImplemented("expire")))
    }

    fn read_watermarks(
        &self,
        partitions: Vec<PartitionId>,
    ) -> BoxFuture<'_, Result<Vec<WatermarkRow>, LakeError>> {
        self.read_back_calls.fetch_add(1, Ordering::SeqCst);
        {
            let mut fails = self.read_back_fails.lock().unwrap();
            if *fails {
                *fails = false;
                return ready(Err(LakeError::Backend("catalog unreachable".into())));
            }
        }
        let lake = self.lake.lock().unwrap();
        let rows = partitions
            .into_iter()
            .filter_map(|p| {
                lake.watermarks.get(p.as_str()).map(|ms| WatermarkRow {
                    partition: p,
                    complete_through_ms: *ms,
                })
            })
            .collect();
        ready(Ok(rows))
    }

    fn attach_info(&self) -> BoxFuture<'_, Result<duckspout_types::AttachInfo, LakeError>> {
        ready(Err(LakeError::NotImplemented("attach_info")))
    }
}

// ---------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------

struct Harness {
    seal: Arc<SealDouble>,
    ledger: Arc<LedgerDouble>,
    committer: Arc<CommitterDouble>,
    parts_store: Arc<object_store::memory::InMemory>,
    scratch: Arc<MemStorage>,
    clock: Arc<TestClock>,
    coordinator: DrainCoordinator,
}

fn harness_with(ledger: LedgerDouble, committer: Arc<CommitterDouble>) -> Harness {
    let scratch = Arc::new(MemStorage::default());
    let seal = Arc::new(SealDouble::new(Arc::clone(&scratch)));
    let ledger = Arc::new(ledger);
    let parts_store = Arc::new(object_store::memory::InMemory::new());
    let clock = Arc::new(TestClock::default());
    let coordinator = DrainCoordinator::new(
        Arc::clone(&seal) as Arc<dyn SealSurface>,
        Arc::clone(&ledger) as Arc<dyn WatermarkBookkeeping>,
        Arc::clone(&committer) as Arc<dyn LakeCommitter>,
        Arc::clone(&parts_store) as Arc<dyn object_store::ObjectStore>,
        Arc::clone(&scratch) as Arc<dyn Storage>,
        Arc::clone(&clock) as Arc<dyn Clock>,
        DrainConfig::default(),
    );
    Harness {
        seal,
        ledger,
        committer,
        parts_store,
        scratch,
        clock,
        coordinator,
    }
}

fn harness() -> Harness {
    harness_with(LedgerDouble::default(), Arc::new(CommitterDouble::new()))
}

fn plan() -> DatasetDrainPlan {
    DatasetDrainPlan {
        order_by: vec!["ts".into()],
        event_time_column: "ts".into(),
        dedup_key: None,
    }
}

fn ds() -> DatasetId {
    DatasetId::new("logs")
}

fn p() -> PartitionId {
    PartitionId::new("tenant1.0")
}

fn drain(h: &Harness, window: u64) -> Result<DrainOutcome, DrainError> {
    block_on(
        h.coordinator
            .drain_window(&ds(), &p(), WindowId(window), &plan()),
    )
}

fn object_bytes(h: &Harness, name: &str) -> Option<Vec<u8>> {
    use object_store::ObjectStoreExt as _;
    let path = object_store::path::Path::from(name);
    block_on(async {
        match h.parts_store.get(&path).await {
            Ok(got) => Some(got.bytes().await.expect("object bytes").to_vec()),
            Err(_) => None,
        }
    })
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

#[test]
fn committed_path_puts_records_and_drops() {
    let h = harness();
    let outcome = drain(&h, 0).expect("drain succeeds");

    // One PUT at the deterministic name, exact sealed bytes (§6.1, §6.5).
    let name = part_name(&ds(), &p(), WindowId(0), &PartDiscriminator::Window);
    assert_eq!(
        object_bytes(&h, name.as_str()).expect("part was PUT"),
        b"parquet-bytes-w0"
    );

    // The commit carried the manifest verbatim from the seal bookkeeping
    // (coverage and dedup_removed pass-through, §6.8) and the previewed
    // watermark row (§6.4).
    let manifests = h.committer.committed_manifests();
    assert_eq!(manifests.len(), 1);
    let m = &manifests[0];
    assert_eq!(m.window_id, WindowId(0));
    assert_eq!(m.rows, 5);
    assert_eq!(m.event_time_min_ms, 100);
    assert_eq!(m.event_time_max_ms, 1_000);
    assert_eq!(m.dedup_removed, 0);
    assert_eq!(m.parts, vec![name]);
    assert_eq!(
        m.origin_coverage,
        vec![OriginSeqRange {
            origin: "o1".into(),
            first_seq: 1,
            last_seq: 5,
        }]
    );

    assert_eq!(
        outcome,
        DrainOutcome::Committed {
            watermark: Some(WatermarkRow {
                partition: p(),
                complete_through_ms: 1_000,
            })
        }
    );
    assert_eq!(h.ledger.recorded(), vec![0], "bookkeeping recorded");
    assert_eq!(h.seal.dropped_windows(), vec![0], "DropWindow ran (§6.9)");
    assert_eq!(
        h.seal.dropped_coverage(),
        vec![m.origin_coverage.clone()],
        "the drop is guarded by exactly the committed coverage (TN-32)"
    );
    assert!(
        !h.scratch.contains("seal/w0.parquet"),
        "scratch discarded after commit"
    );
    assert_eq!(h.committer.read_back_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn dedup_removed_passes_through_verbatim() {
    let scratch = Arc::new(MemStorage::default());
    let mut seal = SealDouble::new(Arc::clone(&scratch));
    seal.dedup_removed = 3;
    seal.rows = 2;
    let seal = Arc::new(seal);
    let committer = Arc::new(CommitterDouble::new());
    let coordinator = DrainCoordinator::new(
        Arc::clone(&seal) as _,
        Arc::new(LedgerDouble::default()) as _,
        Arc::clone(&committer) as _,
        Arc::new(object_store::memory::InMemory::new()) as _,
        Arc::clone(&scratch) as _,
        Arc::new(TestClock::default()) as _,
        DrainConfig::default(),
    );
    block_on(coordinator.drain_window(&ds(), &p(), WindowId(0), &plan())).expect("drain succeeds");
    let manifests = committer.committed_manifests();
    assert_eq!(
        manifests[0].dedup_removed, 3,
        "verbatim pass-through (§6.2)"
    );
    assert_eq!(manifests[0].rows, 2);
}

#[test]
fn aborted_without_a_standing_commit_requeues() {
    let h = harness();
    h.committer.push_script(Script::RejectWithoutApplying);
    let outcome = drain(&h, 0).expect("drain resolves");
    assert_eq!(
        outcome,
        DrainOutcome::Requeue(RequeueReason::RejectedByBackend)
    );
    assert_eq!(
        h.committer.read_back_calls.load(Ordering::SeqCst),
        1,
        "the loser takes exactly one read-back to learn nothing stands"
    );
    assert!(h.ledger.recorded().is_empty(), "nothing recorded");
    assert!(h.seal.dropped_windows().is_empty(), "staging kept (R-5)");

    // The requeue is retry-safe: the next attempt recomputes and commits.
    let retry = drain(&h, 0).expect("retry succeeds");
    assert!(matches!(retry, DrainOutcome::Committed { .. }));
    assert_eq!(h.ledger.recorded(), vec![0]);
    assert_eq!(h.seal.dropped_windows(), vec![0]);
}

#[test]
fn indeterminate_landed_resolves_to_committed_without_resubmit() {
    let h = harness();
    h.committer
        .push_script(Script::Indeterminate { landed: true });
    let outcome = drain(&h, 0).expect("drain resolves");
    assert_eq!(
        outcome,
        DrainOutcome::Committed {
            watermark: Some(WatermarkRow {
                partition: p(),
                complete_through_ms: 1_000,
            })
        }
    );
    assert_eq!(
        h.committer.commit_calls.load(Ordering::SeqCst),
        1,
        "never resubmitted (§6.5: read-back, not blind retry)"
    );
    assert_eq!(
        h.committer.read_back_calls.load(Ordering::SeqCst),
        1,
        "exactly one read-back"
    );
    assert_eq!(
        h.ledger.recorded(),
        vec![0],
        "recorded after the read-back proof"
    );
    assert_eq!(h.seal.dropped_windows(), vec![0]);
}

#[test]
fn indeterminate_lost_requeues_and_the_retry_lands() {
    let h = harness();
    h.committer
        .push_script(Script::Indeterminate { landed: false });
    let outcome = drain(&h, 0).expect("drain resolves");
    assert_eq!(outcome, DrainOutcome::Requeue(RequeueReason::CommitLost));
    assert_eq!(
        h.committer.read_back_calls.load(Ordering::SeqCst),
        1,
        "exactly one read-back"
    );
    assert!(h.ledger.recorded().is_empty());
    assert!(
        h.seal.dropped_windows().is_empty(),
        "the window stays staged (R-5)"
    );

    // Retry: full recomputation (R-2), same deterministic name, lands.
    let retry = drain(&h, 0).expect("retry succeeds");
    assert!(matches!(retry, DrainOutcome::Committed { .. }));
    assert_eq!(h.committer.commit_calls.load(Ordering::SeqCst), 2);
    let name = part_name(&ds(), &p(), WindowId(0), &PartDiscriminator::Window);
    assert_eq!(
        object_bytes(&h, name.as_str()).expect("part stands"),
        b"parquet-bytes-w0",
        "the re-PUT overwrote byte-identical content (§6.5)"
    );
}

#[test]
fn indeterminate_with_catalog_down_is_a_drain_stall() {
    let h = harness();
    h.committer
        .push_script(Script::Indeterminate { landed: true });
    h.committer.fail_next_read_back();
    let outcome = drain(&h, 0).expect("drain resolves");
    assert_eq!(
        outcome,
        DrainOutcome::Requeue(RequeueReason::CatalogUnavailable)
    );
    assert_eq!(
        h.committer.read_back_calls.load(Ordering::SeqCst),
        1,
        "one read-back, no loop"
    );
    assert!(
        h.ledger.recorded().is_empty(),
        "nothing recorded on an unproven outcome"
    );
    assert!(h.seal.dropped_windows().is_empty());
}

#[test]
fn indeterminate_without_observable_advance_is_unproven() {
    // A blocked partition carries no watermark row, so the read-back can
    // prove nothing; the drain must park the window, never guess (R-3).
    let ledger = LedgerDouble {
        blocked: true,
        ..LedgerDouble::default()
    };
    let h = harness_with(ledger, Arc::new(CommitterDouble::new()));
    h.committer
        .push_script(Script::Indeterminate { landed: true });
    let outcome = drain(&h, 0).expect("drain resolves");
    assert_eq!(outcome, DrainOutcome::Requeue(RequeueReason::Unproven));
    assert_eq!(h.committer.read_back_calls.load(Ordering::SeqCst), 1);
    assert!(h.ledger.recorded().is_empty());
}

#[test]
fn replayed_window_completes_cleanup_without_committing() {
    // §6.9 crash-between-commit-and-cleanup: the bookkeeping already holds
    // window 0; the re-attempt completes DropWindow and never seals,
    // PUTs, or commits — "already stands" via expected, not re-record.
    let recorded_range = OriginSeqRange {
        origin: "o1".into(),
        first_seq: 1,
        last_seq: 5,
    };
    let ledger = LedgerDouble::with_recorded(1, Some(1_000), &[(0, recorded_range.clone())]);
    let h = harness_with(ledger, Arc::new(CommitterDouble::new()));
    let outcome = drain(&h, 0).expect("drain resolves");
    assert_eq!(outcome, DrainOutcome::AlreadyCommitted);
    assert_eq!(h.seal.seal_calls.load(Ordering::SeqCst), 0, "no reseal");
    assert_eq!(
        h.committer.commit_calls.load(Ordering::SeqCst),
        0,
        "no commit"
    );
    assert_eq!(
        h.seal.dropped_windows(),
        vec![0],
        "pending DropWindow completed"
    );
    assert_eq!(
        h.seal.dropped_coverage(),
        vec![vec![recorded_range]],
        "the recovery drop is guarded by the RECORDED coverage (TN-32)"
    );
    assert!(h.ledger.recorded().is_empty(), "never blindly re-recorded");
}

#[test]
fn window_ahead_of_the_dense_sequence_fails_closed() {
    let h = harness();
    let err = drain(&h, 1).expect_err("window 1 ahead of dense-next 0");
    assert!(matches!(
        err,
        DrainError::WindowAhead {
            expected: WindowId(0),
            got: WindowId(1),
            ..
        }
    ));
    assert_eq!(h.seal.seal_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn late_arrival_hold_gates_eligibility() {
    let h = harness();
    h.seal.offer(DrainableWindow {
        dataset: ds(),
        partition: p(),
        window: WindowId(0),
        closed_at_ms: 60_000,
    });
    let lateness = DrainConfig::default().allowed_lateness_ms;

    h.clock.set_ms(60_000 + lateness - 1);
    let before = block_on(h.coordinator.eligible_windows()).expect("enumerates");
    assert!(before.is_empty(), "held: one ms short of the §6.3 hold");

    h.clock.set_ms(60_000 + lateness);
    let after = block_on(h.coordinator.eligible_windows()).expect("enumerates");
    assert_eq!(
        after.len(),
        1,
        "eligible exactly at close + allowed_lateness"
    );
    assert_eq!(after[0].window, WindowId(0));
}

#[test]
fn racing_drains_exactly_one_winner() {
    // Two drainers race the same window through one fenced lake — the
    // §6.6 choreography-level guarantee: exactly one commits; the loser is
    // Aborted by the fence, takes the read-back path, proves the commit
    // stands, and completes locally without double-committing. (The
    // backend-level fence proof is #36's DoD; the port double here honors
    // the port contract's fence.)
    let committer = Arc::new(CommitterDouble::new());
    let h1 = harness_with(LedgerDouble::default(), Arc::clone(&committer));
    let h2 = harness_with(LedgerDouble::default(), Arc::clone(&committer));

    let barrier = Arc::new(Barrier::new(2));
    let outcomes: Vec<DrainOutcome> = std::thread::scope(|scope| {
        let handles: Vec<_> = [&h1, &h2]
            .into_iter()
            .map(|h| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    drain(h, 0).expect("both attempts resolve")
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|j| j.join().expect("no panic"))
            .collect()
    });

    let wins = outcomes
        .iter()
        .filter(|o| matches!(o, DrainOutcome::Committed { .. }))
        .count();
    let yields = outcomes
        .iter()
        .filter(|o| matches!(o, DrainOutcome::AlreadyCommitted))
        .count();
    assert_eq!((wins, yields), (1, 1), "exactly one winner: {outcomes:?}");

    assert_eq!(
        committer.committed_manifests().len(),
        1,
        "one commit stands in the lake"
    );
    assert_eq!(
        committer.commit_calls.load(Ordering::SeqCst),
        2,
        "both raced to commit"
    );
    assert_eq!(
        committer.read_back_calls.load(Ordering::SeqCst),
        1,
        "the loser resolved via exactly one read-back"
    );
    // Both sides recorded the standing commit and completed DropWindow.
    assert_eq!(h1.ledger.recorded(), vec![0]);
    assert_eq!(h2.ledger.recorded(), vec![0]);
    assert_eq!(h1.seal.dropped_windows(), vec![0]);
    assert_eq!(h2.seal.dropped_windows(), vec![0]);
}
