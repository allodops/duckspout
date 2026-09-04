//! The port traits (ADR-0008, D-2).
//!
//! Every cross-crate-consumed port trait is defined here; home crates
//! re-export them (`pub use duckspout_types::…`) and own everything beyond
//! the bare signature. This is the only acyclic reading of §10.1's layering:
//! with all protocol×protocol crate edges banned, a port consumed across
//! crates must live in types.
//!
//! Determinism (D-2): protocol crates never touch `tokio::net`,
//! `Instant::now`, `SystemTime::now`, `thread_rng`, or `std::process` — the
//! runtime reaches them only through [`Clock`], [`Scheduler`], [`Transport`],
//! and [`Storage`], for which `duckspout-ctk` provides deterministic doubles.
//!
//! Async style: methods take owned arguments and return [`BoxFuture`], which
//! keeps every trait here object-safe (`dyn Transport` etc.) without an
//! `async-trait` dependency.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::dataset::DatasetKind;
use crate::ids::{DatasetId, PartName, PartitionId, TenantId, WindowId};
use crate::manifest::{OriginSeqRange, WindowManifest};
use crate::otlp::{GrpcCode, OtlpErrorClass};
use crate::watermark::WatermarkRow;

use crate::ids::NodeId;

/// A boxed, `Send` future — the return shape of every async port method.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// The time port (D-2). No invariant reads a clock (§3 — correctness is
/// clock-independent by construction); wall time exists for event-time
/// bookkeeping and skew *warnings* only (§9.6.3).
pub trait Clock: Send + Sync {
    /// Monotonic nanoseconds since an arbitrary process epoch.
    fn monotonic_nanos(&self) -> u64;

    /// Wall-clock Unix milliseconds.
    fn wall_unix_ms(&self) -> i64;
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// The task/timer port (D-2). Protocol crates spawn nothing themselves; the
/// runtime (or the CTK's seedable double) owns execution order.
pub trait Scheduler: Send + Sync {
    /// Submits a task for execution.
    fn spawn(&self, task: BoxFuture<'static, ()>);

    /// A future that completes once `nanos` of scheduler time have elapsed.
    fn sleep(&self, nanos: u64) -> BoxFuture<'static, ()>;
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/// A transport failure, typed for the protocol crates' error handling.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The peer is not reachable through this transport instance.
    #[error("peer {0} unknown to this transport")]
    UnknownPeer(NodeId),
    /// The transport is closed; no further sends or receives will succeed.
    #[error("transport closed")]
    Closed,
}

/// The peer-messaging port (D-2): `Forward` / `Receipt` / registry traffic
/// rides this. Message loss is silent to the sender, as on a real network —
/// delivery evidence is the peer's `Receipt`, never the send result (§5).
pub trait Transport: Send + Sync {
    /// Sends one payload toward a peer. `Ok(())` means handed to the
    /// transport, **not** delivered.
    fn send(&self, to: NodeId, payload: Bytes) -> BoxFuture<'_, Result<(), TransportError>>;

    /// Receives the next inbound `(sender, payload)`.
    fn recv(&self) -> BoxFuture<'_, Result<(NodeId, Bytes), TransportError>>;
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// A location in the node-local durable store, relative to the store's root.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoragePath(String);

impl StoragePath {
    /// Wraps a store-relative path.
    #[must_use]
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// The path as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoragePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A storage failure, typed so `StageCommit` can fail a batch without acking
/// it (§4.3).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The path does not exist (or its name was never made durable).
    #[error("not found: {0}")]
    NotFound(StoragePath),
    /// An fsync was refused or failed; the write's durability is unknown.
    #[error("fsync failed: {0}")]
    FsyncFailed(StoragePath),
    /// A torn (partial) write was detected on read-back (ADR-0003).
    #[error("torn write detected: {0}")]
    TornWrite(StoragePath),
    /// Any other backend failure, described.
    #[error("storage backend: {0}")]
    Backend(String),
}

/// The node-local durable-storage port with explicit fsync discipline
/// (D-2, ADR-0003). There is no separate WAL crate: `DuckDB` persistent tables
/// with fsync-on-commit are the durability primitive ("WAL = hot", §4.2);
/// directory fsync, torn-write detection, and group commit off the reactor
/// live behind this port and its CTK fault injectors.
pub trait Storage: Send + Sync {
    /// Writes `data` at `path`. **Not durable** until [`Storage::fsync_file`]
    /// on the path and [`Storage::fsync_dir`] on its directory both succeed.
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Reads the content at `path`.
    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>>;

    /// Removes `path`. The removal is durable only after
    /// [`Storage::fsync_dir`] on its directory.
    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Makes the *content* at `path` durable.
    fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Makes the directory's *entries* durable — a created file whose
    /// directory was never fsynced may not survive a crash.
    fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>>;
}

// ---------------------------------------------------------------------------
// AcceptAdapter (v0.1)
// ---------------------------------------------------------------------------

/// An accept-side rejection produced while decoding (obligations 1–2 of
/// §4.1.2). Every variant is a permanent, non-retryable rejection: retrying
/// the same bytes can never succeed (§4.1.2's `MalformedPermanent` class).
#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    /// The payload is malformed for this protocol; permanent (§4.1.2).
    #[error("malformed payload: {0}")]
    Malformed(String),
    /// The tenant header failed §2.2's validation (charset, length ≤ 150,
    /// leading `_` reserved for system tenants).
    #[error("invalid tenant identity: {0}")]
    InvalidTenant(String),
}

/// One wire request as the accept seam receives it, protocol-opaque.
#[derive(Debug, Clone)]
pub struct WireRequest {
    /// The undecoded wire payload.
    pub payload: Bytes,
    /// `X-Scope-OrgID` from the mTLS-verified edge, when present (§4.1.2).
    pub tenant_header: Option<String>,
    /// The optional `x-duckspout-idempotency-key` header (§4.1.2, §4.4.1).
    pub idempotency_key: Option<String>,
}

/// A decoded batch: typed record batches for a declared dataset (§4.1.2
/// obligation 1).
#[derive(Debug, Clone)]
pub struct DecodedBatch {
    /// The declared dataset the batch targets.
    pub dataset: DatasetId,
    /// The dataset's kind (§2).
    pub kind: DatasetKind,
    /// The extracted tenant identity.
    pub tenant: TenantId,
    /// The idempotency token, when the client sent one — takes precedence
    /// over the content hash in the dedup key (§4.4.1).
    pub idempotency_key: Option<String>,
    /// The decoded records as one **Arrow IPC stream** (any number of record
    /// batches over one schema), produced with the workspace's compat-pinned
    /// arrow (compat-matrix row 1; `duckspout-staging` re-exports it). The
    /// IPC encoding is what keeps this crate free of the arrow dependency —
    /// this crate stays no-I/O and near-leafless (§10.1) — at the cost of one
    /// in-memory encode/decode per batch on the accept path, bounded by
    /// `max_payload_bytes` (§4.6).
    pub records: Bytes,
}

/// A protocol-native error response, as mapped by an adapter (obligation 3 of
/// §4.1.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    /// The gRPC status code (for OTLP/gRPC; OTLP/HTTP maps it per spec).
    pub grpc_code: GrpcCode,
    /// Whether a `RetryInfo` detail accompanies the status (§4.5, §4.6).
    pub retry_info: bool,
}

/// The accept-adapter port, v0.1 (§4.1.2). An adapter's obligations are
/// exactly three: decode, extract identity, and map outcomes onto the
/// protocol's native error vocabulary. The trait boundary exists so that
/// durability semantics are adapter-invariant — no adapter touches the ack
/// path.
pub trait AcceptAdapter: Send + Sync {
    /// A stable protocol name for registration, e.g. `"otlp-grpc"`.
    fn protocol(&self) -> &'static str;

    /// Decodes a wire payload into typed record batches for a declared
    /// dataset and extracts tenant identity plus the optional idempotency
    /// key (obligations 1–2).
    ///
    /// # Errors
    ///
    /// Returns an [`AcceptError`] for malformed payloads or missing identity.
    fn decode(&self, request: WireRequest) -> Result<DecodedBatch, AcceptError>;

    /// Maps an admission/overload outcome onto the protocol's native error
    /// vocabulary (obligation 3) — for OTLP, the spec's own retryable status
    /// table with no invented extensions.
    fn map_error(&self, class: OtlpErrorClass) -> WireError;
}

// ---------------------------------------------------------------------------
// StageCommitter (v0.1)
// ---------------------------------------------------------------------------

/// Per-origin seq coverage of one committed partition — exactly what
/// `ClientAck` evidence needs (§4.3) and what `Forward` ships (§4.2.3).
/// Serializable because it is the dedup window's stored outcome (§4.4.1):
/// a duplicate replays the original's ack evidence verbatim.
///
/// Defined here (ADR-0008: it crosses the accept↔staging boundary inside
/// [`StageCommitter`]'s signature); `duckspout-staging` re-exports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedCoverage {
    /// The partition the rows landed in.
    pub partition: PartitionId,
    /// The dense, contiguous seq range this commit covers.
    pub range: OriginSeqRange,
}

/// A staging failure as the accept path sees it (§4.3, §4.5): every
/// variant is a **not-staged, not-acked** outcome — no partial state
/// exists. The ladder variants carry the §4.5 growing retry delay so
/// adapters can attach the spec's `RetryInfo` without knowing the measure.
#[derive(Debug, thiserror::Error)]
pub enum StageError {
    /// The [`DecodedBatch::records`] bytes are not a decodable Arrow IPC
    /// stream — an adapter↔stager contract breach, not client-malformed
    /// input (the client's payload already decoded; §4.1.2's malformed class
    /// is the adapter's to raise).
    #[error("records are not a decodable Arrow IPC stream: {0}")]
    MalformedRecords(String),
    /// The staging backend failed the commit; nothing was staged and nothing
    /// is acked (§4.3).
    #[error("staging backend: {0}")]
    Backend(String),
    /// Overload-ladder rung 2 (§4.5, M ≥ 95%): admission gated; the batch
    /// was not staged. Maps to `OtlpErrorClass::Throttled` — UNAVAILABLE
    /// with the carried `RetryInfo` delay.
    #[error("throttled: staging at rung 2, retry in {retry_after_ms} ms")]
    Throttled {
        /// §4.5's growing delay, a pure function of the measure.
        retry_after_ms: u64,
    },
    /// Overload-ladder rung 3 (§4.5, M ≥ 100%): new writes refused. Maps to
    /// `OtlpErrorClass::RefusingIngest` — still the retryable vocabulary.
    #[error("refusing ingest: staging at rung 3, retry in {retry_after_ms} ms")]
    RefusingIngest {
        /// The retry delay carried on the wire (the §4.5 ceiling).
        retry_after_ms: u64,
    },
}

/// A successful `DedupCheck` + `StageCommit` resolution (§4.3, §4.4.1):
/// every non-error port call lands on exactly one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageOutcome {
    /// A fresh batch, staged durably in one transaction. The coverage is
    /// the `ClientAck` evidence (§4.3).
    Committed(Vec<StagedCoverage>),
    /// `DedupCheck` hit an entry whose ack evidence is complete (the
    /// acked-entry branch, and the `AtRF` branch once receipts reached RF —
    /// §3.3, §4.4.1): the original outcome is replayed verbatim, nothing is
    /// re-staged (R-2).
    DuplicateAcked(Vec<StagedCoverage>),
    /// `DedupCheck` hit an entry still short of RF (§4.4.1): the retrying
    /// client gets the retryable signal (`OtlpErrorClass::DuplicateInFlight`)
    /// and keeps retrying until receipts complete. Unreachable at RF = 1
    /// (v0.1: an entry is ack-complete the moment its commit returns); the
    /// branch exists because §3's `DedupCheck` has it, and replication (v0.2)
    /// makes it live.
    DuplicateInFlight,
}

/// The staging port the accept path commits through (§4.3–§4.5, ADR-0008):
/// `DedupCheck` + `StageCommit` as one call, gated by the overload ladder.
///
/// Contract, in §4.3's vocabulary:
///
/// - `Ok(Committed)` means `StageCommit` returned — the whole batch is
///   fsynced-durable locally, atomically, with its dedup-window entry
///   written in the same transaction (§4.4.1). The caller may ack once the
///   replication floor is met (v0.1 single-node: RF = 1, so `Committed`
///   **is** ack-worthy; the RF−1 `Receipt` wait of §4.3 arrives with
///   replication at v0.2 and slots between this call and the ack).
/// - `Ok(DuplicateAcked)` replays the original outcome without re-staging
///   (R-2); `Ok(DuplicateInFlight)` is the pre-RF duplicate signal.
/// - `Err` means nothing was staged and nothing may be acked — including
///   the ladder's `Throttled`/`RefusingIngest` admission refusals (§4.5),
///   which gate **admission only**: a `StageCommit` already begun always
///   completes, whatever the rung ("the ladder gates admission, never
///   promises made"). There is no partial outcome — a batch stages in its
///   entirety or not at all (§4.1.2).
///
/// Home crate: `duckspout-staging` (the WAL=hot engine implements this; the
/// engine is blocking, so implementations may resolve the returned future
/// synchronously — callers embed the port off their reactor, ADR-0003).
pub trait StageCommitter: Send + Sync {
    /// Resolves `batch` through `DedupCheck` and, when fresh, stages it in
    /// one durable transaction (§4.3, §4.4.1).
    ///
    /// # Errors
    ///
    /// [`StageError`] — the batch is not staged and must not be acked.
    fn stage_commit(&self, batch: DecodedBatch) -> BoxFuture<'_, Result<StageOutcome, StageError>>;
}

// ---------------------------------------------------------------------------
// ReplicaLog (v0.2)
// ---------------------------------------------------------------------------

/// One forwarded replication record, exactly as the origin staged it (§5.4
/// `Forward`): the peer applies it at the origin's own `(partition, seq)`
/// range — it never assigns either itself, unlike [`StageCommitter`]'s own
/// commit, which mints a fresh `seq` locally. This is why `PeerApply` cannot
/// reuse [`StageCommitter::stage_commit`]: the two operations differ in
/// exactly the one thing that matters for gap-freedom (who assigns `seq`).
///
/// `range` mirrors [`OriginSeqRange`]: a `StageCommit` may mint more than one
/// sequence number in a single durable transaction (a multi-row batch), so a
/// `Forward` ships the whole contiguous range that commit produced, not one
/// `seq` at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedRecord {
    /// The partition the range belongs to.
    pub partition: PartitionId,
    /// The origin-assigned, contiguous `(origin, seq)` range this record
    /// covers (§4.2.3, §5.4).
    pub range: OriginSeqRange,
    /// The dense per-partition window the rows were staged into (§2.3).
    pub window: WindowId,
    /// The dataset the rows belong to.
    pub dataset: DatasetId,
    /// The rows themselves, as one Arrow IPC stream — opaque here exactly as
    /// [`DecodedBatch::records`] is (§10.1: this crate stays near-leafless).
    pub records: Bytes,
}

/// A `PeerApply` durable-apply failure (§5.4): the record is **not**
/// staged and **must not** be receipted (the exact bug PR #192's ACPR pass
/// on the P model's `Node.p` caught and fixed: a receipt for a record that
/// was never actually staged).
#[derive(Debug, thiserror::Error)]
pub enum ReplicaApplyError {
    /// The replica-log backend failed the durable apply, described.
    #[error("replica-log backend: {0}")]
    Backend(String),
}

/// The peer-side durable-apply boundary `PeerApply` (§5.4) commits through:
/// "the peer applies the batch into its hot staging table for the
/// partition" (`docs/design/replication.md` §4). Defined here, not in
/// `duckspout-staging` directly (ADR-0008: protocol×protocol edges are
/// banned, so a port crossing the replication↔staging boundary lives in
/// `duckspout-types`, exactly as [`SealSurface`] does for the drain↔staging
/// boundary) — `duckspout-staging` is expected to implement it over the same
/// hot engine [`StageCommitter`] commits through ("the table is the log," §4:
/// one storage engine, one fsync discipline, one recovery path for both the
/// origin's own rows and a peer's applied ones).
///
/// `duckspout-replication` is the sole consumer; wiring a concrete
/// `duckspout-staging` implementation into the daemon's composition root is
/// tracked as follow-up work (issue #193), not done in the same change that
/// defines the port — matching how this workspace lands a port and its
/// implementation as separately reviewable steps elsewhere (`SealSurface`
/// landed ahead of the drain that fully exercises it).
///
/// Every method reads or mutates state for one `(origin, partition)` pair —
/// gap-freedom and fencing are both scoped that way (§5.4; generalized here
/// from the origin-only granularity the checker-validated P model uses,
/// since the real system has a partition dimension the P model deliberately
/// omits, `docs/design/p-tla-correspondence.md` §3.2).
pub trait ReplicaLog: Send + Sync {
    /// The highest contiguous `seq` this peer has durably applied for
    /// `(origin, partition)` — `0` when nothing has been applied yet. This
    /// is `AppliedThru` (`specs/DuckSpoutCore.tla`) and the receipt
    /// watermark `Receipt` ships (§5.4).
    fn applied_thru(&self, origin: &NodeId, partition: &PartitionId) -> u64;

    /// Whether `seq` for `(origin, partition)` has already been durably
    /// applied. `PeerApply`'s idempotent-duplicate path (`seq <=
    /// applied_thru`) calls this **before** re-receipting rather than
    /// trusting the incoming Forward's own claim — the defensive guard
    /// PR #192's ACPR pass added to the P model's `Node.p` after finding
    /// that an incoming message reusing an already-used `seq` for a
    /// genuinely different record could otherwise fabricate a receipt for a
    /// record this peer never actually staged.
    fn has_applied(&self, origin: &NodeId, partition: &PartitionId, seq: u64) -> bool;

    /// Durably applies `record` into this peer's hot staging table, exactly
    /// as `StageCommit` durably applies the origin's own rows (one fsynced
    /// transaction, §4.2 A1). Callers (`duckspout-replication`) call this
    /// only after `PeerApply`'s fencing and gap-freedom guards both pass —
    /// this port performs no guard evaluation of its own.
    ///
    /// # Errors
    ///
    /// [`ReplicaApplyError`] — the record is not staged; the caller must not
    /// receipt it (§5.4).
    fn apply(&self, record: ForwardedRecord) -> BoxFuture<'_, Result<(), ReplicaApplyError>>;
}

// ---------------------------------------------------------------------------
// SealSurface (v0.1)
// ---------------------------------------------------------------------------

/// One closed micro-window the staging side offers for drain (§6.2). The
/// close instant feeds the §6.3 lateness hold: the drain schedules a window
/// only once `drain.allowed_lateness` has elapsed past `closed_at_ms`, so
/// ordinary network-delayed rows drain into their home window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainableWindow {
    /// The dataset the window belongs to.
    pub dataset: DatasetId,
    /// The partition the window belongs to.
    pub partition: PartitionId,
    /// The dense per-partition window sequence number.
    pub window: WindowId,
    /// When the window closed to ordinary ingest, Unix milliseconds — the
    /// instant the §6.3 lateness hold starts counting from.
    pub closed_at_ms: i64,
}

/// What one `SealPart` run must do (§6.2): the one sorted, deduplicating
/// `COPY` over the window's staging table, described declaratively so the
/// drain stays engine-neutral.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealRequest {
    /// The dataset whose window is sealed.
    pub dataset: DatasetId,
    /// The partition whose window is sealed.
    pub partition: PartitionId,
    /// The window to seal.
    pub window: WindowId,
    /// The part's sort order — the dataset's declared `sort_key`, or the
    /// event-time default (§6.2). Column names of the staged payload.
    pub order_by: Vec<String>,
    /// The event-time column (a staged `TIMESTAMP` column): its min/max
    /// over the sealed rows become the manifest's event-time statistics.
    pub event_time_column: String,
    /// Drain-time dedup key (§6.2): `Some(cols)` keeps, per distinct key,
    /// the deterministic smallest-`(origin, seq)` winner and counts the rest
    /// as `dedup_removed`; `None` seals every row.
    pub dedup_key: Option<Vec<String>>,
}

/// A sealed part plus the bookkeeping the window manifest needs (§6.2,
/// §6.8). Coverage is accounted **pre-dedup**: removed duplicates are still
/// covered rows — that is why `dedup_removed` is recorded at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedPart {
    /// Node-local scratch location of the sealed Parquet bytes, relative to
    /// the composition's [`Storage`] root. The drain PUTs and then deletes
    /// it; it is never served from.
    pub path: StoragePath,
    /// Rows sealed into the part (post-dedup).
    pub rows: u64,
    /// Event-time minimum over the sealed rows, Unix milliseconds. `0` for
    /// an empty window.
    pub event_time_min_ms: i64,
    /// Event-time maximum over the sealed rows, Unix milliseconds. `0` for
    /// an empty window.
    pub event_time_max_ms: i64,
    /// Rows removed by drain-time dedup (§6.2) — the manifest's
    /// `dedup_removed`, passed through verbatim.
    pub dedup_removed: u64,
    /// Per-origin seq coverage of the window's staged rows (pre-dedup),
    /// as maximal contiguous runs sorted by `(origin, first_seq)`.
    pub origin_coverage: Vec<OriginSeqRange>,
}

/// A seal-side failure. Every variant is a not-sealed outcome; the window
/// stays in staging untouched (R-5: acked data leaves staging only by
/// successful drain).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SealError {
    /// The window is not registered on this node.
    #[error("window {window} of ({dataset}, {partition}) is unknown to staging")]
    UnknownWindow {
        /// The dataset addressed.
        dataset: DatasetId,
        /// The partition addressed.
        partition: PartitionId,
        /// The window addressed.
        window: WindowId,
    },
    /// The staging backend failed the operation, described.
    #[error("seal backend: {0}")]
    Backend(String),
}

/// How a coverage-guarded `DropWindow` ended (§6.9; TLC finding TN-32,
/// PR #137): only rows covered by the lake's committed coverage (or the
/// loss ledger) may leave staging — an uncovered row is durable, acked
/// data that will drain later (as a supplement, §6.6) and must survive the
/// winner's drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropOutcome {
    /// Every staged row was covered: the whole window table is gone (the
    /// O(1) path — the common case, where the committed coverage is the
    /// entire window).
    Dropped,
    /// Uncovered rows existed (e.g. a late arrival landed between the seal
    /// `COPY` and the drop): only the covered rows were deleted; the window
    /// table survives holding exactly the residue, which a later
    /// supplement drain accounts for.
    ResidueKept {
        /// The uncovered rows left in the window.
        rows: u64,
    },
    /// The window was already gone — idempotent re-drop after a crash or a
    /// racing completion.
    AlreadyGone,
}

/// The seal-side read surface of staging (§6.2) — the port through which the
/// drain consumes staging (ADR-0008: protocol×protocol edges are banned, so
/// the trait lives here; `duckspout-staging` implements it over its engine).
///
/// Implementations back onto a blocking embedded engine; the returned
/// futures may therefore complete work synchronously — callers embed the
/// surface off their reactor exactly as for the engine itself.
pub trait SealSurface: Send + Sync {
    /// The closed micro-windows currently offered for drain, sorted by
    /// `(dataset, partition, window)`. The open (current) window of a
    /// partition is never offered; the §6.3 lateness hold on the closed
    /// ones is the drain's scheduling decision, not this surface's.
    fn drainable_windows(&self) -> BoxFuture<'_, Result<Vec<DrainableWindow>, SealError>>;

    /// Seals one window (§6.2): a single sorted, deduplicating `COPY` of
    /// the window's staged rows to one local Parquet part, returning the
    /// part and the manifest bookkeeping. Re-sealing an undrained window is
    /// legal and overwrites the scratch file (drain retries recompute, R-2).
    fn seal_window(&self, request: SealRequest) -> BoxFuture<'_, Result<SealedPart, SealError>>;

    /// Coverage-guarded `DropWindow` (§6.9; TN-32): removes from staging
    /// **only** rows whose `(origin, seq)` falls inside `covered` — the
    /// committed coverage of the window's durable `LakeCommit`(s). When
    /// that covers every staged row (the common case), the whole table is
    /// dropped O(1); otherwise the uncovered residue is kept
    /// ([`DropOutcome::ResidueKept`]) — refusing to drop is always the
    /// safe direction (R-5). Idempotent by design, the drain retries.
    fn drop_window(
        &self,
        dataset: DatasetId,
        partition: PartitionId,
        window: WindowId,
        covered: Vec<OriginSeqRange>,
    ) -> BoxFuture<'_, Result<DropOutcome, SealError>>;
}

// ---------------------------------------------------------------------------
// WatermarkBookkeeping (v0.1)
// ---------------------------------------------------------------------------

/// A rejected bookkeeping mutation, as the drain needs to see it. The rich
/// diagnosis lives with the ledger crate; the port distinguishes exactly
/// what the choreography acts on: the dense-next fence versus a malformed
/// manifest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerRejection {
    /// The window is not the partition's dense-next window (§6.8). A `got`
    /// **below** `expected` means the commit already stands — the drain's
    /// retry path detects "already stands" through this variant (or a
    /// read-back), never by blindly re-recording.
    #[error("window {got} is not the dense-next window of {partition} (expected {expected})")]
    WindowNotNext {
        /// The partition whose dense sequence was violated.
        partition: PartitionId,
        /// The dense-next window id the bookkeeping expects.
        expected: WindowId,
        /// The window id that was offered.
        got: WindowId,
    },
    /// The manifest is malformed or inconsistent with committed state — a
    /// caller bug, described; the bookkeeping is unchanged (R-3).
    #[error("manifest rejected: {0}")]
    Rejected(String),
}

/// The drain's window into watermark bookkeeping (ADR-0010, §6.4): the lake
/// **stores** the watermark, the ledger crate **computes** it, and the drain
/// carries the computed rows on `commit_files` — this port is the
/// computation seam. Defined here because drain and watermark are both
/// protocol crates (ADR-0008); `duckspout-watermark` implements it and the
/// daemon wires the two together.
///
/// Methods take `&self`: implementations synchronize internally, and
/// recording is process-local bookkeeping — durability is the committer's
/// transaction, never this port's I/O.
pub trait WatermarkBookkeeping: Send + Sync {
    /// The dense-next window id the bookkeeping will accept for the
    /// partition — `WindowId(0)` when nothing is recorded. The
    /// choreography's pre-commit fence check reads this.
    fn next_window(&self, partition: &PartitionId) -> WindowId;

    /// The partition's current `complete_through`, Unix milliseconds,
    /// inclusive — `None` while the partition has no provable watermark.
    fn complete_through_ms(&self, partition: &PartitionId) -> Option<i64>;

    /// The watermark row `commit_files` must carry for `manifest` (§6.4:
    /// `WatermarkAdvance` rides `LakeCommit` atomically), computed without
    /// recording anything. `Ok(None)` means the partition still has no
    /// provable watermark; the commit then carries no row.
    ///
    /// # Errors
    ///
    /// [`LedgerRejection`] — the manifest is not recordable; nothing may be
    /// committed under it.
    fn advance_for(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, LedgerRejection>;

    /// Records a **durably committed** manifest (call only after
    /// `CommitOutcome::Committed` or a read-back proving the commit stands)
    /// and returns the partition's watermark row after the advance rule
    /// re-runs.
    ///
    /// # Errors
    ///
    /// [`LedgerRejection`] — the bookkeeping is unchanged.
    fn record_commit(
        &self,
        manifest: &WindowManifest,
    ) -> Result<Option<WatermarkRow>, LedgerRejection>;

    /// The recorded per-origin coverage of one committed window — what the
    /// coverage-guarded `DropWindow` may remove from staging when the
    /// manifest itself is no longer at hand (the §6.9 crash-recovery
    /// completion; TN-32). `None` when the window is not recorded.
    fn recorded_coverage(
        &self,
        partition: &PartitionId,
        window: WindowId,
    ) -> Option<Vec<OriginSeqRange>>;
}

// ---------------------------------------------------------------------------
// LakeCommitter (v0.1)
// ---------------------------------------------------------------------------

/// The three-valued lake-commit outcome (§6.5). Exhaustive: every commit
/// resolves to exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommitOutcome {
    /// Files registered, watermark advanced; the owner proceeds to
    /// `Demote`/`DropWindow` (§6.9).
    Committed,
    /// The backend definitively rejected the commit; nothing changed. The
    /// drain retries or yields to the guard (§6.6).
    Aborted,
    /// The connection dropped mid-COMMIT and the outcome is unknown.
    /// Resolved by exactly one read-back before any retry — blind retry is
    /// forbidden (§6.5).
    Indeterminate,
}

/// A backend-invariant lake failure — misconfiguration or an unimplemented
/// backend. Transient commit-time failures are **not** errors: they fold
/// into [`CommitOutcome`] (`Aborted` / `Indeterminate`) per §6.5.
#[derive(Debug, thiserror::Error)]
pub enum LakeError {
    /// The backend is a bootstrap stub; the real implementation lands at
    /// v0.1.
    #[error("lake operation not implemented: {0}")]
    NotImplemented(&'static str),
    /// The backend cannot operate as configured, described.
    #[error("lake backend misconfigured: {0}")]
    Misconfigured(String),
    /// A non-commit operation failed in the backend, described.
    #[error("lake backend: {0}")]
    Backend(String),
}

/// A monotone, lossless schema change (§2's type lattice; §6.4). Idempotent;
/// concurrent applications converge (commutative-join semantics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaEvolution {
    /// The dataset whose schema evolves.
    pub dataset: DatasetId,
    /// Columns added or widened; each entry is a join with the existing
    /// schema under §2's lattice.
    pub columns: Vec<ColumnSpec>,
}

/// One column of a schema evolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    /// The column name.
    pub name: String,
    /// The column's logical type name in §2's lattice.
    pub logical_type: String,
}

/// What a querying `DuckDB` needs to attach this lake (§6.4); feeds the
/// catalog extension's bind (§7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachInfo {
    /// The catalog URI.
    pub catalog_uri: String,
    /// The shape of credentials the attach requires (e.g. a secret name —
    /// never the credential itself).
    pub credentials_shape: String,
    /// Backend dialect quirks the attaching reader must know.
    pub dialect: String,
}

/// The lake-agnosticism boundary, v0.1 (§6.4): everything above it is
/// lake-neutral, everything below it is one backend crate. Six operations —
/// nothing on the critical path may need more (Keep Rule, §11).
///
/// Its home crate `duckspout-lake-contract` re-exports it and owns the
/// conformance suite; `duckspout-drain` is the sole protocol-side consumer.
pub trait LakeCommitter: Send + Sync {
    /// Atomically registers a set of sealed parts **and** advances the named
    /// partition watermarks in the same commit (§6.4). The only routine
    /// write: `WatermarkAdvance` rides this atomically, which is what makes
    /// `WatermarkHonesty` provable. Where DDL and append cannot combine,
    /// any required evolve commits strictly before add — add-before-evolve
    /// is forbidden.
    ///
    /// # Errors
    ///
    /// [`LakeError`] only for backend-invariant failures; transient failures
    /// resolve into the returned [`CommitOutcome`].
    fn commit_files(
        &self,
        manifest: WindowManifest,
        watermarks: Vec<WatermarkRow>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>>;

    /// Atomically swaps named objects for named replacements. **Emergency
    /// only** (operator-invoked repair, declared-loss annulment §9): never
    /// scheduled, never on the drain path — its existence is not a license
    /// to compact (§6.4).
    ///
    /// # Errors
    ///
    /// As for [`LakeCommitter::commit_files`].
    fn replace_files(
        &self,
        remove: Vec<PartName>,
        add: Vec<PartName>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>>;

    /// Applies a monotone, lossless schema change (§2's lattice). Idempotent;
    /// concurrent applications converge (§6.4).
    ///
    /// # Errors
    ///
    /// [`LakeError`] for backend-invariant failures.
    fn evolve_schema(&self, change: SchemaEvolution) -> BoxFuture<'_, Result<(), LakeError>>;

    /// Whole-file DELETE of named parts (§3 `Expire`). The
    /// changelog-coverage guard (`SnapshotCovered`, §3) is enforced above
    /// the port, before this is ever called (§6.4).
    ///
    /// # Errors
    ///
    /// [`LakeError`] for backend-invariant failures.
    fn expire(&self, parts: Vec<PartName>) -> BoxFuture<'_, Result<(), LakeError>>;

    /// Returns the last committed watermark state for the named partitions —
    /// the read-back half of Indeterminate resolution (§6.5) and of
    /// boot-time recovery (§5).
    ///
    /// # Errors
    ///
    /// [`LakeError`] for backend-invariant failures.
    fn read_watermarks(
        &self,
        partitions: Vec<PartitionId>,
    ) -> BoxFuture<'_, Result<Vec<WatermarkRow>, LakeError>>;

    /// Returns what a querying `DuckDB` needs to attach this lake (§6.4).
    ///
    /// # Errors
    ///
    /// [`LakeError`] for backend-invariant failures.
    fn attach_info(&self) -> BoxFuture<'_, Result<AttachInfo, LakeError>>;
}
