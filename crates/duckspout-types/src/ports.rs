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
use crate::ids::{DatasetId, PartName, PartitionId, TenantId};
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
///
/// Defined here (ADR-0008: it crosses the accept↔staging boundary inside
/// [`StageCommitter`]'s signature); `duckspout-staging` re-exports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedCoverage {
    /// The partition the rows landed in.
    pub partition: PartitionId,
    /// The dense, contiguous seq range this commit covers.
    pub range: OriginSeqRange,
}

/// A staging failure as the accept path sees it (§4.3): every variant is a
/// **not-acked** outcome — the batch may be retried, and a retry can land on
/// a healthy node, so adapters map these onto the retryable wire vocabulary
/// (`StorageFailure`, §4.1.2).
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
}

/// The staging port the accept path commits through (§4.3, ADR-0008): stage
/// one decoded batch in one durable `StageCommit` transaction and return the
/// per-partition coverage evidence `ClientAck` is built from.
///
/// Contract, in §4.3's vocabulary:
///
/// - `Ok` means `StageCommit` returned — the whole batch is fsynced-durable
///   locally, atomically. The caller may ack once the replication floor is
///   met (v0.1 single-node: RF = 1, so `Ok` **is** ack-worthy; the RF−1
///   `Receipt` wait of §4.3 arrives with replication at v0.2 and slots
///   between this call and the ack).
/// - `Err` means nothing was staged and nothing may be acked. There is no
///   partial outcome — a batch stages in its entirety or not at all (§4.1.2).
/// - The dedup-window entry of §4.4.1 rides the same transaction; its
///   client-visible semantics (replay, in-flight throttle) land with
///   issue #33 on this same seam.
///
/// Home crate: `duckspout-staging` (the WAL=hot engine implements this; the
/// engine is blocking, so implementations may resolve the returned future
/// synchronously — callers embed the port off their reactor, ADR-0003).
pub trait StageCommitter: Send + Sync {
    /// Stages `batch` in one durable transaction (§4.3 `StageCommit`).
    ///
    /// Returns the committed per-origin seq coverage, one entry per
    /// partition touched, sorted by partition.
    ///
    /// # Errors
    ///
    /// [`StageError`] — the batch is not staged and must not be acked.
    fn stage_commit(
        &self,
        batch: DecodedBatch,
    ) -> BoxFuture<'_, Result<Vec<StagedCoverage>, StageError>>;
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
