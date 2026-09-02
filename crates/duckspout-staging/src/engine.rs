//! The WAL=hot staging engine over embedded `DuckDB` (§4.2, ADR-0003).
//!
//! # Connection topology (#114)
//!
//! One process owns the hot database. Inside it:
//!
//! - **One write connection**, serialized by a mutex. Every `StageCommit`
//!   ([`StagingEngine::begin`] → [`StageTxn::commit`]) and every
//!   [`StagingEngine::checkpoint`] runs on it.
//! - **Any number of read connections** ([`StagingEngine::reader`]), cloned
//!   from the write connection onto the same in-process database. Reads run
//!   under `DuckDB`'s MVCC snapshots and never take the write mutex — a scan
//!   (the future Flight serve path, the drain's seal `COPY`) cannot sit
//!   behind a commit, and a commit cannot sit behind a scan.
//!
//! # Checkpoints are kept off the ack path (#109)
//!
//! `DuckDB` auto-checkpoints **inside commit** once the WAL crosses
//! `checkpoint_threshold` (16 MiB by default) — the spike measured 219–620 ms
//! commit outliers exactly there, against a §4.3 ack budget of 25 ms. The
//! engine therefore defers automatic checkpointing at open
//! ([`CHECKPOINT_THRESHOLD_DEFERRED`]) and exposes the pause as an explicit
//! [`StagingEngine::checkpoint`] for the drain to invoke in its own window,
//! after `DropWindow`, where it costs drain latency instead of ack latency.
//! Between checkpoints the WAL simply grows; its size is bounded by drain
//! cadence (one micro-window of data per checkpoint interval), and WAL
//! replay at open covers a crash at any point in between.
//!
//! # The storage-port boundary (ADR-0003), stated honestly
//!
//! Inside this crate, `DuckDB` **is** the storage: its documented
//! fsync-on-commit WAL is the content-durability primitive, trusted as
//! published (R-trust-official-docs). The [`Storage`] port covers what that
//! documentation does *not* pin down — **directory-entry durability**. The
//! bundled engine fsyncs file contents but never the containing directory,
//! and a checkpoint deletes and lazily recreates the WAL file, so a freshly
//! created `hot.db`/`hot.db.wal` name can vanish in a crash even though its
//! bytes were fsynced. The engine closes that gap off the ack path: at
//! [`StagingEngine::open`] and after every [`StagingEngine::checkpoint`] it
//! forces the WAL file into existence (a metadata epoch bump) and then
//! `fsync_dir`s the hot directory through the port. Commits in between
//! append to an already-durable name and need no per-commit port call.
//!
//! # Blocking discipline
//!
//! Every method here blocks (`DuckDB` is a blocking engine; the commit is an
//! fsync). Callers embed the engine off their reactor (`spawn_blocking` or a
//! dedicated thread) — ADR-0003's "group commit off the reactor" seam. Port
//! futures are driven to completion on the calling thread.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use duckspout_types::{
    BoxFuture, Clock, DatasetId, NodeId, OriginSeqRange, PartitionId, StagedCoverage, Storage,
    StoragePath, TenantId, WindowId,
};

use crate::naming::window_table_name;

/// The hot database file name inside [`StagingConfig::hot_dir`].
pub const HOT_DB_FILE: &str = "hot.db";

/// The `checkpoint_threshold` the engine sets at open: high enough that
/// `DuckDB` never auto-checkpoints inside a commit (#109). A constant, not a
/// knob (R-12): the WAL is bounded by drain cadence — every drained window
/// invokes [`StagingEngine::checkpoint`] — not by this threshold.
pub const CHECKPOINT_THRESHOLD_DEFERRED: &str = "1TB";

const SYS_COL_ORIGIN: &str = "origin";
const SYS_COL_SEQ: &str = "seq";

/// A typed staging failure. `StageCommit` fails a batch without acking it
/// (§4.3); every variant is a not-acked outcome.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StagingError {
    /// The embedded engine rejected an operation (SQL, appender, or commit).
    #[error("hot store engine: {0}")]
    Engine(#[from] duckdb::Error),
    /// Filesystem-level failure preparing the hot directory.
    #[error("hot directory: {0}")]
    Io(#[from] std::io::Error),
    /// The storage port refused a directory fsync — durability of the hot
    /// store's file *names* is unknown (ADR-0003).
    #[error(transparent)]
    Storage(#[from] duckspout_types::StorageError),
    /// An arrow batch could not be assembled with the system columns.
    #[error("arrow batch: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A payload column's arrow type is outside the supported staging subset.
    #[error("column {column:?} has arrow type {datatype}, outside the supported staging subset")]
    UnsupportedColumnType {
        /// The offending column name.
        column: String,
        /// The arrow `DataType`, rendered.
        datatype: String,
    },
    /// A payload column collides with a system column (§2.3's `origin`/`seq`).
    #[error(
        "column {column:?} collides with a reserved system column ({SYS_COL_ORIGIN}, {SYS_COL_SEQ})"
    )]
    ReservedColumn {
        /// The offending column name.
        column: String,
    },
    /// The dense per-(partition, origin) sequence would overflow `u64`.
    #[error("per-(partition, origin) sequence exhausted for partition {partition}")]
    SeqExhausted {
        /// The partition whose sequence ran out.
        partition: PartitionId,
    },
    /// A thread panicked while holding the write connection; the engine's
    /// in-memory bookkeeping is suspect and the process should reopen it.
    #[error("staging writer poisoned: a thread panicked mid-transaction; reopen the engine")]
    WriterPoisoned,
    /// Stored engine state failed to decode — fails closed, never guessed
    /// around (§11).
    #[error("corrupt engine state: {0}")]
    Corrupt(String),
    /// A guarded scan crossed its per-query byte budget (§7.8) — a typed
    /// abort, **never truncation**: no partial result is returned.
    #[error(
        "scan aborted: {scanned_bytes} bytes crossed the per-query budget of \
         {budget_bytes} (§7.8 — narrow the range, raise \
         query.max_hot_bytes_per_query, or read the lake)"
    )]
    ScanBudgetExceeded {
        /// Bytes accumulated when the budget check tripped.
        scanned_bytes: u64,
        /// The (fill-scaled) budget that was in force.
        budget_bytes: u64,
    },
    /// A guarded scan crossed its deadline (§7.8) — a typed abort, never
    /// truncation.
    #[error(
        "scan aborted: deadline of {deadline_ms} ms exceeded \
         (§7.8 — narrow the range, raise query.hot_scan_deadline, or read the lake)"
    )]
    ScanDeadlineExceeded {
        /// The deadline that was in force, in milliseconds.
        deadline_ms: u64,
    },
}

/// How to open a [`StagingEngine`].
#[derive(Debug, Clone)]
pub struct StagingConfig {
    /// The hot volume directory; created if absent. Holds [`HOT_DB_FILE`]
    /// and its WAL. Local `NVMe` is the assumed substrate (§4.2.1).
    pub hot_dir: PathBuf,
    /// This node's origin identity — `(node_id, incarnation)` rendered per
    /// §5 — stamped on every staged row and every applied-watermark row
    /// (§4.2.3–§4.2.4).
    pub origin: NodeId,
}

/// One dedup-window entry, as `DedupCheck` reads it (§4.4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupEntry {
    /// Whether the entry's ack evidence is complete (§4.4.1: an unacked
    /// entry guards durable data still short of RF; it resolves through the
    /// `AtRF` branch, never through re-staging).
    pub acked: bool,
    /// The stored outcome a duplicate replays — the serialized coverage
    /// evidence, exactly as `ClientAck` computed it.
    pub outcome_json: String,
}

/// One live micro-window table, from the engine's registry (§2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowRef {
    /// The dataset the window belongs to.
    pub dataset: DatasetId,
    /// The partition the window belongs to.
    pub partition: PartitionId,
    /// The dense per-partition window sequence number.
    pub window: WindowId,
    /// The `DuckDB` table name (also a pure function of the identity —
    /// [`window_table_name`]).
    pub table_name: String,
}

type WindowKey = (DatasetId, PartitionId, u64);

struct WriterInner {
    conn: Connection,
    /// Highest committed seq per partition for this node's origin — the
    /// in-memory mirror of the `duckspout_applied` rows (§4.2.4).
    applied: HashMap<PartitionId, u64>,
    /// Live micro-window tables — the in-memory mirror of
    /// `duckspout_windows`.
    windows: HashMap<WindowKey, WindowMeta>,
    /// Highest window id ever created per (dataset, partition) — the
    /// in-memory mirror of `duckspout_window_hw`. Survives `DropWindow` and
    /// restart, which is what keeps allocated window ids dense and
    /// never-reused (§2.3: contiguity of the per-partition window sequence
    /// must stay decidable after windows drain away).
    window_hw: HashMap<(DatasetId, PartitionId), u64>,
    /// Total staged bytes across live windows — the running mirror of
    /// `sum(duckspout_windows.staged_bytes)`, the ladder's measure
    /// numerator (§4.5). Shared with the engine as an atomic so the
    /// measure is readable **without the write mutex** — the serving
    /// path's guard computation must never sit behind an open
    /// `StageCommit` transaction (#114); writers update it while holding
    /// the lock.
    staged_bytes: Arc<AtomicU64>,
}

/// One live window's registry mirror: its table name plus its accounted
/// staged bytes.
#[derive(Debug, Clone)]
struct WindowMeta {
    table: String,
    bytes: u64,
}

/// The staging engine: WAL=hot over one persistent `DuckDB` database. See
/// the [module docs](self) for the connection topology, the checkpoint
/// scheme, and the storage-port boundary.
pub struct StagingEngine<S: Storage> {
    writer: Mutex<WriterInner>,
    /// The lock-free side of the staged-bytes measure (field docs on
    /// [`WriterInner`]).
    staged_bytes: Arc<AtomicU64>,
    storage: S,
    origin: NodeId,
    hot_dir: PathBuf,
}

impl<S: Storage> StagingEngine<S> {
    /// Opens (or creates) the persistent hot database, replays its WAL if a
    /// crash left one, defers automatic checkpointing (#109), bootstraps the
    /// engine's metadata tables, and makes the database file names durable
    /// through the storage port (module docs). Blocking; call off the
    /// reactor.
    ///
    /// `storage` must be rooted at `config.hot_dir` — the engine addresses
    /// the port with paths relative to that root.
    ///
    /// # Errors
    ///
    /// [`StagingError::Io`] if the hot directory cannot be created,
    /// [`StagingError::Engine`] if the database cannot be opened or
    /// bootstrapped, [`StagingError::Storage`] if the directory fsync is
    /// refused (the open is then not durable and the engine is not
    /// returned).
    pub fn open(config: StagingConfig, storage: S) -> Result<Self, StagingError> {
        std::fs::create_dir_all(&config.hot_dir)?;
        let conn = Connection::open(config.hot_dir.join(HOT_DB_FILE))?;
        conn.execute_batch(&format!(
            "SET checkpoint_threshold = '{CHECKPOINT_THRESHOLD_DEFERRED}';"
        ))?;
        // One transaction: metadata tables + a WAL-epoch bump. The bump is a
        // real write, so the WAL file exists on disk when the fsync below
        // runs — even on a reopen after a clean (checkpointed) shutdown.
        conn.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS duckspout_meta (
                 key VARCHAR PRIMARY KEY,
                 n   BIGINT NOT NULL);
             CREATE TABLE IF NOT EXISTS duckspout_applied (
                 partition   VARCHAR NOT NULL,
                 origin      VARCHAR NOT NULL,
                 applied_seq UBIGINT NOT NULL,
                 PRIMARY KEY (partition, origin));
             CREATE TABLE IF NOT EXISTS duckspout_windows (
                 dataset      VARCHAR NOT NULL,
                 partition    VARCHAR NOT NULL,
                 window_id    UBIGINT NOT NULL,
                 table_name   VARCHAR NOT NULL,
                 staged_bytes UBIGINT NOT NULL DEFAULT 0,
                 PRIMARY KEY (dataset, partition, window_id));
             CREATE TABLE IF NOT EXISTS duckspout_dedup (
                 tenant          VARCHAR NOT NULL,
                 dedup_key       VARCHAR NOT NULL,
                 acked           BOOLEAN NOT NULL,
                 outcome         VARCHAR NOT NULL,
                 created_wall_ms BIGINT  NOT NULL,
                 PRIMARY KEY (tenant, dedup_key));
             CREATE TABLE IF NOT EXISTS duckspout_window_hw (
                 dataset   VARCHAR NOT NULL,
                 partition VARCHAR NOT NULL,
                 hw        UBIGINT NOT NULL,
                 PRIMARY KEY (dataset, partition));
             INSERT INTO duckspout_meta (key, n) VALUES ('wal_epoch', 1)
                 ON CONFLICT (key) DO UPDATE SET n = n + 1;
             COMMIT;",
        )?;

        let applied = load_applied(&conn, &config.origin)?;
        let windows = load_windows(&conn)?;
        let window_hw = load_window_hw(&conn)?;
        let staged_bytes = Arc::new(AtomicU64::new(
            windows.values().map(|meta| meta.bytes).sum(),
        ));

        // Name durability for hot.db and hot.db.wal (module docs; ADR-0003).
        block_on(storage.fsync_dir(StoragePath::new("")))?;

        Ok(Self {
            writer: Mutex::new(WriterInner {
                conn,
                applied,
                windows,
                window_hw,
                staged_bytes: Arc::clone(&staged_bytes),
            }),
            staged_bytes,
            storage,
            origin: config.origin,
            hot_dir: config.hot_dir,
        })
    }

    /// The origin stamped on every row this engine stages.
    #[must_use]
    pub fn origin(&self) -> &NodeId {
        &self.origin
    }

    /// The storage port the engine's directory-fsync discipline goes
    /// through.
    #[must_use]
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Begins one `StageCommit` transaction on the write connection. The
    /// returned handle holds the write lock until committed, rolled back, or
    /// dropped (drop rolls back).
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`] if a previous holder panicked;
    /// [`StagingError::Engine`] if `BEGIN` fails.
    pub fn begin(&self) -> Result<StageTxn<'_>, StagingError> {
        let writer = self.lock_writer()?;
        writer.conn.execute_batch("BEGIN TRANSACTION")?;
        Ok(StageTxn {
            writer,
            origin: self.origin.clone(),
            pending_applied: BTreeMap::new(),
            pending_windows: Vec::new(),
            pending_bytes: BTreeMap::new(),
            finished: false,
        })
    }

    /// Opens a dedicated read connection (#114): its queries run under MVCC
    /// snapshots on `DuckDB`'s own concurrency control and never take the
    /// write mutex. Create readers outside open write transactions (this
    /// call briefly takes the write lock to clone the connection).
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`] or [`StagingError::Engine`] if the
    /// clone fails.
    pub fn reader(&self) -> Result<StagingReader, StagingError> {
        let writer = self.lock_writer()?;
        let conn = writer.conn.try_clone()?;
        Ok(StagingReader { conn })
    }

    /// Checkpoints the hot database — the WAL pause the ack path never pays
    /// (#109). The drain invokes this in its own window, after `DropWindow`.
    /// Serializes with commits (write lock held throughout, including the
    /// directory fsync that re-secures the recreated WAL's name — module
    /// docs).
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`], [`StagingError::Engine`], or
    /// [`StagingError::Storage`] if the post-checkpoint directory fsync is
    /// refused (durability of the new WAL file's name is then unknown).
    pub fn checkpoint(&self) -> Result<(), StagingError> {
        let writer = self.lock_writer()?;
        writer.conn.execute_batch("CHECKPOINT")?;
        // The checkpoint deleted the WAL file; recreate it with an epoch
        // bump and make the new name durable before releasing the writer,
        // so no commit can land in a WAL whose name could still vanish.
        writer.conn.execute_batch(
            "BEGIN;
             INSERT INTO duckspout_meta (key, n) VALUES ('wal_epoch', 1)
                 ON CONFLICT (key) DO UPDATE SET n = n + 1;
             COMMIT;",
        )?;
        block_on(self.storage.fsync_dir(StoragePath::new("")))?;
        drop(writer);
        Ok(())
    }

    /// Drops one micro-window table and its registry row in one transaction
    /// — `DropWindow`, the O(1) cleanup after a durable `LakeCommit` (§2.3).
    /// Returns `false` if the window is not registered (already dropped —
    /// idempotent by design, the drain retries).
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`] or [`StagingError::Engine`].
    pub fn drop_window(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
    ) -> Result<bool, StagingError> {
        let mut writer = self.lock_writer()?;
        let key = (dataset.clone(), partition.clone(), window.0);
        let Some(meta) = writer.windows.get(&key).cloned() else {
            return Ok(false);
        };
        if let Err(error) = drop_window_txn(&writer.conn, &meta.table, dataset, partition, window) {
            let _ = writer.conn.execute_batch("ROLLBACK");
            return Err(error.into());
        }
        writer.windows.remove(&key);
        // The dropped window's bytes leave the ladder measure with it
        // (§4.5: staged_bytes sums over LIVE staging tables).
        writer.staged_bytes.fetch_sub(meta.bytes, Ordering::SeqCst);
        Ok(true)
    }

    /// Deletes the rows of `table` matching `predicate` in one durable
    /// write-connection statement — the covered-rows half of the TN-32
    /// coverage-guarded `DropWindow` (the seal surface builds the
    /// predicate; both arguments are engine-generated, never caller
    /// strings).
    pub(crate) fn delete_covered_rows(
        &self,
        table: &str,
        predicate: &str,
    ) -> Result<(), StagingError> {
        let writer = self.lock_writer()?;
        writer
            .conn
            .execute_batch(&format!("DELETE FROM {table} WHERE {predicate}"))?;
        Ok(())
    }

    /// Total accounted staged bytes across live windows — the overload
    /// ladder's measure numerator, `M = staged_bytes / hot.max_bytes`
    /// (§4.5). The accounting unit is the in-memory Arrow size of each
    /// appended payload batch, summed per window in the same transaction as
    /// the rows (exact over what was appended; a proxy for on-disk bytes,
    /// which the engine's own compression makes unknowable per window).
    ///
    /// Lock-free by design: read from an atomic the write path maintains,
    /// so a guard computation on the serve path never waits behind an open
    /// `StageCommit` transaction (#114).
    pub fn staged_bytes(&self) -> u64 {
        self.staged_bytes.load(Ordering::SeqCst)
    }

    /// The live micro-window tables, sorted by (dataset, partition, window).
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`].
    pub fn list_windows(&self) -> Result<Vec<WindowRef>, StagingError> {
        let writer = self.lock_writer()?;
        let mut windows: Vec<WindowRef> = writer
            .windows
            .iter()
            .map(|((dataset, partition, window), meta)| WindowRef {
                dataset: dataset.clone(),
                partition: partition.clone(),
                window: WindowId(*window),
                table_name: meta.table.clone(),
            })
            .collect();
        windows.sort_by(|a, b| {
            (&a.dataset, &a.partition, a.window.0).cmp(&(&b.dataset, &b.partition, b.window.0))
        });
        Ok(windows)
    }

    /// The hot volume directory this engine was opened on.
    #[must_use]
    pub fn hot_dir(&self) -> &std::path::Path {
        &self.hot_dir
    }

    /// The registered table name of one micro-window, or `None` if the
    /// window is not live on this node.
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`].
    pub fn window_table(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
    ) -> Result<Option<String>, StagingError> {
        let key = (dataset.clone(), partition.clone(), window.0);
        Ok(self
            .lock_writer()?
            .windows
            .get(&key)
            .map(|meta| meta.table.clone()))
    }

    /// The highest committed seq for `partition` under this engine's origin
    /// (§4.2.4), or `None` if nothing was ever staged there.
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`].
    pub fn applied_seq(&self, partition: &PartitionId) -> Result<Option<u64>, StagingError> {
        Ok(self.lock_writer()?.applied.get(partition).copied())
    }

    /// The highest window id ever created for `(dataset, partition)`, or
    /// `None` if no window was ever created there. Unlike the live registry
    /// ([`Self::list_windows`]) this survives `DropWindow` and restart, so
    /// window allocators ([`crate::EngineStager`]) can keep
    /// the per-partition window sequence dense without ever reusing a
    /// drained window's id (§2.3).
    ///
    /// # Errors
    ///
    /// [`StagingError::WriterPoisoned`].
    pub fn highest_window_id(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
    ) -> Result<Option<WindowId>, StagingError> {
        Ok(self
            .lock_writer()?
            .window_hw
            .get(&(dataset.clone(), partition.clone()))
            .copied()
            .map(WindowId))
    }

    /// Size of the engine's WAL file (`hot.db.wal`), if present.
    #[must_use]
    pub fn wal_size(&self) -> Option<u64> {
        let mut wal = self.hot_dir.join(HOT_DB_FILE).into_os_string();
        wal.push(".wal");
        std::fs::metadata(PathBuf::from(wal)).ok().map(|m| m.len())
    }

    /// Size of the hot database file, if present.
    #[must_use]
    pub fn db_size(&self) -> Option<u64> {
        std::fs::metadata(self.hot_dir.join(HOT_DB_FILE))
            .ok()
            .map(|m| m.len())
    }

    fn lock_writer(&self) -> Result<MutexGuard<'_, WriterInner>, StagingError> {
        self.writer.lock().map_err(|_| StagingError::WriterPoisoned)
    }
}

/// One open `StageCommit` transaction (§4.3): any number of [`Self::append`]
/// calls, then one [`Self::commit`] — fsync-on-commit is what makes the
/// whole batch durable atomically. Holds the write lock; dropping without
/// committing rolls back and releases the sequence numbers it had assigned.
pub struct StageTxn<'engine> {
    writer: MutexGuard<'engine, WriterInner>,
    origin: NodeId,
    /// (first, last) assigned seq per partition, merged into the engine's
    /// applied map only on commit.
    pending_applied: BTreeMap<PartitionId, (u64, u64)>,
    /// Windows created inside this transaction (their `CREATE TABLE` rolls
    /// back with it — `DuckDB` DDL is transactional).
    pending_windows: Vec<(WindowKey, String)>,
    /// Bytes appended per window inside this transaction, merged into the
    /// engine's staged-bytes accounting only on commit (§4.5).
    pending_bytes: BTreeMap<WindowKey, u64>,
    finished: bool,
}

impl StageTxn<'_> {
    /// Appends one decoded record batch into `(dataset, partition, window)`,
    /// creating the micro-window table on first write (§2.3). The engine
    /// stamps the two system columns itself: `origin` (this node) and `seq`
    /// (the dense per-(partition, origin) sequence, §4.2.3) — payload
    /// columns must not use those names.
    ///
    /// The payload schema is fixed per table at creation; later appends must
    /// match it (the fixed spec-derived OTLP mapping, §4.8 — schema
    /// evolution in hot tables is the type-lattice work, not this seam).
    /// Supported payload types: booleans, integers (8–64 bit, signed and
    /// unsigned), `Float32`/`Float64`, `Utf8`, `Binary`, and
    /// timezone-less microsecond timestamps.
    ///
    /// # Errors
    ///
    /// [`StagingError::ReservedColumn`], [`StagingError::UnsupportedColumnType`],
    /// [`StagingError::SeqExhausted`], [`StagingError::Arrow`], or
    /// [`StagingError::Engine`] (including a payload schema that does not
    /// match the window table). On error the transaction is still open; the
    /// caller commits, rolls back, or drops it.
    pub fn append(
        &mut self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
        batch: &RecordBatch,
    ) -> Result<(), StagingError> {
        let table = self.ensure_window(dataset, partition, window, &batch.schema())?;

        let rows = batch.num_rows() as u64;
        if rows == 0 {
            return Ok(());
        }

        let base = self
            .pending_applied
            .get(partition)
            .map(|(_, last)| *last)
            .or_else(|| self.writer.applied.get(partition).copied())
            .unwrap_or(0);
        let first = base
            .checked_add(1)
            .ok_or_else(|| StagingError::SeqExhausted {
                partition: partition.clone(),
            })?;
        let last = base
            .checked_add(rows)
            .ok_or_else(|| StagingError::SeqExhausted {
                partition: partition.clone(),
            })?;

        let augmented = augment_batch(batch, &self.origin, first, last)?;
        let mut appender = self.writer.conn.appender(&table)?;
        appender.append_record_batch(augmented)?;
        appender.flush()?;
        drop(appender);

        // Staged-bytes accounting (§4.5): the payload's in-memory Arrow
        // size, recorded on the window's registry row in this same
        // transaction — the measure and the data commit or vanish together.
        let bytes = batch.get_array_memory_size() as u64;
        self.writer.conn.execute(
            "UPDATE duckspout_windows SET staged_bytes = staged_bytes + ?
             WHERE dataset = ? AND partition = ? AND window_id = ?",
            duckdb::params![bytes, dataset.as_str(), partition.as_str(), window.0],
        )?;
        let key: WindowKey = (dataset.clone(), partition.clone(), window.0);
        *self.pending_bytes.entry(key).or_insert(0) += bytes;

        self.pending_applied
            .entry(partition.clone())
            .and_modify(|(_, pending_last)| *pending_last = last)
            .or_insert((first, last));
        Ok(())
    }

    /// Commits the transaction: advances the applied-watermark rows
    /// (§4.2.4) in the same `DuckDB` transaction as the staged rows, then
    /// `COMMIT` — when this returns, the batch is fsynced (`StageCommit`
    /// done; ack-worthy pending replication, §4.3). Returns the per-origin
    /// seq coverage per partition, sorted by partition.
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`] — the transaction is rolled back and nothing
    /// is acked.
    pub fn commit(mut self) -> Result<Vec<StagedCoverage>, StagingError> {
        for (partition, (_, last)) in &self.pending_applied {
            self.writer.conn.execute(
                "INSERT INTO duckspout_applied (partition, origin, applied_seq)
                 VALUES (?, ?, ?)
                 ON CONFLICT (partition, origin) DO UPDATE
                     SET applied_seq = excluded.applied_seq",
                duckdb::params![partition.as_str(), self.origin.as_str(), *last],
            )?;
        }
        self.writer.conn.execute_batch("COMMIT")?;
        self.finished = true;

        for (key, table) in self.pending_windows.drain(..) {
            let (dataset, partition, window) = key.clone();
            self.writer
                .window_hw
                .entry((dataset, partition))
                .and_modify(|hw| *hw = (*hw).max(window))
                .or_insert(window);
            self.writer
                .windows
                .insert(key, WindowMeta { table, bytes: 0 });
        }
        let pending_bytes = std::mem::take(&mut self.pending_bytes);
        for (key, bytes) in pending_bytes {
            if let Some(meta) = self.writer.windows.get_mut(&key) {
                meta.bytes += bytes;
            }
            self.writer.staged_bytes.fetch_add(bytes, Ordering::SeqCst);
        }
        let coverage = self.pending_coverage();
        for (partition, (_, last)) in &self.pending_applied {
            self.writer.applied.insert(partition.clone(), *last);
        }
        Ok(coverage)
    }

    /// The coverage this transaction will return from [`Self::commit`] —
    /// knowable before `COMMIT` because seqs are assigned at append (§4.3).
    /// The dedup entry's stored outcome is serialized from this inside the
    /// same transaction (§4.4.1), so replay and original are one value by
    /// construction.
    #[must_use]
    pub fn pending_coverage(&self) -> Vec<StagedCoverage> {
        self.pending_applied
            .iter()
            .map(|(partition, (first, last))| StagedCoverage {
                partition: partition.clone(),
                range: OriginSeqRange {
                    origin: self.origin.clone(),
                    first_seq: *first,
                    last_seq: *last,
                },
            })
            .collect()
    }

    /// `DedupCheck`'s window-table lookup (§4.4.1), inside this transaction
    /// — serialized with every other dedup write by the single-writer
    /// discipline, so check-then-insert cannot race. An entry created
    /// before `min_created_wall_ms` is expired and reads as absent — the
    /// TTL binds at lookup, not merely at the garbage-collecting eviction,
    /// so the documented window bound is exact.
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`].
    pub fn dedup_lookup(
        &mut self,
        tenant: &TenantId,
        dedup_key: &str,
        min_created_wall_ms: i64,
    ) -> Result<Option<DedupEntry>, StagingError> {
        let mut stmt = self.writer.conn.prepare(
            "SELECT acked, outcome FROM duckspout_dedup
             WHERE tenant = ? AND dedup_key = ? AND created_wall_ms >= ?",
        )?;
        let mut rows = stmt.query(duckdb::params![
            tenant.as_str(),
            dedup_key,
            min_created_wall_ms
        ])?;
        match rows.next()? {
            None => Ok(None),
            Some(row) => Ok(Some(DedupEntry {
                acked: row.get(0)?,
                outcome_json: row.get(1)?,
            })),
        }
    }

    /// Writes the dedup-window entry guarding this transaction's data — in
    /// the **same transaction** as the rows, so a crash cannot record one
    /// without the other (§4.4.1). `outcome_json` is the stored outcome a
    /// duplicate replays; `acked` is the ClientAck-evidence-complete flag
    /// (at RF = 1 the commit itself completes the evidence, so v0.1 writes
    /// it `true` here — the pre-RF `false` state arrives with replication).
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`].
    pub fn dedup_insert(
        &mut self,
        tenant: &TenantId,
        dedup_key: &str,
        acked: bool,
        outcome_json: &str,
        created_wall_ms: i64,
    ) -> Result<(), StagingError> {
        self.writer.conn.execute(
            // Upsert: the only way a row can exist here is TTL-expired (a
            // live one would have resolved at `dedup_lookup`, which this
            // transaction ran first) — an expired entry is replaced, never
            // a constraint violation.
            "INSERT INTO duckspout_dedup (tenant, dedup_key, acked, outcome, created_wall_ms)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT (tenant, dedup_key) DO UPDATE SET
                 acked = excluded.acked,
                 outcome = excluded.outcome,
                 created_wall_ms = excluded.created_wall_ms",
            duckdb::params![
                tenant.as_str(),
                dedup_key,
                acked,
                outcome_json,
                created_wall_ms
            ],
        )?;
        Ok(())
    }

    /// Marks an entry's ack evidence complete — `DedupCheck`'s `AtRF`
    /// resolution (§3.3, §4.4.1): a stage-then-unacked entry becomes
    /// replayable-as-acked the moment its receipts reach RF.
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`].
    pub fn dedup_mark_acked(
        &mut self,
        tenant: &TenantId,
        dedup_key: &str,
    ) -> Result<(), StagingError> {
        self.writer.conn.execute(
            "UPDATE duckspout_dedup SET acked = true WHERE tenant = ? AND dedup_key = ?",
            duckdb::params![tenant.as_str(), dedup_key],
        )?;
        Ok(())
    }

    /// Applies the §4.4.1 window bounds inside this transaction: entries
    /// older than `ttl_ms` are dropped, then the oldest entries beyond
    /// `max_entries` are dropped. Returns how many the **count cap** (not
    /// the TTL) evicted — each one shortened the effective window below the
    /// documented retry horizon, which the operator surface warns on
    /// (§4.4.1).
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`].
    pub fn dedup_evict(
        &mut self,
        wall_now_ms: i64,
        ttl_ms: i64,
        max_entries: u64,
    ) -> Result<u64, StagingError> {
        let cutoff = wall_now_ms.saturating_sub(ttl_ms);
        self.writer.conn.execute(
            "DELETE FROM duckspout_dedup WHERE created_wall_ms < ?",
            duckdb::params![cutoff],
        )?;
        let count: u64 =
            self.writer
                .conn
                .query_row("SELECT count(*) FROM duckspout_dedup", [], |row| row.get(0))?;
        if count <= max_entries {
            return Ok(0);
        }
        let excess = count - max_entries;
        let evicted = self.writer.conn.execute(
            "DELETE FROM duckspout_dedup WHERE (tenant, dedup_key) IN (
                 SELECT tenant, dedup_key FROM duckspout_dedup
                 ORDER BY created_wall_ms ASC LIMIT ?)",
            duckdb::params![excess],
        )?;
        Ok(evicted as u64)
    }

    /// Rolls the transaction back explicitly (dropping does the same,
    /// swallowing the error).
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`] if the `ROLLBACK` itself fails.
    pub fn rollback(mut self) -> Result<(), StagingError> {
        self.finished = true;
        self.writer.conn.execute_batch("ROLLBACK")?;
        Ok(())
    }

    /// Returns the (possibly just-created) table name for the window,
    /// creating the table and its registry row inside this transaction on
    /// first write.
    fn ensure_window(
        &mut self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
        schema: &SchemaRef,
    ) -> Result<String, StagingError> {
        let key: WindowKey = (dataset.clone(), partition.clone(), window.0);
        if let Some(existing) = self.writer.windows.get(&key) {
            return Ok(existing.table.clone());
        }
        if let Some((_, pending)) = self.pending_windows.iter().find(|(k, _)| *k == key) {
            return Ok(pending.clone());
        }

        let table = window_table_name(dataset, partition, window);
        let mut columns = String::new();
        for field in schema.fields() {
            let name = field.name();
            if name.eq_ignore_ascii_case(SYS_COL_ORIGIN) || name.eq_ignore_ascii_case(SYS_COL_SEQ) {
                return Err(StagingError::ReservedColumn {
                    column: name.clone(),
                });
            }
            let sql_type = staging_sql_type(field.data_type()).ok_or_else(|| {
                StagingError::UnsupportedColumnType {
                    column: name.clone(),
                    datatype: field.data_type().to_string(),
                }
            })?;
            let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
            // Infallible: writing to a String cannot fail.
            let _ = write!(columns, "{} {sql_type}{nullability}, ", quote_ident(name));
        }
        self.writer.conn.execute_batch(&format!(
            "CREATE TABLE {table} ({columns}{SYS_COL_ORIGIN} VARCHAR NOT NULL, \
             {SYS_COL_SEQ} UBIGINT NOT NULL)"
        ))?;
        self.writer.conn.execute(
            "INSERT INTO duckspout_windows (dataset, partition, window_id, table_name)
             VALUES (?, ?, ?, ?)",
            duckdb::params![dataset.as_str(), partition.as_str(), window.0, table],
        )?;
        // The never-reuse high-water rides the same transaction as the
        // CREATE TABLE, so a crash cannot record one without the other.
        self.writer.conn.execute(
            "INSERT INTO duckspout_window_hw (dataset, partition, hw)
             VALUES (?, ?, ?)
             ON CONFLICT (dataset, partition) DO UPDATE
                 SET hw = greatest(hw, excluded.hw)",
            duckdb::params![dataset.as_str(), partition.as_str(), window.0],
        )?;
        self.pending_windows.push((key, table.clone()));
        Ok(table)
    }
}

impl Drop for StageTxn<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Nothing was acked; releasing the transaction (and its assigned
            // seqs) is all a failed StageCommit leaves behind (§4.3).
            let _ = self.writer.conn.execute_batch("ROLLBACK");
        }
    }
}

/// The §7.8 per-scan guard values, computed by the serving layer per query
/// (the byte budget fill-scaled via `fill_scaled_budget`, the deadline from
/// `query.hot_scan_deadline`). The concurrency guard is not here: it gates
/// *entry* to a scan and lives where scans are admitted, not inside one.
#[derive(Debug, Clone, Copy)]
pub struct ScanGuards {
    /// Per-query byte budget over the scanned batches' Arrow memory size.
    pub max_bytes: u64,
    /// Deadline as a span of [`Clock::monotonic_nanos`] time from the
    /// scan's start.
    pub deadline_nanos: u64,
}

/// A dedicated read connection (#114). Queries run on `DuckDB`'s MVCC
/// snapshots of committed state and never touch the write mutex. For
/// reading: the serve path and the drain own their query discipline —
/// nothing here writes, and writers must not use this handle.
pub struct StagingReader {
    conn: Connection,
}

impl StagingReader {
    /// The read connection, for sibling modules (the seal surface) that
    /// compose their own read-only statements.
    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Runs `sql` and returns the engine's native Arrow output — the result
    /// schema plus record batches exactly as produced (the §7.4 serve seam:
    /// these feed arrow-flight's IPC encoder untouched).
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`] for SQL or execution failures.
    pub fn query_arrow(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), StagingError> {
        let mut stmt = self.conn.prepare(sql)?;
        let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        Ok((stmt.schema(), batches))
    }

    /// [`Self::query_arrow`] under the §7.8 per-query guards: the byte
    /// budget and the deadline are checked as each engine-produced batch
    /// arrives, and a tripped guard is a **typed abort, never truncation**
    /// — the partial result is discarded, not returned. Byte accounting is
    /// the batches' in-memory Arrow size (the same unit as the ladder's
    /// staged-bytes measure); the deadline is measured on the [`Clock`]
    /// port. This is the cooperative half of enforcement — batch-granular
    /// by construction; [`Self::interrupt_handle`] is the hard half a
    /// serving layer arms for scans stuck *inside* one engine call.
    ///
    /// # Errors
    ///
    /// [`StagingError::ScanBudgetExceeded`] /
    /// [`StagingError::ScanDeadlineExceeded`] for tripped guards;
    /// [`StagingError::Engine`] for SQL or execution failures (including a
    /// scan killed by [`Self::interrupt_handle`]).
    pub fn query_arrow_guarded(
        &self,
        sql: &str,
        clock: &dyn Clock,
        guards: &ScanGuards,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), StagingError> {
        let started = clock.monotonic_nanos();
        let mut stmt = self.conn.prepare(sql)?;
        let mut scanned_bytes: u64 = 0;
        let mut batches = Vec::new();
        for batch in stmt.query_arrow([])? {
            scanned_bytes = scanned_bytes.saturating_add(batch.get_array_memory_size() as u64);
            if scanned_bytes > guards.max_bytes {
                return Err(StagingError::ScanBudgetExceeded {
                    scanned_bytes,
                    budget_bytes: guards.max_bytes,
                });
            }
            if clock.monotonic_nanos().saturating_sub(started) > guards.deadline_nanos {
                return Err(StagingError::ScanDeadlineExceeded {
                    deadline_ms: guards.deadline_nanos / 1_000_000,
                });
            }
            batches.push(batch);
        }
        Ok((stmt.schema(), batches))
    }

    /// The engine-level interrupt handle for this read connection — the
    /// hard half of the §7.8 deadline ("enforced via scan interrupt"): a
    /// serving layer arms a watchdog that fires this when a scan is stuck
    /// inside a single engine call, where the cooperative per-batch check
    /// of [`Self::query_arrow_guarded`] cannot run. Interrupting fails the
    /// in-flight statement on this connection only; the write path and
    /// other readers are untouched.
    #[must_use]
    pub fn interrupt_handle(&self) -> Arc<duckdb::InterruptHandle> {
        self.conn.interrupt_handle()
    }

    /// Row count of one micro-window table, addressed by identity (the
    /// table name is recomputed, never interpolated from caller strings).
    ///
    /// # Errors
    ///
    /// [`StagingError::Engine`] — including a window that does not exist.
    pub fn count_window(
        &self,
        dataset: &DatasetId,
        partition: &PartitionId,
        window: WindowId,
    ) -> Result<u64, StagingError> {
        let table = window_table_name(dataset, partition, window);
        let count: u64 =
            self.conn
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
        Ok(count)
    }
}

/// `DropWindow`'s transaction body: `DROP TABLE` + registry delete, one
/// commit. The caller rolls back on error.
fn drop_window_txn(
    conn: &Connection,
    table: &str,
    dataset: &DatasetId,
    partition: &PartitionId,
    window: WindowId,
) -> Result<(), duckdb::Error> {
    conn.execute_batch("BEGIN TRANSACTION")?;
    conn.execute_batch(&format!("DROP TABLE {table}"))?;
    conn.execute(
        "DELETE FROM duckspout_windows
         WHERE dataset = ? AND partition = ? AND window_id = ?",
        duckdb::params![dataset.as_str(), partition.as_str(), window.0],
    )?;
    conn.execute_batch("COMMIT")?;
    Ok(())
}

fn load_applied(
    conn: &Connection,
    origin: &NodeId,
) -> Result<HashMap<PartitionId, u64>, StagingError> {
    let mut stmt =
        conn.prepare("SELECT partition, applied_seq FROM duckspout_applied WHERE origin = ?")?;
    let rows = stmt.query_map([origin.as_str()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
    })?;
    let mut applied = HashMap::new();
    for row in rows {
        let (partition, seq) = row?;
        applied.insert(PartitionId::new(partition), seq);
    }
    Ok(applied)
}

fn load_window_hw(
    conn: &Connection,
) -> Result<HashMap<(DatasetId, PartitionId), u64>, StagingError> {
    let mut stmt = conn.prepare("SELECT dataset, partition, hw FROM duckspout_window_hw")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, u64>(2)?,
        ))
    })?;
    let mut window_hw = HashMap::new();
    for row in rows {
        let (dataset, partition, hw) = row?;
        window_hw.insert((DatasetId::new(dataset), PartitionId::new(partition)), hw);
    }
    Ok(window_hw)
}

fn load_windows(conn: &Connection) -> Result<HashMap<WindowKey, WindowMeta>, StagingError> {
    let mut stmt = conn.prepare(
        "SELECT dataset, partition, window_id, table_name, staged_bytes FROM duckspout_windows",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, u64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, u64>(4)?,
        ))
    })?;
    let mut windows = HashMap::new();
    for row in rows {
        let (dataset, partition, window, table, bytes) = row?;
        windows.insert(
            (DatasetId::new(dataset), PartitionId::new(partition), window),
            WindowMeta { table, bytes },
        );
    }
    Ok(windows)
}

/// The staged payload-type subset → `DuckDB` DDL type. Closed and small on
/// purpose: exactly what the fixed spec-derived OTLP mapping needs (§4.8);
/// widen by need, with tests, never speculatively.
fn staging_sql_type(datatype: &DataType) -> Option<&'static str> {
    match datatype {
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Int8 => Some("TINYINT"),
        DataType::Int16 => Some("SMALLINT"),
        DataType::Int32 => Some("INTEGER"),
        DataType::Int64 => Some("BIGINT"),
        DataType::UInt8 => Some("UTINYINT"),
        DataType::UInt16 => Some("USMALLINT"),
        DataType::UInt32 => Some("UINTEGER"),
        DataType::UInt64 => Some("UBIGINT"),
        DataType::Float32 => Some("FLOAT"),
        DataType::Float64 => Some("DOUBLE"),
        DataType::Utf8 => Some("VARCHAR"),
        DataType::Binary => Some("BLOB"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some("TIMESTAMP"),
        _ => None,
    }
}

/// Quotes an arbitrary payload column name as a `DuckDB` identifier.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The payload batch plus the two system columns (§2.3): `origin` repeated,
/// `seq` = `first..=last`.
fn augment_batch(
    batch: &RecordBatch,
    origin: &NodeId,
    first: u64,
    last: u64,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut fields: Vec<FieldRef> = batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(SYS_COL_ORIGIN, DataType::Utf8, false)));
    fields.push(Arc::new(Field::new(SYS_COL_SEQ, DataType::UInt64, false)));
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![
        origin.as_str();
        batch.num_rows()
    ])));
    columns.push(Arc::new(UInt64Array::from_iter_values(first..=last)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Drives one port future to completion on the calling thread (module docs:
/// the engine is a blocking component; callers embed it off the reactor).
fn block_on<T>(mut future: BoxFuture<'_, T>) -> T {
    struct ThreadWaker(std::thread::Thread);
    impl std::task::Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
