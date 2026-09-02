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
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use duckspout_types::{
    BoxFuture, DatasetId, NodeId, OriginSeqRange, PartitionId, StagedCoverage, Storage,
    StoragePath, WindowId,
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
    windows: HashMap<WindowKey, String>,
    /// Highest window id ever created per (dataset, partition) — the
    /// in-memory mirror of `duckspout_window_hw`. Survives `DropWindow` and
    /// restart, which is what keeps allocated window ids dense and
    /// never-reused (§2.3: contiguity of the per-partition window sequence
    /// must stay decidable after windows drain away).
    window_hw: HashMap<(DatasetId, PartitionId), u64>,
}

/// The staging engine: WAL=hot over one persistent `DuckDB` database. See
/// the [module docs](self) for the connection topology, the checkpoint
/// scheme, and the storage-port boundary.
pub struct StagingEngine<S: Storage> {
    writer: Mutex<WriterInner>,
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
                 dataset    VARCHAR NOT NULL,
                 partition  VARCHAR NOT NULL,
                 window_id  UBIGINT NOT NULL,
                 table_name VARCHAR NOT NULL,
                 PRIMARY KEY (dataset, partition, window_id));
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

        // Name durability for hot.db and hot.db.wal (module docs; ADR-0003).
        block_on(storage.fsync_dir(StoragePath::new("")))?;

        Ok(Self {
            writer: Mutex::new(WriterInner {
                conn,
                applied,
                windows,
                window_hw,
            }),
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
        let Some(table) = writer.windows.get(&key).cloned() else {
            return Ok(false);
        };
        if let Err(error) = drop_window_txn(&writer.conn, &table, dataset, partition, window) {
            let _ = writer.conn.execute_batch("ROLLBACK");
            return Err(error.into());
        }
        writer.windows.remove(&key);
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
            .map(|((dataset, partition, window), table_name)| WindowRef {
                dataset: dataset.clone(),
                partition: partition.clone(),
                window: WindowId(*window),
                table_name: table_name.clone(),
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
        Ok(self.lock_writer()?.windows.get(&key).cloned())
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
            self.writer.windows.insert(key, table);
        }
        let mut coverage = Vec::with_capacity(self.pending_applied.len());
        for (partition, (first, last)) in &self.pending_applied {
            self.writer.applied.insert(partition.clone(), *last);
            coverage.push(StagedCoverage {
                partition: partition.clone(),
                range: OriginSeqRange {
                    origin: self.origin.clone(),
                    first_seq: *first,
                    last_seq: *last,
                },
            });
        }
        Ok(coverage)
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
            return Ok(existing.clone());
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

fn load_windows(conn: &Connection) -> Result<HashMap<WindowKey, String>, StagingError> {
    let mut stmt =
        conn.prepare("SELECT dataset, partition, window_id, table_name FROM duckspout_windows")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, u64>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut windows = HashMap::new();
    for row in rows {
        let (dataset, partition, window, table) = row?;
        windows.insert(
            (DatasetId::new(dataset), PartitionId::new(partition), window),
            table,
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
