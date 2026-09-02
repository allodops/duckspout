//! The first `LakeCommitter` backend: `DuckLake` (§6.4, ADR-0010).
//!
//! The committer embeds a `DuckDB` instance used purely as a
//! metadata-commit executor — rows never transit it. Catalog access goes
//! **through the ducklake extension's committed surface** (`ATTACH
//! 'ducklake:…'`, `ducklake_add_data_files`, plain SQL over lake tables) —
//! never by writing `DuckLake`'s internal catalog schema (ADR-0010).
//!
//! # One snapshot, one atomicity domain (ADR-0010)
//!
//! `commit_files` executes, in **one** explicit transaction on the attached
//! lake: `CALL ducklake_add_data_files(…)` for the sealed parts, the §6.8
//! manifest record (`duckspout_manifests`), the per-partition watermark
//! upsert (`duckspout_watermarks` — the §7.3 registry row *realized* as a
//! lake table), and the fence-row update (below). Commit → all visible;
//! abort/crash → none (proven commit/abort/crash-mid-commit by spike #25,
//! `spike/tests/drain.rs`).
//!
//! # The `SingleDrainCommit` fence (§6.6, ADR-0010, TN-36)
//!
//! `DuckLake` has no UNIQUE constraints and will double-register a file
//! (spike #25), so the fence is built from two mechanisms above the lake's
//! own tables, both inside the commit transaction:
//!
//! - **Check-before-register** (§6.5): a manifest row already present for
//!   the exact `(partition, window_id, part_name)` short-circuits to
//!   `Committed`. Manifest rows are **never deleted** — `expire` only marks
//!   them — so the check spans lake ∪ expired parts (TN-36, issue #142): a
//!   window whose part was expired by retention can never be re-admitted.
//! - **The fence-row conflict** (ADR-0010's candidate mechanism, proven by
//!   the racing-drains tests): every commit `UPDATE`s its partition's row
//!   in `duckspout_fence` (ensured to exist *before* the transaction), so
//!   two racing commits write the same row and exactly one is admitted.
//!   The loser fails at whichever layer catches the race first — the
//!   catalog's own write serialization (SQLite: `database is locked`) or
//!   `DuckLake`'s snapshot-commit conflict — and both are classified
//!   `Aborted` on a local catalog: the executor is in-process, so a
//!   failed `COMMIT` is definitively not-committed.
//!   The loser then resolves via read-back (§6.5). The committer sets
//!   `ducklake_max_retry_count = 0`, and this is **load-bearing**:
//!   `DuckLake`'s silent conflict-retry rebases and replays the loser's
//!   writes *after* the winner — a double commit by exactly the blind
//!   retry §6.5 forbids.
//!
//! # Catalog backends (issue #119)
//!
//! - A **`DuckDB`-file catalog is not multi-process** (a second process's
//!   `ATTACH` fails under an active drain; in-process tests false-pass via
//!   POSIX lock semantics). [`DuckLakeConfig::multi_process`] = `true`
//!   rejects it at open.
//! - A **SQLite catalog requires `META_JOURNAL_MODE 'WAL'`** (with the
//!   default rollback journal the drain's `COMMIT` fails while any client
//!   transaction is open); the committer forces the option at `ATTACH`.
//! - **Postgres is the multi-process answer** (the §7.3 topology).
//!
//! # Blocking discipline
//!
//! Every operation drives the embedded engine synchronously and the port
//! futures complete immediately; callers embed the committer off their
//! reactor (same discipline as the staging engine, ADR-0003).
//!
//! Design home: `docs/design/drain.md` §6.4–§6.5, ADR-0010.

#![forbid(unsafe_code)]

use std::fmt::Write as _;
use std::sync::Mutex;

use duckdb::Connection;
use duckspout_types::{
    AttachInfo, BoxFuture, CommitOutcome, DatasetId, LakeCommitter, LakeError, OriginSeqRange,
    PartName, PartitionId, SchemaEvolution, WatermarkRow, WindowId, WindowManifest,
};

/// The lake catalog alias every SQL statement addresses.
const LAKE: &str = "lake";

/// How a `DuckLake` catalog is reached and where its data lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckLakeConfig {
    /// The catalog location, as the ducklake `ATTACH` URI *body* (the
    /// `ducklake:` scheme is prepended by the committer): a filesystem path
    /// (`DuckDB`-file catalog), `sqlite:<path>`, or `postgres:<libpq DSN>`.
    pub catalog_uri: String,
    /// The lake's `DATA_PATH` — also the base under which a
    /// [`PartName`] resolves to its object (`{data_path}/{part_name}`):
    /// the drain PUTs into the same prefix `DuckLake` registers from.
    pub data_path: String,
    /// Whether more than one process will commit through this catalog.
    /// `true` rejects a `DuckDB`-file catalog at open (issue #119).
    pub multi_process: bool,
    /// S3-compatible credentials the embedded `DuckDB` executor needs when
    /// `data_path` is itself an `s3://` URI: `ducklake_add_data_files`
    /// reads a sealed part's own footer to register it, so the *metadata*
    /// connection needs read access to the object the drain's
    /// `object_store` client just PUT — a second, independent credential
    /// path from the drain's own S3 client (§6.1). `None` for a local
    /// `data_path` (v0.1's only production topology, §9.1); the first
    /// caller is the conformance gate's real-backend capture (§8.2, issue
    /// #44), which is also the first thing in this repository to talk to
    /// S3 at all.
    pub s3: Option<S3Access>,
}

/// The S3-compatible endpoint + credentials `CREATE SECRET` needs
/// (`DuckDB`'s own `httpfs` extension, trusted as documented per
/// R-trust-official-docs:
/// a secret's `ENDPOINT`/`URL_STYLE`/`USE_SSL` fields are exactly `MinIO`'s
/// documented S3-compatibility knobs). Never logged: [`S3Access`]'s own
/// `Debug` impl redacts the credential fields (§9.5).
#[derive(Clone, PartialEq, Eq)]
pub struct S3Access {
    /// `host:port`, no scheme (`DuckDB`'s `httpfs` `ENDPOINT` secret field
    /// convention — `use_ssl` carries the scheme separately), e.g. `MinIO`'s
    /// `127.0.0.1:9000`.
    pub endpoint: String,
    /// A region string `DuckDB`'s `httpfs` requires even against a
    /// region-less endpoint like `MinIO`; any non-empty value is accepted by
    /// the endpoint, `us-east-1` is the documented convention.
    pub region: String,
    /// The `MinIO`/S3 access key id. Redacted by this struct's [`Debug`]
    /// impl (§9.5).
    pub access_key_id: String,
    /// The `MinIO`/S3 secret access key. Redacted by this struct's [`Debug`]
    /// impl (§9.5).
    pub secret_access_key: String,
    /// `true` for `MinIO` and most self-hosted S3-compatible stores
    /// (`URL_STYLE 'path'`); `false` for AWS S3's virtual-hosted style.
    pub url_style_path: bool,
    /// `false` for a plain-HTTP dev endpoint (`MinIO`'s default); `true` for
    /// TLS-terminated S3.
    pub use_ssl: bool,
}

impl std::fmt::Debug for S3Access {
    // Secrets are never printed (§9.5) — same discipline as
    // DuckLakeCommitter's own Debug impl.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Access")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("url_style_path", &self.url_style_path)
            .field("use_ssl", &self.use_ssl)
            .finish()
    }
}

/// The catalog backend classes of issue #119.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogKind {
    DuckDbFile,
    Sqlite,
    Postgres,
}

fn catalog_kind(uri: &str) -> CatalogKind {
    if uri.starts_with("postgres:") || uri.starts_with("postgresql:") {
        CatalogKind::Postgres
    } else if uri.starts_with("sqlite:") {
        CatalogKind::Sqlite
    } else {
        CatalogKind::DuckDbFile
    }
}

/// The `DuckLake` committer: an embedded `DuckDB` with the lake attached,
/// serialized behind a mutex (one metadata transaction at a time per
/// committer; concurrency across committers/processes is the catalog's
/// business — that is what the fence proves).
pub struct DuckLakeCommitter {
    conn: Mutex<Connection>,
    config: DuckLakeConfig,
}

impl std::fmt::Debug for DuckLakeCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeCommitter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DuckLakeCommitter {
    /// Opens the embedded executor: `INSTALL`/`LOAD` ducklake (network on
    /// first ever use; cached in `~/.duckdb`), runs the issue #119 startup
    /// checks, attaches the catalog, disables `DuckLake`'s silent
    /// conflict-retry (module docs), and ensures the sidecar tables.
    ///
    /// # Errors
    ///
    /// [`LakeError::Misconfigured`] for a rejected catalog class (#119);
    /// [`LakeError::Backend`] if the extension, attach, or sidecar
    /// bootstrap fails.
    pub fn open(config: DuckLakeConfig) -> Result<Self, LakeError> {
        let kind = catalog_kind(&config.catalog_uri);
        if config.multi_process && kind == CatalogKind::DuckDbFile {
            return Err(LakeError::Misconfigured(format!(
                "a DuckDB-file catalog ({}) is single-process only: a second process's ATTACH \
                 fails under an active drain (issue #119). Use a Postgres catalog for \
                 multi-process topologies (§7.3), or set multi_process = false.",
                config.catalog_uri
            )));
        }
        let conn = Connection::open_in_memory().map_err(|e| backend(&e))?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .map_err(|e| LakeError::Backend(format!("install/load ducklake extension: {e}")))?;
        // A conflicted commit must surface as Aborted, never be silently
        // replayed after the winner (module docs; §6.5's no-blind-retry).
        conn.execute_batch("SET ducklake_max_retry_count = 0;")
            .map_err(|e| backend(&e))?;
        if let Some(s3) = &config.s3 {
            // httpfs is DuckDB's own S3 client; ducklake_add_data_files
            // resolves an s3:// DATA_PATH through it, so the metadata
            // connection needs the same credential the drain's
            // object_store client PUT the part with (struct docs).
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .map_err(|e| LakeError::Backend(format!("install/load httpfs extension: {e}")))?;
            conn.execute_batch(&format!(
                "CREATE OR REPLACE SECRET duckspout_s3 (
                     TYPE s3,
                     KEY_ID '{}',
                     SECRET '{}',
                     ENDPOINT '{}',
                     REGION '{}',
                     URL_STYLE '{}',
                     USE_SSL {}
                 );",
                sql_str_body(&s3.access_key_id),
                sql_str_body(&s3.secret_access_key),
                sql_str_body(&s3.endpoint),
                sql_str_body(&s3.region),
                if s3.url_style_path { "path" } else { "vhost" },
                s3.use_ssl,
            ))
            .map_err(|e| LakeError::Backend(format!("create S3 secret: {e}")))?;
        }
        let journal = match kind {
            // #119: the default rollback journal deadlocks the drain's
            // COMMIT against any open client transaction.
            CatalogKind::Sqlite => {
                conn.execute_batch("INSTALL sqlite; LOAD sqlite;")
                    .map_err(|e| {
                        LakeError::Backend(format!("install/load sqlite extension: {e}"))
                    })?;
                ", META_JOURNAL_MODE 'WAL'"
            }
            CatalogKind::DuckDbFile | CatalogKind::Postgres => "",
        };
        conn.execute_batch(&format!(
            "ATTACH IF NOT EXISTS 'ducklake:{}' AS {LAKE} (DATA_PATH '{}'{journal});",
            sql_str_body(&config.catalog_uri),
            sql_str_body(&config.data_path),
        ))
        .map_err(|e| LakeError::Backend(format!("attach ducklake catalog: {e}")))?;
        // Sidecar tables: ordinary lake tables under the same snapshot
        // domain (ADR-0010). duckspout_manifests is append-and-mark only —
        // rows are never deleted (TN-36).
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {LAKE}.duckspout_watermarks (
                 partition           VARCHAR,
                 complete_through_ms BIGINT);
             CREATE TABLE IF NOT EXISTS {LAKE}.duckspout_manifests (
                 partition         VARCHAR,
                 window_id         BIGINT,
                 part_name         VARCHAR,
                 dataset           VARCHAR,
                 rows              BIGINT,
                 event_time_min_ms BIGINT,
                 event_time_max_ms BIGINT,
                 dedup_removed     BIGINT,
                 origin_coverage   VARCHAR,
                 expired           BOOLEAN);
             CREATE TABLE IF NOT EXISTS {LAKE}.duckspout_fence (
                 partition   VARCHAR,
                 last_window BIGINT);",
        ))
        .map_err(|e| LakeError::Backend(format!("bootstrap sidecar tables: {e}")))?;
        Ok(Self {
            conn: Mutex::new(conn),
            config,
        })
    }

    /// The configuration this committer was opened with.
    #[must_use]
    pub fn config(&self) -> &DuckLakeConfig {
        &self.config
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LakeError> {
        self.conn
            .lock()
            .map_err(|_| LakeError::Backend("committer poisoned: a holder panicked".into()))
    }

    /// One commit attempt, blocking. See the module docs for the shape.
    #[allow(clippy::too_many_lines)] // one transaction, told linearly
    fn commit_files_blocking(
        &self,
        manifest: &WindowManifest,
        watermarks: &[WatermarkRow],
    ) -> Result<CommitOutcome, LakeError> {
        let conn = self.lock()?;

        // Ensure the fence row exists BEFORE the transaction, so racing
        // commits always collide on an UPDATE of the same row (module
        // docs). Idempotent; a duplicate row from a racing ensure is
        // harmless — both racers then update both rows. Its own autocommit
        // can lose a catalog race too (e.g. SQLite's `database is locked`):
        // whatever became of the *ensure*, the manifest commit never began
        // — a definitive `Aborted`, retried safely by the drain (§6.5).
        if conn
            .execute(
                "INSERT INTO lake.duckspout_fence
                 SELECT ?, -1 WHERE NOT EXISTS
                     (SELECT 1 FROM lake.duckspout_fence WHERE partition = ?)",
                duckdb::params![manifest.partition.as_str(), manifest.partition.as_str()],
            )
            .is_err()
        {
            return Ok(CommitOutcome::Aborted);
        }

        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|e| backend(&e))?;
        let staged = Self::stage_commit_writes(&conn, &self.config, manifest, watermarks);
        match staged {
            Ok(Staged::Wrote) => {}
            Ok(Staged::AlreadyRegistered) => {
                // Check-before-register (§6.5): every named part already
                // stands (expired ones included, TN-36) — short-circuit to
                // Committed without writing anything.
                let _ = conn.execute_batch("ROLLBACK");
                return Ok(CommitOutcome::Committed);
            }
            Err(error) => {
                // A pre-COMMIT failure aborts cleanly: nothing changed.
                let _ = conn.execute_batch("ROLLBACK");
                let rendered = error.to_string();
                let lowered = rendered.to_ascii_lowercase();
                if lowered.contains("conflict") {
                    // A DuckLake transaction conflict surfacing on a write
                    // statement — the fence's Aborted (§6.6).
                    return Ok(CommitOutcome::Aborted);
                }
                if lowered.contains("does not exist")
                    || lowered.contains("binder error")
                    || lowered.contains("catalog error")
                {
                    // Backend-invariant: a retry cannot heal a missing
                    // table/column (evolve-before-add was skipped, §6.4) —
                    // a typed error, never a silent Aborted loop.
                    return Err(error);
                }
                // Anything else is a transient rejection: nothing changed,
                // the drain requeues (§6.5).
                return Ok(CommitOutcome::Aborted);
            }
        }
        match conn.execute_batch("COMMIT") {
            Ok(()) => Ok(CommitOutcome::Committed),
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                Ok(commit_error_outcome(
                    catalog_kind(&self.config.catalog_uri),
                    &error.to_string(),
                ))
            }
        }
    }

    /// The writes of one commit transaction. Per-part
    /// check-before-register (§6.5): a name already present — expired ones
    /// included (TN-36) — is skipped, never re-registered; when every part
    /// is present, [`Staged::AlreadyRegistered`] short-circuits the whole
    /// attempt.
    fn stage_commit_writes(
        conn: &Connection,
        config: &DuckLakeConfig,
        manifest: &WindowManifest,
        watermarks: &[WatermarkRow],
    ) -> Result<Staged, LakeError> {
        let window_id = i64::try_from(manifest.window_id.0).unwrap_or(i64::MAX);
        let mut absent = Vec::new();
        for part in &manifest.parts {
            let present: i64 = conn
                .query_row(
                    "SELECT count(*) FROM lake.duckspout_manifests
                     WHERE partition = ? AND window_id = ? AND part_name = ?",
                    duckdb::params![manifest.partition.as_str(), window_id, part.as_str()],
                    |row| row.get(0),
                )
                .map_err(|e| backend(&e))?;
            if present == 0 {
                absent.push(part);
            }
        }
        if absent.is_empty() && !manifest.parts.is_empty() {
            return Ok(Staged::AlreadyRegistered);
        }

        let table = dataset_table(manifest.dataset.as_str());
        let coverage = serde_json::to_string(&manifest.origin_coverage)
            .map_err(|e| LakeError::Backend(format!("encode coverage: {e}")))?;
        for part in absent {
            let path = format!("{}/{}", config.data_path, part.as_str());
            conn.execute_batch(&format!(
                "CALL ducklake_add_data_files('{LAKE}', '{}', '{}')",
                sql_str_body(&table),
                sql_str_body(&path)
            ))
            .map_err(|e| backend(&e))?;
            conn.execute(
                "INSERT INTO lake.duckspout_manifests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, false)",
                duckdb::params![
                    manifest.partition.as_str(),
                    window_id,
                    part.as_str(),
                    manifest.dataset.as_str(),
                    i64::try_from(manifest.rows).unwrap_or(i64::MAX),
                    manifest.event_time_min_ms,
                    manifest.event_time_max_ms,
                    i64::try_from(manifest.dedup_removed).unwrap_or(i64::MAX),
                    coverage
                ],
            )
            .map_err(|e| backend(&e))?;
        }
        for row in watermarks {
            let updated = conn
                .execute(
                    "UPDATE lake.duckspout_watermarks SET complete_through_ms = ?
                     WHERE partition = ?",
                    duckdb::params![row.complete_through_ms, row.partition.as_str()],
                )
                .map_err(|e| backend(&e))?;
            if updated == 0 {
                conn.execute(
                    "INSERT INTO lake.duckspout_watermarks VALUES (?, ?)",
                    duckdb::params![row.partition.as_str(), row.complete_through_ms],
                )
                .map_err(|e| backend(&e))?;
            }
        }
        // The fence-row write: racing commits collide here (module docs).
        conn.execute(
            "UPDATE lake.duckspout_fence SET last_window = ? WHERE partition = ?",
            duckdb::params![window_id, manifest.partition.as_str()],
        )
        .map_err(|e| backend(&e))?;
        Ok(Staged::Wrote)
    }

    fn read_watermarks_blocking(
        &self,
        partitions: &[PartitionId],
    ) -> Result<Vec<WatermarkRow>, LakeError> {
        let conn = self.lock()?;
        let mut rows = Vec::new();
        for partition in partitions {
            let mut stmt = conn
                .prepare(
                    "SELECT max(complete_through_ms) FROM lake.duckspout_watermarks
                     WHERE partition = ?",
                )
                .map_err(|e| backend(&e))?;
            let value: Option<i64> = stmt
                .query_row(duckdb::params![partition.as_str()], |row| row.get(0))
                .map_err(|e| backend(&e))?;
            if let Some(complete_through_ms) = value {
                rows.push(WatermarkRow {
                    partition: partition.clone(),
                    complete_through_ms,
                });
            }
        }
        Ok(rows)
    }

    /// Every committed window manifest recorded in the lake, across every
    /// partition and dataset (§6.8, ADR-0010's "authoritative-but-
    /// reconstructible" property; issue #153). Deliberately **not** part of
    /// the [`LakeCommitter`] port: it is a `DuckLake`-specific boot-recovery
    /// read, consumed only by `duckspout-daemon`'s own composition code
    /// (which already binds this concrete type in `open_lake`) — keeping it
    /// off the port keeps `LakeCommitter`'s six critical-path operations
    /// (§6.4) exactly six, per the Neutrality Keep Rule (§11) and
    /// `docs/seed.md`'s enumeration of them. A future non-`DuckLake` backend
    /// wired into the daemon would need its own equivalent read (or the port
    /// would need to grow one, decided then, not speculatively here).
    ///
    /// One row per `(partition, window_id, part_name)` in
    /// `duckspout_manifests` collapses back into one [`WindowManifest`] per
    /// `(partition, window_id)` — the scalar fields (`dataset`, `rows`,
    /// event-time bounds, `dedup_removed`, `origin_coverage`) are identical
    /// across a window's part rows because `commit_files` always writes them
    /// from the same [`WindowManifest`] argument in one transaction (§6.4);
    /// only `parts` varies per row. Expired parts are **included** — `expire`
    /// only marks `duckspout_manifests` rows, never deletes them (TN-36) —
    /// because a window's contribution to completeness does not un-happen
    /// when its file is later expired.
    ///
    /// # Errors
    ///
    /// [`LakeError::Backend`] for a query or decode failure (a stored
    /// `origin_coverage` value that fails to deserialize means the lake's
    /// own record is corrupt — surfaced, never silently skipped, R-3).
    pub fn read_manifests(&self) -> Result<Vec<WindowManifest>, LakeError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT partition, window_id, part_name, dataset, rows, event_time_min_ms, \
                 event_time_max_ms, dedup_removed, origin_coverage
                 FROM lake.duckspout_manifests
                 ORDER BY partition, window_id",
            )
            .map_err(|e| backend(&e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, String>(8)?,
                ))
            })
            .map_err(|e| backend(&e))?;

        let mut manifests: Vec<WindowManifest> = Vec::new();
        for row in rows {
            let (
                partition,
                window_id,
                part_name,
                dataset,
                row_count,
                event_time_min_ms,
                event_time_max_ms,
                dedup_removed,
                coverage_json,
            ) = row.map_err(|e| backend(&e))?;
            let window_id = WindowId(u64::try_from(window_id).unwrap_or(u64::MAX));
            match manifests.last_mut() {
                Some(last) if last.partition.as_str() == partition && last.window_id == window_id => {
                    last.parts.push(PartName::new(part_name));
                }
                _ => {
                    let origin_coverage: Vec<OriginSeqRange> = serde_json::from_str(&coverage_json)
                        .map_err(|e| {
                            LakeError::Backend(format!(
                                "decode origin_coverage for partition {partition} window \
                                 {}: {e}",
                                window_id.0
                            ))
                        })?;
                    manifests.push(WindowManifest {
                        dataset: DatasetId::new(dataset),
                        partition: PartitionId::new(partition),
                        window_id,
                        origin_coverage,
                        rows: u64::try_from(row_count).unwrap_or(0),
                        event_time_min_ms,
                        event_time_max_ms,
                        dedup_removed: u64::try_from(dedup_removed).unwrap_or(0),
                        parts: vec![PartName::new(part_name)],
                    });
                }
            }
        }
        Ok(manifests)
    }

    fn evolve_schema_blocking(&self, change: &SchemaEvolution) -> Result<(), LakeError> {
        let conn = self.lock()?;
        let table = dataset_table(change.dataset.as_str());
        let exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_tables()
                 WHERE database_name = ? AND table_name = ?",
                duckdb::params![LAKE, table],
                |row| row.get(0),
            )
            .map_err(|e| backend(&e))?;
        if exists == 0 {
            let mut columns = String::new();
            for (i, col) in change.columns.iter().enumerate() {
                if i > 0 {
                    columns.push_str(", ");
                }
                // Infallible: writing to a String cannot fail.
                let _ = write!(
                    columns,
                    "{} {}",
                    column_ident(&col.name),
                    sql_type(&col.logical_type)?
                );
            }
            if columns.is_empty() {
                return Err(LakeError::Misconfigured(
                    "schema evolution creating a table needs at least one column".into(),
                ));
            }
            conn.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {LAKE}.{table} ({columns})"
            ))
            .map_err(|e| backend(&e))?;
            return Ok(());
        }
        for col in &change.columns {
            conn.execute_batch(&format!(
                "ALTER TABLE {LAKE}.{table} ADD COLUMN IF NOT EXISTS {} {}",
                column_ident(&col.name),
                sql_type(&col.logical_type)?
            ))
            .map_err(|e| backend(&e))?;
        }
        Ok(())
    }

    fn expire_blocking(&self, parts: &[PartName]) -> Result<(), LakeError> {
        let conn = self.lock()?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|e| backend(&e))?;
        let result = self.expire_writes(&conn, parts);
        match result {
            Ok(()) => conn.execute_batch("COMMIT").map_err(|e| backend(&e)),
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// The expire transaction body: mark the manifest rows expired (they
    /// are **kept** — the TN-36 fence spans expired parts) and remove the
    /// parts' rows from the table's current snapshot. Physical file
    /// removal on `DuckLake` rides snapshot expiry
    /// (`ducklake_expire_snapshots` + `ducklake_cleanup_old_files`), an
    /// operations-driven maintenance step outside the port — the §6.7
    /// whole-file DELETE happens there, exactly once.
    fn expire_writes(&self, conn: &Connection, parts: &[PartName]) -> Result<(), LakeError> {
        for part in parts {
            let (dataset, already_expired): (String, bool) = conn
                .query_row(
                    "SELECT dataset, expired FROM lake.duckspout_manifests
                     WHERE part_name = ? LIMIT 1",
                    duckdb::params![part.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|_| {
                    LakeError::Backend(format!(
                        "expire: part {part} is not registered in duckspout_manifests"
                    ))
                })?;
            if already_expired {
                continue; // idempotent re-expire
            }
            let table = dataset_table(&dataset);
            let path = format!("{}/{}", self.config.data_path, part.as_str());
            // Rows of exactly this file leave the current snapshot via the
            // filename virtual column — metadata-only from the table's
            // view (§6.7): no data rewrite, a deletion record only.
            conn.execute(
                &format!("DELETE FROM {LAKE}.{table} WHERE filename = ?"),
                duckdb::params![path],
            )
            .map_err(|e| backend(&e))?;
            conn.execute(
                "UPDATE lake.duckspout_manifests SET expired = true WHERE part_name = ?",
                duckdb::params![part.as_str()],
            )
            .map_err(|e| backend(&e))?;
        }
        Ok(())
    }

    fn attach_info_blocking(&self) -> AttachInfo {
        let credentials_shape = match catalog_kind(&self.config.catalog_uri) {
            CatalogKind::Postgres => "libpq DSN via a DuckDB secret (never inline credentials)",
            CatalogKind::Sqlite | CatalogKind::DuckDbFile => "none (file-backed catalog)",
        };
        AttachInfo {
            catalog_uri: format!("ducklake:{}", self.config.catalog_uri),
            credentials_shape: credentials_shape.to_owned(),
            dialect: "ducklake".to_owned(),
        }
    }
}

impl LakeCommitter for DuckLakeCommitter {
    fn commit_files(
        &self,
        manifest: WindowManifest,
        watermarks: Vec<WatermarkRow>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        let result = self.commit_files_blocking(&manifest, &watermarks);
        Box::pin(async move { result })
    }

    fn replace_files(
        &self,
        _remove: Vec<PartName>,
        _add: Vec<PartName>,
    ) -> BoxFuture<'_, Result<CommitOutcome, LakeError>> {
        // Emergency-only (§6.4): operator-invoked repair. Deliberately not
        // implemented at v0.1 — its absence cannot be mistaken for a
        // license to compact, and no v0.1 flow reaches it.
        Box::pin(async move {
            Err(LakeError::NotImplemented(
                "ducklake replace_files (emergency repair; lands with the operations work)",
            ))
        })
    }

    fn evolve_schema(&self, change: SchemaEvolution) -> BoxFuture<'_, Result<(), LakeError>> {
        let result = self.evolve_schema_blocking(&change);
        Box::pin(async move { result })
    }

    fn expire(&self, parts: Vec<PartName>) -> BoxFuture<'_, Result<(), LakeError>> {
        let result = self.expire_blocking(&parts);
        Box::pin(async move { result })
    }

    fn read_watermarks(
        &self,
        partitions: Vec<PartitionId>,
    ) -> BoxFuture<'_, Result<Vec<WatermarkRow>, LakeError>> {
        let result = self.read_watermarks_blocking(&partitions);
        Box::pin(async move { result })
    }

    fn attach_info(&self) -> BoxFuture<'_, Result<AttachInfo, LakeError>> {
        let result = Ok(self.attach_info_blocking());
        Box::pin(async move { result })
    }
}

/// How one commit transaction's writes resolved before `COMMIT`.
enum Staged {
    /// Writes staged; the transaction proceeds to `COMMIT`.
    Wrote,
    /// Every named part already stands (§6.5 check-before-register, over
    /// lake ∪ expired per TN-36): the attempt short-circuits to
    /// `Committed` without writing.
    AlreadyRegistered,
}

/// The lake table of one dataset: `ds_` + the injective `[a-z0-9_]`
/// encoding of the dataset id (the same encoding discipline as cold object
/// names — the lake table name is likewise a public surface, frozen once
/// data exists).
fn dataset_table(dataset: &str) -> String {
    let mut out = String::with_capacity(dataset.len() + 3);
    out.push_str("ds_");
    for byte in dataset.bytes() {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' => out.push(char::from(byte)),
            other => {
                // Infallible: writing to a String cannot fail.
                let _ = write!(out, "_{other:02x}");
            }
        }
    }
    out
}

/// A column identifier, quoted for `DuckDB`.
fn column_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The body of a single-quoted SQL string literal.
fn sql_str_body(raw: &str) -> String {
    raw.replace('\'', "''")
}

/// The closed logical-type lattice this backend accepts (§2's lattice,
/// mirroring the staged payload subset).
fn sql_type(logical: &str) -> Result<&'static str, LakeError> {
    Ok(match logical {
        "boolean" => "BOOLEAN",
        "int8" => "TINYINT",
        "int16" => "SMALLINT",
        "int32" => "INTEGER",
        "int64" => "BIGINT",
        "uint8" => "UTINYINT",
        "uint16" => "USMALLINT",
        "uint32" => "UINTEGER",
        "uint64" => "UBIGINT",
        "float32" => "FLOAT",
        "float64" => "DOUBLE",
        "utf8" => "VARCHAR",
        "binary" => "BLOB",
        "timestamp_micros" => "TIMESTAMP",
        other => {
            return Err(LakeError::Misconfigured(format!(
                "unknown logical type {other:?} (the §2 lattice is closed; widen by review)"
            )));
        }
    })
}

fn backend(error: &duckdb::Error) -> LakeError {
    LakeError::Backend(error.to_string())
}

/// Classifies a failed commit attempt into the §6.5 outcome, catalog-aware:
///
/// - A message naming a **conflict** is `DuckLake`'s snapshot-commit
///   rejection — the fence's `Aborted` (§6.6).
/// - On a **file or SQLite catalog** every commit failure is `Aborted`:
///   the executor is in-process and the catalog local, so there is no
///   lost-response channel — an error from `COMMIT` (including SQLite's
///   `database is locked` when a racing writer holds the catalog) means
///   the transaction did not commit, definitively. This is where the
///   racing loser lands (proven by the racing-drains tests).
/// - On a **Postgres catalog** the response can genuinely be lost in
///   flight, so a non-conflict commit failure is `Indeterminate` — the
///   caller's one read-back resolves it (§6.5).
fn commit_error_outcome(kind: CatalogKind, rendered: &str) -> CommitOutcome {
    let lowered = rendered.to_ascii_lowercase();
    if lowered.contains("conflict") {
        return CommitOutcome::Aborted;
    }
    match kind {
        CatalogKind::DuckDbFile | CatalogKind::Sqlite => CommitOutcome::Aborted,
        CatalogKind::Postgres => CommitOutcome::Indeterminate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_kinds_classify() {
        assert_eq!(catalog_kind("/x/meta.ducklake"), CatalogKind::DuckDbFile);
        assert_eq!(catalog_kind("sqlite:/x/cat.db"), CatalogKind::Sqlite);
        assert_eq!(catalog_kind("postgres:dbname=lake"), CatalogKind::Postgres);
        assert_eq!(catalog_kind("postgresql://u@h/db"), CatalogKind::Postgres);
    }

    #[test]
    fn multi_process_rejects_duckdb_file_catalogs() {
        let err = DuckLakeCommitter::open(DuckLakeConfig {
            catalog_uri: "/tmp/meta.ducklake".into(),
            data_path: "/tmp/data".into(),
            multi_process: true,
            s3: None,
        })
        .expect_err("issue #119: duckdb-file catalogs are single-process");
        assert!(matches!(err, LakeError::Misconfigured(_)));
    }

    #[test]
    fn dataset_tables_are_injective_and_bare() {
        assert_eq!(dataset_table("logs"), "ds_logs");
        assert_eq!(dataset_table("a_b"), "ds_a_5fb");
        assert_ne!(dataset_table("a_"), dataset_table("a"));
    }

    #[test]
    fn unknown_logical_types_are_refused() {
        assert!(sql_type("utf8").is_ok());
        assert!(matches!(
            sql_type("decimal128"),
            Err(LakeError::Misconfigured(_))
        ));
    }

    #[test]
    fn commit_failures_classify_by_catalog_kind() {
        // A conflict is the fence's Aborted everywhere (§6.6).
        assert_eq!(
            commit_error_outcome(CatalogKind::Postgres, "Transaction conflict on table"),
            CommitOutcome::Aborted
        );
        // Local catalogs have no lost-response channel: a failed COMMIT —
        // SQLite's lock-busy included — is definitively not-committed.
        assert_eq!(
            commit_error_outcome(
                CatalogKind::Sqlite,
                "Failed to flush changes into DuckLake: database is locked"
            ),
            CommitOutcome::Aborted
        );
        assert_eq!(
            commit_error_outcome(CatalogKind::DuckDbFile, "IO Error: something odd"),
            CommitOutcome::Aborted
        );
        // Only a remote catalog can genuinely not know (§6.5).
        assert_eq!(
            commit_error_outcome(CatalogKind::Postgres, "connection reset by peer"),
            CommitOutcome::Indeterminate
        );
    }
}
