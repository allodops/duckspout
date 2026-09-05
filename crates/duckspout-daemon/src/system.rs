//! The composition root's own concrete OS-facing primitives (§10.1: only
//! the daemon and the bins may touch these; every protocol crate reaches
//! time and storage exclusively through the D-2 ports).
//!
//! - [`FsStorage`] — a real-filesystem [`Storage`] port, rooted at one
//!   directory (the hot volume). Promoted from the shape proven in
//!   `tests/otlp_e2e.rs`'s test-local `FsStorage`.
//! - [`SystemClock`] — a real [`Clock`] port: wall time from
//!   [`std::time::SystemTime`], monotonic time from [`std::time::Instant`]
//!   relative to process start.
//! - [`parse_duration`] — the `"60s"` / `"30m"` / `"24h"` string parsing the
//!   §9.6.1 duration settings use on the wire (`config.rs`'s module docs).
//! - [`detect_hot_max_bytes`], [`detect_memory_budget`] — the two §9.6.1
//!   autodetected defaults (`hot.max_bytes`: 75% of the hot volume;
//!   `admission.max_inflight_bytes`: 10% of the memory budget), read once at
//!   boot when the operator leaves the setting unset.
//! - [`detect_node_id`] — this node's [`NodeId`], `<hostname>/<incarnation>`
//!   (§5's origin rendering). v0.1 has no catalog-minted incarnation
//!   (`FenceBoot` is replication's, v0.2): the incarnation is fixed at `1`,
//!   disclosed in the constant's doc rather than guessed silently.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use duckspout_types::{BoxFuture, Clock, NodeId, Storage, StorageError, StoragePath};

/// v0.1's fixed incarnation: no catalog-minted `FenceBoot` sequence exists
/// yet (replication, v0.2) — see [`detect_node_id`].
pub const V01_FIXED_INCARNATION: &str = "1";

/// `hot.max_bytes`'s autodetected fraction of the hot volume's total
/// capacity (§9.6.1: "75% of volume at startup").
const HOT_MAX_BYTES_VOLUME_FRACTION: f64 = 0.75;

/// `admission.max_inflight_bytes`'s autodetected fraction of the memory
/// budget (§9.6.1: "10% of the memory budget").
const ADMISSION_MEMORY_BUDGET_FRACTION: f64 = 0.10;

/// A real-filesystem [`Storage`] port, rooted at one directory. The engine's
/// own content durability is `DuckDB`'s documented fsync-on-commit WAL
/// (ADR-0003, trusted as published); this port covers what that
/// documentation does not pin down — directory-entry durability
/// (`crates/duckspout-staging/src/engine.rs` module docs).
#[derive(Debug, Clone)]
pub struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    /// Roots the port at `root`, creating the directory if absent.
    ///
    /// # Errors
    ///
    /// Any [`std::io::Error`] creating `root`.
    pub fn create(root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn resolve(&self, path: &StoragePath) -> PathBuf {
        if path.as_str().is_empty() {
            self.root.clone()
        } else {
            self.root.join(path.as_str())
        }
    }

    fn ready<T: Send + 'static>(
        result: Result<T, StorageError>,
    ) -> Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send>> {
        Box::pin(async move { result })
    }
}

impl Storage for FsStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::write(self.resolve(&path), &data)
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        Self::ready(
            std::fs::read(self.resolve(&path))
                .map(Bytes::from)
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::remove_file(self.resolve(&path))
                .map_err(|e| StorageError::Backend(e.to_string())),
        )
    }

    fn fsync_file(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::File::open(self.resolve(&path))
                .and_then(|f| f.sync_all())
                .map_err(|_| StorageError::FsyncFailed(path.clone())),
        )
    }

    fn fsync_dir(&self, dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Self::ready(
            std::fs::File::open(self.resolve(&dir))
                .and_then(|f| f.sync_all())
                .map_err(|_| StorageError::FsyncFailed(dir.clone())),
        )
    }
}

/// A real [`Clock`] port: wall time from [`SystemTime`], monotonic time from
/// [`Instant`] relative to this clock's construction (D-2: no invariant
/// reads a clock — this exists for window rolling, dedup TTL bookkeeping,
/// and the §6.3 lateness hold only).
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    epoch: Instant,
}

impl SystemClock {
    /// Starts the clock's monotonic epoch now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn monotonic_nanos(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn wall_unix_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }
}

/// A malformed duration string (§9.6's `"60s"` / `"30m"` / `"24h"` shape).
#[derive(Debug, thiserror::Error)]
#[error("not a duration (expected e.g. \"60s\", \"30m\", \"24h\"): {0:?}")]
pub struct DurationParseError(String);

/// Parses one of the §9.6.1 duration settings: digits followed by exactly
/// one of `s` (seconds), `m` (minutes), or `h` (hours) — the only suffixes
/// any default in `config::defaults` uses. Not a general-purpose duration
/// grammar by design (KISS): widen by need, with a test, never speculatively.
///
/// # Errors
///
/// [`DurationParseError`] for anything else.
pub fn parse_duration(raw: &str) -> Result<std::time::Duration, DurationParseError> {
    let bad = || DurationParseError(raw.to_owned());
    let (digits, unit_secs) = match raw.strip_suffix('h') {
        Some(digits) => (digits, 3_600),
        None => match raw.strip_suffix('m') {
            Some(digits) => (digits, 60),
            None => match raw.strip_suffix('s') {
                Some(digits) => (digits, 1),
                None => return Err(bad()),
            },
        },
    };
    let count: u64 = digits.parse().map_err(|_| bad())?;
    Ok(std::time::Duration::from_secs(
        count.saturating_mul(unit_secs),
    ))
}

/// A parsed §9.6.1 duration, saturating to nanoseconds for the [`Clock`]
/// port's monotonic-time inputs (`hot.window`'s `StagerConfig::window_nanos`
/// — no realistic configured duration is anywhere near `u64::MAX` ns
/// (~584 years), but the conversion is total rather than a silent wrap).
#[must_use]
pub fn duration_nanos_saturating(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// A parsed §9.6.1 duration, saturating to milliseconds for the wall-time
/// settings (`dedup.window_ttl`, `drain.allowed_lateness`).
#[must_use]
pub fn duration_millis_saturating(duration: std::time::Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// `hot.max_bytes`'s autodetected default (§9.6.1): 75% of the hot volume's
/// total capacity, read once at boot when the operator leaves the setting
/// unset. `hot_dir` must already exist.
///
/// # Errors
///
/// Any [`std::io::Error`] reading the volume's capacity.
pub fn detect_hot_max_bytes(hot_dir: &Path) -> std::io::Result<u64> {
    let total = fs4::total_space(hot_dir)?;
    // Precision/sign loss is immaterial here: a byte count converted to
    // `f64` and back loses at most a handful of low bits at realistic
    // volume sizes, and the multiplier is a positive fraction < 1.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let bytes = (total as f64 * HOT_MAX_BYTES_VOLUME_FRACTION) as u64;
    Ok(bytes)
}

/// `admission.max_inflight_bytes`'s autodetected default (§9.6.1): 10% of
/// the memory budget — the cgroup limit when the process is confined to
/// one, else system RAM. Linux-only (the shipped deployment surface, §9.1);
/// falls back through cgroup v2 → cgroup v1 → `/proc/meminfo` and reads no
/// value it cannot parse as a plain byte count.
///
/// # Errors
///
/// [`std::io::Error`] when none of the three sources is readable.
pub fn detect_memory_budget() -> std::io::Result<u64> {
    let budget = memory_budget_bytes()?;
    // See the same justification in `detect_hot_max_bytes` above.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let bytes = (budget as f64 * ADMISSION_MEMORY_BUDGET_FRACTION) as u64;
    Ok(bytes)
}

fn memory_budget_bytes() -> std::io::Result<u64> {
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = raw.trim();
        if trimmed != "max"
            && let Ok(limit) = trimmed.parse::<u64>()
        {
            return Ok(limit);
        }
    }
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        // cgroup v1's "no limit" reads as a page-aligned value near u64::MAX
        // (typically 2^63 minus a page); anything past 2^62 is not a real
        // budget.
        if let Ok(limit) = raw.trim().parse::<u64>()
            && limit < (1_u64 << 62)
        {
            return Ok(limit);
        }
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo")?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib: u64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "unparseable MemTotal")
                })?;
            return Ok(kib.saturating_mul(1024));
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "no memory budget source readable (cgroup v2, cgroup v1, /proc/meminfo)",
    ))
}

/// This node's identity (§5's origin rendering, `<node_id>/<incarnation>`):
/// an explicit [`DUCKSPOUT_NODE_HOSTNAME_OVERRIDE`] override when set,
/// else the OS hostname (`/proc/sys/kernel/hostname`, falling back to the
/// `HOSTNAME` environment variable, falling back to a fixed literal when
/// neither is readable — a dev sandbox with no hostname source is still a
/// bootable single node) paired with `incarnation`.
///
/// v0.1 has no catalog-minted `FenceBoot` incarnation sequence (replication,
/// v0.2): callers pass [`V01_FIXED_INCARNATION`], disclosed here rather than
/// silently assumed.
#[must_use]
pub fn detect_node_id(incarnation: &str) -> NodeId {
    let host = std::env::var(DUCKSPOUT_NODE_HOSTNAME_OVERRIDE)
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .ok()
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| std::env::var("HOSTNAME").ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "duckspout-node".to_owned());
    NodeId::new(format!("{host}/{incarnation}"))
}

/// An explicit override for [`detect_node_id`]'s hostname component,
/// checked before `/proc/sys/kernel/hostname` (issue #201): co-located
/// `duckspout-daemon` processes on ONE physical/container host all share the
/// same kernel hostname, so `duckspout-fleet` sets this per child process to
/// give each real node a distinct [`NodeId`] without a distinct kernel
/// hostname (which would need a network namespace / `CAP_SYS_ADMIN`, not
/// available to a plain fleet-runner process). Deliberately a dedicated
/// name, not a reordering of the existing `HOSTNAME` fallback: many
/// unrelated tools and shells already set `HOSTNAME`, and reordering that
/// check ahead of the kernel hostname would silently change v0.1's existing
/// behavior wherever `HOSTNAME` happens to be set to something else.
pub const DUCKSPOUT_NODE_HOSTNAME_OVERRIDE: &str = "DUCKSPOUT_NODE_HOSTNAME";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parses_the_documented_suffixes() {
        assert_eq!(
            parse_duration("60s").unwrap(),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            parse_duration("30m").unwrap(),
            std::time::Duration::from_mins(30)
        );
        assert_eq!(
            parse_duration("24h").unwrap(),
            std::time::Duration::from_hours(24)
        );
        assert!(parse_duration("60").is_err());
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn system_clock_advances_monotonically_and_reports_real_wall_time() {
        let clock = SystemClock::new();
        let first = clock.monotonic_nanos();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let second = clock.monotonic_nanos();
        assert!(second > first);
        // Sane wall-clock bound: after 2020-01-01T00:00:00Z.
        assert!(clock.wall_unix_ms() > 1_577_836_800_000);
    }

    #[test]
    fn node_id_pairs_a_nonempty_host_with_the_incarnation() {
        let id = detect_node_id(V01_FIXED_INCARNATION);
        assert!(id.as_str().ends_with("/1"));
        assert!(id.as_str().len() > 2);
    }

    #[test]
    fn fs_storage_round_trips_through_a_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::create(dir.path().to_path_buf()).unwrap();
        let path = StoragePath::new("a/b.txt");
        std::fs::create_dir_all(dir.path().join("a")).unwrap();
        futures_lite_block_on(storage.put(path.clone(), Bytes::from_static(b"hi"))).unwrap();
        let back = futures_lite_block_on(storage.get(path.clone())).unwrap();
        assert_eq!(back.as_ref(), b"hi");
        futures_lite_block_on(storage.fsync_file(path.clone())).unwrap();
        futures_lite_block_on(storage.fsync_dir(StoragePath::new("a"))).unwrap();
        futures_lite_block_on(storage.delete(path)).unwrap();
    }

    /// Drives a `Storage` port future to completion without pulling in a
    /// dev-dependency just for this: the port's futures already resolve
    /// synchronously over real `std::fs` calls (module docs).
    fn futures_lite_block_on<T>(mut future: BoxFuture<'_, T>) -> T {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match std::future::Future::poll(future.as_mut(), &mut cx) {
            std::task::Poll::Ready(value) => value,
            std::task::Poll::Pending => panic!("FsStorage future did not resolve synchronously"),
        }
    }
}
