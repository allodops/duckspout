//! `FencedZombie` (§5.7) for the exact scenario shape §8.4's fault-injection
//! schedule names: "process pauses (SIGSTOP long enough to expire claims,
//! then resume — the `FencedZombie` scenario: the paused node's stale
//! incarnation must be rejected)."
//!
//! # Why this test exists, and what it stands in for (issue #203)
//!
//! `duckspout-fleet`'s SIGSTOP-pause injector
//! (`crates/duckspout-fleet/src/fault.rs::run_sigstop_pause`) sends a REAL
//! `SIGSTOP` to a REAL `duckspout-daemon` process, holds it past
//! `HEARTBEAT_TTL_SECS`, then a REAL `SIGCONT` — that half of §8.4's fault
//! is genuinely real, OS-level, end to end. What it CANNOT currently verify
//! against the live fleet is the second half — that a peer rejects the
//! resumed node's stale incarnation — because `duckspout-daemon`'s
//! composition root does not yet wire `duckspout_replication::boot::fence_boot`
//! or `duckspout_replication::fencing::FenceTable` at all
//! (`crates/duckspout-daemon/src/system.rs::V01_FIXED_INCARNATION` is a
//! hardcoded placeholder every node boots under today), and no crate in
//! this workspace implements `duckspout_types::Transport` over a real
//! network yet (`crates/duckspout-daemon/src/wiring.rs`'s own module docs),
//! so nodes never actually Forward/receipt across the wire at all. A real,
//! paused-then-resumed daemon in today's fleet has no live incarnation-
//! fencing peer for `FencedZombie` to even apply to yet — that daemon-
//! composition wiring is a pre-existing gap this issue's scope does not
//! include closing (see this crate's own `boot.rs`/`fencing.rs` module docs,
//! written before this issue, already disclosing exactly this deferral).
//!
//! This test is the most faithful verification available until that wiring
//! lands: it drives the REAL, production `fence_boot` and `FenceTable`
//! functions — not the P model, not a hand-simulated comparison — through
//! the exact operational scenario a real SIGSTOP-and-presumed-dead-then-
//! resume sequence produces:
//!
//! 1. Node `owner`'s first ever boot: `fence_boot` draws incarnation 1
//!    (persisted locally), which it uses to Forward/receipt — peer `replica`'s
//!    real [`FenceTable`] admits it.
//! 2. `owner` is `SIGSTOP`ped (modeled here as simply "nothing happens to
//!    its in-memory or persisted state" — a pause is not a crash, module
//!    docs above). The pause outlasts `HEARTBEAT_TTL_SECS`, so the rest of
//!    the cluster presumes `owner` dead and a **replacement process for the
//!    SAME logical node identity** is started — reading the SAME
//!    persisted-incarnation file `owner`'s original process wrote in step 1
//!    (the one physical/logical node's own local disk, exactly
//!    `boot.rs`'s own "promotion... is a later call to `fence_boot`" framing)
//!    — and completes `FenceBoot`, drawing a genuinely fresh incarnation 2.
//! 3. The replacement forwards under incarnation 2 — `replica`'s
//!    [`FenceTable`] admits it, advancing its high-water mark for `owner`
//!    to 2.
//! 4. `owner`'s ORIGINAL, merely-paused process now receives `SIGCONT` and
//!    resumes — since it was never rebooted, it still believes its
//!    incarnation is 1 (module docs of `boot.rs`: `DegradedBoot`/pause has
//!    no reboot path of its own). It attempts to Forward/receipt again,
//!    under the now-stale incarnation 1.
//! 5. `replica`'s [`FenceTable::admit`] — the SAME shared guard
//!    `Forward`/`PeerApply`/`Receipt` all use (`fencing.rs`'s own module
//!    docs) — must reject it: [`FenceOutcome::Zombie`], never
//!    [`FenceOutcome::Admitted`]. This assertion is this test's whole
//!    point.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use duckspout_replication::{BootOutcome, FenceIdentity, FenceOutcome, FenceTable, fence_boot};
use duckspout_types::{
    BoxFuture, ClaimRole, NodeId, PartitionId, Registry, RegistryError, Storage, StorageError,
    StoragePath,
};

fn block_on<T>(future: impl Future<Output = T>) -> T {
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

/// A hand-rolled, in-memory [`Storage`] double scoped to exactly the
/// persisted-incarnation read/write `fence_boot` needs — mirrors
/// `boot.rs`'s own private `FakeStorage` (this crate cannot depend on
/// `duckspout-ctk`, ADR-0008, and a protocol crate's own `tests/` doubles
/// are conventionally local, `composed_pipeline.rs`'s own module docs).
#[derive(Default)]
struct FakeStorage {
    files: Mutex<HashMap<String, Bytes>>,
}

impl Storage for FakeStorage {
    fn put(&self, path: StoragePath, data: Bytes) -> BoxFuture<'_, Result<(), StorageError>> {
        self.files
            .lock()
            .expect("lock")
            .insert(path.as_str().to_string(), data);
        Box::pin(std::future::ready(Ok(())))
    }

    fn get(&self, path: StoragePath) -> BoxFuture<'_, Result<Bytes, StorageError>> {
        let result = self
            .files
            .lock()
            .expect("lock")
            .get(path.as_str())
            .cloned()
            .ok_or(StorageError::NotFound(path));
        Box::pin(std::future::ready(result))
    }

    fn delete(&self, path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        self.files.lock().expect("lock").remove(path.as_str());
        Box::pin(std::future::ready(Ok(())))
    }

    fn fsync_file(&self, _path: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn fsync_dir(&self, _dir: StoragePath) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

/// A [`Registry`] double whose incarnation sequence is test-controlled —
/// mirrors `boot.rs`'s own private `FakeRegistry`, `reachable_at` variant
/// only (this scenario never exercises a catalog outage).
struct FakeRegistry {
    next: Mutex<u64>,
}

impl FakeRegistry {
    fn starting_at(next: u64) -> Self {
        Self {
            next: Mutex::new(next),
        }
    }
}

impl Registry for FakeRegistry {
    fn next_incarnation(&self, _node: &NodeId) -> BoxFuture<'_, Result<u64, RegistryError>> {
        let mut next = self.next.lock().expect("lock");
        let value = *next;
        *next += 1;
        Box::pin(std::future::ready(Ok(value)))
    }

    fn advertise_claim(
        &self,
        _partition: &PartitionId,
        _node: &NodeId,
        _role: ClaimRole,
    ) -> BoxFuture<'_, Result<(), RegistryError>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

fn incarnation_paths() -> (StoragePath, StoragePath) {
    (
        StoragePath::new("boot/incarnation"),
        StoragePath::new("boot"),
    )
}

/// The full scenario (module docs above), end to end through the real
/// `fence_boot`/`FenceTable` production code.
#[test]
fn a_resumed_pause_survivor_is_fenced_out_once_a_replacement_has_promoted() {
    block_on(async {
        let owner = NodeId::new("owner");
        let (path, dir) = incarnation_paths();

        // Step 1: owner's first-ever boot. The catalog is reachable and
        // hands back incarnation 1 (the sequence starts at 1, since 0 is
        // never a valid draw — `Incarnation`'s own doc comment).
        let owner_storage = FakeStorage::default();
        let registry = FakeRegistry::starting_at(1);
        let first_boot = fence_boot(&owner_storage, &registry, &owner, path.clone(), dir.clone())
            .await
            .expect("owner's first boot succeeds");
        let BootOutcome::Active {
            incarnation: original_incarnation,
        } = first_boot
        else {
            panic!("expected Active on a reachable-catalog first boot, got {first_boot:?}");
        };
        assert_eq!(original_incarnation.0, 1);

        // replica's real fencing bookkeeping — the SAME shared guard
        // Forward/PeerApply/Receipt all use (fencing.rs's own module docs).
        let mut replica_fence_table = FenceTable::new();
        let original_identity = FenceIdentity {
            node: owner.clone(),
            incarnation: original_incarnation,
        };
        assert_eq!(
            replica_fence_table.admit(&original_identity),
            FenceOutcome::Admitted,
            "owner's genuine first message must be admitted"
        );

        // Step 2 + 3: owner is SIGSTOPped (module docs: nothing changes in
        // its state — a pause is not a crash) and outlasts the heartbeat
        // TTL, so the cluster starts a REPLACEMENT process for the SAME
        // logical node identity, reading the SAME persisted-incarnation
        // storage `owner`'s original process already wrote to in step 1
        // (`owner_storage`, not a fresh one) and completing FenceBoot with
        // the catalog's next value.
        let promotion_boot =
            fence_boot(&owner_storage, &registry, &owner, path.clone(), dir.clone())
                .await
                .expect("the replacement's promotion boot succeeds");
        let BootOutcome::Active {
            incarnation: promoted_incarnation,
        } = promotion_boot
        else {
            panic!("expected Active on the replacement's boot, got {promotion_boot:?}");
        };
        assert!(
            promoted_incarnation.0 > original_incarnation.0,
            "the replacement's incarnation must be strictly higher than the paused original's"
        );

        let promoted_identity = FenceIdentity {
            node: owner.clone(),
            incarnation: promoted_incarnation,
        };
        assert_eq!(
            replica_fence_table.admit(&promoted_identity),
            FenceOutcome::Admitted,
            "the replacement's genuine message under its fresh incarnation must be admitted"
        );

        // Step 4 + 5: owner's ORIGINAL, merely-paused process now resumes
        // (SIGCONT) and — never having rebooted — still presents its
        // stale, pre-pause incarnation. `replica`'s FenceTable must reject
        // it: this is FencedZombie's exact yardstick, and this test's
        // entire point.
        let stale_attempt = replica_fence_table.admit(&original_identity);
        assert_eq!(
            stale_attempt,
            FenceOutcome::Zombie {
                highest_seen: promoted_incarnation
            },
            "the resumed node's stale incarnation must be rejected as a zombie, not admitted"
        );
    });
}

/// The inverse sanity check: WITHOUT a replacement ever having promoted
/// (i.e. the pause never outlasted anything, or the cluster never
/// mistakenly promoted a replacement), the SAME resumed message under the
/// SAME never-changed incarnation is legitimately re-admitted — a pause
/// that resolves before anyone acts on it must never itself be mistaken for
/// a zombie. Guards against a fix for the test above that over-corrects
/// into rejecting every repeat message, not only genuinely stale ones
/// (`fencing.rs`'s own `repeat_of_the_same_incarnation_is_admitted` unit
/// test covers this in isolation; this is the same property confirmed
/// through this file's own end-to-end scenario setup).
#[test]
fn a_resumed_pause_with_no_promotion_in_the_meantime_is_not_a_zombie() {
    block_on(async {
        let owner = NodeId::new("owner");
        let (path, dir) = incarnation_paths();
        let owner_storage = FakeStorage::default();
        let registry = FakeRegistry::starting_at(1);

        let boot = fence_boot(&owner_storage, &registry, &owner, path, dir)
            .await
            .expect("owner boots");
        let BootOutcome::Active { incarnation } = boot else {
            panic!("expected Active, got {boot:?}");
        };

        let mut replica_fence_table = FenceTable::new();
        let identity = FenceIdentity {
            node: owner,
            incarnation,
        };
        assert_eq!(replica_fence_table.admit(&identity), FenceOutcome::Admitted);
        // owner pauses and resumes with NO replacement ever having
        // promoted in between — its resumed message carries the exact same
        // incarnation as before.
        assert_eq!(
            replica_fence_table.admit(&identity),
            FenceOutcome::Admitted,
            "a resumed pause with no intervening promotion is a legitimate repeat, not a zombie"
        );
    });
}
