//! `FenceBoot` / `DegradedBoot` (§5.7): the node's own boot-time incarnation
//! ceremony — the BOOTING node's side of incarnation fencing, distinct from
//! [`crate::fencing::FenceTable`]'s RECEIVING side (that module's own docs).
//!
//! Every process boot executes `FenceBoot`: read whatever incarnation this
//! node last persisted locally (if any), then try to draw a fresh one from
//! the catalog's shared monotonic sequence
//! ([`duckspout_types::Registry::next_incarnation`]). Three outcomes, one
//! function ([`fence_boot`]), matching §7's own three-way split and
//! `p/Replication/Node.p`'s checker-validated `eFenceBoot` handler (the most
//! concrete operational reference this module follows — see that file's own
//! comments for the exact edge cases each branch below was built against):
//!
//! - **The catalog answers.** [`BootOutcome::Active`] — a fresh, catalog-drawn
//!   incarnation, strictly greater than every incarnation any node has ever
//!   drawn (not merely greater than this node's own prior value — the
//!   catalog sequence is shared, [`duckspout_types::Registry`]'s own module
//!   docs). Persisted locally before this function returns, so a later
//!   catalog outage still has *something* to fall back to.
//! - **The catalog is unreachable, but a persisted incarnation exists.**
//!   [`BootOutcome::Degraded`] — `DegradedBoot` (§7): this node keeps
//!   applying and receipting replication under its existing incarnation but
//!   takes no ownership action until it promotes. Nothing is persisted here
//!   (nothing changed); a later call to [`fence_boot`] — the daemon's own
//!   promotion retry once the catalog is observed reachable again — is
//!   `FenceBoot` "complet\[ing\]" (§7's own words) and will draw a genuinely
//!   fresh incarnation, exactly the same [`BootOutcome::Active`] path a
//!   never-degraded boot takes.
//! - **The catalog is unreachable AND nothing is persisted.**
//!   [`BootOutcome::Waiting`] — a genuinely new node "has no identity to be
//!   safely partial with" (§7) and cannot complete `FenceBoot` at all yet;
//!   `p/Replication/Node.p`'s `Waiting` state is the operational reference
//!   (every inbound message dropped outright until promotion — this module
//!   does not itself enforce that; see the module docs below for the
//!   boundary).
//!
//! # What this module does NOT do
//!
//! - **Enforce `NoOwnershipWhileDegraded` / `NoIdentityWhileWaiting`
//!   (`p/Replication/Spec.p`).** [`fence_boot`] only ever *reports*
//!   [`BootOutcome::Degraded`] / [`BootOutcome::Waiting`] — refusing
//!   ownership actions (claim advertisement, takeover, drain) while degraded
//!   or waiting is the CALLER's obligation (the daemon composition root and,
//!   eventually, `duckspout-daemon`'s takeover logic, issue #54's scope).
//!   `p/Replication/Node.p`'s `sweepOrphanedKeys`'s own `if (degraded)
//!   return;` guard is the exact shape a caller here must replicate.
//! - **Drive a retry loop.** A degraded or waiting node's promotion is a
//!   future call to [`fence_boot`], triggered by whatever the daemon uses to
//!   observe "the catalog is reachable again" (a periodic probe, a
//!   successful unrelated catalog call) — this module has no `Scheduler`
//!   concept of its own (§10.1: protocol crates reach time only through
//!   ports, and this function needs no timer to do its own job).
//! - **Wire a concrete [`Registry`].** No Postgres-backed implementation
//!   exists yet — this module's own tests use a hand-rolled fake, matching
//!   [`crate::peer_apply`]'s own convention (this crate cannot depend on
//!   `duckspout-ctk`, ADR-0008: no protocol-crate-depends-on-a-concrete-impl
//!   edge). Daemon-composition wiring (replacing
//!   `duckspout-daemon::system::V01_FIXED_INCARNATION` with a real
//!   `fence_boot` call) is deliberately deferred to a follow-up issue,
//!   matching how [`duckspout_types::ReplicaLog`]'s concrete implementation
//!   was deferred past #51 (issue #193's precedent).

use duckspout_types::{NodeId, Registry, RegistryError, Storage, StorageError, StoragePath};

use crate::fencing::Incarnation;

/// The three-way result of one [`fence_boot`] call (§7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootOutcome {
    /// `FenceBoot` completed: a fresh, catalog-drawn incarnation, durably
    /// persisted locally. Full ownership eligibility.
    Active {
        /// The freshly drawn incarnation.
        incarnation: Incarnation,
    },
    /// `DegradedBoot` (§7): the catalog is unreachable, but a locally
    /// persisted incarnation lets this node keep applying and receipting
    /// replication under it. No ownership actions until promotion (a later
    /// [`fence_boot`] call once the catalog is reachable again).
    Degraded {
        /// The persisted incarnation this node continues operating under.
        incarnation: Incarnation,
    },
    /// §7's typed startup state: no persisted incarnation AND the catalog
    /// is unreachable. This node has no identity yet — every inbound
    /// message a caller would otherwise route to it must be dropped, not
    /// merely deferred (`p/Replication/Node.p`'s `Waiting` state).
    Waiting,
}

/// A [`fence_boot`] failure. Every variant means the boot ceremony could not
/// determine an outcome at all — never confused with [`BootOutcome::Degraded`]
/// / [`BootOutcome::Waiting`], which are themselves *successful*, disclosed
/// outcomes of a struggling catalog (R-3: ambiguity fails closed, but a
/// clean "the catalog is down" answer is not ambiguous).
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// The locally persisted incarnation could not be read for a reason
    /// other than "nothing is there yet" (a torn write, a failed fsync
    /// read-back, or any other [`StorageError`]) — this is exactly the kind
    /// of ambiguity CONSTITUTION.md §11 says to fail closed on: a corrupt
    /// local incarnation file is never treated as "no persisted identity"
    /// (which would silently re-admit a node that may have written data
    /// under a real prior incarnation) nor guessed at.
    #[error("could not read the persisted incarnation: {0}")]
    PersistedIncarnationUnreadable(StorageError),
    /// The persisted incarnation file's content could not be parsed as a
    /// `u64`. Same fail-closed reasoning as
    /// [`BootError::PersistedIncarnationUnreadable`].
    #[error("persisted incarnation file is corrupt: {0}")]
    PersistedIncarnationCorrupt(String),
    /// A definitive [`RegistryError::Backend`] failure — NOT a catalog
    /// outage (that is [`BootOutcome::Degraded`] / [`BootOutcome::Waiting`],
    /// not an error at all). A caller must not treat this as "try degraded
    /// instead"; the registry itself said something is wrong, not merely
    /// unreachable.
    #[error("registry backend failure while drawing an incarnation: {0}")]
    RegistryBackend(RegistryError),
    /// The catalog draw succeeded, but persisting the fresh incarnation
    /// locally failed. Deliberately NOT collapsed into
    /// [`BootOutcome::Degraded`]: a fresh incarnation this node cannot
    /// durably remember is worse than useless (a crash immediately after
    /// would silently regress to the prior persisted value, defeating the
    /// whole point of drawing a fresh one at all) — this is a hard boot
    /// failure, not a fallback case.
    #[error("could not persist the freshly drawn incarnation: {0}")]
    PersistFailed(StorageError),
}

/// Runs one `FenceBoot` attempt (§7) for `self_node`, reading/writing its
/// persisted incarnation at `incarnation_path` (inside `incarnation_dir`,
/// fsynced separately per [`Storage`]'s own directory-durability contract —
/// matching `duckspout-staging::engine`'s own explicit-directory convention)
/// and drawing from `registry`. Also the ceremony a degraded or waiting
/// node's PROMOTION runs — module docs — so callers hold no separate
/// "promote" entry point; call this again once the catalog is believed
/// reachable.
///
/// # Errors
///
/// See [`BootError`]'s variants. A catalog outage is never an [`Err`] here —
/// see [`BootOutcome::Degraded`] / [`BootOutcome::Waiting`].
pub async fn fence_boot(
    storage: &dyn Storage,
    registry: &dyn Registry,
    self_node: &NodeId,
    incarnation_path: StoragePath,
    incarnation_dir: StoragePath,
) -> Result<BootOutcome, BootError> {
    let persisted = read_persisted_incarnation(storage, incarnation_path.clone()).await?;

    match registry.next_incarnation(self_node).await {
        Ok(fresh) => {
            persist_incarnation(storage, incarnation_path, incarnation_dir, fresh).await?;
            Ok(BootOutcome::Active {
                incarnation: Incarnation(fresh),
            })
        }
        Err(RegistryError::Unreachable(_)) => Ok(match persisted {
            Some(incarnation) => BootOutcome::Degraded {
                incarnation: Incarnation(incarnation),
            },
            None => BootOutcome::Waiting,
        }),
        Err(err @ RegistryError::Backend(_)) => Err(BootError::RegistryBackend(err)),
    }
}

/// Reads the locally persisted incarnation at `path`, `None` when nothing
/// has ever been written there (a genuinely new node) — never conflated
/// with a read failure (module docs, [`BootError::PersistedIncarnationUnreadable`]).
async fn read_persisted_incarnation(
    storage: &dyn Storage,
    path: StoragePath,
) -> Result<Option<u64>, BootError> {
    match storage.get(path).await {
        Ok(bytes) => {
            let text = std::str::from_utf8(&bytes).map_err(|err| {
                BootError::PersistedIncarnationCorrupt(format!("not valid UTF-8: {err}"))
            })?;
            let value: u64 = text
                .trim()
                .parse()
                .map_err(|err| BootError::PersistedIncarnationCorrupt(format!("{err}")))?;
            Ok(Some(value))
        }
        Err(StorageError::NotFound(_)) => Ok(None),
        Err(err) => Err(BootError::PersistedIncarnationUnreadable(err)),
    }
}

/// Durably persists `incarnation` at `path` — content fsync, then the
/// containing `dir`'s fsync, matching [`Storage::put`]'s own two-fsync
/// durability contract exactly.
async fn persist_incarnation(
    storage: &dyn Storage,
    path: StoragePath,
    dir: StoragePath,
    incarnation: u64,
) -> Result<(), BootError> {
    storage
        .put(path.clone(), bytes::Bytes::from(incarnation.to_string()))
        .await
        .map_err(BootError::PersistFailed)?;
    storage
        .fsync_file(path)
        .await
        .map_err(BootError::PersistFailed)?;
    storage
        .fsync_dir(dir)
        .await
        .map_err(BootError::PersistFailed)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use bytes::Bytes;
    use duckspout_types::BoxFuture;

    use super::*;

    /// The same dependency-free executor `peer_apply.rs`'s tests use (module
    /// docs there for why this crate cannot pull in a real runtime or
    /// `duckspout-ctk`).
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

    /// A hand-rolled, in-memory [`Storage`] double, scoped to exactly the
    /// two-fsync durability discipline this module's tests need — not a
    /// full storage engine. Modeled after `duckspout-ctk::InMemStorage`'s
    /// own content/name-durability distinction (this crate cannot depend on
    /// `duckspout-ctk`, ADR-0008).
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

    /// A hand-rolled [`Registry`] double whose reachability and next-draw
    /// value are test-controlled.
    #[derive(Default)]
    struct FakeRegistry {
        /// `None` means "unreachable"; `Some(n)` is the next value this
        /// draw returns (and then advances past, modeling a real
        /// monotonic sequence).
        next: Mutex<Option<u64>>,
    }

    impl FakeRegistry {
        fn reachable_at(next: u64) -> Self {
            Self {
                next: Mutex::new(Some(next)),
            }
        }

        fn unreachable() -> Self {
            Self {
                next: Mutex::new(None),
            }
        }
    }

    impl Registry for FakeRegistry {
        fn next_incarnation(&self, _node: &NodeId) -> BoxFuture<'_, Result<u64, RegistryError>> {
            let mut next = self.next.lock().expect("lock");
            let result = match *next {
                Some(value) => {
                    *next = Some(value + 1);
                    Ok(value)
                }
                None => Err(RegistryError::Unreachable("catalog down".to_string())),
            };
            Box::pin(std::future::ready(result))
        }

        fn advertise_claim(
            &self,
            _partition: &duckspout_types::PartitionId,
            _node: &NodeId,
            _role: duckspout_types::ClaimRole,
        ) -> BoxFuture<'_, Result<(), RegistryError>> {
            Box::pin(std::future::ready(Ok(())))
        }
    }

    fn paths() -> (StoragePath, StoragePath) {
        (
            StoragePath::new("boot/incarnation"),
            StoragePath::new("boot"),
        )
    }

    /// A genuinely new node (nothing persisted) whose catalog IS reachable
    /// completes `FenceBoot` and lands `Active`, with the drawn value
    /// persisted for a later boot to read back — `TestNewNodeBoot`'s
    /// positive-path shape (`p/Replication/TestDriver.p`), minus the
    /// replication machinery around it.
    #[test]
    fn a_new_node_with_a_reachable_catalog_boots_active_and_persists() {
        block_on(async {
            let storage = FakeStorage::default();
            let registry = FakeRegistry::reachable_at(7);
            let (path, dir) = paths();
            let node = NodeId::new("n1");

            let outcome = fence_boot(&storage, &registry, &node, path.clone(), dir)
                .await
                .expect("boot succeeds");
            assert_eq!(
                outcome,
                BootOutcome::Active {
                    incarnation: Incarnation(7)
                }
            );

            let persisted = read_persisted_incarnation(&storage, path)
                .await
                .expect("read back");
            assert_eq!(persisted, Some(7), "the fresh incarnation must be durable");
        });
    }

    /// §7's OTHER boot case: a genuinely new node whose catalog is
    /// UNREACHABLE has no identity to fall back to and lands `Waiting`, not
    /// `Degraded` — `TestNewNodeBoot`'s own premise
    /// (`p/Replication/TestDriver.p`: "priorIncarnation = 0 -- nothing
    /// persisted to fall back to").
    #[test]
    fn a_new_node_with_an_unreachable_catalog_waits() {
        block_on(async {
            let storage = FakeStorage::default();
            let registry = FakeRegistry::unreachable();
            let (path, dir) = paths();
            let node = NodeId::new("n1");

            let outcome = fence_boot(&storage, &registry, &node, path, dir)
                .await
                .expect("boot succeeds (as Waiting, not an Err)");
            assert_eq!(outcome, BootOutcome::Waiting);
        });
    }

    /// §7's `DegradedBoot`: a node with a persisted incarnation whose
    /// catalog is unreachable boots `Degraded` at its EXISTING incarnation,
    /// not `Waiting` and not a fresh draw — `TestDegradedBoot`'s own premise
    /// (`p/Replication/TestDriver.p`: "priorIncarnation = 1 > 0 plus the
    /// outage puts newOwner straight into degraded").
    #[test]
    fn a_rebooting_node_with_an_unreachable_catalog_degrades_at_its_prior_incarnation() {
        block_on(async {
            let storage = FakeStorage::default();
            let (path, dir) = paths();
            let node = NodeId::new("n1");

            // Simulate a prior successful boot's persisted incarnation.
            persist_incarnation(&storage, path.clone(), dir.clone(), 3)
                .await
                .expect("seed a prior incarnation");

            let registry = FakeRegistry::unreachable();
            let outcome = fence_boot(&storage, &registry, &node, path, dir)
                .await
                .expect("boot succeeds (as Degraded, not an Err)");
            assert_eq!(
                outcome,
                BootOutcome::Degraded {
                    incarnation: Incarnation(3)
                }
            );
        });
    }

    /// Promotion (§7: "promotes itself when the catalog returns and
    /// `FenceBoot` completes") is simply a LATER `fence_boot` call once the
    /// catalog answers again — this test drives exactly that sequence and
    /// confirms the promoted incarnation is strictly greater than the
    /// degraded one, satisfying `FencedZombie`'s own yardstick against any
    /// peer that already admitted a Forward under the degraded incarnation.
    #[test]
    fn promotion_after_a_degraded_boot_draws_a_strictly_higher_incarnation() {
        block_on(async {
            let storage = FakeStorage::default();
            let (path, dir) = paths();
            let node = NodeId::new("n1");
            persist_incarnation(&storage, path.clone(), dir.clone(), 3)
                .await
                .expect("seed a prior incarnation");

            let degraded_registry = FakeRegistry::unreachable();
            let degraded = fence_boot(
                &storage,
                &degraded_registry,
                &node,
                path.clone(),
                dir.clone(),
            )
            .await
            .expect("boots degraded");
            assert_eq!(
                degraded,
                BootOutcome::Degraded {
                    incarnation: Incarnation(3)
                }
            );

            // The catalog returns, offering the next value in its shared
            // sequence -- strictly greater than the degraded incarnation.
            let restored_registry = FakeRegistry::reachable_at(9);
            let promoted = fence_boot(&storage, &restored_registry, &node, path, dir)
                .await
                .expect("promotion succeeds");
            assert_eq!(
                promoted,
                BootOutcome::Active {
                    incarnation: Incarnation(9)
                }
            );
        });
    }

    /// Promotion out of `Waiting` (no prior identity at all) draws this
    /// node's very first incarnation once the catalog returns —
    /// `TestNewNodeBoot`'s own promotion shape (`p/Replication/TestDriver.p`:
    /// "newOwner completes its very first fence... and leaves `Waiting`").
    #[test]
    fn promotion_out_of_waiting_completes_the_first_ever_fence() {
        block_on(async {
            let storage = FakeStorage::default();
            let (path, dir) = paths();
            let node = NodeId::new("n3");

            let waiting_registry = FakeRegistry::unreachable();
            let waiting = fence_boot(
                &storage,
                &waiting_registry,
                &node,
                path.clone(),
                dir.clone(),
            )
            .await
            .expect("boots waiting");
            assert_eq!(waiting, BootOutcome::Waiting);

            let restored_registry = FakeRegistry::reachable_at(1);
            let promoted = fence_boot(&storage, &restored_registry, &node, path, dir)
                .await
                .expect("promotion succeeds");
            assert_eq!(
                promoted,
                BootOutcome::Active {
                    incarnation: Incarnation(1)
                }
            );
        });
    }

    /// A definitive registry backend failure is never conflated with an
    /// outage: `fence_boot` propagates it as an [`Err`], not as
    /// [`BootOutcome::Degraded`] / [`BootOutcome::Waiting`] — the exact
    /// distinction [`RegistryError::Backend`]'s own doc comment demands.
    /// Would catch a `fence_boot` that treats every `Err` from the registry
    /// as "try degraded instead."
    #[test]
    fn a_registry_backend_failure_is_never_treated_as_an_outage() {
        struct BackendFailingRegistry;
        impl Registry for BackendFailingRegistry {
            fn next_incarnation(
                &self,
                _node: &NodeId,
            ) -> BoxFuture<'_, Result<u64, RegistryError>> {
                Box::pin(std::future::ready(Err(RegistryError::Backend(
                    "corrupt sequence row".to_string(),
                ))))
            }

            fn advertise_claim(
                &self,
                _partition: &duckspout_types::PartitionId,
                _node: &NodeId,
                _role: duckspout_types::ClaimRole,
            ) -> BoxFuture<'_, Result<(), RegistryError>> {
                Box::pin(std::future::ready(Ok(())))
            }
        }

        block_on(async {
            let storage = FakeStorage::default();
            let (path, dir) = paths();
            let node = NodeId::new("n1");
            let outcome = fence_boot(&storage, &BackendFailingRegistry, &node, path, dir).await;
            assert!(
                matches!(outcome, Err(BootError::RegistryBackend(_))),
                "a backend failure must propagate as an Err, not a Degraded/Waiting outcome"
            );
        });
    }

    /// A corrupt persisted-incarnation file fails closed rather than being
    /// silently treated as "no persisted identity" — CONSTITUTION.md §11's
    /// frame applied to this module's own local state, not just the
    /// catalog's. Would catch a `read_persisted_incarnation` that swallows a
    /// parse error into `None`.
    #[test]
    fn a_corrupt_persisted_incarnation_fails_closed_not_silently_as_new() {
        block_on(async {
            let storage = FakeStorage::default();
            let (path, dir) = paths();
            storage
                .put(path.clone(), Bytes::from_static(b"not-a-number"))
                .await
                .expect("seed corrupt bytes");

            let registry = FakeRegistry::unreachable();
            let node = NodeId::new("n1");
            let outcome = fence_boot(&storage, &registry, &node, path, dir).await;
            assert!(
                matches!(outcome, Err(BootError::PersistedIncarnationCorrupt(_))),
                "a corrupt local file must fail closed, not resolve to Waiting"
            );
        });
    }
}
