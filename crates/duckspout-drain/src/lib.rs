//! Drain choreography (§6): `SealPart` → `PutPart` → `LakeCommit`, late
//! arrivals, the `SingleDrainCommit` guard, retention.
//!
//! Layering (§10.1): this crate sees the lake exclusively through the
//! `LakeCommitter` contract (`duckspout-lake-contract`) — it never depends
//! on a concrete backend — and PUTs parts through [`object_store`] (§10.2:
//! `PutPart` against every major store). Its workspace dependencies are
//! `duckspout-types` and `duckspout-lake-contract`, nothing else.
//!
//! Ⓢ bootstrap stub — the choreography lands at v0.1.
//!
//! Design home: `docs/design/drain.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §6).

#![forbid(unsafe_code)]

use std::sync::Arc;

use duckspout_lake_contract::LakeCommitter;
use duckspout_types::{LakeError, PartitionId, WindowId};

/// The per-partition drain driver. Ⓢ v0.1: seals one sorted `COPY … TO` per
/// window (§6.2), PUTs the sealed parts, and commits
/// {parts + watermark} atomically through the contract (§6.4), guarded by
/// `SingleDrainCommit` (§6.6).
pub struct DrainCoordinator<C: LakeCommitter> {
    committer: C,
    parts_store: Arc<dyn object_store::ObjectStore>,
}

impl<C: LakeCommitter> DrainCoordinator<C> {
    /// Wires a committer and the cold object store. No I/O happens until the
    /// choreography lands (v0.1).
    pub fn new(committer: C, parts_store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            committer,
            parts_store,
        }
    }

    /// The lake contract this coordinator commits through.
    pub fn committer(&self) -> &C {
        &self.committer
    }

    /// The object store sealed parts are PUT to (§6, §10.2).
    #[must_use]
    pub fn parts_store(&self) -> &Arc<dyn object_store::ObjectStore> {
        &self.parts_store
    }

    /// Drains one window: `SealPart` → `PutPart` → `commit_files`, with
    /// Indeterminate resolved by exactly one read-back (§6.5). Ⓢ v0.1.
    ///
    /// # Errors
    ///
    /// Always [`LakeError::NotImplemented`] until the choreography lands.
    pub fn drain_window_stub(
        &self,
        _partition: &PartitionId,
        _window: WindowId,
    ) -> Result<(), LakeError> {
        Err(LakeError::NotImplemented(
            "drain choreography lands at v0.1 (§6)",
        ))
    }
}
