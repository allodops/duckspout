//! Drain choreography (§6): `SealPart` → `PutPart` → `LakeCommit`, the
//! §6.3 lateness hold, deterministic part naming, and the three-outcome
//! commit discipline with the `SingleDrainCommit` fence sequenced above the
//! port (ADR-0010).
//!
//! Layering (§10.1, ADR-0008): this crate sees the lake exclusively through
//! the `LakeCommitter` contract (`duckspout-lake-contract`), staging
//! through the [`duckspout_types::SealSurface`] port, and watermark
//! bookkeeping through the [`duckspout_types::WatermarkBookkeeping`] port —
//! it depends on no protocol sibling and no concrete backend. `PutPart`
//! goes through [`object_store`] (§10.2: one PUT against every major
//! store); time comes from the `Clock` port (D-2).
//!
//! The module map:
//!
//! - [`schedule`] — which closed micro-windows are drain-eligible (§6.3).
//! - [`naming`] — the deterministic part name (§6.5, §2.7).
//! - [`coordinator`] — the choreography itself, including the retry
//!   discipline (evidence over blind replay, R-2) and the §6.5
//!   one-read-back resolution of unsettled commits.
//!
//! What this crate deliberately does **not** do at v0.1, and why:
//!
//! - **Multi-window part packing and the §6.2 size/age sizing band**
//!   (`drain.part_target_bytes`, `drain.max_age`): v0.1 seals one part per
//!   micro-window, which is correct under every invariant — packing is a
//!   PUT-cost optimization layered on the same choreography, and arrives
//!   with the retention/sizing work (§6.2, §6.7).
//! - **Supplement and snapshot commits** (§6.6–§6.7): the takeover-drain
//!   and snapshot-rollover flows that produce them are replication
//!   (`TakeoverDrain`) and retention work; the naming and fence vocabulary
//!   for both already exist here ([`naming::PartDiscriminator`]).
//! - **The cross-process racing-drains proof**: the fence *mechanism* below
//!   the port is backend work — `DuckLake`'s snapshot-commit conflict,
//!   implemented and proven by issue #36 per ADR-0010. This crate proves
//!   the choreography half: two racing attempts through the port contract
//!   yield exactly one commit, the loser resolving via read-back.
//!
//! Two TLC findings from the formal specs (PR #137) are load-bearing here:
//!
//! - **TN-32**: `DropWindow` is coverage-guarded — only rows the durable
//!   commit's coverage (or the loss ledger) accounts for may leave staging;
//!   uncovered residue (a late arrival landing between the seal `COPY` and
//!   the drop) is kept for the supplement path. The guard is the
//!   `SealSurface::drop_window` contract; this crate always passes the
//!   commit's (or the bookkeeping's recorded) coverage.
//! - **TN-36**: the "does this window's part already stand" fence must span
//!   the lake **including expired parts** — a fence over live entries only
//!   would re-admit a duplicate after retention expires the original. At
//!   this crate's level the evidence used never weakens with expiry (the
//!   watermark is monotone and the bookkeeping's dense-next cursor never
//!   regresses); the backend-level `UNIQUE` fence must uphold the same
//!   (issue #36's definition of done).
//!
//! Design home: `docs/design/drain.md`.

#![forbid(unsafe_code)]

pub mod coordinator;
pub mod naming;
pub mod schedule;

pub use coordinator::{
    DatasetDrainPlan, DrainCoordinator, DrainError, DrainOutcome, RequeueReason,
};
pub use naming::{PartDiscriminator, part_name};
pub use schedule::{DrainConfig, eligible, hold_elapsed};
