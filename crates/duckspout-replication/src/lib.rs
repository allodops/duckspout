//! Replication (§5): HRW ownership placement, ownership routing, `Forward`
//! → `PeerApply` → `Receipt`, total-inclusive RF `ClientAck` gating, and
//! incarnation fencing.
//!
//! The HRW placement function is **implemented** here (ADR-0004: in-house,
//! ~50 pure lines; the `hrw-hash` crate lost on adoption/maintenance). §8.5
//! property-tests HRW's minimal-disruption law exactly from v0.1 on, and it
//! is cross-checked against the TLA+ placement function once
//! `Replication.tla` lands (ADR-0004). Resolving HRW's output into a real
//! routing decision — a partition's owner, its RF replica set, and whether
//! the local node is that owner (§5.2's placement, §5.3's routing) — is
//! [`routing`] (issue #52, `HRW ring integration + ownership routing`);
//! `duckspout-daemon/src/wiring.rs` builds the [`routing::MembershipView`]
//! this module resolves against, from `cluster.seed_peers` (module docs of
//! [`routing`] explain why that stays the only membership source until
//! issue #53's registry lands).
//!
//! `Forward` → `PeerApply` → `Receipt` and total-inclusive RF `ClientAck`
//! gating (issue #51, §4, §5.1, §5.4) are implemented across
//! [`fencing`], [`wire`], [`forward`], [`peer_apply`], and [`receipt`].
//! Incarnation fencing ([`fencing::FenceTable`]) is the one guard all three
//! protocol steps share (§5.7) — scoped there to exactly the
//! comparison-and-reject primitive `Forward`/`PeerApply`/`Receipt` need on
//! the RECEIVING side. `FenceBoot`'s own boot-time incarnation draw and
//! `DegradedBoot`'s catalog-outage boot split (issue #53, §5.7) — the
//! BOOTING node's own side — are [`boot`]; `ClaimAdvertise`'s registry-row
//! idempotency guard (§5.5, issue #53) is [`claims`].
//!
//! `TakeoverDrain`'s ownership-transition half (§5.6 steps 2 and 4, issue
//! #54) is [`takeover`]: resolving the new owner of a dead node's partition
//! (reusing [`routing::route_write`] over a membership view with the dead
//! node excluded -- not new ring-walk logic, `takeover`'s own module docs),
//! the per-`(partition, dead_owner)` idempotency guard
//! ([`takeover::TakeoverTracker`], mirroring [`claims::ClaimTracker`]), and
//! the churn-boundary split's pure coverage arithmetic
//! ([`takeover::compute_residue`]). Actually invoking the drain side crosses
//! into `duckspout-drain`'s territory (a banned protocol×protocol edge,
//! ADR-0008) -- [`duckspout_types::TakeoverDrainTrigger`] is the port a
//! caller uses to hand this module's decision across that boundary,
//! mirroring [`duckspout_types::SealSurface`]'s own precedent; a concrete
//! implementation is deferred, matching how [`duckspout_types::ReplicaLog`]'s
//! concrete backend was deferred past #51 (issue #193's precedent) -- see
//! `takeover`'s module docs and this crate's own PR description for the
//! named follow-up.
//!
//! `DeclareLoss` (§5.8) is `duckspout-watermark`'s scope
//! (`duckspout_watermark::loss::check_declare_loss`) plus
//! [`duckspout_types::LossLedgerCommitter`], not this crate's -- the
//! ceremony's core guard needs no replication-specific logic beyond the
//! live-coverage snapshot a caller assembles from this crate's own
//! [`routing::MembershipView`]/registry state.
//!
//! Layering (§10.1, ADR-0008): depends on `duckspout-types` only among
//! workspace crates; the runtime is reached exclusively through the
//! types-defined ports (D-2) — [`duckspout_types::Transport`] for the wire,
//! [`duckspout_types::ReplicaLog`] for a peer's durable apply (a new port,
//! defined in `duckspout-types` per ADR-0008 exactly as
//! `duckspout_types::SealSurface` crosses the drain↔staging boundary; a
//! concrete `duckspout-staging` implementation and daemon wiring are
//! tracked as follow-up work in issue #193, not part of this crate),
//! [`duckspout_types::Registry`] for the catalog ([`boot`]/[`claims`]'s own
//! module docs — a concrete implementation and daemon-composition wiring,
//! including replacing `duckspout-daemon::system::V01_FIXED_INCARNATION`
//! with a real `FenceBoot` draw, are likewise deferred to a follow-up
//! issue, not part of this crate).
//!
//! Design home: `docs/design/replication.md` (absorbed from `DUCKSPOUT.md`
//! §5).

#![forbid(unsafe_code)]

pub mod boot;
pub mod claims;
pub mod fencing;
pub mod forward;
pub mod hrw;
pub mod peer_apply;
pub mod receipt;
pub mod routing;
pub mod takeover;
pub mod wire;

pub use boot::{BootError, BootOutcome, fence_boot};
pub use claims::ClaimTracker;
pub use fencing::{FenceIdentity, FenceOutcome, FenceTable, Incarnation};
pub use forward::{ForwardBatch, forward_to_peers};
pub use hrw::{hrw_owner, hrw_ranked};
pub use peer_apply::{PeerApplyOutcome, apply_forward};
pub use receipt::{ReceiptOutcome, ReceiptTracker, client_ack_ready};
pub use routing::{MembershipView, RoutingPlan, route_write};
pub use takeover::{TakeoverDecision, TakeoverTracker, compute_residue, resolve_takeover};
pub use wire::{Envelope, ForwardMessage, ReceiptMessage, WireDecodeError};
