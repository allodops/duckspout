//! Replication (§5): HRW ownership placement, `Forward` → `PeerApply` →
//! `Receipt`, total-inclusive RF `ClientAck` gating, and incarnation
//! fencing.
//!
//! The HRW placement function is **implemented** here (ADR-0004: in-house,
//! ~50 pure lines; the `hrw-hash` crate lost on adoption/maintenance). §8.5
//! property-tests HRW's minimal-disruption law exactly from v0.1 on, and it
//! is cross-checked against the TLA+ placement function once
//! `Replication.tla` lands (ADR-0004). Wiring HRW's output into `Forward`'s
//! own peer selection (i.e. an actual live membership view) is issue #52's
//! scope (`HRW ring integration + ownership routing`), not this module's.
//!
//! `Forward` → `PeerApply` → `Receipt` and total-inclusive RF `ClientAck`
//! gating (issue #51, §4, §5.1, §5.4) are implemented across
//! [`fencing`], [`wire`], [`forward`], [`peer_apply`], and [`receipt`].
//! Incarnation fencing ([`fencing::FenceTable`]) is the one guard all three
//! protocol steps share (§5.7) — scoped here to exactly the
//! comparison-and-reject primitive `Forward`/`PeerApply`/`Receipt` need;
//! `FenceBoot`'s own boot-time incarnation draw, `DegradedBoot`'s
//! catalog-outage boot split, and `ClaimAdvertise`'s registry rows are
//! issue #53's separate scope (`Incarnation fencing + registry claims`).
//! `TakeoverDrain`/`DeclareLoss` (§5.6, §5.8) are issue #54's.
//!
//! Layering (§10.1, ADR-0008): depends on `duckspout-types` only among
//! workspace crates; the runtime is reached exclusively through the
//! types-defined ports (D-2) — [`duckspout_types::Transport`] for the wire,
//! [`duckspout_types::ReplicaLog`] for a peer's durable apply (a new port,
//! defined in `duckspout-types` per ADR-0008 exactly as
//! `duckspout_types::SealSurface` crosses the drain↔staging boundary; a
//! concrete `duckspout-staging` implementation and daemon wiring are
//! tracked as follow-up work in issue #193, not part of this crate).
//!
//! Design home: `docs/design/replication.md` (absorbed from `DUCKSPOUT.md`
//! §5).

#![forbid(unsafe_code)]

pub mod fencing;
pub mod forward;
pub mod hrw;
pub mod peer_apply;
pub mod receipt;
pub mod wire;

pub use fencing::{FenceIdentity, FenceOutcome, FenceTable, Incarnation};
pub use forward::{ForwardBatch, forward_to_peers};
pub use hrw::{hrw_owner, hrw_ranked};
pub use peer_apply::{PeerApplyOutcome, apply_forward};
pub use receipt::{ReceiptOutcome, ReceiptTracker, client_ack_ready};
pub use wire::{Envelope, ForwardMessage, ReceiptMessage, WireDecodeError};
