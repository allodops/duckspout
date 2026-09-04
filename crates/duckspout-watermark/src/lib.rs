//! Watermark ledger logic (§2.4, §6.8, §7.3) over the row types owned by
//! `duckspout-types` ([`duckspout_types::WatermarkRow`],
//! [`duckspout_types::AppliedWatermarkRow`], [`duckspout_types::WindowManifest`]).
//!
//! Watermarks are the only registry state that matters (§6.8). This crate is
//! the lake-neutral bookkeeping **above** the `LakeCommitter` port
//! (ADR-0010: the lake stores the watermark; this crate computes what the
//! lake stores): per-`(dataset, partition)` `complete_through` tracking, the
//! advance computation `commit_files` carries (§6.4 — `WatermarkAdvance`
//! rides `LakeCommit` atomically), and reconstruction of the authoritative
//! state from the lake's manifest record (§6.8's
//! authoritative-but-reconstructible). Pure logic: no I/O, no clock, no
//! doubles needed to test it (D-2 is satisfied by construction).
//!
//! # The advance rule
//!
//! One rule serves the live path ([`WatermarkLedger::record_commit`] /
//! [`WatermarkLedger::record_loss`]) and recovery
//! ([`WatermarkLedger::reconstruct`]) — the formal `NewWatermark` definition
//! (specs/formal-core.md §3.3), projected onto the frozen manifest fields:
//! the watermark stands at the highest window of the dense committed prefix
//! after which every origin's committed seq coverage is gap-free from seq 1,
//! every hole excused only by a loss-ledger row (§5.8 — the one sanctioned
//! weakening). `complete_through` is the running maximum of
//! `event_time_max_ms` over the advanced windows — monotone by construction,
//! and honest about lateness: a post-watermark straggler is outside every
//! `complete` read's contract (§6.3).
//!
//! # Conventions (normative for every producer of these inputs)
//!
//! - **Window ids are dense per partition and 0-based**: a partition's first
//!   sealed window is `WindowId(0)` (§2.3 — contiguity must be decidable).
//! - **Per-`(partition, origin)` seqs are 1-based**: gap refusal admits
//!   exactly `applied_seq + 1` and `applied_seq = 0` means nothing applied
//!   (§4.2.4).
//! - **`complete_through_ms` is inclusive**: the §7.5 cold branch takes
//!   at-or-below, so a row *at* the watermark instant is lake-covered
//!   ([`WatermarkLedger::covers`]).
//!
//! # `DeclareLoss` (§5.8, issue #54)
//!
//! [`WatermarkLedger::record_loss`] is the bookkeeping half (recording a
//! [`LossLedgerRow`] and re-running the advance rule); [`check_declare_loss`]
//! is the ceremony's own guard (the literal `accept_data_loss: true` consent,
//! and refusal while any live replica still advertises coverage) — see
//! `crate::loss`'s module docs for the full shape and what stays deliberately
//! deferred (the durable atomic commit through
//! [`duckspout_types::LossLedgerCommitter`], and gathering the live-coverage
//! snapshot itself, both composition-root concerns this crate cannot reach,
//! ADR-0008).
//!
//! Layering (§10.1, ADR-0008): depends on `duckspout-types` only among
//! workspace crates. Persistence is the committer's transaction, never this
//! crate's I/O.
//!
//! Design home: `docs/design/drain.md` (§6.4, §6.8), `docs/design/query.md`
//! (§7.3), `docs/design/data-model.md` (§2.4), ADR-0010.

#![forbid(unsafe_code)]

mod coverage;
mod ledger;
mod loss;
mod port;
mod reconstruct;
#[cfg(test)]
mod testutil;

pub use ledger::{AdvanceError, WatermarkLedger, WindowRecord};
pub use loss::{DeclareLossRequest, LossLedgerRow, LossRefusal, LostRange, check_declare_loss};
pub use port::SharedLedger;
pub use reconstruct::{CoverageHole, ReconstructError, Reconstruction, Stall, StallReason};

/// [`duckspout_types::ReplicaCoverage`], re-exported for convenience next to
/// [`check_declare_loss`], which consumes it — see that function's own doc
/// comment.
pub use duckspout_types::ReplicaCoverage;

/// The port this crate implements for the drain (ADR-0008 home-crate
/// re-export): the drain computes nothing watermark-shaped itself — it
/// carries what this crate computes (ADR-0010).
pub use duckspout_types::{LedgerRejection, WatermarkBookkeeping};
