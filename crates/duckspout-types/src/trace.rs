//! The trace-event vocabulary (§3.3 actions under §3.7's journaling rules;
//! SEED D-6 and Appendix B).
//!
//! One flushed NDJSON line per event, with per-node sequence numbers. Every
//! variant pairs with a tracepoint row in `docs/trace-mapping.md`, validated
//! by the invariant engine.
//!
//! Journaling rules transcribed from §3.7, §6.4, §3.3:
//!
//! - a commit journals its **outcome** name — [`TraceEvent::LakeCommitOk`] /
//!   [`TraceEvent::LakeCommitAbort`] / [`TraceEvent::LakeCommitIndeterminate`],
//!   with the following [`TraceEvent::Reconcile`] naming the Indeterminate
//!   resolution; there is no bare `LakeCommit` event;
//! - `WatermarkAdvance` is **not** a separate event — it rides the `LakeCommit`
//!   outcome atomically (§6.4), so it has no variant;
//! - `RecoverNode` is defined as `FenceBoot` (§3.3) — recovery journals as
//!   [`TraceEvent::FenceBoot`], no separate variant;
//! - [`TraceEvent::ClientTimeout`] is journaled **only by
//!   `duckspout-loadgen`** (a fleet member, §8.4), never by a node;
//! - `CrashNode` and `CrashWipe` are **environment events, never journaled** —
//!   they live in the separate [`EnvironmentEvent`] type used only by the
//!   CTK's schedule stream, so a node emitting one is a type error, not a
//!   convention.

use serde::{Deserialize, Serialize};

use crate::ids::NodeId;

/// The journaled action vocabulary: SEED Appendix B's 27 node-journaled
/// variants plus [`TraceEvent::ClientTimeout`].
///
/// Variants are payload-free at bootstrap; per-variant payloads land with the
/// implementations that journal them (their NDJSON shape stays
/// one-object-per-line).
// trace-enum-begin
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TraceEvent {
    /// Adapter decoded and admitted a batch (§4.3, §4.6).
    Accept,
    /// Dedup-window lookup on `(tenant, hash-or-token)` (§4.4.1).
    DedupCheck,
    /// One hot-DuckDB transaction: rows + dedup entry + applied-watermark
    /// row, fsync on commit (§4.2, §4.3).
    StageCommit,
    /// Success returned to the client after local commit plus RF−1 receipts
    /// (`DurableAck`, §4.3).
    ClientAck,
    /// Overload-ladder rung 2: UNAVAILABLE + `RetryInfo` (§4.5).
    Throttle,
    /// Overload-ladder rung 3: new writes / new-range replication refused
    /// (§4.5).
    Refuse,
    /// Ship an `(origin, seq)` range to RF−1 ring peers (§5).
    Forward,
    /// A replica durably applied a forwarded range (§4.2.4, §5).
    PeerApply,
    /// A peer's durable-apply acknowledgment (§4.3, §5).
    Receipt,
    /// One sorted `COPY … TO` over a window's staging tables; records
    /// `dedup_removed` in the manifest (§6.2).
    SealPart,
    /// A sealed part PUT to the object store (§6).
    PutPart,
    /// Lake commit outcome: Committed (§6.5).
    LakeCommitOk,
    /// Lake commit outcome: Aborted — definitive rejection, nothing changed
    /// (§6.5).
    LakeCommitAbort,
    /// Lake commit outcome: Indeterminate — connection dropped mid-COMMIT
    /// (§6.5).
    LakeCommitIndeterminate,
    /// The single read-back resolving an Indeterminate outcome before any
    /// retry (§6.5).
    Reconcile,
    /// Whole-file retention DELETE of named parts (§6.7).
    Expire,
    /// A drained window demoted in place to the cache class — only when
    /// `dedup_removed = 0` (§2.4, §6.9).
    Demote,
    /// A cache-class table dropped for space; always safe (§2.4, §4.5 rung 0).
    Evict,
    /// A drained window dropped from hot at drain commit (§6.9).
    DropWindow,
    /// A changelog snapshot part sealed at rollover (§6.7).
    SnapshotSeal,
    /// A node advertising its claims to the registry (§5).
    ClaimAdvertise,
    /// Liveness heartbeat (5 s cadence, 15 s TTL — §9.6.3).
    Heartbeat,
    /// Boot with fencing: persisted incarnation resumes, zombies rejected
    /// (§5). Recovery journals as this — `RecoverNode` is defined as
    /// `FenceBoot` (§3.3).
    FenceBoot,
    /// Boot without recovered local state, disclosed as degraded (§5).
    DegradedBoot,
    /// A takeover node draining a dead peer's replicated ranges (§5.6).
    TakeoverDrain,
    /// The `DeclareLoss` ceremony — the one sanctioned watermark weakening
    /// (§5.8, §9).
    DeclareLoss,
    /// A monotone, lossless schema change applied (§2, §6.4).
    EvolveSchema,
    /// A client-observed timeout. Journaled **only by `duckspout-loadgen`**
    /// (§3.7, §8.4) — never by a node.
    ClientTimeout,
}

/// Environment events — injected by the CTK's schedule stream, **never
/// journaled by a node** (§3.7). A separate type so the compiler, not a
/// convention, enforces that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EnvironmentEvent {
    /// Kill a node process; durable state survives.
    CrashNode,
    /// Kill a node and wipe its durable state.
    CrashWipe,
}
// trace-enum-end

/// One journaled trace line: the emitting node, its per-node sequence number,
/// and the event (SEED D-6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRecord {
    /// The journaling node (for [`TraceEvent::ClientTimeout`], the loadgen
    /// fleet member's id).
    pub node: NodeId,
    /// Per-node sequence number: dense, starting at 0, one per journaled
    /// event.
    pub seq: u64,
    /// The journaled event.
    pub event: TraceEvent,
}

impl TraceRecord {
    /// Encodes this record as one NDJSON line: a single JSON object with no
    /// embedded newline, **without** a trailing `\n` (the writer appends and
    /// flushes it — §3.7: one flushed line per event).
    ///
    /// # Errors
    ///
    /// Returns any [`serde_json`] serialization error.
    pub fn to_ndjson_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Decodes one NDJSON line.
    ///
    /// # Errors
    ///
    /// Returns any [`serde_json`] deserialization error.
    pub fn from_ndjson_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }
}

/// The §3.7 capture seam: a tracepoint hands each journaled event to a
/// sink; the sink owns node identity, the dense per-node sequence, and the
/// one-flushed-NDJSON-line-per-event discipline (D-6).
///
/// The trait lives HERE because the emitters are protocol crates, which
/// depend on `duckspout-types` only (ADR-0008); the concrete NDJSON writer
/// is I/O and lives with the harness (`duckspout-ctk`), keeping this crate
/// I/O-free by charter. Tracepoints hold `Option<Arc<dyn TraceSink>>` —
/// `None` journals nothing, which is the production default until the
/// `conformance` ledger row arms (issue #44).
pub trait TraceSink: Send + Sync {
    /// Journals one event. Implementations must write and flush one NDJSON
    /// line before returning (§3.7) — or fail loud: a tracepoint that
    /// silently drops events certifies runs that never happened as
    /// recorded (R-3).
    fn record(&self, event: TraceEvent);
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Every [`TraceEvent`] variant, exactly once — the §3.3 vocabulary
    /// (SEED Appendix B: 27 node-journaled + `ClientTimeout`). Kept beside
    /// the enum; the invariant engine's `trace-mapping` rule pairs the enum
    /// with `docs/trace-mapping.md`, and the vocabulary test below pairs the
    /// serialized token with the variant name.
    const ALL_EVENTS: [TraceEvent; 28] = [
        TraceEvent::Accept,
        TraceEvent::DedupCheck,
        TraceEvent::StageCommit,
        TraceEvent::ClientAck,
        TraceEvent::Throttle,
        TraceEvent::Refuse,
        TraceEvent::Forward,
        TraceEvent::PeerApply,
        TraceEvent::Receipt,
        TraceEvent::SealPart,
        TraceEvent::PutPart,
        TraceEvent::LakeCommitOk,
        TraceEvent::LakeCommitAbort,
        TraceEvent::LakeCommitIndeterminate,
        TraceEvent::Reconcile,
        TraceEvent::Expire,
        TraceEvent::Demote,
        TraceEvent::Evict,
        TraceEvent::DropWindow,
        TraceEvent::SnapshotSeal,
        TraceEvent::ClaimAdvertise,
        TraceEvent::Heartbeat,
        TraceEvent::FenceBoot,
        TraceEvent::DegradedBoot,
        TraceEvent::TakeoverDrain,
        TraceEvent::DeclareLoss,
        TraceEvent::EvolveSchema,
        TraceEvent::ClientTimeout,
    ];

    #[test]
    fn every_event_serializes_as_its_verbatim_action_name() {
        // §3.7: event names are the §3.3 action names, VERBATIM — a
        // `#[serde(rename…)]` attribute or a renamed variant would silently
        // break every recorded trace's refinement pairing; this catches it
        // for all 28 variants (and both environment events) at once.
        for event in ALL_EVENTS {
            assert_eq!(
                serde_json::to_value(event).expect("serialize"),
                serde_json::Value::String(format!("{event:?}")),
                "the serialized token must be the variant name"
            );
        }
        for event in [EnvironmentEvent::CrashNode, EnvironmentEvent::CrashWipe] {
            assert_eq!(
                serde_json::to_value(event).expect("serialize"),
                serde_json::Value::String(format!("{event:?}"))
            );
        }
    }

    proptest! {
        /// §8.5's serialization-stability law for the trace vocabulary
        /// (D-6): for ANY node id — including quotes, newlines, and
        /// non-ASCII — any seq, and any event, the NDJSON line is one line
        /// and decodes back losslessly. Would catch a hand-rolled encoder
        /// (or a future "faster" one) that fails to escape an embedded
        /// newline or quote: an unescaped `\n` in a node id would split one
        /// journaled event into two corrupt lines and break every
        /// downstream trace check.
        #[test]
        fn ndjson_round_trips_any_record(
            node in ".{0,40}",
            seq in any::<u64>(),
            index in 0usize..ALL_EVENTS.len(),
        ) {
            let record = TraceRecord {
                node: NodeId::new(node),
                seq,
                event: ALL_EVENTS[index],
            };
            let line = record.to_ndjson_line().expect("serialize");
            prop_assert!(!line.contains('\n'), "NDJSON: one event, one line");
            prop_assert_eq!(TraceRecord::from_ndjson_line(&line).expect("deserialize"), record);
        }
    }

    #[test]
    fn ndjson_round_trip_is_lossless_and_single_line() {
        let record = TraceRecord {
            node: NodeId::new("node-1"),
            seq: 42,
            event: TraceEvent::LakeCommitIndeterminate,
        };
        let line = record.to_ndjson_line().expect("serialize");
        assert!(!line.contains('\n'), "an NDJSON line must be one line");
        let back = TraceRecord::from_ndjson_line(&line).expect("deserialize");
        assert_eq!(back, record);
    }

    #[test]
    fn event_serializes_as_its_vocabulary_name() {
        let record = TraceRecord {
            node: NodeId::new("n"),
            seq: 0,
            event: TraceEvent::StageCommit,
        };
        let line = record.to_ndjson_line().expect("serialize");
        assert_eq!(line, r#"{"node":"n","seq":0,"event":"StageCommit"}"#);
    }
}
