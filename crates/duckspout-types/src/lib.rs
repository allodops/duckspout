//! Domain types and port traits for `DuckSpout`.
//!
//! This crate is the workspace root of the dependency graph: every protocol
//! crate depends on it and it depends on no workspace crate (§10.1, ADR-0008).
//! It owns:
//!
//! - dataset / tenant / partition / window / part identifier newtypes ([`ids`]);
//! - the dataset-declaration types (§2, §9.6.2) ([`dataset`]);
//! - the frozen window/part manifest, including `dedup_removed` (§2.4, §6.2,
//!   §6.8; frozen per §12.2) ([`manifest`]);
//! - watermark row types (§4.2.4, §6.8) ([`watermark`]);
//! - the closed node-status vocabulary (§4.5, §9.3.2) ([`status`]);
//! - the OTLP error table (§4.1, §4.5, §4.6) ([`otlp`]);
//! - the trace-event vocabulary with NDJSON serde (§3.3, §3.7; SEED
//!   Appendix B) ([`trace`]);
//! - every cross-crate port trait (ADR-0008): [`ports::Clock`],
//!   [`ports::Scheduler`], [`ports::Transport`], [`ports::Storage`],
//!   [`ports::AcceptAdapter`], [`ports::LakeCommitter`].
//!
//! No I/O anywhere: this crate's dependencies are `serde`, `serde_json`,
//! thiserror, and bytes — nothing that can open a socket or a file.
//!
//! Design home: `docs/design/data-model.md` (lands at absorption; until then
//! see `DUCKSPOUT.md` §2 and §10.1).

#![forbid(unsafe_code)]

pub mod dataset;
pub mod ids;
pub mod manifest;
pub mod otlp;
pub mod ports;
pub mod status;
pub mod trace;
pub mod watermark;

pub use dataset::{DatasetDeclaration, DatasetKind};
pub use ids::{DatasetId, NodeId, PartName, PartitionId, TenantId, WindowId};
pub use manifest::{OriginSeqRange, PartKind, WindowManifest};
pub use otlp::{GrpcCode, OtlpErrorClass};
pub use ports::{
    AcceptAdapter, AcceptError, AttachInfo, BoxFuture, Clock, ColumnSpec, CommitOutcome,
    DecodedBatch, LakeCommitter, LakeError, Scheduler, SchemaEvolution, Storage, StorageError,
    StoragePath, Transport, TransportError, WireError, WireRequest,
};
pub use status::{NodeStatus, OverloadStatus};
pub use trace::{EnvironmentEvent, TraceEvent, TraceRecord};
pub use watermark::{AppliedWatermarkRow, DimensionWatermarkRow, WatermarkRow};
