//! Daemon wiring. Ⓢ v0.1 — the composition below is declared so the crate
//! graph is honest at bootstrap (the daemon, and only the daemon plus the
//! bins, may depend on concrete implementations — §10.1); the construction
//! itself lands with the engine.

// Justification for the allow: these aliases declare the v0.1 composition
// shape; nothing constructs them until the wiring lands, and deleting them
// would un-declare the crate graph this binary exists to close (§10.1).
#![allow(dead_code, unused_imports)]

// The concrete composition the daemon will construct at v0.1 (§10.4). The
// aliases pin the intended shape without pretending any of it runs today.

/// The accept seam the daemon registers adapters into (§4.1.2).
pub type Adapters = duckspout_accept::AdapterRegistry;

/// The staging engine, generic over the production storage port.
pub type Staging<S> = duckspout_staging::StagingEngine<S>;

/// The `StageCommitter` port the accept service commits through (§4.3): the
/// engine behind partition/window assignment, generic over the storage and
/// clock ports. Composed end-to-end (real gRPC → port → engine) in
/// `tests/otlp_e2e.rs`; the daemon's own construction is the listener
/// wiring (v0.1).
pub type Stager<S, C> = duckspout_staging::EngineStager<S, C>;

/// The OTLP logs service the daemon serves on `node.otlp_listen` (§4.1).
pub type OtlpLogs<S, C> = duckspout_accept::OtlpLogsService<Stager<S, C>>;

/// The lake backend v1 wires by default (`lake.committer = "ducklake"`).
pub type Committer = duckspout_lake_ducklake::DuckLakeCommitter;

/// The drain driver over that backend, through the contract only.
pub type Drain = duckspout_drain::DrainCoordinator<Committer>;

/// The watermark view the daemon serves reads from (§7).
pub type Watermarks = duckspout_watermark::WatermarkLedger;

/// The self-certification report backends run against the contract (§10.3).
pub type Conformance = duckspout_lake_contract::conformance::ConformanceReport;

/// The disclosed node status — one type, three transports (§9.3.2).
pub type Status = duckspout_types::NodeStatus;

/// Placement: any node accepts, then forwards to the HRW owner (§5).
pub use duckspout_replication::hrw_owner;

/// Bootstrap status line.
pub const STATUS: &str = "wiring lands at v0.1 (§10.4); config surface and manifest are complete";
