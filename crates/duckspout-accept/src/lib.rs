//! The accept surface (§4): the adapter-registration seam, the OTLP
//! adapter (v1's only adapter, §4.1.2), and the ack-sequence service over
//! the `StageCommitter` port (§4.3).
//!
//! The [`AcceptAdapter`] and `StageCommitter` ports are defined in
//! `duckspout-types` (ADR-0008); this crate owns everything beyond the bare
//! signatures — adapter registration ([`AdapterRegistry`]), the concrete
//! OTLP adapter ([`otlp`]), and the gRPC export service ([`server`]) that
//! acks only after the staging port committed. Durability semantics are
//! adapter-invariant: no adapter touches the ack path (§4.1.2).
//!
//! Design home: `docs/design/ingest.md` (§4.1, §4.3).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;

pub use duckspout_types::AcceptAdapter;

pub mod otlp;
pub mod server;

pub use otlp::OtlpGrpcAdapter;
pub use server::OtlpLogsService;

/// The adapter-registration seam: protocol name → adapter. Non-OTLP adapters
/// (post-v1, §4.1.2) plug in here without touching the ack path.
#[derive(Default)]
pub struct AdapterRegistry {
    adapters: HashMap<&'static str, Arc<dyn AcceptAdapter>>,
}

impl AdapterRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an adapter under its [`AcceptAdapter::protocol`] name,
    /// replacing any previous adapter with the same name.
    pub fn register(&mut self, adapter: Arc<dyn AcceptAdapter>) {
        self.adapters.insert(adapter.protocol(), adapter);
    }

    /// Looks up an adapter by protocol name.
    #[must_use]
    pub fn get(&self, protocol: &str) -> Option<&Arc<dyn AcceptAdapter>> {
        self.adapters.get(protocol)
    }

    /// The registered protocol names.
    pub fn protocols(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.adapters.keys().copied()
    }
}
