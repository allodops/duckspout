//! The accept surface (§4): the adapter-registration seam and the OTLP
//! adapter, v1's only adapter (§4.1.2).
//!
//! The [`AcceptAdapter`] port is defined in `duckspout-types` (ADR-0008) and
//! re-exported here; this crate owns everything beyond the bare signature —
//! adapter registration and the concrete adapters. Durability semantics are
//! adapter-invariant: no adapter touches the ack path (§4.1.2).
//!
//! Ⓢ bootstrap stub — the real OTLP decoder lands at v0.1.
//!
//! Design home: `docs/design/ingest.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §4).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;

pub use duckspout_types::AcceptAdapter;

pub mod otlp;

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
