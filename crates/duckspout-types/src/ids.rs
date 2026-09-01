//! Identifier newtypes (§2, §10.1).
//!
//! Every identifier that crosses a crate boundary is a newtype, so a tenant
//! id can never be passed where a dataset id is expected. String-backed ids
//! are opaque; [`WindowId`] is the dense per-partition window sequence number
//! (§6.8 — contiguity must be decidable, hence numeric).

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps a raw identifier.
            #[must_use]
            pub fn new(raw: impl Into<String>) -> Self {
                Self(raw.into())
            }

            /// The identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(raw: &str) -> Self {
                Self::new(raw)
            }
        }
    };
}

string_id! {
    /// A dataset name (§2). Datasets are the unit of declaration, schema, and
    /// retention class.
    DatasetId
}

string_id! {
    /// A tenant identity, extracted at accept time from the mTLS-verified
    /// edge (`X-Scope-OrgID`, §4.1.2). Tenants partition dedup keys and parts.
    TenantId
}

string_id! {
    /// A partition identity — the unit of ownership (HRW placement, §5),
    /// watermarks, and drain (§6).
    PartitionId
}

impl PartitionId {
    /// The canonical partition identity for `(tenant_id, shard)` — the
    /// partition key shape of §2.2. One rendering, defined once, so accept,
    /// replication, and drain can never disagree on which partition a
    /// (tenant, shard) pair names. `event` datasets default to a single
    /// shard (`shard_count` 1, so shard 0) in v1.
    #[must_use]
    pub fn from_tenant_shard(tenant: &TenantId, shard: u32) -> Self {
        Self(format!("{}.{shard}", tenant.as_str()))
    }
}

string_id! {
    /// A node identity. Also used as the *origin* of an `(origin, seq)`
    /// replication range (§4.2.4, §5).
    NodeId
}

string_id! {
    /// A sealed part's object name — a pure function of
    /// `(dataset, partition, window_id, part_kind, discriminator)` (§6.5), so
    /// drain retries produce identical names and re-registration is
    /// detectable.
    PartName
}

/// The dense per-partition window sequence number (§6.8).
///
/// Density is load-bearing: watermark reconstruction decides contiguity by
/// numeric adjacency of committed window ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WindowId(pub u64);

impl std::fmt::Display for WindowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
