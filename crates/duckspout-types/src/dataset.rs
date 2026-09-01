//! Dataset declarations (§2, §9.6.2).

use serde::{Deserialize, Serialize};

use crate::ids::DatasetId;

/// The two dataset kinds (§2). They have different drain, dedup, and
/// retention mechanics; the kind is fixed at declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    /// Append-observations: spans, metric samples, logs. The default.
    #[default]
    Event,
    /// Keyed state changes with keep-latest semantics on `key_cols`.
    Changelog,
}

/// A dataset declaration — schema, not node config, but a ratcheted
/// configuration surface all the same (§9.6.2: 3 entries).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetDeclaration {
    /// The dataset's name.
    pub dataset: DatasetId,
    /// `event` or `changelog` (§9.6.2, default `event`).
    pub kind: DatasetKind,
    /// The changelog identity and shard axis; required iff
    /// `kind = changelog`, fixed at declaration (change = new dataset +
    /// replay).
    pub key_cols: Vec<String>,
    /// Drain `ORDER BY` (§6.2). `None` means the default: event time.
    pub sort_key: Option<Vec<String>>,
}
