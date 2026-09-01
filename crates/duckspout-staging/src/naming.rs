//! Micro-window table naming (§2.3, §4.2.2).
//!
//! One `DuckDB` table per (dataset, partition, micro-window). The table name
//! is a pure function of that identity, so drain, serving, and recovery can
//! all recompute it — the registry table ([`crate::StagingEngine`]'s
//! `duckspout_windows`) exists for *enumeration*, never as a second naming
//! authority.
//!
//! # Injectivity
//!
//! Dataset and partition ids are opaque strings ([`DatasetId`],
//! [`PartitionId`]), so the encoding must be collision-free by construction,
//! not by convention:
//!
//! - `encode_component` maps `a-z` and `0-9` to themselves and every other
//!   byte to `_` followed by exactly two lowercase hex digits. An encoded
//!   component therefore never contains two adjacent underscores — every
//!   `_` inside it is followed by two hex digits.
//! - Components are joined with `__`, which consequently occurs **only** at
//!   the two join points. The full name
//!   `s_<dataset>__<partition>__w<window>` decomposes at its `__`
//!   occurrences unambiguously, and `encode_component` is injective per
//!   component, so distinct identities always yield distinct table names.
//!
//! The output alphabet is `[a-z0-9_]` starting with a letter — always a
//! bare (unquoted, case-insensitive-safe) `DuckDB` identifier.

use std::fmt::Write as _;

use duckspout_types::{DatasetId, PartitionId, WindowId};

/// Encodes one identifier component into the `[a-z0-9_]` table-name
/// alphabet. Injective; see the module docs for the argument.
#[must_use]
fn encode_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' => out.push(char::from(byte)),
            other => {
                // Infallible: writing to a String cannot fail.
                let _ = write!(out, "_{other:02x}");
            }
        }
    }
    out
}

/// The staging table name for one (dataset, partition, micro-window) — a
/// pure function of the identity (§2.3), recomputable by every consumer.
#[must_use]
pub fn window_table_name(dataset: &DatasetId, partition: &PartitionId, window: WindowId) -> String {
    format!(
        "s_{}__{}__w{}",
        encode_component(dataset.as_str()),
        encode_component(partition.as_str()),
        window
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_lowercase_ids_stay_readable() {
        let name = window_table_name(
            &DatasetId::new("logs"),
            &PartitionId::new("tenant1.0"),
            WindowId(7),
        );
        assert_eq!(name, "s_logs__tenant1_2e0__w7");
    }

    #[test]
    fn escaping_covers_underscore_uppercase_and_utf8() {
        assert_eq!(encode_component("a_b"), "a_5fb");
        assert_eq!(encode_component("A"), "_41");
        // Multi-byte UTF-8 is escaped per byte.
        assert_eq!(encode_component("é"), "_c3_a9");
    }

    #[test]
    fn adversarial_ids_do_not_collide() {
        // The pair that defeats naive single-underscore joining: without the
        // double-underscore separator, ("a", "5f_b") and ("a_", "5fb") would
        // produce the same name.
        let w = WindowId(0);
        let left = window_table_name(&DatasetId::new("a"), &PartitionId::new("5f_b"), w);
        let right = window_table_name(&DatasetId::new("a_"), &PartitionId::new("5fb"), w);
        assert_ne!(left, right);
    }

    #[test]
    fn encoded_components_never_contain_adjacent_underscores() {
        for raw in ["_", "__", "_5f", "a__b", "\u{1f}", "A_Z"] {
            let encoded = encode_component(raw);
            assert!(
                !encoded.contains("__"),
                "{raw:?} encoded to {encoded:?} with adjacent underscores"
            );
        }
    }
}
