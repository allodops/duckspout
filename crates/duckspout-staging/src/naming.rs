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
//!   `_` inside it is followed by two hex digits — and never **ends** with
//!   `_`.
//! - Components are joined with `__`. A component that *starts* with an
//!   escape can widen a join into a three-underscore run (e.g. dataset `a`,
//!   partition `A` → `s_a___41__w0`), but the decomposition of
//!   `s_<dataset>__<partition>__w<window>` is still unambiguous: inside a
//!   run, only the split whose left side does not end in `_` puts both
//!   sides back in the encoding's image. With `encode_component` injective
//!   per component, distinct identities always yield distinct table names
//!   (property-tested below over arbitrary identifiers).
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

    mod laws {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            /// The §2.3 injectivity law over ARBITRARY identifiers (§8.5's
            /// property posture; the unit test above pins one adversarial
            /// pair, this quantifies over all of them): distinct
            /// (dataset, partition, window) identities never share a table
            /// name. The name is recomputed independently by drain, serving,
            /// and recovery — a collision would silently merge two windows'
            /// rows. Would catch: a separator weakening (single `_` join), a
            /// dropped escape in `encode_component`, or a case-folding
            /// change that maps two ids onto one name.
            #[test]
            fn distinct_identities_get_distinct_names(
                dataset_a in ".{0,12}", partition_a in ".{0,12}", window_a in 0u64..4,
                dataset_b in ".{0,12}", partition_b in ".{0,12}", window_b in 0u64..4,
            ) {
                let a = (DatasetId::new(dataset_a), PartitionId::new(partition_a), WindowId(window_a));
                let b = (DatasetId::new(dataset_b), PartitionId::new(partition_b), WindowId(window_b));
                prop_assume!(a != b);
                prop_assert_ne!(
                    window_table_name(&a.0, &a.1, a.2),
                    window_table_name(&b.0, &b.1, b.2)
                );
            }

            /// The bare-identifier law: for ANY input — control bytes,
            /// quotes, multi-byte UTF-8 — the name stays in `[a-z0-9_]`,
            /// starts with a letter, and is nonempty; it is always a bare,
            /// case-insensitive-safe `DuckDB` identifier, never something
            /// that needs quoting. Would catch an escape table letting a
            /// byte through verbatim — the SQL-injection-shaped bug.
            #[test]
            fn names_are_always_bare_duckdb_identifiers(
                dataset in ".{0,20}", partition in ".{0,20}", window in any::<u64>(),
            ) {
                let name = window_table_name(
                    &DatasetId::new(dataset), &PartitionId::new(partition), WindowId(window),
                );
                prop_assert!(name.starts_with('s'));
                prop_assert!(
                    name.bytes().all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_')),
                    "name {name:?} left the bare-identifier alphabet"
                );
            }
        }
    }
}
