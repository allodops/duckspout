//! Deterministic part naming (§6.5, §2.7).
//!
//! A part's object name is a **pure function** of
//! `(dataset, partition, window_id, part_kind, discriminator)`, so two
//! attempts to drain the same window produce the same name: a re-PUT
//! overwrites byte-identical content and a re-register is detectable —
//! registration is idempotent by construction (§6.5). The name is also the
//! SQL fence key's rendering: `UNIQUE (partition, window_id, part_kind,
//! discriminator)` (§6.6) and this function fence the same identity.
//!
//! # Layout
//!
//! `{dataset}/{partition}/w{window}-{kind}[-{discriminator}].parquet`, with
//! each opaque id component encoded into `[a-z0-9_]`. The partition id is
//! its own path segment: partitions are `(tenant_id, shard)` (§2.2), so
//! parts are tenant-pure prefixes and cold-side IAM can be prefix-scoped
//! per tenant (§2.7 rule 2).
//!
//! # Injectivity
//!
//! `encode_component` maps `a-z`/`0-9` to themselves and every other byte
//! to `_` + two lowercase hex digits — injective per component, and its
//! output never contains `/` or `-`, so the `/` joins and the `-` separators
//! inside the file name are unambiguous. The kind token (`primary` /
//! `supplement` / `snapshot`) disambiguates the discriminator grammars.
//!
//! Deliberately **not** shared with staging's hot-table naming
//! (`duckspout-staging::naming`), despite the similar encoding: cold object
//! names are frozen forever once objects exist, while hot table names may
//! evolve with the engine — one source of truth per stability domain, two
//! domains.

use std::fmt::Write as _;

use duckspout_types::{DatasetId, NodeId, PartKind, PartName, PartitionId, WindowId};

/// The discriminator slot of the deterministic name (§6.5) — the part kind
/// is implied by the variant, so an invalid (kind, discriminator) pairing
/// is unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartDiscriminator {
    /// The window's sealed winner part: fixed discriminator (`'-'` in the
    /// §6.6 fence key) — at most one primary part per window, ever.
    Window,
    /// A supplement part, discriminated by its per-origin seq range
    /// (§6.6's supplement path).
    Supplement {
        /// The origin whose residue the supplement carries.
        origin: NodeId,
        /// First covered seq, inclusive.
        first_seq: u64,
        /// Last covered seq, inclusive.
        last_seq: u64,
    },
    /// A changelog snapshot part, discriminated by `snapshot_as_of_seq`
    /// (§6.7).
    Snapshot {
        /// The arrival sequence the snapshot is as-of.
        as_of_seq: u64,
    },
}

impl PartDiscriminator {
    /// The part kind this discriminator implies.
    #[must_use]
    pub fn kind(&self) -> PartKind {
        match self {
            Self::Window => PartKind::Primary,
            Self::Supplement { .. } => PartKind::Supplement,
            Self::Snapshot { .. } => PartKind::Snapshot,
        }
    }
}

/// Encodes one opaque id component into `[a-z0-9_]`: `a-z` and `0-9` map to
/// themselves, every other byte to `_` + two lowercase hex digits.
/// Injective (see the module docs).
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

/// The deterministic object name of one part (§6.5). Pure: equal inputs
/// yield equal names on every node, every retry, every recovery.
#[must_use]
pub fn part_name(
    dataset: &DatasetId,
    partition: &PartitionId,
    window: WindowId,
    discriminator: &PartDiscriminator,
) -> PartName {
    let suffix = match discriminator {
        PartDiscriminator::Window => "primary".to_owned(),
        PartDiscriminator::Supplement {
            origin,
            first_seq,
            last_seq,
        } => format!(
            "supplement-{}-{first_seq}-{last_seq}",
            encode_component(origin.as_str())
        ),
        PartDiscriminator::Snapshot { as_of_seq } => format!("snapshot-{as_of_seq}"),
    };
    PartName::new(format!(
        "{}/{}/w{window}-{suffix}.parquet",
        encode_component(dataset.as_str()),
        encode_component(partition.as_str()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_names_are_deterministic_and_readable() {
        let name = part_name(
            &DatasetId::new("logs"),
            &PartitionId::new("tenant1.0"),
            WindowId(7),
            &PartDiscriminator::Window,
        );
        assert_eq!(name.as_str(), "logs/tenant1_2e0/w7-primary.parquet");
        // Pure: recomputation yields the identical name (§6.5 idempotence).
        assert_eq!(
            name,
            part_name(
                &DatasetId::new("logs"),
                &PartitionId::new("tenant1.0"),
                WindowId(7),
                &PartDiscriminator::Window,
            )
        );
    }

    #[test]
    fn kinds_and_discriminators_render_distinctly() {
        let ds = DatasetId::new("d");
        let p = PartitionId::new("p");
        let w = WindowId(0);
        let primary = part_name(&ds, &p, w, &PartDiscriminator::Window);
        let supplement = part_name(
            &ds,
            &p,
            w,
            &PartDiscriminator::Supplement {
                origin: NodeId::new("n1"),
                first_seq: 3,
                last_seq: 9,
            },
        );
        let snapshot = part_name(&ds, &p, w, &PartDiscriminator::Snapshot { as_of_seq: 42 });
        assert_eq!(supplement.as_str(), "d/p/w0-supplement-n1-3-9.parquet");
        assert_eq!(snapshot.as_str(), "d/p/w0-snapshot-42.parquet");
        assert_eq!(
            PartDiscriminator::Window.kind(),
            duckspout_types::PartKind::Primary
        );
        let names = [primary, supplement, snapshot];
        for (i, a) in names.iter().enumerate() {
            for b in &names[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn adversarial_ids_do_not_collide() {
        // Defeats naive joining: without injective component encoding,
        // ("a", "5f_b") and ("a_", "5fb") could collide, and a '/' inside an
        // id could forge a path segment.
        let w = WindowId(0);
        let disc = PartDiscriminator::Window;
        let left = part_name(&DatasetId::new("a"), &PartitionId::new("5f_b"), w, &disc);
        let right = part_name(&DatasetId::new("a_"), &PartitionId::new("5fb"), w, &disc);
        assert_ne!(left, right);
        let forged = part_name(&DatasetId::new("a/b"), &PartitionId::new("c"), w, &disc);
        assert_eq!(forged.as_str(), "a_2fb/c/w0-primary.parquet");
    }
}
