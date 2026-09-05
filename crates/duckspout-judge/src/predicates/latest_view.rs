//! Per-key order and latest-view correctness — the §3 invariant
//! `LatestViewCorrect`, judged end-to-end (§8.4):
//!
//! > for every key, the served latest view equals the fold of that key's
//! > acked changelog in (origin, seq) order, across takeover and snapshot
//! > rollover; tombstones delete.
//!
//! # The fold, and why its order is not a free choice
//!
//! `DuckSpoutCore.tla`'s `LatestSet`/`View` fold each key's records under
//! `OrdLt` — origin first, then the origin-assigned seq — and the spec's
//! own transcription note (TN-19) says any fixed order serves "as long as
//! every fold uses the same one". That freedom belongs to the MODEL, not to
//! this judge: the served view is produced by the real system, whose order
//! is pinned by the sealed part's sort key `(key_cols, origin, seq)` and the
//! "newest `(origin, seq)` for a key wins" rule (`docs/design/drain.md` §2,
//! `docs/design/ingest.md` §4.2). This module therefore folds by
//! `(origin, seq)` with `origin` compared by its identifier value — the same
//! total order that sort establishes. A judge folding in a DIFFERENT fixed
//! order would convict every key two origins ever wrote concurrently, which
//! is a bug in the judge, not a finding.
//!
//! # Takeover and snapshot rollover are covered by construction
//!
//! The fold reads WHAT was acked, never WHO journaled it or WHEN: entries
//! for one key are folded identically whether they were acked by the
//! original owner or by a takeover node, and whether they landed before or
//! after a `SnapshotSeal` rolled the dataset over. That is precisely why the
//! spec can demand correctness "across takeover and snapshot rollover" from
//! one formula — a served view that lost a post-snapshot straggler, or that
//! ordered a takeover node's writes by arrival instead of by
//! `(origin, seq)`, differs from this fold and is convicted. The tests below
//! exercise both shapes explicitly.
//!
//! # What this judge deliberately does NOT convict
//!
//! Only ACKED entries are fold inputs (`ClientAck`-borne, §8.4's "the
//! verifying client is part of the test"). A record can be committed and
//! served without its client ever learning so — a timed-out or ambiguous
//! write that actually landed — so:
//!
//! - a served key this evidence never saw acked at all is IGNORED, not
//!   convicted: the fold of the acked changelog is a LOWER bound on what
//!   the view may legitimately contain, and §3's own formula quantifies
//!   over every committed and staged record, not only the acked ones; and
//! - a key that also appears on a `ClientTimeout` line is excluded from
//!   judging entirely, in either direction: the loadgen's timeout bucket is
//!   the "genuinely don't know" one (`duckspout_loadgen::outcome`), so that
//!   write may or may not have landed, and both "the view shows it" and
//!   "the view doesn't" are consistent with a correct system.
//!
//! The exclusion is deliberately keyed on `ClientTimeout` alone, and not on
//! "any event other than `ClientAck`": a `Throttle`/`Refuse` outcome means
//! the write was never admitted at all (§4.5), which is knowledge, not
//! ambiguity, and a blanket non-ack exclusion would additionally let any
//! future node-side changelog tracepoint (a `StageCommit`-borne entry, say)
//! silently exclude every key in the run — a predicate that can no longer
//! fire on any input, which is the vacuity §8.4 forbids rather than the
//! caution it demands.
//!
//! Both exclusions can only lose convictions, never manufacture them; and a
//! run in which they exclude EVERYTHING checks nothing, which
//! [`Verdict::pass`] turns into `NoVerdict` rather than a vacuous pass.
//!
//! # A record's identity, and a row's (ACPR finding HIGH-1)
//!
//! Two dimensions of `crate::journal::ChangelogEntry` are load-bearing here,
//! and neither is interchangeable with the other:
//!
//! - **`partition` names a record.** `changelog_seq` is dense per
//!   `(partition, origin)` (`docs/design/ingest.md`,
//!   `docs/design/replication.md`), so one origin's records in two
//!   partitions legitimately share a `changelog_seq`. The dedup slot is
//!   therefore `(dataset, partition, origin, changelog_seq)`; keying it on
//!   `(dataset, origin, changelog_seq)` would read two unrelated records as
//!   one record acked twice with different content — a self-inflicted
//!   `NoVerdict` on any multi-partition changelog run.
//! - **`tenant` names a row.** `changelog_key` is unique only within a
//!   tenant (`docs/design/data-model.md`: the partition key is
//!   `(tenant_id, shard)` and `<dataset>_latest` is `tenant_id`-leading), so
//!   the fold key is `(dataset, tenant, changelog_key)` and the read-back is
//!   tenant-scoped too (`crate::final_state::ServedView`). Folding on the
//!   bare key string would collapse two tenants' rows into one and convict a
//!   correct fleet.
//!
//! `partition` deliberately does NOT join the fold key: for a changelog
//! dataset `shard = hash(key_cols)` is fixed at declaration
//! (`docs/design/data-model.md`), so one tenant's key always lands in one
//! partition — adding it would be redundant on correct input and would split
//! a single key's history into two independently-folded halves on incorrect
//! input, hiding exactly the disagreement this predicate exists to find.

use std::collections::{BTreeMap, BTreeSet};

use duckspout_types::{DatasetId, NodeId, PartitionId, TenantId, TraceEvent};

use crate::final_state::{LatestView, ServedView};
use crate::journal::{ChangelogEntry, JournalSet};
use crate::verdict::Verdict;

/// One key whose served latest value disagreed with the fold of its acked
/// changelog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestViewFinding {
    /// The changelog dataset.
    pub dataset: DatasetId,
    /// The tenant whose row this is — part of the row's identity, not
    /// context (module docs).
    pub tenant: TenantId,
    /// The declared key, unique within `tenant`.
    pub key: String,
    /// What folding this key's acked changelog in `(origin, seq)` order
    /// yields: `None` iff the winning entry is a tombstone (the key must be
    /// absent from the view).
    pub expected: Option<String>,
    /// What the view actually served: `None` iff the key was absent.
    pub served: Option<String>,
}

impl std::fmt::Display for LatestViewFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            dataset,
            tenant,
            key,
            expected,
            served,
        } = self;
        match (expected, served) {
            (Some(expected), None) => write!(
                f,
                "{dataset} tenant {tenant} key {key:?}: the acked changelog folds to \
                 {expected:?}, but the latest view does not serve this key at all"
            ),
            (None, Some(served)) => write!(
                f,
                "{dataset} tenant {tenant} key {key:?}: the acked changelog's winning entry is a \
                 tombstone, but the latest view still serves {served:?} — tombstones delete (§7.7)"
            ),
            (Some(expected), Some(served)) => write!(
                f,
                "{dataset} tenant {tenant} key {key:?}: the acked changelog folds to \
                 {expected:?}, but the latest view serves {served:?}"
            ),
            // Unreachable: a finding is only recorded on a disagreement.
            (None, None) => write!(
                f,
                "{dataset} tenant {tenant} key {key:?}: no disagreement (bug)"
            ),
        }
    }
}

/// This predicate's verdict (§8.4's three-valued contract).
pub type LatestViewVerdict = Verdict<LatestViewFinding>;

/// The fold order's key: `(origin, seq)`, compared exactly as the sealed
/// part's own `(key_cols, origin, seq)` sort compares them (module docs).
type FoldOrder = (NodeId, u64);

/// What names ONE changelog record: `changelog_seq` is dense per
/// `(partition, origin)`, so the partition is part of the record's identity
/// and not context (module docs, ACPR finding HIGH-1).
type RecordSlot = (DatasetId, PartitionId, NodeId, u64);

/// What names ONE row of `<dataset>_latest`: a declared key is unique only
/// within a tenant (module docs, ACPR finding HIGH-1).
type RowKey = (DatasetId, TenantId, String);

/// Runs the predicate against `journals`' acked changelog entries and
/// `view`'s read-back of each dataset's `<dataset>_latest`.
#[must_use]
pub fn check<V: LatestView>(journals: &JournalSet, view: &V) -> LatestViewVerdict {
    let acked: Vec<&ChangelogEntry> = journals
        .changelog_events(TraceEvent::ClientAck)
        .map(|(_, entry)| entry)
        .collect();
    if acked.is_empty() {
        return Verdict::NoVerdict(
            "no ClientAck line in any journal carried a changelog entry — this predicate had no \
             acked changelog to fold (§8.4 vacuity teeth)"
                .to_owned(),
        );
    }

    let by_record = match dedup_records(&acked) {
        Ok(by_record) => by_record,
        Err(reason) => return Verdict::NoVerdict(reason),
    };

    // Rows whose outcome is genuinely unknown (module docs): a
    // `ClientTimeout`-borne entry is the loadgen's own "don't know" bucket,
    // so that row cannot be judged in either direction. Scoped per tenant
    // like every other row identity here — one tenant's timed-out write must
    // not silently un-judge another tenant's row that happens to share the
    // key string.
    let unresolved_rows: BTreeSet<RowKey> = journals
        .changelog_events(TraceEvent::ClientTimeout)
        .map(|(_, entry)| {
            (
                entry.dataset.clone(),
                entry.tenant.clone(),
                entry.changelog_key.clone(),
            )
        })
        .collect();

    let winners = fold_winners(&by_record);

    let datasets: BTreeSet<DatasetId> = winners
        .keys()
        .map(|(dataset, _, _)| dataset.clone())
        .collect();
    let mut served_views: BTreeMap<DatasetId, ServedView> = BTreeMap::new();
    for dataset in datasets {
        match view.view(&dataset) {
            Ok(rows) => {
                served_views.insert(dataset, rows);
            }
            Err(err) => {
                return Verdict::NoVerdict(format!(
                    "{err} — a failed read-back is not proof that the view is wrong (nor that it \
                     is right), so this run cannot be certified (§8.4 fail-closed posture)"
                ));
            }
        }
    }

    let mut checked = 0usize;
    let mut findings = Vec::new();
    for (row @ (dataset, tenant, key), (_, expected)) in &winners {
        if unresolved_rows.contains(row) {
            continue;
        }
        checked += 1;
        let served = served_views
            .get(dataset)
            .and_then(|view| view.get(tenant))
            .and_then(|rows| rows.get(key))
            .cloned();
        if &served != expected {
            findings.push(LatestViewFinding {
                dataset: dataset.clone(),
                tenant: tenant.clone(),
                key: key.clone(),
                expected: expected.clone(),
                served,
            });
        }
    }

    if findings.is_empty() {
        Verdict::pass(
            checked,
            "every acked changelog row was also written by a request that timed out, so no row's \
             correct latest value is knowable from this run's evidence — nothing was checked \
             (§8.4 vacuity teeth)",
        )
    } else {
        Verdict::Violation(findings)
    }
}

/// Collapses the acked entries onto one entry per RECORD, or the reason the
/// run cannot be judged at all.
///
/// A `(dataset, partition, origin, changelog_seq)` quad names ONE record
/// (module docs' HIGH-1 note: the sequence is dense per
/// `(partition, origin)`, so the partition is part of the identity). The same
/// record legitimately reappears — §4.4.1's dedup replays an idempotent retry
/// and acks it again with identical content — so an identical repeat is
/// deduplicated. A repeat that DISAGREES is two different records claiming
/// one sequence number, which makes the fold order itself ill-defined; there
/// is no correct answer to compare the view against, so the run fails closed
/// rather than folding whichever copy happened to be journaled first.
fn dedup_records<'a>(
    acked: &[&'a ChangelogEntry],
) -> Result<BTreeMap<RecordSlot, &'a ChangelogEntry>, String> {
    let mut by_record: BTreeMap<RecordSlot, &ChangelogEntry> = BTreeMap::new();
    for entry in acked.iter().copied() {
        let slot = (
            entry.dataset.clone(),
            entry.partition.clone(),
            entry.origin.clone(),
            entry.changelog_seq,
        );
        if let Some(previous) = by_record.insert(slot, entry)
            && previous != entry
        {
            return Err(format!(
                "changelog record ({}, partition {}, origin {}, seq {}) was acked twice with \
                 DIFFERENT content ({:?} vs {:?}) — the (origin, seq) fold order has no answer \
                 here, so this run cannot be judged (ambiguity fails closed, §8.4)",
                entry.dataset, entry.partition, entry.origin, entry.changelog_seq, previous, entry
            ));
        }
    }
    Ok(by_record)
}

/// The fold itself: per row, the entry with the greatest `(origin, seq)`
/// wins; a winning tombstone means the key must be absent from the view
/// (§7.7), which is what the `None` value means here.
fn fold_winners(
    by_record: &BTreeMap<RecordSlot, &ChangelogEntry>,
) -> BTreeMap<RowKey, (FoldOrder, Option<String>)> {
    let mut winners: BTreeMap<RowKey, (FoldOrder, Option<String>)> = BTreeMap::new();
    for entry in by_record.values() {
        let key = (
            entry.dataset.clone(),
            entry.tenant.clone(),
            entry.changelog_key.clone(),
        );
        let order: FoldOrder = (entry.origin.clone(), entry.changelog_seq);
        let value = if entry.tombstone {
            None
        } else {
            entry.value.clone()
        };
        match winners.get(&key) {
            Some((seen, _)) if *seen >= order => {}
            _ => {
                winners.insert(key, (order, value));
            }
        }
    }
    winners
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::final_state::{InMemoryLatestView, LatestView, ViewQueryError};
    use crate::journal::JournalLine;

    /// The tenant every single-tenant test uses, and the partition its
    /// keys hash to (`shard = hash(key_cols)`, `docs/design/data-model.md`).
    const TENANT: &str = "tenant-a";
    const PARTITION: &str = "tenant-a.0";

    /// The changelog entry a test journals, without the envelope around it.
    #[derive(Clone, Copy)]
    struct Entry<'a> {
        dataset: &'a str,
        partition: &'a str,
        tenant: &'a str,
        key: &'a str,
        origin: &'a str,
        changelog_seq: u64,
        /// `None` is a tombstone.
        value: Option<&'a str>,
    }

    impl<'a> Entry<'a> {
        /// The common case: dataset `dim`, one tenant, one partition.
        fn new(key: &'a str, origin: &'a str, changelog_seq: u64, value: Option<&'a str>) -> Self {
            Self {
                dataset: "dim",
                partition: PARTITION,
                tenant: TENANT,
                key,
                origin,
                changelog_seq,
                value,
            }
        }
    }

    /// One journaled changelog entry, on the given event.
    fn entry_line(node: &str, seq: u64, event: TraceEvent, entry: Entry<'_>) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new(node),
            seq,
            event,
            identity: None,
            watermark: None,
            changelog: Some(ChangelogEntry {
                dataset: DatasetId::new(entry.dataset),
                partition: PartitionId::new(entry.partition),
                tenant: TenantId::new(entry.tenant),
                changelog_key: entry.key.to_owned(),
                origin: NodeId::new(entry.origin),
                changelog_seq: entry.changelog_seq,
                tombstone: entry.value.is_none(),
                value: entry.value.map(ToOwned::to_owned),
            }),
        }
    }

    /// The common case: dataset `dim`, acked, journaled by the loadgen.
    fn acked(
        seq: u64,
        key: &str,
        origin: &str,
        changelog_seq: u64,
        value: Option<&str>,
    ) -> JournalLine {
        entry_line(
            "loadgen-0",
            seq,
            TraceEvent::ClientAck,
            Entry::new(key, origin, changelog_seq, value),
        )
    }

    /// The single-tenant view `acked` writes into.
    fn view_of(rows: &[(&str, &str)]) -> InMemoryLatestView {
        InMemoryLatestView::new().with_view(
            "dim",
            TENANT,
            rows.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        )
    }

    #[test]
    fn a_view_matching_the_fold_passes() {
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 2, Some("v2")),
                acked(2, "k2", "n1", 3, Some("w1")),
            ],
        };
        let view = view_of(&[("k1", "v2"), ("k2", "w1")]);
        assert_eq!(check(&journals, &view), Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_stale_latest_value_is_convicted() {
        // The seeded-violation shape: the view kept the earlier version of
        // k1 even though a later acked entry superseded it.
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 2, Some("v2")),
            ],
        };
        let view = view_of(&[("k1", "v1")]);
        match check(&journals, &view) {
            Verdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].expected.as_deref(), Some("v2"));
                assert_eq!(findings[0].served.as_deref(), Some("v1"));
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn a_key_the_view_never_serves_at_all_is_convicted() {
        let journals = JournalSet {
            lines: vec![acked(0, "k1", "n1", 1, Some("v1"))],
        };
        let view = view_of(&[]);
        match check(&journals, &view) {
            Verdict::Violation(findings) => {
                assert_eq!(findings[0].served, None);
                assert_eq!(findings[0].expected.as_deref(), Some("v1"));
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn a_tombstone_that_did_not_delete_is_convicted() {
        // "Tombstones delete" (§7.7): the winning entry is a delete, so the
        // key must be absent from the view — a resurrected key is exactly
        // the defect this clause exists for.
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 2, None),
            ],
        };
        let view = view_of(&[("k1", "v1")]);
        match check(&journals, &view) {
            Verdict::Violation(findings) => {
                assert_eq!(findings[0].expected, None);
                assert_eq!(findings[0].served.as_deref(), Some("v1"));
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn a_tombstone_that_deleted_passes() {
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 2, None),
            ],
        };
        let view = view_of(&[]);
        assert_eq!(check(&journals, &view), Verdict::Pass { checked: 1 });
    }

    #[test]
    fn a_tombstone_superseded_by_a_later_write_resurrects_the_key() {
        // The other direction of the same rule: a delete is not permanent —
        // a later `(origin, seq)` write puts the key back, and a view that
        // kept it deleted would be wrong.
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, None),
                acked(1, "k1", "n1", 2, Some("v2")),
            ],
        };
        assert_eq!(
            check(&journals, &view_of(&[("k1", "v2")])),
            Verdict::Pass { checked: 1 }
        );
        assert!(matches!(
            check(&journals, &view_of(&[])),
            Verdict::Violation(_)
        ));
    }

    #[test]
    fn the_fold_uses_origin_seq_order_not_journal_arrival_order() {
        // The takeover shape, and the sharpest ordering test: n2's entry is
        // journaled FIRST (by the takeover node, seq 0) and n1's straggler
        // arrives later (seq 1), but `(origin, seq)` order puts n2 last, so
        // n2's value must win. A judge — or a system — that folded by
        // arrival would answer "a1" here.
        let journals = JournalSet {
            lines: vec![
                entry_line(
                    "node-b",
                    0,
                    TraceEvent::ClientAck,
                    Entry::new("k1", "n2", 1, Some("b1")),
                ),
                entry_line(
                    "node-a",
                    0,
                    TraceEvent::ClientAck,
                    Entry::new("k1", "n1", 9, Some("a1")),
                ),
            ],
        };
        assert_eq!(
            check(&journals, &view_of(&[("k1", "b1")])),
            Verdict::Pass { checked: 1 }
        );
        match check(&journals, &view_of(&[("k1", "a1")])) {
            Verdict::Violation(findings) => {
                assert_eq!(findings[0].expected.as_deref(), Some("b1"));
            }
            other => panic!("expected the arrival-ordered view to be convicted, got {other:?}"),
        }
    }

    #[test]
    fn a_straggler_acked_after_a_snapshot_seal_must_still_be_in_the_view() {
        // Snapshot rollover: the view is served from the newest snapshot
        // plus the changelog since it (§7.7). A snapshot sealed between two
        // acked entries changes nothing about the fold — a view that
        // reflects only the snapshot has lost the straggler.
        let snapshot_seal = JournalLine {
            source: PathBuf::from("test"),
            line_no: 2,
            node: NodeId::new("node-a"),
            seq: 0,
            event: TraceEvent::SnapshotSeal,
            identity: None,
            watermark: None,
            changelog: None,
        };
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("pre")),
                snapshot_seal,
                acked(1, "k1", "n1", 2, Some("post")),
            ],
        };
        assert_eq!(
            check(&journals, &view_of(&[("k1", "post")])),
            Verdict::Pass { checked: 1 }
        );
        match check(&journals, &view_of(&[("k1", "pre")])) {
            Verdict::Violation(findings) => {
                assert_eq!(findings[0].served.as_deref(), Some("pre"));
            }
            other => panic!("expected the stale-snapshot view to be convicted, got {other:?}"),
        }
    }

    #[test]
    fn a_key_the_evidence_never_saw_acked_is_not_convicted() {
        // A committed-but-unacked write (a timeout that actually landed) is
        // legitimately in the view and invisible to this evidence —
        // convicting on it would be a false conviction (module docs).
        let journals = JournalSet {
            lines: vec![acked(0, "k1", "n1", 1, Some("v1"))],
        };
        let view = view_of(&[("k1", "v1"), ("unknown-key", "x")]);
        assert_eq!(check(&journals, &view), Verdict::Pass { checked: 1 });
    }

    #[test]
    fn a_key_whose_only_evidence_is_a_timeout_is_not_judged_at_all() {
        // k2's only evidence is a ClientTimeout — it may or may not have
        // landed, so neither serving it nor omitting it can be judged.
        let timeout = entry_line(
            "loadgen-0",
            1,
            TraceEvent::ClientTimeout,
            Entry::new("k2", "n1", 2, Some("maybe")),
        );
        let journals = JournalSet {
            lines: vec![acked(0, "k1", "n1", 1, Some("v1")), timeout],
        };
        assert_eq!(
            check(&journals, &view_of(&[("k1", "v1"), ("k2", "maybe")])),
            Verdict::Pass { checked: 1 }
        );
        assert_eq!(
            check(&journals, &view_of(&[("k1", "v1")])),
            Verdict::Pass { checked: 1 }
        );
    }

    #[test]
    fn an_acked_key_that_was_also_timed_out_is_excluded_from_judging() {
        // The retry shape: the client timed out, retried, and the retry was
        // acked. The timed-out attempt may ALSO have landed, so this key's
        // correct latest value is not knowable from the evidence — and with
        // it excluded, nothing at all was checked, which must be NoVerdict
        // rather than a vacuous pass.
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 2, Some("v2")),
                entry_line(
                    "loadgen-0",
                    1,
                    TraceEvent::ClientTimeout,
                    Entry::new("k1", "n1", 1, Some("v1")),
                ),
            ],
        };
        assert!(matches!(
            check(&journals, &view_of(&[("k1", "v9")])),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn no_acked_changelog_entry_at_all_is_no_verdict_not_a_vacuous_pass() {
        let journals = JournalSet::default();
        assert!(matches!(
            check(&journals, &InMemoryLatestView::new()),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn an_identical_idempotent_replay_is_deduplicated_not_treated_as_a_conflict() {
        // §4.4.1: a retried idempotent request is acked again with the same
        // content. That must fold as one record, not fail the run.
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 1, Some("v1")),
            ],
        };
        assert_eq!(
            check(&journals, &view_of(&[("k1", "v1")])),
            Verdict::Pass { checked: 1 }
        );
    }

    #[test]
    fn two_different_records_claiming_one_origin_seq_is_no_verdict() {
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 1, Some("DIFFERENT")),
            ],
        };
        match check(&journals, &view_of(&[("k1", "v1")])) {
            Verdict::NoVerdict(reason) => assert!(reason.contains("DIFFERENT"), "reason: {reason}"),
            other => panic!("expected NoVerdict on an ill-defined fold order, got {other:?}"),
        }
    }

    /// A double whose view query always fails — proves a read-back failure
    /// is never silently read as "the view is empty".
    struct AlwaysFailingView;

    impl LatestView for AlwaysFailingView {
        fn view(&self, dataset: &DatasetId) -> Result<ServedView, ViewQueryError> {
            Err(ViewQueryError {
                dataset: dataset.clone(),
                reason: "simulated Flight stream failure".to_owned(),
            })
        }
    }

    #[test]
    fn a_view_query_failure_is_no_verdict_not_a_violation() {
        let journals = JournalSet {
            lines: vec![acked(0, "k1", "n1", 1, Some("v1"))],
        };
        assert!(matches!(
            check(&journals, &AlwaysFailingView),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn datasets_are_judged_independently() {
        let journals = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                entry_line(
                    "loadgen-0",
                    1,
                    TraceEvent::ClientAck,
                    Entry {
                        dataset: "other",
                        ..Entry::new("k1", "n1", 2, Some("other-v"))
                    },
                ),
            ],
        };
        // Only `dim` is served correctly; `other`'s view is empty.
        match check(&journals, &view_of(&[("k1", "v1")])) {
            Verdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].dataset, DatasetId::new("other"));
            }
            other => panic!("expected exactly the other dataset to be convicted, got {other:?}"),
        }
    }

    #[test]
    fn two_partitions_sharing_one_changelog_seq_are_two_records_not_one_conflict() {
        // ACPR finding HIGH-1(a), as the reviewer's repro: `changelog_seq`
        // is dense per `(partition, origin)` (`docs/design/ingest.md`,
        // `docs/design/replication.md`), so ONE origin writing into two
        // partitions legitimately assigns seq 1 twice. A dedup slot keyed
        // `(dataset, origin, changelog_seq)` reads that as "one record acked
        // twice with different content" and permanently disables this
        // predicate — a spurious `NoVerdict` on any multi-partition
        // changelog run, which is exactly what "skipped ≠ passed" forbids
        // being shrugged off.
        let journals = JournalSet {
            lines: vec![
                entry_line(
                    "loadgen-0",
                    0,
                    TraceEvent::ClientAck,
                    Entry {
                        partition: "tenant-a.0",
                        ..Entry::new("k-in-shard-0", "n1", 1, Some("v-shard-0"))
                    },
                ),
                entry_line(
                    "loadgen-0",
                    1,
                    TraceEvent::ClientAck,
                    Entry {
                        partition: "tenant-a.1",
                        ..Entry::new("k-in-shard-1", "n1", 1, Some("v-shard-1"))
                    },
                ),
            ],
        };
        assert_eq!(
            check(
                &journals,
                &view_of(&[("k-in-shard-0", "v-shard-0"), ("k-in-shard-1", "v-shard-1")])
            ),
            Verdict::Pass { checked: 2 },
            "two partitions' records must be judged as two records, not collapsed into a conflict"
        );
        // …and the conflict rule still fires when it genuinely should: the
        // SAME partition, origin and seq with different content.
        let real_conflict = JournalSet {
            lines: vec![
                acked(0, "k1", "n1", 1, Some("v1")),
                acked(1, "k1", "n1", 1, Some("DIFFERENT")),
            ],
        };
        assert!(matches!(
            check(&real_conflict, &view_of(&[("k1", "v1")])),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn two_tenants_sharing_one_changelog_key_are_two_rows_not_one() {
        // ACPR finding HIGH-1(b), as the reviewer's repro: `changelog_key`
        // is unique only within a tenant (`docs/design/data-model.md`: the
        // partition key is `(tenant_id, shard)` and `<dataset>_latest` is
        // `tenant_id`-leading). A fold keyed on the bare key string folds
        // tenant-b's row on top of tenant-a's and then convicts a view that
        // serves both correctly — a false conviction against a correct
        // fleet.
        let journals = JournalSet {
            lines: vec![
                entry_line(
                    "loadgen-0",
                    0,
                    TraceEvent::ClientAck,
                    Entry {
                        tenant: "tenant-a",
                        partition: "tenant-a.0",
                        ..Entry::new("shared-key", "n1", 1, Some("a-value"))
                    },
                ),
                entry_line(
                    "loadgen-0",
                    1,
                    TraceEvent::ClientAck,
                    Entry {
                        tenant: "tenant-b",
                        partition: "tenant-b.0",
                        ..Entry::new("shared-key", "n1", 2, Some("b-value"))
                    },
                ),
            ],
        };
        let correct = InMemoryLatestView::new()
            .with_view(
                "dim",
                "tenant-a",
                [("shared-key".to_owned(), "a-value".to_owned())],
            )
            .with_view(
                "dim",
                "tenant-b",
                [("shared-key".to_owned(), "b-value".to_owned())],
            );
        assert_eq!(
            check(&journals, &correct),
            Verdict::Pass { checked: 2 },
            "each tenant's row must be folded and compared on its own"
        );
        // …and a view that really did serve one tenant's value under the
        // other's identity is still convicted, exactly once.
        let crossed = InMemoryLatestView::new()
            .with_view(
                "dim",
                "tenant-a",
                [("shared-key".to_owned(), "a-value".to_owned())],
            )
            .with_view(
                "dim",
                "tenant-b",
                [("shared-key".to_owned(), "a-value".to_owned())],
            );
        match check(&journals, &crossed) {
            Verdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].tenant, TenantId::new("tenant-b"));
                assert_eq!(findings[0].expected.as_deref(), Some("b-value"));
                assert_eq!(findings[0].served.as_deref(), Some("a-value"));
            }
            other => panic!("expected exactly tenant-b's row to be convicted, got {other:?}"),
        }
    }

    #[test]
    fn one_tenants_timed_out_write_does_not_un_judge_another_tenants_row() {
        // The tenant dimension applies to the exclusion set too: tenant-b's
        // "don't know" bucket says nothing about tenant-a's row, and letting
        // it exclude that row would silently stop this predicate from
        // checking a row whose value IS knowable.
        let journals = JournalSet {
            lines: vec![
                acked(0, "shared-key", "n1", 1, Some("a-value")),
                entry_line(
                    "loadgen-0",
                    1,
                    TraceEvent::ClientTimeout,
                    Entry {
                        tenant: "tenant-b",
                        partition: "tenant-b.0",
                        ..Entry::new("shared-key", "n1", 2, Some("maybe"))
                    },
                ),
            ],
        };
        match check(&journals, &view_of(&[("shared-key", "STALE")])) {
            Verdict::Violation(findings) => {
                assert_eq!(findings[0].tenant, TenantId::new(TENANT));
            }
            other => panic!("tenant-a's row must still be judged, got {other:?}"),
        }
    }
}
