//! Retention honesty — Keep Rule 10 (`SnapshotCovered`, §3), judged
//! end-to-end (§8.4):
//!
//! > replayed from the journaled `Expire` events against read-back state: no
//! > expired changelog part lacks a committed snapshot covering its arrival
//! > range, and no acked record's last value became unreachable through
//! > expiry.
//!
//! That sentence is two obligations, and this module keeps them apart
//! because they fail differently and are convicted by different evidence:
//!
//! | Obligation | Evidence | Finding |
//! |---|---|---|
//! | **(A) Covered expiry** — the §3 guard itself | journaled `Expire` part descriptors × read-back committed snapshots | [`RetentionFinding::UncoveredExpiry`] |
//! | **(B) Reachable last value** — the guard's *purpose* | the acked changelog's fold × the served `<dataset>_latest` | [`RetentionFinding::LastValueUnreachable`] |
//!
//! (A) is the rule as written; (B) is what the rule exists to prevent, and
//! it can fail even where (A) holds — a snapshot that COVERS a part's
//! arrival range but was sealed with that key dropped satisfies the
//! containment test and still loses the value. Checking only (A) would be
//! checking the guard's syntax against itself.
//!
//! # `CoversArrival`, mechanically
//!
//! The §3 formula is
//!
//! ```text
//! SnapshotCovered ==
//!   \A e \in expired : (IsChangelogData(e) /\ e.kind # "snapshot") =>
//!     \E s \in lake : s.kind = "snapshot" /\ s.part = e.part /\ CoversArrival(s, e)
//! ```
//!
//! and every conjunct maps onto a real field this judge reads:
//!
//! - `IsChangelogData(e)` → [`PartRetention::dataset_kind`] is `changelog`.
//!   An `event` part's expiry is plain age-based retention with nothing to
//!   cover (`docs/design/drain.md` §7), so it is not checked — and, because
//!   it is not checked, it is not counted either (see the vacuity note).
//! - `e.kind # "snapshot"` → [`PartRetention::part_kind`] is not
//!   [`PartKind::Snapshot`]. A snapshot expires only under a NEWER covering
//!   snapshot, which is this same rule one level up; the §3 guard excludes
//!   it and so does this.
//! - `s.part = e.part` → same `(dataset, partition)`. The spec's `part` is
//!   the partition; the dataset is added because two datasets legitimately
//!   share a partition id shape and one's snapshot must never vouch for the
//!   other's part.
//! - `CoversArrival(s, e)` → [`covers_arrival`]: every one of `e`'s
//!   per-origin arrival ranges is contained in one of `s`'s, for the SAME
//!   origin. A part's arrival range is exactly its per-origin seq coverage
//!   (`docs/design/drain.md` §3 "arrival-window placement", §8 "per-origin
//!   seq coverage"), so the containment test is the whole of it.
//!
//! **One snapshot, not a union of snapshots.** The formula says
//! `\E s \in lake`, singular: the covering snapshot is one object. This
//! module does not union several snapshots' coverage to satisfy one expired
//! part, because a union would accept a state the guard rejects — the judge
//! would then acquit exactly what `ExpireUncovered` (the armed broken
//! variant, §3.6) is armed to catch.
//!
//! # The covering set is READ-BACK ONLY — `\E s \in lake`, exactly as written
//!
//! `SnapshotCovered` quantifies the covering snapshot over `lake`, the live
//! lake, and nothing else:
//!
//! ```text
//! \A e \in expired : … => \E s \in lake : …
//! ```
//!
//! So the covering set here is the read-back of committed snapshots, full
//! stop. **No journaled line ever joins it** — neither a `SnapshotSeal` nor
//! an `Expire` of a snapshot part. Both would be the same mistake, and it is
//! the mistake this module's own accusation shape makes fatal: obligation
//! (A)'s *charge* is a journaled `Expire` line, so admitting a journaled line
//! as the *defence* lets the subject acquit itself. A retention scheduler
//! that expires uncovered changelog parts would only have to journal one
//! more `Expire` line, for a snapshot that was never in any lake, claiming
//! whatever coverage it needed (ACPR finding MEDIUM-3, with an executed
//! repro — now [`tests::a_journaled_snapshot_expiry_does_not_vouch_for_an_expiry`]).
//! Read-back is the one piece of evidence the expiring node does not write.
//!
//! An earlier draft of this module took the covering set to be
//! `lake ∪ expired`, citing `specs/formal-core.md`'s `Expire` note. That
//! citation was a misreading: the union in that note defines `InLake(k)` for
//! **`NoAckedLoss`** — a different invariant, about whether an acked
//! record's key is still reachable — and `SnapshotCovered` is written two
//! screens later over bare `lake`. The spec is explicit about why it can
//! afford to be: "In the checked scope snapshots themselves are
//! keep-forever (a snapshot expires only under a newer covering snapshot,
//! which the small configuration never seals)."
//!
//! That rule is what makes read-back-only correct rather than merely strict.
//! `docs/design/drain.md` §7 (§6.7) builds changelog retention as **snapshot
//! rollover**: a newer snapshot is sealed covering at least what the one it
//! retires covered, and only then does the older one become expirable. So a
//! part legitimately expired under a snapshot that has itself since been
//! retired is covered by that snapshot's successor — which IS in read-back.
//! A partition whose only covering snapshot is gone from the lake with no
//! successor is not a false positive: it is a partition whose expired
//! changelog parts really have no committed covering snapshot, which is
//! precisely Keep Rule 10's charge
//! ([`tests::a_rolled_over_snapshots_successor_is_what_acquits_the_expiry`]).
//!
//! # What this judge deliberately does NOT convict
//!
//! Obligation (B) folds only ACKED changelog entries, and inherits
//! `crate::predicates::latest_view`'s two exclusions verbatim (it calls that
//! module's fold, rather than writing a second one that could disagree): a
//! served key never seen acked is ignored, and a row that also appears on a
//! `ClientTimeout` line is not judged in either direction. It then narrows
//! FURTHER, to rows whose winning entry actually falls inside an expired
//! part's arrival range — because a row that no expiry touched is
//! `latest_view`'s subject, not retention's. Convicting it here too would be
//! two predicates reporting one fault twice, and would make a
//! retention-honesty `Violation` mean "something, somewhere, is wrong with
//! the latest view."
//!
//! # Vacuity teeth, PER OBLIGATION
//!
//! The two obligations are counted separately and gated separately, exactly
//! as `crate::predicates::cache_transparency` gates its own
//! `equivalence_checked` and `lock_checked`. A single summed scalar hid a
//! real hazard: obligation (B) contributes zero whenever the journaled
//! partition ids and the served view's partition ids fail to line up — a
//! spelling drift, a tenant scoping change — and obligation (A)'s count
//! alone was enough to report a `Pass`, certifying a sentence half of which
//! had silently gone unexercised.
//!
//! So a `Pass` now requires BOTH: at least one guarded expiry evaluated
//! against read-back, and at least one acked row attributed to an expired
//! part's arrival range. A run that expired only `event` parts, or only
//! snapshots, or whose expiries touched no acked row's last value, reports
//! `NoVerdict` and says which half was empty. A run with no journaled
//! `Expire` descriptor at all short-circuits to `NoVerdict` before any
//! read-back is attempted, which is the state EVERY run in this workspace is
//! in today: nothing journals `Expire` (`crate::journal`'s producer-status
//! note).

use std::collections::{BTreeMap, BTreeSet};

use duckspout_types::{DatasetId, OriginSeqRange, PartKind, PartName, PartitionId, TraceEvent};

use crate::final_state::{CommittedParts, CommittedSnapshot, LatestView, ServedView};
use crate::journal::{ChangelogEntry, JournalSet, PartRetention};
use crate::predicates::latest_view::{dedup_records, fold_winners, unresolved_rows, winning_value};
use crate::verdict::Verdict;

/// One way a run violated Keep Rule 10 (module docs' two obligations).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionFinding {
    /// Obligation (A): a changelog part was expired with no committed
    /// snapshot covering its arrival range — Keep Rule 10 as written.
    UncoveredExpiry {
        /// The changelog dataset.
        dataset: DatasetId,
        /// The partition the part belonged to.
        partition: PartitionId,
        /// The expired object.
        part: PartName,
        /// The arrival range that went uncovered.
        coverage: Vec<OriginSeqRange>,
        /// How many snapshots were considered for this partition (`0` means
        /// the partition has never been snapshotted at all, which is the
        /// starkest form of the violation).
        snapshots_considered: usize,
    },
    /// Obligation (B): an acked record's last value is no longer served, and
    /// expiry is what removed the part that held it.
    LastValueUnreachable {
        /// The changelog dataset.
        dataset: DatasetId,
        /// The tenant whose row this is (a declared key is unique only
        /// within a tenant — `crate::predicates::latest_view`'s HIGH-1 note).
        tenant: duckspout_types::TenantId,
        /// The declared key.
        key: String,
        /// What the acked changelog folds to: `None` iff the winner is a
        /// tombstone, in which case the key must be ABSENT from the view.
        expected: Option<String>,
        /// What the view actually serves.
        served: Option<String>,
        /// The expired object whose arrival range held the winning entry.
        expired_part: PartName,
    },
}

impl std::fmt::Display for RetentionFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionFinding::UncoveredExpiry {
                dataset,
                partition,
                part,
                coverage,
                snapshots_considered,
            } => write!(
                f,
                "{dataset} partition {partition}: changelog part {part} was expired over arrival \
                 range {} with no committed snapshot covering it ({snapshots_considered} \
                 snapshot(s) considered) — Keep Rule 10 (SnapshotCovered, §3): uncovered \
                 changelog parts are keep-forever",
                render_coverage(coverage)
            ),
            RetentionFinding::LastValueUnreachable {
                dataset,
                tenant,
                key,
                expected,
                served,
                expired_part,
            } => write!(
                f,
                "{dataset} tenant {tenant} key {key:?}: the acked changelog folds to {expected:?} \
                 but the latest view serves {served:?}, and the winning entry sat inside expired \
                 part {expired_part} — retention made an acked record's last value unreachable \
                 (§8.4)"
            ),
        }
    }
}

/// Renders an arrival range for a finding message.
fn render_coverage(coverage: &[OriginSeqRange]) -> String {
    coverage
        .iter()
        .map(|range| format!("{}:{}..={}", range.origin, range.first_seq, range.last_seq))
        .collect::<Vec<_>>()
        .join(", ")
}

/// This predicate's verdict (§8.4's three-valued contract).
pub type RetentionVerdict = Verdict<RetentionFinding>;

/// Whether `snapshot` covers every one of `expired`'s per-origin arrival
/// ranges (module docs' `CoversArrival`).
///
/// Per origin, not in aggregate: a snapshot that folded origin `a`'s
/// sequences 1..=100 says nothing whatsoever about origin `b`'s, and a
/// coverage test that compared bare sequence numbers across origins would
/// let one origin's snapshot authorize deleting another's only copy of a
/// key. `changelog_seq` is dense per `(partition, origin)`
/// (`docs/design/ingest.md`), so origin is part of the coordinate, never
/// context.
#[must_use]
pub fn covers_arrival(snapshot: &CommittedSnapshot, expired: &[OriginSeqRange]) -> bool {
    expired.iter().all(|range| {
        snapshot.origin_coverage.iter().any(|covering| {
            covering.origin == range.origin
                && covering.first_seq <= range.first_seq
                && covering.last_seq >= range.last_seq
        })
    })
}

/// Whether `coverage` contains the record at `(origin, seq)`.
fn coverage_contains(coverage: &[OriginSeqRange], entry: &ChangelogEntry) -> bool {
    coverage.iter().any(|range| {
        range.origin == entry.origin
            && range.first_seq <= entry.changelog_seq
            && range.last_seq >= entry.changelog_seq
    })
}

/// Runs the predicate against the run's journaled expiries, the lake's
/// read-back of committed snapshots, and the served latest view.
#[must_use]
pub fn check<P: CommittedParts, V: LatestView>(
    journals: &JournalSet,
    parts: &P,
    view: &V,
) -> RetentionVerdict {
    let expired: Vec<&PartRetention> = journals
        .part_events(TraceEvent::Expire)
        .map(|(_, part)| part)
        .collect();
    if expired.is_empty() {
        return Verdict::NoVerdict(
            "no Expire line in any journal carried a part descriptor — this predicate had no \
             retention decision to replay (§8.4 vacuity teeth). Nothing in this workspace \
             journals Expire yet (crate::journal's producer-status note), so this is the \
             expected verdict until the retention scheduler lands."
                .to_owned(),
        );
    }

    let mut expiries_checked = 0usize;
    let mut findings = Vec::new();

    // Obligation (A). Snapshots are read back once per (dataset, partition)
    // that actually has a guarded expiry — never per expired part, which
    // would issue the same catalog query many times over.
    let guarded: Vec<&PartRetention> = expired
        .iter()
        .copied()
        .filter(|part| {
            part.dataset_kind == duckspout_types::DatasetKind::Changelog
                && part.part_kind != PartKind::Snapshot
        })
        .collect();
    let scopes: BTreeSet<(DatasetId, PartitionId)> = guarded
        .iter()
        .map(|part| (part.dataset.clone(), part.partition.clone()))
        .collect();
    let mut covering: BTreeMap<(DatasetId, PartitionId), Vec<CommittedSnapshot>> = BTreeMap::new();
    for (dataset, partition) in scopes {
        match parts.snapshots(&dataset, &partition) {
            Ok(snapshots) => {
                covering.insert((dataset, partition), snapshots);
            }
            Err(err) => {
                return Verdict::NoVerdict(format!(
                    "{err} — a failed read-back is not proof that a part was expired uncovered \
                     (nor that it was covered), so this run cannot be certified (§8.4 \
                     fail-closed posture)"
                ));
            }
        }
    }
    // Nothing is added to `covering` here, and that omission is the rule:
    // `\E s \in lake` is read-back and only read-back (module docs' covering-
    // set section). A journaled snapshot seal or a journaled snapshot expiry
    // would both let the node whose `Expire` line is on trial write its own
    // acquittal.

    for part in guarded {
        expiries_checked += 1;
        let scope = (part.dataset.clone(), part.partition.clone());
        let snapshots = covering.get(&scope).map_or(&[][..], Vec::as_slice);
        if !snapshots
            .iter()
            .any(|snapshot| covers_arrival(snapshot, &part.origin_coverage))
        {
            findings.push(RetentionFinding::UncoveredExpiry {
                dataset: part.dataset.clone(),
                partition: part.partition.clone(),
                part: part.part.clone(),
                coverage: part.origin_coverage.clone(),
                snapshots_considered: snapshots.len(),
            });
        }
    }

    // Obligation (B).
    let rows_checked = match check_last_values(journals, &expired, view) {
        Ok((rows_checked, mut row_findings)) => {
            findings.append(&mut row_findings);
            rows_checked
        }
        Err(reason) => return Verdict::NoVerdict(reason),
    };

    // A conviction outranks both vacuity gates: the honest headline for a
    // convicted run is the conviction, not "inconclusive" (the same ordering
    // `crate::predicates::cache_transparency::check` states for itself).
    if !findings.is_empty() {
        return Verdict::Violation(findings);
    }
    if expiries_checked == 0 {
        return Verdict::NoVerdict(
            "this run's journaled expiries were all of snapshot or `event` parts, so obligation \
             (A) — the §3 guard itself — evaluated nothing. Keep Rule 10 guards neither kind, and \
             a run that never expired a guarded changelog part certifies nothing about it (§8.4 \
             vacuity teeth)"
                .to_owned(),
        );
    }
    if rows_checked == 0 {
        return Verdict::NoVerdict(
            "no acked row's winning changelog entry fell inside any expired part's arrival range, \
             so obligation (B) — no acked record's last value became unreachable through expiry — \
             evaluated nothing. That is also what a partition-id or tenant spelling drift between \
             the journals and the served view looks like, which is exactly why obligation (A)'s \
             own count is not allowed to carry a `Pass` on its own (§8.4 vacuity teeth)"
                .to_owned(),
        );
    }

    Verdict::pass(
        expiries_checked + rows_checked,
        // Unreachable: both gates above already returned when their own count
        // was zero, so the sum is positive here. `Verdict::pass` is still the
        // constructor used, so this predicate can never grow a path that
        // reports a zero-check `Pass`.
        "nothing was checked",
    )
}

/// Obligation (B): every acked row whose winning entry sat inside an expired
/// part's arrival range must still be served (module docs).
///
/// Returns `(rows_checked, findings)`, or the reason the run cannot be
/// judged at all.
fn check_last_values<V: LatestView>(
    journals: &JournalSet,
    expired: &[&PartRetention],
    view: &V,
) -> Result<(usize, Vec<RetentionFinding>), String> {
    let acked: Vec<&ChangelogEntry> = journals
        .changelog_events(TraceEvent::ClientAck)
        .map(|(_, entry)| entry)
        .collect();
    if acked.is_empty() {
        // Not a `NoVerdict` for the whole predicate: obligation (A) may have
        // checked plenty. This half simply has no acked changelog to
        // attribute to an expiry, which the caller's own `checked` count
        // already reflects honestly.
        return Ok((0, Vec::new()));
    }
    let by_record = dedup_records(&acked)?;
    let unresolved = unresolved_rows(journals);
    let winners = fold_winners(&by_record);

    let mut served_views: BTreeMap<DatasetId, ServedView> = BTreeMap::new();
    let mut checked = 0usize;
    let mut findings = Vec::new();
    for (row @ (dataset, tenant, key), winner) in &winners {
        if unresolved.contains(row) {
            continue;
        }
        // Only rows retention actually touched (module docs' narrowing).
        let Some(holder) = expired.iter().copied().find(|part| {
            &part.dataset == dataset
                && part.partition == winner.partition
                && coverage_contains(&part.origin_coverage, winner)
        }) else {
            continue;
        };
        checked += 1;
        if !served_views.contains_key(dataset) {
            match view.view(dataset) {
                Ok(rows) => {
                    served_views.insert(dataset.clone(), rows);
                }
                Err(err) => {
                    return Err(format!(
                        "{err} — a failed read-back is not proof that expiry made a value \
                         unreachable (nor that it did not), so this run cannot be certified \
                         (§8.4 fail-closed posture)"
                    ));
                }
            }
        }
        let expected = winning_value(winner);
        let served = served_views
            .get(dataset)
            .and_then(|rows| rows.get(tenant))
            .and_then(|rows| rows.get(key))
            .cloned();
        if served != expected {
            findings.push(RetentionFinding::LastValueUnreachable {
                dataset: dataset.clone(),
                tenant: tenant.clone(),
                key: key.clone(),
                expected,
                served,
                expired_part: holder.part.clone(),
            });
        }
    }
    Ok((checked, findings))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use duckspout_types::{DatasetKind, NodeId, TenantId};

    use super::*;
    use crate::final_state::{InMemoryCommittedParts, InMemoryLatestView, PartsQueryError};
    use crate::journal::JournalLine;

    const DATASET: &str = "dim_users";
    const PARTITION: &str = "tenant-a.4";
    const TENANT: &str = "tenant-a";
    const ORIGIN: &str = "n1";

    fn range(first: u64, last: u64) -> OriginSeqRange {
        OriginSeqRange {
            origin: NodeId::new(ORIGIN),
            first_seq: first,
            last_seq: last,
        }
    }

    /// One journaled part descriptor on `event`.
    fn part_line(
        seq: u64,
        event: TraceEvent,
        part: &str,
        part_kind: PartKind,
        dataset_kind: DatasetKind,
        coverage: Vec<OriginSeqRange>,
    ) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new("n1"),
            seq,
            event,
            identity: None,
            watermark: None,
            changelog: None,
            part: Some(PartRetention {
                dataset: DatasetId::new(DATASET),
                partition: PartitionId::new(PARTITION),
                part: PartName::new(part),
                part_kind,
                dataset_kind,
                origin_coverage: coverage,
            }),
        }
    }

    /// The common case: a changelog primary part expired over `coverage`.
    fn expired_changelog_part(seq: u64, part: &str, coverage: Vec<OriginSeqRange>) -> JournalLine {
        part_line(
            seq,
            TraceEvent::Expire,
            part,
            PartKind::Primary,
            DatasetKind::Changelog,
            coverage,
        )
    }

    fn acked_changelog(
        seq: u64,
        key: &str,
        changelog_seq: u64,
        value: Option<&str>,
    ) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new("loadgen-0"),
            seq,
            event: TraceEvent::ClientAck,
            identity: None,
            watermark: None,
            changelog: Some(ChangelogEntry {
                dataset: DatasetId::new(DATASET),
                partition: PartitionId::new(PARTITION),
                tenant: TenantId::new(TENANT),
                changelog_key: key.to_owned(),
                origin: NodeId::new(ORIGIN),
                changelog_seq,
                tombstone: value.is_none(),
                value: value.map(ToOwned::to_owned),
            }),
            part: None,
        }
    }

    fn snapshot(part: &str, coverage: Vec<OriginSeqRange>) -> CommittedSnapshot {
        CommittedSnapshot {
            dataset: DatasetId::new(DATASET),
            partition: PartitionId::new(PARTITION),
            part: PartName::new(part),
            origin_coverage: coverage,
        }
    }

    fn journals(lines: Vec<JournalLine>) -> JournalSet {
        JournalSet { lines }
    }

    /// Obligation (B)'s own subject, for the fixtures whose point is
    /// obligation (A): an acked row whose winning entry sits inside
    /// `part-0`'s expired 1..=9 range and which the served view still
    /// serves. Without one of these, the per-obligation vacuity gate is
    /// (correctly) what decides the verdict, and the test would stop being
    /// about the guard at all. Its own correctness is what
    /// `a_last_value_preserved_through_a_covering_snapshot_passes` asserts.
    fn preserved_row() -> (JournalLine, InMemoryLatestView) {
        (
            acked_changelog(0, "kept", 3, Some("carol")),
            InMemoryLatestView::new().with_view(
                DATASET,
                TENANT,
                [("kept".to_owned(), "carol".to_owned())],
            ),
        )
    }

    #[test]
    fn no_expire_evidence_is_no_verdict_not_a_pass() {
        // The state every run in this workspace is in today: nothing
        // journals `Expire`. Reporting `Pass` here would certify a Keep Rule
        // the run never exercised.
        let verdict = check(
            &journals(vec![acked_changelog(0, "u1", 1, Some("alice"))]),
            &InMemoryCommittedParts::new(),
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::NoVerdict(_)));
        assert_eq!(verdict.exit_code(), 3);
    }

    #[test]
    fn a_covered_changelog_expiry_passes() {
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let (row, view) = preserved_row();
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                row,
            ]),
            &parts,
            &view,
        );
        // Two checks: the guarded expiry, and the row it held.
        assert_eq!(verdict, Verdict::Pass { checked: 2 });
    }

    #[test]
    fn obligation_a_alone_cannot_carry_a_pass() {
        // Per-obligation vacuity teeth (module docs). The guard evaluated a
        // real, covered expiry — but no acked row was attributed to it, and
        // "no attributed row" is also what a partition-id spelling drift
        // between the journals and the served view looks like. Would catch a
        // single summed `checked` scalar letting (A)'s count certify a
        // sentence whose other half never ran.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let verdict = check(
            &journals(vec![expired_changelog_part(0, "part-0", vec![range(1, 9)])]),
            &parts,
            &InMemoryLatestView::new(),
        );
        match verdict {
            Verdict::NoVerdict(reason) => assert!(reason.contains("obligation (B)"), "{reason}"),
            other => panic!("expected NoVerdict, got {other:?}"),
        }
    }

    #[test]
    fn an_uncovered_changelog_expiry_is_a_violation() {
        // Keep Rule 10 as written: the snapshot stops at seq 5, the part
        // reaches 9, so sequences 6..=9 lost their only copy. This is the
        // exact shape `specs/broken/ExpireUncovered.cfg` arms the model
        // checker against, judged end-to-end.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 5)]));
        let verdict = check(
            &journals(vec![expired_changelog_part(0, "part-0", vec![range(1, 9)])]),
            &parts,
            &InMemoryLatestView::new(),
        );
        match verdict {
            Verdict::Violation(findings) => {
                assert!(matches!(
                    findings.as_slice(),
                    [RetentionFinding::UncoveredExpiry {
                        snapshots_considered: 1,
                        ..
                    }]
                ));
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_partition_that_was_never_snapshotted_convicts_its_expiry() {
        let verdict = check(
            &journals(vec![expired_changelog_part(0, "part-0", vec![range(1, 9)])]),
            &InMemoryCommittedParts::new(),
            &InMemoryLatestView::new(),
        );
        match verdict {
            Verdict::Violation(findings) => assert!(matches!(
                findings.as_slice(),
                [RetentionFinding::UncoveredExpiry {
                    snapshots_considered: 0,
                    ..
                }]
            )),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn coverage_is_matched_per_origin_never_across_origins() {
        // Would catch a containment test that compared bare sequence
        // numbers: origin `n2`'s snapshot covering 1..=100 says NOTHING
        // about origin `n1`'s 1..=9, and accepting it would authorize
        // deleting n1's only copy of a key.
        let parts = InMemoryCommittedParts::new().with_snapshot(snapshot(
            "snap-0",
            vec![OriginSeqRange {
                origin: NodeId::new("n2"),
                first_seq: 1,
                last_seq: 100,
            }],
        ));
        let verdict = check(
            &journals(vec![expired_changelog_part(0, "part-0", vec![range(1, 9)])]),
            &parts,
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::Violation(_)));
    }

    #[test]
    fn coverage_is_not_unioned_across_two_snapshots() {
        // The §3 formula is `\E s`, singular (module docs). Two snapshots
        // that between them span 1..=9 do NOT satisfy the guard, and a judge
        // that unioned them would acquit exactly what `ExpireUncovered`
        // exists to convict.
        let parts = InMemoryCommittedParts::new()
            .with_snapshot(snapshot("snap-a", vec![range(1, 4)]))
            .with_snapshot(snapshot("snap-b", vec![range(5, 9)]));
        let verdict = check(
            &journals(vec![expired_changelog_part(0, "part-0", vec![range(1, 9)])]),
            &parts,
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::Violation(_)));
    }

    #[test]
    fn a_journaled_snapshot_expiry_does_not_vouch_for_an_expiry() {
        // ACPR finding MEDIUM-3, as a permanent regression test, and the
        // reviewer's own repro verbatim: an uncovered changelog expiry over a
        // real key range against an EMPTY lake, acquitted by adding one extra
        // `Expire` line claiming broad coverage for a snapshot that never
        // existed in the lake. Obligation (A)'s charge is a journaled
        // `Expire` line; admitting a journaled `Expire` line as the defence
        // lets the subject write its own acquittal (module docs' covering-set
        // section). `\E s \in lake` is read-back and only read-back.
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                part_line(
                    1,
                    TraceEvent::Expire,
                    "snap-that-never-was",
                    PartKind::Snapshot,
                    DatasetKind::Changelog,
                    vec![range(1, 20)],
                ),
            ]),
            &InMemoryCommittedParts::new(),
            &InMemoryLatestView::new(),
        );
        match verdict {
            Verdict::Violation(findings) => assert!(matches!(
                findings.as_slice(),
                [RetentionFinding::UncoveredExpiry {
                    snapshots_considered: 0,
                    ..
                }]
            )),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_rolled_over_snapshots_successor_is_what_acquits_the_expiry() {
        // The legitimate case the read-back-only rule has to keep acquitting,
        // and the reason it does (module docs): changelog retention is
        // SNAPSHOT ROLLOVER (`docs/design/drain.md` §7) — the newer snapshot
        // covers at least what the one it retires covered, and only then does
        // the older one become expirable. So the successor is in read-back
        // and covers the part directly; the retired snapshot's own journaled
        // expiry is never needed, and is not consulted.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-new", vec![range(1, 40)]));
        let (row, view) = preserved_row();
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                part_line(
                    1,
                    TraceEvent::Expire,
                    "snap-old",
                    PartKind::Snapshot,
                    DatasetKind::Changelog,
                    vec![range(1, 20)],
                ),
                row,
            ]),
            &parts,
            &view,
        );
        assert_eq!(verdict, Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_journaled_snapshot_seal_does_not_vouch_for_an_expiry() {
        // A seal that never committed proves nothing (module docs): only
        // read-back puts a snapshot in the covering set. Would catch a judge
        // that let the expiring node vouch for itself.
        let verdict = check(
            &journals(vec![
                part_line(
                    0,
                    TraceEvent::SnapshotSeal,
                    "snap-0",
                    PartKind::Snapshot,
                    DatasetKind::Changelog,
                    vec![range(1, 20)],
                ),
                expired_changelog_part(1, "part-0", vec![range(1, 9)]),
            ]),
            &InMemoryCommittedParts::new(),
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::Violation(_)));
    }

    #[test]
    fn an_event_part_expiry_is_neither_convicted_nor_counted() {
        // Keep Rule 10 is a changelog obligation; an `event` part expires on
        // age alone. Counting it would let a run of pure `event` retention
        // report a `Pass` that certified nothing about the rule.
        let verdict = check(
            &journals(vec![part_line(
                0,
                TraceEvent::Expire,
                "part-0",
                PartKind::Primary,
                DatasetKind::Event,
                vec![range(1, 9)],
            )]),
            &InMemoryCommittedParts::new(),
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::NoVerdict(_)));
    }

    #[test]
    fn a_snapshot_parts_own_expiry_is_not_itself_guarded() {
        // `e.kind # "snapshot"` in the §3 guard: a snapshot expires under a
        // newer snapshot, which is this rule one level up, and the formula
        // excludes it. A judge that guarded it would convict every correct
        // snapshot rollover.
        let verdict = check(
            &journals(vec![part_line(
                0,
                TraceEvent::Expire,
                "snap-old",
                PartKind::Snapshot,
                DatasetKind::Changelog,
                vec![range(1, 9)],
            )]),
            &InMemoryCommittedParts::new(),
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::NoVerdict(_)));
    }

    #[test]
    fn a_last_value_lost_to_a_covered_expiry_is_still_a_violation() {
        // Obligation (B), and the reason it is separate from (A): the
        // snapshot COVERS the expired part's arrival range — the §3 guard is
        // satisfied — but was sealed with this key dropped, so the acked
        // record's last value is gone anyway. Would catch a judge that
        // checked only the guard's syntax.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                acked_changelog(0, "u1", 7, Some("alice")),
            ]),
            &parts,
            &InMemoryLatestView::new(),
        );
        match verdict {
            Verdict::Violation(findings) => assert!(matches!(
                findings.as_slice(),
                [RetentionFinding::LastValueUnreachable { .. }]
            )),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_last_value_preserved_through_a_covering_snapshot_passes() {
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let view = InMemoryLatestView::new().with_view(
            DATASET,
            TENANT,
            [("u1".to_owned(), "alice".to_owned())],
        );
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                acked_changelog(0, "u1", 7, Some("alice")),
            ]),
            &parts,
            &view,
        );
        // Two checks: the guarded expiry, and the row it held.
        assert_eq!(verdict, Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_row_no_expiry_touched_is_left_to_the_latest_view_judge() {
        // Narrowing (module docs): `u1`'s winning entry at seq 40 sits
        // outside the expired part's 1..=9 range, so retention is not what
        // removed it — and the view does not serve it. Convicting it here
        // would report one fault twice and blur what a retention-honesty
        // violation means. `kept` (seq 3, inside the range, still served) is
        // what obligation (B) actually judges, so the `Pass` is earned rather
        // than vacuous.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let (row, view) = preserved_row();
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                row,
                acked_changelog(1, "u1", 40, Some("alice")),
            ]),
            &parts,
            &view,
        );
        assert_eq!(verdict, Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_timed_out_rows_last_value_is_not_judged_in_either_direction() {
        // Inherited from `latest_view`'s exclusion, deliberately shared
        // rather than re-derived: a row whose write timed out may or may not
        // have landed, so its absence from the view proves nothing.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let (row, view) = preserved_row();
        let mut timeout = acked_changelog(2, "u1", 7, Some("alice"));
        timeout.event = TraceEvent::ClientTimeout;
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                row,
                acked_changelog(1, "u1", 7, Some("alice")),
                timeout,
            ]),
            &parts,
            &view,
        );
        // `u1` is not judged in either direction; `kept` is, and passes.
        assert_eq!(verdict, Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_resurrected_tombstone_inside_an_expired_range_is_a_violation() {
        // The mirror of the lost-value case: the winning acked entry is a
        // tombstone, so the key must be ABSENT, and a snapshot that copied
        // the pre-delete value back in is just as wrong as one that dropped
        // a live key.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let view = InMemoryLatestView::new().with_view(
            DATASET,
            TENANT,
            [("u1".to_owned(), "alice".to_owned())],
        );
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                acked_changelog(0, "u1", 7, None),
            ]),
            &parts,
            &view,
        );
        assert!(matches!(verdict, Verdict::Violation(_)));
    }

    #[test]
    fn a_failed_snapshot_read_back_is_a_no_verdict_not_a_conviction() {
        struct FailingParts;
        impl CommittedParts for FailingParts {
            fn snapshots(
                &self,
                dataset: &DatasetId,
                partition: &PartitionId,
            ) -> Result<Vec<CommittedSnapshot>, PartsQueryError> {
                Err(PartsQueryError {
                    dataset: dataset.clone(),
                    partition: partition.clone(),
                    reason: "catalog unreachable".to_owned(),
                })
            }
        }
        let verdict = check(
            &journals(vec![expired_changelog_part(0, "part-0", vec![range(1, 9)])]),
            &FailingParts,
            &InMemoryLatestView::new(),
        );
        match verdict {
            Verdict::NoVerdict(reason) => assert!(reason.contains("catalog unreachable")),
            other => panic!("expected NoVerdict, got {other:?}"),
        }
    }

    #[test]
    fn two_conflicting_acks_for_one_record_fail_the_run_closed() {
        // Inherited from the shared fold: the same
        // (dataset, partition, origin, seq) acked twice with different
        // content makes the fold order itself ill-defined, so there is no
        // correct answer to compare against.
        let parts =
            InMemoryCommittedParts::new().with_snapshot(snapshot("snap-0", vec![range(1, 20)]));
        let verdict = check(
            &journals(vec![
                expired_changelog_part(0, "part-0", vec![range(1, 9)]),
                acked_changelog(0, "u1", 7, Some("alice")),
                acked_changelog(1, "u1", 7, Some("bob")),
            ]),
            &parts,
            &InMemoryLatestView::new(),
        );
        assert!(matches!(verdict, Verdict::NoVerdict(_)));
    }
}
