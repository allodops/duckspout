//! Watermark honesty — the Q-shaped judge, query side (§8.4):
//!
//! > claimed vs. served — every watermark value any node ever advertised is
//! > replayed against the journals: no record acked before that watermark
//! > may be missing from a `complete` read at it, and no `complete` read may
//! > have been served over coverage the journals show did not exist at
//! > serving time. Fail-closed refusals are correct outcomes; optimistic
//! > answers that happened to be right are still violations if coverage was
//! > unproven.
//!
//! # What the journals prove about coverage
//!
//! `WatermarkAdvance` is not an event of its own — it rides the `LakeCommit`
//! outcome atomically (§6.4) — so a watermark value in the journals is
//! either an ADVANCE (the commit that made coverage real) or an
//! ADVERTISEMENT (a registry row, an ack's freshness disclosure: soft state
//! that must be backed by an advance). This module classifies each claim by
//! the event it rode:
//!
//! - [`COVERAGE_SOURCES`] — `LakeCommitOk` and `DeclareLoss` establish
//!   coverage outright (§6.5, §5.8's one sanctioned weakening).
//!   `LakeCommitIndeterminate` and `Reconcile` are counted with them, as an
//!   UPPER bound: an Indeterminate commit may well have landed (§6.5 — "the
//!   implementation cannot know which successor it took"), and a judge that
//!   assumed it had not would convict a correct fleet for advertising
//!   coverage that really did exist.
//! - `LakeCommitAbort` establishes nothing and advertises nothing: the
//!   outcome is a definitive rejection, "nothing changed" (§6.5). A
//!   watermark value on an aborted commit is the value that would have been
//!   reached, so it is neither coverage nor a claim — it is ignored.
//! - Every other event's claim is an advertisement, checked against the
//!   coverage bound.
//!
//! A partition with no coverage source at all has bound **0**, not
//! "unknown": §3 initializes `wm = [p ∈ Partitions ↦ 0]` and advances it
//! only through those actions, so zero coverage is a real, known value, and
//! an advertisement above it with nothing committed is exactly the "claimed
//! vs. served" defect. (An implementation that initializes a partition's
//! `complete_through` above 0 must journal that advance; a claim with no
//! journaled advance behind it is precisely what this predicate exists to
//! convict.)
//!
//! # "At serving time": what is enforced, and what is not (ACPR MEDIUM-3)
//!
//! §8.4's sentence quoted above says coverage must have existed **at serving
//! time**. The journals are per-node dense sequences with no cross-node
//! order (D-6), so "at serving time" is only ever decidable from evidence
//! that carries an ordering handle. This module therefore uses two different
//! bounds, and states plainly which is which:
//!
//! - **Reads are compared against the RUN-WIDE maximum coverage** for their
//!   partition — strictly weaker than §8.4 asks. A [`ReadRecord`] carries no
//!   seq, position, or any other ordering handle relative to the journals
//!   (`crate::read_log`'s own limits section says so), so a read that was
//!   served BEFORE the commit that would justify it is, by construction of
//!   the evidence, indistinguishable from one served after. Convicting on
//!   the run-wide bound is the strongest rule this evidence supports; a
//!   tighter one would be guesswork, and a weaker one would let any
//!   optimistic answer through. This is a disclosed gap, not a satisfied
//!   requirement: closing it needs the read log to carry an ordering handle,
//!   which is a change to the query client's wire shape, not to this judge.
//! - **A node's OWN advertisements are compared against that node's OWN
//!   PREFIX** of coverage, plus every other node's run-wide coverage. Here
//!   D-6 does supply the ordering handle: within one node's journal, seq
//!   order IS time order, so a commit the node journaled at a LATER seq
//!   than its own advertisement provably had not happened yet when the
//!   advertisement was made and cannot back it. Acquitting such an
//!   advertisement via the run-wide maximum would discard evidence the
//!   journals already hold ([`Coverage::bound_for_advertisement`]).
//!
//! # The assumption this rests on, stated
//!
//! Reading a bound off the journals means trusting that the journals are
//! COMPLETE — D-6's premise, and the reason a node journals durably before
//! the external call the predicate demands (§8.4). A truncated journal set
//! (a machine that vanished mid-run) would under-state coverage and could
//! therefore convict a fleet that had really committed more than the
//! evidence shows. That failure mode is not this predicate's to paper over
//! by softening its rule: "a node whose journals simply stop … accuses
//! nothing and certifies nothing" is one of §8.4's own `NoVerdict` rules,
//! landing with the vacuity-teeth work (#208), and it belongs there because
//! it applies to every predicate, not just this one. What this module does
//! guard, because it is the realistic shape today, is a run whose journals
//! carry NO watermark value anywhere: that is not "coverage was zero", it is
//! "nobody disclosed coverage at all", and it returns `NoVerdict` before any
//! rule runs.
//!
//! # "Acked before that watermark", without a global clock
//!
//! The journals are per-node dense sequences (D-6) with no cross-node
//! order, and stamping wall-clock times on them would make every verdict a
//! hostage to clock skew. This predicate therefore reads precedence off a
//! SINGLE line: an ack that discloses both the record's own event-time edge
//! (`max_event_time_ms`) and the partition watermark in force when the ack
//! was issued (`complete_through_ms` on the same line) proves, by itself,
//! that the record was acked while the watermark was still behind it. Since
//! the watermark only ever advances
//! (§3: `CommitWm`/`DeclareLoss` are its only writers), any later `complete`
//! read at or above that record's event time must contain it. No
//! cross-journal ordering is assumed anywhere.
//!
//! **Which watermark value that disclosure must carry (ACPR finding
//! LOW-2).** It has to be the OWNER'S AUTHORITATIVE LEDGER value for the
//! partition — `duckspout.watermarks`, which `docs/design/query.md` calls
//! "transactional, authoritative" and which only `LakeCommit` writes, i.e.
//! the `duckspout-watermark` `WatermarkLedger` state the accepting owner
//! holds — and NEVER a cached or registry-sourced copy of it. The registry's
//! watermark row is advisory soft state by construction
//! (`docs/trace-mapping.md`: `ClaimAdvertise` — "Advisory registry row, Keep
//! Rule 8") and may legitimately lag the real watermark; an ack disclosing a
//! LAGGING value would make a legitimate post-watermark straggler look like
//! a record acked ahead of the watermark, and this predicate would then
//! convict a correct fleet for omitting it. The requirement is restated on
//! the field itself (`crate::journal::RequestIdentity::max_event_time_ms`)
//! so a future producer cannot wire the wrong source without reading it.
//!
//! The converse case is deliberately NOT convicted: an ack whose event time
//! is at or below the watermark already in force is a **post-watermark
//! straggler**, which `docs/design/drain.md` §3 puts "by definition outside
//! every `complete` read's contract" — it takes arrival-window placement and
//! a read that omits it is behaving as designed. Acks that disclose no
//! watermark at all establish no precedence and are likewise not evidence.
//!
//! # Declared loss
//!
//! §5.8's ceremony lets `complete_through` advance past a permanently lost
//! range, and `complete` reads over an annulled range then legitimately omit
//! it (§7.6). The loss ledger names `(partition, origin, seq)` ranges, which
//! this evidence cannot map onto a client's record identities, so a
//! partition touched by any `DeclareLoss` is excluded from the
//! missing-record rule rather than convicted on ranges the judge cannot
//! resolve. Its coverage rules still apply — a declared loss excuses missing
//! rows, never an unproven watermark.
//!
//! A `DeclareLoss` line that names NO partition excuses every partition:
//! `DeclareLossRequest` is multi-partition while a journal line carries at
//! most one `(partition, complete_through_ms)` pair, so a partitionless line
//! is a realistic producer shape and "the ceremony happened, this evidence
//! cannot say where" is the only honest reading of it. But an exclusion is
//! not a check (ACPR finding MEDIUM-2): a read whose missing-record rule was
//! switched off does not count toward that rule having run, exactly as
//! `crate::predicates::latest_view` does not count a row it excluded. A run
//! in which EVERY served read was excused therefore reports `NoVerdict` —
//! the rule was disabled fleet-wide and certified nothing — instead of the
//! `Pass` that a single partitionless line could otherwise buy for the whole
//! run.

use std::collections::{BTreeMap, BTreeSet};

use duckspout_types::{NodeId, PartitionId, TraceEvent};

use crate::journal::{JournalSet, RequestIdentity, WatermarkClaim};
use crate::predicates::SYSTEM_TENANT_PREFIX;
use crate::read_log::{ReadConcern, ReadOutcome, ReadRecord};
use crate::verdict::Verdict;

/// The events whose watermark value counts as coverage actually reached
/// (module docs).
pub const COVERAGE_SOURCES: [TraceEvent; 4] = [
    TraceEvent::LakeCommitOk,
    TraceEvent::DeclareLoss,
    TraceEvent::LakeCommitIndeterminate,
    TraceEvent::Reconcile,
];

/// One way a run failed watermark honesty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatermarkFinding {
    /// A node advertised coverage no journaled commit ever established —
    /// the "claimed vs. served" half, convicted from the journals alone.
    UnbackedAdvertisement {
        /// The node that advertised it.
        node: NodeId,
        /// The partition claimed complete.
        partition: PartitionId,
        /// The advertised `complete_through`, Unix milliseconds.
        advertised_ms: i64,
        /// The highest coverage any journaled commit established for that
        /// partition ([`COVERAGE_SOURCES`]).
        coverage_bound_ms: i64,
    },
    /// A `complete` read was SERVED over coverage the journals never show
    /// existing — an optimistic answer, whether or not it happened to be
    /// right.
    UnprovenCoverage {
        /// The partition read.
        partition: PartitionId,
        /// The `complete_through` the answer was served at.
        served_at_ms: i64,
        /// The highest coverage any journaled commit established.
        coverage_bound_ms: i64,
    },
    /// A record acked while the watermark was still behind it was missing
    /// from a `complete` read at or above its event time.
    MissingUnderWatermark {
        /// The partition read.
        partition: PartitionId,
        /// The watermark the answer was served at.
        served_at_ms: i64,
        /// The acked request whose records were missing.
        request_id: String,
        /// The tenant the request was acked for.
        tenant: String,
        /// The record identities (`{source_incarnation}-{index}`) the acked
        /// range covered but the answer did not contain.
        missing_keys: BTreeSet<String>,
    },
}

impl std::fmt::Display for WatermarkFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnbackedAdvertisement {
                node,
                partition,
                advertised_ms,
                coverage_bound_ms,
            } => write!(
                f,
                "node {node} advertised {partition} complete through {advertised_ms}, but the \
                 journals show coverage never above {coverage_bound_ms} — a claim no commit ever \
                 backed"
            ),
            Self::UnprovenCoverage {
                partition,
                served_at_ms,
                coverage_bound_ms,
            } => write!(
                f,
                "a `complete` read of {partition} was served at watermark {served_at_ms}, but the \
                 journals show coverage never above {coverage_bound_ms} — an answer served over \
                 coverage that did not exist is a violation even if it happened to be right (§8.4)"
            ),
            Self::MissingUnderWatermark {
                partition,
                served_at_ms,
                request_id,
                tenant,
                missing_keys,
            } => write!(
                f,
                "request {request_id} (tenant {tenant}) was acked for {partition} while the \
                 watermark was still behind its records, but a `complete` read served at \
                 watermark {served_at_ms} did not contain {missing_keys:?}"
            ),
        }
    }
}

/// This predicate's verdict (§8.4's three-valued contract).
pub type WatermarkHonestyVerdict = Verdict<WatermarkFinding>;

/// One acked batch whose records are provably owed by any `complete` read
/// at or above `max_event_time_ms` (module docs' precedence rule).
struct OwedBatch<'a> {
    identity: &'a RequestIdentity,
    partition: PartitionId,
    max_event_time_ms: i64,
}

impl OwedBatch<'_> {
    /// The `{source_incarnation}-{index}` identity of every record this ack
    /// covered — the same key `crate::final_state` and the read log speak.
    fn record_keys(&self) -> BTreeSet<String> {
        (0..self.identity.record_count as u64)
            .map(|offset| {
                format!(
                    "{}-{}",
                    self.identity.source_incarnation,
                    self.identity.first_index + offset
                )
            })
            .collect()
    }
}

/// One watermark value, with the journal line that carried it — the node
/// and its own dense seq (D-6) are part of the evidence, not diagnostics:
/// they are what makes [`Coverage::bound_for_advertisement`]'s same-node
/// ordering rule decidable (module docs' "at serving time" section).
struct Claim<'a> {
    node: &'a NodeId,
    seq: u64,
    event: TraceEvent,
    watermark: &'a WatermarkClaim,
}

impl Claim<'_> {
    /// True iff this claim is an ADVERTISEMENT — soft state that must be
    /// backed by an advance — rather than the advance itself or an aborted
    /// commit's would-have-been value (module docs' classification).
    fn is_advertisement(&self) -> bool {
        !COVERAGE_SOURCES.contains(&self.event) && self.event != TraceEvent::LakeCommitAbort
    }
}

/// How much of §8.4's sentence this run actually exercised — three separate
/// numbers because the sentence has three separately-skippable rules, and
/// "skipped ≠ passed" applies to each ([`Tally::vacuity`]).
#[derive(Default)]
struct Tally {
    /// Advertisements replayed against journaled coverage.
    advertisements: usize,
    /// Served `complete` reads whose coverage claim was checked.
    reads: usize,
    /// How many of those reads the missing-record rule actually RAN for — a
    /// vacuity counter, not a third check count (module docs' declared-loss
    /// section, ACPR finding MEDIUM-2).
    missing_rule_reads: usize,
}

impl Tally {
    /// The `NoVerdict` this run owes because one of §8.4's rules never ran,
    /// or `None` if all three did. Called only when no finding was recorded,
    /// so every "held" below is a real result rather than an absence.
    fn vacuity(&self) -> Option<WatermarkHonestyVerdict> {
        let Self {
            advertisements,
            reads,
            missing_rule_reads,
        } = *self;
        if advertisements == 0 {
            // The mirror of the reads guard below (ACPR finding MEDIUM-1).
            // §8.4's sentence is "claimed vs. served", and with nothing
            // advertised anywhere the CLAIMED half never ran: coverage
            // derived from commits was never compared against anything a
            // node told anyone. Reporting `Pass` off the served half alone
            // would claim more than was checked, in exactly the way the
            // reads guard already refuses to.
            return Some(Verdict::NoVerdict(format!(
                "{reads} served `complete` read(s) were replayed against the journals, but no \
                 node advertised a watermark value anywhere in this run's evidence — the \
                 claimed-vs-committed half of watermark honesty is uncertified (§8.4: skipped ≠ \
                 passed)"
            )));
        }
        if reads == 0 {
            // §8.4's sentence has two halves, and only one of them ran. The
            // claimed-vs-committed half genuinely held for every
            // advertisement, but nothing was ever SERVED in this run's
            // evidence, so "no `complete` read was served over coverage that
            // did not exist" is uncertified. Reporting `Pass` would state
            // more than was checked; the honest verdict names exactly what
            // did and did not run (§8.4: skipped ≠ passed).
            return Some(Verdict::NoVerdict(format!(
                "{advertisements} advertised watermark value(s) were replayed against the \
                 journals and every one was backed by journaled coverage, but no `complete` read \
                 was served in this run's evidence — the query-side half of watermark honesty is \
                 uncertified"
            )));
        }
        if missing_rule_reads == 0 {
            // Every served read sat in a partition a §5.8 ceremony had
            // touched (or the ceremony named no partition at all, which
            // excuses them all), so the missing-record rule was switched off
            // for every read this run had. A declared loss legitimately
            // excuses missing rows — but a run where it excuses ALL of them
            // proved nothing about them, and must not buy a `Pass` on the
            // strength of the two rules that did run (ACPR finding
            // MEDIUM-2).
            return Some(Verdict::NoVerdict(format!(
                "{advertisements} advertisement(s) and {reads} served `complete` read(s) held, \
                 but every one of those reads was in a partition a §5.8 DeclareLoss had touched, \
                 so the missing-record rule never ran in this run — a declared loss excuses \
                 missing rows, it does not certify that none were missing (§8.4: skipped ≠ \
                 passed)"
            )));
        }
        None
    }
}

/// Runs the predicate against every watermark value in `journals` and every
/// read in `reads` (the query client's log, `crate::read_log`).
#[must_use]
pub fn check(journals: &JournalSet, reads: &[ReadRecord]) -> WatermarkHonestyVerdict {
    let claims: Vec<Claim<'_>> = journals
        .watermark_claims()
        .map(|(line, watermark)| Claim {
            node: &line.node,
            seq: line.seq,
            event: line.event,
            watermark,
        })
        .collect();
    if claims.is_empty() {
        return Verdict::NoVerdict(
            "no journal line carried a watermark value at all — no node ever advanced or \
             advertised coverage, so there is nothing to replay (§8.4 vacuity teeth)"
                .to_owned(),
        );
    }

    let coverage = Coverage::of(&claims);
    let loss = LossScope::of(journals);
    let owed = owed_batches(journals);

    let mut tally = Tally::default();
    let mut findings = Vec::new();

    // 1. Claimed: every advertisement must be backed by journaled coverage.
    for claim in claims.iter().filter(|claim| claim.is_advertisement()) {
        tally.advertisements += 1;
        let bound =
            coverage.bound_for_advertisement(&claim.watermark.partition, claim.node, claim.seq);
        if claim.watermark.complete_through_ms > bound {
            findings.push(WatermarkFinding::UnbackedAdvertisement {
                node: claim.node.clone(),
                partition: claim.watermark.partition.clone(),
                advertised_ms: claim.watermark.complete_through_ms,
                coverage_bound_ms: bound,
            });
        }
    }

    // 2 & 3. Served: coverage must have existed, and everything owed under
    // it must be in the answer.
    for read in reads {
        if read.concern != ReadConcern::Complete {
            continue;
        }
        let ReadOutcome::Served {
            complete_through_ms,
            record_keys,
        } = &read.outcome
        else {
            // A refusal is a correct outcome (§8.4), and proves nothing
            // about honesty — so it is neither a finding nor a check.
            continue;
        };
        tally.reads += 1;
        let bound = coverage.run_wide_bound(&read.partition);
        if *complete_through_ms > bound {
            findings.push(WatermarkFinding::UnprovenCoverage {
                partition: read.partition.clone(),
                served_at_ms: *complete_through_ms,
                coverage_bound_ms: bound,
            });
        }
        if !loss.excuses(&read.partition) {
            tally.missing_rule_reads += 1;
            findings.extend(missing_under_watermark(
                read,
                *complete_through_ms,
                record_keys,
                &owed,
            ));
        }
    }

    if !findings.is_empty() {
        return Verdict::Violation(findings);
    }
    if let Some(vacuous) = tally.vacuity() {
        return vacuous;
    }
    // Every rule ran (the guard above), so this is always a real `Pass`; it
    // still goes through `Verdict::pass` so that "a pass must have checked
    // something" lives in exactly one place for every predicate
    // (`crate::verdict`), never re-derived per call site.
    Verdict::pass(
        tally.advertisements + tally.reads,
        "nothing was checked — this run certifies nothing about watermark honesty (§8.4 vacuity \
         teeth)",
    )
}

/// §3's initial `complete_through`: `wm = [p ∈ Partitions ↦ 0]` (module
/// docs).
const INITIAL_WATERMARK_MS: i64 = 0;

/// What the journals prove coverage actually reached, indexed so BOTH bounds
/// this predicate uses can be read off one structure (module docs' "at
/// serving time" section): partition → node → that node's own
/// [`COVERAGE_SOURCES`] lines, seq-ascending, each paired with the running
/// maximum over that node's lines up to and including it.
///
/// The running maximum is what makes a prefix query a single lookup rather
/// than a rescan, and it is monotone by construction, so
/// [`slice::partition_point`] over the seqs lands directly on the answer.
struct Coverage(BTreeMap<PartitionId, BTreeMap<NodeId, Vec<(u64, i64)>>>);

impl Coverage {
    fn of(claims: &[Claim<'_>]) -> Self {
        let mut per_node: BTreeMap<PartitionId, BTreeMap<NodeId, Vec<(u64, i64)>>> =
            BTreeMap::new();
        for claim in claims
            .iter()
            .filter(|claim| COVERAGE_SOURCES.contains(&claim.event))
        {
            per_node
                .entry(claim.watermark.partition.clone())
                .or_default()
                .entry(claim.node.clone())
                .or_default()
                .push((claim.seq, claim.watermark.complete_through_ms));
        }
        // Ingestion order already puts each node's lines in seq order (D-6's
        // density, enforced by `crate::journal`), but this structure's whole
        // point is that seq order is load-bearing — so it is established
        // here rather than assumed of the caller, and the running maximum is
        // computed after sorting.
        for by_node in per_node.values_mut() {
            for lines in by_node.values_mut() {
                lines.sort_unstable_by_key(|(seq, _)| *seq);
                let mut running = INITIAL_WATERMARK_MS;
                for (_, ms) in lines.iter_mut() {
                    running = running.max(*ms);
                    *ms = running;
                }
            }
        }
        Self(per_node)
    }

    /// The greatest coverage ANY node established for `partition` anywhere
    /// in the run — the bound a served READ is judged against, and the
    /// deliberately weaker-than-§8.4 one (module docs: a [`ReadRecord`]
    /// carries no ordering handle, so "at serving time" is not decidable for
    /// reads from this evidence).
    fn run_wide_bound(&self, partition: &PartitionId) -> i64 {
        self.0
            .get(partition)
            .map_or(INITIAL_WATERMARK_MS, |by_node| {
                by_node
                    .values()
                    .filter_map(|lines| lines.last().map(|(_, ms)| *ms))
                    .max()
                    .unwrap_or(INITIAL_WATERMARK_MS)
            })
    }

    /// The bound `node`'s own advertisement at its journal seq `seq` is
    /// judged against: everything every OTHER node ever committed for
    /// `partition` (no cross-node order exists, so the run-wide value is the
    /// only honest one there), but only what THIS node had committed
    /// strictly BEFORE the line that made the claim.
    ///
    /// That asymmetry is the recovered evidence of ACPR finding MEDIUM-3(b):
    /// within one node's journal, seq order is time order (D-6), so a commit
    /// the node itself journaled at a later seq than its own advertisement
    /// had not happened when the advertisement was made and cannot back it.
    fn bound_for_advertisement(&self, partition: &PartitionId, node: &NodeId, seq: u64) -> i64 {
        let Some(by_node) = self.0.get(partition) else {
            return INITIAL_WATERMARK_MS;
        };
        let others = by_node
            .iter()
            .filter(|(other, _)| *other != node)
            .filter_map(|(_, lines)| lines.last().map(|(_, ms)| *ms))
            .max()
            .unwrap_or(INITIAL_WATERMARK_MS);
        let own_prefix = by_node.get(node).map_or(INITIAL_WATERMARK_MS, |lines| {
            let before = lines.partition_point(|(line_seq, _)| *line_seq < seq);
            if before == 0 {
                INITIAL_WATERMARK_MS
            } else {
                lines[before - 1].1
            }
        });
        others.max(own_prefix)
    }
}

/// Where a §5.8 ceremony may legitimately excuse missing rows (module docs'
/// declared-loss section).
struct LossScope {
    partitions: BTreeSet<PartitionId>,
    /// A `DeclareLoss` line that named no partition: the ceremony happened
    /// and this evidence cannot say where, so every partition is out of
    /// scope for the missing-record rule.
    unknown: bool,
}

impl LossScope {
    fn of(journals: &JournalSet) -> Self {
        let mut scope = Self {
            partitions: BTreeSet::new(),
            unknown: false,
        };
        for line in &journals.lines {
            if line.event == TraceEvent::DeclareLoss {
                match &line.watermark {
                    Some(claim) => {
                        scope.partitions.insert(claim.partition.clone());
                    }
                    None => scope.unknown = true,
                }
            }
        }
        scope
    }

    fn excuses(&self, partition: &PartitionId) -> bool {
        self.unknown || self.partitions.contains(partition)
    }
}

/// Every record one served `complete` answer owed but did not contain
/// (module docs' precedence rule).
fn missing_under_watermark(
    read: &ReadRecord,
    complete_through_ms: i64,
    record_keys: &BTreeSet<String>,
    owed: &[OwedBatch<'_>],
) -> Vec<WatermarkFinding> {
    owed.iter()
        .filter(|batch| {
            batch.partition == read.partition
                && batch.identity.tenant == read.tenant
                && batch.max_event_time_ms <= complete_through_ms
        })
        .filter_map(|batch| {
            let missing: BTreeSet<String> = batch
                .record_keys()
                .into_iter()
                .filter(|key| !record_keys.contains(key))
                .collect();
            (!missing.is_empty()).then(|| WatermarkFinding::MissingUnderWatermark {
                partition: read.partition.clone(),
                served_at_ms: complete_through_ms,
                request_id: batch.identity.request_id.clone(),
                tenant: batch.identity.tenant.clone(),
                missing_keys: missing,
            })
        })
        .collect()
}

/// Every acked batch whose records a later `complete` read provably owes
/// (module docs' precedence rule): the ack must name its partition and its
/// event-time edge, must disclose the watermark in force when it was issued,
/// and that watermark must still be BELOW the batch's event-time edge
/// (otherwise the batch is a post-watermark straggler, outside every
/// `complete` read's contract).
///
/// There is deliberately no check that the disclosed watermark is ABOUT the
/// batch's partition (ACPR finding LOW-1): `RequestIdentity::partition` and
/// `WatermarkClaim::partition` are decoded from the ONE top-level
/// `partition` field of the same flat journal line (`crate::journal`
/// structurally decodes both payloads out of a single line's `rest`), so on
/// real input they are the same value and cannot disagree. A comparison here
/// would be unreachable code that also implies a wire capability — an ack
/// disclosing some OTHER partition's watermark — the format does not have;
/// if such a disclosure is ever wanted, it needs its own field, and then a
/// real check, not a silent no-op standing in for one.
fn owed_batches(journals: &JournalSet) -> Vec<OwedBatch<'_>> {
    journals
        .identity_events(TraceEvent::ClientAck)
        .filter_map(|(line, identity)| {
            let partition = identity.partition.clone()?;
            let max_event_time_ms = identity.max_event_time_ms?;
            let disclosed = line.watermark.as_ref()?;
            if disclosed.complete_through_ms >= max_event_time_ms
                || identity.record_count == 0
                || identity.tenant.starts_with(SYSTEM_TENANT_PREFIX)
            {
                return None;
            }
            Some(OwedBatch {
                identity,
                partition,
                max_event_time_ms,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::journal::JournalLine;

    fn claim_line(
        node: &str,
        seq: u64,
        event: TraceEvent,
        partition: &str,
        ms: i64,
    ) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new(node),
            seq,
            event,
            identity: None,
            watermark: Some(WatermarkClaim {
                partition: PartitionId::new(partition),
                complete_through_ms: ms,
            }),
            changelog: None,
        }
    }

    /// A loadgen `ClientAck` line carrying BOTH payloads: the batch's
    /// identity/coverage and the watermark in force when it was acked.
    fn ack_line(
        seq: u64,
        tenant: &str,
        partition: &str,
        first_index: u64,
        record_count: usize,
        max_event_time_ms: Option<i64>,
        disclosed_watermark_ms: Option<i64>,
    ) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new("loadgen-0"),
            seq,
            event: TraceEvent::ClientAck,
            identity: Some(RequestIdentity {
                request_id: format!("req-{seq}"),
                tenant: tenant.to_owned(),
                record_count,
                first_index,
                source_incarnation: "loadgen-0-1000".to_owned(),
                partition: Some(PartitionId::new(partition)),
                max_event_time_ms,
            }),
            watermark: disclosed_watermark_ms.map(|ms| WatermarkClaim {
                partition: PartitionId::new(partition),
                complete_through_ms: ms,
            }),
            changelog: None,
        }
    }

    fn served(partition: &str, complete_through_ms: i64, keys: &[&str]) -> ReadRecord {
        ReadRecord {
            tenant: "t".to_owned(),
            partition: PartitionId::new(partition),
            concern: ReadConcern::Complete,
            outcome: ReadOutcome::Served {
                complete_through_ms,
                record_keys: keys.iter().map(|k| (*k).to_owned()).collect(),
            },
        }
    }

    fn refused(partition: &str) -> ReadRecord {
        ReadRecord {
            tenant: "t".to_owned(),
            partition: PartitionId::new(partition),
            concern: ReadConcern::Complete,
            outcome: ReadOutcome::Refused {
                reason: "holder unreachable".to_owned(),
            },
        }
    }

    #[test]
    fn an_advertisement_backed_by_a_commit_passes() {
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 1000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p", 1000),
            ],
        };
        // One advertisement checked, plus the served read that makes the
        // query-side half non-vacuous.
        assert_eq!(
            check(&journals, &[served("p", 1000, &[])]),
            Verdict::Pass { checked: 2 }
        );
    }

    #[test]
    fn an_advertisement_above_every_commit_is_convicted() {
        // The claimed-vs-served defect, convicted from the journals alone:
        // the node told the registry it was complete through 2000 when no
        // commit ever took coverage past 1000.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 1000),
                claim_line("n2", 0, TraceEvent::ClaimAdvertise, "p", 2000),
            ],
        };
        match check(&journals, &[]) {
            Verdict::Violation(findings) => {
                assert_eq!(
                    findings,
                    vec![WatermarkFinding::UnbackedAdvertisement {
                        node: NodeId::new("n2"),
                        partition: PartitionId::new("p"),
                        advertised_ms: 2000,
                        coverage_bound_ms: 1000,
                    }]
                );
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn an_advertisement_backed_only_by_an_aborted_commit_is_convicted() {
        // `LakeCommitAbort` is a definitive rejection — nothing changed
        // (§6.5) — so the value it names never became coverage. Would catch
        // a bound computed from "any watermark value seen on any commit
        // event", which would certify this run clean.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitAbort, "p", 5000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p", 5000),
            ],
        };
        assert!(matches!(check(&journals, &[]), Verdict::Violation(_)));
    }

    #[test]
    fn an_advertisement_backed_by_an_indeterminate_commit_is_not_convicted() {
        // The other side of the same coin: an Indeterminate outcome may
        // well have landed (§6.5), so convicting on it would accuse a
        // correct fleet.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitIndeterminate, "p", 5000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p", 5000),
            ],
        };
        assert_eq!(
            check(&journals, &[served("p", 5000, &[])]),
            Verdict::Pass { checked: 2 }
        );
    }

    #[test]
    fn an_advertisement_with_no_commit_at_all_is_convicted_against_the_initial_watermark() {
        // §3 initializes `wm` to 0, so "nothing was ever committed" is a
        // known coverage of zero, not an unknown that excuses any claim.
        let journals = JournalSet {
            lines: vec![claim_line("n1", 0, TraceEvent::ClaimAdvertise, "p", 1)],
        };
        match check(&journals, &[]) {
            Verdict::Violation(findings) => assert!(matches!(
                findings[0],
                WatermarkFinding::UnbackedAdvertisement {
                    coverage_bound_ms: 0,
                    ..
                }
            )),
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn coverage_is_tracked_per_partition() {
        // Would catch a bound accumulated globally instead of per
        // partition: p2's commit must not back p1's claim.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p2", 9000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p1", 9000),
            ],
        };
        assert!(matches!(check(&journals, &[]), Verdict::Violation(_)));
    }

    #[test]
    fn a_read_served_over_unproven_coverage_is_convicted_even_when_the_rows_are_all_there() {
        // "Optimistic answers that happened to be right are still
        // violations if coverage was unproven" (§8.4), stated as a test:
        // the answer contains every acked record, and is still a violation.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 1000),
                ack_line(0, "t", "p", 0, 2, Some(1500), Some(1000)),
            ],
        };
        let reads = vec![served("p", 2000, &["loadgen-0-1000-0", "loadgen-0-1000-1"])];
        match check(&journals, &reads) {
            Verdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                assert!(matches!(
                    findings[0],
                    WatermarkFinding::UnprovenCoverage {
                        served_at_ms: 2000,
                        coverage_bound_ms: 1000,
                        ..
                    }
                ));
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn a_record_acked_below_a_served_watermark_but_missing_from_the_answer_is_convicted() {
        // The core sentence: the batch was acked while the watermark was
        // still at 1000, behind its event-time edge of 1500; a `complete`
        // read served at 2000 must therefore contain both its records.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                ack_line(0, "t", "p", 0, 2, Some(1500), Some(1000)),
            ],
        };
        let reads = vec![served("p", 2000, &["loadgen-0-1000-0"])];
        match check(&journals, &reads) {
            Verdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                match &findings[0] {
                    WatermarkFinding::MissingUnderWatermark { missing_keys, .. } => {
                        assert_eq!(
                            *missing_keys,
                            BTreeSet::from(["loadgen-0-1000-1".to_owned()])
                        );
                    }
                    other => panic!("expected MissingUnderWatermark, got {other:?}"),
                }
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn a_complete_answer_containing_every_owed_record_passes() {
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                ack_line(0, "t", "p", 0, 2, Some(1500), Some(1000)),
            ],
        };
        let reads = vec![served("p", 2000, &["loadgen-0-1000-0", "loadgen-0-1000-1"])];
        assert_eq!(check(&journals, &reads), Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_post_watermark_straggler_is_outside_the_contract_and_not_convicted() {
        // `docs/design/drain.md` §3: a record whose event time was already
        // below the watermark in force when it was acked takes
        // arrival-window placement and is "by definition outside every
        // `complete` read's contract". Convicting it would accuse a
        // correctly-behaving fleet.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                ack_line(0, "t", "p", 0, 1, Some(500), Some(1000)),
            ],
        };
        let reads = vec![served("p", 2000, &[])];
        assert_eq!(check(&journals, &reads), Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_record_acked_above_the_served_watermark_is_not_owed() {
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                ack_line(0, "t", "p", 0, 1, Some(2500), Some(1000)),
            ],
        };
        // Served at 2000; the record's event time is 2500, above it.
        let reads = vec![served("p", 2000, &[])];
        assert_eq!(check(&journals, &reads), Verdict::Pass { checked: 2 });
    }

    #[test]
    fn an_ack_that_disclosed_no_watermark_establishes_no_precedence() {
        // Without the ack-time watermark there is no single-line proof that
        // the record was acked before coverage reached it, and this judge
        // assumes no cross-journal ordering — so it must not convict. The
        // separate `ClaimAdvertise` keeps the claimed-vs-committed half of
        // the run non-vacuous, so what this test asserts is exactly the
        // absence of a conviction and not the advertisement guard.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p", 2000),
                ack_line(0, "t", "p", 0, 1, Some(1500), None),
            ],
        };
        let reads = vec![served("p", 2000, &[])];
        assert_eq!(check(&journals, &reads), Verdict::Pass { checked: 2 });
    }

    #[test]
    fn an_ack_from_a_different_tenant_or_partition_is_not_owed_by_this_read() {
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                claim_line("n1", 1, TraceEvent::LakeCommitOk, "other", 2000),
                ack_line(0, "other-tenant", "p", 0, 1, Some(1500), Some(1000)),
                ack_line(1, "t", "other", 10, 1, Some(1500), Some(1000)),
            ],
        };
        let reads = vec![served("p", 2000, &[])];
        assert_eq!(check(&journals, &reads), Verdict::Pass { checked: 3 });
    }

    #[test]
    fn a_system_tenant_ack_is_never_owed() {
        // §2.2: system tenants receive no durable acks, so there is nothing
        // for a `complete` read to owe them (the same by-definition
        // exclusion `zero_acked_lost` applies).
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                ack_line(0, "_self", "p", 0, 1, Some(1500), Some(1000)),
            ],
        };
        let reads = vec![ReadRecord {
            tenant: "_self".to_owned(),
            partition: PartitionId::new("p"),
            concern: ReadConcern::Complete,
            outcome: ReadOutcome::Served {
                complete_through_ms: 2000,
                record_keys: BTreeSet::new(),
            },
        }];
        assert_eq!(check(&journals, &reads), Verdict::Pass { checked: 2 });
    }

    #[test]
    fn a_declared_loss_excuses_missing_rows_but_not_unproven_coverage() {
        // §5.8/§7.6: after the ceremony a `complete` read over the annulled
        // range legitimately omits rows — but the ceremony is not a licence
        // to serve coverage no commit established. `q` is an untouched
        // partition whose own read exercises the missing-record rule for
        // real, so the excusal on `p` is what this test isolates rather than
        // the fleet-wide vacuity guard below.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                claim_line("n1", 1, TraceEvent::LakeCommitOk, "q", 2000),
                claim_line("n1", 2, TraceEvent::DeclareLoss, "p", 2000),
                ack_line(0, "t", "p", 0, 1, Some(1500), Some(1000)),
                ack_line(1, "t", "q", 10, 1, Some(1500), Some(1000)),
            ],
        };
        // `p`'s owed record is missing and excused; `q`'s is present.
        assert_eq!(
            check(
                &journals,
                &[
                    served("p", 2000, &[]),
                    served("q", 2000, &["loadgen-0-1000-10"]),
                ]
            ),
            Verdict::Pass { checked: 4 }
        );
        // …and `q`'s is convicted when it is missing, proving the excusal is
        // scoped to `p` and not global.
        assert!(matches!(
            check(&journals, &[served("p", 2000, &[]), served("q", 2000, &[])]),
            Verdict::Violation(_)
        ));
        assert!(matches!(
            check(&journals, &[served("p", 9000, &[])]),
            Verdict::Violation(_)
        ));
    }

    /// A `DeclareLoss` line that names no partition at all — the realistic
    /// producer shape, since `DeclareLossRequest` is multi-partition while a
    /// journal line carries at most one `(partition, complete_through_ms)`
    /// pair.
    fn partitionless_declare_loss(node: &str, seq: u64) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new(node),
            seq,
            event: TraceEvent::DeclareLoss,
            identity: None,
            watermark: None,
            changelog: None,
        }
    }

    #[test]
    fn a_declared_loss_naming_no_partition_excuses_every_read_and_is_no_verdict_not_pass() {
        // ACPR finding MEDIUM-2, the reviewer's exact repro: one
        // `LakeCommitOk`, one payloadless `DeclareLoss`, one ack owed under
        // the watermark, one `complete` read missing it.
        //
        // Two facts, and the verdict has to carry both. (1) The exclusion is
        // still right: with the ceremony's scope unknowable, `p`'s missing
        // row is NOT convicted — reaching the vacuity guard at all proves
        // no finding was recorded. (2) But `LossScope::unknown` switches
        // `MissingUnderWatermark` off for EVERY partition in the run, so the
        // rule was never exercised, and a run that certifies nothing about
        // missing records must not report `Pass`. Would catch the excluded
        // read still counting toward the check that the rule ran and buying
        // the whole run a clean exit 0 — the "silently switched off
        // fleet-wide" shape.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                partitionless_declare_loss("n1", 1),
                ack_line(0, "t", "p", 0, 1, Some(1500), Some(1000)),
            ],
        };
        match check(&journals, &[served("p", 2000, &[])]) {
            Verdict::NoVerdict(reason) => assert!(
                reason.contains("missing-record rule never ran"),
                "reason: {reason}"
            ),
            other => panic!(
                "a run whose missing-record rule was disabled fleet-wide must not Pass: {other:?}"
            ),
        }
        // The unproven-coverage rule is untouched by the ceremony, exactly as
        // it is for a partition-scoped loss.
        assert!(matches!(
            check(&journals, &[served("p", 9000, &[])]),
            Verdict::Violation(_)
        ));
    }

    #[test]
    fn a_refusal_is_a_correct_outcome_and_never_a_check() {
        // "Fail-closed refusals are correct outcomes" (§8.4) — and a
        // refusal proves nothing, so a run of nothing but refusals must be
        // NoVerdict, never a Pass built out of them.
        let journals = JournalSet {
            lines: vec![claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 1000)],
        };
        assert!(matches!(
            check(&journals, &[refused("p")]),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn an_available_read_is_not_graded() {
        // §7.6: `available` narrows silently by design, so an incomplete
        // answer under it is correct behaviour — grading it would
        // manufacture violations out of documented semantics.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 1000),
                ack_line(0, "t", "p", 0, 1, Some(500), Some(100)),
            ],
        };
        let reads = vec![ReadRecord {
            tenant: "t".to_owned(),
            partition: PartitionId::new("p"),
            concern: ReadConcern::Available,
            outcome: ReadOutcome::Served {
                complete_through_ms: 9_999_999,
                record_keys: BTreeSet::new(),
            },
        }];
        // The `available` answer claims an absurd watermark and contains
        // none of the acked records, and neither fact produces a finding —
        // it is not graded at all. It is also not counted, so the run's
        // query side stays uncertified.
        match check(&journals, &reads) {
            Verdict::NoVerdict(reason) => assert!(
                reason.contains("no `complete` read was served"),
                "reason: {reason}"
            ),
            other => panic!("an `available` read must be neither graded nor counted: {other:?}"),
        }
    }

    #[test]
    fn commits_alone_with_nothing_advertised_or_served_is_no_verdict() {
        // A run where the fleet drained but nobody ever claimed coverage to
        // anyone and no read was answered certifies nothing about honesty.
        let journals = JournalSet {
            lines: vec![claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 1000)],
        };
        assert!(matches!(check(&journals, &[]), Verdict::NoVerdict(_)));
    }

    #[test]
    fn no_watermark_value_anywhere_is_no_verdict() {
        assert!(matches!(
            check(&JournalSet::default(), &[]),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn a_run_with_served_reads_but_no_advertisement_at_all_is_no_verdict_not_pass() {
        // ACPR finding MEDIUM-1: the mirror of the reads guard. Coverage was
        // established by a commit and a `complete` read was served under it
        // — but no node ever advertised a watermark to anyone, so the
        // claimed-vs-committed half of §8.4's sentence never ran. Would
        // catch the asymmetric guard that reported a clean `Pass` off the
        // served half alone.
        let journals = JournalSet {
            lines: vec![claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000)],
        };
        match check(&journals, &[served("p", 2000, &[])]) {
            Verdict::NoVerdict(reason) => {
                assert!(reason.contains("no node advertised"), "reason: {reason}");
            }
            other => panic!(
                "a run where nothing was ever advertised certifies only half of §8.4: {other:?}"
            ),
        }
        // The symmetric case still behaves as before: an advertisement with
        // no served read is the other half's `NoVerdict`.
        let advertised_only = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 2000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p", 2000),
            ],
        };
        match check(&advertised_only, &[]) {
            Verdict::NoVerdict(reason) => assert!(
                reason.contains("no `complete` read was served"),
                "reason: {reason}"
            ),
            other => panic!("expected the reads-side NoVerdict, got {other:?}"),
        }
    }

    #[test]
    fn a_same_node_advertisement_preceding_its_own_backing_commit_is_convicted() {
        // ACPR finding MEDIUM-3(b), the reviewer's exact repro: n1 advertises
        // p complete through 5000 at its OWN seq 0, and only journals the
        // commit that would establish that coverage at its own seq 1. Within
        // one node's journal seq order IS time order (D-6), so the commit
        // provably had not happened when the claim was made — "at serving
        // time" is decidable here, and acquitting the claim against the
        // run-wide maximum would throw away evidence the journals already
        // hold. Would catch exactly that: a bound taken as
        // `max(coverage over the entire run)` reports `Pass` on this input.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::ClaimAdvertise, "p", 5000),
                claim_line("n1", 1, TraceEvent::LakeCommitOk, "p", 5000),
            ],
        };
        match check(&journals, &[]) {
            Verdict::Violation(findings) => assert_eq!(
                findings,
                vec![WatermarkFinding::UnbackedAdvertisement {
                    node: NodeId::new("n1"),
                    partition: PartitionId::new("p"),
                    advertised_ms: 5000,
                    // Nothing this node had committed BEFORE seq 0, and no
                    // other node committed anything at all.
                    coverage_bound_ms: 0,
                }]
            ),
            other => panic!(
                "an advertisement its own journal puts before its backing commit must be \
                 convicted, got {other:?}"
            ),
        }
        // The non-regression half: the same two lines in the honest order
        // (commit first) are a clean advertisement.
        let honest = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::LakeCommitOk, "p", 5000),
                claim_line("n1", 1, TraceEvent::ClaimAdvertise, "p", 5000),
            ],
        };
        assert_eq!(
            check(&honest, &[served("p", 5000, &[])]),
            Verdict::Pass { checked: 2 }
        );
    }

    #[test]
    fn another_nodes_commit_backs_an_advertisement_whatever_the_seq_order() {
        // The limit of the MEDIUM-3(b) tightening, stated as a test: seqs
        // are dense PER NODE and carry no cross-node order (D-6), so n2's
        // commit at its own seq 1 says nothing about whether it preceded
        // n1's advertisement at n1's seq 0. Convicting here would accuse a
        // correct fleet on an ordering the evidence does not contain — the
        // run-wide bound is the only honest one across nodes. Would catch
        // the prefix rule being applied to every node's coverage instead of
        // only the claiming node's own.
        let journals = JournalSet {
            lines: vec![
                claim_line("n1", 0, TraceEvent::ClaimAdvertise, "p", 5000),
                claim_line("n2", 0, TraceEvent::LakeCommitOk, "other", 0),
                claim_line("n2", 1, TraceEvent::LakeCommitOk, "p", 5000),
            ],
        };
        assert_eq!(
            check(&journals, &[served("p", 5000, &[])]),
            Verdict::Pass { checked: 2 }
        );
    }
}
