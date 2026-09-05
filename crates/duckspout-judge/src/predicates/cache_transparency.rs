//! Cache transparency under eviction storms (§8.4) — the mechanical
//! discharge of §2.4's read-answer equivalence:
//!
//! > with forced Evict/Demote churn and `DropWindow` racing queries, every
//! > `complete` answer is a function of staging ∪ lake alone — any two cache
//! > states, including empty, yield the identical row set. This judge is the
//! > mechanical discharge of §2.4's read-answer equivalence — the half of
//! > the cache-transparency theorem the §3 lemma deliberately does not carry
//! > (§3.4), including obligation (c): no Evict-held lock ever blocks a
//! > read.
//!
//! # Why this judge has to exist at all (§3.4)
//!
//! `specs/formal-core.md`'s `CacheTransparency` is
//! `\A n \in Nodes : \A t \in cache[n] : Rows(t) = LakeRowsOf(t)` — a
//! ROW-IDENTITY lemma about cache-class tables, and its own note says why it
//! is only a lemma: "the theorem quantifies over every `complete` read's
//! *answer*, and §3 has no read action… `Evict` only removes tables from
//! this lemma's quantifier domain and can never violate row-identity;
//! eviction interleavings stress the read-path equivalence, which is §8.4's
//! job, not this formula's." Locks and served answers are not in the §3
//! state space. This module is the other half, and it works on the only
//! evidence that can carry it: real answers, served by a real node, labelled
//! with the cache state that served them.
//!
//! # The two obligations, and their different scopes
//!
//! | Obligation | Subject | Graded over |
//! |---|---|---|
//! | **Read-answer equivalence** (§2.4's theorem) | the ROW SET an answer contained | `complete` reads only |
//! | **(c) No Evict-held lock blocks a read** (§2.4's fourth proof obligation) | whether an answer arrived at all, and how late | reads of ANY concern |
//!
//! The scopes differ on purpose. An `available` read may narrow silently
//! (§7.6), so its row set proves nothing about completeness and comparing
//! two of them would manufacture findings out of documented behaviour. A
//! LOCK, though, does not care what concern a read ran under: if `Evict`
//! took a lock the read path depends on, every read behind it stalls
//! equally. Restricting (c) to `complete` reads would narrow a live check
//! for no reason.
//!
//! # "Two cache states" is not a metaphor
//!
//! A read's cache state is [`CacheProbe::residency_ops_before`] — the count
//! of `Demote`/`Evict`/`DropWindow` lines in the serving node's own D-6
//! journal when the read was issued (`crate::read_log`'s cache-probe docs
//! for why this is measured from outside the node rather than reported by
//! it). Two reads with different counts were served by demonstrably
//! different residency states of that node. Two reads of the same question
//! at the same pinned coverage must then have returned the same rows —
//! that IS "any two cache states yield the identical row set", with the
//! quantifier instantiated at whatever states the run actually produced.
//!
//! **Note what this does and does not cover at v0.2.** `Demote` and `Evict`
//! are journaled by nothing in this workspace: v1's cache class is empty by
//! construction (`docs/design/data-model.md` §2.4 — `DropWindow` at drain
//! commit), and warm residency is deferred behind a measured experiment
//! (`docs/deferred.md`). So a run today varies its residency state through
//! `DropWindow` alone — the transition where a window stops being served
//! from staging and starts being served from the lake, which is exactly
//! obligation (b)'s "staging XOR lake/cache" boundary and exactly the
//! interleaving §3.4 hands to this judge. The predicate is written over the
//! residency counter rather than over `Evict` specifically, so the day the
//! cache class activates it bites harder with no change: an `Evict` line
//! increments the same counter a `DropWindow` line does.
//!
//! # A question is `(tenant, partition, query, complete_through)`
//!
//! All four, and each one earns its place:
//!
//! - **`query`**: two answers are only required to agree when they answer
//!   the same question. Without it a `SELECT count(*)` and a `SELECT *`
//!   would be compared and every fleet convicted.
//! - **`complete_through_ms`**: a `complete` read is pinned to a watermark
//!   (§7.6's per-transaction pinning), and coverage legitimately GROWS
//!   during a run. Two reads at different pinned coverage are two different
//!   questions; comparing them would convict a fleet for draining.
//! - **`tenant`** and **`partition`**: the scoping every other predicate in
//!   this crate already keeps (`crate::predicates::latest_view`'s HIGH-1
//!   note; watermarks are per-partition, §7.3).
//!
//! # Obligation (c)'s evidence is scoped per serving node AND per concern
//!
//! `LockQuestion` — what obligation (c) groups its evidence by — is
//! `(serving_node, tenant, partition, query, concern)`. The last two
//! coordinates were ACPR findings (HIGH-2 and HIGH-1), and each of them was
//! a real false positive rather than a tidiness point:
//!
//! - **`serving_node`.** The residency counter is a count of lines in ONE
//!   node's own D-6 journal ([`CacheProbe::serving_node`]'s doc comment:
//!   "a probe naming a different node's counter would be comparing two
//!   unrelated clocks"). A busy drainer's counter reaching 900 says nothing
//!   about a quiet node whose counter is at 5, so a bracket closed — or a
//!   latency baseline drawn — across two nodes compares two clocks that
//!   never ticked together. Every comparison below therefore happens inside
//!   one node's journal.
//! - **`concern`.** An `available` read narrows silently (§7.6): it keeps
//!   being SERVED at ever-higher residency counts precisely where a
//!   `complete` read of the same question correctly refuses. Letting a
//!   served `available` read close the upper half of a `complete` refusal's
//!   bracket convicts the one thing the bracket exists to acquit — a
//!   legitimate `DeclareLoss` coverage regression (§5.8). A lock still does
//!   not care what concern a read ran under, which is why (c) is GRADED over
//!   reads of any concern; what it cannot do is let one concern's outcome
//!   stand in as the other's evidence.
//!
//! # Obligation (c) is self-calibrating, deliberately
//!
//! A racing read is convicted as BLOCKED only when it is both slower than
//! [`DEFAULT_MAX_RACING_READ_MS`] *and* slower than the NON-racing baseline
//! of the same lock question in the same run. Both halves are needed:
//!
//! - the absolute ceiling alone would convict an inherently slow query for
//!   being slow, which says nothing about locks;
//! - the relative baseline alone would convict on ordinary scheduling
//!   jitter between two millisecond-scale reads.
//!
//! The baseline is the **median** of the unraced served reads' latencies,
//! over at least [`MIN_UNRACED_BASELINE_SAMPLES`] of them — not their
//! maximum, which was ACPR finding MEDIUM-1. A maximum makes the check
//! structurally unable to fail: one cold first read or one GC pause among
//! the unraced samples raises the bar above anything a real lock could
//! produce, and the run then reports a healthy `Pass` with a nonzero check
//! count while a genuinely blocked racing read sits in the evidence. Three
//! is the sample floor because three is exactly where a median stops
//! following a single outlier: with one sample the "median" IS the outlier,
//! with two it is the mean of the pair (an outlier moves it by half), and
//! with three the middle value is unmoved by either end.
//!
//! A racing read whose lock question has no such baseline is NOT judged for
//! (c) — there is no bar, so there is no statement to make — and it is not
//! counted as checked either.
//!
//! The refusal rule is bracketed for the same reason: a racing read that was
//! REFUSED is convicted only when a non-racing read **of the same lock
//! question** was served both at a LOWER and at a HIGHER residency count.
//! Coverage can legitimately regress exactly once, through the `DeclareLoss`
//! ceremony (§5.8) — after which every later read of that question refuses
//! too, so no higher-count served read exists and the bracket does not close.
//! What the bracket does catch is the shape §2.4's corollary forbids: a
//! question the system answers before and after the storm but fails closed
//! on *during* it. "A cache miss can never fail-close a `complete` read."
//!
//! # Contradictory evidence gates the findings that rest on it
//!
//! A run whose probes claim residency churn no node journaled is
//! contradictory evidence, and this predicate refuses to certify it. That
//! gate runs BEFORE obligation (c) is allowed to convict, because both of
//! (c)'s findings rest entirely on [`CacheProbe::raced_residency_action`] —
//! the very claim the zero-residency-actions evidence contradicts (ACPR
//! finding MEDIUM-2). The row-set half is deliberately NOT gated by it: two
//! different answers to one question at one pinned coverage are a violation
//! of §2.4 whatever the residency evidence says, and that finding never
//! consults the race flag at all.

use std::collections::{BTreeMap, BTreeSet};

use duckspout_types::{NodeId, PartitionId};

use crate::journal::JournalSet;
use crate::read_log::{CacheProbe, ReadConcern, ReadOutcome, ReadRecord};
use crate::verdict::Verdict;

/// Default absolute ceiling, in milliseconds, above which a read that raced
/// a residency action is a candidate for obligation (c) — the module docs'
/// self-calibration note explains why exceeding it is necessary but not
/// sufficient.
///
/// Reasoning for `1_000`: the residency actions this bounds are all O(1)
/// metadata operations. `DropWindow` is "the O(1) cleanup after a durable
/// `LakeCommit`" — one `DROP TABLE` plus one registry row delete in a single
/// transaction (`duckspout_staging::engine`'s own docs); `Evict` is "always
/// safe, no coordination… `Evict` = `DROP TABLE`"
/// (`docs/design/data-model.md` §2.4). None of them has a second of work in
/// it, so a read held up for longer than a second while one ran is not
/// explained by the operation's own cost. This is a conservative starting
/// point, not a derived bound; it is exposed as `--max-racing-read-ms`
/// exactly so an operator on slower hardware can raise it with a reason,
/// the same posture `crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION` takes.
pub const DEFAULT_MAX_RACING_READ_MS: u64 = 1_000;

/// How many unraced SERVED reads of one `LockQuestion` are needed before
/// their median is used as obligation (c)'s relative bar (module docs'
/// self-calibration note for why a median rather than a maximum, and why
/// three rather than one).
pub const MIN_UNRACED_BASELINE_SAMPLES: usize = 3;

/// One way a run violated §2.4's read-answer equivalence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheTransparencyFinding {
    /// The theorem itself: one question, one pinned coverage, two answers.
    RowSetDiffers {
        /// The tenant the reads were issued as.
        tenant: String,
        /// The partition they covered.
        partition: PartitionId,
        /// The question both reads asked.
        query: String,
        /// The coverage both answers were pinned to.
        complete_through_ms: i64,
        /// The reference read's cache state.
        reference_ops: u64,
        /// The disagreeing read's cache state.
        other_ops: u64,
        /// Records the reference answer had and the other did not.
        only_in_reference: Vec<String>,
        /// Records the other answer had and the reference did not.
        only_in_other: Vec<String>,
    },
    /// Obligation (c): a read that raced a residency action was held up far
    /// longer than the same query runs when nothing is racing it.
    BlockedRead {
        /// The node that served it — every comparison behind this finding
        /// happened inside that node's own journal (module docs).
        serving_node: NodeId,
        /// The tenant the read was issued as.
        tenant: String,
        /// The partition it covered.
        partition: PartitionId,
        /// The question it asked.
        query: String,
        /// The concern it ran under — evidence is never pooled across
        /// concerns (module docs).
        concern: ReadConcern,
        /// How long it actually took.
        latency_ms: u64,
        /// The median of this same lock question's unraced served reads.
        baseline_ms: u64,
        /// How many unraced served reads that median was taken over.
        baseline_samples: usize,
        /// The absolute ceiling that was also exceeded.
        ceiling_ms: u64,
    },
    /// Obligation (c), the fail-closed half: a read that raced a residency
    /// action was refused, on a question the system answered both before and
    /// after the storm.
    RefusedWhileRacing {
        /// The node that refused it — the bracket that convicts it was
        /// closed inside that node's own journal (module docs).
        serving_node: NodeId,
        /// The tenant the read was issued as.
        tenant: String,
        /// The partition it covered.
        partition: PartitionId,
        /// The question it asked.
        query: String,
        /// The concern it ran under. The bracket is closed only by served
        /// reads of this SAME concern — an `available` read narrows silently
        /// (§7.6) and so can never vouch for a `complete` read's coverage.
        concern: ReadConcern,
        /// The refusal reason, verbatim from the client.
        reason: String,
    },
}

/// Renders a set of diverging record keys for a finding message. Capped:
/// a `complete` answer can carry millions of rows (`crate::read_log`'s own
/// honesty about that), and a finding is a diagnostic line, not a data dump —
/// the count beside it is always exact.
fn render_keys(keys: &[String]) -> String {
    const MAX_LISTED: usize = 8;
    if keys.is_empty() {
        return "none".to_owned();
    }
    let listed = keys
        .iter()
        .take(MAX_LISTED)
        .map(|key| format!("{key:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    if keys.len() > MAX_LISTED {
        format!("{listed}, … ({} more)", keys.len() - MAX_LISTED)
    } else {
        listed
    }
}

impl std::fmt::Display for CacheTransparencyFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheTransparencyFinding::RowSetDiffers {
                tenant,
                partition,
                query,
                complete_through_ms,
                reference_ops,
                other_ops,
                only_in_reference,
                only_in_other,
            } => write!(
                f,
                // The diverging record keys themselves, not only how many:
                // an operator's first question about a transparency breach is
                // WHICH row appeared or vanished with the cache state, and a
                // count sends them back to the raw read log to find out.
                "tenant {tenant} partition {partition} query {query:?} at complete_through \
                 {complete_through_ms}: the answer served at cache state {reference_ops} and the \
                 one served at cache state {other_ops} are NOT the same row set ({} record(s) \
                 only in the first: {}; {} only in the second: {}) — a `complete` answer must be \
                 a function of staging ∪ lake alone (§2.4)",
                only_in_reference.len(),
                render_keys(only_in_reference),
                only_in_other.len(),
                render_keys(only_in_other)
            ),
            CacheTransparencyFinding::BlockedRead {
                serving_node,
                tenant,
                partition,
                query,
                concern,
                latency_ms,
                baseline_ms,
                baseline_samples,
                ceiling_ms,
            } => write!(
                f,
                "node {serving_node} tenant {tenant} partition {partition} query {query:?} \
                 ({concern:?}): a read that raced a residency action took {latency_ms}ms — past \
                 the {ceiling_ms}ms ceiling AND past the {baseline_ms}ms median of this same \
                 question's {baseline_samples} unraced served read(s) on that node — so a \
                 residency action held something this read path depends on (§2.4 obligation (c))"
            ),
            CacheTransparencyFinding::RefusedWhileRacing {
                serving_node,
                tenant,
                partition,
                query,
                concern,
                reason,
            } => write!(
                f,
                "node {serving_node} tenant {tenant} partition {partition} query {query:?} \
                 ({concern:?}): a read that raced a residency action was REFUSED ({reason:?}) on a \
                 question this node answered under this same concern both before and after the \
                 storm — a cache miss can never fail-close a read (§2.4)"
            ),
        }
    }
}

/// This predicate's verdict (§8.4's three-valued contract).
pub type CacheTransparencyVerdict = Verdict<CacheTransparencyFinding>;

/// What names one QUESTION for the equivalence half (module docs).
type Question = (String, PartitionId, String, i64);

/// What names one question for obligation (c):
/// `(serving_node, tenant, partition, query, concern)`.
///
/// No pinned coverage, because a refusal has none to report and a lock does
/// not care about coverage. But the serving node and the concern are both
/// load-bearing, and the module docs' "scoped per serving node AND per
/// concern" section says exactly which false positive each one removes.
type LockQuestion = (NodeId, String, PartitionId, String, ReadConcern);

/// The lock question one probed read belongs to.
fn lock_question(read: &ReadRecord, probe: &CacheProbe) -> LockQuestion {
    (
        probe.serving_node.clone(),
        read.tenant.clone(),
        read.partition.clone(),
        probe.query.clone(),
        read.concern,
    )
}

/// Obligation (c)'s relative bar for one lock question: the median of its
/// unraced served reads' latencies, or `None` when there are fewer than
/// [`MIN_UNRACED_BASELINE_SAMPLES`] of them (module docs).
///
/// Sorts `latencies` in place. For an even sample count the upper median is
/// taken — the conservative side of the pair, since a higher bar is the one
/// that acquits.
fn unraced_baseline(latencies: &mut [u64]) -> Option<u64> {
    if latencies.len() < MIN_UNRACED_BASELINE_SAMPLES {
        return None;
    }
    latencies.sort_unstable();
    latencies.get(latencies.len() / 2).copied()
}

/// Runs the predicate against the run's read log and journals.
///
/// `journals` is not decoration: the read probes CLAIM residency churn, and
/// the journals are where that churn is actually recorded. A run whose
/// probes disagree with every node's journal is contradictory evidence, and
/// [`check`] refuses to certify it either way.
#[must_use]
pub fn check(
    journals: &JournalSet,
    reads: &[ReadRecord],
    max_racing_read_ms: u64,
) -> CacheTransparencyVerdict {
    let probed: Vec<(&ReadRecord, &CacheProbe)> = reads
        .iter()
        .filter_map(|read| read.cache.as_ref().map(|probe| (read, probe)))
        .collect();

    // Contradictory evidence, decided BEFORE obligation (c) is allowed to
    // speak: both of its findings rest on `raced_residency_action()`, and a
    // run in which no node journaled a residency line at all contradicts
    // exactly that claim (module docs' own section; ACPR finding MEDIUM-2).
    // The row-set half below is deliberately not gated — it never consults
    // the race flag, and two different answers to one question at one pinned
    // coverage are a §2.4 violation regardless of what the journals hold.
    let residency_evidence = journals.residency_action_count() > 0;

    let mut findings = Vec::new();
    let (equivalence_checked, mut equivalence_findings) = check_equivalence(&probed);
    findings.append(&mut equivalence_findings);
    let (lock_checked, mut lock_findings) = if residency_evidence {
        check_no_blocking(&probed, max_racing_read_ms)
    } else {
        (0, Vec::new())
    };
    findings.append(&mut lock_findings);

    // A proven violation outranks every vacuity rule below: the honest
    // headline for a convicted run is the conviction, not "inconclusive"
    // (`crate::verdict::combined_exit_code`'s own reasoning, applied within
    // one predicate).
    if !findings.is_empty() {
        return Verdict::Violation(findings);
    }

    if probed.is_empty() {
        return Verdict::NoVerdict(
            "no read in the read log carried a cache probe — without the cache state that served \
             an answer, §2.4's \"any two cache states yield the identical row set\" is not a \
             statement about anything observable here (§8.4 vacuity teeth). No producer writes \
             one yet: crate::read_log's producer-status note."
                .to_owned(),
        );
    }
    if !residency_evidence {
        return Verdict::NoVerdict(
            "the read probes label cache states, but no node journaled a single \
             Demote/Evict/DropWindow line — the probes claim residency churn the run has no \
             record of, which is contradictory evidence, not a certifiable run (§8.4)"
                .to_owned(),
        );
    }
    if equivalence_checked == 0 {
        return Verdict::NoVerdict(
            "no question was answered at two different cache states: every `complete` read this \
             run logged either had no comparable sibling or was served at the one same residency \
             count. §2.4's theorem quantifies over TWO cache states, so this run compared none \
             and certifies nothing about read-answer equivalence (§8.4 vacuity teeth)"
                .to_owned(),
        );
    }
    if lock_checked == 0 {
        return Verdict::NoVerdict(
            "no read that raced a residency action had enough unraced reads of the same lock \
             question — same serving node, same concern — to be judged against, so obligation \
             (c) — no Evict-held lock ever blocks a read — went unexercised. A run that never \
             made a read and an eviction overlap on one node cannot certify that they do not \
             interfere (§8.4 vacuity teeth)"
                .to_owned(),
        );
    }

    Verdict::pass(
        equivalence_checked + lock_checked,
        // Unreachable: both gates above already returned when their own
        // count was zero, so the sum is positive here. `Verdict::pass` is
        // still the constructor used, so this predicate can never grow a
        // path that reports a zero-check `Pass`.
        "nothing was checked",
    )
}

/// The theorem half: within one question, every probed `complete` answer
/// must be the same row set, and at least two distinct cache states must
/// have been compared for the check to count.
fn check_equivalence(
    probed: &[(&ReadRecord, &CacheProbe)],
) -> (usize, Vec<CacheTransparencyFinding>) {
    let mut answers: BTreeMap<Question, Vec<(u64, &BTreeSet<String>)>> = BTreeMap::new();
    for (read, probe) in probed.iter().copied() {
        // `available` may narrow silently (§7.6), so its row set is not
        // evidence about completeness — module docs' scope table.
        if read.concern != ReadConcern::Complete {
            continue;
        }
        if let ReadOutcome::Served {
            complete_through_ms,
            record_keys,
        } = &read.outcome
        {
            answers
                .entry((
                    read.tenant.clone(),
                    read.partition.clone(),
                    probe.query.clone(),
                    *complete_through_ms,
                ))
                .or_default()
                .push((probe.residency_ops_before, record_keys));
        }
    }

    let mut checked = 0usize;
    let mut findings = Vec::new();
    for ((tenant, partition, query, complete_through_ms), mut group) in answers {
        // Lowest cache state first, so the reference is the state closest to
        // "before the storm" and a finding reads in the run's own order.
        group.sort_by_key(|(ops, _)| *ops);
        let Some(((reference_ops, reference_keys), rest)) = group.split_first() else {
            continue;
        };
        for (other_ops, other_keys) in rest {
            // Only a CROSS-STATE comparison discharges the theorem; two
            // answers at the identical residency count say nothing about
            // "any two cache states". They are still compared — one question
            // at one coverage having two answers is a violation whatever
            // caused it — but they do not earn a check.
            if other_ops != reference_ops {
                checked += 1;
            }
            if other_keys != reference_keys {
                findings.push(CacheTransparencyFinding::RowSetDiffers {
                    tenant: tenant.clone(),
                    partition: partition.clone(),
                    query: query.clone(),
                    complete_through_ms,
                    reference_ops: *reference_ops,
                    other_ops: *other_ops,
                    only_in_reference: reference_keys.difference(other_keys).cloned().collect(),
                    only_in_other: other_keys.difference(reference_keys).cloned().collect(),
                });
            }
        }
    }
    (checked, findings)
}

/// Obligation (c): a residency action must not hold anything a read depends
/// on (module docs' self-calibration note for both rules).
fn check_no_blocking(
    probed: &[(&ReadRecord, &CacheProbe)],
    ceiling_ms: u64,
) -> (usize, Vec<CacheTransparencyFinding>) {
    // Baselines, from the reads that raced nothing, grouped by
    // `LockQuestion` — so every comparison below stays inside one node's
    // journal and one read concern (module docs). Only SERVED unraced reads
    // are baselines: a refusal's latency is the cost of failing closed, not
    // the cost of answering, so using one as the bar a served read must beat
    // would compare two different operations — and, since a refusal is
    // typically the faster of the two, would bias toward convicting.
    let mut unraced_latencies: BTreeMap<LockQuestion, Vec<u64>> = BTreeMap::new();
    let mut served_unraced_ops: BTreeMap<LockQuestion, Vec<u64>> = BTreeMap::new();
    for (read, probe) in probed.iter().copied() {
        if probe.raced_residency_action() || !matches!(read.outcome, ReadOutcome::Served { .. }) {
            continue;
        }
        let question = lock_question(read, probe);
        unraced_latencies
            .entry(question.clone())
            .or_default()
            .push(probe.latency_ms);
        served_unraced_ops
            .entry(question)
            .or_default()
            .push(probe.residency_ops_before);
    }
    let baselines: BTreeMap<LockQuestion, (u64, usize)> = unraced_latencies
        .into_iter()
        .filter_map(|(question, mut latencies)| {
            let samples = latencies.len();
            unraced_baseline(&mut latencies).map(|median| (question, (median, samples)))
        })
        .collect();

    let mut checked = 0usize;
    let mut findings = Vec::new();
    for (read, probe) in probed.iter().copied() {
        if !probe.raced_residency_action() {
            continue;
        }
        let question = lock_question(read, probe);
        match &read.outcome {
            ReadOutcome::Served { .. } => {
                // Too few unraced siblings on this node under this concern,
                // no baseline, no statement (module docs) — and no check
                // counted for it either.
                let Some((baseline_ms, baseline_samples)) = baselines.get(&question).copied()
                else {
                    continue;
                };
                checked += 1;
                if probe.latency_ms > ceiling_ms && probe.latency_ms > baseline_ms {
                    findings.push(CacheTransparencyFinding::BlockedRead {
                        serving_node: probe.serving_node.clone(),
                        tenant: read.tenant.clone(),
                        partition: read.partition.clone(),
                        query: probe.query.clone(),
                        concern: read.concern,
                        latency_ms: probe.latency_ms,
                        baseline_ms,
                        baseline_samples,
                        ceiling_ms,
                    });
                }
            }
            ReadOutcome::Refused { reason } => {
                // The bracket (module docs): served unraced reads of this
                // SAME lock question, both below and above this read's cache
                // state. Without both sides, a legitimate one-way coverage
                // regression is indistinguishable from a cache-caused
                // fail-close.
                let Some(ops) = served_unraced_ops.get(&question) else {
                    continue;
                };
                let bracketed = ops.iter().any(|o| *o <= probe.residency_ops_before)
                    && ops.iter().any(|o| *o >= probe.residency_ops_after);
                if !bracketed {
                    continue;
                }
                checked += 1;
                findings.push(CacheTransparencyFinding::RefusedWhileRacing {
                    serving_node: probe.serving_node.clone(),
                    tenant: read.tenant.clone(),
                    partition: read.partition.clone(),
                    query: probe.query.clone(),
                    concern: read.concern,
                    reason: reason.clone(),
                });
            }
        }
    }
    (checked, findings)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use duckspout_types::{NodeId, TraceEvent};

    use super::*;
    use crate::journal::JournalLine;

    const QUERY: &str = "SELECT count(*) FROM duckspout_windows";
    /// The single node every fixture serves from unless it says otherwise.
    const NODE_A: &str = "fleet-0-1/1";
    /// A SECOND node, for the fixtures that exist because a residency counter
    /// is a per-node clock (module docs; ACPR finding HIGH-2).
    const NODE_B: &str = "fleet-0-2/1";

    /// A run whose journals record real residency churn — the cross-check
    /// every probed run has to survive.
    fn churning_journals(actions: usize) -> JournalSet {
        JournalSet {
            lines: (0..actions)
                .map(|i| JournalLine {
                    source: PathBuf::from("test"),
                    line_no: i + 1,
                    node: NodeId::new("n1"),
                    seq: i as u64,
                    event: TraceEvent::DropWindow,
                    identity: None,
                    watermark: None,
                    changelog: None,
                    part: None,
                })
                .collect(),
        }
    }

    #[derive(Clone, Copy)]
    struct Probe {
        concern: ReadConcern,
        ops_before: u64,
        ops_after: u64,
        latency_ms: u64,
        query: &'static str,
        node: &'static str,
    }

    impl Probe {
        /// A `complete` read on [`NODE_A`] that raced nothing, at cache
        /// state `ops`.
        fn at(ops: u64) -> Self {
            Self {
                concern: ReadConcern::Complete,
                ops_before: ops,
                ops_after: ops,
                latency_ms: 5,
                query: QUERY,
                node: NODE_A,
            }
        }

        /// A read that overlapped one residency action.
        fn racing(ops: u64, latency_ms: u64) -> Self {
            Self {
                ops_after: ops + 1,
                latency_ms,
                ..Self::at(ops)
            }
        }

        /// The same probe, served by a different node.
        fn on(self, node: &'static str) -> Self {
            Self { node, ..self }
        }

        /// The same probe, issued under `available` (§7.6).
        fn available(self) -> Self {
            Self {
                concern: ReadConcern::Available,
                ..self
            }
        }

        /// The same probe, at a different latency.
        fn taking(self, latency_ms: u64) -> Self {
            Self { latency_ms, ..self }
        }
    }

    fn probe_of(probe: Probe) -> CacheProbe {
        CacheProbe {
            query: probe.query.to_owned(),
            serving_node: NodeId::new(probe.node),
            residency_ops_before: probe.ops_before,
            residency_ops_after: probe.ops_after,
            latency_ms: probe.latency_ms,
        }
    }

    fn served(probe: Probe, complete_through_ms: i64, keys: &[&str]) -> ReadRecord {
        ReadRecord {
            tenant: "tenant-a".to_owned(),
            partition: PartitionId::new("t0-s0"),
            concern: probe.concern,
            outcome: ReadOutcome::Served {
                complete_through_ms,
                record_keys: keys.iter().map(|k| (*k).to_owned()).collect(),
            },
            cache: Some(probe_of(probe)),
        }
    }

    fn refused(probe: Probe, reason: &str) -> ReadRecord {
        ReadRecord {
            tenant: "tenant-a".to_owned(),
            partition: PartitionId::new("t0-s0"),
            concern: probe.concern,
            outcome: ReadOutcome::Refused {
                reason: reason.to_owned(),
            },
            cache: Some(probe_of(probe)),
        }
    }

    /// The shape a healthy storm produces: one question, three cache states,
    /// one row set, plus a racing read with a full unraced baseline
    /// ([`MIN_UNRACED_BASELINE_SAMPLES`] served siblings on the same node
    /// under the same concern) so obligation (c) is exercised too.
    fn healthy_run() -> Vec<ReadRecord> {
        vec![
            served(Probe::at(0), 1_000, &["r-0", "r-1"]),
            served(Probe::at(3), 1_000, &["r-0", "r-1"]),
            served(Probe::at(7), 1_000, &["r-0", "r-1"]),
            served(Probe::racing(7, 6), 1_000, &["r-0", "r-1"]),
        ]
    }

    #[test]
    fn identical_answers_across_two_cache_states_pass() {
        let verdict = check(
            &churning_journals(9),
            &healthy_run(),
            DEFAULT_MAX_RACING_READ_MS,
        );
        // Three cross-state comparisons (states 3, 7 and 7-racing against the
        // state-0 reference) plus one obligation-(c) judgment.
        assert!(matches!(verdict, Verdict::Pass { .. }), "got {verdict:?}");
        assert_eq!(verdict.exit_code(), 0);
    }

    #[test]
    fn a_row_lost_at_a_later_cache_state_is_a_violation() {
        // The theorem, convicted: the same question at the same pinned
        // coverage lost a record once the residency state changed, so the
        // answer was a function of the cache, not of staging ∪ lake.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0", "r-1"]),
            served(Probe::at(7), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::Violation(findings) => match findings.as_slice() {
                [
                    CacheTransparencyFinding::RowSetDiffers {
                        only_in_reference,
                        only_in_other,
                        ..
                    },
                ] => {
                    assert_eq!(only_in_reference, &["r-1".to_owned()]);
                    assert!(only_in_other.is_empty());
                }
                other => panic!("expected one RowSetDiffers, got {other:?}"),
            },
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_row_that_appears_only_at_a_later_cache_state_is_equally_a_violation() {
        // The equivalence is an EQUALITY, not a superset relation: an
        // answer that gained a row once the cache changed is just as much a
        // function of the cache as one that lost a row.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(7), 1_000, &["r-0", "r-1"]),
        ];
        assert!(matches!(
            check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS),
            Verdict::Violation(_)
        ));
    }

    #[test]
    fn two_reads_at_the_same_cache_state_compare_no_states_and_certify_nothing() {
        // Would catch a `checked` count that treated any two reads as a
        // discharge of a theorem quantified over two cache STATES.
        let reads = vec![
            served(Probe::at(3), 1_000, &["r-0"]),
            served(Probe::at(3), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn answers_at_different_pinned_coverage_are_different_questions() {
        // Coverage grows as the run drains, so two answers pinned at
        // different watermarks are legitimately different row sets. Would
        // catch a judge that convicted a fleet for draining.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(7), 2_000, &["r-0", "r-1"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        // Never a violation — and, having compared no two states within one
        // question, never a pass either.
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn two_different_queries_are_never_compared_with_each_other() {
        let mut reads = vec![served(Probe::at(0), 1_000, &["r-0"])];
        reads.push(served(
            Probe {
                query: "SELECT 1",
                ..Probe::at(7)
            },
            1_000,
            &[],
        ));
        assert!(matches!(
            check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn available_reads_row_sets_are_not_graded_for_equivalence() {
        // `available` narrows silently (§7.6): comparing two of its answers
        // would manufacture findings out of documented behaviour.
        let reads = vec![
            served(
                Probe {
                    concern: ReadConcern::Available,
                    ..Probe::at(0)
                },
                1_000,
                &["r-0", "r-1"],
            ),
            served(
                Probe {
                    concern: ReadConcern::Available,
                    ..Probe::at(7)
                },
                1_000,
                &["r-0"],
            ),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn a_racing_read_held_past_both_bars_is_a_blocked_read() {
        // Obligation (c): the unraced baseline for this query is a 5ms
        // median over three samples, and the racing read took 4s — past the
        // ceiling and two orders of magnitude past what the query costs when
        // nothing is evicting.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(3), 1_000, &["r-0"]),
            served(Probe::at(7), 1_000, &["r-0"]),
            served(Probe::racing(7, 4_000), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::Violation(findings) => assert!(
                findings
                    .iter()
                    .any(|f| matches!(f, CacheTransparencyFinding::BlockedRead { .. })),
                "got {findings:?}"
            ),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn an_inherently_slow_query_is_not_convicted_for_being_slow() {
        // Self-calibration (module docs): every read of this query takes
        // ~4s, racing or not, so nothing about the racing one implicates a
        // lock. Would catch an absolute-ceiling-only rule.
        let reads = vec![
            served(Probe::at(0).taking(4_000), 1_000, &["r-0"]),
            served(Probe::at(3).taking(4_050), 1_000, &["r-0"]),
            served(Probe::at(7).taking(4_100), 1_000, &["r-0"]),
            served(Probe::racing(7, 4_040), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::Pass { .. }), "got {verdict:?}");
    }

    #[test]
    fn one_slow_unraced_read_does_not_excuse_a_genuinely_blocked_racing_read() {
        // ACPR finding MEDIUM-1, as a permanent regression test. The unraced
        // reads are 5ms, 5ms, 5ms and ONE 30s outlier — a cold first read or
        // a GC pause. Under the old `max` baseline that outlier raised the
        // bar above the 30s racing read and the run reported a healthy
        // `Pass` with a nonzero check count; the median is unmoved by it, so
        // the blocked read is convicted. Would catch any baseline statistic
        // a single unraced sample can drag past the ceiling.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(1), 1_000, &["r-0"]),
            served(Probe::at(2), 1_000, &["r-0"]),
            served(Probe::at(3).taking(30_000), 1_000, &["r-0"]),
            served(Probe::racing(3, 30_000), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::Violation(findings) => assert!(
                findings
                    .iter()
                    .any(|f| matches!(f, CacheTransparencyFinding::BlockedRead { .. })),
                "got {findings:?}"
            ),
            other => panic!("expected a BlockedRead violation, got {other:?}"),
        }
    }

    #[test]
    fn too_few_unraced_samples_are_no_baseline_at_all() {
        // The median only stops following a single outlier at three samples
        // (module docs), so two unraced reads are not a bar — and a racing
        // read judged against a two-sample "median" would be judged against
        // whichever of the two happened to be slower. No baseline, no
        // statement, no check counted, no `Pass`.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(3), 1_000, &["r-0"]),
            served(Probe::racing(3, 30_000), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn millisecond_jitter_past_the_baseline_is_not_convicted() {
        // The other half of self-calibration: 9ms beats the 5ms baseline but
        // is nowhere near the ceiling, and convicting it would make this
        // predicate fire on ordinary scheduling noise. Would catch a
        // baseline-only rule.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(3), 1_000, &["r-0"]),
            served(Probe::at(7), 1_000, &["r-0"]),
            served(Probe::racing(7, 9), 1_000, &["r-0"]),
        ];
        assert!(matches!(
            check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS),
            Verdict::Pass { .. }
        ));
    }

    #[test]
    fn a_racing_read_with_no_unraced_baseline_is_not_judged_for_locks() {
        // No baseline, no statement — and no check counted, so the run
        // cannot pass on the strength of an unjudged read.
        let reads = vec![
            served(Probe::racing(0, 9_000), 1_000, &["r-0"]),
            served(Probe::racing(7, 9_000), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn a_refusal_bracketed_by_served_reads_convicts_the_fail_close() {
        // §2.4's corollary: the system answers this question at cache state
        // 0 and again at state 9, and fails closed only while a residency
        // action is in flight. A cache miss can never fail-close a read.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(9), 1_000, &["r-0"]),
            refused(Probe::racing(5, 5), "cache miss"),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::Violation(findings) => assert!(
                findings
                    .iter()
                    .any(|f| matches!(f, CacheTransparencyFinding::RefusedWhileRacing { .. })),
                "got {findings:?}"
            ),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_with_no_later_served_read_is_a_coverage_regression_not_a_fail_close() {
        // The bracket's upper half is missing: every read after cache state
        // 5 refuses too, which is what a `DeclareLoss` coverage regression
        // (§5.8) looks like — a legitimate, disclosed weakening, not a
        // cache-caused fail-close. Would catch a rule that convicted on the
        // earlier served read alone.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(1), 1_000, &["r-0"]),
            refused(Probe::racing(5, 5), "declared loss"),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        // No conviction; and with no obligation-(c) judgment made at all,
        // no pass either.
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn a_refused_unraced_read_is_not_used_as_a_latency_baseline() {
        // A refusal's latency is the cost of failing closed, not of
        // answering (the baseline's own note). This query genuinely costs
        // ~4s to answer, so the 2s racing read is well inside its 4s median
        // and must not be convicted. Would catch a baseline that POOLED the
        // four 1ms refusals in: the pooled seven-sample median is 1ms, and
        // the racing read then clears both bars and is convicted — a fleet
        // convicted on a comparison between two different operations.
        let reads = vec![
            served(Probe::at(0).taking(4_000), 1_000, &["r-0"]),
            served(Probe::at(3).taking(4_000), 1_000, &["r-0"]),
            served(Probe::at(7).taking(4_000), 1_000, &["r-0"]),
            refused(Probe::at(7).taking(1), "unrelated refusal"),
            refused(Probe::at(7).taking(1), "unrelated refusal"),
            refused(Probe::at(7).taking(1), "unrelated refusal"),
            refused(Probe::at(7).taking(1), "unrelated refusal"),
            served(Probe::racing(7, 2_000), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::Pass { .. }), "got {verdict:?}");
    }

    #[test]
    fn an_unprobed_read_log_certifies_nothing() {
        let mut read = served(Probe::at(0), 1_000, &["r-0"]);
        read.cache = None;
        let verdict = check(&churning_journals(9), &[read], DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::NoVerdict(reason) => assert!(reason.contains("cache probe"), "{reason}"),
            other => panic!("expected NoVerdict, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_read_log_certifies_nothing() {
        assert!(matches!(
            check(&churning_journals(9), &[], DEFAULT_MAX_RACING_READ_MS),
            Verdict::NoVerdict(_)
        ));
    }

    #[test]
    fn probes_claiming_churn_no_journal_recorded_are_contradictory_evidence() {
        // The cross-check: the probes label two cache states, but not one
        // node journaled a Demote/Evict/DropWindow line. Something is wrong
        // with the evidence itself, and a judge must not certify a run it
        // cannot make sense of.
        let verdict = check(
            &churning_journals(0),
            &healthy_run(),
            DEFAULT_MAX_RACING_READ_MS,
        );
        match verdict {
            Verdict::NoVerdict(reason) => {
                assert!(reason.contains("contradictory evidence"), "{reason}");
            }
            other => panic!("expected NoVerdict, got {other:?}"),
        }
    }

    #[test]
    fn a_violation_outranks_every_vacuity_rule() {
        // A convicted run must report the conviction, not "inconclusive" —
        // even when it also failed a vacuity gate (here: no journals at
        // all). Would catch a gate ordering that hid a real finding behind
        // an evidence complaint.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0", "r-1"]),
            served(Probe::at(7), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(0), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::Violation(_)));
        assert_eq!(verdict.exit_code(), 2);
    }

    #[test]
    fn obligation_c_grades_reads_of_any_concern() {
        // A lock does not care what concern a read ran under (module docs'
        // scope table). Would catch a `complete`-only filter applied to the
        // lock half, which would narrow a live check for no reason. The
        // `available` reads carry their OWN baseline, because obligation
        // (c)'s evidence is never pooled across concerns (HIGH-1).
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(7), 1_000, &["r-0"]),
            served(Probe::at(0).available(), 1_000, &["r-0"]),
            served(Probe::at(3).available(), 1_000, &["r-0"]),
            served(Probe::at(7).available(), 1_000, &["r-0"]),
            served(Probe::racing(7, 8_000).available(), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::Violation(findings) => assert!(
                findings.iter().any(|f| matches!(
                    f,
                    CacheTransparencyFinding::BlockedRead {
                        concern: ReadConcern::Available,
                        ..
                    }
                )),
                "got {findings:?}"
            ),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_served_available_read_never_closes_a_complete_refusals_bracket() {
        // ACPR finding HIGH-1, as a permanent regression test. This is the
        // exact shape the bracket exists to ACQUIT: coverage regressed once
        // through the `DeclareLoss` ceremony (§5.8), so every later
        // `complete` read of the question refuses — but an `available` read
        // keeps being served, because §7.6 says it narrows silently instead
        // of failing closed. Pooling that served `available` read into the
        // bracket closes its upper half and convicts the legitimate
        // regression. Would catch a `LockQuestion` with no concern
        // dimension.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(1), 1_000, &["r-0"]),
            refused(Probe::racing(5, 5), "declared loss"),
            served(Probe::at(9).available(), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn another_nodes_residency_counter_never_closes_a_bracket() {
        // ACPR finding HIGH-2, as a permanent regression test. Node A is a
        // busy drainer whose counter has reached 900; node B refuses while
        // racing at its own counter's 5→6. The counters are counts of lines
        // in two different journals (`CacheProbe::serving_node`'s own doc
        // comment), so node A's 900 is not "after" node B's 6 in any sense —
        // and with any second node in the pool there is always some higher
        // count somewhere, which is why HIGH-1's defence does not hold
        // without this one. Would catch a bracket closed across nodes.
        let reads = vec![
            served(Probe::at(0).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(900).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(0).on(NODE_B), 1_000, &["r-0"]),
            refused(Probe::racing(5, 5).on(NODE_B), "declared loss"),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::NoVerdict(_)), "got {verdict:?}");
    }

    #[test]
    fn a_bracket_closed_inside_one_nodes_own_journal_still_convicts() {
        // The other side of HIGH-2: node scoping must not disarm the rule it
        // narrows. Node B answers this question at its own cache states 0
        // and 9 and fails closed only in between, which is exactly §2.4's
        // corollary — and node A's unrelated traffic changes nothing.
        let reads = vec![
            served(Probe::at(400).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(0).on(NODE_B), 1_000, &["r-0"]),
            served(Probe::at(9).on(NODE_B), 1_000, &["r-0"]),
            refused(Probe::racing(5, 5).on(NODE_B), "cache miss"),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::Violation(findings) => assert!(
                findings.iter().any(|f| matches!(
                    f,
                    CacheTransparencyFinding::RefusedWhileRacing { serving_node, .. }
                        if serving_node.as_str() == NODE_B
                )),
                "got {findings:?}"
            ),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn another_nodes_unraced_reads_are_not_a_latency_baseline() {
        // HIGH-2's latency half. Node A answers this query in 5ms and is the
        // busier of the two; node B is on slower hardware and answers it in
        // 4s, racing or not. Pooling the two nodes' samples produces a 5ms
        // median that convicts node B's perfectly ordinary racing read.
        // Scoped per node, node B has its own 4s median and is not convicted
        // — and node B's three unraced reads are what make the check count
        // at all.
        let reads = vec![
            served(Probe::at(0).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(1).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(2).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(3).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(4).on(NODE_A), 1_000, &["r-0"]),
            served(Probe::at(0).on(NODE_B).taking(4_000), 1_000, &["r-0"]),
            served(Probe::at(1).on(NODE_B).taking(4_000), 1_000, &["r-0"]),
            served(Probe::at(2).on(NODE_B).taking(4_000), 1_000, &["r-0"]),
            served(Probe::racing(2, 3_900).on(NODE_B), 1_000, &["r-0"]),
        ];
        let verdict = check(&churning_journals(9), &reads, DEFAULT_MAX_RACING_READ_MS);
        assert!(matches!(verdict, Verdict::Pass { .. }), "got {verdict:?}");
    }

    #[test]
    fn an_obligation_c_finding_needs_residency_evidence_to_rest_on() {
        // ACPR finding MEDIUM-2, as a permanent regression test. Not one
        // node journaled a Demote/Evict/DropWindow line, yet a probe claims
        // it raced one. Both obligation-(c) findings rest entirely on that
        // claim, so the contradictory-evidence gate has to be reached BEFORE
        // they can convict. Would catch the gate being ordered after an
        // early `Violation` return.
        let reads = vec![
            served(Probe::at(0), 1_000, &["r-0"]),
            served(Probe::at(9), 1_000, &["r-0"]),
            refused(Probe::racing(5, 5), "cache miss"),
        ];
        let verdict = check(&churning_journals(0), &reads, DEFAULT_MAX_RACING_READ_MS);
        match verdict {
            Verdict::NoVerdict(reason) => {
                assert!(reason.contains("contradictory evidence"), "{reason}");
            }
            other => panic!("expected NoVerdict, got {other:?}"),
        }
    }

    /// [`render_keys`]' three branches, each of which a finding message
    /// depends on and none of which had a test (an ACPR finding). The
    /// diverging keys are in the finding text because an operator's first
    /// question about a transparency breach is WHICH row moved; a count sends
    /// them back to the raw read log to find out.
    #[test]
    fn diverging_keys_are_listed_exactly_until_the_cap_and_counted_past_it() {
        // Empty: the side of the diff that has nothing in it still has to
        // render as something readable, since both sides are always printed.
        assert_eq!(render_keys(&[]), "none");

        // At and below the cap: every key listed, no elision.
        let eight: Vec<String> = (0..8).map(|i| format!("r-{i}")).collect();
        let listed = render_keys(&eight);
        assert!(listed.contains("\"r-0\""), "{listed}");
        assert!(listed.contains("\"r-7\""), "{listed}");
        assert!(!listed.contains("more)"), "{listed}");

        // Past it: the first eight, then an EXACT remainder count — the
        // count is the part an operator can act on, so it is never elided.
        let nine: Vec<String> = (0..9).map(|i| format!("r-{i}")).collect();
        let capped = render_keys(&nine);
        assert!(capped.contains("\"r-7\""), "{capped}");
        assert!(!capped.contains("\"r-8\""), "{capped}");
        assert!(capped.ends_with("… (1 more)"), "{capped}");

        let many: Vec<String> = (0..1_000).map(|i| format!("r-{i}")).collect();
        assert!(
            render_keys(&many).ends_with("… (992 more)"),
            "the remainder count is exact, not rounded or capped"
        );
    }
}
