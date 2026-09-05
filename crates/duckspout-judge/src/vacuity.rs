//! §8.4's four RUN-LEVEL vacuity rules (issue #208) — the teeth that sit on
//! top of each predicate's own per-obligation vacuity guards.
//!
//! > **Vacuity teeth.** A judge that never rejects anything is
//! > indistinguishable from one too weak to reject anything […] NoVerdict
//! > rules include: a fault schedule that armed faults and fired none
//! > (measured from each injector's own ledger, not assumed from the
//! > profile); a run with no observed cross-node contention when contention
//! > is what the run exists to certify; an ambiguous-outcome fraction above
//! > the profile's ceiling; a node whose journals simply stop (a vanished
//! > machine is exactly the under-reported-loss shape, so it accuses nothing
//! > and certifies nothing).
//!
//! # Two layers, deliberately not collapsed into one
//!
//! Every predicate already downgrades ITSELF when its own evidence is absent
//! (`crate::verdict::Verdict::pass`'s `checked == 0` guard, plus each
//! predicate's per-obligation guards). Those answer "could THIS invariant be
//! checked?" The rules here answer a different question — "was there a RUN
//! here worth checking anything against?" — and a run can fail the second
//! while passing every instance of the first: five predicates can each find
//! real evidence and pass over it while the fault schedule fired nothing, no
//! two nodes ever interacted, and a machine vanished at minute two. That run
//! must not exit `0`, and nothing at the predicate layer would stop it.
//!
//! # One composition rule, not two
//!
//! These rules do NOT get a bespoke combination: each produces an ordinary
//! [`Verdict`] under a `vacuity/…` name, and `crate::runner` feeds them to
//! `crate::verdict::combined_exit_code` in the same slice as the five
//! predicates. The whole run is therefore graded by one function, with one
//! ordering, stated once:
//!
//! - any **Violation** anywhere → `2`;
//! - else any **`NoVerdict`** anywhere — a predicate's or one of these rules' —
//!   → `3`;
//! - else `0`.
//!
//! Note what the first clause means here, because it is a deliberate choice
//! and not an oversight: a run that is vacuous by these rules AND contains a
//! proven violation exits `2`. A journaled `ClientAck` whose record is
//! missing from the final system is a fact about the code, and it stays a
//! fact whether or not the fault schedule fired; downgrading it to "we can't
//! say" would be the judge losing evidence it actually has.
//! `combined_exit_code`'s own docs made that ordering ("a proven violation
//! anywhere outranks an inconclusive predicate elsewhere") the rule for
//! predicates already — this module is what made the loadgen run-summary
//! check obey it too, which as a pre-predicate short-circuit it previously
//! did not.
//!
//! # These rules never convict
//!
//! Every rule here returns `Pass` or `NoVerdict` and never `Violation`.
//! Vacuity is the absence of evidence, and absence of evidence convicts
//! nobody — §8.4's own framing ("it accuses nothing and certifies nothing").
//! A fault that never fired is not a bug in `DuckSpout`; it is a reason this
//! run cannot be used to argue `DuckSpout` is correct.
//!
//! # Disclosed gap: `cross-node-contention` cannot Pass on any run today
//!
//! Stated here rather than left to be discovered, in the same posture #206
//! and #207 took for their own producer-shaped gaps.
//!
//! [`check_cross_node_contention`] recognises `Forward`/`PeerApply`,
//! `Receipt`, and `TakeoverDrain` against `ClaimAdvertise`/`LakeCommitOk`.
//! As of today NOTHING in a real run journals the first three: the OTLP
//! accept path does not call `duckspout_replication::forward::forward_to_peers`
//! at all (`duckspout_daemon::wiring`'s own module docs — there is no peer
//! transport to forward through yet, issue #52's follow-up), so no `Forward`,
//! no `PeerApply` and no `Receipt` ever reach a journal; and no crate in the
//! workspace journals `TakeoverDrain`, so the ownership relation has no
//! producer either. `LakeCommitOk` alone cannot satisfy the rule — it is only
//! ever the passive half of the ownership pair.
//!
//! The consequence is exact and should not be discovered by surprise: on a
//! real ≥2-node fleet run today this rule reports `NoVerdict`, so the
//! distributed tier cannot exit `0` until a peer transport lands. That is the
//! honest outcome — a fleet whose nodes genuinely never contended certifies
//! nothing multi-node, and the judge saying so before the producer exists is
//! the rule working, not failing. It is called out because a rule that can
//! only ever answer one way is indistinguishable from a broken one until its
//! producer arrives, and nobody should have to rediscover which of the two
//! this is. The rule is exercised end-to-end today by the seeded-replay
//! fixture and its unit tests; the gate it guards arms with `ctk-distributed`
//! (staged, nightly, v0.2), by which point the producer is in scope.

use std::collections::BTreeSet;

use duckspout_types::TraceEvent;

use crate::fault_ledger::{AlibiTolerance, FaultLedger};
use crate::journal::JournalSet;
use crate::run_manifest::{RunManifest, node_host};
use crate::summary::{self, SummaryFinding};
use crate::verdict::Verdict;

/// Rule name: "a fault schedule that armed faults and fired none."
pub const RULE_FAULT_SCHEDULE: &str = "vacuity/fault-schedule-fired";
/// Rule name: "no observed cross-node contention."
pub const RULE_CROSS_NODE_CONTENTION: &str = "vacuity/cross-node-contention";
/// Rule name: "a node whose journals simply stop."
pub const RULE_NODE_CONTINUITY: &str = "vacuity/node-journal-continuity";
/// Rule name: "an ambiguous-outcome fraction above the profile's ceiling"
/// (and the loadgen run-summary signals that share its evidence).
pub const RULE_LOADGEN_OUTCOMES: &str = "vacuity/loadgen-outcome-quality";

/// How far a roster node's journal may fall behind the FLEET'S OWN last
/// journal activity before the node counts as one whose journals simply
/// stopped.
///
/// Reasoning for `30_000` (30 s). The measurement is deliberately relative —
/// `RunManifest::last_progress_at_ms` explains why a node is compared against
/// the busiest node rather than against the run's declared end — so the
/// budget only has to absorb the largest gap a HEALTHY node can leave while
/// its peers keep working. The two real sources of such a gap are the drain
/// cadence (a node with no window ready to seal journals nothing between
/// commits) and a node that owns no partition for a stretch after a
/// membership change; both are cadence-scale, seconds not minutes, and the
/// distributed tier's own settle budget (`duckspout-fleet`'s
/// `--settle-timeout-secs`, 60 s by default) is the outer bound on how long
/// the runner will wait for any of it. 30 s sits above the first and below
/// the second.
///
/// It is a conservative starting point, not a derived bound, and it is
/// exposed as `--max-journal-silence-ms` for exactly the reason
/// `crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION` is exposed: an operator
/// tuning a profile's cadence parameters can justify a different value for
/// that profile, and should have to say so rather than edit a constant. Note
/// which way the error falls if it is wrong: too large under-reports a
/// vanished machine (a false `Pass`), too small over-reports one (a false
/// `NoVerdict`). Only the first is dangerous, which is why this constant is
/// deliberately not generous.
pub const DEFAULT_MAX_JOURNAL_SILENCE_MS: u64 = 30_000;

/// Everything the run-level rules grade, gathered once.
#[derive(Debug, Clone, Copy)]
pub struct VacuityInputs<'a> {
    /// Every journal line the run produced.
    pub journals: &'a JournalSet,
    /// The fault injectors' own ledger, if one was supplied.
    pub ledger: Option<&'a FaultLedger>,
    /// The fleet runner's manifest, if one was supplied.
    pub manifest: Option<&'a RunManifest>,
    /// `--max-ambiguous-fraction`
    /// (`crate::summary::DEFAULT_MAX_AMBIGUOUS_FRACTION`).
    pub max_ambiguous_fraction: f64,
    /// `--max-journal-silence-ms` ([`DEFAULT_MAX_JOURNAL_SILENCE_MS`]).
    pub max_journal_silence_ms: u64,
}

/// One reason a run cannot be used to certify anything.
#[derive(Debug, Clone, PartialEq)]
pub enum VacuityFinding {
    /// The rule had no evidence to be applied to at all. Never a `Pass`:
    /// "skipped ≠ passed" (§8.4) is a rule about the run, not only about the
    /// predicates.
    NotApplicable {
        /// Which rule, and why it could not run.
        reason: String,
    },
    /// A fault window the ledger shows was armed and never started.
    ArmedButUnfired {
        /// The injector's id for the window.
        fault_id: String,
        /// Its kind.
        kind: String,
        /// The node it would have hit.
        target_node: String,
    },
    /// A multi-node run in which no two nodes were ever observed
    /// interacting.
    NoCrossNodeContention {
        /// How many nodes the roster had.
        roster_nodes: usize,
    },
    /// A roster node that journaled nothing for the whole run.
    NodeNeverJournaled {
        /// The roster node.
        node: String,
    },
    /// A roster node whose journal the judge was never given, even though
    /// the runner recorded it as having written one.
    NodeJournalNotIngested {
        /// The roster node.
        node: String,
        /// How many lines the runner last saw in its journal.
        journal_lines: u64,
    },
    /// A roster node whose process had already exited when teardown reached
    /// it, with no armed fault accounting for it.
    NodeExitedEarly {
        /// The roster node.
        node: String,
    },
    /// A roster node that fell silent while the rest of the fleet kept
    /// working, with no armed fault accounting for it.
    NodeJournalStopped {
        /// The roster node.
        node: String,
        /// How far behind the fleet's own last activity it fell.
        silent_ms: u64,
        /// The budget it exceeded.
        budget_ms: u64,
    },
    /// A loadgen run-summary signal (`crate::summary`), including §8.4's own
    /// ambiguous-outcome ceiling.
    LoadgenSummary(SummaryFinding),
}

impl std::fmt::Display for VacuityFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VacuityFinding::NotApplicable { reason } => write!(f, "{reason}"),
            VacuityFinding::ArmedButUnfired {
                fault_id,
                kind,
                target_node,
            } => write!(
                f,
                "fault {fault_id} ({kind}, target {target_node}) was armed and never started — \
                 a schedule that armed faults and fired none proves nothing about resilience \
                 under those faults (§8.4)"
            ),
            VacuityFinding::NoCrossNodeContention { roster_nodes } => write!(
                f,
                "{roster_nodes}-node run with no observed cross-node contention: no roster node \
                 journaled a Forward that another journaled a PeerApply for, no roster node \
                 journaled a Receipt attesting a peer's forward, and no TakeoverDrain landed \
                 against another node's advertised claim — a multi-node run that never made two \
                 nodes meet certifies nothing multi-node (§8.4)"
            ),
            VacuityFinding::NodeNeverJournaled { node } => write!(
                f,
                "roster node {node} journaled nothing for the entire run, and no armed fault \
                 accounts for it — a machine that never spoke is exactly the under-reported-loss \
                 shape (§8.4)"
            ),
            VacuityFinding::NodeJournalNotIngested {
                node,
                journal_lines,
            } => write!(
                f,
                "roster node {node} wrote {journal_lines} journal line(s) that this judge run was \
                 never given — a run cannot be certified while a member's evidence is withheld"
            ),
            VacuityFinding::NodeExitedEarly { node } => write!(
                f,
                "roster node {node}'s process had already exited before teardown, and no armed \
                 fault accounts for it — a crash outside the fault schedule (§8.4)"
            ),
            VacuityFinding::NodeJournalStopped {
                node,
                silent_ms,
                budget_ms,
            } => write!(
                f,
                "roster node {node}'s journal stopped {silent_ms} ms before the fleet's own last \
                 journal activity (budget {budget_ms} ms), and no armed fault accounts for it — a \
                 vanished machine accuses nothing and certifies nothing (§8.4)"
            ),
            VacuityFinding::LoadgenSummary(finding) => write!(f, "{finding}"),
        }
    }
}

/// Every run-level rule's verdict, in a stable order, ready to join the
/// predicates' verdicts in `crate::verdict::combined_exit_code` (module
/// docs).
#[must_use]
pub fn check_all(inputs: &VacuityInputs<'_>) -> Vec<(&'static str, Verdict<VacuityFinding>)> {
    vec![
        (
            RULE_FAULT_SCHEDULE,
            to_verdict(check_fault_schedule(inputs.ledger)),
        ),
        (
            RULE_CROSS_NODE_CONTENTION,
            to_verdict(check_cross_node_contention(
                inputs.journals,
                inputs.manifest,
            )),
        ),
        (
            RULE_NODE_CONTINUITY,
            to_verdict(check_node_continuity(
                inputs.journals,
                inputs.manifest,
                inputs.ledger,
                inputs.max_journal_silence_ms,
            )),
        ),
        (
            RULE_LOADGEN_OUTCOMES,
            to_verdict(check_loadgen_outcomes(
                inputs.journals,
                inputs.max_ambiguous_fraction,
            )),
        ),
    ]
}

/// A rule's own `Ok(checked)` / `Err(findings)` result as a [`Verdict`].
///
/// `Verdict::pass`'s `checked == 0` guard is kept in the loop deliberately,
/// even though every rule below reports its own emptiness as a
/// [`VacuityFinding::NotApplicable`]: a rule that ever forgot to would still
/// not be able to report a vacuous `Pass` through this function.
fn to_verdict(result: Result<usize, Vec<VacuityFinding>>) -> Verdict<VacuityFinding> {
    match result {
        Ok(checked) => Verdict::pass(
            checked,
            "this run-level vacuity rule reported no findings but also checked nothing",
        ),
        Err(findings) => Verdict::NoVerdict(
            findings
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        ),
    }
}

/// §8.4 rule 1: every fault the injectors' OWN ledger shows armed must also
/// show started.
///
/// # Errors
///
/// Every armed-but-unfired window, or the reason the rule could not be
/// applied.
pub fn check_fault_schedule(ledger: Option<&FaultLedger>) -> Result<usize, Vec<VacuityFinding>> {
    let Some(ledger) = ledger else {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "no --fault-log was given, so this run's fault firing was never measured \
                     from the injectors' own ledger (§8.4 requires exactly that, and forbids \
                     assuming it from the run's profile)"
                .to_owned(),
        }]);
    };
    if ledger.windows.is_empty() {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "the fault ledger records no fault window at all — the distributed tier \
                     exists to run a fault schedule, so a run that scheduled none certifies \
                     nothing about resilience under faults"
                .to_owned(),
        }]);
    }
    let findings: Vec<VacuityFinding> = ledger
        .windows
        .iter()
        .filter(|window| window.started_at_ms.is_none())
        .map(|window| VacuityFinding::ArmedButUnfired {
            fault_id: window.fault_id.clone(),
            kind: window.kind.clone(),
            target_node: window.target_node.clone(),
        })
        .collect();
    if findings.is_empty() {
        Ok(ledger.windows.len())
    } else {
        Err(findings)
    }
}

/// One observed interaction between two distinct roster nodes
/// ([`check_cross_node_contention`]'s docs for what the relations are and
/// what this evidence can and cannot prove).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ContentionPair {
    relation: &'static str,
    from: String,
    to: String,
}

/// Every roster host that journaled `event`.
fn hosts_journaling(
    journals: &JournalSet,
    roster: &BTreeSet<&str>,
    event: TraceEvent,
) -> BTreeSet<String> {
    journals
        .lines
        .iter()
        .filter(|line| line.event == event)
        .map(|line| node_host(line.node.as_str()).to_owned())
        .filter(|host| roster.contains(host.as_str()))
        .collect()
}

/// Every distinct `(relation, from, to)` with `from != to`.
fn cross_pairs(
    relation: &'static str,
    from: &BTreeSet<String>,
    to: &BTreeSet<String>,
) -> BTreeSet<ContentionPair> {
    let mut pairs = BTreeSet::new();
    for a in from {
        for b in to {
            if a != b {
                pairs.insert(ContentionPair {
                    relation,
                    from: a.clone(),
                    to: b.clone(),
                });
            }
        }
    }
    pairs
}

/// §8.4 rule 2: a multi-node run must show two nodes actually meeting.
///
/// # What counts as cross-node contention, and why exactly this
///
/// §8.4's distributed tier exists to certify a REAL MULTI-NODE FLEET; a run
/// whose nodes never touched each other has exercised a co-located
/// single-node system with extra processes. The evidence for "they touched"
/// has to come from the journals themselves (the manifest only says how many
/// nodes there were), and the recognised shapes are the ones the frozen §3.3
/// vocabulary has and that real code actually journals:
///
/// - **Replication, applied.** `Forward` is journaled by the owner sending a
///   batch to its peers; `PeerApply` by the peer that durably applied it
///   (`duckspout_replication::forward` / `::peer_apply`, and
///   `docs/trace-mapping.md`). One of each, on two different roster nodes, is
///   the write path actually crossing a machine boundary.
/// - **Replication, attested.** A bare `Receipt` on a roster node, with no
///   `PeerApply` beside it, is `duckspout_replication::peer_apply`'s
///   `DuplicateAcked` shape: a forward whose range this node had already
///   applied, re-acked idempotently. That is the NORMAL shape of a forward
///   retried after a partition heals — precisely what a chaos schedule
///   produces — and it needs no partner line to be inter-node evidence,
///   because `apply_forward` refuses `origin == self_node` before it can
///   journal anything. A `Receipt` in a roster node's journal is therefore,
///   by construction, that node attesting a DIFFERENT node's forward.
///   Requiring an adjacent `PeerApply` here was an ACPR finding: it made a
///   healed-partition retry read as "no inter-node traffic at all".
/// - **Ownership.** `TakeoverDrain` is journaled by a replica taking over a
///   dead owner's partitions, and `ClaimAdvertise`/`LakeCommitOk` by a node
///   that held or advanced one. A takeover on one node while another node
///   held claims is ownership genuinely changing hands.
///
/// Every half must come from a node on the manifest's ROSTER, so a journal
/// attributed to a node the runner never provisioned cannot supply the
/// evidence.
///
/// # The honest limits — two of them
///
/// **It is a floor, not a matcher.** `Forward`, `PeerApply` and `Receipt`
/// lines carry no correlation payload today (`crate::journal`'s payload
/// table: only `request_id`, `complete_through_ms`, `changelog_key` and
/// `part` are decoded, and none of them ride these events), so this cannot
/// prove that node B's `PeerApply` applied node A's `Forward` — only that
/// both happened, on two different machines, in one run. Sharpening it into a
/// real pairing is a change to the wire shape (a forwarded batch's identity
/// on both lines), not to this rule.
///
/// **Some real inter-node traffic is invisible to it, and that direction is
/// a false `NoVerdict`.** An earlier version of this comment claimed the
/// floor "cannot produce a false `NoVerdict`". That was wrong, and it is
/// worth stating exactly how, because the gap is on the producer side and
/// cannot be closed here: `duckspout_replication::peer_apply::apply_forward`
/// journals NOTHING at all for its `Fenced` (the §8.4 FencedZombie/SIGSTOP
/// window — a stale incarnation rejected, which is itself two nodes'
/// incarnations meeting), `GapRefused`, `SuspectDuplicate` and `ApplyFailed`
/// outcomes. A run whose only inter-node traffic took those paths is real
/// contention this rule reports as none. Making it visible means journaling
/// at those tracepoints — a change to the producer and to
/// `docs/trace-mapping.md`, not a rule this judge can widen — so it is
/// disclosed here rather than papered over with a proxy (`FenceBoot`, which
/// every node journals at boot, or the fault ledger, which §8.4 forbids
/// reading contention out of). The error direction is the safe one:
/// `NoVerdict`, never a false `Pass`.
///
/// # Errors
///
/// The no-contention finding, or the reason the rule could not be applied.
pub fn check_cross_node_contention(
    journals: &JournalSet,
    manifest: Option<&RunManifest>,
) -> Result<usize, Vec<VacuityFinding>> {
    let Some(manifest) = manifest else {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "no --run-manifest was given, so this run's node roster is unknown — \
                     counting the node ids that happen to appear in the journals cannot \
                     substitute for it (a member that died at boot journals nothing and would \
                     silently shrink the roster to one, which is the very shape that excuses \
                     an absence of cross-node traffic)"
                .to_owned(),
        }]);
    };
    let roster: BTreeSet<&str> = manifest.nodes.iter().map(|n| n.name.as_str()).collect();
    if roster.len() < 2 {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: format!(
                "this run had {} node(s) on its roster: cross-node contention is not something \
                 it could have exercised, so it certifies nothing about it (§8.4's distributed \
                 tier is about real multi-node fleets)",
                roster.len()
            ),
        }]);
    }

    let forwarders = hosts_journaling(journals, &roster, TraceEvent::Forward);
    let appliers = hosts_journaling(journals, &roster, TraceEvent::PeerApply);
    let takers = hosts_journaling(journals, &roster, TraceEvent::TakeoverDrain);
    let mut holders = hosts_journaling(journals, &roster, TraceEvent::ClaimAdvertise);
    holders.extend(hosts_journaling(
        journals,
        &roster,
        TraceEvent::LakeCommitOk,
    ));

    let mut pairs = cross_pairs("replication", &forwarders, &appliers);
    pairs.extend(cross_pairs("ownership", &takers, &holders));

    // A `Receipt` needs no partner line to be cross-node evidence (this
    // function's docs): only `apply_forward` journals one, and only after it
    // has refused a self-origin forward, so the attesting node is already
    // known to be a different machine from the origin. Each attesting roster
    // node counts once.
    let attesters = hosts_journaling(journals, &roster, TraceEvent::Receipt);

    let evidence = pairs.len() + attesters.len();
    if evidence == 0 {
        Err(vec![VacuityFinding::NoCrossNodeContention {
            roster_nodes: roster.len(),
        }])
    } else {
        Ok(evidence)
    }
}

/// §8.4 rule 4: no roster node's journal may simply stop.
///
/// # What a node is expected to have journaled, and until when
///
/// Each roster node is measured over `[last_progress, horizon]`, and both
/// ends matter:
///
/// - **`horizon`** is the run's own last journal activity across every node
///   (`RunManifest::last_progress_at_ms` for why the fleet's high-water mark
///   and not the run's declared end), CAPPED at the moment a terminal fault
///   took this node out of the run for good
///   (`FaultLedger::terminal_horizon`). A killed node is not expected to
///   journal after it was killed — but it IS still expected to have been
///   journaling right up until then, which is the whole point of the cap:
///   a node that went quiet a minute before the kill vanished a minute before
///   the kill, and folding that into the kill's alibi would hide it.
/// - **the alibi** for the remaining interval is a fault window that COVERS
///   it end to end (`FaultWindow::covers`) — a partition lifted halfway
///   through, after which the node still never spoke, is not an alibi.
///
/// The manifest names its roster bare and the ledger names its targets
/// rendered (`<name>/<incarnation>`); `FaultLedger::windows_targeting` is
/// what reconciles the two, and passing a roster name straight into it is
/// correct only because it does.
///
/// Every exemption here is derived from the injectors' LEDGER, never from the
/// run's `--fault-*` flags, so an armed-but-unfired fault excuses nothing; it
/// is its own finding under [`check_fault_schedule`]. This mirrors
/// `duckspout-fleet`'s own precedent, which exempts an intentionally faulted
/// node from its runner-level acceptance check (`ingest_faulted_nodes`)
/// rather than convicting the fault schedule of working.
///
/// # Errors
///
/// Every unexplained continuity finding, or the reason the rule could not be
/// applied.
pub fn check_node_continuity(
    journals: &JournalSet,
    manifest: Option<&RunManifest>,
    ledger: Option<&FaultLedger>,
    budget_ms: u64,
) -> Result<usize, Vec<VacuityFinding>> {
    let Some(manifest) = manifest else {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "no --run-manifest was given: without the runner's roster and its per-node \
                     journal-progress samples, a node whose journal stopped is indistinguishable \
                     from a node that was never in the run (D-6 journals carry no roster and no \
                     timestamps)"
                .to_owned(),
        }]);
    };
    if manifest.nodes.is_empty() {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "the run manifest's roster is empty — no node ran, so none can have \
                     certified anything"
                .to_owned(),
        }]);
    }
    let Some(run_last_progress) = manifest.last_progress_at_ms() else {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "no roster node ever journaled a single line, so there is no fleet activity \
                     to measure any node's silence against — the whole run is the vacuous case"
                .to_owned(),
        }]);
    };
    // The alibi tolerance, asymmetric on purpose
    // (`crate::fault_ledger::AlibiTolerance`'s own docs). The END end
    // forgives only the manifest's sampling grain, since a node killed 400 ms
    // after its last 500 ms-grain sample must not look as if it had gone
    // quiet BEFORE the fault that killed it. The START end additionally
    // forgives this rule's OWN silence budget: below, silence shorter than
    // `budget_ms` is not a finding at all, so a fault that fired within
    // `budget_ms` of a node's last progress must be allowed to explain the
    // silence that followed it. Forgiving only the grain there convicted a
    // node whose legitimate journaling cadence was slower than one sample.
    let tolerance = AlibiTolerance {
        start_slack_ms: budget_ms.saturating_add(manifest.sample_interval_ms),
        end_slack_ms: manifest.sample_interval_ms,
    };
    let ingested: BTreeSet<&str> = journals
        .lines
        .iter()
        .map(|line| node_host(line.node.as_str()))
        .collect();

    // Whether some fault window targeting `node` covers `[from, until]` end
    // to end (this function's docs).
    let covered = |node: &str, from: u64, until: u64| -> bool {
        ledger.is_some_and(|ledger| {
            ledger
                .windows_targeting(node)
                .any(|window| window.covers(from, until, tolerance))
        })
    };
    // When a terminal fault took `node` out of the run, if one did.
    let terminal_at =
        |node: &str| -> Option<u64> { ledger.and_then(|ledger| ledger.terminal_horizon(node)) };

    let mut findings = Vec::new();
    for node in &manifest.nodes {
        // After a terminal fault, this node is expected to journal nothing.
        let horizon = terminal_at(&node.name)
            .unwrap_or(run_last_progress)
            .min(run_last_progress);

        let Some(last_progress) = node.last_progress_at_ms else {
            // Never journaled a single line. Not budget-gated, unlike the
            // silence rule below: a node the runner booted and drove load
            // into and that produced ZERO journal lines is not a node that
            // went quiet, it is a node whose evidence never existed — and
            // there is no length of run over which that certifies anything.
            // A fault covering the whole run from its start still excuses it.
            if !covered(&node.name, manifest.started_at_ms, horizon) {
                findings.push(VacuityFinding::NodeNeverJournaled {
                    node: node.name.clone(),
                });
            }
            continue;
        };
        if !ingested.contains(node.name.as_str()) {
            // The runner watched this node write lines the judge was never
            // handed. No fault excuses that: it is an evidence-delivery
            // failure, not a fleet outcome, and a fault-killed node's journal
            // is still on disk and still owed to the judge.
            findings.push(VacuityFinding::NodeJournalNotIngested {
                node: node.name.clone(),
                journal_lines: node.journal_lines,
            });
            continue;
        }
        if node.exited_early && terminal_at(&node.name).is_none() {
            // It ended on its own, and the ledger shows nothing that ends a
            // node targeting it. (A node killed by a terminal fault is NOT
            // acquitted wholesale here — its pre-kill silence is still
            // measured below, against the capped horizon.)
            findings.push(VacuityFinding::NodeExitedEarly {
                node: node.name.clone(),
            });
            continue;
        }
        let silent_ms = horizon.saturating_sub(last_progress);
        if silent_ms > budget_ms && !covered(&node.name, last_progress, horizon) {
            findings.push(VacuityFinding::NodeJournalStopped {
                node: node.name.clone(),
                silent_ms,
                budget_ms,
            });
        }
    }

    if findings.is_empty() {
        Ok(manifest.nodes.len())
    } else {
        Err(findings)
    }
}

/// §8.4 rule 3: the loadgen's ambiguous-outcome fraction, plus the two
/// sibling signals that share its evidence (`crate::summary`).
///
/// # Errors
///
/// Every run-summary finding, or the reason the rule could not be applied.
pub fn check_loadgen_outcomes(
    journals: &JournalSet,
    max_ambiguous_fraction: f64,
) -> Result<usize, Vec<VacuityFinding>> {
    let sources = summary::loadgen_journal_sources(journals);
    if sources.is_empty() {
        return Err(vec![VacuityFinding::NotApplicable {
            reason: "no loadgen journal is present in this evidence set (no line carries a \
                     payload identity), so the run's own client-side outcome quality — §8.4's \
                     ambiguous-outcome ceiling — went unexamined"
                .to_owned(),
        }]);
    }
    match summary::check_summaries(journals, max_ambiguous_fraction) {
        Ok(()) => Ok(sources.len()),
        Err(findings) => Err(findings
            .into_iter()
            .map(VacuityFinding::LoadgenSummary)
            .collect()),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use duckspout_types::NodeId;

    use super::*;
    use crate::fault_ledger::FaultWindow;
    use crate::journal::JournalLine;
    use crate::run_manifest::NodeRun;

    fn line(node: &str, seq: u64, event: TraceEvent) -> JournalLine {
        JournalLine {
            source: PathBuf::from("/j"),
            line_no: 1,
            node: NodeId::new(node),
            seq,
            event,
            identity: None,
            watermark: None,
            changelog: None,
            part: None,
        }
    }

    fn journals(lines: Vec<JournalLine>) -> JournalSet {
        JournalSet { lines }
    }

    fn node(name: &str, last_progress_at_ms: Option<u64>, exited_early: bool) -> NodeRun {
        NodeRun {
            name: name.to_owned(),
            journal_path: PathBuf::from(format!("/w/{name}.ndjson")),
            journal_lines: u64::from(last_progress_at_ms.is_some()),
            last_progress_at_ms,
            exited_early,
        }
    }

    fn manifest(nodes: Vec<NodeRun>) -> RunManifest {
        RunManifest {
            started_at_ms: 1_000,
            ended_at_ms: 100_000,
            sample_interval_ms: 500,
            nodes,
        }
    }

    fn window(
        fault_id: &str,
        kind: &str,
        target: &str,
        started: Option<u64>,
        ended: Option<u64>,
    ) -> FaultWindow {
        FaultWindow {
            fault_id: fault_id.to_owned(),
            kind: kind.to_owned(),
            target_node: target.to_owned(),
            armed_at_ms: Some(1_000),
            started_at_ms: started,
            ended_at_ms: ended,
        }
    }

    // --- rule 1: armed but unfired ---

    #[test]
    fn a_fault_armed_and_never_started_is_vacuity() {
        let ledger = FaultLedger {
            windows: vec![
                window("kill-0", "node_kill", "n1", Some(2_000), Some(2_100)),
                window("pause-1", "sigstop_pause", "n2", None, None),
            ],
        };
        let findings = check_fault_schedule(Some(&ledger)).expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::ArmedButUnfired {
                fault_id: "pause-1".to_owned(),
                kind: "sigstop_pause".to_owned(),
                target_node: "n2".to_owned(),
            }]
        );
    }

    #[test]
    fn a_schedule_whose_every_fault_fired_passes() {
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n1",
                Some(2_000),
                Some(2_100),
            )],
        };
        assert_eq!(check_fault_schedule(Some(&ledger)), Ok(1));
    }

    /// A ledger with no windows at all is not a clean run: the distributed
    /// tier's premise is a fault schedule. Would catch a rule that reported
    /// "nothing armed, nothing unfired, therefore Pass" — the exact vacuous
    /// pass §8.4's teeth exist to stop.
    #[test]
    fn a_run_that_scheduled_no_faults_is_not_a_pass() {
        let ledger = FaultLedger::default();
        let findings = check_fault_schedule(Some(&ledger)).expect_err("must be vacuous");
        assert!(matches!(findings[0], VacuityFinding::NotApplicable { .. }));
    }

    #[test]
    fn no_fault_ledger_at_all_is_never_a_pass() {
        assert!(check_fault_schedule(None).is_err());
    }

    // --- rule 2: cross-node contention ---

    #[test]
    fn a_forward_on_one_node_and_a_peer_apply_on_another_is_contention() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Forward),
            line("n2/1", 0, TraceEvent::PeerApply),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert_eq!(
            check_cross_node_contention(&set, Some(&manifest)),
            Ok(1),
            "one replication pair"
        );
    }

    /// The whole point of the rule: two nodes that each did plenty of work
    /// but never each other's. Would catch a rule that accepted "≥2 nodes
    /// journaled something" as contention.
    #[test]
    fn two_busy_nodes_that_never_interacted_are_vacuous() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Accept),
            line("n1/1", 1, TraceEvent::StageCommit),
            line("n2/1", 0, TraceEvent::Accept),
            line("n2/1", 1, TraceEvent::StageCommit),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        let findings =
            check_cross_node_contention(&set, Some(&manifest)).expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NoCrossNodeContention { roster_nodes: 2 }]
        );
    }

    /// A node forwarding to ITSELF is not cross-node contention. Would catch
    /// a rule that counted "some node journaled Forward and some node
    /// journaled `PeerApply`" without requiring them to be different machines.
    #[test]
    fn one_node_doing_both_halves_is_not_cross_node_contention() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Forward),
            line("n1/1", 1, TraceEvent::PeerApply),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert!(check_cross_node_contention(&set, Some(&manifest)).is_err());
    }

    /// Contention must be between ROSTER members: a journal claiming to be
    /// from a node the runner never provisioned cannot supply it. Would
    /// catch a rule satisfiable by adding a hand-written journal file.
    #[test]
    fn a_non_roster_node_cannot_supply_the_contention() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Forward),
            line("ghost/1", 0, TraceEvent::PeerApply),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert!(check_cross_node_contention(&set, Some(&manifest)).is_err());
    }

    /// An incarnation change is one machine, not two — so a restarted node
    /// cannot forward "to itself" across incarnations and satisfy the rule.
    #[test]
    fn a_restart_within_one_run_is_still_one_machine() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Forward),
            line("n1/2", 0, TraceEvent::PeerApply),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert!(check_cross_node_contention(&set, Some(&manifest)).is_err());
    }

    /// The ACPR regression for the `DuplicateAcked` shape: a forward retried
    /// after a partition heals journals a `Receipt` and NO `PeerApply`
    /// (`duckspout_replication::peer_apply::apply_forward`), which is exactly
    /// what a chaos schedule produces. Would catch a relation set that
    /// required an adjacent `PeerApply`, under which a healed-partition retry
    /// read as "no inter-node traffic at all".
    #[test]
    fn a_bare_receipt_is_contention_even_with_no_peer_apply_beside_it() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Forward),
            line("n2/1", 0, TraceEvent::Receipt),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert_eq!(check_cross_node_contention(&set, Some(&manifest)), Ok(1));
    }

    /// And it needs no `Forward` on the other side either: `apply_forward`
    /// refuses `origin == self_node` before journaling anything, so a
    /// `Receipt` on a roster node is by construction that node attesting
    /// SOMEBODY ELSE's forward — even if the origin's own journal was lost
    /// with the machine that wrote it.
    #[test]
    fn a_receipt_alone_is_the_attesting_nodes_own_proof_it_met_a_peer() {
        let set = journals(vec![line("n2/1", 0, TraceEvent::Receipt)]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert_eq!(check_cross_node_contention(&set, Some(&manifest)), Ok(1));
    }

    /// The disclosed gap, pinned so it cannot be forgotten (module docs' own
    /// "honest limits"): `apply_forward`'s `Fenced` outcome — §8.4's
    /// FencedZombie/SIGSTOP window, a stale incarnation rejected — journals
    /// NOTHING, so a run whose only inter-node traffic was fenced is real
    /// contention this rule reports as none. This test asserts the current,
    /// disclosed behaviour rather than endorsing it; when a tracepoint is
    /// added at that outcome, this test must be updated to the new one.
    #[test]
    fn a_forward_the_peer_fenced_is_inter_node_traffic_this_rule_cannot_see() {
        // All that reaches the journals is the origin's own Forward: the
        // fencing peer writes no line at all for the rejection.
        let set = journals(vec![line("n1/1", 0, TraceEvent::Forward)]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        let findings =
            check_cross_node_contention(&set, Some(&manifest)).expect_err("the disclosed gap");
        assert_eq!(
            findings,
            vec![VacuityFinding::NoCrossNodeContention { roster_nodes: 2 }],
            "the error direction is NoVerdict, never a false Pass"
        );
    }

    #[test]
    fn a_takeover_against_another_nodes_claim_is_ownership_contention() {
        let set = journals(vec![
            line("n2/1", 0, TraceEvent::TakeoverDrain),
            line("n1/1", 0, TraceEvent::ClaimAdvertise),
        ]);
        let manifest = manifest(vec![node("n1", Some(2), false), node("n2", Some(2), false)]);
        assert_eq!(check_cross_node_contention(&set, Some(&manifest)), Ok(1));
    }

    #[test]
    fn a_single_node_run_certifies_nothing_about_cross_node_contention() {
        let set = journals(vec![line("n1/1", 0, TraceEvent::Accept)]);
        let manifest = manifest(vec![node("n1", Some(2), false)]);
        let findings =
            check_cross_node_contention(&set, Some(&manifest)).expect_err("must be vacuous");
        assert!(matches!(findings[0], VacuityFinding::NotApplicable { .. }));
    }

    #[test]
    fn no_manifest_at_all_is_never_a_pass() {
        let set = journals(vec![
            line("n1/1", 0, TraceEvent::Forward),
            line("n2/1", 0, TraceEvent::PeerApply),
        ]);
        assert!(check_cross_node_contention(&set, None).is_err());
    }

    // --- rule 4: node journal continuity ---

    fn two_node_journals() -> JournalSet {
        journals(vec![
            line("n1/1", 0, TraceEvent::Accept),
            line("n2/1", 0, TraceEvent::Accept),
        ])
    }

    #[test]
    fn a_fleet_that_all_went_quiet_together_convicts_nobody() {
        // Both nodes stop at the same moment: the shutdown tail, not a
        // vanished machine. Would catch a rule anchored on the run's
        // declared end instead of the fleet's own last activity.
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(50_000), false),
            node("n2", Some(50_000), false),
        ]);
        assert_eq!(
            check_node_continuity(&set, Some(&m), None, DEFAULT_MAX_JOURNAL_SILENCE_MS),
            Ok(2)
        );
    }

    #[test]
    fn a_node_silent_past_the_budget_while_its_peers_worked_is_vacuity() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), false),
        ]);
        let findings = check_node_continuity(&set, Some(&m), None, DEFAULT_MAX_JOURNAL_SILENCE_MS)
            .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeJournalStopped {
                node: "n2".to_owned(),
                silent_ms: 80_000,
                budget_ms: DEFAULT_MAX_JOURNAL_SILENCE_MS,
            }]
        );
    }

    /// The intentional-kill exemption, derived from the ledger. Would catch a
    /// rule that convicted the fault schedule of working.
    #[test]
    fn a_node_an_armed_kill_actually_killed_is_not_double_penalised() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), true),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n2",
                Some(10_200),
                Some(10_400),
            )],
        };
        assert_eq!(
            check_node_continuity(
                &set,
                Some(&m),
                Some(&ledger),
                DEFAULT_MAX_JOURNAL_SILENCE_MS
            ),
            Ok(2)
        );
    }

    /// The ACPR join-key regression, at the rule's own layer. Every fleet
    /// injector writes the RENDERED `<name>/<incarnation>` id
    /// (`duckspout_fleet::fault::rendered_node_id`, whose whole purpose is to
    /// be joinable), while the manifest's roster is bare — so this is the
    /// shape of every real run's ledger, and the one the hand-written fixture
    /// above is not. Would catch the raw `target_node == node` join, under
    /// which no real kill ever excused its own target and this legitimate
    /// node-kill scenario produced a false `NodeExitedEarly`.
    #[test]
    fn a_kill_whose_ledger_names_the_rendered_node_id_still_excuses_its_target() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), true),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n2/1",
                Some(10_200),
                Some(10_400),
            )],
        };
        assert_eq!(
            check_node_continuity(
                &set,
                Some(&m),
                Some(&ledger),
                DEFAULT_MAX_JOURNAL_SILENCE_MS
            ),
            Ok(2)
        );
    }

    /// The ACPR tolerance regression: a healthy node whose journaling cadence
    /// is 5 s, partitioned 5 s after its last sample and never heard from
    /// again. Its pre-fault silence is 5 s — well inside the rule's own 30 s
    /// budget, which is to say the rule would not have convicted it for that
    /// silence on its own — so the partition explains the rest. Would catch
    /// an alibi whose from-end tolerance was the 500 ms sampling grain, which
    /// convicted this run.
    #[test]
    fn a_fault_that_fired_within_the_silence_budget_explains_the_silence() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), false),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "part-0",
                "network_partition",
                "n2",
                Some(15_000),
                None,
            )],
        };
        assert_eq!(
            check_node_continuity(
                &set,
                Some(&m),
                Some(&ledger),
                DEFAULT_MAX_JOURNAL_SILENCE_MS
            ),
            Ok(2)
        );
    }

    /// And the tolerance is the BUDGET, not infinity: a node quiet for longer
    /// than the budget before the fault fired vanished on its own first.
    #[test]
    fn a_fault_that_fired_long_after_the_silence_began_explains_nothing() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), false),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "part-0",
                "network_partition",
                "n2",
                Some(45_000),
                None,
            )],
        };
        let findings = check_node_continuity(
            &set,
            Some(&m),
            Some(&ledger),
            DEFAULT_MAX_JOURNAL_SILENCE_MS,
        )
        .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeJournalStopped {
                node: "n2".to_owned(),
                silent_ms: 80_000,
                budget_ms: DEFAULT_MAX_JOURNAL_SILENCE_MS,
            }]
        );
    }

    /// The exemption is scoped to the node the fault actually targeted — a
    /// kill on `n1` does not excuse `n2` vanishing.
    #[test]
    fn a_kill_on_one_node_does_not_excuse_another_vanishing() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), true),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n1",
                Some(10_200),
                Some(10_400),
            )],
        };
        let findings = check_node_continuity(
            &set,
            Some(&m),
            Some(&ledger),
            DEFAULT_MAX_JOURNAL_SILENCE_MS,
        )
        .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeExitedEarly {
                node: "n2".to_owned()
            }]
        );
    }

    /// An ARMED-but-unfired kill is not an alibi: the node really did vanish
    /// on its own. Would catch an exemption keyed off the run's profile
    /// (which `--fault-*` flags were passed) rather than off the ledger.
    #[test]
    fn an_armed_but_unfired_kill_excuses_nothing() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(10_000), true),
        ]);
        let ledger = FaultLedger {
            windows: vec![window("kill-0", "node_kill", "n2", None, None)],
        };
        let findings = check_node_continuity(
            &set,
            Some(&m),
            Some(&ledger),
            DEFAULT_MAX_JOURNAL_SILENCE_MS,
        )
        .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeExitedEarly {
                node: "n2".to_owned()
            }]
        );
    }

    /// A node whose journal the runner watched grow but that the judge was
    /// never handed. No fault excuses a withheld journal.
    #[test]
    fn a_roster_node_whose_journal_was_not_ingested_is_vacuity() {
        let set = journals(vec![line("n1/1", 0, TraceEvent::Accept)]);
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(89_900), false),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n2",
                Some(80_000),
                Some(80_100),
            )],
        };
        let findings = check_node_continuity(
            &set,
            Some(&m),
            Some(&ledger),
            DEFAULT_MAX_JOURNAL_SILENCE_MS,
        )
        .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeJournalNotIngested {
                node: "n2".to_owned(),
                journal_lines: 1,
            }]
        );
    }

    #[test]
    fn a_roster_node_that_never_journaled_anything_is_vacuity() {
        let set = journals(vec![line("n1/1", 0, TraceEvent::Accept)]);
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", None, false),
        ]);
        let findings = check_node_continuity(&set, Some(&m), None, DEFAULT_MAX_JOURNAL_SILENCE_MS)
            .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeNeverJournaled {
                node: "n2".to_owned()
            }]
        );
    }

    /// The horizon cap, in the direction that ACQUITS: a node killed at
    /// 40 s is not expected to journal for the remaining 50 s of the run, so
    /// its 50 s of post-kill silence is not measured against the budget at
    /// all. Would catch a rule that measured every node against the fleet's
    /// last activity regardless of whether the node still existed — which
    /// would make every successful kill a vacuity finding.
    #[test]
    fn a_killed_node_is_not_measured_past_the_moment_it_was_killed() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(39_800), true),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n2",
                Some(40_000),
                Some(40_100),
            )],
        };
        assert_eq!(
            check_node_continuity(
                &set,
                Some(&m),
                Some(&ledger),
                DEFAULT_MAX_JOURNAL_SILENCE_MS
            ),
            Ok(2)
        );
    }

    /// The horizon cap, in the direction that CONVICTS — and the reason it is
    /// a cap rather than a blanket alibi: a node that went quiet at 5 s and
    /// was killed at 40 s vanished 35 s before the kill, and the kill does
    /// not account for that. Would catch an exemption that acquitted any node
    /// a terminal fault eventually reached, which is how a real
    /// under-reported loss would hide inside a scheduled kill.
    #[test]
    fn a_node_that_vanished_long_before_the_kill_is_still_convicted() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(5_000), true),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "kill-0",
                "node_kill",
                "n2",
                Some(40_000),
                Some(40_100),
            )],
        };
        let findings = check_node_continuity(
            &set,
            Some(&m),
            Some(&ledger),
            DEFAULT_MAX_JOURNAL_SILENCE_MS,
        )
        .expect_err("must be vacuous");
        assert_eq!(
            findings,
            vec![VacuityFinding::NodeJournalStopped {
                node: "n2".to_owned(),
                silent_ms: 35_000,
                budget_ms: DEFAULT_MAX_JOURNAL_SILENCE_MS,
            }]
        );
    }

    /// A transient fault LIFTED long before the node's silence ended does not
    /// excuse it (`FaultWindow::covers`), which is what keeps this exemption
    /// from becoming "any node any fault ever touched".
    #[test]
    fn a_partition_that_was_lifted_does_not_excuse_a_later_disappearance() {
        let set = two_node_journals();
        let m = manifest(vec![
            node("n1", Some(90_000), false),
            node("n2", Some(50_000), false),
        ]);
        let ledger = FaultLedger {
            windows: vec![window(
                "part-0",
                "network_partition",
                "n2",
                Some(5_000),
                Some(9_000),
            )],
        };
        assert!(
            check_node_continuity(
                &set,
                Some(&m),
                Some(&ledger),
                DEFAULT_MAX_JOURNAL_SILENCE_MS
            )
            .is_err()
        );
    }

    #[test]
    fn no_manifest_means_node_continuity_certifies_nothing() {
        assert!(
            check_node_continuity(
                &two_node_journals(),
                None,
                None,
                DEFAULT_MAX_JOURNAL_SILENCE_MS
            )
            .is_err()
        );
    }

    // --- composition ---

    /// Every run-level rule is reported on every invocation, and an evidence
    /// set with none of the run-level inputs is `NoVerdict` on all four —
    /// never a silent pass.
    #[test]
    fn every_rule_is_reported_and_none_passes_without_its_evidence() {
        let set = two_node_journals();
        let reports = check_all(&VacuityInputs {
            journals: &set,
            ledger: None,
            manifest: None,
            max_ambiguous_fraction: summary::DEFAULT_MAX_AMBIGUOUS_FRACTION,
            max_journal_silence_ms: DEFAULT_MAX_JOURNAL_SILENCE_MS,
        });
        assert_eq!(reports.len(), 4);
        for (rule, verdict) in &reports {
            assert!(
                matches!(verdict, Verdict::NoVerdict(_)),
                "{rule} must not pass with no evidence: {verdict:?}"
            );
        }
    }

    /// No run-level rule may ever return `Violation`: vacuity accuses nobody
    /// (module docs). Would catch a future rule that convicted the fleet for
    /// an evidence problem.
    #[test]
    fn no_run_level_rule_ever_convicts() {
        let set = two_node_journals();
        let ledger = FaultLedger {
            windows: vec![window("f", "node_kill", "n9", None, None)],
        };
        let m = manifest(vec![node("n1", Some(1), false), node("n2", None, false)]);
        let reports = check_all(&VacuityInputs {
            journals: &set,
            ledger: Some(&ledger),
            manifest: Some(&m),
            max_ambiguous_fraction: summary::DEFAULT_MAX_AMBIGUOUS_FRACTION,
            max_journal_silence_ms: DEFAULT_MAX_JOURNAL_SILENCE_MS,
        });
        for (rule, verdict) in &reports {
            assert!(
                !matches!(verdict, Verdict::Violation(_)),
                "{rule} convicted; vacuity rules never do"
            );
        }
    }
}
