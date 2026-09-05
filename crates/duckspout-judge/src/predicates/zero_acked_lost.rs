//! The zero-acked-lost predicate (the W-shaped judge, write-side; §8.4):
//! every record whose `ClientAck` the load generator journaled must be
//! present in the final system (hot or lake), regardless of what the fault
//! schedule did.
//!
//! # System-class exclusion (§2.2)
//!
//! System-class datasets are excluded **by definition**: the `_self` and
//! `_canary` system tenants' ingest path never issues durable acks
//! (`docs/design/data-model.md` §2.2's "System tenants" — inverted defaults,
//! `DurableAck`/`NoAckedLoss` "not implicated rather than carved out"), so
//! there are no acks to lose for them. In a correctly-behaving fleet this
//! exclusion is normally vacuous here — nothing about the loadgen's own
//! journal format prevents a `_`-prefixed tenant from appearing in a
//! `ClientAck` line, but a real accept-side node never issues a durable ack
//! for one — so this filter should never actually remove anything from a
//! real run's journal. It is applied unconditionally anyway, matching the
//! spec's literal exclusion rather than relying on that absence holding.
//! Because this "should never actually happen," this module warns to
//! stderr whenever the filter actually removes something (ACPR finding
//! LOW-9) — a should-never-happen exclusion firing for real is itself
//! evidence worth surfacing, not silently swallowing.
//!
//! # Correlation identity (ACPR finding HIGH-2)
//!
//! `RequestIdentity::first_index` alone ALIASES: it is `sent * batch_size`
//! counted from 0 independently by every loadgen process
//! (`duckspout_loadgen::main`), so two different fleet members, or one
//! member across a restart, produce numerically identical
//! `[first_index, first_index + record_count)` ranges for the same tenant.
//! Confirmed by ACPR: two members acking the identical `(tenant, [0,2))`
//! range, with only one member's writes actually surviving to the final
//! system, still reported `Pass` when keyed on the bare index alone — a
//! total loss of one member's writes, certified clean. This predicate
//! therefore keys every final-system lookup on the composite
//! `{source_incarnation}-{index}` string (`RequestIdentity::source_incarnation`
//! docs) — the exact value `duckspout_loadgen::client::synthetic_batch`
//! embeds as the record's own `loadgen.index` attribute — which is unique
//! across the whole fleet's lifetime, not just within one process.

use std::collections::BTreeSet;

use duckspout_types::TraceEvent;

use crate::final_state::FinalSystemState;
use crate::journal::{JournalSet, RequestIdentity};
use crate::predicates::SYSTEM_TENANT_PREFIX;
use crate::verdict::Verdict;

/// One acked request whose record range was not entirely present in the
/// final system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckedLostFinding {
    /// The lost request's idempotency key, for correlating back to the
    /// journal.
    pub request_id: String,
    /// The tenant the request was sent as.
    pub tenant: String,
    /// The specific `loadgen.index` values acked but not found present.
    pub missing_indices: BTreeSet<u64>,
}

impl std::fmt::Display for AckedLostFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request {} (tenant {}): acked but missing from the final system at indices {:?}",
            self.request_id, self.tenant, self.missing_indices
        )
    }
}

/// This predicate's three-valued verdict (§8.4's vacuity teeth).
///
/// #205 defined this as its own enum, deferring "how a multi-predicate
/// run's verdicts combine" to #206/#207/#208. #206 answered that in
/// `crate::verdict`, so this is now an alias of the shared, finding-generic
/// [`Verdict`] — same three values, same variant names, same exit contract,
/// one implementation instead of three. Its meanings are unchanged:
/// `Pass { checked }` counts non-system-tenant acks whose full record range
/// was present; `NoVerdict` covers both "nothing to check" and a
/// final-system query that itself failed (ACPR MEDIUM-5(b): a query failure
/// is not proof of absence).
pub type ZeroAckedLostVerdict = Verdict<AckedLostFinding>;

/// True iff `identity` is real, checkable evidence: a non-system tenant
/// (§2.2) whose ack actually covers at least one record. A `record_count:
/// 0` ack covers an empty range by construction (ACPR finding
/// MEDIUM-HIGH-3(a)) — checking it would vacuously succeed and inflate the
/// "something was checked" count without ever having checked anything, so
/// it is excluded from both the vacuity count and the violation search,
/// exactly like a system-tenant ack.
fn is_checkable(identity: &RequestIdentity) -> bool {
    !identity.tenant.starts_with(SYSTEM_TENANT_PREFIX) && identity.record_count > 0
}

/// Runs the predicate against `journals`' loadgen-journaled `ClientAck`
/// identities and `final_state`'s read-back.
#[must_use]
pub fn check<S: FinalSystemState>(journals: &JournalSet, final_state: &S) -> ZeroAckedLostVerdict {
    let all_acks: Vec<&RequestIdentity> = journals
        .identity_events(TraceEvent::ClientAck)
        .map(|(_, identity)| identity)
        .collect();

    // ACPR finding LOW-9: a system-tenant ack should never actually happen
    // (§2.2 — system tenants never issue durable acks), so its appearance
    // is itself evidence of a possible bug elsewhere; surface it rather than
    // silently dropping it in the filter below.
    let excluded_system_tenant_acks: Vec<&&RequestIdentity> = all_acks
        .iter()
        .filter(|identity| identity.tenant.starts_with(SYSTEM_TENANT_PREFIX))
        .collect();
    if !excluded_system_tenant_acks.is_empty() {
        eprintln!(
            "duckspout-judge: warning: {} ClientAck(s) from system tenant(s) were excluded from \
             zero-acked-lost — §2.2 says this should never happen (system tenants never issue \
             durable acks), so its appearance may itself indicate a bug elsewhere: {:?}",
            excluded_system_tenant_acks.len(),
            excluded_system_tenant_acks
                .iter()
                .map(|identity| identity.request_id.as_str())
                .collect::<Vec<_>>()
        );
    }

    let acks: Vec<&RequestIdentity> = all_acks
        .into_iter()
        .filter(|identity| is_checkable(identity))
        .collect();

    if acks.is_empty() {
        return ZeroAckedLostVerdict::NoVerdict(
            "no ClientAck with a non-empty record range was journaled by any loadgen member for \
             a non-system tenant — the zero-acked-lost predicate had nothing to check (§8.4 \
             vacuity teeth)"
                .to_owned(),
        );
    }

    let mut findings = Vec::new();
    for identity in &acks {
        // Decode-time validation (`crate::journal::identity_from_rest`)
        // already guarantees `first_index + record_count` does not overflow
        // for any identity that reached this point via real ingestion
        // (ACPR finding MEDIUM-HIGH-3(b)); `checked_add` here is defense in
        // depth against a `RequestIdentity` built some other way (e.g. a
        // future caller that skips ingestion), never silently panicking or
        // wrapping if that invariant is ever violated.
        let Some(end) = identity
            .first_index
            .checked_add(identity.record_count as u64)
        else {
            return ZeroAckedLostVerdict::NoVerdict(format!(
                "request {} (tenant {}): first_index {} + record_count {} overflows u64 — \
                 fails closed rather than checking a wrapped, wrong range",
                identity.request_id, identity.tenant, identity.first_index, identity.record_count
            ));
        };

        let mut missing = BTreeSet::new();
        for index in identity.first_index..end {
            let record_key = format!("{}-{index}", identity.source_incarnation);
            match final_state.contains(&identity.tenant, &record_key) {
                Ok(true) => {}
                Ok(false) => {
                    missing.insert(index);
                }
                Err(err) => {
                    return ZeroAckedLostVerdict::NoVerdict(format!(
                        "final-system query failed while checking request {} (tenant {}): {err} \
                         — a query failure is not proof of absence, so this run cannot be \
                         certified (§8.4 fail-closed posture, ACPR finding MEDIUM-5)",
                        identity.request_id, identity.tenant
                    ));
                }
            }
        }
        if !missing.is_empty() {
            findings.push(AckedLostFinding {
                request_id: identity.request_id.clone(),
                tenant: identity.tenant.clone(),
                missing_indices: missing,
            });
        }
    }

    if findings.is_empty() {
        // `acks` is non-empty here (the early return above), so this is
        // always a real `Pass`; going through `Verdict::pass` anyway keeps
        // the "a pass must have checked something" rule in exactly one
        // place for every predicate (`crate::verdict`).
        ZeroAckedLostVerdict::pass(
            acks.len(),
            "no checkable ClientAck remained after the §2.2 system-tenant and empty-range \
             exclusions (§8.4 vacuity teeth)",
        )
    } else {
        ZeroAckedLostVerdict::Violation(findings)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use duckspout_types::NodeId;

    use super::*;
    use crate::final_state::InMemoryFinalState;
    use crate::journal::JournalLine;

    fn ack_line_full(
        node: &str,
        seq: u64,
        tenant: &str,
        source_incarnation: &str,
        first_index: u64,
        record_count: usize,
    ) -> JournalLine {
        JournalLine {
            source: PathBuf::from("test"),
            line_no: usize::try_from(seq).expect("test seq fits in usize") + 1,
            node: NodeId::new(node),
            seq,
            event: TraceEvent::ClientAck,
            identity: Some(RequestIdentity {
                request_id: format!("{node}-{seq}"),
                tenant: tenant.to_owned(),
                record_count,
                first_index,
                source_incarnation: source_incarnation.to_owned(),
                partition: None,
                max_event_time_ms: None,
            }),
            watermark: None,
            changelog: None,
            part: None,
        }
    }

    fn ack_line(
        node: &str,
        seq: u64,
        tenant: &str,
        first_index: u64,
        record_count: usize,
    ) -> JournalLine {
        // Default single-incarnation convenience for tests that don't care
        // about ACPR HIGH-2's aliasing fix specifically.
        ack_line_full(node, seq, tenant, node, first_index, record_count)
    }

    #[test]
    fn a_clean_run_passes() {
        // Every acked record's index is present in the final state.
        let journals = JournalSet {
            lines: vec![ack_line("loadgen-0", 0, "tenant-a", 0, 5)],
        };
        let final_state =
            InMemoryFinalState::new().with_present_range("tenant-a", "loadgen-0", 0, 5);
        assert_eq!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::Pass { checked: 1 }
        );
    }

    #[test]
    fn a_genuinely_lost_acked_record_is_convicted() {
        // The seeded-violation-replay fixture this predicate must convict
        // (mission scope: "a REAL, deliberately-broken test fixture ...
        // that this predicate must correctly convict"): index 3 was acked
        // but the final system never has it — the exact W-shaped defect
        // this judge exists to catch.
        let journals = JournalSet {
            lines: vec![ack_line("loadgen-0", 0, "tenant-a", 0, 5)],
        };
        // Present everywhere in [0, 5) EXCEPT index 3.
        let final_state = InMemoryFinalState::new()
            .with_present_range("tenant-a", "loadgen-0", 0, 3)
            .with_present_range("tenant-a", "loadgen-0", 4, 1);
        let verdict = check(&journals, &final_state);
        match verdict {
            ZeroAckedLostVerdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].request_id, "loadgen-0-0");
                assert_eq!(findings[0].tenant, "tenant-a");
                assert_eq!(findings[0].missing_indices, BTreeSet::from([3]));
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn two_members_aliasing_on_first_index_with_one_members_writes_missing_is_convicted() {
        // The EXACT ACPR HIGH-2 repro: loadgen-0 and loadgen-1 both ack
        // tenant "t", range [0, 2) — identical first_index/record_count,
        // because each process computes `first_index` independently from
        // its own `sent * batch_size` starting at 0. Only loadgen-1's
        // writes actually survive to the final system. Before the fix,
        // keying on the bare index alone made this indistinguishable from a
        // clean run (`Pass { checked: 2 }`) — a total loss of loadgen-0's
        // writes, certified clean. The composite
        // `{source_incarnation}-{index}` key must catch it as a Violation.
        let journals = JournalSet {
            lines: vec![
                ack_line_full("loadgen-0", 0, "t", "loadgen-0-1000", 0, 2),
                ack_line_full("loadgen-1", 0, "t", "loadgen-1-2000", 0, 2),
            ],
        };
        // Only loadgen-1's incarnation's records are present.
        let final_state = InMemoryFinalState::new().with_present_range("t", "loadgen-1-2000", 0, 2);
        let verdict = check(&journals, &final_state);
        match verdict {
            ZeroAckedLostVerdict::Violation(findings) => {
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].request_id, "loadgen-0-0");
                assert_eq!(findings[0].missing_indices, BTreeSet::from([0, 1]));
            }
            other => panic!("expected Violation (aliasing must not hide the loss), got {other:?}"),
        }
    }

    #[test]
    fn a_restart_with_a_fresh_incarnation_does_not_alias_the_prior_incarnations_writes() {
        // The restart half of ACPR HIGH-2: the SAME node id, after a
        // restart, gets a fresh `source_incarnation` (a new `start_nonce`)
        // and starts `first_index` back at 0 — identical numeric range to
        // its own prior incarnation. The prior incarnation's writes being
        // present must not be mistaken for THIS incarnation's writes being
        // present.
        let journals = JournalSet {
            lines: vec![ack_line_full(
                "loadgen-0",
                0,
                "t",
                "loadgen-0-2000", // second incarnation (new nonce after restart)
                0,
                2,
            )],
        };
        // Only the FIRST incarnation's records ever made it to the final
        // system (e.g. the crash that caused the restart lost the rest).
        let final_state = InMemoryFinalState::new().with_present_range("t", "loadgen-0-1000", 0, 2);
        let verdict = check(&journals, &final_state);
        match verdict {
            ZeroAckedLostVerdict::Violation(findings) => {
                assert_eq!(findings[0].missing_indices, BTreeSet::from([0, 1]));
            }
            other => panic!("expected Violation, got {other:?}"),
        }
    }

    #[test]
    fn zero_client_acks_is_no_verdict_not_a_vacuous_pass() {
        // The vacuity-teeth case: nothing to check must never look like a
        // clean pass.
        let journals = JournalSet::default();
        let final_state = InMemoryFinalState::new();
        assert!(matches!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::NoVerdict(_)
        ));
    }

    #[test]
    fn only_system_tenant_acks_is_still_no_verdict() {
        // `_self`/`_canary` acks (if any somehow appeared) must not count
        // toward "something was checked" — would catch the system-tenant
        // filter being applied to the VIOLATION search but not to the
        // vacuity check, which would let a run with only system-tenant
        // traffic (i.e. nothing this predicate is meant to certify) report
        // a real Pass.
        let journals = JournalSet {
            lines: vec![ack_line("loadgen-0", 0, "_self", 0, 3)],
        };
        let final_state = InMemoryFinalState::new(); // even if all missing
        assert!(matches!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::NoVerdict(_)
        ));
    }

    #[test]
    fn system_tenant_acks_are_excluded_even_when_mixed_with_real_ones() {
        // A lost `_canary` record must NOT convict the run — §2.2's
        // by-definition exclusion — while a real tenant's ack is still
        // checked normally.
        let journals = JournalSet {
            lines: vec![
                ack_line("loadgen-0", 0, "_canary", 0, 3), // entirely missing below
                ack_line("loadgen-0", 1, "tenant-a", 0, 2),
            ],
        };
        let final_state =
            InMemoryFinalState::new().with_present_range("tenant-a", "loadgen-0", 0, 2);
        assert_eq!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::Pass { checked: 1 }
        );
    }

    #[test]
    fn a_zero_count_ack_does_not_count_toward_the_vacuity_check() {
        // ACPR finding MEDIUM-HIGH-3(a): a `record_count: 0` ack checks an
        // empty range, which trivially "passes" without having checked
        // anything real. It must not, by itself, turn a vacuous run into a
        // reported Pass.
        let journals = JournalSet {
            lines: vec![ack_line("loadgen-0", 0, "tenant-a", 0, 0)],
        };
        let final_state = InMemoryFinalState::new();
        assert!(matches!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::NoVerdict(_)
        ));
    }

    #[test]
    fn a_zero_count_ack_mixed_with_a_real_one_is_excluded_from_the_checked_count() {
        let journals = JournalSet {
            lines: vec![
                ack_line("loadgen-0", 0, "tenant-a", 0, 0),
                ack_line("loadgen-0", 1, "tenant-a", 10, 2),
            ],
        };
        let final_state =
            InMemoryFinalState::new().with_present_range("tenant-a", "loadgen-0", 10, 2);
        assert_eq!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::Pass { checked: 1 }
        );
    }

    #[test]
    fn client_timeout_lines_are_not_mistaken_for_acks() {
        let journals = JournalSet {
            lines: vec![JournalLine {
                source: PathBuf::from("test"),
                line_no: 1,
                node: NodeId::new("loadgen-0"),
                seq: 0,
                event: TraceEvent::ClientTimeout,
                identity: Some(RequestIdentity {
                    request_id: "req-1".to_owned(),
                    tenant: "tenant-a".to_owned(),
                    record_count: 5,
                    first_index: 0,
                    source_incarnation: "loadgen-0-1000".to_owned(),
                    partition: None,
                    max_event_time_ms: None,
                }),
                watermark: None,
                changelog: None,
                part: None,
            }],
        };
        let final_state = InMemoryFinalState::new(); // nothing present
        // A timeout is not an ack — nothing was promised, so there is
        // nothing to check, and the honest verdict is NoVerdict, not a
        // Violation manufactured from an ack that was never made.
        assert!(matches!(
            check(&journals, &final_state),
            ZeroAckedLostVerdict::NoVerdict(_)
        ));
    }

    /// A double that always fails its query — proves a query error is never
    /// silently read as "absent" (ACPR finding MEDIUM-5(b)).
    struct AlwaysFailingFinalState;

    impl FinalSystemState for AlwaysFailingFinalState {
        fn contains(
            &self,
            tenant: &str,
            record_key: &str,
        ) -> Result<bool, crate::final_state::QueryError> {
            Err(crate::final_state::QueryError {
                tenant: tenant.to_owned(),
                record_key: record_key.to_owned(),
                reason: "simulated backend outage".to_owned(),
            })
        }
    }

    #[test]
    fn a_final_state_query_failure_is_no_verdict_not_a_silent_pass_or_violation() {
        let journals = JournalSet {
            lines: vec![ack_line("loadgen-0", 0, "tenant-a", 0, 1)],
        };
        let verdict = check(&journals, &AlwaysFailingFinalState);
        match verdict {
            ZeroAckedLostVerdict::NoVerdict(reason) => {
                assert!(reason.contains("query failed"), "reason: {reason}");
            }
            other => panic!("expected NoVerdict on query failure, got {other:?}"),
        }
    }
}
