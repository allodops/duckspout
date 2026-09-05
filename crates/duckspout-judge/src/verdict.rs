//! The three-valued judge verdict (§8.4's vacuity teeth) and the exit
//! contract every predicate shares.
//!
//! #205 shipped one predicate, so its verdict type was deliberately kept
//! local to that predicate ("how a multi-predicate run's verdicts combine …
//! is #206/#207/#208's territory, not decided by this issue" —
//! `crate::predicates::zero_acked_lost`'s own note). #206 adds the second
//! and third predicates, so that question is now live and answered here,
//! once, rather than three times:
//!
//! - Every predicate returns [`Verdict<F>`] over its OWN typed finding `F`,
//!   so each predicate's tests keep asserting on structured findings rather
//!   than on prose. `Verdict::erase` renders findings through their
//!   [`Display`] impl for the binary's operator-facing report — the only
//!   place strings appear.
//! - The exit contract (`0` Pass · `2` Violation · `3` `NoVerdict`) lives in
//!   [`Verdict::exit_code`], and the combination rule for a whole run in
//!   [`combined_exit_code`]. Neither is re-derived per predicate.
//! - [`Verdict::pass`] is the single place a `Pass` can be constructed from a
//!   check count, and it downgrades `checked == 0` to `NoVerdict`
//!   mechanically. §8.4's "a judge that never rejects anything is
//!   indistinguishable from one too weak to reject anything" is thereby a
//!   property of the type, not a rule each predicate must remember.

use std::fmt::Display;

/// Exit code for a genuine `Pass` (§8.4).
pub const EXIT_PASS: i32 = 0;
/// Exit code for a `Violation` (§8.4).
pub const EXIT_VIOLATION: i32 = 2;
/// Exit code for `NoVerdict` — inconclusive or vacuous, never a pass (§8.4).
pub const EXIT_NO_VERDICT: i32 = 3;

/// One predicate's three-valued verdict over its own finding type `F`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict<F> {
    /// Something real was checked, and every check held.
    Pass {
        /// How many checks were actually performed — never `0` when built
        /// through [`Verdict::pass`].
        checked: usize,
    },
    /// At least one check failed. Non-empty by construction at every call
    /// site (a predicate reports `Pass`/`NoVerdict` when it has no findings).
    Violation(Vec<F>),
    /// The predicate could not certify anything: no evidence, ambiguous
    /// evidence, or a failed read-back. Never a pass (§8.4).
    NoVerdict(String),
}

impl<F> Verdict<F> {
    /// A `Pass` iff `checked > 0`; otherwise `NoVerdict(no_evidence)`.
    ///
    /// Every predicate ends here, so "checked nothing" can never be reported
    /// as a pass by any of them (module docs).
    #[must_use]
    pub fn pass(checked: usize, no_evidence: impl Into<String>) -> Self {
        if checked == 0 {
            Self::NoVerdict(no_evidence.into())
        } else {
            Self::Pass { checked }
        }
    }

    /// This verdict's exit code under the §8.4 contract.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Pass { .. } => EXIT_PASS,
            Self::Violation(_) => EXIT_VIOLATION,
            Self::NoVerdict(_) => EXIT_NO_VERDICT,
        }
    }
}

impl<F: Display> Verdict<F> {
    /// Renders each finding through its [`Display`] impl, so a caller that
    /// reports many predicates together can hold one uniform type without
    /// every predicate's finding type leaking into it.
    #[must_use]
    pub fn erase(self) -> Verdict<String> {
        match self {
            Self::Pass { checked } => Verdict::Pass { checked },
            Self::Violation(findings) => {
                Verdict::Violation(findings.iter().map(ToString::to_string).collect())
            }
            Self::NoVerdict(reason) => Verdict::NoVerdict(reason),
        }
    }
}

/// The whole run's exit code across every predicate's verdict.
///
/// A proven violation anywhere outranks an inconclusive predicate
/// elsewhere: the run's honest headline is "this fleet was convicted", and
/// `2` says so precisely, where `3` would understate it as merely
/// inconclusive. Both are non-zero, so the gate fails either way — the
/// ordering is about which true statement gets reported, not about whether
/// the run passes. A `Pass` requires EVERY predicate to have passed: a
/// predicate that could not check its own invariant leaves that invariant
/// uncertified, and "skipped ≠ passed" (§8.4) applies to the run as a whole.
#[must_use]
pub fn combined_exit_code(verdicts: &[Verdict<String>]) -> i32 {
    if verdicts.is_empty() {
        // No predicate ran at all: nothing was certified. Unreachable
        // through `crate::runner` (which always runs every predicate), but
        // an empty slice must never read as a pass here either.
        return EXIT_NO_VERDICT;
    }
    if verdicts.iter().any(|v| matches!(v, Verdict::Violation(_))) {
        EXIT_VIOLATION
    } else if verdicts.iter().any(|v| matches!(v, Verdict::NoVerdict(_))) {
        EXIT_NO_VERDICT
    } else {
        EXIT_PASS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_that_checked_nothing_is_downgraded_to_no_verdict() {
        // The vacuity teeth, enforced by the type rather than by each
        // predicate remembering to: would catch a predicate that filtered
        // its evidence down to nothing and then reported the empty result
        // as a clean pass.
        let verdict: Verdict<String> = Verdict::pass(0, "nothing to check");
        assert!(matches!(verdict, Verdict::NoVerdict(_)));
        assert_eq!(verdict.exit_code(), EXIT_NO_VERDICT);
    }

    #[test]
    fn a_pass_that_checked_something_is_a_pass() {
        let verdict: Verdict<String> = Verdict::pass(3, "nothing to check");
        assert_eq!(verdict, Verdict::Pass { checked: 3 });
        assert_eq!(verdict.exit_code(), EXIT_PASS);
    }

    #[test]
    fn a_violation_anywhere_outranks_an_inconclusive_predicate() {
        let code = combined_exit_code(&[
            Verdict::Pass { checked: 1 },
            Verdict::Violation(vec!["boom".to_owned()]),
            Verdict::NoVerdict("no evidence".to_owned()),
        ]);
        assert_eq!(code, EXIT_VIOLATION);
    }

    #[test]
    fn one_inconclusive_predicate_makes_the_whole_run_inconclusive() {
        // "Skipped ≠ passed" at run scope: a run where two predicates
        // passed but the third certified nothing must NOT exit 0 — would
        // catch a combination rule that reported the best verdict instead
        // of the honest one.
        let code = combined_exit_code(&[
            Verdict::Pass { checked: 1 },
            Verdict::Pass { checked: 2 },
            Verdict::NoVerdict("no evidence".to_owned()),
        ]);
        assert_eq!(code, EXIT_NO_VERDICT);
    }

    #[test]
    fn every_predicate_passing_is_the_only_way_to_exit_zero() {
        let code =
            combined_exit_code(&[Verdict::Pass { checked: 1 }, Verdict::Pass { checked: 9 }]);
        assert_eq!(code, EXIT_PASS);
    }

    #[test]
    fn no_predicates_at_all_is_never_a_pass() {
        assert_eq!(combined_exit_code(&[]), EXIT_NO_VERDICT);
    }

    #[test]
    fn erase_renders_findings_through_display() {
        #[derive(Debug)]
        struct Finding(u8);
        impl Display for Finding {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "finding #{}", self.0)
            }
        }
        let erased = Verdict::Violation(vec![Finding(1), Finding(2)]).erase();
        assert_eq!(
            erased,
            Verdict::Violation(vec!["finding #1".to_owned(), "finding #2".to_owned()])
        );
    }
}
