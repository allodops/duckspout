//! The `LakeCommitter` conformance suite (§6.4), public so third-party
//! backends can self-certify (§10.3).
//!
//! Ⓢ bootstrap stub: the module and entry point exist so a backend's test
//! harness compiles against the final shape; the suite body — atomicity of
//! commit+watermark, Indeterminate resolution, idempotent re-registration,
//! expire semantics, evolve ordering (§6.4) — lands at v0.1 and is tracked
//! by the arming ledger's `conformance` row.

use duckspout_types::LakeCommitter;

/// What one conformance run verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceReport {
    /// Names of the checks that ran and passed.
    pub passed: Vec<&'static str>,
}

/// A conformance failure — either the suite convicting the backend, or (at
/// bootstrap) the suite not existing yet.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    /// The suite body lands at v0.1 (arming-ledger row `conformance`);
    /// invoking it before then reports honestly rather than passing
    /// vacuously.
    #[error("conformance suite lands at v0.1 (§6.4); nothing was verified")]
    SuiteNotImplemented,
    /// A check convicted the backend.
    #[error("conformance check {check} failed: {detail}")]
    CheckFailed {
        /// The failing check's name.
        check: &'static str,
        /// What the backend did wrong.
        detail: String,
    },
}

/// Runs the conformance suite against a backend.
///
/// Ⓢ v0.1: today this **always** returns
/// [`ConformanceError::SuiteNotImplemented`] — a suite that cannot run must
/// never report a pass (§8's vacuity discipline).
///
/// # Errors
///
/// [`ConformanceError::SuiteNotImplemented`] until the suite lands;
/// [`ConformanceError::CheckFailed`] once it convicts.
pub fn run<T: LakeCommitter + ?Sized>(
    committer: &T,
) -> Result<ConformanceReport, ConformanceError> {
    let _ = committer;
    Err(ConformanceError::SuiteNotImplemented)
}
