//! `duckspout-judge` library surface (§8.4, D-5): NDJSON journal ingestion
//! plus the judge predicates built on top of it. Split out of `main.rs` so
//! the parsing and checking logic is unit-testable without a real fleet run
//! (mirroring `duckspout-loadgen`'s own lib/bin split).
//!
//! # Crate-placement note (#205)
//!
//! This infrastructure lives here, in the judge's own crate, rather than in
//! `duckspout-ctk`: `duckspout-ctk` is the CTK's *in-memory-tier* toolkit —
//! deterministic doubles for the four runtime ports plus armed-vs-fired
//! fault accounting (its own module docs) — a concern about producing
//! controlled executions, not about grading recorded ones after the fact.
//! The judge is deliberately its own standalone binary (D-5, §8.4: "the
//! process that runs the system must not be the process that grades it"),
//! and journal parsing/checking is used by nothing else in the workspace
//! today. Putting it in `duckspout-ctk` would create a dependency edge from
//! the run-time toolkit to grading logic for no present consumer, purely on
//! speculation that a future crate might want it — the KISS call is to keep
//! it local until a second, non-judge consumer actually needs it (matching
//! `AGENTS.md`'s "add on demonstrated need" posture), while still splitting
//! it into its own modules (below) so a later extraction, if ever needed, is
//! a mechanical move rather than a redesign.
//!
//! Design home: `docs/verification.md` (lands at absorption; until then see
//! `DUCKSPOUT.md` §8.4).
//!
//! # `attempt`-matching module removed (ACPR finding LOW-MEDIUM-7)
//!
//! An earlier revision of this crate shipped an `attempt` module — a
//! generic per-node FIFO attempt/resolution matcher, meant as shared
//! plumbing for #206/#207/#208's future predicates (the "sharpest fault
//! window" §8.4 names: `PutPart` → `{LakeCommitOk, LakeCommitAbort,
//! LakeCommitIndeterminate}`). It had zero consumers in this crate — the
//! #205 predicate this crate actually ships (`zero_acked_lost`) does not
//! need it — which directly contradicts this same PR's own stated KISS
//! posture elsewhere ("keep it local until a second, non-judge consumer
//! actually needs it"). ACPR review flagged the same reasoning applies to a
//! second, in-crate consumer just as much as an out-of-crate one, so it was
//! removed rather than kept as speculative infrastructure; whichever of
//! #206/#207/#208 needs attempt/resolution matching first should introduce
//! it fresh, sized to what that predicate actually needs.

#![forbid(unsafe_code)]

pub mod final_state;
pub mod journal;
pub mod predicates;
pub mod runner;
pub mod summary;
