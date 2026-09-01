# AGENTS.md — canonical agent instructions

Per docs/seed.md s§9.1. `CLAUDE.md` is exactly `@AGENTS.md`. Keep this file
under 200 lines; path-scoped rules are deliberately absent (KISS — add on
demonstrated need, by issue).

## Identity & mission

You are a loop agent in `allodops/duckspout`: a pure-Rust workspace monorepo
building DuckSpout — a durable, replicated, queryable-hot ingestion layer
with completeness semantics, draining into lake formats (DuckLake first).
This repository is built for fully autonomous AI-driven development: the
structure makes the architecture undeniable, the gates make the rules
enforceable, and the human sits at exactly one choke point (CODEOWNERS on
the protected set).

## Read first, always

1. `CONSTITUTION.md` — the Keep Rules and seed rules; every one is enforced.
2. The design doc of the crate you touch (`docs/design/*.md`; until
   absorption completes, the crate rustdoc's `§` pointer into `DUCKSPOUT.md`).
3. `docs/arming-ledger.toml` — what is armed, what is staged, and at which
   milestone.

## Task frontend

- `just --list` is the discovery surface; every recipe is documented and
  grouped. The chain is always GHA → `just` → `scripts/*.mjs` (or cargo).
- Run **`just ci` before every PR**. Scope caveat: `just ci` reproduces every
  mechanical constituent of `ci-ok` bit-for-bit; the DCO status
  are CI-only (s§5.1).
- A staged gate invoked directly (e.g. `just conformance`) runs for real and
  exits 78 (`STAGED`) if its inputs don't exist — reported as staged, never
  as success.

## The layering rule (s§4, ADR-0008 — condensed)

- Protocol crates (`accept`, `staging`, `replication`, `drain`, `watermark`)
  depend on `duckspout-types` **only**. All protocol×protocol edges are
  banned; every shared port trait lives in `duckspout-types`.
- Exception: `duckspout-drain` → `duckspout-lake-contract` is allowed;
  `duckspout-drain` → `duckspout-lake-ducklake` is banned.
- Only `duckspout-daemon` and the bin crates depend on concrete
  implementations. `duckspout-ctk` reaches protocol crates only through the
  types-defined port traits.
- The invariant engine (`just invariants`) enforces all of this as
  forbidden edges; do not argue with it in a PR — propose an amendment.

## Settled decisions

The decision record (docs/seed.md s§1) and the ADRs (`docs/adr/`) are
settled. **Propose amendments through the s§9.6 procedure; never re-litigate
them in PRs.** An in-house build where a third-party candidate exists needs
an ADR (candidates, why they lost, revisit trigger) — R-third-party-first.

## PR protocol

- Title: Conventional Commit (release-plz feeds on it; squash-merge only).
- Every commit: DCO sign-off (`Signed-off-by:`).
- Fill the PR template completely — especially **verification evidence**
  (`just ci` summary; each new test and what it would catch) and the
  constitution checklist.
- Blockers are native blocked-by issue relations, never body text.
- Trust official documentation of external systems as published; validate
  empirically only where the docs are vague or insufficient for the exact
  guarantee you rely on — and say so explicitly in the PR
  (R-trust-official-docs).
- ACPR (adversarial critic pass review) is NOT a CI gate: the supervising
  session performs it, at its own judgment, on changes to core features —
  protocol crates, specs, ports, gates. Address or rebut its findings
  in-thread like any review.

## Never

- Touch the protected set (s§9.2: `CODEOWNERS`, `CONSTITUTION.md`,
  `invariants.toml`, the ledger, `docs/adr/`, `docs/seed.md`, `floors/`,
  `specs/`, `.github/`, `Justfile`, `scripts/`, the policy/toolchain files)
  without flagging it — the PR will need a human anyway.
- Add a `*.sh` or `*.bash` file (R-no-bash).
- Version a dependency outside `[workspace.dependencies]`.
- Weaken, narrow, or skip a gate — narrowing a check is the named offense
  (§11); gate changes go through the gate-proposal form and the ledger.
- Use `tokio::net`, `Instant::now`, `SystemTime::now`, `thread_rng`, or
  `std::process` in a protocol crate (R-determinism — ports only).
