# ADR-0007: The invariant engine is in-house (R-third-party-first exception)

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (D-7, D-12 recorded exception), s§7

## Context and Problem Statement

The repo-shape rules — forbidden dependency edges, banned files, banned
source patterns, golden-manifest diffs, pairing checks — need one enforcer
(`scripts/check-invariants.mjs` reading declarative `invariants.toml`).
R-third-party-first requires an ADR to build it in-house.

## Candidates considered

- **cargo-deny `bans`** — lost: workspace-global; cannot express per-crate
  edges ("`duckspout-drain` may depend on `-lake-contract`, no other
  protocol crate may"). It stays in use for advisories/licenses/sources —
  the engine does not duplicate what it does well.
- **clippy `disallowed_methods` / `disallowed_types`** — lost: APIs only;
  cannot audit dependency edges, file existence, or cross-file pairings.
- **In-house engine** (chosen) — one engine hosts every repo-shape rule
  kind: `forbidden-edge` (via `cargo metadata`), `banned-file`,
  `banned-source`, `golden-manifest`, `pairing`.

## Decision Outcome

In-house. No candidate covers per-crate dependency edges, and none covers
the pairing rules (constitution-mechanism, trace-mapping, ledger-integrity,
tool-pins, edge-audit-domain, workspace-inheritance) at all — the rules that
make "a gate absent from both CI and ledger" *detectable*. Splitting rules
across three partial tools would leave the composition itself unaudited.

## Consequences

- Good: rules are data (`invariants.toml`); one engine, one report format,
  one protected-set home for the enforcer (s§9.2).
- Bad: in-house code on the trust path; mitigated by the engine living in
  the protected set — the enforcer cannot be edited by the PRs it gates
  without a human click.

## Revisit when

A maintained third-party tool gains per-crate-edge audits — then re-run this
ADR with it as a candidate for at least the `forbidden-edge` kind.
