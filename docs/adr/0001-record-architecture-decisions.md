# ADR-0001: Record architecture decisions

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1, s§8.3

## Context and Problem Statement

This repository is developed by an autonomous agent loop. Decisions that live
only in chat transcripts or PR threads get re-litigated by every new agent;
docs/seed.md s§1 explicitly marks its decision record "settled; do not
re-litigate". Where do decisions live, and in what form?

## Decision Outcome

Use Architecture Decision Records in MADR format under `docs/adr/`,
numbered sequentially. Every ADR carries **Candidates considered** and
**Revisit when** sections where applicable — both are mandatory for
R-third-party-first exceptions (D-12: ADR-0004, ADR-0007) and for flagged
deviations (ADR-0009). `docs/adr/` is in the protected set (s§9.2), so
creating or amending an ADR is human-approved; amendments go through the
s§9.6 procedure, never through re-litigation in ordinary PRs.

## Consequences

- Good: settled decisions have one home, one format, and a human gate.
- Bad: recording a decision costs a protected-set PR (a human click) — the
  price of "settled" meaning something.

## Revisit when

Never wholesale; individual ADRs carry their own revisit triggers.
