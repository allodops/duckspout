# spike/ — the §12.1 spike charter

Per docs/seed.md s§11 step 7 and D-11. This directory is a **quarantined,
throwaway** Cargo project: workspace-excluded (`exclude = ["spike"]` at the
root, plus its own empty `[workspace]` table for double isolation).

## Scope: one thread of value, end to end (DUCKSPOUT.md §12.1)

OTLP in → hot table → **Airport-served query** → drain → DuckLake commit →
**one SQL query unioning hot and cold with `complete_through` visible**.

The spike exists to force the three riskiest seams *before* any of them is
load-bearing:

1. **Transaction-lifecycle pinning in the DuckDB extension** — prototyped
   throwaway inside `spike/`, community-extension template shape (the
   `duckspout-duckdb` repo proper is deferred to v0.4).
2. **The atomic {add files + watermark} LakeCommit.**
3. **The hot∪cold union.**

## Budget

**Two weeks.** The budget is the point: the spike answers "which of these
seams bites, and how hard" — it does not build the product.

## Rules of the quarantine (D-11)

- Workspace-excluded, so no cargo gate reaches it. The repo-wide rules that
  still bind it: the `banned-file` globs (no `*.sh`/`*.bash`, ever) and the
  anyone→spike forbidden dependency edges (s§7) — no workspace crate may
  ever depend on spike code.
- fmt-clean code is requested as a **courtesy, not a gate**.
- Nothing in here is reviewed to product standards; nothing in here is a
  precedent.

## Lesson-harvest protocol

The spike's only durable output is **lessons**: ADRs, issues, and revised
constants — **never code promoted into `crates/`**. Before deletion, every
lesson worth keeping is filed (ADR via the protected-set path, issue via the
task form, constant change via ordinary PR); anything not filed is
deliberately discarded.

## Deletion criterion

`spike/` is **deleted at v0.1** — when the first real specs and the v0.1
gates arm (arming ledger), the spike has served its purpose. Deletion is a
normal PR; the lessons survive it, the code does not.
