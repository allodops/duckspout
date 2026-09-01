# ADR-0002: `bundled` DuckDB compilation does not violate the no-C++ rule

- Status: accepted
- Date: 2026-08-31
- Cited by: docs/seed.md s§1 (interpretive ruling); DUCKSPOUT.md §10.2

## Context and Problem Statement

§10.2 mandates "no first-party C++ in Repo 1". The `duckdb` crate's
`bundled` feature compiles the vendored DuckDB engine (C++) inside this
workspace's build. Does enabling it violate the rule?

## Decision Outcome

No. The rule bans first-party C++ *code*, not building upstream's. The
`duckdb` crate wraps the stable C API only (satisfying §10.2's interop
constraint), and `bundled` merely compiles DuckDB's own sources as shipped.
No C++ source file is authored, patched, or vendored-with-modifications in
this repository; the invariant engine's `banned-file` scope covers
first-party trees, not `target/`.

## Consequences

- Good: hermetic builds — no system DuckDB dependency, one pinned engine
  version via the compatibility matrix (`compat-matrix.toml` row 1).
- Bad: long cold-build times for the `duckdb` crate; mitigated by
  `Swatinem/rust-cache`.

## Revisit when

A supported dynamic-linking path gives the same version pinning with
meaningfully better build times, or the compatibility matrix moves to a
DuckDB version the `bundled` feature does not carry.
