# DuckSpout task frontend (SEED s§5). The chain is GHA → just → scripts/*.mjs
# (or cargo directly). This file is a proxy: one-liners only, no shell logic.
# `just --list` is the discovery surface.
#
# Recipes whose scripts are absent until their milestone (Ⓜ rows in the arming
# ledger) run through `proc.mjs staged`, which execs the script when it exists
# and exits 78 (STAGED) when it does not — staged is never reported as success.

# --- build ---

# Build the whole workspace
[group('build')]
build:
    cargo build --workspace

# Type-check the whole workspace
[group('build')]
check:
    cargo check --workspace

# Build rustdoc for the workspace, warnings denied (ci.yml:docs)
[group('build')]
doc:
    RUSTDOCFLAGS="--deny warnings" cargo doc --workspace --no-deps

# --- quality ---

# Format all Rust sources in place
[group('quality')]
fmt:
    cargo fmt --all

# Fail if any Rust source is unformatted (ci.yml:fmt)
[group('quality')]
fmt-check:
    cargo fmt --all --check

# Clippy over the workspace, all targets (ci.yml:clippy)
[group('quality')]
clippy:
    cargo clippy --workspace --all-targets

# cargo-deny: advisories, licenses, bans, sources (ci.yml:deny, step 1)
[group('quality')]
deny:
    cargo deny check

# cargo-machete: unused-dependency audit (ci.yml:deny, step 2)
[group('quality')]
machete:
    cargo machete

# Run the invariant engine over invariants.toml (SEED s§7)
[group('quality')]
invariants:
    bun scripts/check-invariants.mjs

# Check the workspace on the declared MSRV toolchain (ci.yml:msrv)
[group('quality')]
msrv:
    cargo +1.96 check --workspace

# Lint .github/ workflows: actionlint then zizmor (ci.yml:workflows)
[group('quality')]
workflows:
    bun scripts/workflows-lint.mjs
# Validate the squash subject / PR title format (scripts/pr-type-label.mjs rule)
[group('quality')]
pr-title-check:
    bun scripts/pr-title-check.mjs


# --- test ---

# Run all tests via cargo-nextest; a flaky test is a red test (retries = 0)
[group('test')]
test:
    cargo nextest run --workspace --retries 0

# Run doctests (nextest cannot; SEED s§3.4)
[group('test')]
test-doc:
    cargo test --doc --workspace

# Loom interleaving exploration of the CTK's own sync primitives (§8.3);
# the `loom` feature swaps in loom's checked types, so the models drive the
# real code (crates/duckspout-ctk/tests/loom.rs)
[group('test')]
test-loom:
    cargo nextest run -p duckspout-ctk --features loom --test loom --retries 0

# Full test suite: nextest + doctests + loom models (ci.yml:test)
[group('test')]
test-all: test test-doc test-loom

# --- spec ---

# Fetch tla2tools.jar + CommunityModules-deps.jar into specs/.tools/, SHA-256 verified
[group('spec')]
tla-install:
    bun scripts/tla.mjs install

# Bounded TLC model check; compares reachable-state counts, runs specs/broken/
[group('spec')]
tla-mc *module:
    bun scripts/tla.mjs mc {{module}}

# TLC simulation mode (nightly)
[group('spec')]
tla-sim *module:
    bun scripts/tla.mjs sim {{module}}

# TLC trace validation of an NDJSON trace against its *Trace.tla spec
[group('spec')]
tla-tv *trace:
    bun scripts/tla.mjs tv {{trace}}

# Trace-conformance driver: fixtures + live harness + real backends (Ⓜ v0.1)
[group('spec')]
conformance:
    bun scripts/lib/proc.mjs staged trace-conformance.mjs

# --- floors ---

# Coverage-floor ratchet: recompute and compare against floors/coverage.toml (Ⓜ v0.1)
[group('floors')]
coverage:
    bun scripts/lib/proc.mjs staged floors.mjs coverage

# iai-callgrind instruction counts vs floors/instr-baselines/, +15% ceiling (Ⓜ v0.1)
[group('floors')]
instr-gate:
    bun scripts/lib/proc.mjs staged instr-gate.mjs

# 1M-record ingest smoke bound (§8.6) (Ⓜ v0.1)
[group('floors')]
smoke:
    bun scripts/lib/proc.mjs staged smoke.mjs

# --- nightly ---

# Mutation-floor ratchet via cargo-mutants (nightly by ADR-0009) (Ⓜ v0.1)
[group('nightly')]
mutants:
    bun scripts/lib/proc.mjs staged floors.mjs mutation

# Nightly nine-metric bench card at RF=2 (§8.7) (Ⓜ v0.4)
[group('nightly')]
bench-card:
    bun scripts/lib/proc.mjs staged bench-card.mjs

# cargo-hack feature-matrix check (nightly.yml:hack-features)
[group('nightly')]
hack-features:
    cargo hack check --workspace --each-feature

# Distributed CTK run: fleet + judge + loadgen (Ⓜ v0.2)
[group('nightly')]
ctk-distributed:
    bun scripts/lib/proc.mjs staged ctk-distributed.mjs

# --- ci ---

# Run every armed cadence="pr" gate from docs/arming-ledger.toml, in order
[group('ci')]
ci:
    bun scripts/lib/proc.mjs ci

# --- agent ---

# Autonomous-loop picker (SEED s§9.4); guarded by DISPATCH_ENABLED
[group('agent')]
dispatch:
    bun scripts/dispatch.mjs
