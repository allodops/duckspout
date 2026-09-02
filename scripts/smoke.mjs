#!/usr/bin/env bun
// Per-PR 1M-record ingest smoke gate (§8.6; ledger row smoke, issue #47).
// Runs crates/duckspout-daemon/tests/smoke_volume.rs — 1,000,000 synthetic
// OTLP log records through the real daemon (accept → stage → drain →
// DuckLake), release-mode — and times the whole run, failing if it exceeds
// floors/smoke-bound.toml's measured bound. §8.6: "catching order-of-
// magnitude regressions cheaply" — a coarse volume/time bound, not a
// per-PR latency claim (that distinction is instr-gate/ADR-0005's, never
// swapped, per ADR-0005's own text).
//
// `bun scripts/smoke.mjs --update-baseline` re-runs and rewrites the floor
// file — an explicit, reviewed baseline-update commit (§8.6), never run
// silently by the gate itself.

import { join } from "node:path";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { group, info, notice } from "./lib/log.mjs";

const CEILING = 1.5; // generous: an order-of-magnitude bound, not a tight one (§8.6)
const FLOOR_PATH = join(repoRoot, "floors", "smoke-bound.toml");
const TEST_ARGS = [
  "cargo",
  "nextest",
  "run",
  "--release",
  "-p",
  "duckspout-daemon",
  "--test",
  "smoke_volume",
  "--run-ignored",
  "ignored-only",
  "--retries",
  "0",
  "--no-capture",
];

// `cargo nextest run` compiles-then-runs in one invocation; on a cold CI
// checkout the release build of the whole dependent crate graph (daemon +
// bundled DuckDB) dominates and swamps any real per-PR timing signal —
// measured locally (warm target dir) this looked like ~28s, on a fresh GHA
// runner the SAME invocation took 886.9s, entirely compile time. §8.6's
// bound is about the 1M-record SCENARIO, never build latency (that
// conflation is exactly the distinction ADR-0005 draws for instr-gate too)
// — so the build is run once, untimed, and only the second (already-built,
// nextest-skips-recompiling) invocation is what `measure()` clocks.
async function measure() {
  await group("build smoke_volume (release, untimed)", () => run([...TEST_ARGS, "--no-run"]));
  const start = performance.now();
  const code = await group("run smoke_volume (release, 1M records)", () => run(TEST_ARGS));
  const elapsedSeconds = (performance.now() - start) / 1000;
  if (code !== 0) fail(`smoke: smoke_volume test exited ${code}`);
  return elapsedSeconds;
}

async function updateBaseline() {
  const seconds = await measure();
  const floor = `# Measured, ratcheted bound for the smoke gate (§8.6; issue #47).
# Recomputed by \`bun scripts/smoke.mjs --update-baseline\` — an explicit,
# reviewed baseline-update commit, never run silently by the gate itself.
# Ceiling applied by scripts/smoke.mjs: seconds * 1.5 (generous — an
# order-of-magnitude regression catch, not a tight latency claim).
seconds = ${Math.ceil(seconds)}
measured_at = "${new Date().toISOString().slice(0, 10)}"
`;
  await Bun.write(FLOOR_PATH, floor);
  notice(`smoke: baseline set to ${Math.ceil(seconds)}s (measured ${seconds.toFixed(1)}s)`);
}

async function gate() {
  const floorFile = Bun.file(FLOOR_PATH);
  if (!(await floorFile.exists()))
    fail(`smoke: no baseline at ${FLOOR_PATH} (never auto-created — run --update-baseline explicitly)`);
  const floor = (await import(FLOOR_PATH, { with: { type: "toml" } })).default;
  const baseline = floor.seconds;
  if (typeof baseline !== "number") fail("smoke: floor file has no numeric 'seconds' field");
  const elapsed = await measure();
  const ceiling = baseline * CEILING;
  info(`smoke: ${elapsed.toFixed(1)}s (baseline ${baseline}s, ceiling ${ceiling.toFixed(1)}s)`);
  if (elapsed > ceiling)
    fail(`smoke: ${elapsed.toFixed(1)}s > ceiling ${ceiling.toFixed(1)}s (baseline ${baseline}s * 1.5, §8.6)`);
  info("smoke: within the baseline's ceiling");
}

if (import.meta.main) {
  if (process.argv.includes("--update-baseline")) await updateBaseline();
  else await gate();
}
