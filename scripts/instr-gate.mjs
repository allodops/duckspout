#!/usr/bin/env bun
// Per-PR instruction-count gate (§8.6, ADR-0005; ledger row instr-gate,
// issue #46). Runs the iai-callgrind harnesses on the ack-path hot
// functions (§4.3: Accept's decode step, staging's StageCommit step),
// reads each run's `summary.json` (iai-callgrind's `--save-summary`
// output, schema v6 as of iai-callgrind 0.16), extracts the total `Ir`
// (instructions-executed) count, and fails the gate if it exceeds its
// floor file's baseline by more than 15% (ADR-0005's ceiling).
//
// Baselines live in floors/instr-baselines/<crate>-<bench>.json — recomputed
// numbers, not descriptions (§8.6's anti-gaming stance). Raising a baseline
// (a real, intentional slowdown) is an ordinary commit; lowering the
// *ceiling* is never done — the ceiling is the fixed +15%, not a per-PR
// knob. `bun scripts/instr-gate.mjs --update-baseline` re-measures and
// rewrites every floor file — an explicit, reviewed baseline-update commit
// (§8.6), never run silently by the gate itself.

import { join } from "node:path";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { group, info, notice } from "./lib/log.mjs";

const CEILING = 1.15; // ADR-0005: baseline + 15%

/** One target per ack-path hot function (§4.3). Add a row here + a
 * `floors/instr-baselines/<id>.json` baseline to gate another function. */
const TARGETS = [
  {
    id: "duckspout-accept-decode",
    crate: "duckspout-accept",
    bench: "decode",
  },
  {
    id: "duckspout-staging-commit",
    crate: "duckspout-staging",
    bench: "commit",
  },
];

// cargo itself honors CARGO_TARGET_DIR over the repo-relative `target/`
// default; this script must resolve summary.json in the same place cargo
// actually wrote it, not just the default (a shared/override target dir is
// an ordinary local dev setup, not a special case to special-case).
const CARGO_TARGET_DIR = process.env.CARGO_TARGET_DIR
  ? join(process.env.CARGO_TARGET_DIR)
  : join(repoRoot, "target");

/** Finds the one summary.json iai-callgrind wrote for this crate's bench run
 * (under `<target-dir>/iai/<package>/…`, exact subpath derived from the
 * harness's group/benchmark names — not hardcoded here, since it is
 * iai-callgrind's internal layout, not this repo's contract). Each bench
 * file defines exactly one group with one benchmark, so exactly one
 * summary.json must exist; more or fewer is a fail-closed surprise, not a
 * guess. */
async function summaryPath(target) {
  const dir = join(CARGO_TARGET_DIR, "iai", target.crate);
  const glob = new Bun.Glob("**/summary.json");
  const matches = [];
  for await (const rel of glob.scan({ cwd: dir })) matches.push(join(dir, rel));
  if (matches.length === 0)
    fail(`instr-gate: ${target.id}: no summary.json under ${dir}/ (iai-callgrind produced nothing?)`);
  if (matches.length > 1)
    fail(`instr-gate: ${target.id}: ${matches.length} summary.json files under ${dir}/ — expected exactly one (bench file grew a second benchmark?): ${matches.join(", ")}`);
  return matches[0];
}

function floorPath(target) {
  return join(repoRoot, "floors", "instr-baselines", `${target.id}.json`);
}

/** Pulls the total `Ir` (instructions executed) metric out of a v6
 * summary.json's Callgrind profile. Fails closed on any shape surprise —
 * §8.6: "a check whose subject vanished fails, it does not shrug." */
function extractInstructions(target, summary) {
  const profile = (summary.profiles ?? []).find((p) => p.tool === "Callgrind");
  if (!profile) fail(`instr-gate: ${target.id}: no Callgrind profile in summary.json`);
  const callgrind = profile.summaries?.total?.summary?.Callgrind;
  if (!callgrind) fail(`instr-gate: ${target.id}: no Callgrind metric summary in summary.json`);
  const ir = callgrind.Ir;
  if (!ir) fail(`instr-gate: ${target.id}: no 'Ir' (instructions) metric in summary.json`);
  const metric = ir.metrics?.Both?.[0] ?? ir.metrics?.Left ?? ir.metrics?.Right;
  const value = metric?.Int;
  if (typeof value !== "number") fail(`instr-gate: ${target.id}: 'Ir' metric is not an integer count`);
  return value;
}

async function measure(target) {
  const code = await run([
    "cargo",
    "bench",
    "-p",
    target.crate,
    "--bench",
    target.bench,
    "--",
    "--save-summary",
  ]);
  if (code !== 0) fail(`instr-gate: ${target.id}: cargo bench exited ${code}`);
  const path = await summaryPath(target);
  const file = Bun.file(path);
  if (!(await file.exists()))
    fail(`instr-gate: ${target.id}: expected summary.json at ${path} (iai-callgrind layout drifted?)`);
  return extractInstructions(target, await file.json());
}

async function updateBaselines() {
  for (const target of TARGETS) {
    const instructions = await group(`measure ${target.id}`, () => measure(target));
    const floor = {
      instructions,
      measured_at: new Date().toISOString().slice(0, 10),
      note: "recomputed by `bun scripts/instr-gate.mjs --update-baseline` (§8.6: an explicit baseline-update commit, reviewed line by line)",
    };
    await Bun.write(floorPath(target), `${JSON.stringify(floor, null, 2)}\n`);
    notice(`instr-gate: ${target.id}: baseline set to ${instructions} instructions`);
  }
}

async function gate() {
  let failures = 0;
  for (const target of TARGETS) {
    const floorFile = Bun.file(floorPath(target));
    if (!(await floorFile.exists())) {
      fail(`instr-gate: ${target.id}: no baseline at ${floorPath(target)} (never auto-created — run --update-baseline explicitly)`);
    }
    const floor = await floorFile.json();
    const baseline = floor.instructions;
    if (typeof baseline !== "number")
      fail(`instr-gate: ${target.id}: floor file has no numeric 'instructions' field`);
    const measured = await group(`measure ${target.id}`, () => measure(target));
    const ceiling = Math.ceil(baseline * CEILING);
    const pct = (((measured - baseline) / baseline) * 100).toFixed(1);
    info(`instr-gate: ${target.id}: ${measured} instructions (baseline ${baseline}, ceiling ${ceiling}, ${pct}%)`);
    if (measured > ceiling) {
      error_over_ceiling(target, measured, baseline, ceiling);
      failures += 1;
    }
  }
  if (failures > 0) fail(`instr-gate: ${failures} target(s) exceeded the +15% ceiling (ADR-0005)`);
  info("instr-gate: every target within its baseline +15% ceiling");
}

function error_over_ceiling(target, measured, baseline, ceiling) {
  notice(
    `instr-gate: ${target.id}: ${measured} > ceiling ${ceiling} (baseline ${baseline} +15%, ADR-0005) — ` +
      "a real regression gets fixed forward; a real, intentional cost gets an explicit baseline-update commit, never a raised ceiling",
  );
}

if (import.meta.main) {
  if (process.argv.includes("--update-baseline")) await updateBaselines();
  else await gate();
}
