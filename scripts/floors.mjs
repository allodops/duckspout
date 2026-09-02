#!/usr/bin/env bun
// floors.mjs — the ratcheted-floor gates of §8.6. Subcommands:
//
//   coverage   cargo-llvm-cov line coverage, recomputed, vs.
//              floors/coverage-floor.toml (ledger row `coverage-floor`,
//              armed by issue #45).
//
// Future floors (instruction counts, the ingest smoke bound) get their own
// scripts per the existing convention (instr-gate.mjs, smoke.mjs — s§6.1's
// lower table); this file only grew a `coverage` subcommand because
// `coverage-floor` is the first floors/ row to arm.
//
// §8.6's posture, mechanically: CI recomputes the number on every run —
// never trusts a cached value or a PR's own claim — and a gate whose
// subject vanished (the JSON export missing the expected shape) fails, it
// does not shrug.

import { join } from "node:path";
import { repoRoot } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { group, info, notice } from "./lib/log.mjs";

const FLOOR_FILE = join(repoRoot, "floors", "coverage-floor.toml");

/**
 * Runs `cargo llvm-cov nextest --workspace`, capturing its LLVM-format JSON
 * export (`llvm-cov export -format=text`'s schema — the tool's own
 * documented output, R-trust-official-docs) on stdout while test output
 * streams to stderr live. Returns the parsed `totals` object for the
 * workspace-wide aggregate (`data[0].totals`).
 */
async function measure() {
  const proc = Bun.spawn(
    ["cargo", "llvm-cov", "nextest", "--workspace", "--json", "--summary-only", "--retries", "0"],
    { cwd: repoRoot, stdin: "inherit", stdout: "pipe", stderr: "inherit" },
  );
  const stdout = await new Response(proc.stdout).text();
  const code = await proc.exited;
  if (code !== 0)
    fail(`floors coverage: cargo-llvm-cov exited ${code} — no measurement, no green (fail-closed)`);
  let report;
  try {
    report = JSON.parse(stdout);
  } catch (e) {
    fail(`floors coverage: cargo-llvm-cov's --json output did not parse: ${e.message}`);
  }
  const totals = report?.data?.[0]?.totals;
  if (!totals)
    fail("floors coverage: cargo-llvm-cov's JSON export has no data[0].totals — its shape changed or the run measured nothing");
  return totals;
}

async function coverageMain() {
  const floor = (await import(FLOOR_FILE, { with: { type: "toml" } })).default;
  if (typeof floor.floor !== "number" || typeof floor.metric !== "string")
    fail(`floors coverage: ${FLOOR_FILE} needs a string 'metric' and a numeric 'floor'`);

  info(
    `floors coverage: recomputing '${floor.metric}' coverage over the whole workspace (cargo-llvm-cov nextest — never trusting a prior number, §8.6)`,
  );
  const totals = await measure();
  const measured = totals[floor.metric]?.percent;
  if (typeof measured !== "number")
    fail(`floors coverage: cargo-llvm-cov's totals has no numeric '${floor.metric}.percent' (${FLOOR_FILE}'s 'metric' value)`);

  info(`floors coverage: measured ${measured.toFixed(2)}% ${floor.metric} coverage; floor is ${floor.floor}%`);
  if (measured < floor.floor)
    fail(
      `floors coverage: ${measured.toFixed(2)}% < floor ${floor.floor}% (${FLOOR_FILE}) — coverage regressed below the ratchet. Raising the floor is an ordinary commit; lowering it needs a reviewed, named decision in the commit message (§8.6) — this failure is neither.`,
    );
  notice(`floors coverage: ${measured.toFixed(2)}% >= floor ${floor.floor}% — green`);
}

async function main() {
  const [cmd] = process.argv.slice(2);
  if (cmd === "coverage") await group("floors: coverage", coverageMain);
  else fail(`floors.mjs: unknown subcommand '${cmd ?? ""}' (expected: coverage)`);
}

await main();
