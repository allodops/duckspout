#!/usr/bin/env bun
// floors.mjs — the ratcheted-floor gates of §8.6. Subcommands:
//
//   coverage   cargo-llvm-cov line coverage, recomputed, vs.
//              floors/coverage-floor.toml (ledger row `coverage-floor`,
//              armed by issue #45).
//   mutation   cargo-mutants kill rate over the protocol crates, recomputed,
//              vs. floors/mutation-floor.toml (ledger row `mutation-floor`,
//              ADR-0009, armed by issue #49). Nightly cadence only — never
//              part of `just ci`'s armed cadence="pr" sequence (ADR-0009's
//              flagged deviation: cargo-mutants over this workspace is
//              hours-scale).
//
// Future floors get their own scripts per the existing convention
// (instr-gate.mjs, smoke.mjs — s§6.1's lower table); this file grows a
// subcommand per floors/ row instead, since coverage and mutation are both
// literally "a checked-in floors/*.toml + a recomputed percentage" shaped.
//
// §8.6's posture, mechanically: CI recomputes the number on every run —
// never trusts a cached value or a PR's own claim — and a gate whose
// subject vanished (the JSON export missing the expected shape) fails, it
// does not shrug.

import { join } from "node:path";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { group, info, notice } from "./lib/log.mjs";

const FLOOR_FILE = join(repoRoot, "floors", "coverage-floor.toml");
const MUTATION_FLOOR_FILE = join(repoRoot, "floors", "mutation-floor.toml");
const MUTANTS_OUT_PARENT = repoRoot; // cargo-mutants creates <parent>/mutants.out/ (gitignored)

// The protocol crates (AGENTS.md's "layering rule" list — accept, staging,
// replication, drain, watermark): the ones D-2/ADR-0008's determinism and
// port-boundary rules bind tightest, and therefore the ones a missed
// mutation matters most for. Concrete-impl/composition crates
// (duckspout-daemon, duckspout-lake-ducklake, the bin crates, duckspout-ctk)
// are deliberately out of scope — same "protocol crates" boundary AGENTS.md
// already draws for the layering rule, reused here rather than inventing a
// second crate grouping.
const PROTOCOL_CRATES = [
  "duckspout-accept",
  "duckspout-staging",
  "duckspout-replication",
  "duckspout-drain",
  "duckspout-watermark",
];

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

/**
 * Runs `cargo mutants` over `PROTOCOL_CRATES`, streaming its own progress
 * output live, then reads back its `mutants.out/outcomes.json` — the
 * tool's own documented machine-readable summary (R-trust-official-docs) —
 * for the `caught`/`missed` totals. `unviable` (didn't build) and
 * `timeout` mutants are excluded from both the numerator and denominator:
 * neither is a "the tests exercised this and passed" result, so folding
 * either into the rate would misstate what was actually tested (the
 * standard mutation-score definition most tools, e.g. PIT, use).
 */
async function measureMutation() {
  const args = [
    "cargo",
    "mutants",
    "--no-times", // deterministic-ish log lines; irrelevant to the parsed result either way
    ...PROTOCOL_CRATES.flatMap((pkg) => ["-p", pkg]),
    "-o",
    MUTANTS_OUT_PARENT,
  ];
  // cargo-mutants' own exit codes (0 = all caught, 2 = some missed, ...) are
  // not what this gate checks — the *rate* against the floor is — so a
  // non-zero exit here is not itself a failure; only a genuinely broken run
  // (couldn't build at all, wrote nothing) is.
  await run(args);
  const outcomesPath = join(MUTANTS_OUT_PARENT, "mutants.out", "outcomes.json");
  const file = Bun.file(outcomesPath);
  if (!(await file.exists()))
    fail(`floors mutation: no ${outcomesPath} — cargo-mutants wrote nothing (a broken run, not a measurement)`);
  const outcomes = await file.json();
  const { caught, missed, total_mutants: total } = outcomes;
  if (typeof caught !== "number" || typeof missed !== "number" || typeof total !== "number")
    fail(`floors mutation: ${outcomesPath} has no numeric caught/missed/total_mutants — its shape changed or the run measured nothing`);
  if (caught + missed === 0)
    fail(`floors mutation: 0 caught + 0 missed out of ${total} mutants — nothing was actually tested (fail-closed, §11: a check whose subject vanished fails, it does not shrug)`);
  return { caught, missed, total, killRate: (caught / (caught + missed)) * 100 };
}

async function mutationMain() {
  const floor = (await import(MUTATION_FLOOR_FILE, { with: { type: "toml" } })).default;
  if (typeof floor.floor !== "number")
    fail(`floors mutation: ${MUTATION_FLOOR_FILE} needs a numeric 'floor'`);

  info(
    `floors mutation: recomputing the mutation kill rate over ${PROTOCOL_CRATES.join(", ")} (cargo-mutants — never trusting a prior number, §8.6; nightly cadence, ADR-0009)`,
  );
  const { caught, missed, total, killRate } = await measureMutation();
  info(
    `floors mutation: ${caught} caught / ${missed} missed (${killRate.toFixed(2)}% kill rate, ${total} mutants total including unviable/timeout); floor is ${floor.floor}%`,
  );
  if (killRate < floor.floor)
    fail(
      `floors mutation: ${killRate.toFixed(2)}% < floor ${floor.floor}% (${MUTATION_FLOOR_FILE}) — the mutation kill rate regressed below the ratchet. Raising the floor is an ordinary commit; lowering it needs a reviewed, named decision in the commit message (§8.6) — this failure is neither. Per ADR-0009, a red nightly run auto-files a blocking issue.`,
    );
  notice(`floors mutation: ${killRate.toFixed(2)}% >= floor ${floor.floor}% — green`);
}

async function updateMutationBaseline() {
  const { caught, missed, total, killRate } = await measureMutation();
  const margin = 2.0; // percentage points of headroom, same posture as coverage-floor's measured-minus-margin pick
  const floorValue = Math.max(0, Math.floor((killRate - margin) * 10) / 10);
  const toml = `# Measured, ratcheted mutation-kill-rate floor (§8.6, ADR-0009; issue #49).
# Recomputed by \`bun scripts/floors.mjs mutation-update-baseline\` — an
# explicit, reviewed baseline-update commit, never run silently by the gate
# itself. Nightly cadence: the merge queue never runs this (ADR-0009).
#
# Scope: ${PROTOCOL_CRATES.join(", ")} (AGENTS.md's "protocol crates" list).
# Metric: caught / (caught + missed) * 100 — unviable and timeout mutants
# excluded from both sides (neither is a tested-and-passed result).
#
# Measured ${new Date().toISOString().slice(0, 10)}: ${caught} caught, ${missed} missed,
# ${total} mutants total (including unviable/timeout) => ${killRate.toFixed(2)}%.
# Floor set to measured minus a ${margin}pp margin, rounded down to 1dp —
# absorbs ordinary run-to-run noise without being so loose it stops
# ratcheting real regressions (same posture as floors/coverage-floor.toml).
floor = ${floorValue}
`;
  await Bun.write(MUTATION_FLOOR_FILE, toml);
  notice(`floors mutation: baseline set to floor = ${floorValue}% (measured ${killRate.toFixed(2)}%)`);
}

async function main() {
  const [cmd] = process.argv.slice(2);
  if (cmd === "coverage") await group("floors: coverage", coverageMain);
  else if (cmd === "mutation") await group("floors: mutation", mutationMain);
  else if (cmd === "mutation-update-baseline") await group("floors: mutation (update baseline)", updateMutationBaseline);
  else fail(`floors.mjs: unknown subcommand '${cmd ?? ""}' (expected: coverage | mutation | mutation-update-baseline)`);
}

await main();
