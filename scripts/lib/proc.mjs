#!/usr/bin/env bun
// Exit-code policy (SEED s§5.2) + the ledger-driven runners (SEED s§5.1).
//
// Policy: uniform failure = nonzero exit + one structured summary line; exit
// code 78 is reserved for STAGED (a gate whose inputs do not exist yet ran
// for real and reported itself staged — never success); no silent catches.
//
// CLI modes:
//   bun scripts/lib/proc.mjs ci                 run every armed cadence="pr"
//                                               ledger gate, in order
//   bun scripts/lib/proc.mjs staged <script> [args…]
//                                               exec scripts/<script> if it
//                                               exists, else exit 78 (STAGED)

import { join } from "node:path";
import { repoRoot, run } from "./sh.mjs";
import { error, group, info, notice } from "./log.mjs";

/** Reserved exit code: gate ran but its inputs are staged (absent until their milestone). */
export const STAGED = 78;

/**
 * Uniform failure: one structured summary line, exit 1.
 * @param {string} msg
 * @returns {never}
 */
export function fail(msg) {
  error(`FAIL: ${msg}`);
  process.exit(1);
}

const LEDGER = "docs/arming-ledger.toml";

/** `just ci`: run the recipes of every armed cadence="pr" ledger row, in order,
 * streaming output, stopping on the first failure (SEED s§5.1, s§6.5). */
async function ciMain() {
  let ledger;
  try {
    ledger = (await import(join(repoRoot, LEDGER), { with: { type: "toml" } })).default;
  } catch (e) {
    fail(`ci: cannot read ${LEDGER}: ${e.message}`);
  }
  const gates = (ledger.gate ?? []).filter(
    (g) => g.status === "armed" && g.cadence === "pr",
  );
  if (gates.length === 0) fail(`ci: no armed cadence="pr" gates in ${LEDGER}`);
  info(`ci: ${gates.length} armed pr gates: ${gates.map((g) => g.id).join(", ")}`);
  for (const gate of gates) {
    const code = await group(`${gate.id} (just ${gate.recipe})`, () =>
      run(["just", gate.recipe]),
    );
    if (code !== 0) {
      error(`FAIL: ci: gate '${gate.id}' (just ${gate.recipe}) exited ${code}`);
      process.exit(code);
    }
  }
  notice(`ci: all ${gates.length} armed pr gates green`);
}

/** `staged <script> [args…]`: exec the script when present, exit 78 when the
 * file is absent until its milestone (SEED s§5.1: staged, never success). */
async function stagedMain(script, args) {
  if (!script) fail("staged: usage: proc.mjs staged <script> [args…]");
  const path = join(repoRoot, "scripts", script);
  if (!(await Bun.file(path).exists())) {
    notice(`STAGED: scripts/${script} absent until its milestone (see ${LEDGER})`);
    process.exit(STAGED);
  }
  process.exit(await run(["bun", path, ...args]));
}

if (import.meta.main) {
  const [mode, ...rest] = process.argv.slice(2);
  if (mode === "ci") await ciMain();
  else if (mode === "staged") await stagedMain(rest[0], rest.slice(1));
  else fail(`proc.mjs: unknown mode '${mode ?? ""}' (expected: ci | staged)`);
}
