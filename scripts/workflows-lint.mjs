#!/usr/bin/env bun
// `just workflows` (ci.yml:workflows): actionlint then zizmor over
// .github/workflows, exit codes forwarded — both run even when the first is
// red, so one pass reports every finding; the exit code is the first nonzero.
// Tools are pinned/installed by .github/actions/setup/action.yml in CI and
// expected on PATH locally.

import { join } from "node:path";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { group, info } from "./lib/log.mjs";

const WORKFLOWS = join(repoRoot, ".github", "workflows");
if (!existsSync(WORKFLOWS))
  fail("workflows-lint: .github/workflows does not exist — nothing to lint is a red gate, not a green one (fail-closed)");

/** Resolve a tool from PATH, with the conventional Go bin dir as a fallback. */
function tool(name) {
  const found = Bun.which(name) ?? [join(homedir(), "go", "bin", name)].find(existsSync);
  if (!found) fail(`workflows-lint: '${name}' not found on PATH (CI installs it via the setup action)`);
  return found;
}

const results = [];
// actionlint discovers .github/workflows itself when run from the repo root.
results.push(await group("actionlint", () => run([tool("actionlint")])));
results.push(await group("zizmor", () => run([tool("zizmor"), WORKFLOWS])));

const firstFailure = results.find((code) => code !== 0);
if (firstFailure !== undefined) process.exit(firstFailure);
info("workflows-lint: actionlint + zizmor green");
