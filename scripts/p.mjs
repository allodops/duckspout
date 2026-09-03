#!/usr/bin/env bun
// P toolchain wrapper (#131, ADR-0012 step 2 plumbing): install | check.
// Phase 1 -- local runs only, no CI wiring (deferred to #134). Requires
// .NET SDK 8.0 + a JRE on PATH (the P checker's official prerequisites);
// `just p-install` installs the pinned `p` CLI tool itself, tool-path
// scoped into p/.tools/ so it never touches the global dotnet tool cache.
//
// This file is the authoritative pin for the P tool's NuGet version
// (Appendix A analog): a specific version on NuGet is immutable once
// published, so version-pinning here is NuGet's equivalent of tla.mjs's
// SHA-256 jar pin -- there is no separate content hash to check against.

import { join } from "node:path";
import { existsSync } from "node:fs";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { info } from "./lib/log.mjs";

const P_DIR = join(repoRoot, "p");
const TOOLS_DIR = join(P_DIR, ".tools");
const P_VERSION = "3.1.0";
const P_BIN = join(TOOLS_DIR, "p");

function requireDotnet() {
  if (!Bun.which("dotnet"))
    fail("p: `dotnet` not found on PATH -- install .NET SDK 8.0 first (https://dot.net), then run `just p-install`");
}

async function install() {
  requireDotnet();
  if (existsSync(P_BIN)) {
    const { code, out } = await captured([P_BIN, "--version"]);
    if (code === 0 && out.includes(P_VERSION)) {
      info(`p: P ${P_VERSION} already present and verified`);
      return;
    }
  }
  info(`p: installing P ${P_VERSION} (tool-path: p/.tools, not global)`);
  const code = await run([
    "dotnet", "tool", "install",
    "--tool-path", TOOLS_DIR,
    "--version", P_VERSION,
    "P",
  ]);
  if (code !== 0) fail(`p install: \`dotnet tool install\` exited ${code}`);
}

async function captured(cmd) {
  const proc = Bun.spawn(cmd, { cwd: repoRoot, stdout: "pipe", stderr: "pipe" });
  const out = await new Response(proc.stdout).text();
  return { code: await proc.exited, out };
}

function requireTool() {
  if (!existsSync(P_BIN)) fail("p: P tool missing — run `just p-install` first");
}

/** Compile + check one model: p/<name>/*.p, test case <name>. */
async function check(name) {
  if (!name) fail("p check: no model given — usage: `just p-check <Model>`");
  requireTool();
  const modelDir = join(P_DIR, name);
  if (!existsSync(modelDir))
    fail(`p check: ${name}: no p/${name}/ directory — models land at v0.2 (#132); a gate with no inputs is red, never green (s§5.1)`);
  info(`p check: ${name}`);
  const compileCode = await run([P_BIN, "compile", "-pp", join(modelDir, `${name}.pproj`)], { cwd: modelDir });
  if (compileCode !== 0) fail(`p check: ${name}: compile failed (exit ${compileCode})`);
  const checkCode = await run([P_BIN, "check", "-tc", name, "-i", "1000"], { cwd: modelDir });
  if (checkCode !== 0) fail(`p check: ${name}: checker found a violation (exit ${checkCode})`);
  info(`p check: ${name}: 1000 schedules explored, no bugs found`);
}

const [cmd, arg] = process.argv.slice(2);
if (cmd === "install") await install();
else if (cmd === "check") await check(arg);
else fail(`p: unknown command '${cmd ?? ""}' — usage: p install | p check <Model>`);
