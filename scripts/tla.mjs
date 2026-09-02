#!/usr/bin/env bun
// TLC wrapper (SEED s§5.3): install | mc | sim | tv. Requires Temurin 21 on
// PATH (setup action in CI; `just tla-install` fetches the jars).
//
// This file is the authoritative pin for the two TLA+ jars (Appendix A):
// URL + SHA-256 constants below. Set TLA_SKIP_VERIFY=1 to bypass hash
// verification (debugging only).

import { join, basename } from "node:path";
import { existsSync, readdirSync } from "node:fs";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { info, notice } from "./lib/log.mjs";

const TOOLS_DIR = join(repoRoot, "specs", ".tools");
const JARS = [
  {
    name: "tla2tools.jar",
    url: "https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar",
    // Upstream replaced the v1.8.0 release asset on 2026-09-01 (GitHub API
    // `updated_at`); this pin tracks the digest GitHub publishes for the
    // hosted asset (verified via `gh api repos/tlaplus/tlaplus/releases`).
    sha256: "dbcc75552f21978a4846688b8e23be1a6b6c0b3fcee35d78fec2df167958ec94",
  },
  {
    name: "CommunityModules-deps.jar",
    url: "https://github.com/tlaplus/CommunityModules/releases/download/202607311834/CommunityModules-deps.jar",
    sha256: "6703730d475c60741624e4dbcfaa9456477a53c970c7c100b531682d0c1a0f8f",
  },
];
const CLASSPATH = JARS.map((j) => join(TOOLS_DIR, j.name)).join(":");
const SPECS = join(repoRoot, "specs");

const sha256 = (bytes) => new Bun.CryptoHasher("sha256").update(bytes).digest("hex");

async function install() {
  for (const jar of JARS) {
    const dest = join(TOOLS_DIR, jar.name);
    if (existsSync(dest) && sha256(await Bun.file(dest).arrayBuffer()) === jar.sha256) {
      info(`tla: ${jar.name} already present and verified`);
      continue;
    }
    info(`tla: fetching ${jar.url}`);
    const res = await fetch(jar.url);
    if (!res.ok) fail(`tla install: ${jar.url} -> HTTP ${res.status}`);
    const bytes = await res.arrayBuffer();
    if (process.env.TLA_SKIP_VERIFY !== "1") {
      const got = sha256(bytes);
      if (got !== jar.sha256)
        fail(`tla install: SHA-256 mismatch for ${jar.name}: expected ${jar.sha256}, got ${got}`);
    } else notice(`tla install: TLA_SKIP_VERIFY=1 — hash NOT verified for ${jar.name}`);
    await Bun.write(dest, bytes);
    info(`tla: installed ${jar.name} (${bytes.byteLength} bytes)`);
  }
}

function requireTools() {
  for (const jar of JARS)
    if (!existsSync(join(TOOLS_DIR, jar.name)))
      fail(`tla: ${jar.name} missing — run \`just tla-install\` first`);
}

/** Modules to check: the named one, or every specs/*.tla with a sibling .cfg. */
function modules(name) {
  if (name) return [name.replace(/\.tla$/, "")];
  const found = readdirSync(SPECS, { recursive: false })
    .filter((f) => f.endsWith(".tla") && existsSync(join(SPECS, f.replace(/\.tla$/, ".cfg"))))
    .map((f) => f.replace(/\.tla$/, ""));
  if (found.length === 0)
    fail("tla: no TLA+ module with a .cfg exists under specs/ — modules land at v0.1 (ledger rows tla-mc-core, tla-sim); a gate with no inputs is red, never green (s§5.1)");
  return found.sort();
}

const tlc = (args, opts = {}) =>
  run(["java", "-XX:+UseParallelGC", "-cp", CLASSPATH, "tlc2.TLC", ...args], { cwd: SPECS, ...opts });

/** Run TLC capturing output (also streamed) so mc can read the state count. */
async function tlcCaptured(args) {
  const proc = Bun.spawn(["java", "-XX:+UseParallelGC", "-cp", CLASSPATH, "tlc2.TLC", ...args], {
    cwd: SPECS, stdout: "pipe", stderr: "inherit",
  });
  const out = await new Response(proc.stdout).text();
  process.stdout.write(out);
  return { code: await proc.exited, out };
}

async function mc(name) {
  requireTools();
  // Exact reachable-state counts are pinned per clean config (§3.1); drift fails.
  const countsPath = join(SPECS, "state-counts.toml");
  if (!existsSync(countsPath))
    fail("tla mc: specs/state-counts.toml missing — state counts are pinned, no counts means no check (fail-closed; lands at v0.1)");
  const counts = (await import(countsPath, { with: { type: "toml" } })).default;
  for (const mod of modules(name)) {
    info(`tla mc: ${mod}`);
    const { code, out } = await tlcCaptured(["-config", `${mod}.cfg`, "-checkpoint", "0", "-cleanup", "-workers", "auto", `${mod}.tla`]);
    if (code !== 0) process.exit(code);
    // The FINAL BFS summary — "N distinct states found, K states left on
    // queue." — is the count to pin; TLC's interim Progress lines share the
    // "distinct states found" phrase but wrap it as "found (R ds/min), K
    // states left on queue", so the comma directly after "found" is what
    // disambiguates the summary from progress. Take the last such match: on
    // a run long enough to print progress, an unanchored first-match read
    // would latch onto an early Progress count instead.
    const matches = [...out.matchAll(/(\d+) distinct states found, \d+ states left on queue/g)];
    const m = matches.at(-1);
    if (!m) fail(`tla mc: ${mod}: could not read the distinct-state count from TLC output`);
    const pinned = counts[mod];
    if (pinned === undefined) fail(`tla mc: ${mod}: no pinned count in specs/state-counts.toml`);
    if (Number(m[1]) !== pinned)
      fail(`tla mc: ${mod}: reachable-state drift — pinned ${pinned}, got ${m[1]} (§3.1: exact)`);
    info(`tla mc: ${mod}: ${m[1]} distinct states (matches pin)`);
  }
  // specs/broken/: every broken variant must FAIL under TLC — a passing broken
  // model means the property lost its teeth. (Witness/FINDINGS classification
  // per §3.5–3.6 rides in with the v0.1 files; ledger row tla-mc-core.)
  const brokenDir = join(SPECS, "broken");
  if (existsSync(brokenDir)) {
    for (const cfg of readdirSync(brokenDir).filter((f) => f.endsWith(".cfg")).sort()) {
      const mod = cfg.replace(/\.cfg$/, "");
      const code = await tlc(["-config", `broken/${cfg}`, "-checkpoint", "0", "-cleanup", "-workers", "auto", `broken/${mod}.tla`]);
      if (code === 0) fail(`tla mc: broken/${mod} passed — a broken variant must stay red`);
      info(`tla mc: broken/${mod}: red as required`);
    }
  }
}

/** Simulation configs (nightly tier, 8.1): every clean root cfg plus the
 * sim-only scopes under specs/sim/ — configurations whose exhaustive space
 * exceeds the per-PR bounded budget (DrainSnapshot: TN-35, >2.5M states)
 * and therefore must NOT have a root cfg, because a root cfg enters mc's
 * pinned-count discovery. Returns [module, cfg-path] pairs (SPECS-relative). */
function simConfigs(name) {
  const simDir = join(SPECS, "sim");
  const simMods = existsSync(simDir)
    ? readdirSync(simDir).filter((f) => f.endsWith(".cfg")).map((f) => f.replace(/\.cfg$/, ""))
    : [];
  const pick = (mod) => {
    if (existsSync(join(SPECS, `${mod}.cfg`))) return [mod, `${mod}.cfg`];
    if (simMods.includes(mod)) {
      if (!existsSync(join(SPECS, `${mod}.tla`)))
        fail(`tla sim: sim/${mod}.cfg exists but specs/${mod}.tla does not (fail-closed)`);
      return [mod, `sim/${mod}.cfg`];
    }
    return fail(`tla sim: no ${mod}.cfg or sim/${mod}.cfg under specs/`);
  };
  if (name) return [pick(name.replace(/\.tla$/, ""))];
  return [...new Set([...modules(), ...simMods])].sort().map(pick);
}

/** argv after `sim`: [Module] [--depth N] [--runs N] [--seed N]. Defaults
 * keep a bare `just tla-sim` terminating (bare TLC -simulate never stops);
 * nightly.yml passes the real budget and the run id as the seed. */
function simArgs(argv) {
  const opts = { name: undefined, depth: 100, runs: 100, seed: undefined };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--depth" || a === "--runs" || a === "--seed") {
      const v = Number(argv[i + 1] ?? "");
      i += 1;
      if (!Number.isSafeInteger(v) || v < (a === "--seed" ? 0 : 1))
        fail(`tla sim: ${a} needs a ${a === "--seed" ? "non-negative" : "positive"} integer (got '${argv[i] ?? ""}')`);
      opts[a.slice(2)] = v;
    } else if (a.startsWith("-")) {
      fail(`tla sim: unknown flag '${a}' (usage: sim [Module] [--depth N] [--runs N] [--seed N])`);
    } else if (opts.name !== undefined) {
      fail(`tla sim: at most one module (got '${opts.name}' and '${a}')`);
    } else opts.name = a;
  }
  return opts;
}

async function sim(argv) {
  requireTools();
  const { name, depth, runs, seed } = simArgs(argv);
  for (const [mod, cfg] of simConfigs(name)) {
    // TLC's num= is PER WORKER (`-workers auto`); on failure TLC prints the
    // full counterexample trace plus "Simulation using seed N and aril M" —
    // the reproduction citation (nightly.yml seeds with the run id).
    info(`tla sim: ${mod} (${cfg}; depth=${depth}, runs=${runs}/worker${seed === undefined ? "" : `, seed=${seed}`})`);
    const args = ["-config", cfg, "-simulate", `num=${runs}`, "-depth", String(depth)];
    if (seed !== undefined) args.push("-seed", String(seed));
    const code = await tlc([...args, "-checkpoint", "0", "-cleanup", "-workers", "auto", `${mod}.tla`]);
    if (code !== 0) process.exit(code);
  }
}

async function tv(trace) {
  requireTools();
  if (!trace) fail("tla tv: usage: tla.mjs tv <trace.ndjson>");
  if (!existsSync(join(repoRoot, trace)) && !existsSync(trace)) fail(`tla tv: trace not found: ${trace}`);
  const tracesDir = join(SPECS, "traces");
  const specs = existsSync(tracesDir) ? readdirSync(tracesDir).filter((f) => f.endsWith("Trace.tla")) : [];
  if (specs.length === 0)
    fail("tla tv: no specs/traces/*Trace.tla exists — trace specs land at v0.1 (ledger row conformance)");
  // The trace file names its module by prefix: <module>-*.ndjson ↔ <Module>Trace.tla.
  const stem = basename(trace).toLowerCase();
  const match = specs.filter((s) => stem.startsWith(s.replace(/Trace\.tla$/, "").toLowerCase()));
  if (match.length !== 1)
    fail(`tla tv: cannot pair ${basename(trace)} with exactly one specs/traces/*Trace.tla (candidates: ${match.join(", ") || "none"})`);
  const spec = match[0].replace(/\.tla$/, "");
  info(`tla tv: ${trace} against ${spec} (-workers 1 per trace, s§5.3)`);
  const abs = existsSync(trace) ? trace : join(repoRoot, trace);
  const code = await tlc(["-config", `traces/${spec}.cfg`, "-workers", "1", `traces/${spec}.tla`], {
    env: { ...process.env, TRACE_PATH: abs },
  });
  process.exit(code);
}

const [cmd, arg] = process.argv.slice(2);
if (cmd === "install") await install();
else if (cmd === "mc") await mc(arg);
else if (cmd === "sim") await sim(process.argv.slice(3));
else if (cmd === "tv") await tv(arg);
else fail(`tla.mjs: unknown subcommand '${cmd ?? ""}' (expected: install | mc | sim | tv)`);
