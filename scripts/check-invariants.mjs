#!/usr/bin/env bun
// The invariant engine (SEED s§7, D-7, ADR-0007). Executes the rules declared
// in invariants.toml — rules are data; this engine implements exactly five
// rule kinds (forbidden-edge, banned-file, banned-source, golden-manifest,
// pairing) and hardcodes no specific edge, glob, or pattern.
//
// Fail-closed: a file a rule needs that does not exist is a violation, never
// a skip. The one sanctioned skip is the ledger issue-number sub-check, which
// needs the GitHub API and is CI-only by declared exception (s§6.5) — it is
// reported loudly as SKIPPED-CI-ONLY.
//
// Output: one line per violation, a summary count, exit 1 on any violation.
// Exit code 78 (STAGED) is never used here.

import { join, isAbsolute, sep } from "node:path";
import { readdirSync } from "node:fs";
import { repoRoot } from "./lib/sh.mjs";
import { error, info, notice } from "./lib/log.mjs";

const violations = [];
const violation = (kind, msg) => violations.push(`VIOLATION [${kind}] ${msg}`);

// ---------- helpers ----------

async function readText(rel) {
  const f = Bun.file(join(repoRoot, rel));
  return (await f.exists()) ? await f.text() : null;
}

/** Read a file a rule depends on; absence is itself a violation (fail-closed). */
async function requireText(kind, rel) {
  const text = await readText(rel);
  if (text === null) violation(kind, `required file missing: ${rel}`);
  return text;
}

async function requireToml(kind, rel) {
  if (!(await Bun.file(join(repoRoot, rel)).exists())) {
    violation(kind, `required file missing: ${rel}`);
    return null;
  }
  try {
    return (await import(join(repoRoot, rel), { with: { type: "toml" } })).default;
  } catch (e) {
    violation(kind, `cannot parse ${rel}: ${e.message}`);
    return null;
  }
}

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// Directories never scanned by glob rules: .git plus the top-level directory
// entries of .gitignore (build/tool litter). Everything else — spike/
// included — is in scope.
async function ignoredPrefixes() {
  const prefixes = [".git/"];
  const gi = await readText(".gitignore");
  if (gi !== null) {
    for (const line of gi.split("\n")) {
      const t = line.trim();
      if (t.endsWith("/") && !t.startsWith("#") && !t.includes("*")) prefixes.push(t.replace(/^\//, ""));
    }
  }
  return prefixes;
}

async function globRepo(pattern) {
  const skip = await ignoredPrefixes();
  const out = [];
  for await (const p of new Bun.Glob(pattern).scan({ cwd: repoRoot, dot: true })) {
    if (!skip.some((pre) => p === pre.slice(0, -1) || p.startsWith(pre) || p.includes(`/${pre}`))) out.push(p);
  }
  return out.sort();
}

/** All markdown table data rows as {header: string[], cells: string[]}, formatting stripped. */
function mdTableRows(md) {
  const rows = [];
  const lines = md.split("\n");
  let header = null;
  const cellsOf = (line) =>
    line.replace(/^\s*\|/, "").replace(/\|\s*$/, "").split("|")
      .map((c) => c.replace(/[`*]/g, "").trim());
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!/^\s*\|/.test(line)) { header = null; continue; }
    if (/^\s*\|[\s:|-]+\|?\s*$/.test(line)) continue; // separator row
    if (header === null && /^\s*\|[\s:|-]+\|?\s*$/.test(lines[i + 1] ?? "")) {
      header = cellsOf(line);
      continue;
    }
    rows.push({ header: header ?? [], cells: cellsOf(line) });
  }
  return rows;
}

// ---------- rule kinds ----------

async function checkForbiddenEdges(rules) {
  const proc = Bun.spawnSync(["cargo", "metadata", "--format-version", "1"], { cwd: repoRoot });
  if (proc.exitCode !== 0) {
    violation("forbidden-edge", `cargo metadata failed (fail-closed): ${proc.stderr.toString().trim().split("\n")[0]}`);
    return;
  }
  const meta = JSON.parse(proc.stdout.toString());
  const members = new Set(meta.workspace_members);
  const cratesDir = join(repoRoot, "crates") + sep;
  for (const pkg of meta.packages) {
    if (!members.has(pkg.id)) continue;
    const direct = pkg.dependencies ?? [];
    for (const rule of rules) {
      if (rule.from !== pkg.name) continue;
      if (direct.some((d) => d.name === rule.to || d.rename === rule.to))
        violation("forbidden-edge", `${rule.from} -> ${rule.to} (${rule.reason})`);
    }
    // Any path dependency reaching outside crates/ is reviewable by definition.
    for (const d of direct) {
      if (d.path !== undefined && d.path !== null && !(d.path + sep).startsWith(cratesDir))
        violation("forbidden-edge", `${pkg.name} -> ${d.name}: path dependency outside crates/ (${d.path})`);
    }
  }
}

async function checkBannedFiles(rules) {
  for (const rule of rules) {
    for (const file of await globRepo(rule.glob))
      violation("banned-file", `${file} matches banned glob ${rule.glob} (${rule.reason})`);
  }
}

async function checkBannedSource(rules) {
  for (const rule of rules) {
    const scopes = Array.isArray(rule.scope) ? rule.scope : [rule.scope];
    for (const scope of scopes) {
      for (const file of await globRepo(scope)) {
        const lines = (await readText(file)).split("\n");
        for (const pattern of rule.patterns) {
          lines.forEach((l, i) => {
            if (l.includes(pattern))
              violation("banned-source", `${file}:${i + 1} contains "${pattern}" (${rule.reason})`);
          });
        }
      }
    }
  }
}

const normalize = (t) => t.split("\n").map((l) => l.replace(/\s+$/, "")).join("\n").replace(/\n+$/, "");

async function checkGoldenManifests(rules) {
  for (const rule of rules) {
    const golden = await requireText("golden-manifest", rule.golden);
    if (golden === null) continue;
    // No quoting in generate commands (a declared constraint of the rule
    // format), so a whitespace split is the whole argv parse — no shell.
    const proc = Bun.spawnSync(rule.generate.split(/\s+/), { cwd: repoRoot });
    if (proc.exitCode !== 0) {
      violation("golden-manifest", `generate command failed (fail-closed): ${rule.generate}`);
      continue;
    }
    const got = normalize(proc.stdout.toString());
    const want = normalize(golden);
    if (got !== want) {
      const gl = got.split("\n"), wl = want.split("\n");
      let i = 0;
      while (i < Math.max(gl.length, wl.length) && gl[i] === wl[i]) i++;
      violation(
        "golden-manifest",
        `${rule.golden} differs from \`${rule.generate}\` at line ${i + 1}: ` +
          `golden ${JSON.stringify(wl[i] ?? "<eof>")} vs generated ${JSON.stringify(gl[i] ?? "<eof>")} (${rule.reason})`,
      );
    }
  }
}

// ---------- pairing kinds ----------

async function pairLedgerIntegrity() {
  const kind = "pairing:ledger-integrity";
  const ledger = await requireToml(kind, "docs/arming-ledger.toml");
  const justfile = await requireText(kind, "Justfile");
  if (ledger === null || justfile === null) return;
  const ids = new Set();
  for (const gate of ledger.gate ?? []) {
    const id = gate.id ?? "<missing id>";
    if (ids.has(id)) violation(kind, `gate '${id}': duplicate id`);
    ids.add(id);
    if (!["armed", "staged"].includes(gate.status)) violation(kind, `gate '${id}': bad status '${gate.status}'`);
    if (!["pr", "nightly"].includes(gate.cadence)) violation(kind, `gate '${id}': bad cadence '${gate.cadence}'`);
    if (!gate.recipe || !new RegExp(`^${esc(gate.recipe)}(\\s|:)`, "m").test(justfile))
      violation(kind, `gate '${id}': recipe '${gate.recipe}' not found in Justfile`);
    if (gate.status === "armed") {
      const m = /^([^:]+\.yml):(.+)$/.exec(gate.workflow_job ?? "");
      if (!m) {
        violation(kind, `gate '${id}': armed but workflow_job '${gate.workflow_job}' is not "<file>.yml:<job>"`);
        continue;
      }
      const wf = await requireText(kind, `.github/workflows/${m[1]}`);
      if (wf !== null && !new RegExp(`^\\s+${esc(m[2])}:`, "m").test(wf))
        violation(kind, `gate '${id}': job '${m[2]}' not found in .github/workflows/${m[1]}`);
    } else {
      if (!gate.milestone) violation(kind, `gate '${id}': staged but milestone is empty`);
      if (ledger.bootstrap !== true && !(gate.issue > 0)) {
        if (process.env.GITHUB_ACTIONS)
          violation(kind, `gate '${id}': staged with issue = ${gate.issue} (must be > 0 once bootstrap = false)`);
        else
          notice(`SKIPPED-CI-ONLY [${kind}] gate '${id}': issue > 0 sub-check needs the GitHub API; it runs in CI only (s§6.5, the one sanctioned skip)`);
      }
    }
  }
}

async function pairToolPins() {
  const kind = "pairing:tool-pins";
  const toolchain = await requireToml(kind, "rust-toolchain.toml");
  const channel = toolchain?.toolchain?.channel;
  if (toolchain !== null && !channel) violation(kind, "rust-toolchain.toml has no toolchain.channel");
  const setup = await requireText(kind, ".github/actions/setup/action.yml");
  // MSRV mirrors: root Cargo.toml rust-version (authoritative) == clippy.toml msrv == the `cargo +N` in the Justfile msrv recipe.
  const cargo = await requireToml(kind, "Cargo.toml");
  const clippy = await requireToml(kind, "clippy.toml");
  const justfile = await requireText(kind, "Justfile");
  const msrv = cargo?.workspace?.package?.["rust-version"];
  if (cargo !== null && !msrv) violation(kind, "Cargo.toml has no workspace.package.rust-version");
  if (clippy !== null && msrv && clippy.msrv !== msrv)
    violation(kind, `clippy.toml msrv '${clippy.msrv}' != Cargo.toml rust-version '${msrv}'`);
  const jm = justfile === null ? null : /cargo \+([0-9.]+) check/.exec(justfile);
  if (justfile !== null && !jm) violation(kind, "Justfile msrv recipe (`cargo +<version> check`) not found");
  if (jm && msrv && jm[1] !== msrv)
    violation(kind, `Justfile msrv toolchain '+${jm[1]}' != Cargo.toml rust-version '${msrv}'`);
  // Rust-version mirrors inside .github/: the setup action installs from
  // rust-toolchain.toml (the authoritative copy — it need not restate the
  // channel), but any LITERAL toolchain/msrv-toolchain value written in a
  // workflow or action must match its authoritative copy.
  const yamls = [[".github/actions/setup/action.yml", setup]];
  for (const f of await globRepo(".github/workflows/*.yml")) yamls.push([f, await readText(f)]);
  for (const [file, text] of yamls) {
    if (text === null) continue;
    for (const m of text.matchAll(/^[ \t]*toolchain:[ \t]*["']?([^\s"'#]+)/gm)) {
      if (m[1].includes("${{")) continue; // an expression, not a literal mirror
      if (channel && msrv && m[1] !== channel && m[1] !== msrv)
        violation(kind, `${file}: literal toolchain '${m[1]}' matches neither the pinned channel '${channel}' nor the MSRV '${msrv}'`);
    }
    for (const m of text.matchAll(/^[ \t]*msrv-toolchain:[ \t]*["']?([^\s"'#]+)/gm)) {
      if (m[1].includes("${{")) continue;
      if (msrv && m[1] !== msrv)
        violation(kind, `${file}: literal msrv-toolchain '${m[1]}' != Cargo.toml rust-version '${msrv}'`);
    }
  }
}

async function pairTraceMapping() {
  const kind = "pairing:trace-mapping";
  const md = await requireText(kind, "docs/trace-mapping.md");
  // The trace enum is delimited in source by `// trace-enum-begin` / `// trace-enum-end`.
  let enumBody = null, enumFile = null;
  for (const file of await globRepo("crates/duckspout-types/src/**/*.rs")) {
    const text = await readText(file);
    const m = /\/\/ trace-enum-begin\n([\s\S]*?)\/\/ trace-enum-end/.exec(text);
    if (m) { enumBody = m[1]; enumFile = file; break; }
  }
  if (enumBody === null)
    violation(kind, "no `// trace-enum-begin` … `// trace-enum-end` block found under crates/duckspout-types/src");
  if (md === null || enumBody === null) return;
  const sourceVariants = new Set();
  for (const line of enumBody.split("\n")) {
    const m = /^\s*([A-Z][A-Za-z0-9]*)\s*(?:[,({]|$)/.exec(line);
    if (m) sourceVariants.add(m[1]);
  }
  const mdVariants = new Set();
  for (const row of mdTableRows(md)) {
    const cell = row.cells[0] ?? "";
    if (/^[A-Z][A-Za-z0-9]*$/.test(cell)) mdVariants.add(cell);
  }
  if (mdVariants.size === 0) violation(kind, "docs/trace-mapping.md carries no variant table rows");
  if (sourceVariants.size === 0) violation(kind, `trace-enum block in ${enumFile} carries no variants`);
  for (const v of sourceVariants)
    if (!mdVariants.has(v)) violation(kind, `enum variant ${v} (${enumFile}) has no docs/trace-mapping.md row`);
  for (const v of mdVariants)
    if (!sourceVariants.has(v)) violation(kind, `docs/trace-mapping.md row ${v} has no enum variant in ${enumFile}`);
}

async function pairConstitutionMechanism() {
  const kind = "pairing:constitution-mechanism";
  const md = await requireText(kind, "CONSTITUTION.md");
  if (md === null) return;
  let ruleRows = 0;
  for (const row of mdTableRows(md)) {
    const id = /^(R-[a-z0-9-]+)/.exec(row.cells[0] ?? "")?.[1];
    if (!id) continue;
    ruleRows++;
    const col = row.header.findIndex((h) => /mechanism/i.test(h));
    if (col === -1) { violation(kind, `${id}: its table has no Mechanism column`); continue; }
    const cell = (row.cells[col] ?? "").replace(/[—–-]/g, "").trim();
    if (cell === "") violation(kind, `${id}: empty Mechanism cell (every rule needs an enforcing mechanism, s§8.2)`);
  }
  if (ruleRows === 0) violation(kind, "CONSTITUTION.md carries no R-* rule table rows (fail-closed)");
}

async function pairEdgeAuditDomain(rules) {
  const kind = "pairing:edge-audit-domain";
  const invText = await requireText(kind, "invariants.toml");
  let dirs;
  try {
    dirs = readdirSync(join(repoRoot, "crates"), { withFileTypes: true }).filter((d) => d.isDirectory());
  } catch {
    violation(kind, "crates/ directory missing (fail-closed)");
    return;
  }
  if (invText === null) return;
  for (const d of dirs)
    if (!invText.includes(d.name))
      violation(kind, `workspace crate ${d.name} appears nowhere in invariants.toml — it joined the workspace unaudited`);
}

const INHERITED_FIELDS = ["edition", "rust-version", "license", "repository", "version"];

async function pairWorkspaceInheritance() {
  const kind = "pairing:workspace-inheritance";
  const manifests = await globRepo("crates/*/Cargo.toml");
  if (manifests.length === 0) violation(kind, "no crates/*/Cargo.toml found (fail-closed)");
  for (const file of manifests) {
    const text = await readText(file);
    for (const field of INHERITED_FIELDS) {
      const inherits = new RegExp(`^${esc(field)}\\.workspace\\s*=\\s*true|^${esc(field)}\\s*=\\s*\\{\\s*workspace\\s*=\\s*true`, "m");
      if (!inherits.test(text)) violation(kind, `${file}: ${field} does not inherit from [workspace.package] (${field}.workspace = true)`);
      if (new RegExp(`^${esc(field)}\\s*=\\s*"`, "m").test(text))
        violation(kind, `${file}: ${field} is set locally; members inherit every [workspace.package] field (s§3.1)`);
    }
  }
}

const PAIRING_KINDS = {
  "ledger-integrity": pairLedgerIntegrity,
  "tool-pins": pairToolPins,
  "trace-mapping": pairTraceMapping,
  "constitution-mechanism": pairConstitutionMechanism,
  "edge-audit-domain": pairEdgeAuditDomain,
  "workspace-inheritance": pairWorkspaceInheritance,
};

// ---------- main ----------

const rules = await requireToml("engine", "invariants.toml");
if (rules === null) {
  error("FAIL: invariants: invariants.toml missing or unparseable — nothing to enforce (fail-closed)");
  process.exit(1);
}

await checkForbiddenEdges(rules["forbidden-edge"] ?? []);
await checkBannedFiles(rules["banned-file"] ?? []);
await checkBannedSource(rules["banned-source"] ?? []);
await checkGoldenManifests(rules["golden-manifest"] ?? []);
for (const rule of rules.pairing ?? []) {
  const impl = PAIRING_KINDS[rule.kind];
  if (impl === undefined) violation("pairing", `unknown pairing kind '${rule.kind}' (fail-closed: the engine cannot enforce it)`);
  else await impl(rules["forbidden-edge"] ?? []);
}

for (const v of violations) error(v);
if (violations.length > 0) {
  error(`FAIL: invariants: ${violations.length} violation(s)`);
  process.exit(1);
}
info(`invariants: all rules hold (${(rules["forbidden-edge"] ?? []).length} edges, ` +
  `${(rules["banned-file"] ?? []).length + (rules["banned-source"] ?? []).length} ban rules, ` +
  `${(rules["golden-manifest"] ?? []).length} golden manifests, ${(rules.pairing ?? []).length} pairing rules)`);
