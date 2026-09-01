// pr-type-label.mjs — the single source of truth for the PR-title →
// type-label mapping and the title-format rule, shared by:
//
//   - scripts/pr-title-check.mjs   (the `pr-title` gate in ci.yml)
//   - scripts/pr-label.mjs         (the labeler, event + backfill paths)
//
// One pure module means the gate and the labeler can never drift
// (pattern learned from cerberus/castle-reboot; adapted to this repo's
// vocabulary). Dependency-free by design — plain ESM, no Bun-only APIs,
// importable from any runtime.
//
// THE TITLE RULE (CONTRIBUTING.md "PR titles"): every PR title MUST carry
// a Conventional-Commit header `type(scope)!?: subject` with a known type —
// titles are squash-merge subjects and release-plz input, so an unparsable
// title is a red, required check. The scope is OPTIONAL; when present it
// must be a recognized area and must not merely restate the type.
//
// Mapping (type → label):
//   feat → enhancement   fix → bug          docs → documentation
//   ci → ci              test → test        refactor → refactor
//   perf → performance   chore → chore      build → build
//   revert → revert      style → (none; cosmetic, no label)
// Scope overrides (checked before the bare-type table):
//   *(deps)        → dependencies
//   chore(release) → release (+ chore)
//
// argv `--self-test` runs the assertion suite and exits.

export const TYPE_TO_LABEL = Object.freeze({
  feat: "enhancement",
  fix: "bug",
  docs: "documentation",
  ci: "ci",
  test: "test",
  refactor: "refactor",
  perf: "performance",
  chore: "chore",
  build: "build",
  revert: "revert",
  style: null, // recognized type, deliberately unlabeled
});

// The scope vocabulary: crate short names (crates/duckspout-<scope>) plus
// the non-crate areas. Extending it is a one-line PR to this array — the
// self-test pins the crate half against the workspace via the invariants
// engine's edge-audit-domain rule (crate adds already require an
// invariants.toml touch, which is the same protected PR).
export const SCOPES = Object.freeze([
  // crates/duckspout-*
  "types", "accept", "staging", "replication", "drain", "watermark",
  "lake-contract", "lake-ducklake", "ctk", "daemon", "ctl", "fleet",
  "judge", "loadgen",
  // non-crate areas
  "spike", "specs", "scripts", "deploy", "ci", "deps", "release",
]);

const HEADER = /^([a-z]+)(?:\(([^)]*)\))?!?:\s+\S/i;

// validateTitle → { ok, reason } where reason ∈
// { null, "no-header", "unknown-type", "unknown-scope", "redundant-scope" }
export function validateTitle(title) {
  const m = String(title ?? "").match(HEADER);
  if (!m) return { ok: false, reason: "no-header" };
  const type = m[1].toLowerCase();
  const scope = (m[2] || "").toLowerCase();
  if (!(type in TYPE_TO_LABEL)) return { ok: false, reason: "unknown-type" };
  if (scope) {
    if (scope === type) return { ok: false, reason: "redundant-scope" };
    if (!SCOPES.includes(scope)) return { ok: false, reason: "unknown-scope" };
  }
  return { ok: true, reason: null };
}

// labelsForTitle → the labels a PR with this title should carry ([] for a
// style: change or an invalid title — the GATE owns rejection, the labeler
// stays silent on invalid input).
export function labelsForTitle(title) {
  const v = validateTitle(title);
  if (!v.ok) return [];
  const m = String(title).match(HEADER);
  const type = m[1].toLowerCase();
  const scope = (m[2] || "").toLowerCase();
  if (scope === "deps") return ["dependencies"];
  if (type === "chore" && scope === "release") return ["release", "chore"];
  const label = TYPE_TO_LABEL[type];
  return label ? [label] : [];
}

function selfTest() {
  const eq = (got, want, why) => {
    const g = JSON.stringify(got);
    const w = JSON.stringify(want);
    if (g !== w) throw new Error(`self-test: ${why} — got ${g}, want ${w}`);
  };
  // The rule accepts this repo's entire merge history:
  eq(validateTitle("feat: bootstrap the DuckSpout workspace (seed step 1)").ok, true, "bare feat passes");
  eq(validateTitle("docs: absorb §1 (What and Why) into README.md").ok, true, "bare docs passes");
  eq(validateTitle("refactor!: ACPR is session-level judgment, not a CI gate").ok, true, "breaking marker passes");
  eq(validateTitle("fix(ci): install rustfmt+clippy components").ok, true, "recognized scope passes");
  eq(validateTitle("chore(spike): DuckDB embed + commit-latency ballpark").ok, true, "spike scope passes");
  eq(validateTitle("build(deps): Bump actions/checkout from 5.1.0 to 7.0.1").ok, true, "deps scope passes");
  // Rejections:
  eq(validateTitle("Update README").reason, "no-header", "headerless title fails");
  eq(validateTitle("wibble: do a thing").reason, "unknown-type", "unknown type fails");
  eq(validateTitle("feat(wibble): thing").reason, "unknown-scope", "unknown scope fails");
  eq(validateTitle("ci(ci): pin a jar").reason, "redundant-scope", "scope restating type fails");
  eq(validateTitle("feat:no-space").reason, "no-header", "missing space after colon fails");
  // Labels:
  eq(labelsForTitle("feat(accept): OTLP adapter"), ["enhancement"], "feat → enhancement");
  eq(labelsForTitle("build(deps): bump x"), ["dependencies"], "deps override");
  eq(labelsForTitle("chore(release): v0.1.0"), ["release", "chore"], "release override");
  eq(labelsForTitle("style: whitespace"), [], "style carries no label");
  eq(labelsForTitle("nonsense title"), [], "invalid title labels nothing (gate owns rejection)");
  console.log("pr-type-label --self-test: all assertions passed");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  if (process.argv.includes("--self-test")) selfTest();
  else {
    console.error("library module; run with --self-test or import it");
    process.exit(1);
  }
}
