#!/usr/bin/env bun
// trace-conformance.mjs — the `conformance` ledger row's script (§8.2;
// armed at v0.1 by issue #44). Three tiers, fail-closed:
//
//   1. FIXTURES (the teeth): every specs/fixtures/*-manifest.toml row runs —
//      the conforming traces must pass refinement, and every doctored trace
//      must fail through EXACTLY its named mechanism (decoder rejection,
//      refinement halt at an asserted cursor, or a named TraceComplete
//      invariant violation). One tooth going blunt cannot hide behind
//      another (§8.2).
//   2. LIVE: a FRESH trace is captured on this very run from the real
//      accept → staging → drain composition (duckspout-daemon's
//      tests/trace_capture.rs) and validated against the refinement spec —
//      a static fixture certifies capture day; the fresh trace certifies
//      the code under test.
//   3. REAL BACKENDS (MinIO + Postgres, issue #44): a FRESH trace is
//      captured against real MinIO + Postgres
//      (duckspout-daemon's tests/trace_capture_real_backends.rs, the same
//      choreography as tier 2's composition — tests/common/capture.rs —
//      over a real S3-compatible object store and a real Postgres
//      DuckLake catalog instead of local doubles) and validated against
//      the refinement spec, THEN doctored the same ways fixtures are
//      (§8.2: "static fixtures would go stale the moment an adapter
//      changed", so this tier doctors the trace it just generated rather
//      than comparing it to a committed fixture) — each doctored variant
//      must fail through exactly its named mechanism, same one-tooth
//      discipline as tier 1. Endpoint env vars absent: fails in CI
//      (`CI=true`, fail-closed per §8.2 — a gate that cannot run fails, it
//      does not shrug), skips with a notice for a contributor without
//      Docker running this outside CI.
//
// Mechanism assertions read TLC's own output: a refinement halt prints
// <<"TraceHalt", N>> from the failed POSTCONDITION (N = the 1-based NDJSON
// line nothing could explain); an invariant rejection prints
// "Invariant <Name> is violated". The decoder tier is this script's own
// structural validation (JSON shape, §3.3 vocabulary from
// docs/trace-mapping.md, dense per-node seqs — D-6).

import { join, basename } from "node:path";
import { existsSync, readdirSync, mkdtempSync, copyFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { repoRoot, run } from "./lib/sh.mjs";
import { STAGED, fail } from "./lib/proc.mjs";
import { group, info, notice } from "./lib/log.mjs";

const SPECS = join(repoRoot, "specs");
const FIXTURES = join(SPECS, "fixtures");
const TRACES = join(SPECS, "traces");

// ---------------------------------------------------------------------------
// Decoder: structural validation of one NDJSON trace (D-6). Returns null on
// success, or the rejection reason. The journaled-event vocabulary is parsed
// from docs/trace-mapping.md (its enum pairing is validated by the invariant
// engine, so the doc is a checked source, not a second copy of the enum);
// everything above the "## Environment events" section journals, the
// environment events never do.
// ---------------------------------------------------------------------------
async function journaledVocabulary() {
  const doc = await Bun.file(join(repoRoot, "docs", "trace-mapping.md")).text();
  const journaled = doc.split("## Environment events")[0];
  const names = [...journaled.matchAll(/^\| `([A-Za-z]+)` \|/gm)].map((m) => m[1]);
  if (names.length === 0) fail("trace-conformance: no vocabulary rows in docs/trace-mapping.md");
  return new Set(names);
}

function decode(text, vocabulary) {
  const nextSeq = new Map();
  const lines = text.split("\n").filter((l) => l.length > 0);
  if (lines.length === 0) return "empty trace";
  for (const [index, line] of lines.entries()) {
    let record;
    try {
      record = JSON.parse(line);
    } catch (e) {
      return `line ${index + 1}: not one JSON object per line: ${e.message}`;
    }
    if (typeof record.node !== "string" || record.node.length === 0)
      return `line ${index + 1}: missing/invalid node`;
    if (typeof record.event !== "string" || !vocabulary.has(record.event))
      return `line ${index + 1}: event ${JSON.stringify(record.event)} outside the §3.3 journaled vocabulary`;
    if (!Number.isInteger(record.seq) || record.seq < 0)
      return `line ${index + 1}: missing/invalid seq`;
    const expected = nextSeq.get(record.node) ?? 0;
    if (record.seq !== expected)
      return `line ${index + 1}: node ${record.node} seq ${record.seq}, expected ${expected} (D-6: dense per-node seqs)`;
    nextSeq.set(record.node, expected + 1);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Refinement: run one trace through `tla.mjs tv` (which pairs it with its
// specs/traces/*Trace.tla sibling), capturing output for the mechanism
// assertions.
// ---------------------------------------------------------------------------
async function refine(tracePath) {
  const proc = Bun.spawn(["bun", join(repoRoot, "scripts", "tla.mjs"), "tv", tracePath], {
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
  });
  const out =
    (await new Response(proc.stdout).text()) + (await new Response(proc.stderr).text());
  const code = await proc.exited;
  return { code, out };
}

const haltCursor = (out) => {
  const m = [...out.matchAll(/<<"TraceHalt", (\d+)>>/g)].at(-1);
  return m ? Number(m[1]) : null;
};

function assertExpectation(name, expectation, decodeError, tlc) {
  const expect = expectation.expect;
  if (expect === "decoder") {
    if (decodeError === null)
      fail(`trace-conformance: ${name}: decoder ACCEPTED a fixture it must reject — the decoder tooth went blunt`);
    info(`  ${name}: rejected by the decoder (${decodeError}) — as required`);
    return;
  }
  if (decodeError !== null)
    fail(`trace-conformance: ${name}: decoder rejected (${decodeError}) but the expected mechanism is '${expect}' — a tooth is hiding behind another`);
  if (expect === "conforms") {
    if (tlc.code !== 0)
      fail(`trace-conformance: ${name}: a conforming trace failed refinement (exit ${tlc.code})`);
    info(`  ${name}: conforms`);
    return;
  }
  if (tlc.code === 0)
    fail(`trace-conformance: ${name}: TLC ACCEPTED a doctored trace — the '${expect}' tooth went blunt`);
  if (expect === "halt") {
    const at = haltCursor(tlc.out);
    if (at !== expectation.halt_at)
      fail(`trace-conformance: ${name}: expected refinement halt at entry ${expectation.halt_at}, TLC halted at ${at ?? "«no TraceHalt printed»"} — wrong mechanism or wrong entry`);
    if (tlc.out.includes("is violated"))
      fail(`trace-conformance: ${name}: an invariant fired before the expected refinement halt — wrong mechanism`);
    info(`  ${name}: refinement halt at entry ${at} — as required`);
    return;
  }
  if (expect === "invariant") {
    const wanted = `Invariant ${expectation.invariant} is violated`;
    if (!tlc.out.includes(wanted))
      fail(`trace-conformance: ${name}: expected "${wanted}", not found in TLC output — wrong mechanism`);
    info(`  ${name}: ${expectation.invariant} violated — as required`);
    return;
  }
  fail(`trace-conformance: ${name}: unknown expectation '${expect}' in the fixture manifest`);
}

// ---------------------------------------------------------------------------
// Tiers
// ---------------------------------------------------------------------------
async function fixturesTier(vocabulary) {
  const manifests = readdirSync(FIXTURES).filter((f) => f.endsWith("-manifest.toml")).sort();
  if (manifests.length === 0) fail("trace-conformance: specs/fixtures/ holds no *-manifest.toml");
  for (const manifestFile of manifests) {
    const manifest = (
      await import(join(FIXTURES, manifestFile), { with: { type: "toml" } })
    ).default;
    const rows = manifest.fixture ?? [];
    if (rows.length === 0) fail(`trace-conformance: ${manifestFile} lists no fixtures`);
    if (!rows.some((r) => r.expect === "conforms") || rows.filter((r) => r.expect !== "conforms").length < 4)
      fail(`trace-conformance: ${manifestFile}: §8.2 demands >= 1 conforming and >= 4 doctored fixtures`);
    for (const row of rows) {
      const path = join(FIXTURES, row.file);
      if (!existsSync(path)) fail(`trace-conformance: fixture ${row.file} named by ${manifestFile} is missing — a vanished subject fails, it does not shrug (§8.6)`);
      const decodeError = decode(await Bun.file(path).text(), vocabulary);
      const tlc = row.expect === "decoder" ? { code: -1, out: "" } : await refine(path);
      assertExpectation(row.file, row, decodeError, tlc);
    }
  }
}

async function liveTier(vocabulary) {
  const scratch = mkdtempSync(join(tmpdir(), "duckspout-trace-"));
  const fresh = join(scratch, "ingest-live-capture.ndjson");
  info("  capturing a fresh trace from the real composition (duckspout-daemon tests/trace_capture.rs)");
  const code = await run(
    ["cargo", "nextest", "run", "-p", "duckspout-daemon", "--test", "trace_capture", "--retries", "0"],
    { env: { ...process.env, DUCKSPOUT_TRACE_CAPTURE_OUT: fresh } },
  );
  if (code !== 0) fail(`trace-conformance: live capture failed (cargo nextest exit ${code}) — no fresh trace, no green (fail-closed)`);
  if (!existsSync(fresh)) fail("trace-conformance: the capture test passed but wrote no trace — fail-closed");
  const decodeError = decode(await Bun.file(fresh).text(), vocabulary);
  if (decodeError !== null) fail(`trace-conformance: fresh capture failed decoding: ${decodeError}`);
  const tlc = await refine(fresh);
  if (tlc.code !== 0) fail(`trace-conformance: the FRESH capture failed refinement (exit ${tlc.code}) — the implementation drifted from the model`);
  info("  fresh capture conforms");
}

// ---------------------------------------------------------------------------
// Tier 3: real backends (MinIO + Postgres, issue #44). Connection facts come
// in one env var per fact (never a pre-assembled DSN/URL) — the same
// convention `.github/workflows/ci.yml`'s `conformance` job and
// `trace_capture_real_backends.rs` share.
// ---------------------------------------------------------------------------
const REAL_BACKEND_ENV = [
  "DUCKSPOUT_CONFORMANCE_S3_ENDPOINT",
  "DUCKSPOUT_CONFORMANCE_S3_BUCKET",
  "DUCKSPOUT_CONFORMANCE_S3_ACCESS_KEY_ID",
  "DUCKSPOUT_CONFORMANCE_S3_SECRET_ACCESS_KEY",
  "DUCKSPOUT_CONFORMANCE_POSTGRES_DSN",
];

/** Renumber (node, seq) densely per node, in the given order (D-6) — every
 * doctoring below removes one or two entries and must re-densify the rest. */
function renumber(records) {
  const next = new Map();
  return records.map((r) => {
    const seq = next.get(r.node) ?? 0;
    next.set(r.node, seq + 1);
    return { node: r.node, seq, event: r.event };
  });
}

const toNdjson = (records) => `${records.map((r) => JSON.stringify(r)).join("\n")}\n`;
const parseTrace = (text) => text.split("\n").filter((l) => l.length > 0).map((l) => JSON.parse(l));

/** The §8.2 doctoring table, minus the Receipt row: v0.1 is RF = 1 (no
 * receipts are ever journaled, module header of specs/traces/IngestTrace.tla)
 * so that doctoring is not reachable here — armed at v0.2 with receipted
 * traces, same deferral the trace config's own WatermarkHonesty omission
 * documents. Each `make` returns `null` when the fresh capture's shape
 * doesn't contain the step to remove (fails loud, never silently skipped).
 */
const REAL_BACKEND_DOCTORINGS = [
  {
    name: "mid-trace-stagecommit",
    expect: "halt",
    make(records) {
      const idx = records.findIndex(
        (r, i) =>
          r.event === "StageCommit" &&
          i < records.length - 1 &&
          records.slice(i + 1).some((later) => later.event === "ClientAck"),
      );
      return idx === -1 ? null : renumber(records.filter((_, i) => i !== idx));
    },
  },
  {
    name: "trailing-lakecommit",
    expect: "invariant",
    invariant: "TraceComplete",
    make(records) {
      const idx = records.findLastIndex((r) => r.event === "LakeCommitOk");
      if (idx === -1) return null;
      const drop = new Set([idx]);
      if (records[idx + 1]?.event === "DropWindow") drop.add(idx + 1);
      return renumber(records.filter((_, i) => !drop.has(i)));
    },
  },
  {
    name: "trailing-clientack",
    expect: "invariant",
    invariant: "TraceComplete",
    make(records) {
      const last = records.length - 1;
      return records[last]?.event === "ClientAck"
        ? renumber(records.filter((_, i) => i !== last))
        : null;
    },
  },
];

async function assertDoctoredMechanism(doctoring, records, vocabulary, scratch) {
  const doctored = doctoring.make(records);
  if (doctored === null)
    fail(
      `trace-conformance: real-backend doctoring '${doctoring.name}': the fresh capture has no matching step to remove — the composition's shape changed`,
    );
  const text = toNdjson(doctored);
  const decodeError = decode(text, vocabulary);
  if (decodeError !== null)
    fail(
      `trace-conformance: real-backend doctoring '${doctoring.name}': doctored trace rejected by the decoder (${decodeError}), expected mechanism '${doctoring.expect}' — a tooth is hiding behind another`,
    );
  const path = join(scratch, `ingest-real-${doctoring.name}.ndjson`);
  await Bun.write(path, text);
  const tlc = await refine(path);
  if (tlc.code === 0)
    fail(
      `trace-conformance: real-backend doctoring '${doctoring.name}': TLC ACCEPTED a doctored trace — the '${doctoring.expect}' tooth went blunt`,
    );
  if (doctoring.expect === "halt") {
    const at = haltCursor(tlc.out);
    if (at === null)
      fail(
        `trace-conformance: real-backend doctoring '${doctoring.name}': expected a refinement halt, TLC printed no TraceHalt cursor`,
      );
    if (tlc.out.includes("is violated"))
      fail(
        `trace-conformance: real-backend doctoring '${doctoring.name}': an invariant fired before the expected refinement halt — wrong mechanism`,
      );
    info(`  real-backend doctoring '${doctoring.name}': refinement halt at entry ${at} — as required`);
    return;
  }
  const wanted = `Invariant ${doctoring.invariant} is violated`;
  if (!tlc.out.includes(wanted))
    fail(
      `trace-conformance: real-backend doctoring '${doctoring.name}': expected "${wanted}", not found in TLC output — wrong mechanism`,
    );
  info(`  real-backend doctoring '${doctoring.name}': ${doctoring.invariant} violated — as required`);
}

async function realBackendsTier(vocabulary) {
  const missing = REAL_BACKEND_ENV.filter((name) => !process.env[name]);
  if (missing.length > 0) {
    if (process.env.CI === "true")
      fail(
        `trace-conformance: real-backend tier: CI must provide MinIO + Postgres (missing ${missing.join(", ")}) — an absent endpoint fails, it does not skip (§8.2)`,
      );
    notice(
      `real-backend tier: ${missing.join(", ")} absent — skipping locally (Docker-optional dev convenience, §8.2; the CI gate does not inherit this skip)`,
    );
    return;
  }
  const scratch = mkdtempSync(join(tmpdir(), "duckspout-trace-real-"));
  // "ingest-" prefix: tla.mjs tv pairs a trace with its *Trace.tla spec by
  // filename prefix (<module>-*.ndjson ↔ <Module>Trace.tla) — see refine().
  const fresh = join(scratch, "ingest-real-backends-capture.ndjson");
  info("  capturing a fresh trace against real MinIO + Postgres (duckspout-daemon tests/trace_capture_real_backends.rs)");
  const code = await run(
    [
      "cargo",
      "nextest",
      "run",
      "-p",
      "duckspout-daemon",
      "--test",
      "trace_capture_real_backends",
      "--retries",
      "0",
    ],
    { env: { ...process.env, DUCKSPOUT_TRACE_CAPTURE_OUT: fresh } },
  );
  if (code !== 0)
    fail(`trace-conformance: real-backend capture failed (cargo nextest exit ${code}) — no fresh trace, no green (fail-closed)`);
  if (!existsSync(fresh))
    fail("trace-conformance: the real-backend capture test passed but wrote no trace — fail-closed");
  const text = await Bun.file(fresh).text();
  const decodeError = decode(text, vocabulary);
  if (decodeError !== null) fail(`trace-conformance: real-backend capture failed decoding: ${decodeError}`);
  const tlc = await refine(fresh);
  if (tlc.code !== 0)
    fail(`trace-conformance: the real-backend capture failed refinement (exit ${tlc.code}) — the implementation drifted from the model`);
  info("  real-backend capture conforms");
  const records = parseTrace(text);
  for (const doctoring of REAL_BACKEND_DOCTORINGS)
    await assertDoctoredMechanism(doctoring, records, vocabulary, scratch);
}

async function main() {
  for (const input of [join(TRACES, "IngestTrace.tla"), join(TRACES, "IngestTrace.cfg")]) {
    if (!existsSync(input)) {
      notice(`STAGED: ${input.replace(repoRoot + "/", "")} absent until its milestone (ledger row 'conformance')`);
      process.exit(STAGED);
    }
  }
  const vocabulary = await journaledVocabulary();
  await group("trace-conformance: fixtures (the teeth)", () => fixturesTier(vocabulary));
  await group("trace-conformance: live capture", () => liveTier(vocabulary));
  await group("trace-conformance: real backends (MinIO + Postgres)", () => realBackendsTier(vocabulary));
  notice("trace-conformance: fixtures, live capture, and real backends all conform");
}

await main();
