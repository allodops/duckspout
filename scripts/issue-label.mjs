#!/usr/bin/env bun
// issue-label.mjs — applies `area/*` labels to ISSUES from the file paths
// and crate names their bodies cite (the castle-reboot pattern; issues
// carry no CC title, so area comes from citations, not a prefix). Modes:
//
//   event    — labels the single issue in GITHUB_EVENT_PATH's payload
//              (issues opened/edited/reopened).
//   backfill — walks every OPEN issue, applies MISSING labels; idempotent.
//
// Inference: `crates/duckspout-<name>` or a bare `duckspout-<name>` crate
// mention → `area/<name>`; `specs/`, `scripts/`, `deploy/`, `spike/`,
// `.github/` → their area labels. Existing hand-applied labels (epic,
// gate-arming, absorption, spike, revisit, ready, blocked) are never
// touched; this only ADDS area labels. NOT a gate.

import { readFileSync } from "node:fs";

const CRATES = [
  "types", "accept", "staging", "replication", "drain", "watermark",
  "lake-contract", "lake-ducklake", "ctk", "daemon", "ctl", "fleet",
  "judge", "loadgen",
];
const PATH_AREAS = [
  [/(^|[\s`(])specs\//, "area/specs"],
  [/(^|[\s`(])scripts\//, "area/scripts"],
  [/(^|[\s`(])deploy\//, "area/deploy"],
  [/(^|[\s`(])spike\//, "spike"], // reuse the existing hand-scheme label
  [/(^|[\s`(])\.github\//, "area/ci"],
];

export function areasForText(text) {
  const t = String(text ?? "");
  const areas = new Set();
  for (const c of CRATES) {
    if (t.includes(`duckspout-${c}`)) areas.add(`area/${c}`);
  }
  for (const [re, label] of PATH_AREAS) if (re.test(t)) areas.add(label);
  return [...areas].sort();
}

const repo = process.env.GITHUB_REPOSITORY;
const token = process.env.GITHUB_TOKEN;

async function gh(path, init = {}) {
  const res = await fetch(`https://api.github.com${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${token}`,
      accept: "application/vnd.github+json",
      "user-agent": "duckspout-issue-label",
      ...(init.body ? { "content-type": "application/json" } : {}),
    },
  });
  if (!res.ok) throw new Error(`${init.method ?? "GET"} ${path} → ${res.status}: ${await res.text()}`);
  return res.json();
}

async function labelIssue(issue) {
  if (issue.pull_request) return false; // PRs are pr-label.mjs's job
  const want = areasForText(`${issue.title}\n${issue.body ?? ""}`);
  if (want.length === 0) return false;
  const have = new Set((issue.labels ?? []).map((l) => l.name));
  const missing = want.filter((l) => !have.has(l));
  if (missing.length === 0) return false;
  await gh(`/repos/${repo}/issues/${issue.number}/labels`, {
    method: "POST",
    body: JSON.stringify({ labels: missing }),
  });
  console.log(`issue-label: #${issue.number} ← ${missing.join(", ")}`);
  return true;
}

function selfTest() {
  const eq = (got, want, why) => {
    if (JSON.stringify(got) !== JSON.stringify(want)) throw new Error(`self-test: ${why} — got ${JSON.stringify(got)}`);
  };
  eq(areasForText("touches crates/duckspout-drain and duckspout-lake-contract"), ["area/drain", "area/lake-contract"], "crate citations infer areas");
  eq(areasForText("edit specs/Ingest.tla and .github/workflows/ci.yml"), ["area/ci", "area/specs"], "path citations infer areas");
  eq(areasForText("no citations here"), [], "no citation, no label");
  eq(areasForText("the duckspout-lake-ducklake backend"), ["area/lake-ducklake"], "longest crate name wins whole");
  console.log("issue-label --self-test: all assertions passed");
}

if (process.argv.includes("--self-test")) {
  selfTest();
} else {
  if (!repo || !token) {
    console.error("::error::issue-label: GITHUB_REPOSITORY and GITHUB_TOKEN are required");
    process.exit(1);
  }
  const mode = process.env.ISSUE_LABEL_MODE ?? "event";
  if (mode === "event") {
    const payload = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, "utf8"));
    if (!payload.issue) throw new Error("event mode requires an issue payload");
    await labelIssue(payload.issue);
  } else if (mode === "backfill") {
    let healed = 0, page = 1, seen = 0;
    for (; page <= 10; page++) {
      const issues = await gh(`/repos/${repo}/issues?state=open&per_page=100&page=${page}`);
      seen += issues.length;
      for (const it of issues) if (await labelIssue(it)) healed++;
      if (issues.length < 100) break;
    }
    console.log(`issue-label backfill: ${seen} open item(s), ${healed} healed.`);
  } else {
    console.error(`::error::issue-label: unknown ISSUE_LABEL_MODE "${mode}"`);
    process.exit(1);
  }
}
