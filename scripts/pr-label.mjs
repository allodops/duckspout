#!/usr/bin/env bun
// pr-label.mjs — applies the Conventional-Commit type label to PRs, from
// the shared mapping in scripts/pr-type-label.mjs. Two modes (one module,
// zero drift — the cerberus pattern):
//
//   event    — labels the single PR in GITHUB_EVENT_PATH's payload
//              (pull_request_target opened/edited/reopened).
//   backfill — walks every OPEN PR and applies any MISSING expected label;
//              idempotent; catches event runs that queued or failed.
//
// BOT PRs ARE SKIPPED in both modes: Dependabot self-labels via
// dependabot.yml, and a foreign label edit makes it refuse to auto-rebase
// (recovery is `@dependabot recreate`).
//
// NOT a gate — the `pr-title` job owns rejection; this only decorates.
// Uses fetch + GITHUB_TOKEN (pull-requests: write); no head code executed.

import { readFileSync } from "node:fs";
import { labelsForTitle } from "./pr-type-label.mjs";

const repo = process.env.GITHUB_REPOSITORY;
const token = process.env.GITHUB_TOKEN;
if (!repo || !token) {
  console.error("::error::pr-label: GITHUB_REPOSITORY and GITHUB_TOKEN are required");
  process.exit(1);
}

async function gh(path, init = {}) {
  const res = await fetch(`https://api.github.com${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${token}`,
      accept: "application/vnd.github+json",
      "user-agent": "duckspout-pr-label",
      ...(init.body ? { "content-type": "application/json" } : {}),
    },
  });
  if (!res.ok) throw new Error(`${init.method ?? "GET"} ${path} → ${res.status}: ${await res.text()}`);
  return res.json();
}

async function labelPr(pr) {
  if ((pr.user?.login ?? "").endsWith("[bot]")) {
    console.log(`pr-label: #${pr.number} is bot-authored (${pr.user.login}); skipped (self-labels).`);
    return false;
  }
  const want = labelsForTitle(pr.title ?? "");
  if (want.length === 0) return false; // pr-title gate owns invalid titles
  const have = new Set((pr.labels ?? []).map((l) => l.name));
  const missing = want.filter((l) => !have.has(l));
  if (missing.length === 0) return false;
  await gh(`/repos/${repo}/issues/${pr.number}/labels`, {
    method: "POST",
    body: JSON.stringify({ labels: missing }),
  });
  console.log(`pr-label: #${pr.number} ← ${missing.join(", ")}`);
  return true;
}

const mode = process.env.PR_LABEL_MODE ?? "event";
if (mode === "event") {
  const payload = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, "utf8"));
  if (!payload.pull_request) throw new Error("event mode requires a pull_request payload");
  await labelPr(payload.pull_request);
} else if (mode === "backfill") {
  let healed = 0, page = 1, seen = 0;
  for (; page <= 5; page++) {
    const prs = await gh(`/repos/${repo}/pulls?state=open&per_page=100&page=${page}`);
    seen += prs.length;
    for (const pr of prs) if (await labelPr(pr)) healed++;
    if (prs.length < 100) break;
  }
  console.log(`pr-label backfill: ${seen} open PR(s), ${healed} healed.`);
} else {
  console.error(`::error::pr-label: unknown PR_LABEL_MODE "${mode}"`);
  process.exit(1);
}
