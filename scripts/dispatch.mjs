#!/usr/bin/env bun
// Autonomous-loop picker (SEED s§9.4). Runs under dispatch.yml (hourly cron,
// serialized by its own concurrency group) or locally with --force.
//
// Token: uses the ambient GH_TOKEN through `gh` — in CI the workflow maps
// DISPATCH_TOKEN (App/PAT) to GH_TOKEN, never the workflow's GITHUB_TOKEN:
// GITHUB_TOKEN events trigger no workflows, and github-actions[bot] would
// fail claude.yml's actor gate. claude.yml is the single runner of agent
// work; this script launches nothing itself.

import { $ } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { info, notice } from "./lib/log.mjs";

const CAP = 2; // max concurrent agent runs (D-17; enforced by counting, s§9.4)
const STALE_MS = 6 * 60 * 60 * 1000; // dispatch comment age that triggers reclaim
const DISPATCH_MARKER = "<!-- duckspout:dispatch -->";
const RECLAIM_MARKER = "<!-- duckspout:reclaim -->";

// Guard (D-17): mechanically off until the human flips DISPATCH_ENABLED at
// s§11 step 3. Locally, --force substitutes for the repo variable.
if (!process.env.DISPATCH_ENABLED && !process.argv.includes("--force")) {
  notice("dispatch: DISPATCH_ENABLED is unset (and no --force) — the loop is off; exiting");
  process.exit(0);
}

const api = async (...args) => await $`gh api ${args}`.json();
const repo = process.env.GITHUB_REPOSITORY ?? (await $`gh repo view --json nameWithOwner`.json()).nameWithOwner;
const me = (await api("user")).login;

// 1. Cap: count in-flight claude.yml runs; a GHA concurrency group cannot
// express "2 running", so counting is the mechanism.
let inFlight = 0;
for (const status of ["in_progress", "queued"])
  inFlight += (await api(`repos/${repo}/actions/workflows/claude.yml/runs?status=${status}&per_page=1`)).total_count;
if (inFlight >= CAP) {
  notice(`dispatch: ${inFlight} agent run(s) in flight (cap ${CAP}) — nothing dispatched`);
  process.exit(0);
}

const readyIssues = (await api(`repos/${repo}/issues?labels=ready&state=open&per_page=100&sort=created&direction=asc`))
  .filter((i) => !i.pull_request);

async function hasOpenPr(n) {
  const timeline = await api(`repos/${repo}/issues/${n}/timeline?per_page=100`);
  return timeline.some(
    (e) => e.event === "cross-referenced" && e.source?.issue?.pull_request && e.source.issue.state === "open",
  );
}

// 2. Reclaim: a crashed run must not strand its issue forever (s§9.4).
for (const issue of readyIssues.filter((i) => i.assignees.length > 0)) {
  const comments = await api(`repos/${repo}/issues/${issue.number}/comments?per_page=100`);
  const dispatches = comments.filter((c) => c.body.includes(DISPATCH_MARKER));
  const last = dispatches.at(-1);
  if (!last) continue; // assigned by a human, not by this picker — leave it alone
  if (Date.now() - Date.parse(last.created_at) < STALE_MS) continue;
  if (await hasOpenPr(issue.number)) continue;
  const priorReclaims = comments.filter((c) => c.body.includes(RECLAIM_MARKER)).length;
  info(`dispatch: reclaiming #${issue.number} (dispatch comment stale, no open PR)`);
  for (const a of issue.assignees)
    await api("--method", "DELETE", `repos/${repo}/issues/${issue.number}/assignees`, "-f", `assignees[]=${a.login}`);
  await api("--method", "POST", `repos/${repo}/issues/${issue.number}/comments`, "-f",
    `body=${RECLAIM_MARKER}\nReclaimed by the dispatcher: the dispatch comment is older than 6h and no open PR references this issue (SEED s§9.4).`);
  if (priorReclaims >= 1) {
    info(`dispatch: #${issue.number} reclaimed twice — labeling blocked for human attention`);
    await api("--method", "POST", `repos/${repo}/issues/${issue.number}/labels`, "-f", "labels[]=blocked");
  }
  issue.assignees = []; // eligible for picking below
}

// 3. Pick: oldest unassigned ready issue whose native blocked-by dependencies
// are all closed — the issue-dependencies API is the one source of dependency
// truth; no task-list parsing (s§9.4).
let picked = null;
for (const issue of readyIssues.filter((i) => i.assignees.length === 0 && !i.labels.some((l) => l.name === "blocked"))) {
  const blockers = await api(`repos/${repo}/issues/${issue.number}/dependencies/blocked_by`);
  if (blockers.every((b) => b.state === "closed")) { picked = issue; break; }
  info(`dispatch: #${issue.number} still blocked by ${blockers.filter((b) => b.state !== "closed").map((b) => `#${b.number}`).join(", ")}`);
}
if (!picked) {
  notice("dispatch: no unassigned ready issue with all blockers closed — nothing dispatched");
  process.exit(0);
}

info(`dispatch: picking #${picked.number} "${picked.title}"`);
await api("--method", "POST", `repos/${repo}/issues/${picked.number}/assignees`, "-f", `assignees[]=${me}`);

// 4. Race check: another assignee appearing between pick and assign wins.
const fresh = await api(`repos/${repo}/issues/${picked.number}`);
if (fresh.assignees.some((a) => a.login !== me)) {
  notice(`dispatch: #${picked.number} was concurrently assigned to ${fresh.assignees.map((a) => a.login).join(", ")} — backing off`);
  await api("--method", "DELETE", `repos/${repo}/issues/${picked.number}/assignees`, "-f", `assignees[]=${me}`);
  process.exit(0);
}

const body = [
  DISPATCH_MARKER,
  `@claude you are dispatched on this issue (autonomous loop, SEED s§9.4).`,
  ``,
  `Task: work issue #${picked.number} — "${picked.title}" — to completion.`,
  `- Read CONSTITUTION.md, the touched crate's design doc, and docs/arming-ledger.toml first (AGENTS.md).`,
  `- Respect the definition-of-done and § citations in the issue body; blockers are native blocked-by relations and are all closed.`,
  `- Run \`just ci\` before opening the PR (acpr + DCO are CI-only, s§5.1).`,
  `- Open one PR with a Conventional-Commit title, DCO sign-off, and a verification-evidence section; reference this issue with \`Fixes #${picked.number}\`.`,
].join("\n");
await api("--method", "POST", `repos/${repo}/issues/${picked.number}/comments`, "-f", `body=${body}`);
notice(`dispatch: dispatched #${picked.number} to the loop (assignee ${me})`);
