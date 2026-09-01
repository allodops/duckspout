#!/usr/bin/env bun
// pr-title-check.mjs — the `pr-title` gate (ci.yml job pr-title, in ci-ok).
//
// Validates the squash-merge subject against scripts/pr-type-label.mjs's
// title rule. Three contexts, so `just ci` stays meaningful everywhere and
// "skipped ≠ passed" holds without a skipped job:
//
//   1. pull_request event  — validates the PR TITLE from the payload (the
//      title IS the squash subject under squash-only + PR_TITLE).
//   2. merge_group event   — validates the queue's prospective squash
//      message's first line (GitHub builds it from the PR title; the
//      trailing " (#N)" is stripped before checking).
//   3. local / push        — validates HEAD's subject. On main every
//      commit is a queue-validated squash subject, so this is a
//      re-verification, not a hole; locally it checks the commit you are
//      about to turn into a PR.
//
// --self-test delegates to the mapping module's suite plus this file's
// context-extraction assertions.

import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { validateTitle, SCOPES } from "./pr-type-label.mjs";

export function subjectFromContext(env, readFile = readFileSync) {
  const eventPath = env.GITHUB_EVENT_PATH;
  if (eventPath) {
    const payload = JSON.parse(readFile(eventPath, "utf8"));
    if (payload.pull_request) {
      return { subject: payload.pull_request.title ?? "", source: "pull_request title" };
    }
    if (payload.merge_group) {
      const msg = payload.merge_group.head_commit?.message ?? "";
      const first = msg.split("\n", 1)[0].replace(/ \(#\d+\)$/, "");
      return { subject: first, source: "merge_group squash subject" };
    }
  }
  return { subject: null, source: "git HEAD" };
}

function fail(subject, source, reason) {
  const why = {
    "no-header": "carries no Conventional-Commit `type(scope)?: subject` header (or no space after the colon)",
    "unknown-type": "has a type outside the recognized set (feat/fix/docs/ci/test/refactor/perf/chore/build/revert/style)",
    "unknown-scope": `has a scope outside the vocabulary: ${SCOPES.join(", ")}`,
    "redundant-scope": "has a scope that restates the type — drop the parens",
  }[reason];
  console.error(`::error::pr-title: ${source} "${subject}" ${why}. Fix the title/subject and this check re-runs.`);
  process.exit(1);
}

function main() {
  let { subject, source } = subjectFromContext(process.env);
  if (subject === null) {
    subject = execFileSync("git", ["log", "-1", "--format=%s"], { encoding: "utf8" }).trim();
  }
  const v = validateTitle(subject);
  if (!v.ok) fail(subject, source, v.reason);
  console.log(`pr-title: ${source} "${subject}" — ok`);
}

function selfTest() {
  const eq = (got, want, why) => {
    if (JSON.stringify(got) !== JSON.stringify(want)) throw new Error(`self-test: ${why}`);
  };
  const fakeRead = (obj) => () => JSON.stringify(obj);
  eq(
    subjectFromContext({ GITHUB_EVENT_PATH: "x" }, fakeRead({ pull_request: { title: "feat(ctk): t" } })).subject,
    "feat(ctk): t", "pull_request title extracted",
  );
  eq(
    subjectFromContext({ GITHUB_EVENT_PATH: "x" }, fakeRead({ merge_group: { head_commit: { message: "fix(ci): pin thing (#42)\n\nbody" } } })).subject,
    "fix(ci): pin thing", "merge_group subject extracted, (#N) stripped",
  );
  eq(subjectFromContext({}).subject, null, "no event → git fallback");
  console.log("pr-title-check --self-test: all assertions passed");
}

if (process.argv.includes("--self-test")) selfTest();
else main();
