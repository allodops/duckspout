# Contributing

DuckSpout is developed by an autonomous agent loop with a human choke point
(docs/seed.md s§9); human contributions follow the same rules the agents do.

## DCO sign-off

Every commit must carry a [Developer Certificate of
Origin](https://developercertificate.org/) sign-off:

```
Signed-off-by: Your Name <you@example.com>
```

`git commit -s` adds it. The DCO check is a required status; unsigned commits
do not merge.

## Conventional Commits

PR titles (and commits) follow [Conventional
Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`,
`chore:`, … with `!` for breaking changes. The repository squash-merges, and
release-plz derives versions and changelogs from PR titles, so the title is
load-bearing.

## Before you open a PR

- Run **`just ci`** — it reproduces every mechanical constituent of the
  `ci-ok` check bit-for-bit (`acpr` and the DCO status are CI-only).
- Read `CONSTITUTION.md`; the checklist in the PR template is not decorative.
- Cite the `§` (DUCKSPOUT.md) and `s§` (docs/seed.md) sections your change
  implements — the ACPR reviewer verifies the diff against them.

## PR expectations

- Fill the PR template completely: what/why, § sections touched,
  verification evidence (`just ci` summary; each new test and the failure it
  would catch), constitution checklist.
- Every PR is reviewed adversarially by ACPR (a required check); address or
  rebut findings in-thread — a re-run cannot go green until then.
- Changes touching the protected set (docs/seed.md s§9.2) additionally need
  CODEOWNERS (human) approval.
- Settled decisions live in `docs/adr/` and docs/seed.md s§1: propose
  amendments through the s§9.6 procedure, don't re-litigate them in PRs.
