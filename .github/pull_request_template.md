<!-- PR template per docs/seed.md s§9.5. Fill every section; "n/a" must be
     argued, not assumed. Title = Conventional Commit (release-plz feeds on
     it); every commit carries a DCO Signed-off-by. -->

## What / why

<!-- What this PR does and why now. Link the task issue. -->

## § sections touched

<!-- The DUCKSPOUT.md (§n) and docs/seed.md (s§n) sections this change
     implements or affects. Reviews verify the diff against these citations. -->

## Verification evidence

- `just ci` summary: <!-- paste the summary line(s); the DCO status is CI-only (s§5.1) -->
- New tests and what they would catch: <!-- for each new test: the failure it detects. "No new tests" needs a stated reason. -->

## Constitution checklist

- [ ] Protected set untouched — or touched and flagged: this PR needs CODEOWNERS (human) review (s§9.2).
- [ ] No new `*.sh`/`*.bash` files (R-no-bash).
- [ ] Dependencies only via `[workspace.dependencies]` (s§3.1).
- [ ] Config surface unchanged — or `floors/config-surface.toml` updated with the §9.6.4 divergent-workload justification in this PR's description.
- [ ] No gate weakened, narrowed, or skipped; ledger (docs/arming-ledger.toml) consistent with CI (R-armed-or-ledgered).
- [ ] Settled decisions (ADRs, docs/seed.md s§1) not re-litigated — amendments go through the s§9.6 procedure.
