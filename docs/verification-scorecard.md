# Verification scorecard

Owner-ratified process (2026-09-01, issue #136): verification earns its
place by catching things. At every milestone close, before the milestone
is marked done, the supervising session fills in this table — kills, cost,
verdict — for each layer. Standing ratchet: a layer that goes **two
milestones without a find** is demoted to nightly or deleted (the R-12
config-surface-ratchet discipline applied to the verification stack
itself). Trim decisions are made from this table, not from taste; trims
land as ordinary PRs through the gate-amendment process.

## v0.1 — durable, single node (close: 2026-09-02)

| Layer | Kills (real findings) | Cost | Verdict |
|---|---|---|---|
| TLC clean configs + pinned counts | 6 design findings — TN-31 (`GapFreedom` formula falsifiable by a legal state), TN-32 (`DropWindow` drops uncovered residue), TN-33 (`CacheTransparency` scope too narrow), TN-34 (snapshots must retain tombstones), plus 2 unlabeled findings (deterministic part naming vs. racing drains; a transient `WatermarkHonesty` race). All landed via PR #137 + design-doc corrections, closing issue #41. | `tla-mc` job, 60 min timeout/leg × 3 modules (Ingest/Drain/Schema), gated per-PR since PR #144. | keep |
| Broken variants | 13 `specs/broken/*.tla` files; confirmed each still fails to model-check clean this cycle — teeth validated, no new regression caught. | Bundled in the `tla-mc` run. | keep |
| Witnesses | 11 `Witness_*` configs; confirmed each reachability target still reaches — no regression caught. | Bundled in the `tla-mc` run. | keep |
| FINDINGS (permanently red) | 5 `Finding_*` configs. By design a "kill" here is confirming they stay red — a green one would mean a documented gap silently vanished. All 5 confirmed still failing. | Bundled in the `tla-mc` run. | keep |
| Trace conformance (#42/#44) | 1 self-catch: PR #161's own new job missed `just tla-install`, fast-failing in 0.015s and misreporting as a TLC counterexample (fixed, commit `cfd1b64`). 2 real bugs found via live MinIO+Postgres: a `PrefixStore` prefix mismatch between drain's S3 PUT and DuckDB's read-back, and DuckLake's undocumented lifetime-pinned `DATA_PATH` (R-trust-official-docs vague-docs exception invoked). | New MinIO + Postgres service containers added to CI (PR #161). | keep |
| CTK schedules + fault injectors | 2 real findings via the racing-drains test (`crates/duckspout-lake-ducklake/tests/racing.rs`): (1) `ducklake_max_retry_count` defaults to 10 and silently defeats the snapshot-conflict fence — root-caused PR #147, fixed by pinning retry=0, ADR-0010 amendment in PR #148; (2) an upstream `ducklake` DuckDB-extension SIGSEGV under concurrent SQLite-catalog access, root-caused via a custom LD_PRELOAD backtrace shim to `DuckLakeCatalog::LoadNameMaps → GetHash`, ~1.7-2% repro rate, documented (not worked around) per issue #157 (intentionally left open as a permanent upstream-limitation record). ScheduleStrategy seam landed PR #127, loom interleavings PR #149. | In-CI, part of the `test` job's integration suite. | keep — proven highest-value layer this milestone |
| Property tests (#40) | 0 shrunk failing seeds recorded yet. Teeth present (landed PR #149). | In-CI. | keep — too new to judge |
| Mutation floor (#49, nightly) | 2 real test gaps found on baseline arming: `duckspout-replication::hrw::fnv1a`'s return-value mutant survives; several `DataType` match-arm deletions survive in `duckspout-staging::engine::staging_sql_type`. Measured baseline kill rate 82.24% (464 mutants); floor set to 80.2% (PR #165). | Nightly only, ~464 mutants per run. | keep |
| Coverage floor (#45) | 0 regressions caught yet; the arming itself (PR #162) is the only event so far. | In-CI. | keep — too new to judge |
| instr-gate (#46) / smoke (#47) | instr-gate: 0 regressions caught yet, teeth present via 2 `iai-callgrind` benches (PR #160). smoke: 2 self-catches — (1) cold release-compile time (886.9s) counted as measured test runtime against a 42s ceiling, fixed by adding an untimed `--no-run` pre-build pass (PR #164, commit `522d9521`); (2) a real *production* bug in `duckspout-staging`'s §4.5 overload-ladder accounting — `RecordBatch::get_array_memory_size()` overcounts a batch decoded from a shared IPC/DuckDB-arrow allocation by summing each column's full shared-buffer capacity rather than its actual content, ~29x inflation measured empirically, which pushed the ladder into false rung-2 throttling under the smoke test's 1M-record volume (issue #168, fixed PR #169). Not a test bug — any production node decoding a shared-allocation batch was affected, throttling ingest far earlier than `hot.max_bytes` was meant to allow. | In-CI, per-PR. | keep — this milestone's highest-value non-CTK catch |

No layer is due for demotion at this close — every layer either produced a
real find this milestone or is too newly armed (< 1 milestone old) to be
eligible for the two-milestone-miss ratchet.
