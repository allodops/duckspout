# specs/ — the TLA+ tree

> Ⓢ stub (docs/seed.md s§8.1): this README carries the module-ownership map
> and tooling how-to at bootstrap. The **§3 prose of `DUCKSPOUT.md` arrives
> here at absorption** (s§10: philosophy, action vocabulary, invariant
> definitions — verbatim until the formal modules land); the **formal content
> itself lands at v0.1/v0.2** per the arming ledger. Every `.tla`/`.cfg` path
> below is reserved-absent (Ⓜ), tracked by ledger rows `tla-mc-core` (v0.1)
> and `tla-mc-replication` (v0.2).

## Module-ownership map (DUCKSPOUT.md §3.1)

Shared definitions (records, keys, parts, the ladder measure) live in
`DuckSpoutCore.tla`; each checked module projects the state space it needs.
"Actions owned" does **not** mean "actions present" — every module
instantiates the full shared `Next`; ownership means this module's
configuration (pinned state count, broken variants, witnesses) is the one
that *arms* the property, where a regression is caught first.

| Module | Actions owned | Properties owned | Lands |
|---|---|---|---|
| `Ingest.tla` | Accept, DedupCheck, StageCommit, Throttle, Refuse, ClientAck, ClientTimeout | DurableAck, LadderMonotone, EveryRequestResolves | v0.1 |
| `Drain.tla` | SealPart, PutPart, LakeCommitOk/Abort/IndeterminateLanded/IndeterminateLost, Reconcile, Demote, Evict, DropWindow, SnapshotSeal, Expire, DeclareLoss | WatermarkHonesty, SingleDrainCommit, CacheTransparency, SnapshotCovered, LossLedgerTruthful, LatestViewCorrect, WatermarkEventuallyAdvances | v0.1 |
| `Schema.tla` | EvolveSchema (+ PeerApply's fail-closed guard) | lattice monotonicity, replay convergence | v0.1 |
| `Replication.tla` | Forward, PeerApply, Receipt, ClaimAdvertise, Heartbeat, TakeoverDrain, CrashNode, CrashWipe, RecoverNode, FenceBoot, DegradedBoot | NoAckedLoss, GapFreedom, FencedZombie | v0.2 |
| `traces/*Trace.tla` | (refinement siblings, §3.7) | TraceComplete + behavior membership | v0.1 |

## The four-file pattern (per module, CCF/etcd-derived — s§8.1)

1. `<Module>.tla` — the module.
2. `<Module>.cfg` — the bounded clean config (2–3 nodes, exhaustively
   checked; reachable-state count pinned exactly in `state-counts.toml`).
3. `traces/<Module>Trace.tla` — the trace-refinement sibling.
4. `broken/` variants + `fixtures/` NDJSON traces — 13 broken variants must
   fail, 11 witness assertions must be reachable, 5 FINDINGS configs must
   stay red (§3.5–§3.6); fixtures are 1 conforming + 4 doctored traces per
   module.

## `just tla-*` how-to

- `just tla-install` — fetches `tla2tools.jar` v1.8.0 +
  `CommunityModules-deps.jar` into `specs/.tools/`, SHA-256-verified
  (`scripts/tla.mjs` holds the authoritative pins); needs Temurin 21 (the CI
  setup action provides it).
- `just tla-mc <Module>` — bounded exhaustive check of `<Module>.cfg`;
  fails on any reachable-state-count drift vs `state-counts.toml`; runs the
  `broken/` suite.
- `just tla-sim <Module>` — simulation mode (nightly cadence).
- `just tla-tv <trace.ndjson>` — trace validation against
  `traces/<Module>Trace.tla`; `-workers 1` per trace, parallel across
  files; includes the mutated-trace negative control that must be rejected.

Until the gates arm, these recipes run for real when invoked directly and
exit 78 (`STAGED`) when their inputs don't exist yet — never a fake green
(s§5.1). TLC scratch (`states/`, `*_TTrace_*.tla`, `specs/.tools/`) is
gitignored.
