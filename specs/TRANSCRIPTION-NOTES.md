# Transcription notes — formal-core.md → runnable TLA+ (issue #41)

Every judgment call made while turning the verbatim formal core
([`formal-core.md`](formal-core.md), §3.2–§3.4) into the runnable modules
(`DuckSpoutCore.tla` + `Ingest`/`Drain`/`Schema`), numbered as `TN-<n>` and
cited from the `.tla` sources at the exact spot each applies. Nothing here
changes semantics silently: the minimal faithful reading was taken in each
case, and the four entries marked **FINDING** are places where transcription
surfaced a genuine gap or falsifiable formula in the source text — each is
also raised on issue #41 for design review.

Section references (`3.x`) are to formal-core.md's §3.2–§3.4 content;
design-doc references name `docs/design/*.md`.

## State space and constants

- **TN-1 — `WindowClosed` made concrete.** The late-arrival hold is
  "window-close timing, abstracted into `WindowClosed`" (3.1), but no
  action closes a window. Rendered as one environment transition:
  variable `closed`, action `CloseWindow(p, w)` (enabled once no request
  of `(p, w)` is pending or inflight), and `Accept` refuses admission into
  a closed window. A request still unsent when its window closes stays
  unsent forever — in the product it would be routed to a later
  arrival window (drain.md §3), which a constant `WinOf` cannot express.
  `CloseWindow` is weakly fair (a window that can close eventually does),
  which is what makes `WatermarkEventuallyAdvances` non-vacuous.
- **TN-2 — `WipeBudget = 0` in v0.1 clean configs.** The RF−1 wipe budget
  is Replication.tla's fault machinery (v0.2). `CrashWipe` stays in the
  shared Next, armed by budget; `Witness_LossDeclared.cfg` raises the
  budget to 2 ("raised past RF − 1", 3.6) to reach `DeclareLoss`.
- **TN-3 — crash bounding and scoping.** `CrashNode` is "enabled at any
  interleaving point — no guard but life"; unbounded, that makes
  `FenceBoot` mint incarnations forever and the state space infinite.
  Bounds: `crashBudget` (constant `MaxCrashes`) counts crashes;
  `Crashable` scopes which nodes crash in a given configuration (the
  hazard needs *a* crash at every interleaving point, not every node ×
  every point). `FenceBoot` is weakly fair so liveness is not lost to a
  node that stays down forever. Ingest runs `MaxCrashes = 1` (crash
  schedules for `ClientTimeout`, `FenceBoot` replay, NoAckedLoss); Drain
  and Schema run `MaxCrashes = 0` in their clean configs — the crash
  dimension multiplies their state spaces ~18× and their owned hazards
  don't need it; `Witness_CrashBetweenCommitAndDemote.cfg` overrides
  Drain's to 1 to reach the commit/demote crash window.
- **TN-4 — advisory registry trimmed.** `claims` rows carry
  `[node, part, thru, inc]` in 3.2, but no v0.1 property may read them
  (they are advisory by rule) and `thru`/`inc` multiply states for
  nothing a check can see. v0.1 claims are `<<node, part>>` pairs;
  `HoldsClaim` is membership. Drain's config pre-seeds all claims
  (`InitClaims = Nodes × Partitions`): every node already routes drains —
  the racing-drain shape, strictly more adversarial for the commit fence.
  `hb` is bounded by `MaxHb = 0` (nothing v0.1 reads heartbeats;
  `Heartbeat` stays in Next, disabled by the bound). v0.2 restores both.
- **TN-5 — `DKeyOf` collapses `TenantOf`/`Hash`.** Only `DKey` equality
  is ever read; the tenant-scoping of collisions ("cross-tenant
  collisions do not exist in any configuration", 3.1) is realized by the
  constant assignment, exactly as the doc pins it.
- **TN-6 — the lattice carrier.** 3.2 declares `LatticeElem` as a
  constant ("a 3-chain + join"). Rendered as `LatticeElem == SUBSET
  Columns` with join = union: `Columns = {c1, c2}` gives exactly a
  3-chain ({} ⊂ {c1} ⊂ {c1,c2}) plus the join sibling {c2}. Configs with
  `Columns = {}` make the lattice trivial and `EvolveSchema` unreachable —
  the schema toggle, without a dedicated switch.
- **TN-7 — `DrainOn`.** The README requires each module to "project the
  state space it needs" and sanctions constant toggles for gated
  machinery. `DrainOn` gates the drain pipeline (seal/put/commit/expire/
  demote/claims/window-close/loss); Ingest and Schema run with it FALSE.
  It also appears, verbatim from 3.5, as `WatermarkEventuallyAdvances`'s
  `DrainEnabled` antecedent.
- **TN-8 — `AcceptorOf` pins arrival routing.** Like `WinOf` pins arrival
  timing (3.2), `AcceptorOf` pins which node a request lands on. The data
  path stays coordinator-free in the model (`Accept` exists for every
  node); the checked configurations pin routing because free routing
  multiplies the space ~16× without adding a hazard the pinned shapes
  miss — each module's header says which shape its pins buy
  (collision-at-entry-holder for Ingest, three origins in one window for
  Drain).
- **TN-9 — uniform `Rec` shape.** TLC compares set elements, so records
  in one set must share a field domain. All recs carry
  `[req, part, origin, seq, window, dataset, elem]`; a schema record has
  `req = None` (`IsSchemaRec`), a data record carries its dataset and the
  schema it was written under (TN-13). Schema records ride `window 0` —
  outside the window plane, never sealed, never dropped: their ordering
  work happens through seq contiguity, which is preserved.

## Selectors formal-core elides (spelled per its own prose)

- **TN-10 — `AppliedThru` as a prefix length.** Defined as the longest
  1-based contiguous prefix of (held seqs ∪ drained seqs). A plain
  cardinality would let a peer skip a hole created by out-of-order window
  commits; `PrefixLen` keeps "replicated through s is one number" exact.
  Drained seqs count because `DropWindow` legitimately removes drained
  rows ("staged ∪ committed still is [a prefix]", 3.4).
- **TN-11 — the origin acks.** `ClientAck`'s `H == {n} ∪ holders` counts
  the acking node as a durable holder, which is only sound when `n` *is*
  the origin ("enabled only when the origin durably holds the record",
  3.3). Added `r.origin = n` to `ClientAck` and `ReceiptWaitExpired`; a
  non-origin resolves duplicates through `DedupCheck` replay instead.
- **TN-12 — `RingPeers(p, n) == Nodes \ {n}`.** HRW placement is a pure
  routing function (ADR-0004); at model scope every peer is in the ring.
- **TN-13 — schema-in-band carried per record, not per message.** 3.3's
  `Forward` snapshots the sender's whole `schema[n]` into each message
  (`sAt`). Model-wise that multiplies `chan` by every schema state a
  sender passes through — with no new hazard, since the fail-closed
  clause exists to keep *data bearing unlearned columns* out. Rendered:
  a data record carries the lattice element it was written under
  (`elem := schema[n][DsOf[q]]` at `StageCommit` — the columns the row
  can actually contain), and `SchemaKnown(m, g)` fails closed unless
  `g.rec.elem ⊆ schema[m][dataset]`. A schema record is self-describing
  (it *teaches* its element; comparing it against the receiver's
  not-yet-widened schema would deadlock the widening). Widen-before-data
  ordering is preserved: a peer cannot apply a record whose columns it
  has not learned, and schema records applied via `PeerApply` join the
  receiver's schema exactly as 3.3's EvolveSchema comment states.
- **TN-14 — drain-time dedup, deterministically.** drain.md §2 pins the
  kept row as smallest `(origin, seq)`; model values are unordered, so
  `CanonKept` keeps one deterministic representative per `DKey`
  (`CHOOSE`), which is the same canonical choice wherever it is
  evaluated. `dedupRemoved = |rows| − |CanonKept(rows)|`.
- **TN-15 — `disc` encodings.** TLC throws on comparing values of
  different types, and `disc` sits in one record field across three
  kinds. All three are sets of 3-tuples: `{}` for kind `window` (the
  fixed `"-"`), the coverage key-set for a supplement (its per-origin seq
  range), `{<<p, o, asof_o>>}` for a snapshot (`snapshot_as_of_seq`).
- **TN-16 — `SealSupplement` added.** 3.3 has no supplement seal action,
  yet supplements must exist for `SingleDrainCommit`'s second conjunct,
  `SupplementOverlap`, and both supplement witnesses — and without them
  `WatermarkEventuallyAdvances` fails at the model's own hands whenever a
  winner part lacks a receipted residue. Minimal rendering: a claim
  holder seals its window rows not covered by committed parts, kind
  `supplement`, disc = the residue's key range, once a window part is
  committed (replication.md §6, drain.md §6 "the supplement path").
- **TN-17 — `RetentionElapsed(pt) == wm[pt.part] >= pt.window`.**
  Retention timing is operational and nondeterministic, but a retention
  clock is orders of magnitude longer than a drain: modeling expiry as
  enabled before the part's window even completes explores no real
  schedule and multiplies the space. The snapshot-expiry clause ("a
  snapshot expires only under a newer covering snapshot") is spelled as
  a guard; with TN-29's one-snapshot scope it never fires, exactly as
  3.3's prose says.
- **TN-18 — `CommittedDurably(p, w) == wm[p] >= w`.** "The lake commit +
  watermark txn is durable" for a window is precisely "the watermark has
  passed it": `Done(w)` requires every extent key committed or
  loss-ledgered, which is the strongest durability statement the catalog
  makes about a window.
- **TN-19 — `NodeOrd`.** The changelog fold is "in (origin, seq) order";
  model values are unordered, so a fixed `CHOOSE`-enumerated order stands
  in. Every fold (`SnapshotSeal`'s `LatestByKey`, both sides of
  `LatestViewCorrect`) uses the same order, which is all the determinism
  the design claims.
- **TN-20 — `FenceBoot` promotes degraded nodes.** 3.3: "FenceBoot, when
  the catalog returns, promotes n" — but its guard `~alive[n]` would
  forbid promoting a live degraded node. Guard is
  `~alive[n] \/ n \in degraded`.
- **TN-21 — `DeclareLoss` over ground truth.** The ceremony's elided
  selectors are spelled as the honest-configuration identity the README
  demands ("the guard and the ground-truth predicate are the identical
  formula"): `Advertises` = actual un-wiped holdings; a loss range is a
  single hole key (an extent key no committed part covers and no ledger
  row admits); the watermark recomputation with the new ledger row *is*
  `AdvancePast`, in the same transaction.
- **TN-22 — `EvolveSchema` widens strictly.** The verbatim guard
  `s = LatticeJoin(schema[n][d], s)` admits `s = schema[n][d]`, a no-op
  that mints a fresh sequence number forever (unbounded). Added
  `s # schema[n][d]`; idempotent re-application still exists where it
  matters (`PeerApply` join; re-forwarded records).

## Spec plumbing

- **TN-23 — quiescence is stuttering, not deadlock.** The runner invokes
  TLC without `-deadlock`, and a fully-resolved, fully-drained
  configuration is a legitimate terminal state. `Next` includes
  `UNCHANGED vars`, the standard stuttering escape; liveness is
  unaffected (fair actions still may not be forever ignored), and
  genuine wedges surface through the liveness properties instead.
- **TN-24 — fairness beyond the 3.5 list.** The 3.5 list (WF on the
  resolver and drain-pipeline actions, "TakeoverDrain's fairness is
  load-bearing") is not sufficient for its own liveness properties at
  v0.1 scope; the additions, each forced by a property the list itself
  owns: WF on `DedupCheck` (the only resolver of a colliding duplicate —
  without it `EveryRequestResolves` fails on the *pinned* colliding-DKey
  configuration), WF on `CloseWindow` and `ClaimAdvertise` (a window
  that may forever not close, or a partition nobody ever claims, stalls
  `WatermarkEventuallyAdvances` — the v0.1 analog of TakeoverDrain's
  load-bearing fairness), WF on `FenceBoot` (a node that may forever not
  recover is TN-3's crash bound turned into a liveness hole), and
  **strong** fairness on `LakeCommitOk` (weak fairness is insufficient:
  Indeterminate/Reconcile cycles disable the commit recurrently, and
  "LakeAccepts" — the catalog accepting commits — is exactly the
  assumption `Finding_WatermarkThroughCatalogOutage` removes to stay
  red).
- **TN-25 — why no v0.1 config runs RF = 1.** v0.1 deploys single-node,
  and at RF = 1 `DurableAck` is fsync-only (the origin's own durable
  write is the entire evidence set — `H = {origin}` satisfies
  `Cardinality(H) >= 1` the moment `StageCommit` lands). But
  formal-core's own reachability pins make an RF = 1 *checked
  configuration* either vacuous or unsound: `DedupCheck`'s pre-RF branch
  is dead code (AtRF is true the instant the original stages),
  `AckBeforeReceipt`'s dropped conjunct changes nothing, and with two
  origins at RF = 1 the sealed extent cannot see the second origin's
  unreceipted acked rows, so `WatermarkHonesty` is genuinely violated —
  the model correctly refuses the configuration. The checked scope is
  the doc's own (3.1: RF = 2), which is also the scope every armed
  variant needs. The v0.1 single-node deployment story is a *deployment*
  fact, not a model scope; its fsync-only ack semantics are stated here
  and in the Ingest header rather than encoded as a config that checks
  nothing.
- **TN-26 — Schema.tla's owned properties.** "Lattice monotonicity" is
  the action property `SchemaMonotone` (`schema[n][d]` only ever widens).
  "Replay convergence" is rendered as safety: two un-wiped nodes with
  equal applied prefixes on a dataset's home partition have equal schema
  (`SchemaConvergence`) — deterministic replay of the same prefix
  converges, with no liveness assumption about when prefixes equalize.
- **TN-29 — drain-scheduler serialization, one candidate per sealer, one
  snapshot.** Three state-space cuts, each anchored in the design docs:
  a sealer re-draining a window produces the *same deterministically
  named part* (drain.md §5), so one window/supplement candidate per
  sealer per window; the drain scheduler serializes per partition
  (drain.md §2/§6); and at most one snapshot exists in the checked scope
  — 3.3's prose verbatim ("a newer covering snapshot, which the small
  configuration never seals"). Commits are sealer-driven
  (`pt.sealer = n` on the commit family): the drainer registers its own
  part (drain.md §4's committer is the drain choreography's own).

## Scope justifications the module headers cite

- **TN-27 — Drain needs three nodes.** With two nodes, every receipted
  key is held by the only possible non-origin peer, so a sealer's extent
  always equals its coverage: no reachable state has a receipted key the
  sealer lacks, hence no hole for `WatermarkPastHole` to widen, no
  `Witness_SupplementPending` state, and no divergent supplement pair for
  `SupplementOverlap`. Three nodes is the minimum scope in which the
  sealed extent does work — and is the doc's own node scope (3.2).
- **TN-35 — the drain hazards are checked as one module, three scopes.**
  Measured, not guessed: every 3-node full-pipeline configuration with
  two or more requests explodes into multiple millions of reachable
  states (successive runs at {q1,q2,q4}-changelog, {q1,q2,q4}-event and
  {q1,q4}-event all left TLC's queue still growing past 1.5M distinct
  states) — the cross product of seal-time part variants (coverage and
  extent are sampled at seal and frozen into the part value), the
  four-way commit outcome, post-drain residency and the replication
  lattice. The resolution follows 3.1's own pin structure:
  - `Drain.cfg` — the exhaustive clean configuration WITH the liveness
    properties — runs 3 nodes, ONE request, event-class, one window: the
    largest scope TLC exhausts, and still the shape carrying the
    module's core hazards (the extent that blocks the watermark, the
    supplement chain that completes it, racing holder-vs-empty
    candidates, the three-way commit, post-drain residency).
  - "Divergent coverage between racing drains" is, in 3.1's own words,
    the pin that makes *DoubleDrain's* two candidate parts differ — it
    lives in `DoubleDrain.cfg` and `SupplementOverlap.cfg` (two origins
    plus a replica); drain-time dedup is `DemoteDirty.cfg`'s
    colliding-key scope; the changelog/snapshot machinery is
    `DrainSnapshot.cfg`'s (2 nodes, one contended client key, a
    tombstone).
  - The single checked window means the multi-window prefix rule of
    `NewWatermark` is not exercised at v0.1 — flagged, with the
    empty-window question below, for v0.2.
  - `IndetOn` scopes the Indeterminate commit outcomes to one node the
    way `Crashable` scopes crashes (TN-3): which node's catalog
    connection can die is a fault-schedule pin, and one dying node
    reaches every Indeterminate/Reconcile behavior the invariants read.
  - The changelog scope (`DrainSnapshot.tla`) ships WITHOUT a clean
    `.cfg`: even at 2 nodes / 2 requests / 1 window / a single drainer,
    its exhaustive space measured past 2.5M states with the queue still
    growing — beyond the per-PR bounded-tier budget. The module is armed
    today through `broken/ExpireUncovered.cfg`, and its exhaustive
    configuration is deferred to the nightly tier (ledger row `tla-sim`,
    issue #48), recorded as issue #41 remainder. The partial sweeps of
    that scope are what surfaced TN-34 and TN-36.
- **TN-37 — LakeCommitAbort is conflict-driven.** The verbatim "conflict
  or refusal" enables an abort of any commit-eligible candidate at any
  moment; a transient catalog refusal only returns the candidate to the
  same commit-eligible state, so modeling it as a distinct journey
  multiplies schedules with no distinct outcome. The transcribed guard
  aborts exactly when the commit guards fail (the conflict path — the
  loser of a race discarding its work), which is the abort the safety
  argument reads.
- **TN-36 (FINDING) — the commit fence spans expired parts.** TLC found a
  clean-config SingleDrainCommit violation with the fence quantified over
  `lake` alone: a window part commits, retention expires it (removing it
  from `lake`), and a second window part then re-registers the same
  (partition, window, kind, disc) fence key — after which a supplement
  sealed against the first part's coverage overlaps the second. drain.md
  section 7 already says expiry is "metadata-only from the table's
  perspective": the registration row — the fence — outlives the file.
  `UniqueOk` and the supplement disjointness proof are transcribed over
  `lake \cup expired` ("at most one window part per window, EVER"); the
  3.3 formula's `x \in lake` should be amended to say which table the
  UNIQUE constraint really lives in.
- **(FINDING, model-external) — the empty-window watermark.**
  `NewWatermark`'s `Done(w)` requires a committed part for `w`; a window
  in which nothing was staged never gets one, so `complete_through` can
  never pass it. The design docs do not say how an empty window
  completes (an empty manifest commit? the next window's manifest
  carrying it?). Flagged on issue #41; the v0.1 configurations populate
  every window they check.
- **TN-28 — Schema needs two partitions.** Within one (partition, origin)
  log, gap refusal alone orders a schema record before the data behind
  it; `SchemaKnown`'s fail-closed clause only does independent work when
  data rides a different partition than the dataset's home log. Data on
  p2, home on p1.

## FINDINGS surfaced by transcription (raised on issue #41)

- **TN-31 (FINDING) — GapFreedom's verbatim formula is falsifiable by a
  legal state.** Windows commit in any order (`wm` is the prefix
  tracker, commits are not ordered), so `DrainedSeqs(p, o)` need not be
  a prefix while a node holds nothing for `(p, o)`: `S = {}`,
  `D = {2}` fails `S ∪ D = 1..|S ∪ D|`. The transcription quantifies
  over holders (`S # {} =>`), which preserves the stated hazard —
  non-contiguous *holdings* — exactly. The source formula should be
  amended.
- **TN-30 — `DrainedSeqs` excludes snapshots** (same family): a
  snapshot's latest-per-key coverage is non-contiguous *by design*;
  counting it as drained log range breaks the prefix arithmetic. A
  snapshot is a derivation, not a drain of the log.
- **TN-32 (FINDING) — `DropWindow` must not drop uncovered residue.**
  Verbatim, `DropWindow` removes *all* window rows once
  `CommittedDurably`. But a winner part seals an extent of receipted
  keys only; a throttled, never-receipted row at another origin is
  invisible to it, `wm` legitimately passes the window (nothing acked is
  missing), and the verbatim drop then discards durable data the design
  promises "will drain" (ingest.md §4.1's stage-then-throttled cure) —
  in the model this surfaced as a GapFreedom violation, and one ack
  later it would be NoAckedLoss. Transcribed: only rows covered by the
  lake or the loss ledger leave staging; the residue stays and later
  seals as a supplement. The source text should state the coverage
  filter.
- **TN-33 (FINDING) — CacheTransparency scoped to "its committed part".**
  A supplement (another origin's residue) can commit *after* a node
  demoted its window: the union of the window's parts then exceeds the
  demoted table, and with colliding DKeys across origins the parts can
  even hold payload-duplicate rows that per-part drain dedup cannot see.
  3.4 states the lemma as "row-identical to **its** committed part";
  the transcription compares the cache table against the kind-`window`
  part it substituted (guard and yardstick both), which is the lemma the
  Demote-safety discharge needs — the read-answer theorem is explicitly
  discharged elsewhere (one-side-serving rule + the §8.4 judge). Two
  design follow-ups raised: (a) demote-then-late-supplement leaves a hot
  table missing supplement rows (harmless under the v1 one-side rule,
  load-bearing if that rule ever relaxes); (b) cross-origin payload
  duplicates that drain into disjoint-coverage parts of the same window
  are deduplicated by no tier.
- **TN-34 (FINDING) — snapshots must retain tombstones.** 3.3's
  `SnapshotSeal` prose says "deleted keys absent", and `LatestViewCorrect`
  asserts snapshot-plus-changelog-since equals the full (origin, seq)
  fold. TLC found these inconsistent: seal a snapshot whose as-of vector
  has seen a tombstone from one origin but not an earlier-ordered upsert
  from a slower origin; the straggler then arrives in changelog-since,
  and with the tombstone folded into *absence* the overlay resurrects
  the deleted key while the ground-truth fold keeps it dead. Retaining
  tombstone rows in the snapshot (dropping them at read time) makes the
  invariant hold for every as-of vector — the same reason Kafka's log
  compaction retains tombstones for a delete-retention window. The model
  keeps tombstones; the design text should choose (retain-with-age-out,
  or constrain snapshot as-of vectors) and say so.
- **(FINDING, model-external) — deterministic part naming vs divergent
  racing drains.** drain.md §5 derives object names from (partition,
  window, kind, discriminator) and argues re-PUTs are byte-identical;
  but two *different* drainers racing the same window produce different
  byte content under the same name, and the loser's PUT can land after
  the winner's commit — an A3 violation (non-identical overwrite) the
  catalog fence cannot see. The model sidesteps it (objects are values,
  not names); flagged on the issue.
- **(FINDING, model-external) — transient WatermarkHonesty race via
  cross-window dedup replay.** If a colliding retry is assigned a later
  window (`WinOf`), its `AtRF` replay can ack a record whose (earlier)
  window the watermark already passed while the record's residue
  supplement is still uncommitted — a reachable state in which an acked
  record is behind `wm` and not yet in the lake. The v0.1 configurations
  pin collisions within one window (Ingest) or across origins with
  origin-pinned acceptors (Drain), where the race is unreachable;
  flagged on the issue for a design answer (e.g. the replay branch
  fail-closed against `wm`, or supplement-before-receipt ordering).

## Toolchain

- The `tla2tools.jar` SHA-256 pin in `scripts/tla.mjs` was updated:
  upstream replaced the v1.8.0 release asset on 2026-09-01 (GitHub API
  `updated_at`; the digest recorded there matches the new pin).
  Verification stays fail-closed. `just tla-mc` runs green on Temurin 17
  and 21; CI's setup provides 21.
