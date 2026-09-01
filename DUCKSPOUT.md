# DUCKSPOUT — Design & Blueprint

DuckSpout is a durable, replicated, queryable-hot ingestion layer for immutable
streams in the DuckDB ecosystem, draining into lake formats — DuckLake first,
Iceberg by contract. It exists to answer one operational question: *the node
holding the last five minutes just died — where are the last five minutes?*
Four pillars define the product: **durable ack** (ClientAck only after local
fsync plus RF total-inclusive replication receipts), **RF replication** with
receipted, reconciliation-free takeover, **queryable-hot** (SQL-visible the
instant its transaction commits), and **completeness** (per-partition
watermarks; `complete` reads fail closed — an empty result is a proof, never a
shrug). This document is the complete design — data model, formal TLA+ core,
ingest, replication, drain, query, verification, operations, library
architecture, governance, and roadmap — and is **pre-implementation**: every
section is normative for the build to come.

## Contents

- **§1 DuckSpout: What and Why** — the defining question, the four pillars, the honest gap, scope doctrine, who deploys this
- **§2 System & Data Model** — datasets, tenancy, micro-windows, STAGING vs CACHE, time-series as the degenerate case, the schema model, cold-layout hard rules
- **§3 The Formal Core** — TLA+ state space, the action set, invariants, liveness, broken variants and witnesses, trace conformance
- **§4 Ingest and Durability** — the accept path, WAL = hot, the ack sequence, idempotency, the overload ladder, admission constants, transforms, RawDuck
- **§5 Replication and Availability** — RF semantics, the HRW ring, ownership routing, the protocol, the registry, node death, fencing, DeclareLoss, corruption, restarts
- **§6 Drain and the Cold Tier** — immutable-with-expiry, SealPart, late arrivals, the LakeCommitter port, commit outcomes, SingleDrainCommit, retention, manifests, class mechanics
- **§7 Query** — the one-ATTACH model, bind-time resolution, registry tables, Airport/Flight, the hot∪cold union, read concerns, changelogs, guards, authorization
- **§8 Verification End-to-End** — TLC, trace conformance, CTK in-memory and distributed, property tests, ratcheted floors, the bench card and durability audit
- **§9 Operations** — deployment, capacity, self-observation, the failure runbook, security operations, the configuration appendix
- **§10 Library Architecture and Extensibility** — crates, the C++/Rust boundary, ports, integration posture
- **§11 Governance: The Keep Rules** — the twelve invariants and how they are held
- **§12 Roadmap** — spike → v1.0, deferred register, collaboration sequencing, license, the v1 cut list

## §1 DuckSpout: What and Why

### 1.1 The defining question

Object storage made the cold tier highly available. Nobody has done the same
for the hot tier. That asymmetry is DuckSpout's entire reason to exist, and it
reduces to one operational question:

> **The node holding the last five minutes just died — where are the last five
> minutes?**

In the DuckDB world, everything shared routes through S3 or a catalog
database; nothing keeps the newest data available when a process dies — and
the in-process architecture means nothing inside DuckDB ever will. This is
structural, not an oversight awaiting a release. DuckDB is a single-writer
in-process engine: a database file admits one writing process, concurrency
guidance routes multi-writer setups *around* the engine (through a server
process or a catalog database), and an in-process library by definition shares
its host's failure domain. The lake formats that grew around it — DuckLake,
Iceberg — inherit the same shape from the other side: they are durable and
highly available precisely *because* they push every byte through object
storage and every commit through a catalog, which makes them batch-latency by
construction. Between "in this process, this instant, gone on crash" and "in
the lake, durable, minutes old," the ecosystem has a hole exactly one
replication protocol wide.

DuckSpout fills that hole. It is a durable, replicated, queryable-hot
ingestion layer with completeness semantics for immutable streams, draining
into lake formats — DuckLake first, Iceberg by contract (§6). Its positioning
sentence is the answer to the defining question: **DuckSpout gives the hot
tier the availability the lake already has.**

### 1.2 The product is the four pillars

The four pillars are not a feature list; they are the derivation of hot-tier
availability, each answering one way the last five minutes can be lost or
lied about:

| # | Pillar | Guarantee | Loss mode it closes | Spec |
|---|---|---|---|---|
| 1 | **Durable ack** | ClientAck is issued only after local fsync (StageCommit) *and* replication Receipts bringing total durable copies to RF (total-inclusive, §5). An ack is a promise the bytes survive node death. | "202 then crash" — acknowledged data that never existed durably | §4 |
| 2 | **RF replication** | Every accepted record reaches RF total-inclusive copies on HRW-ring peers, with Receipts, before ack; a dead owner's partitions are taken over (TakeoverDrain) by a receipted replica. | The bytes survived but the only holder is dead | §5 |
| 3 | **Queryable-hot** | Data is SQL-queryable the instant its transaction commits — no flush, no index build, no visibility delay. | The bytes survived but the answers didn't | §7 |
| 4 | **Completeness** | Per-partition watermarks; read concerns `available` and `complete`, default `complete`, fail-closed. Degraded availability is disclosed, never disguised as an empty result. | The query ran but silently omitted what a dead node held | §2.4, §7 |

Each pillar is enforceable and each is verified: the invariants DurableAck,
NoAckedLoss, and WatermarkHonesty (§3) are the formal statements of pillars
1, 2, and 4, and the end-to-end harness (§8) fails the build when any is
violated under fault injection.

**OTLP is the first accept adapter, not the identity.** DuckSpout's accept
layer is pluggable (§10); it speaks OTLP (gRPC and HTTP) out of the box
because telemetry is the largest immutable-stream workload with the worst
tolerance for silent gaps, and because an otel-collector in front covers
every other telemetry protocol for free. But nothing in the pillars, the data
model, or the formal core mentions spans or metrics. Any immutable stream —
events by time, changelogs by key — is DuckSpout data.

### 1.3 The honest gap: what exists and what doesn't

Streaming ingestion for the DuckDB ecosystem exists today only in fragments,
and it is fair to name them:

- **Batch loaders and schedulers.** A collector writing files plus a cron job
  calling `ducklake_add_data_files` is the ecosystem's real incumbent. It is
  simple and correct — and blind for minutes, with a single-disk queue as its
  only durability and no way to say what it hasn't loaded yet.
- **DuckLake data inlining.** DuckLake can stage small writes in its catalog
  database before materializing Parquet, which raises the floor for
  small-volume streaming over time. It is single-writer-path, catalog-coupled,
  and offers no replication, no ack contract, and no completeness semantics.
- **Community OTLP extensions** (e.g. smithclay/duckdb-otlp). The closest
  conceptual neighbor: OTLP straight into DuckDB, committing to DuckLake and
  Iceberg REST. Its own README disclaims durability — the seal, not the 202,
  is the promise — and it is single-node with no watermarks. Honest about
  exactly the gap it doesn't fill.
- **BoilStream.** A commercial streaming lakehouse ingester, ~1 second to
  queryable, and the only neighbor that escapes the hot single point of
  failure — by making S3 itself the hot store under Raft coordination. It is
  coordinator-heavy on the data path, DuckLake-only, and exposes no
  completeness semantics.

None of these — nor any combination — provides durable ack **and** RF
replication **and** queryable-hot **and** completeness in one system. That
four-pillar conjunction is the gap, and every design decision in this
document exists to fill it without rebuilding what the ecosystem already
does well: DuckSpout builds no query engine (DuckDB is the query engine), no
lake format (DuckLake and Iceberg are the lake formats), no wire protocol
(OTLP and Arrow Flight are the wire protocols). The irreducible new build is
the ack path, the replication of the accepted stream, the watermark ledger,
and the drain choreography — exactly the four guarantees nobody ships.

### 1.4 Scope doctrine: facts, not state

DuckSpout stores **facts, not state** — immutable, append-only streams
corrected by appending. Entity tables as mutable tables are not DuckSpout
data; entity **changelogs** are. A keyed changelog — sequenced upserts and
tombstones about state changes — is itself a stream of facts, and DuckSpout
ingests it, replicates it, drains it, and serves derived latest views over
it. DuckSpout never mutates in place, in hot or cold, and never becomes a
mutable-table database: state whose queries, lifecycle, and mutations have
nothing temporal or sequential about them belongs in ordinary lake tables
(DuckLake handles UPDATE/DELETE) and meets DuckSpout data via JOIN.

**The doorway test.** Before a dataset comes through the door, ask: *do its
queries, lifecycle, and mutations care about time — or is it a keyed
changelog whose corrections are appended?* Yes to either: DuckSpout data.
No to both: an ordinary lake table, joined at query time (§7). The test is
normative; adapters and docs apply it, and no accept adapter may smuggle
mutable-table semantics past it.

### 1.5 Who deploys this — and who should not

Deploy DuckSpout when acknowledged data must survive node death, when the
newest data must be queryable now rather than after the next flush, and when
"no rows" must be distinguishable from "couldn't check" — telemetry
platforms with freshness SLOs, alerting pipelines where absence of a signal
is itself the signal, audit and changelog ingestion where an ack is a legal
or contractual promise, and multi-writer edges feeding one lake.

Do **not** deploy DuckSpout — and the documentation says this in exactly
these terms — if your workload tolerates minutes of ingestion blindness and
never needs absence semantics. A collector writing batches plus a scheduled
lake load is smaller, cheaper, and correct for that workload; running a
replicated ack-ladder ingestion tier under it buys nothing. DuckSpout's
docs ship the collector-plus-cron recipe alongside its own quickstart,
because recommending the simpler tool where it suffices is what makes the
recommendation credible where it doesn't.

---

## §2 System & Data Model

### 2.1 Datasets

The unit of declaration is the **dataset**. A dataset declaration carries
exactly three attributes; the set is closed and ratcheted (§11) — a new
attribute needs a divergent-workload justification, same as a config knob.

| Attribute | Values | Default | Meaning |
|---|---|---|---|
| `kind` | `event` \| `changelog` | `event` | `event`: facts on a time axis. `changelog`: keyed upserts/tombstones about entity state — the converged representation of mutable reference data over append-only substrates (Kafka compacted topics, Debezium envelopes, Materialize UPSERT sources). |
| `key_cols` | column list | — (required for `changelog`, forbidden for `event`) | The changelog's identity key. Shard axis = `hash(key_cols)`, fixed at declaration; a key change is a new dataset plus replay, never an in-place mutation. |
| `sort_key` | column list | event-time column | Drain-time `ORDER BY` for sealed parts. Sealed parts always carry row-group min/max statistics, so range queries on any declared sort column prune from cold via zone maps — layout, not caching (the ClickHouse `ORDER BY` + minmax-skip-index convergence). A changed `sort_key` applies only to parts sealed afterward; rewriting existing parts is banned (§2.7). |

Changelog records carry a system column `_op ∈ {upsert, delete}` (default
`upsert`); deletes are explicit tombstone rows, never absence. A dataset
whose natural monotone axis is not wall-clock time (block height, sequence
number) declares that column into the event-time role; lifecycle machinery
(windows, watermarks, lateness, retention) is time-axis-only and is never
generalized to arbitrary columns — "complete through price = 20" is
undefined, and non-time retention would force part rewrites, which §2.7
bans.

### 2.2 Tenancy

Tenant identity follows the Loki/Mimir/Cortex model verbatim: an
`X-Scope-OrgID` header from the mTLS-verified edge, validated at Accept
(charset, length ≤ 150, leading `_` reserved for system tenants). Tenant is
**a column, never a physical table or schema dimension** — table-per-tenant
explodes to hundreds of thousands of live tables at four-digit tenant
counts, and the converged answer (ClickHouse guidance beyond ~1000 tenants)
is shared tables ordered with `tenant_id` leading.

The partition key is **`(tenant_id, shard)`** — for `event` datasets shard
is time-oriented placement (`shard_count` default 1, per-tenant override
post-v1); for `changelog` datasets shard = `hash(key_cols)`, mandatory.
Per-partition watermarks are therefore per-tenant watermarks by
construction: pillar 4 falls out of the key shape rather than being bolted
on, and a noisy tenant's blast radius is bounded shuffle-sharding-style.

**v1 is structurally multi-tenant, behaviorally single-tenant.** The tenant
column, the partition key, header validation, the reserved `_` prefix, and
the fixed `anonymous` tenant (single-tenant mode, same code path) all ship
in v1, so nothing is retrofitted into the storage layout or the wire
contract later. Every behavioral surface — per-tenant limits, overrides,
retention-class mapping, metering, per-tenant grants — is deferred post-v1
as a design-ahead constraint, not designed later.

**System tenants.** `_self` (self-telemetry, §9) and `_canary` (probe
writes, §9) form a reserved class with inverted defaults: their ingest path
**never issues durable acks** — so DurableAck and NoAckedLoss are not
implicated rather than carved out — read concern defaults to `available`,
and retention is a built-in short class with no knob. The chaos judge's
"zero acked records lost" criterion (§8) excludes them by definition: no
acks exist to lose. This is the only lossy path anywhere in DuckSpout, and
it is named wherever the overload ladder (§4) is specified.

### 2.3 Windows and micro-window tables

The hot store is a DuckDB instance per node holding **one native table per
micro-window per partition** — not files, not segments. `hot.window`
defaults to 60 s. Window ids form a dense per-partition sequence, so
coverage contiguity is decidable — watermark reconstruction after catalog
loss (§9) depends on being able to prove "no window is missing between
these two." Ingest lands in the current window's table inside a durable
transaction (StageCommit, §4); the drain seals and uploads it (SealPart,
PutPart, LakeCommit, §6); and cleanup is DropWindow — an O(1) `DROP TABLE`,
no vacuum debt, no tombstone accumulation. Rationale: DuckDB's own WAL
gives fsync-on-commit durability and crash replay for free, and
table-per-window turns retention into the cheapest operation the engine has.

### 2.4 Two classes of hot data: STAGING and CACHE

The hot tier serves two roles with opposite obligations, and conflating
them is how systems come to either drop acked data under pressure or refuse
ingest to protect a performance cache. DuckSpout splits them into two
**classes**, tracked in node-local hot metadata (data path, not catalog).
The split is doctrine from v1 even though v1's cache class is empty by
construction (DropWindow at drain commit): the vocabulary, the invariants,
and the overload measure need it regardless of whether any table currently
lives in the cache class.

**STAGING** is the pre-drain write buffer: undrained data that must be hot.
**CACHE** is post-drain residency for query performance: the lake is
authoritative, so eviction is always safe.

#### Obligations matrix (normative)

| Obligation | STAGING (pre-drain) | CACHE (post-drain residency) |
|---|---|---|
| Evictable | Never. Throttled ingest (Throttle, Refuse — §4) is always preferred over staging loss. | Always, at any time, no coordination. Evict = `DROP TABLE`. |
| Replication | Total-inclusive RF with Receipts before ClientAck (§5). | Never. Single copy, owner-local; the lake is the durable copy. |
| Completeness | Watermark-bearing; `complete_through` advances only via this data's own drain (LakeCommit, §6) or the DeclareLoss ceremony (§5.8) — the one sanctioned weakening. | Formally invisible to completeness. |
| Serves `complete` reads | Yes (with the lake). | Only when the window's manifest records `dedup_removed = 0`; the Demote rule below makes this unconditional. |
| Cost of loss / miss | Correctness event — the DeclareLoss ceremony (§9). | Latency only, never correctness. |
| Leaves hot via | Successful drain only. | Evict, unrestricted. |
| Overload ladder (§4) | Drives the measure: M = staged_bytes / `hot.max_bytes`. | Invisible to M; shed at rung 0, which is not a disclosed status. |
| Entry path | Ingest (Accept → StageCommit) only. | Demote of a table this node itself staged and drained. **Never** fetched back from the lake. |

The Demote rule: a drained window is demoted in place to the cache class
only when its manifest records `dedup_removed = 0` (drain-time dedup
removed nothing, so the hot table is row-identical to its sealed parts);
otherwise it is dropped. `dedup_removed` is written at SealPart and is part
of the v1 manifest format unconditionally — cheap now, and it spares a
format migration when cache residency activates (§12).

#### The cache-transparency theorem

> **CacheTransparency.** For every partition, every watermark state, and
> every `complete` read, the answer is a function of STAGING ∪ lake alone;
> any two cache states — including empty — yield the identical row set.

Corollaries: a cache miss can never fail-close a `complete` read, and cache
occupancy can never affect ingest availability. Four proof obligations,
stated here; (a), (b), and (d) are discharged formally in §3 (whose
CacheTransparency formula is the Demote-safety *lemma* of this theorem,
§3.4), while (c) and the full read-answer equivalence are discharged
mechanically by §8.4's eviction-storm judge — locks and read answers are
not in the §3 state space:

1. **(a)** Demote happens only after the LakeCommit transaction (part
   registration + watermark advance, one atomic commit — §6) is durable.
2. **(b)** Exactly one side serves any window — staging XOR lake/cache —
   fenced by the drain guard (SingleDrainCommit, §3), including the
   crash-between-LakeCommit-and-Demote recovery path.
3. **(c)** Evict takes no locks the read path depends on.
4. **(d)** A cache-class table substitutes for the lake only when
   `dedup_removed = 0` — discharged by construction under the Demote rule.

### 2.5 Time-series as the degenerate case

Time-series is not a separate regime; it is the general model at its
simplest settings. An `event` dataset's monotone axis *is* event time, so
lifecycle — windows, watermarks, lateness, retention — needs no
generalization. Arrival recency predicts the working set so well that no
residency policy is needed at all: "hot while recent, cold as time passes"
is exactly what recency-ordered eviction produces with zero
timestamp-aware code, and drop-immediately-after-drain is its natural
default because firehose telemetry rarely re-queries drained windows.
Changelog datasets differ only in declaration — a key instead of a
timestamp as identity, `hash(key)` instead of time as the shard axis, a
snapshot instead of age as the retention horizon (§6) — never in mechanism.

### 2.6 The schema model

DuckSpout is schema-later without ever being schema-lossy. The logical
promise is a monotone type lattice — a column's logical type only widens,
and **nothing is ever dropped**. Physically the lattice splits into two
classes, because every surveyed system (Iceberg, DuckLake, Snowflake)
enforces lossless-only in-place changes:

- **Class I — lossless in-place promotions**: the intersection of Iceberg's
  and DuckLake's promotion sets (integer widening, float32 → float64),
  applied to hot tables and, at drain, to the lake via EvolveSchema. Lossy
  moves are excluded on principle: BIGINT → DOUBLE corrupts above 2^53 and
  would make types silently wrong.
- **Class II — generation rebinds**: everything else (type forks, lossy
  widenings) allocates a new field-ID column; the old generation is
  retained, and the query layer (§7) unifies generations with
  deterministic COALESCE + CAST projections. Bind-time unification is
  always an explicit generated projection — never `union_by_name` — so
  output types are a function of DuckSpout's lattice, not of DuckDB's
  coercion rules. A raw lake ATTACH that bypasses DuckSpout sees the
  generations; raw is raw, and the docs say so.

**Overflow, not rejection.** `max_auto_columns` (default 1024, the only
column cap) bounds automatic column creation per table; overflow keys spill
into a JSON overflow column rather than being rejected — the lattice's
terminal type is JSON, and reaching it is sticky. Rejection would violate
"nothing is ever dropped"; the cap exists because workloads genuinely
diverge (curated OTLP attributes vs. unbounded raw log keys).

**Schema changes are replicated facts.** A schema change travels in-band as
a sequenced record in the per-(partition, origin) replication log — the
same total order the data rides (§5). Peers fail closed on unknown columns;
catch-up applies widen-to-origin-schema before data replay. Because the
lattice join is commutative and idempotent, crash-retry and concurrent
appliers converge CRDT-style. At drain, schema evolution and file addition
commit atomically where the lake permits ({EvolveSchema, add files,
watermark} in one LakeCommit); where DDL and append cannot share a commit,
evolve strictly precedes add — files committed ahead of their schema
silently hide columns in both Iceberg and DuckLake, so add-before-evolve is
forbidden.

Logical schema is monotone forever. Column deletion is soft-hide (a
registry flag, ergonomics only); guarded physical drop — only when file
statistics prove physical extinction — is post-v1. Compliance-grade purge
is served by retention and redaction transforms, never by schema surgery.

### 2.7 Cold layout: the hard rules

The cold tier is object storage, and object-storage economics dictate hard
layout rules that the rest of this document treats as load-bearing
invariants (ratcheted in §11):

1. **Cold objects are immutable-with-expiry.** At most one *logical*
   PutPart (byte-identical idempotent re-PUTs on retry permitted, §6.5) and
   one whole-file DELETE per object, never modified in between. All merging
   happens hot, before SealPart: the drain's own sorted COPY over
   micro-window tables *is* the compaction, so each byte is uploaded exactly
   once, in final form. Compacting on object storage — the GET + PUT churn
   every log store eventually drowns in — is banned, not deferred; the
   lake's file-merging utilities are demoted to emergency repair. This ban
   is affordable precisely because of pillars 2 and 3: replication and
   queryable-hot decouple durability and freshness from upload, removing
   the PUT deadline, so the drain seals by size (target 256–512 MiB,
   default 384 MiB) or age cap rather than by clock panic.
2. **Parts are tenant-pure, retention-class-pure, and kind-pure.** A sealed
   part never spans tenants (enabling prefix-scoped cold-side IAM per
   tenant), never spans retention classes, and never mixes changelog and
   snapshot content. Tenant-pure is the default forever; cross-tenant
   packing is a post-v1 opt-in whose disclosed cost is the loss of
   cold-side IAM isolation for the packed tenants.
3. **Parts never span retention boundaries.** Expiry is therefore always a
   metadata-only whole-file drop — one DELETE, no rewrite, no scan.
4. **Late arrivals never trigger rewrites.** A record inside
   `drain.allowed_lateness` (default 15 m) waits in its held window; a
   straggler after seal takes arrival-window placement — its event-time
   column stays truthful, and completeness semantics (§7) already express
   the distinction.
5. **Derivation is not mutation.** A snapshot part (SnapshotSeal, §6) —
   full latest-by-key state for a changelog partition — is a *new* object
   conforming to one-PUT-one-DELETE. The ban is on rewriting existing
   objects, not on appending derived ones; snapshot rollover is exactly the
   append-only escape hatch that makes by-key cold compaction unnecessary.

Immutable-with-expiry has a free consequence worth stating once: every
downstream cache — DuckDB's external file cache, any HTTP cache, any CDN —
is trivially coherent against DuckSpout's cold tier, because an object URL
is never reused with different bytes.

## §3 The Formal Core: TLA+ Actions and Invariants

This section is DuckSpout's vocabulary and ground truth. Every protocol
step named anywhere in this document is one of the actions defined here,
under exactly the name defined here (§3.1 scopes the short list of
operational behaviors that are deliberately not actions); every guarantee
the product claims is one of the invariants defined here. Implementation subsystems emit
execution traces whose event names are these action names verbatim
(§3.7), so the mapping between model and code is a checked artifact, not
a convention.

### 3.1 Modeling philosophy

**Bounded and exhaustive, at small, deliberately chosen scopes.** Each
model is checked by TLC over its entire reachable state space at a scope
small enough to exhaust and large enough to contain the hazard it exists
for: 2–3 nodes, 2 origins, 1–2 partitions, 2 windows, ~4 records, RF = 2.
This is the converged industry practice (Amazon's published TLA+
experience with S3 and DynamoDB: protocol bugs appear at tiny scopes, and
exhaustion at a tiny scope beats sampling at a large one). Every scope
choice is justified in its module header against the hazard it must
represent — e.g. the replication model needs two origins and two records
per window so a window's full coverage genuinely requires a peer's bytes,
the only shape in which message loss matters at all. Two scope pins are
normative here because reachability depends on them: the checked ingest
configuration includes **at least two requests with colliding dedup keys**
(`DKey(q1) = DKey(q2)`) — otherwise both `DedupCheck` branches and the
`DemoteDirty` variant are dead code — and dedup-key collisions are
scoped per tenant by construction (`DKey` carries the tenant, §3.3), so
cross-tenant collisions do not exist in any configuration. The drain
configuration likewise pins **divergent coverage between racing drains**,
so `DoubleDrain`'s two candidate parts differ and the `lake` set union
cannot silently merge them.

Four operational behaviors are deliberately *not* §3 actions, with stated
rationale rather than silence: window quarantine/re-fetch (§5.9) is the
already-modeled catch-up path re-run (`PeerApply` over receipted ranges);
trickle inlining (§6.2) is a backend-private encoding of `LakeCommitOk`
(rows instead of a part pointer ride the same atomic commit, so the
`pt \in objects` guard is read as "the commit's payload is durable");
the owner/replica role flip (§5.3) is advisory local metadata no
invariant reads; and the late-arrival hold (§6.3) is window-close timing,
abstracted into `WindowClosed`. Every protocol step a guarantee depends
on is an action here; these four are routing, timing, or encoding.

**One model family, several modules.** Shared definitions (records, keys,
parts, the ladder measure) live in `DuckSpoutCore.tla`; the checked
modules project the state space each needs:

| Module | Actions owned | Properties owned |
|---|---|---|
| `Ingest.tla` | Accept, DedupCheck, StageCommit, Throttle, Refuse, ClientAck, ClientTimeout | DurableAck, LadderMonotone, EveryRequestResolves |
| `Replication.tla` | Forward, PeerApply, Receipt, ClaimAdvertise, Heartbeat, TakeoverDrain, CrashNode, CrashWipe, RecoverNode, FenceBoot, DegradedBoot | NoAckedLoss, GapFreedom, FencedZombie |
| `Drain.tla` | SealPart, PutPart, LakeCommitOk/Abort/IndeterminateLanded/IndeterminateLost, Reconcile, Demote, Evict, DropWindow, SnapshotSeal, Expire, DeclareLoss | WatermarkHonesty, SingleDrainCommit, CacheTransparency, SnapshotCovered, LossLedgerTruthful, LatestViewCorrect, WatermarkEventuallyAdvances |
| `Schema.tla` | EvolveSchema (+ PeerApply's fail-closed guard) | lattice monotonicity, replay convergence |
| `*Trace.tla` | (refinement modules, §3.7) | TraceComplete + behavior membership |

"Actions owned" does **not** mean "actions present": every module
instantiates the full shared `Next` over a projected state space and
constant set, so no property is checked in a configuration missing the
actions it quantifies over (NoAckedLoss is meaningless without ClientAck;
DurableAck without Receipt is vacuous at RF = 2). Ownership means: this
module's configuration is the one whose pinned state count, broken
variants, and witnesses *arm* the property — the place a regression in it
is caught first.

Gated features follow a state-count pinning discipline: the cache class
and the changelog machinery sit behind `CONSTANT` toggles wired so that
the disabled configuration's pinned state count is *checkably* identical
with and without the feature's variables and actions — the claim "this
addition changes nothing when off" is verified by an exact state-count
assertion, never argued in prose.

**External systems are atomic actions with stated semantics — never
modeled internally.** DuckSpout composes with three external systems; the
model gives each one exactly the semantics DuckSpout depends on, as a
named boundary assumption, and nothing more:

| Assumption | External system | Modeled semantics | Discharge |
|---|---|---|---|
| **A1** | Embedded DuckDB (hot store) | A local transaction commit is one atomic, fsynced state transition. `StageCommit` and `PeerApply` are single actions; a crash either sees the whole transaction or none of it. | Verified empirically: fsync-per-commit granularity is verified against the engine source and pinned in the compatibility matrix before each supported DuckDB version is certified (§4.2.1), and the CTK's fsync fault family (§8.3) exercises the loss modes. This is a tested premise, not a deduction from documentation. |
| **A2** | Catalog DB (Postgres) | A catalog transaction is atomic with a **three-way outcome**: `Committed`, `Aborted`, or `Indeterminate` (the connection died mid-commit). Indeterminate is resolved by **exactly one read-back** before any retry — never by resubmitting the same attempt blind. Postgres ACID is trusted. | Postgres's transactional guarantees are the trusted base; the three-way outcome and read-back discipline are DuckSpout's own obligations, modeled and trace-checked. |
| **A3** | Object store (S3 contract) | A PUT is an atomic object appearance: the object exists whole or not at all; no partial objects, no in-place modification. At most one *logical* PUT (byte-identical retries permitted) and one whole-file DELETE per object, ever (§2's immutable-with-expiry rule). | S3's documented PUT atomicity; the one-logical-PUT-one-DELETE half is DuckSpout's own invariant, enforced by the drain (§6). |
| **A4** | Network | Asynchronous, lossy, reordering, duplicating; no delay bound. Modeled as a message *set* — loss is a message never taken, reordering is inherent, duplication is re-taking. | No discharge needed; this is the adversarial assumption. |

**Ground truth versus what a guard consulted.** Every invariant below is
written over true model state — monotone ledgers, the catalog's committed
register, the real receipt set — never over the sample a node's own guard
happened to read. In the honest configuration the guard and the
ground-truth predicate are the identical formula, so the invariant holds
by construction; each broken variant (§3.6) perturbs exactly one clause
of the *guard* while the yardstick never changes. That separation is what
makes a violation representable rather than definitionally impossible —
a model whose invariant restates its own guard checks nothing.

### 3.2 The state space

```tla
CONSTANTS
  Nodes,        \* model scope: 3
  Partitions,   \* (tenant, shard) pairs; model scope: 1-2
  Windows,      \* dense per-partition window ids; model scope: 2
  Requests,     \* client write requests; model scope: 4, >= 2 sharing a DKey (§3.1)
  RF,           \* replication factor; model scope: 2
  SoftLim, ThrottleLim, HardLim,   \* ladder thresholds on staged bytes
  WipeBudget,   \* permanent-loss fault budget; RF - 1 in the checked config
  Datasets,     \* declared datasets; model scope: 1-2
  LatticeElem,  \* the schema lattice's carrier set; model scope: a 3-chain + join
  WinOf         \* [Requests -> Windows]: arrival timing abstracted to a constant
                \* assignment of requests to windows

VARIABLES
  \* -- per-node hot state ------------------------------------------------
  inflight,     \* [Nodes -> SUBSET Requests]  volatile: accepted, unstaged
  staged,       \* [Nodes -> SUBSET Rec]       durable: fsynced rows (A1)
  dedup,        \* [Nodes -> SUBSET DedupEntry] durable: same txn as staged;
                \* an entry carries [key, acked, orig] - orig names the request
                \* that staged it, so a replay can copy the original's evidence
  cache,        \* [Nodes -> SUBSET WinTbl]    cache class: durable, expendable;
                \* a WinTbl CARRIES its row set - Rows(t) is a field read, so
                \* CacheTransparency is computable from the entry alone
  nextSeq,      \* [Nodes -> [Partitions -> Nat]]  per-origin sequence.
                \* Init: 1 everywhere - sequences are 1-based, so GapFreedom's
                \* prefix arithmetic and AppliedThru + 1 agree by construction
  \* -- replication -------------------------------------------------------
  chan,         \* SUBSET Msg      the network (A4): every message carries inc
  receipts,     \* SUBSET Receipt  durable-apply acknowledgements
  highestSeen,  \* [Nodes -> [Nodes -> Nat]]  fencing: the highest incarnation
                \* each receiver has seen per sender - the fence a node can
                \* actually hold (§5.7); the global inc[] is ground truth that
                \* only invariants and the catalog read
  \* -- client-visible ----------------------------------------------------
  resolved,     \* [Requests -> {"unsent","pending","acked","throttled","refused"}]
  ackEvidence,  \* [Requests -> SUBSET Nodes]  holders ledgered at ack instant
  recOf,        \* [Requests -> Rec \cup {None}]  history ledger (ground-truth-
                \* only): the record each request minted, written at StageCommit
                \* and copied at DedupCheck replay - KeyOf(q) == Key(recOf[q])
                \* and WindowOf(q) == recOf[q].window stay evaluable after
                \* DropWindow removes the record from every staged[n]
  \* -- cold tier and catalog ---------------------------------------------
  sealedParts,  \* SUBSET Part     sealed locally, awaiting PutPart
  objects,      \* SUBSET Part     S3: PUT-complete objects (A3)
  lake,         \* SUBSET Part     catalog-committed parts
  expired,      \* SUBSET Part     history ledger: parts retention has expired
                \* (Expire) - sanctioned removal, distinct from loss
  wm,           \* [Partitions -> Nat]  complete_through, per partition
  lossLedger,   \* SUBSET LossRow  permanent declared-loss rows; a row carries
                \* [part, range, liveAtDecl] - liveAtDecl is a history flag:
                \* TRUE iff a live un-wiped node held coverage of the range at
                \* declaration time (LossLedgerTruthful's yardstick)
  catalogSeq,   \* Nat             the incarnation mint (a catalog sequence)
  pendingCommit,\* [Nodes -> CommitAttempt \cup {None}]  Indeterminate awaiting
                \* read-back; an attempt records the part AND the inc it was
                \* made under
  \* -- membership (advisory) ---------------------------------------------
  claims, hb,   \* registry rows: coverage claims, heartbeats. Advisory only:
                \* no invariant may read them as authority (see §5)
  inc,          \* [Nodes -> Nat]  fencing incarnation (highest minted, per node)
  alive,        \* [Nodes -> BOOLEAN]
  degraded,     \* SUBSET Nodes    booted without the catalog (DegradedBoot):
                \* replicate-and-serve only, no ownership actions
  wiped,        \* SUBSET Nodes    permanently lost disks, |wiped| <= WipeBudget
  \* -- schema -------------------------------------------------------------
  schema,       \* [Nodes -> [Datasets -> LatticeElem]]  each node's applied
                \* logical schema - per-node, so fail-closed-on-unknown-columns
                \* is representable (a receiver can genuinely not know a column)
  staleApplied  \* SUBSET Effect   ledger of effects admitted bearing an inc
                \* lower than the highest the ACCEPTOR had seen for the sender
                \* (or, for commits, lower than the catalog's minted highest).
                \* Empty in every honest configuration; FencedZombie's yardstick

Rec == [req : Requests, part : Partitions, origin : Nodes, seq : Nat,
        window : Windows]                      \* window = WinOf(req), fixed at stage
Key(r)  == <<r.part, r.origin, r.seq>>
KeyOf(q)    == Key(recOf[q])                   \* via the history ledger
WindowOf(q) == recOf[q].window
Part    == [part : Partitions, window : Windows,
            kind : {"window","supplement","snapshot"},
            disc : Disc,       \* discriminator: "-" for kind "window" (fixed, so
                               \* at most one window part); the per-origin seq
                               \* range for a supplement; snapshot_as_of_seq for
                               \* a snapshot - the commit fence keys on it (§6)
            coverage : SUBSET (Partitions \X Nodes \X Nat),
            extent : SUBSET (Partitions \X Nodes \X Nat),
                               \* fixed at seal: the window's full receipted
                               \* per-origin extent as attested by receipts and
                               \* the sealer's own rows - NewWatermark's input
            sealer : Nodes, inc : Nat,   \* who sealed it, under which incarnation
            dedupRemoved : Nat]
```

Three shapes matter. First, `staged` is the staging class and `cache`
the cache class of §2 — distinct variables because their obligations are
opposite (staging is never evicted; cache always may be). Second, the
overload measure is a *definition*, not a variable:

```tla
M(n)     == Cardinality({r \in staged[n] : TRUE})    \* staged bytes; cache-blind
Rung(n)  == IF M(n) >= HardLim     THEN 3            \* refuse
            ELSE IF M(n) >= ThrottleLim THEN 2       \* throttle
            ELSE IF M(n) >= SoftLim THEN 1           \* disclose
            ELSE 0
```

`M` reads `staged` alone — the cache class is invisible to it by
construction, half of LadderMonotone before any action fires. Third,
`claims` and `hb` are advisory: routing reads them (§7), but no safety
invariant quantifies over them — the registry is reconstructible soft
state (§5, §9), and an invariant resting on it would verify a fiction.

### 3.3 The action set

Each action: formal core plus a short rationale. Guards elided as `...`
are spelled in the modules; nothing load-bearing is elided — the two
definitions the most rests on, `NewWatermark` and `IsCatchup`, are
spelled in full below, and the remaining elisions are simple selectors
(`HoldsClaim`, `CommittedDurably`, `Landed`) whose one-line meanings the
prose states where they appear.

#### Ingest: Accept → DedupCheck → StageCommit → ClientAck

```tla
Accept(n, q) ==
  /\ alive[n] /\ resolved[q] = "unsent" /\ Rung(n) < 2   \* no new accepts at
                                                          \* rung 2 or above
  /\ inflight'  = [inflight EXCEPT ![n] = @ \cup {q}]
  /\ resolved'  = [resolved EXCEPT ![q] = "pending"]
```
Admission into volatile memory only; nothing about `Accept` is a promise.
Any node accepts any request — the data path is coordinator-free (§5).

```tla
DKey(q) == <<TenantOf(q), Hash(q)>>   \* tenant is in the key: two tenants may
                                      \* legally send byte-identical bodies, and
                                      \* a collision is therefore always
                                      \* tenant-scoped - cross-tenant collisions
                                      \* do not exist in any configuration
AtRF(e) ==   \* the entry's staged original now has >= RF total-inclusive copies
  LET r == recOf[e.orig]
  IN  /\ r # None
      /\ Cardinality({r.origin} \cup {rc.holder :
           rc \in {rc \in receipts : rc.key = Key(r)}}) >= RF
DedupCheck(n, q) ==
  /\ alive[n] /\ q \in inflight[n]
  /\ \E e \in dedup[n] :
       /\ e.key = DKey(q)
       /\ IF e.acked \/ AtRF(e)
          THEN \* replay the original's success, WITH its evidence: the ledger
               \* and the record linkage are copied, never re-derived
               /\ resolved'    = [resolved EXCEPT ![q] = "acked"]
               /\ ackEvidence' = [ackEvidence EXCEPT ![q] =
                                    IF e.acked THEN ackEvidence[e.orig]
                                    ELSE AckSetOf(e)]  \* computed exactly as
                                                       \* ClientAck computes H
               /\ recOf'       = [recOf EXCEPT ![q] = recOf[e.orig]]
               /\ dedup'       = MarkAcked(dedup, n, DKey(q))
          ELSE /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
               /\ UNCHANGED <<ackEvidence, recOf, dedup>>   \* pre-RF dup:
                                                            \* retryable, never a wait
  /\ inflight' = [inflight EXCEPT ![n] = @ \ {q}]
```
A duplicate of a fully-acked entry returns the original outcome *and*
inherits its evidence — DurableAck and NoAckedLoss hold for replayed
acks by the same ledger entries as for the original. The `AtRF(e)`
disjunct is the stage-then-throttled cure (§4.4.1): an entry whose
request was staged and then resolved retryable is **replayable-as-acked**
the moment its receipts reach RF — the data is durable and will drain,
so replaying success is honest, and the entry is never poisoned. A
duplicate arriving before RF gets a retryable signal — the client in
that window is by definition a retrying client that already handles
retry signals (§4).

```tla
StageCommit(n, q) ==   \* ONE local DuckDB transaction, atomic + fsynced (A1)
  /\ alive[n] /\ q \in inflight[n] /\ ~\E e \in dedup[n] : e.key = DKey(q)
  /\ LET p == PartOf(q)
         r == [req |-> q, part |-> p, origin |-> n, seq |-> nextSeq[n][p],
               window |-> WinOf[q]]
     IN /\ staged'  = [staged  EXCEPT ![n] = @ \cup {r}]
        /\ recOf'   = [recOf   EXCEPT ![q] = r]     \* the history ledger
        /\ dedup'   = [dedup   EXCEPT ![n] = @ \cup {[key |-> DKey(q),
                                                      acked |-> FALSE,
                                                      orig |-> q]}]
        /\ nextSeq' = [nextSeq EXCEPT ![n][p] = @ + 1]
        /\ inflight'= [inflight EXCEPT ![n] = @ \ {q}]
```
The row, the dedup entry, and the sequence bump land in one atomic
fsynced transition — so recovery can never observe a row without its
dedup entry (the replay-safety crux of §4) and per-origin sequences are
gapless at birth (1-based: `nextSeq` initializes to 1).

```tla
ClientAck(n, q) ==
  /\ alive[n] /\ resolved[q] = "pending"
  /\ LET r == TheRec(n, q)
         H == {n} \cup {rc.holder : rc \in {rc \in receipts : rc.key = Key(r)}}
     IN /\ r \in staged[n]
        /\ Cardinality(H) >= RF          \* <- this IS pillar 1
        /\ resolved'    = [resolved EXCEPT ![q] = "acked"]
        /\ ackEvidence' = [ackEvidence EXCEPT ![q] = H]
        /\ dedup'       = MarkAcked(dedup, n, DKey(q))
```
`ClientAck` is enabled only when the origin durably holds the record
*and* holds receipts putting total durable copies at ≥ RF. Two honesty
notes, stated rather than elided. First, `H` counts receipt **history**:
a receipt from a since-wiped peer still counts, and soundness leans on
the fault budget (`WipeBudget ≤ RF − 1` keeps at least one counted
holder alive), not on `H` being a live-copy census. Second, the durable
`MarkAcked` write and the client-visible response are bundled into one
action as a stated modeling assumption (the A1 style): the crash between
them leaves the entry unacked and the client unanswered — and the
client's retry then lands on `DedupCheck`'s `AtRF` branch and replays
success, so the collapse hides no loss mode. The ack is the product's
founding promise; everything in §3.4 exists to keep this one action
honest.

#### Overload and resolution: Throttle, Refuse, ClientTimeout

```tla
ReceiptWaitExpired(n, q) ==   \* the RF receipt wait timed out after the ring
  TheRec(n, q) \in staged[n]  \* walk-down exhausted substitutes; timeouts are
                              \* nondeterministic enablement in an async model (A4)

Throttle(n, q) == /\ alive[n] /\ resolved[q] \in {"unsent","pending"}
                  /\ Rung(n) = 2 \/ ReceiptWaitExpired(n, q)
                  /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
Refuse(n, q)   == /\ alive[n] /\ Rung(n) = 3 /\ resolved[q] = "unsent"
                  /\ resolved' = [resolved EXCEPT ![q] = "refused"]
ClientTimeout(q) ==           \* the client's own deadline, not a node action:
  /\ resolved[q] = "pending"  \* resolves a request left hanging by a dead or
                              \* silent acceptor as retryable
  /\ ~\E n \in Nodes : alive[n] /\ q \in inflight[n]
  /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
```
Throttle and Refuse share one wire form — UNAVAILABLE + RetryInfo (§4) —
and differ in what produces them, never in client-visible semantics:
Throttle resolves a request the node *admitted* (rung 2, or a
replication-receipt timeout on a staged request — the data is durable
and will drain, so the retryable signal is honest by right, and the
retry replays success once receipts complete via `DedupCheck`'s `AtRF`
branch), while Refuse turns away work never admitted, at the hard rung.
Neither is "terminal" in any sense the wire could express. `ClientTimeout`
is the third resolver: a crash after Accept wipes `inflight`, and without
the client's own deadline the request would hang forever — fail-closed is
a liveness discipline (EveryRequestResolves), and the timeout is the
client's, journaled by the verifying load generator in §8, not by any
node. Rung 1 (disclose) restricts nothing and therefore has no action;
rung 0 (cache eviction) is `Evict`, always enabled and not a status. The
ladder never drops acked data — the only lever it has is admission, which
is the whole point of driving it on staged bytes: staged is the one
quantity that cannot be reclaimed without violating NoAckedLoss (§4, §9).

#### Replication: Forward → PeerApply → Receipt

```tla
Forward(n, m, r) ==
  /\ alive[n] /\ r \in staged[n] /\ r.origin = n /\ m \in RingPeers(r.part, n)
  /\ chan' = chan \cup {[to |-> m, rec |-> r, inc |-> inc[n],
                         sAt |-> schema[n]]}  \* schema changes ride in-band (§4)

IsCatchup(g) ==   \* some receipt already stands for this record ANYWHERE: it
                  \* backs a promise already in flight, which the hard rung
                  \* honors; a record no receipt has ever covered is new load
  \E rc \in receipts : rc.key = Key(g.rec)

PeerApply(m, g) ==
  /\ alive[m] /\ g \in chan /\ g.to = m
  /\ g.inc >= highestSeen[m][g.rec.origin]          \* fencing: highest-seen-per-
                                                    \* receiver (§5.7) - the fence
                                                    \* a receiver can actually hold
  /\ g.rec.seq = AppliedThru(m, g.rec.part, g.rec.origin) + 1
                                                    \* GAP REFUSAL: a peer never
                                                    \* applies a non-contiguous seq
  /\ SchemaKnown(m, g)                              \* fail closed on columns the
                                                    \* RECEIVER's schema[m] lacks
  /\ IsCatchup(g) \/ Rung(m) < 3                    \* at the hard rung, refuse NEW
                                                    \* ranges; receipted catch-up continues
  /\ staged'      = [staged EXCEPT ![m] = @ \cup {g.rec}]  \* one local txn (A1)
  /\ highestSeen' = [highestSeen EXCEPT ![m][g.rec.origin] =
                       Max(@, g.inc)]

Receipt(m, r) ==
  /\ alive[m] /\ r \in staged[m] /\ r.origin # m
  /\ receipts' = receipts \cup {[holder |-> m, key |-> Key(r), inc |-> inc[m]]}
```
Gap refusal makes per-(partition, origin) prefixes the unit of truth: a
peer's holdings are always contiguous, so "replicated through seq s" is
one number, coverage claims are cheap, and takeover needs no
reconciliation protocol (§5). The fence is deliberately the *receiver's*
knowledge, not the global `inc[]`: a peer may honestly apply a message
from an incarnation it has not yet seen superseded — that behavior is
real, explored, and harmless (the apply is idempotent, and commits are
fenced at the catalog, which mints incarnations and so does know the
highest). A receipt is issued only after the durable apply — it is a
claim about bytes on a peer's disk, never its memory.

#### Drain: SealPart → PutPart → LakeCommit (∘ WatermarkAdvance)

```tla
SealPart(n, p, w) ==
  /\ alive[n] /\ n \notin degraded /\ HoldsClaim(n, p) /\ WindowClosed(p, w)
  /\ LET cov == {Key(r) : r \in WindowRecs(n, p, w)}
         ext == cov \cup {k \in WindowKeys(p, w) :   \* receipted extent: every
                  \E rc \in receipts : rc.key = k}   \* key some receipt attests
     IN sealedParts' = sealedParts \cup
          {[part |-> p, window |-> w, kind |-> "window", disc |-> "-",
            coverage |-> cov, extent |-> ext,
            sealer |-> n, inc |-> inc[n],
            dedupRemoved |-> DrainDedupCount(n, p, w)]}
```
The seal fixes the part's exact (origin, seq) coverage, its
`dedupRemoved` count, **and the window's receipted extent** in the
manifest — the extent is what `NewWatermark` compares committed coverage
against, so a winner part that lacks a residue another replica holds can
never advance the watermark over that residue. Parts are tenant-pure,
retention-class-pure, and kind-pure by construction: `WindowRecs` draws
from exactly one class of each axis (§2, §6).

```tla
PutPart(n, pt) ==     \* atomic object appearance (A3); the object's only
                      \* LOGICAL put - byte-identical retries collapse into it
  /\ pt \in sealedParts /\ pt \notin objects
  /\ objects' = objects \cup {pt}
```

`LakeCommit` is the linchpin — one catalog transaction with the three-way
outcome of A2, and **WatermarkAdvance is inside it, atomically**. There
is no separate watermark action anywhere in the model or the product.
`NewWatermark` is the model's most load-bearing definition, so it is
spelled here, not elided — its coverage-completeness criterion is the
sealed extent:

```tla
NewWatermark(p, lk, ll) ==
  LET Committed(w) == UNION {x.coverage : x \in {x \in lk :
                        x.part = p /\ x.window = w}}
      Extent(w)    == UNION {x.extent : x \in {x \in lk :
                        x.part = p /\ x.window = w}}
      Done(w)      == /\ \E x \in lk : x.part = p /\ x.window = w
                      /\ \A k \in Extent(w) :
                           k \in Committed(w) \/ \E l \in ll : Covers(l, k)
  IN CHOOSE m \in 0..MaxWindow(p) :
       /\ \A w \in 1..m : Done(w)
       /\ (m = MaxWindow(p) \/ ~Done(m + 1))
```
The watermark advances exactly through the windows whose **committed
coverage equals their sealed receipted extent** (loss-ledgered ranges
excepted) — a winner commit with a supplement still pending does *not*
advance `wm` over its window; the supplement's own commit completes the
extent and advances it. The extent is fixed at seal from receipts and
the sealer's rows: a *definition* may consult the receipt ledger (it is
ground truth — §3.2's advisory ban covers `claims`/`hb`, and the
invariant-side rule bars guards' samples from being yardsticks, not
ledgers from being read).

```tla
LakeCommitOk(n, pt) ==
  /\ alive[n] /\ pt \in objects /\ pendingCommit[n] = None
  /\ pt.inc = inc[pt.sealer]          \* the catalog minted every incarnation and
                                      \* rejects a commit under a superseded one
  /\ ~\E x \in lake : /\ x.part = pt.part /\ x.window = pt.window
                      /\ x.kind = pt.kind /\ x.disc = pt.disc
                      \* UNIQUE(partition, window, kind, discriminator):
                      \* kind "window" has the fixed disc "-", so at most one;
                      \* supplements and snapshots key on their discriminator
  /\ pt.kind = "supplement" =>
       \A x \in lake : SameWindow(x, pt) => x.coverage \cap pt.coverage = {}
                                    \* supplements PROVE pairwise-disjoint
                                    \* (origin,seq) coverage against EVERY prior
                                    \* part, validated inside this same txn
  /\ lake' = lake \cup {pt}
  /\ wm'   = [wm EXCEPT ![pt.part] = NewWatermark(pt.part, lake', lossLedger)]
                                    \* WatermarkAdvance: same atomic commit

LakeCommitAbort(n, pt) ==           \* conflict or refusal: candidate dropped,
  /\ alive[n] /\ ...                \* window remains staged; drain retries.
  /\ UNCHANGED <<lake, wm>>         \* Never a loss - staging never left.

\* "Connection died mid-commit, outcome unknown" (A2) has TWO successors -
\* one where the transaction in fact landed, one where it did not. The node
\* cannot distinguish them; the model must represent both or Reconcile's
\* adopt branch is dead code:
LakeCommitIndeterminateLanded(n, pt) ==
  /\ alive[n] /\ pt \in objects /\ pendingCommit[n] = None
  /\ CommitGuardsHold(n, pt)          \* the same guards as LakeCommitOk
  /\ lake' = lake \cup {pt}           \* the txn DID commit - lake and wm
  /\ wm'   = [wm EXCEPT ![pt.part] = NewWatermark(pt.part, lake', lossLedger)]
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = Attempt(pt, inc[n])]
                                      \* ...but the node only knows "unknown"
LakeCommitIndeterminateLost(n, pt) ==
  /\ alive[n] /\ pt \in objects /\ pendingCommit[n] = None
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = Attempt(pt, inc[n])]
  /\ UNCHANGED <<lake, wm>>           \* the txn did NOT commit

Reconcile(n) ==                     \* EXACTLY ONE read-back before any retry
  /\ alive[n] /\ pendingCommit[n] # None
  /\ IF Landed(pendingCommit[n], lake)
     THEN RecordAsCommitted(n)      \* the write landed: adopt it
     ELSE ClearForRetry(n)          \* it did not: a fresh attempt may begin
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = None]
```
Committing the part and advancing the watermark in one transaction is
what makes WatermarkHonesty checkable: no interleaving exists in which
the lake holds the part but the watermark lies, or vice versa. The
UNIQUE guard is the entire double-drain defense — cheaper and stronger
than any leadership protocol, because it fences at the only place a
drain becomes real (§5, §6). The implementation cannot know which
Indeterminate successor it took; it journals one `LakeCommitIndeterminate`
event and the following `Reconcile` names the outcome (§3.7).

#### Retention: Expire

```tla
Expire(pt) ==            \* the object's second and last storage operation
  /\ pt \in lake /\ RetentionElapsed(pt)
  /\ (IsChangelogData(pt) /\ pt.kind # "snapshot") =>
       \E s \in lake : /\ s.kind = "snapshot" /\ s.part = pt.part
                       /\ CoversArrival(s, pt)   \* Keep Rule 10's guard: a
                                                 \* changelog part expires only
                                                 \* under a covering snapshot
  /\ lake'    = lake \ {pt}
  /\ objects' = objects \ {pt}
  /\ expired' = expired \cup {pt}    \* history ledger: sanctioned removal is
                                     \* recorded, never silent
```
Retention's whole-file DELETE is a destructive protocol step, so it is
an action, not an operational footnote: `SnapshotCovered` (§3.4) is Keep
Rule 10 as ground truth, and the `ExpireUncovered` broken variant (§3.6)
keeps the guard honest. `InLake` in the invariants reads
`lake ∪ expired` — an acked record retired by sanctioned retention was
in the lake when the watermark claimed it, and the ledger proves it. In
the checked scope snapshots themselves are keep-forever (a snapshot
expires only under a newer covering snapshot, which the small
configuration never seals).

#### Post-drain residency: Demote, Evict, DropWindow

```tla
Demote(n, p, w) ==                    \* staging -> cache, in place
  /\ alive[n]
  /\ CommittedDurably(p, w)           \* the lake commit + watermark txn is durable
  /\ DedupRemovedOf(p, w) = 0         \* ONLY then is the hot table row-identical
  /\ {Key(r) : r \in WindowRecs(n, p, w)} = CommittedCoverage(p, w)
                                      \* only the node whose rows ARE the
                                      \* committed part may demote (§2.4: "a
                                      \* table this node itself staged and
                                      \* drained"); a replica holding a partial
                                      \* receipted prefix must DropWindow instead
  /\ cache'  = [cache  EXCEPT ![n] = @ \cup {WinTblOf(n, p, w)}]
  /\ staged' = [staged EXCEPT ![n] = @ \ WindowRecs(n, p, w)]

DropWindow(n, p, w) ==                \* the default exit from staging
  /\ CommittedDurably(p, w)
  /\ staged' = [staged EXCEPT ![n] = @ \ WindowRecs(n, p, w)]

Evict(n, t) ==                        \* cache only; ALWAYS enabled
  /\ t \in cache[n]
  /\ cache' = [cache EXCEPT ![n] = @ \ {t}]
```
`Demote` fires only when the drain removed zero rows as duplicates *and*
this node's own window rows are exactly the committed coverage — then
and only then does substituting the hot table for the lake part preserve
`complete`-read answers unconditionally (the Demote-safety lemma's
discharge). `Evict` has no guard beyond membership: eviction is always
safe because it only shrinks the cache class the lemma quantifies over.

#### Changelog: SnapshotSeal

```tla
SnapshotSeal(n, p) ==
  /\ alive[n] /\ n \notin degraded
  /\ KindOf(p) = "changelog" /\ HoldsClaim(n, p)
  /\ LET s == LatestByKey(n, p)   \* full latest-state as-of arrival seq S;
     IN ...                       \* deleted keys absent; a NEW object -
                                  \* derivation, never rewrite (A3)
  \* fenced at commit by the same UNIQUE(partition, window, kind, disc) guard,
  \* with disc = snapshot_as_of_seq - its own key, not a vocabulary reuse of
  \* the window fence; serialized per partition under the drain scheduler (§6)
```
Snapshots make changelog retention append-only: parts wholly covered by
a snapshot become age-expirable whole files; uncovered parts are
keep-forever (§6, §11). The fence is defined on its own key — not a
vocabulary reuse of the window guard.

#### Membership and failure: ClaimAdvertise, Heartbeat, FenceBoot, DegradedBoot, TakeoverDrain, DeclareLoss

```tla
ClaimAdvertise(n, p) ==  \* advisory registry row: "I hold coverage for p"
  /\ ... /\ claims' = claims \cup {[node |-> n, part |-> p,
                                    thru |-> AppliedThru(n, p, n), inc |-> inc[n]]}
Heartbeat(n) == /\ alive[n] /\ hb' = [hb EXCEPT ![n] = @ + 1]

FenceBoot(n) ==          \* recovery entry point; incarnation from the catalog
  /\ ~alive[n] /\ n \notin wiped
  /\ catalogSeq' = catalogSeq + 1
  /\ inc'   = [inc EXCEPT ![n] = catalogSeq']
  /\ alive' = [alive EXCEPT ![n] = TRUE]
  /\ degraded' = degraded \ {n}
  \* recovery state = staged[n], replayed as-is: staging tables ARE the WAL.
  \* A wiped node never re-enters as itself: an empty disk re-provisions as a
  \* NEW node with a fresh identity; §9.4.2's rejoin-with-data path concerns a
  \* declared-dead node that was never wiped

DegradedBoot(n) ==       \* catalog down at boot, persisted incarnation (§5.7):
  /\ ~alive[n] /\ n \notin wiped /\ inc[n] > 0   \* has an identity to be
                                                 \* safely partial with
  /\ alive'    = [alive EXCEPT ![n] = TRUE]
  /\ degraded' = degraded \cup {n}
  /\ UNCHANGED <<catalogSeq, inc>>               \* no fresh incarnation minted
  \* replica-only: PeerApply, Receipt, Heartbeat, and serving run under the
  \* persisted incarnation; every ownership action (SealPart, SnapshotSeal,
  \* TakeoverDrain - and through them the commits) is guarded on
  \* n \notin degraded. FenceBoot, when the catalog returns, promotes n
```
Every message and registry row carries its sender's incarnation; every
receiver rejects one lower than the highest seen for that node (the
guards in `PeerApply`, `Receipt`, and the catalog's row versions). A
higher incarnation fences a lower one on every message — the converged
epoch-fencing pattern (Kafka's leader epochs), applied without a leader.
`DegradedBoot` is the deliberately narrow exception that keeps rolling
restarts from wedging on a catalog incident: a stale-incarnation node in
degraded mode can only apply and receipt — exactly the operations whose
idempotency and fencing make a stale participant harmless.

```tla
TakeoverDrain(m, p) ==
  /\ alive[m] /\ m \notin degraded
  /\ ~alive[Owner(p)] /\ HeartbeatStale(Owner(p)) /\ SuppressionExpired(Owner(p))
  /\ CoverageAt(m, p) >= MaxAdvertised(p)   \* elects on ADVERTISED (claims)
        \* coverage - what a real election can read. A stale registry may elect
        \* a less-covered replica; safety then rests on SingleDrainCommit and
        \* the extent-complete NewWatermark, never on election quality
  \* not a distinct state change: enabling TakeoverDrain enables SealPart(m, p,
  \* w) over m's OWN replicated rows - the effect is the ordinary drain pipeline
```
Takeover is just a drain performed by a replica — no special commit
path, no election protocol. If the presumed-dead owner was merely slow,
both drains race to `LakeCommitOk` and the UNIQUE guard lets exactly one
win; the loser aborts harmlessly. SingleDrainCommit, not failure
detection, carries the safety (§5).

```tla
DeclareLoss(p, rng) ==   \* OPERATOR action - never autonomous (§9's ceremony)
  /\ ~\E n \in Nodes \ wiped : Advertises(n, p, rng)  \* refused while any live
                                                       \* replica claims coverage
  /\ lossLedger' = lossLedger \cup {[part |-> p, range |-> rng,
        liveAtDecl |-> \E n \in Nodes \ wiped :        \* history flag: was the
             alive[n] /\ HoldsCoverage(n, p, rng)]}    \* confession false?
  /\ wm' = [wm EXCEPT ![p] = AdvancePast(rng)]
  \* ledger row and watermark advance: ONE catalog transaction (A2)
```
The watermark may advance past a hole *only* here, and the permanent
ledger row rides the same transaction — degraded availability disclosed,
never disguised (pillar 4). Within the RF − 1 budget this action is
unreachable for acked records; §3.6 witnesses both facts.

#### Schema: EvolveSchema

```tla
EvolveSchema(n, d, s) ==   \* a schema change IS a sequenced record: it consumes
                           \* a seq and rides the same log as data (§4, §5)
  /\ alive[n] /\ s = LatticeJoin(schema[n][d], s)   \* monotone: join, never a rewrite
  /\ schema'  = [schema  EXCEPT ![n][d] = s]
  /\ LET p == HomePartition(d)
     IN /\ staged'  = [staged  EXCEPT ![n] = @ \cup
                        {SchemaRec(n, d, s, nextSeq[n][p])}]
        /\ nextSeq' = [nextSeq EXCEPT ![n][p] = @ + 1]
  \* peers receive the schema record through Forward/PeerApply like any other
  \* record, in the total order gap refusal already provides; applying it joins
  \* s into schema[m]. SchemaKnown(m, g) compares g.sAt against the RECEIVER's
  \* schema[m] and fails closed on columns m has not yet learned - so
  \* widen-to-origin-schema always precedes the data that needs it
```
The join is commutative and idempotent, so crash-retry and concurrent
evolution converge without coordination; in-band ordering means a peer
never applies a record whose columns it has not learned —
`PeerApply`'s `SchemaKnown` guard fails closed (§4). Because `schema` is
per-node state, "a receiver that does not yet know a column" is a
representable configuration, not a comment — `Witness_SchemaWidensInFlight`
(§3.6) reaches it.

#### Crash and recovery: CrashNode, RecoverNode

```tla
CrashNode(n) ==          \* enabled at ANY interleaving point - no guard but life
  /\ alive[n]
  /\ alive'    = [alive EXCEPT ![n] = FALSE]
  /\ inflight' = [inflight EXCEPT ![n] = {}]   \* volatile state gone
  /\ UNCHANGED staged                          \* fsynced state survives (A1)

CrashWipe(n) ==          \* the disk dies too - bounded by the fault budget.
                         \* No liveness guard: a disk can die under a node
                         \* that already crashed
  /\ Cardinality(wiped \cup {n}) <= WipeBudget
  /\ wiped'  = wiped \cup {n}
  /\ staged' = [staged EXCEPT ![n] = {}] /\ cache' = [cache EXCEPT ![n] = {}]
  /\ ...

RecoverNode(n) == FenceBoot(n)   \* recovery replays from staging tables;
                                 \* there is no other recovery input
                                 \* (DegradedBoot is the catalog-down entry,
                                 \* not a second recovery input: it replays
                                 \* the same staging tables)
```
`CrashNode` interleaves everywhere — between a stage and its forward,
between a PUT and its commit, between a commit and its demotion. That
last window is why `Demote` re-checks catalog durability on recovery:
crash-between-commit-and-demotion is a checked behavior, not a comment.

### 3.4 Invariants

Checked over every reachable state of every module configuration — and,
for LadderMonotone's behavioral conjunct, over every transition. Each is
stated as a formula over ground truth, with its prose meaning. Ten
state invariants plus the ladder action property; the history ledgers (`recOf`,
`expired`, `liveAtDecl`, `staleApplied`) exist so each stays evaluable
after the state it judges has legitimately moved on.

**DurableAck** — an ack is a claim about RF durable copies.
```tla
DurableAck ==
  \A q \in Acked :
    /\ Cardinality(ackEvidence[q]) >= RF
    /\ \A m \in ackEvidence[q] :
         m = OriginOf(q) \/ \E rc \in receipts : rc.holder = m /\ rc.key = KeyOf(q)
```
Every acked request's ledgered evidence names at least RF nodes, each of
which either is the origin (which staged durably before acking) or had
issued a durable-apply receipt. This is pillar 1 as a formula.

**NoAckedLoss** — acked data survives any fault schedule within budget.
```tla
InLake(k) == \E x \in lake \cup expired : k \in x.coverage
             \* expired = retired by sanctioned retention, ledgered (Expire)
NoAckedLoss ==
  \A q \in Acked :
    InLake(KeyOf(q)) \/ \E n \in Nodes \ wiped :
                          \E r \in staged[n] : Key(r) = KeyOf(q)
```
Checked across every schedule of `CrashNode`/`CrashWipe`/`RecoverNode`
with at most RF − 1 permanent losses: every acked record remains
reachable — in a surviving node's staging or committed to the lake. The
quantifier runs over `KeyOf` — the history ledger — so it stays
evaluable for a replayed ack and for a record a `DropWindow` has since
removed from every staging table. Together with `DeclareLoss`'s
live-replica guard, this also proves the loss ceremony cannot touch an
acked record inside the budget.

**WatermarkHonesty** — `complete_through` never lies.
```tla
WatermarkHonesty ==
  \A p \in Partitions : \A q \in Acked :
    (PartOf(q) = p /\ WindowOf(q) <= wm[p]) =>
      InLake(KeyOf(q)) \/ \E l \in lossLedger : Covers(l, KeyOf(q))
```
If the watermark says a cell is complete, every acked record for that
cell is in the lake **or declared lost in the ledger — never silently
missing**. This is the formula a `complete` read (§7) rests on: the
fail-closed default is only as good as this invariant.

**CacheTransparency** — the Demote-safety lemma.
```tla
CacheTransparency ==
  \A n \in Nodes : \A t \in cache[n] : Rows(t) = LakeRowsOf(t)
```
Every cache-class table is row-identical to its committed part —
discharged by `Demote`'s `dedupRemoved = 0` and coverage-identity
guards (a `WinTbl` carries its row set, so `Rows(t)` is a field read).
This formula is deliberately a **lemma**, not §2.4's full theorem: the
theorem quantifies over every `complete` read's *answer*, and §3 has no
read action. The theorem is discharged in three parts — this row-identity
lemma, the one-side-serving tier rule (§7.2, under which v1's read path
never consults the cache class at all), and §8.4's eviction-storm judge,
which checks the read-answer equivalence mechanically. `Evict` only
removes tables from this lemma's quantifier domain and can never violate
row-identity; eviction interleavings stress the read-path equivalence,
which is §8.4's job, not this formula's.

**GapFreedom** — per-(partition, origin) holdings tile a contiguous prefix.
```tla
DrainedSeqs(p, o) == {s \in Nat : \E x \in lake \cup expired :
                        <<p, o, s>> \in x.coverage}
GapFreedom ==
  \A n \in Nodes, p \in Partitions, o \in Nodes :
    LET S == {r.seq : r \in {r \in staged[n] : r.part = p /\ r.origin = o}}
        D == DrainedSeqs(p, o)
    IN  S \cup D = 1..Cardinality(S \cup D)
```
The direct consequence of `PeerApply`'s gap refusal plus `StageCommit`'s
atomic 1-based sequence assignment (`nextSeq` initializes to 1, pinned
in Init). The union with drained coverage is what makes the invariant
survive the drain: after `DropWindow` removes window 1's records, the
staged residue alone is no prefix — staged ∪ committed still is, and
that is exactly the property §7.5's hot∪cold tiling rests on. Everything
cheap about DuckSpout's replication — one-number coverage,
reconciliation-free takeover, supplement-disjointness proofs — depends
on this.

**SingleDrainCommit** — at most one committed part per fence key; supplements disjoint.
```tla
SingleDrainCommit ==
  /\ \A a, b \in lake :
       (a.part = b.part /\ a.window = b.window /\ a.kind = b.kind
        /\ a.disc = b.disc) => a = b
  /\ \A a \in lake : a.kind = "window" =>
       \A b \in lake : (SameWindow(a, b) /\ b.kind = "window") => a = b
  /\ \A s \in lake : s.kind = "supplement" =>
       \A x \in lake : (SameWindow(x, s) /\ x # s) => x.coverage \cap s.coverage = {}
```
The UNIQUE constraint over (partition, window_id, part_kind,
discriminator) — kind `window` carries the fixed discriminator, so at
most one window part exists; supplements may be several (a second
takeover residue, a post-DeclareLoss resurrection — §9.4.2) provided
each proves pairwise-disjoint coverage against every prior part of the
window inside its own commit. This is what makes a zombie's or a racing
replica's second drain an abort instead of a double-count.

**FencedZombie** — no effect lands under a fence its acceptor held.
```tla
FencedZombie == staleApplied = {}
```
`staleApplied` is a ledger populated by any acceptance path that admits
a message bearing an incarnation lower than the highest that acceptor
had *seen* for the sender — or, for commits, lower than the catalog's
minted highest (the catalog, as the mint, knows it). In the honest
configuration every acceptance guard checks exactly that fence, so the
ledger stays empty by construction; the `UnfencedZombie` variant removes
one guard and the ledger fills. An apply from an incarnation the
receiver has not yet seen superseded is *not* stale — it is a real,
explored, harmless behavior (§3.3, PeerApply).

**LossLedgerTruthful** — a confession is never false.
```tla
LossLedgerTruthful == \A l \in lossLedger : ~l.liveAtDecl
```
No loss row was ever declared while a live, un-wiped node held coverage
of its range — `DeclareLoss`'s no-live-coverage guard as ground truth,
via the `liveAtDecl` history flag. Without this invariant, dropping that
guard would violate nothing: the watermark still tells the ledgered
truth (WatermarkHonesty) and the live replica still holds its rows
(NoAckedLoss) — the harm of a false confession is exactly that a
`complete` read skips live data, and this is its formula.

**SnapshotCovered** — Keep Rule 10 as ground truth.
```tla
SnapshotCovered ==
  \A e \in expired : (IsChangelogData(e) /\ e.kind # "snapshot") =>
    \E s \in lake : s.kind = "snapshot" /\ s.part = e.part /\ CoversArrival(s, e)
```
Nothing retention expired ever held the last value of a key: every
expired changelog part has a committed covering snapshot. `Expire`'s
guard is the same formula in guard position; `ExpireUncovered` (§3.6)
perturbs the guard and this yardstick catches it.

**LatestViewCorrect** — the served latest view is the fold of the acked changelog.
```tla
LatestViewCorrect ==
  \A p \in ChangelogPartitions(Partitions) :
    LatestFold(SnapshotRows(p) (+) ChangelogSince(p))
      = LatestFold(AllCommittedAndStaged(p))
```
For every key, newest-snapshot-plus-changelog-since — the read plan of
`<dataset>_latest` (§7.7) — equals the fold of every committed and
staged record for the partition in (origin, seq) order, tombstones
deleting. `SnapshotSeal`'s elided `LatestByKey` content is exactly what
this invariant pins: a snapshot that dropped or resurrected a key
violates it. §8.4's changelog judge is this invariant, judged
end-to-end.

**LadderMonotone** — restriction only tightens as M rises; the cache is invisible.
```tla
Allowed(k) ==   \* the client-visible operations permitted at rung k
  CASE k = 0 -> {"accept", "ack", "replicate-new", "catch-up"}
    [] k = 1 -> {"accept", "ack", "replicate-new", "catch-up"}   \* disclose only
    [] k = 2 -> {"ack", "replicate-new", "catch-up"}   \* throttle: no new accepts
    [] k = 3 -> {"ack", "catch-up"}                    \* refuse + no new ranges

LadderMonotone ==   \* an ACTION property: every step taken is permitted at the
                    \* rung (pre-state) of the node that took it
  /\ \A j, k \in 0..3 : j <= k => Allowed(k) \subseteq Allowed(j)
  /\ [][ \A n \in Nodes, q \in Requests :
           /\ Accept(n, q)    => "accept" \in Allowed(Rung(n))
           /\ ClientAck(n, q) => "ack"    \in Allowed(Rung(n))
         /\ \A m \in Nodes : \A g \in chan :
              PeerApply(m, g) =>
                (IF IsCatchup(g) THEN "catch-up" ELSE "replicate-new")
                   \in Allowed(Rung(m)) ]_vars
```
The first conjunct is the static sanity of the table (antitone in the
rung); the second is the behavioral claim, and it is deliberately an
action property — a state predicate here would either restate `Rung`'s
own definition (a tautology no perturbation could falsify) or quantify
over nothing an action reads. Perturb `Accept`'s guard to admit at rung
≥ 2 and this property, not a definition, produces the counterexample.
`Rung` itself is a pure function of `M`, which reads staging alone —
`Evict` cannot change it, and `Demote` changes it only downward by
shrinking `staged`, which is the intended relief direction. In-flight
acks complete at every rung: the ladder gates admission, never promises
already made.

### 3.5 Liveness, fairness, and honest findings

Safety without liveness lets a node satisfy every invariant by doing
nothing. Two liveness properties are checked under explicit fairness:
weak fairness on `StageCommit`, `Forward`, `PeerApply`, `Receipt`,
`ClientAck`, `ClientTimeout`, `Throttle`, `Refuse`, `SealPart`,
`PutPart`, `LakeCommitOk`, `TakeoverDrain`, and `Reconcile`, plus the
assumption that a message resent forever is eventually applied (A4
tempered by retry, as the implementation behaves). `TakeoverDrain`'s
fairness is load-bearing: without it, WatermarkEventuallyAdvances fails
at the model's own hands for any partition whose owner died —
a takeover that may forever not happen is no availability story.

**EveryRequestResolves** — never silence.
```tla
EveryRequestResolves ==
  \A q \in Requests :
    (resolved[q] = "pending") ~> (resolved[q] \in {"acked", "throttled", "refused"})
```
Every accepted request terminates in an ack, a retryable throttle, or a
refusal. Fail-closed is a liveness discipline as much as a safety one: a
client left hanging is an undisclosed failure.

**WatermarkEventuallyAdvances** — completeness is not vacuous.
```tla
WatermarkEventuallyAdvances ==
  \A p \in Partitions :
    (DrainEnabled /\ LakeAccepts /\ AckedBehindWm(p)) ~> WmAdvanced(p)
```
When drains are enabled and the lake accepts commits, every partition
with acked data behind its watermark eventually advances it. The
antecedent is honest: during a catalog outage the watermark does *not*
advance — and that is a disclosed pause (§9), not a liveness bug.

**The honest-findings convention.** Properties DuckSpout deliberately
does *not* have are kept in the suite as permanently-failing FINDINGS —
checked on every run, required to fail. **This table is the single
authoritative FINDINGS set — five members, exactly; §8.1 runs it and
cross-references it here:**

| Finding (must fail) | What its failure documents |
|---|---|
| `Finding_BoundedAckLatency` | "Every pending request acks within B steps" is false: DuckSpout sets no ack-latency bound under contention. Throttle is the pressure valve, not a deadline. |
| `Finding_WatermarkThroughCatalogOutage` | The watermark does not advance while the catalog is down (WatermarkEventuallyAdvances without the catalog-recovers fairness assumption). Drains pause and say so (§9); no timer ever escalates a catalog outage into data movement. |
| `Finding_PerOriginFairness` | One origin can be throttled indefinitely while others progress; no cross-origin fairness is promised in v1. |
| `Finding_BoundedThrottleDuration` | No upper bound exists on how long a client is throttled while staging is full and drains are stalled. The alternative is shedding acked-adjacent data, which NoAckedLoss forbids. |
| `Finding_RefuseFreeBelowRF` | Below the replication floor, ingest does not eventually accept: refuse-only is the design (§5.1); "ingest always eventually accepts" is false on purpose. |

A finding that goes green fails CI **on purpose**: either the model
drifted from the protocol or the protocol silently acquired a guarantee
nobody committed to documenting — both demand a human decision, not a
quiet pass.

### 3.6 The teeth: broken variants and non-vacuity witnesses

An invariant that never produced a counterexample proves either that the
design is sound or that the model cannot represent the bug — TLC cannot
tell you which. So every checked **safety invariant** ships a
**permanently-armed, deliberately-broken variant**: a configuration
perturbing exactly one clause of one action's *guard* (never the
invariant's yardstick) that MUST produce a counterexample on every CI
run. Liveness is armed the same way through `SuppressionNeverExpires`
(and the FINDINGS above, which are permanently-red liveness checks);
Schema.tla's lattice laws are armed in the property-test tier (§8.5). A
model whose broken variant stops failing is a model that stopped
checking; CI fails closed on it.

| Broken variant (armed `.cfg`) | The one perturbed clause | Property that must catch it |
|---|---|---|
| `AckBeforeReceipt` | `ClientAck` drops the ≥ RF receipt conjunct | DurableAck; NoAckedLoss under one wipe |
| `DrainWithoutWatermark` | `LakeCommitOk` no longer advances `wm`; a separate, unguarded advance action exists | WatermarkHonesty — the freestanding advance fires ahead of the commit it should have ridden; no crash is needed (with the honest commit-then-advance coupling, `wm` can only ever *lag*, which is the safe direction) |
| `EvictStaging` | `Evict` enabled on staging-class tables | NoAckedLoss (DurableAck cannot catch it: its evidence ledgers never shrink) |
| `UnfencedZombie` | `PeerApply`/`LakeCommitOk` accept an incarnation below the acceptor's fence | FencedZombie (alone — the intact UNIQUE guard aborts a zombie drain before SingleDrainCommit could see it) |
| `WatermarkPastHole` | `NewWatermark` may pass an uncovered range with no `lossLedger` row | WatermarkHonesty |
| `GapAcceptingPeer` | `PeerApply` drops the contiguity conjunct | GapFreedom |
| `DemoteDirty` | `Demote` drops `dedupRemoved = 0` | CacheTransparency (reachable because the pinned config includes colliding DKeys, §3.1) |
| `DoubleDrain` | `LakeCommitOk` drops the UNIQUE conjunct | SingleDrainCommit (the config pins divergent coverage between the racing drains, §3.1, so the two parts differ and the `lake` set union cannot mask them) |
| `SupplementOverlap` | The supplement path skips the disjoint-coverage proof | SingleDrainCommit |
| `LossOverLiveReplica` | `DeclareLoss` drops the no-live-coverage guard | LossLedgerTruthful (a live replica's coverage falsely confessed away — invisible to NoAckedLoss, whose record never left the replica) |
| `ExpireUncovered` | `Expire` drops the covering-snapshot conjunct | SnapshotCovered |
| `LadderInversion` | `Accept`'s rung guard re-permits admission at rung ≥ 2 | LadderMonotone (the action property — the step itself is the counterexample) |
| `SuppressionNeverExpires` | `SuppressionExpired` pinned FALSE — takeover never fires for a "restarting" node that never returns (§5.10) | WatermarkEventuallyAdvances |

The clean configuration's state count is pinned exactly — silent drift
means the model's shape changed without the baseline moving. A broken
variant's count is asserted only nonzero (it halts at first
counterexample; its count is a race, not a property); the stable signal
is the violated invariant's *name*.

**Non-vacuity witnesses** prove the model genuinely reaches the states
its guards protect — reachability assertions, permanently armed. **This
table is the definitive armed witness set; §8.1 describes the tier and
cross-references it here:**

| Witness | What it proves is genuinely exercised |
|---|---|
| `Witness_TakeoverCommits` | A `TakeoverDrain` actually lands a dead owner's window in the lake — takeover is a reachable behavior, not a declared one. |
| `Witness_LossDeclared` | With the budget raised past RF − 1, `DeclareLoss` actually fires end-to-end: ledger row and watermark advance in one step. |
| `Witness_LossRefusedOverLiveReplica` | A `DeclareLoss` is refused because a live replica still advertises coverage — the ceremony's unreachability *within* budget, checked as its own reachable refusal. |
| `Witness_IndeterminateResolved` | The three-way commit's least trivial branch — `LakeCommitIndeterminateLanded` followed by `Reconcile` adopting the landed write — occurs. |
| `Witness_SupplementCommits` | A supplement part commits beside a winner with proven-disjoint coverage. |
| `Witness_SupplementPending` | The state between winner commit and supplement commit is reached: the residue is staged on a replica, receipted, and `wm` has **not** advanced over the window — `NewWatermark`'s extent criterion is doing work, not decoration. |
| `Witness_ReceiptOutstandingAtAck` | A `Forward`'s Receipt is outstanding at ClientAck-decision time — the RF wait is a real wait. |
| `Witness_ThrottleAndRefuseTaken` | `Throttle` and `Refuse` are each actually taken — the ladder's upper rungs are reachable behaviors. |
| `Witness_DedupReplayAcked` | A colliding retry replays the original's ack through `DedupCheck`, inheriting its evidence — the replay branch is live, not dead code. |
| `Witness_SchemaWidensInFlight` | An `EvolveSchema` lands mid-window and a catching-up peer applies widen-before-data. |
| `Witness_CrashBetweenCommitAndDemote` | The crash window between `LakeCommitOk` and `Demote` is reached and recovered through. |

Witnesses for the dormant cache class (`Witness_EvictDuringCompleteRead`)
are parked with the feature and arm the day its toggle flips.

### 3.7 Trace conformance: the model checks the code

The models above verify the design. Trace refinement closes the loop to
the implementation: each subsystem emits an execution trace whose event
names are the action names of §3.3, verbatim — `StageCommit`, `Receipt`,
`ClientAck`, `LakeCommitOk`, `FenceBoot` — with the arguments the
corresponding action takes. Three vocabulary rules keep the mapping
exact: a commit journals its outcome name (`LakeCommitOk`,
`LakeCommitAbort`, or `LakeCommitIndeterminate` — the implementation
cannot know which Indeterminate successor it took, so it journals the
one name and the following `Reconcile` names the outcome); `CrashNode`
and `CrashWipe` are environment events, not journaled (a crashed node
cannot journal its own crash) — the trace checker treats them as
unobserved environment steps; and `ClientTimeout` is journaled by the
verifying load generator (§8.4), which is a fleet member, not by any
node. For each module a `*Trace.tla` sibling constrains `Next` to the
recorded step sequence and checks two things:

1. **Every recorded run is a behavior of the model.** A run the model
   cannot take deadlocks at the first impossible step, and the deadlock
   names it — the implementation did something the specification
   forbids, at a specific event.
2. **Every required step was recorded** — the `TraceComplete` invariant.
   A subsystem that performs a modeled transition without emitting its
   event is as broken as one that performs a forbidden transition:
   silent steps are how implementations drift out from under their
   specifications.

Conformance runs are part of the standard suite, executed against runs
from the deterministic harness and from chaos schedules (kills at
arbitrary points, partitions, membership churn). The capture format, the
harness, and the CI wiring — including the rule that a conformance
failure blocks release exactly as a broken-variant regression does — are
specified in §8.

---

The rest of this document uses these names as defined here: §4's "ack
only after RF receipts" is `ClientAck`'s guard, §6's "seal, put, commit"
is `SealPart → PutPart → LakeCommitOk` with WatermarkAdvance inside the
commit, §9's loss ceremony is `DeclareLoss`. One vocabulary; this is it.

## §4 Ingest and Durability

This section specifies the write path from the first byte on the wire to the
`ClientAck` — the accept edge, the WAL=hot storage mechanics, the exact ack
sequence, duplicate semantics, the overload ladder, admission constants, and
the transform pipeline. The formal definitions of the actions named here
(`Accept`, `DedupCheck`, `StageCommit`, `Forward`, `PeerApply`, `Receipt`,
`ClientAck`, `Throttle`, `Refuse`) live in §3; replication mechanics
(`Forward`/`PeerApply`/`Receipt`, ring walk-down, takeover) are §5's subject
and appear here only where the ack path depends on them.

### 4.1 The accept path

#### 4.1.1 otel-collector is the edge, DuckSpout is the terminus

DuckSpout deliberately does not compete with the otel-collector on protocol
breadth or transform richness. The recommended topology is:

```
apps/agents → otel-collector (receivers, processors, batching,
              persistent queue) → OTLP/gRPC → DuckSpout accept node
```

The collector owns protocol fan-in (Prometheus scrape, Jaeger, Fluent,
syslog, StatsD, vendor receivers), edge sampling, attribute processors, and
batching. DuckSpout owns what the collector cannot provide: a durable,
replicated, queryable terminus with completeness semantics. This division
follows the ecosystem's own convergence — the collector is already deployed
at nearly every OTLP edge, and re-implementing its receiver catalog would be
duplicated effort with no durability payoff.

Because the collector's *default* retry/queue configuration drops data (its
documented behavior on queue-full and on retry-elapsed), the recommended
collector edge configuration is part of the DuckSpout product, not an
afterthought:

- `file_storage` extension enabled and bound to the exporter's
  `sending_queue` — the queue survives collector restarts on disk instead of
  vanishing from memory.
- `retry_on_failure.max_elapsed_time` raised well above its 300 s default,
  to outage scale (hours). DuckSpout's `Throttle` responses are explicit
  retry instructions (§4.5); a collector that gives up after five minutes
  converts a disclosed brown-out into silent loss at the edge.

DuckSpout's own dedup window TTL (§4.4) is derived from this recipe: the
window must dominate the retry horizon the recipe configures.

#### 4.1.2 Accept adapters are a trait; OTLP is the first implementation

The accept surface is a pluggable trait in the Rust core (see §10 for the
port catalog). An adapter's obligations are exactly three:

1. Decode the wire payload into typed record batches for a declared dataset
   (kind `event` or `changelog`, §2).
2. Extract tenant identity (`X-Scope-OrgID` from the mTLS-verified edge) and
   the optional `x-duckspout-idempotency-key` header.
3. Map DuckSpout's admission/overload outcomes (§4.5, §4.6) onto the
   protocol's native error vocabulary — for OTLP, the spec's own retryable
   status table, with no invented extensions.

The v1 adapter set is OTLP alone (gRPC and HTTP/protobuf), because the
collector-in-front topology makes every other protocol reachable through
translation. Non-OTLP adapters (Kafka-protocol accept, native Arrow Flight
`DoPut`, JSON/HTTP for changelog datasets) are post-v1 and must be
expressible through the same trait without touching the ack path — the trait
boundary exists precisely so that durability semantics are adapter-invariant.

OTLP `partial_success` is used only for permanently rejected malformed
items; it is never used to smuggle a partial durability outcome. A batch is
acked durable in its entirety or it is refused/throttled in its entirety.

### 4.2 WAL = hot: the durability primitive

#### 4.2.1 One store, not two

DuckSpout does not maintain a separate write-ahead log beside its hot store.
The hot store *is* the WAL: accepted records are inserted into persistent
DuckDB tables, and DuckDB's own fsync-on-commit WAL is the durability
primitive. When `StageCommit` returns, the bytes are on disk, crash-replay
guaranteed by the engine that will also serve queries over them (§7). This
collapses the classic ingest architecture (log → memtable → flush) into a
single transactional store and eliminates an entire class of log/store
divergence bugs — there is no second copy to reconcile after a crash.

The engineering caveat is acknowledged: DuckDB's fsync granularity
(per-commit vs. per-checkpoint) must be verified against the engine source
and pinned in the compatibility matrix before each supported DuckDB version
is certified; NoAckedLoss (§3) is only as strong as that fsync. Local NVMe
is the assumed substrate — fsync latency is the ack path's critical path,
and network volumes with high fsync cost degrade ack p99 directly (§8's
benchmark hardware disclosure exists for this reason).

#### 4.2.2 One table per micro-window

Each (tenant, shard) partition's staging-class data lives in one DuckDB
table per micro-window (`hot.window`, default 60 s; changelog datasets
close windows by the same rule on the arrival axis — `hot.window` (60 s)
of arrival time or `drain.part_target_bytes` of staged size, whichever
first — since they have no wall-clock alignment requirement).
Consequences, each load-bearing:

- **O(1) cleanup.** After a durable `LakeCommit` (§6), the window table is
  `DROP TABLE`d. No vacuum, no tombstone debt, no compaction of the hot
  store — deletion cost is independent of row count.
- **The drain is a sorted `COPY`.** Sealing a part (§6) reads whole tables,
  never row ranges, so the seal is a full sequential scan that doubles as a
  corruption scrub (§9).
- **The ladder's unit of accounting.** `staged_bytes` (§4.5) is a sum over
  live staging tables — cheap and exact.

#### 4.2.3 The hot table doubles as the replication log

Every row carries two system columns: `origin` (accepting node's
`(node_id, incarnation)`, §5) and `seq` (dense per-(partition, origin)
sequence). The staging table, ordered by `(origin, seq)`, *is* the
replication log — `Forward` ships `(origin, seq)` ranges, `PeerApply`
inserts them transactionally on the replica, catch-up after a partition is a
range query over data already durably held. There is no separate log
segment format, no log retention policy distinct from staging retention:
the log lives exactly as long as the window is undrained, which is exactly
as long as replication can need it.

Schema-change records ride the same log as sequenced in-band records, so a
replica applies widenings in the same total order as data (§2 defines the
type lattice; §5 defines gap refusal, which supplies the ordering
guarantee).

#### 4.2.4 Exactly-once apply: the applied-watermark row

Each replica maintains, in the same hot DuckDB, an applied-watermark table:
one row per (partition, origin) holding the highest contiguously applied
`seq`. `PeerApply` inserts the forwarded rows *and advances this row in the
same DuckDB transaction*. Replay after a crash, a duplicate `Forward`, or a
reconnect is therefore idempotent by construction: a range at or below the
applied watermark is acknowledged without re-insertion, a range beyond
watermark+1 is refused (gap refusal, §5), and the transactional coupling
means the watermark can never claim rows the crash discarded or miss rows
the commit kept. This is the standard consumer-offset-in-the-same-store
transplant (Kafka's transactional consumers, Flink's two-phase sinks
converge on the same shape) with DuckDB's transaction as the atomicity
provider. The accepting node uses the identical mechanism for its own
`StageCommit` bookkeeping, so origin and replica share one apply path.

### 4.3 The ack sequence

The durable ack path, in canonical vocabulary:

```
Accept → DedupCheck → StageCommit → Forward → Receipt × (RF−1) → ClientAck
```

| Step | What happens | Fails as |
|---|---|---|
| `Accept` | Adapter decodes, validates tenant, applies admission (§4.6) | protocol-native reject; over-cap is non-retryable |
| `DedupCheck` | Window-table lookup on (tenant, hash-or-token) (§4.4) | hit on acked entry → replay stored outcome; hit on in-flight entry → `Throttle` |
| `StageCommit` | One DuckDB txn: insert rows + dedup-window entry + advance applied-watermark row; fsync on commit | typed storage error; batch not acked |
| `Forward` | Ship the `(origin, seq)` range to RF−1 ring peers (§5) | peer timeout → ring walk-down (§5) |
| `Receipt` | Each peer's `PeerApply` has durably committed | fewer than RF−1 within timeout (after the ring walk-down, §5) → `Throttle` (UNAVAILABLE + RetryInfo) — the batch is staged and durable, so the signal is retryable by right; the retry replays success once receipts complete (§4.4.1) |
| `ClientAck` | Success returned; dedup entry marked acked with stored outcome | — |

Normative consequences:

- **DurableAck (§3):** `ClientAck` is emitted only after local fsynced
  commit *plus* RF−1 durable receipts. There is no ack-degraded mode in v1:
  below the replication floor, DuckSpout throttles what it has staged and
  refuses what it has not admitted (both UNAVAILABLE + RetryInfo, §5.1)
  rather than issuing a weaker promise. This is Kafka's
  `acks=all` + `min.insync.replicas` posture; the difference between "we
  acked it" and "we probably have it" is the product.
- **`Forward` begins after `StageCommit` returns**, never concurrently with
  it — a receipt must never exist for bytes the origin could still lose.
- The system tenants `_self`/`_canary` (§9) never traverse this path's ack:
  their ingest is explicitly ack-less, so DurableAck and NoAckedLoss are not
  implicated rather than carved out.
- Latency budget: the path costs one local fsync plus one replication RTT
  (forwards are parallel). The §8 target — ack p99 ≤ 25 ms at RF=2 with
  fsync on — is the honest price of the promise, disclosed rather than
  hidden behind an async ack.

### 4.4 Idempotency and duplicate semantics

Duplicates are handled at three tiers; each tier's scope and residual leaks
are stated exactly, because "exactly-once" claims without scope are the
field's most common dishonesty.

#### 4.4.1 Tier 1 — the accept-node dedup window

A dedup window table lives in the hot DuckDB and is written **in the same
transaction as `StageCommit`** — the window entry and the data it guards
are atomic, so a crash cannot record one without the other.

- **Key:** `(tenant_id, content_hash)` — the hash computed over the decoded
  payload — or `(tenant_id, idempotency_token)` when the client sent
  `x-duckspout-idempotency-key`, which takes precedence. The tenant is in
  the key because two tenants may legally send byte-identical bodies; a
  tenant-blind hit would answer tenant B's write with tenant A's ack — an
  acked-write loss. This is ClickHouse's insert-dedup design
  (content hash, `insert_deduplication_token` precedence) minus the shared
  coordination store, which ownership routing makes unnecessary.
- **Bounds:** `dedup.window_ttl` default **24 h**, `dedup.window_max_entries`
  default **100 k**. The TTL is derived from the recommended collector
  config (§4.1.1): the window must outlive the longest retry the edge is
  configured to attempt. DuckSpout warns when count-cap eviction pushes the
  effective window age below the documented retry horizon.
- **Semantics:** a duplicate of a fully-acked entry replays the original's
  success, with any `partial_success` reconstructed from stored per-item
  counts (OTLP forbids retrying a populated `partial_success`, so the
  replayed body must match the original's counts). A duplicate arriving
  while the original is still pre-RF gets UNAVAILABLE + RetryInfo — the
  client in that window is by definition a retrying OTLP client that
  already handles retry signaling; no waiter-coalescing machinery.
- **Stage-then-throttled entries are replayable-as-acked.** A request that
  was staged and then resolved retryable (receipt timeout, rung 2 — §4.3,
  §5.1) leaves an unacked entry guarding durable data that *will* drain.
  That entry is never poison: the moment its receipts reach RF, a retry
  replays success — with the ack evidence computed exactly as ClientAck
  computes it (`DedupCheck`'s `AtRF` branch, §3.3). Until then, retries
  keep getting the retryable signal. No 24 h TTL wait, no second staged
  copy, no client that can never succeed.

#### 4.4.2 Tier 2 — drain-time natural-key dedup per sealed part

Tier 1 is per accept node; retries that land on a different node (LB
reshuffle, node death) leak past it. The drain (§6) closes the gap for
keyed signals: within each sealed part, records are deduplicated on their
natural key, deterministic winner = smallest `(origin, seq)`.

| Signal | Natural key | Default |
|---|---|---|
| Spans | `(trace_id, span_id)` | on |
| Metric samples | `(canonical series identity, ts)` | on |
| Logs | none — opt-in via `dedup.log_identity` | **off** |
| Changelog datasets | declared `key_cols`, keep-latest | on (definitional) |

Logs default off because log lines have no spec-given identity and
content-derived identities have produced false drops in production
elsewhere; a deployment that can define a safe identity opts in.
Changelog keep-latest is not merely dedup — it is the kind's semantics
(§2): the newest `(origin, seq)` for a key within the part wins.
Supplement parts (§6) cannot duplicate a winner by construction — their
per-origin seq coverage is validated disjoint against the sealed winner's
manifest inside the commit transaction — so per-part scope stays sound
when a window has multiple parts.

#### 4.4.3 The public contract

Stated verbatim in user-facing docs, following every credible system's
practice of scoping its promise (Kafka per producer session, ClickHouse per
window):

1. **Transport is at-least-once.** Clients and collectors must retry;
   DuckSpout absorbs the retries.
2. **Idempotent within a bounded window per accept node** (24 h / 100 k
   entries, tier 1).
3. **Effectively-once per key per sealed part** for keyed signals — spans,
   metric samples, changelog datasets (tier 2).
4. **Residual duplicate paths, disclosed:** (a) a retry landing on a
   different accept node, for *unkeyed* signals (logs without
   `dedup.log_identity`); (b) a retry arriving after tier-1 window
   eviction, again for unkeyed signals; (c) keyed duplicates split across
   *different* sealed parts of different windows (arrival-time skew beyond
   the window), where read-time semantics (§7) rather than storage dedup
   provide the final answer.

### 4.5 The overload ladder

One measure, one knob, one monotone ladder, one closed status vocabulary.
Acked data never leaves the staging class except by successful drain —
overload is *always* answered by refusing new promises, never by breaking
made ones (NoAckedLoss, §3). This is the converged posture of Kafka's
NotEnoughReplicas, Elasticsearch's indexing-pressure rejections, and Loki's
retryable 429s: terminate in refuse-with-retry, never drop-acked.

**Measure:** `M = staged_bytes / hot.max_bytes`. Only staging-class bytes
count — cache-class residency (§2) is reclaimable at will and would poison
the signal; staged bytes is the one quantity that cannot be reclaimed
without violating NoAckedLoss and that actually predicts write refusal
(the same reason InnoDB keys flushing on dirty-page fraction, not buffer
occupancy). `hot.max_bytes` (default 75 % of hot-volume capacity at
startup) is the *only* configured byte number; every threshold below is a
fixed function of it.

| Rung | Trigger | Behavior |
|---|---|---|
| 0 | staging needs room | `Evict` cache-class tables (§2). Always on, not a status — invisible to clients. Background eviction to a fixed 5 % free low-water once the cache class is live. |
| 1 — disclose | M ≥ 80 % | Status → `drain_stalled` when a stalled drain drove M there, else `staging_pressure` (sheer ingest rate, drains healthy); operator alert fires here (there is no separate disk alert). Ingest unaffected. |
| 2 — `Throttle` | M ≥ 95 % | No new accepts; pending requests resolve UNAVAILABLE + RetryInfo with growing delay. Spec-exact OTLP vocabulary — every conformant client already backs off correctly. New-range replication is still honored: refusing it belongs to rung 3 alone. |
| 3 — `Refuse` | M ≥ 100 % (`hot.max_bytes` itself) | Refuse new writes AND refuse **new-range** replication, a distinguishable signal that makes origins ring-walk to a substitute peer (§5). Catch-up of already-receipted ranges continues — those are made promises. The top rung; nothing above it, ever. |

Rung 3's replication split is what makes the bound real: a node that kept
accepting fresh pre-ack replication while refusing clients would fill past
its hard limit on peers' behalf. Refusing new ranges pushes placement
elsewhere; honoring receipted catch-up keeps existing promises.

**No hysteresis:** the rung is a pure function of M — transitions follow M
directly, up and down (LadderMonotone, §3). Operator-visible flapping at a
boundary is damped by the disclosure cadence — status is sampled and
published on the heartbeat/metrics cadence — never by rung memory: a
stateless rung is what makes the ladder checkable at all.

**Status disclosure:** one closed enum —
`normal | staging_pressure | drain_stalled | throttling | refusing_ingest`
— plus an orthogonal `replication_degraded` boolean, reported identically
on the health endpoint, metrics, and the registry (§9). A closed enum is
what §3's LadderMonotone property and §8's chaos judge can assert over;
free-text status is unverifiable status.

**Catalog outage = drain stall on the same ladder.** When the catalog DB is
unreachable, drains pause (`LakeCommit` needs it), staging grows, and the
node walks the same rungs it walks for any drain stall — status
`drain_stalled`, then throttling, then refusal, purely as a function of M.
No separate mechanism, and **no timer ever escalates**: a 10-minute outage
on a lightly loaded node changes nothing; the same outage at high ingest
rate throttles honestly. Ingest, replication, and already-resolved hot
query service proceed throughout (§11, catalog-independence rule); a drain
stall also freezes cache demotion, so the cache class drains toward zero as
staging grows — the node sheds query acceleration to protect ingest
durability, automatically.

The only lossy path anywhere is the system tenants' (`_self`/`_canary`)
bounded self-telemetry queue (§9), which is ack-less by definition and
carries a dropped-rows counter.

### 4.6 Admission constants

Two limits, both at `Accept`, both with converged industry defaults:

| Limit | Default | Semantics |
|---|---|---|
| `max_payload_bytes` | 4 MiB | Over-cap → RESOURCE_EXHAUSTED **without** RetryInfo — non-retryable, because retrying an over-sized payload can never succeed and instructing a retry manufactures a loop. gRPC's and the collector's shared 4 MiB default. |
| `admission.max_inflight_bytes` | 10 % of the memory budget (the cgroup memory limit where present, else system RAM — autodetected at startup) | Decoded-but-uncommitted bytes in flight; beyond it, `Throttle`. Elasticsearch's 10 %-of-heap indexing-pressure transplant. |

There are **zero rate limits in v1** and no per-listener token bucket ever:
the memory bound and the byte-denominated ladder govern the real resources
directly, and a rate limit would be a proxy measure guarding what M already
guards. When behavioral tenancy lands (§12), per-tenant `ingestion_rate` is
the only rate limit that will ever exist.

### 4.7 Transforms: SQL-only, three stages

DuckSpout will never grow a transform DSL (§11); its transform language is
SQL, applied at three fixed points, each with a distinct latency/authority
trade:

| Stage | When | Typical use | Cost sits on |
|---|---|---|---|
| Apply-time | inside `StageCommit` | typing/normalization of incoming records — optionally RawDuck's schema-later engine (§4.8) | ack latency (bounded; heavy work is forbidden here) |
| Drain-time | during `SealPart` (§6) | sort order (`sort_key`), rollups, redaction — the one chance to shape the immutable cold object | drain throughput, off the ack path |
| Query-time | view definitions (§7) | renames, derived columns, unit fixes, the `<dataset>_latest` argmax view | the reader |

**Raw-before-transform is stored.** The staging table holds records as
accepted (post-decode, pre-transform); drain-time transforms read from it,
so a buggy redaction or rollup is re-runnable against the untransformed
staging data until the window drains, and query-time transforms are
re-runnable forever. A transform pipeline whose input is destroyed by its
own output cannot be debugged, only regretted. The one irreversibility is
the sealed part itself: after `LakeCommit`, drain-time transform output is
immutable (§6) — which is exactly why redaction belongs at drain time (it
must not survive into cold) and why everything else should prefer
query-time views (reversible by editing the view).

### 4.8 RawDuck: optional schema-later typing, never load-bearing

RawDuck (quackscience) is a schema-later ingestion engine for DuckDB:
auto-table-creation from observed payloads, a monotone type-widening
lattice ("nothing is ever dropped"), dot-notation flattening of nested
keys, and adaptive re-sort/projection machinery. Its lattice is the direct
ancestor of DuckSpout's own schema-evolution model (§2) — the two widen
monotonically for the same reason and map onto lake schema evolution the
same way.

Its position in DuckSpout is fixed: **valuable, never load-bearing.**

- **Without RawDuck (the default):** OTLP records land in a fixed,
  spec-derived OTLP schema per signal. Deterministic, boring, zero
  inference on the ack path. This is the v1 shipping configuration.
- **With RawDuck (opt-in, apply-time stage):** heterogeneous or
  attribute-heavy payloads get typed columns instead of a JSON blob —
  schema-later becomes first-class for workloads that want it.

"Never load-bearing" means concretely: DurableAck, replication, drain,
watermarks, and every §3 invariant hold identically with RawDuck absent,
present, or failing; a RawDuck typing failure degrades to the fixed-schema
path for the affected batch, never to a refusal and never to loss. RawDuck
is experimental-grade, small-team software, and the boundary is drawn so
that its maturity risk is confined to column quality — the one property
whose degradation is an inconvenience rather than a broken promise.

## §5 Replication and Availability

The hot tier's availability story is this section. Object storage made the cold
tier highly available; everything below exists to give the newest data — the
window between ClientAck and LakeCommit — the same property. The mechanisms are
few and reused: one ring, one sequenced log per (origin, partition), one
receipt watermark, one fencing token, one operator ceremony for the day the
guarantees genuinely cannot hold. Every protocol step here is a canonical
action defined formally in §3; this section gives the operational semantics.

### 5.1 RF semantics

`cluster.rf` (default **2**) is **total-inclusive**: RF counts every durable
copy of an acked record, including the copy on the node that will own the
drain. RF=2 means the origin's fsynced copy plus one replica receipt before
ClientAck fires (§4). Rationale for total-inclusive counting: it is what Kafka
`replication.factor` and every operator's mental arithmetic already mean — "how
many disks hold this byte" — and it makes the failure math trivial: RF=N
tolerates N−1 simultaneous node losses with zero acked loss (NoAckedLoss).
An additive convention ("origin + R replicas") produces the classic off-by-one
misconfiguration where an operator believes they bought one more copy than
they did; the convergence across Kafka, Elasticsearch (`number_of_replicas`
being the additive counterexample operators routinely get wrong), and
Cassandra's total-inclusive RF settles it.

RF applies only to the **staging class**. Cache-class tables are never
replicated — the lake is their durable copy, single-copy owner-local residency
is the rule, and eviction is always safe (CacheTransparency, §2). Replicating
a cache over a durable backing store buys nothing: the resolver routes hot
reads to one holder, so a second copy is unreachable waste.

Below the RF floor — fewer live, receipt-answering peers than `cluster.rf`
requires — DuckSpout **stops promising** rather than acking at degraded
RF. A request already staged when the receipt wait times out resolves as
`Throttle` (`UNAVAILABLE` + `RetryInfo`): the bytes are fsynced and will
drain, so the retryable signal is honest by right, and the retry replays
success once receipts complete (§4.4.1). New writes are refused
(`Refuse`) with the same wire form. Throttle and Refuse differ in which
state produces them — an admitted, durable request versus work never
admitted — never in client-visible semantics; neither is terminal in any
sense the wire could express. This is Kafka's
`acks=all` + `min.insync.replicas` posture: an ack is a promise about copies
that exist now, never about copies that will hopefully exist later. A
degraded-ack mode is deliberately absent (§12); its target deployment
(two-node edge) is not a v1 target, and stop-promising keeps DurableAck
unconditional.

### 5.2 The HRW ring

Placement is **rendezvous (HRW) hashing** over an advisory membership view:

- **Pure function.** `owner(partition)` and the RF candidate order are a pure
  function of `(candidate set, partition key)`. For each candidate node,
  score = hash(node_id ‖ partition key); sort descending; the top entry is the
  OWNER, the next RF−1 entries the replica set. Any node holding the same
  membership view computes the same answer with no coordination, no token
  ranges, no rebalancing state — exactly the property a coordinator-free data
  path requires.
- **Minimal disruption.** When a node joins or leaves, only the partitions for
  which that node scored in the top RF change hands; every other partition's
  set is untouched. This is HRW's defining property and why it is preferred
  here over ring-token consistent hashing: no virtual-node bookkeeping, no
  token management, identical disruption bound.
- **Membership view.** The candidate set comes from the registry (`nodes`
  table, §5.5), seeded at bootstrap by `cluster.seed_peers` (default `[]`;
  needed only off Kubernetes) and superseded by the registry once reachable.
  The view is **advisory**: two nodes briefly holding different views cannot
  corrupt anything — they route a forward to a non-owner, which costs one
  extra hop, or double-stage a range, which PeerApply idempotency absorbs.
  Correctness never depends on view agreement (§5.5).
- **Zone awareness.** When the membership view exposes ≥2 distinct
  `node.failure_domain` values, the candidate walk filters so the RF set spans
  domains: after the owner is fixed, subsequent candidates in the owner's
  domain are skipped while a cross-domain candidate remains. With one visible
  domain the filter is inert — zone awareness "on" with one zone is
  meaningless, so the knob is a single boolean escape hatch
  (`cluster.zone_aware`, default auto-on) rather than a tri-state. When no
  cross-domain candidate is live, the walk falls back to same-domain peers:
  availability over placement, disclosed via the `replication_degraded` flag
  (§9), never a refusal on its own.

### 5.3 Ownership routing: merge locality by construction

Any node accepts any write (Accept, §4) — the edge needs no routing
intelligence — but the RF set for a partition **always includes the ring
OWNER**. The acceptor Forwards the batch; once the owner's PeerApply is
receipted, the **owner's copy is the primary and the acceptor's own copy
demotes to replica standing** (a role flip in local metadata; no bytes move).
Consequences, in order of importance:

1. **Only owners drain.** All of a partition's window converges onto one
   node's disk before SealPart. The drain's sort, dedup, and part-packing
   (§6) operate over the complete window locally. Cross-writer overlapping
   cold files are **impossible by construction** — there is no protocol case
   in which two nodes hold "their half" of a healthy window and both drain
   it. The only multi-part window is the churn-boundary supplement case
   (§5.6), which is fenced, validated, and rare.
2. **The forwarding is free.** RF ≥ 2 means every accepted byte crosses the
   network at least once anyway; routing that mandatory hop *to the owner*
   converts replication traffic the durability pillar already pays for into
   merge locality. Ownership routing adds zero incremental network cost over
   naive replication.
3. **Load balance is a latency concern, never correctness.** A plain L4/L7
   balancer in front of any-node-accepts is the default (§9). An
   ownership-affine edge — the otel-collector `loadbalancing` exporter keyed
   so batches land on their owner, saving the one forward hop — is documented
   as an **optional optimization only**: the exporter's alpha/beta maturity
   disqualifies it as a dependency, and DuckSpout must be exactly as correct
   behind a dumb balancer.

### 5.4 The replication protocol

#### Forward / PeerApply / Receipt

| Step | Semantics |
|---|---|
| Forward | The acceptor ships the **logical batch** (post-DedupCheck, post-normalization rows, not wire frames) to each member of the RF set, stamped with `(origin_node, partition, seq)` — `seq` a dense per-(origin, partition) sequence assigned at StageCommit. |
| PeerApply | The peer applies the batch into its hot staging table for the partition, **idempotently**: a `seq` at or below its applied watermark for that (origin, partition) is acknowledged without re-applying. A `seq` that would leave a gap is **refused** — the peer never applies out of order, so its applied prefix is always contiguous (GapFreedom). |
| Receipt | The peer's reply carries its **receipt watermark**: the highest contiguous `seq` durably applied for that (origin, partition). Receipts are cumulative acknowledgments — one number, no per-batch bookkeeping, retransmit-safe. |

The origin waits `replication.receipt_timeout` (default **5 s**; revisit by
measurement — intra-AZ and WAN diverge) for the receipts DurableAck requires.
On timeout it **ring-walks down** the candidate order to the next substitute
peer, preserving full RF; only when the walk exhausts live candidates does
the staged request resolve as Throttle and the node stop admitting new
writes (§5.1). At a peer's hard overload threshold the peer refuses
**new-range** replication with a distinguishable signal (the origin ring-walks
immediately rather than waiting out the timeout) while continuing catch-up of
already-receipted ranges — the disk bound holds without ever dropping acked
data (Throttle/Refuse ladder, §4).

#### The table is the log

There is no separate replication journal. The hot staging table — ordered by
`(origin, seq)` within each partition — **is** the log. Catch-up after a
disconnect is one query on the live peer:

```
SELECT ... WHERE origin = ? AND seq > <your receipt watermark> ORDER BY seq
```

streamed back through the same PeerApply path. One storage engine, one fsync
discipline, one recovery path; the WAL that makes the table durable (§4)
makes the log durable for free. This is the design's central economy and the
reason gap refusal is cheap: contiguity of the applied prefix is a table
scan's natural order, not an index to maintain.

#### Schema changes ride in-band

A schema change (EvolveSchema, §3) is a **sequenced record in the same
per-(origin, partition) log**, totally ordered against the data rows around
it by `seq`. Peers **fail closed on unknown columns**: a data row referencing
a column the peer has not yet applied a schema record for is a gap by
definition and is refused like any other gap. On catch-up, peers apply
**widen-first**: schema records replay to the origin's schema state before
the data rows that depend on them. Because the schema lattice is monotone
(widenings only, §2), replay is value-preserving and order-convergent by
construction; the drain then commits schema evolution strictly before file
addition (§6). No side channel, no schema epoch protocol — the log's existing
total order supplies everything.

### 5.5 Claims and the registry: advisory discovery

The catalog DB holds three registry tables — `nodes`, `claims`,
`watermarks` — that the query path resolves against at bind time (§7). Their
maintenance costs the data path almost nothing:

- **ClaimAdvertise** rows (`partition → node, role ∈ {owner, replica}`) are
  published as a **side effect of PeerApply**: the first apply for a
  partition the node has no claim row for triggers the insert. No separate
  claim protocol, no claim heartbeat distinct from the node heartbeat.
- **`replicated_through`** — the per-(partition, node) receipt coverage the
  query path and takeover logic read — is advanced on **Heartbeat cadence**,
  batched, not per-batch. Staleness is bounded by the heartbeat interval and
  costs only freshness of routing decisions.
- **Heartbeat** rows carry a TTL; a node whose heartbeat lapses is treated as
  dead by resolvers and by takeover election (§5.6), subject to the
  suppression window of §5.10.

The registry is **advisory, always**. A wrong or stale entry costs latency —
a query routed to a node that no longer holds the range gets a typed
miss-and-retry, a forward lands one hop off — **never correctness**. The
authoritative facts live elsewhere: watermarks in the same catalog
transaction as LakeCommit (WatermarkHonesty, §6), data coverage in the hot
tables and sealed parts themselves, and the registry is reconstructible from
those plus window manifests after a catalog restore (§9). This is why the
data path survives a catalog outage (§5.7's boot rule aside, running nodes
never block on the registry): ingest, replication, and already-resolved hot
queries proceed; only new bind-time resolution and drains pause, and say so.

### 5.6 Node death, end to end

Timeline for the death of a partition's owner:

1. **Detection.** The owner's Heartbeat TTL lapses (or peers observe hard
   connection failure). Resolvers and replicas treat it as dead once the
   takeover-suppression window (§5.10) — which covers planned restarts — is
   not in effect or has expired.
2. **Write reroute.** The HRW walk over the membership view minus the dead
   node yields a new owner; acceptors forward there. The new owner was, by
   the walk's construction, almost always already in the RF set — it holds
   the partition's receipted prefix and begins accepting with no state
   transfer. Sequences are per-(origin, partition), so no renumbering occurs.
3. **Read reroute.** Bind-time resolution (§7) picks the live claimant with
   the greatest `replicated_through` coverage. `complete` reads whose demanded
   range exceeds every live replica's coverage **fail closed** with a typed
   coverage error (WatermarkHonesty) — degraded availability is disclosed,
   never disguised.
4. **TakeoverDrain.** The new owner drains the dead owner's undrained
   window(s) from its own replica copy. Because receipts guarantee the
   replica's prefix is contiguous, the drained part is gap-free up to
   `replicated_through` (GapFreedom); anything the dead node acked is, by
   DurableAck, within some live replica's receipted prefix, so NoAckedLoss
   holds through the takeover.
5. **The churn-boundary split.** If the old owner had already committed a
   part for the window's early range before dying, the takeover produces a
   **supplement part** covering only the residue. SingleDrainCommit governs:
   the commit guard is `UNIQUE(partition, window_id, part_kind)` with an
   explicit supplement path that **validates disjoint per-(origin, seq)
   coverage against the winner's manifest inside the same catalog
   transaction**. Two drains of the same range cannot both commit; a
   supplement that overlaps the winner is rejected at the guard. Supplement
   parts are the *only* sanctioned multi-part window and cannot duplicate
   winner rows by construction, which is what keeps per-part drain dedup
   scope sound (§6).

Classic split-brain has nothing to bite on: logs are disjoint per-origin
keyed sequences, so there is no shared tail to truncate and no leader term to
dispute — the single contended act is the drain commit, and that is settled
by the catalog's atomic guard (the same single-commit-point discipline as
Iceberg's optimistic concurrency).

There is **no automatic rebalancing**. Takeover-on-death is the only
migration; scale-out changes routing for *new* windows only, and old windows
drain where they were staged. Data at rest never moves to chase the ring.

### 5.7 Incarnation fencing

Every process boot executes **FenceBoot**: the node draws a fresh
`incarnation` from a catalog-DB sequence and persists it locally. Every
message — Forward, PeerApply, Receipt, Heartbeat, drain commit — carries
`(node_id, incarnation)`. Peers and the catalog track the highest incarnation
seen per node and **reject anything older** (FencedZombie): a partitioned
former self that wakes and tries to forward, receipt, or commit is refused
everywhere with a token it cannot forge forward. This is Kafka's epoch
fencing; the catalog sequence gives monotonicity without a coordination
service.

Catalog outage at boot splits two cases, so a rolling restart cannot wedge on
a catalog incident:

- A node with a **persisted incarnation** boots into **replica-only degraded
  mode** (`DegradedBoot`, §3): it applies and receipts replication under its
  existing incarnation but takes no ownership actions (no drains, no
  takeovers — both need the catalog anyway). It promotes itself when the
  catalog returns and FenceBoot completes.
- Only a **genuinely new node** — no persisted incarnation — waits, in a
  typed startup state. It has no identity to be safely partial with.

### 5.8 DeclareLoss: the ceremony for the day RF was not enough

When every replica of an undrained range is gone — RF simultaneous disk
losses, or a declared-dead node that was the last holder — the partition's
watermark freezes and `complete` reads over the missing range fail closed,
indefinitely. Unwedging is a deliberate operator act, never automatic:

- **DeclareLoss** takes the **exact** lost `(partition, origin, seq-range)`
  set — no wildcards, no "whatever is missing."
- It requires the literal parameter `accept_data_loss: true`. The name is the
  consent form.
- It writes a **permanent loss-ledger row** — a first-class queryable table —
  **in the same catalog transaction as the watermark advance** past the lost
  range. The watermark never moves without the confession landing atomically
  beside it; WatermarkHonesty's contract becomes "complete, except the
  ledgered ranges," and the ledger is the auditable record of every such
  exception forever.
- It is **refused while any live replica still advertises coverage** of the
  range. The ceremony destroys the claim to completeness, so it must be
  impossible while completeness is still recoverable.

The shape follows the industry's two established loss ceremonies —
Elasticsearch's `allocate_stale_primary`/`allocate_empty_primary` and Kafka's
opt-in unclean leader election — with one deliberate hardening: both of those
lose data silently after the flag; DuckSpout's ledger makes the loss a
permanent, queryable fact co-committed with its consequence.

### 5.9 Hot-disk corruption

Detection needs no scrubber: DuckDB's per-block checksums catch corruption on
any read, and **the drain reads every staged byte** (§6) — the scrub is the
pipeline. On a checksum failure:

1. **Quarantine** the affected window (it stops serving reads and is excluded
   from drain).
2. **Re-fetch** the exact `(origin, seq)` ranges from a replica via the
   catch-up path (§5.4) — the same query, the same PeerApply machinery,
   nothing corruption-specific.
3. Re-verify and release the window back to normal life.

A double failure — the replica's copy is also bad or gone — escalates to
DeclareLoss (§5.8). There is no repair mode that guesses; there is
re-replication or the ceremony. WAL-replay corruption reports route to the
same quarantine/re-fetch path.

### 5.10 Rolling restarts

Planned restarts must not trigger the machinery built for deaths. The
shutdown sequence (in-daemon, behind SIGTERM; any preStop hook is a thin
delay only):

1. Fail readiness — the balancer stops sending new accepts.
2. Finish in-flight Forwards and flush replication so every acked byte is
   receipted at full RF.
3. Write an **advisory `draining(restart, expected_back_by)` row** to the
   registry — this shape is the protocol statement; §9.1.2 references it.
4. Shut down cleanly. This is a **shallow drain** — the PVC and the replicas
   hold the data; a node never final-drains its windows to cold just to
   restart (Mimir and Strimzi converge on exactly this: rolls are shallow,
   deep drains are for decommission).

Two guards keep restart and takeover from colliding:

- **PDB `maxUnavailable=1`** — one node rolls at a time, so RF−1 copies of
  everything stay live throughout.
- **Takeover suppression**: replicas do not initiate TakeoverDrain for a node
  within **2× the termination grace period** of its draining row (or its last
  heartbeat, when the shutdown was too abrupt to write one). The window is
  **derived, not configured** — it is a function of a value the deployment
  already declares, and a knob here would only let the two numbers disagree.
  Suppression expiring on a node that never returns degrades to the ordinary
  death path of §5.6; the suppression-expiry constant is verified in the §3
  model with a deliberately-broken never-expires variant (§8).

### 5.11 Configuration surface of this section

| Setting | Default | Why it is a knob |
|---|---|---|
| `cluster.rf` | 2 | durability vs cost is a real tradeoff |
| `cluster.zone_aware` | auto | boolean escape hatch only |
| `cluster.seed_peers` | `[]` | non-Kubernetes bootstrap |
| `replication.receipt_timeout` | 5 s | intra-AZ vs WAN latency diverges |
| `node.failure_domain` | zone label / config | non-Kubernetes has no downward API |

Fixed by derivation or constant: takeover suppression = 2× termination grace;
heartbeat cadence is a release constant (5 s, TTL 15 s = 3× cadence —
§9.6.3); the RF floor is `cluster.rf` itself (stop-promising, no
degraded-ack knob). Everything else this section describes is protocol,
not policy.

## §6 Drain and the Cold Tier

The drain moves data from the staging class to the lake. It is the only path by which
acked data leaves staging (§4, §11), the only writer of cold objects, and the single
merge point of the entire system: every reorganization of data — sorting, deduplication,
part packing, snapshot generation — happens hot, on local NVMe, before the first byte
reaches object storage. The cold tier is never compacted.

### 6.1 Cold objects are immutable-with-expiry

Every cold object in DuckSpout's life has exactly two logical storage operations:
one PUT (§3 `PutPart`; byte-identical idempotent re-PUTs on retry collapse into it,
§6.5) and, when retention releases it, one whole-file DELETE (§3 `Expire`). It is
never modified, appended to, rewritten, or merged in between.

Rationale. Compaction on object storage is GET + PUT churn: every merged byte is
downloaded, re-sorted, and re-uploaded, paying request costs and egress twice for data
that was already durable. Lakehouse ecosystems accept this because their writers flush
small files under a clock deadline and repair the damage later (Iceberg's rewrite-data-files
maintenance, DuckLake's `merge_adjacent_files`). DuckSpout removes the cause instead of
treating the symptom: because the hot tier is already durable (fsync, §4), already
replicated (RF, §5), and already queryable (§7), there is no deadline forcing a premature
flush — so parts can be sealed at their final size and final sort order, once. All
merge-shaped I/O runs where it is nearly free: over local DuckDB tables on the owner node.

Consequences, stated as rules:

- **A part is FINAL at seal.** No post-hoc compaction job, no small-file debt, no
  background rewriter. `merge_adjacent_files` (and its Iceberg equivalent) is demoted to
  an emergency repair tool an operator invokes by hand (§9); it is never scheduled.
- **Each byte is uploaded exactly once, in final form.** The cold tier's write
  amplification is 1.0 PUTs per part — a published benchmark metric (§8).
- **Immutability makes downstream caching trivially coherent.** A cold URL is never
  reused with different contents, so query-side file caches (§7) need no invalidation
  protocol.
- **Read-side IAM never needs write carve-outs** (§7): cold prefixes are read-only to
  everything except the drain's PUT and retention's DELETE.

### 6.2 SealPart: one sorted COPY, final parts only

`SealPart` (§3) is a single sorted `COPY ... TO` over the micro-window staging tables of
one partition, executed in the owner's embedded DuckDB, producing one Parquet part (or a
bounded set of parts at the size target). The COPY performs, in one pass:

1. **Merge** across the window's micro-window tables (§2) — the fold that other systems
   defer to compaction happens here, at local-disk speed.
2. **Sort** by the dataset's declared `sort_key`. Default: event time. Parts of
   `changelog` datasets (§2): `(key_cols, origin, seq)` — key-clustered so latest-view reads
   and snapshot generation scan contiguous key ranges. Snapshot parts: `(key_cols)`.
   Sort keys govern only parts sealed after a change; sealed parts are never rewritten.
3. **Drain-time dedup** on the dataset's natural key (spans, metric samples) or declared
   key keep-latest (changelog datasets), deterministic smallest-`(origin, seq)` winner
   (§2). The count of rows removed is recorded as `dedup_removed` in the window manifest —
   load-bearing for `Demote` (§6.9) and for the `CacheTransparency` proof obligations (§3).

**Sizing.** A part seals when it reaches `drain.part_target_bytes` — default 384 MiB,
recommended band 256–512 MiB, matching the row-group and object sizes the Parquet-over-S3
ecosystem converged on for scan efficiency — or when the oldest staged data in the
partition reaches `drain.max_age` (default 30m), whichever first. The age cap bounds
watermark staleness for trickle partitions (`WatermarkEventuallyAdvances`, §3); it is a
freshness bound on `complete` reads, not a durability bound — durability was settled at
ack time (§4).

**Trickle datasets.** A partition that cannot fill a reasonable part within `drain.max_age`
may drain via DuckLake data inlining: rows committed into the catalog database itself,
zero PUTs, folded into Parquet later by the lake's own machinery. This is a
DuckLake-exclusive optimization and is never the portable answer — the portable answer
for small tenants and slow datasets is a longer age cap. Nothing on the critical path
may require inlining (§6.4, §11).

### 6.3 Late arrivals

`drain.allowed_lateness` (default 15m) is the hold: a window remains eligible to absorb
rows whose event time falls inside it for that long past window close, so ordinary
network-delayed data drains into its home window.

A row arriving later than that gets **arrival-window placement**: it is sealed into the
part being drained at its arrival time. The event-time column is never rewritten — the
row remains truthful about when its event happened; only its file placement reflects
arrival. This is the append-only answer to lateness: the alternatives are reopening a
sealed window (a cold rewrite — banned) or dropping the row (violates `NoAckedLoss`).

**Stated cost.** A straggler widens its host part's event-time min/max statistics, so
zone-map pruning (§7) admits that part into scans overlapping the straggler's event time.
The marginal cost is one extra part scanned per straggler-bearing part per overlapping
query — latency, never correctness. Watermark semantics are unaffected: `complete_through`
already accounts for lateness (§2), and a post-watermark straggler is by definition
outside every `complete` read's contract.

### 6.4 The LakeCommitter port

DuckSpout commits to the lake exclusively through the `LakeCommitter` port (§10). The
port is the lake-agnosticism boundary: everything above it is lake-neutral; everything
below it is one backend crate. The contract has six operations:

| Operation | Contract | Notes |
|---|---|---|
| `commit_files` | Atomically register a set of sealed parts **and** advance the named partition watermarks in the same commit. Returns Committed / Aborted / Indeterminate (§6.5). | The only routine write. `WatermarkAdvance` is not a separate step — it rides `LakeCommit` atomically, which is what makes `WatermarkHonesty` (§3) provable: no state exists where files are visible but the watermark lies, or vice versa. Carries the window manifest (§6.8) and any required `evolve_schema` as one atomic unit where the backend allows; where DDL and append cannot combine, evolve commits strictly before add — add-before-evolve is forbidden (files committed ahead of their schema silently hide columns in both DuckLake and Iceberg). |
| `replace_files` | Atomically swap named objects for named replacements. | **Emergency only** (operator-invoked repair, declared-loss annulment §9). Never scheduled, never on the drain path — its existence is not a license to compact. |
| `evolve_schema` | Apply a monotone, lossless schema change (§2's type lattice). Idempotent; concurrent applications converge. | Commutative-join semantics make crash-retry and concurrent owners safe. |
| `expire` | Whole-file DELETE of named parts (§3 `Expire`). | Metadata-only from the table's perspective; the physical DELETE is the object's second and last storage operation. The changelog-coverage guard (Keep Rule 10; `SnapshotCovered`, §3) is enforced above the port, before `expire` is ever called. |
| `read_watermarks` | Return the last committed watermark state for named partitions. | The read-back half of Indeterminate resolution (§6.5) and of boot-time recovery (§5). |
| `attach_info` | Return what a querying DuckDB needs to attach this lake (catalog URI, credentials shape, dialect quirks). | Feeds the catalog extension's bind (§7). |

**First implementation: DuckLake.** The committer embeds a DuckDB instance used purely
as a metadata-commit executor — rows never transit it. `commit_files` executes
`CALL ducklake_add_data_files(...)` for the sealed parts and inserts the watermark
sidecar row into DuckSpout's registry table **in the same Postgres transaction** as
DuckLake's own catalog writes. One transaction, one atomicity domain: the sidecar and the
file registration commit or abort together, which is the whole mechanism behind
`WatermarkHonesty` on this backend.

**Second implementation: Iceberg, by contract.** The Iceberg committer maps
`commit_files` to a REST-catalog append commit and carries the watermark state in the
snapshot's **commit properties** — the portable watermark channel, chosen because every
Iceberg catalog persists snapshot properties atomically with the snapshot itself and
exposes them to readers without a side table. `evolve_schema` maps to Iceberg schema
evolution (the lossless promotion set both formats share, §2); `expire` maps to
delete-files + expire-snapshots.

**Neutrality rule (Keep Rule, §11):** nothing on the critical path may depend on a
DuckLake-exclusive feature. Inlining (§6.2) is an optimization; the sidecar table is a
DuckSpout table, not a DuckLake feature; everything else in the port is expressible in
both backends. The port ships with a published **conformance test suite** — atomicity of
commit+watermark, Indeterminate resolution, idempotent re-registration, expire semantics,
evolve ordering — so that backend #2 (and #3) is a community contribution validated by
the same harness, not a fork.

### 6.5 LakeCommit's three outcomes

`LakeCommit` returns exactly one of:

- **Committed** — files registered, watermark advanced. The owner proceeds to
  `Demote`/`DropWindow` (§6.9).
- **Aborted** — the backend definitively rejected the commit (constraint violation,
  serialization failure). Nothing changed; the drain retries or yields to the guard
  (§6.6).
- **Indeterminate** — the connection dropped mid-COMMIT and the outcome is unknown.

Indeterminate is resolved by **exactly one read-back before any retry**: the committer
calls `read_watermarks` (or checks part registration) to learn whether the commit landed.
Blind retry is forbidden — it either double-registers or trips the guard spuriously; and
unbounded read-back loops are equally forbidden — one read-back either resolves the
outcome or the catalog is down, which is a drain stall on the overload ladder (§4), not
a new state.

Registration is idempotent by construction:

- **Deterministic part naming.** A part's object name is a pure function of
  `(dataset, partition, window_id, part_kind, discriminator)` — where the discriminator
  is the supplement's per-origin seq range or the snapshot's `snapshot_as_of_seq`. Two
  attempts to drain the same window produce the same names, so a re-PUT overwrites
  byte-identical content and a re-register is detectable.
- **Check-before-register.** The committer verifies absence inside the commit
  transaction; a name already present short-circuits to Committed.

### 6.6 The SingleDrainCommit guard

Two drainers must never both commit a window — the owner racing its own retried past
self, or a takeover drainer (§5 `TakeoverDrain`) racing a not-actually-dead owner
(`FencedZombie`, §3). The guard is a database constraint, not a lease:

```
UNIQUE (partition, window_id, part_kind, discriminator)
```

on the registration table, enforced inside the same transaction as `commit_files`.
The discriminator is the deterministic-naming discriminator of §6.5: fixed (`'-'`)
for `part_kind = 'window'` — so **at most one window part per window, ever** — the
per-origin seq range for a supplement, and `snapshot_as_of_seq` for a snapshot. The
first committer of any fence key wins; every other attempt Aborts and the loser
discards its local work. `window_id` is a dense per-partition sequence (§6.8), so
the constraint is exact. This four-column key is the SQL form of
`SingleDrainCommit`'s first conjunct (§3.4); the two state the same fence.

**The supplement path.** Some legitimate flows produce a second — or a later —
part for an already-committed window: a takeover drainer holding receipted ranges
the winner lacked, a second takeover residue after sequential owner deaths at
RF ≥ 3, or a declared-lost node resurrecting with data (§9.4.2). A supplement
commit inserts under `part_kind = 'supplement'` with its seq-range discriminator
and, **in the same transaction**, validates that its per-origin seq coverage is
pairwise disjoint from **every** already-committed part of the window — winner and
prior supplements alike (`SingleDrainCommit`'s second conjunct, §3.4). Multiple
supplements per window are therefore legal and fenced; disjointness makes them
unable to duplicate committed rows by construction, which is what keeps per-part
dedup scope (§6.2) sound with multiple parts per window. Bind-time resolution
(§7) unions all parts of a window.

### 6.7 Retention: whole-file expiry and snapshot rollover

Parts are **tenant-pure, retention-class-pure, and kind-pure** (§2). Therefore a part is
never partially expired: when its retention class's clock runs out, the whole file goes.
Expiry is the §3 `Expire` action — metadata-only from the table's view, one `expire`
call, one whole-file DELETE — because no part ever spans a retention boundary. There is
no retention rewrite, ever.

**Parts of `event` datasets**: age-based expiry at the retention class horizon. Done.

**Parts of `changelog` datasets** carry an obligation event-dataset parts don't: a part
may hold the only record of some key's latest value, so age alone cannot justify
deletion. Retention for changelogs is **snapshot rollover**:

1. **`SnapshotSeal`** (§3): the partition owner periodically seals a part with
   `part_kind = 'snapshot'` containing the full latest-by-key state of the partition
   as-of arrival sequence S (deleted keys absent — tombstones are applied, not copied).
   Generation reads the newest covering snapshot plus changelog-since via the lake and
   local hot state, and **appends a new object** — derivation is not compaction; the ban
   is on rewriting existing objects, and a snapshot part conforms to
   one-PUT-one-DELETE like any other.
2. **Trigger:** dirty ratio 1.0 — changelog bytes accumulated since the last snapshot
   equal the snapshot's size (the log-cleaner convergence Kafka standardized; fixed
   constant, no knob). This bounds space amplification at ≤2× live-state bytes on cold.
3. **Coverage:** once the snapshot at S is committed, changelog parts **wholly older
   than S − `drain.allowed_lateness`** become ordinary age-expirable files. The lateness
   margin guarantees no straggler placed by arrival (§6.3) is covered by a snapshot that
   predates it.
4. **Uncovered changelog parts are keep-forever.** A changelog part may be expired only
   when a sealed snapshot covering its arrival range exists — a Keep Rule (§11), held
   formally by `Expire`'s guard and the `SnapshotCovered` invariant (§3), because
   violating it silently deletes the last value of a key.

**Snapshot fencing.** Snapshots fence on their own discriminator, not a
vocabulary-reuse of the window fence: within the `UNIQUE (partition, window_id,
part_kind, discriminator)` constraint (§6.6), a snapshot's discriminator is its
`snapshot_as_of_seq`, with snapshot generation serialized per partition under the
drain scheduler — one partition at a time, stall-and-disclose under the overload
ladder (§4) when cold reads are slow. The snapshot's manifest is its fencing record.

### 6.8 Window manifests, watermark reconstruction, catalog recovery

Every `LakeCommit` carries a **window manifest**: `window_id` (dense per-partition
sequence — contiguity must be decidable), per-origin seq coverage, row counts,
event-time min/max, `dedup_removed`, and part names. The manifest rides the commit
atomically and is stored queryably.

This makes the watermark state **authoritative-but-reconstructible**: the catalog's
watermark rows are the fast path, but the ground truth is derivable from (a) the dense
manifest sequence in the lake and (b) live hot staging state on the nodes. Nodes and
claims are soft state throughout (§5); watermarks are the only registry state that
matters, and even they can be rebuilt.

**Catalog PITR recovery procedure (sketch — full runbook in §9):**

1. Restore the catalog database from point-in-time recovery. Ingest, replication, and
   already-resolved hot queries were never interrupted (§11); drains and new bind-time
   resolution were stalled and disclosed.
2. Nodes re-register through `FenceBoot` (§5): persisted incarnations resume, fencing
   rejects any pre-restore zombie.
3. **Orphan reconcile:** list cold objects against registrations. Deterministic naming
   (§6.5) makes every orphan attributable to a specific `(partition, window_id,
   part_kind)`; each is either re-registered (its commit was lost to the PITR horizon —
   re-registration is the one read-back path replayed) or deleted (its commit never
   happened anywhere).
4. **Recompute watermarks** from manifest contiguity plus live hot coverage. The
   recomputed watermark is ≤ the true pre-failure watermark, never greater —
   `WatermarkHonesty` holds through recovery; the cost of the PITR gap is temporary
   conservatism, never a false `complete`.
5. Resume drains. Hot tables were never dropped without durable commit confirmation
   (§6.9), so nothing staged was lost to the catalog failure.

### 6.9 Demote, Evict, DropWindow: class mechanics at drain commit

On Committed, the drained window leaves the staging class (§2). What happens to the
local table is a residency decision, never a correctness one:

- **Default (`residency = none`, the v1 behavior): `DropWindow`** — DROP TABLE at
  drain commit. O(1) cleanup, no vacuum debt, disk returned immediately.
- **`Demote`** (when the cache class is active, §2): reclassify the table in place from
  staging to cache — **only if the window's manifest records `dedup_removed = 0`.**
  A window the drain deduplicated is not row-equivalent to its sealed parts; demoting it
  would let a cache table answer differently than the lake, violating
  `CacheTransparency` (§3). With the zero guard, substitution is unconditionally safe;
  any window with `dedup_removed > 0` is dropped instead. Demotion happens strictly
  after the `LakeCommit` is durable — a datum is cache only after the lake owns it.
- **`Evict`** applies to cache-class tables only, is unrestricted, takes no coordination
  and no locks the read path depends on, and can never touch staging (`LadderMonotone`
  and the never-evict-staging rule, §4/§11).

**Crash between LakeCommit and local reclassification:** on recovery the node re-attempts
the drain, the `SingleDrainCommit` guard Aborts it (the commit already stands), and the
node completes the pending `Demote`/`DropWindow` instead. The one-side-serving rule (§3:
a window is served from staging XOR lake/cache, fenced by the guard) holds across the
crash.

A drain stall (catalog outage, cold-store outage) freezes demotion along with commits:
staging grows, cache drains toward zero under rung-0 eviction, and the node sheds query
acceleration to protect ingest durability — automatically, with no new mechanism (§4).

## §7 Query

DuckSpout has no query engine and never grows one. Every query runs inside an
ordinary DuckDB — the user's process, a notebook, a dashboard's connection pool.
DuckSpout's query surface is exactly three things: a catalog extension that makes
hot and cold look like one database, a Flight server on every node that serves the
hot side, and a completeness contract (read concerns) that makes the seam honest.
This section is normative for all three.

### 7.1 The one-ATTACH model

The entire query surface is a single statement:

```sql
ATTACH 'duckspout:<catalog-dsn>' AS ds;
SELECT * FROM ds.events WHERE ts > now() - INTERVAL 15 MINUTE;
```

The DuckSpout catalog extension does everything else internally:

1. Connects to the registry database named by the DSN — with the DuckLake
   backend, the same Postgres that hosts the lake catalog (§7.3 scopes the
   Iceberg topology, where the registry is its own Postgres).
2. Attaches the lake (DuckLake first; any lake the committer contract supports,
   §6) inside the same DuckDB process.
3. At **bind time, per query**, reads the registry (§7.3) and resolves which hot
   nodes — if any — must contribute, then mounts them via Airport (§7.4).

The user never attaches the lake, never names a hot node, never learns the
cluster topology. One line in `~/.duckdbrc` makes it zero-touch:

```sql
-- ~/.duckdbrc
ATTACH IF NOT EXISTS 'duckspout:postgres://duckspout_reader@catalog/prod' AS ds;
```

Rationale: DuckDB's ecosystem converged on ATTACH as the mount point for remote
catalogs (DuckLake, Postgres, Iceberg REST, Airport all use it); a tool that
requires three coordinated attachments plus manual node discovery would be
operated wrong by default. Collapsing them behind one catalog extension is the
only design in which the completeness contract (§7.6) can be enforced at all —
enforcement lives at bind, and bind must be DuckSpout's.

Rejected: a DuckSpout-side distributed-join coordinator or any fan-out execution
layer. Joins belong to the querying DuckDB (§7.7); building a second query engine
would contradict both the one-ATTACH model and the library architecture (§10).

### 7.2 Bind-time resolution: the three resolvers

Resolution maps a query's referenced datasets and predicates to concrete scan
branches. Three resolvers run in sequence at bind; all of their inputs are
advisory soft state except the watermarks, which are transactional (§7.3).

| Resolver | Input | Output | Rule |
|---|---|---|---|
| Tier | query time range × per-partition `complete_through` | hot branch, cold branch, or both | A range at or below `complete_through` is lake-served and **never touches hot**. Only the owed remainder — above the watermark — resolves to hot. |
| File | DuckLake per-file column statistics (min/max, row counts) | cold file set | Standard lake pruning; DuckSpout adds nothing and subtracts nothing here. The lake's own planner prunes with the stats every SealPart/PutPart wrote (§6). |
| Holder | registry claims | **exactly one** hot holder per owed partition | Owner preferred; if the owner is unreachable or its claim is stale, the replica whose `replicated_through` covers the owed range is chosen. One holder per partition, always. |

Three consequences are load-bearing:

- **Hot is never scanned redundantly.** Because a covered range is lake-served
  unconditionally, drained data is read from the lake even while a hot copy still
  exists on a node. Hot serves only what the lake does not yet own. This is the
  query-side half of CacheTransparency (§3): no hot residency decision can change
  the answer to a `complete` read.
- **No cross-node duplication.** One holder per owed partition means the union in
  §7.5 never sees the same row from two nodes, regardless of RF. Replicas exist
  for durability and takeover (§5), not for read fan-out.
- **Everything is advisory except the answer.** Nodes and claims are soft state,
  refreshed by Heartbeat and ClaimAdvertise (§5); a stale claim costs a retry or
  an unreachable-branch error under `complete` (§7.6), never a wrong result.
  Correctness rests solely on watermarks and the drain guard (SingleDrainCommit,
  §3) — the registry is a routing hint, not a truth source.

The Tier rule as stated is the **v1 rule, under which the cache class is
dormant on the read path**: a covered range never touches hot, so no
`complete` read ever consults a cache-class table — §2.4's
serve-`complete`-reads row and the `dedup_removed` gate are armed doctrine
awaiting the deferred cache-coverage advertisement (§12.7). The amended
rule is pre-stated here so the deferral has a design-of-record: when
advertisement lands, a covered range may be served from a cache-class
holder advertised in the registry instead of the lake — transparently, by
CacheTransparency — while the tier boundary itself (hot staging serves
only above the watermark) is unchanged.

Resolution is per-query, not per-session: claims move (takeover, §5; drain
progress, §6), and a session-cached route would silently go stale. The cost is
one registry read per bind against three small indexed tables; this is the same
price every lake query already pays the catalog.

### 7.3 The registry tables

The registry lives in a Postgres database under the `duckspout` schema —
**with the DuckLake backend, the same database as the lake catalog** (the
backend-scoping paragraph below covers Iceberg):

| Table | Contents | Written by | Consistency |
|---|---|---|---|
| `duckspout.nodes` | node id, incarnation, endpoints, failure domain, status enum | Heartbeat | soft state, reconstructible |
| `duckspout.claims` | (partition → holder) coverage: owner claims and replica claims with `replicated_through` | ClaimAdvertise, piggybacked on PeerApply/Heartbeat | soft state, advisory |
| `duckspout.watermarks` | per-partition `complete_through`, per-dataset `dimension_as_of`, loss-ledger annotations | **LakeCommit only** | transactional, authoritative |

The placement is a deliberate collapse: watermarks advance **in the same catalog
transaction as LakeCommit** (§6). There is no window — not a millisecond — in
which files are committed but the watermark lags, or the watermark claims
coverage the lake lacks. WatermarkHonesty (§3) is enforced by transaction
atomicity, not by protocol discipline. A separate watermark store would reopen
exactly the two-phase gap this design exists to close; industry convergence is
the same move lakehouse catalogs made when they folded commit metadata into one
atomic pointer swap.

**Backend scoping.** The single-database collapse is the **DuckLake
backend's** topology: DuckLake's catalog is itself Postgres, so registry
and lake catalog share one instance and the watermark row shares
LakeCommit's transaction (§6.4). With an **Iceberg REST catalog** there is
no shared transaction to join, and the topology is stated rather than
implied: the registry (`nodes`, `claims`, `watermarks`, principals/grants)
lives in **its own Postgres**; the **watermark authority is the snapshot's
commit properties** (§6.4), atomic with the snapshot by every Iceberg
catalog's contract; and the registry's `duckspout.watermarks` row is a
**cached mirror**, written after the snapshot commit succeeds and
therefore always at or behind the authority, never ahead. WatermarkHonesty
survives because the mirror can only understate: a stale mirror makes a
`complete` read conservative (fail-closed refusal or a lower
`complete_through`), never falsely complete. `attach_info` (§6.4) tells
the extension which topology it is binding, so nothing on the critical
path is backend-exclusive (§6.4's neutrality rule).

`duckspout.watermarks` is authoritative but reconstructible: window manifests
ride every LakeCommit, so catalog recovery (PITR + recompute, §9) can rebuild it.
`nodes` and `claims` are pure soft state and are simply re-advertised.

### 7.4 Remote hot: Airport is the client, DuckSpout is the server

DuckSpout does not ship a query client. The client half of remote hot is the
**Airport extension** (Query.Farm): `ATTACH (TYPE AIRPORT)` mounts any Arrow
Flight server as a full DuckDB catalog, with predicate and projection pushdown.
The DuckSpout catalog extension issues these Airport attaches internally at bind
for each resolved holder; the user never sees them.

DuckSpout builds only the **Flight server half**: every node serves its hot
tables over Arrow Flight. This is the entire remote-hot protocol — no bespoke
RPC, no custom wire format. Rationale: Flight is the ecosystem's converged
columnar transport; Airport already solved discovery, catalog mapping, and
pushdown on the client side, and re-implementing that client inside the catalog
extension would duplicate a maintained project to no gain.

**Local hot is also served via Flight**, even on the same machine. DuckDB holds
a single-writer lock per database file; the ingesting daemon owns that lock, so
a querying DuckDB cannot open the hot database directly. Flight-over-localhost
is the fast path (no TLS handshake cost on loopback where configured, kernel
loopback throughput); one code path serves both local and remote, and the
authorization boundary (§7.9) is identical everywhere.

Pushdown honesty, stated normatively because query plans depend on it: Airport
delivers **best-effort single-table filter and projection pushdown only** — never
join keys, never semi-joins. Every join executes inside the querying DuckDB
(§7.7). Client-side filters are best-effort by Airport's own contract; DuckSpout
therefore never relies on pushdown for tenant isolation — that is enforced
server-side (§7.9).

### 7.5 The hot∪cold union

For each dataset, the catalog extension exposes one view whose shape is:

```
dataset = cold_branch(lake files ≤ complete_through)
          UNION ALL
          hot_branch(one holder per partition, > complete_through)
```

Mechanics, each load-bearing:

- **The watermark join prevents double-count.** Each branch is bounded by the
  per-partition `complete_through` read at bind: cold takes at-or-below, hot
  takes above. A row committed by LakeCommit between bind and scan does not
  double-appear, because the query is pinned to its bind-time watermark snapshot
  (§7.6). WatermarkHonesty (§3) guarantees the two branches tile the range
  exactly, with GapFreedom supplying its no-hole premise.
- **One holder per partition prevents cross-node duplication** (§7.2).
- **Per-branch generated projections, never `union_by_name`.** Schema evolves in
  two classes (§2): in-place lossless promotions and generation rebinds. The
  extension generates an explicit projection per branch — CAST-up to the current
  logical type, NULL-fill for columns a generation predates, COALESCE across
  generation columns — so that output types are a deterministic function of
  DuckSpout's schema lattice. `union_by_name` would delegate type resolution to
  DuckDB's coercion rules, which differ by version and can pick lossy widenings;
  a completeness-honest system cannot have its column types decided by whichever
  DuckDB the reader runs. (A raw ATTACH of the lake without DuckSpout shows
  generation columns un-coalesced; documented: raw is raw.)

### 7.6 Read concerns

Read concern is the completeness pillar's query surface. Two values, one axis:

| | `available` | `complete` (**default**) |
|---|---|---|
| Uncovered range (watermark below query range, holder unreachable, resolution impossible) | **Narrows silently**: returns what is reachable | **Throws a typed error** naming the uncovered cells: dataset, partition, range, `complete_through`, and which holder was unreachable |
| Meaning of an empty result | "nothing was reachable" — undecidable | "nothing exists in this range" — proven |
| Intended use | dashboards, exploration, degraded-mode operation | alerting, billing, anything a human or machine acts on |

```sql
SET duckspout_read_concern = 'available';  -- session-scoped opt-out
```

**Complete is the default and fails closed.** SQL's foundational flaw for
completeness is that "empty" and "couldn't check" collapse into the same zero
rows; an alert built on that collapse fires false negatives during exactly the
outages it exists to catch. Every system that lets availability silently narrow
results by default teaches its users to distrust empty sets. DuckSpout inverts
the default: silence is a proof, or it is an error.

**Per-transaction pinning.** The extension's transaction hooks pin, at bind, the
watermark snapshot and resolution used by the query, for the transaction's
lifetime. Data read and coverage claimed are therefore evaluated against the
same instant — without pinning, a watermark advancing mid-query would let a scan
read pre-advance data while reporting post-advance coverage (a
data-vs-coverage TOCTOU), violating WatermarkHonesty from the reader's side.
Multi-statement transactions get one consistent cut for free.

**The choke function.** Alerting on absence goes through one function:

```sql
SELECT duckspout_absent('ds.events', range_start, range_end);
-- true  ⇔  the range is empty AND range_end ≤ complete_through
-- error (typed) if coverage cannot be proven under concern 'complete'
```

`absent ⇔ empty ∧ covered` is the biconditional that makes "no data" a
positive claim; over a multi-partition range, "covered" means
`range_end ≤` the **minimum** per-partition `complete_through`. Alert
pipelines call this, not `COUNT(*) = 0`.

**Freshness disclosure.** `duckspout_freshness()` is a table function returning,
per referenced dataset: `complete_through`, `dimension_as_of` (changelogs,
§7.7), watermark age, and the laggard partition. A `complete` query gated by one
stalled dimension is correct but visible; this function is how the operator finds
the laggard in one call.

Declared loss (DeclareLoss, §5/§9) is the one sanctioned weakening: after the
ceremony, `complete_through` may advance past a permanently lost range, and the
loss-ledger row is queryable alongside the watermark. `complete` reads over an
annulled range succeed and are documented as post-declaration truth.

### 7.7 Querying changelogs and dimensions

Event datasets are the simple case. Changelog datasets (kind `changelog`, §2)
add latest-state semantics:

- **`<dataset>_latest`** is an auto-defined argmax view: latest row per declared
  key, planned from the newest covering snapshot part forward — cost
  O(snapshot + changelog-since-snapshot), not O(full history). This is the
  converged correct read over asynchronously-folded keyed state (the same
  argmax-over-versions pattern ClickHouse documents for replacing-merge tables).
  Tombstones (`_op = 'delete'`) make keys absent from the view.
- **`dimension_as_of`** — a changelog dataset's `complete_through` — is exposed
  as a queryable watermark column and via `duckspout_freshness()`. Under concern
  `complete`, a join gates fail-closed on **every** referenced DuckSpout dataset;
  effective freshness is min(`complete_through`) across them.

**Join execution reality, stated normatively so no one designs against a
fiction:** joins between event streams and dimension datasets execute entirely
inside the querying DuckDB. Airport pushes single-table filters and projections
only; the dimension is re-streamed over Flight on every query. Consequences:

- Direct per-query streaming is acceptable to roughly **32 MB** of dimension
  data (bracketed by the ecosystem's broadcast-join thresholds: Spark's 10 MB
  auto-broadcast, Pinot's 200 MB replicated-dimension cap).
- Above that, the documented pattern is **materialize-and-refresh**: `CREATE
  TEMP TABLE dim AS SELECT * FROM ds.dim_latest`, refreshed on a querier-side
  cadence (30–60 s suggested; the TTL is the querier's setting, documented, not
  a DuckSpout knob).
- For **reproducible** enrichment — the same query yielding the same rows next
  week — use DuckDB's native `ASOF JOIN` against the retained changelog itself,
  pure SQL over data DuckSpout durably keeps. Expensive over a long pre-snapshot
  changelog; audit and backfill workloads only.

**Two freshness modes, never silently mixed:** (A) latest-enrichment via
`<dataset>_latest` — freshness bounded by `dimension_as_of`, disclosed; (B)
reproducible AS-OF enrichment via `ASOF JOIN`. DuckSpout never injects an as-of
column into results and never generates a view that mixes the two modes; a
system that secretly chooses one produces joins whose meaning its users cannot
state. Event-time temporal-join machinery is rejected — DuckSpout does not own a
streaming join engine; it adopts the semantics (dimension staleness bounded by
the dimension-side watermark) without the mechanism.

### 7.8 Query guards, caching, and the cold path

Three per-query guards protect ingest from queries on every Flight server:

| Guard | Default | Semantics |
|---|---|---|
| `query.max_hot_bytes_per_query` | 2 GiB/node, **fill-scaled** | Byte budget per query per node; scales up with hot fill ratio during drain-stall, because when drains stall (§6, §9) hot is the *only* coverage and steady-state limits would kill querying exactly when it matters. |
| `query.hot_scan_deadline` | 30 s | Wall-clock backstop, enforced via scan interrupt. |
| `query.max_concurrent_hot_scans` | 8 | Node-sizing bound; outstanding-queue depth per principal is a fixed constant (32). |

A tripped guard is a **typed error, never truncation**. A silently truncated
result is a completeness lie with extra steps; the error carries which guard
tripped and the remediation (narrow the range, raise the knob, use the lake).
There is no row cap: rows are subsumed by bytes plus deadline.

**No result caching, anywhere in DuckSpout.** Result caches in this lineage
cover completed, aligned ranges; hot is definitionally the uncacheable moving
window — every second changes the answer. Revisit only with measured
repeat-query evidence, and then in a frontend layer, never in the node.

**The cold path bypasses DuckSpout entirely.** Post-drain reads go straight from
the querying DuckDB to the lake's object store; hot nodes serve only data that
entered through their own ingest. The cold accelerator is the **querier's own
DuckDB external file cache** (default-on since 1.3, memory-bounded) — DuckSpout
nodes never operate a cold-object cache. A node-side cache of lake bytes would
be re-shipped over Flight on every query while the querier could have cached the
same bytes locally after one object read; every deployed analytics-over-object-
store system (Snowflake warehouse-local cache, Presto/RaptorX worker caches,
DuckDB's EFC) put this cache at the compute, and DuckSpout follows. Cold objects
being immutable-with-expiry (§6) makes aggressive downstream caching trivially
coherent: a URL's bytes never change.

### 7.9 Authorization

**Identity.** Two channels, one principal: mTLS client-certificate SAN
(preferred) and bearer token (required — Airport is bearer-only today). Both
resolve to a single principal identity; unknown principals are always denied.
When Airport gains mTLS, bearer remains supported; the posture (deny-unknown,
one principal) is fixed, the channel is configurable.

**Grants.** Principals and grants live in catalog tables, cached in-daemon and
refreshed on Heartbeat cadence. During a catalog outage the last-loaded snapshot
**remains valid for the outage's duration — there is no staleness timer.** A
timer would make hot-query availability a function of catalog availability,
violating the data-path independence rule (§5, §11): already-resolved hot
queries must keep working through a catalog outage. The disclosed cost is
revocation lag bounded by the outage length — disclosed, not manufactured into
an outage-triggered denial of service.

**Tenant enforcement is server-side, always on.** The Flight server conjoins a
tenant predicate (`tenant_id IN <principal's granted set>`) into every scan,
keyed off the authenticated principal and bound to the ingest-time tenant
column; catalog metadata visibility is filtered the same way. This is the
row-filter model (Trino/OPA lineage); client-side filters cannot be the
enforcement point because Airport's pushdown is best-effort by its own contract.
Single-tenant deployments are not a special case: one principal granted `*`,
same code path.

**Cold-path honesty.** DuckSpout cannot enforce anything on a querier scanning
the object store directly — and says so. What it does is *enable* enforcement:
parts are tenant-pure under per-tenant prefixes (a hard layout invariant, §6),
so prefix-scoped IAM policies give per-tenant cold-side isolation; reference
policies ship in the operations guide (§9). A system that claimed to enforce
cold-side tenancy it cannot see would be selling a fiction; this one draws the
boundary where it actually is.

**Catalog-side least privilege.** Two Postgres roles: `duckspout_daemon`
(read-write on registry and lake catalog; sole creator of DuckSpout objects,
verified at install) and `duckspout_reader` (SELECT-only, excluding the
principals/grants tables). The reader DSN is what goes in `~/.duckdbrc`; a
compromised querier credential can read routing metadata, not rewrite watermarks
or harvest the grant table.

## §8 Verification End-to-End

DuckSpout's promises are the kind that fail silently: an ack that wasn't durable, a
watermark that overstates coverage, a drained window that quietly double-committed.
None of these produce a stack trace. The verification stack therefore runs from an
abstract model down to real fleets on real backends, and every tier carries **teeth**
— a mechanism proving the tier itself can still reject a wrong answer. A checker that
has never rejected anything is indistinguishable from a checker too weak to reject
anything; DuckSpout treats that as a permanent design constraint on the verification
stack, not a slogan.

Verification grows in-milestone, never trailing (§12): a pillar's mechanism and its
verification land in the same release, because a guarantee that ships before its
check is a claim, not a guarantee. Three postures govern every tier:

1. **Skipped ≠ passed.** A gate that cannot run — missing backend, absent endpoint,
   vanished subject — fails, it does not shrug. Ambiguity fails closed (§11).
2. **CI recomputes, never trusts.** Every floor, count, and threshold is recomputed
   independently on each change; a PR's own claim about a number is never the input
   to the gate that checks that number.
3. **Every check bites, provably.** Each property ships alongside a deliberately
   broken variant that MUST fail; each judge carries vacuity rules that downgrade a
   run that exercised nothing to "no verdict," which is never a pass.

The tiers, top to bottom:

| Tier | Subject | Runs | Catches |
|---|---|---|---|
| TLA+ / TLC (§8.1) | The §3 action system, bounded exhaustive | Every push | Protocol design errors |
| Trace conformance (§8.2) | Real implementation vs. the model, step by step | Every CI run, fresh trace | Implementation drift from the model |
| CTK in-memory (§8.3) | Core library through engine-neutral adapters, injected faults | Every CI run | Fault-path logic errors; port-contract violations |
| CTK distributed (§8.4) | Real fleets, real MinIO + Postgres, chaos windows, post-pass judges | Nightly + release | End-to-end invariant violations under real failures |
| Property tests (§8.5) | Algebraic laws and codecs | Every CI run | Determinism and lattice-law regressions |
| Ratcheted floors (§8.6) | Coverage, mutation kill rate, instruction counts | Every PR | Erosion of the test suite itself; perf regressions |
| Bench card + durability audit (§8.7) | Published performance under durable settings | Nightly + release | Throughput-without-durability dishonesty |

### 8.1 The TLA+ tier

The §3 specification is not documentation — it is checked. TLC runs bounded,
exhaustive model checking of the action system on **every push**, so the models are
deliberately kept small enough for per-PR CI: small node counts (2–3), small window
counts, small per-partition sequence bounds, with the module header of each model
stating why that grain is sound rather than asserting it. Each model's reachable
state count is pinned in the repository; an unexplained change in the pinned count
fails CI, because a state space that silently shrank is a model that silently
stopped exploring something. Feature-gated model extensions (e.g. the cache class,
changelog snapshots) are wired so the disabled configuration's state count is
**checkably** identical with and without the extension's variables and actions —
"the extension changes nothing when off" is verified, never argued.

Checked per push, against every reachable state of the bounded models:

- **Safety**: DurableAck, NoAckedLoss, WatermarkHonesty, CacheTransparency,
  GapFreedom, SingleDrainCommit, FencedZombie, LossLedgerTruthful,
  SnapshotCovered, LatestViewCorrect, and the LadderMonotone action property
  (§3 defines each).
- **Liveness**, under stated fairness assumptions: EveryRequestResolves (every
  Accept eventually reaches ClientAck, Throttle — including the client's own
  timeout — or Refuse) and WatermarkEventuallyAdvances (with the catalog
  reachable and at least one live claim holder, LakeCommit eventually fires and
  the watermark moves).

**The armed-broken-variant convention.** Every checked safety invariant ships a
deliberately broken sibling — a one-clause guard perturbation — that MUST
reproduce its own violation on every run, and liveness is armed through the
suppression variant and the FINDINGS; §3.6's table is the definitive armed set.
Examples: a DurableAck variant where ClientAck may precede the last Receipt; a
SingleDrainCommit variant with the uniqueness guard on (partition, window_id,
part_kind, discriminator) removed; a FencedZombie variant where a pre-crash
incarnation's LakeCommitOk is accepted after RecoverNode. If a broken variant
ever goes green, CI fails: the check stopped biting, whether because the model
drifted, the property was weakened, or TLC's configuration quietly stopped
exploring the states the violation lives in. The broken variants are the tier's
teeth; they convert "the model passes" from an absence of evidence into evidence.

**Non-vacuity witnesses.** A property over states the model never reaches is
vacuously true. Every quantified property therefore carries witness assertions —
reachability claims TLC must confirm. **The definitive armed witness set is
§3.6's table**; representative members: a Forward's Receipt outstanding at
ClientAck-decision time (`Witness_ReceiptOutstandingAtAck`); TakeoverDrain
actually landing a dead owner's window (`Witness_TakeoverCommits`); Throttle and
Refuse each taken (`Witness_ThrottleAndRefuseTaken`); a DeclareLoss refused over
a live replica (`Witness_LossRefusedOverLiveReplica`); EvolveSchema interleaved
with an in-flight drain (`Witness_SchemaWidensInFlight`). A witness that becomes
unreachable fails CI even though every property still "passes."

**Liveness FINDINGS — permanently red on purpose.** Some liveness properties
DuckSpout deliberately does not have, and the honest way to keep that fact visible
is to keep checking them. **The authoritative FINDINGS set is §3.5's table — five
members, exactly**; this tier runs each in a dedicated TLC configuration that MUST
fail, with the design rationale in the model header. A FINDING going green fails
CI exactly like a broken variant doing so: either the model was changed to promise
something the system does not deliver, or the system grew a behavior nobody
designed. Both demand a human decision, not a green tick.

### 8.2 Trace conformance

The models constrain designs; trace conformance constrains the code. Every
subsystem that can emit a trace does, using **exactly the §3 action vocabulary** as
its event names — Accept, DedupCheck, StageCommit, Throttle, Refuse, ClientAck,
ClientTimeout (journaled by the load generator, §3.7), Forward, PeerApply, Receipt,
SealPart, PutPart, LakeCommitOk, LakeCommitAbort, LakeCommitIndeterminate (the
model's Landed/Lost split is unobservable to the emitter; the following Reconcile
names the outcome), Reconcile, Demote, Evict, DropWindow, SnapshotSeal, Expire,
TakeoverDrain, ClaimAdvertise, Heartbeat, FenceBoot, DegradedBoot, DeclareLoss,
EvolveSchema — CrashNode and CrashWipe are environment events, not journaled
(§3.7) — so a recorded trace is directly a candidate behavior of the model, no
translation layer whose own bugs could mask drift. The trace-refinement specs (§3) accept a
finite recorded trace iff the model has a matching execution and the trace is
complete: every step that must have happened was recorded, and no recorded step is
one the model has no transition for.

Two checks per traced subsystem, deliberately not collapsed into one:

**Self-test against doctored fixtures — the tier's teeth.** A committed set of
static fixtures: one real recorded trace that must conform, plus doctorings of it
that must not. Each doctoring deletes a recorded step **four different ways**, and
the harness asserts **which mechanism rejects each** — one tooth going blunt cannot
hide behind another:

| Doctoring | Rejecting mechanism |
|---|---|
| Delete a mid-trace StageCommit whose ClientAck was recorded | Refinement deadlock: a recorded step that could not have happened |
| Delete the trailing LakeCommitOk after a recorded SealPart + PutPart | TraceComplete invariant: a step that happened and was not recorded |
| Delete a Receipt the ClientAck's RF accounting depends on | Refinement deadlock, at the DurableAck decision point |
| Delete the trailing ClientAck itself | TraceComplete invariant, resolution clause |

For deadlock rejections the harness reads the counterexample's own halt cursor and
asserts *which* recorded entry the walk halted at; for TraceComplete rejections it
asserts *which* clause fired, since the checker names the invariant, never the
conjunct. The self-test catches regressions in the **checking mechanism** — the
refinement spec, the trace decoder, the harness — which static fixtures are exactly
the right instrument for, and real-code regressions not at all, which is why the
next check exists.

**Live generation on every CI run.** A fresh trace is generated on every single run
from the real implementation: a multi-node in-process harness (§8.3's adapters)
runs a real ingest → replicate → drain → query cycle — concurrent writers, a forced
takeover, at least one EvolveSchema — records the trace, and checks it against the
refinement specs. This is what actually catches a regression: a static fixture
captured once can only ever certify the code as it was on capture day, while a
fresh trace certifies the code in the PR. A live-generated trace that the model has
no execution for is a red check naming the first unmatched step.

**The real-backend variant.** The same generation-and-check runs against live MinIO
(S3 API) and live Postgres (catalog) in CI containers. A trace from the in-memory
double and a trace from real backends are different inputs to the same model:
different timing, PutPart outcomes the test process does not decide, and — the part
no double reproduces faithfully — real **Indeterminate** resolution, where a
timed-out LakeCommit must be resolved by reading the catalog back rather than
assumed either way. Static fixtures would go stale the moment an adapter changed,
so this variant doctors the trace it just generated, the same four ways, and
requires each rejection to come from the mechanism it was aimed at. The generator
skips gracefully for a contributor without Docker; **the CI gate does not inherit
that skip** — with the endpoint absent, the gate fails rather than reporting a
green run that checked nothing.

### 8.3 CTK, in-memory tier

The Conformance Testing Kit is DuckSpout's own test harness family, published as a
crate so that what verifies DuckSpout also verifies third-party extensions. The
in-memory tier drives the core library through **engine-neutral adapters** — the
kit speaks to a trait surface, never to concrete internals — so a run certifies the
contract, and any conforming implementation can be substituted under the same suite.

**Fault-injecting backends.** Every external effect the core takes goes through an
injectable seam: fsync faults (fail, torn write, fsync-reports-success-then-loses
on simulated power cut), S3 faults (PutPart timeout, 500, slow-then-success,
Indeterminate), catalog faults (LakeCommit timeout, serialization failure, outage
window), peer faults (Forward drop, Receipt delay, duplicated PeerApply). Each
injector keeps a ledger of faults armed and faults fired; a schedule that armed
faults and fired none certifies nothing and the run is reported as such (the same
vacuity discipline as §8.4).

**Deterministic seed-sweep property tests.** The kit runs randomized fault
schedules from a seeded PRNG, sweeping seeds in CI; every run is reproducible from
its seed alone. An in-process oracle judges each run: no journaled ClientAck maps
to unreadable data, no gap admitted past DedupCheck, every ambiguous outcome
resolved before retry, LadderMonotone over the journaled status transitions. A
failing seed is captured and pinned as a permanent regression case — flakes are
bugs, fixed at the root, never hidden behind a retry or a loosened assertion.

**Concurrency exploration.** Where the core's lock-order and atomicity claims are
small enough to model-check at the code level (the staging-commit path, the
dedup-window transaction, the claim-advertise state), loom-style exhaustive
interleaving exploration runs in CI over miniature configurations. It complements
the TLA+ tier: TLC explores the protocol's interleavings, loom explores the Rust
memory-model interleavings of the code implementing one action.

**Published port-conformance suites.** Every port gets a conformance suite a
third-party implementation runs against itself: the LakeCommitter contract
(atomic {EvolveSchema-state, add-files, watermark} commit; commit idempotence
under retry; refusal of add-before-evolve; SingleDrainCommit uniqueness semantics —
this is how the Iceberg committer proves itself equal to the DuckLake one, §6) and
the accept-adapter contract (canonical decode, DedupCheck key derivation
determinism, partial-success semantics, §4). "Iceberg by contract" means exactly
this: the contract is executable, and passing it is the definition of conforming.

### 8.4 CTK, distributed tier

The in-memory tier can inject any fault but cannot produce real timing, real
networks, or real crash semantics. The distributed tier runs **real multi-node
fleets against real MinIO and Postgres**: a fleet runner provisions nodes, drives
sustained load through real OTLP/Arrow Flight ingest, executes a fault schedule,
and journals everything; a separate **judge binary** runs as a post-pass over the
journals plus the final backend state and produces the run's verdict. Judging from
journals after the run — rather than asserting in-line — is what lets the fleet
misbehave freely during the run and still be convicted precisely afterward.

**Fault windows** in the standard schedule (each window journaled with start/end):

- Node kills, including the sharpest one: the partition owner mid-drain, between
  PutPart and LakeCommit — the window where SingleDrainCommit and TakeoverDrain
  are both live.
- Network partitions and asymmetric degradation (drops, delay, bandwidth caps).
- Process pauses (SIGSTOP long enough to expire claims, then resume — the
  FencedZombie scenario: the paused node's stale incarnation must be rejected).
- Membership churn: join and leave under load, not only crash — the fault class
  most implicated in published acked-loss incidents elsewhere, so it is in the v1
  schedule, not deferred.
- Flight-server kill mid-stream (a hot query's stream dies; the client's typed
  error, never a silently truncated result — §7).
- Catalog outage windows (ingest must continue undegraded; drains stall and
  disclose — §4, §9).
- Discovery flapping (ClaimAdvertise/Heartbeat oscillation; routing must converge
  without ever serving a `complete` answer it cannot prove).

**Journals.** Every node journals its events durably and locally in the §3 action
vocabulary, before the corresponding external call where the predicate demands it
(an attempt with no journaled resolution, or a resolution with no journaled
attempt, is itself a finding). The load generator is a first-class fleet member: it
journals every request sent and every ClientAck received, with payload identity —
the verifying client is part of the test, not an afterthought.

**Judges.** Each judge is a predicate over journals plus read-back state:

- **Zero-acked-lost (the W-shaped judge, write-side):** every record whose
  ClientAck the load generator journaled must be present in the final system —
  queryable from hot or lake — regardless of what the fault schedule did.
  System-class datasets are excluded by definition: they receive no durable acks,
  so there are no acks to lose (§2).
- **Watermark honesty (the Q-shaped judge, query-side):** claimed vs. served —
  every watermark value any node ever advertised is replayed against the journals:
  no record acked before that watermark may be missing from a `complete` read at
  it, and no `complete` read may have been served over coverage the journals show
  did not exist at serving time. Fail-closed refusals are correct outcomes;
  optimistic answers that happened to be right are still violations if coverage
  was unproven.
- **Per-key order and latest-view correctness (changelog datasets — the §3
  invariant LatestViewCorrect, judged end-to-end):** for every key, the served
  latest view equals the fold of that key's acked changelog in (origin, seq)
  order, across takeover and snapshot rollover; tombstones delete.
- **Retention honesty (Keep Rule 10 — SnapshotCovered, §3):** replayed from the
  journaled `Expire` events against read-back state: no expired changelog part
  lacks a committed snapshot covering its arrival range, and no acked record's
  last value became unreachable through expiry.
- **Cache transparency under eviction storms:** with forced Evict/Demote churn and
  DropWindow racing queries, every `complete` answer is a function of staging ∪
  lake alone — any two cache states, including empty, yield the identical row set.
  This judge is the mechanical discharge of §2.4's read-answer equivalence — the
  half of the cache-transparency theorem the §3 lemma deliberately does not carry
  (§3.4), including obligation (c): no Evict-held lock ever blocks a read.

**Vacuity teeth.** A judge that never rejects anything is indistinguishable from
one too weak to reject anything, so the verdict is three-valued and the exit codes
are distinct: **Pass** (0), **Violation** (2), **NoVerdict** (3) — and NoVerdict is
never a pass. NoVerdict rules include: a fault schedule that armed faults and fired
none (measured from each injector's own ledger, not assumed from the profile); a
run with no observed cross-node contention when contention is what the run exists
to certify; an ambiguous-outcome fraction above the profile's ceiling; a node whose
journals simply stop (a vanished machine is exactly the under-reported-loss shape,
so it accuses nothing and certifies nothing). Additionally, each judge is
periodically run against a **seeded-violation replay** — a journal set with a known
injected violation — and must convict it; a judge that acquits its own seeded
violation fails CI.

The distributed tier runs nightly and gates releases; it is too heavy for per-PR.

### 8.5 Property tests

Algebraic laws the design leans on are tested as laws, not as examples, with
shrinking on failure and every failing seed pinned as a permanent regression case:

- **Codec round-trips**: OTLP and Arrow decode→canonicalize→encode round-trips;
  journal and manifest serialization stability across versions.
- **Lattice laws**: the schema-widening join is commutative, associative, and
  idempotent — the property that makes EvolveSchema crash-retry and
  concurrent-owner convergence correct (§6); tested over generated type pairs,
  including the JSON terminal.
- **Dedup determinism**: DedupCheck key derivation is a pure function of
  (tenant, content | idempotency token); equal inputs collide, unequal inputs
  don't, across process restarts.
- **Ring exactness**: the HRW placement function's minimal-disruption property is
  tested exactly, not approximately — adding or removing one node reassigns only
  the partitions that node gains or loses, and nothing else moves (§5).
- **Per-signal natural-key dedup**: drain-time winner selection is deterministic
  (smallest (origin, seq)) and produces identical sealed parts from any arrival
  permutation of the same acked set (§6).

### 8.6 Measured, ratcheted floors

Quality gates are **checkable numbers recomputed by CI on every change** — never
trusted from a PR's description, never carried forward from a cached value.

- **Coverage floor** and **mutation-testing floor** (pinned tool versions, pinned
  operator sets): CI recomputes each and fails below the recorded floor. Raising a
  floor is an ordinary commit; lowering one is a reviewed, named decision in the
  commit message — never "adjusted threshold."
- **Per-PR performance gates**: deterministic instruction-count measurements of the
  hot paths (Accept→ClientAck, DedupCheck, drain fold), gated at **baseline +15%**,
  where the baseline moves only via explicit baseline-update commits. Instruction
  counts are deterministic enough to gate per-PR where wall clocks are not; their
  known blind spot — fsync and I/O wait — is documented and covered by the nightly
  wall-clock bench card (§8.7), which blocks release on regression.
- **A 1M-record ingest smoke bound** per PR, catching order-of-magnitude
  regressions cheaply.

**The anti-gaming stance** (§11 makes it a Keep Rule; this section is its
enforcement surface): no change may weaken, disable, or self-lower a gate under any
framing — "temporarily," "to unblock." A diff touching a threshold, an allowlist,
a lint suppression, or a timeout is reviewed line by line, never accepted on its
own explanation. Where a mechanical check cannot decide whether something violates
a rule, it treats it as if it does — **ambiguity fails closed** — and narrowing a
check's scope to dodge the ambiguity is the same offense as weakening the check. A
check whose subject vanished **fails, it does not shrug**: a mutation floor whose
crate was renamed, a trace gate whose generator test was deleted, a broken variant
that no longer compiles — each is a red check demanding a decision, because a gate
that silently stopped measuring anything reports exactly the same green as a gate
that measured everything.

### 8.7 The bench card and the durability audit

Published performance is a correctness claim about honesty: the field's recurring
failure mode is throughput-without-durability — big numbers measured with acks that
promise nothing. Every DuckSpout headline number is measured at **RF=2 with durable
acks** (fsync + replication receipts before ClientAck, §4–§5). RF=1 or no-fsync
numbers are **never published alone**; where shown for context they appear beside
the durable figure, labeled.

**The nine-metric bench card**, every metric mapped to a pillar or a hard rule:

| # | Metric | Floor (published only when beaten) |
|---|---|---|
| 1 | Ingest throughput per signal, per node | logs ≥200k rec/s · spans ≥150k/s (provisional — see note below) · datapoints ≥300k/s |
| 2 | Ack latency (durable) | p99 ≤25 ms, concurrency disclosed |
| 3 | Queryable-hot lag (ClientAck → visible to SQL) | p50 ≤100 ms |
| 4 | Hot-query latency under 80% ingest load | reported, trend-gated |
| 5 | Drain write amplification | →1.0 PUTs per sealed part (§6's one-PUT rule, measured) |
| 6 | Owner-kill recovery (kill → TakeoverDrain → coverage restored) | ≤15 s |
| 7 | Catalog-outage continuity | 0% ingest degradation through a 30-min outage |
| 8 | Chaos invariant score | pass/fail: zero acked-lost, zero `complete` violations (system-class datasets excluded by definition) |
| 9 | Resource envelope (CPU/RSS/disk at target load) | reported, trend-gated |

Span-throughput has no published industry comparable; the floor is re-derived from
the first internal measurement before any external announcement.

**Disclosure norms**, adopted wholesale from ClickBench's: one disclosed hardware
class (16 vCPU / 32 GB, local NVMe — never network gp2-class volumes, because
fsync is the critical path — 3 nodes for cluster runs), full configuration
published, deviations labeled. Every throughput number states its RF, fsync mode,
and active chaos schedule. Chaos runs **inside** the standard benchmark run, not in
a separate friendlier universe: metric 8 is measured on the same run as metrics 1–4.

**The durability audit** is a Jepsen-style self-run harness, and its defining
discipline is that the verifying client is the test: the load generator journals
**every ack it receives**. After the chaos schedule — node kills including
owner-mid-drain, pauses, partitions, and **membership churn**, which is in the v1
schedule because it is the fault class real-world acked-loss reports implicate —
and a fixed heal window (set in the methodology document before the first run,
never tuned to the result), every journaled-acked record must be queryable at read
concern `complete`. The changelog scenario additionally asserts per-key order and
latest-view correctness through churn and takeover (§8.4's judges reused verbatim
— the audit is a CTK-distributed profile, not separate machinery). The full
methodology and raw journals are published with each report. An external Jepsen
engagement is planned post-1.0, after the self-run harness and at least one public
report exist — an external audit is worth buying only once the internal one has
stopped finding things.

## §9 Operations

DuckSpout runs as a fleet of identical daemon processes (§10) with local
NVMe-backed storage, advisory peer discovery (§5), and lake commits through the
catalog database (§6). This section is the operator's contract — deploy, size,
watch, recover, secure — plus the complete, closed configuration surface.
Nothing operational hides outside it.

### 9.1 Deployment

#### 9.1.1 Kubernetes (reference deployment)

DuckSpout ships as a **StatefulSet + headless Service + one PVC per pod**. The
StatefulSet's use-case list — stable network identity, stable per-replica
storage, ordered-enough lifecycle — is DuckSpout's requirement list verbatim,
which is why no operator/CRD layer exists.

| Choice | Value | Rationale |
|---|---|---|
| Workload | StatefulSet | stable identity + per-replica PVC is exactly the node model of §2 |
| Service | headless | peers dial each other directly; no per-request kube-proxy hop on the replication path (§5) |
| Volume | PVC per pod, `ReadWriteOncePod` (fallback `ReadWriteOnce`) | one daemon per volume is a hard assumption of the fsync path (§4); RWOP makes the kubelet enforce it |
| `podManagementPolicy` | `Parallel` | nodes have no boot ordering dependency on each other (FenceBoot orders against the catalog, not against peers); serial rollout would only slow recovery |
| Node identity | the pod name | stable across restarts, unique within the namespace; the incarnation half of identity comes from FenceBoot (§9.4.4), never from Kubernetes |
| Zone awareness | `topologySpreadConstraints` on the failure domain + the HRW candidate filter (§5) keyed on `node.failure_domain` | placement diversity must hold at both layers: the scheduler spreads pods, the ring spreads replicas |

Zone awareness is **automatic when ≥2 failure domains are visible** and carries a
single boolean escape hatch (`cluster.zone_aware`, default `auto`) — a tri-state
was rejected because "on" with one visible domain is meaningless. When no
cross-domain replication candidate exists, the ring falls back to any peer:
availability over placement.

The daemon has **no Kubernetes API dependency**: the failure domain comes from
config (populated via the downward API on K8s), peers from the registry, and
nothing from the apiserver. This subsection is packaging, not architecture.

#### 9.1.2 Rolling upgrades

Upgrade choreography lives **in the daemon behind SIGTERM**; the pod's `preStop`
hook is a thin delay, nothing more. This keeps the same choreography correct
under systemd (§9.1.3) and under a plain `kill -TERM`.

On SIGTERM the daemon performs a **shallow drain**:

1. Fail readiness — the Service stops routing new ingest to this node.
2. Finish in-flight requests: complete StageCommit for accepted batches, flush
   Forward traffic, collect outstanding Receipts, issue remaining ClientAcks.
3. Write an advisory `draining(restart, expected_back_by)` row to the registry.
4. Exit cleanly.

A shallow drain hands responsibility to **the PVC and the replicas** — it never
final-drains staged data to the cold tier. Draining hot data through SealPart /
PutPart / LakeCommit on every rollout would turn a routine upgrade into a bulk
cold-tier write and couple upgrade speed to catalog availability; the fsynced
local state plus RF peers (§5) already guarantee nothing is lost across the
restart. This is the converged practice of replicated stores that roll shallow
rather than rehydrate (Mimir, Strimzi-managed Kafka).

Two guards bound the blast radius:

- **PDB `maxUnavailable: 1`.** With RF=2 (default), two simultaneously absent
  nodes can intersect a partition's entire replica set; the budget makes the
  scheduler respect what the replication math requires.
- **Takeover suppression = 2× `terminationGracePeriodSeconds`, derived, not a
  setting.** The `draining(restart)` row suppresses TakeoverDrain for a window
  long enough to cover a slow restart but short enough that a node that dies
  mid-rollout is still taken over. Deriving the window from the one value the
  operator already tuned removes a knob whose two failure modes (too short:
  spurious takeovers every rollout; too long: a real death goes unhandled)
  would otherwise have to be re-balanced by hand (protocol statement: §5.10).
  The suppression-expiry model is checked in §8 (a deliberately-broken never-expires variant must fail).

A rollout that collides with a catalog outage does not wedge: nodes holding a
persisted incarnation boot into replica-only degraded mode and resume full duty
after FenceBoot completes (§9.4.4); only genuinely new nodes wait for the
catalog.

#### 9.1.3 Non-Kubernetes (first-class, not a footnote)

The same binary, the same SIGTERM choreography, the same config file:

- **systemd**: `Type=notify` (readiness = the daemon's own readiness, not a
  fork guess), `LoadCredential=` for TLS keys and the catalog password —
  secrets reach the process as files with correct permissions, never as
  environment variables or command-line arguments.
- **Bootstrap**: `cluster.seed_peers` (static address list) seeds the advisory
  peer view, exactly as etcd's `initial-cluster` does; the registry supersedes
  the static list as soon as Heartbeats flow. Empty by default — on K8s the
  headless Service DNS makes it unnecessary.
- **docker compose**: shipped, marked **dev-only**. It demonstrates topology;
  it does not demonstrate durability (compose gives no per-node fault
  isolation worth the name).
- **Bootstrap ordering**: the catalog database must exist before the first
  node of a *new* cluster starts — FenceBoot mints the first incarnation from
  a catalog sequence. The install doc states this sequence explicitly for the
  systemd/compose path.

#### 9.1.4 Edge load balancing

The default edge is a **plain L4/L7 load balancer** (a normal Service on K8s).
Because any node accepts any write and forwards to the HRW owner (§4, §5),
balance is a latency concern — at most one extra Forward hop — and never a
correctness concern. The otel-collector `loadbalancingexporter` can make routing
ownership-affine and is documented as an optional optimization, **off by
default**: its alpha/beta maturity alone disqualifies it as a dependency of the
default path.

### 9.2 Capacity: sizing the volume

Provision each node's volume by:

```
pvc = (rate × expansion × residency × RF ÷ nodes + staging) ÷ 0.6
```

| Term | Meaning |
|---|---|
| `rate` | cluster-wide wire ingest, bytes/s |
| `expansion` | wire→hot representation factor (columnar + indexes + dedup window; measure, start at 1.4) |
| `residency` | the **drain-stall budget in seconds** — how long you want to ride out a stalled cold tier (catalog outage, object-store outage) without throttling. Floor: `drain.max_age`. Production recommendation: ≥2h |
| `RF` | `cluster.rf` — staged bytes exist RF times cluster-wide (§5) |
| `staging` | fixed per-node scratch: part assembly, dedup window table, storage-engine WAL overhead (start at 20 GB) |
| `÷ 0.6` | the log-store utilization convergence (Kafka's and Elasticsearch's disk-watermark guidance): run at 60–70% so takeover (§5) has somewhere to land |

**Worked example.** 50 MB/s cluster wire ingest, expansion 1.4 (→ 70 MB/s hot),
residency 7200 s, RF 2, 3 nodes:

```
(70 MB/s × 7200 s × 2 ÷ 3 + 20 GB) ÷ 0.6 = (336 + 20) ÷ 0.6 ≈ 593 GB → provision 600 GB
```

From that volume, `hot.max_bytes` defaults to 75% = 450 GB; the fixed ladder
(§4) puts the soft threshold at 360 GB and the throttle threshold at 427.5 GB of
**staged** bytes. During a full
drain stall this node accrues staged data at ~47 MB/s (70 × 2 ÷ 3), giving
roughly two hours from near-empty to the soft threshold and a further ~24
minutes to Throttle — the 30-minute-catalog-outage target (§8) is met with wide
margin. `duckspoutctl size` computes all of this from the same inputs.

Two alerting rules, and only two, on capacity:

- **Alert on `staged_bytes` at the soft threshold** (80% of `hot.max_bytes`).
  This is the single capacity alert; there is deliberately no freestanding
  disk-percentage alert. Staged bytes is the only quantity that cannot be
  reclaimed without violating NoAckedLoss and the only one that predicts write
  refusal — the same reason InnoDB keys flushing on dirty-page fraction, not
  buffer-pool occupancy.
- **Do not alert on volume fullness.** Once the cache class is live (§2, §12.7),
  near-full is the *designed* steady state: cache is residual and Evict
  reclaims it on demand (§4's rung 0). A disk-percentage alert would page
  forever on a healthy system.

When the soft alert fires without a disclosed drain stall, the designed
responses are volume expansion or adding nodes; the ladder (§4) is the
backstop, not the plan.

### 9.3 Self-observation

#### 9.3.1 The `_self` system tenant

The daemon's own telemetry is ingested into the reserved `_self` tenant through
a deliberately weaker path than customer data:

- **No durable acks.** `_`-prefixed tenants never receive ClientAck with
  durability semantics, so DurableAck and NoAckedLoss (§3) are not implicated —
  the Keep Rules (§11) are scoped to non-system tenants rather than carved out.
- **One-hop amplification rule.** Telemetry about handling `_self` writes is
  never itself ingested; amplification is structurally ≤2.
- **Lossy bounded queue** with a `duckspout_self_dropped_total` counter — the
  only shedding anywhere in the system, and it is named as such in §4's ladder
  text.
- **Built-in short retention class**, not a knob.

This is the ClickHouse system-tables pattern: self-observation must be unable
to compete with, amplify, or deadlock the data path it observes.

#### 9.3.2 The observation listener

A **dedicated listener** (separate port from ingest, Flight, and peer traffic)
serves:

- `/healthz` — liveness.
- `/readyz` — readiness, reported **per pillar**: ingest accepting, replication
  at RF, drain advancing, query serving.
- `/metrics` — Prometheus exposition.

All three read **in-process atomics only** — no DuckDB query, no catalog
round-trip, no lock shared with the data path. A monitor must not share its
subject's failure domain; a health endpoint that queries the database it is
reporting on turns every database stall into a monitoring outage exactly when
observation matters most.

The status surface is the **closed enum**
`normal | staging_pressure | drain_stalled | throttling | refusing_ingest`
plus the orthogonal `replication_degraded` boolean, exposed **identically**
on the health endpoint, the metrics, and the registry (see also §9.5). One
vocabulary, three transports, zero drift — a closed enum is also what the
§3 monotonicity properties and the §8 chaos judge assert over.

#### 9.3.3 The blindness principle and the two external probes

A process cannot observe its own death, and a cluster cannot observe its own
partition from the inside. Self-observation is therefore paired with two
**external black-box probes**, shipped as recipes (config + query, versioned
with the release) that the operator runs outside DuckSpout's failure domain:

1. **Pillar canary** (page-severity): write a record to the reserved `_canary`
   tenant, then read it back **via Flight, addressed to node endpoints
   directly** — not through catalog-mediated binding. `_canary` writes
   traverse the full pipeline shape — Accept → StageCommit → Forward →
   Receipt — with the system-tenant exception that no durable ClientAck is
   ever issued (§2.2): replication runs, the promise is not made. The
   read-back is therefore a **hot-reachability probe**, not a completeness
   proof — a direct-Flight read has no bind-time watermark resolution to
   enforce `complete` against (§7.1), and claiming completeness for it would
   over-claim. It exercises accept, staging, replication, and queryable-hot
   end to end, and it **stays green through a catalog outage**, because a
   catalog outage is a disclosed, designed condition (`drain_stalled`,
   §9.4.1) and must not page as a pillar failure. `_canary` follows the same
   system-tenant rules as `_self`.
2. **Drain-freshness probe** (lower severity): watermark age per partition,
   read from the registry. This is the probe that *does* observe a stalled cold
   tier — at ticket severity, with the runway math of §9.2 telling the operator
   how long the condition is safe.

The split exists because a single canary either pages through every disclosed
drain stall or silently over-claims that a hot-coverage read proves watermark
advancement. It proves neither less nor more than each probe states.

### 9.4 Failure runbook (condensed)

Each entry names the §3 machinery it leans on. The full procedures ship as
operator docs; this is the normative shape.

#### 9.4.1 Catalog database outage

The data path never touches the catalog (§11, Keep Rule): **ingest, replication,
and already-resolved hot query service continue; drains and new bind-time
resolution pause, and say so.** No timer ever escalates a catalog outage into
anything else; it rides the ordinary ladder (§4).

Representative timeline for the §9.2 example cluster:

| T | State |
|---|---|
| T+0 | Catalog unreachable. Status → `drain_stalled` everywhere. Pillar canary green; drain-freshness probe opens a ticket. LakeCommit paused ⇒ watermarks freeze (WatermarkHonesty holds: `complete` reads still answer, bounded by the frozen watermark). Demote freezes with it, so the cache class drains toward zero as staging grows — the node sheds query acceleration to protect ingest durability, automatically. |
| T+0…T+2h | `staged_bytes` accrues at the drain-stall rate (~47 MB/s/node in the example). Grants and registry snapshots loaded before the outage remain valid for its duration — hot queries never fail on a catalog-availability timer. Clients with no cached binding cannot resolve new queries; this is disclosed, not worked around. Nodes that restart with a persisted incarnation boot replica-only degraded (§9.4.4); only brand-new nodes wait. |
| ~T+2h | Soft threshold: operator alert fires; status still `drain_stalled` (disclosure rung). Compute remaining runway: `(0.95 × hot.max_bytes − staged_bytes) ÷ accrual_rate`. |
| ~T+2h25m | Hard-approach: Throttle (UNAVAILABLE + RetryInfo, growing delay); the recommended collector edge config (§4) absorbs this in persistent queues. Status → `throttling`. |
| Hard | Refuse new writes; new-range replication refused so origins ring-walk to substitutes (§5). Status → `refusing_ingest`. NoAckedLoss holds throughout — nothing acked is ever shed. |
| Recovery | Catalog returns. Drains resume, LakeCommit advances watermarks (WatermarkEventuallyAdvances), the ladder unwinds as M falls back through the thresholds, Demote resumes, waiting new nodes complete FenceBoot. No operator action required at any point. |

#### 9.4.2 Loss beyond RF−1: the DeclareLoss ceremony

When every replica of a partition range is gone, the watermark freezes
(GapFreedom forbids advancing over a hole) and **only a human can move it**.
DeclareLoss is deliberately ceremonial:

- The operator names **exact ranges** and passes `accept_data_loss: true`.
- The command writes a **permanent `loss_ledger` row in the same catalog
  transaction as the watermark advance** — the loss and the advance are one
  atomic fact, and the ledger is a first-class queryable table forever.
- DeclareLoss is **refused while any live replica still advertises coverage**
  (ClaimAdvertise, §5) — the ceremony cannot be used to shortcut a slow
  recovery.

This is the Elasticsearch `allocate_stale_primary` / Kafka unclean-election
shape: unrecoverable loss requires an explicit, attributable, audited opt-in,
never a default. If a node holding a declared-lost range later rejoins, its
data drains as supplement parts and the ledger row is annotated — immutable
supplement parts are cheap and truthful (§6).

#### 9.4.3 Catalog corruption or loss

Registry rows for nodes and claims are **soft state**; watermarks are
authoritative-but-reconstructible. Recovery:

1. **Postgres PITR** to the last good point.
2. Nodes **re-register** (Heartbeat, ClaimAdvertise rebuild the advisory view).
3. **Watermark recomputation** from the window manifests that ride every
   LakeCommit plus the live hot tables; window ids are a dense per-partition
   sequence precisely so coverage contiguity is decidable (§6).
4. **Orphan reconcile**: parts present in object storage but absent from the
   restored catalog are re-committed (SingleDrainCommit's uniqueness guard
   makes this idempotent).

No data loss is possible from catalog loss alone: hot tables are dropped
**only after durable LakeCommit confirmation**, so every byte is in staging,
in the lake, or both — never only in the catalog's imagination.

#### 9.4.4 Zombie fencing

Node identity is `(node_id, incarnation)`; FenceBoot mints the incarnation from
a catalog sequence at startup, every message carries it, and every receiver
rejects anything below its highest-seen incarnation (FencedZombie, §3 — the
Kafka epoch-fencing shape). Operationally:

- A previously-provisioned node whose catalog is unreachable at boot **starts
  in replica-only degraded mode** on its persisted incarnation: it may
  PeerApply and serve, it may not Accept new ownership. Rolling upgrades
  therefore never wedge on a catalog outage.
- Only a genuinely **new** node (no persisted incarnation) waits for the
  catalog, in a typed startup state visible on `/readyz`.
- A zombie — an old incarnation resuming after TakeoverDrain — is rejected by
  every peer and every commit guard mechanically. There is no runbook step
  because there is nothing for the operator to do; the invariant, not the
  operator, is the fence.

#### 9.4.5 Disk corruption

Detection is **checksums plus the drain's own full read** — every staged byte
is read completely at SealPart, so the drain is the scrubber and no separate
scrubber (or scrubber knob) exists. On a checksum failure:

1. Quarantine the affected window (it stops serving, stops draining).
2. Re-fetch the `(origin, seq)` ranges from a replica (§5) and rebuild.
3. Corruption of **all** replicas escalates to §9.4.2 — it is exactly a
   loss-beyond-RF−1 event and gets the same ceremony, never a silent skip.

### 9.5 Security operations

- **TLS is explicit or absent — never implicit.** `TlsMode` has **no `Default`
  impl** in the library (§10) and `tls.mode` has no default in the config: the
  embedder or operator must state `disabled` or `enabled` per listener,
  supplying **PEM paths** for cert, key, and CA. DuckSpout ships no bundled
  certificates and generates none. Mutual TLS is optional **per listener**
  (peer and ingest listeners typically mTLS; the observation listener typically
  server-TLS or loopback-plain). A non-loopback listener with TLS disabled
  logs a prominent warning at every boot — allowed, because lab and
  service-mesh deployments are real, but never quiet.
- **Secrets are file paths, only.** `catalog.password_file`, `tls.key`, and
  every future secret are read from files (systemd `LoadCredential`, K8s
  mounted Secrets). Secrets never appear in the TOML values themselves, in
  environment variables, in process arguments, or in any log or error string.
- **Catalog roles**: the daemon connects as `duckspout_daemon` (read-write
  registry + lake catalog); humans and dashboards use `duckspout_reader`
  (SELECT-only, excluded from principals/grants tables). Least privilege is an
  install-time check, not a recommendation (§7 details grants).
- **Disclosure is a security surface.** The closed status enum + 
  `replication_degraded` (§9.3.2) is the *only* degradation vocabulary, and it
  appears **identically** on the health endpoint, the metrics, and the
  registry. No channel ever knows more than another: an operator, an alert
  rule, and a client-side circuit breaker all act on the same fact at the same
  time, and there is no side channel whose absence of a warning implies health
  it cannot promise (CacheTransparency's spirit applied to operations).

### 9.6 The configuration appendix

One TOML file, environment-variable overrides, secrets by file path. **This
table IS the config surface.** Everything not listed is a fixed constant with a
stated value or a stated derivation (sub-note below).

#### 9.6.1 Node settings (27 rows, 32 settings — the ratchet counts settings)

Three rows bundle related settings (`catalog.*`: 2; `tls.*`: 4; `lake.*`:
2), so the 27 rows carry 32 individual settings. **The ratchet baseline
(Rule 12) is the settings count: 32.**

| Setting | Default | Why it must be a knob |
|---|---|---|
| `node.data_dir` | — (required) | deployment-specific |
| `node.otlp_listen` | 4317 (the OTLP/gRPC convention) | network topology |
| `node.flight_listen` | 8815 (the Arrow Flight convention) | network topology |
| `node.peer_listen` | 7946 | network topology |
| `node.advertise_addr` | first non-loopback interface, with the listen ports | network topology (NAT, K8s) |
| `node.failure_domain` | zone label / config | non-K8s has no downward API |
| `cluster.rf` | 2 | durability vs cost is a real tradeoff |
| `cluster.zone_aware` | auto | boolean escape hatch only (§9.1.1) |
| `cluster.seed_peers` | `[]` | non-K8s bootstrap (§9.1.3) |
| `catalog.dsn` (+`password_file`) | — (required) | deployment-specific |
| `tls.{mode,cert,key,ca}` | — (required, no default) | embedder-supplied PEMs, explicit posture (§9.5) |
| `lake.committer` (+`uri`) | `ducklake` | Iceberg-by-contract (§6) |
| `hot.window` | 60s | latency vs table-count tradeoff |
| `hot.max_bytes` | 75% of volume at startup | the disk budget; the only configured byte number; bounds staging+cache combined — cache is residual and evicted first |
| `drain.max_age` | 30m | seal latency vs part size |
| `drain.part_target_bytes` | 384 MiB (256–512 range) | object-store economics diverge |
| `drain.allowed_lateness` | 15m | workload event-time discipline diverges |
| `replication.receipt_timeout` | 5s (revisit by measurement) | intra-AZ vs WAN latency diverges |
| `admission.max_inflight_bytes` | 10% of the memory budget (cgroup limit, else system RAM — autodetected, §4.6) | memory-tight nodes with large batches |
| `max_payload_bytes` | 4 MiB | ecosystem default; edge batching diverges |
| `dedup.window_ttl` | 24h | must dominate the deployed retry horizon (§4) |
| `dedup.window_max_entries` | 100k | burst-rate divergence |
| `dedup.log_identity` | off | false-drop history in the field: opt-in only |
| `max_auto_columns` | 1024 | curated schemas vs unbounded raw keys; overflow spills to JSON, never rejects |
| `query.max_hot_bytes_per_query` | 2 GiB/node, fill-scaled | hot sizing is workload-derived |
| `query.hot_scan_deadline` | 30s | the real backstop; alerting vs exploratory use |
| `query.max_concurrent_hot_scans` | 8 | node sizing diverges |

Post-v1 tenancy adds **per-tenant overrides
of existing knobs** (not new knobs) plus tenant→retention-class mapping,
`shard_count` (per-tenant time-shard override, §2.2), `isolated_parts`
(opt-out of cross-tenant part packing if that packing ever ships — §2.7's
tenant-purity default made per-tenant), and `ingestion_rate` (§4.6) —
each pre-justified where cited; `ingestion_rate` is the only rate limit
that will ever exist.

#### 9.6.2 The dataset-declaration ledger (3 entries, ratcheted)

Dataset declarations are schema, not node config — but they are a configuration
surface all the same, so they live in their own **closed, ratcheted ledger**
with the same divergent-workload test per attribute:

| Attribute | Values / default | Why |
|---|---|---|
| `kind` | `event` \| `changelog`, default `event` | the two dataset kinds have different drain, dedup, and retention mechanics (§2, §6) |
| `key_cols` | required iff `kind = changelog` | the changelog identity and shard axis; fixed at declaration (change = new dataset + replay) |
| `sort_key` | default: event time | drain ORDER BY; sealed parts always carry min/max stats, so non-time range queries prune from cold (§6) |

**Retention classes** are declared beside the dataset ledger as a small
name → horizon map (`retention.classes`), capped at 8 (§9.6.3). v1 ships
two built-ins — `standard` (30 d, every dataset's class until behavioral
tenancy lands) and the system tenants' short class (72 h, not declarable,
§9.3.1) — and an operator may declare further classes up to the cap, so
"keep 13 months" is expressible from v1 as one declared entry.
Tenant→class *mapping* is the post-v1 behavioral-tenancy surface (§2.2);
the horizons themselves are not deferred.

Deferred entries (`residency`, `latest_projection`) are listed but
**uncounted** until their machinery ships; a reserved-but-uncounted knob is
ratchet theater and is not practiced here.

#### 9.6.3 Fixed constants (not configurable, each with a stated derivation)

| Constant | Value |
|---|---|
| Overload ladder thresholds | 80 % (disclose) / 95 % (throttle) / 100 % (refuse) of `hot.max_bytes`, on **staged** bytes (§4) |
| Operator capacity alert | at the soft threshold (§9.2) — the only capacity alert |
| Heartbeat cadence / TTL | 5 s / 15 s (TTL = 3× cadence — one missed beat is jitter, three is death; drives takeover detection, §5.6) |
| Snapshot dirty ratio (changelog rollover trigger) | 1.0 (≤2× space amplification, §6) |
| Background-eviction low-water | 5% free (dormant until the cache class is live) |
| SLRU protected:probationary ratio | start 80:20, bench-validated before it ships — parked with the cache class (§12.7); listed for the design-of-record, meaningful only when its feature lands |
| Clock-skew epsilon | 500 ms — bounds the heartbeat-staleness/event-time-lateness *skew warning* only; no invariant reads a clock (§3's model has no clock variable — correctness is clock-independent by construction) |
| Takeover suppression | derived: 2× termination grace (§9.1.2) |
| Outstanding queries per principal | 32 (a per-principal queue-depth bound: 4× `query.max_concurrent_hot_scans`, so one dashboard's fan-out queues rather than starves — capacity itself is governed by the scan knobs) |
| Shard sanity ceiling | 64 (config validation, not a tunable: shards multiply per-partition window tables, and 64 × live windows is where table count begins to dominate §2.3's hot-store model — beyond it lies a topology decision, not a knob) |
| Retention-class set cap | 8 (classes multiply the part population — parts are class-pure, §2.7 — and surveyed fleet horizons cluster well under eight distinct values) |
| System-tenant retention | built-in short class, 72 h (§9.3.1) |

#### 9.6.4 The KISS ratchet

**Any new setting requires a divergent-workload justification in its PR,
measured against the true count above (32 settings — the counting rule of
§9.6.1).** A knob earns its place only by
evidence that real workloads need different values — never by "someone might
want to tune this." Every constant in §9.6.3 is either tied to a named
benchmark scenario (§8) or ships with the feature that gives it meaning. The
table in §9.6.1 is the floor the ratchet holds: settings can be removed
freely, and added only over a defended threshold.

## §10 Library Architecture and Extensibility

DuckSpout is a library first. The deliverable is an embeddable Rust core
whose protocol crates each stand alone; the daemon is a thin composition
of them, and the DuckDB extension is a thin client to them. Anything the
daemon can do, an embedder can do by depending on the crates directly —
the daemon holds no logic of its own beyond wiring, signal handling, and
the cadence loop that ticks drains and retention.

### 10.1 Repository and crate layout

Two repositories. The split is dictated by the DuckDB
community-extensions registry, whose CI builds an extension with cmake
from the repository root — an extension folded into a Cargo workspace
monorepo cannot satisfy that shape.

**Repo 1 — `duckspout` (Cargo workspace monorepo).**

| Crate | Owns | Embeddable alone |
|---|---|---|
| `duckspout-types` | Wire/domain types: tenant, dataset declaration (`kind`, `key_cols`, `sort_key`), partition, window, part manifest (incl. `dedup_removed`), watermark rows, the status enum, the OTLP error table (§4). No I/O. | yes |
| `duckspout-accept` | Accept, DedupCheck, Throttle, Refuse: listeners, admission caps, the overload ladder's client-visible half, the OTLP adapter behind the AcceptAdapter port (§4). | yes |
| `duckspout-staging` | StageCommit: hot DuckDB lifecycle — micro-window tables, the dedup window table committed in the same transaction, staging/cache class metadata, Demote/Evict/DropWindow (§4, §2). | yes |
| `duckspout-replication` | Forward, PeerApply, Receipt, ClientAck sequencing; HRW ring, ClaimAdvertise, Heartbeat, FenceBoot, DegradedBoot, TakeoverDrain, DeclareLoss (§5). | yes |
| `duckspout-drain` | SealPart, PutPart, LakeCommit choreography (incl. the atomic WatermarkAdvance and Reconcile), keep-latest drain dedup, SnapshotSeal, retention Expire, EvolveSchema ordering (§6). | yes |
| `duckspout-watermark` | The watermark ledger, read-concern gating (`available`/`complete`), `duckspout_freshness()` logic, loss-ledger accounting (§7). | yes |
| `duckspout-lake-contract` | The LakeCommitter port: trait, semantics, and the **published conformance suite** any backend must pass (§10.3). | yes |
| `duckspout-lake-ducklake` | The first LakeCommitter: Parquet written by DuckSpout, committed via an embedded DuckDB used purely as a metadata-commit executor — rows never transit it. | yes |
| `duckspout-ctk` | The Conformance Testing Kit (§8): deterministic simulation of the action vocabulary, fault injectors (CrashNode, RecoverNode, partitions, pauses, membership churn), trace-conformance checking against the §3 model, and the armed broken variant of every checked invariant. | dev-dep |
| `duckspout-daemon` | The binary: config parsing (one TOML file, env overrides; the `TlsMode` type deliberately has no `Default` impl — TLS posture is stated or the config is invalid, §9.5), listener wiring, the cadence loop, systemd/K8s lifecycle (§9). No protocol logic. | n/a |

The dependency direction is one-way: protocol crates depend on
`duckspout-types` and on each other's ports, never on the daemon, never
on a concrete lake backend. `duckspout-drain` depends on
`duckspout-lake-contract`, not on `duckspout-lake-ducklake`; the daemon
selects the committer at composition time. CI audits the dependency
graph and fails the build on any edge from a protocol crate to a
concrete port implementation — the checkable form of the extensibility
claim, because a core that could quietly reference one backend can be
coupled to it before a second backend exposes the coupling.

**Repo 2 — `duckspout-duckdb` (the extension).** Shaped exactly like the
community-extensions template so registry CI builds it unmodified. It is
a client: it implements the `ATTACH 'duckspout:...'` catalog, bind-time
tier resolution (§7), the hot∪cold union with generation COALESCE
(§7.5), read-concern settings, and `duckspout_freshness()`. A checked-in
compatibility matrix (extension version × DuckDB version × daemon
protocol version) is validated by CI in both repos.

### 10.2 Rust at the core, C++ only at the engine wall

DuckDB's boundary has two very different faces, and DuckSpout sits on
the correct side of each.

**Inside the engine, the power APIs are C++-internal with no stable
ABI.** Catalog providers, transaction hooks, and custom settings are
reachable only through DuckDB's internal headers; every extension is a
version-locked rebuild against a specific engine release. There is no
avoiding C++ there, so the extension is C++ — but only for the three
capabilities that genuinely demand internals:

1. **Catalog integration** — the `duckspout:` ATTACH type and its
   bind-time resolution.
2. **Transaction-lifecycle pinning** — the coverage snapshot pinned for
   a transaction's duration, killing the data-vs-coverage TOCTOU that
   makes `complete` reads honest (§7.6, WatermarkHonesty).
3. **Custom settings and typed errors** — `duckspout_read_concern`,
   strict fail-closed errors raised mid-scan.

Everything else the extension needs — value construction, scan output,
client protocol — goes through the stable C API, minimizing the
version-locked surface to the smallest slab that buys the semantics.

**Outside the engine, embedding DuckDB uses the stable C API.** The
daemon's staging and drain crates drive DuckDB as a library through
that boundary, which is versioned and supported; the daemon is not a
version-locked artifact and upgrades independently of the engine
release cadence.

**Rust for the core is chosen on merits, not ideology:** memory safety
at the one place DuckSpout parses untrusted bytes off the network
(Accept); no garbage collector on the ClientAck path, where fsync
latency is the budget and a pause is a p99 regression (§4); mature
ecosystem exactly where DuckSpout needs it — arrow-rs and parquet-rs
for part writing, object_store for PutPart against every major store,
tonic for gRPC/Flight; and the property-test and mutation-test
toolchain (§8) that the verification posture requires as a first-class
citizen, not an afterthought.

### 10.3 Extensibility ports

DuckSpout is extensible where workloads genuinely diverge and closed
where a port would be an invitation to fork semantics. Every port ships
with a published conformance suite; an implementation that has not run
the suite is not a supported implementation.

| Port | Status | Contract in one line |
|---|---|---|
| **LakeCommitter** | v0.1, two planned backends | Six operations (§6.4): `commit_files` (atomic {add files + watermark}), `replace_files` (emergency repair only), `evolve_schema` (monotone), `expire`, `read_watermarks`, `attach_info`. Watermarks ride the commit as the portable contract (§6); nothing on the critical path may depend on a backend-exclusive feature. DuckLake is first; Iceberg is by-contract from day one, kept honest by the conformance suite even before the backend ships. |
| **AcceptAdapter** | v0.1 (OTLP built in) | Decode a protocol's payload into typed rows plus per-item reject verdicts; the durability, dedup, and error vocabulary stay in the core. OTLP (gRPC + HTTP) is the shipped adapter; other protocols are the edge collector's job first (§10.4), an adapter second. |
| **Typing engine seam** | v0.1 seam, optional impl | Schema-later ingestion: monotone type-widening lattice, dot-notation flattening, JSON terminal fallback (§2, §4.8). The default engine is DuckSpout's own fixed-schema OTLP mapping; RawDuck's engine is the optional alternative — never load-bearing, never a critical-path dependency. |
| **Residency/pin policy seam** | parked | The cache class (§2) is doctrine now, mechanism later; the seam exists so the SLRU and pin design land without touching protocol crates, but it is deliberately not a public port until the warm-retention trigger fires (§12.7). |
| **Transform stages** | SQL, permanent posture | Transforms are SQL applied after durability, re-runnable, never destructive (§11 Rule 7). There is no transform plugin API and none is planned — SQL is the extension language. |

### 10.4 Integration posture — what DuckSpout deliberately does not build

DuckSpout's edge in the ecosystem is knowing which halves of the
problem are already solved and refusing to re-solve them.

- **otel-collector is the edge.** Protocol fan-in, batching, edge
  buffering, and exotic-source support belong to the collector;
  DuckSpout ships a recommended collector configuration (persistent
  queue, raised retry horizon — §4) rather than a protocol zoo.
- **Airport (Query.Farm) is the entire client half of remote-hot.**
  `ATTACH (TYPE AIRPORT)` mounts any Arrow Flight server as a DuckDB
  catalog with pushdown; DuckSpout builds only the server half. Local
  hot is served over Flight even same-machine, respecting DuckDB's
  single-writer-process model.
- **DuckLake and Iceberg own the cold tier.** Catalog transactions,
  snapshot isolation, time travel, and file-level metadata are the
  lake's; DuckSpout writes immutable-with-expiry parts and commits
  them (§6). It never re-implements a table format.
- **RawDuck is prior art and a potential partner, not a dependency.**
  Its schema-later typing lattice maps cleanly onto DuckSpout's
  monotone evolution; DuckSpout's durability, replication, and lake
  layers are exactly what it lacks. The relationship is pursued as
  collaboration (§12.8); the default path never requires it.
- **Not built, ever:** a query engine (DuckDB is the query engine; the
  extension resolves, it does not execute — joins run inside the
  querying DuckDB, §7); a transform DSL (SQL is the DSL); a
  coordinator (the data path is coordinator-free by design, §5 — the
  catalog DB arbitrates only maintenance, and discovery is advisory).

## §11 Governance: The Keep Rules

DuckSpout runs on two tiers of rules. **Keep Rules** are the twelve
below, and only those: the invariants whose silent violation is a
correctness regression rather than a re-review. A Keep Rule may be
tightened or loosened through ordinary review — but CI independently
recomputes every rule that reduces to a checkable number, and a diff
touching a threshold, an allowlist, or a check's scope is read line by
line, never taken on the change's own explanation. Everything else —
naming, style, toolchain, process — is ordinary revisable judgment and
earns no ceremony. A rule that is really policy does not get Keep-tier
protection merely because changing it later would be inconvenient.

Where a mechanical check cannot determine whether something violates a
rule, it treats it as if it does: **ambiguity fails closed**, and
narrowing a check to dodge an ambiguous case is the same offense as
weakening the check.

1. **No ack before durability.** ClientAck is issued only after the
   batch is fsynced in local staging and replication Receipts bring the
   total durable copy count to RF (total-inclusive, §4, §5) — for every non-system tenant. `_`-prefixed system
   tenants never receive durable acks, so this rule is not implicated
   by them rather than carved out for them. There is no fast-ack mode,
   no async-ack mode, and no configuration that weakens this.

2. **Retries recompute and apply idempotently; they never replay
   blindly.** A retried request re-enters DedupCheck and resolves to
   the original outcome; PeerApply refuses gaps in the per-(partition,
   origin) sequence and applies each record exactly once (§5). Nothing
   speculatively computed by a failed attempt survives into the retry.

3. **Complete by default; ambiguity fails closed everywhere.** The
   default read concern is `complete`; a read whose coverage cannot be
   proven returns a typed error, never a silently partial result (§7,
   WatermarkHonesty). "Empty" and "couldn't check" are never the same
   answer. `available` is the explicit opt-out, and opting out is
   disclosed, not defaulted.

4. **Cold objects are immutable-with-expiry.** Exactly one PutPart and
   one whole-file DELETE per object, never a modification between. A
   sealed part never spans a tenant, a retention class, or a dataset
   kind. All merging is hot-side, in the drain, before the PUT — never
   compaction on object storage.

5. **Acked data leaves the staging class only by successful drain.**
   Staging is never evicted; Throttle and Refuse are always preferred
   over staging loss (§4). Cache eviction is always legal, requires no
   coordination, and cannot violate this rule by construction — a
   datum is cache class only after its LakeCommit is durable.

6. **`complete` reads depend on staging ∪ lake alone.** No cache state
   may gate them: any two cache states, including empty, yield the
   identical row set (CacheTransparency, §3). A cache miss can cost
   latency, never correctness, and cache occupancy can never affect
   ingest availability.

7. **Transforms are SQL, applied after durability, re-runnable.**
   Never a DSL, never destructive, never on the ack path. A transform
   that cannot be re-run from retained inputs is not a transform; it
   is data loss with extra steps.

8. **Discovery and placement are advisory, never load-bearing.** The
   registry's nodes, claims, and locations are soft state; a wrong or
   stale entry costs a redirect or a slower resolution, never a wrong
   answer (§5). Correctness authority lives in watermarks, manifests,
   and the drain guard — all reconstructible, none advisory.

9. **The data path survives a catalog-DB outage.** Ingest,
   replication, and already-resolved hot query service proceed; new
   bind-time resolution and drains pause, and say so (§9). No timer
   ever converts a disclosed catalog outage into an ingest outage or a
   denial of already-resolved service.

10. **A changelog part may be expired only when a sealed snapshot part
    covers its arrival range.** Uncovered changelog parts are
    keep-forever. Held formally by `Expire`'s guard and the
    `SnapshotCovered` invariant (§3), with the `ExpireUncovered` armed
    variant and the §8.4 retention judge as its teeth. SnapshotSeal
    appends a new object and never modifies an existing one — the ban is
    on rewrite, not on derivation (§6).

11. **Coverage, mutation, and performance floors are checkable numbers
    CI recomputes on every change** (§8). Raising or lowering one goes
    through ordinary review like any other change — never through the
    change it would unblock. Every checked property keeps its armed
    deliberately-broken variant, and CI fails the moment that variant
    stops reproducing its own violation: proof the check bites.

12. **The config surface is a ratchet.** The knob table is the config
    appendix (§9) and is the measured baseline; every addition carries
    a divergent-workload justification in its own review, counted
    against the true total — which is the **settings** count (32), not
    the row count: rows are presentation, settings are the ratchet
    (§9.6.1). Dataset-declaration attributes live in their own closed
    ledger under the same test. A constant that needs
    no divergence is a constant, not a knob; a reserved-but-uncounted
    knob is ratchet theater and is refused.

## §12 Roadmap

Each milestone makes one pillar true and verifiable before the next
widens the blast radius; verification grows in-milestone, never
trailing (§8). Versions are contracts about which invariants are armed,
not dates.

### 12.1 Spike (~2 weeks, single node, throwaway allowed except the lessons)

One thread of value, end to end: OTLP in → hot table → Airport-served
query → drain → DuckLake commit → one SQL query unioning hot and cold
with `complete_through` visible. The spike exists to force the three
riskiest seams early — transaction-lifecycle pinning in the extension,
the atomic {add files + watermark} LakeCommit, and the hot∪cold union —
before any of them is load-bearing.

### 12.2 v0.1 — durable, single node

The ack contract armed: DurableAck and NoAckedLoss hold under
CrashNode/RecoverNode in the CTK. Watermark ledger and both read
concerns live. The part-manifest format is frozen for the series and
includes `dedup_removed` from day one — cheap now, and it spares a
format migration when warm retention needs it. The two-class
staging/cache vocabulary lands as doctrine (Keep Rules 5–6 armed) with
the cache class empty by construction — behaviorally identical to
drop-after-drain, formally ready for what follows.

### 12.3 v0.2 — replicated

Split-friendly: receipts and catch-up first, takeover second if the
milestone slips. Forward/PeerApply/Receipt with gap refusal;
FenceBoot and FencedZombie armed; TakeoverDrain and SingleDrainCommit
armed second. Changelog datasets land here as declaration plus drain:
`kind = changelog`, `key_cols`, hash(key) sharding, keyed keep-latest
drain dedup, tombstone rows — the changelog pipeline through drain,
GA-gated on v0.3's snapshot rollover.

### 12.4 v0.3 — operable

Snapshot rollover under the drain scheduler (arming Keep Rule 10 and
GA-ing changelogs); `<dataset>_latest` argmax views; `dimension_as_of`
and `duckspout_freshness()`. The full overload ladder and ops surface
(§9): status enum, health endpoints, the canary pair, DeclareLoss
ceremony. The Jepsen-style self-run becomes the release gate — kills
including owner-mid-drain, pauses, partitions, and membership churn,
judged on zero acked loss and zero `complete` violations. The SCD-2
SQL spike (LEAD-over-key validity composed with generation COALESCE)
runs before the docs promise AS-OF-by-SQL.

### 12.5 v0.4 — ecosystem

Extension polish and the community-extensions registry submission
(`INSTALL duckspout FROM community`). The querier-caching ops page and
the joining-events-with-dimensions guide ship as products, not
appendices (§7). The warm-retention experiment is defined and run — N
ephemeral queriers, shared working set, RF=2, durable acks, versus
querier-local caching — and its result is the gate for the parked
cache class.

### 12.6 v1.0 — hardened

The nine-metric bench card published under full disclosure norms —
every number at RF=2 with durable acks, hardware named, chaos schedule
active, no RF=1/no-fsync figure ever standing alone. The durability
audit (self-run harness, methodology, and results) public. v1.0 is a
statement about what has been verified, not about feature count.

### 12.7 Deferred register

Every deferral has a design-of-record and a named trigger; nothing is
deferred into vagueness.

| Deferred | Trigger |
|---|---|
| Warm retention, SLRU, `residency` attribute, rung-0 eviction | The v0.4 experiment shows a measured win over querier-local caching |
| Pin (DDL residency `pin`, refuse-new/degrade-grown, fixed cap) | Measured demand the documented temp-table join pattern cannot serve |
| Hot LATEST projection (async-maintained, evict-last under drain stall) | Argmax view + client patterns measurably insufficient for dimension serving |
| Seq-versioned snapshot/revalidation endpoint; keyed diffs behind it | Revalidation traffic dominates full refresh |
| Cache-coverage advertisement in the registry | Post-takeover latency cliff or single-owner saturation observed |
| Predicate sketches and the pin-candidates advisor | The shipped per-dataset counters show a real signal — the advisor never precedes its advisees |
| Iceberg LakeCommitter backend | Contribution-ready from v0.1: the contract and conformance suite are published so the backend can be built, by anyone, against a spec — shipped when a maintainer (internal or external) carries it through the suite |

### 12.8 Collaboration sequencing

The RawDuck conversation opens early and stays scoped: DuckSpout as
the durability/replication/lake layer their engine lacks, their typing
engine as an optional seam here — optional, never load-bearing, on
both sides. The community OTLP-extension effort is tracked for schema
interoperability after the spike, never adopted as a dependency.
DuckLake upstream engagement (catalog-level contributions) waits until
v0.3 ships evidence — proposals travel better with a bench card
attached.

### 12.9 License

Apache-2.0 everything; DCO sign-off. One pre-declared contingency: if
a hosted re-sell threat materializes, the daemon — and only the daemon
— may dual-license under AGPLv3. Never BSL or SSPL (the 2024–2026
record of those moves is one-directional and ends in forks or
walk-backs), and never the extension: the piece that lives inside
users' DuckDB stays maximally permissive, unconditionally.

### 12.10 v1 cut list (condensed)

Deliberately absent, each with its compensating story: behavioral
multi-tenancy (structure ships; limits, retention classes, metering
deferred); all rate limits (memory bound + the ladder govern the real
resources); degraded-RF ack mode (refuse-only); the Iceberg backend
(contract + conformance suite instead); RawDuck as a default path;
result caching; non-OTLP accept adapters (the collector is the
protocol strategy); automatic rebalancing (takeover-on-death only; new
windows route to new membership); cross-region replication; a
background scrubber (the drain's full read is the scrub); a shipped
external monitor (the canary recipe is shipped; running it outside
DuckSpout's failure domain is the operator's half); any transform DSL
(permanent, Keep Rule 7).
