# System & Data Model

> Absorbed from `DUCKSPOUT.md` §2 (docs/seed.md s§10). **Owning crates**:
> [`duckspout-types`](../../crates/duckspout-types) owns the domain types this
> section defines (dataset/tenant/window/part identifiers, the frozen part
> manifest, watermark row types); [`duckspout-staging`](../../crates/duckspout-staging)
> owns the hot-store mechanics (micro-window tables, the STAGING class,
> WAL = hot). The formal statements of this section's invariants (the
> CacheTransparency lemma, the ladder measure) live in the TLA+ tree — see
> [`specs/README.md`](../../specs/README.md). Section citations (`§n`) refer
> to `DUCKSPOUT.md` until its absorption completes.

## 2.1 Datasets

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

## 2.2 Tenancy

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

## 2.3 Windows and micro-window tables

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

## 2.4 Two classes of hot data: STAGING and CACHE

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

### Obligations matrix (normative)

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

### The cache-transparency theorem

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

## 2.5 Time-series as the degenerate case

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

## 2.6 The schema model

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

## 2.7 Cold layout: the hard rules

The cold tier is object storage, and object-storage economics dictate hard
layout rules that the rest of the design treats as load-bearing
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
