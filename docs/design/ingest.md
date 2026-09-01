# Ingest and Durability

> **Provenance:** absorbed from `DUCKSPOUT.md` §4 (docs/seed.md s§10).
> **Owning crates:** `duckspout-accept` (accept path, adapters — §4.1),
> `duckspout-staging` (WAL=hot, ack sequence, dedup tier 1, overload ladder —
> §4.2–§4.6), with the shared vocabulary — the status enum, the OTLP error
> mapping, the `AcceptAdapter` port — owned by `duckspout-types` (ADR-0008).

This document specifies the write path from the first byte on the wire to the
`ClientAck` — the accept edge, the WAL=hot storage mechanics, the exact ack
sequence, duplicate semantics, the overload ladder, admission constants, and
the transform pipeline. The formal definitions of the actions named here
(`Accept`, `DedupCheck`, `StageCommit`, `Forward`, `PeerApply`, `Receipt`,
`ClientAck`, `Throttle`, `Refuse`) live in the formal core (§3, absorbed into
`specs/`); replication mechanics (`Forward`/`PeerApply`/`Receipt`, ring
walk-down, takeover) are `docs/design/replication.md`'s subject and appear
here only where the ack path depends on them.

## 1. The accept path (§4.1)

### 1.1 otel-collector is the edge, DuckSpout is the terminus

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
  retry instructions (§4.5, section 5 below); a collector that gives up
  after five minutes converts a disclosed brown-out into silent loss at the
  edge.

DuckSpout's own dedup window TTL (section 4.1) is derived from this recipe:
the window must dominate the retry horizon the recipe configures.

### 1.2 Accept adapters are a trait; OTLP is the first implementation

The accept surface is a pluggable trait — `AcceptAdapter`, defined in
`duckspout-types` and re-exported by `duckspout-accept` (ADR-0008; see
`docs/architecture.md` for the port catalog). An adapter's obligations are
exactly three:

1. Decode the wire payload into typed record batches for a declared dataset
   (kind `event` or `changelog`, `docs/design/data-model.md`).
2. Extract tenant identity (`X-Scope-OrgID` from the mTLS-verified edge) and
   the optional `x-duckspout-idempotency-key` header.
3. Map DuckSpout's admission/overload outcomes (sections 5–6 below) onto the
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

## 2. WAL = hot: the durability primitive (§4.2)

### 2.1 One store, not two

DuckSpout does not maintain a separate write-ahead log beside its hot store
(**ADR-0003**: there is no WAL crate). The hot store *is* the WAL: accepted
records are inserted into persistent DuckDB tables, and DuckDB's own
fsync-on-commit WAL is the durability primitive. When `StageCommit` returns,
the bytes are on disk, crash-replay guaranteed by the engine that will also
serve queries over them (`docs/design/query.md`). This collapses the classic
ingest architecture (log → memtable → flush) into a single transactional
store and eliminates an entire class of log/store divergence bugs — there is
no second copy to reconcile after a crash.

The engineering caveat is acknowledged: DuckDB's fsync granularity
(per-commit vs. per-checkpoint) must be verified against the engine source
and pinned in the compatibility matrix (`compat-matrix.toml`) before each
supported DuckDB version is certified; NoAckedLoss (§3) is only as strong as
that fsync. Local NVMe is the assumed substrate — fsync latency is the ack
path's critical path, and network volumes with high fsync cost degrade ack
p99 directly (`docs/verification.md`'s benchmark hardware disclosure exists
for this reason). Fsync discipline (directory fsync, torn-write detection,
group commit off the reactor) lives behind the storage port and its CTK
fault injectors (ADR-0003).

### 2.2 One table per micro-window

Each (tenant, shard) partition's staging-class data lives in one DuckDB
table per micro-window (`hot.window`, default 60 s; changelog datasets
close windows by the same rule on the arrival axis — `hot.window` (60 s)
of arrival time or `drain.part_target_bytes` of staged size, whichever
first — since they have no wall-clock alignment requirement).
Consequences, each load-bearing:

- **O(1) cleanup.** After a durable `LakeCommit` (`docs/design/drain.md`),
  the window table is `DROP TABLE`d. No vacuum, no tombstone debt, no
  compaction of the hot store — deletion cost is independent of row count.
- **The drain is a sorted `COPY`.** Sealing a part reads whole tables,
  never row ranges, so the seal is a full sequential scan that doubles as a
  corruption scrub (`docs/operations.md`).
- **The ladder's unit of accounting.** `staged_bytes` (section 5) is a sum
  over live staging tables — cheap and exact.

### 2.3 The hot table doubles as the replication log

Every row carries two system columns: `origin` (accepting node's
`(node_id, incarnation)`, `docs/design/replication.md`) and `seq` (dense
per-(partition, origin) sequence). The staging table, ordered by
`(origin, seq)`, *is* the replication log — `Forward` ships `(origin, seq)`
ranges, `PeerApply` inserts them transactionally on the replica, catch-up
after a partition is a range query over data already durably held. There is
no separate log segment format, no log retention policy distinct from
staging retention: the log lives exactly as long as the window is undrained,
which is exactly as long as replication can need it.

Schema-change records ride the same log as sequenced in-band records, so a
replica applies widenings in the same total order as data (the type lattice
is defined in `docs/design/data-model.md`; gap refusal, which supplies the
ordering guarantee, in `docs/design/replication.md`).

### 2.4 Exactly-once apply: the applied-watermark row

Each replica maintains, in the same hot DuckDB, an applied-watermark table:
one row per (partition, origin) holding the highest contiguously applied
`seq`. `PeerApply` inserts the forwarded rows *and advances this row in the
same DuckDB transaction*. Replay after a crash, a duplicate `Forward`, or a
reconnect is therefore idempotent by construction: a range at or below the
applied watermark is acknowledged without re-insertion, a range beyond
watermark+1 is refused (gap refusal), and the transactional coupling means
the watermark can never claim rows the crash discarded or miss rows the
commit kept. This is the standard consumer-offset-in-the-same-store
transplant (Kafka's transactional consumers, Flink's two-phase sinks
converge on the same shape) with DuckDB's transaction as the atomicity
provider. The accepting node uses the identical mechanism for its own
`StageCommit` bookkeeping, so origin and replica share one apply path.

## 3. The ack sequence (§4.3)

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
- The system tenants `_self`/`_canary` (`docs/operations.md`) never traverse
  this path's ack: their ingest is explicitly ack-less, so DurableAck and
  NoAckedLoss are not implicated rather than carved out.
- Latency budget: the path costs one local fsync plus one replication RTT
  (forwards are parallel). The `docs/verification.md` target — ack p99
  ≤ 25 ms at RF=2 with fsync on — is the honest price of the promise,
  disclosed rather than hidden behind an async ack.

## 4. Idempotency and duplicate semantics (§4.4)

Duplicates are handled at three tiers; each tier's scope and residual leaks
are stated exactly, because "exactly-once" claims without scope are the
field's most common dishonesty.

### 4.1 Tier 1 — the accept-node dedup window

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
  config (section 1.1): the window must outlive the longest retry the edge
  is configured to attempt. DuckSpout warns when count-cap eviction pushes
  the effective window age below the documented retry horizon.
- **Semantics:** a duplicate of a fully-acked entry replays the original's
  success, with any `partial_success` reconstructed from stored per-item
  counts (OTLP forbids retrying a populated `partial_success`, so the
  replayed body must match the original's counts). A duplicate arriving
  while the original is still pre-RF gets UNAVAILABLE + RetryInfo — the
  client in that window is by definition a retrying OTLP client that
  already handles retry signaling; no waiter-coalescing machinery.
- **Stage-then-throttled entries are replayable-as-acked.** A request that
  was staged and then resolved retryable (receipt timeout, rung 2 —
  section 3, `docs/design/replication.md`) leaves an unacked entry guarding
  durable data that *will* drain. That entry is never poison: the moment its
  receipts reach RF, a retry replays success — with the ack evidence
  computed exactly as ClientAck computes it (`DedupCheck`'s `AtRF` branch,
  §3.3). Until then, retries keep getting the retryable signal. No 24 h TTL
  wait, no second staged copy, no client that can never succeed.

### 4.2 Tier 2 — drain-time natural-key dedup per sealed part

Tier 1 is per accept node; retries that land on a different node (LB
reshuffle, node death) leak past it. The drain (`docs/design/drain.md`)
closes the gap for keyed signals: within each sealed part, records are
deduplicated on their natural key, deterministic winner = smallest
`(origin, seq)`.

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
(`docs/design/data-model.md`): the newest `(origin, seq)` for a key within
the part wins. Supplement parts (`docs/design/drain.md`) cannot duplicate a
winner by construction — their per-origin seq coverage is validated disjoint
against the sealed winner's manifest inside the commit transaction — so
per-part scope stays sound when a window has multiple parts.

### 4.3 The public contract

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
   the window), where read-time semantics (`docs/design/query.md`) rather
   than storage dedup provide the final answer.

## 5. The overload ladder (§4.5)

One measure, one knob, one monotone ladder, one closed status vocabulary.
Acked data never leaves the staging class except by successful drain —
overload is *always* answered by refusing new promises, never by breaking
made ones (NoAckedLoss, §3). This is the converged posture of Kafka's
NotEnoughReplicas, Elasticsearch's indexing-pressure rejections, and Loki's
retryable 429s: terminate in refuse-with-retry, never drop-acked.

**Measure:** `M = staged_bytes / hot.max_bytes`. Only staging-class bytes
count — cache-class residency (`docs/design/data-model.md`) is reclaimable
at will and would poison the signal; staged bytes is the one quantity that
cannot be reclaimed without violating NoAckedLoss and that actually predicts
write refusal (the same reason InnoDB keys flushing on dirty-page fraction,
not buffer occupancy). `hot.max_bytes` (default 75 % of hot-volume capacity
at startup) is the *only* configured byte number; every threshold below is a
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
on the health endpoint, metrics, and the registry (`docs/operations.md`).
The enum is one type in `duckspout-types`, three transports. A closed enum
is what §3's LadderMonotone property and the chaos judge
(`docs/verification.md`) can assert over; free-text status is unverifiable
status.

**Catalog outage = drain stall on the same ladder.** When the catalog DB is
unreachable, drains pause (`LakeCommit` needs it), staging grows, and the
node walks the same rungs it walks for any drain stall — status
`drain_stalled`, then throttling, then refusal, purely as a function of M.
No separate mechanism, and **no timer ever escalates**: a 10-minute outage
on a lightly loaded node changes nothing; the same outage at high ingest
rate throttles honestly. Ingest, replication, and already-resolved hot
query service proceed throughout (CONSTITUTION.md R-9, catalog-independence
rule); a drain stall also freezes cache demotion, so the cache class drains
toward zero as staging grows — the node sheds query acceleration to protect
ingest durability, automatically.

The only lossy path anywhere is the system tenants' (`_self`/`_canary`)
bounded self-telemetry queue (`docs/operations.md`), which is ack-less by
definition and carries a dropped-rows counter.

## 6. Admission constants (§4.6)

Two limits, both at `Accept`, both with converged industry defaults.
**These constants are normative here**; the operations appendix
(`docs/operations.md`, §9.6) mirrors them — issue #16 lands the mirror.

| Limit | Default | Semantics |
|---|---|---|
| `max_payload_bytes` | 4 MiB | Over-cap → RESOURCE_EXHAUSTED **without** RetryInfo — non-retryable, because retrying an over-sized payload can never succeed and instructing a retry manufactures a loop. gRPC's and the collector's shared 4 MiB default. |
| `admission.max_inflight_bytes` | 10 % of the memory budget (the cgroup memory limit where present, else system RAM — autodetected at startup) | Decoded-but-uncommitted bytes in flight; beyond it, `Throttle`. Elasticsearch's 10 %-of-heap indexing-pressure transplant. |

There are **zero rate limits in v1** and no per-listener token bucket ever:
the memory bound and the byte-denominated ladder govern the real resources
directly, and a rate limit would be a proxy measure guarding what M already
guards. When behavioral tenancy lands (`docs/deferred.md`), per-tenant
`ingestion_rate` is the only rate limit that will ever exist.

## 7. Transforms: SQL-only, three stages (§4.7)

DuckSpout will never grow a transform DSL (CONSTITUTION.md R-7); its
transform language is SQL, applied at three fixed points, each with a
distinct latency/authority trade:

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
immutable (`docs/design/drain.md`) — which is exactly why redaction belongs
at drain time (it must not survive into cold) and why everything else should
prefer query-time views (reversible by editing the view).

## 8. RawDuck: optional schema-later typing, never load-bearing (§4.8)

RawDuck (quackscience) is a schema-later ingestion engine for DuckDB:
auto-table-creation from observed payloads, a monotone type-widening
lattice ("nothing is ever dropped"), dot-notation flattening of nested
keys, and adaptive re-sort/projection machinery. Its lattice is the direct
ancestor of DuckSpout's own schema-evolution model
(`docs/design/data-model.md`) — the two widen monotonically for the same
reason and map onto lake schema evolution the same way.

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
