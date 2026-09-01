# Query

> **Provenance:** absorbed from `DUCKSPOUT.md` §7 (docs/seed.md s§10).
> **Owning crates:** the query surface spans the daemon's Flight server and
> the catalog extension (the extension itself lives in the deferred
> `duckspout-duckdb` repo — Repo 2, §10.1; its riskiest seam is prototyped in
> `spike/`). Read-concern and status vocabulary are owned by
> `duckspout-types`; watermark semantics by `duckspout-watermark`.

DuckSpout has no query engine and never grows one. Every query runs inside an
ordinary DuckDB — the user's process, a notebook, a dashboard's connection
pool. DuckSpout's query surface is exactly three things: a catalog extension
that makes hot and cold look like one database, a Flight server on every node
that serves the hot side, and a completeness contract (read concerns) that
makes the seam honest. This document is normative for all three.

## 1. The one-ATTACH model (§7.1)

The entire query surface is a single statement:

```sql
ATTACH 'duckspout:<catalog-dsn>' AS ds;
SELECT * FROM ds.events WHERE ts > now() - INTERVAL 15 MINUTE;
```

The DuckSpout catalog extension does everything else internally:

1. Connects to the registry database named by the DSN — with the DuckLake
   backend, the same Postgres that hosts the lake catalog (section 3 scopes
   the Iceberg topology, where the registry is its own Postgres).
2. Attaches the lake (DuckLake first; any lake the committer contract
   supports, `docs/design/drain.md`) inside the same DuckDB process.
3. At **bind time, per query**, reads the registry (section 3) and resolves
   which hot nodes — if any — must contribute, then mounts them via Airport
   (section 4).

The user never attaches the lake, never names a hot node, never learns the
cluster topology. One line in `~/.duckdbrc` makes it zero-touch:

```sql
-- ~/.duckdbrc
ATTACH IF NOT EXISTS 'duckspout:postgres://duckspout_reader@catalog/prod' AS ds;
```

Rationale: DuckDB's ecosystem converged on ATTACH as the mount point for
remote catalogs (DuckLake, Postgres, Iceberg REST, Airport all use it); a
tool that requires three coordinated attachments plus manual node discovery
would be operated wrong by default. Collapsing them behind one catalog
extension is the only design in which the completeness contract (section 6)
can be enforced at all — enforcement lives at bind, and bind must be
DuckSpout's.

Rejected: a DuckSpout-side distributed-join coordinator or any fan-out
execution layer. Joins belong to the querying DuckDB (section 7); building a
second query engine would contradict both the one-ATTACH model and the
library architecture (`docs/architecture.md`).

## 2. Bind-time resolution: the three resolvers (§7.2)

Resolution maps a query's referenced datasets and predicates to concrete
scan branches. Three resolvers run in sequence at bind; all of their inputs
are advisory soft state except the watermarks, which are transactional
(section 3).

| Resolver | Input | Output | Rule |
|---|---|---|---|
| Tier | query time range × per-partition `complete_through` | hot branch, cold branch, or both | A range at or below `complete_through` is lake-served and **never touches hot**. Only the owed remainder — above the watermark — resolves to hot. |
| File | DuckLake per-file column statistics (min/max, row counts) | cold file set | Standard lake pruning; DuckSpout adds nothing and subtracts nothing here. The lake's own planner prunes with the stats every SealPart/PutPart wrote (`docs/design/drain.md`). |
| Holder | registry claims | **exactly one** hot holder per owed partition | Owner preferred; if the owner is unreachable or its claim is stale, the replica whose `replicated_through` covers the owed range is chosen. One holder per partition, always. |

Three consequences are load-bearing:

- **Hot is never scanned redundantly.** Because a covered range is
  lake-served unconditionally, drained data is read from the lake even while
  a hot copy still exists on a node. Hot serves only what the lake does not
  yet own. This is the query-side half of CacheTransparency (§3): no hot
  residency decision can change the answer to a `complete` read.
- **No cross-node duplication.** One holder per owed partition means the
  union in section 5 never sees the same row from two nodes, regardless of
  RF. Replicas exist for durability and takeover
  (`docs/design/replication.md`), not for read fan-out.
- **Everything is advisory except the answer.** Nodes and claims are soft
  state, refreshed by Heartbeat and ClaimAdvertise
  (`docs/design/replication.md`); a stale claim costs a retry or an
  unreachable-branch error under `complete` (section 6), never a wrong
  result. Correctness rests solely on watermarks and the drain guard
  (SingleDrainCommit, §3) — the registry is a routing hint, not a truth
  source (CONSTITUTION.md R-8).

The Tier rule as stated is the **v1 rule, under which the cache class is
dormant on the read path**: a covered range never touches hot, so no
`complete` read ever consults a cache-class table — §2.4's
serve-`complete`-reads row and the `dedup_removed` gate are armed doctrine
awaiting the deferred cache-coverage advertisement (`docs/deferred.md`,
§12.7). The amended rule is pre-stated here so the deferral has a
design-of-record: when advertisement lands, a covered range may be served
from a cache-class holder advertised in the registry instead of the lake —
transparently, by CacheTransparency — while the tier boundary itself (hot
staging serves only above the watermark) is unchanged.

Resolution is per-query, not per-session: claims move (takeover,
`docs/design/replication.md`; drain progress, `docs/design/drain.md`), and a
session-cached route would silently go stale. The cost is one registry read
per bind against three small indexed tables; this is the same price every
lake query already pays the catalog.

## 3. The registry tables (§7.3)

The registry lives in a Postgres database under the `duckspout` schema —
**with the DuckLake backend, the same database as the lake catalog** (the
backend-scoping paragraph below covers Iceberg):

| Table | Contents | Written by | Consistency |
|---|---|---|---|
| `duckspout.nodes` | node id, incarnation, endpoints, failure domain, status enum | Heartbeat | soft state, reconstructible |
| `duckspout.claims` | (partition → holder) coverage: owner claims and replica claims with `replicated_through` | ClaimAdvertise, piggybacked on PeerApply/Heartbeat | soft state, advisory |
| `duckspout.watermarks` | per-partition `complete_through`, per-dataset `dimension_as_of`, loss-ledger annotations | **LakeCommit only** | transactional, authoritative |

The placement is a deliberate collapse: watermarks advance **in the same
catalog transaction as LakeCommit** (`docs/design/drain.md`). There is no
window — not a millisecond — in which files are committed but the watermark
lags, or the watermark claims coverage the lake lacks. WatermarkHonesty (§3)
is enforced by transaction atomicity, not by protocol discipline. A separate
watermark store would reopen exactly the two-phase gap this design exists to
close; industry convergence is the same move lakehouse catalogs made when
they folded commit metadata into one atomic pointer swap.

**Backend scoping.** The single-database collapse is the **DuckLake
backend's** topology: DuckLake's catalog is itself Postgres, so registry
and lake catalog share one instance and the watermark row shares
LakeCommit's transaction. With an **Iceberg REST catalog** there is no
shared transaction to join, and the topology is stated rather than implied:
the registry (`nodes`, `claims`, `watermarks`, principals/grants) lives in
**its own Postgres**; the **watermark authority is the snapshot's commit
properties** (`docs/design/drain.md` section 4), atomic with the snapshot by
every Iceberg catalog's contract; and the registry's `duckspout.watermarks`
row is a **cached mirror**, written after the snapshot commit succeeds and
therefore always at or behind the authority, never ahead. WatermarkHonesty
survives because the mirror can only understate: a stale mirror makes a
`complete` read conservative (fail-closed refusal or a lower
`complete_through`), never falsely complete. `attach_info`
(`docs/design/drain.md` section 4) tells the extension which topology it is
binding, so nothing on the critical path is backend-exclusive (the
neutrality rule).

`duckspout.watermarks` is authoritative but reconstructible: window
manifests ride every LakeCommit, so catalog recovery (PITR + recompute,
`docs/operations.md`) can rebuild it. `nodes` and `claims` are pure soft
state and are simply re-advertised.

## 4. Remote hot: Airport is the client, DuckSpout is the server (§7.4)

DuckSpout does not ship a query client. The client half of remote hot is the
**Airport extension** (Query.Farm): `ATTACH (TYPE AIRPORT)` mounts any Arrow
Flight server as a full DuckDB catalog, with predicate and projection
pushdown. The DuckSpout catalog extension issues these Airport attaches
internally at bind for each resolved holder; the user never sees them.

DuckSpout builds only the **Flight server half**: every node serves its hot
tables over Arrow Flight. This is the entire remote-hot protocol — no
bespoke RPC, no custom wire format. Rationale: Flight is the ecosystem's
converged columnar transport; Airport already solved discovery, catalog
mapping, and pushdown on the client side, and re-implementing that client
inside the catalog extension would duplicate a maintained project to no
gain.

**Local hot is also served via Flight**, even on the same machine. DuckDB
holds a single-writer lock per database file; the ingesting daemon owns that
lock, so a querying DuckDB cannot open the hot database directly.
Flight-over-localhost is the fast path (no TLS handshake cost on loopback
where configured, kernel loopback throughput); one code path serves both
local and remote, and the authorization boundary (section 9) is identical
everywhere.

Pushdown honesty, stated normatively because query plans depend on it:
Airport delivers **best-effort single-table filter and projection pushdown
only** — never join keys, never semi-joins. Every join executes inside the
querying DuckDB (section 7). Client-side filters are best-effort by
Airport's own contract; DuckSpout therefore never relies on pushdown for
tenant isolation — that is enforced server-side (section 9).

## 5. The hot∪cold union (§7.5)

For each dataset, the catalog extension exposes one view whose shape is:

```
dataset = cold_branch(lake files ≤ complete_through)
          UNION ALL
          hot_branch(one holder per partition, > complete_through)
```

Mechanics, each load-bearing:

- **The watermark join prevents double-count.** Each branch is bounded by
  the per-partition `complete_through` read at bind: cold takes
  at-or-below, hot takes above. A row committed by LakeCommit between bind
  and scan does not double-appear, because the query is pinned to its
  bind-time watermark snapshot (section 6). WatermarkHonesty (§3)
  guarantees the two branches tile the range exactly, with GapFreedom
  supplying its no-hole premise.
- **One holder per partition prevents cross-node duplication** (section 2).
- **Per-branch generated projections, never `union_by_name`.** Schema
  evolves in two classes (`docs/design/data-model.md`): in-place lossless
  promotions and generation rebinds. The extension generates an explicit
  projection per branch — CAST-up to the current logical type, NULL-fill
  for columns a generation predates, COALESCE across generation columns —
  so that output types are a deterministic function of DuckSpout's schema
  lattice. `union_by_name` would delegate type resolution to DuckDB's
  coercion rules, which differ by version and can pick lossy widenings; a
  completeness-honest system cannot have its column types decided by
  whichever DuckDB the reader runs. (A raw ATTACH of the lake without
  DuckSpout shows generation columns un-coalesced; documented: raw is raw.)

## 6. Read concerns (§7.6)

Read concern is the completeness pillar's query surface. Two values, one
axis:

| | `available` | `complete` (**default**) |
|---|---|---|
| Uncovered range (watermark below query range, holder unreachable, resolution impossible) | **Narrows silently**: returns what is reachable | **Throws a typed error** naming the uncovered cells: dataset, partition, range, `complete_through`, and which holder was unreachable |
| Meaning of an empty result | "nothing was reachable" — undecidable | "nothing exists in this range" — proven |
| Intended use | dashboards, exploration, degraded-mode operation | alerting, billing, anything a human or machine acts on |

```sql
SET duckspout_read_concern = 'available';  -- session-scoped opt-out
```

**Complete is the default and fails closed** (CONSTITUTION.md R-3). SQL's
foundational flaw for completeness is that "empty" and "couldn't check"
collapse into the same zero rows; an alert built on that collapse fires
false negatives during exactly the outages it exists to catch. Every system
that lets availability silently narrow results by default teaches its users
to distrust empty sets. DuckSpout inverts the default: silence is a proof,
or it is an error.

**Per-transaction pinning.** The extension's transaction hooks pin, at
bind, the watermark snapshot and resolution used by the query, for the
transaction's lifetime. Data read and coverage claimed are therefore
evaluated against the same instant — without pinning, a watermark advancing
mid-query would let a scan read pre-advance data while reporting
post-advance coverage (a data-vs-coverage TOCTOU), violating
WatermarkHonesty from the reader's side. Multi-statement transactions get
one consistent cut for free.

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

**Freshness disclosure.** `duckspout_freshness()` is a table function
returning, per referenced dataset: `complete_through`, `dimension_as_of`
(changelogs, section 7), watermark age, and the laggard partition. A
`complete` query gated by one stalled dimension is correct but visible;
this function is how the operator finds the laggard in one call.

Declared loss (DeclareLoss, `docs/design/replication.md` /
`docs/operations.md`) is the one sanctioned weakening: after the ceremony,
`complete_through` may advance past a permanently lost range, and the
loss-ledger row is queryable alongside the watermark. `complete` reads over
an annulled range succeed and are documented as post-declaration truth.

## 7. Querying changelogs and dimensions (§7.7)

Event datasets are the simple case. Changelog datasets (kind `changelog`,
`docs/design/data-model.md`) add latest-state semantics:

- **`<dataset>_latest`** is an auto-defined argmax view: latest row per
  declared key, planned from the newest covering snapshot part forward —
  cost O(snapshot + changelog-since-snapshot), not O(full history). This is
  the converged correct read over asynchronously-folded keyed state (the
  same argmax-over-versions pattern ClickHouse documents for
  replacing-merge tables). Tombstones (`_op = 'delete'`) make keys absent
  from the view.
- **`dimension_as_of`** — a changelog dataset's `complete_through` — is
  exposed as a queryable watermark column and via `duckspout_freshness()`.
  Under concern `complete`, a join gates fail-closed on **every** referenced
  DuckSpout dataset; effective freshness is min(`complete_through`) across
  them.

**Join execution reality, stated normatively so no one designs against a
fiction:** joins between event streams and dimension datasets execute
entirely inside the querying DuckDB. Airport pushes single-table filters
and projections only; the dimension is re-streamed over Flight on every
query. Consequences:

- Direct per-query streaming is acceptable to roughly **32 MB** of
  dimension data (bracketed by the ecosystem's broadcast-join thresholds:
  Spark's 10 MB auto-broadcast, Pinot's 200 MB replicated-dimension cap).
- Above that, the documented pattern is **materialize-and-refresh**:
  `CREATE TEMP TABLE dim AS SELECT * FROM ds.dim_latest`, refreshed on a
  querier-side cadence (30–60 s suggested; the TTL is the querier's
  setting, documented, not a DuckSpout knob).
- For **reproducible** enrichment — the same query yielding the same rows
  next week — use DuckDB's native `ASOF JOIN` against the retained
  changelog itself, pure SQL over data DuckSpout durably keeps. Expensive
  over a long pre-snapshot changelog; audit and backfill workloads only.

**Two freshness modes, never silently mixed:** (A) latest-enrichment via
`<dataset>_latest` — freshness bounded by `dimension_as_of`, disclosed; (B)
reproducible AS-OF enrichment via `ASOF JOIN`. DuckSpout never injects an
as-of column into results and never generates a view that mixes the two
modes; a system that secretly chooses one produces joins whose meaning its
users cannot state. Event-time temporal-join machinery is rejected —
DuckSpout does not own a streaming join engine; it adopts the semantics
(dimension staleness bounded by the dimension-side watermark) without the
mechanism.

## 8. Query guards, caching, and the cold path (§7.8)

Three per-query guards protect ingest from queries on every Flight server:

| Guard | Default | Semantics |
|---|---|---|
| `query.max_hot_bytes_per_query` | 2 GiB/node, **fill-scaled** | Byte budget per query per node; scales up with hot fill ratio during drain-stall, because when drains stall (§6, §9) hot is the *only* coverage and steady-state limits would kill querying exactly when it matters. |
| `query.hot_scan_deadline` | 30 s | Wall-clock backstop, enforced via scan interrupt. |
| `query.max_concurrent_hot_scans` | 8 | Node-sizing bound; outstanding-queue depth per principal is a fixed constant (32). |

A tripped guard is a **typed error, never truncation**. A silently
truncated result is a completeness lie with extra steps; the error carries
which guard tripped and the remediation (narrow the range, raise the knob,
use the lake). There is no row cap: rows are subsumed by bytes plus
deadline.

**No result caching, anywhere in DuckSpout.** Result caches in this lineage
cover completed, aligned ranges; hot is definitionally the uncacheable
moving window — every second changes the answer. Revisit only with measured
repeat-query evidence, and then in a frontend layer, never in the node.

**The cold path bypasses DuckSpout entirely.** Post-drain reads go straight
from the querying DuckDB to the lake's object store; hot nodes serve only
data that entered through their own ingest. The cold accelerator is the
**querier's own DuckDB external file cache** (default-on since 1.3,
memory-bounded) — DuckSpout nodes never operate a cold-object cache. A
node-side cache of lake bytes would be re-shipped over Flight on every
query while the querier could have cached the same bytes locally after one
object read; every deployed analytics-over-object-store system (Snowflake
warehouse-local cache, Presto/RaptorX worker caches, DuckDB's EFC) put this
cache at the compute, and DuckSpout follows. Cold objects being
immutable-with-expiry (`docs/design/drain.md`) makes aggressive downstream
caching trivially coherent: a URL's bytes never change.

## 9. Authorization (§7.9)

**Identity.** Two channels, one principal: mTLS client-certificate SAN
(preferred) and bearer token (required — Airport is bearer-only today).
Both resolve to a single principal identity; unknown principals are always
denied. When Airport gains mTLS, bearer remains supported; the posture
(deny-unknown, one principal) is fixed, the channel is configurable.

**Grants.** Principals and grants live in catalog tables, cached in-daemon
and refreshed on Heartbeat cadence. During a catalog outage the last-loaded
snapshot **remains valid for the outage's duration — there is no staleness
timer.** A timer would make hot-query availability a function of catalog
availability, violating the data-path independence rule
(`docs/design/replication.md`, CONSTITUTION.md R-9): already-resolved hot
queries must keep working through a catalog outage. The disclosed cost is
revocation lag bounded by the outage length — disclosed, not manufactured
into an outage-triggered denial of service.

**Tenant enforcement is server-side, always on.** The Flight server
conjoins a tenant predicate (`tenant_id IN <principal's granted set>`) into
every scan, keyed off the authenticated principal and bound to the
ingest-time tenant column; catalog metadata visibility is filtered the same
way. This is the row-filter model (Trino/OPA lineage); client-side filters
cannot be the enforcement point because Airport's pushdown is best-effort
by its own contract. Single-tenant deployments are not a special case: one
principal granted `*`, same code path.

**Cold-path honesty.** DuckSpout cannot enforce anything on a querier
scanning the object store directly — and says so. What it does is *enable*
enforcement: parts are tenant-pure under per-tenant prefixes (a hard layout
invariant, `docs/design/drain.md`), so prefix-scoped IAM policies give
per-tenant cold-side isolation; reference policies ship in the operations
guide (`docs/operations.md`). A system that claimed to enforce cold-side
tenancy it cannot see would be selling a fiction; this one draws the
boundary where it actually is.

**Catalog-side least privilege.** Two Postgres roles: `duckspout_daemon`
(read-write on registry and lake catalog; sole creator of DuckSpout
objects, verified at install) and `duckspout_reader` (SELECT-only,
excluding the principals/grants tables). The reader DSN is what goes in
`~/.duckdbrc`; a compromised querier credential can read routing metadata,
not rewrite watermarks or harvest the grant table.
