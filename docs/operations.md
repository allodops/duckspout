# Operations (§9)

Absorbed from DUCKSPOUT.md §9 per docs/seed.md s§10. Section labels (§9.1
… §9.6) are preserved so citations elsewhere — the daemon sources,
CONSTITUTION.md (R-9, R-12), the arming ledger — keep resolving after the
monolith is deleted.

This section is the operator's contract — deploy, size, watch, recover,
secure — plus the complete, closed configuration surface. Nothing
operational hides outside it. DuckSpout runs as a fleet of identical daemon
processes (§10, docs/architecture.md) with local NVMe-backed storage,
advisory peer discovery (§5), and lake commits through the catalog database
(§6).

> **Deployment manifests are Ⓜ(v0.3).** The `deploy/` K8s StatefulSet, PDB,
> systemd unit, collector edge config, and probe recipes land with issue
> #61 (milestone v0.3); until then **this document is their normative
> source** — a manifest that contradicts §9.1 is wrong, not a new decision.
> The only file under `deploy/` today is `deploy/compose/compose.yaml`, the
> **dev-only** conformance backends (MinIO + Postgres) of §9.1.3 — it
> demonstrates topology, never durability.

## §9.1 Deployment

### §9.1.1 Kubernetes (reference deployment)

DuckSpout ships as a **StatefulSet + headless Service + one PVC per pod**.
The StatefulSet's use-case list — stable network identity, stable
per-replica storage, ordered-enough lifecycle — is DuckSpout's requirement
list verbatim, which is why no operator/CRD layer exists.

| Choice | Value | Rationale |
|---|---|---|
| Workload | StatefulSet | stable identity + per-replica PVC is exactly the node model of §2 |
| Service | headless | peers dial each other directly; no per-request kube-proxy hop on the replication path (§5) |
| Volume | PVC per pod, `ReadWriteOncePod` (fallback `ReadWriteOnce`) | one daemon per volume is a hard assumption of the fsync path (§4); RWOP makes the kubelet enforce it |
| `podManagementPolicy` | `Parallel` | nodes have no boot ordering dependency on each other (FenceBoot orders against the catalog, not against peers); serial rollout would only slow recovery |
| Node identity | the pod name | stable across restarts, unique within the namespace; the incarnation half of identity comes from FenceBoot (§9.4.4), never from Kubernetes |
| Zone awareness | `topologySpreadConstraints` on the failure domain + the HRW candidate filter (§5) keyed on `node.failure_domain` | placement diversity must hold at both layers: the scheduler spreads pods, the ring spreads replicas |

Zone awareness is **automatic when ≥2 failure domains are visible** and
carries a single boolean escape hatch (`cluster.zone_aware`, default
`auto`) — a tri-state was rejected because "on" with one visible domain is
meaningless. When no cross-domain replication candidate exists, the ring
falls back to any peer: availability over placement.

The daemon has **no Kubernetes API dependency**: the failure domain comes
from config (populated via the downward API on K8s), peers from the
registry, and nothing from the apiserver. This subsection is packaging, not
architecture.

### §9.1.2 Rolling upgrades

Upgrade choreography lives **in the daemon behind SIGTERM**; the pod's
`preStop` hook is a thin delay, nothing more. This keeps the same
choreography correct under systemd (§9.1.3) and under a plain `kill -TERM`.

On SIGTERM the daemon performs a **shallow drain**:

1. Fail readiness — the Service stops routing new ingest to this node.
2. Finish in-flight requests: complete StageCommit for accepted batches,
   flush Forward traffic, collect outstanding Receipts, issue remaining
   ClientAcks.
3. Write an advisory `draining(restart, expected_back_by)` row to the
   registry.
4. Exit cleanly.

A shallow drain hands responsibility to **the PVC and the replicas** — it
never final-drains staged data to the cold tier. Draining hot data through
SealPart / PutPart / LakeCommit on every rollout would turn a routine
upgrade into a bulk cold-tier write and couple upgrade speed to catalog
availability; the fsynced local state plus RF peers (§5) already guarantee
nothing is lost across the restart. This is the converged practice of
replicated stores that roll shallow rather than rehydrate (Mimir,
Strimzi-managed Kafka).

Two guards bound the blast radius:

- **PDB `maxUnavailable: 1`.** With RF=2 (default), two simultaneously
  absent nodes can intersect a partition's entire replica set; the budget
  makes the scheduler respect what the replication math requires.
- **Takeover suppression = 2× `terminationGracePeriodSeconds`, derived,
  not a setting.** The `draining(restart)` row suppresses TakeoverDrain for
  a window long enough to cover a slow restart but short enough that a node
  that dies mid-rollout is still taken over. Deriving the window from the
  one value the operator already tuned removes a knob whose two failure
  modes (too short: spurious takeovers every rollout; too long: a real
  death goes unhandled) would otherwise have to be re-balanced by hand
  (protocol statement: §5.10). The suppression-expiry model is checked in
  §8 (a deliberately-broken never-expires variant must fail).

A rollout that collides with a catalog outage does not wedge: nodes holding
a persisted incarnation boot into replica-only degraded mode and resume
full duty after FenceBoot completes (§9.4.4); only genuinely new nodes wait
for the catalog.

### §9.1.3 Non-Kubernetes (first-class, not a footnote)

The same binary, the same SIGTERM choreography, the same config file:

- **systemd**: `Type=notify` (readiness = the daemon's own readiness, not a
  fork guess), `LoadCredential=` for TLS keys and the catalog password —
  secrets reach the process as files with correct permissions, never as
  environment variables or command-line arguments.
- **Bootstrap**: `cluster.seed_peers` (static address list) seeds the
  advisory peer view, exactly as etcd's `initial-cluster` does; the
  registry supersedes the static list as soon as Heartbeats flow. Empty by
  default — on K8s the headless Service DNS makes it unnecessary.
- **docker compose**: shipped, marked **dev-only**. It demonstrates
  topology; it does not demonstrate durability (compose gives no per-node
  fault isolation worth the name).
- **Bootstrap ordering**: the catalog database must exist before the first
  node of a *new* cluster starts — FenceBoot mints the first incarnation
  from a catalog sequence. The install doc states this sequence explicitly
  for the systemd/compose path.

### §9.1.4 Edge load balancing

The default edge is a **plain L4/L7 load balancer** (a normal Service on
K8s). Because any node accepts any write and forwards to the HRW owner
(§4, §5), balance is a latency concern — at most one extra Forward hop —
and never a correctness concern. The otel-collector `loadbalancingexporter`
can make routing ownership-affine and is documented as an optional
optimization, **off by default**: its alpha/beta maturity alone
disqualifies it as a dependency of the default path.

## §9.2 Capacity: sizing the volume

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

**Worked example.** 50 MB/s cluster wire ingest, expansion 1.4 (→ 70 MB/s
hot), residency 7200 s, RF 2, 3 nodes:

```
(70 MB/s × 7200 s × 2 ÷ 3 + 20 GB) ÷ 0.6 = (336 + 20) ÷ 0.6 ≈ 593 GB → provision 600 GB
```

From that volume, `hot.max_bytes` defaults to 75% = 450 GB; the fixed
ladder (§4) puts the soft threshold at 360 GB and the throttle threshold at
427.5 GB of **staged** bytes. During a full drain stall this node accrues
staged data at ~47 MB/s (70 × 2 ÷ 3), giving roughly two hours from
near-empty to the soft threshold and a further ~24 minutes to Throttle —
the 30-minute-catalog-outage target (§8) is met with wide margin.
`duckspoutctl size` computes all of this from the same inputs (the
subcommand is a stub in `crates/duckspout-ctl` at bootstrap; it lands at
v0.1 and implements exactly this formula).

Two alerting rules, and only two, on capacity:

- **Alert on `staged_bytes` at the soft threshold** (80% of
  `hot.max_bytes`). This is the single capacity alert; there is
  deliberately no freestanding disk-percentage alert. Staged bytes is the
  only quantity that cannot be reclaimed without violating NoAckedLoss and
  the only one that predicts write refusal — the same reason InnoDB keys
  flushing on dirty-page fraction, not buffer-pool occupancy.
- **Do not alert on volume fullness.** Once the cache class is live (§2,
  §12.7), near-full is the *designed* steady state: cache is residual and
  Evict reclaims it on demand (§4's rung 0). A disk-percentage alert would
  page forever on a healthy system.

When the soft alert fires without a disclosed drain stall, the designed
responses are volume expansion or adding nodes; the ladder (§4) is the
backstop, not the plan.

## §9.3 Self-observation

### §9.3.1 The `_self` system tenant

The daemon's own telemetry is ingested into the reserved `_self` tenant
through a deliberately weaker path than customer data:

- **No durable acks.** `_`-prefixed tenants never receive ClientAck with
  durability semantics, so DurableAck and NoAckedLoss (§3) are not
  implicated — the Keep Rules (§11) are scoped to non-system tenants rather
  than carved out.
- **One-hop amplification rule.** Telemetry about handling `_self` writes
  is never itself ingested; amplification is structurally ≤2.
- **Lossy bounded queue** with a `duckspout_self_dropped_total` counter —
  the only shedding anywhere in the system, and it is named as such in §4's
  ladder text.
- **Built-in short retention class**, not a knob.

This is the ClickHouse system-tables pattern: self-observation must be
unable to compete with, amplify, or deadlock the data path it observes.

### §9.3.2 The observation listener

A **dedicated listener** (separate port from ingest, Flight, and peer
traffic) serves:

- `/healthz` — liveness.
- `/readyz` — readiness, reported **per pillar**: ingest accepting,
  replication at RF, drain advancing, query serving.
- `/metrics` — Prometheus exposition.

All three read **in-process atomics only** — no DuckDB query, no catalog
round-trip, no lock shared with the data path. A monitor must not share its
subject's failure domain; a health endpoint that queries the database it is
reporting on turns every database stall into a monitoring outage exactly
when observation matters most.

The status surface is the **closed enum**
`normal | staging_pressure | drain_stalled | throttling | refusing_ingest`
plus the orthogonal `replication_degraded` boolean, exposed **identically**
on the health endpoint, the metrics, and the registry (see also §9.5). One
vocabulary, three transports, zero drift — a closed enum is also what the
§3 monotonicity properties and the §8 chaos judge assert over. As built,
this is `NodeStatus { overload: OverloadStatus, replication_degraded }` in
`crates/duckspout-types/src/status.rs` — one type, three transports.

### §9.3.3 The blindness principle and the two external probes

A process cannot observe its own death, and a cluster cannot observe its
own partition from the inside. Self-observation is therefore paired with
two **external black-box probes**, shipped as recipes (config + query,
versioned with the release) that the operator runs outside DuckSpout's
failure domain:

1. **Pillar canary** (page-severity): write a record to the reserved
   `_canary` tenant, then read it back **via Flight, addressed to node
   endpoints directly** — not through catalog-mediated binding. `_canary`
   writes traverse the full pipeline shape — Accept → StageCommit →
   Forward → Receipt — with the system-tenant exception that no durable
   ClientAck is ever issued (§2.2): replication runs, the promise is not
   made. The read-back is therefore a **hot-reachability probe**, not a
   completeness proof — a direct-Flight read has no bind-time watermark
   resolution to enforce `complete` against (§7.1), and claiming
   completeness for it would over-claim. It exercises accept, staging,
   replication, and queryable-hot end to end, and it **stays green through
   a catalog outage**, because a catalog outage is a disclosed, designed
   condition (`drain_stalled`, §9.4.1) and must not page as a pillar
   failure. `_canary` follows the same system-tenant rules as `_self`.
2. **Drain-freshness probe** (lower severity): watermark age per partition,
   read from the registry. This is the probe that *does* observe a stalled
   cold tier — at ticket severity, with the runway math of §9.2 telling the
   operator how long the condition is safe.

The split exists because a single canary either pages through every
disclosed drain stall or silently over-claims that a hot-coverage read
proves watermark advancement. It proves neither less nor more than each
probe states.

## §9.4 Failure runbook (condensed)

Each entry names the §3 machinery it leans on. The full procedures ship as
operator docs (validated against the fleet at v0.3, issue #63); this is the
normative shape.

### §9.4.1 Catalog database outage

The data path never touches the catalog (§11, Keep Rule R-9): **ingest,
replication, and already-resolved hot query service continue; drains and
new bind-time resolution pause, and say so.** No timer ever escalates a
catalog outage into anything else; it rides the ordinary ladder (§4).

Representative timeline for the §9.2 example cluster:

| T | State |
|---|---|
| T+0 | Catalog unreachable. Status → `drain_stalled` everywhere. Pillar canary green; drain-freshness probe opens a ticket. LakeCommit paused ⇒ watermarks freeze (WatermarkHonesty holds: `complete` reads still answer, bounded by the frozen watermark). Demote freezes with it, so the cache class drains toward zero as staging grows — the node sheds query acceleration to protect ingest durability, automatically. |
| T+0…T+2h | `staged_bytes` accrues at the drain-stall rate (~47 MB/s/node in the example). Grants and registry snapshots loaded before the outage remain valid for its duration — hot queries never fail on a catalog-availability timer. Clients with no cached binding cannot resolve new queries; this is disclosed, not worked around. Nodes that restart with a persisted incarnation boot replica-only degraded (§9.4.4); only brand-new nodes wait. |
| ~T+2h | Soft threshold: operator alert fires; status still `drain_stalled` (disclosure rung). Compute remaining runway: `(0.95 × hot.max_bytes − staged_bytes) ÷ accrual_rate`. |
| ~T+2h25m | Hard-approach: Throttle (UNAVAILABLE + RetryInfo, growing delay); the recommended collector edge config (§4) absorbs this in persistent queues. Status → `throttling`. |
| Hard | Refuse new writes; new-range replication refused so origins ring-walk to substitutes (§5). Status → `refusing_ingest`. NoAckedLoss holds throughout — nothing acked is ever shed. |
| Recovery | Catalog returns. Drains resume, LakeCommit advances watermarks (WatermarkEventuallyAdvances), the ladder unwinds as M falls back through the thresholds, Demote resumes, waiting new nodes complete FenceBoot. No operator action required at any point. |

### §9.4.2 Loss beyond RF−1: the DeclareLoss ceremony

When every replica of a partition range is gone, the watermark freezes
(GapFreedom forbids advancing over a hole) and **only a human can move
it**. DeclareLoss is deliberately ceremonial:

- The operator names **exact ranges** and passes `accept_data_loss: true`.
- The command writes a **permanent `loss_ledger` row in the same catalog
  transaction as the watermark advance** — the loss and the advance are one
  atomic fact, and the ledger is a first-class queryable table forever.
- DeclareLoss is **refused while any live replica still advertises
  coverage** (ClaimAdvertise, §5) — the ceremony cannot be used to shortcut
  a slow recovery.

This is the Elasticsearch `allocate_stale_primary` / Kafka unclean-election
shape: unrecoverable loss requires an explicit, attributable, audited
opt-in, never a default. If a node holding a declared-lost range later
rejoins, its data drains as supplement parts and the ledger row is
annotated — immutable supplement parts are cheap and truthful (§6).

### §9.4.3 Catalog corruption or loss

Registry rows for nodes and claims are **soft state**; watermarks are
authoritative-but-reconstructible. Recovery:

1. **Postgres PITR** to the last good point.
2. Nodes **re-register** (Heartbeat, ClaimAdvertise rebuild the advisory
   view).
3. **Watermark recomputation** from the window manifests that ride every
   LakeCommit plus the live hot tables; window ids are a dense
   per-partition sequence precisely so coverage contiguity is decidable
   (§6).
4. **Orphan reconcile**: parts present in object storage but absent from
   the restored catalog are re-committed (SingleDrainCommit's uniqueness
   guard makes this idempotent).

No data loss is possible from catalog loss alone: hot tables are dropped
**only after durable LakeCommit confirmation**, so every byte is in
staging, in the lake, or both — never only in the catalog's imagination.

### §9.4.4 Zombie fencing

Node identity is `(node_id, incarnation)`; FenceBoot mints the incarnation
from a catalog sequence at startup, every message carries it, and every
receiver rejects anything below its highest-seen incarnation (FencedZombie,
§3 — the Kafka epoch-fencing shape). Operationally:

- A previously-provisioned node whose catalog is unreachable at boot
  **starts in replica-only degraded mode** on its persisted incarnation: it
  may PeerApply and serve, it may not Accept new ownership. Rolling
  upgrades therefore never wedge on a catalog outage.
- Only a genuinely **new** node (no persisted incarnation) waits for the
  catalog, in a typed startup state visible on `/readyz`.
- A zombie — an old incarnation resuming after TakeoverDrain — is rejected
  by every peer and every commit guard mechanically. There is no runbook
  step because there is nothing for the operator to do; the invariant, not
  the operator, is the fence.

### §9.4.5 Disk corruption

Detection is **checksums plus the drain's own full read** — every staged
byte is read completely at SealPart, so the drain is the scrubber and no
separate scrubber (or scrubber knob) exists. On a checksum failure:

1. Quarantine the affected window (it stops serving, stops draining).
2. Re-fetch the `(origin, seq)` ranges from a replica (§5) and rebuild.
3. Corruption of **all** replicas escalates to §9.4.2 — it is exactly a
   loss-beyond-RF−1 event and gets the same ceremony, never a silent skip.

## §9.5 Security operations

- **TLS is explicit or absent — never implicit.** `TlsMode` has **no
  `Default` impl** in the library (§10) and `tls.mode` has no default in
  the config: the embedder or operator must state `disabled` or `enabled`
  per listener, supplying **PEM paths** for cert, key, and CA (as built the
  enum refines "enabled" into `mutual | server_only`, plus `disabled` —
  `crates/duckspout-daemon/src/config.rs`). DuckSpout ships no bundled
  certificates and generates none. Mutual TLS is optional **per listener**
  (peer and ingest listeners typically mTLS; the observation listener
  typically server-TLS or loopback-plain). A non-loopback listener with TLS
  disabled logs a prominent warning at every boot — allowed, because lab
  and service-mesh deployments are real, but never quiet.
- **Secrets are file paths, only.** `catalog.password_file`, `tls.key`, and
  every future secret are read from files (systemd `LoadCredential`, K8s
  mounted Secrets). Secrets never appear in the TOML values themselves, in
  environment variables, in process arguments, or in any log or error
  string.
- **Catalog roles**: the daemon connects as `duckspout_daemon` (read-write
  registry + lake catalog); humans and dashboards use `duckspout_reader`
  (SELECT-only, excluded from principals/grants tables). Least privilege is
  an install-time check, not a recommendation (§7 details grants).
- **Disclosure is a security surface.** The closed status enum +
  `replication_degraded` (§9.3.2) is the *only* degradation vocabulary, and
  it appears **identically** on the health endpoint, the metrics, and the
  registry. No channel ever knows more than another: an operator, an alert
  rule, and a client-side circuit breaker all act on the same fact at the
  same time, and there is no side channel whose absence of a warning
  implies health it cannot promise (CacheTransparency's spirit applied to
  operations).

## §9.6 The configuration appendix

One TOML file, environment-variable overrides, secrets by file path. **This
table IS the config surface.** Everything not listed is a fixed constant
with a stated value or a stated derivation (sub-note below).

The surface is **engine-enforced**, not aspirational: the daemon's config
structs (`crates/duckspout-daemon/src/config.rs`) carry it, the daemon's
`--dump-config-manifest` emits it, and the `golden-manifest` invariants
rule diffs that dump against `floors/config-surface.toml` on every change
(Keep Rule R-12's mechanism). The fixed constants live in
`crates/duckspout-daemon/src/constants.rs`. This table, the structs, and
the floor file must agree; a drift among them is a red check.

### §9.6.1 Node settings (27 rows, 32 settings — the ratchet counts settings)

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

Post-v1 tenancy adds **per-tenant overrides of existing knobs** (not new
knobs) plus tenant→retention-class mapping, `shard_count` (per-tenant
time-shard override, §2.2), `isolated_parts` (opt-out of cross-tenant part
packing if that packing ever ships — §2.7's tenant-purity default made
per-tenant), and `ingestion_rate` (§4.6) — each pre-justified where cited;
`ingestion_rate` is the only rate limit that will ever exist.

### §9.6.2 The dataset-declaration ledger (3 entries, ratcheted)

Dataset declarations are schema, not node config — but they are a
configuration surface all the same, so they live in their own **closed,
ratcheted ledger** with the same divergent-workload test per attribute:

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

### §9.6.3 Fixed constants (not configurable, each with a stated derivation)

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

### §9.6.4 The KISS ratchet

**Any new setting requires a divergent-workload justification in its PR,
measured against the true count above (32 settings — the counting rule of
§9.6.1).** A knob earns its place only by evidence that real workloads need
different values — never by "someone might want to tune this." Every
constant in §9.6.3 is either tied to a named benchmark scenario (§8) or
ships with the feature that gives it meaning. The table in §9.6.1 is the
floor the ratchet holds: settings can be removed freely, and added only
over a defended threshold.
