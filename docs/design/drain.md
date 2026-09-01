# Drain and the Cold Tier

> **Provenance:** absorbed from `DUCKSPOUT.md` §6 (docs/seed.md s§10).
> **Owning crates:** `duckspout-drain` (SealPart/drain choreography, retention,
> class mechanics), `duckspout-lake-contract` (the `LakeCommitter` port surface
> and its conformance suite — the trait itself and `CommitOutcome` are defined
> in `duckspout-types` and re-exported per ADR-0008), `duckspout-lake-ducklake`
> (the first backend). The **frozen `WindowManifest` struct including
> `dedup_removed`** lives in `duckspout-types` (`manifest::WindowManifest`,
> frozen per §12.2).

The drain moves data from the staging class to the lake. It is the only path
by which acked data leaves staging (CONSTITUTION.md R-5), the only writer of
cold objects, and the single merge point of the entire system: every
reorganization of data — sorting, deduplication, part packing, snapshot
generation — happens hot, on local NVMe, before the first byte reaches object
storage. The cold tier is never compacted.

## 1. Cold objects are immutable-with-expiry (§6.1)

Every cold object in DuckSpout's life has exactly two logical storage
operations: one PUT (§3 `PutPart`; byte-identical idempotent re-PUTs on retry
collapse into it, section 5) and, when retention releases it, one whole-file
DELETE (§3 `Expire`). It is never modified, appended to, rewritten, or merged
in between (CONSTITUTION.md R-4).

Rationale. Compaction on object storage is GET + PUT churn: every merged byte
is downloaded, re-sorted, and re-uploaded, paying request costs and egress
twice for data that was already durable. Lakehouse ecosystems accept this
because their writers flush small files under a clock deadline and repair the
damage later (Iceberg's rewrite-data-files maintenance, DuckLake's
`merge_adjacent_files`). DuckSpout removes the cause instead of treating the
symptom: because the hot tier is already durable (fsync,
`docs/design/ingest.md`), already replicated (RF,
`docs/design/replication.md`), and already queryable
(`docs/design/query.md`), there is no deadline forcing a premature flush — so
parts can be sealed at their final size and final sort order, once. All
merge-shaped I/O runs where it is nearly free: over local DuckDB tables on
the owner node.

Consequences, stated as rules:

- **A part is FINAL at seal.** No post-hoc compaction job, no small-file
  debt, no background rewriter. `merge_adjacent_files` (and its Iceberg
  equivalent) is demoted to an emergency repair tool an operator invokes by
  hand (`docs/operations.md`); it is never scheduled.
- **Each byte is uploaded exactly once, in final form.** The cold tier's
  write amplification is 1.0 PUTs per part — a published benchmark metric
  (`docs/verification.md`).
- **Immutability makes downstream caching trivially coherent.** A cold URL
  is never reused with different contents, so query-side file caches
  (`docs/design/query.md`) need no invalidation protocol.
- **Read-side IAM never needs write carve-outs**
  (`docs/design/query.md`): cold prefixes are read-only to everything except
  the drain's PUT and retention's DELETE.

## 2. SealPart: one sorted COPY, final parts only (§6.2)

`SealPart` (§3) is a single sorted `COPY ... TO` over the micro-window
staging tables of one partition, executed in the owner's embedded DuckDB,
producing one Parquet part (or a bounded set of parts at the size target).
The COPY performs, in one pass:

1. **Merge** across the window's micro-window tables
   (`docs/design/data-model.md`) — the fold that other systems defer to
   compaction happens here, at local-disk speed.
2. **Sort** by the dataset's declared `sort_key`. Default: event time. Parts
   of `changelog` datasets: `(key_cols, origin, seq)` — key-clustered so
   latest-view reads and snapshot generation scan contiguous key ranges.
   Snapshot parts: `(key_cols)`. Sort keys govern only parts sealed after a
   change; sealed parts are never rewritten.
3. **Drain-time dedup** on the dataset's natural key (spans, metric samples)
   or declared key keep-latest (changelog datasets), deterministic
   smallest-`(origin, seq)` winner. The count of rows removed is recorded as
   `dedup_removed` in the window manifest — load-bearing for `Demote`
   (section 9) and for the `CacheTransparency` proof obligations (§3).

**Sizing.** A part seals when it reaches `drain.part_target_bytes` — default
384 MiB, recommended band 256–512 MiB, matching the row-group and object
sizes the Parquet-over-S3 ecosystem converged on for scan efficiency — or
when the oldest staged data in the partition reaches `drain.max_age`
(default 30m), whichever first. The age cap bounds watermark staleness for
trickle partitions (`WatermarkEventuallyAdvances`, §3); it is a freshness
bound on `complete` reads, not a durability bound — durability was settled
at ack time (`docs/design/ingest.md`).

**Trickle datasets.** A partition that cannot fill a reasonable part within
`drain.max_age` may drain via DuckLake data inlining: rows committed into
the catalog database itself, zero PUTs, folded into Parquet later by the
lake's own machinery. This is a DuckLake-exclusive optimization and is never
the portable answer — the portable answer for small tenants and slow
datasets is a longer age cap. Nothing on the critical path may require
inlining (section 4; neutrality rule).

## 3. Late arrivals (§6.3)

`drain.allowed_lateness` (default 15m) is the hold: a window remains
eligible to absorb rows whose event time falls inside it for that long past
window close, so ordinary network-delayed data drains into its home window.

A row arriving later than that gets **arrival-window placement**: it is
sealed into the part being drained at its arrival time. The event-time
column is never rewritten — the row remains truthful about when its event
happened; only its file placement reflects arrival. This is the append-only
answer to lateness: the alternatives are reopening a sealed window (a cold
rewrite — banned) or dropping the row (violates `NoAckedLoss`).

**Stated cost.** A straggler widens its host part's event-time min/max
statistics, so zone-map pruning (`docs/design/query.md`) admits that part
into scans overlapping the straggler's event time. The marginal cost is one
extra part scanned per straggler-bearing part per overlapping query —
latency, never correctness. Watermark semantics are unaffected:
`complete_through` already accounts for lateness
(`docs/design/data-model.md`), and a post-watermark straggler is by
definition outside every `complete` read's contract.

## 4. The LakeCommitter port (§6.4)

DuckSpout commits to the lake exclusively through the `LakeCommitter` port —
defined in `duckspout-types` (`ports::LakeCommitter`), re-exported and owned
beyond the bare signature by `duckspout-lake-contract` (ADR-0008). The port
is the lake-agnosticism boundary: everything above it is lake-neutral;
everything below it is one backend crate. The contract has six operations:

| Operation | Contract | Notes |
|---|---|---|
| `commit_files` | Atomically register a set of sealed parts **and** advance the named partition watermarks in the same commit. Returns Committed / Aborted / Indeterminate (§6.5). | The only routine write. `WatermarkAdvance` is not a separate step — it rides `LakeCommit` atomically, which is what makes `WatermarkHonesty` (§3) provable: no state exists where files are visible but the watermark lies, or vice versa. Carries the window manifest (§6.8) and any required `evolve_schema` as one atomic unit where the backend allows; where DDL and append cannot combine, evolve commits strictly before add — add-before-evolve is forbidden (files committed ahead of their schema silently hide columns in both DuckLake and Iceberg). |
| `replace_files` | Atomically swap named objects for named replacements. | **Emergency only** (operator-invoked repair, declared-loss annulment §9). Never scheduled, never on the drain path — its existence is not a license to compact. |
| `evolve_schema` | Apply a monotone, lossless schema change (§2's type lattice). Idempotent; concurrent applications converge. | Commutative-join semantics make crash-retry and concurrent owners safe. |
| `expire` | Whole-file DELETE of named parts (§3 `Expire`). | Metadata-only from the table's perspective; the physical DELETE is the object's second and last storage operation. The changelog-coverage guard (Keep Rule 10; `SnapshotCovered`, §3) is enforced above the port, before `expire` is ever called. |
| `read_watermarks` | Return the last committed watermark state for named partitions. | The read-back half of Indeterminate resolution (§6.5) and of boot-time recovery (§5). |
| `attach_info` | Return what a querying DuckDB needs to attach this lake (catalog URI, credentials shape, dialect quirks). | Feeds the catalog extension's bind (§7). |

**First implementation: DuckLake** (`duckspout-lake-ducklake`). The
committer embeds a DuckDB instance used purely as a metadata-commit
executor — rows never transit it. `commit_files` executes
`CALL ducklake_add_data_files(...)` for the sealed parts and inserts the
watermark sidecar row into DuckSpout's registry table **in the same Postgres
transaction** as DuckLake's own catalog writes. One transaction, one
atomicity domain: the sidecar and the file registration commit or abort
together, which is the whole mechanism behind `WatermarkHonesty` on this
backend.

**Second implementation: Iceberg, by contract.** The Iceberg committer maps
`commit_files` to a REST-catalog append commit and carries the watermark
state in the snapshot's **commit properties** — the portable watermark
channel, chosen because every Iceberg catalog persists snapshot properties
atomically with the snapshot itself and exposes them to readers without a
side table. `evolve_schema` maps to Iceberg schema evolution (the lossless
promotion set both formats share); `expire` maps to delete-files +
expire-snapshots.

**Neutrality rule (Keep Rule, §11):** nothing on the critical path may
depend on a DuckLake-exclusive feature. Inlining (section 2) is an
optimization; the sidecar table is a DuckSpout table, not a DuckLake
feature; everything else in the port is expressible in both backends. The
port ships with a published **conformance test suite**
(`duckspout_lake_contract::conformance::run`, a public module so third-party
backends can self-certify) — atomicity of commit+watermark, Indeterminate
resolution, idempotent re-registration, expire semantics, evolve ordering —
so that backend #2 (and #3) is a community contribution validated by the
same harness, not a fork.

## 5. LakeCommit's three outcomes (§6.5)

`LakeCommit` returns exactly one of (the three-valued `CommitOutcome` type
in `duckspout-types`):

- **Committed** — files registered, watermark advanced. The owner proceeds
  to `Demote`/`DropWindow` (section 9).
- **Aborted** — the backend definitively rejected the commit (constraint
  violation, serialization failure). Nothing changed; the drain retries or
  yields to the guard (section 6).
- **Indeterminate** — the connection dropped mid-COMMIT and the outcome is
  unknown.

Indeterminate is resolved by **exactly one read-back before any retry**: the
committer calls `read_watermarks` (or checks part registration) to learn
whether the commit landed. Blind retry is forbidden — it either
double-registers or trips the guard spuriously; and unbounded read-back
loops are equally forbidden — one read-back either resolves the outcome or
the catalog is down, which is a drain stall on the overload ladder
(`docs/design/ingest.md`), not a new state.

Registration is idempotent by construction:

- **Deterministic part naming.** A part's object name is a pure function of
  `(dataset, partition, window_id, part_kind, discriminator)` — where the
  discriminator is the supplement's per-origin seq range or the snapshot's
  `snapshot_as_of_seq`. Two attempts to drain the same window produce the
  same names, so a re-PUT overwrites byte-identical content and a
  re-register is detectable.
- **Check-before-register.** The committer verifies absence inside the
  commit transaction; a name already present short-circuits to Committed.

## 6. The SingleDrainCommit guard (§6.6)

Two drainers must never both commit a window — the owner racing its own
retried past self, or a takeover drainer (`TakeoverDrain`,
`docs/design/replication.md`) racing a not-actually-dead owner
(`FencedZombie`, §3). The guard is a database constraint, not a lease:

```
UNIQUE (partition, window_id, part_kind, discriminator)
```

on the registration table, enforced inside the same transaction as
`commit_files`. The discriminator is the deterministic-naming discriminator
of section 5: fixed (`'-'`) for `part_kind = 'window'` — so **at most one
window part per window, ever** — the per-origin seq range for a supplement,
and `snapshot_as_of_seq` for a snapshot. The first committer of any fence
key wins; every other attempt Aborts and the loser discards its local work.
`window_id` is a dense per-partition sequence (section 8), so the constraint
is exact. This four-column key is the SQL form of `SingleDrainCommit`'s
first conjunct (§3.4); the two state the same fence.

**The supplement path.** Some legitimate flows produce a second — or a
later — part for an already-committed window: a takeover drainer holding
receipted ranges the winner lacked, a second takeover residue after
sequential owner deaths at RF ≥ 3, or a declared-lost node resurrecting with
data (§9.4.2). A supplement commit inserts under `part_kind = 'supplement'`
with its seq-range discriminator and, **in the same transaction**, validates
that its per-origin seq coverage is pairwise disjoint from **every**
already-committed part of the window — winner and prior supplements alike
(`SingleDrainCommit`'s second conjunct, §3.4). Multiple supplements per
window are therefore legal and fenced; disjointness makes them unable to
duplicate committed rows by construction, which is what keeps per-part dedup
scope (section 2) sound with multiple parts per window. Bind-time resolution
(`docs/design/query.md`) unions all parts of a window.

## 7. Retention: whole-file expiry and snapshot rollover (§6.7)

Parts are **tenant-pure, retention-class-pure, and kind-pure**
(`docs/design/data-model.md`). Therefore a part is never partially expired:
when its retention class's clock runs out, the whole file goes. Expiry is
the §3 `Expire` action — metadata-only from the table's view, one `expire`
call, one whole-file DELETE — because no part ever spans a retention
boundary. There is no retention rewrite, ever.

**Parts of `event` datasets**: age-based expiry at the retention class
horizon. Done.

**Parts of `changelog` datasets** carry an obligation event-dataset parts
don't: a part may hold the only record of some key's latest value, so age
alone cannot justify deletion. Retention for changelogs is **snapshot
rollover**:

1. **`SnapshotSeal`** (§3): the partition owner periodically seals a part
   with `part_kind = 'snapshot'` containing the full latest-by-key state of
   the partition as-of arrival sequence S (deleted keys absent — tombstones
   are applied, not copied). Generation reads the newest covering snapshot
   plus changelog-since via the lake and local hot state, and **appends a
   new object** — derivation is not compaction; the ban is on rewriting
   existing objects, and a snapshot part conforms to one-PUT-one-DELETE like
   any other.
2. **Trigger:** dirty ratio 1.0 — changelog bytes accumulated since the last
   snapshot equal the snapshot's size (the log-cleaner convergence Kafka
   standardized; fixed constant, no knob). This bounds space amplification
   at ≤2× live-state bytes on cold.
3. **Coverage:** once the snapshot at S is committed, changelog parts
   **wholly older than S − `drain.allowed_lateness`** become ordinary
   age-expirable files. The lateness margin guarantees no straggler placed
   by arrival (section 3) is covered by a snapshot that predates it.
4. **Uncovered changelog parts are keep-forever.** A changelog part may be
   expired only when a sealed snapshot covering its arrival range exists — a
   Keep Rule (CONSTITUTION.md R-10), held formally by `Expire`'s guard and
   the `SnapshotCovered` invariant (§3), because violating it silently
   deletes the last value of a key.

**Snapshot fencing.** Snapshots fence on their own discriminator, not a
vocabulary-reuse of the window fence: within the
`UNIQUE (partition, window_id, part_kind, discriminator)` constraint
(section 6), a snapshot's discriminator is its `snapshot_as_of_seq`, with
snapshot generation serialized per partition under the drain scheduler — one
partition at a time, stall-and-disclose under the overload ladder
(`docs/design/ingest.md`) when cold reads are slow. The snapshot's manifest
is its fencing record.

## 8. Window manifests, watermark reconstruction, catalog recovery (§6.8)

Every `LakeCommit` carries a **window manifest**: `window_id` (dense
per-partition sequence — contiguity must be decidable), per-origin seq
coverage, row counts, event-time min/max, `dedup_removed`, and part names.
The manifest rides the commit atomically and is stored queryably. (The
manifest struct is the frozen `WindowManifest` in `duckspout-types` —
`manifest::WindowManifest` with `OriginSeqRange` and `PartKind` — frozen per
§12.2.)

This makes the watermark state **authoritative-but-reconstructible**: the
catalog's watermark rows are the fast path, but the ground truth is
derivable from (a) the dense manifest sequence in the lake and (b) live hot
staging state on the nodes. Nodes and claims are soft state throughout
(`docs/design/replication.md`); watermarks are the only registry state that
matters, and even they can be rebuilt.

**Catalog PITR recovery procedure (sketch — full runbook in
`docs/operations.md`):**

1. Restore the catalog database from point-in-time recovery. Ingest,
   replication, and already-resolved hot queries were never interrupted
   (CONSTITUTION.md R-9); drains and new bind-time resolution were stalled
   and disclosed.
2. Nodes re-register through `FenceBoot` (`docs/design/replication.md`):
   persisted incarnations resume, fencing rejects any pre-restore zombie.
3. **Orphan reconcile:** list cold objects against registrations.
   Deterministic naming (section 5) makes every orphan attributable to a
   specific `(partition, window_id, part_kind)`; each is either
   re-registered (its commit was lost to the PITR horizon — re-registration
   is the one read-back path replayed) or deleted (its commit never happened
   anywhere).
4. **Recompute watermarks** from manifest contiguity plus live hot coverage.
   The recomputed watermark is ≤ the true pre-failure watermark, never
   greater — `WatermarkHonesty` holds through recovery; the cost of the PITR
   gap is temporary conservatism, never a false `complete`.
5. Resume drains. Hot tables were never dropped without durable commit
   confirmation (section 9), so nothing staged was lost to the catalog
   failure.

## 9. Demote, Evict, DropWindow: class mechanics at drain commit (§6.9)

On Committed, the drained window leaves the staging class
(`docs/design/data-model.md`). What happens to the local table is a
residency decision, never a correctness one:

- **Default (`residency = none`, the v1 behavior): `DropWindow`** — DROP
  TABLE at drain commit. O(1) cleanup, no vacuum debt, disk returned
  immediately.
- **`Demote`** (when the cache class is active): reclassify the table in
  place from staging to cache — **only if the window's manifest records
  `dedup_removed = 0`.** A window the drain deduplicated is not
  row-equivalent to its sealed parts; demoting it would let a cache table
  answer differently than the lake, violating `CacheTransparency` (§3).
  With the zero guard, substitution is unconditionally safe; any window with
  `dedup_removed > 0` is dropped instead. Demotion happens strictly after
  the `LakeCommit` is durable — a datum is cache only after the lake owns
  it.
- **`Evict`** applies to cache-class tables only, is unrestricted, takes no
  coordination and no locks the read path depends on, and can never touch
  staging (`LadderMonotone` and the never-evict-staging rule,
  `docs/design/ingest.md` / CONSTITUTION.md R-5).

**Crash between LakeCommit and local reclassification:** on recovery the
node re-attempts the drain, the `SingleDrainCommit` guard Aborts it (the
commit already stands), and the node completes the pending
`Demote`/`DropWindow` instead. The one-side-serving rule (§3: a window is
served from staging XOR lake/cache, fenced by the guard) holds across the
crash.

A drain stall (catalog outage, cold-store outage) freezes demotion along
with commits: staging grows, cache drains toward zero under rung-0 eviction,
and the node sheds query acceleration to protect ingest durability —
automatically, with no new mechanism (`docs/design/ingest.md`).
