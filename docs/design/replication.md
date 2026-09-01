# Replication and Availability

> **Provenance:** absorbed from `DUCKSPOUT.md` §5 (docs/seed.md s§10).
> **Owning crate:** `duckspout-replication` (HRW placement — implemented as
> `hrw::hrw_score` / `hrw::hrw_owner` / `hrw::hrw_ranked`, ADR-0004 —
> plus incarnation fencing in `fencing`; protocol/fencing choreography lands
> at v0.2). Shared vocabulary — origin/seq identifiers, the
> `replication_degraded` flag, port traits — is owned by `duckspout-types`
> (ADR-0008).

The hot tier's availability story is this document. Object storage made the
cold tier highly available; everything below exists to give the newest data —
the window between ClientAck and LakeCommit — the same property. The
mechanisms are few and reused: one ring, one sequenced log per
(origin, partition), one receipt watermark, one fencing token, one operator
ceremony for the day the guarantees genuinely cannot hold. Every protocol
step here is a canonical action defined formally in §3 (absorbed into
`specs/`); this document gives the operational semantics.

## 1. RF semantics (§5.1)

`cluster.rf` (default **2**) is **total-inclusive**: RF counts every durable
copy of an acked record, including the copy on the node that will own the
drain. RF=2 means the origin's fsynced copy plus one replica receipt before
ClientAck fires (`docs/design/ingest.md`). Rationale for total-inclusive
counting: it is what Kafka `replication.factor` and every operator's mental
arithmetic already mean — "how many disks hold this byte" — and it makes the
failure math trivial: RF=N tolerates N−1 simultaneous node losses with zero
acked loss (NoAckedLoss). An additive convention ("origin + R replicas")
produces the classic off-by-one misconfiguration where an operator believes
they bought one more copy than they did; the convergence across Kafka,
Elasticsearch (`number_of_replicas` being the additive counterexample
operators routinely get wrong), and Cassandra's total-inclusive RF settles
it.

RF applies only to the **staging class**. Cache-class tables are never
replicated — the lake is their durable copy, single-copy owner-local
residency is the rule, and eviction is always safe (CacheTransparency,
`docs/design/data-model.md`). Replicating a cache over a durable backing
store buys nothing: the resolver routes hot reads to one holder, so a second
copy is unreachable waste.

Below the RF floor — fewer live, receipt-answering peers than `cluster.rf`
requires — DuckSpout **stops promising** rather than acking at degraded
RF. A request already staged when the receipt wait times out resolves as
`Throttle` (`UNAVAILABLE` + `RetryInfo`): the bytes are fsynced and will
drain, so the retryable signal is honest by right, and the retry replays
success once receipts complete (`docs/design/ingest.md` §4.4.1). New writes
are refused (`Refuse`) with the same wire form. Throttle and Refuse differ
in which state produces them — an admitted, durable request versus work
never admitted — never in client-visible semantics; neither is terminal in
any sense the wire could express. This is Kafka's
`acks=all` + `min.insync.replicas` posture: an ack is a promise about copies
that exist now, never about copies that will hopefully exist later. A
degraded-ack mode is deliberately absent (`docs/deferred.md`); its target
deployment (two-node edge) is not a v1 target, and stop-promising keeps
DurableAck unconditional.

## 2. The HRW ring (§5.2)

Placement is **rendezvous (HRW) hashing** over an advisory membership view.
The placement function is implemented in `duckspout-replication`'s `hrw`
module (`hrw_score`, `hrw_owner`, `hrw_ranked`) — in-house per **ADR-0004**
(~50 lines, pure; §8.5 property-tests its minimal-disruption law exactly,
and it is cross-checked against the TLA+ placement function once
Replication.tla lands):

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
  table, section 5), seeded at bootstrap by `cluster.seed_peers` (default
  `[]`; needed only off Kubernetes) and superseded by the registry once
  reachable. The view is **advisory**: two nodes briefly holding different
  views cannot corrupt anything — they route a forward to a non-owner, which
  costs one extra hop, or double-stage a range, which PeerApply idempotency
  absorbs. Correctness never depends on view agreement (section 5).
- **Zone awareness.** When the membership view exposes ≥2 distinct
  `node.failure_domain` values, the candidate walk filters so the RF set spans
  domains: after the owner is fixed, subsequent candidates in the owner's
  domain are skipped while a cross-domain candidate remains. With one visible
  domain the filter is inert — zone awareness "on" with one zone is
  meaningless, so the knob is a single boolean escape hatch
  (`cluster.zone_aware`, default auto-on) rather than a tri-state. When no
  cross-domain candidate is live, the walk falls back to same-domain peers:
  availability over placement, disclosed via the `replication_degraded` flag
  (`docs/operations.md`), never a refusal on its own.

## 3. Ownership routing: merge locality by construction (§5.3)

Any node accepts any write (Accept, `docs/design/ingest.md`) — the edge
needs no routing intelligence — but the RF set for a partition **always
includes the ring OWNER**. The acceptor Forwards the batch; once the owner's
PeerApply is receipted, the **owner's copy is the primary and the acceptor's
own copy demotes to replica standing** (a role flip in local metadata; no
bytes move). Consequences, in order of importance:

1. **Only owners drain.** All of a partition's window converges onto one
   node's disk before SealPart. The drain's sort, dedup, and part-packing
   (`docs/design/drain.md`) operate over the complete window locally.
   Cross-writer overlapping cold files are **impossible by construction** —
   there is no protocol case in which two nodes hold "their half" of a
   healthy window and both drain it. The only multi-part window is the
   churn-boundary supplement case (section 6), which is fenced, validated,
   and rare.
2. **The forwarding is free.** RF ≥ 2 means every accepted byte crosses the
   network at least once anyway; routing that mandatory hop *to the owner*
   converts replication traffic the durability pillar already pays for into
   merge locality. Ownership routing adds zero incremental network cost over
   naive replication.
3. **Load balance is a latency concern, never correctness.** A plain L4/L7
   balancer in front of any-node-accepts is the default
   (`docs/operations.md`). An ownership-affine edge — the otel-collector
   `loadbalancing` exporter keyed so batches land on their owner, saving the
   one forward hop — is documented as an **optional optimization only**: the
   exporter's alpha/beta maturity disqualifies it as a dependency, and
   DuckSpout must be exactly as correct behind a dumb balancer.

## 4. The replication protocol (§5.4)

### Forward / PeerApply / Receipt

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
writes (section 1). At a peer's hard overload threshold the peer refuses
**new-range** replication with a distinguishable signal (the origin ring-walks
immediately rather than waiting out the timeout) while continuing catch-up of
already-receipted ranges — the disk bound holds without ever dropping acked
data (Throttle/Refuse ladder, `docs/design/ingest.md`).

### The table is the log

There is no separate replication journal. The hot staging table — ordered by
`(origin, seq)` within each partition — **is** the log. Catch-up after a
disconnect is one query on the live peer:

```
SELECT ... WHERE origin = ? AND seq > <your receipt watermark> ORDER BY seq
```

streamed back through the same PeerApply path. One storage engine, one fsync
discipline, one recovery path; the WAL that makes the table durable
(`docs/design/ingest.md`, ADR-0003) makes the log durable for free. This is
the design's central economy and the reason gap refusal is cheap: contiguity
of the applied prefix is a table scan's natural order, not an index to
maintain.

### Schema changes ride in-band

A schema change (EvolveSchema, §3) is a **sequenced record in the same
per-(origin, partition) log**, totally ordered against the data rows around
it by `seq`. Peers **fail closed on unknown columns**: a data row referencing
a column the peer has not yet applied a schema record for is a gap by
definition and is refused like any other gap. On catch-up, peers apply
**widen-first**: schema records replay to the origin's schema state before
the data rows that depend on them. Because the schema lattice is monotone
(widenings only, `docs/design/data-model.md`), replay is value-preserving
and order-convergent by construction; the drain then commits schema
evolution strictly before file addition (`docs/design/drain.md`). No side
channel, no schema epoch protocol — the log's existing total order supplies
everything.

## 5. Claims and the registry: advisory discovery (§5.5)

The catalog DB holds three registry tables — `nodes`, `claims`,
`watermarks` — that the query path resolves against at bind time
(`docs/design/query.md`). Their maintenance costs the data path almost
nothing:

- **ClaimAdvertise** rows (`partition → node, role ∈ {owner, replica}`) are
  published as a **side effect of PeerApply**: the first apply for a
  partition the node has no claim row for triggers the insert. No separate
  claim protocol, no claim heartbeat distinct from the node heartbeat.
- **`replicated_through`** — the per-(partition, node) receipt coverage the
  query path and takeover logic read — is advanced on **Heartbeat cadence**,
  batched, not per-batch. Staleness is bounded by the heartbeat interval and
  costs only freshness of routing decisions.
- **Heartbeat** rows carry a TTL; a node whose heartbeat lapses is treated as
  dead by resolvers and by takeover election (section 6), subject to the
  suppression window of section 10.

The registry is **advisory, always** (CONSTITUTION.md R-8). A wrong or stale
entry costs latency — a query routed to a node that no longer holds the
range gets a typed miss-and-retry, a forward lands one hop off — **never
correctness**. The authoritative facts live elsewhere: watermarks in the
same catalog transaction as LakeCommit (WatermarkHonesty,
`docs/design/drain.md`), data coverage in the hot tables and sealed parts
themselves, and the registry is reconstructible from those plus window
manifests after a catalog restore (`docs/operations.md`). This is why the
data path survives a catalog outage (section 7's boot rule aside, running
nodes never block on the registry): ingest, replication, and
already-resolved hot queries proceed; only new bind-time resolution and
drains pause, and say so.

## 6. Node death, end to end (§5.6)

Timeline for the death of a partition's owner:

1. **Detection.** The owner's Heartbeat TTL lapses (or peers observe hard
   connection failure). Resolvers and replicas treat it as dead once the
   takeover-suppression window (section 10) — which covers planned
   restarts — is not in effect or has expired.
2. **Write reroute.** The HRW walk over the membership view minus the dead
   node yields a new owner; acceptors forward there. The new owner was, by
   the walk's construction, almost always already in the RF set — it holds
   the partition's receipted prefix and begins accepting with no state
   transfer. Sequences are per-(origin, partition), so no renumbering occurs.
3. **Read reroute.** Bind-time resolution (`docs/design/query.md`) picks the
   live claimant with the greatest `replicated_through` coverage. `complete`
   reads whose demanded range exceeds every live replica's coverage **fail
   closed** with a typed coverage error (WatermarkHonesty) — degraded
   availability is disclosed, never disguised.
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
   scope sound (`docs/design/drain.md`).

Classic split-brain has nothing to bite on: logs are disjoint per-origin
keyed sequences, so there is no shared tail to truncate and no leader term to
dispute — the single contended act is the drain commit, and that is settled
by the catalog's atomic guard (the same single-commit-point discipline as
Iceberg's optimistic concurrency).

There is **no automatic rebalancing**. Takeover-on-death is the only
migration; scale-out changes routing for *new* windows only, and old windows
drain where they were staged. Data at rest never moves to chase the ring.

## 7. Incarnation fencing (§5.7)

Every process boot executes **FenceBoot**: the node draws a fresh
`incarnation` from a catalog-DB sequence and persists it locally. Every
message — Forward, PeerApply, Receipt, Heartbeat, drain commit — carries
`(node_id, incarnation)`. Peers and the catalog track the highest incarnation
seen per node and **reject anything older** (FencedZombie): a partitioned
former self that wakes and tries to forward, receipt, or commit is refused
everywhere with a token it cannot forge forward. This is Kafka's epoch
fencing; the catalog sequence gives monotonicity without a coordination
service. (The identity types live in `duckspout-replication`'s `fencing`
module: `Incarnation`, `FenceIdentity`.)

Catalog outage at boot splits two cases, so a rolling restart cannot wedge on
a catalog incident:

- A node with a **persisted incarnation** boots into **replica-only degraded
  mode** (`DegradedBoot`, §3): it applies and receipts replication under its
  existing incarnation but takes no ownership actions (no drains, no
  takeovers — both need the catalog anyway). It promotes itself when the
  catalog returns and FenceBoot completes.
- Only a **genuinely new node** — no persisted incarnation — waits, in a
  typed startup state. It has no identity to be safely partial with.

## 8. DeclareLoss: the ceremony for the day RF was not enough (§5.8)

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

## 9. Hot-disk corruption (§5.9)

Detection needs no scrubber: DuckDB's per-block checksums catch corruption on
any read, and **the drain reads every staged byte**
(`docs/design/drain.md`) — the scrub is the pipeline. On a checksum failure:

1. **Quarantine** the affected window (it stops serving reads and is excluded
   from drain).
2. **Re-fetch** the exact `(origin, seq)` ranges from a replica via the
   catch-up path (section 4) — the same query, the same PeerApply machinery,
   nothing corruption-specific.
3. Re-verify and release the window back to normal life.

A double failure — the replica's copy is also bad or gone — escalates to
DeclareLoss (section 8). There is no repair mode that guesses; there is
re-replication or the ceremony. WAL-replay corruption reports route to the
same quarantine/re-fetch path.

## 10. Rolling restarts (§5.10)

Planned restarts must not trigger the machinery built for deaths. The
shutdown sequence (in-daemon, behind SIGTERM; any preStop hook is a thin
delay only):

1. Fail readiness — the balancer stops sending new accepts.
2. Finish in-flight Forwards and flush replication so every acked byte is
   receipted at full RF.
3. Write an **advisory `draining(restart, expected_back_by)` row** to the
   registry — this shape is the protocol statement; §9.1.2
   (`docs/operations.md`) references it.
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
  death path of section 6; the suppression-expiry constant is verified in the
  §3 model with a deliberately-broken never-expires variant
  (`docs/verification.md`).

## 11. Configuration surface of this section (§5.11)

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
degraded-ack knob). Everything else this document describes is protocol,
not policy.
