# DuckSpout

DuckSpout is a durable, replicated, queryable-hot ingestion layer with
completeness semantics for immutable streams in the DuckDB ecosystem,
draining into lake formats — DuckLake first, Iceberg by contract.

> Section citations (`§n`) refer to [`DUCKSPOUT.md`](DUCKSPOUT.md), the design
> document being absorbed into `docs/` (docs/seed.md s§10). This page absorbs
> §1.

## The defining question

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

## The product is the four pillars

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

## The honest gap: what exists and what doesn't

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
repository exists to fill it without rebuilding what the ecosystem already
does well: DuckSpout builds no query engine (DuckDB is the query engine), no
lake format (DuckLake and Iceberg are the lake formats), no wire protocol
(OTLP and Arrow Flight are the wire protocols). The irreducible new build is
the ack path, the replication of the accepted stream, the watermark ledger,
and the drain choreography — exactly the four guarantees nobody ships.

## Scope doctrine: facts, not state

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

## Who deploys this — and who should not

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

## Status

**Pre-implementation, at bootstrap.** This repository is the executed seed of
the blueprint in [`docs/seed.md`](docs/seed.md): the workspace skeleton, the
gates, and the autonomous agent loop exist; the protocol implementation
starts at v0.1. The design authority is [`DUCKSPOUT.md`](DUCKSPOUT.md)
(committed temporarily as the absorption input; it is decomposed into
`docs/` and deleted when absorption completes).

**Quality note:** everything up to and including v1.0 is
**alpha/preview** quality — the versions are contracts about which
invariants are armed and verified, not maturity claims.

## Pointers

- [`docs/seed.md`](docs/seed.md) — the seed blueprint (historical record).
- [`DUCKSPOUT.md`](DUCKSPOUT.md) — the full design document.
- [`CONSTITUTION.md`](CONSTITUTION.md) — the Keep Rules and their mechanisms.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — DCO, Conventional Commits, PR
  expectations.

## License

Apache-2.0.
