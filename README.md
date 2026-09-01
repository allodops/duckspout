# DuckSpout

> Seed README (Ⓢ) — the full §1 content (pillars table, doorway test,
> honest-gap section) lands at absorption (docs/seed.md s§10).

Object storage made the cold tier highly available; nobody has done the same
for the hot tier. In the DuckDB world everything shared routes through S3 or
a catalog database, and nothing keeps the newest data available when a
process dies: DuckDB is a single-writer in-process engine that shares its
host's failure domain, and the lake formats that grew around it — DuckLake,
Iceberg — are durable precisely because they push every byte through object
storage and every commit through a catalog, which makes them batch-latency by
construction. Between "in this process, this instant, gone on crash" and "in
the lake, durable, minutes old," the ecosystem has a hole exactly one
replication protocol wide. The operational question DuckSpout exists to
answer: **the node holding the last five minutes just died — where are the
last five minutes?**

DuckSpout fills that hole: a durable, replicated, queryable-hot ingestion
layer with completeness semantics for immutable streams, draining into lake
formats — DuckLake first, Iceberg by contract. Four pillars, each closing one
way the last five minutes can be lost or lied about: **durable ack** (an ack
is issued only after local fsync and RF replication receipts), **RF
replication** (a dead owner's partitions are taken over by a receipted
replica), **queryable-hot** (data is SQL-queryable the instant its
transaction commits), and **completeness** (per-partition watermarks;
"no rows" and "couldn't check" are never the same answer). OTLP is the first
accept adapter, not the identity — any immutable stream is DuckSpout data.
DuckSpout gives the hot tier the availability the lake already has.

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
