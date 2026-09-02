# ADR-0010: With the DuckLake backend, the watermark lives in the lake

- Status: accepted (2026-09-01)
- Deciders: supervising session, from spike evidence (charter protocol,
  issues #25, #27, #28; spike epic #21)

## Context

`docs/design/drain.md` (§6.4) requires the watermark to advance atomically
with the file registration — "no window, not a millisecond" — and
`docs/design/query.md` (§7.3) described `duckspout.watermarks` as a plain
Postgres table written in the same catalog transaction as LakeCommit. The
spike measured what DuckLake actually offers:

1. The ducklake extension exposes **no way to piggyback an arbitrary
   catalog-DB write** onto its metadata transaction (#25): a plain
   Postgres `duckspout.watermarks` table cannot ride the extension's
   commit.
2. `CALL ducklake_add_data_files` **does participate in an explicit DuckDB
   transaction**, and a table living *inside the DuckLake catalog* commits
   or vanishes with the file registration as one unit — commit-then-both /
   abort-then-neither / crash-then-neither all proven by test (#25,
   `spike/tests/drain.rs`).
3. DuckLake **pins snapshots**: an open client transaction sees an
   immutable {file set, watermark} pair while drains commit concurrently,
   because the watermark is itself lake data under the same snapshot
   (#28, `spike/tests/pinning.rs`). The cold half of bind-time pinning
   (§7.5) is engine-given, with zero extension coordination.
4. The union tiles **exactly** under the §7.5 shape (#27,
   `spike/tests/union.rs`): after a drain, moved rows exist on both sides
   and the watermark bound alone keeps total == distinct == ingested. The
   torn-reader experiment reproduced the double count (1400/1000 rows when
   the two branch bounds are read across a concurrent LakeCommit) and
   proved the cure is **one watermark read for both branches** — any
   single consistent bound tiles exactly; staleness costs freshness, never
   correctness. A reader transaction pins {watermark, data} across a
   concurrent LakeCommit *because the watermark is lake data* — the
   consistency falls out of this ADR's decision and would need manual
   re-establishment under the rejected alternative.
5. DuckLake supports **no UNIQUE constraints** and will double-register
   the same physical file (#25) — the §6.6 SingleDrainCommit fence cannot
   be a lake-side constraint.

## Decision

With the DuckLake backend, `duckspout-lake-ducklake` commits the
per-partition watermark **as a lake table riding the same DuckLake
snapshot commit** as the file registration. Binds read the watermark
through the pinned lake snapshot — one snapshot yields a mutually
consistent {file set, `complete_through`} pair, which is the §7.5
double-count guarantee by construction. The registry's
`duckspout.watermarks` row of §7.3 is *realized* as this lake table; its
guarantee ("advances only via LakeCommit, transactional, authoritative")
is unchanged and strengthened.

Consequences accepted with it:

- Catalog access goes **through the ducklake extension's committed
  surface** (`ATTACH 'ducklake:…'`, `ducklake_add_data_files`,
  `AT (VERSION => v)`), not by writing DuckLake's internal catalog schema
  directly — R-third-party-first applied to the interface; the internal
  schema stays free to move under DuckLake releases.
- The **SingleDrainCommit fence moves above the port** — implemented and
  proven (PR #147, `tests/racing.rs` + the contract suite's
  `racing_drains`): DuckLake's snapshot-commit conflict on the fence row
  delivers exactly-one-winner, **but only together with
  `ducklake_max_retry_count = 0`** — DuckLake's default conflict retry
  silently rebases and replays the losing transaction after the winner,
  which is precisely the blind retry §6.5 forbids. The pin is set at
  connection open and is load-bearing; a DuckLake release changing that
  setting's name or semantics breaks the fence and must be caught at
  version certification. The loser classifies `Aborted` on local
  catalogs (in-process executor — no lost-response channel) and resolves
  via read-back; only a Postgres catalog can yield genuine
  `Indeterminate`.
- Readers must **never** consult the raw `__ducklake_metadata_*`
  passthrough for coverage — it is not snapshot-pinned (#28). The
  extension obligations are tracked in #118.

## Candidates considered

- **Direct SQL against DuckLake's documented catalog schema** (file
  registration + plain Postgres watermark + UNIQUE fence in one Postgres
  transaction): preserves §7.3's original wording and a SQL-native fence,
  but binds DuckSpout to DuckLake's *internal* schema as a compatibility
  surface, forfeits the engine-given snapshot consistency of finding 3,
  and re-opens the two-store pinning problem the design exists to avoid.
  Rejected while the extension surface suffices.

## Revisit when

- DuckLake grows a transactional application-table or UNIQUE/fencing
  surface (revisit the fence mechanism), or
- the Iceberg backend lands (#66-era): Iceberg's registry is its own
  Postgres (§7.3) and needs its own instance of this decision, or
- #36's racing-drains test cannot prove the snapshot-conflict fence.
