# Deferred Register and v1 Cut List (§12)

Absorbed from DUCKSPOUT.md §12 per docs/seed.md s§10. The milestone
narratives (§12.1–§12.6) live as the GitHub milestone descriptions —
verified against the source at absorption — and the issue tree under them;
this document is the home of §12's registers: the deferred register
(§12.7), the collaboration sequencing (§12.8), the license posture
(§12.9), and the v1 cut list (§12.10). §12's frame binds all of them:
each milestone makes one pillar true and verifiable before the next widens
the blast radius; verification grows in-milestone, never trailing (§8);
**versions are contracts about which invariants are armed, not dates.**

> **Owner ruling (quality bar):** `v1.0 — hardened` is still
> **alpha/preview quality** — "hardened" refers to the gate/verification
> discipline, not production maturity (recorded at issue-tree creation,
> docs/seed.md s§11 step 5, and on the milestone itself).

## §12.7 Deferred register

Every deferral has a design-of-record and a named trigger; nothing is
deferred into vagueness. The Deferred and Trigger columns are §12.7's text
verbatim; the design-of-record column names where each parked design is
written down, so the register carries both halves of the rule it states.

| Deferred | Design-of-record | Trigger |
|---|---|---|
| Warm retention, SLRU, `residency` attribute, rung-0 eviction | The two-class doctrine and cache-transparency theorem (§2.4); Demote/Evict/DropWindow class mechanics (§6.9); the parked SLRU ratio and rung-0 low-water constants (§9.6.3, docs/operations.md); the residency/pin seam (§10.3, docs/architecture.md); the deciding experiment (defined below, absorbed from §12.5) | The v0.4 experiment shows a measured win over querier-local caching |
| Pin (DDL residency `pin`, refuse-new/degrade-grown, fixed cap) | The residency/pin seam (§10.3); the `residency` attribute is listed-but-uncounted in the dataset ledger (§9.6.2); the pattern it must beat is the documented temp-table materialize-and-refresh join (§7.7) | Measured demand the documented temp-table join pattern cannot serve |
| Hot LATEST projection (async-maintained, evict-last under drain stall) | The `<dataset>_latest` argmax view and the two freshness modes it must not blur (§7.7); the projection's own shape is the parenthetical here — async-maintained, evict-last under drain stall | Argmax view + client patterns measurably insufficient for dimension serving |
| Seq-versioned snapshot/revalidation endpoint; keyed diffs behind it | The materialize-and-refresh cadence it would replace (§7.7); versioning rides the per-(partition, origin) seq vocabulary that already orders the changelog (§5) | Revalidation traffic dominates full refresh |
| Cache-coverage advertisement in the registry | Pre-stated amended Tier rule at §7.2 — written down precisely so this deferral has a design-of-record: a covered range may be served from an advertised cache-class holder, transparently by CacheTransparency, tier boundary unchanged; armed doctrine in §2.4's obligations matrix and the `dedup_removed` demote gate (§6.9) | Post-takeover latency cliff or single-owner saturation observed |
| Predicate sketches and the pin-candidates advisor | The advisees come first by construction: the pin design (§10.3 seam) and the shipped per-dataset counters are the register's own prerequisite — the advisor's design is deliberately no more than this entry until they exist | The shipped per-dataset counters show a real signal — the advisor never precedes its advisees |
| Iceberg LakeCommitter backend | The LakeCommitter port contract and published conformance suite (§6.4, §10.3, `duckspout-lake-contract`) | Contribution-ready from v0.1: the contract and conformance suite are published so the backend can be built, by anyone, against a spec — shipped when a maintainer (internal or external) carries it through the suite |

(§ citations resolve to the absorbed design docs — §2/§6/§7 →
`docs/design/*.md`, §9 → `docs/operations.md`, §10 →
`docs/architecture.md` — and to DUCKSPOUT.md until its deletion PR.)

## §12.8 Collaboration sequencing

The RawDuck conversation opens early and stays scoped: DuckSpout as the
durability/replication/lake layer their engine lacks, their typing engine
as an optional seam here — optional, never load-bearing, on both sides.
The community OTLP-extension effort is tracked for schema interoperability
after the spike, never adopted as a dependency. DuckLake upstream
engagement (catalog-level contributions) waits until v0.3 ships evidence —
proposals travel better with a bench card attached.

## §12.9 License

Apache-2.0 everything; DCO sign-off. One pre-declared contingency: if a
hosted re-sell threat materializes, the daemon — and only the daemon — may
dual-license under AGPLv3. Never BSL or SSPL (the 2024–2026 record of
those moves is one-directional and ends in forks or walk-backs), and never
the extension: the piece that lives inside users' DuckDB stays maximally
permissive, unconditionally.

## §12.10 v1 cut list (condensed)

Deliberately absent, each with its compensating story: behavioral
multi-tenancy (structure ships; limits, retention classes, metering
deferred); all rate limits (memory bound + the ladder govern the real
resources); degraded-RF ack mode (refuse-only); the Iceberg backend
(contract + conformance suite instead); RawDuck as a default path; result
caching; non-OTLP accept adapters (the collector is the protocol
strategy); automatic rebalancing (takeover-on-death only; new windows
route to new membership); cross-region replication; a background scrubber
(the drain's full read is the scrub); a shipped external monitor (the
canary recipe is shipped; running it outside DuckSpout's failure domain is
the operator's half); any transform DSL (permanent, Keep Rule 7).

The cut list is audited before v1.0 ships (issue #74: cut-list audit +
deferred register current) — each absence must still have its compensating
story, and each register entry a live design-of-record and trigger.

## Absorbed §12 milestone narratives (audit blockers B-1..B-3)

Normative fragments that lived only in §12's prose, homed here so the
milestone contracts survive the monolith's deletion (each also reflected in
its GitHub milestone description and tracking issue):

**The warm-retention experiment (§12.5, design-of-record for the
warm-retention register entry above):** "The warm-retention experiment is
defined and run — N ephemeral queriers, shared working set, RF=2, durable
acks, versus querier-local caching — and its result is the gate for the
parked cache class."

**The SCD-2 SQL spike (§12.4):** "The SCD-2 SQL spike (LEAD-over-key
validity composed with generation COALESCE) runs before the docs promise
AS-OF-by-SQL."

**Changelog version scoping (§12.3–§12.4):** changelogs land at v0.2 as
declaration + drain, GA-gated on v0.3's snapshot rollover; snapshot
rollover under the drain scheduler, the `<dataset>_latest` views,
`dimension_as_of`, and `duckspout_freshness()` land at v0.3. Versions are
contracts about which invariants are armed; this scoping is normative.
