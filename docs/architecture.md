# Library Architecture and Extensibility (§10)

Absorbed from DUCKSPOUT.md §10 per docs/seed.md s§10. Section labels
(§10.1 … §10.4) are preserved so citations elsewhere — the ADRs, AGENTS.md,
`invariants.toml` — keep resolving after the monolith is deleted. The
layering statements here were re-verified against docs/seed.md s§4 and
ADR-0008 during absorption, as ADR-0008's revisit clause requires; the
as-built refinements are flagged inline, never silently substituted.

DuckSpout is a library first. The deliverable is an embeddable Rust core
whose protocol crates each stand alone; the daemon is a thin composition of
them, and the DuckDB extension is a thin client to them. Anything the
daemon can do, an embedder can do by depending on the crates directly — the
daemon holds no logic of its own beyond wiring, signal handling, and the
cadence loop that ticks drains and retention.

## §10.1 Repository and crate layout

Two repositories. The split is dictated by the DuckDB community-extensions
registry, whose CI builds an extension with cmake from the repository root
— an extension folded into a Cargo workspace monorepo cannot satisfy that
shape.

**Repo 1 — `duckspout` (Cargo workspace monorepo).** This repository;
every crate lives under `crates/` in a full-name directory
(`crates/duckspout-types`, `crates/duckspout-accept`, …).

| Crate | Owns | Embeddable alone |
|---|---|---|
| `duckspout-types` | Wire/domain types: tenant, dataset declaration (`kind`, `key_cols`, `sort_key`), partition, window, part manifest (incl. `dedup_removed`), watermark rows, the status enum, the OTLP error table (§4). No I/O. | yes |
| `duckspout-accept` | Accept, DedupCheck, Throttle, Refuse: listeners, admission caps, the overload ladder's client-visible half, the OTLP adapter behind the AcceptAdapter port (§4). | yes |
| `duckspout-staging` | StageCommit: hot DuckDB lifecycle — micro-window tables, the dedup window table committed in the same transaction, staging/cache class metadata, Demote/Evict/DropWindow (§4, §2). | yes |
| `duckspout-replication` | Forward, PeerApply, Receipt, ClientAck sequencing; HRW ring, ClaimAdvertise, Heartbeat, FenceBoot, DegradedBoot, TakeoverDrain, DeclareLoss (§5). | yes |
| `duckspout-drain` | SealPart, PutPart, LakeCommit choreography (incl. the atomic WatermarkAdvance and Reconcile), keep-latest drain dedup, SnapshotSeal, retention Expire, EvolveSchema ordering (§6). | yes |
| `duckspout-watermark` | The watermark ledger, read-concern gating (`available`/`complete`), `duckspout_freshness()` logic, loss-ledger accounting (§7). | yes |
| `duckspout-lake-contract` | The LakeCommitter port: trait, semantics, and the **published conformance suite** any backend must pass (§10.3). | yes |
| `duckspout-lake-ducklake` | The first LakeCommitter: Parquet written by DuckSpout, committed via an embedded DuckDB used purely as a metadata-commit executor — rows never transit it. | yes |
| `duckspout-ctk` | The Conformance Testing Kit (§8): deterministic simulation of the action vocabulary, fault injectors (CrashNode, RecoverNode, partitions, pauses, membership churn), trace-conformance checking against the §3 model, and the armed broken variant of every checked invariant. | dev-dep |
| `duckspout-daemon` | The binary: config parsing (one TOML file, env overrides; the `TlsMode` type deliberately has no `Default` impl — TLS posture is stated or the config is invalid, §9.5), listener wiring, the cadence loop, systemd/K8s lifecycle (§9). No protocol logic. | n/a |

As built, the workspace carries four further bin/tool crates the table
above predates — none of them protocol crates, all downstream of the same
layering rule: `duckspout-ctl` (the `duckspoutctl` operator CLI; first
subcommand `size`, §9.2), and the §8.4 distributed-tier machinery
`duckspout-fleet` (fleet runner), `duckspout-loadgen` (the journaling load
generator — a first-class fleet member), and `duckspout-judge` (the
post-pass judge binary), built out at v0.2 (issue #56).

The dependency direction is one-way: protocol crates depend on
`duckspout-types` and on each other's ports, never on the daemon, never on
a concrete lake backend. **ADR-0008 refines that port wording as-built**:
read as crate dependencies, "each other's ports" would create
protocol×protocol edges and cycles, so every cross-crate-consumed port
trait is *defined* in `duckspout-types` and re-exported by its home crate —
protocol crates consume each other's ports while all protocol×protocol
crate edges stay banned. `duckspout-drain` depends on
`duckspout-lake-contract`, not on `duckspout-lake-ducklake` (the sole
protocol→contract edge, docs/seed.md s§4); the daemon selects the committer
at composition time. CI audits the dependency graph and fails the build on
any edge from a protocol crate to a concrete port implementation — the
checkable form of the extensibility claim, because a core that could
quietly reference one backend can be coupled to it before a second backend
exposes the coupling. As built this audit is the invariant engine's
hand-enumerated forbidden-edge list (`invariants.toml`, run by
`just invariants` on every PR).

**Repo 2 — `duckspout-duckdb` (the extension).** Shaped exactly like the
community-extensions template so registry CI builds it unmodified. It is a
client: it implements the `ATTACH 'duckspout:...'` catalog, bind-time tier
resolution (§7), the hot∪cold union with generation COALESCE (§7.5),
read-concern settings, and `duckspout_freshness()`. A checked-in
compatibility matrix (extension version × DuckDB version × daemon protocol
version) is validated by CI in both repos — Repo 1's copy is
`compat-matrix.toml` at the workspace root. Repo 2 does not exist yet: its
charter and build-out are milestone v0.4 work (issue #66); the riskiest
extension seam — transaction-lifecycle pinning — is prototyped throwaway in
`spike/` first (docs/seed.md s§11 step 7).

## §10.2 Rust at the core, C++ only at the engine wall

DuckDB's boundary has two very different faces, and DuckSpout sits on the
correct side of each.

**Inside the engine, the power APIs are C++-internal with no stable ABI.**
Catalog providers, transaction hooks, and custom settings are reachable
only through DuckDB's internal headers; every extension is a version-locked
rebuild against a specific engine release. There is no avoiding C++ there,
so the extension is C++ — but only for the three capabilities that
genuinely demand internals:

1. **Catalog integration** — the `duckspout:` ATTACH type and its
   bind-time resolution.
2. **Transaction-lifecycle pinning** — the coverage snapshot pinned for a
   transaction's duration, killing the data-vs-coverage TOCTOU that makes
   `complete` reads honest (§7.6, WatermarkHonesty).
3. **Custom settings and typed errors** — `duckspout_read_concern`, strict
   fail-closed errors raised mid-scan.

Everything else the extension needs — value construction, scan output,
client protocol — goes through the stable C API, minimizing the
version-locked surface to the smallest slab that buys the semantics.

**Outside the engine, embedding DuckDB uses the stable C API.** The
daemon's staging and drain crates drive DuckDB as a library through that
boundary, which is versioned and supported; the daemon is not a
version-locked artifact and upgrades independently of the engine release
cadence. All first-party C++ therefore lives in Repo 2, none in Repo 1;
compiling the `duckdb` crate's `bundled` engine inside this workspace's
build does not breach that wall — the rule bans *authoring* C++, not
building upstream's as shipped (ADR-0002, with `compat-matrix.toml` pinning
the one engine version).

**Rust for the core is chosen on merits, not ideology:** memory safety at
the one place DuckSpout parses untrusted bytes off the network (Accept); no
garbage collector on the ClientAck path, where fsync latency is the budget
and a pause is a p99 regression (§4); mature ecosystem exactly where
DuckSpout needs it — arrow-rs and parquet-rs for part writing, object_store
for PutPart against every major store, tonic for gRPC/Flight; and the
property-test and mutation-test toolchain (§8) that the verification
posture requires as a first-class citizen, not an afterthought.

## §10.3 Extensibility ports

DuckSpout is extensible where workloads genuinely diverge and closed where
a port would be an invitation to fork semantics. Every port ships with a
published conformance suite; an implementation that has not run the suite
is not a supported implementation. Per ADR-0008, each port's trait is
defined in `duckspout-types` and re-exported by the home crate named below,
which owns everything beyond the bare signature — adapter registration, the
conformance suite, outcome helpers.

| Port | Status | Contract in one line |
|---|---|---|
| **LakeCommitter** | v0.1, two planned backends | Six operations (§6.4): `commit_files` (atomic {add files + watermark}), `replace_files` (emergency repair only), `evolve_schema` (monotone), `expire`, `read_watermarks`, `attach_info`. Watermarks ride the commit as the portable contract (§6); nothing on the critical path may depend on a backend-exclusive feature. DuckLake is first; Iceberg is by-contract from day one, kept honest by the conformance suite even before the backend ships. |
| **AcceptAdapter** | v0.1 (OTLP built in) | Decode a protocol's payload into typed rows plus per-item reject verdicts; the durability, dedup, and error vocabulary stay in the core. OTLP (gRPC + HTTP) is the shipped adapter; other protocols are the edge collector's job first (§10.4), an adapter second. |
| **Typing engine seam** | v0.1 seam, optional impl | Schema-later ingestion: monotone type-widening lattice, dot-notation flattening, JSON terminal fallback (§2, §4.8). The default engine is DuckSpout's own fixed-schema OTLP mapping; RawDuck's engine is the optional alternative — never load-bearing, never a critical-path dependency. |
| **Residency/pin policy seam** | parked | The cache class (§2) is doctrine now, mechanism later; the seam exists so the SLRU and pin design land without touching protocol crates, but it is deliberately not a public port until the warm-retention trigger fires (§12.7). |
| **Transform stages** | SQL, permanent posture | Transforms are SQL applied after durability, re-runnable, never destructive (§11 Rule 7). There is no transform plugin API and none is planned — SQL is the extension language. |

## §10.4 Integration posture — what DuckSpout deliberately does not build

DuckSpout's edge in the ecosystem is knowing which halves of the problem
are already solved and refusing to re-solve them.

- **otel-collector is the edge.** Protocol fan-in, batching, edge
  buffering, and exotic-source support belong to the collector; DuckSpout
  ships a recommended collector configuration (persistent queue, raised
  retry horizon — §4) rather than a protocol zoo.
- **Airport (Query.Farm) is the entire client half of remote-hot.**
  `ATTACH (TYPE AIRPORT)` mounts any Arrow Flight server as a DuckDB
  catalog with pushdown; DuckSpout builds only the server half. Local hot
  is served over Flight even same-machine, respecting DuckDB's
  single-writer-process model.
- **DuckLake and Iceberg own the cold tier.** Catalog transactions,
  snapshot isolation, time travel, and file-level metadata are the lake's;
  DuckSpout writes immutable-with-expiry parts and commits them (§6). It
  never re-implements a table format.
- **RawDuck is prior art and a potential partner, not a dependency.** Its
  schema-later typing lattice maps cleanly onto DuckSpout's monotone
  evolution; DuckSpout's durability, replication, and lake layers are
  exactly what it lacks. The relationship is pursued as collaboration
  (§12.8); the default path never requires it.
- **Not built, ever:** a query engine (DuckDB is the query engine; the
  extension resolves, it does not execute — joins run inside the querying
  DuckDB, §7); a transform DSL (SQL is the DSL); a coordinator (the data
  path is coordinator-free by design, §5 — the catalog DB arbitrates only
  maintenance, and discovery is advisory).
