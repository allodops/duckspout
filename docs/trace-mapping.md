# Trace mapping — variant ↔ tracepoint

Complete at bootstrap (D-6, docs/seed.md Appendix B). One row per variant of
the trace-event enum in `duckspout-types`; the invariant engine's
`trace-mapping` pairing rule fails CI when the enum and this table drift.
Event names are the §3.3 action names, **verbatim**, under §3.7's journaling
rules; the absorption pass re-verifies this table against `DUCKSPOUT.md`
before the monolith is deleted.

Vocabulary rules carried by this table (§3.7, §6.4, §3.3):

- A commit journals its **outcome name** — there is no bare `LakeCommit`
  event; the implementation cannot know which Indeterminate successor the
  model took (`IndeterminateLanded` vs `IndeterminateLost`), so it journals
  the one name `LakeCommitIndeterminate` and the following `Reconcile` names
  the resolution.
- `WatermarkAdvance` is **not a variant** — it rides the LakeCommit outcome
  atomically (§6.4).
- `RecoverNode` is defined as `FenceBoot` (§3.3): recovery journals as
  `FenceBoot`; there is no separate variant.
- `ClientTimeout` is journaled **only by `duckspout-loadgen`** (a fleet
  member, §8.4), never by a node.
- `CrashNode` / `CrashWipe` are **environment events, never journaled**: the
  enum carries them in a separate environment-event type used only by the
  CTK's schedule stream, so a node emitting one is a type error.

## Node-journaled variants (27)

| Variant | Emitting module | Spec action § | Notes |
|---|---|---|---|
| `Accept` | `duckspout-accept` | §3.3 Ingest | Admission into volatile memory; no promise. |
| `DedupCheck` | `duckspout-staging` | §3.3 Ingest | Consults the per-node dedup ledger (§4.4); both branches (replay / throttled) journal the same name. |
| `StageCommit` | `duckspout-staging` | §3.3 Ingest | The fsynced hot-store transaction (A1). |
| `ClientAck` | `duckspout-accept` | §3.3 Ingest | Only after StageCommit + RF receipts (Keep Rule 1). |
| `Throttle` | `duckspout-accept` | §3.3 Overload | Rung 2 or receipt-wait expiry; UNAVAILABLE + RetryInfo on the wire (§4.5). |
| `Refuse` | `duckspout-accept` | §3.3 Overload | Rung 3; work never admitted. |
| `Forward` | `duckspout-replication` | §3.3 Replication | Schema state rides in-band (`sAt`). |
| `PeerApply` | `duckspout-replication` | §3.3 Replication | Refuses gaps per (partition, origin); `SchemaKnown` fails closed. |
| `Receipt` | `duckspout-replication` | §3.3 Replication | The durable-copy attestation counted by `AtRF`. |
| `SealPart` | `duckspout-drain` | §3.3 Drain | Coverage + receipted extent recorded at seal. |
| `PutPart` | `duckspout-drain` | §3.3 Drain | The object's one logical PUT (A3). |
| `LakeCommitOk` | `duckspout-drain` | §3.3 Drain | Outcome name; WatermarkAdvance rides it atomically (§6.4). |
| `LakeCommitAbort` | `duckspout-drain` | §3.3 Drain | Outcome name. |
| `LakeCommitIndeterminate` | `duckspout-drain` | §3.3 Drain | Outcome-name rule: one journaled name for both model successors; the following `Reconcile` names the resolution. |
| `Reconcile` | `duckspout-drain` | §3.3 Drain | Exactly one read-back resolves Indeterminate (A2). |
| `Expire` | `duckspout-drain` | §3.3 Retention | The object's second and last storage operation; changelog parts only under a covering snapshot (Keep Rule 10). |
| `Demote` | `duckspout-drain` | §3.3 Post-drain residency | Staging → cache class at durable commit; re-checked on recovery. |
| `Evict` | `duckspout-drain` | §3.3 Post-drain residency | Cache eviction; always legal, rung 0, not a status. |
| `DropWindow` | `duckspout-drain` | §3.3 Post-drain residency | |
| `SnapshotSeal` | `duckspout-drain` | §3.3 Changelog | A new derived object, never a rewrite (A3); fenced on its own key. |
| `ClaimAdvertise` | `duckspout-replication` | §3.3 Membership and failure | Advisory registry row (Keep Rule 8). |
| `Heartbeat` | `duckspout-replication` | §3.3 Membership and failure | |
| `FenceBoot` | `duckspout-replication` | §3.3 Membership and failure | Also the recovery journal entry: `RecoverNode == FenceBoot` (§3.3). |
| `DegradedBoot` | `duckspout-replication` | §3.3 Membership and failure | Catalog-down boot entry; replays the same staging tables. |
| `TakeoverDrain` | `duckspout-replication` | §3.3 Membership and failure | A receipted replica takes over a dead owner's partitions. |
| `DeclareLoss` | `duckspout-replication` | §3.3 Membership and failure | The §5.8 loss ceremony; ledgered, never silent. |
| `EvolveSchema` | `duckspout-staging` | §3.3 Schema | A schema change is a sequenced record: consumes a seq, rides the same log as data. |

## Loadgen-journaled variant (1)

| Variant | Emitting module | Spec action § | Notes |
|---|---|---|---|
| `ClientTimeout` | `duckspout-loadgen` **only** | §3.3 Overload, §3.7, §8.4 | The client's own deadline, not a node action; resolves a request left hanging by a dead or silent acceptor. Journaled by the verifying load generator (a fleet member), never by a node. |

## Environment events (2 — separate environment-event type, never journaled)

| Event | Carrier | Spec action § | Notes |
|---|---|---|---|
| `CrashNode` | `duckspout-ctk` schedule stream | §3.3 Crash and recovery | A crashed node cannot journal its own crash; the trace checker treats it as an unobserved environment step (§3.7). |
| `CrashWipe` | `duckspout-ctk` schedule stream | §3.3 Crash and recovery | Disk death, bounded by the fault budget; environment-only, same rule. |

Emitting-module attributions follow the §3.3/§4–§6 ownership of each action
(accept path §4.5, staging/dedup §4.4, replication and membership §5, drain,
retention, and residency §6); the enum itself and this table are the frozen
vocabulary, and the per-module tracepoints are exercised when trace
conformance arms (ledger row `conformance`, v0.1).
