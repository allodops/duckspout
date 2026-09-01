# CONSTITUTION

The rules that gate every change in this repository. Two tiers:

1. **Keep Rules R-1..R-12** — quoted from `DUCKSPOUT.md` §11 (the twelve, and
   only those): the invariants whose silent violation is a correctness
   regression rather than a re-review. The quotes below are §11's full rule
   text, **verbatim — confirmed at absorption** (docs/seed.md s§10).
   Everything else — naming, style, toolchain, process — is ordinary
   revisable judgment and earns no ceremony; a rule that is really policy
   does not get Keep-tier protection merely because changing it later would
   be inconvenient (§11).
2. **Seed rules** — added by the seed (docs/seed.md s§8.2), same format.

Every rule carries an **enforcing mechanism** of exactly one of four kinds
(the `constitution-mechanism` pairing check in `invariants.toml` fails CI if
any rule lacks one): an **armed CI job**, an **invariants rule**, a
**CODEOWNERS path**, or a **staged ledger row** in `docs/arming-ledger.toml`
(milestone + tracking issue) — the last is the honest interim state for rules
whose enforcing gates arm at v0.1–v0.2. Ledger row ids below cite the issue
numbers of the pre-created issue tree (docs/seed.md s§11 step 5); the ledger's
own `issue` fields are written by the step-5 fill PR.

§11's frame binds every mechanism here: *"Where a mechanical check cannot
determine whether something violates a rule, it treats it as if it does:
**ambiguity fails closed**, and narrowing a check to dodge an ambiguous case
is the same offense as weakening the check."*

## Keep Rules (DUCKSPOUT.md §11)

| # | Statement (quoted) | Mechanism | Amendment |
|---|---|---|---|
| R-1 | "**No ack before durability.** ClientAck is issued only after the batch is fsynced in local staging and replication Receipts bring the total durable copy count to RF (total-inclusive, §4, §5) — for every non-system tenant. `_`-prefixed system tenants never receive durable acks, so this rule is not implicated by them rather than carved out for them. There is no fast-ack mode, no async-ack mode, and no configuration that weakens this." | Staged ledger rows: `tla-mc-core` (v0.1, #43 — DurableAck) and `ctk-distributed` (v0.2, #58 — NoAckedLoss under fault injection) | s§9.6 procedure (below) |
| R-2 | "**Retries recompute and apply idempotently; they never replay blindly.** A retried request re-enters DedupCheck and resolves to the original outcome; PeerApply refuses gaps in the per-(partition, origin) sequence and applies each record exactly once (§5). Nothing speculatively computed by a failed attempt survives into the retry." | Staged ledger rows: `tla-mc-core` (v0.1, #43 — both DedupCheck branches reachable), `tla-mc-replication` (v0.2, #57 — gap refusal), `conformance` (v0.1, #44) | s§9.6 procedure (below) |
| R-3 | "**Complete by default; ambiguity fails closed everywhere.** The default read concern is `complete`; a read whose coverage cannot be proven returns a typed error, never a silently partial result (§7, WatermarkHonesty). "Empty" and "couldn't check" are never the same answer. `available` is the explicit opt-out, and opting out is disclosed, not defaulted." | Staged ledger rows: `tla-mc-core` (v0.1, #43 — WatermarkHonesty), `conformance` (v0.1, #44) | s§9.6 procedure (below) |
| R-4 | "**Cold objects are immutable-with-expiry.** Exactly one PutPart and one whole-file DELETE per object, never a modification between. A sealed part never spans a tenant, a retention class, or a dataset kind. All merging is hot-side, in the drain, before the PUT — never compaction on object storage." | Staged ledger rows: `tla-mc-core` (v0.1, #43 — A3 discipline in Drain.tla), `conformance` (v0.1, #44 — real-backend PUT/DELETE traces) | s§9.6 procedure (below) |
| R-5 | "**Acked data leaves the staging class only by successful drain.** Staging is never evicted; Throttle and Refuse are always preferred over staging loss (§4). Cache eviction is always legal, requires no coordination, and cannot violate this rule by construction — a datum is cache class only after its LakeCommit is durable." | Staged ledger rows: `tla-mc-core` (v0.1, #43), `ctk-distributed` (v0.2, #58 — ladder-under-pressure faults) | s§9.6 procedure (below) |
| R-6 | "**`complete` reads depend on staging ∪ lake alone.** No cache state may gate them: any two cache states, including empty, yield the identical row set (CacheTransparency, §3). A cache miss can cost latency, never correctness, and cache occupancy can never affect ingest availability." | Staged ledger row: `tla-mc-core` (v0.1, #43 — CacheTransparency) | s§9.6 procedure (below) |
| R-7 | "**Transforms are SQL, applied after durability, re-runnable.** Never a DSL, never destructive, never on the ack path. A transform that cannot be re-run from retained inputs is not a transform; it is data loss with extra steps." | Staged ledger row: `conformance` (v0.1, #44) — the §3.3 vocabulary has no transform action on the ack path, so TraceComplete flags any unmodeled ack-path transition | s§9.6 procedure (below) |
| R-8 | "**Discovery and placement are advisory, never load-bearing.** The registry's nodes, claims, and locations are soft state; a wrong or stale entry costs a redirect or a slower resolution, never a wrong answer (§5). Correctness authority lives in watermarks, manifests, and the drain guard — all reconstructible, none advisory." | Staged ledger row: `tla-mc-replication` (v0.2, #57 — claims modeled as advisory soft state; invariants never read them) | s§9.6 procedure (below) |
| R-9 | "**The data path survives a catalog-DB outage.** Ingest, replication, and already-resolved hot query service proceed; new bind-time resolution and drains pause, and say so (§9). No timer ever converts a disclosed catalog outage into an ingest outage or a denial of already-resolved service." | Staged ledger rows: `ctk-distributed` (v0.2, #58 — catalog-outage fault family), `ctk-release-gate` (v0.3, #60) | s§9.6 procedure (below) |
| R-10 | "**A changelog part may be expired only when a sealed snapshot part covers its arrival range.** Uncovered changelog parts are keep-forever. Held formally by `Expire`'s guard and the `SnapshotCovered` invariant (§3), with the `ExpireUncovered` armed variant and the §8.4 retention judge as its teeth. SnapshotSeal appends a new object and never modifies an existing one — the ban is on rewrite, not on derivation (§6)." | Staged ledger rows: `tla-mc-core` (v0.1, #43 — SnapshotCovered + ExpireUncovered broken variant), `ctk-release-gate` (v0.3, #60 — retention judge) | s§9.6 procedure (below) |
| R-11 | "**Coverage, mutation, and performance floors are checkable numbers CI recomputes on every change** (§8). Raising or lowering one goes through ordinary review like any other change — never through the change it would unblock. Every checked property keeps its armed deliberately-broken variant, and CI fails the moment that variant stops reproducing its own violation: proof the check bites." | Staged ledger rows: `coverage-floor` (v0.1, #45), `instr-gate` (v0.1, #46), `smoke` (v0.1, #47), `mutation-floor` (v0.1, #49 — nightly by ADR-0009's flagged deviation), broken-variant arming via `tla-mc-core` (v0.1, #43) | s§9.6 procedure (below) |
| R-12 | "**The config surface is a ratchet.** The knob table is the config appendix (§9) and is the measured baseline; every addition carries a divergent-workload justification in its own review, counted against the true total — which is the **settings** count (32), not the row count: rows are presentation, settings are the ratchet (§9.6.1). Dataset-declaration attributes live in their own closed ledger under the same test. A constant that needs no divergence is a constant, not a knob; a reserved-but-uncounted knob is ratchet theater and is refused." | Invariants rule: `golden-manifest` (daemon `--dump-config-manifest` diffed against `floors/config-surface.toml`), via the armed CI job `invariants` | s§9.6 procedure (below) |

## Seed rules (docs/seed.md s§8.2)

| # | Statement | Mechanism | Amendment |
|---|---|---|---|
| R-third-party-first | Third-party-first is constitutional (D-12): an in-house build where a third-party candidate exists requires an ADR naming the candidates, why they lost, and a revisit trigger. Recorded exceptions at seed: HRW hashing (ADR-0004), the invariant engine (ADR-0007). | CODEOWNERS path `/docs/adr/` — the required ADR is human-gated; reviews verify the candidates/revisit sections exist | s§9.6 procedure (below) |
| R-no-bash | No `*.sh` or `*.bash` file exists anywhere, ever; scripting is Bun `.mjs` under `scripts/` (D-7). | Invariants rule: `banned-file` globs, via the armed CI job `invariants` | s§9.6 procedure (below) |
| R-determinism | Protocol crates reach time, randomness, network, and processes only through the port traits in `duckspout-types` (D-2): `tokio::net`, `Instant::now`, `SystemTime::now`, `thread_rng`, `std::process` are banned in their sources. | Invariants rule: `banned-source` (D-2 pattern set), via the armed CI job `invariants` | s§9.6 procedure (below) |
| R-armed-or-ledgered | Every gate is either an armed CI job or a staged ledger row (milestone + tracking issue) in `docs/arming-ledger.toml` — never a skipped-green job, never silently absent (D-1, s§6.5). | Invariants rules: `ledger-integrity` + `constitution-mechanism` pairings, via the armed CI job `invariants` | s§9.6 procedure (below) |
| R-protected-set | Changes to the protected set (s§9.2: the gates' data **and** executables, plus the decision record) merge only with CODEOWNERS (human) approval (D-9). | CODEOWNERS paths — the s§9.2 block in `/CODEOWNERS`, which owns itself | s§9.6 procedure (below) |
| R-acpr-session | ACPR (adversarial critic pass review) is **not mechanical** — no CI job, no required check. The supervising session performs it, at its own judgment, on changes to core features (protocol crates, specs, ports, gates); any confirmed finding is addressed or explicitly rebutted before merge (owner ruling 2026-09-01, amending D-10). | Session practice, recorded in AGENTS.md; not machine-enforced by design | s§9.6 procedure (below) |

## Canary discipline (recorded per s§9.3)

The mechanical gates' bite is proven, not assumed: seed canaries at s§11
step 4, then quarterly blind canary DRAFT PRs via `canary-reminder.yml` —
one mechanical flaw per PR, so each gate's catch is demonstrated
independently. Outcomes are recorded in `docs/seed.md` after closure.
ACPR is session-level judgment (R-acpr-session), not a gate, and has no
canary.

## Retired rules

**Absorption rule** (formerly the seed rule guarding `DUCKSPOUT.md` as the
source of truth until absorbed, docs/seed.md s§10) — retired 2026-09-01 by
the s§10 completion PR: all 12 sections absorbed (issues #8–#19), the
completeness audit passed after its blocker fixes (PR #92; §3.2–3.4 live
verbatim in `specs/formal-core.md` until the modules land), and the
monolith was deleted.

## Amendment procedure (docs/seed.md s§9.6)

- **Protected-set changes** (this file included): a normal PR + `ci-ok` +
  CODEOWNERS human approval. No other path exists — including for
  amendments to this procedure.
- **Everything else**: fully autonomous — PR + `ci-ok` + auto-merge.
- Keep Rules may be tightened or loosened only through that review; a diff
  touching a threshold, an allowlist, or a check's scope is read line by
  line, never taken on the change's own explanation (§11). Settled decisions
  (docs/seed.md s§1, ADRs) are amended here, never re-litigated in ordinary
  PRs.
