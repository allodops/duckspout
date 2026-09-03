# The formal core, verbatim (§3.2–§3.4) — interim home

> **Provenance**: DUCKSPOUT.md §3.2 (state space), §3.3 (action set),
> §3.4 (invariants), transcribed verbatim as the interim normative home
> so the monolith can be deleted (docs/seed.md s§10, issue #20) before
> the `.tla` modules land. **Do not paraphrase or edit this content**:
> when the modules land (ledger rows `tla-mc-core` v0.1,
> `tla-mc-replication` v0.2), they become the authoritative formal
> statement and this file is deleted with its content's blessing.

### 3.2 The state space

```tla
CONSTANTS
  Nodes,        \* model scope: 3
  Partitions,   \* (tenant, shard) pairs; model scope: 1-2
  Windows,      \* dense per-partition window ids; model scope: 2
  Requests,     \* client write requests; model scope: 4, >= 2 sharing a DKey (§3.1)
  RF,           \* replication factor; model scope: 2
  SoftLim, ThrottleLim, HardLim,   \* ladder thresholds on staged bytes
  WipeBudget,   \* permanent-loss fault budget; RF - 1 in the checked config
  Datasets,     \* declared datasets; model scope: 1-2
  LatticeElem,  \* the schema lattice's carrier set; model scope: a 3-chain + join
  WinOf         \* [Requests -> Windows]: arrival timing abstracted to a constant
                \* assignment of requests to windows

VARIABLES
  \* -- per-node hot state ------------------------------------------------
  inflight,     \* [Nodes -> SUBSET Requests]  volatile: accepted, unstaged
  staged,       \* [Nodes -> SUBSET Rec]       durable: fsynced rows (A1)
  dedup,        \* [Nodes -> SUBSET DedupEntry] durable: same txn as staged;
                \* an entry carries [key, acked, orig] - orig names the request
                \* that staged it, so a replay can copy the original's evidence
  cache,        \* [Nodes -> SUBSET WinTbl]    cache class: durable, expendable;
                \* a WinTbl CARRIES its row set - Rows(t) is a field read, so
                \* CacheTransparency is computable from the entry alone
  nextSeq,      \* [Nodes -> [Partitions -> Nat]]  per-origin sequence.
                \* Init: 1 everywhere - sequences are 1-based, so GapFreedom's
                \* prefix arithmetic and AppliedThru + 1 agree by construction
  \* -- replication -------------------------------------------------------
  chan,         \* SUBSET Msg      the network (A4): every message carries inc
  receipts,     \* SUBSET Receipt  durable-apply acknowledgements
  highestSeen,  \* [Nodes -> [Nodes -> Nat]]  fencing: the highest incarnation
                \* each receiver has seen per sender - the fence a node can
                \* actually hold (§5.7); the global inc[] is ground truth that
                \* only invariants and the catalog read
  \* -- client-visible ----------------------------------------------------
  resolved,     \* [Requests -> {"unsent","pending","acked","throttled","refused"}]
  ackEvidence,  \* [Requests -> SUBSET Nodes]  holders ledgered at ack instant
  recOf,        \* [Requests -> Rec \cup {None}]  history ledger (ground-truth-
                \* only): the record each request minted, written at StageCommit
                \* and copied at DedupCheck replay - KeyOf(q) == Key(recOf[q])
                \* and WindowOf(q) == recOf[q].window stay evaluable after
                \* DropWindow removes the record from every staged[n]
  \* -- cold tier and catalog ---------------------------------------------
  sealedParts,  \* SUBSET Part     sealed locally, awaiting PutPart
  objects,      \* SUBSET Part     S3: PUT-complete objects (A3)
  lake,         \* SUBSET Part     catalog-committed parts
  expired,      \* SUBSET Part     history ledger: parts retention has expired
                \* (Expire) - sanctioned removal, distinct from loss
  wm,           \* [Partitions -> Nat]  complete_through, per partition
  lossLedger,   \* SUBSET LossRow  permanent declared-loss rows; a row carries
                \* [part, range, liveAtDecl] - liveAtDecl is a history flag:
                \* TRUE iff a live un-wiped node held coverage of the range at
                \* declaration time (LossLedgerTruthful's yardstick)
  catalogSeq,   \* Nat             the incarnation mint (a catalog sequence)
  pendingCommit,\* [Nodes -> CommitAttempt \cup {None}]  Indeterminate awaiting
                \* read-back; an attempt records the part AND the inc it was
                \* made under
  \* -- membership (advisory) ---------------------------------------------
  claims, hb,   \* registry rows: coverage claims, heartbeats. Advisory only:
                \* no invariant may read them as authority (see §5)
  inc,          \* [Nodes -> Nat]  fencing incarnation (highest minted, per node)
  alive,        \* [Nodes -> BOOLEAN]
  degraded,     \* SUBSET Nodes    booted without the catalog (DegradedBoot):
                \* replicate-and-serve only, no ownership actions
  wiped,        \* SUBSET Nodes    permanently lost disks, |wiped| <= WipeBudget
  \* -- schema -------------------------------------------------------------
  schema,       \* [Nodes -> [Datasets -> LatticeElem]]  each node's applied
                \* logical schema - per-node, so fail-closed-on-unknown-columns
                \* is representable (a receiver can genuinely not know a column)
  staleApplied  \* SUBSET Effect   ledger of effects admitted bearing an inc
                \* lower than the highest the ACCEPTOR had seen for the sender
                \* (or, for commits, lower than the catalog's minted highest).
                \* Empty in every honest configuration; FencedZombie's yardstick

Rec == [req : Requests, part : Partitions, origin : Nodes, seq : Nat,
        window : Windows]                      \* window = WinOf(req), fixed at stage
Key(r)  == <<r.part, r.origin, r.seq>>
KeyOf(q)    == Key(recOf[q])                   \* via the history ledger
WindowOf(q) == recOf[q].window
Part    == [part : Partitions, window : Windows,
            kind : {"window","supplement","snapshot"},
            disc : Disc,       \* discriminator: "-" for kind "window" (fixed, so
                               \* at most one window part); the per-origin seq
                               \* range for a supplement; snapshot_as_of_seq for
                               \* a snapshot - the commit fence keys on it (§6)
            coverage : SUBSET (Partitions \X Nodes \X Nat),
            extent : SUBSET (Partitions \X Nodes \X Nat),
                               \* fixed at seal: the window's full receipted
                               \* per-origin extent as attested by receipts and
                               \* the sealer's own rows - NewWatermark's input
            sealer : Nodes, inc : Nat,   \* who sealed it, under which incarnation
            dedupRemoved : Nat]
```

Three shapes matter. First, `staged` is the staging class and `cache`
the cache class of §2 — distinct variables because their obligations are
opposite (staging is never evicted; cache always may be). Second, the
overload measure is a *definition*, not a variable:

```tla
M(n)     == Cardinality({r \in staged[n] : TRUE})    \* staged bytes; cache-blind
Rung(n)  == IF M(n) >= HardLim     THEN 3            \* refuse
            ELSE IF M(n) >= ThrottleLim THEN 2       \* throttle
            ELSE IF M(n) >= SoftLim THEN 1           \* disclose
            ELSE 0
```

`M` reads `staged` alone — the cache class is invisible to it by
construction, half of LadderMonotone before any action fires. Third,
`claims` and `hb` are advisory: routing reads them (§7), but no safety
invariant quantifies over them — the registry is reconstructible soft
state (§5, §9), and an invariant resting on it would verify a fiction.

### 3.3 The action set

Each action: formal core plus a short rationale. Guards elided as `...`
are spelled in the modules; nothing load-bearing is elided — the two
definitions the most rests on, `NewWatermark` and `IsCatchup`, are
spelled in full below, and the remaining elisions are simple selectors
(`HoldsClaim`, `CommittedDurably`, `Landed`) whose one-line meanings the
prose states where they appear.

#### Ingest: Accept → DedupCheck → StageCommit → ClientAck

```tla
Accept(n, q) ==
  /\ alive[n] /\ resolved[q] = "unsent" /\ Rung(n) < 2   \* no new accepts at
                                                          \* rung 2 or above
  /\ inflight'  = [inflight EXCEPT ![n] = @ \cup {q}]
  /\ resolved'  = [resolved EXCEPT ![q] = "pending"]
```
Admission into volatile memory only; nothing about `Accept` is a promise.
Any node accepts any request — the data path is coordinator-free (§5).

```tla
DKey(q) == <<TenantOf(q), Hash(q)>>   \* tenant is in the key: two tenants may
                                      \* legally send byte-identical bodies, and
                                      \* a collision is therefore always
                                      \* tenant-scoped - cross-tenant collisions
                                      \* do not exist in any configuration
AtRF(e) ==   \* the entry's staged original now has >= RF total-inclusive copies
  LET r == recOf[e.orig]
  IN  /\ r # None
      /\ Cardinality({r.origin} \cup {rc.holder :
           rc \in {rc \in receipts : rc.key = Key(r)}}) >= RF
DedupCheck(n, q) ==
  /\ alive[n] /\ q \in inflight[n]
  /\ \E e \in dedup[n] :
       /\ e.key = DKey(q)
       /\ IF e.acked \/ AtRF(e)
          THEN \* replay the original's success, WITH its evidence: the ledger
               \* and the record linkage are copied, never re-derived
               /\ resolved'    = [resolved EXCEPT ![q] = "acked"]
               /\ ackEvidence' = [ackEvidence EXCEPT ![q] =
                                    IF e.acked THEN ackEvidence[e.orig]
                                    ELSE AckSetOf(e)]  \* computed exactly as
                                                       \* ClientAck computes H
               /\ recOf'       = [recOf EXCEPT ![q] = recOf[e.orig]]
               /\ dedup'       = MarkAcked(dedup, n, DKey(q))
          ELSE /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
               /\ UNCHANGED <<ackEvidence, recOf, dedup>>   \* pre-RF dup:
                                                            \* retryable, never a wait
  /\ inflight' = [inflight EXCEPT ![n] = @ \ {q}]
```
A duplicate of a fully-acked entry returns the original outcome *and*
inherits its evidence — DurableAck and NoAckedLoss hold for replayed
acks by the same ledger entries as for the original. The `AtRF(e)`
disjunct is the stage-then-throttled cure (§4.4.1): an entry whose
request was staged and then resolved retryable is **replayable-as-acked**
the moment its receipts reach RF — the data is durable and will drain,
so replaying success is honest, and the entry is never poisoned. A
duplicate arriving before RF gets a retryable signal — the client in
that window is by definition a retrying client that already handles
retry signals (§4).

```tla
StageCommit(n, q) ==   \* ONE local DuckDB transaction, atomic + fsynced (A1)
  /\ alive[n] /\ q \in inflight[n] /\ ~\E e \in dedup[n] : e.key = DKey(q)
  /\ LET p == PartOf(q)
         r == [req |-> q, part |-> p, origin |-> n, seq |-> nextSeq[n][p],
               window |-> WinOf[q]]
     IN /\ staged'  = [staged  EXCEPT ![n] = @ \cup {r}]
        /\ recOf'   = [recOf   EXCEPT ![q] = r]     \* the history ledger
        /\ dedup'   = [dedup   EXCEPT ![n] = @ \cup {[key |-> DKey(q),
                                                      acked |-> FALSE,
                                                      orig |-> q]}]
        /\ nextSeq' = [nextSeq EXCEPT ![n][p] = @ + 1]
        /\ inflight'= [inflight EXCEPT ![n] = @ \ {q}]
```
The row, the dedup entry, and the sequence bump land in one atomic
fsynced transition — so recovery can never observe a row without its
dedup entry (the replay-safety crux of §4) and per-origin sequences are
gapless at birth (1-based: `nextSeq` initializes to 1).

```tla
ClientAck(n, q) ==
  /\ alive[n] /\ resolved[q] = "pending"
  /\ LET r == TheRec(n, q)
         H == {n} \cup {rc.holder : rc \in {rc \in receipts : rc.key = Key(r)}}
     IN /\ r \in staged[n]
        /\ Cardinality(H) >= RF          \* <- this IS pillar 1
        /\ resolved'    = [resolved EXCEPT ![q] = "acked"]
        /\ ackEvidence' = [ackEvidence EXCEPT ![q] = H]
        /\ dedup'       = MarkAcked(dedup, n, DKey(q))
```
`ClientAck` is enabled only when the origin durably holds the record
*and* holds receipts putting total durable copies at ≥ RF. Two honesty
notes, stated rather than elided. First, `H` counts receipt **history**:
a receipt from a since-wiped peer still counts, and soundness leans on
the fault budget (`WipeBudget ≤ RF − 1` keeps at least one counted
holder alive), not on `H` being a live-copy census. Second, the durable
`MarkAcked` write and the client-visible response are bundled into one
action as a stated modeling assumption (the A1 style): the crash between
them leaves the entry unacked and the client unanswered — and the
client's retry then lands on `DedupCheck`'s `AtRF` branch and replays
success, so the collapse hides no loss mode. The ack is the product's
founding promise; everything in §3.4 exists to keep this one action
honest.

#### Overload and resolution: Throttle, Refuse, ClientTimeout

```tla
ReceiptWaitExpired(n, q) ==   \* the RF receipt wait timed out after the ring
  TheRec(n, q) \in staged[n]  \* walk-down exhausted substitutes; timeouts are
                              \* nondeterministic enablement in an async model (A4)

Throttle(n, q) == /\ alive[n] /\ resolved[q] \in {"unsent","pending"}
                  /\ Rung(n) = 2 \/ ReceiptWaitExpired(n, q)
                  /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
Refuse(n, q)   == /\ alive[n] /\ Rung(n) = 3 /\ resolved[q] = "unsent"
                  /\ resolved' = [resolved EXCEPT ![q] = "refused"]
ClientTimeout(q) ==           \* the client's own deadline, not a node action:
  /\ resolved[q] = "pending"  \* resolves a request left hanging by a dead or
                              \* silent acceptor as retryable
  /\ ~\E n \in Nodes : alive[n] /\ q \in inflight[n]
  /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
```
Throttle and Refuse share one wire form — UNAVAILABLE + RetryInfo (§4) —
and differ in what produces them, never in client-visible semantics:
Throttle resolves a request the node *admitted* (rung 2, or a
replication-receipt timeout on a staged request — the data is durable
and will drain, so the retryable signal is honest by right, and the
retry replays success once receipts complete via `DedupCheck`'s `AtRF`
branch), while Refuse turns away work never admitted, at the hard rung.
Neither is "terminal" in any sense the wire could express. `ClientTimeout`
is the third resolver: a crash after Accept wipes `inflight`, and without
the client's own deadline the request would hang forever — fail-closed is
a liveness discipline (EveryRequestResolves), and the timeout is the
client's, journaled by the verifying load generator in §8, not by any
node. Rung 1 (disclose) restricts nothing and therefore has no action;
rung 0 (cache eviction) is `Evict`, always enabled and not a status. The
ladder never drops acked data — the only lever it has is admission, which
is the whole point of driving it on staged bytes: staged is the one
quantity that cannot be reclaimed without violating NoAckedLoss (§4, §9).

#### Replication: Forward → PeerApply → Receipt

```tla
Forward(n, m, r) ==
  /\ alive[n] /\ r \in staged[n] /\ r.origin = n /\ m \in RingPeers(r.part, n)
  /\ chan' = chan \cup {[to |-> m, rec |-> r, inc |-> inc[n],
                         sAt |-> schema[n]]}  \* schema changes ride in-band (§4)

IsCatchup(g) ==   \* some receipt already stands for this record ANYWHERE: it
                  \* backs a promise already in flight, which the hard rung
                  \* honors; a record no receipt has ever covered is new load
  \E rc \in receipts : rc.key = Key(g.rec)

PeerApply(m, g) ==
  /\ alive[m] /\ g \in chan /\ g.to = m
  /\ g.inc >= highestSeen[m][g.rec.origin]          \* fencing: highest-seen-per-
                                                    \* receiver (§5.7) - the fence
                                                    \* a receiver can actually hold
  /\ g.rec.seq = AppliedThru(m, g.rec.part, g.rec.origin) + 1
                                                    \* GAP REFUSAL: a peer never
                                                    \* applies a non-contiguous seq
  /\ SchemaKnown(m, g)                              \* fail closed on columns the
                                                    \* RECEIVER's schema[m] lacks
  /\ IsCatchup(g) \/ Rung(m) < 3                    \* at the hard rung, refuse NEW
                                                    \* ranges; receipted catch-up continues
  /\ staged'      = [staged EXCEPT ![m] = @ \cup {g.rec}]  \* one local txn (A1)
  /\ highestSeen' = [highestSeen EXCEPT ![m][g.rec.origin] =
                       Max(@, g.inc)]

Receipt(m, r) ==
  /\ alive[m] /\ r \in staged[m] /\ r.origin # m
  /\ receipts' = receipts \cup {[holder |-> m, key |-> Key(r), inc |-> inc[m]]}
```
Gap refusal makes per-(partition, origin) prefixes the unit of truth: a
peer's holdings are always contiguous, so "replicated through seq s" is
one number, coverage claims are cheap, and takeover needs no
reconciliation protocol (§5). The fence is deliberately the *receiver's*
knowledge, not the global `inc[]`: a peer may honestly apply a message
from an incarnation it has not yet seen superseded — that behavior is
real, explored, and harmless (the apply is idempotent, and commits are
fenced at the catalog, which mints incarnations and so does know the
highest). A receipt is issued only after the durable apply — it is a
claim about bytes on a peer's disk, never its memory.

#### Drain: SealPart → PutPart → LakeCommit (∘ WatermarkAdvance)

```tla
SealPart(n, p, w) ==
  /\ alive[n] /\ n \notin degraded /\ HoldsClaim(n, p) /\ WindowClosed(p, w)
  /\ LET cov == {Key(r) : r \in WindowRecs(n, p, w)}
         ext == cov \cup {k \in WindowKeys(p, w) :   \* receipted extent: every
                  \E rc \in receipts : rc.key = k}   \* key some receipt attests
     IN sealedParts' = sealedParts \cup
          {[part |-> p, window |-> w, kind |-> "window", disc |-> "-",
            coverage |-> cov, extent |-> ext,
            sealer |-> n, inc |-> inc[n],
            dedupRemoved |-> DrainDedupCount(n, p, w)]}
```
The seal fixes the part's exact (origin, seq) coverage, its
`dedupRemoved` count, **and the window's receipted extent** in the
manifest — the extent is what `NewWatermark` compares committed coverage
against, so a winner part that lacks a residue another replica holds can
never advance the watermark over that residue. Parts are tenant-pure,
retention-class-pure, and kind-pure by construction: `WindowRecs` draws
from exactly one class of each axis (§2, §6).

```tla
PutPart(n, pt) ==     \* atomic object appearance (A3); the object's only
                      \* LOGICAL put - byte-identical retries collapse into it
  /\ pt \in sealedParts /\ pt \notin objects
  /\ objects' = objects \cup {pt}
```

`LakeCommit` is the linchpin — one catalog transaction with the three-way
outcome of A2, and **WatermarkAdvance is inside it, atomically**. There
is no separate watermark action anywhere in the model or the product.
`NewWatermark` is the model's most load-bearing definition, so it is
spelled here, not elided — its coverage-completeness criterion is the
sealed extent:

```tla
NewWatermark(p, lk, ll) ==
  LET Committed(w) == UNION {x.coverage : x \in {x \in lk :
                        x.part = p /\ x.window = w}}
      Extent(w)    == UNION {x.extent : x \in {x \in lk :
                        x.part = p /\ x.window = w}}
      Done(w)      == /\ \E x \in lk : x.part = p /\ x.window = w
                      /\ \A k \in Extent(w) :
                           k \in Committed(w) \/ \E l \in ll : Covers(l, k)
  IN CHOOSE m \in 0..MaxWindow(p) :
       /\ \A w \in 1..m : Done(w)
       /\ (m = MaxWindow(p) \/ ~Done(m + 1))
```
The watermark advances exactly through the windows whose **committed
coverage equals their sealed receipted extent** (loss-ledgered ranges
excepted) — a winner commit with a supplement still pending does *not*
advance `wm` over its window; the supplement's own commit completes the
extent and advances it. The extent is fixed at seal from receipts and
the sealer's rows: a *definition* may consult the receipt ledger (it is
ground truth — §3.2's advisory ban covers `claims`/`hb`, and the
invariant-side rule bars guards' samples from being yardsticks, not
ledgers from being read).

```tla
LakeCommitOk(n, pt) ==
  /\ alive[n] /\ pt \in objects /\ pendingCommit[n] = None
  /\ pt.inc = inc[pt.sealer]          \* the catalog minted every incarnation and
                                      \* rejects a commit under a superseded one
  /\ ~\E x \in lake : /\ x.part = pt.part /\ x.window = pt.window
                      /\ x.kind = pt.kind /\ x.disc = pt.disc
                      \* UNIQUE(partition, window, kind, discriminator):
                      \* kind "window" has the fixed disc "-", so at most one;
                      \* supplements and snapshots key on their discriminator
  /\ pt.kind = "supplement" =>
       \A x \in lake : SameWindow(x, pt) => x.coverage \cap pt.coverage = {}
                                    \* supplements PROVE pairwise-disjoint
                                    \* (origin,seq) coverage against EVERY prior
                                    \* part, validated inside this same txn
  /\ lake' = lake \cup {pt}
  /\ wm'   = [wm EXCEPT ![pt.part] = NewWatermark(pt.part, lake', lossLedger)]
                                    \* WatermarkAdvance: same atomic commit

LakeCommitAbort(n, pt) ==           \* conflict or refusal: candidate dropped,
  /\ alive[n] /\ ...                \* window remains staged; drain retries.
  /\ UNCHANGED <<lake, wm>>         \* Never a loss - staging never left.

\* "Connection died mid-commit, outcome unknown" (A2) has TWO successors -
\* one where the transaction in fact landed, one where it did not. The node
\* cannot distinguish them; the model must represent both or Reconcile's
\* adopt branch is dead code:
LakeCommitIndeterminateLanded(n, pt) ==
  /\ alive[n] /\ pt \in objects /\ pendingCommit[n] = None
  /\ CommitGuardsHold(n, pt)          \* the same guards as LakeCommitOk
  /\ lake' = lake \cup {pt}           \* the txn DID commit - lake and wm
  /\ wm'   = [wm EXCEPT ![pt.part] = NewWatermark(pt.part, lake', lossLedger)]
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = Attempt(pt, inc[n])]
                                      \* ...but the node only knows "unknown"
LakeCommitIndeterminateLost(n, pt) ==
  /\ alive[n] /\ pt \in objects /\ pendingCommit[n] = None
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = Attempt(pt, inc[n])]
  /\ UNCHANGED <<lake, wm>>           \* the txn did NOT commit

Reconcile(n) ==                     \* EXACTLY ONE read-back before any retry
  /\ alive[n] /\ pendingCommit[n] # None
  /\ IF Landed(pendingCommit[n], lake)
     THEN RecordAsCommitted(n)      \* the write landed: adopt it
     ELSE ClearForRetry(n)          \* it did not: a fresh attempt may begin
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = None]
```
Committing the part and advancing the watermark in one transaction is
what makes WatermarkHonesty checkable: no interleaving exists in which
the lake holds the part but the watermark lies, or vice versa. The
UNIQUE guard is the entire double-drain defense — cheaper and stronger
than any leadership protocol, because it fences at the only place a
drain becomes real (§5, §6). The implementation cannot know which
Indeterminate successor it took; it journals one `LakeCommitIndeterminate`
event and the following `Reconcile` names the outcome (§3.7).

#### Retention: Expire

```tla
Expire(pt) ==            \* the object's second and last storage operation
  /\ pt \in lake /\ RetentionElapsed(pt)
  /\ (IsChangelogData(pt) /\ pt.kind # "snapshot") =>
       \E s \in lake : /\ s.kind = "snapshot" /\ s.part = pt.part
                       /\ CoversArrival(s, pt)   \* Keep Rule 10's guard: a
                                                 \* changelog part expires only
                                                 \* under a covering snapshot
  /\ lake'    = lake \ {pt}
  /\ objects' = objects \ {pt}
  /\ expired' = expired \cup {pt}    \* history ledger: sanctioned removal is
                                     \* recorded, never silent
```
Retention's whole-file DELETE is a destructive protocol step, so it is
an action, not an operational footnote: `SnapshotCovered` (§3.4) is Keep
Rule 10 as ground truth, and the `ExpireUncovered` broken variant (§3.6)
keeps the guard honest. `InLake` in the invariants reads
`lake ∪ expired` — an acked record retired by sanctioned retention was
in the lake when the watermark claimed it, and the ledger proves it. In
the checked scope snapshots themselves are keep-forever (a snapshot
expires only under a newer covering snapshot, which the small
configuration never seals).

#### Post-drain residency: Demote, Evict, DropWindow

```tla
Demote(n, p, w) ==                    \* staging -> cache, in place
  /\ alive[n]
  /\ CommittedDurably(p, w)           \* the lake commit + watermark txn is durable
  /\ DedupRemovedOf(p, w) = 0         \* ONLY then is the hot table row-identical
  /\ {Key(r) : r \in WindowRecs(n, p, w)} = CommittedCoverage(p, w)
                                      \* only the node whose rows ARE the
                                      \* committed part may demote (§2.4: "a
                                      \* table this node itself staged and
                                      \* drained"); a replica holding a partial
                                      \* receipted prefix must DropWindow instead
  /\ cache'  = [cache  EXCEPT ![n] = @ \cup {WinTblOf(n, p, w)}]
  /\ staged' = [staged EXCEPT ![n] = @ \ WindowRecs(n, p, w)]

DropWindow(n, p, w) ==                \* the default exit from staging
  /\ CommittedDurably(p, w)
  /\ staged' = [staged EXCEPT ![n] = @ \ WindowRecs(n, p, w)]

Evict(n, t) ==                        \* cache only; ALWAYS enabled
  /\ t \in cache[n]
  /\ cache' = [cache EXCEPT ![n] = @ \ {t}]
```
`Demote` fires only when the drain removed zero rows as duplicates *and*
this node's own window rows are exactly the committed coverage — then
and only then does substituting the hot table for the lake part preserve
`complete`-read answers unconditionally (the Demote-safety lemma's
discharge). `Evict` has no guard beyond membership: eviction is always
safe because it only shrinks the cache class the lemma quantifies over.

#### Changelog: SnapshotSeal

```tla
SnapshotSeal(n, p) ==
  /\ alive[n] /\ n \notin degraded
  /\ KindOf(p) = "changelog" /\ HoldsClaim(n, p)
  /\ LET s == LatestByKey(n, p)   \* full latest-state as-of arrival seq S;
     IN ...                       \* deleted keys absent; a NEW object -
                                  \* derivation, never rewrite (A3)
  \* fenced at commit by the same UNIQUE(partition, window, kind, disc) guard,
  \* with disc = snapshot_as_of_seq - its own key, not a vocabulary reuse of
  \* the window fence; serialized per partition under the drain scheduler (§6)
```
Snapshots make changelog retention append-only: parts wholly covered by
a snapshot become age-expirable whole files; uncovered parts are
keep-forever (§6, §11). The fence is defined on its own key — not a
vocabulary reuse of the window guard.

#### Membership and failure: ClaimAdvertise, Heartbeat, FenceBoot, DegradedBoot, TakeoverDrain, DeclareLoss

```tla
ClaimAdvertise(n, p) ==  \* advisory registry row: "I hold coverage for p"
  /\ ... /\ claims' = claims \cup {[node |-> n, part |-> p,
                                    thru |-> AppliedThru(n, p, n), inc |-> inc[n]]}
Heartbeat(n) == /\ alive[n] /\ hb' = [hb EXCEPT ![n] = @ + 1]

FenceBoot(n) ==          \* recovery entry point; incarnation from the catalog
  /\ ~alive[n] /\ n \notin wiped
  /\ catalogSeq' = catalogSeq + 1
  /\ inc'   = [inc EXCEPT ![n] = catalogSeq']
  /\ alive' = [alive EXCEPT ![n] = TRUE]
  /\ degraded' = degraded \ {n}
  \* recovery state = staged[n], replayed as-is: staging tables ARE the WAL.
  \* A wiped node never re-enters as itself: an empty disk re-provisions as a
  \* NEW node with a fresh identity; §9.4.2's rejoin-with-data path concerns a
  \* declared-dead node that was never wiped

DegradedBoot(n) ==       \* catalog down at boot, persisted incarnation (§5.7):
  /\ ~alive[n] /\ n \notin wiped /\ inc[n] > 0   \* has an identity to be
                                                 \* safely partial with
  /\ alive'    = [alive EXCEPT ![n] = TRUE]
  /\ degraded' = degraded \cup {n}
  /\ UNCHANGED <<catalogSeq, inc>>               \* no fresh incarnation minted
  \* replica-only: PeerApply, Receipt, Heartbeat, and serving run under the
  \* persisted incarnation; every ownership action (SealPart, SnapshotSeal,
  \* TakeoverDrain - and through them the commits) is guarded on
  \* n \notin degraded. FenceBoot, when the catalog returns, promotes n
```
Every message and registry row carries its sender's incarnation; every
receiver rejects one lower than the highest seen for that node (the
guards in `PeerApply`, `Receipt`, and the catalog's row versions). A
higher incarnation fences a lower one on every message — the converged
epoch-fencing pattern (Kafka's leader epochs), applied without a leader.
`DegradedBoot` is the deliberately narrow exception that keeps rolling
restarts from wedging on a catalog incident: a stale-incarnation node in
degraded mode can only apply and receipt — exactly the operations whose
idempotency and fencing make a stale participant harmless.

```tla
TakeoverDrain(m, p) ==
  /\ alive[m] /\ m \notin degraded
  /\ ~alive[Owner(p)] /\ HeartbeatStale(Owner(p)) /\ SuppressionExpired(Owner(p))
  /\ CoverageAt(m, p) >= MaxAdvertised(p)   \* elects on ADVERTISED (claims)
        \* coverage - what a real election can read. A stale registry may elect
        \* a less-covered replica; safety then rests on SingleDrainCommit and
        \* the extent-complete NewWatermark, never on election quality
  \* not a distinct state change: enabling TakeoverDrain enables SealPart(m, p,
  \* w) over m's OWN replicated rows - the effect is the ordinary drain pipeline
```
Takeover is just a drain performed by a replica — no special commit
path, no election protocol. If the presumed-dead owner was merely slow,
both drains race to `LakeCommitOk` and the UNIQUE guard lets exactly one
win; the loser aborts harmlessly. SingleDrainCommit, not failure
detection, carries the safety (§5).

```tla
DeclareLoss(p, rng) ==   \* OPERATOR action - never autonomous (§9's ceremony)
  /\ ~\E n \in Nodes \ wiped : Advertises(n, p, rng)  \* refused while any live
                                                       \* replica claims coverage
  /\ lossLedger' = lossLedger \cup {[part |-> p, range |-> rng,
        liveAtDecl |-> \E n \in Nodes \ wiped :        \* history flag: was the
             alive[n] /\ HoldsCoverage(n, p, rng)]}    \* confession false?
  /\ wm' = [wm EXCEPT ![p] = AdvancePast(rng)]
  \* ledger row and watermark advance: ONE catalog transaction (A2)
```
The watermark may advance past a hole *only* here, and the permanent
ledger row rides the same transaction — degraded availability disclosed,
never disguised (pillar 4). Within the RF − 1 budget this action is
unreachable for acked records; §3.6 witnesses both facts.

#### Schema: EvolveSchema

```tla
EvolveSchema(n, d, s) ==   \* a schema change IS a sequenced record: it consumes
                           \* a seq and rides the same log as data (§4, §5)
  /\ alive[n] /\ s = LatticeJoin(schema[n][d], s)   \* monotone: join, never a rewrite
  /\ schema'  = [schema  EXCEPT ![n][d] = s]
  /\ LET p == HomePartition(d)
     IN /\ staged'  = [staged  EXCEPT ![n] = @ \cup
                        {SchemaRec(n, d, s, nextSeq[n][p])}]
        /\ nextSeq' = [nextSeq EXCEPT ![n][p] = @ + 1]
  \* peers receive the schema record through Forward/PeerApply like any other
  \* record, in the total order gap refusal already provides; applying it joins
  \* s into schema[m]. SchemaKnown(m, g) compares g.sAt against the RECEIVER's
  \* schema[m] and fails closed on columns m has not yet learned - so
  \* widen-to-origin-schema always precedes the data that needs it
```
The join is commutative and idempotent, so crash-retry and concurrent
evolution converge without coordination; in-band ordering means a peer
never applies a record whose columns it has not learned —
`PeerApply`'s `SchemaKnown` guard fails closed (§4). Because `schema` is
per-node state, "a receiver that does not yet know a column" is a
representable configuration, not a comment — `Witness_SchemaWidensInFlight`
(§3.6) reaches it.

#### Crash and recovery: CrashNode, RecoverNode

```tla
CrashNode(n) ==          \* enabled at ANY interleaving point - no guard but life
  /\ alive[n]
  /\ alive'    = [alive EXCEPT ![n] = FALSE]
  /\ inflight' = [inflight EXCEPT ![n] = {}]   \* volatile state gone
  /\ UNCHANGED staged                          \* fsynced state survives (A1)

CrashWipe(n) ==          \* the disk dies too - bounded by the fault budget.
                         \* No liveness guard: a disk can die under a node
                         \* that already crashed
  /\ Cardinality(wiped \cup {n}) <= WipeBudget
  /\ wiped'  = wiped \cup {n}
  /\ staged' = [staged EXCEPT ![n] = {}] /\ cache' = [cache EXCEPT ![n] = {}]
  /\ ...

RecoverNode(n) == FenceBoot(n)   \* recovery replays from staging tables;
                                 \* there is no other recovery input
                                 \* (DegradedBoot is the catalog-down entry,
                                 \* not a second recovery input: it replays
                                 \* the same staging tables)
```
`CrashNode` interleaves everywhere — between a stage and its forward,
between a PUT and its commit, between a commit and its demotion. That
last window is why `Demote` re-checks catalog durability on recovery:
crash-between-commit-and-demotion is a checked behavior, not a comment.

### 3.4 Invariants

Checked over every reachable state of every module configuration — and,
for LadderMonotone's behavioral conjunct, over every transition. Each is
stated as a formula over ground truth, with its prose meaning. Ten
state invariants plus the ladder action property; the history ledgers (`recOf`,
`expired`, `liveAtDecl`, `staleApplied`) exist so each stays evaluable
after the state it judges has legitimately moved on.

**DurableAck** — an ack is a claim about RF durable copies.
```tla
DurableAck ==
  \A q \in Acked :
    /\ Cardinality(ackEvidence[q]) >= RF
    /\ \A m \in ackEvidence[q] :
         m = OriginOf(q) \/ \E rc \in receipts : rc.holder = m /\ rc.key = KeyOf(q)
```
Every acked request's ledgered evidence names at least RF nodes, each of
which either is the origin (which staged durably before acking) or had
issued a durable-apply receipt. This is pillar 1 as a formula.

**NoAckedLoss** — acked data survives any fault schedule within budget.
```tla
InLake(k) == \E x \in lake \cup expired : k \in x.coverage
             \* expired = retired by sanctioned retention, ledgered (Expire)
NoAckedLoss ==
  \A q \in Acked :
    InLake(KeyOf(q)) \/ \E n \in Nodes \ wiped :
                          \E r \in staged[n] : Key(r) = KeyOf(q)
```
Checked across every schedule of `CrashNode`/`CrashWipe`/`RecoverNode`
with at most RF − 1 permanent losses: every acked record remains
reachable — in a surviving node's staging or committed to the lake. The
quantifier runs over `KeyOf` — the history ledger — so it stays
evaluable for a replayed ack and for a record a `DropWindow` has since
removed from every staging table. Together with `DeclareLoss`'s
live-replica guard, this also proves the loss ceremony cannot touch an
acked record inside the budget.

**WatermarkHonesty** — `complete_through` never lies.
```tla
WatermarkHonesty ==
  \A p \in Partitions : \A q \in Acked :
    (PartOf(q) = p /\ WindowOf(q) <= wm[p]) =>
      InLake(KeyOf(q)) \/ \E l \in lossLedger : Covers(l, KeyOf(q))
```
If the watermark says a cell is complete, every acked record for that
cell is in the lake **or declared lost in the ledger — never silently
missing**. This is the formula a `complete` read (§7) rests on: the
fail-closed default is only as good as this invariant.

**CacheTransparency** — the Demote-safety lemma.
```tla
CacheTransparency ==
  \A n \in Nodes : \A t \in cache[n] : Rows(t) = LakeRowsOf(t)
```
Every cache-class table is row-identical to its committed part —
discharged by `Demote`'s `dedupRemoved = 0` and coverage-identity
guards (a `WinTbl` carries its row set, so `Rows(t)` is a field read).
This formula is deliberately a **lemma**, not §2.4's full theorem: the
theorem quantifies over every `complete` read's *answer*, and §3 has no
read action. The theorem is discharged in three parts — this row-identity
lemma, the one-side-serving tier rule (§7.2, under which v1's read path
never consults the cache class at all), and §8.4's eviction-storm judge,
which checks the read-answer equivalence mechanically. `Evict` only
removes tables from this lemma's quantifier domain and can never violate
row-identity; eviction interleavings stress the read-path equivalence,
which is §8.4's job, not this formula's.

**GapFreedom** — per-(partition, origin) *holdings* tile a contiguous prefix.
```tla
DrainedSeqs(p, o) == {s \in Nat : \E x \in lake \cup expired :
                        <<p, o, s>> \in x.coverage}
GapFreedom ==   \* quantified over HOLDERS: windows commit in any order, so a
                \* node holding nothing for (p, o) can see a non-prefix D --
                \* a legal state the original formula miscounted (TN-31)
  \A n \in Nodes, p \in Partitions, o \in Nodes :
    LET S == {r.seq : r \in {r \in staged[n] : r.part = p /\ r.origin = o}}
        D == DrainedSeqs(p, o)
    IN  S # {} => S \cup D = 1..Cardinality(S \cup D)
```
The direct consequence of `PeerApply`'s gap refusal plus `StageCommit`'s
atomic 1-based sequence assignment (`nextSeq` initializes to 1, pinned
in Init) — but only for a node that actually *holds* something for
`(p, o)`. TLC found the unguarded form false: windows commit to the lake
in any order, so a node with an empty `S` for `(p, o)` can observe a `D`
that is not itself a prefix (some later window drained before an earlier
one), which is a legal state, not a violation — the invariant has nothing
to say about non-holders. The `S # {}` guard restricts the claim to nodes
that hold at least one record for the pair, which is what `PeerApply`'s
gap refusal and `StageCommit`'s sequencing actually constrain. The union
with drained coverage is still what makes the invariant survive the
drain: after `DropWindow` removes window 1's records, the staged residue
alone is no prefix — staged ∪ committed still is, and that is exactly the
property §7.5's hot∪cold tiling rests on. Everything cheap about
DuckSpout's replication — one-number coverage, reconciliation-free
takeover, supplement-disjointness proofs — depends on this.

**SingleDrainCommit** — at most one committed part per fence key; supplements disjoint.
```tla
SingleDrainCommit ==
  /\ \A a, b \in lake :
       (a.part = b.part /\ a.window = b.window /\ a.kind = b.kind
        /\ a.disc = b.disc) => a = b
  /\ \A a \in lake : a.kind = "window" =>
       \A b \in lake : (SameWindow(a, b) /\ b.kind = "window") => a = b
  /\ \A s \in lake : s.kind = "supplement" =>
       \A x \in lake : (SameWindow(x, s) /\ x # s) => x.coverage \cap s.coverage = {}
```
The UNIQUE constraint over (partition, window_id, part_kind,
discriminator) — kind `window` carries the fixed discriminator, so at
most one window part exists; supplements may be several (a second
takeover residue, a post-DeclareLoss resurrection — §9.4.2) provided
each proves pairwise-disjoint coverage against every prior part of the
window inside its own commit. This is what makes a zombie's or a racing
replica's second drain an abort instead of a double-count.

**FencedZombie** — no effect lands under a fence its acceptor held.
```tla
FencedZombie == staleApplied = {}
```
`staleApplied` is a ledger populated by any acceptance path that admits
a message bearing an incarnation lower than the highest that acceptor
had *seen* for the sender — or, for commits, lower than the catalog's
minted highest (the catalog, as the mint, knows it). In the honest
configuration every acceptance guard checks exactly that fence, so the
ledger stays empty by construction; the `UnfencedZombie` variant removes
one guard and the ledger fills. An apply from an incarnation the
receiver has not yet seen superseded is *not* stale — it is a real,
explored, harmless behavior (§3.3, PeerApply).

**LossLedgerTruthful** — a confession is never false.
```tla
LossLedgerTruthful == \A l \in lossLedger : ~l.liveAtDecl
```
No loss row was ever declared while a live, un-wiped node held coverage
of its range — `DeclareLoss`'s no-live-coverage guard as ground truth,
via the `liveAtDecl` history flag. Without this invariant, dropping that
guard would violate nothing: the watermark still tells the ledgered
truth (WatermarkHonesty) and the live replica still holds its rows
(NoAckedLoss) — the harm of a false confession is exactly that a
`complete` read skips live data, and this is its formula.

**SnapshotCovered** — Keep Rule 10 as ground truth.
```tla
SnapshotCovered ==
  \A e \in expired : (IsChangelogData(e) /\ e.kind # "snapshot") =>
    \E s \in lake : s.kind = "snapshot" /\ s.part = e.part /\ CoversArrival(s, e)
```
Nothing retention expired ever held the last value of a key: every
expired changelog part has a committed covering snapshot. `Expire`'s
guard is the same formula in guard position; `ExpireUncovered` (§3.6)
perturbs the guard and this yardstick catches it.

**LatestViewCorrect** — the served latest view is the fold of the acked changelog.
```tla
LatestViewCorrect ==
  \A p \in ChangelogPartitions(Partitions) :
    LatestFold(SnapshotRows(p) (+) ChangelogSince(p))
      = LatestFold(AllCommittedAndStaged(p))
```
For every key, newest-snapshot-plus-changelog-since — the read plan of
`<dataset>_latest` (§7.7) — equals the fold of every committed and
staged record for the partition in (origin, seq) order, tombstones
deleting. `SnapshotSeal`'s elided `LatestByKey` content is exactly what
this invariant pins: a snapshot that dropped or resurrected a key
violates it. §8.4's changelog judge is this invariant, judged
end-to-end.

**LadderMonotone** — restriction only tightens as M rises; the cache is invisible.
```tla
Allowed(k) ==   \* the client-visible operations permitted at rung k
  CASE k = 0 -> {"accept", "ack", "replicate-new", "catch-up"}
    [] k = 1 -> {"accept", "ack", "replicate-new", "catch-up"}   \* disclose only
    [] k = 2 -> {"ack", "replicate-new", "catch-up"}   \* throttle: no new accepts
    [] k = 3 -> {"ack", "catch-up"}                    \* refuse + no new ranges

LadderMonotone ==   \* an ACTION property: every step taken is permitted at the
                    \* rung (pre-state) of the node that took it
  /\ \A j, k \in 0..3 : j <= k => Allowed(k) \subseteq Allowed(j)
  /\ [][ \A n \in Nodes, q \in Requests :
           /\ Accept(n, q)    => "accept" \in Allowed(Rung(n))
           /\ ClientAck(n, q) => "ack"    \in Allowed(Rung(n))
         /\ \A m \in Nodes : \A g \in chan :
              PeerApply(m, g) =>
                (IF IsCatchup(g) THEN "catch-up" ELSE "replicate-new")
                   \in Allowed(Rung(m)) ]_vars
```
The first conjunct is the static sanity of the table (antitone in the
rung); the second is the behavioral claim, and it is deliberately an
action property — a state predicate here would either restate `Rung`'s
own definition (a tautology no perturbation could falsify) or quantify
over nothing an action reads. Perturb `Accept`'s guard to admit at rung
≥ 2 and this property, not a definition, produces the counterexample.
`Rung` itself is a pure function of `M`, which reads staging alone —
`Evict` cannot change it, and `Demote` changes it only downward by
shrinking `staged`, which is the intended relief direction. In-flight
acks complete at every rung: the ladder gates admission, never promises
already made.

