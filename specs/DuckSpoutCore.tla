--------------------------- MODULE DuckSpoutCore ---------------------------
(***************************************************************************)
(* DuckSpout formal core: shared CONSTANTS/VARIABLES, the full 3.3 action  *)
(* set, and the 3.4 invariants, transcribed from specs/formal-core.md      *)
(* (the verbatim interim home of DUCKSPOUT.md 3.2-3.4).  Every checked     *)
(* module (Ingest, Drain, Schema; Replication at v0.2) EXTENDS this module *)
(* and instantiates the full shared Next over a projected state space and  *)
(* constant set (specs/README.md, "One model family, several modules").    *)
(*                                                                         *)
(* Transcription judgment calls -- every place this file resolves an       *)
(* ambiguity or elision of formal-core.md -- are documented, one by one,   *)
(* in specs/TRANSCRIPTION-NOTES.md (referenced below as TN-<n>).           *)
(*                                                                         *)
(* Broken variants (3.6) are CONFIGURATIONS ("Broken variant (armed       *)
(* .cfg)", README): each Brk* constant below arms exactly one guard-clause *)
(* perturbation and is FALSE in every clean configuration.  The invariant  *)
(* yardsticks never read the Brk* flags.                                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
  Nodes,        \* model scope: 3 (2 where the module's hazards need fewer)
  Partitions,   \* (tenant, shard) pairs; model scope: 1-2
  Windows,      \* dense per-partition window ids; a 1..W integer interval
  Requests,     \* client write requests; model scope: 4, >= 2 sharing a DKey
  RF,           \* replication factor; model scope: 2
  SoftLim, ThrottleLim, HardLim,   \* ladder thresholds on staged rows (M)
  WipeBudget,   \* permanent-loss fault budget; 0 in v0.1 clean configs (TN-2)
  Datasets,     \* declared datasets; model scope: 1-2
  Columns,      \* schema lattice carrier: LatticeElem == SUBSET Columns (TN-6)
  WinOf,        \* [Requests -> Windows]: arrival timing as a constant
  PartOf,       \* [Requests -> Partitions]: partition routing as a constant
  DKeyOf,       \* [Requests -> model values]: TenantOf/Hash collapsed (TN-5)
  CKeyOf,       \* [Requests -> model values]: changelog client key
  TombOf,       \* [Requests -> BOOLEAN]: changelog tombstone flag
  KindOf,       \* [Partitions -> {"event","changelog"}]
  HomePartition,\* [Datasets -> Partitions]
  DsOf,         \* [Requests -> Datasets]: the dataset a write belongs to
  AcceptorOf,   \* [Requests -> SUBSET Nodes]: arrival routing pin (TN-8)
  MaxCrashes,   \* bound on CrashNode steps (TN-3)
  Crashable,    \* SUBSET Nodes: fault-schedule scope for CrashNode (TN-3)
  IndetOn,      \* SUBSET Nodes: fault-schedule scope for the Indeterminate
                \* commit outcomes (TN-3: which node's catalog connection
                \* can die mid-commit in this configuration)
  InitClaims,   \* initial advisory claim rows (TN-4)
  MaxHb,        \* bound on Heartbeat (0 in v0.1: advisory, nothing reads it)
  DrainOn,      \* module projection toggle: drain pipeline on/off (TN-7)
  TakeoverOn,   \* module projection toggle: takeover dynamics on/off (v0.2,
                \* #55); FALSE in every v0.1 config -- TakeoverDrain contributes
                \* no reachable transition there, so pinned state counts are
                \* unaffected by its addition
  ReclaimOn,    \* module projection toggle: a recovering node's own-orphaned-
                \* seal reclaim on/off (v0.2, #177 -- ReclaimSeal below); FALSE
                \* in every v0.1 config AND in
                \* specs/broken/Finding_TakeoverOrphanedSeal.cfg (which must
                \* keep demonstrating the hazard this closes) -- ReclaimSeal
                \* contributes no reachable transition where it's FALSE, so
                \* those pinned state counts are unaffected by its addition
  None,         \* model value
  \* -- broken-variant switches (3.6); all FALSE in clean configs ----------
  BrkAckBeforeReceipt,     \* ClientAck drops the >= RF receipt conjunct
  BrkDrainWithoutWatermark,\* LakeCommitOk no longer advances wm; free advance
  BrkEvictStaging,         \* Evict enabled on staging-class tables
  BrkWatermarkPastHole,    \* NewWatermark may pass an uncovered range
  BrkDemoteDirty,          \* Demote drops dedupRemoved = 0
  BrkDoubleDrain,          \* LakeCommitOk drops the UNIQUE conjunct
  BrkSupplementOverlap,    \* supplement path skips the disjointness proof
  BrkExpireUncovered,      \* Expire drops the covering-snapshot conjunct
  BrkLadderInversion       \* Accept re-permits admission at rung >= 2

ASSUME Windows = 1..Cardinality(Windows)
ASSUME RF >= 1 /\ MaxCrashes >= 0 /\ WipeBudget >= 0

VARIABLES
  \* -- per-node hot state --------------------------------------------------
  inflight,     \* [Nodes -> SUBSET Requests]  volatile: accepted, unstaged
  staged,       \* [Nodes -> SUBSET Rec]       durable: fsynced rows (A1)
  dedup,        \* [Nodes -> SUBSET DedupEntry] durable: same txn as staged
  cache,        \* [Nodes -> SUBSET WinTbl]    cache class: durable, expendable
  nextSeq,      \* [Nodes -> [Partitions -> Nat]]  per-origin sequence, 1-based
  \* -- replication ---------------------------------------------------------
  chan,         \* SUBSET Msg      the network (A4): loss = never taken
  receipts,     \* SUBSET Receipt  durable-apply acknowledgements
  highestSeen,  \* [Nodes -> [Nodes -> Nat]]  receiver-held fence (5.7)
  \* -- client-visible ------------------------------------------------------
  resolved,     \* [Requests -> {"unsent","pending","acked","throttled","refused"}]
  ackEvidence,  \* [Requests -> SUBSET Nodes]  holders ledgered at ack instant
  recOf,        \* [Requests -> Rec \cup {None}]  history ledger (ground truth)
  \* -- cold tier and catalog -----------------------------------------------
  sealedParts,  \* SUBSET Part     sealed locally, awaiting PutPart
  objects,      \* SUBSET Part     S3: PUT-complete objects (A3)
  lake,         \* SUBSET Part     catalog-committed parts
  expired,      \* SUBSET Part     history ledger: retention-expired parts
  wm,           \* [Partitions -> Nat]  complete_through, per partition
  lossLedger,   \* SUBSET LossRow  [part, range, liveAtDecl]
  catalogSeq,   \* Nat             the incarnation mint (a catalog sequence)
  pendingCommit,\* [Nodes -> CommitAttempt \cup {None}]
  \* -- membership (advisory) -----------------------------------------------
  claims, hb,   \* registry rows / heartbeats; advisory only (TN-4)
  inc,          \* [Nodes -> Nat]  fencing incarnation (highest minted)
  alive,        \* [Nodes -> BOOLEAN]
  degraded,     \* SUBSET Nodes    booted without the catalog
  wiped,        \* SUBSET Nodes    permanently lost disks
  \* -- schema ---------------------------------------------------------------
  schema,       \* [Nodes -> [Datasets -> LatticeElem]]
  staleApplied, \* SUBSET Effect   FencedZombie's yardstick; {} when honest
  \* -- environment (TN-1: WindowClosed made concrete) -----------------------
  closed,       \* SUBSET (Partitions \X Windows)  windows closed to admission
  crashBudget   \* Nat  remaining CrashNode steps (TN-3)

vars == <<inflight, staged, dedup, cache, nextSeq, chan, receipts,
          highestSeen, resolved, ackEvidence, recOf, sealedParts, objects,
          lake, expired, wm, lossLedger, catalogSeq, pendingCommit, claims,
          hb, inc, alive, degraded, wiped, schema, staleApplied, closed,
          crashBudget>>

\* Frame helpers: variables grouped for UNCHANGED discipline.
ingVars == <<inflight, staged, dedup, cache, nextSeq, resolved, ackEvidence, recOf>>
repVars == <<chan, receipts, highestSeen>>
drnVars == <<sealedParts, objects, lake, expired, wm, lossLedger,
             pendingCommit, claims, closed>>
memVars == <<catalogSeq, hb, inc, alive, degraded, wiped, crashBudget>>
schVars == <<schema, staleApplied>>

-----------------------------------------------------------------------------
(* 3.2 -- shapes and definitions *)

LatticeElem == SUBSET Columns
LatticeJoin(a, b) == a \cup b

Key(r) == <<r.part, r.origin, r.seq>>
IsSchemaRec(r) == r.req = None        \* uniform Rec shape; TN-9
KeyOf(q)    == Key(recOf[q])          \* via the history ledger
WindowOf(q) == recOf[q].window
OriginOf(q) == recOf[q].origin
Acked == {q \in Requests : resolved[q] = "acked"}
MaxWin == Cardinality(Windows)

\* Overload measure: a definition, not a variable.  M reads staged alone --
\* the cache class is invisible to it by construction (3.2).
M(n)    == Cardinality(staged[n])
Rung(n) == IF M(n) >= HardLim     THEN 3
           ELSE IF M(n) >= ThrottleLim THEN 2
           ELSE IF M(n) >= SoftLim THEN 1
           ELSE 0

DKey(q) == DKeyOf[q]   \* tenant-scoped by construction (TN-5)
DKR(r)  == DKeyOf[r.req]

Max2(a, b) == IF a >= b THEN a ELSE b

\* Drained coverage, per (partition, origin): every seq some committed or
\* sanctioned-expired part covers (3.4 DrainedSeqs, verbatim over the
\* lake \cup expired ledger).
DrainedSeqs(p, o) ==   \* window-plane parts only: a snapshot is a derivation
                       \* (6), not a drain of the log -- its latest-per-key
                       \* coverage is non-contiguous by design (TN-30)
  LET AC == UNION {x.coverage : x \in {x2 \in lake \cup expired :
                                         x2.kind # "snapshot"}}
  IN  {k[3] : k \in {k2 \in AC : k2[1] = p /\ k2[2] = o}}

\* Longest 1-based contiguous prefix of U (TN-10).
PrefixLen(U) ==
  CHOOSE t \in 0..Cardinality(U) :
    /\ (1..t) \subseteq U
    /\ ~((1..(t+1)) \subseteq U)

AppliedThru(m, p, o) ==
  PrefixLen({r.seq : r \in {r2 \in staged[m] : r2.part = p /\ r2.origin = o}}
              \cup DrainedSeqs(p, o))

-----------------------------------------------------------------------------
(* 3.3 -- Ingest: Accept -> DedupCheck -> StageCommit -> ClientAck *)

Accept(n, q) ==
  /\ alive[n] /\ resolved[q] = "unsent"
  /\ n \in AcceptorOf[q]                          \* arrival routing pin (TN-8)
  /\ BrkLadderInversion \/ Rung(n) < 2            \* no new accepts at rung >= 2
  /\ <<PartOf[q], WinOf[q]>> \notin closed        \* late arrival: window closed
                                                  \* to admission (TN-1)
  /\ inflight' = [inflight EXCEPT ![n] = @ \cup {q}]
  /\ resolved' = [resolved EXCEPT ![q] = "pending"]
  /\ UNCHANGED <<staged, dedup, cache, nextSeq, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

AtRF(e) ==   \* the entry's staged original now has >= RF total copies
  LET r == recOf[e.orig]
  IN  /\ r # None
      /\ Cardinality({r.origin} \cup {rc.holder :
           rc \in {rc2 \in receipts : rc2.key = Key(r)}}) >= RF

AckSetOf(e) ==   \* computed exactly as ClientAck computes H
  LET r == recOf[e.orig]
  IN  {r.origin} \cup {rc.holder : rc \in {rc2 \in receipts : rc2.key = Key(r)}}

MarkAcked(dd, n, k) ==
  [dd EXCEPT ![n] = {IF e.key = k THEN [e EXCEPT !.acked = TRUE] ELSE e : e \in @}]

DedupCheck(n, q) ==
  /\ alive[n] /\ q \in inflight[n]
  /\ \E e \in dedup[n] :
       /\ e.key = DKey(q)
       /\ IF e.acked \/ AtRF(e)
          THEN \* replay the original's success, WITH its evidence
               /\ resolved'    = [resolved EXCEPT ![q] = "acked"]
               /\ ackEvidence' = [ackEvidence EXCEPT ![q] =
                                    IF e.acked THEN ackEvidence[e.orig]
                                    ELSE AckSetOf(e)]
               /\ recOf'       = [recOf EXCEPT ![q] = recOf[e.orig]]
               /\ dedup'       = MarkAcked(dedup, n, DKey(q))
          ELSE /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
               /\ UNCHANGED <<ackEvidence, recOf, dedup>>  \* pre-RF dup:
                                                          \* retryable, never a wait
  /\ inflight' = [inflight EXCEPT ![n] = @ \ {q}]
  /\ UNCHANGED <<staged, cache, nextSeq>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

StageCommit(n, q) ==   \* ONE local DuckDB transaction, atomic + fsynced (A1)
  /\ alive[n] /\ q \in inflight[n] /\ ~\E e \in dedup[n] : e.key = DKey(q)
  /\ LET p == PartOf[q]
         r == [req |-> q, part |-> p, origin |-> n, seq |-> nextSeq[n][p],
               window |-> WinOf[q], dataset |-> DsOf[q],
               elem |-> schema[n][DsOf[q]]]   \* the schema the row was
                                              \* written under (TN-13)
     IN /\ staged'  = [staged  EXCEPT ![n] = @ \cup {r}]
        /\ recOf'   = [recOf   EXCEPT ![q] = r]     \* the history ledger
        /\ dedup'   = [dedup   EXCEPT ![n] = @ \cup {[key |-> DKey(q),
                                                      acked |-> FALSE,
                                                      orig |-> q]}]
        /\ nextSeq' = [nextSeq EXCEPT ![n][p] = @ + 1]
        /\ inflight'= [inflight EXCEPT ![n] = @ \ {q}]
  /\ UNCHANGED <<cache, resolved, ackEvidence>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

ClientAck(n, q) ==
  /\ alive[n] /\ resolved[q] = "pending"
  /\ \E r \in staged[n] :
       /\ r.req = q /\ r.origin = n     \* the origin acks (TN-11)
       /\ LET H == {n} \cup {rc.holder :
                      rc \in {rc2 \in receipts : rc2.key = Key(r)}}
          IN /\ BrkAckBeforeReceipt \/ Cardinality(H) >= RF  \* <- pillar 1
             /\ resolved'    = [resolved EXCEPT ![q] = "acked"]
             /\ ackEvidence' = [ackEvidence EXCEPT ![q] = H]
             /\ dedup'       = MarkAcked(dedup, n, DKey(q))
  /\ UNCHANGED <<inflight, staged, cache, nextSeq, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Overload and resolution: Throttle, Refuse, ClientTimeout *)

ReceiptWaitExpired(n, q) ==   \* the RF receipt wait timed out (TN-11)
  \E r \in staged[n] : r.req = q /\ r.origin = n

Throttle(n, q) ==
  /\ alive[n] /\ resolved[q] \in {"unsent", "pending"}
  /\ Rung(n) = 2 \/ ReceiptWaitExpired(n, q)
  /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
  /\ UNCHANGED <<inflight, staged, dedup, cache, nextSeq, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

Refuse(n, q) ==
  /\ alive[n] /\ Rung(n) = 3 /\ resolved[q] = "unsent"
  /\ resolved' = [resolved EXCEPT ![q] = "refused"]
  /\ UNCHANGED <<inflight, staged, dedup, cache, nextSeq, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

ClientTimeout(q) ==           \* the client's own deadline, not a node action
  /\ resolved[q] = "pending"
  /\ ~\E n \in Nodes : alive[n] /\ q \in inflight[n]
  /\ resolved' = [resolved EXCEPT ![q] = "throttled"]
  /\ UNCHANGED <<inflight, staged, dedup, cache, nextSeq, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Replication: Forward -> PeerApply -> Receipt *)

RingPeers(p, n) == Nodes \ {n}   \* HRW placement abstracted: all peers (TN-12)

Forward(n, m, r) ==
  /\ alive[n] /\ r \in staged[n] /\ r.origin = n /\ m \in RingPeers(r.part, n)
  /\ chan' = chan \cup {[to |-> m, rec |-> r, inc |-> inc[n]]}
       \* schema rides in-band: the rec itself carries the lattice element
       \* it was written under (TN-13)
  /\ UNCHANGED <<receipts, highestSeen>>
  /\ UNCHANGED ingVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

IsCatchup(g) ==   \* some receipt already stands for this record ANYWHERE
  \E rc \in receipts : rc.key = Key(g.rec)

SchemaKnown(m, g) ==   \* fail closed on columns the RECEIVER lacks (TN-13)
  \/ IsSchemaRec(g.rec)           \* a schema record is self-describing
  \/ g.rec.elem \subseteq schema[m][g.rec.dataset]

PeerApply(m, g) ==
  /\ alive[m] /\ g \in chan /\ g.to = m
  /\ g.inc >= highestSeen[m][g.rec.origin]      \* fencing: highest-seen (5.7)
  /\ g.rec.seq = AppliedThru(m, g.rec.part, g.rec.origin) + 1   \* GAP REFUSAL
  /\ SchemaKnown(m, g)
  /\ IsCatchup(g) \/ Rung(m) < 3                \* hard rung: refuse NEW ranges
  /\ staged'      = [staged EXCEPT ![m] = @ \cup {g.rec}]  \* one local txn (A1)
  /\ highestSeen' = [highestSeen EXCEPT ![m][g.rec.origin] = Max2(@, g.inc)]
  /\ schema'      = IF IsSchemaRec(g.rec)       \* applying a schema record
                    THEN [schema EXCEPT         \* joins it (3.3 EvolveSchema)
                           ![m][g.rec.dataset] = LatticeJoin(@, g.rec.elem)]
                    ELSE schema
  /\ UNCHANGED <<chan, receipts>>
  /\ UNCHANGED <<inflight, dedup, cache, nextSeq, resolved, ackEvidence, recOf>>
  /\ UNCHANGED drnVars /\ UNCHANGED memVars /\ UNCHANGED staleApplied

Receipt(m, r) ==
  /\ alive[m] /\ r \in staged[m] /\ r.origin # m
  /\ receipts' = receipts \cup {[holder |-> m, key |-> Key(r), inc |-> inc[m]]}
  /\ UNCHANGED <<chan, highestSeen>>
  /\ UNCHANGED ingVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* Window close: the late-arrival hold, abstracted into WindowClosed (3.1,  *)
(* 6.3) and made concrete as one environment transition (TN-1).            *)

WindowClosed(p, w) == <<p, w>> \in closed

CloseWindow(p, w) ==
  /\ DrainOn /\ <<p, w>> \notin closed
  /\ \A q \in Requests :
       (PartOf[q] = p /\ WinOf[q] = w) =>
         /\ resolved[q] # "pending"
         /\ \A n \in Nodes : q \notin inflight[n]
  /\ closed' = closed \cup {<<p, w>>}
  /\ UNCHANGED <<sealedParts, objects, lake, expired, wm, lossLedger,
                 pendingCommit, claims>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Drain: SealPart -> PutPart -> LakeCommit (o WatermarkAdvance) *)

HoldsClaim(n, p) == <<n, p>> \in claims

WindowRecs(n, p, w) == {r \in staged[n] : r.part = p /\ r.window = w}

WindowKeys(p, w) ==   \* the window's key universe, via the history ledger
  {Key(recOf[q]) : q \in {q2 \in Requests :
     recOf[q2] # None /\ recOf[q2].part = p /\ recOf[q2].window = w}}

\* Drain-time dedup: deterministic one-kept-row-per-DKey (drain.md section 2;
\* the canonical representative stands in for smallest-(origin, seq); TN-14).
CanonRep(k, S) == CHOOSE r \in {r2 \in S : DKR(r2) = k} : TRUE
CanonKept(S)   == {r \in S : r = CanonRep(DKR(r), S)}
DrainDedupCount(n, p, w) ==
  LET RS == WindowRecs(n, p, w)
  IN  Cardinality(RS) - Cardinality(CanonKept(RS))

ReceiptedExtent(p, w) ==
  {k \in WindowKeys(p, w) : \E rc \in receipts : rc.key = k}

SealPart(n, p, w) ==
  /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ HoldsClaim(n, p) /\ WindowClosed(p, w)
  /\ ~\E x \in sealedParts \cup objects :  \* deterministic naming (6.5): a
       /\ x.sealer = n                     \* sealer's re-drain of a window
       /\ x.part = p /\ x.window = w       \* produces the same part, so one
       /\ x.kind = "window"                \* candidate per sealer (TN-29)
  /\ LET cov == {Key(r) : r \in WindowRecs(n, p, w)}
         ext == cov \cup ReceiptedExtent(p, w)  \* receipted extent: every key
                                                \* some receipt attests
         pt  == [part |-> p, window |-> w, kind |-> "window", disc |-> {},
                 coverage |-> cov, extent |-> ext,
                 sealer |-> n, inc |-> inc[n],
                 dedupRemoved |-> DrainDedupCount(n, p, w)]
     IN sealedParts' = sealedParts \cup {pt}    \* disc "-" encoded {} (TN-15)
  /\ UNCHANGED <<objects, lake, expired, wm, lossLedger, pendingCommit,
                 claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

PartsOf(p, w) == {x \in lake \cup expired : x.part = p /\ x.window = w}
CommittedCov(p, w) == UNION {x.coverage : x \in PartsOf(p, w)}
WindowPartCov(p, w) ==   \* the (at most one) kind-"window" part's coverage
  UNION {x.coverage : x \in {x2 \in PartsOf(p, w) : x2.kind = "window"}}

\* The supplement path (6.6, replication.md section 6): a residue holder
\* seals a supplement part for an already-committed window.  formal-core.md
\* elides the supplement seal; this is its minimal rendering (TN-16).
SealSupplement(n, p, w) ==
  /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ HoldsClaim(n, p) /\ WindowClosed(p, w)
  /\ \E x \in lake : x.part = p /\ x.window = w /\ x.kind = "window"
  /\ ~\E x \in sealedParts \cup objects :  \* one candidate per sealer (TN-29)
       x.sealer = n /\ x.part = p /\ x.window = w /\ x.kind = "supplement"
  /\ LET resid == {r \in WindowRecs(n, p, w) :
                     Key(r) \notin CommittedCov(p, w)}
     IN /\ resid # {}
        /\ LET cov == {Key(r) : r \in resid}
               pt  == [part |-> p, window |-> w, kind |-> "supplement",
                       disc |-> cov,   \* per-origin seq range as its key set
                       coverage |-> cov,
                       extent |-> cov \cup ReceiptedExtent(p, w),
                       sealer |-> n, inc |-> inc[n],
                       dedupRemoved |->
                         Cardinality(resid) - Cardinality(CanonKept(resid))]
           IN sealedParts' = sealedParts \cup {pt}
  /\ UNCHANGED <<objects, lake, expired, wm, lossLedger, pendingCommit,
                 claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

PutPart(pt) ==        \* atomic object appearance (A3); the object's only
                      \* LOGICAL put -- byte-identical retries collapse into it
  /\ DrainOn /\ pt \in sealedParts /\ pt \notin objects
  /\ objects' = objects \cup {pt}
  /\ UNCHANGED <<sealedParts, lake, expired, wm, lossLedger, pendingCommit,
                 claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* Recovery reclaim (v0.2, #177): a recovering node's own orphaned seal --   *)
(* sealed and PUT before it crashed, never committed -- is stuck forever     *)
(* under CommitGuardsHold's inc fence (pt.inc = inc[pt.sealer]) once         *)
(* FenceBoot bumps its incarnation, and SealPart's own-sealer uniqueness     *)
(* (TN-29) means it can never seal a FRESH replacement for the same window   *)
(* either -- the object's name is deterministic, so a second PUT would just  *)
(* be the same object again.  What is actually stale is the incarnation the  *)
(* catalog will check the object against, not the object itself: the        *)
(* sealer re-registers its OWN orphaned entry (in sealedParts and/or         *)
(* objects) under its CURRENT incarnation before attempting to commit it.    *)
(* part/window/kind/disc/coverage/extent are untouched, so UniqueOk and      *)
(* DisjointOk -- and therefore SingleDrainCommit -- see the identical        *)
(* candidate they always would have; only pt.inc, the freshness stamp,       *)
(* changes.  A live rival (another node that took the claim over and         *)
(* sealed/committed its own replacement) is still caught: its part shares    *)
(* the same (part, window, kind, disc) key, so UniqueOk rejects the          *)
(* reclaimed candidate exactly as it would reject any other second seal --   *)
(* the fence this loosens is purely the sealer-vs-itself one, never the      *)
(* cross-sealer one SingleDrainCommit polices.                              *)
ReclaimSeal(n, pt) ==
  /\ ReclaimOn /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ pt.sealer = n /\ HoldsClaim(n, pt.part)
  /\ pt \in objects /\ pt.inc # inc[n]        \* genuinely orphaned: stale inc
  /\ LET pt2 == [pt EXCEPT !.inc = inc[n]]
     IN /\ objects'     = (objects \ {pt}) \cup {pt2}
        /\ sealedParts' = IF pt \in sealedParts
                          THEN (sealedParts \ {pt}) \cup {pt2}
                          ELSE sealedParts
  /\ UNCHANGED <<lake, expired, wm, lossLedger, pendingCommit, claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

Covers(l, k) == k \in l.range

\* The model's most load-bearing definition (3.3): the watermark advances
\* exactly through the windows whose committed coverage equals their sealed
\* receipted extent (loss-ledgered ranges excepted).
NewWatermark(p, lk, ll) ==
  LET PW(w)        == {x \in lk : x.part = p /\ x.window = w}
      Committed(w) == UNION {x.coverage : x \in PW(w)}
      Extent(w)    == UNION {x.extent : x \in PW(w)}
      Done(w)      == /\ \E x \in PW(w) : TRUE
                      /\ \/ BrkWatermarkPastHole    \* 3.6: may pass a hole
                         \/ \A k \in Extent(w) :
                              k \in Committed(w) \/ \E l \in ll : Covers(l, k)
  IN CHOOSE m \in 0..MaxWin :
       /\ \A w \in 1..m : Done(w)
       /\ (m = MaxWin \/ ~Done(m + 1))

SameWindow(x, pt) == x.part = pt.part /\ x.window = pt.window

UniqueOk(pt) ==   \* UNIQUE(partition, window, kind, discriminator).  The
                  \* fence row lives in the registration table and OUTLIVES
                  \* the file: Expire is "metadata-only from the table's
                  \* perspective" (drain.md 7), so the fence spans expired
                  \* parts -- "at most one window part per window, EVER"
                  \* (TN-36)
  ~\E x \in lake \cup expired :
     /\ x.part = pt.part /\ x.window = pt.window
     /\ x.kind = pt.kind /\ x.disc = pt.disc

DisjointOk(pt) ==   \* supplements PROVE pairwise-disjoint coverage --
  pt.kind = "supplement" =>          \* against expired parts too (TN-36)
    \A x \in lake \cup expired :
      SameWindow(x, pt) => x.coverage \cap pt.coverage = {}

CommitGuardsHold(n, pt) ==
  /\ pt.inc = inc[pt.sealer]          \* the catalog minted every incarnation
  /\ BrkDoubleDrain \/ UniqueOk(pt)
  /\ BrkSupplementOverlap \/ DisjointOk(pt)

CommitWm(pt) ==   \* WatermarkAdvance: same atomic commit
  IF BrkDrainWithoutWatermark
  THEN wm         \* 3.6: the commit no longer advances wm
  ELSE [wm EXCEPT ![pt.part] = NewWatermark(pt.part, lake \cup {pt}, lossLedger)]

LakeCommitOk(n, pt) ==
  /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ pt.sealer = n   \* the drain choreography is sealer-driven (6): the
                     \* drainer registers its own part (TN-29)
  /\ pt \in objects /\ pendingCommit[n] = None
  /\ CommitGuardsHold(n, pt)
  /\ lake' = lake \cup {pt}
  /\ wm'   = CommitWm(pt)
  /\ UNCHANGED <<sealedParts, objects, expired, lossLedger, pendingCommit,
                 claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

LakeCommitAbort(n, pt) ==     \* conflict or refusal: candidate dropped,
  /\ DrainOn /\ alive[n]      \* window remains staged; drain retries.
  /\ pt.sealer = n
  /\ pt \in sealedParts /\ pt \in objects /\ pendingCommit[n] = None
  /\ ~CommitGuardsHold(n, pt)   \* conflict-driven: a transient catalog
       \* refusal only returns the candidate to the same commit-eligible
       \* state, so modeling it as a distinct journey adds schedules with
       \* no distinct outcome (TN-37)
  /\ sealedParts' = sealedParts \ {pt}
  /\ UNCHANGED <<objects, lake, expired, wm, lossLedger, pendingCommit,
                 claims, closed>>    \* Never a loss -- staging never left.
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

Attempt(pt, i) == [pt |-> pt, inc |-> i]

\* "Connection died mid-commit, outcome unknown" (A2) has TWO successors.
LakeCommitIndeterminateLanded(n, pt) ==
  /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ pt.sealer = n /\ n \in IndetOn
  /\ pt \in objects /\ pendingCommit[n] = None
  /\ CommitGuardsHold(n, pt)          \* the same guards as LakeCommitOk
  /\ lake' = lake \cup {pt}           \* the txn DID commit
  /\ wm'   = CommitWm(pt)
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = Attempt(pt, inc[n])]
  /\ UNCHANGED <<sealedParts, objects, expired, lossLedger, claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

LakeCommitIndeterminateLost(n, pt) ==
  /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ pt.sealer = n /\ n \in IndetOn
  /\ pt \in objects /\ pt \in sealedParts /\ pendingCommit[n] = None
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = Attempt(pt, inc[n])]
  /\ UNCHANGED <<sealedParts, objects, lake, expired, wm, lossLedger,
                 claims, closed>>     \* the txn did NOT commit
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

Landed(att, lk) == att.pt \in lk

Reconcile(n) ==                     \* EXACTLY ONE read-back before any retry
  /\ DrainOn /\ alive[n] /\ pendingCommit[n] # None
  /\ sealedParts' = IF Landed(pendingCommit[n], lake)
                    THEN sealedParts \ {pendingCommit[n].pt}  \* adopt it
                    ELSE sealedParts        \* a fresh attempt may begin
  /\ pendingCommit' = [pendingCommit EXCEPT ![n] = None]
  /\ UNCHANGED <<objects, lake, expired, wm, lossLedger, claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

\* 3.6 DrainWithoutWatermark: "a separate, unguarded advance action exists".
FreeWmAdvance ==
  /\ BrkDrainWithoutWatermark /\ DrainOn
  /\ \E p \in Partitions :
       /\ wm[p] < MaxWin
       /\ wm' = [wm EXCEPT ![p] = @ + 1]
  /\ UNCHANGED <<sealedParts, objects, lake, expired, lossLedger,
                 pendingCommit, claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Retention: Expire *)

IsChangelogData(pt) == KindOf[pt.part] = "changelog"

CoversArrival(s, pt) ==   \* snapshot disc: {<<p, o, asof_o>>} (TN-15)
  \A k \in pt.coverage : \E d \in s.disc : d[2] = k[2] /\ k[3] <= d[3]

RetentionElapsed(pt) ==   \* retention timing is nondeterministic, but a
                          \* retention clock is orders of magnitude longer
                          \* than a drain: it never elapses before the part's
                          \* window completes (TN-17)
  wm[pt.part] >= pt.window

Expire(pt) ==            \* the object's second and last storage operation
  /\ DrainOn /\ pt \in lake /\ RetentionElapsed(pt)
  /\ (IsChangelogData(pt) /\ pt.kind # "snapshot") =>
       \/ BrkExpireUncovered            \* 3.6: guard dropped
       \/ \E s \in lake : /\ s.kind = "snapshot" /\ s.part = pt.part
                          /\ CoversArrival(s, pt)   \* Keep Rule 10's guard
  /\ pt.kind = "snapshot" =>            \* snapshots expire only under a newer
       \E s2 \in lake \ {pt} :          \* covering snapshot (3.3 prose)
         s2.kind = "snapshot" /\ s2.part = pt.part /\ CoversArrival(s2, pt)
  /\ lake'    = lake \ {pt}
  /\ objects' = objects \ {pt}
  /\ expired' = expired \cup {pt}    \* history ledger: sanctioned removal
  /\ UNCHANGED <<sealedParts, wm, lossLedger, pendingCommit, claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Post-drain residency: Demote, Evict, DropWindow *)

CommittedDurably(p, w) == wm[p] >= w   \* commit + watermark txn durable (TN-18)

Demote(n, p, w) ==                    \* staging -> cache, in place
  /\ DrainOn /\ alive[n]
  /\ CommittedDurably(p, w)
  /\ BrkDemoteDirty \/ \A x \in PartsOf(p, w) :
                         x.kind = "window" => x.dedupRemoved = 0
  /\ WindowRecs(n, p, w) # {}
  /\ {Key(r) : r \in WindowRecs(n, p, w)} = WindowPartCov(p, w)
                                      \* only the node whose rows ARE the
                                      \* committed WINDOW part may demote
                                      \* (2.4); supplements are additional
                                      \* parts, never the table's substitute
                                      \* (TN-33)
  /\ cache'  = [cache  EXCEPT ![n] = @ \cup
                  {[part |-> p, window |-> w, rows |-> WindowRecs(n, p, w)]}]
  /\ staged' = [staged EXCEPT ![n] = @ \ WindowRecs(n, p, w)]
  /\ UNCHANGED <<inflight, dedup, nextSeq, resolved, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

DropWindow(n, p, w) ==                \* the default exit from staging
  /\ DrainOn /\ alive[n]
  /\ BrkEvictStaging \/ CommittedDurably(p, w)   \* 3.6: Evict reaches staging
  /\ LET dropSet ==   \* only rows the lake or the loss ledger covers may
                      \* leave staging: an unreceipted residue row is durable
                      \* data that WILL drain (as a supplement) and must
                      \* survive the winner's commit (TN-32)
           IF BrkEvictStaging THEN WindowRecs(n, p, w)
           ELSE {r \in WindowRecs(n, p, w) :
                   \/ Key(r) \in CommittedCov(p, w)
                   \/ \E l \in lossLedger : Covers(l, Key(r))}
     IN /\ dropSet # {}
        /\ staged' = [staged EXCEPT ![n] = @ \ dropSet]
  /\ UNCHANGED <<inflight, dedup, cache, nextSeq, resolved, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

Evict(n, t) ==                        \* cache only; ALWAYS enabled
  /\ t \in cache[n]
  /\ cache' = [cache EXCEPT ![n] = @ \ {t}]
  /\ UNCHANGED <<inflight, staged, dedup, nextSeq, resolved, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Changelog: SnapshotSeal *)

\* A fixed enumeration of Nodes; any fixed order serves the (origin, seq)
\* fold as long as every fold uses the same one (TN-19).
NodeOrd == CHOOSE f \in [Nodes -> 1..Cardinality(Nodes)] :
             \A a, b \in Nodes : a # b => f[a] # f[b]
OrdLt(r1, r2) == \/ NodeOrd[r1.origin] < NodeOrd[r2.origin]
                 \/ (r1.origin = r2.origin /\ r1.seq < r2.seq)

LatestSet(S) == {r \in S : \A r2 \in S :
                   CKeyOf[r2.req] = CKeyOf[r.req] => (r2 = r \/ OrdLt(r2, r))}
View(S) == {r \in LatestSet(S) : ~TombOf[r.req]}   \* tombstones delete

RecsOfCovSet(cov) ==
  {recOf[q] : q \in {q2 \in Requests :
     recOf[q2] # None /\ Key(recOf[q2]) \in cov}}

AllPartCov(p) ==   \* committed window-plane coverage of p (incl. expired)
  UNION {x.coverage : x \in {x2 \in lake \cup expired :
           x2.part = p /\ x2.kind # "snapshot"}}

SnapAsOf(n, p) == [o \in Nodes |-> AppliedThru(n, p, o)]

SnapshotSeal(n, p) ==
  /\ DrainOn /\ alive[n] /\ n \notin degraded
  /\ KindOf[p] = "changelog" /\ HoldsClaim(n, p)
  /\ ~\E s \in sealedParts \cup objects \cup lake :
       s.part = p /\ s.kind = "snapshot"
       \* serialized per partition under the drain scheduler (6); at most one
       \* snapshot in the checked scope -- "a snapshot expires only under a
       \* newer covering snapshot, which the small configuration never
       \* seals" (3.3 prose; TN-29)
  /\ LET asof == SnapAsOf(n, p)
         vis  == {r \in {r2 \in staged[n] :
                           r2.part = p /\ ~IsSchemaRec(r2)} \cup
                         RecsOfCovSet(AllPartCov(p)) :
                    r.seq <= asof[r.origin]}
         rows == LatestSet(vis)   \* full latest-state as-of.  Tombstone
                  \* rows are RETAINED in the snapshot (TN-34, FINDING):
                  \* "deleted keys absent" (3.3) is inconsistent with
                  \* LatestViewCorrect once a slower origin's earlier-ordered
                  \* straggler lands after the seal -- the overlay needs the
                  \* tombstone to keep winning.  Reads drop tombstones.
         cov  == {Key(r) : r \in rows}
         disc == {<<p, o, asof[o]>> : o \in Nodes}  \* snapshot_as_of_seq
         pt   == [part |-> p, window |-> 0, kind |-> "snapshot", disc |-> disc,
                  coverage |-> cov, extent |-> cov,
                  sealer |-> n, inc |-> inc[n], dedupRemoved |-> 0]
     IN /\ \E o \in Nodes : asof[o] > 0
        /\ sealedParts' = sealedParts \cup {pt}
  /\ UNCHANGED <<objects, lake, expired, wm, lossLedger, pendingCommit,
                 claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Membership and failure *)

ClaimAdvertise(n, p) ==  \* advisory registry row (TN-4: fields trimmed to
  /\ DrainOn /\ alive[n] \* what a v0.1 property could ever read: nothing)
  /\ <<n, p>> \in InitClaims   \* the configuration's claimable set: which
                               \* nodes route drains is a routing pin like
                               \* AcceptorOf (TN-4); v0.1 configs pre-seed
                               \* every claimable row, so this action is
                               \* quiescent until v0.2 restores dynamics
  /\ <<n, p>> \notin claims
  /\ claims' = claims \cup {<<n, p>>}
  /\ UNCHANGED <<sealedParts, objects, lake, expired, wm, lossLedger,
                 pendingCommit, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

TakeoverDrain(n, p) ==  \* v0.2: a live node claims an orphaned partition when
  /\ TakeoverOn        \* its current holder(s) are all dead (5.6). WHICH
  /\ alive[n] /\ n \notin degraded   \* live node acquires it is a Rust-level
  /\ <<n, p>> \notin claims           \* placement detail (HRW, ADR-0004);
  /\ ~\E n1 \in Nodes :               \* modeling it as ANY live node proves
       alive[n1] /\ HoldsClaim(n1, p) \* safety for whichever one HRW picks.
                                       \* A dead holder's stale claim is left
                                       \* in place (claims are advisory, TN-4
                                       \* -- real safety is SingleDrainCommit
                                       \* at the catalog, not claim upkeep).
  /\ claims' = claims \cup {<<n, p>>}
  /\ UNCHANGED <<sealedParts, objects, lake, expired, wm, lossLedger,
                 pendingCommit, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

Heartbeat(n) ==
  /\ alive[n] /\ hb[n] < MaxHb
  /\ hb' = [hb EXCEPT ![n] = @ + 1]
  /\ UNCHANGED <<catalogSeq, inc, alive, degraded, wiped, crashBudget>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED drnVars
  /\ UNCHANGED schVars

FenceBoot(n) ==          \* recovery entry point; incarnation from the catalog
  /\ (~alive[n] \/ n \in degraded)   \* also promotes a degraded node (TN-20)
  /\ n \notin wiped
  /\ catalogSeq' = catalogSeq + 1
  /\ inc'   = [inc EXCEPT ![n] = catalogSeq + 1]
  /\ alive' = [alive EXCEPT ![n] = TRUE]
  /\ degraded' = degraded \ {n}
  \* recovery state = staged[n], replayed as-is: staging tables ARE the WAL
  /\ UNCHANGED <<hb, wiped, crashBudget>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED drnVars
  /\ UNCHANGED schVars

DegradedBoot(n) ==       \* catalog down at boot, persisted incarnation (5.7)
  /\ ~alive[n] /\ n \notin wiped /\ inc[n] > 0
  /\ alive'    = [alive EXCEPT ![n] = TRUE]
  /\ degraded' = degraded \cup {n}
  /\ UNCHANGED <<catalogSeq, inc, hb, wiped, crashBudget>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED drnVars
  /\ UNCHANGED schVars

RecoverNode(n) == FenceBoot(n)   \* there is no other recovery input

HoldsCoverageK(n, p, k) == \E r \in staged[n] : r.part = p /\ Key(r) = k

Advertises(n, p, k) ==   \* honest guard == ground truth: the ceremony reads
  HoldsCoverageK(n, p, k)  \* live registries, not a stale cache (TN-21)

CommittedExtent(p) ==
  UNION {x.extent : x \in {x2 \in lake : x2.part = p /\ x2.kind # "snapshot"}}

Holes(p) ==   \* extent keys no committed part covers and no ledger row admits
  {k \in CommittedExtent(p) :
     /\ k \notin AllPartCov(p)
     /\ ~\E l \in lossLedger : Covers(l, k)}

DeclareLoss(p, k) ==     \* OPERATOR action -- never autonomous (9's ceremony)
  /\ DrainOn /\ k \in Holes(p)
  /\ ~\E n \in Nodes \ wiped : Advertises(n, p, k)  \* refused while any
                                                    \* un-wiped replica covers
  /\ lossLedger' = lossLedger \cup {[part |-> p, range |-> {k},
       liveAtDecl |-> \E n \in Nodes \ wiped :      \* history flag: was the
            alive[n] /\ HoldsCoverageK(n, p, k)]}   \* confession false?
  /\ wm' = [wm EXCEPT ![p] = NewWatermark(p, lake, lossLedger')]
  \* ledger row and watermark advance: ONE catalog transaction (A2)
  /\ UNCHANGED <<sealedParts, objects, lake, expired, pendingCommit,
                 claims, closed>>
  /\ UNCHANGED ingVars /\ UNCHANGED repVars /\ UNCHANGED memVars
  /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* 3.3 -- Schema: EvolveSchema *)

EvolveSchema(n, d, s) ==   \* a schema change IS a sequenced record
  /\ alive[n] /\ s \in LatticeElem
  /\ s = LatticeJoin(schema[n][d], s)   \* monotone: join, never a rewrite
  /\ s # schema[n][d]                   \* strict widening (TN-22)
  /\ schema' = [schema EXCEPT ![n][d] = s]
  /\ LET p == HomePartition[d]
         r == [req |-> None, part |-> p, origin |-> n, seq |-> nextSeq[n][p],
               window |-> 0, dataset |-> d, elem |-> s]
     IN /\ staged'  = [staged  EXCEPT ![n] = @ \cup {r}]
        /\ nextSeq' = [nextSeq EXCEPT ![n][p] = @ + 1]
  /\ UNCHANGED <<inflight, dedup, cache, resolved, ackEvidence, recOf>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED memVars
  /\ UNCHANGED staleApplied

-----------------------------------------------------------------------------
(* 3.3 -- Crash and recovery: CrashNode, CrashWipe *)

CrashNode(n) ==          \* enabled at ANY interleaving point (TN-3 bounds it)
  /\ alive[n] /\ n \in Crashable /\ crashBudget > 0
  /\ alive'    = [alive EXCEPT ![n] = FALSE]
  /\ inflight' = [inflight EXCEPT ![n] = {}]   \* volatile state gone
  /\ crashBudget' = crashBudget - 1
  /\ UNCHANGED <<staged, dedup, cache, nextSeq, resolved, ackEvidence, recOf>>
  /\ UNCHANGED <<catalogSeq, hb, inc, degraded, wiped>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED schVars
                                               \* fsynced state survives (A1)

CrashWipe(n) ==          \* the disk dies too -- bounded by the fault budget
  /\ n \notin wiped
  /\ Cardinality(wiped \cup {n}) <= WipeBudget
  /\ wiped'  = wiped \cup {n}
  /\ staged' = [staged EXCEPT ![n] = {}]
  /\ cache'  = [cache EXCEPT ![n] = {}]
  /\ dedup'  = [dedup EXCEPT ![n] = {}]
  /\ inflight' = [inflight EXCEPT ![n] = {}]
  /\ alive'  = [alive EXCEPT ![n] = FALSE]     \* a wiped node never re-enters
  /\ UNCHANGED <<nextSeq, resolved, ackEvidence, recOf>>
  /\ UNCHANGED <<catalogSeq, hb, inc, degraded, crashBudget>>
  /\ UNCHANGED repVars /\ UNCHANGED drnVars /\ UNCHANGED schVars

-----------------------------------------------------------------------------
(* Init and Next *)

Init ==
  /\ inflight  = [n \in Nodes |-> {}]
  /\ staged    = [n \in Nodes |-> {}]
  /\ dedup     = [n \in Nodes |-> {}]
  /\ cache     = [n \in Nodes |-> {}]
  /\ nextSeq   = [n \in Nodes |-> [p \in Partitions |-> 1]]   \* 1-based
  /\ chan      = {}
  /\ receipts  = {}
  /\ highestSeen = [n \in Nodes |-> [o \in Nodes |-> 0]]
  /\ resolved  = [q \in Requests |-> "unsent"]
  /\ ackEvidence = [q \in Requests |-> {}]
  /\ recOf     = [q \in Requests |-> None]
  /\ sealedParts = {} /\ objects = {} /\ lake = {} /\ expired = {}
  /\ wm        = [p \in Partitions |-> 0]
  /\ lossLedger = {}
  /\ catalogSeq = 0
  /\ pendingCommit = [n \in Nodes |-> None]
  /\ claims = InitClaims /\ hb = [n \in Nodes |-> 0]
  /\ inc = [n \in Nodes |-> 0]
  /\ alive = [n \in Nodes |-> TRUE]
  /\ degraded = {} /\ wiped = {}
  /\ schema = [n \in Nodes |-> [d \in Datasets |-> {}]]   \* lattice bottom
  /\ staleApplied = {}
  /\ closed = {}
  /\ crashBudget = MaxCrashes

Next ==
  \/ \E n \in Nodes, q \in Requests :
       Accept(n, q) \/ DedupCheck(n, q) \/ StageCommit(n, q)
         \/ ClientAck(n, q) \/ Throttle(n, q) \/ Refuse(n, q)
  \/ \E q \in Requests : ClientTimeout(q)
  \/ \E n, m \in Nodes : \E r \in staged[n] : Forward(n, m, r)
  \/ \E m \in Nodes : \E g \in chan : PeerApply(m, g)
  \/ \E m \in Nodes : \E r \in staged[m] : Receipt(m, r)
  \/ \E p \in Partitions, w \in Windows : CloseWindow(p, w)
  \/ \E n \in Nodes, p \in Partitions, w \in Windows :
       SealPart(n, p, w) \/ SealSupplement(n, p, w)
         \/ Demote(n, p, w) \/ DropWindow(n, p, w)
  \/ \E pt \in sealedParts : PutPart(pt)
  \/ \E n \in Nodes : \E pt \in objects :
       LakeCommitOk(n, pt) \/ LakeCommitAbort(n, pt)
         \/ LakeCommitIndeterminateLanded(n, pt)
         \/ LakeCommitIndeterminateLost(n, pt)
         \/ ReclaimSeal(n, pt)
  \/ \E n \in Nodes : Reconcile(n)
  \/ \E pt \in lake : Expire(pt)
  \/ \E n \in Nodes : \E t \in cache[n] : Evict(n, t)
  \/ \E n \in Nodes, p \in Partitions :
       SnapshotSeal(n, p) \/ ClaimAdvertise(n, p) \/ TakeoverDrain(n, p)
  \/ \E n \in Nodes :
       Heartbeat(n) \/ CrashNode(n) \/ CrashWipe(n)
         \/ FenceBoot(n) \/ DegradedBoot(n)
  \/ \E p \in Partitions : \E k \in Holes(p) : DeclareLoss(p, k)
  \/ \E n \in Nodes, d \in Datasets : \E s \in LatticeElem : EvolveSchema(n, d, s)
  \/ FreeWmAdvance
  \/ UNCHANGED vars   \* quiescence is stuttering, not deadlock (TN-23)

-----------------------------------------------------------------------------
(* 3.5 -- fairness.  Weak fairness on the resolver and drain pipeline       *)
(* actions (README 3.5 list; TN-24 records the additions the list's own    *)
(* liveness properties force: DedupCheck, CloseWindow, ClaimAdvertise,     *)
(* FenceBoot, and strong fairness on LakeCommitOk).                        *)

AStageCommit == \E n \in Nodes, q \in Requests : StageCommit(n, q)
ADedupCheck  == \E n \in Nodes, q \in Requests : DedupCheck(n, q)
AClientAck   == \E n \in Nodes, q \in Requests : ClientAck(n, q)
AThrottle    == \E n \in Nodes, q \in Requests : Throttle(n, q)
ARefuse      == \E n \in Nodes, q \in Requests : Refuse(n, q)
AClientTimeout == \E q \in Requests : ClientTimeout(q)
AForward     == \E n, m \in Nodes : \E r \in staged[n] : Forward(n, m, r)
APeerApply   == \E m \in Nodes : \E g \in chan : PeerApply(m, g)
AReceipt     == \E m \in Nodes : \E r \in staged[m] : Receipt(m, r)
ACloseWindow == \E p \in Partitions, w \in Windows : CloseWindow(p, w)
ASealPart    == \E n \in Nodes, p \in Partitions, w \in Windows : SealPart(n, p, w)
ASealSupplement == \E n \in Nodes, p \in Partitions, w \in Windows :
                     SealSupplement(n, p, w)
APutPart     == \E pt \in sealedParts : PutPart(pt)
ALakeCommitOk == \E n \in Nodes : \E pt \in objects : LakeCommitOk(n, pt)
AReconcile   == \E n \in Nodes : Reconcile(n)
AClaimAdvertise == \E n \in Nodes, p \in Partitions : ClaimAdvertise(n, p)
AFenceBoot   == \E n \in Nodes : FenceBoot(n)
AReclaimSeal == \E n \in Nodes : \E pt \in objects : ReclaimSeal(n, pt)

FairnessBase ==   \* everything except the catalog-commit fairness
  /\ WF_vars(AStageCommit) /\ WF_vars(ADedupCheck) /\ WF_vars(AClientAck)
  /\ WF_vars(AThrottle) /\ WF_vars(ARefuse) /\ WF_vars(AClientTimeout)
  /\ WF_vars(AForward) /\ WF_vars(APeerApply) /\ WF_vars(AReceipt)
  /\ WF_vars(ACloseWindow) /\ WF_vars(ASealPart) /\ WF_vars(ASealSupplement)
  /\ WF_vars(APutPart) /\ WF_vars(AReconcile)
  /\ WF_vars(AClaimAdvertise) /\ WF_vars(AFenceBoot) /\ WF_vars(AReclaimSeal)

CoreFairness ==   \* the catalog accepts commits (LakeAccepts, 3.5)
  FairnessBase /\ SF_vars(ALakeCommitOk)

-----------------------------------------------------------------------------
(* 3.4 -- Invariants.  Ten state invariants plus the ladder action property.*)

DurableAck ==
  \A q \in Acked :
    /\ Cardinality(ackEvidence[q]) >= RF
    /\ \A m \in ackEvidence[q] :
         m = OriginOf(q) \/ \E rc \in receipts : rc.holder = m /\ rc.key = KeyOf(q)

InLake(k) == \E x \in lake \cup expired : k \in x.coverage

NoAckedLoss ==
  \A q \in Acked :
    InLake(KeyOf(q)) \/ \E n \in Nodes \ wiped :
                          \E r \in staged[n] : Key(r) = KeyOf(q)

WatermarkHonesty ==
  \A p \in Partitions : \A q \in Acked :
    (recOf[q].part = p /\ WindowOf(q) <= wm[p]) =>
      InLake(KeyOf(q)) \/ \E l \in lossLedger : Covers(l, KeyOf(q))

Rows(t) == t.rows   \* a WinTbl CARRIES its row set (3.2)

LakeRowsOf(t) ==   \* ITS committed part (3.4): the kind-"window" part the
                   \* demote substituted the hot table for (TN-33)
  CanonKept(RecsOfCovSet(WindowPartCov(t.part, t.window)))

CacheTransparency ==
  \A n \in Nodes : \A t \in cache[n] : Rows(t) = LakeRowsOf(t)

GapFreedom ==   \* quantified over HOLDERS: windows commit in any order, so a
                \* node holding nothing for (p, o) can see a non-prefix D --
                \* a legal state formal-core.md's formula miscounts (TN-31)
  \A n \in Nodes, p \in Partitions, o \in Nodes :
    LET S == {r.seq : r \in {r2 \in staged[n] : r2.part = p /\ r2.origin = o}}
        D == DrainedSeqs(p, o)
    IN  S # {} => S \cup D = 1..Cardinality(S \cup D)

SingleDrainCommit ==
  /\ \A a, b \in lake :
       (a.part = b.part /\ a.window = b.window /\ a.kind = b.kind
        /\ a.disc = b.disc) => a = b
  /\ \A a \in lake : a.kind = "window" =>
       \A b \in lake : (SameWindow(a, b) /\ b.kind = "window") => a = b
  /\ \A s \in lake : s.kind = "supplement" =>
       \A x \in lake : (SameWindow(x, s) /\ x # s) =>
         x.coverage \cap s.coverage = {}

FencedZombie == staleApplied = {}

LossLedgerTruthful == \A l \in lossLedger : ~l.liveAtDecl

SnapshotCovered ==
  \A e \in expired : (IsChangelogData(e) /\ e.kind # "snapshot") =>
    \E s \in lake : s.kind = "snapshot" /\ s.part = e.part /\ CoversArrival(s, e)

AllRecs(p) ==   \* every committed and staged record for the partition
  RecsOfCovSet(AllPartCov(p)) \cup
    UNION {{r \in staged[n] : r.part = p /\ ~IsSchemaRec(r)} : n \in Nodes}

LatestViewCorrect ==
  \A p \in Partitions : KindOf[p] = "changelog" =>
    \A s \in {x \in lake : x.kind = "snapshot" /\ x.part = p} :
      LET after == {r \in AllRecs(p) :
                      \A d \in s.disc : d[2] = r.origin => r.seq > d[3]}
      IN  View(RecsOfCovSet(s.coverage) \cup after) = View(AllRecs(p))

\* Schema.tla's owned properties: lattice monotonicity and replay
\* convergence (README module map; TN-26).
SchemaMonotone ==   \* widen by join, never a rewrite
  [][ \A n \in Nodes, d \in Datasets : schema[n][d] \subseteq schema'[n][d] ]_vars

SchemaConvergence ==   \* replay convergence as safety: equal applied
  \A n \in Nodes \ wiped : \A m \in Nodes \ wiped : \A d \in Datasets :
    (\A o \in Nodes : AppliedThru(n, HomePartition[d], o)
                        = AppliedThru(m, HomePartition[d], o))
      => schema[n][d] = schema[m][d]

Allowed(k) ==   \* the client-visible operations permitted at rung k
  CASE k = 0 -> {"accept", "ack", "replicate-new", "catch-up"}
    [] k = 1 -> {"accept", "ack", "replicate-new", "catch-up"}  \* disclose
    [] k = 2 -> {"ack", "replicate-new", "catch-up"}  \* throttle: no accepts
    [] k = 3 -> {"ack", "catch-up"}                   \* refuse + no new ranges

LadderMonotone ==   \* an ACTION property (3.4)
  /\ \A j, k \in 0..3 : j <= k => Allowed(k) \subseteq Allowed(j)
  /\ [][ /\ \A n \in Nodes, q \in Requests :
              /\ Accept(n, q)    => "accept" \in Allowed(Rung(n))
              /\ ClientAck(n, q) => "ack"    \in Allowed(Rung(n))
         /\ \A m \in Nodes : \A g \in chan :
              PeerApply(m, g) =>
                (IF IsCatchup(g) THEN "catch-up" ELSE "replicate-new")
                   \in Allowed(Rung(m)) ]_vars

-----------------------------------------------------------------------------
(* 3.5 -- liveness properties *)

EveryRequestResolves ==
  \A q \in Requests :
    (resolved[q] = "pending") ~> (resolved[q] \in {"acked", "throttled", "refused"})

AckedIn(p, w) ==
  \E q \in Acked : recOf[q].part = p /\ recOf[q].window = w

WatermarkEventuallyAdvances ==
  \A p \in Partitions : \A w \in Windows :
    (DrainOn /\ AckedIn(p, w) /\ wm[p] < w) ~> (wm[p] >= w)

-----------------------------------------------------------------------------
(* 3.5 -- FINDINGS: properties DuckSpout deliberately does NOT have.  Each  *)
(* is run in a dedicated configuration under specs/broken/ and MUST fail.   *)

Finding_BoundedAckLatency ==   \* no ack-latency promise: throttle, not deadline
  \A q \in Requests :
    (resolved[q] = "pending") ~> (resolved[q] = "acked")

Finding_PerOriginFairness ==   \* no cross-origin fairness in v1: a throttled
  \A q \in Requests :          \* client may be throttled indefinitely
    (resolved[q] = "throttled") ~> (resolved[q] = "acked")

Finding_BoundedThrottleDuration ==   \* no bound while staging is full and
  \A n \in Nodes :                   \* drains are stalled
    [](Rung(n) >= 2 => <>(Rung(n) < 2))

BelowFloor == Cardinality({n \in Nodes : alive[n] /\ n \notin wiped}) < RF

Finding_RefuseFreeBelowRF ==   \* below the floor, ingest does not eventually
  \A q \in Requests :          \* accept: refuse-only is the design (5.1)
    (resolved[q] = "pending" /\ BelowFloor) ~> (resolved[q] = "acked")

\* Finding_WatermarkThroughCatalogOutage: WatermarkEventuallyAdvances checked
\* under FairnessBase alone -- without the catalog-recovers fairness
\* assumption (SF on LakeCommitOk) -- in its dedicated broken/ config.

-----------------------------------------------------------------------------
(* 3.6 -- non-vacuity witnesses.  Each Witness_* names a state or step the  *)
(* model must genuinely reach; its broken/ config asserts the negation and  *)
(* MUST produce a counterexample.  State witnesses are NoWitness_*          *)
(* invariants; step witnesses are NoWitness_* action properties.            *)

NoWitness_DedupReplayAcked ==   \* the replay branch is live, not dead code
  [][ ~\E n \in Nodes, q \in Requests :
        DedupCheck(n, q) /\ resolved'[q] = "acked" ]_vars

NoWitness_ThrottleAndRefuseTaken ==   \* the upper rungs are reachable
  [][ ~( (\E q \in Requests : resolved[q] = "refused")
         /\ \E n \in Nodes, q \in Requests : Throttle(n, q) ) ]_vars

NoWitness_ReceiptOutstandingAtAck ==  \* the RF wait is a real wait
  ~\E n \in Nodes, q \in Requests :
     /\ resolved[q] = "pending"
     /\ \E r \in staged[n] :
          /\ r.req = q /\ r.origin = n
          /\ \E g \in chan : g.rec = r        \* a Forward is in flight
          /\ Cardinality({n} \cup {rc.holder :
               rc \in {rc2 \in receipts : rc2.key = Key(r)}}) < RF

NoWitness_LossDeclared ==   \* with the budget raised past RF - 1,
  lossLedger = {}           \* DeclareLoss actually fires end-to-end

NoWitness_LossRefusedOverLiveReplica ==   \* the ceremony's refusal is its
  ~\E p \in Partitions : \E k \in Holes(p) :  \* own reachable state
     \E n \in Nodes \ wiped : alive[n] /\ HoldsCoverageK(n, p, k)

NoWitness_IndeterminateResolved ==   \* the three-way commit's least trivial
  [][ ~\E n \in Nodes :              \* branch: Landed, then Reconcile adopts
        /\ Reconcile(n)
        /\ Landed(pendingCommit[n], lake) ]_vars

NoWitness_SupplementCommits ==
  ~\E x \in lake : x.kind = "supplement"

NoWitness_SupplementPending ==   \* winner committed, receipted residue
  ~\E p \in Partitions, w \in Windows :  \* staged, wm has NOT advanced
     /\ \E x \in lake : x.part = p /\ x.window = w /\ x.kind = "window"
     /\ wm[p] < w
     /\ \E n \in Nodes : \E r \in WindowRecs(n, p, w) :
          /\ Key(r) \notin CommittedCov(p, w)
          /\ \E rc \in receipts : rc.key = Key(r)

NoWitness_SchemaWidensInFlight ==   \* a catching-up peer applied a foreign
  ~\E m \in Nodes :                 \* widening and that sender's data;
     \E rs \in staged[m] :          \* PeerApply's SchemaKnown guard enforced
       /\ IsSchemaRec(rs)           \* the widen-before-data order on the way
       /\ rs.origin # m /\ rs.elem # {}
       /\ \E rd \in staged[m] : ~IsSchemaRec(rd) /\ rd.origin = rs.origin

NoWitness_CrashBetweenCommitAndDemote ==   \* the crash window is reached
  ~\E n \in Nodes : \E t \in cache[n] :    \* and recovered through: the part
     \E x \in PartsOf(t.part, t.window) :  \* was committed under an inc the
       /\ x.kind = "window"                \* demoting node has since outgrown,
       /\ x.sealer = n                     \* so a FenceBoot fell strictly
       /\ x.inc < inc[n]                   \* between LakeCommitOk and Demote

NoWitness_TakeoverDrains ==   \* a node with no pre-seeded claim on the
  ~\E x \in lake :             \* partition still sealed and committed a
     /\ x.kind = "window"      \* window part for it end-to-end -- since
     /\ <<x.sealer, x.part>> \notin InitClaims   \* ClaimAdvertise is itself
                               \* restricted to InitClaims pairs (TN-4), the
                               \* only action that can grow a node's claim
                               \* beyond the pre-seeded set is TakeoverDrain

NoWitness_ReclaimSealFires ==   \* the reclaim path (v0.2, #177) is live, not
  [][ ~\E n \in Nodes, pt \in objects :  \* dead code: a recovering node's own
        ReclaimSeal(n, pt) ]_vars        \* orphaned seal genuinely gets
                                          \* reclaimed, not just declared

=============================================================================
