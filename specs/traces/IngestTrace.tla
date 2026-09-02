---------------------------- MODULE IngestTrace ----------------------------
(***************************************************************************)
(* Trace refinement for the ingest surface (3.7, 8.2; specs/README.md      *)
(* four-file pattern, file 3): constrains the shared DuckSpoutCore Next to *)
(* one recorded NDJSON run and checks the two 3.7 obligations:             *)
(*                                                                         *)
(*   1. Every recorded run is a behavior of the model.  A recorded event   *)
(*      the model has no transition for halts the walk at that entry; the  *)
(*      POSTCONDITION (TraceAccepted) then fails and Print names the halt  *)
(*      cursor -- the runner (scripts/trace-conformance.mjs) asserts WHICH *)
(*      entry the walk halted at (8.2's per-mechanism discipline).         *)
(*   2. Every required step was recorded -- the TraceComplete invariant:   *)
(*      obligations a recorded prefix creates (a pending resolution, an    *)
(*      unresolved Indeterminate, a PUT part never committed or            *)
(*      abandoned) must be discharged by the time the trace ends.          *)
(*                                                                         *)
(* Events are payload-free at v0.1 (D-6: the TraceRecord is               *)
(* {node, seq, event}), so each entry pins the acting node and the action  *)
(* NAME, and the action's parameters are existentially matched -- TLC      *)
(* explores every binding, and the walk survives iff some binding does.    *)
(* Per-node seq density is validated by the runner's decoder (a distinct   *)
(* rejection mechanism, armed by its own doctored fixture); the file       *)
(* order of the NDJSON lines is the total order the refinement replays.    *)
(*                                                                         *)
(* Environment transitions are interleaved WITHOUT consuming an entry:     *)
(* CloseWindow is 3.1's un-journaled window-close (TN-1), and CrashNode /  *)
(* CrashWipe are environment events a node cannot journal (3.7) -- this    *)
(* configuration pins MaxCrashes = 0, so only CloseWindow is live until a  *)
(* crash-schedule fixture arms them.                                       *)
(*                                                                         *)
(* Scope: Nodes = {n1, n2} at RF = 1 -- the v0.1 implementation IS        *)
(* single-node-durable (accept acks after the local StageCommit; the RF-1  *)
(* receipt wait is the v0.2 seam), so a faithful trace of today's code     *)
(* cannot satisfy an RF = 2 ClientAck guard.  TN-25's no-RF=1 rule binds   *)
(* the CLEAN model-checking configs, where RF = 2 keeps guards from being  *)
(* dead code; a refinement config must instead match the implementation    *)
(* it accepts traces from.  Two nodes keep the TN-32 residue shape         *)
(* representable (a second holder's uncovered row surviving DropWindow --  *)
(* the 141 fixture); q4 is n2's request, q1..q3 land on n1, and all dedup  *)
(* keys are distinct because the dedup window is not implemented yet       *)
(* (issue 33) -- no trace can journal a DedupCheck today.                  *)
(***************************************************************************)
EXTENDS DuckSpoutCore, Sequences, Json, IOUtils, TLC

CONSTANTS n1, n2, q1, q2, q3, q4, q5, p1, d1, dkA, dkB, dkC, dkD, ckA

\* q2 shares q1's dedup key (the 3.1 colliding-key pin, so DedupCheck's
\* replay branch is matchable — the captured fixture journals a live
\* duplicate replay); q4 is n2's request (the TN-32 residue holder); q5 is
\* the post-drain export into window 2.
TraceWinOf      == (q1 :> 1) @@ (q2 :> 1) @@ (q3 :> 1) @@ (q4 :> 1) @@ (q5 :> 2)
TracePartOf     == [q \in Requests |-> p1]
TraceDKeyOf     == (q1 :> dkA) @@ (q2 :> dkA) @@ (q3 :> dkB) @@ (q4 :> dkC) @@ (q5 :> dkD)
TraceCKeyOf     == [q \in Requests |-> ckA]
TraceTombOf     == [q \in Requests |-> FALSE]
TraceKindOf     == [p \in Partitions |-> "event"]
TraceHome       == [d \in Datasets |-> p1]
TraceDsOf       == [q \in Requests |-> d1]
TraceAcceptorOf == (q1 :> {n1}) @@ (q2 :> {n1}) @@ (q3 :> {n1}) @@ (q4 :> {n2}) @@ (q5 :> {n1})
TraceInitClaims == {<<n1, p1>>, <<n2, p1>>}   \* both nodes may drain p1

\* The recorded run: one JSON object {node, seq, event} per line (D-6),
\* handed in by scripts/tla.mjs tv via the TRACE_PATH environment variable.
Trace == ndJsonDeserialize(IOEnv.TRACE_PATH)

\* Journaling node -> model node.  ClientTimeout entries carry the loadgen
\* fleet member's id and never consult this mapping (3.7).
NodeOf(name) == CASE name = "n1" -> n1 [] name = "n2" -> n2

VARIABLE cursor
traceVars == <<vars, cursor>>

\* TLC register tracking the highest cursor any explored behavior reached;
\* the postcondition reads it to name the halt entry.  Sound because tv
\* runs -workers 1 (s5.3): TLC registers are per-worker.
HaltReg == 0
ASSUME TLCSet(HaltReg, 1)

(***************************************************************************)
(* One recorded entry -> the disjunction of model transitions it may       *)
(* attest.  A name outside the arm set (or outside the 3.3 vocabulary)     *)
(* matches nothing and halts the walk.  SealPart covers SealSupplement     *)
(* too: the supplement seal journals the same action name (3.3 owns no     *)
(* separate name for it), and LakeCommitIndeterminate covers both model    *)
(* successors -- the one-journaled-name rule (3.7, trace-mapping).         *)
(* TakeoverDrain is deliberately FALSE until Replication.tla lands (v0.2). *)
(***************************************************************************)
MatchedStep(t) ==
  CASE t.event = "Accept"       -> \E q \in Requests : Accept(NodeOf(t.node), q)
    [] t.event = "DedupCheck"   -> \E q \in Requests : DedupCheck(NodeOf(t.node), q)
    [] t.event = "StageCommit"  -> \E q \in Requests : StageCommit(NodeOf(t.node), q)
    [] t.event = "ClientAck"    -> \E q \in Requests : ClientAck(NodeOf(t.node), q)
    [] t.event = "Throttle"     -> \E q \in Requests : Throttle(NodeOf(t.node), q)
    [] t.event = "Refuse"       -> \E q \in Requests : Refuse(NodeOf(t.node), q)
    [] t.event = "ClientTimeout" -> \E q \in Requests : ClientTimeout(q)
    [] t.event = "Forward"      -> \E m \in Nodes : \E r \in staged[NodeOf(t.node)] :
                                     Forward(NodeOf(t.node), m, r)
    [] t.event = "PeerApply"    -> \E g \in chan : PeerApply(NodeOf(t.node), g)
    [] t.event = "Receipt"      -> \E r \in staged[NodeOf(t.node)] :
                                     Receipt(NodeOf(t.node), r)
    [] t.event = "SealPart"     -> \E p \in Partitions, w \in Windows :
                                     SealPart(NodeOf(t.node), p, w)
                                       \/ SealSupplement(NodeOf(t.node), p, w)
    [] t.event = "PutPart"      -> \E pt \in sealedParts :
                                     pt.sealer = NodeOf(t.node) /\ PutPart(pt)
    [] t.event = "LakeCommitOk" -> \E pt \in objects : LakeCommitOk(NodeOf(t.node), pt)
    [] t.event = "LakeCommitAbort" -> \E pt \in objects :
                                        LakeCommitAbort(NodeOf(t.node), pt)
    [] t.event = "LakeCommitIndeterminate" ->
         \E pt \in objects : LakeCommitIndeterminateLanded(NodeOf(t.node), pt)
                               \/ LakeCommitIndeterminateLost(NodeOf(t.node), pt)
    [] t.event = "Reconcile"    -> Reconcile(NodeOf(t.node))
    [] t.event = "Expire"       -> \E pt \in lake : Expire(pt)
    [] t.event = "Demote"       -> \E p \in Partitions, w \in Windows :
                                     Demote(NodeOf(t.node), p, w)
    [] t.event = "Evict"        -> \E tb \in cache[NodeOf(t.node)] :
                                     Evict(NodeOf(t.node), tb)
    [] t.event = "DropWindow"   -> \E p \in Partitions, w \in Windows :
                                     DropWindow(NodeOf(t.node), p, w)
    [] t.event = "SnapshotSeal" -> \E p \in Partitions : SnapshotSeal(NodeOf(t.node), p)
    [] t.event = "ClaimAdvertise" -> \E p \in Partitions :
                                       ClaimAdvertise(NodeOf(t.node), p)
    [] t.event = "Heartbeat"    -> Heartbeat(NodeOf(t.node))
    [] t.event = "FenceBoot"    -> FenceBoot(NodeOf(t.node))
    [] t.event = "DegradedBoot" -> DegradedBoot(NodeOf(t.node))
    [] t.event = "TakeoverDrain" -> FALSE   \* v0.2: Replication.tla's action
    [] t.event = "DeclareLoss"  -> \E p \in Partitions :
                                     \E k \in Holes(p) : DeclareLoss(p, k)
    [] t.event = "EvolveSchema" -> \E d \in Datasets : \E s \in LatticeElem :
                                     EvolveSchema(NodeOf(t.node), d, s)
    [] OTHER -> FALSE

\* Consume the entry at the cursor; the register remembers how far any
\* behavior got (cursor' is the next entry to explain -- so after a full
\* walk the register holds Len(Trace) + 1, and after a halt it holds the
\* 1-based index of the entry nothing could explain).
ReadNext ==
  /\ cursor <= Len(Trace)
  /\ MatchedStep(Trace[cursor])
  /\ cursor' = cursor + 1
  /\ TLCSet(HaltReg, Max2(TLCGet(HaltReg), cursor + 1))

\* Un-journaled environment transitions (module header).  Finitely enabled:
\* each window closes at most once, so BFS terminates.
EnvNext ==
  /\ cursor <= Len(Trace)
  /\ \E p \in Partitions, w \in Windows : CloseWindow(p, w)
  /\ UNCHANGED cursor

TraceDone == cursor = Len(Trace) + 1

TraceInit == Init /\ cursor = 1
TraceNext == ReadNext \/ EnvNext
TraceSpec == TraceInit /\ [][TraceNext]_traceVars

(***************************************************************************)
(* Obligation 2: every step that must have happened was recorded.  Each    *)
(* clause discharges an obligation an earlier recorded entry created; a    *)
(* trace ending with one open is a subsystem that performed a modeled      *)
(* transition without journaling it (8.2's "silent steps" failure class).  *)
(***************************************************************************)
TraceComplete ==
  TraceDone =>
    \* Resolution clause: an accepted request resolves in the record
    \* (ClientAck / Throttle / Refuse / the loadgen's ClientTimeout).
    /\ \A q \in Requests : resolved[q] # "pending"
    \* Indeterminate clause: every LakeCommitIndeterminate was Reconciled.
    /\ \A n \in Nodes : pendingCommit[n] = None
    \* Drain-pipeline clause: no PUT part left neither committed nor
    \* abandoned -- a recorded SealPart + PutPart whose LakeCommit outcome
    \* was never journaled.
    /\ \A pt \in objects : pt \in lake \/ pt \notin sealedParts

\* Obligation 1's verdict: some explored behavior consumed the whole trace.
\* On failure, Print names the halt entry (the 1-based NDJSON line index
\* nothing could explain); the runner asserts it per doctored fixture.
TraceAccepted ==
  LET high == TLCGet(HaltReg)
  IN  IF high = Len(Trace) + 1
      THEN TRUE
      ELSE Print(<<"TraceHalt", high>>, FALSE)

=============================================================================
