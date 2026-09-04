------------------------- MODULE ReplicationTrace --------------------------
(***************************************************************************)
(* Trace refinement for the replication surface (3.7, 8.2; specs/README.md *)
(* four-file pattern, file 3) -- Replication.tla's trace-refinement        *)
(* sibling, following IngestTrace.tla's exact pattern.  Constrains the     *)
(* shared DuckSpoutCore Next to one recorded NDJSON run and checks the two *)
(* 3.7 obligations (see IngestTrace.tla's header for the full statement of *)
(* both): every recorded run is a model behavior (a halt names the first   *)
(* unexplainable entry), and every required step was recorded             *)
(* (TraceComplete).                                                        *)
(*                                                                         *)
(* PROVENANCE NOTE (read before trusting this as "real" conformance):      *)
(* issue #55's own tracking comment deliberately deferred this file,       *)
(* because a trace sibling's whole point is checking a REAL captured      *)
(* NDJSON trace against the model, and no Rust `duckspout-replication`    *)
(* implementation exists yet to capture one from (ADR-0012 blocks that     *)
(* implementation, #51, until this issue and the P model, #132, both       *)
(* land) -- hand-crafting a synthetic fixture was called out as "useful    *)
(* only as a self-test of the refinement checker, not the genuine          *)
(* conformance value a trace sibling exists to provide".  This file and    *)
(* its specs/fixtures/replication-*.ndjson siblings are exactly that       *)
(* self-test: every fixture is hand-authored against this module and       *)
(* DuckSpoutCore directly, not captured from running code.  There is no    *)
(* replication-captured.ndjson (contrast ingest-captured.ndjson, backed by *)
(* duckspout-daemon's tests/trace_capture.rs re-capturing on every run) --  *)
(* inventing one with no real capture behind it would misrepresent         *)
(* provenance.  The live and real-backend conformance tiers                *)
(* (scripts/trace-conformance.mjs) stay Ingest-only for the same reason;   *)
(* only the generic fixtures tier picks this manifest up.  Retire this     *)
(* note once a real capture harness lands and a genuine -captured fixture  *)
(* replaces it as the provenance anchor.                                   *)
(*                                                                         *)
(* Events remain payload-free (D-6: {node, seq, event}); action parameters *)
(* are existentially matched exactly as IngestTrace.tla's MatchedStep      *)
(* does.  Two vocabulary changes from IngestTrace.tla, both because        *)
(* Replication.tla has now landed:                                        *)
(*                                                                         *)
(*   - TakeoverDrain is matched for real (IngestTrace.tla pinned it FALSE  *)
(*     "until Replication.tla lands", v0.2) -- this is that landing.       *)
(*   - CrashNode joins CloseWindow as an un-journaled environment          *)
(*     transition (3.7: "a crashed node cannot journal its own crash").    *)
(*     IngestTrace.tla's header left this as a forward reference ("only    *)
(*     CloseWindow is live until a crash-schedule fixture arms them") --   *)
(*     this is that crash-schedule fixture.  CrashWipe does NOT join it:   *)
(*     WipeBudget = 0 below, mirroring Replication.cfg's own justified     *)
(*     scope (the module's checked hazard is takeover-without-disk-loss;   *)
(*     disk death is orthogonal and stays out of both configs' reach), so  *)
(*     no recorded trace could ever journal a wipe-dependent step and no   *)
(*     EnvNext disjunct for it is needed.                                  *)
(*                                                                         *)
(* RecoverNode is not a separate case: docs/trace-mapping.md defines       *)
(* RecoverNode == FenceBoot with no distinct journaled name (matching      *)
(* DuckSpoutCore.tla's own `RecoverNode(n) == FenceBoot(n)`), so recovery  *)
(* from this trace's one CrashNode is a recorded "FenceBoot" entry, same   *)
(* as any other boot.                                                     *)
(*                                                                         *)
(* ClaimAdvertise and Heartbeat keep IngestTrace.tla's matched-but-dormant *)
(* treatment: both are provably unreachable in ANY configuration built on  *)
(* this module's Init (claims = InitClaims exactly at time zero, so        *)
(* ClaimAdvertise's own "<<n,p>> \in InitClaims /\ <<n,p>> \notin claims"  *)
(* guard can never hold -- TN-4's comment calls the action "quiescent      *)
(* until v0.2 restores dynamics", which TakeoverOn's arrival here does not *)
(* itself change) and MaxHb = 0 below (Heartbeat's own guard is           *)
(* `hb[n] < MaxHb`).  DegradedBoot is likewise matched-but-dormant: its    *)
(* guard needs inc[n] > 0 (a PRIOR FenceBoot while live), which a single   *)
(* MaxCrashes = 1 crash-then-recover cycle can never produce for the same  *)
(* reason Replication.cfg's own clean configuration never reaches it       *)
(* either (same MaxCrashes = 1, Crashable = {n1} scope) -- matching the    *)
(* implementation surface this refinement targets, per IngestTrace.tla's   *)
(* TN-25 "no-RF=1 rule" precedent: a refinement config matches the         *)
(* implementation it accepts traces from, not an aspirational superset.    *)
(* ReclaimSeal (v0.2, #177) has no case at all: docs/trace-mapping.md's    *)
(* frozen 27-variant vocabulary has no journaled name for it, so no        *)
(* recorded entry could ever request it regardless of ReclaimOn's value    *)
(* (set FALSE below for that reason -- KISS, no unexercised toggle).       *)
(*                                                                         *)
(* Scope: Nodes = {n1, n2}, RF = 2 -- unlike IngestTrace.tla (RF = 1, the   *)
(* v0.1 implementation's single-node-durable ack), THIS trace's ClientAck  *)
(* must show the real >= RF receipted-ack path (R-1), which is exactly     *)
(* why WatermarkHonesty was absent from IngestTrace.cfg's INVARIANT list   *)
(* ("returns to the trace tier with the v0.2 receipted traces", that       *)
(* comment reads) and is present in this one.  Requests = {q1, q2, q3},    *)
(* ALL originating at n1 with distinct dedup keys, q1/q2 in window 1 and   *)
(* q3 in window 2 -- exactly IngestTrace.tla's own q5/window-2 shape, and  *)
(* for the identical reason (its header): a fixture that deletes a         *)
(* trailing ClientAck must leave its request's WINDOW un-closed by         *)
(* anything the rest of the trace still needs, or the missing resolution   *)
(* blocks CloseWindow and the walk halts instead of reaching TraceComplete *)
(* -- q3's window 2 is never closed here, so deleting its ClientAck is a   *)
(* clean TraceComplete violation, not a cascading halt.  q1 and q2 sharing *)
(* window 1 (rather than one request per window) is what makes            *)
(* replication-doctored-missing-forward.ndjson representable at all: a     *)
(* genuine "PeerApply accepted a gap" doctoring (delete the earlier        *)
(* record's Forward, keep the later one's PeerApply, expect the refusal to *)
(* halt exactly there) turns out NOT representable at this payload-free    *)
(* granularity -- with two records already staged, existential matching    *)
(* always prefers whichever binding keeps the walk alive, so the sole      *)
(* remaining Forward-tagged entry gets silently reinterpreted as the       *)
(* EARLIER record's Forward instead of the deleted one, and the walk       *)
(* halts one hop later than intended: not at the reinterpreted PeerApply,  *)
(* but at the second record's ClientAck, which can never gather RF         *)
(* receipts under ANY interpretation once its own Forward is truly gone.   *)
(* replication-manifest.toml documents this discovered mechanism           *)
(* honestly (halt_at the ClientAck, not the PeerApply) rather than          *)
(* asserting the originally-intended one.  1 partition, MaxCrashes = 1,     *)
(* Crashable = {n1}: the smallest scope in which n1 dies after being fully  *)
(* acked and n2 takes over and drains from its own replicated copy --      *)
(* Replication.tla's own 5.6 narrative, replayed as a trace instead of     *)
(* exhaustively checked.                                                   *)
(***************************************************************************)
EXTENDS DuckSpoutCore, Sequences, Json, IOUtils, TLC

CONSTANTS n1, n2, q1, q2, q3, p1, d1, dkA, dkB, dkC, ckA

TraceWinOf      == (q1 :> 1) @@ (q2 :> 1) @@ (q3 :> 2)
TracePartOf     == [q \in Requests |-> p1]
TraceDKeyOf     == (q1 :> dkA) @@ (q2 :> dkB) @@ (q3 :> dkC)
TraceCKeyOf     == [q \in Requests |-> ckA]
TraceTombOf     == [q \in Requests |-> FALSE]
TraceKindOf     == [p \in Partitions |-> "event"]
TraceHome       == [d \in Datasets |-> p1]
TraceDsOf       == [q \in Requests |-> d1]
TraceAcceptorOf == [q \in Requests |-> {n1}]   \* all three requests
                                               \* originate at n1 -- the
                                               \* node whose death is this
                                               \* scope's story
TraceInitClaims == {<<n1, p1>>}   \* n1 pre-seeded as p1's owner (matches
                                  \* Replication.tla's ReplicationInitClaims)

\* The recorded run: one JSON object {node, seq, event} per line (D-6),
\* handed in by scripts/tla.mjs tv via the TRACE_PATH environment variable.
Trace == ndJsonDeserialize(IOEnv.TRACE_PATH)

\* Journaling node -> model node.
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
(* matches nothing and halts the walk.  TakeoverDrain is matched for real  *)
(* here (IngestTrace.tla pinned it FALSE "until Replication.tla lands" --  *)
(* this is that landing).  ClaimAdvertise, Heartbeat and DegradedBoot are  *)
(* matched but provably dormant in this scope (module header).             *)
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
    [] t.event = "TakeoverDrain" -> \E p \in Partitions : TakeoverDrain(NodeOf(t.node), p)
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

\* Un-journaled environment transitions (module header): CloseWindow (3.1's
\* un-journaled window-close, TN-1) plus CrashNode (3.7: a crashed node
\* cannot journal its own crash).  Both finitely enabled -- each window
\* closes at most once, and crashBudget = MaxCrashes = 1 bounds CrashNode
\* to a single occurrence -- so BFS terminates.
EnvNext ==
  /\ cursor <= Len(Trace)
  /\ \/ \E p \in Partitions, w \in Windows : CloseWindow(p, w)
     \/ \E n \in Nodes : CrashNode(n)
  /\ UNCHANGED cursor

TraceDone == cursor = Len(Trace) + 1

TraceInit == Init /\ cursor = 1
TraceNext == ReadNext \/ EnvNext
TraceSpec == TraceInit /\ [][TraceNext]_traceVars

(***************************************************************************)
(* Obligation 2: every step that must have happened was recorded.  Same    *)
(* three clauses as IngestTrace.tla -- Replication.tla creates no          *)
(* additional standing obligation §3.7 requires this invariant to police   *)
(* (Forward/PeerApply/Receipt leave no dangling record the way an          *)
(* unresolved commit or an unresolved request does).                       *)
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
