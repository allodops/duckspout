//! Smoke test: the loadgen's real client and journal against a real
//! `duckspout-accept` OTLP/gRPC service on a loopback listener — the same
//! wire protocol a fleet accept endpoint serves (§8.4), without the full
//! `duckspout-daemon` composition (`DuckDB`, object store) that a real fleet
//! node also carries. Issue #202's own scope note: a real multi-node fleet
//! does not exist yet (#201), so this is the honest ceiling for
//! "does this actually work end to end" in this PR — real bytes, real
//! tonic client and server, real gRPC deadline race; only the storage
//! backend behind `StageCommitter` is a double (as `duckspout-accept`'s own
//! tests use, since accept ↔ staging is itself a forbidden edge, §10.1).
//!
//! The double is test-local by necessity, not preference: the invariant
//! engine audits all direct dependency edges, dev-deps included, and pulling
//! `duckspout-staging` in just to exercise `duckspout-accept` from here would
//! recreate the same coupling `duckspout-accept`'s own tests avoid.
//!
//! # ACPR finding HIGH-1 — the `ClientTimeout` boundary
//!
//! Two tests below exist specifically to pin the corrected boundary
//! (`duckspout_loadgen::outcome` module docs):
//! `a_hung_but_alive_acceptor_is_ambiguous_not_client_timeout` is the
//! **pre-fix flagship test, corrected** — it used to assert `ClientTimeout`
//! for a server that hangs forever in `stage_commit` while staying alive
//! and connected, which `specs/DuckSpoutCore.tla`'s `ClientTimeout(q)`
//! action forbids (the request never leaves that node's `inflight` set, so
//! the action's `~\E n: alive[n] /\ q \in inflight[n]` guard never opens).
//! `a_dead_acceptor_produces_a_confirmed_client_timeout` is the case that
//! *should* produce it: a target with no live acceptor behind it at all,
//! which trivially satisfies the same guard (there is no `n` for which
//! `alive[n] /\ q \in inflight[n]` could ever hold) — the model's
//! `CrashNode`/`CrashWipe` shape, observed the only way a real client can:
//! the RPC settling with a transport-level failure instead of an app-level
//! answer.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use duckspout_accept::server::{AdmissionConfig, OtlpLogsService};
use duckspout_loadgen::client::{connect, request_id, send_and_journal};
use duckspout_loadgen::journal::LoadgenJournal;
use duckspout_loadgen::outcome::RequestResolution;
use duckspout_types::{
    BoxFuture, DecodedBatch, NodeId, OriginSeqRange, PartitionId, StageCommitter, StageError,
    StageOutcome, StagedCoverage,
};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

enum Script {
    Commit,
    /// Never resolves within the test: the accepting node stays alive and
    /// connected, holding the request in `inflight` forever — the state
    /// `ClientTimeout` must NOT fire in (module docs, HIGH-1).
    Hang(Arc<tokio::sync::Notify>),
    Refuse(u64),
}

struct ScriptedStager {
    script: Script,
    staged: Mutex<Vec<DecodedBatch>>,
}

impl ScriptedStager {
    fn new(script: Script) -> Arc<Self> {
        Arc::new(Self {
            script,
            staged: Mutex::new(Vec::new()),
        })
    }
}

impl StageCommitter for ScriptedStager {
    fn stage_commit(&self, batch: DecodedBatch) -> BoxFuture<'_, Result<StageOutcome, StageError>> {
        Box::pin(async move {
            match &self.script {
                Script::Commit => {
                    self.staged.lock().unwrap().push(batch.clone());
                    Ok(StageOutcome::Committed(vec![StagedCoverage {
                        partition: PartitionId::from_tenant_shard(&batch.tenant, 0),
                        range: OriginSeqRange {
                            origin: NodeId::new("test-node/1"),
                            first_seq: 1,
                            last_seq: 1,
                        },
                    }]))
                }
                Script::Hang(gate) => {
                    gate.notified().await;
                    unreachable!("the test never notifies this gate")
                }
                Script::Refuse(retry_after_ms) => Err(StageError::RefusingIngest {
                    retry_after_ms: *retry_after_ms,
                }),
            }
        })
    }
}

fn open_admission() -> AdmissionConfig {
    AdmissionConfig {
        max_payload_bytes: 4 * 1024 * 1024,
        max_inflight_bytes: u64::MAX,
    }
}

/// Serves `stager` on an ephemeral loopback port; returns its `http://` URL.
async fn serve(stager: Arc<ScriptedStager>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = OtlpLogsService::new(stager, open_admission()).into_server();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    format!("http://{addr}")
}

fn journal_lines(journal: LoadgenJournal<Vec<u8>>) -> Vec<serde_json::Value> {
    String::from_utf8(journal.into_inner())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_batch_is_acked_and_journaled_with_identity() {
    let url = serve(ScriptedStager::new(Script::Commit)).await;
    let mut client = connect(&url).await.unwrap();
    let journal = LoadgenJournal::new(NodeId::new("loadgen-smoke"), Vec::new());
    let id = request_id(&NodeId::new("loadgen-smoke"), 0, 0);

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        id.clone(),
        5,
        20,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(resolution, RequestResolution::Acked);

    let lines = journal_lines(journal);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["event"], "ClientAck");
    assert_eq!(lines[0]["seq"], 0);
    assert_eq!(lines[0]["request_id"], id);
    assert_eq!(lines[0]["tenant"], "tenant-smoke");
    assert_eq!(lines[0]["record_count"], 5);
    // ACPR finding MEDIUM-HIGH-3: the index range must ride along so a
    // future judge (#205) never has to reverse-engineer it from
    // `main.rs`'s `sent * batch_size` arithmetic.
    assert_eq!(lines[0]["first_index"], 20);
}

/// The pre-fix flagship test, corrected (ACPR finding HIGH-1, module docs):
/// a hung-but-alive acceptor never leaves `inflight`, so the model forbids
/// `ClientTimeout` here. The old assertion (`ClientTimeout`) enshrined
/// exactly the state the spec's own `ClientTimeout(q)` action guards
/// against. Nothing is journaled to the frozen §3.3 vocabulary for this —
/// see `duckspout_loadgen::outcome`'s `Ambiguous` docs.
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_but_alive_acceptor_is_ambiguous_not_client_timeout() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let url = serve(ScriptedStager::new(Script::Hang(gate))).await;
    let mut client = connect(&url).await.unwrap();
    let journal = LoadgenJournal::new(NodeId::new("loadgen-smoke"), Vec::new());

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        request_id(&NodeId::new("loadgen-smoke"), 0, 0),
        1,
        0,
        Duration::from_millis(100),
    )
    .await;
    assert_eq!(resolution, RequestResolution::Ambiguous);
    assert!(journal_lines(journal).is_empty());
}

/// The case that *should* produce `ClientTimeout` (module docs, HIGH-1): no
/// live acceptor behind the target at all — the same "no live node ever
/// held this request" shape `CrashNode`/`CrashWipe` leave behind, observed
/// the only way a real client can, a transport-level failure rather than a
/// bare local deadline. `connect_lazy` defers the actual TCP attempt to the
/// first RPC, so the failure surfaces exactly inside `send_and_journal`'s
/// real ack/deadline race, not before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_acceptor_produces_a_confirmed_client_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // free the port; nothing will ever answer on it

    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let mut client = LogsServiceClient::new(endpoint.connect_lazy());
    let journal = LoadgenJournal::new(NodeId::new("loadgen-smoke"), Vec::new());

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        request_id(&NodeId::new("loadgen-smoke"), 0, 0),
        1,
        0,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(resolution, RequestResolution::TimedOut);

    let lines = journal_lines(journal);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["event"], "ClientTimeout");
}

/// An explicit, prompt rejection is neither an ack nor a confirmed timeout
/// (§3.3 has no client-journaled action for it, `outcome`'s module docs) —
/// the journal must stay empty. Would catch conflating "the server said no"
/// with "the connection died," which would fabricate a `ClientTimeout` the
/// judge would then wrongly treat as evidence the acceptor vanished.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_refusal_is_journaled_as_nothing() {
    let url = serve(ScriptedStager::new(Script::Refuse(1_000))).await;
    let mut client = connect(&url).await.unwrap();
    let journal = LoadgenJournal::new(NodeId::new("loadgen-smoke"), Vec::new());

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        request_id(&NodeId::new("loadgen-smoke"), 0, 0),
        1,
        0,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(resolution, RequestResolution::Rejected);
    assert!(journal_lines(journal).is_empty());
}
