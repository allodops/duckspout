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

enum Script {
    Commit,
    /// Never resolves within the test — used to force a `ClientTimeout`.
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
    let id = request_id(&NodeId::new("loadgen-smoke"), 0);

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        id.clone(),
        5,
        0,
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
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_with_no_ack_before_the_deadline_is_journaled_as_client_timeout() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let url = serve(ScriptedStager::new(Script::Hang(gate))).await;
    let mut client = connect(&url).await.unwrap();
    let journal = LoadgenJournal::new(NodeId::new("loadgen-smoke"), Vec::new());

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        request_id(&NodeId::new("loadgen-smoke"), 0),
        1,
        0,
        Duration::from_millis(100),
    )
    .await;
    assert_eq!(resolution, RequestResolution::TimedOut);

    let lines = journal_lines(journal);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["event"], "ClientTimeout");
}

/// An explicit, prompt rejection is neither an ack nor a timeout (§3.3 has
/// no client-journaled action for it, `outcome`'s module docs) — the journal
/// must stay empty. Would catch conflating "the server said no" with "the
/// server never answered," which would fabricate a `ClientTimeout` the
/// judge would then wrongly treat as client-observed evidence of loss.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_refusal_is_journaled_as_nothing() {
    let url = serve(ScriptedStager::new(Script::Refuse(1_000))).await;
    let mut client = connect(&url).await.unwrap();
    let journal = LoadgenJournal::new(NodeId::new("loadgen-smoke"), Vec::new());

    let resolution = send_and_journal(
        &mut client,
        &journal,
        "tenant-smoke",
        request_id(&NodeId::new("loadgen-smoke"), 0),
        1,
        0,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(resolution, RequestResolution::Failed);
    assert!(journal_lines(journal).is_empty());
}
