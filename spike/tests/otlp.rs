//! End-to-end OTLP accept-path test (§4.1, issue #24): a real gRPC client
//! sends an ExportLogsServiceRequest to the tonic server, the ack comes back
//! only after the batch committed, and the hot table then holds exactly the
//! flattened rows.

use std::sync::{Arc, Mutex};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use spike::otlp::{HotWriter, OtlpLogsService, synthetic_request};

#[tokio::test]
async fn otlp_export_acked_then_rows_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hot.db");
    let writer = Arc::new(Mutex::new(HotWriter::open(&db, "hot_w0").unwrap()));

    // In-process server on an ephemeral loopback port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = OtlpLogsService::new(Arc::clone(&writer)).into_server();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    // Send a batch; the ack must be full-batch (no partial_success — §4.1).
    let resp = client
        .export(synthetic_request(37))
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp.partial_success.is_none(),
        "unexpected partial_success: {resp:?}"
    );

    // Ack received ⇒ the rows are committed and queryable, with dense seqs
    // and the flattened payload intact.
    let w = writer.lock().unwrap();
    assert_eq!(w.count().unwrap(), 37, "acked batch not fully present");
    let max_seq: i64 = w
        .core()
        .conn_query_row("SELECT max(seq) FROM hot_w0")
        .unwrap();
    let distinct: i64 = w
        .core()
        .conn_query_row("SELECT count(DISTINCT seq) FROM hot_w0")
        .unwrap();
    assert_eq!(max_seq, 36, "seq not dense from 0");
    assert_eq!(distinct, 37, "seq collision");
    let body: String = w
        .core()
        .conn_query_row("SELECT body FROM hot_w0 WHERE seq = 5")
        .unwrap();
    assert_eq!(body, "synthetic otlp log line 5");
    let attrs: String = w
        .core()
        .conn_query_row("SELECT attrs FROM hot_w0 WHERE seq = 5")
        .unwrap();
    let attrs: serde_json::Value = serde_json::from_str(&attrs).unwrap();
    assert_eq!(attrs["resource"]["service.name"], "spike-bench");
    assert_eq!(attrs["attrs"]["i"], 5);
    assert_eq!(attrs["trace_id"], "ab".repeat(16));
    drop(w);

    // A second batch appends after the first (seq resumes, no clobber).
    let resp = client
        .export(synthetic_request(3))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.partial_success.is_none());
    assert_eq!(writer.lock().unwrap().count().unwrap(), 40);
}
