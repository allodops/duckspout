//! The disclosed node-status endpoint (§9.3, R-9): `NodeId`, the closed
//! [`OverloadStatus`](duckspout_types::OverloadStatus)/rung, watermark per
//! partition, and `drain_stalled` —
//! the same [`NodeStatus`] vocabulary docs/operations.md §9.3.2 mandates
//! "identically on the health endpoint, the metrics, and the registry" (v0.1
//! ships the first of those three transports; the metrics exposition and
//! registry mirror are the v0.3 deploy-manifest work, issue #61).
//!
//! v0.1's disclosure is deliberately one endpoint, not the full
//! `/healthz` `/readyz` `/metrics` surface: every response carries the whole
//! [`StatusSnapshot`] (Keep Rule R-9's discipline — a catalog outage is a
//! disclosed pause, never silence), and this crate never lets a read
//! contend with the data path (§9.3.2: "in-process atomics only") — the
//! snapshot is produced from already-computed state, never a fresh `DuckDB`
//! query or catalog round-trip.

use std::future::Future;
use std::io;
use std::sync::Arc;

use duckspout_types::{NodeId, NodeStatus, WatermarkRow};
use serde::Serialize;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

/// The complete disclosed status (§9.3.2, §9.3.3's "one vocabulary, three
/// transports" — this is transport #1, HTTP/JSON).
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    /// This node's identity (§5).
    pub node_id: NodeId,
    /// Whether the node has completed boot and is accepting ingest (§9.1.2's
    /// SIGTERM choreography flips this to `false` first).
    pub ready: bool,
    /// The overload rung plus the replication flag (§4.5, §9.3.2).
    pub status: NodeStatus,
    /// Whether the drain is stalled (a catalog outage, most likely) —
    /// already folded into `status.overload` when it drove the rung; carried
    /// again here bare because "the drain is stalled" and "the drain is
    /// healthy but ingest is simply heavy" must stay distinguishable even
    /// off the ladder (R-9: never silence).
    pub drain_stalled: bool,
    /// Per-partition `complete_through_ms`, sorted by partition (§7.3).
    pub watermarks: Vec<WatermarkRow>,
}

impl StatusSnapshot {
    /// Renders the snapshot as one JSON object.
    ///
    /// # Errors
    ///
    /// Any [`serde_json`] serialization error (infallible in practice — every
    /// field here is plain data).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Serves `snapshot` as `GET /status` (any request, any path — v0.1's one
/// route, module docs) on `listener` until `shutdown` resolves. Each
/// connection gets one best-effort response; there is no keep-alive (a
/// disclosure endpoint is polled occasionally, not hammered).
pub async fn serve(
    listener: TcpListener,
    snapshot: Arc<dyn Fn() -> StatusSnapshot + Send + Sync>,
    shutdown: impl Future<Output = ()>,
) {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let snapshot = Arc::clone(&snapshot);
                tokio::spawn(async move {
                    let _ = respond(stream, &snapshot()).await;
                });
            }
            () = &mut shutdown => return,
        }
    }
}

async fn respond(mut stream: TcpStream, snapshot: &StatusSnapshot) -> io::Result<()> {
    // Best-effort request read: v0.1 serves the same body regardless of
    // method or path, so the request line and headers are drained and
    // discarded, never parsed.
    let mut buf = [0_u8; 512];
    let _ = stream.read(&mut buf).await;
    let body = snapshot
        .to_json()
        .unwrap_or_else(|e| format!(r#"{{"error":"status encoding: {e}"}}"#));
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use duckspout_types::OverloadStatus;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn serve_answers_a_plain_get_with_the_snapshot_json() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let snapshot = StatusSnapshot {
            node_id: NodeId::new("n/1"),
            ready: true,
            status: NodeStatus {
                overload: OverloadStatus::Normal,
                replication_degraded: false,
            },
            drain_stalled: false,
            watermarks: vec![],
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(listener, Arc::new(move || snapshot.clone()), async {
            let _ = shutdown_rx.await;
        }));

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(parsed["node_id"], "n/1");
        assert_eq!(parsed["ready"], true);
        assert_eq!(parsed["status"]["overload"], "normal");

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }
}
