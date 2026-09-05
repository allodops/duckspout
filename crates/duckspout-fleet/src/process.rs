//! Spawns and supervises one real `duckspout-daemon` child process per
//! [`NodeSpec`] (§8.4, issue #201): launch, poll `/status` until `ready`,
//! and a best-effort graceful shutdown mirroring the daemon's own §9.1.2
//! SIGTERM choreography.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::{Child, Command};

use crate::topology::{NodeSpec, node_hostname_env_key};

/// A running (or exited) fleet member: the child handle plus the identity
/// needed to poll and report on it.
pub struct RunningNode {
    pub spec: NodeSpec,
    child: Child,
}

/// Launches `node`'s `duckspout-daemon` process: `--config`, `--trace-out`
/// (the real §3.3 NDJSON journal, issue #201's journaling-discipline
/// requirement), and `--status-listen` (this node's own status port — the
/// production default collides across co-located nodes, `main.rs`'s own
/// doc comment on `--status-listen`), with
/// [`DUCKSPOUT_NODE_HOSTNAME`](node_hostname_env_key) set so
/// `system::detect_node_id` reports this node's distinct identity rather
/// than the shared host kernel hostname (`system.rs`'s own doc comment).
///
/// # Errors
///
/// If the child process cannot be spawned (bad `daemon_bin` path, most
/// likely) or its log files cannot be created.
pub fn spawn_node(daemon_bin: &std::path::Path, node: &NodeSpec) -> anyhow::Result<RunningNode> {
    let stdout = std::fs::File::create(&node.stdout_path)
        .with_context(|| format!("creating {}", node.stdout_path.display()))?;
    let stderr = std::fs::File::create(&node.stderr_path)
        .with_context(|| format!("creating {}", node.stderr_path.display()))?;

    let child = Command::new(daemon_bin)
        .arg("--config")
        .arg(&node.config_path)
        .arg("--trace-out")
        .arg(&node.journal_path)
        .arg("--status-listen")
        .arg(node.status_port.to_string())
        .env(node_hostname_env_key(), &node.name)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawning {} for node {}", daemon_bin.display(), node.name))?;

    Ok(RunningNode {
        spec: node.clone(),
        child,
    })
}

/// Polls `node`'s `/status` endpoint (§9.3.2) every 250 ms until it reports
/// `ready: true`, or fails as soon as the child process exits early
/// (surfacing the tail of its own stderr log — waiting out the full
/// timeout on a process that already died would only obscure the real
/// error).
///
/// # Errors
///
/// The child's own exit status and stderr tail if it exited before
/// becoming ready; a plain timeout message otherwise.
pub async fn wait_until_ready(node: &mut RunningNode, timeout: Duration) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(status) = node.child.try_wait()? {
            let tail = stderr_tail(&node.spec.stderr_path);
            bail!(
                "node {} exited early with {status} before becoming ready; stderr tail:\n{tail}",
                node.spec.name
            );
        }
        if let Ok(snapshot) = fetch_status(node.spec.status_addr()).await
            && snapshot
                .get("ready")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let tail = stderr_tail(&node.spec.stderr_path);
            bail!(
                "node {} did not become ready within {timeout:?}; stderr tail:\n{tail}",
                node.spec.name
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Fetches and parses `/status`'s JSON body over a plain, best-effort HTTP
/// GET — the same wire shape `duckspout-daemon`'s own
/// `status::serve`/its unit test use, so no HTTP client dependency is
/// needed for a one-route, no-keep-alive disclosure endpoint.
///
/// # Errors
///
/// Any connect/I/O/JSON error — callers treat these as "not ready yet."
pub async fn fetch_status(addr: std::net::SocketAddr) -> anyhow::Result<serde_json::Value> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream
        .write_all(b"GET /status HTTP/1.1\r\nHost: fleet\r\nConnection: close\r\n\r\n")
        .await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .context("malformed /status response: no header/body separator")?;
    Ok(serde_json::from_str(body)?)
}

/// Sends SIGTERM (via the `kill` utility — no new dependency for a signal
/// std does not expose portably) so the daemon runs its own §9.1.2 shallow
/// drain, waits up to `grace` for it to exit on its own, then force-kills.
/// Best-effort throughout: a fleet run's own teardown should not itself
/// hang or panic the caller.
pub async fn shutdown(node: &mut RunningNode, grace: Duration) {
    if let Some(pid) = node.child.id() {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await;
    }
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        match node.child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = node.child.start_kill();
            let _ = node.child.wait().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The last few KiB of `path`, for a stderr-tail diagnostic — best-effort:
/// an unreadable or missing file yields a placeholder, never an error of
/// its own (this only ever augments an already-failing report).
fn stderr_tail(path: &std::path::Path) -> String {
    const TAIL_BYTES: usize = 4096;
    match std::fs::read_to_string(path) {
        Ok(contents) if contents.len() > TAIL_BYTES => {
            contents[contents.len() - TAIL_BYTES..].to_owned()
        }
        Ok(contents) => contents,
        Err(e) => format!("(could not read {}: {e})", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stderr_tail_reports_a_placeholder_for_a_missing_file() {
        let missing = std::env::temp_dir().join("duckspout-fleet-process-test-missing-file");
        let tail = stderr_tail(&missing);
        assert!(
            tail.starts_with("(could not read"),
            "expected a could-not-read placeholder, got {tail:?}"
        );
    }

    #[test]
    fn stderr_tail_returns_the_whole_file_when_short() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-process-test-{}-short",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stderr.log");
        std::fs::write(&path, "boot failed: connection refused").unwrap();
        assert_eq!(stderr_tail(&path), "boot failed: connection refused");
    }

    #[test]
    fn stderr_tail_truncates_to_the_last_4096_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-process-test-{}-long",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stderr.log");
        // 5000 'a's followed by a distinct tail marker so truncation is
        // unambiguous to detect.
        let contents = format!("{}TAIL_MARKER", "a".repeat(5000));
        std::fs::write(&path, &contents).unwrap();
        let tail = stderr_tail(&path);
        assert_eq!(tail.len(), 4096);
        assert!(tail.ends_with("TAIL_MARKER"));
    }

    /// `fetch_status`'s own doc comment claims it speaks the same
    /// no-keep-alive HTTP/1.1 wire shape `duckspout-daemon`'s `/status`
    /// serves, so no HTTP client dependency is needed — this proves that
    /// claim against a plain hand-rolled listener, not the real daemon.
    #[tokio::test]
    async fn fetch_status_parses_a_real_http_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n\
                      {\"ready\":true,\"watermarks\":[]}",
                )
                .await
                .unwrap();
        });

        let status = fetch_status(addr).await.unwrap();
        server.await.unwrap();

        assert_eq!(
            status.get("ready").and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    /// A response with no `\r\n\r\n` header/body separator is exactly the
    /// "not ready yet" shape `wait_until_ready` must tolerate, not a panic.
    #[tokio::test]
    async fn fetch_status_errors_on_a_malformed_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream.write_all(b"not even close to HTTP").await.unwrap();
        });

        let result = fetch_status(addr).await;
        server.await.unwrap();
        assert!(result.is_err());
    }
}
