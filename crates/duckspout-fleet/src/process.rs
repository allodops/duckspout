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
/// `fault_drain_commit_delay_ms`, when `Some`, is passed through as
/// `--fault-drain-commit-delay-ms` (§8.4, issue #203): the fault-only seam
/// that widens this node's real `PutPart`→`LakeCommit` window
/// (`duckspout_daemon::fault`'s own module docs) so `crate::fault`'s
/// node-kill injector can land a real `SIGKILL` inside it deterministically.
/// `None` omits the flag entirely, matching the daemon's own `0`/disabled
/// default.
///
/// # Errors
///
/// If the child process cannot be spawned (bad `daemon_bin` path, most
/// likely) or its log files cannot be created.
pub fn spawn_node(
    daemon_bin: &std::path::Path,
    node: &NodeSpec,
    fault_drain_commit_delay_ms: Option<u64>,
) -> anyhow::Result<RunningNode> {
    let stdout = std::fs::File::create(&node.stdout_path)
        .with_context(|| format!("creating {}", node.stdout_path.display()))?;
    let stderr = std::fs::File::create(&node.stderr_path)
        .with_context(|| format!("creating {}", node.stderr_path.display()))?;

    let mut command = Command::new(daemon_bin);
    command
        .arg("--config")
        .arg(&node.config_path)
        .arg("--trace-out")
        .arg(&node.journal_path)
        .arg("--status-listen")
        .arg(node.status_port.to_string());
    if let Some(delay_ms) = fault_drain_commit_delay_ms {
        command
            .arg("--fault-drain-commit-delay-ms")
            .arg(delay_ms.to_string());
    }
    let child = command
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

/// This node's OS pid, when the child has not yet been reaped
/// (`crate::fault`'s injectors need it to send fault signals directly).
#[must_use]
pub fn pid(node: &RunningNode) -> Option<u32> {
    node.child.id()
}

/// Sends `signal` (e.g. `"-STOP"`, `"-CONT"`, `"-KILL"`) to `node`'s process
/// via the `kill` utility — the same no-new-dependency convention
/// [`shutdown`]'s own `-TERM` already uses. Fault-injection-only signals
/// (§8.4, issue #203): production teardown stays on [`shutdown`]'s own
/// `-TERM`/force-kill choreography; this is `crate::fault`'s primitive for
/// real node kills and real SIGSTOP/SIGCONT pauses.
///
/// # Errors
///
/// If the node has already been reaped (no pid), the `kill` utility itself
/// cannot be spawned, or it exits non-zero (e.g. the pid is already gone).
pub async fn send_signal(node: &RunningNode, signal: &str) -> anyhow::Result<()> {
    let Some(pid) = node.child.id() else {
        bail!(
            "node {} has no pid (already reaped) — cannot send {signal}",
            node.spec.name
        );
    };
    let status = Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .await
        .with_context(|| {
            format!(
                "sending kill {signal} to node {} (pid {pid})",
                node.spec.name
            )
        })?;
    if !status.success() {
        bail!(
            "kill {signal} {pid} for node {} exited with {status}",
            node.spec.name
        );
    }
    Ok(())
}

/// Whether the OS currently reports `pid` as stopped (`/proc/<pid>/stat`'s
/// process-state field is `T`, "stopped (on a signal)" — `proc(5)`) —
/// Linux-only best-effort confirmation that a `SIGSTOP` actually landed,
/// not merely that the signal was sent. `crate::fault`'s SIGSTOP-pause
/// injector uses this to journal the fault window's `Started` phase only
/// once the pause is observably real (§8.4: "each window journaled with
/// start/end").
///
/// # Errors
///
/// If `/proc/<pid>/stat` cannot be read (the process already exited, or
/// this is not Linux).
pub fn is_stopped(pid: u32) -> anyhow::Result<bool> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .with_context(|| format!("reading /proc/{pid}/stat"))?;
    // The second field is `(comm)`, which may itself contain spaces or
    // parentheses — split on the LAST ')' rather than whitespace, exactly
    // as `proc(5)`'s own grammar requires this field to be parsed.
    let after_comm = stat
        .rsplit_once(')')
        .map_or(stat.as_str(), |(_, rest)| rest);
    let state = after_comm.split_whitespace().next();
    Ok(matches!(state, Some("T" | "t")))
}

/// Polls `node`'s process every 100 ms until [`Child::try_wait`] reports it
/// has exited, or `timeout` elapses. `crate::fault`'s node-kill injector
/// uses this to confirm a `SIGKILL` actually landed before journaling the
/// fault window's `Ended` phase (§8.4).
pub async fn wait_exited(node: &mut RunningNode, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match node.child.try_wait() {
            Ok(Some(_)) | Err(_) => return true,
            Ok(None) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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

    /// A minimal [`NodeSpec`] for the fault-signal tests below — its paths
    /// are never actually read/written by `sleep`, only carried for
    /// [`RunningNode::spec`]'s sake.
    fn dummy_spec() -> NodeSpec {
        let dir = std::env::temp_dir().join(format!(
            "duckspout-fleet-process-fault-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        NodeSpec {
            index: 0,
            name: "fault-test-node".to_owned(),
            otlp_port: 0,
            flight_port: 0,
            peer_port: 0,
            status_port: 0,
            data_dir: dir.join("data"),
            config_path: dir.join("config.toml"),
            journal_path: dir.join("journal.ndjson"),
            stdout_path: dir.join("stdout.log"),
            stderr_path: dir.join("stderr.log"),
        }
    }

    /// Spawns a REAL long-lived process (`sleep 30`) wrapped as a
    /// [`RunningNode`] — a stand-in "node" for exercising
    /// `send_signal`/`is_stopped`/`wait_exited` against real OS signal
    /// semantics without needing a built `duckspout-daemon` binary. `sleep`
    /// is chosen because it is present on every Linux CI runner this repo
    /// targets and does nothing that could race with signal delivery.
    fn spawn_sleep(seconds: u64) -> RunningNode {
        let child = Command::new("sleep")
            .arg(seconds.to_string())
            .kill_on_drop(true)
            .spawn()
            .expect("spawning `sleep` (must be on PATH)");
        RunningNode {
            spec: dummy_spec(),
            child,
        }
    }

    /// Polls `check` every 20 ms until it returns `true` or `timeout`
    /// elapses — a small local helper so the tests below do not depend on
    /// signal delivery being instantaneous.
    async fn poll_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if check() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn pid_returns_some_for_a_freshly_spawned_process() {
        let node = spawn_sleep(30);
        assert!(pid(&node).is_some());
        send_signal(&node, "-KILL").await.unwrap();
    }

    /// The exact real-OS round trip `crate::fault::run_sigstop_pause` relies
    /// on: `SIGSTOP` makes `is_stopped` observe `true`, `SIGCONT` makes it
    /// observe `false` again — against a REAL process, not a double. Would
    /// catch `is_stopped`'s `/proc/<pid>/stat` field-parsing being wrong in
    /// either direction (e.g. off-by-one field after the `)`, or comparing
    /// against the wrong state letter).
    #[tokio::test]
    async fn send_signal_stop_then_cont_round_trips_through_is_stopped() {
        let node = spawn_sleep(30);
        let pid = pid(&node).unwrap();
        assert!(
            !is_stopped(pid).unwrap(),
            "a freshly spawned sleep must not already be stopped"
        );

        send_signal(&node, "-STOP").await.unwrap();
        assert!(
            poll_until(Duration::from_secs(2), || is_stopped(pid).unwrap_or(false)).await,
            "is_stopped must observe true after a real SIGSTOP"
        );

        send_signal(&node, "-CONT").await.unwrap();
        assert!(
            poll_until(Duration::from_secs(2), || !is_stopped(pid).unwrap_or(true)).await,
            "is_stopped must observe false again after a real SIGCONT"
        );

        send_signal(&node, "-KILL").await.unwrap();
    }

    /// `wait_exited` confirms a real `SIGKILL` actually reaped the process,
    /// within its timeout — `crate::fault::run_node_kill`'s own
    /// confirmation step.
    #[tokio::test]
    async fn wait_exited_confirms_a_killed_process() {
        let mut node = spawn_sleep(30);
        send_signal(&node, "-KILL").await.unwrap();
        let exited = wait_exited(&mut node, Duration::from_secs(5)).await;
        assert!(exited, "wait_exited must confirm a real SIGKILL");
    }

    /// `wait_exited` returns `false`, not hangs, when the process is merely
    /// paused (`SIGSTOP`, not dead) and the timeout is short — a
    /// stopped-but-alive process must never be confused with an exited one.
    #[tokio::test]
    async fn wait_exited_returns_false_for_a_merely_stopped_process() {
        let mut node = spawn_sleep(30);
        send_signal(&node, "-STOP").await.unwrap();
        let exited = wait_exited(&mut node, Duration::from_millis(200)).await;
        assert!(
            !exited,
            "a stopped-but-alive process must not read as exited"
        );
        send_signal(&node, "-KILL").await.unwrap();
    }

    /// `send_signal` fails closed, rather than panicking or silently
    /// no-op'ing, once the child has been reaped
    /// (`tokio::process::Child::id`'s own documented contract: `None` once
    /// polled to completion) — sending a real signal at a stale pid the
    /// kernel may have since recycled for something else would be
    /// dangerous, not merely wrong.
    #[tokio::test]
    async fn send_signal_fails_once_the_process_has_been_reaped() {
        let mut node = spawn_sleep(1);
        send_signal(&node, "-KILL").await.unwrap();
        assert!(wait_exited(&mut node, Duration::from_secs(5)).await);
        assert!(
            pid(&node).is_none(),
            "tokio::process::Child::id() must be None once reaped"
        );
        assert!(
            send_signal(&node, "-TERM").await.is_err(),
            "send_signal must fail closed with no pid to target"
        );
    }
}
