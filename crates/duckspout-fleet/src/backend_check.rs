//! Real-backend reachability (§8.2's "absent endpoint = red, never skip"
//! posture, applied here at the fleet-runner level, issue #201): before
//! provisioning a single node, confirm Postgres and the S3-compatible
//! endpoint actually answer a TCP connection. A fleet that silently booted
//! nodes against unreachable backends would fail deep inside a child
//! process's own stderr, indistinguishable from a real code defect —
//! failing here, once, with a clear pointer at `deploy/compose/`, is cheaper
//! to diagnose and matches this repo's stated stance that an absent
//! real-backend endpoint is a red result, not a quietly skipped one.
//!
//! DSN parsing itself lives in [`crate::dsn`] — one home shared with #204's
//! catalog fault links, which must rewrite the same addresses this module
//! probes.

use std::time::Duration;

use anyhow::{Context, bail};

/// Parses an S3-endpoint `host:port` pair (the same `ENDPOINT` convention
/// `duckspout-lake-ducklake::S3Access::endpoint` documents — no scheme).
///
/// # Errors
///
/// If `endpoint` has no `:port` suffix.
pub fn s3_host_port(endpoint: &str) -> anyhow::Result<(String, u16)> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .with_context(|| format!("S3 endpoint {endpoint:?} is missing a :port suffix"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("S3 endpoint {endpoint:?} has a non-numeric port"))?;
    Ok((host.to_owned(), port))
}

/// Probes `host:port` with a bare TCP connect, failing closed (never
/// silently skipping) with a message pointing at `deploy/compose/` when
/// nothing answers within `timeout`.
///
/// # Errors
///
/// If the connection is refused, times out, or otherwise fails.
pub async fn check_reachable(
    label: &str,
    host: &str,
    port: u16,
    timeout: Duration,
) -> anyhow::Result<()> {
    let attempt = tokio::time::timeout(timeout, tokio::net::TcpStream::connect((host, port)));
    match attempt.await {
        Ok(Ok(_stream)) => Ok(()),
        Ok(Err(source)) => bail!(
            "{label} unreachable at {host}:{port}: {source} — bring up \
             deploy/compose/compose.yaml first (`docker compose -f \
             deploy/compose/compose.yaml up -d`), or pass --skip-backend-check \
             to bypass this probe"
        ),
        Err(_) => bail!(
            "{label} unreachable at {host}:{port}: no response within {timeout:?} — bring up \
             deploy/compose/compose.yaml first (`docker compose -f \
             deploy/compose/compose.yaml up -d`), or pass --skip-backend-check \
             to bypass this probe"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_endpoint_host_port_parses_the_compose_endpoint() {
        let (host, port) = s3_host_port("127.0.0.1:9000").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 9000);
    }

    #[tokio::test]
    async fn check_reachable_fails_closed_against_a_closed_port() {
        // Port 0 dialed directly never accepts a real connection; a
        // deliberately-unlikely-to-be-listening high port is more portable
        // than reserving one, so probe an address nothing binds to.
        let result = check_reachable(
            "test",
            "127.0.0.1",
            1, // well-known, privileged, never listening in this sandbox
            Duration::from_millis(200),
        )
        .await;
        assert!(result.is_err(), "expected an unreachable-port failure");
    }

    /// The success arm: a real listener answers the bare TCP connect —
    /// the case a working `deploy/compose/` actually produces, not just the
    /// closed-port failure path above.
    #[tokio::test]
    async fn check_reachable_succeeds_against_a_real_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // The probe only needs the TCP handshake to complete, not an actual
        // accept()/response — same as the real Postgres/MinIO probes, which
        // never send or read application bytes.
        let result = check_reachable("test", "127.0.0.1", port, Duration::from_secs(1)).await;
        assert!(
            result.is_ok(),
            "expected success against a real listener: {result:?}"
        );
    }

    /// `s3_host_port` fails closed on an endpoint missing the `:port`
    /// suffix the compose convention requires.
    #[test]
    fn s3_endpoint_host_port_rejects_a_missing_port() {
        assert!(s3_host_port("127.0.0.1").is_err());
    }

    /// `s3_host_port` fails closed on a non-numeric port rather than
    /// panicking on the `.parse()` inside it.
    #[test]
    fn s3_endpoint_host_port_rejects_a_non_numeric_port() {
        assert!(s3_host_port("127.0.0.1:minio").is_err());
    }
}
