//! The real-backend variant of §8.2's trace conformance (issue #44): the
//! exact same choreography as `trace_capture.rs`
//! (`tests/common/capture.rs::capture_ingest_drain_trace`), driven against
//! **real `MinIO` (S3) and real Postgres** instead of the local-filesystem
//! doubles — `deploy/compose/compose.yaml`'s dev/CI backends, or the CI
//! service containers `.github/workflows/ci.yml`'s `conformance` job
//! stands up.
//!
//! This test only *captures*: it writes the fresh trace to
//! `DUCKSPOUT_TRACE_CAPTURE_OUT` and asserts nothing about its content
//! beyond "the composition committed" — decoding, refinement, and the
//! doctored-variant mechanism assertions are
//! `scripts/trace-conformance.mjs`'s job (the real-backend tier), which
//! doctors the trace this test just produced rather than comparing it to a
//! static fixture (§8.2: "static fixtures would go stale the moment an
//! adapter changed").
//!
//! **Skips gracefully without Docker** (§8.2's documented posture): if the
//! `DUCKSPOUT_CONFORMANCE_*` env vars naming the backends are unset, this
//! test prints why and returns — a contributor's plain `cargo test` never
//! needs `MinIO`/Postgres running. The CI gate does **not** inherit that
//! skip: `scripts/trace-conformance.mjs`'s real-backend tier checks
//! `GITHUB_ACTIONS` and fails closed if the vars are absent there, per §8.2.

mod common;

use std::sync::Arc;

use common::capture::capture_ingest_drain_trace;
use duckspout_lake_ducklake::{DuckLakeCommitter, DuckLakeConfig, S3Access};

/// One env var per connection fact — never a pre-assembled DSN/URL, so
/// each half is independently overridable the way `deploy/compose` and
/// `ci.yml`'s service containers hand them over.
struct RealBackends {
    s3_endpoint: String,
    s3_bucket: String,
    s3_access_key_id: String,
    s3_secret_access_key: String,
    postgres_dsn: String,
}

fn real_backends_from_env() -> Option<RealBackends> {
    let var = |name: &str| std::env::var(name).ok();
    Some(RealBackends {
        s3_endpoint: var("DUCKSPOUT_CONFORMANCE_S3_ENDPOINT")?,
        s3_bucket: var("DUCKSPOUT_CONFORMANCE_S3_BUCKET")?,
        s3_access_key_id: var("DUCKSPOUT_CONFORMANCE_S3_ACCESS_KEY_ID")?,
        s3_secret_access_key: var("DUCKSPOUT_CONFORMANCE_S3_SECRET_ACCESS_KEY")?,
        postgres_dsn: var("DUCKSPOUT_CONFORMANCE_POSTGRES_DSN")?,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn captured_trace_against_real_minio_and_postgres() {
    let Some(backends) = real_backends_from_env() else {
        eprintln!(
            "trace_capture_real_backends: DUCKSPOUT_CONFORMANCE_* env vars absent — skipping \
             (Docker-optional dev convenience, §8.2; the CI gate does not inherit this skip: \
             scripts/trace-conformance.mjs's real-backend tier fails closed instead in GitHub Actions)"
        );
        return;
    };

    // A FIXED prefix, deliberately not run-scoped: a `DuckLake` catalog
    // pins its `DATA_PATH` for its whole lifetime (`ATTACH` fails with
    // "does not match existing data path" against any other value, proven
    // empirically against real MinIO + Postgres — the docs are silent on
    // this, R-trust-official-docs's vague-docs exception), so a
    // run-varying prefix breaks the second of two runs against the SAME
    // persistent backend — exactly the `deploy/compose` local-dev shape
    // (§9.1.3), not only CI's ephemeral-per-job containers. Repeated runs
    // against the same backend accumulate rows/parts under this prefix
    // instead, which the composition and its assertions do not mind
    // (mirrors how a real deployment's data_path never changes either).
    let data_prefix = "duckspout-conformance-capture";

    let dir = tempfile::tempdir().unwrap();
    let hot_dir = dir.path().join("hot");
    let trace_path = dir.path().join("capture-real-backends.ndjson");

    let committer = DuckLakeCommitter::open(DuckLakeConfig {
        catalog_uri: backends.postgres_dsn.clone(),
        data_path: format!("s3://{}/{data_prefix}", backends.s3_bucket),
        // Postgres is the multi-process catalog (§7.3, issue #119) — armed
        // here even though this test is single-process, so the real
        // backend's ATTACH path is exercised exactly as a multi-process
        // deployment would open it.
        multi_process: true,
        s3: Some(S3Access {
            endpoint: backends.s3_endpoint.clone(),
            region: "us-east-1".to_owned(),
            access_key_id: backends.s3_access_key_id.clone(),
            secret_access_key: backends.s3_secret_access_key.clone(),
            url_style_path: true, // MinIO
            use_ssl: false,       // dev/CI-only loopback endpoint
        }),
    })
    .expect("real-backend committer opens (MinIO httpfs secret + Postgres ATTACH)");

    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(format!("http://{}", backends.s3_endpoint))
        .with_bucket_name(&backends.s3_bucket)
        .with_access_key_id(&backends.s3_access_key_id)
        .with_secret_access_key(&backends.s3_secret_access_key)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false) // MinIO path style
        .build()
        .expect("real MinIO object_store client builds");
    // Rooted at the SAME prefix as the committer's DATA_PATH above — the
    // choreography (tests/common/capture.rs, shared with the local test)
    // hands `parts_store` a store already rooted at `data_path`, exactly
    // as the local test's `LocalFileSystem::new_with_prefix` is; without
    // this the drain's PUT and `ducklake_add_data_files`' read-back
    // address two different keys and every commit is rejected.
    let parts_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(s3, data_prefix));

    let fresh = capture_ingest_drain_trace(&trace_path, hot_dir, committer, parts_store).await;
    assert!(
        !fresh.is_empty(),
        "a real-backend capture produced no trace"
    );

    // Hand the fresh capture to the conformance runner's real-backend tier
    // (decode → refine → doctor, scripts/trace-conformance.mjs).
    if let Ok(out) = std::env::var("DUCKSPOUT_TRACE_CAPTURE_OUT") {
        std::fs::write(out, fresh).unwrap();
    }
}
