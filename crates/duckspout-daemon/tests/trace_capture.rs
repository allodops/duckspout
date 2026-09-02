//! Live trace generation (§8.2, issue #42): the real accept → staging →
//! drain composition (`tests/common/capture.rs`) journals its §3.3 events
//! through the [`duckspout_types::TraceSink`] seam, and the captured NDJSON
//! must equal the committed fixture `specs/fixtures/ingest-captured.ndjson`
//! byte for byte.
//!
//! Two consumers:
//!
//! - `just conformance` (scripts/trace-conformance.mjs) runs this test with
//!   `DUCKSPOUT_TRACE_CAPTURE_OUT` set, then validates the FRESH capture
//!   against `specs/traces/IngestTrace.tla` — the live tier of §8.2: a
//!   static fixture certifies capture day, the fresh trace certifies the
//!   code in the PR.
//! - The equality assertion ties the committed fixture to reality on every
//!   ordinary `just test` run: if instrumentation or choreography changes
//!   the event sequence, this test goes red and the fixture (plus its
//!   doctored family) is re-captured consciously — a fixture cannot
//!   silently go stale.
//!
//! `trace_capture_real_backends.rs` (issue #44) runs the identical
//! choreography against real `MinIO` + Postgres instead of the local doubles
//! below — the real-backend tier of §8.2.

mod common;

use std::sync::Arc;

use common::capture::capture_ingest_drain_trace;
use duckspout_lake_ducklake::{DuckLakeCommitter, DuckLakeConfig};

#[tokio::test(flavor = "multi_thread")]
async fn captured_trace_matches_the_committed_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let hot_dir = dir.path().join("hot");
    let lake_data = dir.path().join("lake-data");
    std::fs::create_dir_all(&lake_data).unwrap();
    let trace_path = dir.path().join("capture.ndjson");

    let committer = DuckLakeCommitter::open(DuckLakeConfig {
        catalog_uri: dir.path().join("meta.ducklake").display().to_string(),
        data_path: lake_data.display().to_string(),
        multi_process: false,
        s3: None,
    })
    .unwrap();
    let parts_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&lake_data).unwrap());

    let fresh = capture_ingest_drain_trace(&trace_path, hot_dir, committer, parts_store).await;

    let expected = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../specs/fixtures/ingest-captured.ndjson"
    ))
    .unwrap();
    assert_eq!(
        fresh, expected,
        "the fresh capture drifted from specs/fixtures/ingest-captured.ndjson — \
         re-capture the fixture (and re-derive its doctored family) consciously"
    );

    // Hand the fresh capture to the conformance runner's live tier.
    if let Ok(out) = std::env::var("DUCKSPOUT_TRACE_CAPTURE_OUT") {
        std::fs::write(out, fresh).unwrap();
    }
}
