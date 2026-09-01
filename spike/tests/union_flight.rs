//! The full remote shape of the §7.5 union (issue #27 stretch): the hot
//! branch is served by the #26 Flight server over the ingesting process's
//! hot db, while the executor is a SEPARATE DuckDB with only the lake
//! attached — the topology of a real querying DuckDB (lake direct, hot via
//! the serving node; docs/design/query.md §7.4/§7.5).
//!
//! Spike stand-in for Airport: the executor reads `complete_through` from
//! the lake at bind, ships the hot-branch SQL (bound INCLUDED — the bound
//! travels with the bind-time watermark) over Flight, materializes the
//! streamed batches into a local table, and runs the ONE union statement
//! against lake + that table. The real extension binds the remote scan into
//! the plan instead of materializing; the seams exercised — watermark read,
//! bound pushdown, engine-Arrow-over-IPC into a second engine — are the same.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow_flight::{FlightClient, FlightDescriptor};
use futures::TryStreamExt;
use spike::drain::{CommitRequest, DrainCore};
use spike::flight::HotFlightService;
use spike::ingest::{IngestCore, LogRow};
use spike::union_query::{audit, materialize_hot, pinned_union_sql, read_complete_through};

const TOTAL: i64 = 1_000;
const W0_ROWS: i64 = 600;
const CT_W0: i64 = 1_756_600_000_000_000 + W0_ROWS - 1;

/// Hot rows straddling the boundary, remote watermark-bounded union tiling
/// exactly with `complete_through` visible — end to end over a real socket.
/// Would catch a bound that doesn't survive the Flight round trip, type
/// mangling breaking the union's schema compatibility, or the lake and the
/// remote hot disagreeing on the boundary row.
#[tokio::test]
async fn union_with_flight_served_hot_branch() {
    let dir = tempfile::tempdir().unwrap();
    let hot_db = dir.path().join("hot.db");
    let lake = dir.path().join("lake");

    // Ingest two windows, drain w0 into the lake (the #25 atomic commit),
    // then release the hot db so the serving process can own it.
    {
        let mut core = IngestCore::open(&hot_db).unwrap();
        core.create_window("hot_w0").unwrap();
        core.create_window("hot_w1").unwrap();
        let w0: Vec<_> = (0..W0_ROWS).map(LogRow::synthetic).collect();
        let w1: Vec<_> = (W0_ROWS..TOTAL).map(LogRow::synthetic).collect();
        core.insert_batch("hot_w0", &w0).unwrap();
        core.insert_batch("hot_w1", &w1).unwrap();
    }
    {
        let drain = DrainCore::open(&hot_db, &lake).unwrap();
        let part = lake.join("data").join("w0-part0.parquet");
        std::fs::create_dir_all(part.parent().unwrap()).unwrap();
        let stats = drain.seal_part("hot_w0", &part).unwrap();
        drain
            .lake_commit(&CommitRequest {
                partition: "tenant-a/logs/p0".to_string(),
                window_id: 0,
                part,
                complete_through_micros: CT_W0,
                rows: stats.rows,
            })
            .unwrap();
    } // drop → lake catalog unlocked for the executor

    // The serving node: #26's Flight server over the hot db.
    let core = Arc::new(Mutex::new(IngestCore::open(&hot_db).unwrap()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(HotFlightService::new(core).into_server())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);

    // The executor: its own DuckDB (scratch main db), lake attached, no
    // access to the hot file — remote hot is the only hot it has.
    let executor = DrainCore::open(&dir.path().join("executor.db"), &lake).unwrap();

    let t0 = Instant::now();
    // Bind: ONE watermark read; its value bounds the remote hot fetch.
    let ct = read_complete_through(executor.conn()).unwrap().unwrap();
    assert_eq!(ct, CT_W0);
    let hot_sql = format!(
        "SELECT origin, seq, ts, severity, body, attrs
         FROM (SELECT * FROM hot_w0 UNION ALL SELECT * FROM hot_w1) h
         WHERE h.ts > make_timestamp({ct})"
    );
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(hot_sql))
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<_> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let fetched = materialize_hot(executor.conn(), "hot_remote", &batches).unwrap();
    assert_eq!(
        fetched,
        TOTAL - W0_ROWS,
        "server-side watermark bound must exclude drained rows"
    );

    // The ONE union statement, same §7.5 shape, hot branch = the remote rows.
    let a = audit(executor.conn(), &pinned_union_sql("hot_remote")).unwrap();
    eprintln!(
        "remote union ballpark ({TOTAL} rows): bind + flight fetch + materialize + union in {:.1?}",
        t0.elapsed()
    );
    assert_eq!(
        (a.total, a.distinct_rows),
        (TOTAL, TOTAL),
        "tiling broke remotely: {a:?}"
    );
    assert_eq!(a.cold_rows, W0_ROWS);
    assert_eq!(a.hot_rows, TOTAL - W0_ROWS);
    assert_eq!(a.ct_values, 1);
    assert_eq!(a.ct_max_micros, Some(CT_W0));
}
