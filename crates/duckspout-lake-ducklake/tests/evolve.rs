//! The §8.5 lattice-law suite for `evolve_schema` on the real backend
//! (issue #40): "the schema-widening join is commutative, associative, and
//! idempotent — the property that makes EvolveSchema crash-retry and
//! concurrent-owner convergence correct (§6)".
//!
//! Scoping, stated honestly: no free-standing lattice-join type exists in
//! `duckspout-types` — the lattice lives as `evolve_schema`'s closed
//! logical-type table and column-add semantics in this backend — so the
//! laws are checked HERE, on the surface that implements them.
//! Idempotence of one full re-application is already armed in the
//! published conformance suite (`evolve_idempotent`,
//! `duckspout-lake-contract`); this suite adds the order laws it cannot
//! see: convergence across application ORDER and split/batched
//! application, exhaustively over every permutation (the repo's
//! bounded-exhaustive posture: 3 columns, all 6 orders — exhaustion at a
//! tiny scope beats sampling at a large one, §3.1).

mod common;

use common::{lake_paths, open_committer};
use duckspout_types::{ColumnSpec, DatasetId, LakeCommitter, SchemaEvolution};

/// Drives one immediately-resolving port future (the committer is
/// synchronous behind the port — its module docs).
fn block_on<T>(mut future: duckspout_types::BoxFuture<'_, T>) -> T {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => unreachable!("the committer resolves synchronously"),
    }
}

fn column(name: &str, logical_type: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.to_owned(),
        logical_type: logical_type.to_owned(),
    }
}

fn evolution(columns: &[ColumnSpec]) -> SchemaEvolution {
    SchemaEvolution {
        dataset: DatasetId::new("evolve"),
        columns: columns.to_vec(),
    }
}

/// The dataset's schema as an order-insensitive (column, type) set, read
/// back through a raw inspection connection — the yardstick every leg is
/// compared against.
fn schema_set(paths: &common::LakePaths) -> Vec<(String, String)> {
    let conn = common::inspect(paths);
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type FROM duckdb_columns()
             WHERE database_name = 'lake' AND table_name = 'ds_evolve'
             ORDER BY column_name",
        )
        .expect("schema query prepares");
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("schema query runs");
    rows.collect::<Result<_, _>>().expect("schema rows read")
}

/// Convergence across application order and batching (§6.4's
/// "concurrent applications converge (commutative-join semantics)"): every
/// permutation of the three single-column evolutions — with a mid-sequence
/// crash-retry replay and a final batched re-application — yields the
/// identical (column, type) set. Would catch: an `ADD COLUMN` without
/// `IF NOT EXISTS` (the replay leg fails outright — the crash-retry bug),
/// or any order-sensitive application (two racing owners applying the same
/// evolutions in different orders would diverge, exactly the §6 hazard the
/// law exists for).
#[test]
fn evolutions_converge_from_every_application_order() {
    let columns = [
        column("ts", "timestamp_micros"),
        column("body", "utf8"),
        column("flags", "uint32"),
    ];
    let permutations: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    let mut expected: Option<Vec<(String, String)>> = None;
    for order in permutations {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = lake_paths(dir.path());
        let committer = open_committer(&paths);
        for (step, index) in order.into_iter().enumerate() {
            block_on(committer.evolve_schema(evolution(&[columns[index].clone()])))
                .expect("single-column evolution applies");
            if step == 1 {
                // Crash-retry leg: replay the step just applied (§6.4
                // idempotence under retry, mid-sequence rather than the
                // conformance suite's terminal replay).
                block_on(committer.evolve_schema(evolution(&[columns[index].clone()])))
                    .expect("replayed evolution converges");
            }
        }
        // Batched re-application of everything (the other owner's view).
        block_on(committer.evolve_schema(evolution(&columns)))
            .expect("batched re-application converges");

        let got = schema_set(&paths);
        assert_eq!(
            got.len(),
            columns.len(),
            "every column lands exactly once: {got:?}"
        );
        match &expected {
            None => expected = Some(got),
            Some(first) => assert_eq!(
                &got, first,
                "order {order:?} diverged from the first permutation"
            ),
        }
    }
}
