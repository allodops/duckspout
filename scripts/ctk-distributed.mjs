#!/usr/bin/env bun
// The distributed CTK tier (§8.4; ledger row ctk-distributed, nightly, v0.2,
// issue #58): real multi-node `duckspout-fleet` runs against real MinIO +
// Postgres, each with a journaling `duckspout-loadgen` member and a fault
// schedule, each graded afterwards by the separate `duckspout-judge` binary.
//
// The gate is the JUDGE'S exit code, never this script's opinion of the run:
// 0 = Pass, 2 = Violation, 3 = NoVerdict. NoVerdict is never a pass (§8.4's
// vacuity teeth), so this script exits nonzero on it exactly as it does on a
// violation — and it forwards the judge's own code rather than collapsing
// both to 1, so "we found a bug" and "this run proved nothing" stay
// distinguishable in the job log.
//
// The fleet runner's OWN exit code is deliberately NOT a gate. §8.4: judging
// from journals after the run "is what lets the fleet misbehave freely during
// the run and still be convicted precisely afterward" — a fleet that could
// veto its own grading would be the runner grading itself, which D-5 splits
// these two binaries apart to prevent. A fleet run that died before writing
// any evidence still fails this gate, via the only route that is honest about
// why: the judge reports NoVerdict over the evidence that is not there.
//
// Backends are required, never defaulted and never skipped (the same
// fail-closed posture §8.2's conformance tier takes: "an endpoint absent is a
// fail, never a skip"). The MinIO bucket must already exist — the fleet runner
// does not create buckets, and neither does this script.
//
// `DUCKSPOUT_CTK_POSTGRES_DSN` must be libpq's KEYWORD/VALUE form
// (`postgres:host=… port=… dbname=… user=…`), carrying no password — the
// password is a file path (§9.5) that the fleet runner writes from
// `DUCKSPOUT_CTK_POSTGRES_PASSWORD`. The URI form that `duckspout-fleet
// --postgres-dsn`'s own CLI default uses is NOT usable here: issue #212 —
// DuckLake's real `ATTACH` does not parse it and silently treats the whole
// string as a local file path.

import { basename, join } from "node:path";
import { existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { repoRoot, run } from "./lib/sh.mjs";
import { fail } from "./lib/proc.mjs";
import { error, group, info, notice } from "./lib/log.mjs";

// Release, matching the `smoke` gate's reasoning: this drives a REAL daemon
// fleet under sustained load through real drains, and a dev-profile daemon
// changes the timing of every fault window relative to the drain cadence the
// windows are aimed at.
const CARGO_PROFILE = "release";
// cargo honors CARGO_TARGET_DIR over the repo-relative `target/` default, and
// this script has to look for the binaries where cargo actually put them —
// the same reasoning scripts/instr-gate.mjs states for its own artifact path.
const CARGO_TARGET_DIR = process.env.CARGO_TARGET_DIR
  ? join(process.env.CARGO_TARGET_DIR)
  : join(repoRoot, "target");
const BIN_DIR = join(CARGO_TARGET_DIR, CARGO_PROFILE);
const RUNS_DIR = join(CARGO_TARGET_DIR, "ctk-distributed");
const REPLAY_FIXTURES = join(repoRoot, "crates/duckspout-judge/tests/fixtures/replay");

// The drive-load pass every profile runs: 300 batches × 200 ms ≈ 60 s of
// sustained load (§8.4 asks for sustained load, not a burst). Two separate
// lower bounds, both of which this clears by an order of magnitude:
//
//   1. It must OUTLAST every fault window below — the longest is the SIGSTOP
//      pause, whose default duration is `HEARTBEAT_TTL_SECS + 5` = 20 s after
//      a 5 s delay — so no window ever opens on an already-idle fleet, the
//      failure mode `duckspout-fleet`'s own `--load-batches` docs warn about.
//   2. It must outlast `--hot-window` + `--allowed-lateness` several times
//      over, or NOTHING EVER DRAINS: a window is noted closed only once a
//      newer one has been allocated for its partition, and the stager only
//      allocates that when data arrives after `hot.window` has elapsed
//      (`warmUpCatalog`'s own docs carry the full reasoning and the run this
//      was learned from).
const LOAD_BATCHES = 300;
const LOAD_INTERVAL_MS = 200;
const SETTLE_TIMEOUT_SECS = 120;
const BOOT_TIMEOUT_SECS = 60;

// Nodes per judged profile. Three, not one: §8.4's tier exists to run REAL
// multi-node fleets, and the cross-node-contention vacuity rule is a
// statement about a roster with more than one member in it. Three rather than
// more because every node is a full `duckspout-daemon` with its own embedded
// DuckDB, and a profile also boots a fourth process (the loadgen member, plus
// a fifth when `--fault-churn-join` is armed) on one GitHub-hosted runner.
const NODES = 3;

// §8.4's standard fault schedule, split across profiles rather than crammed
// into one run. Every `--fault-*` flag names a node INDEX, so two faults aimed
// at the same index in one run would fight over the same process; splitting
// also keeps each run's evidence attributable to a small, named set of
// windows. Between them these five cover every family §8.4 lists.
//
// Node 0 is never a fault target in any profile: it is the loadgen member's
// endpoint, and a client witness that loses its own endpoint is a fault on the
// harness, not on DuckSpout (`duckspout-fleet`'s `--loadgen-bin` docs).
const PROFILES = [
  {
    id: "kill-mid-drain",
    // §8.4's sharpest fault: the partition owner killed between PutPart and
    // LakeCommit, the window where SingleDrainCommit and TakeoverDrain are
    // both live.
    faults: ["--fault-kill-node", "1", "--fault-kill-mid-drain"],
  },
  {
    id: "partition-degrade",
    // "Network partitions and asymmetric degradation (drops, delay,
    // bandwidth caps)" — both halves, on two different nodes at once.
    faults: ["--fault-partition-node", "1", "--fault-degrade-node", "2"],
  },
  {
    id: "pause-churn",
    // The FencedZombie pause (SIGSTOP past the claim TTL, then SIGCONT) plus
    // membership churn in both directions — a graceful leave under load and a
    // join under load, which §8.4 puts in the v1 schedule explicitly rather
    // than deferring.
    faults: [
      "--fault-sigstop-node",
      "1",
      "--fault-churn-leave-node",
      "2",
      "--fault-churn-join",
    ],
  },
  {
    id: "catalog-flap",
    // "Catalog outage windows (ingest must continue undegraded; drains stall
    // and disclose)" and "discovery flapping".
    faults: [
      "--fault-catalog-outage-node",
      "1",
      "--fault-discovery-flap-node",
      "2",
    ],
  },
  {
    id: "query-cache",
    // "Flight-server kill mid-stream" and the forced residency churn racing
    // real queries that §8.4's cache-transparency judge exists for.
    faults: [
      "--fault-flight-kill-node",
      "1",
      "--fault-cache-churn-node",
      "2",
    ],
  },
];

/** A required backend setting: absent is a red gate, never a skipped one. */
function required(name) {
  const value = process.env[name];
  if (!value)
    fail(
      `ctk-distributed: ${name} is unset — §8.4's distributed tier runs against REAL MinIO and ` +
        `Postgres, and an endpoint absent is a fail, never a skip (see .github/workflows/nightly.yml's ` +
        `ctk-distributed job for the exact backends CI provisions)`,
    );
  return value;
}

const BACKEND = {
  postgresDsn: () => required("DUCKSPOUT_CTK_POSTGRES_DSN"),
  postgresPassword: () => required("DUCKSPOUT_CTK_POSTGRES_PASSWORD"),
  s3Endpoint: () => required("DUCKSPOUT_CTK_S3_ENDPOINT"),
  s3Bucket: () => required("DUCKSPOUT_CTK_S3_BUCKET"),
  s3AccessKeyId: () => required("DUCKSPOUT_CTK_S3_ACCESS_KEY_ID"),
  s3SecretAccessKey: () => required("DUCKSPOUT_CTK_S3_SECRET_ACCESS_KEY"),
};

/** Port block for profile `index`: 10 apart, comfortably more than the 4
 * nodes a profile can provision (NODES, plus the one `--fault-churn-join`
 * adds), so a socket still lingering from one profile cannot land inside the
 * next one's range. */
function ports(index) {
  const stride = 10 * index;
  return {
    otlp: 14317 + stride,
    flight: 18815 + stride,
    peer: 17946 + stride,
    status: 19095 + stride,
  };
}

function fleetArgs(profile, index, workDir) {
  const p = ports(index);
  return [
    join(BIN_DIR, "duckspout-fleet"),
    "--seed", String(index),
    "--nodes", String(NODES),
    "--work-dir", workDir,
    ...fleetCommonArgs(profile.id, p),
    "--load-batches", String(LOAD_BATCHES),
    "--load-interval-ms", String(LOAD_INTERVAL_MS),
    ...profile.faults,
  ];
}

function fleetCommonArgs(tenantSuffix, p) {
  return [
    "--daemon-bin", join(BIN_DIR, "duckspout-daemon"),
    "--loadgen-bin", join(BIN_DIR, "duckspout-loadgen"),
    // A tenant per profile. A tenant is what a partition is keyed by, and the
    // Postgres catalog outlives every run here; sharing one across profiles
    // would let a run torn down with staged-but-undrained data stall the next
    // profile's watermark for reasons that have nothing to do with its faults
    // (`duckspout-fleet`'s `--tenant` docs).
    "--tenant", `ctk-${tenantSuffix}`,
    "--otlp-base-port", String(p.otlp),
    "--flight-base-port", String(p.flight),
    "--peer-base-port", String(p.peer),
    "--status-base-port", String(p.status),
    "--postgres-dsn", BACKEND.postgresDsn(),
    "--postgres-password", BACKEND.postgresPassword(),
    "--s3-endpoint", BACKEND.s3Endpoint(),
    "--s3-bucket", BACKEND.s3Bucket(),
    "--s3-access-key-id", BACKEND.s3AccessKeyId(),
    "--s3-secret-access-key", BACKEND.s3SecretAccessKey(),
    "--boot-timeout-secs", String(BOOT_TIMEOUT_SECS),
    "--settle-timeout-secs", String(SETTLE_TIMEOUT_SECS),
  ];
}

/** Boots a ONE-node, fault-free fleet before any judged profile runs.
 *
 * Not evidence, and deliberately not judged: this is provisioning. Issue #213
 * — filed against this same fleet runner — is that several nodes cold-booting
 * CONCURRENTLY against a genuinely fresh Postgres catalog race DuckLake's own
 * metadata-table initialization, and one of them loses. `boot_fleet` spawns
 * every node at once and only then waits for readiness, so every judged
 * profile below would hit that race on a CI-fresh catalog. One node
 * initializing the catalog first is the same sequencing
 * `crates/duckspout-fleet/tests/fault_injection.rs`'s membership-join
 * scenario relies on, and it is a workaround for #213 rather than a fix: when
 * #213 closes, this step can go.
 *
 * It is also the harness's own smoke check, which is why a non-zero exit is
 * fatal here rather than judged: if one unfaulted node cannot complete a
 * clean boot → ingest → drain → watermark loop against these backends — with
 * the loadgen member started and exited cleanly, since `fleetCommonArgs`
 * passes `--loadgen-bin` here too — every verdict after it would be a
 * statement about the harness, not about DuckSpout.
 *
 * # Why this passes no `--load-batches` / `--load-interval-ms`
 *
 * It inherits `duckspout-fleet`'s own defaults (60 × 200 ms ≈ 12 s), and it
 * must: a window is noted closed only once a NEWER window has been allocated
 * for its `(dataset, partition)` (`duckspout_daemon::wiring`'s
 * `note_closed_windows`: `window.window.0 < high_water.0`), and the stager
 * only allocates the next window when data arrives after `hot.window` has
 * elapsed on the current one. A load pass shorter than `--hot-window` +
 * `--allowed-lateness` therefore rolls no window, closes none, drains none,
 * and commits nothing — with no error anywhere, because nothing went wrong;
 * there was simply never a second window. The first version of this step
 * passed `--load-batches 20 --load-interval-ms 100` (≈2 s against the 5 s
 * default `--hot-window`) and did exactly that: 227 Accept/StageCommit/
 * ClientAck triples in the node's journal, not one `SealWindow`, and a
 * 120-second settle timeout. The judged profiles are not at risk of it
 * (`LOAD_BATCHES` × `LOAD_INTERVAL_MS` ≈ 60 s), but they are not the reason
 * this is safe — running the runner's own sized default is.
 */
async function warmUpCatalog() {
  const workDir = join(RUNS_DIR, "catalog-warmup");
  rmSync(workDir, { recursive: true, force: true });
  mkdirSync(workDir, { recursive: true });
  const code = await group("catalog warm-up (1 node, no faults, not judged — issue #213)", () =>
    run([
      join(BIN_DIR, "duckspout-fleet"),
      "--seed", "99",
      "--nodes", "1",
      "--work-dir", workDir,
      ...fleetCommonArgs("warmup", ports(PROFILES.length)),
    ]),
  );
  if (code !== 0)
    fail(`ctk-distributed: the one-node catalog warm-up exited ${code} — the backends or the fleet runner itself are broken, so no verdict below would be about DuckSpout (artifacts: ${workDir})`);
}

/** Every journal a fleet run left behind: one per provisioned node (including
 * a node that joined mid-run, and one that was killed) plus the loadgen
 * member's. Discovered, never enumerated from the profile — a node whose
 * journal is missing is evidence about the run, and the judge's own
 * node-continuity rule reads the roster from `run.json` to notice it. */
async function journals(workDir) {
  const found = [];
  for await (const rel of new Bun.Glob("**").scan({ cwd: workDir, onlyFiles: true })) {
    // The node journals sit one directory down (`<node>/journal.ndjson`) and
    // the loadgen member's at the top level, so this walks rather than
    // matching a fixed depth.
    if (!rel.endsWith(".ndjson")) continue;
    // faults.ndjson is the injectors' ledger, not a node journal; it goes in
    // under --fault-log, and feeding it as a journal would be feeding the
    // judge evidence in a vocabulary it does not parse.
    if (basename(rel) === "faults.ndjson") continue;
    found.push(join(workDir, rel));
  }
  return found.sort();
}

function judgeArgs(workDir, journalPaths) {
  return [
    join(BIN_DIR, "duckspout-judge"),
    ...journalPaths.flatMap((path) => ["--journal", path]),
    "--fault-log", join(workDir, "faults.ndjson"),
    "--run-manifest", join(workDir, "run.json"),
    // Deliberately NO --final-state-fixture / --latest-view-fixture /
    // --committed-parts-fixture / --read-log. Those four are DEV/TEST doubles
    // (`duckspout-judge`'s own flag docs), and there is no real read-back
    // surface or served-read log behind them yet. Passing a fixture here
    // would be handing the gate fabricated evidence to grade; omitting them
    // makes the affected predicates report NoVerdict, which is the honest
    // state of a tier whose producers have not all landed.
  ];
}

// `duckspout-judge`'s whole exit contract (its `EXIT_CONTRACT` const). Any
// other code is the judge itself having failed to run, which is a broken
// harness rather than a verdict about DuckSpout — `verdictName` refuses to
// launder it into one.
const VERDICTS = { 0: "Pass", 2: "Violation", 3: "NoVerdict" };

function verdictName(code) {
  if (!(code in VERDICTS))
    fail(`ctk-distributed: the judge exited ${code}, which is not one of its three verdicts (0/2/3) — the judge crashed rather than judging`);
  return VERDICTS[code];
}

/** §8.4's composition, applied across profiles the same way the judge applies
 * it across predicates: a proven violation anywhere outranks an inconclusive
 * run elsewhere; anything inconclusive outranks a pass. */
function worst(codes) {
  if (codes.includes(2)) return 2;
  if (codes.some((code) => code !== 0)) return 3;
  return 0;
}

/** §8.4's must-convict self-test, at the process boundary.
 *
 * `crates/duckspout-judge/tests/seeded_violation_replay.rs` already drives
 * every seed through `duckspout_judge::runner` on every PR, and that is the
 * exhaustive test — it also asserts the base passes and that no OTHER
 * predicate convicts a seed. What it cannot reach is the judge as this gate
 * actually invokes it: a BINARY, addressed by CLI flag names, whose verdict
 * reaches us only as an exit code. A renamed flag or a broken exit mapping
 * would leave that test green and this gate grading nothing. So: the clean
 * base must exit 0 through the real argv, and each seeded violation must
 * exit 2 through it.
 */
async function seededViolationReplay() {
  const base = join(REPLAY_FIXTURES, "base");
  const seeds = readdirSync(REPLAY_FIXTURES, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name !== "base")
    .map((entry) => entry.name)
    .sort();
  if (seeds.length === 0)
    fail(`ctk-distributed: no seeded-violation fixtures under ${REPLAY_FIXTURES} — a must-convict self-test with nothing to convict is not a self-test`);

  // Each seed directory holds exactly the ONE file that differs; every other
  // path comes from the clean base. That is the same overlay the crate's own
  // replay harness does, expressed as argv instead of a temp-dir copy.
  const args = (dir) => {
    const at = (name) => (existsSync(join(dir, name)) ? join(dir, name) : join(base, name));
    return [
      join(BIN_DIR, "duckspout-judge"),
      "--journal", at("n1.ndjson"),
      "--journal", at("n2.ndjson"),
      "--journal", at("loadgen.ndjson"),
      "--final-state-fixture", at("final_state.json"),
      "--read-log", at("reads.ndjson"),
      "--latest-view-fixture", at("latest_view.json"),
      "--committed-parts-fixture", at("committed_parts.json"),
      "--fault-log", at("faults.ndjson"),
      "--run-manifest", at("run.json"),
    ];
  };

  const baseCode = await group("seeded-violation replay: the clean base", () => run(args(base)));
  if (baseCode !== 0)
    fail(`ctk-distributed: the judge binary reports ${verdictName(baseCode)} on the CLEAN replay base — a judge that convicts a healthy run convicts nothing (§8.4)`);

  for (const seed of seeds) {
    const code = await group(`seeded-violation replay: ${seed}`, () =>
      run(args(join(REPLAY_FIXTURES, seed))),
    );
    if (code !== 2)
      fail(`ctk-distributed: the judge binary ACQUITTED its own seeded '${seed}' violation (exit ${code} = ${verdictName(code)}, expected 2 = Violation) — §8.4: a judge that acquits its own seeded violation fails CI`);
  }
  info(`ctk-distributed: seeded-violation replay green (base passes, ${seeds.length} seed(s) convicted) through the judge BINARY`);
}

async function build() {
  const code = await group("build the CTK binaries (release)", () =>
    run([
      "cargo", "build", `--${CARGO_PROFILE}`,
      "-p", "duckspout-daemon",
      "-p", "duckspout-fleet",
      "-p", "duckspout-judge",
      "-p", "duckspout-loadgen",
    ]),
  );
  if (code !== 0) fail(`ctk-distributed: building the CTK binaries exited ${code}`);
}

async function runProfile(profile, index) {
  const workDir = join(RUNS_DIR, profile.id);
  rmSync(workDir, { recursive: true, force: true });
  mkdirSync(workDir, { recursive: true });

  const fleetCode = await group(`fleet run: ${profile.id} (${profile.faults.join(" ")})`, () =>
    run(fleetArgs(profile, index, workDir)),
  );
  // Reported, never gated on — see this file's header.
  info(`ctk-distributed: ${profile.id}: duckspout-fleet exited ${fleetCode}`);

  const journalPaths = await journals(workDir);
  info(`ctk-distributed: ${profile.id}: ${journalPaths.length} journal(s) to grade`);
  const judgeCode = await group(`judge: ${profile.id}`, () =>
    run(judgeArgs(workDir, journalPaths)),
  );
  info(`ctk-distributed: ${profile.id}: verdict ${verdictName(judgeCode)} (exit ${judgeCode})`);
  return { id: profile.id, fleetCode, judgeCode, workDir };
}

async function main() {
  // Read every backend setting before anything is built or booted, so a
  // missing one fails in seconds instead of after a release build.
  for (const read of Object.values(BACKEND)) read();
  await build();
  await seededViolationReplay();
  await warmUpCatalog();

  const results = [];
  for (const [index, profile] of PROFILES.entries()) results.push(await runProfile(profile, index));

  info("\nctk-distributed verdicts (§8.4)");
  info("==============================");
  for (const r of results)
    info(`  ${r.id.padEnd(20)} judge=${verdictName(r.judgeCode).padEnd(10)} fleet-exit=${r.fleetCode}  ${r.workDir}`);

  const code = worst(results.map((r) => r.judgeCode));
  if (code === 0) {
    notice(`ctk-distributed: every profile passed (${results.length} run(s))`);
    return 0;
  }
  const offenders = results.filter((r) => r.judgeCode !== 0).map((r) => `${r.id}=${verdictName(r.judgeCode)}`);
  // `fail()` is not used here on purpose: it exits 1, which would erase the
  // Violation/NoVerdict distinction the judge's exit contract exists to make.
  // The one structured summary line s§5.2 asks for is still emitted.
  error(`FAIL: ctk-distributed: ${verdictName(code)} (${offenders.join(", ")}) — §8.4: NoVerdict is never a pass`);
  return code;
}

if (import.meta.main) process.exit(await main());
