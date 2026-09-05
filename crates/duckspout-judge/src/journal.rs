//! NDJSON journal ingestion (§8.4, D-6): the shared plumbing every judge
//! predicate (#205's own zero-acked-lost, and #206/#207/#208's future ones)
//! parses fleet + load-generator journals through.
//!
//! Every journal file — one per fleet node, plus the load generator's own
//! (`duckspout_loadgen::journal`) — is the frozen `{node, seq, event}`
//! triple (`duckspout_types::TraceRecord`) one JSON object per line (D-6).
//! The loadgen's `ClientAck`/`ClientTimeout` lines additionally carry
//! payload identity as extra fields flattened onto the same object
//! (`duckspout_loadgen::journal::RequestIdentity`'s wire shape). This module
//! decodes that shape **structurally** — by field presence, not by trusting
//! which file it came from — without depending on the `duckspout-loadgen`
//! crate at all: a judge parses the wire format the same way a real OTLP
//! client speaks a wire protocol without linking the server it talks to
//! (`duckspout_loadgen::client`'s own module docs make the identical call
//! for the identical reason). `duckspout-accept` also journals `ClientAck`
//! on the node side (`docs/trace-mapping.md`), and those lines carry no
//! identity fields — this module keeps both shapes as one line type with an
//! `Option<RequestIdentity>`, so a plain node-journaled `ClientAck` is not a
//! decode error, just a line with no identity to key on.
//!
//! Malformed input fails the whole ingestion closed
//! (`docs/verification.md` §8.4's "skipped ≠ passed, ambiguity fails
//! closed" posture, echoed in `duckspout_types::trace`'s R-3 fail-loud
//! discipline for the writer side of this same format): a judge that
//! silently drops or ignores an unparseable line could certify a run that
//! never happened as recorded. One bad line anywhere fails the entire
//! ingestion — never a partial, silently-truncated result.
//!
//! # The three payload shapes (#205, #206)
//!
//! The frozen envelope carries no payload (`duckspout_types::trace`:
//! "variants are payload-free at bootstrap; per-variant payloads land with
//! the implementations that journal them"), so every payload this module
//! knows is decoded by FIELD PRESENCE off the line's extra fields, never by
//! which file or which event it rode on:
//!
//! | Keyed on | Decodes to | Written by | Read by |
//! |---|---|---|---|
//! | `request_id` | [`RequestIdentity`] | `duckspout-loadgen` (`ClientAck`/`ClientTimeout`) | `zero_acked_lost`, `watermark_honesty` |
//! | `complete_through_ms` | [`WatermarkClaim`] | the node advancing (`LakeCommitOk`, §6.4) or advertising (`ClaimAdvertise`, §7.3) a watermark | `watermark_honesty` |
//! | `changelog_key` | [`ChangelogEntry`] | the changelog write client, on the line resolving its request | `latest_view` |
//! | `part` | [`PartRetention`] | the node retiring (`Expire`, §6.7) or sealing (`SnapshotSeal`, §6.7) a cold part | `retention_honesty` |
//!
//! **Producer status, stated plainly** (the same honesty
//! `crate::final_state`'s scope note keeps for the read-back side): the
//! `RequestIdentity` shape is written for real today by
//! `duckspout_loadgen::journal`; its two #206 additions
//! (`partition`/`max_event_time_ms`) and the three other payload shapes are
//! formats this judge DEFINES and decodes, and no producer in this
//! workspace emits them yet — the fleet's node-side watermark disclosure
//! and its changelog write client land with the distributed tier's own
//! wiring (#204, #208). A run whose journals carry none of them is not
//! quietly passed: each predicate's vacuity rule turns absent evidence into
//! `NoVerdict` (§8.4), never `Pass`.
//!
//! [`PartRetention`] (#207) is the sharpest case of that honesty, and it is
//! worth naming precisely rather than leaving to the general disclaimer:
//! `Expire`, `SnapshotSeal`, `Demote` and `Evict` are journaled by NOTHING
//! in this workspace today — `docs/trace-mapping.md` attributes all four to
//! `duckspout-drain`, which implements only `DropWindow` of the five
//! post-drain/retention actions, because retention scheduling and the
//! cache class are respectively unbuilt and empty by construction at v1
//! (`docs/design/data-model.md` §2.4, `docs/deferred.md`'s warm-retention
//! row). So `crate::predicates::retention_honesty` is a real predicate over
//! a real, spec-shaped evidence format with no emitter yet, and it reports
//! `NoVerdict` on every run until one exists. That is the same posture #206
//! took for the watermark payloads, for the same reason: the judge for a
//! Keep Rule lands with the milestone that arms the rule's tier
//! (`ctk-release-gate`, v0.3), not after it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use duckspout_types::{
    DatasetId, DatasetKind, NodeId, OriginSeqRange, PartKind, PartName, PartitionId, TenantId,
    TraceEvent,
};
use serde::Deserialize;

/// The loadgen's payload identity, decoded structurally off a journal
/// line's extra fields (module docs) — field-for-field the wire shape of
/// `duckspout_loadgen::journal::RequestIdentity`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RequestIdentity {
    /// The idempotency key the request was sent with.
    pub request_id: String,
    /// The tenant the request was sent as.
    pub tenant: String,
    /// Number of log records the request carried.
    pub record_count: usize,
    /// The 0-based index of the first record the request carried — together
    /// with `record_count`, the `[first_index, first_index + record_count)`
    /// range this ack covers. ALIASES across loadgen fleet members and
    /// across one member's restart (ACPR finding HIGH-2) — never use this
    /// bare as a global correlation key; combine it with
    /// `source_incarnation` below.
    pub first_index: u64,
    /// The `(node, start_nonce)` pair naming the loadgen process incarnation
    /// that sent this request (ACPR HIGH-2,
    /// `duckspout_loadgen::client::source_incarnation`'s wire shape) —
    /// together with `first_index` this is what makes a record's identity
    /// globally unique across the whole fleet's lifetime, not just within
    /// one process. The predicate keys its `FinalSystemState` lookups on
    /// `{source_incarnation}-{index}`, matching the exact string
    /// `duckspout_loadgen::client::synthetic_batch` embeds in the record's
    /// own `loadgen.index` attribute.
    pub source_incarnation: String,
    /// The partition the accept side placed this batch's records in
    /// (§5's unit of ownership), as disclosed back to the client on the
    /// ack. `None` when the ack line does not carry it — the shape every
    /// journal written before #206 has, and the shape
    /// `duckspout-loadgen` still writes today (module docs' producer-status
    /// note). Without it a watermark, which is per-partition (§7.3), cannot
    /// be related to this ack at all, so `watermark_honesty` treats such an
    /// ack as no evidence rather than as evidence about some guessed
    /// partition.
    #[serde(default)]
    pub partition: Option<PartitionId>,
    /// The greatest event timestamp (Unix milliseconds) among the records
    /// this ack covers — the batch's own upper edge, which is what decides
    /// whether the whole batch sits at or below a `complete_through`
    /// (`crate::predicates::watermark_honesty`'s coverage rule). `None`
    /// carries the same meaning as `partition`'s `None`: no evidence.
    ///
    /// **Producer requirement (ACPR finding LOW-2).** This field is only
    /// half of the precedence rule: the OTHER half is the
    /// [`WatermarkClaim`] a producer flattens onto the same ack line, and
    /// which value it reads there is not a free choice. It must be the
    /// OWNER'S OWN AUTHORITATIVE LEDGER VALUE for that partition —
    /// `duckspout.watermarks`, which `docs/design/query.md` calls
    /// "transactional, authoritative" and which only `LakeCommit` writes,
    /// i.e. `duckspout-watermark`'s `WatermarkLedger` state the accepting
    /// owner holds — and NEVER a cached or registry-sourced copy of it. The
    /// registry's watermark row is explicitly advisory soft state
    /// (`docs/trace-mapping.md`: `ClaimAdvertise` — "Advisory registry row,
    /// Keep Rule 8"), so it may legitimately lag the real watermark; a
    /// producer that disclosed a LAGGING value here would make a genuine
    /// post-watermark straggler (already below the real watermark when it
    /// was acked, and therefore outside every `complete` read's contract per
    /// `docs/design/drain.md` §3) look like a record acked ahead of the
    /// watermark — and `watermark_honesty` would falsely convict a correct
    /// fleet for omitting it.
    #[serde(default)]
    pub max_event_time_ms: Option<i64>,
}

/// A watermark value a node either ADVANCED or ADVERTISED, decoded
/// structurally off a journal line carrying `complete_through_ms` (module
/// docs).
///
/// Which of the two it is, is decided by the line's EVENT, not by this
/// shape — `crate::predicates::watermark_honesty` owns that classification
/// (§6.4: the advance rides the `LakeCommit` outcome atomically and has no
/// event of its own; §7.3: the registry rows a node advertises are soft
/// state). Field-for-field the wire shape of
/// `duckspout_types::WatermarkRow`, which is what a node advancing or
/// advertising one holds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WatermarkClaim {
    /// The partition this watermark value is about.
    pub partition: PartitionId,
    /// The instant (Unix milliseconds, inclusive) claimed complete
    /// (`duckspout_types::WatermarkRow::complete_through_ms`).
    pub complete_through_ms: i64,
}

/// One changelog record's identity and content, decoded structurally off a
/// journal line carrying `changelog_key` (module docs).
///
/// `(origin, changelog_seq)` is the §3 fold order's key —
/// `DuckSpoutCore.tla`'s `OrdLt` — and `changelog_seq` is spelled with its
/// prefix deliberately: the frozen envelope already owns the top-level
/// `seq` field (the journaling node's own dense trace sequence, D-6), which
/// is a DIFFERENT number from the origin-assigned record sequence, and
/// flattening two `seq` fields onto one line would collapse them.
///
/// # Why `partition` and `tenant` are both here (ACPR finding HIGH-1)
///
/// Neither is decoration; each fixes one direction of a real
/// misidentification:
///
/// - `changelog_seq` is dense per-**`(partition, origin)`**
///   (`docs/design/ingest.md`: "`seq` (dense per-(partition, origin)
///   sequence)"; `docs/design/replication.md`: "stamped with
///   `(origin_node, partition, seq)`"), NOT per origin. Two records one
///   origin wrote in two different partitions legitimately carry the SAME
///   `changelog_seq`, so `(dataset, origin, changelog_seq)` does not name a
///   record — `(dataset, partition, origin, changelog_seq)` does.
/// - `changelog_key` is unique only WITHIN a tenant: the partition key is
///   `(tenant_id, shard)` and `<dataset>_latest` is a shared,
///   `tenant_id`-leading table (`docs/design/data-model.md`), so two
///   tenants' rows for one key string are two different rows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ChangelogEntry {
    /// The changelog dataset this record belongs to — the dataset whose
    /// `<dataset>_latest` view (§7.7) the fold is compared against.
    pub dataset: DatasetId,
    /// The partition this record was placed in — `(tenant_id, shard)`, with
    /// `shard = hash(key_cols)` for a changelog dataset (mandatory,
    /// `docs/design/data-model.md`). Required, and spelled exactly as
    /// [`WatermarkClaim::partition`] is: both decode from the one top-level
    /// `partition` field of the same flat journal line, so the two payload
    /// shapes this PR introduces stay one wire convention rather than two.
    pub partition: PartitionId,
    /// The tenant this record belongs to (the leading half of `partition`'s
    /// own `(tenant_id, shard)` key), and the scope in which
    /// `changelog_key` below is unique.
    pub tenant: TenantId,
    /// The declared key this record is a version of (§7.7's "latest row per
    /// declared key"), unique within `tenant` — not globally.
    pub changelog_key: String,
    /// The origin node that assigned `changelog_seq` (§4.2.4).
    pub origin: NodeId,
    /// The origin-assigned sequence number of this record, dense per
    /// `(partition, origin)` — see this type's own docs for why it is not a
    /// per-origin identity.
    pub changelog_seq: u64,
    /// True iff this record is a tombstone (`_op = 'delete'`, §7.7):
    /// tombstones delete, so the key is absent from the served view.
    #[serde(default)]
    pub tombstone: bool,
    /// The record's value, as the served view would return it. Exactly one
    /// of `tombstone`/`value` is meaningful, enforced at decode
    /// (`changelog_from_rest`): a tombstone carries no value, and a
    /// non-tombstone with no value is not a fold input, it is corruption.
    #[serde(default)]
    pub value: Option<String>,
}

/// One cold part named by a retention-relevant action, decoded structurally
/// off a journal line carrying `part` (module docs).
///
/// The shape is deliberately the frozen [`WindowManifest`]'s own field
/// vocabulary rather than a parallel one this judge invents: `part_kind` is
/// [`PartKind`], the arrival range is `Vec<OriginSeqRange>`, and both types
/// come from `duckspout-types` verbatim. A part's arrival range IS its
/// per-origin seq coverage (`docs/design/drain.md` §3: "arrival-window
/// placement"; §8: the manifest carries "per-origin seq coverage"), so
/// `SnapshotCovered`'s `CoversArrival(s, e)` is decidable from two of these
/// and nothing else.
///
/// # Which lines carry it
///
/// - **`Expire`** (§6.7): the part being retired. This is the evidence
///   `crate::predicates::retention_honesty` replays.
/// - **`SnapshotSeal`** / **`LakeCommitOk`** (§6.7, §6.4): a sealed snapshot
///   part and the commit that made it real. Read-back is the primary source
///   for "which snapshots are committed" (§8.4 judges "against read-back
///   state"), so a snapshot descriptor on a journal line is corroboration,
///   not the authority — with one exception the spec itself names: a
///   snapshot that was ITSELF later expired is gone from read-back but was
///   in the lake when it covered, which is why `SnapshotCovered`'s
///   surrounding invariants read `lake ∪ expired`
///   (`specs/formal-core.md`'s `Expire` note). The predicate therefore
///   unions read-back with journaled snapshot EXPIRIES, not with journaled
///   snapshot seals — a seal that never committed proves nothing.
///
/// [`WindowManifest`]: duckspout_types::WindowManifest
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PartRetention {
    /// The dataset the part belongs to.
    pub dataset: DatasetId,
    /// The partition the part belongs to — the scope `SnapshotCovered`
    /// quantifies a covering snapshot within (`s.part = e.part` in the §3
    /// formula, where the spec's `part` is the partition).
    pub partition: PartitionId,
    /// The part's deterministic object name (§6.5) — identity, and what a
    /// finding names so an operator can go look at the object.
    pub part: PartName,
    /// `primary` / `supplement` / `snapshot` (§6.2). Keep Rule 10 guards
    /// only the non-snapshot changelog parts: a snapshot expires under a
    /// NEWER covering snapshot, which is the same rule one level up, and the
    /// §3 guard spells the exclusion out as `e.kind # "snapshot"`.
    pub part_kind: PartKind,
    /// `event` or `changelog` (§2). Keep Rule 10 is a changelog obligation
    /// (`IsChangelogData(e)` in the §3 guard); an `event` part's expiry is
    /// plain age-based retention with nothing to cover
    /// (`docs/design/drain.md` §7).
    pub dataset_kind: DatasetKind,
    /// The part's arrival range: per-origin, contiguous seq coverage (§6.8).
    /// Never empty — `part_from_rest` rejects an empty coverage list,
    /// because a part naming no arrival range makes `CoversArrival` a
    /// vacuous truth and would let every expiry of such a part pass
    /// unexamined.
    pub origin_coverage: Vec<OriginSeqRange>,
}

/// One decoded journal line: the frozen envelope plus, when present, the
/// loadgen's payload identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalLine {
    /// The journal file this line came from (diagnostics only).
    pub source: PathBuf,
    /// 1-based line number within `source` (diagnostics only).
    pub line_no: usize,
    /// The journaling node (the loadgen fleet member's id, for
    /// loadgen-journaled lines).
    pub node: NodeId,
    /// The node-local dense sequence number (D-6).
    pub seq: u64,
    /// The journaled event.
    pub event: TraceEvent,
    /// Present exactly on lines carrying a `request_id` field — in
    /// practice the loadgen's own `ClientAck`/`ClientTimeout` lines (module
    /// docs).
    pub identity: Option<RequestIdentity>,
    /// Present exactly on lines carrying a `complete_through_ms` field
    /// (module docs' payload table).
    pub watermark: Option<WatermarkClaim>,
    /// Present exactly on lines carrying a `changelog_key` field (module
    /// docs' payload table).
    pub changelog: Option<ChangelogEntry>,
    /// Present exactly on lines carrying a `part` field (module docs'
    /// payload table).
    pub part: Option<PartRetention>,
}

/// A parsed, queryable set of journal lines from every ingested file
/// (§8.4: "reconstruction of the run's event history keyed by node/seq").
#[derive(Debug, Default, Clone)]
pub struct JournalSet {
    /// Every decoded line, in ingestion order (file order, then line order
    /// within each file) — NOT globally seq-sorted, since seqs are only
    /// dense per node, not across nodes.
    pub lines: Vec<JournalLine>,
}

impl JournalSet {
    /// Every line for `event` that carries a payload identity, in ingestion
    /// order — the primary lookup #205's zero-acked-lost predicate needs,
    /// and reusable by any future predicate keying on an identity-bearing
    /// event.
    pub fn identity_events(
        &self,
        event: TraceEvent,
    ) -> impl Iterator<Item = (&JournalLine, &RequestIdentity)> {
        self.lines.iter().filter_map(move |line| {
            if line.event == event {
                line.identity.as_ref().map(|identity| (line, identity))
            } else {
                None
            }
        })
    }

    /// Every watermark-claim-bearing line, of ANY event, in ingestion order
    /// — `crate::predicates::watermark_honesty` classifies each one by its
    /// event (advance vs. advertisement) itself, so this accessor
    /// deliberately does not pre-filter: a claim riding an event this
    /// module did not anticipate must still reach the predicate rather than
    /// be silently dropped here.
    pub fn watermark_claims(&self) -> impl Iterator<Item = (&JournalLine, &WatermarkClaim)> {
        self.lines
            .iter()
            .filter_map(|line| line.watermark.as_ref().map(|claim| (line, claim)))
    }

    /// Every part-descriptor-bearing line for `event`, in ingestion order —
    /// the accessor `retention_honesty` uses with [`TraceEvent::Expire`] to
    /// get exactly the parts retention actually retired.
    pub fn part_events(
        &self,
        event: TraceEvent,
    ) -> impl Iterator<Item = (&JournalLine, &PartRetention)> {
        self.lines.iter().filter_map(move |line| {
            if line.event == event {
                line.part.as_ref().map(|part| (line, part))
            } else {
                None
            }
        })
    }

    /// How many lines journaled a post-drain residency action —
    /// `Demote`/`Evict`/`DropWindow`, the three §3 actions that move a
    /// window between the staging class, the cache class, and nowhere (§2.4,
    /// §6.9). `crate::predicates::cache_transparency` cross-checks the
    /// eviction-storm evidence against this: a run whose read probes claim
    /// residency churn that NO node journaled is contradictory evidence, not
    /// a certifiable run.
    #[must_use]
    pub fn residency_action_count(&self) -> usize {
        self.lines
            .iter()
            .filter(|line| {
                matches!(
                    line.event,
                    TraceEvent::Demote | TraceEvent::Evict | TraceEvent::DropWindow
                )
            })
            .count()
    }

    /// Every changelog-entry-bearing line for `event`, in ingestion order —
    /// the mirror of [`JournalSet::identity_events`], and the accessor
    /// `latest_view` uses with [`TraceEvent::ClientAck`] to get exactly the
    /// ACKED changelog (a `ClientTimeout`-borne entry was never promised,
    /// so it is not a fold input).
    pub fn changelog_events(
        &self,
        event: TraceEvent,
    ) -> impl Iterator<Item = (&JournalLine, &ChangelogEntry)> {
        self.lines.iter().filter_map(move |line| {
            if line.event == event {
                line.changelog.as_ref().map(|entry| (line, entry))
            } else {
                None
            }
        })
    }
}

/// Ingestion failure — fails the run closed rather than skipping the bad
/// line (module docs).
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// The journal file could not be read at all.
    #[error("reading journal {path}: {source}")]
    Io {
        /// The journal file that failed to open/read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// One line was not a valid `{node, seq, event, ...}` object, or its
    /// identity-shaped extra fields did not decode as [`RequestIdentity`].
    #[error("{path}:{line_no}: not a valid journal line: {source}")]
    Decode {
        /// The journal file the bad line came from.
        path: PathBuf,
        /// The 1-based line number.
        line_no: usize,
        /// The underlying decode error.
        #[source]
        source: serde_json::Error,
    },
    /// A node's seq did not continue the dense, zero-based sequence D-6
    /// requires — a gap (lost lines), a repeat, or an out-of-order line.
    /// Checked BOTH within one file (`parse_journal_file`) and, since ACPR
    /// finding MEDIUM-HIGH-3(c), across every file ingested together
    /// (`ingest_journals`'s cross-file re-check) — so the same `(node, seq)`
    /// reappearing in a second file (a file passed twice, or a rotated
    /// journal re-fed by mistake) is a repeat, caught the same way a repeat
    /// within one file would be, rather than silently double-counted.
    #[error(
        "{path}:{line_no}: node {node} seq {got}, expected {expected} \
         (D-6: dense per-node seqs starting at 0, tracked across every \
         journal file ingested together)"
    )]
    NonDenseSeq {
        /// The journal file the bad line came from.
        path: PathBuf,
        /// The 1-based line number.
        line_no: usize,
        /// The offending node.
        node: NodeId,
        /// The seq the writer's own discipline required next.
        expected: u64,
        /// The seq the line actually carried.
        got: u64,
    },
}

/// The frozen envelope every journal line carries, with everything else
/// (the loadgen's identity fields, if present) captured verbatim so it can
/// be decoded a second time, more strictly, only when it is actually there.
#[derive(Deserialize)]
struct Envelope {
    node: NodeId,
    seq: u64,
    event: TraceEvent,
    #[serde(flatten)]
    rest: serde_json::Value,
}

/// Decodes `rest` (whatever is left on the line after `node`/`seq`/`event`)
/// into a [`RequestIdentity`] when it looks like one, by the presence of
/// `request_id` (module docs' "structurally, by field presence" rule). A
/// `rest` with no `request_id` field is a plain envelope line (e.g. a
/// node-journaled `ClientAck`, or any payload-free event) — not an error.
/// A `rest` WITH a `request_id` field that does not fully decode as
/// [`RequestIdentity`] IS an error: a half-formed identity is corruption,
/// not "no identity here."
///
/// Also rejects, as the same kind of corruption (ACPR finding
/// MEDIUM-HIGH-3(b)): a `first_index`/`record_count` pair whose sum
/// overflows `u64`. Left unchecked, a predicate computing
/// `first_index..first_index + record_count` over this identity would
/// either panic (debug) or silently wrap to an empty, vacuously-passing
/// range (release) — neither is the fail-closed contract this module
/// promises, so the check happens once, here, at decode time, rather than
/// trusting every future caller to redo it correctly.
fn identity_from_rest(
    rest: &serde_json::Value,
) -> Result<Option<RequestIdentity>, serde_json::Error> {
    match rest {
        serde_json::Value::Object(map) if map.contains_key("request_id") => {
            let identity: RequestIdentity = serde_json::from_value(rest.clone())?;
            if identity
                .first_index
                .checked_add(identity.record_count as u64)
                .is_none()
            {
                return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "first_index {} + record_count {} overflows u64 — fails closed rather \
                     than panicking or silently wrapping to an empty range",
                    identity.first_index, identity.record_count
                )));
            }
            Ok(Some(identity))
        }
        _ => Ok(None),
    }
}

/// Decodes `rest` into a [`WatermarkClaim`] when it carries a
/// `complete_through_ms` field, under exactly the rules
/// [`identity_from_rest`] applies to its own key field: presence decides
/// that a claim is being made, and a claim that does not fully decode is
/// corruption, not "no claim here".
fn watermark_from_rest(
    rest: &serde_json::Value,
) -> Result<Option<WatermarkClaim>, serde_json::Error> {
    match rest {
        serde_json::Value::Object(map) if map.contains_key("complete_through_ms") => {
            Ok(Some(serde_json::from_value(rest.clone())?))
        }
        _ => Ok(None),
    }
}

/// Decodes `rest` into a [`ChangelogEntry`] when it carries a
/// `changelog_key` field ([`identity_from_rest`]'s rules again), and
/// additionally rejects the two self-contradictory content shapes at decode
/// time so no predicate has to re-derive what a well-formed entry is:
///
/// - `tombstone: true` WITH a `value`: a delete that also carries content
///   folds to two different answers depending on which field a reader
///   believes.
/// - `tombstone: false` with NO `value`: an upsert of nothing. Folding it
///   would either resurrect a key with an unknown value or silently behave
///   like a delete — the fold's own definition (§7.7: tombstones delete,
///   everything else is the row) has no third case.
fn changelog_from_rest(
    rest: &serde_json::Value,
) -> Result<Option<ChangelogEntry>, serde_json::Error> {
    match rest {
        serde_json::Value::Object(map) if map.contains_key("changelog_key") => {
            let entry: ChangelogEntry = serde_json::from_value(rest.clone())?;
            match (entry.tombstone, entry.value.is_some()) {
                (true, true) => Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "changelog entry for key {:?} is a tombstone AND carries a value — \
                     ambiguity fails closed rather than folding to whichever field is read \
                     first",
                    entry.changelog_key
                ))),
                (false, false) => Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "changelog entry for key {:?} is neither a tombstone nor carries a value — \
                     the §7.7 fold has no third case, so this is corruption, not an input",
                    entry.changelog_key
                ))),
                _ => Ok(Some(entry)),
            }
        }
        _ => Ok(None),
    }
}

/// Decodes `rest` into a [`PartRetention`] when it carries a `part` field
/// ([`identity_from_rest`]'s rules again), and additionally rejects the two
/// coverage shapes that would make `SnapshotCovered` undecidable rather than
/// false — both at decode time, so no predicate has to re-derive what a
/// well-formed part descriptor is:
///
/// - **an empty `origin_coverage`**: a part naming no arrival range is
///   covered by every snapshot vacuously, including by none at all, so
///   accepting it would let a real uncovered expiry through as a silent
///   pass. This is exactly the vacuity §8.4 forbids, at the evidence layer
///   rather than the verdict layer.
/// - **an inverted range** (`first_seq > last_seq`): `first_seq..=last_seq`
///   is empty for such a pair, so containment tests answer "covered" for
///   every snapshot and "contains" for no record — the same vacuous pass in
///   a different disguise.
fn part_from_rest(rest: &serde_json::Value) -> Result<Option<PartRetention>, serde_json::Error> {
    match rest {
        serde_json::Value::Object(map) if map.contains_key("part") => {
            let part: PartRetention = serde_json::from_value(rest.clone())?;
            if part.origin_coverage.is_empty() {
                return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "part {} names no origin coverage — a part with no arrival range is \
                     vacuously covered by every snapshot (and by none), so this fails closed \
                     rather than passing SnapshotCovered unexamined",
                    part.part
                )));
            }
            if let Some(bad) = part
                .origin_coverage
                .iter()
                .find(|range| range.first_seq > range.last_seq)
            {
                return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "part {} carries an inverted arrival range for origin {} ({} > {}) — an \
                     empty range is vacuously covered, so ambiguity fails closed",
                    part.part, bad.origin, bad.first_seq, bad.last_seq
                )));
            }
            Ok(Some(part))
        }
        _ => Ok(None),
    }
}

/// A structural check for a duplicate JSON object key at the TOP LEVEL of
/// one line (ACPR finding MEDIUM-HIGH-3(d)): `serde_json`'s own `Map` (the
/// `preserve_order` feature included) silently keeps "last value wins" on a
/// repeated key, with no built-in opt-in to reject it instead. That is
/// dangerous here specifically: a line with `tenant` duplicated could
/// silently reclassify a real tenant's ack as the system tenant `_self` (or
/// vice versa) and have it wrongly excluded or included.
///
/// Reasonable-effort fix, not a general JSON-validation library: every
/// journal line this crate ever decodes is one FLAT object (`node`/`seq`/
/// `event` plus, at most, the identity fields alongside them — module
/// docs' wire shape) — none of them nest a key one level deeper — so this
/// only walks the top-level object's keys (via a `serde::de::Visitor` that
/// never *builds* a map, so a duplicate key is never lost to the same
/// collapsing a normal deserialize would do) and discards each value with
/// [`serde::de::IgnoredAny`] rather than recursing into it. If the wire
/// shape ever grows a nested object, a duplicate key one level down would
/// not be caught by this check — an intentionally narrow scope matching
/// what this format actually is today, not a claim of full generality.
struct RejectDuplicateKeys;

impl<'de> serde::de::Deserialize<'de> for RejectDuplicateKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct DupKeyVisitor;

        impl<'de> serde::de::Visitor<'de> for DupKeyVisitor {
            type Value = ();

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut seen = std::collections::HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate JSON key {key:?} (ambiguity fails closed — \
                             last-value-wins could silently reclassify identity fields)"
                        )));
                    }
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(())
            }
        }

        deserializer.deserialize_map(DupKeyVisitor).map(|()| Self)
    }
}

/// Rejects one NDJSON line that repeats a top-level JSON key
/// ([`RejectDuplicateKeys`]'s reasoning). Shared with `crate::read_log`,
/// whose lines are the same flat one-object-per-line shape and carry the
/// same hazard (a repeated `concern` key silently downgrading a `complete`
/// read to `available`, or vice versa).
///
/// # Errors
///
/// The `serde_json` error naming the duplicated key, or any parse error the
/// line has on its own.
pub(crate) fn reject_duplicate_keys(raw: &str) -> Result<(), serde_json::Error> {
    serde_json::from_str::<RejectDuplicateKeys>(raw).map(|_| ())
}

/// Parses one journal file's lines, fully — the first malformed line fails
/// the whole file (module docs).
///
/// # Errors
///
/// Returns [`JournalError`] on the first I/O failure, undecodable line, or
/// seq-density violation.
pub fn parse_journal_file(path: &Path) -> Result<Vec<JournalLine>, JournalError> {
    let text = std::fs::read_to_string(path).map_err(|source| JournalError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut next_seq: HashMap<NodeId, u64> = HashMap::new();
    let mut lines = Vec::new();
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        // No special-casing for a blank line: `str::lines` never emits a
        // trailing empty element for a `\n`-terminated string (every writer
        // here flushes exactly one `\n` per line, `duckspout_ctk::trace_writer`
        // / `duckspout_loadgen::journal` module docs), so an empty `raw`
        // here can only mean a genuinely blank line *inside* the file —
        // which `serde_json::from_str` below already rejects on its own
        // (an empty string is not a JSON object), failing this line closed
        // exactly like any other malformed one rather than needing a
        // separate branch.
        reject_duplicate_keys(raw).map_err(|source| JournalError::Decode {
            path: path.to_owned(),
            line_no,
            source,
        })?;
        let envelope: Envelope =
            serde_json::from_str(raw).map_err(|source| JournalError::Decode {
                path: path.to_owned(),
                line_no,
                source,
            })?;
        let expected = *next_seq.get(&envelope.node).unwrap_or(&0);
        if envelope.seq != expected {
            return Err(JournalError::NonDenseSeq {
                path: path.to_owned(),
                line_no,
                node: envelope.node,
                expected,
                got: envelope.seq,
            });
        }
        next_seq.insert(envelope.node.clone(), expected + 1);
        let decode_error = |source| JournalError::Decode {
            path: path.to_owned(),
            line_no,
            source,
        };
        let identity = identity_from_rest(&envelope.rest).map_err(decode_error)?;
        let watermark = watermark_from_rest(&envelope.rest).map_err(decode_error)?;
        let changelog = changelog_from_rest(&envelope.rest).map_err(decode_error)?;
        let part = part_from_rest(&envelope.rest).map_err(decode_error)?;
        lines.push(JournalLine {
            source: path.to_owned(),
            line_no,
            node: envelope.node,
            seq: envelope.seq,
            event: envelope.event,
            identity,
            watermark,
            changelog,
            part,
        });
    }
    Ok(lines)
}

/// Ingests every journal file in `paths` into one queryable [`JournalSet`]
/// (§8.4). The first malformed line, in any file, fails the whole
/// ingestion — never a partial or silently-skipped result (module docs).
///
/// Each file's OWN seq density is checked per-file by [`parse_journal_file`]
/// (starting at 0 there, since one file may legitimately be one node's
/// complete, self-contained journal). This function additionally re-checks
/// density ACROSS every file, in ingestion order (ACPR finding
/// MEDIUM-HIGH-3(c)): `parse_journal_file`'s own per-file check cannot catch
/// the same file being passed twice, or a rotated/split journal fed
/// out-of-order or duplicated, because each file looks internally dense
/// starting from 0 on its own — the bug is only visible once seqs are
/// tracked per node across the WHOLE run.
///
/// # Errors
///
/// Returns the first [`JournalError`] encountered: an I/O or decode failure
/// from an individual file (in `paths` order), or a cross-file
/// [`JournalError::NonDenseSeq`] (in ingestion order) if no single file had
/// one.
pub fn ingest_journals(paths: &[PathBuf]) -> Result<JournalSet, JournalError> {
    let mut lines = Vec::new();
    for path in paths {
        lines.extend(parse_journal_file(path)?);
    }
    check_cross_file_density(&lines)?;
    Ok(JournalSet { lines })
}

/// The cross-file half of `ingest_journals`'s seq-density check (module
/// docs): replays every line, in ingestion order, tracking each node's next
/// expected seq across ALL files together rather than per file.
fn check_cross_file_density(lines: &[JournalLine]) -> Result<(), JournalError> {
    let mut next_seq: HashMap<NodeId, u64> = HashMap::new();
    for line in lines {
        let expected = *next_seq.get(&line.node).unwrap_or(&0);
        if line.seq != expected {
            return Err(JournalError::NonDenseSeq {
                path: line.source.clone(),
                line_no: line.line_no,
                node: line.node.clone(),
                expected,
                got: line.seq,
            });
        }
        next_seq.insert(line.node.clone(), expected + 1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn write_journal(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(text.as_bytes()).expect("write");
        file
    }

    #[test]
    fn parses_plain_node_lines_with_no_identity() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"ClientAck\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        assert_eq!(lines.len(), 3);
        assert!(lines.iter().all(|l| l.identity.is_none()));
        assert_eq!(lines[2].event, TraceEvent::ClientAck);
    }

    #[test]
    fn extracts_identity_from_loadgen_client_ack_lines() {
        // Would catch the identity-extraction logic silently treating the
        // loadgen's own richer `ClientAck` line as a plain envelope line —
        // the exact shape `duckspout_loadgen::journal::LoadgenJournal`
        // produces.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"tenant-a\",\
             \"record_count\":7,\"first_index\":42,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let identity = lines[0].identity.as_ref().expect("identity present");
        assert_eq!(identity.request_id, "req-1");
        assert_eq!(identity.tenant, "tenant-a");
        assert_eq!(identity.record_count, 7);
        assert_eq!(identity.first_index, 42);
        assert_eq!(identity.source_incarnation, "loadgen-0-1000");
    }

    #[test]
    fn a_half_formed_identity_is_a_decode_error_not_a_silent_downgrade() {
        // Would catch treating corruption (a `request_id` present but the
        // rest of the identity shape missing/wrong-typed) as "just no
        // identity here" — ambiguity must fail closed, not get quietly
        // reinterpreted as a plain envelope line.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn malformed_json_fails_closed() {
        let file = write_journal("{not json}\n");
        let err = parse_journal_file(file.path()).expect_err("must fail");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_blank_line_fails_closed_rather_than_being_silently_skipped() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail on the blank line");
        assert!(matches!(err, JournalError::Decode { line_no: 2, .. }));
    }

    #[test]
    fn a_seq_gap_fails_closed() {
        // Would catch silently accepting a journal with a missing line
        // (e.g. a torn write that dropped one event) as if it were complete.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"StageCommit\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail on the gap");
        assert!(matches!(
            err,
            JournalError::NonDenseSeq {
                line_no: 2,
                expected: 1,
                got: 2,
                ..
            }
        ));
    }

    #[test]
    fn each_node_keeps_its_own_dense_seq_within_one_file() {
        // Multiple nodes' lines can legitimately interleave inside one
        // captured stream; D-6's density is per-node, not global.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n2\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n\
             {\"node\":\"n2\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        assert_eq!(lines.len(), 4);
    }

    #[test]
    fn ingest_journals_aggregates_multiple_files_in_order() {
        let f1 = write_journal("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let f2 = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"t\",\"record_count\":1,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        let set = ingest_journals(&[f1.path().to_owned(), f2.path().to_owned()]).expect("ingests");
        assert_eq!(set.lines.len(), 2);
        assert_eq!(set.identity_events(TraceEvent::ClientAck).count(), 1);
    }

    #[test]
    fn ingest_journals_fails_closed_if_any_file_is_bad() {
        // Would catch a partial-ingestion bug where a later good file's
        // lines get returned even though an earlier file was corrupt —
        // exactly the "skipped ≠ passed" gap this module must not have.
        let good = write_journal("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let bad = write_journal("not json at all\n");
        let err = ingest_journals(&[good.path().to_owned(), bad.path().to_owned()])
            .expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { .. }));
    }

    #[test]
    fn a_missing_journal_file_is_an_io_error() {
        let missing = PathBuf::from("/nonexistent/duckspout-judge-test-journal.ndjson");
        let err = parse_journal_file(&missing).expect_err("must fail");
        assert!(matches!(err, JournalError::Io { .. }));
    }

    #[test]
    fn an_overflowing_index_range_is_a_decode_error_not_a_panic_or_wraparound() {
        // ACPR finding MEDIUM-HIGH-3(b): would catch the predicate's range
        // arithmetic panicking (debug) or silently wrapping to an empty,
        // vacuously-passing range (release) instead of failing closed.
        let file = write_journal(&format!(
            "{{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"tenant-a\",\
             \"record_count\":10,\"first_index\":{},\
             \"source_incarnation\":\"loadgen-0-1000\"}}\n",
            u64::MAX - 1
        ));
        let err = parse_journal_file(file.path()).expect_err("must fail closed on overflow");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_duplicate_json_key_fails_closed() {
        // ACPR finding MEDIUM-HIGH-3(d): a duplicated `tenant` key could
        // silently reclassify a real tenant's ack as the system tenant
        // (last-value-wins) — this must be rejected, not silently resolved.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"tenant-a\",\"tenant\":\"_self\",\
             \"record_count\":1,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed on duplicate key");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_repeated_node_seq_across_two_files_fails_closed() {
        // ACPR finding MEDIUM-HIGH-3(c): the same file passed twice (or a
        // rotated journal re-fed by mistake) must not be silently
        // double-counted just because each file looks dense-from-0 on its
        // own — density must hold across the whole ingested run.
        let f1 = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let f2 = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"StageCommit\"}\n",
        );
        let err = ingest_journals(&[f1.path().to_owned(), f2.path().to_owned()])
            .expect_err("must fail closed on cross-file repeat");
        assert!(matches!(
            err,
            JournalError::NonDenseSeq {
                expected: 2,
                got: 0,
                ..
            }
        ));
    }

    #[test]
    fn extracts_a_watermark_claim_from_a_lake_commit_line() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"LakeCommitOk\",\
             \"partition\":\"t0-s0\",\"complete_through_ms\":1700}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let claim = lines[0].watermark.as_ref().expect("claim present");
        assert_eq!(claim.partition, PartitionId::new("t0-s0"));
        assert_eq!(claim.complete_through_ms, 1700);
        assert!(lines[0].changelog.is_none());
        assert!(lines[0].identity.is_none());
    }

    #[test]
    fn a_half_formed_watermark_claim_is_a_decode_error_not_a_silent_downgrade() {
        // Would catch a line that CLAIMS coverage (`complete_through_ms`
        // present) but names no partition being read as "no watermark
        // here" — silently dropping the one claim the Q-shaped judge exists
        // to replay.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"ClaimAdvertise\",\"complete_through_ms\":1700}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn extracts_a_changelog_entry_from_a_client_ack_line() {
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"dim_users\",\"partition\":\"tenant-a.3\",\"tenant\":\"tenant-a\",\
             \"changelog_key\":\"u1\",\"origin\":\"n1\",\
             \"changelog_seq\":7,\"value\":\"alice\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let entry = lines[0].changelog.as_ref().expect("entry present");
        assert_eq!(entry.dataset, DatasetId::new("dim_users"));
        assert_eq!(entry.partition, PartitionId::new("tenant-a.3"));
        assert_eq!(entry.tenant, TenantId::new("tenant-a"));
        assert_eq!(entry.changelog_key, "u1");
        assert_eq!(entry.origin, NodeId::new("n1"));
        assert_eq!(entry.changelog_seq, 7);
        assert!(!entry.tombstone);
        assert_eq!(entry.value.as_deref(), Some("alice"));
    }

    #[test]
    fn a_changelog_entry_naming_no_partition_or_no_tenant_fails_closed() {
        // ACPR finding HIGH-1: `changelog_seq` is dense per
        // `(partition, origin)` and `changelog_key` is unique only within a
        // tenant, so an entry missing either field cannot be placed in the
        // fold at all. Accepting it — by defaulting the missing dimension —
        // is exactly what collided two partitions' records onto one dedup
        // slot and two tenants' rows onto one fold key; a half-formed entry
        // must fail closed like every other half-formed payload here.
        for line in [
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"tenant\":\"t\",\"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":1,\"value\":\"v\"}\n",
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":1,\"value\":\"v\"}\n",
        ] {
            let file = write_journal(line);
            let err = parse_journal_file(file.path()).expect_err("must fail closed");
            assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
        }
    }

    #[test]
    fn a_changelog_record_sequence_is_not_confused_with_the_envelopes_own_seq() {
        // D-6's per-node trace seq and the origin-assigned record seq are
        // different numbers that would collide if the payload spelled its
        // field `seq`: this pins that the fold reads `changelog_seq`, not
        // the envelope's line counter.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"tenant\":\"t\",\
             \"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":42,\"value\":\"v\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        assert_eq!(lines[0].seq, 0);
        assert_eq!(
            lines[0].changelog.as_ref().expect("entry").changelog_seq,
            42
        );
    }

    #[test]
    fn a_tombstone_carrying_a_value_fails_closed() {
        // The fold would answer "key deleted" or "key = v" depending on
        // which field it read first — exactly the ambiguity that must never
        // reach a predicate.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"tenant\":\"t\",\
             \"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":1,\"tombstone\":true,\"value\":\"v\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_non_tombstone_with_no_value_fails_closed() {
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"tenant\":\"t\",\
             \"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":1}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_tombstone_with_no_value_decodes_as_a_delete() {
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"tenant\":\"t\",\
             \"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":1,\"tombstone\":true}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let entry = lines[0].changelog.as_ref().expect("entry present");
        assert!(entry.tombstone);
        assert!(entry.value.is_none());
    }

    #[test]
    fn an_ack_identity_carries_its_optional_coverage_fields_when_present() {
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"t\",\"record_count\":2,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\",\"partition\":\"t0-s0\",\
             \"max_event_time_ms\":1500}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let identity = lines[0].identity.as_ref().expect("identity present");
        assert_eq!(identity.partition, Some(PartitionId::new("t0-s0")));
        assert_eq!(identity.max_event_time_ms, Some(1500));
    }

    #[test]
    fn an_ack_identity_without_the_optional_coverage_fields_still_decodes() {
        // Non-regression for every journal written before #206 (and for
        // what `duckspout-loadgen` writes today): the new fields are
        // absent, which is "no coverage evidence", never a decode failure.
        let file = write_journal(
            "{\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"request_id\":\"req-1\",\"tenant\":\"t\",\"record_count\":2,\"first_index\":0,\
             \"source_incarnation\":\"loadgen-0-1000\"}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let identity = lines[0].identity.as_ref().expect("identity present");
        assert_eq!(identity.partition, None);
        assert_eq!(identity.max_event_time_ms, None);
    }

    #[test]
    fn the_accessors_select_only_their_own_payload_and_event() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"LakeCommitOk\",\
             \"partition\":\"p\",\"complete_through_ms\":10}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"ClaimAdvertise\",\
             \"partition\":\"p\",\"complete_through_ms\":10}\n\
             {\"node\":\"loadgen-0\",\"seq\":0,\"event\":\"ClientAck\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"tenant\":\"t\",\
             \"changelog_key\":\"k\",\"origin\":\"n1\",\
             \"changelog_seq\":1,\"value\":\"v\"}\n\
             {\"node\":\"loadgen-0\",\"seq\":1,\"event\":\"ClientTimeout\",\
             \"dataset\":\"d\",\"partition\":\"t.0\",\"tenant\":\"t\",\
             \"changelog_key\":\"k2\",\"origin\":\"n1\",\
             \"changelog_seq\":2,\"value\":\"v2\"}\n",
        );
        let set = JournalSet {
            lines: parse_journal_file(file.path()).expect("parses"),
        };
        assert_eq!(set.watermark_claims().count(), 2);
        assert_eq!(set.changelog_events(TraceEvent::ClientAck).count(), 1);
        assert_eq!(set.changelog_events(TraceEvent::ClientTimeout).count(), 1);
        assert_eq!(set.identity_events(TraceEvent::ClientAck).count(), 0);
    }

    /// One `Expire` line's part descriptor, with every field the §3 guard
    /// reads: the partition it fences within, the kind that decides whether
    /// Keep Rule 10 applies at all, the dataset kind that decides the same,
    /// and the arrival range a snapshot must cover.
    #[test]
    fn extracts_a_part_descriptor_from_an_expire_line() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Expire\",\
             \"dataset\":\"dim_users\",\"partition\":\"tenant-a.4\",\
             \"part\":\"dim_users/tenant-a.4/7/primary/0.parquet\",\
             \"part_kind\":\"primary\",\"dataset_kind\":\"changelog\",\
             \"origin_coverage\":[{\"origin\":\"n1\",\"first_seq\":1,\"last_seq\":9}]}\n",
        );
        let lines = parse_journal_file(file.path()).expect("parses");
        let part = lines[0].part.as_ref().expect("part present");
        assert_eq!(part.dataset, DatasetId::new("dim_users"));
        assert_eq!(part.partition, PartitionId::new("tenant-a.4"));
        assert_eq!(part.part_kind, PartKind::Primary);
        assert_eq!(part.dataset_kind, DatasetKind::Changelog);
        assert_eq!(part.origin_coverage.len(), 1);
        assert_eq!(part.origin_coverage[0].last_seq, 9);
        assert!(lines[0].changelog.is_none());
        assert!(lines[0].watermark.is_none());
    }

    #[test]
    fn a_half_formed_part_descriptor_is_a_decode_error_not_a_silent_downgrade() {
        // Would catch a line that NAMES a part (`part` present) but omits
        // the kind that decides whether Keep Rule 10 applies to it being
        // read as "no part here" — silently dropping the one expiry the
        // retention judge exists to replay.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Expire\",\
             \"dataset\":\"d\",\"partition\":\"p\",\"part\":\"obj\"}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_part_with_no_arrival_range_fails_closed() {
        // The vacuity tooth at the evidence layer (`part_from_rest`'s own
        // docs): an empty coverage list is covered by every snapshot AND by
        // none, so accepting it would turn a genuinely uncovered expiry into
        // a silent pass — precisely the shape §8.4 forbids.
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Expire\",\
             \"dataset\":\"d\",\"partition\":\"p\",\"part\":\"obj\",\
             \"part_kind\":\"primary\",\"dataset_kind\":\"changelog\",\
             \"origin_coverage\":[]}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn a_part_with_an_inverted_arrival_range_fails_closed() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Expire\",\
             \"dataset\":\"d\",\"partition\":\"p\",\"part\":\"obj\",\
             \"part_kind\":\"primary\",\"dataset_kind\":\"changelog\",\
             \"origin_coverage\":[{\"origin\":\"n1\",\"first_seq\":9,\"last_seq\":1}]}\n",
        );
        let err = parse_journal_file(file.path()).expect_err("must fail closed");
        assert!(matches!(err, JournalError::Decode { line_no: 1, .. }));
    }

    #[test]
    fn part_events_and_residency_actions_select_only_their_own_evidence() {
        let file = write_journal(
            "{\"node\":\"n1\",\"seq\":0,\"event\":\"Expire\",\
             \"dataset\":\"d\",\"partition\":\"p\",\"part\":\"obj-1\",\
             \"part_kind\":\"primary\",\"dataset_kind\":\"changelog\",\
             \"origin_coverage\":[{\"origin\":\"n1\",\"first_seq\":1,\"last_seq\":2}]}\n\
             {\"node\":\"n1\",\"seq\":1,\"event\":\"SnapshotSeal\",\
             \"dataset\":\"d\",\"partition\":\"p\",\"part\":\"snap-1\",\
             \"part_kind\":\"snapshot\",\"dataset_kind\":\"changelog\",\
             \"origin_coverage\":[{\"origin\":\"n1\",\"first_seq\":1,\"last_seq\":5}]}\n\
             {\"node\":\"n1\",\"seq\":2,\"event\":\"DropWindow\"}\n\
             {\"node\":\"n1\",\"seq\":3,\"event\":\"Evict\"}\n\
             {\"node\":\"n1\",\"seq\":4,\"event\":\"Accept\"}\n",
        );
        let set = JournalSet {
            lines: parse_journal_file(file.path()).expect("parses"),
        };
        assert_eq!(set.part_events(TraceEvent::Expire).count(), 1);
        assert_eq!(set.part_events(TraceEvent::SnapshotSeal).count(), 1);
        // `Accept` never carries a part; `DropWindow`/`Evict` are counted by
        // the residency accessor, not by the part one.
        assert_eq!(set.part_events(TraceEvent::Accept).count(), 0);
        assert_eq!(set.residency_action_count(), 2);
    }

    #[test]
    fn distinct_nodes_across_files_are_unaffected_by_the_cross_file_check() {
        // The non-regression case for the same fix: files covering
        // DIFFERENT nodes must aggregate normally — the cross-file density
        // check must not spuriously conflate unrelated nodes' seq counters.
        let f1 = write_journal("{\"node\":\"n1\",\"seq\":0,\"event\":\"Accept\"}\n");
        let f2 = write_journal("{\"node\":\"n2\",\"seq\":0,\"event\":\"Accept\"}\n");
        let set = ingest_journals(&[f1.path().to_owned(), f2.path().to_owned()]).expect("ingests");
        assert_eq!(set.lines.len(), 2);
    }
}
