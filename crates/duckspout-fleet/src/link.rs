//! Real network fault links (§8.4, issue #204): a **userspace TCP proxy**
//! the fleet runner owns, sitting on the real byte path between a fleet
//! member and one of its real counterparties, whose forwarding condition
//! can be changed live — the mechanism `crate::fault`'s network-partition,
//! asymmetric-degradation, catalog-outage and discovery-flapping injectors
//! all fire through.
//!
//! # Why a proxy, and not `iptables`/`tc netem`
//!
//! §8.4's fault list ("network partitions and asymmetric degradation
//! (drops, delay, bandwidth caps)") names effects, not mechanisms. Three
//! mechanisms can produce them against real local processes:
//!
//! 1. **`iptables`/`nftables` rules** (DROP/REJECT on a port) and **`tc
//!    netem`** (delay, rate). Genuinely real, but both need `CAP_NET_ADMIN`
//!    — root, or a privileged container — and both mutate **host-global**
//!    kernel state that outlives a crashed fleet run. On a shared CI runner
//!    (or a developer box) a fleet run that died between "add DROP rule" and
//!    "delete DROP rule" leaves the host firewalling a port nobody can
//!    explain. `tc` additionally attaches to an interface, not a port, so a
//!    loopback `netem` delay degrades **every** co-located node at once —
//!    it cannot express "node 2's link is slow, node 1's is not," which is
//!    exactly what "asymmetric" asks for.
//! 2. **A userspace TCP proxy** (this module). Real TCP, real connections,
//!    real bytes, real resets — no privileges at all, and every effect dies
//!    with the fleet process that owns it (the accept loop is a task; the
//!    listener closes on drop). Per-link and per-direction by construction,
//!    which is what makes ASYMMETRIC degradation expressible.
//! 3. **A fault seam inside the daemon.** Rejected here: it would make the
//!    node aware of its own fault schedule — `crate::faultlog`'s own module
//!    docs on why that is the wrong shape for an uninstrumented failure
//!    mode. (#203's `--fault-drain-commit-delay-ms` is the deliberate,
//!    narrow exception: it widens a real window's *timing*, it does not
//!    simulate a failure.)
//!
//! (2) is what this module implements. It is not a simulation: the daemon
//! really opens a real TCP connection, really writes real bytes into it,
//! and really observes a real connection reset / real added latency /
//! real backpressure. What it is NOT is a *kernel-level* packet drop: see
//! "What a proxy cannot reproduce" below for the honest boundary.
//!
//! # Why [`LinkCondition::Drop`] resets rather than silently blackholes
//!
//! A true packet-level blackhole (kernel `DROP`) leaves the sender waiting
//! out TCP retransmission timeouts — minutes, not seconds — so the effect
//! of a 10-second fault window would still be unfolding long after the
//! window's journaled `Ended` line. §8.4's contract is "each window
//! journaled with **start/end**"; a window whose end does not correspond to
//! the end of its effect makes that journal a lie. [`LinkCondition::Drop`]
//! is therefore REJECT-shaped: while it is set, new connections are
//! accepted and immediately closed and in-flight connections are cut, so
//! the disruption starts and stops inside the journaled window. A caller
//! that wants "the peer hangs" can use a large [`LinkCondition::Delay`]
//! instead, which stalls the byte path without tearing it down.
//!
//! # Overlapping fault windows: conditions are HELD, never assigned
//!
//! Two armed faults can legitimately share one link — `--fault-catalog-
//! outage-node 1` and `--fault-discovery-flap-node 1` both condition node
//! 1's catalog link, and `crate::main`'s `run_network_faults` runs every
//! armed window CONCURRENTLY (a real chaos schedule overlaps its windows).
//! A last-writer-wins `set`/`restore` pair would let the flap's per-cycle
//! restore lift the outage's cut while the outage window is still journaled
//! open — the journal would claim a cut that is not in effect, which is the
//! one thing a start/end-journaled window must never do (an ACPR finding on
//! issue #204, HIGH-1).
//!
//! So a condition is not assigned, it is **held**: [`FaultLink::hold`]
//! returns a [`LinkHold`], and the link's effective condition is the
//! most restrictive condition across every hold currently outstanding
//! ([`LinkCondition::most_restrictive`]). A window lifts only its OWN hold,
//! and the link relaxes only when the LAST hold covering it is released.
//! Refcounting rather than making the overlapping flag combinations
//! mutually exclusive is the deliberate choice: §8.4's schedule is meant to
//! overlap windows, and each new fault class would otherwise need another
//! hand-maintained exclusion rule.
//!
//! # What a proxy cannot reproduce (stated, not papered over)
//!
//! - **Half-open / asymmetric CONNECTIVITY.** Per-direction conditions here
//!   are per-direction *byte handling* on an established connection
//!   ([`LinkConditions`] carries one condition per direction, which is what
//!   makes delay/bandwidth asymmetry real). A connection whose SYN gets
//!   through one way but whose replies are dropped — a genuinely
//!   half-partitioned network — is not expressible: a TCP proxy either
//!   completes both legs or neither.
//! - **Packet-level loss/reordering.** `netem`'s `loss 5%` reorders and
//!   drops individual segments below the stream abstraction; a proxy moves
//!   byte ranges, so it can drop *everything* (cut) or slow the stream, but
//!   not corrupt a stream in place.
//! - **Traffic this fleet does not route through a link.** Today that is
//!   node↔node peer traffic, because there is none: no crate in this
//!   workspace implements `duckspout_types::Transport` over a real network
//!   yet (`duckspout-daemon`'s `wiring.rs` module docs). The links this
//!   crate creates are the real network edges that DO exist — client→node
//!   ingest, node→Postgres catalog, node→S3/`MinIO` lake.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use serde::Serialize;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// How one direction of a [`FaultLink`] currently forwards bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "condition", rename_all = "snake_case")]
pub enum LinkCondition {
    /// Forward immediately, unmodified — the link is a plain byte pump.
    Pass,
    /// Refuse the link: new connections are accepted then immediately
    /// closed, and every in-flight connection is cut (module docs on why
    /// this is REJECT-shaped rather than a silent blackhole).
    Drop,
    /// Forward, but hold each chunk for `ms` first — real added latency on
    /// the real byte path.
    Delay { ms: u64 },
    /// Forward, paced to at most `bytes_per_sec` — a real bandwidth cap,
    /// applied by sleeping proportionally to each forwarded chunk's size
    /// (which is what makes it exert real TCP backpressure on the sender,
    /// rather than merely buffering).
    BandwidthCap { bytes_per_sec: u64 },
}

/// The byte pump's read/forward chunk size — also the reference size
/// [`LinkCondition::most_restrictive`] compares two pacing conditions at.
const CHUNK: usize = 16 * 1024;

impl LinkCondition {
    /// Which of two conditions held on the same direction at the same time
    /// actually applies: the more restrictive one, so a window can never be
    /// relaxed by another window's condition (module docs).
    ///
    /// `Drop` dominates everything (a link nobody may cross is not made
    /// crossable by a second fault that merely slows it); otherwise the
    /// condition that holds a full [`CHUNK`] longer wins, which orders
    /// `Pass` (no hold) below every delay and every cap, and orders a delay
    /// against a cap by their real effect on one forwarded chunk.
    fn most_restrictive(self, other: Self) -> Self {
        match (self, other) {
            (Self::Drop, _) | (_, Self::Drop) => Self::Drop,
            _ if other.pace(CHUNK) > self.pace(CHUNK) => other,
            _ => self,
        }
    }

    /// How long to hold a chunk of `bytes` bytes before forwarding it.
    fn pace(self, bytes: usize) -> Duration {
        match self {
            Self::Pass | Self::Drop => Duration::ZERO,
            Self::Delay { ms } => Duration::from_millis(ms),
            Self::BandwidthCap { bytes_per_sec } => {
                if bytes_per_sec == 0 {
                    // Not "infinitely slow" (which would hang the fleet run
                    // past its own fault window, the exact failure mode the
                    // module docs reject for `Drop`): a zero cap is a
                    // misconfiguration, treated as no pacing at all so the
                    // journal's own byte counters make it obvious.
                    Duration::ZERO
                } else {
                    // Integer nanoseconds, not float seconds: exact for
                    // every chunk size a 16 KiB pump can produce, and no
                    // precision-loss cast.
                    let nanos = u128::from(u64::try_from(bytes).unwrap_or(u64::MAX))
                        * 1_000_000_000
                        / u128::from(bytes_per_sec);
                    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
                }
            }
        }
    }
}

/// Both directions' conditions — the asymmetry §8.4 names is exactly these
/// two fields differing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LinkConditions {
    /// Applied to bytes travelling from the connecting client toward the
    /// upstream server (e.g. a node's request to Postgres).
    pub client_to_server: LinkCondition,
    /// Applied to bytes travelling from the upstream server back to the
    /// client (e.g. Postgres's reply).
    pub server_to_client: LinkCondition,
}

impl LinkConditions {
    /// The unfaulted link: both directions pass.
    #[must_use]
    pub const fn pass() -> Self {
        Self {
            client_to_server: LinkCondition::Pass,
            server_to_client: LinkCondition::Pass,
        }
    }

    /// Both directions dropped — a full partition of this link.
    #[must_use]
    pub const fn dropped() -> Self {
        Self {
            client_to_server: LinkCondition::Drop,
            server_to_client: LinkCondition::Drop,
        }
    }

    /// Whether either direction is currently [`LinkCondition::Drop`] — a
    /// connection-level condition (module docs): a TCP proxy cannot pass
    /// one direction of a connection whose other direction is torn down.
    fn either_dropped(self) -> bool {
        self.client_to_server == LinkCondition::Drop || self.server_to_client == LinkCondition::Drop
    }

    /// Both directions' [`LinkCondition::most_restrictive`] — how two
    /// simultaneously-held windows compose on one link (module docs).
    fn most_restrictive(self, other: Self) -> Self {
        Self {
            client_to_server: self
                .client_to_server
                .most_restrictive(other.client_to_server),
            server_to_client: self
                .server_to_client
                .most_restrictive(other.server_to_client),
        }
    }
}

/// A snapshot of one link's real traffic counters — the evidence a fault
/// window's journal lines carry so a judge (#208, tracked separately) can
/// tell a partition that actually cut real traffic from one that armed
/// against a link nothing ever used (§8.4's vacuity teeth: "a fault
/// schedule that armed faults and fired none").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LinkStats {
    /// Connections accepted on the link's own listener, ever.
    pub conns_accepted: u64,
    /// Connections closed immediately on accept because the link was
    /// dropped at that moment.
    pub conns_refused: u64,
    /// Established proxied connections torn down by a [`FaultLink::hold`]
    /// that dropped the link while they were live.
    pub conns_cut: u64,
    /// Bytes actually forwarded client→server, ever.
    pub bytes_client_to_server: u64,
    /// Bytes actually forwarded server→client, ever.
    pub bytes_server_to_client: u64,
}

impl LinkStats {
    /// `self` minus `earlier`, field by field (saturating — these counters
    /// only ever grow, so a negative delta is impossible in practice and
    /// must never panic a fault log if it somehow happened).
    #[must_use]
    pub fn since(self, earlier: Self) -> Self {
        Self {
            conns_accepted: self.conns_accepted.saturating_sub(earlier.conns_accepted),
            conns_refused: self.conns_refused.saturating_sub(earlier.conns_refused),
            conns_cut: self.conns_cut.saturating_sub(earlier.conns_cut),
            bytes_client_to_server: self
                .bytes_client_to_server
                .saturating_sub(earlier.bytes_client_to_server),
            bytes_server_to_client: self
                .bytes_server_to_client
                .saturating_sub(earlier.bytes_server_to_client),
        }
    }
}

/// Every condition currently HELD on one link, by hold id (module docs:
/// overlapping fault windows compose, they do not overwrite each other).
#[derive(Default)]
struct HoldTable {
    /// Never reused, so a released hold's id can never lift a later one.
    next_id: u64,
    active: BTreeMap<u64, LinkConditions>,
}

impl HoldTable {
    /// The condition actually in force: the most restrictive across every
    /// outstanding hold, and [`LinkConditions::pass`] when there are none.
    fn effective(&self) -> LinkConditions {
        self.active
            .values()
            .copied()
            .fold(LinkConditions::pass(), LinkConditions::most_restrictive)
    }
}

/// The live state every accept/pump task shares with the [`FaultLink`]
/// handle the fault injectors hold.
struct LinkState {
    holds: std::sync::Mutex<HoldTable>,
    /// Bumped whenever a new [`LinkHold`] drops the link that was not
    /// already dropped; every live pump task watches it and tears its
    /// connection down when it changes.
    cut_epoch: watch::Sender<u64>,
    conns_accepted: AtomicU64,
    conns_refused: AtomicU64,
    conns_cut: AtomicU64,
    bytes_client_to_server: AtomicU64,
    bytes_server_to_client: AtomicU64,
}

impl LinkState {
    fn conditions(&self) -> LinkConditions {
        self.holds
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .effective()
    }

    fn stats(&self) -> LinkStats {
        LinkStats {
            conns_accepted: self.conns_accepted.load(Ordering::Relaxed),
            conns_refused: self.conns_refused.load(Ordering::Relaxed),
            conns_cut: self.conns_cut.load(Ordering::Relaxed),
            bytes_client_to_server: self.bytes_client_to_server.load(Ordering::Relaxed),
            bytes_server_to_client: self.bytes_server_to_client.load(Ordering::Relaxed),
        }
    }
}

/// Which way bytes are moving through a pump — selects both the condition
/// and the counter that apply.
#[derive(Debug, Clone, Copy)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

/// One live fault link: a real listener the fleet points a real client at,
/// forwarding to a real upstream under a live-changeable [`LinkConditions`]
/// (module docs). Dropping the handle stops the accept loop and closes the
/// listener.
pub struct FaultLink {
    label: String,
    listen_addr: SocketAddr,
    upstream: String,
    state: Arc<LinkState>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl Drop for FaultLink {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

impl FaultLink {
    /// Binds a link on an ephemeral loopback port, forwarding to
    /// `upstream_host:upstream_port`, initially [`LinkConditions::pass`].
    /// `label` names it in journals (e.g. `"node-1-catalog"`).
    ///
    /// # Errors
    ///
    /// If the loopback listener cannot be bound.
    pub async fn bind(
        label: &str,
        upstream_host: &str,
        upstream_port: u16,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .with_context(|| format!("binding fault link {label}"))?;
        let listen_addr = listener
            .local_addr()
            .with_context(|| format!("reading fault link {label}'s own address"))?;
        let upstream = format!("{upstream_host}:{upstream_port}");
        let state = Arc::new(LinkState {
            holds: std::sync::Mutex::new(HoldTable::default()),
            cut_epoch: watch::channel(0).0,
            conns_accepted: AtomicU64::new(0),
            conns_refused: AtomicU64::new(0),
            conns_cut: AtomicU64::new(0),
            bytes_client_to_server: AtomicU64::new(0),
            bytes_server_to_client: AtomicU64::new(0),
        });
        let accept_task = tokio::spawn(accept_loop(
            listener,
            upstream.clone(),
            Arc::clone(&state),
            label.to_owned(),
        ));
        Ok(Self {
            label: label.to_owned(),
            listen_addr,
            upstream,
            state,
            accept_task,
        })
    }

    /// This link's own name in journals.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The loopback address a client must dial to traverse this link.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// The `host:port` this link forwards to.
    #[must_use]
    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    /// Current traffic counters (module docs of [`LinkStats`]).
    #[must_use]
    pub fn stats(&self) -> LinkStats {
        self.state.stats()
    }

    /// Holds `conditions` on this link until the returned [`LinkHold`] is
    /// dropped — the ONLY way to condition a link (module docs: a window
    /// lifts its own hold, never another window's).
    ///
    /// If this hold makes the link dropped when it was not already, every
    /// currently-established connection is also torn down (module docs: the
    /// fault's effect must start inside its own journaled window, not
    /// merely apply to connections opened after it).
    #[must_use = "the condition is lifted as soon as the hold is dropped"]
    pub fn hold(&self, conditions: LinkConditions) -> LinkHold {
        let mut table = self
            .state
            .holds
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = table.next_id;
        table.next_id += 1;
        let was_dropped = table.effective().either_dropped();
        table.active.insert(id, conditions);
        let now_dropped = table.effective().either_dropped();
        drop(table);
        if now_dropped && !was_dropped {
            self.state.cut_epoch.send_modify(|epoch| *epoch += 1);
        }
        LinkHold {
            state: Arc::clone(&self.state),
            id,
        }
    }
}

/// One fault window's outstanding condition on one link. Dropping it lifts
/// exactly that window's own condition — the link relaxes only once the
/// LAST hold on it is gone (module docs), never as a side effect of another
/// window ending, and never a claim the system has recovered from the fault
/// (`crate::fault`'s own module docs).
pub struct LinkHold {
    state: Arc<LinkState>,
    id: u64,
}

impl Drop for LinkHold {
    fn drop(&mut self) {
        self.state
            .holds
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(&self.id);
        // No `cut_epoch` bump: releasing a hold can only ever RELAX the
        // link, and a relaxation must not tear down live connections.
    }
}

/// Accepts forever, refusing (accept-then-close) while the link is dropped
/// and spawning a bidirectional pump otherwise. Never returns on a single
/// connection's failure — a link whose accept loop died would silently stop
/// being a link at all.
async fn accept_loop(
    listener: TcpListener,
    upstream: String,
    state: Arc<LinkState>,
    label: String,
) {
    loop {
        let Ok((client, _peer)) = listener.accept().await else {
            // A listener-level error (fd exhaustion, or the listener being
            // closed at shutdown) — nothing this loop can do but stop.
            return;
        };
        state.conns_accepted.fetch_add(1, Ordering::Relaxed);
        if state.conditions().either_dropped() {
            state.conns_refused.fetch_add(1, Ordering::Relaxed);
            drop(client);
            continue;
        }
        let upstream = upstream.clone();
        let state = Arc::clone(&state);
        let label = label.clone();
        tokio::spawn(async move {
            if let Err(error) = proxy_connection(client, &upstream, &state).await {
                tracing::debug!(link = %label, %error, "fault link connection ended");
            }
        });
    }
}

/// Pumps one accepted connection in both directions until BOTH directions
/// have ended, or a [`FaultLink::hold`] cuts the link.
///
/// The two directions are joined, not raced: one direction reaching EOF
/// half-closes only that peer's write half and leaves the other direction
/// pumping (see [`pump`]). Racing them — first future to finish tears the
/// whole connection down — makes a legal HTTP/1.1 half-close (a client that
/// sends its request, shuts down its write half, and then reads) destroy
/// the response still in flight the other way, on an UNFAULTED link: the
/// links are bound for the whole run, so an unfaulted node's byte path has
/// to be a faithful pump, not merely a mostly-faithful one (an ACPR finding
/// on issue #204, MEDIUM-3).
async fn proxy_connection(
    client: TcpStream,
    upstream: &str,
    state: &Arc<LinkState>,
) -> anyhow::Result<()> {
    let epoch_at_start = *state.cut_epoch.borrow();
    let server = TcpStream::connect(upstream)
        .await
        .with_context(|| format!("fault link dialing upstream {upstream}"))?;
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();

    let to_server = pump(
        client_read,
        server_write,
        Direction::ClientToServer,
        Arc::clone(state),
    );
    let to_client = pump(
        server_read,
        client_write,
        Direction::ServerToClient,
        Arc::clone(state),
    );
    tokio::join!(to_server, to_client);
    if *state.cut_epoch.borrow() != epoch_at_start {
        state.conns_cut.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// One direction's byte pump: read a chunk, apply the current condition to
/// it, forward it, count it. Returns when this direction's stream ends, a
/// write fails, or the link is cut.
///
/// On a clean end of THIS direction (EOF from `from`), the peer's write
/// half is shut down — propagating the half-close as a half-close, so the
/// other direction keeps pumping until it ends on its own
/// ([`proxy_connection`]'s docs on the ACPR finding this closes).
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    direction: Direction,
    state: Arc<LinkState>,
) {
    let mut cut = state.cut_epoch.subscribe();
    let mut buf = vec![0_u8; CHUNK];
    loop {
        let read = tokio::select! {
            biased;
            _ = cut.changed() => return,
            read = from.read(&mut buf) => read,
        };
        let n = match read {
            Ok(0) => {
                // A half-close, forwarded as one: the upstream sees the
                // FIN it would have seen without this proxy in the path,
                // and this connection's OTHER direction stays open.
                let _ = to.shutdown().await;
                return;
            }
            Err(_) => return,
            Ok(n) => n,
        };
        let condition = match direction {
            Direction::ClientToServer => state.conditions().client_to_server,
            Direction::ServerToClient => state.conditions().server_to_client,
        };
        if condition == LinkCondition::Drop {
            return;
        }
        let pace = condition.pace(n);
        if !pace.is_zero() {
            // A cut during the pacing sleep must still tear the connection
            // down promptly rather than after the full delay — otherwise a
            // long `Delay` would outlive its own fault window.
            tokio::select! {
                biased;
                _ = cut.changed() => return,
                () = tokio::time::sleep(pace) => {}
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
        let counter = match direction {
            Direction::ClientToServer => &state.bytes_client_to_server,
            Direction::ServerToClient => &state.bytes_server_to_client,
        };
        counter.fetch_add(u64::try_from(n).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}

/// Test-only helpers, shared with `crate::fault`'s own injector tests (they
/// exercise the injectors against REAL links, so they need the same real
/// upstream and the same real round-trip probe this module's own tests do).
#[cfg(test)]
pub(crate) mod test_support {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::{Duration, FaultLink, TcpListener, TcpStream};

    /// A real upstream that echoes every byte back — enough to exercise
    /// both directions of a real proxied TCP connection.
    pub(crate) async fn spawn_echo_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0_u8; 16 * 1024];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        port
    }

    /// Sends `payload` through `link` and reads exactly `payload.len()`
    /// bytes back from the echo upstream, returning how long the round trip
    /// took. `Err` if the connection is refused or dies mid-exchange —
    /// which is exactly what a dropped link must produce.
    pub(crate) async fn echo_round_trip(
        link: &FaultLink,
        payload: &[u8],
    ) -> anyhow::Result<Duration> {
        let started = tokio::time::Instant::now();
        let mut stream = TcpStream::connect(link.listen_addr()).await?;
        stream.write_all(payload).await?;
        let mut back = vec![0_u8; payload.len()];
        stream.read_exact(&mut back).await?;
        anyhow::ensure!(back == payload, "echoed payload differs");
        Ok(started.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{echo_round_trip, spawn_echo_server};
    use super::*;

    /// The baseline every other test here is a deviation from: a link with
    /// no condition set is a faithful byte pump — real bytes reach the real
    /// upstream and come back, and the link's own counters say so.
    #[tokio::test]
    async fn a_passing_link_forwards_real_bytes_in_both_directions() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-pass", "127.0.0.1", port)
            .await
            .unwrap();

        echo_round_trip(&link, b"duckspout").await.unwrap();

        let stats = link.stats();
        assert_eq!(stats.conns_accepted, 1);
        assert_eq!(stats.conns_refused, 0);
        assert_eq!(stats.bytes_client_to_server, 9);
        assert_eq!(stats.bytes_server_to_client, 9);
    }

    /// A dropped link refuses NEW connections (accept-then-close, module
    /// docs) — the client sees a dead connection, not a hang, and the
    /// link's `conns_refused` counter records it as evidence the fault
    /// really bit.
    #[tokio::test]
    async fn a_dropped_link_refuses_new_connections() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-drop", "127.0.0.1", port)
            .await
            .unwrap();
        let _dropped = link.hold(LinkConditions::dropped());

        let result = echo_round_trip(&link, b"duckspout").await;
        assert!(
            result.is_err(),
            "a dropped link must not complete a round trip"
        );
        assert_eq!(link.stats().conns_refused, 1);
        assert_eq!(
            link.stats().bytes_client_to_server,
            0,
            "no byte may cross a dropped link"
        );
    }

    /// The half a rule that only refused NEW connections would miss: an
    /// ALREADY-ESTABLISHED connection must be cut the moment the link is
    /// dropped (module docs — the fault's effect has to begin inside its
    /// own journaled window, not only for connections opened after it).
    #[tokio::test]
    async fn dropping_a_link_cuts_an_established_connection() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-cut", "127.0.0.1", port)
            .await
            .unwrap();

        let mut stream = TcpStream::connect(link.listen_addr()).await.unwrap();
        stream.write_all(b"before").await.unwrap();
        let mut back = [0_u8; 6];
        stream.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"before");

        let _dropped = link.hold(LinkConditions::dropped());

        // The cut closes the proxied connection: the next read observes EOF
        // (or a reset), never more echoed data.
        let after_cut = tokio::time::timeout(Duration::from_secs(5), async {
            let _ = stream.write_all(b"after").await;
            let mut buf = [0_u8; 5];
            stream.read(&mut buf).await
        })
        .await
        .expect("the cut must be observed well within 5s");
        assert!(
            matches!(after_cut, Ok(0) | Err(_)),
            "an established connection must be cut, not keep echoing: {after_cut:?}"
        );
        assert_eq!(
            link.stats().conns_cut,
            1,
            "the cut must be counted as evidence the fault bit a live connection"
        );
    }

    /// Releasing a link's last hold makes it a faithful byte pump again —
    /// the `Ended` phase of a partition window must genuinely lift the
    /// condition, or every later fault in the schedule would run against a
    /// still-broken link.
    #[tokio::test]
    async fn releasing_the_last_hold_lets_traffic_through_again() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-restore", "127.0.0.1", port)
            .await
            .unwrap();
        let dropped = link.hold(LinkConditions::dropped());
        assert!(echo_round_trip(&link, b"nope").await.is_err());

        drop(dropped);
        echo_round_trip(&link, b"again")
            .await
            .expect("a restored link must carry real traffic again");
    }

    /// The HIGH-1 ACPR finding on issue #204, at the mechanism level: two
    /// fault windows sharing one link (e.g. `--fault-catalog-outage-node 1`
    /// and `--fault-discovery-flap-node 1`, which resolve to the SAME
    /// catalog link and run concurrently) must not corrupt each other. The
    /// pre-fix `set`/`restore` pair was last-writer-wins, so the flap's
    /// per-cycle `restore()` lifted the outage's cut while the outage
    /// window was still journaled open — traffic really crossed a link the
    /// journal claimed was cut.
    #[tokio::test]
    async fn a_released_hold_does_not_lift_another_windows_still_held_drop() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-two-windows", "127.0.0.1", port)
            .await
            .unwrap();

        let outage = link.hold(LinkConditions::dropped());
        {
            // A second, overlapping window on the same link, released
            // FIRST — exactly the flap's per-cycle up phase.
            let _flap_cycle = link.hold(LinkConditions::dropped());
        }
        assert!(
            echo_round_trip(&link, b"nope").await.is_err(),
            "a second window's release must not lift a window that is still holding the link"
        );

        drop(outage);
        echo_round_trip(&link, b"again")
            .await
            .expect("the link must pass once its LAST hold is released");
    }

    /// The composition rule two overlapping windows resolve through: the
    /// most restrictive condition wins per direction, so no window is ever
    /// relaxed by another window's weaker condition.
    #[test]
    fn the_most_restrictive_held_condition_wins_per_direction() {
        let mut table = HoldTable::default();
        table.active.insert(
            0,
            LinkConditions {
                client_to_server: LinkCondition::Delay { ms: 10 },
                server_to_client: LinkCondition::Pass,
            },
        );
        assert_eq!(
            table.effective().client_to_server,
            LinkCondition::Delay { ms: 10 }
        );

        table.active.insert(
            1,
            LinkConditions {
                client_to_server: LinkCondition::Pass,
                server_to_client: LinkCondition::Drop,
            },
        );
        let effective = table.effective();
        assert_eq!(
            effective.client_to_server,
            LinkCondition::Delay { ms: 10 },
            "a second window's Pass must not relax the first window's delay"
        );
        assert_eq!(
            effective.server_to_client,
            LinkCondition::Drop,
            "Drop dominates every other condition"
        );

        // A slower cap beats a faster one; releasing it falls back to the
        // condition still held, never to Pass.
        table.active.clear();
        table.active.insert(
            0,
            LinkConditions {
                client_to_server: LinkCondition::BandwidthCap {
                    bytes_per_sec: 1_000_000,
                },
                server_to_client: LinkCondition::Pass,
            },
        );
        table.active.insert(
            1,
            LinkConditions {
                client_to_server: LinkCondition::BandwidthCap { bytes_per_sec: 10 },
                server_to_client: LinkCondition::Pass,
            },
        );
        assert_eq!(
            table.effective().client_to_server,
            LinkCondition::BandwidthCap { bytes_per_sec: 10 }
        );
        table.active.remove(&1);
        assert_eq!(
            table.effective().client_to_server,
            LinkCondition::BandwidthCap {
                bytes_per_sec: 1_000_000
            }
        );
        table.active.remove(&0);
        assert_eq!(table.effective(), LinkConditions::pass());
    }

    /// The MEDIUM-3 ACPR finding on issue #204: a `Pass` link must be a
    /// faithful byte pump for a legal HTTP/1.1 half-close too — a client
    /// that sends its request, shuts down its write half, and then reads
    /// must still receive the upstream's reply. The pre-fix pump raced the
    /// two directions and tore the whole connection down on the FIRST EOF,
    /// destroying a response still in flight the other way — on an
    /// UNFAULTED link, which every fault-linked node uses for its whole
    /// run.
    #[tokio::test]
    async fn a_half_closed_client_still_receives_the_upstreams_reply() {
        // An upstream that only replies AFTER it sees the request's EOF —
        // the shape a half-close is for.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    if stream.read_to_end(&mut request).await.is_err() {
                        return;
                    }
                    let _ = stream.write_all(b"reply-after-eof").await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        let link = FaultLink::bind("test-half-close", "127.0.0.1", port)
            .await
            .unwrap();
        let mut stream = TcpStream::connect(link.listen_addr()).await.unwrap();
        stream.write_all(b"request").await.unwrap();
        // FIN on the request direction only — the response direction is
        // still open and still expected to carry the reply.
        stream.shutdown().await.unwrap();

        let mut back = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut back))
            .await
            .expect("the reply must arrive well within 5s");
        read.expect("reading the upstream's reply");
        assert_eq!(
            back, b"reply-after-eof",
            "a half-close must not tear down the direction still carrying data"
        );
    }

    /// A one-direction delay is real, measurable added latency — and it is
    /// exactly the ASYMMETRY §8.4 names: the request direction passes at
    /// full speed while only the reply is held.
    #[tokio::test]
    async fn an_asymmetric_delay_slows_only_the_configured_direction() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-delay", "127.0.0.1", port)
            .await
            .unwrap();

        let fast = echo_round_trip(&link, b"quick").await.unwrap();
        assert!(
            fast < Duration::from_millis(300),
            "an unconditioned round trip must be fast, took {fast:?}"
        );

        let _delayed = link.hold(LinkConditions {
            client_to_server: LinkCondition::Pass,
            server_to_client: LinkCondition::Delay { ms: 400 },
        });
        let slow = echo_round_trip(&link, b"quick").await.unwrap();
        assert!(
            slow >= Duration::from_millis(400),
            "the delayed direction must add its full delay, took {slow:?}"
        );
    }

    /// A bandwidth cap paces real bytes: a payload that would cross a
    /// passing link instantly takes at least `bytes / bytes_per_sec` to
    /// cross a capped one.
    #[tokio::test]
    async fn a_bandwidth_cap_paces_real_bytes() {
        let port = spawn_echo_server().await;
        let link = FaultLink::bind("test-bandwidth", "127.0.0.1", port)
            .await
            .unwrap();
        let _capped = link.hold(LinkConditions {
            // 8 KiB through a 16 KiB/s cap ≈ 0.5s on the request leg alone.
            client_to_server: LinkCondition::BandwidthCap {
                bytes_per_sec: 16 * 1024,
            },
            server_to_client: LinkCondition::Pass,
        });

        let payload = vec![7_u8; 8 * 1024];
        let elapsed = echo_round_trip(&link, &payload).await.unwrap();
        assert!(
            elapsed >= Duration::from_millis(450),
            "a 16 KiB/s cap must pace 8 KiB to ~0.5s, took {elapsed:?}"
        );
    }

    /// `pace` is the whole arithmetic of the delay/cap conditions —
    /// including the deliberate zero-cap escape hatch (its own inline
    /// comment: a misconfigured `0` must not hang the fleet forever).
    #[test]
    fn pace_computes_the_documented_hold_for_each_condition() {
        assert_eq!(LinkCondition::Pass.pace(4096), Duration::ZERO);
        assert_eq!(LinkCondition::Drop.pace(4096), Duration::ZERO);
        assert_eq!(
            LinkCondition::Delay { ms: 250 }.pace(1),
            Duration::from_millis(250),
            "a delay is per chunk, not per byte"
        );
        assert_eq!(
            LinkCondition::BandwidthCap {
                bytes_per_sec: 1000
            }
            .pace(500),
            Duration::from_millis(500)
        );
        assert_eq!(
            LinkCondition::BandwidthCap { bytes_per_sec: 0 }.pace(500),
            Duration::ZERO,
            "a zero cap must not mean an infinite hold"
        );
    }

    /// `LinkStats::since` is what turns ever-growing counters into
    /// per-window evidence — the delta a fault's `Ended` line journals.
    #[test]
    fn stats_since_reports_the_windows_own_delta() {
        let before = LinkStats {
            conns_accepted: 3,
            conns_refused: 0,
            conns_cut: 0,
            bytes_client_to_server: 100,
            bytes_server_to_client: 200,
        };
        let after = LinkStats {
            conns_accepted: 5,
            conns_refused: 2,
            conns_cut: 1,
            bytes_client_to_server: 100,
            bytes_server_to_client: 200,
        };
        let delta = after.since(before);
        assert_eq!(delta.conns_accepted, 2);
        assert_eq!(delta.conns_refused, 2);
        assert_eq!(delta.conns_cut, 1);
        assert_eq!(
            delta.bytes_client_to_server, 0,
            "zero bytes forwarded during the window is the partition's own proof"
        );
        assert_eq!(delta.bytes_server_to_client, 0);
    }

    /// Counters never run backwards in practice, but `since` must not
    /// panic (overflow) if they somehow appeared to — a fault log must
    /// never itself kill the fleet run (R-5, `crate::faultlog`'s own
    /// posture).
    #[test]
    fn stats_since_saturates_rather_than_panicking_on_a_backwards_delta() {
        let later = LinkStats {
            conns_accepted: 1,
            conns_refused: 0,
            conns_cut: 0,
            bytes_client_to_server: 0,
            bytes_server_to_client: 0,
        };
        let earlier = LinkStats {
            conns_accepted: 9,
            conns_refused: 9,
            conns_cut: 9,
            bytes_client_to_server: 9,
            bytes_server_to_client: 9,
        };
        assert_eq!(later.since(earlier).conns_accepted, 0);
    }

    /// A link whose upstream is not listening at all must fail the
    /// CONNECTION, not the whole accept loop: the next connection (once the
    /// upstream is up) still has to work, or a link that briefly outlived
    /// its upstream would be silently dead for the rest of the run.
    #[tokio::test]
    async fn an_unreachable_upstream_does_not_kill_the_accept_loop() {
        // Bind and immediately drop a listener to get a port nothing is on.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);

        let link = FaultLink::bind("test-dead-upstream", "127.0.0.1", dead_port)
            .await
            .unwrap();
        assert!(echo_round_trip(&link, b"x").await.is_err());
        assert_eq!(
            link.stats().conns_accepted,
            1,
            "the link must still have accepted the connection it could not forward"
        );

        // The accept loop is still alive: a second connection is accepted
        // too (and fails the same way, since the upstream is still down).
        assert!(echo_round_trip(&link, b"y").await.is_err());
        assert_eq!(link.stats().conns_accepted, 2);
    }
}
