//! Membership through a few seed nodes that every node keeps a session to.

mod core;
#[cfg(test)]
mod sim;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axon::EventLog;
use axon::trace::unix_micros;
use axon_net::rustls::ClientConfig;
use axon_net::rustls::pki_types::ServerName;
use axon_net::{
    AcceptLimits, Backoff, ClientTls, Codecs, Compression, Config, Error, Lane, Limits, Listener, ListenerStats,
    ListenerStatsReader, Received, Receiver, Sender, ServerTls, Session, SessionStats, SubmitReason, connect,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, sleep, sleep_until, timeout};

use self::core::{MAX_FRAME, MAX_MEMBERS, Node, NodeOut, Out, SYNCED, Server, SessionId, parse_seed};
use crate::{BoxFuture, Discovery, DiscoveryError, NodeRecord, ProviderInfo, ProviderLink, Snapshot, Tls};

const KEEPALIVE: Duration = Duration::from_secs(1);
/// The cap of a seed dial's backoff.
const REDIAL_MAX: Duration = Duration::from_secs(2);
const SERVER_TICK: Duration = Duration::from_millis(10);
const NODE_TICK: Duration = Duration::from_millis(100);
const VIEW_FLOOR: Duration = Duration::from_millis(50);
const REGISTER_WAIT: Duration = Duration::from_secs(2);
const THAW_WAIT: Duration = Duration::from_secs(1);
const LEAVE_FLUSH: Duration = Duration::from_secs(1);

/// How long a seed and its nodes wait on each other. The same on every seed and node of a cluster: a node does not
/// count a seed that runs others (its link's state is `timings`). Changing them means stopping every node and seed at
/// once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeedTimings {
    /// A seed session with nothing received for this long closes: the idle timeout of the seed links.
    pub idle: Duration,
    /// How long a seed keeps a member whose session went quiet, as a dead machine's does.
    pub idle_hold: Duration,
    /// How long a seed keeps a member whose session closed.
    pub closed_grace: Duration,
    /// A new seed withholds `SYNCED` this long, and a node attached again stays frozen this long: both wait for the
    /// nodes to dial again, which takes the dial's 2 s backoff cap and its connect, at most about 3 s with
    /// [`SeedConfig::new`]'s 1 s connect.
    pub warm_up: Duration,
}

impl Default for SeedTimings {
    fn default() -> Self {
        let s = Duration::from_secs;
        Self { idle: s(4), idle_hold: s(5), closed_grace: s(2), warm_up: s(5) }
    }
}

impl SeedTimings {
    fn check(&self) -> Result<(), &'static str> {
        if self.idle <= 2 * KEEPALIVE {
            return Err("SeedTimings::idle must be above 2 s, so that a session lives through a lost keepalive");
        }
        if self.idle_hold < self.closed_grace {
            return Err("SeedTimings::idle_hold must be at least closed_grace");
        }
        if self.idle_hold <= 2 * KEEPALIVE {
            return Err("SeedTimings::idle_hold must be above 2 s, so that drops_after is above zero");
        }
        if self.warm_up < REDIAL_MAX + KEEPALIVE {
            return Err("SeedTimings::warm_up must be at least 3 s, so that a node's redial lands within it");
        }
        if self.closed_grace.is_zero() {
            return Err(
                "SeedTimings::closed_grace must be above zero, so that a member whose session closed can dial again",
            );
        }
        if [self.idle, self.idle_hold, self.closed_grace, self.warm_up].iter().any(|d| *d > Duration::from_secs(3600)) {
            return Err("SeedTimings must each be at most 1 h");
        }
        Ok(())
    }
}

/// The provider's copy of the transport: its own namespace, fast failure detection, blocks that
/// arrive compressed, and frames no larger than a list chunk.
fn transport(base: &Config, idle: Duration, server: bool) -> Config {
    let mut cfg = base.clone();
    cfg.namespace.push_str("/seed");
    (cfg.keepalive_interval, cfg.idle_timeout) = (KEEPALIVE, idle);
    cfg.accept = Codecs::ALL;
    let mut limits = Limits { max_frame: MAX_FRAME, ..Limits::default() };
    if server {
        // A node sends one small frame at a time; a whole list must fit a fresh session's queue.
        limits = Limits { inbound_frames: 8, inbound_bytes: 128 << 10, outbound_frames: 512, ..limits };
    }
    // Seed links dial Control, and a server runs the lane a peer names: each gets the seed's limits.
    for lane in Lane::ALL {
        let lane = cfg.lanes.get_mut(lane);
        (lane.compression, lane.limits) = (Compression::None, limits.clone());
    }
    cfg
}

fn frozen() -> DiscoveryError {
    DiscoveryError::Unavailable("attached to too few seeds".into())
}

/// A session that cannot take a frame is closed: the node redials and gets the list again.
fn send(sender: &Sender, frame: axon_net::Frame) {
    if sender.try_submit(frame).is_err_and(|e| e.reason != SubmitReason::Closed) {
        sender.close();
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SeedServerStats {
    pub sessions: usize,
    pub registered: usize,
    /// Sessions that hold no registration.
    pub observers: usize,
    #[cfg_attr(feature = "serde", serde(rename = "warm_up_left_ns", serialize_with = "axon::ser::ns"))]
    pub warm_up_left: Duration,
    /// By reason: stale, full, invalid.
    pub refusals: [u64; 3],
    pub list_bytes: usize,
    pub listener: ListenerStats,
}

#[derive(Clone)]
pub struct SeedServerConfig {
    pub addr: SocketAddr,
    pub transport: Config,
    pub tls: Option<Tls>,
    pub accept: AcceptLimits,
    pub timings: SeedTimings,
}

impl SeedServerConfig {
    pub fn new(addr: SocketAddr, transport: &Config) -> Self {
        Self {
            addr,
            transport: transport.clone(),
            tls: None,
            accept: AcceptLimits::default(),
            timings: SeedTimings::default(),
        }
    }
}

/// The seed role: one more port on an ordinary node. Dropping it closes the listener and its sessions.
pub struct SeedServer {
    local: SocketAddr,
    core: Arc<Mutex<Server>>,
    listener: ListenerStatsReader,
    tls: Option<ServerTls>,
    started: Instant,
    /// `stats` may be called from a thread outside the runtime, and reads its clock.
    runtime: tokio::runtime::Handle,
    task: JoinHandle<()>,
}

impl SeedServer {
    pub async fn bind(cfg: SeedServerConfig) -> Result<Self, Error> {
        let SeedServerConfig { addr, transport: base, tls, mut accept, timings } = cfg;
        timings.check().map_err(Error::Config)?;
        // A dead session holds its slot for some seconds while its node is already back.
        accept.max_connections = accept.max_connections.max(2 * MAX_MEMBERS);
        let listener =
            Listener::bind(addr, tls.map(|t| t.server), transport(&base, timings.idle, true), accept).await?;
        let started = Instant::now();
        let core = Arc::new(Mutex::new(Server::new(Duration::ZERO, timings)));
        Ok(Self {
            local: listener.local_addr(),
            listener: listener.stats_reader(),
            tls: listener.tls(),
            task: tokio::spawn(serve(listener, core.clone(), started)),
            core,
            started,
            runtime: tokio::runtime::Handle::current(),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// Swaps the certificate this seed serves. New sessions use it; sessions already up keep theirs until they end.
    pub fn set_tls(&self, tls: Tls) -> Result<(), Error> {
        let server = self.tls.as_ref().ok_or(Error::Config("this seed started without TLS"))?;
        server.set(tls.server);
        Ok(())
    }

    pub fn stats(&self) -> SeedServerStats {
        let _clock = self.runtime.enter();
        let core = self.core.lock();
        SeedServerStats {
            sessions: core.sessions(),
            registered: core.members(),
            observers: core.observers(),
            warm_up_left: core.warm_up_left(self.started.elapsed()),
            refusals: core.refusals,
            list_bytes: core.list_bytes(),
            listener: self.listener.get(),
        }
    }
}

impl Drop for SeedServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Owns every session: dropping this future closes them all.
async fn serve(mut listener: Listener, core: Arc<Mutex<Server>>, started: Instant) {
    let mut senders: BTreeMap<SessionId, Sender> = BTreeMap::new();
    let mut readers = JoinSet::new();
    let mut reading = HashMap::new();
    let (frames_tx, mut frames) = mpsc::channel::<(SessionId, Received)>(256);
    let mut tick = tokio::time::interval(SERVER_TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut next: SessionId = 0;
    let mut out = Vec::new();
    loop {
        tokio::select! {
            // In this order a session's last frames, a LEAVE above all, are handled before its close.
            biased;
            _ = tick.tick() => core.lock().tick(started.elapsed(), &mut out),
            session = listener.accept() => {
                let Some(Session { sender, receiver }) = session else { return };
                next += 1;
                let id = sender.peer().identity.clone();
                core.lock().open(next, id.node_id, id.incarnation, started.elapsed(), &mut out);
                senders.insert(next, sender);
                reading.insert(readers.spawn(read(next, receiver, frames_tx.clone())).id(), next);
            }
            Some((session, frame)) = frames.recv() => {
                core.lock().frame(session, frame.into_frame(), started.elapsed(), unix_micros() / 1000, &mut out);
            }
            Some(done) = readers.join_next_with_id() => {
                let (task, idle) = done.unwrap_or_else(|e| (e.id(), false));
                let Some(session) = reading.remove(&task) else { continue };
                senders.remove(&session);
                core.lock().closed(session, idle, started.elapsed());
            }
        }
        for o in out.drain(..) {
            match o {
                Out::To(session, frame) => {
                    if let Some(sender) = senders.get(&session) {
                        send(sender, frame);
                    }
                }
                Out::All(frame) => senders.values().for_each(|s| send(s, frame.clone())),
                Out::Close(session) => senders.get(&session).into_iter().for_each(Sender::close),
            }
        }
    }
}

fn silent(error: &Error) -> bool {
    use std::io::ErrorKind::{HostUnreachable, NetworkUnreachable, TimedOut};
    match error {
        Error::Timeout(_) => true,
        Error::Io(e) => matches!(e.kind(), TimedOut | HostUnreachable | NetworkUnreachable),
        _ => false,
    }
}

/// Answers with whether the session went silent, as a dead machine's does, or was closed.
async fn read(session: SessionId, mut receiver: Receiver, frames: mpsc::Sender<(SessionId, Received)>) -> bool {
    loop {
        match receiver.recv().await {
            Ok(frame) => {
                if frames.send((session, frame)).await.is_err() {
                    return false;
                }
            }
            Err(error) => return silent(&error),
        }
    }
}

#[derive(Clone)]
pub struct SeedConfig {
    /// `host:port` or `id@host:port`. 1 to 16 entries.
    pub seeds: Vec<String>,
    /// Its `connect_timeout` bounds a seed dial's TCP connect: 1 s from [`new`](Self::new), or the handshake timeout
    /// where that is shorter. A dial that does not connect in time is tried again, and a seed drops a node that has not
    /// dialled again within `closed_grace`, so keep that above it: where a lost SYN's answer comes later, set it as
    /// the cluster's. [`SeedDiscovery::start`] refuses a `connect_timeout` within 2 s of `timings.warm_up`: a
    /// transport put here in place of the one `new` set keeps its own, 5 s from `axon_net::Config::new`.
    pub transport: Config,
    pub tls: Option<Tls>,
    /// The seeds' own: a seed with others does not count.
    pub timings: SeedTimings,
}

impl SeedConfig {
    /// Clones `transport`, so the identity is the cluster's, and sets its `connect_timeout`, which the seed links dial
    /// with, to 1 s.
    pub fn new<S: Into<String>>(seeds: impl IntoIterator<Item = S>, transport: &Config) -> Self {
        let seeds = seeds.into_iter().map(Into::into).collect();
        let mut transport = transport.clone();
        transport.connect_timeout = KEEPALIVE.min(transport.handshake_timeout);
        Self { seeds, transport, tls: None, timings: SeedTimings::default() }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SeedLinkStats {
    pub seed: String,
    /// The node id the seed answered with.
    pub identity: Option<String>,
    /// `connecting`, `open`, `synced`, `refused`, `retired`, or `timings`: the seed runs other [`SeedTimings`] and does
    /// not count.
    pub state: &'static str,
    #[cfg_attr(feature = "serde", serde(rename = "since_ns", serialize_with = "axon::ser::ns"))]
    pub since: Duration,
    pub last_error: Option<String>,
    pub session: Option<SessionStats>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SeedStats {
    pub live: bool,
    /// Members of the last view.
    pub members: usize,
    /// Seed entries that reached a seed another entry already reached.
    pub duplicates: usize,
    pub refusals: u64,
    pub seeds: Vec<SeedLinkStats>,
}

struct Link {
    sender: Option<Sender>,
    since: Instant,
    error: Option<String>,
    task: JoinHandle<()>,
}

struct State {
    node: Node,
    links: BTreeMap<String, Link>,
    answered: Option<Instant>,
    events: Option<Arc<EventLog>>,
}

struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<()>,
    cfg: Config,
    timings: SeedTimings,
    tls: Option<Mutex<Arc<ClientConfig>>>,
    started: Instant,
    /// `set_seeds` and `stats` may be called from a thread outside the runtime, and read its clock.
    runtime: tokio::runtime::Handle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.state.get_mut().links.values().for_each(|l| l.task.abort());
    }
}

impl Inner {
    /// Runs one step of the state machine, does what it asked for, and wakes whoever waits on it.
    fn step<T>(self: &Arc<Self>, step: impl FnOnce(&mut State, Duration, &mut Vec<NodeOut>) -> T) -> T {
        let (result, _) = self.advance(step);
        self.changed.send_replace(());
        result
    }

    /// Also answers whether the step asked for anything.
    fn advance<T>(self: &Arc<Self>, step: impl FnOnce(&mut State, Duration, &mut Vec<NodeOut>) -> T) -> (T, bool) {
        let mut out = Vec::new();
        let mut state = self.state.lock();
        let result = step(&mut state, self.started.elapsed(), &mut out);
        let acted = !out.is_empty();
        for o in out {
            match o {
                NodeOut::Send(entry, frame) => {
                    if let Some(sender) = state.links.get(&entry).and_then(|l| l.sender.as_ref()) {
                        send(sender, frame);
                    }
                }
                NodeOut::Close(entry) => {
                    state.links.get(&entry).and_then(|l| l.sender.as_ref()).into_iter().for_each(Sender::close);
                }
                NodeOut::Timings(entry, theirs) => {
                    if let Some(events) = &state.events {
                        record_timings(events, &entry, &theirs, &self.timings);
                    }
                }
                NodeOut::Retire(entry) => {
                    let Some(link) = state.links.remove(&entry) else { continue };
                    // The LEAVE goes out before the session does.
                    self.runtime.spawn(async move {
                        if let Some(sender) = &link.sender {
                            let _ = timeout(LEAVE_FLUSH, sender.flush()).await;
                        }
                        link.task.abort();
                    });
                }
            }
        }
        let missing: Vec<String> =
            state.node.seeds().filter(|s| !state.links.contains_key(*s)).map(str::to_owned).collect();
        for entry in missing {
            let task = self.runtime.spawn(dial(Arc::downgrade(self), entry.clone(), self.cfg.clone()));
            state.links.insert(entry, Link { sender: None, since: Instant::now(), error: None, task });
        }
        (result, acted)
    }
}

fn record_timings(events: &EventLog, entry: &str, theirs: &SeedTimings, ours: &SeedTimings) {
    events.record("seed_timings", entry, format!("the seed runs {theirs:?}, this node {ours:?}: it does not count"));
}

/// The link of the dial task that asks: a retired task may outlive its link by a moment, and the entry
/// may be back by then with another.
fn own_link<'a>(state: &'a mut State, entry: &str) -> Option<&'a mut Link> {
    state.links.get_mut(entry).filter(|l| l.task.id() == tokio::task::id())
}

/// Dropping the last clone closes its sessions.
#[derive(Clone)]
pub struct SeedDiscovery(Arc<Inner>);

impl SeedDiscovery {
    pub async fn start(cfg: SeedConfig) -> Result<Self, DiscoveryError> {
        cfg.timings.check().map_err(DiscoveryError::Invalid)?;
        if cfg.timings.warm_up.saturating_sub(cfg.transport.connect_timeout) < REDIAL_MAX {
            return Err(DiscoveryError::Invalid(
                "SeedTimings::warm_up must be at least 2 s more than SeedConfig::transport.connect_timeout, so that a \
                 node's redial lands within it: build the SeedConfig with SeedConfig::new, which sets the connect to \
                 1 s, set transport.connect_timeout after you replace the transport, or raise warm_up",
            ));
        }
        let transport = transport(&cfg.transport, cfg.timings.idle, false);
        transport.validate(cfg.tls.is_some()).map_err(|e| match e {
            Error::Config(why) => DiscoveryError::Invalid(why),
            _ => DiscoveryError::Invalid("transport"),
        })?;
        let identity = &transport.identity;
        let node = Node::new(identity.node_id.clone(), identity.incarnation, &cfg.seeds, cfg.timings)?;
        let inner = Arc::new(Inner {
            state: Mutex::new(State { node, links: BTreeMap::new(), answered: None, events: None }),
            changed: watch::Sender::new(()),
            cfg: transport,
            timings: cfg.timings,
            tls: cfg.tls.map(|t| Mutex::new(t.client)),
            started: Instant::now(),
            runtime: tokio::runtime::Handle::current(),
        });
        inner.step(|_, _, _| ());
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(NODE_TICK);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(inner) = weak.upgrade() else { return };
                // A tick that asked for nothing and thawed nothing changed nothing.
                let (thawed, acted) = inner.advance(|s, now, out| {
                    let was = s.node.live();
                    s.node.tick(now, out);
                    !was && s.node.live()
                });
                if thawed || acted {
                    inner.changed.send_replace(());
                }
            }
        });
        Ok(Self(inner))
    }

    /// Replaces the seed list on a running node. Seeds that were dropped stay connected until
    /// the node is live on the new list.
    pub fn set_seeds<S: Into<String>>(&self, seeds: impl IntoIterator<Item = S>) -> Result<(), DiscoveryError> {
        let seeds: Vec<String> = seeds.into_iter().map(Into::into).collect();
        let _clock = self.0.runtime.enter();
        self.0.step(|s, now, out| {
            s.node.set_seeds(&seeds)?;
            s.node.tick(now, out);
            Ok(())
        })
    }

    /// Swaps the certificates this node dials its seeds with. New sessions use them; sessions already up keep theirs
    /// until they end.
    pub fn set_tls(&self, tls: Tls) -> Result<(), DiscoveryError> {
        let client = self.0.tls.as_ref().ok_or(DiscoveryError::Invalid("this node started without TLS"))?;
        *client.lock() = tls.client;
        Ok(())
    }

    pub fn stats(&self) -> SeedStats {
        let _clock = self.0.runtime.enter();
        let state = self.0.state.lock();
        let seeds = state.node.states().map(|(entry, identity, kind)| {
            let link = state.links.get(entry);
            SeedLinkStats {
                seed: entry.to_owned(),
                identity: identity.map(str::to_owned),
                state: kind,
                since: link.map_or(Duration::ZERO, |l| l.since.elapsed()),
                last_error: link.and_then(|l| l.error.clone()),
                session: link.and_then(|l| l.sender.as_ref()).map(Sender::stats),
            }
        });
        SeedStats {
            seeds: seeds.collect(),
            live: state.node.live(),
            members: state.node.members(),
            duplicates: state.node.duplicates(),
            refusals: state.node.refusals,
        }
    }

    fn own(&self, id: &str, incarnation: u128) -> Result<(), DiscoveryError> {
        let identity = &self.0.cfg.identity;
        if id != identity.node_id || incarnation != identity.incarnation {
            return Err(DiscoveryError::Invalid("not the identity of this node's transport"));
        }
        Ok(())
    }
}

async fn resolve_and_connect(entry: &str, cfg: &Config, tls: Option<&Arc<ClientConfig>>) -> Result<Session, String> {
    let (id, target) = parse_seed(entry).ok_or("not a seed entry")?;
    let tls = match tls {
        Some(config) => {
            let host = target.rsplit_once(':').map_or(target, |(host, _)| host);
            let host = host.trim_start_matches('[').trim_end_matches(']');
            // As on cluster links: the id, or the address when the id cannot be a server name.
            let name = id.and_then(|id| ServerName::try_from(id.to_owned()).ok());
            let name = name.or_else(|| ServerName::try_from(host.to_owned()).ok());
            let name = name.ok_or_else(|| format!("{host} is not a TLS server name"))?;
            Some(ClientTls { name, config: config.clone() })
        }
        None => None,
    };
    let lookup = timeout(cfg.handshake_timeout, tokio::net::lookup_host(target)).await;
    let addrs = lookup.map_err(|_| "name lookup timed out".to_owned())?.map_err(|e| e.to_string())?;
    let mut error = "the name has no address".to_owned();
    for addr in addrs {
        match connect(addr, Lane::Control, tls.as_ref(), cfg).await {
            Ok(session) => return Ok(session),
            Err(e) => error = format!("{addr}: {e}"),
        }
    }
    Err(error)
}

/// Keeps a session to one seed. The backoff resets only after a session that synced and then held, so
/// one that keeps failing after its handshake, or is closed as too slow again and again, cannot spin.
async fn dial(inner: Weak<Inner>, entry: String, cfg: Config) {
    let backoff = Backoff { max: REDIAL_MAX, ..Backoff::default() };
    let mut failures = 0;
    loop {
        let Some(tls) = inner.upgrade().map(|i| i.tls.as_ref().map(|t| t.lock().clone())) else { return };
        let error = match resolve_and_connect(&entry, &cfg, tls.as_ref()).await {
            Ok(Session { sender, mut receiver }) => {
                let peer = &sender.peer().identity.node_id;
                let Some(strong) = inner.upgrade() else { return };
                strong.step(|s, now, out| {
                    let Some(link) = own_link(s, &entry) else { return };
                    (link.sender, link.since) = (Some(sender.clone()), Instant::now());
                    s.node.opened(&entry, peer.clone(), now, out);
                });
                drop(strong);
                let mut synced_at = None;
                let error = loop {
                    let frame = match receiver.recv().await {
                        Ok(frame) => frame.into_frame(),
                        Err(error) => break error,
                    };
                    let synced = frame.kind == SYNCED;
                    let Some(strong) = inner.upgrade() else { return };
                    strong.step(|s, now, out| {
                        if own_link(s, &entry).is_none() {
                            return;
                        }
                        s.node.frame(&entry, frame, now, unix_micros() / 1000, out);
                        // A seed with other timings is not a seed reached: its sessions keep the backoff.
                        let counted = synced && s.node.synced(&entry);
                        if let Some(link) = own_link(s, &entry).filter(|_| counted) {
                            link.since = Instant::now();
                            synced_at = Some(link.since);
                        }
                    });
                };
                if synced_at.is_some_and(|at| at.elapsed() >= backoff.max) {
                    failures = 0;
                }
                match parse_seed(&entry) {
                    Some((Some(id), _)) if id != peer => format!("answered as {peer}"),
                    _ => error.to_string(),
                }
            }
            Err(error) => error,
        };
        let Some(strong) = inner.upgrade() else { return };
        strong.step(|s, now, out| {
            let Some(link) = own_link(s, &entry) else { return };
            (link.sender, link.since, link.error) = (None, Instant::now(), Some(error));
            s.node.lost(&entry, now, out);
        });
        drop(strong);
        failures += 1;
        sleep(backoff.delay(failures)).await;
    }
}

fn snapshot_of((revision, view): (u64, &[Arc<NodeRecord>])) -> Snapshot {
    Snapshot { revision, nodes: view.iter().map(|r| NodeRecord::clone(r)).collect() }
}

impl Discovery for SeedDiscovery {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        let mut state = self.0.state.lock();
        let snapshot = match state.node.view() {
            Some(view) => Ok(snapshot_of(view)),
            None => Err(frozen()),
        };
        Box::pin(std::future::ready(snapshot))
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let record = record.clone();
        Box::pin(async move {
            record.validate()?;
            self.own(&record.id, record.incarnation)?;
            let mut changes = self.0.changed.subscribe();
            let (before, version) =
                self.0.step(|s, now, out| (s.node.version(), s.node.set_record(Some(record), now, out)));
            // An unchanged record gets the verdict a new one would, so two racing registrations agree. Off line, the
            // call that sent it waited already, and a heartbeat's retry must not wait again.
            let unheard = before == version && !self.0.state.lock().node.live();
            let deadline = Instant::now() + if unheard { Duration::ZERO } else { REGISTER_WAIT };
            while !self.0.state.lock().node.listed(version) {
                if timeout(deadline.saturating_duration_since(Instant::now()), changes.changed()).await.is_err() {
                    break;
                }
            }
            let state = self.0.state.lock();
            match state.node.refusal() {
                _ if state.node.listed(version) => Ok(()),
                Some(refusal) => Err(refusal),
                // A cluster must be able to start before its seeds.
                None if !state.node.live() => Ok(()),
                None => Err(DiscoveryError::Unavailable("most seeds do not list the record yet".into())),
            }
        })
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let own = self.own(id, incarnation);
        Box::pin(async move {
            own?;
            let senders: Vec<Sender> = self.0.step(|s, now, out| {
                s.node.set_record(None, now, out);
                s.links.values().filter_map(|l| l.sender.clone()).collect()
            });
            let flush = async {
                for sender in &senders {
                    let _ = sender.flush().await;
                }
            };
            let _ = timeout(LEAVE_FLUSH, flush).await;
            Ok(())
        })
    }

    fn heartbeat(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let health = self.own(id, incarnation).and_then(|()| {
            let node = &self.0.state.lock().node;
            if node.live() { Ok(()) } else { Err(node.refusal().unwrap_or_else(frozen)) }
        });
        Box::pin(std::future::ready(health))
    }

    /// Never answers with a revision that did not move, and at most once per `VIEW_FLOOR`: the same change
    /// arrives from every seed.
    fn watch(&self, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        Some(Box::pin(async move {
            let mut changes = self.0.changed.subscribe();
            let mut frozen_until = None;
            loop {
                // The floor comes first, so a burst of changes builds one view, not one per frame.
                let floor = self.0.state.lock().answered.map(|at| at + VIEW_FLOOR);
                if let Some(floor) = floor {
                    sleep_until(floor).await;
                }
                let wait_until = {
                    let mut state = self.0.state.lock();
                    let now = Instant::now();
                    match state.node.view() {
                        Some((revision, nodes)) if revision != after => {
                            let snapshot = snapshot_of((revision, nodes));
                            state.answered = Some(now);
                            return Ok(snapshot);
                        }
                        Some(_) => {
                            frozen_until = None;
                            None
                        }
                        None if now >= *frozen_until.get_or_insert(now + THAW_WAIT) => return Err(frozen()),
                        None => frozen_until,
                    }
                };
                tokio::select! {
                    _ = changes.changed() => {}
                    _ = sleep_until(wait_until.unwrap_or(Instant::now() + Duration::from_secs(3600))) => {}
                }
            }
        }))
    }

    /// The two ends see a session's loss at most two keepalives apart, and the seed then holds the member `idle_hold`.
    fn drops_after(&self) -> Option<Duration> {
        Some(self.0.timings.idle_hold - 2 * KEEPALIVE)
    }

    /// Tells the seeds that run other timings, now and from here on.
    fn attach(&self, events: &Arc<EventLog>) {
        let mut state = self.0.state.lock();
        for (entry, theirs) in state.node.odd() {
            record_timings(events, entry, &theirs, &self.0.timings);
        }
        state.events = Some(events.clone());
    }

    fn describe(&self) -> ProviderInfo {
        let stats = self.stats();
        let seeds: Vec<&str> = stats.seeds.iter().map(|l| l.seed.as_str()).collect();
        let mut info = ProviderInfo::new("seed")
            .setting("seeds", seeds.join(", "))
            .setting("tls", if self.0.tls.is_some() { "yes" } else { "no" })
            .stat("live", stats.live)
            .stat("members", stats.members)
            .stat("duplicates", stats.duplicates)
            .stat("refusals", stats.refusals);
        info.links = (stats.seeds.into_iter())
            .map(|l| ProviderLink {
                target: l.seed,
                identity: l.identity,
                state: l.state.into(),
                since: l.since,
                error: l.last_error,
            })
            .collect();
        info
    }
}

#[cfg(test)]
mod tests {
    use axon_net::{Frame, SharedSecret};
    use bytes::Bytes;

    use super::core::{REFUSED, register_frame};
    use super::*;

    fn base(id: &str) -> Config {
        let mut cfg = Config::new(id, SharedSecret::new("a shared secret of the cluster").unwrap());
        cfg.allow_plaintext = true;
        cfg
    }

    fn record(cfg: &Config) -> NodeRecord {
        NodeRecord {
            id: cfg.identity.node_id.clone(),
            incarnation: cfg.identity.incarnation,
            address: "127.0.0.1:1".parse().unwrap(),
            metadata: Default::default(),
            version: 1,
        }
    }

    /// What the seed answers a hand-made registration with.
    async fn answer(server: &SeedServer, cfg: &Config, record: &NodeRecord) -> Frame {
        let transport = transport(cfg, SeedTimings::default().idle, false);
        let mut session = connect(server.local_addr(), Lane::Control, None, &transport).await.unwrap();
        session.sender.try_submit(register_frame(record)).unwrap();
        session.receiver.recv().await.unwrap().into_frame()
    }

    #[tokio::test]
    async fn a_seed_refuses_a_record_that_is_not_the_sessions_or_is_from_the_future() {
        let any = "127.0.0.1:0".parse().unwrap();
        let server = SeedServer::bind(SeedServerConfig::new(any, &base("seed"))).await.unwrap();
        let (me, other) = (base("a"), base("b"));
        let refused = answer(&server, &me, &record(&other)).await;
        assert_eq!((refused.kind, &refused.payload[..]), (REFUSED, &[3][..]));

        let mut future = base("c");
        future.identity.incarnation += (120_000u128) << 64;
        let refused = answer(&server, &future, &record(&future)).await;
        assert_eq!((refused.kind, &refused.payload[..]), (REFUSED, &[3][..]));

        assert_eq!(answer(&server, &me, &record(&me)).await.kind, core::CHANGES);
        assert_eq!((server.stats().refusals, server.stats().registered), ([0, 0, 2], 1));
    }

    /// A seed server runs the lane a peer names: on each it answers with the seed's frame, not the lane's default.
    #[tokio::test]
    async fn a_seed_server_runs_its_own_limits_on_every_lane() {
        let any = "127.0.0.1:0".parse().unwrap();
        let server = SeedServer::bind(SeedServerConfig::new(any, &base("seed"))).await.unwrap();
        // The seed's namespace, and every lane's default frame, larger than the seed's.
        let mut peer = transport(&base("a"), SeedTimings::default().idle, false);
        peer.lanes = Default::default();
        for lane in Lane::ALL {
            let session = connect(server.local_addr(), lane, None, &peer).await.unwrap();
            assert_eq!(session.peer().max_frame, MAX_FRAME, "{lane:?}");
        }
    }

    #[test]
    fn only_a_session_that_went_quiet_counts_as_a_dead_machine() {
        use std::io::ErrorKind::{ConnectionReset, TimedOut};
        let io = |kind: std::io::ErrorKind| Error::from(std::io::Error::from(kind));
        assert!(silent(&Error::Timeout(axon_net::Timeout::Idle)) && silent(&io(TimedOut)));
        assert!(!silent(&Error::Closed) && !silent(&Error::PeerClosed) && !silent(&io(ConnectionReset)));
    }

    #[tokio::test]
    async fn a_session_that_cannot_take_a_frame_is_closed() {
        let any = "127.0.0.1:0".parse().unwrap();
        let mut listener = Listener::bind(any, None, base("seed"), AcceptLimits::default()).await.unwrap();
        let _silent = connect(listener.local_addr(), Lane::Control, None, &base("a")).await.unwrap();
        let Session { sender, receiver: _open } = listener.accept().await.unwrap();
        let frame = Frame { payload: Bytes::from(vec![0; 1 << 20]), ..Frame::default() };
        let sent = (0..64).take_while(|_| sender.is_active()).inspect(|_| send(&sender, frame.clone())).count();
        assert!(!sender.is_active() && sent > 1, "{sent} frames");
    }
}
