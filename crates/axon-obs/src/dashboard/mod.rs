mod api;
mod auth;

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use argon2::password_hash::PasswordHash;
use axon::{ActorIncarnation, ActorSystem, ExtensionFuture, SystemExtension};
use axon_cluster::{Cluster, MembershipSubscription, NodeRecord};
use axon_net::PerIp;
use axon_net::rustls::ServerConfig;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::{get, post};
use bytes::Bytes;
use include_dir::{Dir, include_dir};
use parking_lot::Mutex;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::time::Instant;
use tokio_rustls::server::TlsStream;
use tower_http::compression::predicate::{NotForContentType, Predicate, SizeAbove};

use self::api::{
    Failure, actors, asset, connection, dead_letters, events, history, inventory, key, keys, overview, redact, series,
    streams, timeline, trace, traces,
};
pub use self::auth::hash_password;
use self::auth::{Bucket, authenticate, guard, in_time, login, logout, session, signing_key};
use crate::control::{every, fan_out};
use crate::health::serve;
use crate::metrics::{Boots, history_sources, now_ms, query_metrics};
use crate::model::{ActorQuery, EventPage, Request, boot};
use crate::provider::{Capabilities, ClusterEvent};
use crate::telemetry::{event, fetch};
use crate::{MetricsHealth, MetricsRequest};

static ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");
/// The bundle hash Vite wrote into index.html; a browser on another build offers a reload.
static BUILD: LazyLock<&'static str> = LazyLock::new(|| {
    let index = ASSETS.get_file("index.html").and_then(|f| f.contents_utf8()).unwrap_or("");
    let mut hashes = index.split("assets/index-").skip(1).filter_map(|s| s.split_once('.'));
    hashes.find(|(_, rest)| rest.starts_with("js")).map_or("dev", |(hash, _)| hash)
});

const FRAME_HEAD: &[u8] = b"event: overview\ndata: ";
const NAME_BYTES: usize = 64;
const HANDSHAKES: usize = 64;
const HANDSHAKES_PER_CLIENT: usize = 16;
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
const LIVE_EVENTS: usize = 256;
const STREAMS: usize = 32;
const REQUESTS: usize = 16;
const PARALLELISM: usize = 16;
const IDLE_AFTER: Duration = Duration::from_secs(15);
const PROBE_EVERY: Duration = Duration::from_secs(5);
const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(3);
const INVENTORY_DEADLINE: Duration = Duration::from_secs(7);
const INVENTORY_ROWS: usize = 20_000;
const INVENTORY_TTL: Duration = Duration::from_secs(3);

/// Someone who may sign in to the console. Every user sees the same read-only console.
#[derive(Clone)]
pub struct User {
    pub name: String,
    /// An argon2 PHC string, from [`hash_password`] or any argon2 tool; never the password.
    pub hash: String,
}

impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("User").field("name", &self.name).finish_non_exhaustive()
    }
}

/// How long a sign-in lasts. Nothing is kept on the server: the cookie carries it, signed.
#[derive(Clone)]
pub struct SessionConfig {
    /// Signs out after this long without a request; each request starts it again.
    pub idle: Duration,
    /// Signs out this long after the sign-in, however busy.
    pub max: Duration,
    /// Signs the cookies, 32 bytes or more. Dashboards behind one balancer share it; `None` is random per start.
    pub key: Option<Vec<u8>>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self { idle: Duration::from_secs(30 * 60), max: Duration::from_secs(12 * 3600), key: None }
    }
}

impl std::fmt::Debug for SessionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionConfig").field("idle", &self.idle).field("max", &self.max).finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct DashboardConfig {
    pub bind: SocketAddr,
    pub base_path: String,
    /// Required unless `bind` is loopback. With none the console is open.
    pub users: Vec<User>,
    pub session: SessionConfig,
    /// Whose `X-Forwarded-For`, `-Proto` and `-Host` are believed.
    pub trusted_proxies: Vec<IpAddr>,
    /// Serves HTTPS itself; [`Dashboard::set_tls`] swaps it.
    pub tls: Option<Arc<ServerConfig>>,
    /// Hides the key of every `kind/key` actor name and span name in the API's replies.
    pub redact: bool,
    pub refresh_interval: Duration,
    pub max_nodes: usize,
    /// Pins history to one node. Otherwise the freshest announced source is used, and a
    /// request may name another with `?history=`.
    pub history_node: Option<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            bind: ([127, 0, 0, 1], 8080).into(),
            base_path: "/".into(),
            users: Vec::new(),
            session: SessionConfig::default(),
            trusted_proxies: Vec::new(),
            tls: None,
            redact: false,
            refresh_interval: Duration::from_secs(1),
            max_nodes: 1024,
            history_node: None,
        }
    }
}

impl DashboardConfig {
    fn validate(&mut self) -> Result<(), String> {
        let path = &mut self.base_path;
        let legal = |c: char| c.is_ascii_alphanumeric() || "/_-".contains(c);
        if !path.starts_with('/') || path.contains("//") || !path.chars().all(legal) || path.len() > 128 {
            return Err("base_path must start with `/` and hold only letters, digits, `/`, `_` and `-`".into());
        }
        if !path.ends_with('/') {
            path.push('/');
        }
        if self.users.is_empty() && !self.bind.ip().is_loopback() {
            return Err("a non-loopback bind needs at least one user".into());
        }
        let mut names = std::collections::BTreeSet::new();
        for user in &self.users {
            if user.name.is_empty() || user.name.len() > NAME_BYTES || !names.insert(&user.name) {
                return Err(format!("user names are distinct and 1..={NAME_BYTES} bytes: {:?}", user.name));
            }
            if !PasswordHash::new(&user.hash).is_ok_and(|h| h.algorithm.as_str().starts_with("argon2")) {
                return Err(format!("{}: the hash is not an argon2 PHC string", user.name));
            }
        }
        let session = &self.session;
        if session.idle < Duration::from_secs(1) || session.idle > session.max {
            return Err("session idle must be 1 s or more and no more than max".into());
        }
        if !(Duration::from_millis(100)..=Duration::from_secs(60)).contains(&self.refresh_interval) {
            return Err("refresh_interval must be 100ms..=60s".into());
        }
        if !(1..=4096).contains(&self.max_nodes) {
            return Err("max_nodes must be 1..=4096".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct Source {
    node: String,
    storage: String,
    capabilities: Option<Capabilities>,
    /// When the collector last collected, on this node's clock: its `collected_ago_ms` ages from here.
    #[serde(skip)]
    collected: Option<Instant>,
    /// The collector's own counters, so a lossy pipeline shows on the page it feeds.
    collector: Option<MetricsHealth>,
    error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct History {
    available: bool,
    /// The source used when a request names none.
    node: Option<String>,
    pinned: bool,
    sources: Vec<Source>,
    error: Option<String>,
}

impl History {
    fn choose(&mut self, pin: Option<&str>) {
        let usable = |s: &&Source| s.error.is_none() && s.capabilities.as_ref().is_some_and(|c| c.numeric_history);
        let best = match pin {
            Some(pin) => self.sources.iter().find(|s| s.node == pin),
            None => self.sources.iter().filter(usable).max_by_key(|s| (s.collected, Reverse(&s.node))),
        };
        self.pinned = pin.is_some();
        self.available = best.is_some_and(|s| usable(&s));
        self.node = best.map(|s| s.node.clone());
        self.error = match (best, pin) {
            (Some(source), _) => source.error.clone(),
            (None, Some(pin)) => Some(format!("{pin} does not announce history")),
            (None, None) => self.sources.iter().find_map(|s| s.error.clone()),
        };
    }

    /// The node to ask, honouring `?history=` unless the dashboard is pinned.
    fn source(&self, named: Option<&String>) -> Result<String, Failure> {
        match named.filter(|_| !self.pinned) {
            Some(node) if self.sources.iter().any(|s| &s.node == node) => Ok(node.clone()),
            Some(node) => Err(Failure(StatusCode::BAD_REQUEST, format!("{node} does not announce history"))),
            None => {
                let unavailable = || self.error.clone().unwrap_or_else(|| "history unavailable".into());
                self.node.clone().filter(|_| self.available).ok_or_else(|| Failure::unavailable(unavailable()))
            }
        }
    }
}

#[derive(Serialize)]
struct ObservedNode<'a> {
    node: &'a NodeRecord,
    reachable: bool,
    snapshot: Option<&'a RawValue>,
    error: Option<&'a str>,
}

#[derive(Serialize)]
struct Overview<'a> {
    timestamp_ms: u64,
    build: &'a str,
    observer: &'a str,
    nodes: Vec<ObservedNode<'a>>,
    history: &'a History,
    events: &'a VecDeque<ClusterEvent>,
    discovery_error: Option<&'a str>,
}

#[derive(Default)]
struct Live {
    /// The last snapshot of each node, and the boot it was asked of.
    snapshots: HashMap<String, (u128, Box<RawValue>)>,
    history: History,
    probed: Option<Instant>,
    refreshed: Option<Instant>,
    /// Per node, the boot and position its own timeline was pulled to.
    pulled: BTreeMap<String, (String, u64)>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ActorRow {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    node_incarnation: String,
    incarnation: ActorIncarnation,
    name: Option<String>,
    actor_type: String,
    phase: String,
    restarts: u64,
    started_us: u64,
    restarted_us: Option<u64>,
    diagnostics: serde_json::Value,
}

#[derive(Deserialize)]
struct NodeActors {
    actors: Vec<ActorRow>,
    next: Option<ActorIncarnation>,
    truncated: bool,
}

struct Gathered {
    key: String,
    at: Instant,
    at_ms: u64,
    rows: Vec<ActorRow>,
    truncated: bool,
    errors: BTreeMap<String, String>,
}

struct Inner {
    cluster: Cluster,
    cfg: DashboardConfig,
    address: SocketAddr,
    /// Signs the session cookies; a live stream ends when it changes.
    key: watch::Sender<hmac::Key>,
    tls: Option<Mutex<Arc<ServerConfig>>>,
    attempts: Mutex<BTreeMap<String, Bucket>>,
    frames: watch::Sender<Bytes>,
    live: tokio::sync::Mutex<Live>,
    events: Mutex<(u64, VecDeque<ClusterEvent>)>,
    inventory: tokio::sync::Mutex<Option<Arc<Gathered>>>,
    demand: Mutex<Option<Instant>>,
    closed: AtomicBool,
    streams: Arc<Semaphore>,
    requests: Semaphore,
    stop: Notify,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// A reference to the extension. The system owns it; dropping this handle stops nothing. Once the system's `shutdown`
/// returns, the port is free to bind again.
#[derive(Clone)]
pub struct Dashboard(Arc<Inner>);

impl Dashboard {
    pub async fn start(system: &ActorSystem, cluster: &Cluster, mut cfg: DashboardConfig) -> Result<Self, String> {
        cfg.validate()?;
        let listener = TcpListener::bind(cfg.bind).await.map_err(|e| format!("bind {}: {e}", cfg.bind))?;
        let inner = Arc::new(Inner {
            cluster: cluster.clone(),
            address: listener.local_addr().map_err(|e| e.to_string())?,
            key: watch::Sender::new(signing_key(cfg.session.key.take())?),
            tls: cfg.tls.take().map(Mutex::new),
            attempts: Mutex::default(),
            frames: watch::Sender::new(Bytes::new()),
            live: Default::default(),
            events: Mutex::default(),
            inventory: Default::default(),
            demand: Mutex::default(),
            closed: AtomicBool::new(false),
            streams: Arc::new(Semaphore::new(STREAMS)),
            requests: Semaphore::new(REQUESTS),
            stop: Notify::new(),
            tasks: Mutex::default(),
            cfg,
        });
        system.attach_extension(inner.clone()).map_err(|e| e.to_string())?;

        let weak = Arc::downgrade(&inner);
        let api = Router::new()
            .route("/api/overview", get(overview))
            .route("/api/events", get(events))
            .route("/api/actors", get(actors))
            .route("/api/inventory", get(inventory))
            .route("/api/streams", get(streams))
            .route("/api/key", get(key))
            .route("/api/keys", get(keys))
            .route("/api/connection", get(connection))
            .route("/api/history", get(history))
            .route("/api/series", get(series))
            .route("/api/timeline", get(timeline))
            .route("/api/traces", get(traces))
            .route("/api/trace", get(trace))
            .route("/api/dead-letters", get(dead_letters))
            .route("/api/session", get(session))
            .route_layer(axum::middleware::from_fn_with_state(weak.clone(), authenticate))
            .route("/login", post(login).get(asset).layer(axum::middleware::from_fn(in_time)))
            .route("/logout", post(logout))
            .fallback(asset);
        let base = inner.cfg.base_path.trim_end_matches('/');
        let app = match base.is_empty() {
            true => api,
            false => Router::new().route(&inner.cfg.base_path, get(asset)).nest(base, api),
        };
        let app = app
            .merge(crate::health::routes(system, cluster))
            .layer(axum::middleware::from_fn_with_state(weak.clone(), guard))
            // The default leaves out text/event-stream; the encoder flushes whenever the stream waits, so each event still arrives whole.
            // Fonts and images other than SVG are compressed already.
            .layer(
                tower_http::compression::CompressionLayer::new().compress_when(
                    SizeAbove::new(32)
                        .and(NotForContentType::GRPC)
                        .and(NotForContentType::IMAGES)
                        .and(NotForContentType::const_new("font/")),
                ),
            )
            .with_state(weak.clone());
        let stopping = inner.clone();
        let stopped = async move { stopping.stop.notified().await };
        let mut tasks = Vec::new();
        if inner.tls.is_some() {
            let (sender, accepted) = tokio::sync::mpsc::channel(HANDSHAKES);
            let secure = Handshaken { accepted, address: inner.address };
            tasks.push(tokio::spawn(serve(secure, app, stopped, inner.cfg.trusted_proxies.clone())));
            tasks.push(tokio::spawn(handshake(listener, weak.clone(), sender)));
        } else {
            tasks.push(tokio::spawn(serve(listener, app, stopped, inner.cfg.trusted_proxies.clone())));
        }
        tasks.push(tokio::spawn(run(weak.clone(), inner.cfg.refresh_interval)));
        // Subscribed here, not in the task, so no change after start returns is missed.
        let local = cluster.identity().node_id.clone();
        tasks.push(tokio::spawn(watch_membership(weak, cluster.subscribe(), local)));
        *inner.tasks.lock() = tasks;
        Ok(Self(inner))
    }

    /// Refuses every session cookie signed before: with `key` (32 bytes or more), or a random one.
    /// Dashboards that share a key are each given the new one.
    pub fn sign_out_everyone(&self, key: Option<Vec<u8>>) -> Result<(), String> {
        self.0.key.send_replace(signing_key(key)?);
        self.0.cluster.events().record(
            "signed_out_everyone",
            "",
            "the console refuses every session that it signed before",
        );
        Ok(())
    }

    /// Handshakes from now on present `tls`; connections already open keep theirs.
    pub fn set_tls(&self, tls: Arc<ServerConfig>) -> Result<(), String> {
        *self.0.tls.as_ref().ok_or("the dashboard was started without TLS")?.lock() = tls;
        self.0.cluster.events().record("console_tls_rotated", "", "new handshakes present the new certificate");
        Ok(())
    }

    pub fn address(&self) -> SocketAddr {
        self.0.address
    }

    /// The cluster view as `api/overview` serves it.
    pub async fn overview(&self) -> Bytes {
        self.0.overview().await
    }
}

impl SystemExtension for Inner {
    fn name(&self) -> &str {
        "dashboard"
    }

    /// Returns once the port is free to bind again: a task holds the listener until it ends.
    fn shutdown(&self, _budget: Duration) -> ExtensionFuture {
        self.close();
        let tasks = std::mem::take(&mut *self.tasks.lock());
        Box::pin(async move {
            for task in tasks {
                let _ = task.await;
            }
            Ok(())
        })
    }

    fn close(&self) {
        // An empty frame ends every live stream, which lets the graceful shutdown finish.
        self.closed.store(true, Relaxed);
        self.frames.send_replace(Bytes::new());
        self.stop.notify_one();
        self.tasks.lock().iter().skip(1).for_each(|task| task.abort());
    }
}

async fn run(weak: Weak<Inner>, interval: Duration) {
    every(weak, interval, false, |inner| async move {
        let watched =
            inner.frames.receiver_count() > 0 || inner.demand.lock().is_some_and(|at| at.elapsed() < IDLE_AFTER);
        if watched {
            inner.refresh(interval / 2).await;
        }
    })
    .await
}

/// The newest `LIVE_EVENTS` membership changes and as many from the nodes' timelines, each kept apart: a burst of
/// keyed moves must not push out the `left` a freshly loaded page rebuilds its departures from.
fn trim(events: &mut VecDeque<ClusterEvent>) {
    let (mut membership, mut timeline) = (0, 0);
    events.retain(|e| {
        let kept = if e.id.starts_with("live:") { &mut membership } else { &mut timeline };
        *kept += 1;
        *kept <= LIVE_EVENTS
    });
}

async fn watch_membership(weak: Weak<Inner>, mut subscription: MembershipSubscription, local: String) {
    let mut boots = Boots::default();
    while let Some(event) = subscription.next().await {
        let Some(inner) = weak.upgrade() else { break };
        for (node, kind, detail, _) in boots.told(&event, &local) {
            inner.record(node, kind, detail);
        }
    }
}

/// Hands the server its TLS streams. Handshakes run beside each other, each within a deadline, so a slow
/// client holds up no one else.
async fn handshake(
    listener: TcpListener,
    weak: Weak<Inner>,
    accepted: tokio::sync::mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>,
) {
    let slots = Arc::new(Semaphore::new(HANDSHAKES));
    // Per address, so one that never finishes its handshakes cannot take every slot.
    let pending = PerIp::default();
    loop {
        let Ok(slot) = slots.clone().acquire_owned().await else { return };
        let Ok((tcp, peer)) = listener.accept().await else {
            // Out of descriptors, most likely: give connections time to close.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let Some(inner) = weak.upgrade() else { return };
        let Some(config) = inner.tls.as_ref().map(|tls| tls.lock().clone()) else { return };
        let client = peer.ip().to_canonical();
        let share = if inner.cfg.trusted_proxies.contains(&client) { usize::MAX } else { HANDSHAKES_PER_CLIENT };
        drop(inner);
        let Some(held) = pending.take(client, share) else { continue };
        let accepted = accepted.clone();
        tokio::spawn(async move {
            let shaken = tokio::time::timeout(HANDSHAKE_DEADLINE, tokio_rustls::TlsAcceptor::from(config).accept(tcp));
            if let Ok(Ok(stream)) = shaken.await {
                let _ = accepted.send((stream, peer)).await;
            }
            drop((held, slot));
        });
    }
}

struct Handshaken {
    accepted: tokio::sync::mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    address: SocketAddr,
}

impl axum::serve::Listener for Handshaken {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.accepted.recv().await {
            Some(accepted) => accepted,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.address)
    }
}

impl Inner {
    /// One of the places for requests that ask the nodes or run argon2.
    fn slot(&self) -> Result<tokio::sync::SemaphorePermit<'_>, Failure> {
        self.requests.try_acquire().map_err(|_| Failure(StatusCode::TOO_MANY_REQUESTS, "busy".into()))
    }

    fn record(&self, node: String, kind: &str, detail: String) {
        let mut events = self.events.lock();
        events.0 += 1;
        let id = format!("live:{}", events.0);
        let event =
            ClusterEvent { id, time_ms: now_ms(), node_id: node, kind: kind.into(), subject: String::new(), detail };
        events.1.push_front(event);
        trim(&mut events.1);
    }

    /// One request to each of `nodes`, `PARALLELISM` at a time, within `deadline`.
    async fn ask<T, Fut>(
        &self,
        nodes: Vec<String>,
        deadline: Duration,
        ask: impl Fn(Cluster, String) -> Fut,
    ) -> (Vec<(String, T)>, BTreeMap<String, String>)
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, String>> + Send + 'static,
    {
        let cluster = self.cluster.clone();
        fan_out(nodes, PARALLELISM, deadline, move |node| ask(cluster.clone(), node)).await
    }

    /// One request to every reachable node.
    async fn ask_reachable<T, Fut>(
        &self,
        ask: impl Fn(Cluster, String) -> Fut,
    ) -> (Vec<(String, T)>, BTreeMap<String, String>)
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, String>> + Send + 'static,
    {
        let view = self.cluster.view();
        let ids = view.members.iter().filter(|m| m.reachable).take(self.cfg.max_nodes).map(|m| m.record.id.clone());
        self.ask(ids.collect(), SNAPSHOT_DEADLINE, ask).await
    }

    async fn overview(self: &Arc<Self>) -> Bytes {
        *self.demand.lock() = Some(Instant::now());
        self.refresh(self.cfg.refresh_interval * 2).await;
        let frame = self.frames.borrow().clone();
        // Closing replaces the frame with an empty one.
        let end = frame.len().saturating_sub(2);
        frame.slice(FRAME_HEAD.len().min(end)..end)
    }

    /// Rebuilds the overview unless it is younger than `max_age`. Serialized once; every
    /// stream and poll shares the bytes.
    async fn refresh(self: &Arc<Self>, max_age: Duration) {
        let mut live = self.live.lock().await;
        if live.refreshed.is_some_and(|at| at.elapsed() < max_age) {
            return;
        }
        let view = self.cluster.view();
        let members: Vec<_> = view.members.iter().take(self.cfg.max_nodes).collect();
        let ids: Vec<String> = members.iter().filter(|m| m.reachable).map(|m| m.record.id.clone()).collect();
        let snapshots = self.ask(ids.clone(), SNAPSHOT_DEADLINE, |cluster, node| async move {
            fetch::<Box<RawValue>>(&cluster, &node, &Request::Snapshot).await
        });
        let ((done, mut errors), ()) = tokio::join!(snapshots, self.pull_events(ids, &mut live.pulled));
        #[expect(clippy::disallowed_methods, reason = "keeps or drops each entry on its own key")]
        // A restarted node that does not answer must not show its last boot's snapshot.
        live.snapshots.retain(|node, (boot, _)| view.get(node).is_some_and(|m| m.record.incarnation == *boot));
        for (node, snapshot) in done {
            let boot = view.get(&node).map_or(0, |m| m.record.incarnation);
            live.snapshots.insert(node, (boot, snapshot));
        }
        for member in members.iter().filter(|m| !m.reachable) {
            errors.insert(member.record.id.clone(), "unreachable from the dashboard node".into());
        }
        if live.probed.is_none_or(|at| at.elapsed() >= PROBE_EVERY) {
            live.history = self.probe().await;
            live.probed = Some(Instant::now());
        }
        // Aged on every frame, not only at each probe, so it counts up between them.
        for source in &mut live.history.sources {
            if let (Some(health), Some(at)) = (&mut source.collector, source.collected) {
                health.collected_ago_ms = Some(at.elapsed().as_millis() as u64);
            }
        }

        let events = self.events.lock().1.clone();
        let overview = Overview {
            timestamp_ms: now_ms(),
            build: &BUILD,
            observer: &self.cluster.identity().node_id,
            nodes: (members.iter())
                .map(|m| ObservedNode {
                    node: &m.record,
                    reachable: m.reachable,
                    snapshot: live.snapshots.get(&m.record.id).map(|(_, s)| &**s),
                    error: errors.get(&m.record.id).map(String::as_str),
                })
                .collect(),
            history: &live.history,
            events: &events,
            discovery_error: view.stale.as_deref(),
        };
        let mut frame = FRAME_HEAD.to_vec();
        let written = match self.cfg.redact {
            true => serde_json::to_value(&overview).and_then(|mut value| {
                redact(&mut value, &self.key.borrow().clone());
                serde_json::to_writer(&mut frame, &value)
            }),
            false => serde_json::to_writer(&mut frame, &overview),
        };
        if written.is_ok() {
            frame.extend_from_slice(b"\n\n");
            live.refreshed = Some(Instant::now());
            if !self.closed.load(Relaxed) {
                self.frames.send_replace(frame.into());
            }
        }
    }

    /// Each node's own timeline since it was last pulled, into the live feed beside the membership changes.
    async fn pull_events(&self, nodes: Vec<String>, pulled: &mut BTreeMap<String, (String, u64)>) {
        let me = self.cluster.identity().node_id.clone();
        let incarnation = boot(self.cluster.identity().incarnation);
        let after = pulled.get(&me).filter(|(boot, _)| *boot == incarnation).map_or(0, |(_, after)| *after);
        let others = nodes.into_iter().filter(|node| *node != me).collect();
        let (mut done, _) = self
            .ask(others, SNAPSHOT_DEADLINE, |cluster, node| {
                let (incarnation, after) = pulled.get(&node).cloned().unwrap_or_default();
                let request = Request::Events { incarnation, after, limit: LIVE_EVENTS };
                async move { fetch::<EventPage>(&cluster, &node, &request).await }
            })
            .await;
        // Its own is read in place: a console shows its sign-ins without Telemetry on its node.
        let own = self.cluster.events().since(after, LIVE_EVENTS);
        let next = own.last().map_or(after, |e| e.seq);
        let events = own.iter().map(|e| event(e, &me, &incarnation)).collect();
        done.push((me, EventPage { incarnation, next, missed: 0, events }));
        let view = self.cluster.view();
        pulled.retain(|node, _| view.get(node).is_some());
        let mut events = self.events.lock();
        for (node, page) in done {
            pulled.insert(node, (page.incarnation, page.next));
            events.1.extend(page.events);
        }
        events.1.make_contiguous().sort_by_key(|e| Reverse(e.time_ms));
        trim(&mut events.1);
    }

    async fn probe(&self) -> History {
        let announced: BTreeMap<_, _> = history_sources(&self.cluster).into_iter().collect();
        let nodes = announced.keys().cloned().collect();
        let (done, errors) = self
            .ask(nodes, SNAPSHOT_DEADLINE, |cluster, node| async move {
                let capabilities: Capabilities = query_metrics(&cluster, &node, &MetricsRequest::Capabilities).await?;
                let health: MetricsHealth = query_metrics(&cluster, &node, &MetricsRequest::Health).await?;
                // Stamped as it answers, so a slower collector in the same round does not make this one look fresher.
                let ago = health.collected_ago_ms.map(Duration::from_millis);
                Ok((capabilities, (ago.and_then(|ago| Instant::now().checked_sub(ago)), health)))
            })
            .await;
        let mut probed: HashMap<_, _> = done.into_iter().collect();
        let sources = announced.into_iter().map(|(node, storage)| {
            let (capabilities, answer) = probed.remove(&node).unzip();
            let (collected, collector) = answer.unzip();
            let error = errors.get(&node).cloned();
            Source { error, node, storage, capabilities, collected: collected.flatten(), collector }
        });
        let mut history = History { sources: sources.collect(), ..Default::default() };
        history.choose(self.cfg.history_node.as_deref());
        history
    }

    /// The node to ask, honouring `?history=` unless the dashboard is pinned.
    async fn source(&self, named: Option<&String>) -> Result<String, Failure> {
        self.live.lock().await.history.source(named)
    }

    /// `source`'s node, or `None` where its store says it lacks what `needs` reads.
    async fn store_for(
        &self,
        named: Option<&String>,
        needs: fn(&Capabilities) -> bool,
    ) -> Result<Option<String>, Failure> {
        let live = self.live.lock().await;
        let history = &live.history;
        let node = history.source(named)?;
        let lacks = |s: &Source| s.node == node && s.capabilities.as_ref().is_some_and(|c| !needs(c));
        Ok((!history.sources.iter().any(lacks)).then_some(node))
    }

    /// A failed source is set aside until the next probe, so the next request fails over.
    async fn failed(&self, node: &str, error: &str) {
        let mut live = self.live.lock().await;
        if let Some(source) = live.history.sources.iter_mut().find(|s| s.node == node) {
            source.error = Some(error.to_owned());
        }
        live.history.choose(self.cfg.history_node.as_deref());
    }

    /// Asks the metrics store on `node`, setting it aside when that fails.
    async fn ask_store<T: serde::de::DeserializeOwned>(
        &self,
        node: &str,
        request: &crate::MetricsRequest,
    ) -> Result<T, Failure> {
        let answer = query_metrics(&self.cluster, node, request).await;
        if let Err(error) = &answer {
            self.failed(node, error).await;
        }
        answer.map_err(Failure::upstream)
    }

    async fn gather(self: &Arc<Self>, query: ActorQuery, node: Option<&String>) -> Result<Arc<Gathered>, Failure> {
        // The inventory pages over what it gathered itself; a per-node cursor or id has no meaning here.
        let query = ActorQuery { id: None, after: None, ..query };
        let key = format!("{node:?}|{:?}|{:?}|{:?}|{}", query.search, query.actor_type, query.phase, query.timings);
        let mut cached = self.inventory.lock().await;
        if let Some(found) = cached.as_ref().filter(|g| g.key == key && g.at.elapsed() < INVENTORY_TTL) {
            return Ok(found.clone());
        }
        let view = self.cluster.view();
        let members = view.members.iter().filter(|m| m.reachable && node.is_none_or(|n| n == &m.record.id));
        let members: BTreeMap<String, String> =
            members.take(self.cfg.max_nodes).map(|m| (m.record.id.clone(), boot(m.record.incarnation))).collect();
        if members.is_empty() && node.is_some() {
            return Err(Failure(StatusCode::NOT_FOUND, "node is not in the current membership".into()));
        }
        let budget = (INVENTORY_ROWS / members.len().max(1)).clamp(1, 5000);
        let (done, errors) = self
            .ask(members.keys().cloned().collect(), INVENTORY_DEADLINE, move |cluster, node| {
                let mut query = query.clone();
                async move {
                    let mut rows = Vec::new();
                    loop {
                        query.limit = (budget - rows.len()).min(1000);
                        let page: NodeActors = fetch(&cluster, &node, &Request::Actors(query.clone())).await?;
                        rows.extend(page.actors);
                        query.after = page.next;
                        if page.next.is_none() || page.truncated || rows.len() >= budget {
                            return Ok((rows, page.next.is_some()));
                        }
                    }
                }
            })
            .await;
        let mut gathered = Gathered {
            key,
            at: Instant::now(),
            at_ms: now_ms(),
            rows: Vec::new(),
            truncated: !errors.is_empty(),
            errors,
        };
        for (node, (rows, truncated)) in done {
            gathered.truncated |= truncated;
            let incarnation = members.get(&node).cloned().unwrap_or_default();
            gathered.rows.extend(rows.into_iter().map(|mut row| {
                (row.node_id, row.node_incarnation) = (node.clone(), incarnation.clone());
                row
            }));
        }
        let gathered = Arc::new(gathered);
        *cached = Some(gathered.clone());
        Ok(gathered)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn build_id_is_the_bundle_hash() {
        assert!(super::BUILD.len() >= 8 && !super::BUILD.contains('.'), "{}", *super::BUILD);
    }
}
