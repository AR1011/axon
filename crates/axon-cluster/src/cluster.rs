use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use axon::{
    ActorHandle, ActorSystem, ExtensionFuture, LeaveReason, LifecycleHandle, Reject, RejectReason, SupervisedHandle,
    SystemExtension, WeakActorSystem,
};
use axon_net::{AcceptLimits, Backoff, Identity, Lane};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Instant, timeout, timeout_at};

use crate::discovery::{
    BoxFuture, Caught, DRAINING, Discovery, KINDS, LEAVE_REASON, LIFECYCLE, NodeRecord, ProviderInfo, Snapshot,
    check_nodes,
};
use crate::engine::{Admit, Authorizer, Engine, EngineConfig, EngineStats, Entry, Target, Tls, Waiting};
use crate::membership::{MemberChange, Membership, MembershipEvent, MembershipSubscription, View};
use crate::ownership::OwnershipAuthority;
use crate::spawn::{Kind, Lease, Unique};
use crate::wire::*;
use crate::{ClusterError, DiscoveryError, NamedRef, Nodes, Placement, Rejection, RemoteRef};

const MAX_CONTROL_REQUEST: usize = 64 << 10;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(4);

pub struct ClusterConfig {
    pub bind: SocketAddr,
    /// The address peers dial. Defaults to the bound address; a port of 0 takes the bound port.
    pub advertise: Option<SocketAddr>,
    pub transport: axon_net::Config,
    pub tls: Option<Tls>,
    pub discovery: Arc<dyn Discovery>,
    /// Advertised in discovery metadata and matched by `Placement`. Bounded so that every provider, Consul included,
    /// takes them: at most 60 entries, the cluster's own `axon.*` ones included, keys of 1 to 60 bytes with no control
    /// character, and values of at most 512 bytes.
    pub labels: BTreeMap<String, String>,
    pub authorizer: Option<Arc<dyn Authorizer>>,
    /// Makes keyed actors strictly owned: a lease is taken before each one starts.
    pub ownership: Option<Arc<dyn OwnershipAuthority>>,
    pub accept: AcceptLimits,
    /// Redials of a peer. Its `max` (2 s) bounds how long calls wait once a cut to a listed peer heals.
    pub backoff: Backoff,
    pub request_timeout: Duration,
    pub operation_timeout: Duration,
    pub poll_interval: Duration,
    /// Takes the place of `poll_interval` when the discovery provider reports changes by itself.
    pub resync_interval: Duration,
    pub heartbeat_interval: Duration,
    /// How often the leases are renewed. Defaults to 2/5 of the grace.
    pub lease_renew_interval: Option<Duration>,
    /// A strictly-owned actor stops this long after the last renew that extended its lease was sent, answered or not.
    /// Defaults to 7/8 of the authority's [`hold`](OwnershipAuthority::hold), or 5 s when it has none. Keep it well
    /// inside the lease TTL.
    pub lease_grace: Option<Duration>,
    /// How long discovery may fail to hear this node, or to answer it, before the node asks its peers
    /// whether they still list it, and stops its keyed actors if none does. Keep it well inside the
    /// discovery provider's TTL.
    pub heartbeat_grace: Duration,
    pub route_ttl: Duration,
    /// An outbound link that has carried no call for this long is closed; the next call dials again.
    pub idle_link_timeout: Duration,
    /// How long after a membership change keyed actors are moved to their new owners. Changes within it move keys
    /// together.
    pub rebalance_settle: Duration,
    pub max_nodes: usize,
    pub max_peers: usize,
    pub max_pending: usize,
    pub max_inbound_replies: usize,
    pub max_endpoints: usize,
    /// Most actors other nodes may have this one run, as counted by `ClusterStats::spawned`; past it a start is refused
    /// `SpawnRefusal::Capacity`. `RuntimeConfig::max_actors` and `PaxosConfig::max_leases` bound keyed actors too.
    pub max_spawned: usize,
    /// Control requests served at once; the rest wait up to `control_queue_wait`.
    pub max_control_handlers: usize,
    pub control_queue_wait: Duration,
    /// A control reply is one frame, so it is also bounded by the session's `max_frame`.
    pub max_control_response: usize,
    /// Compress control requests and replies even when the transport codec is none.
    pub compress_control: bool,
    pub lookup_parallelism: usize,
    pub event_capacity: usize,
}

impl ClusterConfig {
    /// Tightens the transport keepalive to 1 s / 4 s idle: it is the cluster's failure detector. A dial's connect gets
    /// 1.5 s, or the handshake timeout where that is shorter, so a SYN lost once, which the kernel sends again at 1 s,
    /// still connects. `Cluster::start` tightens the idle timeout further to the ownership authority's
    /// [`hold`](OwnershipAuthority::hold) when that is shorter, and the keepalive and the connect to a quarter of it.
    pub fn new(bind: SocketAddr, mut transport: axon_net::Config, discovery: impl Discovery) -> Self {
        transport.keepalive_interval = Duration::from_secs(1);
        transport.idle_timeout = Duration::from_secs(4);
        // `validate` refuses a connect longer than the handshake it is part of.
        transport.connect_timeout = Duration::from_millis(1500).min(transport.handshake_timeout);
        Self {
            bind,
            advertise: None,
            transport,
            tls: None,
            discovery: Arc::new(discovery),
            labels: BTreeMap::new(),
            authorizer: None,
            ownership: None,
            accept: AcceptLimits::default(),
            backoff: Backoff { max: Duration::from_secs(2), ..Backoff::default() },
            request_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_secs(1),
            resync_interval: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(1),
            lease_renew_interval: None,
            lease_grace: None,
            heartbeat_grace: Duration::from_secs(5),
            route_ttl: Duration::from_secs(5),
            idle_link_timeout: Duration::from_secs(300),
            rebalance_settle: Duration::from_millis(500),
            max_nodes: 4096,
            max_peers: 4096,
            max_pending: 4096,
            max_inbound_replies: 4096,
            max_endpoints: 1 << 20,
            max_spawned: 1 << 16,
            max_control_handlers: 8,
            control_queue_wait: Duration::from_secs(1),
            max_control_response: 16 << 20,
            compress_control: true,
            lookup_parallelism: 8,
            event_capacity: 256,
        }
    }
}

/// Who may call a control service. Judged on the authenticated session identity, never on
/// anything in the request.
#[derive(Clone, Debug, Default)]
pub enum AccessPolicy {
    #[default]
    Deny,
    Nodes(BTreeSet<String>),
    Cluster,
}

/// A raw endpoint before it is served: [`Cluster::endpoint`].
pub struct EndpointBuilder<'a> {
    cluster: &'a Cluster,
    name: &'a str,
    protocol: u64,
    schema: u16,
    lane: Lane,
}

impl EndpointBuilder<'_> {
    /// The lane its calls take. `Control` is axon's own: only a name under `axon.` takes it.
    pub fn lane(mut self, lane: Lane) -> Self {
        self.lane = lane;
        self
    }

    /// As [`Cluster::serve`].
    pub fn serve<F>(self, handler: F) -> Result<u64, ClusterError>
    where
        F: Fn(&Identity, Bytes, Option<crate::Reply>) -> Result<(), Rejection> + Send + Sync + 'static,
    {
        let Self { cluster, name, protocol, schema, lane } = self;
        if lane == Lane::Control && !name.starts_with("axon.") {
            return Err(ClusterError::Config("Lane::Control takes only endpoints named axon.*"));
        }
        let admit: Admit = Arc::new(move |peer, body, reply| handler(&peer.identity, body, reply).map(|()| None));
        cluster.0.engine.register(None, true, Entry { name: name.into(), protocol, schema, lane, admit })
    }
}

pub struct ControlRequest {
    pub peer: Identity,
    pub body: Bytes,
    pub max_response: usize,
}

pub(crate) struct Route {
    pub incarnation: u128,
    pub protocol: u64,
    /// Each schema the name is served at, the endpoint that takes it and the lane it is served on.
    pub endpoints: Arc<[Endpoint]>,
}

/// An endpoint id, its schema and its lane: one entry of a route.
pub(crate) type Endpoint = (u64, u16, Lane);

impl Route {
    /// The endpoint that takes `schema` of `protocol`, or any for the reserved protocol 0, `Unaddressed`.
    pub fn target(&self, node: Arc<str>, protocol: u64, schema: u16) -> Result<Target, ClusterError> {
        if protocol != 0 && protocol != self.protocol {
            return Err(ClusterError::ProtocolMismatch);
        }
        let Some(&(endpoint, schema, lane)) = self.endpoints.iter().find(|(_, s, _)| protocol == 0 || *s == schema)
        else {
            let mut served: Vec<u16> = self.endpoints.iter().map(|(_, s, _)| *s).collect();
            served.sort_unstable();
            return Err(ClusterError::SchemaMismatch { wanted: schema, served });
        };
        Ok(Target { node, incarnation: self.incarnation, endpoint, schema, lane })
    }

    /// `protocol u64 | (endpoint u64 | schema u16 | lane u8)+`
    pub fn put(out: &mut BytesMut, protocol: u64, endpoints: impl IntoIterator<Item = Endpoint>) {
        out.put_u64(protocol);
        for (endpoint, schema, lane) in endpoints {
            out.put_u64(endpoint);
            out.put_u16(schema);
            out.put_u8(lane.byte());
        }
    }

    /// `None` for a malformed route, one with a lane this build does not know among them.
    pub fn decode(incarnation: u128, mut body: Bytes) -> Option<Self> {
        if body.len() < 19 || body.len() % 11 != 8 {
            return None;
        }
        let protocol = body.get_u64();
        let endpoints = std::iter::from_fn(|| {
            body.has_remaining().then(|| Some((body.get_u64(), body.get_u16(), Lane::from_byte(body.get_u8())?)))
        });
        Some(Self { incarnation, protocol, endpoints: endpoints.collect::<Option<_>>()? })
    }
}

/// One `SCHEMA` of a protocol, decoded straight into an actor's mailbox.
pub(crate) struct Schema<M> {
    pub protocol: u64,
    pub schema: u16,
    pub lane: Lane,
    pub decode: fn(Bytes, Option<crate::Reply>) -> Result<M, Rejection>,
}

/// A protocol takes `Live` or `Bulk`: `publish` and `kind` refuse `Control`, axon's own.
pub(crate) fn refuse_control(lane: Lane) -> Result<(), ClusterError> {
    match lane {
        Lane::Control => Err(ClusterError::Config("Lane::Control is axon's own: a protocol takes Live or Bulk")),
        Lane::Live | Lane::Bulk => Ok(()),
    }
}

impl<M> Clone for Schema<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Copy for Schema<M> {}

impl<M> Schema<M> {
    pub fn of<P: ActorProtocol<Mailbox = M>>() -> Self {
        let decode = |body, reply: Option<crate::Reply>| P::decode(body, reply.map(|r| r.typed(P::encode_reply)));
        Self { protocol: P::ID, schema: P::SCHEMA, lane: P::LANE, decode }
    }
}

#[derive(Default)]
struct Routes {
    stale_mark: u64,
    entries: HashMap<(Arc<str>, String), (Instant, Route)>,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub route_hits: AtomicU64,
    pub route_misses: AtomicU64,
    pub spawn_requests: AtomicU64,
    pub spawn_refused: AtomicU64,
    pub handoffs: AtomicU64,
    pub handoffs_failed: AtomicU64,
    pub keyed_failed: AtomicU64,
    pub leases_lost: AtomicU64,
    pub heartbeat_failures: AtomicU64,
    pub isolations: AtomicU64,
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ClusterStats {
    pub engine: EngineStats,
    pub members: usize,
    pub unreachable: usize,
    pub joined: u64,
    pub left: u64,
    pub replaced: u64,
    pub became_unreachable: u64,
    pub discovery_errors: u64,
    pub heartbeat_failures: u64,
    pub last_heartbeat_error: Option<String>,
    pub route_hits: u64,
    pub route_misses: u64,
    /// Starts other nodes asked for, keyed or not, from the reservation before the lease to the actor's end: it counts
    /// starts still waiting for a lease and actors still draining.
    pub spawned: usize,
    pub spawn_requests: u64,
    pub spawn_refused: u64,
    pub handoffs: u64,
    /// Keyed actors this node gave up whose new owner did not start them. They start again on the next `get_or_spawn`.
    pub handoffs_failed: u64,
    /// Keyed actors that stopped on a failure of their own (a handler error, a panic, a start that failed) and
    /// were not restarted. The next `get_or_spawn` starts the key again.
    pub keyed_failed: u64,
    /// Keys whose lease this node gave up because it could not be renewed.
    pub leases_lost: u64,
    /// Times this node stopped its keyed actors because no peer could confirm its membership.
    pub isolations: u64,
    pub isolated: bool,
    /// [`Cluster::warming`].
    pub warming: bool,
    /// [`Cluster::quotas_starting`].
    pub quotas_starting: bool,
    /// Keys of eager kinds due to start again here, each until it starts, runs elsewhere, or a minute and a lease
    /// after its first try.
    pub eager_waiting: usize,
    /// Questions about a node gone, or this one restarted, that the ownership authority has not answered yet: each a gone
    /// node's for 30 s, this node's own for a minute and a lease.
    pub eager_asking: usize,
    pub discovery: ProviderInfo,
    /// `None` when keyed actors are unique per membership view only.
    pub ownership: Option<ProviderInfo>,
    /// `None` without [`Quotas`](crate::Quotas).
    pub quotas: Option<crate::QuotaStats>,
}

pub(crate) struct Inner {
    pub engine: Arc<Engine>,
    pub bound: SocketAddr,
    pub system: WeakActorSystem,
    pub discovery: Arc<dyn Discovery>,
    pub ownership: Option<Arc<dyn OwnershipAuthority>>,
    pub record: Mutex<NodeRecord>,
    pub kinds: RwLock<BTreeMap<String, Arc<Kind>>>,
    pub unique: Mutex<BTreeMap<(String, String), Unique>>,
    /// The leases held here, by name, from the acquire until the release.
    pub leases: Mutex<BTreeMap<String, Lease>>,
    /// Leases given up, with the epoch lost, until the actor that held one releases it.
    pub lost: Mutex<HashMap<String, u64>>,
    pub lease_grace: Duration,
    /// One per shard of keys: a key is started, and cleared after it ends, under its gate.
    pub gates: Box<[tokio::sync::Mutex<()>]>,
    /// Bumped under `unique` whenever every keyed actor is told to stop, so a start in flight notices.
    pub sweeps: AtomicU64,
    pub rebalance_settle: Duration,
    resync_interval: Duration,
    /// A leave's hand-offs, each under way from its actor's end, and the drain of `eager`, which `finish_leaving`
    /// waits for.
    pub handoff: Mutex<tokio::task::JoinSet<()>>,
    /// Read by each lease release under way, which the leases map no longer lists: `release_all` waits for them.
    pub releasing: Arc<tokio::sync::RwLock<()>>,
    /// Hand-offs to this node under way, by key.
    pub taking: Mutex<HashMap<(String, String), usize>>,
    /// When the draining record has been up a poll interval.
    withdraw_after: Mutex<Instant>,
    /// Keys asked of their new owner at once. A quarter of `max_pending`, at most 256, so a join that moves many keys
    /// paces itself instead of having the calls beyond it refused `Busy`, and leaves room for other calls.
    pub respawns: Semaphore,
    pub spawned: AtomicUsize,
    /// The last `SpawnContext::start` handed out. Its own `Arc`, as a supervised kind's restarts count it too.
    pub starts: Arc<AtomicU64>,
    pub spread: AtomicUsize,
    pub rebalance: Notify,
    /// The watches held here, by key: woken when its actor starts, ends or loses its lease.
    pub watches: Mutex<BTreeMap<(String, String), Arc<Notify>>>,
    pub counters: Counters,
    pub operation_timeout: Duration,
    pub max_spawned: usize,
    routes: Mutex<Routes>,
    route_ttl: Duration,
    max_nodes: usize,
    lookup_parallelism: usize,
    control_slots: Arc<Semaphore>,
    control_queue_wait: Duration,
    max_control_response: usize,
    compress_control: bool,
    heartbeat_error: Mutex<Option<String>>,
    pub heartbeat_grace: Duration,
    heartbeat_ok: Mutex<Instant>,
    snapshot_ok: Mutex<Instant>,
    /// Held from asking discovery for a snapshot to applying it, so no answer replaces one asked for after it.
    refreshing: tokio::sync::Mutex<()>,
    /// Snapshots applied or failed: a watched answer is dropped if one was done while it was on its way.
    refreshes: AtomicU64,
    /// Set while discovery cannot hear this node and no peer vouches for it: no keyed actor starts here.
    pub isolated: AtomicBool,
    /// Since when this isolated node has been heard with no gap that fails the check again: it rejoins once that is a
    /// `heartbeat_grace` ago.
    pub heard_since: Mutex<Option<Instant>>,
    /// Heard once since it started, or listed in its own view as it isolated: `Inner::set_isolated`.
    pub joined: AtomicBool,
    /// The ownership authority's `warming` as the heartbeat last saw it, so each change is one event.
    warming: AtomicBool,
    /// Keys of eager kinds due to start again here, one entry each whatever listed it.
    pub eager: Mutex<crate::spawn::Waiting>,
    /// What the eager asker is to ask the authority, posted by the rebalance loop, which wakes it through `eager_posted`.
    pub asking: Mutex<crate::spawn::Asking>,
    pub eager_posted: Notify,
    /// The system's event log, kept here for what records into it without the system: keyed moves, streams.
    pub events: Arc<axon::EventLog>,
    pub clock: axon::Clock,
    pub tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub quotas: std::sync::OnceLock<Arc<crate::quota::Shared>>,
}

#[derive(Clone)]
pub struct Cluster(pub(crate) Arc<Inner>);

/// A handle that does not keep the cluster alive, for what the cluster itself holds: an ownership authority and a
/// kind's factory keep one, or the cluster could never drop. A factory gets one from
/// [`SpawnContext::cluster`](crate::SpawnContext::cluster). Any other code, an actor included, may hold a `Cluster`.
#[derive(Clone, Debug)]
pub struct WeakCluster(pub(crate) std::sync::Weak<Inner>);

impl WeakCluster {
    pub fn upgrade(&self) -> Option<Cluster> {
        self.0.upgrade().map(Cluster)
    }
}

/// The answer to a request [`start_request_to`](Cluster::start_request_to) admitted, due `request_timeout` after its
/// admission. Until it ends or is dropped it holds a call slot, and the request is counted and traced as one call.
#[must_use = "dropping it abandons the request"]
pub struct PendingReply(BoxFuture<'static, Result<Bytes, ClusterError>>);

impl Future for PendingReply {
    type Output = Result<Bytes, ClusterError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl Cluster {
    /// Binds, registers with discovery, takes a first membership snapshot, and attaches to
    /// the system so that `system.shutdown()` leaves the cluster gracefully.
    pub async fn start(system: &ActorSystem, cfg: ClusterConfig) -> Result<Self, ClusterError> {
        let advertised = cfg.advertise.unwrap_or(cfg.bind);
        if advertised.ip().is_unspecified() {
            return Err(ClusterError::Config("binding a wildcard address requires `advertise`"));
        }
        if cfg.heartbeat_grace.is_zero() {
            return Err(ClusterError::Config("heartbeat_grace must be above zero"));
        }
        let hold = cfg.ownership.as_ref().and_then(|o| o.hold());
        let lease_grace = cfg.lease_grace.or(hold.map(|h| h * 7 / 8)).unwrap_or(Duration::from_secs(5));
        if hold.is_some_and(|hold| lease_grace > hold) {
            return Err(ClusterError::Config(
                "lease_grace must end before the holder's lease: lower it or raise the lease",
            ));
        }
        let renew_every = cfg.lease_renew_interval.unwrap_or(lease_grace * 2 / 5);
        if renew_every.is_zero() {
            return Err(ClusterError::Config("lease_renew_interval must be above zero"));
        }
        // A node that isolates after its peers dropped it runs its keys twice in between; a lease fences them instead.
        let stops_within = cfg.heartbeat_interval.saturating_add(cfg.heartbeat_grace.saturating_mul(3) / 2);
        if cfg.ownership.is_none() && cfg.discovery.drops_after().is_some_and(|drops| stops_within >= drops) {
            return Err(ClusterError::Config(
                "heartbeat_interval + 3/2 heartbeat_grace must be under the discovery's drops_after without an \
                 ownership authority",
            ));
        }
        let mut transport = cfg.transport;
        // A holder silent for a whole lease holds nothing. A session that quiet is not trusted, or a caller holding one
        // would send the holder's keys there until the idle timeout, long after the lease let them go.
        if let Some(hold) = hold.filter(|hold| *hold < transport.idle_timeout) {
            transport.idle_timeout = hold;
            transport.keepalive_interval = transport.keepalive_interval.min(hold / 4);
            transport.connect_timeout = transport.connect_timeout.min(hold / 4);
        }
        let membership = Membership::new(cfg.event_capacity);
        let engine_cfg = EngineConfig {
            transport,
            backoff: cfg.backoff,
            request_timeout: cfg.request_timeout,
            poll_interval: cfg.poll_interval,
            idle_link_timeout: cfg.idle_link_timeout,
            max_peers: cfg.max_peers,
            max_pending: cfg.max_pending,
            max_inbound_replies: cfg.max_inbound_replies,
            max_endpoints: cfg.max_endpoints,
            tracer: system.tracer().clone(),
            dead: system.dead_letters().clone(),
            events: system.events().clone(),
        };
        let (engine, bound) =
            Engine::start(cfg.bind, cfg.accept, engine_cfg, cfg.tls, cfg.authorizer, membership).await?;
        let identity = &engine.local.identity;
        let mut address = cfg.advertise.unwrap_or(bound);
        if address.port() == 0 {
            address.set_port(bound.port());
        }
        let record = NodeRecord {
            id: identity.node_id.clone(),
            incarnation: identity.incarnation,
            address,
            metadata: cfg.labels,
            version: 0,
        };
        if let Err(error) = record.validate() {
            engine.close();
            engine.released().await;
            return Err(error.into());
        }
        let inner = Arc::new(Inner {
            engine,
            bound,
            system: system.downgrade(),
            events: system.events().clone(),
            clock: system.clock().clone(),
            discovery: Arc::new(Caught(cfg.discovery)),
            ownership: cfg.ownership.map(|o| Arc::new(Caught(o)) as Arc<dyn OwnershipAuthority>),
            record: Mutex::new(record),
            kinds: RwLock::default(),
            unique: Mutex::default(),
            leases: Mutex::default(),
            lost: Mutex::default(),
            lease_grace,
            gates: (0..256).map(|_| tokio::sync::Mutex::new(())).collect(),
            sweeps: AtomicU64::new(0),
            rebalance_settle: cfg.rebalance_settle,
            resync_interval: cfg.resync_interval,
            handoff: Mutex::default(),
            releasing: Arc::default(),
            taking: Mutex::default(),
            withdraw_after: Mutex::new(Instant::now()),
            respawns: Semaphore::new((cfg.max_pending / 4).clamp(1, 256)),
            spawned: AtomicUsize::new(0),
            starts: Arc::default(),
            spread: AtomicUsize::new(0),
            rebalance: Notify::new(),
            watches: Mutex::default(),
            counters: Counters::default(),
            operation_timeout: cfg.operation_timeout,
            max_spawned: cfg.max_spawned,
            routes: Mutex::default(),
            route_ttl: cfg.route_ttl,
            max_nodes: cfg.max_nodes,
            lookup_parallelism: cfg.lookup_parallelism.max(1),
            control_slots: Arc::new(Semaphore::new(cfg.max_control_handlers)),
            control_queue_wait: cfg.control_queue_wait,
            max_control_response: cfg.max_control_response.max(1024),
            compress_control: cfg.compress_control,
            heartbeat_error: Mutex::default(),
            heartbeat_grace: cfg.heartbeat_grace,
            heartbeat_ok: Mutex::new(Instant::now()),
            snapshot_ok: Mutex::new(Instant::now()),
            refreshing: tokio::sync::Mutex::default(),
            refreshes: AtomicU64::new(0),
            isolated: AtomicBool::new(false),
            heard_since: Mutex::default(),
            joined: AtomicBool::new(false),
            warming: AtomicBool::new(false),
            eager: Mutex::default(),
            asking: Mutex::default(),
            eager_posted: Notify::new(),
            tasks: Mutex::default(),
            quotas: std::sync::OnceLock::new(),
        });
        inner.discovery.attach(&inner.events);
        let started = async {
            inner.register_system_endpoints()?;
            if let Some(authority) = &inner.ownership {
                authority.attach(&Cluster(inner.clone())).await?;
            }
            inner.announce_until(Instant::now() + inner.operation_timeout * 3).await?;
            inner.refresh().await;
            let (poll, heartbeat) = (cfg.poll_interval, cfg.heartbeat_interval);
            let mut tasks = vec![
                tokio::spawn(inner.clone().poll_loop(poll, heartbeat)),
                tokio::spawn(inner.clone().rebalance_loop()),
            ];
            if inner.ownership.is_some() {
                tasks.push(tokio::spawn(inner.clone().renew_loop(renew_every)));
                tasks.push(tokio::spawn(inner.clone().eager_loop()));
            }
            *inner.tasks.lock() = tasks;
            system.attach_extension(Arc::new(Extension(inner.clone())))?;
            Ok::<(), ClusterError>(())
        }
        .await;
        if let Err(error) = started {
            // A register that timed out may still have landed.
            let _ = inner.withdraw().await;
            inner.close();
            inner.engine.released().await;
            return Err(error);
        }
        Ok(Self(inner))
    }

    pub fn downgrade(&self) -> WeakCluster {
        WeakCluster(Arc::downgrade(&self.0))
    }

    pub fn identity(&self) -> &Identity {
        &self.0.engine.local.identity
    }

    /// The address the listener is bound to, which `advertise` may hide from the record.
    pub fn local_addr(&self) -> SocketAddr {
        self.0.bound
    }

    /// Swaps the certificates this node serves and dials with. New sessions use them; sessions already up keep theirs
    /// until they end. Refused, changing nothing, on a node started without TLS, or when the certificates would not
    /// pass as this node: a handshake with themselves under the name peers dial it by (its id when that is a server
    /// name, else its address), and its authenticator taking what each side shows as its id.
    pub fn set_tls(&self, tls: Tls) -> Result<(), ClusterError> {
        self.0.engine.set_tls(tls, &self.record()).map_err(ClusterError::Config)?;
        self.0.events.record("tls_rotated", "", "new sessions and redials take the new certificates");
        Ok(())
    }

    /// Swaps what decides the calls, spawns and watches peers may make; `None` allows every authenticated peer
    /// everything. Each one that arrives after it returns is checked by the new one. What was admitted before runs to
    /// its end: a request answers, and a watch's held question answers, after which the watch asks again and, refused,
    /// ends. A stream the peer follows stops too: its interest refreshes are refused, and its parent stream node drops a
    /// child it has not heard from for `StreamNodeConfig::downstream_expiry` (checked every `tick`).
    pub fn set_authorizer(&self, authorizer: Option<Arc<dyn Authorizer>>) {
        let detail = if authorizer.is_some() { "set" } else { "removed: every authenticated peer is allowed" };
        self.0.engine.set_authorizer(authorizer);
        self.0.events.record("authorizer_changed", "", detail);
    }

    /// This node's discovery record, including the address peers dial.
    pub fn record(&self) -> NodeRecord {
        self.0.record.lock().clone()
    }

    /// Sets or clears one entry of this node's discovery metadata and announces it. The keys
    /// the cluster maintains itself are refused, and so is an entry past the bounds `ClusterConfig::labels` names.
    pub async fn set_metadata(&self, key: &str, value: Option<&str>) -> Result<(), ClusterError> {
        if [LIFECYCLE, LEAVE_REASON, KINDS, crate::quota::METADATA].contains(&key) {
            return Err(ClusterError::Config("that metadata key belongs to the cluster"));
        }
        self.0.put_metadata(key, value).await
    }

    pub fn view(&self) -> Arc<View> {
        self.0.engine.membership.view()
    }

    pub fn subscribe(&self) -> MembershipSubscription {
        self.0.engine.membership.subscribe()
    }

    /// The first view, current or later, that satisfies `ready`, or `Closed` if the cluster shuts down before one
    /// does. It has no deadline: wrap it in `tokio::time::timeout`.
    pub async fn wait_view(&self, mut ready: impl FnMut(&View) -> bool) -> Result<Arc<View>, ClusterError> {
        let mut events = self.subscribe();
        while let Some(event) = events.next().await {
            let view = match event {
                MembershipEvent::Snapshot(view)
                | MembershipEvent::Changes { view, .. }
                | MembershipEvent::Resync(view) => view,
                // It marks the view stale without carrying it.
                MembershipEvent::Unavailable(_) => self.view(),
            };
            if ready(&view) {
                return Ok(view);
            }
        }
        Err(ClusterError::Closed)
    }

    /// Makes a running actor callable by name from any node. Withdrawn when the actor exits. A name takes one
    /// protocol `ID` and each of its `SCHEMA`s once: publish it again at another schema to serve both side by side.
    pub fn publish<P: ActorProtocol>(
        &self,
        actor: &ActorHandle<P::Mailbox>,
        name: &str,
    ) -> Result<RemoteRef<P>, ClusterError> {
        if name.is_empty() || name.len() > 256 {
            return Err(ClusterError::Config("name must be 1..=256 bytes"));
        }
        let endpoint = self.0.expose(actor, Schema::of::<P>(), name.into(), true)?;
        let (node, incarnation) = (self.0.local_node().into(), self.0.engine.local.identity.incarnation);
        let target = Target { node, incarnation, endpoint, schema: P::SCHEMA, lane: P::LANE };
        Ok(RemoteRef::new(self.0.engine.clone(), target, None))
    }

    /// Fenced: once this returns, nothing more is admitted through the reference.
    pub fn unpublish<P: WireProtocol>(&self, reference: &RemoteRef<P>) {
        let (target, local) = (reference.target(), &self.0.engine.local.identity);
        if *target.node == *local.node_id && target.incarnation == local.incarnation {
            self.0.engine.unregister(target.endpoint);
        }
    }

    pub async fn resolve_on<P: WireProtocol>(&self, node: &str, name: &str) -> Result<RemoteRef<P>, ClusterError> {
        let target = self.0.route_on(&node.into(), name, P::ID, P::SCHEMA).await?;
        Ok(RemoteRef::new(self.0.engine.clone(), target, None))
    }

    /// Every instance published under `name` on an available node, and each node that could not be asked with why:
    /// absent answers are never guessed. The nodes are asked side by side, so a silent one costs a request timeout, and
    /// fails `NotSent`, `Retry::Later`: a lookup sends nothing to the actor.
    pub async fn resolve_all<P: WireProtocol>(&self, name: &str) -> (Vec<RemoteRef<P>>, Vec<(String, ClusterError)>) {
        self.resolve_all_among(name, &Placement::default()).await
    }

    /// [`resolve_all`](Self::resolve_all) of the available nodes that carry every `required` label of `placement`; its
    /// `preferred` labels and `strategy` play no part. A silent node it leaves out costs nothing.
    pub async fn resolve_all_among<P: WireProtocol>(
        &self,
        name: &str,
        placement: &Placement,
    ) -> (Vec<RemoteRef<P>>, Vec<(String, ClusterError)>) {
        let nodes: Vec<Arc<str>> = self.view().admitted(placement).map(|m| m.record.id.as_str().into()).collect();
        let (mut found, mut failed) = (Vec::new(), Vec::new());
        for chunk in nodes.chunks(self.0.lookup_parallelism) {
            let (mut lookups, mut asked) = (tokio::task::JoinSet::new(), Vec::new());
            for node in chunk {
                let (inner, at, name) = (self.0.clone(), node.clone(), name.to_owned());
                let lookup = async move { inner.route_on(&at, &name, P::ID, P::SCHEMA).await };
                asked.push((lookups.spawn(lookup).id(), node));
            }
            while let Some(done) = lookups.join_next_with_id().await {
                // A lookup that panicked, or was aborted with its runtime, did not ask its node.
                let (id, target) = done.unwrap_or_else(|e| (e.id(), Err(ClusterError::Closed)));
                let node = asked.iter().find(|&&(i, _)| i == id).map(|&(_, n)| n.clone()).expect("spawned above");
                match target {
                    Ok(target) => found.push(RemoteRef::new(self.0.engine.clone(), target, None)),
                    Err(ClusterError::NotFound) => {}
                    Err(error) => failed.push((node.to_string(), error)),
                }
            }
        }
        (found, failed)
    }

    /// The single instance published under `name` anywhere in the cluster. A node that cannot be asked fails it.
    pub async fn resolve<P: WireProtocol>(&self, name: &str) -> Result<RemoteRef<P>, ClusterError> {
        self.resolve_among(name, &Placement::default()).await
    }

    pub(crate) async fn resolve_among<P: WireProtocol>(
        &self,
        name: &str,
        placement: &Placement,
    ) -> Result<RemoteRef<P>, ClusterError> {
        let (mut found, failed) = self.resolve_all_among(name, placement).await;
        if let Some((_, error)) = failed.into_iter().next() {
            return Err(error);
        }
        match found.len() {
            0 => Err(ClusterError::NotFound),
            1 => Ok(found.remove(0)),
            n => Err(ClusterError::Ambiguous(n)),
        }
    }

    /// The live set of the available nodes, this one too, that carry every `required` label of `placement`; its
    /// `preferred` labels and `strategy` play no part. No I/O, no task, and the cluster held weakly.
    pub fn nodes(&self, placement: &Placement) -> Nodes {
        Nodes::new(self.downgrade(), placement)
    }

    /// A reference that resolves `name` on its first call, and again after a call fails with
    /// [`Retry::Resolve`](crate::Retry::Resolve). [`among`](NamedRef::among) narrows the nodes it asks. No I/O here.
    pub fn named<P: WireProtocol>(&self, name: &str) -> NamedRef<P> {
        NamedRef::new(self.downgrade(), name, &Placement::default())
    }

    /// Serves a bounded request/response endpoint for node-to-node management traffic.
    pub fn serve_control<F, Fut>(&self, service: &str, policy: AccessPolicy, handler: F) -> Result<(), ClusterError>
    where
        F: Fn(ControlRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Bytes, String>> + Send + 'static,
    {
        let (slots, tokio, handler) = (self.0.control_slots.clone(), self.0.engine.tokio.clone(), Arc::new(handler));
        let (queue_wait, max, compress) =
            (self.0.control_queue_wait, self.0.max_control_response, self.0.compress_control);
        let admit: Admit = Arc::new(move |peer, body, reply| {
            let allowed = match &policy {
                AccessPolicy::Deny => false,
                AccessPolicy::Nodes(nodes) => nodes.contains(&peer.identity.node_id),
                AccessPolicy::Cluster => true,
            };
            if !allowed {
                return Err(Rejection::Forbidden);
            }
            let reply = reply.filter(|_| body.len() <= MAX_CONTROL_REQUEST).ok_or(Rejection::InvalidMessage)?;
            if compress {
                reply.compress();
            }
            let max_response = reply.max_payload().min(max) - 1;
            let (slots, handler, peer) = (slots.clone(), handler.clone(), peer.identity.clone());
            tokio.spawn(async move {
                let queued = Instant::now();
                let result = match timeout(queue_wait, slots.acquire_owned()).await {
                    Ok(Ok(_slot)) => {
                        // Waiting and working share one budget, inside the default 5 s `request_timeout`.
                        let budget = CONTROL_TIMEOUT.saturating_sub(queued.elapsed());
                        let work = handler(ControlRequest { peer, body, max_response });
                        timeout(budget, work).await.unwrap_or_else(|_| Err("handler timed out".into()))
                    }
                    _ => Err("busy: too many control requests in flight".into()),
                };
                let result =
                    result.and_then(|b| if b.len() > max_response { Err("response too large".into()) } else { Ok(b) });
                let _ = reply.send(encode_result(result.map_err(|e| (1, e))));
            });
            Ok(None)
        });
        let (name, protocol, schema) = (service.into(), CONTROL_PROTOCOL, SYSTEM_SCHEMA);
        let entry = Entry { name, protocol, schema, lane: Lane::Live, admit };
        self.0.engine.register(None, true, entry).map(drop)
    }

    pub async fn control(&self, node: &str, service: &str, body: Bytes) -> Result<Bytes, ClusterError> {
        let target = self.0.route_on(&node.into(), service, CONTROL_PROTOCOL, SYSTEM_SCHEMA).await?;
        let reply = self.0.engine.call_with(&target, REQUEST, body, self.0.compress_control, None).await?;
        let reply = reply.ok_or(crate::CallError::NoReply)?;
        decode_result(reply).map_err(|(_, message)| ClusterError::Control(message))
    }

    /// Serves a raw endpoint on `Lane::Live`. The handler runs on the session's read path, in arrival order per
    /// peer and lane: it must not block. Dropping the reply answers the caller with `NoReply`.
    /// Returns the id [`unserve`](Self::unserve) takes.
    pub fn serve<F>(&self, name: &str, protocol: u64, schema: u16, handler: F) -> Result<u64, ClusterError>
    where
        F: Fn(&Identity, Bytes, Option<crate::Reply>) -> Result<(), Rejection> + Send + Sync + 'static,
    {
        self.endpoint(name, protocol, schema).serve(handler)
    }

    /// A raw endpoint to serve, on `Lane::Live` unless [`lane`](EndpointBuilder::lane) says otherwise.
    pub fn endpoint<'a>(&'a self, name: &'a str, protocol: u64, schema: u16) -> EndpointBuilder<'a> {
        EndpointBuilder { cluster: self, name, protocol, schema, lane: Lane::Live }
    }

    /// Once this returns the endpoint's handler is not running and its name is free.
    pub fn unserve(&self, endpoint: u64) {
        self.0.engine.unregister(endpoint);
    }

    /// This node's timeline: keyed actors acquired, lost, handed off and restarted eagerly; streams losing their parent;
    /// a `Live` or `Bulk` session lost and back while `Control` stays up (`lane_down`, `lane_up`).
    pub fn events(&self) -> &Arc<axon::EventLog> {
        &self.0.events
    }

    /// The system's clock, which what this node serves reads its time from.
    pub fn clock(&self) -> &axon::Clock {
        &self.0.clock
    }

    pub fn tracer(&self) -> &Arc<axon::Tracer> {
        &self.0.engine.cfg.tracer
    }

    /// The largest request or reply body this node's sessions on `lane` carry.
    pub fn max_frame(&self, lane: Lane) -> usize {
        self.0.engine.cfg.transport.lanes.get(lane).limits.max_frame
    }

    /// How long a [`request`](Self::request) waits for its reply.
    pub fn request_timeout(&self) -> Duration {
        self.0.engine.cfg.request_timeout
    }

    /// One request to a [`serve`](Self::serve) endpoint on `node`, bounded by `request_timeout`.
    pub async fn request(
        &self,
        node: &str,
        name: &str,
        protocol: u64,
        schema: u16,
        body: Bytes,
    ) -> Result<Bytes, ClusterError> {
        let target = self.0.route_on(&node.into(), name, protocol, schema).await?;
        Ok(self.0.engine.call(&target, REQUEST, body).await?.ok_or(crate::CallError::NoReply)?)
    }

    /// A [`request`](Self::request) to one boot of `node`, `incarnation`: once another boot replaced it, the request is
    /// refused unsent with `StaleNode`, so one sent again after a failure never reaches a boot it was not meant for.
    /// Its answer is due `request_timeout` after the session admits it, as [`start_request_to`](Self::start_request_to)'s.
    pub async fn request_to(
        &self,
        node: &str,
        incarnation: u128,
        name: &str,
        protocol: u64,
        schema: u16,
        body: Bytes,
    ) -> Result<Bytes, ClusterError> {
        self.start_request_to(node, incarnation, name, protocol, schema, body).await?.await
    }

    /// A [`request_to`](Self::request_to) that returns once the session admits it, within `request_timeout`, so that
    /// the next one can follow without waiting for this one's answer. A session sends its frames in the order it
    /// admits them.
    pub async fn start_request_to(
        &self,
        node: &str,
        incarnation: u128,
        name: &str,
        protocol: u64,
        schema: u16,
        body: Bytes,
    ) -> Result<PendingReply, ClusterError> {
        let target = self.0.route_on(&node.into(), name, protocol, schema).await?;
        if target.incarnation != incarnation {
            return Err(crate::CallError::StaleNode.into());
        }
        let answer = self.0.engine.start_request(&target, body).await?;
        Ok(PendingReply(Box::pin(async move { Ok(answer.await?.ok_or(crate::CallError::NoReply)?) })))
    }

    /// The shortest of the last four keepalive round trips on the session this node calls `node` on; `None` before the
    /// first answer to one, or without a session.
    pub fn round_trip(&self, node: &str) -> Option<Duration> {
        self.0.engine.round_trip(node)
    }

    /// Whether discovery cannot hear this node and no peer vouches for it, so its keyed actors are down.
    pub fn isolated(&self) -> bool {
        self.0.isolated.load(Relaxed)
    }

    /// Whether the ownership authority still refuses grants after its start ([`OwnershipAuthority::warming`]).
    pub fn warming(&self) -> bool {
        self.0.ownership.as_ref().is_some_and(|o| o.warming())
    }

    /// Whether this node's [`Quotas`](crate::Quotas) replica is still pulling the other replicas' rows after its start,
    /// so that its grants do not count yet. `/readyz` waits for it; a pull after a handover does not hold it.
    pub fn quotas_starting(&self) -> bool {
        self.0.quotas.get().is_some_and(|q| q.starting())
    }

    pub fn stats(&self) -> ClusterStats {
        let (inner, view) = (&self.0, self.view());
        let (churn, c) = (&inner.engine.membership.churn, &inner.counters);
        ClusterStats {
            engine: inner.engine.stats(),
            members: view.members.len(),
            unreachable: view.members.iter().filter(|m| !m.reachable).count(),
            joined: churn.joined.load(Relaxed),
            left: churn.left.load(Relaxed),
            replaced: churn.replaced.load(Relaxed),
            became_unreachable: churn.unreachable.load(Relaxed),
            discovery_errors: churn.discovery_errors.load(Relaxed),
            heartbeat_failures: c.heartbeat_failures.load(Relaxed),
            last_heartbeat_error: inner.heartbeat_error.lock().clone(),
            route_hits: c.route_hits.load(Relaxed),
            route_misses: c.route_misses.load(Relaxed),
            spawned: inner.spawned.load(Relaxed),
            spawn_requests: c.spawn_requests.load(Relaxed),
            spawn_refused: c.spawn_refused.load(Relaxed),
            handoffs: c.handoffs.load(Relaxed),
            handoffs_failed: c.handoffs_failed.load(Relaxed),
            keyed_failed: c.keyed_failed.load(Relaxed),
            leases_lost: c.leases_lost.load(Relaxed),
            isolations: c.isolations.load(Relaxed),
            isolated: inner.isolated.load(Relaxed),
            warming: self.warming(),
            quotas_starting: self.quotas_starting(),
            eager_waiting: inner.eager.lock().keys.len(),
            eager_asking: inner.asking.lock().nodes.len(),
            discovery: inner.discovery.describe(),
            ownership: inner.ownership.as_ref().map(|o| o.describe()),
            quotas: inner.quotas.get().map(|q| q.stats()),
        }
    }
}

/// `0 | body` or `code | utf-8 message`.
pub(crate) fn encode_result(result: Result<Bytes, (u8, String)>) -> Bytes {
    let (code, body) = match result {
        Ok(body) => (0, body),
        Err((code, message)) => (code.max(1), Bytes::from(message)),
    };
    let mut out = BytesMut::with_capacity(1 + body.len());
    out.put_u8(code);
    out.put_slice(&body);
    out.freeze()
}

pub(crate) fn decode_result(mut body: Bytes) -> Result<Bytes, (u8, String)> {
    match body.try_get_u8() {
        Ok(0) => Ok(body),
        Ok(code) => Err((code, String::from_utf8_lossy(&body).into_owned())),
        Err(_) => Err((u8::MAX, "empty response".into())),
    }
}

impl Inner {
    pub fn local_node(&self) -> &str {
        &self.engine.local.identity.node_id
    }

    /// Registers an endpoint that decodes straight into the actor's mailbox.
    pub fn expose<M: Send + 'static>(
        &self,
        actor: &ActorHandle<M>,
        schema: Schema<M>,
        name: Arc<str>,
        published: bool,
    ) -> Result<u64, ClusterError> {
        let address = actor.address.clone();
        let admit = move |message| address.try_tell(message).map(|_| None).map_err(refused);
        self.expose_to(admit, schema, &actor.lifecycle, name, published)
    }

    /// An endpoint on whichever generation of a supervised actor is running. Between two, a send or a request waits
    /// for the next as a local `send` does.
    pub fn expose_supervised<M: Send + 'static>(
        &self,
        actor: &SupervisedHandle<M>,
        schema: Schema<M>,
        name: Arc<str>,
        published: bool,
    ) -> Result<u64, ClusterError> {
        let supervised = actor.clone();
        let admit = move |message| match supervised.tell(message) {
            // Between generations, or one ended as it was reached.
            Err(Reject { reason: RejectReason::NotReady | RejectReason::Closed, message }) => {
                let supervised = supervised.clone();
                let wait: Waiting = Box::pin(async move { supervised.send(message).await.map(drop).map_err(refused) });
                Ok(Some(wait))
            }
            told => told.map(|_| None).map_err(refused),
        };
        self.expose_to(admit, schema, &actor.lifecycle, name, published)
    }

    fn expose_to<M: Send + 'static>(
        &self,
        admit: impl Fn(M) -> Result<Option<Waiting>, Rejection> + Send + Sync + 'static,
        Schema { protocol, schema, lane, decode }: Schema<M>,
        lifecycle: &LifecycleHandle,
        name: Arc<str>,
        published: bool,
    ) -> Result<u64, ClusterError> {
        if protocol == 0 {
            return Err(ClusterError::Config("protocol ID 0 is reserved for Unaddressed"));
        }
        refuse_control(lane)?;
        let admit: Admit = Arc::new(move |_, body, reply| admit(decode(body, reply)?));
        let endpoint = self.engine.register(None, published, Entry { name, protocol, schema, lane, admit })?;
        let (engine, exited) = (self.engine.clone(), lifecycle.join());
        self.engine.tokio.spawn(async move {
            exited.await;
            engine.unregister(endpoint);
        });
        Ok(endpoint)
    }

    /// Where `name` on `node` takes `schema` of `protocol`, from the route cache or the node's directory.
    pub(crate) async fn route_on(
        &self,
        node: &Arc<str>,
        name: &str,
        protocol: u64,
        schema: u16,
    ) -> Result<Target, ClusterError> {
        let key = (node.clone(), name.to_owned());
        {
            let mut routes = self.routes.lock();
            let mark = self.engine.stale_routes.load(Relaxed);
            if std::mem::replace(&mut routes.stale_mark, mark) != mark {
                routes.entries.clear();
            }
            // A route that lacks the schema is asked again: it may have been cached before the node published it.
            let cached = routes.entries.get(&key).filter(|(expires, _)| *expires > Instant::now());
            match cached.map(|(_, route)| route.target(node.clone(), protocol, schema)) {
                None | Some(Err(ClusterError::SchemaMismatch { .. })) => {}
                Some(target) => {
                    self.counters.route_hits.fetch_add(1, Relaxed);
                    return target;
                }
            }
        }
        self.counters.route_misses.fetch_add(1, Relaxed);
        let asked = self.engine.ask(node.clone(), DIRECTORY, Bytes::from(key.1.clone())).await;
        let (incarnation, answer) = asked.map_err(crate::CallError::of_lookup)?;
        let answer = answer.unwrap_or_default();
        let found = (answer.first() == Some(&1)).then(|| answer.slice(1..));
        let route = found.and_then(|body| Route::decode(incarnation, body)).ok_or(ClusterError::NotFound)?;
        let target = route.target(node.clone(), protocol, schema);
        let mut routes = self.routes.lock();
        // Read under the lock `apply` clears the routes under, after it has taken the new view: an answer from a boot
        // the view has replaced meanwhile is not kept past that clear.
        let current = self.engine.membership.view().get(node).is_some_and(|m| m.record.incarnation == incarnation);
        if !current {
            return target;
        }
        if routes.entries.len() >= 4096 {
            routes.entries.clear();
        }
        routes.entries.insert(key, (Instant::now() + self.route_ttl, route));
        target
    }

    pub async fn put_metadata(&self, key: &str, value: Option<&str>) -> Result<(), ClusterError> {
        {
            let mut record = self.record.lock();
            let mut next = record.clone();
            match value {
                Some(value) => drop(next.metadata.insert(key.to_owned(), value.to_owned())),
                None => drop(next.metadata.remove(key)),
            }
            next.validate()?;
            *record = next;
        }
        self.announce().await?;
        self.refresh().await;
        Ok(())
    }

    /// Writes this node's record to discovery. A read-only provider is not an error.
    pub async fn announce(&self) -> Result<(), ClusterError> {
        let record = self.record.lock().clone();
        record.validate()?;
        match timeout(self.operation_timeout, self.discovery.register(&record)).await {
            Ok(Ok(())) | Ok(Err(DiscoveryError::ReadOnly)) => Ok(()),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(DiscoveryError::Unavailable("register timed out".into()).into()),
        }
    }

    /// Discovery restarting while a node starts is common on the same host; keep trying until `deadline`.
    /// Only an unavailable provider is worth waiting for; a stale or invalid record stays so.
    async fn announce_until(&self, deadline: Instant) -> Result<(), ClusterError> {
        loop {
            match self.announce().await {
                Err(ClusterError::Discovery(DiscoveryError::Unavailable(_)))
                    if Instant::now() + self.engine.cfg.poll_interval < deadline =>
                {
                    self.engine.membership.churn.discovery_errors.fetch_add(1, Relaxed);
                    tokio::time::sleep(self.engine.cfg.poll_interval).await;
                }
                result => return result,
            }
        }
    }

    async fn withdraw(&self) -> Result<(), String> {
        let (id, incarnation) = (self.local_node(), self.engine.local.identity.incarnation);
        match timeout(self.operation_timeout, self.discovery.unregister(id, incarnation)).await {
            Ok(Ok(())) | Ok(Err(DiscoveryError::ReadOnly)) => Ok(()),
            Ok(Err(error)) => Err(format!("unregister: {error}")),
            Err(_) => Err("unregister timed out".into()),
        }
    }

    /// Returns once a snapshot asked for after the call has been applied, or has failed.
    pub async fn refresh(&self) {
        let done = self.refreshes.load(Relaxed);
        let turn = self.refreshing.lock().await;
        // The second refresh done since this call came was asked for after it: callers that queued behind one
        // snapshot share the next, so none waits more than two. Waiting for one alone would mean a snapshot of its
        // own per caller, asked beside the one out: a burst of spawns sent to a node that no longer owns their keys
        // would ask discovery once each.
        if self.refreshes.load(Relaxed) < done + 2 {
            self.snapshot(turn).await;
        }
    }

    async fn snapshot(&self, _turn: tokio::sync::MutexGuard<'_, ()>) {
        match timeout(self.operation_timeout, self.discovery.snapshot()).await {
            Ok(Ok(snapshot)) => _ = self.apply(snapshot),
            Ok(Err(error)) => self.unavailable(error.to_string()),
            Err(_) => self.unavailable("snapshot timed out".into()),
        }
        // ponytail: counted after the apply and inside the lock, or a watched answer that passed the check could
        // land over this snapshot; the tests cannot hit windows that narrow.
        self.refreshes.fetch_add(1, Relaxed);
    }

    /// A watch, with the refreshes done before it was asked for.
    fn watch(&self) -> Option<BoxFuture<'_, (u64, Result<Snapshot, DiscoveryError>)>> {
        let refreshes = self.refreshes.load(Relaxed);
        let next = self.discovery.watch(self.engine.membership.view().revision)?;
        Some(Box::pin(async move { (refreshes, next.await) }))
    }

    /// `snapshot_ok` tells how long discovery has been failing, and a provider that is watched may not have
    /// been asked for a while: the first failure counts from now, not from the last answer.
    fn unavailable(&self, error: String) {
        if self.engine.membership.view().stale.is_none() {
            *self.snapshot_ok.lock() = Instant::now();
        }
        self.engine.membership.unavailable(error);
    }

    /// Whether membership changed.
    fn apply(&self, mut snapshot: Snapshot) -> bool {
        let membership = &self.engine.membership;
        *self.snapshot_ok.lock() = Instant::now();
        if snapshot.nodes.len() > self.max_nodes || check_nodes(&mut snapshot.nodes).is_err() {
            membership.unavailable("invalid snapshot".into());
            return false;
        }
        let changes = membership.apply(snapshot);
        if changes.is_empty() {
            return false;
        }
        for change in &changes {
            match change {
                MemberChange::Left(node) => self.engine.reset_peer(&node.id),
                MemberChange::Replaced { previous, .. } => self.engine.reset_peer(&previous.id),
                MemberChange::Updated { previous, current } if previous.address != current.address => {
                    self.engine.reset_peer(&previous.id)
                }
                _ => {}
            }
        }
        self.routes.lock().entries.clear();
        self.rebalance.notify_one();
        true
    }

    async fn poll_loop(self: Arc<Self>, poll: Duration, heartbeat: Duration) {
        let mut watch = self.watch();
        let pause = poll;
        let mut poll = tokio::time::interval(if watch.is_some() { self.resync_interval } else { poll });
        let mut heartbeat = tokio::time::interval(heartbeat);
        let mut changes = self.engine.membership.subscribe();
        let mut resync: Option<BoxFuture<'_, ()>> = None;
        loop {
            tokio::select! {
                // This loop also beats: it never waits behind another caller's snapshot, which does a tick's job.
                _ = poll.tick() => if let Ok(turn) = self.refreshing.try_lock() {
                    self.snapshot(turn).await
                },
                watched = async { watch.as_mut().expect("guarded").await }, if watch.is_some() => {
                    let (refreshes, watched) = watched;
                    let news = match (self.refreshing.try_lock(), watched) {
                        // A snapshot is out: drop the answer, and pause, so a watch that answers at once does not
                        // spin until the snapshot lands.
                        (Err(_), _) => false,
                        // One landed while this answer was on its way: drop the answer and watch again from it.
                        (Ok(_turn), _) if self.refreshes.load(Relaxed) != refreshes => true,
                        (Ok(_turn), Ok(snapshot)) => {
                            // An answer at the revision asked after (Consul's wait expiring) is no news, but needs
                            // no pause either.
                            let seen = snapshot.revision == self.engine.membership.view().revision;
                            self.apply(snapshot) || seen
                        }
                        (Ok(_turn), Err(error)) => {
                            self.unavailable(error.to_string());
                            false
                        }
                    };
                    // Without news, never faster than polling would be: a provider's revision can move for
                    // reasons that have nothing to do with membership, and a failing one answers at once.
                    let this = &self;
                    watch = Some(Box::pin(async move {
                        if !news {
                            tokio::time::sleep(pause).await;
                        }
                        match this.watch() {
                            Some(next) => next.await,
                            None => std::future::pending().await,
                        }
                    }));
                }
                _ = async { resync.as_mut().expect("guarded").await }, if resync.is_some() => resync = None,
                _ = heartbeat.tick() => {
                    // A watch answers once the revision moves, and discovery may come back at the one a stale view
                    // has: the rebalance pass, which skips a stale view, would wait for the resync. Beside the
                    // heartbeat, not before it, so a snapshot that hangs does not hold the heartbeat back.
                    if watch.is_some() && resync.is_none() && self.engine.membership.view().stale.is_some() {
                        let this = &self;
                        resync = Some(Box::pin(async move {
                            if let Ok(turn) = this.refreshing.try_lock() {
                                this.snapshot(turn).await
                            }
                        }));
                    }
                    self.heartbeat().await
                }
                // Reachability changes come from peer tasks, not from refresh.
                _ = changes.next() => self.rebalance.notify_one(),
            }
            if self.engine.is_closed() {
                return;
            }
        }
    }

    async fn heartbeat(self: &Arc<Self>) {
        let (id, incarnation) = (self.local_node(), self.engine.local.identity.incarnation);
        // A hung provider must not delay the isolation check past the provider's TTL.
        let limit = self.operation_timeout.min(self.heartbeat_grace / 2);
        let deadline = Instant::now() + limit;
        let error = match timeout_at(deadline, self.discovery.heartbeat(id, incarnation)).await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.to_string()),
            Err(_) => Some("heartbeat timed out".into()),
        };
        let mut heard = error.is_none();
        if !heard {
            self.counters.heartbeat_failures.fetch_add(1, Relaxed);
            // The provider may have forgotten us (an agent restart); put the record back. A register can be taken
            // without being heard (seeds take one while frozen), so only the heartbeat after it says so.
            let record = self.record.lock().clone();
            heard = matches!(timeout_at(deadline, self.discovery.register(&record)).await, Ok(Ok(())))
                && matches!(timeout_at(deadline, self.discovery.heartbeat(id, incarnation)).await, Ok(Ok(())));
        }
        *self.heartbeat_error.lock() = error;
        if heard {
            *self.heartbeat_ok.lock() = Instant::now();
        }
        // An agent cut off from its servers still takes heartbeats, but it cannot answer a snapshot.
        let blind =
            self.engine.membership.view().stale.is_some() && self.snapshot_ok.lock().elapsed() >= self.heartbeat_grace;
        if blind || self.heartbeat_ok.lock().elapsed() >= self.heartbeat_grace {
            self.check_isolation(limit).await;
        } else {
            self.rejoined(heard);
        }
        let warming = self.ownership.as_ref().is_some_and(|o| o.warming());
        if self.warming.swap(warming, Relaxed) != warming {
            let (kind, detail) = match warming {
                true => ("acceptor_warming", "the ownership authority refuses grants until it is warm"),
                false => ("acceptor_warm", "the ownership authority grants again"),
            };
            self.events.record(kind, "", detail);
        }
    }

    async fn begin_leaving(self: &Arc<Self>, mut reason: String) -> Result<(), String> {
        self.engine.open.store(false, Relaxed);
        {
            let mut record = self.record.lock();
            record.metadata.insert(LIFECYCLE.into(), DRAINING.into());
            reason.truncate(reason.floor_char_boundary(crate::discovery::MAX_VALUE));
            record.metadata.insert(LEAVE_REASON.into(), reason);
        }
        self.hand_off();
        // Before the register, which the leaving phase may cut off once it landed.
        *self.withdraw_after.lock() = Instant::now() + self.engine.cfg.poll_interval;
        self.announce().await.map_err(|e| e.to_string())?;
        self.refresh().await;
        Ok(())
    }

    async fn finish_leaving(self: &Arc<Self>, budget: Duration) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        // Until none is left: a hand-off no owner took, or an actor whose lease lapsed meanwhile, adds a restart.
        let handed = async {
            loop {
                let handing = std::mem::take(&mut *self.handoff.lock());
                if handing.is_empty() {
                    break;
                }
                handing.join_all().await;
            }
        };
        let _ = tokio::time::timeout_at(deadline - budget * 5 / 8, handed).await;
        // Peers that poll discovery see the drain, and its reason, only if the record outlasts a poll: a leave
        // withdrawn sooner reads as a crash. Before the authority closes, so leases still held keep being renewed,
        // and within the hand-off's share of the budget.
        let shown = *self.withdraw_after.lock();
        tokio::time::sleep_until(shown.min(deadline - budget * 5 / 8)).await;
        if let Some(authority) = &self.ownership {
            // Actors the hand-off did not stop must not run on leases given up here or by the authority.
            self.leases.lock().values().for_each(|lease| lease.fence.hold_for(Duration::ZERO));
            // A release under way can take an operation_timeout: waiting it out must not cost the close its turn, which
            // gives up the rest at once where the authority can (a Consul session). One still under way goes on alone
            // and may land after the close, which gave that lease up already.
            let _ = tokio::time::timeout_at(deadline - budget * 9 / 16, self.release_all()).await;
            let _ = tokio::time::timeout_at(deadline - budget / 2, authority.close()).await;
        }
        self.stop_tasks();
        let withdrawn = tokio::time::timeout_at(deadline - budget / 4, self.withdraw()).await;
        let withdrawn = withdrawn.unwrap_or_else(|_| Err("unregister timed out".into()));
        self.engine.shutdown(deadline).await;
        withdrawn
    }

    fn stop_tasks(&self) {
        self.tasks.lock().drain(..).for_each(|task| task.abort());
        self.handoff.lock().abort_all();
    }

    fn close(&self) {
        self.stop_tasks();
        self.engine.close();
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.close();
    }
}

struct Extension(Arc<Inner>);

impl SystemExtension for Extension {
    fn name(&self) -> &str {
        "cluster"
    }

    fn leaving(&self, reason: &LeaveReason, budget: Duration) -> ExtensionFuture {
        let (inner, reason) = (self.0.clone(), format!("{reason:?}"));
        Box::pin(async move {
            timeout(budget, inner.begin_leaving(reason)).await.unwrap_or_else(|_| Err("announce timed out".into()))
        })
    }

    fn shutdown(&self, budget: Duration) -> ExtensionFuture {
        let inner = self.0.clone();
        Box::pin(async move { inner.finish_leaving(budget).await })
    }

    fn close(&self) {
        self.0.close();
    }
}

fn refused<M>(rejected: Reject<M>) -> Rejection {
    match rejected.reason {
        RejectReason::Full | RejectReason::WaiterLimit => Rejection::Full,
        RejectReason::TooLarge => Rejection::InvalidMessage,
        _ => Rejection::Stopped,
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::Poll;

    use super::*;
    use crate::MemoryDiscovery;
    use crate::engine::tests::node;

    /// `protocol u64 | (endpoint u64 | schema u16 | lane u8)+`: the serving node's lane for each endpoint, and a lane
    /// byte this build does not know refuses the whole route.
    #[test]
    fn a_route_with_an_unknown_lane_byte_is_refused() {
        let route = |lanes: &[u8]| {
            let mut out = BytesMut::new();
            out.put_u64(3);
            for (i, lane) in lanes.iter().enumerate() {
                out.put_u64(17 + i as u64);
                out.put_u16(2 + i as u16);
                out.put_u8(*lane);
            }
            Route::decode(9, out.freeze())
        };
        let known = route(&[2, 1]).expect("lanes Bulk and Live");
        let target = |schema| known.target("b".into(), 3, schema).unwrap();
        assert_eq!((target(2).endpoint, target(2).lane), (17, Lane::Bulk));
        assert_eq!((target(3).endpoint, target(3).lane), (18, Lane::Live));
        assert!(route(&[1, 3]).is_none(), "lane 3");
        assert!(route(&[]).is_none(), "no endpoint");
        // A route from a build before the lane byte, 10 bytes an endpoint, of up to 10 schemas.
        for n in 1..=10u64 {
            let mut out = BytesMut::new();
            out.put_u64(3);
            for i in 0..n {
                out.put_u64(17 + i);
                out.put_u16(2);
            }
            assert!(Route::decode(9, out.freeze()).is_none(), "{n} endpoints without lanes");
        }
    }

    /// A lookup the old boot answered, back after the view took b's new boot and cleared the routes, is not cached:
    /// the next one asks the new boot rather than send its callers to the old one for `route_ttl`.
    #[tokio::test]
    async fn a_lookup_the_replaced_boot_answered_is_not_cached() {
        let discovery = MemoryDiscovery::new();
        let (old_system, old) = node("b", discovery.clone()).await;
        // A later millisecond: an incarnation is `unix millis << 64 | random`.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let (new_system, new) = node("b", MemoryDiscovery::new()).await;
        for b in [&old, &new] {
            b.serve("svc", 7, 1, |_, _, _| Ok(())).unwrap();
        }
        let (a_system, a) = node("a", discovery.clone()).await;
        let b: Arc<str> = "b".into();
        // The link up, so that the lookup's first poll sends it.
        assert_eq!(a.0.engine.incarnation_of(&b).await.unwrap(), old.record().incarnation);

        let mut lookup = pin!(a.0.route_on(&b, "svc", 7, 1));
        // Polled until the old boot's answer is in, and not after.
        loop {
            std::future::poll_fn(|cx| {
                assert!(lookup.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if a.0.engine.stats().peers.iter().all(|peer| peer.pending == 0) {
                break;
            }
        }
        discovery.register(&new.record()).await.unwrap();
        a.0.refresh().await;
        assert_eq!(a.0.engine.membership.view().get("b").unwrap().record.incarnation, new.record().incarnation);
        assert_eq!(lookup.await.unwrap().incarnation, old.record().incarnation);
        // The link to the old boot closes, and the new boot's is dialled.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let again = a.0.route_on(&b, "svc", 7, 1).await.unwrap();
        assert_eq!(again.incarnation, new.record().incarnation, "a route to the old boot was cached");

        for system in [a_system, old_system, new_system] {
            system.shutdown().await.unwrap();
        }
    }
}
