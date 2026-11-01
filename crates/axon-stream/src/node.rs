//! Runs a [`RelayCore`] on a cluster node: serves the stream endpoint, picks parents from
//! membership labels, and moves interests and pushes between nodes.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use axon::{IdHash, Lane};
use axon::{Span, SpanKind, Stream, TraceContext};
use axon_cluster::{
    CallError, Cluster, ClusterError, Member, MemberChange, MembershipEvent, PendingReply, Permit, Permits, Quotas,
    Rate, Rejection, View, rendezvous,
};
use axon_net::Identity;
use bytes::{Buf, Bytes};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::core::{
    CoreConfig, Item, Parents, Push, PushAnswer, PushId, RelayCore, RelayEvent, RelayStats, RelayTimings, Reply,
    StreamInfo, Wake,
};
use crate::disk::{DiskConfig, DiskStore, RETAIN_EVERY, RETRY_WRITE};
use crate::hub::{
    Authorizer, DeliveryPolicy, NamespaceLimit, NamespaceLimits, PeerId, PublishError, StreamError, StreamEvent,
    SubscribeOptions,
};
use crate::recovery::{Cursor, Payload, StreamKey};
use crate::wire;

/// Interests, from a child to its parent.
const ENDPOINT: &str = "axon.stream";
/// Pushes, from a parent to its child, on the lane of their namespace.
const LIVE: &str = "axon.stream.live";
const BULK: &str = "axon.stream.bulk";
const PROTOCOL: u64 = 0x4158_4f4e_5354_524d;
pub(crate) const SCHEMA: u16 = 7;

const RECORDS_NAME: &str = "axon.stream.records";
const RECORDS: Rate = Rate::new(RECORDS_NAME, 0, Duration::from_secs(1));
const BYTES: Rate = Rate::new("axon.stream.bytes", 0, Duration::from_secs(1));
const OPEN: Permits = Permits::new("axon.stream.open", 0);

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct StreamDetail {
    pub streams: Vec<StreamInfo>,
    /// How many streams the node has; `streams` may be a prefix.
    pub total: usize,
    pub events: Vec<RelayEvent>,
}

pub type SharedAuthorizer = Arc<dyn Authorizer + Send + Sync>;

#[derive(Clone)]
pub struct StreamNodeConfig {
    pub core: CoreConfig,
    /// Where this node pulls streams it does not originate, most preferred first. Each tier is
    /// a set of labels a parent must advertise; within a tier the order is a rendezvous hash
    /// of this node's id, so children spread over the tier and each keeps a stable choice.
    /// Empty for an origin. A relay's tiers must not match its own children.
    pub upstream: Vec<BTreeMap<String, String>>,
    /// Tiers for the streams of one namespace, when their origins are not where `upstream` looks.
    pub upstream_by_namespace: BTreeMap<String, Vec<BTreeMap<String, String>>>,
    pub authorizer: SharedAuthorizer,
    /// How often timers are checked when nothing else wakes the node.
    pub tick: Duration,
    /// How long pushes to a downstream node may fail, or no interest of its arrive, before it is dropped. It bounds how
    /// long a child the cluster's authorizer no longer lets call `axon.stream` is still fed. One lost interest leaves a
    /// healthy child silent for `core.refresh`, the cluster's `request_timeout`, `core.retry_failed` and two ticks,
    /// so `start` refuses an expiry that does not outlast that.
    pub downstream_expiry: Duration,
    /// Where the streams this node originates in some namespaces keep their history, so they survive a restart.
    pub disk: Option<DiskConfig>,
    /// Holds the namespaces' rates and open streams across the cluster; `start` refuses such a limit without it.
    pub quotas: Option<Quotas>,
}

impl StreamNodeConfig {
    pub fn new(upstream: Vec<BTreeMap<String, String>>) -> Self {
        Self {
            core: CoreConfig::default(),
            upstream,
            upstream_by_namespace: BTreeMap::new(),
            authorizer: Arc::new(crate::hub::AllowAll),
            tick: Duration::from_millis(200),
            downstream_expiry: Duration::from_secs(10),
            disk: None,
            quotas: None,
        }
    }

    /// Holds the streams of `namespace` to `limits`.
    pub fn limit(mut self, namespace: impl Into<String>, limits: NamespaceLimits) -> Self {
        self.core.namespaces.insert(namespace.into(), limits);
        self
    }

    /// The keys of `namespace` wait on their parents by `timings` instead of `core.timings`.
    pub fn relay_timings(mut self, namespace: impl Into<String>, timings: RelayTimings) -> Self {
        self.core.timings_by_namespace.insert(namespace.into(), timings);
        self
    }

    /// The pushes of `namespace` go on `lane` instead of `Live`: `Bulk` for a namespace that moves much more than this
    /// node's calls, which would queue behind its pushes on `Live`. `start` refuses `Control`.
    pub fn lane(mut self, namespace: impl Into<String>, lane: Lane) -> Self {
        self.core.lanes.insert(namespace.into(), lane);
        self
    }

    /// Streams in `namespace` are pulled from these tiers instead of `upstream`.
    pub fn upstream_for(mut self, namespace: impl Into<String>, tiers: Vec<BTreeMap<String, String>>) -> Self {
        self.upstream_by_namespace.insert(namespace.into(), tiers);
        self
    }
}

type Ranked = (u64, u64, Vec<PeerId>, BTreeMap<String, Vec<PeerId>>);
/// The push driver of each child and lane.
type Pumps = HashMap<(Arc<str>, Lane), Arc<Notify>>;

struct Inner {
    cluster: Cluster,
    runtime: tokio::runtime::Handle,
    core: Mutex<RelayCore<SharedAuthorizer>>,
    upstream: Vec<BTreeMap<String, String>>,
    upstream_by_namespace: BTreeMap<String, Vec<BTreeMap<String, String>>>,
    tick: Duration,
    downstream_expiry: Duration,
    kick: Arc<Notify>,
    /// Told after each disk write and whenever an open ends, for the waits on a cursor.
    cursors: Arc<Notify>,
    /// Told whenever a call leaves something queued for the disk, for its writer.
    writes: Arc<Notify>,
    /// The view the parents were last ranked from, and the parents: the default list and one per namespace.
    ranked: Mutex<Ranked>,
    endpoints: Mutex<Vec<u64>>,
    locals: Mutex<HashMap<u64, Arc<Mutex<Inbox>>, IdHash>>,
    pumps: Mutex<Pumps>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Only with a namespace limited across the cluster.
    quotas: Option<Quotas>,
    /// Of the streams this node originates in a namespace with `open_streams`.
    permits: Mutex<BTreeMap<StreamKey, Permit>>,
}

/// A hub on a cluster node. Cloning shares it; the last drop, or its last subscription's after it, stops its tasks and
/// gives back its permits.
#[derive(Clone)]
pub struct StreamNode(Arc<Handle>);

/// What the handles and subscriptions share. A task holds `Inner` alone, for a turn or a request (a pump's while it
/// has pushes to send or answer), so the node goes with its last handle, not with whichever reference is last.
struct Handle(Arc<Inner>);

impl Deref for Handle {
    type Target = Arc<Inner>;

    fn deref(&self) -> &Arc<Inner> {
        &self.0
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// One span per traced batch of `push`, by item index; the batch goes on downstream as that span's child.
fn take_spans(push: &mut Push, from: &str) -> Vec<(u16, Span)> {
    let mut spans = Vec::new();
    for (index, (key, _, item)) in push.items.iter_mut().enumerate() {
        let Item::Batch(batch) = item else { continue };
        let Some(parent) = batch.trace_context() else { continue };
        let here = parent.child();
        let mut span = Span::begin(parent, here.span, SpanKind::Stream, key.to_string());
        span.peer = Some(from.into());
        *batch = batch.clone().traced(Some((here.trace.get(), here.span)));
        spans.push((index as u16, span));
    }
    spans
}

/// Parents for `me`, most preferred first.
fn candidates(view: &View, me: &str, tiers: &[BTreeMap<String, String>]) -> Vec<PeerId> {
    let mut ranked: Vec<PeerId> = Vec::new();
    for tier in tiers {
        let fits = |m: &&Member| m.available() && m.record.id != me && m.record.has_labels(tier);
        let mut members: Vec<&Member> = view.members.iter().filter(fits).collect();
        members.retain(|m| !ranked.iter().any(|p| *p.node == *m.record.id));
        while let Some(winner) = rendezvous("axon.stream", me, &members) {
            ranked.push(PeerId::new(&winner.record.id, winner.record.incarnation));
            members.retain(|m| m.record.id != winner.record.id);
        }
    }
    ranked
}

/// The nodes an event takes out of the view. A resync's changes are lost: any member it shows may have left.
fn departed(event: &MembershipEvent, left: &mut BTreeSet<String>) {
    match event {
        MembershipEvent::Changes { changes, .. } => left.extend(changes.iter().filter_map(|change| match change {
            MemberChange::Left(record) => Some(record.id.clone()),
            MemberChange::Updated { current, .. } if current.draining() => Some(current.id.clone()),
            MemberChange::Unreachable(id) => Some(id.clone()),
            _ => None,
        })),
        MembershipEvent::Resync(view) => left.extend(view.members.iter().map(|m| m.record.id.clone())),
        MembershipEvent::Snapshot(_) | MembershipEvent::Unavailable(_) => {}
    }
}

/// Takes a namespace's rates for a batch. One past a rate could never be taken: it is split instead.
fn take(quotas: &Quotas, key: &StreamKey, limits: NamespaceLimits, records: &[Payload]) -> Result<(), StreamError> {
    let (n, bytes) = (records.len() as u64, records.iter().map(Payload::len).sum::<usize>() as u64);
    let rates = [(RECORDS, limits.records_per_sec, n), (BYTES, limits.bytes_per_sec, bytes)];
    if rates.iter().any(|(_, max, cost)| max.is_some_and(|max| *cost > max)) {
        return Err(StreamError::LimitExceeded);
    }
    let costs: Vec<(Rate, u64)> =
        rates.into_iter().filter_map(|(rate, max, cost)| Some((rate.with_max(max?), cost))).collect();
    quotas.take(key.namespace(), &costs).map_err(|denied| {
        let limit = if denied.limit() == RECORDS_NAME { NamespaceLimit::Records } else { NamespaceLimit::Bytes };
        StreamError::QuotaExceeded { namespace: key.namespace().into(), limit, retry_after: Some(denied.retry_after()) }
    })
}

impl Inner {
    /// Static seeds carry no incarnation; then a parent is known by node id alone.
    fn peer(&self, identity: &Identity) -> PeerId {
        let known = self.cluster.view().get(&identity.node_id).is_some_and(|m| m.record.incarnation != 0);
        PeerId::new(&identity.node_id, if known { identity.incarnation } else { 0 })
    }

    /// The answer to a call on `lane`: interests on `Control`, pushes on `Live` and `Bulk`.
    fn answer(self: &Arc<Self>, lane: Lane, identity: &Identity, mut body: Bytes) -> Result<Bytes, Rejection> {
        if !body.has_remaining() {
            return Err(Rejection::InvalidMessage);
        }
        let (kind, now) = (body.get_u8(), Instant::now());
        match (kind, lane == Lane::Control) {
            (wire::INTEREST, true) => {
                let interest = wire::decode_interest(&mut body).ok_or(Rejection::InvalidMessage)?;
                let real = PeerId::new(&identity.node_id, identity.incarnation);
                Ok(match self.with(|core| core.handle_interest(&real, interest, now.into_std())) {
                    Ok(statuses) => [&[wire::ANSWERS][..], &wire::encode_answers(&statuses)].concat().into(),
                    Err(StreamError::OutOfSync) => Bytes::from_static(&[wire::OUT_OF_SYNC]),
                    Err(_) => Bytes::from_static(&[wire::REFUSED]),
                })
            }
            (wire::PUSH, false) => {
                let (channel, seq, mut push) = wire::decode_push(&mut body).ok_or(Rejection::InvalidMessage)?;
                let (spans, peer) = (take_spans(&mut push, &identity.node_id), self.peer(identity));
                let boot = identity.incarnation;
                let answer = self.with(|core| core.handle_push(&peer, lane, boot, channel, seq, push, now.into_std()));
                if let PushAnswer::Applied { unwanted, .. } = &answer {
                    for (_, mut span) in spans.into_iter().filter(|(index, _)| !unwanted.contains(index)) {
                        span.busy = now.elapsed();
                        self.cluster.tracer().record(span);
                    }
                }
                Ok(wire::encode_answer(&answer))
            }
            _ => Err(Rejection::InvalidMessage),
        }
    }

    fn with<T>(self: &Arc<Self>, act: impl FnOnce(&mut RelayCore<SharedAuthorizer>) -> T) -> T {
        let mut wakes = Vec::new();
        let (out, queued) = {
            let mut core = self.core.lock();
            let out = act(&mut core);
            // A woken subscription is filled here, under the lock this call already holds, so its poll takes
            // only its own inbox: the subscribers one publish wakes do not queue on the core.
            loop {
                let fresh = core.take_wakes();
                if fresh.is_empty() {
                    break;
                }
                let locals = self.locals.lock();
                for wake in fresh {
                    match wake {
                        Wake::Local(id) => {
                            // Woken at once, not after the loop: measured, a subscriber that runs while the
                            // rest are filled beats a burst of wakes once the lock is free (726 vs 982 ns/record).
                            if let Some(waker) = locals.get(&id).and_then(|i| i.lock().fill(&mut core, id)) {
                                waker.wake();
                            }
                        }
                        wake => wakes.push(wake),
                    }
                }
            }
            (out, core.disk_queued())
        };
        if queued {
            self.writes.notify_one();
        }
        for wake in wakes {
            match wake {
                Wake::Reconcile => self.kick.notify_one(),
                Wake::Local(_) => {}
                Wake::Peer(node, lane) => self.pump(node, lane),
            }
        }
        out
    }

    /// One task per child and lane, so a `Bulk` backlog never holds a `Live` push up.
    fn pump(self: &Arc<Self>, node: Arc<str>, lane: Lane) {
        let mut pumps = self.pumps.lock();
        let channel = (node, lane);
        if let Some(notify) = pumps.get(&channel) {
            return notify.notify_one();
        }
        let notify = Arc::new(Notify::new());
        notify.notify_one();
        pumps.insert(channel.clone(), notify.clone());
        let weak = Arc::downgrade(self);
        let task = self.runtime.spawn(async move {
            let (node, lane) = &channel;
            loop {
                notify.notified().await;
                let Some(inner) = weak.upgrade() else { return };
                inner.drain(node, *lane, &notify).await;
                // Checked under the lock `pump` takes, so a wake for a returning node is never
                // handed to a task that is about to exit.
                let mut pumps = inner.pumps.lock();
                if inner.core.lock().downstream_peer(node, *lane).is_none() {
                    pumps.remove(&channel);
                    return;
                }
            }
        });
        let mut tasks = self.tasks.lock();
        tasks.retain(|t| !t.is_finished());
        tasks.push(task);
    }

    /// Pushes to one node on one lane while its window and the node cap leave room, each to the boot that asked, and
    /// hands the answers to the core in seq order. After a failed call, or a push the child skipped for a gap, it sends
    /// again from the first unanswered push a tick later. Returns once nothing is left to send or answer, or the
    /// node's channel on the lane is gone.
    async fn drain(self: &Arc<Self>, node: &Arc<str>, lane: Lane, wake: &Notify) {
        let name = if lane == Lane::Bulk { BULK } else { LIVE };
        let (mut open, mut retry) = (VecDeque::<(PeerId, PushId, PendingReply)>::new(), None::<Instant>);
        // The go-back count of the last push sent: the core does not go back for a push of an older round, so neither
        // does a failure of one hold the resent pushes a tick.
        let mut round = 0;
        while self.core.lock().downstream_peer(node, lane).is_some() {
            if retry.is_none_or(|at| at <= Instant::now()) {
                retry = None;
                let rtt = self.cluster.round_trip(node);
                while let Some((peer, id, body)) = self.with(|core| {
                    let peer = core.downstream_peer(node, lane)?.clone();
                    let (id, body) = core.next_push(node, lane, Instant::now().into_std(), rtt)?;
                    Some((peer, id, body))
                }) {
                    // Sent to the boot that asked: a push built from its interests would skip records for the next one.
                    match self.cluster.start_request_to(node, peer.incarnation, name, PROTOCOL, SCHEMA, body).await {
                        Ok(reply) => {
                            // A push of an older round went again in this one, and one to another boot or channel is
                            // not wanted: their calls go, so they hold no call slot or answer of this round up.
                            while open
                                .front()
                                .is_some_and(|(p, o, _)| (p, o.channel, o.round) != (&peer, id.channel, id.round))
                            {
                                open.pop_front();
                            }
                            round = id.round;
                            open.push_back((peer, id, reply));
                        }
                        Err(ClusterError::Call(CallError::StaleNode)) => {
                            return self.with(|core| core.expire_downstream(&peer, Instant::now().into_std()));
                        }
                        Err(_) => {
                            self.with(|core| core.push_failed(&peer, lane, id, Instant::now().into_std()));
                            retry.get_or_insert_with(|| Instant::now() + self.tick);
                            break;
                        }
                    }
                }
            }
            if open.is_empty() && retry.is_none() {
                return;
            }
            let front = async {
                match open.front_mut() {
                    Some((_, _, reply)) => Some(reply.await),
                    None => std::future::pending().await,
                }
            };
            let answer = tokio::select! {
                answer = front => answer,
                () = wake.notified() => None,
                () = tokio::time::sleep_until(retry.unwrap_or_else(Instant::now)), if retry.is_some() => None,
            };
            let Some(answer) = answer else { continue };
            let (peer, id, _) = open.pop_front().expect("answered above");
            let now = Instant::now().into_std();
            // An admitted call ends in its answer, a timeout or a lost session, never `StaleNode`.
            let answer = answer.ok().as_deref().and_then(wire::decode_answer);
            let gap = |a: &PushAnswer| matches!(a, PushAnswer::Skipped { applied } if *applied < id.seq);
            if id.round == round && answer.as_ref().is_none_or(gap) {
                retry.get_or_insert_with(|| Instant::now() + self.tick);
            }
            match answer {
                Some(answer) => self.with(|core| core.push_answered(&peer, lane, id, answer, now)),
                None => self.with(|core| core.push_failed(&peer, lane, id, now)),
            }
        }
    }

    fn reconcile(self: &Arc<Self>, left: &BTreeSet<String>) {
        let view = self.cluster.view();
        let (ranked, changed) = {
            let mut ranked = self.ranked.lock();
            let changed = (ranked.0, ranked.1) != (view.sequence, view.revision);
            if changed || view.sequence == 0 {
                let me = &self.cluster.identity().node_id;
                let by_namespace = self.upstream_by_namespace.iter();
                let by_namespace = by_namespace.map(|(ns, tiers)| (ns.clone(), candidates(&view, me, tiers))).collect();
                *ranked = (view.sequence, view.revision, candidates(&view, me, &self.upstream), by_namespace);
            }
            ((ranked.2.clone(), ranked.3.clone()), changed)
        };
        let parents = Parents { default: &ranked.0, by_namespace: &ranked.1 };
        let now = Instant::now().into_std();
        let (interests, timeline) = self.with(|core| {
            if changed {
                core.membership_changed(now);
            }
            core.parents_left(left.iter().map(String::as_str), now);
            core.expire_unheard(now, self.downstream_expiry);
            (core.reconcile(parents, now), core.take_timeline())
        });
        for (kind, (stream, detail, streams)) in timeline {
            let subject = if streams == 1 { stream } else { format!("{stream} and {} more", streams - 1) };
            self.cluster.events().record(kind, subject, detail);
        }
        for (parent, interest) in interests {
            let inner = self.clone();
            self.runtime.spawn(async move {
                let body = wire::encode_interest(&interest);
                let reply = inner.cluster.request(&parent.node, ENDPOINT, PROTOCOL, SCHEMA, body).await;
                let reply = match reply {
                    Ok(body) if body.first() == Some(&wire::ANSWERS) => {
                        wire::decode_answers(&mut body.slice(1..)).into()
                    }
                    Ok(body) if body.as_ref() == [wire::OUT_OF_SYNC] => Reply::OutOfSync,
                    _ => Reply::Failed,
                };
                inner.with(|core| core.interest_result(&parent, interest.revision, reply, Instant::now().into_std()));
            });
        }
    }
}

/// Writes what was published to the streams kept on disk, one write at a time: each starts as soon as the last has
/// landed and something waits, and takes all of it, so a record waits for one write in progress at most, never for a
/// tick. A failed write is tried again after `RETRY_WRITE`.
async fn write_disk(node: Weak<Inner>, writes: Arc<Notify>) {
    loop {
        let Some(inner) = node.upgrade() else { return };
        let (taken, aging) = {
            let mut core = inner.core.lock();
            (core.take_disk_writes(axon::trace::unix_micros() / 1000), core.disk_may_retain())
        };
        drop(inner);
        let Some((files, ops)) = taken else {
            if aging {
                let _ = tokio::time::timeout(RETAIN_EVERY, writes.notified()).await;
            } else {
                writes.notified().await;
            }
            continue;
        };
        // Its own task: a store that panics fails this write, which is taken again, and not the writer. It holds the
        // files, and so their directory, until the write lands, after the node is dropped.
        let write = tokio::spawn(async move { files.apply(ops).await });
        let written = write.await.unwrap_or_else(|e| Err(std::io::Error::other(e)));
        let failed = written.is_err();
        let Some(inner) = node.upgrade() else { return };
        let told = inner.with(|core| core.disk_written(written));
        inner.cursors.notify_waiters();
        if let Some((kind, detail)) = told {
            inner.cluster.events().record(kind, "", detail);
        }
        drop(inner);
        if failed {
            tokio::time::sleep(RETRY_WRITE).await;
        }
    }
}

impl StreamNode {
    /// Must run inside the cluster's tokio runtime. With a `disk`, it checks every segment of the disk's namespaces
    /// once.
    pub async fn start(cluster: &Cluster, cfg: StreamNodeConfig) -> Result<Self, ClusterError> {
        Self::start_on(cluster, cfg, axon_net::host_tcp()).await
    }

    /// As `start`, on a host with these TCP limits.
    pub(crate) async fn start_on(
        cluster: &Cluster,
        cfg: StreamNodeConfig,
        host: Option<axon_net::HostTcp>,
    ) -> Result<Self, ClusterError> {
        let window = cfg.core.max_push_window as u64;
        let identity = cluster.identity();
        let me = PeerId::new(&identity.node_id, identity.incarnation);
        if cfg.core.lanes.values().any(|lane| *lane == Lane::Control) {
            return Err(ClusterError::Config("a namespace's pushes go on Live or Bulk"));
        }
        if cfg.core.lanes.values().chain([&Lane::Live]).any(|lane| cfg.core.max_push_bytes > cluster.max_frame(*lane)) {
            return Err(ClusterError::Config("max_push_bytes exceeds the max_frame of a lane in use"));
        }
        let lost_interest = cfg.core.refresh + cluster.request_timeout() + cfg.core.retry_failed + 2 * cfg.tick;
        if cfg.downstream_expiry <= lost_interest {
            return Err(ClusterError::Config("downstream_expiry must outlast one lost interest"));
        }
        let cluster_wide =
            cfg.core.namespaces.values().any(|l| l.records_per_sec.or(l.bytes_per_sec).or(l.open_streams).is_some());
        if cluster_wide && cfg.quotas.is_none() {
            return Err(ClusterError::Config("a namespace's rates and open streams need StreamNodeConfig::quotas"));
        }
        let quotas = cfg.quotas.filter(|_| cluster_wide);
        let mut core = cfg.core;
        core.relay = !cfg.upstream.is_empty() || !cfg.upstream_by_namespace.is_empty();
        // Interests go on Control.
        core.max_interest_bytes = core.max_interest_bytes.min(cluster.max_frame(Lane::Control));
        let mut core = RelayCore::new(&me, cfg.authorizer, core).map_err(|_| ClusterError::Config("stream limits"))?;
        let writer = cfg.disk.is_some();
        if let Some(disk) = cfg.disk {
            let error = |e| ClusterError::Control(format!("the stream disk could not be read: {e}"));
            core.disk = Some(DiskStore::open(disk).await.map_err(error)?);
        }
        let inner = Arc::new(Inner {
            cluster: cluster.clone(),
            runtime: tokio::runtime::Handle::current(),
            core: Mutex::new(core),
            upstream: cfg.upstream,
            upstream_by_namespace: cfg.upstream_by_namespace,
            tick: cfg.tick,
            downstream_expiry: cfg.downstream_expiry,
            kick: Arc::new(Notify::new()),
            cursors: Arc::new(Notify::new()),
            writes: Arc::new(Notify::new()),
            ranked: Mutex::default(),
            endpoints: Mutex::default(),
            locals: Mutex::default(),
            pumps: Mutex::default(),
            tasks: Mutex::default(),
            quotas,
            permits: Mutex::default(),
        });
        // Interests are upkeep, on Control; the pushes go on their namespace's lane, the parent's choice.
        for (name, lane) in [(ENDPOINT, Lane::Control), (LIVE, Lane::Live), (BULK, Lane::Bulk)] {
            let weak = Arc::downgrade(&inner);
            let endpoint = cluster.endpoint(name, PROTOCOL, SCHEMA).lane(lane).serve(move |identity, body, reply| {
                let (Some(inner), Some(reply)) = (weak.upgrade(), reply) else { return Err(Rejection::Stopped) };
                reply.send(inner.answer(lane, identity, body)?).map_err(|_| Rejection::InvalidMessage)
            });
            match endpoint {
                Ok(endpoint) => inner.endpoints.lock().push(endpoint),
                Err(e) => {
                    inner.stop();
                    return Err(e);
                }
            }
        }
        let weak = Arc::downgrade(&inner);
        let mut membership = cluster.subscribe();
        let (tick, kick) = (inner.tick, inner.kick.clone());
        let task = tokio::spawn(async move {
            let mut left = BTreeSet::new();
            loop {
                // Every event queued: a parent that left and came back since the last turn is in the view again.
                while let Poll::Ready(event) =
                    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut membership).poll_next(cx))).await
                {
                    let Some(event) = event else { return };
                    departed(&event, &mut left);
                }
                match weak.upgrade() {
                    Some(inner) => inner.reconcile(&std::mem::take(&mut left)),
                    None => return,
                }
                tokio::select! {
                    _ = kick.notified() => {}
                    event = membership.next() => match event {
                        Some(event) => departed(&event, &mut left),
                        None => return,
                    },
                    _ = tokio::time::sleep(tick) => {}
                }
            }
        });
        inner.tasks.lock().push(task);
        if writer {
            let task = tokio::spawn(write_disk(Arc::downgrade(&inner), inner.writes.clone()));
            inner.tasks.lock().push(task);
        }
        // One connection moves at most its send buffer a round trip, and is sent at most half the receiver's.
        if let Some(host) = host {
            let short: Vec<String> = [("tcp_wmem", host.wmem_max, window), ("tcp_rmem", host.rmem_max, 2 * window)]
                .into_iter()
                .filter(|(_, max, want)| max < want)
                .map(|(name, max, want)| format!("{name} max is {max}. Set it to {want} or more."))
                .collect();
            if !short.is_empty() {
                let detail = format!(
                    "A far child gets less than the push window of {window} bytes in each round trip. {}",
                    short.join(" ")
                );
                cluster.events().record("host_tcp_limits", "", detail);
            }
        }
        Ok(Self(Arc::new(Handle(inner))))
    }

    /// Makes this node the key's origin. A key it relays becomes its own, and one it already originates is
    /// `AlreadyExists`: `restart_stream` it, or `close_stream` it first. In a namespace with `open_streams`, the
    /// stream holds one of its permits until it closes or this node goes.
    ///
    /// It starts nothing until it is polled, and holds the node until it ends. With a
    /// [`disk`](StreamNodeConfig::disk), opening a stream the disk holds reads its newest segments back first, while
    /// the node's other streams go on: from an actor's handler, hand the future to `cx.submit_future`. Dropped before
    /// it ends, it leaves the key unopened.
    pub fn open_stream(
        &self,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
    ) -> impl Future<Output = Result<(), StreamError>> + Send + 'static {
        let (node, key) = (self.0.clone(), key.clone());
        async move {
            let reader = node.core.lock().disk_read(&key, generation, policy)?;
            let read = match reader {
                Some(reader) => Some(reader.read().await),
                None => None,
            };
            node.with(|core| {
                let open = core.limits(&key).and_then(|l| l.open_streams).filter(|_| !core.originates(&key));
                let permit = match (&node.quotas, open) {
                    (Some(quotas), Some(max)) => {
                        Some(quotas.acquire(key.namespace(), &OPEN.with_max(max)).map_err(|denied| {
                            core.over_quota(&key, NamespaceLimit::OpenStreams, denied.retry_after())
                        })?)
                    }
                    _ => None,
                };
                core.open_read(&key, generation, policy, read)?;
                if let Some(permit) = permit {
                    node.permits.lock().insert(key.clone(), permit);
                }
                Ok(())
            })
        }
    }

    /// Admission is neither delivery nor durability. A rejection returns the records, and none of them was published:
    /// `UnknownStream` when this node does not originate the key (open it again), `LimitExceeded` for a batch past
    /// `hub.max_batch_records`, `hub.max_batch_bytes` or its namespace's rate a second (split it),
    /// `Recovery(InvalidBatch)` for an empty batch or more than one record under `ReplaceableState` or a record that is
    /// not [`keyed`](crate::keyed) under `Compacted`. None of them clears by waiting, and a slow subscriber never
    /// refuses a publish: its `slow_consumer` policy resets, detaches or coalesces it instead. `StateFull` for a batch
    /// that would leave a compacted stream past its limits clears once a publish removes keys. `QuotaExceeded` is the
    /// namespace's ring budget, which its compacted states count toward and closing another of its streams here makes
    /// room in, or its rate, which clears after `retry_after`. A refused batch takes none of the rate.
    ///
    /// On a stream kept on disk the cursor returned is admitted, not yet durable: the records reach subscribers once
    /// they are synced, and [`cursor`](Self::cursor) says how far that is. `Disk(Full)` or `Disk(Failed)` while the
    /// last write failed, and `Disk(Behind)` while `max_unwritten_bytes` wait for the disk, clear once a write
    /// succeeds. A refused batch is not written.
    pub fn publish(&self, key: &StreamKey, records: Vec<Payload>) -> Result<Cursor, PublishError<Vec<Payload>>> {
        let publish = |records: Vec<Payload>, trace| {
            let rated = |l: &NamespaceLimits| l.records_per_sec.or(l.bytes_per_sec).is_some();
            self.0.with(|core| {
                let quota = self.0.quotas.as_ref().and_then(|q| Some((q, core.limits(key).filter(rated)?)));
                core.publish_admitted(key, records, trace, |records| {
                    quota.map_or(Ok(()), |(quotas, limits)| take(quotas, key, limits, records))
                })
            })
        };
        let Some(parent) = TraceContext::current() else { return publish(records, None) };
        let (here, started) = (parent.child(), Instant::now());
        let mut span = Span::begin(parent, here.span, SpanKind::Stream, key.to_string());
        let published = publish(records, Some((here.trace.get(), here.span)));
        span.busy = started.elapsed();
        span.failed = published.is_err();
        self.0.cluster.tracer().record(span);
        published
    }

    pub fn restart_stream(&self, key: &StreamKey, generation: u64) -> Result<(), StreamError> {
        let restarted = self.0.with(|core| core.restart_stream(key, generation));
        self.0.cursors.notify_waiters();
        restarted
    }

    pub fn close_stream(&self, key: &StreamKey) -> Result<(), StreamError> {
        let closed = self.0.with(|core| {
            self.0.permits.lock().remove(key);
            core.close_stream(key)
        });
        self.0.cursors.notify_waiters();
        closed
    }

    /// The last record subscribers can see. On a stream kept on disk, the last one synced: a crash loses none of
    /// them.
    pub fn cursor(&self, key: &StreamKey) -> Option<Cursor> {
        self.0.core.lock().cursor(key)
    }

    /// Waits until [`cursor`](Self::cursor) reaches `at`, a cursor [`publish`](Self::publish) returned, and returns
    /// it: on a stream kept on disk once the write that holds `at` is synced, so an answer sent after it survives any
    /// crash; at once without a disk. Waiting on the last of several publishes waits for them all.
    ///
    /// `UnknownStream` where this node does not originate the key, or once the open that admitted `at` ends first
    /// (`close_stream`, `restart_stream`, the node dropped): `at` was not confirmed, and [`stored`](Self::stored) on
    /// the next start says what the disk kept. `Recovery(GenerationMismatch)` for another generation than the open's,
    /// `InvalidOptions` for a position never admitted. It waits on through `Disk(Full)` and `Disk(Failed)`, as each
    /// write is tried again: bound it with the caller's own deadline. It never keeps the node alive.
    pub fn wait_cursor(
        &self,
        key: &StreamKey,
        at: Cursor,
    ) -> impl Future<Output = Result<Cursor, StreamError>> + Send + 'static {
        let open = self.0.core.lock().wait_open(key, at);
        let (node, cursors, key) = (Arc::downgrade(&self.0.0), self.0.cursors.clone(), key.clone());
        async move {
            let open = match open? {
                Ok(open) => open,
                Err(visible) => return Ok(visible),
            };
            loop {
                let mut told = std::pin::pin!(cursors.notified());
                told.as_mut().enable();
                let Some(node) = node.upgrade() else { return Err(StreamError::UnknownStream) };
                let reached = node.core.lock().reached(&key, at, open);
                if let Some(reached) = reached {
                    return reached;
                }
                drop(node);
                told.await;
            }
        }
    }

    /// The generation and last record the disk holds of a stream this node has not opened since it started: open
    /// that generation to resume it. `None` for a stream the disk does not hold, and once it is open.
    pub fn stored(&self, key: &StreamKey) -> Option<Cursor> {
        self.0.core.lock().stored(key)
    }

    /// On a node with upstream tiers, subscribing to a stream it does not have starts pulling it. A
    /// `Live` subscription follows the key: a restart or a close and re-open is a new `Ready`, not an end.
    pub fn subscribe(&self, key: &StreamKey, options: SubscribeOptions) -> Result<Subscription, StreamError> {
        let inbox = Arc::new(Mutex::new(Inbox { events: VecDeque::new(), behind: true, open: true, waker: None }));
        let id = self.0.with(|core| core.subscribe(key, options))?;
        self.0.locals.lock().insert(id, inbox.clone());
        Ok(Subscription { node: self.0.clone(), id, inbox })
    }

    pub fn parent_of(&self, key: &StreamKey) -> Option<String> {
        self.0.core.lock().parent_of(key).map(|p| p.node.to_string())
    }

    pub fn stats(&self) -> RelayStats {
        self.0.core.lock().stats()
    }

    /// Push tasks alive, one per child and lane being fed; a test checks each ends with its child.
    #[doc(hidden)]
    pub fn pumps(&self) -> usize {
        self.0.pumps.lock().len()
    }

    /// Per-stream positions and recent events. Not sampled by telemetry: it grows with the streams.
    pub fn detail(&self, limit: usize) -> StreamDetail {
        let wall_ms = axon::trace::unix_micros() / 1000;
        // From any thread, on the node's clock.
        let now = {
            let _clock = self.0.runtime.enter();
            Instant::now().into_std()
        };
        let core = self.0.core.lock();
        let (streams, total) = core.streams(limit, now);
        StreamDetail { streams, total, events: core.events(now, wall_ms) }
    }
}

impl Inner {
    fn stop(&self) {
        for endpoint in std::mem::take(&mut *self.endpoints.lock()) {
            self.cluster.unserve(endpoint);
        }
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
        self.permits.lock().clear();
        // The directory is free for the next node once a write still landing lets it go.
        self.core.lock().disk = None;
        self.cursors.notify_waiters();
    }
}

/// Again for a pump that a task's last turn started after the handle stopped the node.
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A local subscriber. Dropping it unsubscribes.
pub struct Subscription {
    node: Arc<Handle>,
    id: u64,
    inbox: Arc<Mutex<Inbox>>,
}

/// What the hub has handed one subscription. It takes a fill only when empty, so it holds at
/// most one hub queue's worth and the hub's limits still see a subscriber that falls behind.
struct Inbox {
    events: VecDeque<StreamEvent>,
    /// The hub may hold events a fill left there.
    behind: bool,
    open: bool,
    waker: Option<Waker>,
}

impl Inbox {
    /// Called with the core locked. Returns the waker to wake once this inbox is unlocked.
    fn fill(&mut self, core: &mut RelayCore<SharedAuthorizer>, id: u64) -> Option<Waker> {
        if !self.events.is_empty() {
            self.behind = true;
            return None;
        }
        self.events.extend(std::iter::from_fn(|| core.poll(id)));
        (self.behind, self.open) = (false, core.is_subscribed(id));
        self.waker.take()
    }
}

impl Subscription {
    /// The next event; `None` after the terminal one.
    pub async fn next(&mut self) -> Option<StreamEvent> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}

/// A subscription is a stream of events, so an actor can own it: `cx.attach(subscription, map)`.
impl Stream for Subscription {
    type Item = StreamEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<StreamEvent>> {
        let this = self.get_mut();
        loop {
            {
                let mut inbox = this.inbox.lock();
                if let Some(event) = inbox.events.pop_front() {
                    return Poll::Ready(Some(event));
                }
                if !inbox.open {
                    return Poll::Ready(None);
                }
                if !inbox.behind {
                    // Registered under the lock a fill takes, so a fill cannot slip in unseen.
                    if !inbox.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                        inbox.waker = Some(cx.waker().clone());
                    }
                    return Poll::Pending;
                }
            }
            let (id, inbox) = (this.id, &this.inbox);
            this.node.with(|core| drop(inbox.lock().fill(core, id)));
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.node.locals.lock().remove(&self.id);
        self.node.with(|core| core.unsubscribe(self.id));
    }
}

#[cfg(test)]
mod tests;
