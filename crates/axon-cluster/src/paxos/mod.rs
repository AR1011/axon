//! Leases decided by a majority of acceptor nodes, with no leader; each acceptor logs who holds what, for the keys a
//! node held to be found after every acceptor restarted (spec/paxos-leases.md). The cores in `core.rs` decide; this
//! drives them over the cluster and writes the log.

mod core;
#[cfg(test)]
mod sim;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use axon::Lane;
use axon::files::Files;
use axon::trace::unix_micros as unix_us;
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use self::core::{
    Acceptor, Answer, HELD_SCHEMA, Item, MAX_ACCEPTORS, MAX_LEASE, Out, Proposer, SCHEMA, Stored, decode_answers,
    decode_held, decode_held_ask, decode_items, encode_answers, encode_held, encode_held_ask, encode_items,
};
use crate::ownership::{lease_hold, lease_wait};
use crate::{
    BoxFuture, Cluster, ClusterError, Holder, OwnershipAuthority, OwnershipError, ProviderInfo, ProviderLink,
    Rejection, Reply, WeakCluster,
};

const SERVICE: &str = "axon.lease";
/// Which keys a node holds, asked of an acceptor.
const HELD: &str = "axon.lease.held";
const PROTOCOL: u64 = 0x4158_4f4e_4c45_4153;
const SWEEP_EVERY: Duration = Duration::from_secs(1);
/// How long the log waits after a failed write before it tries again.
const RETRY_WRITE: Duration = Duration::from_secs(1);

pub struct PaxosConfig {
    /// Node ids of the acceptors, the same list on every node. 1 to 7 entries.
    pub acceptors: Vec<String>,
    /// How long an acceptor keeps a lease it accepted, at most 5 min. `lease_grace` must stay inside it.
    pub lease: Duration,
    /// Upper bound on how fast two clocks drift apart, as a fraction.
    pub drift: f64,
    /// Most keys one acceptor tracks, counting those its log restored and lapsed leases it still lists; a request
    /// about another is answered `Full`. Every acceptor sees every key, so this bounds the keys of the whole cluster.
    pub max_leases: usize,
    /// Where an acceptor logs the leases it grants, each synced before the grant is answered while the store works
    /// (a write that fails, or hangs past a round's patience, lets answers go unsynced until one lands): required on a
    /// node that is on `acceptors`, and kept across its restarts (a directory of its own, `axon::files::OsFiles`, one
    /// per `PaxosLeases`), or a restart of every acceptor at once forgets which node held which key. Not read on other
    /// nodes.
    pub store: Option<Arc<dyn Files>>,
}

impl PaxosConfig {
    pub fn new(acceptors: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            acceptors: acceptors.into_iter().map(Into::into).collect(),
            lease: Duration::from_secs(6),
            drift: 0.01,
            max_leases: 100_000,
            store: None,
        }
    }

    /// How long a holder believes after sending a propose.
    fn hold(&self) -> Duration {
        lease_hold(self.lease, self.drift)
    }

    /// How long a round waits for answers.
    fn patience(&self) -> Duration {
        self.lease / 6
    }

    /// The endpoint's protocol id, so that nodes whose acceptors, lease or drift differ refuse each
    /// other's votes: their majorities need not share an acceptor, and a lease one believes may
    /// outlast another's restart wait. The order of the list does not matter. The log's header carries it too, so an
    /// acceptor does not list another configuration's leases.
    fn protocol(&self) -> u64 {
        let mut acceptors: Vec<_> = self.acceptors.iter().map(String::as_bytes).collect();
        acceptors.sort();
        let settings = [(self.lease.as_micros() as u64).to_le_bytes(), self.drift.to_bits().to_le_bytes()];
        let bytes = acceptors.into_iter().flat_map(|a| a.iter().chain([&0])).chain(settings.iter().flatten());
        crate::placement::fnv(PROTOCOL, bytes.copied())
    }
}

/// Leases for one node over the cluster. A node whose id is in `acceptors` also serves as one.
pub struct PaxosLeases(Arc<Shared>);

struct Shared {
    cfg: PaxosConfig,
    protocol: u64,
    /// The system's, from `attach` on.
    clock: OnceLock<axon::Clock>,
    cluster: OnceLock<WeakCluster>,
    local: OnceLock<String>,
    proposer: Mutex<Option<Proposer>>,
    acceptor: OnceLock<Mutex<Accepting>>,
    /// Wakes the task that writes the acceptor's log.
    writes: Arc<Notify>,
    acquiring: Mutex<HashMap<Arc<str>, VecDeque<Waiter>>>,
    renewal: Mutex<Option<Renewal>>,
    /// Keys lost that no renewal has named yet, some seen by a renewal whose caller gave up.
    gone: Mutex<BTreeSet<Arc<str>>>,
    links: Vec<Mutex<Link>>,
    acquired: AtomicU64,
    held_by_other: AtomicU64,
    lost: AtomicU64,
}

type Waiter = oneshot::Sender<Result<u64, OwnershipError>>;

struct Accepting {
    core: Acceptor,
    swept: Duration,
    /// Answers about keys whose last grant or release is not synced yet, each with the frame it waits for: at most a
    /// round's patience of them, since a write out longer counts as failing.
    waiting: Vec<(u64, To, Vec<Answer>)>,
}

/// Who an acceptor's answers go to.
enum To {
    Peer(Reply),
    Local(oneshot::Sender<Vec<Answer>>),
}

impl To {
    fn send(self, answers: Vec<Answer>) {
        match self {
            To::Peer(reply) => drop(reply.send(encode_answers(&answers))),
            To::Local(tx) => drop(tx.send(answers)),
        }
    }
}

/// One renewal out at a time. Whoever asks meanwhile is refused: its rounds were sent earlier than
/// it asked, and the caller counts a key it does not name as extended from when it asked.
struct Renewal {
    /// The keys it could not extend, but for those lost.
    failed: Vec<(String, OwnershipError)>,
    done: oneshot::Sender<Vec<(String, OwnershipError)>>,
}

#[derive(Default)]
struct Link {
    /// Since when the last request's outcome, answered or `error`, holds; `None` before the first.
    since: Option<Duration>,
    /// When the last request ended, or `can_grant` asked a failing link again.
    checked: Option<Duration>,
    error: Option<String>,
}

impl PaxosLeases {
    pub fn new(cfg: PaxosConfig) -> Result<Self, OwnershipError> {
        let invalid = |why: &str| Err(OwnershipError::Unavailable(why.into()));
        if cfg.acceptors.is_empty() || cfg.acceptors.len() > MAX_ACCEPTORS {
            return invalid(&format!("between 1 and {MAX_ACCEPTORS} acceptors"));
        }
        if cfg.acceptors.iter().enumerate().any(|(i, a)| cfg.acceptors[..i].contains(a)) {
            return invalid("an acceptor listed twice would vote twice");
        }
        if !(0.0..0.1).contains(&cfg.drift) {
            return invalid("drift must be below 10 %");
        }
        if cfg.lease > MAX_LEASE {
            return invalid("the lease must be at most 5 min");
        }
        if cfg.hold() < cfg.lease / 2 {
            return invalid("the lease is too short for the drift and the margin");
        }
        let links = cfg.acceptors.iter().map(|_| Mutex::default()).collect();
        Ok(Self(Arc::new(Shared {
            protocol: cfg.protocol(),
            cfg,
            clock: OnceLock::new(),
            cluster: OnceLock::new(),
            local: OnceLock::new(),
            proposer: Mutex::new(None),
            acceptor: OnceLock::new(),
            writes: Arc::default(),
            acquiring: Mutex::default(),
            renewal: Mutex::new(None),
            gone: Mutex::default(),
            links,
            acquired: AtomicU64::new(0),
            held_by_other: AtomicU64::new(0),
            lost: AtomicU64::new(0),
        })))
    }

    /// Whether this node's acceptor still refuses everything after its start, because it forgot what it
    /// promised before. False on a node that is not an acceptor. Restart the next acceptor only once it is false.
    pub fn warming(&self) -> bool {
        self.0.acceptor.get().is_some_and(|a| a.lock().core.warming(self.0.now()))
    }
}

impl Drop for Shared {
    /// The log's writer holds only a weak reference, and stops when woken without one.
    fn drop(&mut self) {
        self.writes.notify_one();
    }
}

impl Shared {
    /// On the system's clock, so that a read from any thread is right where a simulation gives each node its own.
    fn now(&self) -> Duration {
        self.clock.get().map_or(Duration::ZERO, axon::Clock::now)
    }

    /// Runs `f` on the proposer, then sends what it asks for and hands out what it decided, all
    /// under the proposer's lock so that answers reach their waiters in the order it gave them.
    fn step(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Proposer, Duration, u64, &mut Vec<Out>),
    ) -> Result<Vec<JoinHandle<()>>, OwnershipError> {
        let mut proposer = self.proposer.lock();
        let p = proposer.as_mut().ok_or_else(|| OwnershipError::Unavailable("not attached to a cluster".into()))?;
        let mut out = Vec::new();
        f(p, self.now(), unix_us(), &mut out);
        Ok(self.dispatch(p, out))
    }

    /// Not generic, unlike `step`: the replies it spawns step again.
    fn dispatch(self: &Arc<Self>, p: &mut Proposer, out: Vec<Out>) -> Vec<JoinHandle<()>> {
        let (mut sends, mut out) = (Vec::new(), VecDeque::from(out));
        while let Some(o) = out.pop_front() {
            match o {
                Out::Send { request, acceptor, items } => {
                    let this = self.clone();
                    sends.push(tokio::spawn(async move {
                        let answers = this.ask(acceptor, items).await;
                        let _ = this.step(|p, now, unix_us, out| p.reply(request, answers, now, unix_us, out));
                    }));
                }
                Out::Done { key, renew: true, result: Err(OwnershipError::Lost) } => {
                    self.lost.fetch_add(1, Relaxed);
                    self.gone.lock().insert(key);
                }
                Out::Done { key, renew: true, result: Err(error) } => {
                    if let Some(renewal) = self.renewal.lock().as_mut() {
                        renewal.failed.push((key.to_string(), error));
                    }
                }
                Out::Done { renew: true, .. } => {}
                Out::Done { key, renew: false, result } => {
                    match &result {
                        Ok(_) => {
                            self.acquired.fetch_add(1, Relaxed);
                            // An earlier loss of the key is no news to whoever holds it now.
                            self.gone.lock().remove(&key);
                        }
                        Err(OwnershipError::Held(_)) => {
                            self.held_by_other.fetch_add(1, Relaxed);
                        }
                        Err(_) => {}
                    }
                    let waiter = {
                        let mut acquiring = self.acquiring.lock();
                        let waiter = acquiring.get_mut(&key).and_then(VecDeque::pop_front);
                        if acquiring.get(&key).is_some_and(VecDeque::is_empty) {
                            acquiring.remove(&key);
                        }
                        waiter
                    };
                    // Its caller gave up (`Pending`).
                    let unheard = match waiter {
                        Some(waiter) => waiter.send(result).err(),
                        None => Some(result),
                    };
                    if let Some(Ok(_)) = unheard {
                        let mut released = Vec::new();
                        p.release(&key, &mut released);
                        out.extend(released);
                    }
                }
            }
        }
        if !p.renewing()
            && let Some(renewal) = self.renewal.lock().take()
        {
            let _ = renewal.done.send(renewal.failed);
        }
        sends
    }

    /// One acceptor's answers, or `None` when it did not answer in time or the request failed; `links` says why.
    async fn ask(&self, acceptor: usize, items: Vec<Item>) -> Option<Vec<Answer>> {
        let node = &self.cfg.acceptors[acceptor];
        let answers = if self.local.get() == Some(node) {
            let (tx, rx) = oneshot::channel();
            match self.accept(&items, To::Local(tx)) {
                // Held for the log as a peer's are: the same patience, or the round keeps the request in flight.
                Ok(()) => match tokio::time::timeout(self.cfg.patience(), rx).await {
                    Ok(answers) => answers.map_err(|_| "stopped".to_owned()),
                    Err(_) => Err("timed out".to_owned()),
                },
                Err(why) => Err(why.to_owned()),
            }
        } else {
            let cluster = self.cluster.get().and_then(WeakCluster::upgrade);
            match cluster {
                None => Err("the cluster stopped".to_owned()),
                Some(cluster) => {
                    let call = cluster.request(node, SERVICE, self.protocol, SCHEMA, encode_items(&items));
                    match tokio::time::timeout(self.cfg.patience(), call).await {
                        Ok(Ok(body)) => decode_answers(body).ok_or_else(|| "undecodable answer".to_owned()),
                        Ok(Err(ClusterError::ProtocolMismatch)) => {
                            Err("another acceptor list, lease or drift".to_owned())
                        }
                        Ok(Err(ClusterError::SchemaMismatch { .. })) => Err("another axon version".to_owned()),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("timed out".to_owned()),
                    }
                }
            }
        };
        let (mut link, now) = (self.links[acceptor].lock(), self.now());
        let error = answers.as_ref().err().cloned();
        if link.since.is_none() || link.error.is_some() != error.is_some() {
            link.since = Some(now);
        }
        (link.checked, link.error) = (Some(now), error);
        answers.ok()
    }

    /// The keys of `kinds` that `holder` holds at one acceptor, page by page, or `None` when it did not answer, is
    /// warming, or serves another schema of the question.
    async fn held_at(&self, acceptor: usize, holder: Holder<'_>, kinds: &[String]) -> Option<Vec<String>> {
        let target = &self.cfg.acceptors[acceptor];
        let mut keys: Vec<String> = Vec::new();
        loop {
            let after = keys.last().map_or("", String::as_str);
            let (page, more) = if self.local.get() == Some(target) {
                let (page, more) = self.held(holder, kinds, after)?;
                (page.iter().map(|key| key.to_string()).collect(), more)
            } else {
                let cluster = self.cluster.get()?.upgrade()?;
                let ask = encode_held_ask(holder, after, kinds);
                let call = cluster.request(target, HELD, self.protocol, HELD_SCHEMA, ask);
                decode_held(tokio::time::timeout(self.cfg.patience(), call).await.ok()?.ok()?)?
            };
            // An empty page cannot move `after` on.
            let done = !more || page.is_empty();
            keys.extend(page);
            if done {
                return Some(keys);
            }
        }
    }

    fn start_proposer(&self, ballot_node: Arc<str>) {
        let cfg = &self.cfg;
        // Two phases, each waiting at most for one request ahead of its own.
        let round = cfg.patience() * 4;
        *self.proposer.lock() = Some(Proposer::new(ballot_node, cfg.acceptors.len(), cfg.lease, cfg.hold(), round));
    }

    /// Serves as an acceptor that refuses everything for `wait`, with its log and the task that writes it, if any.
    fn start_acceptor(self: &Arc<Self>, wait: Duration, max_leases: usize, log: Option<(Stored, Arc<dyn Files>)>) {
        let now = self.now();
        let core = Acceptor::new(now, unix_us(), wait, max_leases);
        let (core, files) = match log {
            Some((stored, files)) => (core.with_log(stored, self.protocol, now), Some(files)),
            None => (core, None),
        };
        let _ = self.acceptor.set(Mutex::new(Accepting { core, swept: now, waiting: Vec::new() }));
        if let Some(files) = files {
            tokio::spawn(write_log(Arc::downgrade(self), self.writes.clone(), files, self.cfg.patience()));
        }
    }

    fn held(&self, holder: Holder<'_>, kinds: &[String], after: &str) -> Option<(Vec<Arc<str>>, bool)> {
        self.acceptor.get()?.lock().core.held_by(holder, kinds, after, self.now())
    }

    /// Answers `items` to `to`, once the log has synced what they answer, unless this is not an acceptor.
    fn accept(&self, items: &[Item], to: To) -> Result<(), &'static str> {
        let (now, unix_us) = (self.now(), unix_us());
        let mut acceptor = self.acceptor.get().ok_or("not serving as an acceptor")?.lock();
        let Accepting { core, swept, waiting } = &mut *acceptor;
        if now >= *swept + SWEEP_EVERY {
            core.sweep(now);
            *swept = now;
            // What it forgot is logged whether or not an answer waits for a write.
            self.writes.notify_one();
        }
        let (answers, frame) = core.answer_all(items, now, unix_us);
        let Some(frame) = frame else {
            drop(acceptor);
            to.send(answers);
            return Ok(());
        };
        waiting.push((frame, to, answers));
        self.writes.notify_one();
        Ok(())
    }

    /// Sends the answers the write out lets go of: those it synced once it lands (`Some(true)`), and all of them while
    /// the store fails, after a failed write (`Some(false)`) or one hung past a round's patience (`None`), until one
    /// lands. Returns whether the store fails.
    fn written(&self, landed: Option<bool>) -> bool {
        let Some(acceptor) = self.acceptor.get() else { return false };
        let mut acceptor = acceptor.lock();
        let Accepting { core, waiting, .. } = &mut *acceptor;
        match landed {
            Some(ok) => core.written(ok),
            None => core.stalled(),
        }
        let failing = core.store_failing();
        let synced = if failing { u64::MAX } else { core.synced() };
        let answers: Vec<_> = waiting.extract_if(.., |(frame, ..)| *frame <= synced).collect();
        drop(acceptor);
        answers.into_iter().for_each(|(_, to, answers)| to.send(answers));
        failing
    }
}

/// Writes the acceptor's log, one write at a time, each holding what was logged while the last was out. An answer
/// later than `patience` counts as none, so a write out longer counts as failing until it lands; this task, and the
/// store with it, stays held by a write that never does.
async fn write_log(shared: Weak<Shared>, writes: Arc<Notify>, files: Arc<dyn Files>, patience: Duration) {
    loop {
        let Some(this) = shared.upgrade() else { return };
        let ops = this.acceptor.get().and_then(|a| a.lock().core.take_write());
        drop(this);
        let Some(ops) = ops else {
            writes.notified().await;
            continue;
        };
        // Its own task: a store that panics fails this write, which is taken again, and not the writer.
        let files = files.clone();
        let mut write = tokio::spawn(async move { files.apply(ops).await });
        let landed = match tokio::time::timeout(patience, &mut write).await {
            Ok(landed) => landed,
            Err(_) => {
                let Some(this) = shared.upgrade() else { return };
                this.written(None);
                drop(this);
                write.await
            }
        };
        let ok = matches!(landed, Ok(Ok(())));
        let Some(this) = shared.upgrade() else { return };
        let failing = this.written(Some(ok));
        drop(this);
        if failing {
            tokio::time::sleep(RETRY_WRITE).await;
        }
    }
}

impl OwnershipAuthority for PaxosLeases {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        let key: Arc<str> = key.into();
        Box::pin(async move {
            if key.len() > u16::MAX as usize {
                return Err(OwnershipError::Unavailable("key longer than 64 KiB".into()));
            }
            let (tx, rx) = oneshot::channel();
            self.0.acquiring.lock().entry(key.clone()).or_default().push_back(tx);
            if let Err(error) = self.0.step(|p, now, unix_us, out| p.acquire(key.clone(), now, unix_us, out)) {
                self.0.acquiring.lock().remove(&key);
                return Err(error);
            }
            let mut pending = Pending { shared: &self.0, key, rx };
            (&mut pending.rx).await.unwrap_or_else(|_| Err(OwnershipError::Unavailable("stopped".into())))
        })
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            let mut started = false;
            self.0.step(|p, now, _, out| {
                let mut renewal = self.0.renewal.lock();
                if renewal.is_none() {
                    *renewal = Some(Renewal { failed: Vec::new(), done: tx });
                    drop(renewal);
                    p.renew(now, out);
                    started = true;
                } else {
                    // Only a renewal ends rounds that ran out of time; without this one it could stay out.
                    p.tick(now, out);
                }
            })?;
            if !started {
                return Err(OwnershipError::Unavailable("a renewal is still out".into()));
            }
            let mut failed = rx.await.map_err(|_| OwnershipError::Unavailable("stopped".into()))?;
            failed.extend(self.0.gone.lock().iter().map(|key| (key.to_string(), OwnershipError::Lost)));
            Ok(failed)
        })
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        let key = key.to_owned();
        Box::pin(async move {
            self.0.gone.lock().remove(key.as_str());
            let sends = self.0.step(|p, now, _, out| p.release_at(&key, epoch, now, out))?;
            join(sends).await;
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Ok(sends) = self.0.step(|p, _, _, out| p.release_all(out)) {
                join(sends).await;
            }
        })
    }

    fn attach<'a>(&'a self, cluster: &'a Cluster) -> BoxFuture<'a, Result<(), ClusterError>> {
        Box::pin(async move {
            let identity = cluster.identity();
            let node = identity.node_id.clone();
            let ballot_node: Arc<str> = format!("{node}/{:x}", identity.incarnation).into();
            let cfg = &self.0.cfg;
            // Before anything is set: an attach that fails here leaves these leases as they were.
            let log = match (cfg.acceptors.contains(&node), cfg.store.clone()) {
                (false, _) => None,
                (true, None) => {
                    return Err(ClusterError::Config("a node on the acceptor list needs PaxosConfig::store"));
                }
                (true, Some(files)) => {
                    let stored = Stored::load(&*files, self.0.protocol).await.map_err(|e| {
                        ClusterError::Ownership(OwnershipError::Unavailable(format!("the acceptor's lease log: {e}")))
                    })?;
                    Some((stored, files))
                }
            };
            if self.0.cluster.set(cluster.downgrade()).is_err() {
                return Err(ClusterError::Config("these Paxos leases already serve a cluster"));
            }
            let _ = self.0.clock.set(cluster.clock().clone());
            let _ = self.0.local.set(node.clone());
            self.0.start_proposer(ballot_node);
            let Some(log) = log else { return Ok(()) };
            self.0.start_acceptor(lease_wait(cfg.lease, cfg.drift), cfg.max_leases, Some(log));
            let shared = self.0.clone();
            cluster.endpoint(SERVICE, self.0.protocol, SCHEMA).lane(Lane::Control).serve(move |_, body, reply| {
                let reply = reply.ok_or(Rejection::InvalidMessage)?;
                let items = decode_items(body).ok_or(Rejection::InvalidMessage)?;
                shared.accept(&items, To::Peer(reply)).map_err(|_| Rejection::Stopped)
            })?;
            let shared = self.0.clone();
            cluster.endpoint(HELD, self.0.protocol, HELD_SCHEMA).lane(Lane::Control).serve(move |_, body, reply| {
                let reply = reply.ok_or(Rejection::InvalidMessage)?;
                let (node, after, kinds) = decode_held_ask(body).ok_or(Rejection::InvalidMessage)?;
                let holder = node.as_deref().map_or(Holder::Lapsed, Holder::Node);
                let (keys, more) = shared.held(holder, &kinds, &after).ok_or(Rejection::Stopped)?;
                let _ = reply.send(encode_held(&keys, more));
                Ok(())
            })?;
            Ok(())
        })
    }

    /// The union of what a majority of acceptors answer: a lease a majority accepted is at one of them.
    fn held_by(
        &self,
        holder: Holder<'_>,
        kinds: &[String],
    ) -> Option<BoxFuture<'_, Result<Vec<String>, OwnershipError>>> {
        let node: Option<Arc<str>> = match holder {
            Holder::Node(node) => Some(node.into()),
            Holder::Lapsed => None,
        };
        let kinds: Arc<[String]> = kinds.into();
        Some(Box::pin(async move {
            let mut asks = JoinSet::new();
            for acceptor in 0..self.0.cfg.acceptors.len() {
                let (shared, node, kinds) = (self.0.clone(), node.clone(), kinds.clone());
                asks.spawn(async move {
                    let holder = node.as_deref().map_or(Holder::Lapsed, Holder::Node);
                    shared.held_at(acceptor, holder, &kinds).await
                });
            }
            let (mut answered, mut keys) = (0, BTreeSet::new());
            while let Some(answer) = asks.join_next().await {
                if let Ok(Some(held)) = answer {
                    answered += 1;
                    keys.extend(held);
                }
            }
            if answered * 2 <= self.0.cfg.acceptors.len() {
                return Err(OwnershipError::Unavailable("no majority of acceptors answered".into()));
            }
            Ok(keys.into_iter().collect())
        }))
    }

    fn hold(&self) -> Option<Duration> {
        Some(self.0.cfg.hold())
    }

    /// More than half the acceptors are this node, or reachable in its view and not failing: their last request was
    /// answered. A failure counts until a request is: one older than a round's patience is asked about again, so that a
    /// link recovers on a node with no round out, as one with no key to renew.
    fn can_grant(&self) -> bool {
        let s = &self.0;
        let Some(cluster) = s.cluster.get().and_then(WeakCluster::upgrade) else { return false };
        let (view, local, now) = (cluster.view(), s.local.get(), s.now());
        let mut up = 0;
        for (acceptor, (target, link)) in s.cfg.acceptors.iter().zip(&s.links).enumerate() {
            if local == Some(target) {
                up += 1;
                continue;
            }
            let mut link = link.lock();
            let failed = link.error.is_some();
            if failed && link.checked.is_none_or(|t| now >= t + s.cfg.patience()) {
                // Once a patience: the ask ends within one.
                link.checked = Some(now);
                let shared = s.clone();
                // On the cluster's runtime: a start asks from wherever its caller runs. No items: answered by any
                // acceptor of the same list, and it changes nothing.
                cluster.0.engine.tokio.spawn(async move { shared.ask(acceptor, Vec::new()).await });
            }
            if !failed && view.get(target).is_some_and(|m| m.reachable) {
                up += 1;
            }
        }
        up * 2 > s.cfg.acceptors.len()
    }

    fn warming(&self) -> bool {
        PaxosLeases::warming(self)
    }

    fn describe(&self) -> ProviderInfo {
        let (s, now) = (&self.0, self.0.now());
        let held = s.proposer.lock().as_ref().map_or(0, |p| p.held(now));
        let mut info = ProviderInfo::new("paxos")
            .setting("acceptors", s.cfg.acceptors.join(", "))
            .setting("lease", format!("{:?}", s.cfg.lease))
            .setting("drift", format!("{} %", s.cfg.drift * 100.0))
            .stat("held", held)
            .stat("acquired", s.acquired.load(Relaxed))
            .stat("held by other", s.held_by_other.load(Relaxed))
            .stat("lost", s.lost.load(Relaxed));
        if let Some(acceptor) = s.acceptor.get() {
            let a = &acceptor.lock().core;
            info = info
                .stat("leases", a.leases(now))
                .stat("stored", a.stored())
                .stat("floor", a.floor())
                .stat("warming", a.warming(now))
                .stat("refused", a.refused)
                .stat("clock behind", a.clock_behind)
                .stat("store failing", a.store_failing())
                .stat("store behind", a.store_behind);
        }
        info.links = s
            .cfg
            .acceptors
            .iter()
            .zip(&s.links)
            .map(|(target, link)| {
                let link = link.lock();
                ProviderLink {
                    target: target.clone(),
                    identity: None,
                    state: match (link.since, &link.error) {
                        (None, _) => "idle",
                        (_, None) => "answering",
                        (_, Some(_)) => "failing",
                    }
                    .into(),
                    since: link.since.map_or(Duration::ZERO, |t| now.saturating_sub(t)),
                    error: link.error.clone(),
                }
            })
            .collect();
        info
    }
}

/// An acquire's answer until it is read. A grant nobody reads is given up: nothing here would renew or release it,
/// and the next acquire would be answered from it while the acceptors let it lapse.
struct Pending<'a> {
    shared: &'a Arc<Shared>,
    key: Arc<str>,
    rx: oneshot::Receiver<Result<u64, OwnershipError>>,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        // Closed first: a grant sent after this fails to arrive, and `dispatch` gives it up.
        self.rx.close();
        if let Ok(Ok(epoch)) = self.rx.try_recv() {
            let _ = self.shared.step(|p, now, _, out| p.release_at(&self.key, epoch, now, out));
        }
    }
}

async fn join(sends: Vec<JoinHandle<()>>) {
    for send in sends {
        let _ = send.await;
    }
}

/// What a store in memory's future gives: it is ready at once.
#[cfg(test)]
fn ready<T>(future: impl Future<Output = T>) -> T {
    match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("a store in memory waited"),
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::task::{Context, Waker};

    use axon::files::{FileFuture, FileOp};

    use super::core::{Ballot, Kind};
    use super::*;

    /// One node that is its own and only acceptor, without a cluster.
    fn alone() -> PaxosLeases {
        alone_on(PaxosConfig::new(["a"]))
    }

    /// As `alone`, with `cfg`'s lease and, if it has one, a log in its store.
    fn alone_on(cfg: PaxosConfig) -> PaxosLeases {
        let leases = PaxosLeases::new(cfg).unwrap();
        let s = &leases.0;
        s.clock.set(axon::Clock::system()).unwrap();
        s.local.set("a".into()).unwrap();
        let log = s.cfg.store.clone().map(|files| (ready(Stored::load(&*files, s.protocol)).unwrap(), files));
        s.start_acceptor(Duration::ZERO, 16, log);
        s.start_proposer("a/1".into());
        leases
    }

    /// The id is in every vote and log header: one that changed would refuse the votes of nodes not yet upgraded.
    #[test]
    fn the_protocol_id_of_a_configuration_stays_the_same() {
        assert_eq!(PaxosConfig::new(["p-1", "p-0", "p-2"]).protocol(), 0x15d1_2422_680c_6799);
    }

    /// A store whose writes never land.
    struct Hung;

    impl Files for Hung {
        fn list(&self) -> FileFuture<Vec<(String, u64)>> {
            Box::pin(std::future::ready(Ok(Vec::new())))
        }

        fn read(&self, _: &str) -> FileFuture<Vec<u8>> {
            Box::pin(std::future::ready(Err(io::ErrorKind::NotFound.into())))
        }

        fn apply(&self, _: Vec<FileOp>) -> FileFuture<()> {
            Box::pin(std::future::pending())
        }
    }

    /// A write that hangs fails answers open as one that fails does: none waits for it past a round's patience, so
    /// they hold no more of the node's inbound replies than a patience's worth, and grants go on.
    #[tokio::test]
    async fn a_write_that_hangs_fails_answers_open_until_it_lands() {
        let mut cfg = PaxosConfig::new(["a"]);
        // A patience of 100 ms.
        cfg.lease = Duration::from_millis(600);
        cfg.store = Some(Arc::new(Hung));
        let leases = alone_on(cfg);
        tokio::time::sleep(Duration::from_millis(250)).await;
        for key in ["e/1", "e/2", "e/3"] {
            assert!(leases.acquire(key).await.is_ok(), "{key}");
        }
        let acceptor = leases.0.acceptor.get().unwrap().lock();
        assert!(acceptor.waiting.is_empty() && acceptor.core.store_failing());
    }

    /// Another node wins `key` at the only acceptor.
    fn take_over(leases: &PaxosLeases, key: &str, epoch: u64) {
        let (now, unix_us) = (leases.0.now(), unix_us());
        let acceptor = &mut leases.0.acceptor.get().unwrap().lock().core;
        let ballot = Ballot { epoch: epoch + 1000, node: "b/1".into() };
        for kind in [Kind::Prepare, Kind::Propose(Duration::from_secs(6))] {
            acceptor.answer(&Item { key: key.into(), ballot: ballot.clone(), kind }, now, unix_us);
        }
    }

    #[tokio::test]
    async fn a_key_taken_over_is_the_only_one_a_renewal_reports() {
        let leases = alone();
        let epoch = leases.acquire("e/k").await.unwrap();
        leases.acquire("e/j").await.unwrap();
        take_over(&leases, "e/k", epoch);
        let kinds = ["e".to_string()];
        let held = async |node, kinds: &[String]| leases.held_by(Holder::Node(node), kinds).unwrap().await;
        assert_eq!(held("a", &kinds).await, Ok(vec!["e/j".into()]));
        assert_eq!(held("b", &kinds).await, Ok(vec!["e/k".into()]));
        assert_eq!(held("a", &["x".into()]).await, Ok(vec![]), "only the kinds asked for");
        assert_eq!(leases.renew().await, Ok(vec![("e/k".into(), OwnershipError::Lost)]));
        assert_eq!(leases.renew().await, Ok(vec![("e/k".into(), OwnershipError::Lost)]), "until released");
        leases.release("e/k", epoch).await.unwrap();
        assert_eq!(leases.renew().await, Ok(vec![]));
        assert_eq!(leases.0.proposer.lock().as_ref().unwrap().held(leases.0.now()), 1, "e/j is still held");
    }

    #[tokio::test]
    async fn a_grant_its_caller_gave_up_on_is_released() {
        let leases = alone();
        // Dropped before the grant arrives, and after it arrived unread.
        for (key, arrived) in [("e/early", false), ("e/late", true)] {
            let mut abandoned = leases.acquire(key);
            assert!(abandoned.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            if arrived {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            drop(abandoned);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(leases.0.proposer.lock().as_ref().unwrap().held(leases.0.now()), 0, "{key}");
            assert_eq!(leases.held_by(Holder::Node("a"), &["e".into()]).unwrap().await, Ok(vec![]), "{key}");
        }
        let epoch = leases.acquire("e/late").await.unwrap();
        assert_eq!(leases.acquire("e/late").await, Ok(epoch), "a grant read is kept");
    }

    #[tokio::test]
    async fn a_renewal_asked_for_while_one_is_out_is_refused() {
        let leases = alone();
        leases.acquire("k").await.unwrap();
        let refused = Err(OwnershipError::Unavailable("a renewal is still out".into()));
        assert_eq!(tokio::join!(leases.renew(), leases.renew()), (Ok(vec![]), refused));
        assert_eq!(leases.renew().await, Ok(vec![]), "once it is back");
    }

    #[tokio::test]
    async fn a_loss_seen_by_a_renewal_nobody_waited_for_is_reported_by_the_next() {
        let leases = alone();
        let epoch = leases.acquire("k").await.unwrap();

        let mut abandoned = leases.renew();
        assert!(abandoned.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        take_over(&leases, "k", epoch);
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(abandoned);

        assert_eq!(leases.renew().await, Ok(vec![("k".into(), OwnershipError::Lost)]));
    }
}
