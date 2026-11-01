//! Actor kinds, remote spawn, and keyed actors.
//!
//! A keyed actor lives on the rendezvous winner among the nodes that advertise its kind and
//! satisfy its placement. The receiving node checks that it is the winner in its own view, but for the nodes the
//! caller names as passed under an ownership authority: those that could not take the lease, or that the caller
//! cannot reach. The lease, not the check, keeps a key on one node.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::task::{Poll, ready};
use std::time::Duration;

use axon::trace::random_u64;
use axon::{
    Actor, ActorBuilder, ActorSystem, Execution, Fence, LifecycleHandle, MailboxConfig, Panic, Phase, RestartPolicy,
    SpawnError, Stream, SystemError,
};
use axon_net::{Identity, Lane};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};

use crate::cluster::{Cluster, Endpoint, Inner, Route, Schema, decode_result, encode_result, refuse_control};
use crate::discovery::{BoxFuture, KINDS, MAX_VALUE};
use crate::engine::{Action, Admit, Entry, Target};
use crate::membership::View;
use crate::placement::{Placement, Strategy, rendezvous};
use crate::wire::*;
use crate::{
    CallError, ClusterError, Holder, KeyedRef, KeyedRefs, MembershipSubscription, OwnershipError, Rejection, RemoteRef,
    Retry, SpawnRefusal, WeakCluster,
};

/// What a kind's factory is told about the actor it is building.
#[derive(Clone, Debug)]
pub struct SpawnContext {
    pub kind: String,
    pub key: Option<String>,
    /// The fencing epoch, when keyed actors are strictly owned. Two starts of a key on one node can share it: a
    /// restart under the kind's policy, or a start while the node still holds the lease.
    pub epoch: Option<u64>,
    /// This start's number in this process of the node, from 1, a restart under the kind's policy too: larger than
    /// every start before it in the process, and from 1 again after the node restarts. Under an authority,
    /// `(epoch, start)` still grows with each start of a key: a restarted node's process takes a new lease, of a
    /// larger epoch.
    pub start: u64,
    pub caller: Identity,
    /// The cluster that runs the factory, for the factory and the actor it builds, so the factory captures none. Weak,
    /// since the cluster holds the factory, and a supervised kind keeps this context to build each restart: a
    /// `Cluster` kept there would keep the cluster alive for good. `upgrade` gives the `Cluster` while it runs, which
    /// the actor may keep, as an actor ends by the time its system has shut down.
    pub cluster: WeakCluster,
}

pub(crate) struct Spawned {
    /// One per schema the kind serves, the kind's own first.
    endpoints: Vec<Endpoint>,
    lifecycle: LifecycleHandle,
}

/// A runtime at `max_actors` is full, not broken: the start may work later.
fn start_refused(e: SystemError) -> (SpawnRefusal, String) {
    let refusal = match &e {
        SystemError::Spawn(SpawnError::ActorLimit) => SpawnRefusal::Capacity,
        // A system shutting down refuses every start before its node is marked leaving.
        SystemError::Closed | SystemError::Spawn(SpawnError::RuntimeStopped) => SpawnRefusal::Leaving,
        // A keyed actor's `kind/key`, held by the key's last actor on its way out. Nothing was spawned. No detail: a
        // `Held` refusal's detail names the holder.
        SystemError::NameTaken(_) => return (SpawnRefusal::Held, String::new()),
        _ => SpawnRefusal::Failed,
    };
    (refusal, e.to_string())
}

type Factory = Box<
    dyn Fn(
            ActorSystem,
            Arc<Inner>,
            SpawnContext,
            Option<Fence>,
            Bytes,
        ) -> BoxFuture<'static, Result<Spawned, (SpawnRefusal, String)>>
        + Send
        + Sync,
>;

pub(crate) struct Kind {
    protocol: u64,
    /// Advertised as `kind/2+1`: `P`'s own first, then what `also` added.
    schemas: Vec<u16>,
    factory: Factory,
    /// The args and placement a key of an eager kind restarts with.
    eager: Option<(Bytes, Placement)>,
    /// Carried to the kind registered again in its place.
    counts: Arc<KindCounts>,
}

/// How a key came to start here, indexing [`HostedKind::started`]: a call asked this node (`get_or_spawn`, and an
/// eager restart or a move that this node asked itself or another node asked), or a node that moved the key or left
/// handed it on.
pub const KEYED_STARTS: [&str; 2] = ["call", "handoff"];
/// Why a key's actor here ended, indexing [`HostedKind::ended`]: it stopped (itself, idle, drained, the system shut
/// down), failed on its own, lost its lease, was stopped while this node was isolated, or moved to another node.
pub const KEYED_ENDS: [&str; 5] = ["stopped", "failed", "lease_lost", "isolated", "moved"];

/// A kind's keys here, kept where `unique` changes.
#[derive(Default)]
pub(crate) struct KindCounts {
    keys: AtomicUsize,
    started: [AtomicU64; KEYED_STARTS.len()],
    ended: [AtomicU64; KEYED_ENDS.len()],
}

/// A lease held here, and the fence of the actor it covers: a process paused past `until` wakes to an actor
/// that stops before handling what queued meanwhile, without waiting for the renew loop to be polled.
pub(crate) struct Lease {
    epoch: u64,
    pub fence: Fence,
    /// `lease_grace` after the last acquire or renew that held it was sent.
    until: Instant,
    /// When the acquire answered: a renew sent before then did not cover the key.
    granted: Instant,
}

#[derive(Clone)]
pub(crate) struct Unique {
    endpoints: Arc<[Endpoint]>,
    epoch: Option<u64>,
    lifecycle: LifecycleHandle,
    args: Bytes,
    placement: Placement,
    /// A leave or a move hands the key on once its actor ends, and releases the lease itself after the new owner took
    /// it on: the exit watcher leaves both alone.
    handing: bool,
    counts: Arc<KindCounts>,
}

impl Unique {
    /// The actor's first endpoint, which names it here.
    fn endpoint(&self) -> u64 {
        self.endpoints[0].0
    }

    fn live(&self) -> bool {
        matches!(self.lifecycle.phase(), Phase::Starting | Phase::Running)
    }
}

#[derive(Clone)]
pub(crate) struct SpawnRequest {
    kind: String,
    key: Option<String>,
    placement: Placement,
    /// Nodes ahead of the one asked that the caller found unable to take the key or cannot reach, or all of them when
    /// it asks the holder: a keyed start is taken by the first node of the order without them, when keys are strictly
    /// owned.
    passed: Vec<String>,
    args: Bytes,
}

struct Placed {
    route: Route,
    epoch: Option<u64>,
    /// This request started the actor, rather than found it running.
    started: bool,
}

impl Placed {
    /// `epoch u64 | started u8 | route`
    fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(17 + 11 * self.route.endpoints.len());
        out.put_u64(self.epoch.unwrap_or(0));
        out.put_u8(self.started as u8);
        Route::put(&mut out, self.route.protocol, self.route.endpoints.iter().copied());
        out.freeze()
    }

    fn decode(incarnation: u128, mut body: Bytes) -> Option<Self> {
        let (epoch, started) = (body.try_get_u64().ok()?, get_bool(&mut body)?);
        Some(Self { route: Route::decode(incarnation, body)?, epoch: (epoch != 0).then_some(epoch), started })
    }
}

type Refused = (SpawnRefusal, String);

/// A hand-off of `.1` under way here, counted in `Inner::taking` while it lives.
struct Taking<'a>(&'a Mutex<HashMap<(String, String), usize>>, (String, String));

impl<'a> Taking<'a> {
    fn new(taking: &'a Mutex<HashMap<(String, String), usize>>, slot: &(String, String)) -> Self {
        *taking.lock().entry(slot.clone()).or_default() += 1;
        Self(taking, slot.clone())
    }
}

impl Drop for Taking<'_> {
    fn drop(&mut self) {
        let mut taking = self.0.lock();
        if let Some(n) = taking.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                taking.remove(&self.1);
            }
        }
    }
}

/// Between attempts at a lease another node still holds, and between looks at a key's actor that is still ending,
/// up to `ClusterConfig::operation_timeout`: a key whose lease runs out moves up to this much later. It also paces an
/// eager kind's restarts of the keys a gone node still holds, and a `KeyWatch` asking again after a refused question.
/// `get_or_spawn` asks again this often while the authority names a holder it cannot reach.
pub const HELD_RETRY: Duration = Duration::from_millis(200);
/// How many nodes of the order one round of `get_or_spawn` asks, past those that cannot take the key.
const FALL_THROUGH: usize = 3;
/// With the authority's `hold`, how long an eager key due here is tried from its first try, and how long the sweep of
/// lapsed leases asks again after its first answer: past a gone node's lease, and an authority that is still coming
/// up. `ConsulLeases` frees a key within twice its `ttl` and then `lock_delay`, which must stay inside it.
/// `PaxosLeases` acceptors back from a restart wait out a lease first, and list what lapsed for
/// `paxos::core::LISTED` (60 s) after.
const EAGER_PATIENCE: Duration = Duration::from_secs(60);
/// How often the armed sweep asks again for lapsed leases: twice at least within `LISTED`.
const LAPSED_EVERY: Duration = Duration::from_secs(20);
/// How long a question about a gone node is asked before it ends unanswered and arms the sweep instead. Short of
/// `LISTED` past the node's lease, so the sweep still finds the leases the question would have.
const ASK_PATIENCE: Duration = Duration::from_secs(30);

/// Keys of eager kinds due to start again here, by lease name, and whether a drain is out.
#[derive(Default)]
pub(crate) struct Waiting {
    pub keys: BTreeMap<String, Due>,
    draining: bool,
    /// Bumped by each `due`.
    seq: u64,
}

pub(crate) struct Due {
    /// The node that held it, or `a lapsed lease`: the event's `for ..`. The first listing's.
    why: Arc<str>,
    /// Listed as a node's, or ended here: a `Held` naming a holder this node cannot reach waits for its lease to end.
    listed: bool,
    /// Given up after, from the first try since it was last listed.
    until: Option<Instant>,
    /// The `seq` of its last listing: one listed again while it is placed stays due.
    seq: u64,
}

/// Questions for the eager asker: about each node, until its deadline, and whether to arm the sweep of lapsed leases.
#[derive(Default)]
pub(crate) struct Asking {
    pub nodes: BTreeMap<String, Instant>,
    arm: bool,
}

/// Whether `place_keyed` follows a `Held` refusal to the holder, or returns it.
#[derive(Clone, Copy, PartialEq)]
enum Follow {
    Held,
    No,
}

/// A key's lease name, and its actor's name: `kind/key`.
fn lease_of(kind: &str, key: &str) -> String {
    format!("{kind}/{key}")
}

/// The kind and key of a lease name.
fn slot_of(name: &str) -> Option<(&str, &str)> {
    name.split_once('/')
}

/// `, epoch n` after an event's detail, when there is one.
fn epoch_suffix(epoch: Option<u64>) -> String {
    epoch.map_or_else(String::new, |e| format!(", epoch {e}"))
}

impl SpawnRequest {
    fn new(kind: &str, key: Option<String>, placement: Placement, args: Bytes) -> Self {
        Self { kind: kind.to_owned(), key, placement, passed: Vec::new(), args }
    }

    fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(64 + self.args.len());
        out.put_u8(self.key.is_some() as u8);
        put_str(&mut out, &self.kind);
        put_str(&mut out, self.key.as_deref().unwrap_or(""));
        put_map(&mut out, &self.placement.required);
        put_map(&mut out, &self.placement.preferred);
        out.put_u16(self.passed.len() as u16);
        self.passed.iter().for_each(|node| put_str(&mut out, node));
        out.put_slice(&self.args);
        out.freeze()
    }

    fn decode(mut body: Bytes) -> Option<Self> {
        let keyed = body.try_get_u8().ok()? == 1;
        let (kind, key) = (get_str(&mut body)?, get_str(&mut body)?);
        let placement =
            Placement { required: get_map(&mut body)?, preferred: get_map(&mut body)?, ..Default::default() };
        let passed = (0..body.try_get_u16().ok()?).map(|_| get_str(&mut body)).collect::<Option<_>>()?;
        // A keyed actor keeps its args for life, and `body` is a slice of the transport's read buffer.
        Some(Self { kind, key: keyed.then_some(key), placement, passed, args: Bytes::copy_from_slice(&body) })
    }
}

impl Inner {
    pub(crate) fn register_system_endpoints(self: &Arc<Self>) -> Result<(), ClusterError> {
        // A hand-off is answered as soon as its start has a place here, with nothing: its acquire then waits for the
        // node handing the key on, which releases it once it hears.
        let spawn_at = |hand_off: bool| -> Admit {
            let (weak, tokio) = (Arc::downgrade(self), self.engine.tokio.clone());
            Arc::new(move |peer, body, reply| {
                let inner = weak.upgrade().ok_or(Rejection::Stopped)?;
                let reply = reply.ok_or(Rejection::InvalidMessage)?;
                let request = SpawnRequest::decode(body).ok_or(Rejection::InvalidMessage)?;
                if !inner.engine.authorize(peer, Action::Spawn { kind: &request.kind }) {
                    return Err(Rejection::Forbidden);
                }
                let caller = peer.identity.clone();
                let name = request.key.as_ref().filter(|_| hand_off).map(|key| lease_of(&request.kind, key));
                tokio.spawn(async move {
                    let (checked, under_way) = tokio::sync::oneshot::channel();
                    let mut start = std::pin::pin!(inner.spawn_local(request, caller, hand_off.then_some(checked)));
                    let (placed, early) = tokio::select! {
                        Ok(()) = under_way => (Ok(Bytes::new()), true),
                        started = &mut start => (started.map(|placed| placed.encode()), false),
                    };
                    let _ = reply.send(encode_result(placed.map_err(|(refusal, detail)| (refusal as u8, detail))));
                    // Taken on, and let go by the node that handed it on: if the start fails, it is this node's to
                    // start again.
                    if early && start.await.is_err() {
                        inner.restart_eager(name);
                    }
                });
                Ok(None)
            })
        };
        let weak = Arc::downgrade(self);
        let load: Admit = Arc::new(move |_, _, reply| {
            let system = weak.upgrade().and_then(|inner| inner.system.upgrade()).ok_or(Rejection::Stopped)?;
            let actors = system.diagnostics_page(None, 1).map_or(0, |page| page.total) as u64;
            let _ = reply.ok_or(Rejection::InvalidMessage)?.send(Bytes::copy_from_slice(&actors.to_be_bytes()));
            Ok(None)
        });
        let weak = Arc::downgrade(self);
        let listed: Admit = Arc::new(move |peer, _, reply| {
            let inner = weak.upgrade().ok_or(Rejection::Stopped)?;
            let view = inner.engine.membership.view();
            let caller = &peer.identity;
            let listed = view.get(&caller.node_id).is_some_and(|m| m.record.incarnation == caller.incarnation);
            let _ = reply.ok_or(Rejection::InvalidMessage)?.send(Bytes::copy_from_slice(&[listed as u8]));
            Ok(None)
        });
        let (weak, tokio) = (Arc::downgrade(self), self.engine.tokio.clone());
        let watch: Admit = Arc::new(move |peer, mut body, reply| {
            let inner = weak.upgrade().ok_or(Rejection::Stopped)?;
            let reply = reply.ok_or(Rejection::InvalidMessage)?;
            let (Ok(seen), Ok(hold)) = (body.try_get_u64(), body.try_get_u32()) else {
                return Err(Rejection::InvalidMessage);
            };
            let slot = get_str(&mut body).zip(get_str(&mut body)).ok_or(Rejection::InvalidMessage)?;
            if !inner.engine.authorize(peer, Action::Watch { kind: &slot.0 }) {
                return Err(Rejection::Forbidden);
            }
            let hold = Duration::from_millis(hold.into()).min(inner.engine.cfg.request_timeout);
            tokio.spawn(async move {
                let (endpoint, epoch) = inner.watch_slot(&slot, seen, hold).await;
                let mut out = BytesMut::with_capacity(16);
                out.put_u64(endpoint);
                out.put_u64(epoch.unwrap_or(0));
                let _ = reply.send(out.freeze());
            });
            Ok(None)
        });
        let (weak, tokio) = (Arc::downgrade(self), self.engine.tokio.clone());
        let owns: Admit = Arc::new(move |_, _, reply| {
            let inner = weak.upgrade().ok_or(Rejection::Stopped)?;
            let reply = reply.ok_or(Rejection::InvalidMessage)?;
            tokio.spawn(async move {
                // A no looks again once: the first look asks again an acceptor link that failed long ago, and a
                // healthy one has answered by then.
                let mut takes = inner.takes_keys();
                if !takes {
                    sleep(HELD_RETRY).await;
                    takes = inner.takes_keys();
                }
                let _ = reply.send(Bytes::copy_from_slice(&[takes as u8]));
            });
            Ok(None)
        });
        let system = [
            (SPAWN, "axon.spawn", spawn_at(false)),
            (HANDOFF, "axon.handoff", spawn_at(true)),
            (LOAD, "axon.load", load),
            (LISTED, "axon.listed", listed),
            (WATCH, "axon.watch", watch),
            (OWNS, "axon.owns", owns),
        ];
        for (id, name, admit) in system {
            let entry = Entry { name: name.into(), protocol: 0, schema: SYSTEM_SCHEMA, lane: Lane::Control, admit };
            self.engine.register(Some(id), false, entry)?;
        }
        Ok(())
    }

    fn gate(&self, slot: &(String, String)) -> &tokio::sync::Mutex<()> {
        let mut hasher = std::hash::DefaultHasher::new();
        slot.hash(&mut hasher);
        &self.gates[hasher.finish() as usize % self.gates.len()]
    }

    /// Stops every keyed actor here, and makes any start still in flight give up. Returns their keys.
    fn sweep(&self) -> Vec<String> {
        let unique = self.unique.lock();
        self.sweeps.fetch_add(1, Relaxed);
        unique.values().for_each(|u| u.lifecycle.stop());
        unique.keys().map(|(kind, key)| lease_of(kind, key)).collect()
    }

    /// Starts again, without a caller, the keys of eager kinds among `names` that ended here, or were taken on here and
    /// did not start, while this node stays a member: no survivor would, since the acceptors may list them as nobody's.
    fn restart_eager(self: &Arc<Self>, names: impl IntoIterator<Item = String>) {
        self.due(names, self.local_node(), true);
    }

    /// Makes the keys of eager kinds among `names` due here, once each whatever listed them, and starts a drain unless
    /// one is out. While this node is isolated they wait for it to be heard. A leave waits for the drain as for its
    /// hand-offs.
    fn due(self: &Arc<Self>, names: impl IntoIterator<Item = String>, why: &str, listed: bool) {
        let kinds = self.kinds.read();
        let eager = |name: &String| {
            let kind = slot_of(name).map(|(kind, _)| kind);
            kind.is_some_and(|kind| kinds.get(kind).is_some_and(|k| k.eager.is_some()))
        };
        let names: Vec<_> = names.into_iter().filter(eager).collect();
        drop(kinds);
        let mut waiting = self.eager.lock();
        for name in names {
            waiting.seq += 1;
            let seq = waiting.seq;
            let due = waiting.keys.entry(name).or_insert_with(|| Due { why: why.into(), listed, until: None, seq });
            due.listed |= listed;
            due.until = None;
            due.seq = seq;
        }
        let isolated = self.isolated.load(Relaxed) && self.engine.open.load(Relaxed);
        if waiting.keys.is_empty() || waiting.draining || isolated {
            return;
        }
        waiting.draining = true;
        drop(waiting);
        let mut handing = self.handoff.lock();
        while handing.try_join_next().is_some() {}
        handing.spawn(self.clone().drain());
    }

    /// Discovery heard this node within the grace, now if `heard`, or a peer vouched for it. Isolated, it rejoins once
    /// that has held for a whole `heartbeat_grace`, with no gap that failed the check again: a node heard less often
    /// than the grace stays isolated, and one heard more often would not isolate again. Then what it stopped meanwhile
    /// starts again.
    pub(crate) fn rejoined(self: &Arc<Self>, heard: bool) {
        if self.isolated.load(Relaxed) && self.joined.load(Relaxed) {
            let now = Instant::now();
            if now - *self.heard_since.lock().get_or_insert(now) < self.heartbeat_grace {
                return;
            }
        }
        self.set_isolated(false);
        if heard {
            self.joined.store(true, Relaxed);
        }
        self.restart_eager([]);
    }

    /// The node this one's view places the key on.
    fn owner(&self, kind: &str, key: &str, placement: &Placement) -> Option<Arc<str>> {
        let view = self.engine.membership.view();
        view.owner(kind, key, placement).map(Arc::from)
    }

    fn owns(&self, view: &View, kind: &str, key: &str, placement: &Placement) -> bool {
        view.owner(kind, key, placement) == Some(self.local_node())
    }

    /// Whether this node would take a key moved to it: open, a member, and able to take the lease.
    fn takes_keys(&self) -> bool {
        self.engine.open.load(Relaxed)
            && !self.isolated.load(Relaxed)
            && self.ownership.as_ref().is_none_or(|a| a.can_grant())
    }

    /// `request` on `node`, which must serve the caller's `schema`.
    async fn place_at(
        self: &Arc<Self>,
        view: &View,
        node: Arc<str>,
        request: &SpawnRequest,
        schema: Option<u16>,
    ) -> Result<(Arc<str>, Placed), ClusterError> {
        serves(view, &node, &request.kind, schema)?;
        self.request_spawn(node.clone(), request).await.map(|placed| (node, placed))
    }

    /// Starts or finds a keyed actor, asking the nodes of this node's view in their order. One that is cut off
    /// (`Isolated`), is leaving, or cannot be reached is passed, and the next is asked, naming those passed; one that
    /// names the holder sends the caller there. Any other refusal, such as acceptors still warming up, ends the walk
    /// with the first node's answer: the key would only start away from its owner. A holder that cannot be reached is
    /// waited for, asking again every `HELD_RETRY`, until `operation_timeout`; with `Follow::No` a `Held` refusal is
    /// returned at once. Without an authority only the owner is asked. `schema`: the caller's, which every node asked
    /// must serve.
    async fn place_keyed(
        self: &Arc<Self>,
        mut request: SpawnRequest,
        schema: Option<u16>,
        follow: Follow,
    ) -> Result<(Arc<str>, Placed), ClusterError> {
        let deadline = Instant::now() + self.operation_timeout;
        'round: loop {
            let view = self.engine.membership.view();
            let next = |r: &SpawnRequest| {
                let key = r.key.as_deref().expect("a keyed request");
                view.owner_past(&r.kind, key, &r.placement, &r.passed).map(Arc::<str>::from)
            };
            request.passed.clear();
            let mut node = next(&request).ok_or(ClusterError::NoCandidate)?;
            if self.ownership.is_none() {
                return self.place_at(&view, node, &request, schema).await;
            }
            let key = request.key.as_deref().expect("a keyed request");
            request.passed = view.unavailable_ahead(&request.kind, key, &node);
            let mut first = None;
            for _ in 0..FALL_THROUGH {
                let error = match self.place_at(&view, node.clone(), &request, schema).await {
                    Ok(placed) => return Ok(placed),
                    Err(error) => error,
                };
                if let ClusterError::Spawn(SpawnRefusal::Held, holder) = &error {
                    if follow == Follow::No {
                        return Err(error);
                    }
                    // Named by the node asked: the holder runs the key, or takes it from its own hold.
                    if *holder != *node && view.get(holder).is_some_and(|m| m.reachable) {
                        // Every node ahead of it, so that a holder still in its hold takes the key.
                        while let Some(ahead) = next(&request).filter(|n| **n != **holder) {
                            request.passed.push(ahead.to_string());
                        }
                        if let Ok(placed) = self.place_at(&view, holder.as_str().into(), &request, schema).await {
                            return Ok(placed);
                        }
                    }
                    if Instant::now() + HELD_RETRY >= deadline {
                        return Err(error);
                    }
                    sleep(HELD_RETRY).await;
                    continue 'round;
                }
                if !walks_past(&error) {
                    return Err(first.unwrap_or(error));
                }
                request.passed.push(node.to_string());
                first.get_or_insert(error);
                // One budget per call: a node that timed out may have spent it already.
                if Instant::now() >= deadline {
                    break;
                }
                match next(&request) {
                    Some(after) => node = after,
                    None => break,
                }
            }
            return Err(first.expect("a node was asked"));
        }
    }

    /// `checked` hears once the start has a place here and goes on to the lease.
    async fn spawn_local(
        self: &Arc<Self>,
        request: SpawnRequest,
        caller: Identity,
        checked: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<Placed, Refused> {
        let refuse = |refusal: SpawnRefusal| (refusal, String::new());
        if !self.engine.open.load(Relaxed) {
            return Err(refuse(SpawnRefusal::Leaving));
        }
        let kind = self.kinds.read().get(&request.kind).cloned().ok_or(refuse(SpawnRefusal::UnknownKind))?;
        let system = self.system.upgrade().ok_or(refuse(SpawnRefusal::Leaving))?;
        let incarnation = self.engine.local.identity.incarnation;
        let route = |endpoints| Route { incarnation, protocol: kind.protocol, endpoints };
        let placed = |u: &Unique| Placed { route: route(u.endpoints.clone()), epoch: u.epoch, started: false };

        let slot = request.key.clone().map(|key| (request.kind.clone(), key));
        let _taking = slot.as_ref().filter(|_| checked.is_some()).map(|slot| Taking::new(&self.taking, slot));
        let mut gate = match &slot {
            Some(slot) => Some(self.gate(slot).lock().await),
            None => None,
        };
        let sweeps = self.sweeps.load(Relaxed);
        // One budget for waiting out an ending actor and a lease held elsewhere.
        let deadline = Instant::now() + self.operation_timeout;
        if let Some(slot) = &slot {
            // A key still running here answers below; a new one would start where the cluster may not see it. At once,
            // so that the caller asks the next node: a lease this node cannot take would keep the key down too.
            let isolated = || Err((SpawnRefusal::Isolated, "this node cannot confirm it is still a member".into()));
            // An actor on its way out refuses what it is sent: wait for it to end, or for a supervised
            // one's next generation, then start the key again.
            // ponytail: a supervised generation's end wakes nothing here, so that case is seen up to HELD_RETRY late.
            loop {
                let ending = match self.unique.lock().get(slot) {
                    Some(existing) if existing.live() => return Ok(placed(existing)),
                    // Being handed to its owner: a caller sent here past that owner, as to the holder, would start it
                    // here again and undo the move. It asks again; only this node's own respawn names nobody passed.
                    Some(existing) if existing.handing && !request.passed.is_empty() => {
                        return Err(refuse(SpawnRefusal::Held));
                    }
                    Some(existing) if existing.lifecycle.exit().is_none() => existing.lifecycle.clone(),
                    _ => break,
                };
                if self.isolated.load(Relaxed) {
                    return isolated();
                }
                if Instant::now() >= deadline {
                    return Err(refuse(SpawnRefusal::Held));
                }
                drop(gate.take());
                let _ = timeout_at((Instant::now() + HELD_RETRY).min(deadline), ending.join()).await;
                gate = Some(self.gate(slot).lock().await);
            }
            if self.isolated.load(Relaxed) {
                return isolated();
            }
            if self.ownership.as_ref().is_some_and(|a| !a.can_grant()) {
                return Err((SpawnRefusal::Isolated, "this node reaches no majority of the authority".into()));
            }
            // The caller may simply be ahead of this node's view, or have seen the owner this node picks crash, which
            // a node that never called it cannot see: look again, and try that owner, before refusing.
            // With an authority, the first of the order the caller did not pass. Without one, the owner: nothing would
            // keep a key started past it on one node.
            let passed = if self.ownership.is_some() { &request.passed[..] } else { &[] };
            let placed = || {
                let view = self.engine.membership.view();
                view.owner_past(&request.kind, &slot.1, &request.placement, passed).map(Arc::<str>::from)
            };
            let here = || placed().as_deref() == Some(self.local_node());
            if !here() {
                self.refresh().await;
                if let Some(owner) = placed().filter(|o| **o != *self.local_node()) {
                    // A dial that fails marks the owner unreachable, and the view moves on without it.
                    let _ = timeout_at(deadline, self.engine.incarnation_of(&owner)).await;
                }
                if !here() {
                    return Err(refuse(SpawnRefusal::NotOwner));
                }
            }
        }
        let reserve = |n: usize| (n < self.max_spawned).then_some(n + 1);
        self.spawned.try_update(Relaxed, Relaxed, reserve).map_err(|_| refuse(SpawnRefusal::Capacity))?;
        let handed = checked.is_some();
        if let Some(checked) = checked {
            let _ = checked.send(());
        }

        let lease = slot.as_ref().map(|(kind, key)| lease_of(kind, key));
        let built = async {
            let epoch = match (&self.ownership, &slot, &lease) {
                (Some(authority), Some(slot), Some(lease)) => {
                    // A holder that died without leaving keeps the lease for the authority's lock delay.
                    let acquired = loop {
                        let asked = Instant::now();
                        let acquired = match timeout_at(deadline, authority.acquire(lease)).await {
                            Ok(acquired) => acquired
                                .and_then(|epoch| self.hold(lease, epoch, asked, system.clock()).map(|f| (epoch, f))),
                            Err(_) => Err(OwnershipError::Unavailable("timed out".into())),
                        };
                        match acquired {
                            // Another node's: its caller asks it, or the next node, rather than wait here. A
                            // hand-off waits for the node handing the key on, and so does a start beside one: asked,
                            // that node would start the key again once it has let go. An earlier process of this
                            // node's lease runs out.
                            Err(OwnershipError::Held(Some(holder)))
                                if !handed && holder != self.local_node() && !self.taking.lock().contains_key(slot) =>
                            {
                                break Err(OwnershipError::Held(Some(holder)));
                            }
                            Err(OwnershipError::Held(_)) if Instant::now() + HELD_RETRY < deadline => {
                                // Others on this gate, the exit watcher that would release above all, must
                                // not wait out the lock delay with us. Someone may have started the key meanwhile.
                                gate = None;
                                sleep(HELD_RETRY).await;
                                gate = Some(self.gate(slot).lock().await);
                                if let Some(existing) = self.unique.lock().get(slot).filter(|u| u.live()) {
                                    return Err(Ok(placed(existing)));
                                }
                                // Left meanwhile: its hand-off missed this start, and a grant now would outlive
                                // its session with the authority.
                                if !self.engine.open.load(Relaxed) {
                                    return Err(Err(refuse(SpawnRefusal::Leaving)));
                                }
                            }
                            acquired => break acquired,
                        }
                    };
                    Some(acquired.map_err(|e| match e {
                        OwnershipError::Held(holder) => Err((SpawnRefusal::Held, holder.unwrap_or_default())),
                        e => Err((SpawnRefusal::Ownership, e.to_string())),
                    })?)
                }
                _ => None,
            };
            let (epoch, fence) = epoch.unzip();
            let cluster = WeakCluster(Arc::downgrade(self));
            let context = SpawnContext {
                kind: request.kind.clone(),
                key: request.key.clone(),
                epoch,
                start: self.starts.fetch_add(1, Relaxed) + 1,
                caller,
                cluster,
            };
            match (kind.factory)(system, self.clone(), context, fence.clone(), request.args.clone()).await {
                Ok(spawned) => Ok((spawned, epoch, fence)),
                Err((refusal, detail)) => {
                    self.release(lease.as_deref(), epoch).await;
                    // Refused because this node is leaving: the key's next owner starts it.
                    let refusal = if self.engine.open.load(Relaxed) { refusal } else { SpawnRefusal::Leaving };
                    Err(Err((refusal, detail)))
                }
            }
        };
        // The inner `Err(Ok(placed))` is a key someone else started while the lease was waited for.
        let (spawned, epoch, fence) = match built.await {
            Ok(built) => built,
            Err(placed) => {
                self.spawned.fetch_sub(1, Relaxed);
                return placed;
            }
        };

        // Every keyed actor, or this one's lease, may have been given up while it started, or the node isolated, which
        // under an authority sweeps nothing: it would have been missed.
        let endpoints: Arc<[Endpoint]> = spawned.endpoints.into();
        let swept = slot.as_ref().is_some_and(|slot| {
            let mut unique = self.unique.lock();
            let lapsed = fence.as_ref().is_some_and(|f| !f.is_open());
            let swept = lapsed
                || self.sweeps.load(Relaxed) != sweeps
                || self.isolated.load(Relaxed)
                || !self.engine.open.load(Relaxed);
            if !swept {
                // Started past the owner, or the view moved the key on while the lease was waited for and the pass
                // that would have moved it found nothing: callers would ask an owner this node's renewals keep out.
                if !self.owns(&self.engine.membership.view(), &slot.0, &slot.1, &request.placement) {
                    self.rebalance.notify_one();
                }
                let entry = Unique {
                    endpoints: endpoints.clone(),
                    epoch,
                    lifecycle: spawned.lifecycle.clone(),
                    args: request.args,
                    placement: request.placement,
                    handing: false,
                    counts: kind.counts.clone(),
                };
                kind.counts.keys.fetch_add(1, Relaxed);
                kind.counts.started[handed as usize].fetch_add(1, Relaxed);
                // An ended actor not yet cleared out.
                if let Some(ended) = unique.insert(slot.clone(), entry) {
                    ended.counts.keys.fetch_sub(1, Relaxed);
                }
                self.wake(slot);
            }
            swept
        });
        if swept {
            spawned.lifecycle.stop();
        } else if let (Some(lease), Some(epoch)) = (lease, epoch) {
            // Only a lease: a key without one starts wherever it is asked for, and each start would crowd the ring.
            self.events.record("acquired", lease, format!("epoch {epoch}"));
        }
        let (inner, endpoint, lifecycle, counts) =
            (self.clone(), endpoints[0].0, spawned.lifecycle, kind.counts.clone());
        tokio::spawn(async move {
            let exit = lifecycle.join().await;
            let lapsed = !swept && fence.is_some_and(|f| !f.is_open());
            let own_fault = exit.reason.is_own_failure();
            if slot.is_some() && own_fault {
                inner.counters.keyed_failed.fetch_add(1, Relaxed);
            }
            if let Some(slot) = slot.as_ref().filter(|_| !swept) {
                // Handed on, or already cleared out by the hand-off: only it vacates the slot before this watcher does.
                let moved = match inner.unique.lock().get(slot) {
                    Some(held) if held.endpoint() == endpoint => held.handing,
                    Some(_) => false,
                    None => true,
                };
                let why = if own_fault {
                    "failed"
                } else if lapsed {
                    "lease_lost"
                } else if inner.sweeps.load(Relaxed) != sweeps {
                    "isolated"
                } else if moved {
                    "moved"
                } else {
                    "stopped"
                };
                let why = KEYED_ENDS.iter().position(|w| *w == why).expect("a name in KEYED_ENDS");
                counts.ended[why].fetch_add(1, Relaxed);
            }
            inner.spawned.fetch_sub(1, Relaxed);
            if let Some(slot) = slot
                && inner.vacate(&slot, endpoint, epoch, !swept, false).await
                && lapsed
            {
                // Its lease ran out or was lost, whether its own turn or the renew loop saw it first.
                inner.restart_eager([lease_of(&slot.0, &slot.1)]);
            }
        });
        if swept {
            // Cut off meanwhile: `Isolated`, so that the caller asks the next node.
            let refusal = if self.isolated.load(Relaxed) { SpawnRefusal::Isolated } else { SpawnRefusal::Ownership };
            return Err((refusal, "this node stopped its keyed actors during the start".into()));
        }
        Ok(Placed { route: route(endpoints), epoch, started: true })
    }

    /// Frees the slot and the lease of a keyed actor that has ended, once: a move and the exit watcher both call it.
    /// `listed` says whether the actor ever held the slot, `handed` that the hand-off that marked it calls. Returns
    /// whether it released the lease.
    async fn vacate(
        &self,
        slot: &(String, String),
        endpoint: u64,
        epoch: Option<u64>,
        listed: bool,
        handed: bool,
    ) -> bool {
        let _gate = self.gate(slot).lock().await;
        let release = {
            let mut unique = self.unique.lock();
            match unique.get(slot) {
                None => !listed,
                // A newer actor in the slot re-acquired the same lock; releasing would unlock its key. Or the key is
                // being handed on, which releases it once the new owner has taken it on.
                Some(held) if held.endpoint() != endpoint || (held.handing && !handed) => false,
                Some(_) => {
                    if let Some(ended) = unique.remove(slot) {
                        ended.counts.keys.fetch_sub(1, Relaxed);
                    }
                    self.wake(slot);
                    true
                }
            }
        };
        if release {
            self.release(Some(&lease_of(&slot.0, &slot.1)), epoch).await;
        }
        release
    }

    /// Wakes the watches held here on `slot`.
    fn wake(&self, slot: &(String, String)) {
        if let Some(watches) = self.watches.lock().get(slot) {
            watches.notify_waiters();
        }
    }

    /// The endpoint (0 for none) and epoch of the actor this node runs for `slot`, once its endpoint is not `seen`,
    /// or when `hold` has passed. An actor on its way out, its lease lost or its key moving, runs nothing for it.
    async fn watch_slot(&self, slot: &(String, String), seen: u64, hold: Duration) -> (u64, Option<u64>) {
        let deadline = Instant::now() + hold;
        let wake = self.watches.lock().entry(slot.clone()).or_default().clone();
        let lease = lease_of(&slot.0, &slot.1);
        let now = loop {
            let mut changed = std::pin::pin!(wake.notified());
            changed.as_mut().enable();
            // The slot before the lease: `lose` gives the lease up before it stops the actor.
            let now = self.unique.lock().get(slot).filter(|u| u.live()).map(|u| (u.endpoint(), u.epoch));
            // Past its deadline a lease is lost, whether or not a late renew loop has taken it out yet.
            let held = self.leases.lock().get(&lease).filter(|l| l.until > Instant::now()).map(|l| l.epoch);
            let now = now.filter(|(_, epoch)| epoch.is_none() || *epoch == held).unwrap_or((0, None));
            if now.0 != seen || timeout_at(deadline, changed).await.is_err() {
                break now;
            }
        };
        // The map and this watch hold it, and no other: it is the key's last watch here.
        let mut watches = self.watches.lock();
        if Arc::strong_count(&wake) == 2 {
            watches.remove(slot);
        }
        now
    }

    /// The fence of `lease` at `epoch`, which the authority just granted to an acquire sent at `asked`.
    fn hold(&self, lease: &str, epoch: u64, asked: Instant, clock: &axon::Clock) -> Result<Fence, OwnershipError> {
        // Both at once, as `lose` changes them: between two looks it can take the entry out and note it lost after.
        let (mut leases, lost) = (self.leases.lock(), self.lost.lock());
        // Given up here and not yet released: the authority answered from the hold that lapsed.
        if lost.get(lease) == Some(&epoch) {
            return Err(OwnershipError::Held(None));
        }
        // Taken again before it was released: the answer came from the hold the renewals extend.
        if let Some(held) = leases.get(lease).filter(|held| held.epoch == epoch) {
            return Ok(held.fence.clone());
        }
        let (until, granted) = (asked + self.lease_grace, Instant::now());
        let fence = Fence::new(clock, until.saturating_duration_since(granted));
        leases.insert(lease.to_owned(), Lease { epoch, fence: fence.clone(), until, granted });
        Ok(fence)
    }

    async fn release(&self, lease: Option<&str>, epoch: Option<u64>) {
        if let (Some(authority), Some(lease), Some(epoch)) = (&self.ownership, lease, epoch) {
            // Taken while the map still lists the lease, so `release_all` finds it in one or the other, whenever a
            // caller cut short (as a leave out of time cuts its hand-offs) stops here.
            let under_way = self.releasing.clone().read_owned().await;
            {
                let (mut leases, mut lost) = (self.leases.lock(), self.lost.lock());
                if leases.get(lease).is_some_and(|l| l.epoch == epoch) {
                    leases.remove(lease);
                }
                if lost.get(lease) == Some(&epoch) {
                    lost.remove(lease);
                }
            }
            // In a task of its own, which such a cut does not reach. The key's next owner waits for it.
            let (authority, lease, patience) = (authority.clone(), lease.to_owned(), self.operation_timeout);
            let release = async move {
                let _ = timeout(patience, authority.release(&lease, epoch)).await;
                drop(under_way);
            };
            let _ = tokio::spawn(release).await;
        }
    }

    async fn request_spawn(self: &Arc<Self>, node: Arc<str>, request: &SpawnRequest) -> Result<Placed, ClusterError> {
        if *node != *self.local_node() {
            let (incarnation, body) = self.call_spawn(node, SPAWN, request).await?;
            let placed = Placed::decode(incarnation, body);
            return placed.ok_or_else(|| self.refused(SpawnRefusal::Failed, "malformed spawn reply".into()));
        }
        self.counters.spawn_requests.fetch_add(1, Relaxed);
        // A caller that gives up must not leave a start half done, with its reservation, lease and slot.
        let (inner, caller, request) = (self.clone(), self.engine.local.identity.clone(), request.clone());
        let start = tokio::spawn(async move { inner.spawn_local(request, caller, None).await });
        let placed = start.await.unwrap_or_else(|failed| {
            // The panic's own message: the JoinError's text names a task id that is global to the process.
            let panic = failed.try_into_panic().ok().map(Panic::from);
            let said = panic.as_ref().and_then(Panic::message).unwrap_or("the start panicked or its runtime stopped");
            Err((SpawnRefusal::Failed, said.to_owned()))
        });
        placed.map_err(|(refusal, detail)| self.refused(refusal, detail))
    }

    /// `request` sent to `node`'s `SPAWN` or `HANDOFF`: the incarnation asked and the answer. A hand-off is answered
    /// once its start has a place there, before it waits for the lease.
    async fn call_spawn(
        self: &Arc<Self>,
        node: Arc<str>,
        endpoint: u64,
        request: &SpawnRequest,
    ) -> Result<(u128, Bytes), ClusterError> {
        self.counters.spawn_requests.fetch_add(1, Relaxed);
        let (incarnation, answer) = self.engine.ask(node, endpoint, request.encode()).await?;
        let body = decode_result(answer.unwrap_or_default());
        Ok((incarnation, body.map_err(|(code, detail)| self.refused(SpawnRefusal::from_code(code), detail))?))
    }

    fn refused(&self, refusal: SpawnRefusal, detail: String) -> ClusterError {
        self.counters.spawn_refused.fetch_add(1, Relaxed);
        ClusterError::Spawn(refusal, detail)
    }

    async fn load_of(self: &Arc<Self>, node: &Arc<str>) -> Result<u64, ClusterError> {
        let mut answer = self.engine.ask(node.clone(), LOAD, Bytes::new()).await?.1.unwrap_or_default();
        answer.try_get_u64().map_err(|_| ClusterError::Control("malformed load reply".into()))
    }

    async fn choose(self: &Arc<Self>, kind: &str, placement: &Placement) -> Result<Arc<str>, ClusterError> {
        let view = self.engine.membership.view();
        let nodes: Vec<Arc<str>> =
            placement.candidates(&view, kind).iter().map(|m| m.record.id.as_str().into()).collect();
        if nodes.is_empty() {
            return Err(ClusterError::NoCandidate);
        }
        let pick = match placement.strategy {
            Strategy::Spread => self.spread.fetch_add(1, Relaxed),
            _ => random_u64() as usize,
        } % nodes.len();
        if placement.strategy != Strategy::LeastLoaded || nodes.len() == 1 {
            return Ok(nodes[pick].clone());
        }
        let other = (pick + 1 + random_u64() as usize % (nodes.len() - 1)) % nodes.len();
        let (a, b) = tokio::join!(self.load_of(&nodes[pick]), self.load_of(&nodes[other]));
        match (a, b) {
            (Ok(a), Ok(b)) => Ok(nodes[if b < a { other } else { pick }].clone()),
            (Err(_), Ok(_)) => Ok(nodes[other].clone()),
            (Ok(_), _) => Ok(nodes[pick].clone()),
            (Err(error), Err(_)) => Err(error),
        }
    }

    /// Asks the current owner to run a keyed actor that has ended here, and then releases its lease. The lease is held
    /// until the owner has taken the request on, looked up again after each refusal, or `operation_timeout` passed:
    /// should this node crash meanwhile, the key is still listed as its, for an eager kind's survivors to restart. The
    /// owner's start waits out the lease meanwhile, as it would a crashed holder's.
    async fn respawn(self: &Arc<Self>, slot: (String, String), entry: Unique) {
        let (mut until, mut asked) = (None, None);
        loop {
            // Taken before the owner is picked, and given back between tries: a big join queues here, and the view
            // may move on meanwhile.
            let turn = self.respawns.acquire().await.expect("never closed");
            let Some(owner) = self.owner(&slot.0, &slot.1, &entry.placement) else { break };
            let deadline = *until.get_or_insert_with(|| Instant::now() + self.operation_timeout);
            let request = SpawnRequest::new(&slot.0, Some(slot.1.clone()), entry.placement.clone(), entry.args.clone());
            let taken = if *owner != *self.local_node() {
                self.call_spawn(owner.clone(), HANDOFF, &request).await.map(drop)
            } else if self.engine.open.load(Relaxed) {
                // Back here: started from this node's own hold, and the slot's new actor keeps the lease.
                self.request_spawn(owner.clone(), &request).await.map(drop)
            } else {
                // Leaving, with a view that still has this node as the owner: wait for the view to move on.
                Err(ClusterError::Spawn(SpawnRefusal::Leaving, String::new()))
            };
            let again = taken.as_ref().is_err_and(|e| matches!(e.retry(), Retry::Later | Retry::Resolve));
            if !again || Instant::now() + HELD_RETRY >= deadline {
                asked = Some((owner, taken.is_ok()));
                break;
            }
            drop(turn);
            sleep(HELD_RETRY).await;
        }
        let (subject, moved) = (lease_of(&slot.0, &slot.1), asked.as_ref().is_some_and(|(_, moved)| *moved));
        // Before the release, which a leave out of time cuts short: the owner's answer stands either way.
        if let Some((owner, _)) = &asked {
            let (counter, kind) = if moved {
                (&self.counters.handoffs, "handed_off")
            } else {
                (&self.counters.handoffs_failed, "handoff_failed")
            };
            self.events.record(kind, subject.as_str(), format!("to {owner}{}", epoch_suffix(entry.epoch)));
            counter.fetch_add(1, Relaxed);
        }
        self.vacate(&slot, entry.endpoint(), entry.epoch, true, true).await;
        // No node took it: it waits for a caller, or starts again from here if its kind is eager.
        if !moved {
            self.restart_eager([subject]);
        }
    }

    /// A leave hands each key on as soon as its actor has ended, side by side, as a rebalance does: its lease is held
    /// until then, so waiting for the slowest drain would keep the others' keys down. The eager keys still to start
    /// again here go to their owners too.
    pub(crate) fn hand_off(self: &Arc<Self>) {
        {
            let (mut unique, mut handing) = (self.unique.lock(), self.handoff.lock());
            for (slot, entry) in unique.iter_mut() {
                entry.handing = true;
                let (inner, slot, entry) = (self.clone(), slot.clone(), entry.clone());
                handing.spawn(async move {
                    entry.lifecycle.join().await;
                    inner.respawn(slot, entry).await;
                });
            }
        }
        self.restart_eager([]);
    }

    /// Releases every lease still held: those of hand-offs a leave cut short, with their fences shut. Returns once the
    /// releases already under way have ended too, so the authority closes after every one.
    pub(crate) async fn release_all(self: &Arc<Self>) {
        let held: Vec<_> = self.leases.lock().iter().map(|(name, lease)| (name.clone(), lease.epoch)).collect();
        let mut releases = JoinSet::new();
        for (name, epoch) in held {
            let inner = self.clone();
            releases.spawn(async move { inner.release(Some(&name), Some(epoch)).await });
        }
        releases.join_all().await;
        drop(self.releasing.write().await);
    }

    /// How long a key due here is tried from its first try, and the sweep of lapsed leases asks after its first answer.
    fn eager_patience(&self) -> Duration {
        EAGER_PATIENCE + self.ownership.as_ref().and_then(|a| a.hold()).unwrap_or(EAGER_PATIENCE)
    }

    /// Places the keys due here, side by side under `respawns`, a round every `HELD_RETRY` until none is left, each
    /// until `EAGER_PATIENCE` and a lease from its first try. Ends while this node is isolated and open, keeping them
    /// for `rejoined`.
    async fn drain(self: Arc<Self>) {
        /// Clears `draining` when the drain ends some other way than through the map's lock: a panic or an abort.
        struct Out<'a>(&'a Inner, bool);
        impl Drop for Out<'_> {
            fn drop(&mut self) {
                if self.1 {
                    self.0.eager.lock().draining = false;
                }
            }
        }
        let mut out = Out(&self, true);
        let patience = self.eager_patience();
        loop {
            let now = Instant::now();
            let round: Vec<_> = {
                let mut waiting = self.eager.lock();
                waiting.keys.retain(|_, due| due.until.is_none_or(|until| now < until));
                let isolated = self.isolated.load(Relaxed) && self.engine.open.load(Relaxed);
                if waiting.keys.is_empty() || isolated || self.engine.is_closed() {
                    waiting.draining = false;
                    out.1 = false;
                    return;
                }
                let due = waiting.keys.iter_mut();
                due.map(|(name, due)| {
                    due.until.get_or_insert(now + patience);
                    (name.clone(), due.why.clone(), due.listed, due.seq)
                })
                .collect()
            };
            let mut starts = JoinSet::new();
            for (name, why, listed, seq) in round {
                let this = self.clone();
                starts.spawn(async move { (this.restart_due(&name, &why, listed).await, name, seq) });
            }
            let mut done = Vec::new();
            while let Some(placed) = starts.join_next().await {
                // A panic is its key's alone: it stays due.
                done.extend(placed.ok().filter(|(keep, ..)| !keep));
            }
            let ended = {
                let mut waiting = self.eager.lock();
                for (_, name, seq) in done {
                    if waiting.keys.get(&name).is_some_and(|due| due.seq == seq) {
                        waiting.keys.remove(&name);
                    }
                }
                waiting.draining = !waiting.keys.is_empty();
                !waiting.draining
            };
            if ended {
                out.1 = false;
                return;
            }
            sleep(HELD_RETRY).await;
        }
    }

    /// One try at a key due here. Returns whether it stays due: refused for now, or held by a node this one cannot
    /// reach while `listed`. A key another node holds runs there; a lapsed lease someone took again is theirs.
    async fn restart_due(self: &Arc<Self>, name: &str, why: &str, listed: bool) -> bool {
        let Some((kind, key)) = slot_of(name) else { return false };
        let Some((args, placement)) = self.kinds.read().get(kind).and_then(|k| k.eager.clone()) else {
            return false;
        };
        let slot = (kind.to_owned(), key.to_owned());
        if self.unique.lock().get(&slot).is_some_and(|u| u.live() && !u.handing) {
            return false;
        }
        let _turn = self.respawns.acquire().await.expect("never closed");
        let later = |e: &ClusterError| matches!(e.retry(), Retry::Later | Retry::Resolve);
        let leaving = !self.engine.open.load(Relaxed);
        let owner = leaving.then(|| self.owner(kind, key, &placement)).flatten();
        let request = SpawnRequest::new(kind, Some(key.to_owned()), placement, args);
        // Leaving: the owner takes the key on and keeps at its start, which may outlast the leave.
        if let Some(owner) = owner.filter(|o| **o != *self.local_node()) {
            return self.call_spawn(owner, HANDOFF, &request).await.is_err_and(|e| later(&e));
        }
        // `Resolve`: the owner's view differs from this one's, and the next round asks the owner again.
        match self.place_keyed(request, None, Follow::No).await {
            Ok((node, placed)) if placed.started => {
                let epoch = epoch_suffix(placed.epoch);
                self.events.record("eager_restart", name, format!("on {node} for {why}{epoch}"));
                false
            }
            // Found running: started by a caller, or by another node's restart, which recorded it.
            Ok(_) => false,
            Err(ClusterError::Spawn(SpawnRefusal::Held, holder)) => {
                let view = self.engine.membership.view();
                let reachable = holder != self.local_node() && view.get(&holder).is_some_and(|m| m.reachable);
                listed && !reachable
            }
            Err(e) => later(&e),
        }
    }

    /// Posts questions for the eager asker: about each of `nodes`, for `ASK_PATIENCE` from now, and with `arm` the
    /// sweep of lapsed leases. One about this node itself is asked as long as a key is tried, past the acceptors' wait:
    /// the sweep would start only those of its keys this node owns.
    fn ask_about(&self, nodes: impl IntoIterator<Item = String>, arm: bool) {
        let now = Instant::now();
        let within = |node: &str| now + if node == self.local_node() { self.eager_patience() } else { ASK_PATIENCE };
        let mut asking = self.asking.lock();
        asking.nodes.extend(nodes.into_iter().map(|node| {
            let deadline = within(&node);
            (node, deadline)
        }));
        asking.arm |= arm;
        drop(asking);
        self.eager_posted.notify_one();
    }

    /// Makes due the keys of `held` that are this node's to start: every one with `all` (its own, which its view may
    /// not place on it yet), else those it owns in its view.
    fn feed(self: &Arc<Self>, held: Vec<String>, why: &str, listed: bool, all: bool) {
        let (view, kinds) = (self.engine.membership.view(), self.kinds.read());
        let mine = |name: &String| {
            all || slot_of(name).is_some_and(|(kind, key)| {
                let eager = kinds.get(kind).and_then(|k| k.eager.as_ref());
                eager.is_some_and(|(_, placement)| self.owns(&view, kind, key, placement))
            })
        };
        let held: Vec<_> = held.into_iter().filter(mine).collect();
        drop(kinds);
        self.due(held, why, listed);
    }

    /// Asks the authority which keys of the eager kinds are due here, side by side: about each node posted, until it
    /// answers or its deadline passes, which arms the sweep (the node's leases have run out by then); and, while the
    /// sweep is armed, about lapsed leases, until the first answer and then every `LAPSED_EVERY` until `EAGER_PATIENCE`
    /// and a lease past it. Feeds nothing while this node is leaving.
    pub(crate) async fn eager_loop(self: Arc<Self>) {
        let authority = self.ownership.clone().expect("started with an authority");
        let patience = self.eager_patience();
        // While armed: when it is next asked, its first answer since it was armed, and the pause after no answer.
        let mut sweep: Option<(Instant, Option<Instant>, Duration)> = None;
        // Each ask under way, by task: the node asked about, or `None` for the sweep.
        let (mut asks, mut under_way) = (JoinSet::new(), Vec::<(tokio::task::Id, Option<String>)>::new());
        loop {
            // Leaving: nothing more is asked.
            let sweeping = under_way.iter().any(|(_, q)| q.is_none()) || !self.engine.open.load(Relaxed);
            let wake = sweep.filter(|_| !sweeping).map(|(next, ..)| next);
            let woken = async {
                match wake {
                    Some(at) => sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = self.eager_posted.notified() => {}
                () = woken => {}
                Some(done) = asks.join_next_with_id() => {
                    let (id, held): (_, Option<Vec<String>>) = match done {
                        Ok((id, held)) => (id, held),
                        Err(e) => (e.id(), None),
                    };
                    let Some(at) = under_way.iter().position(|(task, _)| *task == id) else { continue };
                    let open = self.engine.open.load(Relaxed);
                    match (under_way.swap_remove(at).1, held) {
                        // Unanswered past its deadline: dropped below, which arms the sweep.
                        (Some(_), None) => {}
                        (Some(node), Some(held)) => {
                            self.asking.lock().nodes.remove(&node);
                            if open {
                                let own = node == self.local_node();
                                self.feed(held, &node, true, own);
                            }
                        }
                        (None, held) => {
                            let now = Instant::now();
                            if let Some((next, first, pause)) = &mut sweep {
                                if let Some(held) = held {
                                    first.get_or_insert(now);
                                    (*next, *pause) = (now + LAPSED_EVERY, HELD_RETRY);
                                    if open {
                                        self.feed(held, "a lapsed lease", false, false);
                                    }
                                } else {
                                    // Not answered yet: an authority that is down or still warming gets asked less
                                    // often.
                                    *next = now + if first.is_some() { LAPSED_EVERY } else { *pause };
                                    *pause = (*pause * 2).min(Duration::from_secs(1));
                                }
                            }
                            if sweep.is_some_and(|(_, first, _)| first.is_some_and(|f| now >= f + patience)) {
                                sweep = None;
                            }
                        }
                    }
                }
            }
            if self.engine.is_closed() {
                return;
            }
            let now = Instant::now();
            let asking = |node: &str| under_way.iter().any(|(_, q)| q.as_deref() == Some(node));
            let ask: Vec<_> = {
                let mut posted = self.asking.lock();
                let mut arm = std::mem::take(&mut posted.arm);
                posted.nodes.retain(|node, deadline| {
                    let keep = now < *deadline || asking(node);
                    arm |= !keep;
                    keep
                });
                if arm {
                    sweep = Some((now, None, HELD_RETRY));
                }
                posted.nodes.iter().filter(|(node, _)| !asking(node)).map(|(n, d)| (n.clone(), *d)).collect()
            };
            if !self.engine.open.load(Relaxed) {
                continue;
            }
            for (node, deadline) in ask {
                let (this, authority, asked) = (self.clone(), authority.clone(), node.clone());
                let task = asks.spawn(async move {
                    let asking = async {
                        let mut pause = HELD_RETRY;
                        loop {
                            let eager = this.eager_kinds();
                            let held = authority.held_by(Holder::Node(&asked), &eager);
                            if let Ok(held) = held.expect("register refuses an eager kind it cannot list").await {
                                return held;
                            }
                            sleep(pause).await;
                            pause = (pause * 2).min(Duration::from_secs(1));
                        }
                    };
                    timeout_at(deadline, asking).await.ok()
                });
                under_way.push((task.id(), Some(node)));
            }
            if sweep.is_some_and(|(next, ..)| next <= now) && !under_way.iter().any(|(_, q)| q.is_none()) {
                let (this, authority) = (self.clone(), authority.clone());
                let task = asks.spawn(async move {
                    let eager = this.eager_kinds();
                    let held = authority.held_by(Holder::Lapsed, &eager);
                    let held = held.expect("register refuses an eager kind it cannot list");
                    timeout(LAPSED_EVERY, held).await.ok()?.ok()
                });
                under_way.push((task.id(), None));
            }
        }
    }

    /// The eager kinds registered here, sorted.
    fn eager_kinds(&self) -> Vec<String> {
        self.kinds.read().iter().filter(|(_, k)| k.eager.is_some()).map(|(n, _)| n.clone()).collect()
    }

    /// Drains keyed actors whose owner is now another node, then restarts them there, once the owner says it would
    /// take them (`takes_keys`): moved to an owner cut off from the authority, a key would stop. A key kept is looked
    /// at again every `operation_timeout`. Asks which keys of an eager kind a node gone from the view held, and, each
    /// time the eager kinds here change, those this node held before it started and those whose lease lapsed
    /// unreleased (`eager_loop`).
    pub(crate) async fn rebalance_loop(self: Arc<Self>) {
        let (mut last, mut eager_seen) = (self.engine.membership.view(), Vec::new());
        let mut kept = false;
        loop {
            if kept {
                let _ = timeout(self.operation_timeout, self.rebalance.notified()).await;
            } else {
                self.rebalance.notified().await;
            }
            sleep(self.rebalance_settle).await;
            let view = self.engine.membership.view();
            if self.engine.is_closed() {
                return;
            }
            // A kept key stays due across a pass skipped here.
            if !self.engine.open.load(Relaxed) || view.stale.is_some() {
                continue;
            }
            kept = false;
            let eager = self.eager_kinds();
            if eager != eager_seen && !eager.is_empty() {
                self.ask_about([self.local_node().to_owned()], true);
            }
            eager_seen = eager;
            if !eager_seen.is_empty() {
                let here = |m: &crate::Member| view.get(&m.record.id).is_some_and(|n| n.reachable);
                let gone = last.members.iter().filter(|m| m.reachable && m.record.id != self.local_node() && !here(m));
                let gone: Vec<_> = gone.map(|m| m.record.id.clone()).collect();
                if !gone.is_empty() {
                    self.ask_about(gone, false);
                }
            }
            last = view.clone();
            // Few placements are shared by many keys, so the candidates of each are worked out once a pass, and
            // the hashing is done without the lock every keyed start on this node needs.
            let mut known: Vec<(String, Placement, Vec<_>)> = Vec::new();
            let mut hosted = Vec::new();
            for (slot, entry) in self.unique.lock().iter() {
                let same = |k: &(String, Placement, Vec<_>)| k.0 == slot.0 && k.1 == entry.placement;
                let at = known.iter().position(same).unwrap_or_else(|| {
                    known.push((slot.0.clone(), entry.placement.clone(), entry.placement.candidates(&view, &slot.0)));
                    known.len() - 1
                });
                hosted.push((slot.clone(), at));
            }
            let mut elsewhere: BTreeMap<&str, Vec<(String, String)>> = BTreeMap::new();
            for (slot, at) in hosted {
                let owner = rendezvous(&slot.0, &slot.1, &known[at].2).map(|m| m.record.id.as_str());
                if let Some(owner) = owner.filter(|o| *o != self.local_node()) {
                    elsewhere.entry(owner).or_default().push(slot);
                }
            }
            // One question to each owner, side by side, and its keys move once it answers yes. The new owner starts a
            // key only once it has ended here: a drain that overruns is cut short, and a handler still running is
            // waited for, or the key would be alive twice. Side by side, so one slow actor or owner holds back only
            // itself.
            let mut asks = JoinSet::new();
            for (owner, slots) in elsewhere {
                let (this, owner) = (self.clone(), Arc::from(owner));
                asks.spawn(async move {
                    // Whether it would take a key moved to it (`takes_keys`): no answer is no.
                    let asked = this.engine.ask(owner, OWNS, Bytes::new()).await;
                    if !matches!(asked, Ok((_, Some(answer))) if answer[..] == [1]) {
                        return false;
                    }
                    let moved: Vec<_> = {
                        let mut unique = this.unique.lock();
                        let hand = |slot| {
                            let entry = unique.get_mut(&slot)?;
                            entry.handing = true;
                            Some((slot, entry.clone()))
                        };
                        slots.into_iter().filter_map(hand).collect()
                    };
                    let mut moves = JoinSet::new();
                    for (slot, entry) in moved {
                        let this = this.clone();
                        moves.spawn(async move {
                            entry.lifecycle.drain(None);
                            if timeout(this.operation_timeout, entry.lifecycle.join()).await.is_err() {
                                entry.lifecycle.stop();
                                entry.lifecycle.join().await;
                            }
                            if !this.engine.is_closed() {
                                this.respawn(slot, entry).await;
                            }
                        });
                    }
                    while moves.join_next().await.is_some() {}
                    true
                });
            }
            while let Some(asked) = asks.join_next().await {
                kept |= !asked.unwrap_or(false);
            }
        }
    }

    /// Discovery has not heard from this node for the grace period, so the rest of the cluster may be about to run its
    /// keyed actors elsewhere. A peer that still lists it, with none that dropped it, vouches for it: an outage that hits
    /// every node alike stops nothing. Without a vouch the node is isolated. Its keyed actors stop at once when a peer
    /// dropped it or there is no ownership authority; when nobody answered, each leased one stops by its fence instead.
    pub(crate) async fn check_isolation(self: &Arc<Self>, limit: Duration) {
        let view = self.engine.membership.view();
        let peers: Vec<Arc<str>> = (view.members.iter())
            .filter(|m| m.reachable && m.record.id != self.local_node())
            .map(|m| m.record.id.as_str().into())
            .collect();
        let asked = peers.iter().cycle().skip(random_u64() as usize % peers.len().max(1)).take(peers.len().min(8));
        let mut answers = JoinSet::new();
        for peer in asked {
            let (engine, peer) = (self.engine.clone(), peer.clone());
            answers.spawn(timeout(limit, async move {
                let answer = engine.ask(peer, LISTED, Bytes::new()).await.ok()?.1?;
                answer.first().map(|listed| *listed == 1)
            }));
        }
        let (mut listed, mut dropped) = (false, false);
        while let Some(answer) = answers.join_next().await {
            match answer {
                Ok(Ok(Some(true))) => listed = true,
                Ok(Ok(Some(false))) => dropped = true,
                _ => {}
            }
        }
        if listed && !dropped {
            return self.rejoined(true);
        }
        self.heard_since.lock().take();
        self.set_isolated(true);
        if dropped || self.ownership.is_none() {
            let swept = self.sweep();
            self.restart_eager(swept);
        }
    }

    /// Each change is an event, and each start of one is counted, once the node has joined: it has been heard, or its
    /// view lists this boot as it isolates. Before, it is only starting and its view not yet formed, as while its seeds
    /// warm up: that isolation records nothing, and its first hearing ends it at once.
    fn set_isolated(&self, isolated: bool) {
        if self.isolated.swap(isolated, Relaxed) == isolated {
            return;
        }
        if isolated && !self.joined.load(Relaxed) {
            let (me, view) = (&self.engine.local.identity, self.engine.membership.view());
            let listed = view.get(&me.node_id).is_some_and(|m| m.record.incarnation == me.incarnation);
            self.joined.store(listed, Relaxed);
        }
        if !self.joined.load(Relaxed) {
            return;
        }
        if isolated {
            self.counters.isolations.fetch_add(1, Relaxed);
            self.events.record("isolated", "", "discovery cannot hear this node and no peer vouches for it");
        } else {
            self.events.record("isolation_ended", "", "heard for the grace");
        }
    }

    /// A strictly-owned actor stops as soon as its lease is known lost, or `lease_grace` after the last renew
    /// that extended it was sent, answered or not: the authority last heard from this node then. The grace must
    /// stay inside the authority's lease. One key that fails costs only its own actor.
    pub(crate) async fn renew_loop(self: Arc<Self>, every: Duration) {
        let Some(authority) = self.ownership.clone() else { return };
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let first = self.leases.lock().values().map(|l| l.until).min();
            let renew = tokio::select! {
                _ = tick.tick() => first.is_some(),
                _ = sleep_until(first.unwrap_or_else(Instant::now)), if first.is_some() => false,
            };
            if self.engine.is_closed() {
                return;
            }
            let mut lost = false;
            if renew {
                let sent = Instant::now();
                let mut renewal = std::pin::pin!(timeout(self.operation_timeout, authority.renew()));
                // Keys come due on their own deadlines while the answer is out, and only those stop.
                let renewed = loop {
                    let due = self.leases.lock().values().map(|l| l.until).min();
                    tokio::select! {
                        renewed = &mut renewal => break renewed.ok(),
                        _ = sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                            self.lose();
                        }
                    }
                };
                let (mut leases, now) = (self.leases.lock(), Instant::now());
                match renewed {
                    Some(Ok(failed)) => {
                        let failed: HashMap<&str, bool> =
                            failed.iter().map(|(name, e)| (name.as_str(), *e == OwnershipError::Lost)).collect();
                        for (name, lease) in leases.iter_mut() {
                            match failed.get(name.as_str()) {
                                None if lease.granted < sent
                                    && sent < lease.until
                                    && lease.until < sent + self.lease_grace =>
                                {
                                    lease.until = sent + self.lease_grace;
                                    lease.fence.hold_for(lease.until.saturating_duration_since(now));
                                }
                                Some(true) if lease.granted < sent => {
                                    lease.until = now;
                                    lost = true;
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Err(OwnershipError::Lost)) => {
                        leases.values_mut().filter(|lease| lease.granted < sent).for_each(|lease| lease.until = now);
                        lost = true;
                    }
                    _ => {}
                }
            }
            // A key taken since `first` was read and already due is stopped on the next pass.
            if lost || first.is_some_and(|first| first <= Instant::now()) {
                self.lose();
            }
        }
    }

    /// Fences and stops the actors whose leases are due, leaving every other key alone. The keys of eager kinds among
    /// them start again without a caller once their actors have ended.
    fn lose(&self) {
        let now = Instant::now();
        let gone: Vec<_> = {
            let (mut leases, mut lost) = (self.leases.lock(), self.lost.lock());
            let gone: Vec<_> = leases.extract_if(.., |_, lease| lease.until <= now).collect();
            lost.extend(gone.iter().map(|(name, lease)| (name.clone(), lease.epoch)));
            gone
        };
        // Fenced before `unique` is looked at: a start in flight either sees its fence shut or is found here.
        gone.iter().for_each(|(_, lease)| lease.fence.hold_for(Duration::ZERO));
        gone.iter()
            .for_each(|(name, lease)| self.events.record("lease_lost", name.clone(), format!("epoch {}", lease.epoch)));
        self.counters.leases_lost.fetch_add(gone.len() as u64, Relaxed);
        let unique = self.unique.lock();
        for (name, lease) in gone {
            let Some(slot) = slot_of(&name).map(|(kind, key)| (kind.to_owned(), key.to_owned())) else {
                continue;
            };
            if let Some(held) = unique.get(&slot).filter(|u| u.epoch == Some(lease.epoch)) {
                held.lifecycle.stop();
                self.wake(&slot);
            }
        }
    }
}

/// How this node hosts a kind, from [`Cluster::kind`]; [`register`](Self::register) makes it a candidate.
pub struct KindBuilder<'c, P: ActorProtocol> {
    cluster: &'c Cluster,
    kind: String,
    execution: Execution,
    mailbox: Option<MailboxConfig<P::Mailbox>>,
    idle: Option<Duration>,
    policy: Option<RestartPolicy>,
    eager: Option<(Bytes, Placement)>,
    also: Vec<Schema<P::Mailbox>>,
}

impl<P: ActorProtocol> KindBuilder<'_, P> {
    /// Also serves `Q`, another `SCHEMA` of `P`'s protocol, from the same actors: a node of a new build serves the
    /// old schema beside its own while a rolling upgrade has callers of both.
    pub fn also<Q: ActorProtocol<Mailbox = P::Mailbox>>(mut self) -> Self {
        self.also.push(Schema::of::<Q>());
        self
    }

    /// Where each key's actor runs. `Execution::Pool` keeps keyed actors off the shared workers;
    /// `Dedicated` is one thread per key.
    pub fn execution(mut self, execution: Execution) -> Self {
        self.execution = execution;
        self
    }

    /// Each key's actor gets its own mailbox of this shape, instead of the system's.
    pub fn mailbox(mut self, config: impl Into<MailboxConfig<P::Mailbox>>) -> Self {
        self.mailbox = Some(config.into());
        self
    }

    /// A key whose actor has handled no message for `idle` ends, giving up its lease; the next `get_or_spawn` starts
    /// it again from its args. Timers and job results do not count.
    pub fn idle(mut self, idle: Duration) -> Self {
        self.idle = Some(idle);
        self
    }

    /// An actor that fails is rebuilt under `policy` with fresh state, keeping its key, endpoint and lease.
    /// Only when the policy gives up does the key end.
    pub fn policy(mut self, policy: RestartPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// A key whose node is gone restarts on its next owner without waiting for a call, built from `args` and placed
    /// by `placement`, the same for every key: an actor that needs more reads it from a store by its key. The
    /// survivors ask the ownership authority which keys the node held, so this needs one that can tell, such as
    /// `PaxosLeases`, or `ConsulLeases` created with the node's id; `register` refuses it otherwise. The lease still
    /// decides: a key runs once.
    pub fn eager(mut self, args: Bytes, placement: Placement) -> Self {
        self.eager = Some((args, placement));
        self
    }

    /// Lets this node host the kind and advertises it, making the node a placement candidate. `build` turns the
    /// caller's encoded arguments into the actor. Without a policy, an actor that fails stops and is counted in
    /// `keyed_failed`; the next `get_or_spawn` builds it again. Registering a kind again replaces its factory for the
    /// starts after it; actors already running keep going as they were built.
    pub async fn register<A, F>(self, build: F) -> Result<(), ClusterError>
    where
        A: Actor<Message = P::Mailbox> + Send,
        F: Fn(&SpawnContext, Bytes) -> Result<A, String> + Send + Sync + 'static,
    {
        let Self { cluster, kind, execution, mailbox, idle, policy, eager, also } = self;
        let idle = idle.or(mailbox.as_ref().and_then(|m| m.idle));
        let kind = kind.as_str();
        // Commas separate kinds in the record; a slash would let two kinds' keys share a lease named `kind/key`.
        if kind.is_empty() || kind.len() > 64 || kind.contains([',', '/']) {
            return Err(ClusterError::Config("kind must be 1..=64 bytes without commas or slashes"));
        }
        if P::ID == 0 {
            return Err(ClusterError::Config("protocol ID 0 is reserved for Unaddressed"));
        }
        let schemas: Vec<Schema<P::Mailbox>> = [Schema::of::<P>()].into_iter().chain(also).collect();
        let advertised: Vec<u16> = schemas.iter().map(|s| s.schema).collect();
        let twice = advertised.iter().enumerate().any(|(i, s)| advertised[..i].contains(s));
        if schemas.iter().any(|s| s.protocol != P::ID) || twice {
            return Err(ClusterError::Config("a kind serves each schema of its own protocol once"));
        }
        schemas.iter().try_for_each(|s| refuse_control(s.lane))?;
        let inner = &cluster.0;
        // Otherwise the node would advertise the kind and win keys it can never start.
        if let Execution::Pool(pool) = execution
            && !inner.system.upgrade().is_some_and(|s| s.runtime_stats().pools.iter().any(|p| p.name == pool))
        {
            return Err(ClusterError::Config("the kind's execution names no pool of this runtime"));
        }
        let eager_kind = eager.is_some();
        // A probe: `held_by` is `None` when the authority cannot list a node's keys, and the future is never polled.
        if eager_kind && inner.ownership.as_ref().and_then(|a| a.held_by(Holder::Lapsed, &[])).is_none() {
            return Err(ClusterError::Config("an eager kind needs an ownership authority that lists a node's keys"));
        }
        let build = Arc::new(build);
        let factory: Factory = Box::new(move |system, inner, context, fence, args| {
            let built = catch_unwind(AssertUnwindSafe(|| build(&context, args.clone())));
            let (build, policy, mailbox, schemas) = (build.clone(), policy.clone(), mailbox.clone(), schemas.clone());
            Box::pin(async move {
                let failed = |e: String| (SpawnRefusal::Failed, e);
                let actor = built.unwrap_or_else(|_| Err("factory panicked".into())).map_err(failed)?;
                let name: Arc<str> = context.kind.as_str().into();
                // The rule `ActorBuilder::name` checks: a key that cannot make a name runs unnamed rather than fail.
                let named = (context.key.as_ref().map(|key| lease_of(&context.kind, key)))
                    .filter(|n| axon::valid_text(n, axon::MAX_NAME));
                // An actor that cannot serve each schema stops.
                let spawned = |exposed: Result<_, ClusterError>, lifecycle: &LifecycleHandle| -> Result<_, (_, _)> {
                    let endpoints = exposed.inspect_err(|_| lifecycle.stop()).map_err(|e| failed(e.to_string()))?;
                    Ok(Spawned { endpoints, lifecycle: lifecycle.clone() })
                };
                let Some(policy) = policy else {
                    let handle = shaped(system.spawn(actor), named, execution, mailbox, idle, fence).start();
                    let handle = handle.map_err(start_refused)?;
                    // One past the start timeout would hold its name with nothing to stop it.
                    let ready = handle.ready().await.inspect_err(|_| handle.lifecycle.stop());
                    ready.map_err(|e| failed(e.to_string()))?;
                    let exposed = (schemas.iter())
                        .map(|s| Ok((inner.expose(&handle, *s, name.clone(), false)?, s.schema, s.lane)));
                    return spawned(exposed.collect(), &handle.lifecycle);
                };
                // A factory that fails on a restart panics, so the supervisor escalates and the lease is released.
                let (mut first, mut context, starts) = (Some(actor), context, inner.starts.clone());
                let rebuild = move || {
                    first.take().unwrap_or_else(|| {
                        context.start = starts.fetch_add(1, Relaxed) + 1;
                        match build(&context, args.clone()) {
                            Ok(actor) => actor,
                            Err(e) => panic!("kind factory failed on restart: {e}"),
                        }
                    })
                };
                // A start that fails is the policy's to retry, so there is nothing to wait for here.
                let spawn = shaped(system.supervise(rebuild), named, execution, mailbox, idle, fence).policy(policy);
                let handle = spawn.start_supervised().map_err(start_refused)?;
                let exposed = (schemas.iter())
                    .map(|s| Ok((inner.expose_supervised(&handle, *s, name.clone(), false)?, s.schema, s.lane)));
                spawned(exposed.collect(), &handle.lifecycle)
            })
        });
        {
            let mut kinds = inner.kinds.write();
            let schemas = |s: &[u16]| s.iter().map(u16::to_string).collect::<Vec<_>>().join("+");
            let others = kinds.iter().filter(|(k, _)| *k != kind).map(|(k, v)| format!("{k}/{}", schemas(&v.schemas)));
            let mut names: Vec<String> = others.chain([format!("{kind}/{}", schemas(&advertised))]).collect();
            names.sort_unstable();
            let advertised_all = names.join(",");
            if advertised_all.len() > MAX_VALUE {
                return Err(ClusterError::Config("too many kinds to advertise"));
            }
            inner.record.lock().metadata.insert(KINDS.into(), advertised_all);
            let counts = kinds.get(kind).map_or_else(Default::default, |k| k.counts.clone());
            let entry = Kind { protocol: P::ID, schemas: advertised, factory, eager, counts };
            kinds.insert(kind.to_owned(), Arc::new(entry));
        }
        if eager_kind {
            inner.rebalance.notify_one();
        }
        inner.announce().await?;
        inner.refresh().await;
        Ok(())
    }
}

/// A refusal that says nothing of the key's other nodes: this one is cut off, leaving or out of reach.
fn walks_past(error: &ClusterError) -> bool {
    matches!(
        error,
        ClusterError::Spawn(SpawnRefusal::Isolated | SpawnRefusal::Leaving, _)
            | ClusterError::Call(
                CallError::Unreachable(_)
                    | CallError::NoRoute
                    | CallError::StaleNode
                    | CallError::NotSent(_)
                    | CallError::Timeout
                    | CallError::OutcomeUnknown
            )
    )
}

/// A node that does not serve the caller's `schema` of `kind` would start an actor the caller cannot reach.
fn serves(view: &View, node: &str, kind: &str, schema: Option<u16>) -> Result<(), ClusterError> {
    let mut served = view.get(node).and_then(|m| m.record.kind_schemas(kind)).unwrap_or_default();
    match schema {
        Some(wanted) if !served.is_empty() && !served.contains(&wanted) => {
            served.sort_unstable();
            Err(ClusterError::SchemaMismatch { wanted, served })
        }
        _ => Ok(()),
    }
}

/// The lease's fence goes last: it is the one fence a keyed actor must answer to.
fn shaped<'s, A: Actor, S>(
    spawn: ActorBuilder<'s, A, S>,
    name: Option<String>,
    execution: Execution,
    mailbox: Option<MailboxConfig<A::Message>>,
    idle: Option<Duration>,
    fence: Option<Fence>,
) -> ActorBuilder<'s, A, S> {
    let spawn = match name {
        Some(name) => spawn.name(name),
        None => spawn,
    }
    .execution(execution);
    match mailbox {
        Some(mailbox) => spawn.mailbox(mailbox),
        None => spawn,
    }
    .idle(idle)
    .fence(fence)
}

impl Cluster {
    /// Starts registering `kind` (1..=64 bytes, no `,` or `/`) for actors that speak `P`. Its actors run on the
    /// shared workers with the system's mailbox unless the builder says otherwise.
    pub fn kind<P: ActorProtocol>(&self, kind: &str) -> KindBuilder<'_, P> {
        KindBuilder {
            cluster: self,
            kind: kind.to_owned(),
            execution: Execution::Shared,
            mailbox: None,
            idle: None,
            policy: None,
            eager: None,
            also: Vec::new(),
        }
    }

    pub async fn spawn_on<P: WireProtocol>(
        &self,
        node: &str,
        kind: &str,
        args: Bytes,
    ) -> Result<RemoteRef<P>, ClusterError> {
        let request = SpawnRequest::new(kind, None, Placement::default(), args);
        self.place(node.into(), request).await
    }

    /// Starts an actor on a node chosen by `placement`.
    pub async fn spawn<P: WireProtocol>(
        &self,
        kind: &str,
        args: Bytes,
        placement: &Placement,
    ) -> Result<RemoteRef<P>, ClusterError> {
        let node = self.0.choose(kind, placement).await?;
        self.place(node, SpawnRequest::new(kind, None, placement.clone(), args)).await
    }

    /// The one actor of `kind` for `key`, started on its owner node if it is not running.
    /// `args` and `placement` are part of the key's identity: use the same ones everywhere.
    /// Dropping the call does not stop a start already under way. A request to the owner on every
    /// call, even when the actor is running: keep the reference, or call through [`Cluster::keyed`].
    ///
    /// Under an ownership authority, an owner that cannot take the lease (it reaches no majority of acceptors, or is
    /// leaving) or cannot be reached is passed, and the next node of the order is asked, three nodes in all; a node
    /// told that another holds the key names it, and the call asks that node if this one reaches it. The reference
    /// is to the node that answered. A holder that cannot be reached is asked about again every [`HELD_RETRY`]. All
    /// of it within `operation_timeout`.
    pub async fn get_or_spawn<P: WireProtocol>(
        &self,
        kind: &str,
        key: &str,
        args: Bytes,
        placement: &Placement,
    ) -> Result<RemoteRef<P>, ClusterError> {
        let request = SpawnRequest::new(kind, Some(key.to_owned()), placement.clone(), args);
        self.remote(self.0.place_keyed(request, (P::ID != 0).then_some(P::SCHEMA), Follow::Held).await?)
    }

    /// The node this node's view places `key` on now, the first one [`get_or_spawn`](Self::get_or_spawn) asks. No
    /// I/O; `None` when no member matches.
    pub fn owner(&self, kind: &str, key: &str, placement: &Placement) -> Option<Arc<str>> {
        self.0.owner(kind, key, placement)
    }

    /// A reference that follows `key`: it asks with [`get_or_spawn`](Self::get_or_spawn) on its first
    /// call and again after a call fails with [`Retry::Resolve`](crate::Retry::Resolve). No I/O here.
    pub fn keyed<P: WireProtocol>(&self, kind: &str, key: &str, args: Bytes, placement: &Placement) -> KeyedRef<P> {
        KeyedRef::new(self.downgrade(), kind, key, args, placement)
    }

    /// A table of the [`KeyedRef`]s of `kind`, so each key's owner is asked once: at most `max` (at least 2) kept,
    /// always those of the last `max / 2` keys got. No I/O here.
    pub fn keyed_refs<P: WireProtocol>(&self, kind: &str, placement: &Placement, max: usize) -> KeyedRefs<P> {
        KeyedRefs::new(self.downgrade(), kind, placement, max)
    }

    /// Where `key` runs, as it changes: first what its owner runs now, then each start there (`Running`) and each end
    /// (`Stopped`). A watcher that reads late skips to the state at that moment, and `Stopped` also means the owner
    /// cannot be asked. What it sees across crashes and splits is in [`axon::guide::cluster`]. No I/O until polled.
    pub fn watch_key(&self, kind: &str, key: &str, placement: &Placement) -> KeyWatch {
        let watcher = Watcher {
            cluster: self.downgrade(),
            slot: (kind.to_owned(), key.to_owned()),
            placement: placement.clone(),
            events: self.0.engine.membership.subscribe(),
            asked: None,
            shown: None,
        };
        KeyWatch(Some(Box::pin(watcher.next())))
    }

    /// The kinds registered here, the first `limit` keys here in kind and key order, and how many there are. A key
    /// is here from its start until its actor has ended, stopping included. Read from memory, no I/O.
    pub fn hosted(&self, limit: usize) -> Hosted {
        let kind = |(kind, k): (&String, &Arc<Kind>)| {
            let c = &k.counts;
            HostedKind {
                kind: kind.clone(),
                eager: k.eager.is_some(),
                count: c.keys.load(Relaxed),
                started: std::array::from_fn(|i| c.started[i].load(Relaxed)),
                ended: std::array::from_fn(|i| c.ended[i].load(Relaxed)),
            }
        };
        let kinds = self.0.kinds.read().iter().map(kind).collect();
        let unique = self.0.unique.lock();
        let keys = unique.iter().take(limit);
        Hosted { kinds, keys: keys.map(hosted_key).collect(), total: unique.len() }
    }

    /// The keys of `kind` here that start with `prefix`, in key order from `from` on (`from` itself included), at most
    /// `limit`, and whether more follow. A range read under the lock each keyed start takes: O(log n + limit).
    pub fn hosted_keys(&self, kind: &str, prefix: &str, from: Option<&str>, limit: usize) -> (Vec<HostedKey>, bool) {
        let start = from.filter(|from| *from > prefix).unwrap_or(prefix);
        let unique = self.0.unique.lock();
        let mut keys = (unique.range((kind.to_owned(), start.to_owned())..))
            .take_while(|((k, key), _)| k == kind && key.starts_with(prefix))
            .map(hosted_key);
        let page = keys.by_ref().take(limit).collect();
        (page, keys.next().is_some())
    }

    async fn place<P: WireProtocol>(
        &self,
        node: Arc<str>,
        request: SpawnRequest,
    ) -> Result<RemoteRef<P>, ClusterError> {
        self.remote(self.0.place_at(&self.view(), node, &request, (P::ID != 0).then_some(P::SCHEMA)).await?)
    }

    fn remote<P: WireProtocol>(&self, (node, placed): (Arc<str>, Placed)) -> Result<RemoteRef<P>, ClusterError> {
        Ok(RemoteRef::new(self.0.engine.clone(), placed.route.target(node, P::ID, P::SCHEMA)?, placed.epoch))
    }
}

/// What a node hosts, from [`Cluster::hosted`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Hosted {
    pub kinds: Vec<HostedKind>,
    pub keys: Vec<HostedKey>,
    /// Every key here, listed or not.
    pub total: usize,
}

/// A kind registered here, and its keys here since this node started.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HostedKind {
    pub kind: String,
    pub eager: bool,
    /// Its keys here now, as `Hosted::total` counts them.
    pub count: usize,
    /// Keys started here, by [`KEYED_STARTS`].
    pub started: [u64; KEYED_STARTS.len()],
    /// Keys whose actor here ended, by [`KEYED_ENDS`]. A key stopped while it started counts in neither.
    pub ended: [u64; KEYED_ENDS.len()],
}

fn hosted_key(((kind, key), u): (&(String, String), &Unique)) -> HostedKey {
    HostedKey { kind: kind.clone(), key: key.clone(), epoch: u.epoch }
}

/// A key here, under the lease epoch it started with; `None` without an ownership authority.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HostedKey {
    pub kind: String,
    pub key: String,
    pub epoch: Option<u64>,
}

/// Where a keyed actor runs, as [`Cluster::watch_key`] sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Owner {
    /// Started on `node`, under `epoch` when keys are strictly owned.
    Running { node: Arc<str>, epoch: Option<u64> },
    /// The node this one's view places the key on runs no actor for it, or cannot be asked.
    Stopped,
}

/// The owners of one key, from [`Cluster::watch_key`]: a stream, so an actor can own it with `cx.attach`.
pub struct KeyWatch(Option<BoxFuture<'static, Option<(Owner, Watcher)>>>);

impl KeyWatch {
    /// The next owner; `None` once the cluster has shut down or the owner refuses the watch for good.
    pub async fn next(&mut self) -> Option<Owner> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}

impl Stream for KeyWatch {
    type Item = Owner;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Owner>> {
        let Some(next) = self.0.as_mut() else { return Poll::Ready(None) };
        Poll::Ready(match ready!(next.as_mut().poll(cx)) {
            Some((owner, watcher)) => {
                self.0 = Some(Box::pin(watcher.next()));
                Some(owner)
            }
            None => {
                self.0 = None;
                None
            }
        })
    }
}

struct Watcher {
    cluster: WeakCluster,
    slot: (String, String),
    placement: Placement,
    events: MembershipSubscription,
    /// The node and boot last asked, and the endpoint it answered.
    asked: Option<(Arc<str>, u128, u64)>,
    /// What was yielded last, with the boot and endpoint behind it, so that a new start on the same node shows.
    shown: Option<(Owner, u128, u64)>,
}

impl Watcher {
    async fn next(mut self) -> Option<(Owner, Self)> {
        loop {
            let inner = self.cluster.upgrade()?.0;
            let hold = inner.engine.cfg.request_timeout / 2;
            let Self { slot, placement, events, asked, .. } = &mut self;
            // The owner, and its boot as the view lists it: a restart under the same id is asked again too.
            let placed = || {
                let node = inner.owner(&slot.0, &slot.1, placement)?;
                let boot = inner.engine.membership.view().get(&node).map(|m| m.record.incarnation);
                Some((node, boot))
            };
            let owner = placed();
            let ask = async {
                let Some(node) = owner.clone().map(|o| o.0) else { return Ok(None) };
                let incarnation = inner.engine.incarnation_of(&node).await?;
                let seen = asked.as_ref().filter(|(n, i, _)| *n == node && *i == incarnation).map_or(u64::MAX, |a| a.2);
                let mut body = BytesMut::with_capacity(16 + slot.0.len() + slot.1.len());
                body.put_u64(seen);
                body.put_u32(hold.as_millis().try_into().unwrap_or(u32::MAX));
                put_str(&mut body, &slot.0);
                put_str(&mut body, &slot.1);
                let (endpoint, schema, lane) = (WATCH, SYSTEM_SCHEMA, Lane::Control);
                let target = Target { node: node.clone(), incarnation, endpoint, schema, lane };
                let mut answer = inner.engine.call(&target, REQUEST, body.freeze()).await?.ok_or(CallError::NoReply)?;
                if answer.len() != 16 {
                    return Err(CallError::Codec("malformed watch reply".into()));
                }
                Ok(Some((node, incarnation, answer.get_u64(), Some(answer.get_u64()).filter(|e| *e != 0))))
            };
            let answer = {
                let mut ask = std::pin::pin!(ask);
                loop {
                    tokio::select! {
                        biased;
                        event = events.next() => match event {
                            // A change that leaves the key where it was keeps the question held there.
                            Some(_) if placed() == owner => {}
                            Some(_) => break None,
                            None => return None,
                        },
                        answer = &mut ask => break Some(answer),
                    }
                }
            };
            let Some(answer) = answer else { continue };
            let stopped = (Owner::Stopped, 0, 0);
            let (now, wait) = match answer {
                Ok(Some((node, incarnation, endpoint, epoch))) => {
                    *asked = Some((node.clone(), incarnation, endpoint));
                    let running = (endpoint != 0).then_some((Owner::Running { node, epoch }, incarnation, endpoint));
                    (Some(running.unwrap_or(stopped)), Duration::ZERO)
                }
                Ok(None) => (Some(stopped), hold),
                Err(e) if e.retry() == Retry::Later => (None, HELD_RETRY),
                // Refused for good: this node may not watch the kind, or the answer cannot be read.
                Err(e) if e.retry() == Retry::Never => return None,
                Err(e) => {
                    *asked = None;
                    // A node from before watches: asked again only once the view changes or a hold passes.
                    let old = matches!(e, CallError::Rejected(Rejection::UnknownActor));
                    (Some(stopped), if old { hold } else { HELD_RETRY })
                }
            };
            if let Some(now) = now.filter(|now| self.shown.as_ref() != Some(now)) {
                self.shown = Some(now.clone());
                return Some((now.0, self));
            }
            // Until the view changes, or the owner can be asked again.
            if !wait.is_zero() && matches!(timeout(wait, self.events.next()).await, Ok(None)) {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spawn_request_survives_the_wire_and_hostile_bytes_are_refused_not_trusted() {
        let labels = |pairs: &[(&str, &str)]| pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let placement = Placement {
            required: labels(&[("zone", "a")]),
            preferred: labels(&[("ssd", "1"), ("x", "")]),
            ..Default::default()
        };
        for (key, passed) in [(None, vec![]), (Some("user 7".to_string()), vec!["n1".to_string(), "n2".to_string()])] {
            let request = SpawnRequest {
                kind: "session".into(),
                key,
                placement: placement.clone(),
                passed,
                args: Bytes::from_static(b"args"),
            };
            let wire = request.encode();
            let back = SpawnRequest::decode(wire.clone()).unwrap();
            assert_eq!(
                (&back.kind, &back.key, &back.placement, &back.passed, &back.args),
                (&request.kind, &request.key, &placement, &request.passed, &request.args)
            );
            // Everything before the args is needed: any shorter body is refused.
            for cut in 0..wire.len() - request.args.len() {
                assert!(SpawnRequest::decode(wire.slice(..cut)).is_none(), "cut at {cut}");
            }
            let mut read = BytesMut::with_capacity(64 << 10);
            read.extend_from_slice(&wire);
            let args = SpawnRequest::decode(read.freeze()).unwrap().args;
            assert_eq!(BytesMut::from(args).capacity(), 4, "the args pin the frame's read buffer");
        }
        // The bytes a peer reads. When this fails you changed them: bump `axon_net::VERSIONS`.
        let placement = Placement { required: labels(&[("z", "a")]), ..Default::default() };
        let (key, passed, args) = (Some("7".into()), vec!["p".into()], Bytes::from_static(b"x"));
        let request = SpawnRequest { kind: "k".into(), key, placement, passed, args };
        assert_eq!(axon_net::VERSIONS, 7..=7, "the wire version moved: re-pin the bytes below");
        assert_eq!(
            &request.encode()[..],
            [1, 0, 1, b'k', 0, 1, b'7', 0, 1, 0, 1, b'z', 0, 1, b'a', 0, 0, 0, 1, 0, 1, b'p', b'x']
        );
        let route = Route { incarnation: 9, protocol: 3, endpoints: [(17, 2, Lane::Bulk)].into() };
        let placed = Placed { route, epoch: Some(5), started: true }.encode();
        assert_eq!(&placed[..], [0, 0, 0, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 17, 0, 2, 2]);
        let back = Placed::decode(9, placed.clone()).unwrap();
        assert_eq!((back.epoch, back.started, back.route.protocol), (Some(5), true, 3));
        assert_eq!((back.route.incarnation, &back.route.endpoints[..]), (9, &[(17, 2, Lane::Bulk)][..]));
        for cut in 0..placed.len() {
            assert!(Placed::decode(9, placed.slice(..cut)).is_none(), "cut at {cut}");
        }

        // A label count that promises more than the body holds, and text that is not UTF-8.
        assert!(SpawnRequest::decode(Bytes::from_static(&[0, 0, 1, b'k', 0, 0, 0xFF, 0xFF])).is_none());
        assert!(SpawnRequest::decode(Bytes::from_static(&[0, 0, 1, 0xFF, 0, 0, 0, 0, 0, 0])).is_none());
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for len in 0..2000usize {
            let body: Vec<u8> = (0..len % 97)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> if len % 2 == 0 { 0 } else { 61 }) as u8
                })
                .collect();
            if let Some(request) = SpawnRequest::decode(body.into()) {
                let again = SpawnRequest::decode(request.encode()).unwrap();
                assert_eq!(
                    (again.kind, again.key, again.placement, again.passed),
                    (request.kind, request.key, request.placement, request.passed)
                );
            }
        }
        assert_eq!(SpawnRefusal::from_code(200), SpawnRefusal::Failed);
    }

    /// `lose` on the renew task and a start answered from the hold look at `leases` and `lost` at once, on two
    /// threads: the start either finds the due entry, whose fence `lose` shuts, or the epoch lost. Never neither, which
    /// would build a fresh `lease_grace` fence on the epoch just lost, past the acceptors' lease.
    #[tokio::test]
    #[expect(clippy::disallowed_methods, reason = "the test runs real threads")]
    async fn a_start_from_the_hold_never_fences_anew_an_epoch_lose_takes() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let mut transport = axon_net::Config::new("a", axon_net::SharedSecret::new(*b"0123456789abcdef0123").unwrap());
        transport.allow_plaintext = true;
        let cfg = crate::ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, crate::MemoryDiscovery::new());
        let system = ActorSystem::builder().build().unwrap();
        let cluster = Cluster::start(&system, cfg).await.unwrap();
        let (inner, clock, name) = (&cluster.0, system.clock(), "k/x");
        const ROUNDS: usize = 20_000;
        let (go, done, fresh) = (AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0));
        let started = parking_lot::Mutex::new(None);
        // Three threads take turns: on two cores a spinning waiter would hold one for its whole time slice.
        let wait = |at: &AtomicUsize, n: usize| {
            while at.load(SeqCst) < n {
                std::thread::yield_now();
            }
        };
        std::thread::scope(|s| {
            s.spawn(|| {
                for round in 1..=ROUNDS {
                    wait(&go, round);
                    // The two threads meet at every offset within a few hundred spins.
                    (0..round % 97).for_each(|_| std::hint::spin_loop());
                    inner.lose();
                    done.fetch_add(1, SeqCst);
                }
            });
            s.spawn(|| {
                for round in 1..=ROUNDS {
                    wait(&go, round);
                    *started.lock() = inner.hold(name, round as u64, Instant::now(), clock).ok();
                    done.fetch_add(1, SeqCst);
                }
            });
            for round in 1..=ROUNDS {
                let (until, epoch) = (Instant::now(), round as u64);
                let lease = Lease { epoch, fence: Fence::new(clock, Duration::from_secs(60)), until, granted: until };
                inner.leases.lock().insert(name.into(), lease);
                go.store(round, SeqCst);
                wait(&done, 2 * round);
                if started.lock().take().is_some_and(|fence| fence.is_open()) {
                    fresh.fetch_add(1, SeqCst);
                }
                inner.leases.lock().clear();
                inner.lost.lock().clear();
            }
        });
        assert_eq!(fresh.load(SeqCst), 0, "of {ROUNDS} starts racing lose(), these fenced the lost epoch anew");
        system.shutdown().await.unwrap();
    }
}
