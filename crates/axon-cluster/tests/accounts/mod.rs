//! Keyed accounts on `PaxosLeases` and seed discovery (or memory discovery) in the World, writing to a ledger fenced by the lease epoch.
//! Shared by the World test files; each is a binary of its own, with no test that starts a real thread.
#![allow(dead_code, reason = "each World test file uses part of it")]
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, KEYED_ENDS, MemoryDiscovery, PaxosConfig,
    PaxosLeases, Placement, Rejection, Reply, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig, SpawnContext,
    Unaddressed, WireProtocol,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Host, Task, World, WorldConfig};
use bytes::{Buf, Bytes};

const SECRET: &str = "at-least-16-bytes-long";
const SEED_PORT: u16 = 7001;
pub const NODES: [&str; 3] = ["a", "b", "c"];
/// Five nodes, as the rig: the first three are the acceptors, every one a seed.
const FIVE: [&str; 5] = ["a", "b", "c", "d", "e"];

/// A database outside the cluster: no fault reaches it. The lease epoch is the fencing token.
#[derive(Clone, Default)]
pub struct Ledger(Arc<Mutex<BTreeMap<String, Entry>>>);

#[derive(Default)]
struct Entry {
    epoch: u64,
    writes: u64,
    /// Writes from an epoch older than one already seen: two owners overlapped.
    stale: Vec<(u64, u64)>,
    /// The nodes that built an actor for the key at each epoch.
    starts: BTreeMap<u64, BTreeSet<String>>,
}

impl Ledger {
    /// The newest epoch that wrote `key`.
    pub fn epoch(&self, key: &str) -> Option<u64> {
        self.0.lock().unwrap().get(key).map(|e| e.epoch)
    }

    /// The writes to `key` refused for an epoch older than one already seen, as (epoch, id).
    pub fn refused(&self, key: &str) -> Vec<(u64, u64)> {
        self.0.lock().unwrap().get(key).map_or(Vec::new(), |e| e.stale.clone())
    }

    fn apply(&self, key: &str, epoch: u64, id: u64) -> bool {
        let mut all = self.0.lock().unwrap();
        let entry = all.entry(key.to_owned()).or_default();
        if epoch < entry.epoch {
            entry.stale.push((epoch, id));
            return false;
        }
        entry.epoch = epoch;
        entry.writes += 1;
        true
    }

    fn started(&self, key: &str, epoch: u64, node: &str) {
        let mut all = self.0.lock().unwrap();
        all.entry(key.to_owned()).or_default().starts.entry(epoch).or_default().insert(node.to_owned());
    }

    fn writes(&self, key: &str) -> u64 {
        self.0.lock().unwrap().get(key).map_or(0, |e| e.writes)
    }
}

struct Account {
    key: String,
    epoch: u64,
    ledger: Ledger,
}

struct Deposit(u64, Option<Reply<u64>>);

impl Actor for Account {
    type Message = Deposit;
    type Error = ();

    fn on_message(&mut self, Deposit(id, reply): Deposit, _: &mut Context<'_, Self>) -> Result<(), ()> {
        // A stale write is refused and never answered.
        if self.ledger.apply(&self.key, self.epoch, id) {
            drop(reply.map(|r| r.send(id)));
        }
        Ok(())
    }
}

struct AccountApi;

impl WireProtocol for AccountApi {
    type Message = u64;
    type Reply = u64;
    const ID: u64 = 0xA1;
    const SCHEMA: u16 = 1;

    fn encode(id: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&id.to_be_bytes()))
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("short reply".into()))
    }
}

impl ActorProtocol for AccountApi {
    type Mailbox = Deposit;

    fn encode_reply(id: u64) -> Result<Bytes, CallError> {
        Self::encode(id)
    }

    fn decode(mut body: Bytes, reply: Option<Reply<u64>>) -> Result<Deposit, Rejection> {
        body.try_get_u64().map(|id| Deposit(id, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

/// Works on its own: writes the ledger at its epoch as it starts and every 100 ms, under `kind/key`.
struct Ticker {
    key: String,
    epoch: u64,
    ledger: Ledger,
}

struct Tick;

impl Actor for Ticker {
    type Message = Tick;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        self.ledger.apply(&self.key, self.epoch, 0);
        cx.schedule_every(Duration::from_millis(100), || Tick).map(drop).map_err(drop)
    }

    fn on_message(&mut self, Tick: Tick, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.ledger.apply(&self.key, self.epoch, 0);
        Ok(())
    }
}

/// Nothing to call: tickers are started with `get_or_spawn::<Unaddressed>`.
struct TickApi;

impl WireProtocol for TickApi {
    type Message = ();
    type Reply = ();
    const ID: u64 = 0xA2;
    const SCHEMA: u16 = 1;

    fn encode((): ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(_: Bytes) -> Result<(), CallError> {
        Ok(())
    }
}

impl ActorProtocol for TickApi {
    type Mailbox = Tick;

    fn encode_reply((): ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode(_: Bytes, _: Option<Reply<()>>) -> Result<Tick, Rejection> {
        Ok(Tick)
    }
}

#[derive(Clone)]
pub struct Node {
    system: ActorSystem,
    pub cluster: Cluster,
    pub leases: Arc<PaxosLeases>,
    pub seed: Option<Arc<SeedServer>>,
}

/// `lease` is the Paxos lease, when not `PaxosConfig::new`'s; the nodes named in `tickers` host tickers too. Every one
/// of `nodes` is a seed, unless they share `memory`, and the first three are the acceptors.
async fn boot(
    host: Host,
    ledger: Ledger,
    lease: Option<Duration>,
    tickers: &'static [&'static str],
    nodes: &'static [&'static str],
    memory: Option<MemoryDiscovery>,
) -> Node {
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let (mut cfg, seed) = match memory {
        Some(memory) => (ClusterConfig::new(host.addr(), transport, memory), None),
        None => {
            // Every node is also a seed, so discovery partitions and crashes with the nodes.
            let addr = SocketAddr::new(host.ip(), SEED_PORT);
            let seed = SeedServer::bind(SeedServerConfig::new(addr, &transport)).await.unwrap();
            let entries: Vec<_> = nodes.iter().map(|n| SocketAddr::new(host.ip_of(n), SEED_PORT).to_string()).collect();
            let discovery = SeedDiscovery::start(SeedConfig::new(entries, &transport)).await.unwrap();
            let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
            // With seeds, as guide/cluster.md § Seed servers asks.
            cfg.heartbeat_interval = Duration::from_millis(500);
            cfg.heartbeat_grace = Duration::from_secs(1);
            (cfg, Some(Arc::new(seed)))
        }
    };
    let mut paxos = PaxosConfig::new(nodes[..3].iter().copied());
    paxos.store = Some(Arc::new(host.disk().dir("leases")));
    if let Some(lease) = lease {
        // The world's own lease, so that keys move within its run.
        paxos.lease = lease;
    }
    if !tickers.is_empty() {
        // Shorter than the lease a crashed holder leaves, so an eager restart outlasts a start refused `Held`.
        cfg.operation_timeout = Duration::from_secs(3);
    }
    let leases = Arc::new(PaxosLeases::new(paxos).unwrap());
    cfg.ownership = Some(leases.clone());
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    // Before "account", so a node that lists that kind lists these too.
    let hosts_tickers = tickers.iter().any(|t| *t == host.name());
    for (kind, eager) in [("eager", true), ("lazy", false)].into_iter().filter(|_| hosts_tickers) {
        let ledger = ledger.clone();
        let builder = cluster.kind::<TickApi>(kind);
        let builder = if eager { builder.eager(Bytes::new(), Placement::default()) } else { builder };
        builder
            .register(move |cx: &SpawnContext, _: Bytes| {
                let key = format!("{}/{}", cx.kind, cx.key.clone().ok_or("keyed only")?);
                Ok(Ticker { key, epoch: cx.epoch.ok_or("leased only")?, ledger: ledger.clone() })
            })
            .await
            .unwrap();
    }
    let name = host.name().to_owned();
    cluster
        .kind::<AccountApi>("account")
        .register(move |cx: &SpawnContext, _: Bytes| {
            let (key, epoch) = (cx.key.clone().ok_or("keyed only")?, cx.epoch.ok_or("leased only")?);
            ledger.started(&key, epoch, &name);
            Ok(Account { key, epoch, ledger: ledger.clone() })
        })
        .await
        .unwrap();
    Node { system, cluster, leases, seed }
}

/// Three nodes that each list all three with the kind.
pub fn accounts(cfg: WorldConfig) -> (World<Node>, Ledger) {
    world(cfg, None, &[], &NODES, None)
}

/// As `accounts` with Paxos leases of `lease`.
pub fn leased(cfg: WorldConfig, lease: Duration) -> (World<Node>, Ledger) {
    world(cfg, Some(lease), &[], &NODES, None)
}

/// As `accounts` with Paxos leases of `lease`, and each node also hosts tickers of an eager kind and of a lazy one.
pub fn tickers(cfg: WorldConfig, lease: Duration) -> (World<Node>, Ledger) {
    world(cfg, Some(lease), &NODES, &NODES, None)
}

/// As `tickers`, the nodes finding each other in one `MemoryDiscovery`: a node up alone sees a view that is not
/// stale, only short.
pub fn tickers_in_memory(cfg: WorldConfig, lease: Duration) -> (World<Node>, Ledger) {
    world(cfg, Some(lease), &NODES, &NODES, Some(MemoryDiscovery::new()))
}

/// The nodes of `FIVE`, each listing all five with the kind, on Paxos leases of `lease`.
pub fn five(cfg: WorldConfig, lease: Duration) -> (World<Node>, Ledger) {
    world(cfg, Some(lease), &[], &FIVE, None)
}

/// As `five`, with tickers on d and e only: the acceptors run no eager kind.
pub fn five_tickers(cfg: WorldConfig, lease: Duration) -> (World<Node>, Ledger) {
    world(cfg, Some(lease), &["d", "e"], &FIVE, None)
}

fn world(
    cfg: WorldConfig,
    lease: Option<Duration>,
    tickers: &'static [&'static str],
    nodes: &'static [&'static str],
    memory: Option<MemoryDiscovery>,
) -> (World<Node>, Ledger) {
    let ledger = Ledger::default();
    let mut w = World::new(cfg);
    for name in nodes {
        let (l, memory) = (ledger.clone(), memory.clone());
        w.add(name, move |host| boot(host, l.clone(), lease, tickers, nodes, memory.clone()));
    }
    settled(&mut w, nodes.len());
    (w, ledger)
}

/// Each of `n` nodes is up and lists all `n` with the kind.
fn settled(w: &mut World<Node>, n: usize) {
    w.run_until("every member with the kind", Duration::from_secs(30), |w| {
        let with_kind =
            |node: &Node| node.cluster.view().members.iter().filter(|m| m.record.has_kind("account")).count();
        w.nodes().count() == n && w.nodes().all(|(_, node)| with_kind(node) == n)
    });
}

pub fn keys() -> Vec<String> {
    (0..24).map(|k| format!("acct-{k}")).collect()
}

type Call = (String, u64, Task<Result<u64, ClusterError>>);

fn deposit(w: &mut World<Node>, from: &str, key: &str, id: u64) -> Task<Result<u64, ClusterError>> {
    let key = key.to_owned();
    w.spawn_on(from, move |n| async move {
        n.cluster.keyed::<AccountApi>("account", &key, Bytes::new(), &Placement::default()).request(id).await
    })
}

/// The node running the key's actor, as `from` finds it, and the actor's epoch.
pub fn owner(w: &mut World<Node>, from: &str, key: &str) -> Option<(String, Option<u64>)> {
    let key = key.to_owned();
    let found = w.block_on(from, move |n| async move {
        n.cluster.get_or_spawn::<AccountApi>("account", &key, Bytes::new(), &Placement::default()).await
    });
    found.ok()?.ok().map(|r| (r.node().to_owned(), r.epoch()))
}

/// The keys whose actor runs on `node`, as a finds them, with their epochs.
pub fn owned_by(w: &mut World<Node>, keys: &[String], node: &str) -> Vec<(String, Option<u64>)> {
    let mut found = Vec::new();
    for key in keys {
        if let Some((n, epoch)) = owner(w, "a", key)
            && n == node
        {
            found.push((key.clone(), epoch));
        }
    }
    found
}

/// 20 deposits from `from`, round the keys.
pub fn send(w: &mut World<Node>, keys: &[(String, Option<u64>)], from: &str, id: &mut u64, calls: &mut Vec<Call>) {
    for (key, _) in keys.iter().cycle().take(20) {
        *id += 1;
        calls.push((key.clone(), *id, deposit(w, from, key, *id)));
    }
}

/// Deposits from `from`, one every 10 ms round the keys for `d`, each through a reference kept for its key, as a
/// long-lived client's. Ends once every call has.
pub fn stream(w: &mut World<Node>, from: &str, d: Duration, first_id: u64) -> Task<()> {
    w.spawn_on(from, move |n| async move {
        let refs: Vec<_> = keys()
            .iter()
            .map(|key| n.cluster.keyed::<AccountApi>("account", key, Bytes::new(), &Placement::default()))
            .collect();
        let (end, mut calls) = (tokio::time::Instant::now() + d, Vec::new());
        for (id, keyed) in (first_id..).zip(refs.iter().cycle()) {
            if tokio::time::Instant::now() >= end {
                break;
            }
            let keyed = keyed.clone();
            calls.push(tokio::task::spawn_local(async move {
                if let Ok(got) = keyed.request(id).await {
                    assert_eq!(got, id);
                }
            }));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for call in calls {
            call.await.unwrap();
        }
    })
}

/// From `from`, ops on every key through references kept for good for `d`, as the rig's client makes them: each
/// retried every 50 ms with its id until answered, the next sent 100 ms after. `pauses` are the (start, end) of the
/// faults, from now: an op is late by how long it took past its sending or the end of the last fault that began before
/// it was answered, whichever is later. The latest op's lateness, key and sending.
pub fn ops(
    w: &mut World<Node>,
    from: &str,
    d: Duration,
    pauses: Vec<(Duration, Duration)>,
    first_id: u64,
) -> Task<(Duration, String, Duration)> {
    w.spawn_on(from, move |n| async move {
        let start = tokio::time::Instant::now();
        let mut calls = Vec::new();
        for (i, key) in keys().into_iter().enumerate() {
            let keyed = n.cluster.keyed::<AccountApi>("account", &key, Bytes::new(), &Placement::default());
            let pauses = pauses.clone();
            calls.push(tokio::task::spawn_local(async move {
                let mut worst = (Duration::ZERO, String::new(), Duration::ZERO);
                let mut id = first_id + ((i as u64) << 24);
                while start.elapsed() < d {
                    id += 1;
                    let sent = start.elapsed();
                    while keyed.request(id).await.is_err() {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    let done = start.elapsed();
                    let since =
                        pauses.iter().filter(|(on, _)| *on < done).map(|(_, off)| *off).fold(sent, Duration::max);
                    worst = worst.max((done.saturating_sub(since), key.clone(), sent));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                worst
            }));
        }
        let mut worst = (Duration::ZERO, String::new(), Duration::ZERO);
        for call in calls {
            worst = worst.max(call.await.unwrap());
        }
        worst
    })
}

/// From `from`, ops on `key` for `d`, each looked up afresh as a new client does (`get_or_spawn`, then the request)
/// and tried again every 50 ms with its id until answered, the next sent 100 ms after. An op is late by how long it
/// took past its sending or `since` (from now), whichever is later. The latest op's lateness, its sending, how many
/// ops were answered, and the last error of the latest op.
pub fn fresh_ops(
    w: &mut World<Node>,
    from: &str,
    key: &str,
    d: Duration,
    since: Duration,
    first_id: u64,
) -> Task<(Duration, Duration, u64, String)> {
    let key = key.to_owned();
    w.spawn_on(from, move |n| async move {
        let start = tokio::time::Instant::now();
        let (mut worst, mut answered, mut id) = ((Duration::ZERO, Duration::ZERO, String::new()), 0, first_id);
        while start.elapsed() < d {
            id += 1;
            let (sent, mut error) = (start.elapsed(), String::new());
            loop {
                let keyed = n.cluster.keyed::<AccountApi>("account", &key, Bytes::new(), &Placement::default());
                // Longer than `operation_timeout`, so a start refused comes back as its refusal, not as the deadline.
                match keyed.request_within(id, Duration::from_secs(6)).await {
                    Ok(got) => {
                        assert_eq!(got, id);
                        break;
                    }
                    Err(e) => error = format!("{e:?} at {:?}", start.elapsed()),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            answered += 1;
            let late = start.elapsed().saturating_sub(sent.max(since));
            if late > worst.0 {
                worst = (late, sent, error);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (worst.0, worst.1, answered, worst.2)
    })
}

/// The nodes `from`'s view lists with the kind, in the rendezvous order of `account/key`: the owner first, then the
/// owner were it gone, and so on.
pub fn order(w: &mut World<Node>, from: &str, key: &str) -> Vec<String> {
    let key = key.to_owned();
    let order = w.block_on(from, move |n| async move {
        let view = n.cluster.view();
        let mut left: Vec<_> = view.members.iter().filter(|m| m.record.has_kind("account")).collect();
        let mut order = Vec::new();
        while let Some(first) = axon_cluster::rendezvous("account", &key, &left) {
            let id = first.record.id.clone();
            left.retain(|m| m.record.id != id);
            order.push(id);
        }
        order
    });
    order.unwrap_or_default()
}

/// How many keys of the account kind started on `node`, and how many ended there because they moved.
pub fn starts_and_moves(w: &World<Node>, node: &str) -> (u64, u64) {
    let kind = w
        .nodes()
        .find(|(name, _)| *name == node)
        .and_then(|(_, n)| n.cluster.hosted(0).kinds.into_iter().find(|k| k.kind == "account"));
    let moved = KEYED_ENDS.iter().position(|e| *e == "moved").expect("an end");
    kind.map_or((0, 0), |k| (k.started.iter().sum(), k.ended[moved]))
}

/// Waits for each call, and retries an unknown outcome with the same id, as an application does.
pub fn settle(w: &mut World<Node>, calls: Vec<Call>) {
    for (key, id, task) in calls {
        let mut result = w.join(task);
        for _ in 0..5 {
            if matches!(result, Ok(Ok(_))) {
                break;
            }
            let retry = deposit(w, "a", &key, id);
            result = w.join(retry);
        }
        if let Ok(Ok(got)) = result {
            assert_eq!(got, id);
        }
    }
}

/// Starts `kind/key` from `from`, and names the node it runs on and its epoch.
pub fn start_ticker(w: &mut World<Node>, from: &str, kind: &str, key: &str) -> Option<(String, Option<u64>)> {
    let (kind, key) = (kind.to_owned(), key.to_owned());
    let found = w.block_on(from, move |n| async move {
        n.cluster.get_or_spawn::<Unaddressed>(&kind, &key, Bytes::new(), &Placement::default()).await
    });
    found.ok()?.ok().map(|r| (r.node().to_owned(), r.epoch()))
}

/// The node `from`'s view places `kind/key` on.
pub fn placed_on(w: &mut World<Node>, from: &str, kind: &str, key: &str) -> Option<String> {
    let (kind, key) = (kind.to_owned(), key.to_owned());
    let placed = w.block_on(from, move |n| async move { n.cluster.owner(&kind, &key, &Placement::default()) });
    placed.ok()?.map(|node| node.to_string())
}

/// `node` is up and its view lists `other` as reachable with `kind`.
pub fn lists(w: &World<Node>, node: &str, other: &str, kind: &str) -> bool {
    let listed = |n: &Node| n.cluster.view().get(other).is_some_and(|m| m.reachable && m.record.has_kind(kind));
    w.nodes().any(|(name, n)| name == node && listed(n))
}

/// `node` is up and runs `name` (`kind/key`) at `epoch`.
pub fn hosts(w: &World<Node>, node: &str, name: &str, epoch: Option<u64>) -> bool {
    let runs = |n: &Node| {
        let hosted = n.cluster.hosted(usize::MAX).keys;
        hosted.iter().any(|h| format!("{}/{}", h.kind, h.key) == name && h.epoch == epoch)
    };
    w.nodes().any(|(at, n)| at == node && runs(n))
}

/// The nodes that are up and run `name` (`kind/key`), at any epoch.
pub fn hosted_on(w: &World<Node>, name: &str) -> Vec<String> {
    let runs = |n: &Node| n.cluster.hosted(usize::MAX).keys.iter().any(|h| format!("{}/{}", h.kind, h.key) == name);
    w.nodes().filter(|(_, n)| runs(n)).map(|(at, _)| at.to_owned()).collect()
}

/// What `node` put on its timeline, oldest first; nothing from a node that is down.
pub fn told(w: &mut World<Node>, node: &str) -> Vec<(&'static str, String, String)> {
    let told = w.block_on(node, |n| async move { n.cluster.events().since(0, usize::MAX) });
    told.unwrap_or_default().into_iter().map(|e| (e.kind, e.subject, e.detail)).collect()
}

/// Starts a clean leave of `node`, as `system.shutdown()`.
pub fn leave(w: &mut World<Node>, node: &str) -> Task<()> {
    w.spawn_on(node, |n| async move { drop(n.system.shutdown().await) })
}

/// Each of `keys` is written in the next second: its ticker runs.
pub fn running(w: &mut World<Node>, ledger: &Ledger, keys: &[(String, Option<u64>)], seed: u64) {
    let before: Vec<u64> = keys.iter().map(|(key, _)| ledger.writes(key)).collect();
    w.run_for(Duration::from_secs(1));
    for ((key, _), before) in keys.iter().zip(before) {
        assert!(ledger.writes(key) > before, "seed {seed}: {key} no longer runs\n{}", w.trace_tail(60));
    }
}

/// Every key `before` lists was written since with a newer epoch.
pub fn moved(w: &World<Node>, ledger: &Ledger, before: &[(String, Option<u64>)], seed: u64, why: &str) {
    let all = ledger.0.lock().unwrap();
    for (key, epoch) in before {
        let now = all.get(key).map(|e| e.epoch);
        assert!(now > *epoch, "seed {seed}: {why} did not move {key}\n{}", w.trace_tail(40));
    }
}

/// No epoch of a key had its actor built on two nodes.
pub fn one_node_per_epoch(w: &World<Node>, ledger: &Ledger, seed: u64) {
    let ledger = ledger.0.lock().unwrap();
    let shared: Vec<_> = (ledger.iter())
        .flat_map(|(key, e)| e.starts.iter().filter(|(_, on)| on.len() > 1).map(move |(epoch, on)| (key, epoch, on)))
        .collect();
    assert!(shared.is_empty(), "seed {seed}: one epoch started on two nodes {shared:?}\n{}", w.trace_tail(80));
}

/// No key, account or ticker, was written by an older epoch after a newer one.
pub fn no_stale_write(w: &World<Node>, ledger: &Ledger, seed: u64) {
    let ledger = ledger.0.lock().unwrap();
    let stale: Vec<_> = ledger.iter().filter(|(_, e)| !e.stale.is_empty()).map(|(k, e)| (k, &e.stale)).collect();
    assert!(stale.is_empty(), "seed {seed}: two owners wrote {stale:?}\n{}", w.trace_tail(80));
}

/// Once the views agree and every key runs where each view places it, every node finds each key on one node with one
/// epoch; and no key was written by an older epoch after a newer one or started on two nodes at one epoch. Prints the
/// digest, which `scripts/check.sh sim` compares across two processes.
pub fn check(w: &mut World<Node>, ledger: &Ledger, test: &str, seed: u64) {
    settled(w, NODES.len());
    // A move the owner refused while isolated comes again `operation_timeout` later: not between the lookups below.
    // A key no node runs counts too: the first lookup starts it, and a view that places it elsewhere would move it.
    let keys = keys();
    w.run_until(
        "every view placing each key on one node, and the key running only there",
        Duration::from_secs(30),
        |w| {
            keys.iter().all(|key| {
                let mut owners: Vec<_> =
                    w.nodes().map(|(_, n)| n.cluster.owner("account", key, &Placement::default())).collect();
                owners.dedup();
                let [Some(owner)] = &owners[..] else { return false };
                let runs =
                    |n: &Node| n.cluster.hosted_keys("account", key, None, 1).0.first().is_some_and(|h| h.key == *key);
                w.nodes().all(|(at, n)| at == &**owner || !runs(n))
            })
        },
    );
    for key in keys {
        let seen: BTreeSet<_> = NODES.into_iter().filter_map(|from| owner(w, from, &key)).collect();
        assert_eq!(seen.len(), 1, "seed {seed}: {key} has owners {seen:?}\n{}", w.trace_tail(40));
    }
    // Last, so the starts and writes of the wait and the lookups count too.
    no_stale_write(w, ledger, seed);
    one_node_per_epoch(w, ledger, seed);
    println!("world digest {test} seed {seed}: {:016x}", w.digest());
}
