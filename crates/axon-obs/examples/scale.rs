//! How far one node and one cluster stretch. Everything runs in this process over loopback.
//! usage: scale actors [count=1000000] [mailbox capacity=1024]
//!        scale keyed  [keys=5000] [lease ms=1]   `AXON_PAXOS` set to any number: Paxos leases on three nodes instead, then renewals timed
//!        scale nodes  [count=200] [links per node=20] [keys=20000] [bare: no telemetry]
//!        scale streams [streams=50000] [relays=3] [leaves=6] [records/s=200000] [each leaf wants 1 in=2]
//!        scale subscribers [subscriptions=20000] [streams=2] [batches/s=200] [records per batch=1]
//!        scale metrics [MiB=256]   fills a `MemoryMetrics` with playground-shaped batches, then as much again 3 times
//! `nodes` opens two descriptors per link: run it under `ulimit -n 500000`.
//! `AXON_SEEDS=3` puts `nodes` on that many seed servers of this process instead of the in-memory discovery.
//! `AXON_PAXOS=3` gives `nodes` Paxos leases, the first that many nodes serving as acceptors.
//! `AXON_TIMINGS=64` turns actor timings on, one message in 64 (and each actor's first).
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::{Actor, ActorRef, ActorSystem, Context, MailboxLimits, RuntimeConfig};
use axon_cluster::*;
use axon_net::{AcceptLimits, SharedSecret};
use axon_obs::{MemoryMetrics, MetricsSink, Sample, SampleBatch, Telemetry, TelemetryConfig, Value, fetch_samples};
use axon_stream::{
    DeliveryPolicy, RelayStats, RingLimits, StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions,
};
use bytes::Bytes;
use tokio::task::JoinSet;

/// As wide as a small message enum, so a mailbox slot costs what it would in an application.
type Wide = [u64; 8];

struct Idle(Arc<AtomicU64>);

impl Actor for Idle {
    type Message = Wide;
    type Error = ();

    fn on_message(&mut self, _: Wide, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.fetch_add(1, Relaxed);
        Ok(())
    }
}

struct Echo;

impl Actor for Echo {
    type Message = (Bytes, Option<Reply>);
    type Error = ();

    fn on_message(&mut self, (body, reply): Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
        drop(reply.map(|r| r.send(body)));
        Ok(())
    }
}

struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = 0x5ca1e;
    const SCHEMA: u16 = 1;

    fn encode(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for EchoApi {
    type Mailbox = (Bytes, Option<Reply>);

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply>) -> Result<Self::Mailbox, Rejection> {
        Ok((body, reply))
    }
}

/// A lease that costs a round trip, as one held in Consul does.
struct Slow(MemoryAuthority, Duration);

impl OwnershipAuthority for Slow {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        let key = key.to_owned();
        Box::pin(async move {
            if !self.1.is_zero() {
                tokio::time::sleep(self.1).await;
            }
            self.0.acquire(&key).await
        })
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        self.0.renew()
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        self.0.release(key, epoch)
    }
}

fn ps(column: &str) -> String {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps").args(["-o", column, "-p", &pid]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn rss_mb() -> f64 {
    ps("rss=").parse::<f64>().unwrap_or(0.0) / 1024.0
}

fn virtual_mb() -> f64 {
    ps("vsz=").parse::<f64>().unwrap_or(0.0) / 1024.0
}

/// Processor seconds this process has used. Linux counts them in 10 ms ticks; elsewhere `ps` is asked.
fn cpu_s() -> f64 {
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        let fields: Vec<&str> = stat.rsplit(')').next().unwrap_or_default().split_whitespace().collect();
        return (fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap()) / 100.0;
    }
    ps("cputime=").split(':').fold(0.0, |total, part| total * 60.0 + part.parse::<f64>().unwrap_or(0.0))
}

/// Cores kept busy while `work` ran, and how long it took.
async fn busy<T>(work: impl Future<Output = T>) -> (T, Duration, f64) {
    let (started, cpu) = (Instant::now(), cpu_s());
    let out = work.await;
    let took = started.elapsed();
    (out, took, (cpu_s() - cpu) / took.as_secs_f64().max(1e-9))
}

async fn idle(what: &str, span: Duration) {
    let (_, _, cores) = busy(tokio::time::sleep(span)).await;
    println!("{what:<44} {cores:>8.3} cores");
}

fn new_system(max_actors: usize, capacity: usize) -> ActorSystem {
    let runtime = RuntimeConfig { max_actors, ..Default::default() };
    let timings = std::env::var("AXON_TIMINGS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    ActorSystem::builder()
        .runtime(runtime)
        .mailbox(MailboxLimits { messages: capacity, ..Default::default() })
        .timing_sample_every(timings)
        .build()
        .unwrap()
}

fn cluster_config(id: &str, discovery: &MemoryDiscovery, peers: usize) -> ClusterConfig {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"scale-secret-01234567").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.max_peers = peers.max(cfg.max_peers);
    cfg.max_spawned = 1 << 20;
    cfg.max_control_handlers = 64;
    // All nodes dial from one address here.
    cfg.accept.max_handshakes_per_ip = peers.max(cfg.accept.max_handshakes_per_ip);
    cfg
}

async fn in_parallel<F, Fut>(jobs: usize, width: usize, job: F)
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    let job = Arc::new(job);
    let mut workers = JoinSet::new();
    for worker in 0..width {
        let job = job.clone();
        workers.spawn(async move {
            for i in (worker..jobs).step_by(width) {
                job(i).await;
            }
        });
    }
    workers.join_all().await;
}

async fn actors(count: usize, capacity: usize) {
    let system = new_system(count + 1024, capacity);
    let handled = Arc::new(AtomicU64::new(0));
    let (rss, virt) = (rss_mb(), virtual_mb());
    let started = Instant::now();
    let mut all: Vec<ActorRef<Wide>> = Vec::with_capacity(count);
    for _ in 0..count {
        all.push(system.spawn(Idle(handled.clone())).start().unwrap().address);
    }
    let took = started.elapsed();
    println!("{count} actors, mailbox capacity {capacity}, {} B messages", size_of::<Wide>());
    println!("{:<44} {:>8.0} /s", "spawn, one at a time", count as f64 / took.as_secs_f64());
    let per = |mb: f64| mb * 1024.0 * 1024.0 / count as f64;
    println!(
        "{:<44} {:>8.0} B resident, {:.0} B virtual",
        "per idle actor",
        per(rss_mb() - rss),
        per(virtual_mb() - virt)
    );

    let more = (count / 10).max(1);
    let overlapped_system = new_system(more + 1024, capacity);
    let started = Instant::now();
    in_parallel(more, 64, {
        let (system, handled) = (overlapped_system.clone(), handled.clone());
        move |_| {
            let (system, handled) = (system.clone(), handled.clone());
            async move { drop(system.spawn(Idle(handled)).start().unwrap()) }
        }
    })
    .await;
    println!("{:<44} {:>8.0} /s", "spawn, 64 starts in flight", more as f64 / started.elapsed().as_secs_f64());
    drop(overlapped_system);

    let started = Instant::now();
    all.iter().for_each(|a| _ = a.try_tell([0; 8]).unwrap());
    while handled.load(Relaxed) < count as u64 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!("{:<44} {:>8.0} /s", "one message to each", count as f64 / started.elapsed().as_secs_f64());
    println!("{:<44} {:>8.0} B resident", "per actor once it has handled a message", per(rss_mb() - rss));

    idle("idle, no telemetry", Duration::from_secs(5)).await;
    let cluster = Cluster::start(&system, cluster_config("solo", &MemoryDiscovery::new(), 0)).await.unwrap();
    let cfg = TelemetryConfig { scan_limit: 1 << 20, ..Default::default() };
    let _telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    idle("idle, telemetry every 1 s, scanning them all", Duration::from_secs(10)).await;
    println!("{:<44} {:>8.0} MB", "resident at the end", rss_mb());
}

/// Where the acceptors' lease logs go, removed at the end.
fn lease_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("axon-scale-{}", std::process::id()))
}

/// An acceptor's lease log on the file system, as one runs, so that the rows count its writes.
fn lease_log(id: &str) -> Arc<axon::files::OsFiles> {
    Arc::new(axon::files::OsFiles::new(lease_dir().join(id)))
}

async fn keyed(keys: usize, lease: Duration) {
    let discovery = MemoryDiscovery::new();
    let leases = MemoryLeases::new();
    let new_leases = |id: &str| {
        let mut cfg = PaxosConfig::new(["host", "client", "witness"]);
        cfg.max_leases = keys.max(cfg.max_leases);
        cfg.store = Some(lease_log(id));
        Arc::new(PaxosLeases::new(cfg).unwrap())
    };
    let host_leases = paxos().map(|_| new_leases("host"));
    let host_system = new_system(keys + 1024, 64);
    let mut cfg = cluster_config("host", &discovery, 0);
    cfg.ownership = Some(match &host_leases {
        Some(leases) => leases.clone(),
        None => Arc::new(Slow(leases.authority(), lease)),
    });
    cfg.max_pending = 1 << 16;
    let host = Cluster::start(&host_system, cfg).await.unwrap();
    host.kind::<EchoApi>("session").register(|_, _| Ok(Echo)).await.unwrap();

    let mut others = Vec::new();
    let ids: &[&str] = if host_leases.is_some() { &["client", "witness"] } else { &["client"] };
    for id in ids {
        let system = new_system(1024, 64);
        let mut cfg = cluster_config(id, &discovery, 0);
        cfg.max_pending = 1 << 16;
        cfg.ownership = host_leases.as_ref().map(|_| new_leases(id) as Arc<dyn OwnershipAuthority>);
        others.push((Cluster::start(&system, cfg).await.unwrap(), system));
    }
    let client = others[0].0.clone();
    until("membership", || client.view().members.len() > others.len()).await;
    until("warm-up", || !std::iter::once(&host).chain(others.iter().map(|(c, _)| c)).any(Cluster::warming)).await;
    let started = Instant::now();
    in_parallel(keys, 256, move |i| {
        let client = client.clone();
        async move {
            client
                .get_or_spawn::<EchoApi>("session", &format!("k{i}"), Bytes::new(), &Placement::default())
                .await
                .unwrap();
        }
    })
    .await;
    let rate = keys as f64 / started.elapsed().as_secs_f64();
    let how = if host_leases.is_some() { "Paxos leases".into() } else { format!("lease {lease:?}") };
    println!("{keys} keyed spawns on one node, 256 at a time, {how}: {rate:.0} /s");
    if let Some(leases) = host_leases {
        let mut took = Vec::new();
        while took.len() < 5 {
            let started = Instant::now();
            match leases.renew().await {
                Ok(_) => took.push(started.elapsed()),
                // The cluster's own renewal is out.
                Err(OwnershipError::Unavailable(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(e) => panic!("{e}"),
            }
        }
        took.sort();
        println!("renewing those {keys} leases, median of 5: {:.1?}", took[2]);
    }
}

struct Node {
    system: ActorSystem,
    cluster: Cluster,
    telemetry: Option<Telemetry>,
}

/// `AXON_CONSUL=http://host:port` puts discovery and keyed-actor leases on that agent, under a service name of this run.
fn consul() -> Option<ConsulConfig> {
    static SERVICE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let service = SERVICE.get_or_init(|| format!("axon-scale-{}", std::process::id()));
    Some(ConsulConfig::new(std::env::var("AXON_CONSUL").ok()?, service.as_str()))
}

fn paxos() -> Option<usize> {
    std::env::var("AXON_PAXOS").ok()?.parse().ok()
}

static SEEDS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

/// Every node dials from one address here, so the per-address handshake cap has to go.
async fn seed_servers(count: usize, nodes: usize) -> Vec<SeedServer> {
    let accept = AcceptLimits { max_handshakes_per_ip: nodes + 64, ..Default::default() };
    let mut servers = Vec::new();
    for i in 0..count {
        let transport = cluster_config(&format!("seed-{i}"), &MemoryDiscovery::new(), 0).transport;
        let cfg = SeedServerConfig::new("127.0.0.1:0".parse().unwrap(), &transport);
        servers.push(SeedServer::bind(SeedServerConfig { accept: accept.clone(), ..cfg }).await.unwrap());
    }
    if count > 0 {
        SEEDS.set(servers.iter().map(|s| s.local_addr().to_string()).collect()).unwrap();
    }
    servers
}

fn seed_report(servers: &[SeedServer]) {
    for stats in servers.iter().map(SeedServer::stats) {
        println!(
            "  seed: {} sessions, {} registered, {} observers, list {} B, refusals {:?}, listener {:?}",
            stats.sessions, stats.registered, stats.observers, stats.list_bytes, stats.refusals, stats.listener
        );
    }
}

async fn node(id: String, discovery: MemoryDiscovery, peers: usize, bare: bool) -> Node {
    let runtime = RuntimeConfig { workers: 1, ..Default::default() };
    let system = ActorSystem::builder().runtime(runtime).build().unwrap();
    let mut cfg = cluster_config(&id, &discovery, peers);
    if let Some(consul) = consul() {
        cfg.discovery = Arc::new(ConsulDiscovery::new(consul.clone()).unwrap());
        cfg.ownership = Some(Arc::new(ConsulLeases::new(consul, id.as_str()).unwrap()));
    }
    if let Some(acceptors) = paxos() {
        let mut paxos = PaxosConfig::new((0..acceptors).map(|i| format!("n{i:04}")));
        paxos.store = Some(lease_log(&id));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    }
    if let Some(seeds) = SEEDS.get() {
        cfg.discovery = Arc::new(SeedDiscovery::start(SeedConfig::new(seeds, &cfg.transport)).await.unwrap());
        // With seeds, as guide/cluster.md § Seed servers asks.
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
    }
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let cfg = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    let telemetry = if bare { None } else { Some(Telemetry::start(&system, &cluster, cfg).await.unwrap()) };
    let echo = system.spawn(Echo).name("echo").start().unwrap();
    cluster.publish::<EchoApi>(&echo, "echo").unwrap();
    cluster.kind::<EchoApi>("keyed").register(|_, _| Ok(Echo)).await.unwrap();
    Node { system, cluster, telemetry }
}

async fn all_see(nodes: &[Node], members: usize) {
    while nodes.iter().any(|n| n.cluster.view().members.len() != members) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Samples in each node's newest batch: what one sampling interval adds to the metrics store.
/// Rows in each node's last batch, and those batches' size as stored.
async fn rows(nodes: &[Node]) -> String {
    let (mut rows, mut bytes) = (0, 0);
    for n in nodes.iter().filter(|n| n.telemetry.is_some()) {
        let page = fetch_samples(&nodes[0].cluster, n.cluster.identity().node_id.as_str(), None).await.unwrap();
        if let Some(batch) = page.batches.last() {
            rows += batch.samples.len();
            bytes += postcard::to_stdvec(batch).unwrap().len();
        }
    }
    format!("{rows} in {:.1} KB", bytes as f64 / 1e3)
}

async fn nodes(count: usize, links: usize, keys: usize, bare: bool) {
    let discovery = MemoryDiscovery::new();
    let peers = count + 8;
    let rss = rss_mb();
    let seeds = std::env::var("AXON_SEEDS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let servers = seed_servers(seeds, count).await;
    let (mut all, took, cores) = busy(async {
        let mut starting = JoinSet::new();
        (0..count).for_each(|i| drop(starting.spawn(node(format!("n{i:04}"), discovery.clone(), peers, bare))));
        let all = starting.join_all().await;
        all_see(&all, count).await;
        all
    })
    .await;
    all.sort_by(|a, b| a.cluster.identity().node_id.as_str().cmp(b.cluster.identity().node_id.as_str()));
    println!("{count} nodes started and agreed in {took:.2?}, {cores:.2} cores");
    seed_report(&servers);
    println!("{:<44} {:>8.2} MB", "resident per node", (rss_mb() - rss) / count as f64);
    idle("idle, no links", Duration::from_secs(10)).await;
    println!("{:<44} {:>8}", "metric rows per second, no links", rows(&all).await);

    let shared = Arc::new(all);
    let (_, took, cores) = busy(in_parallel(count, 64, {
        let all = shared.clone();
        move |i| {
            let all = all.clone();
            async move {
                for step in 1..=links.min(all.len() - 1) {
                    let peer = all[(i + step) % all.len()].cluster.identity().node_id.as_str();
                    let echo = all[i].cluster.resolve_on::<EchoApi>(peer, "echo").await.unwrap();
                    echo.request(Bytes::from_static(b"hi")).await.unwrap();
                }
            }
        }
    }))
    .await;
    println!("{} links opened in {took:.2?}, {cores:.2} cores", count * links.min(count - 1));
    println!("{:<44} {:>8.2} MB", "resident per node", (rss_mb() - rss) / count as f64);
    tokio::time::sleep(Duration::from_secs(3)).await;
    idle(&format!("idle, {links} links per node"), Duration::from_secs(10)).await;
    println!("{:<44} {:>8}", "metric rows per second, with links", rows(&shared).await);

    let (joined, took, cores) = busy(async {
        let joined = node("x-join".into(), discovery.clone(), peers, bare).await;
        all_see(&shared, count + 1).await;
        joined
    })
    .await;
    println!("a join seen by every node in {took:.2?}, {cores:.2} cores");
    let (_, took, cores) = busy(async {
        joined.system.shutdown().await.unwrap();
        drop(joined);
        all_see(&shared, count).await;
    })
    .await;
    println!("a leave seen by every node in {took:.2?}, {cores:.2} cores");

    let rss = rss_mb();
    let (_, took, cores) = busy(in_parallel(keys, 256, {
        let all = shared.clone();
        move |i| {
            let all = all.clone();
            async move {
                let from = &all[0].cluster;
                from.get_or_spawn::<EchoApi>("keyed", &format!("k{i}"), Bytes::new(), &Placement::default())
                    .await
                    .unwrap();
            }
        }
    }))
    .await;
    println!("{keys} keyed actors placed at {:.0} /s, {cores:.2} cores", keys as f64 / took.as_secs_f64());
    println!("{:<44} {:>8.0} B resident", "per keyed actor", (rss_mb() - rss) * 1024.0 * 1024.0 / keys as f64);
    idle("idle, keyed actors placed", Duration::from_secs(5)).await;
    let handoffs =
        |all: &[Node]| all.iter().map(|n| n.cluster.stats()).map(|s| s.handoffs + s.handoffs_failed).sum::<u64>();
    let before = handoffs(&shared);
    let (rebalanced, took, cores) = busy(async {
        let joined = node("x-rebalance".into(), discovery.clone(), peers, bare).await;
        all_see(&shared, count + 1).await;
        let mut moved = (u64::MAX, Instant::now());
        while moved.1.elapsed() < Duration::from_secs(2) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if handoffs(&shared) != moved.0 {
                moved = (handoffs(&shared), Instant::now());
            }
        }
        joined
    })
    .await;
    let moved = handoffs(&shared) - before;
    println!(
        "a join that moved {moved} keyed actors settled in {:.2?}, {cores:.2} cores",
        took - Duration::from_secs(2)
    );
    println!("{:<44} {:>8.0} MB", "resident at the end", rss_mb());

    // A rolling restart: a tenth of the nodes, one after another, each back under its own name.
    rebalanced.system.shutdown().await.unwrap();
    drop(rebalanced);
    let mut all = Arc::try_unwrap(shared).ok().expect("no job still holds the nodes");
    all_see(&all, count).await;
    // A restarted node starts counting again, so only the others are counted.
    let restarts = (count / 10).max(1);
    let before = handoffs(&all[restarts..]);
    let (_, took, cores) = busy(async {
        for i in 0..restarts {
            let id = all[i].cluster.identity().node_id.as_str().to_string();
            let old = all.remove(i);
            old.system.shutdown().await.unwrap();
            // Dropped before the new boot: an acceptor's lease log is locked while its store lives.
            drop(old);
            // A write the old store started can still be landing on the blocking pool, and it holds the lock.
            if paxos().is_some() {
                let freed = Instant::now() + Duration::from_secs(10);
                while let Err(e) = axon::files::Files::list(&*lease_log(&id)).await {
                    assert!(
                        e.kind() == std::io::ErrorKind::ResourceBusy && Instant::now() < freed,
                        "{id}'s lease log: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            all.insert(i, node(id.clone(), discovery.clone(), peers, bare).await);
            // The count is the same before and after, so the new boot is what has to be seen.
            let boot = all[i].cluster.identity().incarnation;
            while all.iter().any(|n| n.cluster.view().get(&id).is_none_or(|m| m.record.incarnation != boot)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // An acceptor answers again only after its warm-up; stopping the next one sooner loses the majority.
            while all[i].cluster.warming() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await;
    println!(
        "{restarts} nodes restarted one by one in {took:.2?} ({:.2?} each), {cores:.2} cores",
        took / restarts as u32
    );
    idle("idle after the restarts", Duration::from_secs(10)).await;
    println!("{:<44} {:>8}", "keyed actors the other nodes moved back", handoffs(&all[restarts..]) - before);
    let lost: u64 = all[restarts..].iter().map(|n| n.cluster.stats().leases_lost).sum();
    println!("{:<44} {:>8}", "leases the other nodes lost", lost);
    seed_report(&servers);
}

struct Site {
    stream: StreamNode,
    cluster: Cluster,
    _system: ActorSystem,
}

async fn site(id: &str, role: &str, discovery: &MemoryDiscovery, streams: usize, fanout: usize) -> Site {
    let tier = |role: &str| std::collections::BTreeMap::from([("role".to_owned(), role.to_owned())]);
    let mut cfg = cluster_config(id, discovery, 64);
    cfg.labels = tier(role);
    let system = new_system(1024, 1024);
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = match role {
        "origin" => vec![],
        "relay" => vec![tier("origin")],
        _ => vec![tier("relay"), tier("origin")],
    };
    let mut cfg = StreamNodeConfig::new(upstream);
    let hub = &mut cfg.core.hub;
    (hub.max_streams, hub.max_interest_keys, hub.max_subscriptions) =
        (streams + 16, streams + 16, streams * fanout + 16);
    (hub.max_queued_bytes, hub.max_queued_records) = (4 << 30, 64_000_000);
    cfg.core.store.max_streams = streams + 16;
    cfg.core.rings = vec![RingLimits { namespace: "md".into(), max_records: 64, max_bytes: 16 << 10 }];
    let stream = StreamNode::start(&cluster, cfg).await.unwrap();
    Site { stream, cluster, _system: system }
}

fn stream_key(k: usize) -> StreamKey {
    StreamKey::new("md", format!("binancef.sym{k:05}/usd.1m"))
}

async fn until(what: &str, mut done: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < Duration::from_secs(300), "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    started.elapsed()
}

/// One origin, relays under it and leaves under those, all pulling many small streams.
async fn streams(count: usize, relays: usize, leaves: usize, rate: usize, one_in: usize) {
    let discovery = MemoryDiscovery::new();
    let origin = site("origin", "origin", &discovery, count, relays + leaves).await;
    let mut relay_sites = Vec::new();
    for i in 0..relays {
        relay_sites.push(site(&format!("relay-{i}"), "relay", &discovery, count, leaves + 1).await);
    }
    let mut leaf_sites = Vec::new();
    for i in 0..leaves {
        leaf_sites.push(Arc::new(site(&format!("leaf-{i}"), "leaf", &discovery, count, 2).await));
    }
    let members = 1 + relays + leaves;
    until("membership", || leaf_sites.iter().all(|l| l.cluster.view().members.len() == members)).await;

    let (_, took, _) = busy(async {
        for k in 0..count {
            origin.stream.open_stream(&stream_key(k), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
        }
    })
    .await;
    println!("{count} streams opened on the origin in {took:.2?}, rss {:.0} MB", rss_mb());

    let delivered = Arc::new(AtomicU64::new(0));
    let resets = Arc::new(AtomicU64::new(0));
    let listen = |leaf: &Arc<Site>, k: usize| {
        let mut subscription = leaf.stream.subscribe(&stream_key(k), SubscribeOptions::default()).unwrap();
        let (delivered, resets) = (delivered.clone(), resets.clone());
        tokio::spawn(async move {
            while let Some(event) = subscription.next().await {
                match event {
                    StreamEvent::Batch(batch) => drop(delivered.fetch_add(batch.len() as u64, Relaxed)),
                    StreamEvent::ResetRequired(_) => drop(resets.fetch_add(1, Relaxed)),
                    _ => {}
                }
            }
        })
    };
    let wants = |leaf: usize, k: usize| (k + leaf).is_multiple_of(one_in);
    let sum = |sites: &mut dyn Iterator<Item = RelayStats>, field: fn(&RelayStats) -> u64| {
        sites.map(|s| field(&s)).sum::<u64>()
    };
    let below = || relay_sites.iter().map(|s| s.stream.stats()).chain(leaf_sites.iter().map(|s| s.stream.stats()));
    // Branches held for leaves, and the origin serving exactly what the relays pull: interests answered, not just sent.
    let fed = |relay_sites: &[Site]| {
        let (origin, relays) =
            (origin.stream.stats(), relay_sites.iter().map(|r| r.stream.stats()).collect::<Vec<_>>());
        // The origin holds a dead relay's branches until a push to it has failed for long enough.
        let live: Vec<String> = relay_sites.iter().map(|r| r.cluster.identity().node_id.clone()).collect();
        let to_leaves = |s: &RelayStats| {
            s.downstream.iter().filter(|d| d.node.starts_with("leaf")).map(|d| d.branches).sum::<usize>()
        };
        let pulled: usize = relays.iter().map(|r| r.feeds).sum();
        let served: usize = origin.downstream.iter().filter(|d| live.contains(&d.node)).map(|d| d.branches).sum();
        if pulled == served { relays.iter().map(to_leaves).sum::<usize>() + to_leaves(&origin) } else { usize::MAX }
    };
    let mut tasks = std::collections::HashMap::new();
    let (wanted, took, cores) = busy(async {
        let mut wanted = 0;
        for (i, leaf) in leaf_sites.iter().enumerate() {
            for k in (0..count).filter(|k| wants(i, *k)) {
                tasks.insert((i, k), listen(leaf, k));
                wanted += 1;
            }
        }
        until("a branch for every subscription", || fed(&relay_sites) == wanted).await;
        wanted
    })
    .await;
    let (sent, keys) = (sum(&mut below(), |s| s.interests_sent), sum(&mut below(), |s| s.interest_keys));
    println!(
        "{wanted} subscriptions up in {took:.2?} at {cores:.2} cores: {sent} interests carrying {keys} keys, rss {:.0} MB",
        rss_mb()
    );
    let pulled: usize = relay_sites.iter().map(|r| r.stream.stats().feeds).sum();
    let served: usize = origin.stream.stats().downstream.iter().map(|d| d.branches).sum();
    println!("relays pull {pulled} streams, the origin serves {served} branches");

    let before = sum(&mut below(), |s| s.interests_sent);
    idle("idle with every stream pulled", Duration::from_secs(10)).await;
    println!(
        "interests while idle: {} in 10 s, {} resyncs so far",
        sum(&mut below(), |s| s.interests_sent) - before,
        sum(&mut below(), |s| s.resyncs)
    );

    let payload = Bytes::from(vec![7u8; 128]);
    let publish = |seconds: u64| {
        let (origin, payload) = (origin.stream.clone(), payload.clone());
        async move {
            let (started, mut sent, mut k) = (Instant::now(), 0u64, 0);
            while started.elapsed() < Duration::from_secs(seconds) {
                let due = (started.elapsed().as_secs_f64() * rate as f64) as u64;
                while sent < due {
                    sent += origin.publish(&stream_key(k % count), vec![payload.clone()]).is_ok() as u64;
                    k += 1;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            sent
        }
    };
    // Round-robin from stream 0: the first `sent % count` streams get one record more.
    let expect = |tasks: &std::collections::HashMap<(usize, usize), tokio::task::JoinHandle<()>>, sent: u64| {
        let (each, rest) = (sent / count as u64, (sent % count as u64) as usize);
        tasks.keys().map(|(_, k)| each + (*k < rest) as u64).sum::<u64>()
    };
    let (sent, took, cores) = busy(publish(10)).await;
    let expected = expect(&tasks, sent);
    let late = until("deliveries", || delivered.load(Relaxed) >= expected || resets.load(Relaxed) > 0).await;
    println!(
        "published {:.0} records/s over {count} streams for {took:.1?} at {cores:.2} cores: {} of {expected} delivered, last {late:.2?} after, {} resets",
        sent as f64 / took.as_secs_f64(),
        delivered.load(Relaxed),
        resets.load(Relaxed)
    );

    let before = (sum(&mut below(), |s| s.interests_sent), sum(&mut below(), |s| s.interest_keys));
    let (moved, took, cores) = busy(async {
        let (started, mut moved, mut k) = (Instant::now(), 0u64, 0usize);
        while started.elapsed() < Duration::from_secs(10) {
            for _ in 0..50 {
                let (i, k1) = (k % leaves, (k * 7919 + k / leaves) % count);
                match tasks.remove(&(i, k1)) {
                    Some(task) => task.abort(),
                    None => drop(tasks.insert((i, k1), listen(&leaf_sites[i], k1))),
                }
                (moved, k) = (moved + 1, k + 1);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        moved
    })
    .await;
    let after = (sum(&mut below(), |s| s.interests_sent), sum(&mut below(), |s| s.interest_keys));
    println!(
        "{moved} subscriptions changed in {took:.1?} at {cores:.2} cores: {} interests carrying {} keys, {} resyncs so far",
        after.0 - before.0,
        after.1 - before.1,
        sum(&mut below(), |s| s.resyncs)
    );
    let settled = until("dropped streams to outlast their linger", || fed(&relay_sites) == tasks.len()).await;
    println!("{} subscriptions left, the last drop applied {settled:.1?} later", tasks.len());

    let gone = relay_sites.remove(0);
    let (_, took, cores) = busy(async {
        drop(gone);
        until("a branch for every subscription again", || fed(&relay_sites) == tasks.len()).await
    })
    .await;
    let resyncs: u64 = relay_sites
        .iter()
        .map(|s| s.stream.stats())
        .chain(leaf_sites.iter().map(|s| s.stream.stats()))
        .map(|s| s.resyncs)
        .sum();
    println!(
        "a relay lost: every subscription fed by another parent after {took:.2?} at {cores:.2} cores, {resyncs} resyncs, rss {:.0} MB",
        rss_mb()
    );
    let before = delivered.load(Relaxed);
    let (sent, _, _) = busy(publish(3)).await;
    let expected = expect(&tasks, sent);
    let late = until("deliveries", || delivered.load(Relaxed) - before >= expected || resets.load(Relaxed) > 0).await;
    println!(
        "after that: {sent} published, {} of {expected} delivered, last {late:.2?} after, {} resets in all",
        delivered.load(Relaxed) - before,
        resets.load(Relaxed)
    );
}

/// Many local subscribers on one node, every publish waking all of a stream's subscribers at once.
async fn subscribers(subscriptions: usize, count: usize, rate: usize, records: usize) {
    let discovery = MemoryDiscovery::new();
    let origin = site("origin", "origin", &discovery, count, subscriptions.div_ceil(count)).await;
    for k in 0..count {
        origin.stream.open_stream(&stream_key(k), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    // A counter per subscriber: one shared by all of them would be the hottest line in the process.
    let counts: Arc<Vec<[AtomicU64; 2]>> = Arc::new((0..subscriptions).map(|_| Default::default()).collect());
    for i in 0..subscriptions {
        let mut subscription = origin.stream.subscribe(&stream_key(i % count), SubscribeOptions::default()).unwrap();
        let counts = counts.clone();
        tokio::spawn(async move {
            while let Some(event) = subscription.next().await {
                match event {
                    StreamEvent::Batch(batch) => drop(counts[i][0].fetch_add(batch.len() as u64, Relaxed)),
                    StreamEvent::ResetRequired(_) => drop(counts[i][1].fetch_add(1, Relaxed)),
                    _ => {}
                }
            }
        });
    }
    let total = |at: usize| counts.iter().map(|c| c[at].load(Relaxed)).sum::<u64>();
    idle(&format!("idle with {subscriptions} subscriptions"), Duration::from_secs(3)).await;
    let (batch, publisher) = (vec![Bytes::from(vec![7u8; 128]); records], origin.stream.clone());
    // On a worker, as an actor or a push from upstream would be, so wakes go to its local run queue.
    let (sent, took, cores) = busy(tokio::spawn(async move {
        let (started, mut sent) = (Instant::now(), 0usize);
        while started.elapsed() < Duration::from_secs(10) {
            let due = (started.elapsed().as_secs_f64() * rate as f64) as usize;
            for k in sent..due {
                publisher.publish(&stream_key(k % count), batch.clone()).unwrap();
            }
            sent = sent.max(due);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        sent
    }))
    .await;
    let sent = sent.unwrap();
    // Round-robin from stream 0: the first `sent % count` streams get one batch more.
    let expected: u64 = (0..subscriptions).map(|i| (sent / count + usize::from(i % count < sent % count)) as u64).sum();
    let expected = expected * records as u64;
    let cpu = cpu_s();
    let late = until("deliveries", || total(0) >= expected || total(1) > 0).await;
    let (done, spent) = (total(0), took.as_secs_f64() * cores + cpu_s() - cpu);
    println!(
        "{:.0} batches/s of {records} to {subscriptions} subscribers on {count} streams for {took:.1?}: {:.2} M records/s delivered, {:.0} ns processor time per record, {cores:.2} cores while publishing",
        sent as f64 / took.as_secs_f64(),
        done as f64 / (took + late).as_secs_f64() / 1e6,
        spent * 1e9 / done.max(1) as f64,
    );
    println!("{done} of {expected} delivered, last {late:.2?} after, {} resets", total(1));
}

/// A batch shaped like the playground's average: 147 samples, 1.6 labels each, 35 histograms with 21 non-zero buckets
/// over a run of 35. It goes through JSON, as a received batch owns its strings.
fn playground_batch() -> String {
    let samples = (0..147).map(|i| {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("actor".into(), format!("worker-{}", i % 40));
        if i % 5 < 3 {
            labels.insert("peer".into(), format!("node-{}", i % 9));
        }
        let value = if i < 35 {
            let mut h = axon::Histogram::default();
            for j in (0..35).filter(|j| j % 5 != 1 && j % 5 != 3) {
                h.buckets[100 + j] = (i + j) as u64 + 1;
            }
            (h.count, h.sum_ns) = (h.buckets.iter().sum(), 1 << 30);
            Value::Histogram((&h).into())
        } else if i % 2 == 0 {
            Value::Counter(i as u64)
        } else {
            Value::Gauge(i as f64)
        };
        Sample { name: format!("axon.metric_{}.value", i % 60).into(), unit: "ns".into(), labels, value }
    });
    let samples = samples.collect();
    let batch = SampleBatch {
        node_id: "n1".into(),
        incarnation: "0123456789abcdef".into(),
        sequence: 0,
        timestamp_ms: 0,
        samples,
    };
    serde_json::to_string(&batch).expect("a batch serializes")
}

fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    Some(line.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

async fn metrics(mib: usize) {
    let json = playground_batch();
    let store = MemoryMetrics::new(mib << 20);
    let (mut sequence, mut writing) = (0, Duration::ZERO);
    let mut write = async || {
        let mut batch: SampleBatch = serde_json::from_str(&json).expect("the batch reads back");
        sequence += 1;
        (batch.sequence, batch.timestamp_ms) = (sequence, sequence);
        let start = Instant::now();
        store.write(&[batch]).await.expect("a valid batch");
        writing += start.elapsed();
    };
    let base = rss_bytes();
    while store.usage().expect("bounded").evicted_batches == 0 {
        write().await;
    }
    let full = store.usage().expect("bounded");
    for _ in 0..3 * full.held_batches {
        write().await;
    }
    let usage = store.usage().expect("bounded");
    let grown = base.zip(rss_bytes()).map(|(base, now)| now.saturating_sub(base));
    println!(
        "bound {mib} MiB: {} batches held, {} bytes a batch, {} evicted; write {:.1} us a batch",
        usage.held_batches,
        usage.held_bytes / usage.held_batches.max(1),
        usage.evicted_batches,
        writing.as_secs_f64() * 1e6 / sequence as f64,
    );
    match grown {
        Some(grown) => println!(
            "RSS grew {grown} bytes, {:.3} of held_bytes {}",
            grown as f64 / usage.held_bytes as f64,
            usage.held_bytes
        ),
        None => println!("RSS: no /proc/self/status here"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let number = |at: usize, default: usize| args.get(at).and_then(|a| a.parse().ok()).unwrap_or(default);
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    match args.first().map(String::as_str) {
        Some("actors") => runtime.block_on(actors(number(1, 1_000_000), number(2, 1024))),
        Some("keyed") => runtime.block_on(keyed(number(1, 5000), Duration::from_millis(number(2, 1) as u64))),
        Some("nodes") => runtime.block_on(nodes(number(1, 200), number(2, 20), number(3, 20_000), args.len() > 4)),
        Some("streams") => runtime.block_on(streams(
            number(1, 50_000),
            number(2, 3),
            number(3, 6),
            number(4, 200_000),
            number(5, 2).max(1),
        )),
        Some("subscribers") => {
            runtime.block_on(subscribers(number(1, 20_000), number(2, 2).max(1), number(3, 200), number(4, 1).max(1)))
        }
        Some("metrics") => runtime.block_on(metrics(number(1, 256))),
        _ => eprintln!(
            "usage: scale actors|keyed|nodes|streams|subscribers|metrics [..]; see the head of examples/scale.rs"
        ),
    }
    let _ = std::fs::remove_dir_all(lease_dir());
}
