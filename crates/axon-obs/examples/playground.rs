//! A small cluster in one process with the dashboard on http://127.0.0.1:<port>.
//! usage: playground [nodes=6] [port=8080] [base_path=/] [churn]
//! `AXON_CONSUL` swaps the in-memory discovery for a Consul agent; `AXON_CLICKHOUSE` (with `_USER` and
//! `_PASSWORD`, feature `clickhouse`) swaps the in-memory history for ClickHouse. See `compose.yml`.
//! `AXON_PAXOS=3` gives the nodes Paxos leases, the first that many serving as acceptors, each logging its leases under
//! `$TMPDIR/axon-playground-<port>/<node>-leases`.
//! `AXON_DASHBOARD_PASSWORD` makes the console ask `admin` to sign in with it; `AXON_DASHBOARD_IDLE` is the
//! session's idle time in seconds. Publishers keep their stream on disk under `$TMPDIR/axon-playground-<port>`.
//! `AXON_KEYS=n` places n keyed actors in place of the 21 it places by default: `account` 60 % (`acct-eu-000123`, with
//! `-us-` and `-ap-`), `session` 30 % (`sess-000123`), and `device` 10 % (`dev-tokyo-000123`, `dev-reykjavik-…`),
//! registered on the nodes with a region and placed by it. It raises the limits on actors, spawns and leases to fit n,
//! starts 256 at a time and prints its progress.
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorHandle, ActorSystem, Context, Execution, Lane, PoolConfig, RuntimeConfig, TraceContext};
use axon_cluster::{
    AccessPolicy, ActorProtocol, CallError, Cluster, ClusterConfig, ConsulConfig, ConsulDiscovery, ConsulLeases,
    Discovery, MemoryDiscovery, PaxosConfig, PaxosLeases, Permits, QuotaConfig, Quotas, Rate, Rejection, Reply,
    WireProtocol,
};
use axon_net::SharedSecret;
use axon_obs::*;
use axon_stream::{
    DeliveryPolicy, DiskConfig, OsFiles, Retention, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions,
};
use bytes::Bytes;

enum Msg {
    Work(Bytes, Option<Reply>),
    Tick,
}

/// Traced work is passed on to `next`, so a trace has a few hops to show.
struct Relay {
    handled: u64,
    next: Option<ActorHandle<Msg>>,
}

impl Actor for Relay {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.handled += 1;
        if let Msg::Work(body, reply) = msg {
            if let (Some(next), Some(_)) = (&self.next, TraceContext::current()) {
                let _ = next.tell(Msg::Work(body.clone(), None));
            }
            if let Some(reply) = reply {
                let _ = reply.send(body);
            }
        }
        Ok(())
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Work(..) => "work",
            Msg::Tick => "tick",
        }
    }
}

/// Fails on `true`, so its family restarts and the messages queued behind the failure become dead letters.
struct Flaky;

impl Actor for Flaky {
    type Message = bool;
    type Error = &'static str;

    fn on_message(&mut self, fail: bool, _: &mut Context<'_, Self>) -> Result<(), &'static str> {
        if fail { Err("poked to fail") } else { Ok(()) }
    }

    fn label(_: &bool) -> &'static str {
        "poke"
    }
}

struct RelayApi;

impl WireProtocol for RelayApi {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = 0x9e1a;
    const SCHEMA: u16 = 1;

    fn encode(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for RelayApi {
    type Mailbox = Msg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply>) -> Result<Msg, Rejection> {
        Ok(Msg::Work(body, reply))
    }
}

type Node = (ActorSystem, Cluster, Vec<axon::ActorHandle<Msg>>, StreamNode, Quotas);

/// Calls per tenant a second, and requests in flight to a peer, cluster-wide: enough traffic is refused to show.
const CALLS: Rate = Rate::new("calls/s", 40, Duration::from_secs(1));
const REQUESTS: Permits = Permits::new("requests", 2);

/// A raw endpoint on the Bulk lane that answers with nothing, for large transfers between nodes.
const BULK: &str = "playground.bulk";
const BULK_PROTOCOL: u64 = 0x9e1b;

const STREAM_RELAYS: [&str; 2] = ["relay-0", "relay-1"];
/// Nodes that only exist to listen to streams, on top of the `nodes` asked for.
const LISTENERS: [&str; 3] = ["consumer-0", "consumer-1", "direct-0"];

async fn start(name: &str, discovery: &Arc<dyn Discovery>, acceptors: &[String], disk: &Path, keys: usize) -> Node {
    let mut transport = axon_net::Config::new(name, SharedSecret::new(*b"playground-secret-0123").unwrap());
    transport.allow_plaintext = true;
    transport.timing_sample_every = 8;
    // Under the transfers below, so the console shows a Bulk connection that waits for its cap.
    transport.lanes.bulk.max_bytes_per_sec = Some(256 << 10);
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    cfg.discovery = discovery.clone();
    if let Ok(url) = std::env::var("AXON_CONSUL") {
        cfg.ownership = Some(Arc::new(ConsulLeases::new(ConsulConfig::new(url, "axon-playground"), name).unwrap()));
    }
    if !acceptors.is_empty() {
        let mut paxos = PaxosConfig::new(acceptors);
        paxos.max_leases = paxos.max_leases.max(keys + 1024);
        paxos.store = Some(Arc::new(OsFiles::new(disk.join(format!("{name}-leases")))));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    }
    // Room for every key on one node, and for the starts in flight.
    cfg.max_spawned = cfg.max_spawned.max(keys + 1024);
    cfg.max_endpoints = cfg.max_endpoints.max(keys + 1024);
    cfg.max_pending = cfg.max_pending.max(if keys > 0 { 1 << 16 } else { 0 });
    // Half the shipped 1 s, so the console shows a change sooner.
    cfg.poll_interval = Duration::from_millis(500);
    let role = name.split('-').next().unwrap_or_default();
    cfg.labels.insert("role".into(), role.into());
    // Two roles far apart get a flag in the console; the rest stay unplaced.
    let place = match role {
        "publisher" => Some(("JP", "Tokyo")),
        "relay" => Some(("IS", "Reykjavík")),
        _ => None,
    };
    if let Some((country, region)) = place {
        cfg.labels.insert("country".into(), country.into());
        cfg.labels.insert("region".into(), region.into());
    }
    if STREAM_RELAYS.contains(&name) {
        cfg.labels.insert("stream".into(), "relay".into());
    }
    let pools = vec![PoolConfig { name: "relays".into(), workers: 2, ..Default::default() }];
    let system = ActorSystem::builder()
        .timing_sample_every(8)
        .runtime(RuntimeConfig {
            pools,
            max_actors: RuntimeConfig::default().max_actors.max(keys + 1024),
            ..Default::default()
        })
        .build()
        .unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let accept = |_: &axon_net::Identity, _: Bytes, reply: Option<Reply>| {
        drop(reply.map(|reply| reply.send(Bytes::new())));
        Ok(())
    };
    cluster.endpoint(BULK, BULK_PROTOCOL, 1).lane(Lane::Bulk).serve(accept).unwrap();
    let region = place.map_or("", |(_, region)| region).into();
    let quotas = Quotas::start(&cluster, QuotaConfig { region, ..Default::default() }).await.unwrap();
    // Not on publishers or consumers: a registered kind is node metadata, and the e2e expects publisher-1 to have none.
    // The churning visitor hosts keys too, so the placement page shows them leave with it.
    if matches!(role, "console" | "relay" | "visitor") {
        let build = |_: &axon_cluster::SpawnContext, _| Ok(Relay { handled: 0, next: None });
        cluster.kind::<RelayApi>("account").register(build).await.unwrap();
        // With leases, sessions restart on a survivor when their node goes, without waiting for a caller.
        let session = cluster.kind::<RelayApi>("session");
        let session = if acceptors.is_empty() { session } else { session.eager(Bytes::new(), Default::default()) };
        session.register(build).await.unwrap();
    }
    if keys > 0 && place.is_some() {
        cluster.kind::<RelayApi>("device").register(|_, _| Ok(Relay { handled: 0, next: None })).await.unwrap();
    }
    let telemetry = TelemetryConfig {
        process_metrics: cfg!(feature = "system-metrics"),
        host_metrics: cfg!(feature = "system-metrics"),
        access: AccessPolicy::Cluster,
        ..Default::default()
    };
    let telemetry = Telemetry::start(&system, &cluster, telemetry).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&telemetry);
    println!("{name} metrics: http://{}/metrics", health.address());
    // Publishers originate streams and two relays carry them. Consumers share those relays and fall
    // back to the publishers; `direct` nodes always pull from the publishers themselves.
    let publishers = [("role".to_owned(), "publisher".to_owned())].into();
    let relays = [("stream".to_owned(), "relay".to_owned())].into();
    let upstream = match name.split('-').next() {
        Some("publisher") => vec![],
        Some("relay" | "direct") => vec![publishers],
        _ => vec![relays, publishers],
    };
    let mut stream_cfg = StreamNodeConfig::new(upstream);
    if role == "publisher" {
        // Small segments and a short age, so History on disk in the console shows retention within minutes.
        let keep = Retention { namespace: "playground".into(), max_bytes: 4 << 20, max_age: Duration::from_secs(600) };
        let mut disk = DiskConfig::new(Arc::new(OsFiles::new(disk.join(name))), vec![keep]);
        disk.segment_bytes = 1 << 20;
        stream_cfg.disk = Some(disk);
    }
    let streams = StreamNode::start(&cluster, stream_cfg).await.unwrap();
    telemetry.watch_streams(&streams);
    let mut actors: Vec<ActorHandle<Msg>> = Vec::new();
    for i in (0..8).rev() {
        let next = actors.last().filter(|_| i < 3).cloned();
        // The chain that carries the traffic runs on its own pool.
        let execution = if i < 3 { Execution::Pool("relays") } else { Execution::Shared };
        let relay =
            system.spawn(Relay { handled: 0, next }).name(format!("relay-{i}")).execution(execution).start().unwrap();
        if i == 0 {
            cluster.publish::<RelayApi>(&relay, "relay").unwrap();
        }
        actors.push(relay);
    }
    actors.reverse();
    (system, cluster, actors, streams, quotas)
}

/// The i-th of `AXON_KEYS`: account, session and device keys 6 : 3 : 1, devices 7 : 3 between Tokyo and Reykjavík.
fn nth_key(i: usize) -> (&'static str, String, axon_cluster::Placement) {
    let anywhere = axon_cluster::Placement::default();
    match i % 10 {
        0..=5 => ("account", format!("acct-{}-{i:06}", ["eu", "eu", "eu", "us", "us", "ap"][i % 6]), anywhere),
        6..=8 => ("session", format!("sess-{i:06}"), anywhere),
        _ if i % 100 < 70 => ("device", format!("dev-tokyo-{i:06}"), anywhere.require("region", "Tokyo")),
        _ => ("device", format!("dev-reykjavik-{i:06}"), anywhere.require("region", "Reykjavík")),
    }
}

/// Starts n keys from this node, 256 at a time, each asked again until it starts; prints progress every 5 s.
async fn place_keys(cluster: Cluster, n: usize) {
    let done = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(Mutex::new(None::<String>));
    let started = tokio::time::Instant::now();
    let width = n.min(256);
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..width {
        let (cluster, done, failed) = (cluster.clone(), done.clone(), failed.clone());
        workers.spawn(async move {
            for i in (worker..n).step_by(width) {
                let (kind, key, placement) = nth_key(i);
                // Started acceptors refuse for a lease's time; the next ask is the retry.
                while let Err(e) = cluster.get_or_spawn::<RelayApi>(kind, &key, Bytes::new(), &placement).await {
                    *failed.lock().unwrap() = Some(format!("{kind}/{key}: {e}"));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                done.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    let progress = tokio::spawn({
        let done = done.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let (at, secs) = (done.load(Ordering::Relaxed), started.elapsed().as_secs_f64());
                let last = failed.lock().unwrap().take().map(|e| format!(", last refusal {e}")).unwrap_or_default();
                println!("keys: {at} of {n} in {secs:.0} s, {:.0}/s{last}", at as f64 / secs);
            }
        }
    });
    workers.join_all().await;
    progress.abort();
    let secs = started.elapsed().as_secs_f64();
    println!("keys: all {n} placed in {secs:.1} s, {:.0}/s", n as f64 / secs);
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let nodes: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(6);
    let port: u16 = args.next().and_then(|a| a.parse().ok()).unwrap_or(8080);
    let base_path = args.next().unwrap_or_else(|| "/".into());
    let churn = args.next().is_some();
    let discovery: Arc<dyn Discovery> = match std::env::var("AXON_CONSUL") {
        Ok(url) => Arc::new(ConsulDiscovery::new(ConsulConfig::new(url, "axon-playground")).unwrap()),
        Err(_) => Arc::new(MemoryDiscovery::new()),
    };
    let names: Vec<String> = (0..nodes)
        .map(|i| match i {
            0 => "console-0".to_owned(),
            i if i % 2 == 1 => format!("relay-{}", i / 2),
            i => format!("publisher-{}", i / 2),
        })
        .chain(LISTENERS.map(str::to_owned))
        .collect();

    let paxos = std::env::var("AXON_PAXOS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let keyed: usize = std::env::var("AXON_KEYS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let acceptors = &names[..paxos.min(names.len())];
    // Each run starts its publishers' history afresh.
    let disk = std::env::temp_dir().join(format!("axon-playground-{port}"));
    let _ = std::fs::remove_dir_all(&disk);
    let mut running = Vec::new();
    for name in &names {
        running.push(start(name, &discovery, acceptors, &disk, keyed).await);
    }

    let (system, cluster, _, _, _) = &running[0];
    let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
    match std::env::var("AXON_CLICKHOUSE") {
        #[cfg(feature = "clickhouse")]
        Ok(url) => {
            let cfg = ClickHouseConfig {
                url,
                username: std::env::var("AXON_CLICKHOUSE_USER").unwrap_or("default".into()),
                password: std::env::var("AXON_CLICKHOUSE_PASSWORD").unwrap_or_default(),
                ..Default::default()
            };
            let provider = ClickHouseProvider::connect(cfg).await.expect("clickhouse");
            MetricsService::start(system, cluster, Arc::new(provider), metrics).await.unwrap();
        }
        #[cfg(not(feature = "clickhouse"))]
        Ok(_) => panic!("AXON_CLICKHOUSE needs --features clickhouse"),
        Err(_) => {
            MetricsService::start(system, cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await.unwrap();
        }
    }
    let mut cfg = DashboardConfig { bind: ([127, 0, 0, 1], port).into(), base_path, ..Default::default() };
    if let Ok(password) = std::env::var("AXON_DASHBOARD_PASSWORD") {
        cfg.users = vec![User { name: "admin".into(), hash: hash_password(&password).unwrap() }];
    }
    if let Some(idle) = std::env::var("AXON_DASHBOARD_IDLE").ok().and_then(|s| s.parse().ok()) {
        // The playground's own knob, so a short idle sign-out can be tried by hand.
        cfg.session.idle = Duration::from_secs(idle);
    }
    let dashboard = Dashboard::start(system, cluster, cfg).await.unwrap();
    println!("dashboard: http://{}", dashboard.address());

    // Keyed accounts spread over the nodes for the placement page, and one on the console's node, so with leases
    // the lease card it reports has one to show.
    let cluster = cluster.clone();
    if keyed > 0 {
        tokio::spawn(place_keys(cluster.clone(), keyed));
    } else {
        tokio::spawn(async move {
            let here = axon_cluster::Placement::default().require("role", "console");
            let accounts = (0..12).map(|i| ("account", format!("acct-{i}"), axon_cluster::Placement::default()));
            let sessions = (0..8).map(|i| ("session", format!("sess-{i}"), axon_cluster::Placement::default()));
            for (kind, key, placement) in accounts.chain([("account", "one".to_owned(), here)]).chain(sessions) {
                // Started acceptors refuse for a lease's time; the next ask is the retry.
                while cluster.get_or_spawn::<RelayApi>(kind, &key, Bytes::new(), &placement).await.is_err() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
    }

    // Each publisher feeds one stream; every other node listens to all of them.
    let keys: Vec<StreamKey> =
        names.iter().filter(|n| n.starts_with("publisher")).map(|n| StreamKey::new("playground", n.as_str())).collect();
    for (name, (_, _, _, streams, _)) in names.iter().zip(&running) {
        let streams = streams.clone();
        if name.starts_with("publisher") {
            let key = StreamKey::new("playground", name.as_str());
            streams.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
            tokio::spawn(async move {
                let mut round = 0usize;
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let trace = round.is_multiple_of(100).then(TraceContext::root);
                    axon::trace::enter(trace, || drop(streams.publish(&key, vec![Bytes::from(vec![b's'; 256]); 4])));
                    round = round.wrapping_add(1);
                }
            });
        } else if !name.starts_with("relay") {
            for key in keys.clone() {
                let streams = streams.clone();
                tokio::spawn(async move {
                    loop {
                        if let Ok(mut subscription) = streams.subscribe(&key, SubscribeOptions::default()) {
                            while subscription.next().await.is_some() {}
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
            }
        }
    }

    for (index, (_, cluster, actors, _, quotas)) in running.iter().enumerate() {
        let (cluster, names, local, quotas) =
            (cluster.clone(), names.clone(), actors[1].address.clone(), quotas.clone());
        tokio::spawn(async move {
            let payload = Bytes::from(vec![b'x'; 200 + 300 * index]);
            let mut round = 0usize;
            loop {
                tokio::time::sleep(Duration::from_millis(20 + 15 * index as u64)).await;
                round += 1;
                let _ = local.try_tell(Msg::Tick);
                let peer = &names[(index + 1 + round % 2) % names.len()];
                if quotas.take(format!("tenant-{}", round % 3), &[(CALLS, 1)]).is_err() {
                    continue;
                }
                let Ok(relay) = cluster.resolve_on::<RelayApi>(peer, "relay").await else { continue };
                let trace = round.is_multiple_of(40).then(TraceContext::root);
                let call = async {
                    if round.is_multiple_of(3) {
                        if let Ok(permit) = quotas.acquire(peer, &REQUESTS) {
                            let _ = relay.request(payload.clone()).await;
                            drop(permit);
                        }
                    } else {
                        let _ = relay.tell(payload.clone()).await;
                    }
                };
                axon::trace::Scoped::new(trace, call).await;
            }
        });
    }
    // Large transfers from the first node to each other node, on Bulk: four of 256 KiB one after another, then 2 s
    // apart. A burst is more than the cap lets through at once, so the writer waits and each burst takes about 4 s.
    let body = Bytes::from(vec![b'b'; 256 << 10]);
    for peer in &names[1..] {
        let (cluster, body, peer) = (running[0].1.clone(), body.clone(), peer.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                for _ in 0..4 {
                    let _ = cluster.request(&peer, BULK, BULK_PROTOCOL, 1, body.clone()).await;
                }
            }
        });
    }
    // An actor stopped with work still queued, so the dead letters page has something to show.
    let system = running[0].0.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let Ok(scratch) = system.spawn(Relay { handled: 0, next: None }).name("scratch").start() else {
                continue;
            };
            TraceContext::root().enter(|| (0..20).for_each(|_| drop(scratch.tell(Msg::Work(Bytes::new(), None)))));
            (0..5).for_each(|_| drop(scratch.tell(Msg::Tick)));
            scratch.lifecycle.stop();
            scratch.join().await;
        }
    });
    if let Ok(flaky) = running[0].0.supervise(|| Flaky).name("flaky").start_supervised() {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            loop {
                [true, false, false, false].into_iter().for_each(|fail| drop(flaky.tell(fail)));
                // Slower than the restart policy's three failures a minute, or the family gives up.
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
    // A family that fails faster than its policy allows, once, so the timeline shows a supervisor giving up.
    if let Ok(fragile) = running[0].0.supervise(|| Flaky).name("fragile").start_supervised() {
        tokio::spawn(async move {
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                drop(fragile.tell(true));
            }
        });
    }
    // A visitor that keeps joining and leaving, so the timeline has something to show.
    if churn {
        let acceptors = acceptors.to_vec();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let (visitor, _cluster, _actors, _streams, _quotas) =
                    start("visitor-0", &discovery, &acceptors, &disk, keyed).await;
                tokio::time::sleep(Duration::from_secs(20)).await;
                let _ = visitor.shutdown().await;
            }
        });
    }
    tokio::signal::ctrl_c().await.unwrap();
}
