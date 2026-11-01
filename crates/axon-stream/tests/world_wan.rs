//! A cluster across three regions on the World, at the round trips of spec/wan.md: eu-a and eu-b in one site, sin, and
//! us and us-b in another. Seeds and Paxos acceptors on eu-a, sin and us; every node hosts keyed actors and calls every
//! key, and eu-a originates a stream that live followers on sin and us-b read. Every timing is the one the program of
//! guide/cluster.md § Across regions sets for these links (spec/wan.md § 9), and `the_values_are_the_guides` holds
//! `SEED_TIMINGS` and `across_regions` the same as the guide's.
//!
//! Quiet for two minutes, nothing isolates, moves or is lost, and calls take a round trip; after each kind of fault,
//! every call and both followers recover within the guide's time for a dead holder; and a redial whose SYN and answer
//! are each lost once marks a peer unreachable on the shipped timings, not on these.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{
    Cluster, ClusterConfig, KEYED_ENDS, PaxosConfig, PaxosLeases, Placement, SeedConfig, SeedDiscovery, SeedServer,
    SeedServerConfig, SeedTimings, SpawnContext,
};
use axon_net::SharedSecret;
use axon_stream::{DeliveryPolicy, RelayTimings, StreamEvent, StreamNode, StreamNodeConfig, SubscribeOptions};
use axon_testkit::world::{Crash, Host, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::{Buf, Bytes};

mod common;
use common::{Echo, EchoApi, SECRET, SEED_PORT, key, keys, role};

const NODES: [&str; 5] = ["eu-a", "eu-b", "sin", "us", "us-b"];
/// The seeds and the acceptors, one in each region.
const SEEDS: [&str; 3] = ["eu-a", "sin", "us"];
const ORIGIN: &str = "eu-a";
const FOLLOWERS: [&str; 2] = ["sin", "us-b"];

/// spec/wan.md § 9's time for a dead holder's keys to run elsewhere, 14 s, and a margin.
const RECOVER: Duration = Duration::from_secs(15);

fn region(node: &str) -> &'static str {
    match node {
        "eu-a" | "eu-b" => "eu",
        "sin" => "sin",
        _ => "us",
    }
}

/// One way, as between these sites.
fn latency(a: &str, b: &str) -> Duration {
    let mut regions = [region(a), region(b)];
    regions.sort_unstable();
    match regions {
        [a, b] if a == b => Duration::from_micros(120),
        ["eu", "us"] => Duration::from_millis(46),
        ["eu", "sin"] => Duration::from_millis(95),
        _ => Duration::from_micros(109_500),
    }
}

fn rtt(a: &str, b: &str) -> Duration {
    2 * latency(a, b)
}

/// Across regions about one chunk in 200 is lost and sent again after the link's RTO, its round trip and 200 ms, and a
/// SYN after the kernel's 1 s.
fn wire(a: &str, b: &str) -> Wire {
    let (latency, jitter) = (latency(a, b), Duration::from_millis(1));
    if region(a) == region(b) {
        return Wire { latency, jitter, ..Wire::default() };
    }
    let stall = rtt(a, b) + Duration::from_millis(200);
    Wire { latency, jitter, stall_every: 200, stall, syn_stall: Duration::from_secs(1), ..Wire::default() }
}

/// Every SYN, and every answer to one, waits the kernel's first resend, and nothing else stalls.
fn lost_syn(a: &str, b: &str) -> Wire {
    Wire { stall_every: 1, stall: Duration::ZERO, syn_stall: Duration::from_secs(1), ..wire(a, b) }
}

fn others(node: &str) -> Vec<&'static str> {
    NODES.into_iter().filter(|n| *n != node).collect()
}

/// Every link, each way.
fn links() -> impl Iterator<Item = (&'static str, &'static str)> {
    NODES.into_iter().flat_map(|a| others(a).into_iter().map(move |b| (a, b)))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Timings {
    /// `ClusterConfig::new`'s and the other defaults, with the heartbeats guide/cluster.md § Seed servers asks for.
    Shipped,
    /// guide/cluster.md § Across regions.
    Wan,
}

const SEED_TIMINGS: SeedTimings = SeedTimings {
    // For a slowest round trip of about 220 ms, with some loss.
    idle: Duration::from_secs(8),
    idle_hold: Duration::from_secs(7),
    closed_grace: Duration::from_secs(4),
    warm_up: Duration::from_secs(8),
};

/// Every node: the cluster, its leases and its streams.
fn across_regions(cluster: &mut ClusterConfig, paxos: &mut PaxosConfig, streams: &mut StreamNodeConfig) {
    // For a slowest round trip of about 220 ms, with some loss.
    cluster.transport.keepalive_interval = Duration::from_secs(1);
    cluster.transport.idle_timeout = Duration::from_secs(8);
    cluster.transport.connect_timeout = Duration::from_millis(3500);
    cluster.transport.handshake_timeout = Duration::from_secs(5);
    cluster.heartbeat_interval = Duration::from_millis(500);
    cluster.heartbeat_grace = Duration::from_secs(2);
    cluster.request_timeout = Duration::from_secs(5);
    cluster.operation_timeout = Duration::from_secs(7);
    paxos.lease = Duration::from_secs(10);
    streams.downstream_expiry = Duration::from_secs(20);
    paxos.drift = 0.01;
}

#[test]
fn the_values_are_the_guides() {
    let test = include_str!("world_wan.rs");
    let values = &test[test.find("const SEED_TIMINGS").unwrap()..test.find("\n#[test]\nfn the_values").unwrap()];
    let guide = include_str!("../../axon/guide/cluster.md");
    assert!(guide.contains(values), "tests/world_wan.rs and guide/cluster.md § Across regions differ");
}

#[derive(Clone)]
struct Node {
    _system: ActorSystem,
    cluster: Cluster,
    stream: StreamNode,
    discovery: SeedDiscovery,
    seed: Option<Arc<SeedServer>>,
}

async fn boot(host: Host, timings: Timings) -> Node {
    let wan = timings == Timings::Wan;
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let seed_timings = if wan { SEED_TIMINGS } else { SeedTimings::default() };
    let mut seed = None;
    if SEEDS.contains(&host.name()) {
        let at = SocketAddr::new(host.ip(), SEED_PORT);
        let server = SeedServerConfig { timings: seed_timings, ..SeedServerConfig::new(at, &transport) };
        seed = Some(Arc::new(SeedServer::bind(server).await.unwrap()));
    }
    let entries = SEEDS.map(|n| SocketAddr::new(host.ip_of(n), SEED_PORT).to_string());
    let mut seeds = SeedConfig { timings: seed_timings, ..SeedConfig::new(entries, &transport) };
    if wan {
        // A seed dial's connect, as the cluster's in across_regions.
        seeds.transport.connect_timeout = Duration::from_millis(3500);
    }
    let discovery = SeedDiscovery::start(seeds).await.unwrap();
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery.clone());
    let origin = host.name() == ORIGIN;
    cfg.labels = role(if origin { "origin" } else { "member" });
    let mut paxos = PaxosConfig::new(SEEDS);
    paxos.store = Some(Arc::new(host.disk().dir("leases")));
    let mut streams = StreamNodeConfig::new(if origin { vec![] } else { vec![role("origin")] });
    if wan {
        across_regions(&mut cfg, &mut paxos, &mut streams);
        // The guide's, for a namespace whose origin is in another region.
        let relay = RelayTimings { upstream_timeout: Duration::from_secs(7), hold_down: Duration::from_secs(30) };
        streams = streams.relay_timings(key().namespace(), relay);
    } else {
        // With seeds, as guide/cluster.md § Seed servers asks.
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
    }
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    cluster.kind::<EchoApi>("echo").register(|_: &SpawnContext, _: Bytes| Ok(Echo)).await.unwrap();
    let stream = StreamNode::start(&cluster, streams).await.unwrap();
    if origin {
        stream.open_stream(&key(), u64::from(host.boot()), DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    Node { _system: system, cluster, stream, discovery, seed }
}

/// What a live follower read.
#[derive(Default)]
struct Follower {
    /// The payload of the last record.
    last: Option<u64>,
    readies: u32,
    lost: u32,
    wrong: Option<String>,
}

#[derive(Default)]
struct Seen {
    /// The payload of the origin's next record: records are numbered from 0, so record n has sequence n + 1.
    next: Cell<u64>,
    stop: Cell<bool>,
    followers: RefCell<BTreeMap<&'static str, Follower>>,
    /// The node each key was first seen running on, per epoch.
    epochs: RefCell<BTreeMap<String, BTreeMap<u64, String>>>,
    sampled: Cell<Duration>,
}

impl Seen {
    fn last(&self, follower: &str) -> Option<u64> {
        self.followers.borrow().get(follower).and_then(|f| f.last)
    }

    /// Each follower's `Ready`s and `OriginLost`s: a node not carrying the key yet tells two `Ready`s at the start.
    fn resets(&self) -> Vec<(u32, u32)> {
        self.followers.borrow().values().map(|f| (f.readies, f.lost)).collect()
    }

    /// Every 100 ms: no key runs on two nodes at one epoch.
    fn sample(&self, w: &World<Node>) {
        if w.now() < self.sampled.get() + Duration::from_millis(100) {
            return;
        }
        self.sampled.set(w.now());
        let mut epochs = self.epochs.borrow_mut();
        for (name, n) in w.nodes() {
            for hosted in n.cluster.hosted(keys().len()).keys {
                let epoch = hosted.epoch.expect("a leased key");
                let first = epochs.entry(hosted.key.clone()).or_default().entry(epoch).or_insert_with(|| name.into());
                assert_eq!(first, name, "{} at epoch {epoch} on two nodes\n{}", hosted.key, w.trace_tail(40));
            }
        }
    }
}

/// A record every 20 ms from the origin, until `stop`.
fn keep_publishing(w: &mut World<Node>, seen: &Rc<Seen>) {
    let seen = seen.clone();
    drop(w.spawn_on(ORIGIN, move |n| async move {
        while !seen.stop.get() {
            let next = seen.next.get();
            if n.stream.publish(&key(), vec![Bytes::copy_from_slice(&next.to_be_bytes())]).is_ok() {
                seen.next.set(next + 1);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }));
}

/// A `Live` follower on `on`: each record once and in order, never an end, and a `Ready` only where it left off.
fn follow(w: &mut World<Node>, seen: &Rc<Seen>, on: &'static str) {
    let seen = seen.clone();
    drop(w.spawn_on(on, move |n| async move {
        let mut sub = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
        while let Some(event) = sub.next().await {
            let mut followers = seen.followers.borrow_mut();
            let f = followers.entry(on).or_default();
            let wrong = match event {
                StreamEvent::Batch(batch) if batch.generation() != 1 => {
                    Some(format!("generation {}", batch.generation()))
                }
                StreamEvent::Batch(batch) => batch.records().iter().find_map(|record| {
                    let got = (&record[..]).get_u64();
                    let gap = f.last.is_some_and(|last| got != last + 1);
                    let wrong = gap.then(|| format!("record {got} after {:?}", f.last));
                    f.last = Some(got);
                    wrong
                }),
                StreamEvent::Ready { start } => {
                    f.readies += 1;
                    let moved = start.zip(f.last).is_some_and(|(at, last)| at.sequence != last + 1);
                    moved.then(|| format!("Ready at {start:?} after record {:?}", f.last))
                }
                StreamEvent::OriginLost => {
                    f.lost += 1;
                    None
                }
                other if other.is_terminal() => Some(format!("{other:?}")),
                _ => None,
            };
            if f.wrong.is_none() {
                f.wrong = wrong;
            }
        }
    }));
}

fn assert_followers_right(w: &World<Node>, seen: &Seen) {
    for (on, f) in seen.followers.borrow().iter() {
        assert_eq!(f.wrong, None, "the follower on {on}\n{}", w.trace_tail(40));
    }
}

/// Runs for `d`, sampling the epochs.
fn run(w: &mut World<Node>, seen: &Seen, d: Duration) {
    let until = w.now() + d;
    while w.now() < until {
        w.run_for(Duration::from_millis(100).min(until - w.now()));
        seen.sample(w);
    }
}

/// Waits until every node of `callers` has had an answer from every key, and each of `followers` a record published
/// from now on, and says how long after `since` that was. Panics past `since + RECOVER`.
fn recovered(
    w: &mut World<Node>,
    callers: &[&'static str],
    followers: &[&str],
    seen: &Rc<Seen>,
    since: Duration,
) -> Duration {
    let next = seen.next.get();
    // Each caller asks every key at once, so each key's recovery is timed on its own. Until it answers, its last error.
    let waiting: Rc<RefCell<BTreeMap<(&str, String), String>>> = Rc::default();
    for &on in callers {
        for key in keys() {
            waiting.borrow_mut().insert((on, key.clone()), String::new());
            let waiting = waiting.clone();
            drop(w.spawn_on(on, move |n| async move {
                let keyed = n.cluster.keyed::<EchoApi>("echo", &key, Bytes::new(), &Placement::default());
                while let Err(e) = keyed.request(1).await {
                    waiting.borrow_mut().insert((on, key.clone()), e.to_string());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                waiting.borrow_mut().remove(&(on, key));
            }));
        }
    }
    while !(waiting.borrow().is_empty() && followers.iter().all(|f| seen.last(f).is_some_and(|last| last >= next))) {
        // A follower that ended or skipped says why now, not as a timeout.
        assert_followers_right(w, seen);
        assert!(
            w.now() < since + RECOVER,
            "not recovered within {RECOVER:?}: waiting on {:?}, followers at {:?} of {next}\n{}",
            waiting.borrow(),
            followers.iter().map(|f| (f, seen.last(f))).collect::<Vec<_>>(),
            w.trace_tail(40)
        );
        w.run_for(Duration::from_millis(10));
        seen.sample(w);
    }
    assert_followers_right(w, seen);
    w.now() - since
}

/// Five nodes on `timings`, every node having called every key, and both followers following.
fn start(seed: u64, timings: Timings) -> (World<Node>, Rc<Seen>) {
    // At the default 64 KiB a connection moves 337 KB/s at 190 ms: the World, not axon, would set the stream's speed
    // and every call's wait behind it (spec/push-window.md § 10).
    let mut w = World::new(WorldConfig { seed, window: 64 << 20, ..WorldConfig::default() });
    for name in NODES {
        w.add(name, move |host| boot(host, timings));
    }
    for (a, b) in links() {
        w.wire(a, b, wire(a, b));
    }
    // The seeds answer after their warm-up, and the acceptors grant nothing for a lease after they start.
    w.run_until("five members with the kind", Duration::from_secs(60), |w| {
        let with_kind = |n: &Node| n.cluster.view().members.iter().filter(|m| m.record.has_kind("echo")).count();
        w.nodes().count() == NODES.len() && w.nodes().all(|(_, n)| with_kind(n) == NODES.len())
    });
    w.run_until("the acceptors grant", Duration::from_secs(30), |w| w.nodes().all(|(_, n)| !n.cluster.warming()));
    let seen = Rc::new(Seen::default());
    keep_publishing(&mut w, &seen);
    for on in FOLLOWERS {
        follow(&mut w, &seen, on);
    }
    let ready = w.now();
    recovered(&mut w, &NODES, &FOLLOWERS, &seen, ready);
    (w, seen)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Counters {
    /// Peers this node counted unreachable.
    unreachable: u64,
    isolations: u64,
    /// Its keys that ended for a lost lease.
    leases_lost: u64,
}

fn counters(w: &World<Node>) -> BTreeMap<&'static str, Counters> {
    let lease_lost = KEYED_ENDS.iter().position(|e| *e == "lease_lost").expect("a reason");
    let counted = |n: &Node| {
        let (stats, kinds) = (n.cluster.stats(), n.cluster.hosted(0).kinds);
        let leases_lost = kinds.iter().find(|k| k.kind == "echo").map_or(0, |k| k.ended[lease_lost]);
        Counters { unreachable: stats.became_unreachable, isolations: stats.isolations, leases_lost }
    };
    NODES.into_iter().filter_map(|name| Some((name, w.read(name, counted)?))).collect()
}

fn quiet(seed: u64) {
    let (mut w, seen) = start(seed, Timings::Wan);
    let (before, resets) = (counters(&w), seen.resets());
    // (caller, owner, how long) of each call after a caller's first round, which resolves the keys.
    let took = Rc::new(RefCell::new(Vec::new()));
    let failed = Rc::new(RefCell::new(Vec::new()));
    for on in NODES {
        let (seen, took, failed) = (seen.clone(), took.clone(), failed.clone());
        drop(w.spawn_on(on, move |n| async move {
            let anywhere = Placement::default();
            let keyed: Vec<_> =
                keys().iter().map(|k| n.cluster.keyed::<EchoApi>("echo", k, Bytes::new(), &anywhere)).collect();
            let mut first = true;
            while !seen.stop.get() {
                for keyed in &keyed {
                    let started = tokio::time::Instant::now();
                    match keyed.request(1).await {
                        Err(e) => failed.borrow_mut().push(format!("{on}: {e:?}")),
                        Ok(_) if first => {}
                        Ok(_) => took.borrow_mut().push((on, keyed.node().expect("answered"), started.elapsed())),
                    }
                }
                first = false;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }));
    }
    run(&mut w, &seen, Duration::from_secs(120));
    seen.stop.set(true);
    let last = seen.next.get() - 1;
    w.run_until("both followers read the last record", Duration::from_secs(5), |_| {
        FOLLOWERS.iter().all(|f| seen.last(f) == Some(last))
    });
    assert_followers_right(&w, &seen);
    assert_eq!(seen.resets(), resets, "seed {seed}: a follower was reset");
    assert_eq!(*failed.borrow(), Vec::<String>::new(), "seed {seed}");
    let after = counters(&w);
    assert_eq!(after, before, "seed {seed}");
    // From the start too, which `before` does not see: the count of `isolated` events, not held to the newest 256.
    assert!(after.values().all(|c| c.isolations == 0), "seed {seed}: {after:?}");

    let calls = took.borrow().len();
    let mut pairs: BTreeMap<(&str, Arc<str>), Vec<Duration>> = BTreeMap::new();
    for (caller, owner, took) in took.take() {
        pairs.entry((caller, owner)).or_default().push(took);
    }
    let mut worst = Duration::ZERO;
    for ((caller, owner), mut took) in pairs {
        took.sort_unstable();
        let p99 = took[took.len() * 99 / 100];
        let floor = if caller == &*owner { Duration::ZERO } else { rtt(caller, &owner) };
        let n = took.len();
        assert!(p99 < floor + Duration::from_millis(500), "seed {seed}: {caller} to {owner}: p99 {p99:?} of {n}");
        worst = worst.max(p99.saturating_sub(floor));
    }
    println!(
        "world digest wan_quiet seed {seed}: {calls} calls, p99 at most {worst:?} over the round trip: {:016x}",
        w.digest()
    );
}

#[test]
fn a_cluster_across_three_regions_runs_quiet_on_the_wan_timings() {
    check_seeds(seeds(0..32), quiet).unwrap();
}

fn faults(seed: u64) {
    let (mut w, seen) = start(seed, Timings::Wan);
    let before = counters(&w);
    let fault = seed % 7;
    let (at, mut gone) = (w.now(), None);
    match fault {
        // sin from the rest, under `lease_grace` (8.49 s) or past the lease.
        0 | 1 => {
            let rest = others("sin");
            w.partition(&["sin"], &rest);
            let cut = Duration::from_secs(if fault == 0 { 5 } else { 20 });
            if fault == 1 {
                gone = Some(recovered(&mut w, &rest, &["us-b"], &seen, at));
            }
            let left = (at + cut).saturating_sub(w.now());
            run(&mut w, &seen, left);
            w.heal();
        }
        // An acceptor and seed: its keys move once it is unreachable and its lease has lapsed.
        2 => {
            w.crash("us", Crash::Machine);
            gone = Some(recovered(&mut w, &others("us"), &FOLLOWERS, &seen, at));
            w.restart("us");
        }
        3 => {
            w.crash("eu-b", Crash::Process);
            let down = Duration::from_millis(w.pick(0..10_000));
            run(&mut w, &seen, down);
            w.restart("eu-b");
        }
        4 => {
            w.pause("sin", Duration::from_secs(2));
            run(&mut w, &seen, Duration::from_secs(2));
        }
        // us-b's sessions idle out in the cut, and after the heal each redial's SYN and its answer are lost once.
        5 => {
            for (a, b) in links() {
                w.wire(a, b, lost_syn(a, b));
            }
            w.partition(&["us-b"], &others("us-b"));
            run(&mut w, &seen, Duration::from_secs(10));
            w.heal();
        }
        // Within `PaxosConfig::drift`, and a wall step under the minute seed discovery allows between clocks.
        _ => {
            let victim = SEEDS[w.pick(0..3) as usize];
            w.drift(victim, if (seed / 7).is_multiple_of(2) { 5000 } else { -5000 });
            let step = w.pick(1000..59_000) as i64;
            w.step_wall(victim, if (seed / 14).is_multiple_of(2) { step } else { -step });
            let d = Duration::from_millis(w.pick(5000..20_000));
            run(&mut w, &seen, d);
        }
    }
    let ended = w.now();
    let back = recovered(&mut w, &NODES, &FOLLOWERS, &seen, ended);
    if fault == 0 {
        // Each of the others kept a majority of the acceptors, and none isolated.
        let after = counters(&w);
        for name in others("sin") {
            assert_eq!(after[name].leases_lost, before[name].leases_lost, "seed {seed}: {name} lost a lease");
        }
        for name in NODES {
            assert_eq!(after[name].isolations, before[name].isolations, "seed {seed}: {name} isolated");
        }
    }
    println!("world digest wan_faults seed {seed} fault {fault}: gone {gone:?} back {back:?}: {:016x}", w.digest());
}

#[test]
fn every_call_and_a_live_follower_recover_from_each_fault_across_regions() {
    check_seeds(seeds(0..32), faults).unwrap();
}

/// sin cut from the rest for 5 s, every SYN on its links and every answer to one lost once, and after the heal its
/// connections to eu-b and us-b reset: how many times a node counted a peer unreachable.
fn lost_syns(seed: u64, timings: Timings) -> u64 {
    let (mut w, seen) = start(seed, timings);
    let unreachable = |w: &World<Node>| counters(w).values().map(|c| c.unreachable).sum::<u64>();
    let before = unreachable(&w);
    for (a, b) in links().filter(|link| link.0 == "sin" || link.1 == "sin") {
        w.wire(a, b, lost_syn(a, b));
    }
    w.partition(&["sin"], &others("sin"));
    run(&mut w, &seen, Duration::from_secs(5));
    w.heal();
    // A redial on either timings. Not to eu-a or us, which keeps the seed links to sin, and sin's to them, out of it:
    // on the shipped timings they dial within 1 s, and here keep each node on two of its three seeds.
    for other in ["eu-b", "us-b"] {
        w.reset("sin", other);
    }
    run(&mut w, &seen, Duration::from_secs(10));
    let flipped = unreachable(&w) - before;
    println!("world digest wan_lost_syns seed {seed} {timings:?}: unreachable {flipped}: {:016x}", w.digest());
    flipped
}

/// The reason for `connect_timeout` 3.5 s and the 8 s idle timeout. On the shipped timings sin's sessions idle out in
/// the cut, and a redial's answer lands about 2.2 s after its SYN, past the 1.5 s `connect_timeout`: a peer is counted
/// unreachable. On the guide's, the sessions outlast the cut, and the redials after the reset have 3.5 s.
#[test]
fn the_shipped_timings_mark_a_peer_unreachable_on_a_lost_syn_and_the_wan_timings_do_not() {
    check_seeds(seeds(0..32), |seed| {
        let (shipped, wan) = (lost_syns(seed, Timings::Shipped), lost_syns(seed, Timings::Wan));
        assert!(
            shipped > 0 && wan == 0,
            "seed {seed}: unreachable {shipped} times on the shipped timings, {wan} on the WAN"
        );
    })
    .unwrap();
}

/// eu-b's link to the seed on sin reset while every SYN on it, and every answer to one, is lost once: on the WAN
/// timings the redial connects within its 3.5 s, about 2.2 s, and sin keeps eu-b on its list. Before, a seed dial
/// connected within 1 s whatever `connect_timeout` was: each redial failed, and sin dropped eu-b after `closed_grace`.
fn seed_link_lost_syn(seed: u64) {
    let (mut w, seen) = start(seed, Timings::Wan);
    for (a, b) in [("eu-b", "sin"), ("sin", "eu-b")] {
        w.wire(a, b, lost_syn(a, b));
    }
    w.reset("eu-b", "sin");
    let listed = |w: &World<Node>| w.read("sin", |n| n.seed.as_ref().map(|s| s.stats().registered)).flatten();
    let until = w.now() + Duration::from_secs(15);
    while w.now() < until {
        run(&mut w, &seen, Duration::from_millis(100));
        assert_eq!(listed(&w), Some(NODES.len()), "seed {seed}: sin's list at {:?}", w.now());
    }
    let link = |n: &Node| {
        let links = n.discovery.stats().seeds;
        links.into_iter().find(|l| l.identity.as_deref() == Some("sin")).map(|l| (l.state, l.last_error))
    };
    assert_eq!(w.read("eu-b", link).flatten().map(|l| l.0), Some("synced"), "seed {seed}: {:?}", w.read("eu-b", link));
    println!("world digest wan_seed_lost_syn seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_seed_link_redials_across_a_lost_syn_within_the_connect_timeout_and_keeps_the_node_listed() {
    check_seeds(seeds(0..16), seed_link_lost_syn).unwrap();
}
