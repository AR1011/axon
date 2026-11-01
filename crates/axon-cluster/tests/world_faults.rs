//! Scenarios of `tests/cluster/` on the World: real nodes on virtual time, so no bound here is sized for a busy machine.
//! `tests/cluster/` keeps one threaded version of each, for the actor threads the World does not run.
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::*;
use axon_net::{Backoff, SharedSecret};
use axon_testkit::world::{Crash, Host, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::{Buf, Bytes};
use tokio::time::{Instant, sleep, timeout};

mod common;

const SECRET: &str = "at-least-16-bytes-long";
const SEEDS: [&str; 3] = ["s0", "s1", "s2"];

#[derive(Clone)]
enum Op {
    Get,
    Hold,
    /// Refused by the far node's decoder: `Retry::Never`.
    Bad,
}

enum Msg {
    Get(Option<Reply<u64>>),
    Hold(Option<Reply<u64>>),
}

struct Counter {
    total: u64,
    /// Answered never: the caller waits until the session goes.
    held: Vec<Reply<u64>>,
}

impl Actor for Counter {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Get(reply) => drop(reply.map(|r| r.send(self.total))),
            Msg::Hold(reply) => self.held.extend(reply),
        }
        Ok(())
    }
}

struct CounterApi;

impl WireProtocol for CounterApi {
    type Message = Op;
    type Reply = u64;
    const ID: u64 = 0xC0;
    const SCHEMA: u16 = 1;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        Ok(Bytes::from_static(match op {
            Op::Get => &[1],
            Op::Hold => &[4],
            Op::Bad => &[9],
        }))
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("short reply".into()))
    }
}

impl ActorProtocol for CounterApi {
    type Mailbox = Msg;

    fn encode_reply(total: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&total.to_be_bytes()))
    }

    fn decode(body: Bytes, reply: Option<Reply<u64>>) -> Result<Msg, Rejection> {
        match body[..] {
            [1] => Ok(Msg::Get(reply)),
            [4] => Ok(Msg::Hold(reply)),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

fn start(total: u64) -> Bytes {
    Bytes::copy_from_slice(&total.to_be_bytes())
}

/// A member, or a seed server alone on its machine.
#[derive(Clone)]
struct Node {
    cluster: Option<Cluster>,
    system: Option<ActorSystem>,
    seed: Option<Arc<SeedServer>>,
}

#[derive(Clone)]
enum Find {
    Memory(MemoryDiscovery),
    /// And the timings the seeds and their nodes run.
    Seeds(&'static [&'static str], SeedTimings),
}

async fn seed_server(host: Host, timings: SeedTimings) -> Node {
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let seed = SeedServer::bind(SeedServerConfig { timings, ..SeedServerConfig::new(host.addr(), &transport) });
    let seed = seed.await.unwrap();
    Node { cluster: None, system: None, seed: Some(Arc::new(seed)) }
}

/// With the timings of `tests/cluster/`; a node that hosts keys registers the counter kind.
async fn member(host: Host, find: Find, hosts_keys: bool, tweak: impl Fn(&Host, &mut ClusterConfig)) -> Node {
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let mut cfg = match find {
        Find::Memory(discovery) => ClusterConfig::new(host.addr(), transport, discovery),
        Find::Seeds(seeds, timings) => {
            let entries = seeds.iter().map(|s| SocketAddr::new(host.ip_of(s), 7000).to_string());
            let discovery = SeedConfig { timings, ..SeedConfig::new(entries, &transport) };
            let discovery = SeedDiscovery::start(discovery).await.unwrap();
            let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
            // With seeds, as guide/cluster.md § Seed servers asks.
            cfg.heartbeat_interval = Duration::from_millis(500);
            cfg.heartbeat_grace = Duration::from_secs(1);
            cfg
        }
    };
    common::short(&mut cfg);
    tweak(&host, &mut cfg);
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    if hosts_keys {
        let build = |_: &SpawnContext, mut args: Bytes| {
            let total = args.try_get_u64().map_err(|_| "args must be a u64".to_string())?;
            Ok(Counter { total, held: Vec::new() })
        };
        cluster.kind::<CounterApi>("counter").register(build).await.unwrap();
    }
    Node { cluster: Some(cluster), system: Some(system), seed: None }
}

fn world(
    seed: u64,
    find: &Find,
    members: &[&str],
    hosts_keys: &[&str],
    tweak: impl Fn(&Host, &mut ClusterConfig) + Clone + 'static,
) -> World<Node> {
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    if let Find::Seeds(seeds, timings) = *find {
        for name in seeds {
            w.add(name, move |host| seed_server(host, timings));
        }
        // Past their warm-up, so a test's timing starts from a working cluster.
        let warm = w.block_on(seeds[0], |n| async move { n.seed.unwrap().stats().warm_up_left });
        w.run_for(warm.unwrap());
    }
    for name in members {
        let (find, keys, tweak) = (find.clone(), hosts_keys.contains(name), tweak.clone());
        w.add(name, move |host| member(host, find.clone(), keys, tweak.clone()));
    }
    let count = members.len() + if let Find::Seeds(seeds, _) = find { seeds.len() } else { 0 };
    w.run_until("every node booted", Duration::from_secs(30), |w| w.nodes().count() == count);
    w
}

fn cluster<'w>(w: &'w World<Node>, name: &str) -> &'w Cluster {
    let node = w.nodes().find(|(n, _)| *n == name).map(|(_, node)| node);
    node.and_then(|n| n.cluster.as_ref()).unwrap_or_else(|| panic!("{name} is not a running member"))
}

fn with_kind(w: &World<Node>, on: &str) -> usize {
    cluster(w, on).view().members.iter().filter(|m| m.record.has_kind("counter")).count()
}

/// A process or a machine, as the seed picks.
fn crash(w: &mut World<Node>, name: &str) {
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.log(format_args!("{name} {how:?}"));
    w.crash(name, how);
}

/// How long a key takes to move off an owner gone silent, from the timings its nodes run. A caller sees the silence at
/// the first keepalive past the idle timeout, then its redial and the survivor's dial of the owner each give up after
/// `connect_timeout`. Under a lease, `Cluster::start` cuts the idle timeout to the lease's hold and the keepalive and
/// the connect to a quarter of that, and the key moves once the lease runs out and the survivor's next acquire,
/// `HELD_RETRY` (200 ms) later, takes it; for a lease above about 0.75 s the idle path is the later of the two, so it
/// is the bound: 1.48 s under a 1 s lease against 8.1 s without one. The last 100 ms is for the calls between, a few
/// ms each.
fn silent_owner_move(lease: Option<Duration>) -> Duration {
    let transport = axon_net::Config::new("any", SharedSecret::new(SECRET).unwrap());
    let t = ClusterConfig::new(([127, 0, 0, 1], 7000).into(), transport, MemoryDiscovery::new()).transport;
    let (mut idle, mut keepalive, mut connect) = (t.idle_timeout, t.keepalive_interval, t.connect_timeout);
    let mut freed = Duration::ZERO;
    if let Some(lease) = lease {
        let mut paxos = PaxosConfig::new(["p-0"]);
        // The lease the nodes run, to read the hold it gives.
        paxos.lease = lease;
        let hold = PaxosLeases::new(paxos).unwrap().hold().expect("a Paxos lease has a hold");
        if hold < idle {
            (idle, keepalive, connect) = (hold, keepalive.min(hold / 4), connect.min(hold / 4));
        }
        freed = lease + HELD_RETRY;
    }
    (idle + keepalive + connect * 2).max(freed) + Duration::from_millis(100)
}

fn publish_counter(w: &mut World<Node>, on: &str, total: u64) {
    let published = w.block_on(on, move |n| async move {
        let cluster = n.cluster.unwrap();
        let system = n.system.unwrap();
        let actor = system.spawn(Counter { total, held: Vec::new() }).start().unwrap();
        cluster.publish::<CounterApi>(&actor, "counter").map(drop)
    });
    published.unwrap().unwrap();
}

/// Asks `on` for `key` every 20 ms until it runs on `owner` (anywhere when None), and answers where, with which
/// epoch and after how long.
fn spawn_key(
    w: &mut World<Node>,
    on: &str,
    key: &str,
    owner: Option<&str>,
    within: Duration,
) -> (String, Option<u64>, Duration) {
    let (key, owner) = (key.to_owned(), owner.map(str::to_owned));
    let found = w.block_on(on, move |n| async move {
        let (cluster, started) = (n.cluster.unwrap(), Instant::now());
        loop {
            match cluster.get_or_spawn::<CounterApi>("counter", &key, start(0), &Placement::default()).await {
                Ok(r) if owner.as_deref().is_none_or(|o| r.node() == o) => {
                    return (r.node().to_owned(), r.epoch(), started.elapsed());
                }
                other => assert!(started.elapsed() < within, "never spawned on {owner:?}: {other:?}"),
            }
            sleep(Duration::from_millis(20)).await;
        }
    });
    found.unwrap()
}

fn both_ways(w: &mut World<Node>, wire: Wire) {
    w.wire("a", "b", wire);
    w.wire("b", "a", wire);
}

/// One of a and b 5000 ppm fast, the other as slow, and b's wall clock stepped a minute either way.
fn off_clock(w: &mut World<Node>) {
    let fast = if w.pick(0..2) == 0 { ["a", "b"] } else { ["b", "a"] };
    w.drift(fast[0], 5000);
    w.drift(fast[1], -5000);
    let by = if w.pick(0..2) == 0 { 60_000 } else { -60_000 };
    w.step_wall("b", by);
}

fn get(w: &mut World<Node>, remote: &RemoteRef<CounterApi>) -> Result<u64, CallError> {
    let remote = remote.clone();
    w.block_on("a", |_| async move { remote.request(Op::Get).await }).unwrap()
}

/// The counter, resolved on `on`.
fn resolve(w: &mut World<Node>, on: &str) -> RemoteRef<CounterApi> {
    let resolved = w.block_on(on, |n| async move { n.cluster.unwrap().resolve::<CounterApi>("counter").await });
    resolved.unwrap().unwrap_or_else(|e| panic!("resolved on {on}: {e:?}"))
}

/// Runs until a's view holds `n` members.
fn sees(w: &mut World<Node>, n: usize) {
    w.run_until(&format!("a sees {n} members"), Duration::from_secs(30), |w| cluster(w, "a").view().members.len() == n);
}

/// What `call` answers on `on`, and how long it took on that node's clock.
fn timed<T: 'static, F: Future<Output = T> + 'static>(
    w: &mut World<Node>,
    on: &str,
    call: impl FnOnce(Node) -> F + 'static,
) -> (T, Duration) {
    let asked = w.block_on(on, |n| async move {
        let asked = Instant::now();
        (call(n).await, asked.elapsed())
    });
    asked.unwrap()
}

/// How long after now a's calls to `remote` take to get through again.
fn through_again(w: &mut World<Node>, remote: RemoteRef<CounterApi>) -> Duration {
    let calls = move |_| async move {
        while remote.request(Op::Get).await.is_err() {
            sleep(Duration::from_millis(50)).await;
        }
    };
    timed(w, "a", calls).1
}

fn crashed_node(seed: u64, find: Find) {
    let mut w = world(seed, &find, &["a", "b"], &[], |_, _| {});
    sees(&mut w, 2);
    publish_counter(&mut w, "b", 1);
    let (remote, mut events, snapshot) = w
        .block_on("a", |n| async move {
            let cluster = n.cluster.unwrap();
            let mut events = cluster.subscribe();
            let first = events.next().await;
            let snapshot = matches!(first, Some(MembershipEvent::Snapshot(view)) if view.members.len() == 2);
            (cluster.resolve::<CounterApi>("counter").await.unwrap(), events, snapshot)
        })
        .unwrap();
    assert!(snapshot, "seed {seed}: the first event is the view");

    // Requests the peer is sitting on when it dies: each caller must be told, at once, that nobody knows.
    let waiting: Vec<_> = (0..20)
        .map(|_| {
            let remote = remote.clone();
            w.spawn_on("a", move |_| async move { remote.request(Op::Hold).await })
        })
        .collect();
    w.run_until("the requests are with the peer", Duration::from_secs(5), |w| {
        cluster(w, "b").stats().engine.inbound_calls >= 20
    });
    let old_incarnation = cluster(&w, "b").identity().incarnation;
    let crashed = w.now();
    // Always a process: only its reset tells the callers at once. A silent machine's are told at the idle timeout.
    w.crash("b", Crash::Process);
    for call in waiting {
        let told = w.join(call).unwrap();
        assert!(matches!(told, Err(CallError::OutcomeUnknown)), "seed {seed}: {told:?}");
    }
    let took = w.now() - crashed;
    assert!(took < Duration::from_millis(20), "seed {seed}: the callers were told after {took:?}");
    assert_eq!(cluster(&w, "a").stats().engine.unknown_outcomes, 20);
    w.run_until("a marks b unreachable", Duration::from_secs(10), |w| {
        cluster(w, "a").view().get("b").is_some_and(|m| !m.reachable)
    });
    // Discovery still lists b: a seed drops a crashed node after 2 s, and memory never does.
    assert_eq!(cluster(&w, "a").view().members.len(), 2, "seed {seed}");
    let unreachable = get(&mut w, &remote);
    assert!(matches!(unreachable, Err(CallError::Unreachable(_))), "seed {seed}: {unreachable:?}");
    let (none, saw) = w
        .block_on("a", |n| async move {
            let cluster = n.cluster.unwrap();
            let (found, failed) = cluster.resolve_all::<CounterApi>("counter").await;
            let none = found.is_empty() && failed.is_empty();
            let mut saw = false;
            while let Ok(Some(event)) = timeout(Duration::from_millis(200), events.next()).await {
                let gone = MemberChange::Unreachable("b".into());
                saw |= matches!(event, MembershipEvent::Changes { changes, .. } if changes.contains(&gone));
            }
            (none, saw)
        })
        .unwrap();
    assert!(none && saw, "seed {seed}: {none} {saw}");
    assert_eq!(cluster(&w, "a").stats().became_unreachable, 1);

    // The node restarts under the same id: a new boot, which old references must not reach.
    w.restart("b");
    publish_counter(&mut w, "b", 9);
    assert!(cluster(&w, "b").identity().incarnation > old_incarnation);
    let replaced = w.block_on("a", move |n| async move {
        let cluster = n.cluster.unwrap();
        while !cluster.resolve::<CounterApi>("counter").await.is_ok_and(|r| r.node_incarnation() != old_incarnation) {
            sleep(Duration::from_millis(20)).await;
        }
    });
    replaced.unwrap();
    let stale = get(&mut w, &remote);
    assert!(matches!(stale, Err(CallError::StaleNode)), "seed {seed}: {stale:?}");
    assert_eq!(cluster(&w, "a").stats().replaced, 1, "seed {seed}");

    // A node that expires from discovery leaves the view. A seed drops a crashed node by itself, and the removal it
    // had pending for the old boot must not take the new one.
    match find {
        Find::Memory(ref discovery) => discovery.expire("b"),
        Find::Seeds(..) => {
            w.run_for(Duration::from_millis(2200));
            assert_eq!(cluster(&w, "a").view().members.len(), 2, "seed {seed}: the old boot's removal took the new");
            crash(&mut w, "b");
        }
    }
    w.run_until("a sees b go", Duration::from_secs(20), |w| cluster(w, "a").view().members.len() == 1);
    let gone = get(&mut w, &remote);
    assert!(matches!(gone, Err(CallError::NoRoute | CallError::Unreachable(_))), "seed {seed}: {gone:?}");
    let find = if matches!(find, Find::Memory(_)) { "memory" } else { "seeds" };
    println!("world digest crashed_node_{find} seed {seed}: {:016x}", w.digest());
}

#[test]
fn crashed_node_is_detected_before_discovery_notices() {
    check_seeds(seeds(0..64), |seed| crashed_node(seed, Find::Memory(MemoryDiscovery::new()))).unwrap();
}

#[test]
fn crashed_node_is_detected_before_discovery_notices_on_seeds() {
    check_seeds(seeds(0..64), |seed| crashed_node(seed, Find::Seeds(&SEEDS, SeedTimings::default()))).unwrap();
}

/// A machine that restarts before its peers notice it went: the next request on the old session reaches the new boot,
/// whose kernel resets it, and the caller is told `OutcomeUnknown` at once. That is honest: the caller cannot tell
/// whether the old boot ran the request before it went.
#[test]
fn a_request_to_a_peer_that_restarted_unnoticed_is_outcome_unknown_at_once() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, remote) = a_calling_b(seed, None);
        w.crash("b", Crash::Machine);
        w.restart("b");
        let (told, took) = timed(&mut w, "a", move |_| async move { remote.request(Op::Get).await });
        assert!(matches!(told, Err(CallError::OutcomeUnknown)), "seed {seed}: {told:?}");
        assert!(took < Duration::from_millis(20), "seed {seed}: told after {took:?}");
        println!("world digest restarted_unnoticed seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A stopped process: its kernel still accepts, but nothing it is sent is answered, handshakes included.
#[test]
fn a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], quick);
        sees(&mut w, 2);
        publish_counter(&mut w, "b", 1);
        let remote = resolve(&mut w, "a");
        assert_eq!(get(&mut w, &remote).unwrap(), 1);

        let pause = Duration::from_millis(w.pick(4000..8000));
        let stopped = w.now();
        w.pause("b", pause);
        let unreachable = |w: &World<Node>| cluster(w, "a").view().get("b").is_some_and(|m| !m.reachable);
        w.run_until("b is unreachable", pause, unreachable);
        // The idle timeout and one keepalive, about 2 s; waiting out the 5 s handshake took 6.5 s.
        let took = w.now() - stopped;
        assert!(took < Duration::from_millis(3500), "seed {seed}: waited out a handshake: {took:?}");
        w.run_until("b is reachable again", pause + Duration::from_secs(10), |w| {
            cluster(w, "a").view().get("b").is_some_and(|m| m.reachable)
        });
        println!("world digest paused_peer seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Only the TCP connect has to finish within `connect_timeout`: a peer 200 ms away, whose handshake takes about 1.4 s,
/// is never counted unreachable for it.
#[test]
fn a_distant_peer_whose_handshake_outlasts_a_keepalive_stays_reachable() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], |_, _| {});
        sees(&mut w, 2);
        publish_counter(&mut w, "b", 1);
        let far = Wire { latency: Duration::from_millis(200), ..Wire::default() };
        both_ways(&mut w, far);
        let remote = resolve(&mut w, "a");
        assert_eq!(get(&mut w, &remote).unwrap(), 1, "seed {seed}");
        assert_eq!(cluster(&w, "a").stats().engine.dial_failures, 0, "seed {seed}");
        println!("world digest distant_peer seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A SYN lost once is sent again by the kernel at 1 s: under `ClusterConfig::new`'s timings, none changed, a's first
/// dial of b held that long still connects, and b is never counted unreachable for it. A cut of 1.2 s stands in for
/// the lost SYN: it holds the dial and lets it through on the heal.
#[test]
fn a_lost_syn_costs_no_reachability_under_cluster_config_new() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], |_, _| {});
        sees(&mut w, 2);
        publish_counter(&mut w, "b", 1);
        w.cut(&["a"], &["b"]);
        let called = w.spawn_on("a", |n| async move {
            let remote = n.cluster.unwrap().resolve::<CounterApi>("counter").await.map_err(|e| format!("{e:?}"))?;
            remote.request(Op::Get).await.map_err(|e| format!("{e:?}"))
        });
        reachable_through(&mut w, Duration::from_millis(1200));
        w.heal();
        reachable_through(&mut w, Duration::from_millis(500));
        assert_eq!(w.join(called).unwrap().unwrap(), 1, "seed {seed}");
        let stats = cluster(&w, "a").stats();
        assert_eq!((stats.became_unreachable, stats.engine.dial_failures), (0, 0), "seed {seed}");
        println!("world digest lost_syn seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// After a session that went silent the redial gives up after `connect_timeout`, not a keepalive: one that waits for
/// its connect through the rest of a cut, 2 to 3 s, connects on the heal and b stays reachable. The links are EU to
/// us, 46 ms each way.
#[test]
fn a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable() {
    check_seeds(seeds(0..64), |seed| {
        let wan = |_: &Host, cfg: &mut ClusterConfig| {
            // The value guide/cluster.md § Across regions gives for these links (spec/wan.md § 9).
            cfg.transport.connect_timeout = Duration::from_millis(3500);
        };
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], wan);
        both_ways(&mut w, Wire { latency: Duration::from_millis(46), ..Wire::default() });
        sees(&mut w, 2);
        publish_counter(&mut w, "b", 1);
        let remote = resolve(&mut w, "a");
        assert_eq!(get(&mut w, &remote).unwrap(), 1, "seed {seed}");
        // ClusterConfig::new's idle timeout: the session idles out in the cut and the redial starts there.
        w.partition(&["a"], &["b"]);
        reachable_through(&mut w, Duration::from_secs(4 + 3));
        w.heal();
        reachable_through(&mut w, Duration::from_secs(1));
        assert_eq!(get(&mut w, &remote).unwrap(), 1, "seed {seed}");
        assert_eq!(cluster(&w, "a").stats().became_unreachable, 0, "seed {seed}");
        println!("world digest silent_then_syn seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Runs for `d`, checking every 50 ms that a's view has b reachable.
fn reachable_through(w: &mut World<Node>, d: Duration) {
    let until = w.now() + d;
    while w.now() < until {
        w.run_for(Duration::from_millis(50));
        let reachable = cluster(w, "a").view().get("b").is_some_and(|m| m.reachable);
        assert!(reachable, "b counted unreachable at {:?}", w.now());
    }
}

/// Nodes a and b on the shipped redial backoff, or on `backoff`, a having called the counter on b.
fn a_calling_b(seed: u64, backoff: Option<Backoff>) -> (World<Node>, RemoteRef<CounterApi>) {
    let set_backoff = move |_: &Host, cfg: &mut ClusterConfig| {
        let shipped = ClusterConfig::new(cfg.bind, cfg.transport.clone(), MemoryDiscovery::new()).backoff;
        // member() shortens the redial backoff: these tests put the shipped one back, or the one they are about.
        cfg.backoff = backoff.clone().unwrap_or(shipped);
    };
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], set_backoff);
    sees(&mut w, 2);
    publish_counter(&mut w, "b", 1);
    let remote = resolve(&mut w, "a");
    assert_eq!(get(&mut w, &remote).unwrap(), 1);
    (w, remote)
}

/// A cut long enough to push the redial backoff to its cap: once it heals, calls get through within the cap (2 s,
/// which the jitter only shortens), not the up to 36 s of the transport's old default that the real-fault rig saw
/// (spec/fault-rig.md § Findings fixed). Discovery keeps listing the peer throughout, as seeds reached through a third
/// node do.
#[test]
fn calls_resume_within_the_redial_cap_once_a_long_cut_heals() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, remote) = a_calling_b(seed, None);

        w.partition(&["a"], &["b"]);
        let cut = Duration::from_millis(w.pick(35_000..45_000));
        w.run_for(cut);
        w.heal();
        let took = through_again(&mut w, remote);
        assert!(took < Duration::from_secs(3), "seed {seed}: a call got through {took:?} after a {cut:?} cut healed");
        println!("world digest healed_cut seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A peer back as a new boot is dialled as soon as the view shows it: the backoff its last boot's failed dials earned,
/// up to the 2 s cap, is not waited out, and a call made then waits for that dial instead of failing with their error.
#[test]
fn a_peer_back_as_a_new_boot_is_dialled_as_soon_as_the_view_shows_it() {
    check_seeds(seeds(0..64), |seed| asked_as_soon_as_seen(seed, false)).unwrap();
}

/// A dial of the old boot that fails as the view shows the new one fails no call made then: the new boot is dialled.
#[test]
fn a_dial_of_the_old_boot_that_fails_as_the_view_shows_the_new_one_fails_no_call() {
    check_seeds(seeds(0..64), |seed| asked_as_soon_as_seen(seed, true)).unwrap();
}

fn asked_as_soon_as_seen(seed: u64, race: bool) {
    // Redialling every millisecond keeps a dial in flight when b comes back.
    let tight = Backoff { initial: Duration::from_millis(1), max: Duration::from_millis(1), jitter: 0.0 };
    let (mut w, _) = a_calling_b(seed, race.then_some(tight));

    let old = cluster(&w, "b").identity().incarnation;
    if race {
        // A machine answers no SYN: each dial of it waits out its connect timeout (1.5 s).
        w.crash("b", Crash::Machine);
    } else {
        crash(&mut w, "b");
    }
    // Long enough for a's redials to reach the cap.
    let down = Duration::from_millis(w.pick(10_000..20_000));
    w.run_for(down);
    if race {
        // Paused past that timeout across b's return, a resumes with its discovery poll and a dial's timeout both due,
        // the poll first: the dial of the old boot ends in the poll that wakes it for the new one.
        let paused = Duration::from_millis(w.pick(1_600..2_400));
        w.pause("a", paused);
    }
    w.restart("b");
    w.run_until("a sees b's new boot", Duration::from_secs(5), |w| {
        cluster(w, "a").view().get("b").is_some_and(|m| m.record.incarnation != old)
    });
    let seen = w.now();
    // resolve_all skips a member marked unreachable.
    let available = cluster(&w, "a").view().get("b").is_some_and(|m| m.available());
    let asked = w.block_on("a", |n| async move { n.cluster.unwrap().resolve_all::<CounterApi>("counter").await });
    let (_, failed) = asked.unwrap();
    assert!(available && failed.is_empty(), "seed {seed}: b's new boot was not asked: {available} {failed:?}");
    // A dial and a call, a few ms each on the World's wire.
    let took = w.now() - seen;
    assert!(took < Duration::from_millis(100), "seed {seed}: b's new boot was asked {took:?} after a saw it");
    println!("world digest new_boot{} seed {seed}: {:016x}", if race { "_race" } else { "" }, w.digest());
}

/// A process that is not the boot the view names is refused `Auth`: here b under a record one boot ahead of it, and b
/// under another node's name.
#[test]
fn a_process_that_is_not_the_boot_the_view_names_is_refused() {
    check_seeds(seeds(0..64), |seed| {
        let discovery = MemoryDiscovery::new();
        let mut w = world(seed, &Find::Memory(discovery.clone()), &["a", "b"], &[], |_, _| {});
        sees(&mut w, 2);
        publish_counter(&mut w, "b", 1);
        let b = cluster(&w, "b").record();
        let ahead = NodeRecord { incarnation: b.incarnation + 1, ..b.clone() };
        for record in [ahead, NodeRecord { id: "x".into(), ..b.clone() }] {
            let discovery = discovery.clone();
            w.block_on("a", move |_| async move { discovery.register(&record).await }).unwrap().unwrap();
        }
        w.run_until("a sees both", Duration::from_secs(5), |w| {
            let view = cluster(w, "a").view();
            view.get("x").is_some() && view.get("b").is_some_and(|m| m.record.incarnation != b.incarnation)
        });
        let asked = w.block_on("a", |n| async move { n.cluster.unwrap().resolve_all::<CounterApi>("counter").await });
        let (found, failed) = asked.unwrap();
        let auth = |e: &ClusterError| matches!(e, ClusterError::Call(CallError::Unreachable(axon_net::Error::Auth)));
        let mut refused: Vec<_> = failed.iter().filter(|(_, e)| auth(e)).map(|(n, _)| n.as_str()).collect();
        refused.sort_unstable();
        assert!(found.is_empty() && refused == ["b", "x"], "seed {seed}: {} found, {failed:?}", found.len());
        println!("world digest not_the_boot seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Lets the node's calls to the system endpoints through, so its spawns and watches meet their own checks.
struct Refuse(&'static str);

impl Authorizer for Refuse {
    fn authorize(&self, peer: &axon_net::Peer, action: Action<'_>) -> bool {
        *peer.identity.node_id != *self.0 || matches!(action, Action::Call { name } if name.starts_with("axon."))
    }
}

/// Access revoked while the peer is cut off: once the cut heals, over a new session, nothing it sent is run, its next
/// call and spawn are refused `Forbidden` and its watch ends. Access granted again lets the same reference through.
#[test]
fn access_revoked_during_a_partition_is_refused_once_it_heals() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &["a"], |_, _| {});
        w.run_until("b sees the kind on a", Duration::from_secs(30), |w| with_kind(w, "b") == 1);
        publish_counter(&mut w, "a", 1);
        let remote = resolve(&mut w, "b");
        spawn_key(&mut w, "b", "k", Some("a"), Duration::from_secs(5));
        let (seen, ended): (Rc<RefCell<Vec<Owner>>>, Rc<RefCell<bool>>) = Default::default();
        let (log, end) = (seen.clone(), ended.clone());
        drop(w.spawn_on("b", |n| async move {
            let mut watch = n.cluster.unwrap().watch_key("counter", "k", &Placement::default());
            while let Some(owner) = watch.next().await {
                log.borrow_mut().push(owner);
            }
            *end.borrow_mut() = true;
        }));
        w.run_until("the watcher sees k run", Duration::from_secs(1), |_| {
            matches!(seen.borrow().last(), Some(Owner::Running { .. }))
        });

        w.partition(&["a"], &["b"]);
        let (cut, revoke) = (w.pick(500..5000), w.pick(0..500));
        let sent = remote.clone();
        let during = w.spawn_on("b", move |_| async move { sent.request(Op::Get).await });
        w.run_for(Duration::from_millis(revoke));
        w.block_on("a", |n| async move { n.cluster.unwrap().set_authorizer(Some(Arc::new(Refuse("b")))) }).unwrap();
        w.run_for(Duration::from_millis(cut - revoke));
        w.heal();
        let sent = w.join(during).unwrap();
        assert!(sent.is_err(), "seed {seed}: a call sent into the cut ran after the revoke: {sent:?}");

        let asked = remote.clone();
        let refused = w.block_on("b", move |_| async move {
            let healed = Instant::now();
            loop {
                match asked.request(Op::Get).await {
                    Err(CallError::Rejected(Rejection::Forbidden)) => return Ok(healed.elapsed()),
                    Ok(total) => return Err(total),
                    Err(_) => sleep(Duration::from_millis(50)).await,
                }
            }
        });
        let took = refused.unwrap().unwrap_or_else(|total| panic!("seed {seed}: a revoked call answered {total}"));
        assert!(took < CALL_BOUND, "seed {seed}: refused {took:?} after the heal");
        let spawn = w.block_on("b", |n| async move {
            n.cluster.unwrap().get_or_spawn::<CounterApi>("counter", "j", start(0), &Placement::default()).await
        });
        let spawn = spawn.unwrap();
        let forbidden = matches!(spawn, Err(ClusterError::Call(CallError::Rejected(Rejection::Forbidden))));
        assert!(forbidden, "seed {seed}: a revoked spawn: {spawn:?}");
        // The request timeout (2 s) bounds a question held across the cut.
        w.run_until("the watch ends", Duration::from_secs(2) + CALL_BOUND, |_| *ended.borrow());

        w.block_on("a", |n| async move { n.cluster.unwrap().set_authorizer(None) }).unwrap();
        assert_eq!(w.block_on("b", |_| async move { remote.request(Op::Get).await }).unwrap().unwrap(), 1);
        println!("world digest revoked seed {seed} cut {cut} revoke {revoke}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Cuts a, which runs a key, from its seed s0 and its peer b: how long until a isolates and stops it, and until the
/// seed removes a from b's view. The key then runs on b, and a joins again once the cut heals.
fn cut_off(
    seed: u64,
    timings: SeedTimings,
    tweak: impl Fn(&Host, &mut ClusterConfig) + Clone + 'static,
) -> (Duration, Duration) {
    let mut w = world(seed, &Find::Seeds(&["s0"], timings), &["a", "b"], &["a", "b"], tweak);
    w.run_until("kinds are advertised", Duration::from_secs(30), |w| with_kind(w, "b") == 2);
    let key = w
        .block_on("b", |n| async move {
            let cluster = n.cluster.unwrap();
            for i in 0.. {
                let key = format!("key-{i}");
                let anywhere = Placement::default();
                let found = cluster.get_or_spawn::<CounterApi>("counter", &key, start(0), &anywhere);
                if found.await.unwrap().node() == "a" {
                    return key;
                }
            }
            unreachable!()
        })
        .unwrap();
    assert_eq!(cluster(&w, "a").stats().spawned, 1);

    w.partition(&["a"], &["s0", "b"]);
    let (cut, mut isolated) = (w.now(), None);
    w.run_until("the seed removed a", Duration::from_secs(30), |w| {
        let stats = cluster(w, "a").stats();
        if isolated.is_none() && stats.isolated && stats.spawned == 0 {
            isolated = Some(w.now() - cut);
        }
        cluster(w, "b").view().get("a").is_none()
    });
    let removed = w.now() - cut;
    let isolated = isolated.unwrap_or_else(|| panic!("seed {seed}: removed after {removed:?}, never isolated"));
    spawn_key(&mut w, "b", &key, Some("b"), Duration::from_secs(10));

    w.heal();
    w.run_until("a is back", Duration::from_secs(40), |w| {
        !cluster(w, "a").stats().isolated
            && cluster(w, "b").view().members.len() == 2
            && cluster(w, "a").view().members.len() == 2
    });
    println!(
        "world digest isolated seed {seed} idle {:?} at {isolated:?} removed {removed:?}: {:016x}",
        timings.idle,
        w.digest()
    );
    (isolated, removed)
}

#[test]
fn a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed() {
    check_seeds(seeds(0..64), |seed| {
        let (isolated, removed) = cut_off(seed, SeedTimings::default(), |_, _| {});
        assert!(isolated + Duration::from_secs(1) < removed, "seed {seed}: {isolated:?}, {removed:?}");
        assert!(removed > Duration::from_secs(5), "seed {seed}: a silent node is given time: {removed:?}");
    })
    .unwrap();
}

/// At the seed timings, idle timeout and heartbeats for regions far apart (spec/wan.md § 9).
#[test]
fn a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed_at_the_wan_timings() {
    let s = Duration::from_secs;
    // The WAN values, the subject of this test.
    let wan = SeedTimings { idle: s(8), idle_hold: s(7), closed_grace: s(4), warm_up: s(8) };
    check_seeds(seeds(0..64), |seed| {
        let (isolated, removed) = cut_off(seed, wan, move |_, cfg| {
            // The WAN values, the subject of this test.
            cfg.transport.idle_timeout = s(8);
            cfg.heartbeat_grace = s(2);
        });
        assert!(isolated <= s(12) && removed > s(14), "seed {seed}: {isolated:?}, {removed:?}");
        assert!(isolated + Duration::from_millis(1500) <= removed, "seed {seed}: {isolated:?}, {removed:?}");
    })
    .unwrap();
}

/// On seeds a node isolates 4 to 6.5 s after a cut, when its seed sessions have idled out, and at the shipped 6 s
/// lease its `lease_grace` (5.02 s) ends first: a lease this long leaves a window between the two (spec/wan.md § 7).
const LONG_LEASE: Duration = Duration::from_secs(20);

/// Seeds on s0, s1 and s2, acceptors on a, b and c under `LONG_LEASE`, and a key d holds, started at 7, with its epoch.
fn isolated_holder(seed: u64) -> (World<Node>, String, u64) {
    let members = ["a", "b", "c", "d"];
    let leases = |host: &Host, cfg: &mut ClusterConfig| {
        let mut paxos = PaxosConfig::new(["a", "b", "c"]);
        // A window between isolation and the fence.
        paxos.lease = LONG_LEASE;
        paxos.store = Some(Arc::new(host.disk().dir("leases")));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    };
    let mut w = world(seed, &Find::Seeds(&SEEDS, SeedTimings::default()), &members, &members, leases);
    w.run_until("kinds are advertised", Duration::from_secs(30), |w| with_kind(w, "d") == 4);
    let held = w.block_on("d", |n| async move {
        let (cluster, mut i) = (n.cluster.unwrap(), 0);
        loop {
            let key = format!("key-{i}");
            if cluster.owner("counter", &key, &Placement::default()).as_deref() != Some("d") {
                i += 1;
                continue;
            }
            match cluster.get_or_spawn::<CounterApi>("counter", &key, start(7), &Placement::default()).await {
                Ok(placed) if placed.node() == "d" => return (key, placed.epoch().expect("a leased key")),
                Ok(_) => i += 1,
                // The acceptors refuse every grant for a lease after they start.
                Err(_) => sleep(Duration::from_millis(200)).await,
            }
        }
    });
    let (key, epoch) = held.unwrap();
    (w, key, epoch)
}

/// Nodes started with their seeds are not heard until the seeds are past their warm-up, and isolate meanwhile. They
/// were only starting, their views not yet formed: nothing on their timelines or in their counts says so.
#[test]
fn nodes_started_with_their_seeds_record_no_isolation() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        let find = Find::Seeds(&SEEDS, SeedTimings::default());
        for name in SEEDS {
            w.add(name, |host| seed_server(host, SeedTimings::default()));
        }
        for name in ["a", "b"] {
            let find = find.clone();
            w.add(name, move |host| member(host, find.clone(), true, |_, _| {}));
        }
        let formed = |w: &World<Node>| ["a", "b"].iter().all(|n| cluster(w, n).view().members.len() == 2);
        w.run_until("the views form", Duration::from_secs(30), |w| w.nodes().count() == 5 && formed(w));
        w.run_for(Duration::from_secs(2));
        for node in ["a", "b"] {
            let told = cluster(&w, node).events().since(0, usize::MAX);
            let isolations: Vec<_> = told.iter().filter(|e| e.kind.starts_with("isolat")).map(|e| e.kind).collect();
            let stats = cluster(&w, node).stats();
            assert_eq!((isolations, stats.isolations, stats.isolated), (vec![], 0, false), "seed {seed}: {node}");
        }
    })
    .unwrap();
}

/// The epoch `on` runs `key` under, if it runs it.
fn runs(w: &World<Node>, on: &str, key: &str) -> Option<u64> {
    let (hosted, _) = cluster(w, on).hosted_keys("counter", key, None, 1);
    hosted.into_iter().find(|k| k.key == key).map(|k| k.epoch.expect("a leased key"))
}

/// How many keys of the counter kind ended on `on`, by reason.
fn ended(w: &World<Node>, on: &str) -> Vec<(&'static str, u64)> {
    let kinds = cluster(w, on).hosted(0).kinds;
    let ended = kinds.iter().find(|k| k.kind == "counter").map_or([0; KEYED_ENDS.len()], |k| k.ended);
    KEYED_ENDS.into_iter().zip(ended).filter(|(_, n)| *n > 0).collect()
}

/// What d answers for `key`, its epoch and total, and whether a new key d would own does not start there: refused as
/// isolated, or started on the next node of its order.
fn ask_d(w: &mut World<Node>, key: &str) -> (Option<(Option<u64>, u64)>, bool) {
    let key = key.to_owned();
    let asked = w.block_on("d", |n| async move {
        let (cluster, anywhere) = (n.cluster.unwrap(), Placement::default());
        let held = match cluster.get_or_spawn::<CounterApi>("counter", &key, start(0), &anywhere).await {
            Ok(placed) => placed.request(Op::Get).await.ok().map(|total| (placed.epoch(), total)),
            Err(_) => None,
        };
        let mut keys = (0..).map(|i| format!("new-{i}"));
        let new = keys.find(|k| cluster.owner("counter", k, &anywhere).as_deref() == Some("d")).expect("a key d leads");
        let refused = match cluster.get_or_spawn::<CounterApi>("counter", &new, start(0), &anywhere).await {
            Ok(placed) => placed.node() != "d",
            Err(e) => matches!(e, ClusterError::Spawn(SpawnRefusal::Isolated, _)),
        };
        (held, refused)
    });
    asked.unwrap()
}

/// Runs until d is isolated, then on to `until` after the cut, and checks that d still runs `key` under `epoch` and
/// answers for it, and refuses a key it does not run. Then heals, and returns how long d stayed isolated after it.
fn isolated_through(w: &mut World<Node>, seed: u64, cut: Duration, until: Duration, key: &str, epoch: u64) -> Duration {
    w.run_until("d isolates", Duration::from_secs(10), |w| cluster(w, "d").isolated());
    let isolated = w.now() - cut;
    assert!(isolated < until, "seed {seed}: isolated after {isolated:?}");
    w.run_for((cut + until).saturating_sub(w.now()));
    let (held, refused) = ask_d(w, key);
    assert!(cluster(w, "d").isolated(), "seed {seed}: isolated at {isolated:?}, not at {until:?}");
    assert_eq!(held, Some((Some(epoch), 7)), "seed {seed}: d isolated at {isolated:?}");
    assert!(refused, "seed {seed}: a new key started on d while isolated");
    w.heal();
    let healed = w.now();
    w.run_until("d rejoins", Duration::from_secs(20), |w| !cluster(w, "d").isolated());
    assert_eq!(runs(w, "d", key), Some(epoch), "seed {seed}");
    assert_eq!(ended(w, "d"), [], "seed {seed}");
    w.now() - healed
}

/// A holder cut off from every node for 7 s, past its isolation and short of its seeds' removal (8 to 9 s) and of its
/// `lease_grace`, keeps its key and answers for it, refuses a new one, and rejoins once heard for a whole
/// `heartbeat_grace`. Then, starved of CPU, cut from its seeds alone and far from its peers, its asks of them time out
/// while its renewals land: isolated, it keeps the key again (the timed-out peer check of the first pilot).
#[test]
fn a_leased_key_outlives_a_cut_shorter_than_its_lease_while_its_node_is_isolated() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, key, epoch) = isolated_holder(seed);
        let others = ["s0", "s1", "s2", "a", "b", "c"];
        w.partition(&["d"], &others);
        let cut = w.now();
        let back = isolated_through(&mut w, seed, cut, Duration::from_secs(7), &key, epoch);
        // Heard from its first heartbeat after the heal, or a peer's vouch, and rejoined a grace later.
        assert!(back >= Duration::from_secs(1), "seed {seed}: rejoined {back:?} after the heal");

        w.partition(&["d"], &SEEDS);
        // A round trip to a peer past the check's 500 ms and well short of a Paxos round's patience (3.3 s).
        let far = Wire { latency: Duration::from_millis(w.pick(300..400)), ..Wire::default() };
        for peer in ["a", "b", "c"] {
            w.wire("d", peer, far);
            w.wire(peer, "d", far);
        }
        let one_in = w.pick(2..5) as u32;
        w.starve("d", one_in);
        let cut = w.now();
        let starved = isolated_through(&mut w, seed, cut, Duration::from_secs(7), &key, epoch);
        w.starve("d", 1);
        println!("world digest isolated_holder seed {seed} starved {one_in} back {starved:?}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A holder cut off from every node for 30 s stops its key by its fence, `lease_grace` after its last renew, and not
/// as it isolates; the key then runs on another node at a higher epoch, never while d runs it.
#[test]
fn a_leased_key_cut_off_past_its_lease_stops_by_its_fence_and_runs_elsewhere() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, key, epoch) = isolated_holder(seed);
        let elsewhere = key.clone();
        let moved = w.spawn_on("a", |n| async move {
            let (cluster, anywhere) = (n.cluster.unwrap(), Placement::default());
            loop {
                match cluster.get_or_spawn::<CounterApi>("counter", &elsewhere, start(0), &anywhere).await {
                    Ok(placed) if placed.node() != "d" => return placed.epoch(),
                    _ => sleep(Duration::from_millis(200)).await,
                }
            }
        });
        w.partition(&["d"], &["s0", "s1", "s2", "a", "b", "c"]);
        let cut = w.now();
        let (mut isolated, mut stopped, mut started) = (None, None, None);
        w.run_until("the key runs elsewhere", Duration::from_secs(30), |w| {
            let on_d = runs(w, "d", &key).is_some();
            let others: Vec<_> = ["a", "b", "c"].into_iter().filter_map(|n| runs(w, n, &key)).collect();
            assert!(!on_d || others.is_empty(), "seed {seed}: d and {others:?} run it at once");
            if isolated.is_none() && cluster(w, "d").isolated() {
                isolated = Some(w.now() - cut);
            }
            if !on_d && stopped.is_none() {
                stopped = Some(w.now() - cut);
            }
            if started.is_none() && !others.is_empty() {
                started = Some(w.now() - cut);
            }
            started.is_some()
        });
        let (isolated, stopped) = (isolated.expect("d isolated"), stopped.expect("d stopped the key"));
        let mut paxos = PaxosConfig::new(["a"]);
        // The lease the nodes run, to read the `lease_grace` it gives.
        paxos.lease = LONG_LEASE;
        let grace = PaxosLeases::new(paxos).unwrap().hold().expect("a Paxos lease has a hold") * 7 / 8;
        assert!(isolated < stopped && stopped <= grace, "seed {seed}: isolated {isolated:?}, stopped {stopped:?}");
        assert_eq!(ended(&w, "d"), [("lease_lost", 1)], "seed {seed}");
        let moved = w.join(moved).unwrap().expect("a leased key");
        assert!(moved > epoch, "seed {seed}: {moved} after {epoch}");
        w.heal();
        w.run_until("d rejoins", Duration::from_secs(30), |w| !cluster(w, "d").isolated());
        println!("world digest past_lease seed {seed} stopped {stopped:?} moved {started:?}: {:016x}", w.digest());
    })
    .unwrap();
}

const PAXOS_LEASE: Duration = Duration::from_secs(1);

fn paxos(host: &Host, cfg: &mut ClusterConfig) {
    let mut paxos = PaxosConfig::new(["p-0", "p-1", "p-2"]);
    // A 1 s lease, so a lease runs out within the test's wait.
    paxos.lease = PAXOS_LEASE;
    paxos.store = Some(Arc::new(host.disk().dir("leases")));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
}

/// Keyed actors over Paxos leases, the acceptors on nodes of their own: a crashed holder's key runs elsewhere once its
/// lease runs out, and a holder that loses a majority, to crashes, a partition or acceptors that can accept no
/// connection, stops its actor before the acceptors could grant the key again. Its proposer declares the lease lost;
/// the grace timer alone is left to `tests/cluster/leases.rs`.
/// A holder paused for less than its grace keeps the key, and one whose machine went silent loses it with its lease,
/// although the survivor holds a session to it: finding 3 of the S8 results in spec/simulation.md.
fn paxos_leases(seed: u64) {
    let (hosts, all) = (["a", "b"], ["p-0", "p-1", "p-2", "a", "b"]);
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &all, &hosts, paxos);
    w.run_until("kinds are advertised", Duration::from_secs(30), |w| hosts.iter().all(|h| with_kind(w, h) == 2));

    // Acceptors refuse everything while they warm up after starting, then the key is granted.
    let (owner, epoch, _) = spawn_key(&mut w, "a", "k", None, PAXOS_LEASE * 5);
    let survivor = if owner == "a" { "b" } else { "a" };
    // The survivor holds a session to the holder since the first spawn, and a pause inside the grace does not end it.
    let pause = Duration::from_millis(w.pick(100..300));
    w.pause(&owner, pause);
    let (_, kept, _) = spawn_key(&mut w, survivor, "k", Some(&owner), PAXOS_LEASE);
    assert_eq!(kept, epoch, "seed {seed}: paused {pause:?}, the key moved");
    crash(&mut w, &owner);
    let (_, moved, took) = spawn_key(&mut w, survivor, "k", Some(survivor), PAXOS_LEASE * 8);
    assert!(moved > epoch, "seed {seed}: {moved:?} after {epoch:?}");
    assert!(took < silent_owner_move(Some(PAXOS_LEASE)), "seed {seed}: moved after {took:?}");

    let cut = w.now();
    match w.pick(0..3) {
        0 => {
            crash(&mut w, "p-1");
            crash(&mut w, "p-2");
        }
        1 => w.partition(&[survivor], &["p-1", "p-2"]),
        _ => {
            // Out of descriptors: the sessions the holder dialled go, and its redials wait in the acceptors' backlogs.
            w.log("p-1 and p-2 refuse accepts");
            for acceptor in ["p-1", "p-2"] {
                w.refuse_accepts(acceptor, true);
                w.reset(survivor, acceptor);
            }
        }
    }
    w.run_until("the actor stops", PAXOS_LEASE * 5, |w| cluster(w, survivor).stats().spawned == 0);
    let ran = w.now() - cut;
    assert!(ran < PAXOS_LEASE, "seed {seed}: ran {ran:?} without a majority");
    assert_eq!(cluster(&w, survivor).stats().leases_lost, 1, "seed {seed}");
    let again = w.block_on(survivor, |n| async move {
        n.cluster.unwrap().get_or_spawn::<CounterApi>("counter", "k", start(0), &Placement::default()).await
    });
    assert!(again.unwrap().is_err(), "seed {seed}: started without a majority");
    assert_eq!(cluster(&w, survivor).stats().spawned, 0, "seed {seed}");
    println!("world digest paxos_leases seed {seed}: {:016x}", w.digest());
}

#[test]
fn keyed_actors_move_and_stop_with_their_paxos_leases() {
    check_seeds(seeds(0..64), paxos_leases).unwrap();
}

fn crashed_owner(seed: u64) {
    let hosts = ["h1", "h2", "h3"];
    let all = ["h1", "h2", "h3", "caller"];
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &all, &hosts, |_, _| {});
    w.run_until("kinds are advertised", Duration::from_secs(30), |w| with_kind(w, "caller") == 3);
    let (gone, _, _) = spawn_key(&mut w, "caller", "k", None, Duration::from_secs(5));
    crash(&mut w, &gone); // its record stays listed
    w.run_until("the caller sees the owner unreachable", Duration::from_secs(10), |w| {
        cluster(w, "caller").view().get(&gone).is_some_and(|m| !m.reachable)
    });
    // The survivor asked tries the owner before it takes the key. A silent machine answers no SYN, and the dial gives
    // up on it at `connect_timeout`, as on a refusal, so the caller's first ask is answered.
    let transport = axon_net::Config::new("any", SharedSecret::new(SECRET).unwrap());
    let connect = ClusterConfig::new(([127, 0, 0, 1], 7000).into(), transport, MemoryDiscovery::new()).transport;
    let bound = connect.connect_timeout + Duration::from_millis(500);
    let moved = w.block_on("caller", |n| async move {
        let (cluster, started) = (n.cluster.unwrap(), Instant::now());
        let moved = cluster.get_or_spawn::<CounterApi>("counter", "k", start(0), &Placement::default()).await;
        let moved = moved.expect("moved on the first ask");
        (moved.node().to_owned(), moved.request(Op::Get).await, started.elapsed())
    });
    let (on, total, took) = moved.unwrap();
    assert!(on != gone && matches!(total, Ok(0)), "seed {seed}: on {on} with {total:?}");
    assert!(took < bound, "seed {seed}: moved after {took:?}");
    println!("world digest crashed_owner seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_crashed_owner_key_moves_although_the_survivors_never_called_it() {
    check_seeds(seeds(0..64), crashed_owner).unwrap();
}

/// A watcher on a node of its own, which neither hosts the key nor holds a lease, while the key starts, its owner
/// crashes or is cut off (its actor stuck past its lease in the last case), the key moves, and its new owner then
/// loses its lease.
fn watched_owner(seed: u64) {
    let (hosts, all) = (["a", "b"], ["p-0", "p-1", "p-2", "a", "b", "w"]);
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &all, &hosts, paxos);
    w.run_until("kinds are advertised", Duration::from_secs(30), |w| with_kind(w, "w") == 2);
    let seen: Rc<RefCell<Vec<Owner>>> = Rc::default();
    let log = seen.clone();
    drop(w.spawn_on("w", |n| async move {
        let mut watch = n.cluster.unwrap().watch_key("counter", "k", &Placement::default());
        while let Some(owner) = watch.next().await {
            log.borrow_mut().push(owner);
        }
    }));
    w.run_until("the watcher sees nothing run", Duration::from_secs(1), |_| !seen.borrow().is_empty());
    let (owner, epoch, _) = spawn_key(&mut w, "a", "k", None, PAXOS_LEASE * 5);
    let last = |on: &str| matches!(seen.borrow().last(), Some(Owner::Running { node, .. }) if **node == *on);
    // Told by the start, not at the end of the question's hold.
    w.run_until("the watcher sees the owner start", Duration::from_millis(200), |_| last(&owner));
    // Nothing changes, and the question is held open rather than asked again and again.
    let calls = |w: &World<Node>| cluster(w, "w").stats().engine.calls;
    let before = calls(&w);
    w.run_for(Duration::from_secs(3));
    let asked = calls(&w) - before;
    assert!(asked <= 5, "seed {seed}: asked {asked} times in 3 s");

    // The key moves when it is next called, as with no watcher.
    let survivor = if owner == "a" { "b" } else { "a" };
    let (cut, fault, bound) = (w.now(), w.pick(0..4), silent_owner_move(Some(PAXOS_LEASE)));
    w.log(format_args!("{owner} fault {fault}"));
    match fault {
        0 => w.crash(&owner, Crash::Process),
        1 => w.crash(&owner, Crash::Machine),
        _ => {
            if fault == 3 {
                // Its actor outlives the lease, still in the slot when the owner is back in view.
                w.hold_actors(&owner, Duration::from_secs(60));
            }
            let others: Vec<&str> = all.into_iter().filter(|n| *n != owner).collect();
            w.partition(&[&owner], &others);
        }
    }
    spawn_key(&mut w, survivor, "k", Some(survivor), bound);
    w.run_until("the watcher sees the new owner", (cut + bound).saturating_sub(w.now()), |_| last(survivor));
    let took = w.now() - cut;
    let shown = seen.borrow().clone();
    let running = |node: &str| Owner::Running { node: node.into(), epoch: None };
    let without_epochs: Vec<Owner> = shown
        .iter()
        .map(|o| if let Owner::Running { node, .. } = o { running(node) } else { Owner::Stopped })
        .collect();
    let owner_cannot_be_asked = [Owner::Stopped, running(&owner), Owner::Stopped, running(survivor)];
    assert_eq!(without_epochs, owner_cannot_be_asked, "seed {seed} fault {fault}: {shown:?}");

    // Its new owner loses a majority of the acceptors, and its actor stops with the lease: the watcher, which still
    // reaches it, is told at once.
    w.partition(&[survivor], &["p-1", "p-2"]);
    w.run_until("the actor stops", PAXOS_LEASE * 5, |w| cluster(w, survivor).stats().spawned == 0);
    w.run_until("the watcher sees it stop", Duration::from_millis(200), |_| {
        seen.borrow().last() == Some(&Owner::Stopped)
    });
    w.heal();
    w.run_for(Duration::from_secs(5));

    let seen = seen.borrow();
    let epochs: Vec<(&str, u64)> = seen
        .iter()
        .filter_map(|o| if let Owner::Running { node, epoch } = o { Some((&**node, (*epoch)?)) } else { None })
        .collect();
    assert_eq!(epochs.first().map(|e| e.1), epoch, "seed {seed}: {seen:?}");
    let forward = epochs.windows(2).all(|e| e[0].1 < e[1].1 || e[0] == e[1]);
    assert!(forward, "seed {seed}: an epoch went back or showed on two nodes: {seen:?}");
    assert!(seen.windows(2).all(|o| o[0] != o[1]), "seed {seed}: shown twice in a row: {seen:?}");
    // No node runs the key: the old owner, back in view, has only an actor whose lease it lost, if any.
    assert_eq!(seen.last(), Some(&Owner::Stopped), "seed {seed}: {seen:?}");
    println!("world digest watched_owner seed {seed} fault {fault} moved in {took:?}: {:016x}", w.digest());
}

#[test]
fn a_watcher_sees_a_crashed_or_cut_off_owner_key_move_and_never_one_epoch_on_two_nodes() {
    check_seeds(seeds(0..64), watched_owner).unwrap();
}

/// How long each `request_until` below is given: a key moves off a silent machine in about 1 s, and a restarted node
/// is back in its callers' views within a few hundred ms.
const CALL_BOUND: Duration = Duration::from_secs(3);

/// Calls `keyed` from the caller back to back, at least once and for at least `span`, each call given `CALL_BOUND`.
fn call_through(w: &mut World<Node>, keyed: &KeyedRef<CounterApi>, span: Duration) -> Result<usize, String> {
    let keyed = keyed.clone();
    let calls = w.block_on("caller", move |_| async move {
        let (started, mut answered) = (Instant::now(), 0);
        loop {
            let asked = Instant::now();
            let got = keyed.request_until(Op::Get, asked + CALL_BOUND).await;
            if got.is_err() || asked.elapsed() > CALL_BOUND {
                return Err(format!("{got:?} after {:?}, {answered} answered", asked.elapsed()));
            }
            answered += 1;
            if started.elapsed() >= span {
                return Ok(answered);
            }
        }
    });
    calls.unwrap()
}

/// A caller that keeps calling a key through `request_until` sees no error while the key's owner crashes, the key
/// moves to a survivor, the owner restarts and the key moves back to it. A reference called before the caller knows
/// the owner is gone answers once the key moves.
#[test]
fn a_keyed_request_until_answers_across_its_owners_crash_and_restart() {
    check_seeds(seeds(0..64), |seed| {
        let hosts = ["h1", "h2", "h3"];
        let all = ["h1", "h2", "h3", "caller"];
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &all, &hosts, |_, _| {});
        w.run_until("kinds are advertised", Duration::from_secs(30), |w| with_kind(w, "caller") == 3);
        let (owner, _, _) = spawn_key(&mut w, "caller", "k", None, Duration::from_secs(5));
        let keyed = w.block_on("caller", |n| async move {
            n.cluster.unwrap().keyed::<CounterApi>("counter", "k", start(0), &Placement::default())
        });
        let keyed = keyed.unwrap();
        call_through(&mut w, &keyed, Duration::from_millis(200)).unwrap();

        crash(&mut w, &owner);
        // A fresh reference, called while the silent owner is still in the view: finding it fails until the key moves.
        let within = silent_owner_move(None);
        let fresh = w.block_on("caller", move |n| async move {
            let fresh = n.cluster.unwrap().keyed::<CounterApi>("counter", "k", start(0), &Placement::default());
            let asked = Instant::now();
            (fresh.request_until(Op::Get, asked + within).await, asked.elapsed())
        });
        let fresh = fresh.unwrap();
        assert!(matches!(fresh.0, Ok(0)) && fresh.1 <= within, "seed {seed}: a fresh call: {fresh:?}");
        w.run_until("the caller sees the owner unreachable", Duration::from_secs(10), |w| {
            cluster(w, "caller").view().get(&owner).is_some_and(|m| !m.reachable)
        });
        let moved = call_through(&mut w, &keyed, Duration::ZERO);
        assert!(moved.is_ok(), "seed {seed}: after the crash: {moved:?}");

        w.restart(&owner);
        let back = call_through(&mut w, &keyed, Duration::from_secs(4));
        assert!(back.is_ok(), "seed {seed}: after the restart: {back:?}");
        assert_eq!(cluster(&w, &owner).stats().spawned, 1, "seed {seed}: the key moved back while it was called");
        println!("world digest keyed_request_until seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

fn named_call(w: &mut World<Node>, named: &NamedRef<CounterApi>, op: Op, within: Duration) -> Timed {
    let named = named.clone();
    timed(w, "a", move |_| async move { named.request_until(op, Instant::now() + within).await })
}

type Timed = (Result<u64, ClusterError>, Duration);

/// Waits for `on` to see `node` reachable or not.
fn wait_reach(w: &mut World<Node>, on: &str, node: &str, reachable: bool) {
    w.run_until("a reachability change", Duration::from_secs(20), |w| {
        cluster(w, on).view().get(node).is_some_and(|m| m.reachable == reachable)
    });
}

/// A `NamedRef` follows its name to another node and to the next boot of a node. Its `request_until` retries only
/// what was not delivered: an invalid message fails at once, an unanswered request at the request timeout (2 s) or
/// the deadline, and a name nobody publishes at the deadline with `NotFound`. A lookup that a silent node stalls is
/// tried again until the node is marked unreachable.
#[test]
fn a_named_ref_follows_its_name_across_nodes_and_restarts() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b", "c"], &[], |_, _| {});
        sees(&mut w, 3);
        publish_counter(&mut w, "b", 1);
        let (named, nobody) = w
            .block_on("a", |n| async move {
                let cluster = n.cluster.unwrap();
                (cluster.named::<CounterApi>("counter"), cluster.named::<CounterApi>("nobody"))
            })
            .unwrap();
        let bad = named_call(&mut w, &named, Op::Bad, CALL_BOUND * 5);
        let invalid = matches!(bad.0, Err(ClusterError::Call(CallError::Rejected(Rejection::InvalidMessage))));
        assert!(invalid && bad.1 < Duration::from_millis(100), "seed {seed}: {bad:?}");
        let timed_out = |t: &Timed| matches!(t.0, Err(ClusterError::Call(CallError::Timeout)));
        let held = named_call(&mut w, &named, Op::Hold, CALL_BOUND * 5);
        assert!(timed_out(&held) && held.1 < Duration::from_millis(2100), "seed {seed}: {held:?}");
        let cut = named_call(&mut w, &named, Op::Hold, Duration::from_millis(500));
        assert!(timed_out(&cut) && cut.1 <= Duration::from_millis(510), "seed {seed}: {cut:?}");
        let absent = named_call(&mut w, &nobody, Op::Get, Duration::from_secs(1));
        let not_found = matches!(absent.0, Err(ClusterError::NotFound));
        assert!(not_found && absent.1 <= Duration::from_secs(1), "seed {seed}: {absent:?}");
        let first = named_call(&mut w, &named, Op::Get, CALL_BOUND);
        assert!(matches!(first.0, Ok(1)), "seed {seed}: {first:?}");

        // A bystander stops answering: a fresh reference's lookup waits on it until it is marked unreachable.
        let pause = Duration::from_secs(10);
        w.pause("c", pause);
        let fresh = w.block_on("a", |n| async move { n.cluster.unwrap().named::<CounterApi>("counter") }).unwrap();
        let stalled = named_call(&mut w, &fresh, Op::Get, silent_owner_move(None));
        assert!(matches!(stalled.0, Ok(1)), "seed {seed}: with c silent: {stalled:?}");
        wait_reach(&mut w, "a", "c", true);

        // b crashes and c publishes the name: the call answers from c.
        crash(&mut w, "b");
        wait_reach(&mut w, "a", "b", false);
        let down = Duration::from_millis(w.pick(0..2000));
        let (moved, within) = (named.clone(), down + CALL_BOUND);
        let call = w.spawn_on("a", move |_| async move { moved.request_until(Op::Get, Instant::now() + within).await });
        w.run_for(down);
        publish_counter(&mut w, "c", 2);
        let moved = w.join(call).unwrap();
        assert!(matches!(moved, Ok(2)), "seed {seed}: moved to c: {moved:?}");

        // c crashes, and b's next boot publishes the name: the call follows it back.
        crash(&mut w, "c");
        wait_reach(&mut w, "a", "c", false);
        w.restart("b");
        publish_counter(&mut w, "b", 9);
        let back = named_call(&mut w, &named, Op::Get, CALL_BOUND);
        assert!(matches!(back.0, Ok(9)) && back.1 <= CALL_BOUND, "seed {seed}: back on b: {back:?}");
        println!("world digest named_ref seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A lookup sends nothing to the actor it looks for: a `KeyedRef` whose key's owner is silent fails at the request
/// timeout (2 s) with `NotSent`, `Retry::Later`, and not with the `Timeout` of a request that may have run. A
/// `NamedRef`'s lookup: the next test.
#[test]
fn a_lookup_that_a_silent_node_holds_up_fails_as_not_sent() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b", "c"], &["c"], |_, _| {});
        w.run_until("a sees b and c's kind", Duration::from_secs(30), |w| {
            cluster(w, "a").view().members.len() == 3 && with_kind(w, "a") == 1
        });
        publish_counter(&mut w, "b", 1);
        // With a session to c up, so the lookup is sent and goes unanswered.
        resolve(&mut w, "a");
        w.pause("c", Duration::from_secs(10));
        let keyed = w.block_on("a", |n| async move {
            let keyed = n.cluster.unwrap().keyed::<CounterApi>("counter", "k", start(0), &Placement::default());
            keyed.request(Op::Get).await.map(drop)
        });
        let keyed = keyed.unwrap();
        let unsent = matches!(&keyed, Err(e @ ClusterError::Call(CallError::NotSent(_))) if e.retry() == Retry::Later);
        assert!(unsent, "seed {seed}: {keyed:?}");
        println!("world digest unsent_lookup seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A `NamedRef` narrowed to the nodes labelled `role=control` asks only those: a silent node without the label holds
/// up the plain lookup until the request timeout (2 s), and not the narrowed one, nor `resolve_all_among`.
#[test]
fn a_lookup_among_labelled_nodes_does_not_wait_on_a_silent_node_without_the_label() {
    check_seeds(seeds(0..16), |seed| {
        let control = |host: &Host, cfg: &mut ClusterConfig| {
            if host.name() == "ctl" {
                cfg.labels.insert("role".into(), "control".into());
            }
        };
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "ctl", "r"], &[], control);
        sees(&mut w, 3);
        publish_counter(&mut w, "ctl", 1);
        // With a session to r up, so the plain lookup is sent to r and goes unanswered.
        resolve(&mut w, "a");
        w.pause("r", Duration::from_secs(10));
        let among = Placement::default().require("role", "control");
        let labelled = among.clone();
        let narrowed = timed(&mut w, "a", |n| async move {
            n.cluster.unwrap().named::<CounterApi>("counter").among(&among).request(Op::Get).await
        });
        let plain = |n: Node| async move { n.cluster.unwrap().named::<CounterApi>("counter").request(Op::Get).await };
        let plain = timed(&mut w, "a", plain);
        let all = w.block_on("a", |n| async move {
            let (found, failed) = n.cluster.unwrap().resolve_all_among::<CounterApi>("counter", &labelled).await;
            (found.iter().map(|r| r.node().to_owned()).collect::<Vec<_>>(), failed.len())
        });
        let all = all.unwrap();
        assert!(matches!(narrowed.0, Ok(1)) && narrowed.1 < Duration::from_millis(100), "seed {seed}: {narrowed:?}");
        let unsent = matches!(plain.0, Err(ClusterError::Call(CallError::NotSent(_))));
        assert!(unsent && plain.1 >= Duration::from_secs(2), "seed {seed}: plain: {plain:?}");
        assert!(all.0 == ["ctl"] && all.1 == 0, "seed {seed}: resolve_all_among: {all:?}");
        println!("world digest among seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A request refused `Busy`, `Retry::Later`, is tried again: with one pending call allowed, a call made while another
/// is held answers once the held one times out (2 s), through a `NamedRef` and through a `RemoteRef`.
#[test]
fn request_until_waits_out_a_busy_refusal() {
    check_seeds(seeds(0..64), |seed| {
        let one = |_: &Host, cfg: &mut ClusterConfig| cfg.max_pending = 1;
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], one);
        sees(&mut w, 2);
        publish_counter(&mut w, "b", 1);
        let named = w.block_on("a", |n| async move { n.cluster.unwrap().named::<CounterApi>("counter") }).unwrap();
        let first = named_call(&mut w, &named, Op::Get, CALL_BOUND);
        assert!(matches!(first.0, Ok(1)), "seed {seed}: {first:?}");
        let holder = named.clone();
        let _held = w.spawn_on("a", move |_| async move { holder.request(Op::Hold).await });
        w.run_until("the hold is out", Duration::from_secs(1), |w| cluster(w, "a").stats().engine.pending == 1);
        let busy = named_call(&mut w, &named, Op::Get, CALL_BOUND);
        assert!(matches!(busy.0, Ok(1)) && busy.1 > Duration::from_secs(1), "seed {seed}: {busy:?}");

        let (holder, remote) = (named.clone(), resolve(&mut w, "a"));
        let _held = w.spawn_on("a", move |_| async move { holder.request(Op::Hold).await });
        w.run_until("the hold is out", Duration::from_secs(1), |w| cluster(w, "a").stats().engine.pending == 1);
        let call = move |_| async move { remote.request_until(Op::Get, Instant::now() + CALL_BOUND).await };
        let busy = timed(&mut w, "a", call);
        assert!(matches!(busy.0, Ok(1)) && busy.1 > Duration::from_secs(1), "seed {seed}: remote: {busy:?}");
        println!("world digest busy seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A silent machine fails only its own lookup: the other instances come back within the request timeout (2 s).
#[test]
fn resolve_all_returns_the_live_instances_and_names_a_silent_node_within_the_call_timeout() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b", "c"], &[], |_, _| {});
        sees(&mut w, 3);
        publish_counter(&mut w, "b", 1);
        publish_counter(&mut w, "c", 2);
        w.log("c Machine");
        w.crash("c", Crash::Machine);
        let (found, failed, took) = w
            .block_on("a", |n| async move {
                let started = Instant::now();
                let (found, failed) = n.cluster.unwrap().resolve_all::<CounterApi>("counter").await;
                (found, failed, started.elapsed())
            })
            .unwrap();
        let found: Vec<_> = found.iter().map(|r| r.node().to_owned()).collect();
        let failed: Vec<_> = failed.iter().map(|(node, e)| format!("{node}: {e:?}")).collect();
        assert!(
            found == ["b"] && failed.len() == 1 && failed[0].starts_with("c: "),
            "seed {seed}: {found:?} {failed:?}"
        );
        assert!(took < Duration::from_millis(2100), "seed {seed}: took {took:?}");
        println!("world digest resolve_all seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Runs `(id, hold)`: records the id and whether its caller still waited when it came in, then answers the id, at once
/// or after `hold`, when it records whether the caller had given up by then. A tell is only recorded.
struct Deadlines(Arc<Mutex<Vec<Ran>>>);

#[derive(Clone, Copy, Debug)]
struct Ran {
    id: u64,
    fresh: bool,
    abandoned: Option<bool>,
}

enum Work {
    Run(u64, Duration, Option<Reply<u64>>),
    Answer(usize, Reply<u64>),
}

impl Actor for Deadlines {
    type Message = Work;
    type Error = ();

    fn on_message(&mut self, work: Work, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let mut ran = self.0.lock().unwrap();
        match work {
            Work::Run(id, hold, reply) => {
                ran.push(Ran { id, fresh: reply.as_ref().is_none_or(|r| !r.is_abandoned()), abandoned: None });
                let Some(reply) = reply else { return Ok(()) };
                if hold.is_zero() {
                    return reply.send(id).map_err(drop);
                }
                cx.schedule_after(hold, Work::Answer(ran.len() - 1, reply)).map_err(drop)?;
            }
            Work::Answer(at, reply) => {
                let abandoned = reply.is_abandoned();
                ran[at].abandoned = Some(abandoned);
                if !abandoned {
                    return reply.send(ran[at].id).map_err(drop);
                }
            }
        }
        Ok(())
    }
}

struct DeadlineApi;

impl WireProtocol for DeadlineApi {
    type Message = (u64, Duration);
    type Reply = u64;
    const ID: u64 = 0xDE;
    const SCHEMA: u16 = 1;

    fn encode((id, hold): (u64, Duration)) -> Result<Bytes, CallError> {
        Ok([id.to_be_bytes(), (hold.as_millis() as u64).to_be_bytes()].concat().into())
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("short reply".into()))
    }
}

impl ActorProtocol for DeadlineApi {
    type Mailbox = Work;

    fn encode_reply(id: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&id.to_be_bytes()))
    }

    fn decode(mut body: Bytes, reply: Option<Reply<u64>>) -> Result<Work, Rejection> {
        let (id, hold) = (body.try_get_u64(), body.try_get_u64());
        match (id, hold, reply) {
            (Ok(id), Ok(hold), reply) => Ok(Work::Run(id, Duration::from_millis(hold), reply)),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

/// A request's deadline travels with it, and the far node reads it on its own clock less half the session's recent
/// round trip for the way there. On a wire of 20 to 80 ms each way, with the two clocks 1 % apart, a request whose
/// budget runs out on the way is never run and is counted, one that arrives in time runs once, and the caller hears
/// `Ok` only from one that ran. A handler holding a reply past the deadline, or past its caller's session, finds it
/// abandoned. A pause of the caller does not make the next request look late, and the budget covers a lookup.
#[test]
fn a_request_runs_once_within_its_deadline_and_never_once_it_arrives_past_it() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], |_, _| {});
        sees(&mut w, 2);
        let (way, jitter) = (Duration::from_millis(w.pick(20..81)), Duration::from_millis(5));
        for (from, to) in [("a", "b"), ("b", "a")] {
            w.wire(from, to, Wire { latency: way, jitter, ..Wire::default() });
        }
        let fast = if w.pick(0..2) == 0 { ["a", "b"] } else { ["b", "a"] };
        w.drift(fast[0], 5000);
        w.drift(fast[1], -5000);
        let ran = w.block_on("b", |n| async move {
            let ran = Arc::new(Mutex::new(Vec::new()));
            let actor = n.system.unwrap().spawn(Deadlines(ran.clone())).start().unwrap();
            n.cluster.unwrap().publish::<DeadlineApi>(&actor, "deadlines").unwrap();
            ran
        });
        let log = ran.unwrap();
        let within = |named: NamedRef<DeadlineApi>, id, hold, budget| {
            move |_: Node| async move { named.request_within((id, hold), budget).await }
        };
        let named = w.block_on("a", |n| async move { n.cluster.unwrap().named::<DeadlineApi>("deadlines") }).unwrap();
        let first = named.clone();
        let first = w.block_on("a", |_| async move { first.request((0, Duration::ZERO)).await }).unwrap();
        assert!(matches!(first, Ok(0)), "seed {seed}: {first:?}");
        // Until four keepalives have measured the round trip at the new latency.
        w.run_for(Duration::from_millis(4500));

        // Quanta, jitter, and 1 % of the longest budget.
        let margin = jitter + Duration::from_millis(7);
        let answered = 2 * (way + jitter) + margin;
        let mut calls = Vec::new();
        for id in 1..=40u64 {
            let budget = match id {
                1 => Duration::from_millis(1),
                2 => answered,
                _ => Duration::from_millis(w.pick(1..4 * way.as_millis() as u64 + 40)),
            };
            let hold = if id > 2 && w.pick(0..4) == 0 { budget + Duration::from_millis(20) } else { Duration::ZERO };
            let (got, took) = timed(&mut w, "a", within(named.clone(), id, hold, budget));
            calls.push((id, budget, hold, got, took));
        }
        w.run_for(Duration::from_secs(1));

        let ran = log.lock().unwrap().clone();
        for (id, budget, hold, got, took) in &calls {
            let runs: Vec<_> = ran.iter().filter(|r| r.id == *id).collect();
            let why = format!(
                "seed {seed}, {way:?} each way: request {id} within {budget:?}, held {hold:?}: {got:?} after {took:?}, \
                 ran {runs:?}"
            );
            assert!(runs.len() <= 1, "{why}");
            assert!(*took <= *budget + Duration::from_millis(2), "{why}");
            match got {
                Ok(answer) => assert!(answer == id && runs.len() == 1, "{why}"),
                Err(e) => assert!(matches!(e, ClusterError::Call(CallError::Timeout)), "{why}"),
            }
            if *budget + margin <= way {
                assert!(runs.is_empty(), "{why}");
            }
            if *budget >= way + margin {
                assert!(runs.len() == 1 && runs[0].fresh, "{why}");
            }
            if !hold.is_zero() && !runs.is_empty() {
                assert_eq!(runs[0].abandoned, Some(true), "{why}");
            }
            if hold.is_zero() && *budget >= answered {
                assert!(got.is_ok(), "{why}");
            }
        }
        let expired = cluster(&w, "b").stats().engine.inbound_expired;
        assert_eq!(expired as usize + ran.len(), calls.len() + 1, "seed {seed}: every request ran or was counted");

        let run_of = |id| log.lock().unwrap().iter().find(|r| r.id == id).copied();

        // b's keepalive pong waits out a's pause, so that sample is long, but the shortest is what b takes off.
        w.pause("a", Duration::from_millis(1200));
        w.run_for(Duration::from_millis(1300));
        let (got, took) = timed(&mut w, "a", within(named.clone(), 41, Duration::ZERO, answered));
        assert!(matches!(got, Ok(41)), "seed {seed}: right after a pause, {got:?} after {took:?}");

        // `d` covers the lookup, and a name not looked up yet costs a round trip.
        let fresh = w.block_on("a", |n| async move { n.cluster.unwrap().named::<DeadlineApi>("deadlines") }).unwrap();
        let (got, took) = timed(&mut w, "a", within(fresh, 42, Duration::ZERO, Duration::from_millis(10)));
        let deadline = matches!(got, Err(ClusterError::Call(CallError::NotSent(axon_net::SubmitReason::Deadline))));
        assert!(deadline && took <= Duration::from_millis(12), "seed {seed}: lookup: {got:?} after {took:?}");

        // On the actor's own node, the caller that stopped waiting shows at once.
        let near = w.block_on("b", |n| async move { n.cluster.unwrap().named::<DeadlineApi>("deadlines") }).unwrap();
        let (got, _) = timed(&mut w, "b", within(near.clone(), 43, Duration::ZERO, Duration::from_secs(1)));
        assert!(matches!(got, Ok(43)), "seed {seed}: local: {got:?}");
        let (got, _) = timed(&mut w, "b", within(near, 44, Duration::from_millis(50), Duration::from_millis(20)));
        w.run_for(Duration::from_millis(100));
        let local = run_of(44);
        assert!(got.is_err() && local.is_some_and(|r| r.fresh && r.abandoned == Some(true)), "seed {seed}: {local:?}");

        // A caller whose session goes has stopped waiting, with a deadline or without.
        let held = named.clone();
        let task = w.spawn_on("a", move |_| async move { held.request((45, Duration::from_millis(500))).await });
        w.run_until("45 comes in", Duration::from_secs(1), |_| run_of(45).is_some());
        w.reset("a", "b");
        w.run_for(Duration::from_secs(1));
        let got = w.join(task).unwrap();
        let cut = run_of(45);
        assert!(got.is_err() && cut.is_some_and(|r| r.abandoned == Some(true)), "seed {seed}: {got:?} {cut:?}");
        println!("world digest deadlines seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Keepalives at 500 ms and an idle timeout of 1.5 s, so a silent peer is marked within about 2 s.
fn quick(_: &Host, cfg: &mut ClusterConfig) {
    // Short, as the doc says; axon-stream's world.rs runs the shipped timings.
    cfg.transport.keepalive_interval = Duration::from_millis(500);
    cfg.transport.idle_timeout = Duration::from_millis(1500);
    // The redial after a silent session gives up at it: a keepalive, as the bounds below were set for.
    cfg.transport.connect_timeout = Duration::from_millis(500);
}

#[derive(Clone, Copy, Debug)]
enum PeerFault {
    /// What a sends b is held, acknowledgements and dials included.
    CutTo,
    CutFrom,
    /// b is stepped once in 100 ms.
    Starved,
    /// b is stepped once in 3 to 5 s, which its keepalives cannot outlast.
    Stalled,
    /// b is out of descriptors: its sessions are reset and it accepts no new one.
    Refusing,
    /// Both clocks 0.5 % apart and b's wall clock stepped a minute.
    Clocks,
}

/// A peer that one direction of the wire, a starved CPU or its descriptors silence is marked unreachable and every call to it
/// ends within the request timeout with an error that says to try again; one that is only slow or off-clock is never
/// marked and answers every call. A node that accepts nothing still reaches its peers, which accept it. Once the fault
/// ends, calls answer again within the redial cap.
fn peer_fault(seed: u64) {
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], quick);
    sees(&mut w, 2);
    publish_counter(&mut w, "b", 1);
    let remote = resolve(&mut w, "a");
    assert_eq!(get(&mut w, &remote).unwrap(), 1);

    use PeerFault::*;
    let faults = [CutTo, CutFrom, Starved, Stalled, Refusing, Clocks];
    let fault = faults[w.pick(0..faults.len() as u64) as usize];
    w.log(format_args!("fault {fault:?}"));
    match fault {
        CutTo => w.cut(&["a"], &["b"]),
        CutFrom => w.cut(&["b"], &["a"]),
        Starved => w.starve("b", 100),
        Stalled => {
            let one_in = w.pick(3000..5000) as u32;
            w.starve("b", one_in);
        }
        Refusing => {
            w.refuse_accepts("b", true);
            w.reset("a", "b");
        }
        Clocks => off_clock(&mut w),
    }
    let silent = matches!(fault, CutTo | CutFrom | Stalled | Refusing);
    let calling = remote.clone();
    let calls = w.block_on("a", |_| async move {
        let (started, mut calls) = (Instant::now(), Vec::new());
        while started.elapsed() < Duration::from_secs(8) {
            let asked = Instant::now();
            let got = calling.request(Op::Get).await;
            calls.push((got, asked.elapsed()));
            sleep(Duration::from_millis(50)).await;
        }
        calls
    });
    for (got, took) in calls.unwrap() {
        let why = format!("seed {seed} {fault:?}: {got:?} after {took:?}");
        assert!(took <= Duration::from_millis(2100), "{why}");
        match got {
            Ok(total) => assert_eq!(total, 1, "{why}"),
            Err(e) => assert!(silent && e.retry() != Retry::Never, "{why}"),
        }
    }
    let marked = (cluster(&w, "a").stats().became_unreachable, cluster(&w, "b").stats().became_unreachable);
    if silent {
        assert!(marked.0 >= 1, "seed {seed} {fault:?}: b was never marked");
    } else {
        assert_eq!(marked, (0, 0), "seed {seed} {fault:?}: marked unreachable");
    }
    match fault {
        CutTo | CutFrom => w.heal(),
        Refusing => {
            let reachable = cluster(&w, "b").view().get("a").is_some_and(|m| m.reachable);
            assert!(reachable, "seed {seed}: b lost a, which still accepts it");
            w.refuse_accepts("b", false);
        }
        Starved | Stalled => w.starve("b", 1),
        Clocks => {}
    }
    let took = through_again(&mut w, remote);
    assert!(took < Duration::from_secs(3), "seed {seed} {fault:?}: answered {took:?} after the fault ended");
    wait_reach(&mut w, "a", "b", true);
    wait_reach(&mut w, "b", "a", true);
    println!("world digest peer_fault seed {seed} {fault:?}: {:016x}", w.digest());
}

#[test]
fn a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent() {
    check_seeds(seeds(0..64), peer_fault).unwrap();
}

/// Calls over a wire that stalls one chunk in 20 for 200 ms, the sessions between the two nodes reset three times on
/// the way: loss as TCP shows it, and the new sessions it brings. Nothing runs twice, the tells that arrive do in the
/// order they were sent, a caller hears `Ok` only from a request that ran, and the key starts once.
#[test]
fn calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], quick);
        sees(&mut w, 2);
        let logs = w.block_on("b", |n| async move {
            let (named, keyed) = (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(Vec::new())));
            let starts = Arc::new(AtomicU64::new(0));
            let cluster = n.cluster.unwrap();
            let actor = n.system.unwrap().spawn(Deadlines(named.clone())).start().unwrap();
            cluster.publish::<DeadlineApi>(&actor, "log").unwrap();
            let (log, started) = (keyed.clone(), starts.clone());
            let build = move |_: &SpawnContext, _: Bytes| {
                started.fetch_add(1, Relaxed);
                Ok(Deadlines(log.clone()))
            };
            cluster.kind::<DeadlineApi>("log").register(build).await.unwrap();
            (named, keyed, starts)
        });
        let (named_log, keyed_log, starts) = logs.unwrap();
        w.run_until("a sees the kind", Duration::from_secs(10), |w| {
            cluster(w, "a").view().members.iter().any(|m| m.record.has_kind("log"))
        });
        let lossy = Wire {
            jitter: Duration::from_millis(5),
            stall_every: 20,
            stall: Duration::from_millis(200),
            syn_stall: Duration::from_millis(200),
            ..Wire::default()
        };
        both_ways(&mut w, lossy);
        let calls = w.spawn_on("a", |n| async move {
            let cluster = n.cluster.unwrap();
            let named = cluster.named::<DeadlineApi>("log");
            let keyed = cluster.keyed::<DeadlineApi>("log", "k", Bytes::new(), &Placement::default());
            let mut calls = Vec::new();
            for id in 1..=200u64 {
                let message = (id, Duration::ZERO);
                let got = match id % 4 {
                    1 => named.tell(message).await.map(|()| None),
                    3 => keyed.tell(message).await.map(|()| None),
                    0 => named.request_within(message, CALL_BOUND).await.map(Some),
                    _ => keyed.request_within(message, CALL_BOUND).await.map(Some),
                };
                calls.push((id, got));
                sleep(Duration::from_millis(10)).await;
            }
            calls
        });
        for _ in 0..3 {
            let before = Duration::from_millis(w.pick(200..1500));
            w.run_for(before);
            w.reset("a", "b");
        }
        let calls = w.join(calls).unwrap();
        w.run_for(Duration::from_secs(1));

        let (named_log, keyed_log) = (named_log.lock().unwrap().clone(), keyed_log.lock().unwrap().clone());
        for (log, of) in [(&named_log, "named"), (&keyed_log, "keyed")] {
            let ids: Vec<u64> = log.iter().map(|r| r.id).collect();
            let tells: Vec<u64> = ids.iter().copied().filter(|id| id % 2 == 1).collect();
            assert!(tells.windows(2).all(|t| t[0] < t[1]), "seed {seed}: {of} tells out of order: {tells:?}");
            let mut once = ids.clone();
            once.sort_unstable();
            once.dedup();
            assert_eq!(once.len(), ids.len(), "seed {seed}: {of} ran one twice: {ids:?}");
        }
        let mut answered = 0;
        for (id, got) in &calls {
            let log = if id % 4 < 2 { &named_log } else { &keyed_log };
            match got {
                Ok(Some(answer)) => {
                    assert!(answer == id && log.iter().any(|r| r.id == *id), "seed {seed}: {id} answered {answer}");
                    answered += 1;
                }
                Ok(None) => {}
                Err(e) => assert!(e.retry() != Retry::Never, "seed {seed}: {id}: {e:?}"),
            }
        }
        assert!(answered >= 50, "seed {seed}: {answered} of 100 requests answered");
        assert_eq!(starts.load(Relaxed), 1, "seed {seed}: the key started more than once");
        println!("world digest lossy_calls seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpawnFault {
    Crash,
    Partition,
    CutTo,
    CutFrom,
    /// Stalls on the wire and every session reset.
    Loss,
    Latency,
    Clocks,
    Starved,
    Refusing,
    /// The kind's factory panics.
    Panic,
}

/// Twelve remote starts from a to b, 10 ms apart, with a fault landing among them. Each ends within the request
/// timeout. A b stepped once in 100 ms, 200 ms away or off-clock starts every one; a panicking factory refuses every one, and runs nothing.
/// Otherwise an error says to try again, b runs every start a caller was told of and no more than the ones whose
/// outcome is unknown besides, and each reference reaches the actor it started, whatever order a jittered wire and
/// its new sessions delivered them in. Once the fault ends each unknown start sent again runs once more. After the
/// fault a start works, and references from before a crash say to look the actor up again.
fn spawn_under(seed: u64) {
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &["b"], quick);
    w.run_until("a sees the kind", Duration::from_secs(30), |w| with_kind(w, "a") == 1);
    let boom = w.block_on("b", |n| async move {
        let build = |_: &SpawnContext, _: Bytes| -> Result<Counter, String> { panic!("a factory that panics") };
        n.cluster.unwrap().kind::<CounterApi>("boom").register(build).await
    });
    boom.unwrap().unwrap();

    use SpawnFault::*;
    let faults = [Crash, Partition, CutTo, CutFrom, Loss, Latency, Clocks, Starved, Refusing, Panic];
    let fault = faults[w.pick(0..faults.len() as u64) as usize];
    let kind = if fault == Panic { "boom" } else { "counter" };
    let starts: Vec<_> = (0..12u64)
        .map(|i| {
            w.spawn_on("a", move |n| async move {
                sleep(Duration::from_millis(i * 10)).await;
                let asked = Instant::now();
                let got = n.cluster.unwrap().spawn_on::<CounterApi>("b", kind, start(i)).await;
                (i, got, asked.elapsed())
            })
        })
        .collect();
    let at = Duration::from_millis(w.pick(0..120));
    w.run_for(at);
    w.log(format_args!("fault {fault:?} at {at:?}"));
    match fault {
        Crash => crash(&mut w, "b"),
        Partition => w.partition(&["a"], &["b"]),
        CutTo => w.cut(&["a"], &["b"]),
        CutFrom => w.cut(&["b"], &["a"]),
        Loss => {
            let jitter = Duration::from_millis(20);
            let stall = Duration::from_millis(200);
            both_ways(&mut w, Wire { jitter, stall_every: 5, stall, syn_stall: stall, ..Wire::default() });
            w.reset("a", "b");
        }
        Latency => {
            both_ways(&mut w, Wire { latency: Duration::from_millis(200), ..Wire::default() });
        }
        Clocks => off_clock(&mut w),
        Starved => w.starve("b", 100),
        Refusing => {
            w.refuse_accepts("b", true);
            w.reset("a", "b");
        }
        Panic => {}
    }
    let started: Vec<_> = starts.into_iter().map(|s| w.join(s).unwrap()).collect();
    let (mut refs, mut unknown) = (Vec::new(), Vec::new());
    for (i, got, took) in started {
        let why = format!("seed {seed} {fault:?}: start {i}: {got:?} after {took:?}");
        assert!(took <= Duration::from_millis(2100), "{why}");
        match got {
            Ok(remote) => {
                assert!(fault != Panic, "{why}");
                refs.push((i, remote));
            }
            Err(ClusterError::Spawn(SpawnRefusal::Failed, _)) if fault == Panic => {}
            Err(e) => {
                assert!(!matches!(fault, Latency | Clocks | Starved | Panic), "{why}");
                assert!(e.retry() != Retry::Never, "{why}");
                if e.retry() == Retry::IfIdempotent {
                    unknown.push(i);
                }
            }
        }
    }
    match fault {
        Crash => w.restart("b"),
        Partition | CutTo | CutFrom => w.heal(),
        Refusing => w.refuse_accepts("b", false),
        Starved => w.starve("b", 1),
        _ => {}
    }
    w.run_until("a sees the kind again", Duration::from_secs(10), |w| {
        with_kind(w, "a") == 1 && cluster(w, "a").view().get("b").is_some_and(|m| m.reachable)
    });
    // Starts held by a cut arrive now, if their session is still up.
    w.run_for(Duration::from_secs(3));
    if fault == Crash {
        assert_eq!(cluster(&w, "b").stats().spawned, 0, "seed {seed}");
    } else {
        let (spawned, ok) = (cluster(&w, "b").stats().spawned, refs.len());
        let counted = ok <= spawned && spawned <= ok + unknown.len();
        assert!(counted, "seed {seed} {fault:?}: {ok} told, {unknown:?} unknown, {spawned} run");
    }
    for (i, remote) in refs {
        let got = get(&mut w, &remote);
        let why = format!("seed {seed} {fault:?}: start {i}: {got:?}");
        match fault {
            Crash => assert!(got.is_err_and(|e| e.retry() == Retry::Resolve), "{why}"),
            _ => assert_eq!(got.ok(), Some(i), "{why}"),
        }
    }
    // After the fault a fresh start works, and so do the caller's own retries, the duplicates the World knows: each
    // start whose outcome was unknown, sent again, runs once more.
    let before = cluster(&w, "b").stats().spawned;
    for i in unknown.iter().copied().chain([99]) {
        let again = w.block_on("a", move |n| async move {
            let remote = n.cluster.unwrap().spawn_on::<CounterApi>("b", "counter", start(i)).await?;
            remote.request(Op::Get).await.map_err(ClusterError::Call)
        });
        let again = again.unwrap();
        assert!(matches!(again, Ok(n) if n == i), "seed {seed} {fault:?}: start {i} after the fault: {again:?}");
    }
    let after = cluster(&w, "b").stats().spawned;
    assert_eq!(after, before + unknown.len() + 1, "seed {seed} {fault:?}: {unknown:?} sent again");
    println!("world digest spawn_under seed {seed} {fault:?}: {:016x}", w.digest());
}

#[test]
fn a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told() {
    check_seeds(seeds(0..64), spawn_under).unwrap();
}

/// Memory leases shared by two nodes, as in one process: nothing expires by itself, so a crashed holder's key is
/// refused to the other node and to the holder's own next boot until the old boot's leases are expired, and then
/// starts under a newer epoch.
#[test]
fn a_crashed_holders_memory_leases_keep_its_keys_until_they_are_expired() {
    check_seeds(seeds(0..64), |seed| {
        let (leases, boots) = (MemoryLeases::new(), Rc::new(RefCell::new(Vec::new())));
        let (shared, kept) = (leases.clone(), boots.clone());
        let memory = move |host: &Host, cfg: &mut ClusterConfig| {
            quick(host, cfg);
            let authority = Arc::new(shared.authority());
            kept.borrow_mut().push((cfg.transport.identity.node_id.clone(), authority.clone()));
            cfg.ownership = Some(authority);
        };
        let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &["a", "b"], memory);
        w.run_until("kinds are advertised", Duration::from_secs(30), |w| {
            with_kind(w, "a") == 2 && with_kind(w, "b") == 2
        });
        let (owner, epoch, _) = spawn_key(&mut w, "a", "k", None, Duration::from_secs(5));
        let other = if owner == "a" { "b" } else { "a" };
        let old = boots.borrow().iter().rev().find(|(node, _)| *node == owner).map(|b| b.1.clone()).unwrap();
        crash(&mut w, &owner);

        let refused = |w: &mut World<Node>, on: &str| {
            let tried = w.block_on(on, |n| async move {
                let (cluster, started) = (n.cluster.unwrap(), Instant::now());
                while started.elapsed() < Duration::from_secs(3) {
                    let anywhere = Placement::default();
                    if let Ok(r) = cluster.get_or_spawn::<CounterApi>("counter", "k", start(0), &anywhere).await {
                        return Some(r.node().to_owned());
                    }
                    sleep(Duration::from_millis(100)).await;
                }
                None
            });
            tried.unwrap()
        };
        assert_eq!(refused(&mut w, other), None, "seed {seed}: started on the other node while {owner} held it");
        w.restart(&owner);
        w.run_until("the owner is back", Duration::from_secs(10), |w| {
            w.nodes().any(|(node, _)| node == owner)
                && with_kind(w, &owner) == 2
                && cluster(w, other).view().get(&owner).is_some_and(|m| m.reachable)
        });
        assert_eq!(refused(&mut w, &owner), None, "seed {seed}: the next boot took its old boot's key");
        leases.expire(&old);
        let (_, moved, _) = spawn_key(&mut w, other, "k", None, Duration::from_secs(5));
        assert!(moved > epoch, "seed {seed}: {moved:?} after {epoch:?}");
        println!("world digest memory_leases seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum SeedFault {
    /// What a sends its seeds is held.
    CutToSeeds,
    CutFromSeeds,
    /// a is stepped once in 100 ms.
    Starved,
    /// s1 is stepped once in 3 to 5 s.
    StalledSeed,
    /// s1 is out of descriptors: its sessions are reset and it accepts no new one.
    RefusingSeed,
}

/// Faults between nodes and their seeds. A node whose heartbeats or whose answers one direction of the wire holds is
/// dropped from its peer's view and joins again once the cut heals. A node stepped once in 100 ms stays in view. One
/// seed of three stalled or out of descriptors is not missed, and a node that restarts meanwhile joins through the
/// other two.
fn seed_fault(seed: u64) {
    let mut w = world(seed, &Find::Seeds(&SEEDS, SeedTimings::default()), &["a", "b"], &[], |_, _| {});
    let whole = |w: &World<Node>| {
        let up = |n: &str| w.nodes().any(|(node, m)| node == n && m.cluster.is_some());
        ["a", "b"].iter().all(|n| up(n) && cluster(w, n).view().members.len() == 2)
    };
    w.run_until("a and b see each other", Duration::from_secs(30), whole);
    use SeedFault::*;
    let faults = [CutToSeeds, CutFromSeeds, Starved, StalledSeed, RefusingSeed];
    let fault = faults[w.pick(0..faults.len() as u64) as usize];
    w.log(format_args!("fault {fault:?}"));
    match fault {
        CutToSeeds => w.cut(&["a"], &SEEDS),
        CutFromSeeds => w.cut(&SEEDS, &["a"]),
        Starved => w.starve("a", 100),
        StalledSeed => {
            let one_in = w.pick(3000..5000) as u32;
            w.starve("s1", one_in);
        }
        RefusingSeed => {
            w.refuse_accepts("s1", true);
            w.reset("s1", "a");
            w.reset("s1", "b");
        }
    }
    let cut = w.now();
    match fault {
        CutToSeeds | CutFromSeeds => {
            w.run_until("b drops a", Duration::from_secs(30), |w| cluster(w, "b").view().get("a").is_none());
            w.heal();
        }
        Starved | StalledSeed | RefusingSeed => {
            w.run_until("15 s pass", Duration::from_secs(16), |w| {
                assert!(whole(w), "seed {seed} {fault:?}: a view lost a node after {:?}", w.now() - cut);
                w.now() - cut >= Duration::from_secs(15)
            });
            if let RefusingSeed = fault {
                w.crash("a", Crash::Process);
                w.restart("a");
            }
        }
    }
    w.run_until("a and b see each other again", Duration::from_secs(30), whole);
    println!("world digest seed_fault seed {seed} {fault:?}: {:016x}", w.digest());
}

#[test]
fn a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed() {
    check_seeds(seeds(0..64), seed_fault).unwrap();
}

/// Answers `len` with that many bytes, in parts of 256 KiB, each part filled with its index.
struct BlobApi;

impl WireProtocol for BlobApi {
    type Message = u32;
    type Reply = Bytes;
    const ID: u64 = 0xB10B;
    const SCHEMA: u16 = 1;

    fn encode(len: u32) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&len.to_be_bytes()))
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for BlobApi {
    type Mailbox = (u32, Option<Reply<Bytes>>);

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(mut body: Bytes, reply: Option<Reply<Bytes>>) -> Result<Self::Mailbox, Rejection> {
        body.try_get_u32().map(|len| (len, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

const PART: u32 = 256 << 10;

/// The parts of `len` bytes, made as they are pulled.
struct Parted {
    len: u32,
    at: u32,
}

impl axon::Stream for Parted {
    type Item = Result<Bytes, CallError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let (at, end) = (self.at, self.len.min(self.at + PART));
        self.at = end;
        std::task::Poll::Ready((at < end).then(|| Ok(Bytes::from(vec![(at / PART) as u8; (end - at) as usize]))))
    }
}

struct Blob;

impl Actor for Blob {
    type Message = (u32, Option<Reply<Bytes>>);
    type Error = ();

    fn on_message(&mut self, (len, reply): Self::Message, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let Some(reply) = reply else { return Ok(()) };
        let sending = reply.send_parts(Parted { len, at: 0 });
        // What the future returns is not needed: each cut is the caller's to see.
        cx.submit_future(async move {
            drop(sending.await);
            (0, None)
        })
        .map(drop)
        .map_err(drop)
    }
}

/// Reads a reply in parts of `len` bytes, counting what it has read in `read`: the length and how it ended.
async fn read_parts(
    remote: RemoteRef<BlobApi>,
    len: u32,
    read: Arc<AtomicU64>,
) -> Result<(usize, Option<CallError>), CallError> {
    let mut parts = remote.request_parts(len).await?;
    let mut have = 0;
    while let Some(part) = parts.next().await {
        match part {
            Ok(part) => {
                let index = (have / PART as usize) as u8;
                assert!(part.iter().all(|b| *b == index), "part {index} in order");
                have += part.len();
                read.store(have as u64, Relaxed);
            }
            Err(e) => {
                assert!(parts.next().await.is_none(), "one error ends it");
                return Ok((have, Some(e)));
            }
        }
    }
    Ok((have, None))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Cut {
    Crash,
    Reset,
    Partition,
    /// Only the way the credit goes, from b to a.
    Credit,
    Pause,
    Wire,
}

fn parts_cut(seed: u64) {
    let two = |_: &Host, cfg: &mut ClusterConfig| cfg.max_inbound_replies = 2;
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], two);
    sees(&mut w, 2);
    let publish = |w: &mut World<Node>| {
        let published = w.block_on("a", |n| async move {
            let actor = n.system.unwrap().spawn(Blob).start().unwrap();
            n.cluster.unwrap().publish::<BlobApi>(&actor, "blob").map(drop)
        });
        published.unwrap().unwrap();
    };
    publish(&mut w);
    for cut in [Cut::Crash, Cut::Reset, Cut::Partition, Cut::Credit, Cut::Pause, Cut::Wire] {
        let incarnation = cluster(&w, "a").identity().incarnation;
        let remote = w.block_on("b", move |n| async move {
            let cluster = n.cluster.unwrap();
            loop {
                match cluster.resolve::<BlobApi>("blob").await {
                    Ok(r) if r.node_incarnation() == incarnation => return r,
                    _ => sleep(Duration::from_millis(20)).await,
                }
            }
        });
        let remote = remote.unwrap();
        if cut == Cut::Wire {
            // Stalls are loss as a session sees it: the credit must not run dry for it.
            let (latency, jitter, stall_every) = (Duration::from_millis(20), Duration::from_millis(10), 64);
            let slow = Wire { latency, jitter, stall_every, syn_stall: Duration::from_millis(200), ..Wire::default() };
            both_ways(&mut w, slow);
        }
        let read = Arc::new(AtomicU64::new(0));
        let reading = read.clone();
        let reader = w.spawn_on("b", move |_| read_parts(remote, 12 << 20, reading));
        w.run_until("b reads its first megabyte", Duration::from_secs(10), |_| read.load(Relaxed) >= 1 << 20);
        w.log(format_args!("cut {cut:?}"));
        match cut {
            Cut::Crash => w.crash("a", Crash::Process),
            Cut::Reset => w.reset("a", "b"),
            Cut::Partition => w.partition(&["a"], &["b"]),
            Cut::Credit => w.cut(&["b"], &["a"]),
            Cut::Pause => w.pause("a", Duration::from_secs(3)),
            Cut::Wire => {}
        }
        let (have, end) = w.join(reader).unwrap().unwrap_or_else(|e| panic!("seed {seed} {cut:?}: {e:?}"));
        let told = match (cut, &end) {
            (Cut::Crash | Cut::Reset, Some(CallError::OutcomeUnknown)) => true,
            (Cut::Partition | Cut::Credit | Cut::Pause, Some(CallError::Timeout | CallError::OutcomeUnknown)) => true,
            (Cut::Wire, None) => have == 12 << 20,
            _ => false,
        };
        assert!(told, "seed {seed} {cut:?}: {have} bytes, then {end:?}");
        assert!(have < 12 << 20 || cut == Cut::Wire, "seed {seed} {cut:?}: the cut came after the end");
        w.heal();
        both_ways(&mut w, Wire::default());
        if cut == Cut::Crash {
            w.restart("a");
            publish(&mut w);
        }
        w.run_until("b's call slots come back", Duration::from_secs(10), |w| {
            let engine = cluster(w, "b").stats().engine;
            engine.pending == 0 && engine.peers.iter().all(|p| p.pending == 0)
        });
        // a's two reply slots come back: two replies in parts at once, asked again while a system call holds one.
        let incarnation = cluster(&w, "a").identity().incarnation;
        let both = w.block_on("b", move |n| async move {
            let cluster = n.cluster.unwrap();
            let started = Instant::now();
            loop {
                let remote = match cluster.resolve::<BlobApi>("blob").await {
                    Ok(r) if r.node_incarnation() == incarnation => r,
                    _ => {
                        sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                };
                match (remote.request_parts(5 << 20).await, remote.request_parts(5 << 20).await) {
                    (Ok(mut first), Ok(mut second)) => {
                        let mut lens = [0, 0];
                        for (len, parts) in lens.iter_mut().zip([&mut first, &mut second]) {
                            while let Some(part) = parts.next().await {
                                *len += part.map_or(0, |p| p.len());
                            }
                        }
                        return Ok(lens);
                    }
                    other if started.elapsed() > Duration::from_secs(20) => return Err(format!("{other:?}")),
                    _ => sleep(Duration::from_millis(50)).await,
                }
            }
        });
        assert_eq!(both.unwrap(), Ok([5 << 20, 5 << 20]), "seed {seed} after {cut:?}");
    }
    println!("world digest parts_cut seed {seed}: {:016x}", w.digest());
}

/// A reply in parts cut mid-way by a crash, a reset, a partition, a one-way cut of its credit or a pause of its producer
/// past `request_timeout` ends in an error, never in a clean end, and every call and reply slot comes back; over a slow
/// and jittery wire with no fault it completes.
#[test]
fn a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back() {
    check_seeds(seeds(0..64), parts_cut).unwrap();
}

/// Reads the body of an upload, checks each part is filled with its index, and answers with its length; what it read
/// goes to `read` as it goes, and how it ended to `ends`.
#[derive(Clone, Default)]
struct Store {
    read: Arc<AtomicU64>,
    ends: Arc<Mutex<Vec<Read>>>,
}

/// What a body read, and how it ended.
type Read = (usize, Option<CallError>);

struct StoreApi;

impl WireProtocol for StoreApi {
    type Message = ();
    type Reply = u64;
    const ID: u64 = 0x5703;
    const SCHEMA: u16 = 1;

    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("short reply".into()))
    }
}

impl ActorProtocol for StoreApi {
    type Mailbox = Option<Reply<u64>>;

    fn encode_reply(len: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&len.to_be_bytes()))
    }

    fn decode(_: Bytes, reply: Option<Reply<u64>>) -> Result<Self::Mailbox, Rejection> {
        Ok(reply)
    }
}

impl Actor for Store {
    type Message = Option<Reply<u64>>;
    type Error = ();

    fn on_message(&mut self, reply: Self::Message, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let Some(reply) = reply else { return Ok(()) };
        let Some(mut upload) = reply.upload() else { return Ok(()) };
        let (read, ends) = (self.read.clone(), self.ends.clone());
        cx.submit_future(async move {
            let (mut have, mut end) = (0, None);
            while let Some(part) = upload.next().await {
                match part {
                    Ok(part) => {
                        let index = (have / PART as usize) as u8;
                        assert!(part.iter().all(|b| *b == index), "part {index} in order");
                        have += part.len();
                        read.store(have as u64, Relaxed);
                    }
                    Err(e) => {
                        assert!(upload.next().await.is_none(), "one error ends it");
                        end = Some(e);
                    }
                }
            }
            ends.lock().unwrap().push((have, end));
            drop(reply.send(have as u64));
            None
        })
        .map(drop)
        .map_err(drop)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum UpCut {
    /// The handler's node crashes.
    Crash,
    /// The caller's node crashes.
    Caller,
    Reset,
    Partition,
    /// Only the way the credit goes, from a to b.
    Credit,
    Pause,
    Wire,
}

/// b uploads 12 MiB to a's store, and each cut comes once a's handler has read a megabyte.
fn uploads_cut(seed: u64) {
    let two = |_: &Host, cfg: &mut ClusterConfig| cfg.max_inbound_replies = 2;
    let mut w = world(seed, &Find::Memory(MemoryDiscovery::new()), &["a", "b"], &[], two);
    sees(&mut w, 2);
    let store = Store::default();
    let publish = |w: &mut World<Node>| {
        let store = store.clone();
        let published = w.block_on("a", |n| async move {
            let actor = n.system.unwrap().spawn(store).start().unwrap();
            n.cluster.unwrap().publish::<StoreApi>(&actor, "store").map(drop)
        });
        published.unwrap().unwrap();
    };
    publish(&mut w);
    let found = |w: &mut World<Node>| {
        let incarnation = cluster(w, "a").identity().incarnation;
        let remote = w.block_on("b", move |n| async move {
            let cluster = n.cluster.unwrap();
            loop {
                match cluster.resolve::<StoreApi>("store").await {
                    Ok(r) if r.node_incarnation() == incarnation => return r,
                    _ => sleep(Duration::from_millis(20)).await,
                }
            }
        });
        remote.unwrap()
    };
    for cut in [UpCut::Crash, UpCut::Caller, UpCut::Reset, UpCut::Partition, UpCut::Credit, UpCut::Pause, UpCut::Wire] {
        let remote = found(&mut w);
        if cut == UpCut::Wire {
            // Stalls are loss as a session sees it: the credit must not run dry for it.
            let (latency, jitter, stall_every) = (Duration::from_millis(20), Duration::from_millis(10), 64);
            let slow = Wire { latency, jitter, stall_every, syn_stall: Duration::from_millis(200), ..Wire::default() };
            both_ways(&mut w, slow);
        }
        store.read.store(0, Relaxed);
        store.ends.lock().unwrap().clear();
        let caller = w.spawn_on("b", move |_| async move { remote.upload((), Parted { len: 12 << 20, at: 0 }).await });
        w.run_until("a reads its first megabyte", Duration::from_secs(10), |_| store.read.load(Relaxed) >= 1 << 20);
        w.log(format_args!("upload cut {cut:?}"));
        match cut {
            UpCut::Crash => w.crash("a", Crash::Process),
            UpCut::Caller => w.crash("b", Crash::Process),
            UpCut::Reset => w.reset("a", "b"),
            UpCut::Partition => w.partition(&["a"], &["b"]),
            UpCut::Credit => w.cut(&["a"], &["b"]),
            UpCut::Pause => w.pause("a", Duration::from_secs(3)),
            UpCut::Wire => {}
        }
        let answer = w.join(caller);
        let told = match (cut, &answer) {
            (UpCut::Caller, Err(_)) => true,
            (UpCut::Crash | UpCut::Reset, Ok(Err(CallError::OutcomeUnknown))) => true,
            (
                UpCut::Partition | UpCut::Credit | UpCut::Pause,
                Ok(Err(CallError::Timeout | CallError::OutcomeUnknown)),
            ) => true,
            (UpCut::Wire, Ok(Ok(len))) => *len == 12 << 20,
            _ => false,
        };
        assert!(
            told,
            "seed {seed} {cut:?}: the caller was told {answer:?} at {:?}, a read {} {:?}",
            w.now(),
            store.read.load(Relaxed),
            store.ends.lock().unwrap()
        );
        if cut != UpCut::Crash {
            w.run_until("a's handler ends", Duration::from_secs(10), |_| !store.ends.lock().unwrap().is_empty());
            let (have, end) = store.ends.lock().unwrap()[0].clone();
            let ended = match (cut, &end) {
                (UpCut::Caller | UpCut::Reset, Some(CallError::OutcomeUnknown)) => true,
                (
                    UpCut::Partition | UpCut::Credit | UpCut::Pause,
                    Some(CallError::Timeout | CallError::NoReply | CallError::OutcomeUnknown),
                ) => true,
                (UpCut::Wire, None) => have == 12 << 20,
                _ => false,
            };
            assert!(ended, "seed {seed} {cut:?}: the handler read {have} bytes, then {end:?}");
            assert!(have < 12 << 20 || cut == UpCut::Wire, "seed {seed} {cut:?}: the cut came after the end");
        }
        w.heal();
        both_ways(&mut w, Wire::default());
        match cut {
            UpCut::Crash => {
                w.restart("a");
                publish(&mut w);
            }
            UpCut::Caller => {
                w.restart("b");
                w.run_until("b boots", Duration::from_secs(30), |w| w.nodes().any(|(n, _)| n == "b"));
                sees(&mut w, 2);
            }
            _ => {}
        }
        w.run_until("b's call slots come back", Duration::from_secs(30), |w| {
            let engine = cluster(w, "b").stats().engine;
            engine.pending == 0 && engine.peers.iter().all(|p| p.pending == 0)
        });
        // a's two reply slots come back: two uploads at once, sent again while a system call holds one.
        let remote = found(&mut w);
        let both = w.block_on("b", move |_| async move {
            let started = Instant::now();
            loop {
                let first = remote.upload((), Parted { len: 5 << 20, at: 0 });
                let second = remote.upload((), Parted { len: 5 << 20, at: 0 });
                match tokio::join!(first, second) {
                    (Ok(first), Ok(second)) => return Ok([first, second]),
                    other if started.elapsed() > Duration::from_secs(20) => return Err(format!("{other:?}")),
                    _ => sleep(Duration::from_millis(50)).await,
                }
            }
        });
        assert_eq!(both.unwrap(), Ok([5 << 20, 5 << 20]), "seed {seed} after {cut:?}");
    }
    println!("world digest uploads_cut seed {seed}: {:016x}", w.digest());
}

/// An upload cut mid-way by a crash of either node, a reset, a partition, a one-way cut of its credit or a pause of its
/// handler past `request_timeout` ends in an error on both sides, never in a clean end, and every call and reply slot
/// comes back; over a slow and jittery wire with no fault it completes.
#[test]
fn an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back() {
    check_seeds(seeds(0..64), uploads_cut).unwrap();
}
