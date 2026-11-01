//! Quotas in the World (spec/quotas.md §7): nodes in three regions at WAN latency, each running a client that takes a
//! hot key far past its limit and holds its permits, and acquires a permit of a second limit with room on the same key
//! and drops it at once, through every fault the World injects. Each test asserts that a node that is up and not
//! itself faulted never stops admitting either limit, that the cluster holds the limit while nothing is faulted, and
//! that once the clients stop every node holds no permit and every table and every replica's rows empty: a row holding
//! permits or units is never dropped for a node that is up, so empty rows are usage back to zero.
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{
    Cluster, ClusterConfig, ClusterError, Denied, DiscoveryError, Permits, QuotaConfig, Quotas, Rate, SeedConfig,
    SeedDiscovery, SeedServer, SeedServerConfig,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, Task, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};

const SECRET: &str = "at-least-16-bytes-long";
const SEED_PORT: u16 = 7001;
const BLUE: [&str; 6] = ["a1", "a2", "b1", "b2", "c1", "c2"];
const GREEN: [&str; 6] = ["a3", "a4", "b3", "b4", "c3", "c4"];
/// Three quota nodes in one region, so each replicates every key: the gateways of a small cluster.
const GATEWAYS: [&str; 3] = ["g1", "g2", "g3"];
const MAX: u64 = 60;
const REQ: Rate = Rate::new("req/s", MAX, Duration::from_secs(1));
const WS: Permits = Permits::new("ws", 12);
/// Acquired and dropped at once: the key always has room.
const ONE: Permits = Permits::new("one", 12);
const ACCOUNT: Rate = Rate::new("account/s", 1000, Duration::from_secs(1));
/// The longest a node that is up and not faulted may go without admitting the hot key: its window is 1 s.
const GAP: Duration = Duration::from_millis(2000);
/// A faulted node is excused this long after its fault ends.
const SETTLE: Duration = Duration::from_secs(3);
const SLICE: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct Node {
    system: ActorSystem,
    quotas: Quotas,
    cluster: Cluster,
    _seed: Arc<SeedServer>,
}

fn region(name: &str) -> String {
    name[..1].to_owned()
}

/// Every node is also a seed, so discovery partitions and crashes with the nodes. Blue nodes list the blue seeds, green
/// ones every seed, so that the green nodes find each other once the blue are gone; the gateways list each other.
async fn boot(host: Host, capacity: usize) -> Node {
    let transport = || host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let seed =
        SeedServer::bind(SeedServerConfig::new(SocketAddr::new(host.ip(), SEED_PORT), &transport())).await.unwrap();
    let green = GREEN.contains(&host.name());
    let listed: Vec<&str> = match GATEWAYS.contains(&host.name()) {
        true => GATEWAYS.to_vec(),
        false => BLUE.iter().chain(GREEN.iter().filter(|_| green)).copied().collect(),
    };
    let seeds: Vec<String> = listed.iter().map(|n| SocketAddr::new(host.ip_of(n), SEED_PORT).to_string()).collect();
    let cluster = loop {
        // Each try is a new incarnation, from the wall clock.
        let transport = transport();
        let discovery = SeedDiscovery::start(SeedConfig::new(seeds.clone(), &transport)).await.unwrap();
        let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
        // With seeds, as guide/cluster.md § Seed servers asks.
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
        match Cluster::start(&system, cfg).await {
            Ok(cluster) => break cluster,
            // A wall clock stepped back makes an older incarnation than the seeds last saw: they refuse it until the
            // wall passes that one.
            Err(ClusterError::Discovery(DiscoveryError::Stale)) => tokio::time::sleep(Duration::from_secs(5)).await,
            Err(error) => panic!("{} does not start: {error}", host.name()),
        }
    };
    let quotas = QuotaConfig { region: region(host.name()), capacity, ..QuotaConfig::default() };
    let quotas = Quotas::start(&cluster, quotas).await.unwrap();
    Node { system, quotas, cluster, _seed: Arc::new(seed) }
}

/// The clients, and what the test watches of them.
struct Load {
    /// Units admitted per node: the hot rate by `take`, and `ONE` by `acquire`.
    admitted: Arc<Mutex<BTreeMap<(String, &'static str), u64>>>,
    stop: Arc<AtomicBool>,
    /// Other keys each client cycles through besides the hot one.
    keys: usize,
    tasks: Vec<Task<()>>,
    /// Per node and call: its count when it last moved, and when.
    moved: BTreeMap<(String, &'static str), (u64, Duration)>,
    /// Per node: excused until.
    excused: BTreeMap<String, Duration>,
}

impl Load {
    fn new(keys: usize) -> Self {
        let (admitted, stop) = (Arc::default(), Arc::default());
        Self { admitted, stop, keys, tasks: Vec::new(), moved: BTreeMap::new(), excused: BTreeMap::new() }
    }

    /// A client on `name`: every 5 ms, one unit of the hot key's rate, one of its permits held 300 ms, and one of
    /// another permit limit dropped at once.
    fn start(&mut self, w: &mut World<Node>, name: &str) {
        let (admitted, stop, keys, who) = (self.admitted.clone(), self.stop.clone(), self.keys, name.to_owned());
        self.tasks.push(w.spawn_on(name, move |n| async move {
            let mut i = 0;
            while !stop.load(Relaxed) {
                if n.quotas.take("hot", &[(REQ, 1)]).is_ok() {
                    *admitted.lock().unwrap().entry((who.clone(), "take")).or_default() += 1;
                }
                if n.quotas.acquire("hot", &ONE).is_ok() {
                    *admitted.lock().unwrap().entry((who.clone(), "one")).or_default() += 1;
                }
                if let Ok(permit) = n.quotas.acquire("hot", &WS) {
                    tokio::task::spawn_local(async move {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        drop(permit);
                    });
                }
                if keys > 0 {
                    i = (i + 1) % keys;
                    let _ = n.quotas.take(format!("k{i}"), &[(REQ, 1)]);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }

    fn excuse(&mut self, w: &World<Node>, name: &str, d: Duration) {
        self.excused.insert(name.to_owned(), w.now() + d + SETTLE);
    }

    /// Runs for `d`, and fails if a node that is up and not excused went `GAP` without admitting the hot rate by `take`
    /// or `ONE` by `acquire`.
    fn run(&mut self, w: &mut World<Node>, d: Duration, seed: u64) {
        let end = w.now() + d;
        while w.now() < end {
            w.run_for(SLICE);
            let (now, admitted) = (w.now(), self.admitted.lock().unwrap().clone());
            let up: Vec<String> = w.nodes().map(|(name, _)| name.to_owned()).collect();
            self.moved.retain(|(name, _), _| up.contains(name));
            for watched in up.into_iter().flat_map(|name| [(name.clone(), "take"), (name, "one")]) {
                let count = admitted.get(&watched).copied().unwrap_or(0);
                let excused = self.excused.get(&watched.0).is_some_and(|until| now < *until);
                let (last, since) = self.moved.entry(watched.clone()).or_insert((count, now));
                if count != *last || excused {
                    (*last, *since) = (count, now);
                }
                let (name, call) = watched;
                assert!(
                    now - *since <= GAP,
                    "seed {seed}: {name} admitted nothing on `{call}` for {:?}\n{}",
                    now - *since,
                    w.trace_tail(40)
                );
            }
        }
    }

    /// Nothing faulted for 5 s: the cluster admits the hot key within its limit, and most of it.
    fn steady(&mut self, w: &mut World<Node>, seed: u64) {
        let total = |load: &Self| {
            load.admitted.lock().unwrap().iter().filter(|((_, call), _)| *call == "take").map(|(_, n)| n).sum::<u64>()
        };
        let before = total(self);
        self.run(w, Duration::from_secs(5), seed);
        let per_sec = (total(self) - before) as f64 / 5.0;
        let max = MAX as f64;
        // The ring reads up to a bucket high (8/9 of the limit), and grants move a report cycle behind demand.
        assert!((0.75 * max..=1.1 * max).contains(&per_sec), "seed {seed}: {per_sec} a second of {max}");
    }

    /// Stops the clients, and waits for every table and every replica's rows to empty.
    fn drain(self, w: &mut World<Node>, test: &str, seed: u64) {
        self.stop.store(true, Relaxed);
        for task in self.tasks {
            let _ = w.join(task);
        }
        // Past the 300 ms a client holds a permit.
        w.run_for(Duration::from_secs(1));
        for (name, node) in w.nodes() {
            assert_eq!(node.quotas.stats().permits, 0, "seed {seed}: {name} still holds permits");
        }
        // A row goes a little over 2 min after the node last reported its key.
        let end = w.now() + Duration::from_secs(240);
        loop {
            let left: Vec<String> = w
                .nodes()
                .map(|(name, n)| (name, n.quotas.stats()))
                .filter(|(_, s)| s.entries > 0 || s.rows > 0)
                .map(|(name, s)| format!("{name}: {s:?}"))
                .collect();
            if left.is_empty() {
                break;
            }
            assert!(w.now() < end, "seed {seed}: not empty at {:?}: {left:?}", w.now());
            w.run_for(Duration::from_secs(1));
        }
        println!("world digest {test} seed {seed}: {:016x}", w.digest());
    }
}

/// `nodes` in three regions, 40 ms apart, each with a client once every one lists them all.
fn world(seed: u64, nodes: &[&str], capacity: usize, keys: usize) -> (World<Node>, Load) {
    let mut w = World::new(WorldConfig { seed, limit: Duration::from_secs(300), ..WorldConfig::default() });
    for name in nodes {
        w.add(name, move |host| boot(host, capacity));
    }
    wan(&mut w, nodes);
    ready(&mut w, nodes.len());
    let mut load = Load::new(keys);
    for name in nodes {
        load.start(&mut w, name);
    }
    load.run(&mut w, Duration::from_secs(3), seed);
    (w, load)
}

fn wan(w: &mut World<Node>, nodes: &[&str]) {
    let wan = Wire { latency: Duration::from_millis(40), jitter: Duration::from_millis(5), ..Wire::default() };
    for a in nodes {
        for b in nodes.iter().filter(|b| region(b) != region(a)) {
            w.wire(a, b, wan);
        }
    }
}

/// `n` nodes are up and no replica still pulls: what `/readyz` waits for.
fn ready(w: &mut World<Node>, n: usize) {
    w.run_until("the nodes ready", Duration::from_secs(30), |w| {
        w.nodes().count() == n && w.nodes().all(|(_, node)| !node.cluster.quotas_starting())
    });
}

fn restart(w: &mut World<Node>, load: &mut Load, name: &str, seed: u64) {
    w.restart(name);
    load.start(w, name);
    load.excuse(w, name, Duration::ZERO);
    load.run(w, Duration::from_secs(1), seed);
}

/// Leaves the cluster, then its process ends.
fn shut_down(w: &mut World<Node>, name: &str) {
    w.block_on(name, |n| async move { n.system.shutdown().await.map(drop) }).unwrap().unwrap();
    w.crash(name, Crash::Process);
}

#[test]
fn crashes_of_either_kind_and_a_rolling_restart_at_wan_latency_never_pause_admission() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, mut load) = world(seed, &BLUE, 1 << 16, 0);
        load.steady(&mut w, seed);
        for (name, how) in [("a1", Crash::Process), ("b2", Crash::Machine)] {
            let down = Duration::from_millis(w.pick(1000..8000));
            load.excuse(&w, name, down);
            w.crash(name, how);
            load.run(&mut w, down, seed);
            restart(&mut w, &mut load, name, seed);
            load.run(&mut w, Duration::from_secs(4), seed);
        }
        // Each node leaves, restarts and pulls before the next goes, as `/readyz` paces a deploy.
        for name in BLUE {
            load.excuse(&w, name, Duration::from_secs(60));
            shut_down(&mut w, name);
            restart(&mut w, &mut load, name, seed);
            ready(&mut w, BLUE.len());
            load.excuse(&w, name, Duration::ZERO);
            load.run(&mut w, Duration::from_secs(1), seed);
        }
        load.steady(&mut w, seed);
        load.drain(&mut w, "quota_crashes_and_rolling_restart", seed);
    })
    .unwrap();
}

#[test]
fn a_blue_green_switch_under_load_never_pauses_admission() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, mut load) = world(seed, &BLUE, 1 << 16, 0);
        load.steady(&mut w, seed);
        for name in GREEN {
            w.add(name, move |host| boot(host, 1 << 16));
        }
        let all: Vec<&str> = BLUE.iter().chain(&GREEN).copied().collect();
        wan(&mut w, &all);
        ready(&mut w, all.len());
        for name in GREEN {
            load.start(&mut w, name);
        }
        load.run(&mut w, Duration::from_secs(3), seed);
        for name in BLUE {
            load.excuse(&w, name, Duration::from_secs(60));
        }
        let leaving: Vec<_> =
            BLUE.iter().map(|name| w.spawn_on(name, |n| async move { n.system.shutdown().await.map(drop) })).collect();
        for task in leaving {
            w.join(task).unwrap().unwrap();
        }
        for name in BLUE {
            w.crash(name, Crash::Process);
        }
        load.run(&mut w, Duration::from_secs(8), seed);
        load.steady(&mut w, seed);
        load.drain(&mut w, "quota_blue_green", seed);
    })
    .unwrap();
}

#[test]
fn pauses_starvation_drift_and_a_wall_step_never_pause_admission_elsewhere() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, mut load) = world(seed, &BLUE, 1 << 16, 0);
        w.drift("a2", 5000);
        w.drift("c1", -5000);
        load.steady(&mut w, seed);
        // Past the replicas' lease, and within it; a replica paused past 2.5 s ignores its own grants on resume.
        for (name, secs) in [("b1", 3), ("c2", 20)] {
            load.excuse(&w, name, Duration::from_secs(secs));
            w.pause(name, Duration::from_secs(secs));
            load.run(&mut w, Duration::from_secs(secs + 4), seed);
        }
        let one_in = w.pick(500..3000) as u32;
        load.excuse(&w, "a1", Duration::from_secs(10));
        w.starve("a1", one_in);
        load.run(&mut w, Duration::from_secs(10), seed);
        w.starve("a1", 1);
        load.run(&mut w, Duration::from_secs(4), seed);
        // A wall clock stepped back restarts as an older incarnation, which the seeds refuse until it passes.
        w.step_wall("b2", -60_000);
        load.excuse(&w, "b2", Duration::from_secs(90));
        w.crash("b2", Crash::Process);
        restart(&mut w, &mut load, "b2", seed);
        load.run(&mut w, Duration::from_secs(6), seed);
        load.steady(&mut w, seed);
        load.drain(&mut w, "quota_pauses_and_clocks", seed);
    })
    .unwrap();
}

#[test]
fn partitions_cuts_resets_and_refused_accepts_never_pause_admission() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, mut load) = world(seed, &BLUE, 1 << 16, 0);
        load.steady(&mut w, seed);
        // Into its three regions, each of which admits up to the limit meanwhile, and back.
        w.partition(&["a1", "a2"], &["b1", "b2", "c1", "c2"]);
        w.partition(&["b1", "b2"], &["c1", "c2"]);
        let apart = Duration::from_millis(w.pick(3000..10000));
        load.run(&mut w, apart, seed);
        w.heal();
        load.run(&mut w, Duration::from_secs(6), seed);
        // Region a's candidates cut from the other regions in turn: one of them is the hot key's replica there.
        for name in ["a1", "a2"] {
            w.partition(&[name], &["b1", "b2", "c1", "c2"]);
            load.run(&mut w, Duration::from_secs(6), seed);
            w.heal();
            load.run(&mut w, Duration::from_secs(4), seed);
        }
        // b1 hears everyone and nobody hears it.
        w.cut(&["b1"], &["a1", "a2", "b2", "c1", "c2"]);
        load.run(&mut w, Duration::from_secs(6), seed);
        w.heal();
        load.run(&mut w, Duration::from_secs(4), seed);
        for other in ["a1", "b1", "c2"] {
            w.reset("c1", other);
        }
        load.run(&mut w, Duration::from_secs(2), seed);
        // Its sessions reset, then nobody can dial it until it accepts again.
        w.refuse_accepts("c2", true);
        for other in ["a1", "a2", "b1", "b2", "c1"] {
            w.reset("c2", other);
        }
        load.run(&mut w, Duration::from_secs(6), seed);
        w.refuse_accepts("c2", false);
        load.run(&mut w, Duration::from_secs(6), seed);
        load.steady(&mut w, seed);
        load.drain(&mut w, "quota_partitions_and_cuts", seed);
    })
    .unwrap();
}

#[test]
fn eviction_with_more_keys_than_fit_never_pauses_the_hot_key() {
    check_seeds(seeds(0..64), |seed| {
        // Four entries a shard, and 600 keys taken about once every 3 s on each node.
        let (mut w, mut load) = world(seed, &BLUE, 256, 600);
        load.steady(&mut w, seed);
        load.run(&mut w, Duration::from_secs(10), seed);
        load.steady(&mut w, seed);
        load.drain(&mut w, "quota_eviction", seed);
    })
    .unwrap();
}

/// When each request was made, and the refusal if any.
type Got = Vec<(Duration, Option<Denied>)>;

/// `n` requests of `key` on `name` every `every` for `d`.
fn paced(w: &mut World<Node>, name: &str, key: &'static str, n: u32, every: Duration, d: Duration) -> Task<Got> {
    w.spawn_on(name, move |node| async move {
        let start = tokio::time::Instant::now();
        let mut got = Vec::new();
        while start.elapsed() < d {
            for _ in 0..n {
                got.push((start.elapsed(), node.quotas.take(key, &[(ACCOUNT, 1)]).err()));
            }
            tokio::time::sleep(every).await;
        }
        got
    })
}

/// Of the refusals for a share: how many, the longest from one to the next admission, and how many said to retry sooner
/// than the share came: the first request `every` past their `retry_after` is refused for a share again. Refused at the
/// limit instead, it was not the share: the node learns the usage of the others from its replicas' answers.
fn shares(got: &Got, every: Duration) -> (usize, Duration, usize) {
    let (mut n, mut longest, mut early) = (0, Duration::ZERO, 0);
    let mut since = None;
    for (i, (at, denied)) in got.iter().enumerate() {
        let Some(Denied::Share { retry_after, .. }) = denied else {
            longest = longest.max(since.take().map_or(Duration::ZERO, |s| *at - s));
            continue;
        };
        n += 1;
        since.get_or_insert(*at);
        let then = got[i..].iter().find(|(t, _)| *t >= *at + *retry_after + every);
        early += then.is_some_and(|(_, d)| matches!(d, Some(Denied::Share { .. }))) as usize;
    }
    let end = got.last().map_or(Duration::ZERO, |l| l.0);
    (n, since.map_or(longest, |s| longest.max(end - s)), early)
}

/// Three quota nodes under a limit of 1000 a second, as an edge platform's per-account limit. A key taken 50 times
/// a second on a node new to it is admitted from its first request, whether new to the cluster or used last by another
/// node that still holds the room. Where the use moves from a node that took all of the limit, the node it moves to is
/// refused for a share only until the other hands back the room it no longer uses, and the refusal says when to ask
/// again.
#[test]
fn a_key_new_to_a_node_or_moving_between_nodes_is_admitted_where_the_limit_has_room() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = World::new(WorldConfig { seed, limit: Duration::from_secs(300), ..WorldConfig::default() });
        for name in GATEWAYS {
            w.add(name, move |host| boot(host, 1 << 16));
        }
        ready(&mut w, GATEWAYS.len());
        let (ms, second) = (Duration::from_millis(1), Duration::from_secs(1));
        let slow = 20 * ms;
        let task = paced(&mut w, "g1", "fresh", 1, slow, second);
        let fresh = shares(&w.join(task).unwrap(), slow);
        let task = paced(&mut w, "g2", "moved", 1, slow, 2 * second);
        w.join(task).unwrap();
        let task = paced(&mut w, "g1", "moved", 1, slow, second);
        let moved = shares(&w.join(task).unwrap(), slow);
        // g2 used it, then g1 at twice the limit, then g2 again: each time the other holds the room.
        let task = paced(&mut w, "g2", "hot", 1, slow, second);
        w.join(task).unwrap();
        let task = paced(&mut w, "g1", "hot", 2, ms, 2 * second);
        let took = shares(&w.join(task).unwrap(), ms);
        let task = paced(&mut w, "g2", "hot", 1, slow, 2 * second);
        let back = shares(&w.join(task).unwrap(), slow);
        println!("seed {seed}: new {fresh:?}, moved {moved:?}, took {took:?}, back {back:?}");
        assert_eq!(fresh.0, 0, "seed {seed}: a key new to the cluster");
        assert_eq!(moved.0, 0, "seed {seed}: a key new to this node that another used last");
        for (what, (_, longest, early)) in [("took", took), ("back", back)] {
            assert!(longest <= 250 * ms, "seed {seed}: {what}: a share of 0 for {longest:?}");
            assert_eq!(early, 0, "seed {seed}: {what}: retry_after said sooner than the share came");
        }
    })
    .unwrap();
}
