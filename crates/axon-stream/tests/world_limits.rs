//! Namespace limits across the cluster in the World: three origins at WAN latency publish ten times a namespace's
//! records rate and another's bytes rate, and hold its open streams at the limit. The cluster admits each rate within
//! the bounds of spec/quotas.md §5 while nothing is faulted, and at most twice it under the faults, never pausing an
//! origin that is up; a namespace without limits admits everything; an open stream past the limit is refused, an
//! origin that crashes gives its stream's permit back once the replicas stop hearing from it, and one that restarts
//! opens again only as streams close.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, QuotaConfig, Quotas};
use axon_net::SharedSecret;
use axon_stream::{
    DeliveryPolicy, NamespaceLimit, NamespaceLimits, StreamError, StreamKey, StreamNode, StreamNodeConfig,
};
use axon_testkit::world::{Crash, Host, Task, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

const NODES: [&str; 3] = ["a", "b", "c"];
const RECORDS: u64 = 60;
const WIDE: usize = 80;
const BYTES: u64 = RECORDS * WIDE as u64;
const OPEN: u64 = 4;
/// What a node that has no grant for the namespace yet may open on its own (spec/quotas.md §2.5).
const FALLBACK: u64 = OPEN.div_ceil(NODES.len() as u64);
/// Each node publishes one record of this size into its own stream of the namespace, which refuses past this limit.
const LOAD: [(&str, usize, Option<NamespaceLimit>); 3] =
    [("md", 8, Some(NamespaceLimit::Records)), ("wide", WIDE, Some(NamespaceLimit::Bytes)), ("free", 8, None)];
/// The streams of `md` each node may try to open: its own name, then with a 2.
const MD: [&str; 6] = ["a", "a2", "b", "b2", "c", "c2"];
/// The longest an origin that is up and not excused may go without admitting a record of `md`.
const GAP: Duration = Duration::from_secs(2);
/// After a fault ends, before its node must admit again.
const SETTLE: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Node {
    system: ActorSystem,
    cluster: Cluster,
    quotas: Quotas,
    stream: StreamNode,
}

async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let transport = host.transport(SharedSecret::new("at-least-16-bytes-long").unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, ClusterConfig::new(host.addr(), transport, discovery)).await.unwrap();
    let quotas = Quotas::start(&cluster, QuotaConfig::default()).await.unwrap();
    let md = NamespaceLimits {
        records_per_sec: Some(RECORDS),
        // Past what `md` publishes: both rates are taken, and only the records rate refuses.
        bytes_per_sec: Some(1 << 20),
        open_streams: Some(OPEN),
        ..NamespaceLimits::default()
    };
    let wide = NamespaceLimits { bytes_per_sec: Some(BYTES), ..NamespaceLimits::default() };
    let cfg = StreamNodeConfig::new(vec![]).limit("md", md).limit("wide", wide);
    let stream = StreamNode::start(&cluster, StreamNodeConfig { quotas: Some(quotas.clone()), ..cfg }).await.unwrap();
    Node { system, cluster, quotas, stream }
}

fn md(name: &str) -> StreamKey {
    StreamKey::new("md", name)
}

/// The streams of `md` open on the nodes that are up.
fn held(w: &World<Node>) -> u64 {
    w.nodes()
        .map(|(name, n)| MD.iter().filter(|s| s.starts_with(name) && n.stream.cursor(&md(s)).is_some()).count() as u64)
        .sum()
}

/// Opens `md/name` on its node, trying again after each refusal's `retry_after`.
fn open(w: &mut World<Node>, name: &'static str) -> Task<()> {
    w.spawn_on(&name[..1], move |n| async move {
        loop {
            match n.stream.open_stream(&md(name), 1, DeliveryPolicy::ReplayRequired).await {
                Ok(()) => return,
                Err(StreamError::QuotaExceeded {
                    limit: NamespaceLimit::OpenStreams, retry_after: Some(d), ..
                }) => {
                    tokio::time::sleep(d).await;
                }
                Err(error) => panic!("md/{name}: {error:?}"),
            }
        }
    })
}

/// One try, as the refusal it must be.
fn refused(w: &mut World<Node>, name: &'static str, seed: u64) {
    let tried = w.block_on(&name[..1], move |n| async move {
        n.stream.open_stream(&md(name), 1, DeliveryPolicy::ReplayRequired).await
    });
    assert!(
        matches!(&tried, Ok(Err(StreamError::QuotaExceeded { namespace, limit: NamespaceLimit::OpenStreams, retry_after: Some(_) })) if &**namespace == "md"),
        "seed {seed}: md/{name} {tried:?} with {} open\n{}",
        held(w),
        w.trace_tail(40)
    );
}

/// A publisher on each node, into `md`, `wide` and `free`, every 5 ms: ten times what `md` and `wide` admit.
struct Load {
    /// Records admitted per node and namespace.
    admitted: Arc<Mutex<BTreeMap<(String, &'static str), u64>>>,
    /// Refusals of `md` at a bytes rate it uses a thousandth of.
    short: Arc<AtomicU64>,
    /// Anything but the refusal a namespace's limit gives.
    unexpected: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    tasks: Vec<Task<()>>,
    /// The nodes publishing, each with its `md` count when it last moved.
    moved: BTreeMap<String, (u64, Duration)>,
    excused: BTreeMap<String, Duration>,
}

impl Load {
    fn start(w: &mut World<Node>) -> Self {
        let now = w.now();
        let moved = NODES.iter().map(|name| (name.to_string(), (0, now))).collect();
        let mut load = Self {
            admitted: Arc::default(),
            short: Arc::default(),
            unexpected: Arc::default(),
            stop: Arc::default(),
            tasks: Vec::new(),
            moved,
            excused: BTreeMap::new(),
        };
        for name in NODES {
            let (admitted, short, unexpected, stop) =
                (load.admitted.clone(), load.short.clone(), load.unexpected.clone(), load.stop.clone());
            load.tasks.push(w.spawn_on(name, move |n| async move {
                for ns in ["wide", "free"] {
                    n.stream.open_stream(&StreamKey::new(ns, name), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
                }
                while !stop.load(Relaxed) {
                    for (ns, size, limit) in LOAD {
                        let error = match n.stream.publish(&StreamKey::new(ns, name), vec![Bytes::from(vec![0; size])]) {
                            Ok(_) => {
                                *admitted.lock().unwrap().entry((name.to_owned(), ns)).or_default() += 1;
                                continue;
                            }
                            // md/<node> opens after the load starts, and may not yet.
                            Err(e) if ns == "md" && e.error == StreamError::UnknownStream => continue,
                            Err(e) => e.error,
                        };
                        let expected = matches!(&error, StreamError::QuotaExceeded { namespace, limit: l, retry_after: Some(_) } if **namespace == *ns && Some(*l) == limit);
                        // Under a fault a node's share of a rate it barely uses can run short (spec/quotas.md §5);
                        // `steady` checks it does not otherwise.
                        if matches!(&error, StreamError::QuotaExceeded { limit: NamespaceLimit::Bytes, .. } if ns == "md") {
                            short.fetch_add(1, Relaxed);
                        } else if !expected {
                            unexpected.lock().unwrap().push(format!("{name} {ns}: {error:?}"));
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }));
        }
        load
    }

    fn total(&self, ns: &str) -> u64 {
        self.admitted.lock().unwrap().iter().filter(|((_, n), _)| *n == ns).map(|(_, count)| count).sum()
    }

    fn excuse(&mut self, w: &World<Node>, name: &str, d: Duration) {
        self.excused.insert(name.to_owned(), w.now() + d + SETTLE);
    }

    /// Runs for `d`, holding the open streams within what the faults in effect allow, `md` within twice its rate (two
    /// sides of a partition, or a node resuming with what the replicas had not heard of), and failing if a publishing
    /// node that is up and not excused goes `GAP` without admitting a record of it.
    fn run(&mut self, w: &mut World<Node>, d: Duration, most: u64, seed: u64) {
        let (start, before) = (w.now(), self.total("md"));
        // Only the time a run watches counts.
        for (_, since) in self.moved.values_mut() {
            *since = (*since).max(start);
        }
        while w.now() < start + d {
            w.run_for(Duration::from_millis(100));
            assert!(
                held(w) <= most,
                "seed {seed}: {} md streams open, more than {most}\n{}",
                held(w),
                w.trace_tail(40)
            );
            let unexpected = self.unexpected.lock().unwrap();
            assert!(unexpected.is_empty(), "seed {seed}: {unexpected:?}");
            let (now, admitted) = (w.now(), self.admitted.lock().unwrap().clone());
            let up: BTreeSet<&str> = w.nodes().map(|(name, _)| name).collect();
            for (name, (last, since)) in self.moved.iter_mut().filter(|(name, _)| up.contains(name.as_str())) {
                let count = admitted.get(&(name.clone(), "md")).copied().unwrap_or(0);
                if count != *last || self.excused.get(name).is_some_and(|until| now < *until) {
                    (*last, *since) = (count, now);
                }
                assert!(
                    now - *since <= GAP,
                    "seed {seed}: {name} admitted nothing for {:?}\n{}",
                    now - *since,
                    w.trace_tail(40)
                );
            }
        }
        let (taken, secs) = (self.total("md") - before, d.as_secs_f64().ceil() as u64);
        assert!(taken <= 2 * RECORDS * (secs + 1), "seed {seed}: md took {taken} in {d:?}");
    }

    /// Nothing faulted for 5 s: each rate is held within its limit, and most of it is admitted.
    fn steady(&mut self, w: &mut World<Node>, seed: u64) {
        let (before, short) = (["md", "wide", "free"].map(|ns| self.total(ns)), self.short.load(Relaxed));
        let publishers = self.moved.len() as u64;
        self.run(w, Duration::from_secs(5), OPEN, seed);
        let [md, wide, free] = ["md", "wide", "free"].map(|ns| self.total(ns));
        assert_eq!(self.short.load(Relaxed), short, "seed {seed}: md refused at a bytes rate it uses a thousandth of");
        let per_sec = |after: u64, before: u64, each: u64| ((after - before) * each) as f64 / 5.0;
        // The ring reads up to a bucket high (8/9 of the limit), and grants move a report cycle behind demand.
        for (ns, rate, max) in
            [("md", per_sec(md, before[0], 1), RECORDS), ("wide", per_sec(wide, before[1], WIDE as u64), BYTES)]
        {
            let max = max as f64;
            assert!((0.75 * max..=1.1 * max).contains(&rate), "seed {seed}: {ns} took {rate} a second of {max}");
        }
        // Every 5 ms, and the timer's tick past it.
        let offered = publishers * 5 * 150;
        assert!(free - before[2] >= offered, "seed {seed}: free admitted {} of about {offered}", free - before[2]);
    }

    /// Stops the publishers, checks each refusal was counted, closes every stream, and checks no permit is left.
    fn drain(self, mut w: World<Node>, test: &str, seed: u64) {
        self.stop.store(true, Relaxed);
        for task in self.tasks {
            let _ = w.join(task);
        }
        for (name, n) in w.nodes() {
            let stats = n.stream.stats().hub;
            let refused = stats.quota_refused.iter().sum::<u64>();
            assert!(stats.publish_rejected >= refused - stats.quota_refused[NamespaceLimit::OpenStreams as usize]);
            if self.moved.contains_key(name) {
                assert!(stats.quota_refused[NamespaceLimit::Records as usize] > 0, "seed {seed}: {name} {stats:?}");
                assert!(stats.quota_refused[NamespaceLimit::Bytes as usize] > 0, "seed {seed}: {name} {stats:?}");
            }
        }
        for name in NODES {
            w.block_on(name, move |n| async move {
                for key in MD.iter().map(|s| md(s)).chain(["wide", "free"].map(|ns| StreamKey::new(ns, name))) {
                    let _ = n.stream.close_stream(&key);
                }
                assert_eq!(n.quotas.stats().permits, 0, "seed {seed}: {name} still holds permits");
                n.system.shutdown().await.unwrap();
            })
            .unwrap();
        }
        println!("world digest {test} seed {seed}: {:016x}", w.digest());
    }
}

fn ready(w: &mut World<Node>) {
    w.run_until("three members, their quota replicas pulled", Duration::from_secs(30), |w| {
        w.nodes().count() == 3
            && w.nodes().all(|(_, n)| n.cluster.view().members.len() == 3 && !n.cluster.quotas_starting())
    });
}

/// Three origins 20 ms apart, publishing, with four streams of `md` open: a and a2, b, c.
fn world(seed: u64) -> (World<Node>, Load) {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, limit: Duration::from_secs(300), ..WorldConfig::default() });
    for name in NODES {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    let wan = Wire { latency: Duration::from_millis(20), jitter: Duration::from_millis(5), ..Wire::default() };
    for a in NODES {
        for b in NODES.iter().filter(|b| **b != a) {
            w.wire(a, b, wan);
        }
    }
    ready(&mut w);
    let mut load = Load::start(&mut w);
    for name in ["a", "a2", "b", "c"] {
        let task = open(&mut w, name);
        w.join(task).unwrap();
    }
    load.run(&mut w, Duration::from_secs(3), OPEN, seed);
    (w, load)
}

fn a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins(seed: u64) {
    let (mut w, mut load) = world(seed);
    load.steady(&mut w, seed);
    refused(&mut w, "c2", seed);

    // b's permit is counted until the view shows it left, or the replicas have not heard from it for 15-16 s, and a
    // refusal's retry_after later c takes it.
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.crash("b", how);
    load.moved.remove("b");
    let c2 = open(&mut w, "c2");
    w.run_until("md/c2 open", Duration::from_secs(18), |w| {
        w.nodes().any(|(name, n)| name == "c" && n.stream.cursor(&md("c2")).is_some())
    });
    w.join(c2).unwrap();
    load.steady(&mut w, seed);

    // A restarted b may open one stream on the fallback before its first grant; after that, only as others close.
    w.restart("b");
    ready(&mut w);
    let first =
        w.block_on("b", |n| async move { n.stream.open_stream(&md("b"), 1, DeliveryPolicy::ReplayRequired).await });
    let most = OPEN + FALLBACK;
    load.run(&mut w, Duration::from_secs(2), most, seed);
    refused(&mut w, "b2", seed);
    for name in ["c2", "a2"] {
        w.block_on(&name[..1], move |n| async move { n.stream.close_stream(&md(name)) }).unwrap().unwrap();
    }
    let b2 = open(&mut w, "b2");
    w.join(b2).unwrap();
    load.run(&mut w, Duration::from_secs(2), most, seed);
    assert!(held(&w) <= OPEN, "seed {seed}: {} open once b's first ({first:?}) was granted", held(&w));
    load.drain(w, "stream_limits_crash", seed);
}

fn faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds(seed: u64) {
    let (mut w, mut load) = world(seed);
    w.drift("a", 5000);
    w.drift("c", -5000);
    load.steady(&mut w, seed);
    // Each side admits up to the limit meanwhile.
    w.partition(&["a"], &["b", "c"]);
    let apart = Duration::from_millis(w.pick(3000..10000));
    load.run(&mut w, apart, OPEN, seed);
    w.heal();
    load.run(&mut w, Duration::from_secs(6), OPEN, seed);
    // b hears everyone and nobody hears it.
    w.cut(&["b"], &["a", "c"]);
    load.run(&mut w, Duration::from_secs(6), OPEN, seed);
    w.heal();
    load.run(&mut w, Duration::from_secs(4), OPEN, seed);
    // Its sessions reset, then nobody can dial it until it accepts again.
    w.refuse_accepts("c", true);
    for other in ["a", "b"] {
        w.reset("c", other);
    }
    load.run(&mut w, Duration::from_secs(6), OPEN, seed);
    w.refuse_accepts("c", false);
    load.run(&mut w, Duration::from_secs(6), OPEN, seed);
    // Within the replicas' lease, and past the 15 s after which they drop a silent node's permits.
    for (name, secs) in [("b", 3), ("c", 20)] {
        load.excuse(&w, name, Duration::from_secs(secs));
        w.pause(name, Duration::from_secs(secs));
        load.run(&mut w, Duration::from_secs(secs + 4), OPEN, seed);
    }
    let one_in = w.pick(500..3000) as u32;
    load.excuse(&w, "a", Duration::from_secs(10));
    w.starve("a", one_in);
    load.run(&mut w, Duration::from_secs(10), OPEN, seed);
    w.starve("a", 1);
    load.run(&mut w, Duration::from_secs(4), OPEN, seed);
    w.step_wall("b", -30_000);
    load.run(&mut w, Duration::from_secs(4), OPEN, seed);
    load.steady(&mut w, seed);
    load.drain(w, "stream_limits_faults", seed);
}

#[test]
fn a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins_world() {
    check_seeds(seeds(0..64), a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins).unwrap();
}

#[test]
fn faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds_world() {
    check_seeds(seeds(0..64), faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds).unwrap();
}
