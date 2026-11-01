use super::*;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering::Relaxed};

#[tokio::test(flavor = "multi_thread")]
async fn static_seeds_and_authorizer() {
    struct NoSpawn;
    impl Authorizer for NoSpawn {
        fn authorize(&self, peer: &axon_net::Peer, action: Action<'_>) -> bool {
            peer.identity.node_id == "a" || matches!(action, Action::Call { name } if name != "secret")
        }
    }
    // Static seeds need fixed ports: bind first with a throwaway discovery to learn them.
    let probe = MemoryDiscovery::new();
    let a = node_with("a", probe.clone(), |cfg| cfg.authorizer = Some(Arc::new(NoSpawn))).await;
    let seeds = |b: std::net::SocketAddr| {
        let record = |id: &str, address| NodeRecord {
            id: id.into(),
            incarnation: 0,
            address,
            metadata: Default::default(),
            version: 0,
        };
        StaticDiscovery::new(vec![record("a", a.cluster.record().address), record("b", b)]).unwrap()
    };
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let b = node_with("b", seeds(port), |cfg| cfg.bind = port).await;
    assert_eq!(b.cluster.view().members.len(), 2);

    counter_kind(&a).await;
    let actor = publish_counter(&a, 3, "open");
    a.cluster.publish::<CounterApi>(&actor, "secret").unwrap();

    let open = b.cluster.resolve_on::<CounterApi>("a", "open").await.unwrap();
    assert_eq!(open.node_incarnation(), a.cluster.identity().incarnation);
    assert_eq!(number(open.request(Op::Get).await.unwrap()), 3);
    let secret = b.cluster.resolve_on::<CounterApi>("a", "secret").await.unwrap();
    assert!(matches!(secret.request(Op::Get).await, Err(CallError::Rejected(Rejection::Forbidden))));
    let spawn = b.cluster.spawn_on::<CounterApi>("a", "counter", seed(0)).await;
    assert!(matches!(spawn, Err(ClusterError::Call(CallError::Rejected(Rejection::Forbidden)))), "{spawn:?}");
}

const CUT: u8 = 1;

/// A local agent that lost its servers: it still takes heartbeats, which reach nobody.
const BLIND: u8 = 2;

/// Every call hangs.
const HUNG: u8 = 3;

/// Calls answer, and the snapshot lists a node twice.
const LYING: u8 = 4;

/// A registration is taken and goes nowhere, as seeds take one while frozen.
const TAKEN: u8 = 5;

/// Calls answer, and the snapshot lists one node more than the test's `max_nodes`.
const CROWDED: u8 = 6;

/// Every call panics, a heartbeat as it is asked for and a snapshot as it is polled.
const PANICKING: u8 = 7;

/// A node's own path to discovery, which can be cut without touching anyone else's.
#[derive(Clone)]
struct Cuttable(MemoryDiscovery, Arc<AtomicU8>);

impl Cuttable {
    fn pass<T: Send + 'static>(
        &self,
        call: BoxFuture<'static, Result<T, DiscoveryError>>,
    ) -> BoxFuture<'static, Result<T, DiscoveryError>> {
        match self.1.load(Relaxed) {
            0 | LYING | TAKEN | CROWDED | PANICKING => call,
            HUNG => Box::pin(std::future::pending()),
            _ => Box::pin(async { Err(DiscoveryError::Unavailable("cut".into())) }),
        }
    }
}

impl Discovery for Cuttable {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        let (inner, mode) = (self.0.clone(), self.1.load(Relaxed));
        self.pass(Box::pin(async move {
            let mut snapshot = inner.snapshot().await?;
            let first = snapshot.nodes.first().cloned();
            match mode {
                LYING => snapshot.nodes.extend(first),
                CROWDED => snapshot.nodes.extend(first.map(|n| NodeRecord { id: "crowd".into(), ..n })),
                PANICKING => panic!("a provider that panics"),
                _ => {}
            }
            Ok(snapshot)
        }))
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let (inner, record) = (self.0.clone(), record.clone());
        if self.1.load(Relaxed) == TAKEN {
            return Box::pin(async { Ok(()) });
        }
        self.pass(Box::pin(async move { inner.register(&record).await }))
    }

    /// Like a TTL check: a node the provider has forgotten is told so, and registers again.
    fn heartbeat(&self, id: &str, _: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        match self.1.load(Relaxed) {
            BLIND => return Box::pin(async { Ok(()) }),
            PANICKING => panic!("a provider that panics"),
            _ => {}
        }
        let (inner, id) = (self.0.clone(), id.to_owned());
        self.pass(Box::pin(async move {
            let known = inner.snapshot().await?.nodes.iter().any(|n| n.id == id);
            known.then_some(()).ok_or(DiscoveryError::Unavailable("unknown node".into()))
        }))
    }
}

/// Dropped, or cut off, 150 ms after its heartbeats stop or discovery stops answering.
fn quick_heartbeats(cfg: &mut ClusterConfig) {
    // Heartbeats every 30 ms and a grace of 150 ms, not the shipped 5 s, so the test waits less.
    cfg.heartbeat_interval = Duration::from_millis(30);
    cfg.heartbeat_grace = Duration::from_millis(150);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it() {
    dropped(None).await;
}

/// With an authority too: a peer's "dropped" answer stops the keys at once, without waiting for their fences.
#[tokio::test(flavor = "multi_thread")]
async fn a_leased_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it() {
    dropped(Some(MemoryLeases::new())).await;
}

async fn dropped(leases: Option<MemoryLeases>) {
    let discovery = MemoryDiscovery::new();
    let cuts = [Arc::new(AtomicU8::new(0)), Arc::new(AtomicU8::new(0))];
    let tweak = |cfg: &mut ClusterConfig| {
        quick_heartbeats(cfg);
        cfg.ownership = leases.as_ref().map(|l| Arc::new(l.authority()) as Arc<dyn OwnershipAuthority>);
    };
    let a = node_with("a", Cuttable(discovery.clone(), cuts[0].clone()), tweak).await;
    let b = node_with("b", Cuttable(discovery.clone(), cuts[1].clone()), tweak).await;
    counter_kind(&a).await;
    sees(&b, 2).await;
    let anywhere = Placement::default();
    let spawn = || a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere);
    spawn().await.unwrap();

    // Discovery is gone for everyone: nobody can take the key over, so it stays.
    cuts.iter().for_each(|cut| cut.store(CUT, Relaxed));
    eventually("heartbeats fail past the grace", || async { a.cluster.stats().heartbeat_failures > 10 }).await;
    assert_eq!((a.cluster.stats().spawned, a.cluster.stats().isolated), (1, false));

    // Only `a` is cut off and the others have dropped it: they may start the key elsewhere.
    cuts[1].store(0, Relaxed);
    discovery.unregister("a", a.cluster.identity().incarnation).await.unwrap();
    eventually("the keyed actor stops", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!((a.cluster.stats().isolations, a.cluster.stats().isolated), (1, true));
    let refused = spawn().await;
    assert!(matches!(refused, Err(ClusterError::Spawn(SpawnRefusal::Isolated, _))), "{refused:?}");

    cuts[0].store(0, Relaxed);
    let hosts_again = || async { !a.cluster.stats().isolated && spawn().await.is_ok() };
    eventually("the node is heard again and hosts the key", hosts_again).await;

    // Heartbeats still go through, but they reach nobody and snapshots fail: the same verdict.
    cuts[0].store(BLIND, Relaxed);
    discovery.unregister("a", a.cluster.identity().incarnation).await.unwrap();
    eventually("the keyed actor stops again", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!((a.cluster.stats().isolations, a.cluster.stats().isolated), (2, true));

    // Each registration is taken but none is heard: the heartbeat after it still fails.
    cuts[0].store(0, Relaxed);
    eventually("the node hosts the key again", hosts_again).await;
    cuts[0].store(TAKEN, Relaxed);
    discovery.unregister("a", a.cluster.identity().incarnation).await.unwrap();
    eventually("the keyed actor stops a third time", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!((a.cluster.stats().isolations, a.cluster.stats().isolated), (3, true));
    // Each start and end of an isolation is one event on the node's timeline.
    assert_eq!(isolations(&a.cluster), ["isolated", "isolation_ended", "isolated", "isolation_ended", "isolated"]);
    drop(b);
}

/// The starts and ends of isolation on the node's timeline.
fn isolations(cluster: &Cluster) -> Vec<&'static str> {
    told(cluster).into_iter().map(|(kind, ..)| kind).filter(|k| k.starts_with("isolat")).collect()
}

/// A provider whose first registrations land but answer an error, like an agent restarting while the node comes up.
#[derive(Clone)]
struct Flaky(MemoryDiscovery, Arc<AtomicU32>);

impl Discovery for Flaky {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        self.0.snapshot()
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let landed = self.0.register(record);
        if self.1.try_update(SeqCst, SeqCst, |n| n.checked_sub(1)).is_err() {
            return landed;
        }
        Box::pin(async move {
            landed.await?;
            Err(DiscoveryError::Unavailable("agent restarting".into()))
        })
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.unregister(id, incarnation)
    }

    fn heartbeat(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.heartbeat(id, incarnation)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_discovery_that_is_away_while_the_node_starts_is_retried_within_the_budget() {
    let flaky = Flaky(MemoryDiscovery::new(), Arc::new(2.into()));
    let started = std::time::Instant::now();
    // Retried every 50 ms, so two refusals fit the 2 s the test allows.
    let n = node_with("n1", flaky.clone(), |cfg| cfg.poll_interval = Duration::from_millis(50)).await;
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    assert_eq!(n.cluster.stats().discovery_errors, 2, "each refused registration is counted");
    assert_eq!(flaky.0.snapshot().await.unwrap().nodes.len(), 1);
    // Out of budget, the start fails rather than hangs, and takes back the registration that landed and the port.
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let never = Flaky(MemoryDiscovery::new(), Arc::new(u32::MAX.into()));
    let cfg = |cfg: &mut ClusterConfig| {
        // Out of budget in 100 ms rather than the shipped 5 s.
        (cfg.operation_timeout, cfg.poll_interval) = (Duration::from_millis(100), Duration::from_millis(50))
    };
    let system = ActorSystem::builder().build().unwrap();
    let mut config = ClusterConfig::new(port, transport("n2"), never.clone());
    cfg(&mut config);
    assert!(Cluster::start(&system, config).await.is_err());
    assert_eq!(never.0.snapshot().await.unwrap().nodes, vec![]);
    std::net::TcpListener::bind(port).expect("the port is given back");
    // A system that has shut down refuses the cluster last, once it has registered.
    system.shutdown().await.unwrap();
    let discovery = MemoryDiscovery::new();
    assert!(Cluster::start(&system, ClusterConfig::new(port, transport("n3"), discovery.clone())).await.is_err());
    assert_eq!(discovery.snapshot().await.unwrap().nodes, vec![]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lone_node_cut_off_from_discovery_stops_its_keyed_actors_and_hosts_them_again() {
    let cut = Arc::new(AtomicU8::new(0));
    let a = node_with("a", Cuttable(MemoryDiscovery::new(), cut.clone()), quick_heartbeats).await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    let spawn = || a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere);
    spawn().await.unwrap();

    // Nobody can vouch for it, so it cannot tell an outage from having been replaced.
    cut.store(CUT, Relaxed);
    eventually("the keyed actor stops", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!((a.cluster.stats().isolations, a.cluster.stats().isolated), (1, true));
    cut.store(0, Relaxed);
    eventually("it hosts the key again", || async { !a.cluster.stats().isolated && spawn().await.is_ok() }).await;
}

/// Hears the node's heartbeats one in `every`, or none at 0, and takes a registration only when it hears them all.
#[derive(Clone)]
struct Seldom(MemoryDiscovery, Arc<AtomicUsize>, Arc<AtomicUsize>);

impl Discovery for Seldom {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        self.0.snapshot()
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        match self.1.load(Relaxed) {
            1 => self.0.register(record),
            _ => Box::pin(async { Err(DiscoveryError::Unavailable("cut".into())) }),
        }
    }

    fn heartbeat(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let (every, call) = (self.1.load(Relaxed), self.2.fetch_add(1, Relaxed));
        match every {
            0 => Box::pin(async { Err(DiscoveryError::Unavailable("cut".into())) }),
            _ if call % every == 0 => self.0.heartbeat(id, incarnation),
            _ => Box::pin(async { Err(DiscoveryError::Unavailable("missed".into())) }),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_heard_now_and_then_stays_isolated_until_heard_for_a_whole_grace() {
    let every = Arc::new(AtomicUsize::new(1));
    let a = node_with("a", Seldom(MemoryDiscovery::new(), every.clone(), Arc::default()), quick_heartbeats).await;
    every.store(0, Relaxed);
    eventually("the node isolates", || async { a.cluster.stats().isolated }).await;

    // Heard one heartbeat in ten, 300 ms apart, each past the 150 ms grace.
    every.store(10, Relaxed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!((a.cluster.stats().isolations, a.cluster.stats().isolated), (1, true));
    assert_eq!(isolations(&a.cluster), ["isolated"]);

    // Heard one in three, 90 ms apart: inside the grace, so it would not isolate again, and it rejoins.
    every.store(3, Relaxed);
    eventually("the node rejoins", || async { !a.cluster.stats().isolated }).await;
    assert_eq!(a.cluster.stats().isolations, 1);
    assert_eq!(isolations(&a.cluster), ["isolated", "isolation_ended"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_discovery_provider_that_hangs_lies_or_panics_leaves_a_stale_view_and_a_working_node() {
    stale_view(false).await;
    stale_view(true).await;
}

async fn stale_view(watched: bool) {
    let discovery = MemoryDiscovery::new();
    let mode = Arc::new(AtomicU8::new(0));
    let tweak = move |cfg: &mut ClusterConfig| {
        cfg.max_nodes = 2;
        quick_heartbeats(cfg);
        // A hung discovery call gives up in 100 ms, so the test waits less.
        cfg.operation_timeout = Duration::from_millis(100);
        cfg.resync_interval = Duration::from_millis(200);
    };
    let cuttable = Cuttable(discovery.clone(), mode.clone());
    let a = match watched {
        true => node_with("a", Watched(cuttable), tweak).await,
        false => node_with("a", cuttable, tweak).await,
    };
    let b = node("b", &discovery).await;
    counter_kind(&b).await;
    sees(&a, 2).await;
    eventually("a sees that b hosts the kind", || async {
        a.cluster.spawn::<CounterApi>("counter", seed(0), &Placement::default()).await.is_ok()
    })
    .await;
    let counter = a.cluster.spawn::<CounterApi>("counter", seed(0), &Placement::default()).await.unwrap();

    // A watch waits out a snapshot that hangs or lies at the same revision; one that panics ends it.
    let modes: &[u8] = if watched { &[PANICKING] } else { &[HUNG, LYING, CROWDED, PANICKING] };
    for &broken in modes {
        mode.store(broken, Relaxed);
        let stale = a.cluster.wait_view(|view| view.stale.is_some());
        tokio::time::timeout(Duration::from_secs(5), stale).await.expect("the view never went stale").unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let view = a.cluster.view();
        let why = format!("mode {broken}, watched {watched}: {:?}", view.stale);
        assert_eq!((view.members.len(), a.cluster.stats().isolated), (2, false), "{why}");
        counter.tell(Op::Add(1)).await.unwrap();
        mode.store(0, Relaxed);
        let fresh = a.cluster.wait_view(|view| view.stale.is_none());
        tokio::time::timeout(Duration::from_secs(5), fresh).await.expect("the view never came back").unwrap();
    }
    assert_eq!(number(counter.request(Op::Get).await.unwrap()), modes.len() as u64);
}

/// The Nth call to the provider fails, or takes effect and never answers.
struct Fused {
    inner: MemoryDiscovery,
    at: usize,
    hang: bool,
    calls: Arc<AtomicUsize>,
}

impl Fused {
    fn pass<T: Send + 'static>(
        &self,
        call: BoxFuture<'static, Result<T, DiscoveryError>>,
    ) -> BoxFuture<'static, Result<T, DiscoveryError>> {
        match (self.calls.fetch_add(1, SeqCst) + 1 == self.at, self.hang) {
            (false, _) => call,
            (true, true) => Box::pin(async {
                let _ = call.await;
                std::future::pending().await
            }),
            (true, false) => Box::pin(async { Err(DiscoveryError::Unavailable("fused".into())) }),
        }
    }
}

impl Discovery for Fused {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        let inner = self.inner.clone();
        self.pass(Box::pin(async move { inner.snapshot().await }))
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let (inner, record) = (self.inner.clone(), record.clone());
        self.pass(Box::pin(async move { inner.register(&record).await }))
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let (inner, id) = (self.inner.clone(), id.to_owned());
        self.pass(Box::pin(async move { inner.unregister(&id, incarnation).await }))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_discovery_fault_at_any_call_of_a_start_leaves_nothing_behind() {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    // A start that fails before it reaches discovery gives the port back too.
    let mut cfg = ClusterConfig::new(port, transport("a"), MemoryDiscovery::new());
    cfg.labels.insert("bad\nkey".into(), "x".into());
    let system = ActorSystem::builder().build().unwrap();
    assert!(matches!(Cluster::start(&system, cfg).await, Err(ClusterError::Discovery(_))));

    for hang in [false, true] {
        for at in 1..=4 {
            let (discovery, calls) = (MemoryDiscovery::new(), Arc::new(AtomicUsize::new(0)));
            let fused = Fused { inner: discovery.clone(), at, hang, calls: calls.clone() };
            let mut cfg = ClusterConfig::new(port, transport("a"), fused);
            // Short, so each of the four fused calls fails fast.
            (cfg.poll_interval, cfg.operation_timeout) = (Duration::from_millis(40), Duration::from_millis(100));
            let system = ActorSystem::builder().build().unwrap();
            match Cluster::start(&system, cfg).await {
                Ok(cluster) => {
                    let joined = || async { cluster.view().stale.is_none() && cluster.view().get("a").is_some() };
                    eventually("the node gets over the fault", joined).await;
                    system.shutdown().await.unwrap();
                }
                Err(error) => {
                    assert!(calls.load(SeqCst) >= at, "hang {hang} at {at}: failed without a fault: {error}");
                    system.shutdown().await.unwrap();
                }
            }
            let left = discovery.snapshot().await.unwrap().nodes;
            assert!(left.is_empty(), "hang {hang} at {at}: still registered: {left:?}");
        }
    }
}

/// A snapshot of `discovery` past revision `after`, polled every 10 ms.
fn changed<D: Discovery>(discovery: &D, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
    Some(Box::pin(async move {
        loop {
            let snapshot = discovery.snapshot().await?;
            if snapshot.revision != after {
                return Ok(snapshot);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }))
}

#[derive(Clone, Default)]
struct Watched<D>(D);

impl<D: Discovery> Discovery for Watched<D> {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        self.0.snapshot()
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.register(record)
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.unregister(id, incarnation)
    }

    fn watch(&self, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        changed(&self.0, after)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_watches_is_not_polled_and_its_news_is_still_seen() {
    let discovery = MemoryDiscovery::new();
    // A watch that brought news is renewed at once, not a `poll_interval` later.
    let never = |cfg: &mut ClusterConfig| {
        // Never within the test, so only the watch can bring the news.
        cfg.resync_interval = Duration::from_secs(3600);
        cfg.poll_interval = Duration::from_secs(3600);
    };
    let a = node_with("a", Watched(discovery.clone()), never).await;
    let b = node_with("b", Watched(discovery.clone()), never).await;
    sees(&a, 2).await;
    b.system.shutdown().await.unwrap();
    sees(&a, 1).await;
    let c = node_with("c", Watched(discovery.clone()), never).await;
    sees(&a, 2).await;
    sees(&c, 2).await;
}

/// Watched, as a Consul agent: when `BLIND` its reads fail and when `HUNG` they hang, while heartbeats, counted, pass.
#[derive(Clone)]
struct Agent(MemoryDiscovery, Arc<AtomicU8>, Arc<AtomicUsize>);

impl Discovery for Agent {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        match self.1.load(SeqCst) {
            BLIND => Box::pin(async { Err(DiscoveryError::Unavailable("blind".into())) }),
            HUNG => Box::pin(std::future::pending()),
            _ => self.0.snapshot(),
        }
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.register(record)
    }

    fn heartbeat(&self, _: &str, _: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.2.fetch_add(1, SeqCst);
        Box::pin(async { Ok(()) })
    }

    fn watch(&self, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        changed(self, after)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_view_is_fresh_again_once_its_watched_provider_answers_and_a_hung_one_holds_no_heartbeat_back() {
    let (mode, beats) = (Arc::new(AtomicU8::new(0)), Arc::new(AtomicUsize::new(0)));
    let a = node_with("a", Agent(MemoryDiscovery::new(), mode.clone(), beats.clone()), |cfg| {
        // Never within the test, so only a heartbeat's snapshot can freshen the view.
        cfg.resync_interval = Duration::from_secs(3600);
        // Heartbeats every 20 ms, against a hung snapshot's 1 s, and never blind within the test.
        cfg.heartbeat_interval = Duration::from_millis(20);
        cfg.operation_timeout = Duration::from_secs(1);
        cfg.heartbeat_grace = Duration::from_secs(60);
    })
    .await;
    let stale = |stale: bool| a.cluster.wait_view(move |view| view.stale.is_some() == stale);
    // Past the loop's first poll, which is at once.
    eventually("heartbeats go", || async { beats.load(SeqCst) >= 3 }).await;

    // The watch fails, and discovery comes back at the revision the view has, so no watch answers.
    mode.store(BLIND, SeqCst);
    tokio::time::timeout(Duration::from_secs(5), stale(true)).await.expect("the view never went stale").unwrap();
    mode.store(0, SeqCst);
    tokio::time::timeout(Duration::from_secs(1), stale(false)).await.expect("the view stayed stale").unwrap();

    mode.store(BLIND, SeqCst);
    tokio::time::timeout(Duration::from_secs(5), stale(true)).await.expect("the view never went stale").unwrap();
    mode.store(HUNG, SeqCst);
    let before = beats.load(SeqCst);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let beaten = beats.load(SeqCst) - before;
    // About 100 at the interval; one per hung snapshot would be 2.
    assert!(beaten >= 20, "{beaten} heartbeats in 2 s while the snapshot hangs");
    a.system.shutdown().await.unwrap();
}

/// Answers its next snapshot, or its next watch when `watched`, with what it held when asked, once the test lets it.
#[derive(Clone, Default)]
struct Late {
    inner: Watched<MemoryDiscovery>,
    watched: bool,
    armed: Arc<AtomicBool>,
    asked: Arc<AtomicUsize>,
    registered: Arc<AtomicUsize>,
    taken: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl Late {
    fn answer(&self, after: Option<u64>) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        self.asked.fetch_add(1, SeqCst);
        let then = self.inner.snapshot();
        if self.armed.swap(false, SeqCst) {
            return Box::pin(async move {
                let then = then.await;
                self.taken.notify_one();
                self.release.notified().await;
                then
            });
        }
        let Some(after) = after else { return then };
        self.inner.watch(after).expect("Watched watches")
    }
}

impl Discovery for Late {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        if self.watched { self.inner.snapshot() } else { self.answer(None) }
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let registering = self.inner.register(record);
        Box::pin(async move {
            let registered = registering.await;
            self.registered.fetch_add(1, SeqCst);
            registered
        })
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.inner.unregister(id, incarnation)
    }

    fn watch(&self, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        self.watched.then(|| self.answer(Some(after)))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_asked_for_before_a_change_never_replaces_the_view_after_it() {
    let tagged = |view: &View| view.get("a").is_some_and(|m| m.record.label("k") == Some("v"));
    for watched in [false, true] {
        let late = Late { watched, armed: Arc::new(watched.into()), ..Late::default() };
        let a = node_with("a", late.clone(), |cfg| {
            // Not `poll_interval`: it is also the pause after an answer dropped while a snapshot is out.
            if watched {
                // Never within the test, so only the watch can bring the news.
                cfg.resync_interval = Duration::from_secs(3600);
            }
        })
        .await;
        let mut events = a.cluster.subscribe();
        // Watched, the first watch's answer is held; polled, the next poll's.
        if !watched {
            late.armed.store(true, SeqCst);
        }
        late.taken.notified().await;
        let held = late.asked.load(SeqCst);
        let cluster = a.cluster.clone();
        let set = tokio::spawn(async move { cluster.set_metadata("k", Some("v")).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        late.release.notify_one();
        set.await.unwrap().unwrap();
        assert!(tagged(&a.cluster.view()), "watched {watched}: {:?}", a.cluster.view());

        // Once the loop that held the answer asks again, it has applied that answer or dropped it.
        let next = held + if watched { 1 } else { 2 };
        eventually("the provider is asked again", || async { late.asked.load(SeqCst) >= next }).await;
        let mut seen = false;
        while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(200), events.next()).await {
            let (MembershipEvent::Snapshot(view)
            | MembershipEvent::Changes { view, .. }
            | MembershipEvent::Resync(view)) = event
            else {
                continue;
            };
            assert!(!seen || tagged(&view), "watched {watched}: the view went back to {view:?}");
            seen |= tagged(&view);
        }
        assert!(seen, "watched {watched}");
        a.system.shutdown().await.unwrap();
    }
}

/// A refresh that comes while a snapshot is out waits for it, since it was asked for before the change, and for one
/// more, which every caller that came meanwhile shares: two snapshots at most, and one asked for all of them.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_waits_for_the_snapshot_out_and_one_more_shared_with_every_caller() {
    let late = Late::default();
    // Never within the test, so only the refreshes ask for snapshots.
    let a = node_with("a", late.clone(), |cfg| cfg.poll_interval = Duration::from_secs(3600)).await;
    eventually("the poll loop's first snapshot", || async { late.asked.load(SeqCst) >= 2 }).await;
    late.armed.store(true, SeqCst);
    let set = |i: usize| {
        let cluster = a.cluster.clone();
        tokio::spawn(async move { cluster.set_metadata(&format!("k{i}"), Some("v")).await })
    };
    let first = set(0);
    late.taken.notified().await;
    let (held, registered) = (late.asked.load(SeqCst), late.registered.load(SeqCst));
    let queued: Vec<_> = (1..=5).map(set).collect();
    // A caller reads the refreshes done in the same poll that its register returns, with no await between.
    eventually("the queued callers registered", || async { late.registered.load(SeqCst) == registered + 5 }).await;
    assert!(queued.iter().all(|set| !set.is_finished()), "a refresh returned on a snapshot asked for before it");
    late.release.notify_one();
    first.await.unwrap().unwrap();
    for set in queued {
        set.await.unwrap().unwrap();
    }
    assert_eq!(late.asked.load(SeqCst) - held, 1, "the callers that came while one was out shared the next");
    let shown = a.cluster.view().get("a").map(|m| m.record.metadata.clone());
    assert_eq!(shown, Some(a.cluster.record().metadata), "every change is in the view");
    a.system.shutdown().await.unwrap();
}

/// Takes 200 ms over each snapshot and counts them, and notes when each heartbeat comes.
#[derive(Clone, Default)]
struct Slow {
    inner: MemoryDiscovery,
    snapshots: Arc<AtomicUsize>,
    beats: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
}

impl Discovery for Slow {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        self.snapshots.fetch_add(1, SeqCst);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.inner.snapshot().await
        })
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.inner.register(record)
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.inner.unregister(id, incarnation)
    }

    fn heartbeat(&self, _: &str, _: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.beats.lock().push(std::time::Instant::now());
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Refreshes that come together share snapshots instead of queueing for one each, and the heartbeat, which the
/// poll loop sends, never waits behind them.
#[tokio::test(flavor = "multi_thread")]
async fn refreshes_that_come_together_share_snapshots_and_the_heartbeat_goes_on() {
    let slow = Slow::default();
    let tick = Duration::from_millis(100);
    // Short ticks, so the refreshes and heartbeats overlap within the test.
    let a = node_with("a", slow.clone(), |cfg| (cfg.poll_interval, cfg.heartbeat_interval) = (tick, tick)).await;
    let (before, since) = (slow.snapshots.load(SeqCst), std::time::Instant::now());
    let sets: Vec<_> = (0..20)
        .map(|i| {
            let cluster = a.cluster.clone();
            tokio::spawn(async move { cluster.set_metadata("k", Some(&i.to_string())).await })
        })
        .collect();
    for set in sets {
        set.await.unwrap().unwrap();
    }
    // One after another, the 20 refreshes would take 20 snapshots and 4 s, and hold the heartbeat as long.
    let asked = slow.snapshots.load(SeqCst) - before;
    assert!(asked <= 6, "{asked} snapshots in {:?}", since.elapsed());
    tokio::time::sleep(3 * tick).await;
    let mut beats = vec![since];
    beats.extend(slow.beats.lock().iter().filter(|t| **t >= since));
    let gap = beats.windows(2).map(|w| w[1] - w[0]).max().expect("a beat");
    assert!(gap < Duration::from_secs(1), "the heartbeat stopped for {gap:?}");
    a.system.shutdown().await.unwrap();
}
