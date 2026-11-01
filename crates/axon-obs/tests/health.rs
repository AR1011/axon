#![cfg(feature = "dashboard")]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::*;
use axon_obs::{Dashboard, DashboardConfig, Health, User, hash_password};
use axon_testkit::{eventually, eventually_within};
use serde_json::{Value as Json, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

mod common;

/// A node's own path to discovery, which can be cut, one way or both, slowed or made to lose calls without touching
/// anyone else's.
#[derive(Clone, Default)]
struct Path {
    /// Its heartbeats and registrations fail: discovery stops hearing it.
    unheard: Arc<AtomicBool>,
    /// Its snapshots fail: it stops hearing discovery.
    deaf: Arc<AtomicBool>,
    /// About one call in three fails, at random, but never three of one kind in a row: a run of lost snapshots that
    /// spans the grace is discovery down, which isolates the node, as it should.
    lossy: Arc<AtomicBool>,
    lost: Arc<AtomicU64>,
    /// Calls lost in a row, by kind: snapshot, register, heartbeat.
    streaks: Arc<[AtomicU64; 3]>,
    delay_ms: Arc<AtomicU64>,
}

impl Path {
    fn cut(&self, cut: bool) {
        self.unheard.store(cut, Relaxed);
        self.deaf.store(cut, Relaxed);
    }
}

struct Cuttable(MemoryDiscovery, Path);

impl Cuttable {
    fn pass<T: Send + 'static>(
        &self,
        cut: &AtomicBool,
        kind: usize,
        call: BoxFuture<'static, Result<T, DiscoveryError>>,
    ) -> BoxFuture<'static, Result<T, DiscoveryError>> {
        let (path, streak) = (&self.1, &self.1.streaks[kind]);
        let lost = path.lossy.load(Relaxed) && streak.load(Relaxed) < 2 && axon::trace::random_u64().is_multiple_of(3);
        streak.store(if lost { streak.load(Relaxed) + 1 } else { 0 }, Relaxed);
        path.lost.fetch_add(lost as u64, Relaxed);
        let (cut, delay) = (cut.load(Relaxed) || lost, Duration::from_millis(path.delay_ms.load(Relaxed)));
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            match cut {
                false => call.await,
                true => Err(DiscoveryError::Unavailable("cut".into())),
            }
        })
    }
}

impl Discovery for Cuttable {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        let inner = self.0.clone();
        self.pass(&self.1.deaf, 0, Box::pin(async move { inner.snapshot().await }))
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let (inner, record) = (self.0.clone(), record.clone());
        self.pass(&self.1.unheard, 1, Box::pin(async move { inner.register(&record).await }))
    }

    fn heartbeat(&self, _: &str, _: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.pass(&self.1.unheard, 2, Box::pin(async { Ok(()) }))
    }
}

async fn node(id: &str, discovery: impl Discovery, tune: impl FnOnce(&mut ClusterConfig)) -> (ActorSystem, Cluster) {
    common::loopback(id, discovery, ActorSystem::builder(), tune).await
}

async fn probe(base: &str, path: &str) -> Option<(u16, Json)> {
    let response = reqwest::get(format!("{base}/{path}")).await.ok()?;
    Some((response.status().as_u16(), response.json().await.unwrap()))
}

fn ready(checks: Json) -> (u16, Json) {
    (200, json!({ "ok": true, "checks": checks }))
}

/// Every check passes.
fn green() -> (u16, Json) {
    ready(json!({ "system": "ok", "cluster": "ok", "ownership": "ok", "quotas": "ok" }))
}

fn unready(checks: Json) -> (u16, Json) {
    (503, json!({ "ok": false, "checks": checks }))
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_is_refused_while_a_node_is_isolated_and_given_back_once_it_is_heard_again() {
    let path = Path::default();
    let (system, cluster) = node("a", Cuttable(MemoryDiscovery::new(), path.clone()), |cfg| {
        // Isolated 150 ms after discovery stops answering, not the shipped 5 s.
        cfg.heartbeat_interval = Duration::from_millis(30);
        cfg.heartbeat_grace = Duration::from_millis(150);
    })
    .await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", health.address());
    let green = green();
    assert_eq!(probe(&base, "readyz").await.unwrap(), green);
    assert_eq!(probe(&base, "healthz").await.unwrap(), ready(json!({ "system": "ok" })));

    path.cut(true);
    eventually("the node isolates", || async { cluster.isolated() }).await;
    let red = unready(json!({ "system": "ok", "cluster": "isolated", "ownership": "ok", "quotas": "ok" }));
    eventually_within("readiness is refused", Duration::from_secs(1), || async {
        probe(&base, "readyz").await.unwrap() == red
    })
    .await;
    assert_eq!(probe(&base, "healthz").await.unwrap(), ready(json!({ "system": "ok" })), "an isolated node lives");

    path.cut(false);
    eventually("the node is heard again", || async { !cluster.isolated() }).await;
    let given_back = || async { probe(&base, "readyz").await.unwrap() == green };
    eventually_within("readiness is given back", Duration::from_secs(1), given_back).await;
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_waits_for_a_warming_acceptor() {
    let mut paxos = PaxosConfig::new(["a"]);
    // A 1 s lease, so the acceptor warms up within the test.
    paxos.lease = Duration::from_secs(1);
    paxos.store = Some(Arc::new(axon::files::MemoryFiles::new()));
    let leases = Arc::new(PaxosLeases::new(paxos).unwrap());
    let ownership = leases.clone();
    let (system, cluster) = node("a", MemoryDiscovery::new(), |cfg| cfg.ownership = Some(ownership)).await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", health.address());

    assert!(leases.warming() && cluster.stats().warming, "the console reads it from the stats");
    let red = unready(json!({ "system": "ok", "cluster": "ok", "ownership": "warming", "quotas": "ok" }));
    assert_eq!(probe(&base, "readyz").await.unwrap(), red);
    eventually("the acceptor warms up", || async { !leases.warming() }).await;
    let green = green();
    assert_eq!(probe(&base, "readyz").await.unwrap(), green);
    assert!(!cluster.stats().warming);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_waits_for_the_quota_replica_to_pull_and_not_for_a_candidate_that_refuses() {
    let discovery = MemoryDiscovery::new();
    let (system_a, cluster_a) = node("a", discovery.clone(), |_| {}).await;
    // Other candidates per region: a refuses b's pulls for good.
    let other = QuotaConfig { replicas_per_region: 7, ..QuotaConfig::default() };
    let _a = Quotas::start(&cluster_a, other).await.unwrap();
    // A quota node that never answers: its sessions never finish their handshake.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ghost = NodeRecord {
        id: "ghost".into(),
        incarnation: 1,
        address: silent.local_addr().unwrap(),
        metadata: [("axon.quota".to_owned(), String::new())].into(),
        version: 0,
    };
    discovery.register(&ghost).await.unwrap();
    let (system, cluster) = node("b", discovery.clone(), |_| {}).await;
    let _quotas = Quotas::start(&cluster, QuotaConfig::default()).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", health.address());

    let red = unready(json!({ "system": "ok", "cluster": "ok", "ownership": "ok", "quotas": "starting" }));
    assert_eq!(probe(&base, "readyz").await.unwrap(), red);
    assert!(cluster.stats().quotas_starting, "the console reads it from the stats");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(probe(&base, "readyz").await.unwrap(), red, "past a pull's patience, the ghost still has not answered");
    discovery.expire("ghost");
    let green = green();
    eventually("the ghost left, and a refused", || async { probe(&base, "readyz").await.unwrap() == green }).await;
    assert!(cluster.stats().quotas.is_some_and(|q| !q.pulling) && !cluster.stats().quotas_starting);
    system_a.shutdown().await.unwrap();
    system.shutdown().await.unwrap();
}

/// `a` stops hearing discovery before `b` starts, so its view never shows `b`: it serves `b` the keys `b` replicates by
/// `b`'s view, not by its own, which would give `b` none and let it turn ready with nothing pulled.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_quota_replica_pulls_its_keys_from_a_candidate_whose_view_does_not_show_it() {
    const REQ: Rate = Rate::new("req/s", 1000, Duration::from_secs(1));
    let (discovery, path) = (MemoryDiscovery::new(), Path::default());
    let (system_a, cluster_a) = node("a", Cuttable(discovery.clone(), path.clone()), |_| {}).await;
    let a = Quotas::start(&cluster_a, QuotaConfig::default()).await.unwrap();
    for i in 0..100 {
        a.take(format!("k{i}"), &[(REQ, 1)]).unwrap();
    }
    eventually("a replicates its keys", || async { a.stats().rows == 100 }).await;
    path.deaf.store(true, Relaxed);
    let (system_b, cluster_b) = node("b", discovery, |_| {}).await;
    eventually("b sees a", || async { cluster_b.view().get("a").is_some() }).await;
    let b = Quotas::start(&cluster_b, QuotaConfig::default()).await.unwrap();
    eventually("b pulled", || async { !cluster_b.quotas_starting() }).await;
    assert!(cluster_a.view().get("b").is_none(), "a never heard of b");
    assert_eq!(b.stats().rows, 100, "every key is b's too, and only the pull brought their rows");
    for system in [system_a, system_b] {
        system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_system_fails_liveness() {
    let (system, cluster) = node("a", MemoryDiscovery::new(), |_| {}).await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", health.address());
    assert_eq!(probe(&base, "healthz").await.unwrap(), ready(json!({ "system": "ok" })));
    drop((system, cluster));
    eventually("the probe port closes", || async { probe(&base, "healthz").await.is_none() }).await;
}

/// Holds the drain long enough to watch it.
struct Slow;

impl Actor for Slow {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(Duration::from_millis(1500));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_dashboard_host_answers_probes_without_credentials_and_readiness_ends_at_shutdown() {
    let (system, cluster) = node("a", MemoryDiscovery::new(), |_| {}).await;
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        base_path: "/ops".into(),
        users: vec![User { name: "admin".into(), hash: hash_password("secret").unwrap() }],
        ..Default::default()
    };
    let dashboard = Dashboard::start(&system, &cluster, cfg).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let bases = [format!("http://{}", dashboard.address()), format!("http://{}", health.address())];
    let green = green();
    for base in &bases {
        assert_eq!(probe(base, "readyz").await.unwrap(), green, "{base}");
        assert_eq!(probe(base, "healthz").await.unwrap(), ready(json!({ "system": "ok" })), "{base}");
    }
    for path in ["ops/api/overview", "ops/api/overview?at=/healthz"] {
        assert_eq!(reqwest::get(format!("{}/{path}", bases[0])).await.unwrap().status(), 401, "{path}");
    }
    for path in ["healthz/", "ops/healthz", "ops/readyz"] {
        let answer = reqwest::get(format!("{}/{path}", bases[0])).await.unwrap().text().await.unwrap();
        assert!(!answer.contains("checks"), "{path} answered as a probe: {answer}");
    }
    // A probe pointed under the base path would read the console's page as healthy for ever.
    for path in ["ops/healthz", "ops/readyz", "ops/healthz/", "ops/readyz/"] {
        assert_eq!(reqwest::get(format!("{}/{path}", bases[0])).await.unwrap().status(), 404, "{path}");
    }

    system.spawn(Slow).start().unwrap().tell(()).unwrap();
    let stopping = system.clone();
    let shutdown = tokio::spawn(async move { stopping.shutdown().await });
    let asked = Instant::now();
    let mut caught = TcpStream::connect(health.address()).await.unwrap();
    caught.write_all(b"GET /healthz HTTP/1.1\r\nhost: a\r\n").await.unwrap();
    let red = unready(json!({ "system": "shutting down", "cluster": "ok", "ownership": "ok", "quotas": "ok" }));
    for base in &bases {
        eventually("readiness is refused", || async { probe(base, "readyz").await.unwrap() == red }).await;
        assert_eq!(probe(base, "healthz").await.unwrap(), ready(json!({ "system": "ok" })), "draining is alive");
    }
    assert!(asked.elapsed() < Duration::from_secs(1), "{:?}", asked.elapsed());
    shutdown.await.unwrap().unwrap();
    for base in &bases {
        assert!(probe(base, "readyz").await.is_none(), "{base} is closed");
    }
    caught.write_all(b"\r\n").await.unwrap();
    let mut answer = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), caught.read_to_string(&mut answer)).await;
    assert!(answer.starts_with("HTTP/1.1 "), "a request under way at the stop is answered: {answer:?}");
}

/// Sends a probe on each connection and reads its answer, so that every one is kept alive.
async fn keep_alive(connections: &mut [TcpStream]) {
    for connection in connections {
        connection.write_all(b"GET /healthz HTTP/1.1\r\nhost: a\r\n\r\n").await.unwrap();
        let read = tokio::time::timeout(Duration::from_secs(2), connection.read(&mut [0; 1024])).await;
        assert!(matches!(read, Ok(Ok(1..))), "a connection within its share is served: {read:?}");
    }
}

async fn closed_at_once(address: SocketAddr) -> bool {
    let mut past = TcpStream::connect(address).await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), past.read(&mut [0; 1])).await;
    matches!(read, Ok(Ok(0) | Err(_)))
}

#[tokio::test(flavor = "multi_thread")]
async fn one_address_holds_64_connections_a_port_256_and_a_probe_from_elsewhere_still_answers() {
    let (system, cluster) = node("a", MemoryDiscovery::new(), |_| {}).await;
    // The flood comes from a trusted proxy, which has no share of its own: only the port's cap closes the 257th.
    let proxy = vec![[127, 0, 0, 1].into()];
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), trusted_proxies: proxy, ..Default::default() };
    let flooded = Dashboard::start(&system, &cluster, cfg).await.unwrap().address();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap().address();
    let (flooded_base, other) = (format!("http://{flooded}"), format!("http://{health}"));
    let green = green();

    // 256 connections that never send a request fill the port; each holds its place until the 10 s head deadline.
    let opened = Instant::now();
    let mut idle = Vec::new();
    for _ in 0..256 {
        idle.push(TcpStream::connect(flooded).await.unwrap());
    }
    for _ in 0..3 {
        assert!(closed_at_once(flooded).await, "a connection past the cap is closed at once");
        assert_eq!(probe(&other, "readyz").await.unwrap(), green, "the node stays ready and its other port answers");
    }
    keep_alive(&mut idle[255..]).await;
    assert!(opened.elapsed() < Duration::from_secs(9), "checked while the idle ones held: {:?}", opened.elapsed());
    drop(idle);
    let answers = || async { probe(&flooded_base, "readyz").await.is_some_and(|answer| answer == green) };
    eventually("a probe answers once they are gone", answers).await;

    // One address that keeps its connections busy past the head deadline holds 64, and no more.
    let mut busy = Vec::new();
    for _ in 0..64 {
        busy.push(TcpStream::connect(health).await.unwrap());
    }
    for _ in 0..3 {
        keep_alive(&mut busy).await;
        tokio::time::sleep(Duration::from_secs(4)).await;
    }
    keep_alive(&mut busy).await;
    assert!(closed_at_once(health).await, "a connection past its address's share is closed at once");
    // Another address on the same port is still served; only Linux routes all of 127/8 to loopback.
    if cfg!(target_os = "linux") {
        let elsewhere = reqwest::Client::builder().local_address(IpAddr::from([127, 0, 0, 2])).build().unwrap();
        let answer = elsewhere.get(format!("{other}/readyz")).send().await.unwrap();
        assert_eq!((answer.status().as_u16(), answer.json().await.unwrap()), green, "a probe from elsewhere");
    }
    drop(busy);
    let answers = || async { probe(&other, "readyz").await.is_some_and(|answer| answer == green) };
    eventually("the address gets its share back once its connections are gone", answers).await;
    system.shutdown().await.unwrap();
}

/// Probes asked every 20 ms over `span`: each answers ready, and within `at_once` where that is under test.
async fn ready_throughout(base: &str, span: Duration, at_once: Option<Duration>) {
    let green = green();
    let started = Instant::now();
    while started.elapsed() < span {
        let asked = Instant::now();
        assert_eq!(probe(base, "readyz").await.unwrap(), green, "after {:?}", started.elapsed());
        assert!(at_once.is_none_or(|bound| asked.elapsed() < bound), "a probe took {:?}", asked.elapsed());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Discovery stops hearing `a`, but `a` still hears it and `b` still lists `a`: `a` is not isolated, so it stays ready.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_discovery_cannot_hear_stays_ready_while_a_peer_still_lists_it() {
    let (discovery, path) = (MemoryDiscovery::new(), Path::default());
    let quick = |cfg: &mut ClusterConfig| {
        // `a` asks `b` 1 s after discovery stops hearing it, not the shipped 5 s, and waits half that for the answer.
        // A 150 ms grace left 75 ms for the first ask, which dials `b`, and a 2-core CI runner once took longer.
        cfg.heartbeat_interval = Duration::from_millis(30);
        cfg.heartbeat_grace = Duration::from_secs(1);
    };
    let (system, cluster) = node("a", Cuttable(discovery.clone(), path.clone()), quick).await;
    let (b_system, _b) = node("b", discovery, quick).await;
    eventually("a sees b", || async { cluster.view().get("b").is_some_and(|b| b.reachable) }).await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", health.address());

    path.unheard.store(true, Relaxed);
    // Twice the grace, so a node that isolated without asking its peers would show.
    ready_throughout(&base, Duration::from_secs(2), None).await;
    assert!(!cluster.isolated());
    assert!(cluster.stats().last_heartbeat_error.is_some(), "discovery still heard a");
    for system in [system, b_system] {
        system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_discovery_calls_are_lost_one_in_three_stays_ready() {
    let path = Path::default();
    let (system, cluster) = node("a", Cuttable(MemoryDiscovery::new(), path.clone()), |cfg| {
        // Heartbeats every 30 ms with a 600 ms grace, so a lost call would show within the test. Not 300 ms: a lost
        // snapshot stays unanswered while the cluster's loop stalls under load, and past the grace that reads as
        // discovery down.
        cfg.heartbeat_interval = Duration::from_millis(30);
        cfg.heartbeat_grace = Duration::from_millis(600);
    })
    .await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    path.lossy.store(true, Relaxed);
    ready_throughout(&format!("http://{}", health.address()), Duration::from_millis(1500), None).await;
    assert!(path.lost.load(Relaxed) >= 10, "lost {}", path.lost.load(Relaxed));
    system.shutdown().await.unwrap();
}

/// Every discovery call takes 100 ms, inside the grace: readiness holds, and a probe does not wait on discovery.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_with_slow_discovery_stays_ready_and_its_probes_answer_at_once() {
    let path = Path::default();
    let (system, cluster) = node("a", Cuttable(MemoryDiscovery::new(), path.clone()), |cfg| {
        // Heartbeats every 30 ms with a 500 ms grace, so slow discovery would show within the test.
        cfg.heartbeat_interval = Duration::from_millis(30);
        cfg.heartbeat_grace = Duration::from_millis(500);
    })
    .await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    path.delay_ms.store(100, Relaxed);
    let at_once = Some(Duration::from_millis(100));
    ready_throughout(&format!("http://{}", health.address()), Duration::from_millis(1500), at_once).await;
    system.shutdown().await.unwrap();
}

/// A process that restarts takes its probe and console ports back as soon as `shutdown` returns.
// One worker: a task that would close a port after the hooks cannot run before `Rebind` binds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_restarted_node_serves_its_probes_and_console_on_the_same_ports() {
    let mut binds = ["127.0.0.1:0".parse().unwrap(); 2];
    for boot in 0..2 {
        let (system, cluster) = node("a", MemoryDiscovery::new(), |_| {}).await;
        let rebind = Arc::new(common::Rebind::default());
        system.attach_extension(rebind.clone()).unwrap();
        let health = Health::start(&system, &cluster, binds[0]).await.unwrap();
        let cfg = DashboardConfig { bind: binds[1], ..Default::default() };
        let dashboard = Dashboard::start(&system, &cluster, cfg).await.unwrap();
        binds = [health.address(), dashboard.address()];
        *rebind.0.lock() = binds.to_vec();
        let bases = binds.map(|bind| format!("http://{bind}"));
        for base in &bases {
            ready_throughout(base, Duration::from_millis(200), None).await;
        }
        let overview = reqwest::get(format!("{}/api/overview", bases[1])).await.unwrap();
        assert_eq!(overview.status(), 200, "boot {boot}");
        assert_eq!(system.shutdown().await.unwrap().failures, Vec::<String>::new(), "boot {boot}");
    }
}
