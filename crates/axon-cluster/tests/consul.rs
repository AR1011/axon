//! Runs against a real agent, so every test is `#[ignore]`d: `scripts/check.sh live`, or
//! `AXON_CONSUL=http://127.0.0.1:18500 cargo test -p axon-cluster --features consul --test consul -- --ignored`.
#![cfg(feature = "consul")]
#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon_cluster::*;

fn config() -> ConsulConfig {
    let base = std::env::var("AXON_CONSUL").expect("AXON_CONSUL is set for the live tests: scripts/check.sh live");
    // The clock alone is too coarse to tell apart tests that start together.
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let unique = std::time::SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos();
    let count = NEXT.fetch_add(1, Relaxed);
    ConsulConfig::new(base, format!("axon-test-{unique}-{count}"))
}

fn record(id: &str, incarnation: u128, port: u16) -> NodeRecord {
    let metadata = [("axon.kinds".to_string(), "a,b".to_string()), ("region".to_string(), "eu".to_string())].into();
    NodeRecord {
        id: id.into(),
        incarnation,
        address: format!("127.0.0.1:{port}").parse().unwrap(),
        metadata,
        version: 0,
    }
}

#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn discovery_round_trip() {
    let cfg = config();
    let consul = ConsulDiscovery::new(cfg).unwrap();
    let (a, b_old, b_new) = (record("node a/1", 7, 7001), record("b", 1, 7002), record("b", 2, 7003));
    for r in [&a, &b_old, &b_new] {
        consul.register(r).await.unwrap();
    }
    let snapshot = consul.snapshot().await.unwrap();
    assert!(snapshot.revision > 0);
    assert_eq!(snapshot.nodes, vec![b_new.clone(), a.clone()], "newest boot wins, metadata survives");

    let version = |s: &Snapshot, id: &str| s.nodes.iter().find(|n| n.id == id).unwrap().version;
    let first = version(&snapshot, "b");
    consul.heartbeat("b", 2).await.unwrap();
    consul.register(&b_new).await.unwrap();
    assert_eq!(version(&consul.snapshot().await.unwrap(), "b"), first, "a heartbeat or the same record moves nothing");
    consul.unregister("b", 2).await.unwrap();
    consul.register(&b_new).await.unwrap();
    assert!(version(&consul.snapshot().await.unwrap(), "b") > first, "a new registration has a new version");

    let mut draining = a.clone();
    draining.metadata.insert(LIFECYCLE.into(), DRAINING.into());
    consul.register(&draining).await.unwrap();
    consul.heartbeat("b", 2).await.unwrap();
    assert!(consul.snapshot().await.unwrap().nodes.iter().any(|n| n.id == a.id && n.draining()));

    for r in [&a, &b_old, &b_new] {
        consul.unregister(&r.id, r.incarnation).await.unwrap();
    }
    assert!(consul.snapshot().await.unwrap().nodes.is_empty());
    assert!(consul.heartbeat("b", 2).await.is_err());
}

#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn leases_fence_with_growing_epochs() {
    let mut cfg = config();
    // Consul holds a released lock for lock_delay: 1 s, not the shipped 5 s, keeps the test short.
    cfg.lock_delay = Duration::from_secs(1);
    let (one, two) = (ConsulLeases::new(cfg.clone(), "one").unwrap(), ConsulLeases::new(cfg, "two").unwrap());
    let first = one.acquire("pricer/EUR USD").await.unwrap();
    assert_eq!(two.acquire("pricer/EUR USD").await, Err(OwnershipError::Held(None)));
    one.renew().await.unwrap();
    one.release("pricer/EUR USD", first).await.unwrap();
    let second = two.acquire("pricer/EUR USD").await.unwrap();
    assert!(second > first);
    two.release("pricer/EUR USD", second).await.unwrap();
}

#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_watch_waits_for_a_change_and_answers_as_soon_as_there_is_one() {
    let mut cfg = config();
    // Long enough to see the watch wait, shorter than the shipped 30 s.
    cfg.watch_wait = Duration::from_secs(2);
    let consul = std::sync::Arc::new(ConsulDiscovery::new(cfg).unwrap());
    let a = record("a", 1, 7001);
    consul.register(&a).await.unwrap();
    // The registration may still be settling; once it has, a watch has nothing to report and waits.
    let mut quiet = consul.snapshot().await.unwrap();
    loop {
        let started = std::time::Instant::now();
        let next = consul.watch(quiet.revision).unwrap().await.unwrap();
        if started.elapsed() >= Duration::from_secs(2) {
            assert_eq!(next.nodes, quiet.nodes);
            break;
        }
        quiet = next;
    }

    let watcher = consul.clone();
    // Consul writes a service and its check one after the other, so the first news may still lack the node.
    let watching = tokio::spawn(async move {
        let mut news = quiet;
        while news.nodes.len() < 2 {
            news = watcher.watch(news.revision).unwrap().await.unwrap();
        }
        news
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (b, started) = (record("b", 2, 7002), std::time::Instant::now());
    consul.register(&b).await.unwrap();
    let news = watching.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
    assert_eq!(news.nodes.len(), 2);
    for r in [&a, &b] {
        consul.unregister(&r.id, r.incarnation).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_burst_of_lease_calls_stays_under_the_agents_connection_limit() {
    let cfg = config();
    let leases = std::sync::Arc::new(ConsulLeases::new(cfg, "burst").unwrap());
    let mut calls = tokio::task::JoinSet::new();
    for i in 0..400 {
        let leases = leases.clone();
        calls.spawn(async move {
            let key = format!("burst/{i}");
            let epoch = leases.acquire(&key).await.unwrap();
            leases.release(&key, epoch).await.unwrap();
        });
    }
    while let Some(done) = calls.join_next().await {
        done.unwrap();
    }
}

async fn agent(method: reqwest::Method, path: &str) -> serde_json::Value {
    let base = std::env::var("AXON_CONSUL").expect("AXON_CONSUL");
    let response = reqwest::Client::new().request(method, format!("{base}{path}")).send().await.unwrap();
    response.error_for_status().unwrap().json().await.unwrap()
}

fn session_of(leases: &ConsulLeases) -> String {
    leases.describe().stats.into_iter().find(|(name, _)| name == "session").unwrap().1
}

/// Every lease taken in a burst of first acquires is held by the one session the node renews. A session per
/// caller would leave all but one unrenewed, and Consul would unlock their keys under running actors.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_burst_of_first_acquires_shares_one_session() {
    let cfg = config();
    let (service, leases) = (cfg.service.clone(), std::sync::Arc::new(ConsulLeases::new(cfg, "burst").unwrap()));
    let mut calls = tokio::task::JoinSet::new();
    for i in 0..30 {
        let leases = leases.clone();
        calls.spawn(async move { leases.acquire(&format!("candles/{i}")).await.unwrap() });
    }
    while let Some(done) = calls.join_next().await {
        done.unwrap();
    }
    let session = session_of(&leases);
    let sessions = agent(reqwest::Method::GET, "/v1/session/list").await;
    let ours: Vec<_> =
        sessions.as_array().unwrap().iter().filter(|s| s["Name"] == format!("{service}:burst")).collect();
    assert_eq!(ours.len(), 1, "sessions: {:?}", ours.iter().map(|s| &s["ID"]).collect::<Vec<_>>());
    let keys = agent(reqwest::Method::GET, &format!("/v1/kv/{service}/?recurse")).await;
    let holders: Vec<_> = keys.as_array().unwrap().iter().map(|k| k["Session"].as_str().unwrap_or("none")).collect();
    assert_eq!(holders, vec![session.as_str(); 30]);
    agent(reqwest::Method::PUT, &format!("/v1/session/destroy/{session}")).await;
}

/// A node whose leases lapsed while it renewed nothing (its keyed actors were stopped on a renewal timeout or
/// an isolation) still starts keys afterwards: the dead session is replaced, not reused forever.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_lapsed_session_is_replaced_on_the_next_acquire() {
    let leases = ConsulLeases::new(config(), "lapsed").unwrap();
    leases.acquire("candles/a").await.unwrap();
    let lapsed = session_of(&leases);
    agent(reqwest::Method::PUT, &format!("/v1/session/destroy/{lapsed}")).await;
    let epoch = leases.acquire("candles/b").await.unwrap();
    let session = session_of(&leases);
    assert_ne!(session, lapsed);
    // Consul released the key taken under the lapsed session; the renewal must not pass it off as held.
    assert_eq!(leases.renew().await, Ok(vec![("candles/a".into(), OwnershipError::Lost)]));
    assert_eq!(leases.renew().await, Ok(vec![]));
    leases.release("candles/b", epoch).await.unwrap();
    agent(reqwest::Method::PUT, &format!("/v1/session/destroy/{session}")).await;
}

/// What an eager kind asks after a node is gone: the keys it holds, and those it held when its session ended
/// without a release, as a crashed node's does, until another node takes them. A key it released, or of a kind not
/// asked for, is not listed.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn held_by_lists_a_nodes_keys_until_another_node_takes_them() {
    let mut cfg = config();
    // Consul holds a released lock for lock_delay: 1 s, not the shipped 5 s, keeps the test short.
    cfg.lock_delay = Duration::from_secs(1);
    let service = cfg.service.clone();
    let (one, two) = (ConsulLeases::new(cfg.clone(), "one").unwrap(), ConsulLeases::new(cfg, "two").unwrap());
    let kinds = ["eager".to_string()];
    let held = async |node| two.held_by(Holder::Node(node), &kinds).expect("Consul lists a node's keys").await.unwrap();
    assert!(held("one").await.is_empty(), "a kind never leased has no keys in Consul");
    one.acquire("eager/EUR USD").await.unwrap();
    let deleted = one.acquire("lazy/a").await.unwrap();
    agent(reqwest::Method::DELETE, &format!("/v1/kv/{service}/lazy/a")).await;
    one.release("lazy/a", deleted).await.expect("a key Consul no longer has is released");
    let released = one.acquire("eager/b").await.unwrap();
    two.acquire("eager/c").await.unwrap();
    one.release("eager/b", released).await.unwrap();
    assert_eq!(held("one").await, ["eager/EUR USD"]);

    let session = session_of(&one);
    agent(reqwest::Method::PUT, &format!("/v1/session/destroy/{session}")).await;
    assert_eq!(held("one").await, ["eager/EUR USD"], "a session that ended keeps its keys listed");
    let lapsed = two.held_by(Holder::Lapsed, &kinds).expect("Consul lists lapsed keys").await.unwrap();
    assert_eq!(lapsed, ["eager/EUR USD"], "a key with a value and no session, not two's live eager/c");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while two.acquire("eager/EUR USD").await.is_err() {
        assert!(std::time::Instant::now() < deadline, "the lock delay never passed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(held("one").await.is_empty());
    assert_eq!(held("two").await, ["eager/EUR USD", "eager/c"]);
    two.close().await;
}

/// A TCP proxy in front of the agent that holds, slows, drops or answers for it.
struct Proxy {
    url: String,
    faults: std::sync::Arc<Faults>,
}

#[derive(Default)]
struct Faults {
    /// Connections numbered below this hold what their client sends: `u64::MAX` holds them all.
    hold_up_below: AtomicU64,
    /// Connections hold what the server sends back while this is set.
    hold_down: AtomicBool,
    delay_ms: AtomicU64,
    /// New connections are closed at once, as by an agent that is down.
    refuse: AtomicBool,
    /// New connections are answered with these bytes instead of the agent's.
    canned: std::sync::Mutex<Option<Vec<u8>>>,
    /// Chunks a client sent that waited in a hold.
    held: AtomicU64,
    accepted: AtomicU64,
    /// Bumped to close every connection open.
    cut: tokio::sync::watch::Sender<u64>,
}

impl Proxy {
    async fn start() -> Self {
        let agent = std::env::var("AXON_CONSUL").expect("AXON_CONSUL");
        let upstream = agent.trim_start_matches("http://").trim_end_matches('/').to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let faults = std::sync::Arc::new(Faults::default());
        let shared = faults.clone();
        tokio::spawn(async move {
            for id in 0.. {
                let Ok((client, _)) = listener.accept().await else { return };
                shared.accepted.store(id + 1, Relaxed);
                if shared.refuse.load(Relaxed) {
                    continue;
                }
                if let Some(canned) = shared.canned.lock().unwrap().clone() {
                    tokio::spawn(answer(client, canned));
                    continue;
                }
                let Ok(agent) = tokio::net::TcpStream::connect(&upstream).await else { continue };
                let ((from_client, to_client), (from_agent, to_agent)) = (client.into_split(), agent.into_split());
                tokio::spawn(pump(from_client, to_agent, shared.clone(), Some(id)));
                tokio::spawn(pump(from_agent, to_client, shared.clone(), None));
            }
        });
        Self { url, faults }
    }

    fn cut_all(&self) {
        self.faults.cut.send_modify(|n| *n += 1);
    }

    /// Holds what clients send on the connections open now, and lets new ones through.
    fn hold_open_connections(&self) {
        self.faults.hold_up_below.store(self.faults.accepted.load(Relaxed), Relaxed);
    }

    fn clear(&self) {
        let f = &self.faults;
        f.hold_up_below.store(0, Relaxed);
        f.hold_down.store(false, Relaxed);
        f.delay_ms.store(0, Relaxed);
        f.refuse.store(false, Relaxed);
        *f.canned.lock().unwrap() = None;
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    faults: std::sync::Arc<Faults>,
    up: Option<u64>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut buf, mut cut) = (vec![0; 64 << 10], faults.cut.subscribe());
    let held = |f: &Faults| match up {
        Some(id) => id < f.hold_up_below.load(Relaxed),
        None => f.hold_down.load(Relaxed),
    };
    loop {
        let n = tokio::select! {
            read = from.read(&mut buf) => match read {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            },
            _ = cut.changed() => return,
        };
        if held(&faults) {
            faults.held.fetch_add(1, Relaxed);
            while held(&faults) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(faults.delay_ms.load(Relaxed))).await;
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    // A request let go after its client gave up still reaches the agent whole, and is acted on.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = to.shutdown().await;
}

async fn answer(mut client: tokio::net::TcpStream, canned: Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = client.read(&mut [0; 64 << 10]).await;
    let _ = client.write_all(&canned).await;
    let _ = client.shutdown().await;
}

fn through(proxy: &Proxy, service: &str) -> ConsulConfig {
    let mut cfg = ConsulConfig::new(&proxy.url, service);
    // A 2 s session and 1 s calls, so a lapse or a hung call shows within the test.
    cfg.timeout = Duration::from_secs(1);
    cfg.ttl = Duration::from_secs(2);
    cfg.lock_delay = Duration::from_secs(1);
    cfg
}

const CALL_BOUND: Duration = Duration::from_millis(1500);

async fn timed<T>(call: impl std::future::Future<Output = T>) -> (T, Duration) {
    let started = std::time::Instant::now();
    let got = call.await;
    (got, started.elapsed())
}

/// Through an agent that is cut off both ways, whose answers are lost, or that is down, every discovery call fails
/// `Unavailable` within its timeout; through one 300 ms away each call answers. A node whose heartbeats stop, as when
/// it crashed, paused or starved, drops out of the passing list within its TTL, and is back on its next heartbeat.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out() {
    let (direct, proxy) = (config(), Proxy::start().await);
    let near = ConsulDiscovery::new(through(&proxy, &direct.service)).unwrap();
    let direct = ConsulDiscovery::new(direct).unwrap();
    let a = record("a", 1, 7001);
    near.register(&a).await.unwrap();
    let listed = async || direct.snapshot().await.unwrap().nodes.into_iter().map(|n| n.id).collect::<Vec<_>>();
    assert_eq!(listed().await, ["a"]);

    for fault in ["partition", "answers lost", "down"] {
        match fault {
            "partition" => {
                proxy.faults.hold_up_below.store(u64::MAX, Relaxed);
                proxy.faults.hold_down.store(true, Relaxed);
            }
            "answers lost" => proxy.faults.hold_down.store(true, Relaxed),
            _ => {
                proxy.faults.refuse.store(true, Relaxed);
                proxy.cut_all();
            }
        }
        let (beat, took) = timed(near.heartbeat("a", 1)).await;
        assert!(matches!(beat, Err(DiscoveryError::Unavailable(_))) && took < CALL_BOUND, "{fault}: {beat:?} {took:?}");
        let (seen, took) = timed(near.snapshot()).await;
        assert!(matches!(seen, Err(DiscoveryError::Unavailable(_))) && took < CALL_BOUND, "{fault}: {seen:?} {took:?}");
        let (again, took) = timed(near.register(&a)).await;
        assert!(matches!(again, Err(DiscoveryError::Unavailable(_))) && took < CALL_BOUND, "{fault}: {again:?}");
        proxy.clear();
        proxy.cut_all();
    }

    proxy.faults.delay_ms.store(300, Relaxed);
    let (beat, took) = timed(near.heartbeat("a", 1)).await;
    assert!(beat.is_ok() && took >= Duration::from_millis(600), "slow: {beat:?} {took:?}");
    let (seen, _) = timed(near.snapshot()).await;
    assert_eq!(seen.unwrap().nodes.len(), 1, "slow");
    proxy.clear();

    near.heartbeat("a", 1).await.unwrap();
    let stopped = std::time::Instant::now();
    while !listed().await.is_empty() {
        assert!(stopped.elapsed() < Duration::from_secs(6), "a node that stopped heartbeating is still listed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stopped.elapsed() >= Duration::from_millis(1500), "dropped {:?} into a 2 s TTL", stopped.elapsed());
    near.heartbeat("a", 1).await.unwrap();
    assert_eq!(listed().await, ["a"]);
    near.unregister("a", 1).await.unwrap();
}

/// A boot whose wall clock stepped back registers an older incarnation than the boot before it: the newer record wins
/// until its check lapses, and then the older one is listed.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_boot_whose_clock_stepped_back_is_listed_once_the_newer_boots_check_lapses() {
    let mut cfg = config();
    // A 2 s check, so the newer boot's check lapses within the test.
    cfg.ttl = Duration::from_secs(2);
    let consul = ConsulDiscovery::new(cfg).unwrap();
    let (newer, older) = (record("b", 2, 7002), record("b", 1, 7003));
    consul.register(&newer).await.unwrap();
    consul.register(&older).await.unwrap();
    assert_eq!(consul.snapshot().await.unwrap().nodes, vec![newer.clone()]);
    let started = std::time::Instant::now();
    loop {
        consul.heartbeat("b", 1).await.unwrap();
        let nodes = consul.snapshot().await.unwrap().nodes;
        if nodes.iter().any(|n| n.incarnation == 1) {
            assert_eq!(nodes.len(), 1);
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(6), "the lapsed newer boot is still listed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for r in [&newer, &older] {
        consul.unregister(&r.id, r.incarnation).await.unwrap();
    }
}

/// 400 heartbeats and snapshots at once from one node all answer: requests wait their turn instead of opening more
/// connections than the agent allows a client.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_burst_of_discovery_calls_stays_under_the_agents_connection_limit() {
    let consul = std::sync::Arc::new(ConsulDiscovery::new(config()).unwrap());
    let a = record("a", 1, 7001);
    consul.register(&a).await.unwrap();
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..400 {
        let consul = consul.clone();
        calls.spawn(async move {
            consul.heartbeat("a", 1).await.unwrap();
            assert_eq!(consul.snapshot().await.unwrap().nodes.len(), 1);
        });
    }
    while let Some(done) = calls.join_next().await {
        done.unwrap();
    }
    consul.unregister("a", 1).await.unwrap();
}

/// An agent that answers 500 (its disk full or failing), garbage, a body past the size cap, or not HTTP at all: each
/// call that reads the answer fails, none panics, and entries that do not parse are skipped. The next agent that
/// answers properly is taken.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken() {
    let (service, proxy) = (config().service, Proxy::start().await);
    let http = |status: &str, body: &[u8]| {
        let head = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
        [head.as_bytes(), body].concat()
    };
    let a = record("a", 1, 7001);
    let bogus = br#"[{"Service":{"ID":"x","Meta":{"n":"a"}}},{"nope":1},7,{"Service":{"Address":"a","Port":-1}}]"#;
    for (what, canned) in [
        ("disk full", http("500 Internal Server Error", b"rpc error: no space left on device")),
        ("garbage", http("200 OK", b"{nope")),
        ("oversized", http("200 OK", &[b'['; 4096])),
        ("not http", b"\x00\x01\x02 nothing like a status line\r\n\r\n".to_vec()),
        ("bogus entries", http("200 OK", bogus)),
    ] {
        *proxy.faults.canned.lock().unwrap() = Some(canned);
        let mut cfg = through(&proxy, &service);
        cfg.max_response_bytes = 1024;
        let discovery = ConsulDiscovery::new(cfg.clone()).unwrap();
        let seen = discovery.snapshot().await;
        match what {
            "bogus entries" => assert_eq!(seen.unwrap().nodes, vec![], "{what}"),
            _ => assert!(matches!(seen, Err(DiscoveryError::Unavailable(_))), "{what}: {seen:?}"),
        }
        // A registration or heartbeat reads only the status: a 200's body is not looked at.
        if !what.contains("garbage") && !what.contains("bogus") {
            assert!(discovery.register(&a).await.is_err(), "{what}");
            assert!(discovery.heartbeat("a", 1).await.is_err(), "{what}");
        }
        let leases = ConsulLeases::new(cfg, "one").unwrap();
        let took = leases.acquire("k").await;
        assert!(took.is_err(), "{what}: {took:?}");
    }
    proxy.clear();
    let discovery = ConsulDiscovery::new(through(&proxy, &service)).unwrap();
    discovery.register(&a).await.unwrap();
    assert_eq!(discovery.snapshot().await.unwrap().nodes.len(), 1);
    discovery.unregister("a", 1).await.unwrap();
}

/// A holder that stops renewing without a release, crashed, paused or starved, keeps its key from another node and
/// from its own next boot until its session lapses (the 10 s TTL, which Consul lets run up to twice) and the lock
/// delay; then the key is taken under a newer epoch, which the old holder's next boot cannot take from it.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_holder_that_stops_renewing_keeps_its_key_until_its_session_lapses_and_the_next_holder_has_a_newer_epoch() {
    let mut cfg = config();
    // A 1 s lock delay, so the next holder takes the key within the test.
    cfg.lock_delay = Duration::from_secs(1);
    let one = ConsulLeases::new(cfg.clone(), "one").unwrap();
    let first = one.acquire("k").await.unwrap();
    let stopped = std::time::Instant::now();
    drop(one);
    let (two, next_boot) = (ConsulLeases::new(cfg.clone(), "two").unwrap(), ConsulLeases::new(cfg, "one").unwrap());
    assert_eq!(next_boot.acquire("k").await, Err(OwnershipError::Held(None)));
    let second = loop {
        match two.acquire("k").await {
            Ok(epoch) => break epoch,
            Err(e) => assert_eq!(e, OwnershipError::Held(None)),
        }
        assert!(stopped.elapsed() < Duration::from_secs(30), "the lapsed session never let go");
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert!(stopped.elapsed() >= Duration::from_secs(10), "let go after {:?}", stopped.elapsed());
    assert!(second > first);
    assert_eq!(next_boot.acquire("k").await, Err(OwnershipError::Held(None)));
    two.close().await;
    next_boot.close().await;
}

/// Through an agent cut off both ways, lease calls fail `Unavailable` within the timeout, a renewal included, which
/// gives up no key: the holder's grace decides. An acquire whose answer was lost took the lock, and asking again gets
/// it with the same epoch, as does asking twice for a key held. Through an agent 300 ms away every call answers.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn lease_calls_through_a_cut_or_slow_agent_end_in_time_and_an_acquire_asked_again_keeps_its_epoch() {
    let (service, proxy) = (config().service, Proxy::start().await);
    let leases = ConsulLeases::new(through(&proxy, &service), "one").unwrap();
    let held = leases.acquire("a").await.unwrap();
    proxy.faults.hold_up_below.store(u64::MAX, Relaxed);
    proxy.faults.hold_down.store(true, Relaxed);
    let (took, spent) = timed(leases.acquire("b")).await;
    assert!(matches!(took, Err(OwnershipError::Unavailable(_))) && spent < CALL_BOUND, "{took:?} {spent:?}");
    let (renewed, spent) = timed(leases.renew()).await;
    assert!(matches!(renewed, Err(OwnershipError::Unavailable(_))) && spent < CALL_BOUND, "{renewed:?}");
    proxy.clear();
    proxy.cut_all();

    proxy.faults.hold_down.store(true, Relaxed);
    let lost = leases.acquire("b").await;
    assert!(matches!(lost, Err(OwnershipError::Unavailable(_))), "{lost:?}");
    proxy.clear();
    proxy.cut_all();
    let taken = leases.acquire("b").await.unwrap();
    let key = agent(reqwest::Method::GET, &format!("/v1/kv/{service}/b")).await;
    assert_eq!(key[0]["LockIndex"].as_u64(), Some(taken), "the lock the lost answer took is this node's");
    assert_eq!(leases.acquire("a").await, Ok(held), "a second acquire of a held key");

    proxy.faults.delay_ms.store(300, Relaxed);
    assert_eq!(leases.renew().await, Ok(vec![]));
    leases.acquire("c").await.unwrap();
    proxy.clear();
    leases.close().await;
}

/// A release held on the wire past its caller's timeout, which the holder then follows with an acquire of the same
/// key: neither the late release nor the renewal that looks at the key it left locked may unlock the key taken again,
/// or another node takes it while the holder runs it.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_release_that_arrives_after_its_key_was_taken_again_leaves_the_key_held() {
    let (direct, proxy) = (config(), Proxy::start().await);
    let one = ConsulLeases::new(through(&proxy, &direct.service), "one").unwrap();
    let two = ConsulLeases::new(direct, "two").unwrap();
    let epoch = one.acquire("k").await.unwrap();
    proxy.faults.hold_up_below.store(u64::MAX, Relaxed);
    let released = one.release("k", epoch).await;
    assert!(released.is_err() && proxy.faults.held.load(Relaxed) > 0, "the release was not held: {released:?}");
    proxy.hold_open_connections();
    let again = one.acquire("k").await;
    assert!(again.is_ok(), "{again:?}");
    proxy.clear();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // The renewal looks at the key the release left locked, and finds it held again.
    assert_eq!(one.renew().await, Ok(vec![]));
    assert_eq!(two.acquire("k").await, Err(OwnershipError::Held(None)), "a late release unlocked a key held again");
    one.close().await;
    two.close().await;
}

/// A release lost on the wire, and a release after an acquire of the same key whose answer was lost, leave nothing
/// locked under the renewals of the keys still held: the release or the next renewals unlock it, and another node
/// takes it, while the key still held stays held.
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn a_lost_release_or_acquire_leaves_no_key_locked_under_the_renewals() {
    let (direct, proxy) = (config(), Proxy::start().await);
    let one = ConsulLeases::new(through(&proxy, &direct.service), "one").unwrap();
    let two = ConsulLeases::new(direct, "two").unwrap();
    one.acquire("held").await.unwrap();

    let epoch = one.acquire("lost release").await.unwrap();
    proxy.faults.refuse.store(true, Relaxed);
    proxy.cut_all();
    let released = one.release("lost release", epoch).await;
    assert!(matches!(released, Err(OwnershipError::Unavailable(_))), "{released:?}");
    proxy.clear();

    let epoch = one.acquire("lost acquire").await.unwrap();
    proxy.faults.hold_down.store(true, Relaxed);
    let again = one.acquire("lost acquire").await;
    assert!(matches!(again, Err(OwnershipError::Unavailable(_))), "{again:?}");
    proxy.clear();
    proxy.cut_all();
    one.release("lost acquire", epoch).await.unwrap();

    let started = std::time::Instant::now();
    for key in ["lost release", "lost acquire"] {
        while let Err(e) = two.acquire(key).await {
            assert_eq!(e, OwnershipError::Held(None));
            assert!(started.elapsed() < Duration::from_secs(25), "{key} stayed locked under the renewals");
            assert_eq!(one.renew().await, Ok(vec![]));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    assert_eq!(two.acquire("held").await, Err(OwnershipError::Held(None)));
    one.close().await;
    two.close().await;
}
