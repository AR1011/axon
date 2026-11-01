#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axon_cluster::*;
use axon_net::rustls::pki_types::PrivatePkcs8KeyDer;
use axon_net::rustls::{ClientConfig, RootCertStore, ServerConfig};
use axon_net::{AcceptLimits, Config, Listener, SharedSecret};
use axon_testkit::eventually_within;

fn transport_with(id: &str, secret: &str) -> Config {
    let mut cfg = Config::new(id, SharedSecret::new(secret).unwrap());
    cfg.allow_plaintext = true;
    cfg
}

fn transport(id: &str) -> Config {
    transport_with(id, "a shared secret of the cluster")
}

fn record(transport: &Config, port: u16) -> NodeRecord {
    NodeRecord {
        id: transport.identity.node_id.clone(),
        incarnation: transport.identity.incarnation,
        address: SocketAddr::from(([127, 0, 0, 1], port)),
        metadata: Default::default(),
        version: 0,
    }
}

/// Retries: a port that a dropped server held is free a moment later.
async fn bind(cfg: SeedServerConfig) -> SeedServer {
    for _ in 0..100 {
        match SeedServer::bind(cfg.clone()).await {
            Ok(server) => return server,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("{} stayed taken", cfg.addr);
}

async fn server_at(id: &str, addr: SocketAddr, tls: Option<Tls>) -> SeedServer {
    bind(SeedServerConfig { tls, ..SeedServerConfig::new(addr, &transport(id)) }).await
}

async fn server() -> SeedServer {
    server_at("seed", "127.0.0.1:0".parse().unwrap(), None).await
}

async fn node(transport: &Config, seeds: &[&SeedServer]) -> SeedDiscovery {
    let seeds = seeds.iter().map(|s| s.local_addr().to_string());
    SeedDiscovery::start(SeedConfig::new(seeds, transport)).await.unwrap()
}

async fn ids(discovery: &SeedDiscovery) -> Vec<String> {
    discovery.snapshot().await.map_or(Vec::new(), |s| s.nodes.into_iter().map(|n| n.id).collect())
}

async fn sees(discovery: &SeedDiscovery, what: &str, within: Duration, expected: &[&str]) -> Duration {
    eventually_within(what, within, async || ids(discovery).await == expected).await
}

const WARM: Duration = Duration::from_secs(8);

#[tokio::test(flavor = "multi_thread")]
async fn nodes_join_change_leave_crash_and_restart_through_one_seed() {
    let seed = server().await;
    let (ta, tb, tc) = (transport("a"), transport("b"), transport("c"));
    let (a, b, c) = (node(&ta, &[&seed]).await, node(&tb, &[&seed]).await, node(&tc, &[&seed]).await);

    // A cluster starts before its seeds are warm: the registration is taken, and nothing is visible yet.
    let started = Instant::now();
    a.register(&record(&ta, 1)).await.unwrap();
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert!(matches!(a.snapshot().await, Err(DiscoveryError::Unavailable(_))));
    assert!(matches!(a.heartbeat("a", ta.identity.incarnation).await, Err(DiscoveryError::Unavailable(_))));
    assert!(seed.stats().warm_up_left > Duration::ZERO);
    b.register(&record(&tb, 2)).await.unwrap();

    sees(&a, "a sees a and b", WARM, &["a", "b"]).await;
    sees(&c, "an observer sees them too", WARM, &["a", "b"]).await;
    a.heartbeat("a", ta.identity.incarnation).await.unwrap();
    assert!(
        a.stats().live
            && a.stats().seeds[0].state == "synced"
            && a.stats().seeds[0].identity.as_deref() == Some("seed")
    );
    assert_eq!((seed.stats().sessions, seed.stats().registered, seed.stats().observers), (3, 2, 1));

    // Once warm, a registration answers when the seed lists it, and the next snapshot shows it.
    let started = Instant::now();
    c.register(&record(&tc, 3)).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(ids(&c).await, ["a", "b", "c"]);

    // A watch answers a change, never an unchanged view.
    sees(&a, "a sees c", Duration::from_secs(2), &["a", "b", "c"]).await;
    let revision = a.snapshot().await.unwrap().revision;
    let watching = tokio::spawn({
        let a = a.clone();
        async move { a.watch(revision).unwrap().await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!watching.is_finished());
    let mut changed = record(&tb, 2);
    changed.metadata.insert("zone".into(), "east".into());
    b.register(&changed).await.unwrap();
    let watched = watching.await.unwrap();
    assert!(watched.revision != revision);
    assert_eq!(watched.nodes[1].label("zone"), Some("east"));
    assert_eq!(b.snapshot().await.unwrap().nodes[1].label("zone"), Some("east"));

    // Only a node's own, valid record is taken.
    assert!(matches!(b.register(&record(&ta, 9)).await, Err(DiscoveryError::Invalid(_))));
    assert!(matches!(b.register(&record(&tb, 0)).await, Err(DiscoveryError::Invalid(_))));
    assert!(matches!(b.unregister("a", ta.identity.incarnation).await, Err(DiscoveryError::Invalid(_))));

    // A leave is seen at once, and the node that left stays on as an observer and may come back.
    b.unregister("b", tb.identity.incarnation).await.unwrap();
    sees(&a, "b left", Duration::from_secs(2), &["a", "c"]).await;
    assert_eq!(ids(&b).await, ["a", "c"]);
    b.register(&changed).await.unwrap();
    sees(&a, "b is back", Duration::from_secs(2), &["a", "b", "c"]).await;

    // A crash is a closed session: the record outlives it by the grace, and no longer.
    let old = transport("c");
    drop(c);
    let took = sees(&a, "c crashed", Duration::from_secs(4), &["a", "b"]).await;
    assert!(took >= Duration::from_millis(1500), "removed after {took:?}");

    // A restart under the same id replaces the old boot, and an older boot is refused.
    let tc = transport("c");
    let c = node(&tc, &[&seed]).await;
    c.register(&record(&tc, 4)).await.unwrap();
    eventually_within("the new boot of c is listed", Duration::from_secs(2), async || {
        a.snapshot()
            .await
            .unwrap()
            .nodes
            .iter()
            .any(|n| n.incarnation == tc.identity.incarnation && n.address.port() == 4)
    })
    .await;
    let stale = node(&old, &[&seed]).await;
    assert_eq!(stale.register(&record(&old, 5)).await, Err(DiscoveryError::Stale));
    assert_eq!(stale.heartbeat("c", old.identity.incarnation).await, Err(DiscoveryError::Stale));
    assert!(seed.stats().refusals[0] >= 1 && stale.stats().refusals >= 1);
    assert_eq!(a.snapshot().await.unwrap().nodes[2].incarnation, tc.identity.incarnation);

    // A node that leaves and exits at once is gone at once, not after the grace of a crash.
    b.unregister("b", tb.identity.incarnation).await.unwrap();
    drop(b);
    sees(&a, "b left and exited", Duration::from_secs(1), &["a", "c"]).await;

    // Dropping the server closes every session: the nodes freeze on what they had.
    drop(seed);
    eventually_within("a froze", Duration::from_secs(2), async || a.snapshot().await.is_err()).await;
    assert_eq!(a.stats().members, 2);
    let started = Instant::now();
    assert!(matches!(a.watch(0).unwrap().await, Err(DiscoveryError::Unavailable(_))));
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[tokio::test(flavor = "multi_thread")]
async fn one_seed_of_three_is_not_missed_and_two_freeze_the_nodes_until_they_return() {
    let any = "127.0.0.1:0".parse().unwrap();
    let (s0, s1, s2) =
        (server_at("s0", any, None).await, server_at("s1", any, None).await, server_at("s2", any, None).await);
    let addrs = [s0.local_addr(), s1.local_addr()];
    let (ta, tb) = (transport("a"), transport("b"));
    let a = node(&ta, &[&s0, &s1, &s2]).await;
    let b = node(&tb, &[&s0, &s1, &s2]).await;
    a.register(&record(&ta, 1)).await.unwrap();
    b.register(&record(&tb, 2)).await.unwrap();
    sees(&a, "a sees both", WARM, &["a", "b"]).await;
    sees(&b, "b sees both", WARM, &["a", "b"]).await;
    let revision = a.snapshot().await.unwrap().revision;

    drop(s0);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(a.snapshot().await.unwrap().revision, revision);
    a.heartbeat("a", ta.identity.incarnation).await.unwrap();
    assert_eq!(a.stats().seeds.iter().filter(|s| s.state == "synced").count(), 2);

    drop(s1);
    eventually_within("a froze", Duration::from_secs(2), async || {
        a.heartbeat("a", ta.identity.incarnation).await.is_err()
    })
    .await;
    assert!(a.snapshot().await.is_err() && !a.stats().live);
    // An unchanged record is taken as a new one would be, to be sent when the seeds come back.
    assert_eq!(a.register(&record(&ta, 1)).await, Ok(()));

    // The seeds come back empty; the nodes find them, register again by themselves, and thaw.
    let s0 = server_at("s0", addrs[0], None).await;
    let _s1 = server_at("s1", addrs[1], None).await;
    sees(&a, "a thawed", WARM, &["a", "b"]).await;
    sees(&b, "b thawed", WARM, &["a", "b"]).await;
    assert_eq!(s0.stats().registered, 2);

    // The seed list is replaced on a running node; a bad list is refused and changes nothing.
    assert!(a.set_seeds(Vec::<String>::new()).is_err());
    assert!(a.set_seeds(["no port"]).is_err());
    let (from_a_plain_thread, list) = (a.clone(), [addrs[0].to_string(), addrs[1].to_string(), "127.0.0.1:1".into()]);
    std::thread::spawn(move || from_a_plain_thread.set_seeds(list)).join().unwrap().unwrap();
    a.set_seeds([addrs[0].to_string(), addrs[1].to_string()]).unwrap();
    eventually_within("the dropped seed is let go", Duration::from_secs(2), async || a.stats().seeds.len() == 2).await;
    assert_eq!(ids(&a).await, ["a", "b"]);
    drop(s2);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(ids(&a).await, ["a", "b"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_credential_and_a_cluster_port_never_attach() {
    let seed = server().await;
    let intruder = node(&transport_with("x", "another secret, long enough"), &[&seed]).await;
    eventually_within("the handshake is refused", Duration::from_secs(5), async || {
        seed.stats().listener.handshake_failures[0] >= 1 && intruder.stats().seeds[0].last_error.is_some()
    })
    .await;
    assert_eq!((seed.stats().sessions, intruder.stats().seeds[0].state), (0, "connecting"));

    // A named seed must answer as that seed, and two entries that reach one seed count once.
    let addr = seed.local_addr();
    let seeds = [format!("other@{addr}"), format!("{addr}"), format!("localhost:{}", addr.port())];
    let misled = SeedDiscovery::start(SeedConfig::new(seeds, &transport("m"))).await.unwrap();
    eventually_within("the wrong seed is turned away", Duration::from_secs(5), async || {
        let stats = misled.stats();
        stats.seeds.iter().any(|s| s.last_error.as_deref() == Some("answered as seed")) && stats.duplicates == 1
    })
    .await;

    // A cluster port is in another namespace, in both directions.
    let any = "127.0.0.1:0".parse().unwrap();
    let mut cluster_port = Listener::bind(any, None, transport("n"), AcceptLimits::default()).await.unwrap();
    let addr = cluster_port.local_addr().to_string();
    let _lost = SeedDiscovery::start(SeedConfig::new([addr], &transport("y"))).await.unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(1), cluster_port.accept()).await;
    assert!(refused.is_err(), "a seed session formed on a cluster port");
    assert!(cluster_port.stats().handshake_failures.iter().sum::<u64>() >= 1);
    assert!(axon_net::connect(seed.local_addr(), axon_net::Lane::Control, None, &transport("z")).await.is_err());

    assert!(SeedDiscovery::start(SeedConfig::new(Vec::<String>::new(), &transport("y"))).await.is_err());
    let mut secure = transport("y");
    secure.allow_plaintext = false;
    assert!(SeedDiscovery::start(SeedConfig::new(["127.0.0.1:1"], &secure)).await.is_err());
}

fn tls(names: &[&str]) -> Tls {
    let made = rcgen::generate_simple_self_signed(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
    let (cert, key) = (made.cert.der().clone(), PrivatePkcs8KeyDer::from(made.key_pair.serialize_der()));
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    Tls {
        server: Arc::new(
            ServerConfig::builder().with_no_client_auth().with_single_cert(vec![cert], key.into()).unwrap(),
        ),
        client: Arc::new(ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()),
    }
}

/// Node `id`, registered through the seed at `entry` over TLS only.
async fn tls_node(id: &str, entry: String, tls: &Tls) -> SeedDiscovery {
    let mut transport = transport(id);
    transport.allow_plaintext = false;
    let cfg = SeedConfig { tls: Some(tls.clone()), ..SeedConfig::new([entry], &transport) };
    let discovery = SeedDiscovery::start(cfg).await.unwrap();
    discovery.register(&record(&transport, 1)).await.unwrap();
    discovery
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_checks_the_host_name_or_the_named_identity() {
    let tls = tls(&["localhost", "seed"]);
    let seed = server_at("seed", "127.0.0.1:0".parse().unwrap(), Some(tls.clone())).await;
    let port = seed.local_addr().port();
    let (by_host, by_id, unnamed, misnamed, odd_id) = tokio::join!(
        tls_node("a", format!("localhost:{port}"), &tls),
        tls_node("b", format!("seed@127.0.0.1:{port}"), &tls),
        tls_node("c", format!("127.0.0.1:{port}"), &tls),
        tls_node("d", format!("other@127.0.0.1:{port}"), &tls),
        tls_node("e", format!("not a name@localhost:{port}"), &tls),
    );
    sees(&by_host, "by host name", WARM, &["a", "b"]).await;
    sees(&by_id, "by identity", WARM, &["a", "b"]).await;
    assert!(by_id.stats().seeds[0].session.is_some());
    // An id that cannot be a server name falls back to the host, as on cluster links; the identity still counts.
    assert_eq!(odd_id.stats().seeds[0].last_error.as_deref(), Some("answered as seed"));
    for refused in [unnamed, misnamed] {
        let stats = refused.stats();
        let error = stats.seeds[0].last_error.clone().unwrap_or_default();
        assert!(!stats.live && error.to_lowercase().contains("certificate"), "{stats:?}");
    }
}

/// A seed and a node swap certificates while they run: a session that was up keeps going, new ones take the new ones.
#[tokio::test(flavor = "multi_thread")]
async fn a_seed_and_its_nodes_take_new_certificates_without_a_restart() {
    let (old, new) = (tls(&["seed"]), tls(&["seed"]));
    let seed = server_at("seed", "127.0.0.1:0".parse().unwrap(), Some(old.clone())).await;
    let entry = format!("seed@{}", seed.local_addr());
    let a = tls_node("a", entry.clone(), &old).await;
    sees(&a, "a joins", WARM, &["a"]).await;

    seed.set_tls(new.clone()).unwrap();
    let (b, c) = (tls_node("b", entry.clone(), &old).await, tls_node("c", entry.clone(), &new).await);
    sees(&a, "over the session that was up", WARM, &["a", "c"]).await;
    eventually_within("a node that trusts only the old certificate is refused", WARM, async || {
        b.stats().seeds[0].last_error.as_deref().is_some_and(|e| e.to_lowercase().contains("certificate"))
    })
    .await;
    b.set_tls(new).unwrap();
    sees(&b, "once it trusts the new one", WARM, &["a", "b", "c"]).await;
    sees(&c, "c", WARM, &["a", "b", "c"]).await;
}

/// A cluster must be able to start before its seeds, so a first registration before they are warm is taken. The same
/// registration made again (the heartbeat's, racing the one a kind registration makes) is taken the same way.
#[tokio::test(flavor = "multi_thread")]
async fn registering_the_same_record_again_is_taken_like_the_first() {
    let seed = server().await;
    let ta = transport("a");
    let a = node(&ta, &[&seed]).await;
    assert_eq!(a.register(&record(&ta, 1)).await, Ok(()));
    assert_eq!(a.register(&record(&ta, 1)).await, Ok(()), "the seed is still warming up, as it was a moment ago");
    eventually_within("the seed warmed up", WARM, async || a.stats().live).await;
    // Two calls racing to send one changed record, as two kinds registered at once do.
    let changed = record(&ta, 2);
    let (first, second) = tokio::join!(a.register(&changed), a.register(&changed));
    assert_eq!((first, second), (Ok(()), Ok(())));
}

/// A seed at `timings`, on a free port.
async fn server_with(id: &str, timings: SeedTimings) -> SeedServer {
    bind(SeedServerConfig { timings, ..SeedServerConfig::new("127.0.0.1:0".parse().unwrap(), &transport(id)) }).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_does_not_count_a_seed_with_other_timings() {
    // Other timings on s3, the subject of this test.
    let odd = SeedTimings { idle_hold: Duration::from_secs(3), ..SeedTimings::default() };
    let (s1, s2, s3) = (
        server_with("s1", SeedTimings::default()).await,
        server_with("s2", SeedTimings::default()).await,
        server_with("s3", odd).await,
    );
    let ta = transport("a");
    let a = node(&ta, &[&s1, &s2, &s3]).await;
    let events = Arc::new(axon::EventLog::new(axon::Clock::system()));
    a.attach(&events);
    a.register(&record(&ta, 1)).await.unwrap();
    sees(&a, "a is live through s1 and s2", WARM, &["a"]).await;
    let entry = s3.local_addr().to_string();
    let state = || a.stats().seeds.into_iter().find(|s| s.seed == entry).map(|s| s.state);
    eventually_within("s3 shows its other timings", WARM, async || state() == Some("timings")).await;
    // It registered during the warm-up, and left once it saw them.
    eventually_within("s3 lists no node", Duration::from_secs(2), async || s3.stats().registered == 0).await;
    let told: Vec<_> = events.since(0, 16).into_iter().filter(|e| e.kind == "seed_timings").collect();
    assert_eq!(told.len(), 1, "{told:?}");
    assert!(told[0].subject == entry && told[0].detail.contains("idle_hold: 3s"), "{told:?}");

    drop(s2);
    eventually_within("one seed of three that agree is too few", Duration::from_secs(2), async || !a.stats().live)
        .await;
    assert!(a.snapshot().await.is_err());
    assert_eq!(state(), Some("timings"));
}

#[tokio::test(flavor = "multi_thread")]
async fn seed_timings_that_cannot_work_are_refused() {
    let s = Duration::from_secs;
    let bad = [
        // Past each check, the subject of this test.
        SeedTimings { idle: s(2), ..SeedTimings::default() },
        SeedTimings { idle_hold: s(3), closed_grace: s(4), ..SeedTimings::default() },
        SeedTimings { idle_hold: s(2), closed_grace: s(1), ..SeedTimings::default() },
        SeedTimings { warm_up: Duration::from_millis(2999), ..SeedTimings::default() },
        SeedTimings { closed_grace: Duration::ZERO, ..SeedTimings::default() },
        SeedTimings { warm_up: s(3601), ..SeedTimings::default() },
        SeedTimings { idle: Duration::MAX, idle_hold: Duration::MAX, ..SeedTimings::default() },
    ];
    let any = "127.0.0.1:0".parse().unwrap();
    for timings in bad {
        let server = SeedServer::bind(SeedServerConfig { timings, ..SeedServerConfig::new(any, &transport("s")) });
        assert!(matches!(server.await, Err(axon_net::Error::Config(_))), "{timings:?}");
        let node = SeedDiscovery::start(SeedConfig { timings, ..SeedConfig::new(["127.0.0.1:1"], &transport("a")) });
        assert!(matches!(node.await, Err(DiscoveryError::Invalid(_))), "{timings:?}");
    }
    // At the edge of each check.
    let timings = SeedTimings { idle: s(3), idle_hold: s(3), closed_grace: s(3), warm_up: s(3) };
    server_with("s", timings).await;
    let node = SeedConfig { timings, ..SeedConfig::new(["127.0.0.1:1"], &transport("a")) };
    assert_eq!(SeedDiscovery::start(node).await.unwrap().drops_after(), Some(Duration::from_secs(1)));
    // A node's redial is its 2 s backoff cap and its connect: a warm-up that does not hold one is refused.
    let mut node = SeedConfig::new(["127.0.0.1:1"], &transport("a"));
    // The connect under test, past the default warm_up of 5 s with the backoff.
    node.transport.connect_timeout = Duration::from_millis(3001);
    assert!(matches!(SeedDiscovery::start(node.clone()).await, Err(DiscoveryError::Invalid(_))));
    // At the edge.
    node.transport.connect_timeout = s(3);
    SeedDiscovery::start(node).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_transport_put_in_place_of_the_seed_one_is_refused_with_the_fix() {
    let node = transport("a");
    assert_eq!(node.connect_timeout, Duration::from_secs(5), "Config::new's");
    let replaced = SeedConfig { transport: node.clone(), ..SeedConfig::new(["127.0.0.1:1"], &node) };
    let Err(DiscoveryError::Invalid(why)) = SeedDiscovery::start(replaced).await else { panic!("started") };
    for named in ["SeedConfig::transport.connect_timeout", "warm_up", "SeedConfig::new"] {
        assert!(why.contains(named), "{named} in {why:?}");
    }
    SeedDiscovery::start(SeedConfig::new(["127.0.0.1:1"], &node)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_without_an_authority_that_would_isolate_after_the_seeds_drop_it_is_refused() {
    let seed = server().await;
    let leases = MemoryLeases::new();
    let start = async |id: &str, (interval, grace): (u64, u64), leased: bool| {
        let transport = transport(id);
        let discovery = node(&transport, &[&seed]).await;
        assert_eq!(discovery.drops_after(), Some(Duration::from_secs(3)));
        let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery);
        // The subject of this test.
        cfg.heartbeat_interval = Duration::from_millis(interval);
        cfg.heartbeat_grace = Duration::from_millis(grace);
        if leased {
            cfg.ownership = Some(Arc::new(leases.authority()));
        }
        let system = axon::ActorSystem::builder().build().unwrap();
        let started = Cluster::start(&system, cfg).await.map(drop);
        system.shutdown().await.unwrap();
        started
    };
    let refused = start("a", (1000, 5000), false).await;
    assert!(matches!(&refused, Err(ClusterError::Config(why)) if why.contains("drops_after")), "{refused:?}");
    start("b", (500, 1000), false).await.unwrap();
    start("c", (1000, 5000), true).await.unwrap();
}
