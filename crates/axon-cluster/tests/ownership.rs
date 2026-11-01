//! What every ownership authority promises, run against each one, then what Paxos leases add.
#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::*;
use axon_testkit::eventually_within;
use axon_testkit::world::Disk;

mod common;

const KEY: &str = "pricer/EUR USD";

/// Keeps asking while the authority is not ready (acceptors warming) or a lock delay runs.
async fn take(authority: &dyn OwnershipAuthority, key: &str) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match authority.acquire(key).await {
            Ok(epoch) => return epoch,
            Err(error) if Instant::now() > deadline => panic!("{key} never granted: {error:?}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Refused because another process holds the key, whether or not the authority names it.
fn held(result: Result<u64, OwnershipError>) -> bool {
    matches!(result, Err(OwnershipError::Held(_)))
}

async fn conformance(one: &dyn OwnershipAuthority, two: &dyn OwnershipAuthority) {
    let first = take(one, KEY).await;
    assert!(held(two.acquire(KEY).await));
    assert_eq!(one.renew().await, Ok(vec![]));
    assert!(held(two.acquire(KEY).await));

    // Keys are independent.
    let other = take(two, "pricer/GBP USD").await;
    assert_eq!(one.renew().await, Ok(vec![]));
    assert_eq!(two.renew().await, Ok(vec![]));

    one.release(KEY, first).await.unwrap();
    let second = take(two, KEY).await;
    assert!(second > first, "epochs grow: {first} then {second}");

    // A release with an old epoch leaves the holder alone.
    one.release(KEY, first).await.unwrap();
    assert!(held(one.acquire(KEY).await));

    two.release(KEY, second).await.unwrap();
    two.release("pricer/GBP USD", other).await.unwrap();
    assert!(take(one, KEY).await > second);
}

#[tokio::test]
async fn memory_leases_keep_the_promises() {
    let leases = MemoryLeases::new();
    conformance(&leases.authority(), &leases.authority()).await;
}

#[cfg(feature = "consul")]
#[tokio::test]
#[ignore = "needs AXON_CONSUL: scripts/check.sh live"]
async fn consul_leases_keep_the_promises() {
    let base = std::env::var("AXON_CONSUL").expect("AXON_CONSUL is set for the live tests: scripts/check.sh live");
    let unique = std::time::SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos();
    let mut cfg = ConsulConfig::new(base, format!("axon-test-{unique}"));
    // Consul holds a released lock for lock_delay: 1 s, not the shipped 5 s, keeps the test short.
    cfg.lock_delay = Duration::from_secs(1);
    let (one, two) = (ConsulLeases::new(cfg.clone(), "one").unwrap(), ConsulLeases::new(cfg, "two").unwrap());
    conformance(&one, &two).await;
}

struct Node {
    system: ActorSystem,
    cluster: Cluster,
    leases: Arc<PaxosLeases>,
    store: Disk,
}

const LEASE: Duration = Duration::from_secs(1);
const ACCEPTORS: [&str; 3] = ["p-0", "p-1", "p-2"];

fn paxos_config(id: &str, discovery: &MemoryDiscovery, store: &Disk) -> (ClusterConfig, Arc<PaxosLeases>) {
    let mut paxos = PaxosConfig::new(ACCEPTORS);
    // A 1 s lease, so a lease runs out within the test's wait.
    paxos.lease = LEASE;
    paxos.store = Some(Arc::new(store.clone()));
    let leases = Arc::new(PaxosLeases::new(paxos).unwrap());
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), common::transport(id), discovery.clone());
    common::short(&mut cfg);
    cfg.ownership = Some(leases.clone());
    (cfg, leases)
}

async fn paxos_node(id: &str, discovery: &MemoryDiscovery) -> Node {
    paxos_node_on(id, discovery, Disk::default()).await
}

/// With the store of an earlier node of this id: the files of a node that restarts.
async fn paxos_node_on(id: &str, discovery: &MemoryDiscovery, store: Disk) -> Node {
    let (cfg, leases) = paxos_config(id, discovery, &store);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    Node { system, cluster, leases, store }
}

/// Actors that outlive the holder's lease could run beside the next holder's.
#[tokio::test(flavor = "multi_thread")]
async fn a_grace_the_lease_does_not_cover_is_refused() {
    // A zero grace would renew on a zero interval.
    for grace in [LEASE, Duration::ZERO] {
        let (mut cfg, _) = paxos_config("p-0", &MemoryDiscovery::new(), &Disk::default());
        // The grace under test.
        cfg.lease_grace = Some(grace);
        let system = ActorSystem::builder().build().unwrap();
        assert!(matches!(Cluster::start(&system, cfg).await, Err(ClusterError::Config(_))), "{grace:?}");
        system.shutdown().await.unwrap();
    }
}

/// The acceptors keep a lease at most 5 min: a holder believing a longer one would run beside the next holder.
#[test]
fn a_lease_longer_than_the_acceptors_keep_is_refused() {
    for (secs, ok) in [(300, true), (301, false)] {
        let mut paxos = PaxosConfig::new(ACCEPTORS);
        // The longest lease the acceptors keep, and one past it, under test.
        paxos.lease = Duration::from_secs(secs);
        match PaxosLeases::new(paxos) {
            Ok(_) => assert!(ok, "{secs} s taken"),
            Err(e) => assert!(!ok && matches!(&e, OwnershipError::Unavailable(w) if w.contains("5 min")), "{e:?}"),
        }
    }
}

/// Without one an acceptor's restart, with the others', would forget which node held which key.
#[tokio::test(flavor = "multi_thread")]
async fn an_acceptor_without_a_store_is_refused() {
    let (mut cfg, _) = paxos_config("p-0", &MemoryDiscovery::new(), &Disk::default());
    cfg.ownership = Some(Arc::new(PaxosLeases::new(PaxosConfig::new(ACCEPTORS)).unwrap()));
    let system = ActorSystem::builder().build().unwrap();
    let started = Cluster::start(&system, cfg).await;
    assert!(matches!(started, Err(ClusterError::Config(e)) if e.contains("store")), "{:?}", started.err());
    system.shutdown().await.unwrap();
    // One not on the list needs none.
    let (mut cfg, _) = paxos_config("holder", &MemoryDiscovery::new(), &Disk::default());
    cfg.ownership = Some(Arc::new(PaxosLeases::new(PaxosConfig::new(ACCEPTORS)).unwrap()));
    let system = ActorSystem::builder().build().unwrap();
    Cluster::start(&system, cfg).await.unwrap();
    system.shutdown().await.unwrap();
}

fn stat(leases: &PaxosLeases, name: &str) -> String {
    let info = leases.describe();
    info.stats.into_iter().find(|(n, _)| n == name).map(|(_, v)| v).unwrap_or_else(|| panic!("no {name}"))
}

/// An acceptor whose log cannot be written, or whose store panics mid-write, still answers, says so, and logs every
/// lease it holds once a write lands.
#[tokio::test(flavor = "multi_thread")]
async fn an_acceptor_whose_log_fails_answers_and_logs_again_once_it_can() {
    let discovery = MemoryDiscovery::new();
    let node = paxos_node("p-0", &discovery).await;
    // A majority with p-1; p-2 never starts, so every grant is logged at p-0. p-1 holds the keys: p-0's own would be
    // released when it stops.
    let other = paxos_node("p-1", &discovery).await;
    take(&*other.leases, "pricer/first").await;
    assert_eq!(stat(&node.leases, "store failing"), "false");
    let failing = async |want: &str| {
        let what = format!("store failing {want}");
        eventually_within(&what, Duration::from_secs(5), async || stat(&node.leases, "store failing") == want).await;
    };
    // A full disk.
    node.store.set_capacity(Some(0));
    for i in 0..20 {
        take(&*other.leases, &format!("pricer/{i}")).await;
    }
    failing("true").await;
    node.store.set_capacity(None);
    failing("false").await;
    assert_eq!(stat(&node.leases, "stored"), "21");
    node.store.panic_writes(2);
    for i in 20..30 {
        take(&*other.leases, &format!("pricer/{i}")).await;
    }
    let panicked = async || node.store.panics_left() == 0;
    eventually_within("every write that was to panic did", Duration::from_secs(5), panicked).await;
    failing("false").await;
    assert_eq!(stat(&node.leases, "stored"), "31");
    // The rewrite after the failure holds the leases granted while it failed: a restart reads them back.
    node.system.shutdown().await.unwrap();
    let again = paxos_node_on("p-0", &discovery, node.store.clone()).await;
    assert_eq!(stat(&again.leases, "stored"), "31");
    again.system.shutdown().await.unwrap();
    other.system.shutdown().await.unwrap();
}

async fn paxos_cluster(ids: &[&str]) -> (MemoryDiscovery, Vec<Node>) {
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for id in ids {
        nodes.push(paxos_node(id, &discovery).await);
    }
    (discovery, nodes)
}

#[tokio::test(flavor = "multi_thread")]
async fn paxos_leases_keep_the_promises() {
    let (_, nodes) = paxos_cluster(&ACCEPTORS).await;
    conformance(&*nodes[0].leases, &*nodes[1].leases).await;
    let info = nodes[0].cluster.stats().ownership.unwrap();
    assert_eq!(info.kind, "paxos");
    assert!(info.links.iter().all(|l| l.state != "failing"), "{:?}", info.links);
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}

/// Two thousand keys at once share requests to the acceptors, a few hundred keys each, not one request per key.
/// Where a request is not answered within the patience, its keys are refused saying so, to be asked again.
#[tokio::test(flavor = "multi_thread")]
async fn a_burst_of_paxos_acquires_is_granted() {
    let (_, nodes) = paxos_cluster(&ACCEPTORS).await;
    take(&*nodes[0].leases, "warm").await;
    let mut burst = tokio::task::JoinSet::new();
    for i in 0..2000 {
        let leases = nodes[0].leases.clone();
        burst.spawn(async move { (i, leases.acquire(&format!("k{i}")).await) });
    }
    // Two cores shared with six busy processes answer a request of 512 keys in up to 400 ms, past the 167 ms patience
    // of the 1 s lease: the acceptors are alive, so the keys are refused as late, not as voted down, and granted again.
    // A round that also outruns its own deadline says "no majority answered in time: ..."; both end alike.
    let mut again = tokio::task::JoinSet::new();
    for (i, result) in burst.join_all().await {
        if let Err(error) = result {
            let late = matches!(&error, OwnershipError::Unavailable(why) if why.ends_with("acceptors did not answer"));
            assert!(late, "k{i}: {error:?}");
            assert_eq!(ClusterError::Ownership(error).retry(), Retry::Later, "k{i}");
            let leases = nodes[0].leases.clone();
            again.spawn(async move { take(&*leases, &format!("k{i}")).await });
        }
    }
    again.join_all().await;
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_paxos_lease_nobody_renews_runs_out() {
    let (_, nodes) = paxos_cluster(&ACCEPTORS).await;
    let first = take(&*nodes[0].leases, KEY).await;
    let since = Instant::now();
    // Paxos names the holder, so a caller refused by the key's owner can ask it.
    assert_eq!(nodes[1].leases.acquire(KEY).await, Err(OwnershipError::Held(Some("p-0".into()))));
    let second = take(&*nodes[1].leases, KEY).await;
    let waited = since.elapsed();
    assert!(second > first);
    assert!(waited > LEASE / 2 && waited < LEASE * 2, "taken over after {waited:?}");
    assert_eq!(nodes[0].leases.renew().await, Ok(vec![(KEY.into(), OwnershipError::Lost)]));
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn paxos_leases_need_a_majority_of_acceptors() {
    let (_, mut nodes) = paxos_cluster(&ACCEPTORS).await;
    let first = take(&*nodes[0].leases, KEY).await;

    nodes.pop().unwrap().system.shutdown().await.unwrap();
    assert_eq!(nodes[0].leases.renew().await, Ok(vec![]));
    nodes[0].leases.release(KEY, first).await.unwrap();
    let second = take(&*nodes[1].leases, KEY).await;
    assert!(second > first);

    nodes.pop().unwrap().system.shutdown().await.unwrap();
    assert!(matches!(nodes[0].leases.acquire("another").await, Err(OwnershipError::Unavailable(_))));
    nodes.pop().unwrap().system.shutdown().await.unwrap();
}

/// `held_by` takes the union of a majority's answers: a lease one acceptor missed is listed from another, and one
/// acceptor alone is no majority.
#[tokio::test(flavor = "multi_thread")]
async fn paxos_held_by_lists_what_any_acceptor_of_a_majority_accepted() {
    let (discovery, mut nodes) = paxos_cluster(&ACCEPTORS).await;
    take(&*nodes[0].leases, "warm").await;
    nodes.pop().unwrap().system.shutdown().await.unwrap();
    take(&*nodes[0].leases, KEY).await;
    nodes.push(paxos_node("p-2", &discovery).await);
    nodes.remove(1).system.shutdown().await.unwrap();
    // p-2, which never saw the key, answers first, from itself, once it has waited out its restart; p-0 has the key.
    let p2 = &nodes[1].leases;
    let warmed = eventually_within("p-2 warmed up", Duration::from_secs(5), async || !p2.warming()).await;
    let kinds = ["pricer".to_string()];
    let mut held = Ok(vec![]);
    eventually_within("a majority answered", Duration::from_secs(5).saturating_sub(warmed), async || {
        held = p2.held_by(Holder::Node("p-0"), &kinds).expect("Paxos lists a node's keys").await;
        held.is_ok()
    })
    .await;
    assert_eq!(held, Ok(vec![KEY.to_string()]));
    nodes.remove(0).system.shutdown().await.unwrap();
    let alone = nodes[0].leases.held_by(Holder::Node("p-0"), &kinds).expect("Paxos lists a node's keys").await;
    assert!(matches!(alone, Err(OwnershipError::Unavailable(_))), "one of three answered: {alone:?}");
    nodes.pop().unwrap().system.shutdown().await.unwrap();
}

/// Acceptors of the build before serve `held_by` at schema 3, where an empty node is a node named so, which holds
/// nothing: a majority of them is no answer to a question for lapsed leases, not an empty list. The question has a
/// schema of its own, so the lease route's acceptors of both builds still grant to each other.
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_question_to_older_acceptors_is_no_answer() {
    // `PaxosConfig::new(ACCEPTORS)`'s protocol id at `LEASE`, which the held endpoint is served under.
    const PROTOCOL: u64 = 0x6ba0_a1b5_eae8_8d04;
    let discovery = MemoryDiscovery::new();
    let new = paxos_node("p-0", &discovery).await;
    let mut old = Vec::new();
    for id in &ACCEPTORS[1..] {
        let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), common::transport(id), discovery.clone());
        common::short(&mut cfg);
        let system = ActorSystem::builder().build().unwrap();
        let cluster = Cluster::start(&system, cfg).await.unwrap();
        // An empty page.
        cluster
            .serve("axon.lease.held", PROTOCOL, 3, |_, _, reply| {
                drop(reply.ok_or(Rejection::InvalidMessage)?.send(bytes::Bytes::from_static(&[0])));
                Ok(())
            })
            .unwrap();
        old.push((system, cluster));
    }
    eventually_within("p-0 sees the others", Duration::from_secs(5), async || new.cluster.view().members.len() == 3)
        .await;
    eventually_within("p-0 warmed up", Duration::from_secs(5), async || !new.leases.warming()).await;
    // The id is the acceptors' own: p-0 answers a lapsed question (`1`, no page key, no kind) under it at schema 4.
    let probe = old[0].1.request("p-0", "axon.lease.held", PROTOCOL, 4, bytes::Bytes::from_static(&[1, 0, 0])).await;
    assert!(probe.is_ok(), "{probe:?}");
    let asked = new.leases.held_by(Holder::Lapsed, &["pricer".to_string()]).expect("Paxos lists lapsed leases").await;
    assert!(matches!(asked, Err(OwnershipError::Unavailable(_))), "two of three are older: {asked:?}");
    for (system, _) in old {
        system.shutdown().await.unwrap();
    }
    new.system.shutdown().await.unwrap();
}

/// A node configured with other acceptors, or another lease, could win a majority that shares no
/// acceptor with the others', or believe a lease longer than a restarted acceptor waits.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_with_other_acceptors_or_lease_gets_no_votes() {
    let (discovery, nodes) = paxos_cluster(&ACCEPTORS).await;
    take(&*nodes[0].leases, "warm").await;
    let mut other_lease = PaxosConfig::new(ACCEPTORS);
    // A lease the others do not share, under test.
    other_lease.lease = LEASE * 3;
    for (id, mut paxos) in [("stray", PaxosConfig::new(["p-0", "p-1", "stray"])), ("slow", other_lease)] {
        let store = Disk::default();
        let (mut cfg, _) = paxos_config(id, &discovery, &store);
        paxos.store = Some(Arc::new(store));
        let leases = Arc::new(PaxosLeases::new(paxos).unwrap());
        cfg.ownership = Some(leases.clone());
        let system = ActorSystem::builder().build().unwrap();
        let cluster = Cluster::start(&system, cfg).await.unwrap();
        assert!(matches!(leases.acquire(KEY).await, Err(OwnershipError::Unavailable(_))), "{id} won a vote");
        let links = cluster.stats().ownership.unwrap().links;
        let p0 = links.iter().find(|l| l.target == "p-0").unwrap();
        assert!(p0.error.as_deref().is_some_and(|e| e.contains("acceptor list")), "{id}: {p0:?}");
        system.shutdown().await.unwrap();
    }
    take(&*nodes[1].leases, KEY).await;
    assert!(PaxosLeases::new(PaxosConfig::new(["p-0", "p-1", "p-0"])).is_err(), "p-0 would vote twice");
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}

/// A rolling deploy of the acceptors, one at a time and waiting out each one's warm-up, never
/// costs the holder its lease, though each comes back from its log refusing the ballot the holder renewed.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolling_restart_of_the_acceptors_keeps_the_lease() {
    let (discovery, mut nodes) = paxos_cluster(&ACCEPTORS).await;
    let holder = paxos_node("holder", &discovery).await;
    let epoch = take(&*holder.leases, KEY).await;
    let renewing = {
        let leases = holder.leases.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(LEASE / 5).await;
                match leases.renew().await {
                    Ok(failed) if failed.is_empty() => {}
                    failed => return failed,
                }
            }
        })
    };
    for (i, id) in ACCEPTORS.iter().enumerate() {
        let gone = nodes.remove(i);
        gone.system.shutdown().await.unwrap();
        nodes.insert(i, paxos_node_on(id, &discovery, gone.store).await);
        assert!(nodes[i].leases.warming() && nodes[i].cluster.warming(), "{id} forgot its promises, so it waits");
        let warmed =
            eventually_within(&format!("{id} warmed up"), LEASE * 3, async || !nodes[i].cluster.warming()).await;
        // The heartbeat after each change puts it on the node's timeline.
        let told = || nodes[i].cluster.events().since(0, usize::MAX).into_iter().map(|e| e.kind);
        let what = format!("{id} told it is warm");
        eventually_within(&what, LEASE * 4 - warmed, async || told().any(|k| k == "acceptor_warm")).await;
        let changes: Vec<_> = told().filter(|k| k.starts_with("acceptor_")).collect();
        assert_eq!(changes, ["acceptor_warming", "acceptor_warm"], "{id}");
    }
    assert!(!renewing.is_finished(), "renewal failed: {:?}", renewing.await);
    assert!(!holder.leases.warming() && !holder.cluster.warming(), "not an acceptor");
    assert_eq!(holder.leases.acquire(KEY).await, Ok(epoch));
    assert_eq!(nodes[0].leases.acquire(KEY).await, Err(OwnershipError::Held(Some("holder".into()))));
    renewing.abort();
    holder.system.shutdown().await.unwrap();
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}
