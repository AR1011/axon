use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn strict_ownership_fences_and_stops_on_lost_lease() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let authority = Arc::new(leases.authority());
    let held = authority.clone();
    let a = node_with("a", discovery.clone(), move |cfg| {
        cfg.ownership = Some(held);
        // Renewed every 30 ms, and a call gives up in 700 ms, so a lost lease shows within a second.
        cfg.lease_renew_interval = Some(Duration::from_millis(30));
        cfg.operation_timeout = Duration::from_millis(700);
    })
    .await;
    counter_kind(&a).await;
    let anywhere = Placement::default();

    let first = a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    assert_eq!(first.epoch(), Some(1));
    assert_eq!(first.request(Op::Tag).await.unwrap(), "a:1");

    // Someone else holds another key: the spawn waits for the lease up to `operation_timeout`, then is refused.
    let rival = leases.authority();
    rival.acquire("counter/taken").await.unwrap();
    let refused = a.cluster.get_or_spawn::<CounterApi>("counter", "taken", seed(0), &anywhere).await;
    assert!(matches!(refused, Err(ClusterError::Spawn(SpawnRefusal::Held, _))), "{refused:?}");
    assert_eq!(a.cluster.stats().spawned, 1);
    // A holder that lets go in time (a node that died and whose lock delay passes) costs only the wait.
    let late = rival.acquire("counter/late").await.unwrap();
    let (rival, released) = (Arc::new(rival), tokio::time::Instant::now());
    let holder = rival.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        holder.release("counter/late", late).await.unwrap();
    });
    let spawned = a.cluster.get_or_spawn::<CounterApi>("counter", "late", seed(0), &anywhere).await.unwrap();
    assert!(released.elapsed() >= Duration::from_millis(300) && spawned.epoch() == Some(late + 1), "{spawned:?}");

    leases.expire(&authority);
    eventually("actors stop when the leases are lost", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!(a.cluster.stats().leases_lost, 2, "one per key");
    let second = a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    assert_eq!(second.epoch(), Some(2));
    let event = |kind, key: &str, epoch| (kind, format!("counter/{key}"), format!("epoch {epoch}"));
    let told = told(&a.cluster);
    assert_eq!(told.len(), 5, "{told:?}");
    assert_eq!(told[..2], [event("acquired", "k", 1), event("acquired", "late", late + 1)]);
    let mut lost = told[2..4].to_vec();
    lost.sort();
    assert_eq!(lost, [event("lease_lost", "k", 1), event("lease_lost", "late", late + 1)]);
    assert_eq!(told[4..], [event("acquired", "k", 2)]);
}

/// Answers a key it still holds from that hold, without asking again, as `PaxosLeases` does.
struct Caching<A> {
    inner: A,
    held: parking_lot::Mutex<HashMap<String, u64>>,
}

impl<A: OwnershipAuthority> OwnershipAuthority for Caching<A> {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        let (key, held) = (key.to_owned(), self.held.lock().get(key).copied());
        Box::pin(async move {
            if let Some(epoch) = held {
                return Ok(epoch);
            }
            let epoch = self.inner.acquire(&key).await?;
            self.held.lock().insert(key, epoch);
            Ok(epoch)
        })
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        self.inner.renew()
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        self.held.lock().retain(|held, e| held != key || *e != epoch);
        self.inner.release(key, epoch)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn renewals_that_fail_or_panic_stop_strictly_owned_actors_only_once_the_grace_is_spent() {
    for when_down in [Down::Fails, Down::Panics] {
        renewals_that_stop(when_down).await;
    }
}

async fn renewals_that_stop(when_down: Down) {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    let down = Arc::new(AtomicBool::new(false));
    let leases = MemoryLeases::new().authority();
    let authority = Arc::new(Unsteady { leases, down: down.clone(), when_down, failing: Default::default() });
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| {
        cfg.ownership = Some(authority);
        // Renewed every 25 ms with a 400 ms grace, so an outage of the authority shows within a second.
        cfg.lease_renew_interval = Some(Duration::from_millis(25));
        cfg.lease_grace = Some(Duration::from_millis(400));
    })
    .await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    let spawn = || a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere);
    spawn().await.unwrap();

    // A blip shorter than the grace costs nothing.
    down.store(true, Relaxed);
    tokio::time::sleep(Duration::from_millis(120)).await;
    down.store(false, Relaxed);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!((a.cluster.stats().spawned, a.cluster.stats().leases_lost), (1, 0));

    down.store(true, Relaxed);
    eventually("the actor stops once the grace is spent", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!(a.cluster.stats().leases_lost, 1, "{when_down:?}");
    down.store(false, Relaxed);
    eventually("the key is hosted again", || async { spawn().await.is_ok() }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_key_whose_renewal_fails_stops_alone() {
    let failing = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let leases = MemoryLeases::new().authority();
    let authority =
        Arc::new(Unsteady { leases, down: Default::default(), when_down: Down::Fails, failing: failing.clone() });
    let grace = Duration::from_millis(400);
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| {
        cfg.ownership = Some(authority);
        // Renewed every 25 ms, so the failures the test injects come at once, and the grace it measures against.
        cfg.lease_renew_interval = Some(Duration::from_millis(25));
        cfg.lease_grace = Some(grace);
    })
    .await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    let spawn = |key| a.cluster.get_or_spawn::<CounterApi>("counter", key, seed(0), &anywhere);
    spawn("taken").await.unwrap();
    spawn("cut off").await.unwrap();
    let fine = spawn("fine").await.unwrap();
    fine.send(Op::Add(5)).await.unwrap();

    // Held elsewhere now: its actor stops at once, well inside the grace.
    failing.lock().push(("counter/taken".into(), OwnershipError::Lost));
    let since = std::time::Instant::now();
    eventually("the key taken over stops", || async { a.cluster.stats().spawned == 2 }).await;
    assert!(since.elapsed() < grace / 2, "stopped after {:?}", since.elapsed());
    assert_eq!(a.cluster.stats().leases_lost, 1);

    // Not extended, so its last extension still holds it until the grace is spent.
    failing.lock().push(("counter/cut off".into(), OwnershipError::Unavailable("no majority".into())));
    let since = std::time::Instant::now();
    eventually("the key cut off stops", || async { a.cluster.stats().spawned == 1 }).await;
    let ran = since.elapsed();
    assert!(ran > grace / 2 && ran < grace + Duration::from_millis(300), "stopped after {ran:?}");
    assert_eq!(a.cluster.stats().leases_lost, 2);

    tokio::time::sleep(grace).await;
    assert_eq!(number(fine.request(Op::Get).await.unwrap()), 5, "the key renewed all along runs on");
    assert_eq!((a.cluster.stats().spawned, a.cluster.stats().leases_lost), (1, 2));
}

struct SlowAcquire(MemoryAuthority);

impl OwnershipAuthority for SlowAcquire {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        let key = key.to_owned();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.0.acquire(&key).await
        })
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        self.0.renew()
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        self.0.release(key, epoch)
    }
}

/// A caller that gives up on a start does not stop it half way: the reservation, the lease and the slot all
/// belong to the actor it goes on to start.
#[tokio::test(flavor = "multi_thread")]
async fn a_start_whose_caller_gave_up_still_finishes() {
    let a = node_with("a", MemoryDiscovery::new(), |cfg| {
        cfg.ownership = Some(Arc::new(SlowAcquire(MemoryLeases::new().authority())));
    })
    .await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    let start = a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(3), &anywhere);
    assert!(tokio::time::timeout(Duration::from_millis(50), start).await.is_err());
    let started = a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(3), &anywhere).await.unwrap();
    assert_eq!(number(started.request(Op::Get).await.unwrap()), 3);
    assert_eq!(a.cluster.stats().spawned, 1, "one actor, one reservation");
}

/// The lease goes while the factory still builds the actor. A plain actor's start is fenced; a supervised one's is
/// not waited for, so the start itself must notice, or the key stays listed with an actor that never runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_lease_lost_while_its_actor_starts_fails_the_start() {
    let lost = |kind: &str| (format!("{kind}/k"), OwnershipError::Lost);
    let failing = Arc::new(parking_lot::Mutex::new(vec![lost("slow"), lost("sturdy")]));
    let leases = MemoryLeases::new().authority();
    let inner = Unsteady { leases, down: Default::default(), when_down: Down::Fails, failing: failing.clone() };
    let authority = Arc::new(Caching { inner, held: Default::default() });
    let epochs = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let build_epochs = epochs.clone();
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| {
        cfg.ownership = Some(authority);
        // Renewed every 25 ms, so the failures the test injects come at once.
        cfg.lease_renew_interval = Some(Duration::from_millis(25));
    })
    .await;
    let build = move |cx: &SpawnContext, _: Bytes| {
        epochs.lock().push(cx.epoch);
        std::thread::sleep(Duration::from_millis(300));
        Ok(Counter { total: 0, tag: String::new() })
    };
    a.cluster.kind::<CounterApi>("slow").register(build.clone()).await.unwrap();
    a.cluster.kind::<CounterApi>("sturdy").policy(RestartPolicy::default()).register(build).await.unwrap();
    let anywhere = Placement::default();
    let started = a.cluster.get_or_spawn::<CounterApi>("slow", "k", Bytes::new(), &anywhere).await;
    assert!(matches!(started, Err(ClusterError::Spawn(SpawnRefusal::Failed, _))), "{started:?}");
    let started = a.cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &anywhere).await;
    assert!(matches!(started, Err(ClusterError::Spawn(SpawnRefusal::Ownership, _))), "{started:?}");

    // The authority answers from the hold that lapsed until the actor that had it releases it.
    failing.lock().clear();
    let again = a.cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &anywhere).await.unwrap();
    let lost = build_epochs.lock()[1];
    assert_ne!(again.epoch(), lost, "started again under the lease it lost");
    eventually("only the key started again runs", || async { a.cluster.stats().spawned == 1 }).await;
    assert_eq!(a.cluster.stats().leases_lost, 2);
}

/// The authority last heard from the holder when it sent its last good renew, so the grace counts
/// from then, not from when an answer came back or a hung call timed out.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_that_hangs_stops_strictly_owned_actors_when_the_grace_is_spent() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    let stalled = Arc::new(AtomicBool::new(false));
    let leases = MemoryLeases::new().authority();
    let authority =
        Arc::new(Unsteady { leases, down: stalled.clone(), when_down: Down::Hangs, failing: Default::default() });
    let grace = Duration::from_millis(400);
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| {
        cfg.ownership = Some(authority);
        // Renewed every 25 ms, so the stall comes at once, and the grace it measures against.
        cfg.lease_renew_interval = Some(Duration::from_millis(25));
        cfg.lease_grace = Some(grace);
    })
    .await;
    counter_kind(&a).await;
    a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &Placement::default()).await.unwrap();

    stalled.store(true, Relaxed);
    let since = std::time::Instant::now();
    eventually("the actor stops", || async { a.cluster.stats().spawned == 0 }).await;
    let ran = since.elapsed();
    assert!(ran < grace + Duration::from_millis(300), "ran {ran:?} past the last renew the authority saw");
    assert_eq!(a.cluster.stats().leases_lost, 1);
}

/// Handles a timer tick every 10 ms and notes when, on the actor's own workers.
struct Ticker(Arc<parking_lot::Mutex<Vec<std::time::Instant>>>);

impl Actor for Ticker {
    type Message = Msg;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.schedule_every(Duration::from_millis(10), || Msg::Add(1)).map(drop).map_err(drop)
    }

    fn on_message(&mut self, _: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.lock().push(std::time::Instant::now());
        Ok(())
    }
}

/// A holder whose tokio runtime stalls past the grace, as a paused process does, must not act on its lease
/// meanwhile: the actors' workers run on, and the renew loop that would stop them is not polled until it ends.
#[tokio::test]
async fn a_holder_stalled_past_the_grace_handles_nothing_more() {
    let grace = Duration::from_millis(300);
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| {
        cfg.ownership = Some(Arc::new(MemoryLeases::new().authority()));
        // Renewed every 25 ms, and the grace the stall outlasts.
        cfg.lease_renew_interval = Some(Duration::from_millis(25));
        cfg.lease_grace = Some(grace);
    })
    .await;
    let ticks = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = ticks.clone();
    let build = move |_: &SpawnContext, _: Bytes| Ok(Ticker(seen.clone()));
    a.cluster.kind::<CounterApi>("ticker").register(build).await.unwrap();
    a.cluster.get_or_spawn::<CounterApi>("ticker", "k", Bytes::new(), &Placement::default()).await.unwrap();
    eventually("the ticker runs", || async { ticks.lock().len() > 3 }).await;

    // The last renew was sent before the stall, so the lease the grace stands for ends within `grace` of it.
    let stalled = std::time::Instant::now();
    std::thread::sleep(grace * 3);
    let late = ticks.lock().iter().filter(|t| **t >= stalled + grace).count();
    assert_eq!(late, 0, "ticks handled after the grace ran out");
    eventually("the actor is gone", || async { a.cluster.stats().spawned == 0 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_start_side_by_side_and_one_key_still_starts_once() {
    let authority = Arc::new(SlowAcquire(MemoryLeases::new().authority()));
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| cfg.ownership = Some(authority)).await;
    counter_kind(&a).await;
    let started = std::time::Instant::now();
    let mut starts = tokio::task::JoinSet::new();
    for i in 0..40 {
        let cluster = a.cluster.clone();
        starts.spawn(async move {
            let key = format!("k{}", i % 20);
            cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &Placement::default()).await.unwrap();
        });
    }
    starts.join_all().await;
    assert!(started.elapsed() < Duration::from_secs(3), "one at a time would take four: {:?}", started.elapsed());
    assert_eq!(a.cluster.stats().spawned, 20);
}

const PAXOS_LEASE: Duration = Duration::from_secs(1);

async fn paxos_host(id: &str, discovery: &MemoryDiscovery) -> Node {
    node_with(id, discovery.clone(), |cfg| {
        let mut paxos = PaxosConfig::new(["p-0", "p-1", "p-2"]);
        // A 1 s lease, so a lease runs out within the test's wait.
        paxos.lease = PAXOS_LEASE;
        paxos.store = Some(Arc::new(axon::files::MemoryFiles::new()));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    })
    .await
}

/// Keyed actors over Paxos leases, the acceptors on nodes of their own: a crashed holder's key runs
/// elsewhere once its lease runs out, and a holder cut off from a majority stops its actor before
/// the acceptors could grant the key again.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_actors_move_and_stop_with_their_paxos_leases() {
    let discovery = MemoryDiscovery::new();
    let mut acceptors = Vec::new();
    for id in ["p-0", "p-1", "p-2"] {
        acceptors.push(paxos_host(id, &discovery).await);
    }
    let mut hosts = vec![paxos_host("a", &discovery).await, paxos_host("b", &discovery).await];
    for host in &hosts {
        counter_kind(host).await;
    }
    for host in &hosts {
        advertised(host, 2).await;
    }
    let anywhere = Placement::default();
    let spawn = async |host: &Node, on: Option<&str>| {
        let deadline = std::time::Instant::now() + PAXOS_LEASE * 5;
        loop {
            match host.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await {
                Ok(r) if on.is_none_or(|on| r.node() == on) => return r,
                other => assert!(std::time::Instant::now() < deadline, "never spawned: {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    // Acceptors refuse everything while they warm up after starting, then the key is granted.
    let first = spawn(&hosts[0], None).await;
    let owner = hosts.iter().position(|h| h.cluster.identity().node_id == first.node()).unwrap();
    let survivor = hosts[1 - owner].cluster.identity().node_id.clone();
    let crashed = std::time::Instant::now();
    drop(hosts.remove(owner));
    let moved = spawn(&hosts[0], Some(&survivor)).await;
    assert!(moved.epoch() > first.epoch(), "{moved:?} after {first:?}");
    assert!(crashed.elapsed() < PAXOS_LEASE * 3, "moved after {:?}", crashed.elapsed());

    let host = &hosts[0];
    let cut = std::time::Instant::now();
    for acceptor in acceptors.drain(1..) {
        acceptor.system.shutdown().await.unwrap();
    }
    eventually("the actor stops", || async { host.cluster.stats().spawned == 0 }).await;
    assert!(cut.elapsed() < PAXOS_LEASE, "ran {:?} without a majority", cut.elapsed());
    assert_eq!(host.cluster.stats().leases_lost, 1);
    assert!(host.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.is_err());
    assert_eq!(host.cluster.stats().spawned, 0);
}

/// The lease timings follow the lease: with a 1 s lease and nothing else set, the renewals keep one actor
/// through several leases. `keyed_actors_move_and_stop_with_their_paxos_leases` shows the grace ending inside it.
#[tokio::test(flavor = "multi_thread")]
async fn a_short_lease_needs_no_other_setting() {
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for id in ["p-0", "p-1", "p-2", "a"] {
        nodes.push(paxos_host(id, &discovery).await);
    }
    let host = &nodes[3];
    counter_kind(host).await;
    let anywhere = Placement::default();
    let spawn = || host.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere);
    eventually("the acceptors warm up", || async { spawn().await.is_ok() }).await;
    let first = spawn().await.unwrap();
    tokio::time::sleep(PAXOS_LEASE * 3).await;
    assert_eq!(spawn().await.unwrap().epoch(), first.epoch());
    assert_eq!((host.cluster.stats().spawned, host.cluster.stats().leases_lost), (1, 0));
    for node in nodes {
        node.system.shutdown().await.unwrap();
    }
}

/// A start under a lease shares its epoch with the start before it where the node kept the lease between them: a
/// restart under the kind's policy builds again from the same lease. `SpawnContext::start` tells the two apart.
#[tokio::test(flavor = "multi_thread")]
async fn each_start_of_a_key_has_its_own_number_though_starts_share_an_epoch() {
    let a = node_with("a", MemoryDiscovery::new(), |cfg| {
        let mut paxos = PaxosConfig::new(["a"]);
        // A 1 s lease, so the acceptor warms up within the test's wait.
        paxos.lease = PAXOS_LEASE;
        paxos.store = Some(Arc::new(axon::files::MemoryFiles::new()));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    })
    .await;
    let starts = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = starts.clone();
    let build = move |cx: &SpawnContext, _: Bytes| {
        seen.lock().push((cx.epoch, cx.start));
        Ok(Counter { total: 0, tag: String::new() })
    };
    a.cluster.kind::<CounterApi>("plain").register(build.clone()).await.unwrap();
    a.cluster.kind::<CounterApi>("sturdy").policy(RestartPolicy::default()).register(build).await.unwrap();
    let anywhere = Placement::default();
    let spawn = async |kind: &str| a.cluster.get_or_spawn::<CounterApi>(kind, "k", Bytes::new(), &anywhere).await.ok();
    eventually("the acceptor warms up", || async { spawn("sturdy").await.is_some() }).await;
    let sturdy = spawn("sturdy").await.unwrap();
    assert!(sturdy.request(Op::Fail).await.is_err());
    eventually("built again", || async { starts.lock().len() == 2 }).await;
    let plain = spawn("plain").await.unwrap();
    assert!(plain.request(Op::Fail).await.is_err());
    eventually("the plain actor ends", || async { a.cluster.stats().spawned == 1 }).await;
    spawn("plain").await.unwrap();

    let starts = starts.lock().clone();
    let [(e1, s1), (e2, s2), (_, s3), (_, s4)] = starts[..] else { panic!("{starts:?}") };
    assert!(e1.is_some() && e1 == e2, "the policy builds again under the lease it holds: {starts:?}");
    assert!(s1 < s2 && s2 < s3 && s3 < s4, "{starts:?}");
    a.system.shutdown().await.unwrap();
}
