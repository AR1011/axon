use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn keyed_actor_is_unique_and_follows_its_owner() {
    keyed_actor(World::memory()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn keyed_actor_is_unique_and_follows_its_owner_on_seeds() {
    keyed_actor(World::seeds().await).await
}

async fn keyed_actor(world: World) {
    let mut nodes = Vec::new();
    for id in ["n1", "n2", "n3"] {
        nodes.push(world.node(id).await);
        counter_kind(nodes.last().unwrap()).await;
    }
    for n in &nodes {
        advertised(n, 3).await;
    }
    let anywhere = Placement::default();

    // Every node, asked concurrently, lands on the same single instance.
    let ask = |n: usize| nodes[n].cluster.get_or_spawn::<CounterApi>("counter", "k", seed(100), &anywhere);
    let (r0, r1, r2) = tokio::join!(ask(0), ask(1), ask(2));
    let refs = [r0.unwrap(), r1.unwrap(), r2.unwrap()];
    for r in &refs {
        r.send(Op::Add(1)).await.unwrap();
    }
    assert_eq!(number(refs[0].request(Op::Get).await.unwrap()), 103);
    assert_eq!(refs.iter().map(|r| r.node()).collect::<HashSet<_>>().len(), 1);
    assert_eq!(nodes.iter().map(|n| n.cluster.stats().spawned).sum::<usize>(), 1);

    let owner = refs[0].node().to_owned();

    // A caller whose view holds only n1 asks n1 for every key; n1 refuses the ones it does not own.
    let narrow = node_with("x", StaticDiscovery::new(vec![nodes[0].cluster.record()]).unwrap(), |_| {}).await;
    let mut refused = 0;
    for i in 0..16 {
        let key = format!("fenced-{i}");
        match narrow.cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &anywhere).await {
            Ok(_) => {
                let agreed = nodes[1].cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &anywhere).await;
                assert_eq!(agreed.unwrap().node(), "n1");
            }
            Err(ClusterError::Spawn(SpawnRefusal::NotOwner, _)) => refused += 1,
            Err(other) => panic!("{other:?}"),
        }
    }
    assert!(refused > 0 && refused < 16, "{refused}");
    let fenced = 16 - refused;

    // Keys spread over the nodes.
    let mut owners = HashSet::new();
    for i in 0..24 {
        let r = nodes[0].cluster.get_or_spawn::<CounterApi>("counter", &format!("key-{i}"), seed(0), &anywhere).await;
        owners.insert(r.unwrap().node().to_owned());
    }
    assert_eq!(owners.len(), 3);

    // The owner leaves gracefully: its keyed actors restart on their new owners from the same args.
    let leaving = nodes.iter().position(|n| n.cluster.identity().node_id == owner).unwrap();
    let left = nodes.remove(leaving);
    let hosted = left.cluster.stats().spawned;
    assert!(left.system.shutdown().await.unwrap().failures.is_empty());
    assert_eq!(left.cluster.stats().handoffs as usize, hosted);
    assert!(refs[0].request(Op::Get).await.is_err());
    sees(&nodes[0], 2).await;
    sees(&nodes[1], 2).await;
    let moved = nodes[0].cluster.get_or_spawn::<CounterApi>("counter", "k", seed(100), &anywhere).await.unwrap();
    assert_ne!(moved.node(), owner);
    assert_eq!(number(moved.request(Op::Get).await.unwrap()), 100);
    assert_eq!(nodes.iter().map(|n| n.cluster.stats().spawned).sum::<usize>(), 25 + fenced);

    // A joining node takes over the keys it now wins; the old hosts drain theirs.
    let joined = world.node("n4").await;
    counter_kind(&joined).await;
    // The new host counts a key before its answer reaches the old host, which only then counts the handoff.
    eventually("keys rebalance onto the new node", || async {
        let hosted = joined.cluster.stats().spawned;
        hosted > 0
            && hosted + nodes.iter().map(|n| n.cluster.stats().spawned).sum::<usize>() == 25 + fenced
            && nodes.iter().map(|n| n.cluster.stats().handoffs).sum::<u64>() > 0
    })
    .await;
}

/// Every node names the same owner for a key, and it is the node `get_or_spawn` starts the key on.
#[tokio::test(flavor = "multi_thread")]
async fn every_node_names_the_owner_a_key_starts_on() {
    let discovery = MemoryDiscovery::new();
    let nodes = [node("a", &discovery).await, node("b", &discovery).await];
    for n in &nodes {
        counter_kind(n).await;
    }
    for n in &nodes {
        advertised(n, 2).await;
    }
    let anywhere = Placement::default();
    let mut owners = HashSet::new();
    for i in 0..16 {
        let key = format!("key-{i}");
        let owner = nodes[0].cluster.owner("counter", &key, &anywhere).expect("two candidates");
        assert_eq!(nodes[1].cluster.owner("counter", &key, &anywhere), Some(owner.clone()));
        let started = nodes[1].cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &anywhere).await.unwrap();
        assert_eq!(started.node(), &*owner, "{key}");
        owners.insert(owner);
    }
    assert_eq!(owners.len(), 2, "keys spread");
    let nowhere = Placement::default().require("zone", "none");
    assert_eq!(nodes[0].cluster.owner("counter", "k", &nowhere), None);
    assert_eq!(nodes[0].cluster.owner("unregistered", "k", &anywhere), None);
    for n in nodes {
        n.system.shutdown().await.unwrap();
    }
}

/// A watch the owner refuses for good ends, rather than showing a key that is not running.
#[tokio::test(flavor = "multi_thread")]
async fn a_watch_the_owner_refuses_ends() {
    struct NoWatch;
    impl Authorizer for NoWatch {
        fn authorize(&self, _: &axon_net::Peer, action: Action<'_>) -> bool {
            !matches!(action, Action::Watch { .. })
        }
    }
    let discovery = MemoryDiscovery::new();
    let a = node_with("a", discovery.clone(), |cfg| cfg.authorizer = Some(Arc::new(NoWatch))).await;
    let b = node("b", &discovery).await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    eventually("b sees the kind on a", || async { b.cluster.owner("counter", "k", &anywhere).is_some() }).await;
    let mut watch = b.cluster.watch_key("counter", "k", &anywhere);
    assert_eq!(tokio::time::timeout(Duration::from_secs(5), watch.next()).await.expect("the watch ends"), None);
}

/// A supervised keyed actor waiting out its restart reads as starting, and one told to stop there ends only when its
/// worker next runs it: a watch shows it stopped from the moment its lease is lost, not from its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_watch_shows_a_key_stopped_once_its_lease_is_lost_before_its_actor_ends() {
    let failing = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let leases = MemoryLeases::new().authority();
    let authority =
        Arc::new(Unsteady { leases, down: Default::default(), when_down: Down::Fails, failing: failing.clone() });
    let pool = PoolConfig { name: "keys".into(), ..Default::default() };
    let system =
        ActorSystem::builder().runtime(RuntimeConfig { pools: vec![pool], ..Default::default() }).build().unwrap();
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport("a"), MemoryDiscovery::new());
    cfg.ownership = Some(authority);
    // Renewed every 25 ms, so the loss the test injects comes at once.
    cfg.lease_renew_interval = Some(Duration::from_millis(25));
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    // A restart a minute away, that the test never waits out.
    let backoff = Backoff { initial: Duration::from_secs(60), max: Duration::from_secs(60), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    let kind = cluster.kind::<CounterApi>("sturdy").execution(Execution::Pool("keys")).policy(policy);
    kind.register(build).await.unwrap();
    let anywhere = Placement::default();
    let sturdy = cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &anywhere).await.unwrap();
    sturdy.request(Op::Get).await.unwrap();
    assert!(sturdy.request(Op::Fail).await.is_err());
    let phase = || {
        let page = system.diagnostics_page(None, 10).unwrap();
        page.actors.iter().find(|x| x.actor_type.ends_with("Counter")).map(|x| x.phase)
    };
    eventually("the family waits to restart", || async { phase() == Some(Phase::Starting) }).await;
    let mut watch = cluster.watch_key("sturdy", "k", &anywhere);
    let running = tokio::time::timeout(Duration::from_secs(5), watch.next()).await.unwrap();
    assert!(matches!(running, Some(Owner::Running { epoch: Some(_), .. })), "waiting to restart: {running:?}");

    // The pool's one worker is held, so the family told to stop cannot end.
    let (on, hold) = (Arc::new(std::sync::Mutex::new(None)), Arc::new(std::sync::atomic::AtomicBool::new(true)));
    let busy = system.spawn(Busy { on: on.clone(), hold: hold.clone() }).execution(Execution::Pool("keys")).start();
    busy.unwrap().tell(Msg::Add(1)).unwrap();
    eventually("the worker is held", || async { on.lock().unwrap().is_some() }).await;
    failing.lock().push(("sturdy/k".into(), OwnershipError::Lost));
    eventually("the lease is lost", || async { cluster.stats().leases_lost == 1 }).await;
    let shown = tokio::time::timeout(Duration::from_secs(3), watch.next()).await;
    assert_eq!(shown, Ok(Some(Owner::Stopped)));
    assert_eq!(cluster.stats().spawned, 1, "the actor ended first, so the lease was not what the watch went by");

    hold.store(false, SeqCst);
    eventually("the actor ends", || async { cluster.stats().spawned == 0 }).await;
    system.shutdown().await.unwrap();
}

/// A renew loop that is late, as on a starved runtime, has not yet taken out a lease past its grace. Its actor is fenced,
/// and the acceptors may already have granted the key elsewhere: a watch shows it stopped, not running under it.
#[tokio::test(flavor = "multi_thread")]
async fn a_watch_shows_a_key_stopped_once_its_grace_is_spent_before_the_renew_loop_sees_it() {
    let a = node_with("a", MemoryDiscovery::new(), |cfg| {
        cfg.ownership = Some(Arc::new(MemoryLeases::new().authority()));
        // The renew loop, finding no lease at its first tick, sleeps an hour: nothing renews or loses the key.
        cfg.lease_renew_interval = Some(Duration::from_secs(3600));
        // The grace the key outlives while the test runs.
        cfg.lease_grace = Some(Duration::from_millis(200));
    })
    .await;
    counter_kind(&a).await;
    // Time for the renew loop's first tick, before there is a lease to watch.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let anywhere = Placement::default();
    a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!((a.cluster.stats().spawned, a.cluster.stats().leases_lost), (1, 0), "the renew loop has not run");
    let mut watch = a.cluster.watch_key("counter", "k", &anywhere);
    let shown = tokio::time::timeout(Duration::from_secs(3), watch.next()).await;
    assert_eq!(shown, Ok(Some(Owner::Stopped)));
    a.system.shutdown().await.unwrap();
}

/// Busy for a while on every message, and counts how many of its key are alive at once.
struct Stubborn {
    key: String,
    alive: Arc<std::sync::Mutex<std::collections::HashMap<String, (usize, usize)>>>,
}

impl Actor for Stubborn {
    type Message = Msg;
    type Error = ();

    fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), ()> {
        let mut alive = self.alive.lock().unwrap();
        let (now, most) = alive.entry(self.key.clone()).or_default();
        *now += 1;
        *most = (*most).max(*now);
        Ok(())
    }

    fn on_message(&mut self, _: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(Duration::from_millis(900));
        Ok(())
    }

    fn on_stop(&mut self, _: &StopReason) {
        self.alive.lock().unwrap().get_mut(&self.key).unwrap().0 -= 1;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_actor_that_is_slow_to_drain_is_never_alive_twice() {
    let discovery = MemoryDiscovery::new();
    let alive = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let build = || {
        let alive = alive.clone();
        move |cx: &SpawnContext, _: Bytes| Ok(Stubborn { key: cx.key.clone().unwrap(), alive: alive.clone() })
    };
    // A spawn that never starts gives up in 200 ms, not the shipped 5 s.
    let quick = |cfg: &mut ClusterConfig| cfg.operation_timeout = Duration::from_millis(200);
    let first = node_with("n1", discovery.clone(), quick).await;
    first.cluster.kind::<CounterApi>("stubborn").register(build()).await.unwrap();
    let mut busy = Vec::new();
    for i in 0..12 {
        let key = format!("key-{i}");
        busy.push(
            first
                .cluster
                .get_or_spawn::<CounterApi>("stubborn", &key, Bytes::new(), &Placement::default())
                .await
                .unwrap(),
        );
    }
    for actor in &busy {
        actor.tell(Op::Get).await.unwrap();
    }

    // The new node wins some keys while their actors are still inside a handler, far past the drain wait.
    let second = node_with("n2", discovery.clone(), quick).await;
    second.cluster.kind::<CounterApi>("stubborn").register(build()).await.unwrap();
    eventually("keys move to the new node", || async { second.cluster.stats().spawned > 0 }).await;
    eventually("the moves finish", || async { first.cluster.stats().spawned + second.cluster.stats().spawned == 12 })
        .await;
    let most = alive.lock().unwrap().values().map(|(_, most)| *most).max();
    assert_eq!(most, Some(1), "a key was alive on two nodes at once");
}

/// A `KeyedRef` names the node and epoch of the reference it keeps: none before its first call, nor after a call
/// failed with `Resolve`, and the next actor's once a call has asked again.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_ref_shows_the_node_and_epoch_of_the_actor_it_calls() {
    let authority = Arc::new(MemoryLeases::new().authority());
    let a = node_with("a", MemoryDiscovery::new(), move |cfg| cfg.ownership = Some(authority)).await;
    counter_kind(&a).await;
    let k = a.cluster.keyed::<CounterApi>("counter", "k", seed(0), &Placement::default());
    assert_eq!((k.node(), k.epoch()), (None, None), "nothing asked yet");
    k.tell(Op::Add(1)).await.unwrap();
    assert_eq!((k.node().as_deref(), k.epoch()), (Some("a"), Some(1)));

    assert!(k.request(Op::Fail).await.is_err());
    eventually("the failed actor is gone", || async { a.cluster.stats().spawned == 0 }).await;
    let gone = k.request(Op::Get).await.unwrap_err();
    assert_eq!(gone.retry(), Retry::Resolve, "{gone:?}");
    assert_eq!((k.node(), k.epoch()), (None, None), "dropped with the reference");
    k.request(Op::Get).await.unwrap();
    assert_eq!((k.node().as_deref(), k.epoch()), (Some("a"), Some(2)));
    a.system.shutdown().await.unwrap();
}

/// A table of kept references asks each key's owner once, keeps at most its bound, the keys got last always, asks
/// again after `Resolve`, and keeps no cluster alive.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_refs_ask_each_keys_owner_once_and_keep_at_most_their_bound() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    counter_kind(&a).await;
    advertised(&b, 1).await;
    let refs = b.cluster.keyed_refs::<CounterApi>("counter", &Placement::default(), 4);
    let asked = b.cluster.stats().spawn_requests;
    refs.get("k", seed(5)).send(Op::Add(1)).await.unwrap();
    assert_eq!(refs.get("k", seed(5)).node().as_deref(), Some("a"));
    assert_eq!(number(refs.get("k", seed(5)).request(Op::Get).await.unwrap()), 6);
    assert_eq!(b.cluster.stats().spawn_requests, asked + 1, "the owner was asked once");

    // The young half fills at two keys, and the older half goes the next time it does.
    let fresh = b.cluster.keyed_refs::<CounterApi>("counter", &Placement::default(), 4);
    for key in ["k1", "k2", "k3", "k4"] {
        fresh.get(key, seed(0)).request(Op::Get).await.unwrap();
    }
    // Each get keeps its key, so the kept ones are read first.
    for (key, kept) in [("k4", true), ("k3", true), ("k1", false), ("k2", false)] {
        assert_eq!(fresh.get(key, seed(0)).node().is_some(), kept, "{key}");
    }
    let fresh = b.cluster.keyed_refs::<CounterApi>("counter", &Placement::default(), 4);
    for key in ["k1", "k2", "k3", "k4", "k3", "k5"] {
        fresh.get(key, seed(0)).request(Op::Get).await.unwrap();
    }
    for (key, kept) in [("k3", true), ("k5", true), ("k4", false)] {
        assert_eq!(fresh.get(key, seed(0)).node().is_some(), kept, "{key}: a key got again is young again");
    }

    let asked = b.cluster.stats().spawn_requests;
    assert!(refs.get("k", seed(5)).request(Op::Fail).await.is_err());
    eventually("the failed actor is gone", || async { a.cluster.stats().spawned == 5 }).await;
    let gone = refs.get("k", seed(5)).request(Op::Get).await.unwrap_err();
    assert_eq!(gone.retry(), Retry::Resolve, "{gone:?}");
    assert_eq!(number(refs.get("k", seed(5)).request(Op::Get).await.unwrap()), 5, "a new actor from its args");
    assert_eq!(b.cluster.stats().spawn_requests, asked + 1);

    let probe = b.cluster.downgrade();
    b.system.shutdown().await.unwrap();
    drop(b);
    eventually("the cluster is dropped", || async { probe.upgrade().is_none() }).await;
    for key in ["k", "k6"] {
        let closed = refs.get(key, seed(0)).request(Op::Get).await.unwrap_err();
        assert!(matches!(closed, ClusterError::Closed | ClusterError::Call(CallError::Closed)), "{key}: {closed:?}");
    }
    a.system.shutdown().await.unwrap();
}

/// Drains itself on `Add`, and is still inside that handler for a while after.
struct Leaving;

impl Actor for Leaving {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Add(_) => {
                cx.drain(None);
                std::thread::sleep(Duration::from_millis(300));
            }
            Msg::Get(reply) => drop(reply.map(|r| r.send(Bytes::new()))),
            _ => {}
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_asked_for_while_its_actor_drains_gets_the_next_one() {
    let discovery = MemoryDiscovery::new();
    let n = node("a", &discovery).await;
    n.cluster.kind::<CounterApi>("leaving").register(|_: &SpawnContext, _: Bytes| Ok(Leaving)).await.unwrap();
    let anywhere = Placement::default();
    let first = n.cluster.get_or_spawn::<CounterApi>("leaving", "k", Bytes::new(), &anywhere).await.unwrap();
    first.tell(Op::Add(0)).await.unwrap();
    eventually("the actor drains", || async {
        let page = n.system.diagnostics_page(None, 10).unwrap();
        page.actors.iter().any(|x| x.actor_type.ends_with("Leaving") && x.phase == Phase::Draining)
    })
    .await;
    let next = n.cluster.get_or_spawn::<CounterApi>("leaving", "k", Bytes::new(), &anywhere).await.unwrap();
    next.request(Op::Get).await.expect("the next actor answers");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_registered_with_idle_ends_quiet_keys_and_the_next_get_or_spawn_starts_them_fresh() {
    let a = node("a", &MemoryDiscovery::new()).await;
    let build = |_: &SpawnContext, mut args: Bytes| Ok(Counter { total: args.get_u64(), tag: String::new() });
    // Ten times the gap between tells; embedded.rs tests the exact end.
    let idle = Duration::from_secs(1);
    a.cluster.kind::<CounterApi>("quiet").idle(idle).register(build).await.unwrap();
    a.cluster.kind::<CounterApi>("sturdy").idle(idle).policy(RestartPolicy::default()).register(build).await.unwrap();
    for kind in ["quiet", "sturdy"] {
        // A box that stalls this task for the idle time ends the key it was keeping busy, as it should: that run proves
        // nothing, and goes again once the key has ended.
        let mut stalls = 0;
        while let Err(stall) = busy_then_quiet(&a, kind, idle).await {
            stalls += 1;
            assert!(stalls < 3, "{kind}: stalled for the idle time {stalls} times, the last {stall:?}");
            eventually("the stalled key ends", || async { a.cluster.stats().spawned == 0 }).await;
        }
    }
    a.system.shutdown().await.unwrap();
}

/// Keeps `kind`'s key busy for longer than `idle`, then quiet until it ends, then starts it again. `Err` with the time
/// from the start of one call to the refusal of the next, `Resolve`, when that is `idle` or more: the key's actor handled
/// nothing before that start, so it was quiet for its idle time.
async fn busy_then_quiet(a: &Node, kind: &str, idle: Duration) -> Result<(), Duration> {
    let anywhere = Placement::default();
    let refused = |since: tokio::time::Instant, e: CallError| {
        let stall = since.elapsed();
        assert!(stall >= idle && e.retry() == Retry::Resolve, "{kind}: {e:?} {stall:?} after the call before");
        stall
    };
    let mut since = tokio::time::Instant::now();
    let key = a.cluster.get_or_spawn::<CounterApi>(kind, "k", seed(5), &anywhere).await.unwrap();

    // Busy for longer than the idle time, it stays.
    let tells = 12;
    for _ in 0..tells {
        let now = tokio::time::Instant::now();
        key.tell(Op::Add(1)).await.map_err(|e| refused(since, e))?;
        since = now;
        tokio::time::sleep(idle / 10).await;
    }
    let total = key.request(Op::Get).await.map_err(|e| refused(since, e))?;
    assert_eq!(number(total), 5 + tells, "{kind}");

    eventually("the quiet key ends", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!(a.cluster.stats().keyed_failed, 0, "an idle end is not a failure");
    let since = tokio::time::Instant::now();
    let again = a.cluster.get_or_spawn::<CounterApi>(kind, "k", seed(5), &anywhere).await.unwrap();
    let fresh = again.request(Op::Get).await.map_err(|e| refused(since, e))?;
    assert_eq!(number(fresh), 5, "{kind} started fresh from the args");
    eventually("the key ends again", || async { a.cluster.stats().spawned == 0 }).await;
    Ok(())
}

/// A call that lands on a key as it ends for its idle time is refused, unrun, with `Resolve`: a `KeyedRef`'s next call
/// starts the key again, and `request_until` does both in one call.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_call_refused_as_its_key_ends_idle_says_resolve_and_the_next_starts_the_key_again() {
    let a = node("a", &MemoryDiscovery::new()).await;
    // Short, as the test waits it out twice. Its job holds each drain for as long again, for a call to land in.
    let idle = Duration::from_millis(300);
    let build = |_: &SpawnContext, mut args: Bytes| Ok(Stalled(args.get_u64()));
    a.cluster.kind::<CounterApi>("stalled").idle(idle).register(build).await.unwrap();
    let k = a.cluster.keyed::<CounterApi>("stalled", "k", seed(5), &Placement::default());
    let draining = || async {
        let page = a.system.diagnostics_page(None, 10).unwrap();
        page.actors.iter().any(|x| x.actor_type.ends_with("Stalled") && x.phase == Phase::Draining)
    };

    k.send(Op::Add(1)).await.unwrap();
    eventually("the quiet key drains", draining).await;
    let refused = k.send(Op::Add(1)).await.unwrap_err();
    assert_eq!(refused.retry(), Retry::Resolve, "{refused:?}");
    assert_eq!(number(k.request(Op::Get).await.unwrap()), 5, "a fresh actor, which the refused call never reached");

    eventually("the quiet key drains again", draining).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert_eq!(number(k.request_until(Op::Get, deadline).await.unwrap()), 5);
    // Ended, not drained by the shutdown, which waits most of shutdown_timeout for the job.
    eventually("the key ends", || async { a.cluster.stats().spawned == 0 }).await;
    a.system.shutdown().await.unwrap();
}

/// A keyed actor is named `kind/key` on its node, so `lookup` finds it, supervised or not. The name moves to the key's
/// next actor, and a key no name can hold still starts, unnamed.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_actor_is_named_after_its_kind_and_key() {
    let a = node("a", &MemoryDiscovery::new()).await;
    counter_kind(&a).await;
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    a.cluster.kind::<CounterApi>("sturdy").policy(RestartPolicy::default()).register(build).await.unwrap();

    let k = a.cluster.get_or_spawn::<CounterApi>("counter", "user 7", seed(5), &Placement::default()).await.unwrap();
    let first = a.system.lookup::<Msg>("counter/user 7").expect("named kind/key");
    assert!(k.request(Op::Fail).await.is_err());
    eventually("the failed actor is gone", || async { a.cluster.stats().spawned == 0 }).await;
    let again =
        a.cluster.get_or_spawn::<CounterApi>("counter", "user 7", seed(5), &Placement::default()).await.unwrap();
    let next = a.system.lookup::<Msg>("counter/user 7").expect("the next actor has the name");
    assert_ne!(next.lifecycle.incarnation(), first.lifecycle.incarnation());
    assert_eq!(number(again.request(Op::Get).await.unwrap()), 5);

    a.cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &Placement::default()).await.unwrap();
    assert!(a.system.lookup_supervised::<Msg>("sturdy/k").is_some());

    for key in ["line\nbreak".to_string(), "x".repeat(300)] {
        let r = a.cluster.get_or_spawn::<CounterApi>("counter", &key, seed(3), &Placement::default()).await.unwrap();
        assert_eq!(number(r.request(Op::Get).await.unwrap()), 3, "a key no name can hold still starts");
    }
    a.system.shutdown().await.unwrap();
}

/// Counts its starts, each of which takes 300 ms.
struct SlowStart(Arc<std::sync::atomic::AtomicU64>);

impl Actor for SlowStart {
    type Message = Msg;
    type Error = ();

    fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.fetch_add(1, SeqCst);
        std::thread::sleep(Duration::from_millis(300));
        Ok(())
    }

    fn on_message(&mut self, _: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

/// A keyed start whose name another actor holds is refused `Held` before its actor runs anything, and a keyed actor
/// whose `on_start` outlives the start timeout is stopped rather than left running with its name.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_start_refused_its_name_or_its_start_leaves_nothing_running() {
    let system = ActorSystem::builder().start_timeout(Duration::from_millis(100)).build().unwrap();
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport("a"), MemoryDiscovery::new());
    let a = Node { cluster: Cluster::start(&system, cfg).await.unwrap(), system };
    let starts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counted = starts.clone();
    let build = move |_: &SpawnContext, _: Bytes| Ok(SlowStart(counted.clone()));
    a.cluster.kind::<CounterApi>("slow").register(build).await.unwrap();

    let _holder = a.system.spawn(Counter { total: 0, tag: String::new() }).name("slow/taken").start().unwrap();
    for _ in 0..20 {
        let refused = a
            .cluster
            .get_or_spawn::<CounterApi>("slow", "taken", Bytes::new(), &Placement::default())
            .await
            .unwrap_err();
        assert!(matches!(refused, ClusterError::Spawn(SpawnRefusal::Held, _)), "{refused:?}");
        assert_eq!(refused.retry(), Retry::Later);
    }

    let late =
        a.cluster.get_or_spawn::<CounterApi>("slow", "k", Bytes::new(), &Placement::default()).await.unwrap_err();
    assert_eq!(late.retry(), Retry::Never, "{late:?}");
    eventually("the late actor ends", || async { a.system.lookup::<Msg>("slow/k").is_none() }).await;
    assert_eq!(a.cluster.stats().spawned, 0);
    assert_eq!(starts.load(SeqCst), 1, "only the late actor ran on_start");
    a.system.shutdown().await.unwrap();
}

/// A tell to a key whose actor ended on a node that stays up is lost only until that node says so: a later tell fails
/// with `Resolve`, and the one after reaches the key's next actor.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_tell_after_its_actor_ended_on_a_live_node_asks_for_the_key_again() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    let build = |_: &SpawnContext, mut args: Bytes| Ok(Counter { total: args.get_u64(), tag: String::new() });
    // Long enough that a stalled process does not end the next actor between its tell and the Get below.
    b.cluster.kind::<CounterApi>("quiet").idle(Duration::from_secs(1)).register(build).await.unwrap();
    eventually("the kind is advertised", || async {
        a.cluster.view().members.iter().any(|m| m.record.has_kind("quiet"))
    })
    .await;
    let k = a.cluster.keyed::<CounterApi>("quiet", "k", seed(5), &Placement::default());
    k.tell(Op::Add(1)).await.unwrap();
    eventually("the quiet key ends", || async { b.cluster.stats().spawned == 0 }).await;

    let mut refused = None;
    for _ in 0..200 {
        if let Err(e) = k.tell(Op::Add(1)).await {
            refused = Some(e);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let refused = refused.expect("a tell is refused once b says the actor is gone");
    assert_eq!(refused.retry(), Retry::Resolve, "{refused:?}");
    k.tell(Op::Add(2)).await.unwrap();
    assert_eq!(number(k.request(Op::Get).await.unwrap()), 7, "the next actor got the tell");
    for n in [a, b] {
        n.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_actor_that_fails_is_counted_and_the_next_ask_starts_it_again() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    counter_kind(&a).await;
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(10), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let tag = a.cluster.identity().node_id.clone();
    let build =
        move |cx: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: format!("{tag}:{}", cx.epoch.unwrap_or(0)) });
    a.cluster.kind::<CounterApi>("sturdy").policy(policy).register(build).await.unwrap();
    let anywhere = Placement::default();

    let plain = a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(5), &anywhere).await.unwrap();
    assert!(plain.request(Op::Fail).await.is_err());
    eventually("the failed actor is gone", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!(a.cluster.stats().keyed_failed, 1);
    let again = a.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(5), &anywhere).await.unwrap();
    assert_eq!(number(again.request(Op::Get).await.unwrap()), 5, "built again from the args");

    let sturdy = a.cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &anywhere).await.unwrap();
    sturdy.tell(Op::Add(3)).await.unwrap();
    assert_eq!(number(sturdy.request(Op::Get).await.unwrap()), 3);
    assert!(sturdy.request(Op::Fail).await.is_err());
    eventually("the same reference reaches the restarted actor, with fresh state", || async {
        sturdy.request(Op::Get).await.is_ok_and(|body| number(body) == 0)
    })
    .await;
    let stats = a.cluster.stats();
    assert_eq!((stats.spawned, stats.keyed_failed), (2, 1), "a restart is not a failed key");
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_to_a_supervised_keyed_actor_wait_out_its_restart() {
    let discovery = MemoryDiscovery::new();
    let a = node_with("a", discovery.clone(), |cfg| cfg.max_inbound_replies = 1).await;
    let b = node("b", &discovery).await;
    // Every restart waits 500 ms, for the calls to wait out.
    let backoff = Backoff { initial: Duration::from_millis(500), max: Duration::from_millis(500), jitter: 0.0 };
    let policy = RestartPolicy { backoff, max_restarts: 10, ..Default::default() };
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    a.cluster.kind::<CounterApi>("sturdy").policy(policy).register(build).await.unwrap();
    eventually("the kind is advertised", || async {
        b.cluster.view().members.iter().any(|m| m.record.has_kind("sturdy"))
    })
    .await;
    let anywhere = Placement::default();
    let remote = b.cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &anywhere).await.unwrap();
    let local = a.cluster.get_or_spawn::<CounterApi>("sturdy", "k", Bytes::new(), &anywhere).await.unwrap();
    assert_eq!((remote.node(), local.node()), ("a", "a"));
    let running = || {
        let page = a.system.diagnostics_page(None, 10).unwrap();
        page.actors.iter().any(|x| x.actor_type.ends_with("Counter") && x.phase == Phase::Running)
    };
    // A generation reads `Starting` until its `on_start` has run, so wait for `Running` before failing it.
    let fail = async |caller: &RemoteRef<CounterApi>| {
        eventually("the generation runs", || async { running() }).await;
        caller.send(Op::Fail).await.unwrap();
        eventually("the generation failed", || async { !running() }).await;
    };

    // From another node, then from its own: each call lands in the backoff and is taken by the next generation.
    for caller in [&remote, &local] {
        fail(caller).await;
        caller.send(Op::Add(2)).await.unwrap();
        // An answered call has given its slot back, so the next one at once is not refused `Full`.
        for _ in 0..2 {
            assert_eq!(number(caller.request(Op::Get).await.unwrap()), 2);
        }
    }
    assert_eq!(a.cluster.stats().engine.inbound_rejected, [0; 6], "nothing was turned away");

    // A request whose deadline passes while it waits is dropped: the next generation never sees it.
    for (from, caller) in [("b", &remote), ("a", &local)] {
        fail(caller).await;
        assert!(caller.request_within(Op::Add(5), Duration::from_millis(100)).await.is_err(), "from {from}");
        // The host drops it at its own reading of the deadline, on its own timer, which can trail the caller's; until
        // then its slot is held and a call is refused `Full`, a `Later`. Once counted, the slot is free.
        eventually("the host dropped it", || async { a.cluster.stats().engine.inbound_expired == 1 }).await;
        assert!(!running(), "dropped at its deadline, not by the next generation");
        assert_eq!(number(caller.request(Op::Get).await.unwrap()), 0, "from {from}");
    }
    assert_eq!(a.cluster.stats().engine.inbound_expired, 1, "the remote one is counted");

    // A waiting send holds one of `max_inbound_replies`, so what waits is bounded.
    fail(&remote).await;
    let (first, second) = tokio::join!(remote.send(Op::Add(1)), remote.send(Op::Add(1)));
    let full = |sent: &Result<(), CallError>| matches!(sent, Err(CallError::Rejected(Rejection::Full)));
    assert!(full(&first) || full(&second), "{first:?} {second:?}");
}

/// Releases take a while: the new owner's start waits for the lease.
struct SlowRelease(MemoryAuthority, Duration);

/// How long a [`SlowRelease`] takes.
const RELEASE: Duration = Duration::from_millis(300);

impl OwnershipAuthority for SlowRelease {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        self.0.acquire(key)
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        self.0.renew()
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        let key = key.to_owned();
        Box::pin(async move {
            tokio::time::sleep(self.1).await;
            self.0.release(&key, epoch).await
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_leave_hands_each_key_on_and_then_releases_its_lease() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        let authority = Arc::new(SlowRelease(leases.authority(), RELEASE));
        nodes.push(node_with(id, discovery.clone(), move |cfg| cfg.ownership = Some(authority)).await);
        counter_kind(nodes.last().unwrap()).await;
    }
    for n in &nodes {
        sees(n, 2).await;
    }
    let anywhere = Placement::default();
    let first = nodes[0].cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    assert_eq!(first.epoch(), Some(1));
    let owner = nodes.iter().position(|n| n.cluster.identity().node_id == first.node()).unwrap();
    let leaving = nodes.remove(owner);
    leaving.system.shutdown().await.unwrap();
    assert_eq!(leaving.cluster.stats().handoffs, 1, "{:?}", leaving.cluster.stats());
    let moved = nodes[0].cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    assert_eq!((moved.node(), moved.epoch()), (nodes[0].cluster.identity().node_id.as_str(), Some(2)));
    let (from, to) = (&leaving.cluster.identity().node_id, &nodes[0].cluster.identity().node_id);
    let key = || "counter/k".to_owned();
    assert_eq!(
        told(&leaving.cluster),
        [("acquired", key(), "epoch 1".into()), ("handed_off", key(), format!("to {to}, epoch 1"))]
    );
    assert_eq!(told(&nodes[0].cluster), [("acquired", key(), "epoch 2".into())], "{from} handed it over");
}

/// Its drain waits on a job that never ends, as an account's behind a disk that does not answer. Counts as `Counter` does.
struct Stalled(u64);

impl Actor for Stalled {
    type Message = Msg;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.submit_future(std::future::pending()).map(drop).map_err(drop)
    }

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Add(n) => self.0 += n,
            Msg::Get(reply) => drop(reply.map(|r| r.send(seed(self.0)))),
            _ => {}
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_leave_behind_a_key_that_drains_slowly_still_hands_it_off_and_unregisters() {
    let (world, leases) = (World::seeds().await, MemoryLeases::new());
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        // Longer than the leave gives its hand-offs once the drain has taken the actors' share, about 400 ms of the
        // shipped shutdown_timeout: the hand-off is counted, and its release lands, all the same.
        let release = Duration::from_secs(1);
        let authority: Arc<dyn OwnershipAuthority> = Arc::new(SlowRelease(leases.authority(), release));
        let node = world.node_with(id, move |cfg| cfg.ownership = Some(authority)).await;
        node.cluster.kind::<CounterApi>("stalled").register(|_, _| Ok(Stalled(0))).await.unwrap();
        nodes.push(node);
    }
    for n in &nodes {
        sees(n, 2).await;
    }
    let anywhere = Placement::default();
    let first = nodes[0].cluster.get_or_spawn::<CounterApi>("stalled", "k", seed(0), &anywhere).await.unwrap();
    let owner = nodes.iter().position(|n| n.cluster.identity().node_id == first.node()).unwrap();
    let leaving = nodes.remove(owner);

    let report = leaving.system.shutdown().await.unwrap();
    assert_eq!(report.actors.iter().map(|e| e.reason.clone()).collect::<Vec<_>>(), [StopReason::DrainDeadline]);
    assert!(report.failures.is_empty(), "the hand-off, lease release and unregister finish: {report:?}");
    assert_eq!(leaving.cluster.stats().handoffs, 1, "{:?}", leaving.cluster.stats());
    let moved = nodes[0].cluster.get_or_spawn::<CounterApi>("stalled", "k", seed(0), &anywhere).await.unwrap();
    assert_eq!((moved.node(), moved.epoch()), (nodes[0].cluster.identity().node_id.as_str(), Some(2)));
}

/// A `counter` key's release waits until a `held` key's is asked for, then takes [`LATE`]; notes whether it landed
/// after `close`.
struct ClosesAfter {
    leases: MemoryAuthority,
    asked: tokio::sync::watch::Sender<bool>,
    closed: std::sync::atomic::AtomicBool,
    landed: std::sync::Mutex<Option<bool>>,
}

/// Well inside the sixteenth of the leave's budget its releases are waited for, about 270 ms of the shipped 5 s.
const LATE: Duration = Duration::from_millis(100);

impl OwnershipAuthority for ClosesAfter {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        self.leases.acquire(key)
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        self.leases.renew()
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        let key = key.to_owned();
        Box::pin(async move {
            if !key.starts_with("counter/") {
                self.asked.send_replace(true);
                return self.leases.release(&key, epoch).await;
            }
            let _ = self.asked.subscribe().wait_for(|asked| *asked).await;
            tokio::time::sleep(LATE).await;
            let released = self.leases.release(&key, epoch).await;
            *self.landed.lock().unwrap() = Some(self.closed.load(SeqCst));
            released
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        self.closed.store(true, SeqCst);
        self.leases.close()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leave_out_of_time_closes_the_authority_after_the_releases_it_cut_short() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let authority = Arc::new(ClosesAfter {
        leases: leases.authority(),
        asked: tokio::sync::watch::Sender::new(false),
        closed: false.into(),
        landed: std::sync::Mutex::new(None),
    });
    let ownership = authority.clone() as Arc<dyn OwnershipAuthority>;
    let a = node_with("a", discovery.clone(), |cfg| cfg.ownership = Some(ownership)).await;
    let b = node_with("b", discovery.clone(), |cfg| cfg.ownership = Some(Arc::new(leases.authority()))).await;
    // Refuses every start for later: a `held` key's hand-off is tried again until the leave cuts it short, and its lease
    // is released by `release_all`.
    let full = |cfg: &mut ClusterConfig| {
        cfg.ownership = Some(Arc::new(leases.authority()));
        cfg.max_spawned = 0;
    };
    let c = node_with("c", discovery.clone(), full).await;
    for node in [&a, &b] {
        counter_kind(node).await;
    }
    for node in [&a, &c] {
        let held = node.cluster.kind::<CounterApi>("held");
        held.register(|_, _| Ok(Counter { total: 0, tag: String::new() })).await.unwrap();
    }
    let hosts = |kind| a.cluster.view().members.iter().filter(|m| m.record.has_kind(kind)).count();
    eventually("a sees both kinds on two nodes each", || async { hosts("counter") == 2 && hosts("held") == 2 }).await;
    let (anywhere, from) = (Placement::default(), a.cluster.identity().node_id.clone());
    let on_a = |kind| (0..64).map(|i| format!("k{i}")).find(|k| *a.cluster.owner(kind, k, &anywhere).unwrap() == *from);
    for kind in ["counter", "held"] {
        a.cluster.get_or_spawn::<CounterApi>(kind, &on_a(kind).unwrap(), seed(0), &anywhere).await.unwrap();
    }

    // The counter's release waits until `release_all` releases the held key, after the hand-offs were cut short.
    a.system.shutdown().await.unwrap();
    eventually("the cut release lands", || async { authority.landed.lock().unwrap().is_some() }).await;
    assert_eq!(*authority.landed.lock().unwrap(), Some(false), "the cut release lands before the authority closes");
    assert!(authority.closed.load(SeqCst));
    assert_eq!((a.cluster.stats().handoffs, a.cluster.stats().handoffs_failed), (1, 0), "{:?}", a.cluster.stats());
    drop((b, c));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leave_hands_each_key_on_once_its_own_actor_ends_not_after_the_slowest_drain() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        let authority: Arc<dyn OwnershipAuthority> = Arc::new(SlowRelease(leases.authority(), RELEASE));
        let node = node_with(id, discovery.clone(), move |cfg| cfg.ownership = Some(authority)).await;
        node.cluster.kind::<CounterApi>("stalled").register(|_, _| Ok(Stalled(0))).await.unwrap();
        counter_kind(&node).await;
        nodes.push(node);
    }
    for n in &nodes {
        sees(n, 2).await;
    }
    let anywhere = Placement::default();
    let (from, to) = (nodes[0].cluster.identity().node_id.clone(), nodes[1].cluster.identity().node_id.clone());
    let on_a =
        |kind| (0..64).map(|i| format!("k{i}")).find(|k| *nodes[0].cluster.owner(kind, k, &anywhere).unwrap() == *from);
    let (stalled, counter) = (on_a("stalled").unwrap(), on_a("counter").unwrap());
    nodes[0].cluster.get_or_spawn::<CounterApi>("stalled", &stalled, seed(0), &anywhere).await.unwrap();
    nodes[0].cluster.get_or_spawn::<CounterApi>("counter", &counter, seed(0), &anywhere).await.unwrap();

    let leaving = nodes.remove(0);
    let started = tokio::time::Instant::now();
    let shutdown = tokio::spawn(async move { leaving.system.shutdown().await.map(drop) });
    eventually("the leaving node is out of the view's placement", || async {
        nodes[0].cluster.owner("counter", &counter, &anywhere).is_some_and(|o| *o == *to)
    })
    .await;
    // The stalled key holds the drain to its deadline, most of shutdown_timeout (5 s): the counter's lease must not
    // wait for it, or its callers wait out the lease meanwhile.
    let moved = nodes[0].cluster.get_or_spawn::<CounterApi>("counter", &counter, seed(0), &anywhere).await.unwrap();
    let took = started.elapsed();
    assert_eq!((moved.node(), moved.epoch()), (to.as_str(), Some(2)));
    assert!(took < Duration::from_millis(2500), "the counter moved {took:?} into the leave");
    assert!(!shutdown.is_finished(), "the stalled drain still holds the leave");
    shutdown.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_join_hands_each_moved_key_on_and_then_releases_its_lease() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let slow = || Some(Arc::new(SlowRelease(leases.authority(), RELEASE)) as Arc<dyn OwnershipAuthority>);
    let a = node_with("a", discovery.clone(), |cfg| cfg.ownership = slow()).await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    for i in 0..12 {
        a.cluster.get_or_spawn::<CounterApi>("counter", &format!("k{i}"), seed(0), &anywhere).await.unwrap();
    }
    let b = node_with("b", discovery.clone(), |cfg| cfg.ownership = slow()).await;
    counter_kind(&b).await;
    eventually("keys move onto the new node", || async {
        let (a, b) = (a.cluster.stats(), b.cluster.stats());
        b.spawned > 0 && a.spawned + b.spawned == 12 && a.handoffs as usize == b.spawned
    })
    .await;
    assert_eq!(a.cluster.stats().handoffs_failed, 0);

    // Released once each: nothing is left held once both nodes are gone.
    a.system.shutdown().await.unwrap();
    b.system.shutdown().await.unwrap();
    let after = leases.authority();
    for i in 0..12 {
        assert!(after.acquire(&format!("counter/k{i}")).await.is_ok(), "k{i} is still held");
    }
}

/// What a console shows of placement: the kinds a node hosts with their keys counted, started and ended, and each key it
/// runs with its epoch, as keys start, as one is lost, and as others move to a node that joins.
#[tokio::test(flavor = "multi_thread")]
async fn hosted_lists_the_kinds_registered_and_the_keys_run_as_they_start_are_lost_and_move() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let unsteady = |failing| {
        let authority =
            Unsteady { leases: leases.authority(), down: Default::default(), when_down: Down::Fails, failing };
        Some(Arc::new(authority) as Arc<dyn OwnershipAuthority>)
    };
    let failing = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let authority = unsteady(failing.clone());
    let a = node_with("a", discovery.clone(), move |cfg| {
        cfg.ownership = authority;
        // Renewed every 25 ms, so the loss the test injects comes at once.
        cfg.lease_renew_interval = Some(Duration::from_millis(25));
    })
    .await;
    counter_kind(&a).await;
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    let anywhere = Placement::default();
    a.cluster.kind::<CounterApi>("standing").eager(seed(0), anywhere.clone()).register(build).await.unwrap();
    let mut kinds = kinds_of(&["counter", "standing"]);
    kinds[1].eager = true;
    assert_eq!(a.cluster.hosted(10), Hosted { kinds: kinds.clone(), keys: Vec::new(), total: 0 });

    let names: Vec<String> = (0..8).map(|i| format!("k{i}")).collect();
    for key in &names {
        a.cluster.get_or_spawn::<CounterApi>("counter", key, seed(0), &anywhere).await.unwrap();
    }
    let first: Vec<_> =
        names.iter().map(|key| HostedKey { kind: "counter".into(), key: key.clone(), epoch: Some(1) }).collect();
    (kinds[0].count, kinds[0].started) = (8, [8, 0]);
    assert_eq!(a.cluster.hosted(1000), Hosted { kinds: kinds.clone(), keys: first.clone(), total: 8 });
    let capped = a.cluster.hosted(3);
    assert_eq!((&capped.keys[..], capped.total), (&first[..3], 8), "the first by kind and key, and how many there are");

    failing.lock().push(("counter/k0".into(), OwnershipError::Lost));
    eventually("the key whose lease is lost is no longer listed", || async {
        a.cluster.hosted(1000).keys == first[1..]
    })
    .await;
    (kinds[0].count, kinds[0].ended) = (7, [0, 0, 1, 0, 0]);
    assert_eq!(a.cluster.hosted(0).kinds, kinds, "counted as ended for its lease");

    let b = node_with("b", discovery.clone(), |cfg| cfg.ownership = unsteady(Default::default())).await;
    counter_kind(&b).await;
    let owned = |node: &Node, keys: &[HostedKey]| {
        let here = Some(node.cluster.identity().node_id.as_str());
        keys.iter().all(|k| a.cluster.owner("counter", &k.key, &anywhere).as_deref() == here)
    };
    eventually("keys move onto the new node, each listed once, by its owner", || async {
        let (on_a, on_b) = (a.cluster.hosted(1000), b.cluster.hosted(1000));
        on_b.total > 0 && on_a.total + on_b.total == 7 && owned(&a, &on_a.keys) && owned(&b, &on_b.keys)
    })
    .await;
    let (on_a, on_b) = (a.cluster.hosted(1000), b.cluster.hosted(1000));
    assert!(on_a.keys.iter().all(|k| k.epoch == Some(1)), "{on_a:?}");
    assert!(on_b.keys.iter().all(|k| k.epoch == Some(2)), "a moved key runs under the next epoch: {on_b:?}");
    let listed = b.cluster.hosted(0);
    let mut on_b_kinds = kinds_of(&["counter"]);
    (on_b_kinds[0].count, on_b_kinds[0].started) = (on_b.total, [0, on_b.total as u64]);
    assert_eq!(listed, Hosted { kinds: on_b_kinds, keys: Vec::new(), total: on_b.total }, "taken on in a hand-off");
    (kinds[0].count, kinds[0].ended) = (on_a.total, [0, 0, 1, 0, on_b.total as u64]);
    eventually("a counts each key that moved as ended", || async { a.cluster.hosted(0).kinds == kinds }).await;

    // Without an authority a key is unique per view only, and has no epoch.
    let c = node("c", &MemoryDiscovery::new()).await;
    counter_kind(&c).await;
    c.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    let unleased = vec![HostedKey { kind: "counter".into(), key: "k".into(), epoch: None }];
    let mut counted = kinds_of(&["counter"]);
    (counted[0].count, counted[0].started) = (1, [1, 0]);
    assert_eq!(c.cluster.hosted(10), Hosted { kinds: counted, keys: unleased, total: 1 });
    for node in [a, b, c] {
        node.system.shutdown().await.unwrap();
    }
}

fn kinds_of(kinds: &[&str]) -> Vec<HostedKind> {
    let kind =
        |kind: &&str| HostedKind { kind: (*kind).into(), eager: false, count: 0, started: [0; 2], ended: [0; 5] };
    kinds.iter().map(kind).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_join_that_moves_more_keys_than_calls_in_flight_moves_every_one() {
    let discovery = MemoryDiscovery::new();
    let few = |cfg: &mut ClusterConfig| cfg.max_pending = 8;
    let a = node_with("a", discovery.clone(), few).await;
    counter_kind(&a).await;
    let anywhere = Placement::default();
    for i in 0..48 {
        a.cluster.get_or_spawn::<CounterApi>("counter", &format!("k{i}"), seed(0), &anywhere).await.unwrap();
    }
    let b = node_with("b", discovery.clone(), few).await;
    counter_kind(&b).await;
    eventually("every move is tried", || async {
        let a = a.cluster.stats();
        b.cluster.stats().spawned > 0 && (a.handoffs + a.handoffs_failed) as usize + a.spawned == 48
    })
    .await;
    assert_eq!(a.cluster.stats().handoffs_failed, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unaddressed_ask_finds_the_same_actor_without_naming_its_protocol() {
    let discovery = MemoryDiscovery::new();
    let n = node("n1", &discovery).await;
    counter_kind(&n).await;
    let anywhere = Placement::default();
    let existence = n.cluster.get_or_spawn::<Unaddressed>("counter", "k", seed(4), &anywhere).await.unwrap();
    let typed = n.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(4), &anywhere).await.unwrap();
    assert_eq!((existence.node(), existence.epoch()), (typed.node(), typed.epoch()));
    assert_eq!(n.cluster.stats().spawned, 1);
    assert!(typed.request(Op::Get).await.is_ok_and(|body| number(body) == 4));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handoff_the_new_owner_refuses_is_counted() {
    let discovery = MemoryDiscovery::new();
    let first = node("n1", &discovery).await;
    counter_kind(&first).await;
    let anywhere = Placement::default();
    for i in 0..12 {
        first.cluster.get_or_spawn::<CounterApi>("counter", &format!("key-{i}"), seed(7), &anywhere).await.unwrap();
    }
    // The new node wins keys and has no room for any of them.
    let full = node_with("n2", discovery.clone(), |cfg| cfg.max_spawned = 0).await;
    counter_kind(&full).await;
    // The handoffs run side by side: a key leaves `first` when its drain ends, is asked again after each refusal, a
    // refused spawn each, and is counted a failed handoff once `operation_timeout` has passed.
    eventually("every key kept or its refused handoff counted", || async {
        let stats = first.cluster.stats();
        stats.handoffs_failed > 0 && stats.spawned + stats.handoffs_failed as usize == 12
    })
    .await;
    let stats = first.cluster.stats();
    assert_eq!((stats.handoffs, full.cluster.stats().spawned), (0, 0));
    assert!(stats.spawn_refused > stats.handoffs_failed, "the requester counts each refusal: {stats:?}");
    // Each on the timeline as failed, and nothing else: without a lease a start is not an event.
    let told = told(&first.cluster);
    assert_eq!(told.len() as u64, stats.handoffs_failed, "{told:?}");
    assert!(told.iter().all(|(kind, _, detail)| *kind == "handoff_failed" && detail == "to n2"), "{told:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_runtime_is_full_refuses_a_keyed_start_for_later() {
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport("a"), MemoryDiscovery::new());
    let runtime = RuntimeConfig { max_actors: 16, ..RuntimeConfig::default() };
    let system = ActorSystem::builder().runtime(runtime).build().unwrap();
    let node = Node { cluster: Cluster::start(&system, cfg).await.unwrap(), system };
    counter_kind(&node).await;
    let anywhere = Placement::default();
    let mut keys = 0..;
    let refused = loop {
        let key = format!("key-{}", keys.next().unwrap());
        if let Err(e) = node.cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &anywhere).await {
            break e;
        }
    };
    assert!(matches!(refused, ClusterError::Spawn(SpawnRefusal::Capacity, _)), "{refused:?}");
    assert_eq!(refused.retry(), Retry::Later);
}

#[tokio::test(flavor = "multi_thread")]
async fn full_tables_and_a_panicking_decoder_refuse_the_call_and_the_next_one_works() {
    let discovery = MemoryDiscovery::new();
    let host = node_with("b", discovery.clone(), |cfg| cfg.max_inbound_replies = 4).await;
    let narrow = node_with("c", discovery.clone(), |cfg| cfg.max_pending = 2).await;
    let wide = node("a", &discovery).await;
    publish_counter(&host, 5, "counter");
    sees(&wide, 3).await;
    sees(&narrow, 3).await;
    let from_wide = wide.cluster.resolve::<CounterApi>("counter").await.unwrap();
    let from_narrow = narrow.cluster.resolve::<CounterApi>("counter").await.unwrap();

    assert!(matches!(from_wide.request(Op::Boom).await, Err(CallError::Rejected(Rejection::InvalidMessage))));
    assert_eq!(number(from_wide.request(Op::Get).await.unwrap()), 5, "the session outlives the panic");

    let park = |remote: &RemoteRef<CounterApi>| {
        let remote = remote.clone();
        tokio::spawn(async move { remote.request(Op::Park).await })
    };
    let mut parked = vec![park(&from_narrow), park(&from_narrow)];
    eventually("two are parked", || async { PARKED.lock().unwrap().len() == 2 }).await;
    assert!(matches!(from_narrow.request(Op::Get).await, Err(CallError::Busy)), "the caller's own table is full");
    parked.extend([park(&from_wide), park(&from_wide)]);
    eventually("four are parked", || async { PARKED.lock().unwrap().len() == 4 }).await;
    assert!(matches!(from_wide.request(Op::Get).await, Err(CallError::Rejected(Rejection::Full))), "the host's is");

    for reply in PARKED.lock().unwrap().drain(..) {
        reply.send(Bytes::from_static(b"late")).ok().unwrap();
    }
    for call in parked {
        assert_eq!(&call.await.unwrap().unwrap()[..], b"late");
    }
    assert_eq!(number(from_narrow.request(Op::Get).await.unwrap()), 5);
    assert_eq!(number(from_wide.request(Op::Get).await.unwrap()), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_ref_asks_the_owner_once_and_follows_its_key() {
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for id in ["n1", "n2", "n3"] {
        nodes.push(node(id, &discovery).await);
        counter_kind(nodes.last().unwrap()).await;
    }
    let caller = node("x", &discovery).await;
    advertised(&caller, 3).await;
    let k = caller.cluster.keyed::<CounterApi>("counter", "k", seed(3), &Placement::default());
    let tag = k.request(Op::Tag).await.unwrap();
    let owner = std::str::from_utf8(&tag).unwrap().split(':').next().unwrap().to_owned();
    for _ in 0..9 {
        assert_eq!(number(k.clone().request(Op::Get).await.unwrap()), 3);
    }
    k.tell(Op::Add(1)).await.unwrap();
    k.send(Op::Add(1)).await.unwrap();
    assert_eq!(number(k.request(Op::Get).await.unwrap()), 5);
    assert_eq!(caller.cluster.stats().spawn_requests, 1);

    let leaving = nodes.iter().position(|n| n.cluster.identity().node_id == owner).unwrap();
    nodes.remove(leaving).system.shutdown().await.unwrap();
    sees(&caller, 3).await;
    let lost = k.request(Op::Get).await.unwrap_err();
    assert_eq!(lost.retry(), Retry::Resolve, "{lost:?}");
    assert_eq!(caller.cluster.stats().spawn_requests, 1, "nothing is resent");

    assert_eq!(number(k.request(Op::Get).await.unwrap()), 3);
    let tag = k.request(Op::Tag).await.unwrap();
    assert!(!tag.starts_with(owner.as_bytes()), "{tag:?} still on {owner}");
    assert_eq!(caller.cluster.stats().spawn_requests, 2);
    caller.system.shutdown().await.unwrap();
    let closed = k.request(Op::Get).await.unwrap_err();
    assert_eq!(closed.retry(), Retry::Never, "{closed:?}");
    for n in nodes {
        n.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_owner_key_moves_although_the_survivors_never_called_it() {
    let discovery = MemoryDiscovery::new();
    let mut hosts = Vec::new();
    for id in ["h1", "h2", "h3"] {
        hosts.push(node(id, &discovery).await);
        counter_kind(hosts.last().unwrap()).await;
    }
    let caller = node("caller", &discovery).await;
    caller
        .cluster
        .wait_view(|view| view.members.iter().filter(|m| m.record.has_kind("counter")).count() == 3)
        .await
        .unwrap();
    let anywhere = Placement::default();
    let first = caller.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    let owner = hosts.iter().position(|h| h.cluster.identity().node_id == first.node()).unwrap();
    let gone = hosts.remove(owner).cluster.identity().node_id.clone(); // a crash: its record stays listed

    let seen = caller.cluster.wait_view(|view| view.get(&gone).is_some_and(|m| !m.reachable));
    tokio::time::timeout(Duration::from_secs(10), seen).await.unwrap().unwrap();
    let moved = caller.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    assert_ne!(moved.node(), gone);
    assert_eq!(number(moved.request(Op::Get).await.unwrap()), 0);
    for n in hosts.into_iter().chain([caller]) {
        n.system.shutdown().await.unwrap();
    }
}

/// Keeps the reason each of its actors stopped with.
struct Stops(Arc<std::sync::Mutex<Vec<StopReason>>>);

impl Actor for Stops {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, _: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }

    fn on_stop(&mut self, reason: &StopReason) {
        self.0.lock().unwrap().push(reason.clone());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_actor_whose_lease_is_lost_stops_fenced() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let authority = Arc::new(leases.authority());
    let held = authority.clone();
    let a = node_with("a", discovery, move |cfg| {
        cfg.ownership = Some(held);
        // Renewed every 30 ms, so a lost lease shows at once.
        cfg.lease_renew_interval = Some(Duration::from_millis(30));
    })
    .await;
    let stops = Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = stops.clone();
    let build = move |_: &SpawnContext, _: Bytes| Ok(Stops(kept.clone()));
    a.cluster.kind::<CounterApi>("stops").register(build).await.unwrap();
    a.cluster.get_or_spawn::<CounterApi>("stops", "k", Bytes::new(), &Placement::default()).await.unwrap();

    leases.expire(&authority);
    eventually("the actor stops when its lease is lost", || async { a.cluster.stats().spawned == 0 }).await;
    assert_eq!(*stops.lock().unwrap(), [StopReason::Fenced]);
    a.system.shutdown().await.unwrap();
}

/// A key asked of a node while it shuts down must be refused as something the caller can act on (`Leaving`, which
/// is `Retry::Resolve`), not as `Spawn(Failed, "Closed")`, which is `Retry::Never`: the key is fine and the next
/// owner starts it.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_asked_of_a_node_while_it_shuts_down_is_refused_as_retryable() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        let authority = Arc::new(leases.authority());
        let n = node_with(id, discovery.clone(), move |cfg| cfg.ownership = Some(authority)).await;
        let build = |_: &SpawnContext, mut args: Bytes| {
            let total = args.try_get_u64().map_err(|_| "args must be a u64".to_string())?;
            Ok(Counter { total, tag: String::new() })
        };
        n.cluster.kind::<CounterApi>("counter").policy(RestartPolicy::default()).register(build).await.unwrap();
        nodes.push(n);
    }
    for n in &nodes {
        advertised(n, 2).await;
    }
    let b = nodes.pop().unwrap();
    let a = nodes.pop().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut callers = tokio::task::JoinSet::new();
    for caller in 0..16u64 {
        let (cluster, stop) = (a.cluster.clone(), stop.clone());
        callers.spawn(async move {
            let mut never = Vec::new();
            let mut i = 0u64;
            while !stop.load(SeqCst) {
                let key = format!("k-{caller}-{i}");
                i += 1;
                match cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &Placement::default()).await {
                    Err(e) if e.retry() == Retry::Never => never.push(format!("{key}: {e:?}")),
                    _ => {}
                }
            }
            never
        });
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(b.cluster);
    b.system.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, SeqCst);
    let never: Vec<String> = callers.join_all().await.into_iter().flatten().collect();
    assert!(never.is_empty(), "refused for good during a clean leave: {never:?}");
    drop(a.cluster);
    a.system.shutdown().await.unwrap();
}
