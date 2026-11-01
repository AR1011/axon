use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_membership_subscription_ends_when_the_cluster_shuts_down() {
    let discovery = MemoryDiscovery::new();
    let n = node("a", &discovery).await;
    let mut events = n.cluster.subscribe();
    assert!(matches!(events.next().await, Some(MembershipEvent::Snapshot(_))));
    n.system.shutdown().await.unwrap();
    let drained = async { while events.next().await.is_some() {} };
    tokio::time::timeout(Duration::from_secs(2), drained).await.expect("the subscription never ended");
    let mut late = n.cluster.subscribe();
    assert!(matches!(late.next().await, Some(MembershipEvent::Snapshot(_))));
    assert!(late.next().await.is_none());
}

/// Hands on each membership event its subscription brings.
struct Watcher {
    events: Option<MembershipSubscription>,
    out: tokio::sync::mpsc::Sender<MembershipEvent>,
}

impl Actor for Watcher {
    type Message = Option<MembershipEvent>;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.attach(self.events.take().ok_or(())?, |event| event).map(drop).map_err(drop)
    }

    fn on_message(&mut self, event: Option<MembershipEvent>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.out.try_send(event.ok_or(())?).map_err(drop)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_attaches_its_membership_subscription() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    let (out, mut seen) = tokio::sync::mpsc::channel(64);
    a.system.spawn(Watcher { events: Some(a.cluster.subscribe()), out }).start().unwrap();
    let within = Duration::from_secs(5);
    assert!(matches!(tokio::time::timeout(within, seen.recv()).await, Ok(Some(MembershipEvent::Snapshot(_)))));
    let _b = node("b", &discovery).await;
    let joined = async {
        while let Some(event) = seen.recv().await {
            if let MembershipEvent::Changes { changes, .. } = event
                && changes.iter().any(|c| matches!(c, MemberChange::Joined(node) if node.id == "b"))
            {
                return;
            }
        }
        panic!("the watcher stopped");
    };
    tokio::time::timeout(within, joined).await.expect("the actor never saw b join");
}

/// `b` leaves cleanly with nothing to drain, far faster than `a` polls discovery: `a` still sees it drain, and its
/// `Left` carries the reason, so a leave is told from a crash.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_polling_slower_than_a_leave_takes_still_sees_it_drain_and_why() {
    for world in [World::memory(), World::seeds().await] {
        // The shipped 1 s: a leave of a few ms keeps its draining record up that long.
        let shipped = |cfg: &mut ClusterConfig| cfg.poll_interval = Duration::from_secs(1);
        let (a, b) = (world.node_with("a", shipped).await, world.node_with("b", shipped).await);
        sees(&a, 2).await;
        let mut events = a.cluster.subscribe();
        b.system.shutdown().await.unwrap();
        let left = async {
            loop {
                if let Some(MembershipEvent::Changes { changes, .. }) = events.next().await
                    && let Some(record) = changes.iter().find_map(|c| match c {
                        MemberChange::Left(record) => Some(record.clone()),
                        _ => None,
                    })
                {
                    return record;
                }
            }
        };
        let left = tokio::time::timeout(Duration::from_secs(10), left).await.expect("a never saw b leave");
        assert_eq!(left.metadata.get(LEAVE_REASON).map(String::as_str), Some("Requested"), "{left:?}");
        a.system.shutdown().await.unwrap();
    }
}

/// The draining record is held for a poll only within the hand-off's share of the leave: a `poll_interval` longer
/// than the whole shutdown must not keep the node from unregistering.
#[tokio::test(flavor = "multi_thread")]
async fn a_leave_polling_slower_than_its_shutdown_still_unregisters_in_time() {
    let discovery = MemoryDiscovery::new();
    // Longer than the shutdown_timeout of 5 s.
    let n = node_with("a", discovery.clone(), |cfg| cfg.poll_interval = Duration::from_secs(30)).await;
    let started = tokio::time::Instant::now();
    let report = n.system.shutdown().await.unwrap();
    let took = started.elapsed();
    assert!(report.failures.is_empty(), "{report:?}");
    assert!(took < Duration::from_secs(4), "the leave took {took:?}");
    assert!(discovery.snapshot().await.unwrap().nodes.is_empty(), "the node is still registered");
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_view_returns_once_the_condition_holds() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    let alone = a.cluster.wait_view(|view| view.get("a").is_some()).await.unwrap();
    assert_eq!(alone.members.len(), 1);

    let cluster = a.cluster.clone();
    let both = tokio::spawn(async move { cluster.wait_view(|view| view.members.len() == 2).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!both.is_finished());
    let _b = node("b", &discovery).await;
    let view =
        tokio::time::timeout(Duration::from_secs(5), both).await.expect("the wait never ended").unwrap().unwrap();
    assert!(view.get("b").is_some());

    let cluster = a.cluster.clone();
    let never = tokio::spawn(async move { cluster.wait_view(|view| view.members.len() == 3).await });
    a.system.shutdown().await.unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(2), never).await.expect("the wait outlived the cluster");
    assert!(matches!(ended.unwrap(), Err(ClusterError::Closed)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    let discovery = MemoryDiscovery::new();
    let (cut, to_b) = (Arc::new(AtomicBool::new(false)), Arc::default());
    let at_b = relay(Arc::clone(&to_b), cut.clone()).await;
    let quick = |cfg: &mut ClusterConfig| {
        // Down 1.5 s after it goes silent, not the shipped idle timeout, so the test waits less.
        cfg.transport.keepalive_interval = Duration::from_millis(500);
        cfg.transport.idle_timeout = Duration::from_millis(1500);
        // The redial after it goes silent gives up at a keepalive, not the shipped 1.5 s.
        cfg.transport.connect_timeout = Duration::from_millis(500);
    };
    let a = node_with("a", discovery.clone(), quick).await;
    let b = node_with("b", discovery.clone(), |cfg| {
        quick(cfg);
        cfg.advertise = Some(at_b);
    })
    .await;
    to_b.set(b.cluster.local_addr()).unwrap();
    sees(&a, 2).await;
    publish_counter(&b, 1, "counter");
    let remote = a.cluster.resolve::<CounterApi>("counter").await.unwrap();
    remote.request(Op::Get).await.unwrap();

    // A stopped process: its socket still accepts, but nothing it is sent is answered, handshakes included.
    cut.store(true, Relaxed);
    let started = std::time::Instant::now();
    eventually("b is unreachable", || async { a.cluster.view().get("b").is_some_and(|m| !m.reachable) }).await;
    // The idle timeout and one keepalive, about 2 s; waiting out the 5 s handshake took 6.5 s.
    let took = started.elapsed();
    assert!(took < Duration::from_millis(3500), "waited out a handshake: {took:?}");

    cut.store(false, Relaxed);
    eventually("b is reachable again", || async { a.cluster.view().get("b").is_some_and(|m| m.reachable) }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn crashed_node_is_detected_before_discovery_notices() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&a, 2).await;
    publish_counter(&b, 1, "counter");
    let remote = a.cluster.resolve::<CounterApi>("counter").await.unwrap();
    let mut events = a.cluster.subscribe();
    assert!(matches!(events.next().await, Some(MembershipEvent::Snapshot(view)) if view.members.len() == 2));

    // Requests the peer is sitting on when it dies: each caller must be told, at once, that nobody knows.
    let waiting: Vec<_> = (0..20)
        .map(|_| {
            tokio::spawn({
                let remote = remote.clone();
                async move { remote.request(Op::Hold).await }
            })
        })
        .collect();
    eventually("the requests are with the peer", || async { b.cluster.stats().engine.inbound_calls >= 20 }).await;

    // Dropping the system without shutdown is a crash: no leave, the discovery record stays.
    let old_incarnation = b.cluster.identity().incarnation;
    let crashed = std::time::Instant::now();
    drop(b);
    for call in waiting {
        assert!(matches!(call.await.unwrap(), Err(CallError::OutcomeUnknown)));
    }
    assert!(
        crashed.elapsed() < Duration::from_secs(3),
        "the callers waited for their timeout: {:?}",
        crashed.elapsed()
    );
    assert_eq!(a.cluster.stats().engine.unknown_outcomes, 20);
    eventually("peer marked unreachable", || async { a.cluster.view().get("b").is_some_and(|m| !m.reachable) }).await;
    assert_eq!(a.cluster.view().members.len(), 2);
    assert!(matches!(remote.request(Op::Get).await, Err(CallError::Unreachable(_))));
    let (found, failed) = a.cluster.resolve_all::<CounterApi>("counter").await;
    assert!(found.is_empty() && failed.is_empty(), "{failed:?}");
    let mut saw = false;
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(200), events.next()).await {
        saw |= matches!(event, MembershipEvent::Changes { changes, .. } if changes.contains(&MemberChange::Unreachable("b".into())));
    }
    assert!(saw && a.cluster.stats().became_unreachable == 1);

    // The node restarts under the same id: a new boot, which old references must not reach.
    let b = node("b", &discovery).await;
    assert!(b.cluster.identity().incarnation > old_incarnation);
    publish_counter(&b, 9, "counter");
    eventually("replacement is reachable", || async {
        a.cluster.resolve::<CounterApi>("counter").await.is_ok_and(|r| r.node_incarnation() != old_incarnation)
    })
    .await;
    assert!(matches!(remote.request(Op::Get).await, Err(CallError::StaleNode)));
    assert_eq!(a.cluster.stats().replaced, 1);

    // A node that expires from discovery leaves the view.
    discovery.expire("b");
    sees(&a, 1).await;
    assert!(matches!(remote.request(Op::Get).await, Err(CallError::NoRoute | CallError::Unreachable(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_link_nothing_calls_over_is_closed_and_the_next_call_dials_again() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    // Closed after 300 ms unused rather than the shipped 5 min, so the test waits less.
    let b = node_with("b", discovery.clone(), |cfg| cfg.idle_link_timeout = Duration::from_millis(300)).await;
    sees(&b, 2).await;
    publish_counter(&a, 0, "counter");
    let remote = b.cluster.resolve::<CounterApi>("counter").await.unwrap();

    // A link in use stays: calls keep coming for longer than the idle timeout.
    for _ in 0..10 {
        remote.send(Op::Add(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Control, then Live for the calls.
    assert_eq!(b.cluster.stats().engine.connects, 2);

    eventually("the idle links are closed", || async { b.cluster.stats().engine.peers.is_empty() }).await;
    assert_eq!(number(remote.request(Op::Get).await.unwrap()), 10);
    assert_eq!(b.cluster.stats().engine.connects, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    let server = seed_servers(1).await.remove(0);
    let cut = Arc::new(AtomicBool::new(false));
    let to_seed = relay(Arc::new(server.local_addr().into()), cut.clone()).await;
    let (to_a, to_b) = (Arc::default(), Arc::default());
    let (at_a, at_b) = (relay(Arc::clone(&to_a), cut.clone()).await, relay(Arc::clone(&to_b), cut.clone()).await);
    let a = seed_node("a", &[to_seed], |cfg| cfg.advertise = Some(at_a)).await;
    let b = seed_node("b", &[server.local_addr()], |cfg| cfg.advertise = Some(at_b)).await;
    to_a.set(a.cluster.local_addr()).unwrap();
    to_b.set(b.cluster.local_addr()).unwrap();
    for n in [&a, &b] {
        counter_kind(n).await;
    }
    advertised(&b, 2).await;
    let anywhere = Placement::default();
    let mut key = String::new();
    for i in 0.. {
        key = format!("key-{i}");
        if b.cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &anywhere).await.unwrap().node() == "a" {
            break;
        }
    }
    assert_eq!(a.cluster.stats().spawned, 1);

    cut.store(true, Relaxed);
    let started = std::time::Instant::now();
    let mut isolated = None;
    while b.cluster.view().get("a").is_some() {
        assert!(started.elapsed() < Duration::from_secs(15), "the seed never removed the node");
        let stats = a.cluster.stats();
        if isolated.is_none() && stats.isolated && stats.spawned == 0 {
            isolated = Some(started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let removed = started.elapsed();
    assert!(isolated.is_some_and(|at| at + Duration::from_secs(1) < removed), "{isolated:?} and {removed:?}");
    assert!(removed > Duration::from_secs(5), "a silent node is given time: {removed:?}");
    eventually("the key moves", || async {
        b.cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &anywhere).await.is_ok_and(|r| r.node() == "b")
    })
    .await;

    cut.store(false, Relaxed);
    eventually("the node is back", || async { !a.cluster.stats().isolated && b.cluster.view().members.len() == 2 })
        .await;
    sees(&a, 2).await;
}

/// Hands on each change of a node set it attaches, and the end.
struct SetWatcher {
    changes: Option<NodeChanges>,
    out: tokio::sync::mpsc::Sender<Option<NodeChange>>,
}

impl Actor for SetWatcher {
    type Message = Option<NodeChange>;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.attach(self.changes.take().ok_or(())?, |change| change).map(drop).map_err(drop)
    }

    fn on_message(&mut self, change: Option<NodeChange>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.out.try_send(change).map_err(drop)
    }
}

fn runner(cfg: &mut ClusterConfig) {
    cfg.labels.insert("runner".into(), "yes".into());
}

fn ids(ids: &[&str]) -> Vec<Arc<str>> {
    ids.iter().map(|&id| id.into()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_set_follows_its_labels_through_joins_leaves_and_relabels() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    let b = node_with("b", discovery.clone(), runner).await;
    let c = node_with("c", discovery.clone(), runner).await;
    sees(&a, 3).await;
    let runners = a.cluster.nodes(&Placement::default().require("runner", "yes"));
    assert_eq!(runners.ids(), ids(&["b", "c"]));
    let mut unread = runners.changes();

    // On a system of its own, so it sees the stream end when a shuts down.
    let watching = ActorSystem::builder().build().unwrap();
    let (out, mut seen) = tokio::sync::mpsc::channel(64);
    watching.spawn(SetWatcher { changes: Some(runners.changes()), out }).start().unwrap();
    let mut next = async || tokio::time::timeout(Duration::from_secs(5), seen.recv()).await.expect("no change came");
    let joined = |id: &str| Some(Some(NodeChange::Joined(id.into())));
    let left = |id: &str| Some(Some(NodeChange::Left(id.into())));
    assert_eq!(next().await, joined("b"));
    assert_eq!(next().await, joined("c"));

    let d = node_with("d", discovery.clone(), runner).await;
    assert_eq!(next().await, joined("d"));
    c.cluster.set_metadata("runner", None).await.unwrap();
    assert_eq!(next().await, left("c"));
    a.cluster.set_metadata("runner", Some("yes")).await.unwrap();
    assert_eq!(next().await, joined("a"));
    b.system.shutdown().await.unwrap();
    assert_eq!(next().await, left("b"));
    assert_eq!(runners.ids(), ids(&["a", "d"]));
    let mut order = runners.order("k");
    order.sort();
    assert_eq!(order, runners.ids(), "an order is a permutation of the set");

    a.system.shutdown().await.unwrap();
    // a drains as it leaves, so it may leave its own set before the changes end.
    let mut end = tokio::time::timeout(Duration::from_secs(2), seen.recv()).await.expect("the changes never ended");
    if end == left("a") {
        end = tokio::time::timeout(Duration::from_secs(2), seen.recv()).await.expect("the changes never ended");
    }
    assert_eq!(end, Some(None), "nothing else changed after b left, and then the changes ended");
    let late = tokio::time::timeout(Duration::from_millis(100), runners.changes().next()).await;
    assert_eq!(late, Ok(None), "a stream made after the shutdown ends at once");
    // Neither the set nor a stream of it, unread, keeps the cluster alive.
    let probe = a.cluster.downgrade();
    drop(a);
    eventually("a's cluster is dropped", || async { probe.upgrade().is_none() }).await;
    assert!(runners.ids().is_empty() && runners.order("k").is_empty());
    assert_eq!(unread.next().await, Some(NodeChange::Joined("b".into())), "what it held is still read");
    for n in [c, d] {
        n.system.shutdown().await.unwrap();
    }
    watching.shutdown().await.unwrap();
}

/// A burst of joins past the broadcast's capacity reaches the set as a resync: its changes stay in step, and end at
/// the view's set.
#[tokio::test(flavor = "multi_thread")]
async fn node_changes_end_at_the_views_set_after_a_burst_past_the_event_capacity() {
    let discovery = MemoryDiscovery::new();
    // Two events, so twelve joins leave the reader behind.
    let a = node_with("a", discovery.clone(), |cfg| cfg.event_capacity = 2).await;
    let runners = a.cluster.nodes(&Placement::default().require("runner", "yes"));
    let mut changes = runners.changes();
    let mut nodes = Vec::new();
    for i in 0..12 {
        nodes.push(node_with(&format!("n{i:02}"), discovery.clone(), runner).await);
    }
    eventually("a sees the runners", || async { runners.ids().len() == 12 }).await;
    let mut set = std::collections::BTreeSet::new();
    let read = async {
        while set.iter().cloned().collect::<Vec<_>>() != runners.ids() {
            match changes.next().await.expect("the changes ended") {
                NodeChange::Joined(id) => assert!(set.insert(id.clone()), "{id} joined twice"),
                NodeChange::Left(id) => assert!(set.remove(&id), "{id} left before it joined"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), read).await.expect("the changes never reached the view's set");
    assert_eq!(set.len(), 12);
    for n in nodes.into_iter().chain([a]) {
        n.system.shutdown().await.unwrap();
    }
}
