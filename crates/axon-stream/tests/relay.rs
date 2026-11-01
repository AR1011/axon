//! The stream tree over real loopback TCP: relays crash and return mid-stream and every leaf
//! still sees each record exactly once, in order, with one upstream copy per relay.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;

use axon::{ActorSystem, Lane};
use axon_cluster::{Cluster, ClusterConfig, ClusterError, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::*;
use axon_testkit::eventually;
use bytes::Bytes;
use tokio::sync::watch;

mod common;
use common::{NoStreams, key, labels};

struct Node {
    stream: StreamNode,
    cluster: Cluster,
    system: ActorSystem,
}

async fn node(id: &str, role: &str, discovery: &MemoryDiscovery) -> Node {
    node_limited(id, role, discovery, NamespaceLimits::default()).await
}

/// With `md` held to `limits`.
async fn node_limited(id: &str, role: &str, discovery: &MemoryDiscovery, md: NamespaceLimits) -> Node {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"0123456789abcdef0123").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    common::short_cluster(&mut cfg);
    cfg.labels = labels(&[("role", role), ("region", "eu")]);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = match role {
        "origin" => vec![],
        "relay" => vec![labels(&[("role", "origin")])],
        _ => vec![labels(&[("role", "relay"), ("region", "eu")]), labels(&[("role", "origin")])],
    };
    let mut cfg = StreamNodeConfig::new(upstream);
    common::short_stream(&mut cfg);
    let stream = StreamNode::start(&cluster, cfg.limit("md", md)).await.unwrap();
    Node { stream, cluster, system }
}

/// Reads the leaf until it has every record published, failing on anything but the next number; a live subscription
/// outlives the stream's close.
fn collect(leaf: &Node, mut published: watch::Receiver<u64>) -> tokio::task::JoinHandle<u64> {
    let mut subscription = leaf.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    tokio::spawn(async move {
        let mut last = 0;
        loop {
            tokio::select! {
                event = subscription.next() => match event.expect("subscription open") {
                    StreamEvent::Batch(batch) => {
                        for (sequence, record) in (batch.first()..).zip(batch.records()) {
                            assert_eq!((sequence, &record[..]), (last + 1, &sequence.to_be_bytes()[..]));
                            last = sequence;
                        }
                    }
                    terminal if terminal.is_terminal() => panic!("unexpected {terminal:?} after {last}"),
                    _ => {}
                },
                _ = published.changed() => {}
            }
            let target = *published.borrow();
            if target > 0 && target == last {
                return last;
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn leaves_see_every_record_once_while_relays_crash_and_return() {
    let discovery = MemoryDiscovery::new();
    let origin = node("origin", "origin", &discovery).await;
    let mut relays =
        vec![Some(node("relay-a", "relay", &discovery).await), Some(node("relay-b", "relay", &discovery).await)];
    let mut leaves = Vec::new();
    for id in ["leaf-1", "leaf-2", "leaf-3"] {
        leaves.push(node(id, "leaf", &discovery).await);
    }
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let (tell_published, published) = watch::channel(0);
    let collectors: Vec<_> = leaves.iter().map(|leaf| collect(leaf, published.clone())).collect();
    eventually("every leaf is positioned behind a relay", || async {
        leaves.iter().all(|l| {
            l.stream.cursor(&key()).is_some() && l.stream.parent_of(&key()).is_some_and(|p| p.starts_with("relay"))
        })
    })
    .await;

    let (publisher, stop) = (origin.stream.clone(), Arc::new(AtomicBool::new(false)));
    let stopped = stop.clone();
    let publishing = tokio::spawn(async move {
        let mut next = 1u64;
        while !stopped.load(Relaxed) {
            let records = (next..next + 3).map(|n| Bytes::copy_from_slice(&n.to_be_bytes())).collect();
            publisher.publish(&key(), records).unwrap();
            next += 3;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        next - 1
    });

    for round in 0..4 {
        let victim = round % 2;
        let name = ["relay-a", "relay-b"][victim];
        tokio::time::sleep(Duration::from_millis(300)).await;
        relays[victim] = None;
        eventually("no leaf depends on the dead relay", || async {
            leaves.iter().all(|l| l.stream.parent_of(&key()).is_some_and(|p| p != name))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        relays[victim] = Some(node(name, "relay", &discovery).await);
    }
    // A leaf's timeline names the stream whose parent it lost, and nothing a timeline does not need (but the host's TCP
    // limits, once a start on a host below the push window).
    let told = |l: &Node| {
        let events = l.cluster.events().since(0, usize::MAX).into_iter().filter(|e| e.kind != "host_tcp_limits");
        events.map(|e| (e.kind, e.subject))
    };
    let told: Vec<_> = leaves.iter().flat_map(told).collect();
    assert!(told.contains(&("parent_lost", "md/btc".to_owned())), "{told:?}");
    assert!(
        told.iter().all(|(kind, _)| ["parent_lost", "reset", "orphaned", "parent_restored"].contains(kind)),
        "{told:?}"
    );
    // Once per loss: four relay crashes, and a leaf loses its parent only when it was on the one that went.
    for leaf in &leaves {
        let lost = leaf.cluster.events().since(0, usize::MAX).iter().filter(|e| e.kind == "parent_lost").count();
        assert!(lost <= 4, "{lost} parent_lost on one leaf: {told:?}");
    }
    // With both relays down the leaves go direct, and come back once a relay does.
    relays.clear();
    eventually("every leaf pulls from the origin", || async {
        leaves.iter().all(|l| l.stream.parent_of(&key()).as_deref() == Some("origin"))
    })
    .await;
    relays.push(Some(node("relay-a", "relay", &discovery).await));
    relays.push(Some(node("relay-b", "relay", &discovery).await));
    eventually("every leaf is back behind a relay", || async {
        leaves.iter().all(|l| l.stream.parent_of(&key()).is_some_and(|p| p.starts_with("relay")))
    })
    .await;
    eventually("the origin feeds relays only", || async {
        origin.stream.stats().downstream.iter().all(|d| (d.branches > 0) == d.node.starts_with("relay"))
    })
    .await;

    stop.store(true, Relaxed);
    let published = publishing.await.unwrap();
    assert!(published > 500, "published {published}");
    tell_published.send(published).unwrap();
    for collector in collectors {
        let read = tokio::time::timeout(Duration::from_secs(30), collector).await;
        assert_eq!(read.expect("every leaf reads every record within 30 s").unwrap(), published);
    }
    origin.stream.close_stream(&key()).unwrap();
    let switches: u64 = leaves.iter().map(|l| l.stream.stats().switches).sum();
    assert!(switches >= 3, "leaves moved back to a relay {switches} times");
    assert!(leaves.iter().all(|l| l.stream.stats().local_resets == 0));

    let Node { stream, cluster, system: _system } = origin;
    drop(stream);
    eventually("a dropped stream node frees its endpoint", || async {
        StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await.is_ok()
    })
    .await;
}

/// A fresh node has no history of its own: the cursor it resumes from is asked of its parent.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_edge_resumes_after_its_cursor_from_the_origin() {
    let discovery = MemoryDiscovery::new();
    let origin = node("origin", "origin", &discovery).await;
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let publish = |range: std::ops::RangeInclusive<u64>| {
        for n in range {
            origin.stream.publish(&key(), vec![Bytes::copy_from_slice(&n.to_be_bytes())]).unwrap();
        }
    };
    let read = |mut subscription: Subscription, mut last: u64, through: u64| async move {
        let mut events = Vec::new();
        while last < through {
            match subscription.next().await.expect("subscription open") {
                StreamEvent::Batch(batch) => {
                    for (sequence, record) in (batch.first()..).zip(batch.records()) {
                        assert_eq!((sequence, &record[..]), (last + 1, &sequence.to_be_bytes()[..]));
                        last = sequence;
                    }
                }
                event => events.push(event),
            }
        }
        (subscription, events)
    };

    let edge = node("edge", "leaf", &discovery).await;
    let subscription = edge.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    eventually("the edge is positioned", || async { edge.stream.cursor(&key()).is_some() }).await;
    publish(1..=10);
    let reading = tokio::time::timeout(Duration::from_secs(10), read(subscription, 0, 10));
    let (subscription, _) = reading.await.expect("the first ten records");
    drop((subscription, edge));

    publish(11..=20);
    let edge = node("edge", "leaf", &discovery).await;
    let cursor = Cursor { generation: 1, sequence: 10 };
    let options = SubscribeOptions { start: StartPosition::After(cursor), ..Default::default() };
    let resumed = edge.stream.subscribe(&key(), options).expect("a cursor start on a node without the stream");
    let reading = tokio::spawn(tokio::time::timeout(Duration::from_secs(10), read(resumed, 10, 30)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish(21..=30);
    let (_, events) = reading.await.unwrap().expect("the records after the cursor");
    assert_eq!(events, [StreamEvent::Ready { start: Some(cursor) }, StreamEvent::CaughtUp { through: cursor }]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_traced_publish_is_a_span_on_every_hop_and_reaches_the_subscriber() {
    let discovery = MemoryDiscovery::new();
    let origin = node("origin", "origin", &discovery).await;
    let relay = node("relay-a", "relay", &discovery).await;
    let leaf = node("leaf-1", "leaf", &discovery).await;
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let mut subscription = leaf.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    // Positioned, not only parented: a live start is where the pull reached the origin, and what was published
    // before that is not this subscription's (guide/streams.md, Quick start).
    eventually("the leaf is positioned through the relay", || async {
        leaf.stream.cursor(&key()).is_some() && leaf.stream.parent_of(&key()).as_deref() == Some("relay-a")
    })
    .await;

    origin.stream.publish(&key(), vec![Bytes::from_static(b"untraced")]).unwrap();
    let root = axon::TraceContext::root();
    root.enter(|| origin.stream.publish(&key(), vec![Bytes::from_static(b"traced")]).unwrap());
    let reading = async {
        let mut seen = Vec::new();
        while seen.len() < 2 {
            match subscription.next().await.expect("subscription open") {
                StreamEvent::Batch(batch) => seen.push(batch.trace_context()),
                event => assert!(!event.is_terminal(), "{event:?} after {seen:?}"),
            }
        }
        seen
    };
    let seen = tokio::time::timeout(Duration::from_secs(10), reading).await.expect("both records within 10 s");
    let at = |node: &Node| node.system.tracer().since(0, 10);
    // A hop records its span once it has handled the push, which can be after the subscriber has the batch.
    eventually("a span on every hop", || async { [&origin, &relay, &leaf].iter().all(|n| !at(n).is_empty()) }).await;
    let (published, hop_1, hop_2) = (at(&origin), at(&relay), at(&leaf));
    assert_eq!((published.len(), hop_1.len(), hop_2.len()), (1, 1, 1));
    assert_eq!((published[0].parent, published[0].peer.as_deref()), (0, None));
    assert_eq!((hop_1[0].parent, hop_2[0].parent), (published[0].span, hop_1[0].span));
    assert_eq!(
        (&*hop_2[0].name, hop_2[0].peer.as_deref(), hop_2[0].kind),
        ("md/btc", Some("relay-a"), axon::SpanKind::Stream)
    );
    assert_eq!(seen, [None, Some(axon::TraceContext { trace: root.trace, span: hop_2[0].span })]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_that_falls_behind_still_sees_every_record_in_order() {
    let origin = node("origin", "origin", &MemoryDiscovery::new()).await;
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let mut subscription = origin.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    // Nothing polls while these land: the first wake fills the subscription, the rest wait in the hub.
    for sequence in 1..=100u64 {
        origin.stream.publish(&key(), vec![Bytes::copy_from_slice(&sequence.to_be_bytes())]).unwrap();
    }
    let mut last = 0;
    let reading = tokio::time::timeout(Duration::from_secs(10), read_to(&mut subscription, &mut last, 100)).await;
    reading.expect("every record within 10 s");
    drop(subscription);
    assert_eq!((origin.stream.stats().hub.queued_records, origin.stream.stats().hub.subscriptions), (0, 0));
}

/// A producer that moves after its node crashed, to a node that relays its key: a keyed actor restarting where
/// followers already pull the key. Nothing closed the key there, so the relay still holds the dead origin's position.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_relaying_a_key_originates_it_once_the_origin_crashed() {
    let discovery = MemoryDiscovery::new();
    let origin = node("origin", "origin", &discovery).await;
    let relay = node("relay-a", "relay", &discovery).await;
    let leaf = node("leaf", "leaf", &discovery).await;
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let mut followers = [&relay, &leaf].map(|n| n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap());
    eventually("both are positioned", || async { [&relay, &leaf].iter().all(|n| n.stream.cursor(&key()).is_some()) })
        .await;
    // Reads batches until one ends at `through`: never a terminal event, never past it.
    async fn reach(follower: &mut Subscription, through: Cursor) {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), follower.next()).await.expect("a batch");
            match event.expect("open") {
                StreamEvent::Batch(batch) if batch.cursor() == through => return,
                StreamEvent::Batch(batch) => assert!(batch.generation() <= through.generation, "{batch:?}"),
                event => assert!(!event.is_terminal(), "{event:?}"),
            }
        }
    }
    origin.stream.publish(&key(), vec![Bytes::from_static(b"one")]).unwrap();
    for follower in &mut followers {
        reach(follower, Cursor { generation: 1, sequence: 1 }).await;
    }

    drop(origin); // a crash: no close reaches the relay
    relay.stream.open_stream(&key(), 2, DeliveryPolicy::ReplayRequired).await.unwrap();
    // The leaf's branch ends with the relay's old stream, and it joins the new one where it finds it.
    eventually("the leaf follows the new origin", || async {
        leaf.stream.cursor(&key()).is_some_and(|c| c.generation == 2)
    })
    .await;
    relay.stream.publish(&key(), vec![Bytes::from_static(b"two")]).unwrap();
    for follower in &mut followers {
        reach(follower, Cursor { generation: 2, sequence: 1 }).await;
    }
}

/// A namespace's subscriptions are held on the node they are made on, not by the branches they pull, and its ring
/// bytes by the streams a node originates, not by those it relays.
#[tokio::test(flavor = "multi_thread")]
async fn a_namespace_holds_subscriptions_where_they_are_made_and_ring_bytes_where_streams_start() {
    let discovery = MemoryDiscovery::new();
    let md = NamespaceLimits { subscriptions: Some(1), ring_bytes: Some(16), ..NamespaceLimits::default() };
    let origin = node_limited("origin", "origin", &discovery, md).await;
    let relay = node_limited("relay-a", "relay", &discovery, md).await;
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let _here = origin.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let mut far = relay.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let full = relay.stream.subscribe(&key(), SubscribeOptions::default()).err();
    assert!(matches!(full, Some(StreamError::QuotaExceeded { limit: NamespaceLimit::Subscriptions, .. })), "{full:?}");
    eventually("the relay is positioned", || async { relay.stream.cursor(&key()).is_some() }).await;
    for n in 1..=4u64 {
        origin.stream.publish(&key(), vec![Bytes::copy_from_slice(&n.to_be_bytes())]).unwrap();
    }
    let mut last = 0;
    tokio::time::timeout(Duration::from_secs(10), read_to(&mut far, &mut last, 4))
        .await
        .expect("the far subscriber reads");
    // The relayed ring holds 32 bytes of md, and a stream the relay starts has its own 16.
    let eth = StreamKey::new("md", "eth");
    relay.stream.open_stream(&eth, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    relay.stream.publish(&eth, vec![Bytes::from(vec![0; 16])]).unwrap();
    assert_eq!(relay.stream.stats().retained_bytes, 48);
}

/// A leaf follows a key in each of two namespaces whose keys wait on their parents 2 s and 6 s. The origin goes, and
/// comes back 4 s later with a new generation: the first key's follower was told `OriginLost` at about 2 s, the
/// second's never, and both take the new generation.
#[tokio::test(flavor = "multi_thread")]
async fn a_namespace_with_a_longer_upstream_timeout_waits_longer_before_origin_lost() {
    let discovery = MemoryDiscovery::new();
    let near = key();
    let far = StreamKey::new("fx", "eur");
    let origin = node("origin", "origin", &discovery).await;
    let mut transport = axon_net::Config::new("leaf", SharedSecret::new(*b"0123456789abcdef0123").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    common::short_cluster(&mut cfg);
    cfg.labels = labels(&[("role", "leaf")]);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let mut cfg = StreamNodeConfig::new(vec![labels(&[("role", "origin")])]);
    common::short_stream(&mut cfg);
    // Shorter and longer than the time the origin is away.
    cfg.core.timings.upstream_timeout = Duration::from_secs(2);
    let far_timings = RelayTimings { upstream_timeout: Duration::from_secs(6), ..cfg.core.timings };
    let cfg = cfg.relay_timings("fx", far_timings);
    let leaf = StreamNode::start(&cluster, cfg).await.unwrap();
    for key in [&near, &far] {
        origin.stream.open_stream(key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    let mut followers = [&near, &far].map(|key| leaf.subscribe(key, SubscribeOptions::default()).unwrap());
    eventually("the leaf is positioned on both", || async {
        [&near, &far].iter().all(|key| leaf.cursor(key).is_some())
    })
    .await;

    let gone = tokio::time::Instant::now();
    drop(origin);
    let mut lost = [None, None];
    let back = gone + Duration::from_secs(4);
    while tokio::time::Instant::now() < back {
        for (follower, lost) in followers.iter_mut().zip(&mut lost) {
            if let Ok(event) = tokio::time::timeout(Duration::from_millis(20), follower.next()).await {
                match event.expect("open") {
                    StreamEvent::OriginLost => *lost = Some(gone.elapsed()),
                    event => assert!(!event.is_terminal(), "{event:?}"),
                }
            }
        }
    }
    let told = lost[0].expect("the near key's follower was told OriginLost");
    assert!(told >= Duration::from_secs(2) && told < Duration::from_secs(4), "told after {told:?}");
    assert_eq!(lost[1], None, "the far key's follower was told before its 6 s");

    let origin = node("origin", "origin", &discovery).await;
    for key in [&near, &far] {
        origin.stream.open_stream(key, 2, DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    eventually("the leaf follows the new generation", || async {
        [&near, &far].iter().all(|key| leaf.cursor(key).is_some_and(|c| c.generation == 2))
    })
    .await;
    for key in [&near, &far] {
        origin.stream.publish(key, vec![Bytes::from_static(b"two")]).unwrap();
    }
    for follower in &mut followers {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), follower.next()).await.expect("a record");
            match event.expect("open") {
                StreamEvent::Batch(batch) if batch.generation() == 2 => break,
                event => assert!(!matches!(event, StreamEvent::OriginLost) && !event.is_terminal(), "{event:?}"),
            }
        }
    }
}

/// Reads on from `last` through `through`, in order, with nothing terminal between.
async fn read_to(follower: &mut Subscription, last: &mut u64, through: u64) {
    while *last < through {
        match follower.next().await.expect("subscription open") {
            StreamEvent::Batch(batch) => {
                for (sequence, record) in (batch.first()..).zip(batch.records()) {
                    assert_eq!((sequence, &record[..]), (*last + 1, &sequence.to_be_bytes()[..]));
                    *last = sequence;
                }
            }
            event => assert!(!event.is_terminal(), "{event:?} after {last}"),
        }
    }
}

/// A revoked edge cannot refresh its interest, and a parent drops a child it has not heard from for
/// `downstream_expiry`: nothing published after that reaches it, and its session and push task end. Granted access
/// again, it asks again and reads on from its cursor, the records it missed included. An expiry one lost interest
/// would outlast is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_edge_gets_no_more_records_once_its_parent_drops_it() {
    let discovery = MemoryDiscovery::new();
    let (origin, edge) = (node("origin", "origin", &discovery).await, node("edge", "leaf", &discovery).await);
    origin.stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let publish = |range: std::ops::RangeInclusive<u64>| {
        for n in range {
            origin.stream.publish(&key(), vec![Bytes::copy_from_slice(&n.to_be_bytes())]).unwrap();
        }
    };
    let sessions = origin.stream.stats().hub.sessions;
    let mut follower = edge.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
    eventually("the edge is positioned", || async { edge.stream.cursor(&key()).is_some() }).await;
    publish(1..=1);
    let mut last = 0;
    let first = tokio::time::timeout(Duration::from_secs(10), read_to(&mut follower, &mut last, 1)).await;
    first.expect("the first record");

    origin.cluster.set_authorizer(Some(Arc::new(NoStreams("edge"))));
    eventually("the origin drops the edge", || async { origin.stream.stats().downstream.is_empty() }).await;
    assert_eq!(origin.stream.stats().hub.sessions, sessions);
    eventually("the edge's push task ends", || async { origin.stream.pumps() == 0 }).await;
    publish(2..=11);
    let late = tokio::time::timeout(Duration::from_secs(1), read_to(&mut follower, &mut last, 2)).await;
    assert!(late.is_err(), "the revoked edge read through {last}");

    origin.cluster.set_authorizer(None);
    let missed = tokio::time::timeout(Duration::from_secs(10), read_to(&mut follower, &mut last, 11)).await;
    missed.expect("the records published while it was revoked");
    assert_eq!(edge.stream.stats().local_resets, 0);

    let mut cfg = StreamNodeConfig::new(vec![]);
    // refresh 2 s, request_timeout 1 s, retry_failed 500 ms and two ticks of 200 ms.
    cfg.downstream_expiry = Duration::from_millis(3900);
    let refused = StreamNode::start(&origin.cluster, cfg).await.err();
    assert!(matches!(refused, Some(ClusterError::Config("downstream_expiry must outlast one lost interest"))));
}

/// A namespace on `Control` is refused, and so is a `max_push_bytes` larger than the frame of a lane in use: `Live`
/// always, `Bulk` once a namespace names it. `Control`'s frame bounds only the interests.
#[tokio::test(flavor = "multi_thread")]
async fn start_refuses_a_namespace_on_control_and_a_push_larger_than_its_lanes_frame() {
    let mut transport = axon_net::Config::new("small", SharedSecret::new(*b"0123456789abcdef0123").unwrap());
    transport.allow_plaintext = true;
    (transport.lanes.live.limits.max_frame, transport.lanes.bulk.limits.max_frame) = (512 << 10, 256 << 10);
    transport.lanes.control.limits.max_frame = 128 << 10;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let start = async |pushes: usize, namespace: &str, lane: Lane| {
        let mut cfg = StreamNodeConfig::new(vec![]).lane(namespace, lane);
        // A batch the hub admits must fit a push.
        (cfg.core.max_push_bytes, cfg.core.hub.max_batch_bytes) = (pushes, 128 << 10);
        StreamNode::start(&cluster, cfg).await
    };
    let frame = "max_push_bytes exceeds the max_frame of a lane in use";
    let refused = start(256 << 10, "md", Lane::Control).await.err();
    assert!(matches!(refused, Some(ClusterError::Config("a namespace's pushes go on Live or Bulk"))), "{refused:?}");
    let refused = start((512 << 10) + 1, "md", Lane::Live).await.err();
    assert!(matches!(refused, Some(ClusterError::Config(e)) if e == frame), "Live: {refused:?}");
    let refused = start(300 << 10, "bulk", Lane::Bulk).await.err();
    assert!(matches!(refused, Some(ClusterError::Config(e)) if e == frame), "Bulk: {refused:?}");
    let node = start(256 << 10, "bulk", Lane::Bulk).await.expect("pushes that fit Live and Bulk");
    drop((node, cluster));
    system.shutdown().await.unwrap();
}
