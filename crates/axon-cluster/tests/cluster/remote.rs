use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn publish_resolve_call() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&a, 2).await;
    sees(&b, 2).await;

    let actor = a.system.spawn(Counter { total: 0, tag: "a".into() }).start().unwrap();
    let published = a.cluster.publish::<CounterApi>(&actor, "counter").unwrap();
    assert!(matches!(a.cluster.publish::<CounterApi>(&actor, "counter"), Err(ClusterError::NameTaken)));

    let remote = b.cluster.resolve::<CounterApi>("counter").await.unwrap();
    assert_eq!((remote.node(), remote.node_incarnation()), ("a", a.cluster.identity().incarnation));
    remote.send(Op::Add(5)).await.unwrap();
    remote.tell(Op::Add(2)).await.unwrap();
    assert_eq!(number(remote.request(Op::Get).await.unwrap()), 7);
    assert!(matches!(remote.request(Op::Ignore).await, Err(CallError::NoReply)));

    // The publishing node reaches it through the same API without touching the network.
    assert_eq!(number(published.request(Op::Get).await.unwrap()), 7);
    assert_eq!(number(a.cluster.resolve::<CounterApi>("counter").await.unwrap().request(Op::Get).await.unwrap()), 7);

    assert!(matches!(b.cluster.resolve::<OtherApi>("counter").await, Err(ClusterError::ProtocolMismatch)));
    assert!(matches!(b.cluster.resolve::<CounterApi>("nope").await, Err(ClusterError::NotFound)));
    b.cluster.resolve_on::<CounterApi>("a", "counter").await.unwrap();
    assert!(b.cluster.stats().route_hits >= 2);

    let twin = publish_counter(&b, 0, "counter");
    assert!(matches!(a.cluster.resolve::<CounterApi>("counter").await, Err(ClusterError::Ambiguous(2))));
    assert_eq!(a.cluster.resolve_all::<CounterApi>("counter").await.0.len(), 2);

    // Unpublishing fences the reference; a dead actor withdraws itself.
    a.cluster.unpublish(&published);
    assert!(matches!(remote.request(Op::Get).await, Err(CallError::Rejected(Rejection::UnknownActor))));
    // A refused tell is lost without the sender hearing of it: the receiving node keeps a dead letter.
    let root = TraceContext::root();
    root.scope(remote.tell(Op::Add(1))).await.unwrap();
    eventually("refused tell is a dead letter, and its trace shows where it ended", || async {
        let letters = a.system.dead_letters().recent(10);
        let lost =
            letters.iter().any(|l| (l.reason, l.label, l.peer.as_deref()) == ("unknown_actor", "tell", Some("b")));
        let spans = a.system.tracer().trace(root.trace.get());
        lost && spans
            .iter()
            .any(|s| (s.kind, &*s.name, s.failed, s.peer.as_deref()) == (SpanKind::Handle, "tell", true, Some("b")))
    })
    .await;
    twin.lifecycle.stop();
    twin.join().await;
    // A cached route may outlive its actor; the first call through it fails and evicts it.
    let cached = a.cluster.resolve_on::<CounterApi>("b", "counter").await.unwrap();
    eventually("dead actor is withdrawn", || async {
        matches!(cached.request(Op::Get).await, Err(CallError::Rejected(Rejection::UnknownActor)))
    })
    .await;
    assert!(matches!(a.cluster.resolve_on::<CounterApi>("b", "counter").await, Err(ClusterError::NotFound)));

    let stats = b.cluster.stats();
    assert!(stats.engine.succeeded >= 4 && stats.engine.rejected == 1, "{stats:?}");
    assert!(stats.engine.peers.iter().any(|p| p.node == "a" && p.outgoing && p.calls > 0));
}

/// Answers `n` with `n` letters. An empty answer does not encode.
struct Letters;

impl WireProtocol for Letters {
    type Message = u32;
    type Reply = String;
    const ID: u64 = 0x1E;
    const SCHEMA: u16 = 1;

    fn encode(n: u32) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&n.to_be_bytes()))
    }

    fn decode_reply(body: Bytes) -> Result<String, CallError> {
        String::from_utf8(body.into()).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for Letters {
    type Mailbox = (u32, Option<Reply<String>>);

    fn encode_reply(letters: String) -> Result<Bytes, CallError> {
        if letters.is_empty() {
            return Err(CallError::Codec("no letters".into()));
        }
        Ok(Bytes::from(letters))
    }

    fn decode(mut body: Bytes, reply: Option<Reply<String>>) -> Result<Self::Mailbox, Rejection> {
        body.try_get_u32().map(|n| (n, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

/// Keeps what each `Reply::send` returned. The reply to `HOLD` is sent with the next request's, first.
struct Writer(Arc<std::sync::Mutex<Vec<Result<(), CallError>>>>, Option<Reply<String>>);

const HOLD: u32 = 7;

impl Actor for Writer {
    type Message = (u32, Option<Reply<String>>);
    type Error = ();

    fn on_message(&mut self, (n, reply): Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if n == HOLD {
            self.1 = reply;
            return Ok(());
        }
        let mut sent = self.0.lock().unwrap();
        sent.extend(self.1.take().map(|held| held.send("held".into())));
        sent.extend(reply.map(|r| r.send("a".repeat(n as usize))));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_that_does_not_encode_fails_its_send_and_the_caller_hears_no_reply() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;
    let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
    let actor = a.system.spawn(Writer(sent.clone(), None)).start().unwrap();
    let local = a.cluster.publish::<Letters>(&actor, "letters").unwrap();
    let remote = b.cluster.resolve::<Letters>("letters").await.unwrap();

    assert_eq!(remote.request(3u32).await.unwrap(), "aaa");
    assert!(matches!(remote.request(0u32).await, Err(CallError::NoReply)));
    assert!(matches!(remote.request(2u32 << 20).await, Err(CallError::NoReply)), "larger than a frame");
    assert_eq!(remote.request(1u32).await.unwrap(), "a", "the actor outlives both");
    assert!(matches!(local.request(0u32).await, Err(CallError::NoReply)), "on the same node too");
    assert!(tokio::time::timeout(Duration::from_millis(50), remote.request(HOLD)).await.is_err());
    assert_eq!(remote.request(1u32).await.unwrap(), "a", "after the reply to a caller that gave up");
    // The caller hears of a failed send before the actor has kept its result.
    actor.lifecycle.stop();
    actor.join().await;
    let sent = sent.lock().unwrap();
    assert!(
        matches!(
            sent[..],
            [
                Ok(()),
                Err(CallError::Codec(_)),
                Err(CallError::NotSent(axon_net::SubmitReason::Oversized)),
                Ok(()),
                Err(CallError::Codec(_)),
                Ok(()), // to the caller that gave up: not an error
                Ok(())
            ]
        ),
        "{sent:?}"
    );
}

/// `1` answers with what its caller has left, in ms, or `none`; `2` is held unanswered; `3` answers whether the held
/// one is abandoned, and what it has left.
#[derive(Default)]
struct Left(Option<Reply<String>>);

impl Actor for Left {
    type Message = (u32, Option<Reply<String>>);
    type Error = ();

    fn on_message(&mut self, (n, reply): Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
        let Some(reply) = reply else { return Ok(()) };
        let shown = |reply: &Reply<String>| reply.time_left().map_or("none".into(), |d| d.as_millis().to_string());
        match n {
            2 => {
                self.0 = Some(reply);
                Ok(())
            }
            3 => {
                let held = self.0.as_ref().map(|h| format!("{} {}", h.is_abandoned(), shown(h)));
                reply.send(held.unwrap_or_else(|| "nothing held".into())).map_err(drop)
            }
            _ => {
                let left = shown(&reply);
                reply.send(left).map_err(drop)
            }
        }
    }
}

/// A handler reads the deadline that `request_within` sent, on another node and on its own, and none after a plain
/// `request`, whose caller's request timeout does not travel. Past that deadline the reply is abandoned, on both.
#[tokio::test(flavor = "multi_thread")]
async fn a_handler_sees_the_time_its_caller_has_left_only_when_the_caller_sent_a_deadline() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;
    let actor = a.system.spawn(Left::default()).start().unwrap();
    let local = a.cluster.publish::<Letters>(&actor, "left").unwrap();
    let remote = b.cluster.resolve::<Letters>("left").await.unwrap();
    for (what, to) in [("remote", &remote), ("local", &local)] {
        assert_eq!(to.request(1u32).await.unwrap(), "none", "{what}");
        let left: u64 = to.request_within(1u32, Duration::from_secs(3)).await.unwrap().parse().unwrap();
        assert!((2000..=3000).contains(&left), "{what}: {left} ms");

        let waiting = tokio::spawn({
            let to = to.clone();
            async move { to.request(2u32).await }
        });
        eventually("the plain request is held", || async { to.request(3u32).await.unwrap() == "false none" }).await;
        waiting.abort();
        let late = to.request_within(2u32, Duration::from_millis(100)).await;
        assert!(matches!(late, Err(CallError::Timeout)), "{what}: {late:?}");
        assert_eq!(to.request(3u32).await.unwrap(), "true 0", "{what}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn control_rpc_is_default_deny_and_bounded() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;

    let echo = |request: ControlRequest| async move {
        if request.body.is_empty() {
            return Err("empty".to_string());
        }
        Ok(Bytes::from(format!("{}:{}", request.peer.node_id, String::from_utf8_lossy(&request.body))))
    };
    a.cluster.serve_control("closed", AccessPolicy::default(), echo).unwrap();
    // From a thread with no tokio runtime, as an actor's handler is.
    let cluster = a.cluster.clone();
    #[expect(clippy::disallowed_methods, reason = "a thread with no tokio runtime")]
    let served = std::thread::spawn(move || cluster.serve_control("open", AccessPolicy::Cluster, echo));
    served.join().unwrap().unwrap();
    a.cluster.serve_control("only-c", AccessPolicy::Nodes(["c".to_string()].into()), echo).unwrap();

    assert_eq!(b.cluster.control("a", "open", "hi".into()).await.unwrap(), "b:hi");
    assert!(
        matches!(b.cluster.control("a", "open", Bytes::new()).await, Err(ClusterError::Control(e)) if e == "empty")
    );
    for service in ["closed", "only-c"] {
        let denied = b.cluster.control("a", service, "hi".into()).await;
        assert!(matches!(denied, Err(ClusterError::Call(CallError::Rejected(Rejection::Forbidden)))), "{denied:?}");
    }
    let huge = Bytes::from(vec![0; 65 << 10]);
    let refused = b.cluster.control("a", "open", huge).await;
    assert!(matches!(refused, Err(ClusterError::Call(CallError::Rejected(Rejection::InvalidMessage)))));

    let slow = |request: ControlRequest| async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Bytes::from(vec![b'x'; request.max_response.min(900 << 10)]))
    };
    a.cluster.serve_control("slow", AccessPolicy::Cluster, slow).unwrap();
    let calls: Vec<_> = (0..20)
        .map(|_| {
            tokio::spawn({
                let b = b.cluster.clone();
                async move { b.control("a", "slow", "go".into()).await.map(|body| body.len()) }
            })
        })
        .collect();
    for call in calls {
        let len = call.await.unwrap().expect("more calls than handlers wait their turn");
        assert!(len > 768 << 10, "the reply is bounded by the frame, not by a constant: {len}");
    }
    // A frame is counted once written, which may be after `b` has read it.
    let peers = || a.cluster.stats().engine.peers.into_iter();
    let to_b = || peers().find(|p| p.node == "b" && p.lane == axon::Lane::Live).unwrap().session;
    eventually("20 compressed replies counted", || async { to_b().tx_compressed_frames >= 20 }).await;
    assert!(to_b().tx_wire_bytes < 1 << 20, "{:?}", to_b());
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_spawn_follows_placement() {
    remote_spawn(World::memory()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_spawn_follows_placement_on_seeds() {
    remote_spawn(World::seeds().await).await
}

async fn remote_spawn(world: World) {
    let label =
        |region: &'static str| move |cfg: &mut ClusterConfig| drop(cfg.labels.insert("region".into(), region.into()));
    let client = world.node("client").await;
    let eu = world.node_with("eu-1", label("eu")).await;
    let us = [world.node_with("us-1", label("us")).await, world.node_with("us-2", label("us")).await];
    for host in [&eu, &us[0], &us[1]] {
        counter_kind(host).await;
    }
    advertised(&client, 3).await;

    let on = client.cluster.spawn_on::<CounterApi>("us-2", "counter", seed(40)).await.unwrap();
    on.send(Op::Add(2)).await.unwrap();
    assert_eq!((on.node(), number(on.request(Op::Get).await.unwrap())), ("us-2", 42));

    let in_eu = Placement::default().require("region", "eu");
    assert_eq!(client.cluster.spawn::<CounterApi>("counter", seed(0), &in_eu).await.unwrap().node(), "eu-1");
    let prefers_eu = Placement::default().prefer("region", "eu");
    assert_eq!(client.cluster.spawn::<CounterApi>("counter", seed(0), &prefers_eu).await.unwrap().node(), "eu-1");

    let on_mars = Placement::default().require("region", "mars");
    assert!(matches!(
        client.cluster.spawn::<CounterApi>("counter", seed(0), &on_mars).await,
        Err(ClusterError::NoCandidate)
    ));
    let before = client.cluster.stats();
    let unknown = client.cluster.spawn_on::<CounterApi>("client", "counter", seed(0)).await;
    assert!(matches!(unknown, Err(ClusterError::Spawn(SpawnRefusal::UnknownKind, _))));
    let bad_args = client.cluster.spawn_on::<CounterApi>("eu-1", "counter", Bytes::new()).await;
    assert!(matches!(bad_args, Err(ClusterError::Spawn(SpawnRefusal::Failed, detail)) if detail.contains("u64")));
    // One request and one refusal each, refused here or on the far node.
    let after = client.cluster.stats();
    assert_eq!((after.spawn_requests - before.spawn_requests, after.spawn_refused - before.spawn_refused), (2, 2));
    assert!(matches!(
        client.cluster.spawn_on::<OtherApi>("eu-1", "counter", seed(0)).await,
        Err(ClusterError::ProtocolMismatch)
    ));

    let in_us = Placement::default().require("region", "us").strategy(Strategy::Spread);
    let mut spread = HashSet::new();
    for _ in 0..2 {
        spread.insert(client.cluster.spawn::<CounterApi>("counter", seed(0), &in_us).await.unwrap().node().to_owned());
    }
    assert_eq!(spread.len(), 2);

    // us-2 already runs more actors than us-1, so least-loaded must keep choosing us-1 until they level.
    for _ in 0..3 {
        client.cluster.spawn_on::<CounterApi>("us-2", "counter", seed(0)).await.unwrap();
    }
    let least = in_us.clone().strategy(Strategy::LeastLoaded);
    assert_eq!(client.cluster.spawn::<CounterApi>("counter", seed(0), &least).await.unwrap().node(), "us-1");
    assert_eq!(us[1].cluster.stats().spawned, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_may_take_its_own_endpoint_down() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;
    let (cluster, id) = (a.cluster.clone(), Arc::new(std::sync::OnceLock::new()));
    let own = id.clone();
    let once = move |_: &axon_net::Identity, _: Bytes, reply: Option<axon_cluster::Reply>| {
        cluster.unserve(*own.get().expect("set before the first call"));
        reply.expect("a request").send(Bytes::from_static(b"bye")).map_err(|_| Rejection::Stopped)
    };
    id.set(a.cluster.serve("once", 0xE1, 1, once).unwrap()).unwrap();
    let asked = b.cluster.request("a", "once", 0xE1, 1, Bytes::new());
    let answer = tokio::time::timeout(Duration::from_secs(5), asked).await.expect("the handler deadlocked");
    assert_eq!(&answer.unwrap()[..], b"bye");
    assert!(b.cluster.request("a", "once", 0xE1, 1, Bytes::new()).await.is_err());
}

/// A request to one boot reaches that boot, or no boot: once the node restarted it is refused unsent, and one to the
/// new boot gets through.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_to_one_boot_never_reaches_the_next() {
    use std::sync::atomic::AtomicUsize;
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&a, 2).await;
    let echo = |served: Arc<AtomicUsize>| {
        move |_: &axon_net::Identity, body: Bytes, reply: Option<axon_cluster::Reply>| {
            served.fetch_add(1, SeqCst);
            reply.expect("a request").send(body).map_err(|_| Rejection::Stopped)
        }
    };
    let first = b.cluster.identity().incarnation;
    b.cluster.serve("echo", 0xE2, 1, echo(Arc::default())).unwrap();
    let asked = a.cluster.request_to("b", first, "echo", 0xE2, 1, Bytes::from_static(b"1")).await;
    assert_eq!(&asked.unwrap()[..], b"1");
    b.system.shutdown().await.unwrap();
    drop(b);

    let b = node("b", &discovery).await;
    let (next, served) = (b.cluster.identity().incarnation, Arc::new(AtomicUsize::new(0)));
    b.cluster.serve("echo", 0xE2, 1, echo(served.clone())).unwrap();
    eventually("the new boot is in the view", || async {
        a.cluster.view().get("b").is_some_and(|m| m.record.incarnation == next)
    })
    .await;
    let asked = a.cluster.request_to("b", next, "echo", 0xE2, 1, Bytes::from_static(b"2")).await;
    assert_eq!(&asked.unwrap()[..], b"2");
    let stale = a.cluster.request_to("b", first, "echo", 0xE2, 1, Bytes::new()).await;
    assert!(matches!(stale, Err(ClusterError::Call(CallError::StaleNode))), "{stale:?}");
    assert_eq!(served.load(SeqCst), 1, "the old boot's request ran on the new one");
    for n in [b, a] {
        n.system.shutdown().await.unwrap();
    }
}

/// A request that waits for room on a full session counts its `request_timeout` from when the session took it, not from
/// its start, so one queued behind others is not late. b answers it 2.5 s after it arrives, which is past 3 s from its
/// start and within 3 s from its admission.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_admitted_late_counts_its_timeout_from_admission() {
    use std::sync::atomic::AtomicBool;
    let discovery = MemoryDiscovery::new();
    let (to, cut) = (Arc::new(std::sync::OnceLock::new()), Arc::new(AtomicBool::new(false)));
    let hole = relay(to.clone(), cut.clone()).await;
    let b = node_with("b", discovery.clone(), |cfg| cfg.advertise = Some(hole)).await;
    to.set(b.cluster.local_addr()).unwrap();
    let a = node_with("a", discovery.clone(), |cfg| {
        // A small outbox fills soon after the socket does.
        cfg.transport.lanes.live.limits.outbound_bytes = 2 << 20;
        // An answer 2.5 s after admission fits 3 s from there, not from the start; the route is asked once.
        cfg.request_timeout = Duration::from_secs(3);
        cfg.route_ttl = Duration::from_secs(60);
    })
    .await;
    sees(&a, 2).await;
    let slow = |_: &axon_net::Identity, body: Bytes, reply: Option<axon_cluster::Reply>| {
        let reply = reply.expect("a request");
        tokio::spawn(async move {
            if !body.is_empty() {
                tokio::time::sleep(Duration::from_millis(2500)).await;
            }
            drop(reply.send(body));
        });
        Ok(())
    };
    b.cluster.serve("slow", 0xE3, 1, slow).unwrap();
    b.cluster
        .serve("sink", 0xE4, 1, |_, _, reply| {
            reply.expect("a request").send(Bytes::new()).map_err(|_| Rejection::Stopped)
        })
        .unwrap();
    let boot = b.cluster.identity().incarnation;
    a.cluster.request_to("b", boot, "slow", 0xE3, 1, Bytes::new()).await.unwrap();
    a.cluster.request_to("b", boot, "sink", 0xE4, 1, Bytes::new()).await.unwrap();

    cut.store(true, SeqCst);
    let stop = Arc::new(AtomicBool::new(false));
    let fillers: Vec<_> = (0..64)
        .map(|_| {
            let (cluster, stop) = (a.cluster.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(SeqCst) {
                    let body = Bytes::from(vec![0; 256 << 10]);
                    drop(cluster.request_to("b", boot, "sink", 0xE4, 1, body).await);
                }
            })
        })
        .collect();
    let outbox = || {
        let peers = a.cluster.stats().engine.peers;
        let live = peers.iter().find(|p| &*p.node == "b" && p.outgoing && p.lane == axon::Lane::Live);
        live.map_or(0, |p| p.session.outbound_reserved_bytes)
    };
    eventually("the outbox to b is full", || async { outbox() > (2 << 20) - (256 << 10) - 64 }).await;
    let started = tokio::time::Instant::now();
    let late = tokio::spawn({
        let cluster = a.cluster.clone();
        async move { cluster.request_to("b", boot, "slow", 0xE3, 1, Bytes::from_static(b"late")).await }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, SeqCst);
    cut.store(false, SeqCst);
    let answer = late.await.unwrap();
    assert_eq!(&answer.expect("answered within request_timeout of its admission")[..], b"late");
    assert!(started.elapsed() >= Duration::from_millis(4500), "it was not held: {:?}", started.elapsed());
    for filler in fillers {
        filler.await.unwrap();
    }
    // Answered, refused for room past their deadline or timed out: each gave its slot back.
    let e = a.cluster.stats().engine;
    assert_eq!(e.pending, 0, "{e:?}");
    assert_eq!(e.calls, e.succeeded + e.rejected + e.timed_out + e.unknown_outcomes + e.failed + e.abandoned, "{e:?}");
    for n in [b, a] {
        n.system.shutdown().await.unwrap();
    }
}

/// Requests started one after another without waiting for their answers reach the far side in that order, and each
/// holds its call slot until its answer is read; one dropped unread gives its slot back and is counted abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn several_started_requests_are_answered_in_their_order() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&a, 2).await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = {
        let seen = seen.clone();
        move |_: &axon_net::Identity, body: Bytes, reply: Option<axon_cluster::Reply>| {
            seen.lock().unwrap().push(body[0]);
            reply.expect("a request").send(body).map_err(|_| Rejection::Stopped)
        }
    };
    b.cluster.serve("log", 0xE5, 1, log).unwrap();
    let boot = b.cluster.identity().incarnation;
    let mut pending = Vec::new();
    for i in 0..16u8 {
        pending.push(a.cluster.start_request_to("b", boot, "log", 0xE5, 1, Bytes::from(vec![i])).await.unwrap());
    }
    eventually("b saw them all", || async { seen.lock().unwrap().len() == 16 }).await;
    assert_eq!(*seen.lock().unwrap(), (0..16).collect::<Vec<u8>>());
    assert!(a.cluster.stats().engine.pending >= 16, "an answer not yet read holds its slot");
    for (i, answer) in pending.into_iter().enumerate() {
        assert_eq!(&answer.await.unwrap()[..], [i as u8]);
    }
    drop(a.cluster.start_request_to("b", boot, "log", 0xE5, 1, Bytes::from_static(&[16])).await.unwrap());
    let e = a.cluster.stats().engine;
    assert_eq!((e.pending, e.abandoned), (0, 1), "{e:?}");
    assert_eq!(e.calls, e.succeeded + e.rejected + e.timed_out + e.unknown_outcomes + e.failed + e.abandoned, "{e:?}");
    // The keepalive a session sends as it comes up has been answered by now.
    assert!(a.cluster.round_trip("b").is_some_and(|rtt| rtt < Duration::from_secs(1)));
    assert_eq!(a.cluster.round_trip("nobody"), None);
    for n in [b, a] {
        n.system.shutdown().await.unwrap();
    }
}

/// A name looked up again after its node restarted finds the new boot, though the route to the old one has not
/// expired: a set's member is called through `resolve_on`, which the route cache answers.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_resolved_before_its_node_restarted_reaches_the_new_boot_on_the_next_lookup() {
    let discovery = MemoryDiscovery::new();
    // A route that outlives the test, so only its invalidation can retire it.
    let a = node_with("a", discovery.clone(), |cfg| cfg.route_ttl = Duration::from_secs(60)).await;
    let b = node("b", &discovery).await;
    sees(&a, 2).await;
    publish_counter(&b, 1, "echo");
    let old = a.cluster.resolve_on::<CounterApi>("b", "echo").await.unwrap();
    assert_eq!(number(old.request(Op::Get).await.unwrap()), 1);
    assert_eq!(
        number(a.cluster.resolve_on::<CounterApi>("b", "echo").await.unwrap().request(Op::Get).await.unwrap()),
        1
    );
    let hits = a.cluster.stats().route_hits;
    assert!(hits >= 1, "the second lookup was not answered from the cache");
    b.system.shutdown().await.unwrap();
    drop(b);

    let b = node("b", &discovery).await;
    let next = b.cluster.identity().incarnation;
    publish_counter(&b, 2, "echo");
    eventually("the new boot is in the view", || async {
        a.cluster.view().get("b").is_some_and(|m| m.record.incarnation == next && m.available())
    })
    .await;
    assert!(matches!(old.request(Op::Get).await, Err(CallError::StaleNode)));
    let misses = a.cluster.stats().route_misses;
    let new = a.cluster.resolve_on::<CounterApi>("b", "echo").await.unwrap();
    assert_eq!(new.node_incarnation(), next);
    assert_eq!(number(new.request(Op::Get).await.unwrap()), 2);
    let again = a.cluster.resolve_on::<CounterApi>("b", "echo").await.unwrap();
    assert_eq!(number(again.request(Op::Get).await.unwrap()), 2);
    assert_eq!(a.cluster.stats().route_misses, misses + 1, "the new boot was asked once, then cached");
    for n in [b, a] {
        n.system.shutdown().await.unwrap();
    }
}

/// A call whose deadline passes while the session to its node is still being set up sent nothing: retrying it is safe.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_that_runs_out_of_time_before_a_session_is_up_was_not_sent() {
    let discovery = MemoryDiscovery::new();
    // Accepts every connection and holds it, so no handshake with b ever finishes.
    let hole = relay(Arc::default(), Arc::new(std::sync::atomic::AtomicBool::new(true))).await;
    let a = node("a", &discovery).await;
    let b = node_with("b", discovery.clone(), |cfg| cfg.advertise = Some(hole)).await;
    for n in [&a, &b] {
        counter_kind(n).await;
    }
    advertised(&a, 2).await;
    let mut i = 0;
    let error = loop {
        i += 1;
        let key = format!("key-{i}");
        if let Err(e) = a.cluster.get_or_spawn::<CounterApi>("counter", &key, seed(0), &Placement::default()).await {
            break e;
        }
    };
    assert!(matches!(error, ClusterError::Call(CallError::NotSent(axon_net::SubmitReason::Deadline))), "{error:?}");
    assert_eq!(error.retry(), Retry::Later);
    for n in [a, b] {
        n.system.shutdown().await.unwrap();
    }
}

#[test]
fn every_error_says_whether_to_retry_resolve_or_stop() {
    use Retry::*;
    use axon_net::SubmitReason;
    let io = || axon_net::Error::Io(Arc::new(std::io::Error::other("refused")));
    let calls = [
        (CallError::Busy, Later),
        (CallError::NotSent(SubmitReason::Full), Later),
        (CallError::NotSent(SubmitReason::Closed), Later),
        (CallError::NotSent(SubmitReason::Deadline), Later),
        (CallError::NotSent(SubmitReason::Oversized), Never),
        (CallError::Rejected(Rejection::Full), Later),
        (CallError::Rejected(Rejection::UnknownActor), Resolve),
        (CallError::Rejected(Rejection::Stopped), Resolve),
        (CallError::StaleNode, Resolve),
        (CallError::NoRoute, Resolve),
        (CallError::Unreachable(io()), Resolve),
        (CallError::Timeout, IfIdempotent),
        (CallError::OutcomeUnknown, IfIdempotent),
        (CallError::NoReply, IfIdempotent),
        (CallError::Codec("bad".into()), Never),
        (CallError::Closed, Never),
        (CallError::Rejected(Rejection::SchemaMismatch), Never),
        (CallError::Rejected(Rejection::Forbidden), Never),
        (CallError::Rejected(Rejection::InvalidMessage), Never),
    ];
    for (error, retry) in calls {
        assert_eq!(error.retry(), retry, "{error:?}");
        let error = ClusterError::Call(error);
        assert_eq!(error.retry(), retry, "{error:?}");
    }
    let spawn = |refusal| ClusterError::Spawn(refusal, String::new());
    let clusters = [
        (spawn(SpawnRefusal::NotOwner), Resolve),
        (spawn(SpawnRefusal::Leaving), Resolve),
        (spawn(SpawnRefusal::UnknownKind), Resolve),
        (ClusterError::NoCandidate, Resolve),
        (ClusterError::NotFound, Resolve),
        (ClusterError::Ownership(OwnershipError::Lost), Resolve),
        (spawn(SpawnRefusal::Held), Later),
        (spawn(SpawnRefusal::Capacity), Later),
        (spawn(SpawnRefusal::Ownership), Later),
        (spawn(SpawnRefusal::Isolated), Later),
        (ClusterError::Discovery(DiscoveryError::Unavailable("away".into())), Later),
        (ClusterError::Discovery(DiscoveryError::Stale), Later),
        (ClusterError::Ownership(OwnershipError::Held(None)), Later),
        (ClusterError::Ownership(OwnershipError::Unavailable("away".into())), Later),
        (ClusterError::Transport(io()), Later),
        (ClusterError::Transport(axon_net::Error::Config("bad")), Never),
        (spawn(SpawnRefusal::Forbidden), Never),
        (spawn(SpawnRefusal::Failed), Never),
        (ClusterError::ProtocolMismatch, Never),
        (ClusterError::Ambiguous(2), Never),
        (ClusterError::Config("bad"), Never),
        (ClusterError::Closed, Never),
        (ClusterError::NameTaken, Never),
        (ClusterError::Control("bad".into()), Never),
        (ClusterError::System(axon::SystemError::Closed), Never),
        (ClusterError::Discovery(DiscoveryError::ReadOnly), Never),
        (ClusterError::Discovery(DiscoveryError::Invalid("bad")), Never),
        (ClusterError::Discovery(DiscoveryError::Capacity), Never),
    ];
    for (error, retry) in clusters {
        assert_eq!(error.retry(), retry, "{error:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_its_caller_gave_up_on_is_counted_in_an_outcome() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&a, 2).await;
    sees(&b, 2).await;
    publish_counter(&a, 0, "counter");
    let remote = b.cluster.resolve::<CounterApi>("counter").await.unwrap();

    assert!(tokio::time::timeout(Duration::from_millis(50), remote.request(Op::Hold)).await.is_err());
    let e = b.cluster.stats().engine;
    assert_eq!(e.pending, 0, "the abandoned call gave back its slot");
    assert_eq!(e.abandoned, 1, "{e:?}");
    assert_eq!(e.calls, e.succeeded + e.rejected + e.timed_out + e.unknown_outcomes + e.failed + e.abandoned, "{e:?}");
    for n in [b, a] {
        n.system.shutdown().await.unwrap();
    }
}
