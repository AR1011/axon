//! Lanes in the World, three acceptors under `PaxosLeases`: uploads on `Bulk` that fill every link beside a lease and
//! timed `Live` calls, one lane cut both ways while `Control` stays up, and a peer that restarts between its dials.
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use axon::{Actor, ActorHandle, ActorSystem, Context, Lane, MailboxLimits, Stream};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, MemoryDiscovery, PaxosConfig, PaxosLeases,
    Placement, Rejection, Reply, Retry, SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, Task, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::{Buf, Bytes};
use tokio::time::Instant;

const NODES: [&str; 3] = ["a", "b", "c"];
const ECHO: u64 = 0xEC;
/// An upload's body, sent in parts of `PART`.
const BODY: usize = 10_000_000;
const PART: usize = 256 << 10;

/// Adds to a keyed total and answers with it: a call on `Live`.
struct TallyApi;

impl WireProtocol for TallyApi {
    type Message = u64;
    type Reply = u64;
    const ID: u64 = 0x7A11;
    const SCHEMA: u16 = 1;

    fn encode(n: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&n.to_be_bytes()))
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("a u64".into()))
    }
}

impl ActorProtocol for TallyApi {
    type Mailbox = Add;

    fn encode_reply(n: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&n.to_be_bytes()))
    }

    fn decode(mut body: Bytes, reply: Option<Reply<u64>>) -> Result<Add, Rejection> {
        body.try_get_u64().map(|n| Add(n, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

struct Add(u64, Option<Reply<u64>>);

struct Tally(u64);

impl Actor for Tally {
    type Message = Add;
    type Error = ();

    fn on_message(&mut self, Add(n, reply): Add, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0 += n;
        drop(reply.map(|reply| reply.send(self.0)));
        Ok(())
    }
}

/// An upload whose answer is how many bytes of the pattern arrived, 0 for a body cut or changed: on `Bulk`.
struct PutApi;

impl WireProtocol for PutApi {
    type Message = ();
    type Reply = u64;
    const ID: u64 = 0x9071;
    const SCHEMA: u16 = 1;
    const LANE: Lane = Lane::Bulk;

    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(body: Bytes) -> Result<u64, CallError> {
        TallyApi::decode_reply(body)
    }
}

impl ActorProtocol for PutApi {
    type Mailbox = Put;

    fn encode_reply(n: u64) -> Result<Bytes, CallError> {
        TallyApi::encode_reply(n)
    }

    fn decode(_: Bytes, reply: Option<Reply<u64>>) -> Result<Put, Rejection> {
        reply.map(Put::Take).ok_or(Rejection::InvalidMessage)
    }
}

enum Put {
    Take(Reply<u64>),
    Read,
}

/// Reads each body to its end in a job.
struct Sink;

impl Actor for Sink {
    type Message = Put;
    type Error = ();

    fn on_message(&mut self, put: Put, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let Put::Take(reply) = put else { return Ok(()) };
        let Some(mut upload) = reply.upload() else { return reply.send(0).map_err(drop) };
        let read = async move {
            let (mut at, mut whole) = (0, true);
            while let Some(part) = upload.next().await {
                let Ok(part) = part else {
                    whole = false;
                    break;
                };
                whole &= part.iter().enumerate().all(|(i, b)| *b == ((at + i) % 251) as u8);
                at += part.len();
            }
            drop(reply.send(if whole { at as u64 } else { 0 }));
            Put::Read
        };
        cx.submit_future(read).map(drop).map_err(drop)
    }
}

/// `len` bytes of the pattern.
struct Pattern {
    at: usize,
    len: usize,
}

impl Stream for Pattern {
    type Item = Result<Bytes, CallError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        if self.at >= self.len {
            return Poll::Ready(None);
        }
        let end = self.len.min(self.at + PART);
        let part: Vec<u8> = (self.at..end).map(|i| (i % 251) as u8).collect();
        self.at = end;
        Poll::Ready(Some(Ok(part.into())))
    }
}

#[derive(Clone)]
struct Node {
    cluster: Cluster,
    _system: ActorSystem,
    _sink: ActorHandle<Put>,
}

async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let transport = host.transport(SharedSecret::new("at-least-16-bytes-long").unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    let mut paxos = PaxosConfig::new(NODES);
    paxos.store = Some(Arc::new(host.disk().dir("leases")));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    cluster.kind::<TallyApi>("tally").register(|_: &SpawnContext, _| Ok(Tally(0))).await.unwrap();
    let jobs = MailboxLimits { jobs: 16, ..MailboxLimits::default() };
    let sink = system.spawn(Sink).mailbox(jobs).start().unwrap();
    cluster.publish::<PutApi>(&sink, "sink").unwrap();
    cluster
        .serve("echo", ECHO, 1, |_, body, reply| {
            drop(reply.map(|reply| reply.send(body)));
            Ok(())
        })
        .unwrap();
    Node { cluster, _system: system, _sink: sink }
}

/// Three nodes that each list all three with the kind, past the acceptors' warm-up.
fn world(cfg: WorldConfig) -> World<Node> {
    let (mut w, discovery) = (World::new(cfg), MemoryDiscovery::new());
    for name in NODES {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    w.run_until("every member with the kind, warm", Duration::from_secs(30), |w| {
        let ready = |n: &Node| {
            !n.cluster.warming() && n.cluster.view().members.iter().filter(|m| m.record.has_kind("tally")).count() == 3
        };
        w.nodes().count() == 3 && w.nodes().all(|(_, n)| ready(n))
    });
    w
}

/// The node running the key's actor, as `from` finds it, and its epoch.
fn owner(w: &mut World<Node>, from: &str, key: &str) -> Option<(String, Option<u64>)> {
    let key = key.to_owned();
    let found = w.block_on(from, move |n| async move {
        n.cluster.get_or_spawn::<TallyApi>("tally", &key, Bytes::new(), &Placement::default()).await
    });
    found.ok()?.ok().map(|r| (r.node().to_owned(), r.epoch()))
}

/// A key started on `node`, and its epoch.
fn key_on(w: &mut World<Node>, node: &str) -> (String, Option<u64>) {
    (0..)
        .map(|k| format!("k{k}"))
        .find_map(|key| owner(w, "a", &key).filter(|(on, _)| on == node).map(|(_, epoch)| (key, epoch)))
        .expect("some key")
}

/// `n` added to the key's total from a.
fn add(w: &mut World<Node>, key: &str, n: u64) -> Result<u64, ClusterError> {
    let key = key.to_owned();
    let added = w.block_on("a", move |a| async move {
        a.cluster.keyed::<TallyApi>("tally", &key, Bytes::new(), &Placement::default()).request(n).await
    });
    added.expect("a is up")
}

/// One upload from a to `to`'s sink: the bytes that arrived whole.
fn upload(w: &mut World<Node>, to: &'static str) -> Result<u64, ClusterError> {
    let put = w.block_on("a", move |a| async move {
        let sink = a.cluster.resolve_on::<PutApi>(to, "sink").await?;
        Ok(sink.upload((), Pattern { at: 0, len: 1 << 20 }).await?)
    });
    put.expect("a is up")
}

/// How long the lane `from` dials `to` on has had no session, while `Control` is up.
fn down_ms(w: &World<Node>, from: &str, to: &str, lane: Lane) -> Option<u64> {
    let row =
        |n: &Node| n.cluster.stats().engine.peers.into_iter().find(|p| p.node == to && p.outgoing && p.lane == lane);
    w.read(from, |n| row(n)?.down_ms).flatten()
}

/// What `node` put on its timeline, oldest first.
fn told(w: &mut World<Node>, node: &str) -> Vec<(&'static str, String, String)> {
    let told = w.read(node, |n| n.cluster.events().since(0, usize::MAX)).unwrap_or_default();
    told.into_iter().map(|e| (e.kind, e.subject, e.detail)).collect()
}

/// No node lost a lease or isolated itself.
fn leases_held(w: &mut World<Node>, seed: u64) {
    for node in NODES {
        let lost: Vec<_> =
            told(w, node).into_iter().filter(|(kind, ..)| ["lease_lost", "isolated"].contains(kind)).collect();
        assert!(lost.is_empty(), "seed {seed}: {node} {lost:?}\n{}", w.trace_tail(40));
    }
}

/// Uploads of `BODY` from a to `to` until `run` has passed: how many arrived whole.
fn uploads(w: &mut World<Node>, to: &'static str, run: Duration) -> Task<usize> {
    w.spawn_on("a", move |a| async move {
        let sink = a.cluster.resolve_on::<PutApi>(to, "sink").await.unwrap();
        let (end, mut whole) = (Instant::now() + run, 0);
        while Instant::now() < end {
            let put = sink.upload((), Pattern { at: 0, len: BODY }).await;
            assert_eq!(put.as_ref().ok(), Some(&(BODY as u64)), "an upload to {to}: {put:?}");
            whole += 1;
        }
        whole
    })
}

#[test]
fn bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast() {
    check_seeds(seeds(0..4), |seed| {
        // A 20 ms round trip at 2 MiB/s on every link: four uploads fill it.
        let wire = Wire { latency: Duration::from_millis(10), bandwidth: 2 << 20, ..Wire::default() };
        // The calls and uploads run for a minute, and each task is joined at its end.
        let limit = Duration::from_secs(180);
        let mut w = world(WorldConfig { seed, wire, limit, ..WorldConfig::default() });
        let (key, epoch) = key_on(&mut w, "a");
        let no_bulk = |n: &Node| n.cluster.stats().engine.peers.iter().all(|p| p.lane != Lane::Bulk);
        assert!(
            NODES.iter().all(|node| w.read(node, no_bulk).unwrap()),
            "seed {seed}: a Bulk session before a Bulk call"
        );

        let run = Duration::from_secs(60);
        let puts: Vec<_> = ["b", "b", "c", "c"].into_iter().map(|to| uploads(&mut w, to, run)).collect();
        let timed = w.spawn_on("a", move |a| async move {
            let (end, mut took) = (Instant::now() + run, Vec::new());
            while Instant::now() < end {
                let sent = Instant::now();
                let echoed = a.cluster.request("b", "echo", ECHO, 1, Bytes::from(vec![7; 256])).await;
                took.push(echoed.map(|_| sent.elapsed()));
                tokio::time::sleep_until(sent + Duration::from_millis(50)).await;
            }
            took
        });
        let took = w.join(timed).unwrap();
        let whole: Vec<usize> = puts.into_iter().map(|put| w.join(put).unwrap()).collect();
        assert!(whole.iter().all(|n| *n > 0), "seed {seed}: uploads {whole:?}");
        let failed: Vec<_> = took.iter().filter_map(|t| t.as_ref().err()).collect();
        assert!(failed.is_empty(), "seed {seed}: Live calls failed {failed:?}\n{}", w.trace_tail(40));
        let mut took: Vec<Duration> = took.into_iter().flatten().collect();
        took.sort_unstable();
        let p99 = took[took.len() * 99 / 100];
        assert!(p99 <= Duration::from_millis(70), "seed {seed}: Live p99 {p99:?}, max {:?}", took.last());
        assert_eq!(owner(&mut w, "b", &key), Some(("a".to_owned(), epoch)), "seed {seed}: {key} moved");
        leases_held(&mut w, seed);
        println!("world digest bulk_saturating seed {seed}: {:016x}, uploads {whole:?}, p99 {p99:?}", w.digest());
    })
    .unwrap();
}

/// `lane` from a to b cut both ways for 10 s while b holds a key: the key stays, a's calls on the lane fail
/// `Retry::Later` once a sees it down, its other calls answer, and a names the lane down once and back once.
fn one_lane_cut(seed: u64, lane: Lane) {
    let mut w = world(WorldConfig { seed, ..WorldConfig::default() });
    let (key, epoch) = key_on(&mut w, "b");
    assert_eq!(add(&mut w, &key, 1).ok(), Some(1), "seed {seed}");
    assert_eq!(upload(&mut w, "b").ok(), Some(1 << 20), "seed {seed}");
    let call = |w: &mut World<Node>, lane| match lane {
        Lane::Live => add(w, &key, 0).map(drop),
        _ => upload(w, "b").map(drop),
    };

    let at = w.now();
    w.cut_lane(&["a"], &["b"], lane);
    w.cut_lane(&["b"], &["a"], lane);
    w.run_until("a sees the lane down", Duration::from_secs(10), |w| down_ms(w, "a", "b", lane).is_some());
    let refused = call(&mut w, lane);
    assert!(refused.as_ref().is_err_and(|e| e.retry() == Retry::Later), "seed {seed}: {lane:?} call {refused:?}");
    let other = if lane == Lane::Live { Lane::Bulk } else { Lane::Live };
    assert!(call(&mut w, other).is_ok(), "seed {seed}: a {other:?} call while {lane:?} is cut");
    w.run_for((at + Duration::from_secs(10)).saturating_sub(w.now()));
    let reachable = w.read("a", |a| a.cluster.view().get("b").is_some_and(|m| m.reachable));
    assert_eq!(reachable, Some(true), "seed {seed}: b unreachable with only {lane:?} cut");
    assert!(down_ms(&w, "a", "b", lane).is_some_and(|ms| ms > 0), "seed {seed}");

    w.heal();
    w.run_until("a has the lane back", Duration::from_secs(20), |w| down_ms(w, "a", "b", lane).is_none());
    assert!(call(&mut w, lane).is_ok(), "seed {seed}: the first {lane:?} call after the heal");
    let named = format!("{lane:?}");
    let lanes: Vec<_> = told(&mut w, "a").into_iter().filter(|(kind, ..)| kind.starts_with("lane_")).collect();
    let once = [("lane_down", "b".to_owned(), named.clone()), ("lane_up", "b".to_owned(), named)];
    assert_eq!(lanes, once, "seed {seed}");
    assert_eq!(owner(&mut w, "c", &key), Some(("b".to_owned(), epoch)), "seed {seed}: {key} moved");
    leases_held(&mut w, seed);
    println!("world digest one_lane_cut {lane:?} seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_dead_bulk_lane_fails_only_its_calls_and_the_peer_stays_reachable() {
    check_seeds(seeds(0..8), |seed| one_lane_cut(seed, Lane::Bulk)).unwrap();
}

#[test]
fn a_node_with_a_dead_live_lane_keeps_its_keys_and_its_live_calls_retry_later() {
    check_seeds(seeds(0..8), |seed| one_lane_cut(seed, Lane::Live)).unwrap();
}

/// b restarts while a's `Control` is up to it and before a's first `Live` dial, which comes up to a seeded 1.5 s
/// later: a call built for the old boot never reaches the new one, and a call that looks b up again does, on a `Live`
/// session to the new boot.
#[test]
fn a_peer_that_restarts_between_its_lanes_dials_is_called_as_its_new_boot_only() {
    check_seeds(seeds(0..8), |seed| {
        let mut w = world(WorldConfig { seed, ..WorldConfig::default() });
        let live = |w: &World<Node>| {
            let row =
                |n: &Node| n.cluster.stats().engine.peers.into_iter().find(|p| p.node == "b" && p.lane == Lane::Live);
            w.read("a", |n| row(n).map(|p| p.incarnation)).flatten()
        };
        assert_eq!(live(&w), None, "seed {seed}: a Live session before any call");
        let old = w.read("b", |b| b.cluster.identity().incarnation).expect("b is up");
        w.crash("b", Crash::Process);
        w.restart("b");
        let wait = w.pick(0..1500);
        w.run_for(Duration::from_millis(wait));
        let stale = w.block_on("a", move |a| async move {
            a.cluster.request_to("b", old, "echo", ECHO, 1, Bytes::from_static(b"old")).await
        });
        assert!(stale.as_ref().is_ok_and(Result::is_err), "seed {seed}: the old boot's call {stale:?}");

        let mut fresh = None;
        for _ in 0..100 {
            let asked = w.block_on("a", |a| async move { a.cluster.request("b", "echo", ECHO, 1, Bytes::new()).await });
            if asked.as_ref().is_ok_and(Result::is_ok) {
                fresh = w.read("b", |b| b.cluster.identity().incarnation);
                break;
            }
            w.run_for(Duration::from_millis(200));
        }
        assert!(fresh.is_some_and(|new| new != old), "seed {seed}: no call reached the new boot");
        assert_eq!(live(&w), fresh, "seed {seed}: a's Live session is to another boot");
        println!("world digest restart_between_dials seed {seed}: {:016x}, waited {wait} ms, {stale:?}", w.digest());
    })
    .unwrap();
}
