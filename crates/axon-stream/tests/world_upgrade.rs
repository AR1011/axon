//! A rolling upgrade of five nodes, one at a time, under the shipped timings: each drains with
//! `LeaveReason::Maintenance` and comes back as the next version, which speaks one more wire version and serves its
//! protocols at schema 2 beside schema 1. Throughout, every node calls keyed actors under `PaxosLeases` and a published
//! actor, each at its own version's schema, and followers on the leaves read a stream relayed from the origin.
//!
//! No call is lost for a caller that retries as `err.retry()` says, no key is out for more than a lease and a tick,
//! no follower is told to reset, and what was in flight is gone at the end.
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::{ActorSystem, LeaveReason};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, Placement, Rejection, Reply, Retry, SeedServer, SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use axon_stream::{Cursor, DeliveryPolicy, StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions};
use axon_testkit::world::{Crash, Host, Task, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::{Buf, BufMut, Bytes, BytesMut};

mod common;
use common::{Call, Echo, SECRET, keys, role, seeded};

const NODES: [&str; 5] = ["a", "b", "c", "d", "e"];
const LEAVES: [&str; 3] = ["c", "d", "e"];
/// `PaxosConfig::new`'s lease.
const LEASE: Duration = Duration::from_secs(6);
/// The cluster's keepalive, 1 s: how late a caller may see that a key moved.
const TICK: Duration = Duration::from_secs(1);

/// The wire versions this build speaks, and the next build's: one more.
fn versions(upgraded: bool) -> RangeInclusive<u16> {
    let now = axon_net::VERSIONS;
    if upgraded { *now.start()..=*now.end() + 1 } else { now }
}

#[derive(Clone)]
struct Node {
    system: ActorSystem,
    cluster: Cluster,
    stream: StreamNode,
    upgraded: bool,
    _seed: Arc<SeedServer>,
}

/// The old build's contract. The new one's, `V2`, has the same ID and encodes differently, so a body reaching the
/// wrong decoder is refused, not misread.
struct V1;
struct V2;

fn be(n: u64) -> Result<Bytes, CallError> {
    Ok(Bytes::copy_from_slice(&n.to_be_bytes()))
}

fn from_be(mut body: Bytes) -> Option<u64> {
    (body.len() == 8).then(|| body.get_u64())
}

fn le(n: u64) -> Result<Bytes, CallError> {
    let mut out = BytesMut::with_capacity(9);
    out.put_u8(2);
    out.put_u64_le(n);
    Ok(out.freeze())
}

fn from_le(mut body: Bytes) -> Option<u64> {
    (body.len() == 9 && body.get_u8() == 2).then(|| body.get_u64_le())
}

macro_rules! schema {
    ($t:ident, $schema:literal, $encode:ident, $decode:ident) => {
        impl WireProtocol for $t {
            type Message = u64;
            type Reply = u64;
            const ID: u64 = 0xEC;
            const SCHEMA: u16 = $schema;

            fn encode(n: u64) -> Result<Bytes, CallError> {
                $encode(n)
            }

            fn decode_reply(body: Bytes) -> Result<u64, CallError> {
                $decode(body).ok_or(CallError::Codec("not this schema".into()))
            }
        }

        impl ActorProtocol for $t {
            type Mailbox = Call;

            fn encode_reply(n: u64) -> Result<Bytes, CallError> {
                $encode(n)
            }

            fn decode(body: Bytes, reply: Option<Reply<u64>>) -> Result<Call, Rejection> {
                $decode(body).map(|n| (n, reply)).ok_or(Rejection::InvalidMessage)
            }
        }
    };
}
schema!(V1, 1, be, from_be);
schema!(V2, 2, le, from_le);

fn key() -> StreamKey {
    StreamKey::new("prices", "eur")
}

/// Every node is a seed and an acceptor. a is the stream's origin, b relays it, and the leaves pull it from b, or
/// from a while b is away. Only what the guide asks for with seeds is set, and what the upgrade changes.
async fn boot(host: Host, upgraded: bool) -> Node {
    let mut transport = host.transport(SharedSecret::new(SECRET).unwrap());
    transport.versions = versions(upgraded);
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let (me, upstream) = match host.name() {
        "a" => ("origin", vec![]),
        "b" => ("relay", vec![role("origin")]),
        _ => ("leaf", vec![role("relay"), role("origin")]),
    };
    let (cluster, seed) = seeded(&host, &system, &transport, &NODES, me).await;
    let echo = system.spawn(Echo).start().unwrap();
    cluster.publish::<V1>(&echo, "ping").unwrap();
    let account = |_: &SpawnContext, _: Bytes| Ok(Echo);
    if upgraded {
        cluster.publish::<V2>(&echo, "ping").unwrap();
        cluster.kind::<V2>("account").also::<V1>().register(account).await.unwrap();
    } else {
        cluster.kind::<V1>("account").register(account).await.unwrap();
    }
    let stream = StreamNode::start(&cluster, StreamNodeConfig::new(upstream)).await.unwrap();
    if me == "origin" {
        stream.open_stream(&key(), u64::from(host.boot()), DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    Node { system, cluster, stream, upgraded, _seed: seed }
}

/// The longest a caller waited for an answer, what it called and when it sent it.
type Worst = Rc<RefCell<(Duration, String, Duration)>>;

/// Calls until it has an answer, retrying anything `retry()` does not call hopeless, as an application does: the echo
/// is idempotent. Panics on a wrong answer or a `Never`.
async fn answered(what: &str, mut call: impl AsyncFnMut() -> Result<u64, axon_cluster::ClusterError>, id: u64) {
    loop {
        match call().await {
            Ok(got) => return assert_eq!(got, id, "{what}: another call's answer"),
            Err(e) if e.retry() == Retry::Never => panic!("{what}: {e:?}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Each key and the published actor, called from `n` at its build's schema until `stop`.
async fn traffic<P>(n: Node, from: String, stop: Rc<Cell<bool>>, worst: Worst, ops: Rc<Cell<u64>>)
where
    P: WireProtocol<Message = u64, Reply = u64>,
{
    let start = tokio::time::Instant::now();
    let mut calls = Vec::new();
    for (i, k) in keys().into_iter().enumerate() {
        let keyed = n.cluster.keyed::<P>("account", &k, Bytes::new(), &Placement::default());
        let (stop, worst, ops, from) = (stop.clone(), worst.clone(), ops.clone(), from.clone());
        calls.push(tokio::task::spawn_local(async move {
            let mut id = (i as u64) << 32;
            while !stop.get() {
                id += 1;
                let sent = start.elapsed();
                answered(&format!("{k} from {from}"), async || keyed.request(id).await, id).await;
                let took = start.elapsed() - sent;
                worst.replace_with(|w| w.clone().max((took, format!("{k} from {from}"), sent)));
                ops.set(ops.get() + 1);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }));
    }
    let (cluster, mut id) = (n.cluster.clone(), 0u64);
    while !stop.get() {
        id += 1;
        let sent = start.elapsed();
        // Any node that speaks this build's schema will do: during the roll, a new build's calls go to new nodes.
        let call = async || {
            let (found, failed) = cluster.resolve_all::<P>("ping").await;
            match found.get(id as usize % found.len().max(1)) {
                Some(remote) => remote.request(id).await.map_err(Into::into),
                None => Err(failed.into_iter().next().map_or(axon_cluster::ClusterError::NotFound, |(_, e)| e)),
            }
        };
        answered(&format!("ping from {from}"), call, id).await;
        let took = start.elapsed() - sent;
        worst.replace_with(|w| w.clone().max((took, format!("ping from {from}"), sent)));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for call in calls {
        call.await.unwrap();
    }
}

fn start_traffic(w: &mut World<Node>, on: &str, stop: &Rc<Cell<bool>>, worst: &Worst, ops: &Rc<Cell<u64>>) -> Task<()> {
    let (from, stop, worst, ops) = (on.to_owned(), stop.clone(), worst.clone(), ops.clone());
    w.spawn_on(on, move |n| async move {
        if n.upgraded {
            traffic::<V2>(n, from, stop, worst, ops).await
        } else {
            traffic::<V1>(n, from, stop, worst, ops).await
        }
    })
}

#[derive(Default)]
struct Seen {
    /// The payload of a's next record: records are numbered across its boots.
    next: Cell<u64>,
    /// Per leaf, one past the highest record its follower got.
    followed: RefCell<BTreeMap<String, u64>>,
    wrong: RefCell<Option<String>>,
}

/// A record every 10 ms from a, for as long as it runs.
fn keep_publishing(w: &mut World<Node>, seen: &Rc<Seen>) {
    let seen = seen.clone();
    drop(w.spawn_on("a", move |n| async move {
        loop {
            let next = seen.next.get();
            if n.stream.publish(&key(), vec![Bytes::copy_from_slice(&next.to_be_bytes())]).is_ok() {
                seen.next.set(next + 1);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }));
}

/// A `Live` follower on `on`, until its node goes: it never ends, a batch always follows the one before it within its
/// generation, and a `Ready` only moves to a newer generation.
fn follow(w: &mut World<Node>, on: &str, seen: &Rc<Seen>) {
    let (seen, on) = (seen.clone(), on.to_owned());
    drop(w.spawn_on(&on.clone(), move |n| async move {
        let mut sub = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
        let mut at: Option<Cursor> = None;
        let wrong = |what: String| {
            seen.wrong.borrow_mut().get_or_insert(format!("the follower on {on}: {what}"));
        };
        while let Some(event) = sub.next().await {
            match event {
                StreamEvent::Ready { start: Some(start) } => {
                    if at.is_some_and(|at| start != at && start.generation <= at.generation) {
                        wrong(format!("Ready at {start:?} after {at:?}"));
                    }
                    at = Some(start);
                }
                StreamEvent::Batch(batch) => {
                    let (g, first) = (batch.generation(), batch.first());
                    if let Some(at) = at.filter(|at| (g, first) != (at.generation, at.sequence + 1)) {
                        wrong(format!("a gap or a repeat: {g}/{first} after {at:?}"));
                    }
                    at = Some(Cursor { generation: g, sequence: first + batch.len() as u64 - 1 });
                    let last = batch.records().iter().map(|r| (&r[..]).get_u64()).max().map_or(0, |l| l + 1);
                    let mut followed = seen.followed.borrow_mut();
                    let past = followed.entry(on.clone()).or_default();
                    *past = (*past).max(last);
                }
                StreamEvent::Ready { start: None } | StreamEvent::OriginLost | StreamEvent::CaughtUp { .. } => {}
                other => wrong(format!("{other:?}")),
            }
        }
    }));
}

/// Waits until every node sees all five, each advertising the kind at the schemas of its build, and no acceptor warms.
fn settled(w: &mut World<Node>, upgraded: &Rc<RefCell<BTreeSet<String>>>) {
    let up = upgraded.borrow().clone();
    let schemas = |m: &str| Some(if up.contains(m) { vec![2, 1] } else { vec![1] });
    w.run_until("five members at their builds' schemas, and warm", Duration::from_secs(40), |w| {
        w.nodes().count() == NODES.len()
            && w.nodes().all(|(_, n)| {
                let view = n.cluster.view();
                let at = |name: &str| view.get(name).and_then(|m| m.record.kind_schemas("account"));
                !n.cluster.warming() && NODES.iter().all(|m| at(m) == schemas(m))
            })
    });
}

/// Each session runs the highest version both ends speak: the next one between two upgraded nodes.
fn sessions(w: &World<Node>, seed: u64) {
    let (old, new) = (*versions(false).end(), *versions(true).end());
    for (name, n) in w.nodes() {
        let view = n.cluster.view();
        for peer in n.cluster.stats().engine.peers {
            let current = view.get(&peer.node).is_some_and(|m| m.record.incarnation == peer.incarnation);
            let both = n.upgraded && w.nodes().any(|(other, o)| other == peer.node && o.upgraded);
            let want = if both { new } else { old };
            assert!(!current || peer.version == want, "seed {seed}: {name} to {} runs {}", peer.node, peer.version);
        }
    }
}

fn roll(seed: u64) {
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    let upgraded = Rc::new(RefCell::new(BTreeSet::<String>::new()));
    for name in NODES {
        let upgraded = upgraded.clone();
        w.add(name, move |host| {
            let new = upgraded.borrow().contains(host.name());
            boot(host, new)
        });
    }
    settled(&mut w, &upgraded);
    let seen = Rc::new(Seen::default());
    keep_publishing(&mut w, &seen);
    for leaf in LEAVES {
        follow(&mut w, leaf, &seen);
    }
    let (worst, ops) = (Worst::default(), Rc::new(Cell::new(0)));
    let mut running: BTreeMap<&str, (Rc<Cell<bool>>, Task<()>)> = BTreeMap::new();
    for name in NODES {
        let stop = Rc::new(Cell::new(false));
        running.insert(name, (stop.clone(), start_traffic(&mut w, name, &stop, &worst, &ops)));
    }
    w.run_for(Duration::from_secs(5));

    let mut order = NODES.to_vec();
    for i in (1..order.len()).rev() {
        let j = w.pick(0..i as u64 + 1) as usize;
        order.swap(i, j);
    }
    for name in order {
        // The node's own callers stop first, as an application's do before it drains.
        let (stop, calls) = running.remove(name).expect("running");
        stop.set(true);
        w.join(calls).expect("its callers end before it drains");
        let left = w.block_on(name, |n| async move { n.system.shutdown_with_reason(LeaveReason::Maintenance).await });
        assert!(matches!(left, Ok(Ok(_))), "seed {seed}: {name} did not drain");
        w.crash(name, Crash::Process);
        upgraded.borrow_mut().insert(name.to_owned());
        w.restart(name);
        settled(&mut w, &upgraded);
        sessions(&w, seed);
        let stop = Rc::new(Cell::new(false));
        running.insert(name, (stop.clone(), start_traffic(&mut w, name, &stop, &worst, &ops)));
        if name == "a" {
            keep_publishing(&mut w, &seen);
        }
        if LEAVES.contains(&name) {
            follow(&mut w, name, &seen);
        }
        let between = Duration::from_millis(w.pick(2000..8000));
        w.run_for(between);
    }

    // Every follower gets records published after the last node came back.
    let next = seen.next.get();
    w.run_until("every leaf follows a record published after the roll", Duration::from_secs(20), |_| {
        let followed = seen.followed.borrow();
        LEAVES.iter().all(|l| followed.get(*l).is_some_and(|f| *f > next))
    });
    for (_, (stop, calls)) in std::mem::take(&mut running) {
        stop.set(true);
        w.join(calls).expect("the callers end");
    }
    assert_eq!(*seen.wrong.borrow(), None, "seed {seed}\n{}", w.trace_tail(40));
    let (late, what, sent) = worst.borrow().clone();
    assert!(
        late <= LEASE + TICK,
        "seed {seed}: {what} sent at {sent:?} was answered {late:?} later\n{}",
        w.trace_tail(60)
    );
    assert!(ops.get() > 1000, "seed {seed}: only {} keyed calls", ops.get());
    sessions(&w, seed);
    // Nothing in flight, each key running once, and no session refused for its version.
    let version = axon_net::HANDSHAKE_FAILURE_REASONS.iter().position(|r| *r == "version").expect("a reason");
    w.run_until("nothing in flight and each key running once", Duration::from_secs(10), |w| {
        let stats: Vec<_> = w.nodes().map(|(_, n)| n.cluster.stats()).collect();
        stats.iter().all(|s| s.engine.pending == 0) && stats.iter().map(|s| s.spawned).sum::<usize>() == keys().len()
    });
    for (name, n) in w.nodes() {
        let refused = n.cluster.stats().engine.listener.handshake_failures[version];
        assert_eq!(refused, 0, "seed {seed}: {name} refused a session for its version");
    }
    println!(
        "world digest upgrade seed {seed}: worst {late:?} ({what}), {} keyed calls: {:016x}",
        ops.get(),
        w.digest()
    );
}

#[test]
fn a_rolling_upgrade_loses_no_call_holds_no_key_past_a_lease_and_resets_no_follower() {
    // A seed takes about 6 s in a debug build; `SEEDS=0..512 scripts/check.sh sim` runs the rest.
    check_seeds(seeds(0..16), roll).unwrap();
}
