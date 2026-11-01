//! A stream tree of real nodes on the World's virtual time: the origin crashes and returns as a new generation, and a
//! live follower on the leaf is told of the loss, then sees the new generation from its record 1, each record once and
//! in order, through a one-way cut, and soon after the origin is back although its relay's backoff had grown.
//!
//! Then the shipped timings under the faults that exercise them. The real-fault rig found a redial backoff whose 30 s
//! cap held calls for half a minute after a heal, which no test ran because each shortened it (spec/fault-rig.md §
//! Findings fixed). After each fault, every node's calls to every key answer and a live follower gets
//! records published since, within a bound built from the documented defaults.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{
    Cluster, ClusterConfig, Placement, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig, SpawnContext,
};
use axon_net::SharedSecret;
use axon_stream::{Cursor, DeliveryPolicy, StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions};
use axon_testkit::world::{Crash, Host, Task, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::{Buf, Bytes};
use parking_lot::Mutex;

mod common;
use common::{Echo, EchoApi, SECRET, SEED_PORT, keys, role, seeded};

#[derive(Clone)]
struct Node {
    _system: ActorSystem,
    cluster: Cluster,
    stream: StreamNode,
    _seed: Option<Arc<SeedServer>>,
}

fn key() -> StreamKey {
    StreamKey::new("prices", "eur")
}

async fn boot(host: Host) -> Node {
    let (me, upstream) = match host.name() {
        "o" => ("origin", vec![]),
        "r" => ("relay", vec![role("origin")]),
        _ => ("leaf", vec![role("relay")]),
    };
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    // r and l are the seeds, so the origin's crash does not take discovery with it.
    let seed = if me == "origin" {
        None
    } else {
        let at = SocketAddr::new(host.ip(), SEED_PORT);
        Some(Arc::new(SeedServer::bind(SeedServerConfig::new(at, &transport)).await.unwrap()))
    };
    let entries = ["r", "l"].map(|n| SocketAddr::new(host.ip_of(n), SEED_PORT).to_string());
    let discovery = SeedDiscovery::start(SeedConfig::new(entries, &transport)).await.unwrap();
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    cfg.labels = role(me);
    // With seeds, as guide/cluster.md § Seed servers asks.
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let stream = StreamNode::start(&cluster, StreamNodeConfig::new(upstream)).await.unwrap();
    if me == "origin" {
        // Each boot of the origin is a new generation.
        stream.open_stream(&key(), u64::from(host.boot()), DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    Node { _system: system, cluster, stream, _seed: seed }
}

enum Got {
    Ready(Option<Cursor>),
    Batch { generation: u64, first: u64, records: Vec<Bytes> },
    OriginLost,
    Terminal(String),
}

type Events = Arc<Mutex<Vec<Got>>>;

/// Whether the leaf has a position in `generation`.
fn follows(got: &Events, generation: u64) -> bool {
    got.lock().iter().any(|e| match e {
        Got::Ready(Some(at)) => at.generation == generation,
        Got::Batch { generation: g, .. } => *g == generation,
        _ => false,
    })
}

/// (generation, sequence) -> payload, recorded when publish acknowledges it.
type Published = Arc<Mutex<BTreeMap<(u64, u64), Bytes>>>;

fn publish(w: &mut World<Node>, published: &Published, count: u64) -> Task<()> {
    let published = published.clone();
    w.spawn_on("o", move |n| async move {
        for i in 0..count {
            let payload = Bytes::from(i.to_be_bytes().to_vec());
            if let Ok(at) = n.stream.publish(&key(), vec![payload.clone()]) {
                published.lock().insert((at.generation, at.sequence), payload);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
}

/// Three members, a `Live` follower on the leaf, and generation 1 published until the origin goes.
fn start(seed: u64) -> (World<Node>, Events, Published) {
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in ["o", "r", "l"] {
        w.add(name, boot);
    }
    w.run_until("three members", Duration::from_secs(30), |w| {
        w.nodes().count() == 3 && w.nodes().all(|(_, n)| n.cluster.view().members.len() == 3)
    });
    let got = Events::default();
    let g = got.clone();
    drop(w.spawn_on("l", move |n| async move {
        // Live follows the key: a restart is a new Ready and a lost origin an OriginLost, never an end.
        let mut sub = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
        while let Some(event) = sub.next().await {
            let item = match event {
                StreamEvent::Batch(b) => {
                    Got::Batch { generation: b.generation(), first: b.first(), records: b.records().to_vec() }
                }
                StreamEvent::Ready { start } => Got::Ready(start),
                StreamEvent::OriginLost => Got::OriginLost,
                // Both ResetRequired and End are terminal.
                other if other.is_terminal() => Got::Terminal(format!("{other:?}")),
                _ => continue,
            };
            g.lock().push(item);
        }
    }));
    let published = Published::default();
    drop(publish(&mut w, &published, u64::MAX));
    w.run_until("the leaf has a record of generation 1", Duration::from_secs(5), |_| {
        got.lock().iter().any(|e| matches!(e, Got::Batch { generation: 1, .. }))
    });
    (w, got, published)
}

fn crash_origin(w: &mut World<Node>) {
    // Process: peers read a reset. Machine: silence, no FIN, no RST.
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.crash("o", how);
}

/// Generation 2's first `early` records go out from the origin's boot on, before anything follows it.
fn restart_origin(w: &mut World<Node>, published: &Published, early: u64) {
    w.restart("o");
    let early = publish(w, published, early);
    w.join(early).expect("the origin stays up");
}

/// Waits for generation 2's last record, then checks what the leaf saw: each batch continues from the last `Ready` or
/// batch, a `Ready` moves only to a newer generation, the origin's loss is told once and between the generations, and
/// generation 2 arrives whole, from its record 1.
fn check(w: &mut World<Node>, got: &Events, published: &Published, seed: u64) {
    let last = published.lock().keys().filter(|(g, _)| *g == 2).max().copied();
    let last = last.expect("generation 2 published");
    // Or has ended, which the checks below report.
    w.run_until("the leaf has generation 2 up to the last record", Duration::from_secs(20), |_| {
        got.lock().iter().any(|e| match e {
            Got::Batch { generation: 2, first, records } => first + records.len() as u64 - 1 == last.1,
            other => matches!(other, Got::Terminal(_)),
        })
    });

    let (published, got) = (published.lock(), got.lock());
    let (mut at, mut gens, mut delivered, mut lost) = (None::<Cursor>, Vec::new(), Vec::new(), 0);
    for event in got.iter() {
        match event {
            Got::Terminal(what) => panic!("seed {seed}: a live subscription ended: {what}\n{}", w.trace_tail(60)),
            Got::OriginLost => {
                let within = at.map(|c| c.generation) == Some(1);
                assert!(within, "seed {seed}: the origin's loss told outside the gap between generations, at {at:?}");
                lost += 1;
            }
            Got::Ready(None) => {}
            Got::Ready(Some(start)) => {
                // Only a new generation may start further along; within one, a Ready that moved would hide a gap.
                let fine = at.is_none_or(|at| *start == at || start.generation > at.generation);
                assert!(fine, "seed {seed}: Ready at {start:?} after {at:?}");
                at = Some(*start);
            }
            Got::Batch { generation: g, first, records } => {
                if let Some(at) = at {
                    assert_eq!((*g, *first), (at.generation, at.sequence + 1), "seed {seed}: a gap or a repeat");
                }
                for (s, record) in (*first..).zip(records) {
                    assert_eq!(published.get(&(*g, s)), Some(record), "seed {seed}: {g}/{s} was never published");
                    if *g == 2 {
                        delivered.push(s);
                    }
                }
                at = Some(Cursor { generation: *g, sequence: first + records.len() as u64 - 1 });
                if gens.last() != Some(g) {
                    gens.push(*g);
                }
            }
        }
    }
    assert_eq!(gens, vec![1, 2], "seed {seed}: the restart shows as generation 2");
    assert_eq!(lost, 1, "seed {seed}: the origin's loss is told once");
    // A new generation is fetched from its record 1, so every record of it arrives.
    let sent: Vec<u64> = published.keys().filter(|(g, _)| *g == 2).map(|(_, s)| *s).collect();
    assert_eq!(delivered, sent, "seed {seed}: generation 2 lost records");
}

#[test]
fn a_live_follower_of_a_crashed_origin_sees_every_record_once_and_in_order() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, got, published) = start(seed);
        let run = Duration::from_millis(w.pick(50..400));
        w.run_for(run);
        crash_origin(&mut w);
        let crashed = w.now();
        w.run_until("the leaf is told its origin is lost", Duration::from_secs(20), |_| {
            got.lock().iter().any(|e| matches!(e, Got::OriginLost))
        });
        // The relay's upstream_timeout (3 s) after it sees the crash, then its refusal: 3 to 9 s over the seeds. The
        // leaf's own upstream_timeout after that refusal would take 8 to 15 s.
        let took = w.now() - crashed;
        assert!(took <= Duration::from_secs(10), "seed {seed}: the loss was told {took:?} after the crash");
        let down = Duration::from_millis(w.pick(0..3000));
        w.run_for(down);
        restart_origin(&mut w, &published, 100);
        w.run_until("the leaf follows generation 2", Duration::from_secs(20), |_| follows(&got, 2));
        let second = publish(&mut w, &published, 300);
        let before_cut = Duration::from_millis(w.pick(10..200));
        w.run_for(before_cut);
        // Origin to relay is cut for less than the idle timeout: bytes are held, then delivered.
        w.cut(&["o"], &["r"]);
        let cut = Duration::from_millis(w.pick(200..2000));
        w.run_for(cut);
        w.heal();
        w.join(second).expect("the origin stays up");
        check(&mut w, &got, &published, seed);
        println!("world digest stream_origin_crash seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// While the origin is down, the relay has no parent and refuses the leaf, whose backoff on it grows up to 64 s. The
/// origin's return is a membership change, and that retries every refused parent at once, and one that refuses just
/// after, on a view behind the leaf's.
#[test]
fn a_relay_whose_parent_returns_is_asked_again_within_a_keepalive_of_the_membership_change() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, got, published) = start(seed);
        let incarnation = |w: &World<Node>| {
            let (_, l) = w.nodes().find(|(name, _)| *name == "l").expect("the leaf is up");
            l.cluster.view().get("o").map(|m| m.record.incarnation)
        };
        let before = incarnation(&w);
        crash_origin(&mut w);
        let down = Duration::from_millis(w.pick(10_000..40_000));
        w.run_for(down);
        restart_origin(&mut w, &published, 100);
        w.run_until("the leaf sees the new origin", Duration::from_secs(20), |w| {
            incarnation(w).is_some_and(|i| Some(i) != before)
        });
        let seen = w.now();
        w.run_until("the leaf follows generation 2", Duration::from_secs(70), |_| follows(&got, 2));
        let took = w.now() - seen;
        // The cluster's keepalive.
        assert!(took <= Duration::from_secs(1), "seed {seed}: generation 2 reached the leaf {took:?} after its view");
        check(&mut w, &got, &published, seed);
        println!("world digest stream_relay_backoff seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

const NODES: [&str; 3] = ["a", "b", "c"];

/// `PaxosConfig::new`'s lease.
const LEASE: Duration = Duration::from_secs(6);
/// `ClusterConfig::backoff`'s cap on a redial, 2 s, and its 20 % jitter.
const REDIAL: Duration = Duration::from_millis(2400);
/// Everything answers within this of a fault's start or end: a key waits out its lost owner's lease, and a peer cut off
/// is dialled again within the redial cap. A caller sees a silent owner go within the idle timeout, 4 s, which runs
/// alongside the lease. Kept apart from the configs, so a changed default has to change this too.
const RECOVER: Duration = LEASE.saturating_add(REDIAL);

/// Every node is a seed and an acceptor; a is the stream's origin, and b and c pull it from a. Only addresses, ids, the
/// secret, labels and the heartbeats the guide asks for with seeds are set.
async fn boot_defaults(host: Host) -> Node {
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let origin = host.name() == "a";
    let (cluster, seed) = seeded(&host, &system, &transport, &NODES, if origin { "origin" } else { "relay" }).await;
    cluster.kind::<EchoApi>("echo").register(|_: &SpawnContext, _: Bytes| Ok(Echo)).await.unwrap();
    let upstream = if origin { vec![] } else { vec![role("origin")] };
    let stream = StreamNode::start(&cluster, StreamNodeConfig::new(upstream)).await.unwrap();
    if origin {
        stream.open_stream(&key(), u64::from(host.boot()), DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    Node { _system: system, cluster, stream, _seed: Some(seed) }
}

#[derive(Default)]
struct Seen {
    /// The payload of a's next record: records are numbered across its boots.
    next: Cell<u64>,
    /// One past the highest record the follower on c got.
    followed: Cell<u64>,
    ended: RefCell<Option<String>>,
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

/// A `Live` follower on c, which follows the key across a's boots and never ends.
fn follow(w: &mut World<Node>, seen: &Rc<Seen>) {
    let seen = seen.clone();
    drop(w.spawn_on("c", move |n| async move {
        let mut sub = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
        while let Some(event) = sub.next().await {
            match event {
                StreamEvent::Batch(batch) => {
                    let last = batch.records().iter().map(|r| (&r[..]).get_u64()).max();
                    seen.followed.set(seen.followed.get().max(last.map_or(0, |l| l + 1)));
                }
                other if other.is_terminal() => *seen.ended.borrow_mut() = Some(format!("{other:?}")),
                _ => {}
            }
        }
    }));
}

/// Waits until every node of `callers` has had an answer from every key, and, with `stream`, the follower a record
/// published from now on, and says how long after `since` that was. Panics past `since + RECOVER`.
fn recovered(w: &mut World<Node>, callers: &[&str], stream: bool, seen: &Rc<Seen>, since: Duration) -> Duration {
    let (next, done) = (seen.next.get(), Rc::new(Cell::new(0)));
    for on in callers {
        let done = done.clone();
        drop(w.spawn_on(on, move |n| async move {
            for key in keys() {
                let keyed = n.cluster.keyed::<EchoApi>("echo", &key, Bytes::new(), &Placement::default());
                while keyed.request(1).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            done.set(done.get() + 1);
        }));
    }
    let what = format!("{callers:?} answered{}", if stream { " and c followed" } else { "" });
    w.run_until(&what, (since + RECOVER).saturating_sub(w.now()), |_| {
        done.get() == callers.len() && (!stream || seen.followed.get() > next)
    });
    assert_eq!(*seen.ended.borrow(), None, "the live follower ended\n{}", w.trace_tail(40));
    w.now() - since
}

fn defaults(seed: u64) {
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in NODES {
        w.add(name, boot_defaults);
    }
    // Seeds collect registrations for 5 s before they answer, and acceptors refuse for a lease after they start.
    w.run_until("three members with the kind", Duration::from_secs(30), |w| {
        let with_kind = |n: &Node| n.cluster.view().members.iter().filter(|m| m.record.has_kind("echo")).count();
        w.nodes().count() == NODES.len() && w.nodes().all(|(_, n)| with_kind(n) == NODES.len())
    });
    let seen = Rc::new(Seen::default());
    keep_publishing(&mut w, &seen);
    follow(&mut w, &seen);
    let booted = w.now();
    recovered(&mut w, &NODES, true, &seen, booted);

    // Two clocks as far apart as `PaxosConfig::drift` allows, 1 %, or nearer.
    let ppm = w.pick(0..5001) as i32;
    w.drift("b", ppm);
    w.drift("c", -ppm);
    let victim = NODES[w.pick(0..3) as usize];
    let others: Vec<&str> = NODES.into_iter().filter(|n| *n != victim).collect();
    // Without a, nothing is published; without c, nothing follows.
    let stream_without = victim == "b";
    let fault = seed % 5;
    let (at, mut gone) = (w.now(), None);
    match fault {
        // Long enough for a redial backoff to reach a 30 s cap, as `Backoff::default()`'s did: the shipped one is 2 s.
        0 => {
            w.partition(&[victim], &others);
            gone = Some(recovered(&mut w, &others, stream_without, &seen, at));
            let cut = Duration::from_millis(w.pick(65_000..80_000));
            w.run_for((at + cut).saturating_sub(w.now()));
            w.heal();
        }
        // Back within the lease its keys were held under, or after a machine's keys moved.
        1 | 2 => {
            let how = if fault == 1 { Crash::Process } else { Crash::Machine };
            w.crash(victim, how);
            if how == Crash::Machine {
                gone = Some(recovered(&mut w, &others, stream_without, &seen, at));
            } else {
                let down = Duration::from_millis(w.pick(0..LEASE.as_millis() as u64));
                w.run_for(down);
            }
            w.restart(victim);
        }
        // Longer than the lease: it resumes with its fence closed and its keys moved.
        3 => {
            let pause = Duration::from_millis(w.pick(LEASE.as_millis() as u64 + 1000..2 * LEASE.as_millis() as u64));
            w.pause(victim, pause);
            gone = Some(recovered(&mut w, &others, stream_without, &seen, at));
            w.run_for((at + pause).saturating_sub(w.now()));
        }
        // Its next boot's incarnation and epochs start from the stepped clock: later, or older than the last. Less than
        // a minute, the most seed discovery allows between clocks (spec/seed-discovery.md § 4.4): a node further behind
        // drops every other record, and one further ahead is refused.
        _ => {
            let step = w.pick(1000..59_000) as i64;
            w.step_wall(victim, if (seed / 5).is_multiple_of(2) { -step } else { step });
            w.crash(victim, Crash::Process);
            w.restart(victim);
        }
    }
    let ended = w.now();
    if matches!(fault, 1 | 2 | 4) && victim == "a" {
        keep_publishing(&mut w, &seen);
    }
    if matches!(fault, 1 | 2 | 4) && victim == "c" {
        follow(&mut w, &seen);
    }
    let back = recovered(&mut w, &NODES, true, &seen, ended);
    println!(
        "world digest defaults seed {seed} fault {fault} on {victim}: gone {gone:?} back {back:?}: {:016x}",
        w.digest()
    );
}

#[test]
fn every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings() {
    check_seeds(seeds(0..64), defaults).unwrap();
}
