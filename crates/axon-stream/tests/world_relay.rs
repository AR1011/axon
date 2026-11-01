//! Scenarios of `relay.rs` on the World: real nodes on virtual time, so no bound here is sized for a busy machine.
//! `relay.rs` keeps one threaded version of each, for the actor threads the World does not run. The faults on the
//! wire, the clocks and the CPU that only the World injects end the file.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::{Compression, Lane, SharedSecret};
use axon_stream::*;
use axon_testkit::world::{Crash, Host, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

mod common;
use common::{NoStreams, SECRET, key, labels};

#[derive(Clone)]
struct Node {
    stream: StreamNode,
    cluster: Cluster,
    system: ActorSystem,
}

/// With the timings of `relay.rs`. The role is the name's: `origin`, `relay-*`, or a leaf. Each boot of the origin
/// opens the key as a new generation. `set` runs on the stream config last.
async fn boot(
    host: Host,
    discovery: MemoryDiscovery,
    compression: Compression,
    set: fn(&mut StreamNodeConfig),
) -> Node {
    let role = match host.name() {
        "origin" => "origin",
        name if name.starts_with("relay") => "relay",
        _ => "leaf",
    };
    let mut transport = host.transport(SharedSecret::new(SECRET).unwrap());
    for lane in Lane::ALL {
        transport.lanes.get_mut(lane).compression = compression;
    }
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    common::short_cluster(&mut cfg);
    cfg.labels = labels(&[("role", role), ("region", "eu")]);
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = match role {
        "origin" => vec![],
        "relay" => vec![labels(&[("role", "origin")])],
        _ => vec![labels(&[("role", "relay"), ("region", "eu")]), labels(&[("role", "origin")])],
    };
    let mut cfg = StreamNodeConfig::new(upstream);
    common::short_stream(&mut cfg);
    // Two relays lost in a row to silent machines leave a leaf 10 s or more without its parent: at 1500 records a
    // second that is more than the default ring of 16384 records, and the leaf would rightly reset.
    cfg.core.store.max_records_per_stream = 1 << 17;
    set(&mut cfg);
    let stream = StreamNode::start(&cluster, cfg).await.unwrap();
    if role == "origin" {
        stream.open_stream(&key(), u64::from(host.boot()), DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    Node { stream, cluster, system }
}

fn world(seed: u64, names: &[&str], compression: Compression) -> World<Node> {
    world_with(WorldConfig { seed, ..WorldConfig::default() }, names, compression, |_| {}).0
}

/// Each node booted with `set` on its stream config.
fn world_with(
    cfg: WorldConfig,
    names: &[&str],
    compression: Compression,
    set: fn(&mut StreamNodeConfig),
) -> (World<Node>, MemoryDiscovery) {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(cfg);
    for name in names {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone(), compression, set));
    }
    w.run_until("every node booted", Duration::from_secs(30), |w| w.nodes().count() == names.len());
    (w, discovery)
}

fn stream<'w>(w: &'w World<Node>, name: &str) -> &'w StreamNode {
    &w.nodes().find(|(n, _)| *n == name).unwrap_or_else(|| panic!("{name} is down")).1.stream
}

fn crash(w: &mut World<Node>, name: &str) {
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.log(format_args!("{name} {how:?}"));
    w.crash(name, how);
}

/// The last record a leaf read, or what was wrong: anything but the next number, or an end.
type Read = Rc<RefCell<Result<u64, String>>>;

/// Records start with their sequence numbers, so a reader that starts after a cursor expects the next one.
fn collect(w: &mut World<Node>, leaf: &str, key: StreamKey, start: StartPosition) -> Read {
    let after = if let StartPosition::After(cursor) = start { cursor.sequence } else { 0 };
    let read = Read::new(RefCell::new(Ok(after)));
    let r = read.clone();
    drop(w.spawn_on(leaf, move |n| async move {
        let mut subscription = n.stream.subscribe(&key, SubscribeOptions { start, ..Default::default() }).unwrap();
        while let Some(event) = subscription.next().await {
            let last = r.borrow().clone().unwrap();
            let wrong = match event {
                StreamEvent::Batch(batch) => {
                    (batch.first()..).zip(batch.records()).try_fold(last, |last, (s, record)| {
                        let right = s == last + 1 && record.starts_with(&s.to_be_bytes());
                        if right { Ok(s) } else { Err(format!("{s} after {last}")) }
                    })
                }
                terminal if terminal.is_terminal() => Err(format!("{terminal:?} after {last}")),
                _ => Ok(last),
            };
            *r.borrow_mut() = wrong.clone();
            if wrong.is_err() {
                return;
            }
        }
        *r.borrow_mut() = Err("the subscription closed".into());
    }));
    read
}

/// Waits up to 20 s without panicking, so a leaf that stalled is named with what it read and its node's events.
fn read_through(w: &mut World<Node>, seed: u64, leaves: &[&str], reads: &[Read], published: u64) {
    let until = w.now() + Duration::from_secs(20);
    while w.now() < until && reads.iter().any(|r| r.borrow().as_ref().is_ok_and(|last| *last < published)) {
        w.run_for(Duration::from_millis(100));
    }
    for (leaf, read) in leaves.iter().zip(reads) {
        let read = read.borrow().clone();
        if read != Ok(published) {
            // On the node, so the events carry the World's time.
            let events = w.block_on(leaf, |n| async move { n.stream.detail(0).events }).unwrap();
            panic!("seed {seed}: {leaf} read {read:?}, not Ok({published})\n{events:#?}\n{}", w.trace_tail(30));
        }
    }
}

fn relays(seed: u64) {
    let leaves = ["leaf-1", "leaf-2", "leaf-3"];
    let names = ["origin", "relay-a", "relay-b", "leaf-1", "leaf-2", "leaf-3"];
    // Odd seeds compress the transport.
    let compression = if seed % 2 == 1 { Compression::Zstd } else { Compression::None };
    let mut w = world(seed, &names, compression);
    let reads: Vec<Read> = leaves.iter().map(|leaf| collect(&mut w, leaf, key(), StartPosition::Live)).collect();
    let behind = |w: &World<Node>, on: &dyn Fn(&str) -> bool| {
        leaves.iter().all(|l| stream(w, l).parent_of(&key()).is_some_and(|p| on(&p)))
    };
    w.run_until("every leaf is positioned behind a relay", Duration::from_secs(30), |w| {
        leaves.iter().all(|l| stream(w, l).cursor(&key()).is_some()) && behind(w, &|p| p.starts_with("relay"))
    });

    let stop = Rc::new(Cell::new(false));
    let stopped = stop.clone();
    let publishing = w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stopped.get() {
            let records = (next..next + 3).map(|n| Bytes::copy_from_slice(&n.to_be_bytes())).collect();
            n.stream.publish(&key(), records).unwrap();
            next += 3;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        next - 1
    });

    for round in 0..4 {
        let name = ["relay-a", "relay-b"][round % 2];
        let up = Duration::from_millis(w.pick(100..500));
        w.run_for(up);
        crash(&mut w, name);
        w.run_until("no leaf depends on the dead relay", Duration::from_secs(20), |w| behind(w, &|p| p != name));
        let down = Duration::from_millis(w.pick(0..400));
        w.run_for(down);
        w.restart(name);
    }
    // With both relays down the leaves go direct, and come back once a relay does.
    crash(&mut w, "relay-a");
    crash(&mut w, "relay-b");
    w.run_until("every leaf pulls from the origin", Duration::from_secs(20), |w| behind(w, &|p| p == "origin"));
    w.restart("relay-a");
    w.restart("relay-b");
    w.run_until("every leaf is back behind a relay", Duration::from_secs(20), |w| {
        behind(w, &|p| p.starts_with("relay"))
    });
    w.run_until("the origin feeds relays only", Duration::from_secs(20), |w| {
        stream(w, "origin").stats().downstream.iter().all(|d| (d.branches > 0) == d.node.starts_with("relay"))
    });

    stop.set(true);
    let published = w.join(publishing).unwrap();
    assert!(published > 500, "seed {seed}: published {published}");
    read_through(&mut w, seed, &leaves, &reads, published);
    let switches: u64 = leaves.iter().map(|l| stream(&w, l).stats().switches).sum();
    assert!(switches >= 3, "seed {seed}: leaves moved back to a relay {switches} times");
    assert!(leaves.iter().all(|l| stream(&w, l).stats().local_resets == 0), "seed {seed}");
    println!("world digest relays seed {seed}: {:016x}", w.digest());
}

/// A parent lost to a silent machine is found at the idle timeout, and its successor's catch-up lands in one burst,
/// which default subscribers that kept up hold.
#[test]
fn leaves_see_every_record_once_while_relays_crash_and_return() {
    check_seeds(seeds(0..64), relays).unwrap();
}

fn publish(w: &mut World<Node>, range: std::ops::RangeInclusive<u64>) {
    let published = w.block_on("origin", move |n| async move {
        for i in range {
            n.stream.publish(&key(), vec![Bytes::copy_from_slice(&i.to_be_bytes())]).unwrap();
        }
    });
    published.unwrap();
}

/// Reads records `last + 1..=through` in order, and answers every other event it saw.
async fn read(mut subscription: Subscription, mut last: u64, through: u64) -> Vec<StreamEvent> {
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
    events
}

/// A fresh node has no history of its own: the cursor it resumes from is asked of its parent.
#[test]
fn a_restarted_edge_resumes_after_its_cursor_from_the_origin() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &["origin", "edge"], Compression::None);
        let first = w.spawn_on("edge", |n| async move {
            read(n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap(), 0, 10).await
        });
        w.run_until("the edge is positioned", Duration::from_secs(30), |w| stream(w, "edge").cursor(&key()).is_some());
        publish(&mut w, 1..=10);
        w.join(first).unwrap();
        crash(&mut w, "edge");

        publish(&mut w, 11..=20);
        w.restart("edge");
        let cursor = Cursor { generation: 1, sequence: 10 };
        let resumed = w.spawn_on("edge", move |n| async move {
            let options = SubscribeOptions { start: StartPosition::After(cursor), ..Default::default() };
            let resumed = n.stream.subscribe(&key(), options).expect("a cursor start on a node without the stream");
            read(resumed, 10, 30).await
        });
        let wait = Duration::from_millis(w.pick(0..200));
        w.run_for(wait);
        publish(&mut w, 21..=30);
        let events = w.join(resumed).unwrap();
        let expected = [StreamEvent::Ready { start: Some(cursor) }, StreamEvent::CaughtUp { through: cursor }];
        assert_eq!(events, expected, "seed {seed}");
        println!("world digest restarted_edge seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// The batches a follower read, by their cursors, and any end.
type Seen = Rc<RefCell<Vec<Result<Cursor, String>>>>;

/// A producer that moves after its node crashed, to a node that relays its key: a keyed actor restarting where
/// followers already pull the key. Nothing closed the key there, so the relay still holds the dead origin's position.
#[test]
fn a_node_relaying_a_key_originates_it_once_the_origin_crashed() {
    check_seeds(seeds(0..64), |seed| {
        let followers = ["relay-a", "leaf"];
        let mut w = world(seed, &["origin", "relay-a", "leaf"], Compression::None);
        let seen: Vec<Seen> = followers
            .iter()
            .map(|f| {
                let seen = Rc::new(RefCell::new(Vec::new()));
                let s = seen.clone();
                drop(w.spawn_on(f, move |n| async move {
                    let mut follower = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
                    while let Some(event) = follower.next().await {
                        match event {
                            StreamEvent::Batch(batch) => s.borrow_mut().push(Ok(batch.cursor())),
                            event if event.is_terminal() => s.borrow_mut().push(Err(format!("{event:?}"))),
                            _ => {}
                        }
                    }
                }));
                seen
            })
            .collect();
        w.run_until("both are positioned", Duration::from_secs(30), |w| {
            followers.iter().all(|f| stream(w, f).cursor(&key()).is_some())
        });
        let reached = |seen: &[Seen], through: Cursor| {
            seen.iter().all(|s| s.borrow().iter().any(|c| c.as_ref().is_ok_and(|c| *c == through)))
        };
        let one = Cursor { generation: 1, sequence: 1 };
        publish(&mut w, 1..=1);
        w.run_until("both read the first record", Duration::from_secs(10), |_| reached(&seen, one));

        crash(&mut w, "origin"); // no close reaches the relay
        let opened =
            w.block_on(
                "relay-a",
                |n| async move { n.stream.open_stream(&key(), 2, DeliveryPolicy::ReplayRequired).await },
            );
        opened.unwrap().unwrap();
        // The leaf's branch ends with the relay's old stream, and it joins the new one where it finds it.
        w.run_until("the leaf follows the new origin", Duration::from_secs(10), |w| {
            stream(w, "leaf").cursor(&key()).is_some_and(|c| c.generation == 2)
        });
        let published =
            w.block_on(
                "relay-a",
                |n| async move { n.stream.publish(&key(), vec![Bytes::from_static(b"two")]).map(drop) },
            );
        published.unwrap().unwrap();
        let two = Cursor { generation: 2, sequence: 1 };
        w.run_until("both read the new origin's record", Duration::from_secs(10), |_| reached(&seen, two));
        for (follower, seen) in followers.iter().zip(&seen) {
            assert_eq!(*seen.borrow(), [Ok(one), Ok(two)], "seed {seed}: {follower}");
        }
        println!("world digest relay_originates seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A follower revoked while it is cut off from the origin cannot refresh its interest once the cut heals, and the origin
/// drops a child it has not heard from for `downstream_expiry` (2 s here): nothing published after that reaches it,
/// and its session ends. Until then, a follower that refreshes is never dropped. Granted access again, it asks again
/// and reads on from its cursor, the records it missed included.
#[test]
fn a_follower_revoked_during_a_partition_gets_no_more_records_once_it_heals() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, &["origin", "edge"], Compression::None);
        let sessions = stream(&w, "origin").stats().hub.sessions;
        let read = collect(&mut w, "edge", key(), StartPosition::Live);
        w.run_until("the edge is positioned", Duration::from_secs(30), |w| stream(w, "edge").cursor(&key()).is_some());
        publish(&mut w, 1..=5);
        w.run_until("the edge reads the first records", Duration::from_secs(10), |_| *read.borrow() == Ok(5));
        w.run_for(Duration::from_secs(5));
        assert_eq!(stream(&w, "origin").stats().downstream_expired, 0, "seed {seed}: a healthy edge was dropped");

        w.partition(&["origin"], &["edge"]);
        let (cut, revoke) = (w.pick(500..5000), w.pick(0..500));
        w.run_for(Duration::from_millis(revoke));
        let revoked = w.block_on("origin", |n| async move {
            n.cluster.set_authorizer(Some(Arc::new(NoStreams("edge"))));
            axon::trace::unix_micros() / 1000
        });
        let revoked = revoked.unwrap();
        w.run_for(Duration::from_millis(cut - revoke));
        w.heal();
        w.run_until("the origin drops the edge", Duration::from_secs(10), |w| {
            stream(w, "origin").stats().downstream.is_empty()
        });
        // Its last interest came before the cut: dropped within the expiry and a tick of the revoke, cut or not.
        let events = w.block_on("origin", |n| async move { n.stream.detail(0).events }).unwrap();
        let expired = events.iter().find(|e| e.kind == "downstream_expired").map(|e| e.time_ms);
        assert!(expired.is_some_and(|at| at <= revoked + 2050), "seed {seed}: revoked at {revoked} ms\n{events:#?}");
        assert_eq!(stream(&w, "origin").stats().hub.sessions, sessions, "seed {seed}: the edge's session outlived it");
        publish(&mut w, 6..=15);
        w.run_for(Duration::from_secs(1));
        assert_eq!(*read.borrow(), Ok(5), "seed {seed}: the revoked edge read on");

        w.block_on("origin", |n| async move { n.cluster.set_authorizer(None) }).unwrap();
        w.run_until("the edge reads on from its cursor", Duration::from_secs(10), |_| *read.borrow() == Ok(15));
        println!("world digest revoked_follower seed {seed} cut {cut} revoke {revoke}: {:016x}", w.digest());
    })
    .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// The relay is stopped, as by SIGSTOP.
    Paused,
    Partition,
    /// What the relay sends the leaf is held, acknowledgements and dials included.
    CutTo,
    CutFrom,
    /// The leaf is stepped once in 100 ms.
    Starved,
    /// The relay is stepped once in 3 to 5 s.
    Stalled,
    /// The relay is out of descriptors: its session with the leaf is reset and it accepts no new one.
    Refusing,
    /// The relay and the leaf 0.5 % apart, and the leaf's wall clock stepped a minute.
    Clocks,
    /// 30 to 80 ms each way, with as much jitter.
    Slow,
    /// About one chunk in 40 each way waits longer than the request timeout, so interests and pushes are sent again.
    Lossy,
}

/// Records are the key's sequence numbers, 500 a second on each key.
fn fault(seed: u64) {
    let keys: Vec<StreamKey> = (0..5).map(|n| StreamKey::new("md", format!("k{n}"))).collect();
    let mut w = world(seed, &["origin", "relay-a", "leaf"], Compression::None);
    let opening = keys.clone();
    let opened = w.block_on("origin", move |n| async move {
        for k in &opening {
            n.stream.open_stream(k, 1, DeliveryPolicy::ReplayRequired).await?;
        }
        Ok::<_, StreamError>(())
    });
    opened.unwrap().unwrap();
    let mut reads = vec![
        collect(&mut w, "leaf", keys[0].clone(), StartPosition::Live),
        collect(&mut w, "leaf", keys[2].clone(), StartPosition::Live),
    ];
    // Dropped during the fault: the leaf's parents must let it go.
    let quit = Rc::new(Cell::new(false));
    let quitting = quit.clone();
    let dropped = keys[1].clone();
    drop(w.spawn_on("leaf", move |n| async move {
        let mut subscription = n.stream.subscribe(&dropped, SubscribeOptions::default()).unwrap();
        while !quitting.get() {
            if let Ok(None) = tokio::time::timeout(Duration::from_millis(50), subscription.next()).await {
                return;
            }
        }
    }));
    w.run_until("the leaf pulls three keys through the relay", Duration::from_secs(30), |w| {
        let leaf = stream(w, "leaf");
        keys[..3].iter().all(|k| leaf.cursor(k).is_some() && leaf.parent_of(k).as_deref() == Some("relay-a"))
    });

    let stop = Rc::new(Cell::new(false));
    let stopped = stop.clone();
    let publishing = keys.clone();
    let publisher = w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stopped.get() {
            for k in &publishing {
                n.stream.publish(k, vec![Bytes::copy_from_slice(&next.to_be_bytes())]).unwrap();
            }
            next += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        next - 1
    });
    let before = Duration::from_millis(w.pick(200..1000));
    w.run_for(before);

    use Fault::*;
    let faults = [Paused, Partition, CutTo, CutFrom, Starved, Stalled, Refusing, Clocks, Slow, Lossy];
    // From the seed, so that any ten seeds in a row inject every fault.
    let fault = faults[(seed % faults.len() as u64) as usize];
    let lasts = Duration::from_millis(w.pick(2000..5000));
    w.log(format_args!("fault {fault:?} for {lasts:?}"));
    let both_ways = |w: &mut World<Node>, wire: Wire| {
        w.wire("relay-a", "leaf", wire);
        w.wire("leaf", "relay-a", wire);
    };
    match fault {
        Paused => w.pause("relay-a", lasts),
        Partition => w.partition(&["relay-a"], &["leaf"]),
        CutTo => w.cut(&["relay-a"], &["leaf"]),
        CutFrom => w.cut(&["leaf"], &["relay-a"]),
        Starved => w.starve("leaf", 100),
        Stalled => {
            let one_in = w.pick(3000..5000) as u32;
            w.starve("relay-a", one_in);
        }
        Refusing => {
            w.refuse_accepts("relay-a", true);
            w.reset("relay-a", "leaf");
        }
        Clocks => {
            w.drift("relay-a", 5000);
            w.drift("leaf", -5000);
            let by = if w.pick(0..2) == 0 { 60_000 } else { -60_000 };
            w.step_wall("leaf", by);
        }
        Slow => {
            let ms = Duration::from_millis(w.pick(30..80));
            both_ways(&mut w, Wire { latency: ms, jitter: ms, ..Wire::default() });
        }
        Lossy => {
            let stall = Duration::from_millis(1500);
            both_ways(&mut w, Wire { stall_every: 40, stall, syn_stall: stall, ..Wire::default() });
        }
    }
    // Within the fault the leaf wants two keys more, one of them from a cursor it never held, and drops one.
    w.run_for(lasts / 3);
    let head = stream(&w, "origin").cursor(&keys[3]).expect("the origin holds its key").sequence;
    let cursor = Cursor { generation: 1, sequence: w.pick(1..head) };
    reads.push(collect(&mut w, "leaf", keys[3].clone(), StartPosition::After(cursor)));
    reads.push(collect(&mut w, "leaf", keys[4].clone(), StartPosition::First));
    quit.set(true);
    w.run_for(lasts - lasts / 3);
    match fault {
        Starved => w.starve("leaf", 1),
        Stalled => w.starve("relay-a", 1),
        Refusing => w.refuse_accepts("relay-a", false),
        Clocks => {
            w.drift("relay-a", 0);
            w.drift("leaf", 0);
        }
        Slow | Lossy => both_ways(&mut w, Wire::default()),
        _ => w.heal(),
    }

    let ended = w.now();
    stop.set(true);
    let published = w.join(publisher).unwrap();
    let until = w.now() + Duration::from_secs(20);
    while w.now() < until && reads.iter().any(|r| r.borrow().as_ref().is_ok_and(|last| *last < published)) {
        w.run_for(Duration::from_millis(100));
    }
    for (key, read) in [0, 2, 3, 4].iter().map(|i| &keys[*i]).zip(&reads) {
        let read = read.borrow().clone();
        if read != Ok(published) {
            let events = w.block_on("leaf", |n| async move { n.stream.detail(0).events }).unwrap();
            panic!("seed {seed} {fault:?}: {key:?} read {read:?}, not Ok({published})\n{events:#?}");
        }
    }
    let caught_up = w.now() - ended;
    assert!(caught_up < Duration::from_secs(10), "seed {seed} {fault:?}: caught up {caught_up:?} after the fault");

    // Once the dropped key's linger ran out on both, each parent holds exactly what its child wants.
    let wanted = [&keys[0], &keys[2], &keys[3], &keys[4]];
    let branches = |w: &World<Node>, parent: &str, child: &str| {
        let stats = stream(w, parent).stats();
        stats.downstream.iter().find(|d| d.node == child).map_or(0, |d| d.branches)
    };
    let exact = |w: &World<Node>| {
        let leaf = stream(w, "leaf");
        wanted.iter().all(|k| leaf.parent_of(k).as_deref() == Some("relay-a"))
            && leaf.stats().feeds == wanted.len()
            && stream(w, "relay-a").stats().feeds == wanted.len()
            && branches(w, "relay-a", "leaf") == wanted.len()
            && branches(w, "origin", "relay-a") == wanted.len()
            && branches(w, "origin", "leaf") == 0
    };
    let until = w.now() + Duration::from_secs(30);
    while w.now() < until && !exact(&w) {
        w.run_for(Duration::from_millis(100));
    }
    if !exact(&w) {
        let stats =
            ["origin", "relay-a", "leaf"].map(|n| (n, stream(&w, n).stats().downstream, stream(&w, n).stats().feeds));
        panic!("seed {seed} {fault:?}: the parents do not hold what the leaf wants\n{stats:#?}");
    }
    assert_eq!(stream(&w, "leaf").stats().local_resets, 0, "seed {seed} {fault:?}");
    println!("world digest fault seed {seed}: {:016x}", w.digest());
}

/// Record `s` of the compacted key sets key `s % 20` to `s`, or removes it when `s` is a multiple of 7.
fn state_record(s: u64) -> Bytes {
    let key = (s % 20).to_be_bytes();
    if s.is_multiple_of(7) { keyed(&key, b"") } else { keyed(&key, &s.to_be_bytes()) }
}

fn apply_state(state: &mut BTreeMap<u64, u64>, record: &Bytes) {
    let (key, value) = split_keyed(record).expect("a keyed record");
    let key = u64::from_be_bytes(key[..].try_into().expect("an 8-byte key"));
    match value.len() {
        0 => state.remove(&key),
        _ => state.insert(key, u64::from_be_bytes(value[..].try_into().expect("an 8-byte value"))),
    };
}

fn state_at(through: u64) -> BTreeMap<u64, u64> {
    let mut state = BTreeMap::new();
    (1..=through).for_each(|s| apply_state(&mut state, &state_record(s)));
    state
}

/// What a state reader holds: the position its state is at, and the state, or what was wrong.
type State = Rc<RefCell<Result<(u64, BTreeMap<u64, u64>), String>>>;

fn compacted(seed: u64) {
    let state_key = StreamKey::new("md", "state");
    let mut w = world(seed, &["origin", "relay-a", "leaf"], Compression::None);
    let limits = CompactLimits { max_keys: 20, max_bytes: 20 * 18 };
    let opening = state_key.clone();
    let opened = w.block_on("origin", move |n| async move {
        n.stream.open_stream(&opening, 1, DeliveryPolicy::Compacted(limits)).await
    });
    opened.unwrap().unwrap();
    // The relay pulls the key already, as it would for another leaf.
    let following = state_key.clone();
    drop(w.spawn_on("relay-a", move |n| async move {
        let mut follower = n.stream.subscribe(&following, SubscribeOptions::default()).unwrap();
        while follower.next().await.is_some() {}
    }));

    let stop = Rc::new(Cell::new(false));
    let stopped = stop.clone();
    let publishing = state_key.clone();
    let publisher = w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stopped.get() {
            n.stream.publish(&publishing, vec![state_record(next)]).unwrap();
            next += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        next - 1
    });
    let late = Duration::from_millis(w.pick(200..800));
    w.run_for(late);

    let (state, resets) = (State::new(RefCell::new(Err("no snapshot yet".into()))), Rc::new(Cell::new(0)));
    let (s, r, reading) = (state.clone(), resets.clone(), state_key.clone());
    let taken = Rc::new(Cell::new(false));
    let t = taken.clone();
    drop(w.spawn_on("leaf", move |n| async move {
        let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
        loop {
            let mut subscription = n.stream.subscribe(&reading, options).unwrap();
            *s.borrow_mut() = Err("no snapshot yet".into());
            let mut ready = None;
            while let Some(event) = subscription.next().await {
                let held = s.borrow().clone();
                let next = match (event, held) {
                    // One `Ready`, then the snapshot: a waiting subscriber sees no position of the relay's own.
                    (StreamEvent::Ready { start }, Err(_)) if ready.is_none() => {
                        ready = start;
                        continue;
                    }
                    (StreamEvent::Snapshot(snapshot), Err(_)) => {
                        t.set(true);
                        let mut state = BTreeMap::new();
                        snapshot.records().iter().for_each(|record| apply_state(&mut state, record));
                        let through = snapshot.through().sequence;
                        if ready != Some(snapshot.through()) {
                            Err(format!("a snapshot through {through} after Ready {ready:?}"))
                        } else if state != state_at(through) {
                            Err(format!("the snapshot through {through} is not the state there"))
                        } else {
                            Ok((through, state))
                        }
                    }
                    (StreamEvent::Batch(batch), Ok((last, mut state))) if batch.first() == last + 1 => {
                        batch.records().iter().for_each(|record| apply_state(&mut state, record));
                        Ok((batch.last(), state))
                    }
                    (StreamEvent::CaughtUp { through }, Ok((last, state))) if through.sequence == last => {
                        Ok((last, state))
                    }
                    // The origin's ring holds every record, so a leaf that holds the state continues it exactly.
                    (StreamEvent::ResetRequired(reason), Ok((last, _))) => {
                        Err(format!("{reason:?} after {last}, holding the state"))
                    }
                    (StreamEvent::ResetRequired(_), Err(_)) => {
                        r.set(r.get() + 1);
                        break;
                    }
                    (event, held) => Err(format!("{event:?} with {:?}", held.map(|h| h.0))),
                };
                let failed = next.is_err();
                *s.borrow_mut() = next;
                if failed {
                    return;
                }
            }
            if s.borrow().as_ref().is_err_and(|e| e != "no snapshot yet") {
                return;
            }
        }
    }));
    // The relay crashes, and comes back, in half the seeds while the leaf asks it for the state, which takes longer
    // than 40 ms, and in the other half after the leaf holds it and follows on.
    if w.pick(0..2) == 0 {
        let crash_after = Duration::from_millis(w.pick(0..40));
        w.run_for(crash_after);
    } else {
        w.run_until("the leaf holds the state", Duration::from_secs(20), |_| taken.get());
        let crash_after = Duration::from_millis(w.pick(0..300));
        w.run_for(crash_after);
    }
    let crashed = if taken.get() { "after" } else { "before" };
    w.log(format_args!("the relay crashes {crashed} the leaf holds the state"));
    crash(&mut w, "relay-a");
    let down = Duration::from_millis(w.pick(0..400));
    w.run_for(down);
    w.restart("relay-a");
    w.run_until("the leaf pulls through the relay again", Duration::from_secs(20), |w| {
        stream(w, "leaf").parent_of(&state_key).as_deref() == Some("relay-a")
    });
    let more = Duration::from_millis(w.pick(100..500));
    w.run_for(more);

    stop.set(true);
    let published = w.join(publisher).unwrap();
    let until = w.now() + Duration::from_secs(20);
    // A leaf that moved parents just before the publisher stopped may still wait for its state.
    let behind = |state: &State| match &*state.borrow() {
        Ok((last, _)) => *last < published,
        Err(waiting) => waiting == "no snapshot yet",
    };
    while w.now() < until && behind(&state) {
        w.run_for(Duration::from_millis(100));
    }
    let held = state.borrow().clone();
    match held {
        Ok((last, state)) if last == published && state == state_at(published) => {}
        held => {
            let events = w.block_on("leaf", |n| async move { n.stream.detail(0).events }).unwrap();
            panic!("seed {seed}: the leaf holds {:?}, not the state at {published}\n{events:#?}", held.map(|h| h.0));
        }
    }
    println!(
        "world digest compacted seed {seed} resets {} crash {crashed} the state: {:016x}",
        resets.get(),
        w.digest()
    );
}

/// A leaf joins a compacted key late, through a relay that crashes and returns, while the leaf asks for the state or
/// after it holds it. The leaf gets the state its snapshot names, exactly, then every record after it, and ends at the
/// published head with the origin's state. A reset before it holds the state starts it over; one after is a failure,
/// since the origin's ring holds every record.
#[test]
fn a_late_leaf_gets_the_compacted_state_then_every_record_while_its_relay_crashes() {
    check_seeds(seeds(0..64), compacted).unwrap();
}

/// A relay paused, stalled, cut off either way or out of descriptors, a starved leaf, clocks that disagree, and a slow
/// or lossy wire: the leaf reads every record of every key once and in order, a cursor it never held included, catches
/// up soon after the fault, and its parents end up holding exactly the keys it still wants.
#[test]
fn a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them() {
    check_seeds(seeds(0..64), fault).unwrap();
}

/// A `Live` follower that only holds the key, so the node pulls it.
fn follow_live(w: &mut World<Node>, on: &str) {
    drop(w.spawn_on(on, |n| async move {
        let mut subscription = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
        while subscription.next().await.is_some() {}
    }));
}

/// The rig's leave-minority episode: the origin and the leaves' relay are cut off from the leaves, the relay leaves or
/// crashes and comes back on the origin's side, where it takes the key up live, and the cut heals. The origin published
/// throughout and its ring holds every record, so each leaf reads each record once and in order: what the restarted
/// relay lacks, the origin serves.
fn leave_minority(seed: u64) {
    let leaves = ["leaf-1", "leaf-2", "leaf-3"];
    let mut w = world(seed, &["origin", "relay-a", "leaf-1", "leaf-2", "leaf-3"], Compression::None);
    let reads: Vec<Read> = leaves.iter().map(|leaf| collect(&mut w, leaf, key(), StartPosition::Live)).collect();
    follow_live(&mut w, "relay-a");
    w.run_until("every leaf is positioned behind the relay", Duration::from_secs(30), |w| {
        leaves.iter().all(|l| {
            let s = stream(w, l);
            s.cursor(&key()).is_some() && s.parent_of(&key()).as_deref() == Some("relay-a")
        })
    });
    let stop = Rc::new(Cell::new(false));
    let stopped = stop.clone();
    let publishing = w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stopped.get() {
            n.stream.publish(&key(), vec![Bytes::copy_from_slice(&next.to_be_bytes())]).unwrap();
            next += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        next - 1
    });
    let before = Duration::from_millis(w.pick(200..1000));
    w.run_for(before);

    w.partition(&["origin", "relay-a"], &leaves);
    let leave = Duration::from_millis(w.pick(0..3000));
    w.run_for(leave);
    if w.pick(0..2) == 0 {
        w.log("relay-a leaves");
        w.block_on("relay-a", |n| async move { n.system.shutdown().await.map(drop) }).unwrap().unwrap();
        w.crash("relay-a", Crash::Process);
    } else {
        crash(&mut w, "relay-a");
    }
    let down = Duration::from_millis(w.pick(0..4000));
    w.run_for(down);
    w.restart("relay-a");
    follow_live(&mut w, "relay-a");
    let heal = Duration::from_millis(w.pick(0..1500));
    w.run_for(heal);
    w.heal();
    w.log(format_args!("before {before:?} leave {leave:?} down {down:?} heal {heal:?}"));

    let after = Duration::from_millis(w.pick(1000..3000));
    w.run_for(after);
    stop.set(true);
    let published = w.join(publishing).unwrap();
    read_through(&mut w, seed, &leaves, &reads, published);
    println!("world digest leave_minority seed {seed}: {:016x}", w.digest());
}

#[test]
fn leaves_miss_nothing_when_their_relay_leaves_on_the_origins_side_of_a_partition() {
    check_seeds(seeds(0..64), leave_minority).unwrap();
}

/// Nodes 100 ms apart each way, each booted with `set` on its stream config.
fn far(seed: u64, names: &[&str], window: usize, set: fn(&mut StreamNodeConfig)) -> (World<Node>, MemoryDiscovery) {
    let wire = Wire { latency: Duration::from_millis(100), ..Wire::default() };
    world_with(WorldConfig { seed, wire, window, ..WorldConfig::default() }, names, Compression::None, set)
}

/// Record `s`: its sequence number, then zeros up to `len` bytes.
fn padded(s: u64, len: usize) -> Bytes {
    let mut record = vec![0u8; len];
    record[..8].copy_from_slice(&s.to_be_bytes());
    Bytes::from(record)
}

const BIG: usize = 4096;

/// Opens `keys` on the origin and publishes `records` padded records of `BIG` bytes on each, 64 to a batch.
fn backlog(w: &mut World<Node>, keys: &[StreamKey], records: u64) {
    let keys = keys.to_vec();
    let opened = w.block_on("origin", move |n| async move {
        for k in &keys {
            if *k != key() {
                n.stream.open_stream(k, 1, DeliveryPolicy::ReplayRequired).await?;
            }
            for first in (1..=records).step_by(64) {
                n.stream.publish(k, (first..first + 64).map(|s| padded(s, BIG)).collect()).unwrap();
            }
        }
        Ok::<_, StreamError>(())
    });
    opened.unwrap().unwrap();
}

/// Runs until the leaf's readers read `want` bytes of `BIG` records, at most 20 s: the World time from its first
/// record to then, and the most pushes in flight to it the origin had meanwhile.
fn time_to_read(w: &mut World<Node>, reads: &[Read], want: u64) -> (Option<Duration>, usize) {
    let (until, mut first, mut pushes) = (w.now() + Duration::from_secs(20), None, 0);
    while w.now() < until {
        w.run_for(Duration::from_millis(10));
        let read: u64 = reads.iter().map(|r| r.borrow().as_ref().map_or(0, |last| last * BIG as u64)).sum();
        let downstream = stream(w, "origin").stats().downstream;
        pushes = pushes.max(downstream.iter().find(|d| d.node == "leaf").map_or(0, |d| d.inflight_pushes));
        if read > 0 && first.is_none() {
            first = Some(w.now());
        }
        if read >= want {
            return (first.map(|first| w.now() - first), pushes);
        }
    }
    (None, pushes)
}

/// A parent and a child 200 ms apart, 32 streams with a 1 MiB backlog each: the child reads 30 MiB within 2 s of its
/// first record, where one push per round trip took 6 s.
#[test]
fn a_leaf_200_ms_away_reads_several_pushes_per_round_trip() {
    // 64 MiB unacknowledged on a connection, the push window's ceiling: the World's default 64 KiB would cap the link
    // at 320 KiB a round trip, below one push.
    let (mut w, _) = far(0, &["origin", "leaf"], 64 << 20, |_| {});
    let keys: Vec<StreamKey> = (0..32).map(|n| StreamKey::new("md", format!("k{n}"))).collect();
    backlog(&mut w, &keys, 256);
    let reads: Vec<Read> = keys.iter().map(|k| collect(&mut w, "leaf", k.clone(), StartPosition::First)).collect();
    let (took, pushes) = time_to_read(&mut w, &reads, 30 << 20);
    println!("30 MiB read in {took:?}, at most {pushes} pushes in flight");
    assert!(took.is_some_and(|t| t < Duration::from_secs(2)), "30 MiB in {took:?}");
    read_through(&mut w, 0, &["leaf"; 32], &reads, 256);
    println!("world digest far_wide: {:016x}", w.digest());
}

/// One stream 200 ms away with a 24 MiB backlog: up to half its branch in flight per round trip, so the child
/// reads 20 MiB within 2 s of its first record, and its default subscriber holds them applied back to back.
#[test]
fn one_stream_200_ms_away_reads_several_pushes_per_round_trip() {
    // As above: 64 MiB unacknowledged on a connection, and 24 MiB of history on one stream, past the 16 MiB default.
    let (mut w, _) = far(0, &["origin", "leaf"], 64 << 20, |cfg| cfg.core.store.max_bytes_per_stream = 32 << 20);
    backlog(&mut w, &[key()], 6144);
    let reads = [collect(&mut w, "leaf", key(), StartPosition::First)];
    let (took, pushes) = time_to_read(&mut w, &reads, 20 << 20);
    println!("20 MiB read in {took:?}, at most {pushes} pushes in flight");
    assert!(took.is_some_and(|t| t < Duration::from_secs(2)) && pushes >= 4, "20 MiB in {took:?}, {pushes} pushes");
    read_through(&mut w, 0, &["leaf"], &reads, 6144);
    println!("world digest far_deep: {:016x}", w.digest());
}

fn small_pushes(cfg: &mut StreamNodeConfig) {
    // The pushes of the default config at a sixteenth, so 64 seeds at once hold little: 64 KiB pushes, a window from
    // 128 KiB to 2 MiB, 16 KiB batches. One push a round trip is then 320 KiB a second, below the publisher's 800.
    (cfg.core.max_push_bytes, cfg.core.max_push_window) = (64 << 10, 2 << 20);
    (cfg.core.hub.max_batch_bytes, cfg.core.hub.max_batch_records) = (16 << 10, 256);
}

/// A live stream 200 ms from the leaf, published faster than one push a round trip carries, through a reset of the
/// session between relay and leaf, a cut of the leaf's answers, a pause and a starved leaf, then a restart of the
/// relay on a clock stepped back and a crash of the leaf: the leaf reads every record once and in order.
fn window_faults(seed: u64) {
    // 4 MiB unacknowledged on a connection, twice the window: the World's default 64 KiB would cap the link at 320 KiB
    // a round trip, one push.
    let (mut w, discovery) = far(seed, &["origin", "relay-a", "leaf"], 4 << 20, small_pushes);
    let read = collect(&mut w, "leaf", key(), StartPosition::First);
    w.run_until("the leaf pulls through the relay", Duration::from_secs(30), |w| {
        stream(w, "leaf").parent_of(&key()).as_deref() == Some("relay-a")
    });
    let stop = Rc::new(Cell::new(false));
    let stopped = stop.clone();
    let publishing = w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stopped.get() {
            n.stream.publish(&key(), (next..next + 8).map(|s| padded(s, 1024)).collect()).unwrap();
            next += 8;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        next - 1
    });
    let run = |w: &mut World<Node>, ms: std::ops::Range<u64>| {
        let d = Duration::from_millis(w.pick(ms));
        w.run_for(d);
    };
    run(&mut w, 1000..2000);
    w.log("reset relay-a leaf");
    w.reset("relay-a", "leaf");
    run(&mut w, 1000..2000);
    // Pushes arrive, their answers do not, past the request timeout (1 s): every one times out and goes again, and past
    // the downstream expiry (2 s) the relay drops the leaf, whose interests are cut too, until it is heard again.
    w.log("cut leaf -> relay-a");
    w.cut(&["leaf"], &["relay-a"]);
    run(&mut w, 1500..3000);
    w.heal();
    run(&mut w, 1000..2000);
    // Past the request timeout: the half-open case.
    let pause = Duration::from_millis(w.pick(1200..2500));
    w.log(format_args!("pause leaf {pause:?}"));
    w.pause("leaf", pause);
    run(&mut w, 2000..3000);
    let one_in = w.pick(100..500) as u32;
    w.log(format_args!("starve leaf 1 in {one_in}"));
    w.starve("leaf", one_in);
    run(&mut w, 2000..3000);
    w.starve("leaf", 1);
    run(&mut w, 1000..2000);
    let resent = stream(&w, "relay-a").stats().push_resent;
    assert!(resent > 0, "seed {seed}: the cut answers sent nothing again");
    // Its next boot's incarnation is older than the last: the leaf must not order the relay's boots by it. Memory
    // discovery refuses an older incarnation, so the dead one is expired first, as a health check would.
    let back = w.pick(1000..30_000) as i64;
    w.log(format_args!("step relay-a back {back} ms"));
    w.step_wall("relay-a", -back);
    crash(&mut w, "relay-a");
    discovery.expire("relay-a");
    run(&mut w, 0..500);
    w.restart("relay-a");
    run(&mut w, 2000..3000);
    let last = read.borrow().clone().unwrap_or_else(|e| panic!("seed {seed}: the leaf read {e}"));
    crash(&mut w, "leaf");
    run(&mut w, 0..500);
    w.restart("leaf");
    let start = StartPosition::After(Cursor { generation: 1, sequence: last });
    let read = collect(&mut w, "leaf", key(), start);
    run(&mut w, 2000..3000);
    stop.set(true);
    let published = w.join(publishing).unwrap();
    read_through(&mut w, seed, &["leaf"], &[read], published);
    println!("world digest window_faults seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides() {
    check_seeds(seeds(0..64), window_faults).unwrap();
}

/// What a `Live` follower saw: how many times it was told `ResetRequired(HistoryUnavailable)` and subscribed again,
/// and the last record it read; or the first thing wrong, a record that does not follow the last of its generation.
type Followed = Rc<RefCell<Result<(u32, u64), String>>>;

/// A `Live` follower on `on` that subscribes again after each `ResetRequired(HistoryUnavailable)`.
fn follow_checked(w: &mut World<Node>, on: &str) -> Followed {
    let seen = Followed::new(RefCell::new(Ok((0, 0))));
    let s = seen.clone();
    drop(w.spawn_on(on, move |n| async move {
        loop {
            let mut subscription = n.stream.subscribe(&key(), SubscribeOptions::default()).unwrap();
            let mut last: Option<Cursor> = None;
            while let Some(event) = subscription.next().await {
                let wrong = match event {
                    StreamEvent::Batch(batch) => {
                        let continues =
                            last.is_none_or(|l| l.generation != batch.generation() || batch.first() == l.sequence + 1);
                        let at = last.map(|l| l.sequence);
                        last = Some(Cursor { generation: batch.generation(), sequence: batch.last() });
                        (!continues)
                            .then(|| format!("{} after {at:?}, generation {}", batch.first(), batch.generation()))
                    }
                    StreamEvent::ResetRequired(ResetReason::HistoryUnavailable) => break,
                    terminal if terminal.is_terminal() => Some(format!("{terminal:?} after {last:?}")),
                    _ => None,
                };
                let mut seen = s.borrow_mut();
                match (wrong, &mut *seen) {
                    (Some(wrong), Ok(_)) => *seen = Err(wrong),
                    (None, Ok((_, read))) => *read = last.map_or(*read, |l| l.sequence),
                    _ => {}
                }
            }
            if let Ok((resets, _)) = &mut *s.borrow_mut() {
                *resets += 1;
            }
        }
    }));
    seen
}

/// A `Live` follower on a node whose link to the origin carries less than the origin publishes: the node falls out of
/// the origin's ring, and the follower is told `ResetRequired(HistoryUnavailable)` each time, never moved past the
/// records it lost by a `Ready`.
fn slow_link(seed: u64) {
    // 1 MB/s 100 ms each way, with 4 MiB unacknowledged on a connection, above what the link carries in a round trip.
    let wire = Wire { latency: Duration::from_millis(100), bandwidth: 1_000_000, ..Wire::default() };
    let cfg = WorldConfig { seed, wire, window: 4 << 20, ..WorldConfig::default() };
    // A ring of 1 MiB, half a second of what the origin publishes.
    let set = |cfg: &mut StreamNodeConfig| cfg.core.store.max_bytes_per_stream = 1 << 20;
    let (mut w, _) = world_with(cfg, &["origin", "relay-a"], Compression::None, set);
    let seen = follow_checked(&mut w, "relay-a");
    w.run_until("the follower reads", Duration::from_secs(30), |w| stream(w, "relay-a").cursor(&key()).is_some());
    let stop = Rc::new(Cell::new(false));
    let stopped = stop.clone();
    // 2 MB/s.
    let publishing = w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stopped.get() {
            n.stream.publish(&key(), (next..next + 16).map(|s| padded(s, 1024)).collect()).unwrap();
            next += 16;
            tokio::time::sleep(Duration::from_millis(8)).await;
        }
    });
    w.run_for(Duration::from_secs(20));
    stop.set(true);
    w.join(publishing).unwrap();
    let seen = seen.borrow().clone();
    let resets = stream(&w, "relay-a").stats().local_resets;
    match seen {
        Ok((told, read)) => assert!(told > 0 && read > 0, "seed {seed}: told {told} times, read to {read}"),
        Err(wrong) => panic!("seed {seed}: the follower read {wrong}; its node reset {resets} times"),
    }
    println!("world digest slow_link seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_live_follower_behind_a_link_slower_than_the_origin_is_reset_and_never_skips() {
    check_seeds(seeds(0..16), slow_link).unwrap();
}
