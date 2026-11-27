//! An origin that opens a namespace's keys when something wants them, on the World: an application reads the
//! node's demand feed and opens each key told wanted, closes each told unwanted, while followers come, leave and crash
//! and the origin restarts.
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::*;
use axon_testkit::world::{Crash, Host, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

mod common;
use common::{SECRET, labels};

/// Longer than a leaf takes to ask again after its parent's restart, as the linger must be.
const LINGER: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Node {
    stream: StreamNode,
    boot: u32,
    _cluster: Cluster,
    _system: ActorSystem,
}

fn job() -> StreamKey {
    StreamKey::new("jobs", "j0")
}

/// The origin keeps `jobs` on demand; every other node is a leaf that asks it.
async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let origin = host.name() == "origin";
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(SharedSecret::new(SECRET).unwrap()), discovery);
    common::short_cluster(&mut cfg);
    cfg.labels = labels(&[("role", if origin { "origin" } else { "leaf" })]);
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = if origin { vec![] } else { vec![labels(&[("role", "origin")])] };
    let mut cfg = StreamNodeConfig::new(upstream);
    common::short_stream(&mut cfg);
    if origin {
        cfg = cfg.on_demand("jobs", LINGER).claim("jobs", |key| key.name().starts_with('j'));
    }
    let stream = StreamNode::start(&cluster, cfg).await.unwrap();
    Node { stream, boot: host.boot(), _cluster: cluster, _system: system }
}

fn world(seed: u64, names: &[&str]) -> World<Node> {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in names {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    w.run_until("every node booted", Duration::from_secs(30), |w| w.nodes().count() == names.len());
    w
}

/// What the application was told, in order, with the boot of the origin it ran on.
type Told = Rc<RefCell<Vec<(u32, Demand)>>>;

/// The application on the origin: it opens each key wanted, each open in the next generation from 1, and closes each
/// unwanted. `opens` outlives the origin's restarts, as a clock would.
fn application(w: &mut World<Node>, told: &Told, opens: &Rc<Cell<u64>>) {
    let (told, opens) = (told.clone(), opens.clone());
    drop(w.spawn_on("origin", move |n| async move {
        let open = async |key: StreamKey| {
            opens.set(opens.get() + 1);
            n.stream.open_stream(&key, opens.get(), DeliveryPolicy::ReplayRequired).await.unwrap()
        };
        let (wanted, mut feed) = n.stream.demand("jobs").unwrap();
        for key in wanted {
            told.borrow_mut().push((n.boot, Demand::Wanted(key.clone())));
            open(key).await;
        }
        while let Some(demand) = feed.next().await {
            told.borrow_mut().push((n.boot, demand.clone()));
            match demand {
                Demand::Wanted(key) => open(key).await,
                Demand::Unwanted(key) => n.stream.close_stream(&key).unwrap(),
            }
        }
    }));
}

/// The batches a follower read, by their cursors, and any end.
type Seen = Rc<RefCell<Vec<Result<Cursor, String>>>>;

/// A `Live` follower on `on` that leaves once it has read `through`, or never with `None`.
fn follow(w: &mut World<Node>, on: &str, through: Option<Cursor>) -> Seen {
    let seen = Seen::default();
    let s = seen.clone();
    drop(w.spawn_on(on, move |n| async move {
        let mut follower = n.stream.subscribe(&job(), SubscribeOptions::default()).unwrap();
        while let Some(event) = follower.next().await {
            match event {
                StreamEvent::Batch(batch) => s.borrow_mut().push(Ok(batch.cursor())),
                event if event.is_terminal() => s.borrow_mut().push(Err(format!("{event:?}"))),
                _ => {}
            }
            if s.borrow().last().is_some_and(|c| c.as_ref().ok() == through.as_ref()) {
                return;
            }
        }
    }));
    seen
}

fn publish(w: &mut World<Node>, records: u64) {
    let published = w.block_on("origin", move |n| async move {
        for _ in 0..records {
            n.stream.publish(&job(), vec![Bytes::from_static(b"x")]).unwrap();
        }
    });
    published.unwrap();
}

fn read(seen: &Seen, through: Cursor) -> bool {
    seen.borrow().iter().any(|c| c.as_ref() == Ok(&through))
}

/// Two leaves want a key the origin has not opened: they wait, and the application is told once and opens it. One
/// leaf crashes and the other leaves: the application is told the key is unwanted a linger after the last one went,
/// not before, and closes it.
#[test]
fn a_key_is_opened_when_leaves_want_it_and_closed_a_linger_after_they_are_gone() {
    check_seeds(seeds(0..32), |seed| {
        let mut w = world(seed, &["origin", "leaf-a", "leaf-b"]);
        let told = Told::default();
        application(&mut w, &told, &Rc::default());
        let two = Cursor { generation: 1, sequence: 2 };
        let a = follow(&mut w, "leaf-a", None);
        let b = follow(&mut w, "leaf-b", Some(two));
        // Each leaf holds the open's position before anything is published: a `Live` follower reads what comes after.
        w.run_until("both leaves follow the opened key", Duration::from_secs(10), |w| {
            w.nodes().all(|(_, node)| node.stream.cursor(&job()).is_some())
        });
        publish(&mut w, 2);
        w.run_until("both leaves read the records", Duration::from_secs(10), |_| read(&a, two) && read(&b, two));
        // leaf-b has left; leaf-a goes with its machine.
        w.crash("leaf-a", Crash::Machine);
        let origin = |w: &World<Node>| w.nodes().find(|(n, _)| *n == "origin").expect("up").1.stream.stats();
        w.run_until("nothing wants the key", Duration::from_secs(20), |w| origin(w).hub.demand_keys == 0);
        let gone = w.now();
        let unwanted = (1, Demand::Unwanted(job()));
        w.run_until("the key is told unwanted", Duration::from_secs(20), |_| told.borrow().contains(&unwanted));
        // At the first reconcile past the linger: they come every `tick`, 25 ms here.
        let after = w.now() - gone;
        assert!(after >= LINGER && after < LINGER + Duration::from_millis(200), "seed {seed}: told {after:?} after");
        assert_eq!(*told.borrow(), [(1, Demand::Wanted(job())), unwanted], "seed {seed}");
        w.run_until("the application closes the key", Duration::from_secs(1), |w| {
            w.nodes().all(|(_, node)| node.stream.cursor(&job()).is_none())
        });
        for seen in [&a, &b] {
            assert_eq!(*seen.borrow(), [Ok(Cursor { generation: 1, sequence: 1 }), Ok(two)], "seed {seed}");
        }
        println!("world digest on_demand_linger seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// The origin crashes and restarts while a leaf wants a key: the leaf's follower waits, the restarted origin is told
/// the key is wanted again and opens it in a new generation, and the follower goes on in it.
#[test]
fn a_restarted_origin_is_told_again_of_the_keys_leaves_want() {
    check_seeds(seeds(0..32), |seed| {
        let mut w = world(seed, &["origin", "leaf"]);
        let (told, opens) = (Told::default(), Rc::default());
        application(&mut w, &told, &opens);
        let seen = follow(&mut w, "leaf", None);
        w.run_until("the origin is told", Duration::from_secs(10), |_| !told.borrow().is_empty());
        publish(&mut w, 1);
        let one = Cursor { generation: 1, sequence: 1 };
        w.run_until("the leaf reads the record", Duration::from_secs(10), |_| read(&seen, one));
        let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
        w.crash("origin", how);
        w.restart("origin");
        application(&mut w, &told, &opens);
        w.run_until("the restarted origin is told", Duration::from_secs(20), |_| {
            told.borrow().iter().any(|(boot, _)| *boot == 2)
        });
        publish(&mut w, 1);
        let again = Cursor { generation: 2, sequence: 1 };
        w.run_until("the leaf reads the new generation", Duration::from_secs(10), |_| read(&seen, again));
        assert_eq!(*seen.borrow(), [Ok(one), Ok(again)], "seed {seed}");
        assert_eq!(*told.borrow(), [(1, Demand::Wanted(job())), (2, Demand::Wanted(job()))], "seed {seed}");
        println!("world digest on_demand_restart seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// A leaf that wants a key is paused or cut off from the origin, over a slow link that loses chunks. Gone for less
/// than the origin takes to drop it plus the linger, it costs nothing: the key stays open and the follower goes on in
/// its generation. Gone for longer, the key is told unwanted and closed, told wanted again once the leaf is back, and
/// the follower goes on in the generation opened next. Never a reset.
#[test]
fn a_leaf_gone_for_less_than_the_linger_keeps_its_key_open_and_one_gone_longer_has_it_opened_again() {
    check_seeds(seeds(0..32), |seed| {
        let mut w = world(seed, &["origin", "leaf"]);
        let ms = Duration::from_millis(w.pick(1..40));
        let stall = Duration::from_millis(200);
        for (from, to) in [("origin", "leaf"), ("leaf", "origin")] {
            w.wire(from, to, Wire { latency: ms, jitter: ms, stall_every: 40, stall, ..Wire::default() });
        }
        let told = Told::default();
        application(&mut w, &told, &Rc::default());
        let seen = follow(&mut w, "leaf", None);
        w.run_until("the leaf follows the opened key", Duration::from_secs(10), |w| {
            w.nodes().all(|(_, node)| node.stream.cursor(&job()).is_some())
        });
        publish(&mut w, 1);
        let one = Cursor { generation: 1, sequence: 1 };
        w.run_until("the leaf reads the record", Duration::from_secs(10), |_| read(&seen, one));
        // Short: within `downstream_expiry` (2 s), so the origin may drop the leaf only just before the heal, and the
        // leaf asks again well inside the linger. Long: past `downstream_expiry` and the linger with room to spare.
        let long = w.pick(0..2) == 1;
        let gone = Duration::from_millis(if long { w.pick(8_000..12_000) } else { w.pick(200..1_500) });
        if w.pick(0..2) == 0 {
            w.pause("leaf", gone);
            w.run_for(gone);
        } else {
            w.partition(&["leaf"], &["origin"]);
            w.run_for(gone);
            w.heal();
        }
        let next = Cursor { generation: if long { 2 } else { 1 }, sequence: if long { 1 } else { 2 } };
        w.run_until("the leaf follows the key again", Duration::from_secs(20), |w| {
            w.nodes().all(|(_, node)| node.stream.cursor(&job()).is_some_and(|c| c.generation == next.generation))
        });
        publish(&mut w, 1);
        w.run_until("the leaf reads the next record", Duration::from_secs(10), |_| read(&seen, next));
        assert_eq!(*seen.borrow(), [Ok(one), Ok(next)], "seed {seed}, gone {gone:?}");
        let wanted = (1, Demand::Wanted(job()));
        let expected = if long { vec![wanted.clone(), (1, Demand::Unwanted(job())), wanted] } else { vec![wanted] };
        assert_eq!(*told.borrow(), expected, "seed {seed}, gone {gone:?}");
        println!("world digest on_demand_gone seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}
