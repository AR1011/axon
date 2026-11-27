//! An origin that opens a namespace's keys when something wants them, on the World: an application reads the
//! node's demand feed and opens each key told wanted, closes each told unwanted, while followers come, leave and crash
//! and the origin restarts.
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, View};
use axon_net::SharedSecret;
use axon_stream::*;
use axon_testkit::world::{Crash, Host, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

mod common;
use common::{SECRET, role};

/// Longer than a leaf takes to ask again after its parent's restart, as the linger must be.
const LINGER: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Node {
    stream: StreamNode,
    boot: u32,
    cluster: Cluster,
    _system: ActorSystem,
}

fn job() -> StreamKey {
    StreamKey::new("jobs", "j0")
}

/// Which keys of `jobs` an owner opens: from its view of the cluster, its own name and the key.
type Owns = fn(&View, &str, &StreamKey) -> bool;

/// The origin keeps `jobs` on demand; an owner keeps the keys `Owns` gives it on demand and pulls the others from the
/// other owners; every other node is a leaf that asks the origins.
async fn boot(host: Host, discovery: MemoryDiscovery, owns: Option<Owns>) -> Node {
    let origin = host.name() == "origin" || owns.is_some();
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(SharedSecret::new(SECRET).unwrap()), discovery);
    common::short_cluster(&mut cfg);
    cfg.labels = role(if origin { "origin" } else { "leaf" });
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = if origin && owns.is_none() { vec![] } else { vec![role("origin")] };
    let mut cfg = StreamNodeConfig::new(upstream);
    common::short_stream(&mut cfg);
    if let Some(owns) = owns {
        let (view, me) = (cluster.clone(), host.name().to_owned());
        cfg = cfg.on_demand("jobs", LINGER).claim("jobs", move |key| owns(&view.view(), &me, key));
    } else if origin {
        cfg = cfg.on_demand("jobs", LINGER).claim("jobs", |key| key.name().starts_with('j'));
    }
    let stream = StreamNode::start(&cluster, cfg).await.unwrap();
    Node { stream, boot: host.boot(), cluster, _system: system }
}

fn world(seed: u64, names: &[&str], owns: Option<Owns>) -> World<Node> {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in names {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone(), owns));
    }
    w.run_until("every node booted", Duration::from_secs(30), |w| w.nodes().count() == names.len());
    w
}

fn node<'w>(w: &'w World<Node>, name: &str) -> Option<&'w Node> {
    w.nodes().find(|(n, _)| *n == name).map(|(_, node)| node)
}

fn cursor_on(w: &World<Node>, name: &str, key: &StreamKey) -> Option<Cursor> {
    node(w, name)?.stream.cursor(key)
}

/// What the application was told, in order, with the boot of the origin it ran on.
type Told = Rc<RefCell<Vec<(u32, Demand)>>>;

/// The application on the origin: it opens each key wanted, each open in the next generation from 1, and closes each
/// unwanted. `opens` outlives the origin's restarts, as a clock would.
fn application(w: &mut World<Node>, on: &str, told: &Told, opens: &Rc<Cell<u64>>) {
    let (told, opens) = (told.clone(), opens.clone());
    drop(w.spawn_on(on, move |n| async move {
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
                // A key whose open failed is told `Unwanted` too: nothing to close.
                Demand::Unwanted(key) => match n.stream.close_stream(&key) {
                    Ok(()) | Err(StreamError::UnknownStream) => {}
                    Err(error) => panic!("closing {key:?}: {error:?}"),
                },
            }
        }
    }));
}

/// The batches a follower read, by their cursors, and any end.
type Seen = Rc<RefCell<Vec<Result<Cursor, String>>>>;

/// A `Live` follower of `key` on `on` that leaves once it has read `through`, or never with `None`.
fn follow(w: &mut World<Node>, on: &str, key: StreamKey, through: Option<Cursor>) -> Seen {
    let seen = Seen::default();
    let s = seen.clone();
    drop(w.spawn_on(on, move |n| async move {
        let mut follower = n.stream.subscribe(&key, SubscribeOptions::default()).unwrap();
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

fn publish(w: &mut World<Node>, on: &str, key: StreamKey, records: u64) {
    let published = w.block_on(on, move |n| async move {
        for _ in 0..records {
            n.stream.publish(&key, vec![Bytes::from_static(b"x")]).unwrap();
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
        let mut w = world(seed, &["origin", "leaf-a", "leaf-b"], None);
        let told = Told::default();
        application(&mut w, "origin", &told, &Rc::default());
        let two = Cursor { generation: 1, sequence: 2 };
        let a = follow(&mut w, "leaf-a", job(), None);
        let b = follow(&mut w, "leaf-b", job(), Some(two));
        // Each leaf holds the open's position before anything is published: a `Live` follower reads what comes after.
        w.run_until("both leaves follow the opened key", Duration::from_secs(10), |w| {
            w.nodes().all(|(_, node)| node.stream.cursor(&job()).is_some())
        });
        publish(&mut w, "origin", job(), 2);
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
        let mut w = world(seed, &["origin", "leaf"], None);
        let (told, opens) = (Told::default(), Rc::default());
        application(&mut w, "origin", &told, &opens);
        let seen = follow(&mut w, "leaf", job(), None);
        w.run_until("the origin is told", Duration::from_secs(10), |_| !told.borrow().is_empty());
        publish(&mut w, "origin", job(), 1);
        let one = Cursor { generation: 1, sequence: 1 };
        w.run_until("the leaf reads the record", Duration::from_secs(10), |_| read(&seen, one));
        let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
        w.crash("origin", how);
        w.restart("origin");
        application(&mut w, "origin", &told, &opens);
        w.run_until("the restarted origin is told", Duration::from_secs(20), |_| {
            told.borrow().iter().any(|(boot, _)| *boot == 2)
        });
        publish(&mut w, "origin", job(), 1);
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
        let mut w = world(seed, &["origin", "leaf"], None);
        let ms = Duration::from_millis(w.pick(1..40));
        let stall = Duration::from_millis(200);
        for (from, to) in [("origin", "leaf"), ("leaf", "origin")] {
            w.wire(from, to, Wire { latency: ms, jitter: ms, stall_every: 40, stall, ..Wire::default() });
        }
        let told = Told::default();
        application(&mut w, "origin", &told, &Rc::default());
        let seen = follow(&mut w, "leaf", job(), None);
        w.run_until("the leaf follows the opened key", Duration::from_secs(10), |w| {
            w.nodes().all(|(_, node)| node.stream.cursor(&job()).is_some())
        });
        publish(&mut w, "origin", job(), 1);
        let one = Cursor { generation: 1, sequence: 1 };
        w.run_until("the leaf reads the record", Duration::from_secs(10), |_| read(&seen, one));
        // Short: within `downstream_expiry` (2 s), so the origin may drop the leaf only just before the heal, and the
        // leaf asks again well inside the linger. Long: past `downstream_expiry` and the linger with room to spare.
        // Each seed's arms from the seed, so the first four seeds run all four.
        let (long, paused) = (seed % 2 == 1, seed % 4 < 2);
        let gone = Duration::from_millis(if long { w.pick(8_000..12_000) } else { w.pick(200..1_500) });
        if paused {
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
        publish(&mut w, "origin", job(), 1);
        w.run_until("the leaf reads the next record", Duration::from_secs(10), |_| read(&seen, next));
        assert_eq!(*seen.borrow(), [Ok(one), Ok(next)], "seed {seed}, gone {gone:?}");
        let wanted = (1, Demand::Wanted(job()));
        let expected = if long { vec![wanted.clone(), (1, Demand::Unwanted(job())), wanted] } else { vec![wanted] };
        assert_eq!(*told.borrow(), expected, "seed {seed}, gone {gone:?}");
        println!("world digest on_demand_gone seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// The last by name of the origins a node's view holds, which owns every key of `jobs` there.
fn last_origin(view: &View) -> Option<&str> {
    let origins = view.members.iter().filter(|m| m.available() && m.record.has_labels(&role("origin")));
    origins.map(|m| m.record.id.as_str()).max()
}

fn last_origin_owns(view: &View, me: &str, _: &StreamKey) -> bool {
    last_origin(view) == Some(me)
}

/// `a` owns the even keys, `b` the odd ones.
fn owner(key: &StreamKey) -> &'static str {
    if key.name().ends_with(['0', '2', '4', '6', '8']) { "a" } else { "b" }
}

fn parity_owns(_: &View, me: &str, key: &StreamKey) -> bool {
    owner(key) == me
}

/// What an application was told, without the boots: `wanted j0`.
fn changes(told: &Told) -> Vec<String> {
    let change = |demand: &Demand| match demand {
        Demand::Wanted(key) => format!("wanted {}", key.name()),
        Demand::Unwanted(key) => format!("unwanted {}", key.name()),
    };
    told.borrow().iter().map(|(_, demand)| change(demand)).collect()
}

/// Three owners, each the owner of every key while it is the last origin in its view, and the others pull the key from
/// it. `b` follows a key `c` owns. `c` crashes: the membership makes the key `b`'s, which stops pulling it, is told it
/// is wanted and opens it. `c` comes back: the key is `c`'s again, `b`'s application closes it, and `b` pulls it for
/// its follower from `c`, which is told and opens it. The follower reads each open in turn. Before, `b` went on pulling
/// the key with `c` gone and was never told, and after its close its follower waited for an open of its own.
#[test]
fn a_key_moves_to_the_owner_the_membership_names_and_back() {
    check_seeds(seeds(0..32), |seed| {
        let mut w = world(seed, &["a", "b", "c"], Some(last_origin_owns as Owns));
        let opens = Rc::default();
        let [a, b, c] = [Told::default(), Told::default(), Told::default()];
        for (on, told) in [("a", &a), ("b", &b), ("c", &c)] {
            application(&mut w, on, told, &opens);
        }
        w.run_until("every node sees c", Duration::from_secs(10), |w| {
            w.nodes().count() == 3 && w.nodes().all(|(_, n)| last_origin(&n.cluster.view()) == Some("c"))
        });
        let seen = follow(&mut w, "b", job(), None);
        w.run_until("b follows the key c opened", Duration::from_secs(10), |w| cursor_on(w, "b", &job()).is_some());
        publish(&mut w, "c", job(), 1);
        let first = Cursor { generation: 1, sequence: 1 };
        w.run_until("b reads c's record", Duration::from_secs(10), |_| read(&seen, first));

        w.crash("c", Crash::Machine);
        let crashed = w.now();
        w.run_until("b sees c go", Duration::from_secs(20), |w| {
            node(w, "b").is_some_and(|n| last_origin(&n.cluster.view()) == Some("b"))
        });
        let seen_gone = w.now() - crashed;
        w.run_until("b is told the key is wanted", Duration::from_secs(20), |_| !b.borrow().is_empty());
        // At the reconcile that sees the change, with no refusal or its backoff between.
        let late = w.now() - crashed - seen_gone;
        assert!(late < Duration::from_millis(100), "seed {seed}: told {late:?} after b saw c go");
        let second = Cursor { generation: 2, sequence: 1 };
        w.run_until("b opens the key", Duration::from_secs(10), |w| {
            cursor_on(w, "b", &job()).is_some_and(|c| c.generation == 2)
        });
        publish(&mut w, "b", job(), 1);
        w.run_until("b's follower reads b's record", Duration::from_secs(10), |_| read(&seen, second));

        w.restart("c");
        application(&mut w, "c", &c, &opens);
        w.run_until("b sees c again", Duration::from_secs(20), |w| {
            node(w, "b").is_some_and(|n| last_origin(&n.cluster.view()) == Some("c"))
        });
        // b's application learns, as the claim does, that the key is no longer its own.
        w.block_on("b", |n| async move { n.stream.close_stream(&job()) }).unwrap().unwrap();
        w.run_until("b follows the key c opened again", Duration::from_secs(20), |w| {
            ["b", "c"].iter().all(|on| cursor_on(w, on, &job()).is_some_and(|c| c.generation == 3))
        });
        publish(&mut w, "c", job(), 1);
        let third = Cursor { generation: 3, sequence: 1 };
        w.run_until("b's follower reads c's record", Duration::from_secs(10), |_| read(&seen, third));
        assert_eq!(*seen.borrow(), [Ok(first), Ok(second), Ok(third)], "seed {seed}");
        assert!(a.borrow().is_empty(), "seed {seed}");
        assert_eq!(*b.borrow(), [(1, Demand::Wanted(job()))], "seed {seed}");
        assert_eq!(*c.borrow(), [(1, Demand::Wanted(job())), (2, Demand::Wanted(job()))], "seed {seed}");
        println!("world digest on_demand_owner_moves seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// `a` owns the even keys and `b` the odd ones, and each pulls the other's from it. Each follows every key: every want
/// reaches its owner, through the other's refusal where it asked the wrong one first, each follower reads each record
/// once and in order, and `a` closing a key it is told is wanted opens it again, its followers on both nodes going on
/// in the new generation. No want goes round between the two: no loop is refused, and once settled they send each
/// other at most the whole set once per resync.
#[test]
fn two_owners_that_pull_each_others_keys_reach_each_owner_with_no_loop() {
    check_seeds(seeds(0..32), |seed| {
        let keys: Vec<StreamKey> = (0..4).map(|n| StreamKey::new("jobs", format!("j{n}"))).collect();
        let mut w = world(seed, &["a", "b"], Some(parity_owns as Owns));
        let opens = Rc::default();
        let [a, b] = [Told::default(), Told::default()];
        for (on, told) in [("a", &a), ("b", &b)] {
            application(&mut w, on, told, &opens);
        }
        let mut seen = Vec::new();
        for on in ["a", "b"] {
            for key in &keys {
                seen.push((on, key.clone(), follow(&mut w, on, key.clone(), None)));
            }
        }
        w.run_until("every key is open at its owner and followed", Duration::from_secs(20), |w| {
            ["a", "b"].iter().all(|on| keys.iter().all(|k| cursor_on(w, on, k).is_some()))
        });
        let round = |w: &mut World<Node>| {
            for key in &keys {
                publish(w, owner(key), key.clone(), 1);
            }
            w.run_for(Duration::from_millis(500));
        };
        for _ in 0..5 {
            round(&mut w);
        }
        // Closed while both nodes want it: told wanted again, and opened in the next generation.
        let j0 = keys[0].clone();
        let before = cursor_on(&w, "a", &j0).expect("open").generation;
        w.block_on("a", move |n| async move { n.stream.close_stream(&j0) }).unwrap().unwrap();
        w.run_until("a opens the key again, and b follows it", Duration::from_secs(20), |w| {
            ["a", "b"].iter().all(|on| cursor_on(w, on, &keys[0]).is_some_and(|c| c.generation > before))
        });
        for _ in 0..5 {
            round(&mut w);
        }
        let sent = |w: &World<Node>| ["a", "b"].map(|on| node(w, on).expect("up").stream.stats().interest_keys);
        let settled = sent(&w);
        let quiet = Duration::from_secs(180);
        w.run_for(quiet);
        round(&mut w);
        w.run_for(Duration::from_secs(1));
        // Each pulls two keys: the whole set once per resync (60 s) at most, plus one resync that falls at the start.
        let most = 2 * (quiet.as_secs() / 60 + 1);
        for (on, (now, then)) in ["a", "b"].iter().zip(sent(&w).into_iter().zip(settled)) {
            assert!(now - then <= most, "seed {seed}: {on} sent {} wants and drops once settled", now - then);
            let loops = node(&w, on).expect("up").stream.stats().refusals[Status::Loop as usize];
            assert_eq!(loops, 0, "seed {seed}: {on} refused a loop");
        }
        for (on, key, seen) in &seen {
            let seen = seen.borrow();
            let generation = |c: &Result<Cursor, String>| c.as_ref().map(|c| c.generation).unwrap_or_default();
            let gens: Vec<u64> = seen.iter().map(generation).collect();
            let mut runs: Vec<(u64, Vec<u64>)> = Vec::new();
            for c in seen.iter() {
                let c = c.as_ref().unwrap_or_else(|e| panic!("seed {seed}: {on} {key}: {e}"));
                match runs.last_mut() {
                    Some((g, s)) if *g == c.generation => s.push(c.sequence),
                    _ => runs.push((c.generation, vec![c.sequence])),
                }
            }
            let total: usize = runs.iter().map(|(_, s)| s.len()).sum();
            assert!(gens.windows(2).all(|g| g[0] <= g[1]), "seed {seed}: {on} {key}: generations {gens:?}");
            for (_, s) in &runs {
                assert!(s.windows(2).all(|p| p[1] == p[0] + 1), "seed {seed}: {on} {key}: {runs:?}");
            }
            assert_eq!(total, 11, "seed {seed}: {on} {key}: {runs:?}");
        }
        // The first two in either order: each is told as its first want reaches it.
        let (mut a, mut b) = (changes(&a), changes(&b));
        a[..2].sort();
        b.sort();
        assert_eq!(a, ["wanted j0", "wanted j2", "wanted j0"], "seed {seed}");
        assert_eq!(b, ["wanted j1", "wanted j3"], "seed {seed}");
        println!("world digest on_demand_two_owners seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}
