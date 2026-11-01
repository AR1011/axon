//! Streams on lanes in the World: an origin and a leaf, the namespace `bulk` on `Bulk` beside `md` on `Live`. `Bulk`
//! filling the link, `Bulk` cut both ways and reset while `Control` stays up, and the leaf restarting.
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use axon::{ActorSystem, Lane};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::*;
use axon_testkit::world::{Crash, Host, Task, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

mod common;
use common::{SECRET, key, role};

#[derive(Clone)]
struct Node {
    stream: StreamNode,
    _cluster: Cluster,
    _system: ActorSystem,
}

fn bulk() -> StreamKey {
    StreamKey::new("bulk", "dump")
}

/// With the timings of `relay.rs`. Each boot of the origin opens both keys as a new generation; the leaf pulls them.
async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let origin = host.name() == "origin";
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    common::short_cluster(&mut cfg);
    cfg.labels = role(if origin { "origin" } else { "leaf" });
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = if origin { vec![] } else { vec![role("origin")] };
    let mut cfg = StreamNodeConfig::new(upstream).lane("bulk", Lane::Bulk);
    common::short_stream(&mut cfg);
    // The first test's 32 MiB backlog, past the 16 MiB default.
    cfg.core.store.max_bytes_per_stream = 40 << 20;
    let stream = StreamNode::start(&cluster, cfg).await.unwrap();
    if origin {
        for k in [key(), bulk()] {
            stream.open_stream(&k, u64::from(host.boot()), DeliveryPolicy::ReplayRequired).await.unwrap();
        }
    }
    Node { stream, _cluster: cluster, _system: system }
}

fn world(seed: u64, wire: Wire) -> World<Node> {
    let (mut w, discovery) = (World::new(WorldConfig { seed, wire, ..WorldConfig::default() }), MemoryDiscovery::new());
    for name in ["origin", "leaf"] {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    w.run_until("both nodes booted", Duration::from_secs(30), |w| w.nodes().count() == 2);
    w
}

fn stream<'w>(w: &'w World<Node>, name: &str) -> &'w StreamNode {
    &w.nodes().find(|(n, _)| *n == name).unwrap_or_else(|| panic!("{name} is down")).1.stream
}

/// The last record the leaf read, or what was wrong: anything but the next number, or an end.
type Read = Rc<RefCell<Result<u64, String>>>;
/// Each `md` record's time from its publish to its read, in µs.
type Took = Rc<RefCell<Vec<u64>>>;

/// Records start with their sequence numbers, and those of `md` then carry their publish time in µs.
fn collect(w: &mut World<Node>, key: StreamKey, start: StartPosition) -> (Read, Took) {
    let after = if let StartPosition::After(cursor) = start { cursor.sequence } else { 0 };
    let (read, took) = (Read::new(RefCell::new(Ok(after))), Took::default());
    let (r, t, timed) = (read.clone(), took.clone(), key == common::key());
    drop(w.spawn_on("leaf", move |n| async move {
        let mut subscription = n.stream.subscribe(&key, SubscribeOptions { start, ..Default::default() }).unwrap();
        while let Some(event) = subscription.next().await {
            let last = r.borrow().clone().unwrap();
            let wrong = match event {
                StreamEvent::Batch(batch) => {
                    (batch.first()..).zip(batch.records()).try_fold(last, |last, (s, record)| {
                        if s != last + 1 || !record.starts_with(&s.to_be_bytes()) {
                            return Err(format!("{s} after {last}"));
                        }
                        if let Some(sent) = record.get(8..16).filter(|_| timed) {
                            let sent = u64::from_be_bytes(sent.try_into().unwrap());
                            t.borrow_mut().push(axon::trace::unix_micros().saturating_sub(sent));
                        }
                        Ok(s)
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
    (read, took)
}

fn last(read: &Read) -> u64 {
    read.borrow().clone().unwrap_or_else(|e| panic!("the leaf read {e}"))
}

/// Waits up to 30 s without panicking, so a reader that stalled is named with what it read and the leaf's events.
fn read_through(w: &mut World<Node>, seed: u64, reads: &[(&Read, u64)]) {
    let until = w.now() + Duration::from_secs(30);
    while w.now() < until && reads.iter().any(|(r, want)| r.borrow().as_ref().is_ok_and(|last| last < want)) {
        w.run_for(Duration::from_millis(100));
    }
    for (read, want) in reads {
        let read = read.borrow().clone();
        if read != Ok(*want) {
            let events = w.block_on("leaf", |n| async move { n.stream.detail(0).events }).unwrap();
            panic!("seed {seed}: the leaf read {read:?}, not Ok({want})\n{events:#?}\n{}", w.trace_tail(30));
        }
    }
}

fn following(w: &World<Node>) -> bool {
    [key(), bulk()].iter().all(|k| stream(w, "leaf").parent_of(k).is_some())
}

/// Record `s`: its sequence number, then zeros up to `len` bytes.
fn padded(s: u64, len: usize) -> Bytes {
    let mut record = vec![0u8; len];
    record[..8].copy_from_slice(&s.to_be_bytes());
    Bytes::from(record)
}

/// Four records of 1 KiB on each key every 50 ms until `stop`: the last record published on each.
fn publish_both(w: &mut World<Node>, stop: &Rc<Cell<bool>>) -> Task<u64> {
    let stop = stop.clone();
    w.spawn_on("origin", move |n| async move {
        let mut next = 1u64;
        while !stop.get() {
            for k in [key(), bulk()] {
                n.stream.publish(&k, (next..next + 4).map(|s| padded(s, 1024)).collect()).unwrap();
            }
            next += 4;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        next - 1
    })
}

fn run(w: &mut World<Node>, ms: std::ops::Range<u64>) {
    let d = Duration::from_millis(w.pick(ms));
    w.run_for(d);
}

/// Origin and leaf 20 ms apart at 8 MiB/s. A 32 MiB backlog on `bulk` fills the link for about 4 s, while an `md`
/// record every 20 ms for 3 s reaches the leaf within 100 ms at the 99th percentile: `Bulk`'s connection holds at most
/// the World's 64 KiB window, 8 ms, at the link. On one push queue for both, each `md` record waited behind the push
/// window's 2 MiB of `bulk`, 250 ms or more.
#[test]
fn a_bulk_namespace_saturating_the_links_keeps_a_live_namespaces_records_fast() {
    check_seeds(seeds(0..4), |seed| {
        let wire = Wire { latency: Duration::from_millis(10), bandwidth: 8 << 20, ..Wire::default() };
        let mut w = world(seed, wire);
        let (md, took) = collect(&mut w, key(), StartPosition::First);
        let (dump, _) = collect(&mut w, bulk(), StartPosition::First);
        w.run_until("the leaf follows both keys", Duration::from_secs(10), following);
        let records = 2048;
        let published = w.block_on("origin", move |n| async move {
            for first in (1..=records).step_by(32) {
                n.stream.publish(&bulk(), (first..first + 32).map(|s| padded(s, 16 << 10)).collect()).unwrap();
            }
        });
        published.unwrap();
        w.run_until("the first bulk record arrives", Duration::from_secs(10), |_| dump.borrow().as_ref() != Ok(&0));
        let timed = w.spawn_on("origin", |n| async move {
            let (end, mut s) = (tokio::time::Instant::now() + Duration::from_secs(3), 0u64);
            while tokio::time::Instant::now() < end {
                s += 1;
                let mut record = s.to_be_bytes().to_vec();
                record.extend_from_slice(&axon::trace::unix_micros().to_be_bytes());
                n.stream.publish(&key(), vec![record.into()]).unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            s
        });
        let timed = w.join(timed).unwrap();
        let moving = last(&dump);
        assert!(moving < records, "seed {seed}: the backlog was read before the timed records ended");
        read_through(&mut w, seed, &[(&md, timed), (&dump, records)]);

        let mut took = took.borrow().clone();
        took.sort_unstable();
        let p99 = Duration::from_micros(took[took.len() * 99 / 100]);
        let max = Duration::from_micros(took[took.len() - 1]);
        assert!(p99 <= Duration::from_millis(100), "seed {seed}: md p99 {p99:?}, max {max:?}");
        let rows = stream(&w, "origin").stats().downstream;
        let pushed = |lane| rows.iter().find(|d| d.node == "leaf" && d.lane == lane).map_or(0, |d| d.pushed_bytes);
        let (live, bulk) = (pushed(Lane::Live), pushed(Lane::Bulk));
        assert!(bulk >= 32 << 20 && live < 1 << 20, "seed {seed}: Live pushed {live} bytes, Bulk {bulk}");
        println!("world digest bulk_saturating seed {seed}: {:016x}, md p99 {p99:?}, max {max:?}", w.digest());
    })
    .unwrap();
}

/// Both keys published at once. `Bulk` cut both ways for less than the downstream expiry: the leaf reads `md` on
/// meanwhile and, healed, `bulk` from the origin's ring. Then `Bulk`'s session reset, and a cut past the expiry: the
/// origin gives the leaf up, as a child its pushes cannot reach, and the leaf, asking again on `Control`, reads both
/// keys on from the ring. Every record once and in order.
fn dead_bulk(seed: u64) {
    let mut w = world(seed, Wire::default());
    let (md, _) = collect(&mut w, key(), StartPosition::First);
    let (dump, _) = collect(&mut w, bulk(), StartPosition::First);
    w.run_until("the leaf follows both keys", Duration::from_secs(10), following);
    let stop = Rc::new(Cell::new(false));
    let publishing = publish_both(&mut w, &stop);
    run(&mut w, 1000..2000);

    let cut = Duration::from_millis(w.pick(500..1500));
    w.log(format_args!("cut Bulk both ways {cut:?}"));
    w.cut_lane(&["origin"], &["leaf"], Lane::Bulk);
    w.cut_lane(&["leaf"], &["origin"], Lane::Bulk);
    let before = last(&md);
    w.run_for(cut);
    let (during, stalled) = (last(&md), last(&dump));
    // 80 records a second on each key.
    assert!(during >= before + 16, "seed {seed}: md read {before} to {during} while Bulk was cut");
    assert!(stalled + 16 <= during, "seed {seed}: bulk read through {stalled} while Bulk was cut, md {during}");
    w.heal();
    w.run_until("bulk catches up from the ring", Duration::from_secs(5), |_| last(&dump) >= during);

    w.log("reset Bulk");
    w.reset_lane("origin", "leaf", Lane::Bulk);
    run(&mut w, 1000..2000);

    let cut = Duration::from_millis(w.pick(3000..4000));
    w.log(format_args!("cut Bulk both ways {cut:?}"));
    w.cut_lane(&["origin"], &["leaf"], Lane::Bulk);
    w.cut_lane(&["leaf"], &["origin"], Lane::Bulk);
    w.run_for(cut);
    w.heal();
    run(&mut w, 1000..2000);
    stop.set(true);
    let published = w.join(publishing).unwrap();
    read_through(&mut w, seed, &[(&md, published), (&dump, published)]);
    let events = w.block_on("origin", |n| async move { n.stream.detail(0).events }).unwrap();
    let expired: Vec<_> = events.iter().filter(|e| e.kind == "downstream_expired").map(|e| e.detail.as_str()).collect();
    assert!(expired.contains(&"leaf could not be reached"), "seed {seed}: {expired:?}");
    println!("world digest dead_bulk seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring() {
    check_seeds(seeds(0..16), dead_bulk).unwrap();
}

/// Both keys published at once, the origin feeding the leaf with a push task per lane. The leaf crashes: both tasks
/// end with it. Restarted, it reads each key on after the cursor it had, every record once and in order.
fn child_restart(seed: u64) {
    let mut w = world(seed, Wire::default());
    let (md, _) = collect(&mut w, key(), StartPosition::First);
    let (dump, _) = collect(&mut w, bulk(), StartPosition::First);
    w.run_until("the leaf follows both keys", Duration::from_secs(10), following);
    let stop = Rc::new(Cell::new(false));
    let publishing = publish_both(&mut w, &stop);
    w.run_until("a push task per lane", Duration::from_secs(5), |w| stream(w, "origin").pumps() == 2);
    run(&mut w, 1000..2000);

    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.log(format_args!("leaf {how:?}"));
    w.crash("leaf", how);
    w.run_until("both push tasks end", Duration::from_secs(10), |w| stream(w, "origin").pumps() == 0);
    run(&mut w, 0..500);
    w.restart("leaf");
    w.run_until("the leaf is back", Duration::from_secs(30), |w| w.nodes().count() == 2);
    let after = |read: &Read| StartPosition::After(Cursor { generation: 1, sequence: last(read) });
    let (md, _) = collect(&mut w, key(), after(&md));
    let (dump, _) = collect(&mut w, bulk(), after(&dump));
    w.run_until("a push task per lane again", Duration::from_secs(10), |w| stream(w, "origin").pumps() == 2);
    run(&mut w, 1000..2000);
    stop.set(true);
    let published = w.join(publishing).unwrap();
    read_through(&mut w, seed, &[(&md, published), (&dump, published)]);
    println!("world digest child_restart seed {seed}: {:016x}", w.digest());
}

#[test]
fn a_child_restart_ends_both_lanes_pumps_and_each_namespace_resumes() {
    check_seeds(seeds(0..16), child_restart).unwrap();
}
