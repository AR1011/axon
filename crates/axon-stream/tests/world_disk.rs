//! An origin that keeps its history on disk, on the World: its process or machine crashes under a relay and a leaf,
//! and the stream goes on in the same generation. The World's `Disk` keeps what was synced, and a machine crash keeps
//! a seeded first part of the rest, a torn frame at the end; a write is synced one file after another, `DISK_SYNC`
//! each, after it is handed over.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::*;
use axon_testkit::world::{Crash, DISK_SYNC, Host, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

mod common;
use common::{SECRET, key, labels};

#[derive(Clone)]
struct Node {
    stream: StreamNode,
    cluster: Cluster,
    _system: ActorSystem,
}

/// With the timings of `world_relay.rs`. The origin keeps `md` on its disk and opens the key as generation 1 on every
/// boot, which resumes what the disk holds. A `relaying` origin also pulls the key from the relay, as every node of
/// the fault rig does, and on a boot that resumes it opens the key only once the relay has asked it for the key, so
/// the open takes back a key it relays.
async fn boot(host: Host, discovery: MemoryDiscovery, relaying: bool) -> Node {
    let role = host.name().to_owned();
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(SharedSecret::new(SECRET).unwrap()), discovery);
    common::short_cluster(&mut cfg);
    cfg.labels = labels(&[("role", &role)]);
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let upstream = match &*role {
        "origin" if relaying => vec![labels(&[("role", "relay")])],
        "origin" => vec![],
        "relay" => vec![labels(&[("role", "origin")])],
        _ => vec![labels(&[("role", "relay")])],
    };
    let mut cfg = StreamNodeConfig::new(upstream);
    common::short_stream(&mut cfg);
    if role == "origin" {
        // An hour: nothing here is old enough to go.
        let retention = Retention { namespace: "md".into(), max_bytes: 1 << 20, max_age: Duration::from_secs(3600) };
        let mut disk = DiskConfig::new(Arc::new(host.disk().dir("streams")), vec![retention]);
        // Small, so a run rolls through many segments.
        disk.segment_bytes = 2048;
        cfg.disk = Some(disk);
    }
    let stream = StreamNode::start(&cluster, cfg).await.unwrap();
    if role == "origin" {
        while relaying
            && stream.stored(&key()).is_some()
            && !stream.detail(1).streams.iter().any(|s| s.downstream_branches > 0)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        stream.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    }
    Node { stream, cluster, _system: system }
}

fn world(seed: u64, relaying: bool) -> World<Node> {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in ["origin", "relay", "leaf"] {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone(), relaying));
    }
    w.run_until("every node booted", Duration::from_secs(30), |w| w.nodes().count() == 3);
    w
}

fn stream<'w>(w: &'w World<Node>, name: &str) -> &'w StreamNode {
    &w.nodes().find(|(n, _)| *n == name).unwrap_or_else(|| panic!("{name} is down")).1.stream
}

fn restart_origin(w: &mut World<Node>) {
    w.restart("origin");
    w.run_until("the origin is back", Duration::from_secs(10), |w| w.nodes().any(|(n, _)| n == "origin"));
}

/// What a leaf subscriber read, by sequence, or what was wrong: a gap, a repeat, another generation or an end.
type Read = Rc<RefCell<Result<BTreeMap<u64, Bytes>, String>>>;

fn collect(w: &mut World<Node>, start: StartPosition) -> Read {
    let read = Read::new(RefCell::new(Ok(BTreeMap::new())));
    let r = read.clone();
    let mut last = if let StartPosition::After(cursor) = start { cursor.sequence } else { 0 };
    drop(w.spawn_on("leaf", move |n| async move {
        let mut subscription = n.stream.subscribe(&key(), SubscribeOptions { start, ..Default::default() }).unwrap();
        while let Some(event) = subscription.next().await {
            let wrong = match event {
                StreamEvent::Batch(batch) if batch.generation() == 1 && batch.first() == last + 1 => {
                    let mut read = r.borrow_mut();
                    let read = read.as_mut().expect("stops at the first wrong event");
                    read.extend((batch.first()..).zip(batch.records().iter().cloned()));
                    last = batch.last();
                    continue;
                }
                StreamEvent::Batch(batch) => format!("{:?} from {} after {last}", batch.cursor(), batch.first()),
                event if event.is_terminal() => format!("{event:?} after {last}"),
                _ => continue,
            };
            *r.borrow_mut() = Err(wrong);
            return;
        }
    }));
    read
}

fn last(read: &Read) -> u64 {
    read.borrow().as_ref().map_or(0, |r| r.keys().next_back().copied().unwrap_or(0))
}

/// What the origin admitted, by sequence, and the last durable position it saw after each publish.
#[derive(Clone, Default)]
struct Published {
    records: Rc<RefCell<BTreeMap<u64, Bytes>>>,
    durable: Rc<Cell<u64>>,
}

/// One record a millisecond, each naming its boot, until `stop`. One refused while the store's last write failed is
/// not published, and the next is tried.
fn publisher(w: &mut World<Node>, boot: u32, published: &Published, stop: &Rc<Cell<bool>>) {
    let (published, stop) = (published.clone(), stop.clone());
    drop(w.spawn_on("origin", move |n| async move {
        for i in 0u64.. {
            if stop.get() {
                return;
            }
            let record = Bytes::from(format!("{boot}:{i}"));
            let at = match n.stream.publish(&key(), vec![record.clone()]) {
                Ok(at) => at,
                Err(e) if e.error == StreamError::Disk(DiskError::Failed) => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
                Err(e) => panic!("{:?}", e.error),
            };
            assert_eq!(at.generation, 1);
            published.records.borrow_mut().insert(at.sequence, record);
            published.durable.set(n.stream.cursor(&key()).map_or(0, |c| c.sequence));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }));
}

/// `fault` runs while the origin publishes, then the origin crashes as `crashes` say. After the first, it crashes again
/// right after the leaf read up to where it resumed: what it resumed from its files, and the leaf saw, must survive
/// that crash too. Then the leaf, and a reader after what it had seen, read every record published, once, and the
/// origin has written all of it.
fn resumes(seed: u64, relaying: bool, label: &str, crashes: &[Crash], fault: impl FnOnce(&mut World<Node>)) {
    let mut w = world(seed, relaying);
    let live = collect(&mut w, StartPosition::Live);
    w.run_until("the leaf follows through the relay", Duration::from_secs(30), |w| {
        stream(w, "leaf").parent_of(&key()).as_deref() == Some("relay") && stream(w, "leaf").cursor(&key()).is_some()
    });
    let published = Published::default();
    let stop = Rc::new(Cell::new(false));
    publisher(&mut w, 1, &published, &stop);
    let up = Duration::from_millis(w.pick(100..400));
    w.run_for(up);
    fault(&mut w);

    let mut seen = 0;
    for &how in crashes {
        w.log(format_args!("origin {how:?}"));
        w.crash("origin", how);
        let admitted = published.records.borrow().keys().next_back().copied().unwrap_or(0);
        let durable = published.durable.get();
        seen = last(&live);
        let down = Duration::from_millis(w.pick(0..300));
        w.run_for(down);
        restart_origin(&mut w);
        let resumed = stream(&w, "origin").cursor(&key()).expect("the key is open");
        assert_eq!(resumed.generation, 1, "seed {seed}");
        assert!(
            resumed.sequence >= durable.max(seen),
            "seed {seed}: resumed at {resumed:?} after a {how:?} crash, {durable} was durable and {seen} seen"
        );
        // A write is taken as soon as the last lands and synced `DISK_SYNC` a file later, two files when it rolls a
        // segment: what was admitted while the last synced write and the one after it ran, at most `2 * 2 DISK_SYNC`
        // of it, one record a millisecond, may be gone; a torn tail keeps some of it and is cut at its last whole record.
        let lost = admitted.saturating_sub(resumed.sequence);
        assert!(lost <= 2 * 2 * 2 + 2, "seed {seed}: {lost} of {admitted} records lost to a {how:?} crash");
        w.run_until("the leaf read up to where the origin resumed", Duration::from_secs(10), |_| {
            last(&live) >= resumed.sequence || live.borrow().is_err()
        });
    }
    let from = collect(&mut w, StartPosition::After(Cursor { generation: 1, sequence: seen }));

    publisher(&mut w, 2, &published, &stop);
    let more = Duration::from_millis(w.pick(100..400));
    w.run_for(more);
    stop.set(true);
    w.run_for(Duration::from_millis(20));
    let through = published.records.borrow().keys().next_back().copied().unwrap_or(0);
    w.run_until("the leaf read everything", Duration::from_secs(10), |_| {
        last(&live) >= through || live.borrow().is_err()
    });
    w.run_until("the late reader read everything", Duration::from_secs(10), |_| {
        last(&from) >= through || from.borrow().is_err()
    });
    let published = published.records.borrow();
    for (reader, read, after) in [("live", &live, 0), ("after", &from, seen)] {
        let read = read.borrow().clone().unwrap_or_else(|wrong| panic!("seed {seed}: {reader} read {wrong}"));
        let expected: BTreeMap<u64, Bytes> = published.range(after + 1..).map(|(s, r)| (*s, r.clone())).collect();
        assert!(read == expected, "seed {seed}: {reader} read other records than the origin published");
    }
    assert_eq!(stream(&w, "leaf").stats().local_resets, 0, "seed {seed}");
    let disk = stream(&w, "origin").stats().disk.expect("kept on disk");
    assert_eq!((disk.unwritten_bytes, disk.failing), (0, None), "seed {seed}");
    println!("world digest disk_{label}_{crashes:?} seed {seed}: {:016x}", w.digest());
}

/// Nothing a subscriber saw is lost, and nothing is seen twice: records are shown once they are synced, and the
/// origin resumes after the last synced one.
#[test]
fn a_process_crash_resumes_the_generation_after_the_last_durable_record() {
    check_seeds(seeds(0..64), |seed| resumes(seed, false, "crash", &[Crash::Process], |_| {})).unwrap();
}

/// A record whose wait for its cursor ended is on disk: a machine crash at any moment after loses none of them, and a
/// reader from the first record reads each once.
#[test]
fn a_record_waited_for_survives_a_machine_crash_at_any_moment() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, false);
        let acked = Rc::new(RefCell::new(BTreeMap::new()));
        let a = acked.clone();
        drop(w.spawn_on("origin", move |n| async move {
            for i in 0u64.. {
                let record = Bytes::from(format!("{i}"));
                let Ok(at) = n.stream.publish(&key(), vec![record.clone()]) else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                };
                if n.stream.wait_cursor(&key(), at).await.is_err() {
                    return;
                }
                a.borrow_mut().insert(at.sequence, record);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
        let up = Duration::from_millis(w.pick(20..400));
        w.run_for(up);
        w.crash("origin", Crash::Machine);
        let acked = acked.borrow().clone();
        let last_acked = acked.keys().next_back().copied().unwrap_or(0);
        restart_origin(&mut w);
        let resumed = stream(&w, "origin").cursor(&key()).expect("the key is open");
        assert!(resumed.sequence >= last_acked, "seed {seed}: resumed at {resumed:?}, {last_acked} was acked");
        let first = collect(&mut w, StartPosition::First);
        w.run_until("a reader from the first record read what was acked", Duration::from_secs(10), |_| {
            last(&first) >= last_acked || first.borrow().is_err()
        });
        let read = first.borrow().clone().unwrap_or_else(|wrong| panic!("seed {seed}: read {wrong}"));
        assert!(acked.iter().all(|(s, r)| read.get(s) == Some(r)), "seed {seed}: an acked record was lost");
    })
    .unwrap();
}

/// An origin with nothing to write starts a write as soon as a record is published: the wait until it is durable is
/// that write's own time, `DISK_SYNC` here, and no wait for a tick before it.
#[test]
fn a_record_published_to_an_idle_origin_is_durable_after_one_write() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, false);
        let gaps: Vec<u64> = (0..20).map(|_| w.pick(0..5000)).collect();
        let waits = w.block_on("origin", move |n| async move {
            let mut waits = Vec::new();
            for gap in gaps {
                tokio::time::sleep(Duration::from_micros(gap)).await;
                let started = tokio::time::Instant::now();
                let at = n.stream.publish(&key(), vec![Bytes::from_static(b"x")]).unwrap();
                n.stream.wait_cursor(&key(), at).await.unwrap();
                waits.push(started.elapsed());
            }
            waits
        });
        let slowest = waits.unwrap().into_iter().max().expect("waited");
        assert!(slowest < DISK_SYNC + Duration::from_millis(1), "seed {seed}: a wait took {slowest:?}");
    })
    .unwrap();
}

/// Retention by age goes on with nothing published: the writer, waiting for a publish, still looks once a second while
/// a stream has a segment it could remove.
#[test]
fn an_idle_origin_still_removes_segments_past_their_age() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, false);
        let written = w.block_on("origin", |n| async move {
            let mut at = None;
            for _ in 0..40 {
                at = Some(n.stream.publish(&key(), vec![Bytes::from(vec![b'x'; 100])]).unwrap());
            }
            n.stream.wait_cursor(&key(), at.expect("published")).await.unwrap();
        });
        written.unwrap();
        let disk = w.disk("origin").dir("streams");
        assert!(disk.list().len() > 1, "seed {seed}: {:?}", disk.list());
        // Past the hour `max_age` keeps.
        w.step_wall("origin", 2 * 3_600_000);
        w.run_until("retention removed the old segments", Duration::from_secs(3), |_| disk.list().len() == 1);
        let resumed = stream(&w, "origin").cursor(&key()).expect("the key is open");
        assert_eq!(resumed.sequence, 40, "seed {seed}");
    })
    .unwrap();
}

/// An origin that relays its own key, as one with `upstream` tiers does from its boot until it opens the key again,
/// takes it back with the open: its children ask again from their positions, and the same generation goes on for its
/// followers from where they were. Before, the open ended the children's branches as a close does, and they took the
/// generation again from record 1, or from the live edge past the last durable record.
#[test]
fn an_origin_that_relays_its_key_resumes_it_with_no_repeat_and_no_gap() {
    check_seeds(seeds(0..64), |seed| resumes(seed, true, "relaying", &[Crash::Process], |_| {})).unwrap();
}

/// A machine crash also loses what was written and not synced, or all but a torn part of it: at most the last
/// interval of records.
#[test]
fn a_machine_crash_loses_at_most_the_last_interval_and_no_subscriber_sees_a_gap() {
    check_seeds(seeds(0..64), |seed| resumes(seed, false, "crash", &[Crash::Machine], |_| {})).unwrap();
}

/// A process crash leaves writes the disk never synced; the origin syncs what it reads back before it serves it, so
/// a machine crash next loses none of it.
#[test]
fn a_machine_crash_after_a_process_crash_loses_nothing_a_subscriber_saw() {
    check_seeds(seeds(0..64), |seed| resumes(seed, false, "crash", &[Crash::Process, Crash::Machine], |_| {})).unwrap();
}

/// The origin paused, mid-write or between writes, and the relay too in half the seeds: the writes wait and go on,
/// and the origin crashing after still resumes after its last durable record.
#[test]
fn a_paused_origin_loses_nothing_and_resumes_after_the_last_durable_record() {
    check_seeds(seeds(0..64), |seed| {
        resumes(seed, false, "pause", &[Crash::Process], |w| {
            let paused = Duration::from_millis(w.pick(50..3000));
            w.pause("origin", paused);
            if w.pick(0..2) == 0 {
                let relay = Duration::from_millis(w.pick(50..3000));
                w.pause("relay", relay);
            }
            let after = Duration::from_millis(w.pick(0..200));
            w.run_for(paused + after);
        })
    })
    .unwrap();
}

/// Segment names and retention by age read the wall clock. The origin's steps past `max_age` forward, so retention
/// removes every segment but the one being written, and the origin crashes at most 50 ms later; or back, so the
/// segments written meanwhile are named older than the ones before, and it is set right before the crash, as a restart
/// an hour back would give an older incarnation its peers refuse. Its monotonic clock drifts throughout. It resumes
/// after its last durable record whatever the names' times, and nothing a subscriber saw is lost.
#[test]
fn a_stepped_and_drifting_clock_at_the_origin_loses_nothing() {
    check_seeds(seeds(0..64), |seed| {
        resumes(seed, false, "clock", &[Crash::Process], |w| {
            let ppm = w.pick(0..400_001) as i32 - 200_000;
            w.drift("origin", ppm);
            let (by, back) = ((w.pick(1..3) * 3_600_000 + 60_000) as i64, w.pick(0..2) == 0);
            w.step_wall("origin", if back { -by } else { by });
            let run = Duration::from_millis(if back { w.pick(1100..2000) } else { w.pick(0..50) });
            w.run_for(run);
            if back {
                w.step_wall("origin", by);
            }
        })
    })
    .unwrap();
}

/// A starved origin publishes and writes late and in bursts, and crashes while starved: it resumes after its last
/// durable record all the same, and loses at most the last interval.
#[test]
fn a_starved_origin_writes_late_and_loses_nothing_a_subscriber_saw() {
    check_seeds(seeds(0..64), |seed| {
        resumes(seed, false, "starve", &[Crash::Process], |w| {
            let one_in = w.pick(2..20) as u32;
            w.starve("origin", one_in);
            let run = Duration::from_millis(w.pick(200..2000));
            w.run_for(run);
        })
    })
    .unwrap();
}

/// A store whose writes panic after their appends and before their sync: each is a failed write, taken again, and
/// publishes are refused `Disk(Failed)` meanwhile. The writer goes on, nothing a subscriber saw is lost or seen twice,
/// and the frames written twice are read once after a crash.
#[test]
fn a_store_that_panics_mid_write_fails_the_write_and_the_writer_goes_on() {
    check_seeds(seeds(0..64), |seed| {
        resumes(seed, false, "panic", &[Crash::Process], |w| {
            let panics = w.pick(1..4) as u32;
            let disk = w.disk("origin").dir("streams");
            disk.panic_writes(panics);
            w.run_until("every write that was to panic did", Duration::from_secs(10), |_| disk.panics_left() == 0);
            w.run_until("a write went through", Duration::from_secs(10), |w| {
                stream(w, "origin").stats().disk.expect("kept on disk").failing.is_none()
            });
            let failures = stream(w, "origin").stats().disk.expect("kept on disk").write_failures;
            assert_eq!(failures, u64::from(panics), "seed {seed}");
            let run = Duration::from_millis(w.pick(0..100));
            w.run_for(run);
        })
    })
    .unwrap();
}

/// A boot cuts a torn tail off a segment already full, so the next record begins a segment. The cut is synced before
/// that record is shown: a machine crash next does not bring the torn tail back, which the boot after it would cut
/// again, removing the newer segment with the record a subscriber saw.
#[test]
fn a_tail_a_boot_cut_stays_cut_through_a_machine_crash() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, false);
        let publish = |w: &mut World<Node>, n: u64| {
            let published = w.block_on("origin", move |node| async move {
                let mut at = None;
                for _ in 0..n {
                    at = Some(node.stream.publish(&key(), vec![Bytes::from(vec![b'x'; 100])]).unwrap());
                }
                let at = at.expect("published");
                node.stream.wait_cursor(&key(), at).await.unwrap();
                at.sequence
            });
            published.unwrap()
        };
        // 17 frames of 125 bytes: the first segment holds 2048 and more, and the next record begins another.
        assert_eq!(publish(&mut w, 17), 17, "seed {seed}");
        w.crash("origin", Crash::Process);
        let disk = w.disk("origin").dir("streams");
        let [(segment, _)] = &disk.list()[..] else { panic!("seed {seed}: {:?}", disk.list()) };
        // A frame the crash tore: its length runs past the end of the file.
        disk.append(segment, &[0xff; 30]).unwrap();
        restart_origin(&mut w);
        assert_eq!(stream(&w, "origin").stats().disk.expect("kept on disk").truncated, 1, "seed {seed}");
        assert_eq!(publish(&mut w, 1), 18, "seed {seed}");
        assert_eq!(disk.list().len(), 2, "seed {seed}");
        w.crash("origin", Crash::Machine);
        restart_origin(&mut w);
        let resumed = stream(&w, "origin").cursor(&key()).expect("the key is open");
        assert_eq!(resumed.sequence, 18, "seed {seed}: a record a subscriber saw is gone");
        println!("world digest disk_cut seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// `restart_stream` and `close_stream` on a stream kept on disk: the new generation is written and durable, and each
/// removes the files of what went before.
#[test]
fn a_restart_and_a_close_remove_the_old_files() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, false);
        let disk = w.disk("origin").dir("streams");
        let generations = move || {
            let names = disk.list().into_iter().map(|(name, _)| name);
            names
                .map(|name| name.split('.').nth(2).unwrap().parse().unwrap())
                .collect::<std::collections::BTreeSet<u64>>()
        };
        let done = w.block_on("origin", |n| async move {
            n.stream
                .wait_cursor(&key(), n.stream.publish(&key(), vec![Bytes::from_static(b"one")]).unwrap())
                .await
                .unwrap();
            n.stream.restart_stream(&key(), 2).unwrap();
            let at = n.stream.publish(&key(), vec![Bytes::from_static(b"two")]).unwrap();
            assert_eq!(at, Cursor { generation: 2, sequence: 1 });
            n.stream.wait_cursor(&key(), at).await.unwrap();
        });
        done.unwrap();
        assert_eq!(generations(), [2].into(), "seed {seed}");
        w.block_on("origin", |n| async move { n.stream.close_stream(&key()).unwrap() }).unwrap();
        w.run_for(Duration::from_millis(20));
        assert!(generations().is_empty(), "seed {seed}");
    })
    .unwrap();
}

/// A compacted stream resumes its state: a subscriber that starts from a snapshot after the restart gets every key.
#[test]
fn a_compacted_stream_resumes_its_state_after_a_crash() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, false);
        let book = StreamKey::new("md", "book");
        let policy = DeliveryPolicy::Compacted(CompactLimits { max_keys: 8, max_bytes: 1 << 10 });
        let set = |k: &str, v: &str| vec![keyed(k.as_bytes(), v.as_bytes())];
        let (b, s) = (book.clone(), set);
        let written = w.block_on("origin", move |n| async move {
            n.stream.open_stream(&b, 1, policy).await.unwrap();
            for records in [s("a", "1"), s("b", "1"), s("a", "2")] {
                n.stream.publish(&b, records).unwrap();
            }
            n.stream.wait_cursor(&b, Cursor { generation: 1, sequence: 3 }).await.unwrap();
        });
        written.unwrap();
        w.crash("origin", Crash::Process);
        restart_origin(&mut w);
        let snapshot = w.block_on("origin", move |n| async move {
            n.stream.open_stream(&book, 1, policy).await.unwrap();
            let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
            let mut subscription = n.stream.subscribe(&book, options).unwrap();
            loop {
                match subscription.next().await.expect("open") {
                    StreamEvent::Snapshot(snapshot) => return (snapshot.through(), snapshot.records().to_vec()),
                    event => assert!(!event.is_terminal(), "{event:?}"),
                }
            }
        });
        let (through, mut records) = snapshot.unwrap();
        records.sort();
        assert_eq!(through, Cursor { generation: 1, sequence: 3 }, "seed {seed}");
        assert_eq!(records, [set("a", "2"), set("b", "1")].concat(), "seed {seed}");
    })
    .unwrap();
}

/// A `SnapshotThenLive` or `First` subscriber holds a position in one generation. Its origin crashes and resumes that
/// generation from its disk: the subscriber goes on with the next record, nothing in between. Its origin crashes and
/// opens a newer generation: the subscriber ends with `ResetRequired(GenerationChanged)`, where a `Live` one would go
/// on with a new `Ready`, since what it holds is of a sequence that is gone.
#[test]
fn a_positioned_subscriber_goes_on_through_a_resumed_generation_and_ends_at_a_new_one() {
    check_seeds(seeds(0..16), |seed| {
        let mut w = world(seed, false);
        let book = StreamKey::new("md", "book");
        let policy = DeliveryPolicy::Compacted(CompactLimits { max_keys: 8, max_bytes: 1 << 10 });
        let open = |w: &mut World<Node>, generation: u64, value: &'static [u8]| {
            let b = book.clone();
            let at = w.block_on("origin", move |n| async move {
                n.stream.open_stream(&b, generation, policy).await.unwrap();
                let at = n.stream.publish(&b, vec![keyed(b"a", value)]).unwrap();
                n.stream.wait_cursor(&b, at).await.unwrap();
                at
            });
            at.unwrap()
        };
        let restart = |w: &mut World<Node>| {
            w.crash("origin", Crash::Process);
            let down = Duration::from_millis(w.pick(0..300));
            w.run_for(down);
            restart_origin(w);
        };
        let one = open(&mut w, 1, b"1");

        let readers = [StartPosition::SnapshotThenLive, StartPosition::First].map(|start| {
            let reader = Reader { start, events: Rc::default(), ended: Rc::default() };
            let (events, ended, b) = (reader.events.clone(), reader.ended.clone(), book.clone());
            drop(w.spawn_on("leaf", move |n| async move {
                let mut subscription =
                    n.stream.subscribe(&b, SubscribeOptions { start, ..Default::default() }).unwrap();
                while let Some(event) = subscription.next().await {
                    events.borrow_mut().push(event);
                }
                ended.set(true);
            }));
            reader
        });
        let holds_one = |event: &StreamEvent| match event {
            StreamEvent::Snapshot(snapshot) => snapshot.through() == one,
            StreamEvent::Batch(batch) => batch.cursor() == one,
            _ => false,
        };
        w.run_until("the leaf's readers hold the first record", Duration::from_secs(10), |_| {
            readers.iter().all(|r| r.events.borrow().iter().any(holds_one))
        });

        restart(&mut w);
        let marks = readers.each_ref().map(|r| r.events.borrow().len());
        let two = open(&mut w, 1, b"2");
        assert_eq!(two, Cursor { generation: 1, sequence: 2 }, "seed {seed}");
        w.run_until("the leaf's readers read the resumed generation", Duration::from_secs(10), |_| {
            readers.iter().zip(marks).all(|(r, mark)| r.ended.get() || !r.since(mark).is_empty())
        });
        for (r, mark) in readers.iter().zip(marks) {
            assert_eq!(r.since(mark), ["batch 1:2"], "seed {seed}: {:?}", r.start);
        }

        restart(&mut w);
        let marks = readers.each_ref().map(|r| r.events.borrow().len());
        open(&mut w, 2, b"3");
        w.run_until("the leaf's subscriptions end", Duration::from_secs(10), |_| readers.iter().all(|r| r.ended.get()));
        for (r, mark) in readers.iter().zip(marks) {
            assert_eq!(r.since(mark), ["ResetRequired(GenerationChanged)"], "seed {seed}: {:?}", r.start);
        }
        println!("world digest positioned_generations seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// What one subscription on the leaf received, and whether it ended.
struct Reader {
    start: StartPosition,
    events: Rc<RefCell<Vec<StreamEvent>>>,
    ended: Rc<Cell<bool>>,
}

impl Reader {
    fn since(&self, mark: usize) -> Vec<String> {
        let shown = |event: &StreamEvent| match event {
            StreamEvent::Batch(batch) => format!("batch {}:{}", batch.generation(), batch.last()),
            event => format!("{event:?}"),
        };
        self.events.borrow()[mark..].iter().map(shown).collect()
    }
}

/// A full disk refuses publishes with `Disk(Full)`, and the records admitted before are held and written once there
/// is room. Subscribers go on reading what the stream has, and publishing resumes on its own.
#[test]
fn a_full_disk_refuses_publishes_and_the_stream_keeps_serving() {
    check_seeds(seeds(0..64), |seed| {
        let mut w = world(seed, false);
        let live = collect(&mut w, StartPosition::Live);
        w.run_until("the leaf follows", Duration::from_secs(30), |w| stream(w, "leaf").cursor(&key()).is_some());
        let disk = w.disk("origin").dir("streams");
        let room = w.pick(0..2000);
        disk.set_capacity(Some(disk.list().iter().map(|(_, len)| len).sum::<u64>() + room));
        let refused = w.block_on("origin", |n| async move {
            let mut admitted = BTreeMap::new();
            for i in 0u64.. {
                let record = Bytes::from(format!("{i:08}"));
                match n.stream.publish(&key(), vec![record.clone()]) {
                    Ok(at) => admitted.insert(at.sequence, record),
                    Err(e) => return (admitted, e.error, n.stream.stats().disk.expect("kept on disk")),
                };
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            unreachable!()
        });
        let (mut admitted, error, stats) = refused.unwrap();
        assert_eq!(error, StreamError::Disk(DiskError::Full), "seed {seed}");
        assert_eq!((stats.failing, stats.refused), (Some(DiskError::Full), 1), "seed {seed}");
        let durable = stream(&w, "origin").cursor(&key()).expect("open").sequence;
        assert!(durable < admitted.len() as u64, "seed {seed}: every record was durable before the refusal");
        w.run_until("the leaf read what is durable", Duration::from_secs(10), |_| last(&live) == durable);
        let first = collect(&mut w, StartPosition::First);
        w.run_until("a new reader reads what is durable", Duration::from_secs(10), |_| last(&first) == durable);

        disk.set_capacity(None);
        let more = w.block_on("origin", |n| async move {
            loop {
                match n.stream.publish(&key(), vec![Bytes::from_static(b"after")]) {
                    Ok(at) => return at.sequence,
                    Err(e) => assert_eq!(e.error, StreamError::Disk(DiskError::Full)),
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        let more = more.unwrap();
        admitted.insert(more, Bytes::from_static(b"after"));
        assert_eq!(more, admitted.len() as u64, "seed {seed}: a refused publish took a position");
        w.run_until("the leaf read on", Duration::from_secs(10), |_| last(&live) == more || live.borrow().is_err());
        assert!(*live.borrow() == Ok(admitted), "seed {seed}: {:?}", live.borrow().as_ref().err());
        assert_eq!(stream(&w, "origin").stats().disk.expect("kept on disk").failing, None, "seed {seed}");
        // The run of failures is two events on the origin's timeline, not one per failed write.
        let origin = &w.nodes().find(|(n, _)| *n == "origin").expect("up").1.cluster;
        let told: Vec<_> = origin.events().since(0, usize::MAX).into_iter().map(|e| e.kind).collect();
        let disk: Vec<_> = told.into_iter().filter(|k| k.starts_with("disk_")).collect();
        assert_eq!(disk, ["disk_failing", "disk_recovered"], "seed {seed}");
        println!("world digest disk_full seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}
