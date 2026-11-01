//! The World's faults, each on the smallest node that shows it: raw pipes, a listener, or one actor system.
use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use axon::{
    Actor, ActorSystem, Backoff, Context, LifecycleHandle, RejectReason, ReplyTo, RequestError, RestartPolicy, Router,
    Routing, Sink, SinkEvent, SinkHandle, StopReason, TurnBudget,
};
use axon_cluster::{Discovery, DiscoveryError, NodeRecord, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use axon_net::{AcceptLimits, Lane, Listener, ListenerStatsReader, Network, SharedSecret};
use axon_testkit::world::{Crash, Host, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use common::{SECRET, at, wires};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, timeout};

mod common;

const MS: Duration = Duration::from_millis(1);

fn count() -> Arc<AtomicU64> {
    Arc::default()
}

fn get(n: &AtomicU64) -> u64 {
    n.load(Ordering::Relaxed)
}

/// On `b`: accepts every connection and reads each to its end, adding what it reads to `read`.
fn reader(w: &mut World<Arc<dyn Network>>, read: &Arc<AtomicU64>, accepted: &Arc<AtomicU64>) {
    let (read, accepted) = (read.clone(), accepted.clone());
    drop(w.spawn_on("b", move |net| async move {
        let (_, mut incoming) = net.bind(at(2)).unwrap();
        while let Some(Ok((mut pipe, _))) = incoming.recv().await {
            accepted.fetch_add(1, Ordering::Relaxed);
            let read = read.clone();
            tokio::task::spawn_local(async move {
                let mut buf = [0; 4096];
                while let Ok(n @ 1..) = pipe.read(&mut buf).await {
                    read.fetch_add(n as u64, Ordering::Relaxed);
                }
            });
        }
    }));
}

#[test]
fn a_partition_holds_bytes_and_dials_both_ways_and_they_flow_on_the_heal() {
    check_seeds(seeds(0..8), |seed| {
        let mut w = wires(seed, Wire::default(), &["a", "b"]);
        let (read, accepted) = (count(), count());
        reader(&mut w, &read, &accepted);
        w.wire("a", "b", Wire { latency: Duration::from_millis(20), jitter: Duration::ZERO, ..Wire::default() });
        let mut pipe = w.block_on("a", |net| async move { net.connect(at(2), Lane::Control).await.unwrap() }).unwrap();
        let sent = w.now();
        pipe = w.block_on("a", |_| async move { pipe.write_all(b"x").await.map(|()| pipe).unwrap() }).unwrap();
        w.run_until("the byte is read", Duration::from_secs(1), |_| get(&read) == 1);
        let took = w.now() - sent;
        assert!((Duration::from_millis(20)..Duration::from_millis(23)).contains(&took), "a>b took {took:?}");
        let back = w
            .block_on("b", |net| async move {
                let sent = Instant::now();
                // Nothing listens there: the SYN crosses b>a, and the refusal comes back over a>b.
                net.connect(SocketAddr::from(([10, 0, 0, 1], 7002)), Lane::Control).await.err().map(|_| sent.elapsed())
            })
            .unwrap();
        let round_trip = Duration::from_millis(20)..Duration::from_millis(25);
        assert!(back.is_some_and(|t| round_trip.contains(&t)), "b>a 1 ms, a>b 20 ms: the refusal took {back:?}");

        w.partition(&["a"], &["b"]);
        pipe = w.block_on("a", |_| async move { pipe.write_all(&[7; 100]).await.map(|()| pipe).unwrap() }).unwrap();
        let dialled = count();
        let (d, t) = (dialled.clone(), dialled.clone());
        drop(w.spawn_on("a", move |net| async move {
            d.fetch_add(u64::from(net.connect(at(2), Lane::Control).await.is_ok()), Ordering::Relaxed)
        }));
        drop(w.spawn_on("b", move |net| async move {
            // Nothing listens there: the refusal comes back only once the SYN crosses.
            let back = net.connect(SocketAddr::from(([10, 0, 0, 1], 7002)), Lane::Control).await;
            t.fetch_add(u64::from(back.is_err()) * 10, Ordering::Relaxed);
        }));
        w.run_for(Duration::from_secs(1));
        assert_eq!((get(&read), get(&accepted), get(&dialled)), (1, 1, 0), "seed {seed}: nothing crosses a partition");

        w.heal();
        w.run_until("the held bytes and the dial", Duration::from_millis(100), |_| {
            get(&read) == 101 && get(&accepted) == 2 && get(&dialled) == 11
        });

        // A cut drops what it holds and the sender's kernel sends it again, which a crashed machine never does.
        w.cut(&["a"], &["b"]);
        pipe = w.block_on("a", |_| async move { pipe.write_all(&[7; 13]).await.map(|()| pipe).unwrap() }).unwrap();
        w.run_for(Duration::from_millis(30));
        w.crash("a", Crash::Machine);
        w.restart("a");
        w.run_for(Duration::from_millis(10));
        w.heal();
        w.run_for(Duration::from_millis(100));
        assert_eq!(get(&read), 101, "seed {seed}: a dead machine's held bytes reached b");
        drop(pipe);
    })
    .unwrap();
}

#[test]
fn a_one_way_cut_holds_the_acknowledgements_too_so_the_reverse_writer_stalls_at_its_window() {
    check_seeds(seeds(0..8), |seed| {
        const TOTAL: usize = 1 << 20;
        let window = WorldConfig::default().window as u64;
        let mut w = wires(seed, Wire::default(), &["a", "b"]);
        let (b_read, accepted, a_read, b_wrote) = (count(), count(), count(), count());
        reader(&mut w, &b_read, &accepted);
        let mut pipe = w.block_on("a", |net| async move { net.connect(at(2), Lane::Control).await.unwrap() }).unwrap();
        // b's side of the connection writes to a: b dials a, a reads.
        let r = a_read.clone();
        drop(w.spawn_on("a", move |net| async move {
            let (_, mut incoming) = net.bind(at(1)).unwrap();
            let (mut pipe, _) = incoming.recv().await.unwrap().unwrap();
            let mut buf = [0; 4096];
            while let Ok(n @ 1..) = pipe.read(&mut buf).await {
                r.fetch_add(n as u64, Ordering::Relaxed);
            }
        }));
        let wrote = b_wrote.clone();
        drop(w.spawn_on("b", move |net| async move {
            let mut pipe = net.connect(at(1), Lane::Control).await.unwrap();
            let bytes = vec![1; TOTAL];
            let mut at = 0;
            while at < TOTAL {
                at += pipe.write(&bytes[at..]).await.unwrap();
                wrote.store(at as u64, Ordering::Relaxed);
            }
        }));
        w.run_until("b writing", Duration::from_secs(1), |_| get(&b_wrote) > 0);

        w.cut(&["a"], &["b"]);
        pipe = w.block_on("a", |_| async move { pipe.write_all(&[7; 10]).await.map(|()| pipe).unwrap() }).unwrap();
        let redialled = count();
        let r = redialled.clone();
        drop(w.spawn_on("b", move |net| async move {
            // Its SYN crosses, but the answer could not come back.
            drop(net.connect(at(1), Lane::Control).await);
            r.store(1, Ordering::Relaxed);
        }));
        let (wrote, read) = (get(&b_wrote), get(&a_read));
        w.run_for(Duration::from_secs(1));
        assert_eq!(get(&b_read), 0, "seed {seed}: a's bytes are held");
        assert_eq!(get(&redialled), 0, "seed {seed}: a dial against the cut completed");
        assert_eq!(get(&a_read), get(&b_wrote), "seed {seed}: a reads all b sends");
        assert!(
            get(&b_wrote) <= read + window,
            "seed {seed}: b wrote past its window: {} after {wrote}",
            get(&b_wrote)
        );
        assert!(get(&b_wrote) < TOTAL as u64, "seed {seed}: b's writer never stalled");
        let stalled = get(&b_wrote);
        w.run_for(Duration::from_millis(100));
        assert_eq!(get(&b_wrote), stalled, "seed {seed}: b's writer moved without acknowledgements");

        w.heal();
        w.run_until("everything both ways", Duration::from_secs(5), |_| {
            get(&b_read) == 10 && get(&a_read) == TOTAL as u64 && get(&redialled) == 1
        });
        pipe = w.block_on("a", |_| async move { pipe.write_all(b"y").await.map(|()| pipe).unwrap() }).unwrap();
        w.run_until("a's bytes flow again", Duration::from_millis(100), |_| get(&b_read) == 11);
        drop(pipe);
    })
    .unwrap();
}

#[test]
fn a_reset_ends_every_connection_between_two_nodes_at_both_ends_and_no_other() {
    check_seeds(seeds(0..8), |seed| {
        let mut w = wires(seed, Wire::default(), &["a", "b", "c"]);
        let got = Arc::new(Mutex::new(Vec::new()));
        for (name, n) in [("b", 2), ("c", 3)] {
            let got = got.clone();
            drop(w.spawn_on(name, move |net| async move {
                let (_, mut incoming) = net.bind(at(n)).unwrap();
                let (mut pipe, _) = incoming.recv().await.unwrap().unwrap();
                let read = pipe.read(&mut [0; 1]).await.map_err(|e| e.kind());
                got.lock().unwrap().push((name, read));
            }));
        }
        for n in [2, 3] {
            let got = got.clone();
            drop(w.spawn_on("a", move |net| async move {
                let mut pipe = net.connect(at(n), Lane::Control).await.unwrap();
                let read = pipe.read(&mut [0; 1]).await.map_err(|e| e.kind());
                got.lock().unwrap().push(("a", read));
            }));
        }
        w.run_for(Duration::from_millis(20));
        // a dialled b; either order names the pair.
        w.reset("b", "a");
        w.run_for(MS);
        let mut got = got.lock().unwrap().clone();
        got.sort();
        let reset = Err(ErrorKind::ConnectionReset);
        assert_eq!(got, [("a", reset), ("b", reset)], "seed {seed}: at once, at both ends, and a to c is untouched");
    })
    .unwrap();
}

#[test]
fn a_lane_cut_or_reset_holds_or_ends_that_lanes_connections_and_no_other() {
    check_seeds(seeds(0..8), |seed| {
        let mut w = wires(seed, Wire::default(), &["a", "b"]);
        let (read, accepted, a_read) = (count(), count(), count());
        reader(&mut w, &read, &accepted);
        let r = a_read.clone();
        drop(w.spawn_on("a", move |net| async move {
            let (_, mut incoming) = net.bind(at(1)).unwrap();
            while let Some(Ok((mut pipe, _))) = incoming.recv().await {
                let r = r.clone();
                tokio::task::spawn_local(async move {
                    while let Ok(n @ 1..) = pipe.read(&mut [0; 64]).await {
                        r.fetch_add(n as u64, Ordering::Relaxed);
                    }
                });
            }
        }));
        let (control, live, bulk) = w
            .block_on("a", |net| async move {
                let dial = |lane| net.connect(at(2), lane);
                (dial(Lane::Control).await.unwrap(), dial(Lane::Live).await.unwrap(), dial(Lane::Bulk).await.unwrap())
            })
            .unwrap();
        let back = w.block_on("b", |net| async move { net.connect(at(1), Lane::Live).await.unwrap() }).unwrap();
        w.cut_lane(&["a"], &["b"], Lane::Live);
        w.cut_lane(&["b"], &["a"], Lane::Live);
        let write = |w: &mut World<Arc<dyn Network>>, name, mut pipe: Box<dyn axon_net::Io>| {
            w.block_on(name, |_| async move { pipe.write_all(b"x").await.map(|()| pipe).unwrap() }).unwrap()
        };
        let (control, bulk) = (write(&mut w, "a", control), write(&mut w, "a", bulk));
        w.run_until("the Control and Bulk bytes are read", Duration::from_secs(1), |_| get(&read) == 2);
        let (mut live, _back) = (write(&mut w, "a", live), write(&mut w, "b", back));
        let dialled = count();
        for (name, to, lane, n) in [
            ("a", 2, Lane::Live, 1),
            ("b", 1, Lane::Live, 10),
            ("a", 2, Lane::Bulk, 100),
            ("a", 2, Lane::Control, 1000),
        ] {
            let d = dialled.clone();
            drop(w.spawn_on(name, move |net| async move {
                let _pipe = net.connect(at(to), lane).await.unwrap();
                d.fetch_add(n, Ordering::Relaxed);
                std::future::pending::<()>().await;
            }));
        }
        w.run_for(Duration::from_secs(1));
        assert_eq!(
            (get(&read), get(&a_read)),
            (2, 0),
            "seed {seed}: a Live byte crossed the cut, one way or the other"
        );
        assert_eq!(get(&dialled), 1100, "seed {seed}: Live dials either way wait, Bulk and Control ones complete");

        w.heal();
        w.run_until("the Live bytes and dials flow on the heal", Duration::from_secs(1), |_| {
            get(&read) == 3 && get(&a_read) == 1 && get(&dialled) == 1111
        });

        w.reset_lane("b", "a", Lane::Live);
        let ended = w
            .block_on("a", |_| async move {
                timeout(MS, live.read(&mut [0; 1])).await.map(|r| r.map_err(|e| e.kind()).err())
            })
            .unwrap();
        assert_eq!(
            ended,
            Ok(Some(ErrorKind::ConnectionReset)),
            "seed {seed}: the Live connection was not reset at once"
        );
        drop((write(&mut w, "a", control), write(&mut w, "a", bulk)));
        w.run_until("Control and Bulk still deliver after the Live reset", Duration::from_secs(1), |_| get(&read) == 5);
    })
    .unwrap();
}

#[test]
fn a_pause_fires_every_overdue_timer_in_one_burst_on_a_clock_that_ran_through_it() {
    let mut w = wires(0, Wire::default(), &["a", "b"]);
    let fired = Arc::new(Mutex::new(Vec::new()));
    let f = fired.clone();
    let start = w
        .block_on("b", move |_| async move {
            let start = Instant::now();
            for k in 1..=9 {
                let f = f.clone();
                tokio::task::spawn_local(async move {
                    tokio::time::sleep_until(start + Duration::from_millis(100) * k).await;
                    f.lock().unwrap().push(start.elapsed());
                });
            }
            start
        })
        .unwrap();
    let since = w.now();
    w.run_for(Duration::from_millis(50));
    w.pause("b", Duration::from_secs(1));
    w.run_for(Duration::from_millis(999));
    assert!(fired.lock().unwrap().is_empty(), "a paused node ran its timers: {:?}", fired.lock().unwrap());
    w.run_for(Duration::from_millis(3));
    let fired = fired.lock().unwrap().clone();
    assert_eq!(fired.len(), 9, "{fired:?}");
    assert!(
        fired.iter().all(|t| *t == fired[0]) && fired[0] >= Duration::from_millis(1049),
        "not one burst: {fired:?}"
    );
    let local = w.block_on("b", move |_| async move { start.elapsed() }).unwrap();
    assert_eq!(local, w.now() - since, "the node's clock ran through the pause");
}

#[test]
fn a_starved_node_runs_its_timers_late_in_bursts_while_its_clock_runs_on() {
    let mut w = wires(0, Wire::default(), &["a", "b"]);
    let late = Arc::new(Mutex::new(Vec::new()));
    let l = late.clone();
    let start = w
        .block_on("b", move |_| async move {
            tokio::task::spawn_local(async move {
                loop {
                    let asleep = Instant::now();
                    tokio::time::sleep(MS).await;
                    l.lock().unwrap().push(asleep.elapsed());
                }
            });
            Instant::now()
        })
        .unwrap();
    let since = w.now();
    w.run_for(Duration::from_millis(100));
    assert!(late.lock().unwrap().iter().all(|t| *t == MS), "fed, every 1 ms sleep takes 1 ms");
    late.lock().unwrap().clear();
    w.starve("b", 4);
    w.run_for(Duration::from_millis(400));
    let late = late.lock().unwrap().clone();
    assert!(
        late.len() <= 210 && late.iter().max() >= Some(&Duration::from_millis(3)),
        "{} sleeps: {late:?}",
        late.len()
    );
    let local = w.block_on("b", move |_| async move { start.elapsed() }).unwrap();
    assert!(local.abs_diff(w.now() - since) <= Duration::from_millis(4), "the clock ran on: {local:?}");
}

#[test]
fn a_clock_at_plus_5000_ppm_ticks_1005_times_while_the_world_ticks_1000() {
    let mut w = wires(0, Wire::default(), &["a", "b", "c"]);
    let ticks: Vec<_> = ["a", "b", "c"].map(|_| count()).into();
    for (name, ticks) in ["a", "b", "c"].into_iter().zip(&ticks) {
        let ticks = ticks.clone();
        drop(w.spawn_on(name, move |_| async move {
            let mut every = tokio::time::interval(MS);
            loop {
                every.tick().await;
                ticks.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    w.run_for(Duration::from_millis(10));
    w.drift("b", 5000);
    w.drift("c", -5000);
    let before: Vec<_> = ticks.iter().map(|t| get(t)).collect();
    w.run_for(Duration::from_secs(1));
    let counted: Vec<_> = ticks.iter().zip(before).map(|(t, b)| get(t) - b).collect();
    assert_eq!(counted, [1000, 1005, 995]);
}

/// Answers a Get; any other message is work.
enum Job {
    Work,
    Get(ReplyTo<u64>),
}

struct Worker(u64);

impl Actor for Worker {
    type Message = Job;
    type Error = ();

    fn on_message(&mut self, job: Job, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match job {
            Job::Work => self.0 += 1,
            Job::Get(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

/// Counts a tick every 10 ms of its node's clock.
struct Ticker(Arc<AtomicU64>);

impl Actor for Ticker {
    type Message = ();
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.schedule_every(Duration::from_millis(10), || ()).map(drop).map_err(drop)
    }

    fn on_message(&mut self, (): (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn systems(budget: TurnBudget) -> World<ActorSystem> {
    let mut w = World::new(WorldConfig::default());
    w.add("a", move |host| async move {
        let runtime = axon::RuntimeConfig { turn_budget: budget, ..host.runtime() };
        ActorSystem::builder().runtime(runtime).build().unwrap()
    });
    w
}

#[test]
fn a_turn_cost_trips_the_turn_budget_and_lapses_a_queued_calls_deadline() {
    let budget = TurnBudget { max_elapsed: Some(MS), ..TurnBudget::messages(64) };
    let mut w = systems(budget);
    // 100 turns of 10 us take 1 ms, not 100: the sub-millisecond rest carries from turn to turn.
    w.cost("a", Duration::from_micros(10));
    let took = w
        .block_on("a", |system| async move {
            let worker = system.spawn(Worker(0)).start().unwrap();
            worker.ready().await.unwrap();
            let clock = system.clock().clone();
            let began = clock.now();
            for _ in 0..100 {
                worker.tell(Job::Work).unwrap();
                tokio::task::yield_now().await;
            }
            assert_eq!(worker.address.request(None, Job::Get).await.unwrap(), 100);
            clock.now() - began
        })
        .unwrap();
    assert!((MS..Duration::from_millis(2)).contains(&took), "100 turns of 10 us took {took:?}");

    w.cost("a", Duration::from_millis(3));
    let (asked, answered, overruns) = w
        .block_on("a", |system| async move {
            let worker = system.spawn(Worker(0)).start().unwrap();
            worker.ready().await.unwrap();
            for _ in 0..3 {
                worker.tell(Job::Work).unwrap();
            }
            let deadline = system.clock().now() + Duration::from_millis(5);
            let asked = worker.address.request(Some(deadline), Job::Get).await;
            let answered = worker.address.request(None, Job::Get).await;
            (asked, answered, worker.address.diagnostics().budget_overruns)
        })
        .unwrap();
    assert_eq!(asked, Err(RequestError::Deadline { admitted: true }), "queued behind three 3 ms turns");
    assert_eq!(answered, Ok(3));
    assert!(overruns >= 3, "a 3 ms turn overruns a 1 ms budget: {overruns}");

    // No max_elapsed and no timing sampled: a turn reads no clock, and is charged when it ends.
    let mut w = systems(TurnBudget::messages(1));
    w.cost("a", Duration::from_millis(3));
    let took = w
        .block_on("a", |system| async move {
            let worker = system.spawn(Worker(0)).start().unwrap();
            worker.ready().await.unwrap();
            let began = system.clock().now();
            for _ in 0..3 {
                worker.tell(Job::Work).unwrap();
            }
            assert_eq!(worker.address.request(None, Job::Get).await.unwrap(), 3);
            system.clock().now() - began
        })
        .unwrap();
    assert!(took >= Duration::from_millis(12), "four turns of 3 ms took {took:?}");

    // While a turn keeps the actors busy, a caller's deadline fires on time, as the clock thread fires it.
    w.cost("a", Duration::ZERO);
    let worker = w
        .block_on("a", |system| async move {
            let worker = system.spawn(Worker(0)).start().unwrap();
            worker.ready().await.unwrap();
            worker
        })
        .unwrap();
    w.cost("a", Duration::from_millis(50));
    let (asked, waited) = w
        .block_on("a", move |system| async move {
            worker.tell(Job::Work).unwrap();
            let (sent, deadline) = (Instant::now(), system.clock().now() + Duration::from_millis(5));
            (worker.address.request(Some(deadline), Job::Get).await, sent.elapsed())
        })
        .unwrap();
    assert_eq!(asked, Err(RequestError::Deadline { admitted: true }), "queued behind a 50 ms turn");
    assert!(waited < Duration::from_millis(7), "the 5 ms deadline fired after {waited:?}");
}

#[test]
fn held_actors_stop_while_the_nodes_tasks_and_timers_go_on() {
    let mut w = systems(TurnBudget::default());
    let (actor, task) = (count(), count());
    let (a, t) = (actor.clone(), task.clone());
    drop(w.spawn_on("a", move |system| async move {
        system.spawn(Ticker(a)).start().unwrap();
        let mut every = tokio::time::interval(Duration::from_millis(10));
        loop {
            every.tick().await;
            t.fetch_add(1, Ordering::Relaxed);
        }
    }));
    w.run_for(Duration::from_millis(55));
    let before = (get(&actor), get(&task));
    assert!(before.0 >= 4, "{before:?}");
    w.hold_actors("a", Duration::from_millis(100));
    w.run_for(Duration::from_millis(99));
    assert_eq!((get(&actor), get(&task) - before.1), (before.0, 10), "held: the actor stops, the task goes on");
    w.run_for(Duration::from_millis(2));
    assert!(get(&actor) > before.0, "released on time, the actor ticks its overdue timer at once");
}

#[test]
fn refused_accepts_are_counted_as_accept_errors_until_they_are_allowed_again() {
    let mut w = World::new(WorldConfig::default());
    for name in ["a", "b"] {
        w.add(name, |host| async move { host.transport(SharedSecret::new(SECRET).unwrap()) });
    }
    let listen = |w: &mut World<axon_net::Config>| {
        let stats = Arc::new(Mutex::new(None::<ListenerStatsReader>));
        let s = stats.clone();
        drop(w.spawn_on("b", move |cfg| async move {
            let mut listener = Listener::bind(at(2), None, cfg, AcceptLimits::default()).await.unwrap();
            *s.lock().unwrap() = Some(listener.stats_reader());
            let mut open = Vec::new();
            while let Some(session) = listener.accept().await {
                open.push(session);
            }
        }));
        w.run_for(Duration::from_millis(10));
        move || stats.lock().unwrap().as_ref().unwrap().get()
    };
    let dial = |w: &mut World<axon_net::Config>| {
        w.block_on("a", |cfg| async move {
            timeout(Duration::from_secs(1), axon_net::connect(at(2), Lane::Control, None, &cfg)).await
        })
    };
    let stats = listen(&mut w);
    w.refuse_accepts("b", true);
    let refused = dial(&mut w).unwrap();
    assert!(!matches!(refused, Ok(Ok(_))), "a session past a listener that cannot accept");
    let during = stats();
    assert!(during.accept_errors > 0 && during.accepted == 0, "{during:?}");
    w.refuse_accepts("b", false);
    assert!(matches!(dial(&mut w).unwrap(), Ok(Ok(_))));
    assert_eq!(stats().accepted, 1);

    w.refuse_accepts("b", true);
    w.crash("b", Crash::Process);
    w.restart("b");
    let stats = listen(&mut w);
    assert!(matches!(dial(&mut w).unwrap(), Ok(Ok(_))), "a restart ends the refusal");
    assert_eq!(stats().accepted, 1);
}

#[derive(Clone)]
struct Seeded {
    cfg: axon_net::Config,
    discovery: SeedDiscovery,
    _seed: Option<Arc<SeedServer>>,
}

/// s is the seed, and every node's discovery registers there.
async fn seeded(host: Host) -> Seeded {
    let cfg = host.transport(SharedSecret::new(SECRET).unwrap());
    let seed = match host.name() {
        "s" => Some(Arc::new(SeedServer::bind(SeedServerConfig::new(host.addr(), &cfg)).await.unwrap())),
        _ => None,
    };
    let discovery = SeedDiscovery::start(SeedConfig::new([at(1).to_string()], &cfg)).await.unwrap();
    Seeded { cfg, discovery, _seed: seed }
}

/// b's incarnation, and what the seed made of its record.
fn register_b(w: &mut World<Seeded>) -> (u128, Result<(), DiscoveryError>) {
    let heard = w.block_on("b", |n| async move {
        let incarnation = n.cfg.identity.incarnation;
        let record =
            NodeRecord { id: "b".into(), incarnation, address: at(2), metadata: Default::default(), version: 0 };
        drop(n.discovery.register(&record).await);
        // A node that has not heard from its seeds registers anyway: wait for the seed's answer.
        while !n.discovery.stats().live && n.discovery.stats().refusals == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        (incarnation, n.discovery.heartbeat("b", incarnation).await)
    });
    heard.unwrap()
}

#[test]
fn a_wall_step_back_gives_an_older_incarnation_that_the_seeds_refuse() {
    let mut w = World::new(WorldConfig::default());
    for name in ["s", "b"] {
        w.add(name, seeded);
    }
    let (first, listed) = register_b(&mut w);
    assert_eq!(listed, Ok(()));

    let read = |w: &World<Seeded>| w.read("b", |_| (axon::trace::unix_micros(), Instant::now())).unwrap();
    let (wall, mono) = read(&w);
    w.step_wall("b", -60_000);
    let (stepped, mono_after) = read(&w);
    assert_eq!((wall - stepped, mono_after), (60_000_000, mono), "the wall clock steps back a minute, alone");
    w.run_for(Duration::from_millis(10));
    let (later, mono_later) = read(&w);
    assert_eq!((later - stepped, mono_later - mono), (10_000, Duration::from_millis(10)), "both run on together");

    // The seed holds b's first boot until the reset's grace is over.
    w.crash("b", Crash::Process);
    w.restart("b");
    let (again, refused) = register_b(&mut w);
    assert!(again >> 64 < first >> 64, "the next boot is older: {first:x} then {again:x}");
    assert_eq!(refused, Err(DiscoveryError::Stale));
}

// ---- the actor features of one node while its actors are held, the node is paused, or it is starved: each part
// gets what it was sent or owed once, late, and nothing twice.

const FAULT: Duration = Duration::from_millis(200);

/// How often each part saw each thing, as "part thing".
type Tally = Arc<Mutex<BTreeMap<String, u64>>>;

enum Msg {
    Work,
    Fail,
    Down,
    Tick,
    Done,
    Ready,
    Closed,
    Get(ReplyTo<u64>),
}

#[derive(Default)]
struct GateState {
    open: bool,
    waker: Option<Waker>,
    written: Vec<u32>,
}

/// A sink that takes nothing until it is opened.
#[derive(Clone, Default)]
struct Gate(Arc<Mutex<GateState>>);

impl Gate {
    fn open(&self) {
        let waker = {
            let mut state = self.0.lock().unwrap();
            state.open = true;
            state.waker.take()
        };
        waker.into_iter().for_each(Waker::wake);
    }

    fn written(&self) -> Vec<u32> {
        self.0.lock().unwrap().written.clone()
    }
}

impl Sink<u32> for Gate {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<(), ()>> {
        let mut state = self.0.lock().unwrap();
        if state.open {
            return Poll::Ready(Ok(()));
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, item: u32) -> Result<(), ()> {
        self.0.lock().unwrap().written.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
}

enum Role {
    Plain,
    Watches(LifecycleHandle),
    Links(LifecycleHandle),
    /// Ticks every 10 ms, runs a 100 ms job, and writes 0..6 through the gate two at a time.
    Busy(Gate),
}

struct Part {
    name: &'static str,
    tally: Tally,
    role: Role,
    keep: Option<Box<dyn Any + Send>>,
    sink: Option<SinkHandle<u32>>,
    outbox: VecDeque<u32>,
    work: u64,
}

fn part(name: &'static str, tally: &Tally, role: Role) -> Part {
    Part { name, tally: tally.clone(), role, keep: None, sink: None, outbox: VecDeque::new(), work: 0 }
}

impl Part {
    fn saw(&self, what: &str) {
        *self.tally.lock().unwrap().entry(format!("{} {what}", self.name)).or_default() += 1;
    }
}

impl Actor for Part {
    type Message = Msg;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match &self.role {
            Role::Plain => {}
            Role::Watches(target) => self.keep = Some(Box::new(cx.watch(target, |_| Msg::Down).map_err(drop)?)),
            Role::Links(peer) => self.keep = Some(Box::new(cx.link(peer))),
            Role::Busy(gate) => {
                cx.schedule_every(Duration::from_millis(10), || Msg::Tick).map_err(drop)?;
                cx.submit_future(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Msg::Done
                })
                .map_err(drop)?;
                let events = |event| if event == SinkEvent::Ready { Msg::Ready } else { Msg::Closed };
                self.sink = Some(cx.attach_sink(gate.clone(), 2, events).map_err(drop)?);
                self.outbox.extend(0..6);
            }
        }
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        let what = match msg {
            Msg::Work => {
                self.work += 1;
                "work"
            }
            Msg::Fail => return Err(()),
            Msg::Down => "down",
            Msg::Tick => "tick",
            Msg::Done => "done",
            Msg::Ready => "ready",
            Msg::Closed => "closed",
            Msg::Get(reply) => return reply.reply(self.work).map_err(drop),
        };
        self.saw(what);
        Ok(())
    }

    fn on_turn_end(&mut self, _: &mut Context<'_, Self>) -> Result<(), ()> {
        let Some(sink) = &self.sink else { return Ok(()) };
        while let Some(item) = self.outbox.pop_front() {
            if let Err(refused) = sink.try_send(item) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message);
                }
                break;
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, reason: &StopReason) {
        self.saw(match reason {
            StopReason::Linked(_) => "linked",
            StopReason::Failed(_) => "failed",
            StopReason::Stopped => "stopped",
            StopReason::Drained => "drained",
            _ => "ended otherwise",
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    Hold,
    Pause,
    Starve,
}

fn under(seed: u64, fault: Fault) {
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    w.add("a", |host| async move { ActorSystem::builder().runtime(host.runtime()).build().unwrap() });
    let (tally, gate) = (Tally::default(), Gate::default());
    let (t, g) = (tally.clone(), gate.clone());
    let (family, target, peer, router, drained) = w
        .block_on("a", move |system| async move {
            let policy = RestartPolicy {
                // A restart 50 ms after the failure, exactly.
                backoff: Backoff { initial: Duration::from_millis(50), jitter: 0.0, ..RestartPolicy::BACKOFF },
                ..Default::default()
            };
            let f = t.clone();
            let family = system.supervise(move || part("family", &f, Role::Plain)).policy(policy);
            let family = family.start_supervised().unwrap();
            let spawn = |part| system.spawn(part).start().unwrap();
            let target = spawn(part("target", &t, Role::Plain));
            let peer = spawn(part("peer", &t, Role::Plain));
            let router = Router::new(Routing::RoundRobin);
            (0..3).for_each(|_| router.add(spawn(part("member", &t, Role::Plain))));
            let drained = spawn(part("drained", &t, Role::Plain));
            for other in [
                spawn(part("watcher", &t, Role::Watches(target.lifecycle.clone()))),
                spawn(part("linked", &t, Role::Links(peer.lifecycle.clone()))),
                spawn(part("busy", &t, Role::Busy(g))),
            ] {
                other.ready().await.unwrap();
            }
            // The family goes into its backoff as the fault begins.
            assert_eq!(family.ask(Msg::Get).await.unwrap(), 0);
            family.tell(Msg::Fail).unwrap();
            while family.family.current().is_some() {
                tokio::task::yield_now().await;
            }
            (family, target, peer, router, drained)
        })
        .unwrap();

    let what = format!("{fault:?}, seed {seed}");
    let (before, start) = (tally.lock().unwrap().clone(), w.now());
    match fault {
        Fault::Hold => w.hold_actors("a", FAULT),
        Fault::Pause => w.pause("a", FAULT),
        Fault::Starve => w.starve("a", 8),
    }
    let f = family.clone();
    drop(w.spawn_on("a", move |_| async move {
        target.lifecycle.stop();
        peer.tell(Msg::Fail).unwrap();
        for _ in 0..50 {
            drained.tell(Msg::Work).unwrap();
        }
        drained.lifecycle.drain(None);
        f.send(Msg::Work).await.unwrap();
        for _ in 0..100 {
            router.send(Msg::Work).await.unwrap();
        }
    }));
    w.run_for(FAULT / 4);
    gate.open();
    w.run_for(FAULT - FAULT / 4 - MS);
    if fault != Fault::Starve {
        assert_eq!(*tally.lock().unwrap(), before, "{what}: nothing is handled while the fault lasts");
    }
    if fault == Fault::Pause {
        assert!(gate.written().is_empty(), "{what}: a paused node writes nothing");
    }
    w.run_for(MS);
    if fault == Fault::Starve {
        w.starve("a", 1);
    }

    let expected: BTreeMap<String, u64> = [
        ("busy done", 1),
        ("drained drained", 1),
        ("drained work", 50),
        ("family failed", 1),
        ("family work", 1),
        ("linked linked", 1),
        ("member work", 100),
        ("peer failed", 1),
        ("target stopped", 1),
        ("watcher down", 1),
    ]
    .into_iter()
    .map(|(what, n)| (what.to_owned(), n))
    .collect();
    w.run_until(&format!("{what}: each part gets what it was sent or owed"), Duration::from_secs(2), |_| {
        let tally = tally.lock().unwrap();
        expected.iter().all(|(k, n)| tally.get(k) >= Some(n)) && gate.written().len() == 6
    });
    w.run_for(Duration::from_secs(1));

    let mut seen = tally.lock().unwrap().clone();
    let ticks = seen.remove("busy tick").unwrap_or(0);
    assert!(seen.remove("busy ready").is_some(), "{what}: the refused write was owed a Ready");
    assert_eq!(seen, expected, "{what}: once each, and nothing twice");
    assert_eq!(gate.written(), [0, 1, 2, 3, 4, 5], "{what}: the sink wrote in order");
    let most = (w.now().as_millis() / 10) as u64 + 1;
    assert!((1..=most).contains(&ticks), "{what}: {ticks} ticks, at most {most}");
    if fault != Fault::Starve {
        // One tick for the 20 periods the fault took, and the period that follows, not one for each.
        let after = ticks - before.get("busy tick").copied().unwrap_or(0);
        let most = ((w.now() - start - FAULT).as_millis() / 10) as u64 + 3;
        assert!(after <= most, "{what}: {after} ticks from the fault on, at most {most}: missed periods skipped");
    }
    assert_eq!(w.block_on("a", move |_| async move { family.family.restarts() }).unwrap(), 1, "{what}");
}

#[test]
fn held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late() {
    check_seeds(seeds(0..8), |seed| {
        for fault in [Fault::Hold, Fault::Pause, Fault::Starve] {
            under(seed, fault);
        }
    })
    .unwrap();
}
