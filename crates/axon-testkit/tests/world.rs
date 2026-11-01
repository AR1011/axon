//! The World itself: real clusters on virtual time, crashed and restarted, over its byte pipes.
use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::{FileOp, Files, MemoryFiles};
use axon::{Actor, ActorSystem, Backoff, Context, Execution, ReplyTo, RestartPolicy};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, KeyedRef, MemoryDiscovery, OwnershipAuthority,
    PaxosConfig, PaxosLeases, Placement, Rejection, Reply, SpawnContext, WireProtocol,
};
use axon_net::{Compression, Lane, SharedSecret};
use axon_testkit::world::{Crash, Crashed, DISK_SYNC, Disk, Host, Task, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, crashes, seeds};
use bytes::{Buf, Bytes};
use common::{SECRET, at, contents, now, wires, wires_in, writes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

mod common;

const NODES: [&str; 3] = ["a", "b", "c"];
/// The same for every keyed call. It rides in each spawn request, where it is what compression finds to squeeze.
const ARGS: &[u8] = &[b'x'; 256];

/// Weak handles to what each boot of a node made, by node and by what they stand for.
#[derive(Clone, Default)]
struct Probes(Arc<Mutex<Vec<Probe>>>);

type Probe = (String, &'static str, Box<dyn Fn() -> bool + Send>);

impl Probes {
    /// A token that lives exactly as long as whatever holds it.
    fn token(&self, node: &str, what: &'static str) -> Arc<()> {
        let token = Arc::new(());
        let weak = Arc::downgrade(&token);
        self.0.lock().unwrap().push((node.to_owned(), what, Box::new(move || weak.strong_count() > 0)));
        token
    }

    fn system(&self, node: &str, system: &ActorSystem) {
        let weak = system.downgrade();
        self.0.lock().unwrap().push((node.to_owned(), "system", Box::new(move || weak.upgrade().is_some())));
    }

    fn count(&self, node: &str, what: &str) -> usize {
        self.0.lock().unwrap().iter().filter(|(n, w, _)| n == node && *w == what).count()
    }

    fn alive(&self, node: &str) -> Vec<&'static str> {
        let all = self.0.lock().unwrap();
        all.iter().filter(|(n, _, alive)| n == node && alive()).map(|(_, what, _)| *what).collect()
    }
}

/// A deposit the account fails on, so that it restarts.
const FAIL: u64 = u64::MAX;

/// Answers a deposit with its id, keeps the reply to deposit 0, so that call waits, and fails on `FAIL`.
struct Account {
    kept: Vec<(Reply<u64>, Arc<()>)>,
    probes: Probes,
    node: String,
    _cell: Arc<()>,
}

struct Deposit(u64, Option<Reply<u64>>);

impl Actor for Account {
    type Message = Deposit;
    type Error = ();

    fn on_message(&mut self, Deposit(id, reply): Deposit, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match reply {
            _ if id == FAIL => return Err(()),
            Some(reply) if id == 0 => self.kept.push((reply, self.probes.token(&self.node, "cluster reply"))),
            Some(reply) => drop(reply.send(id)),
            None => {}
        }
        Ok(())
    }
}

struct AccountApi;

impl WireProtocol for AccountApi {
    type Message = u64;
    type Reply = u64;
    const ID: u64 = 0xA1;
    const SCHEMA: u16 = 1;

    fn encode(id: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&id.to_be_bytes()))
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("short reply".into()))
    }
}

impl ActorProtocol for AccountApi {
    type Mailbox = Deposit;

    fn encode_reply(id: u64) -> Result<Bytes, CallError> {
        Self::encode(id)
    }

    fn decode(mut body: Bytes, reply: Option<Reply<u64>>) -> Result<Deposit, Rejection> {
        body.try_get_u64().map(|id| Deposit(id, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

/// A local ask, never answered.
struct Keep {
    _reply: ReplyTo<u64>,
    _token: Arc<()>,
}

struct Keeper {
    kept: Vec<Keep>,
    _cell: Arc<()>,
}

impl Actor for Keeper {
    type Message = Keep;
    type Error = ();

    fn on_message(&mut self, keep: Keep, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.kept.push(keep);
        Ok(())
    }
}

/// Paused on a job that never ends, so what it is sent stays in its mailbox.
struct Paused {
    _cell: Arc<()>,
}

impl Actor for Paused {
    type Message = Keep;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let job = cx.submit_future(std::future::pending()).map_err(drop)?;
        cx.pause_until(&job).map_err(drop)
    }

    fn on_message(&mut self, _: Keep, _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

#[derive(Clone)]
struct Node {
    cluster: Cluster,
    _system: ActorSystem,
}

impl Node {
    fn account(&self, key: &str) -> KeyedRef<AccountApi> {
        self.cluster.keyed::<AccountApi>("account", key, Bytes::from_static(ARGS), &Placement::default())
    }
}

/// Changes a node's config on the node, where anything that holds a clock must be built.
type Tweak = fn(&Host, &mut ClusterConfig);

async fn boot(host: Host, discovery: MemoryDiscovery, probes: Probes, tweak: Tweak) -> Node {
    let name = host.name().to_owned();
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    probes.system(&name, &system);
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    // Short, so each seed runs in moments of virtual time; axon-stream's world.rs runs the shipped timings.
    cfg.poll_interval = Duration::from_millis(20);
    tweak(&host, &mut cfg);
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let (p, n) = (probes.clone(), name.clone());
    let build = move |_: &SpawnContext, _: Bytes| {
        Ok(Account { kept: Vec::new(), probes: p.clone(), node: n.clone(), _cell: p.token(&n, "cell") })
    };
    // All jitter: when a failed account is back depends only on the draw.
    let backoff = Backoff { initial: Duration::from_millis(20), jitter: 1.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..RestartPolicy::default() };
    cluster.kind::<AccountApi>("account").policy(policy).register(build).await.unwrap();
    let keeper = Keeper { kept: Vec::new(), _cell: probes.token(&name, "cell") };
    let keeper = system.spawn(keeper).execution(Execution::Dedicated).start().unwrap();
    let paused = system.spawn(Paused { _cell: probes.token(&name, "cell") }).start().unwrap();
    for to in [keeper.clone(), keeper, paused] {
        let token = probes.token(&name, "reply");
        drop(tokio::task::spawn_local(async move { to.ask(|reply| Keep { _reply: reply, _token: token }).await }));
    }
    Node { cluster, _system: system }
}

/// Nodes that all see each other with the kind.
fn cluster(cfg: WorldConfig, names: &[&str], probes: &Probes, tweak: Tweak) -> World<Node> {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(cfg);
    for name in names {
        let (discovery, probes) = (discovery.clone(), probes.clone());
        w.add(name, move |host| boot(host, discovery.clone(), probes.clone(), tweak));
    }
    let count = names.len();
    w.run_until("every member with the kind", Duration::from_secs(30), |w| {
        let with_kind = |n: &Node| n.cluster.view().members.iter().filter(|m| m.record.has_kind("account")).count();
        w.nodes().count() == count && w.nodes().all(|(_, n)| with_kind(n) == count)
    });
    w
}

fn deposit(w: &mut World<Node>, from: &str, key: &str, id: u64) -> Task<Result<u64, ClusterError>> {
    let key = key.to_owned();
    w.spawn_on(from, move |n| async move { n.account(&key).request(id).await })
}

/// A key whose actor runs on `node`.
fn key_on(w: &mut World<Node>, node: &str) -> String {
    for k in 0.. {
        let key = format!("acct-{k}");
        let k = key.clone();
        let on = w.block_on("a", move |n| async move {
            let found = n
                .cluster
                .get_or_spawn::<AccountApi>("account", &k, Bytes::from_static(ARGS), &Placement::default())
                .await;
            found.unwrap().node().to_owned()
        });
        if on.unwrap() == node {
            return key;
        }
    }
    unreachable!()
}

fn members(w: &World<Node>) -> Vec<Vec<String>> {
    w.nodes().map(|(_, n)| n.cluster.view().members.iter().map(|m| m.record.id.clone()).collect()).collect()
}

#[test]
fn three_nodes_on_memory_discovery_reach_a_steady_view_and_answer_a_keyed_call() {
    check_seeds(seeds(0..4), |seed| {
        let mut w = cluster(WorldConfig { seed, ..WorldConfig::default() }, &NODES, &Probes::default(), |_, _| {});
        let view = members(&w);
        w.run_for(Duration::from_secs(2));
        assert_eq!(members(&w), view, "seed {seed}: the view moved\n{}", w.trace_tail(40));
        let key = key_on(&mut w, "b");
        let answer = deposit(&mut w, "c", &key, 7);
        assert_eq!(w.join(answer).unwrap().unwrap(), 7, "seed {seed}\n{}", w.trace_tail(40));
        println!("world digest three_nodes seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn after_a_crash_no_byte_leaves_the_node() {
    check_seeds(seeds(0..4), |seed| {
        let how = if seed % 2 == 0 { Crash::Machine } else { Crash::Process };
        // No jitter: what b wrote before the crash lands one quantum and a latency after it at the latest.
        let wire = Wire { jitter: Duration::ZERO, ..Wire::default() };
        let mut w =
            cluster(WorldConfig { seed, wire, ..WorldConfig::default() }, &NODES, &Probes::default(), |_, _| {});
        let key = key_on(&mut w, "b");
        let held = deposit(&mut w, "a", &key, 0);
        w.run_for(Duration::from_millis(50));
        w.crash("b", how);
        w.run_for(wire.latency + Duration::from_millis(1));
        w.log("landed");
        w.run_for(Duration::from_secs(6));
        let trace = w.trace_tail(usize::MAX);
        let (before, after) = trace.split_once("log landed").unwrap();
        assert!(before.contains(" b>a "), "seed {seed}: b never sent anything\n{before}");
        let sent: Vec<_> = after.lines().filter(|line| line.contains(" b>")).collect();
        assert!(sent.is_empty(), "seed {seed}: {how:?}: b sent after its crash:\n{}", sent.join("\n"));
        let reset =
            before.lines().skip_while(|l| !l.contains("crash b")).any(|l| l.contains(" b>a ") && l.ends_with("rst"));
        assert_eq!(reset, how == Crash::Process, "seed {seed}: {how:?}: only a process crash resets its peers");
        assert!(matches!(w.join(held), Ok(Err(_))), "seed {seed}: the held call ends when b goes");
        println!("world digest after_a_crash seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn a_thousand_crashes_leave_no_system_cell_or_reply_alive() {
    const CYCLES: usize = 1000;
    let probes = Probes::default();
    let mut w =
        cluster(WorldConfig { seed: seeds(7..8).start, ..WorldConfig::default() }, &["a", "b"], &probes, |_, _| {});
    let key = key_on(&mut w, "b");
    // a keeps a call to b's key open; each incarnation of b keeps the one that reaches it.
    drop(w.spawn_on("a", move |n| async move {
        let account = n.account(&key);
        loop {
            drop(tokio::time::timeout(Duration::from_millis(50), account.request(0)).await);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }));
    for cycle in 0..CYCLES {
        let kept = probes.count("b", "cluster reply");
        w.run_until("b keeps a call", Duration::from_secs(10), |_| probes.count("b", "cluster reply") > kept);
        let how = if cycle % 2 == 0 { Crash::Process } else { Crash::Machine };
        w.crash("b", how);
        let alive = probes.alive("b");
        assert!(alive.is_empty(), "cycle {cycle}, {how:?}: {alive:?} outlived the crash\n{}", w.trace_tail(30));
        w.restart("b");
    }
    for (what, each) in [("system", 1), ("cell", 3), ("reply", 3), ("cluster reply", 1)] {
        assert!(probes.count("b", what) >= CYCLES * each, "{what}: {}", probes.count("b", what));
    }
}

#[test]
#[cfg_attr(not(tokio_unstable), ignore = "reproducible only in the sim build: scripts/check.sh sim")]
fn the_same_seed_gives_the_same_trace() {
    // Paxos leases, and zstd on every frame that pays: the World's one run with compression on.
    let tweak: Tweak = |host, cfg| {
        let mut paxos = PaxosConfig::new(NODES);
        paxos.store = Some(Arc::new(host.disk().dir("leases")));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
        for lane in Lane::ALL {
            cfg.transport.lanes.get_mut(lane).compression = Compression::Zstd;
        }
        cfg.transport.compression_threshold = 64;
        cfg.transport.compression_min_savings = 0;
    };
    check_seeds(seeds(0..2), |seed| {
        // With `reads`, every node is read between each two steps of a `run_for`, and around the calls: its clocks,
        // its seeded stream and its stats. A read that moved any of them, or charged c a turn, would part the runs.
        let run = |reads: bool| {
            let look = |w: &World<Node>| {
                for name in NODES.into_iter().filter(|_| reads) {
                    std::hint::black_box(w.read(name, |n| {
                        let (c, wall) = (&n.cluster, axon::trace::unix_micros());
                        (wall, tokio::time::Instant::now(), axon::trace::random_u64(), c.clock().now(), c.stats())
                    }));
                }
            };
            let pass = |w: &mut World<Node>, d: Duration| {
                let end = w.now() + d;
                while w.now() < end {
                    w.run_for(if reads { WorldConfig::default().quantum } else { d });
                    look(w);
                }
            };
            let mut w = cluster(WorldConfig { seed, ..WorldConfig::default() }, &NODES, &Probes::default(), tweak);
            // Each turn on c takes time of its own.
            w.cost("c", Duration::from_micros(50));
            // Past PaxosConfig::wait, which a freshly started acceptor waits out before it grants.
            pass(&mut w, Duration::from_secs(7));
            let key = key_on(&mut w, "b");
            // Submitted together, so the session writer's gate decides, by the node's clock, what goes out in one write.
            let burst: Vec<_> = (1..=20).map(|id| (id, deposit(&mut w, "a", &key, id))).collect();
            for (id, answer) in burst {
                look(&w);
                let answer = w.join(answer);
                assert!(matches!(answer, Ok(Ok(got)) if got == id), "seed {seed}: {answer:?}\n{}", w.trace_tail(40));
            }
            // The restart's jitter decides when the next answer goes out.
            let failed = deposit(&mut w, "a", &key, FAIL);
            assert!(matches!(w.join(failed), Ok(Err(_))), "seed {seed}: the failing deposit was answered");
            let back = (0..50).find_map(|_| {
                let answer = deposit(&mut w, "a", &key, 22);
                w.join(answer).ok()?.ok()
            });
            assert_eq!(back, Some(22), "seed {seed}: the account never came back\n{}", w.trace_tail(40));
            // Held on b when it crashes, so the callers hear together, in call order; each then calls c, which puts
            // that order on the wire.
            let on_c = key_on(&mut w, "c");
            let held: Vec<_> = (31..=38)
                .map(|id| {
                    let (key, on_c) = (key.clone(), on_c.clone());
                    let task = w.spawn_on("a", move |n| async move {
                        (n.account(&key).request(0).await.is_err(), n.account(&on_c).request(id).await)
                    });
                    (id, task)
                })
                .collect();
            pass(&mut w, Duration::from_millis(100));
            w.crash("b", Crash::Process);
            for (id, task) in held {
                look(&w);
                let got = w.join(task);
                assert!(matches!(got, Ok((true, Ok(got))) if got == id), "seed {seed}: {got:?}\n{}", w.trace_tail(40));
            }
            // Past the lease, so a and c move b's key between them.
            pass(&mut w, Duration::from_secs(8));
            // A call that still goes to b is refused, which marks b unreachable, so the next one places the key anew.
            let mut moved = (0..3).map(|_| {
                let answer = deposit(&mut w, "c", &key, 21);
                look(&w);
                w.join(answer)
            });
            assert!(moved.any(|answer| matches!(answer, Ok(Ok(21)))), "seed {seed}\n{}", w.trace_tail(40));
            w.restart("b");
            pass(&mut w, Duration::from_secs(3));
            let session =
                |n: &Node| n.cluster.stats().engine.peers.iter().map(|p| p.session.tx_compressed_frames).sum::<u64>();
            assert!(w.nodes().map(|(_, n)| session(n)).sum::<u64>() > 0, "seed {seed}: nothing was compressed");
            w
        };
        let (one, two) = (run(false), run(true));
        println!("world digest the_same_seed seed {seed}: {:016x}", one.digest());
        if let Some(report) = one.divergence(&two) {
            panic!("{report}");
        }
    })
    .unwrap();
}

#[derive(Clone)]
struct Acceptor {
    leases: Arc<PaxosLeases>,
    cluster: Cluster,
    _system: ActorSystem,
}

async fn acceptor(host: Host, discovery: MemoryDiscovery) -> Acceptor {
    let transport = host.transport(SharedSecret::new(SECRET).unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let mut paxos = PaxosConfig::new(NODES);
    paxos.store = Some(Arc::new(host.disk().dir("leases")));
    let leases = Arc::new(PaxosLeases::new(paxos).unwrap());
    let mut cfg = ClusterConfig::new(host.addr(), transport, discovery);
    // Short, so each seed runs in moments of virtual time; axon-stream's world.rs runs the shipped timings.
    cfg.poll_interval = Duration::from_millis(20);
    cfg.ownership = Some(leases.clone());
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    Acceptor { leases, cluster, _system: system }
}

/// Asks every 100 ms until the key is granted, and answers how long that took on the node's clock.
fn take(w: &mut World<Acceptor>, on: &str, key: &'static str) -> Duration {
    let took = w.block_on(on, move |n| async move {
        let started = tokio::time::Instant::now();
        while n.leases.acquire(key).await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        started.elapsed()
    });
    took.unwrap()
}

/// A restarted acceptor refuses for as long as a lease it forgot could last, measured on its own clock. Read from the
/// real clock, that wait outlasted hundreds of virtual seconds.
#[test]
fn a_paxos_acquire_after_the_acceptors_restart_waits_out_their_restart_in_virtual_time() {
    check_seeds(seeds(0..2), |seed| {
        let discovery = MemoryDiscovery::new();
        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        for name in NODES {
            let discovery = discovery.clone();
            w.add(name, move |host| acceptor(host, discovery.clone()));
        }
        take(&mut w, "a", "before");
        let start_ms = WorldConfig::default().wall_start_ms;
        let incarnation_ms = |w: &mut World<Acceptor>, name| {
            w.block_on(name, |n| async move { (n.cluster.record().incarnation >> 64) as u64 }).unwrap()
        };
        let first = incarnation_ms(&mut w, "b");
        w.crash("b", Crash::Process);
        let restarted_ms = start_ms + w.now().as_millis() as u64;
        w.restart("b");
        w.crash("c", Crash::Process);
        w.restart("c");
        // The node's wall clock: the World's start, plus the World's time.
        let again = incarnation_ms(&mut w, "b");
        assert!(first < again && again.abs_diff(restarted_ms) <= 1, "seed {seed}: {first} then {again}");
        let micros = w.read("a", |_| axon::trace::unix_micros()).unwrap();
        assert_eq!(micros, start_ms * 1000 + w.now().as_micros() as u64, "seed {seed}");
        let took = take(&mut w, "a", "after");
        // PaxosConfig::wait: the 6 s lease, 1 % drift and a 200 ms margin.
        let wait = Duration::from_millis(6260);
        println!("seed {seed}: the acquire after the restart took {took:?} of virtual time");
        assert!((wait..wait + Duration::from_secs(1)).contains(&took), "seed {seed}: {took:?}\n{}", w.trace_tail(40));
    })
    .unwrap();
}

#[test]
fn a_read_runs_as_the_node_at_once_and_sees_its_clocks_where_its_last_step_left_them() {
    let cfg = WorldConfig::default();
    let mut w: World<()> = World::new(cfg.clone());
    w.add("a", |_| async {});
    let start_us = cfg.wall_start_ms * 1000;
    let clocks = |w: &World<()>| w.read("a", |_| (tokio::time::Instant::now(), axon::trace::unix_micros()));
    assert!(clocks(&w).is_none(), "read before its boot");
    w.run_for(Duration::from_millis(250));
    let at = w.now();
    let (t0, wall) = clocks(&w).expect("booted");
    // No time passes in a read; the node's wall clock is the World's start plus the World's time, exactly.
    assert_eq!((w.now(), wall), (at, start_us + at.as_micros() as u64));
    // What a task stamps in the next step is no earlier, and less than a quantum later.
    let stamped = w.block_on("a", |_| async { tokio::time::Instant::now() }).unwrap();
    assert!((t0..t0 + cfg.quantum).contains(&stamped), "{:?} after the read", stamped - t0);
    // Drift: 10 % fast is 110 ms of the node's clock in 100 ms of the World's.
    w.drift("a", 100_000);
    let (t1, _) = clocks(&w).unwrap();
    w.run_for(Duration::from_millis(100));
    let (t2, _) = clocks(&w).unwrap();
    assert_eq!(t2 - t1, Duration::from_millis(110));
    w.drift("a", 0);
    // Suspended, its clock stands still; paused, it reads where its last step left it, and catches up at the next.
    w.suspend("a", Duration::from_millis(50));
    w.run_for(Duration::from_millis(50));
    assert_eq!(clocks(&w).unwrap().0, t2);
    w.pause("a", Duration::from_millis(50));
    w.run_for(Duration::from_millis(20));
    assert_eq!(clocks(&w).unwrap().0, t2);
    w.run_for(Duration::from_millis(40));
    assert_eq!(clocks(&w).unwrap().0 - t2, Duration::from_millis(60));
    // A read draws from a copy of the node's stream.
    let draw = |w: &World<()>| w.read("a", |_| axon::trace::random_u64()).unwrap();
    assert_eq!(draw(&w), draw(&w));
    // Down, then booting: nothing to read.
    w.crash("a", Crash::Process);
    assert!(clocks(&w).is_none());
    w.restart("a");
    assert!(clocks(&w).is_none());
    w.run_for(cfg.quantum);
    assert!(clocks(&w).is_some());
    // A panic in the read is the caller's, and the thread's wall clock is the machine's again.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.read("a", |_| panic!("in a read"))));
    assert!(panicked.is_err());
    assert!(axon::trace::unix_micros().abs_diff(start_us) > 86_400_000_000, "the node's wall outlived the read");
}

/// None: it waited out its timeout.
fn outcome<T>(done: Result<std::io::Result<T>, tokio::time::error::Elapsed>) -> Option<Result<T, ErrorKind>> {
    done.ok().map(|done| done.map_err(|e| e.kind()))
}

#[test]
fn a_connection_delivers_its_bytes_in_order_and_holds_a_writer_at_its_window() {
    check_seeds(seeds(0..8), |seed| {
        // A SYN-ACK held a second would let b's sleep end before a writes, and b read before the window filled.
        let stall = Duration::from_millis(20);
        let wire = Wire { stall_every: 16, stall, syn_stall: stall, ..Wire::default() };
        let mut w = wires(seed, wire, &["a", "b"]);
        let window = WorldConfig::default().window;
        let sent: Vec<u8> = (0..1u32 << 20).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
        let read = w.spawn_on("b", |net| async move {
            let (_, mut incoming) = net.bind(at(2)).unwrap();
            let (mut pipe, _) = incoming.recv().await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let (mut got, mut buf) = (Vec::new(), [0; 4096]);
            loop {
                match pipe.read(&mut buf).await.unwrap() {
                    0 => return got,
                    n => got.extend_from_slice(&buf[..n]),
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        let bytes = sent.clone();
        let write = w.spawn_on("a", move |net| async move {
            let mut pipe = net.connect(at(2), Lane::Control).await.unwrap();
            assert_eq!(pipe.write(&bytes).await.unwrap(), window, "a write takes at most the window");
            let past = timeout(Duration::from_millis(50), pipe.write(&bytes[window..])).await;
            assert!(past.is_err(), "a write went past the window before b read anything");
            // Small writes put many chunks on the wire at once, for the jitter and stalls to reorder.
            for piece in bytes[window..].chunks(1000) {
                pipe.write_all(piece).await.unwrap();
            }
        });
        w.join(write).unwrap();
        let got = w.join(read).unwrap();
        let first = got.iter().zip(&sent).position(|(x, y)| x != y);
        assert!(got == sent, "seed {seed}: {} of {} bytes, the first wrong at {first:?}", got.len(), sent.len());
    })
    .unwrap();
}

#[test]
fn a_dial_completes_one_round_trip_after_it_starts() {
    check_seeds(seeds(0..8), |seed| {
        let ms = Duration::from_millis;
        let mut w = wires(seed, Wire::default(), &["a", "b"]);
        let wire = |latency| Wire { latency, jitter: Duration::ZERO, ..Wire::default() };
        w.wire("a", "b", wire(ms(20)));
        w.wire("b", "a", wire(ms(30)));
        // Both tasks start in the same step, so their clocks start together.
        let accepted = w.spawn_on("b", |net| async move {
            let started = tokio::time::Instant::now();
            let (_, mut incoming) = net.bind(at(2)).unwrap();
            let pipe = incoming.recv().await.unwrap().unwrap();
            (pipe, started.elapsed())
        });
        let dialled = w.spawn_on("a", |net| async move {
            let started = tokio::time::Instant::now();
            let pipe = net.connect(at(2), Lane::Control).await.unwrap();
            (pipe, started.elapsed())
        });
        let (_far, accept) = w.join(accepted).unwrap();
        let (_pipe, dial) = w.join(dialled).unwrap();
        // A quantum for the SYN to leave, then each way's latency.
        assert!((ms(20)..ms(23)).contains(&accept), "seed {seed}: b accepted after {accept:?}");
        assert!((ms(50)..ms(53)).contains(&dial), "seed {seed}: a's dial completed after {dial:?}");
        w.crash("b", Crash::Process);
        let refused = w
            .block_on("a", |net| async move {
                let started = tokio::time::Instant::now();
                let refused = net.connect(at(2), Lane::Control).await.err().map(|e| e.kind());
                (refused, started.elapsed())
            })
            .unwrap();
        assert_eq!(refused.0, Some(ErrorKind::ConnectionRefused), "seed {seed}");
        assert!((ms(50)..ms(53)).contains(&refused.1), "seed {seed}: the refusal came back after {:?}", refused.1);
    })
    .unwrap();
}

#[test]
fn a_syn_that_draws_a_stall_waits_syn_stall_and_the_bytes_after_it_do_not() {
    check_seeds(seeds(0..8), |seed| {
        let ms = Duration::from_millis;
        let (latency, jitter, stall) = (ms(10), Duration::ZERO, Duration::ZERO);
        let wire = Wire { latency, jitter, stall_every: 1, stall, syn_stall: ms(1000), ..Wire::default() };
        let mut w = wires(seed, wire, &["a", "b"]);
        let read = w.spawn_on("b", |net| async move {
            let (_, mut incoming) = net.bind(at(2)).unwrap();
            let (mut pipe, _) = incoming.recv().await.unwrap().unwrap();
            let mut got = Vec::new();
            pipe.read_to_end(&mut got).await.unwrap();
            got.len()
        });
        let (dial, write) = w
            .block_on("a", |net| async move {
                let started = tokio::time::Instant::now();
                let mut pipe = net.connect(at(2), Lane::Control).await.unwrap();
                let dial = started.elapsed();
                let started = tokio::time::Instant::now();
                pipe.write_all(&vec![7; 1 << 20]).await.unwrap();
                (dial, started.elapsed())
            })
            .unwrap();
        // Every chunk draws a stall: the SYN and its answer wait a second each, the bytes nothing.
        assert!((ms(2020)..ms(2023)).contains(&dial), "seed {seed}: the dial completed after {dial:?}");
        assert!(write < ms(1000), "seed {seed}: 1 MiB took {write:?}, as if a byte waited syn_stall");
        assert_eq!(w.join(read).unwrap(), 1 << 20, "seed {seed}");
    })
    .unwrap();
}

#[test]
fn a_link_with_a_bandwidth_sends_one_chunk_at_a_time_in_the_order_they_came() {
    check_seeds(seeds(0..8), |seed| {
        const MIB: usize = 1 << 20;
        let wire = Wire {
            latency: Duration::from_millis(10),
            jitter: Duration::ZERO,
            bandwidth: MIB as u64,
            ..Wire::default()
        };
        // A window over the transfer, so each write is one chunk and only the link holds it back.
        let mut w = wires_in(WorldConfig { seed, wire, window: 2 * MIB, ..WorldConfig::default() }, &["a", "b"]);
        let read = Arc::new(Mutex::new([0; 2]));
        let counts = read.clone();
        drop(w.spawn_on("b", move |net| async move {
            let (_, mut incoming) = net.bind(at(2)).unwrap();
            for i in 0..2 {
                let (mut pipe, _) = incoming.recv().await.unwrap().unwrap();
                let counts = counts.clone();
                tokio::task::spawn_local(async move {
                    let mut buf = vec![0; 64 << 10];
                    while let Ok(n @ 1..) = pipe.read(&mut buf).await {
                        counts.lock().unwrap()[i] += n;
                    }
                });
            }
        }));
        let (mut first, mut second) = w
            .block_on("a", |net| async move {
                let first = net.connect(at(2), Lane::Control).await.unwrap();
                (first, net.connect(at(2), Lane::Control).await.unwrap())
            })
            .unwrap();
        let sent = w.now();
        let write = w.spawn_on("a", |_| async move {
            first.write_all(&vec![1; MIB]).await.unwrap();
            second.write_all(&vec![2; MIB / 2]).await.unwrap();
            (first, second)
        });
        let ms = Duration::from_millis;
        let mut took = [None; 2];
        w.run_until("both transfers read", ms(3000), |w| {
            for (i, len) in [MIB, MIB / 2].into_iter().enumerate() {
                if took[i].is_none() && read.lock().unwrap()[i] == len {
                    took[i] = Some(w.now() - sent);
                }
            }
            took.iter().all(Option::is_some)
        });
        let [first, second] = took.map(Option::unwrap);
        // A quantum for the write to leave, a second on the link, the latency and a quantum to read it.
        assert!((ms(1010)..ms(1014)).contains(&first), "seed {seed}: 1 MiB at 1 MiB/s took {first:?}");
        assert!((ms(1510)..ms(1514)).contains(&second), "seed {seed}: the second connection's took {second:?}");
        drop(w.join(write).unwrap());
    })
    .unwrap();
}

#[test]
fn a_process_crash_resets_and_refuses_and_a_machine_crash_goes_silent_until_its_restart() {
    check_seeds(seeds(0..8), |seed| {
        for how in [Crash::Process, Crash::Machine] {
            let mut w = wires(seed, Wire::default(), &["a", "b"]);
            // b never accepts, so its backlog fills.
            let listening = w.spawn_on("b", |net| async move {
                let _incoming = net.bind(at(2)).unwrap();
                std::future::pending::<()>().await
            });
            let (mut pipe, mut other, dialled) = w
                .block_on("a", |net| async move {
                    let mut pipes = Vec::new();
                    while let Ok(pipe) = timeout(Duration::from_millis(50), net.connect(at(2), Lane::Control)).await {
                        pipes.push(pipe.unwrap());
                    }
                    let dialled = pipes.len();
                    let mut pipes = pipes.into_iter();
                    (pipes.next().unwrap(), pipes.next().unwrap(), dialled)
                })
                .unwrap();
            assert_eq!(dialled, 64, "seed {seed}: a backlog holds 64 connections, and the next dial waits");
            let read = w.spawn_on("a", move |_| async move {
                let started = tokio::time::Instant::now();
                // A timeout polls the read once more when it fires, so only the time tells a woken read from that.
                let read = timeout(Duration::from_millis(100), pipe.read(&mut [0; 1])).await;
                (pipe, outcome(read), started.elapsed())
            });
            w.run_for(Duration::from_millis(10));
            w.crash("b", how);
            assert_eq!(w.join(listening).err(), Some(Crashed));
            let (mut pipe, read, took) = w.join(read).unwrap();
            if how == Crash::Process {
                assert!(took < Duration::from_millis(20), "seed {seed}: the reset reached the read after {took:?}");
            }
            let dial = w.block_on("a", |net| async move {
                outcome(timeout(Duration::from_millis(100), net.connect(at(2), Lane::Control)).await)
                    .map(|dial| dial.map(drop))
            });
            let expected = match how {
                Crash::Process => (Some(Err(ErrorKind::ConnectionReset)), Some(Err(ErrorKind::ConnectionRefused))),
                Crash::Machine => (None, None),
            };
            assert_eq!((read, dial.unwrap()), expected, "seed {seed}: {how:?}: what a's read and a new dial see");
            if how == Crash::Machine {
                // A byte sent into the silence is unacknowledged when b is back, so b's kernel resets the connection.
                pipe = w
                    .block_on("a", |_| async move {
                        pipe.write_all(b"x").await.unwrap();
                        pipe
                    })
                    .unwrap();
                w.run_for(Duration::from_millis(10));
                w.restart("b");
                // And a byte sent to the new boot on an old connection is answered with a reset.
                let reads = w.block_on("a", |_| async move {
                    let unacked = outcome(timeout(Duration::from_millis(100), pipe.read(&mut [0; 1])).await);
                    other.write_all(b"y").await.unwrap();
                    (unacked, outcome(timeout(Duration::from_millis(100), other.read(&mut [0; 1])).await))
                });
                let reset = Some(Err(ErrorKind::ConnectionReset));
                assert_eq!(reads.unwrap(), (reset, reset), "seed {seed}: after b's restart");
            }
        }
    })
    .unwrap();
}

#[test]
fn the_divergence_report_names_the_first_quantum_that_differs() {
    let run = |extra: bool| {
        let mut w = wires(0, Wire::default(), &["a", "b"]);
        w.run_for(Duration::from_millis(5));
        if extra {
            w.log("extra");
        }
        w.run_for(Duration::from_millis(5));
        w
    };
    let one = run(false);
    assert_eq!(one.divergence(&run(false)), None);
    let report = one.divergence(&run(true)).expect("a log line is in the digest");
    assert!(report.contains("part at quantum 5 ") && report.contains("log extra"), "{report}");
}

/// A process crash leaves what was written in the page cache. A machine crash keeps what was synced, and of what was
/// not the first bytes the seed draws per file: across seeds none of them, all of them and a torn part, the trace
/// saying which, and the same seed keeps the same.
#[test]
fn a_machine_crash_keeps_a_seeded_part_of_what_was_not_synced_and_a_process_crash_keeps_it_all() {
    let kept = |seed: u64| {
        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        w.add("a", |host: Host| async move { host.disk() });
        let written = w.block_on("a", |disk: Disk| async move {
            disk.append("f", b"ab").unwrap();
            disk.sync("f");
            disk.append("f", b"c").unwrap();
        });
        written.unwrap();
        w.crash("a", Crash::Process);
        let disk = w.disk("a");
        assert_eq!(disk.read("f").as_deref(), Some(&b"abc"[..]));
        w.restart("a");
        w.block_on("a", |disk: Disk| async move { disk.append("f", b"defg").unwrap() }).unwrap();
        w.crash("a", Crash::Machine);
        let f = disk.read("f").unwrap();
        assert!(f.len() >= 2 && b"abcdefg".starts_with(&f), "seed {seed}: {f:?}");
        let line = format!("disk f kept {} of 5 unsynced bytes", f.len() - 2);
        assert!(w.trace_tail(4).contains(&line), "seed {seed}: {line} not in\n{}", w.trace_tail(4));
        f.len() - 2
    };
    let seen: Vec<usize> = (0..64).map(kept).collect();
    assert!(seen.contains(&0) && seen.contains(&5) && seen.iter().any(|k| (1..5).contains(k)), "{seen:?}");
    assert_eq!((0..8).map(kept).collect::<Vec<_>>(), seen[..8], "a seed keeps the same");
}

/// A truncation not synced since is undone by a machine crash, whole, and a full disk refuses an append.
#[test]
fn a_machine_crash_undoes_a_truncation_not_synced() {
    let mut w = World::new(WorldConfig::default());
    w.add("a", |host: Host| async move { host.disk() });
    w.block_on("a", |disk: Disk| async move {
        disk.append("f", b"ab").unwrap();
        disk.sync("f");
    })
    .unwrap();
    w.crash("a", Crash::Process);
    let disk = w.disk("a");
    for sync in [false, true] {
        w.restart("a");
        w.block_on("a", move |disk: Disk| async move {
            disk.truncate("f", 1);
            disk.append("f", b"z").unwrap();
            if sync {
                disk.sync("f");
            }
        })
        .unwrap();
        w.crash("a", Crash::Machine);
        assert_eq!(disk.read("f").as_deref(), Some(if sync { &b"az"[..] } else { &b"ab"[..] }));
    }
    disk.set_capacity(Some(3));
    assert_eq!(disk.append("f", b"xy").unwrap_err().kind(), ErrorKind::StorageFull);
    assert_eq!((disk.free(), disk.list()), (1, vec![("f".to_owned(), 2)]));
}

/// A write over a file's synced bytes is lost to a machine crash until the file is synced, as an append is, and kept by
/// a process crash; it never grows the file.
#[test]
fn a_write_over_synced_bytes_is_lost_to_a_machine_crash_until_it_is_synced() {
    let mut w = World::new(WorldConfig::default());
    w.add("a", |host: Host| async move { host.disk() });
    let disk = w.disk("a");
    disk.append("f", b"0000").unwrap();
    disk.sync("f");
    for (crash, sync, kept) in
        [(Crash::Machine, false, b"0000"), (Crash::Process, false, b"0xy0"), (Crash::Machine, true, b"0xy0")]
    {
        w.block_on("a", move |disk: Disk| async move {
            disk.write("f", 1, b"xy").unwrap();
            if sync {
                disk.sync("f");
            }
        })
        .unwrap();
        w.crash("a", crash);
        assert_eq!(disk.read("f").as_deref(), Some(&kept[..]), "{crash:?}, synced {sync}");
        if crash == Crash::Process {
            disk.write("f", 1, b"00").unwrap();
        }
        w.restart("a");
    }
    assert_eq!(disk.write("f", 3, b"zz").unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(disk.write("g", 0, b"").unwrap_err().kind(), ErrorKind::NotFound);
}

/// A write syncs each file it appended to, one after another as `OsFiles` does, `DISK_SYNC` each: it takes longer with
/// each file it touches, not with each append. A file it cuts is synced at once, one it replaces before it lands, and
/// a write that syncs no file takes one sync still, the directory's. A file removed or replaced is not synced first.
#[test]
fn a_write_takes_a_sync_for_each_file_it_syncs() {
    let mut w = World::new(WorldConfig::default());
    w.add("a", |host: Host| async move { host.disk() });
    let op = |name: &str, data: &'static [u8]| FileOp::Append(name.into(), Bytes::from_static(data));
    let cases = [
        (vec![op("a", b"1"), op("a", b"2"), op("a", b"3")], 1),
        (vec![op("a", b"1"), op("b", b"2"), op("a", b"3"), op("c", b"4")], 3),
        (vec![FileOp::Remove("c".into())], 1),
        (vec![op("a", b"1"), FileOp::Truncate("b".into(), 0), op("a", b"2")], 3),
        (vec![FileOp::Replace("c".into(), Bytes::from_static(b"5")), op("a", b"3")], 2),
        (vec![FileOp::Truncate("b".into(), 9)], 1),
        (vec![op("a", b"1"), op("b", b"2"), FileOp::Replace("a".into(), Bytes::from_static(b"6"))], 2),
        (vec![op("a", b"1"), op("b", b"2"), FileOp::Remove("a".into())], 1),
    ];
    for (ops, syncs) in cases {
        let write = ops.clone();
        let took = w.block_on("a", move |disk: Disk| async move {
            let started = tokio::time::Instant::now();
            disk.apply(write).await.unwrap();
            started.elapsed()
        });
        assert_eq!(took.unwrap(), DISK_SYNC * syncs, "{ops:?}");
    }
}

/// A machine crash at any moment of a write leaves each case whole. A store that writes a file anew and removes the
/// old one, as a log's compaction might, keeps one of the two: what a write appends is synced before a removal after
/// it takes effect. A file replaced holds its old content or its new one, never part of either, and the new one only
/// with what the write appended before the replacement.
#[test]
fn a_machine_crash_inside_a_write_keeps_one_whole() {
    type Kept = [Option<Vec<u8>>; 2];
    let removed: fn(&Kept) -> bool = |kept| kept[0].as_deref() == Some(b"1") || kept[1].as_deref() == Some(b"12");
    let replaced: fn(&Kept) -> bool = |kept| match kept[0].as_deref() {
        Some(b"old") => true,
        Some(b"new!") => kept[1].as_deref() == Some(b"g"),
        _ => false,
    };
    let cases = [
        (
            FileOp::Append("old".into(), Bytes::from_static(b"1")),
            vec![FileOp::Append("new".into(), Bytes::from_static(b"12")), FileOp::Remove("old".into())],
            ["old", "new"],
            removed,
        ),
        (
            FileOp::Replace("f".into(), Bytes::from_static(b"old")),
            vec![
                FileOp::Append("g".into(), Bytes::from_static(b"g")),
                FileOp::Replace("f".into(), Bytes::from_static(b"new!")),
            ],
            ["f", "g"],
            replaced,
        ),
    ];
    for (before, write, names, whole) in cases {
        for at in 0..=2 * DISK_SYNC.as_millis() as u64 + 1 {
            let mut w = World::new(WorldConfig::default());
            w.add("a", |host: Host| async move { host.disk().dir("store") });
            let before = vec![before.clone()];
            w.block_on("a", |disk: Disk| async move { disk.apply(before).await }).unwrap().unwrap();
            let write = write.clone();
            let _write = w.spawn_on("a", |disk: Disk| async move { disk.apply(write).await });
            w.run_for(Duration::from_millis(at));
            w.crash("a", Crash::Machine);
            let disk = w.disk("a").dir("store");
            let kept = names.map(|name| disk.read(name));
            assert!(whole(&kept), "{names:?} crashed at {at} ms: {kept:?}");
        }
    }
}

/// A machine crash at any moment of a write leaves one of the ends `crashes` goes through: the World's disk and the
/// crash points are one model of `Files`, so a store proven on one holds on the other.
#[test]
fn a_machine_crash_inside_a_write_leaves_one_of_the_ends_crashes_goes_through() {
    let crashed_at = |seed: u64| {
        let mut writes = writes(seed);
        let write = writes.pop().expect("one write at least");
        let files = MemoryFiles::new();
        writes.iter().for_each(|ops| now(files.apply(ops.clone())));
        let ends: BTreeSet<_> = crashes(&files, &write).map(|(_, copy)| contents(&copy)).collect();

        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        w.add("a", |host: Host| async move { host.disk() });
        let landed = w.block_on("a", |disk: Disk| async move {
            for ops in writes {
                disk.apply(ops).await.unwrap();
            }
        });
        landed.unwrap();
        let ops = write.clone();
        let _write = w.spawn_on("a", |disk: Disk| async move { disk.apply(ops).await });
        let longest = (write.len() as u64 + 2) * DISK_SYNC.as_millis() as u64;
        let at = w.pick(0..longest + 1);
        w.run_for(Duration::from_millis(at));
        w.crash("a", Crash::Machine);
        let left = contents(&w.disk("a"));
        assert!(ends.contains(&left), "seed {seed}: crashed at {at} ms, {left:?} of {write:?}\n{}", w.trace_tail(4));
    };
    assert_eq!(check_seeds(seeds(0..256), crashed_at), Ok(()));
}

/// A directory of a machine's disk is a namespace of its own on the same machine: its parent's capacity, crashes and
/// panicking writes are its own.
#[test]
fn a_disk_directory_shares_its_machine() {
    let mut w = World::new(WorldConfig::default());
    w.add("a", |host: Host| async move { host.disk() });
    w.run_for(Duration::from_millis(1));
    let disk = w.disk("a");
    let (leases, streams) = (disk.dir("leases"), disk.dir("streams"));
    leases.append("f", b"12").unwrap();
    streams.append("f", b"3").unwrap();
    disk.append("f", b"4").unwrap();
    leases.sync("f");
    assert_eq!(leases.list(), [("f".to_owned(), 2)]);
    assert_eq!(disk.list(), [("f".to_owned(), 1)], "a directory's files are not its parent's");
    assert_eq!((streams.read("f").as_deref(), disk.read("f").as_deref()), (Some(&b"3"[..]), Some(&b"4"[..])));
    w.crash("a", Crash::Machine);
    assert_eq!(leases.read("f").as_deref(), Some(&b"12"[..]));
    assert!(matches!(streams.read("f").as_deref(), Some(b"" | b"3")), "{:?}", streams.read("f"));
    disk.set_capacity(Some(4));
    let used: usize = [&leases, &streams, &disk].iter().map(|d| d.read("f").map_or(0, |f| f.len())).sum();
    assert_eq!(streams.free(), 4 - used as u64, "the machine's capacity, less what its directories hold");
    assert_eq!(streams.append("g", b"xyz").unwrap_err().kind(), ErrorKind::StorageFull);
    streams.panic_writes(1);
    assert_eq!(disk.panics_left(), 1);
}

/// A write whose disk another write on the machine fills while it syncs fails, and leaves none of its appends but
/// those before a removal that landed: they may be all that is left of what it removed.
#[test]
fn a_write_that_finds_the_disk_filled_partway_cuts_its_appends_back() {
    for removes in [false, true] {
        let mut w = World::new(WorldConfig::default());
        w.add("a", |host: Host| async move { host.disk().dir("store") });
        w.run_for(Duration::from_millis(1));
        let (store, other) = (w.disk("a").dir("store"), w.disk("a").dir("other"));
        store.append("a", b"1").unwrap();
        if removes {
            store.append("x", b"0").unwrap();
        }
        store.set_capacity(Some(8));
        let write = vec![
            FileOp::Append("a".into(), Bytes::from_static(b"23")),
            FileOp::Remove("x".into()),
            FileOp::Append("b".into(), Bytes::from_static(b"45")),
        ];
        let write = w.spawn_on("a", |disk: Disk| async move { disk.apply(write).await });
        w.run_until("the first append landed", Duration::from_secs(1), |_| store.read("a").as_deref() == Some(b"123"));
        other.append("f", b"wxyz").unwrap();
        let written = w.join(write).unwrap();
        assert_eq!(written.unwrap_err().kind(), ErrorKind::StorageFull);
        let a: &[u8] = if removes { b"123" } else { b"1" };
        assert_eq!((store.read("a").as_deref(), store.read("b"), store.read("x")), (Some(a), None, None), "{removes}");
    }
}

enum LogMsg {
    Loaded(std::io::Result<usize>),
    Len(ReplyTo<usize>),
}

/// Appends a line to its log at each start, reading the log and writing it in one job.
struct Log {
    disk: Disk,
    len: usize,
}

impl Actor for Log {
    type Message = LogMsg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let disk = self.disk.clone();
        let job = cx.submit_future(async move {
            let had = match Files::read(&disk, "log").await {
                Ok(data) => data.len(),
                Err(e) if e.kind() == ErrorKind::NotFound => 0,
                Err(e) => return LogMsg::Loaded(Err(e)),
            };
            let start = FileOp::Append("log".into(), Bytes::from_static(b"start\n"));
            LogMsg::Loaded(disk.apply(vec![start]).await.map(|()| had + 6))
        });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: LogMsg, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            LogMsg::Loaded(len) => self.len = len.map_err(|e| e.to_string())?,
            LogMsg::Len(reply) => drop(reply.reply(self.len)),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Store {
    log: axon::ActorHandle<LogMsg>,
    _system: ActorSystem,
}

/// A store reads its files and writes them in one `submit_future` job: the disk's futures need no thread, which the
/// World would refuse, and what each start wrote outlives a machine crash.
#[test]
fn a_store_reads_then_writes_its_disk_in_one_job() {
    let mut w = World::new(WorldConfig::default());
    w.add("a", |host: Host| async move {
        let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
        let log = system.spawn(Log { disk: host.disk().dir("store"), len: 0 }).start().unwrap();
        Store { log, _system: system }
    });
    for starts in 1..=3 {
        let len = w.block_on("a", |n| async move { n.log.ask(LogMsg::Len).await.unwrap() }).unwrap();
        assert_eq!(len, 6 * starts);
        w.crash("a", Crash::Machine);
        w.restart("a");
    }
}

/// A failed seed that started a World's node says whether this build replays it, and how to build one that does; one
/// that started none, as a pure simulation's, replays in any build and does not say so.
#[test]
fn a_failed_seed_says_whether_this_build_replays_it() {
    let failed = check_seeds(0..2, |seed| {
        let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
        w.add("a", |_| async {});
        w.run_for(Duration::from_millis(1));
        assert!(seed == 0, "seed {seed} fails");
    });
    let message = failed.expect_err("seed 1 fails").message;
    assert_eq!(message.contains("RUSTFLAGS=\"--cfg tokio_unstable\""), cfg!(not(tokio_unstable)), "{message}");
    let pure = check_seeds(0..1, |seed| panic!("seed {seed} fails")).expect_err("it fails").message;
    assert!(!pure.contains("replay"), "{pure}");
}
