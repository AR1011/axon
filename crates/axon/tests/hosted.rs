//! Real threads: ordering, backpressure, shutdown honesty, close/commit linearization.
#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context as Cx, Poll, Wake, Waker};
use std::time::Duration;

use axon::*;

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(f: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut f = pin!(f);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut Cx::from_waker(&waker)) {
            return v;
        }
        std::thread::park();
    }
}

enum Msg {
    From(usize, u32),
    Snapshot(ReplyTo<Vec<Vec<u32>>>),
    Block(Duration),
    /// Says it has begun, then blocks until released: a stop that lands before it begins discards it instead.
    Hold(std::sync::mpsc::SyncSender<()>, std::sync::mpsc::Receiver<()>),
}

struct Collector(Vec<Vec<u32>>);

impl Actor for Collector {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::From(p, v) => self.0[p].push(v),
            Msg::Snapshot(reply) => drop(reply.reply(self.0.clone())),
            Msg::Block(d) => std::thread::sleep(d),
            Msg::Hold(began, release) => {
                let _ = began.send(());
                let _ = release.recv();
            }
        }
        Ok(())
    }
}

const SECOND: Duration = Duration::from_secs(1);

fn eventually(what: &str, done: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + SECOND;
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::yield_now();
    }
}

// Registry cleanup happens just after the exit is published.
fn eventually_empty(rt: &Runtime) {
    eventually("actors still registered", || rt.actor_count() == 0);
}

fn runtime(workers: usize) -> Runtime {
    Runtime::new(RuntimeConfig { workers, ..Default::default() }).unwrap()
}

#[test]
fn concurrent_producers_keep_their_own_order_under_backpressure() {
    let rt = runtime(2);
    let limits = MailboxLimits { messages: 8, ..Default::default() };
    let actor = rt.spawn(Collector(vec![vec![]; 4]), Execution::Shared, limits).unwrap().wait_ready(SECOND).unwrap();
    std::thread::scope(|s| {
        for p in 0..4 {
            let address = actor.address.clone();
            s.spawn(move || (0..2000).for_each(|v| block_on(address.send(Msg::From(p, v), None)).map(drop).unwrap()));
        }
    });
    let seen = block_on(actor.address.request(None, Msg::Snapshot)).unwrap();
    assert!(seen.iter().all(|s| s.iter().copied().eq(0..2000)));
    // The snapshot's bytes are released when its batch ends, just after the handler has replied.
    eventually("the snapshot's bytes are released", || actor.address.diagnostics().mailbox_bytes == 0);
    let d = actor.address.diagnostics();
    assert_eq!((d.admitted, d.waiters, d.mailbox_bytes), (8001, 0, 0));
    assert_eq!(rt.shutdown(SECOND).exits.len(), 1);
    eventually_empty(&rt);
}

#[test]
fn dedicated_actors_and_many_shared_actors_all_answer() {
    let rt = runtime(4);
    let mut actors: Vec<_> = (0..200)
        .map(|_| rt.spawn(Collector(vec![vec![]]), Execution::Shared, MailboxLimits::default()).unwrap())
        .collect();
    actors.push(rt.spawn(Collector(vec![vec![]]), Execution::Dedicated, MailboxLimits::default()).unwrap());
    for (i, a) in actors.iter().enumerate() {
        let a = a.wait_ready(SECOND).unwrap();
        a.address.try_tell(Msg::From(0, i as u32)).unwrap();
        assert_eq!(block_on(a.address.request(None, Msg::Snapshot)).unwrap(), [[i as u32]]);
    }
    let stats = rt.stats();
    assert_eq!(stats.actors, 201);
    // An ask from this plain thread may run its turn here, not on a worker.
    assert!(stats.workers.iter().map(|w| w.turns).sum::<u64>() >= 200);
    assert_eq!(rt.shutdown(SECOND).exits.len(), 201);
}

#[test]
fn a_handler_that_blocks_does_not_hold_up_the_actors_that_share_its_worker() {
    // Without a spin, idle workers look for work only in their sleep loop.
    for spin in [RuntimeConfig::default().spin, Duration::ZERO] {
        let rt = Runtime::new(RuntimeConfig { workers: 2, spin, ..Default::default() }).unwrap();
        let actors: Vec<_> = (0..32)
            .map(|_| rt.spawn(Collector(vec![vec![]]), Execution::Shared, MailboxLimits::default()).unwrap())
            .map(|a| a.wait_ready(SECOND).unwrap())
            .collect();
        // Long enough for both workers to stop spinning and sleep, so none is looking when the turn blocks.
        std::thread::sleep(Duration::from_millis(20));
        actors[0].address.try_tell(Msg::Block(Duration::from_millis(500))).unwrap();
        let began = std::time::Instant::now();
        // Placed round robin: the even ones share the blocked actor's worker and queue behind its turn. They go
        // first, so nothing sent to the other worker wakes it.
        for group in [(2..32).step_by(2), (1..32).step_by(2)] {
            for i in group.clone() {
                actors[i].address.try_tell(Msg::From(0, i as u32)).unwrap();
            }
            for i in group {
                assert_eq!(block_on(actors[i].address.request(None, Msg::Snapshot)).unwrap(), [[i as u32]]);
            }
        }
        let waited = began.elapsed();
        assert!(waited < Duration::from_millis(250), "spin {spin:?}: {waited:?}");
        assert_eq!(rt.shutdown(SECOND).exits.len(), 32);
    }
}

#[test]
fn a_worker_that_is_looking_takes_what_lands_on_a_sleeping_one() {
    struct Where;
    impl Actor for Where {
        type Message = ReplyTo<std::thread::ThreadId>;
        type Error = ();
        fn on_message(&mut self, reply: Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
            let _ = reply.reply(std::thread::current().id());
            Ok(())
        }
    }
    let spin = Duration::from_millis(50);
    let rt = Runtime::new(RuntimeConfig { workers: 3, spin, ..Default::default() }).unwrap();
    // Placed round robin, one on each worker. Worker 1 going busy wakes worker 0 to watch it, so the one left
    // asleep is worker 2.
    let [held, awake, asleep] = [(); 3].map(|_| {
        rt.spawn(Where, Execution::Shared, MailboxLimits::default()).unwrap().wait_ready(SECOND).unwrap().address
    });
    // Never polled, so never sent: with an ask outstanding on this thread, the others run no turn here.
    let _held = held.request(None, |reply| reply);
    let whose = |address: &ActorRef<_>| block_on(address.request(None, |reply| reply)).unwrap();
    // Past the longest spin, five times `spin`, so all sleep. A message from outside wakes only its home worker, and
    // no sleeper looks for work while none is busy, so this runs at home.
    std::thread::sleep(spin * 6);
    let home = whose(&asleep);
    // Waking a sleeping worker takes longer than one that is spinning (worker 1, or worker 0 watching it) needs to
    // see its queue, but not always: measured, the home worker won 1 try in 100 idle, and at most 56 on two cores
    // shared with two busy loops, where 30 tries all fail fewer than once in 10^7 runs.
    let taken = (0..30).any(|_| {
        std::thread::sleep(spin * 6);
        whose(&awake);
        whose(&asleep) != home
    });
    assert!(taken, "every message waited for its own worker to wake");
    assert_eq!(rt.shutdown(SECOND).exits.len(), 3);
}

#[test]
fn a_handler_that_wakes_several_actors_runs_them_all() {
    struct Fan(Vec<ActorRef<Msg>>, ReplyTo<()>);
    struct Fanout;
    impl Actor for Fanout {
        type Message = Fan;
        type Error = ();
        fn on_message(&mut self, Fan(to, done): Fan, _: &mut Context<'_, Self>) -> Result<(), ()> {
            for (i, a) in to.iter().enumerate() {
                a.try_tell(Msg::From(0, i as u32)).unwrap();
            }
            let _ = done.reply(());
            Ok(())
        }
    }
    // One worker, so every wake lands where the handler runs, and only one of them can run next.
    let rt = runtime(1);
    let spawn =
        |actor| rt.spawn(actor, Execution::Shared, MailboxLimits::default()).unwrap().wait_ready(SECOND).unwrap();
    let targets: Vec<_> = (0..4).map(|_| spawn(Collector(vec![vec![]]))).collect();
    let fanout = rt.spawn(Fanout, Execution::Shared, MailboxLimits::default()).unwrap().wait_ready(SECOND).unwrap();
    let to = targets.iter().map(|t| t.address.clone()).collect();
    // Once it has answered, what it sent is ahead of the requests below.
    block_on(fanout.address.request(Some(rt.clock().now() + SECOND), |done| Fan(to, done))).unwrap();
    for (i, t) in targets.iter().enumerate() {
        let seen = block_on(t.address.request(Some(rt.clock().now() + SECOND), Msg::Snapshot));
        assert_eq!(seen.unwrap(), [[i as u32]], "target {i}");
    }
    assert_eq!(rt.shutdown(SECOND).exits.len(), 5);
}

#[test]
fn two_actors_that_keep_waking_each_other_do_not_starve_a_third() {
    enum Rally {
        Peer(ActorRef<Rally>),
        Ball,
    }
    struct Player(Option<ActorRef<Rally>>);
    impl Actor for Player {
        type Message = Rally;
        type Error = ();
        fn on_message(&mut self, msg: Rally, _: &mut Context<'_, Self>) -> Result<(), ()> {
            match msg {
                Rally::Peer(p) => self.0 = Some(p),
                Rally::Ball => drop(self.0.as_ref().map(|p| p.try_tell(Rally::Ball))),
            }
            Ok(())
        }
    }
    let rt = runtime(1);
    let spawn = || rt.spawn(Player(None), Execution::Shared, MailboxLimits::default()).unwrap();
    let (a, b) = (spawn().wait_ready(SECOND).unwrap(), spawn().wait_ready(SECOND).unwrap());
    let third = rt
        .spawn(Collector(vec![vec![]]), Execution::Shared, MailboxLimits::default())
        .unwrap()
        .wait_ready(SECOND)
        .unwrap();
    a.address.try_tell(Rally::Peer(b.address.clone())).unwrap();
    b.address.try_tell(Rally::Peer(a.address.clone())).unwrap();
    a.address.try_tell(Rally::Ball).unwrap();
    for i in 0..100 {
        third.address.try_tell(Msg::From(0, i)).unwrap();
    }
    let seen = block_on(third.address.request(Some(rt.clock().now() + SECOND), Msg::Snapshot));
    assert_eq!(seen.unwrap(), [(0..100).collect::<Vec<_>>()]);
    assert_eq!(rt.shutdown(SECOND).exits.len(), 3);
}

fn pool(name: &str, workers: usize, balance: bool) -> PoolConfig {
    PoolConfig { name: name.into(), workers, balance, ..Default::default() }
}

#[test]
fn a_pool_without_balance_keeps_each_actor_on_the_worker_it_was_placed_on() {
    enum Ring {
        Next(ActorRef<Ring>),
        Token(u32),
        Threads(ReplyTo<Vec<String>>),
    }
    struct Hop(Option<ActorRef<Ring>>, Vec<String>, Arc<AtomicU64>);
    impl Actor for Hop {
        type Message = Ring;
        type Error = ();
        fn on_message(&mut self, msg: Ring, _: &mut Context<'_, Self>) -> Result<(), ()> {
            let thread = std::thread::current().name().unwrap_or_default().to_string();
            if !self.1.contains(&thread) {
                self.1.push(thread);
            }
            match msg {
                Ring::Next(next) => self.0 = Some(next),
                Ring::Token(0) => _ = self.2.fetch_add(1, Ordering::Release),
                Ring::Token(n) => self.0.as_ref().unwrap().try_tell(Ring::Token(n - 1)).map(drop).unwrap(),
                Ring::Threads(reply) => drop(reply.reply(self.1.clone())),
            }
            Ok(())
        }
    }
    for balance in [false, true] {
        let config = RuntimeConfig { workers: 1, pools: vec![pool("ring", 2, balance)], ..Default::default() };
        let rt = Runtime::new(config).unwrap();
        let done = Arc::new(AtomicU64::new(0));
        let hops: Vec<_> = (0..4)
            .map(|_| rt.spawn(Hop(None, Vec::new(), done.clone()), Execution::Pool("ring"), MailboxLimits::default()))
            .map(|h| h.unwrap().wait_ready(SECOND).unwrap())
            .collect();
        for (i, hop) in hops.iter().enumerate() {
            hop.address.try_tell(Ring::Next(hops[(i + 1) % 4].address.clone())).map(drop).unwrap();
        }
        hops[0].address.try_tell(Ring::Token(400)).map(drop).unwrap();
        eventually("the token goes round", || done.load(Ordering::Acquire) == 1);
        let threads: Vec<_> = (hops.iter())
            .map(|h| block_on(h.address.request(Some(rt.clock().now() + SECOND), Ring::Threads)).unwrap())
            .collect();
        // Placed round robin.
        let home = |i: usize| format!("ring-{}", i % 2);
        if balance {
            // A hop woken by the one before it runs next on that one's worker, so one of the two is away from home.
            assert!(threads.iter().enumerate().any(|(i, t)| t.iter().any(|t| *t != home(i))), "{threads:?}");
        } else {
            assert_eq!(threads, (0..4).map(|i| vec![home(i)]).collect::<Vec<_>>());
        }
        let stats = rt.stats();
        assert_eq!((stats.pools.len(), stats.pools[0].name.as_str(), stats.pools[0].workers.len()), (1, "ring", 2));
        assert_eq!(rt.shutdown(SECOND).exits.len(), 4);
    }
}

#[test]
fn a_turn_that_blocks_holds_up_the_actors_placed_with_it_only_without_balance() {
    // A sleeping worker looks for work only with balance; a spinning one looks all the time.
    for (balance, spin) in [false, true].into_iter().flat_map(|b| [(b, PoolConfig::default().spin), (b, Duration::MAX)])
    {
        let p = PoolConfig { spin, ..pool("p", 2, balance) };
        let rt = Runtime::new(RuntimeConfig { workers: 1, pools: vec![p], ..Default::default() }).unwrap();
        // Placed round robin: the first and third on worker 0.
        let actors: Vec<_> = (0..3)
            .map(|_| rt.spawn(Collector(vec![vec![]]), Execution::Pool("p"), MailboxLimits::default()).unwrap())
            .map(|a| a.wait_ready(SECOND).unwrap())
            .collect();
        std::thread::sleep(Duration::from_millis(20));
        actors[0].address.try_tell(Msg::Block(Duration::from_millis(300))).unwrap();
        let began = std::time::Instant::now();
        actors[2].address.try_tell(Msg::From(0, 7)).unwrap();
        let seen = block_on(actors[2].address.request(Some(rt.clock().now() + SECOND), Msg::Snapshot));
        assert_eq!(seen.unwrap(), [[7]]);
        let waited = began.elapsed();
        let held = waited >= Duration::from_millis(250);
        assert_eq!(held, !balance, "balance {balance}, spin {spin:?}: {waited:?}");
        assert_eq!(rt.shutdown(SECOND).exits.len(), 3);
    }
}

#[test]
fn a_pool_that_never_sleeps_keeps_answering() {
    // The pool under test: its worker never sleeps.
    let never_sleeps = PoolConfig { spin: Duration::MAX, ..pool("hot", 1, true) };
    let rt = Runtime::new(RuntimeConfig { workers: 1, pools: vec![never_sleeps], ..Default::default() }).unwrap();
    let actor = rt.spawn(Collector(vec![vec![]]), Execution::Pool("hot"), MailboxLimits::default()).unwrap();
    let actor = actor.wait_ready(SECOND).unwrap();
    // Messages that land while the worker spins are what lengthen its next spin.
    for i in 0..20 {
        actor.address.try_tell(Msg::From(0, i)).unwrap();
        std::thread::sleep(Duration::from_micros(200));
    }
    let seen = block_on(actor.address.request(Some(rt.clock().now() + SECOND), Msg::Snapshot));
    assert_eq!(seen.unwrap(), [(0..20).collect::<Vec<_>>()]);
    assert_eq!(rt.shutdown(SECOND).exits.len(), 1);
}

#[test]
fn a_pool_takes_its_own_turn_budget() {
    struct Batches(usize);
    enum Load {
        One,
        Block(Arc<Barrier>),
        Largest(ReplyTo<usize>),
    }
    impl Actor for Batches {
        type Message = Load;
        type Error = ();
        fn on_message(&mut self, msg: Load, _: &mut Context<'_, Self>) -> Result<(), ()> {
            match msg {
                Load::One => {}
                Load::Block(b) => {
                    b.wait();
                    b.wait();
                }
                Load::Largest(reply) => drop(reply.reply(self.0)),
            }
            Ok(())
        }
        fn on_batch(&mut self, batch: &mut MailboxBatch<'_, Load>, cx: &mut Context<'_, Self>) -> Result<(), ()> {
            let mut n = 0;
            for msg in batch.by_ref() {
                // Counted as it goes: the request for the count may be in the same batch.
                if matches!(msg, Load::One) {
                    n += 1;
                    self.0 = self.0.max(n);
                }
                self.on_message(msg, cx)?;
            }
            Ok(())
        }
    }
    let large = PoolConfig { turn_budget: TurnBudget::messages(1000), ..pool("large", 1, true) };
    let rt = Runtime::new(RuntimeConfig { workers: 1, pools: vec![large], ..Default::default() }).unwrap();
    let limits = MailboxLimits { messages: 1024, ..Default::default() };
    // The actor is held inside Block until all 500 are queued, so they are one batch unless the budget splits them.
    for (execution, largest) in [(Execution::Shared, 64), (Execution::Pool("large"), 500)] {
        let actor = rt.spawn(Batches(0), execution, limits).unwrap().wait_ready(SECOND).unwrap();
        let held = Arc::new(Barrier::new(2));
        actor.address.try_tell(Load::Block(held.clone())).map(drop).unwrap();
        held.wait();
        (0..500).for_each(|_| actor.address.try_tell(Load::One).map(drop).unwrap());
        held.wait();
        let seen = block_on(actor.address.request(Some(rt.clock().now() + SECOND), Load::Largest)).unwrap();
        assert_eq!(seen, largest, "{execution:?}");
    }
    assert_eq!(rt.shutdown(SECOND).exits.len(), 2);
}

#[test]
fn pools_are_checked_when_the_runtime_is_built_and_when_an_actor_is_placed() {
    let invalid = |pools: Vec<PoolConfig>| {
        matches!(
            Runtime::new(RuntimeConfig { workers: 1, pools, ..Default::default() }),
            Err(SpawnError::InvalidPool(_))
        )
    };
    assert!(invalid(vec![pool("", 1, true)]), "a pool needs a name");
    assert!(invalid(vec![pool("a", 1, true), pool("a", 1, true)]), "names are distinct");
    assert!(invalid(vec![pool("a\0", 1, true)]), "a thread name cannot hold a NUL");
    assert!(invalid(vec![PoolConfig { cores: vec![1 << 20], ..pool("a", 1, true) }]), "no such core");
    // Under `taskset` or a container's cpuset these need not start at core 0.
    let cores = RuntimeConfig::allowed_cores();
    assert!(!cores.is_empty(), "the OS says which cores this thread may run on");
    let pools = vec![PoolConfig { cores: cores.clone(), ..pool("a", 1, true) }];
    let rt = Runtime::new(RuntimeConfig { workers: 1, cores, pools, ..Default::default() })
        .expect("a pool may pin to any core this thread may run on");
    let placed = rt.spawn(Collector(vec![vec![]]), Execution::Pool("missing"), MailboxLimits::default());
    assert!(matches!(placed, Err(SpawnError::InvalidPool(_))));
    assert_eq!(rt.actor_count(), 0);
    rt.shutdown(SECOND);
    // The list is the calling thread's own, not numbered from 0: narrowed to its last core, the first is refused.
    #[cfg(target_os = "linux")]
    if let [first, .., last] = RuntimeConfig::allowed_cores()[..] {
        std::thread::spawn(move || {
            assert!(core_affinity::set_for_current(core_affinity::CoreId { id: last }));
            assert_eq!(RuntimeConfig::allowed_cores(), vec![last]);
            let narrowed = Runtime::new(RuntimeConfig { workers: 1, cores: vec![first], ..Default::default() });
            assert!(matches!(narrowed, Err(SpawnError::InvalidPool(_))));
        })
        .join()
        .expect("a thread narrowed to one core may pin only to it");
    }
}

#[test]
fn shutdown_is_honest_about_handlers_that_do_not_finish() {
    let rt = runtime(1);
    let actor =
        rt.spawn(Collector(vec![]), Execution::Shared, MailboxLimits::default()).unwrap().wait_ready(SECOND).unwrap();
    let ((began, holding), (release, held)) = (std::sync::mpsc::sync_channel(1), std::sync::mpsc::sync_channel(1));
    actor.address.try_tell(Msg::Hold(began, held)).unwrap();
    holding.recv_timeout(SECOND).expect("the handler begins");
    actor.lifecycle.stop();
    assert_eq!(actor.lifecycle.wait(Duration::from_millis(1)), None);
    let report = rt.shutdown(Duration::from_millis(10));
    assert_eq!((report.exits.len(), report.still_running.len()), (0, 1));
    release.send(()).unwrap();
    assert_eq!(actor.lifecycle.wait(SECOND).unwrap().reason, StopReason::Stopped);
    assert!(matches!(
        rt.spawn(Collector(vec![]), Execution::Shared, MailboxLimits::default()),
        Err(SpawnError::RuntimeStopped)
    ));
}

#[test]
fn dropping_the_runtime_stops_actors_and_limits_are_enforced() {
    let rt = Runtime::new(RuntimeConfig { workers: 1, max_actors: 1, ..Default::default() }).unwrap();
    let zero = MailboxLimits { messages: 0, ..Default::default() };
    assert!(matches!(rt.spawn(Collector(vec![]), Execution::Shared, zero), Err(SpawnError::InvalidLimits(_))));
    let family = rt.spawn_supervised(|| Collector(vec![]), Execution::Shared, zero, RestartPolicy::default());
    assert!(matches!(family, Err(SpawnError::InvalidLimits(_))));
    let actor = rt.spawn(Collector(vec![]), Execution::Shared, MailboxLimits::default()).unwrap();
    assert!(matches!(
        rt.spawn(Collector(vec![]), Execution::Shared, MailboxLimits::default()),
        Err(SpawnError::ActorLimit)
    ));
    drop(rt);
    assert_eq!(actor.lifecycle.wait(SECOND).unwrap().reason, StopReason::RuntimeShutdown);
}

#[test]
fn a_runtime_built_directly_runs_jobs_only_on_the_executor_it_is_given() {
    struct Io(std::sync::mpsc::SyncSender<Result<u32, TaskAdmissionError>>);
    impl Actor for Io {
        type Message = u32;
        type Error = ();
        fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
            if let Err(e) = cx.submit_io(|| 7u32) {
                let _ = self.0.send(Err(e.reason));
            }
            Ok(())
        }
        fn on_message(&mut self, v: u32, _: &mut Context<'_, Self>) -> Result<(), ()> {
            let _ = self.0.send(Ok(v));
            Ok(())
        }
    }
    for (executor, outcome) in
        [(None, Err(TaskAdmissionError::NoExecutor)), (Some(JobExecutor::thread_pool(1, 1).unwrap()), Ok(7))]
    {
        let rt = Runtime::new(RuntimeConfig { workers: 1, job_executor: executor, ..Default::default() }).unwrap();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        rt.spawn(Io(tx), Execution::Shared, MailboxLimits::default()).unwrap();
        assert_eq!(rx.recv_timeout(SECOND), Ok(outcome));
        rt.shutdown(SECOND);
    }
}

/// A job thread polls with no tokio runtime entered: `OsFiles` works in place there, so one job reads and writes.
#[test]
fn a_job_on_a_thread_pool_executor_reads_then_writes_os_files() {
    use axon::files::{FileOp, Files, OsFiles};
    struct Store(Arc<OsFiles>, std::sync::mpsc::SyncSender<Result<usize, String>>);
    impl Actor for Store {
        type Message = Result<usize, String>;
        type Error = String;
        fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
            let (read, files) = (self.0.read("log"), self.0.clone());
            let job = cx.submit_future(async move {
                let had = read.await.map_err(|e| e.to_string())?;
                files.apply(vec![FileOp::Append("log".into(), "!".into())]).await.map_err(|e| e.to_string())?;
                Ok(had.len() + 1)
            });
            job.map(drop).map_err(|e| format!("{:?}", e.reason))
        }
        fn on_message(&mut self, written: Result<usize, String>, _: &mut Context<'_, Self>) -> Result<(), String> {
            let _ = self.1.send(written);
            Ok(())
        }
    }
    let dir = std::env::temp_dir().join(format!("axon-hosted-files-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("log"), b"12").unwrap();
    let job_executor = Some(JobExecutor::thread_pool(1, 1).unwrap());
    let rt = Runtime::new(RuntimeConfig { workers: 1, job_executor, ..Default::default() }).unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    rt.spawn(Store(Arc::new(OsFiles::new(&dir)), tx), Execution::Shared, MailboxLimits::default()).unwrap();
    assert_eq!(rx.recv_timeout(SECOND), Ok(Ok(3)));
    rt.shutdown(SECOND);
    assert_eq!(std::fs::read(dir.join("log")).unwrap(), b"12!");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_committed_message_is_delivered_or_rejected_never_lost() {
    struct Count(Arc<AtomicU64>);
    impl Actor for Count {
        type Message = ();
        type Error = ();
        fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let rt = runtime(2);
    for _ in 0..200 {
        let handled = Arc::new(AtomicU64::new(0));
        let actor = rt.spawn(Count(handled.clone()), Execution::Shared, MailboxLimits::default()).unwrap();
        let actor = actor.wait_ready(SECOND).unwrap();
        let permit = block_on(actor.address.reserve(8, None)).unwrap();
        let life = actor.lifecycle.clone();
        let closer = std::thread::spawn(move || life.drain(None));
        let committed = permit.commit(()).is_ok();
        closer.join().unwrap();
        let exit = actor.lifecycle.wait(SECOND).unwrap();
        let delivered = handled.load(Ordering::SeqCst) + exit.discarded_messages() as u64;
        assert_eq!(delivered, committed as u64);
    }
}

#[test]
fn hosted_supervision_restarts_and_publishes_the_new_address() {
    struct Flaky;
    impl Actor for Flaky {
        type Message = bool;
        type Error = &'static str;
        fn on_message(&mut self, fail: bool, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            if fail { Err("flaky") } else { Ok(()) }
        }
    }
    let rt = runtime(2);
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = rt.spawn_supervised(|| Flaky, Execution::Shared, MailboxLimits::default(), policy).unwrap();
    let first = family.wait_for_generation(0, SECOND).unwrap();
    first.try_tell(true).unwrap();
    let second = family.wait_for_generation(1, SECOND).unwrap();
    assert_eq!(second.incarnation().generation, 2);
    assert_eq!(first.try_tell(false).unwrap_err().reason, RejectReason::Closed);
    second.try_tell(false).unwrap();
    family.lifecycle().stop();
    assert_eq!(family.lifecycle().wait(SECOND).unwrap().reason, StopReason::Stopped);
    assert_eq!(family.wait_for_generation(2, SECOND).unwrap_err(), SupervisionWaitError::Stopped);
    eventually_empty(&rt);
}
