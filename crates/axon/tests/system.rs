#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use axon::*;
use parking_lot::Mutex;

mod common;
use common::{eventually, park_on, threadless};

struct Add(u64);

enum Command {
    Add(u64),
    Get(ReplyTo<u64>),
    Fetch(u64),
    Hang(tokio::sync::mpsc::Sender<()>),
    CancelLast,
    Crash,
}

impl From<Add> for Command {
    fn from(a: Add) -> Self {
        Command::Add(a.0)
    }
}

struct Counter(u64);

struct Jobs(u64, Option<JobHandle>);

impl Actor for Counter {
    type Message = Command;
    type Error = &'static str;

    fn on_message(&mut self, msg: Command, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Command::Add(n) => self.0 += n,
            Command::Get(reply) => drop(reply.reply(self.0)),
            Command::Fetch(n) => drop(cx.submit_future(async move {
                tokio::task::yield_now().await;
                Command::Add(n)
            })),
            Command::Crash => return Err("crash"),
            Command::Hang(_) | Command::CancelLast => {}
        }
        Ok(())
    }
}

impl Actor for Jobs {
    type Message = Command;
    type Error = &'static str;

    fn on_message(&mut self, msg: Command, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Command::Add(n) => self.0 += n,
            Command::Get(reply) => drop(reply.reply(self.0)),
            Command::Hang(held) => {
                self.1 = cx
                    .submit_future(async move {
                        std::future::pending::<()>().await;
                        drop(held);
                        Command::Add(0)
                    })
                    .ok()
            }
            Command::CancelLast => self.1.as_ref().expect("a job").cancel(),
            Command::Fetch(_) | Command::Crash => {}
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_job_drops_its_future() {
    let system = ActorSystem::builder().build().unwrap();
    let actor = system.spawn(Jobs(0, None)).start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    actor.send(Command::Hang(tx)).await.unwrap();
    eventually("the job is counted", async || actor.address.diagnostics().jobs == 1).await;
    assert!(tokio::time::timeout(Duration::from_millis(20), rx.recv()).await.is_err(), "still running");
    actor.send(Command::CancelLast).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.is_ok(), "the future was dropped");
    // The future is dropped before its job settles.
    eventually("the job settles", async || actor.address.diagnostics().jobs == 0).await;
    let exit = system.shutdown().await.unwrap();
    assert!(exit.actors.iter().all(|e| e.reason == StopReason::Drained), "{:?}", exit.actors);
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_tell_send_ask_lookup_and_names() {
    let system = ActorSystem::builder().timing_sample_every(1).build().unwrap();
    let counter = system.spawn(Counter(0)).name("orders").start().unwrap();
    counter.tell(Add(10)).unwrap();
    counter.send(Command::Add(32)).await.unwrap();
    assert_eq!(counter.ask(Command::Get).await.unwrap(), 42);

    let found = system.lookup::<Command>("orders").unwrap();
    assert_eq!(found.ask(Command::Get).await.unwrap(), 42);
    assert!(system.lookup::<u8>("orders").is_none());
    assert!(matches!(system.spawn(Counter(0)).name("orders").start(), Err(SystemError::NameTaken(_))));
    assert!(matches!(system.spawn(Counter(0)).name("").start(), Err(SystemError::Configuration(_))));

    counter.tell(Command::Fetch(8)).unwrap();
    eventually("the fetched add lands", async || counter.ask(Command::Get).await.unwrap() == 50).await;

    let page = system.diagnostics_page(None, 10).unwrap();
    assert_eq!((page.total, page.actors.len(), page.next), (1, 1, None));
    let me = &page.actors[0];
    assert_eq!((me.name.as_deref(), me.phase), (Some("orders"), Phase::Running));
    assert!(me.actor_type.ends_with("Counter") && me.diagnostics.timings.is_some());

    counter.lifecycle.stop();
    counter.join().await;
    assert!(system.lookup::<Command>("orders").is_none());
    system.spawn(Counter(0)).name("orders").start().unwrap();
}

/// A start refused its name spawns nothing: its `on_start` never runs, supervised or not. One the runtime refuses
/// leaves the name free.
#[tokio::test(flavor = "multi_thread")]
async fn a_start_refused_its_name_runs_nothing() {
    struct Starts(Arc<AtomicU64>);
    impl Actor for Starts {
        type Message = ();
        type Error = ();
        fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), ()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
            Ok(())
        }
    }
    let system = ActorSystem::builder().build().unwrap();
    let starts = Arc::new(AtomicU64::new(0));
    system.spawn(Starts(starts.clone())).name("once").start().unwrap().ready().await.unwrap();
    for _ in 0..50 {
        let refused = system.spawn(Starts(starts.clone())).name("once").start();
        assert!(matches!(refused, Err(SystemError::NameTaken(_))));
        let starts = starts.clone();
        let refused = system.supervise(move || Starts(starts.clone())).name("once").start_supervised();
        assert!(matches!(refused, Err(SystemError::NameTaken(_))));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(starts.load(Ordering::Relaxed), 1);
    system.shutdown().await.unwrap();

    // A start the runtime refuses gives its name back.
    let full =
        ActorSystem::builder().runtime(RuntimeConfig { max_actors: 1, ..RuntimeConfig::default() }).build().unwrap();
    let only = full.spawn(Starts(starts.clone())).start().unwrap();
    let refused = full.spawn(Starts(starts.clone())).name("later").start();
    assert!(matches!(refused, Err(SystemError::Spawn(SpawnError::ActorLimit))));
    only.lifecycle.stop();
    eventually("the name and the slot are free", async || {
        full.spawn(Starts(starts.clone())).name("later").start().is_ok()
    })
    .await;
    full.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_reports_a_failed_or_late_start() {
    struct Stuck(Duration, bool);
    impl Actor for Stuck {
        type Message = ();
        type Error = &'static str;
        fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            std::thread::sleep(self.0);
            if self.1 { Err("bad") } else { Ok(()) }
        }
        fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            Ok(())
        }
    }
    // The default start timeout: on a loaded box even a start that fails at once can take tens of milliseconds.
    let system = ActorSystem::builder().build().unwrap();
    let failed = system.spawn(Stuck(Duration::ZERO, true)).start().unwrap();
    assert!(matches!(failed.ready().await, Err(SystemError::Start(StartError::Failed(StopReason::StartFailed(_))))));
    system.shutdown().await.unwrap();
    // A start that sleeps past the timeout times out however loaded the box.
    let system = ActorSystem::builder().start_timeout(Duration::from_millis(50)).build().unwrap();
    let late = system.spawn(Stuck(Duration::from_millis(200), false)).start().unwrap();
    assert!(matches!(late.ready().await, Err(SystemError::Start(StartError::TimedOut))));
    // A late start is not cancelled: it finishes on its own.
    eventually("the late start finishes", async || late.ready().await.is_ok()).await;
    assert_eq!(system.diagnostics_page(None, 10).unwrap().total, 1);
}

enum Load {
    Loaded(Result<u64, String>),
    Get(ReplyTo<u64>),
}

/// Loads its balance in `on_start`, from a channel the test answers, and pauses until it has.
struct Loading(Option<tokio::sync::oneshot::Receiver<Result<u64, String>>>, u64);

impl Actor for Loading {
    type Message = Load;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let rx = self.0.take().expect("one start");
        let load = cx.submit_future(async move { Load::Loaded(rx.await.unwrap_or(Err("gone".into()))) });
        cx.pause_until(&load.map_err(|e| format!("{e:?}"))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Load, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Load::Loaded(balance) => self.1 = balance?,
            Load::Get(reply) => drop(reply.reply(self.1)),
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_waits_for_the_load_on_start_pauses_for() {
    let system = ActorSystem::builder().build().unwrap();
    for loaded in [Ok(100), Err("unreadable".to_string())] {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let account = system.spawn(Loading(Some(rx), 0)).start().unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), account.ready()).await.is_err(), "still loading");
        assert_eq!(account.address.phase(), Phase::Starting);
        tx.send(loaded.clone()).unwrap();
        match loaded {
            Ok(balance) => {
                account.ready().await.unwrap();
                assert_eq!(account.ask(Load::Get).await.unwrap(), balance);
            }
            Err(why) => {
                let failed = StopReason::Failed(format!("{why:?}"));
                let ready = account.ready().await;
                assert!(matches!(&ready, Err(SystemError::Start(StartError::Failed(r))) if *r == failed), "{ready:?}");
            }
        }
    }
    system.shutdown().await.unwrap();
}

enum Run {
    /// Answers the name of the thread its job ran on: on the actor's executor, or on the system's.
    Where(bool, ReplyTo<Option<String>>),
    Ran(ReplyTo<Option<String>>, Option<String>),
    /// A job on the actor's executor that never ends.
    Hang,
    /// A job that may panic, its panic caught as its result.
    Try(bool, ReplyTo<Result<u64, Option<String>>>),
    Tried(ReplyTo<Result<u64, Option<String>>>, Result<u64, Option<String>>),
    /// Blocking work whose panics, one a message and one not, are caught as results.
    Blocking(ReplyTo<Vec<Option<String>>>),
    Caught(ReplyTo<Vec<Option<String>>>, Vec<Option<String>>),
    Unwrapped,
}

struct Runner(JobExecutor);

fn uncaught() -> Run {
    panic!("not caught")
}

impl Actor for Runner {
    type Message = Run;
    type Error = String;

    fn on_message(&mut self, msg: Run, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let refused = |reason: TaskAdmissionError| format!("{reason:?}");
        match msg {
            Run::Where(own, reply) => {
                let job = async move { Run::Ran(reply, std::thread::current().name().map(str::to_owned)) };
                let submitted = if own { cx.submit_future_on(&self.0, job) } else { cx.submit_future(job) };
                submitted.map_err(|e| refused(e.reason))?;
            }
            Run::Hang => drop(cx.submit_future_on(&self.0, std::future::pending()).map_err(|e| refused(e.reason))?),
            Run::Try(panics, reply) => {
                let run = axon::catch_panic(async move {
                    assert!(!panics, "boom");
                    7
                });
                let job = async move { Run::Tried(reply, run.await.map_err(|p| p.message().map(str::to_owned))) };
                cx.submit_future_on(&self.0, job).map_err(|e| refused(e.reason))?;
            }
            Run::Blocking(reply) => {
                let job = move || {
                    let message = |work: fn() -> u64| {
                        let panic = std::panic::catch_unwind(work).map_err(Panic::from).expect_err("it panics");
                        panic.message().map(str::to_owned)
                    };
                    Run::Caught(
                        reply,
                        vec![message(|| panic!("blocking {}", "boom")), message(|| std::panic::panic_any(5u8))],
                    )
                };
                cx.submit_io(job).map_err(|e| refused(e.reason))?;
            }
            Run::Ran(reply, name) => drop(reply.reply(name)),
            Run::Caught(reply, messages) => drop(reply.reply(messages)),
            Run::Tried(reply, ran) => drop(reply.reply(ran)),
            Run::Unwrapped => drop(cx.submit_future(async { uncaught() }).map_err(|e| refused(e.reason))?),
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn jobs_on_a_runtime_of_their_own_run_there_and_the_rest_on_the_systems() {
    let js =
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("js").enable_all().build().unwrap();
    let system = ActorSystem::builder().build().unwrap();
    let runner = system.spawn(Runner(JobExecutor::tokio(js.handle().clone()))).start().unwrap();
    assert_eq!(runner.ask(|r| Run::Where(true, r)).await.unwrap().as_deref(), Some("js"));
    assert_ne!(runner.ask(|r| Run::Where(false, r)).await.unwrap().as_deref(), Some("js"));
    let theirs = system.spawn(Runner(system.job_executor())).start().unwrap();
    assert_ne!(theirs.ask(|r| Run::Where(true, r)).await.unwrap().as_deref(), Some("js"), "the system's own");

    // The application's runtime goes first, with a job still on it: the actor ends, and nothing waits for the job.
    runner.tell(Run::Hang).unwrap();
    eventually("the job is counted", async || runner.address.diagnostics().jobs == 1).await;
    js.shutdown_background();
    let exit = tokio::time::timeout(Duration::from_secs(5), runner.join()).await.expect("the actor ends");
    assert_eq!((exit.reason, exit.pending_jobs), (StopReason::Failed("a job was dropped by its executor".into()), 0));
    assert_eq!(runner.address.diagnostics().jobs, 0);
    tokio::time::timeout(Duration::from_secs(10), system.shutdown()).await.expect("no hang").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn catch_panic_makes_a_jobs_panic_its_result() {
    let system = ActorSystem::builder().build().unwrap();
    let runner = system.spawn(Runner(system.job_executor())).start().unwrap();
    assert_eq!(runner.ask(|r| Run::Try(true, r)).await.unwrap(), Err(Some("boom".into())));
    assert_eq!(runner.ask(|r| Run::Try(false, r)).await.unwrap(), Ok(7));
    assert_eq!(runner.ask(Run::Blocking).await.unwrap(), [Some("blocking boom".into()), None]);
    assert_eq!(runner.address.phase(), Phase::Running);
    // A job left to unwind ends its actor, as a handler's panic does.
    runner.tell(Run::Unwrapped).unwrap();
    assert_eq!(runner.join().await.reason, StopReason::Panicked);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_sent_before_on_start_wait_for_it() {
    struct Slow(u64);
    impl Actor for Slow {
        type Message = Add;
        type Error = &'static str;
        fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            std::thread::sleep(Duration::from_millis(20));
            self.0 = 100;
            Ok(())
        }
        fn on_message(&mut self, Add(n): Add, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            self.0 += n;
            if self.0 == 108 { Err("done") } else { Ok(()) }
        }
    }
    let system = ActorSystem::builder().build().unwrap();
    let slow = system.spawn(Slow(0)).start().unwrap();
    for _ in 0..8 {
        slow.tell(Add(1)).unwrap();
    }
    // `on_start` ran first: the eighth message sees the count it set.
    assert_eq!(slow.join().await.reason, StopReason::Failed("\"done\"".into()));
    // The turn that ended the actor is counted in what its exit retired.
    assert_eq!(system.stats().retired[std::any::type_name::<Slow>()].processed, 8);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_start_dead_letters_what_was_queued() {
    struct Bad;
    impl Actor for Bad {
        type Message = Add;
        type Error = &'static str;
        fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            std::thread::sleep(Duration::from_millis(20));
            Err("no")
        }
        fn on_message(&mut self, _: Add, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
            Ok(())
        }
    }
    let system = ActorSystem::builder().build().unwrap();
    let bad = system.spawn(Bad).start().unwrap();
    for _ in 0..3 {
        bad.tell(Add(1)).unwrap();
    }
    let exit = bad.join().await;
    assert_eq!((&exit.reason, exit.discarded_messages()), (&StopReason::StartFailed("\"no\"".into()), 3));
    let letters = system.dead_letters().recent(10);
    assert_eq!(letters.iter().map(|l| (l.reason, l.count)).collect::<Vec<_>>(), [("actor_failed", 3)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn supervised_handles_follow_the_current_generation() {
    let system = ActorSystem::builder().timing_sample_every(1).build().unwrap();
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(|| Counter(100)).name("family").policy(policy).start_supervised().unwrap();
    family.tell(Add(1)).unwrap();
    assert_eq!(family.ask(Command::Get).await.unwrap(), 101);
    let processed = || system.diagnostics_page(None, 10).unwrap().actors[0].diagnostics.processed;
    let before = processed();
    family.tell(Command::Crash).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(family.ask(Command::Get).await.unwrap(), 100);
    assert_eq!(system.diagnostics_page(None, 10).unwrap().actors[0].restarts, 1);
    assert!(processed() > before, "the family's counters add up its generations: {before} then {}", processed());
    let d = system.diagnostics_page(None, 10).unwrap().actors.remove(0).diagnostics;
    // Generation 0 handled at least Add and Get before its Crash; the asks are counted as they are made.
    assert_eq!((d.asks, d.timings.is_some_and(|t| t.handler.count >= 2)), (2, true));
    let found = system.lookup_supervised::<Command>("family").unwrap();
    assert_eq!(found.ask(Command::Get).await.unwrap(), 100);
    assert!(system.lookup_supervised::<u64>("family").is_none() && system.lookup::<Command>("family").is_none());

    // It ends at generation 1 and still gives back what it holds under generation 0.
    family.lifecycle.stop();
    family.join().await;
    assert!(system.lookup_supervised::<Command>("family").is_none());
    assert_eq!((system.stats().stopped, system.diagnostics_page(None, 10).unwrap().total), (1, 0));
    let stats = system.stats();
    let retired = &stats.retired[std::any::type_name::<Counter>()];
    assert!(retired.processed > before + 1, "and so do its retired");
    let timed = retired.timings.as_ref().is_some_and(|t| t.handler.count >= 4);
    assert_eq!((retired.asks, retired.turns >= 2, timed), (3, true, true));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_moves_since_restart_but_not_up() {
    let system = ActorSystem::builder().build().unwrap();
    let before = trace::unix_micros();
    let plain = system.spawn(Counter(0)).start().unwrap();
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(|| Counter(0)).policy(policy).start_supervised().unwrap();
    family.ask(Command::Get).await.unwrap();
    let started = |i: usize| {
        let a = &system.diagnostics_page(None, 10).unwrap().actors[i];
        (a.started_us, a.restarted_us)
    };
    let ((up, since), (plain_up, plain_since)) = (started(1), started(0));
    let now = trace::unix_micros();
    assert!([up, plain_up].iter().all(|t| (before..=now).contains(t)) && since.is_none() && plain_since.is_none());

    tokio::time::sleep(Duration::from_millis(2)).await;
    let crashed = trace::unix_micros();
    family.tell(Command::Crash).unwrap();
    eventually("the family restarts", || async { system.diagnostics_page(None, 10).unwrap().actors[1].restarts == 1 })
        .await;
    let (after_up, after_since) = started(1);
    assert_eq!(after_up, up);
    assert!(after_since.is_some_and(|since| since > crashed), "{after_since:?} {crashed}");
    plain.lifecycle.stop();
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn supervised_send_and_ask_wait_out_a_restart() {
    let system = ActorSystem::builder().build().unwrap();
    // A restart that takes 100 ms exactly, for the send and the ask to wait out.
    let backoff = Backoff { initial: Duration::from_millis(100), jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(|| Counter(100)).policy(policy).start_supervised().unwrap();
    let backoff = async || {
        family.tell(Command::Crash).unwrap();
        eventually("the generation ends", async || family.family.current().is_none()).await;
    };
    backoff().await;
    assert!(matches!(family.tell(Add(1)), Err(Reject { reason: RejectReason::NotReady, .. })));
    family.send(Add(1)).await.unwrap();
    assert_eq!(family.ask(Command::Get).await.unwrap(), 101);

    // A stop during the backoff ends a waiting send at once, not at its timeout.
    backoff().await;
    let waiting = tokio::spawn({
        let family = family.clone();
        async move { family.send(Add(1)).await.map_err(|r| r.reason) }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    family.lifecycle.stop();
    let sent = tokio::time::timeout(Duration::from_secs(1), waiting).await.expect("the send ended with the family");
    assert_eq!((sent.unwrap(), family.join().await.reason), (Err(RejectReason::Closed), StopReason::Stopped));

    // A drain refuses a send while it runs, as an unsupervised actor's does.
    let sleepy = system.supervise(Sleepy::default).start_supervised().unwrap();
    sleepy.tell(300u64).unwrap();
    sleepy.lifecycle.drain(None);
    let refused = sleepy.send(0u64).await.map_err(|r| r.reason);
    assert_eq!((refused, sleepy.lifecycle.exit()), (Err(RejectReason::Closed), None));
    assert_eq!(sleepy.join().await.reason, StopReason::Drained);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_supervised_ask_goes_to_the_next_generation_only_if_untaken() {
    enum Nap {
        FailAfter(u64),
        Generation(ReplyTo<u64>),
    }
    struct Napper;
    impl Actor for Napper {
        type Message = Nap;
        type Error = ();
        fn on_message(&mut self, msg: Nap, cx: &mut Context<'_, Self>) -> Result<(), ()> {
            match msg {
                Nap::FailAfter(ms) => {
                    std::thread::sleep(Duration::from_millis(ms));
                    return Err(());
                }
                Nap::Generation(reply) => drop(reply.reply(cx.incarnation().generation)),
            }
            Ok(())
        }
    }
    let system = ActorSystem::builder().build().unwrap();
    let limits = MailboxLimits { messages: 1, ..Default::default() };
    let family = system.supervise(|| Napper).mailbox(limits).start_supervised().unwrap();
    // The first generation naps and fails with its one slot held: the ask waits for room and is closed out untaken.
    family.tell(Nap::FailAfter(200)).unwrap();
    assert_eq!(family.ask(Nap::Generation).await, Ok(2));
    // With room, the failing generation takes the ask, and it is not sent again.
    let roomy = system.supervise(|| Napper).start_supervised().unwrap();
    roomy.tell(Nap::FailAfter(200)).unwrap();
    assert_eq!(roomy.ask(Nap::Generation).await, Err(RequestError::ReplyDropped { admitted: true }));
    system.shutdown().await.unwrap();
}

struct Probe {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    panic_on_shutdown: bool,
}

impl SystemExtension for Probe {
    fn name(&self) -> &str {
        self.name
    }

    fn leaving(&self, reason: &LeaveReason, _: Duration) -> ExtensionFuture {
        self.log.lock().push(format!("{} leaving {reason:?}", self.name));
        Box::pin(async { Err("not reachable".to_string()) })
    }

    fn shutdown(&self, _: Duration) -> ExtensionFuture {
        self.log.lock().push(format!("{} shutdown", self.name));
        if self.panic_on_shutdown {
            panic!("extension panic");
        }
        Box::pin(async { Ok(()) })
    }

    fn close(&self) {
        self.log.lock().push(format!("{} close", self.name));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_orders_extensions_and_shares_one_report() {
    let system = ActorSystem::builder().build().unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    for (name, panic_on_shutdown) in [("a", false), ("b", true)] {
        system.attach_extension(Arc::new(Probe { name, log: log.clone(), panic_on_shutdown })).unwrap();
    }
    let counter = system.spawn(Counter(0)).start().unwrap();
    (0..100).for_each(|_| drop(counter.tell(Add(1))));

    let bad = LeaveReason::Signal("x".repeat(33));
    assert!(matches!(system.shutdown_with_reason(bad).await, Err(SystemError::Configuration(_))));

    let (first, second) = tokio::join!(system.shutdown_with_reason(LeaveReason::RollingRestart), system.shutdown());
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.leave, LeaveReason::RollingRestart);
    assert_eq!(second.leave, first.leave);
    assert_eq!(system.leave_reason(), Some(first.leave.clone()));
    assert_eq!((first.actors.len(), first.still_running.len()), (1, 0));
    assert_eq!((first.actors[0].reason.clone(), first.actors[0].discarded_messages()), (StopReason::Drained, 0));
    assert_eq!((first.warnings.len(), first.failures.len()), (2, 1));
    assert_eq!(
        *log.lock(),
        ["a leaving RollingRestart", "b leaving RollingRestart", "b shutdown", "b close", "a shutdown", "a close"]
    );

    assert!(matches!(system.spawn(Counter(0)).start(), Err(SystemError::Closed)));
    assert!(matches!(
        system.attach_extension(Arc::new(Probe { name: "c", log, panic_on_shutdown: false })),
        Err(SystemError::Closed)
    ));
    assert_eq!(counter.tell(Add(1)).unwrap_err().reason, RejectReason::Closed);
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_system_stops_actors_without_draining() {
    let system = ActorSystem::builder().build().unwrap();
    let weak = system.downgrade();
    let counter = system.spawn(Counter(0)).start().unwrap();
    let jobs = system.spawn(Jobs(0, None)).start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    jobs.send(Command::Hang(tx)).await.unwrap();
    eventually("the job runs", async || jobs.address.diagnostics().jobs == 1).await;
    drop(system);
    assert_eq!(counter.join().await.reason, StopReason::RuntimeShutdown);
    assert!(weak.upgrade().is_none());
    let dropped = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
    assert!(dropped.is_ok(), "a job's future ends with the system");
}

#[test]
fn building_without_tokio_is_an_error() {
    assert!(matches!(ActorSystem::builder().build(), Err(SystemError::NoExecutor)));
}

/// Says when each nap begins: a stop that lands before then discards the message instead.
#[derive(Default)]
struct Sleepy(Arc<tokio::sync::Notify>);

impl Actor for Sleepy {
    type Message = u64;
    type Error = ();

    fn on_message(&mut self, ms: u64, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.notify_one();
        std::thread::sleep(Duration::from_millis(ms));
        if ms == 99 { Err(()) } else { Ok(()) }
    }

    fn label(ms: &u64) -> &'static str {
        match ms {
            0 => "quick",
            7 => panic!("a label that panics"),
            _ => "nap",
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_label_that_panics_as_its_message_is_discarded_still_leaves_a_dead_letter() {
    let system = ActorSystem::builder().build().unwrap();
    let began = Arc::new(tokio::sync::Notify::new());
    let actor = system.spawn(Sleepy(began.clone())).name("sleepy").start().unwrap();
    actor.tell(200u64).unwrap();
    tokio::time::timeout(Duration::from_secs(5), began.notified()).await.expect("the nap begins");
    actor.tell(7u64).unwrap();
    actor.tell(0u64).unwrap();
    actor.lifecycle.stop();
    assert_eq!(actor.join().await.discarded_messages(), 2);
    eventually("the letters are recorded", async || system.stats().dead_letters == 2).await;
    let letters = system.dead_letters().recent(10);
    let seen: Vec<_> = letters.iter().map(|l| (l.label, l.count, l.reason)).collect();
    assert_eq!(seen, [("quick", 1, "actor_stopped"), ("", 1, "actor_stopped")], "only its own is unlabelled");

    let next = system.spawn(Counter(0)).start().unwrap();
    assert_eq!(next.ask(Command::Get).await.unwrap(), 0, "the system goes on");
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_discarded_at_exit_become_dead_letters() {
    let system = ActorSystem::builder().build().unwrap();
    let began = Arc::new(tokio::sync::Notify::new());
    let actor = system.spawn(Sleepy(began.clone())).name("sleepy").start().unwrap();
    actor.tell(200u64).unwrap();
    tokio::time::timeout(Duration::from_secs(5), began.notified()).await.expect("the nap begins");
    let trace = TraceContext::root();
    trace.enter(|| actor.tell(0u64).unwrap());
    actor.tell(0u64).unwrap();
    actor.tell(1u64).unwrap();
    actor.lifecycle.stop();
    assert_eq!(actor.join().await.discarded_messages(), 3);
    eventually("the letters are recorded", async || system.stats().dead_letters > 0).await;

    let mut letters = system.dead_letters().recent(10);
    letters.sort_by_key(|l| l.label);
    let seen: Vec<_> = letters.iter().map(|l| (l.label, l.count, l.reason, l.target.as_deref(), l.trace)).collect();
    assert_eq!(
        seen,
        [
            ("nap", 1, "actor_stopped", Some("sleepy"), 0),
            ("quick", 2, "actor_stopped", Some("sleepy"), trace.trace.get())
        ]
    );
    assert_eq!(system.stats().dead_letters, 3);
    // No handler ran the traced message, so the drop is its trace's span: without it the trace would not exist.
    let spans = system.tracer().trace(trace.trace.get());
    let seen: Vec<_> =
        spans.iter().map(|s| (&*s.name, s.kind, s.parent, s.failed, s.busy, s.actor.map(|a| a.0))).collect();
    assert_eq!(seen, [("quick", SpanKind::Handle, 0, true, Duration::ZERO, Some(actor.address.incarnation()))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generation_that_fails_and_restarts_leaves_its_queue_as_dead_letters() {
    let system = ActorSystem::builder().build().unwrap();
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(10), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..RestartPolicy::default() };
    let family = system.supervise(Sleepy::default).name("sleepy").policy(policy).start_supervised().unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    family.tell(99u64).unwrap();
    let trace = TraceContext::root();
    trace.enter(|| family.tell(0u64)).unwrap();
    family.tell(0u64).unwrap();
    eventually("the letters are recorded", async || system.stats().dead_letters > 0).await;

    let letters = system.dead_letters().recent(10);
    let seen: Vec<_> = letters.iter().map(|l| (l.label, l.count, l.reason, l.target.as_deref())).collect();
    assert_eq!(seen, [("quick", 2, "actor_failed", Some("sleepy"))]);
    assert_eq!(letters[0].actor.unwrap().0.generation, 1);
    let spans = system.tracer().trace(trace.trace.get());
    let ended: Vec<_> = spans.iter().map(|s| (&*s.name, s.failed, s.actor.map(|a| a.0.generation))).collect();
    assert_eq!(ended, [("quick", true, Some(1))], "the failed generation's queue ends its traces");
    let lost = || system.diagnostics_page(None, 10).unwrap().actors[0].diagnostics.dead_letters;
    assert_eq!(lost(), 2, "the family counts its generations' dead letters");
    // The ring lets go of them; the count does not.
    for i in 0..2000 {
        let other = DeadLetter {
            seq: 0,
            at_us: 0,
            reason: "full",
            label: "other",
            actor: None,
            target: Some(format!("elsewhere-{i}").into()),
            peer: None,
            trace: 0,
            count: 1,
        };
        system.dead_letters().record(other, None);
    }
    assert!(system.dead_letters().recent(usize::MAX).iter().all(|l| l.actor.is_none()));
    assert_eq!(lost(), 2);

    // The family lives on, and its own exit does not count that generation again.
    tokio::time::sleep(Duration::from_millis(60)).await;
    family.lifecycle.stop();
    family.join().await;
    assert_eq!(system.stats().dead_letters, 2);
    assert_eq!(system.stats().retired[std::any::type_name::<Sleepy>()].dead_letters, 2);
}

/// Stashes everything but a crash.
struct Hoarder;

impl Actor for Hoarder {
    type Message = Command;
    type Error = &'static str;

    fn on_message(&mut self, msg: Command, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Command::Crash => Err("crash"),
            held => cx.stash(held).map_err(|_| "stash full"),
        }
    }

    fn label(msg: &Command) -> &'static str {
        if matches!(msg, Command::Get(_)) { "get" } else { "held" }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generation_that_fails_leaves_its_stash_as_dead_letters_and_its_stashed_ask_answered() {
    let system = ActorSystem::builder().build().unwrap();
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(10), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..RestartPolicy::default() };
    let family = system.supervise(|| Hoarder).name("hoarder").policy(policy).start_supervised().unwrap();
    family.tell(Add(1)).unwrap();
    family.tell(Add(2)).unwrap();
    let asked = tokio::spawn({
        let family = family.clone();
        async move { family.ask(Command::Get).await }
    });
    let first = family.family.current().unwrap();
    eventually("all three are stashed", async || first.diagnostics().processed == 3).await;
    family.tell(Command::Crash).unwrap();
    let answer =
        tokio::time::timeout(Duration::from_secs(5), asked).await.expect("the stashed ask ends with its actor");
    assert_eq!(answer.unwrap(), Err(RequestError::ReplyDropped { admitted: true }));
    eventually("the stash becomes dead letters", async || system.stats().dead_letters == 3).await;
    let mut letters: Vec<_> =
        system.dead_letters().recent(10).iter().map(|l| (l.label, l.count, l.reason, l.target.clone())).collect();
    letters.sort();
    let name = Some(Arc::from("hoarder"));
    assert_eq!(letters, [("get", 1, "actor_failed", name.clone()), ("held", 2, "actor_failed", name)]);

    // The next generation starts with an empty stash, and a clean drain leaves what it stashes as `drained` letters.
    eventually("the next generation runs", async || family.family.current().is_some()).await;
    family.tell(Add(3)).unwrap();
    let second = family.family.current().unwrap();
    eventually("its one message is stashed", async || second.diagnostics().processed == 1).await;
    family.lifecycle.drain(None);
    let exit = family.join().await;
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Drained, 1));
    eventually("the drained stash is a dead letter", async || system.stats().dead_letters == 4).await;
    let drained =
        system.dead_letters().recent(10).into_iter().find(|l| l.reason == "drained").map(|l| (l.label, l.count));
    assert_eq!(drained, Some(("held", 1)));
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stop_during_a_restart_backoff_ends_the_family_as_stopped() {
    let system = ActorSystem::builder().build().unwrap();
    // Long enough that the stop comes while the family waits out the backoff.
    let backoff = Backoff { initial: Duration::from_secs(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..RestartPolicy::default() };
    let family = system.supervise(Sleepy::default).name("sleepy").policy(policy).start_supervised().unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    family.tell(99u64).unwrap();
    family.tell(0u64).unwrap();
    family.tell(0u64).unwrap();
    eventually("the generation fails", async || family.family.current().is_none()).await;
    family.lifecycle.stop();
    let exit = tokio::time::timeout(Duration::from_secs(1), family.join())
        .await
        .expect("a stop does not wait out the backoff");
    // The failed generation's two went as dead letters at its failure: the family's exit does not count them again.
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Stopped, 0));
    let letters = system.dead_letters().recent(8);
    assert_eq!(letters.iter().map(|l| (l.reason, l.count)).collect::<Vec<_>>(), [("actor_failed", 2)]);
    system.shutdown().await.unwrap();
}

/// Fails its first message once the gate opens, and stops its own family from `on_stop`: after the failure, before the
/// supervisor sees it.
struct StopsItsFamily(Arc<OnceLock<LifecycleHandle>>, Arc<std::sync::Barrier>);

impl Actor for StopsItsFamily {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.1.wait();
        Err(())
    }

    fn on_stop(&mut self, _: &StopReason) {
        self.0.get().expect("set before the first message").stop();
    }
}

/// Ends as a stop during the backoff does, though this stop lands before the backoff is set.
#[tokio::test]
async fn a_stop_asked_for_as_a_generation_fails_ends_the_family_as_stopped() {
    let system = ActorSystem::builder().build().unwrap();
    let family_of = Arc::new(OnceLock::new());
    let gate = Arc::new(std::sync::Barrier::new(2));
    let (slot, opens) = (family_of.clone(), gate.clone());
    let family = system.supervise(move || StopsItsFamily(slot.clone(), opens.clone())).start_supervised().unwrap();
    family_of.set(family.lifecycle.clone()).ok();
    for _ in 0..3 {
        family.tell(()).unwrap();
    }
    gate.wait();
    let exit = tokio::time::timeout(Duration::from_secs(1), family.join()).await.expect("the stop ends the family");
    assert_eq!((exit.reason.clone(), exit.discarded_messages(), family.family.restarts()), (StopReason::Stopped, 0, 0));
    let letters = system.dead_letters().recent(8);
    assert_eq!(letters.iter().map(|l| (l.reason, l.count)).collect::<Vec<_>>(), [("actor_failed", 2)]);
    system.shutdown().await.unwrap();
}

/// What comes while the fence is open is handled; once it lapses nothing is, and a supervised family ends
/// instead of restarting into it.
#[tokio::test]
async fn nothing_is_handled_once_the_fence_lapses() {
    let system = ActorSystem::builder().build().unwrap();
    let fence = Fence::new(system.clock(), Duration::from_secs(60));
    let counter = system.spawn(Counter(0)).fence(fence.clone()).start().unwrap();
    counter.tell(Add(1)).unwrap();
    assert_eq!(counter.ask(Command::Get).await.unwrap(), 1);
    fence.hold_for(Duration::ZERO);
    counter.tell(Add(1)).unwrap();
    let exit = counter.join().await;
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Fenced, 1));
    assert_eq!(system.dead_letters().recent(1)[0].reason, "fenced");

    let family = system.supervise(|| Counter(0)).fence(fence).start_supervised().unwrap();
    family.tell(Add(1)).ok();
    let exit = tokio::time::timeout(Duration::from_secs(1), family.join()).await.expect("the family ends");
    assert_eq!((exit.reason, family.family.restarts()), (StopReason::Fenced, 0));

    // A family stopped past its fence while it waits out a restart ends because of the fence, as its owner asked.
    let fence = Fence::new(system.clock(), Duration::from_secs(60));
    // Long enough that the family is still waiting out the restart when the fence lapses.
    let backoff = Backoff { initial: Duration::from_secs(60), jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(|| Counter(0)).fence(fence.clone()).policy(policy).start_supervised().unwrap();
    assert_eq!(family.ask(Command::Get).await.unwrap(), 0);
    family.tell(Command::Crash).unwrap();
    while family.lifecycle.phase() != Phase::Starting {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    fence.hold_for(Duration::ZERO);
    family.lifecycle.stop();
    let exit = tokio::time::timeout(Duration::from_secs(1), family.join()).await.expect("the stop ends the family");
    assert_eq!(exit.reason, StopReason::Fenced);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drain_right_after_tells_never_drops_them() {
    let system = ActorSystem::builder().build().unwrap();
    for round in 0..20_000 {
        let counter = system.spawn(Counter(0)).start().unwrap();
        for _ in 0..8 {
            counter.tell(Add(1)).unwrap();
        }
        counter.lifecycle.drain(None);
        let exit = counter.join().await;
        assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Drained, 0), "round {round}");
    }
}

/// Takes every line at once, so the pump races the actor through the `Ready` it owes.
struct Lines(Arc<Mutex<Vec<u32>>>);

impl Sink<u32> for Lines {
    type Error = std::io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, line: u32) -> Result<(), Self::Error> {
        self.0.lock().push(line);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

enum Line {
    Say(u32),
    Wire(SinkEvent<std::io::Error>),
}

struct Writer {
    wire: Option<Lines>,
    tx: Option<SinkHandle<u32>>,
    outbox: std::collections::VecDeque<u32>,
}

impl Actor for Writer {
    type Message = Line;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let wire = self.wire.take().ok_or("started twice")?;
        self.tx = Some(cx.attach_sink(wire, 2, Line::Wire).map_err(|e| format!("{e:?}"))?);
        Ok(())
    }

    fn on_message(&mut self, msg: Line, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Line::Say(n) => self.outbox.push_back(n),
            Line::Wire(SinkEvent::Ready) => {}
            Line::Wire(SinkEvent::Closed(result)) => result.map_err(|e| e.to_string())?,
        }
        Ok(())
    }

    fn on_turn_end(&mut self, _: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(tx) = &self.tx else { return Ok(()) };
        while let Some(n) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(n) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message);
                }
                break;
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drain_ends_once_the_sink_has_written_everything() {
    let system = ActorSystem::builder().build().unwrap();
    for round in 0..20_000 {
        let written = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer { wire: Some(Lines(written.clone())), tx: None, outbox: Default::default() };
        let writer = system.spawn(writer).start().unwrap();
        for n in 0..10 {
            writer.send(Line::Say(n)).await.unwrap();
        }
        writer.lifecycle.drain(None);
        let exit = tokio::time::timeout(Duration::from_secs(5), writer.join()).await;
        let exit = exit.unwrap_or_else(|_| panic!("round {round} hung"));
        assert_eq!((exit.reason, written.lock().clone()), (StopReason::Drained, (0..10).collect()), "round {round}");
    }
    system.shutdown().await.unwrap();
}

/// On paused time a missed wake hangs until this lapses, instead of until the job's timeout.
async fn soon<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), f).await.expect("a missed wake")
}

/// Every task spawned on this tokio runtime ends, a threadless pump included.
async fn tasks_end() {
    soon(async {
        while tokio::runtime::Handle::current().metrics().num_alive_tasks() > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
}

/// Counts a tick each period, after a blocking job that counts nothing.
struct Ticker(Arc<AtomicU64>, Duration);

impl Actor for Ticker {
    type Message = u64;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.schedule_every(self.1, || 1u64).map_err(drop)?;
        cx.submit_io(|| 0u64).map_err(drop)?;
        Ok(())
    }

    fn on_message(&mut self, n: u64, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    fn on_stop(&mut self, _: &StopReason) {
        self.0.store(u64::MAX, Ordering::Relaxed);
    }
}

/// Keeps what it is asked, unanswered.
struct Keeps(Vec<ReplyTo<u64>>);

impl Actor for Keeps {
    type Message = ReplyTo<u64>;
    type Error = ();

    fn on_message(&mut self, reply: ReplyTo<u64>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.push(reply);
        Ok(())
    }
}

/// Asks `keeps` with a deadline `within` from now and checks it ends in `Deadline` no more than a second after it. A
/// poll after the deadline reads it itself, so only the time it took shows a wake the clock thread slept through.
async fn times_out(keeps: &ActorRef<ReplyTo<u64>>, within: Duration) {
    let deadline = keeps.clock().now() + within;
    let ended = tokio::time::timeout(Duration::from_secs(5), keeps.request(Some(deadline), |reply| reply)).await;
    assert!(matches!(ended, Ok(Err(RequestError::Deadline { admitted: true }))), "{ended:?}");
    let late = keeps.clock().now() - deadline;
    assert!(late < Duration::from_secs(1), "the deadline's wake came {late:?} late");
}

/// The clock thread sleeps until its earliest wake. A wake registered earlier than that wakes it; one registered later
/// need not.
#[tokio::test(flavor = "multi_thread")]
async fn an_earlier_deadline_registered_after_a_later_one_still_fires_on_time() {
    let system = ActorSystem::builder().build().unwrap();
    let keeps = system.spawn(Keeps(Vec::new())).start().unwrap();
    keeps.ready().await.unwrap();
    let address = keeps.address.clone();
    let mut long = Box::pin(address.request(Some(address.clock().now() + Duration::from_secs(30)), |reply| reply));
    assert!(long.as_mut().poll(&mut TaskContext::from_waker(std::task::Waker::noop())).is_pending());
    // Long enough for the clock thread to go to sleep until the 30 s wake.
    tokio::time::sleep(Duration::from_millis(20)).await;
    times_out(&address, Duration::from_millis(50)).await;

    let askers: Vec<_> = (0..8u64)
        .map(|task| {
            let address = address.clone();
            tokio::spawn(async move {
                for i in 0..50 {
                    times_out(&address, Duration::from_millis(1 + (task + i) % 5)).await;
                }
            })
        })
        .collect();
    for asker in askers {
        asker.await.unwrap();
    }
    drop(long);
    assert_eq!(system.clock().pending_wakes(), 0);
    assert_eq!(address.diagnostics().requests, 0);
    system.shutdown().await.unwrap();
}

/// Answers each ask from a timer, long after an asker's wait on its thread is over.
struct Later;

enum Delayed {
    Ask(ReplyTo<u64>),
    Answer(ReplyTo<u64>),
}

impl Actor for Later {
    type Message = Delayed;
    type Error = ();

    fn on_message(&mut self, msg: Delayed, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Delayed::Ask(reply) => drop(cx.schedule_after(Duration::from_millis(10), Delayed::Answer(reply))),
            Delayed::Answer(reply) => drop(reply.reply(7)),
        }
        Ok(())
    }
}

/// A lone ask off the workers waits on its thread before it sleeps; an answer after that wait must still wake it, and
/// leave no wake or request behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_lone_ask_answered_after_its_spin_still_wakes() {
    let system = ActorSystem::builder().build().unwrap();
    let later = system.spawn(Later).start().unwrap();
    later.ready().await.unwrap();
    let (thread, task) = (later.clone(), later.clone());
    let asked = std::thread::spawn(move || {
        let answers: Vec<_> = (0..20).map(|_| park_on(thread.ask(Delayed::Ask)).unwrap()).collect();
        let tokio = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let more = tokio.block_on(tokio.spawn(async move {
            let mut answers = Vec::new();
            for _ in 0..20 {
                answers.push(task.ask(Delayed::Ask).await.unwrap());
            }
            answers
        }));
        (answers, more.unwrap())
    });
    eventually("every ask is answered", async || asked.is_finished()).await;
    let (answers, more) = asked.join().unwrap();
    assert_eq!((answers, more), (vec![7; 20], vec![7; 20]));
    assert_eq!(system.clock().pending_wakes(), 0);
    let d = later.address.diagnostics();
    assert_eq!((d.requests, d.asks, d.ask_timeouts), (0, 40, 0));
    system.shutdown().await.unwrap();
}

/// Answers with the thread its turn ran on, after `self.0` of work.
struct Where(Duration);

impl Actor for Where {
    type Message = ReplyTo<std::thread::ThreadId>;
    type Error = ();

    fn on_message(&mut self, reply: ReplyTo<std::thread::ThreadId>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(self.0);
        let _ = reply.reply(std::thread::current().id());
        Ok(())
    }
}

/// A lone ask off the workers runs its idle actor's turn on the asking thread, which would only wait there. An actor
/// whose turns there are slow goes back to the workers, and is tried there again only now and then, so the thread's
/// other tasks do not wait behind it.
#[tokio::test(flavor = "multi_thread")]
async fn a_lone_ask_runs_a_quick_turn_on_its_own_thread() {
    let system = ActorSystem::builder().build().unwrap();
    let quick = system.spawn(Where(Duration::ZERO)).start().unwrap();
    let slow = system.spawn(Where(Duration::from_micros(50))).start().unwrap();
    quick.ready().await.unwrap();
    slow.ready().await.unwrap();
    let (q, s) = (quick.clone(), slow.clone());
    let here = std::thread::spawn(move || {
        let me = std::thread::current().id();
        let count =
            |a: &ActorHandle<_>| (0..1000).filter(|_| park_on(a.ask(std::convert::identity)).unwrap() == me).count();
        (count(&q), count(&s))
    });
    let (quick_here, slow_here) = here.join().unwrap();
    // Unoptimised, every turn is slow. Optimised, the first turns here are cold, and a turn timed while the machine
    // preempts this thread may send a quick actor away too.
    assert!(slow_here <= 20, "{slow_here} slow turns ran here");
    // Two preempted turns in a row send even a quick actor to the workers for 250 asks: a loaded box may do so twice.
    assert!(cfg!(debug_assertions) || quick_here >= 200, "{quick_here} quick turns ran here");
    assert_eq!((quick.address.diagnostics().asks, slow.address.diagnostics().asks), (1000, 1000));
    system.shutdown().await.unwrap();
}

enum Seq {
    Ask { from: usize, seq: u64, reply: ReplyTo<std::thread::ThreadId> },
    Tell(u64),
    Disordered(ReplyTo<usize>),
}

/// Counts turns that overlap another of its own, and messages that come before an earlier one from the same sender.
struct InOrder {
    busy: Arc<std::sync::atomic::AtomicBool>,
    overlaps: Arc<std::sync::atomic::AtomicUsize>,
    last: [u64; 3],
    disordered: usize,
}

impl InOrder {
    fn order(&mut self, from: usize, seq: u64) {
        self.disordered += usize::from(seq <= self.last[from]);
        self.last[from] = seq;
    }
}

impl Actor for InOrder {
    type Message = Seq;
    type Error = ();

    fn on_message(&mut self, msg: Seq, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if self.busy.swap(true, Ordering::SeqCst) {
            self.overlaps.fetch_add(1, Ordering::Relaxed);
        }
        match msg {
            Seq::Ask { from, seq, reply } => {
                self.order(from, seq);
                let _ = reply.reply(std::thread::current().id());
            }
            Seq::Tell(seq) => self.order(2, seq),
            Seq::Disordered(reply) => {
                let _ = reply.reply(self.disordered);
            }
        }
        self.busy.store(false, Ordering::SeqCst);
        Ok(())
    }
}

/// Turns run on asking threads while a worker runs the same actor's turns for its tells: one turn at a time, and each
/// sender's messages in the order sent.
#[tokio::test(flavor = "multi_thread")]
async fn turns_on_asking_threads_never_overlap_and_keep_each_senders_order() {
    let system = ActorSystem::builder().build().unwrap();
    let overlaps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let busy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let actor =
        system.spawn(InOrder { busy, overlaps: overlaps.clone(), last: [0; 3], disordered: 0 }).start().unwrap();
    actor.ready().await.unwrap();
    let askers: Vec<_> = (0..2)
        .map(|from| {
            let actor = actor.clone();
            std::thread::spawn(move || {
                let me = std::thread::current().id();
                (1..=5000)
                    .filter(|&seq| park_on(actor.ask(|reply| Seq::Ask { from, seq, reply })).unwrap() == me)
                    .count()
            })
        })
        .collect();
    for seq in 1..=20_000 {
        actor.send(Seq::Tell(seq)).await.unwrap();
    }
    let here: usize = askers.into_iter().map(|t| t.join().unwrap()).sum();
    let disordered = actor.ask(Seq::Disordered).await.unwrap();
    assert_eq!((overlaps.load(Ordering::Relaxed), disordered), (0, 0));
    assert!(cfg!(debug_assertions) || here > 0, "no turn ran on an asking thread");
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_threadless_ticker_counts_on_virtual_time() {
    let (system, _) = threadless();
    // Parked on a wake past what tokio's `Instant` holds, the pump must not overflow and die.
    system.spawn(Ticker(Arc::default(), Duration::MAX)).start().unwrap();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let ticks = Arc::new(AtomicU64::new(0));
    system.spawn(Ticker(ticks.clone(), Duration::from_millis(100))).start().unwrap();
    tokio::time::sleep(Duration::from_millis(10_050)).await;
    assert_eq!(ticks.load(Ordering::Relaxed), 100);
    let clock = system.clock().clone();
    let outside = std::thread::spawn(move || clock.now()).join().unwrap();
    assert_eq!(outside, system.clock().now(), "read off the runtime, the clock still reads its time");
    soon(system.shutdown()).await.unwrap();
    drop(system);
    tasks_end().await;
}

#[tokio::test(start_paused = true)]
async fn a_threadless_ask_times_out_at_its_virtual_deadline() {
    let (system, threadless) = threadless();
    let counter = system.spawn(Counter(0)).start().unwrap();
    counter.ready().await.unwrap();
    threadless.hold(true);
    // Already scheduled, so the ask below queues nothing: only its deadline can wake the parked pump.
    counter.tell(Add(1)).unwrap();
    tokio::task::yield_now().await;
    let asked = tokio::time::Instant::now();
    let asking = tokio::spawn(async move { counter.ask(Command::Get).await });
    let answer = tokio::time::timeout(Duration::from_secs(60), asking).await.expect("a missed wake").unwrap();
    let took = asked.elapsed();
    assert!(matches!(answer, Err(RequestError::Deadline { admitted: true })), "{answer:?}");
    assert!((Duration::from_secs(5)..=Duration::from_millis(5_001)).contains(&took), "{took:?}");
    // No ask here: its deadline would wake the pump.
    threadless.hold(false);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(system.runtime_stats().workers[0].queued, 0, "the release wakes the pump");
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_threadless_fence_lapses_at_its_virtual_deadline_and_not_before() {
    let (system, _) = threadless();
    let fence = Fence::new(system.clock(), Duration::from_secs(1));
    let counter = system.spawn(Counter(0)).fence(fence.clone()).start().unwrap();
    tokio::time::sleep(Duration::from_millis(999)).await;
    assert!(fence.is_open());
    counter.tell(Add(1)).unwrap();
    assert_eq!(soon(counter.ask(Command::Get)).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(!fence.is_open());
    counter.tell(Add(1)).unwrap();
    assert_eq!(soon(counter.join()).await.reason, StopReason::Fenced);
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_threadless_restart_lands_at_its_virtual_backoff() {
    let (system, _) = threadless();
    let wait = Duration::from_secs(2);
    // Every restart waits exactly 2 s on the threadless clock.
    let backoff = Backoff { initial: wait, max: wait, jitter: 0.0 };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(|| Counter(0)).policy(policy).start_supervised().unwrap();
    assert_eq!(family.ask(Command::Get).await.unwrap(), 0);
    family.tell(Command::Crash).unwrap();
    while family.family.current().is_some() {
        tokio::task::yield_now().await;
    }
    let crashed = tokio::time::Instant::now();
    // The ask waits for the next generation.
    assert_eq!(soon(family.ask(Command::Get)).await.unwrap(), 0);
    let took = crashed.elapsed();
    assert!((Duration::from_secs(2)..=Duration::from_millis(2_001)).contains(&took), "{took:?}");
    system.shutdown().await.unwrap();
}

type Log = Arc<Mutex<Vec<(u64, &'static str)>>>;

/// Logs `what` for its generation when dropped.
struct Dropped(u64, &'static str, Log);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.2.lock().push((self.0, self.1));
    }
}

/// Takes nothing, ever.
struct Stuck(#[allow(dead_code)] Dropped);

impl Sink<u64> for Stuck {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), ()>> {
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, _: u64) -> Result<(), ()> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
}

enum Leaving {
    Late,
    Fail,
    Done,
}

/// Each generation sets a timer, starts a job that never ends and feeds a sink that takes nothing.
struct Leaves {
    generation: u64,
    log: Log,
    sink: Option<SinkHandle<u64>>,
}

impl Actor for Leaves {
    type Message = Leaving;
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.schedule_after(Duration::from_secs(1), Leaving::Late).map_err(drop)?;
        let flag = Dropped(self.generation, "job dropped", self.log.clone());
        cx.submit_future(async move {
            std::future::pending::<()>().await;
            drop(flag);
            Leaving::Done
        })
        .map_err(drop)?;
        let stuck = Stuck(Dropped(self.generation, "sink dropped", self.log.clone()));
        let sink = cx.attach_sink(stuck, 1, |_| Leaving::Done).map_err(drop)?;
        sink.try_send(1).map_err(drop)?;
        self.sink = Some(sink);
        Ok(())
    }

    fn on_message(&mut self, msg: Leaving, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Leaving::Late => self.log.lock().push((self.generation, "late")),
            Leaving::Done => self.log.lock().push((self.generation, "done")),
            Leaving::Fail => return Err(()),
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_restart_leaves_the_last_generations_timers_jobs_and_sink_behind() {
    let (system, _) = threadless();
    let log = Log::default();
    let (l, mut generation) = (log.clone(), 0);
    let factory = move || {
        generation += 1;
        Leaves { generation, log: l.clone(), sink: None }
    };
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(10), jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(factory).policy(policy).start_supervised().unwrap();
    soon(family.send(Leaving::Fail)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let mut seen = log.lock().clone();
    seen.sort();
    assert_eq!(seen, [(1, "job dropped"), (1, "sink dropped"), (2, "late")], "generation 2 gets only its own");
    assert_eq!(family.family.restarts(), 1);
    soon(system.shutdown()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_full_or_restarting_family_refuses_and_hands_the_message_back() {
    let threadless = Threadless::default();
    let runtime = RuntimeConfig { threadless: Some(threadless.clone()), ..Default::default() };
    let system = ActorSystem::builder().runtime(runtime).request_timeout(Duration::from_secs(5)).build().unwrap();
    let wait = Duration::from_secs(8);
    // Every restart waits exactly 8 s.
    let backoff = Backoff { initial: wait, max: wait, jitter: 0.0 };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let limits = MailboxLimits { messages: 2, ..Default::default() };
    let family = system.supervise(|| Counter(0)).mailbox(limits).policy(policy).start_supervised().unwrap();
    assert_eq!(soon(family.ask(Command::Get)).await.unwrap(), 0);
    threadless.hold(true);
    family.tell(Add(1)).unwrap();
    family.tell(Add(1)).unwrap();
    assert!(matches!(family.tell(Add(7)), Err(Reject { reason: RejectReason::Full, message: Command::Add(7) })));
    threadless.hold(false);
    assert_eq!(soon(family.ask(Command::Get)).await.unwrap(), 2);

    family.tell(Command::Crash).unwrap();
    while family.family.current().is_some() {
        tokio::task::yield_now().await;
    }
    assert!(matches!(family.tell(Add(7)), Err(Reject { reason: RejectReason::NotReady, message: Command::Add(7) })));
    // The backoff outlasts the send's timeout: it is refused at its deadline, message in hand.
    let refused = soon(family.send(Add(9))).await;
    assert!(matches!(refused, Err(Reject { reason: RejectReason::NotReady, message: Command::Add(9) })));
    assert_eq!(soon(family.ask(Command::Get)).await.unwrap(), 0, "the next generation starts empty");
    soon(system.shutdown()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_drain_refuses_the_senders_waiting_on_a_full_mailbox_at_once_and_handles_what_it_holds() {
    let (system, threadless) = threadless();
    let limits = MailboxLimits { messages: 4, ..Default::default() };
    let counter = system.spawn(Counter(0)).mailbox(limits).start().unwrap();
    counter.ready().await.unwrap();
    threadless.hold(true);
    for _ in 0..4 {
        counter.tell(Add(1)).unwrap();
    }
    let waiting: Vec<_> = (0..3)
        .map(|_| {
            let counter = counter.clone();
            tokio::spawn(async move { counter.send(Add(1)).await.is_ok() })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(counter.address.diagnostics().waiters, 3);
    counter.lifecycle.drain(None);
    // Still held, so no slot frees: the drain itself wakes and refuses them.
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(waiting.iter().all(tokio::task::JoinHandle::is_finished), "a waiting send outlived the drain");
    threadless.hold(false);
    for waiting in waiting {
        assert!(!soon(waiting).await.unwrap(), "a send admitted after the drain began");
    }
    let exit = soon(counter.join()).await;
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Drained, 0));
    assert_eq!(counter.address.diagnostics().admitted, 4, "what it held was handled");
    soon(system.shutdown()).await.unwrap();
}

/// A crash in a simulator: nothing the forsaken system held runs on, it is freed, and no thread is left.
#[test]
fn a_forsaken_threadless_system_is_freed_and_leaves_no_thread() {
    // Alone in a process, where every axon thread is this test's.
    const CHILD: &str = "AXON_FORSAKEN";
    if std::env::var_os(CHILD).is_none() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "a_forsaken_threadless_system_is_freed_and_leaves_no_thread", "--nocapture"])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success() && stdout.contains("1 passed"),
            "{stdout}{}",
            String::from_utf8_lossy(&out.stderr)
        );
        return;
    }
    let tokio = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    let ticks = Arc::new(AtomicU64::new(0));
    let t = ticks.clone();
    tokio.block_on(async move {
        let (system, threadless) = threadless();
        let every = Duration::from_millis(100);
        let ticker = system.spawn(Ticker(t, every)).execution(Execution::Dedicated).start().unwrap();
        // A restart a minute away, that the test never waits out.
        let backoff = Backoff { initial: Duration::from_secs(60), max: Duration::from_secs(60), jitter: 0.0 };
        let policy = RestartPolicy { backoff, ..Default::default() };
        let family = system.supervise(|| Counter(0)).policy(policy).start_supervised().unwrap();
        family.tell(Command::Crash).unwrap();
        let keeps = system.spawn(Keeps(Vec::new())).start().unwrap();
        drop(tokio::spawn(async move { keeps.ask(|reply| reply).await }));
        let idle = system.spawn(Counter(0)).start().unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        // Neither a dedicated actor nor a blocking job has a thread.
        assert_eq!(threads(), Vec::<String>::new());

        // Crashed while stalled, with a turn queued.
        threadless.hold(true);
        ticker.tell(0u64).unwrap();
        threadless.forsake();
        let life = family.lifecycle.clone();
        assert!(system.spawn(Counter(0)).start().is_err(), "a crashed system spawns nothing");
        assert_eq!(system.clock().pending_wakes(), 0);
        idle.tell(Add(1)).unwrap();
        assert_eq!(system.runtime_stats().workers[0].queued, 0, "a wake after the crash queues nothing");
        tasks_end().await;
        let system_of = system.downgrade();
        drop((system, family));
        assert!(system_of.upgrade().is_none(), "the system outlives its crash");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(ticker.lifecycle.exit().is_none() && life.exit().is_none(), "a crash publishes no exit");
    });
    drop(tokio);
    assert_eq!(ticks.load(Ordering::Relaxed), 2, "the ticks stop at the crash, and on_stop never runs");
    assert_eq!(Arc::strong_count(&ticks), 1, "the crashed actor is freed");
    assert_eq!(threads(), Vec::<String>::new());
}

/// This process's axon and tokio threads, on Linux.
fn threads() -> Vec<String> {
    if !cfg!(target_os = "linux") {
        return Vec::new();
    }
    let named = |task: std::fs::DirEntry| std::fs::read_to_string(task.path().join("comm")).ok();
    let tasks = std::fs::read_dir("/proc/self/task").unwrap().flatten().filter_map(named);
    tasks.filter(|name| name.starts_with("axon-") || name.starts_with("tokio-")).collect()
}

#[tokio::test(start_paused = true)]
async fn a_threadless_runtime_is_refused_where_it_cannot_run() {
    let threadless = Threadless::default();
    let config = || RuntimeConfig { threadless: Some(threadless.clone()), ..Default::default() };
    assert!(Runtime::new(config()).is_err(), "only an ActorSystem drives a threadless runtime");
    let manual = RuntimeConfig { clock: Clock::manual().clock(), ..config() };
    assert!(matches!(ActorSystem::builder().runtime(manual).build(), Err(SystemError::Configuration(_))));
    let system = ActorSystem::builder().runtime(config()).build().unwrap();
    assert!(ActorSystem::builder().runtime(config()).build().is_err(), "a second runtime on one Threadless");
    let counter = system.spawn(Counter(0)).start().unwrap();
    // Blocking the one thread the actors run on would wait for itself.
    let wait = || counter.lifecycle.wait(Duration::from_millis(1));
    let waited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(wait));
    assert_eq!(waited.is_err(), cfg!(debug_assertions));
    system.shutdown().await.unwrap();
}

enum Mourn {
    Watch(LifecycleHandle),
    Down(Exit),
    Downs(ReplyTo<Vec<StopReason>>),
}

struct Mourner(Vec<StopReason>);

impl Actor for Mourner {
    type Message = Mourn;
    type Error = String;

    fn on_message(&mut self, msg: Mourn, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Mourn::Watch(target) => drop(cx.watch(&target, Mourn::Down).map_err(|e| format!("{e:?}"))?),
            Mourn::Down(exit) => self.0.push(exit.reason),
            Mourn::Downs(reply) => drop(reply.reply(self.0.clone())),
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_on_a_supervised_family_is_told_when_the_family_ends_not_on_a_restart() {
    let system = ActorSystem::builder().build().unwrap();
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(|| Counter(0)).policy(policy).start_supervised().unwrap();
    let plain = system.spawn(Counter(0)).start().unwrap();
    let mourner = system.spawn(Mourner(Vec::new())).start().unwrap();
    mourner.tell(Mourn::Watch(family.lifecycle.clone())).unwrap();
    mourner.tell(Mourn::Watch(plain.lifecycle.clone())).unwrap();
    let downs = async || mourner.ask(Mourn::Downs).await.unwrap();

    family.tell(Command::Crash).unwrap();
    eventually("a restart", async || family.family.restarts() == 1 && family.family.current().is_some()).await;
    plain.lifecycle.stop();
    eventually("the plain actor's end", async || downs().await.len() == 1).await;
    assert_eq!(downs().await, [StopReason::Stopped], "a restart is not an end");

    family.lifecycle.stop();
    eventually("the family's end", async || downs().await.len() == 2).await;
    // Watched once it has ended: told at once.
    mourner.tell(Mourn::Watch(family.lifecycle.clone())).unwrap();
    eventually("the family's end again", async || downs().await.len() == 3).await;
    assert_eq!(downs().await, vec![StopReason::Stopped; 3]);
    system.shutdown().await.unwrap();
}

enum Succession {
    Watch(LifecycleHandle, Arc<str>),
    Down(Arc<str>),
}

/// On a worker's end, starts another under its name, and says how that went, with nothing else to wake it.
struct Heir(ActorSystem, tokio::sync::mpsc::Sender<Result<(), String>>);

impl Actor for Heir {
    type Message = Succession;
    type Error = String;

    fn on_message(&mut self, msg: Succession, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let said = match msg {
            Succession::Watch(worker, name) => {
                cx.watch(&worker, move |_| Succession::Down(name)).map(drop).map_err(|e| format!("{e:?}"))
            }
            Succession::Down(name) => {
                self.0.spawn(Counter(0)).name(&*name).start().map(drop).map_err(|e| e.to_string())
            }
        };
        self.1.try_send(said).map_err(|e| e.to_string())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watcher_left_idle_is_woken_by_the_down_and_finds_the_name_free() {
    let system = ActorSystem::builder().build().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    // On a thread of its own, so it handles the down while the worker's end is still being told to the others.
    let heir = system.spawn(Heir(system.clone(), tx)).execution(Execution::Dedicated).start().unwrap();
    let mourners: Vec<_> = (0..4).map(|_| system.spawn(Mourner(Vec::new())).start().unwrap()).collect();
    let mut next = async || tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
    for round in 0..20 {
        let name: Arc<str> = format!("worker-{round}").into();
        let worker = system.spawn(Counter(0)).name(&*name).start().unwrap();
        // Many watchers ahead of the heir, so a down told before the system's own would be handled while it still
        // holds the name.
        for mourner in &mourners {
            for _ in 0..50 {
                mourner.tell(Mourn::Watch(worker.lifecycle.clone())).unwrap();
            }
            mourner.ask(Mourn::Downs).await.unwrap();
        }
        heir.tell(Succession::Watch(worker.lifecycle.clone(), name)).unwrap();
        assert_eq!(next().await, Ok(()), "round {round}: the watch");
        worker.lifecycle.stop();
        assert_eq!(next().await, Ok(()), "round {round}: the worker's name was still taken on its down");
    }
    system.shutdown().await.unwrap();
}

enum Tie {
    Link(LifecycleHandle),
    Unlink(LifecycleHandle),
    /// Unlinks by the handles its links returned.
    Release,
    Fail,
    /// Answered once all before it are handled.
    Settled(ReplyTo<()>),
}

/// Links as told, fails when told, and says how it stopped.
struct Tied(Arc<Mutex<Vec<StopReason>>>, Vec<LinkHandle>);

impl Actor for Tied {
    type Message = Tie;
    type Error = &'static str;

    fn on_message(&mut self, msg: Tie, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Tie::Link(peer) => self.1.push(cx.link(&peer)),
            Tie::Unlink(peer) => cx.unlink(&peer),
            Tie::Release => self.1.drain(..).for_each(|l| l.cancel()),
            Tie::Fail => return Err("failed"),
            Tie::Settled(reply) => drop(reply.reply(())),
        }
        Ok(())
    }

    fn on_stop(&mut self, reason: &StopReason) {
        self.0.lock().push(reason.clone());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failure_stops_each_actor_linked_to_it_once_through_a_chain_or_a_cycle() {
    let system = ActorSystem::builder().build().unwrap();
    for cycle in [false, true] {
        let stops = Arc::new(Mutex::new(Vec::new()));
        let tied: Vec<_> = (0..4).map(|_| system.spawn(Tied(stops.clone(), Vec::new())).start().unwrap()).collect();
        for pair in tied.windows(2) {
            pair[0].send(Tie::Link(pair[1].lifecycle.clone())).await.unwrap();
        }
        if cycle {
            tied[3].send(Tie::Link(tied[0].lifecycle.clone())).await.unwrap();
        }
        // Linked again, from either end: still the one link.
        tied[1].send(Tie::Link(tied[0].lifecycle.clone())).await.unwrap();
        tied[0].send(Tie::Link(tied[1].lifecycle.clone())).await.unwrap();
        for t in &tied {
            t.ask(Tie::Settled).await.unwrap();
        }

        let failed_before = system.stats().failed;
        tied[2].tell(Tie::Fail).unwrap();
        let mut exits = Vec::new();
        for t in &tied {
            exits.push(soon(t.join()).await.reason);
        }
        let failed = tied[2].lifecycle.incarnation();
        assert!(matches!(exits[2], StopReason::Failed(_)), "cycle {cycle}: {exits:?}");
        assert_eq!((&exits[1], &exits[3]), (&StopReason::Linked(failed), &StopReason::Linked(failed)));
        assert!(matches!(exits[0], StopReason::Linked(_)), "cycle {cycle}: {exits:?}");
        assert!(exits.iter().all(StopReason::is_failure));
        assert_eq!(stops.lock().len(), 4, "cycle {cycle}: each stopped once: {:?}", stops.lock());
        assert_eq!(system.stats().failed, failed_before + 1, "one fault, counted once");
    }
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stop_or_a_drain_or_an_unlinked_failure_stops_nobody_else() {
    let system = ActorSystem::builder().build().unwrap();
    let stops = Arc::new(Mutex::new(Vec::new()));
    let spawn = || system.spawn(Tied(stops.clone(), Vec::new())).start().unwrap();
    let (hub, rest): (_, Vec<_>) = (spawn(), (0..4).map(|_| spawn()).collect());
    for other in &rest {
        hub.send(Tie::Link(other.lifecycle.clone())).await.unwrap();
        other.ask(Tie::Settled).await.unwrap(); // started, so its stop runs on_stop
    }
    hub.ask(Tie::Settled).await.unwrap();
    rest[0].lifecycle.stop();
    rest[1].lifecycle.drain(None);
    assert_eq!(soon(rest[0].join()).await.reason, StopReason::Stopped);
    assert_eq!(soon(rest[1].join()).await.reason, StopReason::Drained);
    hub.send(Tie::Unlink(rest[2].lifecycle.clone())).await.unwrap();
    hub.ask(Tie::Settled).await.unwrap();
    rest[2].tell(Tie::Fail).unwrap();
    soon(rest[2].join()).await;
    // Unlinked by its handle: the hub's failure reaches nobody.
    hub.send(Tie::Release).await.unwrap();
    hub.tell(Tie::Fail).unwrap();
    assert!(matches!(soon(hub.join()).await.reason, StopReason::Failed(_)));
    assert_eq!(rest[3].lifecycle.phase(), Phase::Running);
    assert_eq!(stops.lock().len(), 4);
    system.shutdown().await.unwrap();
}

struct Unstartable;

impl Actor for Unstartable {
    type Message = ();
    type Error = &'static str;

    fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Err("no")
    }

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn linking_to_an_actor_that_failed_to_start_already_stops_this_one_at_once() {
    let system = ActorSystem::builder().build().unwrap();
    let dead = system.spawn(Unstartable).start().unwrap();
    assert!(matches!(soon(dead.join()).await.reason, StopReason::StartFailed(_)));
    let stops = Arc::new(Mutex::new(Vec::new()));
    let late = system.spawn(Tied(stops, Vec::new())).start().unwrap();
    late.tell(Tie::Link(dead.lifecycle.clone())).unwrap();
    assert_eq!(soon(late.join()).await.reason, StopReason::Linked(dead.lifecycle.incarnation()));
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_supervised_actor_stopped_by_a_link_restarts_whether_it_or_its_family_was_linked() {
    let system = ActorSystem::builder().build().unwrap();
    let stops = Arc::new(Mutex::new(Vec::new()));
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let tied = {
        let stops = stops.clone();
        move || Tied(stops.clone(), Vec::new())
    };
    let family = system.supervise(tied).policy(policy).start_supervised().unwrap();
    let peer = system.spawn(Tied(stops.clone(), Vec::new())).start().unwrap();
    // The generation links, not the family: a restart is a failure like any other, and the next one is unlinked.
    family.send(Tie::Link(peer.lifecycle.clone())).await.unwrap();
    family.ask(Tie::Settled).await.unwrap();
    peer.tell(Tie::Fail).unwrap();
    soon(peer.join()).await;
    eventually("a restart", async || family.family.restarts() == 1 && family.family.current().is_some()).await;
    assert_eq!(stops.lock()[1], StopReason::Linked(peer.lifecycle.incarnation()));

    // Linked to the family's lifecycle, a failure fails its current generation, and the policy restarts it.
    let other = system.spawn(Tied(stops.clone(), Vec::new())).start().unwrap();
    other.send(Tie::Link(family.lifecycle.clone())).await.unwrap();
    other.tell(Tie::Fail).unwrap();
    soon(other.join()).await;
    eventually("a second restart", async || family.family.restarts() == 2 && family.family.current().is_some()).await;
    assert_eq!(stops.lock()[3], StopReason::Linked(other.lifecycle.incarnation()));
    assert!(family.lifecycle.exit().is_none());
    system.shutdown().await.unwrap();
}
