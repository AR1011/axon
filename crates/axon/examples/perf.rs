//! cargo run --release -p axon --example perf -- <embedded|hosted|traced|contended|fanout|ask|asks|start|route|topic> [n]
//! `traced` is `hosted` with every message starting a trace and recording a span.
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axon::*;

struct Counter(Arc<AtomicU64>, u64);

enum Msg {
    Value(u64),
    Get(ReplyTo<u64>),
}

impl From<u64> for Msg {
    fn from(v: u64) -> Self {
        Msg::Value(v)
    }
}

impl Actor for Counter {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Value(v) => self.1 = self.1.wrapping_add(v),
            Msg::Get(reply) => drop(reply.reply(self.1)),
        }
        Ok(())
    }

    fn on_batch(&mut self, batch: &mut MailboxBatch<'_, Msg>, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let mut handled = 0;
        for msg in batch.by_ref() {
            handled += 1;
            self.on_message(msg, cx)?;
        }
        self.0.fetch_add(handled, Ordering::Release);
        Ok(())
    }
}

fn measure(mode: &str, n: usize, run: impl FnOnce()) {
    let began = Instant::now();
    run();
    let elapsed = began.elapsed();
    let ns = elapsed.as_nanos() as f64 / n as f64;
    println!("{mode}: n={n} elapsed={elapsed:?} ns/msg={ns:.1} Mmsg/s={:.2}", 1e3 / ns);
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).map_or("embedded", String::as_str);
    let n: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(2_000_000);
    let limits = MailboxLimits { messages: 4096, ..Default::default() };

    if mode == "embedded" {
        let done = Arc::new(AtomicU64::new(0));
        let (mut driver, spawn) = EmbeddedDriver::new(Counter(done, 0), limits, Clock::manual().clock()).unwrap();
        driver.run_turn(TurnBudget::default());
        let budget = TurnBudget::messages(256);
        measure(mode, n, || {
            let mut sent = 0;
            while sent < n {
                let chunk = (n - sent).min(256);
                for i in 0..chunk {
                    spawn.address.try_tell(Msg::Value(i as u64)).unwrap();
                }
                sent += chunk;
                black_box(driver.run_turn(budget));
            }
        });
        return;
    }

    if mode == "start" {
        // Spawns from outside onto 8 workers, each start awaited: a worker that sleeps between them must be woken.
        let rt = Runtime::new(RuntimeConfig { workers: 8, ..Default::default() }).unwrap();
        let n = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(40_000);
        let done = Arc::new(AtomicU64::new(0));
        let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
        measure(mode, n, || {
            for _ in 0..n {
                let spawned = rt.spawn(Counter(done.clone(), 0), Execution::Shared, limits).unwrap();
                let mut ready = std::pin::pin!(spawned.ready());
                let actor = loop {
                    let polled = std::future::Future::poll(ready.as_mut(), &mut std::task::Context::from_waker(&waker));
                    if let std::task::Poll::Ready(actor) = polled {
                        break actor.unwrap();
                    }
                    std::thread::park();
                };
                actor.address.try_tell(Msg::Value(1)).unwrap();
            }
        });
        rt.shutdown(Duration::from_secs(5));
        return;
    }

    if mode == "route" {
        // 1 producer → 4 actors on 4 workers, through an ActorSystem: a direct tell to each in turn, then each routing.
        let tokio = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
        let _enter = tokio.enter();
        let config = RuntimeConfig { workers: 4, turn_budget: TurnBudget::messages(256), ..Default::default() };
        let system = ActorSystem::builder().runtime(config).mailbox(limits).build().unwrap();
        let done = Arc::new(AtomicU64::new(0));
        let actors: Vec<_> = (0..4).map(|_| system.spawn(Counter(done.clone(), 0)).start().unwrap()).collect();
        let (round_robin, least_loaded) = (Router::new(Routing::RoundRobin), Router::new(Routing::LeastLoaded));
        for actor in &actors {
            tokio.block_on(actor.ready()).unwrap();
            round_robin.add(actor.clone());
            least_loaded.add(actor.clone());
        }
        let mut sent = 0;
        let mut run = |name: &str, tell: &dyn Fn(usize, Msg) -> Result<Enqueued, Reject<Msg>>| {
            measure(name, n, || {
                for i in 0..n {
                    let mut msg = Msg::Value(i as u64);
                    while let Err(reject) = tell(i, msg) {
                        msg = reject.message;
                        std::thread::yield_now();
                    }
                }
                sent += n as u64;
                while done.load(Ordering::Acquire) < sent {
                    std::thread::sleep(Duration::from_micros(50));
                }
            })
        };
        // Twice: the first round also wakes the workers.
        for _ in 0..2 {
            run("route direct", &|i, msg| actors[i % 4].tell(msg));
            run("route round-robin", &|_, msg| round_robin.tell(msg));
            run("route least-loaded", &|_, msg| least_loaded.tell(msg));
            run("route hash", &|i, msg| round_robin.member(&i).expect("4 members").tell(msg));
        }
        tokio.block_on(system.shutdown()).unwrap();
        return;
    }

    if mode == "asks" {
        // 1000 tokio tasks of 100 asks each over 16 actors on 4 workers, each ask with the system's 5 s deadline, from
        // a tokio runtime of one thread, as the actor comparison's harness runs them.
        let tokio = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
        let _enter = tokio.enter();
        let system =
            ActorSystem::builder().runtime(RuntimeConfig { workers: 4, ..Default::default() }).build().unwrap();
        let actors: Vec<_> = (0..16).map(|_| system.spawn(Counter(Arc::default(), 0)).start().unwrap()).collect();
        for actor in &actors {
            tokio.block_on(actor.ready()).unwrap();
        }
        let (tasks, each) = (1000, 100);
        // Twice: the first round also wakes the workers.
        for _ in 0..2 {
            measure(mode, tasks * each, || {
                tokio.block_on(async {
                    let asking: Vec<_> = (0..tasks)
                        .map(|t| {
                            let actor = actors[t % actors.len()].clone();
                            tokio::spawn(async move {
                                for _ in 0..each {
                                    black_box(actor.ask(Msg::Get).await.unwrap());
                                }
                            })
                        })
                        .collect();
                    for task in asking {
                        task.await.unwrap();
                    }
                })
            });
        }
        tokio.block_on(system.shutdown()).unwrap();
        return;
    }

    if mode == "topic" {
        // 1 producer → 64 actors on 4 workers, through an ActorSystem: a tell to each in turn, then one publish to all,
        // in rounds of 1024 that fit every mailbox, so nothing is missed.
        let tokio = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
        let _enter = tokio.enter();
        let config = RuntimeConfig { workers: 4, turn_budget: TurnBudget::messages(256), ..Default::default() };
        let system = ActorSystem::builder().runtime(config).mailbox(limits).build().unwrap();
        let done = Arc::new(AtomicU64::new(0));
        let actors: Vec<_> = (0..64).map(|_| system.spawn(Counter(done.clone(), 0)).start().unwrap()).collect();
        let topic = system.topic::<Msg>("perf").unwrap();
        for actor in &actors {
            tokio.block_on(actor.ready()).unwrap();
            topic.subscribe(actor.clone()).unwrap();
        }
        let rounds = (n / (1024 * 64)).max(1) as u64;
        let mut sent = 0;
        let mut run = |name: &str, fan: &dyn Fn(u64) -> usize| {
            measure(name, (rounds * 1024 * 64) as usize, || {
                for round in 0..rounds {
                    for i in 0..1024 {
                        assert_eq!(fan(round * 1024 + i), 64, "a mailbox was full");
                    }
                    sent += 1024 * 64;
                    while done.load(Ordering::Acquire) < sent {
                        std::thread::sleep(Duration::from_micros(50));
                    }
                }
            })
        };
        // Twice: the first round also wakes the workers.
        for _ in 0..2 {
            run("topic direct", &|i| actors.iter().filter(|a| a.tell(i).is_ok()).count());
            run("topic publish", &|i| topic.publish(i));
        }
        tokio.block_on(system.shutdown()).unwrap();
        return;
    }

    let (producers, actor_count, workers) = match mode {
        "contended" => (4, 1, 1),
        "fanout" => (1, 64, 4),
        _ => (1, 1, 1),
    };
    let rt =
        Runtime::new(RuntimeConfig { workers, turn_budget: TurnBudget::messages(256), ..Default::default() }).unwrap();
    let (actors, done): (Vec<_>, Vec<_>) = (0..actor_count)
        .map(|_| {
            let done = Arc::new(AtomicU64::new(0));
            let mut config = MailboxConfig::from(limits);
            if mode == "traced" {
                config = config.with_tracer(Tracer::new(4096, 1));
            }
            let actor = rt.spawn(Counter(done.clone(), 0), Execution::Shared, config).unwrap();
            (actor.wait_ready(Duration::from_secs(5)).unwrap(), done)
        })
        .unzip();

    if mode == "ask" {
        let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
        measure(mode, n, || {
            for _ in 0..n {
                let mut req = std::pin::pin!(actors[0].address.request(None, Msg::Get));
                while std::future::Future::poll(req.as_mut(), &mut std::task::Context::from_waker(&waker)).is_pending()
                {
                    std::thread::park();
                }
            }
        });
        return;
    }

    measure(mode, n * producers, || {
        std::thread::scope(|scope| {
            for _ in 0..producers {
                let actors = &actors;
                scope.spawn(move || {
                    for i in 0..n {
                        let mut msg = Msg::Value(i as u64);
                        while let Err(reject) = actors[i % actor_count].address.try_tell(msg) {
                            msg = reject.message;
                            std::thread::yield_now();
                        }
                    }
                });
            }
        });
        while done.iter().map(|d| d.load(Ordering::Acquire)).sum::<u64>() < (n * producers) as u64 {
            std::thread::sleep(Duration::from_micros(50));
        }
    });
    rt.shutdown(Duration::from_secs(5));
}

struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
