//! Actors on one node, apart from the cluster: a timer every 100 ms, a topic published to every 10 ms with 4 plain
//! subscribers and 4 supervised ones that fail on every 500th message (each on its own), and a round-robin router of
//! 4 workers asked every 100 ms. One `local` line per 10 s in `local.jsonl`, counters since the process started except
//! `timer_ticks` and `late_max_ms`, which are the window's: the timer runs on the monotonic clock, so a wall step
//! moves neither.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::{Actor, Context, ReplyTo, RestartPolicy, Router, Routing};
use serde_json::json;

use crate::Tick;
use crate::account::Log;

const PERIOD: Duration = Duration::from_millis(100);
/// A supervised subscriber fails on each message n with n % FAIL_EVERY == its index.
pub const FAIL_EVERY: u64 = 500;
const PLAIN: usize = 4;
const FLAKY: u64 = 4;

#[derive(Default)]
struct Counts {
    timer_ticks: AtomicU64,
    late_max_us: AtomicU64,
    got: [AtomicU64; PLAIN],
    gaps: AtomicU64,
    dups: AtomicU64,
    flaky_got: AtomicU64,
    planned_fails: AtomicU64,
    published: AtomicU64,
    told: AtomicU64,
    asks_ok: AtomicU64,
    asks_err: AtomicU64,
}

struct Ticker {
    c: Arc<Counts>,
    last: Option<Instant>,
}

impl Actor for Ticker {
    type Message = ();
    type Error = &'static str;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        cx.schedule_every(PERIOD, || ()).map_err(|_| "no timer")?;
        Ok(())
    }

    fn on_message(&mut self, (): (), _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let now = Instant::now();
        if let Some(last) = self.last.replace(now) {
            let late = now.saturating_duration_since(last + PERIOD).as_micros() as u64;
            self.c.late_max_us.fetch_max(late, Relaxed);
        }
        self.c.timer_ticks.fetch_add(1, Relaxed);
        Ok(())
    }
}

/// Each publisher's messages reach a subscriber in order (guide/actors.md § Topics): any hole is a message lost.
struct Plain {
    i: usize,
    c: Arc<Counts>,
    last: Option<u64>,
}

impl Actor for Plain {
    type Message = u64;
    type Error = &'static str;

    fn on_message(&mut self, n: u64, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match self.last {
            Some(l) if n <= l => self.c.dups.fetch_add(1, Relaxed),
            Some(l) => self.c.gaps.fetch_add(n - l - 1, Relaxed),
            None => 0,
        };
        self.last = Some(self.last.map_or(n, |l| l.max(n)));
        self.c.got[self.i].fetch_add(1, Relaxed);
        Ok(())
    }
}

struct Flaky {
    k: u64,
    c: Arc<Counts>,
}

impl Actor for Flaky {
    type Message = u64;
    type Error = &'static str;

    fn on_message(&mut self, n: u64, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        self.c.flaky_got.fetch_add(1, Relaxed);
        if n % FAIL_EVERY == self.k {
            self.c.planned_fails.fetch_add(1, Relaxed);
            return Err("planned");
        }
        Ok(())
    }
}

struct Worker;

impl Actor for Worker {
    type Message = ReplyTo<u64>;
    type Error = &'static str;

    fn on_message(&mut self, reply: ReplyTo<u64>, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let _ = reply.reply(1);
        Ok(())
    }
}

pub fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn Error>> {
    let (system, c, tick) = (cx.system, Arc::new(Counts::default()), Tick::default());
    system.spawn(Ticker { c: c.clone(), last: None }).name("rig.local.timer").start()?;
    let topic = system.topic::<u64>("rig.local")?;
    for i in 0..PLAIN {
        topic.subscribe(system.spawn(Plain { i, c: c.clone(), last: None }).start()?)?;
    }
    // Room for a failure every 5 s from each, as FAIL_EVERY at 100 messages a second makes.
    let policy = RestartPolicy { max_restarts: 100, ..RestartPolicy::default() };
    // A topic of their own: a dead letter names the topic, and theirs, missed while restarting, are planned.
    let flaky = system.topic::<u64>("rig.local.flaky")?;
    let mut families = Vec::new();
    for k in 0..FLAKY {
        let c = c.clone();
        let family = system
            .supervise(move || Flaky { k, c: c.clone() })
            .name(format!("rig.local.flaky-{k}"))
            .policy(policy.clone())
            .start_supervised()?;
        flaky.subscribe(family.clone())?;
        families.push(family);
    }
    let router = Router::new(Routing::RoundRobin);
    for _ in 0..4 {
        router.add(system.spawn(Worker).start()?);
    }
    let log = Log::create(&cx.log_dir.join("local.jsonl"))?;
    let (pc, ticks) = (c.clone(), tick.clone());
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_millis(10));
        let mut n = 0u64;
        loop {
            every.tick().await;
            n += 1;
            pc.told.fetch_add((topic.publish(n) + flaky.publish(n)) as u64, Relaxed);
            pc.published.store(n, Relaxed);
            ticks.fetch_add(1, Relaxed);
        }
    });
    let ac = c.clone();
    tokio::spawn(async move {
        let mut every = tokio::time::interval(PERIOD);
        loop {
            every.tick().await;
            match router.ask(|r| r).await {
                Ok(_) => ac.asks_ok.fetch_add(1, Relaxed),
                Err(_) => ac.asks_err.fetch_add(1, Relaxed),
            };
        }
    });
    tokio::spawn(async move {
        let mut every = crate::reports();
        loop {
            every.tick().await;
            let restarts: u64 = families.iter().map(|f| f.family.restarts()).sum();
            let alive = families.iter().filter(|f| f.lifecycle.exit().is_none()).count();
            log.line(
                "local",
                json!({"timer_ticks": c.timer_ticks.swap(0, Relaxed), "late_max_ms": c.late_max_us.swap(0, Relaxed) / 1000,
                       "published": c.published.load(Relaxed), "told": c.told.load(Relaxed),
                       "got": c.got.iter().map(|g| g.load(Relaxed)).collect::<Vec<_>>(),
                       "gaps": c.gaps.load(Relaxed), "dups": c.dups.load(Relaxed),
                       "flaky_got": c.flaky_got.load(Relaxed), "planned_fails": c.planned_fails.load(Relaxed),
                       "restarts": restarts, "families": alive,
                       "asks_ok": c.asks_ok.load(Relaxed), "asks_err": c.asks_err.load(Relaxed)}),
            );
        }
    });
    Ok(tick)
}
