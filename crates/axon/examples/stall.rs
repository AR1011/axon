//! One sender fans out to `actors` actors with `try_tell`. Actor 0 handles each message in `slow_us` and the sender
//! offers it more than that, so its mailbox stays nearly full; the rest handle theirs at once. `load` threads spin on
//! the cores the workers run on, so a worker is descheduled at any point of its turn.
//!
//! cargo run --release -p axon-actors --example stall -- [secs=10] [actors=64] [workers=4] [slow_us=10]
//!     [messages=4096] [words=2] [load=44] [rate=50000] [sender=0]
//!
//! The sender runs alone on core `sender` (and its SMT sibling, `sender + cores / 2`, is left idle), the workers and
//! the load threads pinned in turn to the other cores. No other thread of this process runs on the sender's core, so its
//! voluntary context switches (`vol`) are sleeps on a lock, and its involuntary ones (`invol`) are another process
//! taking the core: on a box others share, a long send is either. Each second it prints, for
//! sends to the slow actor and to the rest: how many, how many were refused `Full`, the p99 and the worst time of one
//! `try_tell`, how many took over 100 µs and how many of those slept (and the worst of them), then the slow actor's
//! queue and the sender's context switches. A message is `words` u64s; `rate` is rounds of one message to each actor
//! per second.
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time and starts its own threads")]

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axon::*;

struct Args {
    secs: u64,
    actors: usize,
    workers: usize,
    slow_us: u64,
    messages: usize,
    words: usize,
    load: usize,
    rate: u64,
    sender: usize,
}

fn args() -> Args {
    let mut a = Args {
        secs: 10,
        actors: 64,
        workers: 4,
        slow_us: 10,
        messages: 4096,
        words: 2,
        load: 44,
        rate: 50_000,
        sender: 0,
    };
    for arg in std::env::args().skip(1) {
        let (k, v) = arg.split_once('=').unwrap_or_else(|| panic!("expected key=value, got {arg}"));
        let n = || v.parse::<u64>().unwrap_or_else(|_| panic!("{k}={v} is not a number"));
        match k {
            "secs" => a.secs = n(),
            "actors" => a.actors = n() as usize,
            "workers" => a.workers = n() as usize,
            "slow_us" => a.slow_us = n(),
            "messages" => a.messages = n() as usize,
            "words" => a.words = n() as usize,
            "load" => a.load = n() as usize,
            "rate" => a.rate = n(),
            "sender" => a.sender = n() as usize,
            _ => panic!("unknown argument {k}"),
        }
    }
    a
}

struct Sink<const W: usize>(Duration);

impl<const W: usize> Actor for Sink<W> {
    type Message = [u64; W];
    type Error = ();

    fn on_message(&mut self, msg: [u64; W], _: &mut Context<'_, Self>) -> Result<(), ()> {
        black_box(msg);
        if !self.0.is_zero() {
            let began = Instant::now();
            while began.elapsed() < self.0 {
                std::hint::spin_loop();
            }
        }
        Ok(())
    }
}

/// Times in ns, in 16 linear steps per power of two: a p99 within 6 %, the worst exact.
struct Hist {
    counts: Vec<u64>,
    sends: u64,
    full: u64,
    /// Sends over 100 µs, far past the sub-µs of an uncontended one.
    long: u64,
    worst: u64,
    /// Long sends during which the sender slept (its voluntary context switches rose), and the worst of them: a wait for
    /// a lock, where a long send that did not sleep is the sender descheduled.
    slept: u64,
    slept_max: u64,
}

impl Hist {
    fn new() -> Self {
        Self { counts: vec![0; 64 * 16], sends: 0, full: 0, long: 0, worst: 0, slept: 0, slept_max: 0 }
    }

    fn record(&mut self, ns: u64, full: bool) {
        let i = if ns < 16 {
            ns as usize
        } else {
            let e = 63 - ns.leading_zeros() as usize;
            (e - 3) * 16 + ((ns >> (e - 4)) & 15) as usize
        };
        self.counts[i] += 1;
        self.sends += 1;
        self.full += u64::from(full);
        self.long += u64::from(ns > 100_000);
        self.worst = self.worst.max(ns);
    }

    fn floor(i: usize) -> u64 {
        if i < 16 { i as u64 } else { (16 + (i % 16) as u64) << (i / 16 - 1) }
    }

    fn p99(&self) -> u64 {
        let mut left = self.sends - self.sends * 99 / 100;
        for i in (0..self.counts.len()).rev() {
            if self.counts[i] >= left {
                return Self::floor(i);
            }
            left -= self.counts[i];
        }
        0
    }

    fn line(&self) -> String {
        format!(
            "sends={} full={} p99_us={:.1} max_us={:.1} over_100us={} slept={} slept_max_us={:.1}",
            self.sends,
            self.full,
            us(self.p99()),
            us(self.worst),
            self.long,
            self.slept,
            us(self.slept_max),
        )
    }
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1e3
}

/// The calling thread's voluntary and involuntary context switches; zeros where `/proc` is missing.
fn switches() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/thread-self/status").unwrap_or_default();
    let field = |name: &str| status.lines().find_map(|l| l.strip_prefix(name)?.trim().parse().ok()).unwrap_or(0);
    (field("voluntary_ctxt_switches:"), field("nonvoluntary_ctxt_switches:"))
}

fn pin(core: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core });
}

fn main() {
    let a = args();
    match a.words {
        2 => run::<2>(&a),
        32 => run::<32>(&a),
        128 => run::<128>(&a),
        w => panic!("words={w}: 2, 32 or 128"),
    }
}

fn run<const W: usize>(a: &Args) {
    let allowed = RuntimeConfig::allowed_cores();
    let at = allowed.iter().position(|&c| c == a.sender).expect("the sender's core is one this process may use");
    let sibling = allowed.len().is_multiple_of(2).then(|| allowed[(at + allowed.len() / 2) % allowed.len()]);
    let others: Vec<usize> = allowed.iter().copied().filter(|&c| c != a.sender && Some(c) != sibling).collect();
    println!(
        "stall: secs={} actors={} workers={} slow_us={} messages={} words={} load={} rate={} sender={} sibling={sibling:?} \
         others={} entry_bytes={}",
        a.secs,
        a.actors,
        a.workers,
        a.slow_us,
        a.messages,
        a.words,
        a.load,
        a.rate,
        a.sender,
        others.len(),
        size_of::<[u64; W]>(),
    );
    let config = RuntimeConfig {
        workers: a.workers,
        cores: others.clone(),
        turn_budget: TurnBudget::messages(256),
        ..Default::default()
    };
    let rt = Runtime::new(config).unwrap();
    let limits = MailboxLimits { messages: a.messages, ..Default::default() };
    let actors: Vec<_> = (0..a.actors)
        .map(|i| {
            let slow = if i == 0 { Duration::from_micros(a.slow_us) } else { Duration::ZERO };
            let actor = rt.spawn(Sink::<W>(slow), Execution::Shared, limits).unwrap();
            actor.wait_ready(Duration::from_secs(5)).unwrap().address
        })
        .collect();
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        for i in 0..a.load {
            let (stop, core) = (&stop, others[(a.workers + i) % others.len()]);
            scope.spawn(move || {
                pin(core);
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..1000 {
                        std::hint::spin_loop();
                    }
                }
            });
        }
        pin(a.sender);
        send(a, &actors);
        stop.store(true, Ordering::Relaxed);
    });
    rt.shutdown(Duration::from_secs(5));
}

fn send<const W: usize>(a: &Args, actors: &[ActorRef<[u64; W]>]) {
    let round = Duration::from_nanos(1_000_000_000 / a.rate.max(1));
    let (mut total_slow, mut total_rest) = (Hist::new(), Hist::new());
    let mut seconds_p99 = Vec::new();
    let began = Instant::now();
    let (vol0, invol0) = switches();
    let mut next = began;
    for sec in 1..=a.secs {
        let (mut slow, mut rest) = (Hist::new(), Hist::new());
        let (vol, invol) = switches();
        let mut seen = vol;
        let end = began + Duration::from_secs(sec);
        let mut i = 0u64;
        while Instant::now() < end {
            for (j, actor) in actors.iter().enumerate() {
                i += 1;
                let t = Instant::now();
                let full = actor.try_tell([i; W]).is_err();
                let ns = t.elapsed().as_nanos() as u64;
                let h = if j == 0 { &mut slow } else { &mut rest };
                h.record(ns, full);
                if ns > 100_000 {
                    let now = switches().0;
                    if now > seen {
                        h.slept += 1;
                        h.slept_max = h.slept_max.max(ns);
                    }
                    seen = now;
                }
            }
            // Behind, it sends the next round at once rather than a burst of the rounds it missed.
            next = (next + round).max(Instant::now());
            while Instant::now() < next {
                std::hint::spin_loop();
            }
        }
        let (vol_now, invol_now) = switches();
        println!(
            "sec={sec} slow: {} | rest: {} | slow_depth={} vol={} invol={}",
            slow.line(),
            rest.line(),
            actors[0].diagnostics().mailbox_depth,
            vol_now - vol,
            invol_now - invol,
        );
        seconds_p99.push(slow.p99());
        for (total, h) in [(&mut total_slow, &slow), (&mut total_rest, &rest)] {
            total.counts.iter_mut().zip(&h.counts).for_each(|(t, c)| *t += c);
            total.sends += h.sends;
            total.full += h.full;
            total.long += h.long;
            total.worst = total.worst.max(h.worst);
            total.slept += h.slept;
            total.slept_max = total.slept_max.max(h.slept_max);
        }
    }
    let (vol1, invol1) = switches();
    seconds_p99.sort_unstable();
    println!(
        "stall total: slow: {} median_second_p99_us={:.1} | rest: {} | vol={} invol={}",
        total_slow.line(),
        us(seconds_p99[seconds_p99.len() / 2]),
        total_rest.line(),
        vol1 - vol0,
        invol1 - invol0,
    );
}
