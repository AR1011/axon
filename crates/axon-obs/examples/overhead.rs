//! Telemetry overhead: fixed-duration trials after a warm-up, variants interleaved round-robin
//! so drift hits all of them alike, medians and IQR reported against the first variant.
//! usage: overhead [rounds=9] [trial ms=1000]
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::{
    Actor, ActorSystem, Clock, Context, EmbeddedDriver, MailboxBatch, MailboxConfig, MailboxLimits, RuntimeConfig,
    TurnBudget,
};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::{AcceptLimits, Config, Frame, Lane, Listener, SharedSecret, connect};
use axon_obs::{Telemetry, TelemetryConfig};
use bytes::Bytes;

struct Sink<const LABELLED: bool>(Arc<AtomicU64>);

impl<const LABELLED: bool> Actor for Sink<LABELLED> {
    type Message = u64;
    type Error = ();

    fn on_message(&mut self, _: u64, _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }

    fn on_batch(&mut self, batch: &mut MailboxBatch<'_, u64>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        let n = batch.map(std::hint::black_box).count() as u64;
        self.0.fetch_add(n, Relaxed);
        Ok(())
    }

    fn label(msg: &u64) -> &'static str {
        if !LABELLED {
            ""
        } else if msg.is_multiple_of(2) {
            "even"
        } else {
            "odd"
        }
    }
}

struct Idle;

impl Actor for Idle {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

fn mailbox(sample_every: u32) -> MailboxConfig<u64> {
    MailboxConfig { timing_sample_every: sample_every, ..MailboxLimits { messages: 4096, ..Default::default() }.into() }
}

/// Admission plus dispatch on one thread: the per-message cost with no scheduling in the way.
fn embedded<const LABELLED: bool>(sample_every: u32, warm_up: Duration, trial: Duration) -> f64 {
    let sink = Sink::<LABELLED>(Arc::default());
    let (mut driver, spawn) = EmbeddedDriver::new(sink, mailbox(sample_every), Clock::system()).unwrap();
    driver.run_turn(TurnBudget::default());
    let budget = TurnBudget::messages(256);
    let mut run = |span: Duration| {
        let (started, mut sent) = (Instant::now(), 0u64);
        while started.elapsed() < span {
            for _ in 0..256 {
                spawn.address.try_tell(sent).unwrap();
                sent += 1;
            }
            std::hint::black_box(driver.run_turn(budget));
        }
        started.elapsed().as_nanos() as f64 / sent as f64
    };
    run(warm_up);
    run(trial)
}

/// One producer thread feeding one hosted actor, optionally next to the telemetry extension
/// sampling this many idle actors at 1 Hz.
async fn hosted(extension: Option<usize>, warm_up: Duration, trial: Duration) -> f64 {
    let runtime = RuntimeConfig { workers: 1, turn_budget: TurnBudget::messages(256), ..Default::default() };
    let system = ActorSystem::builder().runtime(runtime).build().unwrap();
    if let Some(idle) = extension {
        let mut transport = Config::new("bench", SharedSecret::new("local-benchmark-only").unwrap());
        transport.allow_plaintext = true;
        let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
        let cluster = Cluster::start(&system, cfg).await.unwrap();
        Telemetry::start(&system, &cluster, TelemetryConfig::default()).await.unwrap();
        for _ in 0..idle {
            system.spawn(Idle).start().unwrap();
        }
    }
    let delivered = Arc::new(AtomicU64::new(0));
    let actor = system.spawn(Sink::<true>(delivered.clone())).mailbox(mailbox(128)).start().unwrap();
    let address = actor.address.clone();
    let producing = std::thread::spawn(move || {
        let run = |span: Duration| {
            let (started, before, mut sent) = (Instant::now(), delivered.load(Relaxed), 0u64);
            while started.elapsed() < span {
                for _ in 0..1024 {
                    match address.try_tell(sent) {
                        Ok(_) => sent += 1,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }
            while delivered.load(Relaxed) - before < sent {
                std::thread::yield_now();
            }
            started.elapsed().as_nanos() as f64 / sent as f64
        };
        run(warm_up);
        run(trial)
    });
    let ns = tokio::task::spawn_blocking(move || producing.join().unwrap()).await.unwrap();
    system.shutdown().await.unwrap();
    ns
}

async fn transport(sample_every: u32, warm_up: Duration, trial: Duration) -> f64 {
    let config = |name: &str| {
        let mut cfg = Config::new(name, SharedSecret::new("local-benchmark-only").unwrap());
        cfg.allow_plaintext = true;
        cfg.timing_sample_every = sample_every;
        cfg
    };
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, config("server"), AcceptLimits::default()).await.unwrap();
    let client = connect(listener.local_addr(), Lane::Control, None, &config("client")).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    tokio::spawn(async move { while server.receiver.recv().await.is_ok() {} });
    let payload = Bytes::from(vec![7u8; 256]);
    let mut ns = 0.0;
    for span in [warm_up, trial] {
        let (started, mut sent) = (Instant::now(), 0u64);
        while started.elapsed() < span {
            for _ in 0..256 {
                let frame = Frame { id: sent, payload: payload.clone(), ..Default::default() };
                client.sender.submit(frame, None).await.unwrap();
                sent += 1;
            }
        }
        client.sender.flush().await.unwrap();
        ns = started.elapsed().as_nanos() as f64 / sent as f64;
    }
    ns
}

fn quartiles(samples: &mut [f64]) -> (f64, f64, f64) {
    samples.sort_by(f64::total_cmp);
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    (at(0.25), at(0.5), at(0.75))
}

fn report(title: &str, unit: &str, names: &[&str], results: &mut [Vec<f64>]) {
    println!("\n{title}\n{:<28} {:>10} {:>16} {:>10}", "variant", unit, "IQR", "overhead");
    let mut baseline = 0.0;
    for (name, samples) in names.iter().zip(results) {
        let (q1, median, q3) = quartiles(samples);
        if baseline == 0.0 {
            baseline = median;
        }
        let overhead = (median / baseline - 1.0) * 100.0;
        println!("{name:<28} {median:>10.2} {:>16} {overhead:>+9.1}%", format!("{q1:.2}..{q3:.2}"));
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1).map(|a| a.parse::<u64>().expect("a number"));
    let rounds = args.next().unwrap_or(9) as usize;
    let trial = Duration::from_millis(args.next().unwrap_or(1000));
    let warm_up = trial / 4;

    type Run = fn(Duration, Duration) -> f64;
    let variants: [(&str, Run); 5] = [
        ("timings off", |w, t| embedded::<false>(0, w, t)),
        ("timings 1/128", |w, t| embedded::<false>(128, w, t)),
        ("timings 1/128 + labels", |w, t| embedded::<true>(128, w, t)),
        ("timings 1/16 + labels", |w, t| embedded::<true>(16, w, t)),
        ("timings 1/1 + labels", |w, t| embedded::<true>(1, w, t)),
    ];
    let mut results = vec![Vec::new(); variants.len()];
    for _ in 0..rounds {
        for ((_, run), samples) in variants.iter().zip(&mut results) {
            samples.push(run(warm_up, trial));
        }
    }
    let names: Vec<_> = variants.iter().map(|v| v.0).collect();
    report("embedded: admission + dispatch on one thread", "ns/msg", &names, &mut results);

    let variants = [("1/128 + labels", None), ("... + extension, 1000 actors", Some(1000))];
    let mut results = vec![Vec::new(); variants.len()];
    for _ in 0..rounds {
        for ((_, extension), samples) in variants.iter().zip(&mut results) {
            samples.push(hosted(*extension, warm_up, trial).await);
        }
    }
    let names: Vec<_> = variants.iter().map(|v| v.0).collect();
    report("hosted: 1 producer -> 1 actor, 1 worker", "ns/msg", &names, &mut results);

    let variants = [("timings off", 0), ("timings 1/128", 128), ("timings 1/16", 16)];
    let mut results = vec![Vec::new(); variants.len()];
    for _ in 0..rounds {
        for ((_, every), samples) in variants.iter().zip(&mut results) {
            samples.push(transport(*every, warm_up, trial).await);
        }
    }
    let names: Vec<_> = variants.iter().map(|v| v.0).collect();
    report("transport: 256 B frames one way over TCP loopback", "ns/frame", &names, &mut results);
    println!("\nbudget: counters <= 2 %, sampled timings <= 5 %. Counters cannot be compiled out, so");
    println!("\"timings off\" already includes them. Threads are not pinned. The hosted figure moves with");
    println!("how often the worker parks, so read it only as extension on versus off.");
}
