//! Quotas (spec/quotas.md §6): the check, memory per entry and per replica row, background bytes, and a new replica's
//! pull. Every node runs in this process over loopback. `cargo run --release -p axon-cluster --example quota -- <mode>`
//! usage: quota check [secs=2] [keys..]   take with one and two limits, acquire and drop, on 1, 4, 12 and 24 threads at 10³,
//!                                10⁶ and 10⁷ keys: uniform, Zipf (s = 1) and one hot key
//!        quota memory [keys=200000]    resident bytes per entry and per replica row, each case a process of its own
//!        quota bytes [nodes=50] [keys=1000000] [secs=20]   wire bytes a second per node, idle and under load
//!        quota pull [keys=1000000]     how long a handover, then a new process, take to pull the rows of that many keys
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time and runs its own threads")]

use std::time::{Duration, Instant};

use axon::{ActorSystem, RuntimeConfig};
use axon_cluster::*;
use axon_net::SharedSecret;

/// Limits no case reaches, so that every check admits, but for `AtLimit`.
const REQ: Rate = Rate::new("req/s", 1 << 40, Duration::from_secs(1));
const WEIGHT: Rate = Rate::new("weight/min", 1 << 40, Duration::from_secs(60));
const WS: Permits = Permits::new("ws", 1 << 40);
const TIGHT: Rate = Rate::new("tight/s", 100, Duration::from_secs(1));

struct Node {
    system: ActorSystem,
    cluster: Cluster,
    quotas: Quotas,
}

async fn node(id: &str, discovery: &MemoryDiscovery, region: &str, capacity: usize) -> Node {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"benchmark-secret-0123").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    // All nodes dial from one address here.
    cfg.accept.max_handshakes_per_ip = 256;
    let runtime = RuntimeConfig { workers: 1, ..Default::default() };
    let system = ActorSystem::builder().runtime(runtime).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let cfg = QuotaConfig { region: region.into(), capacity, ..QuotaConfig::default() };
    let quotas = Quotas::start(&cluster, cfg).await.unwrap();
    Node { system, cluster, quotas }
}

async fn start(ids: impl IntoIterator<Item = (String, String)>, capacity: usize) -> (Vec<Node>, MemoryDiscovery) {
    let discovery = MemoryDiscovery::new();
    let mut nodes = Vec::new();
    for (id, region) in ids {
        nodes.push(node(&id, &discovery, &region, capacity).await);
    }
    all_see(&nodes, nodes.len()).await;
    (nodes, discovery)
}

/// Until every node sees `n` quota nodes and none is pulling.
async fn all_see(nodes: &[Node], n: usize) {
    let quota = |c: &Cluster| c.view().members.iter().filter(|m| m.record.metadata.contains_key("axon.quota")).count();
    while nodes.iter().any(|x| quota(&x.cluster) != n || x.cluster.quotas_starting()) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn stop(nodes: Vec<Node>) {
    for n in nodes {
        n.system.shutdown().await.unwrap();
    }
}

async fn rows_reach(nodes: &[Node], rows: usize) {
    while nodes.iter().any(|n| n.quotas.stats().rows < rows) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum Keys {
    Uniform,
    Zipf,
    Hot,
}

/// `n` keys of `0..keys`, drawn before the clock starts. Zipf with s = 1 as the continuous inverse, ⌊keys^u⌋ − 1.
fn draws(keys: u64, dist: Keys, seed: u64, n: usize) -> Vec<[u8; 8]> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let u = (x >> 11) as f64 / (1u64 << 53) as f64;
            let k = match dist {
                Keys::Uniform => x % keys,
                Keys::Zipf => (keys as f64).powf(u) as u64 - 1,
                Keys::Hot => 0,
            };
            k.to_le_bytes()
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Take,
    TakeTwo,
    AcquireDrop,
    AtLimit,
}

fn once(quotas: &Quotas, op: Op, key: &[u8]) -> bool {
    match op {
        Op::Take => quotas.take(key, &[(REQ, 1)]).is_ok(),
        Op::TakeTwo => quotas.take(key, &[(REQ, 1), (WEIGHT, 5)]).is_ok(),
        Op::AcquireDrop => quotas.acquire(key, &WS).is_ok(),
        Op::AtLimit => quotas.take(key, &[(TIGHT, 1)]).is_ok(),
    }
}

/// Calls a second on `threads` threads, each cycling through its own draws.
fn run(quotas: &Quotas, op: Op, draws: &[Vec<[u8; 8]>], secs: f64) -> (f64, f64) {
    let began = Instant::now();
    let deadline = began + Duration::from_secs_f64(secs);
    let (ops, admitted) = std::thread::scope(|s| {
        let threads: Vec<_> = draws
            .iter()
            .map(|keys| {
                s.spawn(move || {
                    let (mut n, mut ok) = (0u64, 0u64);
                    for key in keys.iter().cycle() {
                        ok += once(quotas, op, key) as u64;
                        n += 1;
                        if n.is_multiple_of(4096) && Instant::now() >= deadline {
                            break;
                        }
                    }
                    (n, ok)
                })
            })
            .collect();
        threads.into_iter().map(|t| t.join().unwrap()).fold((0, 0), |(n, ok), (a, b)| (n + a, ok + b))
    });
    let secs = began.elapsed().as_secs_f64();
    (ops as f64 / secs, admitted as f64 / ops as f64)
}

/// Processor seconds this process has used. Linux counts them in 10 ms ticks; elsewhere `ps` is asked.
fn cpu_s() -> f64 {
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        let fields: Vec<&str> = stat.rsplit(')').next().unwrap_or_default().split_whitespace().collect();
        return (fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap()) / 100.0;
    }
    ps("cputime=").split(':').fold(0.0, |total, part| total * 60.0 + part.parse::<f64>().unwrap_or(0.0))
}

fn ps(column: &str) -> String {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps").args(["-o", column, "-p", &pid]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn rss_bytes() -> f64 {
    ps("rss=").parse::<f64>().unwrap_or(0.0) * 1024.0
}

async fn check(secs: f64, sizes: &[u64]) {
    const THREADS: [usize; 4] = [1, 4, 12, 24];
    println!(
        "{:<12} {:<8} {:>9} {:>7} {:>10} {:>9} {:>8}",
        "op", "keys", "of", "threads", "M/s", "ns/call", "admitted"
    );
    for &keys in sizes {
        // Room for every key of the three limits: eviction is not what this measures.
        let (nodes, _) = start([("a".into(), String::new())], (4 * keys as usize).max(1 << 20)).await;
        let quotas = nodes[0].quotas.clone();
        let ops: &[Op] = match keys {
            1_000 => &[Op::Take, Op::AcquireDrop, Op::AtLimit],
            1_000_000 => &[Op::Take, Op::TakeTwo, Op::AcquireDrop],
            _ => &[Op::Take],
        };
        // Every key once per op on 24 threads: ns a call a thread.
        let fill = || {
            let began = Instant::now();
            std::thread::scope(|s| {
                for t in 0..24u64 {
                    let quotas = &quotas;
                    s.spawn(move || {
                        (t..keys)
                            .step_by(24)
                            .for_each(|k| ops.iter().for_each(|op| _ = once(quotas, *op, &k.to_le_bytes())))
                    });
                }
            });
            began.elapsed().as_nanos() as f64 * 24.0 / (keys as usize * ops.len()) as f64
        };
        let new = tokio::task::block_in_place(fill);
        if keys > 1_000 {
            println!("first call for a key: {new:.0} ns a thread, 24 threads");
        }
        // Until a pass over every key admits nothing on the fallback, each entry having its grant, or 10 minutes. The
        // passes keep every entry in use, so that none is swept (60 s unused).
        let waited = Instant::now();
        let left = loop {
            let fallback = quotas.stats().fallback;
            tokio::task::block_in_place(fill);
            let left = quotas.stats().fallback - fallback;
            if left == 0 || waited.elapsed() > Duration::from_secs(600) {
                break left;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let s = quotas.stats();
        let fallback = s.fallback;
        println!(
            "{} entries, {} rows, {left} units on the fallback in the last pass, {:.1?} after they were made",
            s.entries,
            s.rows,
            waited.elapsed()
        );
        let mut cases = vec![(Op::Take, Keys::Uniform), (Op::Take, Keys::Zipf)];
        if keys == 1_000 {
            cases.extend([(Op::Take, Keys::Hot), (Op::AcquireDrop, Keys::Hot), (Op::AtLimit, Keys::Hot)]);
        }
        if keys == 1_000_000 {
            cases.extend([(Op::TakeTwo, Keys::Zipf), (Op::AcquireDrop, Keys::Zipf)]);
        }
        for (op, dist) in cases {
            for threads in THREADS {
                let draws: Vec<_> = (0..threads).map(|t| draws(keys, dist, t as u64 + 1, 1 << 20)).collect();
                let (rate, admitted) = tokio::task::block_in_place(|| run(&quotas, op, &draws, secs));
                let per = 1e9 * threads as f64 / rate;
                let of = format!("{dist:?}");
                println!(
                    "{:<12} {keys:<8} {of:>9} {threads:>7} {:>10.2} {per:>9.1} {admitted:>8.3}",
                    format!("{op:?}"),
                    rate / 1e6
                );
            }
        }
        let fell_back = quotas.stats().fallback - fallback;
        let cpu = cpu_s();
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!(
            "idle with {} entries: {:.2} cores; {fell_back} units on the fallback while timed",
            quotas.stats().entries,
            (cpu_s() - cpu) / 5.0
        );
        drop(quotas);
        stop(nodes).await;
    }
}

/// One memory case, run in a process of its own so that what an earlier case freed does not hide what this one takes:
/// `a` alone, or three nodes where `a` or all three use every key. Prints resident bytes per key.
async fn memory_case(case: &str, keys: u64) {
    let ids: Vec<(String, String)> = match case {
        "lone" => vec![("a".into(), String::new())],
        _ => ["a", "b", "c"].map(|n| (n.to_owned(), String::new())).to_vec(),
    };
    let (nodes, _) = start(ids, (2 * keys as usize).max(1 << 20)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before = rss_bytes();
    let users = if case == "all" { &nodes[..] } else { &nodes[..1] };
    tokio::task::block_in_place(|| {
        std::thread::scope(|s| {
            for n in users {
                s.spawn(|| (0..keys).for_each(|k| _ = n.quotas.take(k.to_le_bytes(), &[(REQ, 1)])));
            }
        })
    });
    rows_reach(&nodes, keys as usize * users.len()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("{}", (rss_bytes() - before) / keys as f64);
    stop(nodes).await;
}

fn memory(keys: u64) {
    let case = |case: &str| {
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new(exe).args(["memory-case", case, &keys.to_string()]).output().unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().unwrap()
    };
    // lone: an entry, and a key with one row at its one replica. one: an entry, and three such keys. all: three
    // entries, and three keys of three rows each.
    let (lone, one, all) = (case("lone"), case("one"), case("all"));
    let key = (one - lone) / 2.0;
    let entry = lone - key;
    let row = (all - 3.0 * entry - 3.0 * key) / 6.0;
    println!(
        "{keys} keys of a rate, resident bytes per key: lone {lone:.0}, three nodes one using {one:.0}, all three {all:.0}"
    );
    println!("an entry, granted: {entry:.0} B; a replica's key with one row: {key:.0} B; each further row: {row:.0} B");
}

fn wire_bytes(nodes: &[Node]) -> u64 {
    nodes.iter().flat_map(|n| n.cluster.stats().engine.peers).map(|p| p.session.tx_wire_bytes).sum()
}

async fn bytes(count: usize, keys: u64, secs: u64) {
    let ids = (0..count).map(|i| (format!("n{i:02}"), format!("r{}", i % 3)));
    let (nodes, _) = start(ids, 1 << 20).await;
    let nodes = std::sync::Arc::new(nodes);
    println!("{count} quota nodes in 3 regions; each takes Zipf keys of {keys} under a rate of 100 a second");
    for per_node in [0u64, 1_000, 10_000] {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        for (i, n) in nodes.iter().enumerate().filter(|_| per_node > 0) {
            let (quotas, running) = (n.quotas.clone(), running.clone());
            let keys = draws(keys, Keys::Zipf, i as u64 + 1, 1 << 16);
            tokio::spawn(async move {
                let mut every = tokio::time::interval(Duration::from_millis(10));
                let mut at = keys.iter().cycle();
                while running.load(std::sync::atomic::Ordering::Relaxed) {
                    every.tick().await;
                    at.by_ref().take(per_node as usize / 100).for_each(|k| _ = quotas.take(k, &[(TIGHT, 1)]));
                }
            });
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let stats = |nodes: &[Node]| {
            nodes
                .iter()
                .map(|n| n.quotas.stats())
                .fold((0, 0, 0), |(r, f, d), s| (r + s.reports, f + s.fallback, d + s.denied_limit + s.denied_share))
        };
        let (bytes, (reports, fallback, denied), began) = (wire_bytes(&nodes), stats(&nodes), Instant::now());
        tokio::time::sleep(Duration::from_secs(secs)).await;
        let took = began.elapsed().as_secs_f64() * count as f64;
        let (r, f, d) = stats(&nodes);
        let sent = (wire_bytes(&nodes) - bytes) as f64 / took;
        println!(
            "{per_node:>6} takes/s a node: {sent:>9.0} B/s sent a node, {:.1} reports/s a node, {:.0} fallback units/s, {:.0} denied/s a node",
            (r - reports) as f64 / took,
            (f - fallback) as f64 / took,
            (d - denied) as f64 / took,
        );
        running.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    let rows: usize = nodes.iter().map(|n| n.quotas.stats().rows).sum();
    println!("rows at the replicas in all: {rows}");
    stop(std::sync::Arc::into_inner(nodes).unwrap()).await;
}

/// A handover: four nodes, `a` using every key, each key replicated on three. When `d` leaves, each of the others
/// replicates every key and pulls the rows of those it gains from the other two. Then `d` starts again, a new process
/// that pulls the rows of its keys from all three before it is ready.
async fn pull(keys: u64) {
    let (mut nodes, discovery) = start(["a", "b", "c", "d"].map(|n| (n.to_owned(), String::new())), 1 << 24).await;
    tokio::task::block_in_place(|| (0..keys).for_each(|k| _ = nodes[0].quotas.take(k.to_le_bytes(), &[(REQ, 1)])));
    let began = Instant::now();
    rows_reach(&nodes, keys as usize * 7 / 10).await;
    let rows: Vec<usize> = nodes.iter().map(|n| n.quotas.stats().rows).collect();
    println!("{keys} keys used on a, rows at a, b, c and d {rows:?} in {:.1?}", began.elapsed());
    let d = nodes.pop().expect("four nodes");
    let began = Instant::now();
    d.system.shutdown().await.unwrap();
    // The pulls, and a's reports of the keys whose grants it dropped with d, both bring rows: timed apart.
    let (mut began_pulling, mut pulled, mut every_key) = (false, None, None);
    while pulled.is_none() || every_key.is_none() {
        let stats: Vec<QuotaStats> = nodes.iter().map(|n| n.quotas.stats()).collect();
        // A pull too short to be seen still ends before every key is held.
        began_pulling |= stats.iter().any(|s| s.pulling) || every_key.is_some();
        if began_pulling && pulled.is_none() && stats.iter().all(|s| !s.pulling) {
            pulled = Some(began.elapsed());
        }
        if every_key.is_none() && stats.iter().all(|s| s.keys >= keys as usize) {
            every_key = Some(began.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let rows: Vec<usize> = nodes.iter().map(|n| n.quotas.stats().rows).collect();
    println!(
        "d left; a, b and c hold rows {rows:?}: none pulling {:.1?} after, every key held {:.1?} after",
        pulled.unwrap(),
        every_key.unwrap()
    );
    let began = Instant::now();
    nodes.push(node("d", &discovery, "", 1 << 24).await);
    while nodes[3].cluster.quotas_starting() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!("d back: ready {:.1?} after its start, holding {} rows", began.elapsed(), nodes[3].quotas.stats().rows);
    stop(nodes).await;
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize, default: u64| args.get(i).and_then(|a| a.parse().ok()).unwrap_or(default);
    match args.first().map(String::as_str) {
        Some("check") => {
            let sizes: Vec<u64> = args.iter().skip(2).filter_map(|a| a.parse().ok()).collect();
            let sizes = if sizes.is_empty() { vec![1_000, 1_000_000, 10_000_000] } else { sizes };
            check(args.get(1).and_then(|a| a.parse().ok()).unwrap_or(2.0), &sizes).await
        }
        Some("memory") => memory(arg(1, 200_000)),
        Some("memory-case") => memory_case(&args[1], arg(2, 200_000)).await,
        Some("bytes") => bytes(arg(1, 50) as usize, arg(2, 1_000_000), arg(3, 20)).await,
        Some("pull") => pull(arg(1, 1_000_000)).await,
        _ => eprintln!("usage: quota check | memory | bytes | pull (see the file's head)"),
    }
}
