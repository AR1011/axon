//! `files::Log` on the file system: records a second and the latency of each write, by records per write, on segments
//! that grow and on zero-filled ones, with one owner and with several on the same disk.
//!
//! cargo run --release -p axon --example log -- [secs=5] [body=128] [keys=1024] [owners=1,4] [per=1,16,256]
//!     [shapes=growing,zero-filled] [fill=bytes] [dir=/tmp]
//!
//! Each round appends `per` records, each to the next of `keys` keys, takes one write and waits for it: a write and a
//! sync of that many records, as an owner's turn gives it. A write's latency is from `take_write` to `written`. The
//! segments are 64 MiB; a zero-filled one is filled 256 KiB ahead of its frames (`fill=` sets it), so about one write
//! in 256 KiB of records also grows the file. With several owners, each runs its own log in its own directory on a
//! thread of its own, all at once; records/s and writes/s are the sum of each owner's rate, from after its open, the
//! percentiles and the slowest write over every owner's writes.
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time and runs each owner on a thread")]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axon::Histogram;
use axon::files::{Log, LogConfig, OsFiles};
use bytes::Bytes;

fn arg(name: &str, default: &str) -> String {
    std::env::args()
        .skip(1)
        .find_map(|a| a.strip_prefix(&format!("{name}=")).map(str::to_owned))
        .unwrap_or_else(|| default.to_owned())
}

/// One owner's write latencies, and its records and writes a second from after its open.
struct Owned {
    latency: Histogram,
    records: f64,
    writes: f64,
}

fn owner(dir: &Path, cfg: LogConfig, per: usize, secs: u64, body: &Bytes, names: &[Vec<u8>]) -> Result<Owned, String> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| e.to_string())?;
    let mut log = runtime.block_on(Log::open(Arc::new(OsFiles::new(dir)), cfg)).map_err(|e| format!("{e:?}"))?;
    let (mut latency, mut writes, mut next) = (Histogram::default(), 0u64, 0usize);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(secs) {
        for _ in 0..per {
            log.append(&names[next % names.len()], 1, body.clone()).map_err(|e| format!("{e:?}"))?;
            next += 1;
        }
        let began = Instant::now();
        while let Some(write) = log.take_write(0) {
            let result = runtime.block_on(write);
            log.written(result);
        }
        latency.record(began.elapsed());
        writes += 1;
        if let Some(failing) = log.failing() {
            return Err(format!("the write failed: {failing:?}"));
        }
    }
    let secs = start.elapsed().as_secs_f64();
    Ok(Owned { latency, records: (writes * per as u64) as f64 / secs, writes: writes as f64 / secs })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secs: u64 = arg("secs", "5").parse()?;
    let body = Bytes::from(vec![7u8; arg("body", "128").parse()?]);
    let keys: usize = arg("keys", "1024").parse()?;
    let owners: Vec<usize> = arg("owners", "1,4").split(',').map(str::parse).collect::<Result<_, _>>()?;
    let pers: Vec<usize> = arg("per", "1,16,256").split(',').map(str::parse).collect::<Result<_, _>>()?;
    let shapes = arg("shapes", "growing,zero-filled");
    let fill_ahead: u64 = arg("fill", &LogConfig::default().fill_ahead.to_string()).parse()?;
    if owners.contains(&0) {
        return Err("owners: each count is 1 or more".into());
    }
    let root = std::path::PathBuf::from(arg("dir", "/tmp")).join(format!("axon-log-bench-{}", std::process::id()));
    let names: Vec<Vec<u8>> = (0..keys).map(|k| format!("key-{k}").into_bytes()).collect();
    println!("| Owners | Segments | records a write | records/s | writes/s | p50 µs | p99 µs | p99.9 µs | max µs |");
    println!("|---|---|---|---|---|---|---|---|---|");
    for &n in &owners {
        for (preallocate, shape) in [(false, "growing"), (true, "zero-filled")] {
            if !shapes.split(',').any(|s| s == shape) {
                continue;
            }
            for &per in &pers {
                let dir = root.join(format!("{n}-{preallocate}-{per}"));
                let results: Vec<Result<Owned, String>> = std::thread::scope(|s| {
                    let threads: Vec<_> = (0..n)
                        .map(|i| {
                            let (dir, body, names) = (dir.join(i.to_string()), &body, &names);
                            // Nothing is trimmed: a fast disk must not make the run Full.
                            let cfg =
                                LogConfig { preallocate, fill_ahead, max_bytes: u64::MAX, ..LogConfig::default() };
                            s.spawn(move || owner(&dir, cfg, per, secs, body, names))
                        })
                        .collect();
                    threads.into_iter().map(|t| t.join().unwrap_or_else(|_| Err("an owner panicked".into()))).collect()
                });
                let (mut latency, mut records, mut writes) = (Histogram::default(), 0.0, 0.0);
                for r in results {
                    let r = r?;
                    latency.merge(&r.latency);
                    records += r.records;
                    writes += r.writes;
                }
                let us = |p: f64| latency.percentile(p).map_or(0, |d| d.as_micros());
                println!(
                    "| {n} | {shape} | {per} | {records:.0} | {writes:.0} | {} | {} | {} | {} |",
                    us(0.5),
                    us(0.99),
                    us(0.999),
                    us(1.0)
                );
                std::fs::remove_dir_all(&dir)?;
            }
        }
    }
    let _ = std::fs::remove_dir(&root);
    Ok(())
}
