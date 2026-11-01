//! An origin's publish throughput, and the wait for a record to be durable, with its stream in memory and on disk, and
//! with 256 streams on disk, each write appending to every one of their files.
//! `cargo run --release -p axon-stream --features cluster --example disk [dir]` (default `target/disk-bench`)
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::*;
use bytes::Bytes;

const BATCH: usize = 16;
const RECORD: usize = 64;
const ROUNDS: usize = 5;
const STREAMS: usize = 256;

fn median(mut runs: Vec<f64>) -> f64 {
    runs.sort_by(f64::total_cmp);
    runs[runs.len() / 2]
}

async fn origin(cluster: &Cluster, dir: Option<&std::path::Path>, streams: usize) -> (StreamNode, Vec<StreamKey>) {
    let mut cfg = StreamNodeConfig::new(vec![]);
    if let Some(dir) = dir {
        let _ = std::fs::remove_dir_all(dir);
        let keep = Retention { namespace: "bench".into(), max_bytes: 1 << 30, max_age: Duration::from_secs(3600) };
        cfg.disk = Some(DiskConfig::new(Arc::new(OsFiles::new(dir)), vec![keep]));
    }
    let node = StreamNode::start(cluster, cfg).await.unwrap();
    let mut keys = Vec::new();
    for i in 0..streams {
        let key = StreamKey::new("bench", if streams == 1 { "stream".into() } else { format!("stream-{i}") });
        node.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
        keys.push(key);
    }
    (node, keys)
}

async fn durable(node: &StreamNode, key: &StreamKey, cursor: Cursor) {
    while node.cursor(key) != Some(cursor) {
        tokio::task::yield_now().await;
    }
}

/// Publishes `records` as fast as the origin admits them, a batch to each stream in turn, until the last is durable.
/// Returns records per second.
async fn throughput(cluster: &Cluster, dir: Option<&std::path::Path>, streams: usize, records: usize) -> f64 {
    let (node, keys) = origin(cluster, dir, streams).await;
    let payload = Bytes::from(vec![7u8; RECORD]);
    let start = Instant::now();
    let mut last = vec![None; keys.len()];
    for i in 0..records / BATCH {
        let at = i % keys.len();
        loop {
            match node.publish(&keys[at], vec![payload.clone(); BATCH]) {
                Ok(cursor) => break last[at] = Some(cursor),
                Err(e) if e.error == StreamError::Disk(DiskError::Behind) => tokio::task::yield_now().await,
                Err(e) => panic!("{:?}", e.error),
            }
        }
    }
    for (key, last) in keys.iter().zip(last) {
        durable(&node, key, last.unwrap()).await;
    }
    (records / BATCH * BATCH) as f64 / start.elapsed().as_secs_f64()
}

/// A batch to each stream at a time, waited for until all are durable. Returns the median wait in µs.
async fn latency(cluster: &Cluster, dir: Option<&std::path::Path>, streams: usize, rounds: usize) -> f64 {
    let (node, keys) = origin(cluster, dir, streams).await;
    let payload = Bytes::from(vec![7u8; RECORD]);
    let mut waits = Vec::new();
    for _ in 0..rounds {
        let start = Instant::now();
        let cursors: Vec<Cursor> =
            keys.iter().map(|key| node.publish(key, vec![payload.clone(); BATCH]).unwrap()).collect();
        for (key, cursor) in keys.iter().zip(cursors) {
            durable(&node, key, cursor).await;
        }
        waits.push(start.elapsed().as_secs_f64() * 1e6);
    }
    median(waits)
}

#[tokio::main]
async fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).unwrap_or("target/disk-bench".into()));
    let mut transport = axon_net::Config::new("bench", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let system = ActorSystem::builder().build().unwrap();
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let cluster = Cluster::start(&system, cfg).await.unwrap();

    for (name, dir, streams, rounds) in [
        ("memory", None, 1, 200),
        ("disk", Some(dir.as_path()), 1, 200),
        ("streams256", Some(dir.as_path()), STREAMS, 50),
    ] {
        let mut runs = Vec::new();
        for _ in 0..ROUNDS {
            runs.push(throughput(&cluster, dir, streams, 2_000_000).await);
        }
        let wait = latency(&cluster, dir, streams, rounds).await;
        println!(
            "{name}: {:.2} M records/s published and durable, {wait:.1} µs median until durable",
            median(runs) / 1e6
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    system.shutdown().await.unwrap();
}
