//! The disk workload: each node originates `disk-<node>/s0` and `s1`, kept on the streams disk (`STREAM_DISK`), 20
//! records of about 2 KiB a second each; the streams workload, which runs the stream node, follows every other node's
//! (src/streams.rs). A record carries the incarnation that published it and the position its stream was opened after,
//! so a follower sees a restart that went back over records it had: the disk lost what it had synced. Opens, refused
//! publishes and their end, and the disk's stats every 10 s go to `disk.jsonl`.

use std::error::Error;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axon_stream::{DeliveryPolicy, DiskConfig, OsFiles, Retention, StreamKey, StreamNode};
use bytes::Bytes;
use serde_json::json;
use tokio::time::MissedTickBehavior;

use crate::Tick;
use crate::account::Log;
use crate::store::now_us;

pub const NAMES: [&str; 2] = ["s0", "s1"];
const EVERY: Duration = Duration::from_millis(50);
const PAD: usize = 2000;
const MAX_BYTES: u64 = 8 << 20;
// Small, so retention removes a segment every few seconds and a node out of inodes meets a new one within a smoke's
// 6 s episode (40 KiB/s a stream).
const SEGMENT_BYTES: u64 = 128 << 10;
// Behind within about 3 s of a stalled disk at 80 KiB/s, so a hang shows as one.
const MAX_UNWRITTEN: usize = 256 << 10;

/// The streams workload's node and incarnation (its generation count, one per start), set once it started the node.
pub static NODE: OnceLock<(StreamNode, u64)> = OnceLock::new();

pub fn namespace(origin: &str) -> String {
    format!("disk-{origin}")
}

/// Only this node's namespace: the origin keeps a stream's history, its relays only their rings.
pub fn config(me: &str) -> Result<DiskConfig, Box<dyn Error>> {
    let dir =
        std::env::var("STREAM_DISK").map_err(|_| "the disk workload needs STREAM_DISK, a directory on its disk")?;
    let retention = Retention { namespace: namespace(me), max_bytes: MAX_BYTES, max_age: Duration::from_secs(600) };
    let mut cfg = DiskConfig::new(Arc::new(OsFiles::new(dir)), vec![retention]);
    cfg.segment_bytes = SEGMENT_BYTES;
    cfg.max_unwritten_bytes = MAX_UNWRITTEN;
    Ok(cfg)
}

pub async fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn Error>> {
    let (node, inc) =
        NODE.get().cloned().ok_or("the disk workload runs after streams, which starts the stream node")?;
    let log = Arc::new(Log::create(&cx.log_dir.join("disk.jsonl"))?);
    let mut keys = Vec::new();
    for s in NAMES {
        let key = StreamKey::new(namespace(&cx.sh.node), s);
        let stored = node.stored(&key);
        // What the disk holds goes on in its generation; a stream it lost starts at this incarnation's, above all before.
        let generation = stored.map_or(inc, |c| c.generation);
        node.open_stream(&key, generation, DeliveryPolicy::ReplayRequired).await?;
        let at = node.cursor(&key).map_or(0, |c| c.sequence);
        let truncated = node.stats().disk.map(|d| d.truncated);
        log.line(
            "open",
            json!({"s": key.to_string(), "g": generation, "stored": stored.map(|c| [c.generation, c.sequence]),
                   "at": at, "inc": inc, "truncated": truncated}),
        );
        keys.push((key, generation, at));
    }
    let tick = Tick::default();
    tokio::spawn(publish(node.clone(), log.clone(), keys, inc, tick.clone()));
    tokio::spawn(report(node, log));
    Ok(tick)
}

/// `<stream> <generation> <sequence> <published µs> <incarnation> <opened after> <padding>`, each record at the
/// sequence the stream gives it: one past the last it took.
async fn publish(node: StreamNode, log: Arc<Log>, keys: Vec<(StreamKey, u64, u64)>, inc: u64, round: Tick) {
    let pad = "x".repeat(PAD);
    let mut next: Vec<u64> = keys.iter().map(|(_, _, at)| at + 1).collect();
    let mut failing = vec![crate::Failing::default(); keys.len()];
    let mut tick = tokio::time::interval(EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        round.fetch_add(1, Relaxed);
        for (i, (key, generation, at)) in keys.iter().enumerate() {
            let record = format!("{key} {generation} {} {} {inc} {at} {pad}", next[i], now_us());
            match node.publish(key, vec![Bytes::from(record)]) {
                Ok(cursor) => {
                    if cursor.sequence != next[i] {
                        log.line("publish_seq", json!({"s": key.to_string(), "want": next[i], "got": cursor.sequence}));
                    }
                    next[i] = cursor.sequence + 1;
                    if let Some(after) = failing[i].ok() {
                        log.line("publish_ok", json!({"s": key.to_string(), "after_ms": after.as_millis() as u64}));
                    }
                }
                // Once per run of failures, and its end once one is taken again.
                Err(e) => failing[i].err(&log, key, &e.error),
            }
        }
    }
}

/// The disk's stats, and `cap`, what retention bounds its bytes to: per stream its maximum and a segment being written,
/// and what waits to be written.
async fn report(node: StreamNode, log: Arc<Log>) {
    let mut tick = crate::reports();
    loop {
        tick.tick().await;
        let Some(d) = node.stats().disk else { continue };
        let cap = d.streams as u64 * (MAX_BYTES + SEGMENT_BYTES) + MAX_UNWRITTEN as u64;
        log.line(
            "disk",
            json!({"streams": d.streams, "bytes": d.bytes, "unwritten": d.unwritten_bytes, "cap": cap, "writes": d.writes,
                   "write_failures": d.write_failures, "refused": d.refused, "failing": d.failing.map(|f| format!("{f:?}")),
                   "error": d.error, "truncated": d.truncated}),
        );
    }
}
