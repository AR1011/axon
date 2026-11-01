//! The `compacted` workload, on the streams workload's hub: each node originates one compacted stream, `<node>/c0`, of
//! `KEYS` keys, publishing one keyed record every 100 ms (a removal one time in six), and logs to compacted.jsonl each
//! record the origin took with its cursor, and each it refused. It follows every other node's stream in rounds of 5 to
//! 15 s from `SnapshotThenLive`, logging the snapshot, and at the round's end the state it folded from the snapshot and
//! the records after it. check.py's `compact-fold` folds the origin's log to each logged cursor and compares;
//! `compact-origin` holds the origin to its limits.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon::trace::random_u64;
use axon_stream::{
    CompactLimits, Cursor, DeliveryPolicy, StartPosition, StreamError, StreamEvent, StreamKey, StreamNode,
    SubscribeOptions, keyed, split_keyed,
};
use bytes::Bytes;
use serde_json::json;
use tokio::time::MissedTickBehavior;

use crate::Tick;
use crate::account::Log;

const EVERY: Duration = Duration::from_millis(100);
/// More keys than `LIMITS` holds, so the origin refuses a new key while it is full.
const KEYS: u64 = 40;
const LIMITS: CompactLimits = CompactLimits { max_keys: 32, max_bytes: 4096 };

pub async fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn std::error::Error>> {
    let node = crate::streams::NODE.get().cloned().ok_or("compacted runs on the streams hub: list streams first")?;
    let dir = cx.log_dir.join("compacted");
    std::fs::create_dir_all(&dir)?;
    let generation = crate::streams::next_generation(&dir)?;
    let log = Arc::new(Log::create(&cx.log_dir.join("compacted.jsonl"))?);
    let me = cx.sh.node.as_str();
    let key = StreamKey::new(me, "c0");
    node.open_stream(&key, generation, DeliveryPolicy::Compacted(LIMITS)).await?;
    log.line("open", json!({"s": key.to_string(), "g": generation, "max_keys": LIMITS.max_keys}));
    let tick = Tick::default();
    tokio::spawn(publish(node.clone(), log.clone(), key, tick.clone()));
    for origin in cx.others() {
        tokio::spawn(follow(node.clone(), log.clone(), StreamKey::new(origin.as_str(), "c0")));
    }
    Ok(tick)
}

async fn publish(node: StreamNode, log: Arc<Log>, key: StreamKey, round: Tick) {
    let (s, mut n, mut failing) = (key.to_string(), 0u64, crate::Failing::default());
    let mut tick = tokio::time::interval(EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        round.fetch_add(1, Relaxed);
        let k = format!("K{}", random_u64() % KEYS);
        let v = match random_u64() % 6 {
            0 => String::new(),
            _ => {
                n += 1;
                format!("v{n}")
            }
        };
        match node.publish(&key, vec![keyed(k.as_bytes(), v.as_bytes())]) {
            Ok(c) => {
                failing.ok();
                log.line("pub", json!({"s": s, "g": c.generation, "seq": c.sequence, "k": k, "v": v}));
            }
            Err(e) if matches!(e.error, StreamError::StateFull) => log.line("full", json!({"s": s, "k": k})),
            Err(e) => failing.err(&log, &s, &e.error),
        }
    }
}

fn apply(table: &mut BTreeMap<String, String>, records: &[Bytes]) {
    for r in records {
        let Some((k, v)) = split_keyed(r) else { continue };
        let k = String::from_utf8_lossy(&k).into_owned();
        match v.is_empty() {
            true => table.remove(&k),
            false => table.insert(k, String::from_utf8_lossy(&v).into_owned()),
        };
    }
}

/// One round: the snapshot, then every record after it; its fold logged at the end, or where the records move to
/// another generation, after which it folds nothing until the next snapshot.
async fn follow(node: StreamNode, log: Arc<Log>, key: StreamKey) {
    let s = key.to_string();
    loop {
        let until = tokio::time::Instant::now() + Duration::from_secs(5 + random_u64() % 11);
        let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
        let mut state: Option<(Cursor, BTreeMap<String, String>)> = None;
        let fold = |state: &mut Option<(Cursor, BTreeMap<String, String>)>| {
            if let Some((at, table)) = state.take() {
                log.line("fold", json!({"s": s, "g": at.generation, "through": at.sequence, "state": table}));
            }
        };
        match node.subscribe(&key, options) {
            Err(e) => log.line("subscribe_err", json!({"s": s, "err": format!("{e:?}")})),
            Ok(mut sub) => loop {
                let Ok(Some(event)) = tokio::time::timeout_at(until, sub.next()).await else { break };
                match event {
                    StreamEvent::Snapshot(snap) => {
                        fold(&mut state); // a new generation's snapshot ends what the last one folded
                        let mut table = BTreeMap::new();
                        apply(&mut table, snap.records());
                        let at = snap.through();
                        log.line("snap", json!({"s": s, "g": at.generation, "through": at.sequence, "state": table}));
                        state = Some((at, table));
                    }
                    StreamEvent::Batch(batch) => match &mut state {
                        Some((at, _)) if at.generation != batch.cursor().generation => fold(&mut state),
                        Some((at, table)) => {
                            apply(table, batch.records());
                            *at = batch.cursor();
                        }
                        None => {}
                    },
                    // As src/streams.rs logs them, so check.py judges both alike.
                    StreamEvent::ResetRequired(why) => {
                        log.line("reset", json!({"s": s, "why": format!("{why:?}")}));
                        break;
                    }
                    StreamEvent::End(why) => {
                        log.line("reset", json!({"s": s, "why": format!("{why:?}")}));
                        break;
                    }
                    _ => {}
                }
            },
        }
        fold(&mut state);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
