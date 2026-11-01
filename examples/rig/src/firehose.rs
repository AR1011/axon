//! The `firehose` workload (spec/push-window.md § The rig (D3)), after `streams`, whose node it uses: b1 originates
//! `firehose/f0` on the Bulk lane and publishes `FIREHOSE_MBS` (13) MB/s into it in 4 KiB records, each stamped with
//! its publish time; b2 and b3 follow it `Live`. To `firehose.jsonl`, each second: b1 a `pub` line (`bytes`,
//! `records`), each follower a `recv` line (`bytes`, `records`, `lag_ms`, the largest lag behind a record's publish
//! time, `last` its last cursor), and every node a `window` line, each push channel's `DownstreamStats` and the node's
//! `push_inflight_bytes` and `push_resent`. A follower's end or reset is a `reset` line; a batch that skips records of
//! its generation a `gap` line, which check.py's firehose-gap fails; each `Ready`, `OriginLost` and `Replacement` a line.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use axon::Lane;
use axon_stream::{
    DeliveryPolicy, RingLimits, StartPosition, StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions,
};
use bytes::{BufMut, BytesMut};
use serde_json::json;

use crate::Tick;
use crate::account::Log;
use crate::store::now_us;

const NS: &str = "firehose";
const ORIGIN: &str = "b1";
const FOLLOWERS: [&str; 2] = ["b2", "b3"];
const RECORD: usize = 4 << 10;
const EVERY: Duration = Duration::from_millis(10);

/// The namespace on Bulk, a ring of about 5 s of it, and its followers' parent the origin. `FIREHOSE_BRANCH_MB` sets
/// every branch's bytes (the default 32 MiB), which a push holds half of for one stream.
pub fn config(cfg: StreamNodeConfig, me: &str) -> StreamNodeConfig {
    let mut cfg = cfg.lane(NS, Lane::Bulk);
    cfg.core.rings.push(RingLimits { namespace: NS.into(), max_records: 1 << 16, max_bytes: 64 << 20 });
    if let Ok(mb) = crate::env("FIREHOSE_BRANCH_MB", "").parse::<usize>() {
        cfg.core.branch.max_bytes = mb << 20;
    }
    if me == ORIGIN { cfg } else { cfg.upstream_for(NS, vec![BTreeMap::from([("node".into(), ORIGIN.into())])]) }
}

pub async fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn Error>> {
    let node = crate::streams::NODE.get().ok_or("firehose runs after streams")?.clone();
    let log = Arc::new(Log::create(&cx.log_dir.join("firehose.jsonl"))?);
    let (me, tick, key) = (cx.sh.node.as_str(), Tick::default(), StreamKey::new(NS, "f0"));
    if me == ORIGIN {
        let generation = crate::streams::next_generation(cx.log_dir)?;
        node.open_stream(&key, generation, DeliveryPolicy::ReplayRequired).await?;
        log.line("open", json!({"s": key.to_string(), "g": generation}));
        let mbs: f64 = crate::env("FIREHOSE_MBS", "13").parse()?;
        tokio::spawn(publish(node.clone(), key, mbs * 1e6, log.clone(), tick.clone()));
    } else if FOLLOWERS.contains(&me) {
        tokio::spawn(follow(node.clone(), key, log.clone(), tick.clone()));
    }
    tokio::spawn(windows(node, log, tick.clone()));
    Ok(tick)
}

async fn publish(node: StreamNode, key: StreamKey, rate: f64, log: Arc<Log>, tick: Tick) {
    let pad = crate::blob::body(RECORD);
    let (start, mut sent, mut second) = (Instant::now(), 0f64, (0u64, 0u64));
    let mut failing = crate::Failing::default();
    let mut every = tokio::time::interval(EVERY);
    let mut report = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = every.tick() => {
                tick.fetch_add(1, Relaxed);
                // A stall is not made up past a second's worth, and a batch stays under the hub's 512 KiB.
                let due = rate * start.elapsed().as_secs_f64();
                sent = sent.max(due - rate);
                let n = (((due - sent) / RECORD as f64) as usize).min(64);
                if n == 0 {
                    continue;
                }
                let at = now_us();
                let records = (0..n).map(|_| {
                    let mut r = BytesMut::with_capacity(RECORD);
                    r.put_u64(at);
                    r.put_slice(&pad[8..]);
                    r.freeze()
                });
                match node.publish(&key, records.collect()) {
                    Ok(_) => {
                        failing.ok();
                        second = (second.0 + (n * RECORD) as u64, second.1 + n as u64);
                    }
                    Err(e) => failing.err(&log, &key, &e.error),
                }
                sent += (n * RECORD) as f64;
            }
            _ = report.tick() => {
                log.line("pub", json!({"s": key.to_string(), "bytes": second.0, "records": second.1}));
                second = (0, 0);
            }
        }
    }
}

/// Within one subscription a `gap` line for each batch that does not follow the last record of its generation (`after`
/// the record it follows, `first` its own, and the events since that record), and a line for every `Ready`
/// (`start`), `OriginLost` and `Replacement`: a `Live` follower skips nothing short of a reset.
async fn follow(node: StreamNode, key: StreamKey, log: Arc<Log>, tick: Tick) {
    let (s, mut second, mut lag_us) = (key.to_string(), (0u64, 0u64), 0u64);
    let mut report = tokio::time::interval(Duration::from_secs(1));
    loop {
        let mut sub = match node.subscribe(&key, SubscribeOptions { start: StartPosition::Live, ..Default::default() })
        {
            Ok(sub) => sub,
            Err(e) => {
                log.line("subscribe_err", json!({"s": s, "err": format!("{e:?}")}));
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        // The last record of this subscription, and what came since it other than a batch.
        let (mut last, mut since) = (None::<(u64, u64)>, Vec::<&str>::new());
        loop {
            tokio::select! {
                event = sub.next() => match event {
                    Some(StreamEvent::Batch(batch)) => {
                        let now = now_us();
                        let (g, first) = (batch.generation(), batch.first());
                        if let Some((lg, ls)) = last.filter(|&(lg, ls)| lg == g && first != ls + 1) {
                            log.line("gap", json!({"s": s, "g": lg, "after": ls, "first": first, "since": since}));
                        }
                        (last, since) = (Some((g, batch.last())), Vec::new());
                        for r in batch.records() {
                            second = (second.0 + r.len() as u64, second.1 + 1);
                            let at = r.get(..8).and_then(|b| b.try_into().ok()).map_or(now, u64::from_be_bytes);
                            lag_us = lag_us.max(now.saturating_sub(at));
                        }
                    }
                    Some(StreamEvent::End(why)) => {
                        log.line("reset", json!({"s": s, "why": format!("{why:?}")}));
                        break;
                    }
                    Some(StreamEvent::ResetRequired(why)) => {
                        log.line("reset", json!({"s": s, "why": format!("{why:?}")}))
                    }
                    Some(StreamEvent::Ready { start }) => {
                        since.push("ready");
                        let start = start.map(|c| [c.generation, c.sequence]);
                        log.line("ready", json!({"s": s, "start": start, "last": last.map(|(g, q)| [g, q])}));
                    }
                    Some(StreamEvent::OriginLost) => {
                        since.push("origin_lost");
                        log.line("origin_lost", json!({"s": s, "last": last.map(|(g, q)| [g, q])}));
                    }
                    Some(StreamEvent::Replacement { batch }) => {
                        since.push("replacement");
                        log.line("replacement", json!({"s": s, "g": batch.generation(), "first": batch.first(),
                                                       "last": batch.last()}));
                        last = Some((batch.generation(), batch.last()));
                    }
                    Some(_) => {}
                    None => break,
                },
                _ = report.tick() => {
                    tick.fetch_add(1, Relaxed);
                    log.line("recv", json!({"s": s, "bytes": second.0, "records": second.1, "lag_ms": lag_us / 1000,
                                            "last": last.map(|(g, q)| [g, q])}));
                    (second, lag_us) = ((0, 0), 0);
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn windows(node: StreamNode, log: Arc<Log>, tick: Tick) {
    let mut every = tokio::time::interval(Duration::from_secs(1));
    loop {
        every.tick().await;
        tick.fetch_add(1, Relaxed);
        let st = node.stats();
        let down: Vec<_> = st
            .downstream
            .iter()
            .map(|d| {
                json!({"node": d.node, "lane": format!("{:?}", d.lane), "channel": d.channel,
                       "window": d.window_bytes, "inflight": d.inflight_bytes, "pushes": d.inflight_pushes,
                       "resent": d.resent, "rtt_us": d.round_trip_us, "rate": d.delivery_rate})
            })
            .collect();
        let inflight: usize = st.downstream.iter().map(|d| d.inflight_bytes).sum();
        log.line("window", json!({"downstream": down, "push_inflight_bytes": inflight, "push_resent": st.push_resent}));
    }
}
