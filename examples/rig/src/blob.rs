//! Blob echo: every node publishes an echo actor as `blob`, and once a second sends each other node a body of 64 KiB to
//! 4 MiB (log-uniform), which must come back byte for byte. A reply that differs is a `blob_bad` line at once; the rest
//! is one `blobs` line per 10 s in `blob.jsonl`: `ok`, `busy` (refused before it was sent: backpressure), `err` (every
//! other failure, by kind in `errs`), `bytes`, `max_ms`, and `ok_by` per peer.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::trace::random_u64;
use axon::{Actor, Context};
use axon_cluster::{ActorProtocol, CallError, Cluster, Rejection, Reply, WireProtocol};
use axon_net::SubmitReason as Submit;
use bytes::Bytes;
use serde_json::json;
use tokio::task::JoinSet;

use crate::Tick;
use crate::account::Log;

const MIN: usize = 64 << 10;
/// The largest body; the transport's `max_frame` is raised above it (main.rs) when this workload runs.
pub const MAX: usize = 4 << 20;
const WITHIN: Duration = Duration::from_secs(5);

pub struct BlobApi;

impl WireProtocol for BlobApi {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = 0xb10b;
    const SCHEMA: u16 = 1;

    fn encode(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for BlobApi {
    type Mailbox = (Bytes, Option<Reply<Bytes>>);

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply<Bytes>>) -> Result<Self::Mailbox, Rejection> {
        Ok((body, reply))
    }
}

struct Echo;

impl Actor for Echo {
    type Message = (Bytes, Option<Reply<Bytes>>);
    type Error = &'static str;

    fn on_message(&mut self, (body, reply): Self::Message, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        if let Some(reply) = reply {
            let _ = reply.send(body); // rig-plant: blob
        }
        Ok(())
    }
}

#[derive(Default)]
struct Window {
    ok: u64,
    busy: u64,
    err: u64,
    bytes: u64,
    max_ms: u64,
    errs: BTreeMap<String, u64>,
    ok_by: BTreeMap<String, u64>,
}

pub fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn Error>> {
    let echo = cx.system.spawn(Echo).name("blob").start()?;
    cx.cluster.publish::<BlobApi>(&echo, "blob")?;
    let log = Arc::new(Log::create(&cx.log_dir.join("blob.jsonl"))?);
    let (tick, window) = (Tick::default(), Arc::new(Mutex::new(Window::default())));
    tokio::spawn(send(cx.cluster.clone(), cx.sh.node.clone(), log.clone(), window.clone(), tick.clone()));
    tokio::spawn(report(log, window));
    Ok(tick)
}

/// Bytes no codec shrinks, so each body crosses the wire at its size.
pub(crate) fn body(len: usize) -> Bytes {
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        out.extend_from_slice(&random_u64().to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

async fn send(cluster: Cluster, me: String, log: Arc<Log>, window: Arc<Mutex<Window>>, tick: Tick) {
    let mut every = tokio::time::interval(Duration::from_secs(1));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        every.tick().await;
        tick.fetch_add(1, Relaxed);
        // Bounded, with the calls' own bound, under the 10 s between two stats lines, so a round never stalls the tick.
        let Ok((refs, _)) = tokio::time::timeout(Duration::from_secs(3), cluster.resolve_all::<BlobApi>("blob")).await
        else {
            continue;
        };
        let mut calls = JoinSet::new();
        for remote in refs.into_iter().filter(|r| r.node() != me) {
            let len = (MIN as f64 * ((MAX / MIN) as f64).powf(crate::load::unit())) as usize;
            let sent = body(len);
            calls.spawn(async move {
                let at = Instant::now();
                let got = remote.request_within(sent.clone(), WITHIN).await;
                (remote.node().to_owned(), sent, got, at.elapsed().as_millis() as u64)
            });
        }
        // A call that panicked is one error, not the end of the round and of the calls still running.
        while let Some(done) = calls.join_next().await {
            tick.fetch_add(1, Relaxed);
            let mut w = window.lock().expect("the window's lock is never poisoned: nothing panics holding it");
            let Ok((to, sent, got, ms)) = done else {
                *w.errs.entry("Panicked".into()).or_default() += 1;
                w.err += 1;
                continue;
            };
            match got {
                Ok(back) if back == sent => {
                    (w.ok, w.bytes, w.max_ms) = (w.ok + 1, w.bytes + sent.len() as u64, w.max_ms.max(ms));
                    *w.ok_by.entry(to).or_default() += 1;
                }
                Ok(back) => {
                    let at =
                        sent.iter().zip(back.iter()).position(|(a, b)| a != b).unwrap_or(sent.len().min(back.len()));
                    log.line("blob_bad", json!({"to": to, "bytes": sent.len(), "back": back.len(), "first_diff": at}));
                }
                // Not Oversized: a body past the session's frame is a gap, not backpressure.
                Err(CallError::Busy | CallError::NotSent(Submit::Full | Submit::Closed | Submit::Deadline)) => {
                    w.busy += 1
                }
                Err(e) => {
                    w.err += 1;
                    let kind = format!("{e:?}");
                    *w.errs.entry(kind.split(['(', ' ']).next().unwrap_or("").to_owned()).or_default() += 1;
                }
            }
        }
    }
}

async fn report(log: Arc<Log>, window: Arc<Mutex<Window>>) {
    let mut every = crate::reports();
    loop {
        every.tick().await;
        let w = std::mem::take(&mut *window.lock().expect("the window's lock is never poisoned"));
        log.line(
            "blobs",
            json!({"ok": w.ok, "busy": w.busy, "err": w.err, "bytes": w.bytes, "max_ms": w.max_ms, "errs": w.errs,
                   "ok_by": w.ok_by}),
        );
    }
}
