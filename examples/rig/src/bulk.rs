//! The `bulk` workload (spec/lanes.md § The rig (D3)): every node publishes `bulk`, a protocol on the Bulk lane, and
//! b1 uploads 10 MB bodies to every other node in 256 KiB parts, two at once per peer, while the `bulk` fault's file
//! (`BULK`) exists. The answer is the body's CRC: one that differs is a `bulk_bad` line at once. The rest is one `bulks`
//! line per 10 s in `bulk.jsonl`: `on` (whether it uploaded), `ok`, `err` (by kind in `errs`), `bytes`, `max_ms` and
//! `ok_by` per peer.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use axon::files::crc32;
use axon::{Actor, Context, Lane};
use axon_cluster::{ActorProtocol, CallError, Cluster, Rejection, Reply, WireProtocol};
use bytes::{Bytes, BytesMut};
use serde_json::json;

use crate::Tick;
use crate::account::Log;

const BODY: usize = 10_000_000;
const PART: usize = 256 << 10;
const AT_ONCE: usize = 2;

pub struct BulkApi;

impl WireProtocol for BulkApi {
    type Message = ();
    type Reply = u32;
    const ID: u64 = 0xb01c;
    const SCHEMA: u16 = 1;
    const LANE: Lane = Lane::Bulk; // rig-plant: bulk-on-live

    fn encode((): ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(body: Bytes) -> Result<u32, CallError> {
        body[..].try_into().map(u32::from_be_bytes).map_err(|_| CallError::Codec("a u32".into()))
    }
}

impl ActorProtocol for BulkApi {
    type Mailbox = Msg;

    fn encode_reply(crc: u32) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&crc.to_be_bytes()))
    }

    fn decode(_: Bytes, reply: Option<Reply<u32>>) -> Result<Msg, Rejection> {
        reply.map(Msg::Put).ok_or(Rejection::InvalidMessage)
    }
}

pub enum Msg {
    Put(Reply<u32>),
    Read,
}

struct Sink;

impl Actor for Sink {
    type Message = Msg;
    type Error = &'static str;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let Msg::Put(reply) = msg else { return Ok(()) };
        let Some(mut upload) = reply.upload() else { return Ok(()) };
        // In a job: each part read grants the caller more credit. Refused, the reply drops and the caller gets NoReply.
        let _ = cx.submit_future(async move {
            let mut body = BytesMut::with_capacity(BODY);
            while let Some(part) = upload.next().await {
                // A body past BODY is no rig upload: the reply drops, the caller gets NoReply.
                let Ok(part) = part else { return Msg::Read };
                if body.len() + part.len() > BODY {
                    return Msg::Read;
                }
                body.extend_from_slice(&part);
            }
            let _ = reply.send(crc32(&body));
            Msg::Read
        });
        Ok(())
    }
}

/// A body in parts of `PART` bytes, each a slice of it.
struct Parts(Bytes);

impl axon::Stream for Parts {
    type Item = Result<Bytes, CallError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let n = self.0.len().min(PART);
        Poll::Ready((n > 0).then(|| Ok(self.0.split_to(n))))
    }
}

#[derive(Default)]
struct Window {
    on: bool,
    ok: u64,
    err: u64,
    bytes: u64,
    max_ms: u64,
    errs: BTreeMap<String, u64>,
    ok_by: BTreeMap<String, u64>,
}

type Shared = Arc<Mutex<Window>>;

fn window(w: &Shared) -> std::sync::MutexGuard<'_, Window> {
    w.lock().expect("the window's lock is never poisoned: nothing panics holding it")
}

pub fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn Error>> {
    let sink = cx.system.spawn(Sink).name("bulk").start()?;
    cx.cluster.publish::<BulkApi>(&sink, "bulk")?;
    let log = Arc::new(Log::create(&cx.log_dir.join("bulk.jsonl"))?);
    let (tick, w, on) = (Tick::default(), Shared::default(), Arc::new(AtomicBool::new(false)));
    if cx.sh.node == "b1" {
        // One body for every upload, its parts slices of it: the answer's CRC still catches a corrupted one.
        let body = crate::blob::body(BODY);
        let want = crc32(&body);
        for to in cx.others() {
            for _ in 0..AT_ONCE {
                let up = upload(cx.cluster.clone(), to.clone(), body.clone(), want, on.clone(), log.clone(), w.clone());
                tokio::spawn(up);
            }
        }
    }
    let flag = PathBuf::from(crate::env("BULK", "/run/rig-fault/bulk"));
    let (t, w2) = (tick.clone(), w.clone());
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_secs(1));
        loop {
            every.tick().await;
            t.fetch_add(1, Relaxed);
            let up = flag.exists();
            on.store(up, Relaxed);
            window(&w2).on |= up;
        }
    });
    tokio::spawn(report(log, w));
    Ok(tick)
}

async fn upload(cluster: Cluster, to: String, body: Bytes, want: u32, on: Arc<AtomicBool>, log: Arc<Log>, w: Shared) {
    loop {
        if !on.load(Relaxed) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let at = Instant::now();
        let got = match cluster.resolve_on::<BulkApi>(&to, "bulk").await {
            Ok(remote) => remote.upload((), Parts(body.clone())).await.map_err(|e| format!("{e:?}")),
            Err(e) => Err(format!("{e:?}")),
        };
        let ms = at.elapsed().as_millis() as u64;
        let failed = got.is_err();
        {
            let mut x = window(&w);
            match got {
                Ok(crc) if crc == want => {
                    (x.ok, x.bytes, x.max_ms) = (x.ok + 1, x.bytes + BODY as u64, x.max_ms.max(ms));
                    *x.ok_by.entry(to.clone()).or_default() += 1;
                }
                Ok(crc) => log.line("bulk_bad", json!({"to": to, "bytes": BODY, "crc": crc, "want": want})),
                Err(e) => {
                    x.err += 1;
                    *x.errs.entry(e.split(['(', ' ']).next().unwrap_or("").to_owned()).or_default() += 1;
                }
            }
        }
        if failed {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn report(log: Arc<Log>, w: Shared) {
    let mut every = crate::reports();
    loop {
        every.tick().await;
        let x = std::mem::take(&mut *window(&w));
        log.line(
            "bulks",
            json!({"on": x.on, "ok": x.ok, "err": x.err, "bytes": x.bytes, "max_ms": x.max_ms, "errs": x.errs,
                   "ok_by": x.ok_by}),
        );
    }
}
