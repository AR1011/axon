//! Two nodes in one process over TCP loopback: the round trip of small requests while 10 MB bodies go to the same node,
//! as one frame (`max_frame` raised) or in parts (`upload`) on the same lane, against none; then with the bodies in
//! parts on `Bulk`, the round trip of a `Live` request (`lanes-live-p99-bulk`) and of a raw endpoint on `Control`
//! (`lanes-control-round-bulk`). cpu is the process's user and system time over the row's, in cores (Linux only).
//! `cargo run --release -p axon-cluster --example uploads -- [seconds per row=3] [only the rows that start so]`
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::task::Poll;
use std::time::{Duration, Instant};

use axon::{Actor, ActorSystem, Context, Lane};
use axon_cluster::*;
use axon_net::SharedSecret;
use bytes::Bytes;

const BODY: usize = 10_000_000;
const PART: usize = 256 << 10;

/// Answers a plain request with its body, and an upload with the length of its body once read.
struct Echo;

impl Actor for Echo {
    type Message = (Bytes, Option<Reply>);
    type Error = ();

    fn on_message(&mut self, (body, reply): Self::Message, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let Some(reply) = reply else { return Ok(()) };
        let Some(mut upload) = reply.upload() else {
            drop(reply.send(if body.len() > 256 { Bytes::from(body.len().to_be_bytes().to_vec()) } else { body }));
            return Ok(());
        };
        let read = cx.submit_future(async move {
            let mut len = 0;
            while let Some(Ok(part)) = upload.next().await {
                len += part.len();
            }
            drop(reply.send(Bytes::from(len.to_be_bytes().to_vec())));
            (Bytes::new(), None)
        });
        read.map(drop).map_err(drop)
    }
}

/// `Echo`'s protocol, on `Bulk` or on `Live`.
struct EchoOn<const BULK: bool>;
type EchoApi = EchoOn<false>;
type BulkApi = EchoOn<true>;

impl<const BULK: bool> WireProtocol for EchoOn<BULK> {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = if BULK { 0xE3 } else { 0xE1 };
    const SCHEMA: u16 = 1;
    const LANE: Lane = if BULK { Lane::Bulk } else { Lane::Live };

    fn encode(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl<const BULK: bool> ActorProtocol for EchoOn<BULK> {
    type Mailbox = (Bytes, Option<Reply>);

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply>) -> Result<Self::Mailbox, Rejection> {
        Ok((body, reply))
    }
}

/// The raw endpoint on `Control` that `lanes-control-round-bulk` times.
const ROUND: &str = "axon.bench.round";
const ROUND_PROTOCOL: u64 = 0xE2;

/// User and system time of this process, in seconds (100 ticks a second); 0 where there is no `/proc`.
fn cpu_secs() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<&str> = stat.rsplit_once(')').map_or("", |(_, rest)| rest).split_whitespace().collect();
    let tick = |i: usize| fields.get(i).and_then(|f| f.parse::<f64>().ok()).unwrap_or(0.0);
    (tick(11) + tick(12)) / 100.0
}

/// `BODY` bytes in parts of `PART`, cut from one buffer.
struct Parts(Bytes);

impl axon::Stream for Parts {
    type Item = Result<Bytes, CallError>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let take = self.0.len().min(PART);
        Poll::Ready((take > 0).then(|| Ok(self.0.split_to(take))))
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"benchmark-secret-0123").unwrap());
    transport.allow_plaintext = true;
    // Room for one body as one frame, on both rows that send bodies.
    transport.lanes.live.limits.max_frame = 16 << 20;
    transport.lanes.live.limits.outbound_bytes = 32 << 20;
    transport.lanes.live.limits.inbound_bytes = 32 << 20;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let seconds = std::env::args().nth(1).map_or(3, |a| a.parse::<u64>().expect("a number of seconds"));
    let discovery = MemoryDiscovery::new();
    let (server_system, server) = node("server", &discovery).await;
    let (_client_system, client) = node("client", &discovery).await;
    let actor = server_system.spawn(Echo).start().unwrap();
    server.publish::<EchoApi>(&actor, "echo").unwrap();
    let bulk_actor = server_system.spawn(Echo).start().unwrap();
    server.publish::<BulkApi>(&bulk_actor, "echo-bulk").unwrap();
    server
        .endpoint(ROUND, ROUND_PROTOCOL, 1)
        .lane(Lane::Control)
        .serve(|_, body, reply| {
            if let Some(reply) = reply {
                drop(reply.send(body));
            }
            Ok(())
        })
        .unwrap();
    client.wait_view(|view| view.members.len() == 2).await.unwrap();
    let remote = client.resolve::<EchoApi>("echo").await.unwrap();
    let remote_bulk = client.resolve::<BulkApi>("echo-bulk").await.unwrap();
    let small = Bytes::from(vec![7u8; 256]);
    let body = Bytes::from(vec![9u8; BODY]);
    for _ in 0..2000 {
        remote.request(small.clone()).await.unwrap();
        client.request("server", ROUND, ROUND_PROTOCOL, 1, small.clone()).await.unwrap();
    }

    let rows = [
        "no bodies",
        "10 MB as one frame",
        "10 MB in 256 KiB parts",
        "lanes-live-p99-bulk",
        "lanes-control-round-bulk",
    ];
    let only = std::env::args().nth(2).unwrap_or_default();
    for row in rows.into_iter().filter(|row| row.starts_with(&only)) {
        let control = row == "lanes-control-round-bulk";
        let stop = Arc::new(AtomicBool::new(false));
        let sent = Arc::new(AtomicU64::new(0));
        let bulk = (row != "no bodies").then(|| {
            let (remote, remote_bulk, body) = (remote.clone(), remote_bulk.clone(), body.clone());
            let (stop, sent) = (stop.clone(), sent.clone());
            tokio::spawn(async move {
                while !stop.load(Relaxed) {
                    let answer = if row.starts_with("lanes") {
                        remote_bulk.upload(Bytes::new(), Parts(body.clone())).await
                    } else if row.contains("parts") {
                        remote.upload(Bytes::new(), Parts(body.clone())).await
                    } else {
                        remote.request(body.clone()).await
                    };
                    assert_eq!(answer.unwrap()[..], BODY.to_be_bytes());
                    sent.fetch_add(1, Relaxed);
                }
            })
        });
        let (start, cpu, mut rtts) = (Instant::now(), cpu_secs(), Vec::new());
        while start.elapsed() < Duration::from_secs(seconds) {
            let call = Instant::now();
            if control {
                client.request("server", ROUND, ROUND_PROTOCOL, 1, small.clone()).await.unwrap();
            } else {
                remote.request(small.clone()).await.unwrap();
            }
            rtts.push(call.elapsed());
        }
        let took = start.elapsed();
        let cores = (cpu_secs() - cpu) / took.as_secs_f64();
        stop.store(true, Relaxed);
        if let Some(bulk) = bulk {
            bulk.await.unwrap();
        }
        rtts.sort_unstable();
        // In µs to a tenth at any size: a printed step of 0.1 ms would move a gated p99 of about 1 ms by 8 %.
        let at = |p: f64| format!("{:.1}µs", rtts[((rtts.len() - 1) as f64 * p) as usize].as_secs_f64() * 1e6);
        let bodies = sent.load(Relaxed);
        println!(
            "{row:<24} 256 B requests: p50 {:>10}  p99 {:>10}  max {:>10}  ({} calls) ; bodies {:>6.0} MB/s ; \
             cpu {cores:.2}",
            at(0.5),
            at(0.99),
            at(1.0),
            rtts.len(),
            (bodies as usize * BODY) as f64 / 1e6 / took.as_secs_f64()
        );
    }
}
