//! Two nodes in one process over TCP loopback: request round trips, concurrent request
//! throughput, and fire-and-forget throughput.
//! `cargo run --release -p axon-cluster --example remote_calls -- [requests=400000] [tells=1000000]`
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::{Actor, ActorSystem, Context};
use axon_cluster::*;
use axon_net::SharedSecret;
use bytes::Bytes;

struct Echo(Arc<AtomicU64>);

impl Actor for Echo {
    type Message = (Bytes, Option<Reply>);
    type Error = ();

    fn on_message(&mut self, (body, reply): Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.fetch_add(1, Relaxed);
        drop(reply.map(|r| r.send(body)));
        Ok(())
    }
}

struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = 0xE0;
    const SCHEMA: u16 = 1;

    fn encode(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for EchoApi {
    type Mailbox = (Bytes, Option<Reply>);

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply>) -> Result<Self::Mailbox, Rejection> {
        Ok((body, reply))
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"benchmark-secret-0123").unwrap());
    transport.allow_plaintext = true;
    transport.lanes.live.limits.outbound_frames = 4096;
    transport.lanes.live.limits.inbound_frames = 4096;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.max_pending = 1 << 16;
    cfg.max_inbound_replies = 1 << 16;
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

async fn sequential<F: Future<Output = Result<Bytes, CallError>>>(name: &str, call: impl Fn() -> F) {
    let mut rtts = Vec::with_capacity(20_000);
    for _ in 0..20_000 {
        let start = Instant::now();
        call().await.unwrap();
        rtts.push(start.elapsed());
    }
    rtts.sort_unstable();
    let at = |p: f64| rtts[((rtts.len() - 1) as f64 * p) as usize];
    println!("sequential {name}, 256 B: p50 {:?}  p99 {:?}  p99.9 {:?}", at(0.5), at(0.99), at(0.999));
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1).map(|a| a.parse::<usize>().expect("a number"));
    let (total, tells) = (args.next().unwrap_or(400_000), args.next().unwrap_or(1_000_000) as u64);
    let discovery = MemoryDiscovery::new();
    let (server_system, server) = node("server", &discovery).await;
    let (_client_system, client) = node("client", &discovery).await;
    let handled = Arc::new(AtomicU64::new(0));
    let actor = server_system.spawn(Echo(handled.clone())).start().unwrap();
    server.publish::<EchoApi>(&actor, "echo").unwrap();
    client.wait_view(|view| view.members.len() == 2).await.unwrap();
    let remote = client.resolve::<EchoApi>("echo").await.unwrap();
    let body = Bytes::from(vec![7u8; 256]);

    for _ in 0..2000 {
        remote.request(body.clone()).await.unwrap();
    }
    sequential("request", || remote.request(body.clone())).await;
    sequential("request_within", || remote.request_within(body.clone(), Duration::from_secs(5))).await;

    for in_flight in [16usize, 256] {
        let start = Instant::now();
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..in_flight {
            let (remote, body) = (remote.clone(), body.clone());
            workers.spawn(async move {
                for _ in 0..total / in_flight {
                    remote.request(body.clone()).await.unwrap();
                }
            });
        }
        workers.join_all().await;
        println!("{in_flight:>4} concurrent requests: {:>9.0} req/s", total as f64 / start.elapsed().as_secs_f64());
    }

    // Tells are not acknowledged: a full mailbox drops them, which the server counts as rejected.
    let before = handled.load(Relaxed);
    let start = Instant::now();
    for _ in 0..tells {
        remote.tell(body.clone()).await.unwrap();
    }
    let sent = start.elapsed();
    let mut seen = 0;
    while std::mem::replace(&mut seen, handled.load(Relaxed)) != handled.load(Relaxed) || seen == before {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (delivered, dropped) = (seen - before, server.stats().engine.inbound_rejected.iter().sum::<u64>());
    println!(
        "tell, 256 B: {:>9.0} msg/s sent, {delivered} delivered, {dropped} dropped",
        tells as f64 / sent.as_secs_f64()
    );
    let stats = client.stats().engine;
    println!("calls {}  ok {}  failed {}", stats.calls, stats.succeeded, stats.calls - stats.succeeded);
}
