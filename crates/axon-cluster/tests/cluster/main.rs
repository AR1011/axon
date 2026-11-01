#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use axon::{
    Actor, ActorSystem, Backoff, Context, Execution, MailboxLimits, Phase, PoolConfig, RestartPolicy, RuntimeConfig,
    SpanKind, StopReason, TraceContext,
};
use axon_cluster::*;
use axon_testkit::eventually;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[path = "../common/mod.rs"]
mod common;
mod discovery;
mod keyed;
mod kinds;
mod lanes;
mod leases;
mod membership;
mod parts;
mod quota;
mod remote;
mod tls;
mod trace;
mod uploads;

use common::transport;

#[derive(Clone)]
enum Op {
    Add(u64),
    Get,
    Ignore,
    Tag,
    Hold,
    Park,
    Boom,
    Fail,
}

enum Msg {
    Add(u64),
    Get(Option<Reply>),
    Ignore(Option<Reply>),
    Tag(Option<Reply>),
    Hold(Option<Reply>),
    Park(Option<Reply>),
    Fail,
}

/// Replies kept until a test lets them go.
static PARKED: std::sync::Mutex<Vec<Reply>> = std::sync::Mutex::new(Vec::new());

struct Counter {
    total: u64,
    tag: String,
}

impl Actor for Counter {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Add(n) => self.total += n,
            Msg::Get(reply) => drop(reply.map(|r| r.send(Bytes::copy_from_slice(&self.total.to_be_bytes())))),
            Msg::Ignore(reply) => drop(reply),
            Msg::Tag(reply) => drop(reply.map(|r| r.send(Bytes::from(self.tag.clone())))),
            // Never answered and never dropped: the caller waits until the session goes.
            Msg::Hold(reply) => std::mem::forget(reply),
            Msg::Park(reply) => PARKED.lock().unwrap().extend(reply),
            Msg::Fail => return Err(()),
        }
        Ok(())
    }
}

struct CounterApi;

impl WireProtocol for CounterApi {
    type Message = Op;
    type Reply = Bytes;
    const ID: u64 = 0xC0;
    const SCHEMA: u16 = 1;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        let mut out = BytesMut::new();
        match op {
            Op::Add(n) => {
                out.put_u8(0);
                out.put_u64(n);
            }
            Op::Get => out.put_u8(1),
            Op::Ignore => out.put_u8(2),
            Op::Tag => out.put_u8(3),
            Op::Hold => out.put_u8(4),
            Op::Park => out.put_u8(5),
            Op::Boom => out.put_u8(6),
            Op::Fail => out.put_u8(7),
        }
        Ok(out.freeze())
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for CounterApi {
    type Mailbox = Msg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(mut body: Bytes, reply: Option<Reply>) -> Result<Msg, Rejection> {
        match body.try_get_u8() {
            Ok(0) => body.try_get_u64().map(Msg::Add).map_err(|_| Rejection::InvalidMessage),
            Ok(1) => Ok(Msg::Get(reply)),
            Ok(2) => Ok(Msg::Ignore(reply)),
            Ok(3) => Ok(Msg::Tag(reply)),
            Ok(4) => Ok(Msg::Hold(reply)),
            Ok(5) => Ok(Msg::Park(reply)),
            Ok(6) => panic!("a decoder that panics"),
            Ok(7) => Ok(Msg::Fail),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

struct OtherApi;

impl WireProtocol for OtherApi {
    type Message = ();
    type Reply = ();
    const ID: u64 = 0xBAD;
    const SCHEMA: u16 = 1;

    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(_: Bytes) -> Result<(), CallError> {
        Ok(())
    }
}

struct Node {
    system: ActorSystem,
    cluster: Cluster,
}

async fn node_from(
    transport: axon_net::Config,
    discovery: impl Discovery,
    tweak: impl FnOnce(&mut ClusterConfig),
) -> Node {
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery);
    common::short(&mut cfg);
    tweak(&mut cfg);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    Node { system, cluster }
}

async fn node_with(id: &str, discovery: impl Discovery, tweak: impl FnOnce(&mut ClusterConfig)) -> Node {
    node_from(transport(id), discovery, tweak).await
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> Node {
    node_with(id, discovery.clone(), |_| {}).await
}

/// Seeds past their warm-up, so a test's timing starts from a working cluster.
async fn seed_servers(count: usize) -> Vec<SeedServer> {
    let bind = "127.0.0.1:0".parse().unwrap();
    let mut servers = Vec::new();
    for i in 0..count {
        let transport = transport(&format!("seed-{i}"));
        servers.push(SeedServer::bind(SeedServerConfig::new(bind, &transport)).await.unwrap());
    }
    tokio::time::sleep(servers[count - 1].stats().warm_up_left + Duration::from_millis(50)).await;
    servers
}

async fn seed_node(id: &str, seeds: &[SocketAddr], tweak: impl FnOnce(&mut ClusterConfig)) -> Node {
    let transport = transport(id);
    let discovery =
        SeedDiscovery::start(SeedConfig::new(seeds.iter().map(SocketAddr::to_string), &transport)).await.unwrap();
    let tweak = |cfg: &mut ClusterConfig| {
        // With seeds, as guide/cluster.md § Seed servers asks.
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
        tweak(cfg)
    };
    node_from(transport, discovery, tweak).await
}

/// Where a test's nodes find each other.
enum World {
    Memory(MemoryDiscovery),
    Seeds(Vec<SeedServer>),
}

impl World {
    fn memory() -> Self {
        Self::Memory(MemoryDiscovery::new())
    }

    async fn seeds() -> Self {
        Self::Seeds(seed_servers(3).await)
    }

    async fn node_with(&self, id: &str, tweak: impl FnOnce(&mut ClusterConfig)) -> Node {
        match self {
            Self::Memory(discovery) => node_with(id, discovery.clone(), tweak).await,
            Self::Seeds(servers) => {
                let seeds: Vec<_> = servers.iter().map(SeedServer::local_addr).collect();
                seed_node(id, &seeds, tweak).await
            }
        }
    }

    async fn node(&self, id: &str) -> Node {
        self.node_with(id, |_| {}).await
    }
}

async fn sees(node: &Node, members: usize) {
    eventually("membership converges", || async { node.cluster.view().members.len() == members }).await;
}

fn number(body: Bytes) -> u64 {
    u64::from_be_bytes(body[..].try_into().unwrap())
}

async fn counter_kind(node: &Node) {
    let tag = node.cluster.identity().node_id.clone();
    let build = move |cx: &SpawnContext, mut args: Bytes| {
        let total = args.try_get_u64().map_err(|_| "args must be a u64".to_string())?;
        Ok(Counter { total, tag: format!("{tag}:{}", cx.epoch.unwrap_or(0)) })
    };
    node.cluster.kind::<CounterApi>("counter").register(build).await.unwrap();
}

/// A `Counter` from `total`, tagged with the node's id, published as `name`.
fn publish_counter(node: &Node, total: u64, name: &str) -> axon::ActorHandle<Msg> {
    let tag = node.cluster.identity().node_id.to_string();
    let actor = node.system.spawn(Counter { total, tag }).start().unwrap();
    node.cluster.publish::<CounterApi>(&actor, name).unwrap();
    actor
}

/// Waits until `node` sees `n` members that host the counter kind.
async fn advertised(node: &Node, n: usize) {
    let hosts = || node.cluster.view().members.iter().filter(|m| m.record.has_kind("counter")).count();
    eventually("kinds are advertised", || async { hosts() == n }).await;
}

/// Notes the thread it first runs on, and stays in that first handler while `hold` is set.
struct Busy {
    on: Arc<std::sync::Mutex<Option<String>>>,
    hold: Arc<std::sync::atomic::AtomicBool>,
}

impl Actor for Busy {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, _: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if self.on.lock().unwrap().replace(std::thread::current().name().unwrap_or_default().into()).is_none() {
            while self.hold.load(SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        Ok(())
    }
}

/// What the node put on its timeline, oldest first.
fn told(cluster: &Cluster) -> Vec<(&'static str, String, String)> {
    cluster.events().since(0, usize::MAX).into_iter().map(|e| (e.kind, e.subject, e.detail)).collect()
}

fn seed(n: u64) -> Bytes {
    Bytes::copy_from_slice(&n.to_be_bytes())
}

/// An authority whose renewals can be made to fail, to hang as behind a partition, or to panic, without the
/// leases being lost, or to fail for the keys in `failing` only.
struct Unsteady {
    leases: MemoryAuthority,
    down: Arc<std::sync::atomic::AtomicBool>,
    when_down: Down,
    failing: Arc<parking_lot::Mutex<Vec<(String, OwnershipError)>>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Down {
    Fails,
    Hangs,
    Panics,
}

impl OwnershipAuthority for Unsteady {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        self.leases.acquire(key)
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        match (self.down.load(std::sync::atomic::Ordering::Relaxed), self.when_down) {
            (false, _) => {
                let failing = self.failing.lock().clone();
                let renewed = self.leases.renew();
                Box::pin(async move { renewed.await.map(|_| failing) })
            }
            (true, Down::Fails) => Box::pin(async { Err(OwnershipError::Unavailable("down".into())) }),
            (true, Down::Hangs) => Box::pin(std::future::pending()),
            (true, Down::Panics) => panic!("an authority that panics in renew"),
        }
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        self.leases.release(key, epoch)
    }

    /// Lists nothing, so an eager kind registers: no test here has a node go without leaving.
    fn held_by(&self, _: Holder<'_>, _: &[String]) -> Option<BoxFuture<'_, Result<Vec<String>, OwnershipError>>> {
        Some(Box::pin(async { Ok(Vec::new()) }))
    }
}

/// Carries TCP to `to`, once that is known, and can stop without closing anything, as a dead network does.
async fn relay(to: Arc<std::sync::OnceLock<SocketAddr>>, cut: Arc<std::sync::atomic::AtomicBool>) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn carry(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        cut: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let mut buf = vec![0; 16 << 10];
        loop {
            let read = from.read(&mut buf).await;
            while cut.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let Ok(n @ 1..) = read else { return };
            if to.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((near, _)) = listener.accept().await {
            while cut.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let Some(to) = to.get() else { continue };
            let Ok(far) = tokio::net::TcpStream::connect(to).await else { continue };
            let ((near_rx, near_tx), (far_rx, far_tx)) = (near.into_split(), far.into_split());
            tokio::spawn(carry(near_rx, far_tx, cut.clone()));
            tokio::spawn(carry(far_rx, near_tx, cut.clone()));
        }
    });
    addr
}
