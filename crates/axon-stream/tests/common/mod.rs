#![allow(dead_code, reason = "each stream test file uses part of it")]
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    Action, ActorProtocol, Authorizer, CallError, Cluster, ClusterConfig, PaxosConfig, PaxosLeases, Rejection, Reply,
    Retry, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig, WireProtocol,
};
use axon_net::{Backoff, Peer};
use axon_stream::{StreamKey, StreamNodeConfig};
use axon_testkit::world::Host;
use bytes::{Buf, Bytes};

pub const SECRET: &str = "at-least-16-bytes-long";

pub fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

pub fn role(role: &str) -> BTreeMap<String, String> {
    labels(&[("role", role)])
}

pub fn key() -> StreamKey {
    StreamKey::new("md", "btc")
}

pub fn short_cluster(cfg: &mut ClusterConfig) {
    // Short, so these tests run in seconds, or a World seed in moments of virtual time; world.rs runs the shipped
    // timings.
    cfg.poll_interval = Duration::from_millis(40);
    cfg.backoff = Backoff { initial: Duration::from_millis(20), max: Duration::from_millis(100), jitter: 0.1 };
    cfg.request_timeout = Duration::from_secs(1);
}

pub fn short_stream(cfg: &mut StreamNodeConfig) {
    // Short, as in `short_cluster`.
    cfg.tick = Duration::from_millis(25);
    cfg.downstream_expiry = Duration::from_secs(2);
    cfg.core.timings.hold_down = Duration::from_millis(400);
    cfg.core.refresh = Duration::from_millis(300);
    cfg.core.retry_failed = Duration::from_millis(50);
    cfg.core.retry_refused = Duration::from_millis(200);
}

/// Refuses one node the stream endpoints, as an operator revoking it would.
pub struct NoStreams(pub &'static str);

impl Authorizer for NoStreams {
    fn authorize(&self, peer: &Peer, action: Action<'_>) -> bool {
        let stream = |name: &str| name == "axon.stream" || name.starts_with("axon.stream.");
        *peer.identity.node_id != *self.0 || !matches!(action, Action::Call { name } if stream(name))
    }
}

pub type Call = (u64, Option<Reply<u64>>);

/// Answers each call with its own number.
pub struct Echo;

impl Actor for Echo {
    type Message = Call;
    type Error = ();

    fn on_message(&mut self, (id, reply): Call, _: &mut Context<'_, Self>) -> Result<(), ()> {
        drop(reply.map(|r| r.send(id)));
        Ok(())
    }
}

/// The protocol of `Echo` as a keyed kind.
pub struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = u64;
    type Reply = u64;
    const ID: u64 = 0xEC;
    const SCHEMA: u16 = 1;

    fn encode(id: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&id.to_be_bytes()))
    }

    fn decode_reply(mut body: Bytes) -> Result<u64, CallError> {
        body.try_get_u64().map_err(|_| CallError::Codec("short reply".into()))
    }
}

impl ActorProtocol for EchoApi {
    type Mailbox = Call;

    fn encode_reply(id: u64) -> Result<Bytes, CallError> {
        Self::encode(id)
    }

    fn decode(mut body: Bytes, reply: Option<Reply<u64>>) -> Result<Call, Rejection> {
        body.try_get_u64().map(|id| (id, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

/// The keyed actors every node calls.
pub fn keys() -> Vec<String> {
    (0..12).map(|k| format!("k{k}")).collect()
}

pub const SEED_PORT: u16 = 7001;

/// A node of `nodes`, every one a seed and a `PaxosLeases` acceptor, labelled with its `role`, with the heartbeats
/// guide/cluster.md § Seed servers asks for and nothing else set.
pub async fn seeded(
    host: &Host,
    system: &ActorSystem,
    transport: &axon_net::Config,
    nodes: &[&str],
    role: &str,
) -> (Cluster, Arc<SeedServer>) {
    let at = SocketAddr::new(host.ip(), SEED_PORT);
    let seed = SeedServer::bind(SeedServerConfig::new(at, transport)).await.unwrap();
    let entries: Vec<_> = nodes.iter().map(|n| SocketAddr::new(host.ip_of(n), SEED_PORT).to_string()).collect();
    loop {
        let discovery = SeedDiscovery::start(SeedConfig::new(entries.clone(), transport)).await.unwrap();
        let mut cfg = ClusterConfig::new(host.addr(), transport.clone(), discovery);
        cfg.labels = self::role(role);
        let mut paxos = PaxosConfig::new(nodes.iter().copied());
        paxos.store = Some(Arc::new(host.disk().dir("leases")));
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));
        // With seeds, as guide/cluster.md § Seed servers asks.
        cfg.heartbeat_interval = Duration::from_millis(500);
        cfg.heartbeat_grace = Duration::from_secs(1);
        match Cluster::start(system, cfg).await {
            // After a wall step back this boot is older than the one the seeds still list: `Stale` until they drop it.
            Err(e) if e.retry() == Retry::Later => tokio::time::sleep(Duration::from_secs(1)).await,
            started => return (started.unwrap(), Arc::new(seed)),
        }
    }
}
