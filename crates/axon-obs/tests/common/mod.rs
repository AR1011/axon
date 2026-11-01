#![allow(dead_code, reason = "each obs test file uses part of it")]
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::time::Duration;

use axon::{Actor, ActorSystem, ActorSystemBuilder, Context, ExtensionFuture, SystemExtension};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, Discovery, Placement, Rejection, Reply, Unaddressed, WireProtocol,
};
use axon_net::SharedSecret;
use axon_obs::{ClusterEvent, MetricsFuture, MetricsSink, OWN_SEQUENCE, SampleBatch};
use bytes::Bytes;
use parking_lot::Mutex;

/// A plaintext node on loopback, sharing every test node's secret.
pub async fn loopback(
    id: &str,
    discovery: impl Discovery,
    system: ActorSystemBuilder,
    tweak: impl FnOnce(&mut ClusterConfig),
) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"0123456789abcdef0123").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery);
    // Short, so these threaded tests run in seconds; axon-stream's world.rs runs the shipped timings.
    cfg.poll_interval = Duration::from_millis(40);
    tweak(&mut cfg);
    let system = system.build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

/// A keyed actor that does nothing, and the protocol of its kind: for tests that only need keys to run.
pub struct Quiet;

impl Actor for Quiet {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

impl WireProtocol for Quiet {
    type Message = ();
    type Reply = ();
    const ID: u64 = 1;
    const SCHEMA: u16 = 1;

    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(_: Bytes) -> Result<(), CallError> {
        Ok(())
    }
}

impl ActorProtocol for Quiet {
    type Mailbox = ();

    fn encode_reply(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode(_: Bytes, _: Option<Reply<()>>) -> Result<(), Rejection> {
        Ok(())
    }
}

/// Registers `kind` and starts each of `keys` here.
pub async fn keyed(cluster: &Cluster, kind: &str, keys: impl IntoIterator<Item = String>) {
    cluster.kind::<Quiet>(kind).register(|_, _| Ok(Quiet)).await.unwrap();
    for key in keys {
        cluster.get_or_spawn::<Unaddressed>(kind, &key, Bytes::new(), &Placement::default()).await.unwrap();
    }
}

/// Keeps what it is given and can say how far it got, as a store that sums would: a batch given twice counts twice.
/// The first `lost_replies` writes are kept and then reported as failed, which is what a reply lost on the way back
/// looks like.
#[derive(Default)]
pub struct Kept {
    pub lost_replies: AtomicU32,
    pub stored: Mutex<Vec<(String, String, u64)>>,
}

impl MetricsSink for Kept {
    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        let theirs = batches.iter().filter(|b| b.sequence < OWN_SEQUENCE);
        self.stored.lock().extend(theirs.map(|b| (b.node_id.clone(), b.incarnation.clone(), b.sequence)));
        let lost = !batches.is_empty() && self.lost_replies.try_update(Relaxed, Relaxed, |n| n.checked_sub(1)).is_ok();
        Box::pin(async move { if lost { Err("the reply was lost".into()) } else { Ok(()) } })
    }

    fn stored_through(&self, node: &str, incarnation: &str) -> MetricsFuture<'_, Option<u64>> {
        let held = self.of(node, Some(incarnation)).into_iter().max();
        Box::pin(async move { Ok(held) })
    }

    fn write_events(&self, _: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl Kept {
    /// The sequences of `node`, of its boot `incarnation` or of every boot, in the order stored.
    pub fn of(&self, node: &str, incarnation: Option<&str>) -> Vec<u64> {
        let stored = self.stored.lock();
        let of = stored.iter().filter(|(n, i, _)| n == node && incarnation.is_none_or(|incarnation| i == incarnation));
        of.map(|(.., s)| *s).collect()
    }

    /// Every batch from the first stored on, each once.
    pub fn once_each(&self, node: &str, incarnation: Option<&str>) -> Vec<u64> {
        let stored = self.of(node, incarnation);
        let first = stored.first().copied().unwrap_or(1);
        assert_eq!(stored, (first..first + stored.len() as u64).collect::<Vec<_>>(), "a batch came twice or never");
        stored
    }
}

/// Binds the ports it is given in its shutdown hook, which runs after those of every extension attached after it.
#[derive(Default)]
pub struct Rebind(pub Mutex<Vec<SocketAddr>>);

impl SystemExtension for Rebind {
    fn name(&self) -> &str {
        "rebind"
    }

    fn shutdown(&self, _: Duration) -> ExtensionFuture {
        let bind = |at: &SocketAddr| std::net::TcpListener::bind(at).map(drop).map_err(|e| format!("{at}: {e}"));
        Box::pin(std::future::ready(self.0.lock().iter().try_for_each(bind)))
    }
}
