use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use parking_lot::Mutex;

use crate::DiscoveryError;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub const LIFECYCLE: &str = "axon.lifecycle";
pub const DRAINING: &str = "draining";
pub const LEAVE_REASON: &str = "axon.leave.reason";
pub const KINDS: &str = "axon.kinds";

/// Metadata bounds are Consul's, so a record valid here is valid in every provider.
pub(crate) const MAX_METADATA: usize = 60;
pub(crate) const MAX_KEY: usize = 60;
pub(crate) const MAX_VALUE: usize = 512;

#[derive(Clone, Debug, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct NodeRecord {
    pub id: String,
    /// Boot identity, newer boots compare greater. `0` means unknown (static seeds): the
    /// incarnation is then learned from the handshake.
    #[cfg_attr(feature = "serde", serde(serialize_with = "axon::ser::hex"))]
    pub incarnation: u128,
    pub address: SocketAddr,
    pub metadata: BTreeMap<String, String>,
    /// Set by discovery: moves whenever the registration is made, changed or regains health, the
    /// same for every observer. `0` when the provider has no such thing. Not part of equality.
    pub version: u64,
}

impl PartialEq for NodeRecord {
    fn eq(&self, other: &Self) -> bool {
        (&self.id, self.incarnation, self.address, &self.metadata)
            == (&other.id, other.incarnation, other.address, &other.metadata)
    }
}

impl NodeRecord {
    /// What tells one boot of a node from another, and from the same boot at another address.
    pub(crate) fn boot(&self) -> (SocketAddr, u128) {
        (self.address, self.incarnation)
    }

    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if !axon::valid_text(&self.id, axon::MAX_NAME) {
            return Err(DiscoveryError::Invalid("node id"));
        }
        if self.address.port() == 0 {
            return Err(DiscoveryError::Invalid("port"));
        }
        if self.metadata.len() > MAX_METADATA {
            return Err(DiscoveryError::Invalid("too many metadata entries"));
        }
        if !self.metadata.iter().all(|(k, v)| axon::valid_text(k, MAX_KEY) && v.len() <= MAX_VALUE) {
            return Err(DiscoveryError::Invalid("metadata entry"));
        }
        Ok(())
    }

    /// One of the node's labels, from [`ClusterConfig::labels`](crate::ClusterConfig::labels) or
    /// [`Cluster::set_metadata`](crate::Cluster::set_metadata): what a [`Placement`](crate::Placement) matches. They
    /// share `metadata` with the cluster's own entries, whose keys start with `axon.`.
    pub fn label(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(String::as_str)
    }

    /// Whether the node carries every one of `labels`, each value matched whole.
    pub fn has_labels(&self, labels: &BTreeMap<String, String>) -> bool {
        labels.iter().all(|(k, v)| self.label(k) == Some(v.as_str()))
    }

    pub fn draining(&self) -> bool {
        self.metadata.get(LIFECYCLE).is_some_and(|v| v == DRAINING)
    }

    pub fn has_kind(&self, kind: &str) -> bool {
        self.kind_schemas(kind).is_some()
    }

    /// The `SCHEMA`s the node serves `kind` at, from `kind/2+1` in its [`KINDS`]: first the one its own callers use,
    /// the `P` of its `cluster.kind::<P>`, then each it serves `also`. `None` when it does not advertise the kind.
    pub fn kind_schemas(&self, kind: &str) -> Option<Vec<u16>> {
        let kinds = self.metadata.get(KINDS)?;
        let (_, schemas) = kinds.split(',').map(|k| k.split_once('/').unwrap_or((k, ""))).find(|(k, _)| *k == kind)?;
        Some(schemas.split('+').filter_map(|s| s.parse().ok()).collect())
    }
}

/// What a discovery provider or an ownership authority says about itself, for consoles.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ProviderInfo {
    /// `static`, `memory`, `seed` or `consul`; a custom provider is named after its type.
    pub kind: String,
    /// What it was configured with, in reading order.
    pub settings: Vec<(String, String)>,
    /// Live figures, in reading order.
    pub stats: Vec<(String, String)>,
    /// The endpoints it keeps a session to.
    pub links: Vec<ProviderLink>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ProviderLink {
    pub target: String,
    /// The identity the endpoint answered with.
    pub identity: Option<String>,
    pub state: String,
    #[cfg_attr(feature = "serde", serde(rename = "since_ns", serialize_with = "axon::ser::ns"))]
    pub since: Duration,
    pub error: Option<String>,
}

impl ProviderInfo {
    pub fn new(kind: impl Into<String>) -> Self {
        Self { kind: kind.into(), ..Self::default() }
    }

    /// Named after a type: `my_crate::EtcdDiscovery` becomes `EtcdDiscovery`.
    pub fn of<T: ?Sized>() -> Self {
        let name = std::any::type_name::<T>();
        let name = name.split('<').next().unwrap_or(name);
        Self::new(name.rsplit("::").next().unwrap_or("custom"))
    }

    pub fn setting(mut self, key: &str, value: impl ToString) -> Self {
        self.settings.push((key.to_owned(), value.to_string()));
        self
    }

    pub fn stat(mut self, key: &str, value: impl ToString) -> Self {
        self.stats.push((key.to_owned(), value.to_string()));
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// Provider-local change counter; going backwards means the provider was reset.
    pub revision: u64,
    pub nodes: Vec<NodeRecord>,
}

/// Where nodes find each other. Discovery never confers ownership of anything.
pub trait Discovery: Send + Sync + 'static {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>>;

    /// Upsert. A record with an older incarnation than the registered one is `Stale`.
    fn register(&self, _record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        Box::pin(std::future::ready(Err(DiscoveryError::ReadOnly)))
    }

    fn unregister(&self, _id: &str, _incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        Box::pin(std::future::ready(Err(DiscoveryError::ReadOnly)))
    }

    fn heartbeat(&self, _id: &str, _incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// A snapshot newer than revision `after`, waiting for one if there is none yet; it may also answer with
    /// an unchanged one after a while. `None` when the provider can only be polled. A cluster whose provider
    /// watches polls every `resync_interval` instead of every `poll_interval`.
    fn watch(&self, _after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        None
    }

    /// How soon a peer can drop a node that went quiet from this provider's view, where the provider does that
    /// itself: without an ownership authority, `Cluster::start` refuses heartbeats that would isolate the node later.
    fn drops_after(&self) -> Option<Duration> {
        None
    }

    /// The node's event log, from `Cluster::start`.
    fn attach(&self, _events: &Arc<axon::EventLog>) {}

    fn describe(&self) -> ProviderInfo {
        ProviderInfo::of::<Self>()
    }
}

/// A provider whose call or future panics answers `Unavailable` instead: the cluster's own tasks call it, and a
/// panic there would end its polling, heartbeats or renewals for good.
pub(crate) struct Caught<P: ?Sized>(pub Arc<P>);

pub(crate) fn caught<'a, T: Send + 'a, E: Send + 'a>(
    panicked: fn() -> E,
    call: impl FnOnce() -> BoxFuture<'a, Result<T, E>>,
) -> BoxFuture<'a, Result<T, E>> {
    let Ok(mut call) = catch_unwind(AssertUnwindSafe(call)) else {
        return Box::pin(std::future::ready(Err(panicked())));
    };
    Box::pin(std::future::poll_fn(move |cx| {
        catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(cx))).unwrap_or_else(|_| Poll::Ready(Err(panicked())))
    }))
}

/// [`caught`] for a call that may have no future to give.
pub(crate) fn caught_some<'a, T: Send + 'a, E: Send + 'a>(
    panicked: fn() -> E,
    call: impl FnOnce() -> Option<BoxFuture<'a, Result<T, E>>>,
) -> Option<BoxFuture<'a, Result<T, E>>> {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(future) => future.map(|f| caught(panicked, || f)),
        Err(_) => Some(Box::pin(std::future::ready(Err(panicked())))),
    }
}

fn panicked() -> DiscoveryError {
    DiscoveryError::Unavailable("the provider panicked".into())
}

impl Discovery for Caught<dyn Discovery> {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        caught(panicked, || self.0.snapshot())
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        caught(panicked, || self.0.register(record))
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        caught(panicked, || self.0.unregister(id, incarnation))
    }

    fn heartbeat(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        caught(panicked, || self.0.heartbeat(id, incarnation))
    }

    fn watch(&self, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        caught_some(panicked, || self.0.watch(after))
    }

    fn drops_after(&self) -> Option<Duration> {
        self.0.drops_after()
    }

    fn attach(&self, events: &Arc<axon::EventLog>) {
        self.0.attach(events)
    }

    fn describe(&self) -> ProviderInfo {
        self.0.describe()
    }
}

/// Fixed seed list. Seeds usually carry incarnation `0`.
pub struct StaticDiscovery(Vec<NodeRecord>);

impl StaticDiscovery {
    pub fn new(mut nodes: Vec<NodeRecord>) -> Result<Self, DiscoveryError> {
        check_nodes(&mut nodes)?;
        Ok(Self(nodes))
    }
}

/// Each record valid, sorted by id, and no id twice.
pub(crate) fn check_nodes(nodes: &mut [NodeRecord]) -> Result<(), DiscoveryError> {
    nodes.iter().try_for_each(NodeRecord::validate)?;
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    if nodes.windows(2).any(|w| w[0].id == w[1].id) {
        return Err(DiscoveryError::Invalid("duplicate node id"));
    }
    Ok(())
}

impl Discovery for StaticDiscovery {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        Box::pin(std::future::ready(Ok(Snapshot { revision: 1, nodes: self.0.clone() })))
    }

    fn describe(&self) -> ProviderInfo {
        let seeds: Vec<String> = self.0.iter().map(|n| format!("{}@{}", n.id, n.address)).collect();
        ProviderInfo::new("static").setting("seeds", seeds.join(", ")).stat("nodes", self.0.len())
    }
}

/// Shared in-process registry for tests and single-process clusters.
#[derive(Clone, Default)]
pub struct MemoryDiscovery(Arc<Mutex<(u64, BTreeMap<String, NodeRecord>)>>);

impl MemoryDiscovery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes a node regardless of incarnation, as an expired health check would.
    pub fn expire(&self, id: &str) {
        let mut state = self.0.lock();
        if state.1.remove(id).is_some() {
            state.0 += 1;
        }
    }
}

impl Discovery for MemoryDiscovery {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        let state = self.0.lock();
        let snapshot = Snapshot { revision: state.0, nodes: state.1.values().cloned().collect() };
        Box::pin(std::future::ready(Ok(snapshot)))
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let result = record.validate().and_then(|()| {
            let mut state = self.0.lock();
            match state.1.get(&record.id) {
                Some(current) if current.incarnation > record.incarnation => Err(DiscoveryError::Stale),
                Some(current) if current == record => Ok(()),
                _ => {
                    state.0 += 1;
                    let version = state.0;
                    state.1.insert(record.id.clone(), NodeRecord { version, ..record.clone() });
                    Ok(())
                }
            }
        });
        Box::pin(std::future::ready(result))
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let mut state = self.0.lock();
        if state.1.get(id).is_some_and(|r| r.incarnation == incarnation) {
            state.1.remove(id);
            state.0 += 1;
        }
        Box::pin(std::future::ready(Ok(())))
    }

    fn describe(&self) -> ProviderInfo {
        let state = self.0.lock();
        ProviderInfo::new("memory").stat("nodes", state.1.len()).stat("revision", state.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Etcd;
    impl Discovery for Etcd {
        fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
            unreachable!()
        }
    }
    struct Cached<D>(D);
    impl<D: Discovery> Discovery for Cached<D> {
        fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
            self.0.snapshot()
        }
    }

    fn record(id: &str) -> NodeRecord {
        NodeRecord {
            id: id.into(),
            incarnation: 1,
            address: "127.0.0.1:9".parse().unwrap(),
            metadata: BTreeMap::new(),
            version: 0,
        }
    }

    #[test]
    fn providers_describe_themselves() {
        assert_eq!(Etcd.describe(), ProviderInfo::new("Etcd"));
        assert_eq!(Cached(Etcd).describe().kind, "Cached");
        let fixed = StaticDiscovery::new(vec![record("b"), record("a")]).unwrap().describe();
        assert_eq!(fixed.kind, "static");
        assert_eq!(fixed.settings, [("seeds".to_owned(), "a@127.0.0.1:9, b@127.0.0.1:9".to_owned())]);
        let memory = MemoryDiscovery::new();
        block_on(memory.register(&record("a"))).unwrap();
        assert_eq!(
            memory.describe().stats,
            [("nodes".to_owned(), "1".to_owned()), ("revision".to_owned(), "1".to_owned())]
        );
    }

    fn block_on<T>(f: BoxFuture<'_, T>) -> T {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(f)
    }
}
