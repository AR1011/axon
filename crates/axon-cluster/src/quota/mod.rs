//! Cluster-wide rate and concurrency limits checked locally (spec/quotas.md). The cores in `core.rs` decide; the
//! messages between nodes and replicas are in `wire.rs`; this drives them over the cluster.

mod core;
#[cfg(test)]
mod model;
#[cfg(test)]
mod sim;
mod wire;

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon::{Clock, Lane};
use bytes::Bytes;
use parking_lot::Mutex;

pub use self::core::Denied;
use self::core::{Candidates, HEARTBEAT, Limit, Members, Node, PATIENCE, REPORT_EVERY, Table};
use self::wire::{
    PROTOCOL, Request, SCHEMA, decode_ack, decode_grants, decode_page, decode_request, encode_ack, encode_grants,
    encode_page, encode_pull, encode_push, encode_report,
};
use crate::ownership::lease_hold;
use crate::{CallError, Cluster, ClusterError, ProviderInfo, ProviderLink, Rejection, View, WeakCluster};

/// The metadata key quota nodes advertise their region under, and the name of their endpoint.
pub(crate) const METADATA: &str = "axon.quota";
/// How long a replica keeps a silent node's grants, and a node's grant holds after its report, less drift (§2.4).
const LEASE: Duration = Duration::from_secs(5);
const DRIFT: f64 = 0.01;

/// At most `max` units per `window` for each key, from 1 s to 1 h. Its name names it alone: no other rate or
/// permits may share it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate(Limit);

impl Rate {
    pub const fn new(name: &'static str, max: u64, window: Duration) -> Self {
        let ns = window.as_nanos();
        assert!(ns >= 1_000_000_000 && ns <= 3_600_000_000_000, "a window from 1 s to 1 h");
        Self(Limit { name, max, window })
    }

    /// The same limit with another maximum, a plan's say. Every node must use the same maximum for a key.
    pub const fn with_max(self, max: u64) -> Self {
        Self(Limit { max, ..self.0 })
    }
}

/// At most `max` permits held at once for each key. Its name names it alone, as a rate's does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Permits(Limit);

impl Permits {
    pub const fn new(name: &'static str, max: u64) -> Self {
        Self(Limit { name, max, window: Duration::ZERO })
    }

    pub const fn with_max(self, max: u64) -> Self {
        Self(Limit { max, ..self.0 })
    }
}

impl Borrow<Limit> for Rate {
    fn borrow(&self) -> &Limit {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct QuotaConfig {
    /// Where this node serves, as its clients see it: a key has a replica in each of up to five regions. Nodes
    /// without one form region `""`.
    pub region: String,
    /// Replica candidates per region, the same on every node: a node talks to at most this many per region. Nodes that
    /// differ refuse each other's reports and pulls, so change it only with every quota node stopped.
    pub replicas_per_region: usize,
    /// Entries this node keeps, one per limit and key it used lately, before it evicts the least recently used idle
    /// ones. Entries in use, holding permits or units in the window, stay past it. An entry costs about 470 B, so the
    /// default, 2²⁰, holds up to about 500 MB.
    pub capacity: usize,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self { region: String::new(), replicas_per_region: Candidates::PER_REGION, capacity: 1 << 20 }
    }
}

/// What a node's quotas hold and did, for telemetry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct QuotaStats {
    /// Limits and keys this node used lately.
    pub entries: usize,
    /// Permits held on this node, of every key.
    pub permits: u64,
    /// Keys this node replicates, and their rows, one per node using the key.
    pub keys: usize,
    pub rows: usize,
    /// The replica is still pulling the rows of the others, after its start or a handover; `/readyz` waits for the
    /// start's alone ([`Cluster::quotas_starting`]).
    pub pulling: bool,
    /// Reports that named another maximum for a key than the one before: nodes that disagree on a limit.
    pub mismatched: u64,
    pub denied_limit: u64,
    pub denied_share: u64,
    /// Units admitted with no fresh grant of a majority of the key's replicas (spec/quotas.md §2.5).
    pub fallback: u64,
    pub reports: u64,
    pub report_failures: u64,
}

/// Cluster-wide limits per key, checked on this node without waiting (spec/quotas.md). A clone shares them.
#[derive(Clone)]
pub struct Quotas(Arc<Shared>);

/// A permit of a `Permits` limit, given back when dropped.
#[must_use = "dropping a permit gives it back at once"]
pub struct Permit {
    shared: Arc<Shared>,
    id: u64,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.shared.table.release(self.id, 1, self.shared.clock.now());
    }
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit").field("id", &self.id).finish()
    }
}

pub(crate) struct Shared {
    cluster: WeakCluster,
    clock: Clock,
    cfg: QuotaConfig,
    protocol: u64,
    table: Table,
    // ponytail: one lock over the replica and the drive, held through a report cycle's scan of the table, which report
    // answers wait behind; split them if the benchmark at 10⁷ keys shows it.
    node: Mutex<Node>,
    /// Per replica node: since when its answers came or failed, and the error while they fail.
    health: Mutex<BTreeMap<Arc<str>, Health>>,
    reports: AtomicU64,
    report_failures: AtomicU64,
}

type Health = (Duration, Option<String>);

/// The quota nodes `view` shows not draining, reachable or not (§2.2): views that disagree on reachability must still
/// choose the same replicas and count the same nodes, or their majorities need not meet.
fn members(view: &View, replicas_per_region: usize) -> Members {
    let quota = view.members.iter().filter(|m| !m.record.draining()).filter_map(|m| {
        let region = m.record.metadata.get(METADATA)?;
        Some((m.record.id.as_str(), m.record.incarnation, region.as_str()))
    });
    let unreachable = view.members.iter().filter(|m| !m.reachable).map(|m| m.record.id.as_str());
    Members::new(PROTOCOL, replicas_per_region, quota).unreachable(unreachable)
}

impl Quotas {
    /// Advertises this node as a quota node of `cfg.region`, serves as a replica, and reports to the replicas of the
    /// keys used here every 100 ms. One per cluster.
    pub async fn start(cluster: &Cluster, cfg: QuotaConfig) -> Result<Self, ClusterError> {
        if cfg.replicas_per_region == 0 || cfg.capacity == 0 {
            return Err(ClusterError::Config("replicas_per_region and capacity must be above zero"));
        }
        let system = cluster.0.system.upgrade().ok_or(ClusterError::Closed)?;
        let clock = system.clock().clone();
        let identity = cluster.identity();
        let now = clock.now();
        let table = Table::new(PROTOCOL, lease_hold(LEASE, DRIFT), cfg.capacity);
        let seen = members(&cluster.view(), cfg.replicas_per_region);
        let node = Node::new(&identity.node_id, identity.incarnation, seen, &table, LEASE, now);
        let shared = Arc::new(Shared {
            cluster: cluster.downgrade(),
            // Nodes that disagree on it choose other replicas: they refuse each other rather than mix.
            protocol: PROTOCOL.wrapping_add(cfg.replicas_per_region as u64),
            table,
            node: Mutex::new(node),
            health: Mutex::default(),
            cfg,
            clock,
            reports: AtomicU64::new(0),
            report_failures: AtomicU64::new(0),
        });
        let serving = shared.clone();
        // The endpoint's name is taken once per cluster: a second start is refused here.
        let endpoint = cluster.endpoint(METADATA, shared.protocol, SCHEMA).lane(Lane::Control);
        let endpoint = endpoint.serve(move |peer, body, reply| {
            let reply = reply.ok_or(Rejection::InvalidMessage)?;
            let now = serving.clock.now();
            let answer = match decode_request(body, peer.incarnation).ok_or(Rejection::InvalidMessage)? {
                Request::Report(report) => {
                    let mut node = serving.node.lock();
                    let first = node.replica.mismatched == 0;
                    let answer = node.answer(&peer.node_id.as_str().into(), &report, now).map(|g| encode_grants(&g));
                    serving.push(&mut node, now);
                    if first && node.replica.mismatched > 0 {
                        drop(node);
                        if let Some(cluster) = serving.cluster.upgrade() {
                            let detail = "its report named another maximum for a limit than the one before";
                            cluster.events().record("quota_mismatch", peer.node_id.as_str(), detail);
                        }
                    }
                    answer
                }
                Request::Pull { start, regions } => {
                    let (rows, next) = serving.node.lock().serve_pull(&peer.node_id, regions, start, now);
                    Some(encode_page(&rows, next))
                }
                Request::Push(grants) => {
                    if grants.incarnation != peer.incarnation {
                        return Err(Rejection::InvalidMessage);
                    }
                    // Under the node's lock, so that a view `see` installs meanwhile cannot have its `forget` undone.
                    let (node, replica) = (serving.node.lock(), peer.node_id.as_str());
                    let ack =
                        serving.table.pushed(replica, &grants, |id| node.members.candidates.serves(id, replica), now);
                    drop(node);
                    Some(ack.map_or_else(Bytes::new, |(r, acked)| encode_ack(r, acked)))
                }
            };
            if let Some(answer) = answer {
                let _ = reply.send(answer);
            }
            Ok(())
        })?;
        if let Err(error) = cluster.0.put_metadata(METADATA, Some(&shared.cfg.region)).await {
            cluster.unserve(endpoint);
            let _ = cluster.0.put_metadata(METADATA, None).await;
            return Err(error);
        }
        let _ = cluster.0.quotas.set(shared.clone());
        shared.node.lock().see(&shared.table, members(&cluster.view(), shared.cfg.replicas_per_region), now);
        let task = tokio::spawn(shared.clone().run());
        cluster.0.tasks.lock().push(task);
        Ok(Self(shared))
    }

    /// Takes every cost against its own limit for `key`, or none. Never waits.
    pub fn take(&self, key: impl AsRef<[u8]>, costs: &[(Rate, u64)]) -> Result<(), Denied> {
        self.0.table.take(key.as_ref(), costs, self.0.clock.now())
    }

    /// Adds costs known only after the work. Never refuses: what it adds over the limit refuses later takes.
    pub fn charge(&self, key: impl AsRef<[u8]>, costs: &[(Rate, u64)]) {
        self.0.table.charge(key.as_ref(), costs, self.0.clock.now());
    }

    /// One permit of `limit` for `key`, given back when the `Permit` drops. Never waits.
    pub fn acquire(&self, key: impl AsRef<[u8]>, limit: &Permits) -> Result<Permit, Denied> {
        let key = key.as_ref();
        self.0.table.take(key, &[(limit.0, 1)], self.0.clock.now())?;
        Ok(Permit { shared: self.0.clone(), id: self.0.table.id(&limit.0, key) })
    }

    pub fn stats(&self) -> QuotaStats {
        self.0.stats()
    }

    pub fn describe(&self) -> ProviderInfo {
        let (s, stats) = (&self.0, self.0.stats());
        let now = s.clock.now();
        let mut info = ProviderInfo::new("quotas")
            .setting("region", &s.cfg.region)
            .setting("replicas per region", s.cfg.replicas_per_region)
            .setting("capacity", s.cfg.capacity)
            .stat("entries", stats.entries)
            .stat("permits", stats.permits)
            .stat("keys", stats.keys)
            .stat("rows", stats.rows)
            .stat("pulling", stats.pulling)
            .stat("mismatched", stats.mismatched)
            .stat("denied at the limit", stats.denied_limit)
            .stat("denied for a share", stats.denied_share)
            .stat("fallback", stats.fallback)
            .stat("reports", stats.reports)
            .stat("report failures", stats.report_failures);
        info.links = s
            .health
            .lock()
            .iter()
            .map(|(target, (since, error))| ProviderLink {
                target: target.to_string(),
                identity: None,
                state: if error.is_none() { "answering" } else { "failing" }.into(),
                since: now.saturating_sub(*since),
                error: error.clone(),
            })
            .collect();
        info
    }
}

impl Shared {
    pub(crate) fn stats(&self) -> QuotaStats {
        let entries = self.table.len();
        let node = self.node.lock();
        QuotaStats {
            entries,
            permits: self.table.permits(),
            keys: node.replica.keys(),
            rows: node.replica.rows(),
            pulling: node.replica.pulling(),
            mismatched: node.replica.mismatched,
            denied_limit: self.table.denied_limit.load(Relaxed),
            denied_share: self.table.denied_share.load(Relaxed),
            fallback: self.table.fallback.load(Relaxed),
            reports: self.reports.load(Relaxed),
            report_failures: self.report_failures.load(Relaxed),
        }
    }

    pub(crate) fn starting(&self) -> bool {
        self.node.lock().replica.starting()
    }

    async fn run(self: Arc<Self>) {
        let mut every = tokio::time::interval(REPORT_EVERY);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            every.tick().await;
            let Some(cluster) = self.cluster.upgrade() else { return };
            self.round(&cluster);
        }
    }

    /// One report cycle: the view, the reports, and the pages a young replica asks for.
    fn round(self: &Arc<Self>, cluster: &Cluster) {
        let now = self.clock.now();
        let seen = members(&cluster.view(), self.cfg.replicas_per_region);
        let mut node = self.node.lock();
        node.see(&self.table, seen, now);
        let reports = node.round(&self.table, now);
        let (round, pulls) = node.pulls(now);
        for (r, link, report) in reports {
            self.reports.fetch_add(1, Relaxed);
            if r == node.me {
                if let Some(grants) = node.answer(&r, &report, now) {
                    self.table.apply(link, &grants, |id| node.members.candidates.serves(id, &r), now);
                }
                continue;
            }
            let (this, cluster, body) = (self.clone(), cluster.clone(), encode_report(&report));
            // ponytail: a report per replica per cycle, each bounded by a heartbeat; a slow replica has at most ten
            // out. Skipping one would lose its items, which only a gap in the numbering brings back.
            tokio::spawn(async move {
                let call = cluster.request(&r, METADATA, this.protocol, SCHEMA, body);
                let answer = tokio::time::timeout(HEARTBEAT, call).await;
                let grants = match answer {
                    Ok(Ok(body)) => decode_grants(body).ok_or_else(|| "undecodable grants".to_owned()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err("timed out".to_owned()),
                };
                let error = grants.as_ref().err().cloned();
                let now = this.clock.now();
                let mut health = this.health.lock();
                let (since, was) = health.entry(r.clone()).or_insert((now, error.clone()));
                if was.is_some() != error.is_some() {
                    *since = now;
                }
                *was = error;
                drop(health);
                match grants {
                    Ok(grants) => {
                        let node = this.node.lock();
                        this.table.apply(link, &grants, |id| node.members.candidates.serves(id, &r), now);
                    }
                    Err(_) => drop(this.report_failures.fetch_add(1, Relaxed)),
                }
            });
        }
        self.push(&mut node, now);
        let known = node.members.clone();
        drop(node);
        self.health.lock().retain(|r, _| known.incarnation(r).is_some());
        for (peer, start) in pulls {
            let body = encode_pull(start, known.candidates.regions());
            tokio::spawn(self.clone().pull(cluster.clone(), peer, round, body));
        }
    }

    /// Sends the grants the replica has to push: to this node's table at once, to another node in a request whose reply
    /// may acknowledge them (empty where it does not), which may free room to push to a third.
    fn push(self: &Arc<Self>, node: &mut Node, now: Duration) {
        let Some(cluster) = self.cluster.upgrade() else { return };
        for ((to, incarnation), grants) in node.pushes(&self.table, now) {
            let (this, cluster, body) = (self.clone(), cluster.clone(), encode_push(&grants));
            tokio::spawn(async move {
                let call = cluster.request_to(&to, incarnation, METADATA, this.protocol, SCHEMA, body);
                let Ok(Ok(reply)) = tokio::time::timeout(HEARTBEAT, call).await else { return };
                let Some((replica, acked)) = decode_ack(reply) else { return };
                let now = this.clock.now();
                let mut node = this.node.lock();
                node.ack(&(to, incarnation), replica, acked, now);
                this.push(&mut node, now);
            });
        }
    }

    /// One page of a candidate's rows.
    async fn pull(self: Arc<Self>, cluster: Cluster, peer: Arc<str>, round: u64, body: Bytes) {
        let call = cluster.request(&peer, METADATA, self.protocol, SCHEMA, body);
        let page = match tokio::time::timeout(PATIENCE, call).await {
            Ok(Ok(body)) => decode_page(body),
            // Refused for good, its protocol, schema or configuration apart: it has nothing to take in (§2.6). A closed
            // link or a lost reply is asked again, since the candidate may still hold rows.
            Ok(Err(
                ClusterError::ProtocolMismatch
                | ClusterError::SchemaMismatch { .. }
                | ClusterError::Call(CallError::Rejected(
                    Rejection::SchemaMismatch | Rejection::InvalidMessage | Rejection::Forbidden,
                )),
            )) => None,
            _ => return,
        };
        self.node.lock().page(&peer, round, page, self.clock.now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Member;
    use crate::discovery::{DRAINING, LIFECYCLE, NodeRecord};

    /// The rig's run of 2026-10-01: nodes that each found other peers unreachable chose replicas among their own
    /// reachable nodes, and admitted up to 2.7 times a limit.
    #[test]
    fn members_count_unreachable_nodes_a_pull_skips_and_leave_out_draining_ones() {
        let member = |id: &str, reachable, draining| {
            let mut metadata = BTreeMap::from([(METADATA.to_owned(), "r".to_owned())]);
            if draining {
                metadata.insert(LIFECYCLE.to_owned(), DRAINING.to_owned());
            }
            let address = "127.0.0.1:1".parse().expect("an address");
            Member { record: NodeRecord { id: id.into(), incarnation: 1, address, metadata, version: 0 }, reachable }
        };
        let view = View {
            members: vec![member("a", true, false), member("b", false, false), member("c", true, true)],
            ..View::default()
        };
        let quota = members(&view, 8);
        assert_eq!(quota.incarnation("b"), Some(1));
        assert_eq!(quota.incarnation("c"), None);
        assert_eq!(quota.candidates.all().count(), 2);
        // A pull does not wait for a node it cannot reach.
        assert_eq!(quota.to_pull("a").count(), 0);
    }
}
