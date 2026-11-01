use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::time::Duration;

use axon::{ActorSystem, ExtensionFuture, Panic, SystemExtension};
use axon_cluster::{
    AccessPolicy, Cluster, LEAVE_REASON, MemberChange, MembershipEvent, MembershipSubscription, NodeRecord, View,
};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, timeout};

use crate::control::{call, encode, every, fan_out, serve};
use crate::model::{Cursor, EventPage, Request, SampleBatch, boot, range};
use crate::provider::*;
use crate::telemetry::{Out, fetch, fetch_samples};
use crate::traces::{DeadLetterPage, DeadLetterRecord, MAX_TRACE_LIMIT, SpanPage, SpanRecord, TraceQuery, parse_id};

pub const METRICS_SERVICE: &str = "axon.metrics";
/// Discovery metadata a node carries while it answers history; the value is the storage name.
pub const HISTORY_SOURCE: &str = "axon.metrics";

/// Keeps the collector's own batches apart from its node's telemetry batches.
pub const OWN_SEQUENCE: u64 = 1 << 63;
const PAGES_PER_CYCLE: usize = 8;
const EVENTS_PER_WRITE: usize = 128;
const MAX_QUERIES: usize = 32;
const MAX_SINKS: usize = 8;

#[derive(Clone, Copy)]
enum Rows<'a> {
    Spans(&'a [SpanRecord]),
    Letters(&'a [DeadLetterRecord]),
    /// With the events the node's ring let go of before they were pulled.
    Events(&'a [ClusterEvent], u64),
}

impl Rows<'_> {
    /// How many there are, or an error when one is not `node`'s own.
    fn own(self, node: &str) -> Result<usize, String> {
        let (len, foreign, what) = match self {
            Rows::Spans(spans) => (spans.len(), spans.iter().any(|s| s.node_id != node), "spans"),
            Rows::Letters(letters) => (letters.len(), letters.iter().any(|l| l.node_id != node), "dead letters"),
            Rows::Events(events, _) => (events.len(), events.iter().any(|e| e.node_id != node), "events"),
        };
        if foreign { Err(format!("the node answered with {what} that are not its own")) } else { Ok(len) }
    }
}

/// The boot and position a node's spans, dead letters and events were stored through.
#[derive(Default)]
struct RowCursors {
    spans: (String, u64),
    letters: (String, u64),
    events: (String, u64),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsRequest {
    Capabilities,
    Health,
    Query(HistoryQuery),
    /// One round trip for a whole page of charts; answers `Vec<Result<HistoryResult, String>>` in order.
    Queries(Vec<HistoryQuery>),
    /// The series of a metric stored in the range, as a `HistoryResult` without values.
    Series(HistoryQuery),
    /// `Vec<ClusterEvent>`, newest first: the stored ones and those not yet written.
    Events(EventQuery),
    /// `Vec<TraceSummary>`, newest first.
    Traces(TraceQuery),
    /// The stored `Vec<SpanRecord>` of one trace.
    Trace {
        trace: String,
    },
    /// The stored `Vec<DeadLetterRecord>`, newest first.
    DeadLetters {
        limit: usize,
    },
}

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    /// `None` on a query-only host.
    pub collection_interval: Option<Duration>,
    /// One budget for a whole collection cycle.
    pub cycle_deadline: Duration,
    pub write_timeout: Duration,
    pub parallelism: usize,
    pub max_nodes: usize,
    /// Events waiting for storage; the oldest are dropped beyond this.
    pub event_buffer: usize,
    pub access: AccessPolicy,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            collection_interval: Some(Duration::from_secs(5)),
            cycle_deadline: Duration::from_secs(5),
            write_timeout: Duration::from_secs(3),
            parallelism: 8,
            max_nodes: 1024,
            event_buffer: 1024,
            access: AccessPolicy::Deny,
        }
    }
}

impl MetricsConfig {
    fn validate(&self, sinks: usize) -> Result<(), String> {
        range("sinks", sinks, 0, MAX_SINKS)?;
        if self.collection_interval.is_some() && sinks == 0 {
            return Err("collection_interval needs a sink; a query-only host sets it to None".into());
        }
        if self.collection_interval.is_some_and(|i| i < Duration::from_millis(100)) {
            return Err("collection_interval must be at least 100ms".into());
        }
        if self.cycle_deadline.is_zero() || self.write_timeout.is_zero() {
            return Err("cycle_deadline and write_timeout must not be zero".into());
        }
        range("parallelism", self.parallelism, 1, 64)?;
        range("max_nodes", self.max_nodes, 1, 4096)?;
        range("event_buffer", self.event_buffer, 1, 65536)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MetricsHealth {
    pub cycles: u64,
    /// How long ago the last cycle ended, on this collector's monotonic clock (its system's), so it compares across
    /// collectors whose wall clocks differ; `None` before the first.
    #[serde(default)]
    pub collected_ago_ms: Option<u64>,
    pub last_cycle_duration_ms: u64,
    /// Batches stored by at least one sink.
    pub stored_batches: u64,
    /// Spans stored by at least one sink.
    #[serde(default)]
    pub stored_spans: u64,
    /// Batches no sink would ever accept (they fail validation); skipped so they cannot wedge a node.
    pub rejected_batches: u64,
    pub collection_gaps: u64,
    pub source_resets: u64,
    pub write_errors: u64,
    pub dropped_events: u64,
    pub pending_events: usize,
    /// Sinks whose last write failed, by `index:name`. Each sink keeps its own cursors, so one
    /// that is down falls behind alone.
    pub sink_errors: BTreeMap<String, String>,
    pub node_errors: BTreeMap<String, String>,
    /// What each sink that can tell holds, by `index:name`, with its growth and when it fills.
    #[serde(default)]
    pub sink_usage: BTreeMap<String, SinkUsage>,
}

#[derive(Default)]
struct Events {
    sequence: u64,
    pending: VecDeque<(u64, ClusterEvent)>,
}

struct Sink {
    sink: Arc<dyn MetricsSink>,
    label: String,
    cursors: Mutex<HashMap<String, Cursor>>,
    /// Nodes a write failed for: the sink may hold more of them than the cursor says.
    unsure: Mutex<HashSet<String>>,
    events_written: AtomicU64,
    /// The cycle in which a write last failed; the sink sits out the rest of that cycle.
    failed_in: AtomicU64,
    /// As `failed_in`, for spans and dead letters: their cursor is shared, so only their own failures sit a sink out.
    rows_failed_in: AtomicU64,
}

struct Inner {
    cluster: Cluster,
    sinks: Vec<Arc<Sink>>,
    query: Arc<dyn MetricsQuery>,
    cfg: MetricsConfig,
    incarnation: String,
    health: Mutex<MetricsHealth>,
    /// On the system's clock, which `health` reads from whatever thread it is called on.
    collected: Mutex<Option<Duration>>,
    cycle: AtomicU64,
    events: Mutex<Events>,
    rows: Mutex<HashMap<String, RowCursors>>,
    flushing: tokio::sync::Mutex<()>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// A reference to the extension. The system owns it; dropping this handle stops nothing.
#[derive(Clone)]
pub struct MetricsService(Arc<Inner>);

impl MetricsService {
    pub async fn start<P: MetricsSink + MetricsQuery>(
        system: &ActorSystem,
        cluster: &Cluster,
        provider: Arc<P>,
        cfg: MetricsConfig,
    ) -> Result<Self, String> {
        Self::with_providers(system, cluster, vec![provider.clone()], provider, cfg).await
    }

    /// Every sink receives everything. With no sink this is a query-only host reading history
    /// someone else wrote.
    pub async fn with_providers(
        system: &ActorSystem,
        cluster: &Cluster,
        sinks: Vec<Arc<dyn MetricsSink>>,
        query: Arc<dyn MetricsQuery>,
        cfg: MetricsConfig,
    ) -> Result<Self, String> {
        cfg.validate(sinks.len())?;
        let sinks = sinks.into_iter().enumerate().map(|(i, sink)| {
            let label = format!("{i}:{}", sink.name());
            Arc::new(Sink {
                sink,
                label,
                cursors: Mutex::default(),
                unsure: Mutex::default(),
                events_written: AtomicU64::new(0),
                failed_in: AtomicU64::new(0),
                rows_failed_in: AtomicU64::new(0),
            })
        });
        let inner = Arc::new(Inner {
            cluster: cluster.clone(),
            incarnation: boot(cluster.identity().incarnation),
            health: Mutex::default(),
            collected: Mutex::default(),
            cycle: AtomicU64::new(0),
            events: Mutex::default(),
            rows: Mutex::default(),
            flushing: tokio::sync::Mutex::default(),
            tasks: Mutex::default(),
            sinks: sinks.collect(),
            query,
            cfg,
        });
        let capabilities = inner.query.capabilities();
        if capabilities.numeric_history || capabilities.events {
            cluster.set_metadata(HISTORY_SOURCE, Some(&capabilities.storage)).await.map_err(|e| e.to_string())?;
        }
        let (weak, access) = (Arc::downgrade(&inner), inner.cfg.access.clone());
        serve(cluster, METRICS_SERVICE, access, weak.clone(), "metrics stopped", Inner::serve)?;
        system.attach_extension(inner.clone()).map_err(|e| e.to_string())?;
        let mut tasks = Vec::new();
        if !inner.sinks.is_empty() {
            // Subscribed here, not in the task, so no change after start returns is missed.
            tasks.push(tokio::spawn(watch_membership(weak.clone(), cluster.subscribe())));
        }
        if let Some(interval) = inner.cfg.collection_interval {
            tasks.push(tokio::spawn(every(weak, interval, false, |inner| async move { inner.collect().await })));
        }
        *inner.tasks.lock() = tasks;
        Ok(Self(inner))
    }

    pub fn health(&self) -> MetricsHealth {
        self.0.health()
    }
}

/// The members that announce history, as `(node, storage)`, from the local membership view.
pub fn history_sources(cluster: &Cluster) -> Vec<(String, String)> {
    let view = cluster.view();
    let sources = view.members.iter().filter(|m| m.reachable);
    sources.filter_map(|m| Some((m.record.id.clone(), m.record.metadata.get(HISTORY_SOURCE)?.clone()))).collect()
}

pub async fn query_metrics<T: DeserializeOwned>(
    cluster: &Cluster,
    node: &str,
    request: &MetricsRequest,
) -> Result<T, String> {
    call(cluster, node, METRICS_SERVICE, request).await
}

/// Boots remembered, so a node that left and joined again with a new one shows as `restarted`.
const BOOTS: usize = 4096;

/// A membership change as a timeline shows it: `(node, kind, detail, record)`. `record` is there, with the kind
/// discovery gave the change, for what discovery says; an event's id is built from that kind, so collectors that
/// remember different boots still store a change once.
pub(crate) type Told<'a> = (String, &'static str, String, Option<(&'static str, &'a NodeRecord)>);

/// The boot each node was last seen with, so a node back under a new one shows as `restarted`, on the collector's
/// store and on a console's live feed alike.
#[derive(Default)]
pub(crate) struct Boots(BTreeMap<String, u128>);

impl Boots {
    /// Whether `record` is another boot than the one last seen of its node.
    fn new_boot(&mut self, record: &NodeRecord, view: &View) -> bool {
        let before = self.0.insert(record.id.clone(), record.incarnation);
        if self.0.len() > BOOTS {
            // The nodes that are gone are the ones to forget.
            self.0.retain(|id, _| view.get(id).is_some());
            while self.0.len() > BOOTS {
                self.0.pop_first();
            }
        }
        before.is_some_and(|b| b != record.incarnation)
    }

    pub(crate) fn told<'a>(&mut self, event: &'a MembershipEvent, local: &str) -> Vec<Told<'a>> {
        match event {
            MembershipEvent::Snapshot(view) => {
                view.members.iter().for_each(|m| _ = self.new_boot(&m.record, view));
                Vec::new()
            }
            MembershipEvent::Resync(view) => {
                let resync = (local.to_owned(), "resync", "membership changes were missed".to_owned(), None);
                // What was missed may have been a restart.
                let restarted = (view.members.iter()).filter(|m| self.new_boot(&m.record, view)).map(|m| {
                    let (node, _, detail, _) = describe(&MemberChange::Joined(m.record.clone()));
                    (node, "restarted", detail, Some(("joined", &m.record)))
                });
                std::iter::once(resync).chain(restarted).collect()
            }
            MembershipEvent::Unavailable(error) => {
                vec![(local.to_owned(), "discovery_unavailable", error.clone(), None)]
            }
            MembershipEvent::Changes { view, changes } => (changes.iter())
                .map(|change| {
                    let (node, kind, detail, record) = describe(change);
                    let shown = match change {
                        MemberChange::Joined(r) | MemberChange::Replaced { current: r, .. }
                            if self.new_boot(r, view) =>
                        {
                            "restarted"
                        }
                        _ => kind,
                    };
                    (node, shown, detail, record.map(|r| (kind, r)))
                })
                .collect(),
        }
    }
}

async fn watch_membership(weak: Weak<Inner>, mut subscription: MembershipSubscription) {
    let mut boots = Boots::default();
    while let Some(event) = subscription.next().await {
        let Some(inner) = weak.upgrade() else { break };
        let local = inner.cluster.identity().node_id.clone();
        let told = boots.told(&event, &local);
        if told.is_empty() {
            continue;
        }
        told.into_iter().for_each(|(node, kind, detail, record)| inner.record(node, kind, detail, record));
        inner.flush_events().await;
    }
}

/// The record is present for what discovery says, which every collector sees alike; absent for
/// what only this node observed.
fn describe(change: &MemberChange) -> (String, &'static str, String, Option<&NodeRecord>) {
    let reason = |r: &NodeRecord| r.metadata.get(LEAVE_REASON).cloned().unwrap_or_default();
    let (record, kind, detail) = match change {
        MemberChange::Joined(r) => (r, "joined", r.address.to_string()),
        MemberChange::Left(r) => (r, "left", reason(r)),
        MemberChange::Updated { previous, current } if current.draining() && !previous.draining() => {
            (current, "leaving", reason(current))
        }
        MemberChange::Updated { current, .. } => (current, "updated", String::new()),
        MemberChange::Replaced { current, .. } => (current, "restarted", current.address.to_string()),
        MemberChange::Unreachable(id) => return (id.clone(), "unreachable", String::new(), None),
        MemberChange::Reachable(id) => return (id.clone(), "reachable", String::new(), None),
    };
    // The incarnation pairs a departure with the boot it ends, and tells a restart from a rejoin.
    let incarnation = format!("incarnation {:032x}", record.incarnation);
    let detail = if detail.is_empty() { incarnation } else { format!("{detail} · {incarnation}") };
    (record.id.clone(), kind, detail, Some(record))
}

/// FNV-1a, because the id it feeds has to come out the same in every collector process.
fn fingerprint(record: &NodeRecord) -> u64 {
    let (incarnation, address) = (record.incarnation.to_be_bytes(), record.address.to_string());
    let metadata = record.metadata.iter().flat_map(|(k, v)| [k.as_bytes(), b"=", v.as_bytes(), b";"]);
    let parts = [&incarnation[..], address.as_bytes()].into_iter().chain(metadata);
    parts.flatten().fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ *byte as u64).wrapping_mul(0x0100_0000_01b3))
}

pub(crate) fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1000
}

/// `fut`, or an error that says `what` timed out once `wait` passes.
async fn within<T>(wait: Duration, what: &str, fut: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    timeout(wait, fut).await.unwrap_or_else(|_| Err(format!("{what} timed out")))
}

/// Runs `work` for each sink side by side, each in a task of its own, so a panic fails only that sink's work: `None`.
async fn each_sink<T, Fut>(sinks: &[Arc<Sink>], work: impl Fn(Arc<Sink>) -> Fut) -> Vec<Option<T>>
where
    T: Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let tasks: Vec<_> = sinks.iter().map(|sink| tokio::spawn(work(sink.clone()))).collect();
    let mut done = Vec::with_capacity(tasks.len());
    for task in tasks {
        done.push(task.await.ok());
    }
    done
}

impl Inner {
    fn health(&self) -> MetricsHealth {
        let mut health = self.health.lock().clone();
        health.pending_events = self.events.lock().pending.len();
        let now = self.cluster.clock().now();
        health.collected_ago_ms = self.collected.lock().map(|at| now.saturating_sub(at).as_millis() as u64);
        health.sink_usage =
            self.sinks.iter().filter_map(|s| Some((s.label.clone(), s.sink.usage()?.with_growth()))).collect();
        health
    }

    async fn serve(self: Arc<Self>, request: MetricsRequest, max_response: usize) -> Result<Bytes, String> {
        match request {
            MetricsRequest::Events(_) if !self.query.capabilities().events => Err("this provider has no events".into()),
            MetricsRequest::Traces(_) | MetricsRequest::Trace { .. } if !self.query.capabilities().traces => {
                Err(NO_TRACES.into())
            }
            MetricsRequest::Capabilities => encode(&self.query.capabilities()),
            MetricsRequest::Health => encode(&self.health()),
            MetricsRequest::Query(query) => {
                self.admits(&query)?;
                encode(&self.query.query(&query).await?)
            }
            MetricsRequest::Queries(queries) => {
                if queries.len() > MAX_QUERIES {
                    return Err(format!("at most {MAX_QUERIES} queries per request"));
                }
                let checked: Vec<_> = queries.iter().map(|q| self.admits(q)).collect();
                let valid: Vec<_> =
                    queries.into_iter().zip(&checked).filter(|(_, c)| c.is_ok()).map(|(q, _)| q).collect();
                let (this, asked) = (self.clone(), valid.len());
                let answered = tokio::spawn(async move { this.query.queries(&valid).await }).await;
                let answered = answered.unwrap_or_else(|failed| {
                    // The panic's own message: the JoinError's text names a task id global to the process.
                    let panic = failed.try_into_panic().ok().map(Panic::from);
                    let said = panic.as_ref().and_then(Panic::message);
                    Err(said.unwrap_or("the query panicked or its runtime stopped").to_owned())
                });
                let answered = answered.and_then(|answers| match answers.len() {
                    n if n == asked => Ok(answers),
                    n => Err(format!("the provider answered {n} of {asked} queries")),
                });
                let mut answered = answered.unwrap_or_else(|error| vec![Err(error); asked]).into_iter();
                // Each answer is sized on its own, so one oversized chart does not cost the page.
                let (mut body, mut room) = (vec![b'['], max_response.saturating_sub(2));
                for check in checked {
                    let answer =
                        check.and_then(|()| answered.next().expect("one answer per valid query, counted above"));
                    let mut encoded = serde_json::to_vec(&answer).map_err(|e| e.to_string())?;
                    if encoded.len() + 1 > room {
                        let refusal: Result<HistoryResult, _> = Err("the answer does not fit; ask for less");
                        encoded = serde_json::to_vec(&refusal).map_err(|e| e.to_string())?;
                    }
                    room = room.saturating_sub(encoded.len() + 1);
                    if body.len() > 1 {
                        body.push(b',');
                    }
                    body.extend_from_slice(&encoded);
                }
                body.push(b']');
                Ok(Bytes::from(body))
            }
            MetricsRequest::Series(query) => {
                self.admits(&query)?;
                encode(&self.query.series(&query).await?)
            }
            MetricsRequest::Events(query) => {
                query.validate()?;
                let mut events = self.query.events(&query).await?;
                let pending = self.events.lock().pending.clone();
                events.extend(pending.into_iter().map(|(_, e)| e).filter(|e| query.matches(e)));
                let mut seen = HashSet::new();
                events.retain(|e| seen.insert(e.id.clone()));
                events.sort_by(|a, b| (b.time_ms, &a.id).cmp(&(a.time_ms, &b.id)));
                events.truncate(query.limit);
                encode(&events)
            }
            MetricsRequest::Traces(query) => {
                query.validate()?;
                encode(&self.query.traces(&query).await?)
            }
            MetricsRequest::DeadLetters { limit } => {
                encode(&self.query.dead_letters(limit.clamp(1, MAX_TRACE_LIMIT)).await?)
            }
            MetricsRequest::Trace { trace } => encode(&self.query.trace(parse_id(&trace)?).await?),
        }
    }

    fn admits(&self, query: &HistoryQuery) -> Result<(), String> {
        let capabilities = self.query.capabilities();
        if !capabilities.numeric_history {
            return Err("this provider has no numeric history".into());
        }
        query.validate()?;
        if query.max_points > capabilities.max_points {
            return Err(format!("max_points must be at most {}", capabilities.max_points));
        }
        Ok(())
    }

    /// A discovery record names its change by its boot and version, the same in every collector, so
    /// collectors sharing a store converge however fast a node comes and goes; the boot because a
    /// provider may number each boot's versions afresh, as seed discovery does. A provider with no
    /// versions falls back to the record's content and the minute.
    fn record(&self, node_id: String, kind: &str, mut detail: String, shared: Option<(&str, &NodeRecord)>) {
        detail.truncate(detail.floor_char_boundary(1024));
        let time_ms = now_ms();
        let mut events = self.events.lock();
        events.sequence += 1;
        let id = match shared {
            Some((said, record)) if record.version != 0 => {
                format!("{node_id}:{said}:{:032x}:v{}", record.incarnation, record.version)
            }
            Some((said, record)) => format!("{node_id}:{said}:{:016x}:{}", fingerprint(record), time_ms / 60_000),
            None => format!("{}:{}:{}", self.cluster.identity().node_id, self.incarnation, events.sequence),
        };
        let sequence = events.sequence;
        let event = ClusterEvent { id, time_ms, node_id, kind: kind.into(), subject: String::new(), detail };
        events.pending.push_back((sequence, event));
        if events.pending.len() > self.cfg.event_buffer {
            events.pending.pop_front();
            self.health.lock().dropped_events += 1;
        }
    }

    async fn flush_events(self: &Arc<Self>) {
        let _one_at_a_time = self.flushing.lock().await;
        each_sink(&self.sinks, |sink| {
            let this = self.clone();
            async move {
                let result = write_events(&sink, this.unwritten(&sink), this.cfg.write_timeout).await;
                this.wrote(&sink, result);
            }
        })
        .await;
        let written = self.sinks.iter().map(|s| s.events_written.load(Relaxed)).min().unwrap_or(u64::MAX);
        let mut events = self.events.lock();
        while events.pending.front().is_some_and(|(sequence, _)| *sequence <= written) {
            events.pending.pop_front();
        }
    }

    fn unwritten(&self, sink: &Sink) -> Vec<(u64, ClusterEvent)> {
        let written = sink.events_written.load(Relaxed);
        self.events.lock().pending.iter().filter(|(sequence, _)| *sequence > written).cloned().collect()
    }

    fn wrote(&self, sink: &Sink, result: Result<(), String>) {
        let mut health = self.health.lock();
        match result {
            Ok(()) => drop(health.sink_errors.remove(&sink.label)),
            Err(mut error) => {
                error.truncate(error.floor_char_boundary(512));
                health.write_errors += 1;
                health.sink_errors.insert(sink.label.clone(), error);
                sink.failed_in.store(self.cycle.load(Relaxed), Relaxed);
            }
        }
    }

    /// Writes to the given sinks side by side; true where the write was stored.
    async fn write_all(self: &Arc<Self>, sinks: &[Arc<Sink>], batches: Vec<SampleBatch>) -> Vec<bool> {
        let batches = Arc::new(batches);
        let stored = each_sink(sinks, |sink| {
            let (this, batches) = (self.clone(), batches.clone());
            async move {
                let result = within(this.cfg.write_timeout, "storage write", sink.sink.write(&batches)).await;
                let stored = result.is_ok();
                this.wrote(&sink, result);
                stored
            }
        });
        stored.await.into_iter().map(|stored| stored == Some(true)).collect()
    }

    async fn collect(self: &Arc<Self>) {
        let started = Instant::now();
        self.cycle.fetch_add(1, Relaxed);
        self.flush_events().await;
        let view = self.cluster.view();
        let nodes: Vec<_> = view.members.iter().filter(|m| m.reachable).map(|m| m.record.id.clone()).collect();
        for sink in &self.sinks {
            #[expect(clippy::disallowed_methods, reason = "keeps or drops each entry on its own key")]
            sink.cursors.lock().retain(|node, _| view.get(node).is_some());
            #[expect(clippy::disallowed_methods, reason = "keeps or drops each entry on its own key")]
            sink.unsure.lock().retain(|node| view.get(node).is_some());
        }
        #[expect(clippy::disallowed_methods, reason = "keeps or drops each entry on its own key")]
        self.rows.lock().retain(|node, _| view.get(node).is_some());
        let mut errors = if nodes.len() > self.cfg.max_nodes {
            let error = format!("{} members exceed max_nodes {}", nodes.len(), self.cfg.max_nodes);
            BTreeMap::from([(self.cluster.identity().node_id.clone(), error)])
        } else {
            let this = self.clone();
            let pull = move |node: String| {
                let this = this.clone();
                async move {
                    let samples = this.pull(&node).await;
                    let spans = this.pull_rows(
                        &node,
                        |c| &mut c.spans,
                        |incarnation, after, limit| Request::Spans { incarnation, after, limit },
                        |p: &SpanPage| (&p.incarnation, p.next, Rows::Spans(&p.spans)),
                    );
                    let letters = this.pull_rows(
                        &node,
                        |c| &mut c.letters,
                        |incarnation, after, limit| Request::SettledDeadLetters { incarnation, after, limit },
                        |p: &DeadLetterPage| (&p.incarnation, p.next, Rows::Letters(&p.letters)),
                    );
                    let events = this.pull_rows(
                        &node,
                        |c| &mut c.events,
                        |incarnation, after, limit| Request::Events { incarnation, after, limit },
                        |p: &EventPage| (&p.incarnation, p.next, Rows::Events(&p.events, p.missed)),
                    );
                    samples.and(spans.await).and(letters.await).and(events.await)
                }
            };
            fan_out(nodes, self.cfg.parallelism, self.cfg.cycle_deadline, pull).await.1
        };
        errors.values_mut().for_each(|e| e.truncate(e.floor_char_boundary(512)));

        let own = {
            let mut health = self.health.lock();
            health.cycles += 1;
            *self.collected.lock() = Some(self.cluster.clock().now());
            health.last_cycle_duration_ms = started.elapsed().as_millis() as u64;
            health.node_errors = errors;
            self.own_batch(&health)
        };
        self.write_all(&self.sinks, vec![own]).await;
    }

    /// A sink's cursor moves only once that sink stored the batches, so a failed write is pulled
    /// again. Sinks standing at the same cursor share one pull, which is all of them when all
    /// are healthy; a sink whose write failed sits out the rest of the cycle.
    async fn pull(self: &Arc<Self>, node: &str) -> Result<(), String> {
        let cycle = self.cycle.load(Relaxed);
        let adrift = |s: &&Arc<Sink>| {
            s.failed_in.load(Relaxed) != cycle
                && (!s.cursors.lock().contains_key(node) || s.unsure.lock().contains(node))
        };
        let adrift: Vec<_> = self.sinks.iter().filter(adrift).cloned().collect();
        each_sink(&adrift, |sink| {
            let (this, node) = (self.clone(), node.to_owned());
            async move { this.anchor(&sink, &node).await }
        })
        .await;
        for _ in 0..PAGES_PER_CYCLE {
            let healthy = (self.sinks.iter().enumerate()).filter(|(_, s)| s.failed_in.load(Relaxed) != cycle);
            let groups = groups(healthy.map(|(i, sink)| (i, sink.cursors.lock().get(node).cloned())));
            let (mut advanced, mut gap, mut reset) = (false, false, false);
            for (after, members) in groups {
                let mut sinks: Vec<_> = members.into_iter().map(|i| self.sinks[i].clone()).collect();
                let page = fetch_samples(&self.cluster, node, after).await?;
                let honest = |b: &SampleBatch| {
                    b.node_id == node && b.incarnation == page.next.incarnation && b.sequence < OWN_SEQUENCE
                };
                if !page.batches.iter().all(honest) {
                    return Err("the node answered with batches that are not its own".into());
                }
                (gap, reset) = (gap | page.gap, reset | page.reset);
                let (valid, invalid): (Vec<_>, Vec<_>) =
                    page.batches.into_iter().partition(|b| validate_batches(std::slice::from_ref(b)).is_ok());
                for chunk in valid.chunks(MAX_BATCHES_PER_WRITE) {
                    let stored = self.write_all(&sinks, chunk.to_vec()).await;
                    let kept = |(sink, stored): (Arc<Sink>, bool)| {
                        if !stored {
                            sink.unsure.lock().insert(node.to_owned());
                        }
                        stored.then_some(sink)
                    };
                    sinks = sinks.into_iter().zip(stored).filter_map(kept).collect();
                    if sinks.is_empty() {
                        break;
                    }
                    let through = Cursor { sequence: chunk.last().map_or(0, |b| b.sequence), ..page.next.clone() };
                    sinks.iter().for_each(|sink| drop(sink.cursors.lock().insert(node.to_owned(), through.clone())));
                    self.health.lock().stored_batches += chunk.len() as u64;
                    advanced = true;
                }
                sinks.iter().for_each(|sink| drop(sink.cursors.lock().insert(node.to_owned(), page.next.clone())));
                // Counted when a cursor moves past them, so a sink that never stores cannot recount them.
                if !sinks.is_empty() {
                    self.health.lock().rejected_batches += invalid.len() as u64;
                }
            }
            let mut health = self.health.lock();
            health.collection_gaps += gap as u64;
            health.source_resets += reset as u64;
            if !advanced {
                break;
            }
        }
        Ok(())
    }

    /// Moves a sink's cursor up to what the sink already holds. For a cursor that cannot be trusted: there is
    /// none, or a write for the node failed and may have been stored all the same.
    async fn anchor(&self, sink: &Sink, node: &str) {
        let known = sink.cursors.lock().get(node).cloned();
        let Some(incarnation) = self.cluster.view().get(node).map(|m| boot(m.record.incarnation)) else {
            return;
        };
        match within(self.cfg.write_timeout, "storage position", sink.sink.stored_through(node, &incarnation)).await {
            Ok(held) => {
                let behind =
                    |held: &u64| known.as_ref().is_none_or(|c| c.incarnation != incarnation || c.sequence < *held);
                if let Some(sequence) = held.filter(behind) {
                    sink.cursors.lock().insert(node.to_owned(), Cursor { incarnation, sequence });
                }
                sink.unsure.lock().remove(node);
            }
            Err(error) => self.wrote(sink, Err(error)),
        }
    }

    /// One cursor per node and kind for all sinks: it moves once a sink stored the page, and a sink that already had
    /// it sees the same rows again.
    async fn pull_rows<P: DeserializeOwned>(
        &self,
        node: &str,
        cursor: fn(&mut RowCursors) -> &mut (String, u64),
        request: fn(String, u64, usize) -> Request,
        rows: fn(&P) -> (&String, u64, Rows<'_>),
    ) -> Result<(), String> {
        for _ in 0..PAGES_PER_CYCLE {
            let (incarnation, after) = self.rows.lock().get_mut(node).map(|c| cursor(c).clone()).unwrap_or_default();
            let page: P = fetch(&self.cluster, node, &request(incarnation, after, MAX_TRACE_LIMIT)).await?;
            let (incarnation, next, rows) = rows(&page);
            let count = rows.own(node)?;
            if !self.store(rows).await {
                break;
            }
            *cursor(self.rows.lock().entry(node.to_owned()).or_default()) = (incarnation.clone(), next);
            if count != MAX_TRACE_LIMIT {
                break;
            }
        }
        Ok(())
    }

    /// Offers the rows to every sink that has not failed them this cycle; true when one stored them. The cursor is
    /// shared, so a sink that fails misses these rows rather than holding the healthy sinks back.
    async fn store(&self, rows: Rows<'_>) -> bool {
        let (cycle, mut stored) = (self.cycle.load(Relaxed), false);
        for sink in self.sinks.iter().filter(|s| s.rows_failed_in.load(Relaxed) != cycle) {
            let write = match rows {
                Rows::Spans(spans) => sink.sink.write_spans(spans),
                Rows::Letters(letters) => sink.sink.write_dead_letters(letters),
                Rows::Events(events, _) => sink.sink.write_events(events),
            };
            match within(self.cfg.write_timeout, "storage write", write).await {
                Ok(()) => stored = true,
                Err(error) => {
                    sink.rows_failed_in.store(cycle, Relaxed);
                    self.wrote(sink, Err(error));
                }
            }
        }
        match rows {
            Rows::Spans(spans) if stored => self.health.lock().stored_spans += spans.len() as u64,
            Rows::Events(_, missed) if stored => self.health.lock().dropped_events += missed,
            _ => {}
        }
        stored
    }

    fn own_batch(&self, health: &MetricsHealth) -> SampleBatch {
        let mut out = Out(Vec::new());
        out.counter("collector.cycles", "cycles", &[], health.cycles);
        out.gauge("collector.cycle_duration", "milliseconds", &[], health.last_cycle_duration_ms as f64);
        out.counter("collector.stored_batches", "batches", &[], health.stored_batches);
        out.counter("collector.rejected_batches", "batches", &[], health.rejected_batches);
        out.counter("collector.gaps", "gaps", &[], health.collection_gaps);
        out.counter("collector.source_resets", "resets", &[], health.source_resets);
        out.counter("collector.write_errors", "errors", &[], health.write_errors);
        out.counter("collector.dropped_events", "events", &[], health.dropped_events);
        out.gauge("collector.node_errors", "nodes", &[], health.node_errors.len() as f64);
        out.gauge("collector.failing_sinks", "sinks", &[], health.sink_errors.len() as f64);
        SampleBatch {
            node_id: self.cluster.identity().node_id.clone(),
            incarnation: self.incarnation.clone(),
            sequence: OWN_SEQUENCE | health.cycles,
            timestamp_ms: now_ms(),
            samples: out.0,
        }
    }
}

async fn write_events(sink: &Sink, events: Vec<(u64, ClusterEvent)>, wait: Duration) -> Result<(), String> {
    for chunk in events.chunks(EVENTS_PER_WRITE) {
        let batch: Vec<_> = chunk.iter().map(|(_, event)| event.clone()).collect();
        within(wait, "storage write", sink.sink.write_events(&batch)).await?;
        sink.events_written.store(chunk.last().map_or(0, |(sequence, _)| *sequence), Relaxed);
    }
    Ok(())
}

impl SystemExtension for Inner {
    fn name(&self) -> &str {
        "metrics"
    }

    fn shutdown(&self, budget: Duration) -> ExtensionFuture {
        self.close();
        let (sinks, wait) = (self.sinks.clone(), self.cfg.write_timeout);
        let unwritten: Vec<_> = sinks.iter().map(|sink| self.unwritten(sink)).collect();
        Box::pin(async move {
            let last = async {
                let mut failures = Vec::new();
                for (sink, events) in sinks.iter().zip(unwritten) {
                    if let Err(error) = write_events(sink, events, wait).await {
                        failures.push(format!("{}: {error}", sink.label));
                    }
                }
                if failures.is_empty() { Ok(()) } else { Err(failures.join("; ")) }
            };
            timeout(budget, last).await.unwrap_or_else(|_| Err("metrics flush exceeded the shutdown budget".into()))
        })
    }

    fn close(&self) {
        self.tasks.lock().drain(..).for_each(|task| task.abort());
    }
}

#[cfg(test)]
mod tests {
    use axon_cluster::{ClusterConfig, MemoryDiscovery};
    use axon_net::SharedSecret;

    use super::*;

    #[tokio::test]
    async fn a_departed_nodes_cursors_are_dropped() {
        let mut transport = axon_net::Config::new("k", SharedSecret::new(*b"0123456789abcdef0123").unwrap());
        transport.allow_plaintext = true;
        let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
        let system = ActorSystem::builder().build().unwrap();
        let cluster = Cluster::start(&system, cfg).await.unwrap();
        // No timer: the one cycle is run by hand below.
        let cfg = MetricsConfig { collection_interval: None, ..Default::default() };
        let store = Arc::new(MemoryMetrics::new(1 << 20));
        let service = MetricsService::start(&system, &cluster, store, cfg).await.unwrap();
        let inner = &service.0;
        inner.rows.lock().insert("gone".into(), RowCursors::default());
        for sink in &inner.sinks {
            sink.cursors.lock().insert("gone".into(), Cursor { incarnation: String::new(), sequence: 0 });
            sink.unsure.lock().insert("gone".into());
        }
        inner.collect().await;
        assert!(!inner.rows.lock().contains_key("gone"));
        let held = |s: &Arc<Sink>| s.cursors.lock().contains_key("gone") || s.unsure.lock().contains("gone");
        assert!(!inner.sinks.iter().any(held));
        system.shutdown().await.unwrap();
    }
}
