use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axon::{
    ActorDiagnostics, ActorIncarnation, ActorSystem, ExtensionFuture, Histogram, HistogramSnapshot, RejectReason,
    SystemExtension, Timings, WeakActorSystem,
};
use axon_cluster::{AccessPolicy, Cluster, KEYED_ENDS, KEYED_STARTS, PeerStats, REJECTION_NAMES};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::de::DeserializeOwned;

use crate::control::{ask, call, encode, every, serve};
use crate::model::*;
use crate::provider::ClusterEvent;
use crate::sources::Sources;
use crate::traces::{DeadLetterPage, DeadLetterRecord, MAX_TRACE_LIMIT, SpanPage, SpanRecord, parse_id, summarize};

const PAGE: usize = 1000;
const ENVELOPE: usize = 128;

#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    pub sample_interval: Duration,
    pub retained_samples: usize,
    pub retained_bytes: usize,
    /// Batches above this size are dropped and reported as a gap.
    pub max_batch_bytes: usize,
    pub snapshot_connections: usize,
    /// Connections that get stored series each sample: the ones that moved the most bytes since the last one.
    /// The rest still count in `cluster.connections`.
    pub sample_connections: usize,
    pub snapshot_members: usize,
    /// Keyed actors listed, from [`Cluster::hosted`](axon_cluster::Cluster::hosted), and at most a quarter of the
    /// transport's `max_frame` of them as JSON, so a snapshot of long keys still fits a peer's control reply. None by
    /// default: `Request::Keys` pages them, and every snapshot reaches every console tab each second.
    pub snapshot_keys: usize,
    /// Actors visited per sample, and per `Actors` request.
    pub scan_limit: usize,
    /// Named actors that get their own persisted series; everything else is per type.
    pub persist_actors: Vec<String>,
    pub process_metrics: bool,
    pub host_metrics: bool,
    /// A series whose value has not changed is stored again only once the batches' stamps are this far apart, or at
    /// once when the wall stepped back; zero stores every series every sample. At most half of [`HELD_MS`], so a
    /// store can tell a series that holds its value from one that has ended.
    pub rewrite_unchanged: Duration,
    pub access: AccessPolicy,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_secs(1),
            retained_samples: 60,
            retained_bytes: 16 << 20,
            max_batch_bytes: 512 << 10,
            snapshot_connections: 64,
            sample_connections: 32,
            snapshot_members: 256,
            snapshot_keys: 0,
            scan_limit: 16384,
            persist_actors: Vec::new(),
            process_metrics: false,
            host_metrics: false,
            rewrite_unchanged: Duration::from_secs(30),
            access: AccessPolicy::Deny,
        }
    }
}

impl TelemetryConfig {
    fn validate(&self) -> Result<(), String> {
        if self.sample_interval < Duration::from_millis(10) {
            return Err("sample_interval must be at least 10ms".into());
        }
        range("retained_samples", self.retained_samples, 1, 3600)?;
        range("max_batch_bytes", self.max_batch_bytes, 1024, 700 << 10)?;
        range("retained_bytes", self.retained_bytes, self.max_batch_bytes, 1 << 30)?;
        range("snapshot_connections", self.snapshot_connections, 0, 1024)?;
        range("sample_connections", self.sample_connections, 0, 4096)?;
        range("snapshot_members", self.snapshot_members, 0, 4096)?;
        range("snapshot_keys", self.snapshot_keys, 0, 10_000)?;
        range("scan_limit", self.scan_limit, 1, 1 << 20)?;
        range("persist_actors", self.persist_actors.len(), 0, 64)?;
        if self.rewrite_unchanged > Duration::from_millis(HELD_MS / 2) {
            return Err("rewrite_unchanged must be at most 30s".into());
        }
        if (self.process_metrics || self.host_metrics) && !cfg!(feature = "system-metrics") {
            return Err("process_metrics and host_metrics need the `system-metrics` feature".into());
        }
        Ok(())
    }
}

#[derive(Default)]
struct State {
    sequence: u64,
    snapshot: Bytes,
    ring: VecDeque<(u64, Vec<u8>)>,
    ring_bytes: usize,
    /// Every series of the last sample, changed or not, and the Prometheus text of it once a scrape asked.
    latest: Vec<Sample>,
    exposition: Option<Bytes>,
}

struct Sampler {
    workers: Option<(Instant, Vec<Duration>)>,
    /// Wire bytes each connection had moved at the last sample.
    traffic: HashMap<(String, bool, axon::Lane), u64>,
    sources: Sources,
    /// Each series' value and the stamp of the batch that last stored it, by a hash of its name and labels.
    stored: HashMap<u64, (Value, u64)>,
    skipped: u64,
}

impl Sampler {
    fn new(sources: Sources) -> Self {
        Self { workers: None, traffic: HashMap::new(), sources, stored: HashMap::new(), skipped: 0 }
    }

    /// What a store needs from this sample: the series that changed or are due again, and the count of the rest,
    /// which is in every batch so that a store knows how far it holds this node. A series is due again on the stamps,
    /// which stores judge gaps on, and at once when the wall stepped back behind the one that last stored it.
    /// A type's counters and histograms never dip below what was last stored, and when missing are carried from
    /// `shown`, the previous sample: they count its exited actors too, but one that exits while `sample` pages the rest
    /// is in neither for that sample, and a scraper reads a dip as a reset and a series that leaves as gone. After a
    /// lost batch every series is written again, and the values held stay the floor.
    fn changed(
        &mut self,
        all: &mut Vec<Sample>,
        shown: Vec<Sample>,
        now_ms: u64,
        every: u64,
        lost: bool,
    ) -> Vec<Sample> {
        let Sampler { stored, skipped, .. } = self;
        let hasher = stored.hasher().clone();
        let key = |sample: &Sample| hasher.hash_one((&sample.name, &sample.labels));
        let mut kept = HashMap::with_capacity_and_hasher(stored.len(), hasher.clone());
        let holds = |s: &Sample| s.labels.contains_key("actor_type") && !matches!(s.value, Value::Gauge(_));
        let mut due = |sample: &mut Sample, before: Option<(Value, u64)>| {
            if let Some((before, _)) = before.as_ref().filter(|_| holds(sample)) {
                hold(&mut sample.value, before);
            }
            let held =
                before.filter(|(value, at)| !lost && *value == sample.value && (*at..at + every).contains(&now_ms));
            let write = held.is_none();
            kept.insert(key(sample), held.unwrap_or_else(|| (sample.value.clone(), now_ms)));
            write
        };
        let mut samples: Vec<_> = (all.iter_mut())
            .filter_map(|sample| due(sample, stored.remove(&key(sample))).then(|| sample.clone()))
            .collect();
        // What is left in `stored` was shown and is missing now; most samples lose nothing.
        if !stored.is_empty() {
            for mut sample in shown.into_iter().filter(holds) {
                let Some(before) = stored.remove(&key(&sample)) else { continue };
                if due(&mut sample, Some(before)) {
                    samples.push(sample.clone());
                }
                all.push(sample);
            }
        }
        *skipped += (all.len() - samples.len()) as u64;
        *stored = kept;
        let mut out = Out(samples);
        out.counter("telemetry.samples_skipped", "samples", &[], *skipped);
        out.0
    }
}

struct Inner {
    system: WeakActorSystem,
    cluster: Cluster,
    cfg: TelemetryConfig,
    persist: HashSet<String>,
    incarnation: String,
    started: tokio::time::Instant,
    state: Mutex<State>,
    sampler: Mutex<Sampler>,
    /// A batch was dropped, or a reader missed some: the next one carries every series, not only those that changed.
    lost: AtomicBool,
    streams: Mutex<Option<axon_stream::StreamNode>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// A reference to the extension. The system owns it; dropping this handle stops nothing.
#[derive(Clone)]
pub struct Telemetry(Arc<Inner>);

impl Telemetry {
    pub async fn start(system: &ActorSystem, cluster: &Cluster, cfg: TelemetryConfig) -> Result<Self, String> {
        cfg.validate()?;
        let inner = Arc::new(Inner {
            system: system.downgrade(),
            cluster: cluster.clone(),
            persist: cfg.persist_actors.iter().cloned().collect(),
            incarnation: boot(cluster.identity().incarnation),
            started: tokio::time::Instant::now(),
            state: Mutex::default(),
            sampler: Mutex::new(Sampler::new(Sources::new(cfg.process_metrics, cfg.host_metrics))),
            lost: AtomicBool::new(false),
            streams: Mutex::default(),
            task: Mutex::default(),
            cfg,
        });
        let first = inner.clone();
        #[expect(clippy::disallowed_methods, reason = "telemetry reads real threads; the World never runs it")]
        tokio::task::spawn_blocking(move || first.sample()).await.map_err(|e| e.to_string())?;

        let (weak, access) = (Arc::downgrade(&inner), inner.cfg.access.clone());
        serve(cluster, TELEMETRY_SERVICE, access, weak.clone(), "telemetry stopped", Inner::serve)?;
        system.attach_extension(inner.clone()).map_err(|e| e.to_string())?;
        // The first sample was taken above.
        let run = every(weak, inner.cfg.sample_interval, true, |inner| async move {
            #[expect(clippy::disallowed_methods, reason = "telemetry reads real threads; the World never runs it")]
            let _ = tokio::task::spawn_blocking(move || inner.sample()).await;
        });
        *inner.task.lock() = Some(tokio::spawn(run));
        Ok(Self(inner))
    }

    /// Samples this node's streams from now on.
    pub fn watch_streams(&self, node: &axon_stream::StreamNode) {
        *self.0.streams.lock() = Some(node.clone());
    }

    /// The latest local snapshot as JSON.
    pub fn snapshot(&self) -> Bytes {
        self.0.state.lock().snapshot.clone()
    }

    /// The last sample in the Prometheus text format, rendered once however many scrapes ask for it.
    #[cfg_attr(not(feature = "dashboard"), expect(dead_code, reason = "served by Health"))]
    pub(crate) fn exposition(&self) -> Bytes {
        let mut state = self.0.state.lock();
        let State { latest, exposition, .. } = &mut *state;
        exposition.get_or_insert_with(|| crate::prometheus::render(latest).into()).clone()
    }
}

impl SystemExtension for Inner {
    fn name(&self) -> &str {
        "telemetry"
    }

    fn shutdown(&self, _budget: Duration) -> ExtensionFuture {
        self.close();
        Box::pin(async { Ok(()) })
    }

    fn close(&self) {
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }
}

/// The greatest incarnation ordered before `id`, for a page that starts at `id`.
fn before(id: ActorIncarnation) -> Option<ActorIncarnation> {
    let ActorIncarnation { runtime, slot, generation } = id;
    match (runtime, slot, generation) {
        (_, _, 1..) => Some(ActorIncarnation { generation: generation - 1, ..id }),
        (_, 1.., 0) => Some(ActorIncarnation { slot: slot - 1, generation: u64::MAX, ..id }),
        (1.., 0, 0) => Some(ActorIncarnation { runtime: runtime - 1, slot: u64::MAX, generation: u64::MAX }),
        (0, 0, 0) => None,
    }
}

pub async fn fetch_samples(cluster: &Cluster, node: &str, after: Option<Cursor>) -> Result<SamplePage, String> {
    let reply = ask(cluster, node, TELEMETRY_SERVICE, &Request::Samples { after }).await?;
    let page: SamplePage = postcard::from_bytes(&reply).map_err(|e| e.to_string())?;
    match page.schema {
        SAMPLES_SCHEMA => Ok(page),
        other => Err(format!("sample page schema {other}, expected {SAMPLES_SCHEMA}")),
    }
}

/// For every request but `Samples`: they answer in JSON.
pub async fn fetch<T: DeserializeOwned>(cluster: &Cluster, node: &str, request: &Request) -> Result<T, String> {
    call(cluster, node, TELEMETRY_SERVICE, request).await
}

/// A page that does not fit would never be collected: send the part that does, halving until it fits.
fn fit<T>(
    mut items: Vec<T>,
    max: usize,
    encode: impl Fn(&[T]) -> serde_json::Result<Vec<u8>>,
) -> Result<Bytes, String> {
    loop {
        let body = encode(&items).map_err(|e| e.to_string())?;
        if body.len() <= max || items.len() <= 1 {
            return Ok(body.into());
        }
        items.truncate(items.len() / 2);
    }
}

/// A page of what follows `after` that fits `max`, as `wrap(next, items)`: `next` is the position of its last item,
/// or `after` when there is none.
fn page<T: Clone, P: serde::Serialize>(
    items: Vec<(u64, T)>,
    after: u64,
    max: usize,
    wrap: impl Fn(u64, Vec<T>) -> P,
) -> Result<Bytes, String> {
    fit(items, max, |items| {
        let next = items.last().map_or(after, |(seq, _)| *seq);
        serde_json::to_vec(&wrap(next, items.iter().map(|(_, item)| item.clone()).collect()))
    })
}

/// Its id names the node, boot and position, so a page pulled twice is stored once.
pub(crate) fn event(event: &axon::Event, node: &str, incarnation: &str) -> ClusterEvent {
    let detail = match event.count {
        1 => event.detail.clone(),
        n => format!("{n} times · {}", event.detail),
    };
    ClusterEvent {
        id: format!("{node}:{incarnation}:e{}", event.seq),
        time_ms: event.at_us / 1000,
        node_id: node.into(),
        kind: event.kind.into(),
        subject: event.subject.clone(),
        detail,
    }
}

/// Streams listed per `Request::Streams`.
const MAX_STREAMS: usize = 1000;
/// Keys listed per `Request::Keys`: a console's page of 1000 and the one it asks past it.
const MAX_KEYS: usize = 1024;

impl Inner {
    async fn serve(self: Arc<Self>, request: Request, max: usize) -> Result<Bytes, String> {
        let incarnation = || self.incarnation.clone();
        Ok(match request {
            Request::Snapshot => self.state.lock().snapshot.clone(),
            Request::Samples { after } => self.samples(after, max).into(),
            Request::Streams { limit } => {
                let detail = self.streams.lock().as_ref().map(|node| node.detail(limit.min(MAX_STREAMS)));
                encode(&detail.unwrap_or_default())?
            }
            Request::Spans { incarnation: asked, after, limit } => {
                let after = self.after(&asked, after);
                let spans = self.spans(|tracer| tracer.since(after, limit.min(MAX_TRACE_LIMIT)))?;
                page(spans, after, max, |next, spans| SpanPage { incarnation: incarnation(), next, spans })?
            }
            Request::Traces { limit } => {
                let spans = self.spans(|tracer| tracer.since(0, usize::MAX))?;
                let mut traces = summarize(spans.iter().map(|(_, span)| span));
                traces.sort_by_key(|t| std::cmp::Reverse(t.start_us));
                traces.truncate(limit.min(MAX_TRACE_LIMIT));
                encode(&traces)?
            }
            Request::Trace { trace } => {
                let trace = parse_id(&trace)?;
                let spans: Vec<_> = self.spans(|tracer| tracer.trace(trace))?.into_iter().map(|(_, s)| s).collect();
                encode(&spans)?
            }
            Request::DeadLetters { limit } => {
                let system = self.system.upgrade().ok_or("system stopped")?;
                let node = &self.cluster.identity().node_id;
                let letters = system.dead_letters().recent(limit.min(MAX_TRACE_LIMIT));
                let letters = letters.iter().map(|l| DeadLetterRecord::new(l, node)).collect();
                fit(letters, max, serde_json::to_vec)?
            }
            Request::SettledDeadLetters { incarnation: asked, after, limit } => {
                let system = self.system.upgrade().ok_or("system stopped")?;
                let node = &self.cluster.identity().node_id;
                let after = self.after(&asked, after);
                let letters = system.dead_letters().settled(after, limit.min(MAX_TRACE_LIMIT));
                let letters = letters.iter().map(|l| (l.seq, DeadLetterRecord::new(l, node))).collect();
                page(letters, after, max, |next, letters| DeadLetterPage { incarnation: incarnation(), next, letters })?
            }
            Request::Events { incarnation: asked, after, limit } => {
                let system = self.system.upgrade().ok_or("system stopped")?;
                let node = &self.cluster.identity().node_id;
                let (same, after) = (asked == self.incarnation, self.after(&asked, after));
                let events = system.events().since(after, limit.min(MAX_TRACE_LIMIT));
                let missed = if same { events.first().map_or(0, |e| e.seq - after - 1) } else { 0 };
                let events = events.iter().map(|e| (e.seq, event(e, node, &self.incarnation))).collect();
                page(events, after, max, |next, events| EventPage { incarnation: incarnation(), next, missed, events })?
            }
            Request::Connection { peer, outgoing, lane } => {
                let peers = self.cluster.stats().engine.peers;
                // A down lane has no session to time. While a replaced session drains, the open one stands for it.
                let moved =
                    |p: &&PeerStats| (p.session.closed.is_none(), p.session.tx_wire_bytes + p.session.rx_wire_bytes);
                let session =
                    |p: &&PeerStats| p.node == peer && p.outgoing == outgoing && p.lane == lane && p.down_ms.is_none();
                let found = peers.iter().filter(session).max_by_key(moved);
                encode(
                    &found.map(|p| serde_json::json!({ "timings": p.session.timings, "request_rtt": p.request_rtt })),
                )?
            }
            Request::Keys { kind, prefix, from, limit } => {
                let (keys, more) = self.cluster.hosted_keys(&kind, &prefix, from.as_deref(), limit.min(MAX_KEYS));
                let keys: Vec<_> = keys.into_iter().map(|k| ListedKey { key: k.key, epoch: k.epoch }).collect();
                let all = keys.len();
                fit(keys, max, |keys| {
                    serde_json::to_vec(&KeyPage { keys: keys.to_vec(), more: more || keys.len() < all })
                })?
            }
            Request::Actors(query) => {
                #[expect(clippy::disallowed_methods, reason = "telemetry reads real threads; the World never runs it")]
                let page =
                    tokio::task::spawn_blocking(move || self.actors(query)).await.map_err(|e| e.to_string())??;
                encode(&page)?
            }
        })
    }

    /// Where a page asked for boot `asked` starts: after `after` in this one, from the first in another.
    fn after(&self, asked: &str, after: u64) -> u64 {
        if asked == self.incarnation { after } else { 0 }
    }

    fn spans(&self, pick: impl FnOnce(&axon::Tracer) -> Vec<axon::Span>) -> Result<Vec<(u64, SpanRecord)>, String> {
        let system = self.system.upgrade().ok_or("system stopped")?;
        let node = &self.cluster.identity().node_id;
        let mut names = HashMap::new();
        let mut record = |span: &axon::Span| {
            let name = span.actor.and_then(|(id, _)| names.entry(id).or_insert_with(|| system.name_of(id)).clone());
            (span.seq, SpanRecord::new(span, node, name.as_deref().unwrap_or_default()))
        };
        Ok(pick(system.tracer()).iter().map(&mut record).collect())
    }

    /// The ring holds encoded batches, so a page is its header, a count, and those bytes as they are.
    fn samples(&self, after: Option<Cursor>, max: usize) -> Vec<u8> {
        let state = self.state.lock();
        let reset = after.as_ref().is_some_and(|c| c.incarnation != self.incarnation);
        let from = after.as_ref().filter(|_| !reset).map_or(0, |c| c.sequence);
        let (mut picked, mut bytes) = (Vec::new(), ENVELOPE);
        for (sequence, batch) in state.ring.iter().filter(|(s, _)| *s > from) {
            // A batch no page could carry is passed over, and shows as a gap, so it cannot wedge the reader.
            if batch.len() + ENVELOPE > max {
                continue;
            }
            bytes += batch.len();
            if bytes > max {
                break;
            }
            picked.push((*sequence, batch));
        }
        let (gap, next) = match picked.last() {
            Some((last, _)) => (after.is_some() && !reset && last - from > picked.len() as u64, *last),
            None => (state.sequence > from, state.sequence.max(from)),
        };
        if gap {
            self.lost.store(true, Ordering::Relaxed);
        }
        let header = (
            SAMPLES_SCHEMA,
            reset,
            gap,
            Cursor { incarnation: self.incarnation.clone(), sequence: next },
            picked.len(),
        );
        let mut page = postcard::to_stdvec(&header).unwrap_or_default();
        picked.iter().for_each(|(_, batch)| page.extend_from_slice(batch));
        page
    }

    fn actors(&self, query: ActorQuery) -> Result<ActorPage, String> {
        let system = self.system.upgrade().ok_or("system stopped")?;
        if !(1..=PAGE).contains(&query.limit) {
            return Err(format!("limit must be 1..={PAGE}"));
        }
        let search = query.search.as_deref().map(str::to_lowercase);
        let matches = |a: &ActorDiagnostics| {
            query.actor_type.as_deref().is_none_or(|t| t == a.actor_type)
                && query.phase.as_deref().is_none_or(|p| p.eq_ignore_ascii_case(&format!("{:?}", a.phase)))
                && search.as_deref().is_none_or(|s| {
                    a.actor_type.to_lowercase().contains(s)
                        || a.name.as_deref().is_some_and(|n| {
                            let n = if query.redacted { crate::model::shown(n) } else { n };
                            n.to_lowercase().contains(s)
                        })
                })
        };
        let mut page = ActorPage { actors: Vec::new(), total: 0, next: query.after, truncated: false };
        if let Some(id) = query.id {
            let chunk = system.diagnostics_page(before(id), 1).map_err(|e| e.to_string())?;
            page.total = chunk.total;
            page.actors = chunk.actors.into_iter().filter(|a| a.incarnation == id && matches(a)).collect();
            page.next = None;
            return Ok(page);
        }
        let mut scanned = 0;
        loop {
            let chunk = system.diagnostics_page(page.next, PAGE).map_err(|e| e.to_string())?;
            page.total = chunk.total;
            for mut actor in chunk.actors {
                scanned += 1;
                page.next = Some(actor.incarnation);
                if matches(&actor) {
                    if !query.timings {
                        actor.diagnostics.timings = None;
                    }
                    page.actors.push(actor);
                }
                if page.actors.len() == query.limit || scanned == self.cfg.scan_limit {
                    page.truncated = page.actors.len() < query.limit;
                    return Ok(page);
                }
            }
            if chunk.next.is_none() {
                page.next = None;
                return Ok(page);
            }
        }
    }

    fn sample(&self) {
        let Some(system) = self.system.upgrade() else { return };
        let view = self.cluster.view();
        let mut notices: Vec<_> =
            (view.stale.iter()).map(|error| Notice { code: "membership_stale", message: error.clone() }).collect();
        let mut out = Out(Vec::with_capacity(256));

        let runtime = system.runtime_stats();
        let stats = system.stats();
        let mut types: HashMap<&'static str, ActorTypeSnapshot> = (stats.retired.into_iter())
            .map(|(actor_type, totals)| (actor_type, ActorTypeSnapshot { actor_type, totals, ..Default::default() }))
            .collect();
        let (mut after, mut scanned, mut actor_total) = (None, 0, 0);
        while let Ok(page) = system.diagnostics_page(after, PAGE) {
            actor_total = page.total;
            for actor in page.actors {
                scanned += 1;
                let d = &actor.diagnostics;
                let t = types.entry(actor.actor_type).or_default();
                t.actor_type = actor.actor_type;
                t.live += 1;
                t.mailbox_depth += d.mailbox_depth as u64;
                t.mailbox_depth_max = t.mailbox_depth_max.max(d.mailbox_depth as u64);
                t.mailbox_bytes += d.mailbox_bytes as u64;
                t.waiters += d.waiters as u64;
                t.requests += d.requests as u64;
                t.oldest_message_age_ns =
                    t.oldest_message_age_ns.max(d.oldest_message_age.map_or(0, |age| age.as_nanos() as u64));
                t.totals.add(actor.restarts, d);
                if let Some(name) = actor.name.as_deref().filter(|n| self.persist.contains(*n)) {
                    out.actor(name, &actor);
                }
            }
            after = page.next;
            if after.is_none() || scanned >= self.cfg.scan_limit {
                break;
            }
        }
        if after.is_some() {
            let message = format!("per-type series cover the first {scanned} of {actor_total} actors");
            notices.push(Notice { code: "actor_scan_truncated", message });
        }
        #[expect(clippy::disallowed_methods, reason = "the hash order never shows: sorted on the next line")]
        let mut actor_types: Vec<_> = types.into_values().collect();
        actor_types.sort_by_key(|t| t.actor_type);

        let mut sampler = self.sampler.lock();
        #[expect(clippy::disallowed_methods, reason = "telemetry reads real threads; the World never runs it")]
        let now = Instant::now();
        let all = || runtime.workers.iter().chain(runtime.pools.iter().flat_map(|p| &p.workers));
        let busy: Vec<_> = all().map(|w| w.busy).collect();
        let utilisation: Vec<f64> = match &sampler.workers {
            Some((at, before)) if before.len() == busy.len() => {
                let wall = now.duration_since(*at).as_secs_f64().max(1e-9);
                busy.iter().zip(before).map(|(b, a)| (b.saturating_sub(*a).as_secs_f64() / wall).min(1.0)).collect()
            }
            _ => Vec::new(),
        };
        sampler.workers = Some((now, busy));
        let mut rest = utilisation.iter().copied();
        let worker_utilisation: Vec<f64> = rest.by_ref().take(runtime.workers.len()).collect();
        let pool_utilisation: Vec<Vec<f64>> =
            runtime.pools.iter().map(|p| rest.by_ref().take(p.workers.len()).collect()).collect();
        let (process, host) = sampler.sources.sample();
        let mut cluster = self.cluster.stats();
        // Busiest first, so that what is stored and what the snapshot shows are the connections that matter.
        let moved = |p: &PeerStats| p.session.tx_wire_bytes + p.session.rx_wire_bytes;
        let before = std::mem::take(&mut sampler.traffic);
        let key = |p: &PeerStats| (p.node.clone(), p.outgoing, p.lane);
        let since = |p: &PeerStats| moved(p).saturating_sub(*before.get(&key(p)).unwrap_or(&0));
        cluster.engine.peers.sort_by_cached_key(|p| std::cmp::Reverse((since(p), moved(p))));
        sampler.traffic = cluster.engine.peers.iter().map(|p| (key(p), moved(p))).collect();
        drop(sampler);

        let mut members = view.members.clone();

        out.gauge("runtime.actors", "actors", &[], runtime.actors as f64);
        out.counter("trace.spans", "spans", &[], system.tracer().recorded());
        out.counter("runtime.actors_started", "actors", &[], stats.started);
        out.counter("runtime.actors_stopped", "actors", &[], stats.stopped);
        out.counter("runtime.actors_failed", "actors", &[], stats.failed);
        out.counter("runtime.dead_letters", "messages", &[], stats.dead_letters);
        // Utilisation is empty or one per worker, in this order.
        let mut utilisation = utilisation.iter();
        let pools = runtime.pools.iter().map(|p| (Some(p.name.as_str()), &p.workers));
        for (pool, workers) in std::iter::once((None, &runtime.workers)).chain(pools) {
            for (i, w) in workers.iter().enumerate() {
                let worker = i.to_string();
                let labels = match pool {
                    Some(pool) => vec![("pool", pool), ("worker", worker.as_str())],
                    None => vec![("worker", worker.as_str())],
                };
                out.counter("runtime.worker.turns", "turns", &labels, w.turns);
                out.counter("runtime.worker.parks", "parks", &labels, w.parks);
                out.counter("runtime.worker.busy", "nanoseconds", &labels, w.busy.as_nanos() as u64);
                out.gauge("runtime.worker.queued", "actors", &labels, w.queued as f64);
                if let Some(u) = utilisation.next() {
                    out.gauge("runtime.worker.utilisation", "ratio", &labels, *u);
                }
            }
        }
        actor_types.iter().for_each(|t| out.actor_type(t));
        out.cluster(&cluster, view.stale.is_some(), self.cfg.sample_connections);
        let streams = self.streams.lock().as_ref().map(axon_stream::StreamNode::stats);
        if let Some(streams) = &streams {
            out.streams(streams);
        }
        if let Some(p) = &process {
            out.gauge("process.rss_bytes", "bytes", &[], p.rss_bytes as f64);
            out.gauge("process.virtual_bytes", "bytes", &[], p.virtual_bytes as f64);
            out.counter("process.read_bytes", "bytes", &[], p.read_bytes);
            out.counter("process.written_bytes", "bytes", &[], p.written_bytes);
            if let Some(cpu) = p.cpu_percent {
                out.gauge("process.cpu_percent", "percent", &[], cpu as f64);
            }
        }
        if let Some(h) = &host {
            out.gauge("host.total_memory_bytes", "bytes", &[], h.total_memory_bytes as f64);
            out.gauge("host.available_memory_bytes", "bytes", &[], h.available_memory_bytes as f64);
            out.counter("host.received_bytes", "bytes", &[], h.received_bytes);
            out.counter("host.transmitted_bytes", "bytes", &[], h.transmitted_bytes);
            if let Some(cpu) = h.cpu_percent {
                out.gauge("host.cpu_percent", "percent", &[], cpu as f64);
            }
        }
        for (code, what, kept, len, max) in [
            (
                "connections_truncated",
                "connections",
                " busiest",
                cluster.engine.peers.len(),
                self.cfg.snapshot_connections,
            ),
            ("members_truncated", "members", "", members.len(), self.cfg.snapshot_members),
        ] {
            if len > max {
                let message = format!(
                    "the snapshot shows the {max}{kept} of {len} {what}; TelemetryConfig::snapshot_{what} keeps more"
                );
                notices.push(Notice { code, message });
            }
        }
        cluster.engine.peers.truncate(self.cfg.snapshot_connections);
        members.truncate(self.cfg.snapshot_members);

        let leave = system.leave_reason();
        let mut hosted = self.cluster.hosted(self.cfg.snapshot_keys);
        // A key may be 64 KiB, and the snapshot is one control reply, one frame.
        let (budget, mut bytes) = (self.cluster.max_frame(axon::Lane::Live) / 4, 0);
        if let Some(fits) = hosted.keys.iter().position(|key| {
            bytes += serde_json::to_vec(key).map_or(budget, |json| json.len() + 1);
            bytes > budget
        }) {
            hosted.keys.truncate(fits);
        }
        for k in &hosted.kinds {
            let kind = [("kind", k.kind.as_str())];
            out.gauge("cluster.keyed", "actors", &kind, k.count as f64);
            for (how, n) in KEYED_STARTS.iter().zip(k.started) {
                out.counter("cluster.keyed_started", "actors", &[kind[0], ("how", how)], n);
            }
            for (reason, n) in KEYED_ENDS.iter().zip(k.ended) {
                out.counter("cluster.keyed_ended", "actors", &[kind[0], ("reason", reason)], n);
            }
        }
        if self.cfg.snapshot_keys > 0 && hosted.keys.len() < hosted.total {
            let (listed, total) = (hosted.keys.len(), hosted.total);
            let message = format!(
                "the snapshot lists {listed} of {total} keys, at most {} KiB of them; TelemetryConfig::snapshot_keys keeps more",
                budget >> 10
            );
            notices.push(Notice { code: "keys_truncated", message });
        }
        let (timestamp_ms, uptime_ms) = (crate::metrics::now_ms(), self.started.elapsed().as_millis() as u64);
        let mut state = self.state.lock();
        let (every, lost) = (self.cfg.rewrite_unchanged.as_millis() as u64, self.lost.swap(false, Ordering::Relaxed));
        let shown = std::mem::take(&mut state.latest);
        let samples = self.sampler.lock().changed(&mut out.0, shown, timestamp_ms, every, lost);
        (state.latest, state.exposition) = (out.0, None);
        state.sequence += 1;
        let batch = SampleBatch {
            node_id: self.cluster.identity().node_id.clone(),
            incarnation: self.incarnation.clone(),
            sequence: state.sequence,
            timestamp_ms,
            samples,
        };
        match postcard::to_stdvec(&batch) {
            Ok(encoded) if encoded.len() <= self.cfg.max_batch_bytes => {
                state.ring_bytes += encoded.len();
                state.ring.push_back((batch.sequence, encoded));
                while state.ring.len() > self.cfg.retained_samples || state.ring_bytes > self.cfg.retained_bytes {
                    let Some((_, evicted)) = state.ring.pop_front() else { break };
                    state.ring_bytes -= evicted.len();
                }
            }
            Ok(encoded) => {
                let message = format!("dropped a {} byte batch, limit {}", encoded.len(), self.cfg.max_batch_bytes);
                notices.push(Notice { code: "sample_batch_dropped", message });
                self.lost.store(true, Ordering::Relaxed);
            }
            Err(error) => {
                notices.push(Notice { code: "sample_batch_dropped", message: error.to_string() });
                self.lost.store(true, Ordering::Relaxed);
            }
        }
        let snapshot = NodeSnapshot {
            node_id: batch.node_id,
            incarnation: batch.incarnation,
            sequence: batch.sequence,
            timestamp_ms,
            uptime_ms,
            lifecycle: Lifecycle {
                phase: if leave.is_some() { "draining" } else { "running" },
                reason: leave.map(|reason| format!("{reason:?}")),
            },
            system: SystemCounters {
                started: stats.started,
                stopped: stats.stopped,
                failed: stats.failed,
                dead_letters: stats.dead_letters,
            },
            runtime,
            worker_utilisation,
            pool_utilisation,
            actor_types,
            actor_total,
            cluster,
            members,
            hosted,
            streams,
            process,
            host,
            notices,
        };
        state.snapshot = serde_json::to_vec(&snapshot).unwrap_or_default().into();
    }
}

/// `value`, or what it was `before` where that is higher: a counter, and each bucket of a histogram.
fn hold(value: &mut Value, before: &Value) {
    match (value, before) {
        (Value::Counter(n), Value::Counter(b)) => *n = (*n).max(*b),
        (Value::Histogram(h), Value::Histogram(b)) if h != b => {
            let (mut held, b) = (Histogram::from(&*h), Histogram::from(b));
            held.buckets.iter_mut().zip(&b.buckets).for_each(|(n, b)| *n = (*n).max(*b));
            (held.count, held.sum_ns) = (held.count.max(b.count), held.sum_ns.max(b.sum_ns));
            *h = HistogramSnapshot::from(&held);
        }
        _ => {}
    }
}

pub(crate) struct Out(pub(crate) Vec<Sample>);

impl Out {
    fn push(&mut self, name: &'static str, unit: &'static str, labels: &[(&'static str, &str)], value: Value) {
        let labels = labels.iter().map(|(k, v)| ((*k).into(), v.to_string())).collect();
        self.0.push(Sample { name: name.into(), unit: unit.into(), labels, value });
    }

    pub(crate) fn gauge(
        &mut self,
        name: &'static str,
        unit: &'static str,
        labels: &[(&'static str, &str)],
        value: f64,
    ) {
        self.push(name, unit, labels, Value::Gauge(value));
    }

    pub(crate) fn counter(
        &mut self,
        name: &'static str,
        unit: &'static str,
        labels: &[(&'static str, &str)],
        value: u64,
    ) {
        self.push(name, unit, labels, Value::Counter(value));
    }

    fn histogram(&mut self, name: &'static str, labels: &[(&'static str, &str)], h: &HistogramSnapshot) {
        if h.count() != 0 {
            self.push(name, "nanoseconds", labels, Value::Histogram(h.clone()));
        }
    }

    /// Converted only when it has a value: an actor type's timings, most of them empty, every second.
    fn dense(&mut self, name: &'static str, labels: &[(&'static str, &str)], h: &Histogram) {
        if h.count != 0 {
            self.histogram(name, labels, &h.into());
        }
    }

    fn by_reason(&mut self, name: &'static str, labels: &[(&'static str, &str)], reasons: &[&str], counts: &[u64]) {
        for (reason, count) in reasons.iter().zip(counts).filter(|(_, n)| **n != 0) {
            let mut labels = labels.to_vec();
            labels.push(("reason", reason));
            self.counter(name, "messages", &labels, *count);
        }
    }

    fn timings(&mut self, labels: &[(&'static str, &str)], t: &Timings) {
        self.dense("actor.mailbox_wait", labels, &t.mailbox_wait);
        self.dense("actor.handler", labels, &t.handler);
        self.dense("actor.turn", labels, &t.turn);
        self.dense("actor.timer_lag", labels, &t.timer_lag);
        for m in &t.messages {
            let mut labels = labels.to_vec();
            labels.push(("message", m.label));
            self.dense("actor.message.handler", &labels, &m.handler);
        }
    }

    fn actor_type(&mut self, t: &ActorTypeSnapshot) {
        let (labels, totals) = ([("actor_type", t.actor_type)], &t.totals);
        self.gauge("actor.count", "actors", &labels, t.live as f64);
        self.gauge("actor.mailbox_depth", "messages", &labels, t.mailbox_depth as f64);
        self.gauge("actor.mailbox_depth_max", "messages", &labels, t.mailbox_depth_max as f64);
        self.gauge("actor.mailbox_bytes", "bytes", &labels, t.mailbox_bytes as f64);
        self.gauge("actor.waiters", "senders", &labels, t.waiters as f64);
        self.gauge("actor.requests", "requests", &labels, t.requests as f64);
        self.gauge("actor.oldest_message_age", "nanoseconds", &labels, t.oldest_message_age_ns as f64);
        self.counter("actor.admitted", "messages", &labels, totals.admitted);
        self.counter("actor.processed", "messages", &labels, totals.processed);
        self.counter("actor.turns", "turns", &labels, totals.turns);
        self.counter("actor.budget_overruns", "turns", &labels, totals.budget_overruns);
        self.counter("actor.restarts", "restarts", &labels, totals.restarts);
        self.counter("actor.asks", "requests", &labels, totals.asks);
        self.counter("actor.ask_timeouts", "requests", &labels, totals.ask_timeouts);
        self.by_reason("actor.rejected", &labels, &RejectReason::NAMES, &totals.rejected);
        if let Some(timings) = &totals.timings {
            self.timings(&labels, timings);
        }
    }

    fn actor(&mut self, name: &str, a: &ActorDiagnostics) {
        let (labels, d) = ([("actor", name)], &a.diagnostics);
        self.gauge("actor.mailbox_depth", "messages", &labels, d.mailbox_depth as f64);
        self.gauge("actor.mailbox_bytes", "bytes", &labels, d.mailbox_bytes as f64);
        self.counter("actor.admitted", "messages", &labels, d.admitted);
        self.counter("actor.processed", "messages", &labels, d.processed);
        self.counter("actor.restarts", "restarts", &labels, a.restarts);
        self.by_reason("actor.rejected", &labels, &RejectReason::NAMES, &d.rejected);
        if let Some(timings) = &d.timings {
            self.timings(&labels, timings);
        }
    }

    fn streams(&mut self, s: &axon_stream::RelayStats) {
        let h = &s.hub;
        self.gauge("stream.streams", "streams", &[], h.streams as f64);
        self.gauge("stream.sessions", "sessions", &[], h.sessions as f64);
        self.gauge("stream.subscriptions", "subscriptions", &[], h.subscriptions as f64);
        self.gauge("stream.recovering", "subscriptions", &[], h.recovering as f64);
        self.gauge("stream.demand_keys", "streams", &[], h.demand_keys as f64);
        self.gauge("stream.queued_bytes", "bytes", &[], h.queued_bytes as f64);
        self.gauge("stream.queued_records", "records", &[], h.queued_records as f64);
        self.gauge("stream.originated", "streams", &[], s.originated as f64);
        self.gauge("stream.feeds", "streams", &[], s.feeds as f64);
        self.gauge("stream.orphaned", "streams", &[], s.orphaned as f64);
        self.counter("stream.published_batches", "batches", &[], h.published_batches);
        self.counter("stream.published_records", "records", &[], h.published_records);
        self.counter("stream.published_bytes", "bytes", &[], h.published_bytes);
        self.counter("stream.duplicate_records", "records", &[], h.duplicate_records);
        self.counter("stream.publish_rejected", "batches", &[], h.publish_rejected);
        self.counter("stream.delivered_records", "records", &[], h.delivered_records);
        self.counter("stream.delivered_bytes", "bytes", &[], h.delivered_bytes);
        self.counter("stream.replayed_records", "records", &[], h.replayed_records);
        self.counter("stream.recovery_blocked", "reads", &[], h.recovery_blocked);
        self.counter("stream.coalesced", "batches", &[], h.coalesced);
        self.by_reason("stream.resets", &[], &axon_stream::RESET_REASONS, &h.resets);
        self.by_reason("stream.ends", &[], &axon_stream::END_REASONS, &h.ends);
        self.by_reason("stream.quota_refused", &[], &axon_stream::NAMESPACE_LIMITS, &h.quota_refused);
        self.by_reason("stream.refusals", &[], &axon_stream::STATUSES, &s.refusals);
        self.counter("stream.parents_lost", "streams", &[], s.parents_lost);
        self.counter("stream.switches", "streams", &[], s.switches);
        self.counter("stream.local_resets", "streams", &[], s.local_resets);
        self.counter("stream.downstream_expired", "nodes", &[], s.downstream_expired);
        self.counter("stream.interests", "requests", &[("outcome", "sent")], s.interests_sent);
        self.counter("stream.interests", "requests", &[("outcome", "failed")], s.interests_failed);
        self.counter("stream.interests", "requests", &[("outcome", "resync")], s.resyncs);
        self.counter("stream.interest_keys", "keys", &[], s.interest_keys);
        self.gauge("stream.retained", "bytes", &[], s.retained_bytes as f64);
        if let Some(d) = &s.disk {
            self.gauge("stream.disk_streams", "streams", &[], d.streams as f64);
            self.gauge("stream.disk_bytes", "bytes", &[], d.bytes as f64);
            self.gauge("stream.disk_unwritten", "bytes", &[], d.unwritten_bytes as f64);
            self.gauge("stream.disk_failing", "bool", &[], d.failing.is_some() as u8 as f64);
            self.counter("stream.disk_writes", "writes", &[("outcome", "ok")], d.writes);
            self.counter("stream.disk_writes", "writes", &[("outcome", "failed")], d.write_failures);
            self.counter("stream.disk_refused", "batches", &[], d.refused);
            self.counter("stream.disk_truncated", "segments", &[], d.truncated);
        }
        self.counter("stream.unwanted_items", "events", &[], s.unwanted_items);
        for (direction, pushes, bytes) in
            [("in", s.pushes_in, s.push_bytes_in), ("out", s.pushes_out, s.push_bytes_out)]
        {
            self.counter("stream.pushes", "requests", &[("direction", direction)], pushes);
            self.counter("stream.push_bytes", "bytes", &[("direction", direction)], bytes);
        }
        let inflight: usize = s.downstream.iter().map(|d| d.inflight_bytes).sum();
        self.gauge("stream.push_inflight_bytes", "bytes", &[], inflight as f64);
        self.counter("stream.push_resent", "requests", &[], s.push_resent);
        self.by_reason("stream.push_skipped", &[], &axon_stream::PUSH_SKIPPED, &s.push_skipped);
        self.counter("stream.push_capped", "waits", &[], s.push_capped);
    }

    fn cluster(&mut self, c: &axon_cluster::ClusterStats, stale: bool, connections: usize) {
        let e = &c.engine;
        self.gauge("cluster.members", "nodes", &[], c.members as f64);
        self.gauge("cluster.unreachable", "nodes", &[], c.unreachable as f64);
        self.gauge("cluster.view_stale", "bool", &[], stale as u8 as f64);
        self.gauge("cluster.isolated", "bool", &[], c.isolated as u8 as f64);
        let sessions = e.peers.iter().filter(|p| p.down_ms.is_none()).count();
        self.gauge("cluster.connections", "connections", &[], sessions as f64);
        self.gauge("cluster.pending_requests", "requests", &[], e.pending as f64);
        self.gauge("cluster.endpoints", "endpoints", &[], e.endpoints as f64);
        self.gauge("cluster.spawned", "actors", &[], c.spawned as f64);
        for (kind, n) in
            [("joined", c.joined), ("left", c.left), ("replaced", c.replaced), ("unreachable", c.became_unreachable)]
        {
            self.counter("cluster.membership_changes", "changes", &[("kind", kind)], n);
        }
        for (outcome, n) in [
            ("succeeded", e.succeeded),
            ("rejected", e.rejected),
            ("timed_out", e.timed_out),
            ("unknown", e.unknown_outcomes),
            ("failed", e.failed),
            ("abandoned", e.abandoned),
        ] {
            self.counter("cluster.calls", "calls", &[("outcome", outcome)], n);
        }
        self.counter("cluster.calls_started", "calls", &[], e.calls);
        self.counter("cluster.inbound_calls", "calls", &[], e.inbound_calls);
        self.counter("cluster.inbound_expired", "calls", &[], e.inbound_expired);
        self.by_reason("cluster.inbound_rejected", &[], &REJECTION_NAMES, &e.inbound_rejected);
        self.histogram("cluster.request_rtt", &[], &e.request_rtt);
        self.counter("cluster.connects", "connections", &[], e.connects);
        self.counter("cluster.dial_failures", "dials", &[], e.dial_failures);
        self.counter("cluster.discovery_errors", "errors", &[], c.discovery_errors);
        self.counter("cluster.heartbeat_failures", "errors", &[], c.heartbeat_failures);
        self.counter("cluster.route_hits", "lookups", &[], c.route_hits);
        self.counter("cluster.route_misses", "lookups", &[], c.route_misses);
        self.counter("cluster.spawn_requests", "spawns", &[], c.spawn_requests);
        self.counter("cluster.spawn_refused", "spawns", &[], c.spawn_refused);
        self.counter("cluster.handoffs", "actors", &[], c.handoffs);
        self.counter("cluster.handoffs_failed", "actors", &[], c.handoffs_failed);
        self.counter("cluster.keyed_failed", "actors", &[], c.keyed_failed);
        self.counter("cluster.leases_lost", "leases", &[], c.leases_lost);
        self.counter("cluster.isolations", "events", &[], c.isolations);
        if let Some(q) = &c.quotas {
            self.gauge("quota.entries", "entries", &[], q.entries as f64);
            self.gauge("quota.permits", "permits", &[], q.permits as f64);
            self.gauge("quota.keys", "keys", &[], q.keys as f64);
            self.gauge("quota.rows", "rows", &[], q.rows as f64);
            self.gauge("quota.pulling", "bool", &[], q.pulling as u8 as f64);
            self.counter("quota.denied", "requests", &[("reason", "limit")], q.denied_limit);
            self.counter("quota.denied", "requests", &[("reason", "share")], q.denied_share);
            self.counter("quota.fallback", "units", &[], q.fallback);
            self.counter("quota.mismatched", "reports", &[], q.mismatched);
            self.counter("quota.reports", "reports", &[], q.reports);
            self.counter("quota.report_failures", "reports", &[], q.report_failures);
        }
        let l = &e.listener;
        self.counter("listener.accepted", "connections", &[], l.accepted);
        self.counter("listener.accept_errors", "errors", &[], l.accept_errors);
        self.counter("listener.rejected", "connections", &[("reason", "busy")], l.rejected_busy);
        self.counter("listener.rejected", "connections", &[("reason", "per_ip")], l.rejected_per_ip);
        let reasons = axon_net::HANDSHAKE_FAILURE_REASONS;
        self.by_reason("listener.handshake_failures", &[], &reasons, &l.handshake_failures);
        // A down lane is a row with no session, so no series.
        let sessions = e.peers.iter().filter(|p| p.down_ms.is_none());
        sessions.take(connections).for_each(|p| self.connection(p));
    }

    fn connection(&mut self, p: &PeerStats) {
        let direction = if p.outgoing { "out" } else { "in" };
        let labels = [("peer", p.node.as_str()), ("direction", direction), ("lane", p.lane.name())];
        let (s, t) = (&p.session, &p.session.timings);
        for (name, unit, n) in [
            ("connection.sent_bytes", "bytes", s.tx_wire_bytes),
            ("connection.received_bytes", "bytes", s.rx_wire_bytes),
            ("connection.sent_payload_bytes", "bytes", s.tx_payload_bytes),
            ("connection.received_payload_bytes", "bytes", s.rx_payload_bytes),
            ("connection.sent_frames", "frames", s.tx_frames),
            ("connection.received_frames", "frames", s.rx_frames),
            ("connection.sent_compressed_frames", "frames", s.tx_compressed_frames),
            ("connection.received_compressed_frames", "frames", s.rx_compressed_frames),
            ("connection.write_batches", "batches", s.tx_batches),
            ("connection.calls", "calls", p.calls),
            ("connection.paced", "nanoseconds", s.paced.as_nanos() as u64),
        ] {
            self.counter(name, unit, &labels, n);
        }
        for (name, unit, n) in [
            ("connection.queued_bytes", "bytes", s.outbound_reserved_bytes),
            ("connection.queued_frames", "frames", s.outbound_queued_frames),
            ("connection.inbound_queued_bytes", "bytes", s.inbound_reserved_bytes),
            ("connection.inbound_queued_frames", "frames", s.inbound_queued_frames),
            ("connection.pending_requests", "requests", p.pending),
        ] {
            self.gauge(name, unit, &labels, n as f64);
        }
        if s.rtt_ns != 0 {
            self.gauge("connection.rtt", "nanoseconds", &labels, s.rtt_ns as f64);
        }
        self.by_reason("connection.rejected_frames", &labels, &axon_net::SUBMIT_REASONS, &s.tx_rejected);
        if let Some(tcp) = &s.tcp {
            self.gauge("connection.tcp.rtt", "nanoseconds", &labels, tcp.rtt_ns as f64);
            self.gauge("connection.tcp.cwnd", "bytes", &labels, tcp.cwnd_bytes as f64);
            for (name, unit, n) in [
                ("connection.tcp.retransmitted_segments", "segments", tcp.retransmitted_segments),
                ("connection.tcp.retransmitted_bytes", "bytes", tcp.retransmitted_bytes),
            ] {
                if let Some(n) = n {
                    self.counter(name, unit, &labels, n);
                }
            }
        }
        self.histogram("connection.rtt_distribution", &labels, &t.rtt);
        self.histogram("connection.queue", &labels, &t.queue);
        self.histogram("connection.encode", &labels, &t.encode);
        self.histogram("connection.write", &labels, &t.write);
        self.histogram("connection.decode", &labels, &t.decode);
        self.histogram("connection.request_rtt", &labels, &p.request_rtt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_that_does_not_fit_is_halved_until_it_does_and_one_item_always_goes() {
        let encode = |items: &[u32]| serde_json::to_vec(items);
        assert_eq!(&fit((0..100).collect(), 20, encode).unwrap()[..], b"[0,1,2,3,4,5]");
        assert_eq!(&fit(vec![123_456], 2, encode).unwrap()[..], b"[123456]");
    }

    #[test]
    fn a_held_counter_or_bucket_never_goes_below_what_was_shown_and_a_gauge_is_not_held() {
        let mut counter = Value::Counter(5);
        hold(&mut counter, &Value::Counter(8));
        let mut gauge = Value::Gauge(1.0);
        hold(&mut gauge, &Value::Gauge(3.0));
        let (mut now, mut before) = (Histogram::default(), Histogram::default());
        (now.buckets[1], now.buckets[2], now.count, now.sum_ns) = (1, 7, 8, 70);
        (before.buckets[1], before.buckets[2], before.count, before.sum_ns) = (4, 2, 6, 50);
        let mut histogram = Value::Histogram((&now).into());
        hold(&mut histogram, &Value::Histogram((&before).into()));
        let Value::Histogram(h) = &histogram else { unreachable!() };
        assert_eq!((counter, gauge), (Value::Counter(8), Value::Gauge(1.0)));
        assert_eq!((h.first(), h.counts(), h.count(), h.sum_ns()), (1, &[4, 7][..], 8, 70));
    }

    /// An actor that exits while `sample` reads the system is in neither `retired` nor the page for that sample: its
    /// type may be missing, or present by an untimed actor with no histograms. What its type showed stays.
    #[test]
    fn a_types_counters_and_histograms_stay_shown_while_the_actors_behind_them_are_between_the_page_and_retired() {
        let mut sampler = Sampler::new(Sources::new(false, false));
        let series = |name: &'static str, (key, label): (&'static str, &str), value| Sample {
            name: name.into(),
            unit: "x".into(),
            labels: [(key.into(), label.to_string())].into(),
            value,
        };
        let brief = ("actor_type", "Brief");
        let mut turn = Histogram::default();
        (turn.buckets[0], turn.count, turn.sum_ns) = (3, 3, 30);
        let turn = || series("actor.turn", brief, Value::Histogram((&turn).into()));
        let names = |all: &[Sample]| {
            let mut names: Vec<_> = all.iter().map(|s| (s.name.to_string(), s.value.clone())).collect();
            names.sort_by(|a, b| a.0.cmp(&b.0));
            names
        };
        let mut first = vec![
            series("actor.turns", brief, Value::Counter(3)),
            turn(),
            series("actor.count", brief, Value::Gauge(1.0)),
            series("connection.frames_sent", ("peer", "b"), Value::Counter(9)),
        ];
        sampler.changed(&mut first, Vec::new(), 1_000, 30_000, false);

        // The next Brief is live and untimed; the timed one is in the gap.
        let mut gap =
            vec![series("actor.turns", brief, Value::Counter(1)), series("actor.count", brief, Value::Gauge(1.0))];
        let written = sampler.changed(&mut gap, first, 2_000, 30_000, false);
        assert_eq!(
            names(&gap),
            [
                ("actor.count".into(), Value::Gauge(1.0)),
                ("actor.turn".into(), turn().value),
                ("actor.turns".into(), Value::Counter(3)),
            ],
            "the type's counter held, its histogram carried, the gone peer's series left"
        );
        assert_eq!(names(&written), [("telemetry.samples_skipped".into(), Value::Counter(3))], "none changed");

        // No Brief at all: live, retired or paged.
        let mut none = Vec::new();
        sampler.changed(&mut none, gap, 3_000, 30_000, false);
        let carried: [(String, Value); 2] =
            [("actor.turn".into(), turn().value), ("actor.turns".into(), Value::Counter(3))];
        assert_eq!(names(&none), carried, "a gauge is not held");

        // Still carried once `every` is up: written again, so a store sees no gap in it either.
        let mut later = Vec::new();
        let written = sampler.changed(&mut later, none, 31_000, 30_000, false);
        assert_eq!(names(&later), carried);
        let skipped: (String, Value) = ("telemetry.samples_skipped".into(), Value::Counter(5));
        assert_eq!(names(&written), [carried[0].clone(), carried[1].clone(), skipped], "due again");
    }

    #[test]
    fn an_event_that_stands_for_several_says_how_many_in_its_detail() {
        let restarts = |count| axon::Event {
            seq: 7,
            at_us: 5_000,
            kind: "actor_restarted",
            subject: "worker".into(),
            detail: "boom".into(),
            count,
        };
        let told = event(&restarts(4), "a", "b0");
        assert_eq!(
            (&*told.id, told.time_ms, &*told.subject, &*told.detail),
            ("a:b0:e7", 5, "worker", "4 times · boom")
        );
        assert_eq!(event(&restarts(1), "a", "b0").detail, "boom");
    }
}
