#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axon::{ActorSystem, Histogram};
use axon_cluster::{
    AccessPolicy, ActorProtocol, BoxFuture, CallError, Cluster, Discovery, DiscoveryError, MemoryDiscovery,
    MemoryLeases, NodeRecord, Placement, Rejection, Reply, Snapshot, WireProtocol,
};
use axon_obs::*;
use axon_testkit::eventually;
use bytes::Bytes;
use parking_lot::Mutex;
use serde_json::Value as Json;

mod common;

use common::Kept;

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    common::loopback(id, discovery.clone(), ActorSystem::builder(), |_| {}).await
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn sampled() -> TelemetryConfig {
    // Sampled every 20 ms, so batches come within the test.
    TelemetryConfig { sample_interval: Duration::from_millis(20), access: AccessPolicy::Cluster, ..Default::default() }
}

fn only(nodes: &[&str]) -> AccessPolicy {
    AccessPolicy::Nodes(nodes.iter().map(|n| n.to_string()).collect())
}

/// Never answers, which is worse than refusing.
struct Hung;

impl MetricsSink for Hung {
    fn name(&self) -> &str {
        "hung"
    }

    fn write(&self, _: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        Box::pin(std::future::pending())
    }

    fn write_events(&self, _: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        Box::pin(std::future::pending())
    }

    fn write_spans(&self, _: &[SpanRecord]) -> MetricsFuture<'_, ()> {
        Box::pin(std::future::pending())
    }

    fn write_dead_letters(&self, _: &[DeadLetterRecord]) -> MetricsFuture<'_, ()> {
        Box::pin(std::future::pending())
    }

    /// 1 GB over a day, with 10 GB left.
    fn usage(&self) -> Option<SinkUsage> {
        let (first_ms, last_ms) = (Some(0), Some(86_400_000));
        let table = TableUsage { held_bytes: 1 << 30, first_ms, last_ms, ..Default::default() };
        Some(SinkUsage { tables: vec![table], free_bytes: Some(10 << 30), ..Default::default() })
    }
}

/// Refuses the first writes, and remembers what it stored.
struct Flaky {
    store: MemoryMetrics,
    refusals: AtomicU32,
    stored: Mutex<Vec<(String, u64)>>,
}

impl MetricsSink for Flaky {
    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        if !batches.is_empty() && self.refusals.try_update(Relaxed, Relaxed, |n| n.checked_sub(1)).is_ok() {
            return Box::pin(async { Err("disk on fire".into()) });
        }
        self.stored.lock().extend(batches.iter().map(|b| (b.node_id.clone(), b.sequence)));
        self.store.write(batches)
    }

    fn write_events(&self, events: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        self.store.write_events(events)
    }

    fn write_spans(&self, spans: &[SpanRecord]) -> MetricsFuture<'_, ()> {
        self.store.write_spans(spans)
    }

    fn write_dead_letters(&self, letters: &[DeadLetterRecord]) -> MetricsFuture<'_, ()> {
        self.store.write_dead_letters(letters)
    }

    fn usage(&self) -> Option<SinkUsage> {
        self.store.usage()
    }
}

impl MetricsQuery for Flaky {
    fn capabilities(&self) -> Capabilities {
        self.store.capabilities()
    }

    fn query(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        self.store.query(query)
    }

    fn series(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        self.store.series(query)
    }

    fn events(&self, query: &EventQuery) -> MetricsFuture<'_, Vec<ClusterEvent>> {
        self.store.events(query)
    }

    fn traces(&self, query: &TraceQuery) -> MetricsFuture<'_, Vec<TraceSummary>> {
        self.store.traces(query)
    }

    fn trace(&self, trace: u64) -> MetricsFuture<'_, Vec<SpanRecord>> {
        self.store.trace(trace)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_collector_stores_the_cluster_and_answers_history() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (c_system, c)) = (node("a", &discovery).await, node("c", &discovery).await);
    let (r_system, r) = node("r", &discovery).await;
    eventually("membership converges", || async { c.view().members.len() == 3 }).await;

    let telemetry = || TelemetryConfig {
        // Sampled every 50 ms, so batches come within the test.
        sample_interval: Duration::from_millis(50),
        access: only(&["a", "c"]),
        ..Default::default()
    };
    Telemetry::start(&a_system, &a, telemetry()).await.unwrap();
    Telemetry::start(&c_system, &c, telemetry()).await.unwrap();

    // `r` answers for telemetry itself: to `a` it lies about whose batches these are, to `c` it
    // hands one batch no sink can take ahead of a good one, and to both dead letters that are `a`'s.
    let incarnation = format!("{:032x}", r.identity().incarnation);
    r.serve_control(TELEMETRY_SERVICE, only(&["a", "c"]), move |request| {
        let after: Request = serde_json::from_slice(&request.body).unwrap();
        let fresh = matches!(after, Request::Samples { after: None });
        let batch = |sequence, value| SampleBatch {
            node_id: if request.peer.node_id == "a" { "c".into() } else { "r".into() },
            incarnation: incarnation.clone(),
            sequence,
            timestamp_ms: now_ms(),
            samples: vec![Sample { name: "forged".into(), unit: "".into(), labels: Default::default(), value }],
        };
        let page = SamplePage {
            schema: SAMPLES_SCHEMA,
            reset: false,
            gap: false,
            next: Cursor { incarnation: incarnation.clone(), sequence: 2 },
            batches: if fresh { vec![batch(1, Value::Gauge(f64::NAN)), batch(2, Value::Gauge(1.0))] } else { vec![] },
        };
        let spans = match after {
            Request::Spans { .. } => Some(serde_json::to_vec(&SpanPage::default()).unwrap()),
            Request::SettledDeadLetters { .. } => {
                let letters = vec![DeadLetterRecord { node_id: "a".into(), ..Default::default() }];
                Some(serde_json::to_vec(&DeadLetterPage { letters, ..Default::default() }).unwrap())
            }
            _ => None,
        };
        async move { Ok(spans.unwrap_or_else(|| postcard::to_stdvec(&page).unwrap()).into()) }
    })
    .unwrap();

    let provider =
        Arc::new(Flaky { store: MemoryMetrics::new(64 << 20), refusals: AtomicU32::new(3), stored: Mutex::default() });
    let query_only: Arc<dyn MetricsQuery> = provider.clone();
    let refused = MetricsService::with_providers(&r_system, &r, vec![], query_only.clone(), MetricsConfig::default());
    assert!(refused.await.err().unwrap().starts_with("collection_interval needs a sink"));
    // A query-only host collects nothing.
    let reader = MetricsConfig { collection_interval: None, access: only(&["r"]), ..Default::default() };
    MetricsService::with_providers(&r_system, &r, vec![], query_only.clone(), reader).await.unwrap();

    let cfg = MetricsConfig {
        // Collected every 100 ms, and a hung sink given up on after 30 ms, so cycles come within the test.
        collection_interval: Some(Duration::from_millis(100)),
        write_timeout: Duration::from_millis(30),
        access: only(&["r"]),
        ..Default::default()
    };
    let sinks: Vec<Arc<dyn MetricsSink>> = vec![Arc::new(Hung), provider.clone()];
    let service = MetricsService::with_providers(&c_system, &c, sinks, query_only, cfg.clone()).await.unwrap();
    MetricsService::start(&a_system, &a, provider.clone(), cfg).await.unwrap();

    eventually("history sources are announced", || async {
        history_sources(&r) == [("a", "memory"), ("c", "memory"), ("r", "memory")].map(|(n, s)| (n.into(), s.into()))
    })
    .await;
    assert!(c.set_metadata("axon.kinds", Some("x")).await.is_err(), "the cluster's own keys are refused");

    // Both collectors see each of d's records before the next one comes, so they give each change the same id: a
    // view that polls past a record tells its change under the next record's version. d's own leave announces a
    // draining record just before it goes, too briefly for both to be sure to see it, so its third leave is an
    // expiry too and it shuts down at the end.
    let (d_system, d) = node("d", &discovery).await;
    let (on_a, on_c) = (&a, &c);
    let both_see = move |n: usize| async move { on_a.view().members.len() == n && on_c.view().members.len() == n };
    eventually("d joins", || both_see(4)).await;
    for visit in 0..3 {
        discovery.expire("d");
        eventually("d drops out", || both_see(3)).await;
        if visit < 2 {
            discovery.register(&d.record()).await.unwrap();
            eventually("d is back", || both_see(4)).await;
        }
    }

    eventually("a few cycles are stored", || async {
        let health = service.health();
        health.cycles >= 6 && health.stored_batches >= 10
    })
    .await;
    let health: MetricsHealth = query_metrics(&r, "c", &MetricsRequest::Health).await.unwrap();
    assert!(health.write_errors >= 3, "{health:?}");
    assert!(health.collected_ago_ms.is_some_and(|ago| ago < 10_000), "on the collector's clock: {health:?}");
    let counts_up = || async { service.health().collected_ago_ms.is_some_and(|ago| ago >= 30) };
    eventually("the time since a cycle counts up between cycles 100 ms apart", counts_up).await;
    assert!(health.sink_usage["1:sink"].held_bytes > 0, "{health:?}");
    let hung = &health.sink_usage["0:hung"];
    let (growth, full) = (Some(1 << 30), Some(10 * 86_400_000));
    assert_eq!((hung.growth_bytes_per_day, hung.full_in_ms), (growth, full), "a hung sink still says what it holds");
    assert_eq!(health.rejected_batches, 1, "a batch no sink can take is skipped, not retried for ever: {health:?}");
    assert!(provider.stored.lock().contains(&("r".to_string(), 2)), "and the good one behind it is stored");
    let health_of_a: MetricsHealth = query_metrics(&r, "a", &MetricsRequest::Health).await.unwrap();
    assert!(health_of_a.node_errors["r"].contains("not its own"), "{health_of_a:?}");
    let r_error = "the node answered with dead letters that are not its own";
    assert_eq!(health.node_errors, BTreeMap::from([("r".into(), r_error.into())]), "{health:?}");
    assert_eq!(health.collection_gaps, 0, "{health:?}");
    let failing: Vec<_> = health.sink_errors.iter().collect();
    assert_eq!(format!("{failing:?}"), r#"[("0:hung", "storage write timed out")]"#, "a dead sink falls behind alone");

    let mut from_a: Vec<_> =
        provider.stored.lock().iter().filter(|(n, s)| n == "a" && *s < 1 << 63).map(|(_, s)| *s).collect();
    from_a.sort_unstable();
    from_a.dedup();
    let expected: Vec<u64> = (1..=from_a.len() as u64).collect();
    assert_eq!(from_a, expected, "a refused write is pulled again, by each of the two collectors");

    let root = axon::TraceContext::root();
    a_system.tracer().record(axon::Span::begin(root, 9, axon::SpanKind::Handle, "probe"));
    let recent = TraceQuery { to_ms: u64::MAX / 1000, limit: 10, ..Default::default() };
    eventually("the span is collected", || async {
        let found: Vec<TraceSummary> = query_metrics(&r, "c", &MetricsRequest::Traces(recent.clone())).await.unwrap();
        found.iter().any(|t| (&*t.trace, &*t.root, &*t.root_node) == (&*hex(root.trace.get()), "probe", "a"))
    })
    .await;
    eventually("the collector with a hung sink stores spans in its healthy one", || async {
        service.health().stored_spans > 0
    })
    .await;
    let stored = MetricsRequest::Trace { trace: hex(root.trace.get()) };
    let stored: Vec<SpanRecord> = query_metrics(&r, "c", &stored).await.unwrap();
    assert_eq!(stored.iter().map(|s| &*s.span).collect::<Vec<_>>(), [hex(9)]);
    assert!(query_metrics::<Vec<SpanRecord>>(&r, "c", &MetricsRequest::Trace { trace: "1".into() }).await.is_err());

    let now = now_ms();
    // A batch sampled before `now` may still be on its way, and the next one stored moves how far a value is held:
    // the window stops changing once a batch of a and of c from past it is stored, as each collector stores in order.
    let past = HistoryQuery::new("runtime.actors", now + 1000, now + 61_000, 1000);
    eventually("a and c are stored past the window", || async {
        let past: HistoryResult = query_metrics(&r, "c", &MetricsRequest::Query(past.clone())).await.unwrap();
        past.series.iter().map(|s| s.node_id.as_str()).collect::<Vec<_>>() == ["a", "c"]
    })
    .await;
    let actors = HistoryQuery::new("runtime.actors", now - 60_000, now, 1000);
    let history: HistoryResult = query_metrics(&r, "c", &MetricsRequest::Query(actors.clone())).await.unwrap();
    let nodes: Vec<_> = history.series.iter().map(|s| s.node_id.as_str()).collect();
    assert_eq!(nodes, ["a", "c"]);
    assert!(history.series.iter().all(|s| s.kind == "gauge" && s.values.iter().any(Option::is_some)));
    let history_of_actors = history;
    // A counter shows from its second step, the gain over the first, which may come after `now`.
    let own = HistoryQuery::new("collector.cycles", now - 60_000, now + 60_000, 1000);
    eventually("each collector stores its own cycles", || async {
        let history: HistoryResult = query_metrics(&r, "c", &MetricsRequest::Query(own.clone())).await.unwrap();
        history.series.iter().map(|s| s.node_id.as_str()).collect::<Vec<_>>() == ["a", "c"]
    })
    .await;

    let page = MetricsRequest::Queries(vec![actors.clone(), HistoryQuery { step_ms: 1, ..actors.clone() }]);
    let answers: Vec<Result<HistoryResult, String>> = query_metrics(&r, "c", &page).await.unwrap();
    assert_eq!(answers[0].as_ref().unwrap(), &history_of_actors);
    assert!(answers[1].as_ref().unwrap_err().contains("step_ms must be"));
    let too_many = MetricsRequest::Queries(vec![actors.clone(); 33]);
    assert!(query_metrics::<Json>(&r, "c", &too_many).await.unwrap_err().contains("at most 32"));

    let bad = HistoryQuery { step_ms: 1, ..actors.clone() };
    let error = query_metrics::<HistoryResult>(&r, "c", &MetricsRequest::Query(bad)).await.unwrap_err();
    assert!(error.contains("step_ms must be"), "{error}");
    let same: HistoryResult = query_metrics(&r, "r", &MetricsRequest::Query(actors)).await.unwrap();
    assert_eq!(same.series.len(), 2, "a query-only host reads what the collector wrote");
    let capabilities: Capabilities = query_metrics(&r, "r", &MetricsRequest::Capabilities).await.unwrap();
    assert_eq!(capabilities.storage, "memory");
    let refused = query_metrics::<Capabilities>(&a, "c", &MetricsRequest::Capabilities).await.unwrap_err();
    assert_eq!(
        refused,
        "c refuses to answer: its MetricsConfig::access (default Deny) or ClusterConfig::authorizer does not admit this node"
    );

    eventually("d's visit is recorded", || async {
        let request = MetricsRequest::Events(EventQuery { limit: 100, ..Default::default() });
        let events: Vec<ClusterEvent> = query_metrics(&r, "c", &request).await.unwrap();
        assert!(events.windows(2).all(|w| w[0].time_ms >= w[1].time_ms));
        let ids = |kind: &str| -> Vec<String> {
            events.iter().filter(|e| e.node_id == "d" && e.kind == kind).map(|e| e.id.clone()).collect()
        };
        let (joined, left) = (ids("joined"), ids("left"));
        assert!(joined.len() <= 3 && left.len() <= 3, "two collectors, one id per change: {joined:?} {left:?}");
        assert!(joined.iter().all(|id| id.starts_with("d:joined:") && id.contains(":v")), "{joined:?}");
        joined.len() == 3 && left.len() == 3
    })
    .await;

    let failures = c_system.shutdown().await.unwrap().failures;
    assert!(failures.len() == 1 && failures[0].contains("0:hung: storage write timed out"), "{failures:?}");
    drop(d);
    for system in [a_system, d_system, r_system] {
        assert!(system.shutdown().await.unwrap().failures.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_reply_or_a_new_collector_never_gives_a_sink_the_same_batch_twice() {
    let discovery = MemoryDiscovery::new();
    let (a_system, a) = node("a", &discovery).await;
    Telemetry::start(&a_system, &a, sampled()).await.unwrap();

    let sink = Arc::new(Kept { lost_replies: AtomicU32::new(3), ..Default::default() });
    // Collected every 100 ms, so batches come within the test.
    let collect = || MetricsConfig { collection_interval: Some(Duration::from_millis(100)), ..Default::default() };
    for collector in ["k1", "k2"] {
        let (system, cluster) = node(collector, &discovery).await;
        eventually("the collector sees a", || async { cluster.view().get("a").is_some() }).await;
        let query = Arc::new(MemoryMetrics::new(1 << 20));
        MetricsService::with_providers(&system, &cluster, vec![sink.clone()], query, collect()).await.unwrap();
        let before = sink.of("a", None).len();
        eventually("batches of a arrive", || async { sink.of("a", None).len() >= before + 10 }).await;
        system.shutdown().await.unwrap();
    }
    assert_eq!(sink.lost_replies.load(Relaxed), 0);
    sink.once_each("a", None);
}

/// Stores, then panics before it can say so, on every third write.
#[derive(Default)]
struct Panicky(Kept, AtomicU32);

impl MetricsSink for Panicky {
    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        let stored = self.0.write(batches);
        if !batches.is_empty() && self.1.fetch_add(1, Relaxed) % 3 == 2 {
            panic!("the sink fell over after storing");
        }
        stored
    }

    fn stored_through(&self, node: &str, incarnation: &str) -> MetricsFuture<'_, Option<u64>> {
        self.0.stored_through(node, incarnation)
    }

    fn write_events(&self, _: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct Collecting {
    service: MetricsService,
    discovery: MemoryDiscovery,
    nodes: Vec<(ActorSystem, Cluster)>,
}

impl Collecting {
    async fn stop(self) {
        for (system, _) in self.nodes {
            system.shutdown().await.unwrap();
        }
    }
}

/// `k` collecting `a` into `sink`, every tenth of a second.
async fn collecting(sink: Arc<dyn MetricsSink>, tune: impl FnOnce(&mut MetricsConfig)) -> Collecting {
    let discovery = MemoryDiscovery::new();
    let (a_system, a) = node("a", &discovery).await;
    Telemetry::start(&a_system, &a, sampled()).await.unwrap();
    let (system, cluster) = node("k", &discovery).await;
    eventually("the collector sees a", || async { cluster.view().get("a").is_some() }).await;
    // Collected every 100 ms, so batches come within the test.
    let mut collect = MetricsConfig { collection_interval: Some(Duration::from_millis(100)), ..Default::default() };
    tune(&mut collect);
    let query = Arc::new(MemoryMetrics::new(1 << 20));
    let service = MetricsService::with_providers(&system, &cluster, vec![sink], query, collect).await.unwrap();
    Collecting { service, discovery, nodes: vec![(a_system, a), (system, cluster)] }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sink_that_panics_after_storing_is_asked_where_it_stands_and_gets_each_batch_once() {
    let sink = Arc::new(Panicky::default());
    let run = collecting(sink.clone(), |_| {}).await;
    eventually("batches of a arrive past several panics", || async {
        sink.0.of("a", None).len() >= 30 && sink.1.load(Relaxed) >= 9
    })
    .await;
    sink.0.once_each("a", None);
    run.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_past_max_nodes_is_an_error_not_a_cycle_that_asks_them_all() {
    let sink = Arc::new(Kept::default());
    let run = collecting(sink.clone(), |cfg| cfg.max_nodes = 1).await;
    eventually("the cycle refuses", || async {
        run.service.health().node_errors.get("k").is_some_and(|e| e == "2 members exceed max_nodes 1")
    })
    .await;
    assert!(sink.of("a", None).is_empty(), "a was collected all the same");
    run.stop().await;
}

/// Fails every write while the disk is full, as a store out of space does.
#[derive(Default)]
struct Full {
    full: std::sync::atomic::AtomicBool,
    kept: Kept,
    events: Mutex<Vec<ClusterEvent>>,
}

impl MetricsSink for Full {
    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        if self.full.load(Relaxed) {
            return Box::pin(async { Err("No space left on device (os error 28)".into()) });
        }
        self.kept.write(batches)
    }

    fn stored_through(&self, node: &str, incarnation: &str) -> MetricsFuture<'_, Option<u64>> {
        self.kept.stored_through(node, incarnation)
    }

    fn write_events(&self, events: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        if self.full.load(Relaxed) {
            return Box::pin(async { Err("No space left on device (os error 28)".into()) });
        }
        self.events.lock().extend(events.iter().cloned());
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_disk_is_the_sinks_error_until_it_frees_and_then_every_batch_is_stored_once() {
    let sink = Arc::new(Full { full: true.into(), ..Default::default() });
    let run = collecting(sink.clone(), |_| {}).await;
    let service = &run.service;
    eventually("the sink says why", || async {
        let health = service.health();
        health.cycles >= 5 && health.sink_errors.values().any(|e| e.contains("No space left"))
    })
    .await;
    assert!(sink.kept.of("a", None).is_empty());
    sink.full.store(false, Relaxed);
    eventually("it catches up and clears", || async {
        sink.kept.of("a", None).len() >= 30 && service.health().sink_errors.is_empty()
    })
    .await;
    let stored = sink.kept.once_each("a", None);
    assert_eq!(stored[0], 1, "the first batches were lost");
    assert_eq!(service.health().collection_gaps, 0);
    run.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn events_a_sink_cannot_take_are_held_up_to_the_buffer_and_the_oldest_are_dropped_and_counted() {
    let sink = Arc::new(Full { full: true.into(), ..Default::default() });
    let mut run = collecting(sink.clone(), |cfg| (cfg.event_buffer, cfg.access) = (2, only(&["k"]))).await;
    let service = run.service.clone();
    for id in ["b", "c", "d", "e", "f"] {
        let joined = node(id, &run.discovery).await;
        run.nodes.push(joined);
    }
    eventually("the oldest are dropped", || async {
        let health = service.health();
        health.dropped_events >= 3 && health.pending_events == 2
    })
    .await;
    // What is held is answered, filtered as the store would be.
    let collector = run.nodes[1].1.clone();
    let held = async |node: &str, kind: &str| {
        let query = EventQuery { node: node.into(), kind: kind.into(), ..Default::default() };
        query_metrics::<Vec<ClusterEvent>>(&collector, "k", &MetricsRequest::Events(query)).await.unwrap()
    };
    let mut each = Vec::new();
    for node in ["a", "k", "b", "c", "d", "e", "f"] {
        let of = held(node, "").await;
        assert!(of.iter().all(|e| e.node_id == node), "asked for {node}: {of:?}");
        each.extend(of);
    }
    assert_eq!(each.len(), 2, "each held event is one node's: {each:?}");
    let joins = held("", "joined").await;
    assert!(joins.iter().all(|e| e.kind == "joined"), "{joins:?}");
    sink.full.store(false, Relaxed);
    eventually("what is held is stored", || async { service.health().pending_events == 0 }).await;
    let stored: Vec<_> = sink.events.lock().iter().map(|e| e.node_id.clone()).collect();
    assert!(stored.contains(&"f".to_string()), "the newest are kept: {stored:?}");
    run.stop().await;
}

/// Fails on every message, so each one is a restart.
struct Failing;

impl axon::Actor for Failing {
    type Message = ();
    type Error = &'static str;
    fn on_message(&mut self, _: (), _: &mut axon::Context<'_, Self>) -> Result<(), &'static str> {
        Err("boom")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_collector_stores_each_nodes_timeline_once_and_answers_it_by_node_and_kind() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (k_system, k)) = (node("a", &discovery).await, node("k", &discovery).await);
    let (d_system, d) = node("d", &discovery).await;
    eventually("membership converges", || async { k.view().members.len() == 3 }).await;
    let telemetry = || TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    Telemetry::start(&a_system, &a, telemetry()).await.unwrap();
    Telemetry::start(&k_system, &k, telemetry()).await.unwrap();
    Telemetry::start(&d_system, &d, telemetry()).await.unwrap();
    let store = Arc::new(MemoryMetrics::new(1 << 20));
    let cfg = MetricsConfig {
        // Collected every 100 ms, so events come within the test.
        collection_interval: Some(Duration::from_millis(100)),
        access: only(&["k"]),
        ..Default::default()
    };
    let service = MetricsService::start(&k_system, &k, store, cfg).await.unwrap();
    let events = |query: EventQuery| {
        let k = k.clone();
        async move { query_metrics::<Vec<ClusterEvent>>(&k, "k", &MetricsRequest::Events(query)).await.unwrap() }
    };
    fn kinds(events: &[ClusterEvent]) -> Vec<(&str, &str, &str)> {
        events.iter().map(|e| (&*e.node_id, &*e.kind, &*e.subject)).collect()
    }

    a_system.events().record("acquired", "orders/cust-7", "epoch 3");
    // Restarted at once, as often as it fails: one event now, the rest in a minute.
    let backoff = axon::Backoff { jitter: 0.0, ..axon::RestartPolicy::BACKOFF };
    let policy = axon::RestartPolicy { max_restarts: 100, backoff, ..Default::default() };
    let worker = a_system.supervise(|| Failing).name("worker").policy(policy).start_supervised().unwrap();
    for n in 1..=3 {
        worker.send(()).await.unwrap();
        eventually("it restarts", || async { worker.family.restarts() == n }).await;
    }
    // A node that comes back under a new boot is read from its start, not from where its last boot was.
    let handed = EventQuery { node: "d".into(), kind: "handed_off".into(), ..Default::default() };
    for _ in 0..2 {
        d_system.events().record("handed_off", "orders/cust-9", "to a");
    }
    eventually("d's first boot is pulled", || async { events(handed.clone()).await.len() == 2 }).await;
    d_system.shutdown().await.unwrap();
    drop(d);
    eventually("d is gone", || async { k.view().get("d").is_none() }).await;
    let (d_system, d) = node("d", &discovery).await;
    Telemetry::start(&d_system, &d, telemetry()).await.unwrap();
    d_system.events().record("handed_off", "orders/cust-9", "to k");
    eventually("d's new boot is pulled", || async { events(handed.clone()).await.len() == 3 }).await;
    let boot = |e: &ClusterEvent| e.id.rsplit_once(':').map(|(boot, _)| boot.to_owned());
    let handed = events(handed).await;
    assert!(handed[0].id.ends_with(":e1") && handed[0].detail == "to k", "{handed:?}");
    assert!(boot(&handed[0]) != boot(&handed[1]) && boot(&handed[1]) == boot(&handed[2]), "{handed:?}");
    let from_a = EventQuery { node: "a".into(), ..Default::default() };
    eventually("a's events and d's restart are stored", || async {
        let restarted = EventQuery { node: "d".into(), kind: "restarted".into(), ..Default::default() };
        events(from_a.clone()).await.len() == 2 && events(restarted).await.len() == 1
    })
    .await;
    let a_told = events(from_a.clone()).await;
    assert_eq!(kinds(&a_told), [("a", "actor_restarted", "worker"), ("a", "acquired", "orders/cust-7")]);
    assert_eq!(a_told[1].detail, "epoch 3");
    assert!(a_told[1].id.starts_with("a:") && a_told[1].id.ends_with(":e1"), "{:?}", a_told[1].id);
    let acquired = events(EventQuery { kind: "acquired".into(), ..Default::default() }).await;
    assert_eq!(kinds(&acquired), [("a", "acquired", "orders/cust-7")], "filtered by kind alone");
    let d_told = events(EventQuery { node: "d".into(), ..Default::default() }).await;
    let d_kinds: Vec<_> = d_told.iter().map(|e| &*e.kind).filter(|k| *k != "handed_off").collect();
    assert_eq!(d_kinds[0], "restarted", "a node back with a new boot is a restart, not a join: {d_told:?}");
    assert!(d_kinds.contains(&"left") && !d_kinds.contains(&"joined"), "{d_told:?}");

    // Cycles later each is still stored once, and a burst past the node's ring is counted as dropped.
    eventually("more cycles run", || async { service.health().cycles >= 3 }).await;
    let cycles = service.health().cycles;
    for n in 0..300 {
        a_system.events().record("acquired", format!("orders/{n}"), "epoch 1");
    }
    eventually("the burst is pulled", || async { service.health().cycles >= cycles + 3 }).await;
    let everything = events(EventQuery::default()).await;
    let mut ids: Vec<_> = everything.iter().map(|e| e.id.clone()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), everything.len(), "each event is stored once");
    assert_eq!(events(from_a).await.len(), 2 + 256, "what a's ring still held");
    assert_eq!(service.health().dropped_events, 44, "the events a's ring let go of before they were pulled");

    for system in [a_system, d_system, k_system] {
        assert!(system.shutdown().await.unwrap().failures.is_empty());
    }
}

/// A keyed actor that answers with what it is sent.
struct Echo;

impl axon::Actor for Echo {
    type Message = (Bytes, Option<Reply>);
    type Error = ();

    fn on_message(&mut self, (body, reply): Self::Message, _: &mut axon::Context<'_, Self>) -> Result<(), ()> {
        drop(reply.map(|r| r.send(body)));
        Ok(())
    }
}

struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = 0xec40;
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

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_actor_that_moves_to_a_joining_node_is_on_the_stored_timeline_once() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let leased = |id: &'static str| {
        let authority = Arc::new(leases.authority());
        common::loopback(id, discovery.clone(), ActorSystem::builder(), move |cfg| cfg.ownership = Some(authority))
    };
    let (k_system, k) = node("k", &discovery).await;
    let ((a_system, a), (b_system, b)) = (leased("a").await, leased("b").await);
    let telemetry = || TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    for (system, cluster) in [(&k_system, &k), (&a_system, &a), (&b_system, &b)] {
        Telemetry::start(system, cluster, telemetry()).await.unwrap();
    }
    for cluster in [&a, &b] {
        cluster.kind::<EchoApi>("orders").register(|_, _| Ok(Echo)).await.unwrap();
    }
    let cfg = MetricsConfig {
        // Collected every 100 ms, so events come within the test.
        collection_interval: Some(Duration::from_millis(100)),
        access: only(&["k"]),
        ..Default::default()
    };
    let _service = MetricsService::start(&k_system, &k, Arc::new(MemoryMetrics::new(1 << 20)), cfg).await.unwrap();
    let events = |kind: &str, node: &str| {
        let (k, query) = (k.clone(), EventQuery { kind: kind.into(), node: node.into(), ..Default::default() });
        async move { query_metrics::<Vec<ClusterEvent>>(&k, "k", &MetricsRequest::Events(query)).await.unwrap() }
    };
    let anywhere = Placement::default();
    eventually("a and b host orders", || async {
        a.view().members.iter().filter(|m| m.record.has_kind("orders")).count() == 2
    })
    .await;
    let keys: Vec<String> = (0..12).map(|n| format!("cust-{n}")).collect();
    for key in &keys {
        let placed = a.get_or_spawn::<EchoApi>("orders", key, Bytes::new(), &anywhere).await.unwrap();
        assert_eq!(placed.epoch(), Some(1));
    }

    // c joins; the keys it now owns drain where they ran and start on c under the next epoch.
    let (c_system, c) = leased("c").await;
    Telemetry::start(&c_system, &c, telemetry()).await.unwrap();
    c.kind::<EchoApi>("orders").register(|_, _| Ok(Echo)).await.unwrap();
    eventually("a sees c host orders", || async {
        a.view().members.iter().any(|m| m.record.id == "c" && m.record.has_kind("orders"))
    })
    .await;
    let moved: Vec<String> = keys
        .iter()
        .filter(|key| a.owner("orders", key, &anywhere).is_some_and(|o| &*o == "c"))
        .map(|key| format!("orders/{key}"))
        .collect();
    assert!(!moved.is_empty(), "c owns some of the keys");
    eventually("each move is stored", || async {
        events("handed_off", "").await.len() == moved.len() && events("acquired", "c").await.len() == moved.len()
    })
    .await;
    eventually("more cycles run", || async { _service.health().cycles >= 3 }).await;

    let mut handed = events("handed_off", "").await;
    handed.sort_by(|x, y| x.subject.cmp(&y.subject));
    let mut subjects: Vec<_> = handed.iter().map(|e| e.subject.clone()).collect();
    subjects.dedup();
    let mut expected = moved.clone();
    expected.sort();
    assert_eq!(subjects, expected, "each moved key handed off once: {handed:?}");
    assert!(handed.iter().all(|e| e.node_id != "c" && e.detail == "to c, epoch 1"), "{handed:?}");
    let arrived = events("acquired", "c").await;
    assert!(arrived.iter().all(|e| e.detail == "epoch 2" && moved.contains(&e.subject)), "{arrived:?}");
    assert!(events("lease_lost", "").await.is_empty(), "a handoff releases the lease, it is not lost");

    for system in [a_system, b_system, c_system, k_system] {
        assert!(system.shutdown().await.unwrap().failures.is_empty());
    }
}

/// Numbers each boot's record versions from 1, as seed discovery does.
#[derive(Clone, Default)]
struct PerBoot(MemoryDiscovery, Arc<Mutex<BTreeMap<u128, u64>>>);

impl Discovery for PerBoot {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        Box::pin(async move {
            let mut snapshot = self.0.snapshot().await?;
            let mut first = self.1.lock();
            for record in &mut snapshot.nodes {
                record.version -= *first.entry(record.incarnation).or_insert(record.version) - 1;
            }
            Ok(snapshot)
        })
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.register(record)
    }

    fn unregister(&self, id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        self.0.unregister(id, incarnation)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_versions_restart_at_each_boot_is_on_the_stored_timeline_for_each_boot() {
    let discovery = PerBoot::default();
    let (k_system, k) = common::loopback("k", discovery.clone(), ActorSystem::builder(), |_| {}).await;
    let cfg = MetricsConfig {
        // Collected every 100 ms, so events come within the test.
        collection_interval: Some(Duration::from_millis(100)),
        access: only(&["k"]),
        ..Default::default()
    };
    let service = MetricsService::start(&k_system, &k, Arc::new(MemoryMetrics::new(1 << 20)), cfg).await.unwrap();
    for _ in 0..2 {
        let (d_system, d) = common::loopback("d", discovery.clone(), ActorSystem::builder(), |_| {}).await;
        eventually("d joins", || async { k.view().get("d").is_some() }).await;
        d_system.shutdown().await.unwrap();
        drop(d);
        eventually("d is gone", || async { k.view().get("d").is_none() }).await;
    }
    let cycles = service.health().cycles;
    eventually("more cycles run", || async { service.health().cycles >= cycles + 3 }).await;
    let query = MetricsRequest::Events(EventQuery { node: "d".into(), ..Default::default() });
    let events: Vec<ClusterEvent> = query_metrics(&k, "k", &query).await.unwrap();
    let kinds: Vec<_> =
        events.iter().map(|e| &*e.kind).filter(|k| ["joined", "restarted", "left"].contains(k)).collect();
    assert_eq!(kinds, ["left", "restarted", "left", "joined"], "{events:?}");

    assert!(k_system.shutdown().await.unwrap().failures.is_empty());
}

/// Holds events and spans, and says it keeps neither.
struct Unannounced(MemoryMetrics);

impl MetricsQuery for Unannounced {
    fn capabilities(&self) -> Capabilities {
        Capabilities { events: false, traces: false, ..self.0.capabilities() }
    }

    fn query(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        self.0.query(query)
    }

    fn events(&self, query: &EventQuery) -> MetricsFuture<'_, Vec<ClusterEvent>> {
        self.0.events(query)
    }

    fn traces(&self, query: &TraceQuery) -> MetricsFuture<'_, Vec<TraceSummary>> {
        self.0.traces(query)
    }

    fn trace(&self, trace: u64) -> MetricsFuture<'_, Vec<SpanRecord>> {
        self.0.trace(trace)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_collector_answers_events_and_traces_only_where_its_provider_says_it_keeps_them() {
    let (system, cluster) = node("k", &MemoryDiscovery::new()).await;
    let store = MemoryMetrics::new(1 << 20);
    let span = SpanRecord { trace: hex(7), span: hex(1), kind: "handle".into(), start_us: 1_000, ..Default::default() };
    store.write_spans(&[span]).await.unwrap();
    let event = ClusterEvent {
        id: "e".into(),
        time_ms: 1,
        node_id: "k".into(),
        kind: "joined".into(),
        subject: String::new(),
        detail: String::new(),
    };
    store.write_events(&[event]).await.unwrap();
    let provider = Arc::new(Unannounced(store));
    let (traces, events) =
        (TraceQuery { to_ms: u64::MAX / 1000, limit: 10, ..Default::default() }, EventQuery::default());
    assert_eq!(provider.traces(&traces).await.unwrap().len(), 1, "the provider answers when asked itself");
    assert_eq!(provider.events(&events).await.unwrap().len(), 1);

    // A query-only host: with no sink it collects nothing.
    let cfg = MetricsConfig { collection_interval: None, access: only(&["k"]), ..Default::default() };
    MetricsService::with_providers(&system, &cluster, vec![], provider, cfg).await.unwrap();
    let asked = [
        (MetricsRequest::Traces(traces), "this provider keeps no traces"),
        (MetricsRequest::Trace { trace: hex(7) }, "this provider keeps no traces"),
        (MetricsRequest::Events(events), "this provider has no events"),
    ];
    for (request, refusal) in asked {
        let answer = query_metrics::<Json>(&cluster, "k", &request).await;
        assert!(answer.as_ref().is_err_and(|e| e.contains(refusal)), "{request:?}: {answer:?}");
    }
    assert!(system.shutdown().await.unwrap().failures.is_empty());
}

fn batches(node: &str, base: u64) -> Vec<SampleBatch> {
    let mut histogram = Histogram::default();
    (1..=4u64)
        .map(|i| {
            histogram.record(Duration::from_micros(i * 100));
            let sample = |name: &'static str, labels: &[(&'static str, &str)], value| Sample {
                name: name.into(),
                unit: "things".into(),
                labels: labels.iter().map(|(k, v)| ((*k).into(), v.to_string())).collect(),
                value,
            };
            SampleBatch {
                node_id: node.into(),
                incarnation: "00ff".into(),
                sequence: i,
                timestamp_ms: base + (i - 1) * 1000,
                samples: vec![
                    sample("t.gauge", &[], Value::Gauge(i as f64)),
                    sample("t.counter", &[("peer", "x")], Value::Counter(i * 10)),
                    sample("t.counter", &[("peer", "y\t'\\")], Value::Counter(i * 100)),
                    sample("t.latency", &[("peer", "x")], Value::Histogram((&histogram).into())),
                ],
            }
        })
        .collect()
}

async fn contract<P: MetricsSink + MetricsQuery>(provider: &P) {
    let node = format!("n{}", now_ms());
    let base = now_ms() / 2000 * 2000 - 10_000;
    let written = batches(&node, base);
    provider.write(&written).await.unwrap();
    provider.write(&written).await.unwrap();

    let query =
        |metric: &str| HistoryQuery { node: Some(node.clone()), ..HistoryQuery::new(metric, base, base + 9999, 2000) };
    // The known numbers of each series.
    let values = |result: &HistoryResult| -> Vec<Vec<f64>> {
        let number = |r: &Reading| if let Reading::Number(n) = r { Some(*n) } else { None };
        result.series.iter().map(|s| s.values.iter().flatten().filter_map(number).collect()).collect()
    };
    let gauge = provider.query(&query("t.gauge")).await.unwrap();
    assert_eq!((gauge.from, gauge.step, gauge.count, gauge.truncated), (base, 2000, 5, false));
    let (two, four) = (Some(Reading::Number(2.0)), Some(Reading::Number(4.0)));
    assert_eq!(gauge.series[0].values, [two, four, None, None, None], "held no further than the node was stored");
    assert_eq!(gauge.settled, (base + 3000 - HELD_MS) / 2000 * 2000);
    assert_eq!((gauge.series[0].kind.as_str(), gauge.series[0].unit.as_str()), ("gauge", "things"));
    for (aggregation, expected) in
        [(Aggregation::Mean, [1.5, 3.5]), (Aggregation::Min, [1.0, 3.0]), (Aggregation::Max, [2.0, 4.0])]
    {
        let result = provider.query(&HistoryQuery { aggregation, ..query("t.gauge") }).await.unwrap();
        assert_eq!(values(&result), [expected], "{aggregation:?} over a write that was retried");
    }

    // A counter is its gain over each step, so the first step, with nothing before it, is unknown.
    let counters = provider.query(&query("t.counter")).await.unwrap();
    assert_eq!(values(&counters), [[20.0], [200.0]]);
    let mut odd = query("t.counter");
    odd.labels.insert("peer".into(), "y\t'\\".into());
    let filtered = provider.query(&odd).await.unwrap();
    assert_eq!(values(&filtered), [[200.0]]);
    assert_eq!(filtered.series[0].labels["peer"], "y\t'\\");
    let summed = provider.query(&HistoryQuery { sum: Some(Sum::Cluster), ..query("t.counter") }).await.unwrap();
    assert_eq!((values(&summed), summed.series[0].summed), (vec![vec![220.0]], Some(2)));

    let latency = provider.query(&HistoryQuery { aggregation: Aggregation::Mean, ..query("t.latency") }).await.unwrap();
    let histogram = |batch: &SampleBatch| match &batch.samples[3].value {
        Value::Histogram(h) => Histogram::from(h),
        _ => unreachable!("the fourth sample is the histogram"),
    };
    let gained = histogram(&written[3]).since(&histogram(&written[1])).expect("it only grew");
    assert_eq!(latency.series[0].values[1], Some(Reading::Histogram((&gained).into())), "the 300 and 400 µs");
    let p50 = gained.percentile(0.5);
    assert!(p50.is_some_and(|p| p <= Duration::from_micros(340)), "p50 of 300 and 400 µs came back as {p50:?}");
    assert_eq!(latency.series[0].values.iter().flatten().count(), 1);

    // Whole minutes and hours may be answered from a rollup: the numbers must not change.
    let hour = now_ms() / 3_600_000 * 3_600_000 - 3_600_000;
    for step_ms in [60_000, 3_600_000] {
        let earlier = format!("{node}h{step_ms}");
        let mut written = batches(&earlier, hour);
        written[3].timestamp_ms = hour + step_ms;
        provider.write(&written).await.unwrap();
        provider.write(&written).await.unwrap();
        let coarse = |metric: &str, aggregation| HistoryQuery {
            node: Some(earlier.clone()),
            aggregation,
            ..HistoryQuery::new(metric, hour, hour + step_ms + 9999, step_ms)
        };
        for (aggregation, expected) in
            [(Aggregation::Last, 3.0), (Aggregation::Mean, 2.0), (Aggregation::Min, 1.0), (Aggregation::Max, 3.0)]
        {
            let result = provider.query(&coarse("t.gauge", aggregation)).await.unwrap();
            assert_eq!(values(&result), [[expected, 4.0]], "{aggregation:?} at {step_ms} ms");
            assert_eq!(result.from, hour);
        }
        let latency = provider.query(&coarse("t.latency", Aggregation::Mean)).await.unwrap();
        let gained = histogram(&written[3]).since(&histogram(&written[2])).expect("it only grew");
        assert_eq!(latency.series[0].values, [None, Some(Reading::Histogram((&gained).into()))]);
        let peers = provider.query(&coarse("t.counter", Aggregation::Last)).await.unwrap();
        assert_eq!(values(&peers), [[10.0], [100.0]]);
    }

    // A series is stored when it changes, so a step without a row holds the value before it: carried in from the
    // minute before the range, up to how far its node was stored, and for less than HELD_MS after its last row.
    let at = now_ms() / 60_000 * 60_000 - 300_000;
    let quiet = |node: &str, sequence: u64, after: u64, gauge: Option<f64>| {
        let beat = [("t.beat", Value::Counter(sequence))].into_iter();
        let samples = beat.chain(gauge.map(|v| ("t.gauge", Value::Gauge(v))));
        let samples =
            samples.map(|(name, value)| Sample { name: name.into(), unit: "things".into(), labels: [].into(), value });
        SampleBatch {
            node_id: node.into(),
            incarnation: "00ff".into(),
            sequence,
            timestamp_ms: at + after,
            samples: samples.collect(),
        }
    };
    let (held, ended) = (format!("{node}q"), format!("{node}e"));
    let written = [quiet(&held, 1, 0, Some(5.0)), quiet(&held, 2, 5500, Some(7.0)), quiet(&held, 3, 9000, None)];
    provider.write(&written).await.unwrap();
    provider.write(&[quiet(&ended, 1, 0, Some(5.0)), quiet(&ended, 2, 150_000, None)]).await.unwrap();
    let range = |node: &str, from: u64, to: u64, step| HistoryQuery {
        node: Some(node.into()),
        ..HistoryQuery::new("t.gauge", at + from, at + to, step)
    };
    let times = |result: &HistoryResult| {
        let known = result.series[0].values.iter().enumerate().filter(|(_, v)| v.is_some());
        known.map(|(i, _)| result.from + i as u64 * result.step - at).collect::<Vec<_>>()
    };
    let gauge = provider.query(&range(&held, 1000, 30_000, 1000)).await.unwrap();
    assert_eq!(values(&gauge), [[5.0, 5.0, 5.0, 5.0, 7.0, 7.0, 7.0, 7.0, 7.0]]);
    assert_eq!(times(&gauge), (1..=9).map(|s| s * 1000).collect::<Vec<_>>());
    let gone = provider.query(&range(&ended, 0, 150_000, 1000)).await.unwrap();
    assert_eq!(times(&gone), (0..60).map(|s| s * 1000).collect::<Vec<_>>());
    let gone = provider.query(&range(&ended, 0, 150_000, 60_000)).await.unwrap();
    assert_eq!(values(&gone), [[5.0]], "a minute step without a row is a series that ended");

    let hostile = provider.query(&query("t.gauge' OR 1=1 --")).await.unwrap();
    assert!(hostile.series.is_empty());
    let cut = provider.query(&HistoryQuery { max_points: 1, ..query("t.gauge") }).await.unwrap();
    assert_eq!((cut.series.len(), cut.truncated), (0, true), "a series comes whole or not at all");
    let cut = provider.query(&HistoryQuery { max_series: 1, ..query("t.counter") }).await.unwrap();
    assert_eq!((cut.series.len(), cut.truncated), (1, true));
    assert!(provider.query(&HistoryQuery { step_ms: 0, ..query("t.gauge") }).await.is_err());

    // The series stored, without values: what a filter can offer.
    let listed = provider.series(&query("t.counter")).await.unwrap();
    let peers: Vec<_> = listed.series.iter().map(|s| (s.labels["peer"].as_str(), s.values.len())).collect();
    assert_eq!((peers, listed.truncated), (vec![("x", 0), ("y\t'\\", 0)], false));
    assert_eq!(provider.series(&odd).await.unwrap().series.len(), 1);
    let one = provider.series(&HistoryQuery { max_series: 1, ..query("t.counter") }).await.unwrap();
    assert_eq!((one.series.len(), one.truncated), (1, true));

    // A page of charts answers as each chart alone: a shared range, other ranges and steps, a metric twice, a refusal.
    let page = [
        query("t.gauge"),
        query("t.counter"),
        HistoryQuery { aggregation: Aggregation::Max, ..query("t.gauge") },
        odd.clone(),
        query("t.latency"),
        query("t.gauge"),
        range(&held, 1000, 30_000, 1000),
        HistoryQuery { max_series: 1, ..query("t.counter") },
        HistoryQuery { sum: Some(Sum::Node), by: vec!["peer".into()], ..query("t.counter") },
        HistoryQuery { step_ms: 0, ..query("t.gauge") },
        query("t.none"),
    ];
    let answers = provider.queries(&page).await.unwrap();
    assert_eq!(answers.len(), page.len());
    for (asked, answer) in page.iter().zip(&answers) {
        match provider.query(asked).await {
            Ok(alone) => assert_eq!(answer.as_ref().ok(), Some(&alone), "{asked:?}"),
            Err(_) => assert!(answer.is_err(), "{asked:?}"),
        }
    }

    let event = |n: u64, kind: &str, on: &str| ClusterEvent {
        id: format!("{node}:{n}"),
        time_ms: base + n,
        node_id: format!("{node}{on}"),
        kind: kind.into(),
        subject: format!("orders/it's-{n}"),
        detail: "it's \"quoted\"\n".into(),
    };
    let events = [event(1, "joined", ""), event(2, "left", ""), event(3, "acquired", "-far")];
    provider.write_events(&events).await.unwrap();
    provider.write_events(&events).await.unwrap();
    let window = EventQuery { from_ms: base, to_ms: base + 10, ..Default::default() };
    let mut read = provider.events(&window).await.unwrap();
    read.retain(|e| e.node_id.starts_with(&node));
    assert_eq!(read, [events[2].clone(), events[1].clone(), events[0].clone()], "each stored once, newest first");
    assert_eq!(provider.events(&EventQuery { limit: 1, ..window.clone() }).await.unwrap().len(), 1);
    let on = |node: &str, kind: &str| EventQuery { node: node.into(), kind: kind.into(), ..window.clone() };
    assert_eq!(provider.events(&on(&node, "")).await.unwrap(), [events[1].clone(), events[0].clone()]);
    assert_eq!(provider.events(&on(&node, "joined")).await.unwrap(), [events[0].clone()]);
    assert_eq!(provider.events(&on(&format!("{node}-far"), "acquired")).await.unwrap(), [events[2].clone()]);
    assert_eq!(provider.events(&on(&node, "' OR 1=1 --")).await.unwrap(), []);
    let everywhere = EventQuery { kind: "acquired".into(), ..window.clone() };
    // A shared store holds other runs' events in the window too.
    let mut acquired = provider.events(&everywhere).await.unwrap();
    assert!(acquired.iter().all(|e| e.kind == "acquired"), "{acquired:?}");
    acquired.retain(|e| e.node_id.starts_with(&node));
    assert_eq!(acquired, [events[2].clone()]);

    let trace = base << 8 | 1;
    let span = |id: u64, parent: u64, at: u64, failed: bool| SpanRecord {
        trace: hex(trace),
        span: hex(id),
        parent: if parent == 0 { String::new() } else { hex(parent) },
        kind: "handle".into(),
        name: format!("it's \"{id}\""),
        node_id: if id == 3 { format!("{node}-far") } else { node.clone() },
        actor: "0:1:2".into(),
        actor_name: "worker-1".into(),
        actor_type: "app::Worker".into(),
        peer: String::new(),
        start_us: base * 1000 + at,
        wait_ns: 5,
        busy_ns: 2_000_000,
        failed,
    };
    let spans = [span(1, 0, 0, false), span(2, 1, 500, false), span(3, 2, 900, true)];
    provider.write_spans(&spans).await.unwrap();
    provider.write_spans(&spans[1..]).await.unwrap();
    assert_eq!(provider.trace(trace).await.unwrap(), spans);
    assert_eq!(provider.trace(trace + 1).await.unwrap(), []);
    let list = |query: TraceQuery| async move {
        let found = provider.traces(&query).await.unwrap();
        found.into_iter().filter(|t| t.trace == hex(trace)).collect::<Vec<_>>()
    };
    let query = TraceQuery { from_ms: base, to_ms: base + 1, limit: 1000, ..Default::default() };
    let found = list(query.clone()).await;
    assert_eq!(found.len(), 1);
    let t = &found[0];
    assert_eq!((t.start_us, t.end_us, t.spans, t.failed), (base * 1000, base * 1000 + 2900, 3, true));
    assert_eq!((&*t.root, &*t.root_actor, &t.root_node), ("it's \"1\"", "app::Worker", &node));
    assert_eq!((&*t.root_kind, &*t.root_actor_name, &*t.root_peer), ("handle", "worker-1", ""));
    assert_eq!(t.nodes, [node.clone(), format!("{node}-far")]);
    assert_eq!(
        list(TraceQuery { search: Some("WORKER".into()), failed: true, min_us: 2900, ..query.clone() }).await,
        found
    );
    assert_eq!(list(TraceQuery { search: Some("' OR 1=1 --".into()), ..query.clone() }).await, []);
    assert_eq!(list(TraceQuery { min_us: 2901, ..query.clone() }).await, []);
    assert_eq!(list(TraceQuery { from_ms: base + 2, to_ms: base + 3, ..query.clone() }).await, []);
    assert!(provider.traces(&TraceQuery { limit: 0, ..query }).await.is_err());
    assert!(provider.events(&EventQuery { limit: 0, ..Default::default() }).await.is_err());

    let letter = |seq: u64| DeadLetterRecord {
        node_id: node.clone(),
        seq,
        at_us: base * 1000 + seq,
        reason: "full".into(),
        label: "it's \"quoted\"".into(),
        actor: "0:1:2".into(),
        actor_type: "app::Worker".into(),
        target: "orders".into(),
        peer: "far".into(),
        trace: hex(trace),
        count: 3,
    };
    let letters = [letter(1), letter(2)];
    provider.write_dead_letters(&letters).await.unwrap();
    provider.write_dead_letters(&letters[1..]).await.unwrap();
    let mut read = provider.dead_letters(1000).await.unwrap();
    read.retain(|l| l.node_id == node);
    assert_eq!(read, [letters[1].clone(), letters[0].clone()]);

    let mut invalid = batches(&node, base);
    invalid[0].sequence = 0;
    assert_eq!(provider.write(&invalid).await.unwrap_err(), "sequence must not be 0");
}

#[tokio::test]
async fn memory_provider_contract() {
    contract(&MemoryMetrics::new(64 << 10)).await;
}

#[tokio::test]
async fn memory_metrics_holds_at_most_its_bytes_and_lets_the_oldest_go_first() {
    let base = now_ms() / 1000 * 1000 - 200_000;
    let mut histogram = Histogram::default();
    let batch = |sequence: u64, histogram: &Histogram| {
        let sample = |name: &'static str, peer: &str, value| Sample {
            name: name.into(),
            unit: "things".into(),
            labels: [("peer".into(), peer.to_string())].into(),
            value,
        };
        let mut samples: Vec<_> = (0..16).map(|i| sample("t.count", &format!("p{i}"), Value::Counter(i))).collect();
        samples.push(sample("t.gauge", "x", Value::Gauge(sequence as f64)));
        samples.push(sample("t.latency", "x", Value::Histogram(histogram.into())));
        SampleBatch {
            node_id: "n".into(),
            incarnation: "00ff".into(),
            sequence,
            timestamp_ms: base + sequence * 1000,
            samples,
        }
    };
    let store = MemoryMetrics::new(64 << 10);
    for sequence in 1..=150 {
        histogram.record(Duration::from_micros(sequence * 37));
        let written = batch(sequence, &histogram);
        store.write(std::slice::from_ref(&written)).await.unwrap();
        let usage = store.usage().expect("a bounded store says what it holds");
        assert!(usage.held_bytes <= usage.max_bytes, "{usage:?}");
        let floor = written.samples.len() * size_of::<Sample>();
        assert!(usage.held_batches as usize * floor <= usage.max_bytes as usize, "every sample counts: {usage:?}");
    }
    let usage = store.usage().unwrap();
    assert_eq!(usage.max_bytes, 64 << 10);
    assert_eq!(usage.held_batches + usage.evicted_batches, 150, "{usage:?}");
    assert!(usage.evicted_batches > 0 && usage.held_batches > 0, "{usage:?}");

    let query = |metric: &str| HistoryQuery {
        node: Some("n".into()),
        labels: [("peer".into(), "x".into())].into(),
        ..HistoryQuery::new(metric, base, base + 200_000, 1000)
    };
    let gauge = store.query(&query("t.gauge")).await.unwrap();
    let number = |r: &Reading| if let Reading::Number(n) = r { *n } else { f64::NAN };
    let held: Vec<f64> = gauge.series[0].values.iter().flatten().map(number).collect();
    let newest: Vec<f64> = (150 - usage.held_batches + 1..=150).map(|s| s as f64).collect();
    assert_eq!(held, newest, "the newest are kept and the oldest let go");
    let latency = store.query(&query("t.latency")).await.unwrap();
    let last = latency.series[0].values.iter().flatten().last();
    let mut gained = Histogram::default();
    gained.record(Duration::from_micros(150 * 37));
    assert_eq!(last, Some(&Reading::Histogram((&gained).into())), "a histogram reads back as what it gained");

    store.write(&[batch(1, &histogram)]).await.unwrap();
    let again = store.usage().unwrap();
    assert_eq!(again.held_batches + again.evicted_batches, 151, "an evicted batch's id goes with it: {again:?}");
}

#[tokio::test]
async fn memory_metrics_says_what_it_holds_and_the_room_left() {
    let store = MemoryMetrics::new(1 << 20);
    let usage = store.usage().unwrap();
    let empty = TableUsage { name: "samples".into(), ..Default::default() };
    assert_eq!((usage.tables, usage.free_bytes), (vec![empty], Some(1 << 20)));

    let before = now_ms();
    store.write(&batches("n", before + 2 * 86_400_000)).await.unwrap();
    let usage = store.usage().unwrap();
    let table = &usage.tables[0];
    assert_eq!((table.rows, table.retention_days), (16, None));
    let stored = table.first_ms.zip(table.last_ms).unwrap();
    assert!(stored.0 >= before && stored.1 <= now_ms(), "when it was stored, not a skewed node's clock: {usage:?}");
    let held = usage.held_bytes;
    assert_eq!((table.held_bytes, table.uncompressed_bytes), (held, held), "memory does not compress");
    assert_eq!(usage.free_bytes, Some((1 << 20) - held));
    assert_eq!((usage.growth_bytes_per_day, usage.full_in_ms), (None, None), "the collector works them out");
}

#[tokio::test]
async fn memory_metrics_counts_what_each_sample_keeps() {
    let store = MemoryMetrics::new(1 << 20);
    let mut sequence = 0;
    let mut cost = async |node: &str, labels: usize, histogram: &Histogram| {
        let before = store.usage().unwrap().held_bytes;
        sequence += 1;
        let sample = Sample {
            name: "t.latency".into(),
            unit: "ns".into(),
            labels: (0..labels).map(|i| (format!("k{i}").into(), "v".repeat(40))).collect(),
            value: Value::Histogram(histogram.into()),
        };
        let samples = vec![sample];
        let batch =
            SampleBatch { node_id: node.into(), incarnation: "00ff".into(), sequence, timestamp_ms: 1, samples };
        store.write(&[batch]).await.unwrap();
        (store.usage().unwrap().held_bytes - before) as usize
    };
    let (mut narrow, mut wide) = (Histogram::default(), Histogram::default());
    narrow.record(Duration::from_micros(1));
    wide.record(Duration::from_micros(1));
    wide.record(Duration::from_millis(1));
    let run = wide.buckets.iter().rposition(|&n| n > 0).unwrap() - wide.buckets.iter().position(|&n| n > 0).unwrap();
    let one = cost("n", 1, &narrow).await;
    let wider = cost("n", 1, &wide).await;
    assert!(wider >= one + 8 * (run - 1), "a histogram counts its run of buckets, and only that: {one} {wider}");
    assert!(cost("n", 2, &narrow).await >= one + size_of::<(String, String)>() + 40, "and each label");
    assert!(cost(&"n".repeat(100), 1, &narrow).await >= one + 100, "and the batch's ids");
}

/// Runs against `AXON_CLICKHOUSE=http://host:port` (with `AXON_CLICKHOUSE_USER` and
/// `AXON_CLICKHOUSE_PASSWORD`): `scripts/check.sh live`.
#[cfg(feature = "clickhouse")]
#[tokio::test]
#[ignore = "needs AXON_CLICKHOUSE: scripts/check.sh live"]
async fn clickhouse_provider_contract() {
    let url =
        std::env::var("AXON_CLICKHOUSE").expect("AXON_CLICKHOUSE is set for the live tests: scripts/check.sh live");
    let cfg = ClickHouseConfig {
        url,
        database: "axon_test".into(),
        username: std::env::var("AXON_CLICKHOUSE_USER").unwrap_or("default".into()),
        password: std::env::var("AXON_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        retention_days: 1,
        rollup_retention_days: 2,
    };
    for (field, bad) in [
        ("url", ClickHouseConfig { url: "http://user:pw@127.0.0.1:8123".into(), ..cfg.clone() }),
        ("url", ClickHouseConfig { url: "ftp://127.0.0.1".into(), ..cfg.clone() }),
        ("database", ClickHouseConfig { database: "x`; DROP".into(), ..cfg.clone() }),
        ("retention_days", ClickHouseConfig { retention_days: 0, ..cfg.clone() }),
        ("rollup_retention_days", ClickHouseConfig { rollup_retention_days: 0, ..cfg.clone() }),
    ] {
        assert!(ClickHouseProvider::connect(bad).await.err().unwrap().starts_with(field));
    }
    let wrong = ClickHouseConfig { password: "not the password".into(), ..cfg.clone() };
    assert!(ClickHouseProvider::connect(wrong).await.err().unwrap().starts_with("ClickHouse 4"));

    let provider = ClickHouseProvider::connect(cfg.clone()).await.unwrap();
    contract(&provider).await;
    contract(&ClickHouseProvider::connect(cfg.clone()).await.unwrap()).await;

    let placed = format!("p{}", now_ms());
    // In the past, since a store reports no point later than its clock.
    let mut written = batches(&placed, now_ms() - 10_000);
    written[3].sequence = OWN_SEQUENCE + 9;
    provider.write(&written).await.unwrap();
    assert_eq!(provider.stored_through(&placed, "00ff").await.unwrap(), Some(3), "the collector's own do not count");
    assert_eq!(provider.stored_through(&placed, "00fe").await.unwrap(), None);

    let fresh = ClickHouseProvider::connect(cfg.clone()).await.unwrap();
    let usage = fresh.usage().expect("read once at connect, from system.parts, system.tables and system.disks");
    let table = |name: &str| usage.tables.iter().find(|t| t.name == name).cloned().unwrap_or_else(|| panic!("{name}"));
    let (samples, hourly) = (table("samples"), table("samples_1h"));
    assert!(samples.rows >= 16 && samples.parts > 0 && samples.uncompressed_bytes > samples.held_bytes, "{samples:?}");
    let written_at = written[2].timestamp_ms;
    assert!(samples.first_ms <= Some(written_at) && samples.last_ms >= Some(written_at - 1000), "{samples:?}");
    assert_eq!((samples.retention_days, hourly.retention_days), (Some(1), Some(2)), "each table's TTL");
    assert!(!usage.tables.iter().any(|t| t.name.ends_with("_feed")), "a view holds nothing: {usage:?}");
    assert_eq!(usage.held_bytes, usage.tables.iter().map(|t| t.held_bytes).sum::<u64>());
    assert!(usage.free_bytes.is_some_and(|free| free > 0), "{usage:?}");
    let mut more = batches(&format!("{placed}b"), now_ms());
    more.iter_mut().for_each(|b| b.sequence += 10);
    fresh.write(&more).await.unwrap();
    assert_eq!(fresh.usage(), Some(usage.clone()), "read at most once a minute, never per ask");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(fresh.usage(), Some(usage.clone()), "no ask started a read");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    eventually("a minute on, an ask reads it again", || async { fresh.usage() != Some(usage.clone()) }).await;
    let rows = |u: Option<SinkUsage>| u.unwrap().tables.into_iter().find(|t| t.name == "samples").unwrap().rows;
    assert!(rows(fresh.usage()) > samples.rows, "and sees the rows written since");

    let id = format!("live{}", now_ms());
    let (system, cluster) = node(&id, &MemoryDiscovery::new()).await;
    let telemetry =
        // Sampled every 50 ms, so rows come within the test.
        TelemetryConfig { sample_interval: Duration::from_millis(50), access: only(&[&id]), ..Default::default() };
    Telemetry::start(&system, &cluster, telemetry).await.unwrap();
    // Collected every 100 ms, so rows come within the test.
    let collect = MetricsConfig { collection_interval: Some(Duration::from_millis(100)), ..Default::default() };
    let provider = Arc::new(ClickHouseProvider::connect(cfg).await.unwrap());
    let service = MetricsService::start(&system, &cluster, provider.clone(), collect).await.unwrap();
    eventually("real telemetry lands in ClickHouse", || async { service.health().stored_batches >= 4 }).await;
    let health = service.health();
    assert_eq!((health.write_errors, health.node_errors.len()), (0, 0), "{health:?}");
    let query =
        HistoryQuery { node: Some(id), ..HistoryQuery::new("runtime.worker.turns", now_ms() - 60_000, now_ms(), 1000) };
    // Listed, not stepped: a counter's gain needs two steps, and the batches may span one.
    let turns = provider.series(&query).await.unwrap();
    assert!(
        !turns.series.is_empty() && turns.series.iter().all(|s| s.kind == "counter" && s.labels.contains_key("worker"))
    );
    system.shutdown().await.unwrap();
}

#[cfg(feature = "clickhouse")]
#[tokio::test]
#[ignore = "needs AXON_CLICKHOUSE: scripts/check.sh live"]
async fn a_clickhouse_events_table_from_before_subjects_gains_the_column_and_its_rows_still_read() {
    let cfg = ClickHouseConfig {
        url: std::env::var("AXON_CLICKHOUSE")
            .expect("AXON_CLICKHOUSE is set for the live tests: scripts/check.sh live"),
        database: format!("axon_upgrade_{}", now_ms()),
        username: std::env::var("AXON_CLICKHOUSE_USER").unwrap_or("default".into()),
        password: std::env::var("AXON_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        retention_days: 1,
        rollup_retention_days: 2,
    };
    let (db, http) = (&cfg.database, reqwest::Client::new());
    let run = |sql: String| {
        let request = http.post(&cfg.url).basic_auth(&cfg.username, Some(&cfg.password)).body(sql);
        async move { request.send().await.unwrap().error_for_status().unwrap() }
    };
    let (before, now) = (now_ms() - 10_000, now_ms());
    // The table as it was made before events had a subject.
    run(format!("CREATE DATABASE `{db}`")).await;
    run(format!(
        "CREATE TABLE `{db}`.events (id String, time_ms UInt64, node_id String, kind LowCardinality(String), \
         detail String) ENGINE = ReplacingMergeTree PARTITION BY intDiv(time_ms, 86400000) ORDER BY id \
         TTL toDateTime(intDiv(time_ms, 1000)) + INTERVAL 1 DAY"
    ))
    .await;
    run(format!("INSERT INTO `{db}`.events VALUES ('old:1:e1', {before}, 'old', 'left', 'gone')")).await;

    let provider = ClickHouseProvider::connect(cfg.clone()).await.unwrap();
    let event = |id: &str, time_ms, node: &str, kind: &str, subject: &str, detail: &str| ClusterEvent {
        id: id.into(),
        time_ms,
        node_id: node.into(),
        kind: kind.into(),
        subject: subject.into(),
        detail: detail.into(),
    };
    let new = event("new:1:e1", now, "new", "acquired", "orders/cust-1", "epoch 2");
    provider.write_events(std::slice::from_ref(&new)).await.unwrap();
    let old = event("old:1:e1", before, "old", "left", "", "gone");
    assert_eq!(provider.events(&EventQuery::default()).await.unwrap(), [new.clone(), old]);
    let acquired = EventQuery { kind: "acquired".into(), ..Default::default() };
    assert_eq!(provider.events(&acquired).await.unwrap(), [new], "filtered by kind on the upgraded table");
    // A second connect finds the column there.
    ClickHouseProvider::connect(cfg.clone()).await.unwrap();
    run(format!("DROP DATABASE `{db}`")).await;
}
