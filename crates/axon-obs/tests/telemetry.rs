#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]
use std::time::Duration;

use axon::{Actor, ActorSystem, Context, Lane, LeaveReason, PoolConfig, ReplyTo, RuntimeConfig};
use axon_cluster::{AccessPolicy, Cluster, ClusterConfig, LEAVE_REASON, MemoryDiscovery, Reply};
use axon_net::VERSIONS;
use axon_obs::*;
use axon_testkit::eventually;
use bytes::Bytes;
use serde_json::Value as Json;

mod common;

enum Msg {
    Work,
    Count(ReplyTo<u64>),
}

#[derive(Default)]
struct Worker(u64);

impl Actor for Worker {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Work => self.0 += 1,
            Msg::Count(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Work => "work",
            Msg::Count(_) => "count",
        }
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    node_on(id, discovery, RuntimeConfig::default()).await
}

/// a and b, on a discovery of their own.
async fn pair() -> ((ActorSystem, Cluster), (ActorSystem, Cluster)) {
    let discovery = MemoryDiscovery::new();
    (node("a", &discovery).await, node("b", &discovery).await)
}

async fn node_on(id: &str, discovery: &MemoryDiscovery, runtime: RuntimeConfig) -> (ActorSystem, Cluster) {
    let system = ActorSystem::builder().runtime(runtime).timing_sample_every(1);
    common::loopback(id, discovery.clone(), system, |cfg| cfg.transport.timing_sample_every = 1).await
}

fn sample<'a>(batch: &'a SampleBatch, name: &str, labels: &[(&str, &str)]) -> Option<&'a Value> {
    let has = |s: &Sample| labels.iter().all(|(k, v)| s.labels.get(*k).is_some_and(|found| found == v));
    batch.samples.iter().find(|s| s.name == name && has(s)).map(|s| &s.value)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_is_inspectable_from_an_allowed_peer() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (b_system, b)) = (node("a", &discovery).await, node("b", &discovery).await);
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;

    let bad = TelemetryConfig { retained_samples: 0, ..Default::default() };
    assert_eq!(Telemetry::start(&a_system, &a, bad).await.err().unwrap(), "retained_samples must be 1..=3600");
    let cfg = TelemetryConfig {
        // Sampled every 50 ms, so samples come within the test.
        sample_interval: Duration::from_millis(50),
        retained_samples: 4,
        // Every sample written, changed or not.
        rewrite_unchanged: Duration::ZERO,
        persist_actors: vec!["pinned".into()],
        access: AccessPolicy::Nodes(["b".to_string()].into()),
        ..Default::default()
    };
    drop(Telemetry::start(&a_system, &a, cfg).await.unwrap());
    Telemetry::start(&b_system, &b, TelemetryConfig::default()).await.unwrap();

    let pinned = a_system.spawn(Worker::default()).name("pinned").start().unwrap();
    let passing = a_system.spawn(Worker::default()).start().unwrap();
    for _ in 0..10 {
        pinned.send(Msg::Work).await.unwrap();
        passing.send(Msg::Work).await.unwrap();
    }
    assert_eq!(pinned.ask(Msg::Count).await.unwrap(), 10);
    assert_eq!(passing.ask(Msg::Count).await.unwrap(), 10);
    passing.lifecycle.stop();
    passing.join().await;

    let worker = std::any::type_name::<Worker>();
    eventually("the snapshot shows the exited actor's work", || async {
        let snapshot: Json = fetch(&b, "a", &Request::Snapshot).await.unwrap();
        let of_type = |t: &&Json| t["actor_type"] == worker;
        let workers = snapshot["actor_types"].as_array().unwrap().iter().find(of_type).cloned().unwrap_or_default();
        snapshot["node_id"] == "a"
            && snapshot["system"]["stopped"] == 1
            && workers["live"] == 1
            && workers["totals"]["processed"] == 22
            && workers["totals"]["asks"] == 2
            && snapshot["members"].as_array().unwrap().len() == 2
            && snapshot["cluster"]["engine"]["peers"].as_array().is_some_and(|p| !p.is_empty())
            && snapshot["cluster"]["engine"]["versions"] == Json::from(vec![*VERSIONS.start(), *VERSIONS.end()])
            && (snapshot["cluster"]["warming"] == false && snapshot["cluster"]["quotas_starting"] == false)
            && snapshot["actors"].is_null()
            && (snapshot["cluster"]["engine"]["peers"].as_array().unwrap().iter())
                .all(|p| p["request_rtt"].is_null() && p["session"]["timings"].is_null() && p["session"].is_object())
    })
    .await;

    let query = ActorQuery { limit: 10, search: Some("PIN".into()), timings: true, ..Default::default() };
    let page: Json = fetch(&b, "a", &Request::Actors(query)).await.unwrap();
    assert_eq!((page["actors"].as_array().unwrap().len(), &page["actors"][0]["name"]), (1, &Json::from("pinned")));
    let labels: Vec<_> = (page["actors"][0]["diagnostics"]["timings"]["messages"].as_array().unwrap().iter())
        .map(|m| (m["label"].as_str().unwrap(), m["handler"]["count"].as_u64().unwrap()))
        .collect();
    assert_eq!(labels, [("work", 10), ("count", 1)]);
    let up = page["actors"][0]["started_us"].as_u64().unwrap();
    assert!(up > 0 && up <= axon::trace::unix_micros() && page["actors"][0]["restarted_us"].is_null(), "{page}");

    let first = fetch_samples(&b, "a", None).await.unwrap();
    assert!(!first.gap && !first.reset && !first.batches.is_empty());
    let sequences: Vec<_> = first.batches.iter().map(|b| b.sequence).collect();
    assert!(sequences.windows(2).all(|w| w[1] == w[0] + 1) && first.next.sequence == *sequences.last().unwrap());

    eventually("per-type, per-actor and transport series are emitted", || async {
        let page = fetch_samples(&b, "a", None).await.unwrap();
        let batch = page.batches.last().unwrap();
        let handled = sample(batch, "actor.message.handler", &[("message", "work"), ("actor_type", worker)]);
        matches!(handled, Some(Value::Histogram(h)) if h.count() == 20)
            && sample(batch, "actor.processed", &[("actor", "pinned")]) == Some(&Value::Counter(11))
            && matches!(sample(batch, "connection.decode", &[("peer", "b")]), Some(Value::Histogram(_)))
            && matches!(sample(batch, "cluster.calls", &[("outcome", "succeeded")]), Some(Value::Counter(_)))
    })
    .await;

    eventually("an old cursor reports the gap", || async {
        let after = Some(Cursor { sequence: 1, ..first.next.clone() });
        let page = fetch_samples(&b, "a", after).await.unwrap();
        page.gap && page.batches.len() == 4
    })
    .await;
    let caught_up = fetch_samples(&b, "a", Some(first.next.clone())).await.unwrap();
    let again = Cursor { sequence: caught_up.next.sequence + 100, ..first.next.clone() };
    let idle = fetch_samples(&b, "a", Some(again.clone())).await.unwrap();
    assert!(idle.batches.is_empty() && !idle.gap && idle.next == again);
    let foreign = Some(Cursor { incarnation: "0".repeat(32), sequence: 1 << 40 });
    let page = fetch_samples(&b, "a", foreign).await.unwrap();
    assert!(page.reset && !page.batches.is_empty());

    let refused = fetch::<Json>(&a, "b", &Request::Snapshot).await.unwrap_err();
    assert_eq!(
        refused,
        "b refuses to answer: its TelemetryConfig::access (default Deny) or ClusterConfig::authorizer does not admit this node"
    );
    assert_eq!(fetch_samples(&a, "b", None).await.unwrap_err(), refused);

    a_system.shutdown().await.unwrap();
    b_system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_that_missed_batches_gets_every_series_again() {
    let ((system, a), (_b_system, b)) = pair().await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    let cfg = TelemetryConfig {
        // Sampled every 20 ms, so samples come within the test.
        sample_interval: Duration::from_millis(20),
        retained_samples: 3,
        access: AccessPolicy::Nodes(["b".to_string()].into()),
        ..Default::default()
    };
    let _telemetry = Telemetry::start(&system, &a, cfg).await.unwrap();
    let first = fetch_samples(&b, "a", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let missed = fetch_samples(&b, "a", Some(first.next)).await.unwrap();
    assert!(missed.gap);
    // Nothing fails here, so without the gap this series would not be stored again for 30 s.
    let mut cursor = missed.next;
    let again = async {
        loop {
            let page = fetch_samples(&b, "a", Some(cursor.clone())).await.unwrap();
            if page.batches.iter().any(|b| sample(b, "runtime.actors_failed", &[]).is_some()) {
                break;
            }
            cursor = page.next;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), again).await.expect("every series is stored again after a gap");
    system.shutdown().await.unwrap();
}

/// Busy for a second on each message, so a shutdown drains for that long.
struct Busy;

impl Actor for Busy {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(Duration::from_secs(1));
        Ok(())
    }
}

/// The placement page's data: the kinds a node hosts with their keys counted, and no keys by default; with
/// `snapshot_keys` the first of them by kind and key, and a notice that the list is cut. No epoch without an ownership
/// authority.
#[tokio::test(flavor = "multi_thread")]
async fn the_snapshot_counts_the_keys_per_kind_and_lists_them_only_when_asked() {
    let (system, cluster) = node("a", &MemoryDiscovery::new()).await;
    common::keyed(&cluster, "orders", (0..1001).map(|i| format!("o-{i:04}"))).await;
    let telemetry = Telemetry::start(&system, &cluster, TelemetryConfig::default()).await.unwrap();
    let snapshot: Json = serde_json::from_slice(&telemetry.snapshot()).unwrap();
    let hosted = &snapshot["hosted"];
    let orders = serde_json::json!([{
        "kind": "orders", "eager": false, "count": 1001, "started": [1001, 0], "ended": [0, 0, 0, 0, 0]
    }]);
    assert_eq!(
        (&hosted["kinds"], &hosted["keys"], &hosted["total"]),
        (&orders, &serde_json::json!([]), &Json::from(1001))
    );
    assert!(!snapshot["notices"].as_array().unwrap().iter().any(|n| n["code"] == "keys_truncated"), "{snapshot}");
    system.shutdown().await.unwrap();

    let (system, cluster) = node("a", &MemoryDiscovery::new()).await;
    common::keyed(&cluster, "orders", (0..1001).map(|i| format!("o-{i:04}"))).await;
    let cfg = TelemetryConfig { snapshot_keys: 1000, ..Default::default() };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    let snapshot: Json = serde_json::from_slice(&telemetry.snapshot()).unwrap();
    let hosted = &snapshot["hosted"];
    assert_eq!(hosted["keys"][0], serde_json::json!({ "kind": "orders", "key": "o-0000", "epoch": null }));
    assert_eq!(hosted["keys"][999]["key"], "o-0999");
    assert_eq!((hosted["keys"].as_array().unwrap().len(), &hosted["total"]), (1000, &Json::from(1001)));
    assert!(snapshot["notices"].as_array().unwrap().iter().any(|n| n["code"] == "keys_truncated"), "{snapshot}");
    system.shutdown().await.unwrap();
}

/// A node pages one kind's keys by prefix, from a key on, without listing them all: what the console's key list and
/// key search ask.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_pages_a_kinds_keys_by_prefix_from_a_key_on() {
    let ((system, a), (_b_system, b)) = pair().await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    let eu = (0..300).map(|i| format!("acct-eu-{i:04}"));
    common::keyed(&a, "account", eu.chain((0..200).map(|i| format!("acct-us-{i:04}")))).await;
    common::keyed(&a, "session", (0..50).map(|i| format!("acct-eu-{i:04}"))).await;
    let access = AccessPolicy::Nodes(["b".to_string()].into());
    let _telemetry = Telemetry::start(&system, &a, TelemetryConfig { access, ..Default::default() }).await.unwrap();
    let page = async |prefix: &str, from: Option<&str>, limit| {
        let request =
            Request::Keys { kind: "account".into(), prefix: prefix.into(), from: from.map(Into::into), limit };
        let page: KeyPage = fetch(&b, "a", &request).await.unwrap();
        (page.keys.iter().map(|k| k.key.clone()).collect::<Vec<_>>(), page.more)
    };
    let (keys, more) = page("acct-eu-", None, 100).await;
    assert_eq!((keys.len(), &keys[0][..], &keys[99][..], more), (100, "acct-eu-0000", "acct-eu-0099", true));
    let (keys, more) = page("acct-eu-", Some("acct-eu-0250"), 100).await;
    assert_eq!((keys.len(), &keys[0][..], &keys[49][..], more), (50, "acct-eu-0250", "acct-eu-0299", false));
    let (keys, more) = page("acct-us-01", None, 100).await;
    assert_eq!((keys.len(), &keys[0][..], more), (100, "acct-us-0100", false), "the prefix is the search");
    assert_eq!(page("", Some("acct-us-0199"), 10).await, (vec!["acct-us-0199".to_string()], false));
    assert_eq!(page("zzz", None, 10).await, (vec![], false));
    let other: KeyPage =
        fetch(&b, "a", &Request::Keys { kind: "session".into(), prefix: "".into(), from: None, limit: 2000 })
            .await
            .unwrap();
    assert_eq!((other.keys.len(), other.more), (50, false), "another kind's keys are its own");
    assert_eq!(other.keys[0], ListedKey { key: "acct-eu-0000".into(), epoch: None });

    let sampled = fetch_samples(&b, "a", None).await.unwrap();
    let samples = &sampled.batches[0].samples;
    let value = |name: &str, labels: &[(&str, &str)]| {
        let labelled = |s: &&Sample| labels.iter().all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v));
        samples.iter().filter(|s| s.name == name).find(labelled).map(|s| s.value.clone())
    };
    assert_eq!(value("cluster.keyed", &[("kind", "account")]), Some(Value::Gauge(500.0)));
    assert_eq!(value("cluster.keyed", &[("kind", "session")]), Some(Value::Gauge(50.0)));
    assert_eq!(value("cluster.keyed_started", &[("kind", "account"), ("how", "call")]), Some(Value::Counter(500)));
    assert_eq!(value("cluster.keyed_started", &[("kind", "session"), ("how", "handoff")]), Some(Value::Counter(0)));
    assert_eq!(value("cluster.keyed_ended", &[("kind", "account"), ("reason", "moved")]), Some(Value::Counter(0)));
    system.shutdown().await.unwrap();
}

/// A key may be 64 KiB: a snapshot lists at most a quarter of `max_frame` (1 MiB) of keys, so a node running many long ones still answers a
/// peer, and says its list is cut. A page of `Request::Keys` is cut to the reply's limit too, and says more follow.
#[tokio::test(flavor = "multi_thread")]
async fn long_keys_are_listed_up_to_a_budget_so_a_peer_still_reads_the_snapshot() {
    let ((system, a), (_b_system, b)) = pair().await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    // Only a registers the kind, so every key runs there: 1000 of 1.1 KB would be over a 1 MiB reply.
    common::keyed(&a, "long", (0..1000).map(|i| format!("{i:04}{}", "x".repeat(1100)))).await;
    let access = AccessPolicy::Nodes(["b".to_string()].into());
    let cfg = TelemetryConfig { access, snapshot_keys: 1000, ..Default::default() };
    let _telemetry = Telemetry::start(&system, &a, cfg).await.unwrap();
    let snapshot: Json = fetch(&b, "a", &Request::Snapshot).await.unwrap();
    let listed = snapshot["hosted"]["keys"].as_array().unwrap();
    assert!((100..1000).contains(&listed.len()), "{}", listed.len());
    assert!(serde_json::to_vec(listed).unwrap().len() <= 256 << 10);
    assert_eq!(snapshot["hosted"]["total"], 1000);
    assert!(snapshot["notices"].as_array().unwrap().iter().any(|n| n["code"] == "keys_truncated"));
    let request = Request::Keys { kind: "long".into(), prefix: String::new(), from: None, limit: 1000 };
    let page: KeyPage = fetch(&b, "a", &request).await.unwrap();
    assert!((100..1000).contains(&page.keys.len()) && page.more, "{}", page.keys.len());
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draining_node_shows_why_in_its_snapshot_and_its_record() {
    let (system, cluster) = node("a", &MemoryDiscovery::new()).await;
    // Sampled every 20 ms, so the drain is sampled.
    let cfg = TelemetryConfig { sample_interval: Duration::from_millis(20), ..Default::default() };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    system.spawn(Busy).start().unwrap().tell(()).unwrap();
    let leaving = tokio::spawn({
        let system = system.clone();
        async move { system.shutdown_with_reason(LeaveReason::RollingRestart).await }
    });
    eventually("the snapshot shows the drain and its reason", || async {
        let snapshot: Json = serde_json::from_slice(&telemetry.snapshot()).unwrap_or_default();
        snapshot["lifecycle"] == serde_json::json!({ "phase": "draining", "reason": "RollingRestart" })
    })
    .await;
    leaving.await.unwrap().unwrap();
    assert_eq!(cluster.record().metadata.get(LEAVE_REASON).map(String::as_str), Some("RollingRestart"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_batch_is_not_counted_as_stored() {
    let ((system, a), (_b_system, b)) = pair().await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    let cfg = TelemetryConfig {
        // Sampled every 20 ms, so samples come within the test.
        sample_interval: Duration::from_millis(20),
        max_batch_bytes: 1024,
        access: AccessPolicy::Nodes(["b".to_string()].into()),
        ..Default::default()
    };
    let _telemetry = Telemetry::start(&system, &a, cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let snapshot: Json = fetch(&b, "a", &Request::Snapshot).await.unwrap();
    let notices = snapshot["notices"].as_array().unwrap();
    assert!(notices.iter().any(|n| n["code"] == "sample_batch_dropped"), "every series fits in 1 KiB");
    // Had the dropped batches counted, the smaller ones after them would carry only what changed.
    let page = fetch_samples(&b, "a", None).await.unwrap();
    assert!(page.batches.iter().all(|b| sample(b, "runtime.actors_failed", &[]).is_some()), "{}", page.batches.len());
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_series_is_stored_when_it_changes_and_when_it_is_due_again() {
    let ((system, a), (_b_system, b)) = pair().await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    let every = Duration::from_millis(600);
    let cfg = TelemetryConfig {
        // Sampled every 20 ms, so the rewrite every 600 ms under test comes within it.
        sample_interval: Duration::from_millis(20),
        rewrite_unchanged: every,
        retained_samples: 200,
        access: AccessPolicy::Nodes(["b".to_string()].into()),
        ..Default::default()
    };
    assert!(
        // Over the 30 s most, so refused.
        Telemetry::start(&system, &a, TelemetryConfig { rewrite_unchanged: Duration::from_secs(31), ..cfg.clone() })
            .await
            .is_err()
    );
    let _telemetry = Telemetry::start(&system, &a, cfg).await.unwrap();
    let first = fetch_samples(&b, "a", None).await.unwrap();
    let before = first.batches.iter().rev().find_map(|b| sample(b, "runtime.actors_started", &[])).cloned();
    // Halfway between two rewrites, so a change that waited for one would land far outside the batches checked below.
    tokio::time::sleep(every / 2).await;
    let now = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let spawning = now();
    system.spawn(Worker::default()).start().unwrap();
    let spawned = now();
    tokio::time::sleep(every * 3 / 2).await;

    let page = fetch_samples(&b, "a", None).await.unwrap();
    let skipped = |b: &SampleBatch| match sample(b, "telemetry.samples_skipped", &[]) {
        Some(Value::Counter(n)) => Some(*n),
        _ => None,
    };
    let skipped: Vec<_> = page.batches.iter().map(skipped).collect();
    assert!(skipped.iter().all(Option::is_some) && skipped.windows(2).all(|w| w[0] <= w[1]), "{skipped:?}");
    let full = page.batches.iter().map(|b| b.samples.len()).max().unwrap();
    assert!(page.batches.iter().any(|b| b.samples.len() * 4 < full), "every batch carried every series");
    // Nothing fails here, so this one only comes back when it is due, and it does.
    let failed: Vec<_> = page
        .batches
        .iter()
        .filter(|b| sample(b, "runtime.actors_failed", &[]).is_some())
        .map(|b| b.timestamp_ms)
        .collect();
    assert!(failed.len() >= 2 && failed.windows(2).all(|w| w[1] - w[0] >= every.as_millis() as u64), "{failed:?}");
    // A batch is stamped after it counts, so the change is in one stamped from `spawning` on, and at the latest in the
    // second stamped after `spawned`.
    let Some(Value::Counter(before)) = before else { panic!("the first batch holds every series") };
    let after = page.batches.iter().position(|b| b.timestamp_ms > spawned).expect("batches after the spawn");
    let stored =
        |b: &SampleBatch| matches!(sample(b, "runtime.actors_started", &[]), Some(Value::Counter(n)) if *n > before);
    let window = &page.batches[..(after + 2).min(page.batches.len())];
    assert!(window.iter().filter(|b| b.timestamp_ms >= spawning).any(stored), "a change waited to be stored");
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_named_pools_workers_are_sampled_under_its_name() {
    let discovery = MemoryDiscovery::new();
    let pools = vec![PoolConfig { name: "shards".into(), workers: 2, ..Default::default() }];
    let (a_system, a) = node_on("a", &discovery, RuntimeConfig { workers: 1, pools, ..Default::default() }).await;
    let (b_system, b) = node("b", &discovery).await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    let cfg = TelemetryConfig {
        // Sampled every 50 ms, every sample written, so samples come within the test.
        sample_interval: Duration::from_millis(50),
        rewrite_unchanged: Duration::ZERO,
        access: AccessPolicy::Nodes(["b".to_string()].into()),
        ..Default::default()
    };
    let telemetry = Telemetry::start(&a_system, &a, cfg).await.unwrap();
    eventually("both pools are sampled", || async {
        let page = fetch_samples(&b, "a", None).await.unwrap();
        page.batches.last().is_some_and(|batch| {
            // The shared pool's series keep the labels they had before there were pools.
            let shared = |name: &str| {
                let worker_0 = |s: &Sample| s.labels.len() == 1 && s.labels.get("worker").is_some_and(|w| w == "0");
                batch.samples.iter().any(|s| s.name == name && worker_0(s))
            };
            shared("runtime.worker.turns")
                && shared("runtime.worker.utilisation")
                && sample(batch, "runtime.worker.turns", &[("pool", "shards"), ("worker", "1")]).is_some()
                && sample(batch, "runtime.worker.queued", &[("pool", "shards"), ("worker", "0")]).is_some()
                && sample(batch, "runtime.worker.utilisation", &[("pool", "shards"), ("worker", "1")]).is_some()
        })
    })
    .await;
    let snapshot: Json = serde_json::from_slice(&telemetry.snapshot()).unwrap();
    assert_eq!(snapshot["runtime"]["pools"][0]["name"], "shards");
    assert_eq!(snapshot["worker_utilisation"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["pool_utilisation"][0].as_array().unwrap().len(), 2);
    a_system.shutdown().await.unwrap();
    b_system.shutdown().await.unwrap();
}

#[cfg(feature = "system-metrics")]
#[tokio::test(flavor = "multi_thread")]
async fn process_and_host_metrics_are_sampled() {
    let (system, cluster) = node("solo", &MemoryDiscovery::new()).await;
    let cfg = TelemetryConfig { process_metrics: true, host_metrics: true, ..Default::default() };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    let snapshot: Json = serde_json::from_slice(&telemetry.snapshot()).unwrap();
    assert!(snapshot["process"]["rss_bytes"].as_u64().unwrap() > 0 && snapshot["process"]["cpu_percent"].is_null());
    assert!(snapshot["host"]["total_memory_bytes"].as_u64().unwrap() > 0);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_busiest_connections_get_stored_series() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (b_system, b), (c_system, c)) =
        (node("a", &discovery).await, node("b", &discovery).await, node("c", &discovery).await);
    eventually("membership converges", || async { a.view().members.len() == 3 }).await;
    let cfg = |sample_connections| TelemetryConfig {
        // Sampled every 50 ms, so samples come within the test.
        sample_interval: Duration::from_millis(50),
        sample_connections,
        // Every sample written, changed or not.
        rewrite_unchanged: Duration::ZERO,
        access: AccessPolicy::Cluster,
        ..Default::default()
    };
    let _a = Telemetry::start(&a_system, &a, TelemetryConfig { snapshot_connections: 1, ..cfg(1) }).await.unwrap();
    let _b = Telemetry::start(&b_system, &b, cfg(32)).await.unwrap();
    let _c = Telemetry::start(&c_system, &c, cfg(32)).await.unwrap();

    fetch_samples(&a, "c", None).await.unwrap();
    eventually("the one stored connection is the one in use", || async {
        fetch_samples(&a, "b", None).await.unwrap();
        let page = fetch_samples(&b, "a", None).await.unwrap();
        let batch = page.batches.last().unwrap();
        let peers: Vec<_> = (batch.samples.iter())
            .filter(|s| s.name == "connection.sent_bytes")
            .map(|s| s.labels["peer"].as_str())
            .collect();
        peers == ["b"] && sample(batch, "cluster.connections", &[]).is_some_and(|v| *v != Value::Gauge(1.0))
    })
    .await;
    eventually("the snapshot keeps the busiest connection and names the knob that keeps more", || async {
        fetch_samples(&a, "b", None).await.unwrap();
        let snapshot: Json = fetch(&b, "a", &Request::Snapshot).await.unwrap();
        let notice =
            snapshot["notices"].as_array().unwrap().iter().find(|n| n["code"] == "connections_truncated").cloned();
        snapshot["cluster"]["engine"]["peers"][0]["node"] == "b"
            && notice.is_some_and(|n| n["message"].as_str().unwrap().contains("TelemetryConfig::snapshot_connections"))
    })
    .await;
}

/// A peer's Control, Live and Bulk sessions are three rows in the snapshot and three sets of `connection.*` series, each
/// with its `lane`; `Request::Connection` names the lane it asks about.
#[tokio::test(flavor = "multi_thread")]
async fn each_lane_of_a_peer_is_a_row_and_a_series_of_its_own() {
    let discovery = MemoryDiscovery::new();
    let tweak = |cfg: &mut ClusterConfig| {
        cfg.transport.timing_sample_every = 1;
        // Slow enough that the Bulk session's writer waits for the cap.
        cfg.transport.lanes.bulk.max_bytes_per_sec = Some(1 << 20);
    };
    let system = || ActorSystem::builder().timing_sample_every(1);
    let (a_system, a) = common::loopback("a", discovery.clone(), system(), tweak).await;
    let (b_system, b) = common::loopback("b", discovery.clone(), system(), tweak).await;
    eventually("membership converges", || async { a.view().members.len() == 2 }).await;
    let echo = |_: &axon_net::Identity, body: Bytes, reply: Option<Reply>| {
        drop(reply.map(|reply| reply.send(body)));
        Ok(())
    };
    b.endpoint("echo-live", 7, 1).serve(echo).unwrap();
    b.endpoint("echo-bulk", 7, 1).lane(Lane::Bulk).serve(echo).unwrap();
    let cfg = TelemetryConfig {
        // Sampled every 50 ms, so samples come within the test.
        sample_interval: Duration::from_millis(50),
        // Every sample written, changed or not.
        rewrite_unchanged: Duration::ZERO,
        access: AccessPolicy::Cluster,
        ..Default::default()
    };
    let _a = Telemetry::start(&a_system, &a, cfg.clone()).await.unwrap();
    let _b = Telemetry::start(&b_system, &b, cfg).await.unwrap();
    a.request("b", "echo-live", 7, 1, Bytes::from_static(b"x")).await.unwrap();
    for _ in 0..7 {
        a.request("b", "echo-bulk", 7, 1, Bytes::from(vec![0; 64 << 10])).await.unwrap();
    }

    eventually("the snapshot has a row for each lane to b", || async {
        let snapshot: Json = fetch(&b, "a", &Request::Snapshot).await.unwrap();
        let peers = snapshot["cluster"]["engine"]["peers"].as_array().unwrap().clone();
        let to_b = peers.iter().filter(|p| p["node"] == "b" && p["outgoing"] == true);
        let mut lanes: Vec<_> = to_b.map(|p| p["lane"].as_str().unwrap_or_default().to_owned()).collect();
        lanes.sort();
        lanes == ["bulk", "control", "live"]
    })
    .await;
    eventually("each lane has series of its own, and Bulk's writer waited", || async {
        let page = fetch_samples(&b, "a", None).await.unwrap();
        let batch = page.batches.last().unwrap();
        let on = |lane| [("peer", "b"), ("direction", "out"), ("lane", lane)];
        sample(batch, "connection.calls", &on("bulk")) == Some(&Value::Counter(7))
            && sample(batch, "connection.calls", &on("live")) == Some(&Value::Counter(1))
            && sample(batch, "connection.sent_bytes", &on("control")).is_some()
            && matches!(sample(batch, "connection.paced", &on("bulk")), Some(Value::Counter(ns)) if *ns > 0)
            && sample(batch, "connection.paced", &on("live")) == Some(&Value::Counter(0))
    })
    .await;
    let asked = |lane: &str| {
        let body = serde_json::json!({ "connection": { "peer": "b", "outgoing": true, "lane": lane } });
        let b = b.clone();
        async move {
            let reply = b.control("a", "axon.telemetry", serde_json::to_vec(&body).unwrap().into()).await.unwrap();
            serde_json::from_slice::<Json>(&reply).unwrap()["request_rtt"]["count"].as_u64()
        }
    };
    assert_eq!((asked("bulk").await, asked("live").await), (Some(7), Some(1)), "each lane's own request round trips");

    a_system.shutdown().await.unwrap();
    b_system.shutdown().await.unwrap();
}

/// A refusal at a namespace's limit is sampled under the limit it met.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_at_a_namespace_limit_is_sampled_by_its_limit() {
    use axon_stream::{DeliveryPolicy, NamespaceLimits, StreamKey, StreamNode, StreamNodeConfig};
    let ((system, a), (_b_system, b)) = pair().await;
    eventually("membership converges", || async { b.view().members.len() == 2 }).await;
    let cfg = TelemetryConfig {
        // Sampled every 20 ms, so the refusal shows within the test.
        sample_interval: Duration::from_millis(20),
        access: AccessPolicy::Nodes(["b".to_string()].into()),
        ..Default::default()
    };
    let telemetry = Telemetry::start(&system, &a, cfg).await.unwrap();
    let md = NamespaceLimits { ring_bytes: Some(4), ..NamespaceLimits::default() };
    let streams = StreamNode::start(&a, StreamNodeConfig::new(vec![]).limit("md", md)).await.unwrap();
    let key = StreamKey::new("md", "btc");
    streams.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    assert!(streams.publish(&key, vec![bytes::Bytes::from_static(b"12345")]).is_err());
    telemetry.watch_streams(&streams);
    eventually("the refusal is sampled", || async {
        let page = fetch_samples(&b, "a", None).await.unwrap();
        let refused = |batch: &SampleBatch| sample(batch, "stream.quota_refused", &[("reason", "ring_bytes")]).cloned();
        page.batches.iter().any(|batch| matches!(refused(batch), Some(Value::Counter(1))))
    })
    .await;
    drop(streams);
    system.shutdown().await.unwrap();
}

/// Telemetry copies every link's stats each second, so they keep each histogram's nonzero buckets, not all `BUCKETS`
/// of them: six dense ones made a link's stats 15 KB, and a sample of 200 nodes with 20 links each took 0.3 cores more.
#[test]
fn a_links_stats_keep_their_histograms_sparse() {
    let size = size_of::<axon_cluster::PeerStats>();
    assert!(size < 1024, "a link's stats take {size} B");

    // Sent as the `Histogram` it stands for, so the console and a collector read it unchanged.
    let shared = axon::AtomicHistogram::default();
    [1000, 1001, 1100].into_iter().for_each(|ns| shared.record(Duration::from_nanos(ns)));
    let (snapshot, dense) = (shared.snapshot(), axon::Histogram::from(&shared.snapshot()));
    assert_eq!(serde_json::to_string(&snapshot).unwrap(), serde_json::to_string(&dense).unwrap());
    assert_eq!(postcard::to_stdvec(&snapshot).unwrap(), postcard::to_stdvec(&dense).unwrap());
    // What the dense one serialised as before snapshots were sparse.
    assert_eq!(serde_json::to_string(&dense).unwrap(), r#"{"first":63,"counts":[2,1],"count":3,"sum_ns":3101}"#);
    assert_eq!(postcard::to_stdvec(&dense).unwrap(), [63, 2, 2, 1, 3, 157, 24]);
    let padded = r#"{"first":62,"counts":[0,2,1,0],"count":3,"sum_ns":3101}"#;
    assert_eq!(serde_json::from_str::<axon::HistogramSnapshot>(padded).ok(), Some(snapshot));
}
