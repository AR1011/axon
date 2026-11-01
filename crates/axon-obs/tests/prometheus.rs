//! `/metrics` on the `Health` port: this node's telemetry in the Prometheus text format, checked line by line.
#![cfg(feature = "dashboard")]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

use axon::{Actor, ActorSystem, Backoff, Context, ReplyTo, RestartPolicy};
use axon_cluster::{AccessPolicy, Cluster, MemoryDiscovery};
use axon_obs::{Health, Telemetry, TelemetryConfig, Value, fetch_samples};
use axon_testkit::eventually;
use tokio::net::TcpStream;
use tokio::time::Instant;

mod common;

/// A message label that would forge a line, or end a label value, if it were not escaped.
const ODD: &str = "say \"hi\"\\\nnow";

enum Msg {
    Work,
    Odd,
    Count(ReplyTo<u64>),
    Fail,
}

#[derive(Default)]
struct Worker(u64);

impl Actor for Worker {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Work | Msg::Odd => self.0 += 1,
            Msg::Count(reply) => drop(reply.reply(self.0)),
            Msg::Fail => return Err(()),
        }
        Ok(())
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Work => "work",
            Msg::Odd => ODD,
            Msg::Count(_) => "count",
            Msg::Fail => "fail",
        }
    }
}

/// Fails on `Fail`. `Brief<0>` and `Brief<1>` are types apart, so that neither's totals hide a dip in the other's.
struct Brief<const TYPE: u8>;

impl<const TYPE: u8> Actor for Brief<TYPE> {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if matches!(msg, Msg::Fail) { Err(()) } else { Ok(()) }
    }
}

async fn node_in(id: &str, discovery: MemoryDiscovery) -> (ActorSystem, Cluster) {
    common::loopback(id, discovery, ActorSystem::builder().timing_sample_every(1), |_| {}).await
}

type Labels = BTreeMap<String, String>;

/// One scrape, every line checked: each family has one HELP and right after it one TYPE, its samples follow before the
/// next family, names and label names are legal, label values are escaped, no series comes twice, and each histogram's
/// buckets rise in `le` and in count and end in `+Inf`, equal to its `_count`.
#[derive(Debug, Default)]
struct Scrape {
    kinds: BTreeMap<String, String>,
    series: BTreeMap<(String, Labels), f64>,
}

impl Scrape {
    fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let labels = labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        self.series.get(&(name.to_string(), labels)).copied()
    }
}

fn legal(name: &str, colon: bool) -> bool {
    let first = |c: char| c.is_ascii_alphabetic() || c == '_' || (colon && c == ':');
    let mut chars = name.chars();
    chars.next().is_some_and(first) && chars.all(|c| first(c) || c.is_ascii_digit())
}

fn number(text: &str) -> f64 {
    match text {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        _ => text.parse().unwrap_or_else(|_| panic!("not a number: {text:?}")),
    }
}

/// `name{a="x",b="y"} value`, with no timestamp.
fn sample_line(line: &str) -> (String, Labels, f64) {
    let end = line.find(['{', ' ']).unwrap_or_else(|| panic!("no value: {line:?}"));
    let (name, mut rest) = line.split_at(end);
    assert!(legal(name, true), "illegal name {name:?}");
    let mut labels = Labels::new();
    if let Some(inner) = rest.strip_prefix('{') {
        rest = inner;
        while !rest.starts_with('}') {
            let (label, after) = rest.split_once("=\"").unwrap_or_else(|| panic!("no label value: {line:?}"));
            assert!(legal(label, false) && !label.starts_with("__"), "illegal label name {label:?}");
            let (mut value, mut chars) = (String::new(), after.char_indices());
            let close = loop {
                match chars.next().unwrap_or_else(|| panic!("unterminated label value: {line:?}")) {
                    (at, '"') => break at,
                    (_, '\\') => match chars.next().map(|(_, c)| c) {
                        Some('\\') => value.push('\\'),
                        Some('"') => value.push('"'),
                        Some('n') => value.push('\n'),
                        other => panic!("bad escape {other:?} in {line:?}"),
                    },
                    (_, c) => value.push(c),
                }
            };
            assert!(labels.insert(label.to_string(), value).is_none(), "label {label} twice: {line:?}");
            rest = &after[close + 1..];
            rest = rest.strip_prefix(',').unwrap_or(rest);
        }
        rest = &rest[1..];
    }
    let value = rest.strip_prefix(' ').unwrap_or_else(|| panic!("no space before the value: {line:?}"));
    assert!(!value.contains(' '), "a timestamp or junk after the value: {line:?}");
    (name.to_string(), labels, number(value))
}

fn parse(text: &str) -> Scrape {
    assert!(text.ends_with('\n'), "the last line ends too");
    let (mut scrape, mut helped, mut family) = (Scrape::default(), BTreeSet::new(), None::<(String, String)>);
    let mut help = None;
    let mut buckets: BTreeMap<(String, Labels), Vec<(f64, f64)>> = BTreeMap::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, _) = rest.split_once(' ').unwrap_or((rest, ""));
            assert!(legal(name, true) && helped.insert(name.to_string()), "HELP {name} is illegal or twice");
            (help, family) = (Some(name.to_string()), None);
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').unwrap();
            assert_eq!(help.take().as_deref(), Some(name), "TYPE {name} comes right after its HELP");
            assert!(["counter", "gauge", "histogram"].contains(&kind), "{line}");
            assert!(kind != "counter" || name.ends_with("_total"), "a counter's name ends in _total: {name}");
            assert!(scrape.kinds.insert(name.into(), kind.into()).is_none(), "TYPE {name} twice");
            family = Some((name.into(), kind.into()));
        } else {
            assert!(!line.starts_with('#') && !line.is_empty(), "a stray line: {line:?}");
            let (name, mut labels, value) = sample_line(line);
            let (family, kind) = family.as_ref().unwrap_or_else(|| panic!("a sample outside its family: {line:?}"));
            let suffix = name.strip_prefix(family.as_str()).unwrap_or_else(|| panic!("{name} under {family}"));
            match (kind.as_str(), suffix) {
                ("histogram", "_bucket") => {
                    let le = number(&labels.remove("le").unwrap_or_else(|| panic!("no le: {line:?}")));
                    buckets.entry((family.clone(), labels.clone())).or_default().push((le, value));
                    labels.insert("le".into(), format!("{le}"));
                }
                ("histogram", "_sum" | "_count") | ("counter" | "gauge", "") => {}
                _ => panic!("{name} does not belong to {kind} {family}"),
            }
            assert!(scrape.series.insert((name, labels), value).is_none(), "a series twice: {line:?}");
        }
    }
    assert!(help.is_none(), "a HELP without its TYPE");
    for ((family, labels), buckets) in &buckets {
        assert!(buckets.windows(2).all(|w| w[0].0 < w[1].0 && w[0].1 <= w[1].1), "{family} {labels:?}: {buckets:?}");
        let (le, all) = *buckets.last().unwrap();
        assert_eq!(le, f64::INFINITY, "{family} {labels:?} ends in +Inf");
        assert_eq!(scrape.series.get(&(format!("{family}_count"), labels.clone())), Some(&all), "{family} {labels:?}");
        assert!(scrape.series.contains_key(&(format!("{family}_sum"), labels.clone())), "{family} {labels:?}");
    }
    let histograms = scrape.kinds.iter().filter(|(_, kind)| *kind == "histogram").count();
    assert_eq!(buckets.keys().map(|(family, _)| family).collect::<BTreeSet<_>>().len(), histograms);
    scrape
}

async fn scrape(health: SocketAddr) -> Result<String, u16> {
    let answer = reqwest::get(format!("http://{health}/metrics")).await.unwrap();
    match answer.status().as_u16() {
        200 => {
            let kind = answer.headers()["content-type"].to_str().unwrap().to_string();
            assert_eq!(kind, "text/plain; version=0.0.4; charset=utf-8");
            Ok(answer.text().await.unwrap())
        }
        status => Err(status),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_are_off_until_served_and_show_the_nodes_latest_sample_escaped_in_the_text_format() {
    let (system, cluster) = node_in("a", MemoryDiscovery::new()).await;
    let pinned = system.spawn(Worker::default()).name("pinned").start().unwrap();
    for n in 0..100 {
        pinned.send(if n % 5 < 3 { Msg::Work } else { Msg::Odd }).await.unwrap();
    }
    assert_eq!(pinned.ask(Msg::Count).await.unwrap(), 100);
    let cfg = TelemetryConfig {
        // Once an hour: the one sample is taken at start, so the scrape and the stored batch are the same sample.
        sample_interval: Duration::from_secs(3600),
        persist_actors: vec!["pinned".into()],
        access: AccessPolicy::Cluster,
        ..Default::default()
    };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    assert_eq!(scrape(health.address()).await, Err(404), "off until served");

    health.serve_metrics(&telemetry);
    let text = scrape(health.address()).await.unwrap();
    let exposed = parse(&text);
    assert!(text.contains(r#"message="say \"hi\"\\\nnow""#), "{text}");
    let worker = std::any::type_name::<Worker>();
    let handled = |family: &str, by: &str, name: &str, message: &str| {
        exposed.get(&format!("axon_{family}message_handler_seconds_count"), &[(by, name), ("message", message)])
    };
    let (by_actor, by_type) =
        (handled("persisted_actor_", "actor", "pinned", ODD), handled("actor_", "actor_type", worker, "work"));
    assert_eq!((by_actor, by_type), (Some(40.0), Some(60.0)));
    assert_eq!(exposed.get("axon_persisted_actor_admitted_total", &[("actor", "pinned")]), Some(101.0));
    assert_eq!(exposed.get("axon_actor_admitted_total", &[("actor_type", worker)]), Some(101.0));
    assert_eq!(exposed.get("axon_actor_count", &[("actor_type", worker)]), Some(1.0));
    assert_eq!(exposed.kinds["axon_runtime_worker_busy_seconds_total"], "counter");

    // Every series of the batch Telemetry stored is exposed, and nothing else is: one line each, and a histogram 18
    // bounds, `+Inf`, `_sum` and `_count`. `src/prometheus.rs` pins how each is written. The batch's own count of what
    // it spared the store is not a series of the node.
    let page = fetch_samples(&cluster, "a", None).await.unwrap();
    let [batch] = &page.batches[..] else { panic!("one sample: {:?}", page.batches.len()) };
    let lines = |value: &Value| if matches!(value, Value::Histogram(_)) { 21 } else { 1 };
    let series: usize =
        batch.samples.iter().filter(|s| s.name != "telemetry.samples_skipped").map(|s| lines(&s.value)).sum();
    assert_eq!(exposed.series.len(), series, "exposed beyond the batch, or short of it");
    system.shutdown().await.unwrap();
}

/// While actors work, a supervised one fails and restarts every 50 messages, and others of its type come and go past
/// 2000 idle ones, no counter or histogram goes back, and no series leaves once shown, changed or not.
#[tokio::test(flavor = "multi_thread")]
async fn counters_and_histograms_never_go_backwards_across_scrapes_while_the_node_works() {
    let (system, cluster) = node_in("a", MemoryDiscovery::new()).await;
    let cfg = TelemetryConfig {
        // Sampled every 20 ms, so the scrapes below see many samples.
        sample_interval: Duration::from_millis(20),
        ..Default::default()
    };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&telemetry);
    let idle: Vec<_> = (0..2000).map(|_| system.spawn(Worker::default()).start().unwrap()).collect();
    let workers: Vec<_> = (0..4).map(|_| system.spawn(Worker::default()).start().unwrap()).collect();
    // Restarts for as long as the test runs, each soon after the failure.
    let backoff = Backoff { max: Duration::from_millis(20), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { max_restarts: usize::MAX, backoff, ..Default::default() };
    let family = system.supervise(|| Brief::<1>).policy(policy).start_supervised().unwrap();
    let (spawner, mut n) = (system.clone(), 0u64);
    let busy = tokio::spawn(async move {
        loop {
            for worker in &workers {
                if worker.send(Msg::Work).await.is_err() {
                    return;
                }
            }
            n += 1;
            let _ = family.send(if n % 50 == 0 { Msg::Fail } else { Msg::Work }).await;
            let Ok(brief) = spawner.spawn(Brief::<0>).start() else { return };
            for _ in 0..5 {
                let _ = brief.send(Msg::Work).await;
            }
            brief.lifecycle.stop();
            tokio::task::yield_now().await;
        }
    });

    let rising = |kind: &str, name: &str| {
        kind == "counter" || (kind == "histogram" && (name.ends_with("_bucket") || name.ends_with("_count")))
    };
    let (mut last, mut scrapes, mut changed, mut exposed) = (BTreeMap::new(), 0, 0, Scrape::default());
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(1500) {
        exposed = parse(&scrape(health.address()).await.unwrap());
        for key in last.keys() {
            assert!(exposed.series.contains_key(key), "{key:?} left the scrape");
        }
        for ((name, labels), value) in &exposed.series {
            let family = exposed.kinds.keys().rev().find(|f| name.starts_with(f.as_str())).unwrap();
            if !rising(&exposed.kinds[family], name) {
                continue;
            }
            if let Some(before) = last.insert((name.clone(), labels.clone()), *value) {
                assert!(*value >= before, "{name} {labels:?} went back from {before} to {value}");
                changed += (*value > before) as usize;
            }
        }
        scrapes += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    busy.abort();
    assert!(scrapes >= 20 && changed >= 20, "{scrapes} scrapes, {changed} rises");
    let restarts = exposed.get("axon_actor_restarts_total", &[("actor_type", std::any::type_name::<Brief<1>>())]);
    assert!(restarts >= Some(3.0), "the family restarted while scraped: {restarts:?}");
    drop(idle);
    system.shutdown().await.unwrap();
}

/// A thousand actors of one type are one series per metric, and only an actor named in `persist_actors` gets its own.
#[tokio::test(flavor = "multi_thread")]
async fn series_are_per_actor_type_and_per_persisted_actor_never_per_actor() {
    let (system, cluster) = node_in("a", MemoryDiscovery::new()).await;
    let mut actors = Vec::new();
    for n in 0..1000 {
        let spawn = system.spawn(Worker::default());
        let spawn = match n {
            0 => spawn.name("pinned"),
            1 => spawn.name("secret/key"),
            _ => spawn,
        };
        actors.push(spawn.start().unwrap());
    }
    for actor in &actors {
        actor.send(Msg::Work).await.unwrap();
    }
    let cfg = TelemetryConfig { persist_actors: vec!["pinned".into()], ..Default::default() };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&telemetry);
    let text = scrape(health.address()).await.unwrap();
    let exposed = parse(&text);
    let worker = std::any::type_name::<Worker>();
    assert_eq!(exposed.get("axon_actor_count", &[("actor_type", worker)]), Some(1000.0));
    assert_eq!(exposed.get("axon_actor_admitted_total", &[("actor_type", worker)]), Some(1000.0));
    let actors: BTreeSet<_> = exposed.series.keys().filter_map(|(_, labels)| labels.get("actor")).collect();
    assert_eq!(actors, BTreeSet::from([&"pinned".to_string()]));
    assert!(!text.contains("secret"), "an actor name not listed in persist_actors never shows");
    assert!(exposed.series.len() < 500, "{} series for 1000 actors", exposed.series.len());
    system.shutdown().await.unwrap();
}

/// Dropped without a shutdown, the node's port closes; booted again on the same port, it serves the new boot's counters,
/// which start again from zero as a Prometheus counter reset.
#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_node_booted_again_serves_its_new_counters_on_the_same_port() {
    let mut bind = SocketAddr::from(([127, 0, 0, 1], 0));
    for (boot, work) in [(0, 10), (1, 3)] {
        let (system, cluster) = node_in("a", MemoryDiscovery::new()).await;
        let worker = system.spawn(Worker::default()).name("pinned").start().unwrap();
        for _ in 0..work {
            worker.send(Msg::Work).await.unwrap();
        }
        assert_eq!(worker.ask(Msg::Count).await.unwrap(), work);
        let cfg = TelemetryConfig {
            // Sampled every 20 ms, so the scrape follows the workload.
            sample_interval: Duration::from_millis(20),
            persist_actors: vec!["pinned".into()],
            ..Default::default()
        };
        let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
        // Below the ephemeral ranges (32768 up on Linux, 49152 on macOS): between the boots, another process's outbound
        // connection or port 0 bind may take a freed ephemeral port, but never this one.
        let health = loop {
            if boot == 0 {
                bind.set_port(10_000 + (axon::trace::random_u64() % 22_000) as u16);
            }
            match Health::start(&system, &cluster, bind).await {
                Ok(health) => break health,
                Err(e) => assert_eq!(boot, 0, "{e}"),
            }
        };
        health.serve_metrics(&telemetry);
        eventually("the boot's own count is served", || async {
            let exposed = parse(&scrape(bind).await.unwrap());
            exposed.get("axon_persisted_actor_processed_total", &[("actor", "pinned")]) == Some(work as f64 + 1.0)
        })
        .await;
        if boot == 0 {
            drop((system, cluster, telemetry, health, worker));
            eventually("the port closes", || async { TcpStream::connect(bind).await.is_err() }).await;
        } else {
            system.shutdown().await.unwrap();
        }
    }
}

/// A peer with a session on Control and one on Live, and a second boot of it that replaces both, is a series for each
/// lane, never one twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_is_a_series_for_each_lane() {
    let discovery = MemoryDiscovery::new();
    let (system, cluster) = node_in("a", discovery.clone()).await;
    let cfg = TelemetryConfig {
        // Sampled every 20 ms, so the scrape follows the connections.
        sample_interval: Duration::from_millis(20),
        access: AccessPolicy::Cluster,
        ..Default::default()
    };
    let telemetry = Telemetry::start(&system, &cluster, cfg).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&telemetry);
    let mut peers = Vec::new();
    for _ in 0..2 {
        let (peer, peer_cluster) = node_in("b", discovery.clone()).await;
        // While both boots are up a session can be replaced under the call (OutcomeUnknown): a read may go again.
        eventually("b reads the samples of a", || async { fetch_samples(&peer_cluster, "a", None).await.is_ok() })
            .await;
        peers.push((peer, peer_cluster));
    }
    let lanes = || {
        let peers = cluster.stats().engine.peers;
        peers.iter().filter(|p| &*p.node == "b" && !p.outgoing).map(|p| p.lane).collect::<Vec<_>>()
    };
    eventually("a session per lane, the first boot's replaced", || async {
        lanes() == [axon::Lane::Control, axon::Lane::Live]
    })
    .await;
    eventually("the scrape shows each lane of the peer", || async {
        let exposed = parse(&scrape(health.address()).await.unwrap());
        let calls =
            |lane| exposed.get("axon_connection_calls_total", &[("direction", "in"), ("lane", lane), ("peer", "b")]);
        calls("control").is_some() && calls("live").is_some()
    })
    .await;
    for (peer, _) in peers {
        peer.shutdown().await.unwrap();
    }
    system.shutdown().await.unwrap();
}
