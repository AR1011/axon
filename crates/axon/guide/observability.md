# Observability

Crate: `axon-obs`. Every Rust snippet here is compiled, and the complete programs are run, by
`cargo test --doc -p axon-obs --all-features`.

Four extensions, each optional. The first three are carried over the cluster's own authenticated sessions; `Health`
answers plain HTTP without credentials:

| Extension | Runs on | Does |
|---|---|---|
| `Telemetry` | every node | samples the node once a second; answers questions about its actors, connections, streams, traces and dead letters |
| `MetricsService` | one collector node | pulls samples, each node's events, spans and dead letters from every node into a store; answers history queries |
| `Dashboard` | any node | serves the web console and its JSON API |
| `Health` | any node without a dashboard, and any node Prometheus scrapes | serves `/healthz` and `/readyz` ([probes](#health-probes)), and `/metrics` once asked to ([Prometheus](#prometheus)) |

```toml
[dependencies]
axon-obs = { git = "<this repository's url>", features = ["dashboard"] }   # also: "clickhouse", "system-metrics"
```

Try it without writing code: `cargo run -p axon-obs --features dashboard,system-metrics --example playground` starts a
six-node cluster with traffic and prints the console's address.

## A node with a console

```rust,no_run
use std::sync::Arc;

use axon::ActorSystem;
use axon_cluster::{AccessPolicy, Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_obs::{Dashboard, DashboardConfig, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long")?);
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse()?, transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().timing_sample_every(64).tracing(4096, 1000).build()?;
    let cluster = Cluster::start(&system, cfg).await?;

    // every node
    let telemetry = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    Telemetry::start(&system, &cluster, telemetry).await?;

    // one node: keeps history. Here in memory, at most 256 MiB of samples.
    let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
    MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;

    // any node
    let dashboard = Dashboard::start(&system, &cluster, DashboardConfig::default()).await?;
    println!("console: http://{}", dashboard.address()); // 127.0.0.1:8080 by default

    tokio::signal::ctrl_c().await?;
    system.shutdown().await?;
    Ok(())
}
```

- `access` decides which nodes may ask. It defaults to `Deny`, so a console sees nothing until you open it:
  `AccessPolicy::Cluster` for every authenticated node, or `AccessPolicy::Nodes(..)` to name the console and collector.
  A refused caller hears it in words, "b refuses to answer: its TelemetryConfig::access (default Deny) or
  ClusterConfig::authorizer does not admit this node", and the console repeats it at the top of every page.
- The extensions live as long as the `ActorSystem`. Dropping their handles stops nothing.
- The console is read-only. A bind address that is not loopback must list `DashboardConfig::users`, who sign in on a
  login page, and needs TLS: its own (`DashboardConfig::tls`) or a proxy's ([dashboard § Hosting](dashboard.md#hosting)).
- `MemoryMetrics::new(max_bytes)` bounds the samples it holds by their size in memory and lets the oldest batches go
  first; `MetricsHealth.sink_usage` says how much it holds and how much it has let go, and Settings shows it per table
  ([telemetry § Collection and history](telemetry.md#collection-and-history)). A bound below one batch holds
  nothing. A histogram keeps only its buckets from the first non-zero one to the last. A batch shaped like the
  playground's, a node's second of 147 samples with 35 histograms, counts about 63 KB with the allocator's overhead,
  so 256 MiB is about 8 minutes of its nine nodes. The newest 1024 events, 65 536 spans and 4096 dead letters are
  kept besides, outside the bound.
- The console finds the collector by itself through cluster metadata. With several collectors it uses the freshest;
  pin one with `DashboardConfig::history_node`.
- `timing_sample_every(64)` times one message in 64, which feeds the latency charts. `tracing(4096, 1000)` keeps 4096
  spans per node and starts a trace for one message in 1000. Both are off by default.
- A timing is a `Histogram` of eight buckets per power of two, so a percentile it reports (`h.percentile(0.99)`, or
  p50 · p95 · p99 in the console) is the upper bound of its bucket: never below the true value and at most 12.5 % above
  it, up to about 17 min; above that it is the open last bucket, `Duration::from_nanos(u64::MAX)`. `Histogram::record`
  is an index computation and an add, with no allocation; an `AtomicHistogram` does the same with relaxed atomics for a
  shared path. Its `snapshot()` is a `HistogramSnapshot`, only the run of buckets from the first non-zero one to the
  last, read from between the lowest and highest bucket recorded into: a link's stats (`SessionTimings`,
  `PeerStats::request_rtt`) hold six of them, and telemetry takes every link's each second
  ([benchmarks](benchmarks.md#telemetry-overhead-axon-obs)). `Histogram::from(&snapshot)` gives back the full one to
  merge or read percentiles from; both serialise the same. An actor's timings are one `Timings` per actor behind a
  mutex, which telemetry's scan also takes to copy them, about 10 KB and 2.4 KB more per message label.

Pages: overview (its topology panel opens full size), cluster, placement (the keys each node runs, with lease epochs), nodes, actors (with per-message
timings), workload (the backlog heatmap and handler time by message), connections, streams, traces, dead letters,
events, settings. All view state is in the URL. Details: [dashboard](dashboard.md).

## Health probes

`GET /healthz` and `GET /readyz` answer orchestrators and load balancers: 200 when every check is `"ok"`, 503
otherwise, with the checks in the body. They need no credentials and ignore `base_path`: under it the same names
answer 404, so a probe pointed there fails instead of reading the console's page as healthy. A node with a `Dashboard`
serves them on its port; any other node starts `Health` on a port of its own (feature `dashboard`), which stops with
the system after its actors have drained, so a draining node keeps answering 503. Either port is free to bind again
once `shutdown` returns, unless its report's `failures` names it because the budget ran out, so a process that
restarts takes it straight back. Either port bounds its connections and request heads
([dashboard § Hosting](dashboard.md#hosting)). Probes on the dashboard's port share its places with the console's
clients, so where untrusted clients can reach that port, start `Health` on a port of its own for the orchestrator too.

```rust,no_run
# use axon::ActorSystem;
# use axon_cluster::Cluster;
# async fn probes(system: &ActorSystem, cluster: &Cluster) -> Result<(), String> {
let health = axon_obs::Health::start(system, cluster, "0.0.0.0:8081".parse().expect("an address")).await?;
println!("probes: http://{}/readyz", health.address());
# Ok(())
# }
```

| Route | Checks | 503 when |
|---|---|---|
| `healthz` | `system` | never: answering is the check. Once the system is shut down or dropped the port is closed, and a refused connection fails the probe |
| `readyz` | `system`, `cluster`, `ownership` (`ok` without an authority), `quotas` (`ok` without [`Quotas`](cluster.md#quotas)) | `shutting down`; `isolated` (discovery cannot hear the node and no peer vouches for it, so its keyed actors are down); `warming` (a restarted Paxos acceptor still refuses); `starting` (a new or restarted quota replica has not pulled the other replicas' rows yet) |

```json
{"ok": false, "checks": {"system": "ok", "cluster": "isolated", "ownership": "ok", "quotas": "ok"}}
```

Each answer is computed when asked, so `readyz` turns within a probe period of the node isolating or starting
shutdown. A Kubernetes pod with the probes on port 8081:

```yaml
livenessProbe:  { httpGet: { path: /healthz, port: 8081 }, periodSeconds: 10, failureThreshold: 3 }
readinessProbe: { httpGet: { path: /readyz, port: 8081 }, periodSeconds: 1, failureThreshold: 1 }
```

Liveness stays 200 while the node is isolated, warming or draining: a restart would not help any of them. In a rolling
restart of Paxos acceptors or quota nodes, wait for `readyz` on each before restarting the next. The console shows the
same checks per node, from `ClusterStats::isolated`, `warming` and `quotas_starting` and the node's phase, in the nodes
page's Ready column.

## Prometheus

`health.serve_metrics(&telemetry)` answers `GET /metrics` on the `Health` port with the node's latest telemetry sample
in the Prometheus text format (0.0.4), so a stock Prometheus scrapes every node itself, with no collector between. Until
it is called the route answers 404. It is on the `Health` port, not the `Dashboard`'s, because a scraper sends a
basic-auth or bearer header and cannot sign in to the console, and because the port already caps connections, per
address and in all, and cuts a head or an answer that stalls ([dashboard § Hosting](dashboard.md#hosting)). It
carries no credentials, so bind it where only the scraper and the orchestrator reach it. `DashboardConfig::redact` is
the console's alone: `/metrics` shows the `persist_actors` names, `Actor::label` values and peer node ids as they are,
so list no keyed actor in `persist_actors` whose key is a secret.

```rust,no_run
use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_obs::{Health, Telemetry, TelemetryConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long")?);
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse()?, transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().timing_sample_every(64).build()?;
    let cluster = Cluster::start(&system, cfg).await?;

    let telemetry = Telemetry::start(&system, &cluster, TelemetryConfig::default()).await?;
    let health = Health::start(&system, &cluster, "0.0.0.0:8081".parse()?).await?;
    health.serve_metrics(&telemetry);
    println!("scrape http://{}/metrics", health.address());

    tokio::signal::ctrl_c().await?;
    system.shutdown().await?;
    Ok(())
}
```

```yaml
scrape_configs:
  - job_name: axon
    scrape_interval: 15s
    static_configs:
      - targets: ["node-1:8081", "node-2:8081", "node-3:8081"]
```

The series are the ones Telemetry samples ([telemetry § Persisted samples](telemetry.md#persisted-samples)), but for
`telemetry.samples_skipped`, which counts what the store was spared, as they stood at its last sample, so a scrape is at
most `sample_interval` (1 s) old; `TelemetryConfig::access` governs only the cluster's requests, not this port. A name
is `axon_` and the telemetry name with `_` for `.`, then the unit where it is a base one, then `_total` on a counter:
`actor.processed` is `axon_actor_processed_total`, `runtime.worker.busy` `axon_runtime_worker_busy_seconds_total`,
`stream.retained` `axon_stream_retained_bytes`, `runtime.worker.utilisation` `axon_runtime_worker_utilisation_ratio`.
Nanoseconds are given in seconds. A histogram is `_bucket`, `_sum` and `_count`, with `le` at each power of four from 15
ns to 275 s (15 ns, 63 ns, 255 ns, …) and `+Inf`: 19 bounds its buckets hold exactly, so `histogram_quantile` lands in
the right factor of four and `_sum / _count` is exact. Labels are those of the table: per actor type and message label,
never per actor except the at most 64 names in `persist_actors`, and per connection only the `sample_connections`
busiest, one per peer and direction, so the count of series does not grow with the actors or the cluster. A persisted
actor's series are a family of their own, `axon_persisted_actor_processed_total{actor="pinned"}`, since its type's
`axon_actor_processed_total` already counts it: a `sum` over either family counts each message once. A type's counters
take in its exited actors and every generation of a supervised one, so they never go back while the node runs, and
once shown a type's counter and histogram series stay in every scrape. Counters
restart with each node incarnation, each connection's session and each new actor under a persisted name, which
Prometheus reads as a reset. The text is rendered once per sample, on the first scrape after it, and every scrape until
the next one shares it.

## History in ClickHouse

```rust,no_run
use std::sync::Arc;

use axon::ActorSystem;
use axon_cluster::{AccessPolicy, Cluster};
use axon_obs::{ClickHouseConfig, ClickHouseProvider, MetricsConfig, MetricsService};

async fn collector(system: &ActorSystem, cluster: &Cluster) -> Result<(), String> {
    let store = ClickHouseProvider::connect(ClickHouseConfig {
        url: "http://127.0.0.1:8123".into(),
        username: "axon".into(),
        password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
        ..Default::default() // database axon_metrics, 30 days raw, 365 days of rollups
    })
    .await?;
    let cfg = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
    MetricsService::start(system, cluster, Arc::new(store), cfg).await?;
    Ok(())
}
```

Feature `clickhouse`. The provider creates its tables, with one-minute and one-hour rollups that long time ranges
read from. Writes are idempotent, so a retried batch is stored once. To write somewhere else, implement
`MetricsSink` (two required methods) and, to serve history, `MetricsQuery`. A sink that adds up what it is given
should also answer `stored_through`: a new collector, or one whose write got no answer, resumes after the position
the sink reports instead of sending those batches again. Several sinks can run at once with
`MetricsService::with_providers`.

What is sampled and how it is stored: [telemetry](telemetry.md).

## Ask from code

```rust,no_run
use axon_cluster::Cluster;
use axon_obs::{HistoryQuery, HistoryResult, MetricsRequest, Request, fetch, query_metrics};

async fn look(cluster: &Cluster, now_ms: u64) -> Result<(), String> {
    // live, from one node
    let snapshot: serde_json::Value = fetch(cluster, "node-7", &Request::Snapshot).await?;
    println!("{}", snapshot["actors"]);

    // history, from the collector: handler latency over the last hour in 10 s steps
    let query = HistoryQuery::new("actor.handler", now_ms - 3_600_000, now_ms, 10_000);
    let history: HistoryResult = query_metrics(cluster, "collector", &MetricsRequest::Query(query)).await?;
    for series in history.series {
        let known = series.values.iter().flatten().count();
        println!("{} {:?}: {known} of {} steps", series.node_id, series.labels, history.count);
    }
    Ok(())
}
```

The same data is served as JSON over HTTP by the dashboard (`api/overview`, `api/actors`, `api/history`,
`api/traces`, `api/dead-letters`, ...).

## Tracing

A trace follows a message through every actor, node and stream it causes. Start one with `TraceContext::root()`, or
sample them with `ActorSystem::builder().tracing(spans, sample_every)`; the collector stores the spans, and the
console's traces page shows each trace as a waterfall. How the context travels, what each span records and how to
read one back: [tracing](tracing.md).

## Dead letters

A dead letter is a message a node accepted and never handled, such as one still queued when its actor stops. A send
refused up front is not one, because the sender is told.

```rust
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};

struct Slow;

impl Actor for Slow {
    type Message = u32;
    type Error = ();
    fn on_message(&mut self, _msg: u32, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(Duration::from_millis(100));
        Ok(())
    }
    fn label(_msg: &u32) -> &'static str {
        "work"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let slow = system.spawn(Slow).name("slow").start()?;
    for n in 0..4u32 {
        slow.tell(n)?;
    }
    slow.lifecycle.stop(); // the queue is dropped
    slow.join().await; // once it returns, the dead letters are recorded

    for letter in system.dead_letters().recent(10) {
        println!("{} x{} for {:?}: {}", letter.label, letter.count, letter.target, letter.reason);
    }
    assert!(system.stats().dead_letters >= 1);
    Ok(())
}
```

What else counts, the ring each node keeps and how repeats share a row, and where the collector stores them:
[telemetry § Dead letters](telemetry.md#dead-letters). The console lists them with a link to the trace when there is
one. A traced message dropped before any handler ran it leaves one failed handle span of zero length in the tracer,
named by its label, under the span that sent it, so the link opens the trace at the place the message ended.
