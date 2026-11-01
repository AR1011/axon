//! Faults on the wire, the clock and the workers, against telemetry, the collector and the console together. The World
//! cannot host them: telemetry samples on blocking threads and the console serves real HTTP. So they run on real
//! threads, with a TCP proxy in front of each node that holds, slows or resets what crosses it.
#![cfg(feature = "dashboard")]
#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axon::{Actor, ActorSystem, Context, RuntimeConfig};
use axon_cluster::{AccessPolicy, Cluster, Discovery, MemoryDiscovery};
use axon_obs::*;
use axon_testkit::eventually;
use bytes::Bytes;
use parking_lot::Mutex;
use serde_json::Value as Json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, sleep};

mod common;

use common::Kept;

/// A TCP proxy at the address a node advertises, so every session with the node crosses it.
#[derive(Default)]
struct Proxy {
    node: Mutex<Option<SocketAddr>>,
    /// What peers send the node is held here, as is what it sends them.
    hold_in: AtomicBool,
    hold_out: AtomicBool,
    delay_ms: AtomicU64,
    /// Bumped to end every connection open.
    resets: tokio::sync::watch::Sender<u64>,
    /// New connections are closed as soon as they are taken, as by a process that went away.
    refuse: AtomicBool,
    /// New connections are given this answer to their first request instead, then closed.
    canned: Mutex<Option<Bytes>>,
}

impl Proxy {
    async fn bind() -> (Arc<Self>, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (proxy, addr) = (Arc::new(Self::default()), listener.local_addr().unwrap());
        let serving = proxy.clone();
        tokio::spawn(async move {
            while let Ok((peer, _)) = listener.accept().await {
                let proxy = serving.clone();
                tokio::spawn(async move {
                    if proxy.refuse.load(Relaxed) {
                        return;
                    }
                    let canned = proxy.canned.lock().clone();
                    if let Some(canned) = canned {
                        return answer(peer, &canned).await;
                    }
                    let Some(node) = *proxy.node.lock() else { return };
                    let Ok(node) = TcpStream::connect(node).await else { return };
                    let ((from_peer, to_peer), (from_node, to_node)) = (peer.into_split(), node.into_split());
                    tokio::join!(pump(from_peer, to_node, &proxy, true), pump(from_node, to_peer, &proxy, false));
                });
            }
        });
        (proxy, addr)
    }

    fn hold(&self, inbound: bool, outbound: bool) {
        self.hold_in.store(inbound, Relaxed);
        self.hold_out.store(outbound, Relaxed);
    }

    fn heal(&self) {
        self.hold(false, false);
        self.delay_ms.store(0, Relaxed);
    }

    fn reset(&self) {
        self.resets.send_modify(|n| *n += 1);
    }
}

/// Reads one HTTP request to its end, so closing after the answer resets nothing, and answers it.
async fn answer(mut peer: TcpStream, canned: &[u8]) {
    let (mut request, mut buf) = (Vec::new(), vec![0; 64 << 10]);
    loop {
        match peer.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
        }
        let Some(head) = request.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
        let length = (String::from_utf8_lossy(&request[..head]).lines())
            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
            .unwrap_or(0usize);
        if request.len() >= head + 4 + length {
            break;
        }
    }
    let _ = peer.write_all(canned).await;
}

async fn pump(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, proxy: &Proxy, inbound: bool) {
    let (mut buf, mut resets) = (vec![0; 64 << 10], proxy.resets.subscribe());
    let held = || if inbound { proxy.hold_in.load(Relaxed) } else { proxy.hold_out.load(Relaxed) };
    loop {
        let n = tokio::select! {
            read = from.read(&mut buf) => match read {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            },
            _ = resets.changed() => return,
        };
        let pass = async {
            while held() {
                sleep(Duration::from_millis(10)).await;
            }
            sleep(Duration::from_millis(proxy.delay_ms.load(Relaxed))).await;
        };
        tokio::select! {
            biased;
            _ = resets.changed() => return,
            () = pass => {}
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

struct Node {
    system: ActorSystem,
    cluster: Cluster,
    proxy: Arc<Proxy>,
}

/// Quick to notice silence, and quick to give up on a call.
async fn node(id: &str, discovery: &MemoryDiscovery) -> Node {
    node_found_by(id, discovery.clone()).await
}

async fn node_found_by(id: &str, discovery: impl Discovery + 'static) -> Node {
    let (proxy, advertised) = Proxy::bind().await;
    let system = ActorSystem::builder().runtime(RuntimeConfig { workers: 2, ..Default::default() });
    let (system, cluster) = common::loopback(id, discovery, system, |cfg| {
        // A silent peer is dropped within 2 s and calls give up in 500 ms, so each fault plays out in seconds.
        cfg.transport.keepalive_interval = Duration::from_millis(300);
        cfg.transport.idle_timeout = Duration::from_millis(1500);
        // A silent peer's redial gives up at a keepalive, so it is dropped within the 2 s too.
        cfg.transport.connect_timeout = Duration::from_millis(300);
        cfg.request_timeout = Duration::from_millis(500);
        cfg.advertise = Some(advertised);
    })
    .await;
    *proxy.node.lock() = Some(cluster.local_addr());
    Node { system, cluster, proxy }
}

fn telemetry() -> TelemetryConfig {
    TelemetryConfig {
        // Sampled every 50 ms, so samples come within the test.
        sample_interval: Duration::from_millis(50),
        retained_samples: 3600,
        access: AccessPolicy::Cluster,
        ..Default::default()
    }
}

/// A node that collects into `Kept` and serves the console, over the default refresh of a tenth of a second.
struct Console {
    sink: Arc<Kept>,
    service: MetricsService,
    base: String,
}

async fn console(m: &Node) -> Console {
    Telemetry::start(&m.system, &m.cluster, telemetry()).await.unwrap();
    let sink = Arc::new(Kept::default());
    let collect = MetricsConfig {
        // Collected every 200 ms within a 1 s deadline, so a held node shows within the test.
        collection_interval: Some(Duration::from_millis(200)),
        cycle_deadline: Duration::from_secs(1),
        access: AccessPolicy::Cluster,
        ..Default::default()
    };
    let query = Arc::new(MemoryMetrics::new(1 << 20));
    let sinks: Vec<Arc<dyn MetricsSink>> = vec![sink.clone()];
    let service = MetricsService::with_providers(&m.system, &m.cluster, sinks, query, collect).await.unwrap();
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        // Refreshed every 100 ms, so the page follows the fault within the test.
        refresh_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let dashboard = Dashboard::start(&m.system, &m.cluster, cfg).await.unwrap();
    Console { sink, service, base: format!("http://{}", dashboard.address()) }
}

impl Console {
    /// The overview, and how long it took to come.
    async fn overview(&self) -> (Json, Duration) {
        let asked = Instant::now();
        let response = reqwest::get(format!("{}/api/overview", self.base)).await.unwrap();
        assert_eq!(response.status(), 200);
        let overview = response.json().await.unwrap();
        (overview, asked.elapsed())
    }

    async fn shown(&self, node: &str) -> Option<Json> {
        let (overview, _) = self.overview().await;
        overview["nodes"].as_array().unwrap().iter().find(|n| n["node"]["id"] == node).cloned()
    }

    /// While a fault on a lasts: the console answers within its deadline and still lists a.
    async fn lists_a(&self, fault: impl std::fmt::Debug) {
        let (overview, took) = self.overview().await;
        assert!(took < Duration::from_secs(4), "{fault:?}: the console took {took:?}");
        assert!(overview["nodes"].as_array().unwrap().iter().any(|n| n["node"]["id"] == "a"), "{fault:?}: a is gone");
    }

    /// Once it ends: the console shows a's `newest` sample, and the collector stores up to it, each batch once.
    async fn caught_up(&self, a_boot: &str, newest: u64, fault: impl std::fmt::Debug) {
        eventually("the console shows a's newest sample", || async {
            self.shown("a").await.is_some_and(|n| {
                n["reachable"] == true && n["error"].is_null() && n["snapshot"]["sequence"].as_u64() >= Some(newest)
            })
        })
        .await;
        eventually("the collector stores a up to it", || async {
            self.sink.of("a", Some(a_boot)).iter().max() >= Some(&newest)
        })
        .await;
        self.sink.once_each("a", Some(a_boot));
        assert_eq!(self.service.health().collection_gaps, 0, "{fault:?}");
    }
}

fn boot(cluster: &Cluster) -> String {
    format!("{:032x}", cluster.identity().incarnation)
}

/// The newest batch a node's telemetry holds.
fn sequence(telemetry: &Telemetry) -> u64 {
    serde_json::from_slice::<Json>(&telemetry.snapshot()).unwrap()["sequence"].as_u64().unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    Partition,
    /// What m sends a is held.
    CutTo,
    CutFrom,
    /// 50 ms on every chunk each way.
    Slow,
    /// Every connection with a ends every 300 ms, often in the middle of a page, which is asked for again.
    Lossy,
    /// a's answers are held a second at a time, past the request timeout, then let go: pages come in after m asked
    /// again, behind its newer requests.
    Late,
}

/// m collects a and serves the console. While the fault lasts the console answers within its deadline and keeps a on
/// the page; once it ends, the console shows a's newest sample and the collector stores up to it, each batch once.
async fn ride_out(fault: Fault) {
    let discovery = MemoryDiscovery::new();
    let (m, a) = (node("m", &discovery).await, node("a", &discovery).await);
    eventually("m sees a", || async { m.cluster.view().get("a").is_some_and(|n| n.reachable) }).await;
    let of_a = Telemetry::start(&a.system, &a.cluster, telemetry()).await.unwrap();
    let console = console(&m).await;
    let a_boot = boot(&a.cluster);
    eventually("a is collected and shown", || async {
        console.sink.of("a", Some(&a_boot)).len() >= 5
            && console.shown("a").await.is_some_and(|n| n["snapshot"].is_object())
    })
    .await;

    let (into_a, into_m) = (&a.proxy, &m.proxy);
    match fault {
        Fault::Partition => [into_a, into_m].iter().for_each(|w| w.hold(true, true)),
        Fault::CutTo => {
            into_a.hold(true, false);
            into_m.hold(false, true);
        }
        Fault::CutFrom => {
            into_a.hold(false, true);
            into_m.hold(true, false);
        }
        Fault::Slow => [into_a, into_m].iter().for_each(|w| w.delay_ms.store(50, Relaxed)),
        Fault::Lossy | Fault::Late => {}
    }
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        match fault {
            Fault::Lossy => [into_a, into_m].iter().for_each(|w| w.reset()),
            Fault::Late => {
                let holding = started.elapsed().as_secs().is_multiple_of(2);
                into_a.hold(false, holding);
                into_m.hold(holding, false);
            }
            _ => {}
        }
        console.lists_a(fault).await;
        sleep(Duration::from_millis(300)).await;
    }
    into_a.heal();
    into_m.heal();
    console.caught_up(&a_boot, sequence(&of_a), fault).await;
    for n in [m, a] {
        n.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_between_the_console_and_a_node_holds_nothing_up_and_loses_nothing() {
    ride_out(Fault::Partition).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_one_way_cut_to_a_node_holds_nothing_up_and_loses_nothing() {
    ride_out(Fault::CutTo).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_one_way_cut_from_a_node_holds_nothing_up_and_loses_nothing() {
    ride_out(Fault::CutFrom).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_wire_to_a_node_holds_nothing_up_and_loses_nothing() {
    ride_out(Fault::Slow).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_that_keep_breaking_repeat_pages_but_store_each_batch_once() {
    ride_out(Fault::Lossy).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_that_come_after_the_next_request_store_each_batch_once() {
    ride_out(Fault::Late).await;
}

/// a dies without a goodbye: reads of it end within the request timeout, the console keeps answering and says so, and
/// a's next boot is shown and collected from its first batch.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_crashes_and_boots_again_is_shown_and_collected_from_its_new_boot() {
    let discovery = MemoryDiscovery::new();
    let (m, a) = (node("m", &discovery).await, node("a", &discovery).await);
    eventually("m sees a", || async { m.cluster.view().get("a").is_some_and(|n| n.reachable) }).await;
    Telemetry::start(&a.system, &a.cluster, telemetry()).await.unwrap();
    let console = console(&m).await;
    let first_boot = boot(&a.cluster);
    eventually("a is collected", || async { console.sink.of("a", Some(&first_boot)).len() >= 5 }).await;

    drop(a);
    let asked = Instant::now();
    assert!(fetch_samples(&m.cluster, "a", None).await.is_err());
    assert!(asked.elapsed() < Duration::from_secs(1), "a read of a dead node took {:?}", asked.elapsed());
    eventually("the console says a cannot be read", || async {
        let (overview, took) = console.overview().await;
        assert!(took < Duration::from_secs(4), "the console took {took:?}");
        let a = overview["nodes"].as_array().unwrap().iter().find(|n| n["node"]["id"] == "a").cloned();
        a.is_some_and(|a| a["reachable"] == false || !a["error"].is_null())
    })
    .await;

    let a = node("a", &discovery).await;
    Telemetry::start(&a.system, &a.cluster, telemetry()).await.unwrap();
    let second_boot = boot(&a.cluster);
    eventually("the console shows a's new boot", || async {
        console.shown("a").await.is_some_and(|n| n["snapshot"]["incarnation"] == second_boot.as_str())
    })
    .await;
    eventually("the collector stores it", || async { console.sink.of("a", Some(&second_boot)).len() >= 5 }).await;
    console.sink.once_each("a", Some(&first_boot));
    assert_eq!(console.sink.once_each("a", Some(&second_boot))[0], 1, "a new boot is read from its first batch");
    for n in [m, a] {
        n.system.shutdown().await.unwrap();
    }
}

/// Holds its worker for as long as it is told: asleep, as a handler that does not return, or spinning.
struct Hog {
    spin: bool,
}

impl Actor for Hog {
    type Message = Duration;
    type Error = ();

    fn on_message(&mut self, hold: Duration, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if self.spin {
            let until = std::time::Instant::now() + hold;
            while std::time::Instant::now() < until {
                std::hint::spin_loop();
            }
        } else {
            std::thread::sleep(hold);
        }
        Ok(())
    }
}

/// Every actor worker on both nodes held for 2.5 s: telemetry goes on sampling and answering, the console, the
/// probes and `/metrics` answer at once, and the collector goes on storing.
async fn held_workers(spin: bool) {
    let discovery = MemoryDiscovery::new();
    let (m, a) = (node("m", &discovery).await, node("a", &discovery).await);
    eventually("m sees a", || async { m.cluster.view().get("a").is_some_and(|n| n.reachable) }).await;
    let of_a = Telemetry::start(&a.system, &a.cluster, telemetry()).await.unwrap();
    let health = Health::start(&a.system, &a.cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&of_a);
    let console = console(&m).await;
    let a_boot = boot(&a.cluster);
    eventually("a is collected", || async { console.sink.of("a", Some(&a_boot)).len() >= 5 }).await;

    let hold = Duration::from_millis(2500);
    for n in [&m, &a] {
        for _ in 0..3 {
            n.system.spawn(Hog { spin }).start().unwrap().tell(hold).unwrap();
        }
    }
    sleep(Duration::from_millis(100)).await;
    let (sampled, stored) = (sequence(&of_a), console.sink.of("a", Some(&a_boot)).len());
    let (held, mut scraped) = (Instant::now(), Vec::new());
    while held.elapsed() < hold - Duration::from_millis(300) {
        let asked = Instant::now();
        let ready = reqwest::get(format!("http://{}/readyz", health.address())).await.unwrap().status().as_u16();
        assert_eq!((ready, asked.elapsed() < Duration::from_millis(300)), (200, true), "{:?}", asked.elapsed());
        let asked = Instant::now();
        let scrape = reqwest::get(format!("http://{}/metrics", health.address())).await.unwrap();
        assert_eq!(scrape.status(), 200);
        scraped.push(scrape.text().await.unwrap());
        assert!(asked.elapsed() < Duration::from_millis(300), "a scrape took {:?}", asked.elapsed());
        let (_, took) = console.overview().await;
        assert!(took < Duration::from_secs(1), "the console took {took:?}");
        let asked = Instant::now();
        fetch_samples(&m.cluster, "a", None).await.unwrap();
        assert!(asked.elapsed() < Duration::from_millis(500), "a read of a took {:?}", asked.elapsed());
        sleep(Duration::from_millis(200)).await;
    }
    assert!(sequence(&of_a) >= sampled + 20, "telemetry sampled {} times in 2 s", sequence(&of_a) - sampled);
    assert_ne!(scraped.first(), scraped.last(), "scrapes follow the samples through the hold");
    assert!(console.sink.of("a", Some(&a_boot)).len() >= stored + 20, "the collector stopped");
    console.sink.once_each("a", Some(&a_boot));
    for n in [m, a] {
        n.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_held_by_handlers_that_do_not_return_stop_no_telemetry_probe_console_or_collector() {
    held_workers(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_spinning_in_their_handlers_stop_no_telemetry_probe_console_or_collector() {
    held_workers(true).await;
}

/// On Linux, where `/proc` shows when every thread of a process has stopped.
#[cfg(target_os = "linux")]
mod sigstop {
    use std::io::{BufRead, BufReader, Read};
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    use axon_cluster::{NodeRecord, StaticDiscovery};

    use super::*;

    /// Set in the process `a_node_stopped_by_sigstop…` runs node a in: the address of the node a joins.
    const STOPPED_CHILD: &str = "AXON_STOPPED_CHILD";

    /// Kills its process, stopped or not, however the test ends.
    struct Child(std::process::Child);

    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Node a, with telemetry and probes, joined to `m`. It says where it serves on stdout and runs until stdin closes.
    async fn stopped_child(m: SocketAddr) {
        let seed = NodeRecord { id: "m".into(), incarnation: 0, address: m, metadata: Default::default(), version: 0 };
        let a = node_found_by("a", StaticDiscovery::new(vec![seed]).unwrap()).await;
        Telemetry::start(&a.system, &a.cluster, telemetry()).await.unwrap();
        let health = Health::start(&a.system, &a.cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
        println!("{STOPPED_CHILD} {} {} {}", a.cluster.record().address, health.address(), boot(&a.cluster));
        let _ = tokio::task::spawn_blocking(|| std::io::stdin().read_to_end(&mut Vec::new())).await;
        a.system.shutdown().await.unwrap();
    }

    /// A SIGSTOP holds every thread of a process, so node a runs in a child: this test again, told so by
    /// `STOPPED_CHILD`. Stopped for 3 s, past the idle timeout, the console keeps answering; once continued, a probe
    /// asked meanwhile is answered at once, a is ready again, and its telemetry samples on and is collected, each batch
    /// once, with no gap.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_node_stopped_by_sigstop_answers_its_probes_and_is_sampled_and_collected_once_it_continues() {
        if let Ok(m) = std::env::var(STOPPED_CHILD) {
            return stopped_child(m.parse().unwrap()).await;
        }
        let discovery = MemoryDiscovery::new();
        let m = node("m", &discovery).await;
        let console = console(&m).await;
        let name =
            "sigstop::a_node_stopped_by_sigstop_answers_its_probes_and_is_sampled_and_collected_once_it_continues";
        let mut child = Child(
            Command::new(std::env::current_exe().unwrap())
                .args([name, "--exact", "--nocapture"])
                .env(STOPPED_CHILD, m.cluster.record().address.to_string())
                // In a group of its own: the kernel continues every member of a group that a process leaves orphaned, as
                // the group of a test run under nohup setsid always is.
                .process_group(0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stdout = BufReader::new(child.0.stdout.take().unwrap());
        let line = tokio::task::spawn_blocking(move || {
            stdout.lines().map_while(Result::ok).find(|l| l.starts_with(STOPPED_CHILD))
        })
        .await
        .unwrap()
        .expect("a says where it serves");
        let [_, address, health, a_boot] = line.split(' ').collect::<Vec<_>>()[..] else { panic!("{line}") };
        let (address, a_boot) = (address.parse().unwrap(), a_boot.to_string());
        let record = NodeRecord { id: "a".into(), incarnation: 0, address, metadata: Default::default(), version: 0 };
        discovery.register(&record).await.unwrap();
        let probe = |path: &str| {
            let url = format!("http://{health}/{path}");
            async move { reqwest::Client::new().get(url).timeout(Duration::from_secs(10)).send().await.unwrap().status() }
        };
        let pid = child.0.id().to_string();
        let signal =
            |signal: &str| assert!(Command::new("kill").args([signal, pid.as_str()]).status().unwrap().success());
        eventually("a is collected and shown", || async {
            console.sink.of("a", Some(&a_boot)).len() >= 5
                && console.shown("a").await.is_some_and(|n| n["snapshot"].is_object())
        })
        .await;
        assert_eq!(probe("readyz").await, 200);
        let sampled = |snapshot: Json| snapshot["sequence"].as_u64().unwrap();
        let before = sampled(fetch(&m.cluster, "a", &Request::Snapshot).await.unwrap());

        signal("-STOP");
        // The stop takes each thread only as it next enters the kernel: until every one has, a probe can still be answered.
        eventually("every thread of a is stopped", || async {
            let stopped = |task: std::fs::DirEntry| {
                let stat = std::fs::read_to_string(task.path().join("stat")).unwrap_or_default();
                stat.rsplit_once(") ").is_some_and(|(_, rest)| rest.starts_with('T'))
            };
            std::fs::read_dir(format!("/proc/{pid}/task")).unwrap().flatten().all(stopped)
        })
        .await;
        let asked = tokio::spawn(probe("healthz"));
        let stopped = Instant::now();
        while stopped.elapsed() < Duration::from_secs(3) {
            console.lists_a("SIGSTOP").await;
            sleep(Duration::from_millis(300)).await;
        }
        assert!(!asked.is_finished(), "a stopped process answered its probe");
        signal("-CONT");
        let continued = Instant::now();
        assert_eq!(asked.await.unwrap(), 200);
        assert!(
            continued.elapsed() < Duration::from_millis(500),
            "the probe was answered {:?} after",
            continued.elapsed()
        );
        eventually("a is ready again", || async { probe("readyz").await == 200 }).await;

        let newest = std::cell::Cell::new(0);
        eventually("a samples on", || async {
            newest.set(fetch(&m.cluster, "a", &Request::Snapshot).await.map_or(0, sampled));
            newest.get() > before + 20
        })
        .await;
        console.caught_up(&a_boot, newest.get(), "SIGSTOP").await;
        drop(child);
        m.system.shutdown().await.unwrap();
    }
}

fn system_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

/// A runtime whose threads, this one included, read the wall clock `wall` makes of the system's, in µs.
fn with_wall(wall: impl Fn(u64) -> u64 + Clone + Send + Sync + 'static) -> tokio::runtime::Runtime {
    let set = move || {
        let wall = wall.clone();
        axon::trace::simulate_wall(Some(Rc::new(move || wall(system_micros()))));
    };
    set();
    tokio::runtime::Builder::new_multi_thread().enable_all().on_thread_start(set).build().unwrap()
}

/// A runtime whose threads, this one included, read a wall clock `step` µs off the system's.
fn stepped(step: &Arc<AtomicI64>) -> tokio::runtime::Runtime {
    let step = step.clone();
    with_wall(move |now| now.saturating_add_signed(step.load(Relaxed)))
}

const TEN_MINUTES_US: i64 = 600_000_000;

/// A series that holds its value is stored again when it is due, whichever way the wall clock stepped: not only once
/// the wall catches up after a step back, and not a full wait after a step forward. A store would take it for ended.
#[test]
fn an_unchanged_series_is_stored_again_when_due_whichever_way_the_wall_clock_steps() {
    for by in [-TEN_MINUTES_US, TEN_MINUTES_US] {
        let step = Arc::new(AtomicI64::new(0));
        stepped(&step).block_on(async {
            let a = node("a", &MemoryDiscovery::new()).await;
            let every = Duration::from_millis(300);
            let cfg =
                // Sampled every 20 ms and rewritten every 300 ms, so the stepped wall shows within the test.
                TelemetryConfig { sample_interval: Duration::from_millis(20), rewrite_unchanged: every, ..telemetry() };
            Telemetry::start(&a.system, &a.cluster, cfg).await.unwrap();
            let (mut batches, mut after, mut from) = (Vec::new(), None, None);
            let started = Instant::now();
            while started.elapsed() < every * 6 {
                if from.is_none() && started.elapsed() >= every * 2 {
                    step.store(by, Relaxed);
                    from = Some(batches.len());
                }
                let page = fetch_samples(&a.cluster, "a", after.take()).await.unwrap();
                after = Some(page.next);
                batches.extend(page.batches);
                sleep(Duration::from_millis(50)).await;
            }
            let carries = |b: &&SampleBatch| b.samples.iter().any(|s| s.name == "runtime.actors_failed");
            let mut last = None;
            for batch in &batches {
                if carries(&batch) {
                    last = Some(batch.timestamp_ms);
                } else if let Some(last) = last {
                    let since = batch.timestamp_ms.saturating_sub(last);
                    assert!(since < every.as_millis() as u64, "step {by} µs: a batch {since} ms after the last carry");
                }
            }
            let stored = batches[from.unwrap()..].iter().filter(carries).count();
            assert!(stored >= 2, "step {by} µs: stored {stored} times in {} batches after it", batches.len());
            a.system.shutdown().await.unwrap();
        });
    }
}

/// Stores judge a series' gaps on the batches' stamps, so an unchanged series comes again once they are
/// `rewrite_unchanged` apart and no sooner, on a wall clock that runs 2 % slower than the monotonic one as well.
#[test]
fn an_unchanged_series_is_stored_again_once_the_stamps_say_it_is_due_on_a_wall_clock_that_runs_slow() {
    let start = system_micros();
    with_wall(move |now| start + (now - start) * 49 / 50).block_on(async {
        let a = node("a", &MemoryDiscovery::new()).await;
        let every = Duration::from_millis(300);
        let cfg =
            // Sampled every 20 ms and rewritten every 300 ms, so the slow wall shows within the test.
            TelemetryConfig { sample_interval: Duration::from_millis(20), rewrite_unchanged: every, ..telemetry() };
        Telemetry::start(&a.system, &a.cluster, cfg).await.unwrap();
        sleep(every * 6).await;
        let (mut carried, mut after) = (Vec::new(), None);
        loop {
            let page = fetch_samples(&a.cluster, "a", after).await.unwrap();
            let carries = |b: &&SampleBatch| b.samples.iter().any(|s| s.name == "runtime.actors_failed");
            carried.extend(page.batches.iter().filter(carries).map(|b| b.timestamp_ms));
            if page.batches.is_empty() {
                break;
            }
            after = Some(page.next);
        }
        let every = every.as_millis() as u64;
        assert!(carried.len() >= 4 && carried.windows(2).all(|w| w[1] - w[0] >= every), "{carried:?}");
        a.system.shutdown().await.unwrap();
    });
}

/// Nobody has looked for longer than the console waits before it goes idle, so it stops asking the nodes, however
/// the wall clock moved meanwhile.
#[test]
fn a_console_nobody_watches_goes_idle_after_the_wall_clock_steps_back() {
    let step = Arc::new(AtomicI64::new(0));
    stepped(&step).block_on(async {
        let a = node("a", &MemoryDiscovery::new()).await;
        let asked = Arc::new(AtomicU64::new(0));
        let counting = asked.clone();
        a.cluster
            .serve_control(TELEMETRY_SERVICE, AccessPolicy::Cluster, move |_| {
                counting.fetch_add(1, Relaxed);
                async { Ok(Bytes::from_static(b"{}")) }
            })
            .unwrap();
        let cfg = DashboardConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            // Refreshed every 100 ms, so the page follows the fault within the test.
            refresh_interval: Duration::from_millis(100),
            ..Default::default()
        };
        let dashboard = Dashboard::start(&a.system, &a.cluster, cfg).await.unwrap();
        dashboard.overview().await;
        step.store(-TEN_MINUTES_US, Relaxed);
        // The console goes idle 15 s after the last look.
        sleep(Duration::from_secs(17)).await;
        let before = asked.load(Relaxed);
        sleep(Duration::from_secs(1)).await;
        assert_eq!(asked.load(Relaxed), before, "the console still asks the nodes for nobody");
        a.system.shutdown().await.unwrap();
    });
}

/// The collector's cursors are positions, not times: a wall clock stepped back and then forward loses and repeats
/// nothing.
#[test]
fn a_collector_stores_each_batch_once_while_the_wall_clock_steps_back_and_forward() {
    let step = Arc::new(AtomicI64::new(0));
    stepped(&step).block_on(async {
        let discovery = MemoryDiscovery::new();
        let (m, a) = (node("m", &discovery).await, node("a", &discovery).await);
        eventually("m sees a", || async { m.cluster.view().get("a").is_some_and(|n| n.reachable) }).await;
        Telemetry::start(&a.system, &a.cluster, telemetry()).await.unwrap();
        let console = console(&m).await;
        let a_boot = boot(&a.cluster);
        for to in [0, -TEN_MINUTES_US, TEN_MINUTES_US] {
            step.store(to, Relaxed);
            let stored = console.sink.of("a", Some(&a_boot)).len();
            eventually("the collector goes on", || async { console.sink.of("a", Some(&a_boot)).len() >= stored + 10 })
                .await;
        }
        console.sink.once_each("a", Some(&a_boot));
        let health = console.service.health();
        assert_eq!((health.collection_gaps, health.source_resets, health.write_errors), (0, 0, 0), "{health:?}");
        for n in [m, a] {
            n.system.shutdown().await.unwrap();
        }
    });
}

/// History is served by the collector that collected last, by each one's own clock: `ahead`, whose wall runs ten
/// minutes ahead, collects once and stops, and does not serve it for as long as its wall stays ahead.
#[test]
fn history_is_served_by_the_collector_that_collected_last_whatever_their_walls_say() {
    let discovery = MemoryDiscovery::new();
    let collector =
        // Each collector's interval is the test's to choose: one collects once, the other every 100 ms.
        |every| MetricsConfig { collection_interval: Some(every), access: AccessPolicy::Cluster, ..Default::default() };
    let store = || Arc::new(MemoryMetrics::new(1 << 20));
    let ahead_runtime = stepped(&Arc::new(AtomicI64::new(TEN_MINUTES_US)));
    let ahead = ahead_runtime.block_on(async {
        let ahead = node("ahead", &discovery).await;
        let cfg = collector(Duration::from_secs(3600));
        MetricsService::start(&ahead.system, &ahead.cluster, store(), cfg).await.unwrap();
        ahead
    });
    axon::trace::simulate_wall(None);
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
        sleep(Duration::from_millis(500)).await;
        let m = node("m", &discovery).await;
        MetricsService::start(&m.system, &m.cluster, store(), collector(Duration::from_millis(100))).await.unwrap();
        let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
        let dashboard = Dashboard::start(&m.system, &m.cluster, cfg).await.unwrap();
        // Sources are probed every 5 s.
        let until = Instant::now() + Duration::from_secs(12);
        loop {
            let history = serde_json::from_slice::<Json>(&dashboard.overview().await).unwrap()["history"].clone();
            let sources = history["sources"].as_array().cloned().unwrap_or_default();
            if sources.len() == 2 && sources.iter().all(|s| s["collector"]["cycles"].as_u64() >= Some(1)) {
                assert_eq!(history["node"], "m", "{history:#}");
                break;
            }
            assert!(Instant::now() < until, "both collectors never showed: {history:#}");
            sleep(Duration::from_millis(200)).await;
        }
        m.system.shutdown().await.unwrap();
    });
    ahead_runtime.block_on(ahead.system.shutdown()).unwrap();
}

fn http(status: &str, body: &[u8]) -> Bytes {
    let head = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    [head.as_bytes(), body].concat().into()
}

/// A ClickHouse behind the proxy, and the same one straight, to see what it holds. Each fault ends with the batch written
/// again, as the collector does when it cannot tell whether a write was stored: it is stored once, whatever the fault
/// did to the first write.
#[cfg(feature = "clickhouse")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs AXON_CLICKHOUSE: scripts/check.sh live"]
async fn the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path() {
    let url =
        std::env::var("AXON_CLICKHOUSE").expect("AXON_CLICKHOUSE is set for the live tests: scripts/check.sh live");
    let (username, password) = (
        std::env::var("AXON_CLICKHOUSE_USER").unwrap_or("default".into()),
        std::env::var("AXON_CLICKHOUSE_PASSWORD").unwrap_or_default(),
    );
    let config = |url: String| ClickHouseConfig {
        url,
        database: "axon_faults".into(),
        username: username.clone(),
        password: password.clone(),
        retention_days: 1,
        rollup_retention_days: 2,
    };
    let (proxy, proxied) = Proxy::bind().await;
    let server = tokio::net::lookup_host(url.trim_start_matches("http://").trim_end_matches('/')).await.unwrap();
    *proxy.node.lock() = server.into_iter().next();
    let direct = ClickHouseProvider::connect(config(url.clone())).await.unwrap();
    let sink = ClickHouseProvider::connect(config(format!("http://{proxied}"))).await.unwrap();

    let now = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let (node, incarnation) = (format!("f{}", now()), "00ff");
    let batch = |sequence: u64, timestamp_ms: u64| SampleBatch {
        node_id: node.clone(),
        incarnation: incarnation.into(),
        sequence,
        timestamp_ms,
        samples: vec![Sample {
            name: "t.faults".into(),
            unit: "things".into(),
            labels: Default::default(),
            value: Value::Counter(sequence),
        }],
    };
    let http_client = reqwest::Client::new();
    let rows = || async {
        let sql = format!("SELECT count() FROM axon_faults.samples WHERE node_id = '{node}'");
        let asked = http_client.post(&url).basic_auth(&username, Some(&password)).body(sql).send().await.unwrap();
        asked.text().await.unwrap().trim().parse::<u64>().unwrap()
    };

    let mut sequence = 0;
    for fault in [
        "crash",
        "restart",
        "pause",
        "partition",
        "one-way cut",
        "loss early",
        "loss late",
        "slow",
        "disk full",
        "disk error",
        "overload",
    ] {
        sequence += 1;
        let written = [batch(sequence, now())];
        // Nothing rides on a connection from before the fault.
        proxy.reset();
        match fault {
            "crash" => *proxy.canned.lock() = Some(http("200 OK", b"").slice(..30)),
            "restart" => proxy.refuse.store(true, Relaxed),
            // A paused server takes the connection and the request, and does nothing with them until it continues;
            // across a partition the request never arrives.
            "pause" | "partition" => proxy.hold(true, true),
            "one-way cut" => proxy.hold(false, true),
            "loss early" | "loss late" => proxy.delay_ms.store(300, Relaxed),
            "slow" => proxy.delay_ms.store(500, Relaxed),
            "disk full" => {
                let text = b"Code: 243. DB::Exception: Cannot reserve 1.00 MiB, not enough space. (NOT_ENOUGH_SPACE)";
                *proxy.canned.lock() = Some(http("507 Insufficient Storage", text));
            }
            "disk error" => {
                let text = b"Code: 74. DB::Exception: Cannot read from file: Input/output error. (CANNOT_READ_FROM_FILE_DESCRIPTOR)";
                *proxy.canned.lock() = Some(http("500 Internal Server Error", text));
            }
            "overload" => {
                let text = b"Code: 202. DB::Exception: Too many simultaneous queries. (TOO_MANY_SIMULTANEOUS_QUERIES)";
                *proxy.canned.lock() = Some(http("503 Service Unavailable", text));
            }
            other => unreachable!("{other}"),
        }
        let (write, asked) = (sink.write(&written), Instant::now());
        let result = if fault.starts_with("loss") {
            let reset = if fault == "loss early" { 100 } else { 450 };
            let proxy = proxy.clone();
            let resetting = tokio::spawn(async move {
                sleep(Duration::from_millis(reset)).await;
                proxy.reset();
            });
            let written = write.await;
            resetting.await.unwrap();
            written
        } else {
            write.await
        };
        let took = asked.elapsed();
        match fault {
            "slow" => assert!(result.is_ok() && took >= Duration::from_millis(500), "{fault}: {result:?} in {took:?}"),
            "disk full" | "disk error" | "overload" => {
                let error = result.unwrap_err();
                let code = ["NOT_ENOUGH_SPACE", "CANNOT_READ_FROM_FILE", "TOO_MANY_SIMULTANEOUS"];
                assert!(
                    error.starts_with("ClickHouse 5") && code.iter().any(|c| error.contains(c)),
                    "{fault}: {error}"
                );
            }
            "crash" | "restart" | "pause" | "partition" | "one-way cut" | "loss early" | "loss late" => {
                assert!(result.is_err(), "{fault}: {result:?}")
            }
            other => unreachable!("{other}"),
        }
        assert!(took < Duration::from_millis(3500), "{fault}: a write took {took:?}");
        if fault == "partition" {
            proxy.reset();
        }
        proxy.heal();
        proxy.refuse.store(false, Relaxed);
        *proxy.canned.lock() = None;
        if fault == "pause" {
            eventually("it runs the write the sink gave up on", || async { rows().await == sequence }).await;
        }
        sink.write(&written).await.unwrap_or_else(|e| panic!("{fault}: {e}"));
        assert_eq!(rows().await, sequence, "{fault}: stored twice or never");
        assert_eq!(direct.stored_through(&node, incarnation).await.unwrap(), Some(sequence), "{fault}");
    }

    // Out of order, and stamped a minute either side of now.
    let (later, sooner) = (batch(sequence + 2, now() + 60_000), batch(sequence + 1, now() - 60_000));
    sink.write(&[later]).await.unwrap();
    sink.write(&[sooner]).await.unwrap();
    assert_eq!(rows().await, sequence + 2);
    assert_eq!(direct.stored_through(&node, incarnation).await.unwrap(), Some(sequence + 2));

    // A reply past what a reader holds, and replies that are not rows.
    *proxy.canned.lock() = Some(http("200 OK", &vec![b'{'; 17 << 20]));
    proxy.reset();
    assert_eq!(sink.stored_through(&node, incarnation).await.unwrap_err(), "ClickHouse: response exceeds 16 MiB");
    for garbage in [&b"{\"through\": \"many\"}\n"[..], b"\xff\xfe not json", b"[1, 2]\n"] {
        *proxy.canned.lock() = Some(http("200 OK", garbage));
        proxy.reset();
        let error = sink.stored_through(&node, incarnation).await.unwrap_err();
        assert!(error.starts_with("ClickHouse row"), "{error}");
        let error = sink.events(&EventQuery { to_ms: now(), limit: 10, ..Default::default() }).await.unwrap_err();
        assert!(error.starts_with("ClickHouse row"), "{error}");
    }
    *proxy.canned.lock() = None;
    assert_eq!(sink.stored_through(&node, incarnation).await.unwrap(), Some(sequence + 2));
}
