//! Streams over real round trips: an origin and a child, each a process in a network namespace with netem and a
//! 1 Gbit/s link between them, both Paxos acceptors each holding a key. The origin keeps a backlog in each of its
//! streams, one or many, as fast as its branches drain; each node probes a plain request to the other and times its
//! lease renewals meanwhile. `tcp-recv` and `tcp-send` measure one TCP connection's ceiling between the same
//! namespaces. `scripts/relay-wan.sh` sets up the namespaces and runs the matrix; one process by hand:
//! `relay_wan node <w1|w2> <secs> <max_frame MiB> <streams> <dir> [shipped] [bulk]`, `relay_wan tcp-recv <addr> <secs>`,
//! `relay_wan tcp-send <addr> <secs>`. Each prints one line of `name=value` pairs.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::files::OsFiles;
use axon::{Actor, ActorSystem, Context, Lane};
use axon_cluster::{
    ActorProtocol, BoxFuture, CallError, Cluster, ClusterConfig, ClusterError, Holder, OwnershipAuthority,
    OwnershipError, PaxosConfig, PaxosLeases, Placement, ProviderInfo, Rejection, Reply, SeedConfig, SeedDiscovery,
    SeedServer, SeedServerConfig, Unaddressed, WireProtocol,
};
use axon_net::SharedSecret;
use axon_stream::{
    BranchLimits, DeliveryPolicy, RelayStats, StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions,
};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, sleep, sleep_until};

/// w1 is the origin and the seed; w2 follows its streams. Both are acceptors.
const NODES: [&str; 2] = ["w1", "w2"];
/// A push carries at most `hub.max_batch_records` (4096) records of a stream: 8 MiB pushes need records of 2 KiB or
/// more.
const RECORD: usize = 4096;
/// A push carries whole publishes: at 64 a 1 MiB push held three (768 KiB), at 16 fifteen (960 KiB).
const BATCH: usize = 16;
static PAYLOAD: [u8; RECORD] = [7; RECORD];
/// From the streams' first record to the start of the measured window: slow start, the window's growth.
const WARM: Duration = Duration::from_secs(3);
/// How long the origin publishes past its window, so the leaf's, which starts later, has a backlog to its end.
const TAIL: Duration = Duration::from_secs(3);
const PROBE_EVERY: Duration = Duration::from_millis(50);
const SAMPLE_EVERY: Duration = Duration::from_millis(50);

fn addr(id: &str, port: u16) -> std::net::SocketAddr {
    format!("10.78.0.{}:{port}", &id[1..]).parse().expect("w1..w9")
}

/// What the child's subscriber to one stream saw.
#[derive(Clone, Default)]
struct Seen {
    records: u64,
    gaps: u64,
    resets: u64,
    last: u64,
    reasons: BTreeMap<String, u64>,
}

impl Seen {
    fn take(&mut self, event: Option<StreamEvent>) {
        match event {
            Some(StreamEvent::Batch(batch)) => {
                self.gaps += u64::from(self.last != 0 && batch.first() != self.last + 1);
                self.last = batch.last();
                self.records += batch.len() as u64;
            }
            Some(event) if !event.is_terminal() => {}
            ended => {
                // The next subscription starts at the live tail: no gap to count against this one.
                self.last = 0;
                self.resets += 1;
                *self.reasons.entry(format!("{ended:?}").replace(' ', "")).or_default() += 1;
            }
        }
    }

    /// Every stream's, added up.
    fn sum(all: &[Seen]) -> Seen {
        let mut sum = Seen::default();
        for s in all {
            sum.records += s.records;
            sum.gaps += s.gaps;
            sum.resets += s.resets;
            for (reason, n) in &s.reasons {
                *sum.reasons.entry(reason.clone()).or_default() += n;
            }
        }
        sum
    }
}

struct Echo;

impl Actor for Echo {
    type Message = (Bytes, Option<Reply>);
    type Error = ();

    fn on_message(&mut self, (body, reply): Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
        drop(reply.map(|r| r.send(body)));
        Ok(())
    }
}

struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = Bytes;
    type Reply = Bytes;
    const ID: u64 = 0xE1;
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

/// Each renewal's start, how long it took, and how it ended; one dropped unanswered counts as failed.
type Renewals = Arc<Mutex<Vec<(Instant, Duration, Outcome)>>>;

#[derive(Clone, Copy, PartialEq)]
enum Outcome {
    Renewed,
    Failed,
    /// Refused at once: the last one is still out.
    Busy,
}

/// `PaxosLeases`, timing each renewal the cluster asks of it.
struct Timed(PaxosLeases, Renewals);

struct Renewal<'a>(&'a Renewals, Instant, Outcome);

impl Drop for Renewal<'_> {
    fn drop(&mut self) {
        self.0.lock().expect("not poisoned").push((self.1, self.1.elapsed(), self.2));
    }
}

impl OwnershipAuthority for Timed {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        self.0.acquire(key)
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        Box::pin(async move {
            let mut renewal = Renewal(&self.1, Instant::now(), Outcome::Failed);
            let renewed = self.0.renew().await;
            renewal.2 = match &renewed {
                Ok(failed) if failed.is_empty() => Outcome::Renewed,
                Err(OwnershipError::Unavailable(why)) if why == "a renewal is still out" => Outcome::Busy,
                _ => Outcome::Failed,
            };
            renewed
        })
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        self.0.release(key, epoch)
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        self.0.close()
    }

    fn describe(&self) -> ProviderInfo {
        self.0.describe()
    }

    fn held_by(
        &self,
        holder: Holder<'_>,
        kinds: &[String],
    ) -> Option<BoxFuture<'_, Result<Vec<String>, OwnershipError>>> {
        self.0.held_by(holder, kinds)
    }

    fn hold(&self) -> Option<Duration> {
        self.0.hold()
    }

    fn can_grant(&self) -> bool {
        self.0.can_grant()
    }

    fn warming(&self) -> bool {
        OwnershipAuthority::warming(&self.0)
    }

    fn attach<'a>(&'a self, cluster: &'a Cluster) -> BoxFuture<'a, Result<(), ClusterError>> {
        self.0.attach(cluster)
    }
}

/// `p50/p99/max` in ms, and the count.
fn spread(out: &mut String, name: &str, mut ms: Vec<f64>) {
    ms.sort_by(f64::total_cmp);
    let at = |p: f64| ms.get(((ms.len().max(1) - 1) as f64 * p) as usize).copied().unwrap_or(f64::NAN);
    let _ = write!(
        out,
        " {name}_n={} {name}_p50={:.1} {name}_p99={:.1} {name}_max={:.1}",
        ms.len(),
        at(0.5),
        at(0.99),
        at(1.0)
    );
}

/// User and system CPU of this process, in seconds (100 ticks a second).
fn cpu_secs() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<&str> = stat.rsplit_once(')').map_or("", |(_, rest)| rest).split_whitespace().collect();
    let tick = |i: usize| fields.get(i).and_then(|f| f.parse::<f64>().ok()).unwrap_or(0.0);
    (tick(11) + tick(12)) / 100.0
}

/// Plain requests to `peer`'s echo, one at a time every `PROBE_EVERY`, from `from` until `until`.
async fn probe(cluster: Cluster, peer: &'static str, from: Instant, until: Instant) -> (Vec<f64>, u64) {
    let remote = cluster.resolve_on::<EchoApi>(peer, "echo").await.expect("the peer publishes echo");
    let (body, mut ms, mut failed) = (Bytes::from(vec![7u8; 256]), Vec::new(), 0);
    sleep_until(from).await;
    let mut next = from;
    while next < until {
        let start = Instant::now();
        match remote.request(body.clone()).await {
            Ok(_) => ms.push(start.elapsed().as_secs_f64() * 1e3),
            Err(_) => failed += 1,
        }
        next = (next + PROBE_EVERY).max(Instant::now());
        sleep_until(next).await;
    }
    (ms, failed)
}

/// Samples, every `SAMPLE_EVERY` in the window, in which this node was isolated and in which a peer was not
/// available; and the means of the child's push window, what was in flight to it and the round trip, at the origin.
async fn watch(cluster: Cluster, stream: StreamNode, from: Instant, until: Instant) -> String {
    sleep_until(from).await;
    // Samples, and those with a round trip: one not worked out yet is not a round trip of 0.
    let (mut isolated, mut unavailable, mut n, mut window) = (0, 0, [0u32; 2], [0.0; 4]);
    while Instant::now() < until {
        isolated += u64::from(cluster.isolated());
        let view = cluster.view();
        unavailable += u64::from(view.members.len() < NODES.len() || !view.members.iter().all(|m| m.available()));
        for d in stream.stats().downstream.iter().filter(|d| d.branches > 0) {
            for (sum, x) in window.iter_mut().zip([d.window_bytes, d.inflight_bytes, d.inflight_pushes]) {
                *sum += x as f64;
            }
            n[0] += 1;
            if let Some(us) = d.round_trip_us {
                window[3] += us as f64 / 1e3;
                n[1] += 1;
            }
        }
        sleep(SAMPLE_EVERY).await;
    }
    let mean = |i: usize| window[i] / f64::from(n[usize::from(i == 3)].max(1));
    format!(
        " isolated_samples={isolated} unavailable_samples={unavailable} window_kib={:.0} inflight_kib={:.0} \
         inflight_pushes={:.1} rtt_ms={:.1}",
        mean(0) / 1024.0,
        mean(1) / 1024.0,
        mean(2),
        mean(3)
    )
}

/// What the origin's branches to the child hold, queued and in flight; `None` until it has a branch of every stream,
/// as one without would run ahead of its ring.
fn held(s: &RelayStats, streams: usize) -> Option<usize> {
    let d = &s.downstream;
    (d.iter().map(|d| d.branches).sum::<usize>() >= streams)
        .then(|| d.iter().map(|d| d.queued_bytes + d.inflight_bytes).sum())
}

async fn node(id: &'static str, secs: u64, frame_mib: usize, streams: usize, dir: &str, shipped: bool, bulk: bool) {
    let (window, origin) = (Duration::from_secs(secs), id == "w1");
    let lane = if bulk { Lane::Bulk } else { Lane::Live };
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"relay-wan-secret-0123").unwrap());
    transport.allow_plaintext = true;
    if frame_mib > 1 {
        // Interests on Control keep within its frame.
        let limits = &mut transport.lanes.get_mut(lane).limits;
        limits.max_frame = limits.max_frame.max(frame_mib << 20);
        // Each queue must hold one maximal frame: two of them, and never less than the lane's default.
        limits.outbound_bytes = limits.outbound_bytes.max(2 * limits.max_frame);
        limits.inbound_bytes = limits.inbound_bytes.max(2 * limits.max_frame);
    }
    let _seed = if origin {
        let mut seed = transport.clone();
        seed.identity.node_id = "w1-seed".into();
        Some(SeedServer::bind(SeedServerConfig::new(addr(id, 7001), &seed)).await.expect("seed binds"))
    } else {
        None
    };
    let discovery = SeedDiscovery::start(SeedConfig::new([addr("w1", 7001).to_string()], &transport)).await.unwrap();
    let mut paxos = PaxosConfig::new(NODES);
    paxos.store = Some(Arc::new(OsFiles::new(format!("{dir}/leases-{id}"))));
    let renewals = Renewals::default();
    let mut cfg = ClusterConfig::new(addr(id, 7400), transport, discovery);
    // With seeds, as guide/cluster.md § Seed servers asks.
    (cfg.heartbeat_interval, cfg.heartbeat_grace) = (Duration::from_millis(500), Duration::from_secs(1));
    cfg.labels.insert("node".into(), id.into());
    cfg.ownership = Some(Arc::new(Timed(PaxosLeases::new(paxos).unwrap(), renewals.clone())));
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let echo = system.spawn(Echo).start().unwrap();
    cluster.publish::<EchoApi>(&echo, "echo").unwrap();
    cluster.kind::<EchoApi>("hold").register(|_, _| Ok(Echo)).await.unwrap();
    cluster.wait_view(|v| v.members.iter().filter(|m| m.record.has_kind("hold")).count() == NODES.len()).await.unwrap();

    let mut scfg =
        StreamNodeConfig::new(if origin { vec![] } else { vec![BTreeMap::from([("node".into(), "w1".into())])] })
            .lane("wan", lane);
    scfg.core.max_push_bytes = (frame_mib << 20) - 64; // the default at 1
    // Publish while the child's branches hold under three quarters of their bounds, and under half the hub's total:
    // a branch over its bound resets its child.
    let high = (streams * scfg.core.branch.max_bytes * 3 / 4).min(scfg.core.hub.max_queued_bytes / 2);
    let max_push = scfg.core.max_push_bytes;
    let stream = StreamNode::start(&cluster, scfg).await.unwrap();
    let keys: Vec<_> = (0..streams).map(|i| StreamKey::new("wan", format!("s{i}"))).collect();
    if origin {
        for key in &keys {
            stream.open_stream(key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
        }
    }
    // Its own key, held here: the acceptors warm for a little over a lease after they start.
    let here = Placement::default().require("node", id);
    let epoch = loop {
        match cluster.get_or_spawn::<Unaddressed>("hold", id, Bytes::new(), &here).await {
            Ok(r) => break r.epoch(),
            Err(_) => sleep(Duration::from_millis(200)).await,
        }
    };

    // Unless shipped, a subscriber holds at least four pushes: a subscriber smaller than a push is reset by it at once.
    let push = max_push * 4;
    let limits = BranchLimits::default();
    let options = if shipped {
        SubscribeOptions::default()
    } else {
        SubscribeOptions {
            limits: BranchLimits {
                max_bytes: limits.max_bytes.max(push),
                max_records: limits.max_records.max(push / RECORD * 2),
                max_events: limits.max_events.max(1 << 16),
            },
            ..Default::default()
        }
    };
    let seen = Arc::new(Mutex::new(vec![Seen::default(); if origin { 0 } else { streams }]));
    let followers: Vec<_> = if origin {
        Vec::new()
    } else {
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let (stream, key, seen) = (stream.clone(), key.clone(), seen.clone());
                tokio::spawn(async move {
                    // An ended subscription is replaced: its `next` is `None` from then on.
                    let mut sub = stream.subscribe(&key, options).unwrap();
                    loop {
                        let event = sub.next().await;
                        let ended = event.as_ref().is_none_or(StreamEvent::is_terminal);
                        seen.lock().unwrap()[i].take(event);
                        if ended {
                            sub = stream.subscribe(&key, options).unwrap();
                        }
                    }
                })
            })
            .collect()
    };
    let seen_now = || Seen::sum(&seen.lock().unwrap());
    if origin {
        while held(&stream.stats(), streams).is_none() {
            sleep(Duration::from_millis(10)).await;
        }
    } else {
        while seen_now().records == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    }
    eprintln!("measuring"); // scripts/relay-wan.sh starts perf and ss on it
    let from = Instant::now() + WARM;
    let until = from + window;
    let peer = if origin { "w2" } else { "w1" };
    let probing = tokio::spawn(probe(cluster.clone(), peer, from, until));
    let watching = tokio::spawn(watch(cluster.clone(), stream.clone(), from, until));
    let pushes = |s: &RelayStats| {
        if origin { (s.pushes_out, s.push_bytes_out) } else { (s.pushes_in, s.push_bytes_in) }
    };
    let snapshot = |published| (published, seen_now(), cpu_secs(), pushes(&stream.stats()));

    let (mut published, mut refused) = (0u64, 0u64);
    let (at_from, at_until) = if origin {
        let payload = Bytes::from_static(&PAYLOAD);
        let (mut at_from, mut at_until) = (None, None);
        loop {
            let now = Instant::now();
            if at_from.is_none() && now >= from {
                at_from = Some(snapshot(published));
            }
            if at_until.is_none() && now >= until {
                at_until = Some(snapshot(published));
            }
            if now >= until + TAIL {
                break;
            }
            if published % (8 * BATCH as u64) == 0 && held(&stream.stats(), streams).is_none_or(|h| h > high) {
                sleep(Duration::from_millis(1)).await;
                continue;
            }
            // Round robin, so each stream keeps a backlog.
            match stream.publish(&keys[(published / BATCH as u64) as usize % streams], vec![payload.clone(); BATCH]) {
                Ok(_) => published += BATCH as u64,
                Err(_) => {
                    refused += 1;
                    sleep(Duration::from_millis(1)).await;
                }
            }
            tokio::task::yield_now().await;
        }
        (at_from.expect("the window started"), at_until.expect("the window ended"))
    } else {
        sleep_until(from).await;
        let at_from = snapshot(0);
        sleep_until(until).await;
        (at_from, snapshot(0))
    };
    // A follower ends only by a panic, after which its stream is not counted: no result rather than a low one.
    assert!(!followers.iter().any(tokio::task::JoinHandle::is_finished), "a follower ended: its panic is above");
    let (published_from, seen_from, cpu_from, pushes_from) = at_from;
    let (published_until, seen, cpu_until, (pushes_now, bytes_now)) = at_until;
    let (cpu, s) = (cpu_until - cpu_from, stream.stats());
    let n = if origin { published_until - published_from } else { seen.records - seen_from.records };
    let pushed = pushes_now - pushes_from.0;
    let per_sec = |x: u64| x as f64 / window.as_secs_f64();
    let mut out = format!(
        "node={id} role={} streams={streams} lane={} mb_s={:.2} rec_s={:.0} push_mb_s={:.2} pushes_s={:.1} \
         push_kib={:.0} resets={} gaps={} local_resets={} refused={refused} resent={} capped={} cpu={:.2}",
        if origin { "origin" } else { "leaf" },
        lane.name(),
        per_sec(n * RECORD as u64) / 1e6,
        per_sec(n),
        per_sec(bytes_now - pushes_from.1) / 1e6,
        per_sec(pushed),
        (bytes_now - pushes_from.1) as f64 / pushed.max(1) as f64 / 1024.0,
        seen.resets - seen_from.resets,
        seen.gaps - seen_from.gaps,
        s.local_resets,
        s.push_resent,
        s.push_capped,
        cpu / window.as_secs_f64(),
    );
    let (ms, failed) = probing.await.unwrap();
    spread(&mut out, &format!("req_to_{peer}"), ms);
    let _ = write!(out, " req_failed={failed}");
    let renewed: Vec<_> = renewals.lock().unwrap().iter().filter(|r| r.0 >= from && r.0 < until).copied().collect();
    let count = |o: Outcome| renewed.iter().filter(|r| r.2 == o).count();
    let _ = write!(out, " renew_failed={} renew_busy={}", count(Outcome::Failed), count(Outcome::Busy));
    let answered = renewed.iter().filter(|r| r.2 != Outcome::Busy);
    spread(&mut out, "renew", answered.map(|r| r.1.as_secs_f64() * 1e3).collect());
    out += &watching.await.unwrap();
    let hosted = cluster.hosted(16);
    let kept = hosted.keys.iter().any(|k| k.key == id && k.epoch == epoch);
    let ended = hosted.kinds.iter().map(|k| k.ended.iter().sum::<u64>()).sum::<u64>();
    let mut kinds = BTreeMap::<&str, u64>::new();
    for e in cluster.events().since(0, 256) {
        *kinds.entry(e.kind).or_default() += 1;
    }
    for e in stream.detail(0).events {
        *kinds.entry(e.kind).or_default() += 1;
    }
    for (reason, n) in &seen.reasons {
        *kinds.entry(reason).or_default() += n;
    }
    let kinds: Vec<_> = kinds.iter().map(|(k, n)| format!("{k}:{n}")).collect();
    let _ = write!(out, " key_kept={kept} keys_ended={ended} events={}", kinds.join(","));
    println!("{out}");
    // The child's window starts at its first batch, after the origin's: stay up until both nodes have printed, which
    // scripts/relay-wan.sh says with `<dir>/stop`, or a minute.
    let stop = std::path::Path::new(dir).join("stop");
    for _ in 0..600 {
        if stop.exists() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    followers.iter().for_each(tokio::task::JoinHandle::abort);
    system.shutdown().await.unwrap();
}

/// Bytes read from one connection between `WARM` after its first byte and `secs` later.
async fn tcp_recv(bind: &str, secs: u64) {
    let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut buf = vec![0u8; 1 << 20];
    let mut total = socket.read(&mut buf).await.unwrap() as u64;
    let from = Instant::now() + WARM;
    let until = from + Duration::from_secs(secs);
    let mut at_from = None;
    loop {
        let n = socket.read(&mut buf).await.unwrap();
        let now = Instant::now();
        if at_from.is_none() && now >= from {
            at_from = Some(total);
        }
        total += n as u64;
        if n == 0 || now >= until {
            break;
        }
    }
    println!("tcp mb_s={:.2}", (total - at_from.unwrap_or(total)) as f64 / secs as f64 / 1e6);
}

async fn tcp_send(to: &str, secs: u64) {
    let mut socket = loop {
        match tokio::net::TcpStream::connect(to).await {
            Ok(s) => break s,
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    };
    let (buf, until) = (vec![7u8; 1 << 20], Instant::now() + WARM + Duration::from_secs(secs + 1));
    while Instant::now() < until && socket.write_all(&buf).await.is_ok() {}
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).map(String::as_str).unwrap_or_default();
    let secs = |i: usize| arg(i).parse::<u64>().expect("seconds");
    match arg(0) {
        "node" => {
            let id = NODES.into_iter().find(|n| *n == arg(1)).expect("w1 or w2");
            let frame_mib = arg(3).parse().ok().filter(|&m| m > 0).expect("max_frame in MiB, 1 or more");
            let streams = arg(4).parse().ok().filter(|&n| n > 0).expect("streams, 1 or more");
            let flag = |f: &str| args.iter().skip(6).any(|a| a == f);
            node(id, secs(2), frame_mib, streams, arg(5), flag("shipped"), flag("bulk")).await;
        }
        "tcp-recv" => tcp_recv(arg(1), secs(2)).await,
        "tcp-send" => tcp_send(arg(1), secs(2)).await,
        _ => eprintln!(
            "relay_wan node <w1|w2> <secs> <max_frame MiB> <streams> <dir> [shipped] [bulk] | tcp-recv <addr> <secs> \
             | tcp-send <addr> <secs>"
        ),
    }
}
