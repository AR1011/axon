//! The `quota` workload (spec/fault-rig.md § Workloads): cluster-wide limits under faults, judged from `quota.jsonl`.
//!
//! - Hot rates `q0`, `q1` of `rig.hot` (200 a second): every node offers them at a fixed pace, 2.5 times the limit
//!   between them, and ignores `retry_after`, as an abusive client does.
//! - An under-limit rate `r0` of `rig.under` (200 a second), offered at 20 a second per node; a refused call waits its
//!   `retry_after` and tries again.
//! - Permits `p0`, `p1` of `rig.ws` (12 at once): six holders per node and key hold one for 0.2 to 2 s, logging `pacq`
//!   once they have it and `prel` before they drop it, so the logged spans lie inside the held ones.
//! - Every 60 s of wall clock the holders idle for 15 s: at 8 s each node logs what it holds (`qheld`), and at 9 s one
//!   node takes every permit of each key (`drain`), which only works if every other node's were given back.
//! - With `streams` after it in `WORKLOADS`, the stream node limits namespace `lim` ([`limits`]) and each node opens,
//!   publishes to, subscribes to and closes streams there ([`lim`]).
//!
//! Admissions are counted per 125 ms of the monotonic clock since the workload started, `m` on every line, so a node
//! whose wall clock is stepped is still placed on the timeline (check.py maps `m` per incarnation).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axon::trace::random_u64;
use axon_cluster::{Cluster, Denied, Permits, QuotaConfig, Quotas, Rate};
use axon_stream::{
    DeliveryPolicy, NamespaceLimit, NamespaceLimits, StartPosition, StreamError, StreamEvent, StreamKey, StreamNode,
    StreamNodeConfig, SubscribeOptions,
};
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::time::{MissedTickBehavior, interval, sleep};

use crate::Tick;
use crate::account::Log;
use crate::store::now_us;

const HOT_MAX: u64 = 200;
const UNDER_MAX: u64 = 200;
const HOT: Rate = Rate::new("rig.hot", HOT_MAX, Duration::from_secs(1));
const UNDER: Rate = Rate::new("rig.under", UNDER_MAX, Duration::from_secs(1));
const WS_MAX: u64 = 12;
const WS: Permits = Permits::new("rig.ws", WS_MAX);
const HOLDERS: usize = 6;
const LIM: NamespaceLimits = NamespaceLimits {
    subscriptions: Some(4),
    ring_bytes: None,
    records_per_sec: Some(200),
    bytes_per_sec: None,
    open_streams: Some(6),
};
/// A rate's bucket, window/8: admissions are counted on this grid.
const BUCKET_MS: u64 = 125;
const DRAIN_EVERY_S: u64 = 60;
const IDLE_S: u64 = 15;

struct Q {
    quotas: Quotas,
    cluster: Cluster,
    log: Log,
    started: Instant,
    held: [AtomicU64; 2],
    /// Streams of `lim` this node has open, each holding a permit of the open-streams limit in the same table.
    lim_open: AtomicU64,
}

static Q: OnceLock<Arc<Q>> = OnceLock::new();

impl Q {
    fn m(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn line(&self, r: &str, mut v: Value) {
        v["m"] = self.m().into();
        self.log.line(r, v);
    }
}

/// One key's calls, per bucket: admitted, refused at the limit, refused for a share; and the refusals' `retry_after`.
struct Grid {
    k: &'static str,
    /// The limit, and its window in ms (0 for permits).
    max: u64,
    w: u64,
    counts: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
    buckets: BTreeMap<u64, [u64; 3]>,
    /// The fewest and most ms a refusal said to wait, rounded up: 0 is a refusal that did not say.
    ra: Option<(u64, u64)>,
    /// The longest a caller that honours `retry_after` went from its first try to its admission, or so far.
    stuck: u64,
}

impl Grid {
    fn new(k: &'static str, max: u64, w: u64) -> Arc<Self> {
        Arc::new(Grid { k, max, w, counts: Mutex::default() })
    }

    fn count(&self, m: u64, i: usize, retry: Option<Duration>) {
        let Ok(mut c) = self.counts.lock() else { return };
        c.buckets.entry(m / BUCKET_MS).or_default()[i] += 1;
        if let Some(ra) = retry.map(|d| d.as_nanos().div_ceil(1_000_000) as u64) {
            c.ra = Some(c.ra.map_or((ra, ra), |(lo, hi)| (lo.min(ra), hi.max(ra))));
        }
    }

    fn denied(&self, m: u64, d: Denied) -> Duration {
        let (i, retry) = (if matches!(d, Denied::Limit { .. }) { 1 } else { 2 }, d.retry_after());
        self.count(m, i, Some(retry));
        retry
    }

    fn stuck(&self, ms: u64) {
        if let Ok(mut c) = self.counts.lock() {
            c.stuck = c.stuck.max(ms);
        }
    }

    /// Logs the buckets that ended, as `qrate`: `b` the first bucket's index, then per bucket `a`, `l` and `s`.
    fn report(&self, q: &Q) {
        let now = q.m() / BUCKET_MS;
        let Ok(mut c) = self.counts.lock() else { return };
        let rest = c.buckets.split_off(&now);
        let done = std::mem::replace(&mut c.buckets, rest);
        let (Some(&first), Some(&last)) = (done.keys().next(), done.keys().next_back()) else { return };
        let at = |i: usize| (first..=last).map(|b| done.get(&b).map_or(0, |v| v[i])).collect::<Vec<_>>();
        let (ra, stuck) = (c.ra.take(), std::mem::take(&mut c.stuck));
        drop(c);
        q.line(
            "qrate",
            json!({"k": self.k, "max": self.max, "w": self.w, "b": first, "a": at(0), "l": at(1), "s": at(2),
                   "ra": ra.map(|(lo, hi)| [lo, hi]), "stuck": stuck}),
        );
    }
}

/// Starts Quotas on this node and the load on its keys. Its tick counts the hot keys' calls.
pub async fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn std::error::Error>> {
    let at = |name: &str| cx.workloads.iter().position(|w| w == name);
    if let (Some(s), Some(q)) = (at("streams"), at("quota"))
        && s < q
    {
        return Err("list quota before streams in WORKLOADS: the stream node takes its quotas".into());
    }
    let q = Arc::new(Q {
        quotas: Quotas::start(cx.cluster, QuotaConfig::default()).await?,
        cluster: cx.cluster.clone(),
        log: Log::create(&cx.log_dir.join("quota.jsonl"))?,
        started: Instant::now(),
        held: Default::default(),
        lim_open: AtomicU64::new(0),
    });
    let _ = Q.set(q.clone());
    q.line(
        "qup",
        json!({"inc": cx.boot_ms, "nodes": cx.nodes, "hot": HOT_MAX, "under": UNDER_MAX, "ws": WS_MAX,
               "lim": {"subscriptions": LIM.subscriptions, "records": LIM.records_per_sec, "open": LIM.open_streams}}),
    );
    let tick = Tick::default();
    let mut grids = Vec::new();
    // 2.5 times the limit between the nodes, at a fixed pace.
    let every = Duration::from_micros(2_000_000 * cx.nodes as u64 / (5 * HOT_MAX));
    for k in ["q0", "q1"] {
        let g = Grid::new(k, HOT_MAX, 1000);
        tokio::spawn(hot(q.clone(), g.clone(), every, tick.clone()));
        grids.push(g);
    }
    let g = Grid::new("r0", UNDER_MAX, 1000);
    tokio::spawn(under(q.clone(), g.clone()));
    grids.push(g);
    for (i, k) in ["p0", "p1"].into_iter().enumerate() {
        let g = Grid::new(k, WS_MAX, 0);
        for _ in 0..HOLDERS {
            tokio::spawn(holder(q.clone(), g.clone(), i));
        }
        grids.push(g);
    }
    let me: usize = cx.sh.node.trim_start_matches('b').parse().unwrap_or(1);
    tokio::spawn(drains(q.clone(), me, cx.nodes));
    tokio::spawn(ready(q.clone()));
    tokio::spawn(report(q, grids));
    Ok(tick)
}

async fn hot(q: Arc<Q>, g: Arc<Grid>, every: Duration, tick: Tick) {
    let mut iv = interval(every);
    iv.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        iv.tick().await;
        tick.fetch_add(1, Relaxed);
        match q.quotas.take(g.k, &[(HOT, 1)]) {
            Ok(()) => g.count(q.m(), 0, None),
            Err(d) => {
                g.denied(q.m(), d);
            }
        }
    }
}

async fn under(q: Arc<Q>, g: Arc<Grid>) {
    let mut iv = interval(Duration::from_millis(50));
    iv.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        iv.tick().await;
        let first = q.m();
        loop {
            match q.quotas.take(g.k, &[(UNDER, 1)]) {
                Ok(()) => {
                    let m = q.m();
                    g.count(m, 0, None);
                    g.stuck(m - first);
                    break;
                }
                Err(d) => {
                    // A caller never admitted shows in every report until it is.
                    let m = q.m();
                    g.stuck(m - first);
                    sleep(g.denied(m, d)).await
                }
            }
        }
    }
}

/// In the drain's idle 15 s of each minute of wall clock.
fn idle() -> Option<Duration> {
    let s = now_us() / 1_000_000 % DRAIN_EVERY_S;
    (s < IDLE_S).then(|| Duration::from_secs(IDLE_S - s))
}

async fn holder(q: Arc<Q>, g: Arc<Grid>, i: usize) {
    loop {
        if let Some(d) = idle() {
            sleep(d).await;
            continue;
        }
        match q.quotas.acquire(g.k, &WS) {
            Ok(permit) => {
                g.count(q.m(), 0, None);
                q.held[i].fetch_add(1, Relaxed);
                q.line("pacq", json!({"k": g.k}));
                sleep(Duration::from_millis(200 + random_u64() % 1800)).await;
                q.line("prel", json!({"k": g.k}));
                q.held[i].fetch_sub(1, Relaxed);
                drop(permit);
            }
            Err(d) => sleep(g.denied(q.m(), d)).await,
        }
    }
}

/// At 8 s into each idle every node logs the permits it holds, which must be none; at 9 s node
/// `b{minute % nodes + 1}` takes every permit of each key, for up to 5 s, then gives them back.
async fn drains(q: Arc<Q>, me: usize, nodes: usize) {
    loop {
        let into = now_us() % (DRAIN_EVERY_S * 1_000_000);
        let wait = (DRAIN_EVERY_S * 1_000_000 + 8_000_000 - into) % (DRAIN_EVERY_S * 1_000_000);
        sleep(Duration::from_micros(wait.max(1))).await;
        let minute = now_us() / 1_000_000 / DRAIN_EVERY_S;
        let held: Vec<u64> = q.held.iter().map(|h| h.load(Relaxed)).collect();
        q.line("qheld", json!({"held": held, "table": q.quotas.stats().permits, "lim_open": q.lim_open.load(Relaxed)}));
        sleep(Duration::from_secs(1)).await;
        if minute as usize % nodes + 1 != me {
            continue;
        }
        let (at, mut got) = (Instant::now(), [Vec::new(), Vec::new()]);
        let short = |got: &[Vec<_>; 2]| got.iter().any(|g| (g.len() as u64) < WS_MAX);
        while short(&got) && at.elapsed() < Duration::from_secs(5) {
            let mut wait = Duration::from_secs(1);
            for (k, g) in ["p0", "p1"].into_iter().zip(&mut got) {
                while (g.len() as u64) < WS_MAX {
                    match q.quotas.acquire(k, &WS) {
                        Ok(p) => g.push(p),
                        Err(denied) => {
                            wait = wait.min(denied.retry_after());
                            break;
                        }
                    }
                }
            }
            if short(&got) {
                sleep(wait).await;
            }
        }
        for (k, g) in ["p0", "p1"].into_iter().zip(got) {
            q.line("drain", json!({"k": k, "got": g.len(), "want": WS_MAX, "ms": at.elapsed().as_millis() as u64}));
        }
    }
}

/// `qready` once this node's replica has pulled the others' rows after its start: what `/readyz` waits for.
async fn ready(q: Arc<Q>) {
    while q.cluster.quotas_starting() {
        sleep(Duration::from_millis(100)).await;
    }
    q.line("qready", json!({}));
}

async fn report(q: Arc<Q>, grids: Vec<Arc<Grid>>) {
    let mut iv = interval(Duration::from_secs(1));
    let mut n = 0u64;
    loop {
        iv.tick().await;
        for g in grids.iter().chain(LIM_GRIDS.get().into_iter().flatten()) {
            g.report(&q);
        }
        if let Some(s) = LIM_SUBS.get() {
            s.report(&q);
        }
        n += 1;
        if n.is_multiple_of(10) {
            let s = q.quotas.stats();
            q.line(
                "qstats",
                json!({"ready": !q.cluster.quotas_starting(), "pulling": s.pulling, "entries": s.entries,
                       "permits": s.permits, "keys": s.keys, "rows": s.rows, "mismatched": s.mismatched,
                       "denied_limit": s.denied_limit, "denied_share": s.denied_share, "fallback": s.fallback,
                       "reports": s.reports, "report_failures": s.report_failures}),
            );
        }
    }
}

static LIM_GRIDS: OnceLock<[Arc<Grid>; 2]> = OnceLock::new();
static LIM_SUBS: OnceLock<Arc<Subs>> = OnceLock::new();

/// The stream node's limits on namespace `lim`, through this node's quotas, where the `quota` workload runs.
pub fn limits(cfg: StreamNodeConfig) -> StreamNodeConfig {
    let Some(q) = Q.get() else { return cfg };
    let mut cfg = cfg.limit("lim", LIM);
    cfg.quotas = Some(q.quotas.clone());
    cfg
}

/// Subscriptions to `lim` this node's subscribers hold: counted up once admitted and down before they are dropped, so
/// the count is never above the node's own.
#[derive(Default)]
struct Subs {
    held: AtomicU64,
    max: AtomicU64,
    refused: AtomicU64,
    /// Ends read, after which the node gave the place back before the count went down.
    ends: AtomicU64,
    errors: AtomicU64,
}

impl Subs {
    fn report(&self, q: &Q) {
        let held = self.held.load(Relaxed);
        q.line(
            "lsub",
            json!({"held": held, "max": self.max.swap(held, Relaxed), "refused": self.refused.swap(0, Relaxed),
                   "ends": self.ends.swap(0, Relaxed), "errors": self.errors.swap(0, Relaxed)}),
        );
    }
}

/// The `lim` load on `node`, whose streams `lim/<me>-0..2` this node opens for 2 to 6 s at a time, publishing to each
/// every 10 ms, while six subscribers hold subscriptions to them for 1 to 3 s.
pub fn lim(node: &StreamNode, me: &str) {
    let Some(q) = Q.get() else { return };
    let grid = |k, max: Option<u64>, w| Grid::new(k, max.unwrap_or(0), w);
    let grids =
        LIM_GRIDS.get_or_init(|| [grid("lim", LIM.records_per_sec, 1000), grid("lim.open", LIM.open_streams, 0)]);
    let subs = LIM_SUBS.get_or_init(Arc::default);
    let keys: Vec<StreamKey> = (0..3).map(|j| StreamKey::new("lim", format!("{me}-{j}"))).collect();
    for key in keys.iter().cloned() {
        tokio::spawn(opener(q.clone(), node.clone(), key, grids.clone()));
    }
    for i in 0..6 {
        tokio::spawn(subscriber(node.clone(), keys[i % keys.len()].clone(), subs.clone()));
    }
}

/// A refusal at a limit, and how long it said to wait: zero where it did not, which only the local limits may do.
fn retry(e: &StreamError) -> Option<Duration> {
    match e {
        StreamError::QuotaExceeded { retry_after, .. } => Some(retry_after.unwrap_or_default()),
        _ => None,
    }
}

async fn opener(q: Arc<Q>, node: StreamNode, key: StreamKey, [rate, open]: [Arc<Grid>; 2]) {
    let s = key.to_string();
    // A new generation each time, above the last incarnation's: its boot ms, then a count.
    let mut generation = now_us() / 1000 * 1000;
    loop {
        generation += 1;
        match node.open_stream(&key, generation, DeliveryPolicy::ReplayRequired).await {
            Ok(()) => {
                open.count(q.m(), 0, None);
                q.lim_open.fetch_add(1, Relaxed);
                q.line("lopen", json!({"s": s, "g": generation}));
            }
            Err(e) => {
                match retry(&e) {
                    Some(d) => {
                        open.count(q.m(), 1, Some(d));
                        sleep(d.max(Duration::from_millis(100))).await;
                    }
                    None => {
                        q.line("lerr", json!({"s": s, "op": "open", "err": format!("{e:?}")}));
                        sleep(Duration::from_secs(1)).await;
                    }
                }
                continue;
            }
        }
        let until = Instant::now() + Duration::from_millis(2000 + random_u64() % 4000);
        let mut iv = interval(Duration::from_millis(10));
        iv.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut failing = false;
        while Instant::now() < until {
            iv.tick().await;
            match node.publish(&key, vec![Bytes::from_static(b"x")]) {
                Ok(_) => rate.count(q.m(), 0, None),
                Err(e) => match (&e.error, retry(&e.error)) {
                    (StreamError::QuotaExceeded { limit: NamespaceLimit::Records, .. }, Some(d)) => {
                        rate.count(q.m(), 1, Some(d))
                    }
                    _ if !failing => {
                        failing = true;
                        q.line("lerr", json!({"s": s, "op": "publish", "err": format!("{:?}", e.error)}));
                    }
                    _ => {}
                },
            }
        }
        q.line("lclose", json!({"s": s, "g": generation}));
        q.lim_open.fetch_sub(1, Relaxed);
        if let Err(e) = node.close_stream(&key) {
            q.line("lerr", json!({"s": s, "op": "close", "err": format!("{e:?}")}));
        }
        sleep(Duration::from_millis(random_u64() % 500)).await;
    }
}

async fn subscriber(node: StreamNode, key: StreamKey, subs: Arc<Subs>) {
    loop {
        match node.subscribe(&key, SubscribeOptions { start: StartPosition::Live, ..Default::default() }) {
            Ok(mut sub) => {
                let held = subs.held.fetch_add(1, Relaxed) + 1;
                subs.max.fetch_max(held, Relaxed);
                let until = tokio::time::Instant::now() + Duration::from_millis(1000 + random_u64() % 2000);
                while let Ok(Some(event)) = tokio::time::timeout_at(until, sub.next()).await {
                    if let StreamEvent::End(_) = event {
                        subs.ends.fetch_add(1, Relaxed);
                        break;
                    }
                }
                subs.held.fetch_sub(1, Relaxed);
                drop(sub);
            }
            Err(e) => {
                let c = if retry(&e).is_some() { &subs.refused } else { &subs.errors };
                c.fetch_add(1, Relaxed);
                sleep(Duration::from_millis(200)).await;
            }
        }
        sleep(Duration::from_millis(random_u64() % 200)).await;
    }
}
