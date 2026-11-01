//! usage: rig store [bind=127.0.0.1:7900] [log=store.jsonl]
//!        rig node
//!        rig peer <addr> <mode> [claim_id]   a malformed/confused-peer probe (src/peer.rs); env in that file
//! env (node): RUN        the run's name, in the journal's file name (default local)
//!             NODE_ID    this node's id (b1); IP where it binds and the others reach it (127.0.0.1)
//!             NODES      every node's seed server, `ip` or `ip:port` (127.0.0.1:7001)
//!             ACCEPTORS  b1,b2,b3; PORT 7400; SEED_PORT 7001; STORE 127.0.0.1:7900
//!             LOAD       ledger (store and journal) | serve (answer at once)
//!             WORKLOADS  what the node runs, each a `start` in src/<workload>.rs (accounts in src/account.rs):
//!                        accounts,streams under ledger, accounts under serve; also compacted (after streams), disk,
//!                        quota, eager, watch, echo, idle, blob, local; bulk (BULK, the file of the bulk fault, while
//!                        which b1 uploads) and firehose (FIREHOSE_MBS 13), both after streams
//!             EAGER_KEYS 100, WATCH_KEYS 20, IDLE_KEYS 20: the keys of those workloads; AUTHZ, the file of the
//!                        authz fault that echo reads (/run/rig-fault/authz)
//!             KEYS 2000; EVERY_MS 20000, each key's period on this node; LEASE_SECS 6; SECRET, the same everywhere
//!             LOG_DIR    where node.jsonl and client.jsonl go (.); JOURNAL, the journal's directory (LOG_DIR)
//!             LEASE_DIR  an acceptor's lease log, kept across its restarts (LOG_DIR/leases)
//!             STREAMS    streams this node originates (4); the nodes are b1..bN, N the length of NODES, and each
//!                        follows the others' (src/streams.rs), logging to LOG_DIR/stream.jsonl
//!             STREAM_DISK where the disk workload keeps its streams (src/disk.rs), which runs after streams
//!             quota      in WORKLOADS before streams: rates and permits across the cluster, and the stream node's
//!                        limits on namespace lim (src/quota.rs), logging to LOG_DIR/quota.jsonl
//!             HEALTH_PORT /healthz, /readyz and /metrics on IP (7401), src/surface.rs
//!             SEEDS      how many nodes serve seeds, b1..bSEEDS, the first of NODES (every node)
//!             TLS_DIR    mutual TLS instead of the secret, from TLS_DIR/current, again on SIGHUP (src/surface.rs)
//!             HEARTBEAT_MS  500, the grace twice it: another value only as fault.sh's cfgdrift
//!             TIMINGS    wan: spec/wan.md § 9's values for a cluster across regions, the grace four times the
//!                        heartbeat, and a cap on each Bulk session (spec/lanes.md § The rig (D3))
mod account;
mod blob;
mod bulk;
mod compacted;
mod dead;
mod disk;
mod eager;
mod echo;
mod firehose;
mod hooks;
mod idle;
mod load;
mod local;
mod peer;
mod quota;
mod resources;
mod store;
mod streams;
mod surface;
mod watch;

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::ActorSystem;
use axon_cluster::{
    Cluster, ClusterConfig, ClusterError, DiscoveryError, OwnershipError, PaxosConfig, PaxosLeases, SeedConfig,
    SeedDiscovery, SeedServer, SeedServerConfig, SeedTimings,
};
use axon_net::{CertificateIdentity, SharedSecret};
use axon_stream::StreamError;
use serde_json::{Map, json};
use tokio::signal::unix::{SignalKind, signal};

use account::{Log, Shared};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// A workload's heartbeat: its own loop bumps it, and each `stats` line carries it, so check.py tells a wedged
/// workload from a quiet one.
pub type Tick = Arc<AtomicU64>;

/// What a workload's `start` gets.
pub struct Ctx<'a> {
    pub cluster: &'a Cluster,
    pub sh: &'a Arc<Shared>,
    pub log_dir: &'a Path,
    /// The nodes b1..b{nodes}, from `NODES`.
    pub nodes: usize,
    pub boot_ms: u64,
    /// Every workload this node runs, for one that serves another (streams keeps disk's streams).
    pub workloads: &'a [String],
    pub system: &'a ActorSystem,
}

impl Ctx<'_> {
    pub fn ids(&self) -> Vec<String> {
        (1..=self.nodes).map(|i| format!("b{i}")).collect()
    }

    /// Every node but this one.
    pub fn others(&self) -> Vec<String> {
        self.ids().into_iter().filter(|n| *n != self.sh.node).collect()
    }
}

/// How often a workload logs its summary line: check.py's `HEARTBEAT` (15 s) expects one at least that often.
pub const REPORT: Duration = Duration::from_secs(10);

/// Ticks every `REPORT`, the first one `REPORT` from now.
pub fn reports() -> tokio::time::Interval {
    tokio::time::interval_at(tokio::time::Instant::now() + REPORT, REPORT)
}

/// A run of publish failures: logged once, as `publish_err`, and over once a publish is taken again.
#[derive(Clone, Copy, Default)]
pub struct Failing(Option<Instant>);

impl Failing {
    pub fn err(&mut self, log: &Log, s: &impl std::fmt::Display, err: &StreamError) {
        if self.0.is_none() {
            self.0 = Some(Instant::now());
            log.line("publish_err", json!({"s": s.to_string(), "err": format!("{err:?}")}));
        }
    }

    /// How long the run that this ends lasted, if one did.
    pub fn ok(&mut self) -> Option<Duration> {
        self.0.take().map(|since| since.elapsed())
    }
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// The rig's WAN profile, `TIMINGS=wan`.
pub fn wan() -> bool {
    env("TIMINGS", "") == "wan"
}

/// Each Bulk session's cap under the WAN profile: above firehose's 13 MB/s, which shares b1's Bulk sessions with the
/// uploads, and under `rate-bulk`'s 150 Mbit/s on b1's links.
const BULK_CAP: u64 = 16_000_000;

/// Four lost segments' retransmits before an idle-out, from the sin <-> us round trip: the transport's and the seeds'.
const WAN_IDLE: Duration = Duration::from_secs(8);

/// spec/wan.md § 9, each value from the slowest link, sin <-> us at 219 ms. `ClusterConfig::new` sets the first three
/// again, so a cluster's transport takes this after it.
fn wan_transport(t: &mut axon_net::Config) {
    // a ping a second; WAN_IDLE; two lost SYNs in a connect
    t.keepalive_interval = Duration::from_secs(1);
    t.idle_timeout = WAN_IDLE;
    t.connect_timeout = Duration::from_millis(3500);
    t.handshake_timeout = Duration::from_secs(5);
    t.lanes.bulk.max_bytes_per_sec = Some(BULK_CAP);
}

fn list(name: &str, default: &str) -> Vec<String> {
    env(name, default).split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect()
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize, default: &str| args.get(i).cloned().unwrap_or_else(|| default.to_owned());
    match arg(0, "").as_str() {
        "store" => Ok(store::serve(&arg(1, "127.0.0.1:7900"), &arg(2, "store.jsonl"))?),
        "node" => tokio::runtime::Runtime::new()?.block_on(node()),
        "peer" => peer::run(&arg(1, "127.0.0.1:7400"), &arg(2, "secret"), &arg(3, "b1")),
        _ => Err("usage: rig store [bind] [log] | node | peer <addr> <mode> [claim_id]".into()),
    }
}

async fn node() -> Result<()> {
    let id = env("NODE_ID", "b1");
    let ip = env("IP", "127.0.0.1");
    let seed_port = env("SEED_PORT", "7001");
    let log_dir = std::path::PathBuf::from(env("LOG_DIR", "."));
    let journal_dir = std::path::PathBuf::from(env("JOURNAL", &log_dir.to_string_lossy()));
    std::fs::create_dir_all(&log_dir)?;
    std::fs::create_dir_all(&journal_dir)?;
    let boot_ms = store::now_us() / 1000;
    // Before anything slow: until it exists a SIGHUP, which a CA rotation sends a node still starting, kills it.
    let mut hup = signal(SignalKind::hangup())?;
    let sh = Arc::new(Shared {
        node: id.clone(),
        serve: env("LOAD", "ledger") == "serve",
        store: store::Client::new(env("STORE", "127.0.0.1:7900")),
        journal: Log::create(&journal_dir.join(format!("journal-{}.jsonl", env("RUN", "local"))))?,
        node_log: Log::create(&log_dir.join("node.jsonl"))?,
        client_log: Log::create(&log_dir.join("client.jsonl"))?,
        rtt: Default::default(),
        fsync: Default::default(),
        latency: Default::default(),
        acks: Default::default(),
        give_ups: Default::default(),
        errors: Default::default(),
    });
    let workloads = list("WORKLOADS", if sh.serve { "accounts" } else { "accounts,streams" });

    let secret = env("SECRET", "rig-shared-secret");
    let certs = surface::Certs::from_env(&id);
    let tls = certs.as_ref().map(surface::Certs::load).transpose()?.map(|(pass, tls)| {
        sh.node_log.line("tls", json!({"pass": pass, "err": null, "at": "boot"}));
        tls
    });
    let blobs = workloads.iter().any(|w| w == "blob");
    let transport = |id: &str| -> Result<axon_net::Config> {
        let mut t = if tls.is_some() {
            axon_net::Config::new(id, CertificateIdentity)
        } else {
            let mut t = axon_net::Config::new(id, SharedSecret::new(secret.as_bytes())?);
            t.allow_plaintext = true; // a private network behind the rig's allowlist: crates/axon/README.md rule 8
            t
        };
        if blobs {
            t.lanes.live.limits.max_frame = blob::MAX + (64 << 10);
        }
        if wan() {
            wan_transport(&mut t);
        }
        Ok(t)
    };
    let seed_addr: SocketAddr = format!("{ip}:{seed_port}").parse()?;
    let serving = match env("SEEDS", "0").parse()? {
        0 => usize::MAX,
        n => n,
    };
    let seed = if serving == usize::MAX || id.trim_start_matches('b').parse().is_ok_and(|i: usize| i <= serving) {
        Some(
            SeedServer::bind(SeedServerConfig {
                tls: tls.clone(),
                timings: seed_timings(),
                ..SeedServerConfig::new(seed_addr, &transport(&format!("{id}-seed"))?)
            })
            .await?,
        )
    } else {
        None
    };
    // Over TLS a seed is dialled by its id, the name its certificate holds.
    let seeds = list("NODES", "127.0.0.1").into_iter().take(serving).enumerate().map(|(i, n)| {
        let n = if n.contains(':') { n } else { format!("{n}:{seed_port}") };
        if tls.is_some() { format!("b{}-seed@{n}", i + 1) } else { n }
    });
    let transport = transport(&id)?;
    let mut seed_cfg = SeedConfig::new(seeds.collect::<Vec<_>>(), &transport);
    seed_cfg.tls = tls.clone();
    seed_cfg.timings = seed_timings();
    if wan() {
        // spec/wan.md § 9: the seed links' connect, as the cluster's
        seed_cfg.transport.connect_timeout = Duration::from_millis(3500);
    }
    let discovery = SeedDiscovery::start(seed_cfg).await?;
    let lease = Duration::from_secs_f64(env("LEASE_SECS", "6").parse()?);
    let beat = Duration::from_millis(env("HEARTBEAT_MS", "500").parse()?);
    let system = ActorSystem::builder().build()?;
    let lease_dir = env("LEASE_DIR", &log_dir.join("leases").to_string_lossy());
    let cluster = loop {
        let mut paxos = PaxosConfig::new(list("ACCEPTORS", "b1,b2,b3"));
        // PaxosConfig::new's 6 s unless LEASE_SECS says otherwise.
        paxos.lease = lease;
        // A store of its own each attempt: one a failed attempt's writer still holds is refused, not shared.
        paxos.store = Some(Arc::new(axon::files::OsFiles::new(&lease_dir)));
        let bind = format!("{ip}:{}", env("PORT", "7400")).parse()?;
        let mut cfg = ClusterConfig::new(bind, transport.clone(), discovery.clone());
        cfg.tls = tls.clone();
        cfg.labels.insert("node".into(), id.clone()); // where streams::start looks for a stream's origin and relay
        // With seeds, as guide/cluster.md § Seed servers asks: 500 ms and twice that, unless cfgdrift says.
        (cfg.heartbeat_interval, cfg.heartbeat_grace) = (beat, beat * 2);
        if wan() {
            // spec/wan.md § 9: four times it, so the peer check's half of the grace holds an ask and a retransmit
            cfg.heartbeat_grace = beat * 4;
            wan_transport(&mut cfg.transport);
            // spec/wan.md § 9: an operation holds a whole Paxos round at lease 10 s, 6.67 s, and a round trip
            cfg.operation_timeout = Duration::from_secs(7);
        }
        cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos)?));
        // Seeds still restarting after a cold restart: the record is not listed yet. Retry::Later says wait. Or the
        // lease log still in use by the attempt before.
        let again = |e: &ClusterError| match e {
            ClusterError::Discovery(DiscoveryError::Unavailable(_)) => true,
            ClusterError::Ownership(OwnershipError::Unavailable(why)) => why.contains("in use"),
            _ => false,
        };
        match Cluster::start(&system, cfg).await {
            Err(e) if again(&e) => {
                eprintln!("cluster start: {e}, again in 1 s");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            started => break started?,
        }
    };
    sh.node_log.line(
        "up",
        json!({"node": id, "inc": boot_ms, "workloads": workloads, "build": account::BUILD.0,
               "schemas": account::BUILD.1, "lease_ms": lease.as_millis() as u64, "heartbeat_ms": beat.as_millis() as u64,
               "timings": env("TIMINGS", "shipped")}),
    );
    println!("{id} on {} ({})", cluster.local_addr(), env("LOAD", "ledger"));
    surface::health(&system, &cluster, &sh, &ip).await;

    let nodes = list("NODES", "127.0.0.1").len();
    let cx =
        Ctx { cluster: &cluster, sh: &sh, log_dir: &log_dir, nodes, boot_ms, workloads: &workloads, system: &system };
    // Before the workloads, whose starts can wait seconds (acceptors warming): a hook on meanwhile still acts.
    hooks::start(&cx);
    let mut ticks = Vec::new();
    for w in &workloads {
        let tick = match w.as_str() {
            "accounts" => account::start(&cx).await?,
            "streams" => streams::start(&cx).await?,
            "disk" => disk::start(&cx).await?,
            "quota" => quota::start(&cx).await?,
            "compacted" => compacted::start(&cx).await?,
            "eager" => eager::start(&cx).await?,
            "watch" => watch::start(&cx)?,
            "idle" => idle::start(&cx).await?,
            "echo" => echo::start(&cx).await?,
            "blob" => blob::start(&cx)?,
            "bulk" => bulk::start(&cx)?,
            "firehose" => firehose::start(&cx).await?,
            "local" => local::start(&cx)?,
            _ => return Err(format!("unknown workload {w}").into()),
        };
        ticks.push((w.clone(), tick));
    }

    let mut term = signal(SignalKind::terminate())?;
    let mut tick = reports();
    let reason = loop {
        tokio::select! {
            _ = term.recv() => break "sigterm",
            _ = tokio::signal::ctrl_c() => break "sigint",
            _ = hup.recv() => match &certs {
                Some(certs) => certs.reload(&cluster, &discovery, seed.as_ref(), &sh),
                None => sh.node_log.line("tls", json!({"pass": null, "err": "not in TLS mode", "at": "hup"})),
            },
            _ = tick.tick() => {
                stats(&system, &cluster, &sh, &ticks);
                resources::log(&system, &cluster, &sh);
                dead::log(&system, &sh);
            }
        }
    };
    // A clean leave: each key's lease is released once its actor stopped. check.py's `shutdown-bounded` reads this.
    let at = Instant::now();
    let down = system.shutdown().await;
    let (err, still) = match &down {
        Ok(r) => (r.failures.first().cloned(), r.still_running.len()),
        Err(e) => (Some(e.to_string()), 0),
    };
    let ms = at.elapsed().as_millis() as u64;
    sh.node_log.line("down", json!({"ms": ms, "err": err, "reason": reason, "still_running": still}));
    // Out at once: dropping the runtime waits for blocking jobs, which a slow disk holds past systemd's stop timeout.
    std::process::exit(match down {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {e}");
            1
        }
    })
}

/// spec/wan.md § 9 under `TIMINGS=wan`, else the shipped ones. Every seed and node must run the same.
fn seed_timings() -> SeedTimings {
    if !wan() {
        return SeedTimings::default();
    }
    SeedTimings {
        // spec/wan.md § 9: idle as the transport's; drops_after 5 s above the 3.5 s a node takes to isolate; a LEAVE's
        // retransmits; a redial at WAN loss
        idle: WAN_IDLE,
        idle_hold: Duration::from_secs(7),
        closed_grace: Duration::from_secs(4),
        warm_up: Duration::from_secs(8),
    }
}

fn stats(system: &ActorSystem, cluster: &Cluster, sh: &Shared, ticks: &[(String, Tick)]) {
    let c = cluster.stats();
    let s = system.stats();
    let ownership: Map<_, _> =
        c.ownership.map(|o| o.stats).unwrap_or_default().into_iter().map(|(k, v)| (k, v.into())).collect();
    let pct = |samples: &std::sync::Mutex<Vec<u64>>| {
        let mut s = samples.lock().map(|mut s| std::mem::take(&mut *s)).unwrap_or_default();
        s.sort_unstable();
        let at = |q: f64| s.get(((s.len() as f64 * q) as usize).min(s.len().saturating_sub(1))).copied().unwrap_or(0);
        json!({"n": s.len(), "p50": at(0.5), "p99": at(0.99), "max": s.last().copied().unwrap_or(0)})
    };
    // Supervised restarts: the retired families' and the live ones'.
    let mut restarts: u64 = s.retired.values().map(|t| t.restarts).sum();
    let mut after = None;
    while let Ok(page) = system.diagnostics_page(after, 1000) {
        restarts += page.actors.iter().map(|a| a.restarts).sum::<u64>();
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let (e, l) = (&c.engine, &c.engine.listener);
    let (acks, give_ups) = (sh.acks.load(Relaxed), sh.give_ups.load(Relaxed));
    let ticks: Map<_, _> = ticks.iter().map(|(w, t)| (w.clone(), t.load(Relaxed).into())).collect();
    sh.node_log.line(
        "stats",
        json!({"spawned": c.spawned, "leases_lost": c.leases_lost, "isolations": c.isolations, "isolated": c.isolated,
               "keyed_failed": c.keyed_failed, "members": c.members, "unreachable": c.unreachable,
               "ownership": ownership,
               "rtt_us": pct(&sh.rtt), "fsync_us": pct(&sh.fsync), "acks": acks, "give_ups": give_ups,
               "net": {"dials": e.connects, "dial_failures": e.dial_failures, "accepted": l.accepted,
                       "accept_errors": l.accept_errors, "rejected_busy": l.rejected_busy,
                       "rejected_per_ip": l.rejected_per_ip, "handshake_failures": l.handshake_failures},
               "replaced": c.replaced, "became_unreachable": c.became_unreachable,
               "heartbeat_failures": c.heartbeat_failures, "discovery_errors": c.discovery_errors,
               "handoffs": c.handoffs, "handoffs_failed": c.handoffs_failed, "warming": cluster.warming(),
               "dead_letters": s.dead_letters, "actors_failed": s.failed, "restarts": restarts, "ticks": ticks}),
    );
    if sh.serve {
        let mut load = pct(&sh.latency);
        load["errors"] = sh.errors.swap(0, Relaxed).into();
        sh.node_log.line("load", load);
    }
}
