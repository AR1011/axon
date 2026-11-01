//! The Quotas cores on a simulated network of 50 nodes in 3 or 4 regions (spec/quotas.md §7): lost, duplicated, late
//! and reordered messages, clocks that drift, views that lag the truth and each other, crashes, restarts as new
//! incarnations, pauses, and partitions into regions and back, while demand for a hot key moves between regions and
//! every node draws Zipf keys with weighted costs and `charge`. Each node runs a table and a replica, reports every
//! 100 ms to its keys' replicas as §2.2 chooses them, and a new replica pulls from every other candidate.
//!
//! The oracle knows real time and every unit admitted. Checked at each admission: over any window, a limit's key admits
//! no more than its limit plus what was taken on the fallback, what was charged, and what the faults in effect allow,
//! each fault its row of §5 for as long as the row says, so that both the peak and the time-integrated excess stay
//! within the rows. A node takes at most `a` on the fallback, and only while too few of the key's replicas could answer
//! it, or briefly after what §2.5 lists. Outside faults, a hot key whose demand exceeds its limit is admitted at least
//! 95 % of what the limit allows. `SIM_SEED=n` replays one seed, `SEEDS=n` or `SEEDS=a..b` runs more, `SIM_DEBUG=1`
//! prints each run.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon_testkit::{Link as Wire, NodeId, SimConfig, Simulator, check_seeds};
use bytes::Bytes;

use super::core::*;
use super::wire::*;
use crate::ownership::lease_hold;
use crate::sim::Clock;

const NS: u64 = 7;
const NODES: usize = 50;
const LEASE: Duration = Duration::from_secs(5);
/// How far a clock runs from real time, at most.
const PPM: u64 = 5_000;
/// ρ between any two clocks.
const DRIFT: f64 = 2.0 * PPM as f64 / 1e6;
const TICK: Duration = Duration::from_millis(20);
const SAMPLE: Duration = Duration::from_millis(100);
const WARM: Duration = Duration::from_secs(5);
const FAULTS_END: Duration = Duration::from_secs(35);
const RUN: Duration = Duration::from_secs(55);
/// A crashed process leaves the truth this long after, unless it restarted first.
const DETECT: Duration = Duration::from_secs(3);
/// How far a process's view lags the truth, at most.
const LAG_MS: u64 = 1500;
/// Liveness counts again this long after a fault ended, and after demand moved.
const SETTLE: Duration = Duration::from_secs(6);
const MOVE_SETTLE: Duration = Duration::from_secs(3);
/// A key admits on the fallback no longer than this after too few of its replicas could answer a node, after its
/// replicas moved in the node's view, or after the node first used it, booted or resumed.
const GRACE: Duration = Duration::from_millis(1500);
const CAPACITY: usize = 1 << 14;
/// Requests a node's clients keep waiting for the hot key, per limit.
const BACKLOG: usize = 20;
const ZIPF: usize = 100;
/// Key 0 is the hot key.
const KEYS: usize = ZIPF + 1;
const REGIONS: [&str; 4] = ["r0", "r1", "r2", "r3"];

const REQ: usize = 0;
const WEIGHT: usize = 1;
const WS: usize = 2;
const FEW: usize = 3;
/// The hot key takes `req/s`, `ws` and `ws/few`; `ws` gives a hot node about 15 permits, `ws/few` 1 or 2, where
/// grants come in fragments.
const LIMITS: [Limit; 4] = [
    Limit { name: "req/s", max: 200, window: Duration::from_secs(1) },
    Limit { name: "weight/10s", max: 2000, window: Duration::from_secs(10) },
    Limit { name: "ws", max: 400, window: Duration::ZERO },
    Limit { name: "ws/few", max: 40, window: Duration::ZERO },
];
/// The least share of `ws/few` held outside faults: replicas that disagree by one on a grant of 1 or 2 lose it.
const FEW_HELD: f64 = 0.75;

/// What a session carries besides the body: the sender's incarnation, the link or pull a reply belongs to, and when
/// the request left, for the driver's timeouts (`Rig` alone applies them). A push goes to one process of a node, and
/// its acknowledgement names that process.
#[derive(Clone)]
enum Msg {
    Report { incarnation: u128, link: usize, body: Bytes, at: Duration },
    Grants { link: usize, body: Bytes, at: Duration },
    Push { to: u128, body: Bytes, at: Duration },
    Ack { incarnation: u128, body: Bytes, at: Duration },
    Pull { round: u64, body: Bytes, at: Duration },
    Page { round: u64, body: Bytes, at: Duration },
}

/// Members as a view shows them: node, incarnation and region, and as the cores see them.
struct View {
    members: BTreeMap<Arc<str>, (u128, usize)>,
    quota: Members,
}

impl View {
    fn new(members: BTreeMap<Arc<str>, (u128, usize)>) -> Self {
        let quota = Members::new(
            NS,
            Candidates::PER_REGION,
            members.iter().map(|(node, (inc, r))| (&**node, *inc, REGIONS[*r])),
        );
        Self { members, quota }
    }
}

/// One process of a node: its table, and the drive of it and its replica.
struct Proc {
    inc: u128,
    clock: Clock,
    lag: Duration,
    table: Table,
    node: Node,
    /// Where in the truth's history its view is, and the nodes it shows unreachable.
    view: usize,
    cut: BTreeSet<Arc<str>>,
    next_report: Duration,
    /// Permits held: their tally, when released, and whether taken on the fallback.
    permits: Vec<(usize, Duration, bool)>,
    /// Rate units taken on the fallback in the longest window: when, their tally, and how many.
    fell: VecDeque<(Duration, usize, u64)>,
    /// When it booted or last resumed, and first tried each tally.
    fresh: Duration,
    first: BTreeMap<usize, Duration>,
    /// The views it left lately, and when.
    left: VecDeque<(Duration, Arc<View>)>,
    paused_until: Option<Duration>,
    inbox: Vec<(NodeId, Msg)>,
    /// When each of the hot key's refused requests is retried, per limit it takes.
    waiting: [Vec<Duration>; 3],
}

struct Host {
    name: Arc<str>,
    region: usize,
    proc: Option<Proc>,
    incs: u128,
    up_at: Option<Duration>,
    down_since: Option<Duration>,
}

/// Per limit and key: what was admitted in the window, or the permits held.
#[derive(Default)]
struct Tally {
    admits: VecDeque<(Duration, u64, Kind)>,
    sum: u64,
    fallback: u64,
    charged: u64,
    held: u64,
    held_fallback: u64,
}

impl Tally {
    fn drop_permit(&mut self, fallback: bool) {
        (self.held, self.held_fallback) = (self.held - 1, self.held_fallback - fallback as u64);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Grant,
    Fallback,
    Charge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Fault {
    Crash,
    Pause,
    Partition,
    Cut,
    Lost,
    /// `Rig`'s full links and the reachability they flap: replicas whose rows lag by seconds (§5).
    Slow,
}

/// The rig's episode of 2026-10-01 on five nodes in one region: a rate fault fills every link's queue, so messages
/// wait seconds, in order; one node is killed in it, and its next process replaces it in the views two seconds later
/// but runs Quotas only seconds after that; while the links are full, each node finds peers unreachable, and
/// reachable again, on its own.
struct Rig {
    from: Duration,
    until: Duration,
    /// Per pair of nodes, the longest its queue grows.
    caps: BTreeMap<(NodeId, NodeId), Duration>,
    victim: NodeId,
    crash: Duration,
    /// When its next process replaces it in the views, and when that one runs Quotas.
    replaced: Duration,
    up: Duration,
    /// Per node and peer, one in how many flip reachability every 500 ms while the links are full; 0 for never.
    flap: u64,
    /// Per node, how long after the crash it finds the victim unreachable.
    noticed: Vec<Duration>,
    crashed: bool,
}

/// A fault's allowance over the limit, from `from` until `until`.
#[derive(Clone, Copy, Debug)]
struct Episode {
    from: Duration,
    until: Duration,
    units: u64,
}

/// What a run measured: per set of faults in effect, the most over the limit and its integral, in shares of the
/// limit; and, outside faults, the hot keys' admitted share.
#[derive(Clone, Debug, Default, PartialEq)]
struct Measured {
    over: BTreeMap<String, (f64, f64)>,
    rate: (f64, u64),
    permits: (f64, u64),
    few: (f64, u64),
    fallback: u64,
    admitted: u64,
    /// Per set of faults in effect, the hot key's units taken on the fallback, and all of them.
    hot: BTreeMap<String, (u64, u64)>,
}

struct World {
    sim: Simulator<Msg>,
    hosts: Vec<Host>,
    index: BTreeMap<Arc<str>, NodeId>,
    truth: Vec<(Duration, Arc<View>)>,
    keys: Vec<Vec<u8>>,
    ids: Vec<u64>,
    tallies: Vec<Tally>,
    episodes: BTreeMap<usize, Vec<Episode>>,
    /// Partitions: `k − 1` times the limit.
    sides: Vec<Episode>,
    /// Each fault from its start to the end of its row's how long, to name what an excess happened under.
    active: Vec<(Fault, Duration, Duration)>,
    regions: usize,
    hot: Vec<bool>,
    zipf: Vec<f64>,
    next_move: Duration,
    moved_at: Duration,
    faulty_until: Duration,
    healing_at: Option<Duration>,
    /// Per node, its side of the partition or cut in effect.
    side: Vec<usize>,
    /// Per node and candidate in its view, when the candidate last could not answer it.
    out: BTreeMap<(NodeId, NodeId), Duration>,
    next_sample: Duration,
    faults: bool,
    log: Vec<String>,
    measured: Measured,
    rig: Option<Rig>,
    /// `(node, peer)`: the node's view shows the peer unreachable.
    cut: BTreeSet<(NodeId, NodeId)>,
    /// Per node, the hot rate's units admitted this second on a grant and on the fallback, logged each second in `Rig`.
    second: Vec<[u64; 2]>,
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn tally(limit: usize, key: usize) -> usize {
    limit * KEYS + key
}

impl World {
    /// The 50 nodes and random faults, or with `rig` the rig's episode (`Rig`) on its five nodes over sessions, which
    /// neither lose nor duplicate.
    fn new(seed: u64, rig: bool) -> Self {
        let mut sim = Simulator::new(SimConfig {
            seed,
            base_delay: ms(1),
            jitter: if rig { ms(2) } else { ms(20) },
            drop_every: if rig { 0 } else { 50 },
            duplicate_every: if rig { 0 } else { 50 },
            ordered: rig,
            max_trace: 1024,
            ..SimConfig::default()
        });
        let (nodes, regions) = if rig { (5, 1) } else { (NODES, 3 + seed as usize % 2) };
        let hosts: Vec<Host> = (0..nodes)
            .map(|i| {
                sim.add_node(&format!("n{i}")).expect("50 nodes fit");
                let region = if i < 4 * regions { i % regions } else { sim.pick(0..regions as u64) as usize };
                Host { name: format!("n{i}").into(), region, proc: None, incs: 0, up_at: None, down_since: None }
            })
            .collect();
        let mut delays = BTreeMap::new();
        for a in 0..nodes {
            for b in a + 1..nodes {
                let (ra, rb) = (hosts[a].region.min(hosts[b].region), hosts[a].region.max(hosts[b].region));
                let delay = match ra == rb {
                    true => ms(1),
                    false => *delays.entry((ra, rb)).or_insert_with(|| ms(20 + sim.pick(0..60))),
                };
                sim.set_link(a, b, Wire { up: true, delay: Some(delay) }).expect("known nodes");
            }
        }
        let keys: Vec<Vec<u8>> =
            (0..KEYS).map(|k| if k == 0 { b"hot".to_vec() } else { format!("k{k}").into_bytes() }).collect();
        let ids = (0..LIMITS.len() * KEYS).map(|t| entry_id(NS, &LIMITS[t / KEYS], &keys[t % KEYS])).collect();
        let weights: Vec<f64> = (1..=ZIPF).map(|k| (k as f64).powf(-1.1)).collect();
        let total: f64 = weights.iter().sum();
        let zipf = weights.iter().scan(0.0, |sum, w| Some(*sum + w / total).inspect(|s| *sum = *s)).collect();
        let mut hot = vec![false; regions];
        hot[sim.pick(0..regions as u64) as usize] = true;
        let mut w = Self {
            sim,
            index: hosts.iter().enumerate().map(|(i, h)| (h.name.clone(), i)).collect(),
            hosts,
            truth: Vec::new(),
            keys,
            ids,
            tallies: (0..LIMITS.len() * KEYS).map(|_| Tally::default()).collect(),
            episodes: BTreeMap::new(),
            sides: Vec::new(),
            active: Vec::new(),
            regions,
            hot,
            zipf,
            next_move: Duration::ZERO,
            moved_at: Duration::ZERO,
            faulty_until: Duration::ZERO,
            healing_at: None,
            side: vec![0; nodes],
            out: BTreeMap::new(),
            next_sample: SAMPLE,
            faults: true,
            log: Vec::new(),
            measured: Measured::default(),
            rig: None,
            cut: BTreeSet::new(),
            second: vec![[0; 2]; nodes],
        };
        w.next_move = ms(6000 + w.sim.pick(0..6000));
        // The cluster formed before the limits are used: views disagree from the first membership change on.
        let members = w.hosts.iter().map(|h| (h.name.clone(), (1, h.region))).collect();
        w.truth.push((Duration::ZERO, Arc::new(View::new(members))));
        for h in 0..nodes {
            w.boot(h);
        }
        if rig {
            let (from, until) = (ms(12_000), ms(24_000));
            let mut caps = BTreeMap::new();
            for a in 0..nodes {
                for b in a + 1..nodes {
                    caps.insert((a, b), ms(1000 + w.sim.pick(0..5000)));
                }
            }
            let victim = w.sim.pick(0..nodes as u64) as usize;
            let crash = from + ms(1000 + w.sim.pick(0..2000));
            let replaced = crash + ms(2000);
            let up = replaced + ms(1000 + w.sim.pick(0..7000));
            let flap = 8;
            let noticed = (0..nodes).map(|_| ms(200 + w.sim.pick(0..1300))).collect();
            w.log.push(format!(
                "links full {from:?}..{until:?}, {} killed at {crash:?}, replaced at {replaced:?}, Quotas again at \
                 {up:?}, reachability flips one in {flap} per 500 ms",
                w.hosts[victim].name
            ));
            w.active.push((Fault::Slow, from, until + ms(2000) + LIMITS[WEIGHT].window + LEASE));
            // §5: reports queued for seconds leave each replica dividing the limit among the nodes it heard from. A
            // node admits on the smaller grant of two of the three replicas, at most their mean, and each replica's
            // grants sum to at most the limit: together at most 3/2 of it, until the queues drain and the usage
            // admitted on those grants leaves the window.
            let drained = until + caps.values().max().copied().unwrap_or_default() + HEARTBEAT;
            for t in 0..w.tallies.len() {
                let Limit { max, window, .. } = LIMITS[t / KEYS];
                w.episode(t, from, drained + window.mul_f64(1.0 + DRIFT), max.div_ceil(2));
            }
            w.faulty_until = until + ms(2000) + SETTLE;
            w.rig = Some(Rig { from, until, caps, victim, crash, replaced, up, flap, noticed, crashed: false });
        }
        w
    }

    /// `Rig`'s episode, each tick.
    fn script(&mut self, real: Duration) {
        let Some(rig) = self.rig.as_mut() else { return };
        let full = real >= rig.from && real < rig.until;
        for (&(a, b), cap) in &rig.caps {
            // A full queue grows a second each second until the sender's window holds it; ordered, it drains in order.
            let delay = if full { (real - rig.from).min(*cap).max(ms(1)) } else { ms(1) };
            self.sim.set_link(a, b, Wire { up: true, delay: Some(delay) }).expect("known nodes");
        }
        let (victim, n) = (rig.victim, self.hosts.len());
        if rig.crashed && real < rig.replaced {
            for h in (0..n).filter(|h| real >= rig.crash + rig.noticed[*h]) {
                self.cut.insert((h, victim));
            }
        }
        if real >= rig.replaced && real < rig.replaced + TICK {
            self.cut.retain(|(_, o)| *o != victim);
        }
        let flapping = real >= rig.from + ms(1000) && real < rig.until + ms(2000);
        if flapping && real.as_millis().is_multiple_of(500) {
            for h in 0..n {
                for o in (0..n).filter(|o| *o != h) {
                    if self.sim.pick(0..rig.flap) == 0 && !self.cut.remove(&(h, o)) {
                        self.cut.insert((h, o));
                    }
                }
            }
        }
        if real >= rig.until + ms(2000) && real < rig.until + ms(2000) + TICK {
            self.cut.clear();
        }
        if !rig.crashed && real >= rig.crash {
            rig.crashed = true;
            let (up, replaced) = (rig.up, rig.replaced);
            self.crash(victim, real, up - real);
            // Its next process replaces it in the views then; that one runs Quotas only at `up`.
            self.hosts[victim].down_since = Some(replaced - DETECT);
        }
    }

    fn view(&self) -> Arc<View> {
        self.truth.last().expect("booted").1.clone()
    }

    fn set_truth(&mut self, change: impl FnOnce(&mut BTreeMap<Arc<str>, (u128, usize)>)) {
        let mut members = self.truth.last().map(|t| t.1.members.clone()).unwrap_or_default();
        change(&mut members);
        self.truth.push((self.sim.now(), Arc::new(View::new(members))));
    }

    fn boot(&mut self, h: usize) {
        let real = self.sim.now();
        let clock =
            Clock { ppm: self.sim.pick(0..2 * PPM + 1) as i64 - PPM as i64, base: ms(self.sim.pick(0..1_000_000)) };
        let lag = ms(self.sim.pick(0..LAG_MS));
        let stagger = ms(self.sim.pick(0..100));
        let host = &mut self.hosts[h];
        host.incs += 1;
        let (name, inc, region) = (host.name.clone(), host.incs, host.region);
        self.set_truth(|m| {
            m.insert(name, (inc, region));
        });
        let view = self.view();
        let now = clock.now(real);
        let table = Table::new(NS, lease_hold(LEASE, DRIFT), CAPACITY);
        let node = Node::new(&self.hosts[h].name, inc, view.quota.clone(), &table, LEASE, now);
        let proc = Proc {
            inc,
            clock,
            lag,
            table,
            node,
            view: self.truth.len() - 1,
            cut: BTreeSet::new(),
            next_report: now + stagger,
            permits: Vec::new(),
            fell: VecDeque::new(),
            fresh: real,
            first: BTreeMap::new(),
            left: VecDeque::new(),
            paused_until: None,
            inbox: Vec::new(),
            waiting: [Vec::new(), Vec::new(), Vec::new()],
        };
        let host = &mut self.hosts[h];
        (host.proc, host.up_at, host.down_since) = (Some(proc), None, None);
        if inc > 1 {
            self.sim.set_node_down(h, false).expect("a known node");
            self.log.push(format!("{real:?} {}#{inc} up", self.hosts[h].name));
        }
    }

    fn run(&mut self) {
        let mut next_tick = TICK;
        while self.sim.now() < RUN {
            if self.sim.next_delivery_at().is_some_and(|at| at <= next_tick) {
                if let Some(d) = self.sim.step() {
                    self.deliver(d.from, d.to, d.payload);
                }
            } else {
                self.sim.advance_idle_to(next_tick).expect("nothing arrives before the tick");
            }
            if self.sim.now() >= next_tick {
                next_tick += TICK;
                self.tick();
            }
        }
    }

    /// Whether a view shows either of them unreachable from the other.
    fn cut_between(&self, a: NodeId, b: NodeId) -> bool {
        self.cut.contains(&(a, b)) || self.cut.contains(&(b, a))
    }

    fn send(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        // A view that shows a peer unreachable lost its session to it: nothing goes either way until it redials.
        if self.cut_between(from, to) {
            return;
        }
        let _ = self.sim.send(from, to, msg);
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        let Some(p) = self.hosts[to].proc.as_mut() else { return };
        if p.paused_until.is_some() {
            p.inbox.push((from, msg));
        } else {
            self.handle(from, to, msg);
        }
    }

    fn handle(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        // Nor does what was queued on the session it lost.
        if self.cut_between(from, to) {
            return;
        }
        let real = self.sim.now();
        let sender = self.hosts[from].name.clone();
        // The driver gives up on an answer after these, and drops it when it comes.
        let timeouts = self.rig.is_some();
        let late = |at: Duration, patience: Duration| timeouts && real > at + patience;
        let Some(p) = self.hosts[to].proc.as_mut() else { return };
        let now = p.clock.now(real);
        match msg {
            Msg::Report { incarnation, link, body, at } => {
                let Some(Request::Report(report)) = decode_request(body, incarnation) else {
                    panic!("a report decodes")
                };
                if let Some(grants) = p.node.answer(&sender, &report, now) {
                    self.send(to, from, Msg::Grants { link, body: encode_grants(&grants), at });
                }
                self.push(to, now);
            }
            Msg::Grants { at, .. } | Msg::Ack { at, .. } if late(at, HEARTBEAT) => {}
            Msg::Page { at, .. } if late(at, PATIENCE) => {}
            Msg::Grants { link, body, .. } => {
                let grants = decode_grants(body).expect("grants decode");
                // An answer to an earlier process of this host names a link of its table, as a session would not.
                if p.table.replicas().get(link) == Some(&sender) {
                    p.table.apply(link, &grants, |id| p.node.members.candidates.serves(id, &sender), now);
                    assert!(p.table.copies_agree(), "a check reads the links as they are");
                }
            }
            Msg::Push { to: inc, body, at } => {
                let Some(Request::Push(grants)) = decode_request(body, 0) else { panic!("a push decodes") };
                // `request_to` reaches only the process it names.
                if inc != p.inc {
                    return;
                }
                let members = p.node.members.clone();
                let replicates = |id| members.candidates.serves(id, &sender);
                if let Some((replica, acked)) = p.table.pushed(&sender, &grants, replicates, now) {
                    assert!(p.table.copies_agree(), "a check reads the links as they are");
                    let incarnation = p.inc;
                    self.send(to, from, Msg::Ack { incarnation, body: encode_ack(replica, acked), at });
                }
            }
            Msg::Ack { incarnation, body, .. } => {
                let (replica, acked) = decode_ack(body).expect("an ack decodes");
                p.node.ack(&(sender, incarnation), replica, acked, now);
                self.push(to, now);
            }
            Msg::Pull { round, body, at } => {
                let Some(Request::Pull { start, regions }) = decode_request(body, 0) else { panic!("a pull decodes") };
                let (rows, next) = p.node.serve_pull(&sender, regions, start, now);
                self.send(to, from, Msg::Page { round, body: encode_page(&rows, next), at });
            }
            Msg::Page { round, body, .. } => {
                p.node.page(&sender, round, Some(decode_page(body).expect("a page decodes")), now);
            }
        }
    }

    fn tick(&mut self) {
        let real = self.sim.now();
        for h in 0..self.hosts.len() {
            let host = &self.hosts[h];
            if host.up_at.is_some_and(|at| real >= at) {
                self.boot(h);
            } else if host.down_since.is_some_and(|at| real >= at + DETECT) && host.proc.is_none() {
                let (name, inc) = (host.name.clone(), host.incs);
                self.hosts[h].down_since = None;
                self.set_truth(|m| {
                    m.remove(&name);
                });
                self.log.push(format!("{real:?} {name}#{inc} leaves the views"));
            }
        }
        if self.faults {
            self.fault(real);
        }
        if real >= self.next_move {
            self.next_move = real + ms(6000 + self.sim.pick(0..6000));
            self.moved_at = real;
            let first = self.sim.pick(0..self.regions as u64) as usize;
            let second = self.sim.pick(0..self.regions as u64) as usize;
            self.hot = (0..self.regions).map(|r| r == first || r == second).collect();
            self.log.push(format!("{real:?} demand in {:?}", self.hot));
        }
        for h in 0..self.hosts.len() {
            self.step(h, real);
        }
        self.mark_out(real);
        if real >= self.next_sample {
            self.next_sample += SAMPLE;
            self.sample(real);
        }
        if self.rig.is_some() && real.as_millis().is_multiple_of(1000) {
            self.log_second(real);
        }
    }

    /// `Rig`: per node, the hot rate's units admitted in the last second on a grant and on the fallback, and what it
    /// admits against now, with the replicas its view shows.
    fn log_second(&mut self, real: Duration) {
        let id = self.ids[tally(REQ, 0)];
        let [granted, fell] = self.second.iter().fold([0, 0], |[g, f], s| [g + s[0], f + s[1]]);
        let mut line = format!("{real:?} req/s of the hot key: {granted} on grants, {fell} on the fallback;");
        for (h, host) in self.hosts.iter().enumerate() {
            let [granted, fell] = std::mem::take(&mut self.second[h]);
            let Some(p) = host.proc.as_ref() else {
                line += &format!(" {} down;", host.name);
                continue;
            };
            let now = p.clock.now(real);
            let view = &self.truth[p.view].1;
            let replicas: Vec<&str> = view.quota.candidates.replicas(id).iter().map(|r| &***r).collect();
            let young = if p.node.replica.young(now) { " young" } else { "" };
            line += &format!(
                " {}#{}{young} {granted}+{fell} on {:?}, replicas {replicas:?}, cut from {:?};",
                host.name,
                p.inc,
                p.table.bound(&self.keys[0], &LIMITS[REQ], now),
                self.cut.range((h, 0)..(h + 1, 0)).map(|(_, o)| &*self.hosts[*o].name).collect::<Vec<_>>(),
            );
        }
        self.log.push(line);
    }

    fn step(&mut self, h: usize, real: Duration) {
        let Some(p) = self.hosts[h].proc.as_mut() else { return };
        if let Some(until) = p.paused_until {
            if real < until {
                return;
            }
            (p.paused_until, p.fresh) = (None, real);
            for (from, msg) in std::mem::take(&mut p.inbox) {
                self.handle(from, h, msg);
            }
        }
        let cut: BTreeSet<Arc<str>> =
            self.cut.range((h, 0)..(h + 1, 0)).map(|(_, o)| self.hosts[*o].name.clone()).collect();
        let p = self.hosts[h].proc.as_mut().expect("up");
        let seen = self.truth.iter().rposition(|(at, _)| *at + p.lag <= real).unwrap_or(0);
        if seen > p.view || cut != p.cut {
            let seen = seen.max(p.view);
            self.see(h, seen, cut);
        }
        let p = self.hosts[h].proc.as_mut().expect("up");
        let now = p.clock.now(real);
        let released: Vec<(usize, Duration, bool)> = p.permits.extract_if(.., |(_, at, _)| *at <= real).collect();
        for (t, _, fallback) in released {
            p.table.release(self.ids[t], 1, now);
            self.tallies[t].drop_permit(fallback);
        }
        self.demand(h, real);
        let p = self.hosts[h].proc.as_mut().expect("up");
        if now >= p.next_report {
            p.next_report = now + REPORT_EVERY;
            self.report(h, now);
        }
    }

    /// The process's view moves on to `seen` in the truth's history, showing the nodes `cut` unreachable.
    fn see(&mut self, h: usize, seen: usize, cut: BTreeSet<Arc<str>>) {
        let real = self.sim.now();
        let p = self.hosts[h].proc.as_mut().expect("up");
        let (old, new) = (self.truth[p.view].1.clone(), self.truth[seen].1.clone());
        let now = p.clock.now(real);
        p.view = seen;
        p.left.retain(|(at, _)| real < *at + GRACE);
        p.left.push_back((real, old.clone()));
        p.node.see(&p.table, new.quota.clone().unreachable(cut.iter().map(|n| &**n)), now);
        p.cut = cut;
    }

    /// A report cycle: the reports, and the pages a young replica asks for.
    fn report(&mut self, h: usize, now: Duration) {
        let at = self.sim.now();
        let p = self.hosts[h].proc.as_mut().expect("up");
        let mut out = Vec::new();
        for (r, link, report) in p.node.round(&p.table, now) {
            out.push((self.index[&r], Msg::Report { incarnation: p.inc, link, body: encode_report(&report), at }));
        }
        self.push(h, now);
        let p = self.hosts[h].proc.as_mut().expect("up");
        assert!(p.table.copies_agree(), "a check reads the links as they are");
        let (round, pulls) = p.node.pulls(now);
        for (peer, start) in pulls {
            let body = encode_pull(start, p.node.members.candidates.regions());
            out.push((self.index[&peer], Msg::Pull { round, body, at }));
        }
        for (to, msg) in out {
            self.send(h, to, msg);
        }
    }

    /// The grants host `h`'s replica has to push, each to the process its view shows; as the driver, its own at once.
    fn push(&mut self, h: usize, now: Duration) {
        let at = self.sim.now();
        let p = self.hosts[h].proc.as_mut().expect("up");
        for ((node, inc), grants) in p.node.pushes(&p.table, now) {
            self.send(h, self.index[&node], Msg::Push { to: inc, body: encode_push(&grants), at });
        }
    }

    /// The requests of one tick: the hot key where demand is, twice what its limits allow, and Zipf keys everywhere.
    fn demand(&mut self, h: usize, real: Duration) {
        let region = self.hosts[h].region;
        let active = self.hosts.iter().filter(|h| self.hot[h.region] && h.proc.is_some()).count().max(1) as f64;
        let tick = TICK.as_secs_f64();
        // Permits are held 1.25 s on average.
        for (i, (l, rate)) in [(REQ, 1.0), (WS, 1.25), (FEW, 1.25)].into_iter().enumerate() {
            let new = match self.hot[region] {
                true => self.draw(2.0 * LIMITS[l].max as f64 / rate * tick / active),
                false => 0,
            };
            // New clients try at once, refused ones after the refusal's `retry_after`; where demand is not, they give up.
            let p = self.hosts[h].proc.as_mut().expect("up");
            let waiting = &mut p.waiting[i];
            if !self.hot[region] {
                waiting.clear();
            }
            let due = new as usize + waiting.extract_if(.., |at| *at <= real).count();
            for _ in 0..due {
                if let Err(denied) = self.take(h, 0, &[(l, 1)], real) {
                    let waiting = &mut self.hosts[h].proc.as_mut().expect("up").waiting[i];
                    if waiting.len() < BACKLOG {
                        waiting.push(real + denied.retry_after());
                    }
                }
            }
        }
        for _ in 0..self.draw(0.3) {
            let u = self.sim.pick(0..1 << 30) as f64 / (1u64 << 30) as f64;
            let key = 1 + self.zipf.partition_point(|c| *c < u).min(ZIPF - 1);
            let weight = 1 + self.sim.pick(0..20);
            if self.take(h, key, &[(REQ, 1), (WEIGHT, weight)], real).is_ok() && self.sim.pick(0..10) == 0 {
                let extra = 1 + self.sim.pick(0..30);
                self.charge(h, key, extra, real);
            }
        }
    }

    fn draw(&mut self, mean: f64) -> u64 {
        mean as u64 + ((self.sim.pick(0..1 << 20) as f64) < mean.fract() * (1 << 20) as f64) as u64
    }

    fn take(&mut self, h: usize, key: usize, costs: &[(usize, u64)], real: Duration) -> Result<(), Denied> {
        let hold = ms(500 + self.sim.pick(0..1500));
        let p = self.hosts[h].proc.as_mut().expect("up");
        let now = p.clock.now(real);
        let bounds: Vec<Option<bool>> =
            costs.iter().map(|(l, cost)| p.table.falls(&self.keys[key], &LIMITS[*l], *cost, now)).collect();
        let limits: Vec<(Limit, u64)> = costs.iter().map(|(l, cost)| (LIMITS[*l], *cost)).collect();
        for (l, _) in costs {
            p.first.entry(tally(*l, key)).or_insert(real);
        }
        p.table.take(&self.keys[key], &limits, now)?;
        for (&(l, cost), bound) in costs.iter().zip(bounds) {
            let (fallback, t) = (bound == Some(true), tally(l, key));
            let p = self.hosts[h].proc.as_mut().expect("up");
            if LIMITS[l].window.is_zero() {
                p.permits.push((t, real + hold, fallback));
                let t = &mut self.tallies[t];
                (t.held, t.held_fallback) = (t.held + cost, t.held_fallback + fallback as u64 * cost);
            } else {
                if fallback {
                    p.fell.push_back((real, t, cost));
                }
                self.admit(t, real, cost, if fallback { Kind::Fallback } else { Kind::Grant });
            }
            if fallback {
                self.check_fallback(h, t, real);
                self.check_fallback_excused(h, t, real);
            }
            if key == 0 && l == REQ {
                self.second[h][fallback as usize] += cost;
            }
            if key == 0 {
                let hot = self.measured.hot.entry(self.faults_at(real)).or_default();
                (hot.0, hot.1) = (hot.0 + fallback as u64 * cost, hot.1 + cost);
            }
            self.measured.fallback += fallback as u64 * cost;
            self.measured.admitted += cost;
            self.check(t, real);
        }
        Ok(())
    }

    fn charge(&mut self, h: usize, key: usize, extra: u64, real: Duration) {
        let p = self.hosts[h].proc.as_mut().expect("up");
        p.table.charge(&self.keys[key], &[(LIMITS[WEIGHT], extra)], p.clock.now(real));
        self.admit(tally(WEIGHT, key), real, extra, Kind::Charge);
        self.check(tally(WEIGHT, key), real);
    }

    fn admit(&mut self, t: usize, real: Duration, units: u64, kind: Kind) {
        let tally = &mut self.tallies[t];
        tally.admits.push_back((real, units, kind));
        tally.sum += units;
        match kind {
            Kind::Fallback => tally.fallback += units,
            Kind::Charge => tally.charged += units,
            Kind::Grant => {}
        }
    }

    /// What a key admitted in the window, or holds: all of it, and the part taken on the fallback or charged.
    fn used(&mut self, t: usize, real: Duration) -> (u64, u64, u64) {
        let window = LIMITS[t / KEYS].window;
        let tally = &mut self.tallies[t];
        if window.is_zero() {
            return (tally.held, tally.held_fallback, 0);
        }
        while let Some(&(_, units, kind)) = tally.admits.front().filter(|a| a.0 + window <= real) {
            tally.admits.pop_front();
            tally.sum -= units;
            match kind {
                Kind::Fallback => tally.fallback -= units,
                Kind::Charge => tally.charged -= units,
                Kind::Grant => {}
            }
        }
        (tally.sum, tally.fallback, tally.charged)
    }

    /// The units over the limit that the faults in effect allow.
    fn allowance(&self, t: usize, real: Duration) -> u64 {
        let Limit { max, window, .. } = LIMITS[t / KEYS];
        let on = |e: &&Episode| e.from <= real && real < e.until;
        let own: u64 = self.episodes.get(&t).map_or(0, |es| es.iter().filter(on).map(|e| e.units).sum());
        // A side's grants hold a lease past the heal, and what it admitted stays a window.
        let side = |e: &&Episode| e.from <= real && real < e.until.saturating_add(window);
        own + self.sides.iter().filter(side).map(|e| e.units * max).sum::<u64>()
    }

    fn faults_at(&self, real: Duration) -> String {
        let on: BTreeSet<Fault> =
            self.active.iter().filter(|(_, from, until)| *from <= real && real < *until).map(|a| a.0).collect();
        if on.is_empty() { "none".into() } else { on.iter().map(|f| format!("{f:?}")).collect::<Vec<_>>().join("+") }
    }

    fn check(&mut self, t: usize, real: Duration) {
        let (all, fallback, charged) = self.used(t, real);
        let limit = LIMITS[t / KEYS];
        let over = all.saturating_sub(limit.max + fallback + charged);
        let allowed = self.allowance(t, real);
        assert!(
            over <= allowed,
            "{} of key {}: {all} in use, {fallback} of it on the fallback and {charged} charged, over {} by {over} \
             where the faults allow {allowed}, at {real:?} under {}\n{}",
            limit.name,
            t % KEYS,
            limit.max,
            self.faults_at(real),
            self.log.join("\n")
        );
    }

    /// §2.5: a process admits at most `a = ceil(M / N)` on the fallback, over its window or held. Its ring counts a
    /// window of its own clock, which runs within `DRIFT` of real time.
    fn check_fallback(&mut self, h: usize, t: usize, real: Duration) {
        let p = self.hosts[h].proc.as_ref().expect("up");
        let Limit { name, max, window } = LIMITS[t / KEYS];
        let a = max.div_ceil(self.truth[p.view].1.members.len() as u64);
        let p = self.hosts[h].proc.as_mut().expect("up");
        let fell = if window.is_zero() {
            p.permits.iter().filter(|(of, _, fallback)| *of == t && *fallback).count() as u64
        } else {
            while p.fell.front().is_some_and(|f| f.0 + LIMITS[WEIGHT].window <= real) {
                p.fell.pop_front();
            }
            let since = real.saturating_sub(window.mul_f64(1.0 - DRIFT));
            p.fell.iter().filter(|f| f.1 == t && f.0 > since).map(|f| f.2).sum()
        };
        assert!(
            fell <= a,
            "{} took {fell} of {name} of key {} on the fallback where a = {a}, at {real:?}\n{}",
            self.hosts[h].name,
            t % KEYS,
            self.log.join("\n")
        );
    }

    /// Every candidate that cannot answer a node now: down, paused, young, pulling, cut off, or not the incarnation
    /// the node's view shows.
    fn mark_out(&mut self, real: Duration) {
        // Answers that wait past the driver's timeout are dropped: while the links are full or drain, none counts.
        let slow = self.rig.as_ref().is_some_and(|r| {
            real >= r.from && real < r.until + r.caps.values().max().copied().unwrap_or_default() + HEARTBEAT
        });
        for h in 0..self.hosts.len() {
            let Some(p) = self.hosts[h].proc.as_ref().filter(|p| p.paused_until.is_none()) else { continue };
            let view = &self.truth[p.view].1;
            for c in view.quota.candidates.all() {
                let r = self.index[c];
                let answers = self.hosts[r].proc.as_ref().is_some_and(|q| {
                    q.paused_until.is_none()
                        && !q.node.replica.young(q.clock.now(real))
                        && !q.node.replica.pulling()
                        && view.members.get(c).is_some_and(|m| m.0 == q.inc)
                });
                if !answers || slow || self.cut_between(h, r) || self.side[h] != self.side[r] {
                    self.out.insert((h, r), real);
                }
            }
        }
    }

    /// §2.5: a node admits on the fallback only where fewer than a majority of the key's replicas in its view could
    /// answer it lately, where they moved in its view lately, or where it is new to the key or back.
    fn check_fallback_excused(&self, h: usize, t: usize, real: Duration) {
        let p = self.hosts[h].proc.as_ref().expect("up");
        let view = &self.truth[p.view].1;
        let replicas = view.quota.candidates.replicas(self.ids[t]);
        let answer = replicas
            .iter()
            .filter(|r| self.out.get(&(h, self.index[**r])).is_none_or(|at| real >= *at + GRACE))
            .count();
        let recent = |at: Duration| real < at + GRACE;
        let excused = answer <= replicas.len() / 2
            || recent(p.fresh)
            || p.first.get(&t).is_some_and(|at| recent(*at))
            || p.left.iter().any(|(at, old)| recent(*at) && old.quota.candidates.replicas(self.ids[t]) != replicas);
        assert!(
            excused,
            "{} took {} of key {} on the fallback at {real:?} where {answer} of its replicas {replicas:?} could \
             answer it for {GRACE:?}\n{}",
            self.hosts[h].name,
            LIMITS[t / KEYS].name,
            t % KEYS,
            self.log.join("\n")
        );
    }

    /// Adds up the excess per set of faults in effect, and the hot keys' share where nothing is at fault.
    fn sample(&mut self, real: Duration) {
        let faults = self.faults_at(real);
        for t in 0..self.tallies.len() {
            let (all, fallback, charged) = self.used(t, real);
            let max = LIMITS[t / KEYS].max;
            let over = all.saturating_sub(max + fallback + charged) as f64 / max as f64;
            if over > 0.0 {
                let entry = self.measured.over.entry(faults.clone()).or_default();
                (entry.0, entry.1) = (entry.0.max(over), entry.1 + over * SAMPLE.as_secs_f64());
            }
        }
        let quiet = real >= self.faulty_until && real >= self.moved_at + MOVE_SETTLE && real >= WARM;
        if quiet && [REQ, WS, FEW].iter().all(|l| self.allowance(tally(*l, 0), real) == 0) {
            let [rate, held, few] =
                [REQ, WS, FEW].map(|l| self.used(tally(l, 0), real).0 as f64 / LIMITS[l].max as f64);
            for (m, share) in
                [(&mut self.measured.rate, rate), (&mut self.measured.permits, held), (&mut self.measured.few, few)]
            {
                (m.0, m.1) = (m.0 + share, m.1 + 1);
            }
        }
    }

    fn episode(&mut self, t: usize, from: Duration, until: Duration, units: u64) {
        if units > 0 {
            self.episodes.entry(t).or_default().push(Episode { from, until, units });
        }
    }

    fn fault(&mut self, real: Duration) {
        if self.rig.is_some() {
            return self.script(real);
        }
        if self.healing_at.is_some_and(|at| real >= at) {
            self.heal(real);
        }
        if real >= FAULTS_END {
            self.faults = false;
            self.heal(real);
            for h in 0..NODES {
                if self.hosts[h].proc.is_none() {
                    self.hosts[h].up_at = Some(real);
                } else if let Some(p) = self.hosts[h].proc.as_mut().filter(|p| p.paused_until.is_some()) {
                    p.paused_until = Some(real);
                }
            }
            return;
        }
        if real < WARM || self.sim.pick(0..50) != 0 {
            return;
        }
        let left = FAULTS_END - real;
        let h = self.sim.pick(0..NODES as u64) as usize;
        match self.sim.pick(0..12) {
            0..=2 => {
                let down = ms(500 + self.sim.pick(0..5500)).min(left);
                self.crash(h, real, down);
            }
            3..=4 => {
                let replicas = self.hot_replicas();
                let h = replicas[self.sim.pick(0..replicas.len() as u64) as usize];
                let down = ms(500 + self.sim.pick(0..5500)).min(left);
                self.crash(h, real, down);
            }
            5..=7 => {
                let d = [300, 1000, 2000, 3000, 6000, 20_000][self.sim.pick(0..6) as usize];
                self.pause(h, real, ms(d).min(left));
            }
            8..=9 if self.healing_at.is_none() => self.partition(real, left),
            10 if self.healing_at.is_none() => {
                let replicas = self.view().quota.candidates.all().map(|r| self.index[r]).collect::<Vec<_>>();
                let h = replicas[self.sim.pick(0..replicas.len() as u64) as usize];
                let others: Vec<NodeId> =
                    (0..NODES).filter(|o| self.hosts[*o].region != self.hosts[h].region).collect();
                self.sim.partition(&[h], &others).expect("known nodes");
                self.side = (0..NODES).map(|n| (self.hosts[n].region != self.hosts[h].region) as usize).collect();
                let heal = real + ms(2000 + self.sim.pick(0..4000)).min(left);
                self.healing_at = Some(heal);
                self.active.push((Fault::Cut, real, heal + LIMITS[WEIGHT].window + LEASE));
                self.faulty_until = self.faulty_until.max(heal + SETTLE);
                self.log.push(format!("{real:?} {} cut from the other regions", self.hosts[h].name));
            }
            11 if self.sim.pick(0..3) == 0 => {
                for h in self.hot_replicas() {
                    let down = ms(500 + self.sim.pick(0..2500)).min(left);
                    self.crash(h, real, down);
                }
            }
            _ => {}
        }
    }

    /// The nodes of the replicas of the hot key's rate.
    fn hot_replicas(&self) -> Vec<NodeId> {
        self.view().quota.candidates.replicas(self.ids[tally(REQ, 0)]).iter().map(|r| self.index[*r]).collect()
    }

    /// The rate grants a table admits against: per tally, its value.
    fn rate_grants(&self, table: &Table, now: Duration) -> Vec<(usize, u64)> {
        let grant = |l: usize, key: usize| match table.bound(&self.keys[key], &LIMITS[l], now) {
            Bound::Grant { value, .. } => Some((tally(l, key), value)),
            _ => None,
        };
        [REQ, WEIGHT].into_iter().flat_map(|l| (0..KEYS).filter_map(move |key| grant(l, key))).collect()
    }

    fn crash(&mut self, h: usize, real: Duration, down: Duration) {
        let Some(p) = self.hosts[h].proc.take() else { return };
        let now = p.clock.now(real);
        for (t, _, fallback) in &p.permits {
            self.tallies[*t].drop_permit(*fallback);
        }
        // §5: its unreported usage, at most an eighth of its grant, until it leaves the window.
        for (t, value) in self.rate_grants(&p.table, now) {
            let until = real + LIMITS[t / KEYS].window.mul_f64(1.0 + DRIFT);
            self.episode(t, real, until, value.div_ceil(8));
        }
        self.sim.crash_node(h).expect("a known node");
        self.sim.set_node_down(h, true).expect("a known node");
        let up = real + down;
        let host = &mut self.hosts[h];
        (host.up_at, host.down_since) = (Some(up), Some(real));
        self.active.push((Fault::Crash, real, up + ms(LAG_MS) + LIMITS[WEIGHT].window));
        self.faulty_until = self.faulty_until.max(up + SETTLE);
        self.log.push(format!("{real:?} {}#{} crashed, up in {down:?}", host.name, host.incs));
        // §5: a key none of whose replicas is left with its rows admits up to its departed usage, for a window.
        let view = self.view();
        for t in 0..self.tallies.len() {
            let lost = view.quota.candidates.replicas(self.ids[t]).iter().all(|r| {
                let host = &self.hosts[self.index[*r]];
                host.proc.as_ref().is_none_or(|p| p.node.replica.young(p.clock.now(real)))
            });
            if lost {
                let until = up.max(real + DETECT) + ms(LAG_MS) + LIMITS[t / KEYS].window + Duration::from_secs(1);
                self.episode(t, real, until, LIMITS[t / KEYS].max);
                self.active.push((Fault::Lost, real, until));
            }
        }
    }

    fn pause(&mut self, h: usize, real: Duration, d: Duration) {
        let Some(p) = self.hosts[h].proc.as_mut().filter(|p| p.paused_until.is_none()) else { return };
        p.paused_until = Some(real + d);
        // §5: a lease after the replicas last heard from it, at most a heartbeat before the pause, they count its
        // reported usage alone, and 15 s after drop its permit rows. What it took that they had not heard of, cut off
        // or lost with a report, counts over until it reports after the pause, or its rate usage leaves the window: at
        // most the permits it holds, or its rate grant.
        let freed = (real + LEASE.mul_f64(1.0 - 2.0 * DRIFT)).saturating_sub(HEARTBEAT);
        let reports = real + d + HEARTBEAT;
        let now = p.clock.now(real);
        let mut unheard: BTreeMap<usize, (Duration, u64)> = BTreeMap::new();
        for (t, ..) in &p.permits {
            unheard.entry(*t).or_insert((reports, 0)).1 += 1;
        }
        let p = self.hosts[h].proc.as_ref().expect("up");
        for (t, value) in self.rate_grants(&p.table, now) {
            unheard.insert(t, (reports.min(real + LIMITS[t / KEYS].window.mul_f64(1.0 + DRIFT)), value));
        }
        for (t, (until, units)) in unheard.into_iter().filter(|u| u.1.0 > freed) {
            self.episode(t, freed, until, units);
        }
        self.active.push((Fault::Pause, real, real + d + LIMITS[WEIGHT].window + LEASE));
        self.faulty_until = self.faulty_until.max(real + d + SETTLE);
        self.log.push(format!("{real:?} {} paused for {d:?}", self.hosts[h].name));
    }

    /// Splits the regions into `k` sides: each admits up to the limit (§5).
    fn partition(&mut self, real: Duration, left: Duration) {
        let k = 2 + self.sim.pick(0..self.regions as u64 - 1) as usize;
        let mut side: Vec<usize> = (0..self.regions).map(|r| r % k).collect();
        for r in (1..self.regions).rev() {
            side.swap(r, self.sim.pick(0..r as u64 + 1) as usize);
        }
        for a in 0..k {
            for b in a + 1..k {
                let of = |s| (0..NODES).filter(|n| side[self.hosts[*n].region] == s).collect::<Vec<_>>();
                self.sim.partition(&of(a), &of(b)).expect("known nodes");
            }
        }
        self.side = (0..NODES).map(|n| side[self.hosts[n].region]).collect();
        let heal = real + ms(2000 + self.sim.pick(0..6000)).min(left);
        self.healing_at = Some(heal);
        self.sides.push(Episode { from: real, until: Duration::MAX, units: k as u64 - 1 });
        self.active.push((Fault::Partition, real, Duration::MAX));
        self.faulty_until = self.faulty_until.max(heal + SETTLE);
        self.log.push(format!("{real:?} regions split into sides {side:?}"));
    }

    fn heal(&mut self, real: Duration) {
        if self.healing_at.take().is_none() {
            return;
        }
        self.sim.heal();
        self.side = vec![0; NODES];
        for e in self.sides.iter_mut().filter(|e| e.until == Duration::MAX) {
            e.until = real + LEASE;
        }
        for a in self.active.iter_mut().filter(|a| a.2 == Duration::MAX) {
            a.2 = real + LEASE + LIMITS[WEIGHT].window;
        }
        self.log.push(format!("{real:?} healed"));
    }

    fn finish(mut self) -> Self {
        let real = self.sim.now();
        let debug = std::env::var("SIM_DEBUG").is_ok();
        if debug {
            eprintln!("{}\n{:?}", self.log.join("\n"), self.measured);
        }
        for p in self.hosts.iter().filter_map(|h| h.proc.as_ref()) {
            assert!(
                !p.node.replica.young(p.clock.now(real)),
                "a replica still young at the end\n{}",
                self.log.join("\n")
            );
        }
        assert!(self.measured.rate.1 >= 50, "only {} quiet samples\n{}", self.measured.rate.1, self.log.join("\n"));
        let mean = |m: &mut (f64, u64)| {
            m.0 /= m.1 as f64;
            m.0
        };
        let (rate, permits, few) =
            (mean(&mut self.measured.rate), mean(&mut self.measured.permits), mean(&mut self.measured.few));
        eprintln!("measured: rate {rate:.3}, permits {permits:.3}, in fragments {few:.3}");
        // The ring counts a unit for up to 9/8 of the window, so a saturated rate admits 8/9 of its limit per window.
        let log = self.log.join("\n");
        assert!(rate >= 0.95 * 8.0 / 9.0, "the hot rate admitted {rate:.3} of its limit outside faults\n{log}");
        assert!(permits >= 0.95, "the hot permits were {permits:.3} held outside faults\n{log}");
        assert!(few >= FEW_HELD, "the hot permits in fragments were {few:.3} held outside faults\n{log}");
        self
    }
}

fn run(seed: u64, rig: bool) -> World {
    let mut w = World::new(seed, rig);
    w.run();
    w.finish()
}

/// The rig's run of 2026-10-01, `Rig`: the cluster admitted up to 2.7 times the hot rate's limit for 8 s, most of it
/// on grants near the whole limit held by several nodes at once.
#[test]
fn full_links_a_restart_inside_and_views_that_disagree_keep_a_key_within_its_limit() {
    check_seeds(axon_testkit::seeds(0..16), |seed| drop(run(seed, true))).unwrap();
}

#[test]
fn keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults() {
    let worst =
        Mutex::new(Measured { rate: (f64::MAX, 0), permits: (f64::MAX, 0), few: (f64::MAX, 0), ..Measured::default() });
    check_seeds(axon_testkit::seeds(0..2), |seed| {
        let m = run(seed, false).measured;
        let mut worst = worst.lock().expect("no panic holds it");
        for (faults, (peak, integral)) in m.over {
            let w = worst.over.entry(faults).or_default();
            (w.0, w.1) = (w.0.max(peak), w.1.max(integral));
        }
        worst.rate.0 = worst.rate.0.min(m.rate.0);
        worst.permits.0 = worst.permits.0.min(m.permits.0);
        worst.few.0 = worst.few.0.min(m.few.0);
        worst.fallback += m.fallback;
        worst.admitted += m.admitted;
        for (faults, (fallback, all)) in m.hot {
            let w = worst.hot.entry(faults).or_default();
            (w.0, w.1) = (w.0 + fallback, w.1 + all);
        }
    })
    .unwrap();
    let worst = worst.into_inner().expect("no panic holds it");
    eprintln!(
        "over the limit, beyond the fallback and charges, per faults in effect (most, integral in limit·s): {:?}; \
         outside faults the hot rate admitted at least {:.3} of the limit, and its permits were held at least {:.3}, \
         {:.3} in fragments; {:.4} of units on the fallback; the hot key's share on the fallback per faults in effect: \
         {:?}",
        worst.over,
        worst.rate.0,
        worst.permits.0,
        worst.few.0,
        worst.fallback as f64 / worst.admitted as f64,
        worst.hot.iter().map(|(f, (fell, all))| (f, format!("{:.4}", *fell as f64 / *all as f64))).collect::<Vec<_>>()
    );
}

#[test]
#[expect(clippy::disallowed_methods, reason = "the test runs real threads")]
fn same_seed_same_run() {
    // On two threads: each run takes half a minute, and nothing but the seed may pass between them.
    let (one, two) = std::thread::scope(|s| {
        let two = s.spawn(|| run(3, false));
        (run(3, false), two.join().expect("the second run"))
    });
    assert!(
        one.sim.trace() == two.sim.trace() && one.log == two.log && one.measured == two.measured,
        "seed 3 ran two ways"
    );
}
