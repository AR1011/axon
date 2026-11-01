//! A seeded network simulator for testing protocols under loss, duplication, reordering,
//! partitions and crashes. Time is virtual, so a run is instant, and the same seed, config and
//! calls give the same trace every time.
//!
//! The simulator only carries messages. Your protocol state lives outside it: send, take each
//! delivery from [`Simulator::step`], and feed it to the node it is for. Draw the model's own choices from
//! [`Simulator::pick`], so one seed replays both.
//!
//! ```
//! use std::time::Duration;
//! use axon_testkit::{SimConfig, Simulator, check_seeds};
//!
//! check_seeds(0..50, |seed| {
//!     let mut net = Simulator::new(SimConfig { seed, jitter: Duration::from_millis(5), ..Default::default() });
//!     let (a, b) = (net.add_node("a").unwrap(), net.add_node("b").unwrap());
//!     for n in 0..10u32 {
//!         net.send(a, b, n).unwrap();
//!     }
//!     let mut seen = Vec::new();
//!     net.run(100, |_, delivery| seen.push(delivery.payload));
//!     assert_eq!(seen, (0..10).collect::<Vec<_>>()); // links keep order unless `ordered` is off
//! })
//! .unwrap();
//! ```
//!
//! For a single actor, use `axon::EmbeddedDriver` with `axon::Clock::manual()` instead. For real clusters on virtual
//! time, crashed and restarted over simulated wires, use [`world::World`]. For a store's files, [`crashes`] gives what
//! a crash at each point inside a write leaves.

#![deny(clippy::unwrap_used)]
mod crash;
pub mod world;

pub use crash::{crash_points, crashed, crashes};

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZero;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

use world::lock;

pub type NodeId = usize;

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub base_delay: Duration,
    /// Each message takes `base_delay` plus up to this much more.
    pub jitter: Duration,
    /// About one message in this many is lost. 0: none.
    pub drop_every: u32,
    /// About one message in this many arrives twice. 0: none.
    pub duplicate_every: u32,
    /// Messages on one link arrive in the order sent, as on a TCP session.
    pub ordered: bool,
    pub max_nodes: usize,
    pub max_pending: usize,
    /// The trace keeps this many newest events.
    pub max_trace: usize,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            base_delay: Duration::from_millis(1),
            jitter: Duration::ZERO,
            drop_every: 0,
            duplicate_every: 0,
            ordered: true,
            max_nodes: 64,
            max_pending: 65_536,
            max_trace: 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimError {
    UnknownNode(NodeId),
    TooManyNodes,
    /// `max_pending` messages are in flight.
    Full,
    /// The time is in the past, or a delivery is due before it.
    NotIdle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    pub up: bool,
    /// Replaces `base_delay` on this link.
    pub delay: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    Lost,
    LinkDown,
    NodeDown,
    Crashed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceKind {
    Sent,
    Duplicated,
    Delivered,
    Dropped(DropReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceEvent {
    pub at: Duration,
    pub id: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub kind: TraceKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimStats {
    pub sent: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub duplicated: u64,
}

#[derive(Debug)]
pub struct Delivery<T> {
    pub at: Duration,
    pub id: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub payload: T,
}

struct Node {
    name: String,
    down: bool,
    incarnation: u64,
}

struct InFlight<T> {
    id: u64,
    from: NodeId,
    to: NodeId,
    payload: T,
}

pub struct Simulator<T> {
    cfg: SimConfig,
    rng: u64,
    now: Duration,
    next_key: u64,
    next_id: u64,
    nodes: Vec<Node>,
    links: BTreeMap<(NodeId, NodeId), Link>,
    last_arrival: BTreeMap<(NodeId, NodeId), Duration>,
    pending: BTreeMap<(Duration, u64), InFlight<T>>,
    trace: VecDeque<TraceEvent>,
    stats: SimStats,
}

fn pair(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    (a.min(b), a.max(b))
}

impl<T: Clone> Simulator<T> {
    pub fn new(cfg: SimConfig) -> Self {
        Self {
            rng: cfg.seed,
            cfg,
            now: Duration::ZERO,
            next_key: 0,
            next_id: 0,
            nodes: Vec::new(),
            links: BTreeMap::new(),
            last_arrival: BTreeMap::new(),
            pending: BTreeMap::new(),
            trace: VecDeque::new(),
            stats: SimStats::default(),
        }
    }

    pub fn add_node(&mut self, name: &str) -> Result<NodeId, SimError> {
        if self.nodes.len() >= self.cfg.max_nodes {
            return Err(SimError::TooManyNodes);
        }
        self.nodes.push(Node { name: name.into(), down: false, incarnation: 1 });
        Ok(self.nodes.len() - 1)
    }

    /// Queues a message and returns its id. A lost message is still `Ok`: the sender cannot tell.
    pub fn send(&mut self, from: NodeId, to: NodeId, payload: T) -> Result<u64, SimError> {
        self.check(from)?;
        self.check(to)?;
        if self.pending.len() >= self.cfg.max_pending {
            return Err(SimError::Full);
        }
        let id = self.next_id;
        self.next_id += 1;
        self.stats.sent += 1;
        self.record(id, from, to, TraceKind::Sent);
        if self.nodes[from].down {
            self.dropped(id, from, to, DropReason::NodeDown);
        } else if self.one_in(self.cfg.drop_every) {
            self.dropped(id, from, to, DropReason::Lost);
        } else {
            if self.pending.len() + 1 < self.cfg.max_pending && self.one_in(self.cfg.duplicate_every) {
                self.stats.duplicated += 1;
                self.record(id, from, to, TraceKind::Duplicated);
                self.enqueue(id, from, to, payload.clone());
            }
            self.enqueue(id, from, to, payload);
        }
        Ok(id)
    }

    /// Moves time to the next arrival it delivers and returns it, dropping on the way the ones a cut link or a down
    /// node loses. `None` when nothing is left to deliver.
    pub fn step(&mut self) -> Option<Delivery<T>> {
        self.arrive(Duration::MAX)
    }

    /// Arrivals before `until`: drops the lost ones and returns the first delivered.
    fn arrive(&mut self, until: Duration) -> Option<Delivery<T>> {
        while let Some(entry) = self.pending.first_entry().filter(|entry| entry.key().0 < until) {
            let ((at, _), InFlight { id, from, to, payload }) = entry.remove_entry();
            self.now = at;
            if let Some(reason) = self.lost(from, to) {
                self.dropped(id, from, to, reason);
            } else {
                self.stats.delivered += 1;
                self.record(id, from, to, TraceKind::Delivered);
                return Some(Delivery { at, id, from, to, payload });
            }
        }
        None
    }

    /// Hands up to `max` deliveries to `on_delivery`, which may send more. Returns how many it saw.
    pub fn run(&mut self, max: usize, mut on_delivery: impl FnMut(&mut Self, Delivery<T>)) -> usize {
        let mut delivered = 0;
        while delivered < max {
            let Some(delivery) = self.step() else { break };
            on_delivery(self, delivery);
            delivered += 1;
        }
        delivered
    }

    /// Moves time forward while nothing arrives, to fire a protocol timer. Arrivals before `at` that a cut link or a
    /// down node loses are dropped on the way.
    pub fn advance_idle_to(&mut self, at: Duration) -> Result<(), SimError> {
        if at < self.now || self.next_delivery_at().is_some_and(|next| next < at) {
            return Err(SimError::NotIdle);
        }
        let delivered = self.arrive(at);
        debug_assert!(delivered.is_none(), "next_delivery_at saw nothing live before {at:?}");
        self.now = at;
        Ok(())
    }

    pub fn set_link(&mut self, a: NodeId, b: NodeId, link: Link) -> Result<(), SimError> {
        self.check(a)?;
        self.check(b)?;
        self.links.insert(pair(a, b), link);
        Ok(())
    }

    /// Cuts every link between the two groups. Messages in flight across the cut are dropped on arrival.
    pub fn partition(&mut self, left: &[NodeId], right: &[NodeId]) -> Result<(), SimError> {
        left.iter().chain(right).try_for_each(|node| self.check(*node))?;
        for a in left {
            for b in right {
                let link = Link { up: false, ..self.link(*a, *b) };
                self.links.insert(pair(*a, *b), link);
            }
        }
        Ok(())
    }

    /// Brings every link back up.
    pub fn heal(&mut self) {
        self.links.values_mut().for_each(|link| link.up = true);
    }

    /// A down node loses what it sends and what arrives for it.
    pub fn set_node_down(&mut self, node: NodeId, down: bool) -> Result<(), SimError> {
        self.check(node)?;
        self.nodes[node].down = down;
        Ok(())
    }

    /// Drops everything in flight to and from the node and returns its new incarnation.
    pub fn crash_node(&mut self, node: NodeId) -> Result<u64, SimError> {
        self.check(node)?;
        let lost: Vec<_> =
            self.pending.iter().filter(|(_, m)| m.from == node || m.to == node).map(|(key, _)| *key).collect();
        for key in lost {
            let message = self.pending.remove(&key).expect("collected from pending above");
            self.dropped(message.id, message.from, message.to, DropReason::Crashed);
        }
        self.last_arrival.retain(|(from, to), _| *from != node && *to != node);
        self.nodes[node].incarnation += 1;
        Ok(self.nodes[node].incarnation)
    }

    pub fn now(&self) -> Duration {
        self.now
    }

    /// The simulator's seeded choice, for the model's own decisions, so one seed replays both.
    pub fn pick(&mut self, range: Range<u64>) -> u64 {
        pick(&mut self.rng, range)
    }

    pub fn name(&self, node: NodeId) -> &str {
        &self.nodes[node].name
    }

    pub fn incarnation(&self, node: NodeId) -> u64 {
        self.nodes[node].incarnation
    }

    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// The next arrival that the links and nodes, as they are now, deliver; one they would drop is not waited for.
    pub fn next_delivery_at(&self) -> Option<Duration> {
        // ponytail: scans past the lost arrivals, O(pending) while a cut holds many; index by link if sims grow
        self.pending.iter().find(|(_, m)| self.lost(m.from, m.to).is_none()).map(|((at, _), _)| *at)
    }

    pub fn stats(&self) -> SimStats {
        self.stats
    }

    pub fn trace(&self) -> &VecDeque<TraceEvent> {
        &self.trace
    }

    fn check(&self, node: NodeId) -> Result<(), SimError> {
        if node < self.nodes.len() { Ok(()) } else { Err(SimError::UnknownNode(node)) }
    }

    fn link(&self, a: NodeId, b: NodeId) -> Link {
        self.links.get(&pair(a, b)).copied().unwrap_or(Link { up: true, delay: None })
    }

    /// Why an arrival from `from` to `to` would be dropped now.
    fn lost(&self, from: NodeId, to: NodeId) -> Option<DropReason> {
        if !self.link(from, to).up {
            Some(DropReason::LinkDown)
        } else if self.nodes[to].down {
            Some(DropReason::NodeDown)
        } else {
            None
        }
    }

    fn enqueue(&mut self, id: u64, from: NodeId, to: NodeId, payload: T) {
        let delay = self.link(from, to).delay.unwrap_or(self.cfg.base_delay) + jitter(&mut self.rng, self.cfg.jitter);
        let mut at = self.now + delay;
        if self.cfg.ordered {
            let last = self.last_arrival.entry((from, to)).or_default();
            at = at.max(*last);
            *last = at;
        }
        self.pending.insert((at, self.next_key), InFlight { id, from, to, payload });
        self.next_key += 1;
    }

    fn dropped(&mut self, id: u64, from: NodeId, to: NodeId, reason: DropReason) {
        self.stats.dropped += 1;
        self.record(id, from, to, TraceKind::Dropped(reason));
    }

    fn record(&mut self, id: u64, from: NodeId, to: NodeId, kind: TraceKind) {
        if self.trace.len() >= self.cfg.max_trace {
            self.trace.pop_front();
        }
        if self.cfg.max_trace > 0 {
            self.trace.push_back(TraceEvent { at: self.now, id, from, to, kind });
        }
    }

    fn one_in(&mut self, n: u32) -> bool {
        n != 0 && self.random().is_multiple_of(n as u64)
    }

    fn random(&mut self) -> u64 {
        splitmix(&mut self.rng)
    }
}

/// splitmix64
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    axon::trace::mix(*state)
}

fn pick(rng: &mut u64, range: Range<u64>) -> u64 {
    assert!(!range.is_empty(), "pick from an empty range");
    range.start + splitmix(rng) % (range.end - range.start)
}

/// Up to `max`, both ends included, and no draw for a `max` of zero.
fn jitter(rng: &mut u64, max: Duration) -> Duration {
    let max = max.as_nanos() as u64;
    Duration::from_nanos(if max == 0 { 0 } else { splitmix(rng) % (max + 1) })
}

/// Fisher-Yates.
fn shuffle<T>(rng: &mut u64, items: &mut [T]) {
    for i in (1..items.len()).rev() {
        items.swap(i, (splitmix(rng) % (i as u64 + 1)) as usize);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FailedSeed {
    pub seed: u64,
    pub message: String,
}

/// Runs `check` for each seed and returns the lowest one that panics, so a failure names its seed. The seeds run at
/// most `available_parallelism` at once across the process, so libtest's own parallel tests do not multiply them, on
/// threads named after the calling one, which libtest names after the test. Every seed below a failure runs, and none
/// above it starts after it. It prints `seeds <test>: a..b` for the seeds that passed, and `, then SIM_SEED=n failed`
/// after a failure, which `scripts/check.sh sim` reports. Where the failed seed started a World's node in a build that
/// cannot replay it (without `--cfg tokio_unstable`), its message says so and how to replay it.
pub fn check_seeds(seeds: impl IntoIterator<Item = u64>, check: impl Fn(u64) + Sync) -> Result<(), FailedSeed> {
    static RUNNING: Mutex<usize> = Mutex::new(0);
    static FREED: Condvar = Condvar::new();
    let cores = std::thread::available_parallelism().map_or(1, NonZero::get);
    let test = std::thread::current().name().unwrap_or("main").to_owned();
    let seeds: Vec<u64> = seeds.into_iter().collect();
    let next = AtomicUsize::new(0);
    let failed: Mutex<Option<(usize, String)>> = Mutex::new(None);
    let run = || {
        loop {
            let mut running = lock(&RUNNING);
            while *running >= cores {
                running = FREED.wait(running).unwrap_or_else(PoisonError::into_inner);
            }
            *running += 1;
            drop(running);
            let i = next.fetch_add(1, Relaxed);
            let lower_failed = || lock(&failed).as_ref().is_some_and(|&(f, _)| f < i);
            let result = (i < seeds.len() && !lower_failed()).then(|| {
                // Left by a seed before this one on this thread.
                world::take_unseeded();
                catch_unwind(AssertUnwindSafe(|| check(seeds[i])))
            });
            *lock(&RUNNING) -= 1;
            FREED.notify_one();
            match result {
                None => return,
                Some(Ok(())) => {}
                Some(Err(panic)) => {
                    let mut message = axon::Panic::from(panic).message().unwrap_or_default().to_owned();
                    // On this thread, which ran the seed.
                    if world::take_unseeded() {
                        message = format!("{message}\n{}", world::not_replayed(seeds[i], &test));
                    }
                    let mut failed = lock(&failed);
                    if failed.as_ref().is_none_or(|&(f, _)| i < f) {
                        *failed = Some((i, message));
                    }
                }
            }
        }
    };
    std::thread::scope(|scope| {
        for _ in 0..cores.min(seeds.len()) {
            #[expect(clippy::disallowed_methods, reason = "seeds run side by side, each in its own World")]
            std::thread::Builder::new().name(test.clone()).spawn_scoped(scope, run).expect("a thread for seeds");
        }
    });
    let passed = |n: usize| (n > 0).then(|| seeds[0]..seeds[n - 1] + 1);
    match failed.into_inner().unwrap_or_else(PoisonError::into_inner) {
        Some((i, message)) => {
            println!("seeds {test}: {}, then SIM_SEED={} failed", ranged(passed(i)), seeds[i]);
            Err(FailedSeed { seed: seeds[i], message })
        }
        None => {
            println!("seeds {test}: {}", ranged(passed(seeds.len())));
            Ok(())
        }
    }
}

fn ranged(ran: Option<Range<u64>>) -> String {
    ran.map_or("none".to_owned(), |ran| format!("{ran:?}"))
}

/// Seeds for a World test: `SIM_SEED=n` runs only n, and `SEEDS=a..b` or `SEEDS=n` (`0..n`) replaces `default`,
/// for sharding across machines.
pub fn seeds(default: Range<u64>) -> Range<u64> {
    let var = |name| std::env::var(name).ok();
    let number = |s: &str| s.trim().parse::<u64>().unwrap_or_else(|_| panic!("not a seed: {s:?}"));
    if let Some(seed) = var("SIM_SEED") {
        let seed = number(&seed);
        return seed..seed + 1;
    }
    match var("SEEDS") {
        Some(seeds) => match seeds.split_once("..") {
            Some((from, to)) => number(from)..number(to),
            None => 0..number(&seeds),
        },
        None => default,
    }
}

/// Polls `check` every 20 ms until it holds, and panics naming `what` if it has not within 10 s.
pub async fn eventually(what: &str, check: impl AsyncFnMut() -> bool) {
    eventually_within(what, Duration::from_secs(10), check).await;
}

/// [`eventually`] with its own deadline, returning how long `check` took to hold.
pub async fn eventually_within(what: &str, within: Duration, mut check: impl AsyncFnMut() -> bool) -> Duration {
    let started = tokio::time::Instant::now();
    while !check().await {
        assert!(started.elapsed() < within, "{what}: not within {within:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    started.elapsed()
}
