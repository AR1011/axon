//! The two sides of Quotas (spec/quotas.md), with no I/O and no clock: every call is handed `now`, monotonic. A node's
//! [`Table`] admits against the grants it holds; a [`Replica`] adds up every node's usage of a key and hands out grants.

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Bound::{Included, Unbounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use axon::IdHash;
use parking_lot::{Mutex, MutexGuard, RwLock};

/// An id's top six bits: `Table::lock` and `Table::sweep` hold a set of shards in a `u64`.
const SHARDS: usize = 64;
/// A rate's ring: the current bucket of window/8 and the eight before it, so it reads at most one bucket high.
pub(crate) const BUCKETS: usize = 9;
/// How often a node reports what changed.
pub(crate) const REPORT_EVERY: Duration = Duration::from_millis(100);
/// How often a node reports to each replica when nothing changed.
pub(crate) const HEARTBEAT: Duration = Duration::from_secs(1);
/// How long a node keeps an entry, with its grants, after its last request: the sweep, up to `SWEEP_EVERY` and a
/// report cycle later, drops it.
const KEEP: Duration = Duration::from_secs(60);
/// An entry in use is reported at least this often, so that a replica can tell the rows no node uses any more, and one
/// holding permits is, used or not, so that a replica can tell a row its node no longer reports there.
const REFRESH: Duration = Duration::from_secs(30);
/// A row without usage that nobody reported for this long has its grant lowered to zero, and goes once that is
/// acknowledged: a node keeps an entry at most `REFRESH + KEEP + SWEEP_EVERY` and a report cycle after its last
/// report of it.
const ROW_KEEP: Duration = Duration::from_secs(120);
/// A permit row of a node silent this long is dropped.
const SILENT: Duration = Duration::from_secs(15);
/// Reports a link remembers, to date the grants that answer them and to tell those lost.
const SENT_KEPT: usize = 64;
/// Bits per generation of the evicted set: two generations make 1 MiB.
const EVICTED_BITS: usize = 1 << 22;
/// Most items, grants or rows in one message.
pub(crate) const MAX_ITEMS: usize = 4096;
/// How long a page of a pull is waited for before it is asked again.
pub(crate) const PATIENCE: Duration = Duration::from_millis(500);
/// How often a replica walks its rows for the timeouts above: about 50 ns a row, so that walking them every report
/// cycle took a core at 3 M rows (benchmarks.md § Quotas).
const WALK_EVERY: Duration = Duration::from_secs(1);
/// How long a node takes to sweep all its entries, a share each report cycle by the clock, so that long cycles do not
/// stretch it: a sweep of every shard at once took 0.8 s at 10⁷ entries (benchmarks.md § Quotas).
const SWEEP_EVERY: Duration = Duration::from_millis(6400);
/// Entries the sweep looks at per hold of a shard's lock, so that a check waits for a few µs at most: a whole shard at
/// once, 156 k entries at 10⁷, cost one thread's checks 12 % (benchmarks.md § Quotas).
const SLICE: usize = 32;

/// A limit as the cores see it. `window` is zero for permits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Limit {
    pub name: &'static str,
    pub max: u64,
    pub window: Duration,
}

/// Why `take` or `acquire` refused. Nothing was taken either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denied {
    /// The key is at its limit: answer 429 with `retry_after`.
    Limit { limit: &'static str, retry_after: Duration },
    /// The limit has room, but this node's grant is too small for the cost, which is reported as demand: `retry_after`
    /// is when this node expects the room, once its next report and a node that holds the room hand it over, or once
    /// its own usage leaves the window, if sooner.
    Share { limit: &'static str, retry_after: Duration },
}

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit { limit, retry_after } => write!(f, "{limit} is at its limit; retry after {retry_after:?}"),
            Self::Share { limit, retry_after } => {
                write!(f, "this node's share of {limit} is used; retry after {retry_after:?}")
            }
        }
    }
}

impl std::error::Error for Denied {}

impl Denied {
    /// The name of the limit that refused.
    pub fn limit(&self) -> &'static str {
        let (Self::Limit { limit, .. } | Self::Share { limit, .. }) = *self;
        limit
    }

    /// When to try again: the `Retry-After` of a 429, either way.
    pub fn retry_after(&self) -> Duration {
        let (Self::Limit { retry_after, .. } | Self::Share { retry_after, .. }) = *self;
        retry_after
    }

    /// The refusal of a take refused on two limits: at its limit where either is, and waiting for both.
    fn worse(self, other: Self) -> Self {
        let retry_after = self.retry_after().max(other.retry_after());
        match (self, other) {
            (Self::Limit { limit, .. }, _) | (Self::Share { .. }, Self::Limit { limit, .. }) => {
                Self::Limit { limit, retry_after }
            }
            (Self::Share { limit, .. }, Self::Share { .. }) => Self::Share { limit, retry_after },
        }
    }
}

/// The id of a limit's key, the same on every node with the same seed. A name names one limit: a rate and permits of one
/// name would share the entry.
pub(crate) fn entry_id(seed: u64, limit: &Limit, key: &[u8]) -> u64 {
    crate::placement::hash(seed, limit.name.bytes().chain([0]).chain(key.iter().copied()))
}

/// A view's quota nodes as replicas are chosen from them (§2.2): each region's `per_region` best scored, the regions
/// with the most first.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct Candidates {
    seed: u64,
    per_region: usize,
    regions: Vec<Vec<Arc<str>>>,
}

impl Candidates {
    /// Candidates per region unless configured: a node talks to at most this many per region.
    pub(crate) const PER_REGION: usize = 8;
    /// Regions a key has a replica in, at most.
    const REGIONS: usize = 5;
    /// Replicas a key has, at least, where there are as many candidates.
    const AT_LEAST: usize = 3;

    /// From `(node, region)` pairs. `seed` and `per_region` must be the same on every node.
    pub(crate) fn new<'a>(seed: u64, per_region: usize, members: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut by: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (node, region) in members {
            by.entry(region).or_default().push(node);
        }
        let mut regions: Vec<Vec<&str>> = by.into_values().collect();
        // By candidates, not members: a node leaving a region of more than eight must not reorder the regions.
        regions.sort_by_key(|nodes| std::cmp::Reverse(nodes.len().min(per_region)));
        let regions = regions
            .into_iter()
            .map(|mut nodes| {
                nodes.sort_by_key(|node| (std::cmp::Reverse(crate::placement::hash(seed, node.bytes())), *node));
                nodes.into_iter().take(per_region).map(Arc::from).collect()
            })
            .collect();
        Self { seed, per_region, regions }
    }

    /// The regions replicas are chosen from, as a puller sends them (§2.6).
    pub(crate) fn regions(&self) -> &[Vec<Arc<str>>] {
        &self.regions[..self.regions.len().min(Self::REGIONS)]
    }

    /// Another node's candidates from the regions it sent: `seed` and `per_region` are the same on every node.
    pub(crate) fn of(&self, mut regions: Vec<Vec<Arc<str>>>) -> Self {
        regions.truncate(Self::REGIONS);
        regions.iter_mut().for_each(|nodes| nodes.truncate(self.per_region));
        Self { seed: self.seed, per_region: self.per_region, regions }
    }

    /// Every candidate: the nodes a new replica pulls from.
    pub(crate) fn all(&self) -> impl Iterator<Item = &Arc<str>> {
        self.regions.iter().flatten()
    }

    /// How many replicas a key has.
    pub(crate) fn per_key(&self) -> usize {
        self.regions.len().clamp(Self::AT_LEAST, Self::REGIONS).min(self.all().count())
    }

    /// `id`'s replicas: the rendezvous winner among each region's candidates, in the largest regions; with fewer than
    /// three regions, the largest give their second or third.
    pub(crate) fn replicas(&self, id: u64) -> Vec<&Arc<str>> {
        let want = self.per_key();
        let score =
            |node: &Arc<str>| crate::placement::hash(self.seed, id.to_le_bytes().into_iter().chain(node.bytes()));
        let ranked: Vec<Vec<&Arc<str>>> = self
            .regions()
            .iter()
            .map(|nodes| {
                let mut nodes: Vec<&Arc<str>> = nodes.iter().collect();
                nodes.sort_by_cached_key(|node| (std::cmp::Reverse(score(node)), *node));
                nodes
            })
            .collect();
        (0..self.per_region)
            .flat_map(|i| ranked.iter().filter_map(move |nodes| nodes.get(i).copied()))
            .take(want)
            .collect()
    }

    /// Whether `node` is one of `id`'s replicas.
    pub(crate) fn serves(&self, id: u64, node: &str) -> bool {
        self.replicas(id).iter().any(|r| ***r == *node)
    }

    /// Whether `me` replicates keys here that it did not in `old`: it was no candidate there, or a candidate of its
    /// region left, whose keys go to the others. A candidate that joins only takes keys. Where a region gives other
    /// than one replica per key, any change.
    pub(crate) fn gains(&self, old: &Self, me: &str) -> bool {
        let region = |c: &Self| c.regions.iter().position(|nodes| nodes.iter().any(|n| **n == *me));
        let (Some(now), Some(before)) = (region(self), region(old)) else { return region(self).is_some() };
        let one_each = |c: &Self| c.per_key() == c.regions.len();
        if !one_each(self) || !one_each(old) || self.regions.len() != old.regions.len() {
            return self.regions != old.regions;
        }
        old.regions[before].iter().any(|n| !self.regions[now].contains(n))
    }
}

/// The quota nodes a view shows: each one's incarnation, and the candidates replicas are chosen from, reachable or
/// not; and those it shows unreachable, which a pull does not wait for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Members {
    incarnations: BTreeMap<Arc<str>, u128>,
    pub candidates: Candidates,
    unreachable: BTreeSet<Arc<str>>,
}

impl Members {
    /// From `(node, incarnation, region)`. `seed` and `per_region` must be the same on every node.
    pub(crate) fn new<'a>(
        seed: u64,
        per_region: usize,
        members: impl IntoIterator<Item = (&'a str, u128, &'a str)>,
    ) -> Self {
        let members: Vec<_> = members.into_iter().collect();
        let candidates = Candidates::new(seed, per_region, members.iter().map(|(node, _, region)| (*node, *region)));
        let incarnations = members.into_iter().map(|(node, inc, _)| (Arc::from(node), inc)).collect();
        Self { incarnations, candidates, unreachable: BTreeSet::new() }
    }

    pub(crate) fn unreachable<'a>(mut self, nodes: impl IntoIterator<Item = &'a str>) -> Self {
        self.unreachable = nodes.into_iter().map(Arc::from).collect();
        self
    }

    /// The other candidates a pull asks: one the view shows unreachable may never answer, as a dead node static
    /// discovery keeps listing, and holds what its rows hold only for nodes that reached it.
    pub(crate) fn to_pull<'a>(&'a self, me: &'a str) -> impl Iterator<Item = &'a Arc<str>> {
        self.candidates.all().filter(move |c| &***c != me && !self.unreachable.contains(*c))
    }

    pub(crate) fn incarnation(&self, node: &str) -> Option<u128> {
        self.incarnations.get(node).copied()
    }
}

fn bucket(now: Duration, width: Duration) -> u64 {
    (now.as_nanos() / width.as_nanos()) as u64
}

/// How long ago the current bucket began.
fn phase(now: Duration, width: Duration) -> Duration {
    Duration::from_nanos((now.as_nanos() % width.as_nanos()) as u64)
}

#[derive(Clone, Debug, Hash)]
enum Usage {
    Permits(u64),
    /// Units per bucket, and the number of the newest: `now / width`.
    Rate {
        ring: [u64; BUCKETS],
        newest: u64,
    },
}

#[derive(Clone, Copy, Debug, Hash)]
struct Grant {
    link: usize,
    epoch: u32,
    value: u64,
    used: u64,
}

/// What an entry admits against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Bound {
    /// The smallest fresh grant of the replicas that are not young, and the most usage one of them counts in all.
    Grant { value: u64, used: u64 },
    /// No fresh grant: `ceil(max / quota nodes)`.
    Fallback(u64),
    /// Evicted before, and not admitted on a grant since.
    Refused,
}

/// A node's usage of one limit's key, and the grants it holds for it.
#[derive(Clone, Debug, Hash)]
pub(crate) struct Entry {
    limit: &'static str,
    max: u64,
    /// A bucket of a rate's ring, zero for permits.
    width: Duration,
    usage: Usage,
    grants: Vec<Grant>,
    /// The usage last reported, and when.
    reported: (u64, Duration),
    dirty: bool,
    /// Cost refused in this heartbeat and the one before, and once refused, the most used at once in each: replicas cap
    /// a grant at usage and demand, and one report cycle often sees no refusal of a steady demand.
    demand: [(u64, u64); 2],
    /// When this heartbeat's count began.
    counted: Duration,
    /// The last report carried demand.
    asked: bool,
    last_use: Duration,
    /// Evicted before: refused until a majority of its replicas granted, so that eviction cannot hand out the
    /// fallback again and again.
    evicted: bool,
    /// New here, and in the heartbeat `counted` began with: it admits up to the fallback's `a` where its grants are
    /// smaller, while a node that holds the room hands it back (§2.5). Not after an eviction.
    new: bool,
}

impl Entry {
    pub(crate) fn new(limit: &Limit, now: Duration, evicted: bool) -> Self {
        let width = limit.window / 8;
        let usage = if width.is_zero() {
            Usage::Permits(0)
        } else {
            Usage::Rate { ring: [0; BUCKETS], newest: bucket(now, width) }
        };
        Self {
            limit: limit.name,
            max: limit.max,
            width,
            usage,
            grants: Vec::new(),
            reported: (0, now),
            dirty: true,
            demand: [(0, 0); 2],
            counted: now,
            asked: false,
            last_use: now,
            evicted,
            new: !evicted,
        }
    }

    pub(crate) fn usage(&mut self, now: Duration) -> u64 {
        match &mut self.usage {
            Usage::Permits(held) => *held,
            Usage::Rate { ring, newest } => {
                let b = bucket(now, self.width);
                for n in *newest + 1..=b.min(*newest + BUCKETS as u64) {
                    ring[(n % BUCKETS as u64) as usize] = 0;
                }
                *newest = (*newest).max(b);
                ring.iter().fold(0, |sum, units| sum.saturating_add(*units))
            }
        }
    }

    /// The smallest fresh grant once `quorum`, a majority of the key's replicas, gave one: any two nodes then share a
    /// replica, whose grants to both count in its budget. A replica that never heard of this node gives none.
    pub(crate) fn bound(&self, links: &[Fresh], now: Duration, nodes: usize, quorum: usize) -> Bound {
        let fresh =
            self.grants.iter().filter(|g| links.get(g.link).is_some_and(|l| l.epoch == g.epoch && now < l.until));
        let (n, value, used) = fresh.fold((0, u64::MAX, 0), |(n, v, u), g| (n + 1, v.min(g.value), u.max(g.used)));
        match n >= quorum.max(1) {
            true => Bound::Grant { value, used },
            false if self.evicted => Bound::Refused,
            false => Bound::Fallback(self.max.div_ceil(nodes.max(1) as u64)),
        }
    }

    /// Whether `cost` more fits `bound`, and whether on the fallback. A refusal is reported as demand.
    pub(crate) fn check(&mut self, cost: u64, bound: Bound, now: Duration) -> Result<bool, Denied> {
        if let Bound::Grant { .. } = bound {
            self.evicted = false;
        }
        let u = self.usage(now);
        let (at_limit, value) = match bound {
            Bound::Grant { value, .. } | Bound::Fallback(value)
                if u.saturating_add(cost) <= value && cost <= self.max =>
            {
                return Ok(matches!(bound, Bound::Fallback(_)));
            }
            // Its own usage may be newer than what the replicas counted.
            Bound::Grant { value, used } => (used.max(u).saturating_add(cost) > self.max, value),
            Bound::Fallback(value) => (false, value),
            Bound::Refused => (false, 0),
        };
        self.asking(now);
        self.demand[0].0 = self.demand[0].0.saturating_add(cost);
        self.dirty = true;
        self.last_use = now;
        let limit = self.limit;
        Err(if at_limit || cost > self.max {
            let retry_after = match self.usage {
                Usage::Permits(_) => HEARTBEAT,
                Usage::Rate { .. } => self.width - phase(now, self.width),
            };
            Denied::Limit { limit, retry_after }
        } else {
            Denied::Share { limit, retry_after: self.drained(u.saturating_add(cost).saturating_sub(value), now) }
        })
    }

    /// `check`, and where it refuses for a share a key new here, the fallback's `a = ceil(max / nodes)` over its
    /// grant: still reported as demand, so that the replicas grant what it admits. `check` refusing has rolled the
    /// heartbeat, so `new` reads as of `now`.
    pub(crate) fn admit(&mut self, cost: u64, bound: Bound, nodes: usize, now: Duration) -> Result<bool, Denied> {
        match self.check(cost, bound, now) {
            Err(Denied::Share { .. })
                if self.new && self.usage(now).saturating_add(cost) <= self.max.div_ceil(nodes.max(1) as u64) =>
            {
                Ok(true)
            }
            checked => checked,
        }
    }

    /// How long until `units` of a rate's usage leave the window, at most a heartbeat; a heartbeat for permits.
    fn drained(&self, units: u64, now: Duration) -> Duration {
        let Usage::Rate { ring, newest } = &self.usage else { return HEARTBEAT };
        let mut left = 0u64;
        for j in 0..BUCKETS as u64 {
            left = left.saturating_add(ring[((*newest + 1 + j) % BUCKETS as u64) as usize]);
            if left >= units {
                return (self.width - phase(now, self.width) + self.width * j as u32).min(HEARTBEAT);
            }
        }
        HEARTBEAT
    }

    /// Returns the permits it added, none for a rate. Once refused, it asks for the most it uses at once while in use:
    /// a row that stops asking is lowered to its usage, which reads 0 for a permit dropped at once.
    pub(crate) fn add(&mut self, cost: u64, now: Duration) -> u64 {
        let before = self.usage(now);
        let added = match &mut self.usage {
            Usage::Permits(held) => {
                let was = *held;
                *held = held.saturating_add(cost);
                *held - was
            }
            Usage::Rate { ring, newest } => {
                let units = &mut ring[(*newest % BUCKETS as u64) as usize];
                *units = units.saturating_add(cost);
                0
            }
        };
        if self.asking(now) {
            self.demand[0].1 = self.demand[0].1.max(before.saturating_add(cost));
        }
        self.touched(now);
        added
    }

    /// Returns the permits it gave back.
    pub(crate) fn release(&mut self, n: u64, now: Duration) -> u64 {
        let mut released = 0;
        if let Usage::Permits(held) = &mut self.usage {
            released = n.min(*held);
            *held -= released;
        }
        self.touched(now);
        released
    }

    /// Whether it was refused, or used since, in this heartbeat or the one before.
    fn asking(&mut self, now: Duration) -> bool {
        if now >= self.counted + HEARTBEAT {
            self.demand = [(0, 0), if now >= self.counted + 2 * HEARTBEAT { (0, 0) } else { self.demand[0] }];
            (self.counted, self.new) = (now, false);
        }
        self.demand != [(0, 0); 2]
    }

    /// Marks the entry for the next report once its usage moved by an eighth of its grant, or it was not reported for
    /// long.
    fn touched(&mut self, now: Duration) {
        self.last_use = now;
        let grant = self.grants.iter().map(|g| g.value).min().unwrap_or(0);
        if self.usage(now).abs_diff(self.reported.0) >= (grant / 8).max(1) || now >= self.reported.1 + REFRESH {
            self.dirty = true;
        }
    }

    /// What the next report says of it, if anything changed. Replicas divide by the demand last reported, so an entry
    /// that reported demand is reported again a heartbeat later, until it reports none.
    pub(crate) fn report(&mut self, id: u64, now: Duration) -> Option<Item> {
        let held = matches!(self.usage, Usage::Permits(n) if n > 0);
        let due = |every| now >= self.reported.1 + every;
        if !self.dirty && !(self.asked && due(HEARTBEAT)) && !(held && due(REFRESH)) {
            return None;
        }
        let u = self.usage(now);
        let usage = match &self.usage {
            Usage::Permits(held) => Reported::Permits(*held),
            Usage::Rate { ring, newest } => Reported::Rate {
                width: self.width,
                phase: phase(now, self.width),
                buckets: std::array::from_fn(|j| ring[((*newest + 1 + j as u64) % BUCKETS as u64) as usize]),
            },
        };
        self.asking(now);
        let [(r0, p0), (r1, p1)] = self.demand;
        // What it needs beyond its usage, which the replicas' cap adds back.
        let demand = r0.saturating_add(r1).saturating_add(p0.max(p1).saturating_sub(u));
        let item = Item { id, max: self.max, usage, demand };
        (self.dirty, self.asked, self.reported) = (false, demand > 0, (u, now));
        Some(item)
    }

    pub(crate) fn set_grant(&mut self, link: usize, epoch: u32, value: u64, used: u64) {
        let grant = Grant { link, epoch, value, used };
        match self.grants.iter_mut().find(|g| g.link == link) {
            Some(g) => *g = grant,
            None => self.grants.push(grant),
        }
    }
}

/// A node's side of its exchange with one replica node.
#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct Link {
    replica: Arc<str>,
    /// The replica's incarnation as the view shows it: only its grants are taken in.
    incarnation: u128,
    /// Bumped when the replica restarts, or a message comes after the grants lapsed: grants filed under an earlier
    /// epoch count no more.
    epoch: u32,
    seq: u64,
    /// The last reports: each one's seq, when it left, and whether its answer is still to come.
    sent: VecDeque<(u64, Duration, bool)>,
    /// The last grants taken in, the report they answered, and the last answer taken in, which the next report
    /// acknowledges: a push's reply acknowledges the push.
    applied: u64,
    answered: u64,
    took: u64,
    /// The replica may have rows this node did not report: a report's answer did not come within a heartbeat, or the
    /// replica has just pulled the rows of the others, which may have missed one.
    resync: bool,
    until: Duration,
    young: bool,
    /// From the last report answered to its answer.
    rtt: Duration,
}

/// What a check needs of a link: the grants filed under `epoch` count until `until`, which is zero while the replica
/// is young.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Fresh {
    epoch: u32,
    until: Duration,
}

pub(crate) fn fresh(links: &[Link]) -> Vec<Fresh> {
    links.iter().map(|l| Fresh { epoch: l.epoch, until: if l.young { Duration::ZERO } else { l.until } }).collect()
}

impl Link {
    /// The view shows the replica as `incarnation`. Another than before, whether its clock read later or not, starts
    /// over: what its predecessor granted no longer counts, and it has been told of no entry yet.
    pub(crate) fn retarget(&mut self, incarnation: u128) -> bool {
        let moved = incarnation != self.incarnation;
        if moved {
            let (replica, seq, mut sent) =
                (std::mem::take(&mut self.replica), self.seq, std::mem::take(&mut self.sent));
            sent.iter_mut().for_each(|s| s.2 = false);
            *self = Link { replica, incarnation, epoch: self.epoch + 1, seq, sent, resync: true, ..Link::default() };
        }
        moved
    }

    /// Numbers the next report and remembers when it left: its seq, and the last answer taken in, with the incarnation
    /// that gave it.
    pub(crate) fn send(&mut self, now: Duration) -> (u64, u128, u64) {
        self.seq += 1;
        if self.sent.len() == SENT_KEPT {
            let answered = self.answered;
            self.resync |= self.sent.pop_front().is_some_and(|s| s.2 && s.0 < answered);
        }
        self.sent.push_back((self.seq, now, true));
        (self.seq, self.incarnation, self.took)
    }

    /// A report whose answer did not come within a heartbeat, while a later one's did, is lost, as the driver gives up
    /// on it then: the replica may lack what it carried. One whose answer came after a later one's is not: the replica
    /// had it, and the grants of that answer go again until acknowledged (spec/quotas.md §6). Nor is one to a replica
    /// that answers nothing, until it answers again: everything reported again meanwhile would reach no one.
    fn lose(&mut self, now: Duration) {
        let answered = self.answered;
        for s in self.sent.iter_mut().filter(|s| s.2 && s.0 < answered && now >= s.1 + HEARTBEAT) {
            (s.2, self.resync) = (false, true);
        }
    }

    /// The epoch to file `grants` under, or `None` when they are old news or from another incarnation. An `answer`
    /// from this link's incarnation closes its report, taken in or not; one from another leaves it to be lost.
    /// Where the grants taken in before lapsed, they count no more: the replica may have freed them, and a message that
    /// leaves an entry out would otherwise make its old grant count again (spec/quotas.md §2.3). Their entries are
    /// reported again.
    pub(crate) fn accept(&mut self, grants: &Grants, answer: bool, now: Duration) -> Option<u32> {
        let ours = grants.incarnation == self.incarnation;
        if let Some(s) = self.sent.iter_mut().find(|s| answer && ours && s.0 == grants.answers) {
            s.2 = false;
        }
        let ok = ours && grants.gseq > self.applied;
        if ok && self.applied > 0 && now >= self.until {
            (self.epoch, self.resync) = (self.epoch + 1, true);
        }
        ok.then_some(self.epoch)
    }

    /// Once the entries took `grants` in: they hold until `hold` after the report they answer left, not after they
    /// arrived, since the replica frees them a lease after it last heard from this node. Grants a replica pushed
    /// answer the last report it had.
    pub(crate) fn commit(&mut self, grants: &Grants, answer: bool, hold: Duration, now: Duration) {
        self.resync |= self.young && !grants.young;
        let sent = self.sent.iter().find(|s| s.0 == grants.answers).map(|s| s.1);
        // A push answers a report it did not come back for, so only an answer times a round trip.
        if answer && grants.answers > self.answered {
            self.rtt = sent.map_or(self.rtt, |at| now.saturating_sub(at));
        }
        (self.applied, self.answered) = (grants.gseq, grants.answers);
        if answer {
            self.took = grants.gseq;
        }
        self.until = sent.map_or(Duration::ZERO, |at| at + hold);
        self.young = grants.young;
    }
}

/// Keys evicted lately, as bits in two generations: the older is cleared once the newer has taken its share. A lookup,
/// made for every new entry, takes no lock; an insert takes one. They meet on an id only under its shard's lock.
struct Evicted {
    bits: Box<[AtomicU64]>,
    /// The generation inserts go to, and how many it took.
    current: Mutex<(usize, usize)>,
}

impl Evicted {
    const WORDS: usize = EVICTED_BITS / 64;

    fn new() -> Self {
        Self { bits: (0..2 * Self::WORDS).map(|_| AtomicU64::new(0)).collect(), current: Mutex::new((0, 0)) }
    }

    fn at(id: u64) -> (usize, u64) {
        let bit = id as usize % EVICTED_BITS;
        (bit / 64, 1 << (bit % 64))
    }

    fn insert(&self, id: u64) {
        let (current, set) = &mut *self.current.lock();
        if *set == EVICTED_BITS / 16 {
            *current ^= 1;
            self.bits[*current * Self::WORDS..][..Self::WORDS].iter().for_each(|w| w.store(0, Relaxed));
            *set = 0;
        }
        let (word, mask) = Self::at(id);
        self.bits[*current * Self::WORDS + word].fetch_or(mask, Relaxed);
        *set += 1;
    }

    fn contains(&self, id: u64) -> bool {
        let (word, mask) = Self::at(id);
        (self.bits[word].load(Relaxed) | self.bits[Self::WORDS + word].load(Relaxed)) & mask != 0
    }
}

/// A shard's entries, and a copy of each link's `Fresh`, so that a check reads the links under its shard's lock alone:
/// with one lock over the links read by every check, 24 threads made fewer checks than 4. Aligned so that two shards'
/// locks never share a cache line.
#[derive(Default)]
#[repr(align(128))]
struct Shard {
    entries: Entries,
    links: Vec<Fresh>,
    /// Permits its entries hold.
    permits: u64,
    /// The ids of `entries`, for the sweep to walk a slice at a time. An id only moves towards the front, so each walk
    /// reaches an entry no later into it than the walk before.
    order: Vec<u64>,
    /// How far into `order` the sweep is.
    swept: usize,
}

type Entries = HashMap<u64, Entry, IdHash>;
type Guards<'a> = Vec<(usize, MutexGuard<'a, Shard>)>;

/// A node's usage and grants, per limit and key. Aligned off the line of the `Arc` counts it lives under, which each
/// `Permit` writes twice while every call reads the shards through `Table`: sharing it cost a quarter to a third of
/// acquire-and-drop on 4 and 12 threads (benchmarks.md, build-1).
#[repr(align(128))]
pub(crate) struct Table {
    seed: u64,
    hold: Duration,
    per_shard: usize,
    /// Quota nodes in this node's view, for the fallback.
    nodes: AtomicUsize,
    /// Replicas per key: a grant counts once a majority of them gave one.
    key_replicas: AtomicUsize,
    shards: Box<[Mutex<Shard>]>,
    /// Written under this lock, which `publish` holds while it copies them into the shards.
    links: RwLock<Vec<Link>>,
    applying: Mutex<()>,
    evicted: Evicted,
    /// Units admitted with no fresh grant of a majority (§2.5).
    pub fallback: AtomicU64,
    pub denied_limit: AtomicU64,
    pub denied_share: AtomicU64,
    /// The shard the last report that ran out of room stopped in: the next starts there, so that none waits behind
    /// the others for long.
    cursor: AtomicUsize,
    /// When the next report cycle is due, and the slowest round trip of the last answers, in ns: what a refusal for a
    /// share waits for.
    next_round: AtomicU64,
    rtt: AtomicU64,
}

impl Table {
    /// `seed` must be the same on every node, as the ids travel. A grant holds `hold` after the report it answers left.
    pub(crate) fn new(seed: u64, hold: Duration, capacity: usize) -> Self {
        Self {
            seed,
            hold,
            per_shard: capacity.div_ceil(SHARDS).max(1),
            nodes: AtomicUsize::new(1),
            key_replicas: AtomicUsize::new(3),
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            links: RwLock::default(),
            applying: Mutex::new(()),
            evicted: Evicted::new(),
            fallback: AtomicU64::new(0),
            denied_limit: AtomicU64::new(0),
            denied_share: AtomicU64::new(0),
            cursor: AtomicUsize::new(0),
            next_round: AtomicU64::new(0),
            rtt: AtomicU64::new(0),
        }
    }

    /// A report cycle ran at `now`.
    pub(crate) fn reported(&self, now: Duration) {
        self.next_round.store((now + REPORT_EVERY).as_nanos() as u64, Relaxed);
    }

    /// When a refusal for a share expects the room: the next report carries its demand, the replica pushes a lower to
    /// a node that holds the room, which acknowledges it in its reply, and pushes the raise. A round trip each, about.
    fn handback(&self, now: Duration) -> Duration {
        let next = Duration::from_nanos(self.next_round.load(Relaxed));
        let until = if next > now { next - now } else { REPORT_EVERY };
        until + 3 * Duration::from_nanos(self.rtt.load(Relaxed))
    }

    pub(crate) fn set_nodes(&self, nodes: usize) {
        self.nodes.store(nodes, Relaxed);
    }

    pub(crate) fn set_key_replicas(&self, replicas: usize) {
        self.key_replicas.store(replicas, Relaxed);
    }

    pub(crate) fn id(&self, limit: &Limit, key: &[u8]) -> u64 {
        entry_id(self.seed, limit, key)
    }

    fn shard(id: u64) -> usize {
        (id >> 58) as usize
    }

    /// Locks the shards of `ids`, each once, in index order, so that two calls never wait on each other in a cycle.
    fn lock(&self, ids: &[(u64, &Limit, u64)]) -> Guards<'_> {
        let mut set = ids.iter().fold(0u64, |set, (id, _, _)| set | 1 << Self::shard(*id));
        let mut guards = Vec::with_capacity(set.count_ones() as usize);
        while set != 0 {
            let s = set.trailing_zeros() as usize;
            guards.push((s, self.shards[s].lock()));
            set &= set - 1;
        }
        guards
    }

    /// `id`'s entry, made if new, its shard's copy of the links, and the permits its shard holds.
    fn entry<'a>(
        &self,
        guards: &'a mut Guards<'_>,
        id: u64,
        limit: &Limit,
        now: Duration,
    ) -> (&'a mut Entry, &'a [Fresh], &'a mut u64) {
        let shard = &mut guards.iter_mut().find(|(s, _)| *s == Self::shard(id)).expect("locked by the caller").1;
        if shard.entries.len() >= self.per_shard && !shard.entries.contains_key(&id) {
            self.evict(shard, now);
        }
        let Shard { entries, links, permits, order, .. } = &mut **shard;
        let entry = entries.entry(id).or_insert_with(|| {
            order.push(id);
            Entry::new(limit, now, self.evicted.contains(id))
        });
        entry.max = limit.max;
        (entry, links, permits)
    }

    /// Drops the least recently used eighth of a full shard, but for entries in use: permits held, or units in the
    /// window, which a replica would otherwise forget with them.
    fn evict(&self, shard: &mut Shard, now: Duration) {
        let Shard { entries, order, swept, .. } = shard;
        #[expect(clippy::disallowed_methods, reason = "the oldest go whatever order they are found in")]
        let mut idle: Vec<(Duration, u64)> =
            entries.iter_mut().filter_map(|(id, e)| (e.usage(now) == 0).then_some((e.last_use, *id))).collect();
        let n = (entries.len() / 8).max(1).min(idle.len());
        if n == 0 {
            return;
        }
        idle.select_nth_unstable(n - 1);
        for (_, id) in &idle[..n] {
            entries.remove(id);
            self.evicted.insert(*id);
        }
        // Looked up in a set of the evicted alone, which fits in cache where `entries` may not. The sweep's place moves
        // back past those it had passed, so that it skips none.
        let gone: HashSet<u64, IdHash> = idle[..n].iter().map(|(_, id)| *id).collect();
        *swept -= order[..*swept].iter().filter(|id| gone.contains(id)).count();
        order.retain(|id| !gone.contains(id));
    }

    /// Each limit's id and cost, a limit named twice counted once with both costs.
    fn ids<'a, L: Borrow<Limit>>(&self, key: &[u8], costs: &'a [(L, u64)]) -> Vec<(u64, &'a Limit, u64)> {
        let mut ids: Vec<_> = costs.iter().map(|(l, cost)| (self.id(l.borrow(), key), l.borrow(), *cost)).collect();
        ids.sort_unstable_by_key(|(id, _, _)| *id);
        ids.dedup_by(|b, a| (a.0 == b.0).then(|| a.2 = a.2.saturating_add(b.2)).is_some());
        ids
    }

    fn quorum(&self) -> usize {
        self.key_replicas.load(Relaxed) / 2 + 1
    }

    /// `limit`'s key as `take` would find it now, and the bound it would check against: the simulation's oracle asks.
    #[cfg(test)]
    fn peek(&self, key: &[u8], limit: &Limit, now: Duration) -> (Entry, Bound) {
        let id = self.id(limit, key);
        let shard = self.shards[Self::shard(id)].lock();
        let mut entry =
            shard.entries.get(&id).cloned().unwrap_or_else(|| Entry::new(limit, now, self.evicted.contains(id)));
        entry.max = limit.max;
        let bound = entry.bound(&shard.links, now, self.nodes.load(Relaxed), self.quorum());
        (entry, bound)
    }

    #[cfg(test)]
    pub(crate) fn bound(&self, key: &[u8], limit: &Limit, now: Duration) -> Bound {
        self.peek(key, limit, now).1
    }

    /// Whether `take` would admit `cost` of `limit`'s key now on the fallback or the floor of a key new here, `None`
    /// where it would refuse it.
    #[cfg(test)]
    pub(crate) fn falls(&self, key: &[u8], limit: &Limit, cost: u64, now: Duration) -> Option<bool> {
        let (mut entry, bound) = self.peek(key, limit, now);
        entry.admit(cost, bound, self.nodes.load(Relaxed), now).ok()
    }

    /// Takes every cost or none, each against its own limit.
    pub(crate) fn take<L: Borrow<Limit>>(&self, key: &[u8], costs: &[(L, u64)], now: Duration) -> Result<(), Denied> {
        let ids = self.ids(key, costs);
        let mut guards = self.lock(&ids);
        let (nodes, quorum) = (self.nodes.load(Relaxed), self.quorum());
        let (mut denied, mut fallback) = (None::<Denied>, 0);
        for &(id, limit, cost) in &ids {
            let (entry, links, _) = self.entry(&mut guards, id, limit, now);
            let bound = entry.bound(links, now, nodes, quorum);
            match entry.admit(cost, bound, nodes, now) {
                Ok(fell) => fallback += fell as u64 * cost,
                Err(mut d) => {
                    if let Denied::Share { retry_after, .. } = &mut d {
                        *retry_after = (*retry_after).min(self.handback(now));
                    }
                    denied = Some(denied.map_or(d, |was| was.worse(d)));
                }
            }
        }
        if let Some(denied) = denied {
            match denied {
                Denied::Limit { .. } => self.denied_limit.fetch_add(1, Relaxed),
                Denied::Share { .. } => self.denied_share.fetch_add(1, Relaxed),
            };
            return Err(denied);
        }
        if fallback > 0 {
            self.fallback.fetch_add(fallback, Relaxed);
        }
        self.add(&mut guards, &ids, now);
        Ok(())
    }

    /// Adds costs known only afterwards. Never refuses.
    pub(crate) fn charge<L: Borrow<Limit>>(&self, key: &[u8], costs: &[(L, u64)], now: Duration) {
        let ids = self.ids(key, costs);
        let mut guards = self.lock(&ids);
        self.add(&mut guards, &ids, now);
    }

    /// Ids hash the name alone, so a rate named like a permit limit adds to that entry's permits.
    #[inline]
    fn add(&self, guards: &mut Guards<'_>, ids: &[(u64, &Limit, u64)], now: Duration) {
        for &(id, limit, cost) in ids {
            let (entry, _, permits) = self.entry(guards, id, limit, now);
            let added = entry.add(cost, now);
            // A rate adds none. Storing that 0 would still take the line of the shard that every check reads: 17 % of
            // the checks at 12 and 24 threads (benchmarks.md, Quotas).
            if added != 0 {
                *permits += added;
            }
        }
    }

    /// Gives back `n` permits of entry `id`.
    pub(crate) fn release(&self, id: u64, n: u64, now: Duration) {
        let mut shard = self.shards[Self::shard(id)].lock();
        let Shard { entries, permits, .. } = &mut *shard;
        if let Some(entry) = entries.get_mut(&id) {
            *permits -= entry.release(n, now);
        }
    }

    /// Points each link at the incarnation of its replica `incarnation` gives, where it gives one.
    pub(crate) fn relink(&self, incarnation: impl Fn(&str) -> Option<u128>) {
        let mut links = self.links.write();
        let mut moved = false;
        for link in links.iter_mut() {
            if let Some(shown) = incarnation(&link.replica) {
                moved |= link.retarget(shown);
            }
        }
        if moved {
            self.publish(&links);
        }
    }

    /// Copies each link's `Fresh` into every shard. Called with the links locked to write, so that copies land in order.
    fn publish(&self, links: &[Link]) {
        let fresh = fresh(links);
        self.shards.iter().for_each(|shard| shard.lock().links.clone_from(&fresh));
    }

    /// Whether every shard's copy of the links reads as they do: a link it has no copy of yet counts no grant.
    #[cfg(test)]
    pub(crate) fn copies_agree(&self) -> bool {
        let fresh = fresh(&self.links.read());
        self.shards.iter().all(|shard| {
            let copies = &shard.lock().links;
            copies.len() <= fresh.len()
                && fresh.iter().enumerate().all(|(i, f)| copies.get(i).map_or(f.until.is_zero(), |c| c == f))
        })
    }

    /// Every replica node this node reported to.
    pub(crate) fn replicas(&self) -> Vec<Arc<str>> {
        self.links.read().iter().map(|l| l.replica.clone()).collect()
    }

    /// The link to a replica node, made on first use, for the incarnation the view shows.
    pub(crate) fn link(&self, replica: &str, incarnation: u128) -> usize {
        let mut links = self.links.write();
        let i = links.iter().position(|l| *l.replica == *replica).unwrap_or_else(|| {
            links.push(Link { replica: replica.into(), ..Link::default() });
            links.len() - 1
        });
        // A new link needs no copy: a shard without one counts none of its grants, and grants come with `apply`.
        if links[i].retarget(incarnation) {
            self.publish(&links);
        }
        i
    }

    /// Numbers a report to a replica node: its seq, and the last grants taken in from there with the incarnation that
    /// gave them.
    pub(crate) fn send(&self, link: usize, now: Duration) -> (u64, u128, u64) {
        self.links.write()[link].send(now)
    }

    /// What changed since the last report, by id: at most `MAX_ITEMS`, the rest staying for the next. A link that
    /// wants a resync, or lost a report, has every entry reported again that it granted, or that lacks a replica's
    /// grant: the replica may have answered young, or lost the report that carried it.
    // ponytail: scans every entry under its shard lock, about 17 ms per 2 M entries, and resumes at the shard, not the
    // entry, where the last report ran out; a list of dirty ids per shard if the benchmark at 10⁷ keys shows it.
    #[expect(
        clippy::disallowed_methods,
        reason = "marking needs no order; items are sorted before they leave, and IdHash's order is the same each run"
    )]
    pub(crate) fn dirty(&self, now: Duration) -> Vec<Item> {
        let mut items = Vec::new();
        let (stale, epochs): (Vec<bool>, Vec<u32>) = self
            .links
            .write()
            .iter_mut()
            .map(|l| {
                l.lose(now);
                (std::mem::take(&mut l.resync), l.epoch)
            })
            .unzip();
        if stale.contains(&true) {
            let replicas = self.key_replicas.load(Relaxed);
            let kept = |g: &Grant| stale.get(g.link) == Some(&false) && epochs.get(g.link) == Some(&g.epoch);
            for shard in &*self.shards {
                shard
                    .lock()
                    .entries
                    .values_mut()
                    .for_each(|e| e.dirty |= e.grants.iter().filter(|g| kept(g)).count() < replicas);
            }
        }
        let start = self.cursor.load(Relaxed);
        for s in (start..SHARDS).chain(0..start) {
            let room = MAX_ITEMS - items.len();
            items.extend(self.shards[s].lock().entries.iter_mut().filter_map(|(id, e)| e.report(*id, now)).take(room));
            if items.len() == MAX_ITEMS {
                self.cursor.store(s, Relaxed);
                break;
            }
        }
        items.sort_unstable_by_key(|i| i.id);
        items
    }

    /// Takes in a replica node's answer, the grants for keys it `replicates` in this node's view. The entries take
    /// the grants before the link acknowledges them or dates them, so that no report acknowledges a lower an entry has
    /// not taken; a link retargeted meanwhile acknowledges nothing of its predecessor's.
    pub(crate) fn apply(&self, link: usize, grants: &Grants, replicates: impl Fn(u64) -> bool, now: Duration) {
        self.take_in(link, grants, true, replicates, now);
    }

    fn take_in(&self, link: usize, grants: &Grants, answer: bool, replicates: impl Fn(u64) -> bool, now: Duration) {
        let _one = self.applying.lock();
        let epoch = self.links.write().get_mut(link).and_then(|l| l.accept(grants, answer, now));
        let Some(epoch) = epoch else { return };
        for &(id, value, used) in grants.entries.iter().filter(|e| replicates(e.0)) {
            if let Some(entry) = self.shards[Self::shard(id)].lock().entries.get_mut(&id) {
                entry.set_grant(link, epoch, value, used);
            }
        }
        let mut links = self.links.write();
        if links[link].epoch == epoch {
            links[link].commit(grants, answer, self.hold, now);
            // A push most often answers a report whose answer came: the copies would not change.
            let fresh = fresh(&links[link..=link])[0];
            if self.shards[0].lock().links.get(link) != Some(&fresh) {
                self.publish(&links);
            }
            let rtt = links.iter().map(|l| l.rtt).max().unwrap_or_default();
            self.rtt.store(rtt.as_nanos() as u64, Relaxed);
        }
    }

    /// Takes in grants the replica node `replica` pushed, as `apply` does, where they answer one of the last two
    /// reports sent there, and returns what its reply acknowledges: the replica's incarnation this node's view shows
    /// and the push. A replica that missed the later reports, as one that does not hear this node, plans from a row
    /// that ages, and would push a lower that this node's usage does not call for. Acknowledged only where this node
    /// took the push in, it answers the last report, and none of its entries changed since: the replica then knows the
    /// usage the lowers leave, as from a report. Usage taken on the old grant and not reported yet would otherwise not
    /// count, and the room it holds went to another node.
    pub(crate) fn pushed(
        &self,
        replica: &str,
        grants: &Grants,
        replicates: impl Fn(u64) -> bool,
        now: Duration,
    ) -> Option<(u128, u64)> {
        let (link, seq) =
            self.links.read().iter().enumerate().find(|(_, l)| *l.replica == *replica).map(|(i, l)| (i, l.seq))?;
        if seq <= grants.answers + 1 {
            self.take_in(link, grants, false, replicates, now);
        }
        let (applied, incarnation) = self.links.read().get(link).map(|l| (l.applied, l.incarnation))?;
        let dirty = |id: u64| self.shards[Self::shard(id)].lock().entries.get(&id).is_some_and(|e| e.dirty);
        if seq != grants.answers || applied != grants.gseq || grants.entries.iter().any(|e| dirty(e.0)) {
            return None;
        }
        Some((incarnation, grants.gseq))
    }

    /// Once the view changed: drops the grants of links that no longer replicate their key, which would otherwise
    /// count towards its majority, and reports the entry to its replicas now, which may never have heard of it.
    #[expect(clippy::disallowed_methods, reason = "each entry on its own")]
    pub(crate) fn forget(&self, replicates: impl Fn(u64, &str) -> bool) {
        let replicas = self.replicas();
        for shard in &*self.shards {
            shard.lock().entries.iter_mut().for_each(|(id, entry)| {
                let had = entry.grants.len();
                entry.grants.retain(|g| replicates(*id, &replicas[g.link]));
                entry.dirty |= entry.grants.len() < had;
            });
        }
    }

    /// Takes each shard's sweep `into`'s share of `SWEEP_EVERY` of the way through its entries, dropping those unused
    /// for `KEEP` but for those in use: permits held, or units in the window. `SWEEP_EVERY` ends the sweep and the next
    /// starts from the front. The share is of the shard's `order` capacity, which only grows, so that a sweep spreads
    /// over the whole of `SWEEP_EVERY`: a share of `per_shard` did a shard far below it in the first part, in the report
    /// ticks there, and at 10⁷ keys the replicas held a third of the rows. The shards take turns a slice at a time, so
    /// that a check waiting on a shard goes before its next slice.
    pub(crate) fn sweep(&self, into: Duration, now: Duration) {
        let end = into >= SWEEP_EVERY;
        let mut sweeping = u64::MAX;
        while sweeping != 0 {
            let mut turn = sweeping;
            while turn != 0 {
                let s = turn.trailing_zeros() as usize;
                turn &= turn - 1;
                let mut shard = self.shards[s].lock();
                let Shard { entries, order, swept, .. } = &mut *shard;
                let due = if end {
                    usize::MAX
                } else {
                    (order.capacity() as u128 * into.as_nanos() / SWEEP_EVERY.as_nanos()) as usize
                };
                let to = due.min(order.len());
                for _ in 0..SLICE.min(to.saturating_sub(*swept)) {
                    let id = order[*swept];
                    let e = entries.get_mut(&id).expect("every id in order has its entry");
                    if e.usage(now) == 0 && now >= e.last_use + KEEP {
                        entries.remove(&id);
                        order.swap_remove(*swept);
                    } else {
                        *swept += 1;
                    }
                }
                if *swept >= due.min(order.len()) {
                    sweeping &= !(1 << s);
                    if end {
                        *swept = 0;
                    }
                }
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().entries.len()).sum()
    }

    /// Permits held, of every key.
    pub(crate) fn permits(&self) -> u64 {
        self.shards.iter().map(|s| s.lock().permits).sum()
    }
}

/// A report's usage of an entry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Reported {
    Permits(u64),
    /// The buckets oldest first, the newest begun `phase` ago.
    Rate {
        width: Duration,
        phase: Duration,
        buckets: [u64; BUCKETS],
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Item {
    pub id: u64,
    pub max: u64,
    pub usage: Reported,
    pub demand: u64,
}

/// What a node tells a replica node: the entries that changed, and the last answer it took in from there, `acked`, from
/// the replica's incarnation `replica`. A restarted replica's numbering starts over, so an acknowledgement of its
/// predecessor's grants acknowledges none of its own.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Report {
    pub incarnation: u128,
    pub seq: u64,
    pub replica: u128,
    pub acked: u64,
    pub items: Vec<Item>,
}

/// A replica's answer to a report, or a push: `(id, grant, usage it counts in all)` for grants the node has not
/// acknowledged, as `Replica::grants` picks them. The usage tells a refusal at the limit from one for want of a grant.
/// `young` says to take none of them into account yet.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Grants {
    pub incarnation: u128,
    pub answers: u64,
    pub gseq: u64,
    pub young: bool,
    pub entries: Vec<(u64, u64, u64)>,
}

/// Usage as a replica counts it: permits held, or a rate's units with when they leave the window.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Used {
    Permits(u64),
    Rate(Vec<(Duration, u64)>),
}

impl Used {
    fn of(reported: &Reported, now: Duration) -> Self {
        match reported {
            Reported::Permits(held) => Used::Permits(*held),
            // The node's clock started the bucket `phase` before it sent; this one reads later, so it counts longer.
            Reported::Rate { width, phase, buckets } => Used::Rate(
                buckets
                    .iter()
                    .enumerate()
                    .filter(|(_, units)| **units > 0)
                    .map(|(j, units)| (now + (*width * (j as u32 + 1)).saturating_sub(*phase), *units))
                    .collect(),
            ),
        }
    }

    fn at(&self, now: Duration) -> u64 {
        match self {
            Used::Permits(held) => *held,
            Used::Rate(drops) => drops.iter().filter(|(at, _)| *at > now).fold(0, |sum, (_, u)| sum.saturating_add(*u)),
        }
    }

    /// Moves the drop times from one clock to another: `from` on the first reads as `to` on the second.
    fn shift(&self, from: Duration, to: Duration) -> Self {
        match self {
            Used::Permits(held) => Used::Permits(*held),
            Used::Rate(drops) => Used::Rate(
                drops.iter().filter(|(at, _)| *at > from).map(|(at, units)| (*at - from + to, *units)).collect(),
            ),
        }
    }
}

/// A row as one replica hands it to another: rate drop times count from when the page was made, and the most the
/// node may hold on that replica's grants.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Pulled {
    pub id: u64,
    pub max: u64,
    pub width: Duration,
    pub node: Arc<str>,
    pub incarnation: u128,
    pub used: Used,
    pub demand: u64,
    pub held: u64,
}

/// A process of a node: its id and incarnation.
pub(crate) type Peer = (Arc<str>, u128);

/// Grants a message carries besides those its report named: at most this many set since the last message to the node,
/// and, in an answer, at most this many more it has not acknowledged, so that the work a message makes is bounded
/// however far the node fell behind (spec/quotas.md §2.3).
pub(crate) const RESEND: usize = 256;
/// Grants messages to a node whose entries a replica keeps until the node acknowledges one: the oldest go first, and
/// their entries wait for the next time they are sent.
const UNACKED: usize = 16;

/// A replica's side of its exchange with one process of a node.
#[derive(Clone, Debug, Hash)]
struct PeerState {
    seq: u64,
    heard: Duration,
    /// Entries whose grant the node has not acknowledged, and those of them set since the last message to it.
    pending: BTreeSet<u64>,
    fresh: BTreeSet<u64>,
    keys: BTreeSet<u64>,
    /// Where the next answer's walk of `pending` starts.
    cursor: u64,
    /// The last grants messages to it, each with the entries it carried, until the node acknowledges it.
    sent: VecDeque<(u64, Vec<u64>)>,
    /// Its grants are to be pushed: a lower that frees room another node asks for, or a raise it asks for. When the
    /// last push left.
    push: bool,
    pushed: Duration,
}

impl PeerState {
    fn new(now: Duration) -> Self {
        Self {
            seq: 0,
            heard: now,
            pending: BTreeSet::new(),
            fresh: BTreeSet::new(),
            keys: BTreeSet::new(),
            cursor: 0,
            sent: VecDeque::new(),
            push: false,
            pushed: Duration::ZERO,
        }
    }

    /// `id`'s grant changed: the next message to the node carries it.
    fn set(&mut self, id: u64) {
        self.pending.insert(id);
        self.fresh.insert(id);
    }

    /// Ends the holds of the grants that message `acked`, which the node took in, carried; the last message sent being
    /// `last`. Only those: a message that left a grant out does not show that the node has it, and one taken in does
    /// not show that the node took in those before it. Returns the keys whose hold fell.
    fn settle(&mut self, peer: &Peer, keys: &mut BTreeMap<u64, Key>, acked: u64, last: u64) -> Vec<u64> {
        let mut fell = Vec::new();
        let Some(at) = self.sent.iter().position(|(gseq, _)| *gseq == acked) else { return fell };
        let (_, carried) = self.sent.remove(at).expect("found above");
        for id in carried {
            if !self.pending.contains(&id) {
                continue;
            }
            let Some(row) = keys.get_mut(&id).and_then(|k| k.rows.get_mut(peer)) else {
                self.pending.remove(&id);
                self.fresh.remove(&id);
                continue;
            };
            if row.pulled.is_some() {
                continue;
            }
            let was = row.held();
            if row.changed <= acked {
                (row.held, row.mark) = ([0, row.grant], last);
                self.pending.remove(&id);
                self.fresh.remove(&id);
            } else if acked > row.mark {
                (row.held, row.mark) = ([row.held[1], row.grant], last);
            }
            if row.held() < was {
                fell.push(id);
            }
        }
        fell
    }

    /// Heard from within a lease: its grants still count.
    fn live(&self, lease: Duration, now: Duration) -> bool {
        now < self.heard + lease
    }
}

#[derive(Clone, Debug, Hash)]
struct Row {
    used: Used,
    demand: u64,
    /// The grant last sent.
    grant: u64,
    /// The most the node may hold, in two generations: grants set before `mark` and since. A raise counts from when
    /// it is set, a lower once the node took in an answer built after it. The older generation goes once the node
    /// takes in an answer sent after `mark`, since a grant that moves again before each acknowledgement would
    /// otherwise count its highest for as long as it moves.
    held: [u64; 2],
    /// The last grants message when the generations last moved.
    mark: u64,
    /// The first grants message from which on every one to the node carries the grant as it is.
    changed: u64,
    reported: Duration,
    /// When a pulled hold came in that the node has not reported the key since. It stands in for the grant of the
    /// replica this one replaces, which a node whose view lags still admits on while it leaves this one's grants out,
    /// yet acknowledges the answers: only an answer after the node reported the key here, which its view then shows
    /// this one serving, ends it.
    pulled: Option<Duration>,
}

impl Row {
    fn new(used: Used, demand: u64, now: Duration) -> Self {
        Self { used, demand, grant: 0, held: [0; 2], mark: 0, changed: 0, reported: now, pulled: None }
    }

    fn held(&self) -> u64 {
        self.held[0].max(self.held[1])
    }
}

#[derive(Clone, Debug, Hash)]
struct Key {
    max: u64,
    width: Duration,
    rows: Rows,
}

/// A key's rows in node order. A key has a row per node that used it lately, most of them one or a few, and a
/// `BTreeMap` allocates room for eleven: that made a key with one row 1.7 kB (benchmarks.md § Quotas).
#[derive(Clone, Debug, Default, Hash)]
struct Rows(Vec<(Peer, Row)>);

impl Rows {
    fn at(&self, peer: &Peer) -> Result<usize, usize> {
        self.0.binary_search_by(|(p, _)| p.cmp(peer))
    }

    fn get(&self, peer: &Peer) -> Option<&Row> {
        self.at(peer).ok().map(|i| &self.0[i].1)
    }

    fn get_mut(&mut self, peer: &Peer) -> Option<&mut Row> {
        self.at(peer).ok().map(|i| &mut self.0[i].1)
    }

    /// `peer`'s row, made by `new` if it has none, and whether it was made.
    fn entry(&mut self, peer: &Peer, new: impl FnOnce() -> Row) -> (&mut Row, bool) {
        let (i, made) = match self.at(peer) {
            Ok(i) => (i, false),
            Err(i) => {
                // A vector's first allocation holds four.
                self.0.reserve_exact(1);
                self.0.insert(i, (peer.clone(), new()));
                (i, true)
            }
        };
        (&mut self.0[i].1, made)
    }

    fn remove(&mut self, peer: &Peer) -> Option<Row> {
        self.at(peer).ok().map(|i| self.0.remove(i).1)
    }

    fn retain(&mut self, mut keep: impl FnMut(&Peer, &mut Row) -> bool) {
        self.0.retain_mut(|(peer, row)| keep(peer, row));
    }

    fn iter(&self) -> impl Iterator<Item = (&Peer, &Row)> {
        self.0.iter().map(|(peer, row)| (peer, row))
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&Peer, &mut Row)> {
        self.0.iter_mut().map(|(peer, row)| (&*peer, row))
    }

    fn values(&self) -> impl Iterator<Item = &Row> {
        self.0.iter().map(|(_, row)| row)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
impl std::ops::Index<&Peer> for Rows {
    type Output = Row;

    fn index(&self, peer: &Peer) -> &Row {
        &self.0[self.at(peer).expect("a row of that peer")].1
    }
}

impl Key {
    fn used(&self, now: Duration) -> u64 {
        self.rows.values().fold(0, |sum, r| sum.saturating_add(r.used.at(now)))
    }

    /// Folds a departed process's rate usage into the one row of departed processes, its drop times rounded up to a
    /// bucket so that the row stays a window long whatever number of processes departed.
    fn depart(&mut self, used: &Used, now: Duration) {
        let (Used::Rate(drops), false) = (used, self.width.is_zero()) else { return };
        let (row, _) = self.rows.entry(&departed(), || Row::new(Used::Rate(Vec::new()), 0, now));
        let Used::Rate(kept) = &mut row.used else { return };
        let width = self.width.as_nanos();
        let mut merged = BTreeMap::new();
        for (at, units) in kept.iter().chain(drops).filter(|(at, _)| *at > now) {
            let at = Duration::from_nanos((at.as_nanos().div_ceil(width) * width) as u64);
            *merged.entry(at).or_insert(0) += units;
        }
        *kept = merged.into_iter().collect();
    }
}

/// The row of departed processes, whose usage still counts until it leaves the window.
fn departed() -> Peer {
    (Arc::from(""), 0)
}

/// The grants a key's rows ask for, in row order: max-min fair. Every row that asks gets one level, but never more
/// than its usage and demand; what no row needs is spread evenly over every row, so that one that uses the key only
/// now and then is not refused while that spread covers its cost. A row using more than the level gets the level, and
/// admits again once its usage drains below it: floored at its usage, a row over its share kept it, refilling as its
/// window slid, while a row whose window had just emptied got nothing, over a second after a node's fallback took the
/// key over its limit, in the World. The level does not follow a row's usage or demand while those are on either side
/// of it, so replicas whose rows differ in small moves still give the same grants; a row asking little does not take a
/// share it would not use. Remainders go to the rows first in order. A pure function of the rows. `None` is a row not
/// to grant.
pub(crate) fn targets(max: u64, rows: &[(u64, Option<u64>)]) -> Vec<u64> {
    let shares = |r: &&(u64, Option<u64>)| r.1.is_some();
    let need = |r: &(u64, Option<u64>)| r.0.saturating_add(r.1.unwrap_or(0));
    let fixed = rows.iter().filter(|r| !shares(r)).fold(0u64, |sum, r| sum.saturating_add(r.0));
    let budget = max.saturating_sub(fixed);
    let at = |level: u64| rows.iter().filter(shares).fold(0, |sum: u64, r| sum.saturating_add(level.min(need(r))));
    let mut out = vec![0; rows.len()];
    let (mut level, mut hi) = (0, max);
    while level < hi {
        let mid = level + (hi - level).div_ceil(2);
        if at(mid) <= budget { level = mid } else { hi = mid - 1 }
    }
    let mut left = budget - at(level);
    for (i, r) in rows.iter().enumerate().filter(|(_, r)| shares(r)) {
        out[i] = level.min(need(r));
        if left > 0 && level < need(r) {
            (out[i], left) = (out[i] + 1, left - 1);
        }
    }
    // Left over only once every row that asks has its need.
    let n = rows.iter().filter(shares).count() as u64;
    for (k, (i, _)) in rows.iter().enumerate().filter(|(_, r)| shares(r)).enumerate() {
        out[i] += left / n + u64::from((k as u64) < left % n);
    }
    out
}

/// One replica node's rows and grants, for every key it replicates.
#[derive(Clone, Debug, Hash)]
pub(crate) struct Replica {
    incarnation: u128,
    lease: Duration,
    /// Pulling the rows of the other candidates: young for every key after a start, for the keys handed to it after a
    /// handover.
    pulling: bool,
    /// While pulling after handovers: this node, and the candidates before each; a key is young unless this node
    /// replicated it in every one of them.
    handed: Option<(Arc<str>, Vec<Candidates>)>,
    /// Young after a pause, until every node has reported again.
    quiet_until: Duration,
    /// Every key was granted again since it was last young.
    settled: bool,
    ticked: Duration,
    /// When `tick` last walked the rows.
    walked: Duration,
    /// The last grants message, to any node: numbered across nodes, so that a node this replica forgot and hears from
    /// again never sees a number it has taken in already.
    gseq: u64,
    peers: BTreeMap<Peer, PeerState>,
    keys: BTreeMap<u64, Key>,
    /// Reports that named another maximum for a key than the one before.
    pub mismatched: u64,
}

impl Replica {
    /// Young until `pulled`: it has not got the rows of the key's other replicas yet. It frees a node's
    /// grants `lease` after it last heard from that node.
    pub(crate) fn new(incarnation: u128, lease: Duration, now: Duration) -> Self {
        Self {
            incarnation,
            lease,
            pulling: true,
            handed: None,
            quiet_until: now,
            settled: false,
            ticked: now,
            walked: now,
            gseq: 0,
            peers: BTreeMap::new(),
            keys: BTreeMap::new(),
            mismatched: 0,
        }
    }

    /// Young for every key: its grants do not count. After a handover it is young only for the keys handed to it.
    pub(crate) fn young(&self, now: Duration) -> bool {
        self.pulling && self.handed.is_none() || now < self.quiet_until
    }

    /// Pulling, young for every key or for the keys handed to it.
    pub(crate) fn pulling(&self) -> bool {
        self.pulling
    }

    /// Pulling after its start, young for every key: what `/readyz` waits for. A handover leaves it ready.
    pub(crate) fn starting(&self) -> bool {
        self.pulling && self.handed.is_none()
    }

    /// Whether it went unticked long enough that it must have been paused: a driver ticks before it answers then.
    pub(crate) fn paused(&self, now: Duration) -> bool {
        now.saturating_sub(self.ticked) > self.lease / 2
    }

    pub(crate) fn keys(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn rows(&self) -> usize {
        self.keys.values().map(|k| k.rows.len()).sum()
    }

    /// Whether it is young; once it no longer is, grants every key again, since a node takes in the zeros it sent
    /// while young as soon as it is not.
    fn wake(&mut self, now: Duration) -> bool {
        let young = self.young(now);
        if !young && !self.settled {
            self.settled = true;
            let ids: Vec<u64> = self.keys.keys().copied().collect();
            self.regrant_each(ids, now);
        }
        young
    }

    /// Takes in a node's report and answers it, or `None` for one older than what it has. Every entry reported is
    /// answered with its grant, zero or more: a node counts only replicas that know it, and one that dropped the
    /// entry, or missed an answer, gets it again.
    pub(crate) fn report(&mut self, node: &Arc<str>, report: &Report, now: Duration) -> Option<Grants> {
        let peer = (node.clone(), report.incarnation);
        let young = self.wake(now);
        let next = self.gseq + 1;
        let Self { peers, keys, lease, mismatched, gseq, incarnation, .. } = self;
        let p = peers.entry(peer.clone()).or_insert_with(|| PeerState::new(now));
        if report.seq <= p.seq {
            return None;
        }
        let freed = !p.live(*lease, now);
        if freed {
            // Its grants were freed: they start again from zero, and the node hears so before it trusts them again.
            for &id in &p.keys {
                if let Some(row) = keys.get_mut(&id).and_then(|k| k.rows.get_mut(&peer)) {
                    (row.grant, row.held, row.changed, row.pulled) = (0, [0; 2], next, None);
                    p.pending.insert(id);
                    p.fresh.insert(id);
                }
            }
        }
        (p.seq, p.heard) = (report.seq, now);
        let acked = if report.replica == *incarnation { report.acked } else { 0 };
        let fell = p.settle(&peer, keys, acked, *gseq);
        for item in &report.items {
            let width = match item.usage {
                Reported::Rate { width, .. } => width,
                Reported::Permits(_) => Duration::ZERO,
            };
            let key = keys.entry(item.id).or_insert_with(|| Key { max: item.max, width, rows: Rows::default() });
            if key.max != item.max {
                *mismatched += 1;
                key.max = item.max;
            }
            let (row, _) = key.rows.entry(&peer, || Row::new(Used::Permits(0), 0, now));
            (row.used, row.demand, row.reported, row.pulled) = (Used::of(&item.usage, now), item.demand, now, None);
            if p.pending.insert(item.id) {
                row.changed = next;
            }
            p.keys.insert(item.id);
        }
        let mut reported: Vec<u64> = report.items.iter().map(|i| i.id).collect();
        reported.sort_unstable();
        reported.dedup();
        if !young {
            // The room an acknowledged lower freed goes to the rows that wait for it now.
            let mut touched: Vec<u64> = if freed { p.keys.iter().copied().collect() } else { fell };
            touched.extend(&reported);
            touched.sort_unstable();
            touched.dedup();
            self.regrant_each(touched, now);
        }
        Some(self.grants(&peer, report.seq, Some(&reported), young, now))
    }

    /// The next grants message to `peer`, answering its report `answers`: the grants of the entries an answer's report
    /// named, those set since the last message to the node, at most `RESEND`, and in an answer at most `RESEND` more
    /// of those it has not acknowledged, from where the last answer's walk stopped, so that each goes again until it is
    /// acknowledged. The walk visits at most that many, and a message at most `MAX_ITEMS`.
    fn grants(&mut self, peer: &Peer, answers: u64, reported: Option<&[u64]>, young: bool, now: Duration) -> Grants {
        let next = self.gseq + 1;
        let Self { peers, keys, handed: h, .. } = self;
        let p = peers.get_mut(peer).expect("a peer that reported");
        let (answer, reported) = (reported.is_some(), reported.unwrap_or_default());
        let reported = &reported[..reported.len().min(MAX_ITEMS)];
        let room = |taken: usize| RESEND.min(MAX_ITEMS - reported.len() - taken);
        let others = |id: &u64| reported.binary_search(id).is_err();
        let fresh: Vec<u64> = p.fresh.iter().copied().filter(others).take(room(0)).collect();
        let mut walk = Vec::new();
        if answer {
            let new = |id: &u64| others(id) && fresh.binary_search(id).is_err();
            let from = p.cursor;
            walk = p
                .pending
                .range(from..)
                .chain(p.pending.range(..from))
                .copied()
                .filter(new)
                .take(room(fresh.len()))
                .collect();
            p.cursor = walk.last().map_or(from, |id| id.wrapping_add(1));
        }
        let mut entries = Vec::with_capacity(reported.len() + fresh.len() + walk.len());
        for &id in reported.iter().chain(&fresh).chain(&walk) {
            p.fresh.remove(&id);
            debug_assert!(p.pending.contains(&id), "each is reported, fresh or walked, all pending");
            let Some((key, row)) = keys.get(&id).and_then(|k| Some((k, k.rows.get(peer)?))) else {
                p.pending.remove(&id);
                continue;
            };
            // A key handed to it has no grant until it pulled, so the node counts it among the replicas that gave none.
            if !handed(h, id) {
                entries.push((id, row.grant, key.used(now)));
            }
        }
        if p.sent.len() == UNACKED {
            p.sent.pop_front();
        }
        p.sent.push_back((next, entries.iter().map(|e| e.0).collect()));
        p.push = false;
        self.gseq = next;
        Grants { incarnation: self.incarnation, answers, gseq: next, young, entries }
    }

    /// Takes in a node's acknowledgement of pushed grants, from its reply: grants message `acked` of this
    /// replica's incarnation `replica`.
    pub(crate) fn ack(&mut self, peer: &Peer, replica: u128, acked: u64, now: Duration) {
        let young = self.wake(now);
        let Some(p) = self.peers.get_mut(peer).filter(|_| replica == self.incarnation) else { return };
        let fell = p.settle(peer, &mut self.keys, acked, self.gseq);
        if !young {
            self.regrant_each(fell, now);
        }
    }

    /// `regrant`s each of `ids` but those handed to it, a change first carried by the next grants message.
    fn regrant_each(&mut self, ids: Vec<u64>, now: Duration) {
        for id in ids.into_iter().filter(|id| !handed(&self.handed, *id)) {
            regrant(&mut self.keys, &mut self.peers, id, self.lease, self.gseq + 1, now);
        }
    }

    /// Grants to push now, at most one message per node a report cycle: each answers the last report the node sent, and
    /// carries the grants set since the last message to it.
    pub(crate) fn pushes(&mut self, now: Duration) -> Vec<(Peer, Grants)> {
        if self.young(now) {
            return Vec::new();
        }
        let (lease, mut out) = (self.lease, Vec::new());
        let due: Vec<Peer> = self
            .peers
            .iter()
            .filter(|(_, p)| p.push && p.live(lease, now) && now >= p.pushed + REPORT_EVERY)
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in due {
            let p = self.peers.get_mut(&peer).expect("listed above");
            if p.fresh.is_empty() {
                p.push = false;
                continue;
            }
            p.pushed = now;
            let answers = p.seq;
            let grants = self.grants(&peer, answers, None, false, now);
            out.push((peer, grants));
        }
        out
    }

    /// `id`'s rows in node order, as `targets` sees them, each with the grant it plans: what `regrant` moves each
    /// grant towards.
    #[cfg(test)]
    pub(crate) fn plan(&self, id: u64, now: Duration) -> Vec<(Peer, (u64, Option<u64>), u64)> {
        let Some(key) = self.keys.get(&id) else { return Vec::new() };
        let asks = asks(key, &self.peers, self.lease, now);
        let targets = targets(key.max, &asks);
        key.rows.iter().zip(asks).zip(targets).map(|(((peer, _), ask), target)| (peer.clone(), ask, target)).collect()
    }

    /// Notices a pause and, once a `WALK_EVERY`, drops rows no one needs and lowers to zero the grants of rows nobody
    /// reported for long.
    pub(crate) fn tick(&mut self, now: Duration) {
        if self.paused(now) {
            // Its rows are stale and its clock jumped: its grants wait for a round of fresh reports.
            (self.quiet_until, self.settled) = (now + HEARTBEAT + REPORT_EVERY, false);
        }
        self.ticked = now;
        let mut stale = Vec::new();
        if now >= self.walked + WALK_EVERY {
            self.walked = now;
            let Self { peers, keys, .. } = self;
            keys.retain(|id, key| {
                key.rows.retain(|peer, row| {
                    if let Used::Rate(drops) = &mut row.used {
                        drops.retain(|(at, _)| *at > now);
                    }
                    // The node dropped the key, or its view never moved it here: the next acknowledgement ends the
                    // hold.
                    if row.pulled.is_some_and(|at| now >= at + ROW_KEEP) {
                        row.pulled = None;
                    }
                    // Its node refreshes the permits it holds: one it has not refreshed for this long no longer
                    // reports the key here, its view having moved the key, and holds nothing here.
                    if let Used::Permits(held) = &mut row.used
                        && now >= row.reported + ROW_KEEP
                    {
                        *held = 0;
                    }
                    let p = peers.get_mut(peer);
                    let silent = p.as_ref().is_none_or(|p| now >= p.heard + SILENT);
                    let empty = row.used.at(now) == 0;
                    let unused = empty && row.held() == 0 && row.grant == 0 && now >= row.reported + ROW_KEEP;
                    let gone = unused || silent && (empty || matches!(row.used, Used::Permits(_)));
                    if let (Some(p), true) = (p, gone) {
                        p.keys.remove(id);
                        p.pending.remove(id);
                        p.fresh.remove(id);
                    }
                    if !gone && empty && row.grant > 0 && now >= row.reported + ROW_KEEP {
                        stale.push(*id);
                    }
                    !gone
                });
                !key.rows.is_empty()
            });
            peers.retain(|_, p| !p.keys.is_empty() || now < p.heard + SILENT);
        }
        if !self.wake(now) {
            stale.dedup();
            self.regrant_each(stale, now);
        }
    }

    /// The view moved from `old` to `new`. Retires the processes it shows gone: their node left, or restarted as a
    /// later incarnation, which retires every earlier one, even one it heard from late that no view here showed
    /// (§2.4). When `new` hands `me` keys, it is young for them until `pulled` (§2.6). Returns whether to pull, from
    /// every other candidate of `new`.
    pub(crate) fn see(&mut self, me: &str, old: &Members, new: &Members, now: Duration) -> bool {
        self.retire(
            |node, inc| match new.incarnation(node) {
                Some(shown) => shown > inc,
                None => old.incarnation(node).is_some_and(|was| was >= inc),
            },
            now,
        );
        if !new.candidates.gains(&old.candidates, me) {
            return false;
        }
        if !self.pulling {
            (self.pulling, self.handed) = (true, Some((Arc::from(me), Vec::new())));
        }
        if let Some((_, before)) = &mut self.handed {
            before.push(old.candidates.clone());
        }
        true
    }

    /// Retires the processes `gone` names: their permits went with them, and their rate usage stays until it leaves
    /// the window, in the row of departed processes.
    fn retire(&mut self, gone: impl Fn(&str, u128) -> bool, now: Duration) {
        let peers: Vec<Peer> =
            self.peers.keys().filter(|(node, incarnation)| gone(node, *incarnation)).cloned().collect();
        for peer in peers {
            let Some(p) = self.peers.remove(&peer) else { continue };
            for id in p.keys {
                if let Some(key) = self.keys.get_mut(&id)
                    && let Some(row) = key.rows.remove(&peer)
                {
                    key.depart(&row.used, now);
                }
            }
        }
    }

    /// Rows of the keys from `from` on that `want` names, a page of at most `max` unless one key has more, and where
    /// the next page starts.
    pub(crate) fn pull(
        &self,
        from: u64,
        max: usize,
        want: impl Fn(u64) -> bool,
        now: Duration,
    ) -> (Vec<Pulled>, Option<u64>) {
        let mut rows = Vec::new();
        for (id, key) in self.keys.range((Included(from), Unbounded)).filter(|(id, _)| want(**id)) {
            if !rows.is_empty() && rows.len() + key.rows.len() > max {
                return (rows, Some(*id));
            }
            rows.extend(key.rows.iter().map(|((node, incarnation), row)| Pulled {
                id: *id,
                max: key.max,
                width: key.width,
                node: node.clone(),
                incarnation: *incarnation,
                used: row.used.shift(now, Duration::ZERO),
                demand: row.demand,
                held: row.held(),
            }));
        }
        (rows, None)
    }

    /// Takes in a page of another replica's rows of the keys it is young for, keeping the larger usage and the larger
    /// hold where both have a row. The hold stands in for the grants of the incarnation this one replaces, which it
    /// cannot know: a node admits on a majority, so one of the others it pulls from gave the node at least as much. It
    /// counts until the node, having reported the key here since, acknowledges an answer carrying it; or a lease after
    /// the pull if the node stays silent, or `ROW_KEEP` after it if the node never reports the key. A row of an
    /// incarnation `view` shows a later one of is retired as it arrives; one of a node it does not show may be of one
    /// that joined since, and stays.
    pub(crate) fn absorb(&mut self, rows: Vec<Pulled>, view: &Members, now: Duration) {
        let next = self.gseq + 1;
        for r in rows {
            if self.handed.is_some() && !handed(&self.handed, r.id) {
                continue;
            }
            let peer = (r.node, r.incarnation);
            let key =
                self.keys.entry(r.id).or_insert_with(|| Key { max: r.max, width: r.width, rows: Rows::default() });
            let used = r.used.shift(Duration::ZERO, now);
            if view.incarnation(&peer.0).is_some_and(|shown| shown > peer.1) {
                key.depart(&used, now);
                continue;
            }
            let held = r.held;
            let (row, made) =
                key.rows.entry(&peer, || Row { held: [0, held], ..Row::new(used.clone(), r.demand, now) });
            if !made {
                row.held[1] = row.held[1].max(r.held);
                if row.used.at(now) < used.at(now) {
                    row.used = used;
                }
            }
            if !peer.0.is_empty() {
                let p = self.peers.entry(peer).or_insert_with(|| PeerState::new(now));
                p.keys.insert(r.id);
                // Answers carry the entry, even to a node that no longer reports it, so that the hold can end.
                if row.held() > row.grant {
                    p.set(r.id);
                    (row.changed, row.pulled) = (next, Some(now));
                }
            }
        }
    }

    /// The pull is done: its grants count from now on, and it grants every key again.
    pub(crate) fn pulled(&mut self) {
        if std::mem::take(&mut self.pulling) {
            (self.handed, self.settled) = (None, false);
        }
    }
}

/// A quota node's drive of its table and replica, with no I/O: the views it takes in, the reports it sends each cycle,
/// and the pulls of its replica while young. The cluster's driver and the simulation both run it.
pub(crate) struct Node {
    pub me: Arc<str>,
    pub replica: Replica,
    pub members: Arc<Members>,
    next_beat: Duration,
    /// When the sweep under way began.
    swept: Duration,
    /// Bumped with each pull, so that a page of an earlier one is dropped.
    round: u64,
    /// Per candidate still to pull from: where its next page starts, and when it was asked.
    pulls: BTreeMap<Arc<str>, (u64, Option<Duration>)>,
}

impl Node {
    /// A new process, young for every key until it pulled from every other candidate of `members`, whether they show
    /// it yet or not.
    pub(crate) fn new(
        me: &str,
        incarnation: u128,
        members: Members,
        table: &Table,
        lease: Duration,
        now: Duration,
    ) -> Self {
        let mut node = Self {
            me: me.into(),
            replica: Replica::new(incarnation, lease, now),
            members: Arc::new(members),
            next_beat: now,
            swept: now,
            round: 0,
            pulls: BTreeMap::new(),
        };
        node.configure(table);
        node.start_pull();
        node
    }

    fn configure(&self, table: &Table) {
        table.set_nodes(self.members.incarnations.len().max(1));
        table.set_key_replicas(self.members.candidates.per_key());
        table.relink(|node| self.members.incarnation(node));
    }

    fn start_pull(&mut self) {
        self.round += 1;
        self.pulls = self.members.to_pull(&self.me).map(|c| (c.clone(), (0, None))).collect();
        if self.pulls.is_empty() {
            self.replica.pulled();
        }
    }

    /// Takes in the quota nodes a view shows, if they changed (§2.2, §2.6).
    pub(crate) fn see(&mut self, table: &Table, new: Members, now: Duration) {
        if *self.members == new {
            return;
        }
        let new = Arc::new(new);
        let old = std::mem::replace(&mut self.members, new.clone());
        let pull = self.replica.see(&self.me, &old, &new, now);
        self.configure(table);
        table.forget(|id, replica| new.candidates.serves(id, replica));
        if pull {
            self.start_pull();
        } else {
            // A candidate the view no longer shows, or shows unreachable, has nothing left to pull.
            self.pulls.retain(|node, _| new.to_pull(&self.me).any(|c| c == node));
            if self.pulls.is_empty() {
                self.replica.pulled();
            }
        }
    }

    /// One report cycle: the replica's tick, and per replica node its link and report of what changed, or a heartbeat.
    pub(crate) fn round(&mut self, table: &Table, now: Duration) -> Vec<(Arc<str>, usize, Report)> {
        self.replica.tick(now);
        let into = now.saturating_sub(self.swept);
        if into >= SWEEP_EVERY {
            table.sweep(SWEEP_EVERY, now);
            self.swept = if into >= 2 * SWEEP_EVERY { now } else { self.swept + SWEEP_EVERY };
        }
        table.sweep(now.saturating_sub(self.swept), now);
        table.reported(now);
        let mut by: BTreeMap<Arc<str>, Vec<Item>> = BTreeMap::new();
        for item in table.dirty(now) {
            for r in self.members.candidates.replicas(item.id) {
                by.entry(r.clone()).or_default().push(item.clone());
            }
        }
        if now >= self.next_beat {
            self.next_beat = now + HEARTBEAT;
            for r in table.replicas() {
                by.entry(r).or_default();
            }
        }
        let incarnation = self.replica.incarnation;
        by.into_iter()
            .filter_map(|(r, items)| {
                let link = table.link(&r, self.members.incarnation(&r)?);
                let (seq, replica, acked) = table.send(link, now);
                Some((r, link, Report { incarnation, seq, replica, acked, items }))
            })
            .collect()
    }

    /// The pages to ask for now, of pull `round`: per candidate, where its page starts.
    pub(crate) fn pulls(&mut self, now: Duration) -> (u64, Vec<(Arc<str>, u64)>) {
        let mut asks = Vec::new();
        for (peer, (start, asked)) in &mut self.pulls {
            if asked.is_none_or(|at| now >= at + PATIENCE) {
                *asked = Some(now);
                asks.push((peer.clone(), *start));
            }
        }
        (self.round, asks)
    }

    /// `from`'s page of pull `round`, or `None` when `from` refused the pull for good: it has nothing this replica can
    /// take in.
    pub(crate) fn page(&mut self, from: &str, round: u64, page: Option<(Vec<Pulled>, Option<u64>)>, now: Duration) {
        let Some(ask) = self.pulls.get_mut(from).filter(|_| round == self.round) else { return };
        if let Some((rows, next)) = page {
            self.replica.absorb(rows, &self.members, now);
            if let Some(start) = next {
                *ask = (start, None);
                return;
            }
        }
        self.pulls.remove(from);
        if self.pulls.is_empty() {
            self.replica.pulled();
        }
    }

    /// Answers `node`'s report. Back from a pause before its next cycle, the replica goes quiet first (§2.6).
    pub(crate) fn answer(&mut self, node: &Arc<str>, report: &Report, now: Duration) -> Option<Grants> {
        self.woken(now).report(node, report, now)
    }

    /// Takes in `peer`'s acknowledgement of pushed grants, from its reply.
    pub(crate) fn ack(&mut self, peer: &Peer, replica: u128, acked: u64, now: Duration) {
        self.woken(now).ack(peer, replica, acked, now);
    }

    fn woken(&mut self, now: Duration) -> &mut Replica {
        if self.replica.paused(now) {
            self.replica.tick(now);
        }
        &mut self.replica
    }

    /// The grants the replica has to push, each to the process the view shows: this node's own are taken in at once,
    /// and their acknowledgement may free room to push to another. Returns those for other nodes.
    pub(crate) fn pushes(&mut self, table: &Table, now: Duration) -> Vec<(Peer, Grants)> {
        let (mut pushes, mut out) = (self.replica.pushes(now), Vec::new());
        while let Some((peer, grants)) = pushes.pop() {
            if self.members.incarnation(&peer.0) != Some(peer.1) {
                continue;
            }
            if peer.0 != self.me {
                out.push((peer, grants));
                continue;
            }
            let me = &*self.me;
            if let Some((replica, acked)) = table.pushed(me, &grants, |id| self.members.candidates.serves(id, me), now)
            {
                self.ack(&peer, replica, acked, now);
                pushes.extend(self.replica.pushes(now));
            }
        }
        out
    }

    /// A page of the rows of the keys `from` replicates, from `start` on, by the candidates `from` sent: this node's
    /// view may not show `from` yet, or still show a node that left, and would leave out keys `from` needs (§2.6).
    pub(crate) fn serve_pull(
        &self,
        from: &str,
        regions: Vec<Vec<Arc<str>>>,
        start: u64,
        now: Duration,
    ) -> (Vec<Pulled>, Option<u64>) {
        let theirs = self.members.candidates.of(regions);
        self.replica.pull(start, MAX_ITEMS, |id| theirs.serves(id, from), now)
    }
}

/// Whether `id` was handed to this replica in a handover it is still pulling for.
fn handed(handed: &Option<(Arc<str>, Vec<Candidates>)>, id: u64) -> bool {
    handed.as_ref().is_some_and(|(me, before)| before.iter().any(|c| !c.serves(id, me)))
}

/// Each row's usage, and its demand unless it is not to be granted: its node is silent, or it went unused for long.
fn asks(key: &Key, peers: &BTreeMap<Peer, PeerState>, lease: Duration, now: Duration) -> Vec<(u64, Option<u64>)> {
    let ask = |(peer, row): (&Peer, &Row)| {
        let u = row.used.at(now);
        let quiet = u == 0 && now >= row.reported + ROW_KEEP;
        let live = peers.get(peer).is_some_and(|p| p.live(lease, now));
        // A node that asks reports every heartbeat: demand older than two was lost with a report, or pulled.
        let demand = if now < row.reported + 2 * HEARTBEAT { row.demand } else { 0 };
        (u, (live && !quiet).then_some(demand))
    };
    key.rows.iter().map(ask).collect()
}

/// Moves `id`'s grants towards the plan, a change first carried by grants message `next`. The sum over nodes of
/// `max(usage, held)` stays within the limit: a lower is sent at once, a raise only as far as there is room, and a node
/// silent for a lease counts its usage alone. A raise grows from what the node holds, or from its usage while the sum
/// is within the limit: usage over it, taken on the fallback or charged, would otherwise turn into a grant that
/// outlives it. Room short of every raise is split max-min fair, as the limit is: handed out in node order, it went to
/// the first rows while a node that went silent held its grant, and the last node, refused by this replica, admitted
/// nothing on the smallest grant for as long, in the World. Where a row that wants more, by its demand or by usage over
/// its grant, is left short of its plan, the nodes holding a lower not yet acknowledged are pushed it, so that the room
/// comes back within a round trip and not at their next heartbeat; and a raise of a row that wants it is pushed too.
fn regrant(
    keys: &mut BTreeMap<u64, Key>,
    peers: &mut BTreeMap<Peer, PeerState>,
    id: u64,
    lease: Duration,
    next: u64,
    now: Duration,
) {
    let Some(key) = keys.get_mut(&id) else { return };
    let asks = asks(key, peers, lease, now);
    let plan = targets(key.max, &asks);
    let live = |peer: &Peer| peers.get(peer).is_some_and(|p| p.live(lease, now));
    let have = |row: &Row| row.used.at(now).max(row.held());
    let counted = |(peer, row): (&Peer, &Row)| if live(peer) { have(row) } else { row.used.at(now) };
    let total = key.rows.iter().map(counted).fold(0, u64::saturating_add);
    let (mut room, fits) = (key.max.saturating_sub(total), total <= key.max);
    let raise = |((peer, row), target): ((&Peer, &Row), &u64)| {
        (0, (live(peer) && *target >= row.grant).then(|| target.saturating_sub(have(row))))
    };
    let shares = targets(room, &key.rows.iter().zip(&plan).map(raise).collect::<Vec<_>>());
    let mut short = false;
    for ((((peer, row), target), share), &(u, d)) in key.rows.iter_mut().zip(plan).zip(shares).zip(&asks) {
        let Some(p) = peers.get_mut(peer).filter(|p| p.live(lease, now)) else { continue };
        let wants = d.is_some_and(|d| d > 0 || u > row.grant);
        let have = have(row);
        let base = if fits { have } else { row.held() };
        let grant =
            if target < row.grant { target } else { target.min(base.saturating_add(share.min(room))).max(row.grant) };
        room -= grant.saturating_sub(have);
        short |= wants && grant < target;
        if grant != row.grant {
            p.push |= wants && grant > row.grant;
            (row.grant, row.held[1], row.changed) = (grant, row.held[1].max(grant), next);
            p.set(id);
        }
    }
    if short {
        for (peer, _) in key.rows.iter().filter(|(_, row)| row.held() > row.grant) {
            if let Some(p) = peers.get_mut(peer).filter(|p| p.live(lease, now)) {
                p.push = true;
            }
        }
    }
}

#[cfg(test)]
mod tests;
