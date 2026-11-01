//! A seeded simulation of a stream tree under faults: one origin, two relays, four leaves, and eight keys in two
//! namespaces, each with relay timings of its own. Relays are killed and restarted, links are cut and healed, answers
//! are lost, and a request the child timed out can reach its parent late, behind the child's newer ones, its answer
//! later still. Several pushes are in flight to each child; one can be lost, come twice, or come before one sent
//! ahead of it. Every leaf subscriber must still see each record exactly once, in order.
#![expect(
    clippy::disallowed_methods,
    reason = "single-threaded and seeded: Instant::now is only the base of the virtual clock"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use axon::Lane;
use axon_stream::*;
use bytes::Bytes;
use push::{answer, decode, deliver};

mod push;

const RELAYS: [&str; 2] = ["r1", "r2"];
const LEAVES: [&str; 4] = ["l1", "l2", "l3", "l4"];

fn preferences(node: &str) -> &'static [&'static str] {
    match node {
        "o" => &[],
        "r1" | "r2" => &["o"],
        "l1" | "l3" => &["r1", "r2", "o"],
        _ => &["r2", "r1", "o"],
    }
}

fn key() -> StreamKey {
    StreamKey::new("md", "btc")
}

/// The keys of a run of the fault schedule.
const KEYS: usize = 8;

/// The round trip each parent is told it has to its children.
const RTT: Duration = Duration::from_millis(60);

/// Key `k` of a run: the first is `key()`, the first half in `md` and the rest in `tr`.
fn key_of(k: usize) -> StreamKey {
    match k {
        0 => key(),
        k if k < KEYS / 2 => StreamKey::new("md", format!("k{k}")),
        k => StreamKey::new("tr", format!("k{k}")),
    }
}

/// A leaf and the index of a key.
type Sub = (&'static str, usize);

struct Node {
    core: RelayCore,
    incarnation: u128,
    up: bool,
}

#[derive(Clone)]
struct InFlight {
    from: PeerId,
    to: PeerId,
    id: PushId,
    body: Bytes,
}

struct Sim {
    rng: u64,
    base: Instant,
    now_ms: u64,
    cfg: CoreConfig,
    nodes: BTreeMap<&'static str, Node>,
    cut: HashSet<(&'static str, &'static str)>,
    interests: Vec<(PeerId, PeerId, Interest)>,
    pushes: Vec<InFlight>,
    /// The most pushes one parent had in flight to one child.
    deepest: usize,
    keys: usize,
    /// Each key gets the same records, so one count holds for all.
    published: u64,
    subs: HashMap<Sub, u64>,
    seen: HashMap<Sub, Vec<u64>>,
    readied: HashSet<Sub>,
    /// The record each leaf's subscription must take next, from its last `Ready` or record; 0 after a `Ready` that
    /// names no position, which another must follow.
    next: HashMap<Sub, u64>,
    resets: usize,
    /// Leaves that subscribe from the first record once this many are out.
    late: Vec<(&'static str, u64)>,
}

impl Sim {
    fn origin(&mut self) -> &mut RelayCore {
        &mut self.nodes.get_mut("o").unwrap().core
    }

    fn new(seed: u64, cfg: CoreConfig, keys: usize) -> Self {
        let mut sim = Self::empty(seed, cfg, keys);
        for leaf in LEAVES {
            sim.subscribe(leaf, StartPosition::Live);
        }
        sim
    }

    /// The origin's streams are open and no leaf has subscribed.
    fn empty(seed: u64, cfg: CoreConfig, keys: usize) -> Self {
        let mut sim = Self {
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            base: Instant::now(),
            now_ms: 0,
            cfg,
            nodes: BTreeMap::new(),
            cut: HashSet::new(),
            interests: Vec::new(),
            pushes: Vec::new(),
            deepest: 0,
            keys,
            published: 0,
            subs: HashMap::new(),
            seen: HashMap::new(),
            readied: HashSet::new(),
            next: HashMap::new(),
            resets: 0,
            late: Vec::new(),
        };
        if keys > KEYS / 2 {
            // The second namespace waits on its parents by timings of its own, drawn from the seed.
            let upstream_timeout = Duration::from_millis(2000 + sim.pick(4000) as u64);
            let hold_down = Duration::from_millis(5000 + sim.pick(10_000) as u64);
            sim.cfg.timings_by_namespace.insert("tr".into(), RelayTimings { upstream_timeout, hold_down });
        }
        for name in ["o"].into_iter().chain(RELAYS).chain(LEAVES) {
            sim.boot(name, 1);
        }
        for k in 0..keys {
            sim.origin().open_stream(&key_of(k), 1, DeliveryPolicy::ReplayRequired).unwrap();
        }
        sim
    }

    fn boot(&mut self, name: &'static str, incarnation: u128) {
        let cfg = CoreConfig { relay: name != "o", ..self.cfg.clone() };
        let core = RelayCore::new(&PeerId::new(name, incarnation), AllowAll, cfg).unwrap();
        self.nodes.insert(name, Node { core, incarnation, up: true });
    }

    fn subscribe(&mut self, leaf: &'static str, start: StartPosition) {
        for k in 0..self.keys {
            let options = SubscribeOptions { start, ..Default::default() };
            let id = self.nodes.get_mut(leaf).unwrap().core.subscribe(&key_of(k), options).unwrap();
            self.subs.insert((leaf, k), id);
            self.readied.remove(&(leaf, k));
            self.next.remove(&(leaf, k));
        }
    }

    fn pick(&mut self, n: usize) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng % n as u64) as usize
    }

    fn now(&self) -> Instant {
        self.base + Duration::from_millis(self.now_ms)
    }

    fn peer(&self, name: &str) -> PeerId {
        PeerId::new(name, self.nodes[name].incarnation)
    }

    fn name(&self, peer: &PeerId) -> &'static str {
        self.nodes.keys().find(|n| **n == &*peer.node).unwrap()
    }

    fn reachable(&self, a: &'static str, b: &'static str) -> bool {
        self.nodes[a].up && self.nodes[b].up && !self.cut.contains(&(a.min(b), a.max(b)))
    }

    /// Whether `peer` is the running boot of its node and `from` can talk to it.
    fn delivers(&self, from: &PeerId, to: &PeerId) -> bool {
        let (from, target) = (self.name(from), self.name(to));
        self.reachable(from, target) && self.nodes[target].incarnation == to.incarnation
    }

    fn reconcile(&mut self, name: &'static str) {
        if !self.nodes[name].up {
            return;
        }
        let candidates: Vec<PeerId> =
            preferences(name).iter().filter(|p| self.reachable(name, p)).map(|p| self.peer(p)).collect();
        let (me, now) = (self.peer(name), self.now());
        let node = self.nodes.get_mut(name).unwrap();
        let out = node.core.reconcile(&candidates, now);
        node.core.take_wakes();
        node.core.expire_unheard(now, Duration::from_secs(10));
        self.interests.extend(out.into_iter().map(|(parent, interest)| (me.clone(), parent, interest)));
    }

    fn deliver_interest(&mut self, index: usize, lose_answer: bool) {
        let (child, parent, interest) = self.interests.swap_remove(index);
        let (child_name, now, revision) = (self.name(&child), self.now(), interest.revision);
        if self.nodes[child_name].incarnation != child.incarnation || !self.nodes[child_name].up {
            return;
        }
        let mut reply = Reply::Failed;
        if self.delivers(&child, &parent) {
            let parent_name = self.name(&parent);
            reply = self.nodes.get_mut(parent_name).unwrap().core.handle_interest(&child, interest, now).into();
        }
        let reply = if lose_answer { Reply::Failed } else { reply };
        self.nodes.get_mut(child_name).unwrap().core.interest_result(&parent, revision, reply, now);
    }

    /// The child gives up on a request that stays in flight, so its parent may get it after the child's newer ones.
    /// Giving up on one twice changes nothing.
    fn time_out_interest(&mut self, index: usize) {
        let (child, parent, interest) = &self.interests[index];
        let (child, parent, revision, now) = (child.clone(), parent.clone(), interest.revision, self.now());
        let node = self.nodes.get_mut(self.name(&child)).unwrap();
        if node.incarnation == child.incarnation && node.up {
            node.core.interest_result(&parent, revision, Reply::Failed, now);
        }
    }

    /// Every push the node's window and cap let go to each of its children.
    fn pump(&mut self, name: &'static str) {
        if !self.nodes[name].up {
            return;
        }
        let (from, now) = (self.peer(name), self.now());
        for to in self.nodes[name].core.downstream_peers() {
            while let Some((id, body)) =
                self.nodes.get_mut(name).unwrap().core.next_push(&to.node, Lane::Live, now, Some(RTT))
            {
                self.pushes.push(InFlight { from: from.clone(), to: to.clone(), id, body });
            }
            let depth = self.pushes.iter().filter(|p| p.from == from && p.to == to).count();
            self.deepest = self.deepest.max(depth);
        }
    }

    /// The oldest push in flight between the two ends of push `index`: one session delivers in order.
    fn oldest(&self, index: usize) -> usize {
        let (from, to) = (&self.pushes[index].from, &self.pushes[index].to);
        self.pushes.iter().position(|p| p.from == *from && p.to == *to).expect("push index itself")
    }

    /// Push `index` arrives, or its call fails. A lost answer fails the call, as `OutcomeUnknown` does; a duplicate
    /// leaves a copy to arrive again.
    fn deliver_push(&mut self, index: usize, lose_answer: bool, duplicate: bool) {
        let (from, to, now) = (self.pushes[index].from.clone(), self.pushes[index].to.clone(), self.now());
        let (parent, child) = (self.name(&from), self.name(&to));
        if self.nodes[parent].incarnation != from.incarnation || !self.nodes[parent].up {
            self.pushes.remove(index);
            return;
        }
        // Sent to one boot of the child: the next one refuses it unsent, StaleNode.
        if self.reachable(parent, child) && self.nodes[child].incarnation != to.incarnation {
            self.pushes.remove(index);
            return self.nodes.get_mut(parent).unwrap().core.expire_downstream(&to, now);
        }
        let sent = if duplicate { self.pushes[index].clone() } else { self.pushes.remove(index) };
        if !self.delivers(&from, &to) {
            return self.nodes.get_mut(parent).unwrap().core.push_failed(&to, Lane::Live, sent.id, now);
        }
        let answer = answer(&mut self.nodes.get_mut(child).unwrap().core, &from, &sent.body, now);
        let core = &mut self.nodes.get_mut(parent).unwrap().core;
        if lose_answer {
            return core.push_failed(&to, Lane::Live, sent.id, now);
        }
        core.push_answered(&to, Lane::Live, sent.id, answer, now);
    }

    fn publish(&mut self, count: u64) {
        let records: Vec<Bytes> =
            (1..=count).map(|n| Bytes::copy_from_slice(&(self.published + n).to_be_bytes())).collect();
        for k in 0..self.keys {
            self.origin().publish(&key_of(k), records.clone()).unwrap();
        }
        self.published += count;
        let late = std::mem::take(&mut self.late);
        let (due, late): (Vec<_>, Vec<_>) = late.into_iter().partition(|(_, at)| *at <= self.published);
        self.late = late;
        for (leaf, _) in due {
            self.subscribe(leaf, StartPosition::First);
        }
    }

    /// Drains a leaf's subscribers, checking that positions and contents are contiguous.
    fn consume(&mut self, leaf: &'static str, may_reset: bool) {
        for k in 0..self.keys {
            self.consume_key((leaf, k), may_reset);
        }
    }

    fn consume_key(&mut self, sub: Sub, may_reset: bool) {
        let Some(&id) = self.subs.get(&sub) else { return };
        let leaf = sub.0;
        let core = &mut self.nodes.get_mut(leaf).unwrap().core;
        let seen = self.seen.entry(sub).or_default();
        while let Some(event) = core.poll(id) {
            match event {
                StreamEvent::Batch(batch) => {
                    let next = self.next.insert(sub, batch.first() + batch.len() as u64);
                    assert!(
                        next.is_none_or(|n| n == batch.first()),
                        "{leaf} took {} where {next:?} was next",
                        batch.first()
                    );
                    for (sequence, record) in (batch.first()..).zip(batch.records()) {
                        assert_eq!(u64::from_be_bytes(record[..].try_into().unwrap()), sequence);
                        assert!(
                            may_reset || sequence == seen.len() as u64 + 1,
                            "{leaf} saw {sequence} after {}",
                            seen.len()
                        );
                        assert!(
                            seen.last().is_none_or(|last| sequence > *last),
                            "{leaf} saw a duplicate of {sequence}"
                        );
                        seen.push(sequence);
                    }
                }
                StreamEvent::ResetRequired(reason) => {
                    assert!(may_reset, "{leaf} was reset: {reason:?}");
                    self.resets += 1;
                    let options = SubscribeOptions::default();
                    *self.subs.get_mut(&sub).unwrap() = core.subscribe(&key_of(sub.1), options).unwrap();
                    self.readied.remove(&sub);
                    self.next.remove(&sub);
                }
                StreamEvent::Ready { start } => {
                    self.next.insert(sub, start.map_or(0, |s| s.sequence + 1));
                    if start.is_some() && !self.readied.insert(sub) {
                        assert!(may_reset, "{leaf} was moved");
                        self.resets += 1;
                    }
                }
                StreamEvent::End(reason) => panic!("{leaf} ended: {reason:?}"),
                // The origin never goes: a close is not a loss.
                StreamEvent::OriginLost => panic!("{leaf} was told its origin is lost"),
                _ => {}
            }
        }
    }

    fn step(&mut self, chaos: bool, may_reset: bool) {
        self.now_ms += self.pick(40) as u64;
        let names: Vec<&'static str> = self.nodes.keys().copied().collect();
        match self.pick(if chaos { 14 } else { 10 }) {
            0 | 1 => {
                let name = names[self.pick(names.len())];
                self.reconcile(name);
            }
            2 | 3 => {
                let name = names[self.pick(names.len())];
                self.pump(name);
            }
            4 | 5 if !self.interests.is_empty() => {
                let (index, lose) = (self.pick(self.interests.len()), chaos && self.pick(8) == 0);
                if chaos && self.pick(8) == 0 {
                    self.time_out_interest(index);
                } else {
                    self.deliver_interest(index, lose);
                }
            }
            6 | 7 if !self.pushes.is_empty() => {
                let index = self.pick(self.pushes.len());
                // Mostly in order: pushes of two sessions, an old one and its redial, can arrive out of it.
                let index = if chaos && self.pick(8) == 0 { index } else { self.oldest(index) };
                let (lose, duplicate) = (chaos && self.pick(8) == 0, chaos && self.pick(8) == 0);
                self.deliver_push(index, lose, duplicate);
            }
            8 => {
                let leaf = LEAVES[self.pick(LEAVES.len())];
                self.consume(leaf, may_reset);
            }
            9 if chaos => {
                let count = 1 + self.pick(3) as u64;
                self.publish(count);
            }
            10 => {
                let relay = RELAYS[self.pick(2)];
                let node = self.nodes.get_mut(relay).unwrap();
                if node.up {
                    node.up = false;
                } else {
                    let incarnation = node.incarnation + 1;
                    self.boot(relay, incarnation);
                }
            }
            11 => {
                let (a, b) = if self.pick(3) == 0 {
                    ("o", RELAYS[self.pick(2)])
                } else {
                    (LEAVES[self.pick(LEAVES.len())], RELAYS[self.pick(2)])
                };
                let pair = (a.min(b), a.max(b));
                if !self.cut.remove(&pair) {
                    self.cut.insert(pair);
                }
            }
            12 => self.now_ms += self.pick(4000) as u64,
            _ => {}
        }
    }

    /// Heals everything and runs rounds until the tree has long settled.
    fn settle(&mut self, may_reset: bool) {
        self.cut.clear();
        for relay in RELAYS {
            if !self.nodes[relay].up {
                let incarnation = self.nodes[relay].incarnation + 1;
                self.boot(relay, incarnation);
            }
        }
        for _ in 0..400 {
            self.round(may_reset);
        }
    }

    /// 100 ms on, then every node reconciles and everything it sends is delivered.
    fn round(&mut self, may_reset: bool) {
        self.now_ms += 100;
        let names: Vec<&'static str> = self.nodes.keys().copied().collect();
        for name in names {
            self.reconcile(name);
            while !self.interests.is_empty() {
                self.deliver_interest(0, false);
            }
            self.pump(name);
            while !self.pushes.is_empty() {
                self.deliver_push(0, false, false);
            }
        }
        for leaf in LEAVES {
            self.consume(leaf, may_reset);
        }
    }
}

fn run(seed: u64, cfg: CoreConfig, may_reset: bool) -> Sim {
    faults(Sim::new(seed, cfg, KEYS), seed, may_reset)
}

/// The origin has published 40 records before any leaf subscribes, and every leaf starts from the first: two at
/// once, the other two only once more are out, while relays fail and return.
fn late(seed: u64, cfg: CoreConfig, may_reset: bool) -> Sim {
    let mut sim = Sim::empty(seed, cfg, KEYS);
    for _ in 0..10 {
        sim.publish(4);
    }
    for leaf in ["l1", "l2"] {
        sim.subscribe(leaf, StartPosition::First);
    }
    sim.late = vec![("l3", 41 + sim.pick(100) as u64), ("l4", 41 + sim.pick(200) as u64)];
    let sim = faults(sim, seed, may_reset);
    assert!(sim.late.is_empty(), "seed {seed}: a leaf never subscribed");
    sim
}

/// A calm start, then faults, then the checks that hold once everything has healed.
fn faults(mut sim: Sim, seed: u64, may_reset: bool) -> Sim {
    for _ in 0..600 {
        sim.step(false, may_reset);
    }
    sim.settle(may_reset);
    for _ in 0..4000 {
        sim.step(true, may_reset);
    }
    sim.settle(may_reset);
    sim.publish(1);
    sim.settle(may_reset);
    for leaf in LEAVES {
        for k in 0..sim.keys {
            let parent = sim.nodes[leaf].core.parent_of(&key_of(k)).map(|p| p.node.to_string());
            let moved = parent.as_deref() == Some(preferences(leaf)[0]);
            assert!(moved, "seed {seed}: {leaf} {k} did not move back to its relay");
            assert_eq!(sim.seen[&(leaf, k)].last(), Some(&sim.published), "seed {seed}: {leaf} {k} did not catch up");
        }
    }
    let origin = sim.nodes["o"].core.stats();
    let feeding: Vec<_> = origin.downstream.iter().filter(|d| d.branches > 0).map(|d| d.node.as_str()).collect();
    assert_eq!(feeding, RELAYS, "seed {seed}: the origin feeds each relay once and no leaf");
    assert_eq!(origin.hub.queued_records, 0);
    for (name, node) in &sim.nodes {
        let pushes: usize = node.core.stats().downstream.iter().map(|d| d.inflight_pushes).sum();
        assert_eq!(pushes, 0, "seed {seed}: {name} holds pushes once everything healed");
    }
    sim
}

/// Every record of every key once, in order, at each leaf.
fn every_record_once(sim: &Sim, seed: u64) {
    for leaf in LEAVES {
        for k in 0..sim.keys {
            assert_eq!(sim.seen[&(leaf, k)], (1..=sim.published).collect::<Vec<_>>(), "seed {seed} {leaf} {k}");
        }
    }
}

#[test]
fn every_leaf_sees_every_record_once_while_relays_fail_and_return() {
    let (mut switches, mut lost, mut deepest) = (0, 0, 0);
    for seed in 0..300 {
        let sim = run(seed, CoreConfig::default(), false);
        every_record_once(&sim, seed);
        for leaf in LEAVES {
            let stats = sim.nodes[leaf].core.stats();
            switches += stats.switches;
            lost += stats.parents_lost;
        }
        assert!(sim.published > 100);
        deepest = deepest.max(sim.deepest);
    }
    assert!(switches > 300 && lost > 300, "the schedule barely exercised failover: {switches} switches, {lost} losses");
    assert!(deepest > 1, "never more than one push in flight to a child");
}

#[test]
fn pushes_resent_after_a_lost_answer_are_applied_once() {
    let (mut resent, mut duplicates, mut gaps) = (0, 0, 0);
    for seed in 0..100 {
        let sim = run(seed, CoreConfig::default(), false);
        every_record_once(&sim, seed);
        for node in sim.nodes.values() {
            let stats = node.core.stats();
            (resent, duplicates, gaps) =
                (resent + stats.push_resent, duplicates + stats.push_skipped[0], gaps + stats.push_skipped[1]);
        }
    }
    println!("{resent} pushes sent again, {duplicates} duplicates and {gaps} gaps skipped");
    assert!(resent > 0 && duplicates > 0 && gaps > 0, "{resent} resent, {duplicates} duplicates, {gaps} gaps");
}

#[test]
fn a_late_leaf_that_starts_from_the_first_record_gets_the_origin_history_through_relays() {
    for seed in 0..150 {
        every_record_once(&late(seed, CoreConfig::default(), false), seed);
    }
}

#[test]
fn the_same_seed_gives_the_same_run() {
    for schedule in [run, late] {
        let (a, b) = (schedule(11, CoreConfig::default(), true), schedule(11, CoreConfig::default(), true));
        assert_eq!((&a.seen, a.published, a.resets), (&b.seen, b.published, b.resets));
        for name in a.nodes.keys() {
            let stats = |sim: &Sim| format!("{:?}", sim.nodes[name].core.stats());
            assert_eq!(stats(&a), stats(&b), "{name}");
        }
    }
}

#[test]
fn history_that_nobody_holds_is_an_explicit_reset_never_a_silent_gap() {
    let mut cfg = CoreConfig::default();
    cfg.hub.max_batch_records = 8;
    cfg.store.max_records_per_stream = 8;
    cfg.branch.max_records = 8;
    let mut resets = 0;
    for seed in 0..150 {
        resets += run(seed, cfg.clone(), true).resets;
        // No ring holds the first of the 40 records the late leaves ask for.
        assert!(late(seed, cfg.clone(), true).resets >= LEAVES.len(), "seed {seed}: a first start was not reset");
    }
    assert!(resets > 0, "the schedule never lost history");
}

/// A position no ring holds is given up only once no parent has appeared for `upstream_timeout`, as one that appeared
/// last may hold it. In a view that has stood that long the reset is at once, so the waits of a relay and its leaf do
/// not add up. A parent that left and came back between two reconciles has appeared as well: before, it kept the
/// time it first appeared, and the leaf gave up at once.
#[test]
fn a_position_no_ring_holds_is_given_up_at_once_in_a_settled_view_and_after_upstream_timeout_in_a_new_one() {
    let mut cfg = CoreConfig::default();
    cfg.hub.max_batch_records = 8;
    cfg.store.max_records_per_stream = 8;
    cfg.branch.max_records = 8;
    let timeout = cfg.timings.upstream_timeout;
    let reset_after = |settled: bool, flapped: bool| {
        let mut sim = Sim::empty(1, cfg.clone(), 1);
        if settled {
            sim.settle(false);
        }
        for _ in 0..10 {
            sim.publish(4);
        }
        if flapped {
            let now = sim.now();
            sim.nodes.get_mut("l1").unwrap().core.parents_left(["r2"], now);
        }
        sim.subscribe("l1", StartPosition::First);
        let from = sim.now_ms;
        while sim.resets == 0 {
            assert!(sim.now_ms - from < 20_000, "l1 was never reset");
            sim.round(true);
        }
        Duration::from_millis(sim.now_ms - from)
    };
    let (settled, new, flapped) = (reset_after(true, false), reset_after(false, false), reset_after(true, true));
    println!("reset {settled:?} after the ask in a settled view, {new:?} in a new one, {flapped:?} after a flap");
    assert!(settled < timeout / 3, "reset {settled:?} after the ask in a settled view");
    assert!(new >= timeout && new < timeout + timeout / 3, "reset {new:?} after the ask in a new view");
    assert!(flapped >= timeout && flapped < timeout + timeout / 3, "reset {flapped:?} after the ask past a flap");
}

#[test]
fn a_new_generation_gets_past_a_relay_that_was_asked_to_continue_the_old_one() {
    let mut sim = Sim::new(1, CoreConfig::default(), 1);
    sim.settle(false);
    sim.publish(5);
    sim.settle(false);
    // Fresh relays are asked by the leaves to continue after 5 of generation 1.
    for relay in RELAYS {
        sim.boot(relay, 2);
    }
    sim.settle(false);
    sim.nodes.get_mut("o").unwrap().core.restart_stream(&key(), 2).unwrap();
    sim.published = 0;
    sim.seen.clear();
    sim.settle(true);
    sim.publish(3);
    sim.settle(true);
    for leaf in LEAVES {
        assert_eq!(sim.seen[&(leaf, 0)], [1, 2, 3], "{leaf}");
        let core = &sim.nodes[leaf].core;
        let kinds: Vec<_> = core.events(sim.now(), 1_000_000).iter().map(|e| e.kind).collect();
        assert!(
            kinds.contains(&"parent_chosen") && kinds.contains(&"parent_lost") && kinds.contains(&"reset"),
            "{kinds:?}"
        );
        assert_eq!(core.streams(10, sim.now()).0[0].parent.as_deref(), Some(preferences(leaf)[0]));
        // A cluster's timeline gets the loss and the reset, and not the choice that followed.
        let timeline = sim.nodes.get_mut(leaf).unwrap().core.take_timeline();
        assert!(timeline.contains_key("parent_lost") && timeline.contains_key("reset"), "{timeline:?}");
        assert!(sim.nodes.get_mut(leaf).unwrap().core.take_timeline().is_empty(), "each is taken once");
    }
}

#[test]
fn a_live_subscription_follows_the_key_through_a_restart_and_a_close() {
    let mut sim = Sim::new(3, CoreConfig::default(), 1);
    sim.settle(false);
    sim.publish(4);
    sim.settle(false);
    let ids = sim.subs.clone();
    // Each new generation is published before anything follows it: the relays take it from its record 1.
    sim.origin().restart_stream(&key(), 2).unwrap();
    (sim.published, sim.seen) = (0, HashMap::new());
    sim.publish(3);
    sim.settle(true);
    for leaf in LEAVES {
        assert_eq!(sim.seen[&(leaf, 0)], [1, 2, 3], "{leaf} after the restart");
    }
    sim.origin().close_stream(&key()).unwrap();
    sim.settle(true);
    for leaf in LEAVES {
        assert!(sim.nodes[leaf].core.is_subscribed(sim.subs[&(leaf, 0)]), "{leaf} is parked, not ended");
    }
    sim.origin().open_stream(&key(), 3, DeliveryPolicy::ReplayRequired).unwrap();
    (sim.published, sim.seen) = (0, HashMap::new());
    sim.publish(2);
    sim.settle(true);
    for leaf in LEAVES {
        assert_eq!(sim.seen[&(leaf, 0)], [1, 2], "{leaf} after the close");
    }
    assert_eq!(sim.subs, ids, "no leaf subscribed again");
    assert_eq!(sim.resets, 2 * LEAVES.len(), "one Ready per discontinuity per leaf");
}

/// Settle covers 40 s, well past `upstream_timeout`. A closed key is neither an unreachable parent nor a
/// generation the relays are ahead of: no orphan, no mismatch churn, and the old followers get the new stream.
#[test]
fn a_key_closed_for_long_and_opened_again_older_is_followed_without_churn() {
    let mut sim = Sim::new(5, CoreConfig::default(), 1);
    sim.settle(false);
    sim.publish(2);
    sim.settle(false);
    let ids = sim.subs.clone();
    sim.origin().restart_stream(&key(), 4).unwrap();
    sim.settle(true);
    sim.origin().close_stream(&key()).unwrap();
    sim.settle(true);
    sim.origin().open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    (sim.published, sim.seen) = (0, HashMap::new());
    sim.settle(true);
    sim.publish(3);
    sim.settle(true);
    for leaf in LEAVES {
        assert_eq!(sim.seen[&(leaf, 0)], [1, 2, 3], "{leaf} after the older open");
    }
    assert_eq!(sim.subs, ids, "no leaf subscribed again");
    for (name, node) in &sim.nodes {
        let stats = node.core.stats();
        let mismatches = stats.refusals[Status::GenerationMismatch as usize];
        let orphaned = node.core.events(Instant::now(), 0).iter().filter(|e| e.kind == "orphaned").count();
        assert_eq!((mismatches, orphaned), (0, 0), "{name} {stats:?}");
    }
}

/// An origin that also relays other keys (a processor pulling trades) is asked again for the key it closed by
/// relays whose followers still want it, which makes it pull the key itself. Opening the key again takes it
/// back: before, every open was refused `AlreadyExists` for as long as a follower was left.
#[test]
fn an_origin_that_relays_opens_its_key_again_while_children_still_ask_for_it() {
    let mut sim = Sim::new(3, CoreConfig::default(), 1);
    let relaying = sim.origin();
    *relaying = core("o");
    relaying.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    sim.settle(false);
    sim.publish(2);
    sim.settle(false);
    let ids = sim.subs.clone();
    sim.origin().close_stream(&key()).unwrap();
    sim.settle(true);
    sim.origin().open_stream(&key(), 2, DeliveryPolicy::ReplayRequired).unwrap();
    (sim.published, sim.seen) = (0, HashMap::new());
    sim.settle(true);
    sim.publish(3);
    sim.settle(true);
    for leaf in LEAVES {
        assert_eq!(sim.seen[&(leaf, 0)], [1, 2, 3], "{leaf} after the open");
    }
    assert_eq!(sim.subs, ids, "no leaf subscribed again");
    assert_eq!(sim.origin().stats().originated, 1);
}

/// The key moves: its origin closes it and another node opens a new generation. A `Live` follower on the node that
/// closed it follows it there, from its record 1. Before, the close parked it and nothing pulled the key: it never
/// saw the new generation, with no reset and no end.
#[test]
fn a_live_follower_on_the_origin_follows_its_key_to_the_node_that_opens_it_next() {
    let mut now = Instant::now();
    let tree: Tree = &[("a", &["b"]), ("b", &["a"])];
    let mut cores = Cores::from([("a", node("a", CoreConfig::default())), ("b", node("b", CoreConfig::default()))]);
    let a = &mut cores.get_mut("a").unwrap().1;
    a.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    let id = a.subscribe(&key(), SubscribeOptions::default()).unwrap();
    a.publish(&key(), vec![Bytes::new(); 3]).unwrap();
    drive(&mut cores, tree, &mut now, 4);
    let follower = |cores: &mut Cores<AllowAll>| drain(&mut cores.get_mut("a").unwrap().1, id);
    assert_eq!(follower(&mut cores).last().map(String::as_str), Some("1 1..3"));

    cores.get_mut("a").unwrap().1.close_stream(&key()).unwrap();
    let b = &mut cores.get_mut("b").unwrap().1;
    b.open_stream(&key(), 2, DeliveryPolicy::ReplayRequired).unwrap();
    b.publish(&key(), vec![Bytes::new(); 2]).unwrap();
    drive(&mut cores, tree, &mut now, 10);
    // As a relay's followers see a close: no position while the key is closed, then the new generation's.
    let waiting = format!("{:?}", StreamEvent::Ready { start: None });
    assert_eq!(follower(&mut cores), [waiting, ready(2, 0), "2 1..2".into()]);
    assert_eq!(cores["a"].1.parent_of(&key()).map(|p| p.node.to_string()).as_deref(), Some("b"));
    assert!(cores["a"].1.is_subscribed(id), "the same subscription");
}

/// What a close pulls rests on one fact: after it, only a `Live` follower on the node holds the key's demand, as the
/// close ends every branch and every other subscription. A node with none asks its parents for nothing; with one, it
/// asks them, and its own next open still takes the key back.
#[test]
fn a_close_pulls_the_key_only_for_a_live_follower_on_its_node() {
    let mut now = Instant::now();
    let tree: Tree = &[("l", &["o"])];
    let mut cores = Cores::from([("o", node("o", CoreConfig::default())), ("l", node("l", CoreConfig::default()))]);
    let first = SubscribeOptions { start: StartPosition::First, ..Default::default() };
    let o = &mut cores.get_mut("o").unwrap().1;
    o.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    let replay = o.subscribe(&key(), first).unwrap();
    let child = cores.get_mut("l").unwrap().1.subscribe(&key(), first).unwrap();
    publish(&mut cores, 2);
    drive(&mut cores, tree, &mut now, 4);
    assert_eq!(drain(&mut cores.get_mut("l").unwrap().1, child).last().map(String::as_str), Some("1 1..2"));
    let parent = PeerId::new("p", 1);
    let asks = |cores: &mut Cores<AllowAll>, now| {
        let interests = cores.get_mut("o").unwrap().1.reconcile(std::slice::from_ref(&parent), now);
        interests.iter().any(|(_, i)| i.wants.iter().any(|w| w.key == key()))
    };

    cores.get_mut("o").unwrap().1.close_stream(&key()).unwrap();
    drive(&mut cores, tree, &mut now, 4);
    assert!(!asks(&mut cores, now), "a close with no follower on its node pulled the key");
    let closed = format!("{:?}", StreamEvent::End(EndReason::StreamClosed));
    assert_eq!(drain(&mut cores.get_mut("o").unwrap().1, replay).last(), Some(&closed));
    assert_eq!(drain(&mut cores.get_mut("l").unwrap().1, child).last(), Some(&closed));
    assert_eq!(cores["o"].1.cursor(&key()), None);

    let o = &mut cores.get_mut("o").unwrap().1;
    o.open_stream(&key(), 2, DeliveryPolicy::ReplayRequired).unwrap();
    let live = o.subscribe(&key(), SubscribeOptions::default()).unwrap();
    o.close_stream(&key()).unwrap();
    assert!(asks(&mut cores, now), "the follower's key was not asked for");
    let o = &mut cores.get_mut("o").unwrap().1;
    o.open_stream(&key(), 3, DeliveryPolicy::ReplayRequired).unwrap();
    o.publish(&key(), vec![Bytes::new(); 2]).unwrap();
    let events = drain(o, live);
    assert_eq!(events[events.len() - 2..], [ready(3, 0), "3 1..2".into()], "{events:?}");
}

/// A node that pulls a key and is asked to open it with limits no stream can take refuses before it lets the key go:
/// its follower goes on from the origin. Before, the open closed the pulled stream and stopped pulling, then was
/// refused, and the follower got nothing more.
#[test]
fn an_open_refused_for_its_limits_leaves_a_pulled_key_pulled() {
    let mut now = Instant::now();
    let tree: Tree = &[("r", &["o"])];
    let mut cores =
        Cores::from([("o", origin(AllowAll, CoreConfig::default())), ("r", node("r", CoreConfig::default()))]);
    let id = cores.get_mut("r").unwrap().1.subscribe(&key(), SubscribeOptions::default()).unwrap();
    drive(&mut cores, tree, &mut now, 4);
    publish(&mut cores, 2);
    drive(&mut cores, tree, &mut now, 4);
    let follower = |cores: &mut Cores<AllowAll>| drain(&mut cores.get_mut("r").unwrap().1, id);
    assert_eq!(follower(&mut cores).last().map(String::as_str), Some("1 1..2"));

    let empty = DeliveryPolicy::Compacted(CompactLimits { max_keys: 0, max_bytes: 1024 });
    let refused = cores.get_mut("r").unwrap().1.open_stream(&key(), 2, empty);
    assert_eq!(refused, Err(StreamError::InvalidOptions));
    publish(&mut cores, 2);
    drive(&mut cores, tree, &mut now, 4);
    assert_eq!(follower(&mut cores), ["1 3..4"]);
}

struct Only(&'static str);

impl Authorizer for Only {
    fn authorize(&self, peer: &PeerId, _: &StreamKey, _: Access) -> bool {
        peer.node.ends_with(self.0)
    }
}

#[test]
fn a_backlog_goes_out_in_pushes_that_fit_a_frame_and_a_child_that_leaves_holds_nothing() {
    let cfg = CoreConfig { relay: false, ..Default::default() };
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, cfg.clone()).unwrap();
    origin.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    for _ in 0..12 {
        origin.publish(&key(), vec![Bytes::from(vec![7u8; 256]); 1024]).unwrap();
    }
    let (child, now) = (PeerId::new("c", 1), Instant::now());
    let want = Want {
        key: key(),
        epoch: 1,
        start: StartPosition::After(Cursor { generation: 1, sequence: 0 }),
        snapshot: false,
    };
    assert_eq!(
        origin.handle_interest(&child, Interest { revision: 1, wants: vec![want], ..Default::default() }, now),
        Ok(vec![Answer { status: Status::Ok, path: vec!["o".into()] }])
    );
    let (mut records, mut largest) = (0, 0);
    while let Some((id, body)) = origin.next_push("c", Lane::Live, now, None) {
        let encoded = body.len();
        assert!(encoded <= cfg.max_push_bytes, "a push of {encoded} bytes");
        let (_, _, push) = decode(&body);
        largest = largest.max(encoded);
        for (_, _, item) in &push.items {
            if let Item::Batch(batch) = item {
                assert_eq!(batch.first(), records + 1);
                records += batch.len() as u64;
            }
        }
        origin.push_answered(
            &child,
            Lane::Live,
            id,
            PushAnswer::Applied { applied: id.seq, unwanted: Vec::new() },
            now,
        );
    }
    assert_eq!(records, 12 * 1024);
    assert!(largest > cfg.max_push_bytes / 2, "pushes stopped filling up: {largest}");
    let fed = &origin.stats().downstream[0];
    assert_eq!((fed.node.as_str(), fed.pushed_records, fed.pushed_bytes), ("c", records, records * 256));
    let (streams, total) = origin.streams(10, now);
    assert_eq!(
        (total, streams[0].origin, streams[0].sequence, streams[0].downstream_branches),
        (1, true, Some(records), 1)
    );

    let leave = Interest { revision: 2, base: 1, drops: vec![key()], ..Default::default() };
    assert_eq!(origin.handle_interest(&child, leave, now), Ok(vec![]));
    let stats = origin.stats();
    assert_eq!((stats.hub.sessions, stats.downstream.len()), (1, 0));
}

#[test]
fn a_peer_can_neither_make_a_relay_pull_what_it_may_not_read_nor_take_the_local_session() {
    let mut relay = RelayCore::new(&PeerId::new("r", 1), Only("r"), CoreConfig::default()).unwrap();
    let interest = || Interest {
        revision: 1,
        wants: vec![Want { key: key(), epoch: 1, start: StartPosition::Live, snapshot: false }],
        ..Default::default()
    };
    let now = Instant::now();
    assert_eq!(relay.handle_interest(&PeerId::new("c", 1), interest(), now), Ok(vec![Status::Forbidden.into()]));
    assert_eq!(relay.stats().feeds, 0);
    assert_eq!(relay.handle_interest(&PeerId::new("local:r", 9), interest(), now), Err(StreamError::Forbidden));
    assert!(relay.subscribe(&key(), SubscribeOptions::default()).is_ok());
}

/// Everything, except replay for any session but the node's own.
struct NoReplay;

impl Authorizer for NoReplay {
    fn authorize(&self, peer: &PeerId, _: &StreamKey, access: Access) -> bool {
        access != Access::Replay || peer.node.starts_with("local:")
    }
}

#[test]
fn a_child_that_may_not_replay_cannot_make_a_relay_pull_history() {
    let mut relay = RelayCore::new(&PeerId::new("r", 1), NoReplay, CoreConfig::default()).unwrap();
    let first = Want { key: key(), epoch: 1, start: StartPosition::First, snapshot: false };
    let interest = Interest { revision: 1, wants: vec![first], ..Default::default() };
    let now = Instant::now();
    let answers = relay.handle_interest(&PeerId::new("c", 1), interest, now).unwrap();
    assert_eq!(answers[0].status, Status::Ok);
    let asked = relay.reconcile(&[PeerId::new("o", 1)], now);
    assert_eq!(asked[0].1.wants[0].start, StartPosition::Live);
}

/// A relay with no position asks its parents from the earliest start its children still want. Before, it kept the
/// earliest any child had asked after that child moved on, the origin refused it, and the relay reset the branch of a
/// child whose own start the origin holds.
#[test]
fn a_relay_asks_from_the_starts_its_children_still_want() {
    let mut relay = core("r");
    let (child, now) = (PeerId::new("c", 1), Instant::now());
    let want = |epoch, start| Want { key: key(), epoch, start, snapshot: false };
    let first = Interest { revision: 1, wants: vec![want(1, StartPosition::First)], ..Default::default() };
    relay.handle_interest(&child, first, now).unwrap();
    // The child took the key up to 15 elsewhere, and asks again from there.
    let after = StartPosition::After(Cursor { generation: 1, sequence: 15 });
    let moved = Interest { base: 1, revision: 2, wants: vec![want(2, after)], ..Default::default() };
    relay.handle_interest(&child, moved, now).unwrap();
    let o = PeerId::new("o", 1);
    let [(_, asked)] = &relay.reconcile(std::slice::from_ref(&o), now)[..] else { panic!("one request") };
    assert_eq!(asked.wants[0].start, after);
    relay.interest_result(&o, asked.revision, Ok::<_, StreamError>(vec![Status::Ok.into()]), now);
    // Once no child waits, nothing holds a local follower back from the live edge.
    relay.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let gone = Interest { base: 2, revision: 3, drops: vec![key()], ..Default::default() };
    relay.handle_interest(&child, gone, now).unwrap();
    let [(_, asked)] = &relay.reconcile(std::slice::from_ref(&o), now)[..] else { panic!("one request") };
    assert_eq!(asked.wants[0].start, StartPosition::Live);
}

/// Named cores, each with its boot.
type Cores<A> = BTreeMap<&'static str, (u128, RelayCore<A>)>;
/// Each child and its parents.
type Tree = &'static [(&'static str, &'static [&'static str])];

/// Rounds of 500 ms: every child asks its parents, then each of them pushes what it has.
fn drive<A: Authorizer>(cores: &mut Cores<A>, tree: Tree, now: &mut Instant, rounds: u32) {
    for _ in 0..rounds {
        *now += Duration::from_millis(500);
        let peer = |cores: &Cores<A>, name: &str| PeerId::new(name, cores[name].0);
        for (child, parents) in tree {
            let ids: Vec<PeerId> = parents.iter().map(|p| peer(cores, p)).collect();
            let me = peer(cores, child);
            for (parent, interest) in cores.get_mut(child).unwrap().1.reconcile(&ids, *now) {
                let revision = interest.revision;
                let reply = cores.get_mut(&*parent.node).unwrap().1.handle_interest(&me, interest, *now);
                cores.get_mut(child).unwrap().1.interest_result(&parent, revision, reply, *now);
            }
            for parent in *parents {
                let from = peer(cores, parent);
                while let Some((id, body)) = cores.get_mut(parent).unwrap().1.next_push(child, Lane::Live, *now, None) {
                    let answer = answer(&mut cores.get_mut(child).unwrap().1, &from, &body, *now);
                    cores.get_mut(parent).unwrap().1.push_answered(&me, Lane::Live, id, answer, *now);
                }
            }
        }
    }
}

/// Every event waiting for a subscription, each batch as its generation and sequences.
fn drain<A: Authorizer>(core: &mut RelayCore<A>, id: u64) -> Vec<String> {
    std::iter::from_fn(|| core.poll(id))
        .map(|event| match event {
            StreamEvent::Batch(b) => format!("{} {}..{}", b.generation(), b.first(), b.first() + b.len() as u64 - 1),
            other => format!("{other:?}"),
        })
        .collect()
}

fn node(name: &str, cfg: CoreConfig) -> (u128, RelayCore) {
    (1, RelayCore::new(&PeerId::new(name, 1), AllowAll, cfg).unwrap())
}

fn core(name: &str) -> RelayCore {
    node(name, CoreConfig::default()).1
}

fn origin<A: Authorizer>(authorizer: A, cfg: CoreConfig) -> (u128, RelayCore<A>) {
    origin_of(authorizer, cfg, DeliveryPolicy::ReplayRequired)
}

fn origin_of<A: Authorizer>(authorizer: A, cfg: CoreConfig, policy: DeliveryPolicy) -> (u128, RelayCore<A>) {
    let mut core = RelayCore::new(&PeerId::new("o", 1), authorizer, CoreConfig { relay: false, ..cfg }).unwrap();
    core.open_stream(&key(), 1, policy).unwrap();
    (1, core)
}

fn publish<A: Authorizer>(cores: &mut Cores<A>, records: usize) {
    cores.get_mut("o").unwrap().1.publish(&key(), vec![Bytes::new(); records]).unwrap();
}

fn ready(generation: u64, sequence: u64) -> String {
    format!("{:?}", StreamEvent::Ready { start: Some(Cursor { generation, sequence }) })
}

/// Record 1 of a new generation is a best effort. A node that may not replay from its parent, here a relay that
/// restarted and so tells its child the new generation as a `Ready`, and a parent whose ring has turned over while
/// another parent lacks the key, both give the live edge at once, where asking for record 1 again stalled the key.
#[test]
fn a_new_generation_its_parent_cannot_give_from_record_1_comes_from_the_live_edge() {
    let mut now = Instant::now();
    let tree: Tree = &[("r", &["o"]), ("l", &["r"])];
    let relay = |boot| (boot, RelayCore::new(&PeerId::new("r", boot), NoReplay, CoreConfig::default()).unwrap());
    let mut cores = Cores::from([("o", origin(NoReplay, CoreConfig::default())), ("r", relay(1))]);
    let mut leaf = RelayCore::new(&PeerId::new("l", 1), NoReplay, CoreConfig::default()).unwrap();
    let id = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    cores.insert("l", (1, leaf));
    drive(&mut cores, tree, &mut now, 10);
    publish(&mut cores, 3);
    drive(&mut cores, tree, &mut now, 10);
    let leaf = |cores: &mut Cores<NoReplay>| drain(&mut cores.get_mut("l").unwrap().1, id);
    assert_eq!(leaf(&mut cores).last().map(String::as_str), Some("1 1..3"));
    cores.insert("r", relay(2));
    cores.get_mut("o").unwrap().1.restart_stream(&key(), 2).unwrap();
    publish(&mut cores, 3);
    drive(&mut cores, tree, &mut now, 10);
    publish(&mut cores, 2);
    drive(&mut cores, tree, &mut now, 10);
    let events = leaf(&mut cores);
    assert_eq!(events[events.len() - 2..], [ready(2, 3), "2 4..5".into()], "{events:?}");
    assert_eq!(cores["l"].1.stats().refusals[Status::Forbidden as usize], 1);

    let tree: Tree = &[("l", &["o", "x"])];
    let rings = vec![RingLimits { namespace: "md".into(), max_records: 4, max_bytes: 1 << 20 }];
    let mut cores = Cores::from([("o", origin(AllowAll, CoreConfig { rings, ..Default::default() }))]);
    let solo = CoreConfig { relay: false, ..Default::default() };
    cores.insert("x", node("x", solo));
    let mut leaf = core("l");
    let id = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    cores.insert("l", (1, leaf));
    drive(&mut cores, tree, &mut now, 10);
    cores.get_mut("o").unwrap().1.restart_stream(&key(), 2).unwrap();
    publish(&mut cores, 10);
    drive(&mut cores, tree, &mut now, 10);
    let before = drain(&mut cores.get_mut("l").unwrap().1, id);
    publish(&mut cores, 1);
    drive(&mut cores, tree, &mut now, 10);
    assert_eq!(before.last(), Some(&ready(2, 10)), "{before:?}");
    assert_eq!(drain(&mut cores.get_mut("l").unwrap().1, id), ["2 11..11"]);
}

/// A relay that falls behind its parent's ring resets its branches as a gap: each child asks again from its own
/// position, which the relay then takes from its parent if that holds it. Before, a child ahead of what the relay lost
/// took the reset as a refusal of its position and, with no other parent, gave it up.
#[test]
fn a_relay_that_falls_behind_has_a_child_ahead_of_it_ask_again_and_go_on() {
    let mut cfg = CoreConfig::default();
    cfg.hub.max_batch_records = 8;
    cfg.store.max_records_per_stream = 8;
    cfg.branch.max_records = 8;
    let mut now = Instant::now();
    let node = |name| (1, RelayCore::new(&PeerId::new(name, 1), AllowAll, cfg.clone()).unwrap());
    let mut cores = Cores::from([("o", origin(AllowAll, cfg.clone())), ("r", node("r")), ("l", node("l"))]);
    let id = cores.get_mut("l").unwrap().1.subscribe(&key(), SubscribeOptions::default()).unwrap();
    cores.get_mut("r").unwrap().1.subscribe(&key(), SubscribeOptions::default()).unwrap();
    publish(&mut cores, 5);
    drive(&mut cores, &[("r", &["o"]), ("l", &["o", "r"])], &mut now, 4);
    assert!(cores["r"].1.cursor(&key()).is_some_and(|c| c.sequence == 5));
    // The relay is cut from the origin while the leaf, fed by the origin, goes on past the origin's ring.
    for _ in 0..4 {
        publish(&mut cores, 5);
        drive(&mut cores, &[("l", &["o", "r"])], &mut now, 2);
    }
    let before = drain(&mut cores.get_mut("l").unwrap().1, id);
    assert!(before.last().is_some_and(|e| e.ends_with("..25")), "{before:?}");
    // Now the leaf reaches only the relay, in its view for long, which finds it fell behind.
    let tree: Tree = &[("r", &["o"]), ("l", &["r"])];
    drive(&mut cores, tree, &mut now, 20);
    publish(&mut cores, 3);
    drive(&mut cores, tree, &mut now, 4);
    assert_eq!(drain(&mut cores.get_mut("l").unwrap().1, id), ["1 26..28"]);
    assert!(cores["r"].1.stats().local_resets > 0, "the relay fell behind");
}

/// Each loss is told once to every `Live` follower, one that subscribes after it included, and never to a
/// subscription from a position, nor while the key was never fed.
#[test]
fn every_loss_of_the_origin_is_told_to_each_live_follower_once() {
    let mut now = Instant::now();
    let mut cores = Cores::from([("o", origin(AllowAll, CoreConfig::default()))]);
    let mut leaf = core("l");
    let early = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let first = SubscribeOptions { start: StartPosition::First, ..Default::default() };
    let replay = leaf.subscribe(&key(), first).unwrap();
    cores.insert("l", (1, leaf));
    let (up, down): (Tree, Tree) = (&[("l", &["o"])], &[("l", &[])]);
    let lost = |cores: &mut Cores<AllowAll>, id| {
        drain(&mut cores.get_mut("l").unwrap().1, id).iter().filter(|e| *e == "OriginLost").count()
    };
    drive(&mut cores, down, &mut now, 10);
    assert_eq!(lost(&mut cores, early), 0, "nothing was lost");
    drive(&mut cores, up, &mut now, 4);
    publish(&mut cores, 3);
    drive(&mut cores, up, &mut now, 4);
    let mut late = None;
    for round in 1..=2 {
        drive(&mut cores, down, &mut now, 10);
        let leaf = &mut cores.get_mut("l").unwrap().1;
        let after = *late.get_or_insert_with(|| leaf.subscribe(&key(), SubscribeOptions::default()).unwrap());
        assert_eq!([early, after, replay].map(|id| lost(&mut cores, id)), [1, 1, 0], "loss {round}");
        drive(&mut cores, up, &mut now, 4);
        publish(&mut cores, 1);
        drive(&mut cores, up, &mut now, 4);
    }
}

#[test]
fn a_parent_that_keeps_refusing_a_stream_is_retried_but_noted_once() {
    let mut leaf = core("c");
    let _sub = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let parent = [PeerId::new("p", 1)];
    let mut now = Instant::now();
    for _ in 0..3 {
        let asked = leaf.reconcile(&parent, now);
        assert_eq!(asked.len(), 1);
        leaf.interest_result(&parent[0], asked[0].1.revision, Some(vec![Status::UnknownStream.into()]), now);
        now += Duration::from_secs(120);
    }
    let refused = leaf.events(now, 0).iter().filter(|e| e.kind == "refused").count();
    assert_eq!((leaf.stats().refusals[Status::UnknownStream as usize], refused), (3, 1));
}

#[test]
fn a_namespace_with_its_own_parents_is_asked_there_and_the_rest_at_the_default() {
    let mut leaf = core("c");
    let _trades = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let candles = StreamKey::new("candles", "btc");
    let _candles = leaf.subscribe(&candles, SubscribeOptions::default()).unwrap();
    let (a, b) = (PeerId::new("a", 1), PeerId::new("b", 1));
    let by_namespace = BTreeMap::from([("candles".to_string(), vec![b.clone()])]);
    let parents = Parents { default: std::slice::from_ref(&a), by_namespace: &by_namespace };
    let mut asked: Vec<(String, Vec<String>)> = leaf
        .reconcile(parents, Instant::now())
        .into_iter()
        .map(|(peer, interest)| {
            (peer.node.to_string(), interest.wants.iter().map(|w| w.key.namespace().to_string()).collect())
        })
        .collect();
    asked.sort();
    assert_eq!(asked, [("a".to_string(), vec!["md".to_string()]), ("b".to_string(), vec!["candles".to_string()])]);
}

/// `child` reconciles against `parent` alone and the one interest it sends is answered.
fn ask(child: &mut RelayCore, parent: &mut RelayCore, ids: (&PeerId, &PeerId), now: Instant) {
    for (to, interest) in child.reconcile(std::slice::from_ref(ids.1), now) {
        let answer = parent.handle_interest(ids.0, interest.clone(), now);
        child.interest_result(&to, interest.revision, answer, now);
    }
}

/// A cursor start that comes while the node's live want is unanswered asks the parent again, from
/// there, instead of being reset by the live position the parent would answer.
#[test]
fn a_first_start_behind_an_unanswered_live_want_is_asked_of_the_parent_again() {
    let (o, c) = (PeerId::new("o", 1), PeerId::new("c", 1));
    let (mut origin, mut leaf) = (core("o"), core("c"));
    origin.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    for n in 1..=4u8 {
        origin.publish(&key(), vec![Bytes::from(vec![n])]).unwrap();
    }
    let now = Instant::now();
    let _live = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    ask(&mut leaf, &mut origin, (&c, &o), now);
    let first = leaf.subscribe(&key(), SubscribeOptions { start: StartPosition::First, ..Default::default() }).unwrap();
    ask(&mut leaf, &mut origin, (&c, &o), now);
    deliver(&mut origin, &o, &mut leaf, &c, now);
    let events: Vec<StreamEvent> = std::iter::from_fn(|| leaf.poll(first)).collect();
    let at = Cursor { generation: 1, sequence: 0 };
    let joined = [StreamEvent::Ready { start: Some(at) }, StreamEvent::CaughtUp { through: at }];
    assert_eq!(events.get(..2), Some(&joined[..]), "{events:?}");
    let records: Vec<u64> = events[2..]
        .iter()
        .flat_map(|event| match event {
            StreamEvent::Batch(batch) => batch.first()..=batch.last(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(records, [1, 2, 3, 4]);
}

/// A request the child timed out reaches its parent late, behind the newer one the child sent in its place: the parent
/// refuses it and keeps what the newer one set, and the child, which has moved on, ignores the late answer.
#[test]
fn a_request_that_arrives_behind_a_newer_one_is_refused_and_its_late_answer_changes_nothing() {
    // No linger, so a child that moved on lets go of its parent at once.
    let cfg = CoreConfig { linger: Duration::ZERO, ..Default::default() };
    let core = |name: &str| RelayCore::new(&PeerId::new(name, 1), AllowAll, cfg.clone()).unwrap();
    let (o, c) = (PeerId::new("o", 1), PeerId::new("c", 1));
    let (mut origin, mut leaf) = (core("o"), core("c"));
    origin.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    let live = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let (mut now, retry) = (Instant::now(), cfg.retry_failed);
    let [(_, timed_out)] = &leaf.reconcile(std::slice::from_ref(&o), now)[..] else { panic!("one request") };
    leaf.interest_result(&o, timed_out.revision, Reply::Failed, now);
    now += retry;
    ask(&mut leaf, &mut origin, (&c, &o), now);
    assert_eq!(origin.stats().downstream[0].branches, 1);

    let late = origin.handle_interest(&c, timed_out.clone(), now);
    assert_eq!(late, Err(StreamError::StaleRevision));
    leaf.unsubscribe(live);
    let [(_, leave)] = &leaf.reconcile(std::slice::from_ref(&o), now)[..] else { panic!("one request") };
    leaf.interest_result(&o, timed_out.revision, late, now);
    let answer = origin.handle_interest(&c, leave.clone(), now);
    leaf.interest_result(&o, leave.revision, answer, now);
    assert!(origin.stats().downstream.is_empty(), "the parent still feeds the child");
    now += retry * 2;
    assert!(leaf.reconcile(std::slice::from_ref(&o), now).is_empty(), "the child asks again");
}

/// A parent that stalled sends what it held in full pushes once it can. The node hands a push to its subscribers at
/// once, so a subscriber with the default limits must hold a whole one, of single records or of larger batches, or
/// every catch-up resets it.
#[test]
fn a_default_subscriber_takes_a_full_push() {
    let (o, c) = (PeerId::new("o", 1), PeerId::new("c", 1));
    let max = CoreConfig::default().max_push_items as u64;
    for per_batch in [1, 5] {
        let (mut origin, mut leaf) = (core("o"), core("c"));
        origin.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
        let live = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
        let now = Instant::now();
        ask(&mut leaf, &mut origin, (&c, &o), now);
        for n in 0..max {
            let batch = (0..per_batch).map(|i| Bytes::copy_from_slice(&(n * per_batch + i).to_be_bytes())).collect();
            origin.publish(&key(), batch).unwrap();
        }
        let (mut largest, mut records) = (0, Vec::new());
        while let Some((id, body)) = origin.next_push("c", Lane::Live, now, None) {
            let (_, _, push) = decode(&body);
            largest = largest.max(push.items.len());
            let answer = answer(&mut leaf, &o, &body, now);
            origin.push_answered(&c, Lane::Live, id, answer, now);
            // The subscriber reads only between pushes.
            while let Some(event) = leaf.poll(live) {
                match event {
                    StreamEvent::Batch(batch) => records.extend(batch.first()..=batch.last()),
                    StreamEvent::Ready { .. } | StreamEvent::CaughtUp { .. } => {}
                    other => panic!("{per_batch} records a batch: {other:?} after {} records", records.len()),
                }
            }
        }
        if per_batch == 1 {
            assert_eq!(largest as u64, max, "the backlog did not go out as one full push");
        }
        assert_eq!(records, (1..=max * per_batch).collect::<Vec<_>>());
    }
}

#[test]
fn a_parent_whose_path_names_the_child_is_refused_as_a_loop() {
    let (o, a, b) = (PeerId::new("o", 1), PeerId::new("a", 1), PeerId::new("b", 1));
    let (mut origin, mut relay, mut leaf) = (core("o"), core("a"), core("b"));
    origin.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    let _sub = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let mut now = Instant::now();
    ask(&mut leaf, &mut relay, (&b, &a), now);
    ask(&mut relay, &mut origin, (&a, &o), now);
    now += Duration::from_secs(3);
    deliver(&mut relay, &a, &mut leaf, &b, now);
    assert_eq!((relay.parent_of(&key()), leaf.parent_of(&key())), (Some(&o), Some(&a)));
    let parents = |core: &RelayCore| {
        let stats = core.stats();
        (stats.parents.iter().map(|p| (p.node.clone(), p.keys, p.joining)).collect::<Vec<_>>(), stats.orphaned)
    };
    assert_eq!(parents(&relay), (vec![("o".to_string(), 1, 0)], 0));

    // The origin goes; the only parent left to the relay is its own child.
    ask(&mut relay, &mut leaf, (&a, &b), now);
    assert_eq!(relay.parent_of(&key()), None);
    assert_eq!(parents(&relay), (vec![], 1));
    assert_eq!(relay.stats().refusals[Status::Loop as usize], 1);
    assert!(relay.events(now, 0).iter().any(|e| e.kind == "refused" && e.detail == "b answered loop"));
    let seen = &leaf.streams(1, now).0[0];
    assert_eq!(
        (seen.path.as_slice(), relay.streams(1, now).0[0].refused.as_slice()),
        (&["o".to_string(), "a".to_string()][..], &["b answered loop".to_string()][..])
    );
    // With no parent at all for `upstream_timeout`, the relay's timeline says so.
    let timeout = CoreConfig::default().timings.upstream_timeout;
    for later in [now + timeout, now + timeout * 2] {
        let _ = relay.reconcile(&[] as &[PeerId], later);
    }
    assert!(relay.take_timeline().contains_key("orphaned"));
    // A parent back after that is on the timeline too, so the orphaned stream is seen to recover.
    let _ = relay.reconcile(std::slice::from_ref(&o), now + timeout * 3);
    let restored = relay.take_timeline().remove("parent_restored");
    assert_eq!(restored, Some((key().to_string(), "pulling from o".to_owned(), 1)));

    // Two relays that only know each other: neither has a path yet, so the second answer closes the loop.
    let (x, y) = (PeerId::new("x", 1), PeerId::new("y", 1));
    let (mut left, mut right) = (core("x"), core("y"));
    let _sub = left.subscribe(&key(), SubscribeOptions::default()).unwrap();
    ask(&mut left, &mut right, (&x, &y), now);
    ask(&mut right, &mut left, (&y, &x), now);
    assert_eq!((left.parent_of(&key()), right.parent_of(&key())), (Some(&y), None));
    assert_eq!(right.stats().refusals[Status::Loop as usize], 1);
}

#[test]
fn a_relay_that_changes_parents_tells_its_children_the_new_path_at_once() {
    let ids = ["o", "a", "x", "b", "c"].map(|n| PeerId::new(n, 1));
    let [o, a, x, b, c] = &ids;
    let (mut origin, mut first, mut second, mut relay, mut leaf) =
        (core("o"), core("a"), core("x"), core("b"), core("c"));
    origin.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    let _sub = leaf.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let now = Instant::now();
    ask(&mut leaf, &mut relay, (c, b), now);
    ask(&mut relay, &mut first, (b, a), now);
    ask(&mut first, &mut origin, (a, o), now);
    let path = |core: &RelayCore| core.streams(1, now).0[0].path.join(">");
    assert_eq!(path(&leaf), "b", "asked before anyone upstream knew its path");

    // Each answer on the way down is pushed on: nobody asks again to learn its path.
    let tell = |parent: &mut RelayCore, child: &mut RelayCore, from: &PeerId, to: &PeerId| {
        let (id, body) = parent.next_push(&to.node, Lane::Live, now, None).expect("a path to pass on");
        let (_, _, push) = decode(&body);
        assert!(push.items.iter().any(|(_, _, item)| matches!(item, Item::Path(_))), "{push:?}");
        let answer = answer(child, from, &body, now);
        assert_eq!(answer, PushAnswer::Applied { applied: id.seq, unwanted: Vec::new() });
        parent.push_answered(to, Lane::Live, id, answer, now);
    };
    tell(&mut first, &mut relay, a, b);
    assert_eq!(path(&relay), "o>a");
    tell(&mut relay, &mut leaf, b, c);
    assert_eq!(path(&leaf), "o>a>b");

    // `a` goes and `b` moves under `x`: the leaf must not keep naming `a`, or `a` could never pull from it.
    ask(&mut relay, &mut second, (b, x), now);
    ask(&mut second, &mut origin, (x, o), now);
    assert_eq!(relay.parent_of(&key()), Some(x));
    tell(&mut second, &mut relay, x, b);
    deliver(&mut relay, b, &mut leaf, c, now);
    assert_eq!(path(&leaf), "o>x>b");
    ask(&mut first, &mut leaf, (a, c), now);
    assert_eq!((first.parent_of(&key()), first.stats().refusals[Status::Loop as usize]), (Some(c), 0));
}

/// One publish per key, each a value of `value` bytes.
fn set_keys<A: Authorizer>(cores: &mut Cores<A>, keys: std::ops::Range<u32>, value: usize) {
    for k in keys {
        cores.get_mut("o").unwrap().1.publish(&key(), vec![keyed(&k.to_be_bytes(), &vec![1; value])]).unwrap();
    }
}

/// The sequences of every batch waiting for a subscription, and its other events.
fn split<A: Authorizer>(core: &mut RelayCore<A>, id: u64) -> (Vec<u64>, Vec<StreamEvent>) {
    let (mut sequences, mut other) = (Vec::new(), Vec::new());
    while let Some(event) = core.poll(id) {
        match event {
            StreamEvent::Batch(b) => sequences.extend(b.first()..=b.last()),
            event => other.push(event),
        }
    }
    (sequences, other)
}

fn start<A: Authorizer>(cores: &mut Cores<A>, node: &str, start: StartPosition) -> u64 {
    cores.get_mut(node).unwrap().1.subscribe(&key(), SubscribeOptions { start, ..Default::default() }).unwrap()
}

/// Everything, except the state for any session but the node's own.
struct NoSnapshot;

impl Authorizer for NoSnapshot {
    fn authorize(&self, peer: &PeerId, _: &StreamKey, access: Access) -> bool {
        access != Access::Snapshot || peer.node.starts_with("local:")
    }
}

/// A node its parent denies the state ends what waits for it there with `End(Revoked)`, once, and every follower on
/// the way keeps every record.
#[test]
fn a_node_denied_the_state_ends_its_waits_and_its_followers_keep_every_record() {
    let mut now = Instant::now();
    let tree: Tree = &[("r", &["o"]), ("l", &["r"])];
    let node = |name| (1, RelayCore::new(&PeerId::new(name, 1), NoSnapshot, CoreConfig::default()).unwrap());
    let limits = CompactLimits { max_keys: 16, max_bytes: 1 << 10 };
    let mut cores = Cores::from([
        ("o", origin_of(NoSnapshot, CoreConfig::default(), DeliveryPolicy::Compacted(limits))),
        ("r", node("r")),
        ("l", node("l")),
    ]);
    let followers = [start(&mut cores, "r", StartPosition::Live), start(&mut cores, "l", StartPosition::Live)];
    drive(&mut cores, tree, &mut now, 4);
    set_keys(&mut cores, 0..3, 8);
    drive(&mut cores, tree, &mut now, 4);
    let waits = [
        start(&mut cores, "r", StartPosition::SnapshotThenLive),
        start(&mut cores, "l", StartPosition::SnapshotThenLive),
    ];
    drive(&mut cores, tree, &mut now, 4);
    set_keys(&mut cores, 3..6, 8);
    drive(&mut cores, tree, &mut now, 20);
    for (node, (follower, wait)) in ["r", "l"].into_iter().zip(followers.into_iter().zip(waits)) {
        let core = &mut cores.get_mut(node).unwrap().1;
        assert_eq!(split(core, follower).0, (1..=6).collect::<Vec<_>>(), "{node}");
        assert_eq!(split(core, wait), (vec![], vec![StreamEvent::End(EndReason::Revoked)]), "{node}");
        assert_eq!(core.stats().refusals[Status::Forbidden as usize], 1, "{node} asked once");
    }
}

/// A relay whose pushes are smaller than its parent's cannot pass the state on: the child's waiting subscriber ends
/// `End(SlowConsumer)`, and its follower keeps every record.
#[test]
fn a_state_too_large_for_a_relays_pushes_ends_the_wait_below_it_and_nothing_else() {
    let mut now = Instant::now();
    let tree: Tree = &[("r", &["o"]), ("l", &["r"])];
    // Smaller pushes and batches than the origin's, which is valid on their own.
    let small = CoreConfig {
        max_push_bytes: 256 << 10,
        hub: HubLimits { max_batch_bytes: 128 << 10, ..Default::default() },
        ..Default::default()
    };
    let limits = CompactLimits { max_keys: 100, max_bytes: 600_000 };
    let mut cores = Cores::from([
        ("o", origin_of(AllowAll, CoreConfig::default(), DeliveryPolicy::Compacted(limits))),
        ("r", node("r", small)),
        ("l", node("l", CoreConfig::default())),
    ]);
    let follower = start(&mut cores, "l", StartPosition::Live);
    drive(&mut cores, tree, &mut now, 4);
    set_keys(&mut cores, 0..100, 5000);
    drive(&mut cores, tree, &mut now, 4);
    let wait = start(&mut cores, "l", StartPosition::SnapshotThenLive);
    drive(&mut cores, tree, &mut now, 4);
    set_keys(&mut cores, 0..3, 8);
    drive(&mut cores, tree, &mut now, 20);
    let leaf = &mut cores.get_mut("l").unwrap().1;
    assert_eq!(split(leaf, follower).0, (1..=103).collect::<Vec<_>>());
    assert_eq!(split(leaf, wait), (vec![], vec![StreamEvent::End(EndReason::SlowConsumer)]));
    assert_eq!(cores["r"].1.stats().downstream[0].queued_bytes, 0);
}

/// A relay asks for the state while its parent is ahead of it. The branch replays first and takes the state once it has
/// room, so a branch with room for little more than the state still carries both.
#[test]
fn a_branch_replays_before_it_takes_the_state() {
    let mut now = Instant::now();
    let tree: Tree = &[("r", &["o"])];
    // A branch just larger than the state: 64 KiB, against ten keys of about 6000 bytes.
    let cfg = CoreConfig {
        branch: BranchLimits { max_bytes: 64 << 10, ..CoreConfig::default().branch },
        hub: HubLimits { max_batch_bytes: 16 << 10, ..Default::default() },
        ..Default::default()
    };
    let limits = CompactLimits { max_keys: 10, max_bytes: 60_000 };
    let mut cores = Cores::from([
        ("o", origin_of(AllowAll, cfg.clone(), DeliveryPolicy::Compacted(limits))),
        ("r", node("r", cfg)),
    ]);
    let follower = start(&mut cores, "r", StartPosition::Live);
    drive(&mut cores, tree, &mut now, 4);
    set_keys(&mut cores, 0..10, 5984);
    drive(&mut cores, tree, &mut now, 4);
    assert_eq!(split(&mut cores.get_mut("r").unwrap().1, follower).0, (1..=10).collect::<Vec<_>>());
    let wait = start(&mut cores, "r", StartPosition::SnapshotThenLive);
    set_keys(&mut cores, 0..2, 5984);
    drive(&mut cores, tree, &mut now, 10);
    set_keys(&mut cores, 2..3, 5984);
    drive(&mut cores, tree, &mut now, 10);
    let relay = &mut cores.get_mut("r").unwrap().1;
    assert_eq!(split(relay, follower).0, [11, 12, 13]);
    let (after, events) = split(relay, wait);
    let [StreamEvent::Ready { .. }, StreamEvent::Snapshot(state), StreamEvent::CaughtUp { .. }] = &events[..] else {
        panic!("{events:?}")
    };
    assert_eq!(state.records().len(), 10);
    assert_eq!(after, (state.through().sequence + 1..=13).collect::<Vec<_>>());
    assert_eq!(cores["o"].1.stats().downstream[0].queued_bytes, 0);
}

/// A snapshot the node cannot take, here one ahead of its position, is asked for again on a fresh branch.
#[test]
fn a_snapshot_ahead_of_the_node_is_asked_for_again() {
    let mut leaf = core("l");
    let id = leaf.subscribe(&key(), SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() });
    let (p, now) = (PeerId::new("p", 1), Instant::now());
    let [(_, interest)] = &leaf.reconcile(std::slice::from_ref(&p), now)[..] else { panic!("one request") };
    let want = interest.wants[0].clone();
    assert!(want.snapshot);
    let answer = Answer { status: Status::Ok, path: vec!["p".into()] };
    leaf.interest_result(&p, interest.revision, Ok(vec![answer]), now);
    let at = Cursor { generation: 1, sequence: 2 };
    let batch = SequencedBatch::new(1, 1, vec![keyed(b"k", b"1"), keyed(b"k", b"2")]).unwrap();
    let ahead = Snapshot::new(Cursor { sequence: 3, ..at }, vec![keyed(b"k", b"3")]);
    let items = [Item::Ready(Cursor { sequence: 0, ..at }), Item::Batch(batch), Item::Snapshot(ahead)];
    let push = Push { items: items.into_iter().map(|item| (key(), want.epoch, item)).collect() };
    leaf.handle_push(&p, Lane::Live, 1, 1, 1, push, now);
    assert_eq!(leaf.poll(id.unwrap()), None, "nothing before a snapshot it can take");
    let [(_, again)] = &leaf.reconcile(std::slice::from_ref(&p), now)[..] else { panic!("not asked again") };
    assert!(again.wants.iter().any(|w| w.snapshot && w.epoch != want.epoch), "{again:?}");
}

/// An origin whose global queue cannot take the state now resets the branch that asked, which asks again, instead of
/// holding the state where nothing would try it again: once a slow local reader drains, the relay's waiting subscriber
/// gets the state though nothing more is published.
#[test]
fn a_state_the_origin_has_no_room_for_yet_is_sent_once_room_frees_on_an_idle_stream() {
    let mut now = Instant::now();
    let tree: Tree = &[("r", &["o"])];
    // Room for the slow reader's four records and one more in flight, not for the state beside them.
    let hub = HubLimits { max_queued_bytes: 6000, ..CoreConfig::default().hub };
    let cfg = CoreConfig { hub, ..Default::default() };
    let limits = CompactLimits { max_keys: 4, max_bytes: 4000 };
    let mut cores = Cores::from([
        ("o", origin_of(AllowAll, cfg, DeliveryPolicy::Compacted(limits))),
        ("r", node("r", CoreConfig::default())),
    ]);
    let slow = start(&mut cores, "o", StartPosition::Live);
    let follower = start(&mut cores, "r", StartPosition::Live);
    drive(&mut cores, tree, &mut now, 4);
    for k in 0..4 {
        set_keys(&mut cores, k..k + 1, 900);
        drive(&mut cores, tree, &mut now, 1);
    }
    let wait = start(&mut cores, "r", StartPosition::SnapshotThenLive);
    drive(&mut cores, tree, &mut now, 6);
    assert_eq!(split(&mut cores.get_mut("r").unwrap().1, wait), (vec![], vec![]), "no room for the state yet");
    assert_eq!(split(&mut cores.get_mut("o").unwrap().1, slow).0, [1, 2, 3, 4]);
    drive(&mut cores, tree, &mut now, 6);
    let relay = &mut cores.get_mut("r").unwrap().1;
    assert_eq!(split(relay, follower).0, [1, 2, 3, 4]);
    let (after, events) = split(relay, wait);
    let [StreamEvent::Ready { .. }, StreamEvent::Snapshot(state), StreamEvent::CaughtUp { .. }] = &events[..] else {
        panic!("{events:?}")
    };
    assert_eq!((after, state.records().len(), state.through().sequence), (vec![], 4, 4));
    assert_eq!(cores["o"].1.stats().hub.queued_bytes, 0);
}

fn short_ring() -> Vec<RingLimits> {
    vec![RingLimits { namespace: "md".into(), max_records: 4, max_bytes: 1 << 20 }]
}

/// The batches waiting for a subscription.
fn batches(core: &mut RelayCore, id: u64) -> Vec<String> {
    drain(core, id).into_iter().filter(|e| e.starts_with(|c: char| c.is_ascii_digit())).collect()
}

/// A relay cut from the origin when it opened generation 2 is behind a node that followed it there. Asked to continue
/// the node, it is refused as any parent on an older generation is, and the node keeps its position: before, the
/// relay's GenerationMismatch reset the node as if the relay were ahead.
#[test]
fn a_parent_still_on_the_previous_generation_is_refused_and_the_position_kept() {
    let mut now = Instant::now();
    let mut cores = Cores::from([
        ("o", origin(AllowAll, CoreConfig::default())),
        ("r", node("r", CoreConfig::default())),
        ("l", node("l", CoreConfig::default())),
    ]);
    let id = start(&mut cores, "l", StartPosition::Live);
    let all: Tree = &[("r", &["o"]), ("l", &["r", "o"])];
    drive(&mut cores, all, &mut now, 4);
    publish(&mut cores, 2);
    drive(&mut cores, all, &mut now, 4);
    cores.get_mut("o").unwrap().1.restart_stream(&key(), 2).unwrap();
    publish(&mut cores, 3);
    drive(&mut cores, &[("r", &[]), ("l", &["o"])], &mut now, 2);
    drive(&mut cores, &[("r", &[]), ("l", &["r"])], &mut now, 2);
    drive(&mut cores, all, &mut now, 30);
    publish(&mut cores, 2);
    drive(&mut cores, all, &mut now, 4);
    assert_eq!(batches(&mut cores.get_mut("l").unwrap().1, id).join(", "), "1 1..2, 2 1..3, 2 4..5");
    assert_eq!(cores["l"].1.stats().local_resets, 1);
}

/// A parent that refused an older position may hold the one the node has moved on to: with no other parent left, it
/// is asked again before the position is given up, though its backoff has not run out.
#[test]
fn a_parent_that_refused_an_older_position_is_asked_again_before_the_position_is_given_up() {
    let mut now = Instant::now();
    // A backoff that outlasts upstream_timeout, as a parent's doublings make it.
    let leaf = CoreConfig { retry_refused: Duration::from_secs(10), ..Default::default() };
    let mut cores = Cores::from([
        ("o", origin(AllowAll, CoreConfig::default())),
        ("r", node("r", CoreConfig { rings: short_ring(), ..Default::default() })),
        ("l", node("l", leaf)),
    ]);
    let id = start(&mut cores, "l", StartPosition::Live);
    let all: Tree = &[("r", &["o"]), ("l", &["r", "o"])];
    drive(&mut cores, all, &mut now, 4);
    publish(&mut cores, 3);
    drive(&mut cores, all, &mut now, 4);
    // Ten more go out while l is cut off: r's ring holds 10..13 only, the origin's all of them.
    let alone: Tree = &[("r", &["o"]), ("l", &[])];
    drive(&mut cores, alone, &mut now, 1);
    publish(&mut cores, 10);
    drive(&mut cores, alone, &mut now, 2);
    // Back, l is refused 3 by r and catches up from the origin, then loses the origin.
    drive(&mut cores, all, &mut now, 8);
    let relayed: Tree = &[("r", &["o"]), ("l", &["r"])];
    drive(&mut cores, relayed, &mut now, 4);
    publish(&mut cores, 2);
    drive(&mut cores, relayed, &mut now, 4);
    let batches = batches(&mut cores.get_mut("l").unwrap().1, id);
    assert_eq!(batches.last().map(String::as_str), Some("1 14..15"), "{batches:?}");
    assert_eq!(cores["l"].1.stats().local_resets, 0);
}

/// A node that gave up a position no ring held, and takes the same generation up again further on, resets its `Live`
/// follower instead of moving it past the records it lost: before, the follower saw a new `Ready` and read on.
#[test]
fn a_node_that_gave_up_a_position_resets_its_follower_rather_than_skip_records() {
    let mut now = Instant::now();
    let mut cores = Cores::from([
        ("o", origin(AllowAll, CoreConfig { rings: short_ring(), ..Default::default() })),
        ("l", node("l", CoreConfig::default())),
    ]);
    let id = start(&mut cores, "l", StartPosition::Live);
    let up: Tree = &[("l", &["o"])];
    drive(&mut cores, up, &mut now, 4);
    publish(&mut cores, 3);
    drive(&mut cores, up, &mut now, 4);
    assert_eq!(batches(&mut cores.get_mut("l").unwrap().1, id), ["1 1..3"]);
    drive(&mut cores, &[("l", &[])], &mut now, 1);
    publish(&mut cores, 10);
    for round in 0.. {
        assert!(round < 20, "l never gave its position up");
        if cores["l"].1.stats().local_resets > 0 {
            break;
        }
        drive(&mut cores, up, &mut now, 1);
    }
    publish(&mut cores, 2);
    drive(&mut cores, up, &mut now, 4);
    let after = start(&mut cores, "l", StartPosition::Live);
    publish(&mut cores, 1);
    drive(&mut cores, up, &mut now, 4);
    let followed = drain(&mut cores.get_mut("l").unwrap().1, id);
    assert_eq!(followed.last().map(String::as_str), Some("ResetRequired(HistoryUnavailable)"), "{followed:?}");
    assert!(!followed.iter().any(|e| e.starts_with("1 ")), "{followed:?}");
    // One that subscribed after the give-up had nothing to lose.
    assert_eq!(batches(&mut cores.get_mut("l").unwrap().1, after), ["1 16..16"]);
}

/// A node that gave up a position no ring held takes the generation the origin opened meanwhile from its record 1, as
/// it does after a restart it follows: before, it took it from the live edge.
#[test]
fn a_node_that_gave_up_a_position_takes_the_next_generation_from_its_record_1() {
    let mut now = Instant::now();
    let mut cores = Cores::from([
        ("o", origin(AllowAll, CoreConfig { rings: short_ring(), ..Default::default() })),
        ("l", node("l", CoreConfig::default())),
    ]);
    let id = start(&mut cores, "l", StartPosition::Live);
    let up: Tree = &[("l", &["o"])];
    drive(&mut cores, up, &mut now, 4);
    publish(&mut cores, 3);
    drive(&mut cores, up, &mut now, 4);
    drive(&mut cores, &[("l", &[])], &mut now, 1);
    publish(&mut cores, 10);
    // Back, l finds record 4 nowhere and gives its position up; the origin opens generation 2 before l asks again.
    for round in 0.. {
        assert!(round < 20, "l never gave its position up");
        if cores["l"].1.stats().local_resets > 0 {
            break;
        }
        drive(&mut cores, up, &mut now, 1);
    }
    cores.get_mut("o").unwrap().1.restart_stream(&key(), 2).unwrap();
    publish(&mut cores, 2);
    drive(&mut cores, up, &mut now, 4);
    publish(&mut cores, 1);
    drive(&mut cores, up, &mut now, 4);
    let batches = batches(&mut cores.get_mut("l").unwrap().1, id);
    let new: Vec<&str> = batches.iter().map(String::as_str).filter(|b| b.starts_with("2 ")).collect();
    assert_eq!(new, ["2 1..2", "2 3..3"], "{batches:?}");
}
