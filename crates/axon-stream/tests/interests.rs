//! A seeded simulation of the interest protocol over many streams: leaves subscribe and leave at
//! random while interests and their answers are lost, the relay restarts and parents forget their
//! children. Changes go out a few keys at a time, and in the end every parent holds exactly the
//! branches its children want, every subscriber saw its records in order, and nothing is left
//! behind once nobody listens.
#![expect(
    clippy::disallowed_methods,
    reason = "single-threaded and seeded: Instant::now is only the base of the virtual clock"
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use axon::Lane;
use axon_stream::*;
use axon_testkit::check_seeds;
use bytes::Bytes;
use push::{answer, decode, deliver};

mod push;

const STREAMS: usize = 40;
const LEAVES: [&str; 2] = ["l1", "l2"];

fn key(k: usize) -> StreamKey {
    StreamKey::new("md", format!("s{k:02}"))
}

/// A child's interest bounds are applied to its parent's keys, so every process must order keys alike: by name, never by
/// the hash each process draws, which one process's runs all share.
#[test]
fn keys_order_by_name_not_by_hash() {
    let mut keys: Vec<StreamKey> = (0..64).map(key).collect();
    keys.sort();
    assert!(keys.is_sorted_by_key(|k| (k.namespace().to_string(), k.name().to_string())));
}

fn preferences(node: &str) -> &'static [&'static str] {
    match node {
        "o" => &[],
        "r" => &["o"],
        _ => &["r", "o"],
    }
}

struct Sim {
    rng: u64,
    base: Instant,
    now_ms: u64,
    cfg: CoreConfig,
    nodes: BTreeMap<&'static str, (RelayCore, u128, bool)>,
    interests: Vec<(PeerId, PeerId, Interest)>,
    pushes: Vec<(PeerId, PeerId, PushId, Bytes)>,
    published: [u64; STREAMS],
    /// Per leaf and stream: the subscription and the last sequence it saw.
    subs: BTreeMap<(&'static str, usize), (u64, Option<u64>)>,
    largest_interest: usize,
    /// Folds every (stream, sequence) a leaf consumed: two runs of one seed must agree on it.
    trace: u64,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let cfg = CoreConfig {
            max_keys_per_interest: 5,
            // Short, so resyncs and hold-downs happen within each seed's run.
            resync: Duration::from_secs(20),
            timings: RelayTimings { hold_down: Duration::from_secs(2), ..Default::default() },
            hub: HubLimits { max_batch_bytes: 512 << 10, max_streams: 64, ..Default::default() },
            store: MemoryLimits { max_streams: 64, ..Default::default() },
            ..Default::default()
        };
        let mut sim = Self {
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            base: Instant::now(),
            now_ms: 0,
            cfg,
            nodes: BTreeMap::new(),
            interests: Vec::new(),
            pushes: Vec::new(),
            published: [0; STREAMS],
            subs: BTreeMap::new(),
            largest_interest: 0,
            trace: 0,
        };
        for name in ["o", "r"].into_iter().chain(LEAVES) {
            sim.boot(name, 1);
        }
        for k in 0..STREAMS {
            sim.core("o").open_stream(&key(k), 1, DeliveryPolicy::ReplayRequired).unwrap();
        }
        sim
    }

    fn boot(&mut self, name: &'static str, incarnation: u128) {
        let cfg = CoreConfig { relay: name != "o", ..self.cfg.clone() };
        let core = RelayCore::new(&PeerId::new(name, incarnation), AllowAll, cfg).unwrap();
        self.nodes.insert(name, (core, incarnation, true));
    }

    fn core(&mut self, name: &str) -> &mut RelayCore {
        &mut self.nodes.get_mut(name).unwrap().0
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
        PeerId::new(name, self.nodes[name].1)
    }

    fn live(&self, peer: &PeerId) -> bool {
        let node = &self.nodes[&*peer.node];
        node.2 && node.1 == peer.incarnation
    }

    fn reconcile(&mut self, name: &'static str) {
        if !self.nodes[name].2 {
            return;
        }
        let candidates: Vec<PeerId> =
            preferences(name).iter().filter(|p| self.nodes[**p].2).map(|p| self.peer(p)).collect();
        let (me, now) = (self.peer(name), self.now());
        let out = self.core(name).reconcile(&candidates, now);
        self.core(name).take_wakes();
        for (parent, interest) in out {
            self.largest_interest = self.largest_interest.max(interest.wants.len() + interest.drops.len());
            self.interests.push((me.clone(), parent, interest));
        }
    }

    fn deliver_interest(&mut self, index: usize, lose: bool) {
        let (child, parent, interest) = self.interests.swap_remove(index);
        if !self.live(&child) {
            return;
        }
        let (now, revision) = (self.now(), interest.revision);
        let mut reply = Reply::Failed;
        if self.live(&parent) {
            let decoded = wire::decode_interest(&mut wire::encode_interest(&interest).slice(1..)).unwrap();
            assert_eq!(decoded, interest);
            reply = self.core(&parent.node.clone()).handle_interest(&child, interest, now).into();
        }
        let reply = if lose { Reply::Failed } else { reply };
        self.core(&child.node.clone()).interest_result(&parent, revision, reply, now);
    }

    fn pump(&mut self, name: &'static str) {
        let from = self.peer(name);
        if !self.live(&from) {
            return;
        }
        for to in self.core(name).downstream_peers() {
            if self.pushes.iter().any(|p| p.0 == from && p.1.node == to.node) {
                continue;
            }
            let now = self.now();
            if let Some((id, push)) = self.core(name).next_push(&to.node, Lane::Live, now, None) {
                self.pushes.push((from.clone(), to, id, push));
            }
        }
    }

    fn deliver_push(&mut self, index: usize) {
        let (from, to, id, push) = self.pushes.swap_remove(index);
        if !self.live(&from) {
            return;
        }
        let now = self.now();
        if !self.live(&to) {
            return self.core(&from.node.clone()).expire_downstream(&to, now);
        }
        let answer = answer(self.core(&to.node.clone()), &from, &push, now);
        self.core(&from.node.clone()).push_answered(&to, Lane::Live, id, answer, now);
    }

    fn consume(&mut self, leaf: &'static str) {
        let keys: Vec<usize> = self.subs.keys().filter(|(l, _)| *l == leaf).map(|(_, k)| *k).collect();
        for k in keys {
            let (id, mut last) = self.subs[&(leaf, k)];
            let mut again = None;
            while let Some(event) = self.core(leaf).poll(id) {
                match event {
                    StreamEvent::Batch(batch) => {
                        for (sequence, record) in (batch.first()..).zip(batch.records()) {
                            assert_eq!(u64::from_be_bytes(record[..].try_into().unwrap()), sequence);
                            assert!(last.is_none_or(|l| sequence == l + 1), "{leaf} s{k}: {sequence} after {last:?}");
                            last = Some(sequence);
                            self.trace = (self.trace.rotate_left(7) ^ (k as u64) << 32 ^ sequence)
                                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
                        }
                    }
                    StreamEvent::ResetRequired(_) => {
                        again = Some(self.core(leaf).subscribe(&key(k), SubscribeOptions::default()).unwrap());
                        last = None;
                    }
                    StreamEvent::End(reason) => panic!("{leaf} s{k} ended: {reason:?}"),
                    _ => {}
                }
            }
            self.subs.insert((leaf, k), (again.unwrap_or(id), last));
        }
    }

    fn step(&mut self) {
        self.now_ms += self.pick(30) as u64;
        let names = ["o", "r", "l1", "l2"];
        let (node, leaf) = (names[self.pick(4)], LEAVES[self.pick(2)]);
        match self.pick(16) {
            0..=2 => self.reconcile(node),
            3 | 4 => self.pump(node),
            5 | 6 if !self.interests.is_empty() => {
                let (index, lose) = (self.pick(self.interests.len()), self.pick(6) == 0);
                self.deliver_interest(index, lose);
            }
            7 | 8 if !self.pushes.is_empty() => {
                let index = self.pick(self.pushes.len());
                self.deliver_push(index);
            }
            9 => self.consume(leaf),
            10 | 11 => {
                let k = self.pick(STREAMS);
                match self.subs.remove(&(leaf, k)) {
                    Some((id, _)) => self.core(leaf).unsubscribe(id),
                    None => {
                        let id = self.core(leaf).subscribe(&key(k), SubscribeOptions::default()).unwrap();
                        self.subs.insert((leaf, k), (id, None));
                    }
                }
            }
            12 => {
                let k = self.pick(STREAMS);
                self.published[k] += 1;
                let record = Bytes::copy_from_slice(&self.published[k].to_be_bytes());
                self.core("o").publish(&key(k), vec![record]).unwrap();
            }
            13 if self.pick(12) == 0 => {
                let (up, incarnation) = (self.nodes["r"].2, self.nodes["r"].1);
                if up {
                    self.nodes.get_mut("r").unwrap().2 = false;
                } else {
                    self.boot("r", incarnation + 1);
                }
            }
            14 if self.pick(12) == 0 => {
                // A parent gives up on a child that is still there.
                let (parent, child) = [("o", "r"), ("r", "l1"), ("r", "l2")][self.pick(3)];
                let (peer, now) = (self.peer(child), self.now());
                self.pushes.retain(|p| !(&*p.0.node == parent && p.1 == peer));
                self.core(parent).expire_downstream(&peer, now);
            }
            15 => self.now_ms += self.pick(3000) as u64,
            _ => {}
        }
    }

    fn settle(&mut self) {
        if !self.nodes["r"].2 {
            let incarnation = self.nodes["r"].1 + 1;
            self.boot("r", incarnation);
        }
        for _ in 0..600 {
            self.now_ms += 100;
            for name in ["o", "r", "l1", "l2"] {
                self.reconcile(name);
                while !self.interests.is_empty() {
                    self.deliver_interest(0, false);
                }
                self.pump(name);
                while !self.pushes.is_empty() {
                    self.deliver_push(0);
                }
            }
            LEAVES.into_iter().for_each(|leaf| self.consume(leaf));
        }
    }

    fn branches(&mut self, parent: &str, child: &str) -> usize {
        self.core(parent).stats().downstream.iter().find(|d| d.node == child).map_or(0, |d| d.branches)
    }
}

fn run(seed: u64) -> Sim {
    let mut sim = Sim::new(seed);
    for _ in 0..6000 {
        sim.step();
    }
    sim.settle();
    for k in 0..STREAMS {
        sim.published[k] += 1;
        let record = Bytes::copy_from_slice(&sim.published[k].to_be_bytes());
        sim.core("o").publish(&key(k), vec![record]).unwrap();
    }
    sim.settle();

    assert!(sim.largest_interest <= 5, "an interest of {} keys", sim.largest_interest);
    for ((leaf, k), (_, last)) in &sim.subs {
        assert_eq!(*last, Some(sim.published[*k]), "seed {seed}: {leaf} s{k}");
    }
    let mut wanted = 0;
    for leaf in LEAVES {
        let keys = sim.subs.keys().filter(|(l, _)| *l == leaf).count();
        assert_eq!((sim.branches("r", leaf), sim.branches("o", leaf)), (keys, 0), "seed {seed}: {leaf}");
        wanted += keys;
    }
    let distinct = (0..STREAMS).filter(|k| LEAVES.iter().any(|l| sim.subs.contains_key(&(*l, *k)))).count();
    assert!(wanted >= distinct && sim.core("r").stats().feeds == distinct, "seed {seed}");
    assert_eq!(sim.branches("o", "r"), distinct, "seed {seed}");

    for ((leaf, _), (id, _)) in std::mem::take(&mut sim.subs) {
        sim.core(leaf).unsubscribe(id);
    }
    sim.settle();
    for name in ["o", "r", "l1", "l2"] {
        let stats = sim.core(name).stats();
        assert_eq!((stats.feeds, stats.downstream.len()), (0, 0), "seed {seed}: {name} still holds something");
    }
    sim
}

#[test]
fn the_same_seed_gives_the_same_run() {
    let (a, b) = (run(7), run(7));
    assert_eq!((a.trace, a.published), (b.trace, b.published));
    assert_ne!(a.trace, run(8).trace);
}

#[test]
fn parents_hold_what_their_children_want_through_lost_answers_restarts_and_forgetful_parents() {
    // `SIM_SEED` alone, not `SEEDS`: CI's 4 seeds per World test would cut this cheap model to a tenth.
    let seeds = std::env::var("SIM_SEED").ok().map_or(1..41, |s| {
        let s: u64 = s.parse().unwrap();
        s..s + 1
    });
    check_seeds(seeds, |seed| drop(run(seed))).unwrap();
}

#[test]
fn a_large_wanted_set_goes_out_in_slices_and_a_forgotten_child_sends_it_whole_again() {
    let cfg = CoreConfig { max_keys_per_interest: 8, ..Default::default() };
    let (o, c) = (PeerId::new("o", 1), PeerId::new("c", 1));
    let mut origin = RelayCore::new(&o, AllowAll, CoreConfig { relay: false, ..cfg.clone() }).unwrap();
    let mut child = RelayCore::new(&c, AllowAll, cfg).unwrap();
    let mut now = Instant::now();
    let _subs: Vec<u64> = (0..30)
        .map(|k| {
            origin.open_stream(&key(k), 1, DeliveryPolicy::ReplayRequired).unwrap();
            child.subscribe(&key(k), SubscribeOptions::default()).unwrap()
        })
        .collect();
    let exchange = |origin: &mut RelayCore, child: &mut RelayCore, now: Instant| {
        let mut sizes = Vec::new();
        loop {
            let asked = child.reconcile(std::slice::from_ref(&o), now);
            let Some((to, interest)) = asked.into_iter().next() else { return sizes };
            sizes.push((interest.whole.is_some(), interest.wants.len()));
            let reply = origin.handle_interest(&c, interest.clone(), now);
            child.interest_result(&to, interest.revision, reply, now);
        }
    };
    assert_eq!(exchange(&mut origin, &mut child, now), [(false, 8), (false, 8), (false, 8), (false, 6)]);
    assert_eq!(origin.stats().downstream[0].branches, 30);
    assert_eq!(exchange(&mut origin, &mut child, now), [], "nothing changed, nothing is sent");
    for _ in 0..2 {
        now += Duration::from_secs(3);
        assert_eq!(exchange(&mut origin, &mut child, now), [(false, 0)], "a check the origin agrees with");
    }

    // The origin forgets the child; the next check finds out and the set goes out again, whole.
    origin.expire_downstream(&c, now);
    now += Duration::from_secs(3);
    let sizes = exchange(&mut origin, &mut child, now);
    assert_eq!(sizes, [(false, 0), (true, 8), (true, 8), (true, 8), (true, 6)]);
    assert_eq!((origin.stats().downstream[0].branches, child.stats().resyncs), (30, 1));
}

/// An origin and one child with five-key interests, stepped by hand.
struct Pair {
    o: RelayCore,
    c: RelayCore,
    po: PeerId,
    pc: PeerId,
    t0: Instant,
    ms: u64,
}
impl Pair {
    fn new() -> Self {
        let cfg = CoreConfig {
            max_keys_per_interest: 5,
            hub: HubLimits { max_batch_bytes: 512 << 10, max_streams: 64, ..Default::default() },
            store: MemoryLimits { max_streams: 64, ..Default::default() },
            ..Default::default()
        };
        let (po, pc) = (PeerId::new("o", 1), PeerId::new("c", 1));
        let mut o = RelayCore::new(&po, AllowAll, CoreConfig { relay: false, ..cfg.clone() }).unwrap();
        let c = RelayCore::new(&pc, AllowAll, cfg).unwrap();
        for k in 0..12 {
            o.open_stream(&key(k), 1, DeliveryPolicy::ReplayRequired).unwrap();
        }
        Pair { o, c, po, pc, t0: Instant::now(), ms: 0 }
    }
    fn now(&self) -> Instant {
        self.t0 + Duration::from_millis(self.ms)
    }
    fn interests(&mut self) -> Vec<Interest> {
        let now = self.now();
        self.c.reconcile(std::slice::from_ref(&self.po), now).into_iter().map(|(_, i)| i).collect()
    }
    fn deliver(&mut self, i: Interest) -> Reply {
        let now = self.now();
        let rev = i.revision;
        let reply: Reply = self.o.handle_interest(&self.pc, i, now).into();
        self.c.interest_result(&self.po, rev, reply.clone(), now);
        reply
    }
    fn pump(&mut self) {
        let now = self.now();
        deliver(&mut self.o, &self.po, &mut self.c, &self.pc, now);
    }
    fn step(&mut self, ms: u64) {
        self.ms += ms;
        for i in self.interests() {
            self.deliver(i);
        }
        self.pump();
    }
    fn branches(&self) -> usize {
        self.o.stats().downstream.first().map_or(0, |d| d.branches)
    }
}

#[test]
fn a_parent_that_forgets_the_child_in_the_middle_of_a_whole_set_is_sent_it_again() {
    let mut w = Pair::new();
    let _subs: Vec<u64> = (0..12).map(|k| w.c.subscribe(&key(k), SubscribeOptions::default()).unwrap()).collect();
    for _ in 0..10 {
        w.step(100);
    }
    assert_eq!(w.branches(), 12);
    // parent forgets the child; heartbeat finds out
    let now = w.now();
    w.o.expire_downstream(&w.pc, now);
    w.ms += 2100;
    let hb = w.interests();
    assert_eq!(hb.len(), 1);
    assert_eq!(w.deliver(hb.into_iter().next().unwrap()), Reply::OutOfSync);
    // first chunk of the resync
    let c1 = w.interests();
    assert!(c1[0].whole.is_some());
    w.deliver(c1.into_iter().next().unwrap());
    // parent forgets the child again mid-sync
    let now = w.now();
    w.o.expire_downstream(&w.pc, now);
    for _ in 0..100 {
        w.step(100);
    } // 10 s of heartbeats
    assert_eq!(w.branches(), 12, "child believes 12 fed, parent holds fewer");
}

#[test]
fn a_stream_dropped_while_its_slice_is_in_flight_is_not_left_on_the_parent() {
    let mut w = Pair::new();
    let subs: Vec<u64> = (0..12).map(|k| w.c.subscribe(&key(k), SubscribeOptions::default()).unwrap()).collect();
    for _ in 0..10 {
        w.step(100);
    }
    let now = w.now();
    w.o.expire_downstream(&w.pc, now);
    w.ms += 2100;
    let hb = w.interests();
    w.deliver(hb.into_iter().next().unwrap());
    let c1 = w.interests(); // chunk 1 in flight: s00..s04
    w.c.unsubscribe(subs[0]);
    w.ms += 6000; // linger passes while in flight
    assert!(w.interests().is_empty());
    w.deliver(c1.into_iter().next().unwrap());
    for _ in 0..100 {
        w.step(100);
    }
    assert_eq!(w.branches(), w.c.stats().feeds);
}

#[test]
fn a_listener_that_returns_restarts_the_linger() {
    let mut w = Pair::new();
    let s = w.c.subscribe(&key(0), SubscribeOptions::default()).unwrap();
    for _ in 0..5 {
        w.step(100);
    }
    w.c.unsubscribe(s);
    w.step(100); // idle from here (t=600)
    w.ms = 4600;
    let s = w.c.subscribe(&key(0), SubscribeOptions::default()).unwrap();
    w.step(0);
    w.ms = 5000;
    w.c.unsubscribe(s);
    w.step(0);
    w.ms = 5700; // 0.7 s after the last listener left; linger is 5 s
    w.step(0);
    assert_eq!(w.c.stats().feeds, 1, "feed expired {} ms after its last listener left", 700);
}

#[test]
fn a_branch_with_a_record_is_pushed_however_many_drained_branches_are_ahead_of_it() {
    let streams = 3000;
    let cfg = CoreConfig {
        hub: HubLimits { max_streams: 4096, max_interest_keys: 4096, ..CoreConfig::default().hub },
        store: MemoryLimits { max_streams: 4096, ..Default::default() },
        ..Default::default()
    };
    let (o, c) = (PeerId::new("o", 1), PeerId::new("c", 1));
    let mut origin = RelayCore::new(&o, AllowAll, CoreConfig { relay: false, ..cfg.clone() }).unwrap();
    let mut child = RelayCore::new(&c, AllowAll, cfg).unwrap();
    let now = Instant::now();
    let _subs: Vec<u64> = (0..streams)
        .map(|k| {
            origin.open_stream(&key(k), 1, DeliveryPolicy::ReplayRequired).unwrap();
            child.subscribe(&key(k), SubscribeOptions::default()).unwrap()
        })
        .collect();
    while let Some((to, interest)) = child.reconcile(std::slice::from_ref(&o), now).into_iter().next() {
        let reply = origin.handle_interest(&c, interest.clone(), now);
        child.interest_result(&to, interest.revision, reply, now);
    }
    let drain = |origin: &mut RelayCore, child: &mut RelayCore| {
        let mut records = 0;
        while let Some((id, body)) = origin.next_push("c", Lane::Live, now, None) {
            let (_, _, push) = decode(&body);
            records += push.items.iter().filter(|item| matches!(item.2, Item::Batch(_))).count();
            let answer = answer(child, &o, &body, now);
            origin.push_answered(&c, Lane::Live, id, answer, now);
        }
        records
    };
    drain(&mut origin, &mut child);
    for round in 0..3 {
        for k in 0..streams {
            origin.publish(&key(k), vec![Bytes::from_static(b"x")]).unwrap();
        }
        assert_eq!(drain(&mut origin, &mut child), streams, "round {round}");
        origin.publish(&key(streams - 1), vec![Bytes::from_static(b"y")]).unwrap();
        assert_eq!(drain(&mut origin, &mut child), 1, "round {round}, behind the branches just drained");
    }
}
