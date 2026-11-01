//! The acceptor and proposer cores on a simulated network: late, lost and duplicated messages,
//! clocks that drift, crashes, restarts and partitions, and each acceptor's log on a disk whose
//! writes take a tick and are cut by a crash at any point, inside a rewrite too. The oracle knows
//! real time: at no instant do two processes believe they hold one key, every grant of a key
//! carries a larger epoch than the one before, once every acceptor restarts at once each key
//! granted and not released is listed, and once the faults stop every key can be taken again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axon::files::{Files, MemoryFiles};
use axon_testkit::{Link, NodeId, SimConfig, Simulator, check_seeds, crashed, seeds};
use bytes::Bytes;

use super::core::*;
use super::ready;
use crate::OwnershipError;
use crate::sim::Clock;

const LEASE: Duration = Duration::from_secs(1);
const DRIFT_PPM: i64 = 4_000;
const MARGIN: Duration = Duration::from_millis(20);
const PATIENCE: Duration = Duration::from_millis(500);
const TICK: Duration = Duration::from_millis(20);
const RUN: Duration = Duration::from_secs(60);
const UNIX_US: u64 = 1_800_000_000_000_000;
const ACCEPTORS: usize = 3;
const PROPOSERS: usize = 3;
const KEYS: [&str; 2] = ["k/a", "k/b"];
const PROTOCOL: u64 = 0x0053_494d;

/// Holders assume twice the drift any two clocks here have between them.
const DRIFT: f64 = 2.0 * 2.0 * DRIFT_PPM as f64 / 1e6;

fn hold() -> Duration {
    LEASE.mul_f64(1.0 - DRIFT) - MARGIN
}

fn wait() -> Duration {
    LEASE.mul_f64(1.0 + DRIFT) + MARGIN
}

#[derive(Clone)]
enum Msg {
    Req { request: u64, body: Bytes },
    Rep { request: u64, body: Bytes },
}

struct AcceptorHost {
    at: NodeId,
    clock: Clock,
    core: Option<Acceptor>,
    up_at: Option<Duration>,
    /// Its log's files, kept across crashes.
    disk: MemoryFiles,
    /// Replies held until the log holds what they answered: the frames each waits for, and to whom.
    waiting: Vec<(u64, NodeId, Msg)>,
}

impl AcceptorHost {
    fn boot(&mut self, real: Duration) {
        let (now, stored) = (self.clock.elapsed(real), ready(Stored::load(&self.disk, PROTOCOL)).expect("in memory"));
        self.core = Some(Acceptor::new(now, self.clock.unix_us(real), wait(), 1024).with_log(stored, PROTOCOL, now));
    }
}

struct ProposerHost {
    at: NodeId,
    id: String,
    boot: u64,
    clock: Clock,
    core: Option<Proposer>,
    up_at: Option<Duration>,
    renew_at: Duration,
    acquiring: BTreeSet<String>,
    /// Request id -> its deadline in real time.
    outstanding: BTreeMap<u64, Duration>,
}

struct World {
    sim: Simulator<Msg>,
    acceptors: Vec<AcceptorHost>,
    proposers: Vec<ProposerHost>,
    granted: BTreeMap<String, u64>,
    /// Keys granted and not released since, with the process they went to.
    unreleased: BTreeMap<String, String>,
    /// Keys the restart of every acceptor found listed.
    listed: u64,
    /// Refusals by acceptors that crashed since.
    refused: u64,
    grants: u64,
    faults: bool,
    log: Vec<String>,
}

impl World {
    fn new(seed: u64) -> Self {
        let mut sim = Simulator::new(SimConfig {
            seed,
            jitter: Duration::from_millis(50),
            drop_every: 20,
            duplicate_every: 20,
            ordered: false,
            ..SimConfig::default()
        });
        // Each wall clock is off by up to 0.4 s; the monotonic one is its `elapsed`.
        let clock = |sim: &mut Simulator<Msg>| Clock {
            ppm: sim.pick(0..2 * DRIFT_PPM as u64 + 1) as i64 - DRIFT_PPM,
            base: Duration::from_micros((UNIX_US as i64 + sim.pick(0..800_000) as i64 - 400_000) as u64),
        };
        let acceptors = (0..ACCEPTORS)
            .map(|i| {
                let clock = clock(&mut sim);
                let at = sim.add_node(&format!("acc-{i}")).unwrap();
                let mut host =
                    AcceptorHost { at, clock, core: None, up_at: None, disk: MemoryFiles::new(), waiting: Vec::new() };
                host.boot(Duration::ZERO);
                host
            })
            .collect();
        let proposers = (0..PROPOSERS)
            .map(|i| {
                let clock = clock(&mut sim);
                let id = format!("node-{i}");
                let at = sim.add_node(&id).unwrap();
                let mut host = ProposerHost {
                    at,
                    id,
                    boot: 0,
                    clock,
                    core: None,
                    up_at: None,
                    renew_at: Duration::ZERO,
                    acquiring: BTreeSet::new(),
                    outstanding: BTreeMap::new(),
                };
                host.start();
                host
            })
            .collect();
        Self {
            sim,
            acceptors,
            proposers,
            granted: BTreeMap::new(),
            unreleased: BTreeMap::new(),
            listed: 0,
            refused: 0,
            grants: 0,
            faults: true,
            log: Vec::new(),
        }
    }

    fn run(&mut self, until: Duration) {
        let mut next_tick = self.sim.now() + TICK;
        while self.sim.now() < until {
            if self.sim.next_delivery_at().is_some_and(|at| at <= next_tick) {
                if let Some(delivery) = self.sim.step() {
                    self.deliver(delivery.from, delivery.to, delivery.payload);
                }
            } else {
                self.sim.advance_idle_to(next_tick).unwrap();
            }
            if self.sim.now() >= next_tick {
                next_tick = self.sim.now() + TICK;
                self.tick();
            }
            self.check();
        }
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        let real = self.sim.now();
        match msg {
            Msg::Req { request, body } => {
                let Some(a) = self.acceptors.iter_mut().find(|a| a.at == to) else { return };
                let Some(core) = a.core.as_mut() else { return };
                let items = decode_items(body).expect("a request the cores encoded decodes");
                let (now, unix_us) = (a.clock.elapsed(real), a.clock.unix_us(real));
                let (answers, frame) = core.answer_all(&items, now, unix_us);
                assert_eq!(core.stored(), core.stored_counted(), "the leases counted as the log holds them");
                let reply = Msg::Rep { request, body: encode_answers(&answers) };
                match frame {
                    Some(frame) => a.waiting.push((frame, from, reply)),
                    None => {
                        let _ = self.sim.send(to, from, reply);
                    }
                }
            }
            Msg::Rep { request, body } => {
                let Some(p) = self.proposers.iter().position(|p| p.at == to) else { return };
                if self.proposers[p].outstanding.remove(&request).is_none() {
                    return;
                }
                let answers = decode_answers(body).expect("an answer the cores encoded decodes");
                self.reply(p, request, Some(answers));
            }
        }
    }

    fn reply(&mut self, p: usize, request: u64, answers: Option<Vec<Answer>>) {
        let real = self.sim.now();
        let host = &mut self.proposers[p];
        let (now, unix_us) = (host.clock.elapsed(real), host.clock.unix_us(real));
        let mut out = Vec::new();
        if let Some(core) = host.core.as_mut() {
            core.reply(request, answers, now, unix_us, &mut out);
        }
        self.apply(p, out);
    }

    fn apply(&mut self, p: usize, out: Vec<Out>) {
        let real = self.sim.now();
        for o in out {
            match o {
                Out::Send { request, acceptor, items } => {
                    let host = &mut self.proposers[p];
                    for item in items.iter().filter(|i| i.kind == Kind::Release) {
                        if self.unreleased.get(&*item.key).is_some_and(|by| *by == *item.ballot.node) {
                            self.unreleased.remove(&*item.key);
                        }
                    }
                    host.outstanding.insert(request, real + PATIENCE);
                    let to = self.acceptors[acceptor].at;
                    let _ = self.sim.send(host.at, to, Msg::Req { request, body: encode_items(&items) });
                }
                Out::Done { key, renew, result } => {
                    let host = &mut self.proposers[p];
                    assert!(
                        !matches!(&result, Err(OwnershipError::Unavailable(why)) if why.starts_with("no majority answered in time")),
                        "{} stalled on {key} at {real:?}\n{}",
                        host.id,
                        self.log.join("\n")
                    );
                    // Only the acceptors fail a renewal: this node's own release or acquire of a key ends it quietly.
                    assert!(
                        !(renew && matches!(&result, Err(OwnershipError::Unavailable(why)) if why == "superseded")),
                        "{} failed a renewal by releasing {key} at {real:?}",
                        host.id
                    );
                    if renew || !host.acquiring.remove(&*key) {
                        continue;
                    }
                    if let Ok(epoch) = result {
                        let last = self.granted.insert(key.to_string(), epoch);
                        assert!(
                            last.is_none_or(|last| last < epoch),
                            "{} granted {key} with epoch {epoch} after epoch {last:?} at {real:?}\n{}",
                            host.id,
                            self.log.join("\n")
                        );
                        self.grants += 1;
                        self.unreleased.insert(key.to_string(), format!("{}/{}", host.id, host.boot));
                        self.log.push(format!("{real:?} {} took {key} at {epoch}", host.id));
                    }
                }
            }
        }
    }

    fn tick(&mut self) {
        let real = self.sim.now();
        for p in 0..self.proposers.len() {
            let late: Vec<u64> = self.proposers[p]
                .outstanding
                .iter()
                .filter(|(_, deadline)| real >= **deadline)
                .map(|(request, _)| *request)
                .collect();
            for request in late {
                self.proposers[p].outstanding.remove(&request);
                self.reply(p, request, None);
            }
            let host = &mut self.proposers[p];
            let (now, unix_us) = (host.clock.elapsed(real), host.clock.unix_us(real));
            let mut out = Vec::new();
            if let Some(core) = host.core.as_mut() {
                core.tick(now, &mut out);
                if now >= host.renew_at {
                    host.renew_at = now + LEASE / 4;
                    core.renew(now, &mut out);
                }
                if self.faults && self.sim.pick(0..10) == 0 {
                    let key = KEYS[self.sim.pick(0..KEYS.len() as u64) as usize];
                    if core.holds(key, now).is_none() && !host.acquiring.contains(key) {
                        host.acquiring.insert(key.to_owned());
                        core.acquire(key.into(), now, unix_us, &mut out);
                    }
                }
                if self.faults && self.sim.pick(0..100) == 0 {
                    core.release_all(&mut out);
                }
            }
            self.apply(p, out);
        }
        for a in &mut self.acceptors {
            let Some(core) = a.core.as_mut() else { continue };
            core.sweep(a.clock.elapsed(real));
            assert_eq!(core.stored(), core.stored_counted(), "the leases counted after a sweep");
            // A write every other tick or so, landing whole: then the replies it lets go.
            if self.sim.pick(0..2) == 0
                && let Some(ops) = core.take_write()
            {
                drop(a.disk.apply(ops));
                let core = a.core.as_mut().expect("still up");
                core.written(true);
                let synced = core.synced();
                for (_, to, reply) in a.waiting.extract_if(.., |(through, ..)| *through <= synced) {
                    let _ = self.sim.send(a.at, to, reply);
                }
            }
        }
        if self.faults {
            self.fault();
        }
        self.restart();
    }

    fn fault(&mut self) {
        let real = self.sim.now();
        match self.sim.pick(0..1000) {
            0..=2 => {
                let a = self.sim.pick(0..ACCEPTORS as u64) as usize;
                if self.acceptors[a].core.is_some() {
                    self.crash_acceptor(a);
                    self.sim.set_node_down(self.acceptors[a].at, true).unwrap();
                    self.acceptors[a].up_at = Some(real + Duration::from_millis(self.sim.pick(0..3000)));
                    self.log.push(format!("{real:?} acc-{a} crashed"));
                }
            }
            3..=4 => {
                let p = self.sim.pick(0..PROPOSERS as u64) as usize;
                if self.proposers[p].core.take().is_some() {
                    self.sim.crash_node(self.proposers[p].at).unwrap();
                    self.sim.set_node_down(self.proposers[p].at, true).unwrap();
                    self.proposers[p].up_at = Some(real + Duration::from_millis(self.sim.pick(0..3000)));
                    self.log.push(format!("{real:?} {} crashed", self.proposers[p].id));
                }
            }
            5..=7 => {
                let all: Vec<NodeId> =
                    self.acceptors.iter().map(|a| a.at).chain(self.proposers.iter().map(|p| p.at)).collect();
                let cut = 1 + self.sim.pick(0..all.len() as u64 - 1) as usize;
                let shift = self.sim.pick(0..all.len() as u64) as usize;
                let order: Vec<NodeId> = (0..all.len()).map(|i| all[(i + shift) % all.len()]).collect();
                self.sim.partition(&order[..cut], &order[cut..]).unwrap();
                self.log.push(format!("{real:?} partition {:?} | {:?}", &order[..cut], &order[cut..]));
            }
            8..=15 => {
                self.sim.heal();
            }
            // One node's links turn slow, so its answers come back late but inside its patience.
            16..=20 => {
                let p = self.sim.pick(0..PROPOSERS as u64) as usize;
                let delay = Some(Duration::from_millis(self.sim.pick(0..200)));
                for a in 0..ACCEPTORS {
                    self.sim.set_link(self.proposers[p].at, self.acceptors[a].at, Link { up: true, delay }).unwrap();
                }
            }
            _ => {}
        }
    }

    /// Every proposer's link to every acceptor.
    fn links(&mut self, link: Link) {
        for p in &self.proposers {
            for a in &self.acceptors {
                self.sim.set_link(p.at, a.at, link).unwrap();
            }
        }
    }

    /// Drops what it had not written, but for what a crash inside its next write leaves, and the replies waiting on it.
    fn crash_acceptor(&mut self, a: usize) {
        let torn = self.sim.pick(0..1 << 20);
        let host = &mut self.acceptors[a];
        let mut core = host.core.take().expect("up");
        self.refused += core.refused;
        if let Some(ops) = core.take_write() {
            drop(host.disk.apply(crashed(&ops, torn)));
        }
        host.waiting.clear();
        self.sim.crash_node(host.at).unwrap();
    }

    fn restart(&mut self) {
        let real = self.sim.now();
        for (i, a) in self.acceptors.iter_mut().enumerate() {
            if a.up_at.is_some_and(|at| real >= at) {
                a.up_at = None;
                self.sim.set_node_down(a.at, false).unwrap();
                a.boot(real);
                self.log.push(format!("{real:?} acc-{i} restarted"));
            }
        }
        for p in &mut self.proposers {
            if p.up_at.is_some_and(|at| real >= at) {
                p.up_at = None;
                self.sim.set_node_down(p.at, false).unwrap();
                p.start();
                self.log.push(format!("{real:?} {} restarted", p.id));
            }
        }
    }

    /// At no instant do two processes believe they hold one key.
    fn check(&self) {
        let real = self.sim.now();
        for key in KEYS {
            let holders: Vec<String> = self
                .proposers
                .iter()
                .filter_map(|p| {
                    p.core.as_ref()?.holds(key, p.clock.elapsed(real)).map(|e| format!("{}#{} at {e}", p.id, p.boot))
                })
                .collect();
            assert!(holders.len() <= 1, "{key} held by {holders:?} at {real:?}\n{}", self.log.join("\n"));
        }
    }

    /// Every acceptor crashes at once, cut off from the proposers, and starts again: once warm, each key granted and
    /// not released is listed as lapsed by every majority of them, as a starting node's sweep asks. Not always for
    /// the process it went to: a later propose that reached fewer than a majority may have replaced it at one.
    fn restart_every_acceptor(&mut self) {
        self.faults = false;
        self.sim.heal();
        self.links(Link { up: false, delay: None });
        let real = self.sim.now();
        if self.sim.pick(0..2) == 0 {
            self.step_clocks_back(real);
        }
        // Then each crashes again inside the rewrite its start hands out, and starts again.
        for _ in 0..2 {
            for a in 0..ACCEPTORS {
                if self.acceptors[a].core.is_some() {
                    self.crash_acceptor(a);
                    self.sim.set_node_down(self.acceptors[a].at, true).unwrap();
                }
                self.acceptors[a].up_at = Some(real);
            }
            self.restart();
        }
        self.log.push(format!("{real:?} every acceptor restarted, twice"));
        let warm = real + wait().mul_f64(1.1) + TICK;
        self.run(warm);
        let real = self.sim.now();
        let kinds = ["k".to_string()];
        let listed: Vec<BTreeSet<String>> = self
            .acceptors
            .iter()
            .map(|a| {
                let core = a.core.as_ref().expect("restarted");
                let (keys, _) = core.held_by(crate::Holder::Lapsed, &kinds, "", a.clock.elapsed(real)).expect("warm");
                keys.iter().map(|k| k.to_string()).collect()
            })
            .collect();
        for (one, two) in [(0, 1), (0, 2), (1, 2)] {
            for (key, by) in &self.unreleased {
                assert!(
                    listed[one].contains(key) || listed[two].contains(key),
                    "{key}, granted to {by} and not released, is not listed by acc-{one} and acc-{two}\n{}",
                    self.log.join("\n")
                );
            }
        }
        self.listed += self.unreleased.len() as u64;
        self.links(Link { up: true, delay: None });
        self.faults = true;
    }

    /// Every wall clock steps back an hour, as when clocks that ran ahead are set right, and every proposer restarts
    /// with the acceptors, so no process remembers an epoch: the next grant of a key must still be above the last.
    fn step_clocks_back(&mut self, real: Duration) {
        const STEP: Duration = Duration::from_secs(3600);
        for a in &mut self.acceptors {
            a.clock.base -= STEP;
        }
        for p in &mut self.proposers {
            p.clock.base -= STEP;
            if p.core.take().is_some() {
                self.sim.crash_node(p.at).unwrap();
                self.sim.set_node_down(p.at, true).unwrap();
            }
            p.up_at = Some(real);
        }
        self.log.push(format!("{real:?} every clock stepped back {STEP:?}, every proposer crashed"));
    }

    /// Faults stop, every lease runs out, and each key is taken by one node within a few rounds.
    fn settle_and_take(&mut self) {
        self.faults = false;
        self.links(Link { up: true, delay: None });
        let real = self.sim.now();
        for a in &mut self.acceptors {
            a.up_at = a.up_at.map(|_| real);
        }
        for p in &mut self.proposers {
            p.up_at = p.up_at.map(|_| real);
        }
        self.restart();
        for p in 0..self.proposers.len() {
            let mut out = Vec::new();
            if let Some(core) = self.proposers[p].core.as_mut() {
                core.release_all(&mut out);
            }
            self.apply(p, out);
            self.proposers[p].renew_at = Duration::MAX;
        }
        let quiet = self.sim.now() + 3 * wait();
        self.run(quiet);
        // Messages are still lost now and then, so a node tries a few times.
        for key in KEYS {
            let before = self.grants;
            for tries in 0.. {
                if self.grants > before {
                    break;
                }
                assert!(tries < 5, "{key} could not be taken once faults stopped\n{}", self.log.join("\n"));
                let real = self.sim.now();
                let host = &mut self.proposers[0];
                let (now, unix_us) = (host.clock.elapsed(real), host.clock.unix_us(real));
                let mut out = Vec::new();
                host.acquiring.insert(key.to_owned());
                host.core.as_mut().expect("restarted above").acquire(key.into(), now, unix_us, &mut out);
                self.apply(0, out);
                self.run(real + 2 * PATIENCE);
            }
        }
    }
}

impl ProposerHost {
    fn start(&mut self) {
        self.boot += 1;
        let node: Arc<str> = format!("{}/{}", self.id, self.boot).into();
        // Every request ends by PATIENCE and a tick, answered or not, and waits at most one request
        // ahead of it; a round has two phases. So a round the core has to end by its own deadline
        // stalled with every answer in.
        self.core = Some(Proposer::new(node, ACCEPTORS, LEASE, hold(), 5 * PATIENCE));
        self.acquiring.clear();
        self.outstanding.clear();
        self.renew_at = Duration::ZERO;
    }
}

fn run(seed: u64) -> World {
    let mut world = World::new(seed);
    for _ in 0..3 {
        let at = world.sim.now() + Duration::from_millis(world.sim.pick(5_000..20_000));
        world.run(at);
        world.restart_every_acceptor();
    }
    world.run(RUN.max(world.sim.now()));
    world.settle_and_take();
    world
}

#[test]
fn leases_stay_exclusive_and_epochs_grow_under_faults() {
    let seeds = seeds(0..40);
    let n = seeds.end - seeds.start;
    let (grants, refused, listed) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    check_seeds(seeds, |seed| {
        let world = run(seed);
        grants.fetch_add(world.grants, Ordering::Relaxed);
        listed.fetch_add(world.listed, Ordering::Relaxed);
        let r: u64 = world.acceptors.iter().filter_map(|a| a.core.as_ref()).map(|a| a.refused).sum();
        refused.fetch_add(r + world.refused, Ordering::Relaxed);
    })
    .unwrap();
    let (grants, refused, listed) = (grants.into_inner(), refused.into_inner(), listed.into_inner());
    eprintln!("{n} seeds: {grants} grants, {refused} refusals, {listed} keys listed after every acceptor restarted");
    // A holder that keeps renewing takes nothing new, so this is counted over all seeds.
    assert!(grants >= 10 * n && refused > 0, "the runs exercised little: {grants} grants, {refused} refusals");
    assert!(listed >= n, "few keys were held when every acceptor restarted: {listed}");
}

#[test]
fn same_seed_same_run() {
    let (one, two) = (run(7), run(7));
    assert!(one.sim.trace() == two.sim.trace() && one.log == two.log, "seed 7 ran two ways");
}
