//! The two state machines on a simulated network. Sessions, their loss and the timers are the
//! harness's: a closed process is seen at once, a dead link after the idle timeout.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use axon_net::frame::{Decoder, Encoder};
use axon_net::{Codecs, Compression, Frame};
use axon_testkit::{Link, SimConfig, Simulator, check_seeds, seeds};
use bytes::{Bytes, BytesMut};

use super::SeedTimings;
use super::core::*;
use crate::{DiscoveryError, NodeRecord};

const TICK: Duration = Duration::from_millis(10);
const REDIAL: Duration = Duration::from_secs(1);
const UNIX_MS: u64 = 1_800_000_000_000;

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s)
}

fn warm_up() -> Duration {
    SeedTimings::default().warm_up
}

/// spec/wan.md § 9.
fn wan() -> SeedTimings {
    SeedTimings { idle: secs(8.0), idle_hold: secs(7.0), closed_grace: secs(4.0), warm_up: secs(8.0) }
}

fn through_wire(frame: &Frame) -> Frame {
    let mut wire = Vec::new();
    Encoder::new(Compression::None, 1, 512, 32, MAX_FRAME).unwrap().encode(frame, &mut wire).unwrap();
    let (h, payload) = Decoder::new(Codecs::ALL, MAX_FRAME).decode(&mut BytesMut::from(&wire[..])).unwrap().unwrap();
    Frame { kind: h.kind, schema: h.schema, payload, ..Frame::default() }
}

/// Id, incarnation, version.
type Members = Vec<(String, u128, u64)>;

#[derive(Clone)]
struct Msg {
    session: SessionId,
    to_seed: bool,
    frame: Frame,
}

struct SeedHost {
    at: usize,
    id: String,
    entry: String,
    timings: SeedTimings,
    server: Option<Server>,
}

struct NodeHost {
    at: usize,
    id: String,
    boot: u64,
    incarnation: u128,
    node: Option<Node>,
    metadata: u64,
    sessions: BTreeMap<String, SessionId>,
    redial: BTreeMap<String, Duration>,
    seen: Option<(u64, Members)>,
}

struct Wire {
    node: usize,
    seed: usize,
    dead_since: Option<Duration>,
}

struct World {
    sim: Simulator<Msg>,
    /// The nodes', and every seed's but an odd one's.
    timings: SeedTimings,
    seeds: Vec<SeedHost>,
    nodes: Vec<NodeHost>,
    wires: BTreeMap<SessionId, Wire>,
    cut: BTreeSet<(usize, usize)>,
    next_session: SessionId,
    /// Nodes that a view may lose.
    disturbed: BTreeSet<String>,
}

impl World {
    fn new(seed: u64, seeds: usize, nodes: usize) -> Self {
        let mut sim = Simulator::new(SimConfig { seed, jitter: Duration::from_millis(3), ..SimConfig::default() });
        let seeds = (0..seeds)
            .map(|i| {
                let id = format!("seed-{i}");
                let entry = if i % 2 == 0 { format!("{id}:7001") } else { format!("{id}@{id}:7001") };
                let (at, timings) = (sim.add_node(&id).unwrap(), SeedTimings::default());
                SeedHost { at, id, entry, timings, server: Some(Server::new(Duration::ZERO, timings)) }
            })
            .collect();
        let mut world = Self {
            sim,
            timings: SeedTimings::default(),
            seeds,
            nodes: Vec::new(),
            wires: BTreeMap::new(),
            cut: BTreeSet::new(),
            next_session: 1,
            disturbed: BTreeSet::new(),
        };
        for _ in 0..nodes {
            world.add_node();
        }
        world
    }

    fn random(&mut self, below: usize) -> usize {
        self.sim.pick(0..below as u64) as usize
    }

    fn now(&self) -> Duration {
        self.sim.now()
    }

    fn unix_ms(&self) -> u64 {
        UNIX_MS + self.now().as_millis() as u64
    }

    fn entries(&self) -> Vec<String> {
        self.seeds.iter().map(|s| s.entry.clone()).collect()
    }

    fn add_node(&mut self) -> usize {
        let id = format!("node-{}", self.nodes.len());
        let at = self.sim.add_node(&id).unwrap();
        self.nodes.push(NodeHost {
            at,
            id,
            boot: 0,
            incarnation: 0,
            node: None,
            metadata: 0,
            sessions: BTreeMap::new(),
            redial: BTreeMap::new(),
            seen: None,
        });
        let n = self.nodes.len() - 1;
        self.boot(n);
        n
    }

    fn record(&self, n: usize) -> NodeRecord {
        let host = &self.nodes[n];
        NodeRecord {
            id: host.id.clone(),
            incarnation: host.incarnation,
            address: format!("10.0.0.{}:7000", n + 1).parse().unwrap(),
            metadata: [("rev".to_owned(), host.metadata.to_string())].into(),
            version: 0,
        }
    }

    fn boot(&mut self, n: usize) {
        let incarnation = (self.unix_ms() as u128) << 64 | (self.nodes[n].boot + 1) as u128;
        self.boot_as(n, incarnation);
    }

    fn boot_as(&mut self, n: usize, incarnation: u128) {
        assert!(self.nodes[n].node.is_none());
        let entries = self.entries();
        let host = &mut self.nodes[n];
        host.boot += 1;
        host.incarnation = incarnation;
        host.node = Some(Node::new(host.id.clone(), incarnation, &entries, self.timings).unwrap());
        host.seen = None;
        self.register(n);
    }

    fn register(&mut self, n: usize) {
        let (record, now, mut out) = (self.record(n), self.now(), Vec::new());
        self.nodes[n].node.as_mut().unwrap().set_record(Some(record), now, &mut out);
        self.node_out(n, out);
    }

    fn change(&mut self, n: usize) {
        self.nodes[n].metadata += 1;
        self.register(n);
    }

    fn leave(&mut self, n: usize) {
        self.disturbed.insert(self.nodes[n].id.clone());
        let (now, mut out) = (self.now(), Vec::new());
        self.nodes[n].node.as_mut().unwrap().set_record(None, now, &mut out);
        self.node_out(n, out);
    }

    fn crash(&mut self, n: usize) {
        self.disturbed.insert(self.nodes[n].id.clone());
        self.nodes[n].node = None;
        self.nodes[n].redial.clear();
        self.sim.crash_node(self.nodes[n].at).unwrap();
        for session in std::mem::take(&mut self.nodes[n].sessions).into_values() {
            self.close(session, false);
        }
    }

    fn crash_seed(&mut self, s: usize) {
        self.seeds[s].server = None;
        self.sim.crash_node(self.seeds[s].at).unwrap();
        let sessions: Vec<_> = self.wires.iter().filter(|(_, w)| w.seed == s).map(|(id, _)| *id).collect();
        for session in sessions {
            self.close(session, false);
        }
    }

    fn start_seed(&mut self, s: usize) {
        self.seeds[s].server = Some(Server::new(self.now(), self.seeds[s].timings));
    }

    fn set_cut(&mut self, n: usize, s: usize, cut: bool) {
        if cut {
            self.disturbed.insert(self.nodes[n].id.clone());
            self.cut.insert((n, s));
        } else if self.cut.remove(&(n, s)) {
            // What was sent across the cut is lost, which a session does not survive.
            let broken: Vec<_> =
                self.wires.iter().filter(|(_, w)| (w.node, w.seed) == (n, s)).map(|(id, _)| *id).collect();
            broken.into_iter().for_each(|session| self.close(session, false));
        }
        self.sim.set_link(self.nodes[n].at, self.seeds[s].at, Link { up: !cut, delay: None }).unwrap();
    }

    fn isolate(&mut self, n: usize) {
        for s in 0..self.seeds.len() {
            self.set_cut(n, s, true);
        }
    }

    fn heal(&mut self) {
        for (n, s) in self.cut.clone() {
            self.set_cut(n, s, false);
        }
    }

    /// Ends a session on both ends.
    fn close(&mut self, session: SessionId, idle: bool) {
        let Some(wire) = self.wires.remove(&session) else { return };
        let now = self.now();
        let entry = self.seeds[wire.seed].entry.clone();
        if let Some(server) = &mut self.seeds[wire.seed].server {
            server.closed(session, idle, now);
        }
        let host = &mut self.nodes[wire.node];
        if host.sessions.get(&entry) == Some(&session) {
            host.sessions.remove(&entry);
            host.redial.insert(entry.clone(), now + Duration::from_millis(100));
            if let Some(node) = &mut host.node {
                let mut out = Vec::new();
                node.lost(&entry, now, &mut out);
                self.node_out(wire.node, out);
            }
        }
    }

    fn seed_out(&mut self, s: usize, out: Vec<Out>) {
        for o in out {
            let (targets, frame): (Vec<SessionId>, _) = match o {
                Out::To(session, frame) => (vec![session], frame),
                Out::All(frame) => (self.wires.iter().filter(|(_, w)| w.seed == s).map(|(id, _)| *id).collect(), frame),
                Out::Close(session) => {
                    self.close(session, false);
                    continue;
                }
            };
            for session in targets {
                let Some(wire) = self.wires.get(&session) else { continue };
                let msg = Msg { session, to_seed: false, frame: frame.clone() };
                self.sim.send(self.seeds[s].at, self.nodes[wire.node].at, msg).unwrap();
            }
        }
    }

    fn node_out(&mut self, n: usize, out: Vec<NodeOut>) {
        for o in out {
            match o {
                NodeOut::Send(entry, frame) => {
                    let Some(session) = self.nodes[n].sessions.get(&entry).copied() else { continue };
                    let seed = self.seeds[self.wires[&session].seed].at;
                    self.sim.send(self.nodes[n].at, seed, Msg { session, to_seed: true, frame }).unwrap();
                }
                NodeOut::Timings(..) => {}
                NodeOut::Close(entry) | NodeOut::Retire(entry) => {
                    self.nodes[n].redial.remove(&entry);
                    if let Some(session) = self.nodes[n].sessions.get(&entry).copied() {
                        self.close(session, false);
                    }
                }
            }
        }
    }

    fn deliver(&mut self, msg: Msg) {
        let Some(wire) = self.wires.get(&msg.session) else { return };
        let (n, s, now, unix_ms) = (wire.node, wire.seed, self.now(), self.unix_ms());
        let frame = through_wire(&msg.frame);
        if msg.to_seed {
            let mut out = Vec::new();
            self.seeds[s].server.as_mut().unwrap().frame(msg.session, frame, now, unix_ms, &mut out);
            self.seed_out(s, out);
        } else {
            let mut out = Vec::new();
            let entry = self.seeds[s].entry.clone();
            self.nodes[n].node.as_mut().unwrap().frame(&entry, frame, now, unix_ms, &mut out);
            self.node_out(n, out);
        }
    }

    fn tick(&mut self) {
        let now = self.now();
        let mut idle = Vec::new();
        for (session, wire) in &mut self.wires {
            match self.cut.contains(&(wire.node, wire.seed)) {
                true if now >= *wire.dead_since.get_or_insert(now) + self.timings.idle => idle.push(*session),
                true => {}
                false => wire.dead_since = None,
            }
        }
        for session in idle {
            self.close(session, true);
        }
        for n in 0..self.nodes.len() {
            let Some(node) = &self.nodes[n].node else { continue };
            let entries: Vec<String> = node.seeds().map(str::to_owned).collect();
            for entry in entries {
                let host = &mut self.nodes[n];
                if host.sessions.contains_key(&entry) || host.redial.get(&entry).is_some_and(|at| now < *at) {
                    continue;
                }
                let Some(s) = self.seeds.iter().position(|s| s.entry == entry) else { continue };
                if self.cut.contains(&(n, s)) || self.seeds[s].server.is_none() {
                    host.redial.insert(entry, now + REDIAL);
                    continue;
                }
                let session = self.next_session;
                self.next_session += 1;
                self.wires.insert(session, Wire { node: n, seed: s, dead_since: None });
                host.sessions.insert(entry.clone(), session);
                let (id, incarnation, peer) = (host.id.clone(), host.incarnation, self.seeds[s].id.clone());
                let mut out = Vec::new();
                self.seeds[s].server.as_mut().unwrap().open(session, id, incarnation, now, &mut out);
                self.seed_out(s, out);
                let mut out = Vec::new();
                self.nodes[n].node.as_mut().unwrap().opened(&entry, peer, now, &mut out);
                self.node_out(n, out);
            }
        }
        for s in 0..self.seeds.len() {
            let mut out = Vec::new();
            if let Some(server) = &mut self.seeds[s].server {
                server.tick(now, &mut out);
                assert!(server.members() <= MAX_MEMBERS && server.list_bytes() <= MAX_LIST);
            }
            self.seed_out(s, out);
        }
        for n in 0..self.nodes.len() {
            let mut out = Vec::new();
            if let Some(node) = &mut self.nodes[n].node {
                node.tick(now, &mut out);
            }
            self.node_out(n, out);
        }
        self.check();
    }

    /// What must hold at every moment.
    fn check(&mut self) {
        for host in &mut self.nodes {
            let Some(node) = &mut host.node else { continue };
            let registered = node.listed(0);
            let Some((revision, view)) = node.view() else { continue };
            let view: Vec<_> = view.iter().map(|r| (r.id.clone(), r.incarnation, r.version)).collect();
            if registered {
                assert!(
                    view.iter().any(|(id, inc, _)| *id == host.id && *inc == host.incarnation),
                    "{} not in its view",
                    host.id
                );
            }
            if let Some((seen_revision, seen)) = &host.seen {
                assert_eq!(*seen_revision == revision, *seen == view, "{}: a revision is a change", host.id);
                for (id, ..) in seen.iter().filter(|(id, ..)| !view.iter().any(|(other, ..)| other == id)) {
                    assert!(self.disturbed.contains(id), "{} lost {id}, which nothing happened to", host.id);
                }
            }
            host.seen = Some((revision, view));
        }
    }

    fn run(&mut self, time: Duration) {
        let until = self.now() + time;
        while self.now() < until {
            let next = self.now() + TICK;
            while self.sim.next_delivery_at().is_some_and(|at| at <= next) {
                if let Some(delivery) = self.sim.step() {
                    self.deliver(delivery.payload);
                }
            }
            self.sim.advance_idle_to(next).unwrap();
            self.tick();
        }
    }

    /// Runs until `done`, and returns how long that took.
    fn until(&mut self, limit: Duration, mut done: impl FnMut(&mut Self) -> bool) -> Duration {
        let start = self.now();
        while !done(self) {
            assert!(self.now() - start < limit, "not within {limit:?}");
            self.run(TICK);
        }
        self.now() - start
    }

    fn live(&self, n: usize) -> bool {
        self.nodes[n].node.as_ref().is_some_and(Node::live)
    }

    fn view(&mut self, n: usize) -> Option<Members> {
        let (_, view) = self.nodes[n].node.as_mut()?.view()?;
        Some(view.iter().map(|r| (r.id.clone(), r.incarnation, r.version)).collect())
    }

    fn sees(&mut self, n: usize, other: usize) -> bool {
        let id = self.nodes[other].id.clone();
        self.view(n).is_some_and(|v| v.iter().any(|(seen, ..)| *seen == id))
    }

    /// Every running node is live and sees exactly the running, registered nodes.
    fn assert_settled(&mut self) {
        let expected: Vec<_> = (0..self.nodes.len())
            .filter(|n| self.nodes[*n].node.as_ref().is_some_and(|node| node.listed(0)))
            .map(|n| self.nodes[n].id.clone())
            .collect();
        let mut first = None;
        for n in 0..self.nodes.len() {
            if self.nodes[n].node.is_none() {
                continue;
            }
            let view = self.view(n).unwrap_or_else(|| panic!("node-{n} is frozen"));
            assert_eq!(view.iter().map(|(id, ..)| id.clone()).collect::<Vec<_>>(), expected);
            assert_eq!(first.get_or_insert(view.clone()), &view, "views differ");
        }
    }
}

#[test]
fn views_settle_for_every_seed_count() {
    for (seeds, nodes) in [(1, 2), (2, 3), (3, 8), (4, 4), (5, 8)] {
        let mut w = World::new(1, seeds, nodes);
        // Nobody is live before more than half of the seeds are past their warm-up.
        w.run(warm_up() - secs(0.1));
        assert!((0..nodes).all(|n| !w.live(n)));
        w.run(secs(1.0));
        w.assert_settled();
        assert_eq!(w.view(0).unwrap().len(), nodes);
        // A node keeps one copy of a record, however many seeds sent it.
        let (_, view) = w.nodes[0].node.as_mut().unwrap().view().unwrap();
        assert!(view.iter().all(|r| std::sync::Arc::strong_count(r) == seeds + 1));
    }
}

#[test]
fn one_seed_down_is_invisible_and_two_freeze_everyone() {
    let mut w = World::new(2, 3, 5);
    w.run(secs(6.0));
    let before = w.nodes[0].seen.clone();
    w.crash_seed(0);
    w.run(secs(10.0));
    assert_eq!(w.nodes[0].seen, before);
    w.assert_settled();

    w.crash_seed(1);
    w.run(secs(1.0));
    assert!((0..5).all(|n| !w.live(n) && w.view(n).is_none()));

    // The restarted seed's list is whole before it counts: the views come back as they were.
    w.start_seed(0);
    w.until(secs(8.0), |w| w.live(0));
    assert_eq!(w.nodes[0].seen, before);
    w.start_seed(1);
    w.run(secs(7.0));
    w.assert_settled();
    assert_eq!(w.nodes[0].seen, before);
}

#[test]
fn a_leave_is_seen_at_once_a_crash_in_three_seconds_a_dead_machine_in_eleven() {
    let mut w = World::new(3, 3, 5);
    w.run(secs(6.0));
    w.leave(1);
    assert!(w.until(secs(0.2), |w| !w.sees(0, 1)) < secs(0.1));

    w.crash(2);
    let gone = w.until(secs(3.0), |w| (0..5).all(|n| !w.sees(n, 2)));
    assert!(gone >= secs(2.0), "a crash is held for the grace: {gone:?}");

    w.isolate(3);
    let frozen = w.until(secs(5.0), |w| !w.live(3));
    let gone = w.until(secs(11.0), |w| !w.sees(0, 3)) + frozen;
    assert!(frozen <= secs(4.1) && gone >= frozen + secs(4.0), "frozen {frozen:?}, gone {gone:?}");
    assert!(w.view(3).is_none());

    w.heal();
    w.register(1);
    w.boot(2);
    w.run(secs(5.0));
    w.assert_settled();
    assert_eq!(w.view(0).unwrap().len(), 5);
}

#[test]
fn a_restart_replaces_the_old_boot_and_an_older_boot_is_refused_until_it_is_gone() {
    let mut w = World::new(4, 3, 3);
    w.run(secs(6.0));
    let old = w.nodes[1].incarnation;
    w.crash(1);
    w.boot(1);
    w.until(secs(0.5), |w| w.view(0).is_some_and(|v| v.iter().any(|(id, inc, _)| id == "node-1" && *inc > old)));
    w.run(secs(3.0));
    assert!(w.sees(0, 1), "the old boot's session ending removed the new one");

    // A boot whose clock is behind: refused while the newer one is listed, then heals by itself.
    let newer = w.nodes[1].incarnation;
    w.crash(1);
    w.boot_as(1, newer - 1);
    w.until(secs(1.0), |w| w.nodes[1].node.as_ref().unwrap().refusal() == Some(DiscoveryError::Stale));
    w.until(secs(5.0), |w| w.live(1) && w.nodes[1].node.as_ref().unwrap().refusal().is_none());
    w.run(secs(1.0));
    w.assert_settled();
}

#[test]
fn a_lower_boot_is_not_live_while_a_seed_it_reaches_still_lists_the_old_one() {
    let mut w = World::new(6, 3, 3);
    w.run(secs(6.0));
    let old = w.nodes[1].incarnation;
    // Seed 2 loses the old boot to the idle timeout and holds it 5 s; the others see the crash at once.
    w.set_cut(1, 2, true);
    w.run(w.timings.idle + secs(0.5));
    w.crash(1);
    w.heal();
    w.boot_as(1, old - 1);
    // Seeds 0 and 1 list the new boot from 2 s on, seed 2 the old one until 4.5 s: check() holds that
    // a node listed by a majority sees itself.
    let live = w.until(secs(8.0), |w| w.live(1));
    assert!(live >= secs(4.5), "live at {live:?} while seed 2 listed the old boot");
    w.run(secs(1.0));
    w.assert_settled();
}

#[test]
fn a_node_that_reaches_a_minority_of_seeds_is_in_no_view() {
    let mut w = World::new(5, 3, 4);
    w.run(secs(6.0));
    w.set_cut(2, 0, true);
    w.set_cut(2, 1, true);
    w.run(secs(10.0));
    assert!(!w.live(2) && (0..4).all(|n| !w.sees(n, 2)));
    w.heal();
    w.run(secs(3.0));
    w.assert_settled();
    assert_eq!(w.view(0).unwrap().len(), 4);
}

/// Nodes cut off from most seeds, two of them long enough to withdraw. The first back, which withdrew too or not, is
/// live only once the others are listed again: a view without them chose other quota replicas than theirs (§2.2).
#[test]
fn a_node_attached_again_is_live_only_with_the_others_cut_off_with_it() {
    let cut = |w: &mut World, n: usize, on| (0..3).filter(|s| *s != n).for_each(|s| w.set_cut(n, s, on));
    for own in [10.0, 6.5] {
        let mut w = World::new(7, 3, 3);
        w.run(secs(6.0));
        (1..3).for_each(|n| cut(&mut w, n, true));
        w.run(secs(10.0 - own));
        cut(&mut w, 0, true);
        w.run(secs(own));
        assert!((0..3).all(|n| !w.live(n)));
        cut(&mut w, 0, false);
        w.run(secs(1.0));
        w.heal();
        w.until(secs(10.0), |w| w.live(0));
        assert!((1..3).all(|o| w.sees(0, o)), "node-0 is live without the others after a {own} s cut");
        w.run(warm_up());
        w.assert_settled();
    }
}

#[test]
fn a_changed_record_reaches_every_view() {
    let mut w = World::new(6, 3, 4);
    w.run(secs(6.0));
    let version = |w: &mut World, n: usize| w.view(n).unwrap().iter().find(|(id, ..)| id == "node-1").unwrap().2;
    let before = version(&mut w, 0);
    w.change(1);
    w.until(secs(0.1), |w| (0..4).all(|n| version(w, n) > before));
    assert!(w.nodes[1].node.as_ref().unwrap().listed(before + 1));
}

#[test]
fn the_seed_list_is_replaced_one_seed_at_a_time_without_a_view_losing_anyone() {
    let mut w = World::new(7, 3, 4);
    w.run(secs(6.0));
    for replaced in 0..3 {
        let id = format!("seed-{}", w.seeds.len());
        let at = w.sim.add_node(&id).unwrap();
        let (entry, server) = (format!("{id}:7001"), Some(Server::new(w.now(), w.timings)));
        w.seeds.push(SeedHost { at, entry, id, timings: w.timings, server });
        let entries: Vec<String> = w.entries().into_iter().skip(replaced + 1).collect();
        for n in 0..4 {
            w.nodes[n].node.as_mut().unwrap().set_seeds(&entries).unwrap();
        }
        w.run(secs(7.0));
        assert!((0..4).all(|n| w.nodes[n].node.as_ref().unwrap().seeds().count() == 3), "the old seed is let go");
        w.crash_seed(replaced);
        w.run(secs(1.0));
        w.assert_settled();
        assert_eq!(w.view(0).unwrap().len(), 4);
    }
    let node = w.nodes[0].node.as_mut().unwrap();
    assert!(node.set_seeds(&[]).is_err() && node.set_seeds(&["nowhere".into()]).is_err());
}

/// Twelve random faults, then everything heals and the eight nodes reach one view. The timings are the shipped ones or
/// the WAN set, and one seed in five, short of a majority, runs a shorter `idle_hold`, which nodes do not count.
fn faults(seed: u64) -> World {
    let seeds = [1, 3, 5][seed as usize % 3];
    let mut w = World::new(seed, seeds, 0);
    if w.random(2) == 1 {
        w.timings = wan();
    }
    let mut odd = 0;
    for s in 0..seeds {
        w.seeds[s].timings = w.timings;
        if w.random(5) == 0 && odd < (seeds - 1) / 2 {
            odd += 1;
            w.seeds[s].timings.idle_hold = w.timings.closed_grace.max(secs(3.0));
        }
        w.start_seed(s);
    }
    for _ in 0..8 {
        w.add_node();
    }
    w.run(w.timings.warm_up + secs(1.0));
    for _ in 0..12 {
        let (n, s) = (w.random(8), w.random(seeds));
        match (w.random(7), w.nodes[n].node.is_some()) {
            (0, true) => w.crash(n),
            (0 | 1, false) => w.boot(n),
            (1, true) => w.leave(n),
            (2, true) => w.change(n),
            (3, true) => w.set_cut(n, s, true),
            (4, true) => w.isolate(n),
            (5, _) if w.seeds[s].server.is_some() => w.crash_seed(s),
            (5, _) => w.start_seed(s),
            _ => w.heal(),
        }
        let pause = w.random(6000) as u64;
        w.run(Duration::from_millis(pause));
    }
    w.heal();
    for s in 0..seeds {
        if w.seeds[s].server.is_none() {
            w.start_seed(s);
        }
    }
    for n in 0..8 {
        match w.nodes[n].node.is_some() {
            true => w.register(n),
            false => w.boot(n),
        }
    }
    // A seed started last warms up, then its nodes thaw.
    w.run(w.timings.warm_up * 12 / 5);
    if std::env::var("SIM_DEBUG").is_ok() {
        for n in 0..8 {
            let host = &w.nodes[n];
            let node = host.node.as_ref().unwrap();
            eprintln!(
                "{} inc={:x} live={} listed={} refusal={:?} sessions={:?}",
                host.id,
                host.incarnation,
                node.live(),
                node.listed(0),
                node.refusal(),
                host.sessions
            );
            eprintln!("{}", node.debug());
        }
    }
    w.assert_settled();
    assert_eq!(w.view(0).unwrap().len(), 8);
    w
}

#[test]
fn random_faults_never_break_a_view_and_everything_heals() {
    check_seeds(seeds(0..24), |seed| drop(faults(seed))).unwrap();
}

#[test]
fn the_same_seed_gives_the_same_run() {
    let trace = |seed| faults(seed).sim.trace().clone();
    assert_eq!(trace(7), trace(7));
    assert_ne!(trace(7), trace(8));
}

const T0: Duration = Duration::ZERO;

/// The record a node registers with. When this fails you changed what seed servers and nodes exchange: bump `VERSION`.
#[test]
fn record_bytes_are_pinned() {
    let metadata = [("k".to_owned(), "v".to_owned())].into();
    let r = NodeRecord { id: "n".into(), incarnation: 2, address: "10.0.0.1:7".parse().unwrap(), metadata, version: 3 };
    let mut pinned = vec![0, 1, b'n'];
    pinned.extend(2u128.to_be_bytes());
    pinned.extend([4, 10, 0, 0, 1, 0, 7]);
    pinned.extend(3u64.to_be_bytes());
    pinned.extend([0, 1, 0, 1, b'k', 0, 1, b'v']);
    assert_eq!(register_frame(&r).payload, pinned);
}

fn record(id: &str, incarnation: u128, version: u64) -> NodeRecord {
    let metadata = [("zone".to_owned(), "a".repeat(400))].into();
    NodeRecord { id: id.into(), incarnation, address: "[::1]:7000".parse().unwrap(), metadata, version }
}

/// A node with a session to one warm server; returns what the server sent it.
fn registered(server: &mut Server, session: SessionId, r: &NodeRecord, clock: &mut Duration) -> (Node, Vec<Out>) {
    // Past the last call's batching window, so that this one's echo is in `out`.
    *clock += secs(1.0);
    let now = *clock;
    let mut node = Node::new(r.id.clone(), r.incarnation, &["seed:1".into()], SeedTimings::default()).unwrap();
    let (mut sent, mut out) = (Vec::new(), Vec::new());
    server.open(session, r.id.clone(), r.incarnation, now, &mut out);
    node.opened("seed:1", "seed".into(), now, &mut sent);
    node.set_record(Some(r.clone()), now, &mut sent);
    for o in sent {
        let NodeOut::Send(_, frame) = o else { panic!("{o:?}") };
        server.frame(session, through_wire(&frame), now, UNIX_MS, &mut out);
    }
    (node, out)
}

fn feed(node: &mut Node, out: Vec<Out>) {
    for o in out {
        if let Out::To(_, frame) | Out::All(frame) = o {
            node.frame("seed:1", through_wire(&frame), T0, UNIX_MS, &mut Vec::new());
        }
    }
}

fn warm() -> (Server, Duration) {
    let mut server = Server::new(T0, SeedTimings::default());
    server.tick(warm_up(), &mut Vec::new());
    (server, warm_up())
}

fn closes(out: &[Out]) -> Vec<SessionId> {
    out.iter().filter_map(|o| if let Out::Close(s) = o { Some(*s) } else { None }).collect()
}

fn refusals(out: &[Out]) -> Vec<u8> {
    out.iter()
        .filter_map(|o| if let Out::To(_, f) = o { (f.kind == REFUSED).then(|| f.payload[0]) } else { None })
        .collect()
}

#[test]
fn a_record_survives_the_wire_and_a_long_list_is_split_into_packed_frames() {
    let (mut server, mut now) = warm();
    let inc = (UNIX_MS as u128) << 64;
    for i in 0..400 {
        registered(&mut server, i, &record(&format!("n{i:03}"), inc, 1), &mut now);
    }
    let mine = NodeRecord { address: "10.1.2.3:9".parse().unwrap(), ..record("me", inc | 7, 1) };
    let (mut node, out) = registered(&mut server, 1000, &mine, &mut now);
    let lists = out.iter().filter(|o| matches!(o, Out::To(_, f) if f.kind == CHANGES)).count();
    assert!(lists >= 3, "400 records of about 450 bytes do not fit two frames: {lists}");
    assert!(out.iter().all(|o| !matches!(o, Out::To(_, f) if f.kind == CHANGES && f.packed != Compression::Zstd)));
    feed(&mut node, out);
    let (_, view) = node.view().unwrap();
    assert_eq!(view.len(), 401);
    let seen = view.iter().find(|r| r.id == "me").unwrap();
    assert_eq!((&**seen, seen.version), (&mine, 1));
}

#[test]
fn only_the_session_that_registered_a_record_can_end_it() {
    let (mut server, mut now) = warm();
    let inc = (UNIX_MS as u128) << 64;
    registered(&mut server, 1, &record("a", inc, 1), &mut now);
    // The same boot again, on a new session while the old one lingers.
    registered(&mut server, 2, &record("a", inc, 1), &mut now);
    server.closed(1, true, now);
    server.tick(now + secs(60.0), &mut Vec::new());
    assert_eq!(server.members(), 1);

    // A newer boot closes the old one's session, and that ending removes nothing either.
    let (_, out) = registered(&mut server, 3, &record("a", inc + 1, 1), &mut now);
    assert_eq!(closes(&out), [2]);
    server.closed(2, false, now);
    server.tick(now + secs(120.0), &mut Vec::new());
    assert_eq!(server.members(), 1);

    server.closed(3, false, now + secs(120.0));
    server.tick(now + secs(121.9), &mut Vec::new());
    assert_eq!(server.members(), 1);
    server.tick(now + secs(122.0), &mut Vec::new());
    assert_eq!(server.members(), 0);
}

#[test]
fn a_seed_refuses_what_it_must_and_stays_within_its_bounds() {
    let (mut server, now) = warm();
    let inc = (UNIX_MS as u128) << 64;
    let mut out = Vec::new();
    let send = |server: &mut Server, session, r: &NodeRecord| {
        let mut out = Vec::new();
        server.frame(session, register_frame(r), now, UNIX_MS, &mut out);
        out
    };
    server.open(1, "a".into(), inc, now, &mut out);
    assert_eq!(refusals(&send(&mut server, 1, &record("b", inc, 1))), [3], "another node's record");
    assert_eq!(refusals(&send(&mut server, 1, &record("a", inc + 1, 1))), [3], "another boot's record");
    assert_eq!(
        refusals(&send(&mut server, 1, &NodeRecord { address: "1.1.1.1:0".parse().unwrap(), ..record("a", inc, 1) })),
        [3]
    );
    let future = (UNIX_MS as u128 + 61_000) << 64;
    server.open(2, "f".into(), future, now, &mut out);
    assert_eq!(refusals(&send(&mut server, 2, &record("f", future, 1))), [3], "a boot from the future");
    assert!(refusals(&send(&mut server, 1, &record("a", inc, 1))).is_empty());
    // The version it already has is not news, whatever it says: the list keeps its size.
    let listed = server.list_bytes();
    send(&mut server, 1, &NodeRecord { metadata: [].into(), ..record("a", inc, 1) });
    assert_eq!(server.list_bytes(), listed);
    server.open(3, "a".into(), inc - 1, now, &mut out);
    assert_eq!(refusals(&send(&mut server, 3, &record("a", inc - 1, 1))), [1]);
    assert_eq!(server.refusals, [1, 0, 4]);

    let mut out = Vec::new();
    server.frame(1, frame(REGISTER, "junk".into()), now, UNIX_MS, &mut out);
    server.frame(1, frame(99, Bytes::new()), now, UNIX_MS, &mut out);
    assert_eq!(closes(&out), [1, 1]);

    for i in 0..MAX_MEMBERS as u64 - 1 {
        let id = format!("m{i}");
        server.open(10 + i, id.clone(), inc, now, &mut Vec::new());
        assert!(
            refusals(&send(&mut server, 10 + i, &NodeRecord { metadata: [].into(), ..record(&id, inc, 1) })).is_empty()
        );
    }
    assert_eq!(server.members(), MAX_MEMBERS);
    server.open(9, "late".into(), inc, now, &mut Vec::new());
    assert_eq!(refusals(&send(&mut server, 9, &record("late", inc, 1))), [2]);
    let listed = server.list_bytes();
    let change = send(&mut server, 1, &NodeRecord { metadata: [].into(), ..record("a", inc, 2) });
    assert!(refusals(&change).is_empty() && server.list_bytes() < listed, "a member can still change");
    let mut out = Vec::new();
    for i in 0..MAX_MEMBERS as u64 - 1 {
        server.frame(10 + i, frame(LEAVE, Bytes::new()), now, UNIX_MS, &mut out);
    }
    server.frame(1, frame(LEAVE, Bytes::new()), now, UNIX_MS, &mut out);
    assert_eq!((server.members(), server.list_bytes()), (0, 0));
}

#[test]
fn sessions_that_never_register_are_capped() {
    let (mut server, now) = warm();
    for session in 0..70 {
        server.open(session, format!("o{session}"), 1, now, &mut Vec::new());
    }
    let mut out = Vec::new();
    server.tick(now + secs(9.9), &mut out);
    assert!(closes(&out).is_empty(), "they may still be about to register");
    server.tick(now + secs(10.0), &mut out);
    assert_eq!(closes(&out), (0..70 - MAX_OBSERVERS as u64).collect::<Vec<_>>());
}

#[test]
fn two_entries_that_reach_one_seed_count_once_and_a_named_seed_must_be_that_seed() {
    let entries = ["a:1".to_owned(), "b:1".into(), "c@c:1".into()];
    let mut node = Node::new("n".into(), 1, &entries, SeedTimings::default()).unwrap();
    let synced = frame(SYNCED, put_timings(&SeedTimings::default()));
    for entry in ["a:1", "b:1"] {
        node.opened(entry, "seed-a".into(), T0, &mut Vec::new());
        node.frame(entry, synced.clone(), T0, UNIX_MS, &mut Vec::new());
    }
    assert!(!node.live() && node.duplicates() == 1);

    let mut out = Vec::new();
    node.opened("c@c:1", "seed-a".into(), T0, &mut out);
    assert_eq!(out, [NodeOut::Close("c@c:1".into())]);
    node.opened("c@c:1", "c".into(), T0, &mut Vec::new());
    node.frame("c@c:1", synced, T0, UNIX_MS, &mut Vec::new());
    assert!(node.live() && node.duplicates() == 1);
}

#[test]
fn a_node_drops_a_boot_from_the_future_and_an_older_put() {
    let (mut server, mut now) = warm();
    let inc = (UNIX_MS as u128) << 64;
    let (mut node, out) = registered(&mut server, 1, &record("me", inc, 1), &mut now);
    feed(&mut node, out);
    let (mut peer, first) = registered(&mut server, 2, &record("peer", inc, 0), &mut now);
    let (mut sent, mut second) = (Vec::new(), Vec::new());
    peer.set_record(Some(NodeRecord { metadata: [].into(), ..record("peer", inc, 0) }), now, &mut sent);
    let Some(NodeOut::Send(_, frame)) = sent.pop() else { panic!() };
    server.frame(2, frame, now + secs(1.0), UNIX_MS, &mut second);
    feed(&mut node, second);
    feed(&mut node, first);
    // The seed's clock allows what this node's does not.
    let (mut far, _) = warm();
    let future = (UNIX_MS as u128 + 300_000) << 64;
    let mut out = Vec::new();
    far.open(3, "peer".into(), future, now, &mut out);
    let mut sent = Vec::new();
    let mut peer = Node::new("peer".into(), future, &["seed:1".into()], SeedTimings::default()).unwrap();
    peer.opened("seed:1", "seed".into(), T0, &mut sent);
    peer.set_record(Some(record("peer", future, 9)), T0, &mut sent);
    let Some(NodeOut::Send(_, frame)) = sent.pop() else { panic!() };
    far.frame(3, frame, now, UNIX_MS + 300_000, &mut out);
    feed(&mut node, out);
    let (_, view) = node.view().unwrap();
    let seen = view.iter().find(|r| r.id == "peer").unwrap();
    assert_eq!((seen.incarnation, seen.version, seen.metadata.len()), (inc, 2, 0));
}

#[test]
fn a_node_that_a_seed_drops_registers_again_and_an_invalid_record_is_not_taken() {
    let (mut server, mut now) = warm();
    let inc = (UNIX_MS as u128) << 64;
    let (mut node, out) = registered(&mut server, 1, &record("me", inc, 0), &mut now);
    feed(&mut node, out);
    assert!(node.live());

    // The seed took another session of this boot for the record's, and that one ended.
    let (_, _) = registered(&mut server, 2, &record("me", inc, 0), &mut now);
    server.closed(2, false, now);
    let mut removed = Vec::new();
    server.tick(now + secs(2.0), &mut removed);
    assert_eq!(server.members(), 0);
    let mut sent = Vec::new();
    for o in removed {
        let Out::All(frame) = o else { panic!("{o:?}") };
        node.frame("seed:1", through_wire(&frame), now, UNIX_MS, &mut sent);
    }
    assert!(!node.live() && matches!(&sent[..], [NodeOut::Send(_, f)] if f.kind == REGISTER));

    let bad = NodeRecord { address: "10.0.0.1:0".parse().unwrap(), ..record("bad", inc, 1) };
    let put = [&[1][..], &register_frame(&bad).payload, &[1], &register_frame(&record("ok", inc, 1)).payload].concat();
    let mut out = Vec::new();
    node.frame("seed:1", frame(CHANGES, put.into()), now, UNIX_MS, &mut out);
    assert!(out.is_empty(), "the session is kept");
    let Some(NodeOut::Send(_, register)) = sent.pop() else { panic!("{sent:?}") };
    let mut out = Vec::new();
    server.frame(1, register, now + secs(3.0), UNIX_MS, &mut out);
    feed(&mut node, out);
    let ids: Vec<_> = node.view().unwrap().1.iter().map(|r| r.id.clone()).collect();
    assert_eq!(ids, ["me", "ok"]);
}

#[test]
fn a_batch_keeps_the_last_put_of_a_record_and_every_remove() {
    let (mut server, mut now) = warm();
    let (new, old) = ((UNIX_MS as u128) << 64, (UNIX_MS as u128 - 1) << 64);
    let (mut watcher, out) = registered(&mut server, 1, &record("w", new, 1), &mut now);
    feed(&mut watcher, out);
    let (_, out) = registered(&mut server, 2, &record("x", new, 1), &mut now);
    feed(&mut watcher, out);

    // Inside one batching window: two more versions, a leave, and an older boot of the same id.
    let mut out = Vec::new();
    for version in [2, 3] {
        server.frame(2, register_frame(&record("x", new, version)), now, UNIX_MS, &mut out);
    }
    server.frame(2, frame(LEAVE, Bytes::new()), now, UNIX_MS, &mut out);
    server.open(3, "x".into(), old, now, &mut Vec::new());
    server.frame(3, register_frame(&record("x", old, 1)), now, UNIX_MS, &mut out);
    assert!(out.is_empty(), "{out:?}");
    server.tick(now + secs(0.02), &mut out);
    let [Out::All(batch)] = &out[..] else { panic!("{out:?}") };
    let one_put = register_frame(&record("x", old, 1)).payload.len();
    assert!(through_wire(batch).payload.len() < one_put + 64, "the puts that were overtaken are still there");

    feed(&mut watcher, std::mem::take(&mut out));
    watcher.frame("seed:1", frame(SYNCED, put_timings(&SeedTimings::default())), T0, UNIX_MS, &mut Vec::new());
    let view = watcher.view().unwrap().1;
    assert_eq!(view.iter().map(|r| (r.id.as_str(), r.incarnation)).collect::<Vec<_>>(), [("w", new), ("x", old)]);

    // Another version of the protocol is not guessed at, the one before above all.
    server.frame(3, Frame { kind: LEAVE, schema: VERSION - 1, ..Frame::default() }, now, UNIX_MS, &mut out);
    assert_eq!(closes(&out), [3]);
}

/// What a seed tells its nodes once warm. When this fails you changed what seed servers and nodes exchange: bump
/// `VERSION`.
#[test]
fn synced_bytes_are_pinned() {
    let mut out = Vec::new();
    let mut server = Server::new(T0, wan());
    server.open(1, "n".into(), 1, T0, &mut out);
    server.tick(wan().warm_up, &mut out);
    let [Out::To(1, synced)] = &out[..] else { panic!("{out:?}") };
    assert_eq!((synced.kind, synced.schema), (4, 2));
    let pinned = [8000u32, 7000, 4000, 8000].map(u32::to_be_bytes).concat();
    assert_eq!(synced.payload, pinned);
}

/// A seed that runs other timings is not counted, nor told the record: the node leaves it, keeps its list out of the
/// view, and says which timings it runs, once until they change. A `SYNCED` that is not four timings, or a second one,
/// ends the session.
#[test]
fn a_node_does_not_count_a_seed_with_other_timings() {
    let entries = ["a:1".to_owned(), "b:1".into(), "c:1".into()];
    let mut node = Node::new("n".into(), 1, &entries, SeedTimings::default()).unwrap();
    node.set_record(Some(record("n", 1, 0)), T0, &mut Vec::new());
    let ours = frame(SYNCED, put_timings(&SeedTimings::default()));
    let odd = SeedTimings { idle_hold: secs(3.0), ..SeedTimings::default() };
    let mut out = Vec::new();
    for (entry, peer) in [("a:1", "a"), ("b:1", "b"), ("c:1", "c")] {
        node.opened(entry, peer.into(), T0, &mut out);
    }
    assert_eq!(out.iter().filter(|o| matches!(o, NodeOut::Send(_, f) if f.kind == REGISTER)).count(), 3);
    let mut out = Vec::new();
    node.frame("a:1", ours.clone(), T0, UNIX_MS, &mut out);
    node.frame("c:1", frame(SYNCED, put_timings(&odd)), T0, UNIX_MS, &mut out);
    assert_eq!(out, [NodeOut::Timings("c:1".into(), odd), NodeOut::Send("c:1".into(), frame(LEAVE, Bytes::new()))]);
    assert!(node.odd().eq([("c:1", odd)]));
    assert!(node.states().any(|s| s == ("c:1", Some("c"), "timings")));
    assert!(!node.live() && node.view().is_none(), "one seed of three that agree");
    // Each lists this node, and the seed with other timings one more that no view shows.
    let put = |r: &NodeRecord| frame(CHANGES, [&[1][..], &register_frame(r).payload].concat().into());
    for entry in ["a:1", "b:1", "c:1"] {
        node.frame(entry, put(&record("n", 1, 1)), T0, UNIX_MS, &mut Vec::new());
    }
    node.frame("c:1", put(&record("x", 1, 1)), T0, UNIX_MS, &mut Vec::new());
    node.frame("b:1", ours.clone(), T0, UNIX_MS, &mut Vec::new());
    assert!(node.live());
    assert_eq!(node.view().unwrap().1.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(), ["n"]);

    // Not told again, nor sent the record, in this session or the next while its timings stay others.
    let mut out = Vec::new();
    node.tick(secs(10.0), &mut out);
    assert!(out.is_empty(), "{out:?}");
    node.lost("c:1", T0, &mut Vec::new());
    assert!(node.odd().eq([("c:1", odd)]));
    let mut out = Vec::new();
    node.opened("c:1", "c".into(), T0, &mut out);
    node.frame("c:1", put(&record("n", 1, 1)), T0, UNIX_MS, &mut out);
    node.frame("c:1", frame(SYNCED, put_timings(&odd)), T0, UNIX_MS, &mut out);
    assert!(out.is_empty(), "{out:?}");
    // A second `SYNCED` in a session ends it.
    node.frame("c:1", ours.clone(), T0, UNIX_MS, &mut out);
    assert_eq!(out, [NodeOut::Close("c:1".into())]);
    // Restarted with this node's timings, it counts again, with the list it sent before its `SYNCED`.
    node.lost("c:1", T0, &mut Vec::new());
    let mut out = Vec::new();
    node.opened("c:1", "c".into(), T0, &mut out);
    node.frame("c:1", put(&record("n", 1, 1)), T0, UNIX_MS, &mut out);
    node.frame("c:1", ours.clone(), T0, UNIX_MS, &mut out);
    assert!(node.odd().next().is_none() && node.synced("c:1"));
    assert!(out.iter().any(|o| matches!(o, NodeOut::Send(e, f) if e == "c:1" && f.kind == REGISTER)), "{out:?}");
    assert!(node.listed(1));

    for entry in ["a:1", "b:1"] {
        node.lost(entry, T0, &mut Vec::new());
        node.opened(entry, entry[..1].into(), T0, &mut Vec::new());
    }
    let mut out = Vec::new();
    node.frame("a:1", frame(SYNCED, Bytes::new()), T0, UNIX_MS, &mut out);
    node.frame("b:1", frame(SYNCED, put_timings(&odd).slice(..12)), T0, UNIX_MS, &mut out);
    assert_eq!(out, [NodeOut::Close("a:1".into()), NodeOut::Close("b:1".into())]);
    // A seed of version 1 is closed, not read as one with other timings.
    node.lost("a:1", T0, &mut Vec::new());
    node.opened("a:1", "a".into(), T0, &mut Vec::new());
    let mut out = Vec::new();
    node.frame("a:1", Frame { schema: VERSION - 1, ..frame(SYNCED, Bytes::new()) }, T0, UNIX_MS, &mut out);
    assert_eq!(out, [NodeOut::Close("a:1".into())]);
    assert!(node.odd().all(|(entry, _)| entry != "a:1"));
}
