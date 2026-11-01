//! Every schedule, up to a budget of acts and faults, of two nodes and three replicas sharing one key of `M = 2`
//! permits (spec/quotas.md §7), on integer ticks of a second: messages lost, duplicated, or late (arriving after later
//! ones, or a tick later), a clock that gains or loses a tick, a node paused, and a replica restarted, which pulls from
//! the other two. The nodes take one or two permits or give one back, once a tick each. In every state reached, what
//! the nodes hold or could still take on their grants exceeds the limit by no more than the permits taken on the
//! fallback (§5: none, but `a` for a node back from a long pause or new to the key), and replicas that see the same
//! rows plan the same grants. Checking what could be taken, not only what is held, finds an over-grant without the
//! takes that would spend it, which keeps the budgets small.
//!
//! A message not made late arrives within its tick, so that the schedules differ where it matters: in what each side
//! knows when it acts. A replica takes a report in as it is sent; a node takes in the grants to it at any point, and
//! always before it acts. Grants on different links commute, and taking one in reads no clock.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use super::core::{Bound, Candidates, Entry, Grants, Limit, Link, Members, Replica, Report, fresh};
use crate::ownership::lease_hold;

const M: u64 = 2;
const LIMIT: Limit = Limit { name: "ws", max: M, window: Duration::ZERO };
const ID: u64 = 1;
const NODES: usize = 2;
const REPLICAS: usize = 3;
const QUORUM: usize = REPLICAS / 2 + 1;
/// A tick is a second. Over a lease of four, a clock gaining or losing one is a drift of a quarter.
const LEASE: Duration = Duration::from_secs(4);
const DRIFT: f64 = 0.25;
/// A late message is delivered within a tick of being sent, or lost.
const LATE: u64 = 1;
/// A node with nothing new reports this often: every other tick, not every tick as `core::HEARTBEAT` would, which
/// keeps the states few while silence still differs from a heartbeat.
const REPORT_TICKS: u64 = 2;
/// Every clock starts here, so that none reads zero.
const START: i64 = 10;

#[derive(Clone, Copy, Debug, Default, Hash)]
struct Budget {
    /// Takes and releases, over both nodes.
    acts: u8,
    lose: u8,
    duplicate: u8,
    late: u8,
    drift: u8,
    pause: u8,
    restart: u8,
}

/// What happens to the copy of a report to one replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Lose,
    Late,
}

#[derive(Clone, Hash)]
struct Node {
    links: [Link; REPLICAS],
    /// The newest incarnation of each replica it heard of: the view shows a restarted replica no earlier than its
    /// first answer, the latest that keeps the predecessor's grants.
    seen: [u128; REPLICAS],
    entry: Option<Entry>,
    held: u64,
    /// Of the permits held, those taken on the fallback: the most `held` may exceed the limit by.
    fallback: u64,
    reported: u64,
    acted: bool,
    paused: bool,
}

#[derive(Clone, Debug, Hash)]
enum Msg {
    /// Only a late one: the others are taken in as they are sent.
    Report {
        from: usize,
        to: usize,
        sent: u64,
        report: Report,
    },
    Grants {
        from: usize,
        to: usize,
        sent: u64,
        late: bool,
        grants: Grants,
    },
}

impl Msg {
    fn sent(&self) -> u64 {
        match self {
            Msg::Report { sent, .. } | Msg::Grants { sent, .. } => *sent,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Event {
    /// A node takes, then reports, as it does within `REPORT_EVERY`.
    Take(usize, u64, Option<(usize, Fault)>),
    Release(usize),
    Report(usize, Option<(usize, Fault)>),
    /// A node takes in the grants to it that are not late, before anything else happens.
    Receive(usize),
    /// A late message arrives.
    Deliver(usize),
    /// Grants that would have arrived before their node next acts are made late.
    Delay(usize),
    Lose(usize),
    Duplicate(usize),
    /// With one process's clock a tick fast or slow.
    Tick(Option<(usize, bool)>),
    Pause(usize),
    Resume(usize),
    Restart(usize),
    Pull(usize, usize),
}

#[derive(Clone)]
struct State {
    t: u64,
    /// Per process, nodes first: how far its clock is from the others.
    skew: [i64; NODES + REPLICAS],
    nodes: [Node; NODES],
    replicas: [Replica; REPLICAS],
    incarnations: [u128; REPLICAS],
    /// Per replica, as bits, the others it pulled from since it restarted.
    pulls: [u8; REPLICAS],
    msgs: Vec<Msg>,
    budget: Budget,
}

fn node_id(n: usize) -> Arc<str> {
    format!("n{n}").into()
}

impl State {
    fn new(budget: Budget) -> Self {
        let node = Node {
            links: Default::default(),
            seen: [0; REPLICAS],
            entry: None,
            held: 0,
            fallback: 0,
            reported: 0,
            acted: false,
            paused: false,
        };
        let replica = || {
            let mut r = Replica::new(1, LEASE, Duration::from_secs(START as u64));
            r.pulled();
            r
        };
        Self {
            t: 0,
            skew: [0; NODES + REPLICAS],
            nodes: [node.clone(), node],
            replicas: [replica(), replica(), replica()],
            incarnations: [1; REPLICAS],
            pulls: [0; REPLICAS],
            msgs: Vec::new(),
            budget,
        }
    }

    fn clock(&self, process: usize) -> Duration {
        Duration::from_secs((START + self.t as i64 + self.skew[process]) as u64)
    }

    fn fingerprint(&self) -> u64 {
        let mut h = DefaultHasher::new();
        (self.t, self.skew, &self.nodes, &self.replicas, self.incarnations, self.pulls, self.budget).hash(&mut h);
        let mut msgs: Vec<u64> = self
            .msgs
            .iter()
            .map(|m| {
                let mut h = DefaultHasher::new();
                m.hash(&mut h);
                h.finish()
            })
            .collect();
        msgs.sort_unstable();
        msgs.hash(&mut h);
        h.finish()
    }

    /// Whether node `n` owes a report before the next tick: usage or demand moved, or a heartbeat is due.
    fn dirty(&self, n: usize) -> bool {
        let node = &self.nodes[n];
        let due = self.t >= node.reported + REPORT_TICKS;
        node.entry.as_ref().is_some_and(|e| due || e.clone().report(ID, self.clock(n)).is_some())
    }

    /// The most node `n` could hold without another message: its permits, or all its grant allows. Checking this
    /// rather than what is held finds an over-grant without the takes that would spend it.
    fn reach(&self, n: usize) -> u64 {
        let node = &self.nodes[n];
        match node.entry.as_ref().map(|e| e.bound(&fresh(&node.links), self.clock(n), NODES, QUORUM)) {
            Some(Bound::Grant { value, .. }) => node.held.max(value),
            _ => node.held,
        }
    }

    /// The faults a report may meet, none first.
    fn faults(&self) -> Vec<Option<(usize, Fault)>> {
        let mut faults = vec![None];
        for (fault, left) in [(Fault::Lose, self.budget.lose), (Fault::Late, self.budget.late)] {
            if left > 0 {
                faults.extend((0..REPLICAS).map(|r| Some((r, fault))));
            }
        }
        faults
    }

    fn events(&self, ticks: u64) -> Vec<Event> {
        let mut events = Vec::new();
        let faults = self.faults();
        for (n, node) in self.nodes.iter().enumerate() {
            if node.paused {
                events.push(Event::Resume(n));
                continue;
            }
            if !node.acted && self.budget.acts > 0 {
                for cost in [1, 2] {
                    events.extend(faults.iter().map(|f| Event::Take(n, cost, *f)));
                }
                if node.held > 0 {
                    events.push(Event::Release(n));
                }
            }
            if self.dirty(n) {
                events.extend(faults.iter().map(|f| Event::Report(n, *f)));
            }
            if self.msgs.iter().any(|m| matches!(m, Msg::Grants { to, late: false, .. } if *to == n)) {
                events.push(Event::Receive(n));
            }
            if self.budget.pause > 0 {
                events.push(Event::Pause(n));
            }
        }
        for (i, msg) in self.msgs.iter().enumerate() {
            match msg {
                Msg::Report { .. } => events.push(Event::Deliver(i)),
                Msg::Grants { to, late: true, .. } if !self.nodes[*to].paused => events.push(Event::Deliver(i)),
                Msg::Grants { late: false, .. } if self.budget.late > 0 => events.push(Event::Delay(i)),
                Msg::Grants { .. } => {}
            }
            if self.budget.lose > 0 {
                events.push(Event::Lose(i));
            }
            if self.budget.duplicate > 0 {
                events.push(Event::Duplicate(i));
            }
        }
        let owed = (0..NODES).any(|n| !self.nodes[n].paused && self.dirty(n));
        let late = self.msgs.iter().any(|m| matches!(m, Msg::Report { .. }) && m.sent() + LATE <= self.t);
        if self.t < ticks && !owed && !late {
            events.push(Event::Tick(None));
            if self.budget.drift > 0 {
                events.extend(
                    (0..NODES + REPLICAS).flat_map(|p| [Event::Tick(Some((p, true))), Event::Tick(Some((p, false)))]),
                );
            }
        }
        for r in 0..REPLICAS {
            if self.budget.restart > 0 {
                events.push(Event::Restart(r));
            }
            if self.replicas[r].young(self.clock(NODES + r)) {
                let todo = |j: &usize| *j != r && self.pulls[r] >> j & 1 == 0;
                events.extend((0..REPLICAS).filter(todo).map(|j| Event::Pull(r, j)));
            }
        }
        events
    }

    /// A node's report to every replica, each taking it in as it is sent but for the copy `fault` names.
    fn report(&mut self, n: usize, fault: Option<(usize, Fault)>) {
        let (now, t) = (self.clock(n), self.t);
        let node = &mut self.nodes[n];
        node.reported = t;
        let items: Vec<_> = node.entry.as_mut().and_then(|e| e.report(ID, now)).into_iter().collect();
        let mut reports = Vec::new();
        for link in &mut node.links {
            let (seq, replica, acked) = link.send(now);
            reports.push(Report { incarnation: 1, seq, replica, acked, items: items.clone() });
        }
        for (to, report) in reports.into_iter().enumerate() {
            match fault {
                Some((r, Fault::Lose)) if r == to => self.budget.lose -= 1,
                Some((r, Fault::Late)) if r == to => {
                    self.budget.late -= 1;
                    self.msgs.push(Msg::Report { from: n, to, sent: t, report });
                }
                _ => self.take_in(n, to, &report),
            }
        }
    }

    fn take_in(&mut self, from: usize, to: usize, report: &Report) {
        let now = self.clock(NODES + to);
        if let Some(grants) = self.replicas[to].report(&node_id(from), report, now) {
            self.msgs.push(Msg::Grants { from: to, to: from, sent: self.t, late: false, grants });
        }
    }

    /// Takes the grants `pick` names out of flight, in the order sent, into their nodes.
    fn receive(&mut self, pick: impl Fn(&Msg) -> bool, hold: Duration) {
        let (picked, kept) = std::mem::take(&mut self.msgs).into_iter().partition::<Vec<_>, _>(pick);
        self.msgs = kept;
        for msg in picked {
            self.deliver(msg, hold);
        }
    }

    fn deliver(&mut self, msg: Msg, hold: Duration) {
        match msg {
            Msg::Report { from, to, report, .. } => self.take_in(from, to, &report),
            Msg::Grants { from, to, grants, .. } => {
                let now = self.clock(to);
                let node = &mut self.nodes[to];
                if grants.incarnation > node.seen[from] {
                    node.seen[from] = grants.incarnation;
                    node.links[from].retarget(grants.incarnation);
                }
                let link = &mut node.links[from];
                if let Some(epoch) = link.accept(&grants, true, now) {
                    for &(id, value, used) in &grants.entries {
                        if let (Some(entry), ID) = (node.entry.as_mut(), id) {
                            entry.set_grant(from, epoch, value, used);
                        }
                    }
                    link.commit(&grants, true, hold, now);
                }
            }
        }
    }

    /// Node `n` takes in the grants to it that are not late.
    fn arrive(&mut self, n: usize, hold: Duration) {
        self.receive(|m| matches!(m, Msg::Grants { to, late: false, .. } if *to == n), hold);
    }

    fn apply(&mut self, event: Event, hold: Duration) {
        match event {
            Event::Take(n, cost, fault) => {
                self.arrive(n, hold);
                self.budget.acts -= 1;
                let now = self.clock(n);
                let node = &mut self.nodes[n];
                node.acted = true;
                let entry = node.entry.get_or_insert_with(|| Entry::new(&LIMIT, now, false));
                let bound = entry.bound(&fresh(&node.links), now, NODES, QUORUM);
                if let Ok(fell) = entry.admit(cost, bound, NODES, now) {
                    entry.add(cost, now);
                    node.held += cost;
                    if fell {
                        node.fallback += cost;
                    }
                }
                self.report(n, fault);
            }
            Event::Release(n) => {
                self.arrive(n, hold);
                self.budget.acts -= 1;
                let now = self.clock(n);
                let node = &mut self.nodes[n];
                node.acted = true;
                node.entry.as_mut().expect("held").release(1, now);
                node.held -= 1;
                node.fallback = node.fallback.min(node.held);
            }
            Event::Report(n, fault) => {
                self.arrive(n, hold);
                self.report(n, fault);
            }
            Event::Receive(n) => self.arrive(n, hold),
            Event::Deliver(i) => {
                let msg = self.msgs.remove(i);
                self.deliver(msg, hold);
            }
            Event::Delay(i) => {
                self.budget.late -= 1;
                if let Msg::Grants { late, .. } = &mut self.msgs[i] {
                    *late = true;
                }
            }
            Event::Lose(i) => {
                self.budget.lose -= 1;
                self.msgs.remove(i);
            }
            Event::Duplicate(i) => {
                self.budget.duplicate -= 1;
                self.msgs.push(self.msgs[i].clone());
            }
            Event::Tick(drift) => {
                let (t, paused): (_, [bool; NODES]) = (self.t, std::array::from_fn(|n| self.nodes[n].paused));
                let due = |m: &Msg| matches!(m, Msg::Grants { to, sent, late, .. } if !paused[*to] && (!late || sent + LATE <= t));
                self.receive(due, hold);
                self.t += 1;
                if let Some((p, fast)) = drift {
                    self.budget.drift -= 1;
                    self.skew[p] += if fast { 1 } else { -1 };
                }
                for node in &mut self.nodes {
                    node.acted = false;
                }
                for r in 0..REPLICAS {
                    let now = self.clock(NODES + r);
                    self.replicas[r].tick(now);
                }
            }
            Event::Pause(n) => {
                self.budget.pause -= 1;
                self.nodes[n].paused = true;
            }
            Event::Resume(n) => self.nodes[n].paused = false,
            Event::Restart(r) => {
                self.budget.restart -= 1;
                self.incarnations[r] += 1;
                self.replicas[r] = Replica::new(self.incarnations[r], LEASE, self.clock(NODES + r));
                self.pulls[r] = 0;
            }
            Event::Pull(r, j) => {
                let (rows, _) = self.replicas[j].pull(0, usize::MAX, |_| true, self.clock(NODES + j));
                let now = self.clock(NODES + r);
                self.replicas[r].absorb(
                    rows,
                    &Members::new(0, Candidates::PER_REGION, [("n0", 1, ""), ("n1", 1, "")]),
                    now,
                );
                // From every other replica: a restarted one and one that missed a report are a majority that
                // knows nothing of a grant the lost incarnation gave.
                self.pulls[r] |= 1 << j;
                if self.pulls[r].count_ones() as usize == REPLICAS - 1 {
                    self.replicas[r].pulled();
                }
            }
        }
    }

    fn check(&self) -> Result<(), String> {
        let reach: Vec<u64> = (0..NODES).map(|n| self.reach(n)).collect();
        let fallback: u64 = self.nodes.iter().map(|n| n.fallback).sum();
        if reach.iter().sum::<u64>() > M + fallback {
            return Err(format!("the nodes could hold {reach:?}, {fallback} on the fallback, over a limit of {M}"));
        }
        let plans: Vec<_> = (0..REPLICAS)
            .filter(|r| !self.replicas[*r].young(self.clock(NODES + r)))
            .map(|r| (r, self.replicas[r].plan(ID, self.clock(NODES + r))))
            .collect();
        for (i, (r, a)) in plans.iter().enumerate() {
            for (j, b) in &plans[i + 1..] {
                let same = a.iter().map(|(peer, ask, _)| (peer, ask)).eq(b.iter().map(|(peer, ask, _)| (peer, ask)));
                if same && a != b {
                    return Err(format!("replicas {r} and {j} see the same rows and plan {a:?} and {b:?}"));
                }
            }
        }
        Ok(())
    }

    fn describe(&self) -> String {
        let nodes: Vec<_> = self.nodes.iter().map(|n| (n.held, n.fallback, n.paused)).collect();
        format!(
            "t {} skew {:?} nodes (held, fallback, paused) {nodes:?} in flight {}",
            self.t,
            self.skew,
            self.msgs.len()
        )
    }
}

/// Visits every state reachable within `ticks`, checking each; the number visited.
fn explore(budget: Budget, ticks: u64) -> usize {
    let hold = lease_hold(LEASE, DRIFT);
    let start = State::new(budget);
    let mut seen: HashMap<u64, Option<(u64, Event)>> = HashMap::from([(start.fingerprint(), None)]);
    let mut stack = vec![start.clone()];
    while let Some(state) = stack.pop() {
        let parent = state.fingerprint();
        for event in state.events(ticks) {
            let mut next = state.clone();
            next.apply(event, hold);
            let fingerprint = next.fingerprint();
            if seen.contains_key(&fingerprint) {
                continue;
            }
            seen.insert(fingerprint, Some((parent, event)));
            if let Err(why) = next.check() {
                panic!("{why}\n{}", trace(&start, &seen, fingerprint, hold));
            }
            stack.push(next);
        }
    }
    seen.len()
}

/// The events that led to a state, replayed from the start with what each did.
fn trace(start: &State, seen: &HashMap<u64, Option<(u64, Event)>>, mut at: u64, hold: Duration) -> String {
    let mut events = Vec::new();
    while let Some(Some((parent, event))) = seen.get(&at) {
        events.push(*event);
        at = *parent;
    }
    let mut state = start.clone();
    let mut out = String::new();
    for event in events.into_iter().rev() {
        let what = match event {
            Event::Deliver(i) | Event::Delay(i) | Event::Lose(i) | Event::Duplicate(i) => {
                format!(" {:?}", state.msgs[i])
            }
            _ => String::new(),
        };
        state.apply(event, hold);
        out += &format!("{event:?}{what}\n  -> {}\n", state.describe());
    }
    out
}

fn explore_all(budgets: &[(Budget, u64)]) {
    for (budget, ticks) in budgets {
        let states = explore(*budget, *ticks);
        println!("{budget:?} over {ticks} ticks: {states} states");
    }
}

#[test]
fn without_faults_the_nodes_hold_no_more_than_the_limit() {
    explore_all(&[(Budget { acts: 3, ..Budget::default() }, 4)]);
}

#[test]
fn lost_and_duplicated_messages_admit_nothing_over_the_limit() {
    explore_all(&[
        (Budget { acts: 3, lose: 1, ..Budget::default() }, 3),
        (Budget { acts: 2, duplicate: 1, ..Budget::default() }, 3),
    ]);
}

#[test]
fn late_messages_admit_nothing_over_the_limit() {
    explore_all(&[(Budget { acts: 2, late: 1, ..Budget::default() }, 3)]);
}

#[test]
fn a_drifting_clock_admits_nothing_over_the_limit() {
    explore_all(&[
        (Budget { acts: 2, drift: 1, ..Budget::default() }, 4),
        (Budget { acts: 2, drift: 1, pause: 1, ..Budget::default() }, 3),
    ]);
}

#[test]
fn a_paused_node_admits_only_the_fallback_over_the_limit() {
    explore_all(&[(Budget { acts: 2, pause: 1, ..Budget::default() }, 6)]);
}

#[test]
fn a_restarted_replica_admits_nothing_over_the_limit_while_it_pulls() {
    explore_all(&[
        (Budget { acts: 2, restart: 1, ..Budget::default() }, 2),
        (Budget { acts: 2, restart: 1, lose: 2, ..Budget::default() }, 0),
    ]);
}

/// Plays `events` from the start, checking every state, with a budget that allows them all: schedules longer than the
/// enumeration reaches.
fn replay(events: &[Event]) -> State {
    let hold = lease_hold(LEASE, DRIFT);
    let mut state =
        State::new(Budget { acts: 99, lose: 99, duplicate: 99, late: 99, drift: 99, pause: 99, restart: 99 });
    for (i, event) in events.iter().enumerate() {
        state.apply(*event, hold);
        if let Err(why) = state.check() {
            panic!("after {:?}: {why}", &events[..=i]);
        }
    }
    state
}

#[test]
fn an_acknowledgement_of_a_restarted_replicas_predecessor_frees_nothing_it_pulled() {
    use Event::*;
    // Node 0 never reaches replica 2, node 1 never reaches replica 1; replica 0 restarts and pulls from both.
    let (off0, off1) = (Some((2, Fault::Lose)), Some((1, Fault::Lose)));
    replay(&[
        Take(0, 2, off0),
        Receive(0),
        Restart(0),
        Pull(0, 1),
        Pull(0, 2),
        Take(1, 2, off1),
        Receive(1),
        Take(0, 1, off0),
        Take(1, 2, off1),
        Receive(1),
    ]);
}

#[test]
fn after_a_handover_the_only_node_on_a_key_is_granted_all_of_it() {
    use Event::*;
    let s = replay(&[
        Take(0, 1, None),
        Receive(0),
        Restart(0),
        Take(0, 1, None),
        Release(0),
        Report(0, None),
        Receive(0),
        Pull(0, 1),
        Pull(0, 2),
        Report(0, None),
        Receive(0),
    ]);
    let node = &s.nodes[0];
    let bound = node.entry.as_ref().map(|e| e.bound(&fresh(&node.links), s.clock(0), NODES, QUORUM));
    assert!(matches!(bound, Some(Bound::Grant { value: M, .. })), "{bound:?}");
}
