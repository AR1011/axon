//! An eager kind in the World: the node holding its keys crashes, is partitioned off, pauses or restarts, and every key
//! it held runs again with nobody calling it, on a survivor or on itself once back, while the keys of a lazy kind wait
//! for a caller; a leave cut short by a crash loses none, a key fenced on a node that stays a member starts again, and
//! so does a key handed back to a node whose start then fails, and one lost just before its node leaves. A holder no
//! node saw go has its keys run again by a node that asks which leases lapsed, a question the authority does not answer
//! ends, and each restart of a key is recorded once, by the node whose request started it, whatever listed it.
//! Tickers write a ledger fenced by the lease epoch every 100 ms, so two holders of one key show as a stale write.
mod accounts;

use std::collections::BTreeMap;
use std::time::Duration;

use accounts::{
    Ledger, NODES, Node, five_tickers, hosted_on, hosts, leave, lists, no_stale_write, running, start_ticker, tickers,
    tickers_in_memory, told,
};
use axon_testkit::world::{Crash, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};

/// Starts twelve keys of `kind` from a, and names each `kind/key` with its node and epoch. A start is refused until
/// the seeds have warmed up.
fn start(w: &mut World<Node>, kind: &str) -> Vec<(String, String, Option<u64>)> {
    start_from(w, "a", kind)
}

/// As `start`, from `from`.
fn start_from(w: &mut World<Node>, from: &str, kind: &str) -> Vec<(String, String, Option<u64>)> {
    let (mut started, until) = (Vec::new(), w.now() + Duration::from_secs(30));
    for key in (0..12).map(|k| format!("t{k}")) {
        let (on, epoch) = loop {
            match start_ticker(w, from, kind, &key) {
                Some(placed) => break placed,
                None if w.now() < until => w.run_for(Duration::from_millis(500)),
                None => panic!("{kind}/{key} never started\n{}", w.trace_tail(40)),
            }
        };
        started.push((format!("{kind}/{key}"), on, epoch));
    }
    started
}

type Started = [(String, String, Option<u64>)];

fn on(keys: &Started, node: &str) -> Vec<(String, Option<u64>)> {
    keys.iter().filter(|(_, on, _)| on == node).map(|(key, _, epoch)| (key.clone(), *epoch)).collect()
}

/// The node that started the most of `keys`, and the others.
fn busiest(keys: &Started) -> (&'static str, Vec<&'static str>) {
    let node = NODES.into_iter().max_by_key(|n| on(keys, n).len()).expect("three nodes");
    (node, NODES.into_iter().filter(|n| *n != node).collect())
}

/// Whether each of `keys` runs under a newer epoch than the one it names.
fn newer(ledger: &Ledger, keys: &[(String, Option<u64>)]) -> bool {
    keys.iter().all(|(key, epoch)| ledger.epoch(key) > *epoch)
}

/// The epoch an event's detail ends with.
fn epoch_of(detail: &str) -> Option<u64> {
    detail.rsplit_once("epoch ").and_then(|(_, e)| e.parse().ok())
}

/// The `eager_restart` events `node` recorded, by key and epoch, each with what it was for.
fn restarts(w: &mut World<Node>, node: &str) -> BTreeMap<(String, Option<u64>), Vec<String>> {
    let mut restarts: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (_, key, detail) in told(w, node).into_iter().filter(|(kind, ..)| *kind == "eager_restart") {
        restarts.entry((key, epoch_of(&detail))).or_default().push(detail);
    }
    restarts
}

/// No two `eager_restart`s of one key and epoch, on one node or on two: each key waits for its restart once per node,
/// whatever listed it, and only the node whose request started the actor records it.
fn restarted_once(w: &mut World<Node>, seed: u64) {
    let nodes: Vec<String> = w.nodes().map(|(name, _)| name.to_owned()).collect();
    let mut by_restart: BTreeMap<_, Vec<String>> = BTreeMap::new();
    for node in nodes {
        for (restart, details) in restarts(w, &node) {
            by_restart.entry(restart).or_default().extend(details.into_iter().map(|d| format!("{node}: {d}")));
        }
    }
    let twice: Vec<_> = by_restart.into_iter().filter(|(_, details)| details.len() > 1).collect();
    assert!(twice.is_empty(), "seed {seed}: restarted {twice:?}\n{}", w.trace_tail(40));
}

/// `node` is up and its view has `other` in it, reachable or not.
fn in_view(w: &World<Node>, node: &str, other: &str) -> bool {
    w.read(node, |n| n.cluster.view().get(other).is_some()).unwrap_or(false)
}

/// The keys `node` waits to start again for an eager kind, and its questions about gone nodes not answered yet: none
/// while it is down.
fn eager_now(w: &World<Node>, node: &str) -> (usize, usize) {
    w.read(node, |n| n.cluster.stats()).map_or((0, 0), |stats| (stats.eager_waiting, stats.eager_asking))
}

/// `PaxosConfig::new`'s lease, or a shorter one.
fn lease_for(seed: u64) -> Duration {
    Duration::from_secs(if (seed / 2).is_multiple_of(2) { 6 } else { 2 })
}

#[test]
fn a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller() {
    check_seeds(seeds(0..64), |seed| {
        // `PaxosConfig::new`'s, or one shorter than it takes to see a silent node gone.
        let lease = Duration::from_secs(if seed / 5 % 2 == 0 { 6 } else { 2 });
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        let (eager, lazy) = (start(&mut w, "eager"), start(&mut w, "lazy"));
        w.run_for(Duration::from_secs(1));
        let (victim, others) = busiest(&eager);
        let (eager, lazy) = (on(&eager, victim), on(&lazy, victim));

        let (fault, lasts) = match seed % 5 {
            0 | 1 => {
                let how = if seed % 5 == 0 { Crash::Process } else { Crash::Machine };
                w.crash(victim, how);
                (format!("{how:?}"), Duration::ZERO)
            }
            4 => {
                w.crash(victim, Crash::Process);
                w.restart(victim);
                ("restart".into(), Duration::ZERO)
            }
            2 => {
                w.partition(&[victim], &others);
                ("partition".into(), Duration::from_millis(w.pick(9000..12000)))
            }
            _ => {
                let pause = Duration::from_millis(w.pick(7000..14000));
                w.pause(victim, pause);
                ("pause".into(), pause)
            }
        };
        w.log(format!("{fault} of {victim}, holding {eager:?}"));
        let from = w.now();
        // The acceptors' lease on each key ends within a lease of the victim's last renewal; the survivors see it
        // gone within the transport's idle timeout (4 s), ask after `rebalance_settle`, and keep asking until then. A
        // restarted victim boots and joins first.
        let within = lease + Duration::from_secs(6) + if seed % 5 == 4 { lease } else { Duration::ZERO };
        w.run_until("every eager key the victim held runs again", within, |_| newer(&ledger, &eager));
        w.run_for((from + lasts).saturating_sub(w.now()));
        w.heal();
        w.run_for(Duration::from_secs(5));

        for (key, epoch) in &lazy {
            assert_eq!(ledger.epoch(key), *epoch, "seed {seed}: {fault} restarted lazy {key}\n{}", w.trace_tail(40));
        }
        no_stale_write(&w, &ledger, seed);
        // Whoever restarted a key says so on its timeline, with the epoch it started under.
        let told: Vec<_> = NODES.into_iter().flat_map(|n| told(&mut w, n)).collect();
        for (key, epoch) in &eager {
            let restarted = |(kind, subject, detail): &(&str, String, String)| {
                let under = detail.rsplit_once("epoch ").and_then(|(_, e)| e.parse::<u64>().ok());
                *kind == "eager_restart" && subject == key && under > *epoch
            };
            assert!(told.iter().any(restarted), "seed {seed}: no eager_restart of {key}: {told:?}");
        }
        restarted_once(&mut w, seed);
        println!("world digest eager seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn a_leave_cut_short_by_a_crash_loses_no_eager_key() {
    check_seeds(seeds(0..64), |seed| {
        // The shorter lease is shorter than it takes to see a silent node gone.
        let lease = lease_for(seed);
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        let eager = start(&mut w, "eager");
        w.run_for(Duration::from_secs(1));
        let (victim, _) = busiest(&eager);
        let eager = on(&eager, victim);
        let _leaving = leave(&mut w, victim);
        // Anywhere in the leave, which takes about 0.3 s here: its actors stop within 10 ms, and its keys are handed to
        // their new owners after.
        let cut = Duration::from_millis(w.pick(0..320));
        w.run_for(cut);
        w.crash(victim, Crash::Process);
        let back = seed % 2 == 1;
        if back {
            w.restart(victim);
        }
        w.log(format!("{victim} crashed {cut:?} into its leave, holding {eager:?}, back at once: {back}"));
        // A key handed off runs at once; one still held waits for its lease to lapse and the survivors to see the
        // victim gone, as in the test above.
        let within = lease + Duration::from_secs(6) + if back { lease } else { Duration::ZERO };
        w.run_until("every eager key the victim held runs again", within, |_| newer(&ledger, &eager));
        running(&mut w, &ledger, &eager, seed);
        no_stale_write(&w, &ledger, seed);
        println!("world digest eager-leave seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn an_eager_key_fenced_on_a_node_that_stays_a_member_starts_again() {
    fenced_on_a_member("eager-fenced", false);
}

#[test]
fn an_eager_key_fenced_by_its_own_turn_on_a_node_that_stays_a_member_starts_again() {
    fenced_on_a_member("eager-turn-fenced", true);
}

/// A majority of the acceptors is down for longer than a lease: the node they leave loses its leases, or stops its
/// keys as isolated (a process crash resets its sessions to the others at once), and the acceptors come back having
/// forgotten every lease, so none lists the keys as anyone's. With `turn_cost`, a turn there reads the actors' clock up
/// to its cost ahead of the tasks' clock: a ticker finds its fence lapsed and ends before the renew loop's timer for
/// its lease fires.
fn fenced_on_a_member(name: &str, turn_cost: bool) {
    check_seeds(seeds(0..64), |seed| {
        let lease = lease_for(seed);
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        let eager = start(&mut w, "eager");
        w.run_for(Duration::from_secs(1));
        let (kept, others) = busiest(&eager);
        let eager = on(&eager, kept);
        let cost = turn_cost.then(|| Duration::from_millis(w.pick(20..80)));
        if let Some(cost) = cost {
            w.cost(kept, cost);
        }
        let how = if seed % 2 == 0 { Crash::Process } else { Crash::Machine };
        others.iter().for_each(|other| w.crash(other, how));
        let down = lease + Duration::from_millis(w.pick(0..lease.as_millis() as u64));
        let turns = cost.map_or_else(String::new, |cost| format!(", turns there cost {cost:?}"));
        w.log(format!("{how:?} of the acceptors but {kept} for {down:?}{turns}, holding {eager:?}"));
        w.run_for(down);
        if cost.is_some() {
            w.cost(kept, Duration::ZERO);
        }
        others.iter().for_each(|other| w.restart(other));
        // The restarted acceptors warm up for a lease, and the starts retry until then.
        w.run_until("every eager key the kept node held runs again", lease * 2 + Duration::from_secs(10), |_| {
            newer(&ledger, &eager)
        });
        running(&mut w, &ledger, &eager, seed);
        no_stale_write(&w, &ledger, seed);
        println!("world digest {name} seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn an_eager_key_handed_back_to_a_node_whose_start_then_fails_starts_again() {
    check_seeds(seeds(0..64), |seed| {
        let lease = lease_for(seed);
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        let eager = start(&mut w, "eager");
        w.run_for(Duration::from_secs(1));
        let (victim, others) = busiest(&eager);
        let eager = on(&eager, victim);
        w.crash(victim, if seed % 2 == 0 { Crash::Process } else { Crash::Machine });
        w.run_until("the survivors restart every eager key the victim held", lease + Duration::from_secs(10), |_| {
            newer(&ledger, &eager)
        });
        let moved: Vec<_> = eager.iter().map(|(key, _)| (key.clone(), ledger.epoch(key))).collect();
        w.restart(victim);
        // The survivors hand the keys back once they list the victim again. Its acquire then hears from no other
        // acceptor within its patience (a sixth of the lease), while its answer to the hand-off arrives in time.
        w.run_until("a survivor lists the victim again", Duration::from_secs(30), |w| {
            others.iter().any(|other| lists(w, other, victim, "eager"))
        });
        let slow = Wire { latency: Duration::from_secs(2), ..Wire::default() };
        others.iter().for_each(|other| w.wire(victim, other, slow));
        w.log(format!("{victim} back, its acquires slow, the survivors holding {moved:?}"));
        w.run_for(Duration::from_secs(8));
        others.iter().for_each(|other| w.wire(victim, other, Wire::default()));
        // Back on the victim, their owner: a survivor may start a key again meanwhile, and hand it on later.
        w.run_until("every eager key the victim held runs there again", lease + Duration::from_secs(20), |w| {
            moved.iter().all(|(key, epoch)| ledger.epoch(key) > *epoch && hosts(w, victim, key, ledger.epoch(key)))
        });
        running(&mut w, &ledger, &eager, seed);
        no_stale_write(&w, &ledger, seed);
        println!("world digest eager-handback seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn an_eager_key_lost_just_before_its_node_leaves_runs_on_a_survivor() {
    check_seeds(seeds(0..64), |seed| {
        // The restarted acceptors warm up for a lease: long enough that the others rejoin first, and that the leave
        // ends before then.
        let lease = Duration::from_secs(10);
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        let eager = start(&mut w, "eager");
        w.run_for(Duration::from_secs(1));
        let (kept, others) = busiest(&eager);
        let eager = on(&eager, kept);
        // As in `fenced_on_a_member`: `kept` loses its leases or stops its keys as isolated, and cannot start them
        // again until the restarted acceptors are warm. It leaves before that, once it lists them again.
        let how = if seed % 2 == 0 { Crash::Process } else { Crash::Machine };
        others.iter().for_each(|other| w.crash(other, how));
        let down = lease + Duration::from_millis(w.pick(0..lease.as_millis() as u64));
        w.run_for(down);
        others.iter().for_each(|other| w.restart(other));
        w.run_until("the kept node lists the others again", Duration::from_secs(30), |w| {
            others.iter().all(|other| lists(w, kept, other, "eager"))
        });
        w.log(format!("{kept} leaves after {how:?} of the acceptors for {down:?}, holding {eager:?}"));
        let _leaving = leave(&mut w, kept);
        w.run_until(
            "every eager key the kept node held runs on a survivor",
            lease * 2 + Duration::from_secs(10),
            |_| newer(&ledger, &eager),
        );
        running(&mut w, &ledger, &eager, seed);
        no_stale_write(&w, &ledger, seed);
        println!("world digest eager-lost-leave seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// Starts the eager keys in the world `make` builds with Paxos leases of `lease`, has `lose` crash and restart nodes,
/// then checks that every key it names runs again, with a newer epoch, on one node only, and keeps running.
fn run_again_after(
    seed: u64,
    name: &str,
    make: fn(WorldConfig, Duration) -> (World<Node>, Ledger),
    lease: Duration,
    lose: impl FnOnce(&mut World<Node>, &Started) -> Vec<(String, Option<u64>)>,
) {
    let (mut w, ledger) = make(WorldConfig { seed, ..WorldConfig::default() }, lease);
    // The acceptors wait out a lease at their first start too.
    w.run_for(lease);
    let started = start(&mut w, "eager");
    w.run_for(Duration::from_secs(1));
    let eager = lose(&mut w, &started);
    // The acceptors warm up for a lease, then each node restarts what it held.
    let within = lease * 2 + Duration::from_secs(10);
    let end = w.now() + within;
    while !newer(&ledger, &eager) && w.now() < end {
        w.run_for(Duration::from_millis(100));
    }
    if !newer(&ledger, &eager) {
        let down: Vec<_> =
            eager.iter().filter(|(key, epoch)| ledger.epoch(key) <= *epoch).map(|(key, _)| key).collect();
        let up: Vec<_> = NODES.into_iter().filter(|n| NODES.into_iter().all(|o| lists(&w, n, o, "eager"))).collect();
        let told: Vec<_> =
            NODES.into_iter().flat_map(|n| told(&mut w, n)).filter(|(k, ..)| k.starts_with("eager")).collect();
        panic!(
            "seed {seed}: {} of {} eager keys not running again {within:?} on: {down:?}; nodes listing all three: \
             {up:?}; eager events: {told:?}\n{}",
            down.len(),
            eager.len(),
            w.trace_tail(20)
        );
    }
    running(&mut w, &ledger, &eager, seed);
    for (key, _) in &eager {
        let on = hosted_on(&w, key);
        assert_eq!(on.len(), 1, "seed {seed}: {key} runs on {on:?}\n{}", w.trace_tail(40));
    }
    no_stale_write(&w, &ledger, seed);
    // A node's own keys and those whose lease lapsed are often the same: both lists place each once.
    restarted_once(&mut w, seed);
    w.run_until("no node waits for a key or asks about a node", Duration::from_secs(30), |w| {
        NODES.into_iter().all(|n| eager_now(w, n) == (0, 0))
    });
    println!("world digest {name} seed {seed}: {:016x}", w.digest());
}

fn all(started: &Started) -> Vec<(String, Option<u64>)> {
    started.iter().map(|(key, _, epoch)| (key.clone(), *epoch)).collect()
}

/// Every node is an acceptor here, so every acceptor restarting at once restarts every holder too: nobody keeps the
/// keys in memory, and the acceptors come back having forgotten every promise.
fn every_acceptor_at_once(w: &mut World<Node>, started: &Started, seed: u64) -> Vec<(String, Option<u64>)> {
    let how = if seed.is_multiple_of(2) { Crash::Process } else { Crash::Machine };
    NODES.into_iter().for_each(|n| w.crash(n, how));
    NODES.into_iter().for_each(|n| w.restart(n));
    w.log(format!("{how:?} of every acceptor at once, the keys {started:?}"));
    all(started)
}

#[test]
fn every_eager_key_runs_again_after_every_acceptor_restarts_at_once() {
    check_seeds(seeds(0..64), |seed| {
        run_again_after(seed, "eager-acceptors", tickers, lease_for(seed), |w, started| {
            every_acceptor_at_once(w, started, seed)
        });
    })
    .unwrap();
}

/// A lease longer than an eager restart keeps at a key: the restarted acceptors answer only once it has run out, and
/// the sweeps wait for that.
#[test]
fn every_eager_key_runs_again_after_every_acceptor_restarts_at_once_under_a_long_lease() {
    check_seeds(seeds(0..8), |seed| {
        // Past `EAGER_PATIENCE`, 60 s.
        let lease = Duration::from_secs(70);
        run_again_after(seed, "eager-acceptors-long", tickers, lease, |w, started| {
            every_acceptor_at_once(w, started, seed)
        });
    })
    .unwrap();
}

/// An acceptor's lease log, as its disk holds it, synced or not.
fn lease_log(w: &World<Node>, node: &str) -> Option<Vec<u8>> {
    w.disk(node).dir("leases").read("leases.log")
}

/// Every machine crashes, and once back each crashes again inside the rewrite its start makes: as soon as the file
/// differs from what the first crash left, before an append written in its place would be synced. A torn frame left at
/// the end of each log makes every rewrite differ from it. A rewrite is one `Replace`, kept whole, so every key runs
/// again.
fn inside_the_start_rewrite(w: &mut World<Node>, started: &Started, seed: u64) -> Vec<(String, Option<u64>)> {
    NODES.into_iter().for_each(|n| w.crash(n, Crash::Machine));
    for n in NODES {
        let leases = w.disk(n).dir("leases");
        leases.append("leases.log", &[0xff; 3]).expect("no capacity set");
        leases.sync("leases.log");
    }
    let mut left: Vec<_> = NODES.into_iter().map(|n| (n, lease_log(w, n))).collect();
    NODES.into_iter().for_each(|n| w.restart(n));
    let (until, mut caught) = (w.now() + Duration::from_secs(10), 0);
    while !left.is_empty() {
        w.run_for(Duration::from_millis(1));
        let late = w.now() >= until;
        let seen: Vec<_> = left.extract_if(.., |(n, log)| late || lease_log(w, n) != *log).collect();
        for (n, _) in seen {
            caught += usize::from(!late);
            w.crash(n, Crash::Machine);
            w.restart(n);
        }
    }
    assert_eq!(caught, NODES.len(), "seed {seed}: a rewrite not seen within 10 s\n{}", w.trace_tail(20));
    w.log(format!("every machine crashed inside its start's rewrite ({caught} seen), the keys {started:?}"));
    all(started)
}

#[test]
fn every_eager_key_runs_again_after_every_acceptor_crashes_inside_its_start_rewrite() {
    check_seeds(seeds(0..32), |seed| {
        run_again_after(seed, "eager-rewrite", tickers, lease_for(seed), |w, started| {
            inside_the_start_rewrite(w, started, seed)
        });
    })
    .unwrap();
}

/// Power lost everywhere: what was not synced is gone. The machines come back one by one, in any order, and on some
/// seeds the first a while before the others: longer than an eager restart keeps at a key.
fn cold_restart(w: &mut World<Node>, started: &Started, lease: Duration, seed: u64) -> Vec<(String, Option<u64>)> {
    NODES.into_iter().for_each(|n| w.crash(n, Crash::Machine));
    let down = Duration::from_millis(w.pick(0..lease.as_millis() as u64));
    w.run_for(down);
    let mut order: Vec<_> = NODES.into();
    for i in (1..order.len()).rev() {
        order.swap(i, w.pick(0..i as u64 + 1) as usize);
    }
    let mut alone = if seed % 4 == 3 { Duration::from_secs(70) } else { Duration::ZERO };
    w.log(format!(
        "cold restart after {down:?} in the order {order:?}, the first {alone:?} alone, the keys {started:?}"
    ));
    for n in order {
        w.restart(n);
        let gap = Duration::from_millis(w.pick(0..1500));
        w.run_for(gap + std::mem::take(&mut alone));
    }
    all(started)
}

#[test]
fn every_eager_key_runs_again_after_a_cold_restart() {
    check_seeds(seeds(0..64), |seed| {
        let lease = lease_for(seed);
        run_again_after(seed, "eager-cold", tickers, lease, |w, started| cold_restart(w, started, lease, seed));
    })
    .unwrap();
}

/// As above, the nodes finding each other in memory: the first one back, alone, does not wait for a view of the
/// others, and its sweeps outlast its 70 s alone.
#[test]
fn every_eager_key_runs_again_after_a_cold_restart_in_memory() {
    check_seeds(seeds(0..32), |seed| {
        let lease = lease_for(seed);
        run_again_after(seed, "eager-cold-memory", tickers_in_memory, lease, |w, started| {
            cold_restart(w, started, lease, seed)
        });
    })
    .unwrap();
}

#[test]
fn the_eager_keys_of_a_node_that_never_returns_from_a_cold_restart_run_on_the_others() {
    check_seeds(seeds(0..64), |seed| {
        run_again_after(seed, "eager-never-back", tickers, lease_for(seed), |w, started| {
            let (victim, _) = busiest(started);
            // The others boot without the victim, so no view of theirs ever loses it.
            let how = if seed % 2 == 0 { Crash::Process } else { Crash::Machine };
            NODES.into_iter().for_each(|n| w.crash(n, how));
            NODES.into_iter().filter(|n| *n != victim).for_each(|n| w.restart(n));
            w.log(format!("{how:?} of every node, all but {victim} back, which held {:?}", on(started, victim)));
            on(started, victim)
        });
    })
    .unwrap();
}

/// d holds every key, and no node of the eager kind sees it go: the acceptors run none, and e is down. e starts once d
/// has gone from every view, within a lease of d's end, so the leases d held still run when e first asks which leases
/// lapsed. e asks again until well past a lease after that, and runs d's keys once their leases lapse.
#[test]
fn an_unseen_holder_that_ends_after_a_node_starts_has_its_keys_run_again() {
    check_seeds(seeds(0..32), |seed| {
        // Long enough to outlast d leaving every view and e's start.
        let lease = Duration::from_secs(10);
        let (mut w, ledger) = five_tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        w.crash("e", Crash::Process);
        w.run_until("e has left d's view", Duration::from_secs(30), |w| !in_view(w, "d", "e"));
        let started = start_from(&mut w, "d", "eager");
        let held = on(&started, "d");
        assert_eq!(held.len(), started.len(), "seed {seed}: d holds every key: {started:?}");
        w.run_for(Duration::from_secs(1));
        w.crash("d", Crash::Process);
        let ended = w.now();
        w.run_until("d has left every view", Duration::from_secs(30), |w| {
            ["a", "b", "c"].iter().all(|n| !in_view(w, n, "d"))
        });
        w.restart("e");
        w.log(format!("e back {:?} after d crashed holding {held:?}", w.now() - ended));
        // Each lease lapses within a lease of d's end, and e asks again every 20 s.
        w.run_until("every key d held runs again", lease + Duration::from_secs(30), |_| newer(&ledger, &held));
        running(&mut w, &ledger, &held, seed);
        for (key, _) in &held {
            assert_eq!(hosted_on(&w, key), ["e"], "seed {seed}: {key}\n{}", w.trace_tail(40));
        }
        let restarts = restarts(&mut w, "e");
        for (key, epoch) in &held {
            let told: Vec<_> = restarts.iter().filter(|((k, e), _)| k == key && e > epoch).collect();
            let lapsed =
                matches!(&told[..], [(_, details)] if details.len() == 1 && details[0].contains("for a lapsed lease"));
            assert!(lapsed, "seed {seed}: {key} restarted {told:?}");
        }
        no_stale_write(&w, &ledger, seed);
        w.run_until("e waits for no key", Duration::from_secs(15), |w| eager_now(w, "e") == (0, 0));
        println!("world digest eager-unseen seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

/// e is cut off from every acceptor when d, which holds keys, crashes: nothing answers e's question about d, which
/// ends within half a minute while e is still cut off. Once e hears the acceptors again it asks which leases lapsed, and
/// runs d's keys.
#[test]
fn a_question_the_authority_does_not_answer_ends() {
    check_seeds(seeds(0..32), |seed| {
        let lease = lease_for(seed);
        let (mut w, ledger) = five_tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        let started = start_from(&mut w, "d", "eager");
        let held = on(&started, "d");
        w.run_for(Duration::from_secs(1));
        let acceptors = ["a", "b", "c"];
        w.cut(&["e"], &acceptors);
        w.cut(&acceptors, &["e"]);
        w.crash("d", Crash::Process);
        w.log(format!("e cut off from the acceptors, d crashed holding {held:?}"));
        w.run_until("e asks about d", Duration::from_secs(15), |w| eager_now(w, "e").1 > 0);
        // 30 s from the question, and a pass of the asker.
        w.run_until("e's questions end while it is cut off", Duration::from_secs(35), |w| eager_now(w, "e").1 == 0);
        w.heal();
        w.log("healed".to_string());
        w.run_until("every key d held runs again", lease + Duration::from_secs(15), |_| newer(&ledger, &held));
        w.run_until("e waits for no key", Duration::from_secs(15), |w| eager_now(w, "e") == (0, 0));
        running(&mut w, &ledger, &all(&started), seed);
        for (key, _, _) in &started {
            assert_eq!(hosted_on(&w, key), ["e"], "seed {seed}: {key}\n{}", w.trace_tail(40));
        }
        no_stale_write(&w, &ledger, seed);
        restarted_once(&mut w, seed);
        println!("world digest eager-unanswered seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}
