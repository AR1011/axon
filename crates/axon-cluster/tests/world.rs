//! Keyed accounts on PaxosLeases through a crash, a pause and a one-way cut, in the World (spec/simulation.md).
//! Also beside a member that comes and goes.
mod accounts;

use std::time::Duration;

use accounts::{
    NODES, Node, accounts, check, keys, leave, member, moved, owned_by, owner, send, settle, starts_and_moves, stream,
};
use axon_testkit::world::{Crash, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};

fn owners_crash_pause_and_lose_a_link(seed: u64) -> World<Node> {
    // Jitter reorders what crosses different connections.
    let wire = Wire { jitter: Duration::from_millis(5), ..Wire::default() };
    let (mut w, ledger) = accounts(WorldConfig { seed, wire, ..WorldConfig::default() });
    let keys = keys();
    let (mut id, mut calls) = (0u64, Vec::new());

    // 1. b dies while deposits to its keys are on the wire.
    let on_b = owned_by(&mut w, &keys, "b");
    send(&mut w, &on_b, "a", &mut id, &mut calls);
    let wait = Duration::from_millis(w.pick(1..6));
    w.run_for(wait);
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    w.crash("b", how);
    w.run_for(Duration::from_secs(10)); // past the lease: a and c are a majority and take b's keys
    // b returns while a client deposits and a's handlers are stuck: what a took moves back to b only once it has
    // ended on a, queue and all.
    let back = stream(&mut w, "c", Duration::from_secs(8), 1 << 33);
    w.run_for(Duration::from_secs(1));
    let hold = Duration::from_millis(w.pick(3000..6000));
    w.hold_actors("a", hold);
    w.restart("b");
    w.run_for(Duration::from_secs(9));

    // 2. c stops like SIGSTOP while a client's deposits to its keys fill its buffers, a and b take its keys and write
    //    with newer epochs, then c resumes. Its monotonic clock ran through the pause, so its fence must be closed.
    //    Its leases run out 6 s after its last renewal: it may resume before twice its grace.
    let on_c = owned_by(&mut w, &keys, "c");
    let pause = Duration::from_millis(w.pick(7000..14000));
    let client = stream(&mut w, "a", pause + Duration::from_secs(4), 1 << 32);
    w.run_for(Duration::from_secs(1));
    w.pause("c", pause);
    w.run_for(pause + Duration::from_secs(3));

    // 3. a stops hearing c for less than a lease; c still hears a.
    let on_c_now = owned_by(&mut w, &keys, "c");
    send(&mut w, &on_c_now, "b", &mut id, &mut calls);
    w.cut(&["c"], &["a"]);
    let cut = Duration::from_millis(w.pick(500..4000));
    w.run_for(cut);
    w.heal();

    settle(&mut w, calls);
    w.join(back).unwrap();
    w.join(client).unwrap();
    w.run_for(Duration::from_secs(2));
    moved(&w, &ledger, &on_c, seed, "the pause");
    check(&mut w, &ledger, "owners_crash_pause_and_lose_a_link", seed);
    w
}

/// A member that registers no kind, as `ops` (spec/operations.md §2), joins each second for 10 s and leaves a second
/// later while a client calls every key: each join and leave clears the route caches and rehashes the hosted keys,
/// and moves none.
fn a_member_comes_and_goes(seed: u64) {
    let (mut w, _) = accounts(WorldConfig { seed, ..WorldConfig::default() });
    let owners = |w: &mut World<Node>| keys().iter().map(|key| owner(w, "a", key)).collect::<Vec<_>>();
    let moves = |w: &World<Node>| NODES.iter().map(|node| starts_and_moves(w, node).1).sum::<u64>();
    // The leases warm first: every key runs before the member comes.
    let mut before = owners(&mut w);
    for _ in 0..30 {
        if before.iter().all(Option::is_some) {
            break;
        }
        w.run_for(Duration::from_secs(1));
        before = owners(&mut w);
    }
    assert!(before.iter().all(Option::is_some), "seed {seed}: {before:?}");
    let moved_before = moves(&w);
    let client = stream(&mut w, "a", Duration::from_secs(11), 1);
    // One joins each second, and leaves a second later: its shutdown takes that second, beside the next one's join.
    let listed =
        |w: &World<Node>, name: &str| w.nodes().any(|(at, n)| at == "a" && n.cluster.view().get(name).is_some());
    let churn = |w: &World<Node>| {
        let stats = w.nodes().find(|(at, _)| *at == "a").map(|(_, n)| n.cluster.stats()).expect("a is up");
        (stats.left, stats.became_unreachable)
    };
    let churn_before = churn(&w);
    let mut leaving = Vec::new();
    for i in 0..10 {
        let (name, start) = (format!("ops-{i}"), w.now());
        member(&mut w, &name);
        w.run_until("a lists the member", Duration::from_millis(900), |w| listed(w, &name));
        w.run_for(Duration::from_secs(1) - (w.now() - start));
        leaving.push((leave(&mut w, &name), name));
    }
    for (left, name) in leaving {
        w.join(left).unwrap();
        w.run_until("a lists the member no more", Duration::from_millis(100), |w| !listed(w, &name));
    }
    // Each one left: none went first through unreachable, as one that stops without a shutdown does.
    let (left, unreachable) = churn(&w);
    assert_eq!((left - churn_before.0, unreachable - churn_before.1), (10, 0), "seed {seed}");
    let failed = w.join(client).unwrap();
    w.run_for(Duration::from_secs(2));
    let counts = (moves(&w) - moved_before, failed);
    assert_eq!(counts, (0, 0), "seed {seed}: keys moved, calls failed\n{}", w.trace_tail(40));
    assert_eq!(owners(&mut w), before, "seed {seed}");
}

#[test]
fn a_member_that_joins_and_leaves_each_second_moves_no_key_and_drops_no_call() {
    check_seeds(seeds(0..8), a_member_comes_and_goes).unwrap();
}

#[test]
fn owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners() {
    check_seeds(seeds(0..64), |seed| drop(owners_crash_pause_and_lose_a_link(seed))).unwrap();
}

#[test]
#[cfg_attr(not(tokio_unstable), ignore = "reproducible only in the sim build: scripts/check.sh sim")]
fn the_same_seed_gives_the_same_trace() {
    check_seeds(seeds(0..2), |seed| {
        let (one, two) = (owners_crash_pause_and_lose_a_link(seed), owners_crash_pause_and_lose_a_link(seed));
        if let Some(report) = one.divergence(&two) {
            panic!("{report}");
        }
    })
    .unwrap();
}
