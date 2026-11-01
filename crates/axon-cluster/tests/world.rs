//! Keyed accounts on PaxosLeases through a crash, a pause and a one-way cut, in the World (spec/simulation.md).
mod accounts;

use std::time::Duration;

use accounts::{Node, accounts, check, keys, moved, owned_by, send, settle, stream};
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
