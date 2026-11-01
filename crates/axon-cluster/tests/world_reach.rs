//! Keyed accounts on five nodes, the first three the acceptors, when reachability is not transitive: two nodes that
//! cannot reach each other while every caller reaches both, and a placement owner cut from a majority of acceptors
//! while callers reach a node that can take the lease, and a key started past its owner going back only once the owner
//! can take it. The rig's chaos run 2026-10-01 s1001 (episodes 37, 46, 79, 100, 117, 118) left keys unserved for the
//! whole of such a cut.
mod accounts;

use std::time::Duration;

use accounts::{
    Ledger, Node, five, fresh_ops, lists, no_stale_write, one_node_per_epoch, order, owner, starts_and_moves, told,
};
use axon_testkit::world::{Crash, Task, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};

/// The rig's smoke lease, so that a cut of ten leases is 20 s of virtual time.
const LEASE: Duration = Duration::from_secs(2);
const CUT: Duration = Duration::from_secs(20);

/// A key whose owner is `first` with every node up, and `second` without `first`.
fn key_placed(w: &mut World<Node>, first: &str, second: &str) -> String {
    (0..)
        .map(|k| format!("acct-{k}"))
        .find(|key| {
            let order = order(w, "a", key);
            order.len() == 5 && order[0] == first && order[1] == second
        })
        .expect("some key")
}

/// Five nodes past the acceptors' restart wait, and a key placed on d, then e, started on d.
fn setup(seed: u64) -> (World<Node>, Ledger, String) {
    let (mut w, ledger) = five(WorldConfig { seed, limit: Duration::from_secs(90), ..WorldConfig::default() }, LEASE);
    w.run_for(Duration::from_secs(3));
    let key = key_placed(&mut w, "d", "e");
    let first = owner(&mut w, "a", &key);
    assert!(first.as_ref().is_some_and(|(on, _)| on == "d"), "seed {seed}: {key} started on {first:?}");
    (w, ledger, key)
}

type Calls = Vec<(&'static str, Task<(Duration, Duration, u64, String)>)>;

/// Fresh ops on `key` from each of `froms` for the cut, each caller with ids of its own.
fn callers(w: &mut World<Node>, froms: &[&'static str], key: &str, since: Duration) -> Calls {
    (1..).zip(froms).map(|(i, &from)| (from, fresh_ops(w, from, key, CUT, since, i << 40))).collect()
}

/// Every caller's latest op waited at most `bound`; no write from an older epoch landed after a newer one's, and no
/// epoch started on two nodes.
/// Prints the digest, which `scripts/check.sh sim` compares across two processes.
fn judge(
    w: &mut World<Node>,
    ledger: &Ledger,
    seed: u64,
    key: &str,
    calls: Calls,
    bound: (Duration, &str),
    test: &str,
) {
    let (bound, what) = bound;
    let results: Vec<_> = calls.into_iter().map(|(from, task)| (from, w.join(task).unwrap())).collect();
    let mut late = Vec::new();
    for (from, (worst, sent, answered, error)) in results {
        println!("seed {seed}: from {from}: worst {worst:?} {what} for the op sent at {sent:?}, {answered} answered");
        if worst > bound {
            late.push(format!("from {from} the op sent at {sent:?} waited {worst:?}, {answered} answered, {error}"));
        }
    }
    let told: Vec<_> = ["d", "e"]
        .into_iter()
        .flat_map(|n| told(w, n).into_iter().filter(|e| e.1.ends_with(key)).map(move |e| format!("{n}: {e:?}")))
        .collect();
    assert!(late.is_empty(), "seed {seed}: on {key}, {what}: {late:#?}\n{told:#?}\n{}", w.trace_tail(40));
    no_stale_write(w, ledger, seed);
    one_node_per_epoch(w, ledger, seed);
    println!("world digest {test} seed {seed}: {:016x}", w.digest());
}

/// Callers on a, b and c, after d restarted cut from e, both ways: the key's lease is e's, taken while d was down, and
/// every caller's view places it on d again. Every caller reaches e, which runs the key, so each op is answered within
/// two leases of d's restart, the bound `every_op_answers_within_two_leases_of_a_paused_owners_resume` holds a restart
/// to, however long the cut lasts.
#[test]
fn callers_that_reach_the_holder_are_served_while_the_placement_owner_is_cut_from_it() {
    check_seeds(seeds(0..16), |seed| {
        let (mut w, ledger, key) = setup(seed);
        w.crash("d", Crash::Process);
        let mut moved = None;
        for _ in 0..40 {
            w.run_for(Duration::from_millis(500));
            moved = owner(&mut w, "a", &key).filter(|(on, _)| on == "e");
            if moved.is_some() {
                break;
            }
        }
        assert!(moved.is_some(), "seed {seed}: {key} did not move to e\n{}", w.trace_tail(40));
        w.partition(&["d"], &["e"]);
        w.restart("d");
        let calls = callers(&mut w, &["a", "b", "c"], &key, Duration::ZERO);
        w.run_until("d back in a's view", Duration::from_secs(10), |w| lists(w, "a", "d", "account"));
        let back = w.now();
        w.log(format!("d back at {back:?}"));
        w.run_for(CUT.saturating_sub(Duration::from_secs(1)));
        w.heal();
        judge(&mut w, &ledger, seed, &key, calls, (LEASE * 2, "of d's restart"), "reach_holder");
    })
    .unwrap();
}

/// d, the key's placement owner and its holder, is cut from acceptors b and c, both ways, and reaches only a. Callers
/// on a and e reach d and every acceptor; callers on b and c reach e, a and each other. d cannot renew or take the
/// lease without b or c, so the key must run elsewhere: within three leases of the cut, as the spec's cluster test
/// holds a crashed holder's key to, since the lease runs out a lease after d's last renewal, and a node must notice
/// and acquire.
#[test]
fn callers_are_served_while_the_placement_owner_reaches_only_a_minority_of_acceptors() {
    check_seeds(seeds(0..16), |seed| {
        let (mut w, ledger, key) = setup(seed);
        w.partition(&["d"], &["b", "c"]);
        let calls = callers(&mut w, &["a", "b", "c", "e"], &key, LEASE * 3);
        w.run_for(CUT);
        w.heal();
        // A few tries of one op past the three leases, each answered in milliseconds once the key runs.
        judge(&mut w, &ledger, seed, &key, calls, (LEASE / 4, "past three leases of the cut"), "reach_minority");
    })
    .unwrap();
}

/// The cut of the test above, with callers on b and c only. e starts the key past d, which cannot take its lease, and
/// keeps it for the whole cut: moved to d while d reaches no majority, it would stop there and start again on e at
/// each next call. Once the cut heals, d says it would take the key, and it moves back.
#[test]
fn a_key_started_past_its_cut_off_owner_stays_until_the_owner_can_take_it_then_moves_back() {
    check_seeds(seeds(0..16), |seed| {
        let (mut w, ledger, key) = setup(seed);
        w.partition(&["d"], &["b", "c"]);
        let calls = callers(&mut w, &["b", "c"], &key, LEASE * 3);
        w.run_for(CUT);
        let (started, moved) = starts_and_moves(&w, "e");
        assert_eq!((started, moved), (1, 0), "seed {seed}: e started {key} {started} times, moved it {moved}");
        w.heal();
        // `operation_timeout` (5 s) apart, e asks d again, and a move takes a drain and a hand-off.
        w.run_until("the key moves back to d", Duration::from_secs(8), |w| {
            starts_and_moves(w, "e").1 == 1 && starts_and_moves(w, "d").0 == 2
        });
        let back = owner(&mut w, "a", &key);
        assert!(back.as_ref().is_some_and(|(on, _)| on == "d"), "seed {seed}: {key} runs on {back:?}");
        judge(&mut w, &ledger, seed, &key, calls, (LEASE / 4, "past three leases of the cut"), "reach_back");
    })
    .unwrap();
}
