//! Paxos leases in the World: a holder whose clock drifts, one starved of CPU, an acceptor whose wall clock steps back,
//! owners paused past their leases, an owner that restarts inside its own lease, a grant that arrives after its start
//! gave up, a holder suspended past its lease, and reads of a restarted acceptor's warm-up from the test's thread.
//! Keyed accounts write to a ledger fenced by the lease epoch, and a client keeps depositing throughout; tickers write
//! it on their own.
mod accounts;

use std::time::Duration;

use accounts::{
    NODES, Node, accounts, check, keys, leased, moved, no_stale_write, ops, owned_by, placed_on, start_ticker, stream,
    tickers,
};
use axon_testkit::world::{Crash, Wire, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};

#[test]
fn holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger) = accounts(WorldConfig { seed, ..WorldConfig::default() });
        w.drift("b", 5000);
        w.drift("c", -5000);
        let deposits = stream(&mut w, "a", Duration::from_secs(40), 0);
        w.run_for(Duration::from_secs(2));
        for holder in ["b", "c"] {
            // Nothing the holder sends arrives: its renewals stop, and its keys move once their leases run out on the
            // acceptors' clocks, while the holder counts its grace on its own. It also stops like SIGSTOP and resumes
            // on either side of the lease's end with deposits in its buffers, so only its fence, on its drifting
            // clock, keeps it from writing them after the next owner.
            let held = owned_by(&mut w, &keys(), holder);
            let others: Vec<_> = NODES.into_iter().filter(|n| *n != holder).collect();
            w.cut(&[holder], &others);
            let pause = Duration::from_millis(w.pick(4000..7000));
            w.pause(holder, pause);
            let cut = Duration::from_millis(w.pick(9000..12000));
            w.run_for(cut);
            w.heal();
            w.run_for(Duration::from_secs(5));
            moved(&w, &ledger, &held, seed, "a one-way cut");
        }
        w.join(deposits).unwrap();
        check(&mut w, &ledger, "drift", seed);
    })
    .unwrap();
}

#[test]
fn a_holder_starved_past_its_lease_stops_its_keys_before_another_nodes_epoch_writes() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger) = accounts(WorldConfig { seed, ..WorldConfig::default() });
        let deposits = stream(&mut w, "a", Duration::from_secs(35), 0);
        w.run_for(Duration::from_secs(2));
        let held = owned_by(&mut w, &keys(), "c");
        // Stepped once in 7 to 12 s: each time, what arrived meanwhile and every overdue timer run together.
        let one_in = w.pick(7000..12000) as u32;
        w.starve("c", one_in);
        w.run_for(Duration::from_secs(20));
        w.starve("c", 1);
        w.run_for(Duration::from_secs(8));
        moved(&w, &ledger, &held, seed, "starving its holder");
        w.join(deposits).unwrap();
        check(&mut w, &ledger, "starved", seed);
    })
    .unwrap();
}

#[test]
fn a_wall_step_back_on_an_acceptor_does_not_let_an_older_epoch_win() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger) = accounts(WorldConfig { seed, ..WorldConfig::default() });
        let deposits = stream(&mut w, "c", Duration::from_secs(35), 0);
        w.run_for(Duration::from_secs(2));
        let held = owned_by(&mut w, &keys(), "b");
        // A minute is more than an acceptor's restart wait: b's floor and its own epochs start below what it granted.
        w.step_wall("b", -60_000);
        w.crash("b", Crash::Process);
        w.run_for(Duration::from_secs(10));
        moved(&w, &ledger, &held, seed, "b's crash");
        w.restart("b");
        // b's keys come back once it has waited out its restart.
        w.run_for(Duration::from_secs(15));
        let wall = w.read("b", |_| axon::trace::unix_micros()).unwrap();
        let back = owned_by(&mut w, &keys(), "b");
        assert!(!back.is_empty(), "seed {seed}: no key came back to b\n{}", w.trace_tail(40));
        for (key, epoch) in &back {
            // Refused below the promises a and c made, b asked again above them, not from its clock.
            assert!(
                epoch.is_some_and(|e| e > wall),
                "seed {seed}: {key} came back to b at {epoch:?}, its clock {wall}"
            );
        }
        w.join(deposits).unwrap();
        check(&mut w, &ledger, "wall_step", seed);
    })
    .unwrap();
}

#[test]
fn every_op_answers_within_two_leases_of_a_paused_owners_resume() {
    check_seeds(seeds(0..64), |seed| {
        // The rig's smoke lease: a pause of a few leases is a few seconds.
        let lease = Duration::from_secs(2);
        let (mut w, ledger) = leased(WorldConfig { seed, ..WorldConfig::default() }, lease);
        // Pauses past the lease, as the rig's episodes: the victim's keys start on the others for their callers, its
        // actors are fenced as it resumes, and the keys go back to it. A node that took keys may be the next victim.
        let (mut pauses, mut at) = (Vec::new(), Duration::from_secs(2));
        for _ in 0..3 {
            let victim = NODES[w.pick(0..3) as usize];
            let d = Duration::from_millis(w.pick(3000..7000));
            pauses.push((victim, at, d));
            at += d + Duration::from_millis(w.pick(8000..14000));
        }
        let windows: Vec<_> = pauses.iter().map(|(_, on, d)| (*on, *on + *d)).collect();
        let calls: Vec<_> = (NODES.into_iter().enumerate())
            .map(|(i, from)| ops(&mut w, from, at, windows.clone(), (i as u64 + 1) << 40))
            .collect();
        let start = w.now();
        for (victim, on, d) in &pauses {
            w.run_for(start + *on - w.now());
            w.pause(victim, *d);
        }
        w.run_for(start + at - w.now());
        for (from, calls) in NODES.into_iter().zip(calls) {
            let (late, key, sent) = w.join(calls).unwrap();
            // Each caller asks the key's owner once it is back, and the node that took the key hands it back meanwhile.
            assert!(
                late <= lease * 2,
                "seed {seed}: pauses {pauses:?}: an op from {from} on {key} sent at {sent:?} ended {late:?} late\n{}",
                w.trace_tail(60)
            );
        }
        check(&mut w, &ledger, "paused_owners", seed);
    })
    .unwrap();
}

#[test]
fn a_grant_that_arrives_after_its_start_gave_up_leaves_nothing_held() {
    check_seeds(seeds(0..64), |seed| {
        // A round trip on b's slow links below fits an ask's patience (lease / 6), and a prepare and a propose outlast
        // a start (3 s).
        let lease = Duration::from_secs(15);
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, lease);
        // Past the acceptors' restart wait.
        w.run_for(Duration::from_secs(18).saturating_sub(w.now()));
        let key = (0..).map(|k| format!("o{k}")).find(|k| placed_on(&mut w, "b", "eager", k).as_deref() == Some("b"));
        let key = key.expect("some key is b's");
        let slow = Wire { latency: Duration::from_secs(1), ..Wire::default() };
        for to in ["a", "c"] {
            w.wire("b", to, slow);
        }
        let start = w.now();
        assert_eq!(start_ticker(&mut w, "b", "eager", &key), None, "seed {seed}: the start outran the slow links");
        // The grant arrives after the start gave up.
        w.run_for(start + Duration::from_secs(6) - w.now());
        for to in ["a", "c"] {
            w.wire("b", to, Wire::default());
        }
        // Inside the hold that grant would leave, nothing renewing it: the acceptors let it go a lease after they
        // accepted it, about 18 s from `start`, whatever the fence of a start answered from it says.
        w.run_for(start + Duration::from_secs(15) - w.now());
        let again = start_ticker(&mut w, "b", "eager", &key);
        assert!(again.as_ref().is_some_and(|(on, _)| on == "b"), "seed {seed}: {key} started on {again:?}");
        // b sleeps past the end of that lease, while c or a restarts the key, and wakes inside the fence.
        w.pause("b", Duration::from_secs(10));
        w.run_for(Duration::from_secs(16));
        no_stale_write(&w, &ledger, seed);
        println!("world digest late_grant seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}

#[test]
fn a_key_started_elsewhere_while_its_owner_restarted_goes_back_to_it() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger) = accounts(WorldConfig { seed, ..WorldConfig::default() });
        let deposits =
            [stream(&mut w, "a", Duration::from_secs(30), 0), stream(&mut w, "c", Duration::from_secs(30), 1 << 32)];
        w.run_for(Duration::from_secs(2));
        // Callers that see b gone ask a or c for its keys, and those wait out b's lease. b is back before the lease
        // ends, so a start that began without b ends with b in the view, where every caller now looks for the key.
        w.crash("b", Crash::Process);
        let down = Duration::from_millis(w.pick(1000..5000));
        w.run_for(down);
        w.restart("b");
        w.run_for(Duration::from_secs(20));
        for d in deposits {
            w.join(d).unwrap();
        }
        check(&mut w, &ledger, "owner_restart", seed);
    })
    .unwrap();
}

#[test]
fn a_restarted_acceptor_warms_up_on_its_own_clock_whichever_thread_reads_it() {
    check_seeds(seeds(0..8), |seed| {
        let (mut w, _) = accounts(WorldConfig { seed, ..WorldConfig::default() });
        w.crash("b", Crash::Process);
        w.run_for(Duration::from_secs(1));
        w.restart("b");
        // Read from the test's thread, as a `run_until` predicate does, never inside b's runtime.
        let b = |w: &World<Node>| w.nodes().find(|(name, _)| *name == "b").map(|(_, n)| n.clone());
        let warming = |n: &Node| (n.leases.warming(), n.cluster.warming(), n.cluster.stats().warming);
        w.run_until("b booted", Duration::from_secs(10), |w| b(w).is_some());
        let node = b(&w).expect("booted");
        assert_eq!(warming(&node), (true, true, true), "seed {seed}: b does not warm up after its restart");
        let seed_server = node.seed.as_ref().expect("every node is a seed");
        let left = seed_server.stats().warm_up_left;
        w.run_for(Duration::from_secs(2));
        let later = seed_server.stats().warm_up_left;
        let two = Duration::from_secs(2);
        assert_eq!(left - later, two.min(left), "seed {seed}: the seed's warm-up left {left:?}, 2 s later {later:?}");
        // A little over one lease (6 s) after its start.
        w.run_until("b warmed up", Duration::from_secs(10), |_| warming(&node) == (false, false, false));
    })
    .unwrap();
}

#[test]
fn a_holder_suspended_past_its_lease_wakes_and_writes_and_only_the_epoch_check_refuses_it() {
    check_seeds(seeds(0..16), |seed| {
        let (mut w, ledger) = tickers(WorldConfig { seed, ..WorldConfig::default() }, Duration::from_secs(6));
        // Past the acceptors' restart wait.
        w.run_for(Duration::from_secs(8).saturating_sub(w.now()));
        let key = (0..).map(|k| format!("o{k}")).find(|k| placed_on(&mut w, "a", "eager", k).as_deref() == Some("b"));
        let key = key.expect("some key is b's");
        let (on, old) = start_ticker(&mut w, "a", "eager", &key).expect("the ticker starts");
        assert_eq!(on, "b", "seed {seed}");
        let (name, old) = (format!("eager/{key}"), old.expect("leased"));
        // Past the lease and the time an eager key takes to start elsewhere. b's clocks stand still meanwhile, so it
        // wakes inside its fence, its ticker still running at the old epoch.
        let d = Duration::from_millis(w.pick(12_000..16_000));
        w.suspend("b", d);
        w.run_for(d);
        let now = ledger.epoch(&name);
        assert!(now > Some(old), "seed {seed}: {name} did not start elsewhere\n{}", w.trace_tail(40));
        w.run_for(Duration::from_secs(1));
        let refused = ledger.refused(&name);
        assert!(
            refused.iter().any(|(epoch, _)| *epoch == old),
            "seed {seed}: b wrote nothing at {old} after its wake, refused {refused:?}\n{}",
            w.trace_tail(40)
        );
        println!("world digest suspended seed {seed}: {:016x}", w.digest());
    })
    .unwrap();
}
