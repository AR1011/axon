use std::time::Duration;

use axon_testkit::*;

const MS: Duration = Duration::from_millis(1);

fn hostile(seed: u64) -> SimConfig {
    SimConfig { seed, jitter: 20 * MS, drop_every: 4, duplicate_every: 5, ordered: false, ..Default::default() }
}

#[derive(Clone, Debug)]
enum Packet {
    Data(u32),
    Ack(u32),
}

/// Stop-and-wait with retransmission: the receiver must see every number once and in order
/// while the network loses, duplicates and reorders packets and is cut in half for a while.
fn transfer(seed: u64) -> Vec<TraceEvent> {
    const COUNT: u32 = 40;
    let mut net = Simulator::new(hostile(seed));
    let (a, b) = (net.add_node("a").unwrap(), net.add_node("b").unwrap());
    let (mut next, mut received) = (1, Vec::new());
    net.send(a, b, Packet::Data(next)).unwrap();
    for round in 0.. {
        if next > COUNT {
            break;
        }
        match round {
            30 => net.partition(&[a], &[b]).unwrap(),
            60 => net.heal(),
            _ => {}
        }
        match net.step() {
            Some(Delivery { payload: Packet::Data(n), .. }) => {
                if n == received.len() as u32 + 1 {
                    received.push(n);
                }
                net.send(b, a, Packet::Ack(received.len() as u32)).unwrap();
            }
            Some(Delivery { payload: Packet::Ack(n), .. }) if n == next => {
                next += 1;
                net.send(a, b, Packet::Data(next.min(COUNT))).unwrap();
            }
            Some(_) => {}
            None => {
                net.advance_idle_to(net.now() + 100 * MS).unwrap();
                net.send(a, b, Packet::Data(next.min(COUNT))).unwrap();
            }
        }
    }
    assert_eq!(received, (1..=COUNT).collect::<Vec<_>>(), "seed {seed}");
    let stats = net.stats();
    assert!(stats.dropped > 0 && stats.duplicated > 0, "seed {seed}: {stats:?}");
    net.trace().iter().copied().collect()
}

#[test]
fn a_retransmitting_protocol_survives_a_hostile_network_on_every_seed() {
    assert_eq!(check_seeds(0..200, |seed| drop(transfer(seed))), Ok(()));
}

#[test]
fn the_same_seed_gives_the_same_trace_and_another_seed_does_not() {
    assert_eq!(transfer(7), transfer(7));
    assert_ne!(transfer(7), transfer(8));
}

/// A model's own choices, drawn between the network's, replay from the same seed.
#[test]
fn picks_share_the_networks_seed() {
    let run = |seed| {
        let mut net = Simulator::new(hostile(seed));
        let (a, b) = (net.add_node("a").unwrap(), net.add_node("b").unwrap());
        let mut picks = Vec::new();
        for n in 0..20u32 {
            picks.push(net.pick(10..20));
            net.send(a, b, Packet::Data(n)).unwrap();
        }
        net.run(100, |net, _| picks.push(net.pick(10..20)));
        assert!(picks.iter().all(|p| (10..20).contains(p)));
        (picks, net.trace().iter().copied().collect::<Vec<_>>())
    };
    assert_eq!(run(7), run(7));
    assert_ne!(run(7).0[..20], run(8).0[..20], "the picks before any delivery differ");
}

#[test]
fn a_refused_operation_changes_nothing() {
    let cfg = SimConfig { max_nodes: 2, max_pending: 2, ..hostile(3) };
    let (mut tried, mut clean) = (Simulator::new(cfg.clone()), Simulator::new(cfg));
    for net in [&mut tried, &mut clean] {
        net.add_node("a").unwrap();
        net.add_node("b").unwrap();
    }
    assert_eq!(tried.add_node("c"), Err(SimError::TooManyNodes));
    assert_eq!(tried.send(0, 9, 1), Err(SimError::UnknownNode(9)));
    assert_eq!(tried.partition(&[0], &[9]), Err(SimError::UnknownNode(9)));
    assert_eq!(tried.crash_node(9), Err(SimError::UnknownNode(9)));
    for net in [&mut tried, &mut clean] {
        while net.pending() < 2 {
            net.send(0, 1, 0).unwrap();
        }
    }
    assert_eq!(tried.send(0, 1, 1), Err(SimError::Full));
    assert_eq!(tried.advance_idle_to(Duration::from_secs(9)), Err(SimError::NotIdle));
    for net in [&mut tried, &mut clean] {
        for n in 0..20 {
            net.send(1, 0, n).ok();
            net.step();
        }
    }
    assert_eq!((tried.trace(), tried.stats(), tried.now()), (clean.trace(), clean.stats(), clean.now()));
}

#[test]
fn links_keep_order_by_default_and_faults_drop_with_a_reason() {
    let mut net = Simulator::new(SimConfig { jitter: 50 * MS, max_trace: 3, ..Default::default() });
    let [a, b, c] = ["a", "b", "c"].map(|name| net.add_node(name).unwrap());
    for n in 0..50 {
        net.send(a, b, n).unwrap();
    }
    let mut seen = Vec::new();
    assert_eq!(net.run(usize::MAX, |_, d| seen.push(d.payload)), 50);
    assert_eq!(seen, (0..50).collect::<Vec<_>>());
    assert_eq!(net.trace().len(), 3);

    let reason = |net: &Simulator<i32>| match net.trace().back().unwrap().kind {
        TraceKind::Dropped(reason) => reason,
        kind => panic!("{kind:?}"),
    };
    net.set_link(a, c, Link { up: true, delay: Some(Duration::from_secs(1)) }).unwrap();
    net.send(a, c, 1).unwrap();
    let wait = net.next_delivery_at().unwrap() - net.now();
    assert!((Duration::from_secs(1)..=Duration::from_secs(1) + 50 * MS).contains(&wait), "{wait:?}");
    net.partition(&[a, b], &[c]).unwrap();
    assert!(net.step().is_none());
    assert_eq!(reason(&net), DropReason::LinkDown);

    net.heal();
    net.set_node_down(c, true).unwrap();
    net.send(a, c, 2).unwrap();
    assert!(net.step().is_none());
    assert_eq!(reason(&net), DropReason::NodeDown);
    net.send(c, a, 3).unwrap();
    assert_eq!((reason(&net), net.pending()), (DropReason::NodeDown, 0));

    net.set_node_down(c, false).unwrap();
    net.send(a, c, 4).unwrap();
    net.send(a, b, 5).unwrap();
    assert_eq!((net.crash_node(c), net.incarnation(c), net.pending()), (Ok(2), 2, 1));
    assert_eq!(reason(&net), DropReason::Crashed);
    assert_eq!(net.step().map(|d| (net.name(d.to).to_string(), d.payload)), Some(("b".into(), 5)));
}

#[test]
fn a_failing_seed_is_named() {
    let ran = std::sync::Mutex::new(Vec::new());
    let failed = check_seeds(0..10, |seed| {
        ran.lock().unwrap().push(seed);
        if seed == 6 {
            std::thread::sleep(Duration::from_millis(50)); // so that 9, on another core, fails first
        }
        assert!(seed != 6 && seed != 9, "bad {seed}");
    });
    assert_eq!(failed.unwrap_err(), FailedSeed { seed: 6, message: "bad 6".into() });
    let mut ran = ran.into_inner().unwrap();
    ran.sort();
    assert_eq!(ran[..7], [0, 1, 2, 3, 4, 5, 6]);
}

#[test]
fn a_timer_due_before_the_next_live_arrival_fires_though_a_cut_drops_an_earlier_one() {
    let mut net = Simulator::new(SimConfig::default());
    let [a, b, c] = ["a", "b", "c"].map(|name| net.add_node(name).unwrap());
    net.send(a, b, 1).unwrap(); // arrives at 1 ms, across the cut
    net.set_link(a, c, Link { up: true, delay: Some(10 * MS) }).unwrap();
    net.send(a, c, 2).unwrap();
    net.partition(&[a], &[b]).unwrap();

    assert_eq!(net.next_delivery_at(), Some(10 * MS));
    net.advance_idle_to(5 * MS).unwrap();
    assert_eq!((net.now(), net.stats().dropped), (5 * MS, 1));
    assert!(matches!(net.step(), Some(Delivery { at, payload: 2, .. }) if at == 10 * MS));
}
