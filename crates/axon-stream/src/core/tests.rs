#![expect(clippy::disallowed_methods, reason = "Instant::now is only the base of each test's virtual clock")]

use super::*;

fn key(n: usize) -> StreamKey {
    StreamKey::new("md", format!("k{n}"))
}

const RECORD: usize = 1024;

/// The pushes of the default config, at a sixteenth: 64 KiB pushes, so a window from 128 KiB to 2 MiB.
fn scaled() -> CoreConfig {
    let mut cfg =
        CoreConfig { relay: false, max_push_bytes: 64 << 10, max_push_window: 2 << 20, ..CoreConfig::default() };
    (cfg.hub.max_batch_bytes, cfg.hub.max_batch_records) = (16 << 10, 256);
    cfg
}

/// An origin of `keys` streams, each with `records` records of `RECORD` bytes in batches of 16.
fn origin(cfg: &CoreConfig, keys: usize, records: usize) -> RelayCore {
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, cfg.clone()).expect("a valid config");
    let record = Bytes::from(vec![7u8; RECORD]);
    for k in 0..keys {
        origin.open_stream(&key(k), 1, DeliveryPolicy::ReplayRequired).expect("opens");
        for _ in 0..records / 16 {
            origin.publish(&key(k), vec![record.clone(); 16]).expect("publishes");
        }
    }
    origin
}

/// `child` follows the first `keys` streams of `origin` from their first record.
fn follow(origin: &mut RelayCore, child: &PeerId, keys: usize, now: Instant) {
    let start = StartPosition::After(Cursor { generation: 1, sequence: 0 });
    let wants = (0..keys).map(|k| Want { key: key(k), epoch: 1, start, snapshot: false }).collect();
    let interest = Interest { revision: 1, wants, ..Default::default() };
    origin.handle_interest(child, interest, now).expect("an interest the origin holds");
}

/// The child asks its parent until it has nothing more to ask.
pub(crate) fn ask<A: Authorizer, B: Authorizer>(
    parent: &mut RelayCore<A>,
    p: &PeerId,
    child: &mut RelayCore<B>,
    c: &PeerId,
    now: Instant,
) {
    for _ in 0..8 {
        let asked = child.reconcile(std::slice::from_ref(p), now);
        if asked.is_empty() {
            return;
        }
        for (to, interest) in asked {
            let reply = parent.handle_interest(c, interest.clone(), now);
            child.interest_result(&to, interest.revision, reply, now);
        }
    }
}

/// Every push `parent` has for `child`, each answered at once. Returns how many went.
pub(crate) fn deliver<A: Authorizer, B: Authorizer>(
    parent: &mut RelayCore<A>,
    p: &PeerId,
    child: &mut RelayCore<B>,
    c: &PeerId,
    now: Instant,
) -> usize {
    let mut pushes = 0;
    while let Some((id, body)) = parent.next_push(&c.node, Lane::Live, now, None) {
        let (channel, seq, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
        let answer = child.handle_push(p, Lane::Live, p.incarnation, channel, seq, push, now);
        parent.push_answered(c, Lane::Live, id, answer, now);
        pushes += 1;
    }
    pushes
}

pub(crate) fn records<A: Authorizer>(core: &mut RelayCore<A>, id: u64) -> Vec<u64> {
    let mut seen = Vec::new();
    while let Some(event) = core.poll(id) {
        if let StreamEvent::Batch(batch) = event {
            seen.extend(batch.first()..batch.first() + batch.len() as u64);
        }
    }
    seen
}

fn applied(seq: u64) -> PushAnswer {
    PushAnswer::Applied { applied: seq, unwanted: Vec::new() }
}

/// Each child's pushes, answered `Applied` one round trip after they went, until `until`. `each` runs after every
/// round of sends.
fn run(
    origin: &mut RelayCore,
    children: &[(PeerId, Duration)],
    now: Instant,
    until: Instant,
    each: impl FnMut(&mut RelayCore, Instant),
) {
    run_late(origin, children, |rtt| rtt, now, until, each);
}

/// [`run`], with each answer `late(rtt)` after its push.
fn run_late(
    origin: &mut RelayCore,
    children: &[(PeerId, Duration)],
    late: impl Fn(Duration) -> Duration,
    mut now: Instant,
    until: Instant,
    mut each: impl FnMut(&mut RelayCore, Instant),
) {
    let (mut flights, mut n) = (BTreeMap::new(), 0u64);
    loop {
        for (i, (c, rtt)) in children.iter().enumerate() {
            while let Some((id, _)) = origin.next_push(&c.node, Lane::Live, now, Some(*rtt)) {
                flights.insert((now + late(*rtt), n), (i, id));
                n += 1;
            }
        }
        each(origin, now);
        let Some(((at, _), (i, id))) = flights.pop_first().filter(|((at, _), _)| *at <= until) else { return };
        now = at;
        origin.push_answered(&children[i].0, Lane::Live, id, applied(id.seq), now);
    }
}

#[test]
fn a_child_applies_only_the_next_push_of_its_channel() {
    let mut child = RelayCore::new(&PeerId::new("c", 1), AllowAll, CoreConfig::default()).expect("a valid config");
    let (p, now) = (PeerId::new("p", 1), Instant::now());
    let mut push = |channel, seq| child.handle_push(&p, Lane::Live, 1, channel, seq, Push::default(), now);
    assert_eq!(push(1, 2), PushAnswer::Retired, "a channel it never followed, past its first push");
    assert_eq!(push(1, 1), applied(1));
    assert_eq!(push(2, 2), PushAnswer::Skipped { applied: 0 }, "a newer channel, before its first push");
    assert_eq!(push(2, 1), applied(1));
    assert_eq!(push(2, 2), applied(2));
    assert_eq!(push(2, 2), PushAnswer::Skipped { applied: 2 }, "the same push twice");
    assert_eq!(push(2, 1), PushAnswer::Skipped { applied: 2 }, "an older one");
    assert_eq!(push(1, 2), PushAnswer::Retired, "the channel before");
    let stats = child.stats();
    assert_eq!((stats.pushes_in, stats.push_skipped), (3, [2, 1, 2]), "nothing applied twice");
}

/// A namespace on `Bulk` has a channel of its own to each child: its pushes carry only its streams, a full `Bulk`
/// window leaves `Live` its own, and the child follows each lane's channel apart.
#[test]
fn each_lane_has_a_channel_and_a_window_of_its_own() {
    let (mut cfg, o, c, now) = (scaled(), PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    cfg.lanes.insert("bulk".into(), Lane::Bulk);
    let bulk = StreamKey::new("bulk", "dump");
    let mut origin = origin(&cfg, 1, 16);
    origin.open_stream(&bulk, 1, DeliveryPolicy::ReplayRequired).expect("opens");
    for _ in 0..1024 / 16 {
        origin.publish(&bulk, vec![Bytes::from(vec![7u8; RECORD]); 16]).expect("publishes");
    }
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig { relay: true, ..cfg.clone() }).expect("a valid config");
    let first = SubscribeOptions { start: StartPosition::First, ..Default::default() };
    let subs = [&key(0), &bulk].map(|k| child.subscribe(k, first).expect("subscribes"));
    ask(&mut origin, &o, &mut child, &c, now);

    let mut pushes = |origin: &mut RelayCore, lane| {
        let mut sent = Vec::new();
        while let Some((id, body)) = origin.next_push("c", lane, now, None) {
            let (channel, seq, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
            assert!(push.items.iter().all(|(k, ..)| cfg.lane(k) == lane), "a push on {lane:?} of another lane");
            sent.push((id, child.handle_push(&o, lane, o.incarnation, channel, seq, push, now)));
        }
        sent
    };
    let bulk_sent = pushes(&mut origin, Lane::Bulk);
    let window = origin.stats().downstream.iter().find(|d| d.lane == Lane::Bulk).expect("a Bulk row").window_bytes;
    assert!(bulk_sent.len() >= 2 && window < 1024 * RECORD, "the Bulk window is full: {}", bulk_sent.len());
    let live_sent = pushes(&mut origin, Lane::Live);
    assert_eq!(live_sent.len(), 1, "Live pushes beside a full Bulk window");
    for (lane, sent) in [(Lane::Bulk, bulk_sent), (Lane::Live, live_sent)] {
        for (id, answer) in sent {
            assert_eq!(answer, applied(id.seq), "{lane:?} is a channel of its own at the child");
            origin.push_answered(&c, lane, id, answer, now);
        }
    }
    assert_eq!(records(&mut child, subs[0]), (1..=16).collect::<Vec<_>>());
    let rows: Vec<_> = origin.stats().downstream.into_iter().map(|d| (d.lane, d.branches, d.channel)).collect();
    assert!(matches!(rows[..], [(Lane::Live, 1, a), (Lane::Bulk, 1, b)] if a != b), "a row per lane: {rows:?}");
}

/// Interests go on `Control`: within `max_interest_bytes`, where smaller than `max_push_bytes`, and never below three
/// of the largest wants.
#[test]
fn interests_keep_within_max_interest_bytes() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let small = CoreConfig { max_interest_bytes: 3 * 4096, ..CoreConfig::default() };
    assert!(RelayCore::new(&c, AllowAll, small).is_err(), "too small for three wants");
    let cfg = CoreConfig { max_interest_bytes: 16 << 10, ..CoreConfig::default() };
    let mut child = RelayCore::new(&c, AllowAll, cfg).expect("a bound that holds three wants");
    let long = |k: usize| StreamKey::new("md", format!("{k:0>200}"));
    for k in 0..400 {
        child.subscribe(&long(k), SubscribeOptions::default()).expect("subscribes");
    }
    let (mut wanted, mut interests) = (0, 0);
    for _ in 0..64 {
        let Some((to, interest)) = child.reconcile(std::slice::from_ref(&o), now).into_iter().next() else { break };
        let bytes = wire::encode_interest(&interest).len();
        assert!(bytes <= 16 << 10, "an interest of {bytes} bytes");
        (wanted, interests) = (wanted + interest.wants.len(), interests + 1);
        let answers = interest.wants.iter().map(|_| Status::Ok.into()).collect();
        child.interest_result(&to, interest.revision, Reply::Answers(answers), now);
    }
    assert_eq!(wanted, 400);
    assert!(interests >= 5, "{interests} interests");
}

#[test]
fn a_downstream_made_again_gets_a_new_channel_and_its_old_pushes_are_retired() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 1, 16);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    let (old_id, old) = origin.next_push("c", Lane::Live, now, None).expect("the backlog");
    assert_eq!((old_id.channel, old_id.seq), (1, 1));

    origin.expire_downstream(&c, now);
    // The child's next refresh finds the parent holds nothing for it, and sends the whole set again.
    let later = now + Duration::from_secs(3);
    ask(&mut origin, &o, &mut child, &c, later);
    assert_eq!(origin.stats().downstream[0].channel, 2, "a new channel");
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, later), 1);
    assert_eq!(records(&mut child, id), (1..=17).collect::<Vec<_>>());

    let (channel, seq, push) = wire::decode_push(&mut old.slice(1..)).expect("a push decodes");
    assert_eq!(child.handle_push(&o, Lane::Live, 1, channel, seq, push, later), PushAnswer::Retired);
    origin.push_answered(&c, Lane::Live, old_id, PushAnswer::Retired, later);
    assert_eq!(origin.downstream_peer("c", Lane::Live), Some(&c), "an answer on the old channel changes nothing");
    assert_eq!((child.stats().pushes_in, records(&mut child, id)), (1, vec![]), "nothing of the old push applied");
}

#[test]
fn a_parent_boot_retired_stays_retired() {
    let mut child = RelayCore::new(&PeerId::new("c", 1), AllowAll, CoreConfig::default()).expect("a valid config");
    let (p, now) = (PeerId::new("p", 0), Instant::now());
    let mut push = |boot, seq| child.handle_push(&p, Lane::Live, boot, 1, seq, Push::default(), now);
    assert_eq!(push(1, 1), applied(1));
    assert_eq!(push(2, 2), PushAnswer::Skipped { applied: 0 }, "a new boot is followed from its first push");
    assert_eq!(push(2, 1), applied(1));
    assert_eq!(push(1, 2), PushAnswer::Retired, "a late push of the boot before");
    for boot in 3..=6 {
        assert_eq!(push(boot, 1), applied(1));
        assert_eq!(push(boot - 1, 2), PushAnswer::Retired, "boot {}", boot - 1);
    }
    for boot in 2..=5 {
        assert_eq!(push(boot, 1), PushAnswer::Retired, "boot {boot} is among the 4 kept");
    }
    assert_eq!(push(1, 1), applied(1), "a fifth boot back is forgotten");
}

#[test]
fn a_parent_boot_seen_on_one_lane_retires_the_one_before_on_each() {
    let mut child = RelayCore::new(&PeerId::new("c", 1), AllowAll, CoreConfig::default()).expect("a valid config");
    let (p, now) = (PeerId::new("p", 0), Instant::now());
    let mut push = |lane, boot, seq| child.handle_push(&p, lane, boot, 1, seq, Push::default(), now);
    assert_eq!((push(Lane::Live, 1, 1), push(Lane::Bulk, 1, 1)), (applied(1), applied(1)));
    assert_eq!(push(Lane::Live, 2, 1), applied(1));
    assert_eq!(push(Lane::Bulk, 1, 2), PushAnswer::Retired, "a late push of the boot before, on the other lane");
    assert_eq!(push(Lane::Bulk, 2, 1), applied(1), "the new boot's first push there");

    // The new boot's first push on a lane not followed yet retires the boot before on the others too.
    let mut child = RelayCore::new(&PeerId::new("c", 1), AllowAll, CoreConfig::default()).expect("a valid config");
    let mut push = |lane, boot, seq| child.handle_push(&p, lane, boot, 1, seq, Push::default(), now);
    assert_eq!((push(Lane::Live, 1, 1), push(Lane::Bulk, 2, 1)), (applied(1), applied(1)));
    assert_eq!(push(Lane::Live, 1, 2), PushAnswer::Retired, "a late push of the boot before, on the lane it had");
    assert_eq!(push(Lane::Bulk, 1, 1), PushAnswer::Retired);
    assert_eq!((push(Lane::Live, 2, 1), push(Lane::Bulk, 2, 2)), (applied(1), applied(2)), "the new boot goes on");
}

#[test]
fn a_late_push_of_a_boot_never_followed_costs_the_live_boot_one_new_channel() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 1, 16);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, now), 1);

    // A boot of the parent the child never followed, or one more than four back: it is taken as the newest, and the
    // live boot is retired.
    assert_eq!(child.handle_push(&o, Lane::Live, 9, 1, 1, Push::default(), now), applied(1));
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let (id2, push) = origin.next_push("c", Lane::Live, now, None).expect("the new record");
    let (channel, seq, push) = wire::decode_push(&mut push.slice(1..)).expect("a push decodes");
    let answer = child.handle_push(&o, Lane::Live, 1, channel, seq, push, now);
    assert_eq!(answer, PushAnswer::Retired);
    origin.push_answered(&c, Lane::Live, id2, answer, now);
    assert_eq!(origin.downstream_peer("c", Lane::Live), None, "the parent dropped the channel");

    // The child's next refresh is answered out of sync, and it forgets what it followed of that parent.
    let later = now + Duration::from_secs(3);
    ask(&mut origin, &o, &mut child, &c, later);
    assert_eq!(origin.stats().downstream[0].channel, 2);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, later), 1);
    assert_eq!(records(&mut child, id), (1..=17).collect::<Vec<_>>(), "nothing lost, nothing twice");
}

#[test]
fn a_child_that_answers_below_what_it_applied_makes_the_parent_start_a_new_channel() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 1, 16);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, now), 1);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let (late, _) = origin.next_push("c", Lane::Live, now, None).expect("the new record");
    origin.push_answered(&c, Lane::Live, late, PushAnswer::Skipped { applied: 0 }, now);
    assert_eq!(origin.downstream_peer("c", Lane::Live), None, "a child that went back cannot be fed on that channel");

    let later = now + Duration::from_secs(3);
    ask(&mut origin, &o, &mut child, &c, later);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, later), 1);
    assert_eq!(records(&mut child, id), (1..=17).collect::<Vec<_>>());
}

#[test]
fn a_child_that_skips_every_push_is_given_up_though_its_interests_arrive() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 1, 16);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    child
        .subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() })
        .expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, now), 1);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let expiry = Duration::from_secs(10);
    for s in 0..12 {
        let at = now + Duration::from_secs(s);
        ask(&mut origin, &o, &mut child, &c, at);
        let Some((id, _)) = origin.next_push("c", Lane::Live, at, None) else { break };
        origin.push_answered(&c, Lane::Live, id, PushAnswer::Skipped { applied: 1 }, at);
        origin.expire_unheard(at, expiry);
    }
    assert_eq!(origin.downstream_peer("c", Lane::Live), None, "a child that never takes a push is failing");
}

#[test]
fn a_child_that_forgot_its_channel_makes_the_parent_start_a_new_one() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 1, 16);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, now), 1);
    assert_eq!(records(&mut child, id), (1..=16).collect::<Vec<_>>());

    // The parent left the child's view and came back: the child forgot the channel, the parent did not.
    child.parents_left(["o"], now);
    ask(&mut origin, &o, &mut child, &c, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let (id2, push) = origin.next_push("c", Lane::Live, now, None).expect("the new record");
    assert_eq!((id2.channel, id2.seq), (1, 2));
    let (channel, seq, push) = wire::decode_push(&mut push.slice(1..)).expect("a push decodes");
    let answer = child.handle_push(&o, Lane::Live, 1, channel, seq, push, now);
    assert_eq!(answer, PushAnswer::Retired);
    origin.push_answered(&c, Lane::Live, id2, answer, now);
    assert_eq!(origin.downstream_peer("c", Lane::Live), None, "the parent dropped the channel");

    let later = now + Duration::from_secs(3);
    ask(&mut origin, &o, &mut child, &c, later);
    assert_eq!(origin.stats().downstream[0].channel, 2);
    assert_eq!(deliver(&mut origin, &o, &mut child, &c, later), 1);
    assert_eq!(records(&mut child, id), [17], "from the position the child's interest carries");
}

#[test]
fn the_window_grows_with_the_rate_and_stops_at_its_ceiling() {
    let (cfg, c, now, rtt) = (scaled(), PeerId::new("c", 1), Instant::now(), Duration::from_millis(200));
    let mut origin = origin(&cfg, 32, 384);
    follow(&mut origin, &c, 32, now);
    let mut peaks = vec![0; 8];
    run(&mut origin, &[(c, rtt)], now, now + rtt * 8, |origin, at| {
        let round = (at - now).as_millis() as usize / 200;
        let d = origin.downstream["c"].lanes[Lane::Live as usize].as_ref().expect("a Live channel");
        assert!(d.inflight <= cfg.max_push_window, "{} bytes in flight", d.inflight);
        peaks[round.min(7)] = peaks[round.min(7)].max(d.inflight);
    });
    println!("bytes in flight per round trip: {peaks:?}");
    for (round, peak) in peaks.iter().enumerate() {
        let want = (2 * cfg.max_push_bytes * 3usize.pow(round as u32)).min(cfg.max_push_window);
        assert!(*peak >= want * 3 / 4 && *peak <= want, "round {round}: {peak} bytes in flight, {want} wanted");
    }
    let d = &origin.stats().downstream[0];
    assert_eq!((d.window_bytes, d.round_trip_us), (cfg.max_push_window, Some(200_000)));
    assert!(d.delivery_rate >= (cfg.max_push_window as u64 * 7 / 8) * 5, "{} bytes a second", d.delivery_rate);
}

#[test]
fn one_stream_reaches_several_pushes_per_round_trip_up_to_half_its_branch() {
    let (c, now, rtt) = (PeerId::new("c", 1), Instant::now(), Duration::from_millis(200));
    let peak = |branch: usize| {
        let mut cfg = scaled();
        cfg.branch.max_bytes = branch;
        let mut origin = origin(&cfg, 1, 12 << 10);
        follow(&mut origin, &c, 1, now);
        let (mut bytes, mut pushes) = (0, 0);
        run(&mut origin, &[(c.clone(), rtt)], now, now + rtt * 8, |origin, _| {
            let d = &origin.downstream["c"];
            let live = d.lanes[Lane::Live as usize].as_ref().expect("a Live channel");
            (bytes, pushes) = (bytes.max(live.inflight), pushes.max(live.unacked.len()));
            let id = d.branches[&key(0)].1;
            let held = origin.hub.inflight(id).expect("the branch").bytes;
            assert!(held < branch / 2 + cfg.max_push_bytes, "{held} bytes of the stream in flight");
        });
        (bytes, pushes)
    };
    let (half, pushes) = peak(512 << 10);
    println!("a 512 KiB branch: {half} bytes in {pushes} pushes in flight");
    assert!(pushes >= 4 && (256 << 10..(256 << 10) + (64 << 10)).contains(&half), "{half} bytes in {pushes} pushes");
    let (window, pushes) = peak(8 << 20);
    println!("an 8 MiB branch: {window} bytes in {pushes} pushes in flight");
    assert!(window >= (2 << 20) * 7 / 8 && pushes >= 28, "{window} bytes in {pushes} pushes");
}

/// Under loss an answer comes 2.2 round trips after its push at best: a lost packet holds what follows it on the
/// connection one round trip more, and the connection paces what follows at its low estimate. A third push needs a
/// window a quarter above two, so the window leaves its floor at 3 × rate × rtt (1.36 times) and not at 2 (0.91).
#[test]
fn a_window_grows_though_every_answer_comes_a_lost_packet_late() {
    let (cfg, c, now, rtt) = (scaled(), PeerId::new("c", 1), Instant::now(), Duration::from_millis(200));
    let mut origin = origin(&cfg, 32, 384);
    follow(&mut origin, &c, 32, now);
    run_late(&mut origin, &[(c, rtt)], |rtt| rtt * 11 / 5, now, now + rtt * 40, |_, _| {});
    let window = origin.stats().downstream[0].window_bytes;
    assert!(window >= 8 * cfg.max_push_bytes, "{window} bytes: the window stayed at its floor");
}

#[test]
fn a_slow_reader_is_reset_with_pushes_in_flight() {
    let (mut cfg, c, now, rtt) = (scaled(), PeerId::new("c", 1), Instant::now(), Duration::from_millis(200));
    cfg.branch.max_bytes = 512 << 10;
    // The child keeps up with a backlog for a few round trips, so its window outgrows half the branch.
    let mut origin = origin(&cfg, 1, 1024);
    follow(&mut origin, &c, 1, now);
    run(&mut origin, &[(c.clone(), rtt)], now, now + rtt * 8, |_, _| {});
    let id = origin.downstream["c"].branches[&key(0)].1;
    assert!(origin.channel(&c, Lane::Live).expect("the child").window(&cfg, now + rtt * 8) >= 512 << 10);
    assert_eq!(origin.stats().downstream[0].pushed_records, 1024, "the branch is live");
    // Then no answer comes, while the stream goes on.
    let record = Bytes::from(vec![7u8; RECORD]);
    let publish = |origin: &mut RelayCore| {
        let _ = origin.publish(&key(0), vec![record.clone(); 16]);
    };
    let (mut at, mut reset) = (now + rtt * 8, None);
    for _ in 0..200 {
        at += Duration::from_millis(10);
        publish(&mut origin);
        let held = |origin: &RelayCore| {
            let (queued, inflight) = (origin.stats().downstream[0].queued_bytes, origin.hub.inflight(id));
            queued + inflight.map_or(0, |f| f.bytes)
        };
        assert!(held(&origin) <= cfg.branch.max_bytes, "{} bytes held for the stream", held(&origin));
        while let Some((_, body)) = origin.next_push("c", Lane::Live, at, Some(rtt)) {
            let (_, _, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
            if let Some((_, _, Item::Reset(reason))) = push.items.last() {
                reset = Some(*reason);
            }
        }
        if reset.is_some() {
            break;
        }
    }
    assert_eq!(reset, Some(ResetReason::SlowConsumer), "the reset goes out with pushes in flight");
    assert_eq!(origin.downstream["c"].branches.get(&key(0)), None);
}

/// A branch held at half its bound in flight is not ready to push, but what it queues is the child's backlog.
#[test]
fn a_backed_up_childs_queued_bytes_count_every_branch() {
    let (mut cfg, c, now) = (scaled(), PeerId::new("c", 1), Instant::now());
    cfg.branch.max_bytes = 64 << 10;
    // A branch on each lane: a channel counts only its own lane's.
    cfg.lanes.insert("bulk".into(), Lane::Bulk);
    let (mut origin, bulk) = (origin(&cfg, 1, 0), StreamKey::new("bulk", "dump"));
    origin.open_stream(&bulk, 1, DeliveryPolicy::ReplayRequired).expect("opens");
    let start = StartPosition::After(Cursor { generation: 1, sequence: 0 });
    let wants = [key(0), bulk.clone()].map(|key| Want { key, epoch: 1, start, snapshot: false }).into();
    origin.handle_interest(&c, Interest { revision: 1, wants, ..Default::default() }, now).expect("an interest");
    let batch = vec![Bytes::from(vec![7u8; RECORD]); 16];
    for _ in 0..4 {
        origin.publish(&key(0), batch.clone()).expect("publishes");
    }
    origin.publish(&bulk, batch).expect("publishes");
    while origin.next_push("c", Lane::Live, now, None).is_some() {}
    let id = origin.downstream["c"].branches[&key(0)].1;
    let queued = origin.hub.queue_usage(id).expect("the branch").bytes;
    assert!(queued > 0 && !origin.channel(&c, Lane::Live).expect("the child").ready.contains(&id), "held back");
    let lanes: Vec<_> = origin.stats().downstream.iter().map(|d| (d.lane, d.queued_bytes)).collect();
    assert_eq!(lanes, [(Lane::Live, queued), (Lane::Bulk, 16 * RECORD)]);
}

#[test]
fn a_streams_charge_is_released_on_applied_on_its_end_and_on_a_retire_but_not_on_a_go_back() {
    let (mut cfg, c, now) = (scaled(), PeerId::new("c", 1), Instant::now());
    cfg.hub.max_batch_bytes = 32 << 10;
    let mut origin = origin(&cfg, 2, 0);
    follow(&mut origin, &c, 2, now);
    // Each send half a push, so the next goes with one in flight.
    let record = Bytes::from(vec![7u8; 2 * RECORD]);
    let send = |origin: &mut RelayCore, k| {
        origin.publish(&key(k), vec![record.clone(); 16]).expect("publishes");
        origin.next_push("c", Lane::Live, now, None).expect("a push")
    };
    let branch = |origin: &RelayCore, k| origin.downstream["c"].branches.get(&key(k)).map(|b| b.1);
    let charged = |origin: &RelayCore, id| origin.hub.inflight(id).map(|f| f.records);
    let (one, two) = (branch(&origin, 0).expect("a branch"), branch(&origin, 1).expect("a branch"));

    let (a, bytes) = send(&mut origin, 0);
    assert_eq!(charged(&origin, one), Some(16), "taken");
    origin.push_failed(&c, Lane::Live, a, now);
    assert_eq!(charged(&origin, one), Some(16), "kept through a failed call");
    let (again, resent) = origin.next_push("c", Lane::Live, now, None).expect("sent again");
    assert_eq!((again.seq, &resent), (a.seq, &bytes), "the same bytes");
    let (b, _) = send(&mut origin, 0);
    origin.push_answered(&c, Lane::Live, b, PushAnswer::Skipped { applied: 0 }, now);
    assert_eq!(charged(&origin, one), Some(32), "kept through a gap");
    let _ = origin.next_push("c", Lane::Live, now, None).expect("a sent again");
    let _ = origin.next_push("c", Lane::Live, now, None).expect("b sent again");
    origin.push_answered(&c, Lane::Live, again, applied(a.seq), now);
    assert_eq!(charged(&origin, one), Some(16), "released on Applied");

    // An `unwanted` ends the branch, its charge with it.
    let (d, _) = send(&mut origin, 1);
    assert_eq!(charged(&origin, two), Some(16));
    origin.push_answered(&c, Lane::Live, d, PushAnswer::Applied { applied: d.seq, unwanted: vec![0] }, now);
    assert_eq!((branch(&origin, 1), charged(&origin, two)), (None, None), "released with its branch");

    // A branch made again for the key starts at zero, and a late answer for the old one frees nothing of it.
    let (old, _) = send(&mut origin, 0);
    let interest = Interest { revision: 2, base: 1, drops: vec![key(0)], ..Default::default() };
    origin.handle_interest(&c, interest, now).expect("drops the key");
    assert_eq!(charged(&origin, one), None, "released on its end");
    let start = StartPosition::After(Cursor { generation: 1, sequence: 48 });
    let want = Want { key: key(0), epoch: 2, start, snapshot: false };
    let interest = Interest { revision: 3, whole: Some((None, None)), wants: vec![want], ..Default::default() };
    origin.handle_interest(&c, interest, now).expect("wants it again");
    let fresh = branch(&origin, 0).expect("a new branch");
    assert_eq!(charged(&origin, fresh), Some(0));
    let (new, _) = send(&mut origin, 0);
    assert_eq!(charged(&origin, fresh), Some(16));
    origin.push_answered(&c, Lane::Live, old, applied(b.seq), now);
    assert_eq!(charged(&origin, fresh), Some(16), "a late answer for the old branch");

    origin.push_answered(&c, Lane::Live, new, PushAnswer::Retired, now);
    assert_eq!(
        (origin.downstream_peer("c", Lane::Live), charged(&origin, fresh)),
        (None, None),
        "released on a retire"
    );
    assert_eq!(origin.stats().hub.queued_records, 0);
}

#[test]
fn the_node_cap_holds_every_channel_past_its_first_push() {
    let (mut cfg, now, rtt) = (scaled(), Instant::now(), Duration::from_millis(200));
    cfg.max_push_inflight = 3 << 20;
    let mut origin = origin(&cfg, 32, 384);
    let children: Vec<_> = (0..4).map(|n| (PeerId::new(format!("c{n}").as_str(), 1), rtt)).collect();
    for (c, _) in &children {
        follow(&mut origin, c, 32, now);
    }
    let mut peak = 0;
    run(&mut origin, &children, now, now + rtt * 10, |origin, _| {
        let past: usize = origin.channels().map(|(_, _, c)| c.past_first().0).sum();
        assert!(past <= cfg.max_push_inflight, "{past} bytes past each channel's first push");
        peak = peak.max(past);
    });
    println!("at most {peak} bytes past the first pushes");
    assert!(peak >= cfg.max_push_inflight * 7 / 8, "the cap was never reached: {peak}");
    assert!(origin.stats().push_capped > 0);
}

#[test]
fn the_node_cap_is_shared_fairly_between_busy_channels() {
    let (mut cfg, now) = (scaled(), Instant::now());
    cfg.max_push_inflight = 3 << 20;
    let mut origin = origin(&cfg, 32, 1536);
    let fast = (PeerId::new("fast", 1), Duration::from_millis(100));
    let slow = (1..=3).map(|n| (PeerId::new(format!("slow{n}").as_str(), 1), Duration::from_millis(250)));
    let children: Vec<_> = std::iter::once(fast).chain(slow).collect();
    for (c, _) in &children {
        follow(&mut origin, c, 32, now);
    }
    let share = cfg.max_push_inflight / children.len();
    let mut peaks = BTreeMap::new();
    run(&mut origin, &children, now, now + Duration::from_secs(2), |origin, _| {
        let fast = origin.downstream["fast"].lanes[Lane::Live as usize].as_ref().expect("a Live channel");
        assert!(fast.busy(), "the fast child ran out of backlog");
        for (d, _, c) in origin.channels() {
            let peak = peaks.entry(d.peer.node.to_string()).or_insert(0);
            *peak = (*peak).max(c.past_first().0);
        }
    });
    println!("past their first push, at most: {peaks:?}");
    for (node, peak) in &peaks {
        assert!(*peak + cfg.max_push_bytes >= share, "{node} held at most {peak}, its share is {share}");
    }
}

#[test]
fn an_answer_past_the_last_push_sent_drops_the_child() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    // A backlog of more than one push, so a second goes before the first is answered.
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 1, 2048);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    let (first, _) = origin.next_push("c", Lane::Live, now, None).expect("the backlog");
    let (second, _) = origin.next_push("c", Lane::Live, now, None).expect("the rest of it");
    assert_eq!((first.seq, second.seq), (1, 2));
    // Neither push reached the child: an answer past them comes from a child that is wrong or lies.
    origin.push_answered(&c, Lane::Live, second, applied(9), now);
    assert_eq!(origin.downstream_peer("c", Lane::Live), None, "nothing it says on that channel can be believed");
    assert_eq!(origin.stats().hub.queued_records, 0, "what the pushes held is released with the channel");

    let later = now + Duration::from_secs(3);
    ask(&mut origin, &o, &mut child, &c, later);
    assert_eq!(origin.stats().downstream[0].channel, 2);
    assert!(deliver(&mut origin, &o, &mut child, &c, later) >= 1);
    assert_eq!(records(&mut child, id), (1..=2048).collect::<Vec<_>>(), "from the position the child holds");
}

#[test]
fn records_published_while_a_push_that_took_all_there_was_is_in_flight_wait_for_its_answer() {
    let (o, c, now) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now());
    let mut origin = origin(&CoreConfig { relay: false, ..CoreConfig::default() }, 4, 16);
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let ids: Vec<u64> = (0..4)
        .map(|k| child.subscribe(&key(k), SubscribeOptions { start: StartPosition::First, ..Default::default() }))
        .collect::<Result<_, _>>()
        .expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    let (first, body) = origin.next_push("c", Lane::Live, now, None).expect("the backlog");
    // Each record alone would be a call of its own: they wait, and go together once the push before is answered.
    for k in 0..4 {
        origin.publish(&key(k), vec![Bytes::from_static(b"x")]).expect("publishes");
        assert!(origin.next_push("c", Lane::Live, now, None).is_none(), "a push of one record while one is in flight");
    }
    let (channel, seq, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
    let answer = child.handle_push(&o, Lane::Live, 1, channel, seq, push, now);
    origin.push_answered(&c, Lane::Live, first, answer, now);
    let (_, body) = origin.next_push("c", Lane::Live, now, None).expect("the records published meanwhile");
    let (channel, seq, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
    assert_eq!(push.items.len(), 4, "one push for the four streams");
    child.handle_push(&o, Lane::Live, 1, channel, seq, push, now);
    for id in ids {
        assert_eq!(records(&mut child, id), (1..=17).collect::<Vec<_>>());
    }
}

#[test]
fn a_backlog_of_small_records_read_back_from_the_ring_fills_the_window() {
    let (cfg, c, now) = (CoreConfig { relay: false, ..CoreConfig::default() }, PeerId::new("c", 1), Instant::now());
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, cfg.clone()).expect("a valid config");
    origin.open_stream(&key(0), 1, DeliveryPolicy::ReplayRequired).expect("opens");
    for _ in 0..32 {
        origin.publish(&key(0), vec![Bytes::from_static(b"x"); 256]).expect("publishes");
    }
    follow(&mut origin, &c, 1, now);
    // A push holds a stream's `max_batch_records` at most, far under half a push of bytes: the rest is not drained.
    origin.next_push("c", Lane::Live, now, None).expect("the backlog");
    assert!(origin.next_push("c", Lane::Live, now, None).is_some(), "the rest of the backlog with a push in flight");
}

/// A parent whose channels 100 ms or more away send without delay, with one stream of no records, and a child `c` that
/// follows it through subscription `id`: (parent, child, id).
fn without_delay(cfg: CoreConfig, now: Instant) -> (RelayCore, RelayCore, u64) {
    // The threshold under test, between the near and far round trips below.
    let cfg = CoreConfig { relay: false, no_delay_rtt: Some(Duration::from_millis(100)), ..cfg };
    let mut origin = origin(&cfg, 1, 0);
    let (o, c) = (PeerId::new("o", 1), PeerId::new("c", 1));
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig::default()).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    ask(&mut origin, &o, &mut child, &c, now);
    deliver(&mut origin, &o, &mut child, &c, now);
    (origin, child, id.expect("subscribes"))
}

const NEAR: Duration = Duration::from_millis(20);
const FAR: Duration = Duration::from_millis(200);

fn hand(child: &mut RelayCore, body: &Bytes, now: Instant) -> PushAnswer {
    let (channel, seq, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
    child.handle_push(&PeerId::new("o", 1), Lane::Live, 1, channel, seq, push, now)
}

#[test]
fn a_channel_past_no_delay_rtt_sends_each_publish_while_pushes_are_in_flight() {
    let (c, now) = (PeerId::new("c", 1), Instant::now());
    let (mut origin, mut child, id) = without_delay(CoreConfig::default(), now);
    let mut sent = Vec::new();
    for n in 1..=4u8 {
        origin.publish(&key(0), vec![Bytes::from(vec![n])]).expect("publishes");
        sent.push(origin.next_push("c", Lane::Live, now, Some(FAR)).expect("a push for each record, unanswered"));
    }
    let first = sent[0].0.seq;
    let seqs: Vec<u64> = sent.iter().map(|(id, _)| id.seq - first).collect();
    assert_eq!(seqs, [0, 1, 2, 3], "four in flight, in order");
    for (push, body) in sent {
        let answer = hand(&mut child, &body, now);
        origin.push_answered(&c, Lane::Live, push, answer, now);
    }
    assert_eq!(records(&mut child, id), [1, 2, 3, 4]);

    // Nearer than `no_delay_rtt`, Nagle's rule holds on the same channel.
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    origin.next_push("c", Lane::Live, now, Some(NEAR)).expect("the first push in flight always goes");
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    assert!(origin.next_push("c", Lane::Live, now, Some(NEAR)).is_none(), "a near child waits for the answer");
    assert!(origin.next_push("c", Lane::Live, now, None).is_none(), "and so does one whose round trip is not known");
    assert!(origin.next_push("c", Lane::Live, now, Some(FAR)).is_some(), "a far one does not");
}

#[test]
fn pushes_past_no_delay_rtt_stay_under_the_node_cap() {
    let (mut cfg, now) = (scaled(), Instant::now());
    // The cap at its least, one window: 32 children with 128 KiB windows would hold 4 MiB past their first pushes.
    (cfg.relay, cfg.no_delay_rtt, cfg.max_push_inflight) =
        (false, Some(Duration::from_millis(100)), cfg.max_push_window);
    let mut origin = origin(&cfg, 1, 0);
    let children: Vec<_> = (0..32).map(|n| PeerId::new(format!("c{n}").as_str(), 1)).collect();
    for c in &children {
        follow(&mut origin, c, 1, now);
    }
    let send = |origin: &mut RelayCore| -> usize {
        let mut sent = 0;
        for c in &children {
            while origin.next_push(&c.node, Lane::Live, now, Some(FAR)).is_some() {
                sent += 1;
            }
        }
        sent
    };
    // Each one record of 12 KiB: a push under half of one, so without `no_delay_rtt` one in flight per child.
    let mut peak = 0;
    for _ in 0..16 {
        origin.publish(&key(0), vec![Bytes::from(vec![7u8; 12 << 10])]).expect("publishes");
        send(&mut origin);
        let past: usize = origin.channels().map(|(_, _, c)| c.past_first().0).sum();
        assert!(past <= cfg.max_push_inflight, "{past} bytes past each channel's first push");
        peak = peak.max(past);
    }
    // Each holds about its share less a push: past one push each, and the cap is what stopped them.
    println!("at most {peak} bytes past the first pushes");
    assert!(peak >= cfg.max_push_inflight / 2, "the cap was never reached: {peak}");
    let capped = origin.stats().push_capped;
    assert!(capped > 0);
    assert_eq!(send(&mut origin), 0, "nothing frees room");
    assert_eq!(origin.stats().push_capped, capped, "a channel asked again while it waits counts the same wait");
}

#[test]
fn pushes_past_no_delay_rtt_stop_at_the_most_pushes_and_bytes_a_channel_holds_in_flight() {
    let (c, now) = (PeerId::new("c", 1), Instant::now());
    let (mut origin, _, _) = without_delay(CoreConfig::default(), now);
    let mut pushes = Vec::new();
    for _ in 0..100 {
        origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
        pushes.extend(origin.next_push("c", Lane::Live, now, Some(FAR)));
    }
    assert_eq!(pushes.len(), MAX_PUSHES_IN_FLIGHT, "one push of one record each, up to the most in flight");
    // An answer frees one push, which takes the 36 records that waited.
    origin.push_answered(&c, Lane::Live, pushes[0].0, applied(pushes[0].0.seq), now);
    let (_, body) = origin.next_push("c", Lane::Live, now, Some(FAR)).expect("one push frees one");
    let (_, _, push) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
    let batched = |item: &Item| if let Item::Batch(batch) = item { batch.len() } else { 0 };
    assert_eq!(push.items.iter().map(|(_, _, item)| batched(item)).sum::<usize>(), 36);
    assert!(origin.next_push("c", Lane::Live, now, Some(FAR)).is_none());

    // Pushes of 12 KiB records in a 128 KiB window: the eighth leaves under half a 64 KiB push of room (8 x 12 KiB is
    // 96 KiB without the headers), so it is the last.
    let (mut origin, _, _) = without_delay(scaled(), now);
    let mut pushes = 0;
    for _ in 0..16 {
        origin.publish(&key(0), vec![Bytes::from(vec![7u8; 12 << 10])]).expect("publishes");
        pushes += usize::from(origin.next_push("c", Lane::Live, now, Some(FAR)).is_some());
    }
    let (_, _, channel) = origin.channels().next().expect("a channel");
    assert_eq!((pushes, channel.window), (8, 128 << 10));
    assert!(channel.inflight <= channel.window, "{} bytes in flight", channel.inflight);
}

#[test]
fn pushes_past_no_delay_rtt_go_again_in_order_after_a_lost_answer() {
    let (c, now) = (PeerId::new("c", 1), Instant::now());
    let (mut origin, mut child, id) = without_delay(CoreConfig::default(), now);
    let mut sent = Vec::new();
    for n in 1..=4u8 {
        origin.publish(&key(0), vec![Bytes::from(vec![n])]).expect("publishes");
        sent.extend(origin.next_push("c", Lane::Live, now, Some(FAR)));
    }
    assert_eq!(sent.len(), 4, "four pushes in flight");
    let first = sent[0].0.seq;
    // The child applies the first, whose answer is lost, and the parent goes back (a failure of the second, sent before
    // that, changes nothing); the third and fourth come after a gap.
    assert_eq!(hand(&mut child, &sent[0].1, now), applied(first));
    origin.push_failed(&c, Lane::Live, sent[0].0, now);
    origin.push_failed(&c, Lane::Live, sent[1].0, now);
    for (push, body) in &sent[2..] {
        let answer = hand(&mut child, body, now);
        assert_eq!(answer, PushAnswer::Skipped { applied: first });
        origin.push_answered(&c, Lane::Live, *push, answer, now);
    }
    // The three the child lacks go again in order, as the same bytes, and a record published since after them.
    origin.publish(&key(0), vec![Bytes::from_static(&[5])]).expect("publishes");
    let mut again = Vec::new();
    while let Some(push) = origin.next_push("c", Lane::Live, now, Some(FAR)) {
        again.push(push);
    }
    assert_eq!(again.iter().map(|(id, _)| id.seq - first).collect::<Vec<_>>(), [1, 2, 3, 4]);
    assert!(again[..3].iter().zip(&sent[1..]).all(|(a, s)| a.1 == s.1), "the same bytes");
    for (push, body) in again {
        let answer = hand(&mut child, &body, now);
        assert_eq!(answer, applied(push.seq));
        origin.push_answered(&c, Lane::Live, push, answer, now);
    }
    assert_eq!(records(&mut child, id), [1, 2, 3, 4, 5], "each once, in order");
    assert_eq!((origin.stats().push_resent, child.stats().push_skipped[GAP]), (3, 2));
    assert_eq!(origin.stats().hub.queued_records, 0);
}

#[test]
fn a_default_subscriber_holds_the_pushes_of_a_round_trip_applied_back_to_back() {
    let (o, c, now, rtt) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now(), Duration::from_millis(200));
    // A backlog of three half branches, so the window reaches half the branch, in rings that keep it all.
    let half = BranchLimits::BRANCH.max_bytes / 2;
    let records = 3 * half / RECORD;
    let mut cfg = CoreConfig::default();
    (cfg.store.max_bytes_per_stream, cfg.store.max_records_per_stream) = (4 * half, records);
    let mut origin = origin(&CoreConfig { relay: false, ..cfg.clone() }, 1, records);
    let mut child = RelayCore::new(&c, AllowAll, cfg).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    // The child's reader reads once a round trip, after every push of the round is applied.
    let (mut at, mut seen, mut peak) = (now, Vec::new(), 0);
    for _ in 0..16 {
        let (mut answers, mut bytes) = (Vec::new(), 0);
        while let Some((push, body)) = origin.next_push("c", Lane::Live, at, Some(rtt)) {
            bytes += body.len();
            let (channel, seq, items) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
            answers.push((push, child.handle_push(&o, Lane::Live, 1, channel, seq, items, at)));
        }
        peak = peak.max(bytes);
        at += rtt;
        for (push, answer) in answers {
            origin.push_answered(&c, Lane::Live, push, answer, at);
        }
        while let Some(event) = child.poll(id) {
            match event {
                StreamEvent::Batch(batch) => seen.extend(batch.first()..batch.first() + batch.len() as u64),
                StreamEvent::ResetRequired(_) | StreamEvent::End(_) => panic!("{event:?} after {} records", seen.len()),
                _ => {}
            }
        }
    }
    println!("at most {peak} bytes of one stream in a round trip");
    assert!(peak > half, "never past half the branch in flight: {peak}");
    assert_eq!(seen, (1..=records as u64).collect::<Vec<_>>());
}

/// A relay whose stream closed asks its parent again after the position it closed at, so an open of the same
/// generation goes on from there for its followers, and not from record 1.
#[test]
fn a_relay_asks_again_after_the_position_its_stream_closed_at() {
    let now = Instant::now();
    let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
    let mut origin = origin(&scaled(), 1, 32);
    let mut relay = RelayCore::new(&r, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let follower = relay.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut relay, &r, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    deliver(&mut origin, &o, &mut relay, &r, now);
    assert_eq!(records(&mut relay, follower), vec![33]);
    origin.close_stream(&key(0)).expect("closes");
    deliver(&mut origin, &o, &mut relay, &r, now);
    // Past any refusal backoff: the relay asks for the key again.
    let later = now + Duration::from_secs(60);
    let asked = relay.reconcile(std::slice::from_ref(&o), later);
    let starts: Vec<_> = asked.iter().flat_map(|(_, i)| &i.wants).map(|w| w.start).collect();
    assert_eq!(starts, vec![StartPosition::After(Cursor { generation: 1, sequence: 33 })]);
}

/// A change in the view asks a parent that lacked the key again at once, but does not start over the wait before a
/// node gives up a position no parent holds: a change every 2 s, under `upstream_timeout`, does not hold the reset off.
#[cfg(feature = "cluster")]
#[test]
fn a_change_in_the_view_does_not_start_the_wait_to_give_a_position_up_over() {
    let mut now = Instant::now();
    let (o, l) = (PeerId::new("o", 1), PeerId::new("l", 1));
    let mut origin = origin(&scaled(), 1, 16);
    let mut leaf = RelayCore::new(&l, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let follower = leaf.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut leaf, &l, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    deliver(&mut origin, &o, &mut leaf, &l, now);
    assert_eq!(records(&mut leaf, follower), vec![17]);
    let parents = [PeerId::new("p1", 1), PeerId::new("p2", 1)];
    let statuses = [Status::HistoryUnavailable, Status::UnknownStream];
    for round in 1..=40 {
        now += Duration::from_millis(500);
        if round % 4 == 0 {
            leaf.membership_changed(now);
        }
        for (peer, interest) in leaf.reconcile(&parents[..], now) {
            let i = parents.iter().position(|p| *p == peer).expect("one of the parents");
            leaf.interest_result(&peer, interest.revision, Some(vec![statuses[i].into(); interest.wants.len()]), now);
        }
    }
    assert_eq!(leaf.stats().local_resets, 1);
}

/// Each round, `leaf` asks its `parents` and the one fed by `origin` is answered by it, the others by their `statuses`;
/// then `origin` delivers what it has. 500 ms a round.
fn rounds(
    origin: &mut RelayCore,
    leaf: &mut RelayCore,
    parents: &[(PeerId, Status)],
    now: &mut Instant,
    n: usize,
) -> Vec<PeerId> {
    let (o, l) = (PeerId::new("o", 1), PeerId::new("l", 1));
    let candidates: Vec<_> = parents.iter().map(|(p, _)| p.clone()).collect();
    let mut asked = Vec::new();
    for _ in 0..n {
        *now += Duration::from_millis(500);
        for (peer, interest) in leaf.reconcile(&candidates[..], *now) {
            asked.push(peer.clone());
            let reply: Reply = match parents.iter().find(|(p, _)| *p == peer) {
                Some((p, _)) if *p == o => origin.handle_interest(&l, interest.clone(), *now).into(),
                Some((_, status)) => Some(vec![(*status).into(); interest.wants.len()]).into(),
                None => panic!("asked {peer:?}, not a parent"),
            };
            leaf.interest_result(&peer, interest.revision, reply, *now);
        }
        deliver(origin, &o, leaf, &l, *now);
    }
    asked
}

/// A parent that catches a node up holds its position, so a later lack of one waits `upstream_timeout` from then: the
/// node left with no parent at a position it held through a stretch on such a parent does not reset at once for a lack
/// of the key it saw before.
#[test]
fn a_parent_that_caught_the_node_up_starts_the_wait_to_give_its_position_up_afresh() {
    let mut now = Instant::now();
    let (o, l) = (PeerId::new("o", 1), PeerId::new("l", 1));
    let mut origin = origin(&scaled(), 1, 16);
    let mut leaf = RelayCore::new(&l, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let follower = leaf.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut leaf, &l, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    deliver(&mut origin, &o, &mut leaf, &l, now);
    assert_eq!(records(&mut leaf, follower), vec![17]);
    let (r, s) = (PeerId::new("r", 1), PeerId::new("s", 1));
    let lacking = [(r.clone(), Status::HistoryUnavailable), (s.clone(), Status::UnknownStream)];
    // 2 s with no parent for 17: r lacks the history, s the key.
    rounds(&mut origin, &mut leaf, &lacking, &mut now, 4);
    assert_eq!(leaf.stats().local_resets, 0);
    // o comes into view and holds 17: it catches the node up and feeds it, with nothing new, for 10 s.
    let with_o = [lacking[0].clone(), lacking[1].clone(), (o.clone(), Status::Ok)];
    rounds(&mut origin, &mut leaf, &with_o, &mut now, 20);
    assert_eq!(leaf.parent_of(&key(0)), Some(&o));
    // o leaves: the wait runs from now, not from the first lack, 11 s ago.
    rounds(&mut origin, &mut leaf, &lacking, &mut now, 5);
    assert_eq!(leaf.stats().local_resets, 0, "within upstream_timeout of o leaving");
    rounds(&mut origin, &mut leaf, &lacking, &mut now, 4);
    assert_eq!(leaf.stats().local_resets, 1, "past upstream_timeout of o leaving");
}

/// A parent asked again long after its last refusal, as in a later lack of the key, is asked again at the first
/// backoff: strikes from a lack that ended in a reset do not keep it from being asked within `upstream_timeout`.
#[test]
fn a_parent_refusing_long_after_its_last_refusal_is_asked_again_at_the_first_backoff() {
    let mut now = Instant::now();
    let (o, l) = (PeerId::new("o", 1), PeerId::new("l", 1));
    let mut origin = origin(&scaled(), 1, 16);
    let mut leaf = RelayCore::new(&l, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let follower = leaf.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut leaf, &l, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    deliver(&mut origin, &o, &mut leaf, &l, now);
    assert_eq!(records(&mut leaf, follower), vec![17]);
    let (r, s) = (PeerId::new("r", 1), PeerId::new("s", 1));
    let lacking = [(r.clone(), Status::HistoryUnavailable), (s.clone(), Status::UnknownStream)];
    // A first lack: s refuses at 1 s and 3 s, two strikes, and the node gives 17 up.
    for round in 0.. {
        assert!(round < 20, "never reset");
        if leaf.stats().local_resets > 0 {
            break;
        }
        rounds(&mut origin, &mut leaf, &lacking, &mut now, 1);
    }
    // o, ranked first, feeds the node for 100 s, past s's longest backoff; s, ranked last, is not asked.
    let with_o = [(o.clone(), Status::Ok), lacking[0].clone(), lacking[1].clone()];
    rounds(&mut origin, &mut leaf, &with_o, &mut now, 4);
    origin.publish(&key(0), vec![Bytes::from_static(b"y")]).expect("publishes");
    let asked = rounds(&mut origin, &mut leaf, &with_o, &mut now, 196);
    assert_eq!(leaf.parent_of(&key(0)), Some(&o));
    assert!(!asked.contains(&s), "s is not asked while o feeds the node");
    assert_eq!(leaf.cursor(&key(0)), Some(Cursor { generation: 1, sequence: 18 }));
    // o leaves and s never opens the key: before the node gives 18 up, s is asked again after one retry_refused.
    let mut asked = Vec::new();
    for round in 1.. {
        assert!(round <= 20, "never reset: {asked:?}");
        let these = rounds(&mut origin, &mut leaf, &lacking, &mut now, 1);
        if leaf.stats().local_resets > 1 {
            break;
        }
        asked.extend(these.into_iter().map(|p| format!("{round} {}", p.node)));
    }
    assert_eq!(asked.iter().filter(|a| a.ends_with(" s")).count(), 2, "{asked:?}");
}

/// A parent that keeps refusing is asked at a backoff that doubles from `retry_refused`: s refuses at 1 s, 3 s, 7 s and
/// 15 s, so about 4 times in 30 s, where a backoff that stayed at 2 s would ask it 12 times.
#[test]
fn a_parent_that_keeps_refusing_is_asked_at_a_backoff_that_doubles() {
    let mut now = Instant::now();
    let (o, l) = (PeerId::new("o", 1), PeerId::new("l", 1));
    let mut origin = origin(&scaled(), 1, 16);
    let mut leaf = RelayCore::new(&l, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let follower = leaf.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut leaf, &l, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    deliver(&mut origin, &o, &mut leaf, &l, now);
    assert_eq!(records(&mut leaf, follower), vec![17]);
    let s = PeerId::new("s", 1);
    let refusing = [(PeerId::new("u", 1), Status::Unavailable), (s.clone(), Status::UnknownStream)];
    let asked = rounds(&mut origin, &mut leaf, &refusing, &mut now, 60);
    let n = asked.iter().filter(|p| **p == s).count();
    assert!((3..=5).contains(&n), "s asked {n} times: {asked:?}");
}

/// A node opens a key it relays only in a generation no older than its own: an `open_stream` or an
/// `open_stream_after` of an older one is refused `GenerationMismatch`, while the relay holds a position in the newer
/// one and after its parent closed the key, where only its follower holds one. The node goes on relaying, and the
/// follower stays in its generation. Before, the open took the key, and the follower, back a generation.
#[test]
fn an_open_of_a_generation_older_than_the_node_holds_is_refused() {
    let rows = ["relaying", "closed"].map(|row| {
        let now = Instant::now();
        let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
        let mut origin = RelayCore::new(&o, AllowAll, CoreConfig { relay: false, ..scaled() }).expect("a valid config");
        origin.open_stream(&key(0), 2, DeliveryPolicy::ReplayRequired).expect("opens");
        let mut relay = RelayCore::new(&r, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
        let log = MemoryStore::new(MemoryLimits::default()).expect("valid limits");
        relay.set_history("md", Arc::new(Mutex::new(log)));
        let live = relay.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
        ask(&mut origin, &o, &mut relay, &r, now);
        origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
        deliver(&mut origin, &o, &mut relay, &r, now);
        assert_eq!(records(&mut relay, live), (1..=10).collect::<Vec<_>>());
        if row == "closed" {
            origin.close_stream(&key(0)).expect("closes");
            deliver(&mut origin, &o, &mut relay, &r, now);
        }
        let older = Cursor { generation: 1, sequence: 5 };
        let refused = [
            relay.open_stream(&key(0), 1, DeliveryPolicy::ReplayRequired),
            relay.open_stream_after(&key(0), older, DeliveryPolicy::ReplayRequired),
        ];
        if row == "relaying" {
            origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
            deliver(&mut origin, &o, &mut relay, &r, now);
        }
        (row, refused, seen(&mut relay, live), relay.is_subscribed(live))
    });
    let mismatch = || Err(StreamError::Recovery(RecoveryError::GenerationMismatch));
    let refused = [mismatch(), mismatch()];
    assert_eq!(rows, [("relaying", refused.clone(), vec!["2 11..11".into()], true), ("closed", refused, vec![], true)]);
}

/// A follower parked on a node that closed a key it originated holds the node's generation: an `open_stream` or an
/// `open_stream_after` of an older one is refused `GenerationMismatch`, and an open of a newer one goes on, with the
/// follower in it. Without the parked follower counted, the open of generation 1 took it back a generation.
#[test]
fn a_parked_follower_in_a_newer_generation_refuses_an_open_of_an_older_one() {
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, scaled()).expect("a valid config");
    let log = MemoryStore::new(MemoryLimits::default()).expect("valid limits");
    origin.set_history("md", Arc::new(Mutex::new(log)));
    origin.open_stream(&key(0), 2, DeliveryPolicy::ReplayRequired).expect("opens");
    let live = origin.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
    assert_eq!(records(&mut origin, live), (1..=10).collect::<Vec<_>>());
    origin.close_stream(&key(0)).expect("closes");
    let parked = (origin.hub.has_stream(&key(0)), origin.hub.newest_generation(&key(0)));
    let older = Cursor { generation: 1, sequence: 5 };
    let refused = [
        origin.open_stream(&key(0), 1, DeliveryPolicy::ReplayRequired),
        origin.open_stream_after(&key(0), older, DeliveryPolicy::ReplayRequired),
    ];
    let newer = origin.open_stream(&key(0), 3, DeliveryPolicy::ReplayRequired);
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let mismatch = || Err(StreamError::Recovery(RecoveryError::GenerationMismatch));
    assert_eq!(
        (parked, refused, newer, seen(&mut origin, live)),
        ((false, Some(2)), [mismatch(), mismatch()], Ok(()), vec!["3 1..1".to_string()])
    );
}

/// An open of a key this node originates is `AlreadyExists` in any generation, with `open_stream` and
/// `open_stream_after`: in a newer one, where only the key conflicts, and in an older one, where the generation does
/// too. The stream goes on as it was. Before, the older one was `GenerationMismatch`.
#[test]
fn an_open_of_a_key_the_node_originates_is_already_exists_before_its_generation_is_checked() {
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, scaled()).expect("a valid config");
    let log = MemoryStore::new(MemoryLimits::default()).expect("valid limits");
    origin.set_history("md", Arc::new(Mutex::new(log)));
    origin.open_stream(&key(0), 2, DeliveryPolicy::ReplayRequired).expect("opens");
    let live = origin.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 3]).expect("publishes");
    assert_eq!(records(&mut origin, live), vec![1, 2, 3]);
    let opens = [1, 3].map(|generation| {
        let after = Cursor { generation, sequence: 10 };
        let opened = [
            origin.open_stream(&key(0), generation, DeliveryPolicy::ReplayRequired),
            origin.open_stream_after(&key(0), after, DeliveryPolicy::ReplayRequired),
        ];
        (generation, opened)
    });
    origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let exists = || [Err(StreamError::AlreadyExists), Err(StreamError::AlreadyExists)];
    assert_eq!(opens, [(1, exists()), (3, exists())]);
    assert_eq!(seen(&mut origin, live), vec!["2 4..4".to_string()]);
}

/// Everything, except replay for the node it names.
struct NoReplayTo(&'static str);

impl Authorizer for NoReplayTo {
    fn authorize(&self, peer: &PeerId, _: &StreamKey, access: Access) -> bool {
        access != Access::Replay || &*peer.node != self.0
    }
}

/// What a subscription is given, a batch as its generation and range, without the `Ready`s.
fn seen<A: Authorizer>(core: &mut RelayCore<A>, id: u64) -> Vec<String> {
    std::iter::from_fn(|| core.poll(id))
        .filter_map(|event| match event {
            StreamEvent::Batch(b) => Some(format!("{} {}..{}", b.generation(), b.first(), b.last())),
            StreamEvent::Ready { .. } => None,
            other => Some(format!("{other:?}")),
        })
        .collect()
}

/// A relay its parent may not replay to asks again after the position its stream closed at. A parent that has gone
/// past it refuses the start as `HistoryUnavailable`, not `Forbidden`, so the relay takes the stream live and its
/// follower, which missed records, is reset. Before, the relay asked after the position forever and the follower
/// heard nothing.
#[test]
fn a_relay_its_parent_may_not_replay_to_goes_on_live_after_a_close() {
    // Records the origin publishes before the relay asks again, and what the relay's follower then sees.
    let reset = format!("{:?}", StreamEvent::ResetRequired(ResetReason::HistoryUnavailable));
    let rows: [(usize, Vec<String>); 2] = [(0, vec!["1 34..34".into()]), (2, vec![reset])];
    for (early, expected) in rows {
        let now = Instant::now();
        let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
        let mut origin =
            RelayCore::new(&o, NoReplayTo("r"), CoreConfig { relay: false, ..scaled() }).expect("a valid config");
        let log = MemoryStore::new(MemoryLimits::default()).expect("valid limits");
        origin.set_history("md", Arc::new(Mutex::new(log)));
        origin.open_stream(&key(0), 1, DeliveryPolicy::ReplayRequired).expect("opens");
        origin.publish(&key(0), vec![Bytes::from_static(b"x"); 32]).expect("publishes");
        let mut relay = RelayCore::new(&r, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
        let follower = relay.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
        ask(&mut origin, &o, &mut relay, &r, now);
        origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
        deliver(&mut origin, &o, &mut relay, &r, now);
        assert_eq!(records(&mut relay, follower), vec![33]);
        origin.close_stream(&key(0)).expect("closes");
        deliver(&mut origin, &o, &mut relay, &r, now);
        origin
            .open_stream_after(&key(0), Cursor { generation: 1, sequence: 33 }, DeliveryPolicy::ReplayRequired)
            .expect("opens after");
        for _ in 0..early {
            origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
        }
        // Past any refusal backoff and the upstream timeout, twice: the relay asks, then gives up the position.
        let mut at = now;
        for _ in 0..4 {
            at += Duration::from_secs(60);
            ask(&mut origin, &o, &mut relay, &r, at);
            deliver(&mut origin, &o, &mut relay, &r, at);
        }
        origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
        deliver(&mut origin, &o, &mut relay, &r, at);
        assert_eq!(seen(&mut relay, follower), expected, "{early} records published before the relay asked again");
    }
}

/// A standby that takes a key up after its origin, from the history it keeps, opens it at the last record the history
/// holds, past what reached it as a relay. Its own followers behind the open replay the rest from that history and go
/// on live, each record once, a follower still replaying its start too. Before, both were reset as if the records
/// were lost.
#[test]
fn a_follower_on_the_standby_behind_the_open_replays_the_rest_from_the_history() {
    let now = Instant::now();
    let (o, s) = (PeerId::new("o", 1), PeerId::new("s", 1));
    let cfg = CoreConfig { relay: false, ..scaled() };
    let mut origin = RelayCore::new(&o, AllowAll, cfg).expect("a valid config");
    origin.open_stream(&key(0), 1, DeliveryPolicy::ReplayRequired).expect("opens");
    let mut standby = RelayCore::new(&s, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    // The history the standby keeps: every record the origin committed, ahead of what it relayed.
    let log = Arc::new(Mutex::new(MemoryStore::new(MemoryLimits::default()).expect("valid limits")));
    lock(&log).open(&key(0), 1).expect("opens");
    let batch = SequencedBatch::new(1, 1, vec![Bytes::from_static(b"x"); 20]).expect("a batch");
    lock(&log).append(&key(0), &batch).expect("appends");
    standby.set_history("md", log);
    let live = standby.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut standby, &s, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
    deliver(&mut origin, &o, &mut standby, &s, now);
    assert_eq!(records(&mut standby, live), (1..=10).collect::<Vec<_>>());
    // Subscribed from record 1 and not polled: it is still replaying its start from the ring.
    let start = StartPosition::FollowAfter(Cursor { generation: 1, sequence: 0 });
    let after = standby.subscribe(&key(0), SubscribeOptions { start, ..Default::default() }).expect("follows after");
    // Records 11 to 20 never reach the standby as a relay.
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
    let through = Cursor { generation: 1, sequence: 20 };
    standby.open_stream_after(&key(0), through, DeliveryPolicy::ReplayRequired).expect("opens after the history");
    standby.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    assert_eq!(records(&mut standby, live), (11..=21).collect::<Vec<_>>());
    assert_eq!(records(&mut standby, after), (1..=21).collect::<Vec<_>>());
    assert!(standby.is_subscribed(live) && standby.is_subscribed(after), "neither was reset");
}

/// A follower on the standby that may not replay is reset at the open, as it would be at a subscribe behind it: it
/// is not given the records it missed, nor anything after.
#[test]
fn a_follower_on_the_standby_behind_the_open_that_may_not_replay_is_reset() {
    // The standby's own subscriptions are `local:s` to its authorizer.
    let (mut standby, live) = a_standby_catching_up(NoReplayTo("local:s"));
    standby.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
    let reset = format!("{:?}", StreamEvent::ResetRequired(ResetReason::HistoryUnavailable));
    assert_eq!(seen(&mut standby, live), vec![reset]);
    assert!(!standby.is_subscribed(live));
}

/// A standby with a `Live` follower that read records 1 to 10 as a relay, opened after the 20 its history holds, and
/// the follower not polled since.
fn a_standby_catching_up<A: Authorizer>(authorizer: A) -> (RelayCore<A>, u64) {
    let now = Instant::now();
    let (o, s) = (PeerId::new("o", 1), PeerId::new("s", 1));
    let mut origin = origin(&scaled(), 1, 0);
    let cfg = CoreConfig { relay: true, ..scaled() };
    let mut standby = RelayCore::new(&s, authorizer, cfg).expect("a valid config");
    let log = Arc::new(Mutex::new(MemoryStore::new(MemoryLimits::default()).expect("valid limits")));
    lock(&log).open(&key(0), 1).expect("opens");
    let batch = SequencedBatch::new(1, 1, vec![Bytes::from_static(b"x"); 20]).expect("a batch");
    lock(&log).append(&key(0), &batch).expect("appends");
    standby.set_history("md", log);
    let live = standby.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut standby, &s, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
    deliver(&mut origin, &o, &mut standby, &s, now);
    assert_eq!(records(&mut standby, live), (1..=10).collect::<Vec<_>>());
    let through = Cursor { generation: 1, sequence: 20 };
    standby.open_stream_after(&key(0), through, DeliveryPolicy::ReplayRequired).expect("opens after the history");
    (standby, live)
}

/// A `Live` follower whose replay from the history stops at a new generation, a restart or a close and an open of
/// the next, follows the new generation from its first record, as any `Live` follower does. Its `CaughtUp`: the test
/// below.
#[test]
fn a_follower_whose_catch_up_stops_at_a_new_generation_follows_it_from_its_first_record() {
    let rows = ["restart", "close and open"].map(|row| {
        let (mut standby, live) = a_standby_catching_up(AllowAll);
        if row == "restart" {
            standby.restart_stream(&key(0), 2).expect("restarts");
        } else {
            standby.close_stream(&key(0)).expect("closes");
            standby.open_stream(&key(0), 2, DeliveryPolicy::ReplayRequired).expect("opens generation 2");
        }
        standby.publish(&key(0), vec![Bytes::from_static(b"x"); 20]).expect("publishes");
        let mut events = seen(&mut standby, live);
        events.retain(|e| !e.starts_with("CaughtUp"));
        (row, events, standby.is_subscribed(live))
    });
    assert_eq!(rows, [("restart", vec!["2 1..20".into()], true), ("close and open", vec!["2 1..20".into()], true)]);
}

/// A `Live` follower is told `CaughtUp` once for each catch-up, where it goes on live: at the end of its replay, or,
/// when a restart or a close stops the replay, at the last position it was given. Before, one whose replay stopped
/// was never told.
#[test]
fn a_follower_whose_catch_up_stops_is_told_it_caught_up_once_where_it_goes_on() {
    let caught_up = |sequence| format!("{:?}", StreamEvent::CaughtUp { through: Cursor { generation: 1, sequence } });
    let rows = ["finishes", "restart", "close and open"].map(|row| {
        let (mut standby, live) = a_standby_catching_up(AllowAll);
        let mut events = if row == "finishes" { seen(&mut standby, live) } else { Vec::new() };
        if row == "close and open" {
            standby.close_stream(&key(0)).expect("closes");
            standby.open_stream(&key(0), 2, DeliveryPolicy::ReplayRequired).expect("opens generation 2");
        } else {
            standby.restart_stream(&key(0), 2).expect("restarts");
        }
        standby.publish(&key(0), vec![Bytes::from_static(b"x"); 20]).expect("publishes");
        events.extend(seen(&mut standby, live));
        (row, events)
    });
    let new = "2 1..20".to_string();
    assert_eq!(
        rows,
        [
            ("finishes", vec!["1 11..20".into(), caught_up(20), new.clone()]),
            ("restart", vec![caught_up(10), new.clone()]),
            ("close and open", vec![caught_up(10), new]),
        ]
    );
}

/// `OriginLost` told to a follower still replaying its start waits behind the live records held behind the replay:
/// each record still comes once and in order.
#[test]
fn origin_lost_told_to_a_follower_still_replaying_comes_after_the_records_it_holds() {
    let now = Instant::now();
    let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
    let mut origin = origin(&scaled(), 1, 0);
    let mut relay = RelayCore::new(&r, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let live = relay.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut relay, &r, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
    deliver(&mut origin, &o, &mut relay, &r, now);
    let start = StartPosition::FollowAfter(Cursor { generation: 1, sequence: 0 });
    let after = relay.subscribe(&key(0), SubscribeOptions { start, ..Default::default() }).expect("follows after");
    for _ in 0..2 {
        origin.publish(&key(0), vec![Bytes::from_static(b"x")]).expect("publishes");
        deliver(&mut origin, &o, &mut relay, &r, now);
    }
    // No parent is left: once the key is orphaned, the relay tells its followers it lost the origin.
    for later in [60, 120] {
        relay.reconcile(&[], now + Duration::from_secs(later));
    }
    let lost = format!("{:?}", StreamEvent::OriginLost);
    assert_eq!(seen(&mut relay, live).last(), Some(&lost));
    let caught_up = format!("{:?}", StreamEvent::CaughtUp { through: Cursor { generation: 1, sequence: 10 } });
    assert_eq!(seen(&mut relay, after), vec!["1 1..10".into(), caught_up, "1 11..11".into(), "1 12..12".into(), lost]);
}

/// A relay whose follower was parked in the generation that closed asks for a subscriber that came after the reopen in
/// the next: the parent holds it, so that subscriber goes on, and neither is reset.
#[test]
fn a_subscriber_in_the_new_generation_goes_on_beside_a_follower_parked_in_the_old() {
    let now = Instant::now();
    let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
    let mut origin = origin(&scaled(), 1, 0);
    let mut relay = RelayCore::new(&r, AllowAll, CoreConfig { relay: true, ..scaled() }).expect("a valid config");
    let live = relay.subscribe(&key(0), SubscribeOptions::default()).expect("follows");
    ask(&mut origin, &o, &mut relay, &r, now);
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 10]).expect("publishes");
    deliver(&mut origin, &o, &mut relay, &r, now);
    assert_eq!(records(&mut relay, live), (1..=10).collect::<Vec<_>>());
    origin.close_stream(&key(0)).expect("closes");
    deliver(&mut origin, &o, &mut relay, &r, now);
    origin.open_stream(&key(0), 2, DeliveryPolicy::ReplayRequired).expect("opens generation 2");
    origin.publish(&key(0), vec![Bytes::from_static(b"x"); 20]).expect("publishes");
    let start = StartPosition::After(Cursor { generation: 2, sequence: 10 });
    let after = relay.subscribe(&key(0), SubscribeOptions { start, ..Default::default() }).expect("subscribes");
    let mut at = now;
    for _ in 0..4 {
        at += Duration::from_secs(60);
        ask(&mut origin, &o, &mut relay, &r, at);
        deliver(&mut origin, &o, &mut relay, &r, at);
    }
    let mut events = seen(&mut relay, after);
    events.retain(|e| !e.starts_with("CaughtUp"));
    assert_eq!(events, vec!["2 11..20"]);
    assert!(relay.is_subscribed(after) && relay.is_subscribed(live), "{:?}", seen(&mut relay, live));
}

/// A replay from the namespace's history, four branches long, goes to the child a window at a time: the branch is
/// never asked to hold more than it has room for, so it is not reset as a slow consumer.
#[test]
fn a_source_replay_larger_than_the_branch_limit_goes_through_the_push_window_without_a_slow_consumer_reset() {
    let (o, c, now, rtt) = (PeerId::new("o", 1), PeerId::new("c", 1), Instant::now(), Duration::from_millis(200));
    let mut cfg = scaled();
    cfg.branch = BranchLimits { max_bytes: 256 << 10, ..BranchLimits::BRANCH };
    cfg.rings = vec![RingLimits { namespace: "md".into(), max_records: 16, max_bytes: 16 * RECORD }];
    let records = 1024;
    let limits = MemoryLimits {
        max_records_per_stream: records,
        max_bytes_per_stream: 2 * records * RECORD,
        ..Default::default()
    };
    let log = Arc::new(Mutex::new(MemoryStore::new(limits).expect("valid limits")));
    lock(&log).open(&key(0), 1).expect("opens");
    let mut origin = RelayCore::new(&o, AllowAll, CoreConfig { relay: false, ..cfg.clone() }).expect("a valid config");
    origin.set_history("md", log.clone());
    origin.open_stream(&key(0), 1, DeliveryPolicy::ReplayRequired).expect("opens");
    let batch = vec![Bytes::from(vec![7u8; RECORD]); 16];
    for first in (1..=records as u64).step_by(16) {
        // Written to the log before it is published.
        lock(&log).append(&key(0), &SequencedBatch::new(1, first, batch.clone()).expect("a batch")).expect("appends");
        origin.publish(&key(0), batch.clone()).expect("publishes");
    }
    let mut child = RelayCore::new(&c, AllowAll, CoreConfig { relay: true, ..cfg }).expect("a valid config");
    let id = child.subscribe(&key(0), SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let id = id.expect("subscribes");
    ask(&mut origin, &o, &mut child, &c, now);
    let (mut at, mut seen) = (now, Vec::new());
    for _ in 0..64 {
        let mut answers = Vec::new();
        while let Some((push, body)) = origin.next_push("c", Lane::Live, at, Some(rtt)) {
            let (channel, seq, items) = wire::decode_push(&mut body.slice(1..)).expect("a push decodes");
            answers.push((push, child.handle_push(&o, Lane::Live, 1, channel, seq, items, at)));
        }
        at += rtt;
        for (push, answer) in answers {
            origin.push_answered(&c, Lane::Live, push, answer, at);
        }
        while let Some(event) = child.poll(id) {
            match event {
                StreamEvent::Batch(batch) => seen.extend(batch.first()..batch.first() + batch.len() as u64),
                StreamEvent::ResetRequired(_) | StreamEvent::End(_) => panic!("{event:?} after {} records", seen.len()),
                _ => {}
            }
        }
    }
    assert_eq!(seen, (1..=records as u64).collect::<Vec<_>>());
    let hub = origin.stats().hub;
    assert_eq!((hub.resets, hub.replayed_records), ([0; 6], records as u64));
}
