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
fn ask(parent: &mut RelayCore, p: &PeerId, child: &mut RelayCore, c: &PeerId, now: Instant) {
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

fn records(core: &mut RelayCore, id: u64) -> Vec<u64> {
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
    mut now: Instant,
    until: Instant,
    mut each: impl FnMut(&mut RelayCore, Instant),
) {
    let (mut flights, mut n) = (BTreeMap::new(), 0u64);
    loop {
        for (i, (c, rtt)) in children.iter().enumerate() {
            while let Some((id, _)) = origin.next_push(&c.node, Lane::Live, now, Some(*rtt)) {
                flights.insert((now + *rtt, n), (i, id));
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
        let want = ((2 * cfg.max_push_bytes) << round).min(cfg.max_push_window);
        assert!(*peak >= want * 7 / 8 && *peak <= want, "round {round}: {peak} bytes in flight, {want} wanted");
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
