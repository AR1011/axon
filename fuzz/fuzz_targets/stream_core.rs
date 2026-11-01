#![no_main]
//! `cargo +nightly fuzz run stream_core`. A relay with a subscriber on each of two namespaces takes whatever pushes, on
//! whatever lane, boot, channel and number, and interests a hostile parent sends, and whatever answers to its own pushes
//! a hostile child sends. Oracle: no panic; neither subscriber sees a position twice or a gap inside a generation
//! without a reset in between, nor a generation older than one it was shown; each push to the child on a lane carries
//! only that lane's namespaces; and once the child answers everything, nothing is left in flight to it. The top bit of
//! the first byte starts the subscribers from `First`: then their first position is 0 and their first record 1. The
//! next two put `md` and `bulk` on `Bulk`.
use std::time::{Duration, Instant};

use axon::Lane;
use axon_stream::*;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

/// What one subscriber was shown.
struct Seen {
    sub: u64,
    from_first: bool,
    last: Option<(u64, u64)>,
    newest: u64,
}

fuzz_target!(|data: &[u8]| {
    let keys = [StreamKey::new("md", "btc"), StreamKey::new("bulk", "dump")];
    let (parent, child) = (PeerId::new("p", 1), PeerId::new("c", 1));
    let first = data.first().copied().unwrap_or(0);
    let mut cfg = CoreConfig::default();
    if first & 0x40 != 0 {
        cfg.lanes.insert("md".into(), Lane::Bulk);
    }
    cfg.lanes.insert("bulk".into(), if first & 0x20 != 0 { Lane::Bulk } else { Lane::Live });
    let max_push = cfg.max_push_bytes;
    let mut core = RelayCore::new(&PeerId::new("r", 1), AllowAll, cfg.clone()).unwrap();
    let from_first = first & 0x80 != 0;
    let start = if from_first { StartPosition::First } else { StartPosition::Live };
    let mut seen = keys.clone().map(|key| {
        let sub = core.subscribe(&key, SubscribeOptions { start, ..Default::default() }).unwrap();
        Seen { sub, from_first, last: None, newest: 0 }
    });
    #[expect(clippy::disallowed_methods, reason = "an origin only; the input moves the clock from there")]
    let mut now = Instant::now();
    let mut epochs = [1; 2];
    for (to, interest) in core.reconcile(std::slice::from_ref(&parent), now) {
        for want in &interest.wants {
            epochs[usize::from(want.key == keys[1])] = want.epoch;
        }
        let answers = interest.wants.iter().map(|_| Status::Ok.into()).collect();
        core.interest_result(&to, interest.revision, Reply::Answers(answers), now);
    }

    let mut sent = Vec::new();
    for chunk in data.split(|b| *b == 0xA5) {
        let Some((which, rest)) = chunk.split_first() else { continue };
        let mut body = Bytes::copy_from_slice(rest);
        now += Duration::from_millis(*which as u64);
        let lane = if which & 8 != 0 { Lane::Bulk } else { Lane::Live };
        match which % 4 {
            0 => {
                if let Some((channel, seq, mut push)) = wire::decode_push(&mut body) {
                    // Aim what decodes at the streams that are open, or nearly nothing gets past the key check.
                    for (i, item) in push.items.iter_mut().enumerate() {
                        let k = (i + usize::from(*which >> 4)) % 2;
                        (item.0, item.1) = (keys[k].clone(), epochs[k]);
                    }
                    let _ = core.handle_push(&parent, lane, u128::from(which >> 6), channel, seq, push, now);
                }
            }
            1 => {
                if let Some(mut interest) = wire::decode_interest(&mut body) {
                    if which & 16 != 0 {
                        for (i, want) in interest.wants.iter_mut().enumerate() {
                            want.key = keys[i % 2].clone();
                        }
                    }
                    let _ = core.handle_interest(&child, interest, now);
                }
                let rtt = (which & 4 != 0).then_some(Duration::from_millis(u64::from(*which)));
                for lane in [Lane::Live, Lane::Bulk] {
                    while let Some((id, push)) = core.next_push("c", lane, now, rtt) {
                        assert!(push.len() <= max_push);
                        let (_, _, items) = wire::decode_push(&mut push.slice(1..)).expect("a push decodes");
                        for (key, _, _) in &items.items {
                            assert_eq!(cfg.lane(key), lane, "{key:?} pushed on {lane:?}");
                        }
                        sent.push((lane, id));
                    }
                }
            }
            3 => {
                // Each byte answers one push sent, in any order and any way.
                for b in rest {
                    if sent.is_empty() {
                        break;
                    }
                    let (lane, id) = sent.remove(usize::from(*b >> 2) % sent.len());
                    match b % 4 {
                        // Now and then one past it, which drops the child, or with its first item unwanted.
                        0 => {
                            let unwanted = if b & 64 != 0 { vec![0] } else { vec![] };
                            let applied = PushAnswer::Applied { applied: id.seq + u64::from(b >> 7), unwanted };
                            core.push_answered(&child, lane, id, applied, now);
                        }
                        1 => core.push_answered(
                            &child,
                            lane,
                            id,
                            PushAnswer::Skipped { applied: u64::from(*b >> 2) },
                            now,
                        ),
                        2 => core.push_failed(&child, lane, id, now),
                        _ => core.push_answered(&child, lane, id, PushAnswer::Retired, now),
                    }
                }
            }
            _ => {
                // A child silent for 2 s is dropped, as a revoked one would be; its next interest joins it again.
                core.expire_unheard(now, Duration::from_secs(2));
                drop(core.reconcile(std::slice::from_ref(&parent), now));
            }
        }
        for (key, s) in keys.iter().zip(&mut seen) {
            while let Some(event) = core.poll(s.sub) {
                match event {
                    StreamEvent::Ready { start: Some(start) } if s.from_first => {
                        assert_eq!(start.sequence, 0, "a First start began at {start:?}");
                        (s.from_first, s.last) = (false, Some((start.generation, 0)));
                    }
                    StreamEvent::Batch(batch) => {
                        assert!(batch.generation() >= s.newest, "back to generation {}", batch.generation());
                        s.newest = batch.generation();
                        if let Some((generation, sequence)) = s.last.filter(|(g, _)| *g == batch.generation()) {
                            assert_eq!(batch.first(), sequence + 1, "{key:?} generation {generation}");
                        }
                        s.last = Some((batch.generation(), batch.last()));
                    }
                    StreamEvent::ResetRequired(_) => {
                        (s.from_first, s.last) = (false, None);
                        s.sub = core.subscribe(key, SubscribeOptions::default()).unwrap();
                    }
                    StreamEvent::End(_) => return,
                    _ => {}
                }
            }
        }
    }
    // A child that answers each push in order, from the oldest it was not answered for.
    for (lane, id) in sent {
        core.push_answered(&child, lane, id, PushAnswer::Applied { applied: id.seq, unwanted: vec![] }, now);
    }
    for lane in [Lane::Live, Lane::Bulk] {
        for _ in 0..10_000 {
            let Some((id, _)) = core.next_push("c", lane, now, None) else { break };
            core.push_answered(&child, lane, id, PushAnswer::Applied { applied: id.seq, unwanted: vec![] }, now);
        }
    }
    for d in core.stats().downstream {
        assert_eq!((d.inflight_pushes, d.inflight_bytes), (0, 0), "left in flight to {} on {:?}", d.node, d.lane);
    }
});
