#![expect(clippy::disallowed_methods, reason = "Instant::now only gives the cores a time base; nothing waits on it")]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use axon::Lane;
use axon_stream::*;
use bytes::Bytes;

type Store = Arc<Mutex<MemoryStore>>;

fn me() -> PeerId {
    PeerId::new("local", 1)
}

fn key() -> StreamKey {
    StreamKey::new("md", "btc")
}

fn record(n: u64) -> Bytes {
    Bytes::copy_from_slice(&n.to_be_bytes())
}

fn hub() -> (StreamHub, Store) {
    let mut hub = StreamHub::new(AllowAll, HubLimits::default()).unwrap();
    hub.open_session(&me()).unwrap();
    hub.open_stream(&me(), &key(), 7, DeliveryPolicy::ReplayRequired).unwrap();
    let mut store = MemoryStore::new(MemoryLimits::default()).unwrap();
    store.open(&key(), 7).unwrap();
    (hub, Arc::new(Mutex::new(store)))
}

/// Records `first..first + n` of `generation`.
fn batch(generation: u64, first: u64, n: u64) -> SequencedBatch {
    SequencedBatch::new(generation, first, (first..first + n).map(record).collect::<Vec<_>>()).unwrap()
}

/// A hub that relays the key, and a store for its ring of generation 7.
fn relay_hub() -> (StreamHub, Store) {
    let mut hub = StreamHub::new(AllowAll, HubLimits::default()).unwrap();
    hub.open_session(&me()).unwrap();
    hub.open_relay_stream(&me(), &key(), DeliveryPolicy::ReplayRequired).unwrap();
    let mut store = MemoryStore::new(MemoryLimits::default()).unwrap();
    store.open(&key(), 7).unwrap();
    (hub, Arc::new(Mutex::new(store)))
}

/// Records `first..first + n` of generation 7 from the relay's parent, kept in its ring.
fn accept(hub: &mut StreamHub, store: &Store, first: u64, n: u64) {
    let batch = batch(7, first, n);
    store.lock().unwrap().append(&key(), &batch).unwrap();
    hub.accept_batch(&me(), &key(), batch).unwrap();
}

fn publish(hub: &mut StreamHub, store: &Store, range: std::ops::RangeInclusive<u64>, batch: usize) {
    let all: Vec<u64> = range.collect();
    for chunk in all.chunks(batch) {
        let batch = hub.publish(&me(), &key(), chunk.iter().map(|n| record(*n)).collect()).unwrap();
        store.lock().unwrap().append(&key(), &batch).unwrap();
    }
}

fn after(sequence: u64) -> SubscribeOptions {
    SubscribeOptions { start: StartPosition::After(Cursor { generation: 7, sequence }), ..Default::default() }
}

/// Drains a subscription, checking the event order, and returns the record numbers seen.
fn drain(hub: &mut StreamHub, id: u64, page: usize) -> (Vec<u64>, Option<StreamEvent>) {
    let (mut seen, mut next) = (Vec::new(), None::<u64>);
    loop {
        if hub.is_recovering(id) {
            hub.drive_recovery(id, ReadBounds { max_records: page, max_bytes: 1 << 20 }).unwrap();
        }
        let Some(event) = hub.poll(id) else {
            if hub.is_recovering(id) {
                continue;
            }
            return (seen, None);
        };
        match event {
            StreamEvent::Ready { .. } => {}
            StreamEvent::CaughtUp { .. } => assert!(!hub.is_recovering(id)),
            StreamEvent::Batch(batch) => {
                assert!(next.is_none_or(|n| n == batch.first()), "gap or duplicate");
                next = Some(batch.last() + 1);
                seen.extend(batch.records().iter().map(|r| u64::from_be_bytes(r[..].try_into().unwrap())));
            }
            terminal if terminal.is_terminal() => return (seen, Some(terminal)),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[test]
fn replay_joins_live_without_gaps_for_every_batch_and_page_size() {
    for batch in [1, 2, 3, 5, 8, 13, 40, 64, 100] {
        for page in [1, 2, 3, 7, 16, 50, 1000] {
            let (mut hub, store) = hub();
            publish(&mut hub, &store, 1..=60, batch);
            let id = hub.subscribe(&me(), &key(), after(10), Some(store.clone())).unwrap();
            publish(&mut hub, &store, 61..=90, batch);
            let (seen, end) = drain(&mut hub, id, page);
            assert_eq!(seen, (11..=90).collect::<Vec<_>>(), "batch {batch} page {page}");
            assert_eq!(end, None);
            hub.forget(id);
            assert_eq!(hub.total_queued(), (0, 0));
        }
    }
}

#[test]
fn live_batches_wait_behind_the_replay() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=4, 2);
    let id = hub.subscribe(&me(), &key(), after(0), Some(store.clone())).unwrap();
    publish(&mut hub, &store, 5..=6, 2);
    assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(Cursor { generation: 7, sequence: 0 }) }));
    assert_eq!(hub.poll(id), None, "the live batch is held until the replay is in front of it");
    assert_eq!(drain(&mut hub, id, 100).0, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn a_live_start_is_caught_up_at_once() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=3, 3);
    let id = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    let at = Cursor { generation: 7, sequence: 3 };
    assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(at) }));
    assert_eq!(hub.poll(id), Some(StreamEvent::CaughtUp { through: at }));
    publish(&mut hub, &store, 4..=4, 1);
    assert!(matches!(hub.poll(id), Some(StreamEvent::Batch(b)) if b.first() == 4));
}

#[test]
fn history_that_is_gone_is_refused_at_subscribe() {
    let mut store = MemoryStore::new(MemoryLimits { max_records_per_stream: 10, ..Default::default() }).unwrap();
    store.open(&key(), 7).unwrap();
    let (mut hub, _) = hub();
    let store = Arc::new(Mutex::new(store));
    publish(&mut hub, &store, 1..=30, 5);
    assert_eq!(store.lock().unwrap().available_range(&key()).unwrap().first, 21);
    let refused = hub.subscribe(&me(), &key(), after(5), Some(store.clone()));
    assert_eq!(refused, Err(StreamError::Recovery(RecoveryError::HistoryUnavailable)));
    assert_eq!(hub.subscribe(&me(), &key(), after(31), Some(store.clone())), refused);
    let wrong =
        SubscribeOptions { start: StartPosition::After(Cursor { generation: 8, sequence: 25 }), ..Default::default() };
    assert_eq!(
        hub.subscribe(&me(), &key(), wrong, Some(store.clone())),
        Err(StreamError::Recovery(RecoveryError::GenerationMismatch))
    );
    let id = hub.subscribe(&me(), &key(), after(20), Some(store)).unwrap();
    assert_eq!(drain(&mut hub, id, 4).0, (21..=30).collect::<Vec<_>>());
}

#[test]
fn a_slow_branch_ends_alone() {
    let (mut hub, store) = hub();
    let tight = BranchLimits { max_records: 4, ..Default::default() };
    let reset = hub.subscribe(&me(), &key(), SubscribeOptions { limits: tight, ..Default::default() }, None).unwrap();
    let detach = SubscribeOptions { limits: tight, slow_consumer: SlowConsumerPolicy::Detach, ..Default::default() };
    let detach = hub.subscribe(&me(), &key(), detach, None).unwrap();
    let healthy = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    publish(&mut hub, &store, 1..=10, 2);
    assert_eq!(drain(&mut hub, reset, 1), (vec![], Some(StreamEvent::ResetRequired(ResetReason::SlowConsumer))));
    assert_eq!(drain(&mut hub, detach, 1), (vec![], Some(StreamEvent::End(EndReason::SlowConsumer))));
    assert_eq!(drain(&mut hub, healthy, 1).0, (1..=10).collect::<Vec<_>>());
    assert_eq!(hub.total_queued(), (0, 0));
    assert_eq!(hub.demand_count(&key()), 1);
    let stats = hub.stats();
    assert_eq!(
        (stats.resets[ResetReason::SlowConsumer as usize], stats.ends[EndReason::SlowConsumer as usize]),
        (1, 1)
    );
    assert_eq!((stats.published_records, stats.delivered_records), (10, 10));
}

#[test]
fn a_replaceable_stream_coalesces_to_the_latest() {
    let mut hub = StreamHub::new(AllowAll, HubLimits::default()).unwrap();
    hub.open_session(&me()).unwrap();
    hub.open_stream(&me(), &key(), 1, DeliveryPolicy::ReplaceableState).unwrap();
    let two = hub.publish(&me(), &key(), vec![record(1), record(2)]).unwrap_err();
    assert_eq!(two.error, StreamError::Recovery(RecoveryError::InvalidBatch));
    let options = SubscribeOptions {
        slow_consumer: SlowConsumerPolicy::CoalesceLatest,
        limits: BranchLimits { max_events: 2, ..Default::default() },
        ..Default::default()
    };
    let id = hub.subscribe(&me(), &key(), options, None).unwrap();
    for n in 1..=5 {
        hub.publish(&me(), &key(), vec![record(n)]).unwrap();
    }
    let events: Vec<_> = std::iter::from_fn(|| hub.poll(id)).collect();
    assert!(
        matches!(&events[..], [StreamEvent::Ready { .. }, StreamEvent::CaughtUp { .. }, StreamEvent::Replacement { batch }] if batch.first() == 5)
    );
    assert_eq!(hub.total_queued(), (0, 0));
    hub.open_stream(&me(), &StreamKey::new("md", "eth"), 1, DeliveryPolicy::ReplayRequired).unwrap();
    assert_eq!(hub.subscribe(&me(), &StreamKey::new("md", "eth"), options, None), Err(StreamError::InvalidOptions));
}

#[test]
fn a_rejected_publish_returns_the_callers_records() {
    let (mut hub, _) = hub();
    let records = vec![record(1)];
    let pointer = records.as_ptr();
    let rejected = hub.publish(&me(), &StreamKey::new("md", "none"), records).unwrap_err();
    assert_eq!((rejected.error, rejected.returned.as_ptr()), (StreamError::UnknownStream, pointer));
    let stale = hub.publish(&PeerId::new("local", 2), &key(), vec![record(1)]).unwrap_err();
    assert_eq!(stale.error, StreamError::StaleSession);
    assert_eq!(hub.stats().publish_rejected, 2);
}

#[test]
fn a_relay_stream_adopts_positions_drops_overlap_and_refuses_gaps() {
    let (mut hub, _) = relay_hub();
    let id = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    assert_eq!(hub.cursor(&key()), Ok(None));
    hub.accept_batch(&me(), &key(), batch(9, 501, 3)).unwrap();
    assert_eq!(hub.cursor(&key()), Ok(Some(Cursor { generation: 9, sequence: 503 })));
    assert_eq!(hub.accept_batch(&me(), &key(), batch(9, 501, 3)).unwrap(), None);
    let fresh = hub.accept_batch(&me(), &key(), batch(9, 502, 4)).unwrap().unwrap();
    assert_eq!((fresh.first(), fresh.last()), (504, 505));
    let gap = hub.accept_batch(&me(), &key(), batch(9, 510, 1)).unwrap_err();
    assert_eq!(gap.error, StreamError::Recovery(RecoveryError::Gap { expected: 506, actual: 510 }));
    hub.accept_batch(&me(), &key(), gap.returned.clone()).unwrap_err();
    hub.accept_batch(&me(), &key(), batch(9, 506, 4)).unwrap();
    hub.accept_batch(&me(), &key(), gap.returned).unwrap();
    assert_eq!(drain(&mut hub, id, 1).0, (501..=510).collect::<Vec<_>>());
    assert_eq!(hub.stats().duplicate_records, 3 + 2);

    hub.reset_relay_stream(&me(), &key(), ResetReason::GenerationChanged).unwrap();
    assert_eq!(hub.poll(id), Some(StreamEvent::ResetRequired(ResetReason::GenerationChanged)));
    hub.accept_batch(&me(), &key(), batch(10, 1, 1)).unwrap();
    assert_eq!(hub.cursor(&key()), Ok(Some(Cursor { generation: 10, sequence: 1 })));

    // The last position there is cannot be used, so the one after a record always exists: in the hub and the store.
    assert_eq!(SequencedBatch::new(11, u64::MAX, vec![record(0)]).unwrap_err(), RecoveryError::PositionOverflow);
    assert_eq!(SequencedBatch::new(11, u64::MAX - 2, vec![record(0); 3]).unwrap_err(), RecoveryError::PositionOverflow);
    hub.reset_relay_stream(&me(), &key(), ResetReason::GenerationChanged).unwrap();
    let last = || SequencedBatch::new(11, u64::MAX - 1, vec![record(0)]).unwrap();
    hub.accept_batch(&me(), &key(), last()).unwrap();
    assert_eq!(hub.accept_batch(&me(), &key(), last()).unwrap(), None);
    assert_eq!(hub.cursor(&key()), Ok(Some(Cursor { generation: 11, sequence: u64::MAX - 1 })));
    let mut store = MemoryStore::new(MemoryLimits::default()).unwrap();
    store.open_at(&key(), Cursor { generation: 11, sequence: u64::MAX - 2 }).unwrap();
    store.append(&key(), &last()).unwrap();
    assert_eq!(store.available_range(&key()).unwrap().last, u64::MAX - 1);
}

/// A relay that gave up its position (`HistoryUnavailable`) moves a `Live` follower on to a newer generation with a
/// `Ready`, and resets it when the same generation comes back further on, however many resets came between: one that
/// subscribed after the give-up had nothing to lose.
#[test]
fn a_relay_that_lost_its_position_resets_a_follower_rather_than_move_it_within_the_generation() {
    let at = |generation, sequence| Cursor { generation, sequence };
    let lost = |hub: &mut StreamHub| hub.reset_relay_stream(&me(), &key(), ResetReason::HistoryUnavailable).unwrap();
    let (mut hub, store) = relay_hub();
    let id = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(7, 0)).unwrap();
    accept(&mut hub, &store, 1, 3);
    assert_eq!(drain(&mut hub, id, 8).0, [1, 2, 3]);
    lost(&mut hub);
    lost(&mut hub);
    let late = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(7, 10)).unwrap();
    assert_eq!(drain(&mut hub, id, 8), (vec![], Some(StreamEvent::ResetRequired(ResetReason::HistoryUnavailable))));
    let events: Vec<_> = std::iter::from_fn(|| hub.poll(late)).collect();
    assert_eq!(events.last(), Some(&StreamEvent::Ready { start: Some(at(7, 10)) }), "{events:?}");

    let (mut hub, store) = relay_hub();
    let id = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(7, 0)).unwrap();
    accept(&mut hub, &store, 1, 3);
    drain(&mut hub, id, 8);
    lost(&mut hub);
    hub.adopt_relay_stream(&me(), &key(), at(8, 0)).unwrap();
    let events: Vec<_> = std::iter::from_fn(|| hub.poll(id)).collect();
    assert_eq!(events.last(), Some(&StreamEvent::Ready { start: Some(at(8, 0)) }), "{events:?}");

    // A position taken from a batch, with no Ready before it, is held to the same; one at the position given up lost
    // nothing.
    let (mut hub, store) = relay_hub();
    let id = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(7, 0)).unwrap();
    accept(&mut hub, &store, 1, 3);
    drain(&mut hub, id, 8);
    lost(&mut hub);
    hub.accept_batch(&me(), &key(), batch(7, 11, 2)).unwrap();
    assert_eq!(drain(&mut hub, id, 8), (vec![], Some(StreamEvent::ResetRequired(ResetReason::HistoryUnavailable))));
    let (mut hub, store) = relay_hub();
    let id = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(7, 0)).unwrap();
    accept(&mut hub, &store, 1, 3);
    drain(&mut hub, id, 8);
    lost(&mut hub);
    hub.adopt_relay_stream(&me(), &key(), at(7, 3)).unwrap();
    accept(&mut hub, &store, 4, 1);
    assert_eq!(drain(&mut hub, id, 8), (vec![4], None));
}

/// The follower a lost position would skip ends alone, with what it had queued: the stream and a follower that came
/// after the give-up carry on.
#[test]
fn a_follower_reset_for_a_lost_position_ends_alone_and_gives_back_its_queue() {
    let at = |sequence| Cursor { generation: 7, sequence };
    let (mut hub, store) = relay_hub();
    let skips = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(0)).unwrap();
    accept(&mut hub, &store, 1, 3);
    assert_eq!(drain(&mut hub, skips, 8).0, [1, 2, 3]);
    // Still queued when it is reset.
    accept(&mut hub, &store, 4, 2);
    hub.reset_relay_stream(&me(), &key(), ResetReason::HistoryUnavailable).unwrap();
    let late = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();

    hub.adopt_relay_stream(&me(), &key(), at(10)).unwrap();
    hub.accept_batch(&me(), &key(), batch(7, 11, 3)).unwrap();
    let reset = Some(StreamEvent::ResetRequired(ResetReason::HistoryUnavailable));
    assert_eq!(drain(&mut hub, skips, 8).1, reset);
    assert_eq!((hub.poll(skips), hub.stats().subscriptions), (None, 1));
    assert_eq!(hub.total_queued().1, 3, "the late follower's, and none of the reset one's");
    assert_eq!(drain(&mut hub, late, 8), (vec![11, 12, 13], None));
    hub.accept_batch(&me(), &key(), batch(7, 14, 1)).unwrap();
    assert_eq!(drain(&mut hub, late, 8), (vec![14], None));
    assert_eq!((hub.cursor(&key()), hub.demand_count(&key())), (Ok(Some(at(14))), 1));
    assert_eq!(hub.total_queued(), (0, 0));
}

#[test]
fn a_cursor_subscription_on_a_relay_stream_without_a_position_waits_for_it() {
    let at = |sequence| Cursor { generation: 7, sequence };
    let from = |start| SubscribeOptions { start, ..Default::default() };
    let joins = |hub: &mut StreamHub, id, after| {
        assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(at(after)) }));
        assert_eq!(hub.poll(id), Some(StreamEvent::CaughtUp { through: at(after) }));
    };

    // Given the cursor's own position: live from there.
    let mut hub = relay_hub().0;
    let id = hub.subscribe(&me(), &key(), after(10), None).unwrap();
    assert_eq!(hub.poll(id), None, "nothing before the stream has a position");
    hub.adopt_relay_stream(&me(), &key(), at(10)).unwrap();
    hub.accept_batch(&me(), &key(), batch(7, 11, 3)).unwrap();
    joins(&mut hub, id, 10);
    assert_eq!(drain(&mut hub, id, 1).0, vec![11, 12, 13]);

    // Given an earlier one: it waits for the stream to reach the cursor, and what came before is not seen.
    let mut hub = relay_hub().0;
    let id = hub.subscribe(&me(), &key(), after(10), None).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(4)).unwrap();
    hub.accept_batch(&me(), &key(), batch(7, 5, 4)).unwrap();
    assert_eq!(hub.poll(id), None);
    hub.accept_batch(&me(), &key(), batch(7, 9, 4)).unwrap();
    joins(&mut hub, id, 10);
    assert_eq!(drain(&mut hub, id, 1).0, vec![11, 12]);

    // `First` is record 1 of the generation the stream is given, and a first batch positions it as well.
    let mut hub = relay_hub().0;
    let id = hub.subscribe(&me(), &key(), from(StartPosition::First), None).unwrap();
    hub.accept_batch(&me(), &key(), batch(7, 1, 2)).unwrap();
    joins(&mut hub, id, 0);
    assert_eq!(drain(&mut hub, id, 1).0, vec![1, 2]);

    // A stream that starts past the position, or in another generation, is an explicit reset.
    let mut hub = relay_hub().0;
    let late = hub.subscribe(&me(), &key(), after(10), None).unwrap();
    let first = hub.subscribe(&me(), &key(), from(StartPosition::First), None).unwrap();
    let older = from(StartPosition::After(Cursor { generation: 6, sequence: 20 }));
    let other = hub.subscribe(&me(), &key(), older, None).unwrap();
    let live = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    hub.adopt_relay_stream(&me(), &key(), at(11)).unwrap();
    assert_eq!(hub.poll(late), Some(StreamEvent::ResetRequired(ResetReason::HistoryUnavailable)));
    assert_eq!(hub.poll(first), Some(StreamEvent::ResetRequired(ResetReason::HistoryUnavailable)));
    assert_eq!(hub.poll(other), Some(StreamEvent::ResetRequired(ResetReason::GenerationChanged)));
    assert_eq!(hub.poll(live), Some(StreamEvent::Ready { start: None }));
    assert_eq!(hub.demand_count(&key()), 1);
    hub.forget(live);
    assert_eq!((hub.total_queued(), hub.stats().subscriptions), ((0, 0), 0));
}

#[test]
fn a_follower_sees_each_restart_as_a_ready_and_waits_through_a_close() {
    let (mut hub, _) = hub();
    let publish = |hub: &mut StreamHub, n| hub.publish(&me(), &key(), vec![record(n)]).unwrap();
    let cursor = |generation, sequence| Some(Cursor { generation, sequence });
    let ready = |generation| Some(StreamEvent::Ready { start: cursor(generation, 0) });
    assert_eq!(hub.follow(&me(), &key(), after(0)), Err(StreamError::InvalidOptions));
    let f = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    publish(&mut hub, 1);
    publish(&mut hub, 2);
    hub.restart_stream(&me(), &key(), 8).unwrap();
    publish(&mut hub, 3);
    assert_eq!(hub.poll(f), ready(7));
    assert_eq!(hub.poll(f), Some(StreamEvent::CaughtUp { through: Cursor { generation: 7, sequence: 0 } }));
    assert!(matches!(hub.poll(f), Some(StreamEvent::Batch(b)) if b.generation() == 7 && b.first() == 1));
    assert!(matches!(hub.poll(f), Some(StreamEvent::Batch(b)) if b.generation() == 7 && b.first() == 2));
    assert_eq!(hub.poll(f), ready(8));
    assert!(matches!(hub.poll(f), Some(StreamEvent::Batch(b)) if b.generation() == 8 && b.first() == 1));

    hub.close_stream(&me(), &key(), EndReason::StreamClosed).unwrap();
    assert_eq!((hub.poll(f), hub.has_stream(&key()), hub.demand_count(&key())), (None, false, 1));
    assert!(hub.contains(f), "parked, not ended");
    hub.open_stream(&me(), &key(), 9, DeliveryPolicy::ReplayRequired).unwrap();
    publish(&mut hub, 4);
    assert_eq!(hub.poll(f), ready(9));
    assert!(matches!(hub.poll(f), Some(StreamEvent::Batch(b)) if b.generation() == 9 && b.first() == 1));
    hub.unsubscribe(f);
    assert_eq!(hub.poll(f), Some(StreamEvent::End(EndReason::Unsubscribed)));
    assert_eq!((hub.poll(f), hub.demand_count(&key())), (None, 0));
}

#[test]
fn a_follower_that_leaves_while_its_key_is_closed_stays_gone_when_it_reopens() {
    let (mut hub, _) = hub();
    let gone = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    let kept = hub.follow(&me(), &key(), SubscribeOptions::default()).unwrap();
    hub.close_stream(&me(), &key(), EndReason::StreamClosed).unwrap();
    hub.forget(gone);
    assert_eq!(hub.demand_count(&key()), 1);
    hub.open_stream(&me(), &key(), 9, DeliveryPolicy::ReplayRequired).unwrap();
    hub.publish(&me(), &key(), vec![record(1)]).unwrap();
    assert_eq!(hub.poll(gone), None);
    while let Some(event) = hub.poll(kept) {
        if matches!(event, StreamEvent::Batch(_)) {
            return;
        }
    }
    panic!("the follower still parked was not revived");
}

#[test]
fn restart_and_close_end_every_subscription_that_does_not_follow() {
    let (mut hub, _) = hub();
    let a = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    for stale in [7, 6] {
        assert_eq!(
            hub.restart_stream(&me(), &key(), stale),
            Err(StreamError::Recovery(RecoveryError::GenerationMismatch))
        );
    }
    hub.restart_stream(&me(), &key(), 8).unwrap();
    assert_eq!(drain(&mut hub, a, 1).1, Some(StreamEvent::ResetRequired(ResetReason::GenerationChanged)));
    assert_eq!(hub.poll(a), None);
    let b = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    hub.close_stream(&me(), &key(), EndReason::StreamClosed).unwrap();
    assert_eq!(drain(&mut hub, b, 1).1, Some(StreamEvent::End(EndReason::StreamClosed)));
    assert!(!hub.has_stream(&key()));
    assert_eq!(hub.demand_count(&key()), 0);
    hub.open_stream(&me(), &key(), 9, DeliveryPolicy::ReplayRequired).unwrap();
}

struct Deny(Mutex<BTreeSet<String>>);

impl Authorizer for Deny {
    fn authorize(&self, peer: &PeerId, _: &StreamKey, _: Access) -> bool {
        !self.0.lock().unwrap().contains(&*peer.node)
    }
}

#[test]
fn revocation_ends_subscriptions_and_drops_interest() {
    let mut hub = StreamHub::new(Deny(Mutex::default()), HubLimits::default()).unwrap();
    let (alice, bob) = (PeerId::new("alice", 1), PeerId::new("bob", 1));
    for s in [&me(), &alice, &bob] {
        hub.open_session(s).unwrap();
    }
    hub.open_stream(&me(), &key(), 1, DeliveryPolicy::ReplayRequired).unwrap();
    let a = hub.subscribe(&alice, &key(), SubscribeOptions::default(), None).unwrap();
    let b = hub.subscribe(&bob, &key(), SubscribeOptions::default(), None).unwrap();
    hub.replace_interests(&alice, 1, BTreeSet::from([key()])).unwrap();
    assert_eq!(hub.demand_count(&key()), 3);
    hub.update_authorizer(|deny| {
        deny.0.lock().unwrap().insert("alice".into());
    });
    assert_eq!(hub.demand_count(&key()), 1);
    assert_eq!(std::iter::from_fn(|| hub.poll(a)).last(), Some(StreamEvent::End(EndReason::Revoked)));
    hub.publish(&me(), &key(), vec![record(1)]).unwrap();
    assert!(std::iter::from_fn(|| hub.poll(b)).any(|e| matches!(e, StreamEvent::Batch(_))));
    assert_eq!(hub.subscribe(&alice, &key(), SubscribeOptions::default(), None), Err(StreamError::Forbidden));
}

#[test]
fn a_replaced_session_loses_its_subscriptions_and_cannot_act() {
    let (mut hub, _) = hub();
    let (old, new) = (PeerId::new("peer", 1), PeerId::new("peer", 2));
    hub.open_session(&old).unwrap();
    let id = hub.subscribe(&old, &key(), SubscribeOptions::default(), None).unwrap();
    hub.replace_interests(&old, 4, BTreeSet::from([key()])).unwrap();
    hub.open_session(&new).unwrap();
    assert_eq!(std::iter::from_fn(|| hub.poll(id)).last(), Some(StreamEvent::End(EndReason::SessionReplaced)));
    assert_eq!(hub.demand_count(&key()), 0);
    assert_eq!(hub.replace_interests(&old, 5, BTreeSet::new()), Err(StreamError::StaleSession));
    assert_eq!(hub.subscribe(&old, &key(), SubscribeOptions::default(), None), Err(StreamError::StaleSession));
}

#[test]
fn interest_demand_matches_a_model_under_random_operations() {
    let keys: Vec<StreamKey> = (0..4).map(|n| StreamKey::new("md", format!("k{n}"))).collect();
    for seed in 0..64u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let mut hub = StreamHub::new(AllowAll, HubLimits::default()).unwrap();
        let mut model: Vec<Option<(u128, u64, BTreeSet<StreamKey>)>> = vec![None; 4];
        for _ in 0..200 {
            let peer = next(4) as usize;
            let name = format!("p{peer}");
            match next(4) {
                0 => {
                    let incarnation = model[peer].as_ref().map_or(1, |m| m.0 + 1);
                    hub.open_session(&PeerId::new(&name, incarnation)).unwrap();
                    model[peer] = Some((incarnation, 0, BTreeSet::new()));
                }
                1 => {
                    if let Some((incarnation, _, _)) = model[peer].take() {
                        hub.expire_session(&PeerId::new(&name, incarnation)).unwrap();
                    }
                }
                2 => {
                    let Some((incarnation, revision, set)) = &mut model[peer] else { continue };
                    let wanted: BTreeSet<_> = keys.iter().filter(|_| next(2) == 0).cloned().collect();
                    *revision += 1;
                    hub.replace_interests(&PeerId::new(&name, *incarnation), *revision, wanted.clone()).unwrap();
                    *set = wanted;
                }
                _ => {
                    let Some((incarnation, revision, _)) = &model[peer] else { continue };
                    let stale = PeerId::new(&name, incarnation.wrapping_sub(1));
                    assert_eq!(
                        hub.replace_interests(&stale, revision + 9, BTreeSet::new()),
                        Err(StreamError::StaleSession)
                    );
                    if *revision > 1 {
                        let late =
                            hub.replace_interests(&PeerId::new(&name, *incarnation), revision - 1, BTreeSet::new());
                        assert_eq!(late, Err(StreamError::StaleRevision));
                    }
                }
            }
            for key in &keys {
                let expected = model.iter().flatten().filter(|m| m.2.contains(key)).count();
                assert_eq!(hub.demand_count(key), expected, "seed {seed}");
            }
        }
    }
}

#[test]
fn recovery_blocks_on_a_full_branch_and_resumes_after_polling() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=20, 5);
    let options = SubscribeOptions { limits: BranchLimits { max_records: 6, ..Default::default() }, ..after(0) };
    let id = hub.subscribe(&me(), &key(), options, Some(store.clone())).unwrap();
    let bounds = ReadBounds { max_records: 100, max_bytes: 1 << 20 };
    assert!(matches!(hub.drive_recovery(id, bounds), Ok(RecoveryProgress::Advanced(c)) if c.sequence == 6));
    assert_eq!(hub.drive_recovery(id, bounds), Ok(RecoveryProgress::Blocked));
    assert_eq!(drain(&mut hub, id, 100).0, (1..=20).collect::<Vec<_>>());
    assert_eq!(hub.drive_recovery(id, bounds), Ok(RecoveryProgress::Live));
    assert!(hub.stats().recovery_blocked >= 1);
}

#[test]
fn recovery_resets_rather_than_deadlock_when_the_global_pool_is_held_by_live_tails() {
    let limits = HubLimits { max_queued_records: 8, ..Default::default() };
    let mut hub = StreamHub::new(AllowAll, limits).unwrap();
    hub.open_session(&me()).unwrap();
    hub.open_stream(&me(), &key(), 7, DeliveryPolicy::ReplayRequired).unwrap();
    let mut store = MemoryStore::new(MemoryLimits::default()).unwrap();
    store.open(&key(), 7).unwrap();
    let store = Arc::new(Mutex::new(store));
    publish(&mut hub, &store, 1..=4, 4);
    let id = hub.subscribe(&me(), &key(), after(0), Some(store.clone())).unwrap();
    assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(Cursor { generation: 7, sequence: 0 }) }));
    publish(&mut hub, &store, 5..=12, 4);
    assert_eq!(
        hub.drive_recovery(id, ReadBounds { max_records: 4, max_bytes: 1 << 20 }),
        Ok(RecoveryProgress::Blocked)
    );
    assert_eq!(hub.poll(id), Some(StreamEvent::ResetRequired(ResetReason::GlobalCapacity)));
    assert_eq!(hub.total_queued(), (0, 0));
}

struct Lying(Store);

impl RecoverySource for Lying {
    fn available_range(&self, key: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        self.0.available_range(key)
    }

    fn read_after(
        &self,
        key: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        let after = Cursor { sequence: after.sequence + 1, ..after };
        self.0.read_after(key, after, through, bounds)
    }
}

#[test]
fn a_source_that_skips_records_is_a_reset_not_a_gap() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=10, 5);
    let id = hub.subscribe(&me(), &key(), after(2), Some(Arc::new(Lying(store)))).unwrap();
    assert_eq!(drain(&mut hub, id, 3), (vec![], Some(StreamEvent::ResetRequired(ResetReason::Gap))));
}

struct NoRange;

impl RecoverySource for NoRange {
    fn available_range(&self, _: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        Err(RecoveryError::Adapter("down".into()))
    }

    fn read_after(
        &self,
        _: &StreamKey,
        _: Cursor,
        _: u64,
        _: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        Err(RecoveryError::Adapter("down".into()))
    }
}

/// A stream without compaction has no state: its snapshot is empty at record 0 and the replay is its whole history.
#[test]
fn a_subscribe_that_fails_after_its_snapshot_charges_nothing() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=10, 5);
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    assert!(hub.subscribe(&me(), &key(), options, Some(Arc::new(NoRange))).is_err());
    assert_eq!(hub.total_queued(), (0, 0));
    let id = hub.subscribe(&me(), &key(), options, Some(store.clone())).unwrap();
    assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(Cursor { generation: 7, sequence: 0 }) }));
    let empty = Snapshot::new(Cursor { generation: 7, sequence: 0 }, Vec::new());
    assert_eq!(hub.poll(id), Some(StreamEvent::Snapshot(empty)));
    publish(&mut hub, &store, 11..=12, 2);
    assert_eq!(drain(&mut hub, id, 3), ((1..=12).collect(), None));
    assert_eq!(hub.total_queued(), (0, 0));
}

/// A policy with a bug: it panics on the key `poison` while `panics` is set.
struct Buggy {
    panics: bool,
}

impl Authorizer for Buggy {
    fn authorize(&self, _: &PeerId, key: &StreamKey, _: Access) -> bool {
        assert!(!(self.panics && key.name() == "poison"), "a policy bug");
        true
    }
}

fn panics(f: impl FnOnce()) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err()
}

#[test]
fn a_policy_that_panics_leaves_the_hub_as_it_was_and_serving() {
    let peer = PeerId::new("peer", 1);
    let poison = StreamKey::new("md", "poison");
    let mut hub = StreamHub::new(Buggy { panics: false }, HubLimits::default()).unwrap();
    hub.open_session(&me()).unwrap();
    hub.open_session(&peer).unwrap();
    for k in [key(), poison.clone()] {
        hub.open_stream(&me(), &k, 7, DeliveryPolicy::ReplayRequired).unwrap();
    }
    let live = hub.subscribe(&me(), &key(), SubscribeOptions::default(), None).unwrap();
    hub.subscribe(&me(), &poison, SubscribeOptions::default(), None).unwrap();
    hub.replace_interests(&peer, 1, [key(), poison.clone()].into()).unwrap();

    assert!(panics(|| hub.update_authorizer(|policy| policy.panics = true)));
    assert!(panics(|| drop(hub.subscribe(&me(), &poison, SubscribeOptions::default(), None))));
    assert!(panics(|| drop(hub.replace_interests(&peer, 2, [poison.clone()].into()))));
    let stats = hub.stats();
    assert_eq!((stats.subscriptions, hub.demand_count(&key()), hub.demand_count(&poison)), (2, 2, 2));

    hub.update_authorizer(|policy| policy.panics = false);
    hub.publish(&me(), &key(), vec![record(1)]).unwrap();
    assert_eq!(hub.poll(live), Some(StreamEvent::Ready { start: Some(Cursor { generation: 7, sequence: 0 }) }));
    assert_eq!(hub.poll(live), Some(StreamEvent::CaughtUp { through: Cursor { generation: 7, sequence: 0 } }));
    assert!(matches!(hub.poll(live), Some(StreamEvent::Batch(b)) if b.first() == 1));
    hub.replace_interests(&peer, 2, BTreeSet::new()).unwrap();
    for id in [live, live + 1] {
        hub.forget(id);
    }
    assert_eq!((hub.stats().subscriptions, hub.demand_count(&key()), hub.demand_count(&poison)), (0, 0, 0));
    assert_eq!(hub.total_queued(), (0, 0));
}

/// A recovery source with a bug: it panics where `at` says.
struct Exploding {
    store: Store,
    at: &'static str,
}

impl RecoverySource for Exploding {
    fn available_range(&self, key: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        assert_ne!(self.at, "range", "a source bug");
        self.store.available_range(key)
    }

    fn read_after(
        &self,
        key: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        assert_ne!(self.at, "read", "a source bug");
        self.store.read_after(key, after, through, bounds)
    }
}

#[test]
fn a_recovery_source_that_panics_charges_nothing_and_a_sound_one_replays_after_it() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=10, 5);
    let at_range = Arc::new(Exploding { store: store.clone(), at: "range" });
    assert!(panics(|| drop(hub.subscribe(&me(), &key(), after(2), Some(at_range)))));
    assert_eq!((hub.stats().subscriptions, hub.demand_count(&key())), (0, 0));

    let at_read = Arc::new(Exploding { store: store.clone(), at: "read" });
    let id = hub.subscribe(&me(), &key(), after(2), Some(at_read)).unwrap();
    let bounds = ReadBounds { max_records: 100, max_bytes: 1 << 20 };
    assert!(panics(|| drop(hub.drive_recovery(id, bounds))));
    assert!(hub.is_recovering(id));
    hub.forget(id);
    assert_eq!((hub.stats().subscriptions, hub.demand_count(&key()), hub.total_queued()), (0, 0, (0, 0)));

    let id = hub.subscribe(&me(), &key(), after(2), Some(store.clone())).unwrap();
    publish(&mut hub, &store, 11..=12, 1);
    assert_eq!(drain(&mut hub, id, 3), ((3..=12).collect(), None));
}

#[test]
fn a_record_larger_than_the_branch_resets_the_replay_instead_of_hanging() {
    let (mut hub, store) = hub();
    publish(&mut hub, &store, 1..=2, 1);
    let options = SubscribeOptions { limits: BranchLimits { max_bytes: 4, ..Default::default() }, ..after(0) };
    let id = hub.subscribe(&me(), &key(), options, Some(store)).unwrap();
    assert!(hub.drive_recovery(id, ReadBounds { max_records: usize::MAX, max_bytes: usize::MAX }).is_err());
    assert_eq!(hub.poll(id), Some(StreamEvent::ResetRequired(ResetReason::SlowConsumer)));
}

#[test]
fn the_memory_store_checks_overlap_and_evicts_from_the_front() {
    let mut store = MemoryStore::new(MemoryLimits { max_records_per_stream: 4, ..Default::default() }).unwrap();
    store.open(&key(), 1).unwrap();
    store.append(&key(), &batch(1, 1, 3)).unwrap();
    assert_eq!(store.append(&key(), &batch(1, 5, 1)), Err(RecoveryError::Gap { expected: 4, actual: 5 }));
    let forged = SequencedBatch::new(1, 3, vec![record(99), record(4)]).unwrap();
    assert_eq!(store.append(&key(), &forged), Err(RecoveryError::ConflictingDuplicate));
    store.append(&key(), &batch(1, 2, 3)).unwrap();
    store.append(&key(), &batch(1, 4, 3)).unwrap();
    store.append(&key(), &batch(1, 1, 6)).unwrap();
    assert_eq!(store.available_range(&key()).unwrap(), ReplayRange { generation: 1, first: 3, last: 6 });
    store.append(&key(), &batch(1, 1, 4)).unwrap();
    assert_eq!(store.retained_bytes(&key()), 32);
    let bounds = ReadBounds { max_records: 2, max_bytes: 1 << 20 };
    let at = |sequence| Cursor { generation: 1, sequence };
    assert_eq!(store.read_after(&key(), at(1), 6, bounds), Err(RecoveryError::HistoryUnavailable));
    let page = store.read_after(&key(), at(2), 6, bounds).unwrap();
    assert_eq!((page[0].first(), page[0].last()), (3, 4));
    assert_eq!(store.read_after(&key(), at(6), 6, bounds), Ok(vec![]));
    assert_eq!(
        store.read_after(&key(), at(2), 6, ReadBounds { max_records: 9, max_bytes: 4 }),
        Err(RecoveryError::LimitExceeded)
    );
}

#[test]
fn every_table_is_bounded_and_a_freed_place_can_be_taken_again() {
    let limits = HubLimits {
        max_sessions: 2,
        max_streams: 2,
        max_subscriptions: 2,
        max_interests_per_session: 2,
        max_interest_keys: 3,
        max_key_bytes: 16,
        ..Default::default()
    };
    let mut hub = StreamHub::new(AllowAll, limits).unwrap();
    let (other, third) = (PeerId::new("other", 1), PeerId::new("third", 1));
    let name = |n: usize| StreamKey::new("md", format!("k{n}"));
    let full = StreamError::LimitExceeded;

    hub.open_session(&me()).unwrap();
    hub.open_session(&other).unwrap();
    assert_eq!(hub.open_session(&third).unwrap_err(), full);
    hub.open_session(&me()).unwrap();

    hub.open_stream(&me(), &name(0), 1, DeliveryPolicy::ReplayRequired).unwrap();
    hub.open_stream(&me(), &name(1), 1, DeliveryPolicy::ReplayRequired).unwrap();
    assert_eq!(hub.open_stream(&me(), &name(2), 1, DeliveryPolicy::ReplayRequired).unwrap_err(), full);
    let long = StreamKey::new("md", "a-name-past-the-key-limit");
    assert!(hub.open_stream(&me(), &long, 1, DeliveryPolicy::ReplayRequired).is_err());

    let first = hub.subscribe(&me(), &name(0), SubscribeOptions::default(), None).unwrap();
    let second = hub.subscribe(&other, &name(0), SubscribeOptions::default(), None).unwrap();
    assert_eq!(hub.subscribe(&other, &name(1), SubscribeOptions::default(), None).unwrap_err(), full);

    let keys = |range: std::ops::Range<usize>| range.map(name).collect::<BTreeSet<_>>();
    assert_eq!(hub.replace_interests(&me(), 1, keys(0..3)).unwrap_err(), full, "per session");
    hub.replace_interests(&me(), 1, keys(0..2)).unwrap();
    assert_eq!(hub.replace_interests(&other, 1, keys(2..4)).unwrap_err(), full, "over all sessions");
    hub.replace_interests(&other, 1, keys(1..3)).unwrap();
    assert_eq!((hub.demand_count(&name(1)), hub.demanded().count()), (2, 3));

    // What was refused changed nothing, and what is given up can be taken again.
    hub.publish(&me(), &name(0), vec![record(1)]).unwrap();
    assert_eq!((drain(&mut hub, first, 1).0, drain(&mut hub, second, 1).0), (vec![1], vec![1]));
    hub.unsubscribe(second);
    assert_eq!(hub.subscribe(&other, &name(1), SubscribeOptions::default(), None).unwrap_err(), full);
    assert_eq!(
        hub.poll(second),
        Some(StreamEvent::End(EndReason::Unsubscribed)),
        "the place is held until its end is read"
    );
    hub.subscribe(&other, &name(1), SubscribeOptions::default(), None).unwrap();
    hub.expire_session(&other).unwrap();
    hub.open_session(&third).unwrap();
    assert_eq!(hub.demanded().count(), 2, "an expired session takes its interests with it");
    hub.replace_interests(&third, 1, keys(2..3)).unwrap();
    hub.close_stream(&me(), &name(1), EndReason::StreamClosed).unwrap();
    hub.open_stream(&me(), &name(2), 1, DeliveryPolicy::ReplayRequired).unwrap();
    assert_eq!(hub.stats().sessions, 2);
}

#[test]
fn a_relay_refuses_a_parent_that_is_behind_the_newest_generation_it_was_shown() {
    let (parent, stale) = (PeerId::new("p", 1), PeerId::new("s", 1));
    let mut relay = RelayCore::new(&PeerId::new("r", 1), AllowAll, CoreConfig::default()).unwrap();
    let sub = relay.subscribe(&key(), SubscribeOptions::default()).unwrap();
    let now = std::time::Instant::now();
    let link = |relay: &mut RelayCore, to: &PeerId| {
        let asked = relay.reconcile(std::slice::from_ref(to), now);
        relay.interest_result(to, asked[0].1.revision, Some(vec![Status::Ok.into()]), now);
        asked[0].1.wants[0].epoch
    };
    let batch = |generation| SequencedBatch::new(generation, 1, vec![Bytes::from_static(b"x")]).unwrap();
    let push = |epoch, item| Push { items: vec![(key(), epoch, item)] };

    let epoch = link(&mut relay, &parent);
    relay.handle_push(&parent, Lane::Live, 1, 1, 1, push(epoch, Item::Batch(batch(4))), now);
    // History is lost, so the relay holds no position and would take whatever comes next.
    relay.handle_push(&parent, Lane::Live, 1, 1, 2, push(epoch, Item::Reset(ResetReason::Gap)), now);
    let epoch = link(&mut relay, &stale);
    relay.handle_push(&stale, Lane::Live, 1, 1, 1, push(epoch, Item::Batch(batch(3))), now);
    assert_eq!((relay.parent_of(&key()), relay.stats().refusals[Status::GenerationMismatch as usize]), (None, 1));

    let mut generations = Vec::new();
    while let Some(event) = relay.poll(sub) {
        if let StreamEvent::Batch(batch) = event {
            generations.push(batch.generation());
        }
    }
    assert_eq!(generations, [4]);
}

#[test]
fn a_namespace_ring_bounds_its_streams_and_a_batch_larger_than_the_ring_keeps_its_tail() {
    let ring = RingLimits { namespace: "md".into(), max_records: 4, max_bytes: 1 << 20 };
    let mut store = MemoryStore::new(MemoryLimits::default()).unwrap().with_rings(vec![ring]);
    let wide = StreamKey::new("l3", "btc");
    let batch = SequencedBatch::new(1, 1, (1..=6).map(record).collect::<Vec<_>>()).unwrap();
    for key in [key(), wide.clone()] {
        store.open(&key, 1).unwrap();
        store.append(&key, &batch).unwrap();
    }
    assert_eq!(store.available_range(&key()).unwrap(), ReplayRange { generation: 1, first: 3, last: 6 });
    assert_eq!(store.available_range(&wide).unwrap(), ReplayRange { generation: 1, first: 1, last: 6 });
    assert_eq!(store.total_bytes(), (4 + 6) * 8);
    store.restart(&key(), 2).unwrap();
    assert_eq!(store.total_bytes(), 6 * 8);
    store.remove(&wide);
    assert_eq!(store.total_bytes(), 0);
}

fn limits(limits: NamespaceLimits) -> BTreeMap<String, NamespaceLimits> {
    BTreeMap::from([("md".to_owned(), limits)])
}

fn over(limit: NamespaceLimit) -> StreamError {
    StreamError::QuotaExceeded { namespace: "md".into(), limit, retry_after: None }
}

#[test]
fn a_namespace_holds_its_subscriptions_to_its_limit_and_a_freed_one_can_be_taken_again() {
    let md = limits(NamespaceLimits { subscriptions: Some(2), ..Default::default() });
    let mut core =
        RelayCore::new(&PeerId::new("o", 1), AllowAll, CoreConfig { namespaces: md, ..CoreConfig::default() }).unwrap();
    let (eth, fx) = (StreamKey::new("md", "eth"), StreamKey::new("fx", "eur"));
    for key in [&key(), &eth, &fx] {
        core.open_stream(key, 1, DeliveryPolicy::ReplayRequired).unwrap();
    }
    let (live, first) =
        (SubscribeOptions::default, || SubscribeOptions { start: StartPosition::First, ..Default::default() });
    let ending = core.subscribe(&key(), first()).unwrap();
    let follower = core.subscribe(&eth, live()).unwrap();
    let full = over(NamespaceLimit::Subscriptions);
    assert_eq!(core.subscribe(&key(), first()), Err(full.clone()));
    assert_eq!(core.subscribe(&eth, live()), Err(full.clone()));
    let unlimited: Vec<u64> = (0..3).map(|_| core.subscribe(&fx, live()).unwrap()).collect();
    assert_eq!(core.stats().hub.quota_refused, [2, 0, 0, 0, 0]);

    // A close ends the subscription, whose place is held until its end is read.
    core.close_stream(&key()).unwrap();
    assert_eq!(core.subscribe(&eth, live()), Err(full.clone()), "held until its end is read");
    while core.poll(ending).is_some() {}
    let second = core.subscribe(&eth, live()).unwrap();
    for id in [second, follower].into_iter().chain(unlimited) {
        core.unsubscribe(id);
    }
    assert_eq!(core.stats().hub.subscriptions, 0);
    core.subscribe(&eth, first()).unwrap();
    core.subscribe(&eth, live()).unwrap();
    assert_eq!(core.subscribe(&eth, live()), Err(full), "the count went back to zero");

    // A child's branch is not one of them: its subscribers are held on their own node.
    let want = Want { key: eth.clone(), epoch: 1, start: StartPosition::Live, snapshot: false };
    let interest = Interest { revision: 1, wants: vec![want], ..Default::default() };
    let answers = core.handle_interest(&PeerId::new("c", 1), interest, std::time::Instant::now()).unwrap();
    assert_eq!(answers.iter().map(|a| a.status).collect::<Vec<_>>(), [Status::Ok]);
    assert_eq!((core.stats().hub.quota_refused, core.stats().hub.publish_rejected), ([4, 0, 0, 0, 0], 0));
}

/// Four 8-byte records fill the namespace's rings. An origin's ring gives up its own oldest records for a batch,
/// never another's; a close or a restart gives the room back.
#[test]
fn a_namespace_holds_its_rings_to_its_budget_and_a_close_or_its_own_trim_makes_room() {
    let md = limits(NamespaceLimits { ring_bytes: Some(32), ..Default::default() });
    let cfg = CoreConfig { namespaces: md, ..CoreConfig::default() };
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, cfg).unwrap();
    let (eth, sol, fx) = (StreamKey::new("md", "eth"), StreamKey::new("md", "sol"), StreamKey::new("fx", "eur"));
    for key in [&key(), &eth, &sol, &fx] {
        origin.open_stream(key, 1, DeliveryPolicy::ReplayRequired).unwrap();
    }
    let records = |range: std::ops::Range<u64>| range.map(record).collect::<Vec<_>>();
    origin.publish(&key(), records(1..4)).unwrap();
    origin.publish(&eth, records(1..2)).unwrap();
    let refused = origin.publish(&eth, records(2..4)).unwrap_err();
    assert_eq!((refused.error, refused.returned), (over(NamespaceLimit::RingBytes), records(2..4)));
    origin.publish(&eth, records(2..3)).unwrap();
    origin.publish(&key(), records(4..5)).unwrap();
    assert_eq!(origin.publish(&sol, records(1..2)).unwrap_err().error, over(NamespaceLimit::RingBytes));
    for n in 0..100 {
        origin.publish(&fx, records(n..n + 1)).unwrap();
    }
    assert_eq!(origin.stats().retained_bytes, 32 + 100 * 8, "another namespace is not held to it");
    let stats = origin.stats().hub;
    assert_eq!((stats.quota_refused, stats.publish_rejected), ([0, 2, 0, 0, 0], 2));

    origin.close_stream(&key()).unwrap();
    origin.publish(&eth, records(3..5)).unwrap();
    origin.publish(&sol, records(1..2)).unwrap();
    origin.close_stream(&eth).unwrap();
    origin.close_stream(&sol).unwrap();
    origin.close_stream(&fx).unwrap();
    assert_eq!(origin.stats().retained_bytes, 0);
    origin.open_stream(&key(), 2, DeliveryPolicy::ReplayRequired).unwrap();
    origin.publish(&key(), records(1..5)).unwrap();
    assert_eq!(origin.publish(&eth, records(1..2)).unwrap_err().error, StreamError::UnknownStream);
    origin.open_stream(&eth, 1, DeliveryPolicy::ReplayRequired).unwrap();
    assert_eq!(origin.publish(&eth, records(1..2)).unwrap_err().error, over(NamespaceLimit::RingBytes));
    origin.restart_stream(&key(), 3).unwrap();
    origin.publish(&eth, records(1..5)).unwrap();
    assert_eq!(origin.stats().retained_bytes, 32);
}

/// A compacted stream's state is memory its origin holds for the namespace, as `retained_bytes` counts it: it takes its
/// share of the rings' budget and, unlike a ring, gives none of it up. A batch the budget refuses leaves the state as
/// it was; a restart or a close frees it.
#[test]
fn a_namespace_budget_counts_a_compacted_state_beside_its_ring() {
    let md = limits(NamespaceLimits { ring_bytes: Some(32), ..Default::default() });
    let cfg = CoreConfig { namespaces: md, ..CoreConfig::default() };
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, cfg).unwrap();
    let eth = StreamKey::new("md", "eth");
    let compacted = DeliveryPolicy::Compacted(CompactLimits { max_keys: 16, max_bytes: 1024 });
    origin.open_stream(&key(), 1, compacted).unwrap();
    origin.open_stream(&eth, 1, DeliveryPolicy::ReplayRequired).unwrap();
    // 8 bytes each: in the ring, and again in the state while it is its key's latest.
    let set = |k: &[u8]| keyed(k, &[7; 5]);
    origin.publish(&key(), vec![set(b"a")]).unwrap();
    origin.publish(&key(), vec![set(b"b")]).unwrap();
    assert_eq!(origin.stats().retained_bytes, 32);
    assert_eq!(origin.publish(&eth, vec![record(1)]).unwrap_err().error, over(NamespaceLimit::RingBytes));
    // The ring gives up its oldest records to the state, never the state its keys.
    origin.publish(&key(), vec![set(b"a")]).unwrap();
    origin.publish(&key(), vec![set(b"c")]).unwrap();
    assert_eq!(origin.stats().retained_bytes, 32);
    let refused = origin.publish(&key(), vec![set(b"d")]).unwrap_err();
    assert_eq!((refused.error, refused.returned), (over(NamespaceLimit::RingBytes), vec![set(b"d")]));
    assert_eq!(origin.stats().retained_bytes, 32, "the refused batch's state is not held");
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let id = origin.subscribe(&key(), options).unwrap();
    assert!(matches!(origin.poll(id), Some(StreamEvent::Ready { .. })));
    let Some(StreamEvent::Snapshot(state)) = origin.poll(id) else { panic!("the state") };
    assert_eq!(state.records(), [set(b"a"), set(b"b"), set(b"c")], "the refused batch left it as it was");
    assert_eq!(origin.stats().hub.quota_refused, [0, 2, 0, 0, 0]);

    origin.restart_stream(&key(), 2).unwrap();
    origin.publish(&eth, (1..5).map(record).collect()).unwrap();
    assert_eq!(origin.stats().retained_bytes, 32);
    origin.close_stream(&eth).unwrap();
    origin.publish(&key(), vec![set(b"a"), set(b"b")]).unwrap();
    origin.close_stream(&key()).unwrap();
    assert_eq!(origin.stats().retained_bytes, 0);
    origin.open_stream(&eth, 1, DeliveryPolicy::ReplayRequired).unwrap();
    origin.publish(&eth, (1..5).map(record).collect()).unwrap();
}

/// A client that reconnects to a node whose relay is a few records behind the node it came from: its cursor is ahead
/// of this relay's position, not behind its ring, so it waits for the relay to reach it, as on a relay with no
/// position yet. An origin has no such excuse and refuses it (`history_that_is_gone_is_refused_at_subscribe`).
#[test]
fn a_cursor_ahead_of_a_relay_stream_waits_for_it() {
    let (mut hub, store) = relay_hub();
    accept(&mut hub, &store, 1, 5);
    let id = hub.subscribe(&me(), &key(), after(8), Some(store.clone())).expect("waits for the relay to reach 8");
    accept(&mut hub, &store, 6, 7);
    assert_eq!(drain(&mut hub, id, 4).0, vec![9, 10, 11, 12]);
}

fn compacted(max_keys: usize, max_bytes: usize) -> StreamHub {
    let mut hub = StreamHub::new(AllowAll, HubLimits::default()).unwrap();
    hub.open_session(&me()).unwrap();
    let policy = DeliveryPolicy::Compacted(CompactLimits { max_keys, max_bytes });
    hub.open_stream(&me(), &key(), 7, policy).unwrap();
    hub
}

fn set(key: &str, value: &str) -> Bytes {
    keyed(key.as_bytes(), value.as_bytes())
}

/// Applies records as a subscriber of a compacted stream does: an empty value removes the key.
fn apply(state: &mut BTreeMap<Bytes, Bytes>, records: &[Bytes]) {
    for record in records {
        let (key, value) = split_keyed(record).expect("a keyed record");
        if value.is_empty() {
            state.remove(&key);
        } else {
            state.insert(key, value);
        }
    }
}

#[test]
fn a_compacted_stream_hands_a_late_subscriber_the_latest_record_per_key_then_every_record_after_it() {
    let mut hub = compacted(16, 1 << 10);
    let mut model = BTreeMap::new();
    let batches = [
        vec![set("a", "1"), set("b", "1"), set("c", "1")],
        vec![set("a", "2")],
        vec![keyed(b"b", b""), set("d", "1")],
        // Set and removed in one batch, and set twice in one batch.
        vec![set("e", "1"), keyed(b"e", b""), set("f", "1"), set("f", "2")],
        vec![keyed(b"never", b"")],
    ];
    for batch in batches {
        apply(&mut model, &batch);
        hub.publish(&me(), &key(), batch).unwrap();
    }
    let cut = Cursor { generation: 7, sequence: 11 };
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let id = hub.subscribe(&me(), &key(), options, None).unwrap();
    let later = [vec![set("a", "3"), keyed(b"c", b"")], vec![set("g", "1")]];
    for batch in later.clone() {
        hub.publish(&me(), &key(), batch).unwrap();
    }
    assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(cut) }));
    let Some(StreamEvent::Snapshot(snapshot)) = hub.poll(id) else { panic!("no snapshot") };
    assert_eq!(snapshot.through(), cut);
    // Exactly the latest record per key, in key order: no tombstone, no older value.
    assert_eq!(snapshot.records(), [set("a", "2"), set("c", "1"), set("d", "1"), set("f", "2")]);
    let mut state = BTreeMap::new();
    apply(&mut state, snapshot.records());
    assert_eq!(state, model);
    assert_eq!(hub.poll(id), Some(StreamEvent::CaughtUp { through: cut }));
    let mut next = cut.sequence + 1;
    for batch in later {
        let Some(StreamEvent::Batch(got)) = hub.poll(id) else { panic!("no batch") };
        assert_eq!((got.first(), got.records()), (next, &batch[..]), "nothing twice, nothing missing across the cut");
        next = got.last() + 1;
        apply(&mut state, got.records());
        apply(&mut model, &batch);
    }
    assert_eq!(state, model);
    assert_eq!(hub.poll(id), None);
    assert_eq!(hub.total_queued(), (0, 0));

    // A restart is a new generation with no state.
    hub.restart_stream(&me(), &key(), 8).unwrap();
    let id = hub.subscribe(&me(), &key(), options, None).unwrap();
    let empty = Snapshot::new(Cursor { generation: 8, sequence: 0 }, Vec::new());
    let events: Vec<_> = std::iter::from_fn(|| hub.poll(id)).collect();
    assert_eq!(events[1], StreamEvent::Snapshot(empty));
}

#[test]
fn a_compacted_stream_refuses_a_publish_past_its_limits_and_changes_nothing() {
    // Each of these records is 2 bytes of key length, 1 of key, 3 of value.
    let mut hub = compacted(2, 14);
    hub.publish(&me(), &key(), vec![set("a", "xxx"), set("b", "xxx")]).unwrap();
    let refused = |hub: &mut StreamHub, records: Vec<Bytes>| {
        let rejected = hub.publish(&me(), &key(), records.clone()).unwrap_err();
        assert_eq!(rejected.returned, records, "the caller gets its records back");
        rejected.error
    };
    assert_eq!(refused(&mut hub, vec![set("c", "x")]), StreamError::StateFull, "a third key");
    assert_eq!(refused(&mut hub, vec![set("a", "xxxxxx")]), StreamError::StateFull, "15 bytes");
    // All or nothing: the first record fits alone.
    assert_eq!(refused(&mut hub, vec![set("a", "xxxxx"), set("c", "x")]), StreamError::StateFull);
    let malformed = vec![set("a", "x"), Bytes::from_static(b"\x00")];
    assert_eq!(refused(&mut hub, malformed), StreamError::Recovery(RecoveryError::InvalidBatch));
    assert_eq!(hub.cursor(&key()), Ok(Some(Cursor { generation: 7, sequence: 2 })));
    assert_eq!(hub.stats().publish_rejected, 4);
    hub.publish(&me(), &key(), vec![set("a", "xxxxx")]).unwrap();
    hub.publish(&me(), &key(), vec![keyed(b"b", b""), set("c", "x")]).unwrap();

    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let id = hub.subscribe(&me(), &key(), options, None).unwrap();
    hub.poll(id);
    let Some(StreamEvent::Snapshot(snapshot)) = hub.poll(id) else { panic!("no snapshot") };
    assert_eq!(snapshot.records(), [set("a", "xxxxx"), set("c", "x")]);
    // It travels framed, 20 bytes: a smaller branch is refused at once and holds nothing.
    assert_eq!(snapshot.bytes(), 20);
    let small = SubscribeOptions { limits: BranchLimits { max_bytes: 19, ..Default::default() }, ..options };
    assert_eq!(hub.subscribe(&me(), &key(), small, None), Err(StreamError::LimitExceeded));
    assert_eq!(hub.total_queued(), (0, 0));

    for limits in [CompactLimits { max_keys: 0, max_bytes: 1 }, CompactLimits { max_keys: 1, max_bytes: 0 }] {
        let policy = DeliveryPolicy::Compacted(limits);
        assert_eq!(hub.open_stream(&me(), &StreamKey::new("md", "z"), 1, policy), Err(StreamError::InvalidOptions));
    }
    // An origin refuses a state that would not fit one push to its children.
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, CoreConfig::default()).unwrap();
    let huge = DeliveryPolicy::Compacted(CompactLimits { max_keys: 1024, max_bytes: 1 << 20 });
    assert_eq!(origin.open_stream(&key(), 1, huge), Err(StreamError::InvalidOptions));
    // Nor one that fits a push but not a branch.
    let narrow = CoreConfig {
        branch: BranchLimits { max_bytes: 512 << 10, ..CoreConfig::default().branch },
        ..Default::default()
    };
    let mut origin = RelayCore::new(&PeerId::new("o", 1), AllowAll, narrow).unwrap();
    let wide = DeliveryPolicy::Compacted(CompactLimits { max_keys: 16, max_bytes: 600 << 10 });
    assert_eq!(origin.open_stream(&key(), 1, wide), Err(StreamError::InvalidOptions));
    // The state is retained memory, beside the history ring: two records of 6 bytes there, one here.
    origin.open_stream(&key(), 1, DeliveryPolicy::Compacted(CompactLimits { max_keys: 1, max_bytes: 6 })).unwrap();
    origin.publish(&key(), vec![set("a", "xxx"), set("a", "yyy")]).unwrap();
    assert_eq!(origin.stats().retained_bytes, 3 * 6);
}

/// A relay holds no state: its parent sends one, after every record it covers, and the relay continues it from its
/// own ring. A snapshot ahead of the relay, or of another generation, is refused.
#[test]
fn a_relay_stream_starts_a_state_subscriber_from_the_snapshot_its_parent_sends() {
    let (mut hub, store) = relay_hub();
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let id = hub.subscribe(&me(), &key(), options, Some(store.clone())).expect("waits for a snapshot");
    // One that cannot hold the state ends once it comes, and is not asked to retry.
    let small = SubscribeOptions { limits: BranchLimits { max_bytes: 7, ..Default::default() }, ..options };
    let small = hub.subscribe(&me(), &key(), small, Some(store.clone())).unwrap();
    hub.adopt_relay_stream(&me(), &key(), Cursor { generation: 7, sequence: 0 }).unwrap();
    accept(&mut hub, &store, 1, 5);
    assert_eq!(hub.poll(id), None, "nothing before the snapshot");
    let ahead = Snapshot::new(Cursor { generation: 7, sequence: 6 }, vec![]);
    assert_eq!(hub.accept_snapshot(&me(), &key(), ahead), Err(RecoveryError::InvalidSnapshot.into()));
    let other = Snapshot::new(Cursor { generation: 6, sequence: 3 }, vec![]);
    assert_eq!(hub.accept_snapshot(&me(), &key(), other), Err(RecoveryError::GenerationMismatch.into()));
    assert_eq!(hub.poll(id), None);
    let through = Cursor { generation: 7, sequence: 3 };
    let snapshot = Snapshot::new(through, vec![set("k", "3")]);
    hub.accept_snapshot(&me(), &key(), snapshot.clone()).unwrap();
    assert_eq!(snapshot.bytes(), 8);
    assert_eq!(hub.poll(small), Some(StreamEvent::End(EndReason::SlowConsumer)));
    accept(&mut hub, &store, 6, 2);
    assert_eq!(hub.poll(id), Some(StreamEvent::Ready { start: Some(through) }));
    assert_eq!(hub.poll(id), Some(StreamEvent::Snapshot(snapshot)));
    assert_eq!(drain(&mut hub, id, 1), ((4..=7).collect(), None));
    assert_eq!(hub.total_queued(), (0, 0));
}

/// A state larger than the hub's whole queue: refused at the origin, and a reset where a relay's subscriber waits.
#[test]
fn a_state_larger_than_the_hub_queue_is_refused_and_charges_nothing() {
    let limits = HubLimits { max_queued_bytes: 7, ..Default::default() };
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let mut origin = StreamHub::new(AllowAll, limits).unwrap();
    origin.open_session(&me()).unwrap();
    let policy = DeliveryPolicy::Compacted(CompactLimits { max_keys: 1, max_bytes: 64 });
    origin.open_stream(&me(), &key(), 7, policy).unwrap();
    origin.publish(&me(), &key(), vec![set("k", "3")]).unwrap();
    assert_eq!(origin.subscribe(&me(), &key(), options, None), Err(StreamError::LimitExceeded));

    let mut relay = StreamHub::new(AllowAll, limits).unwrap();
    relay.open_session(&me()).unwrap();
    relay.open_relay_stream(&me(), &key(), DeliveryPolicy::ReplayRequired).unwrap();
    let at = Cursor { generation: 7, sequence: 1 };
    relay.adopt_relay_stream(&me(), &key(), at).unwrap();
    let id = relay.subscribe(&me(), &key(), options, None).unwrap();
    relay.accept_snapshot(&me(), &key(), Snapshot::new(at, vec![set("k", "3")])).unwrap();
    assert_eq!(relay.poll(id), Some(StreamEvent::ResetRequired(ResetReason::GlobalCapacity)));
    assert_eq!((origin.total_queued(), relay.total_queued()), ((0, 0), (0, 0)));
}

/// Denies the named sessions the state alone.
struct NoState(Mutex<BTreeSet<String>>);

impl Authorizer for NoState {
    fn authorize(&self, peer: &PeerId, _: &StreamKey, access: Access) -> bool {
        access != Access::Snapshot || !self.0.lock().unwrap().contains(&*peer.node)
    }
}

/// A relay's subscriber waiting for the state ends once it may no longer have it; its other subscriptions stay.
#[test]
fn a_subscriber_denied_the_state_while_it_waits_for_it_ends() {
    let mut hub = StreamHub::new(NoState(Mutex::default()), HubLimits::default()).unwrap();
    let alice = PeerId::new("alice", 1);
    for s in [&me(), &alice] {
        hub.open_session(s).unwrap();
    }
    hub.open_relay_stream(&me(), &key(), DeliveryPolicy::ReplayRequired).unwrap();
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let waiting = hub.subscribe(&alice, &key(), options, None).unwrap();
    let live = hub.subscribe(&alice, &key(), SubscribeOptions::default(), None).unwrap();
    hub.update_authorizer(|deny| {
        deny.0.lock().unwrap().insert("alice".into());
    });
    assert_eq!(hub.poll(waiting), Some(StreamEvent::End(EndReason::Revoked)));
    assert!(std::iter::from_fn(|| hub.poll(live)).all(|e| !e.is_terminal()));
    assert!(hub.contains(live));
}
