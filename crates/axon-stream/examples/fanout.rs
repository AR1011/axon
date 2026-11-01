//! Hub throughput: live fanout to N subscribers, publish with and without compaction, an origin's publish, and the
//! replay-to-live join.
//! `cargo run --release -p axon-stream --example fanout`
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]

use std::sync::{Arc, Mutex};
use std::time::Instant;

use axon_stream::*;
use bytes::Bytes;

const BATCH: usize = 16;
const RECORD: usize = 64;
const ROUNDS: usize = 7;

fn median(mut runs: Vec<f64>) -> f64 {
    runs.sort_by(f64::total_cmp);
    runs[runs.len() / 2]
}

fn hub(policy: DeliveryPolicy) -> (StreamHub, PeerId, StreamKey) {
    let (me, key) = (PeerId::new("local", 1), StreamKey::new("bench", "stream"));
    let mut hub = StreamHub::new(AllowAll, HubLimits::default()).unwrap();
    hub.open_session(&me).unwrap();
    hub.open_stream(&me, &key, 1, policy).unwrap();
    (hub, me, key)
}

/// Publish one batch, poll it on every subscriber. Returns ns per delivered record.
fn fanout(subscribers: usize) -> f64 {
    let (mut hub, me, key) = hub(DeliveryPolicy::ReplayRequired);
    let ids: Vec<u64> =
        (0..subscribers).map(|_| hub.subscribe(&me, &key, SubscribeOptions::default(), None).unwrap()).collect();
    for id in &ids {
        while hub.poll(*id).is_some() {}
    }
    let payload = Bytes::from(vec![7u8; RECORD]);
    let batches = (2_000_000 / (subscribers * BATCH)).max(200);
    let start = Instant::now();
    for _ in 0..batches {
        hub.publish(&me, &key, vec![payload.clone(); BATCH]).unwrap();
        for id in &ids {
            std::hint::black_box(hub.poll(*id));
        }
    }
    assert_eq!(hub.total_queued(), (0, 0));
    start.elapsed().as_nanos() as f64 / (batches * BATCH * subscribers) as f64
}

/// Publishes batches of keyed records over 1024 keys, with no subscriber. Returns ns per record.
fn publish(policy: DeliveryPolicy) -> f64 {
    let (mut hub, me, key) = hub(policy);
    let records: Vec<Bytes> = (0..1024u32).map(|k| keyed(&k.to_be_bytes(), &[7u8; RECORD - 6])).collect();
    let batches = 2_000_000 / BATCH;
    let start = Instant::now();
    for i in 0..batches {
        let at = i * BATCH % records.len();
        hub.publish(&me, &key, records[at..at + BATCH].to_vec()).unwrap();
    }
    start.elapsed().as_nanos() as f64 / (batches * BATCH) as f64
}

/// Publishes through an origin's `RelayCore`, which keeps the ring too: what `StreamNode::publish` runs under its
/// lock. Returns ns per record.
fn origin() -> f64 {
    let key = StreamKey::new("bench", "stream");
    let mut core = RelayCore::new(&PeerId::new("o", 1), AllowAll, CoreConfig::default()).unwrap();
    core.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).unwrap();
    let payload = Bytes::from(vec![7u8; RECORD]);
    let batches = 2_000_000 / BATCH;
    let start = Instant::now();
    for _ in 0..batches {
        core.publish(&key, vec![payload.clone(); BATCH]).unwrap();
    }
    start.elapsed().as_nanos() as f64 / (batches * BATCH) as f64
}

/// Replays `records` of history through one subscription. Returns ns per replayed record.
fn join(records: usize) -> f64 {
    let (mut hub, me, key) = hub(DeliveryPolicy::ReplayRequired);
    let mut store = MemoryStore::new(MemoryLimits {
        max_records_per_stream: records,
        max_bytes_per_stream: 1 << 30,
        ..Default::default()
    })
    .unwrap();
    store.open(&key, 1).unwrap();
    let store = Arc::new(Mutex::new(store));
    let payload = Bytes::from(vec![7u8; RECORD]);
    for _ in 0..records / BATCH {
        let batch = hub.publish(&me, &key, vec![payload.clone(); BATCH]).unwrap();
        store.lock().unwrap().append(&key, &batch).unwrap();
    }
    let options =
        SubscribeOptions { start: StartPosition::After(Cursor { generation: 1, sequence: 0 }), ..Default::default() };
    let start = Instant::now();
    let id = hub.subscribe(&me, &key, options, Some(store)).unwrap();
    let mut seen = 0;
    while seen < records / BATCH * BATCH {
        hub.drive_recovery(id, ReadBounds { max_records: 1024, max_bytes: 1 << 20 }).unwrap();
        while let Some(event) = hub.poll(id) {
            if let StreamEvent::Batch(batch) = event {
                seen += batch.len();
            }
        }
    }
    start.elapsed().as_nanos() as f64 / seen as f64
}

/// Decodes a push of one batch of `record`-byte records, as a relay receives it. Returns ns per record.
fn decode(record: usize) -> f64 {
    let batch = SequencedBatch::new(1, 1, vec![Bytes::from(vec![7u8; record]); BATCH]).unwrap();
    let push = Push { items: vec![(StreamKey::new("bench", "stream"), 1, Item::Batch(batch))] };
    let body = wire::encode_push(1, 1, &push).slice(1..);
    let rounds = 100_000;
    let start = Instant::now();
    for _ in 0..rounds {
        std::hint::black_box(wire::decode_push(&mut body.clone()));
    }
    start.elapsed().as_nanos() as f64 / (rounds * BATCH) as f64
}

fn main() {
    println!("batch {BATCH} x {RECORD} B, median of {ROUNDS}");
    for subscribers in [1, 10, 100, 1000, 10_000] {
        let ns = median((0..ROUNDS).map(|_| fanout(subscribers)).collect());
        println!(
            "fanout  {subscribers:>6} subscribers  {ns:>7.1} ns/record/subscriber  {:>6.1} M deliveries/s",
            1e3 / ns
        );
    }
    let compacted = DeliveryPolicy::Compacted(CompactLimits { max_keys: 1024, max_bytes: 1 << 20 });
    for (name, policy) in [("replay required", DeliveryPolicy::ReplayRequired), ("compacted, 1024 keys", compacted)] {
        let ns = median((0..ROUNDS).map(|_| publish(policy)).collect());
        println!("publish {name:<20}  {ns:>7.1} ns/record  {:>6.1} M records/s", 1e3 / ns);
    }
    let ns = median((0..ROUNDS).map(|_| origin()).collect());
    println!("origin publish, no subscribers  {ns:>7.1} ns/record  {:>6.1} M records/s", 1e3 / ns);
    let ns = median((0..ROUNDS).map(|_| join(1 << 20)).collect());
    println!("replay join, 1M records, 1024-record pages  {ns:>7.1} ns/record  {:>6.1} M records/s", 1e3 / ns);
    for record in [RECORD, 4096] {
        let ns = median((0..ROUNDS).map(|_| decode(record)).collect());
        println!("push decode, {record:>4} B records  {ns:>7.1} ns/record  {:>6.1} M records/s", 1e3 / ns);
    }
}
