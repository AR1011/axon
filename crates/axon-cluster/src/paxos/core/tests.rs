use std::io;

use axon::files::{FileFuture, MemoryFiles};

use super::*;
use crate::paxos::ready;

const U: u64 = 1_800_000_000_000_000;
/// The protocol id the logs here are written for.
const P: u64 = 0x0102_0304_0506_0708;
const LEASE: Duration = Duration::from_secs(6);

fn ballot(epoch: u64, node: &str) -> Ballot {
    Ballot { epoch, node: node.into() }
}

fn fixtures() -> (Vec<Item>, Vec<Answer>) {
    let items = [Kind::Prepare, Kind::Propose(Duration::from_secs(6)), Kind::Release]
        .map(|kind| Item { key: "k".into(), ballot: ballot(7, "n/1"), kind })
        .to_vec();
    let answers = vec![
        Answer::Promised(None),
        Answer::Promised(Some("n/1".into())),
        Answer::Accepted,
        Answer::Refused(9),
        Answer::Warming,
        Answer::Full,
    ];
    (items, answers)
}

#[test]
fn wire_round_trips_and_refuses_every_truncation() {
    let (items, answers) = fixtures();
    let (i, a) = (encode_items(&items), encode_answers(&answers));
    assert_eq!(decode_items(i.clone()), Some(items));
    assert_eq!(decode_answers(a.clone()), Some(answers));
    for n in 0..i.len() {
        assert_eq!(decode_items(i.slice(..n)), None, "items cut at {n}");
    }
    for n in 0..a.len() {
        assert_eq!(decode_answers(a.slice(..n)), None, "answers cut at {n}");
    }
    assert_eq!(decode_items([&i[..], &[0]].concat().into()), None, "trailing byte");
    let many = Bytes::copy_from_slice(&(MAX_ITEMS as u16 + 1).to_be_bytes());
    assert_eq!(decode_items(many), None, "more items than allowed");
}

/// The bytes peers see for `SCHEMA` 3. When this fails you changed the protocol: bump
/// `SCHEMA` and re-pin, or acceptors on the old schema refuse what they cannot decode.
#[test]
fn wire_bytes_are_pinned_to_the_schema() {
    assert_eq!(SCHEMA, 3, "SCHEMA moved: re-pin the bytes below");
    let (items, answers) = fixtures();
    const ITEMS: &[u8] = &[
        0x00, 0x03, 0x00, 0x03, 0x6e, 0x2f, 0x31, 0x01, 0x00, 0x01, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x07, 0x02, 0x00, 0x01, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x17, 0x70, 0x03,
        0x00, 0x01, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
    ];
    const ANSWERS: &[u8] = &[
        0x00, 0x06, 0x01, 0x00, 0x01, 0x01, 0x00, 0x03, 0x6e, 0x2f, 0x31, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x09, 0x04, 0x05,
    ];
    assert_eq!(&encode_items(&items)[..], ITEMS, "items encoding changed");
    assert_eq!(&encode_answers(&answers)[..], ANSWERS, "answers encoding changed");
    const ASK: &[u8] = &[0x00, 0x00, 0x01, 0x6e, 0x00, 0x00, 0x00, 0x04, 0x6b, 0x69, 0x6e, 0x64];
    const LAPSED: &[u8] = &[0x01, 0x00, 0x03, 0x65, 0x2f, 0x6b, 0x00, 0x04, 0x6b, 0x69, 0x6e, 0x64];
    const HELD: &[u8] = &[0x01, 0x00, 0x03, 0x65, 0x2f, 0x6b];
    let kind = || vec!["kind".to_string()];
    assert_eq!(&encode_held_ask(Holder::Node("n"), "", &kind())[..], ASK, "held ask encoding changed");
    assert_eq!(&encode_held_ask(Holder::Lapsed, "e/k", &kind())[..], LAPSED, "lapsed ask encoding changed");
    assert_eq!(&encode_held(&["e/k".into()], true)[..], HELD, "held encoding changed");
    assert_eq!(decode_held_ask(Bytes::from_static(ASK)), Some((Some("n".into()), String::new(), kind())));
    assert_eq!(decode_held_ask(Bytes::from_static(LAPSED)), Some((None, "e/k".into(), kind())));
    assert_eq!(decode_held_ask(Bytes::from_static(&[0x02, 0x00, 0x00])), None, "an unknown holder");
    assert_eq!(decode_held(Bytes::from_static(HELD)), Some((vec!["e/k".into()], true)));
    assert_eq!(decode_held(Bytes::from_static(&HELD[..HELD.len() - 1])), None);
    assert_eq!(decode_held(Bytes::from_static(&[2])), None);
}

/// What an eager kind restarts: a node's leases of its kinds that were not released, while no other node took
/// them, from every process of it, for `LISTED` after they ran out however short the lease, page by page.
#[test]
fn a_nodes_unreleased_leases_are_listed_until_long_past_their_end() {
    let lease = Duration::from_secs(2);
    let mut a = Acceptor::new(Duration::ZERO, U, lease, 16);
    let (kinds, n) = (["e".to_string()], Holder::Node("n"));
    assert_eq!(a.held_by(n, &kinds, "", lease - Duration::from_millis(1)), None, "a warming acceptor forgot");
    let epoch = U + 2 * lease.as_micros() as u64;
    let held = [("e/k", "n/1"), ("e/j", "n/2"), ("e/r", "n/1"), ("e/o", "m/1"), ("e/p", "nn/1"), ("x/q", "n/1")];
    for (key, node) in held {
        grant(&mut a, key, &ballot(epoch, node), lease, lease);
    }
    a.answer(&Item { key: "e/r".into(), ballot: ballot(epoch, "n/1"), kind: Kind::Release }, lease, epoch);
    let listed = |a: &Acceptor, now| {
        let (keys, more) = a.held_by(n, &kinds, "", now)?;
        Some((keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(), more))
    };
    let both = Some((vec!["e/j".to_string(), "e/k".into()], false));
    assert_eq!(listed(&a, lease), both);
    let end = lease * 2 + LISTED;
    a.sweep(end - Duration::from_millis(1));
    assert_eq!(listed(&a, end), both, "ran out long ago, kept");
    a.sweep(end);
    assert_eq!(listed(&a, end), Some((vec![], false)));

    let long: Vec<String> = ["a", "b", "c", "d"].iter().map(|c| format!("e/{}", c.repeat(HELD_PAGE / 3))).collect();
    for key in &long {
        grant(&mut a, key, &ballot(epoch + 1, "n/1"), lease, end);
    }
    let (first, more) = a.held_by(n, &kinds, "", end).unwrap();
    assert!(more && first.len() == 3, "one page stops once it passes HELD_PAGE");
    let (rest, more) = a.held_by(n, &kinds, &first[2], end).unwrap();
    assert_eq!((rest.len(), more, &*rest[0]), (1, false, long[3].as_str()));
}

/// A restarted acceptor forgot what it promised. A prepare from a clock ahead of it, sent before
/// it restarted and arriving late, must still lose.
#[test]
fn a_restarted_acceptor_refuses_epochs_from_before_it_restarted() {
    let wait = Duration::from_secs(6);
    let mut a = Acceptor::new(Duration::ZERO, U, wait, 16);
    let late = Item { key: "k".into(), ballot: ballot(U + wait.as_micros() as u64 / 2, "q/1"), kind: Kind::Prepare };
    assert_eq!(a.answer(&late, wait - Duration::from_millis(1), U), Answer::Warming);
    assert!(matches!(a.answer(&late, wait, U + wait.as_micros() as u64), Answer::Refused(_)));
}

/// `MemoryFiles` lands a write before it returns.
fn apply(dir: &MemoryFiles, ops: Vec<FileOp>) {
    drop(dir.apply(ops));
}

fn appended(a: &Acceptor) -> u64 {
    a.log.as_ref().expect("a log").appended
}

/// Every write the acceptor hands out, landed.
fn flush(a: &mut Acceptor, dir: &MemoryFiles) {
    while let Some(ops) = a.take_write() {
        apply(dir, ops);
        a.written(true);
    }
    assert_eq!(a.synced(), appended(a), "nothing left to sync");
}

fn replay(dir: &MemoryFiles) -> Stored {
    ready(Stored::load(dir, P)).expect("in memory")
}

fn names(dir: &MemoryFiles) -> Vec<String> {
    ready(dir.list()).expect("in memory").into_iter().map(|(name, _)| name).collect()
}

fn grant(a: &mut Acceptor, key: &str, ballot: &Ballot, lease: Duration, now: Duration) -> Answer {
    let item = |kind| Item { key: key.into(), ballot: ballot.clone(), kind };
    assert_eq!(a.answer(&item(Kind::Prepare), now, ballot.epoch), Answer::Promised(None), "{key}");
    a.answer(&item(Kind::Propose(lease)), now, ballot.epoch)
}

fn keys(listed: Option<(Vec<Arc<str>>, bool)>) -> Vec<String> {
    let (keys, more) = listed.expect("warm");
    assert!(!more);
    keys.iter().map(|k| k.to_string()).collect()
}

/// The bytes of the acceptor's log. When this fails you changed the format: bump `LOG_VERSION` and re-pin, and an
/// acceptor started on an older log forgets it (it reads a log whose header it does not know as empty). A record added
/// at the end of a rewrite, as `TOP` was, keeps the version: an older build reads the leases before it.
#[test]
fn log_bytes_are_pinned_to_the_version() {
    assert_eq!(LOG_VERSION, 2, "LOG_VERSION moved: re-pin the bytes below");
    const LOG: &[u8] = &[
        0x00, 0x00, 0x00, 0x0f, 0xf8, 0xc7, 0xcf, 0x02, 0x00, 0x41, 0x58, 0x4c, 0x53, 0x00, 0x02, 0x01, 0x02, 0x03,
        0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x13, 0x0c, 0xfb, 0x4a, 0xce, 0x01, 0x00, 0x03, 0x65, 0x2f,
        0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x03, 0x6e, 0x2f, 0x31, 0x00, 0x00, 0x00, 0x0e,
        0x98, 0xcc, 0x87, 0x67, 0x02, 0x00, 0x03, 0x65, 0x2f, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
        0x00, 0x00, 0x00, 0x06, 0xa3, 0x83, 0xf5, 0x95, 0x03, 0x00, 0x03, 0x65, 0x2f, 0x6b, 0x00, 0x00, 0x00, 0x09,
        0xc2, 0x39, 0xfd, 0x06, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
    ];
    let records = vec![
        Record::Grant("e/k".into(), ballot(7, "n/1")),
        Record::Release("e/k".into(), 7),
        Record::Forget("e/k".into()),
        Record::Top(9),
    ];
    let mut out = BytesMut::new();
    put_log_header(&mut out, P);
    records.iter().for_each(|r| put_record(&mut out, r));
    assert_eq!(&out[..], LOG, "log encoding changed");
    assert_eq!(decode_log(Bytes::from_static(LOG)), Some((P, records.clone())));
    // A crash mid-append leaves a torn frame, and a bad sector one whose CRC fails: both end the log there.
    let ends = [23, 50, 72, 86, 103];
    for n in 0..LOG.len() {
        let whole = (n >= 23).then(|| ends.iter().filter(|end| **end <= n).count() - 1);
        assert_eq!(decode_log(Bytes::from_static(&LOG[..n])).map(|(_, r)| r.len()), whole, "cut at {n}");
    }
    let mut rotten = LOG.to_vec();
    rotten[60] ^= 1;
    assert_eq!(decode_log(rotten.into()), Some((P, records[..1].to_vec())), "a frame whose CRC fails ends it");
    let bad_header = [&[0, 0, 0, 15, 0, 0, 0, 0][..], &LOG[8..23]].concat();
    assert_eq!(decode_log(bad_header.into()), None, "a header whose CRC fails");
}

/// A restart of every acceptor at once: from its log each lists which node held which key and was not released,
/// for `held_by` and the sweep, and a release of the ballot it logged still clears it. The key promises only the
/// floor, so the logged ballot itself is refused, as is a late prepare from before the restart.
#[test]
fn a_restarted_acceptor_lists_from_its_log_and_promises_only_the_floor() {
    let wait = Duration::from_secs(6);
    let (mut a, dir) =
        (Acceptor::new(Duration::ZERO, U, wait, 16).with_log(Stored::default(), P, Duration::ZERO), MemoryFiles::new());
    let epoch = U + wait.as_micros() as u64 + 1;
    let (k, j, r) = (ballot(epoch, "n/1"), ballot(epoch + 1, "m/1"), ballot(epoch + 2, "n/1"));
    for (key, b) in [("e/k", &k), ("e/j", &j), ("e/r", &r), ("e/gone", &r)] {
        assert_eq!(grant(&mut a, key, b, LEASE, wait), Answer::Accepted);
    }
    a.answer(&Item { key: "e/r".into(), ballot: r.clone(), kind: Kind::Release }, wait, epoch);
    flush(&mut a, &dir);
    let renew = Item { key: "e/k".into(), ballot: k.clone(), kind: Kind::Propose(wait) };
    let written = appended(&a);
    assert_eq!(a.answer(&renew, wait, epoch), Answer::Accepted);
    assert_eq!(appended(&a), written, "a renewal writes nothing");
    assert_eq!(a.waits_for(&[renew]), None, "nor waits for the log");
    a.sweep(wait * 2 + LISTED);
    assert_eq!(appended(&a), written + 3, "the sweep forgets each lease it drops, not e/r's released slot");
    let forgot = replay(&dir);
    assert_eq!(forgot.leases.len(), 3, "not written yet");
    flush(&mut a, &dir);
    assert!(replay(&dir).leases.is_empty(), "forgotten in the log too");
    for (key, b) in [("e/k", &k), ("e/j", &j), ("e/gone", &r)] {
        let next = Ballot { epoch: b.epoch + LISTED.as_micros() as u64 * 3, ..b.clone() };
        assert_eq!(grant(&mut a, key, &next, LEASE, wait * 2 + LISTED), Answer::Accepted);
    }
    flush(&mut a, &dir);

    let (now, unix) = (Duration::from_secs(1000), U + 1_000_000_000);
    let stored = replay(&dir);
    assert_eq!(stored.leases.len(), 3);
    let mut b = Acceptor::new(now, unix, wait, 16).with_log(stored, P, now);
    let kinds = ["e".to_string()];
    let (n, m) = (Holder::Node("n"), Holder::Node("m"));
    assert_eq!(b.held_by(n, &kinds, "", now), None, "warming");
    let warm = now + wait;
    assert_eq!(keys(b.held_by(n, &kinds, "", warm)), ["e/gone", "e/k"]);
    assert_eq!(keys(b.held_by(m, &kinds, "", warm)), ["e/j"]);
    assert!(keys(b.held_by(Holder::Node(""), &kinds, "", warm)).is_empty(), "a node named so holds nothing");
    assert_eq!(keys(b.held_by(Holder::Lapsed, &kinds, "", warm)), ["e/gone", "e/j", "e/k"], "every lapsed lease");
    assert_eq!(b.leases(warm), 0, "none live");
    assert_eq!(b.stored(), 3);

    let logged = b.slots["e/k"].lease.clone().expect("restored").0;
    let propose = Item { key: "e/k".into(), ballot: logged.clone(), kind: Kind::Propose(wait) };
    assert_eq!(b.answer(&propose, warm, unix), Answer::Refused(b.floor()), "the logged ballot is not a promise");
    let late = Item { key: "e/k".into(), ballot: ballot(b.floor() - 1, "q/1"), kind: Kind::Prepare };
    assert!(logged.epoch < late.ballot.epoch);
    assert_eq!(b.answer(&late, warm, unix), Answer::Refused(b.floor()), "nor anything below the floor");
    let release = Item {
        key: "e/gone".into(),
        ballot: b.slots["e/gone"].lease.clone().expect("restored").0,
        kind: Kind::Release,
    };
    assert_eq!(b.answer(&release, warm, unix), Answer::Accepted);
    assert_eq!(keys(b.held_by(n, &kinds, "", warm)), ["e/k"], "released as logged");
    let next = ballot(b.floor() + 1, "o/1");
    assert_eq!(grant(&mut b, "e/k", &next, LEASE, warm), Answer::Accepted, "a lapsed lease is free");
    assert_eq!(keys(b.held_by(Holder::Lapsed, &kinds, "", warm)), ["e/j"], "a live lease is not listed by the sweep");
    flush(&mut b, &dir);
    let stored = replay(&dir);
    assert_eq!(stored.leases.keys().map(|k| &**k).collect::<Vec<_>>(), ["e/j", "e/k"]);
    assert_eq!(stored.leases["e/k"], ballot(b.floor() + 1, "o/1"));
    assert_eq!(names(&dir), [LOG_FILE], "one file, rewritten in place");
}

/// A key's epoch never goes back, whatever the clocks do. Clocks ran an hour ahead: a node took the key, released it,
/// and the acceptor restarted, its start's rewrite holding no lease. Then the clocks are set right and the acceptor
/// restarts again: its floor is the highest epoch its log holds, not its clock, so it refuses every epoch up to the
/// first, and the proposer retries above it.
#[test]
fn a_keys_epoch_never_goes_back_after_a_clock_steps_back_across_restarts() {
    const HOUR: u64 = 3_600_000_000;
    let (wait, dir) = (Duration::from_secs(6), MemoryFiles::new());
    let wait_us = wait.as_micros() as u64;
    let start = |now: Duration, unix: u64| {
        let mut a = Acceptor::new(now, unix, wait, 16).with_log(replay(&dir), P, now);
        flush(&mut a, &dir);
        a
    };
    let mut a = start(Duration::ZERO, U + HOUR);
    let first = ballot(U + HOUR + wait_us + 1, "n/1");
    assert_eq!(grant(&mut a, "e/k", &first, LEASE, wait), Answer::Accepted);
    a.answer(&Item { key: "e/k".into(), ballot: first.clone(), kind: Kind::Release }, wait, first.epoch);
    flush(&mut a, &dir);
    drop(start(wait, U + HOUR + wait_us));
    assert!(replay(&dir).leases.is_empty(), "released, and rewritten without it");

    let mut a = start(wait, U);
    let prepare = Item { key: "e/k".into(), ballot: ballot(U + 2 * wait_us, "m/1"), kind: Kind::Prepare };
    assert_eq!(a.answer(&prepare, wait * 2, U + wait_us), Answer::Refused(first.epoch), "the clock's epoch");
}

/// The leases an acceptor's slots hold: what its log says once every frame logged is written.
fn held(a: &Acceptor) -> BTreeMap<Arc<str>, Ballot> {
    a.slots.iter().filter_map(|(key, slot)| Some((key.clone(), slot.lease.as_ref()?.0.clone()))).collect()
}

/// What a crash inside `ops` at each point leaves on a copy of `dir`, read back.
fn crashes(dir: &MemoryFiles, ops: &[FileOp]) -> impl Iterator<Item = (u64, BTreeMap<Arc<str>, Ballot>)> {
    axon_testkit::crashes(dir, ops).map(|(n, copy)| (n, replay(&copy).leases))
}

/// A crash anywhere inside a write leaves a log that reads back the leases before it, or after one of its frames: an
/// append's frames in order up to a torn one, a rewrite's all or none. A rewrite that cut the file and wrote it again,
/// or removed it and wrote it anew, would read back some of its leases and lose the others.
#[test]
fn a_crash_inside_a_write_reads_back_the_log_before_it_or_after() {
    let mut a = Acceptor::new(Duration::ZERO, U, Duration::ZERO, 16).with_log(Stored::default(), P, Duration::ZERO);
    let dir = MemoryFiles::new();
    let release = |a: &mut Acceptor, key: &str, b: &Ballot| {
        a.answer(&Item { key: key.into(), ballot: b.clone(), kind: Kind::Release }, Duration::ZERO, U);
    };
    let (n, m) = (ballot(U + 1, "n/1"), ballot(U + 2, "m/1"));
    for key in ["e/a", "e/b", "e/c"] {
        assert_eq!(grant(&mut a, key, &n, LEASE, Duration::ZERO), Answer::Accepted);
    }
    release(&mut a, "e/c", &n);
    flush(&mut a, &dir);
    let synced = held(&a);
    assert_eq!(replay(&dir).leases, synced);

    // An append: each frame's leases in turn.
    let mut states = vec![synced.clone()];
    assert_eq!(grant(&mut a, "e/d", &m, LEASE, Duration::ZERO), Answer::Accepted);
    states.push(held(&a));
    release(&mut a, "e/a", &n);
    states.push(held(&a));
    let ops = a.take_write().expect("the append");
    assert!(matches!(&ops[..], [FileOp::Append(..)]), "{ops:?}");
    for (at, read) in crashes(&dir, &ops) {
        assert!(states.contains(&read), "a crash at {at} read back {read:?}");
    }
    // It fails, and the store could not cut back what it wrote of it: the rewrite after it replaces those bytes.
    let [FileOp::Append(name, data)] = &ops[..] else { unreachable!() };
    apply(&dir, vec![FileOp::Append(name.clone(), data.slice(..30))]);
    a.written(false);
    let before = replay(&dir).leases;
    assert_eq!(before, states[1], "the failed write left its first frame");

    // A rewrite: what the file held, or every lease.
    assert_eq!(grant(&mut a, "e/e", &m, LEASE, Duration::ZERO), Answer::Accepted);
    let ops = a.take_write().expect("the rewrite after the failure");
    assert!(matches!(&ops[..], [FileOp::Replace(name, _)] if name == LOG_FILE), "{ops:?}");
    let after = held(&a);
    let mut seen = (false, false);
    for (at, read) in crashes(&dir, &ops) {
        assert!(read == before || read == after, "a crash at {at} read back {read:?}");
        seen = (seen.0 || read == before, seen.1 || read == after);
    }
    assert_eq!(seen, (true, true), "crashes land the rewrite and miss it");
    apply(&dir, ops);
    a.written(true);
    assert_eq!(replay(&dir).leases, after);
}

/// A log of another version or configuration lists nothing, and the start's rewrite replaces it; other names, the
/// numbered files of version 1 among them, are neither read nor removed.
#[test]
fn a_log_of_another_version_or_configuration_is_replaced() {
    let file = |version: u16, protocol: u64| {
        let mut out = BytesMut::new();
        put_frame(&mut out, |out| {
            out.put_u8(LOG_HEADER);
            out.put_slice(LOG_MAGIC);
            out.put_u16(version);
            out.put_u64(protocol);
        });
        put_record(&mut out, &Record::Grant("e/k".into(), ballot(U, "n/1")));
        out.freeze()
    };
    let dir = MemoryFiles::new();
    apply(&dir, vec![FileOp::Append(LOG_FILE.into(), file(LOG_VERSION, P))]);
    assert_eq!(replay(&dir).leases.len(), 1, "this version's");
    // Another configuration's lists nothing, but its epochs still raise the floor, so a key's epoch does not go back
    // across a change of `lease`, `drift` or acceptors.
    for (log, top) in [(file(1, P), 0), (file(LOG_VERSION, P + 1), U)] {
        let dir = MemoryFiles::new();
        // A header this version reads: what it lists from there would show that a numbered file was read.
        let old = FileOp::Append("leases-1.log".into(), file(LOG_VERSION, P));
        apply(&dir, vec![FileOp::Replace(LOG_FILE.into(), log), old]);
        let stored = replay(&dir);
        assert!(stored.leases.is_empty());
        assert_eq!(stored.top, top);
        let mut a = Acceptor::new(Duration::ZERO, 0, Duration::ZERO, 16).with_log(stored, P, Duration::ZERO);
        assert_eq!(a.floor(), top);
        let ops = a.take_write().expect("the start's rewrite");
        assert!(matches!(&ops[..], [FileOp::Replace(name, _)] if name == LOG_FILE), "{ops:?}");
        apply(&dir, ops);
        a.written(true);
        assert_eq!(names(&dir), ["leases-1.log", LOG_FILE]);
        assert!(replay(&dir).leases.is_empty());
        assert_eq!(replay(&dir).top, top, "and the rewrite keeps it");
    }
}

/// The log is rewritten at the start, once it outgrows its last rewrite, and after a failed write; while writes fail,
/// answers do not wait for them, and too much left unwritten refuses grants but not renewals.
#[test]
fn the_log_is_rewritten_when_it_grows_or_a_write_fails() {
    let mut a =
        Acceptor::new(Duration::ZERO, U, Duration::ZERO, 100_000).with_log(Stored::default(), P, Duration::ZERO);
    let dir = MemoryFiles::new();
    let rewrite = |ops: &[FileOp]| matches!(ops, [FileOp::Replace(name, _)] if name == LOG_FILE);
    let ops = a.take_write().expect("the start's rewrite");
    assert!(rewrite(&ops), "{ops:?}");
    apply(&dir, ops);
    a.written(true);
    assert_eq!(a.take_write(), None, "nothing to write");

    let big = |i: usize| format!("e/{i:04}{}", "x".repeat(2000));
    let held = ballot(U + 1, "n/1");
    for i in 0..600 {
        assert_eq!(grant(&mut a, &big(i), &held, LEASE, Duration::ZERO), Answer::Accepted);
        a.answer(&Item { key: big(i).into(), ballot: held.clone(), kind: Kind::Release }, Duration::ZERO, U);
    }
    assert_eq!(grant(&mut a, "e/kept", &held, LEASE, Duration::ZERO), Answer::Accepted);
    let ops = a.take_write().expect("past the size of the last rewrite");
    assert!(rewrite(&ops), "{ops:?}");
    assert!(matches!(&ops[0], FileOp::Replace(_, data) if data.len() < 100), "one grant");
    apply(&dir, ops);
    a.written(true);

    assert_eq!(grant(&mut a, "e/new", &held, LEASE, Duration::ZERO), Answer::Accepted);
    let new = [Item { key: "e/new".into(), ballot: held.clone(), kind: Kind::Release }];
    assert!(a.waits_for(&new).is_some(), "an answer about it waits for its grant");
    let prepare = Item { key: "e/new".into(), ballot: ballot(U + 2, "m/1"), kind: Kind::Prepare };
    assert_eq!(a.waits_for(&[prepare]), None, "a promise does not: a grant waits before it is answered");
    let kept = Item { key: "e/kept".into(), ballot: held.clone(), kind: Kind::Propose(Duration::from_secs(6)) };
    assert_eq!(a.waits_for(&[kept]), None, "one about a key whose grant is synced does not");
    assert!(a.take_write().is_some());
    a.written(false);
    assert!(a.store_failing() && a.waits_for(&new).is_none(), "answers no longer wait");
    let ops = a.take_write().expect("a rewrite after the failure");
    assert!(rewrite(&ops), "{ops:?}");
    a.written(false);
    let ops = a.take_write().expect("the rewrite again");
    assert!(rewrite(&ops), "{ops:?}");
    apply(&dir, ops);
    a.written(true);
    assert!(!a.store_failing() && a.waits_for(&new).is_none(), "the rewrite holds it");
    flush(&mut a, &dir);
    assert_eq!(names(&dir), [LOG_FILE]);
    assert_eq!(replay(&dir).leases.len(), 2, "e/kept and e/new");

    let out = a.take_write();
    assert_eq!(out, None);
    let mut granted = 0;
    while grant(&mut a, &big(granted), &held, LEASE, Duration::ZERO) == Answer::Accepted {
        granted += 1;
    }
    assert!(granted * 2100 >= MAX_UNWRITTEN && a.store_behind == 1, "{granted} granted before the log was behind");
    let propose = |key: &str, b: &Ballot| Item {
        key: key.into(),
        ballot: b.clone(),
        kind: Kind::Propose(Duration::from_secs(6)),
    };
    let other = propose("e/other", &ballot(U + 2, "m/1"));
    assert_eq!(a.answer(&other, Duration::ZERO, U), Answer::Full, "a grant waits for the log");
    assert_eq!(a.answer(&propose("e/kept", &held), Duration::ZERO, U), Answer::Accepted, "a renewal does not");
    assert_eq!(a.store_behind, 2);
}

/// A log grows dead frames only by releases, forgets and new holders: one of grants alone, however large, is appended
/// to and never rewritten.
#[test]
fn a_log_of_grants_alone_is_never_rewritten() {
    let mut a =
        Acceptor::new(Duration::ZERO, U, Duration::ZERO, 1_000_000).with_log(Stored::default(), P, Duration::ZERO);
    let dir = MemoryFiles::new();
    flush(&mut a, &dir);
    let (held, mut granted, mut written) = (ballot(U + 1, "n/1"), 0, 0);
    while written < 3 << 20 {
        for _ in 0..1000 {
            assert_eq!(grant(&mut a, &format!("e/{granted:08}"), &held, LEASE, Duration::ZERO), Answer::Accepted);
            granted += 1;
        }
        let ops = a.take_write().expect("the grants");
        let [FileOp::Append(name, data)] = &ops[..] else { panic!("{granted} granted, then {ops:?}") };
        assert_eq!(name, LOG_FILE);
        written += data.len();
        apply(&dir, ops);
        a.written(true);
    }
    assert_eq!(a.stored(), granted);
    assert_eq!(replay(&dir).leases.len(), granted);
}

/// A log that cannot be read fails the start and says which file, a directory among them: the start's rewrite could
/// not replace it.
#[test]
fn a_log_that_cannot_be_read_fails_the_start() {
    struct Unreadable(io::ErrorKind);
    impl Files for Unreadable {
        fn list(&self) -> FileFuture<Vec<(String, u64)>> {
            Box::pin(std::future::ready(Ok(vec![(LOG_FILE.into(), 0)])))
        }
        fn read(&self, _: &str) -> FileFuture<Vec<u8>> {
            Box::pin(std::future::ready(Err(self.0.into())))
        }
        fn apply(&self, _: Vec<FileOp>) -> FileFuture<()> {
            Box::pin(std::future::ready(Ok(())))
        }
    }
    for kind in [io::ErrorKind::IsADirectory, io::ErrorKind::PermissionDenied] {
        let Err(e) = ready(Stored::load(&Unreadable(kind), P)) else { panic!("an unreadable log") };
        assert!(e.kind() == kind && e.to_string().contains(LOG_FILE), "{e}");
    }
}

/// A renewal goes to the acceptors that logged its grant in a lane of its own, so it waits for no other key's write:
/// with every acceptor's log behind on a new key's grant, the key held is still renewed by those that accepted it,
/// the one heard only after the majority too.
#[test]
fn a_renewal_waits_for_no_other_keys_write() {
    let (mut net, mut p) = (net(16, Duration::ZERO).logged(), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("e/held".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![Ok(U)]);

    net.land = false;
    let mut out = Vec::new();
    p.acquire("e/new".into(), Duration::ZERO, U + 1, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![]);
    assert_eq!(net.waiting, 3, "each acceptor's answer to e/new waits for its log");

    // Within e/new's round, so that round is still out.
    net.now = Duration::from_millis(500);
    let mut out = Vec::new();
    p.renew(net.now, &mut out);
    let sent: Vec<_> = out
        .iter()
        .map(|o| match o {
            Out::Send { acceptor, items, .. } => (*acceptor, items.len()),
            _ => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(sent, [(0, 1), (1, 1), (2, 1)], "to all three, 2 heard after the grant, without e/new");
    assert_eq!(net.settle(&mut p, out, U), vec![]);
    assert_eq!(net.waiting, 3, "their answers wait for nothing");
    assert!(!p.renewing());
    assert_eq!(p.holds("e/held", net.now + Duration::from_millis(4900)), Some(U), "renewed");
}

/// A renewal answered after the lease ran out must not revive it.
#[test]
fn a_renewal_that_comes_back_late_does_not_extend() {
    let (mut net, mut p) = (net(16, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("k".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![Ok(U)]);
    let mut out = Vec::new();
    p.renew(Duration::from_millis(4500), &mut out);
    // Past the 5 s the proposer holds it, within the renewal's round.
    net.now = Duration::from_millis(5001);
    assert_eq!(net.settle(&mut p, out, U), vec![Err(OwnershipError::Lost)]);
    assert_eq!(p.holds("k", net.now), None);
}

struct Net {
    acceptors: Vec<Acceptor>,
    /// When requests are answered, on the acceptors' and the proposer's clock alike.
    now: Duration,
    /// Whether an acceptor's log write lands before it answers.
    land: bool,
    /// Requests delivered.
    sends: usize,
    /// Requests whose answers wait for an acceptor's log, never delivered.
    waiting: usize,
}

/// Three acceptors, each tracking up to `max` keys and refusing everything for `wait` after the start.
fn net(max: usize, wait: Duration) -> Net {
    let acceptors = (0..3).map(|_| Acceptor::new(Duration::ZERO, 0, wait, max)).collect();
    Net { acceptors, now: Duration::ZERO, land: true, sends: 0, waiting: 0 }
}

/// Lease 6 s, held 5 s, a round of 1 s.
fn proposer(node: &str) -> Proposer {
    Proposer::new(node.into(), 3, LEASE, Duration::from_secs(5), Duration::from_secs(1))
}

impl Net {
    /// Each acceptor with a log, its start's rewrite landed.
    fn logged(mut self) -> Self {
        self.acceptors = (self.acceptors.into_iter())
            .map(|a| {
                let mut a = a.with_log(Stored::default(), P, Duration::ZERO);
                a.take_write().expect("the start's rewrite");
                a.written(true);
                a
            })
            .collect();
        self
    }

    /// Delivers the sends and passes on what the proposer does next, decisions included.
    fn step(&mut self, p: &mut Proposer, out: Vec<Out>, unix_us: u64) -> Vec<Out> {
        let mut next = Vec::new();
        for o in out {
            let Out::Send { request, acceptor, items } = o else {
                next.push(o);
                continue;
            };
            self.sends += 1;
            let a = &mut self.acceptors[acceptor];
            let (answers, frame) = a.answer_all(&items, self.now, unix_us);
            while self.land && a.take_write().is_some() {
                a.written(true);
            }
            if frame.is_some_and(|f| f > a.synced()) {
                self.waiting += 1;
            } else {
                p.reply(request, Some(answers), self.now, unix_us, &mut next);
            }
        }
        next
    }

    /// Steps until the proposer has nothing left to send, and returns what it decided.
    fn settle(&mut self, p: &mut Proposer, mut out: Vec<Out>, unix_us: u64) -> Vec<Result<u64, OwnershipError>> {
        while out.iter().any(|o| matches!(o, Out::Send { .. })) {
            out = self.step(p, out, unix_us);
        }
        out.into_iter().filter_map(|o| if let Out::Done { result, .. } = o { Some(result) } else { None }).collect()
    }
}

/// The model's counterexample to ordering ballots by `(epoch, node)`: two nodes refused by one
/// promise both retry one above it, and both would win the key with the same epoch.
#[test]
fn two_nodes_never_win_one_epoch() {
    let mut net = net(16, Duration::ZERO);
    let (mut ahead, mut a, mut b) = (proposer("x/1"), proposer("a/1"), proposer("b/1"));

    let mut out = Vec::new();
    ahead.acquire("k".into(), Duration::ZERO, U + 1000, &mut out);
    assert_eq!(net.settle(&mut ahead, out, U + 1000), vec![Ok(U + 1000)]);
    let mut out = Vec::new();
    ahead.release("k", &mut out);
    net.settle(&mut ahead, out, U + 1000);

    let (mut out_a, mut out_b) = (Vec::new(), Vec::new());
    a.acquire("k".into(), Duration::ZERO, U, &mut out_a);
    b.acquire("k".into(), Duration::ZERO, U, &mut out_b);
    let retry_a = net.step(&mut a, out_a, U);
    let retry_b = net.step(&mut b, out_b, U);
    assert_eq!(net.settle(&mut a, retry_a, U), vec![Ok(U + 1001)]);
    let mut out = Vec::new();
    a.release("k", &mut out);
    net.settle(&mut a, out, U);
    let won = net.settle(&mut b, retry_b, U);
    assert!(!won.contains(&Ok(U + 1001)), "b won epoch {} after a: {won:?}", U + 1001);
}

/// The trace of a rolling restart: one acceptor refuses the renewal, one is warming and the
/// last accepts. Every acceptor has answered, so the round goes on without waiting for more.
#[test]
fn a_round_every_acceptor_answered_moves_on() {
    let (mut net, mut p) = (net(16, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("k".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![Ok(U)]);

    let mut sends = Vec::new();
    p.renew(Duration::ZERO, &mut sends);
    let mut out = Vec::new();
    for (o, answer) in sends.into_iter().zip([Answer::Accepted, Answer::Refused(U + 1), Answer::Warming]).rev() {
        let Out::Send { request, .. } = o else { panic!("{o:?}") };
        p.reply(request, Some(vec![answer]), Duration::ZERO, U, &mut out);
    }
    assert!(
        matches!(&out[..], [Out::Send { items, .. }, ..] if matches!(items[..], [Item { kind: Kind::Prepare, .. }])),
        "{out:?}"
    );
}

/// A node whose view says it owns a key asks for it again and again and is answered `Held`, naming the holder. Those
/// prepares must not refuse the holder's renewal: on the soak, they made a holder miss its grace
/// and drop every key it had.
#[test]
fn a_prepare_answered_held_leaves_the_holders_renewal_alone() {
    let mut net = net(16, Duration::ZERO);
    let (mut holder, mut other) = (proposer("a/1"), proposer("b/1"));
    let mut out = Vec::new();
    holder.acquire("k".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut holder, out, U), vec![Ok(U)]);

    let mut renewal = Vec::new();
    holder.renew(Duration::ZERO, &mut renewal);
    for unix_us in [U + 1000, U + 2000] {
        let mut out = Vec::new();
        other.acquire("k".into(), Duration::ZERO, unix_us, &mut out);
        assert_eq!(net.settle(&mut other, out, unix_us), vec![Err(OwnershipError::Held(Some("a".into())))]);
        renewal = net.step(&mut holder, renewal, unix_us);
    }
    // A renewal that succeeds says nothing.
    assert_eq!(net.settle(&mut holder, renewal, U + 2000), vec![]);
    assert!(!holder.renewing());
}

/// A key released while its renewal is out is the caller's doing, not a failed renewal: a
/// rebalance releasing keys every few seconds made whole nodes miss their grace and stop.
#[test]
fn a_release_during_a_renewal_does_not_fail_it() {
    let (mut net, mut p) = (net(16, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("a".into(), Duration::ZERO, U, &mut out);
    p.acquire("b".into(), Duration::ZERO, U, &mut out);
    assert!(net.settle(&mut p, out, U).iter().all(Result::is_ok));

    let mut sends = Vec::new();
    p.renew(Duration::ZERO, &mut sends);
    let mut out = Vec::new();
    p.release("a", &mut out);
    out.extend(sends);
    let done = net.settle(&mut p, out, U);
    assert!(done.iter().all(Result::is_ok), "{done:?}");
    assert!(!p.renewing());
    assert_eq!(p.holds("b", Duration::ZERO), Some(U + 1));
}

/// A release lands after the renewal of its key that went before it in the other lane: landing first, it would leave
/// the renewal to grant the key again, held by nobody until it runs out.
#[test]
fn a_release_never_overtakes_its_keys_renewal() {
    let (mut net, mut p) = (net(16, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("k".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![Ok(U)]);

    let mut out = Vec::new();
    p.renew(Duration::ZERO, &mut out);
    p.release("k", &mut out);
    // Newest first, as sends that overtake each other.
    out.reverse();
    assert_eq!(net.settle(&mut p, out, U), vec![]);
    let leased: Vec<_> = net.acceptors.iter().map(|a| a.leases(Duration::ZERO)).collect();
    assert_eq!(leased, [0, 0, 0], "a lease left behind the release");
}

/// A renewal still queued when its key is released is never sent: going after the release, it would grant the key
/// again.
#[test]
fn a_renewal_queued_behind_a_release_is_dropped() {
    let (mut net, mut p) = (net(16, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("k".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![Ok(U)]);
    // Another key's prepares take the quick lanes, so the renewal waits behind them.
    let (mut prepares, mut out) = (Vec::new(), Vec::new());
    p.acquire("other".into(), Duration::ZERO, U + 1, &mut prepares);
    p.renew(Duration::ZERO, &mut out);
    assert!(out.is_empty(), "{out:?}");
    p.release("k", &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![]);
    assert_eq!(net.settle(&mut p, prepares, U + 1), vec![Ok(U + 1)]);
    let leased: Vec<_> = net.acceptors.iter().map(|a| a.leases(Duration::ZERO)).collect();
    assert_eq!(leased, [1, 1, 1], "k leased again behind its release");
}

/// A renewal ends a lease that ran out, and leaves alone the acquire that is taking its key again.
#[test]
fn a_renewal_after_a_lease_ran_out_leaves_a_new_acquire_of_it_alone() {
    let (mut net, mut p) = (net(16, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("k".into(), Duration::ZERO, U, &mut out);
    assert_eq!(net.settle(&mut p, out, U), vec![Ok(U)]);

    let (late, later) = (Duration::from_secs(5), U + 5_000_000);
    let mut out = Vec::new();
    p.acquire("k".into(), late, later, &mut out);
    p.renew(late, &mut out);
    let done = net.settle(&mut p, out, later);
    assert!(done.len() == 2 && done.contains(&Err(OwnershipError::Lost)) && done.contains(&Ok(later)), "{done:?}");
}

#[test]
fn a_failed_round_says_why() {
    let says = |done: &[Result<u64, OwnershipError>], what: &str| {
        done.iter().any(|r| matches!(r, Err(OwnershipError::Unavailable(why)) if why.contains(what)))
    };
    let (mut net, mut p) = (net(1, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    p.acquire("a".into(), Duration::ZERO, U, &mut out);
    p.acquire("b".into(), Duration::ZERO, U, &mut out);
    let done = net.settle(&mut p, out, U);
    assert!(done.contains(&Ok(U)) && says(&done, "max_leases"), "{done:?}");

    let mut net = self::net(16, Duration::from_secs(6));
    let mut out = Vec::new();
    p.acquire("c".into(), Duration::ZERO, U, &mut out);
    let done = net.settle(&mut p, out, U);
    assert!(says(&done, "no majority: acceptors are waiting out a restart"), "{done:?}");

    // One acceptor never answers, so the round runs out of time; it still says what the others said.
    let mut out = Vec::new();
    p.acquire("d".into(), Duration::ZERO, U, &mut out);
    out.retain(|o| !matches!(o, Out::Send { acceptor: 2, .. }));
    let mut out = net.step(&mut p, out, U);
    p.tick(Duration::from_secs(1), &mut out);
    let done = net.settle(&mut p, out, U);
    assert!(says(&done, "in time: acceptors are waiting out a restart"), "{done:?}");

    // Requests that fail or time out, as to acceptors alive on two starved cores, are no vote: a bare `no majority`
    // read as if they had refused. A warming answer after one says more, and wins.
    let unavailable = |why: &str| Err(OwnershipError::Unavailable(format!("no majority: {why}")));
    for (key, warming, why) in [("e", None, NO_ANSWER), ("f", Some(1), "acceptors are waiting out a restart")] {
        let (mut p, mut sends, mut out) = (proposer("n/2"), Vec::new(), Vec::new());
        p.acquire(key.into(), Duration::ZERO, U, &mut sends);
        for o in sends {
            let Out::Send { request, acceptor, .. } = o else { panic!("{o:?}") };
            let answer = (warming == Some(acceptor)).then(|| vec![Answer::Warming]);
            p.reply(request, answer, Duration::ZERO, U, &mut out);
        }
        assert!(matches!(&out[..], [Out::Done { result, .. }] if *result == unavailable(why)), "{out:?}");
    }
}

/// A burst of keys leaves as a few requests per acceptor, not one per key, and every key is granted.
#[test]
fn a_burst_of_keys_shares_requests() {
    let (mut net, mut p) = (net(16_000, Duration::ZERO), proposer("n/1"));
    let mut out = Vec::new();
    for i in 0..1000 {
        p.acquire(format!("k{i}").into(), Duration::ZERO, U, &mut out);
    }
    assert_eq!(out.len(), 3 * IN_FLIGHT, "the rest wait for an answer and leave together");
    let done = net.settle(&mut p, out, U);
    // Two phases to three acceptors, prepares in one lane and proposes in the other, each request up to `MAX_ITEMS`:
    // 18 requests where one per key would be 6000.
    assert!(net.sends <= 18, "{} requests", net.sends);
    assert_eq!(done.len(), 1000);
    assert!(done.iter().all(Result::is_ok), "{done:?}");
}
