use super::*;

fn fixtures() -> (Interest, Push) {
    let key = StreamKey::new("md", "btc");
    let interest = Interest {
        revision: 9,
        base: 8,
        whole: Some((Some(StreamKey::new("md", "a")), None)),
        drops: vec![StreamKey::new("md", "sol")],
        wants: vec![
            Want {
                key: key.clone(),
                epoch: 3,
                start: StartPosition::After(Cursor { generation: 1, sequence: 40 }),
                snapshot: true,
            },
            Want { key: StreamKey::new("md", "eth"), epoch: 4, start: StartPosition::Live, snapshot: false },
            Want { key: StreamKey::new("md", "ada"), epoch: 5, start: StartPosition::First, snapshot: false },
        ],
    };
    let batch = SequencedBatch::new(1, 41, vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")]).unwrap();
    let push = Push {
        items: vec![
            (key.clone(), 3, Item::Ready(Cursor { generation: 1, sequence: 40 })),
            (key.clone(), 3, Item::Batch(batch.clone())),
            (key.clone(), 3, Item::Batch(batch.clone().traced(Some((7, 8))))),
            (key.clone(), 3, Item::CaughtUp),
            (key.clone(), 3, Item::Snapshot(Snapshot::new(Cursor { generation: 1, sequence: 42 }, batch.records()))),
            (key.clone(), 3, Item::Reset(ResetReason::GlobalCapacity)),
            (key.clone(), 3, Item::Path(vec!["o".into(), "relay-1".into()])),
            (key, 3, Item::End(EndReason::UpstreamLost)),
        ],
    };
    (interest, push)
}

/// The bytes peers see for `SCHEMA` 7. When this fails you changed the protocol: bump `SCHEMA` in `node.rs`
/// and re-pin, or nodes on the old schema refuse frames they cannot decode.
#[test]
fn wire_bytes_are_pinned_to_the_schema() {
    #[cfg(feature = "cluster")]
    assert_eq!(crate::node::SCHEMA, 7, "SCHEMA moved: re-pin the bytes below");
    let (interest, push) = fixtures();
    let answers = [Answer { status: Status::Ok, path: vec!["o".into(), "r".into()] }, Status::Unavailable.into()];
    const INTEREST_BYTES: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x03,
        0x00, 0x02, 0x6d, 0x64, 0x00, 0x01, 0x61, 0x00, 0x00, 0x00, 0x03, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x62,
        0x74, 0x63, 0x00, 0x00, 0x00, 0x03, 0x81, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x65, 0x74, 0x68, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x61, 0x64, 0x61, 0x00, 0x00, 0x00, 0x05, 0x02, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x73, 0x6f, 0x6c,
    ];
    const PUSH_BYTES: &[u8] = &[
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
        0x00, 0x00, 0x08, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x02, 0x6d,
        0x64, 0x00, 0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x61,
        0x00, 0x00, 0x00, 0x02, 0x62, 0x63, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00,
        0x03, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x61, 0x00, 0x00, 0x00, 0x02, 0x62, 0x63, 0x00, 0x02, 0x6d, 0x64, 0x00,
        0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x62, 0x74, 0x63,
        0x00, 0x00, 0x00, 0x03, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x61, 0x00, 0x00, 0x00, 0x02, 0x62, 0x63,
        0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00, 0x03, 0x03, 0x05, 0x00, 0x02, 0x6d,
        0x64, 0x00, 0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00, 0x03, 0x06, 0x02, 0x00, 0x01, 0x6f, 0x00, 0x07, 0x72,
        0x65, 0x6c, 0x61, 0x79, 0x2d, 0x31, 0x00, 0x02, 0x6d, 0x64, 0x00, 0x03, 0x62, 0x74, 0x63, 0x00, 0x00, 0x00,
        0x03, 0x04, 0x06,
    ];
    const ANSWERS_BYTES: &[u8] =
        &[0x00, 0x02, 0x02, 0x00, 0x01, 0x6f, 0x00, 0x01, 0x72, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x01];
    const APPLIED_BYTES: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x01, 0x2c];
    const SKIPPED_BYTES: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05];
    const RETIRED_BYTES: &[u8] = &[0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(&encode_interest(&interest)[..], INTEREST_BYTES, "interest encoding changed");
    assert_eq!(&encode_push(0x0102, 7, &push)[..], PUSH_BYTES, "push encoding changed");
    assert_eq!(&encode_answers(&answers)[..], ANSWERS_BYTES, "answers encoding changed");
    let applied = PushAnswer::Applied { applied: 7, unwanted: vec![1, 300] };
    assert_eq!(&encode_answer(&applied)[..], APPLIED_BYTES, "push answer encoding changed");
    assert_eq!(&encode_answer(&PushAnswer::Skipped { applied: 5 })[..], SKIPPED_BYTES);
    assert_eq!(&encode_answer(&PushAnswer::Retired)[..], RETIRED_BYTES);
}

#[test]
fn frames_round_trip_and_every_truncation_is_refused() {
    let (interest, push) = fixtures();
    let sized: usize = push
        .items
        .iter()
        .map(|(k, _, i)| match i {
            Item::Batch(b) => item_len(k, b.bytes(), b.len()),
            Item::Path(path) => item_len(k, path_len(path), 0),
            Item::Snapshot(s) => item_len(k, s.bytes(), 0),
            _ => item_len(k, 0, 0),
        })
        .sum();
    assert!(encode_push(1, 1, &push).len() <= PUSH_HEADER + sized);
    let Item::Batch(zero) = push.items[1].2.clone() else { unreachable!() };
    let zero = Push { items: vec![(push.items[1].0.clone(), 3, Item::Batch(zero.traced(Some((0, 8)))))] };
    assert_eq!(decode_push(&mut encode_push(1, 1, &zero).slice(1..)), None, "a trace id is never 0");
    let alone = Push { items: vec![push.items[2].clone()] };
    assert_eq!(encode_push(1, 1, &alone).len(), PUSH_HEADER + item_len(&alone.items[0].0, 3, 2));
    let (a, b) = (encode_interest(&interest).slice(1..), encode_push(u64::MAX, 9, &push).slice(1..));
    assert_eq!(decode_interest(&mut a.clone()), Some(interest));
    // The last want's start, just before the 13 bytes of drops.
    let mut unknown = a.to_vec();
    let at = unknown.len() - 14;
    assert_eq!(unknown[at], 2, "the First want's start");
    unknown[at] = 3;
    assert_eq!(decode_interest(&mut Bytes::from(unknown)), None, "an unknown start is refused");
    assert_eq!(decode_push(&mut b.clone()), Some((u64::MAX, 9, push)));
    for cut in 0..a.len() {
        assert_eq!(decode_interest(&mut a.slice(..cut)), None);
    }
    for cut in 0..b.len() {
        assert_eq!(decode_push(&mut b.slice(..cut)), None);
    }
    let path: Vec<Arc<str>> = vec!["origin".into(), "relay-1".into()];
    let mut answers: Vec<Answer> = STATUS.iter().map(|s| Answer::from(*s)).collect();
    answers.extend([Answer { status: Status::Ok, path: path.clone() }, Answer { status: Status::Ok, path }]);
    let c = encode_answers(&answers);
    assert_eq!(c.len(), 2 + 1 + (1 + 8 + 9) + 3 * answers.len(), "a shared path is sent once");
    assert_eq!(decode_answers(&mut c.clone()).as_ref(), Some(&answers));
    // The child holds a cut between answers against the number it asked for.
    for cut in 0..c.len() {
        assert!(decode_answers(&mut c.slice(..cut)).is_none_or(|short| short.len() < answers.len()));
    }
    let deep = Answer { status: Status::Ok, path: vec!["n".into(); MAX_PATH + 1] };
    assert_eq!(decode_answers(&mut encode_answers(&[deep])), None);
    assert_eq!(decode_answers(&mut encode_answers(&[Status::Loop.into()])), None);
    let pushes = [
        PushAnswer::Applied { applied: 3, unwanted: vec![0, 700] },
        PushAnswer::Applied { applied: u64::MAX, unwanted: Vec::new() },
        PushAnswer::Skipped { applied: 0 },
        PushAnswer::Retired,
    ];
    for answer in pushes {
        let bytes = encode_answer(&answer);
        assert_eq!(decode_answer(&bytes), Some(answer));
        for cut in 0..9 {
            assert_eq!(decode_answer(&bytes[..cut]), None, "a cut answer is refused");
        }
        assert_eq!(decode_answer(&[&bytes[..], &[0]].concat()), None, "an odd byte after the answer is refused");
    }
    let applied = encode_answer(&PushAnswer::Applied { applied: 1, unwanted: vec![2] });
    for status in [1, 2] {
        assert_eq!(decode_answer(&[&[status], &applied[1..]].concat()), None, "unwanted only after Applied");
    }
    assert_eq!(decode_answer(&[&[3], &applied[1..9]].concat()), None, "status 3 is refused");
}

#[test]
fn a_decoded_record_holds_only_its_own_batch() {
    let key = StreamKey::new("md", "btc");
    let batch = SequencedBatch::new(1, 1, vec![Bytes::from(vec![7; 100]); 3]).unwrap();
    let push = encode_push(1, 1, &Push { items: vec![(key, 3, Item::Batch(batch))] });
    // The transport hands a push over as a slice of its 64 KiB read buffer.
    let mut read = BytesMut::with_capacity(64 << 10);
    read.extend_from_slice(&push[1..]);
    read.put_bytes(0, 32 << 10);
    let mut body = read.freeze().slice(..push.len() - 1);
    let Some((_, _, Push { mut items })) = decode_push(&mut body) else { panic!("a push that does not decode") };
    drop(body);
    let Some((_, _, Item::Batch(batch))) = items.pop() else { panic!("not a batch") };
    let record = batch.records()[2].clone();
    drop(batch);
    let held = BytesMut::from(record).capacity();
    assert!(held <= 3 * 104, "a kept record of 100 bytes pins {held}");
}
