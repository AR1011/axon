#![no_main]
//! `cargo +nightly fuzz run stream_wire`. Oracle: whatever decodes must encode back to something that decodes equal.
//! The interest and push encoders lead with the kind byte that the node strips before decoding.
use axon_stream::wire::*;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((which, rest)) = data.split_first() else { return };
    let mut body = Bytes::copy_from_slice(rest);
    match which % 4 {
        0 => {
            if let Some(interest) = decode_interest(&mut body) {
                assert_eq!(decode_interest(&mut encode_interest(&interest).slice(1..)), Some(interest));
            }
        }
        1 => {
            if let Some(answers) = decode_answers(&mut body) {
                assert_eq!(decode_answers(&mut encode_answers(&answers)), Some(answers));
            }
        }
        2 => {
            if let Some((channel, seq, push)) = decode_push(&mut body) {
                let mut again = encode_push(channel, seq, &push).slice(1..);
                assert_eq!(decode_push(&mut again), Some((channel, seq, push)));
                assert!(again.is_empty());
            }
        }
        _ => {
            if let Some(answer) = decode_answer(rest) {
                assert_eq!(decode_answer(&encode_answer(&answer)), Some(answer));
            }
        }
    }
});
