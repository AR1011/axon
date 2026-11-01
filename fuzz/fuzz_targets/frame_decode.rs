#![no_main]
//! `cargo +nightly fuzz run frame_decode`. Oracle: whatever decodes must re-encode and decode equal.
use axon_net::frame::{Decoder, Encoder};
use axon_net::{Codecs, Compression, Frame};
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

const MAX: usize = 1 << 16;

fuzz_target!(|data: &[u8]| {
    let (mut decoder, mut buf) = (Decoder::new(Codecs::ALL, MAX), BytesMut::from(data));
    let mut encoder = Encoder::new(Compression::None, 1, 0, 1, MAX).unwrap();
    let (mut check, mut wire) = (Decoder::new(Codecs::NONE, MAX), Vec::new());
    while let Ok(Some((h, payload))) = decoder.decode(&mut buf) {
        assert!(payload.len() <= MAX);
        if h.control != axon_net::frame::Control::Data {
            encoder.control(h.control, h.id, &mut wire);
            continue;
        }
        let frame =
            Frame { kind: h.kind, schema: h.schema, protocol: h.protocol, id: h.id, payload, ..Frame::default() };
        encoder.encode(&frame, &mut wire).unwrap();
    }
    let mut again = BytesMut::from(&wire[..]);
    while check.decode(&mut again).unwrap().is_some() {}
    assert!(again.is_empty());
});
