use axon_net::frame::{Decoder, Encoder, HEADER_LEN, pack};
use axon_net::{Codecs, Compression, Frame, FrameError};
use bytes::{Bytes, BytesMut};

fn frame(id: u64, payload: Vec<u8>) -> Frame {
    Frame { kind: 7, schema: 2, protocol: 0xfeed, id, payload: Bytes::from(payload), ..Frame::default() }
}

/// An endless xorshift stream from `seed`.
fn xorshift(mut seed: u64) -> impl FnMut() -> u64 {
    move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    }
}

fn encoder(codec: Compression) -> Encoder {
    Encoder::new(codec, 1, 64, 8, 1 << 16).unwrap()
}

#[test]
fn every_codec_round_trips_and_falls_back_when_compression_does_not_pay() {
    let mut x = xorshift(0x2545f4914f6cdd1d);
    let noise: Vec<u8> = std::iter::repeat_with(|| x() as u8).take(4096).collect();
    for codec in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let frames =
            [frame(1, vec![]), frame(2, b"tiny".to_vec()), frame(3, b"abcd".repeat(2000)), frame(4, noise.clone())];
        let (mut enc, mut wire) = (encoder(codec), Vec::new());
        let compressed: Vec<bool> = frames.iter().map(|f| enc.encode(f, &mut wire).unwrap()).collect();
        assert_eq!(compressed, [false, false, codec != Compression::None, false]);
        let (mut dec, mut buf) = (Decoder::new(Codecs::ALL, 1 << 16), BytesMut::new());
        let mut seen = Vec::new();
        // Byte-at-a-time delivery exercises every partial-frame boundary.
        for byte in wire {
            buf.extend_from_slice(&[byte]);
            while let Some((h, payload)) = dec.decode(&mut buf).unwrap() {
                seen.push(Frame {
                    kind: h.kind,
                    schema: h.schema,
                    protocol: h.protocol,
                    id: h.id,
                    payload,
                    ..Frame::default()
                });
            }
        }
        assert_eq!(seen, frames);
    }
}

#[test]
fn a_frame_can_ask_for_compression_on_a_session_without_a_codec() {
    let asking = Frame { compress: true, ..frame(1, b"abcd".repeat(2000)) };
    for (accepted, expected) in [
        (Codecs::ALL, Some(Compression::Zstd)),
        (Codecs::NONE.with(Compression::Lz4), Some(Compression::Lz4)),
        (Codecs::NONE, None),
    ] {
        let (mut enc, mut wire) = (encoder(Compression::None).on_request(accepted), Vec::new());
        assert!(!enc.encode(&frame(0, b"abcd".repeat(2000)), &mut wire).unwrap(), "only on request");
        assert_eq!(enc.encode(&asking, &mut wire).unwrap(), expected.is_some());
        let (mut dec, mut buf) = (Decoder::new(Codecs::ALL, 1 << 16), BytesMut::from(&wire[..]));
        dec.decode(&mut buf).unwrap().unwrap();
        let (header, payload) = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!((header.codec, payload), (expected.unwrap_or_default(), asking.payload.clone()));
    }
    let mut lz4 = encoder(Compression::Lz4).on_request(Codecs::ALL);
    let mut wire = Vec::new();
    lz4.encode(&asking, &mut wire).unwrap();
    assert_eq!(wire[12], Compression::Lz4 as u8, "the session codec wins over the on-request one");
}

#[test]
fn a_packed_frame_is_written_as_is_and_decodes_like_any_other() {
    let payload = b"abcd".repeat(2000);
    let block = pack(&payload, 1).unwrap();
    assert!(block.len() < payload.len() / 4);
    let packed = Frame { payload: block.clone(), packed: Compression::Zstd, ..frame(0, Vec::new()) };

    // Two sessions share the one block; neither encoder compresses anything.
    for _ in 0..2 {
        let (mut enc, mut wire) = (encoder(Compression::None).on_request(Codecs::ALL), Vec::new());
        assert!(enc.encode(&packed, &mut wire).unwrap());
        assert_eq!(&wire[HEADER_LEN..], &block[..]);
        let (mut dec, mut buf) = (Decoder::new(Codecs::ALL, 1 << 16), BytesMut::from(&wire[..]));
        let (header, seen) = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!((header.codec, header.kind, &seen[..]), (Compression::Zstd, 7, &payload[..]));
    }

    let mut out = Vec::new();
    let mut refusing = encoder(Compression::None).on_request(Codecs::NONE);
    assert_eq!(refusing.encode(&packed, &mut out), Err(FrameError::Codec));
    let big = Frame { payload: pack(&vec![0; (1 << 16) + 1], 1).unwrap(), ..packed.clone() };
    assert_eq!(encoder(Compression::None).encode(&big, &mut out), Err(FrameError::Oversized));
    let short = Frame { payload: Bytes::from_static(b"ab"), ..packed };
    assert_eq!(encoder(Compression::None).encode(&short, &mut out), Err(FrameError::Oversized));
    assert!(out.is_empty());
}

#[test]
fn limits_codecs_and_sequence_are_enforced_before_work() {
    let mut out = Vec::new();
    assert_eq!(
        encoder(Compression::None).encode(&frame(0, vec![0; (1 << 16) + 1]), &mut out),
        Err(FrameError::Oversized)
    );
    assert!(out.is_empty());

    let mut wire = Vec::new();
    encoder(Compression::Lz4).encode(&frame(0, b"abcd".repeat(2000)), &mut wire).unwrap();
    let decode = |accept, max, wire: &[u8]| Decoder::new(accept, max).decode(&mut BytesMut::from(wire)).map(drop);
    assert_eq!(decode(Codecs::NONE, 1 << 16, &wire), Err(FrameError::Codec));
    assert_eq!(decode(Codecs::ALL, 4000, &wire), Err(FrameError::Oversized));
    assert_eq!(decode(Codecs::ALL, 1 << 16, &wire), Ok(()));

    let mut lying = wire.clone();
    lying[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&7999u32.to_be_bytes());
    assert_eq!(decode(Codecs::ALL, 1 << 16, &lying), Err(FrameError::Corrupt));

    let mut gap = wire.clone();
    gap[7] = 1;
    assert_eq!(decode(Codecs::ALL, 1 << 16, &gap), Err(FrameError::Sequence));
    let mut huge = wire;
    huge[0] = 0x7f;
    assert_eq!(decode(Codecs::ALL, 1 << 16, &huge), Err(FrameError::Oversized));
}

#[test]
fn mutated_input_never_panics() {
    let mut next = xorshift(0x9e3779b97f4a7c15);
    for codec in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let (mut enc, mut wire) = (encoder(codec), Vec::new());
        for i in 0..4 {
            enc.encode(&frame(i, b"axon ".repeat(100 * (i as usize + 1))), &mut wire).unwrap();
        }
        for _ in 0..20_000 {
            let mut bad = wire.clone();
            for _ in 0..1 + next() % 4 {
                let at = next() as usize % bad.len();
                bad[at] = next() as u8;
            }
            bad.truncate(1 + next() as usize % bad.len());
            let (mut dec, mut buf) = (Decoder::new(Codecs::ALL, 1 << 12), BytesMut::from(&bad[..]));
            while let Ok(Some((_, payload))) = dec.decode(&mut buf) {
                assert!(payload.len() <= 1 << 12);
            }
        }
    }
}
