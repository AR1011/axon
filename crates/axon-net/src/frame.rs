//! Pure wire codec, public so it can be fuzzed and reused.
//!
//! 32-byte big-endian header, then the payload:
//! `len u32 | sequence u32 | kind u16 | schema u16 | codec u8 | control u8 | 0 u16 | protocol u64 | id u64`.
//! A compressed payload is `decoded_len u32` followed by one independent block.

use bytes::{Buf, Bytes, BytesMut};

use crate::FrameError;

pub const HEADER_LEN: usize = 32;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Frame {
    pub kind: u16,
    pub schema: u16,
    pub protocol: u64,
    pub id: u64,
    /// Received from a read that filled half the session's read buffer or more, a slice of that buffer (64 KiB or
    /// more), which it keeps alive; otherwise its own bytes. A holder that keeps a small payload long copies it.
    pub payload: Bytes,
    /// Compress this frame even when the session's codec is none, with the best codec the
    /// peer accepts. Not carried on the wire: a received frame always has it unset.
    pub compress: bool,
    /// The payload is already `decoded_len u32 | block` in this codec, as [`pack`] makes it,
    /// and is written as is: one compression shared by every session it is sent on.
    pub packed: Compression,
}

impl Frame {
    /// The payload's length once decoded; `None` for a packed payload without its length.
    pub fn decoded_len(&self) -> Option<usize> {
        match self.packed {
            Compression::None => Some(self.payload.len()),
            _ => self.payload.first_chunk::<4>().map(|len| u32::from_be_bytes(*len) as usize),
        }
    }

    /// Whether a side whose limit is `max_frame` will take this frame.
    pub fn fits(&self, max_frame: usize) -> bool {
        let prefix = if self.packed == Compression::None { 0 } else { 4 };
        self.payload.len() <= max_frame + prefix && self.decoded_len().is_some_and(|len| len <= max_frame)
    }
}

/// Compresses `payload` once, for frames that set [`Frame::packed`] to zstd.
pub fn pack(payload: &[u8], zstd_level: i32) -> std::io::Result<Bytes> {
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&zstd::bulk::compress(payload, zstd_level)?);
    Ok(out.into())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    #[default]
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

/// Set of codecs a side will decode. `None` is always a member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Codecs(u8);

impl Codecs {
    pub const ALL: Self = Self(0b111);
    pub const NONE: Self = Self(0b001);

    pub fn with(self, codec: Compression) -> Self {
        Self(self.0 | 1 << codec as u8)
    }

    pub fn contains(self, codec: Compression) -> bool {
        self.0 & (1 << codec as u8) != 0
    }

    pub(crate) fn bits(self) -> u8 {
        self.0
    }

    pub(crate) fn from_bits(bits: u8) -> Self {
        Self(bits & Self::ALL.0 | Self::NONE.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Control {
    Data = 0,
    Ping = 1,
    Pong = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub encoded_len: u32,
    pub sequence: u32,
    pub kind: u16,
    pub schema: u16,
    pub codec: Compression,
    pub control: Control,
    pub protocol: u64,
    pub id: u64,
}

impl Header {
    fn put(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.encoded_len.to_be_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.kind.to_be_bytes());
        out.extend_from_slice(&self.schema.to_be_bytes());
        out.extend_from_slice(&[self.codec as u8, self.control as u8, 0, 0]);
        out.extend_from_slice(&self.protocol.to_be_bytes());
        out.extend_from_slice(&self.id.to_be_bytes());
    }

    fn parse(mut b: &[u8]) -> Result<Self, FrameError> {
        let (encoded_len, sequence, kind, schema) = (b.get_u32(), b.get_u32(), b.get_u16(), b.get_u16());
        let codec = match b.get_u8() {
            0 => Compression::None,
            1 => Compression::Lz4,
            2 => Compression::Zstd,
            _ => return Err(FrameError::Codec),
        };
        let control = match b.get_u8() {
            0 => Control::Data,
            1 => Control::Ping,
            2 => Control::Pong,
            _ => return Err(FrameError::Reserved),
        };
        if b.get_u16() != 0 {
            return Err(FrameError::Reserved);
        }
        Ok(Self { encoded_len, sequence, kind, schema, codec, control, protocol: b.get_u64(), id: b.get_u64() })
    }
}

pub struct Encoder {
    codec: Compression,
    on_request: Compression,
    accepted: Codecs,
    zstd_level: i32,
    threshold: usize,
    min_savings: usize,
    max_frame: usize,
    sequence: u32,
    zstd: Option<zstd::bulk::Compressor<'static>>,
    scratch: Vec<u8>,
}

impl Encoder {
    pub fn new(
        codec: Compression,
        zstd_level: i32,
        threshold: usize,
        min_savings: usize,
        max_frame: usize,
    ) -> std::io::Result<Self> {
        let zstd = (codec == Compression::Zstd).then(|| zstd::bulk::Compressor::new(zstd_level)).transpose()?;
        let on_request = codec;
        Ok(Self {
            codec,
            on_request,
            accepted: Codecs::ALL,
            zstd_level,
            threshold,
            min_savings,
            max_frame,
            sequence: 0,
            zstd,
            scratch: Vec::new(),
        })
    }

    /// The codec for frames that ask for compression on a session whose codec is none.
    pub fn on_request(mut self, accepted: Codecs) -> Self {
        self.accepted = accepted;
        if self.codec == Compression::None {
            let best = [Compression::Zstd, Compression::Lz4].into_iter().find(|c| accepted.contains(*c));
            self.on_request = best.unwrap_or_default();
        }
        self
    }

    fn next_sequence(&mut self) -> u32 {
        let s = self.sequence;
        self.sequence = s.wrapping_add(1);
        s
    }

    /// Compresses into `scratch`; `None` when compression is off, fails, or does not pay.
    fn compress(&mut self, payload: &[u8], requested: bool) -> Option<(Compression, usize)> {
        let codec = if requested { self.on_request } else { self.codec };
        if codec == Compression::None || payload.len() < self.threshold {
            return None;
        }
        if codec == Compression::Zstd && self.zstd.is_none() {
            self.zstd = Some(zstd::bulk::Compressor::new(self.zstd_level).ok()?);
        }
        let bound = match codec {
            Compression::Zstd => zstd::zstd_safe::compress_bound(payload.len()),
            _ => lz4_flex::block::get_maximum_output_size(payload.len()),
        };
        if self.scratch.len() < bound {
            self.scratch.resize(bound, 0);
        }
        let n = match (&mut self.zstd, codec) {
            (Some(z), Compression::Zstd) => z.compress_to_buffer(payload, &mut self.scratch[..bound]).ok()?,
            _ => lz4_flex::block::compress_into(payload, &mut self.scratch[..bound]).ok()?,
        };
        (n + 4 + self.min_savings <= payload.len()).then_some((codec, n))
    }

    /// Appends one frame; `out` is untouched on error. Returns whether it was compressed.
    pub fn encode(&mut self, frame: &Frame, out: &mut Vec<u8>) -> Result<bool, FrameError> {
        let payload = &frame.payload[..];
        if !frame.fits(self.max_frame) {
            return Err(FrameError::Oversized);
        }
        let packed = frame.packed != Compression::None;
        if packed && !self.accepted.contains(frame.packed) {
            return Err(FrameError::Codec);
        }
        let compressed = if packed { None } else { self.compress(payload, frame.compress) };
        let (codec, encoded_len) = compressed.map_or((frame.packed, payload.len()), |(codec, n)| (codec, n + 4));
        Header {
            encoded_len: encoded_len as u32,
            sequence: self.next_sequence(),
            kind: frame.kind,
            schema: frame.schema,
            codec,
            control: Control::Data,
            protocol: frame.protocol,
            id: frame.id,
        }
        .put(out);
        match compressed {
            Some((_, n)) => {
                out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                out.extend_from_slice(&self.scratch[..n]);
            }
            None => out.extend_from_slice(payload),
        }
        Ok(packed || compressed.is_some())
    }

    pub fn control(&mut self, control: Control, id: u64, out: &mut Vec<u8>) {
        let sequence = self.next_sequence();
        Header { encoded_len: 0, sequence, kind: 0, schema: 0, codec: Compression::None, control, protocol: 0, id }
            .put(out);
    }
}

pub struct Decoder {
    accept: Codecs,
    max_frame: usize,
    sequence: u32,
    zstd: Option<zstd::bulk::Decompressor<'static>>,
}

impl Decoder {
    pub fn new(accept: Codecs, max_frame: usize) -> Self {
        Self { accept, max_frame, sequence: 0, zstd: None }
    }

    /// Takes one whole frame off the front of `buf`, or `None` if more bytes are needed.
    /// Everything is validated against local limits before any allocation or decompression.
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<(Header, Bytes)>, FrameError> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let header = Header::parse(&buf[..HEADER_LEN])?;
        let len = header.encoded_len as usize;
        let compressed = header.codec != Compression::None;
        if header.sequence != self.sequence {
            return Err(FrameError::Sequence);
        }
        if !self.accept.contains(header.codec) {
            return Err(FrameError::Codec);
        }
        if len > self.max_frame + 4 * compressed as usize {
            return Err(FrameError::Oversized);
        }
        if header.control != Control::Data && len != 0 {
            return Err(FrameError::Reserved);
        }
        if buf.len() < HEADER_LEN + len {
            buf.reserve(HEADER_LEN + len - buf.len());
            return Ok(None);
        }
        buf.advance(HEADER_LEN);
        let body = buf.split_to(len).freeze();
        self.sequence = self.sequence.wrapping_add(1);
        let payload = if compressed { self.decompress(header.codec, &body)? } else { body };
        Ok(Some((header, payload)))
    }

    fn decompress(&mut self, codec: Compression, mut body: &[u8]) -> Result<Bytes, FrameError> {
        if body.len() < 4 {
            return Err(FrameError::Corrupt);
        }
        let decoded_len = body.get_u32() as usize;
        if decoded_len > self.max_frame {
            return Err(FrameError::Oversized);
        }
        let mut out = vec![0; decoded_len];
        let n = match codec {
            Compression::Zstd => {
                let zstd = match &mut self.zstd {
                    Some(zstd) => zstd,
                    slot => slot.insert(zstd::bulk::Decompressor::new().map_err(|_| FrameError::Corrupt)?),
                };
                zstd.decompress_to_buffer(body, &mut out[..]).ok()
            }
            _ => lz4_flex::block::decompress_into(body, &mut out).ok(),
        };
        if n != Some(decoded_len) {
            return Err(FrameError::Corrupt);
        }
        Ok(out.into())
    }
}
