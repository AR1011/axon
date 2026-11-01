//! Frame kinds and the remote-call contract.
//!
//! A call is one frame; the frame header carries everything but the body:
//! `kind` = how much the caller wants back, `protocol` = destination endpoint id,
//! `schema` = the contract's schema, `id` = correlation id. The answer is one `REPLY` or `END` frame, or parts (below).
//! A tell has none, but for one to an endpoint that is gone: an `END` that names the endpoint, as dynamic ids are never
//! reused.
//! The body starts with the caller's trace context if `kind` has `TRACED`, then with its deadline if `DEADLINE`, then
//! with its window if `PARTS`.
//!
//! A request with `PARTS` is answered in parts: `PART` frames, each carrying the caller's call id, and then `END`,
//! `ADMITTED` once the handler's stream ended, `NO_REPLY` where it was cut, a rejection where it never started. Never
//! `REPLY`. The caller grants the window in its request and more with `CREDIT` frames, `u32` bytes each, where 0 stops
//! the reply. A part costs its length and 64 bytes, and the handler sends one while its credit is above zero: a
//! part past it, a `CREDIT` without its 4 bytes, or a `PARTS` request without its window closes the session.
//!
//! A request with `BODY` is followed by its body in parts, the other way: `PART | BODY` frames under the caller's call
//! id, then `END | BODY`, one byte, `ADMITTED` once the caller's stream ended or `NO_REPLY` where it was cut. The caller
//! starts with `WINDOW` of credit and the handler grants more with `CREDIT | BODY`, 0 stopping the body. A body part
//! past its credit, a second `BODY` request under an id in use, an `END | BODY` without its byte or `BODY` on another
//! kind closes the session; a body frame for a body the handler no longer reads is dropped. Its answer is a request's.

use std::collections::BTreeMap;
use std::time::Duration;

use axon::{Lane, TraceContext};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::engine::Reply;
use crate::{CallError, Rejection};

/// Fire and forget.
pub(crate) const TELL: u16 = 1;
/// Answered by `END` carrying the admission verdict.
pub(crate) const SEND: u16 = 2;
/// Answered by `REPLY`, or by `END` if rejected or the handler dropped the reply.
pub(crate) const REQUEST: u16 = 3;
pub(crate) const REPLY: u16 = 4;
pub(crate) const END: u16 = 5;
/// One part of a reply in parts, to the caller.
pub(crate) const PART: u16 = 6;
/// More credit for a reply in parts, `u32` bytes, to the handler: 0 stops it.
pub(crate) const CREDIT: u16 = 7;
/// Set on a call whose body starts with the caller's trace context.
pub(crate) const TRACED: u16 = 0x8000;
/// Set on a call whose body carries the time its caller has left, `u64` microseconds, measured as the frame is made.
pub(crate) const DEADLINE: u16 = 0x4000;
/// Set on a request answered in parts, whose body carries the caller's window, `u32` bytes, after its deadline.
pub(crate) const PARTS: u16 = 0x2000;
/// Set on a request whose body follows in parts, and on the frames of that body.
pub(crate) const BODY: u16 = 0x1000;

pub(crate) const ADMITTED: u8 = 0;
pub(crate) const NO_REPLY: u8 = 1;

pub(crate) const DIRECTORY: u64 = 1;
pub(crate) const SPAWN: u64 = 2;
pub(crate) const LOAD: u64 = 3;
/// Whether the answering node lists the caller as a member.
pub(crate) const LISTED: u64 = 4;
/// What the answering node runs for a key, answered once that is not what the caller saw, or after a hold.
pub(crate) const WATCH: u64 = 5;
/// A spawn answered once the start has a place, before its acquire: a hand-off from a node that holds the lease.
pub(crate) const HANDOFF: u64 = 6;
/// Whether the answering node would take a key moved to it: asked before a move.
pub(crate) const OWNS: u64 = 7;
pub(crate) const FIRST_DYNAMIC: u64 = 16;
pub(crate) const SYSTEM_SCHEMA: u16 = 1;
pub(crate) const CONTROL_PROTOCOL: u64 = 0x4158_4f4e_4354_524c;

/// A remote contract, shared by both sides. `ID` and `SCHEMA` are checked when a reference is
/// made, `SCHEMA` again on every call. `LANE` is the connection the protocol's calls take to a peer, the serving
/// node's choice: calls on one lane keep their order, calls on two may pass each other. `Control` is axon's own.
pub trait WireProtocol: 'static {
    type Message;
    type Reply;
    const ID: u64;
    const SCHEMA: u16;
    const LANE: Lane = Lane::Live;

    fn encode(message: Self::Message) -> Result<Bytes, CallError>;
    fn decode_reply(body: Bytes) -> Result<Self::Reply, CallError>;
}

/// Existence only: `get_or_spawn::<Unaddressed>` starts or finds a keyed actor of any kind without naming its
/// protocol, for a caller that only needs it to run. The reference it returns carries the node and epoch and
/// cannot send anything. ID 0 is reserved for it.
pub struct Unaddressed;

impl WireProtocol for Unaddressed {
    type Message = std::convert::Infallible;
    type Reply = std::convert::Infallible;
    const ID: u64 = 0;
    const SCHEMA: u16 = 0;

    fn encode(message: Self::Message) -> Result<Bytes, CallError> {
        match message {}
    }

    fn decode_reply(_: Bytes) -> Result<Self::Reply, CallError> {
        Err(CallError::Rejected(Rejection::SchemaMismatch))
    }
}

/// The serving side: turns a body straight into the actor's mailbox type.
pub trait ActorProtocol: WireProtocol {
    type Mailbox: Send + 'static;

    /// Called by [`Reply::send`]. An error fails that send, and the caller gets `NoReply`.
    fn encode_reply(reply: Self::Reply) -> Result<Bytes, CallError>;

    /// `reply` is present for requests. Dropping it answers the caller with `NoReply`.
    fn decode(body: Bytes, reply: Option<Reply<Self::Reply>>) -> Result<Self::Mailbox, Rejection>;
}

/// The call's kind and body, with the trace context, the deadline and the window in front when there are any.
pub(crate) fn stamp(
    kind: u16,
    trace: Option<TraceContext>,
    left: Option<Duration>,
    window: Option<u32>,
    body: Bytes,
) -> (u16, Bytes) {
    if trace.is_none() && left.is_none() && window.is_none() {
        return (kind, body);
    }
    let mut out = BytesMut::with_capacity(TraceContext::WIRE_LEN + 8 + 4 + body.len());
    let mut kind = kind;
    if let Some(trace) = trace {
        kind |= TRACED;
        out.put_slice(&trace.to_bytes());
    }
    if let Some(left) = left {
        kind |= DEADLINE;
        out.put_u64(left.as_micros() as u64);
    }
    if let Some(window) = window {
        kind |= PARTS;
        out.put_u32(window);
    }
    out.put_slice(&body);
    (kind, out.freeze())
}

pub(crate) fn take(body: &mut Bytes, n: usize) -> Option<Bytes> {
    (body.remaining() >= n).then(|| body.split_to(n))
}

pub(crate) fn get_bool(body: &mut Bytes) -> Option<bool> {
    let byte = take(body, 1)?[0];
    (byte <= 1).then_some(byte == 1)
}

pub(crate) fn put_str(out: &mut BytesMut, s: &str) {
    out.put_u16(s.len() as u16);
    out.put_slice(s.as_bytes());
}

pub(crate) fn get_str(body: &mut Bytes) -> Option<String> {
    let len = (body.remaining() >= 2).then(|| body.get_u16() as usize).filter(|n| *n <= body.remaining())?;
    String::from_utf8(body.split_to(len).into()).ok()
}

pub(crate) fn put_map(out: &mut BytesMut, map: &BTreeMap<String, String>) {
    out.put_u16(map.len() as u16);
    for (k, v) in map {
        put_str(out, k);
        put_str(out, v);
    }
}

pub(crate) fn get_map(body: &mut Bytes) -> Option<BTreeMap<String, String>> {
    let count = (body.remaining() >= 2).then(|| body.get_u16())?;
    (0..count).map(|_| Some((get_str(body)?, get_str(body)?))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bytes peers see in front of a call's body. When this fails you changed the call frame, which no schema
    /// guards: a node that does not know a bit of `kind` closes the session, so a new bit is a breaking change.
    #[test]
    fn call_frame_bytes_are_pinned() {
        let trace = TraceContext::from_bytes([0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9]);
        let body = Bytes::from_static(b"hi");
        let (kind, both) = stamp(REQUEST, trace, Some(Duration::from_micros(1500)), None, body.clone());
        let mut pinned = vec![0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9];
        pinned.extend([0, 0, 0, 0, 0, 0, 0x05, 0xdc, b'h', b'i']);
        assert_eq!((kind, &both[..]), (0xc003, &pinned[..]));
        let (kind, left) = stamp(REQUEST, None, Some(Duration::from_millis(2)), None, body.clone());
        assert_eq!((kind, &left[..]), (0x4003, &[0, 0, 0, 0, 0, 0, 0x07, 0xd0, b'h', b'i'][..]));
        assert_eq!(stamp(SEND, None, None, None, body.clone()), (SEND, body.clone()));
        let (kind, window) = stamp(REQUEST, None, None, Some(4 << 20), body.clone());
        assert_eq!((kind, &window[..]), (0x2003, &[0, 0x40, 0, 0, b'h', b'i'][..]));
        let (kind, all) = stamp(REQUEST, trace, Some(Duration::from_micros(1500)), Some(4 << 20), body);
        pinned.splice(24..24, [0, 0x40, 0, 0]);
        assert_eq!((kind, &all[..]), (0xe003, &pinned[..]));
        assert_eq!((PART, CREDIT), (6, 7));
        let (kind, upload) = stamp(REQUEST | BODY, None, None, Some(4 << 20), Bytes::from_static(b"hi"));
        assert_eq!((kind, &upload[..]), (0x3003, &[0, 0x40, 0, 0, b'h', b'i'][..]));
        assert_eq!([PART, END, CREDIT].map(|kind| kind | BODY), [0x1006, 0x1005, 0x1007]);
    }
}
