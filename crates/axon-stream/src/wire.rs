//! The frames stream nodes exchange. Decoding is bounds-checked: bodies come from peers.

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::core::{Answer, Interest, Item, MAX_PATH, Push, PushAnswer, Status, Want};
use crate::hub::{EndReason, ResetReason, StartPosition};
use crate::recovery::{Cursor, Payload, SequencedBatch, Snapshot, StreamKey};

pub const INTEREST: u8 = 1;
pub const PUSH: u8 = 2;

/// The first byte of an interest's reply: the answers follow, or the interest was refused, or it was built on a
/// revision the parent does not hold.
pub(crate) const ANSWERS: u8 = 0;
pub(crate) const REFUSED: u8 = 1;
pub(crate) const OUT_OF_SYNC: u8 = 2;

const RESETS: [ResetReason; 6] = [
    ResetReason::GenerationChanged,
    ResetReason::SlowConsumer,
    ResetReason::HistoryUnavailable,
    ResetReason::Gap,
    ResetReason::InvalidRecovery,
    ResetReason::GlobalCapacity,
];
const ENDS: [EndReason; 7] = [
    EndReason::Unsubscribed,
    EndReason::SessionReplaced,
    EndReason::SessionExpired,
    EndReason::Revoked,
    EndReason::SlowConsumer,
    EndReason::StreamClosed,
    EndReason::UpstreamLost,
];
/// `Loop` is a child's own verdict and is refused from a peer.
const STATUS: [Status; 7] = [
    Status::Ok,
    Status::Forbidden,
    Status::UnknownStream,
    Status::HistoryUnavailable,
    Status::GenerationMismatch,
    Status::Unavailable,
    Status::LimitExceeded,
];

fn put_key(out: &mut BytesMut, key: &StreamKey) {
    put_str(out, key.namespace());
    put_str(out, key.name());
}

fn put_str(out: &mut BytesMut, text: &str) {
    out.put_u16(text.len() as u16);
    out.put_slice(text.as_bytes());
}

fn put_cursor(out: &mut BytesMut, cursor: Cursor) {
    out.put_u64(cursor.generation);
    out.put_u64(cursor.sequence);
}

fn need(body: &Bytes, n: usize) -> Option<()> {
    (body.remaining() >= n).then_some(())
}

fn get_str(body: &mut Bytes) -> Option<Arc<str>> {
    need(body, 2)?;
    let len = body.get_u16() as usize;
    need(body, len)?;
    let text = std::str::from_utf8(&body[..len]).ok().map(Arc::from);
    body.advance(len);
    text
}

fn get_key(body: &mut Bytes) -> Option<StreamKey> {
    Some(StreamKey::new(get_str(body)?, get_str(body)?))
}

fn get_cursor(body: &mut Bytes) -> Option<Cursor> {
    need(body, 16)?;
    Some(Cursor { generation: body.get_u64(), sequence: body.get_u64() })
}

/// Bytes of an interest before its bounds, wants and drops.
pub const INTEREST_HEADER: usize = 26;

/// Bytes of a want, drop or bound of an interest besides its key's names.
pub const WANT_HEADER: usize = 25;

/// An upper bound on one encoded want, drop or bound of an interest.
pub fn want_len(key: &StreamKey) -> usize {
    WANT_HEADER + key.namespace().len() + key.name().len()
}

pub fn encode_interest(interest: &Interest) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(INTEREST);
    out.put_u64(interest.revision);
    out.put_u64(interest.base);
    let bounds = interest.whole.as_ref().map(|(from, to)| [from, to]);
    let flag = |i: usize| bounds.is_some_and(|b| b[i].is_some()) as u8;
    out.put_u8(bounds.is_some() as u8 | flag(0) << 1 | flag(1) << 2);
    for key in bounds.into_iter().flatten().flatten() {
        put_key(&mut out, key);
    }
    out.put_u32(interest.wants.len() as u32);
    for want in &interest.wants {
        put_key(&mut out, &want.key);
        out.put_u32(want.epoch);
        let snapshot = if want.snapshot { 0x80 } else { 0 };
        match want.start {
            // `SnapshotThenLive` is never asked of a parent: `snapshot` is.
            StartPosition::Live | StartPosition::SnapshotThenLive => out.put_u8(snapshot),
            StartPosition::After(after) => {
                out.put_u8(snapshot | 1);
                put_cursor(&mut out, after);
            }
            StartPosition::First => out.put_u8(snapshot | 2),
        }
    }
    out.put_u32(interest.drops.len() as u32);
    interest.drops.iter().for_each(|key| put_key(&mut out, key));
    out.freeze()
}

pub fn decode_interest(body: &mut Bytes) -> Option<Interest> {
    need(body, 17)?;
    let (revision, base, flags) = (body.get_u64(), body.get_u64(), body.get_u8());
    if flags > 7 || (flags & 1 == 0 && flags != 0) {
        return None;
    }
    let mut bound = |bit: u8| if flags & bit != 0 { get_key(body).map(Some) } else { Some(None) };
    let whole = if flags & 1 != 0 { Some((bound(2)?, bound(4)?)) } else { None };
    need(body, 4)?;
    let count = body.get_u32() as usize;
    let mut wants = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        let key = get_key(body)?;
        need(body, 5)?;
        let (epoch, start) = (body.get_u32(), body.get_u8());
        let snapshot = start & 0x80 != 0;
        let start = match start & 0x7f {
            0 => StartPosition::Live,
            1 => StartPosition::After(get_cursor(body)?),
            2 => StartPosition::First,
            _ => return None,
        };
        wants.push(Want { key, epoch, start, snapshot });
    }
    need(body, 4)?;
    let count = body.get_u32() as usize;
    let mut drops = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        drops.push(get_key(body)?);
    }
    Some(Interest { revision, base, whole, wants, drops })
}

/// Each distinct path once, then a status and a path index per answer: streams from one parent share a path.
pub fn encode_answers(answers: &[Answer]) -> Bytes {
    let mut paths: Vec<&[Arc<str>]> = Vec::new();
    let mut tail = BytesMut::with_capacity(answers.len() * 3);
    for answer in answers {
        let index = paths.iter().position(|p| *p == &answer.path[..]).unwrap_or_else(|| {
            paths.push(&answer.path);
            paths.len() - 1
        });
        tail.put_u8(answer.status as u8);
        tail.put_u16(index as u16);
    }
    let mut out = BytesMut::new();
    out.put_u16(paths.len() as u16);
    paths.into_iter().for_each(|path| put_path(&mut out, path));
    out.put_slice(&tail);
    out.freeze()
}

/// The encoded length of a path.
pub fn path_len(path: &[Arc<str>]) -> usize {
    1 + path.iter().map(|node| 2 + node.len()).sum::<usize>()
}

fn put_path(out: &mut BytesMut, path: &[Arc<str>]) {
    out.put_u8(path.len() as u8);
    path.iter().for_each(|node| put_str(out, node));
}

fn get_path(body: &mut Bytes) -> Option<Vec<Arc<str>>> {
    need(body, 1)?;
    let hops = body.get_u8() as usize;
    if hops > MAX_PATH {
        return None;
    }
    (0..hops).map(|_| get_str(body)).collect()
}

pub fn decode_answers(body: &mut Bytes) -> Option<Vec<Answer>> {
    need(body, 2)?;
    let mut paths = Vec::new();
    for _ in 0..body.get_u16() {
        paths.push(get_path(body)?);
    }
    if !body.len().is_multiple_of(3) {
        return None;
    }
    let mut answers = Vec::with_capacity(body.len() / 3);
    while body.has_remaining() {
        let (status, index) = (*STATUS.get(body.get_u8() as usize)?, body.get_u16() as usize);
        answers.push(Answer { status, path: paths.get(index)?.clone() });
    }
    Some(answers)
}

/// Bytes before the first item of a push: its kind, channel, seq and item count.
pub const PUSH_HEADER: usize = 21;

/// Bytes of a push item besides its key's names, its payload and each record's length.
pub const ITEM_FIXED: usize = 45;

/// An upper bound on one encoded push item carrying `bytes` in `records`; exact for a traced batch.
pub fn item_len(key: &StreamKey, bytes: usize, records: usize) -> usize {
    ITEM_FIXED + key.namespace().len() + key.name().len() + bytes + 4 * records
}

/// Push `seq` of `channel`, both numbered from 1.
pub fn encode_push(channel: u64, seq: u64, push: &Push) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(PUSH);
    out.put_u64(channel);
    out.put_u64(seq);
    out.put_u32(push.items.len() as u32);
    for (key, epoch, item) in &push.items {
        put_key(&mut out, key);
        out.put_u32(*epoch);
        match item {
            Item::Ready(start) => {
                out.put_u8(0);
                put_cursor(&mut out, *start);
            }
            Item::Batch(batch) => {
                match batch.trace() {
                    Some((trace, span)) => {
                        out.put_u8(5);
                        out.put_u64(trace);
                        out.put_u64(span);
                    }
                    None => out.put_u8(1),
                }
                out.put_u64(batch.generation());
                out.put_u64(batch.first());
                put_records(&mut out, batch.records());
            }
            Item::Snapshot(snapshot) => {
                out.put_u8(7);
                put_cursor(&mut out, snapshot.through());
                put_records(&mut out, snapshot.records());
            }
            Item::CaughtUp => out.put_u8(2),
            Item::Reset(reason) => out.put_slice(&[3, *reason as u8]),
            Item::End(reason) => out.put_slice(&[4, *reason as u8]),
            Item::Path(path) => {
                out.put_u8(6);
                put_path(&mut out, path);
            }
        }
    }
    out.freeze()
}

fn put_records(out: &mut BytesMut, records: &[Payload]) {
    out.put_u32(records.len() as u32);
    for record in records {
        out.put_u32(record.len() as u32);
        out.put_slice(record);
    }
}

/// Copied out of `body` in one block: `body` can be a slice of a transport read buffer many times its size, and a
/// record kept in a history ring would keep all of it.
fn get_records(body: &mut Bytes) -> Option<Vec<Payload>> {
    need(body, 4)?;
    let n = body.get_u32() as usize;
    let mut end = 0;
    for _ in 0..n {
        let len = u32::from_be_bytes(body.get(end..end + 4)?.try_into().ok()?) as usize;
        end = end.checked_add(4 + len).filter(|end| *end <= body.len())?;
    }
    let mut block = Bytes::copy_from_slice(&body[..end]);
    body.advance(end);
    Some(
        (0..n)
            .map(|_| {
                let len = block.get_u32() as usize;
                block.split_to(len)
            })
            .collect(),
    )
}

/// `(channel, seq, push)`, after the kind byte.
pub fn decode_push(body: &mut Bytes) -> Option<(u64, u64, Push)> {
    need(body, 20)?;
    let (channel, seq, count) = (body.get_u64(), body.get_u64(), body.get_u32() as usize);
    let mut items = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        let key = get_key(body)?;
        need(body, 5)?;
        let (epoch, kind) = (body.get_u32(), body.get_u8());
        let item = match kind {
            0 => Item::Ready(get_cursor(body)?),
            1 | 5 => {
                need(body, if kind == 5 { 32 } else { 16 })?;
                let trace = (kind == 5).then(|| (body.get_u64(), body.get_u64()));
                if trace.is_some_and(|(trace, _)| trace == 0) {
                    return None;
                }
                let (generation, first) = (body.get_u64(), body.get_u64());
                Item::Batch(SequencedBatch::new(generation, first, get_records(body)?).ok()?.traced(trace))
            }
            7 => Item::Snapshot(Snapshot::new(get_cursor(body)?, get_records(body)?)),
            2 => Item::CaughtUp,
            3 | 4 => {
                need(body, 1)?;
                let code = body.get_u8() as usize;
                if kind == 3 { Item::Reset(*RESETS.get(code)?) } else { Item::End(*ENDS.get(code)?) }
            }
            6 => Item::Path(get_path(body)?),
            _ => return None,
        };
        items.push((key, epoch, item));
    }
    Some((channel, seq, Push { items }))
}

/// `status u8 | applied u64 | unwanted u16…`, the unwanted only after `Applied`.
pub fn encode_answer(answer: &PushAnswer) -> Bytes {
    let (status, applied, unwanted) = match answer {
        PushAnswer::Applied { applied, unwanted } => (0, *applied, &unwanted[..]),
        PushAnswer::Skipped { applied } => (1, *applied, &[][..]),
        PushAnswer::Retired => (2, 0, &[][..]),
    };
    let mut out = BytesMut::with_capacity(9 + 2 * unwanted.len());
    out.put_u8(status);
    out.put_u64(applied);
    unwanted.iter().for_each(|index| out.put_u16(*index));
    out.freeze()
}

pub fn decode_answer(body: &[u8]) -> Option<PushAnswer> {
    let (&status, rest) = body.split_first()?;
    let (applied, unwanted) = rest.split_first_chunk::<8>()?;
    let (pairs, odd) = unwanted.as_chunks::<2>();
    if !odd.is_empty() || (status != 0 && !pairs.is_empty()) {
        return None;
    }
    let applied = u64::from_be_bytes(*applied);
    match status {
        0 => Some(PushAnswer::Applied { applied, unwanted: pairs.iter().map(|c| u16::from_be_bytes(*c)).collect() }),
        1 => Some(PushAnswer::Skipped { applied }),
        2 => Some(PushAnswer::Retired),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
