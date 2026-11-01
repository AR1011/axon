//! The quota messages. A node's report to a replica node is answered by that replica's grants; grants a replica pushes
//! are answered by the node's acknowledgement; a replica's pull of another's rows is answered by a page of them. A
//! request body starts with its kind; counts and units are LEB128. A report does not name its sender's incarnation: the
//! session does.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::core::{BUCKETS, Grants, Item, MAX_ITEMS, Pulled, Report, Reported, Used};
use crate::wire::{get_bool, get_str, put_str, take};

/// The endpoint's protocol id, "AXONQUOT", before a node adds its `replicas_per_region` to it.
pub(crate) const PROTOCOL: u64 = 0x4158_4f4e_5155_4f54;
/// The route's schema: `Cluster::serve` refuses any other, so bodies carry none.
pub(crate) const SCHEMA: u16 = 3;

const REPORT: u8 = 1;
const PULL: u8 = 2;
const PUSH: u8 = 3;

const PERMITS: u8 = 1;
const RATE: u8 = 2;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Request {
    Report(Report),
    /// The rows of the keys from this id on that the sender replicates among its candidates, by region.
    Pull {
        start: u64,
        regions: Vec<Vec<Arc<str>>>,
    },
    /// Grants the replica sends unasked, answering the last report it had.
    Push(Grants),
}

fn put_var(out: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        out.put_u8(v as u8 | 0x80);
        v >>= 7;
    }
    out.put_u8(v as u8);
}

fn get_var(body: &mut Bytes) -> Option<u64> {
    let mut v = 0;
    for shift in (0..64).step_by(7) {
        let b = body.has_remaining().then(|| body.get_u8())?;
        if shift == 63 && b > 1 {
            return None;
        }
        v |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Some(v);
        }
    }
    None
}

fn put_us(out: &mut BytesMut, d: Duration) {
    put_var(out, d.as_micros() as u64);
}

fn get_us(body: &mut Bytes) -> Option<Duration> {
    get_var(body).map(Duration::from_micros)
}

fn count(body: &mut Bytes) -> Option<usize> {
    get_var(body).map(|n| n as usize).filter(|n| *n <= MAX_ITEMS)
}

fn put_used(out: &mut BytesMut, used: &Used) {
    match used {
        Used::Permits(held) => {
            out.put_u8(PERMITS);
            put_var(out, *held);
        }
        Used::Rate(drops) => {
            out.put_u8(RATE);
            put_var(out, drops.len() as u64);
            for (left, units) in drops {
                // Rounded up, so that usage counted until then is not counted shorter.
                put_var(out, left.as_nanos().div_ceil(1000) as u64);
                put_var(out, *units);
            }
        }
    }
}

fn get_used(body: &mut Bytes) -> Option<Used> {
    match take(body, 1)?.get_u8() {
        PERMITS => Some(Used::Permits(get_var(body)?)),
        RATE => {
            let n = count(body)?;
            let mut drops = Vec::with_capacity(n.min(16));
            for _ in 0..n {
                drops.push((get_us(body)?, get_var(body)?));
            }
            Some(Used::Rate(drops))
        }
        _ => None,
    }
}

pub(crate) fn encode_report(report: &Report) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(REPORT);
    put_var(&mut out, report.seq);
    out.put_u128(report.replica);
    put_var(&mut out, report.acked);
    put_var(&mut out, report.items.len() as u64);
    for Item { id, max, usage, demand } in &report.items {
        out.put_u64(*id);
        put_var(&mut out, *max);
        match usage {
            Reported::Permits(held) => {
                out.put_u8(PERMITS);
                put_var(&mut out, *held);
            }
            Reported::Rate { width, phase, buckets } => {
                out.put_u8(RATE);
                put_us(&mut out, *width);
                put_us(&mut out, *phase);
                buckets.iter().for_each(|units| put_var(&mut out, *units));
            }
        }
        put_var(&mut out, *demand);
    }
    out.freeze()
}

pub(crate) fn encode_pull(start: u64, regions: &[Vec<Arc<str>>]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(PULL);
    out.put_u64(start);
    put_var(&mut out, regions.len() as u64);
    for nodes in regions {
        put_var(&mut out, nodes.len() as u64);
        nodes.iter().for_each(|node| put_str(&mut out, node));
    }
    out.freeze()
}

/// A request from the incarnation `sender` of its node.
pub(crate) fn decode_request(mut body: Bytes, sender: u128) -> Option<Request> {
    let request = match take(&mut body, 1)?.get_u8() {
        REPORT => {
            let seq = get_var(&mut body)?;
            let (replica, acked) = (take(&mut body, 16)?.get_u128(), get_var(&mut body)?);
            let n = count(&mut body)?;
            let mut items = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let (id, max) = (take(&mut body, 8)?.get_u64(), get_var(&mut body)?);
                let usage = match take(&mut body, 1)?.get_u8() {
                    PERMITS => Reported::Permits(get_var(&mut body)?),
                    RATE => {
                        let (width, phase) = (get_us(&mut body)?, get_us(&mut body)?);
                        let mut buckets = [0; BUCKETS];
                        for units in &mut buckets {
                            *units = get_var(&mut body)?;
                        }
                        (!width.is_zero() && phase < width).then_some(())?;
                        Reported::Rate { width, phase, buckets }
                    }
                    _ => return None,
                };
                items.push(Item { id, max, usage, demand: get_var(&mut body)? });
            }
            Request::Report(Report { incarnation: sender, seq, replica, acked, items })
        }
        PULL => {
            let start = take(&mut body, 8)?.get_u64();
            let mut regions = Vec::new();
            for _ in 0..count(&mut body)? {
                let n = count(&mut body)?;
                regions.push((0..n).map(|_| get_str(&mut body).map(Arc::from)).collect::<Option<_>>()?);
            }
            Request::Pull { start, regions }
        }
        PUSH => Request::Push(decode_grants(std::mem::take(&mut body))?),
        _ => return None,
    };
    body.is_empty().then_some(request)
}

pub(crate) fn encode_push(grants: &Grants) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(PUSH);
    put_grants(&mut out, grants);
    out.freeze()
}

pub(crate) fn encode_grants(grants: &Grants) -> Bytes {
    let mut out = BytesMut::new();
    put_grants(&mut out, grants);
    out.freeze()
}

fn put_grants(out: &mut BytesMut, grants: &Grants) {
    out.put_u128(grants.incarnation);
    put_var(out, grants.answers);
    put_var(out, grants.gseq);
    out.put_u8(grants.young.into());
    put_var(out, grants.entries.len() as u64);
    for (id, grant, used) in &grants.entries {
        out.put_u64(*id);
        put_var(out, *grant);
        put_var(out, *used);
    }
}

/// A node's reply to pushed grants: the replica's incarnation its view shows, and the last grants it took in from it.
pub(crate) fn encode_ack(replica: u128, acked: u64) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u128(replica);
    put_var(&mut out, acked);
    out.freeze()
}

pub(crate) fn decode_ack(mut body: Bytes) -> Option<(u128, u64)> {
    let ack = (take(&mut body, 16)?.get_u128(), get_var(&mut body)?);
    body.is_empty().then_some(ack)
}

pub(crate) fn decode_grants(mut body: Bytes) -> Option<Grants> {
    let incarnation = take(&mut body, 16)?.get_u128();
    let (answers, gseq) = (get_var(&mut body)?, get_var(&mut body)?);
    let young = get_bool(&mut body)?;
    let n = count(&mut body)?;
    let mut entries = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        entries.push((take(&mut body, 8)?.get_u64(), get_var(&mut body)?, get_var(&mut body)?));
    }
    body.is_empty().then_some(Grants { incarnation, answers, gseq, young, entries })
}

/// A page of rows, and the id the next page starts from if there is one.
pub(crate) fn encode_page(rows: &[Pulled], next: Option<u64>) -> Bytes {
    let mut out = BytesMut::new();
    match next {
        Some(id) => {
            out.put_u8(1);
            out.put_u64(id);
        }
        None => out.put_u8(0),
    }
    put_var(&mut out, rows.len() as u64);
    for Pulled { id, max, width, node, incarnation, used, demand, held } in rows {
        out.put_u64(*id);
        put_var(&mut out, *max);
        put_us(&mut out, *width);
        put_str(&mut out, node);
        out.put_u128(*incarnation);
        put_used(&mut out, used);
        put_var(&mut out, *demand);
        put_var(&mut out, *held);
    }
    out.freeze()
}

pub(crate) fn decode_page(mut body: Bytes) -> Option<(Vec<Pulled>, Option<u64>)> {
    let next = match take(&mut body, 1)?.get_u8() {
        0 => None,
        1 => Some(take(&mut body, 8)?.get_u64()),
        _ => return None,
    };
    let n = count(&mut body)?;
    let mut rows = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let (id, max, width) = (take(&mut body, 8)?.get_u64(), get_var(&mut body)?, get_us(&mut body)?);
        let node = get_str(&mut body)?.into();
        let incarnation = take(&mut body, 16)?.get_u128();
        rows.push(Pulled {
            id,
            max,
            width,
            node,
            incarnation,
            used: get_used(&mut body)?,
            demand: get_var(&mut body)?,
            held: get_var(&mut body)?,
        });
    }
    body.is_empty().then_some((rows, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Report {
        let rate = Reported::Rate {
            width: Duration::from_millis(125),
            phase: Duration::from_millis(1),
            buckets: [0, 0, 0, 0, 0, 0, 0, 1, 300],
        };
        let items = vec![
            Item { id: 0x0102_0304_0506_0708, max: 300, usage: Reported::Permits(5), demand: 1 },
            Item { id: 9, max: 10, usage: rate, demand: 0 },
        ];
        Report { incarnation: 7, seq: 3, replica: 5, acked: 2, items }
    }

    fn grants() -> Grants {
        Grants { incarnation: 7, answers: 3, gseq: 4, young: true, entries: vec![(9, 4, 130)] }
    }

    fn rows() -> Vec<Pulled> {
        let row = |used, node: &str| Pulled {
            id: 9,
            max: 10,
            width: Duration::from_millis(125),
            node: node.into(),
            incarnation: 7,
            used,
            demand: 1,
            held: 2,
        };
        vec![row(Used::Rate(vec![(Duration::from_millis(1), 2)]), "n"), row(Used::Permits(3), "")]
    }

    fn regions() -> Vec<Vec<Arc<str>>> {
        vec![vec!["a".into(), "b".into()], vec![]]
    }

    #[test]
    fn messages_round_trip_and_refuse_what_is_cut_or_trailing() {
        let bodies = [
            (encode_report(&report()), "report"),
            (encode_pull(9, &regions()), "pull"),
            (encode_grants(&grants()), "grants"),
            (encode_page(&rows(), Some(10)), "page"),
            (encode_push(&grants()), "push"),
            (encode_ack(5, 300), "ack"),
        ];
        assert_eq!(decode_request(bodies[0].0.clone(), 7), Some(Request::Report(report())));
        assert_eq!(decode_request(bodies[1].0.clone(), 7), Some(Request::Pull { start: 9, regions: regions() }));
        assert_eq!(decode_grants(bodies[2].0.clone()), Some(grants()));
        assert_eq!(decode_page(bodies[3].0.clone()), Some((rows(), Some(10))));
        assert_eq!(decode_request(bodies[4].0.clone(), 7), Some(Request::Push(grants())));
        assert_eq!(decode_ack(bodies[5].0.clone()), Some((5, 300)));
        for (body, what) in &bodies {
            let decodes = |b: Bytes| match *what {
                "report" | "pull" | "push" => decode_request(b, 7).is_some(),
                "grants" => decode_grants(b).is_some(),
                "ack" => decode_ack(b).is_some(),
                _ => decode_page(b).is_some(),
            };
            for n in 0..body.len() {
                assert!(!decodes(body.slice(..n)), "{what} cut at {n}");
            }
            assert!(!decodes([&body[..], &[0]].concat().into()), "{what} with a trailing byte");
        }
        let mut many = BytesMut::new();
        put_var(&mut many, MAX_ITEMS as u64 + 1);
        assert_eq!(get_var(&mut many.clone().freeze()), Some(MAX_ITEMS as u64 + 1));
        assert_eq!(count(&mut many.freeze()), None, "more items than allowed");
        let mut out = BytesMut::new();
        put_var(&mut out, u64::MAX);
        assert_eq!(get_var(&mut out.freeze()), Some(u64::MAX));
        assert_eq!(get_var(&mut Bytes::from_static(&[0xff; 10])), None, "a varint past 64 bits");
    }

    /// The bytes peers see for `SCHEMA` 3. When this fails you changed the protocol: bump `SCHEMA` and re-pin, or
    /// replicas on the old schema refuse what they cannot decode.
    #[test]
    fn wire_bytes_are_pinned_to_the_schema() {
        assert_eq!(SCHEMA, 3, "SCHEMA moved: re-pin the bytes below");
        assert_eq!(&PROTOCOL.to_be_bytes(), b"AXONQUOT");
        const REPORT_BYTES: &[u8] = &[
            1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 2, 2, 1, 2, 3, 4, 5, 6, 7, 8, 172, 2, 1, 5, 1, 0, 0,
            0, 0, 0, 0, 0, 9, 10, 2, 200, 208, 7, 232, 7, 0, 0, 0, 0, 0, 0, 0, 1, 172, 2, 0,
        ];
        const PULL_BYTES: &[u8] = &[2, 0, 0, 0, 0, 0, 0, 0, 9, 2, 2, 0, 1, 97, 0, 1, 98, 0];
        const GRANTS_BYTES: &[u8] =
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 3, 4, 1, 1, 0, 0, 0, 0, 0, 0, 0, 9, 4, 130, 1];
        const PAGE_BYTES: &[u8] = &[
            1, 0, 0, 0, 0, 0, 0, 0, 10, 2, 0, 0, 0, 0, 0, 0, 0, 9, 10, 200, 208, 7, 0, 1, 110, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 7, 2, 1, 232, 7, 2, 1, 2, 0, 0, 0, 0, 0, 0, 0, 9, 10, 200, 208, 7, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 1, 3, 1, 2,
        ];
        assert_eq!(&encode_report(&report())[..], REPORT_BYTES, "report encoding changed");
        assert_eq!(&encode_pull(9, &regions())[..], PULL_BYTES, "pull encoding changed");
        assert_eq!(&encode_grants(&grants())[..], GRANTS_BYTES, "grants encoding changed");
        assert_eq!(&encode_page(&rows(), Some(10))[..], PAGE_BYTES, "page encoding changed");
        assert_eq!(&encode_push(&grants())[..], &[&[3][..], GRANTS_BYTES].concat()[..], "push encoding changed");
        const ACK_BYTES: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 172, 2];
        assert_eq!(&encode_ack(5, 300)[..], ACK_BYTES, "ack encoding changed");
    }
}
