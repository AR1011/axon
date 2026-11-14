//! The view a node keeps across its restarts, [`ClusterConfig::view_store`](crate::ClusterConfig::view_store): a node
//! that restarts cut from its discovery starts from it, frozen, rather than with no peer at all.

use std::sync::Arc;
use std::time::Duration;

use axon::files::{FileOp, Files, crc32};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::discovery::{NodeRecord, check_nodes};
use crate::membership::{MemberChange, Membership, MembershipEvent, View};
use crate::seed::{get_record, put_record};
use crate::wire::take;

/// The file's name in the store.
const FILE: &str = "view";
const MAGIC: &[u8; 4] = b"AXVW";
const VERSION: u16 = 1;

/// `"AXVW" | version u16 | revision u64 | count u32 | record* | crc32`, each record as the seed protocol encodes it.
fn encode(view: &View) -> Bytes {
    let mut out = BytesMut::new();
    out.put_slice(MAGIC);
    out.put_u16(VERSION);
    out.put_u64(view.revision);
    out.put_u32(view.members.len() as u32);
    for member in &view.members {
        put_record(&mut out, &member.record);
    }
    let crc = crc32(&out);
    out.put_u32(crc);
    out.freeze()
}

/// The records, or `None` for a file torn, corrupt or of another version. The revision is for whoever reads the file:
/// the provider's may have moved anywhere since it was written.
fn decode(file: Vec<u8>) -> Option<Vec<NodeRecord>> {
    let mut body = Bytes::from(file);
    let crc = body.split_off(body.len().checked_sub(4)?);
    if crc32(&body).to_be_bytes() != crc[..] || take(&mut body, 4)? != MAGIC[..] {
        return None;
    }
    if take(&mut body, 2)?.get_u16() != VERSION {
        return None;
    }
    let _revision = take(&mut body, 8)?;
    let count = take(&mut body, 4)?.get_u32();
    let records = (0..count).map(|_| get_record(&mut body)).collect::<Option<Vec<_>>>()?;
    body.is_empty().then_some(records)
}

/// The records of the view `files` holds, but `own`'s: they are its old boots. `None` when there is no such file or
/// it does not read.
pub(crate) async fn load(files: &dyn Files, own: &str) -> Option<Vec<NodeRecord>> {
    let mut nodes = decode(files.read(FILE).await.ok()?)?;
    nodes.retain(|node| node.id != own);
    check_nodes(&mut nodes).ok()?;
    Some(nodes)
}

/// Writes each live view `membership` moves to, at most once per `every`, until it closes. A write that fails is
/// tried again at the next change.
pub(crate) async fn keep(files: Arc<dyn Files>, membership: Arc<Membership>, every: Duration) {
    let (mut events, mut written) = (membership.subscribe(), None);
    while let Some(event) = events.next().await {
        // Reachability is not in the file. No change at all is a view gone live again, after a cached one too: written.
        let reach = |c: &MemberChange| matches!(c, MemberChange::Reachable(_) | MemberChange::Unreachable(_));
        if let MembershipEvent::Changes { changes, .. } = &event
            && !changes.is_empty()
            && changes.iter().all(reach)
        {
            continue;
        }
        let view = membership.view();
        // A change that came while the last was written or waited is in this view already.
        if view.stale.is_some() || written == Some(view.sequence) {
            continue;
        }
        if files.apply(vec![FileOp::Replace(FILE.into(), encode(&view))]).await.is_ok() {
            written = Some(view.sequence);
        }
        tokio::time::sleep(every).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::Member;

    #[test]
    fn a_view_reads_back_and_a_flipped_byte_or_a_torn_tail_does_not() {
        let record = |id: &str| NodeRecord {
            id: id.into(),
            incarnation: 7,
            address: ([10, 0, 0, 1], 7000).into(),
            metadata: [("axon.kinds".to_owned(), "counter/1".to_owned())].into(),
            version: 2,
        };
        let members = ["a", "b"].map(|id| Member { record: record(id), reachable: true }).into();
        let file = encode(&View { revision: 9, members, ..View::default() });
        assert_eq!((&file[..6], u64::from_be_bytes(file[6..14].try_into().unwrap())), (&b"AXVW\0\x01"[..], 9));
        assert_eq!(decode(file.to_vec()), Some(vec![record("a"), record("b")]));
        for at in 0..file.len() {
            let mut flipped = file.to_vec();
            flipped[at] ^= 1;
            assert_eq!(decode(flipped), None, "byte {at}");
            assert_eq!(decode(file[..at].to_vec()), None, "{at} bytes");
        }
    }
}
