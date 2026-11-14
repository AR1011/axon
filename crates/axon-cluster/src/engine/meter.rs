//! What crossed the links to each region, on each lane, by protocol, for the node's life: spec/lanes.md § 8.1.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};

use axon::Lane;
use axon_net::frame::HEADER_LEN;
use axon_net::{Frame, Sender, SubmitError};
use parking_lot::{Mutex, RwLock};
use tokio::time::Instant;

use crate::membership::Membership;
use crate::wire::CONTROL_PROTOCOL;

/// One protocol's frames on one link and their bytes: the header and the payload before compression. Sent then
/// received.
#[derive(Default)]
pub(crate) struct Meter([AtomicU64; 4]);

impl Meter {
    fn bytes(frame: &Frame) -> u64 {
        (HEADER_LEN + frame.decoded_len().unwrap_or(0)) as u64
    }

    fn sent(&self, bytes: u64) {
        self.0[0].fetch_add(1, Relaxed);
        self.0[1].fetch_add(bytes, Relaxed);
    }

    pub fn received(&self, frame: &Frame) {
        self.0[2].fetch_add(1, Relaxed);
        self.0[3].fetch_add(Self::bytes(frame), Relaxed);
    }

    /// Puts `frame` in `sender`'s outbox now, and counts it if it went in.
    pub fn try_submit(&self, sender: &Sender, frame: Frame) -> Result<(), SubmitError> {
        let bytes = Self::bytes(&frame);
        sender.try_submit(frame).inspect(|()| self.sent(bytes))
    }

    /// Waits for room in `sender`'s outbox until `deadline`, and counts `frame` if it went in.
    pub async fn submit(&self, sender: &Sender, frame: Frame, deadline: Option<Instant>) -> Result<(), SubmitError> {
        let bytes = Self::bytes(&frame);
        sender.submit(frame, deadline).await.inspect(|()| self.sent(bytes))
    }

    fn read(&self) -> [u64; 4] {
        self.0.each_ref().map(|n| n.load(Relaxed))
    }
}

/// One link's meters: a meter per protocol it carried, its session's bytes, and the peer's region once the view names
/// the peer. A link accepted before the view lists its peer is counted from then on, all of it. The region is looked
/// for when the link opens, on each protocol's first frame, on each read and at the close; a link whose peer the view
/// named at none of those is filed under `""`.
pub(crate) struct LinkMeters {
    id: u64,
    node: Arc<str>,
    lane: Lane,
    region: OnceLock<Arc<str>>,
    sender: Sender,
    protocols: RwLock<BTreeMap<u64, Arc<Meter>>>,
    book: Arc<Meters>,
    membership: Arc<Membership>,
}

impl LinkMeters {
    /// The meter of `protocol`'s frames on this link; 0 for frames whose call has gone or whose endpoint is not here. A
    /// protocol's first frame also looks for the peer's region, so that a link the view named while it was up keeps it.
    pub fn meter(&self, protocol: u64) -> Arc<Meter> {
        // Read-locked on the path every frame takes, so the callers on one link do not queue on each other.
        if let Some(meter) = self.protocols.read().get(&protocol) {
            return meter.clone();
        }
        self.region();
        self.protocols.write().entry(protocol).or_default().clone()
    }

    /// The peer's `region` label, `""` for none, fixed once the view has named the peer.
    fn region(&self) -> Option<&Arc<str>> {
        if let Some(region) = self.region.get() {
            return Some(region);
        }
        let view = self.membership.view();
        let region = view.get(&self.node)?.record.label("region").unwrap_or("");
        Some(self.region.get_or_init(|| region.into()))
    }

    /// Takes the link's counts into its region's for good, in one step under the lock a reader takes, so that no total
    /// ever drops. Called once nothing counts on it any more: when its link is dropped.
    pub fn close(&self) {
        let mut book = self.book.0.lock();
        book.open.remove(&self.id);
        let region = self.region().cloned().unwrap_or_default();
        book.closed.entry((region, self.lane)).or_default().add(self);
    }
}

/// A region and lane's session bytes each way, and each protocol's frames and bytes sent and received.
#[derive(Clone, Default)]
struct Totals {
    wire: [u64; 2],
    protocols: BTreeMap<u64, [u64; 4]>,
}

impl Totals {
    fn add(&mut self, link: &LinkMeters) {
        let stats = link.sender.stats();
        self.wire[0] += stats.tx_wire_bytes;
        self.wire[1] += stats.rx_wire_bytes;
        for (protocol, meter) in &*link.protocols.read() {
            let sum = self.protocols.entry(*protocol).or_default();
            sum.iter_mut().zip(meter.read()).for_each(|(sum, n)| *sum += n);
        }
    }
}

/// The links that are open, by number, and what the closed ones carried, by the peer's region and lane.
#[derive(Default)]
struct Book {
    next: u64,
    open: BTreeMap<u64, Arc<LinkMeters>>,
    closed: BTreeMap<(Arc<str>, Lane), Totals>,
}

/// Every link this node has had.
#[derive(Default)]
pub(crate) struct Meters(Mutex<Book>);

impl Meters {
    /// Meters for a link on `sender`'s session.
    pub fn open(self: &Arc<Self>, sender: &Sender, membership: &Arc<Membership>) -> Arc<LinkMeters> {
        let (mut book, peer) = (self.0.lock(), sender.peer());
        book.next += 1;
        let link = Arc::new(LinkMeters {
            id: book.next,
            node: peer.identity.node_id.as_str().into(),
            lane: peer.lane,
            region: OnceLock::new(),
            sender: sender.clone(),
            protocols: RwLock::default(),
            book: self.clone(),
            membership: membership.clone(),
        });
        link.region();
        book.open.insert(link.id, link.clone());
        link
    }

    /// Two rows a protocol, sent and received, and two a region and lane with the session bytes, in a stable order.
    pub fn read(&self, here: &str) -> Vec<LinkMeter> {
        let totals = {
            let book = self.0.lock();
            let mut totals = book.closed.clone();
            for link in book.open.values() {
                if let Some(region) = link.region() {
                    totals.entry((region.clone(), link.lane)).or_default().add(link);
                }
            }
            totals
        };
        let mut rows = Vec::new();
        for ((region, lane), totals) in totals {
            let row = |sent: bool, protocol, [frames, frame_bytes, wire_bytes]: [u64; 3]| {
                let (src, dst) = if sent { (here, &*region) } else { (&*region, here) };
                LinkMeter {
                    src_region: src.into(),
                    dst_region: dst.into(),
                    lane,
                    direction: if sent { "sent" } else { "received" },
                    protocol,
                    frames,
                    frame_bytes,
                    wire_bytes,
                }
            };
            let [tx, rx] = totals.wire;
            rows.extend([row(true, None, [0, 0, tx]), row(false, None, [0, 0, rx])]);
            for (protocol, [sent, sent_bytes, received, received_bytes]) in totals.protocols {
                let label = label(protocol);
                rows.push(row(true, Some(label.clone()), [sent, sent_bytes, 0]));
                rows.push(row(false, Some(label), [received, received_bytes, 0]));
            }
        }
        rows
    }
}

/// `axon.control` for the system endpoints, else the id as 16 hex digits. 0 counts frames whose call had gone.
fn label(protocol: u64) -> String {
    match protocol {
        CONTROL_PROTOCOL => "axon.control".into(),
        _ => format!("{protocol:016x}"),
    }
}

/// What crossed the links between two regions on one lane, as this node counted it, since it started. Never reset:
/// a session that closes leaves its bytes in.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct LinkMeter {
    /// Where the bytes went from and to: this node's `region` label and the peer's, `""` for none.
    pub src_region: String,
    pub dst_region: String,
    pub lane: Lane,
    /// `sent` or `received`, by this node: what tells the two apart when both regions are the same.
    pub direction: &'static str,
    /// The protocol's label, `axon.control` for the system endpoints and else its id in 16 hex digits; `None` on the
    /// row that holds `wire_bytes`.
    pub protocol: Option<String>,
    pub frames: u64,
    /// The frame headers and the payloads before compression: an upper bound on what crossed when compressed.
    pub frame_bytes: u64,
    /// The sessions' bytes after compression, above TLS, without the handshake.
    pub wire_bytes: u64,
}
