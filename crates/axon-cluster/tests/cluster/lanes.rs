//! Lanes on real sockets: which lane a typed protocol, a raw endpoint, a reply in parts and an upload's body take.
use std::collections::BTreeMap;

use axon::Lane;

use super::parts::{Ask, How, Pieces, joined, pattern, read_all, settled, two};
use super::*;

/// `CounterApi` at schema 2 on `Bulk`, served by the same actors.
struct BulkCounterApi;

impl WireProtocol for BulkCounterApi {
    type Message = Op;
    type Reply = Bytes;
    const ID: u64 = CounterApi::ID;
    const SCHEMA: u16 = 2;
    const LANE: Lane = Lane::Bulk;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        CounterApi::encode(op)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for BulkCounterApi {
    type Mailbox = Msg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply>) -> Result<Msg, Rejection> {
        CounterApi::decode(body, reply)
    }
}

/// `CounterApi` at schema 3 on `Control`, which only axon's own endpoints take.
struct ControlCounterApi;

impl WireProtocol for ControlCounterApi {
    type Message = Op;
    type Reply = Bytes;
    const ID: u64 = CounterApi::ID;
    const SCHEMA: u16 = 3;
    const LANE: Lane = Lane::Control;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        CounterApi::encode(op)
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for ControlCounterApi {
    type Mailbox = Msg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply>) -> Result<Msg, Rejection> {
        CounterApi::decode(body, reply)
    }
}

/// Answers each upload in parts with its body as it comes, on `Bulk`.
struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = ();
    type Reply = Bytes;
    const ID: u64 = 0xEC0;
    const SCHEMA: u16 = 1;
    const LANE: Lane = Lane::Bulk;

    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for EchoApi {
    type Mailbox = Echoing;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(_: Bytes, reply: Option<Reply>) -> Result<Echoing, Rejection> {
        reply.map(Echoing::Ask).ok_or(Rejection::InvalidMessage)
    }
}

enum Echoing {
    Ask(Reply),
    Done,
}

struct Echo;

impl Actor for Echo {
    type Message = Echoing;
    type Error = ();

    fn on_message(&mut self, msg: Echoing, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let Echoing::Ask(reply) = msg else { return Ok(()) };
        let upload = reply.upload().ok_or(())?;
        let sending = reply.send_parts(upload);
        let sent = async move {
            drop(sending.await);
            Echoing::Done
        };
        cx.submit_future(sent).map(drop).map_err(drop)
    }
}

/// What `node` sent `to` on `lane` on the session it dialled: calls, and bytes each way.
fn sent(node: &Node, to: &str, lane: Lane) -> (u64, u64, u64) {
    let peers = node.cluster.stats().engine.peers;
    let row = peers.into_iter().find(|p| p.node == to && p.outgoing && p.lane == lane);
    row.map_or((0, 0, 0), |p| (p.calls, p.session.tx_wire_bytes, p.session.rx_wire_bytes))
}

fn echo(_: &axon_net::Identity, body: Bytes, reply: Option<Reply>) -> Result<(), Rejection> {
    drop(reply.map(|reply| reply.send(body)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_and_kind_refuse_control_and_the_raw_builder_takes_it_only_under_axon() {
    let (a, b) = two(|_| {}).await;
    let actor = a.system.spawn(Counter { total: 0, tag: "a".into() }).start().unwrap();
    let published = a.cluster.publish::<ControlCounterApi>(&actor, "counter").err();
    assert!(matches!(published, Some(ClusterError::Config(_))), "{published:?}");
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    let kind = a.cluster.kind::<ControlCounterApi>("counter").register(build).await;
    assert!(matches!(kind, Err(ClusterError::Config(_))), "{kind:?}");
    let also = a.cluster.kind::<CounterApi>("counter").also::<ControlCounterApi>().register(build).await;
    assert!(matches!(also, Err(ClusterError::Config(_))), "{also:?}");

    let refused = a.cluster.endpoint("app.echo", 0xE0, 1).lane(Lane::Control).serve(echo);
    assert!(matches!(refused, Err(ClusterError::Config(_))), "{refused:?}");
    a.cluster.endpoint("axon.test.echo", 0xE0, 1).lane(Lane::Control).serve(echo).unwrap();
    a.cluster.endpoint("app.echo", 0xE1, 1).lane(Lane::Bulk).serve(echo).unwrap();
    a.cluster.serve("app.live", 0xE2, 1, echo).unwrap();
    for (name, protocol, lane) in
        [("axon.test.echo", 0xE0, Lane::Control), ("app.echo", 0xE1, Lane::Bulk), ("app.live", 0xE2, Lane::Live)]
    {
        let calls = |b: &Node| Lane::ALL.map(|lane| sent(b, "a", lane).0);
        let before = calls(&b);
        assert_eq!(b.cluster.request("a", name, protocol, 1, Bytes::from_static(b"hi")).await.unwrap(), "hi");
        let after = calls(&b);
        for other in Lane::ALL {
            // The lookup is a call on Control, before each first call to a name.
            let grew = after[other as usize] > before[other as usize];
            assert_eq!(grew, other == lane || other == Lane::Control, "{name}: {other:?} {before:?} {after:?}");
        }
    }
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_also_serving_a_bulk_schema_answers_each_schema_on_its_own_lane() {
    let (a, b) = two(|_| {}).await;
    let build = |_: &SpawnContext, mut args: Bytes| Ok(Counter { total: args.get_u64(), tag: String::new() });
    a.cluster.kind::<CounterApi>("counter").also::<BulkCounterApi>().register(build).await.unwrap();
    eventually("a hosts the kind", || async { b.cluster.view().members.iter().any(|m| m.record.has_kind("counter")) })
        .await;
    let live = b.cluster.keyed::<CounterApi>("counter", "k", seed(5), &Placement::default());
    let bulk = b.cluster.keyed::<BulkCounterApi>("counter", "k", seed(5), &Placement::default());
    assert_eq!(number(live.request(Op::Get).await.unwrap()), 5);
    bulk.tell(Op::Add(1)).await.unwrap();
    assert_eq!(number(bulk.request(Op::Get).await.unwrap()), 6, "a tell and a request on one lane keep their order");
    assert_eq!(number(live.request(Op::Get).await.unwrap()), 6);
    assert_eq!((sent(&b, "a", Lane::Live).0, sent(&b, "a", Lane::Bulk).0), (2, 2), "calls on Live and on Bulk");
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bulk_upload_and_its_reply_parts_stay_on_the_bulk_session() {
    let (a, b) = two(|_| {}).await;
    let jobs = MailboxLimits { jobs: 16, ..MailboxLimits::default() };
    let actor = a.system.spawn(Echo).mailbox(jobs).start().unwrap();
    a.cluster.publish::<EchoApi>(&actor, "echo").unwrap();
    let remote = b.cluster.resolve::<EchoApi>("echo").await.unwrap();
    let len = 4 << 20;
    let ask = Ask { len, part: 256 << 10, cut: u32::MAX, how: How::Whole, pause_ms: 0 };
    let mut parts = remote.upload_parts((), Pieces::new(ask, &Arc::default())).await.unwrap();
    let (parts, end) = read_all(&mut parts).await;
    assert!(end.is_none() && joined(&parts) == pattern(0, len), "{end:?}");
    let ((_, bulk_tx, bulk_rx), len) = (sent(&b, "a", Lane::Bulk), u64::from(len));
    assert!(bulk_tx > len && bulk_rx > len, "Bulk carried {bulk_tx} and {bulk_rx} bytes of {len}");
    for lane in [Lane::Control, Lane::Live] {
        let (_, tx, rx) = sent(&b, "a", lane);
        assert!(tx < 64 << 10 && rx < 64 << 10, "{lane:?} carried {tx} and {rx} bytes");
    }
    settled(&b).await;
}

/// A peer has at most two sessions on one lane at the node it dials: the newest, and the one before, which may still
/// carry replies to its calls. A third closes the first, and one from another boot closes both.
#[tokio::test]
async fn a_third_session_of_one_peer_on_one_lane_closes_the_first() {
    let a = node("a", &MemoryDiscovery::new()).await;
    let (z, rebooted) = (transport("z"), transport("z"));
    let closed = async |session: &mut axon_net::Session, what: &str| {
        let end = tokio::time::timeout(Duration::from_secs(5), session.receiver.recv()).await;
        assert!(end.expect("closed").is_err(), "{what} is still served");
    };
    for lane in Lane::ALL {
        let dial = |cfg| axon_net::connect(a.cluster.local_addr(), lane, None, cfg);
        let mut first = dial(&z).await.unwrap();
        let mut second = dial(&z).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(first.sender.is_active(), "{lane:?}: the second closed the first");
        let rows = a.cluster.stats().engine.peers.into_iter().filter(|p| p.node == "z" && p.lane == lane).count();
        assert_eq!(rows, 1, "{lane:?}: a row per peer, direction and lane");
        let mut third = dial(&z).await.unwrap();
        closed(&mut first, &format!("{lane:?}: the first")).await;
        let new = dial(&rebooted).await.unwrap();
        closed(&mut second, &format!("{lane:?}: the second")).await;
        closed(&mut third, &format!("{lane:?}: the third")).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(new.sender.is_active(), "{lane:?}: the new boot's is served");
    }
    a.system.shutdown().await.unwrap();
}

/// A row of a node's link meters: its region pair, lane, direction and protocol.
type Row = (String, String, Lane, &'static str, Option<String>);

/// Each node's rows: frames, frame bytes and wire bytes.
fn meters(node: &Node) -> BTreeMap<Row, [u64; 3]> {
    let rows = node.cluster.stats().links.into_iter();
    rows.map(|l| {
        ((l.src_region, l.dst_region, l.lane, l.direction, l.protocol), [l.frames, l.frame_bytes, l.wire_bytes])
    })
    .collect()
}

/// A link's meters keep what its closed sessions moved: the `Live` session closes idle and the next call dials it
/// again, and no row of either node drops, while the frames count the calls on both sessions.
#[tokio::test(flavor = "multi_thread")]
async fn a_links_meters_take_in_the_sessions_it_closed() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    // Closed after 300 ms unused rather than the shipped 5 min, so the test waits less.
    let b = node_with("b", discovery.clone(), |cfg| cfg.idle_link_timeout = Duration::from_millis(300)).await;
    sees(&b, 2).await;
    a.cluster.endpoint("echo", 7, 1).serve(echo).unwrap();
    let mut seen = [BTreeMap::new(), BTreeMap::new()];
    let mut never_drop = |what: &str| {
        for (node, seen) in [&a, &b].into_iter().zip(&mut seen) {
            let now = meters(node);
            for (row, before) in seen.iter() {
                let after = now.get(row).unwrap_or_else(|| panic!("{what}: {row:?} left"));
                assert!(after.iter().zip(before).all(|(after, before)| after >= before), "{what}: {row:?} dropped");
            }
            *seen = now;
        }
    };
    let calls = async |n| {
        for _ in 0..n {
            b.cluster.request("a", "echo", 7, 1, Bytes::from(vec![1; 100])).await.unwrap();
        }
    };
    calls(5).await;
    never_drop("after the first session's calls");
    eventually("the idle links are closed", async || {
        never_drop("while they close");
        b.cluster.stats().engine.peers.is_empty() && a.cluster.stats().engine.peers.is_empty()
    })
    .await;
    never_drop("once closed");
    calls(5).await;
    never_drop("after the second session's calls");
    assert_eq!(b.cluster.stats().engine.connects, 4, "Control and Live, twice");

    let echoes = Some("0000000000000007".to_owned());
    let row = |direction| (String::new(), String::new(), Lane::Live, direction, echoes.clone());
    let wire = |direction| (String::new(), String::new(), Lane::Live, direction, None);
    let (at_a, at_b) = (meters(&a), meters(&b));
    assert_eq!(at_b[&row("sent")][0], 10, "b's requests");
    assert_eq!(at_b[&row("received")], [10, 10 * (32 + 100), 0], "b's replies");
    assert_eq!(at_a[&row("received")][0], 10, "a's requests");
    assert_eq!(at_a[&row("sent")], [10, 10 * (32 + 100), 0], "a's replies");
    for (node, meters) in [("a", &at_a), ("b", &at_b)] {
        let (sent, received) = (meters[&wire("sent")][2], meters[&wire("received")][2]);
        assert!(sent > 10 * 132 && received > 10 * 132, "{node}'s Live sessions moved {sent} and {received} bytes");
    }
    settled(&b).await;
}

/// A tell to an endpoint that has gone, and the verdict that says so, count under its protocol at the node that
/// withdrew it, not under 0. The sender keeps nothing of a tell it sent, so the verdict counts under 0 there.
#[tokio::test(flavor = "multi_thread")]
async fn a_tell_to_an_endpoint_that_has_gone_counts_under_its_protocol() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&a, 2).await;
    sees(&b, 2).await;
    let actor = a.system.spawn(Counter { total: 0, tag: "a".into() }).start().unwrap();
    let published = a.cluster.publish::<CounterApi>(&actor, "counter").unwrap();
    let remote = b.cluster.resolve::<CounterApi>("counter").await.unwrap();
    a.cluster.unpublish(&published);
    remote.tell(Op::Add(1)).await.unwrap();
    // Counts by direction and protocol, over every lane.
    let counts = |node: &Node| {
        let mut counts = BTreeMap::new();
        for ((.., direction, protocol), [n, ..]) in meters(node) {
            if let Some(protocol) = protocol {
                *counts.entry((direction, protocol)).or_insert(0) += n;
            }
        }
        counts
    };
    let (counter, zero) = ("00000000000000c0".to_owned(), "0000000000000000".to_owned());
    let verdict = ("received", zero.clone());
    eventually("b hears that the endpoint has gone", async || counts(&b).contains_key(&verdict)).await;
    let (at_a, at_b) = (counts(&a), counts(&b));
    let at =
        |counts: &BTreeMap<_, u64>, direction, protocol: &String| counts.get(&(direction, protocol.clone())).copied();
    assert_eq!((at(&at_a, "received", &counter), at(&at_a, "sent", &counter)), (Some(1), Some(1)), "{at_a:?}");
    assert!(at_a.keys().all(|(_, protocol)| *protocol != zero), "{at_a:?}");
    assert_eq!((at(&at_b, "sent", &counter), at(&at_b, "received", &zero)), (Some(1), Some(1)), "{at_b:?}");
    settled(&b).await;
}
