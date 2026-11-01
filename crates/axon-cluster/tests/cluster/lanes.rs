//! Lanes on real sockets: which lane a typed protocol, a raw endpoint, a reply in parts and an upload's body take.
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
