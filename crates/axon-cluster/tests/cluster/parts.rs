//! Replies in parts: `request_parts` on the caller, `Reply::send_parts` on the handler.
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::task::Poll;

use axon::{ActorHandle, Backoff, JobHandle, Stream};
use tokio::time::Instant;

use super::*;

/// How a reply in parts ends once `cut` parts have gone.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum How {
    /// Every part, then the end.
    Whole,
    /// One `Reply::send` of the whole value.
    One,
    /// The stream yields an error.
    Fails,
    /// The stream yields a part that does not encode.
    Unencodable,
    /// The stream yields a part over the caller's frame.
    Oversized,
    /// The stream panics.
    Panics,
    /// The stream waits `pause_ms` before each part after the cut.
    Pauses,
    /// The handler drops the future without polling it.
    Unpolled,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Ask {
    pub len: u32,
    pub part: u32,
    pub cut: u32,
    pub how: How,
    pub pause_ms: u32,
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Fetch(Ask),
    /// Cancels the job of the last fetch.
    Cancel,
    /// The handler fails, so a supervised actor restarts.
    Fail,
}

enum Msg {
    Fetch(Ask, Option<Reply<Bytes>>),
    Sent(Result<(), CallError>),
    Cancel,
    Fail,
}

fn fetch(len: u32, part: u32, how: How) -> Op {
    Op::Fetch(Ask { len, part, cut: 2, how, pause_ms: 3_600_000 })
}

/// Bytes `from..to` of every reply: the same value whatever the parts.
pub(super) fn pattern(from: u32, to: u32) -> Bytes {
    (from..to).map(|i| (i % 251) as u8).collect::<Vec<u8>>().into()
}

const UNENCODABLE: &[u8] = b"unencodable";

struct BlobApi;

impl WireProtocol for BlobApi {
    type Message = Op;
    type Reply = Bytes;
    const ID: u64 = 0xB10B;
    const SCHEMA: u16 = 1;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        let mut out = BytesMut::new();
        match op {
            Op::Fetch(ask) => {
                out.put_u8(0);
                for n in [ask.len, ask.part, ask.cut, ask.pause_ms] {
                    out.put_u32(n);
                }
                out.put_u8(ask.how as u8);
            }
            Op::Cancel => out.put_u8(1),
            Op::Fail => out.put_u8(2),
        }
        Ok(out.freeze())
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for BlobApi {
    type Mailbox = Msg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        if body == UNENCODABLE {
            return Err(CallError::Codec("unencodable".into()));
        }
        Ok(body)
    }

    fn decode(mut body: Bytes, reply: Option<Reply<Bytes>>) -> Result<Msg, Rejection> {
        use How::*;
        let hows = [Whole, One, Fails, Unencodable, Oversized, Panics, Pauses, Unpolled];
        match body.try_get_u8() {
            Ok(0) if body.len() == 17 => {
                let [len, part, cut, pause_ms] = std::array::from_fn(|_| body.get_u32());
                let how = *hows.get(body.get_u8() as usize).ok_or(Rejection::InvalidMessage)?;
                Ok(Msg::Fetch(Ask { len, part, cut, how, pause_ms }, reply))
            }
            Ok(1) => Ok(Msg::Cancel),
            Ok(2) => Ok(Msg::Fail),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

/// The parts of an ask, counting each one it yields in `pulled`.
pub(super) struct Pieces {
    ask: Ask,
    at: u32,
    yielded: u32,
    pulled: Arc<AtomicUsize>,
    pause: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Pieces {
    pub(super) fn new(ask: Ask, pulled: &Arc<AtomicUsize>) -> Self {
        Self { ask, at: 0, yielded: 0, pulled: pulled.clone(), pause: None }
    }
}

impl Stream for Pieces {
    type Item = Result<Bytes, CallError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let ask = self.ask;
        if self.yielded >= ask.cut {
            match ask.how {
                How::Fails => return Poll::Ready(Some(Err(CallError::Codec("cut".into())))),
                How::Unencodable if self.yielded == ask.cut => {
                    self.yielded += 1;
                    return Poll::Ready(Some(Ok(Bytes::from_static(UNENCODABLE))));
                }
                How::Oversized => return Poll::Ready(Some(Ok(Bytes::from(vec![0; (1 << 20) + 1])))),
                How::Panics => panic!("a stream that panics"),
                How::Pauses => {
                    let pause = Duration::from_millis(ask.pause_ms.into());
                    let sleep = self.pause.get_or_insert_with(|| Box::pin(tokio::time::sleep(pause)));
                    std::task::ready!(sleep.as_mut().poll(cx));
                    self.pause = None;
                }
                _ => {}
            }
        }
        if self.at >= ask.len {
            return Poll::Ready(None);
        }
        let end = ask.len.min(self.at + ask.part);
        let part = pattern(self.at, end);
        (self.at, self.yielded) = (end, self.yielded + 1);
        self.pulled.fetch_add(1, SeqCst);
        Poll::Ready(Some(Ok(part)))
    }
}

/// Serves each fetch with `send_parts` in a job, keeping what each job's future returned.
#[derive(Clone, Default)]
struct Blob {
    pulled: Arc<AtomicUsize>,
    sent: Arc<std::sync::Mutex<Vec<Result<(), CallError>>>>,
    last: Option<Arc<JobHandle>>,
}

impl Actor for Blob {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Fetch(ask, Some(reply)) => {
                if ask.how == How::One {
                    return reply.send(pattern(0, ask.len)).map_err(drop);
                }
                let pieces = Pieces::new(ask, &self.pulled);
                let sending = reply.send_parts(pieces);
                if ask.how == How::Unpolled {
                    drop(sending);
                    return Ok(());
                }
                let job = cx.submit_future(async move { Msg::Sent(sending.await) }).map_err(drop)?;
                self.last = Some(Arc::new(job));
            }
            Msg::Fetch(_, None) => {}
            Msg::Sent(sent) => self.sent.lock().unwrap().push(sent),
            Msg::Cancel => drop(self.last.take().map(|job| job.cancel())),
            Msg::Fail => return Err(()),
        }
        Ok(())
    }
}

impl Blob {
    fn sent(&self) -> Vec<String> {
        self.sent.lock().unwrap().iter().map(|r| format!("{r:?}")).collect()
    }

    fn spawn(&self, node: &Node, name: &str) -> (ActorHandle<Msg>, RemoteRef<BlobApi>) {
        let jobs = MailboxLimits { jobs: 128, ..MailboxLimits::default() };
        let actor = node.system.spawn(self.clone()).mailbox(jobs).start().unwrap();
        let local = node.cluster.publish::<BlobApi>(&actor, name).unwrap();
        (actor, local)
    }
}

/// Every part to the end: the parts, and the error that cut them, after which there is nothing more.
pub(super) async fn read_all<P: WireProtocol>(parts: &mut Parts<P>) -> (Vec<P::Reply>, Option<CallError>) {
    let mut have = Vec::new();
    while let Some(part) = parts.next().await {
        match part {
            Ok(part) => have.push(part),
            Err(e) => {
                assert!(parts.next().await.is_none(), "one error ends it");
                return (have, Some(e));
            }
        }
    }
    (have, None)
}

/// The parts of a call, with an error of the first answer as the end.
async fn fetch_all(to: &RemoteRef<BlobApi>, op: Op) -> (Vec<Bytes>, Option<CallError>) {
    match to.request_parts(op).await {
        Ok(mut parts) => read_all(&mut parts).await,
        Err(e) => (Vec::new(), Some(e)),
    }
}

pub(super) fn joined(parts: &[Bytes]) -> Bytes {
    parts.concat().into()
}

/// Nothing is left waiting on the caller: no call slot, no reply on any session.
pub(super) async fn settled(node: &Node) {
    eventually("every call slot is back", || async {
        let engine = node.cluster.stats().engine;
        engine.pending == 0 && engine.peers.iter().all(|p| p.pending == 0)
    })
    .await;
}

pub(super) async fn two(tweak: impl Fn(&mut ClusterConfig) + Clone) -> (Node, Node) {
    let discovery = MemoryDiscovery::new();
    let a = node_with("a", discovery.clone(), tweak.clone()).await;
    let b = node_with("b", discovery, tweak).await;
    sees(&b, 2).await;
    (a, b)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_larger_than_a_frame_arrives_whole_in_parts_on_another_node_and_on_its_own() {
    let (a, b) = two(|_| {}).await;
    let (_actor, local) = Blob::default().spawn(&a, "blob");
    let remote = b.cluster.resolve::<BlobApi>("blob").await.unwrap();
    for (what, to) in [("remote", &remote), ("local", &local)] {
        let (parts, end) = fetch_all(to, fetch(12 << 20, 256 << 10, How::Whole)).await;
        assert!(end.is_none(), "{what}: {end:?}");
        assert_eq!(parts.len(), 48, "{what}");
        assert!(joined(&parts) == pattern(0, 12 << 20), "{what}: the bytes differ");

        let mut empty = to.request_parts(fetch(0, 1, How::Whole)).await.unwrap();
        assert!(empty.next().await.is_none(), "{what}: a reply with no parts");
        let (one, end) = fetch_all(to, fetch(5, 5, How::One)).await;
        assert_eq!((one, end.is_none()), (vec![pattern(0, 5)], true), "{what}: Reply::send is one part");
        assert_eq!(to.request(fetch(5, 5, How::One)).await.unwrap(), pattern(0, 5), "{what}: a plain request");
    }
    let named = b.cluster.named::<BlobApi>("blob");
    let mut parts = named.request_parts(fetch(3 << 20, 1 << 20, How::Whole)).await.unwrap();
    let (parts, end) = read_all(&mut parts).await;
    assert!(end.is_none() && joined(&parts) == pattern(0, 3 << 20), "{end:?}");
    settled(&b).await;
    settled(&a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_does_not_read_holds_its_window_and_the_producer_waits() {
    let (a, b) = two(|_| {}).await;
    let blob = Blob::default();
    let (_actor, local) = blob.spawn(&a, "blob");
    let remote = b.cluster.resolve::<BlobApi>("blob").await.unwrap();
    for (what, to) in [("remote", &remote), ("local", &local)] {
        blob.pulled.store(0, SeqCst);
        let mut parts = to.request_parts(fetch(12 << 20, 1 << 20, How::Whole)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        // A window of 4 MiB, each part 1 MiB and 64 bytes: the fourth takes it below zero.
        assert_eq!(blob.pulled.load(SeqCst), 4, "{what}");
        let (parts, end) = read_all(&mut parts).await;
        assert!(end.is_none() && joined(&parts) == pattern(0, 12 << 20), "{what}: {end:?}");
        assert_eq!(blob.pulled.load(SeqCst), 12, "{what}");
    }
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_cut_by_its_producer_ends_in_no_reply_never_in_a_clean_end() {
    let (a, b) = two(|_| {}).await;
    for what in ["remote", "local"] {
        let blob = Blob::default();
        let (actor, local) = blob.spawn(&a, &format!("cut-{what}"));
        let to = match what {
            "remote" => b.cluster.resolve::<BlobApi>(&format!("cut-{what}")).await.unwrap(),
            _ => local,
        };
        let cut = |how| Op::Fetch(Ask { len: 1 << 20, part: 64 << 10, cut: 2, how, pause_ms: 3_600_000 });
        let mut hows = vec![How::Fails, How::Unencodable, How::Pauses, How::Unpolled, How::Panics];
        if what == "remote" {
            hows.insert(2, How::Oversized); // a local caller takes a part of any size
        }
        for how in hows {
            if how == How::Panics {
                let sent = if what == "remote" { 3 } else { 2 };
                eventually("the futures that returned are kept", || async { blob.sent.lock().unwrap().len() == sent })
                    .await;
            }
            let mut parts = match to.request_parts(cut(how)).await {
                Ok(parts) => parts,
                Err(e) => {
                    assert!(how == How::Unpolled && matches!(e, CallError::NoReply), "{what} {how:?}: {e:?}");
                    continue;
                }
            };
            let first: Vec<_> = [parts.next().await, parts.next().await].into_iter().flatten().collect();
            assert!(matches!(&first[..], [Ok(_), Ok(_)]), "{what} {how:?}: {first:?}");
            if how == How::Pauses {
                to.send(Op::Cancel).await.unwrap(); // JobHandle::cancel drops the future, polled and waiting
            }
            let (rest, end) = read_all(&mut parts).await;
            assert!(rest.is_empty() && matches!(end, Some(CallError::NoReply)), "{what} {how:?}: {rest:?} {end:?}");
        }
        assert_eq!(actor.join().await.reason, StopReason::Panicked, "{what}: a job that panics");
        let sent = blob.sent();
        let expected = if what == "remote" {
            vec!["Err(Codec(\"cut\"))", "Err(Codec(\"unencodable\"))", "Err(NotSent(Oversized))"]
        } else {
            vec!["Err(Codec(\"cut\"))", "Err(Codec(\"unencodable\"))"]
        };
        assert_eq!(sent, expected, "{what}");
    }
    settled(&b).await;
    settled(&a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drain_finishes_a_reply_in_parts_and_a_stop_cuts_it() {
    let (a, b) = two(|_| {}).await;
    let blob = Blob::default();
    let (actor, _) = blob.spawn(&a, "drained");
    let remote = b.cluster.resolve::<BlobApi>("drained").await.unwrap();
    let mut parts = remote.request_parts(fetch(12 << 20, 256 << 10, How::Whole)).await.unwrap();
    actor.lifecycle.drain(Some(Duration::from_secs(10)));
    let (got, end) = read_all(&mut parts).await;
    assert!(end.is_none() && joined(&got) == pattern(0, 12 << 20), "{end:?}");
    assert_eq!(actor.join().await.reason, StopReason::Drained);

    let (actor, _) = blob.spawn(&a, "stopped");
    let remote = b.cluster.resolve::<BlobApi>("stopped").await.unwrap();
    let mut parts = remote.request_parts(fetch(1 << 20, 64 << 10, How::Pauses)).await.unwrap();
    assert!(matches!((parts.next().await, parts.next().await), (Some(Ok(_)), Some(Ok(_)))));
    actor.lifecycle.stop();
    let (rest, end) = read_all(&mut parts).await;
    assert!(rest.is_empty() && matches!(end, Some(CallError::NoReply)), "{rest:?} {end:?}");
    assert_eq!(actor.join().await.reason, StopReason::Stopped);

    // A supervised restart cancels the old generation's jobs: its reply is cut too.
    let made = blob.clone();
    let build = move |_: &SpawnContext, _: Bytes| Ok(made.clone());
    // Restarts soon, so the test does not wait out the default's.
    let backoff = Backoff { initial: Duration::from_millis(10), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..RestartPolicy::default() };
    a.cluster.kind::<BlobApi>("blob").policy(policy).register(build).await.unwrap();
    let hosts = || b.cluster.view().members.iter().any(|m| m.record.has_kind("blob"));
    eventually("b sees a host the kind", || async { hosts() }).await;
    let keyed = b.cluster.keyed::<BlobApi>("blob", "k", Bytes::new(), &Placement::default());
    let mut parts = keyed.request_parts(fetch(1 << 20, 64 << 10, How::Pauses)).await.unwrap();
    assert!(matches!((parts.next().await, parts.next().await), (Some(Ok(_)), Some(Ok(_)))));
    keyed.send(Op::Fail).await.unwrap();
    let (rest, end) = read_all(&mut parts).await;
    assert!(rest.is_empty() && matches!(end, Some(CallError::NoReply)), "{rest:?} {end:?}");
    let mut again = keyed.request_parts(fetch(3 << 20, 1 << 20, How::Whole)).await.unwrap();
    let (got, end) = read_all(&mut again).await;
    assert!(end.is_none() && joined(&got) == pattern(0, 3 << 20), "the next generation: {end:?}");
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_parts_stops_the_far_stream_and_frees_its_slot() {
    let (a, b) = two(|cfg| cfg.max_inbound_replies = 1).await;
    let blob = Blob::default();
    let (_actor, local) = blob.spawn(&a, "blob");
    let remote = b.cluster.resolve::<BlobApi>("blob").await.unwrap();
    for (what, to) in [("remote", &remote), ("local", &local)] {
        let before = blob.sent.lock().unwrap().len();
        let mut parts = to.request_parts(fetch(16 << 20, 256 << 10, How::Whole)).await.unwrap();
        assert!(parts.next().await.is_some_and(|p| p.is_ok()), "{what}");
        drop(parts);
        eventually("the producer ends", || async { blob.sent.lock().unwrap().len() == before + 1 }).await;
        assert_eq!(blob.sent()[before], "Ok(())", "{what}: a caller gone is not an error");
        assert!(blob.pulled.load(SeqCst) < 64, "{what}: the stream went on");
        // A system call of the cluster's own may hold the one slot for a moment.
        eventually("the next is admitted", || async {
            let (got, end) = fetch_all(to, fetch(3 << 20, 1 << 20, How::Whole)).await;
            end.is_none() && joined(&got) == pattern(0, 3 << 20)
        })
        .await;
        blob.pulled.store(0, SeqCst);
    }
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_late_past_request_timeout_or_a_reply_past_its_deadline_times_out() {
    // Short, so a stalled part times out in moments.
    let (a, b) = two(|cfg| cfg.request_timeout = Duration::from_millis(300)).await;
    let blob = Blob::default();
    let (_actor, local) = blob.spawn(&a, "slow");
    let remote = b.cluster.resolve::<BlobApi>("slow").await.unwrap();
    let slow = |pause_ms| Op::Fetch(Ask { len: 1 << 20, part: 64 << 10, cut: 2, how: How::Pauses, pause_ms });
    for (what, to) in [("remote", &remote), ("local", &local)] {
        let before = blob.sent.lock().unwrap().len();
        let mut parts = to.request_parts(slow(1000)).await.unwrap();
        assert!(matches!((parts.next().await, parts.next().await), (Some(Ok(_)), Some(Ok(_)))), "{what}");
        let waited = Instant::now();
        let (rest, end) = read_all(&mut parts).await;
        let waited = waited.elapsed();
        assert!(rest.is_empty() && matches!(end, Some(CallError::Timeout)), "{what}: {rest:?} {end:?}");
        assert!(waited >= Duration::from_millis(290) && waited < Duration::from_millis(900), "{what}: {waited:?}");

        let asked = Instant::now();
        let mut parts = to.request_parts_within(slow(1000), Duration::from_millis(500)).await.unwrap();
        assert!(matches!((parts.next().await, parts.next().await), (Some(Ok(_)), Some(Ok(_)))), "{what}");
        let (rest, end) = read_all(&mut parts).await;
        let took = asked.elapsed();
        assert!(rest.is_empty() && matches!(end, Some(CallError::Timeout)), "{what}: {rest:?} {end:?}");
        assert!(took >= Duration::from_millis(490) && took < Duration::from_millis(900), "{what}: {took:?}");
        eventually("both producers end", || async { blob.sent.lock().unwrap().len() == before + 2 }).await;
        assert_eq!(blob.sent()[before..], ["Ok(())", "Ok(())"], "{what}: a caller gone is not an error");
    }
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn send_parts_to_a_plain_request_is_refused_as_invalid_for_both() {
    let (a, b) = two(|_| {}).await;
    let blob = Blob::default();
    let (_actor, local) = blob.spawn(&a, "blob");
    let remote = b.cluster.resolve::<BlobApi>("blob").await.unwrap();
    for (what, to) in [("remote", &remote), ("local", &local)] {
        let refused = to.request(fetch(1 << 20, 64 << 10, How::Whole)).await;
        assert!(matches!(refused, Err(CallError::Rejected(Rejection::InvalidMessage))), "{what}: {refused:?}");
        assert_eq!(refused.unwrap_err().retry(), Retry::Never);
    }
    eventually("both futures return", || async { blob.sent.lock().unwrap().len() == 2 }).await;
    assert_eq!(blob.sent(), ["Err(Rejected(InvalidMessage))"; 2]);
    assert_eq!(blob.pulled.load(SeqCst), 0, "the stream was never polled");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_carries_64_replies_in_parts_and_refuses_the_next_full() {
    let discovery = MemoryDiscovery::new();
    let (a, b, c) = (node("a", &discovery).await, node("b", &discovery).await, node("c", &discovery).await);
    sees(&b, 3).await;
    sees(&c, 3).await;
    let blob = Blob::default();
    blob.spawn(&a, "blob");
    let (from_b, from_c) =
        (b.cluster.resolve::<BlobApi>("blob").await.unwrap(), c.cluster.resolve::<BlobApi>("blob").await.unwrap());
    let held = |len| Op::Fetch(Ask { len, part: 1 << 10, cut: 1, how: How::Pauses, pause_ms: 3_600_000 });
    let mut open = Vec::new();
    for _ in 0..64 {
        open.push(from_b.request_parts(held(4 << 10)).await.unwrap());
    }
    let refused = from_b.request_parts(held(4 << 10)).await;
    assert!(matches!(refused, Err(CallError::Rejected(Rejection::Full))), "{refused:?}");
    assert_eq!(refused.unwrap_err().retry(), Retry::Later);
    let (got, end) = fetch_all(&from_c, fetch(3 << 20, 1 << 20, How::Whole)).await;
    assert!(end.is_none() && joined(&got) == pattern(0, 3 << 20), "another session is not affected: {end:?}");

    drop(open.pop());
    let given = Instant::now() + Duration::from_secs(10);
    loop {
        match from_b.request_parts(held(4 << 10)).await {
            Ok(parts) => break open.push(parts),
            Err(e) => assert!(Instant::now() < given, "a slot comes back: {e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(open);
    // The 64 held, c's and the one admitted again.
    eventually("every producer ends", || async { blob.sent.lock().unwrap().len() == 66 }).await;
    assert!(blob.sent().iter().all(|s| s == "Ok(())"));
    settled(&b).await;
}

/// Frames a peer could send that break the protocol close its session, and only its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_short_credit_or_a_parts_request_without_its_window_closes_only_that_session() {
    let (a, b) = two(|_| {}).await;
    let echo = |_: &axon_net::Identity, body: Bytes, reply: Option<axon_cluster::Reply>| {
        reply.expect("a request").send(body).map_err(|_| Rejection::Stopped)
    };
    let endpoint = a.cluster.serve("echo", 0xE0, 1, echo).unwrap();
    // REQUEST with PARTS, then PART, END and CREDIT, from the call frame's pinned bytes.
    let (parts_request, part, end, credit) = (0x2003, 6, 5, 7);
    let frame = |kind, id, payload: &[u8]| axon_net::Frame {
        kind,
        schema: 1,
        protocol: endpoint,
        id,
        payload: Bytes::copy_from_slice(payload),
        ..axon_net::Frame::default()
    };
    let window = (4u32 << 20).to_be_bytes();
    let asking = |id, body: &[u8]| frame(parts_request, id, &[&window[..], body].concat());
    async fn answer(raw: &mut axon_net::Session) -> Result<(u16, u64, Bytes), axon_net::Error> {
        let answer = tokio::time::timeout(Duration::from_secs(5), raw.receiver.recv()).await.expect("an answer");
        answer.map(|a| a.into_frame()).map(|f| (f.kind, f.id, f.payload))
    }
    let admitted = Bytes::from_static(&[0]);

    let mut raw =
        axon_net::connect(a.cluster.local_addr(), axon_net::Lane::Control, None, &transport("z")).await.unwrap();
    raw.sender.submit(asking(1, b"hi"), None).await.unwrap();
    assert_eq!(answer(&mut raw).await.unwrap(), (part, 1, Bytes::from_static(b"hi")), "Reply::send is one part");
    assert_eq!(answer(&mut raw).await.unwrap(), (end, 1, admitted.clone()));
    raw.sender.submit(frame(credit, 99, &window), None).await.unwrap();
    raw.sender.submit(asking(2, b"again"), None).await.unwrap();
    assert_eq!(answer(&mut raw).await.unwrap(), (part, 2, Bytes::from_static(b"again")), "a credit for no reply");
    assert_eq!(answer(&mut raw).await.unwrap(), (end, 2, admitted));
    raw.sender.submit(frame(credit, 2, &[0, 1]), None).await.unwrap();
    assert!(answer(&mut raw).await.is_err(), "a short credit closes the session");

    let mut raw =
        axon_net::connect(a.cluster.local_addr(), axon_net::Lane::Control, None, &transport("z")).await.unwrap();
    raw.sender.submit(frame(parts_request, 1, &[0, 0, 1]), None).await.unwrap();
    assert!(answer(&mut raw).await.is_err(), "a request in parts without its window closes the session");

    let echoed = b.cluster.request("a", "echo", 0xE0, 1, Bytes::from_static(b"still")).await.unwrap();
    assert_eq!(echoed, "still", "b's session to a is another");
}

/// A protocol whose actor notes the number of each message as it takes it, and answers each that waits.
struct InOrderApi;

impl WireProtocol for InOrderApi {
    type Message = u8;
    type Reply = Bytes;
    const ID: u64 = 0x0D3;
    const SCHEMA: u16 = 1;

    fn encode(n: u8) -> Result<Bytes, CallError> {
        Ok(Bytes::from(vec![n]))
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for InOrderApi {
    type Mailbox = Took;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply<Bytes>>) -> Result<Took, Rejection> {
        match body[..] {
            [n] => Ok(Took(n, reply)),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

struct Took(u8, Option<Reply<Bytes>>);

struct InOrder(Arc<std::sync::Mutex<Vec<u8>>>);

impl Actor for InOrder {
    type Message = Took;
    type Error = ();

    fn on_message(&mut self, Took(n, reply): Took, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.lock().unwrap().push(n);
        drop(reply.map(|reply| reply.send(Bytes::new())));
        Ok(())
    }
}

/// A protocol has one lane, so its calls keep the order they were made in, whatever their size: a guard against a
/// change that moves a call to a lane by what it carries.
#[tokio::test(flavor = "multi_thread")]
async fn a_tell_an_upload_and_a_request_on_one_protocol_arrive_in_order() {
    let (a, b) = two(|_| {}).await;
    let took = Arc::new(std::sync::Mutex::new(Vec::new()));
    let actor = a.system.spawn(InOrder(took.clone())).start().unwrap();
    a.cluster.publish::<InOrderApi>(&actor, "in-order").unwrap();
    let remote = b.cluster.resolve::<InOrderApi>("in-order").await.unwrap();
    let pulled = Arc::default();
    for n in (0..60).step_by(3) {
        remote.tell(n).await.unwrap();
        let ask = Ask { len: 2 << 20, part: 256 << 10, cut: u32::MAX, how: How::Whole, pause_ms: 0 };
        let mut upload = std::pin::pin!(remote.upload(n + 1, Pieces::new(ask, &pulled)));
        // Polled once, the upload's head is on the session before the request is made.
        let mut early = None;
        std::future::poll_fn(|cx| {
            if let Poll::Ready(done) = upload.as_mut().poll(cx) {
                early = Some(done);
            }
            Poll::Ready(())
        })
        .await;
        remote.request(n + 2).await.unwrap();
        match early {
            Some(done) => done,
            None => upload.await,
        }
        .unwrap();
    }
    eventually("every call is taken", || async { took.lock().unwrap().len() == 60 }).await;
    assert_eq!(*took.lock().unwrap(), (0..60).collect::<Vec<u8>>());
    settled(&b).await;
}
