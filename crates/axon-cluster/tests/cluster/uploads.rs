//! Requests in parts: `upload` and `upload_parts` on the caller, `Reply::upload` on the handler.
use std::sync::atomic::AtomicUsize;

use axon::ActorHandle;
use tokio::time::Instant;

use super::parts::{Ask, How, Pieces, joined, pattern, read_all, settled, two};
use super::*;

/// What the handler does with a request's body.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Take {
    /// Reads it to its end in a job, then answers with its length and how it ended.
    Read,
    /// Keeps it unread until `Release`, then reads it as `Read` does.
    Hold,
    /// Answers at once without taking it.
    Answer,
    /// Drops it, and keeps the reply until `Release`.
    Drop,
    /// Answers in parts with the body as it comes: a two-way stream.
    Echo,
}

enum Up {
    Take(Take),
    Release,
}

struct SinkApi;

impl WireProtocol for SinkApi {
    type Message = Up;
    type Reply = Bytes;
    const ID: u64 = 0x5141;
    const SCHEMA: u16 = 1;

    fn encode(up: Up) -> Result<Bytes, CallError> {
        Ok(match up {
            Up::Take(take) => Bytes::from(vec![0, take as u8]),
            Up::Release => Bytes::from_static(&[1]),
        })
    }

    fn decode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
}

impl ActorProtocol for SinkApi {
    type Mailbox = SinkMsg;

    fn encode_reply(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }

    fn decode(body: Bytes, reply: Option<Reply<Bytes>>) -> Result<SinkMsg, Rejection> {
        use Take::*;
        match (&body[..], reply) {
            ([0, take], Some(reply)) => {
                let take = *[Read, Hold, Answer, Drop, Echo].get(*take as usize).ok_or(Rejection::InvalidMessage)?;
                Ok(SinkMsg::Take(take, reply))
            }
            ([1], _) => Ok(SinkMsg::Release),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

enum SinkMsg {
    Take(Take, Reply<Bytes>),
    Release,
    Ended(Bytes, String),
}

/// What every handler of a test saw: each body it read, and how each ended.
#[derive(Clone, Default)]
struct Seen {
    bodies: Arc<std::sync::Mutex<Vec<Bytes>>>,
    ends: Arc<std::sync::Mutex<Vec<String>>>,
    holding: Arc<AtomicUsize>,
    /// Requests taken to be read at once.
    reading: Arc<AtomicUsize>,
}

impl Seen {
    fn ends(&self) -> Vec<String> {
        self.ends.lock().unwrap().clone()
    }

    async fn ended(&self, n: usize) -> Vec<String> {
        eventually("the handler's reads end", || async { self.ends.lock().unwrap().len() >= n }).await;
        self.ends()
    }

    fn spawn(&self, node: &Node, name: &str) -> (ActorHandle<SinkMsg>, RemoteRef<SinkApi>) {
        let actor = node.system.spawn(Sink::new(self)).mailbox(jobs()).start().unwrap();
        let local = node.cluster.publish::<SinkApi>(&actor, name).unwrap();
        (actor, local)
    }
}

fn jobs() -> MailboxLimits {
    MailboxLimits { jobs: 128, ..MailboxLimits::default() }
}

struct Sink {
    seen: Seen,
    held: Vec<(Option<Upload>, Reply<Bytes>)>,
}

impl Sink {
    fn new(seen: &Seen) -> Self {
        Self { seen: seen.clone(), held: Vec::new() }
    }

    /// Reads `upload` to its end in a job, then answers.
    fn read(upload: Option<Upload>, reply: Reply<Bytes>, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let job = async move {
            let Some(mut upload) = upload else {
                let _ = reply.send(Bytes::from_static(b"no body"));
                return SinkMsg::Ended(Bytes::new(), "no body".into());
            };
            let (mut body, mut end) = (BytesMut::new(), None);
            while let Some(part) = upload.next().await {
                match part {
                    Ok(part) => body.extend_from_slice(&part),
                    Err(e) => {
                        assert!(upload.next().await.is_none(), "one error ends it");
                        end = Some(e);
                    }
                }
            }
            let how = format!("{} {end:?}", body.len());
            let _ = reply.send(Bytes::from(how.clone()));
            SinkMsg::Ended(body.freeze(), how)
        };
        cx.submit_future(job).map(drop).map_err(drop)
    }
}

impl Actor for Sink {
    type Message = SinkMsg;
    type Error = ();

    fn on_message(&mut self, msg: SinkMsg, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            SinkMsg::Take(Take::Read, reply) => {
                self.seen.reading.fetch_add(1, SeqCst);
                Self::read(reply.upload(), reply, cx)?;
            }
            SinkMsg::Take(Take::Hold, reply) => {
                self.held.push((reply.upload(), reply));
                self.seen.holding.fetch_add(1, SeqCst);
            }
            SinkMsg::Take(Take::Answer, reply) => drop(reply.send(Bytes::from_static(b"early"))),
            SinkMsg::Take(Take::Drop, reply) => {
                drop(reply.upload());
                self.held.push((None, reply));
            }
            SinkMsg::Take(Take::Echo, reply) => {
                let upload = reply.upload().expect("an upload");
                let sending = reply.send_parts(upload);
                cx.submit_future(async move { SinkMsg::Ended(Bytes::new(), format!("echo {:?}", sending.await)) })
                    .map_err(drop)?;
            }
            SinkMsg::Release => {
                for (upload, reply) in std::mem::take(&mut self.held) {
                    Self::read(upload, reply, cx)?;
                }
            }
            SinkMsg::Ended(body, how) => {
                self.seen.bodies.lock().unwrap().push(body);
                self.seen.ends.lock().unwrap().push(how);
            }
        }
        Ok(())
    }
}

/// `len` bytes of the pattern in parts of `part`, counting each part pulled.
fn body(len: u32, part: u32, pulled: &Arc<AtomicUsize>) -> Pieces {
    Pieces::new(Ask { len, part, cut: u32::MAX, how: How::Whole, pause_ms: 0 }, pulled)
}

/// 1 MiB in 64 KiB parts, ending as `how` says after two.
fn cut(how: How, pause_ms: u32, pulled: &Arc<AtomicUsize>) -> Pieces {
    Pieces::new(Ask { len: 1 << 20, part: 64 << 10, cut: 2, how, pause_ms }, pulled)
}

fn read() -> Up {
    Up::Take(Take::Read)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upload_larger_than_a_frame_arrives_whole_on_another_node_and_on_its_own() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    let (_actor, local) = seen.spawn(&a, "sink");
    let remote = b.cluster.resolve::<SinkApi>("sink").await.unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    for (i, (what, to)) in [("remote", &remote), ("local", &local)].into_iter().enumerate() {
        let answer = to.upload(read(), body(12 << 20, 256 << 10, &pulled)).await.unwrap();
        assert_eq!(answer, "12582912 None", "{what}");
        // The handler records what it read after it answers: three reads a turn.
        seen.ended(3 * i + 1).await;
        assert!(seen.bodies.lock().unwrap()[3 * i] == pattern(0, 12 << 20), "{what}: the bytes differ");
        assert_eq!(to.upload(read(), body(0, 1, &pulled)).await.unwrap(), "0 None", "{what}: a body with no parts");
        assert_eq!(to.request(read()).await.unwrap(), "no body", "{what}: a plain request has none");
    }
    let named = b.cluster.named::<SinkApi>("sink");
    assert_eq!(named.upload(read(), body(3 << 20, 1 << 20, &pulled)).await.unwrap(), "3145728 None");

    let made = seen.clone();
    a.cluster.kind::<SinkApi>("sink").mailbox(jobs()).register(move |_, _| Ok(Sink::new(&made))).await.unwrap();
    let hosts = || b.cluster.view().members.iter().any(|m| m.record.has_kind("sink"));
    eventually("b sees a host the kind", || async { hosts() }).await;
    let keyed = b.cluster.keyed::<SinkApi>("sink", "k", Bytes::new(), &Placement::default());
    assert_eq!(keyed.upload(read(), body(3 << 20, 1 << 20, &pulled)).await.unwrap(), "3145728 None");
    seen.ended(8).await;
    assert!(seen.bodies.lock().unwrap().iter().all(|b| *b == pattern(0, b.len() as u32)));
    settled(&b).await;
    settled(&a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_does_not_read_holds_the_window_and_the_caller_waits() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    let (_actor, local) = seen.spawn(&a, "sink");
    let remote = b.cluster.resolve::<SinkApi>("sink").await.unwrap();
    for (what, to) in [("remote", remote), ("local", local)] {
        let pulled = Arc::new(AtomicUsize::new(0));
        let (up, body) = (to.clone(), body(12 << 20, 1 << 20, &pulled));
        let call = tokio::spawn(async move { up.upload(Up::Take(Take::Hold), body).await });
        // A window of 4 MiB, each part 1 MiB and 64 bytes: the fourth takes it below zero.
        eventually("the window is spent", || async { pulled.load(SeqCst) >= 4 }).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(pulled.load(SeqCst), 4, "{what}");
        // A send on the same session passes the body that waits.
        to.send(Up::Release).await.unwrap();
        assert_eq!(call.await.unwrap().unwrap(), "12582912 None", "{what}");
        assert_eq!(pulled.load(SeqCst), 12, "{what}");
    }
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_cut_by_its_caller_ends_the_handlers_upload_in_no_reply() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    let (_actor, local) = seen.spawn(&a, "sink");
    let remote = b.cluster.resolve::<SinkApi>("sink").await.unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    let two_parts = "131072 Some(NoReply)";
    for (what, to) in [("remote", &remote), ("local", &local)] {
        let failed = to.upload(read(), cut(How::Fails, 0, &pulled)).await;
        assert!(matches!(&failed, Err(CallError::Codec(e)) if e == "cut"), "{what}: the stream's error: {failed:?}");
        assert_eq!(seen.ended(1).await.pop().unwrap(), two_parts, "{what}");
        seen.ends.lock().unwrap().clear();

        if what == "remote" {
            // A local handler takes a part of any size.
            let over = to.upload(read(), cut(How::Oversized, 0, &pulled)).await;
            assert!(matches!(over, Err(CallError::NotSent(axon_net::SubmitReason::Oversized))), "{over:?}");
            assert_eq!(seen.ended(1).await.pop().unwrap(), two_parts, "{what}");
            seen.ends.lock().unwrap().clear();
        }

        let (up, panics) = (to.clone(), cut(How::Panics, 0, &pulled));
        let panicked = tokio::spawn(async move { up.upload(read(), panics).await }).await;
        assert!(panicked.is_err_and(|e| e.is_panic()), "{what}: the caller's stream panics");
        assert_eq!(seen.ended(1).await.pop().unwrap(), two_parts, "{what}: a panic cuts the body");
        seen.ends.lock().unwrap().clear();

        let dropped =
            tokio::time::timeout(Duration::from_millis(300), to.upload(read(), cut(How::Pauses, 3_600_000, &pulled)));
        assert!(dropped.await.is_err(), "{what}: the body waits on its stream");
        assert_eq!(seen.ended(1).await.pop().unwrap(), two_parts, "{what}: the caller dropped the call");
        seen.ends.lock().unwrap().clear();
    }
    settled(&b).await;
    settled(&a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_answers_first_or_drops_the_upload_stops_the_body() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    let (_actor, local) = seen.spawn(&a, "sink");
    let remote = b.cluster.resolve::<SinkApi>("sink").await.unwrap();
    for (what, to) in [("remote", remote), ("local", local)] {
        let pulled = Arc::new(AtomicUsize::new(0));
        let early = to.upload(Up::Take(Take::Answer), body(12 << 20, 1 << 20, &pulled)).await.unwrap();
        assert_eq!(early, "early", "{what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(pulled.load(SeqCst) <= 4, "{what}: the body went on: {pulled:?}");

        let pulled = Arc::new(AtomicUsize::new(0));
        let (up, body) = (to.clone(), body(12 << 20, 1 << 20, &pulled));
        let call = tokio::spawn(async move { up.upload(Up::Take(Take::Drop), body).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(pulled.load(SeqCst) <= 4 && !call.is_finished(), "{what}: the caller waits for the reply");
        to.send(Up::Release).await.unwrap();
        assert_eq!(call.await.unwrap().unwrap(), "no body", "{what}");
    }
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drain_finishes_an_upload_and_a_stop_cuts_it() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    let (actor, _) = seen.spawn(&a, "drained");
    let remote = b.cluster.resolve::<SinkApi>("drained").await.unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    let (up, whole) = (remote.clone(), body(12 << 20, 256 << 10, &pulled));
    let call = tokio::spawn(async move { up.upload(read(), whole).await });
    eventually("the handler reads the body", || async { seen.reading.load(SeqCst) == 1 }).await;
    actor.lifecycle.drain(Some(Duration::from_secs(10)));
    assert_eq!(call.await.unwrap().unwrap(), "12582912 None");
    assert_eq!(actor.join().await.reason, StopReason::Drained);

    let (actor, _) = seen.spawn(&a, "stopped");
    let remote = b.cluster.resolve::<SinkApi>("stopped").await.unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    let (up, stalls) = (remote.clone(), cut(How::Pauses, 3_600_000, &pulled));
    let call = tokio::spawn(async move { up.upload(read(), stalls).await });
    // The handler took it before the stop: one stopped first refuses it instead.
    let taken = || seen.reading.load(SeqCst) == 2 && pulled.load(SeqCst) == 2;
    eventually("the handler reads the body", || async { taken() }).await;
    actor.lifecycle.stop();
    let stopped = call.await.unwrap();
    assert!(matches!(stopped, Err(CallError::NoReply)), "{stopped:?}");
    assert_eq!(actor.join().await.reason, StopReason::Stopped);
    settled(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upload_answered_in_parts_is_a_two_way_stream() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    let (_actor, local) = seen.spawn(&a, "echo");
    let remote = b.cluster.resolve::<SinkApi>("echo").await.unwrap();
    let echo = || Up::Take(Take::Echo);
    for (what, to) in [("remote", &remote), ("local", &local)] {
        let pulled = Arc::new(AtomicUsize::new(0));
        let mut parts = to.upload_parts(echo(), body(12 << 20, 256 << 10, &pulled)).await.unwrap();
        let (got, end) = read_all(&mut parts).await;
        assert!(end.is_none() && joined(&got) == pattern(0, 12 << 20), "{what}: {end:?}");
        assert_eq!(seen.ended(1).await.pop().unwrap(), "echo Ok(())", "{what}");
        seen.ends.lock().unwrap().clear();

        // Dropping the reply's parts cuts the body too.
        let pulled = Arc::new(AtomicUsize::new(0));
        let mut parts = to.upload_parts(echo(), cut(How::Pauses, 3_600_000, &pulled)).await.unwrap();
        assert!(matches!((parts.next().await, parts.next().await), (Some(Ok(_)), Some(Ok(_)))), "{what}");
        drop(parts);
        assert_eq!(seen.ended(1).await.pop().unwrap(), "echo Ok(())", "{what}: a caller gone is not an error");
        assert_eq!(pulled.load(SeqCst), 2, "{what}");
        seen.ends.lock().unwrap().clear();
    }
    let named = b.cluster.named::<SinkApi>("echo");
    let pulled = Arc::new(AtomicUsize::new(0));
    let mut parts = named.upload_parts(echo(), body(3 << 20, 1 << 20, &pulled)).await.unwrap();
    let (got, end) = read_all(&mut parts).await;
    assert!(end.is_none() && joined(&got) == pattern(0, 3 << 20), "{end:?}");
    settled(&b).await;
    settled(&a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upload_waits_for_credit_and_for_parts_at_most_request_timeout() {
    // Short, so a stalled body times out in moments.
    let (a, b) = two(|cfg| cfg.request_timeout = Duration::from_millis(300)).await;
    let seen = Seen::default();
    let (_actor, local) = seen.spawn(&a, "slow");
    let remote = b.cluster.resolve::<SinkApi>("slow").await.unwrap();
    for (what, to) in [("remote", &remote), ("local", &local)] {
        // A handler that does not read: the caller waits for credit, then cuts the body.
        let pulled = Arc::new(AtomicUsize::new(0));
        let asked = Instant::now();
        let held = to.upload(Up::Take(Take::Hold), body(12 << 20, 1 << 20, &pulled)).await;
        let waited = asked.elapsed();
        assert!(matches!(held, Err(CallError::Timeout)), "{what}: {held:?}");
        // Well under the default 5 s, on a loaded box too.
        assert!(waited >= Duration::from_millis(290) && waited < Duration::from_secs(3), "{what}: {waited:?}");
        // A send may time out in 300 ms on a loaded box; a second release finds nothing held.
        eventually("the handler is told to read", || async { to.send(Up::Release).await.is_ok() }).await;
        assert_eq!(seen.ended(1).await.pop().unwrap(), "4194304 Some(NoReply)", "{what}: the window, then the cut");
        seen.ends.lock().unwrap().clear();

        // A body whose stream stalls: the handler's wait for a part ends, and it answers.
        let pulled = Arc::new(AtomicUsize::new(0));
        let stalled = to.upload(read(), cut(How::Pauses, 1000, &pulled)).await.unwrap();
        assert_eq!(stalled, "131072 Some(Timeout)", "{what}");
        // The handler records its end after it answers.
        assert_eq!(seen.ended(1).await.pop().unwrap(), stalled, "{what}");
        seen.ends.lock().unwrap().clear();
    }
    settled(&b).await;
    settled(&a).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_carries_64_bodies_and_refuses_the_next_full() {
    let (a, b) = two(|_| {}).await;
    let seen = Seen::default();
    seen.spawn(&a, "sink");
    let remote = b.cluster.resolve::<SinkApi>("sink").await.unwrap();
    let pulled = Arc::new(AtomicUsize::new(0));
    let hold = |remote: &RemoteRef<SinkApi>| {
        let (remote, body) = (remote.clone(), body(4 << 10, 1 << 10, &pulled));
        tokio::spawn(async move { remote.upload(Up::Take(Take::Hold), body).await })
    };
    let held: Vec<_> = (0..64).map(|_| hold(&remote)).collect();
    eventually("64 bodies are held", || async { seen.holding.load(SeqCst) == 64 }).await;
    let refused = remote.upload(read(), body(4 << 10, 1 << 10, &pulled)).await;
    assert!(matches!(refused, Err(CallError::Rejected(Rejection::Full))), "{refused:?}");
    assert_eq!(refused.unwrap_err().retry(), Retry::Later);

    remote.send(Up::Release).await.unwrap();
    for call in held {
        assert_eq!(call.await.unwrap().unwrap(), "4096 None");
    }
    assert_eq!(remote.upload(read(), body(4 << 10, 1 << 10, &pulled)).await.unwrap(), "4096 None", "a slot is back");
    settled(&b).await;
}

/// Body frames a peer could send that break the protocol close its session, and only its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_part_past_its_credit_or_a_short_body_frame_closes_only_that_session() {
    let (a, b) = two(|_| {}).await;
    static KEPT: std::sync::Mutex<Vec<(Upload, axon_cluster::Reply)>> = std::sync::Mutex::new(Vec::new());
    // Answers with the body, or keeps it unread, and the reply unsent, for a head of "keep".
    let concat = |_: &axon_net::Identity, head: Bytes, reply: Option<axon_cluster::Reply>| {
        let reply = reply.ok_or(Rejection::InvalidMessage)?;
        let mut upload = reply.upload().ok_or(Rejection::InvalidMessage)?;
        if head == "keep" {
            KEPT.lock().unwrap().push((upload, reply));
            return Ok(());
        }
        tokio::spawn(async move {
            let mut all = BytesMut::from(&head[..]);
            while let Some(part) = upload.next().await {
                all.extend_from_slice(&part.unwrap_or_else(|_| Bytes::from_static(b"!cut")));
            }
            let _ = reply.send(all.freeze());
        });
        Ok(())
    };
    let endpoint = a.cluster.serve("concat", 0xC0C, 1, concat).unwrap();
    // REQUEST, PART, END and CREDIT with BODY, and a TELL with it, from the call frame's pinned bytes.
    let (upload, part, end, credit, tell, reply) = (0x1003, 0x1006, 0x1005, 0x1007, 0x1001, 4);
    let frame = |kind, id, payload: &[u8]| axon_net::Frame {
        kind,
        schema: 1,
        protocol: endpoint,
        id,
        payload: Bytes::copy_from_slice(payload),
        ..axon_net::Frame::default()
    };
    /// The next frame that is not a grant of credit.
    async fn answer(raw: &mut axon_net::Session) -> Result<(u16, u64, Bytes), axon_net::Error> {
        loop {
            let answer = tokio::time::timeout(Duration::from_secs(5), raw.receiver.recv()).await.expect("an answer");
            let f = answer?.into_frame();
            if f.kind != 0x1007 {
                return Ok((f.kind, f.id, f.payload));
            }
        }
    }
    let z = transport("z");
    let raw_session = || axon_net::connect(a.cluster.local_addr(), axon_net::Lane::Control, None, &z);

    let mut raw = raw_session().await.unwrap();
    for f in [frame(upload, 1, b"head:"), frame(part, 1, b"ab"), frame(part, 1, b"cd"), frame(end, 1, &[0])] {
        raw.sender.submit(f, None).await.unwrap();
    }
    assert_eq!(answer(&mut raw).await.unwrap(), (reply, 1, Bytes::from_static(b"head:abcd")));
    // A part for a body nobody reads is dropped, and a cut body reads as cut.
    raw.sender.submit(frame(part, 99, b"lost"), None).await.unwrap();
    for f in [frame(upload, 2, b"head:"), frame(part, 2, b"ab"), frame(end, 2, &[1])] {
        raw.sender.submit(f, None).await.unwrap();
    }
    assert_eq!(answer(&mut raw).await.unwrap(), (reply, 2, Bytes::from_static(b"head:ab!cut")));
    // Five parts of 1 MiB to a body nobody reads: the fourth takes its 4 MiB below zero, the fifth is past it.
    raw.sender.submit(frame(upload, 3, b"keep"), None).await.unwrap();
    for _ in 0..5 {
        raw.sender.submit(frame(part, 3, &vec![7; 1 << 20]), None).await.unwrap();
    }
    assert!(answer(&mut raw).await.is_err(), "a part past its credit closes the session");

    for bad in [frame(upload, 1, b"head:"), frame(end, 1, &[])] {
        let mut raw = raw_session().await.unwrap();
        raw.sender.submit(frame(upload, 1, b"head:"), None).await.unwrap();
        raw.sender.submit(bad, None).await.unwrap();
        assert!(answer(&mut raw).await.is_err(), "an id in use or an end without its byte closes the session");
    }
    for bad in [frame(credit, 1, &[0, 1]), frame(tell, 1, b"x")] {
        let mut raw = raw_session().await.unwrap();
        raw.sender.submit(bad, None).await.unwrap();
        assert!(answer(&mut raw).await.is_err(), "a short credit or a tell with a body closes the session");
    }
    // A plain request has no body: the endpoint refuses it, over b's own session to a.
    let plain = b.cluster.request("a", "concat", 0xC0C, 1, Bytes::from_static(b"still")).await;
    assert!(matches!(plain, Err(ClusterError::Call(CallError::Rejected(Rejection::InvalidMessage)))), "{plain:?}");
    KEPT.lock().unwrap().clear();
}
