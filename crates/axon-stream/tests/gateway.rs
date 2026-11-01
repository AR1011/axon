//! The websocket gateway of the streams guide (§ A websocket gateway) on two nodes, end to end: a token that names no
//! account is refused, an account's sockets and requests are limited over both nodes, a client sees only what its plan
//! may and its cursor moves past the rest, a burst past the outbox pauses a reading client without losing a record, a
//! client that stops reading is dropped and gives its socket back, a paused one the rings passed is told to reset, and
//! a client resumes after its cursor on another node. The program is the guide's, and `the_program_is_the_guides` holds
//! the two the same.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll, ready};
use std::time::Duration;

use axon::{
    Actor, ActorHandle, ActorSystem, Context, JobHandle, RejectReason, Sink, SinkEvent, SinkHandle, Stream,
    SystemError, TimerId,
};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, Permit, Permits, QuotaConfig, Quotas, Rate};
use axon_net::SharedSecret;
use axon_stream::{
    Cursor, DeliveryPolicy, SequencedBatch, StartPosition, StreamEvent, StreamKey, StreamNode, StreamNodeConfig,
    SubscribeOptions,
};
use bytes::Bytes;
use tokio::sync::mpsc;

/// Sockets an account holds open at once, on every node together.
const SOCKETS: Permits = Permits::new("ws/sockets", 2);
/// Requests an account sends a second, over all its sockets.
const REQUESTS: Rate = Rate::new("ws/requests", 20, Duration::from_secs(1));
/// Frames the socket's writer holds before `try_send` says `Full`.
const IN_FLIGHT: usize = 64;
/// Frames waiting behind a full socket, past which the session takes no more records until the client reads.
const OUTBOX: usize = 256;
/// How long a client that stopped reading keeps its socket.
const PATIENCE: Duration = Duration::from_secs(5);

fn prices() -> StreamKey {
    StreamKey::new("prices", "fx")
}

/// An account and the symbols its plan may see.
struct Account {
    name: &'static str,
    symbols: &'static [&'static str],
}

/// Stands in for checking a signed token, or asking the service that issued it.
fn authenticate(token: &str) -> Option<Account> {
    match token {
        "alice-token" => Some(Account { name: "alice", symbols: &["EURUSD", "GBPUSD"] }),
        "bob-token" => Some(Account { name: "bob", symbols: &["EURUSD", "GBPUSD", "USDJPY"] }),
        _ => None,
    }
}

/// The socket's read half: frames from the client.
struct Frames(mpsc::Receiver<String>);

impl Stream for Frames {
    type Item = String;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<String>> {
        self.0.poll_recv(cx)
    }
}

type Room = Pin<Box<dyn Future<Output = Option<mpsc::OwnedPermit<String>>> + Send>>;

/// The socket's write half: it waits for the client to read, as a socket waits for its peer's window.
struct Wire {
    tx: mpsc::Sender<String>,
    room: Option<Room>,
    permit: Option<mpsc::OwnedPermit<String>>,
}

impl Sink<String> for Wire {
    type Error = std::io::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        if self.permit.is_none() {
            let tx = self.tx.clone();
            let room = self.room.get_or_insert_with(|| Box::pin(async move { tx.reserve_owned().await.ok() }));
            let permit = ready!(room.as_mut().poll(cx));
            self.room = None;
            self.permit = Some(permit.ok_or(std::io::ErrorKind::BrokenPipe)?);
        }
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, frame: String) -> Result<(), Self::Error> {
        self.permit.take().expect("poll_ready first").send(frame);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

enum Msg {
    Client(Option<String>),
    Socket(SinkEvent<std::io::Error>),
    /// Tagged with its subscription: a cancelled source's items still arrive, and a replaced one's are ignored.
    Prices(u32, Option<StreamEvent>),
    /// A pause's deadline, tagged the same way.
    Stalled(u32),
}

/// One per socket, and only for an authenticated one.
struct Session {
    account: Account,
    /// One of the account's `SOCKETS`, given back when the session ends, however it ends.
    _socket: Permit,
    quotas: Quotas,
    streams: StreamNode,
    halves: Option<(Frames, Wire)>,
    tx: Option<SinkHandle<String>>,
    outbox: VecDeque<String>,
    following: Option<JobHandle>,
    follows: u32,
    /// Where a subscription paused for a slow client resumes, and the deadline for it to read.
    paused: Option<(Cursor, TimerId)>,
}

struct Gateway {
    system: ActorSystem,
    quotas: Quotas,
    streams: StreamNode,
}

impl Gateway {
    /// The upgrade request: the token names the account before anything is counted, then one of the account's
    /// `SOCKETS` admits it. A refusal is the HTTP answer.
    fn admit(&self, token: &str) -> Result<(Account, Permit), (u16, String)> {
        let account = authenticate(token).ok_or((401, "unknown token".to_string()))?;
        let permit = self.quotas.acquire(account.name, &SOCKETS).map_err(|denied| (429, denied.to_string()))?;
        Ok((account, permit))
    }

    /// The upgraded socket: its session holds the permit until it ends.
    fn open(&self, admitted: (Account, Permit), halves: (Frames, Wire)) -> Result<ActorHandle<Msg>, SystemError> {
        let (account, _socket) = admitted;
        let session = Session {
            account,
            _socket,
            quotas: self.quotas.clone(),
            streams: self.streams.clone(),
            halves: Some(halves),
            tx: None,
            outbox: VecDeque::new(),
            following: None,
            follows: 0,
            paused: None,
        };
        self.system.spawn(session).start()
    }
}

impl Session {
    fn say(&mut self, frame: String, cx: &mut Context<'_, Self>) {
        if self.outbox.len() < 2 * OUTBOX {
            self.outbox.push_back(frame);
        } else {
            cx.stop(); // records stop at OUTBOX: this client asks without reading
        }
    }

    /// The application's own protocol: `follow`, `follow <generation>:<sequence>` and `echo <text>`.
    fn request(&mut self, request: &str, cx: &mut Context<'_, Self>) {
        if let Err(denied) = self.quotas.take(self.account.name, &[(REQUESTS, 1)]) {
            return self.say(format!("429 {denied}"), cx);
        }
        let reply = match request.split_once(' ').unwrap_or((request, "")) {
            ("echo", text) => text.to_string(),
            ("follow", after) => self.follow(after, cx).map_or_else(|e| format!("error {e}"), |()| "following".into()),
            _ => format!("error unknown request {request:?}"),
        };
        self.say(reply, cx);
    }

    fn follow(&mut self, after: &str, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let start = match after.split_once(':') {
            None => StartPosition::Live,
            Some((generation, sequence)) => {
                let (Ok(generation), Ok(sequence)) = (generation.parse(), sequence.parse()) else {
                    return Err(format!("bad cursor {after:?}"));
                };
                StartPosition::After(Cursor { generation, sequence })
            }
        };
        self.subscribe(start, cx)
    }

    /// Replaces the subscription, if any.
    fn subscribe(&mut self, start: StartPosition, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let options = SubscribeOptions { start, ..Default::default() };
        let subscription = self.streams.subscribe(&prices(), options).map_err(|e| format!("{e:?}"))?;
        self.stop_following(cx);
        let follows = self.follows;
        let job = cx.attach(subscription, move |event| Msg::Prices(follows, event)).map_err(|e| format!("{e:?}"))?;
        self.following = Some(job);
        Ok(())
    }

    fn stop_following(&mut self, cx: &mut Context<'_, Self>) {
        if let Some(job) = self.following.take() {
            job.cancel();
        }
        if let Some((_, stalled)) = self.paused.take() {
            cx.cancel_timer(stalled);
        }
        self.follows = self.follows.wrapping_add(1);
    }

    fn flush(&mut self) {
        let Some(tx) = &self.tx else { return };
        while let Some(frame) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(frame) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message); // until SinkEvent::Ready
                }
                break;
            }
        }
    }

    fn deliver(&mut self, batch: &SequencedBatch, cx: &mut Context<'_, Self>) {
        let generation = batch.generation();
        let mut unseen = None; // the last record handled, when the filter dropped it
        for (sequence, record) in (batch.first()..).zip(batch.records()) {
            if self.outbox.len() >= OUTBOX {
                // The client is OUTBOX frames behind: keep its place, not its records, and give it PATIENCE to read.
                self.stop_following(cx);
                match cx.schedule_after(PATIENCE, Msg::Stalled(self.follows)) {
                    Ok(stalled) => self.paused = Some((Cursor { generation, sequence: sequence - 1 }, stalled)),
                    Err(_) => cx.stop(),
                }
                break;
            }
            // The client's filter: what its plan may see. Each frame carries its cursor.
            let symbol = record.split(|&b| b == b' ').next().unwrap_or_default();
            if self.account.symbols.iter().any(|s| s.as_bytes() == symbol) {
                self.outbox.push_back(format!("{generation}:{sequence} {}", String::from_utf8_lossy(record)));
                unseen = None;
            } else {
                unseen = Some(sequence);
            }
        }
        // What the filter drops moves the client's cursor too, or a narrow plan's cursor falls out of the rings.
        if let Some(sequence) = unseen {
            self.say(format!("at {generation}:{sequence}"), cx);
        }
    }
}

impl Actor for Session {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (frames, wire) = self.halves.take().ok_or("started twice")?;
        cx.attach(frames, Msg::Client).map_err(|e| format!("{e:?}"))?;
        self.tx = Some(cx.attach_sink(wire, IN_FLIGHT, Msg::Socket).map_err(|e| format!("{e:?}"))?);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Client(Some(request)) => self.request(&request, cx),
            Msg::Client(None) | Msg::Socket(SinkEvent::Closed(_)) => cx.stop(),
            Msg::Socket(SinkEvent::Ready) => {} // on_turn_end writes the outbox
            Msg::Prices(follows, _) | Msg::Stalled(follows) if follows != self.follows => {} // replaced since
            Msg::Prices(_, Some(StreamEvent::Batch(batch))) => self.deliver(&batch, cx),
            // Continuity broke (a restart, or no ring holds the cursor): the client follows again.
            Msg::Prices(_, Some(event)) if event.is_terminal() => self.say(format!("reset {event:?}"), cx),
            // Still paused after PATIENCE: dropped, it reconnects and resumes after its cursor.
            Msg::Stalled(_) => cx.stop(),
            Msg::Prices(..) => {} // Ready, CaughtUp, OriginLost, the end after a terminal event
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.flush();
        // Written after the flush: an outbox it empties brings no SinkEvent::Ready to resume on.
        if let Some((at, _)) = self.paused.filter(|_| self.outbox.len() <= OUTBOX / 2)
            && let Err(e) = self.subscribe(StartPosition::After(at), cx)
        {
            self.stop_following(cx);
            self.say(format!("reset {e}"), cx); // the rings turned over while it read
            self.flush();
        }
        Ok(())
    }
}

/// The client's end of the pipe.
struct Client(mpsc::Sender<String>, mpsc::Receiver<String>);

impl Client {
    async fn send(&self, frame: &str) {
        self.0.send(frame.into()).await.expect("socket open");
    }
    async fn next(&mut self) -> String {
        let frame = tokio::time::timeout(Duration::from_secs(10), self.1.recv()).await;
        frame.expect("a frame within 10 s").expect("socket open")
    }
}

fn socket() -> ((Frames, Wire), Client) {
    let (to_server, from_client) = mpsc::channel(16);
    let (to_client, from_server) = mpsc::channel(16);
    let wire = Wire { tx: to_client, room: None, permit: None };
    ((Frames(from_client), wire), Client(to_server, from_server))
}

struct Node {
    gateway: Gateway,
    cluster: Cluster,
}

async fn node(id: &str, role: &str, discovery: &MemoryDiscovery) -> Node {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.labels.insert("role".into(), role.into());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let quotas = Quotas::start(&cluster, QuotaConfig::default()).await.unwrap();
    let upstream = match role {
        "origin" => vec![],
        _ => vec![std::collections::BTreeMap::from([("role".to_string(), "origin".to_string())])],
    };
    let mut config = StreamNodeConfig::new(upstream);
    // Small enough that a narrow plan's stale cursor falls out of it.
    let ring = axon_stream::RingLimits { namespace: "prices".into(), max_records: 1024, max_bytes: 1 << 20 };
    config.core.rings = vec![ring];
    let streams = StreamNode::start(&cluster, config).await.unwrap();
    Node { gateway: Gateway { system, quotas, streams }, cluster }
}

/// An origin and an edge, each a gateway, each seeing both as quota nodes, neither still pulling.
async fn cluster() -> (Node, Node) {
    let discovery = MemoryDiscovery::new();
    let a = node("a", "origin", &discovery).await;
    let b = node("b", "edge", &discovery).await;
    for node in [&a, &b] {
        let quota_nodes = |view: &axon_cluster::View| {
            view.members.iter().filter(|m| m.record.metadata.contains_key("axon.quota")).count() == 2
        };
        node.cluster.wait_view(quota_nodes).await.unwrap();
    }
    axon_testkit::eventually("no replica still pulls", || async {
        !a.cluster.quotas_starting() && !b.cluster.quotas_starting()
    })
    .await;
    a.gateway.streams.open_stream(&prices(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    (a, b)
}

fn publish(origin: &Node, records: &[&'static str]) {
    origin.gateway.streams.publish(&prices(), records.iter().map(|r| Bytes::from(*r)).collect()).unwrap();
}

/// Connects, retrying while this node's share is used but the account has room, as a client told 429 with
/// `retry_after` would. Any other refusal is returned.
async fn connect(gateway: &Gateway, token: &str) -> Result<(ActorHandle<Msg>, Client), (u16, String)> {
    for _ in 0..100 {
        match gateway.admit(token) {
            Ok(admitted) => {
                let (halves, client) = socket();
                return Ok((gateway.open(admitted, halves).unwrap(), client));
            }
            Err((429, why)) if why.contains("share") => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(refused) => return Err(refused),
        }
    }
    panic!("{token}: this node's share never grew");
}

/// Sends a request and returns its reply, asking again on a 429 as a client would after `retry_after`: a key new to a
/// node has only a fallback share of the account's requests until its grant comes. For a request whose reply comes
/// next, ahead of any record.
async fn ask(client: &mut Client, request: &str) -> String {
    for _ in 0..100 {
        client.send(request).await;
        match client.next().await {
            refused if refused.starts_with("429 ") => again().await,
            reply => return reply,
        }
    }
    panic!("{request}: refused 100 times");
}

/// The wait a 429 asks for.
async fn again() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[test]
fn the_program_is_the_guides() {
    let test = include_str!("gateway.rs");
    let program = &test[test.find("use std::").unwrap()..test.find("\nstruct Node").unwrap()];
    let guide = include_str!("../../axon/guide/streams.md");
    assert!(guide.contains(program), "tests/gateway.rs and guide/streams.md § A websocket gateway differ");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_sees_only_its_plan_and_resumes_after_its_cursor_on_another_node() {
    let (a, b) = cluster().await;
    publish(&a, &["EURUSD 1.0842", "USDJPY 149.10", "GBPUSD 1.2710"]);

    let (session, mut alice) = connect(&b.gateway, "alice-token").await.unwrap();
    let (_bob_session, mut bob) = connect(&b.gateway, "bob-token").await.unwrap();
    assert_eq!(ask(&mut alice, "follow 1:0").await, "following");
    assert_eq!(alice.next().await, "1:1 EURUSD 1.0842");
    assert_eq!(alice.next().await, "1:3 GBPUSD 1.2710");
    assert_eq!(ask(&mut bob, "follow 1:0").await, "following");
    for frame in ["1:1 EURUSD 1.0842", "1:2 USDJPY 149.10", "1:3 GBPUSD 1.2710"] {
        assert_eq!(bob.next().await, frame);
    }

    drop(alice);
    session.join().await;
    publish(&a, &["USDJPY 149.20", "EURUSD 1.0845", "GBPUSD 1.2702", "USDJPY 149.30"]);
    for frame in ["1:4 USDJPY 149.20", "1:5 EURUSD 1.0845", "1:6 GBPUSD 1.2702", "1:7 USDJPY 149.30"] {
        assert_eq!(bob.next().await, frame);
    }

    let (_session, mut alice) = connect(&a.gateway, "alice-token").await.unwrap();
    assert_eq!(ask(&mut alice, "follow 1:3").await, "following");
    assert_eq!(alice.next().await, "1:5 EURUSD 1.0845");
    assert_eq!(alice.next().await, "1:6 GBPUSD 1.2702");
    assert_eq!(alice.next().await, "at 1:7");

    // More records her plan does not see than the ring holds: her cursor moves past them, so it stays in the ring.
    publish(&a, &["USDJPY 149.40"; 2000]);
    assert_eq!(alice.next().await, "at 1:2007");
    let refused = ask(&mut alice, "follow 1:6").await;
    assert!(refused.starts_with("error ") && refused.contains("HistoryUnavailable"), "{refused}");
    // Following again on the same socket replaces the live subscription: nothing of it arrives after the reply.
    assert_eq!(ask(&mut alice, "follow 1:2007").await, "following");
    publish(&a, &["EURUSD 1.0850"]);
    assert_eq!(alice.next().await, "1:2008 EURUSD 1.0850");
    publish(&a, &["GBPUSD 1.2700"]);
    assert_eq!(alice.next().await, "1:2009 GBPUSD 1.2700");
    // The stream restarts as generation 2: continuity broke, and the client is told.
    a.gateway.streams.restart_stream(&prices(), 2).unwrap();
    assert_eq!(alice.next().await, "reset ResetRequired(GenerationChanged)");
    a.gateway.system.shutdown().await.unwrap();
    b.gateway.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn following_again_mid_burst_keeps_every_record_once_and_in_order() {
    let (a, _b) = cluster().await;
    let (_session, mut bob) = connect(&a.gateway, "bob-token").await.unwrap();
    assert_eq!(ask(&mut bob, "follow").await, "following");
    publish(&a, &["EURUSD 1.0842"; 100]);
    for sequence in 1..=100 {
        assert_eq!(bob.next().await, format!("1:{sequence} EURUSD 1.0842"));
    }
    let (mut next, mut published) = (101, 100); // the sequence bob must get next, and the last one published
    for round in 0..15 {
        // The follow request and a batch of the subscription it replaces race into the session.
        let after = next - 4;
        if round % 2 == 0 {
            bob.send(&format!("follow 1:{after}")).await;
        }
        publish(&a, &["EURUSD 1.0842"; 100]);
        published += 100;
        if round % 2 == 1 {
            bob.send(&format!("follow 1:{after}")).await;
        }
        let mut replied = false;
        while !replied || next <= published {
            match bob.next().await.as_str() {
                "following" => (next, replied) = (after + 1, true),
                // The burst can outrun this node's share of bob's requests before its grant comes, as in `ask`.
                refused if refused.starts_with("429 ") => {
                    again().await;
                    bob.send(&format!("follow 1:{after}")).await;
                }
                frame => {
                    assert_eq!(frame, format!("1:{next} EURUSD 1.0842"), "round {round}");
                    next += 1;
                }
            }
        }
    }
    a.gateway.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sockets_and_requests_are_limited_per_account_over_both_nodes_and_a_slow_client_gives_its_socket_back() {
    let (a, b) = cluster().await;
    assert_eq!(connect(&b.gateway, "forged").await.err().map(|(status, _)| status), Some(401));

    let (_alice_a, mut alice_on_a) = connect(&a.gateway, "alice-token").await.unwrap();
    let (slow_session, mut alice_on_b) = connect(&b.gateway, "alice-token").await.unwrap();
    let (_bob_a, mut bob_on_a) = connect(&a.gateway, "bob-token").await.unwrap();
    let (_bob_b, mut bob_on_b) = connect(&b.gateway, "bob-token").await.unwrap();
    // A key new to both nodes may take a fallback share on each until the replicas have heard from both, a report
    // cycle (spec/quotas.md §5, a new key); after that the limit holds over both.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for gateway in [&a.gateway, &b.gateway] {
        let (status, why) = connect(gateway, "alice-token").await.err().expect("a third socket of alice's");
        assert_eq!(status, 429);
        assert!(why.contains("at its limit"), "{why}");
    }

    // Each of bob's sockets asks 50 times a second for 2 s.
    let mut every = tokio::time::interval(Duration::from_millis(20));
    for n in 0..100 {
        every.tick().await;
        bob_on_a.send(&format!("echo {n}")).await;
        bob_on_b.send(&format!("echo {n}")).await;
    }
    let mut admitted = 0;
    for bob in [&mut bob_on_a, &mut bob_on_b] {
        for n in 0..100 {
            match bob.next().await {
                frame if frame.starts_with("429 ") => {}
                frame => {
                    assert_eq!(frame, n.to_string());
                    admitted += 1;
                }
            }
        }
    }
    // One account's 20 a second over both sockets, 40 in the 2 s; a rate per socket admits 80.
    assert!(admitted > 0 && admitted < 60, "{admitted} of 200 admitted");

    // Both of alice's sockets resume from the start, 300 records behind: more than OUTBOX.
    publish(&a, &["EURUSD 1.0842"; 300]);
    assert_eq!(ask(&mut alice_on_b, "follow 1:0").await, "following");
    assert_eq!(ask(&mut alice_on_a, "follow 1:0").await, "following");
    for sequence in 1..=300 {
        assert_eq!(alice_on_a.next().await, format!("1:{sequence} EURUSD 1.0842"));
    }
    // The one on b does not read: its session pauses, then drops it after PATIENCE and gives its socket back.
    let exit = tokio::time::timeout(PATIENCE * 3, slow_session.join()).await;
    assert!(exit.is_ok(), "the slow client's session still runs");
    let (_again, _) = connect(&b.gateway, "alice-token").await.expect("the slow session's socket came back");

    // A reading client falls OUTBOX behind and pauses; the rings turn over before it reads, so it is told to reset.
    publish(&a, &["EURUSD 1.0842"; 400]);
    publish(&a, &["EURUSD 1.0842"; 2000]);
    let mut next = 301;
    let reset = loop {
        match alice_on_a.next().await {
            frame if frame.starts_with("reset ") => break frame,
            frame => assert_eq!(frame, format!("1:{next} EURUSD 1.0842")),
        }
        next += 1;
    };
    assert!(next > 301 + OUTBOX / 2 && reset.contains("HistoryUnavailable"), "at 1:{next}: {reset}");
    a.gateway.system.shutdown().await.unwrap();
    b.gateway.system.shutdown().await.unwrap();
}
