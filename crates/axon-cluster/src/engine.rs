//! Sessions to peers, the endpoint registry, and the call protocol.
//!
//! Each known peer has a task per lane that owns that lane's outgoing session while calls use it: dial, serve, redial
//! with backoff. It ends when the link goes idle for `idle_link_timeout` or the node leaves the view, and the next call
//! starts another. The `Control` task is also the failure detector — the transport keepalive ends a dead session, and
//! a failed redial marks the member unreachable. After a session that went silent, so does a redial that has not
//! connected within a keepalive interval. A `Live` or `Bulk` task dials the boot `Control` is up to, once it is, and
//! closes when `Control` goes down; its own failures fail only its calls.

#[cfg(axon_loom)]
use loom::sync::atomic::AtomicU8;
use std::collections::{BTreeMap, HashMap, btree_map};
use std::future::Future;
use std::net::SocketAddr;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
#[cfg(not(axon_loom))]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon::trace::{Scoped, enter, unix_micros};
use axon::{AtomicHistogram, DeadLetter, DeadLetters, HistogramSnapshot, Span, SpanKind, Stream, TraceContext, Tracer};
use axon_net::rustls::pki_types::ServerName;
use axon_net::rustls::{ClientConfig, ServerConfig};
use axon_net::{
    AcceptLimits, Backoff, ClientTls, DialError, Error, Frame, Lane, Listener, ListenerStatsReader, Peer, PeerContext,
    Receiver, Redial, Sender, ServerTls, SessionStats, SubmitReason, Timeout,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Handle;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tokio::time::{Instant, timeout_at};

use crate::discovery::NodeRecord;
use crate::membership::Membership;
use crate::wire::*;
use crate::{CallError, Rejection};

mod parts;
use parts::{Body, Flow, Grants};
pub use parts::{Parts, Upload};

/// What a peer asks: to call an endpoint, to start an actor of a kind, or where a key of a kind runs, which
/// `Cluster::watch_key` asks once per hold.
pub enum Action<'a> {
    Call { name: &'a str },
    Spawn { kind: &'a str },
    Watch { kind: &'a str },
}

/// Decides what an authenticated peer may do. Without one, every authenticated peer may do anything. It runs on the
/// I/O path, a call's check under the lock that finds its endpoint: it must not block or call into the `Cluster`.
pub trait Authorizer: Send + Sync + 'static {
    fn authorize(&self, peer: &Peer, action: Action<'_>) -> bool;
}

#[derive(Clone)]
pub struct Tls {
    pub server: Arc<ServerConfig>,
    pub client: Arc<ClientConfig>,
}

impl Tls {
    /// Certificates that fail a handshake with themselves under `name`, what peers dial this node by, or that this
    /// node's authenticator would not take as its own, would make it another node.
    fn check(&self, transport: &axon_net::Config, name: ServerName<'static>) -> Result<(), &'static str> {
        use axon_net::rustls::{ClientConnection, Connection, ServerConnection};
        let refused = Err("the certificates do not pass a TLS handshake under this node's id");
        let client = ClientConnection::new(self.client.clone(), name).map(Connection::from);
        let server = ServerConnection::new(self.server.clone()).map(Connection::from);
        let (Ok(mut client), Ok(mut server)) = (client, server) else { return refused };
        // TLS 1.2 with a client certificate takes two round trips, TLS 1.3 after a HelloRetryRequest three.
        for _ in 0..4 {
            if !client.is_handshaking() && !server.is_handshaking() {
                break;
            }
            if shuttle(&mut client, &mut server).and_then(|()| shuttle(&mut server, &mut client)).is_none() {
                return refused;
            }
        }
        if client.is_handshaking() || server.is_handshaking() {
            return refused;
        }
        let takes = |certificates: Option<&[_]>| {
            let (identity, namespace) = (&transport.identity, &transport.namespace);
            let certificates = certificates.unwrap_or_default();
            let peer = PeerContext { identity, namespace, remote: None, secure: true, certificates };
            catch_unwind(AssertUnwindSafe(|| transport.auth.authorize(&peer))).unwrap_or(false)
        };
        if !(takes(client.peer_certificates()) && takes(server.peer_certificates())) {
            return Err("this node's authenticator does not take the certificates as this node's id");
        }
        Ok(())
    }
}

/// A node is dialled by its id when that is a server name, else by its address.
fn dial_name(id: &str, address: SocketAddr) -> ServerName<'static> {
    ServerName::try_from(id.to_owned()).unwrap_or_else(|_| address.ip().into())
}

fn shuttle(from: &mut axon_net::rustls::Connection, to: &mut axon_net::rustls::Connection) -> Option<()> {
    let mut bytes = Vec::new();
    while from.wants_write() {
        from.write_tls(&mut bytes).ok()?;
    }
    let mut rest = &bytes[..];
    while !rest.is_empty() {
        to.read_tls(&mut rest).ok()?;
        to.process_new_packets().ok()?;
    }
    Some(())
}

/// The longest a call's budget runs: a longer one is cut to it, rather than overflow an `Instant` or the wire's
/// microseconds.
pub(crate) const LONGEST: Duration = Duration::from_secs(86_400 * 365 * 30);

/// The verdict on a message that has to wait for its actor: a supervised one between generations.
pub(crate) type Waiting = Pin<Box<dyn Future<Output = Result<(), Rejection>> + Send>>;
/// `Ok(None)` is admitted, `Ok(Some(wait))` once `wait` says so.
pub(crate) type Admit = Arc<dyn Fn(&Peer, Bytes, Option<Reply>) -> Result<Option<Waiting>, Rejection> + Send + Sync>;

pub(crate) struct Entry {
    pub name: Arc<str>,
    pub protocol: u64,
    pub schema: u16,
    /// Where callers send: a call that comes on another lane is served all the same.
    pub lane: Lane,
    pub admit: Admit,
}

#[derive(Default)]
pub(crate) struct Registry {
    by_id: HashMap<u64, Entry>,
    /// One endpoint per schema a name is published at, in the order they were.
    by_name: HashMap<Arc<str>, Vec<u64>>,
    /// Here, under the lock every admission already takes, so that swapping it costs a call nothing.
    authorizer: Option<Arc<dyn Authorizer>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Target {
    pub node: Arc<str>,
    pub incarnation: u128,
    pub endpoint: u64,
    pub schema: u16,
    pub lane: Lane,
}

pub(crate) enum Outcome {
    Reply(Bytes),
    End(u8),
}

impl Outcome {
    pub fn body(self) -> Result<Option<Bytes>, CallError> {
        match self {
            Outcome::Reply(body) => Ok(Some(body)),
            Outcome::End(code) => ended(code).map(|()| None),
        }
    }
}

fn ended(code: u8) -> Result<(), CallError> {
    match code {
        ADMITTED => Ok(()),
        NO_REPLY => Err(CallError::NoReply),
        code => Err(CallError::Rejected(Rejection::from_code(code))),
    }
}

fn end(id: u64, code: u8) -> Frame {
    static CODES: [u8; 8] = [0, 1, 2, 3, 4, 5, 6, 7];
    let code = (code as usize).min(7);
    Frame { kind: END, id, payload: Bytes::from_static(&CODES[code..code + 1]), ..Frame::default() }
}

enum Sink {
    /// `parts` for a reply in parts: the credit the caller has granted.
    Remote {
        link: Arc<Link>,
        id: u64,
        tokio: Handle,
        wait: Duration,
        parts: Option<Arc<Flow>>,
    },
    Local(Mutex<Option<oneshot::Sender<Outcome>>>),
    /// A reply in parts on this node: the reader's own flow.
    Flow(Arc<Flow>),
}

const PENDING: u8 = 0;
const ARMED: u8 = 1;
const DROPPED: u8 = 2;
const DONE: u8 = 3;

/// Which of the endpoint's return, the reply and its drop tells the caller: one of them, once. Each claims it with a
/// read-modify-write of this one atomic, and those are totally ordered even Relaxed; what is told synchronises in its
/// sink.
struct ReplyState(AtomicU8);

impl ReplyState {
    fn new() -> Self {
        Self(AtomicU8::new(PENDING))
    }

    /// The endpoint has returned: the END it owes the caller, if any. A reply dropped during a failed admission must
    /// surface as the rejection, not as `NoReply`, hence the states.
    fn settle(&self, admitted: Result<(), Rejection>) -> Option<u8> {
        match admitted {
            Ok(()) => match self.0.compare_exchange(PENDING, ARMED, Relaxed, Relaxed) {
                Err(DROPPED) => {
                    // DONE stays the only state after the caller is told, whoever calls next.
                    self.0.store(DONE, Relaxed);
                    Some(NO_REPLY)
                }
                _ => None,
            },
            Err(rejection) => (self.0.swap(DONE, Relaxed) != DONE).then_some(rejection as u8),
        }
    }

    /// Whether the reply is what the caller is told.
    fn reply(&self) -> bool {
        self.0.swap(DONE, Relaxed) != DONE
    }

    /// Whether dropping the reply unanswered owes the caller `NoReply`; before the endpoint returns, `settle` decides.
    fn drop_reply(&self) -> bool {
        self.0.compare_exchange(PENDING, DROPPED, Relaxed, Relaxed).is_err()
            && self.0.compare_exchange(ARMED, DONE, Relaxed, Relaxed).is_ok()
    }
}

struct ReplyCore {
    state: ReplyState,
    compress: AtomicBool,
    sink: Sink,
    /// The caller's, when it sent one, on this node's clock.
    deadline: Option<Instant>,
    max_payload: usize,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    /// The body of a request in parts, until the handler takes it.
    body: Mutex<Option<Upload>>,
}

impl ReplyCore {
    fn new(
        sink: Sink,
        deadline: Option<Instant>,
        max_payload: usize,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Arc<Self> {
        let (state, compress, permit) = (ReplyState::new(), AtomicBool::new(false), Mutex::new(permit));
        Arc::new(Self { state, compress, sink, deadline, max_payload, permit, body: Mutex::default() })
    }

    fn flow(&self) -> Option<&Arc<Flow>> {
        match &self.sink {
            Sink::Remote { parts, .. } => parts.as_ref(),
            Sink::Flow(flow) => Some(flow),
            Sink::Local(_) => None,
        }
    }

    /// A reply in parts leaves the session's count, so the caller's next one finds room before it hears this end.
    fn unlist(&self) {
        if let Sink::Remote { link, id, parts: Some(_), .. } = &self.sink {
            link.producing.lock().remove(id);
        }
    }

    /// Frees the reply's slots and tells the caller nothing: it has gone.
    fn release(&self) {
        self.unlist();
        drop(self.permit.lock().take());
    }

    fn finish(&self, outcome: Outcome) {
        // A body the handler did not take is stopped: it answered without it.
        let body = self.body.lock().take();
        drop(body);
        // The slot is free before the answer can reach the caller, so the caller's next call finds it.
        let mut slot = self.permit.lock().take().map(|permit| permit.semaphore().clone());
        self.unlist();
        match &self.sink {
            Sink::Local(tx) => drop(tx.lock().take().map(|tx| tx.send(outcome))),
            Sink::Flow(flow) => match outcome {
                Outcome::Reply(part) => {
                    flow.push(part);
                    flow.end(Ok(()));
                }
                Outcome::End(code) => flow.end(ended(code)),
            },
            // A caller that stopped its reply in parts hears nothing more.
            Sink::Remote { parts: Some(flow), .. } if flow.stopped() => {}
            Sink::Remote { link, id, tokio, wait, parts } => {
                let compress = self.compress.load(Relaxed);
                let (kind, last) = if parts.is_some() { (PART, Some(end(*id, ADMITTED))) } else { (REPLY, None) };
                let frames = match outcome {
                    Outcome::Reply(payload) => {
                        [Some(Frame { kind, id: *id, payload, compress, ..Frame::default() }), last]
                    }
                    Outcome::End(code) => [Some(end(*id, code)), None],
                };
                let mut frames = frames.into_iter().flatten();
                while let Some(frame) = frames.next() {
                    let Err(e) = link.sender.try_submit(frame) else { continue };
                    // A full outbox costs this one reply a bounded wait, never the session. It waits holding a slot
                    // again, so what waits stays bounded; with none free it is dropped, and its caller times out.
                    if e.reason == SubmitReason::Full
                        && let Some(permit) = slot.take().and_then(|slot| slot.try_acquire_owned().ok())
                    {
                        let (sender, at) = (link.sender.clone(), Instant::now() + *wait);
                        let rest: Vec<_> = std::iter::once(e.frame).chain(frames).collect();
                        tokio.spawn(async move {
                            for frame in rest {
                                if sender.submit(frame, Some(at)).await.is_err() {
                                    break;
                                }
                            }
                            drop(permit);
                        });
                    }
                    break;
                }
            }
        }
    }

    /// Called once the endpoint has returned.
    fn settle(&self, admitted: Result<(), Rejection>) {
        if let Some(code) = self.state.settle(admitted) {
            self.finish(Outcome::End(code));
        }
    }
}

impl Drop for ReplyCore {
    /// A request that expired waiting for its actor is dropped unanswered.
    fn drop(&mut self) {
        self.unlist();
    }
}

/// Answers `NoReply` when dropped unanswered.
struct Answer(Arc<ReplyCore>);

impl Drop for Answer {
    fn drop(&mut self) {
        if self.0.state.drop_reply() {
            self.0.finish(Outcome::End(NO_REPLY));
        }
    }
}

/// The handle a request hands to its actor, taking the reply before it is encoded: the contract's `Reply` behind an
/// [`ActorProtocol`], `Bytes` on a raw endpoint. Dropping it unanswered tells the caller `NoReply`.
pub struct Reply<R = Bytes> {
    answer: Answer,
    encode: fn(R) -> Result<Bytes, CallError>,
}

impl Reply {
    pub(crate) fn typed<R>(self, encode: fn(R) -> Result<Bytes, CallError>) -> Reply<R> {
        Reply { answer: self.answer, encode }
    }
}

impl<R> Reply<R> {
    /// Largest body the caller's session accepts.
    pub fn max_payload(&self) -> usize {
        self.answer.0.max_payload
    }

    /// Whether the caller has stopped waiting: its deadline has passed, as this node reads it (on this node too), its
    /// session is gone, it stopped a reply in parts, or, on this node, it dropped the call. A handler that holds the
    /// reply across a job asks before the work, and drops the reply unanswered if so. Only
    /// [`request_within`](crate::RemoteRef::request_within) sends a deadline: a remote caller that sent none gave up at
    /// its own `request_timeout`, which this node does not see.
    pub fn is_abandoned(&self) -> bool {
        let core = &self.answer.0;
        core.deadline.is_some_and(|at| at <= Instant::now())
            || core.flow().is_some_and(|flow| flow.stopped())
            || match &core.sink {
                Sink::Local(tx) => tx.lock().as_ref().is_none_or(|tx| tx.is_closed()),
                Sink::Remote { link, .. } => !link.sender.is_active(),
                Sink::Flow(_) => false,
            }
    }

    /// What is left, as this node reads it, of the deadline a [`request_within`](crate::RemoteRef::request_within)
    /// sent, zero once it has passed; `None` without one, as after a plain `request`.
    pub fn time_left(&self) -> Option<Duration> {
        self.answer.0.deadline.map(|at| at.saturating_duration_since(Instant::now()))
    }

    /// Asks for the reply to be compressed even on a session without a codec.
    pub fn compress(&self) {
        self.answer.0.compress.store(true, Relaxed);
    }

    /// The body of an [`upload`](crate::RemoteRef::upload), the first time it is asked for; `None` for any other
    /// request. Answering without taking it, or dropping it, stops the body. The answer may go at any point, before the
    /// body ends too.
    pub fn upload(&self) -> Option<Upload> {
        self.answer.0.body.lock().take()
    }

    /// Never blocks. A reply that does not encode fails with the encoder's error, one over
    /// [`max_payload`](Self::max_payload) with `NotSent(Oversized)`, and either way the caller gets `NoReply`.
    /// A caller that has gone is not an error: the reply is dropped and this answers `Ok`. So an `Err` is about the
    /// reply itself, the caller has been told, and this `Reply` is spent: log it or ignore it, nothing else is owed.
    /// To a [`request_parts`](crate::RemoteRef::request_parts) it is one part.
    pub fn send(self, reply: R) -> Result<(), CallError> {
        let body = (self.encode)(reply)?;
        let core = &self.answer.0;
        if body.len() > core.max_payload {
            return Err(CallError::NotSent(SubmitReason::Oversized));
        }
        if core.state.reply() {
            core.finish(Outcome::Reply(body));
        }
        Ok(())
    }

    /// Answers a [`request_parts`](crate::RemoteRef::request_parts) with the parts `parts` yields, pulling the next
    /// only while the caller has credit for it: a caller holds at most 4 MiB and one part unread. The stream's end ends
    /// the reply. Any other ending cuts it, and the caller gets `NoReply` after the parts it has: an `Err` from the
    /// stream (returned), a part that does not encode (the encoder's error) or one over
    /// [`max_payload`](Self::max_payload) (`NotSent(Oversized)`), and the future dropped, polled or not, or a panic
    /// in it. `Ok` once the stream ended, and when the caller has gone: it stopped, its session closed or its deadline
    /// passed, which ends the future at once, even in the middle of a poll of the stream. Run it with
    /// `cx.submit_future`, so that a drain waits for it and a stop cuts it, or await it in a job. To a plain request it
    /// polls nothing: both the caller and the future get `Rejected(InvalidMessage)`.
    pub fn send_parts<S, E>(self, parts: S) -> impl Future<Output = Result<(), E>> + Send + 'static
    where
        S: axon::Stream<Item = Result<R, E>> + Send + 'static,
        E: From<CallError> + 'static,
        R: 'static,
    {
        parts::send(self, parts)
    }
}

/// A call waiting for its answer: one frame, or parts.
enum Waiter {
    One(oneshot::Sender<Outcome>),
    Parts(Arc<Flow>),
}

pub(crate) struct Link {
    pub sender: Sender,
    pub outgoing: bool,
    pending: Mutex<HashMap<u64, Waiter>>,
    /// The peer's requests answered in parts, by its call id: where its credit goes.
    producing: Mutex<BTreeMap<u64, Arc<Flow>>>,
    /// The bodies of the peer's requests in parts, by its call id, while their handlers read them.
    bodies: Mutex<BTreeMap<u64, Arc<Flow>>>,
    /// The bodies of this node's requests in parts, by call id: where the peer's credit goes.
    uploading: Mutex<BTreeMap<u64, Arc<Flow>>>,
    next_id: AtomicU64,
    calls: AtomicU64,
    rtt: AtomicHistogram,
    /// Endpoints the peer said are gone, by `id % 256`: dynamic ids are handed out in turn, and one pushed out by
    /// another only loses a tell once more before it is named again.
    gone: [AtomicU64; 256],
}

impl Link {
    fn new(sender: Sender, outgoing: bool) -> Arc<Self> {
        Arc::new(Self {
            sender,
            outgoing,
            pending: Mutex::default(),
            producing: Mutex::default(),
            bodies: Mutex::default(),
            uploading: Mutex::default(),
            next_id: AtomicU64::new(1),
            calls: AtomicU64::new(0),
            rtt: AtomicHistogram::default(),
            gone: std::array::from_fn(|_| AtomicU64::new(0)),
        })
    }

    fn is_gone(&self, endpoint: u64) -> bool {
        self.gone[endpoint as usize % 256].load(Relaxed) == endpoint
    }

    fn complete(&self, frame: Frame) {
        let Some(waiter) = self.pending.lock().remove(&frame.id) else {
            // Nobody waits on a tell: its END only ever names an endpoint that is gone. The system endpoints never are.
            if frame.kind == END
                && frame.payload[..] == [Rejection::UnknownActor as u8]
                && frame.protocol >= FIRST_DYNAMIC
            {
                self.gone[frame.protocol as usize % 256].store(frame.protocol, Relaxed);
            }
            return;
        };
        let outcome = match frame.kind {
            REPLY => Outcome::Reply(frame.payload),
            _ => Outcome::End(frame.payload.first().copied().unwrap_or(Rejection::InvalidMessage as u8)),
        };
        match (waiter, outcome) {
            (Waiter::One(tx), outcome) => drop(tx.send(outcome)),
            (Waiter::Parts(flow), Outcome::End(code)) => flow.end(ended(code)),
            // A reply in parts never ends in one frame.
            (Waiter::Parts(flow), Outcome::Reply(_)) => {
                flow.end(Err(CallError::OutcomeUnknown));
                self.sender.close();
            }
        }
    }

    /// A part past its reader's credit, or one to a call that is not in parts, closes the session. One to a call or a
    /// body that has ended is dropped: its reader stopped it.
    fn part(&self, frame: Frame) {
        let fits = if frame.kind & BODY != 0 {
            self.bodies.lock().get(&frame.id).is_none_or(|flow| flow.push(frame.payload))
        } else {
            match self.pending.lock().get(&frame.id) {
                Some(Waiter::Parts(flow)) => flow.push(frame.payload),
                Some(Waiter::One(_)) => false,
                None => true,
            }
        };
        if !fits {
            self.sender.close();
        }
    }

    /// The end of a body: one byte, `ADMITTED` or `NO_REPLY`.
    fn body_end(&self, frame: Frame) {
        let code = match frame.payload[..] {
            [code @ (ADMITTED | NO_REPLY)] => code,
            _ => return self.sender.close(),
        };
        if let Some(flow) = self.bodies.lock().get(&frame.id) {
            flow.end(ended(code));
        }
    }

    fn credit(&self, frame: Frame) {
        let Ok(credit) = <[u8; 4]>::try_from(&frame.payload[..]).map(u32::from_be_bytes) else {
            return self.sender.close();
        };
        let flows = if frame.kind & BODY != 0 { &self.uploading } else { &self.producing };
        let flow = flows.lock().get(&frame.id).cloned();
        match flow {
            Some(flow) if credit == 0 => _ = flow.stop(),
            Some(flow) => flow.grant(credit),
            None => {}
        }
    }
}

/// Where an admitted request's answer comes, and on another node its entry on the session, taken back when the wait
/// ends without the answer, with when it was sent where its round trip is sampled.
struct Admitted {
    rx: oneshot::Receiver<Outcome>,
    entry: Option<(Arc<Link>, u64, Option<Instant>)>,
}

impl Admitted {
    async fn wait(mut self, counters: &Counters, deadline: Instant) -> Result<Outcome, CallError> {
        let outcome = timeout_at(deadline, &mut self.rx).await.map_err(|_| CallError::Timeout)?;
        // Whatever ended the wait removed the entry, so it need not be locked to find it gone.
        if let Some((link, _, Some(at))) = self.entry.take() {
            let rtt = at.elapsed();
            link.rtt.record(rtt);
            counters.rtt.record(rtt);
        }
        outcome.map_err(|_| CallError::OutcomeUnknown)
    }
}

impl Drop for Admitted {
    fn drop(&mut self) {
        if let Some((link, id, _)) = self.entry.take() {
            link.pending.lock().remove(&id);
        }
    }
}

/// A node's address and incarnation.
type Boot = (SocketAddr, u128);

#[derive(Clone)]
enum LinkState {
    Connecting,
    /// With the boot it is to.
    Up(Arc<Link>, Boot),
    /// The boot the error concerns; `None` once the link's task has ended.
    Down(Error, Option<Boot>),
}

struct PeerSlot {
    state: watch::Sender<LinkState>,
    /// Wakes the link's task to dial the view's record now, dropping a dial or backoff for the old one.
    redial: Notify,
    /// A `Live` or `Bulk` slot's: the `Control` slot whose boot it dials.
    control: Option<Arc<PeerSlot>>,
    /// Since when a `Live` or `Bulk` lane has had no session, once a dial of it failed while `Control` was up.
    down_since: Mutex<Option<Instant>>,
}

impl PeerSlot {
    fn new(control: Option<Arc<PeerSlot>>) -> Arc<Self> {
        let state = watch::Sender::new(LinkState::Connecting);
        Arc::new(Self { state, redial: Notify::new(), control, down_since: Mutex::default() })
    }
}

/// A peer's slot on each lane, indexed by `Lane as usize`.
type PeerLanes = [Option<Arc<PeerSlot>>; 3];
type PeerLane = (Arc<str>, Lane);
/// A peer's newest session on a lane, and the one it replaced.
type Accepted = (Arc<Link>, Option<Arc<Link>>);

/// One call, counted and traced from `begin` to `end`. Dropped before its end, it is counted abandoned.
struct Counted<E: Deref<Target = Engine>> {
    engine: E,
    span: Option<(Span, Instant)>,
    ended: bool,
}

impl<E: Deref<Target = Engine>> Counted<E> {
    /// The call, its deadline, and the trace context it carries.
    fn begin(
        engine: E,
        target: &Target,
        name: &'static str,
        within: Option<Duration>,
    ) -> Result<(Self, Instant, Option<TraceContext>), CallError> {
        if engine.is_closed() {
            return Err(CallError::Closed);
        }
        engine.counters.calls.fetch_add(1, Relaxed);
        let now = Instant::now();
        let deadline = now + within.map_or(engine.cfg.request_timeout, |d| d.min(LONGEST));
        let parent = TraceContext::current();
        let span = parent.map(|parent| {
            let mut span = Span::begin(parent, parent.child().span, SpanKind::Call, name);
            span.peer = Some(target.node.clone());
            (span, now)
        });
        let here = span.as_ref().map(|(span, _)| TraceContext { span: span.span, ..parent.expect("traced") });
        Ok((Self { engine, span, ended: false }, deadline, here))
    }

    fn end<T>(mut self, result: Result<T, CallError>) -> Result<T, CallError> {
        if let Some((mut span, started)) = self.span.take() {
            span.busy = started.elapsed();
            span.failed = result.is_err();
            self.engine.cfg.tracer.record(span);
        }
        let c = &self.engine.counters;
        let counter = match &result {
            Ok(_) => &c.succeeded,
            Err(CallError::Rejected(rejection)) => {
                if *rejection == Rejection::UnknownActor {
                    self.engine.stale_routes.fetch_add(1, Relaxed);
                }
                &c.rejected
            }
            Err(CallError::Timeout) => &c.timed_out,
            Err(CallError::OutcomeUnknown) => &c.unknown,
            Err(_) => &c.failed,
        };
        counter.fetch_add(1, Relaxed);
        self.ended = true;
        result
    }
}

impl<E: Deref<Target = Engine>> Drop for Counted<E> {
    fn drop(&mut self) {
        if !self.ended {
            self.engine.counters.abandoned.fetch_add(1, Relaxed);
        }
    }
}

#[derive(Default)]
struct Counters {
    calls: AtomicU64,
    succeeded: AtomicU64,
    rejected: AtomicU64,
    timed_out: AtomicU64,
    unknown: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
    inbound: AtomicU64,
    inbound_rejected: [AtomicU64; 6],
    inbound_expired: AtomicU64,
    connects: AtomicU64,
    dial_failures: AtomicU64,
    rtt: AtomicHistogram,
}

impl Counters {
    fn refuse(&self, rejection: Rejection) {
        self.inbound_rejected[rejection as usize - 2].fetch_add(1, Relaxed);
    }
}

pub const REJECTION_NAMES: [&str; 6] =
    ["unknown_actor", "schema_mismatch", "full", "forbidden", "invalid_message", "stopped"];

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct PeerStats {
    pub node: String,
    #[cfg_attr(feature = "serde", serde(serialize_with = "axon::ser::hex"))]
    pub incarnation: u128,
    pub outgoing: bool,
    pub pending: usize,
    pub calls: u64,
    /// Sampled request round trips on this link.
    pub request_rtt: HistogramSnapshot,
    pub session: SessionStats,
    /// The wire version the session runs.
    pub version: u16,
    pub lane: Lane,
    /// How long an outgoing `Live` or `Bulk` lane has had no session while `Control` is up, once its dial failed: a
    /// row with no session of its own. `None` for a session.
    pub down_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct EngineStats {
    pub calls: u64,
    pub succeeded: u64,
    pub rejected: u64,
    pub timed_out: u64,
    pub unknown_outcomes: u64,
    pub failed: u64,
    /// Calls their caller dropped before they ended, as a `tokio::time::timeout` around one does. Every call counts
    /// once in these six.
    pub abandoned: u64,
    pub inbound_calls: u64,
    /// Indexed like `REJECTION_NAMES`.
    pub inbound_rejected: [u64; 6],
    /// Requests dropped unrun and unanswered because their caller's deadline passed before they came in, or while they
    /// waited for their actor's next generation.
    pub inbound_expired: u64,
    pub request_rtt: HistogramSnapshot,
    pub connects: u64,
    pub dial_failures: u64,
    pub pending: usize,
    pub endpoints: usize,
    pub listener: axon_net::ListenerStats,
    pub peers: Vec<PeerStats>,
    /// The lowest and highest wire version this node speaks.
    pub versions: (u16, u16),
}

pub(crate) struct EngineConfig {
    pub transport: axon_net::Config,
    pub backoff: Backoff,
    pub request_timeout: Duration,
    pub poll_interval: Duration,
    pub idle_link_timeout: Duration,
    pub max_peers: usize,
    pub max_pending: usize,
    pub max_inbound_replies: usize,
    pub max_endpoints: usize,
    pub tracer: Arc<Tracer>,
    pub dead: Arc<DeadLetters>,
    /// Where a lane lost and got back while `Control` stays up is told: `lane_down`, `lane_up`.
    pub events: Arc<axon::EventLog>,
}

pub(crate) struct Engine {
    pub cfg: EngineConfig,
    pub local: Peer,
    pub membership: Arc<Membership>,
    /// Cleared when leaving: the directory stops answering and nothing new is published.
    pub open: AtomicBool,
    /// Bumped whenever a peer reports an unknown endpoint, so cached routes can be dropped.
    pub stale_routes: AtomicU64,
    registry: RwLock<Registry>,
    next_endpoint: AtomicU64,
    peers: Mutex<HashMap<Arc<str>, PeerLanes>>,
    /// At most two sessions a peer and lane, the newest and the one it replaced, which may still owe replies.
    incoming: Mutex<BTreeMap<PeerLane, Accepted>>,
    pending: Arc<Semaphore>,
    replies: Arc<Semaphore>,
    closed: watch::Sender<bool>,
    accepting: Mutex<Option<tokio::task::JoinHandle<()>>>,
    listener: ListenerStatsReader,
    /// What the listener serves, and what dials present, both swapped by `set_tls`.
    tls: Option<(ServerTls, Mutex<Arc<ClientConfig>>)>,
    pub tokio: Handle,
    counters: Counters,
}

impl Engine {
    pub async fn start(
        bind: SocketAddr,
        accept: AcceptLimits,
        cfg: EngineConfig,
        tls: Option<Tls>,
        authorizer: Option<Arc<dyn Authorizer>>,
        membership: Arc<Membership>,
    ) -> Result<(Arc<Self>, SocketAddr), Error> {
        let server = tls.as_ref().map(|tls| tls.server.clone());
        let listener = Listener::bind(bind, server, cfg.transport.clone(), accept).await?;
        let tls = listener.tls().zip(tls.map(|tls| Mutex::new(tls.client)));
        let address = listener.local_addr();
        let local = Peer {
            identity: cfg.transport.identity.clone(),
            remote: None,
            secure: true,
            compression: axon_net::Compression::None,
            max_frame: usize::MAX,
            version: *cfg.transport.versions.end(),
            lane: Lane::Control,
        };
        let engine = Arc::new(Self {
            local,
            membership,
            open: AtomicBool::new(true),
            stale_routes: AtomicU64::new(0),
            registry: RwLock::new(Registry { authorizer, ..Registry::default() }),
            next_endpoint: AtomicU64::new(FIRST_DYNAMIC),
            peers: Mutex::default(),
            incoming: Mutex::default(),
            pending: Arc::new(Semaphore::new(cfg.max_pending)),
            replies: Arc::new(Semaphore::new(cfg.max_inbound_replies)),
            closed: watch::Sender::new(false),
            accepting: Mutex::default(),
            listener: listener.stats_reader(),
            tls,
            tokio: Handle::current(),
            counters: Counters::default(),
            cfg,
        });
        *engine.accepting.lock() = Some(tokio::spawn(engine.clone().accept(listener)));
        Ok((engine, address))
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Flushes what was admitted on every session, then closes them. Peer tasks exit.
    pub async fn shutdown(&self, deadline: Instant) {
        self.closed.send_replace(true);
        let mut flushing = tokio::task::JoinSet::new();
        for link in self.links(&self.peer_lanes(), true) {
            flushing.spawn(async move { drop(link.sender.shutdown(deadline).await) });
        }
        flushing.join_all().await;
        self.released().await;
    }

    /// Once closed: returns when the listening port is free to bind again.
    pub async fn released(&self) {
        let accepting = self.accepting.lock().take();
        if let Some(accepting) = accepting {
            let _ = accepting.await;
        }
    }

    pub fn close(&self) {
        self.closed.send_replace(true);
        self.membership.close();
        self.links(&self.peer_lanes(), true).iter().for_each(|link| link.sender.close());
    }

    /// Every peer's slots, by peer.
    fn peer_lanes(&self) -> Vec<(Arc<str>, PeerLanes)> {
        #[expect(clippy::disallowed_methods, reason = "the hash order never shows: sorted by peer before use")]
        let mut peers: Vec<_> = self.peers.lock().iter().map(|(peer, lanes)| (peer.clone(), lanes.clone())).collect();
        peers.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        peers
    }

    /// Every session that is up: the accepted ones, with those they replaced when `replaced`, then the dialled ones of
    /// `peers`, each by peer and lane.
    fn links(&self, peers: &[(Arc<str>, PeerLanes)], replaced: bool) -> Vec<Arc<Link>> {
        let dialled = peers.iter().flat_map(|(_, lanes)| lanes.iter().flatten());
        let dialled = dialled.filter_map(|slot| match &*slot.state.borrow() {
            LinkState::Up(link, _) => Some(link.clone()),
            _ => None,
        });
        let incoming = self.incoming.lock();
        let accepted = incoming
            .values()
            .flat_map(|(newest, before)| std::iter::once(newest).chain(before.as_ref().filter(|_| replaced)));
        accepted.cloned().chain(dialled).collect()
    }

    // ---- registry ----

    /// `fixed` pins a well-known id; otherwise ids are allocated and never reused, so a stale
    /// reference can only ever miss.
    pub fn register(&self, fixed: Option<u64>, published: bool, entry: Entry) -> Result<u64, crate::ClusterError> {
        let mut registry = self.registry.write();
        if !self.open.load(Relaxed) {
            return Err(crate::ClusterError::Closed);
        }
        if registry.by_id.len() >= self.cfg.max_endpoints {
            return Err(crate::ClusterError::Config("endpoint limit reached"));
        }
        // A name takes one protocol, at one endpoint per schema.
        let mut served = registry.by_name.get(&entry.name).into_iter().flatten().map(|id| &registry.by_id[id]);
        if published && served.any(|e| e.protocol != entry.protocol || e.schema == entry.schema) {
            return Err(crate::ClusterError::NameTaken);
        }
        let id = fixed.unwrap_or_else(|| self.next_endpoint.fetch_add(1, Relaxed));
        if published {
            registry.by_name.entry(entry.name.clone()).or_default().push(id);
        }
        registry.by_id.insert(id, entry);
        Ok(id)
    }

    /// A fence: once this returns, no admission through `id` is running or can start.
    pub fn unregister(&self, id: u64) {
        let mut registry = self.registry.write();
        if let Some(entry) = registry.by_id.remove(&id)
            && let Some(ids) = registry.by_name.get_mut(&entry.name)
        {
            ids.retain(|i| *i != id);
            if ids.is_empty() {
                registry.by_name.remove(&entry.name);
            }
        }
    }

    pub fn set_authorizer(&self, authorizer: Option<Arc<dyn Authorizer>>) {
        let old = std::mem::replace(&mut self.registry.write().authorizer, authorizer);
        drop(old);
    }

    /// For spawns and watches, which are off the call path: out of the lock, so the authorizer holds nothing up.
    pub fn authorize(&self, peer: &Peer, action: Action<'_>) -> bool {
        let authorizer = self.registry.read().authorizer.clone();
        authorizer.is_none_or(|authorizer| authorizer.authorize(peer, action))
    }

    /// `1 | route`, or `0`.
    fn lookup(&self, name: &[u8]) -> Bytes {
        let registry = self.registry.read();
        let found = std::str::from_utf8(name).ok().filter(|_| self.open.load(Relaxed));
        let Some(ids) = found.and_then(|n| registry.by_name.get(n)) else {
            return Bytes::from_static(&[0]);
        };
        let mut out = BytesMut::with_capacity(9 + 11 * ids.len());
        out.put_u8(1);
        let endpoints = ids.iter().map(|id| (*id, registry.by_id[id].schema, registry.by_id[id].lane));
        crate::cluster::Route::put(&mut out, registry.by_id[&ids[0]].protocol, endpoints);
        out.freeze()
    }

    fn dispatch(
        &self,
        kind: u16,
        peer: &Peer,
        endpoint: u64,
        schema: u16,
        body: Bytes,
        reply: Option<&Arc<ReplyCore>>,
    ) -> Result<Option<Waiting>, Rejection> {
        if endpoint == DIRECTORY {
            let reply = reply.ok_or(Rejection::InvalidMessage)?;
            reply.state.reply();
            reply.finish(Outcome::Reply(self.lookup(&body)));
            return Ok(None);
        }
        // Run outside the lock: a handler may drop the last owner of an endpoint, which unregisters it.
        let admit = {
            let registry = self.registry.read();
            let entry = registry.by_id.get(&endpoint).ok_or(Rejection::UnknownActor)?;
            if entry.schema != schema {
                return Err(Rejection::SchemaMismatch);
            }
            if let Some(authorizer) = &registry.authorizer
                && !authorizer.authorize(peer, Action::Call { name: &entry.name })
            {
                return Err(Rejection::Forbidden);
            }
            entry.admit.clone()
        };
        let reply = reply.map(|core| Reply { answer: Answer(core.clone()), encode: Ok });
        match catch_unwind(AssertUnwindSafe(|| admit(peer, body, reply))).unwrap_or(Err(Rejection::InvalidMessage)) {
            // A tell does not wait: its sender hears nothing either way.
            Ok(Some(_)) if kind == TELL => Err(Rejection::Stopped),
            admitted => admitted,
        }
    }

    // ---- inbound ----

    fn inbound(self: &Arc<Self>, link: &Arc<Link>, frame: Frame) {
        self.counters.inbound.fetch_add(1, Relaxed);
        let (kind, id, peer) = (frame.kind & !(TRACED | DEADLINE | PARTS | BODY), frame.id, link.sender.peer());
        let mut payload = frame.payload;
        let trace = (frame.kind & TRACED != 0 && payload.len() >= TraceContext::WIRE_LEN)
            .then(|| TraceContext::from_bytes(payload.split_to(TraceContext::WIRE_LEN)[..].try_into().ok()?))
            .flatten();
        let mut malformed = frame.kind & TRACED != 0 && trace.is_none();
        let left = match (frame.kind & DEADLINE != 0 && !malformed).then(|| payload.try_get_u64()) {
            Some(Ok(us)) => Some(Duration::from_micros(us)),
            Some(Err(_)) => {
                malformed = true;
                None
            }
            None => None,
        };
        let parts = frame.kind & PARTS != 0;
        let window = match (parts && !malformed).then(|| payload.try_get_u32()) {
            Some(Ok(window)) => window,
            Some(Err(_)) => return link.sender.close(),
            None => 0,
        };
        // What the caller had left when it sent the frame, less the way here: half the session's recent round trip.
        let deadline = match left.map(|left| left.checked_sub(link.sender.min_rtt() / 2)) {
            Some(Some(left)) if !left.is_zero() => Instant::now().checked_add(left),
            Some(_) => {
                self.counters.inbound_expired.fetch_add(1, Relaxed);
                return;
            }
            None => None,
        };
        let core = match kind {
            REQUEST => {
                let refuse = || {
                    self.counters.refuse(Rejection::Full);
                    drop(link.sender.try_submit(end(id, Rejection::Full as u8)));
                };
                let Ok(permit) = self.replies.clone().try_acquire_owned() else { return refuse() };
                let flow = parts.then(|| Flow::new(window));
                let body = (frame.kind & BODY != 0).then(|| Flow::new(parts::WINDOW));
                {
                    let (mut producing, mut bodies) = (link.producing.lock(), link.bodies.lock());
                    let full = |list: &BTreeMap<u64, _>, flow: &Option<_>| flow.is_some() && list.len() >= parts::MOST;
                    let taken = |list: &BTreeMap<u64, _>, flow: &Option<_>| flow.is_some() && list.contains_key(&id);
                    if full(&producing, &flow) || full(&bodies, &body) {
                        drop((producing, bodies));
                        return refuse();
                    }
                    if taken(&producing, &flow) || taken(&bodies, &body) {
                        // The peer reused the id of a reply or a body in parts still under way.
                        drop((producing, bodies));
                        return link.sender.close();
                    }
                    producing.extend(flow.iter().map(|flow| (id, flow.clone())));
                    bodies.extend(body.iter().map(|body| (id, body.clone())));
                }
                let wait = self.cfg.transport.write_timeout;
                let sink = Sink::Remote { link: link.clone(), id, tokio: self.tokio.clone(), wait, parts: flow };
                let core = ReplyCore::new(sink, deadline, peer.max_frame, Some(permit));
                if let Some(body) = body {
                    let grants = Grants { link: link.clone(), id, tokio: self.tokio.clone(), wait, body: true };
                    *core.body.lock() = Some(Upload::new(body, Some(grants), self.cfg.request_timeout));
                }
                Some(core)
            }
            _ => None,
        };
        let admitted = if malformed {
            Err(Rejection::InvalidMessage)
        } else {
            enter(trace, || self.dispatch(kind, peer, frame.protocol, frame.schema, payload, core.as_ref()))
        };
        let admitted = match admitted {
            Ok(Some(wait)) => return self.wait(&link.sender, id, core, deadline, Scoped::new(trace, wait)),
            admitted => admitted.map(drop),
        };
        match kind {
            TELL => {
                // A refused tell is lost. The sender hears only that an endpoint is gone, so its next call there fails
                // and a keyed reference asks for the key again.
                if let Err(rejection) = admitted {
                    if rejection == Rejection::UnknownActor && frame.protocol >= FIRST_DYNAMIC {
                        let verdict = Frame { protocol: frame.protocol, ..end(id, rejection as u8) };
                        drop(link.sender.try_submit(verdict));
                    }
                    self.counters.refuse(rejection);
                    let target = self.registry.read().by_id.get(&frame.protocol).map(|e| e.name.clone());
                    let letter = DeadLetter {
                        seq: 0,
                        at_us: unix_micros(),
                        reason: REJECTION_NAMES[rejection as usize - 2],
                        label: "tell",
                        actor: None,
                        target: target.or_else(|| Some(format!("endpoint {:x}", frame.protocol).into())),
                        peer: Some(peer.identity.node_id.as_str().into()),
                        trace: 0,
                        count: 1,
                    };
                    self.cfg.dead.record(letter, trace);
                }
            }
            _ => self.answer(&link.sender, id, core.as_deref(), admitted),
        }
    }

    /// The verdict on a send or a request. If the outbox is full a send's is dropped and the caller times out.
    fn answer(&self, sender: &Sender, id: u64, core: Option<&ReplyCore>, admitted: Result<(), Rejection>) {
        if let Err(rejection) = admitted {
            self.counters.refuse(rejection);
        }
        match core {
            Some(core) => core.settle(admitted),
            None => drop(sender.try_submit(end(id, admitted.map_or_else(|r| r as u8, |()| ADMITTED)))),
        }
    }

    /// Answers once `wait` has. A send holds a reply permit while it waits, as a request holds its own, so what waits
    /// is bounded by `max_inbound_replies`. A request still waiting at its caller's deadline is dropped unrun and
    /// unanswered, as if it had come in past it.
    fn wait(
        self: &Arc<Self>,
        sender: &Sender,
        id: u64,
        core: Option<Arc<ReplyCore>>,
        deadline: Option<Instant>,
        wait: Scoped<Waiting>,
    ) {
        let permit = match core {
            Some(_) => None,
            None => match self.replies.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => return self.answer(sender, id, None, Err(Rejection::Full)),
            },
        };
        let (engine, sender) = (self.clone(), sender.clone());
        self.tokio.spawn(async move {
            let admitted = match deadline {
                Some(at) => match timeout_at(at, wait).await {
                    Ok(admitted) => admitted,
                    Err(_) => {
                        // Its slot is free before it is counted, so a reader of the count finds it.
                        drop((core, permit));
                        engine.counters.inbound_expired.fetch_add(1, Relaxed);
                        return;
                    }
                },
                None => wait.await,
            };
            // A send's slot is free before its answer can reach the caller; a request's frees in `finish`.
            drop(permit);
            engine.answer(&sender, id, core.as_deref(), admitted);
        });
    }

    async fn serve(self: &Arc<Self>, link: &Arc<Link>, mut receiver: Receiver) -> Error {
        let error = loop {
            let frame = match receiver.recv().await {
                Ok(frame) => frame.into_frame(),
                Err(error) => break error,
            };
            let flags = frame.kind & (TRACED | DEADLINE | PARTS | BODY);
            match frame.kind & !flags {
                REQUEST => self.inbound(link, frame),
                TELL | SEND if flags & (PARTS | BODY) == 0 => self.inbound(link, frame),
                PART if flags == BODY => link.part(frame),
                END if flags == BODY => link.body_end(frame),
                CREDIT if flags == BODY => link.credit(frame),
                _ if flags != 0 => link.sender.close(),
                REPLY | END => link.complete(frame),
                PART => link.part(frame),
                CREDIT => link.credit(frame),
                _ => link.sender.close(),
            }
        };
        // Callers hear in call order, not in hash order, which differs per process.
        #[expect(
            clippy::disallowed_methods,
            reason = "the hash order never shows: sorted by call id before any is dropped"
        )]
        let mut pending: Vec<_> = link.pending.lock().drain().collect();
        pending.sort_unstable_by_key(|(id, _)| *id);
        for (_, waiter) in pending {
            // A dropped sender tells its caller the same.
            if let Waiter::Parts(flow) = waiter {
                flow.end(Err(CallError::OutcomeUnknown));
            }
        }
        let bodies = std::mem::take(&mut *link.bodies.lock());
        for flow in bodies.values() {
            flow.end(Err(CallError::OutcomeUnknown));
        }
        let producing = std::mem::take(&mut *link.producing.lock());
        let uploading = std::mem::take(&mut *link.uploading.lock());
        for flow in producing.values().chain(uploading.values()) {
            flow.stop();
        }
        error
    }

    async fn accept(self: Arc<Self>, mut listener: Listener) {
        let mut closed = self.closed.subscribe();
        loop {
            let session = tokio::select! {
                session = listener.accept() => session,
                _ = closed.wait_for(|c| *c) => None,
            };
            let Some(session) = session else { return listener.close().await };
            let link = Link::new(session.sender.clone(), false);
            let (identity, lane) = (&session.peer().identity, session.peer().lane);
            let key = (identity.node_id.as_str().into(), lane);
            // A peer dials a lane again while the session it let go idle still carries replies to its calls: that one
            // stays, until its peer closes it or a third comes. Sessions of another boot are gone.
            let closing = match self.incoming.lock().entry(key.clone()) {
                btree_map::Entry::Vacant(entry) => {
                    entry.insert((link.clone(), None));
                    [None, None]
                }
                btree_map::Entry::Occupied(mut entry) => {
                    let (newest, before) = entry.get_mut();
                    let replaced = std::mem::replace(newest, link.clone());
                    match replaced.sender.peer().identity.incarnation == identity.incarnation {
                        true => [before.replace(replaced), None],
                        false => [Some(replaced), before.take()],
                    }
                }
            };
            closing.into_iter().flatten().for_each(|link| link.sender.close());
            let engine = self.clone();
            tokio::spawn(async move {
                engine.serve(&link, session.receiver).await;
                let mut incoming = engine.incoming.lock();
                let Some((newest, before)) = incoming.get_mut(&key) else { return };
                if before.as_ref().is_some_and(|held| Arc::ptr_eq(held, &link)) {
                    *before = None;
                } else if Arc::ptr_eq(newest, &link) {
                    match before.take() {
                        Some(before) => *newest = before,
                        None => drop(incoming.remove(&key)),
                    }
                }
            });
        }
    }

    // ---- outbound ----

    /// `record` is this node's own: the check runs under the name peers dial it by.
    pub fn set_tls(&self, tls: Tls, record: &NodeRecord) -> Result<(), &'static str> {
        let Some((server, client)) = &self.tls else { return Err("this node started without TLS") };
        tls.check(&self.cfg.transport, dial_name(&record.id, record.address))?;
        // Held across both halves, so two calls at once cannot leave halves that were never checked together.
        let mut client = client.lock();
        server.set(tls.server);
        *client = tls.client;
        Ok(())
    }

    async fn maintain(self: Arc<Self>, node: Arc<str>, slot: Arc<PeerSlot>) {
        let mut closed = self.closed.subscribe();
        let mut dial: Option<(NodeRecord, Redial)> = None;
        // The last session ended because the peer went quiet: a redial that hangs is not waited on for its handshake.
        let mut silent = None;
        while !*closed.borrow() {
            let Some(record) = self.membership.view().get(&node).map(|m| m.record.clone()) else { break };
            if dial.as_ref().is_none_or(|(r, _)| r.boot() != record.boot()) {
                let cfg = self.cfg.transport.clone();
                dial = Some((
                    record.clone(),
                    Redial::new(record.address, Lane::Control, None, cfg, self.cfg.backoff.clone()),
                ));
            }
            // Each attempt presents the certificates of the moment: `set_tls` may have swapped them.
            let tls = (self.tls.as_ref()).map(|(_, client)| ClientTls {
                name: dial_name(&record.id, record.address),
                config: client.lock().clone(),
            });
            dial.as_mut().expect("dial set").1.tls = tls;
            let quiet = silent.take();
            let give_up = async {
                if let Some(error) = quiet {
                    tokio::time::sleep(self.cfg.transport.connect_timeout).await;
                    self.membership.set_reachable(&node, record.incarnation, false);
                    slot.state.send_replace(LinkState::Down(error, Some(record.boot())));
                }
                std::future::pending::<std::convert::Infallible>().await
            };
            let attempt = tokio::select! {
                attempt = dial.as_mut().expect("dial set").1.connect() => attempt,
                never = give_up => match never {},
                _ = slot.redial.notified() => continue,
                _ = closed.wait_for(|c| *c) => break,
            };
            // The dial can end in the same poll as reset_peer's wake, for a record the view has since replaced: it is
            // judged by the record now, so its outcome neither turns away the node the view names nor fails the calls
            // waiting for a dial of it.
            let dialled = record.boot();
            let record = self.membership.view().get(&node).map_or(record, |m| m.record.clone());
            let named = |peer: &axon_net::Identity| {
                *peer.node_id == *node && (record.incarnation == 0 || peer.incarnation == record.incarnation)
            };
            let pause = match attempt {
                Ok(session) if named(&session.peer().identity) => {
                    self.counters.connects.fetch_add(1, Relaxed);
                    let link = Link::new(session.sender.clone(), true);
                    // The boot the handshake answered for, at the address dialled.
                    slot.state.send_replace(LinkState::Up(link.clone(), (dialled.0, record.incarnation)));
                    self.membership.set_reachable(&node, record.incarnation, true);
                    if *closed.borrow() {
                        link.sender.close();
                    }
                    let serve = std::pin::pin!(self.serve(&link, session.receiver));
                    let served =
                        self.served_until_idle(&node, Lane::Control, &slot, &link, serve, std::future::pending());
                    let Some(error) = served.await else { return };
                    silent = Some(error).filter(|e| matches!(e, Error::Timeout(Timeout::Idle | Timeout::Write)));
                    slot.state.send_replace(LinkState::Connecting);
                    Duration::ZERO
                }
                _ if record.boot() != dialled => {
                    if matches!(attempt, Err(DialError::Failed(_))) {
                        self.counters.dial_failures.fetch_add(1, Relaxed);
                    }
                    continue;
                }
                Ok(_) => {
                    // Discovery is behind the process at that address; wait for it to catch up.
                    slot.state.send_replace(LinkState::Down(Error::Auth, Some(dialled)));
                    self.cfg.poll_interval
                }
                Err(DialError::Backoff { retry_in }) => retry_in,
                Err(DialError::Failed(error)) => {
                    self.counters.dial_failures.fetch_add(1, Relaxed);
                    // Before the slot says so: a caller that saw the dial fail finds the view already without it.
                    self.membership.set_reachable(&node, record.incarnation, false);
                    slot.state.send_replace(LinkState::Down(error, Some(dialled)));
                    Duration::ZERO
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(pause) => {}
                _ = slot.redial.notified() => {}
                _ = closed.wait_for(|c| *c) => break,
            }
        }
        self.forget(&node, Lane::Control, &slot);
        slot.state.send_replace(LinkState::Down(Error::Closed, None));
    }

    /// A `Live` or `Bulk` lane of `node`: dialled while `control` is up, to the boot it is up to, and closed when it
    /// goes down. Its dial failing fails only its calls; the node stays reachable.
    async fn maintain_lane(self: Arc<Self>, node: Arc<str>, lane: Lane, slot: Arc<PeerSlot>, control: Arc<PeerSlot>) {
        let (mut closed, mut on) = (self.closed.subscribe(), control.state.subscribe());
        let mut dial: Option<(Boot, Redial)> = None;
        // When the last session ended, and whether the slot only says what `Control` does.
        let (mut lost, mut mirrored) = (None, false);
        while !*closed.borrow() {
            let control_state = on.borrow_and_update().clone();
            let up = match control_state {
                LinkState::Up(link, boot) => Some((boot, link.sender.peer().identity.incarnation)),
                LinkState::Down(_, None) => break,
                down @ LinkState::Down(..) => {
                    slot.state.send_replace(down);
                    // The lane lost its session with `Control`'s: not the lane's own failure.
                    (lost, mirrored) = (None, true);
                    None
                }
                LinkState::Connecting => None,
            };
            let Some((boot, incarnation)) = up else {
                tokio::select! {
                    _ = on.changed() => continue,
                    _ = closed.wait_for(|c| *c) => break,
                }
            };
            if std::mem::take(&mut mirrored) {
                slot.state.send_replace(LinkState::Connecting);
            }
            if dial.as_ref().is_none_or(|(b, _)| *b != boot) {
                let (cfg, backoff) = (self.cfg.transport.clone(), self.cfg.backoff.clone());
                dial = Some((boot, Redial::new(boot.0, lane, None, cfg, backoff)));
            }
            let redial = &mut dial.as_mut().expect("dial set").1;
            redial.tls = (self.tls.as_ref())
                .map(|(_, client)| ClientTls { name: dial_name(&node, boot.0), config: client.lock().clone() });
            let attempt = tokio::select! {
                attempt = redial.connect() => attempt,
                _ = on.changed() => continue,
                _ = slot.redial.notified() => continue,
                _ = closed.wait_for(|c| *c) => break,
            };
            let pause = match attempt {
                Ok(session)
                    if *session.peer().identity.node_id == *node
                        && session.peer().identity.incarnation == incarnation =>
                {
                    self.counters.connects.fetch_add(1, Relaxed);
                    let link = Link::new(session.sender.clone(), true);
                    if slot.down_since.lock().take().is_some() {
                        self.cfg.events.record("lane_up", &*node, format!("{lane:?}"));
                    }
                    slot.state.send_replace(LinkState::Up(link.clone(), boot));
                    if *closed.borrow() {
                        link.sender.close();
                    }
                    let serve = std::pin::pin!(self.serve(&link, session.receiver));
                    let gone = on.wait_for(|s| match s {
                        LinkState::Connecting => false,
                        LinkState::Up(_, up) => *up != boot,
                        LinkState::Down(..) => true,
                    });
                    let served = self.served_until_idle(&node, lane, &slot, &link, serve, async { drop(gone.await) });
                    if served.await.is_none() {
                        return;
                    }
                    lost = Some(Instant::now());
                    slot.state.send_replace(LinkState::Connecting);
                    Duration::ZERO
                }
                Err(DialError::Backoff { retry_in }) => retry_in,
                failed => {
                    let (error, pause) = match failed {
                        Err(DialError::Failed(error)) => {
                            self.counters.dial_failures.fetch_add(1, Relaxed);
                            (error, Duration::ZERO)
                        }
                        // Another boot answers where `Control`'s is: `Control` hears of it too.
                        _ => (Error::Auth, self.cfg.poll_interval),
                    };
                    let up = matches!(&*on.borrow(), LinkState::Up(_, up) if *up == boot);
                    let mut since = slot.down_since.lock();
                    if up && since.is_none() {
                        *since = Some(lost.unwrap_or_else(Instant::now));
                        self.cfg.events.record("lane_down", &*node, format!("{lane:?}"));
                    }
                    // A lane down for as long as an idle one lasts is forgotten, so `Control` can idle; the next
                    // call dials it again.
                    let expired = since.is_some_and(|since| since.elapsed() >= self.cfg.idle_link_timeout);
                    drop(since);
                    slot.state.send_replace(LinkState::Down(error, Some(boot)));
                    if expired {
                        break;
                    }
                    pause
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(pause) => {}
                _ = slot.redial.notified() => {}
                _ = on.changed() => {}
                _ = closed.wait_for(|c| *c) => break,
            }
        }
        self.forget(&node, lane, &slot);
        slot.state.send_replace(LinkState::Down(Error::Closed, None));
    }

    fn forget(&self, node: &Arc<str>, lane: Lane, slot: &Arc<PeerSlot>) {
        let mut peers = self.peers.lock();
        let Some(lanes) = peers.get_mut(node) else { return };
        if lanes[lane as usize].as_ref().is_some_and(|held| Arc::ptr_eq(held, slot)) {
            lanes[lane as usize] = None;
            if lanes.iter().all(Option::is_none) {
                peers.remove(node);
            }
        }
    }

    /// Returns once `link` has carried no call, and waited for no reply, for `span`, and `busy` says no.
    async fn quiet(link: &Link, span: Duration, busy: impl Fn() -> bool) {
        let mut seen = link.calls.load(Relaxed);
        loop {
            tokio::time::sleep(span).await;
            let now = link.calls.load(Relaxed);
            if now == seen && link.pending.lock().is_empty() && !busy() {
                return;
            }
            seen = now;
        }
    }

    /// `None` when the link went idle and was closed for it, else what ended the session, or `gone` closed it. New
    /// calls dial afresh as soon as the slot is forgotten; a call that had already picked this link still gets its
    /// reply. `Control` is not idle while the node has another lane: it closes last.
    async fn served_until_idle(
        &self,
        node: &Arc<str>,
        lane: Lane,
        slot: &Arc<PeerSlot>,
        link: &Link,
        mut serve: std::pin::Pin<&mut impl Future<Output = Error>>,
        gone: impl Future<Output = ()>,
    ) -> Option<Error> {
        let others =
            || lane == Lane::Control && self.peers.lock().get(node).is_some_and(|l| l[1..].iter().any(Option::is_some));
        tokio::select! {
            error = &mut serve => return Some(error),
            () = gone => {
                link.sender.close();
                return Some(serve.await);
            }
            _ = Self::quiet(link, self.cfg.idle_link_timeout, others) => {}
        }
        self.forget(node, lane, slot);
        tokio::select! {
            _ = &mut serve => {}
            _ = Self::quiet(link, Duration::from_secs(1), || false) => {}
        }
        link.sender.close();
        slot.state.send_replace(LinkState::Down(Error::Closed, None));
        None
    }

    /// Ends the session to a node whose record changed under it; its task redials from the view. A dial or a backoff
    /// for the old record gives way to a dial of the new one at once, which calls wait for rather than fail with the
    /// old one's error. What is already about the boot the view names, which a call may have dialled first, is kept.
    pub fn reset_peer(&self, node: &str) {
        // Held until the state is set, so a task that ends by forgetting the slot then setting it `Down` comes after.
        let peers = self.peers.lock();
        let Some(lanes) = peers.get(node) else { return };
        let named = self.membership.view().get(node).map(|m| m.record.boot());
        for slot in lanes.iter().flatten() {
            let redial = slot.state.send_if_modified(|state| match state {
                LinkState::Up(link, boot) => {
                    if named != Some(*boot) {
                        link.sender.close();
                    }
                    false
                }
                LinkState::Down(_, boot) if *boot == named => false,
                _ => {
                    *state = LinkState::Connecting;
                    true
                }
            });
            if redial {
                slot.redial.notify_one();
            }
        }
    }

    /// The session to `node` on `lane`. A `Live` or `Bulk` one is dialled once `Control` is up, and while it cannot be
    /// had with `Control` up its calls fail `NotSent(Closed)`, `Retry::Later`.
    async fn link(self: &Arc<Self>, node: &Arc<str>, lane: Lane, deadline: Instant) -> Result<Arc<Link>, CallError> {
        if lane != Lane::Control {
            let peers = self.peers.lock();
            if let Some(lanes @ [Some(control), ..]) = peers.get(node)
                && let Some(slot) = &lanes[lane as usize]
            {
                // One watch lock at a time: the lane's state is copied out before `Control`'s is read.
                let up = match &*slot.state.borrow() {
                    LinkState::Up(link, boot) => Some((link.clone(), *boot)),
                    _ => None,
                };
                if let Some((link, boot)) = up
                    && matches!(&*control.state.borrow(), LinkState::Up(_, up) if *up == boot)
                {
                    return Ok(link);
                }
            }
        }
        loop {
            let (control, link, boot) = self.control(node, deadline).await?;
            if lane == Lane::Control {
                return Ok(link);
            }
            let slot = {
                let mut peers = self.peers.lock();
                let slot = &mut peers.entry(node.clone()).or_default()[lane as usize];
                match slot {
                    Some(slot) if slot.control.as_ref().is_some_and(|c| Arc::ptr_eq(c, &control)) => slot.clone(),
                    _ => {
                        let made = PeerSlot::new(Some(control.clone()));
                        *slot = Some(made.clone());
                        self.tokio.spawn(self.clone().maintain_lane(node.clone(), lane, made.clone(), control.clone()));
                        made
                    }
                }
            };
            let mut state = slot.state.subscribe();
            let settled = state.wait_for(|s| match s {
                LinkState::Up(_, b) | LinkState::Down(_, Some(b)) => *b == boot,
                LinkState::Down(_, None) => true,
                LinkState::Connecting => false,
            });
            let settled =
                timeout_at(deadline, settled).await.map_err(|_| CallError::NotSent(SubmitReason::Deadline))?;
            // Out of the lane's lock before `Control`'s is taken: no watch lock is held while another is.
            let settled = settled.map(|state| state.clone());
            let control = control.state.borrow().clone();
            match settled {
                Ok(LinkState::Up(link, _)) => return Ok(link),
                Ok(LinkState::Down(_, Some(_))) => match control {
                    LinkState::Down(error, _) => return Err(CallError::Unreachable(error)),
                    // `Control`'s old state mirrored before its lane task sees it up: not the lane's answer.
                    LinkState::Up(..) if slot.down_since.lock().is_none() => {
                        if timeout_at(deadline, state.changed()).await.is_err() {
                            return Err(CallError::NotSent(SubmitReason::Deadline));
                        }
                    }
                    _ => return Err(CallError::NotSent(SubmitReason::Closed)),
                },
                // The lane's task ended under the call: the next turn starts another.
                _ => {}
            }
        }
    }

    /// The `Control` slot of `node`, its session and the boot that is to.
    async fn control(
        self: &Arc<Self>,
        node: &Arc<str>,
        deadline: Instant,
    ) -> Result<(Arc<PeerSlot>, Arc<Link>, Boot), CallError> {
        let slot = {
            let mut peers = self.peers.lock();
            match peers.get(node).and_then(|lanes| lanes[Lane::Control as usize].clone()) {
                Some(slot) => slot,
                None if self.membership.view().get(node).is_none() => return Err(CallError::NoRoute),
                None if !peers.contains_key(node) && peers.len() >= self.cfg.max_peers => return Err(CallError::Busy),
                None => {
                    let slot = PeerSlot::new(None);
                    peers.entry(node.clone()).or_default()[Lane::Control as usize] = Some(slot.clone());
                    self.tokio.spawn(self.clone().maintain(node.clone(), slot.clone()));
                    slot
                }
            }
        };
        let up = match &*slot.state.borrow() {
            LinkState::Up(link, boot) => Some((link.clone(), *boot)),
            _ => None,
        };
        if let Some((link, boot)) = up {
            return Ok((slot, link, boot));
        }
        // The view can name a new boot before `reset_peer` has cleared the old one's error: that one is not this call's
        // answer, so it is cleared here and the new boot dialled, as `reset_peer` will.
        let named = self.membership.view().get(node).map(|m| m.record.boot());
        let stale = slot.state.send_if_modified(|state| {
            let stale = matches!(state, LinkState::Down(_, Some(boot)) if named.is_some_and(|named| named != *boot));
            if stale {
                *state = LinkState::Connecting;
            }
            stale
        });
        if stale {
            slot.redial.notify_one();
        }
        let mut state = slot.state.subscribe();
        let settled = timeout_at(deadline, state.wait_for(|s| !matches!(s, LinkState::Connecting))).await;
        let (link, boot) = match settled.map_err(|_| CallError::NotSent(SubmitReason::Deadline))?.as_deref() {
            Ok(LinkState::Up(link, boot)) => (link.clone(), *boot),
            Ok(LinkState::Down(error, _)) => return Err(CallError::Unreachable(error.clone())),
            _ => return Err(CallError::Closed),
        };
        Ok((slot, link, boot))
    }

    /// A call's admission on this node, waiting for a supervised actor's next generation until `deadline`.
    async fn admit_local(
        &self,
        target: &Target,
        kind: u16,
        body: Bytes,
        deadline: Instant,
        trace: Option<TraceContext>,
        core: Option<&Arc<ReplyCore>>,
    ) -> Result<Result<(), Rejection>, CallError> {
        if target.incarnation != self.local.identity.incarnation {
            return Err(CallError::StaleNode);
        }
        match enter(trace, || self.dispatch(kind, &self.local, target.endpoint, target.schema, body, core)) {
            Ok(Some(wait)) => timeout_at(deadline, Scoped::new(trace, wait))
                .await
                .map_err(|_| CallError::NotSent(SubmitReason::Deadline)),
            admitted => Ok(admitted.map(drop)),
        }
    }

    /// `told` is `deadline` when the caller gave it, for [`Reply::time_left`].
    async fn call_local(
        &self,
        target: &Target,
        kind: u16,
        body: Bytes,
        deadline: Instant,
        told: Option<Instant>,
        trace: Option<TraceContext>,
    ) -> Result<Outcome, CallError> {
        if kind != REQUEST {
            let admitted = self.admit_local(target, kind, body, deadline, trace, None).await?;
            return Ok(Outcome::End(admitted.map_or_else(|r| r as u8, |()| ADMITTED)));
        }
        self.start_local(target, body, deadline, told, trace).await?.wait(&self.counters, deadline).await
    }

    /// A request admitted on this node by `deadline`.
    async fn start_local(
        &self,
        target: &Target,
        body: Bytes,
        deadline: Instant,
        told: Option<Instant>,
        trace: Option<TraceContext>,
    ) -> Result<Admitted, CallError> {
        let (tx, rx) = oneshot::channel();
        let core = ReplyCore::new(Sink::Local(Mutex::new(Some(tx))), told, usize::MAX, None);
        core.settle(self.admit_local(target, REQUEST, body, deadline, trace, Some(&core)).await?);
        Ok(Admitted { rx, entry: None })
    }

    /// The session to `target` and the call's frame. `DEADLINE` in `kind` sends the time left before `deadline`, and
    /// `PARTS` the window.
    async fn prepare(
        self: &Arc<Self>,
        target: &Target,
        kind: u16,
        payload: Bytes,
        compress: bool,
        deadline: Instant,
        trace: Option<TraceContext>,
    ) -> Result<(Arc<Link>, Frame), CallError> {
        let link = self.link(&target.node, target.lane, deadline).await?;
        if link.sender.peer().identity.incarnation != target.incarnation {
            return Err(CallError::StaleNode);
        }
        if link.is_gone(target.endpoint) {
            return Err(CallError::Rejected(Rejection::UnknownActor));
        }
        // A body that only fits the frame without its trace context goes untraced rather than not at all. A deadline
        // and a window are never left out, so they take 8 and 4 bytes of the body's room.
        let left = (kind & DEADLINE != 0).then(|| deadline.saturating_duration_since(Instant::now()));
        let window = (kind & PARTS != 0).then_some(parts::WINDOW);
        let room = payload.len() + TraceContext::WIRE_LEN + left.map_or(0, |_| 8) + window.map_or(0, |_| 4);
        let trace = trace.filter(|_| room <= link.sender.peer().max_frame);
        let (kind, payload) = stamp(kind, trace, left, window, payload);
        link.calls.fetch_add(1, Relaxed);
        let id = link.next_id.fetch_add(1, Relaxed);
        let frame =
            Frame { kind, schema: target.schema, protocol: target.endpoint, id, payload, compress, ..Frame::default() };
        Ok((link, frame))
    }

    /// A frame the session to `target` refused. One closed under a call because its node left the view is `NoRoute`,
    /// and because the view names another boot of it `StaleNode`, so the caller looks again rather than retries a
    /// boot that is gone. A record of incarnation 0, as a static list can give, names no boot.
    fn not_sent(&self, target: &Target, reason: SubmitReason) -> CallError {
        if reason != SubmitReason::Closed {
            return CallError::NotSent(reason);
        }
        match self.membership.view().get(&target.node) {
            None => CallError::NoRoute,
            Some(m) if ![0, target.incarnation].contains(&m.record.incarnation) => CallError::StaleNode,
            Some(_) => CallError::NotSent(reason),
        }
    }

    async fn call_remote(
        self: &Arc<Self>,
        target: &Target,
        kind: u16,
        payload: Bytes,
        compress: bool,
        deadline: Instant,
        trace: Option<TraceContext>,
    ) -> Result<Outcome, CallError> {
        let _permit = self.pending.try_acquire().map_err(|_| CallError::Busy)?;
        if kind & !DEADLINE == TELL {
            let (link, frame) = self.prepare(target, kind, payload, compress, deadline, trace).await?;
            link.sender.submit(frame, Some(deadline)).await.map_err(|e| self.not_sent(target, e.reason))?;
            return Ok(Outcome::End(ADMITTED));
        }
        let admitted = self.send_request(target, kind, payload, compress, deadline, trace).await?;
        admitted.wait(&self.counters, deadline).await
    }

    /// A request's frame queued on the session to `target` by `deadline`.
    async fn send_request(
        self: &Arc<Self>,
        target: &Target,
        kind: u16,
        payload: Bytes,
        compress: bool,
        deadline: Instant,
        trace: Option<TraceContext>,
    ) -> Result<Admitted, CallError> {
        let (link, frame) = self.prepare(target, kind, payload, compress, deadline, trace).await?;
        let ((tx, rx), id) = (oneshot::channel(), frame.id);
        link.pending.lock().insert(id, Waiter::One(tx));
        let every = self.cfg.transport.timing_sample_every as u64;
        // A watch is held open by design: its wait is not a round trip.
        let timed = every != 0 && id.is_multiple_of(every) && target.endpoint != WATCH;
        let admitted = Admitted { rx, entry: Some((link, id, timed.then(Instant::now))) };
        // Sent through the entry, which a refusal or a drop here takes back.
        if let Some((link, ..)) = &admitted.entry {
            link.sender.submit(frame, Some(deadline)).await.map_err(|e| self.not_sent(target, e.reason))?;
        }
        Ok(admitted)
    }

    /// A request admitted on its session, or on this node, by `request_timeout` from now: its answer is due
    /// `request_timeout` after that. Until the answer or a drop it holds a call slot, counted and traced as one call.
    pub(crate) async fn start_request(
        self: &Arc<Self>,
        target: &Target,
        body: Bytes,
    ) -> Result<impl Future<Output = Result<Option<Bytes>, CallError>> + Send + 'static, CallError> {
        let (counted, deadline, here) = Counted::begin(self.clone(), target, "request", None)?;
        let admitted = async {
            let permit = self.pending.clone().try_acquire_owned().map_err(|_| CallError::Busy)?;
            let admitted = if *target.node == *self.local.identity.node_id {
                self.start_local(target, body, deadline, None, here).await?
            } else {
                self.send_request(target, REQUEST, body, false, deadline, here).await?
            };
            Ok((permit, admitted))
        };
        match admitted.await {
            Ok((permit, admitted)) => {
                let due = Instant::now() + self.cfg.request_timeout;
                Ok(async move {
                    let outcome = admitted.wait(&counted.engine.counters, due).await;
                    drop(permit);
                    counted.end(outcome.and_then(Outcome::body))
                })
            }
            Err(e) => counted.end(Err(e)),
        }
    }

    pub async fn call(self: &Arc<Self>, target: &Target, kind: u16, body: Bytes) -> Result<Option<Bytes>, CallError> {
        self.call_with(target, kind, body, false, None).await
    }

    /// `within` replaces `request_timeout`, and the far side is told it.
    pub async fn call_with(
        self: &Arc<Self>,
        target: &Target,
        kind: u16,
        body: Bytes,
        compress: bool,
        within: Option<Duration>,
    ) -> Result<Option<Bytes>, CallError> {
        let name = ["tell", "send", "request"][(kind - TELL) as usize];
        self.counted(target, name, within, |deadline, here| async move {
            let outcome = if *target.node == *self.local.identity.node_id {
                self.call_local(target, kind, body, deadline, within.map(|_| deadline), here).await
            } else {
                let kind = if within.is_some() { kind | DEADLINE } else { kind };
                self.call_remote(target, kind, body, compress, deadline, here).await
            };
            outcome.and_then(Outcome::body)
        })
        .await
    }

    /// A request answered in parts, counted and traced as one call by its first answer, at which it returns. It holds
    /// a call slot until its last part is read or it is dropped.
    pub(crate) async fn call_parts<P>(
        self: &Arc<Self>,
        target: &Target,
        body: Bytes,
        within: Option<Duration>,
    ) -> Result<Parts<P>, CallError> {
        self.counted(target, "request_parts", within, |deadline, here| async move {
            let permit = self.pending.clone().try_acquire_owned().map_err(|_| CallError::Busy)?;
            let flow = Flow::new(parts::WINDOW);
            let idle = within.is_none().then_some(self.cfg.request_timeout);
            let mut parts = if *target.node == *self.local.identity.node_id {
                // Made first, so that a call that never gets to it stops the reply.
                let parts = Parts::new(flow.clone(), None, permit, idle, deadline);
                let core = ReplyCore::new(Sink::Flow(flow), within.map(|_| deadline), usize::MAX, None);
                core.settle(self.admit_local(target, REQUEST, body, deadline, here, Some(&core)).await?);
                parts
            } else {
                let kind = REQUEST | PARTS | if within.is_some() { DEADLINE } else { 0 };
                let (link, frame) = self.prepare(target, kind, body, false, deadline, here).await?;
                let (id, sender) = (frame.id, link.sender.clone());
                link.pending.lock().insert(id, Waiter::Parts(flow.clone()));
                let (tokio, wait) = (self.tokio.clone(), self.cfg.transport.write_timeout);
                let grants = Grants { link, id, tokio, wait, body: false };
                let mut parts = Parts::new(flow, Some(grants), permit, idle, deadline);
                if let Err(e) = sender.submit(frame, Some(deadline)).await {
                    parts.finish();
                    return Err(self.not_sent(target, e.reason));
                }
                parts
            };
            parts.first().await?;
            Ok(parts)
        })
        .await
    }

    /// A request whose body follows in `parts`, counted and traced as one call. It returns at the reply, cutting what
    /// is left of the body; each wait for credit, and the wait for the reply after the handler last read the body, is
    /// bounded by `request_timeout`.
    pub(crate) async fn call_upload<S>(
        self: &Arc<Self>,
        target: &Target,
        head: Bytes,
        parts: S,
    ) -> Result<Option<Bytes>, CallError>
    where
        S: Stream<Item = Result<Bytes, CallError>>,
    {
        self.counted(target, "upload", None, |deadline, here| async move {
            let _permit = self.pending.try_acquire().map_err(|_| CallError::Busy)?;
            let (tx, rx) = oneshot::channel();
            let mut body = self.open_upload::<()>(target, head, Waiter::One(tx), None, deadline, here).await?;
            let mut rx = std::pin::pin!(rx);
            let pumped = tokio::select! {
                biased;
                outcome = rx.as_mut() => return outcome.map_err(|_| CallError::OutcomeUnknown)?.body(),
                pumped = body.pump(parts, self.cfg.request_timeout) => pumped,
            };
            body.end(pumped?);
            body.answer(rx, self.cfg.request_timeout).await?.map_err(|_| CallError::OutcomeUnknown)?.body()
        })
        .await
    }

    /// An upload answered in parts: the body goes from a task of its own, cut when the reply ends or its `Parts` is
    /// dropped. It returns at the first part of the reply, as `call_parts` does.
    pub(crate) async fn call_upload_parts<P, S>(
        self: &Arc<Self>,
        target: &Target,
        head: Bytes,
        parts: S,
    ) -> Result<Parts<P>, CallError>
    where
        S: Stream<Item = Result<Bytes, CallError>> + Send + 'static,
    {
        self.counted(target, "upload_parts", None, |deadline, here| async move {
            let permit = self.pending.clone().try_acquire_owned().map_err(|_| CallError::Busy)?;
            let (flow, idle) = (Flow::new(parts::WINDOW), self.cfg.request_timeout);
            // Made first, so that a call that never gets to it stops the reply.
            let mut reply = Parts::new(flow.clone(), None, permit, Some(idle), deadline);
            let body = self.open_upload(target, head, Waiter::Parts(flow), Some(&mut reply), deadline, here).await?;
            reply.send_body(&self.tokio, body, parts, idle);
            reply.first().await?;
            Ok(reply)
        })
        .await
    }

    /// Sends the head of an upload, whose answer goes to `waiter`, and returns the caller's end of its body. `reply`
    /// reads an answer in parts, and on another node learns where its credit goes.
    async fn open_upload<P>(
        self: &Arc<Self>,
        target: &Target,
        head: Bytes,
        waiter: Waiter,
        reply: Option<&mut Parts<P>>,
        deadline: Instant,
        here: Option<TraceContext>,
    ) -> Result<Body, CallError> {
        let flow = Flow::new(parts::WINDOW);
        if *target.node == *self.local.identity.node_id {
            let sink = match waiter {
                Waiter::One(tx) => Sink::Local(Mutex::new(Some(tx))),
                Waiter::Parts(reply) => Sink::Flow(reply),
            };
            let core = ReplyCore::new(sink, None, usize::MAX, None);
            *core.body.lock() = Some(Upload::new(flow.clone(), None, self.cfg.request_timeout));
            // Made first, so that a call that never gets to it cuts the body.
            let body = Body::new(flow, None, false);
            core.settle(self.admit_local(target, REQUEST, head, deadline, here, Some(&core)).await?);
            return Ok(body);
        }
        let kind = REQUEST | BODY | if reply.is_some() { PARTS } else { 0 };
        let (link, frame) = self.prepare(target, kind, head, false, deadline, here).await?;
        let (id, sender, tokio, wait) =
            (frame.id, link.sender.clone(), self.tokio.clone(), self.cfg.transport.write_timeout);
        let one = matches!(waiter, Waiter::One(_));
        link.pending.lock().insert(id, waiter);
        link.uploading.lock().insert(id, flow.clone());
        if let Some(reply) = reply {
            reply.grants(Grants { link: link.clone(), id, tokio: tokio.clone(), wait, body: false });
        }
        let mut body = Body::new(flow, Some(Grants { link, id, tokio, wait, body: true }), one);
        if let Err(e) = sender.submit(frame, Some(deadline)).await {
            // The head never went: there is no body to end.
            body.end(false);
            return Err(self.not_sent(target, e.reason));
        }
        Ok(body)
    }

    /// Counts, traces and bounds one call: `call` gets its deadline and the trace context it carries.
    async fn counted<T, F: Future<Output = Result<T, CallError>>>(
        &self,
        target: &Target,
        name: &'static str,
        within: Option<Duration>,
        call: impl FnOnce(Instant, Option<TraceContext>) -> F,
    ) -> Result<T, CallError> {
        let (counted, deadline, here) = Counted::begin(self, target, name, within)?;
        counted.end(call(deadline, here).await)
    }

    /// The boot incarnation of `node` as proven by its handshake, connecting if needed.
    pub async fn incarnation_of(self: &Arc<Self>, node: &Arc<str>) -> Result<u128, CallError> {
        if **node == *self.local.identity.node_id {
            return Ok(self.local.identity.incarnation);
        }
        let link = self.link(node, Lane::Control, Instant::now() + self.cfg.request_timeout).await?;
        Ok(link.sender.peer().identity.incarnation)
    }

    /// The shortest of the last four keepalive round trips on the `Control` session this node calls `node` on.
    pub fn round_trip(&self, node: &str) -> Option<Duration> {
        let peers = self.peers.lock();
        let slot = peers.get(node)?[Lane::Control as usize].as_ref()?;
        let LinkState::Up(link, _) = &*slot.state.borrow() else { return None };
        Some(link.sender.min_rtt()).filter(|rtt| !rtt.is_zero())
    }

    /// A request to one of `node`'s system endpoints, with the incarnation it was sent to.
    pub(crate) async fn ask(
        self: &Arc<Self>,
        node: Arc<str>,
        endpoint: u64,
        body: Bytes,
    ) -> Result<(u128, Option<Bytes>), CallError> {
        let incarnation = self.incarnation_of(&node).await?;
        let target = Target { node, incarnation, endpoint, schema: SYSTEM_SCHEMA, lane: Lane::Control };
        Ok((incarnation, self.call(&target, REQUEST, body).await?))
    }

    pub fn stats(&self) -> EngineStats {
        let c = &self.counters;
        let lanes = self.peer_lanes();
        // A row per peer, direction and lane: an accepted session's that it replaced is not one.
        let links = self.links(&lanes, false);
        let peers = links.iter().map(|link| {
            let identity = &link.sender.peer().identity;
            PeerStats {
                node: identity.node_id.clone(),
                incarnation: identity.incarnation,
                outgoing: link.outgoing,
                pending: link.pending.lock().len(),
                calls: link.calls.load(Relaxed),
                request_rtt: link.rtt.snapshot(),
                session: link.sender.stats(),
                version: link.sender.peer().version,
                lane: link.sender.peer().lane,
                down_ms: None,
            }
        });
        let down = lanes.iter().flat_map(|(node, lanes)| {
            let others = lanes.iter().zip(Lane::ALL).skip(1);
            let down = others.filter_map(|(slot, lane)| Some((lane, (*slot.as_ref()?.down_since.lock())?)));
            down.filter_map(move |(lane, since)| {
                let (incarnation, version) = match &*lanes[Lane::Control as usize].as_ref()?.state.borrow() {
                    LinkState::Up(link, _) => (link.sender.peer().identity.incarnation, link.sender.peer().version),
                    _ => return None,
                };
                Some(PeerStats {
                    node: node.to_string(),
                    incarnation,
                    outgoing: true,
                    pending: 0,
                    calls: 0,
                    request_rtt: HistogramSnapshot::default(),
                    session: SessionStats::default(),
                    version,
                    lane,
                    down_ms: Some(u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)),
                })
            })
        });
        EngineStats {
            calls: c.calls.load(Relaxed),
            succeeded: c.succeeded.load(Relaxed),
            rejected: c.rejected.load(Relaxed),
            timed_out: c.timed_out.load(Relaxed),
            unknown_outcomes: c.unknown.load(Relaxed),
            failed: c.failed.load(Relaxed),
            abandoned: c.abandoned.load(Relaxed),
            inbound_calls: c.inbound.load(Relaxed),
            inbound_rejected: std::array::from_fn(|i| c.inbound_rejected[i].load(Relaxed)),
            inbound_expired: c.inbound_expired.load(Relaxed),
            request_rtt: c.rtt.snapshot(),
            connects: c.connects.load(Relaxed),
            dial_failures: c.dial_failures.load(Relaxed),
            pending: self.cfg.max_pending - self.pending.available_permits(),
            endpoints: self.registry.read().by_id.len(),
            listener: self.listener.get(),
            peers: peers.chain(down).collect(),
            versions: (*self.cfg.transport.versions.start(), *self.cfg.transport.versions.end()),
        }
    }
}

/// Every interleaving of the endpoint's return against the reply on another thread.
/// `RUSTFLAGS="--cfg axon_loom" cargo test -p axon-cluster --lib --release --target-dir target/loom engine::model`
#[cfg(test)] // apart from `axon_loom`, so clippy's allow-unwrap-in-tests sees a test module
#[cfg(axon_loom)]
mod model;

#[cfg(test)]
pub(crate) mod tests {
    use axon::ActorSystem;
    use axon_net::SharedSecret;

    use super::*;
    use crate::discovery::Discovery;
    use crate::{Cluster, ClusterConfig, MemoryDiscovery};

    pub(crate) async fn node(id: &str, discovery: MemoryDiscovery) -> (ActorSystem, Cluster) {
        let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"0123456789abcdef0123").unwrap());
        transport.allow_plaintext = true;
        let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery);
        let hour = Duration::from_secs(3600);
        // A boot that refused a dial is not dialled again while the test runs, unless a call has it redialled.
        cfg.backoff = Backoff { initial: hour, max: hour, jitter: 0.0 };
        // Nor does anything else wake the link's task: an Auth refusal waits out poll_interval before its redial.
        (cfg.poll_interval, cfg.resync_interval, cfg.heartbeat_interval) = (hour, hour, hour);
        let system = ActorSystem::builder().build().unwrap();
        let cluster = Cluster::start(&system, cfg).await.unwrap();
        (system, cluster)
    }

    /// On a multi-thread runtime a call can see b's new boot in the view before `reset_peer` has cleared the old
    /// boot's error from b's link. Here the view takes discovery's new snapshot without it, and on this one thread
    /// nothing runs between that and the call. The old boot is refused at another address, at another address under
    /// the same incarnation, and at b's own address, where b's handshake turns it away.
    #[tokio::test]
    async fn a_call_that_sees_a_new_boot_before_the_link_is_reset_waits_for_its_dial() {
        let (b_system, b) = node("b", MemoryDiscovery::new()).await;
        let new = b.record();
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let olds = [
            NodeRecord { incarnation: new.incarnation - 1, address: dead, ..new.clone() },
            NodeRecord { address: dead, ..new.clone() },
            NodeRecord { incarnation: new.incarnation - 1, ..new.clone() },
        ];
        for old in olds {
            let discovery = MemoryDiscovery::new();
            discovery.register(&old).await.unwrap();
            let (a_system, a) = node("a", discovery.clone()).await;
            let (engine, id) = (&a.0.engine, Arc::<str>::from("b"));

            let refused = engine.incarnation_of(&id).await;
            assert!(matches!(refused, Err(CallError::Unreachable(_))), "{old:?}: {refused:?}");
            // The boot the view names is down: a call fails at once rather than waiting out a redial.
            let again = engine.incarnation_of(&id).await;
            assert!(matches!(again, Err(CallError::Unreachable(_))), "{old:?}: {again:?}");

            discovery.register(&new).await.unwrap();
            engine.membership.apply(discovery.snapshot().await.unwrap());
            assert_eq!(engine.incarnation_of(&id).await.unwrap(), new.incarnation, "{old:?}");

            // The reset that comes after keeps the link the call dialled to the boot the view names.
            let connects = engine.counters.connects.load(Relaxed);
            engine.reset_peer(&id);
            tokio::task::yield_now().await;
            assert_eq!(engine.incarnation_of(&id).await.unwrap(), new.incarnation, "{old:?}");
            assert_eq!(engine.counters.connects.load(Relaxed), connects, "{old:?}: the reset closed the new link");

            a_system.shutdown().await.unwrap();
        }
        b_system.shutdown().await.unwrap();
    }

    /// `reset_peer` closes the session to a node that left the view or that the view names another boot of, and
    /// until the link's task sees it end, a call still takes that link. On this one thread nothing runs between the
    /// reset and the call, which must say `NoRoute` or `StaleNode` and not `NotSent(Closed)`, so that a `KeyedRef`
    /// looks again rather than waits for a boot that is gone.
    #[tokio::test]
    async fn a_call_on_a_session_closed_because_its_node_left_or_restarted_looks_again() {
        for restarted in [false, true] {
            let discovery = MemoryDiscovery::new();
            let (b_system, b) = node("b", discovery.clone()).await;
            let (a_system, a) = node("a", discovery.clone()).await;
            let (engine, id) = (&a.0.engine, Arc::<str>::from("b"));
            let incarnation = engine.incarnation_of(&id).await.unwrap();

            if restarted {
                discovery.register(&NodeRecord { incarnation: incarnation + 1, ..b.record() }).await.unwrap();
            } else {
                discovery.unregister(&id, incarnation).await.unwrap();
            }
            engine.membership.apply(discovery.snapshot().await.unwrap());
            engine.reset_peer(&id);
            let target = Target { node: id, incarnation, endpoint: LISTED, schema: SYSTEM_SCHEMA, lane: Lane::Control };
            let refused = engine.call(&target, REQUEST, Bytes::new()).await;
            let looks_again = if restarted {
                matches!(refused, Err(CallError::StaleNode))
            } else {
                matches!(refused, Err(CallError::NoRoute))
            };
            assert!(looks_again, "restarted {restarted}: {refused:?}");

            a_system.shutdown().await.unwrap();
            b_system.shutdown().await.unwrap();
        }
    }
}
