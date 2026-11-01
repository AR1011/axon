//! Fault loops over an in-memory stream: the same exchange is run again and again with the Nth
//! read or write of one side failing, for N = 1, 2, ... until a run finishes untouched, in plaintext
//! and over TLS. Whatever breaks, neither side may hang, what arrived must be an unbroken prefix, and
//! no credit may stay reserved.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use axon_net::rustls::ServerConfig;
use axon_net::*;
use bytes::BytesMut;
use common::{config, identity, one_way};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::mpsc;

mod common;

const FRAMES: u64 = 40;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    ReadError,
    ReadEof,
    WriteError,
    /// Not a failure: every read yields one byte, so every header and body arrives in pieces.
    Trickle,
    /// Not a failure: every write takes one byte, and every other one is refused until the writer is woken, so a
    /// frame the caller began writing is finished by the writer task.
    WriteTrickle,
    /// From the Nth read or write on, neither returns: a pause, or a partition.
    Stall,
    /// From the Nth read on, nothing arrives: a one-way cut towards this side.
    Deaf,
    /// From the Nth write on, what is written is lost: a one-way cut away from this side.
    Mute,
    /// Not a failure: each time the runtime runs a task, 5 ms pass.
    Starved,
    /// Not a failure: each write takes at most 4 KiB and the next waits 250 ms, 16 KB/s that never stops moving.
    Slow,
    /// From the Nth write on, no write returns while reads go on: a peer that stopped reading.
    Blocked,
}

struct Faulty {
    inner: DuplexStream,
    fault: Fault,
    at: usize,
    ops: Arc<AtomicUsize>,
    dead: bool,
    /// Where a cut stream is kept once this side lets go of it, so no end of stream crosses the cut.
    cut: Arc<Mutex<Vec<DuplexStream>>>,
    pause: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Faulty {
    fn fires(&mut self, on: &[Fault]) -> bool {
        if !on.contains(&self.fault) {
            return false;
        }
        self.dead |= self.ops.fetch_add(1, Ordering::SeqCst) + 1 == self.at;
        self.dead
    }

    /// Stalled, blocked or muted: what a write, flush or shutdown returns instead of reaching the far side.
    fn cut_off<T>(&self, done: T) -> Option<Poll<io::Result<T>>> {
        match (self.dead, self.fault) {
            (true, Fault::Stall | Fault::Blocked) => Some(Poll::Pending),
            (true, Fault::Mute) => Some(Poll::Ready(Ok(done))),
            _ => None,
        }
    }
}

impl Drop for Faulty {
    fn drop(&mut self) {
        if self.dead && [Fault::Stall, Fault::Mute].contains(&self.fault) {
            let inner = std::mem::replace(&mut self.inner, tokio::io::duplex(1).0);
            self.cut.lock().unwrap().push(inner);
        }
    }
}

impl AsyncRead for Faulty {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.fault == Fault::Trickle {
            let mut byte = [0];
            let mut one = ReadBuf::new(&mut byte);
            let read = Pin::new(&mut self.inner).poll_read(cx, &mut one);
            buf.put_slice(one.filled());
            return read;
        }
        if self.fires(&[Fault::Stall, Fault::Deaf]) {
            return Poll::Pending;
        }
        let before = buf.filled().len();
        let read = Pin::new(&mut self.inner).poll_read(cx, buf);
        if read.is_ready() && self.fires(&[Fault::ReadError, Fault::ReadEof]) {
            buf.set_filled(before);
            return Poll::Ready(if self.fault == Fault::ReadEof {
                Ok(())
            } else {
                Err(io::ErrorKind::ConnectionReset.into())
            });
        }
        read
    }
}

impl AsyncWrite for Faulty {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if self.fires(&[Fault::WriteError]) {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if self.fires(&[Fault::Stall, Fault::Mute, Fault::Blocked]) {
            return self.cut_off(bytes.len()).expect("cut off");
        }
        if self.fault == Fault::Slow {
            if let Some(pause) = self.pause.as_mut() {
                ready!(pause.as_mut().poll(cx));
            }
            let n = ready!(Pin::new(&mut self.inner).poll_write(cx, &bytes[..bytes.len().min(4096)]))?;
            self.pause = Some(Box::pin(tokio::time::sleep(Duration::from_millis(250))));
            return Poll::Ready(Ok(n));
        }
        if self.fault == Fault::WriteTrickle {
            if self.ops.fetch_add(1, Ordering::SeqCst).is_multiple_of(2) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            return Pin::new(&mut self.inner).poll_write(cx, &bytes[..bytes.len().min(1)]);
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.cut_off(()).unwrap_or_else(|| Pin::new(&mut self.inner).poll_flush(cx))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.cut_off(()).unwrap_or_else(|| Pin::new(&mut self.inner).poll_shutdown(cx))
    }
}

/// Sends `FRAMES` frames and reads the peer's. Returns the ids that arrived, and how the session ended.
async fn exchange(mut session: Session) -> (Vec<u64>, SessionStats) {
    let sender = session.sender.clone();
    let sending = tokio::spawn(async move {
        for id in 0..FRAMES {
            let payload = vec![id as u8; 3000].into();
            let frame = Frame { kind: 1, schema: 1, protocol: 42, id, payload, ..Frame::default() };
            if sender.submit(frame, None).await.is_err() {
                return;
            }
            // An idle socket is written by the caller rather than the writer task, so both are faulted.
            if id.is_multiple_of(8) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let _ = sender.flush().await;
    });
    let (mut seen, mut kept) = (Vec::new(), Vec::new());
    while seen.len() < FRAMES as usize {
        let Ok(got) = session.receiver.recv().await else { break };
        assert!(got.payload.len() == 3000 && got.payload.iter().all(|b| *b == got.id as u8), "a mangled frame");
        seen.push(got.id);
        kept.push(got.into_frame().payload);
    }
    // Every read here fills a sliver of the 64 KiB read buffer: a payload kept from one must not keep the buffer.
    for payload in kept {
        let held = BytesMut::from(payload).capacity();
        assert!(held < 6000, "a kept payload of 3000 bytes pins {held}");
    }
    sending.await.unwrap();
    if seen.len() < FRAMES as usize {
        session.sender.closed().await;
    }
    // The other half lets go of its queue a moment after the session reports closed.
    for _ in 0..200 {
        if reserved(&session.sender.stats()) == (0, 0, 0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (seen, session.sender.stats())
}

fn reserved(s: &SessionStats) -> (usize, usize, usize) {
    (s.outbound_queued_frames, s.outbound_reserved_bytes, s.inbound_reserved_bytes)
}

type Tls = (Arc<ServerConfig>, ClientTls);

/// The responder's certificate, and an initiator that trusts it and presents none: the shared secret authenticates.
fn tls() -> Tls {
    one_way(&identity("b"), "b")
}

/// One dial and one accept, over the two ends a scenario made.
struct OneShot(Mutex<(Option<Faulty>, Option<DuplexStream>)>);

impl Network for OneShot {
    fn connect(&self, _: SocketAddr, _: Lane) -> Dial {
        let near = self.0.lock().unwrap().0.take();
        Box::pin(async move { Ok(Box::new(near.ok_or(io::ErrorKind::ConnectionRefused)?) as Box<dyn Io>) })
    }

    fn bind(&self, at: SocketAddr) -> io::Result<(SocketAddr, Incoming)> {
        let far = self.0.lock().unwrap().1.take().ok_or(io::ErrorKind::AddrInUse)?;
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(Ok((Box::new(far) as Box<dyn Io>, SocketAddr::from(([10, 0, 0, 1], 40_000))))).unwrap();
        Ok((at, rx))
    }
}

type Ends = (Result<Session, Error>, Result<Session, Error>);

/// `a`'s Bulk lane capped at 32 KiB/s from a bucket of 8 KiB, so an exchange of 120 KB waits for it most of its 4 s.
fn capped(node: &str) -> Config {
    let mut cfg = config(node);
    cfg.lanes.bulk.max_bytes_per_sec = Some(32 << 10);
    cfg.lanes.bulk.limits.write_batch_bytes = 8 << 10;
    cfg
}

/// Both ends of the handshake: over `establish`, or with TLS through `connect` and a `Listener`. A `lane` of Bulk is
/// capped on both sides.
async fn handshake(faulty: Faulty, far: DuplexStream, tls: Option<&Tls>, lane: Lane) -> Ends {
    let (a, b) = if lane == Lane::Bulk { (capped("a"), capped("b")) } else { (config("a"), config("b")) };
    let Some((server, client)) = tls else {
        return tokio::join!(establish(faulty, Role::Initiator(lane), &a), establish(far, Role::Responder, &b));
    };
    let network: Arc<dyn Network> = Arc::new(OneShot(Mutex::new((Some(faulty), Some(far)))));
    let on = |mut cfg: Config| {
        (cfg.network, cfg.allow_plaintext) = (Some(network.clone()), false);
        cfg
    };
    let at = SocketAddr::from(([10, 0, 0, 2], 7000));
    let mut listener = Listener::bind(at, Some(server.clone()), on(b), AcceptLimits::default()).await.unwrap();
    // A handshake the listener gave up on is counted, not handed over: its accept loop then ends.
    let a = on(a);
    let (dialled, accepted) = tokio::join!(connect(at, lane, Some(client), &a), listener.accept());
    (dialled, accepted.ok_or(Error::Closed))
}

/// Returns how many fused operations the faulty side made.
async fn scenario(fault: Fault, at: usize, tls: Option<&Tls>, lane: Lane) -> usize {
    let (x, y) = tokio::io::duplex(1 << 12);
    let ops = Arc::new(AtomicUsize::new(0));
    let cut = Arc::default();
    let faulty = Faulty { inner: x, fault, at, ops: ops.clone(), dead: false, cut: Arc::clone(&cut), pause: None };
    let what = format!("{fault:?} at op {at}, tls {}, {lane:?}", tls.is_some());
    let starving = (fault == Fault::Starved).then(|| {
        tokio::spawn(async {
            loop {
                tokio::time::advance(Duration::from_millis(5)).await;
                tokio::task::yield_now().await;
            }
        })
    });
    let run = async {
        match handshake(faulty, y, tls, lane).await {
            (Ok(a), Ok(b)) => {
                let sides = tokio::join!(exchange(a), exchange(b));
                let whole = [&sides.0, &sides.1].iter().all(|(seen, _)| seen.len() == FRAMES as usize);
                if lane == Lane::Bulk && whole {
                    assert!(!sides.0.1.paced.min(sides.1.1.paced).is_zero(), "{what}: the cap never held a batch");
                }
                Some(sides)
            }
            // The responder may have finished its half before the initiator's fault: it learns on first use.
            (Err(_), Ok(b)) => {
                let (_, stats) = exchange(b).await;
                assert_eq!(reserved(&stats), (0, 0, 0), "{what}: credit left behind by the responder alone");
                None
            }
            (a, Err(error)) => {
                assert!(a.is_err(), "{what}: the healthy side failed alone: {error}");
                None
            }
        }
    };
    // Virtual time in the paused tests, where a cut session ends on its idle timeout.
    let outcome = tokio::time::timeout(Duration::from_secs(120), run).await;
    starving.inspect(tokio::task::JoinHandle::abort);
    let outcome = outcome.unwrap_or_else(|_| panic!("{what}: a side hung"));
    drop(cut);
    let steady = [Fault::Trickle, Fault::WriteTrickle, Fault::Starved, Fault::Slow].contains(&fault);
    let fired = !steady && ops.load(Ordering::SeqCst) >= at;
    if let Some(sides) = outcome {
        for (seen, stats) in [&sides.0, &sides.1] {
            assert_eq!(*seen, (0..seen.len() as u64).collect::<Vec<_>>(), "{what}: not a prefix");
            assert!(fired || seen.len() == FRAMES as usize, "{what}: lost frames without a fault");
            assert_eq!(reserved(stats), (0, 0, 0), "{what}: credit left behind");
        }
        // The far side hears nothing more and waits out its idle timer: the blocked side's write timer ends it first.
        if fault == Fault::Blocked && sides.1.0.len() < FRAMES as usize {
            assert_eq!(sides.0.1.closed, Some("timeout_write"), "{what}");
        }
    } else {
        assert!(fired, "{what}: the handshake failed without a fault");
    }
    ops.load(Ordering::SeqCst)
}

/// Runs `fault` at each operation in turn, in plaintext and over TLS, until a run ends before the fault.
async fn at_every_operation(fault: Fault, lane: Lane) {
    let tls = tls();
    for tls in [None, Some(&tls)] {
        let mut at = 1;
        while scenario(fault, at, tls, lane).await >= at {
            at += 1;
        }
        assert!(at > 20, "{fault:?}, tls {}: the exchange only made {at} operations", tls.is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_io_fault_at_any_operation_ends_both_sides_cleanly() {
    for fault in [Fault::ReadError, Fault::ReadEof, Fault::WriteError] {
        at_every_operation(fault, Lane::Control).await;
    }
}

/// On paused time, so a batch trickled through TLS is not timed on a debug build's CPU. At 16 KB/s a batch takes
/// longer than the write timeout, which each write that moves starts again.
#[tokio::test(start_paused = true)]
async fn a_slow_stream_or_a_starved_runtime_loses_nothing() {
    let tls = tls();
    for tls in [None, Some(&tls)] {
        for fault in [Fault::Trickle, Fault::WriteTrickle, Fault::Starved, Fault::Slow] {
            scenario(fault, usize::MAX, tls, Lane::Control).await;
        }
    }
}

/// Nothing fails: each side must notice by its own timers, and a cut side hears no end of stream.
#[tokio::test(start_paused = true)]
async fn a_stream_that_stalls_or_is_cut_one_way_at_any_operation_ends_both_sides_on_their_timers() {
    for fault in [Fault::Stall, Fault::Deaf, Fault::Mute, Fault::Blocked] {
        at_every_operation(fault, Lane::Control).await;
    }
}

/// A capped session is one whose writer waits on its bucket between batches: a fault there, or a close while it waits,
/// must still end both sides and give back every frame's credit. On paused time, as the cap waits about 4 s a run.
#[tokio::test(start_paused = true)]
async fn the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit() {
    for fault in [Fault::ReadError, Fault::WriteError, Fault::Stall, Fault::Mute, Fault::Blocked] {
        at_every_operation(fault, Lane::Bulk).await;
    }
}

/// Lets through every peer but one named `panics`, and panics on it; with `true`, panics for its secret too.
struct Moody(bool);

impl Authenticator for Moody {
    fn secret(&self) -> &[u8] {
        assert!(!self.0, "a secret that panics");
        b"a-cluster-wide-secret"
    }

    fn authorize(&self, peer: &PeerContext<'_>) -> bool {
        assert!(peer.identity.node_id != "panics", "an authorizer that panics");
        true
    }
}

fn moody(node: &str, secret_panics: bool) -> Config {
    let mut cfg = Config::new(node, Moody(secret_panics));
    cfg.allow_plaintext = true;
    cfg
}

#[tokio::test]
async fn an_authenticator_that_panics_fails_its_handshake_on_either_side_and_the_listener_serves_the_next() {
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, moody("server", false), AcceptLimits::default()).await.unwrap();
    let addr = listener.local_addr();
    let dial = |cfg: Config| async move {
        let dialled = tokio::spawn(async move { connect(addr, Lane::Control, None, &cfg).await.map(drop) }).await;
        dialled.map_err(|e| e.is_panic())
    };

    // The listener's authorizer panics on the dialler; then the dialler's own on the listener, then its secret.
    assert!(matches!(dial(moody("panics", false)).await, Ok(Err(Error::Auth))));
    let named = Listener::bind(any, None, moody("panics", false), AcceptLimits::default()).await.unwrap();
    let to_named = named.local_addr();
    let dialled =
        tokio::spawn(async move { connect(to_named, Lane::Control, None, &moody("client", false)).await.map(drop) })
            .await;
    assert!(matches!(dialled.map_err(|e| e.is_panic()), Ok(Err(Error::Auth))), "the dialler's authorizer");
    assert!(matches!(dial(moody("client", true)).await, Ok(Err(Error::Auth))), "the dialler's secret");

    dial(moody("client", false)).await.unwrap().unwrap();
    assert!(listener.accept().await.is_some(), "the listener serves on");
    let failures = listener.stats().handshake_failures;
    assert_eq!(failures[0] + failures[2] + failures[3], 2, "each failed handshake is counted: {failures:?}");
    assert_eq!(failures[0], 1, "the panic is an authentication failure: {failures:?}");
}
