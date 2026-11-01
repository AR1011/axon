use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use axon::{AtomicHistogram, HistogramSnapshot};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior, timeout_at};

use crate::error::within;
use crate::frame::{Compression, Control, Decoder, Encoder, HEADER_LEN};
use crate::handshake::{self, Link, Role};
use crate::queued::Queued;
use crate::{Config, Error, Frame, Identity, Lane, Limits, SubmitError, SubmitReason, TcpHealth, Timeout};

const READ_CHUNK: usize = 64 << 10;
/// A submit writes the socket itself only when nothing was written for this long. A caller sending more often than
/// that is batched by the writer task, so a stream of frames costs a syscall per batch, not per frame.
const WRITE_NOW_IDLE: Duration = Duration::from_micros(5);
/// Larger frames are left to the writer task, so the caller (an actor's worker, for a reply) never copies or
/// compresses more than this.
const WRITE_NOW_MAX: usize = 4 << 10;

/// Negotiated facts about the other side.
#[derive(Clone, Debug)]
pub struct Peer {
    pub identity: Identity,
    pub remote: Option<SocketAddr>,
    pub secure: bool,
    /// Codec used for our outbound frames.
    pub compression: Compression,
    /// Largest payload we may send: `min(ours, theirs)`.
    pub max_frame: usize,
    /// The wire version the session runs: the highest both ends speak.
    pub version: u16,
    /// The lane the initiator named. Each end runs it with its own [`Config::lanes`] entry.
    pub lane: Lane,
}

const RECENT_RTTS: usize = 4;

#[derive(Default)]
struct Counters {
    tx_frames: AtomicU64,
    tx_wire_bytes: AtomicU64,
    tx_payload_bytes: AtomicU64,
    tx_compressed_frames: AtomicU64,
    tx_batches: AtomicU64,
    rx_frames: AtomicU64,
    rx_wire_bytes: AtomicU64,
    rx_payload_bytes: AtomicU64,
    rx_compressed_frames: AtomicU64,
    pings: AtomicU64,
    pongs: AtomicU64,
    rtt_ns: AtomicU64,
    /// The shortest of the last `RECENT_RTTS`; zero until the first pong.
    min_rtt_ns: AtomicU64,
    submits: AtomicU64,
    rejected: [AtomicU64; 4],
    paced_ns: AtomicU64,
    queue: AtomicHistogram,
    encode: AtomicHistogram,
    write: AtomicHistogram,
    decode: AtomicHistogram,
    rtt: AtomicHistogram,
}

/// `queue`, `encode` and `decode` are per sampled frame, `write` per sampled batch, `rtt` per keepalive.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SessionTimings {
    pub sample_every: u32,
    pub queue: HistogramSnapshot,
    pub encode: HistogramSnapshot,
    pub write: HistogramSnapshot,
    pub decode: HistogramSnapshot,
    pub rtt: HistogramSnapshot,
}

fn sampled(every: u32, ordinal: u64) -> bool {
    every != 0 && ordinal.is_multiple_of(every as u64)
}

pub const SUBMIT_REASONS: [&str; 4] = ["full", "closed", "deadline", "oversized"];

/// Byte counters sit above TLS: framed bytes, excluding the handshake and TLS/TCP overhead.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SessionStats {
    pub tx_frames: u64,
    pub tx_wire_bytes: u64,
    pub tx_payload_bytes: u64,
    pub tx_compressed_frames: u64,
    pub tx_batches: u64,
    /// Frames this side refused to admit, indexed like `SUBMIT_REASONS`. Admitted frames are never dropped.
    pub tx_rejected: [u64; 4],
    pub rx_frames: u64,
    pub rx_wire_bytes: u64,
    pub rx_payload_bytes: u64,
    pub rx_compressed_frames: u64,
    pub pings: u64,
    pub pongs: u64,
    /// Last keepalive round trip; 0 until the first pong.
    pub rtt_ns: u64,
    pub outbound_queued_frames: usize,
    pub outbound_reserved_bytes: usize,
    pub inbound_queued_frames: usize,
    pub inbound_reserved_bytes: usize,
    #[cfg_attr(feature = "serde", serde(rename = "age_ns", serialize_with = "axon::ser::ns"))]
    pub age: Duration,
    /// How long the writer waited for the lane's `max_bytes_per_sec`.
    #[cfg_attr(feature = "serde", serde(rename = "paced_ns", serialize_with = "axon::ser::ns"))]
    pub paced: Duration,
    pub closed: Option<&'static str>,
    /// `None` off TCP and on platforms without the socket option. Refreshed at each keepalive tick.
    pub tcp: Option<TcpHealth>,
    pub timings: SessionTimings,
}

struct Shared {
    peer: Peer,
    limits: Limits,
    status: watch::Sender<Option<Error>>,
    accepting: AtomicBool,
    out_bytes: Arc<Semaphore>,
    in_bytes: Arc<Semaphore>,
    inbound: mpsc::WeakSender<Received>,
    queued: Queued,
    writer: tokio::sync::Mutex<Writer>,
    /// Wakes the writer task to finish what a submit could not write without waiting.
    unfinished: Notify,
    counters: Counters,
    fd: Option<i32>,
    tcp: Mutex<Option<TcpHealth>>,
    sample_every: u32,
    opened: Instant,
    _connection: Option<OwnedSemaphorePermit>,
}

impl Shared {
    fn close(&self, reason: Error) {
        self.accepting.store(false, Relaxed);
        self.out_bytes.close();
        self.status.send_if_modified(|s| {
            s.is_none() && {
                *s = Some(reason);
                true
            }
        });
    }
}

enum Command {
    Frame(Frame, OwnedSemaphorePermit, Option<Instant>),
    Flush(oneshot::Sender<()>),
}

/// The write half, held by the writer task for a batch or by a submit for one frame. `out[sent..]` is still to be
/// written; `out` is cleared, and its credit returned, once it has been written and flushed.
struct Writer {
    io: Option<Box<dyn AsyncWrite + Send + Unpin>>,
    encoder: Encoder,
    out: Vec<u8>,
    sent: usize,
    credit: Option<OwnedSemaphorePermit>,
    frames: u64,
    payload_bytes: u64,
    compressed: u64,
    written: Instant,
}

impl Writer {
    fn encode(
        &mut self,
        frame: &Frame,
        credit: OwnedSemaphorePermit,
        queued: Option<Instant>,
        c: &Counters,
    ) -> Result<(), Error> {
        let started = queued.map(|at| {
            let now = Instant::now();
            c.queue.record(now - at);
            now
        });
        self.compressed += self.encoder.encode(frame, &mut self.out)? as u64;
        if let Some(at) = started {
            c.encode.record(at.elapsed());
        }
        self.frames += 1;
        self.payload_bytes += frame.decoded_len().unwrap_or(0) as u64;
        match &mut self.credit {
            Some(held) => held.merge(credit),
            None => self.credit = Some(credit),
        }
        Ok(())
    }

    /// Writes as much of `out` as the stream takes without waiting.
    fn push(&mut self) -> Poll<std::io::Result<()>> {
        let Some(io) = self.io.as_mut() else { return Poll::Ready(Err(std::io::ErrorKind::NotConnected.into())) };
        let mut cx = Context::from_waker(Waker::noop());
        while self.sent < self.out.len() {
            match Pin::new(&mut *io).poll_write(&mut cx, &self.out[self.sent..]) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(n)) => self.sent += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(io).poll_flush(&mut cx)
    }

    /// `started`: when the sampled batch began writing.
    fn written(&mut self, c: &Counters, started: Option<Instant>) {
        if let Some(at) = started {
            c.write.record(at.elapsed());
        }
        c.tx_frames.fetch_add(std::mem::take(&mut self.frames), Relaxed);
        c.tx_payload_bytes.fetch_add(std::mem::take(&mut self.payload_bytes), Relaxed);
        c.tx_compressed_frames.fetch_add(std::mem::take(&mut self.compressed), Relaxed);
        c.tx_wire_bytes.fetch_add(self.out.len() as u64, Relaxed);
        c.tx_batches.fetch_add(1, Relaxed);
        self.discard();
        self.written = Instant::now();
    }

    fn discard(&mut self) {
        self.out.clear();
        (self.sent, self.credit, self.frames, self.payload_bytes, self.compressed) = (0, None, 0, 0, 0);
    }
}

/// An inbound frame. It holds inbound byte credit until dropped, so retention throttles the socket.
pub struct Received {
    frame: Frame,
    _credit: OwnedSemaphorePermit,
}

impl Received {
    pub fn into_frame(self) -> Frame {
        self.frame
    }
}

impl Deref for Received {
    type Target = Frame;
    fn deref(&self) -> &Frame {
        &self.frame
    }
}

pub struct Session {
    pub sender: Sender,
    pub receiver: Receiver,
}

impl Session {
    pub fn peer(&self) -> &Peer {
        self.sender.peer()
    }
}

/// Admission means the local outbox took the frame — not that the peer received it. After any
/// session failure the outcome of admitted frames is unknown.
#[derive(Clone)]
pub struct Sender {
    tx: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}

impl Sender {
    pub fn peer(&self) -> &Peer {
        &self.shared.peer
    }

    fn stamp(&self) -> Option<Instant> {
        let every = self.shared.sample_every;
        (every != 0 && sampled(every, self.shared.counters.submits.fetch_add(1, Relaxed))).then(Instant::now)
    }

    fn cost(&self, frame: &Frame) -> Result<u32, SubmitReason> {
        if !self.shared.accepting.load(Relaxed) {
            return Err(SubmitReason::Closed);
        }
        if !frame.fits(self.shared.peer.max_frame) {
            return Err(SubmitReason::Oversized);
        }
        Ok((HEADER_LEN + frame.payload.len()) as u32)
    }

    pub fn try_submit(&self, frame: Frame) -> Result<(), SubmitError> {
        let admit = || {
            let cost = self.cost(&frame)?;
            let credit = self.shared.out_bytes.clone().try_acquire_many_owned(cost).map_err(|_| SubmitReason::Full)?;
            let Err(credit) = self.write_now(&frame, credit) else { return Ok(None) };
            Ok(Some((credit, self.tx.try_reserve().map_err(|_| SubmitReason::Full)?)))
        };
        let closed = |reason| if self.shared.status.borrow().is_some() { SubmitReason::Closed } else { reason };
        self.finish(admit().map_err(closed), frame)
    }

    /// Waits for outbox capacity until `deadline`. Dropping the future never admits the frame.
    pub async fn submit(&self, frame: Frame, deadline: Option<Instant>) -> Result<(), SubmitError> {
        let admit = async {
            let cost = self.cost(&frame)?;
            let credit = self.shared.out_bytes.clone().acquire_many_owned(cost).await;
            let credit = credit.map_err(|_| SubmitReason::Closed)?;
            let Err(credit) = self.write_now(&frame, credit) else { return Ok(None) };
            Ok(Some((credit, self.tx.reserve().await.map_err(|_| SubmitReason::Closed)?)))
        };
        let admitted = match deadline {
            Some(deadline) => timeout_at(deadline, admit).await.unwrap_or(Err(SubmitReason::Deadline)),
            None => admit.await,
        };
        self.finish(admitted, frame)
    }

    /// Queues an admitted frame, unless it was already written (`None`), or hands a refused one back.
    fn finish(
        &self,
        admitted: Result<Option<(OwnedSemaphorePermit, mpsc::Permit<'_, Command>)>, SubmitReason>,
        frame: Frame,
    ) -> Result<(), SubmitError> {
        match admitted {
            Ok(Some((credit, slot))) => {
                self.shared.queued.queue(|| slot.send(Command::Frame(frame, credit, self.stamp())))
            }
            Ok(None) => {}
            Err(reason) => {
                self.shared.counters.rejected[reason as usize].fetch_add(1, Relaxed);
                return Err(SubmitError { reason, frame });
            }
        }
        Ok(())
    }

    /// Writes the frame on the caller's thread when the socket is idle and nothing is queued ahead of it, which saves
    /// waking the writer task. What the stream does not take at once is left to that task. Hands the credit back when
    /// the frame has to go through the outbox.
    fn write_now(&self, frame: &Frame, credit: OwnedSemaphorePermit) -> Result<(), OwnedSemaphorePermit> {
        let s = &self.shared;
        if frame.payload.len() > WRITE_NOW_MAX {
            return Err(credit);
        }
        // A socket that refused the last write is left to the writer task. Credit taken here and not written is
        // returned when that task ends.
        let Some(mut w) = s.queued.write_now(
            || s.writer.try_lock().ok(),
            |w| w.out.is_empty() && w.io.is_some() && w.written.elapsed() >= WRITE_NOW_IDLE,
        ) else {
            return Err(credit);
        };
        let c = &s.counters;
        if let Err(e) = w.encode(frame, credit, self.stamp(), c) {
            s.close(e);
            return Ok(());
        }
        let started = sampled(s.sample_every, c.tx_batches.load(Relaxed)).then(Instant::now);
        match w.push() {
            Poll::Ready(Ok(())) => w.written(c, started),
            Poll::Ready(Err(e)) => s.close(e.into()),
            Poll::Pending => s.unfinished.notify_one(),
        }
        Ok(())
    }

    /// Resolves once everything admitted before it has been written to the stream.
    pub async fn flush(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let sent = self.tx.send(Command::Flush(tx)).await;
        if sent.is_err() || rx.await.is_err() {
            return Err(self.closed().await);
        }
        Ok(())
    }

    /// Stops admission, flushes what was admitted until `deadline`, then closes.
    pub async fn shutdown(&self, deadline: Instant) -> Result<(), Error> {
        self.shared.accepting.store(false, Relaxed);
        let flushed = within(deadline, Timeout::Shutdown, self.flush()).await;
        self.close();
        flushed
    }

    pub fn close(&self) {
        self.shared.close(Error::Closed);
    }

    pub fn is_active(&self) -> bool {
        self.shared.status.borrow().is_none()
    }

    /// The shortest of the last four keepalive round trips; zero until the first pong. A pong held up behind a paused
    /// peer or a backlog lengthens a sample, not the shortest of four, which still follows a way that grows within
    /// four keepalives.
    pub fn min_rtt(&self) -> Duration {
        Duration::from_nanos(self.shared.counters.min_rtt_ns.load(Relaxed))
    }

    pub async fn closed(&self) -> Error {
        let mut status = self.shared.status.subscribe();
        let reason = status.wait_for(Option::is_some).await.ok().and_then(|s| s.clone());
        reason.unwrap_or(Error::Closed)
    }

    pub fn stats(&self) -> SessionStats {
        let (s, c) = (&self.shared, &self.shared.counters);
        let inbound = s.inbound.upgrade().map_or(0, |tx| tx.max_capacity() - tx.capacity());
        SessionStats {
            tx_frames: c.tx_frames.load(Relaxed),
            tx_wire_bytes: c.tx_wire_bytes.load(Relaxed),
            tx_payload_bytes: c.tx_payload_bytes.load(Relaxed),
            tx_compressed_frames: c.tx_compressed_frames.load(Relaxed),
            tx_batches: c.tx_batches.load(Relaxed),
            tx_rejected: std::array::from_fn(|i| c.rejected[i].load(Relaxed)),
            rx_frames: c.rx_frames.load(Relaxed),
            rx_wire_bytes: c.rx_wire_bytes.load(Relaxed),
            rx_payload_bytes: c.rx_payload_bytes.load(Relaxed),
            rx_compressed_frames: c.rx_compressed_frames.load(Relaxed),
            pings: c.pings.load(Relaxed),
            pongs: c.pongs.load(Relaxed),
            rtt_ns: c.rtt_ns.load(Relaxed),
            outbound_queued_frames: self.tx.max_capacity() - self.tx.capacity(),
            outbound_reserved_bytes: s.limits.outbound_bytes.saturating_sub(s.out_bytes.available_permits()),
            inbound_queued_frames: inbound,
            inbound_reserved_bytes: s.limits.inbound_bytes.saturating_sub(s.in_bytes.available_permits()),
            age: s.opened.elapsed(),
            paced: Duration::from_nanos(c.paced_ns.load(Relaxed)),
            closed: s.status.borrow().as_ref().map(Error::label),
            tcp: s.tcp.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            timings: SessionTimings {
                sample_every: s.sample_every,
                queue: c.queue.snapshot(),
                encode: c.encode.snapshot(),
                write: c.write.snapshot(),
                decode: c.decode.snapshot(),
                rtt: c.rtt.snapshot(),
            },
        }
    }
}

/// Single consumer. Dropping it closes the session in both directions.
pub struct Receiver {
    rx: mpsc::Receiver<Received>,
    sender: Sender,
}

impl Receiver {
    /// Yields frames in wire order, then the terminal reason once drained.
    pub async fn recv(&mut self) -> Result<Received, Error> {
        match self.rx.recv().await {
            Some(frame) => Ok(frame),
            None => Err(self.sender.closed().await),
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.sender.close();
    }
}

/// Runs the handshake over an arbitrary plaintext stream. Requires `allow_plaintext`.
pub async fn establish<S>(stream: S, role: Role, cfg: &Config) -> Result<Session, Error>
where
    S: crate::Io,
{
    cfg.validate(false)?;
    let link = Link { role, secure: false, certificates: Vec::new(), remote: None, fd: None };
    let established = establish_with(stream, link, None, cfg, shared_halves);
    within(Instant::now() + cfg.handshake_timeout, Timeout::Handshake, established).await
}

pub(crate) type Halves = (Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>);

/// `io::split` puts one lock around both halves, taken on every read and write; TCP's own halves need none.
pub(crate) fn tcp_halves(stream: TcpStream) -> Halves {
    let (reader, writer) = stream.into_split();
    (Box::new(reader), Box::new(writer))
}

/// For a stream whose halves drive one state, such as a TLS session.
pub(crate) fn shared_halves<S: crate::Io>(stream: S) -> Halves {
    let (reader, writer) = tokio::io::split(stream);
    (Box::new(reader), Box::new(writer))
}

pub(crate) async fn establish_with<S>(
    mut stream: S,
    link: Link,
    connection: Option<OwnedSemaphorePermit>,
    cfg: &Config,
    split: fn(S) -> Halves,
) -> Result<Session, Error>
where
    S: crate::Io,
{
    let (hello, version, lane) = handshake::run(&mut stream, &link, cfg).await?;
    let lc = cfg.lanes.get(lane);
    let limits = &lc.limits;
    if let (Some(fd), Some(bytes)) = (link.fd, lc.notsent_lowat) {
        // A tuning, not a condition of the session: both ends have finished the handshake, and a kernel that refuses
        // the option still carries it. `TcpHealth::notsent_lowat` shows what took.
        drop(crate::tcp::set_notsent_lowat(fd, bytes));
    }
    let peer = Peer {
        identity: hello.identity,
        remote: link.remote,
        secure: link.secure,
        compression: if hello.accept.contains(lc.compression) { lc.compression } else { Compression::None },
        max_frame: limits.max_frame.min(hello.max_frame),
        version,
        lane,
    };
    let encoder = Encoder::new(
        peer.compression,
        cfg.zstd_level,
        cfg.compression_threshold,
        cfg.compression_min_savings,
        peer.max_frame,
    )?
    .on_request(hello.accept);
    let decoder = Decoder::new(cfg.accept, limits.max_frame);
    let pace = lc.max_bytes_per_sec.map(|per_sec| Pace::new(per_sec, limits.write_batch_bytes));

    let (reader, writer) = split(stream);
    let (out_tx, out_rx) = mpsc::channel(limits.outbound_frames);
    let (in_tx, in_rx) = mpsc::channel(limits.inbound_frames);
    let (ctl_tx, ctl_rx) = mpsc::channel(4);
    let shared = Arc::new(Shared {
        peer,
        limits: limits.clone(),
        status: watch::Sender::new(None),
        accepting: AtomicBool::new(true),
        out_bytes: Arc::new(Semaphore::new(limits.outbound_bytes)),
        in_bytes: Arc::new(Semaphore::new(limits.inbound_bytes)),
        inbound: in_tx.downgrade(),
        // A capped session's frames all go through its bucket, in the writer task.
        queued: Queued::new(pace.is_some()),
        writer: tokio::sync::Mutex::new(Writer {
            io: Some(writer),
            encoder,
            out: Vec::new(),
            sent: 0,
            credit: None,
            frames: 0,
            payload_bytes: 0,
            compressed: 0,
            written: Instant::now(),
        }),
        unfinished: Notify::new(),
        counters: Counters::default(),
        fd: link.fd,
        tcp: Mutex::new(None),
        sample_every: cfg.timing_sample_every,
        opened: Instant::now(),
        _connection: connection,
    });

    let (keepalive, idle, write_timeout) = (cfg.keepalive_interval, cfg.idle_timeout, cfg.write_timeout);
    let reading = shared.clone();
    tokio::spawn(async move {
        let read = read_loop(reader, in_tx, &ctl_tx, decoder, reading.clone(), keepalive, idle);
        until_closed(reading, read).await;
        // Only once the reader's reason is recorded: a writer that saw `ctl` close first would end the session as
        // `Closed`, calling a silent peer one that hung up. A reader that panics still drops it and ends the writer.
        drop(ctl_tx);
    });
    let (writing, mut outbox) = (shared.clone(), out_rx);
    tokio::spawn(async move {
        let write = write_loop(&mut outbox, ctl_rx, writing.clone(), write_timeout, pace);
        until_closed(writing.clone(), write).await;
        // The socket closes once both halves are gone (a TCP write half sends its FIN here), and a submit must not
        // find this one.
        let mut w = writing.writer.lock().await;
        w.io = None;
        w.discard();
        drop(w);
        // A slot reserved before the close is filled after it: drain, or that frame keeps its credit and its flush.
        outbox.close();
        while outbox.recv().await.is_some() {}
    });

    let sender = Sender { tx: out_tx, shared };
    Ok(Session { receiver: Receiver { rx: in_rx, sender: sender.clone() }, sender })
}

async fn until_closed(shared: Arc<Shared>, run: impl Future<Output = Error>) {
    let mut status = shared.status.subscribe();
    tokio::select! {
        _ = status.wait_for(Option::is_some) => {}
        reason = run => shared.close(reason),
    }
}

enum Ctl {
    Ping(u64),
    Pong(u64),
}

async fn read_loop<R: AsyncRead + Unpin>(
    mut io: R,
    inbound: mpsc::Sender<Received>,
    ctl: &mpsc::Sender<Ctl>,
    mut decoder: Decoder,
    shared: Arc<Shared>,
    keepalive: Duration,
    idle: Duration,
) -> Error {
    let c = &shared.counters;
    let (mut buf, mut sparse) = (BytesMut::with_capacity(READ_CHUNK), true);
    // The first tick is at once: a ping as the session comes up measures its round trip one round trip in.
    let mut tick = tokio::time::interval(keepalive);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut heard = Instant::now();
    let mut ping: Option<(u64, Instant)> = None;
    let mut ping_id = 0;
    let (mut recent, mut pongs) = ([None; RECENT_RTTS], 0);
    let mut decodes = 0u64;
    // `io` is half of the stream that owns `fd`, so the descriptor is live wherever this task samples it.
    let sample = || {
        if let Some(health) = shared.fd.and_then(crate::tcp::sample) {
            *shared.tcp.lock().unwrap_or_else(|e| e.into_inner()) = Some(health);
        }
    };
    sample();
    loop {
        loop {
            let started = sampled(shared.sample_every, decodes).then(Instant::now);
            let (header, payload) = match decoder.decode(&mut buf) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(e) => return e.into(),
            };
            decodes += 1;
            if let Some(at) = started {
                c.decode.record(at.elapsed());
            }
            match header.control {
                Control::Ping => drop(ctl.try_send(Ctl::Pong(header.id))),
                Control::Pong => {
                    c.pongs.fetch_add(1, Relaxed);
                    if let Some((_, sent)) = ping.take_if(|(id, _)| *id == header.id) {
                        let rtt = sent.elapsed();
                        let ns = rtt.as_nanos() as u64;
                        c.rtt_ns.store(ns, Relaxed);
                        recent[pongs % RECENT_RTTS] = Some(ns);
                        pongs += 1;
                        c.min_rtt_ns.store(recent.iter().flatten().fold(ns, |m, &n| m.min(n)), Relaxed);
                        c.rtt.record(rtt);
                    }
                }
                Control::Data => {
                    // A slice keeps all of `buf` alive: copied out after a read that filled under half a chunk, so a
                    // quiet session's payloads hold only their own bytes. A small payload kept out of a fuller read
                    // still holds the buffer; a holder that keeps one long copies it (`Frame::payload`).
                    let payload = if sparse && header.codec == Compression::None {
                        Bytes::copy_from_slice(&payload)
                    } else {
                        payload
                    };
                    c.rx_frames.fetch_add(1, Relaxed);
                    c.rx_payload_bytes.fetch_add(payload.len() as u64, Relaxed);
                    c.rx_compressed_frames.fetch_add((header.codec != Compression::None) as u64, Relaxed);
                    let credit = shared.in_bytes.clone().acquire_many_owned(payload.len().max(1) as u32).await;
                    let Ok(_credit) = credit else { return Error::Closed };
                    let frame = Frame {
                        kind: header.kind,
                        schema: header.schema,
                        protocol: header.protocol,
                        id: header.id,
                        payload,
                        ..Frame::default()
                    };
                    if inbound.send(Received { frame, _credit }).await.is_err() {
                        return Error::Closed;
                    }
                    // A slow consumer is not a silent peer.
                    heard = Instant::now();
                }
            }
        }
        buf.reserve(READ_CHUNK);
        tokio::select! {
            read = io.read_buf(&mut buf) => match read {
                Ok(0) if buf.is_empty() => return Error::PeerClosed,
                Ok(0) => return std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into(),
                Ok(n) => {
                    c.rx_wire_bytes.fetch_add(n as u64, Relaxed);
                    sparse = buf.len() < READ_CHUNK / 2;
                    heard = Instant::now();
                }
                Err(e) => return e.into(),
            },
            _ = tick.tick() => {
                if heard.elapsed() >= idle {
                    return Error::Timeout(Timeout::Idle);
                }
                sample();
                ping_id += 1;
                ping = Some((ping_id, Instant::now()));
                c.pings.fetch_add(1, Relaxed);
                drop(ctl.try_send(Ctl::Ping(ping_id)));
            }
        }
    }
}

/// `LaneConfig::max_bytes_per_sec`: a bucket filled at the rate, as deep as one write batch. The writer takes frames
/// while it holds bytes, so the last frame of a batch may overdraw it; the next frame waits until that is paid back.
/// Over any span the frames written come to at most the rate times the span, the depth and one frame.
struct Pace {
    per_sec: f64,
    depth: f64,
    tokens: f64,
    at: Instant,
}

impl Pace {
    fn new(per_sec: u64, depth: usize) -> Self {
        Self { per_sec: per_sec as f64, depth: depth as f64, tokens: depth as f64, at: Instant::now() }
    }

    fn fill(&mut self, now: Instant) {
        self.tokens = (self.tokens + self.per_sec * (now - self.at).as_secs_f64()).min(self.depth);
        self.at = now;
    }

    /// When the bucket next holds a byte, if it holds none now.
    fn until(&mut self, now: Instant) -> Option<Instant> {
        self.fill(now);
        (self.tokens < 1.0).then(|| now + Duration::from_secs_f64((1.0 - self.tokens) / self.per_sec))
    }
}

async fn write_loop(
    outbox: &mut mpsc::Receiver<Command>,
    mut ctl: mpsc::Receiver<Ctl>,
    shared: Arc<Shared>,
    write_timeout: Duration,
    mut pace: Option<Pace>,
) -> Error {
    enum Next {
        Ctl(Ctl),
        Command(Command),
        Unfinished,
        Paced,
    }
    let (c, limits) = (&shared.counters, &shared.limits);
    let mut flushes = Vec::new();
    loop {
        // A capped session in debt takes no frame from the outbox until it is paid back; pings and pongs still go.
        let paced = pace.as_mut().and_then(|p| {
            let now = Instant::now();
            p.until(now).map(|at| (now, at))
        });
        let first = tokio::select! {
            biased;
            next = ctl.recv() => next.map(Next::Ctl),
            () = async { if let Some((_, at)) = paced { tokio::time::sleep_until(at).await } }, if paced.is_some() => {
                Some(Next::Paced)
            }
            next = outbox.recv(), if paced.is_none() => next.map(Next::Command),
            () = shared.unfinished.notified() => Some(Next::Unfinished),
        };
        // Only a wait that a frame is queued behind: paying back the last frame's debt with nothing queued holds
        // nothing up.
        if let Some((since, _)) = paced.filter(|_| !outbox.is_empty()) {
            c.paced_ns.fetch_add(since.elapsed().as_nanos() as u64, Relaxed);
        }
        let Some(mut next) = first else { return Error::Closed };
        let mut guard = shared.writer.lock().await;
        let w = &mut *guard;
        if let Some(p) = &mut pace {
            p.fill(Instant::now());
        }
        let mut dequeued = 0;
        // Never delays a ready frame to fill a batch.
        loop {
            match next {
                Next::Ctl(Ctl::Ping(id)) => w.encoder.control(Control::Ping, id, &mut w.out),
                Next::Ctl(Ctl::Pong(id)) => w.encoder.control(Control::Pong, id, &mut w.out),
                Next::Command(Command::Flush(done)) => flushes.push(done),
                Next::Command(Command::Frame(frame, permit, queued)) => {
                    dequeued += 1;
                    let before = w.out.len();
                    if let Err(e) = w.encode(&frame, permit, queued, c) {
                        return e;
                    }
                    if let Some(p) = &mut pace {
                        p.tokens -= (w.out.len() - before) as f64;
                    }
                }
                Next::Unfinished | Next::Paced => {}
            }
            if w.frames >= limits.write_batch_frames as u64 || w.out.len() >= limits.write_batch_bytes {
                break;
            }
            next = if let Ok(ctl) = ctl.try_recv() {
                Next::Ctl(ctl)
            } else if pace.as_ref().is_some_and(|p| p.tokens < 1.0) {
                break;
            } else if let Ok(command) = outbox.try_recv() {
                Next::Command(command)
            } else {
                break;
            };
        }
        if !w.out.is_empty() {
            let Some(io) = w.io.as_mut() else { return Error::Closed };
            let started = sampled(shared.sample_every, c.tx_batches.load(Relaxed)).then(Instant::now);
            // Each write must make progress within the timeout, not the whole batch: a slow link that keeps moving
            // is not a dead one.
            while w.sent < w.out.len() {
                match within(Instant::now() + write_timeout, Timeout::Write, io.write(&w.out[w.sent..])).await {
                    Ok(0) => return std::io::Error::from(std::io::ErrorKind::WriteZero).into(),
                    Ok(n) => w.sent += n,
                    Err(e) => return e,
                }
            }
            if let Err(e) = within(Instant::now() + write_timeout, Timeout::Write, io.flush()).await {
                return e;
            }
            w.written(c, started);
        }
        shared.queued.written(dequeued);
        drop(guard);
        for done in flushes.drain(..) {
            let _ = done.send(());
        }
    }
}
