//! Trace context that follows messages, and the ring of finished spans a system keeps.
//!
//! The context is ambient: while a traced message is handled it is the thread's current context,
//! and everything admitted to a mailbox from that thread carries it on.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::num::NonZeroU128;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::clock::Clock;
use crate::histogram::sampled;
use crate::types::ActorIncarnation;

thread_local! {
    static CURRENT: Cell<Option<TraceContext>> = const { Cell::new(None) };
    static RANDOM: Cell<u64> = const { Cell::new(0) };
    static SEEDED: Cell<bool> = const { Cell::new(false) };
    static WALL: RefCell<Option<Rc<dyn Fn() -> u64>>> = const { RefCell::new(None) };
}

/// Non-cryptographic and never 0, per thread. Placement, `Backoff` jitter and trace ids draw from it, and so does
/// axon-net's randomness on a thread that `simulate_seed` seeded.
pub fn random_u64() -> u64 {
    RANDOM.with(|state| {
        let mut x = state.get();
        if x == 0 {
            #[expect(clippy::disallowed_methods, reason = "the thread's own entropy, which simulate_seed replaces")]
            let entropy = RandomState::new();
            x = entropy.build_hasher().finish() | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

/// splitmix64's finish: every bit of `h` moves every bit of the result. Rendezvous orders, placement and the World's
/// randomness rank with it, so its values never change.
#[doc(hidden)]
#[inline]
pub fn mix(mut h: u64) -> u64 {
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// The only wall clock axon reads, so that a simulator can give each node its own.
pub fn unix_micros() -> u64 {
    // try_with: a thread-local's destructor may call this after WALL's has run.
    if let Some(micros) = WALL.try_with(|wall| wall.borrow().as_ref().map(|wall| wall())).ok().flatten() {
        return micros;
    }
    #[expect(clippy::disallowed_methods, reason = "the wall clock itself, when no simulator set one")]
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64)
}

/// Test only: makes `unix_micros` on this thread read `wall`, and the system clock again after `None`.
/// Production never calls it.
#[doc(hidden)]
pub fn simulate_wall(wall: Option<Rc<dyn Fn() -> u64>>) {
    WALL.set(wall);
}

/// Test only: makes this thread's `random_u64`, and axon-net's randomness with it, a stream from `seed`. After `None`,
/// axon-net takes the system's randomness again and `random_u64` goes on from where it is. Production never calls it:
/// a seeded thread's handshake nonces are predictable.
#[doc(hidden)]
pub fn simulate_seed(seed: Option<u64>) {
    if let Some(seed) = seed {
        RANDOM.set(seed | 1);
    }
    SEEDED.set(seed.is_some());
}

/// Whether `simulate_seed` seeded this thread: axon-net then draws from `random_u64` instead of the system's randomness.
#[doc(hidden)]
pub fn simulated() -> bool {
    SEEDED.get()
}

/// A trace's id: 128 bits and never 0, as a W3C `traceparent` carries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TraceId(NonZeroU128);

impl TraceId {
    pub const fn new(id: u128) -> Option<Self> {
        match NonZeroU128::new(id) {
            Some(id) => Some(Self(id)),
            None => None,
        }
    }

    pub const fn get(self) -> u128 {
        self.0.get()
    }

    fn random() -> Self {
        let id = (random_u64() as u128) << 64 | random_u64() as u128;
        Self(NonZeroU128::new(id).expect("random_u64 is never 0"))
    }
}

/// 32 hex digits, as W3C writes it.
impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.get())
    }
}

/// `span` is the span that causes whatever is sent next; 0 at the root. `flags` are W3C's trace flags, carried as
/// they came: axon records a traced message whatever they say.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TraceContext {
    pub trace: TraceId,
    pub span: u64,
    pub flags: u8,
}

struct Restore(Option<TraceContext>);

impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.set(self.0);
    }
}

impl TraceContext {
    /// Trace id, span id and flags, big-endian, in `traceparent` order.
    pub const WIRE_LEN: usize = 25;
    /// W3C's `sampled` flag, which a root sets.
    pub const SAMPLED: u8 = 0x01;

    /// One begun outside the cluster, such as from a `traceparent` header.
    pub const fn new(trace: TraceId, span: u64, flags: u8) -> Self {
        Self { trace, span, flags }
    }

    /// Starts a new trace. With the `tracing` feature it says so, with the trace id, in the current `tracing` span.
    pub fn root() -> Self {
        let root = Self::fresh();
        #[cfg(feature = "tracing")]
        quietly(|| tracing::debug!(trace = %root.trace, "trace started"));
        root
    }

    fn fresh() -> Self {
        Self { trace: TraceId::random(), span: 0, flags: Self::SAMPLED }
    }

    pub fn current() -> Option<Self> {
        CURRENT.get()
    }

    /// The same trace and flags, caused by a new span.
    pub fn child(self) -> Self {
        Self { span: random_u64(), ..self }
    }

    /// Messages sent from `f` on this thread belong to this trace.
    pub fn enter<T>(self, f: impl FnOnce() -> T) -> T {
        enter(Some(self), f)
    }

    /// Messages sent from `future`, on whatever thread polls it, belong to this trace.
    pub fn scope<F: Future>(self, future: F) -> Scoped<F> {
        Scoped::new(Some(self), future)
    }

    pub fn to_bytes(self) -> [u8; Self::WIRE_LEN] {
        let mut out = [0; Self::WIRE_LEN];
        out[..16].copy_from_slice(&self.trace.get().to_be_bytes());
        out[16..24].copy_from_slice(&self.span.to_be_bytes());
        out[24] = self.flags;
        out
    }

    /// `None` for a trace id of 0.
    pub fn from_bytes(bytes: [u8; Self::WIRE_LEN]) -> Option<Self> {
        let (trace, rest) = bytes.split_first_chunk::<16>()?;
        let (span, flags) = rest.split_first_chunk::<8>()?;
        Some(Self {
            trace: TraceId::new(u128::from_be_bytes(*trace))?,
            span: u64::from_be_bytes(*span),
            flags: flags[0],
        })
    }

    /// The W3C text form, `00-<trace>-<span>-<flags>`. A root's span is 0, which W3C does not allow: inside a
    /// handler, [`current`](Self::current) names the span that runs.
    pub fn traceparent(&self) -> String {
        format!("00-{}-{:016x}-{:02x}", self.trace, self.span, self.flags)
    }

    /// `None` unless `text` is a valid `traceparent`: lowercase hex, neither id 0, version `00` exactly 55
    /// characters, a later version at least that and `-` after it, and never version `ff`.
    pub fn from_traceparent(text: &str) -> Option<Self> {
        let hex = |at: usize, len: usize| {
            let digits = text.get(at..at + len)?;
            let lower = digits.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c));
            lower.then(|| u128::from_str_radix(digits, 16).ok()).flatten()
        };
        let bytes = text.as_bytes();
        let fits = match hex(0, 2)? {
            0x00 => bytes.len() == 55,
            0xff => false,
            _ => bytes.len() == 55 || bytes.get(55) == Some(&b'-'),
        };
        if !fits || [2, 35, 52].iter().any(|&at| bytes.get(at) != Some(&b'-')) {
            return None;
        }
        let span = hex(36, 16)? as u64;
        if span == 0 {
            return None;
        }
        Some(Self::new(TraceId::new(hex(3, 32)?)?, span, hex(53, 2)? as u8))
    }
}

/// Runs `f` with `context` current, none included, and puts back what was current before.
pub fn enter<T>(context: Option<TraceContext>, f: impl FnOnce() -> T) -> T {
    let _restore = Restore(CURRENT.replace(context));
    f()
}

pub(crate) fn replace_current(context: Option<TraceContext>) -> Option<TraceContext> {
    CURRENT.replace(context)
}

pub struct Scoped<F> {
    context: Option<TraceContext>,
    future: Pin<Box<F>>,
}

impl<F> Scoped<F> {
    pub fn new(context: Option<TraceContext>, future: F) -> Self {
        Self { context, future: Box::pin(future) }
    }
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let context = self.context;
        enter(context, || self.future.as_mut().poll(cx))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanKind {
    /// An actor handled a message.
    Handle,
    /// A call to another node, from submission to its answer.
    Call,
    /// A node took a stream batch.
    Stream,
    /// A step the application recorded itself, with [`Tracer::app`].
    App,
}

/// An attribute's value on an [`SpanKind::App`] span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Attr {
    U64(u64),
    I64(i64),
    Str(Cow<'static, str>),
}

impl From<u64> for Attr {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<i64> for Attr {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<&'static str> for Attr {
    fn from(value: &'static str) -> Self {
        Self::Str(value.into())
    }
}

impl From<String> for Attr {
    fn from(value: String) -> Self {
        Self::Str(value.into())
    }
}

impl std::fmt::Display for Attr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U64(n) => write!(f, "{n}"),
            Self::I64(n) => write!(f, "{n}"),
            Self::Str(text) => f.write_str(text),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Span {
    /// Position in the recording system's ring, from 1.
    pub seq: u64,
    pub trace: TraceId,
    pub span: u64,
    /// 0 for the first span of a trace.
    pub parent: u64,
    pub kind: SpanKind,
    /// The message label, the call kind, the stream, or the application's step.
    pub name: Cow<'static, str>,
    pub actor: Option<(ActorIncarnation, &'static str)>,
    pub peer: Option<Arc<str>>,
    pub start_us: u64,
    /// Time in the mailbox before the handler ran.
    pub wait: Duration,
    pub busy: Duration,
    /// What it ended with, when it failed: `error` or `panic` for a handler, a dead letter's reason, a call's
    /// `CallError::kind`, a stream's error, or the application's own.
    pub error: Option<&'static str>,
    /// An `App` span's attributes, `None` on axon's own. Shared rather than owned, so that copying a page of spans
    /// out of the ring costs no more for them (guide/benchmarks.md).
    pub attrs: Option<Arc<[(&'static str, Attr)]>>,
}

impl Span {
    /// A span under `parent` that starts now; the caller fills in the rest.
    pub fn begin(parent: TraceContext, id: u64, kind: SpanKind, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            seq: 0,
            trace: parent.trace,
            span: id,
            parent: parent.span,
            kind,
            name: name.into(),
            actor: None,
            peer: None,
            start_us: unix_micros(),
            wait: Duration::ZERO,
            busy: Duration::ZERO,
            error: None,
            attrs: None,
        }
    }
}

/// Spans in a sealed segment of the ring.
const SEGMENT: usize = 32;

/// Spans in consecutive positions, the newest last, in segments. A reader clones the segments it wants under the
/// lock and copies their spans after it, so the lock is held for an `Arc` a segment.
#[derive(Default)]
struct Ring {
    /// The last position given out.
    next: u64,
    /// Full segments of [`SEGMENT`] spans, oldest first.
    sealed: VecDeque<Arc<Vec<Span>>>,
    /// The newest spans, fewer than [`SEGMENT`]. A writer copies them first if a reader still holds them.
    open: Arc<Vec<Span>>,
}

impl Ring {
    /// The position of the oldest span still in memory, kept or not.
    fn base(&self) -> u64 {
        self.next + 1 - (self.sealed.len() * SEGMENT + self.open.len()) as u64
    }

    fn segment(&self, index: usize) -> &Arc<Vec<Span>> {
        self.sealed.get(index).unwrap_or(&self.open)
    }
}

/// The most recent spans of one system.
pub struct Tracer {
    capacity: usize,
    sample_every: u32,
    ring: Mutex<Ring>,
    /// The most spans a reader looked at in one hold of the ring's lock.
    most_held: AtomicUsize,
    /// What an [`AppSpan`]'s `busy` is read on.
    clock: Clock,
}

impl Tracer {
    /// The most spans a reader looks at in one hold of the ring's lock, so a traced handler recording a span waits
    /// for one page at most.
    pub const PAGE: usize = 256;

    /// Keeps `capacity` spans. One in `sample_every` messages that arrive outside any trace starts
    /// one; 0 leaves starting traces to [`TraceContext::root`].
    pub fn new(capacity: usize, sample_every: u32) -> Arc<Self> {
        Self::clocked(capacity, sample_every, Clock::system())
    }

    pub(crate) fn clocked(capacity: usize, sample_every: u32, clock: Clock) -> Arc<Self> {
        Arc::new(Self { capacity, sample_every, ring: Mutex::default(), most_held: AtomicUsize::new(0), clock })
    }

    pub(crate) fn sample(&self, ordinal: u64) -> Option<TraceContext> {
        // Not `root`: this runs under the mailbox's lock, and the trace starts at the receiver.
        sampled(self.sample_every, ordinal).then(TraceContext::fresh)
    }

    /// With the `tracing` feature the span is an `axon::trace` event first, whatever the capacity.
    pub fn record(&self, mut span: Span) {
        #[cfg(feature = "tracing")]
        if tracing::level_filters::STATIC_MAX_LEVEL >= tracing::Level::DEBUG
            && tracing::level_filters::LevelFilter::current() >= tracing::Level::DEBUG
        {
            emit(&span);
        }
        if self.capacity == 0 {
            return;
        }
        let mut ring = self.ring.lock();
        ring.next += 1;
        span.seq = ring.next;
        if Arc::get_mut(&mut ring.open).is_none() {
            // A reader still holds the open spans: copy them, with room for the whole segment.
            let mut open = Vec::with_capacity(SEGMENT);
            open.extend_from_slice(&ring.open);
            ring.open = Arc::new(open);
        }
        Arc::make_mut(&mut ring.open).push(span);
        let mut evicted = None;
        if ring.open.len() == SEGMENT {
            let sealed = std::mem::replace(&mut ring.open, Arc::new(Vec::with_capacity(SEGMENT)));
            ring.sealed.push_back(sealed);
            // The oldest segment goes once the newer ones hold `capacity` spans; a reader skips evicted spans before.
            if (ring.sealed.len() - 1) * SEGMENT >= self.capacity {
                evicted = ring.sealed.pop_front();
            }
        }
        drop(ring);
        drop(evicted);
    }

    /// A span of the application's under the current trace, named `name`; `None` outside a trace. It records itself
    /// when dropped.
    pub fn app(self: &Arc<Self>, name: impl Into<Cow<'static, str>>) -> Option<AppSpan> {
        let parent = TraceContext::current()?;
        let here = parent.child();
        let span = Span::begin(parent, here.span, SpanKind::App, name);
        Some(AppSpan { tracer: self.clone(), span, attrs: Vec::new(), here, began: self.clock.now() })
    }

    /// Spans recorded so far, including the ones the ring has dropped.
    pub fn recorded(&self) -> u64 {
        self.ring.lock().next
    }

    /// Spans the ring let go of to make room for newer ones.
    pub fn evicted(&self) -> u64 {
        self.recorded().saturating_sub(self.capacity as u64)
    }

    /// Up to `limit` spans after position `after`, oldest first, read a [`PAGE`](Self::PAGE) at a time.
    pub fn since(&self, after: u64, limit: usize) -> Vec<Span> {
        self.read(after, limit, None)
    }

    /// The spans of `trace` the ring holds, read a page at a time.
    pub fn trace(&self, trace: TraceId) -> Vec<Span> {
        self.read(0, usize::MAX, Some(trace))
    }

    /// Test only: the most spans a reader looked at in one hold of the ring's lock.
    #[doc(hidden)]
    pub fn most_held(&self) -> usize {
        self.most_held.load(Ordering::Relaxed)
    }

    /// Copies the spans after `after`, of `trace` alone if given, up to `limit`, taking a page of them per hold of
    /// the lock. It stops at the newest span of its first hold, so writers faster than it cannot keep it reading.
    /// A span the ring lets go between two holds is skipped, as it would be had the reader come later.
    fn read(&self, mut after: u64, limit: usize, trace: Option<TraceId>) -> Vec<Span> {
        let keep = |span: &&Span| trace.is_none_or(|trace| span.trace == trace);
        let (mut out, mut newest, mut most) = (Vec::new(), None, 0);
        // Allocated outside the lock, and big enough for any page.
        let mut segments = Vec::with_capacity(Self::PAGE / SEGMENT + 1);
        while out.len() < limit {
            let page = Self::PAGE.min(limit - out.len());
            let ring = self.ring.lock();
            let newest = *newest.get_or_insert(ring.next);
            let base = ring.base();
            let first = (after + 1).max(base).max((ring.next + 1).saturating_sub(self.capacity as u64));
            let held = ((newest + 1).saturating_sub(first) as usize).min(page);
            let (from, to) = ((first - base) as usize, (first - base) as usize + held);
            let mut at = from;
            while at < to {
                let start = at / SEGMENT * SEGMENT;
                let end = (start + SEGMENT).min(to);
                segments.push((ring.segment(at / SEGMENT).clone(), at - start..end - start));
                at = end;
            }
            drop(ring);
            for (segment, range) in segments.drain(..) {
                out.extend(segment[range].iter().filter(keep).cloned());
            }
            after = first + held as u64 - 1;
            most = most.max(held);
            if held < page {
                break;
            }
        }
        self.most_held.fetch_max(most, Ordering::Relaxed);
        out
    }
}

/// A step of the application's inside a trace, from [`Tracer::app`]. What is sent under [`enter`](Self::enter) is
/// its child. Dropped, it records itself, `busy` from its start.
pub struct AppSpan {
    tracer: Arc<Tracer>,
    span: Span,
    attrs: Vec<(&'static str, Attr)>,
    here: TraceContext,
    began: Duration,
}

impl AppSpan {
    pub fn attr(&mut self, key: &'static str, value: impl Into<Attr>) -> &mut Self {
        self.attrs.push((key, value.into()));
        self
    }

    /// Marks it failed, with `kind` the name of what went wrong.
    pub fn error(&mut self, kind: &'static str) -> &mut Self {
        self.span.error = Some(kind);
        self
    }

    /// Runs `f` with this span current, so that what `f` sends is caused by it.
    pub fn enter<T>(&self, f: impl FnOnce() -> T) -> T {
        self.here.enter(f)
    }
}

impl Drop for AppSpan {
    fn drop(&mut self) {
        let busy = self.tracer.clock.now().saturating_sub(self.began);
        let attrs = (!self.attrs.is_empty()).then(|| std::mem::take(&mut self.attrs).into());
        let name = std::mem::take(&mut self.span.name);
        self.tracer.record(Span { name, attrs, busy, ..self.span.clone() });
    }
}

// Out of line: inline, it cost every recorded span 33 ns with no subscriber listening (guide/benchmarks.md).
#[cfg(feature = "tracing")]
#[cold]
fn emit(span: &Span) {
    quietly(|| {
        tracing::debug!(
            trace = %span.trace,
            span = %format_args!("{:016x}", span.span),
            parent = %format_args!("{:016x}", span.parent),
            name = %span.name,
            actor = span.actor.map(|(_, actor)| actor),
            peer = span.peer.as_deref(),
            start_us = span.start_us,
            wait_us = span.wait.as_micros() as u64,
            busy_us = span.busy.as_micros() as u64,
            error = span.error,
            "{:?}",
            span.kind,
        );
    });
}

/// The subscriber is the application's code, run on a worker or inside a call: its panic ends here.
#[cfg(feature = "tracing")]
fn quietly(event: impl FnOnce()) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(event));
}

#[cfg(test)]
#[test]
fn mix_is_splitmix64s_finish() {
    // splitmix64's first output from state 0.
    assert_eq!(mix(0x9e37_79b9_7f4a_7c15), 0xe220_a839_7b1d_cdaf);
}
