//! Trace context that follows messages, and the ring of finished spans a system keeps.
//!
//! The context is ambient: while a traced message is handled it is the thread's current context,
//! and everything admitted to a mailbox from that thread carries it on.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

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

/// `span` is the span that causes whatever is sent next; 0 at the root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TraceContext {
    pub trace: NonZeroU64,
    pub span: u64,
}

struct Restore(Option<TraceContext>);

impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.set(self.0);
    }
}

impl TraceContext {
    pub const WIRE_LEN: usize = 16;

    /// Starts a new trace. With the `tracing` feature it says so, with the trace id, in the current `tracing` span.
    pub fn root() -> Self {
        let root = Self::fresh();
        #[cfg(feature = "tracing")]
        quietly(|| tracing::debug!(trace = %format_args!("{:016x}", root.trace), "trace started"));
        root
    }

    fn fresh() -> Self {
        Self { trace: NonZeroU64::new(random_u64()).expect("ids are non-zero"), span: 0 }
    }

    pub fn current() -> Option<Self> {
        CURRENT.get()
    }

    /// The same trace, caused by a new span.
    pub fn child(self) -> Self {
        Self { trace: self.trace, span: random_u64() }
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
        out[..8].copy_from_slice(&self.trace.get().to_be_bytes());
        out[8..].copy_from_slice(&self.span.to_be_bytes());
        out
    }

    pub fn from_bytes(bytes: [u8; Self::WIRE_LEN]) -> Option<Self> {
        let (trace, span) = bytes.split_at(8);
        Some(Self {
            trace: NonZeroU64::new(u64::from_be_bytes(trace.try_into().ok()?))?,
            span: u64::from_be_bytes(span.try_into().ok()?),
        })
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
}

#[derive(Clone, Debug)]
pub struct Span {
    /// Position in the recording system's ring, from 1.
    pub seq: u64,
    pub trace: u64,
    pub span: u64,
    /// 0 for the first span of a trace.
    pub parent: u64,
    pub kind: SpanKind,
    /// The message label, the call kind, or the stream.
    pub name: Cow<'static, str>,
    pub actor: Option<(ActorIncarnation, &'static str)>,
    pub peer: Option<Arc<str>>,
    pub start_us: u64,
    /// Time in the mailbox before the handler ran.
    pub wait: Duration,
    pub busy: Duration,
    pub failed: bool,
}

impl Span {
    /// A span under `parent` that starts now; the caller fills in the rest.
    pub fn begin(parent: TraceContext, id: u64, kind: SpanKind, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            seq: 0,
            trace: parent.trace.get(),
            span: id,
            parent: parent.span,
            kind,
            name: name.into(),
            actor: None,
            peer: None,
            start_us: unix_micros(),
            wait: Duration::ZERO,
            busy: Duration::ZERO,
            failed: false,
        }
    }
}

#[derive(Default)]
struct Ring {
    /// The last position given out.
    next: u64,
    spans: VecDeque<Span>,
}

/// The most recent spans of one system.
pub struct Tracer {
    capacity: usize,
    sample_every: u32,
    ring: Mutex<Ring>,
}

impl Tracer {
    /// Keeps `capacity` spans. One in `sample_every` messages that arrive outside any trace starts
    /// one; 0 leaves starting traces to [`TraceContext::root`].
    pub fn new(capacity: usize, sample_every: u32) -> Arc<Self> {
        Arc::new(Self { capacity, sample_every, ring: Mutex::default() })
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
        if ring.spans.len() == self.capacity {
            ring.spans.pop_front();
        }
        ring.spans.push_back(span);
    }

    /// Spans recorded so far, including the ones the ring has dropped.
    pub fn recorded(&self) -> u64 {
        self.ring.lock().next
    }

    /// Up to `limit` spans after position `after`, oldest first.
    pub fn since(&self, after: u64, limit: usize) -> Vec<Span> {
        let ring = self.ring.lock();
        let skip = ring.spans.partition_point(|s| s.seq <= after);
        ring.spans.iter().skip(skip).take(limit).cloned().collect()
    }

    pub fn trace(&self, trace: u64) -> Vec<Span> {
        self.ring.lock().spans.iter().filter(|s| s.trace == trace).cloned().collect()
    }
}

// Out of line: inline, it cost every recorded span 33 ns with no subscriber listening (guide/benchmarks.md).
#[cfg(feature = "tracing")]
#[cold]
fn emit(span: &Span) {
    quietly(|| {
        tracing::debug!(
            trace = %format_args!("{:016x}", span.trace),
            span = %format_args!("{:016x}", span.span),
            parent = %format_args!("{:016x}", span.parent),
            name = %span.name,
            actor = span.actor.map(|(_, actor)| actor),
            peer = span.peer.as_deref(),
            start_us = span.start_us,
            wait_us = span.wait.as_micros() as u64,
            busy_us = span.busy.as_micros() as u64,
            failed = span.failed,
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
