use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::histogram::Timings;
use crate::trace::TraceContext;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorIncarnation {
    pub runtime: u64,
    pub slot: u64,
    pub generation: u64,
}

impl fmt::Display for ActorIncarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.runtime, self.slot, self.generation)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[repr(u8)]
pub enum Phase {
    Starting,
    Running,
    Draining,
    Stopping,
    Stopped,
}

impl Phase {
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            0 => Phase::Starting,
            1 => Phase::Running,
            2 => Phase::Draining,
            3 => Phase::Stopping,
            _ => Phase::Stopped,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopReason {
    Stopped,
    Drained,
    DrainDeadline,
    Failed(String),
    Panicked,
    StartFailed(String),
    RuntimeShutdown,
    DriverDropped,
    /// It stopped past its [`Fence`](crate::Fence), whoever asked it to.
    Fenced,
    /// Stopped because an actor it was linked to (`cx.link`) failed: that one's incarnation.
    Linked(ActorIncarnation),
}

impl StopReason {
    /// Its own failure, or a linked one: what a link passes on and the default restart policy restarts.
    pub fn is_failure(&self) -> bool {
        self.is_own_failure() || matches!(self, Self::Linked(_))
    }

    /// Its own failure: `is_failure` but for a linked actor's.
    pub fn is_own_failure(&self) -> bool {
        matches!(self, Self::Failed(_) | Self::StartFailed(_) | Self::Panicked)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exit {
    pub incarnation: ActorIncarnation,
    pub reason: StopReason,
    /// The discarded messages by label.
    pub discarded: Vec<Discarded>,
    pub pending_jobs: usize,
    /// Timers that were set and never fired: a drain drops them, as does any other stop.
    pub pending_timers: usize,
    pub children_pending: usize,
}

impl Exit {
    pub(crate) fn new(incarnation: ActorIncarnation, reason: StopReason) -> Self {
        Self { incarnation, reason, discarded: Vec::new(), pending_jobs: 0, pending_timers: 0, children_pending: 0 }
    }

    /// All the messages it discarded: the sum of `discarded`.
    pub fn discarded_messages(&self) -> usize {
        self.discarded.iter().map(|d| d.count).sum()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Discarded {
    pub label: &'static str,
    pub count: usize,
    /// The first trace among them.
    pub trace: Option<TraceContext>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RejectReason {
    NotReady,
    Full,
    TooLarge,
    Closed,
    WaiterLimit,
    Deadline,
}

impl RejectReason {
    /// Each reason's name in metrics and dead letters, indexed by `reason as usize`.
    pub const NAMES: [&str; 6] = ["not_ready", "full", "too_large", "closed", "waiter_limit", "deadline"];
}

/// A rejected admission. Always carries the caller's message back.
#[must_use]
pub struct Reject<M> {
    pub reason: RejectReason,
    pub message: M,
}

impl<M> fmt::Debug for Reject<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Reject({:?})", self.reason)
    }
}

impl<M> fmt::Display for Reject<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "message rejected: {:?}", self.reason)
    }
}

impl<M> std::error::Error for Reject<M> {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Enqueued;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MailboxLimits {
    pub messages: usize,
    pub bytes: usize,
    pub max_message_bytes: usize,
    pub waiters: usize,
    /// Local asks (`ask` on an `ActorHandle`, `SupervisedHandle` or `Router`) waiting on this actor, 128 by default,
    /// each from its first poll until it returns or is dropped; past it, `RequestError::RequestLimit` at once, nothing
    /// sent. A remote request, to a published or keyed actor, is a plain message here and takes none: `messages` and
    /// `bytes` bound it, and its node's `ClusterConfig::max_inbound_replies`, each refusing the caller with
    /// `Rejected(Full)`.
    pub requests: usize,
    /// Jobs (`submit_io`, `submit_future` and their `_on` forms, on whichever executor) in flight, or settled with a
    /// result not yet handled.
    pub jobs: usize,
    /// Attached sources and sinks, and watches: they can live as long as the actor, so they have their own budget.
    pub sources: usize,
    pub timers: usize,
    pub children: usize,
}

impl Default for MailboxLimits {
    fn default() -> Self {
        Self {
            messages: 1024,
            bytes: 16 << 20,
            max_message_bytes: 1 << 20,
            waiters: 256,
            requests: 128,
            jobs: 16,
            sources: 1024,
            timers: 128,
            children: 64,
        }
    }
}

impl MailboxLimits {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.messages == 0 {
            return Err("messages must be non-zero");
        }
        if self.max_message_bytes > self.bytes {
            return Err("max_message_bytes exceeds bytes");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TurnBudget {
    pub max_messages: NonZeroUsize,
    pub max_elapsed: Option<Duration>,
}

impl Default for TurnBudget {
    fn default() -> Self {
        Self::messages(64)
    }
}

impl TurnBudget {
    pub fn messages(n: usize) -> Self {
        Self { max_messages: NonZeroUsize::new(n.max(1)).unwrap_or(NonZeroUsize::MIN), max_elapsed: None }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnReport {
    pub ordinary: usize,
    pub completions: usize,
    pub timers: usize,
    pub budget_overrun: bool,
    pub runnable: bool,
    pub stopped: bool,
    pub next_timer: Option<Duration>,
}

/// `EmbeddedDriver::run_until_idle` still had work due after its last round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotIdle;

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Diagnostics {
    pub mailbox_depth: usize,
    pub mailbox_bytes: usize,
    pub reservations: usize,
    pub waiters: usize,
    /// Local asks waiting on it now, of `MailboxLimits::requests`: remote requests are not among them.
    pub requests: usize,
    pub jobs: usize,
    pub timers: usize,
    pub admitted: u64,
    /// Indexed by `RejectReason as usize`.
    pub rejected: [u64; 6],
    pub processed: u64,
    pub turns: u64,
    pub budget_overruns: u64,
    pub asks: u64,
    pub ask_timeouts: u64,
    /// Messages it accepted and never handled, queued or stashed when it ended, each a dead letter: so for a family,
    /// those of the generations that ended, and for a type's retired actors, all of theirs. A live actor has none.
    pub dead_letters: u64,
    #[cfg_attr(feature = "serde", serde(rename = "oldest_message_age_ns", serialize_with = "crate::ser::opt_ns"))]
    /// Age of the oldest queued message that was sampled at admission: a lower bound.
    pub oldest_message_age: Option<Duration>,
    pub timings: Option<Box<Timings>>,
}

/// Adds `$d`'s counters and timings to `$into`'s: a family's `Diagnostics` or a type's `TypeTotals`, whose fields share
/// their names.
macro_rules! add_counters {
    ($into:expr, $d:expr) => {{
        let (into, d) = ($into, $d);
        into.admitted += d.admitted;
        into.rejected.iter_mut().zip(d.rejected).for_each(|(a, b)| *a += b);
        into.processed += d.processed;
        into.turns += d.turns;
        into.budget_overruns += d.budget_overruns;
        into.asks += d.asks;
        into.ask_timeouts += d.ask_timeouts;
        into.dead_letters += d.dead_letters;
        if let Some(t) = &d.timings {
            into.timings.get_or_insert_default().merge(t);
        }
    }};
}
pub(crate) use add_counters;

/// Why `cx.pause_until` refused a handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseError {
    /// An attached source, which lives as long as the actor: pausing until it ends would pause for good.
    Source,
}

/// Why `cx.submit_io`, `submit_future`, `attach`, `attach_sink` or `watch` refused. `Draining` and `Closed` mean the actor is
/// ending, so a handler can drop the work; `Full` and `NoExecutor` are the handler's to act on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskAdmissionError {
    /// `MailboxLimits::jobs` (or `sources`) are in flight or held, or the job executor's queue is full.
    Full,
    /// The actor is draining: it finishes what it has and takes no new work.
    Draining,
    /// The actor is stopping or has stopped.
    Closed,
    /// The runtime runs no jobs: a `Runtime` built directly with no `RuntimeConfig::job_executor`.
    NoExecutor,
}

/// A refused `cx.submit_io` or `submit_future`: why, and the work, which has not run. Dropping the work drops what it
/// holds, so a `ReplyTo` in it answers its caller `ReplyDropped`; keep it to submit again once a job ends.
pub struct SubmitError<W> {
    pub reason: TaskAdmissionError,
    pub work: W,
}

impl<W> fmt::Debug for SubmitError<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SubmitError({:?})", self.reason)
    }
}

impl<W> From<SubmitError<W>> for TaskAdmissionError {
    fn from(refused: SubmitError<W>) -> Self {
        refused.reason
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestError {
    RequestLimit,
    NotAdmitted(RejectReason),
    Deadline { admitted: bool },
    ReplyDropped { admitted: bool },
}

/// An error type whose `Display` is its `Debug`.
macro_rules! display_as_debug {
    ($($t:ty),*) => {$(
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{self:?}")
            }
        }
        impl std::error::Error for $t {}
    )*};
}
#[cfg(feature = "system")]
pub(crate) use display_as_debug;
display_as_debug!(RequestError, SpawnError);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartError {
    Failed(StopReason),
    TimedOut,
}

#[derive(Debug)]
pub enum SpawnError {
    InvalidLimits(&'static str),
    ActorLimit,
    FactoryPanicked,
    RuntimeStopped,
    Thread(std::io::Error),
    InvalidPool(&'static str),
}

/// Why a timer was refused. `Draining` and `Closed` mean the actor is ending, so a handler can drop the timer;
/// `Full` means `MailboxLimits::timers` are already set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerReject {
    Full,
    Draining,
    Closed,
}

pub struct TimerError<M> {
    pub reason: TimerReject,
    pub message: Option<M>,
}

impl<M> fmt::Debug for TimerError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TimerError({:?})", self.reason)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerId {
    pub(crate) incarnation: ActorIncarnation,
    pub(crate) seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerCancel {
    Cancelled,
    AlreadyDispatched,
    StaleIncarnation,
}

/// For maps keyed by what hashes as one `u64` that is an id or a hash already, and needs no defence against chosen
/// keys: the multiply spreads it into the bits the map probes by. axon-stream's maps and the quota table's use it.
#[doc(hidden)]
#[derive(Clone, Copy, Default)]
pub struct IdHasher(u64);
#[doc(hidden)]
pub type IdHash = std::hash::BuildHasherDefault<IdHasher>;

impl std::hash::Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _: &[u8]) {
        unreachable!("keys hash as one u64")
    }

    #[inline]
    fn write_u64(&mut self, id: u64) {
        self.0 = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}
