//! [actors]: guide::actors
//! [cluster]: guide::cluster
//! [streams]: guide::streams
//! [observability]: guide::observability
//! [telemetry]: guide::telemetry
//! [dashboard]: guide::dashboard
//! [tracing]: guide::tracing
//! [simulating]: guide::cluster#simulating-a-cluster
//! [quotas]: guide::cluster#quotas
//! [keyed-disk]: guide::cluster#a-keys-state-on-disk
//! [gateway]: guide::streams#a-websocket-gateway
//! [benchmarks]: guide::benchmarks
#![doc = include_str!("../README.md")]
#![deny(clippy::unwrap_used, clippy::iter_over_hash_type)]
mod backoff;
mod clock;
mod context;
mod dead;
mod driver;
mod events;
pub mod files;
mod histogram;
mod jobs;
mod lane;
mod lifecycle;
mod mailbox;
mod reply;
#[cfg(feature = "system")]
mod router;
mod runtime;
#[cfg(feature = "serde")]
pub mod ser;
mod sink;
mod supervision;
mod sync;
#[cfg(feature = "system")]
mod system;
#[cfg(feature = "system")]
mod topic;
pub mod trace;
mod types;

pub use backoff::Backoff;
pub use clock::{Clock, ManualClock};
pub use context::{Actor, Context, WatchHandle};
pub use dead::{DeadLetter, DeadLetters};
pub use driver::{EmbeddedDriver, MailboxBatch, Ready, SpawnHandle};
pub use events::{EVENTS, Event, EventLog};
/// What `cx.attach` takes: the ecosystem's stream trait, re-exported so a source needs no other crate.
pub use futures_core::Stream;
/// What `cx.attach_sink` takes, re-exported for the same reason.
pub use futures_sink::Sink;
pub use histogram::{
    AtomicHistogram, BUCKETS, Histogram, HistogramSnapshot, MAX_MESSAGE_LABELS, MessageTimings, Timings,
};
pub use jobs::{CatchPanic, Job, JobExecutor, JobHandle, ManualJobs, Panic, catch_panic};
pub use lane::Lane;
pub use lifecycle::{Join, LifecycleHandle, LinkHandle};
pub use mailbox::{ActorRef, Fence, MailboxConfig, Permit, Reserve, SendMessage, Weight};
pub use reply::{ReplyTo, Request};
#[cfg(feature = "system")]
pub use router::{Router, Routing};
pub use runtime::{
    Execution, PoolConfig, PoolStats, Runtime, RuntimeConfig, RuntimeStats, ShutdownReport, Threadless, WorkerStats,
};
pub use sink::{SinkEvent, SinkHandle};
pub use supervision::{RestartPolicy, Supervised, SupervisionHandle, SupervisionReport, SupervisionWaitError};
#[cfg(feature = "system")]
pub use system::*;
#[cfg(feature = "system")]
pub use topic::{Subscriber, Topic};
pub use trace::{Span, SpanKind, TraceContext, Tracer};
pub use types::*;

/// The most bytes a name or a node id may have.
#[doc(hidden)]
pub const MAX_NAME: usize = 256;

/// Not empty, at most `max` bytes, and no control characters: the names and ids every crate takes.
#[doc(hidden)]
pub fn valid_text(s: &str, max: usize) -> bool {
    (1..=max).contains(&s.len()) && !s.chars().any(char::is_control)
}

/// What the unit tests share.
#[cfg(test)]
mod test_support {
    use crate::{Actor, Context};

    /// An actor that does nothing.
    pub(crate) struct Idle;

    impl Actor for Idle {
        type Message = ();
        type Error = ();

        fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
            Ok(())
        }
    }

    /// A system that runs every actor on one task of the test's tokio runtime, and starts no thread.
    #[cfg(feature = "system")]
    pub(crate) fn threadless_system() -> crate::ActorSystem {
        let runtime = crate::RuntimeConfig { threadless: Some(crate::Threadless::default()), ..Default::default() };
        crate::ActorSystem::builder().runtime(runtime).build().unwrap()
    }
}

/// The guides, one page each. Their programs run as doctests of `axon-obs`, which can reach every crate.
#[cfg(not(doctest))]
pub mod guide {
    #[doc = include_str!("../guide/actors.md")]
    pub mod actors {}
    #[doc = include_str!("../guide/cluster.md")]
    pub mod cluster {}
    #[doc = include_str!("../guide/streams.md")]
    pub mod streams {}
    #[doc = include_str!("../guide/observability.md")]
    pub mod observability {}
    #[doc = include_str!("../guide/telemetry.md")]
    pub mod telemetry {}
    #[doc = include_str!("../guide/dashboard.md")]
    pub mod dashboard {}
    #[doc = include_str!("../guide/tracing.md")]
    pub mod tracing {}
    #[doc = include_str!("../guide/benchmarks.md")]
    pub mod benchmarks {}
}
