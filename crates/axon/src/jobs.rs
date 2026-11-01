use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{TrySendError, sync_channel};
use std::task::{Context, Poll, Wake, Waker};

use parking_lot::Mutex;

type AsyncJob = Pin<Box<dyn Future<Output = ()> + Send>>;

pub enum Job {
    Blocking(Box<dyn FnOnce() + Send>),
    Async(AsyncJob),
}

impl Job {
    /// Runs to completion on the current thread.
    pub fn run(self) {
        match self {
            Job::Blocking(f) => f(),
            Job::Async(fut) => block_on(fut),
        }
    }
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on(mut fut: AsyncJob) {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    while fut.as_mut().poll(&mut cx).is_pending() {
        std::thread::park();
    }
}

type Submit = dyn Fn(Job) -> Result<(), Job> + Send + Sync;

/// Where actor jobs run. Submission never blocks the actor: a saturated executor refuses. A job it drops unfinished
/// settles anyway: as cancelled if its actor had cancelled it, and otherwise by ending its actor
/// `Failed("a job was dropped by its executor")`.
#[derive(Clone)]
pub struct JobExecutor {
    submit: Arc<Submit>,
    /// A manual executor's queue, which `EmbeddedDriver::run_until_idle` runs.
    pub(crate) manual: Option<ManualJobs>,
}

impl JobExecutor {
    pub fn custom(submit: impl Fn(Job) -> Result<(), Job> + Send + Sync + 'static) -> Self {
        Self { submit: Arc::new(submit), manual: None }
    }

    /// Async jobs as tasks of the tokio runtime `handle` is of, blocking ones on its blocking threads. It never
    /// refuses: the actors' job limits bound what it holds. A runtime shut down drops what it held, which ends their
    /// actors.
    #[cfg(feature = "system")]
    pub fn tokio(handle: tokio::runtime::Handle) -> Self {
        Self::custom(move |job| {
            match job {
                #[expect(clippy::disallowed_methods, reason = "blocking jobs on the runtime the application named")]
                Job::Blocking(f) => drop(handle.spawn_blocking(f)),
                Job::Async(fut) => drop(handle.spawn(fut)),
            }
            Ok(())
        })
    }

    /// `threads` workers behind a queue of `queued` jobs. Threads exit when the executor is dropped.
    pub fn thread_pool(threads: usize, queued: usize) -> std::io::Result<Self> {
        let (tx, rx) = sync_channel::<Job>(queued.max(1));
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads.max(1) {
            let rx = rx.clone();
            #[expect(clippy::disallowed_methods, reason = "an executor of job threads the application asked for")]
            std::thread::Builder::new().name(format!("axon-job-{i}")).spawn(move || {
                loop {
                    let job = rx.lock().recv();
                    match job {
                        Ok(job) => job.run(),
                        Err(_) => return,
                    }
                }
            })?;
        }
        Ok(Self::custom(move |job| {
            tx.try_send(job).map_err(|e| match e {
                TrySendError::Full(j) | TrySendError::Disconnected(j) => j,
            })
        }))
    }

    pub fn manual(capacity: usize) -> (Self, ManualJobs) {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let manual = ManualJobs(queue.clone());
        let executor = Self::custom(move |job| {
            let mut q = queue.lock();
            if q.len() >= capacity {
                return Err(job);
            }
            q.push_back(job);
            Ok(())
        });
        (Self { manual: Some(manual.clone()), ..executor }, manual)
    }

    pub(crate) fn submit(&self, job: Job) -> Result<(), Job> {
        (self.submit)(job)
    }
}

/// Deterministic executor for tests: jobs run only when asked. A blocking job runs to completion;
/// an async one is polled once and queued again when it is woken, so a source that waits for its
/// next item leaves `run_next` and comes back when the item arrives.
#[derive(Clone)]
pub struct ManualJobs(Arc<Mutex<VecDeque<Job>>>);

/// Wakes an async job by putting it back in the queue.
struct Requeue {
    queue: Arc<Mutex<VecDeque<Job>>>,
    /// Woken before it was parked, and the parked future. One lock, so no wake lands in between.
    parked: Mutex<(bool, Option<AsyncJob>)>,
}

impl Wake for Requeue {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut parked = self.parked.lock();
        match parked.1.take() {
            Some(fut) => {
                drop(parked);
                self.queue.lock().push_back(Job::Async(fut));
            }
            None => parked.0 = true,
        }
    }
}

impl ManualJobs {
    pub fn pending(&self) -> usize {
        self.0.lock().len()
    }

    pub fn run_next(&self) -> bool {
        let Some(job) = self.0.lock().pop_front() else { return false };
        let mut fut = match job {
            Job::Blocking(f) => {
                f();
                return true;
            }
            Job::Async(fut) => fut,
        };
        let requeue = Arc::new(Requeue { queue: self.0.clone(), parked: Mutex::new((false, None)) });
        let waker = Waker::from(requeue.clone());
        if fut.as_mut().poll(&mut Context::from_waker(&waker)).is_pending() {
            let mut parked = requeue.parked.lock();
            if std::mem::take(&mut parked.0) {
                drop(parked);
                self.0.lock().push_back(Job::Async(fut));
            } else {
                parked.1 = Some(fut);
            }
        }
        true
    }
}

/// `fut`'s output, or the panic it raised as `Err`: [`catch_panic`].
pub struct CatchPanic<F: ?Sized>(Option<Pin<Box<F>>>);

/// Makes a panic in `fut` its result: `Err(Panic)` with the panic's message, and `fut` dropped (a panic in that drop is
/// caught too). What `fut` shared with others may be left half changed, so use it on work whose state the job owns.
pub fn catch_panic<F: Future>(fut: F) -> CatchPanic<F> {
    CatchPanic::boxed(Box::pin(fut))
}

impl<F: Future + ?Sized> CatchPanic<F> {
    pub(crate) fn boxed(fut: Pin<Box<F>>) -> Self {
        Self(Some(fut))
    }
}

impl<F: Future + ?Sized> Future for CatchPanic<F> {
    type Output = Result<F::Output, Panic>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = self.0.as_mut().expect("a CatchPanic is not polled after it is ready");
        match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(payload) => {
                let fut = self.0.take();
                let _ = catch_unwind(AssertUnwindSafe(|| drop(fut)));
                Poll::Ready(Err(Panic::from(payload)))
            }
        }
    }
}

/// A panic caught as a result, with its message when it carried a `&str` or a `String`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Panic(Option<String>);

impl Panic {
    pub fn message(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

/// A blocking job's `std::panic::catch_unwind` payload.
impl From<Box<dyn Any + Send>> for Panic {
    fn from(payload: Box<dyn Any + Send>) -> Self {
        let text = match payload.downcast::<String>() {
            Ok(text) => Some(*text),
            Err(payload) => payload.downcast_ref::<&str>().map(|text| text.to_string()),
        };
        Self(text)
    }
}

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().unwrap_or("a panic with no message"))
    }
}

impl std::error::Error for Panic {}

/// Shared between a job and its handle: the flag, and the waker of the future that watches it.
#[derive(Debug, Default)]
pub(crate) struct Cancel {
    flag: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Cancel {
    pub(crate) fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }

    /// The watched future is done; nothing to wake any more.
    pub(crate) fn clear(&self) {
        self.waker.lock().take();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// `Ready` once cancelled; the waker is kept so a later cancel wakes the future.
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_cancelled() {
            return Poll::Ready(());
        }
        let mut slot = self.waker.lock();
        if !slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            *slot = Some(cx.waker().clone());
        }
        if self.is_cancelled() { Poll::Ready(()) } else { Poll::Pending }
    }
}

/// A job or an attached source. Cancelling drops an async job's future at its next poll and
/// suppresses the result of a blocking one, which cannot be interrupted.
#[derive(Clone, Debug)]
pub struct JobHandle {
    pub(crate) operation: u64,
    pub(crate) cancel: Arc<Cancel>,
}

impl JobHandle {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}
