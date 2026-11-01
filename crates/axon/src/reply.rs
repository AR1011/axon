use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use parking_lot::Mutex;

use crate::clock::DeadlineWake;
use crate::mailbox::{ActorRef, Mailbox, SendMessage};
#[cfg(axon_loom)]
use crate::sync::TryUpdate as _;
use crate::types::{RejectReason, RequestError};

/// How long a lone ask off the workers waits on its thread for the answer before the thread sleeps: a quick handler's
/// answer comes inside it, and saves the thread wake, about 1.6 µs.
const ASK_SPIN: Duration = Duration::from_micros(2);

struct Cell<R> {
    value: Option<R>,
    waker: Option<Waker>,
    replier_gone: bool,
    requester_gone: bool,
}

/// A reply's state. `settled` is raised under the lock once the value is written or the replier is gone, so an asker
/// waiting on its thread looks without the lock; the answer itself is read under it.
struct Slot<R> {
    settled: AtomicBool,
    cell: Mutex<Cell<R>>,
}

std::thread_local! {
    /// The requests made on this thread that may wait on it and are not dropped yet, wherever they are polled now.
    static ASKING: Arc<AtomicUsize> = Arc::default();
}

/// Single-use reply capability carried inside an ordinary message.
pub struct ReplyTo<R>(Arc<Slot<R>>);

impl<R> ReplyTo<R> {
    /// Returns the value back if the requester has already given up.
    pub fn reply(self, value: R) -> Result<(), R> {
        let waker = {
            let mut cell = self.0.cell.lock();
            if cell.requester_gone {
                return Err(value);
            }
            cell.value = Some(value);
            self.0.settled.store(true, Ordering::Release);
            cell.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
        Ok(())
    }

    /// Whether the requester has stopped waiting: a late reply would come back as Err.
    pub fn is_abandoned(&self) -> bool {
        self.0.cell.lock().requester_gone
    }

    /// Out of line: inlined into the drop of every message that may hold a `ReplyTo`, it made a loop of tells 3 ns a
    /// message slower (`perf embedded`).
    #[inline(never)]
    fn give_up(&self) {
        let waker = {
            let mut cell = self.0.cell.lock();
            cell.replier_gone = true;
            self.0.settled.store(true, Ordering::Release);
            cell.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl<R> Drop for ReplyTo<R> {
    fn drop(&mut self) {
        // Only this capability settles its slot: settled already, it has replied.
        if !self.0.settled.load(Ordering::Relaxed) {
            self.give_up();
        }
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Request<M: Send + 'static, R> {
    mailbox: Arc<Mailbox<M>>,
    send: Option<SendMessage<M>>,
    reply: Arc<Slot<R>>,
    deadline: Option<Duration>,
    wake: DeadlineWake,
    /// This request's place in the count of its thread's requests, held while it may still wait there.
    asking: Option<Arc<AtomicUsize>>,
    slot: bool,
    /// Alone on its actor and on its thread, off the workers, on the system clock, and no poll has found it
    /// unanswered yet: the send runs the actor's turn on this thread when it was idle, and the first poll that finds
    /// no answer waits up to `ASK_SPIN` on its thread.
    spin: bool,
    /// Answered, or told the replier is gone: nobody is left to read the cell.
    done: bool,
}

impl<M: Send + 'static, R> Unpin for Request<M, R> {}

impl<M: Send + 'static> ActorRef<M> {
    /// Ask: `build` wraps the reply capability into a message. `deadline` is absolute on the actor's clock and covers
    /// both admission and the reply.
    pub fn request<R>(&self, deadline: Option<Duration>, build: impl FnOnce(ReplyTo<R>) -> M) -> Request<M, R> {
        let mb = &self.0;
        let cell = Cell { value: None, waker: None, replier_gone: false, requester_gone: false };
        let reply = Arc::new(Slot { settled: AtomicBool::new(false), cell: Mutex::new(cell) });
        let before =
            mb.requests.try_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < mb.limits.requests).then_some(n + 1));
        let slot = before.is_ok();
        mb.stats.asks.fetch_add(1, Ordering::Relaxed);
        // A thread with other asks outstanding has other tasks to run: none of its asks waits on it.
        let asking = (slot && mb.clock.is_system() && !crate::runtime::on_worker())
            .then(|| ASKING.try_with(Arc::clone).ok())
            .flatten();
        let spin = asking.as_ref().is_some_and(|n| n.fetch_add(1, Ordering::Relaxed) == 0) && before == Ok(0);
        let send = slot.then(|| self.send(build(ReplyTo(reply.clone())), deadline));
        let wake = DeadlineWake::default();
        Request { mailbox: mb.clone(), send, reply, deadline, wake, asking, slot, spin, done: !slot }
    }
}

impl<M: Send + 'static, R> Request<M, R> {
    /// Waits on this thread until the answer settles or `ASK_SPIN` passes, reading the clock every 64 spins: an answer
    /// there already, as a turn run here leaves it, reads none.
    #[inline(never)]
    fn wait_here(&self) {
        let clock = &self.mailbox.clock;
        let mut until = None;
        let mut spins = 0u32;
        while !self.reply.settled.load(Ordering::Acquire) {
            std::hint::spin_loop();
            spins = spins.wrapping_add(1);
            if spins.is_multiple_of(64) {
                let now = clock.now();
                if now >= *until.get_or_insert(now.saturating_add(ASK_SPIN)) {
                    return;
                }
            }
        }
    }
}

impl<M: Send + 'static, R> Future for Request<M, R> {
    type Output = Result<R, RequestError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.slot {
            return Poll::Ready(Err(RequestError::RequestLimit));
        }
        if let Some(send) = &mut this.send {
            // A thread that would wait for the answer takes the actor's turn itself.
            let outcome =
                if this.spin { crate::runtime::run_here(|| Pin::new(send).poll(cx)) } else { Pin::new(send).poll(cx) };
            let outcome = std::task::ready!(outcome);
            this.send = None;
            if let Err(reject) = outcome {
                return Poll::Ready(Err(match reject.reason {
                    RejectReason::Deadline => {
                        this.mailbox.stats.ask_timeouts.fetch_add(1, Ordering::Relaxed);
                        RequestError::Deadline { admitted: false }
                    }
                    reason => RequestError::NotAdmitted(reason),
                }));
            }
        }
        // The system clock's deadline is read only once its wake is armed, so a first poll and one its answer woke
        // read no clock, and a deadline already past at the first poll arms a wake due at once. A manual or driven
        // clock fires a wake only when it is moved: it is read at every poll.
        let clock = &this.mailbox.clock;
        let lazy = clock.is_system();
        let expired = !lazy && this.deadline.is_some_and(|d| clock.now() >= d);
        if std::mem::take(&mut this.spin) {
            this.wait_here();
        }
        let mut cell = this.reply.cell.lock();
        if let Some(value) = cell.value.take() {
            this.done = true;
            return Poll::Ready(Ok(value));
        }
        if cell.replier_gone {
            this.done = true;
            return Poll::Ready(Err(RequestError::ReplyDropped { admitted: true }));
        }
        if expired || (lazy && this.wake.armed() && this.deadline.is_some_and(|d| clock.now() >= d)) {
            drop(cell);
            this.mailbox.stats.ask_timeouts.fetch_add(1, Ordering::Relaxed);
            return Poll::Ready(Err(RequestError::Deadline { admitted: true }));
        }
        let old = cell.waker.replace(cx.waker().clone());
        drop(cell);
        drop(old);
        this.wake.arm(&this.mailbox.clock, this.deadline, cx.waker());
        Poll::Pending
    }
}

impl<M: Send + 'static, R> Drop for Request<M, R> {
    fn drop(&mut self) {
        self.wake.disarm(&self.mailbox.clock);
        if self.slot {
            self.mailbox.requests.fetch_sub(1, Ordering::AcqRel);
        }
        if let Some(asking) = &self.asking {
            asking.fetch_sub(1, Ordering::Relaxed);
        }
        if self.done {
            return;
        }
        let stale = {
            let mut cell = self.reply.cell.lock();
            cell.requester_gone = true;
            (cell.value.take(), cell.waker.take())
        };
        drop(stale);
    }
}
