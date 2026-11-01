use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures_sink::Sink;
use parking_lot::Mutex;

use crate::jobs::JobHandle;
use crate::types::{Reject, RejectReason};

/// What an attached sink tells its actor.
#[derive(Debug, PartialEq, Eq)]
pub enum SinkEvent<E> {
    /// A `try_send` was refused with `Full` and the queue has room again.
    Ready,
    /// The sink is closed: after `close`, after a drain, or on the sink's error. Queued items it had not taken are
    /// dropped. A cancelled or stopped sink sends nothing.
    Closed(Result<(), E>),
}

/// The actor's end of an attached sink. `try_send` never waits; the job writes and flushes. Dropping the handle
/// closes the sink.
pub struct SinkHandle<I> {
    pub(crate) shared: Arc<Shared<I>>,
    pub(crate) job: JobHandle,
}

impl<I> SinkHandle<I> {
    /// Queues `item`. `Full` owes the actor a `SinkEvent::Ready`; `Closed` means no more items are taken.
    pub fn try_send(&self, item: I) -> Result<(), Reject<I>> {
        let mut state = self.shared.0.lock();
        let reason = if state.closing || state.gone {
            RejectReason::Closed
        } else if state.queue.len() >= state.capacity {
            state.refused = true;
            RejectReason::Full
        } else {
            state.queue.push_back(item);
            let waker = state.waker.take();
            drop(state);
            if let Some(waker) = waker {
                waker.wake();
            }
            return Ok(());
        };
        Err(Reject { reason, message: item })
    }

    /// Writes what is queued, flushes and closes; `SinkEvent::Closed` follows.
    pub fn close(&self) {
        self.shared.close();
    }

    /// Drops the sink at once, with what is queued. No event follows.
    pub fn cancel(&self) {
        self.job.cancel();
    }
}

impl<I> Drop for SinkHandle<I> {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl<I> std::fmt::Debug for SinkHandle<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkHandle").field("operation", &self.job.operation).finish()
    }
}

struct State<I> {
    queue: VecDeque<I>,
    capacity: usize,
    closing: bool,
    /// The job is over; nothing reads the queue.
    gone: bool,
    /// A `try_send` was refused and no `Ready` has answered it yet.
    refused: bool,
    /// A `Ready` is on its way to the actor's queue: the actor still owes this sink. Cleared under the actor's job
    /// lock as the `Ready` lands, so a drain sees one or the other.
    delivering: bool,
    waker: Option<Waker>,
}

pub(crate) struct Shared<I>(Mutex<State<I>>);

impl<I> Shared<I> {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(State {
            queue: VecDeque::new(),
            capacity,
            closing: false,
            gone: false,
            refused: false,
            delivering: false,
            waker: None,
        })))
    }

    fn close(&self) {
        let mut state = self.0.lock();
        state.closing = true;
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// How a drain reaches a sink without knowing its item type.
pub(crate) trait Drainable: Send + Sync {
    fn close(&self);
    fn owes_ready(&self) -> bool;
    fn delivered(&self);
    /// Closed or over: nothing more to ask of it.
    fn closing(&self) -> bool;
}

impl<I: Send> Drainable for Shared<I> {
    fn close(&self) {
        Shared::close(self);
    }

    fn owes_ready(&self) -> bool {
        let state = self.0.lock();
        (state.refused || state.delivering) && !state.gone
    }

    fn delivered(&self) {
        self.0.lock().delivering = false;
    }

    fn closing(&self) -> bool {
        let state = self.0.lock();
        state.closing || state.gone
    }
}

/// The job behind a sink: feeds the queue to the sink one item at a time, flushes when the queue runs dry, and ends
/// with the `Closed` event as its result.
pub(crate) struct Pump<S: Sink<I>, I, M> {
    pub sink: Pin<Box<S>>,
    pub shared: Arc<Shared<I>>,
    pub map: Box<dyn FnMut(SinkEvent<S::Error>) -> M + Send>,
    /// Hands `Ready` to the actor alongside job results, so it arrives during a drain too.
    pub deliver: Box<dyn Fn(M) + Send>,
    pub held: Option<I>,
    pub unflushed: bool,
}

/// Items fed per poll before the job flushes and yields, so an actor that keeps pace cannot hold an executor thread.
const BUDGET: usize = 64;

impl<S: Sink<I>, I, M> Pump<S, I, M> {
    fn drive(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        let mut fed = 0;
        loop {
            if self.held.is_some() {
                if fed == BUDGET {
                    std::task::ready!(self.sink.as_mut().poll_flush(cx))?;
                    self.unflushed = false;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                std::task::ready!(self.sink.as_mut().poll_ready(cx))?;
                self.sink.as_mut().start_send(self.held.take().expect("checked above"))?;
                self.unflushed = true;
                fed += 1;
            }
            let (closing, ready) = {
                let mut state = self.shared.0.lock();
                self.held = state.queue.pop_front();
                // Half empty, so the actor refills while the sink still has work.
                let ready = state.refused && !state.closing && state.queue.len() <= state.capacity / 2;
                state.refused &= !ready;
                state.delivering |= ready;
                if self.held.is_none() && !state.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    state.waker = Some(cx.waker().clone());
                }
                (state.closing, ready)
            };
            if ready {
                (self.deliver)((self.map)(SinkEvent::Ready));
            }
            if self.held.is_some() {
                continue;
            }
            if self.unflushed {
                std::task::ready!(self.sink.as_mut().poll_flush(cx))?;
                self.unflushed = false;
                continue;
            }
            return if closing { self.sink.as_mut().poll_close(cx) } else { Poll::Pending };
        }
    }
}

impl<S: Sink<I>, I, M> Future for Pump<S, I, M> {
    type Output = M;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<M> {
        let this = &mut *self;
        let result = std::task::ready!(this.drive(cx));
        this.shared.0.lock().gone = true;
        Poll::Ready((this.map)(SinkEvent::Closed(result)))
    }
}

// Nothing is pinned in place: the sink is boxed and the rest moves freely.
impl<S: Sink<I>, I, M> Unpin for Pump<S, I, M> {}

impl<S: Sink<I>, I, M> Drop for Pump<S, I, M> {
    fn drop(&mut self) {
        let queue = {
            let mut state = self.shared.0.lock();
            state.gone = true;
            std::mem::take(&mut state.queue)
        };
        drop(queue);
    }
}
