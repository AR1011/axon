//! Replies and requests in parts: the flow a reader and its producer share, the readers ([`Parts`] of a reply,
//! [`Upload`] of a request's body), and the producer's loop that both directions run.
//!
//! A remote flow has a copy at each end, the reader's queue and its view of the credit, and the producer's credit; one
//! on this node is the same flow at both.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

use axon::Stream;
use axon_net::{Frame, SubmitReason};
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::AbortHandle;
use tokio::time::{Instant, Sleep, sleep, sleep_until};

use super::{Link, Outcome, Reply, ReplyCore, Sink, end, ended};
use crate::wire::{ADMITTED, BODY, CREDIT, END, NO_REPLY, PART, WireProtocol};
use crate::{CallError, Rejection};

/// What a reader grants at the start: it holds at most this and one part unread.
pub(crate) const WINDOW: u32 = 4 << 20;
/// Replies in parts, and bodies, one session carries at once, so that a peer that does not read cannot take every
/// reply slot.
pub(crate) const MOST: usize = 64;

/// A part's bytes and 64 more, so that empty parts are not free.
fn cost(part: &Bytes) -> i64 {
    part.len() as i64 + 64
}

fn wake(waker: &mut Option<Waker>) {
    if let Some(waker) = waker.take() {
        waker.wake();
    }
}

fn register(slot: &mut Option<Waker>, cx: &Context<'_>) {
    if !slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
        *slot = Some(cx.waker().clone());
    }
}

#[derive(Default)]
struct State {
    queue: VecDeque<Bytes>,
    /// What the producer may still send: it sends while this is above zero.
    credit: i64,
    window: i64,
    /// What the reader has read since its last grant.
    read: i64,
    end: Option<Result<(), CallError>>,
    /// The reader has gone.
    stopped: bool,
    reader: Option<Waker>,
    producer: Option<Waker>,
    /// Grants made while one waits for room in the outbox, sent after it: one at a time per flow.
    owed: i64,
    granting: bool,
    /// Grants so far, which tell the caller of an upload that its handler is still reading.
    granted: u64,
    watcher: Option<Waker>,
}

pub(crate) struct Flow(Mutex<State>);

impl Flow {
    pub fn new(window: u32) -> Arc<Self> {
        let window = window.into();
        Arc::new(Self(Mutex::new(State { credit: window, window, ..State::default() })))
    }

    /// Queues a part for the reader and takes its cost from the credit: false if there was none to take.
    pub fn push(&self, part: Bytes) -> bool {
        let mut s = self.0.lock();
        let had = s.credit > 0;
        s.credit -= cost(&part);
        s.queue.push_back(part);
        wake(&mut s.reader);
        had
    }

    /// The producer of a remote flow sends a part.
    fn spend(&self, part: &Bytes) {
        self.0.lock().credit -= cost(part);
    }

    pub fn grant(&self, credit: u32) {
        let mut s = self.0.lock();
        s.credit += i64::from(credit);
        s.granted += 1;
        wake(&mut s.producer);
        wake(&mut s.watcher);
    }

    /// The grants so far; a later one wakes the task.
    fn granted(&self, cx: &Context<'_>) -> u64 {
        let mut s = self.0.lock();
        register(&mut s.watcher, cx);
        s.granted
    }

    /// The first end wins: the producer's, or the session's.
    pub fn end(&self, end: Result<(), CallError>) {
        let mut s = self.0.lock();
        s.end.get_or_insert(end);
        wake(&mut s.reader);
    }

    /// The reader has gone: whether the producer has not ended yet, and so is owed a stop.
    pub fn stop(&self) -> bool {
        let mut s = self.0.lock();
        s.stopped = true;
        wake(&mut s.producer);
        s.end.is_none()
    }

    pub fn stopped(&self) -> bool {
        self.0.lock().stopped
    }

    /// The reader's next part, with the credit it gives back: what was read since the last grant, once that is half
    /// the window or nothing more is queued. `Ready(None)` after the producer's clean end.
    fn read(&self, cx: &Context<'_>) -> Poll<Option<Result<(Bytes, i64), CallError>>> {
        let mut s = self.0.lock();
        let Some(part) = s.queue.pop_front() else {
            return match s.end.take() {
                Some(end) => Poll::Ready(end.err().map(Err)),
                None => {
                    register(&mut s.reader, cx);
                    Poll::Pending
                }
            };
        };
        s.read += cost(&part);
        let grant = if s.read >= s.window / 2 || s.queue.is_empty() { std::mem::take(&mut s.read) } else { 0 };
        s.credit += grant;
        if grant > 0 {
            s.granted += 1;
            wake(&mut s.producer);
            wake(&mut s.watcher);
        }
        Poll::Ready(Some(Ok((part, grant))))
    }

    /// Ready once a part or the end has come, with the error of an end that came first.
    fn first(&self, cx: &Context<'_>) -> Poll<Result<(), CallError>> {
        let mut s = self.0.lock();
        if !s.queue.is_empty() {
            return Poll::Ready(Ok(()));
        }
        match &s.end {
            Some(Ok(())) => Poll::Ready(Ok(())),
            Some(Err(_)) => Poll::Ready(s.end.take().expect("an end").map(drop)),
            None => {
                register(&mut s.reader, cx);
                Poll::Pending
            }
        }
    }

    /// The producer's view: whether the reader has gone, and whether there is credit to send on. Either changing
    /// wakes the task.
    fn watch(&self, cx: &Context<'_>) -> (bool, bool) {
        let mut s = self.0.lock();
        register(&mut s.producer, cx);
        (s.stopped, s.credit > 0)
    }

    /// Whether a grant may go now; if one is going, this one is owed after it.
    fn may_grant(&self, credit: i64) -> bool {
        let mut s = self.0.lock();
        if s.granting {
            s.owed += credit;
        }
        !s.granting
    }

    /// What was granted while the last grant waited, or `None`, and then the next grant goes at once.
    fn owed(&self, granting: bool) -> Option<i64> {
        let mut s = self.0.lock();
        let owed = std::mem::take(&mut s.owed);
        s.granting = granting && owed > 0 && !s.stopped;
        s.granting.then_some(owed)
    }
}

/// Puts `frame` in the outbox, or, when it is full, waits up to `wait` for room on a task.
fn submit_soon(link: &Link, frame: Frame, tokio: &Handle, wait: Duration) {
    let Err(e) = link.sender.try_submit(frame) else { return };
    if e.reason == SubmitReason::Full {
        let sender = link.sender.clone();
        tokio.spawn(async move { drop(sender.submit(e.frame, Some(Instant::now() + wait)).await) });
    }
}

/// The reader's end of a remote flow: a reply's at its caller, a body's at its handler.
#[derive(Clone)]
pub(crate) struct Grants {
    pub link: Arc<Link>,
    pub id: u64,
    pub tokio: Handle,
    pub wait: Duration,
    /// A request's body, not a reply.
    pub body: bool,
}

impl Grants {
    fn frame(&self, credit: i64) -> Frame {
        let credit = u32::try_from(credit).unwrap_or(u32::MAX);
        let kind = if self.body { CREDIT | BODY } else { CREDIT };
        Frame { kind, id: self.id, payload: Bytes::copy_from_slice(&credit.to_be_bytes()), ..Frame::default() }
    }

    /// Sends `more` credit, or a stop for 0. A full outbox costs it a bounded wait on a task, one at a time per flow,
    /// and grants made meanwhile go after it, together. One that does not go out in time is lost, and the reader's
    /// wait bounds what follows.
    fn credit(&self, flow: &Arc<Flow>, more: i64) {
        if more > 0 && !flow.may_grant(more) {
            return;
        }
        let Err(e) = self.link.sender.try_submit(self.frame(more)) else { return };
        if e.reason != SubmitReason::Full {
            return;
        }
        if more > 0 {
            flow.0.lock().granting = true;
        }
        let (this, flow) = (self.clone(), flow.clone());
        self.tokio.spawn(async move {
            let mut frame = e.frame;
            loop {
                let _ = this.link.sender.submit(frame, Some(Instant::now() + this.wait)).await;
                match flow.owed(more > 0) {
                    Some(owed) => frame = this.frame(owed),
                    None => return,
                }
            }
        });
    }

    /// The read has ended: the frames still coming for it are dropped.
    fn forget(&self) {
        if self.body {
            self.link.bodies.lock().remove(&self.id);
        } else {
            self.link.pending.lock().remove(&self.id);
        }
    }
}

/// What reads a flow, for [`Parts`] and [`Upload`]: each part as it comes, granting credit as it reads, one error or
/// `None` at the end, a bounded wait for each part, and a stop when it is dropped first.
struct Reader {
    flow: Arc<Flow>,
    /// `None` on this node.
    remote: Option<Grants>,
    permit: Option<OwnedSemaphorePermit>,
    /// Each wait's bound from when it starts; `None` when `sleep` bounds the whole flow.
    idle: Option<Duration>,
    sleep: Pin<Box<Sleep>>,
    /// `sleep` bounds the wait under way.
    armed: bool,
    done: bool,
    /// The body of the upload this reply answers, cut when the reply ends, and its flow, whose grants put off the
    /// first answer's due time.
    body: Option<(AbortHandle, Arc<Flow>)>,
}

impl Reader {
    /// `deadline` bounds the first wait, then each with `idle`, else the whole flow; without one, `idle` bounds each.
    fn new(
        flow: Arc<Flow>,
        remote: Option<Grants>,
        permit: Option<OwnedSemaphorePermit>,
        idle: Option<Duration>,
        deadline: Option<Instant>,
    ) -> Self {
        let sleep = Box::pin(sleep_until(deadline.unwrap_or_else(Instant::now)));
        Self { flow, remote, permit, idle, sleep, armed: deadline.is_some(), done: false, body: None }
    }

    /// The read has ended: its slot, its entry and any body it answers are free.
    fn finish(&mut self) {
        self.done = true;
        self.permit = None;
        if let Some(remote) = &self.remote {
            remote.forget();
        }
        if let Some((body, _)) = self.body.take() {
            body.abort();
        }
    }

    fn stop(&mut self) {
        if self.flow.stop()
            && let Some(remote) = &self.remote
        {
            remote.credit(&self.flow, 0);
        }
        self.finish();
    }

    fn timed_out(&mut self) -> CallError {
        self.stop();
        CallError::Timeout
    }

    fn poll_part(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, CallError>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.flow.read(cx) {
            Poll::Ready(Some(Ok((part, grant)))) => {
                self.armed = false;
                if grant > 0
                    && let Some(remote) = &self.remote
                {
                    remote.credit(&self.flow, grant);
                }
                return Poll::Ready(Some(Ok(part)));
            }
            Poll::Ready(Some(Err(e))) => {
                self.finish();
                return Poll::Ready(Some(Err(e)));
            }
            Poll::Ready(None) => {
                self.finish();
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }
        if let Some(idle) = self.idle
            && !self.armed
        {
            self.sleep.as_mut().reset(Instant::now() + idle);
            self.armed = true;
        }
        ready!(self.sleep.as_mut().poll(cx));
        Poll::Ready(Some(Err(self.timed_out())))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        if !self.done {
            self.stop();
        }
    }
}

/// A reply in parts, from [`request_parts`](crate::RemoteRef::request_parts): each part as it comes, then `None` once
/// the handler's stream ended, or one `Err` where the reply was cut. Dropping it stops the reply on the far side.
/// Reading it grants the far side more credit: one that is not read holds its window, its call slot and the far reply
/// slot until it is dropped. It is an [`axon::Stream`], for [`cx.attach`](axon::Context::attach).
pub struct Parts<P> {
    reader: Reader,
    protocol: PhantomData<fn() -> P>,
}

impl<P> Parts<P> {
    /// `deadline` bounds the first answer, then each wait with `idle`, else the whole reply.
    pub(crate) fn new(
        flow: Arc<Flow>,
        remote: Option<Grants>,
        permit: OwnedSemaphorePermit,
        idle: Option<Duration>,
        deadline: Instant,
    ) -> Self {
        Self { reader: Reader::new(flow, remote, Some(permit), idle, Some(deadline)), protocol: PhantomData }
    }

    /// Where its credit goes, once the request is on its way.
    pub(crate) fn grants(&mut self, remote: Grants) {
        self.reader.remote = Some(remote);
    }

    /// Returns once the first part or the end has come: the error of `request` where it never started. While the
    /// handler reads the body it answers, each read puts the due time off to `idle` after it.
    pub(crate) async fn first(&mut self) -> Result<(), CallError> {
        let reader = &mut self.reader;
        let mut seen = None;
        poll_fn(|cx| {
            if let Poll::Ready(first) = reader.flow.first(cx) {
                if first.is_err() {
                    reader.finish();
                }
                return Poll::Ready(first);
            }
            if let (Some((_, body)), Some(idle)) = (&reader.body, reader.idle) {
                let granted = body.granted(cx);
                if seen.replace(granted).is_some_and(|seen| seen != granted) {
                    reader.sleep.as_mut().reset(Instant::now() + idle);
                }
            }
            ready!(reader.sleep.as_mut().poll(cx));
            Poll::Ready(Err(reader.timed_out()))
        })
        .await
    }

    /// The call has ended: its slot and its entry are free.
    pub(crate) fn finish(&mut self) {
        self.reader.finish();
    }
}

impl<P: WireProtocol> Parts<P> {
    pub async fn next(&mut self) -> Option<Result<P::Reply, CallError>> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}

impl<P: WireProtocol> Stream for Parts<P> {
    type Item = Result<P::Reply, CallError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let reader = &mut self.get_mut().reader;
        Poll::Ready(ready!(reader.poll_part(cx)).map(|part| {
            // A part that does not decode ends the reply too: one error, then nothing.
            part.and_then(P::decode_reply).inspect_err(|_| {
                if !reader.done {
                    reader.stop();
                }
            })
        }))
    }
}

impl<P> std::fmt::Debug for Parts<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.reader.flow.0.lock();
        write!(f, "Parts({} queued, done: {})", s.queue.len(), self.reader.done)
    }
}

/// The body of a request in parts, from [`Reply::upload`](crate::Reply::upload): each part as the caller sent it, then
/// `None` once the caller's stream ended, or one `Err`: `NoReply` where the caller cut it, `OutcomeUnknown` where the
/// session failed, `Timeout` where no part came for this node's `request_timeout`. Reading it grants the caller more
/// credit, so a handler holds at most 4 MiB and one part unread. Dropping it stops the body: the caller sends nothing
/// more and waits for the reply. It is an [`axon::Stream`], for [`cx.attach`](axon::Context::attach) or
/// [`Reply::send_parts`](crate::Reply::send_parts).
pub struct Upload(Reader);

impl Upload {
    pub(crate) fn new(flow: Arc<Flow>, remote: Option<Grants>, idle: Duration) -> Self {
        Self(Reader::new(flow, remote, None, Some(idle), None))
    }

    pub async fn next(&mut self) -> Option<Result<Bytes, CallError>> {
        poll_fn(|cx| self.0.poll_part(cx)).await
    }
}

impl Stream for Upload {
    type Item = Result<Bytes, CallError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().0.poll_part(cx)
    }
}

impl std::fmt::Debug for Upload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.0.flow.0.lock();
        write!(f, "Upload({} queued, done: {})", s.queue.len(), self.0.done)
    }
}

/// Ends the reply when dropped: with `code`, or, once the caller has gone, telling it nothing.
struct Cut {
    core: Arc<ReplyCore>,
    code: Option<u8>,
}

impl Drop for Cut {
    fn drop(&mut self) {
        match self.code {
            Some(code) => self.core.finish(Outcome::End(code)),
            None => self.core.release(),
        }
    }
}

pub(super) fn send<R, E, S>(reply: Reply<R>, parts: S) -> impl Future<Output = Result<(), E>> + Send + 'static
where
    S: Stream<Item = Result<R, E>> + Send + 'static,
    E: From<CallError> + 'static,
    R: 'static,
{
    let Reply { answer, encode } = reply;
    let core = answer.0.clone();
    let claimed = core.state.reply();
    drop(answer);
    let refused = core.flow().is_none();
    if refused && claimed {
        core.finish(Outcome::End(Rejection::InvalidMessage as u8));
    }
    // Made now, so that a future dropped unpolled cuts the reply.
    let mut cut = (claimed && !refused).then(|| Cut { core, code: Some(NO_REPLY) });
    async move {
        if refused {
            return Err(CallError::Rejected(Rejection::InvalidMessage).into());
        }
        // Else the caller has been told already.
        let Some(cut) = &mut cut else { return Ok(()) };
        let core = cut.core.clone();
        let flow = core.flow().expect("a reply in parts");
        let to = match &core.sink {
            Sink::Remote { link, id, .. } => Some((&**link, *id, PART, core.compress.load(Relaxed))),
            _ => None,
        };
        let ended = pump(flow, parts, encode, to, core.max_payload, core.deadline, None).await?;
        cut.code = ended.then_some(ADMITTED);
        Ok(())
    }
}

/// The caller's end of a request's body. Dropped before [`end`](Self::end), it cuts the body: the handler reads
/// `NoReply`.
pub(super) struct Body {
    flow: Arc<Flow>,
    /// `None` on this node.
    to: Option<Grants>,
    /// The handler is owed this end.
    code: Option<u8>,
    /// The call waits for one reply under `to`'s id, whose entry goes with this.
    one: bool,
}

impl Body {
    /// `to` is where the body goes, its `id` the caller's call id.
    pub fn new(flow: Arc<Flow>, to: Option<Grants>, one: bool) -> Self {
        Self { flow, to, code: Some(NO_REPLY), one }
    }

    /// The pump has returned `ended`: the handler is told the body ended, or, once it stopped reading, nothing.
    pub fn end(&mut self, ended: bool) {
        self.code = ended.then_some(ADMITTED);
        self.tell();
    }

    fn tell(&mut self) {
        let Some(code) = self.code.take() else { return };
        match &self.to {
            None => self.flow.end(ended(code)),
            Some(to) => submit_soon(&to.link, Frame { kind: END | BODY, ..end(to.id, code) }, &to.tokio, to.wait),
        }
    }

    /// Sends `parts` while the handler grants credit, each wait for it or for room in the outbox at most `starve`
    /// (`Timeout`): `Ok(true)` once the stream ended, `Ok(false)` once the handler stopped reading or the session failed.
    pub async fn pump<S>(&self, parts: S, starve: Duration) -> Result<bool, CallError>
    where
        S: Stream<Item = Result<Bytes, CallError>>,
    {
        let to = self.to.as_ref().map(|to| (&*to.link, to.id, PART | BODY, false));
        let max = to.map_or(usize::MAX, |(link, ..)| link.sender.peer().max_frame);
        pump(&self.flow, parts, Ok, to, max, None, Some(starve)).await
    }

    /// Waits for `answer` once the body has ended: `Timeout` once the handler has neither read any of it nor answered
    /// for `idle`, as what is still on the wire may take longer than that to come.
    pub async fn answer<F: Future>(&self, answer: F, idle: Duration) -> Result<F::Output, CallError> {
        let mut answer = pin!(answer);
        let mut due = pin!(sleep(idle));
        let mut seen = None;
        poll_fn(|cx| {
            if let Poll::Ready(answer) = answer.as_mut().poll(cx) {
                return Poll::Ready(Ok(answer));
            }
            let granted = self.flow.granted(cx);
            if seen.replace(granted).is_some_and(|seen| seen != granted) {
                due.as_mut().reset(Instant::now() + idle);
            }
            due.as_mut().poll(cx).map(|()| Err(CallError::Timeout))
        })
        .await
    }
}

impl Drop for Body {
    fn drop(&mut self) {
        self.tell();
        if let Some(to) = &self.to {
            to.link.uploading.lock().remove(&to.id);
            if self.one {
                to.link.pending.lock().remove(&to.id);
            }
        }
    }
}

/// Pulls a part only while the reader has credit, and stops as soon as the reader has gone or `deadline` has passed,
/// even in the middle of a poll of the stream or of a wait for room in the outbox. `to` is the session, the id, the
/// kind and whether to compress, `None` on this node. A wait for credit, or for room in the outbox, longer than
/// `starve` fails with `Timeout`.
/// `Ok(true)` once the stream ended, `Ok(false)` once the reader has gone.
async fn pump<R, E, S>(
    flow: &Arc<Flow>,
    parts: S,
    encode: fn(R) -> Result<Bytes, CallError>,
    to: Option<(&Link, u64, u16, bool)>,
    max: usize,
    deadline: Option<Instant>,
    starve: Option<Duration>,
) -> Result<bool, E>
where
    S: Stream<Item = Result<R, E>>,
    E: From<CallError>,
{
    let mut parts = pin!(parts);
    let mut expires = deadline.map(|at| Box::pin(sleep_until(at)));
    let mut gone = |cx: &mut Context<'_>| {
        let (stopped, credit) = flow.watch(cx);
        let late = expires.as_mut().is_some_and(|at| at.as_mut().poll(cx).is_ready());
        (stopped || late, credit)
    };
    loop {
        let mut starved = None;
        let next = poll_fn(|cx| match gone(cx) {
            (true, _) => Poll::Ready(None),
            (false, false) => {
                let Some(starve) = starve else { return Poll::Pending };
                let at = starved.get_or_insert_with(|| Box::pin(sleep(starve)));
                ready!(at.as_mut().poll(cx));
                Poll::Ready(Some(Some(Err(CallError::Timeout.into()))))
            }
            (false, true) => {
                starved = None;
                parts.as_mut().poll_next(cx).map(Some)
            }
        });
        // Matched as it comes, so that no item is held across the wait for the outbox.
        let part = match next.await {
            None => return Ok(false),
            Some(None) => return Ok(true),
            Some(Some(item)) => encode(item?)?,
        };
        if part.len() > max {
            return Err(CallError::NotSent(SubmitReason::Oversized).into());
        }
        let Some((link, id, kind, compress)) = to else {
            flow.push(part);
            continue;
        };
        flow.spend(&part);
        let frame = Frame { kind, id, payload: part, compress, ..Frame::default() };
        let mut submit = pin!(link.sender.submit(frame, deadline));
        // An outbox smaller than the window fills before the credit runs out: no room is as stuck as no credit.
        let mut stalled = starve.map(|starve| Box::pin(sleep(starve)));
        let sent = poll_fn(|cx| match gone(cx) {
            (true, _) => Poll::Ready(Some(false)),
            _ => match submit.as_mut().poll(cx) {
                Poll::Ready(sent) => Poll::Ready(Some(sent.is_ok())),
                Poll::Pending if stalled.as_mut().is_some_and(|at| at.as_mut().poll(cx).is_ready()) => {
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            },
        })
        .await;
        match sent {
            Some(true) => {}
            Some(false) => return Ok(false),
            None => return Err(CallError::Timeout.into()),
        }
    }
}

impl<P> Parts<P> {
    /// Sends the body of the upload this reply answers, from a task of its own, cut when the reply ends. An error of
    /// the body ends the reply with it, after what is queued, and stops the far reply.
    pub(super) fn send_body<S>(&mut self, tokio: &Handle, mut body: Body, parts: S, starve: Duration)
    where
        S: Stream<Item = Result<Bytes, CallError>> + Send + 'static,
    {
        let (flow, remote, granted) = (self.reader.flow.clone(), self.reader.remote.clone(), body.flow.clone());
        let task = tokio.spawn(async move {
            match body.pump(parts, starve).await {
                Ok(ended) => body.end(ended),
                Err(e) => {
                    if flow.stop()
                        && let Some(remote) = remote
                    {
                        remote.credit(&flow, 0);
                    }
                    flow.end(Err(e));
                }
            }
        });
        self.reader.body = Some((task.abort_handle(), granted));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_part_past_its_credit_is_refused_and_credit_comes_back_at_half_the_window() {
        let cx = Context::from_waker(Waker::noop());
        let flow = Flow::new(WINDOW);
        let part = Bytes::from(vec![7; 1 << 20]);
        // 4 MiB, each part 1 MiB and 64 bytes: the fourth takes it below zero, and a fifth is past it.
        assert_eq!([0; 5].map(|_| flow.push(part.clone())), [true, true, true, true, false]);
        let read = |flow: &Flow| match flow.read(&cx) {
            Poll::Ready(Some(Ok((_, grant)))) => grant,
            _ => panic!("a part is queued"),
        };
        assert_eq!(read(&flow), 0, "less than half the window read");
        assert_eq!(read(&flow), 2 * cost(&part), "half the window read");
        assert_eq!((read(&flow), read(&flow)), (0, 2 * cost(&part)), "and again");
        assert_eq!(read(&flow), cost(&part), "nothing more queued");
        assert!(flow.read(&cx).is_pending());
        assert!(flow.watch(&cx).1, "the credit is back: {}", flow.0.lock().credit);

        flow.end(Err(CallError::NoReply));
        flow.end(Ok(()));
        assert!(matches!(flow.read(&cx), Poll::Ready(Some(Err(CallError::NoReply)))), "the first end wins");
    }
}
