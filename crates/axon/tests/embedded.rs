//! Deterministic contract tests: embedded driver, manual clock, manual jobs.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as Cx, Poll, Waker};
use std::time::Duration;

use axon::*;

mod common;
use common::{Recv, losing};

fn poll<F: Future>(f: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Cx::from_waker(Waker::noop()))
}

fn limits(messages: usize) -> MailboxLimits {
    MailboxLimits { messages, ..Default::default() }
}

/// A 10 s idle time, which the tests step past on the manual clock.
fn idle<M>(messages: usize) -> MailboxConfig<M> {
    MailboxConfig { idle: Some(Duration::from_secs(10)), ..limits(messages).into() } // stepped past, never waited
}

enum Msg {
    Push(u32),
    Get(ReplyTo<Vec<u32>>),
    Hold(ReplyTo<()>),
    Fail,
    Panic,
    Stop,
    Timer(u32),
    Every,
    CancelLast(TimerCancel),
    Job(u64),
    PauseOn(u64),
    JobPanics,
    FuturePanics,
    Done(u32),
    Attach(Recv),
    CancelLastJob,
    Bomb(#[allow(dead_code)] DropBomb),
    /// A job whose result is the message it carries.
    JobThen(Box<Msg>),
    /// A timer of so many seconds that delivers the message it carries.
    After(u64, Box<Msg>),
    /// A job that may be refused: the refusal is kept, with its reason, in `refused`.
    TryJob(u64),
    /// A job on the actor's other executor.
    JobOther(u64),
    PauseOnOther(u64),
    /// Pauses on the last job `Job` submitted.
    PauseOnLast,
}

struct DropBomb;

impl Drop for DropBomb {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            panic!("bomb");
        }
    }
}

#[derive(Default)]
struct Recorder {
    seen: Vec<u32>,
    held: Vec<ReplyTo<()>>,
    fail_start: bool,
    setup_on_start: bool,
    stops: Arc<AtomicUsize>,
    last_timer: Option<TimerId>,
    last_job: Option<JobHandle>,
    /// What `Push` and `Done` brought, readable after the actor has ended.
    log: Arc<std::sync::Mutex<Vec<u32>>>,
    refused: Arc<std::sync::Mutex<Vec<(u64, TaskAdmissionError)>>>,
    other: Option<JobExecutor>,
    /// `on_start` pauses on a job that answers this.
    load: Option<fn() -> Msg>,
}

struct LoudError;

impl std::fmt::Debug for LoudError {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("debug panics")
    }
}

impl Actor for Recorder {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if self.setup_on_start {
            cx.schedule_after(Duration::from_secs(1), Msg::Push(1)).map_err(|e| format!("{e:?}"))?;
            cx.submit_io(|| Msg::Done(2)).map_err(|e| format!("{e:?}"))?;
        }
        if let Some(load) = self.load {
            let job = cx.submit_io(load).map_err(|e| format!("{e:?}"))?;
            cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
        }
        if self.fail_start { Err("nope".into()) } else { Ok(()) }
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Push(v) | Msg::Done(v) => {
                self.seen.push(v);
                self.log.lock().unwrap().push(v);
            }
            Msg::Get(reply) => drop(reply.reply(self.seen.clone())),
            Msg::Hold(reply) => self.held.push(reply),
            Msg::Fail => return Err("boom".into()),
            Msg::Panic => panic!("handler panic"),
            Msg::Stop => cx.stop(),
            Msg::Timer(v) => self.last_timer = cx.schedule_after(Duration::from_secs(v as u64), Msg::Push(v)).ok(),
            Msg::Every => self.last_timer = cx.schedule_every(Duration::from_secs(1), || Msg::Push(7)).ok(),
            Msg::CancelLast(expect) => assert_eq!(cx.cancel_timer(self.last_timer.unwrap()), expect),
            Msg::Job(op) => {
                self.last_job = Some(cx.submit_io(move || Msg::Done(op as u32)).map_err(|e| format!("{e:?}"))?)
            }
            Msg::PauseOn(op) => {
                let job = cx.submit_io(move || Msg::Done(op as u32)).map_err(|e| format!("{e:?}"))?;
                cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
            }
            Msg::JobPanics => drop(cx.submit_io(|| panic!("job panic"))),
            Msg::FuturePanics => drop(cx.submit_future(async { panics() })),
            Msg::Attach(source) => {
                let map = |item: Option<u32>| item.map_or(Msg::Push(99), Msg::Push);
                self.last_job = Some(cx.attach(source, map).map_err(|e| format!("{e:?}"))?);
            }
            Msg::CancelLastJob => self.last_job.as_ref().expect("a job").cancel(),
            Msg::PauseOnLast => cx.pause_until(self.last_job.as_ref().expect("a job")).map_err(|e| format!("{e:?}"))?,
            Msg::Bomb(_) => {}
            Msg::JobThen(next) => drop(cx.submit_io(move || *next).map_err(|e| format!("{e:?}"))?),
            Msg::After(secs, msg) => {
                cx.schedule_after(Duration::from_secs(secs), *msg).map_err(|e| format!("{e:?}"))?;
            }
            Msg::TryJob(op) => match cx.submit_io(move || Msg::Done(op as u32)) {
                Ok(job) => self.last_job = Some(job),
                Err(refused) => {
                    assert!(matches!((refused.work)(), Msg::Done(v) if v as u64 == op), "the work comes back");
                    self.refused.lock().unwrap().push((op, refused.reason));
                }
            },
            Msg::JobOther(op) | Msg::PauseOnOther(op) => {
                let pause = matches!(msg, Msg::PauseOnOther(_));
                let other = self.other.as_ref().expect("an executor of its own");
                let job = cx.submit_io_on(other, move || Msg::Done(op as u32)).map_err(|e| format!("{e:?}"))?;
                if pause {
                    cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
                }
                self.last_job = Some(job);
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.stops.fetch_add(1, Ordering::SeqCst);
    }
}

struct Rig {
    driver: EmbeddedDriver<Recorder>,
    log: Arc<std::sync::Mutex<Vec<u32>>>,
    refused: Arc<std::sync::Mutex<Vec<(u64, TaskAdmissionError)>>>,
    actor: ActorRef<Msg>,
    life: LifecycleHandle,
    clock: ManualClock,
    jobs: ManualJobs,
}

fn rig_with(actor: Recorder, config: MailboxConfig<Msg>, start: bool) -> Rig {
    let (log, refused) = (actor.log.clone(), actor.refused.clone());
    let clock = Clock::manual();
    let (executor, jobs) = JobExecutor::manual(8);
    let (mut driver, spawn) = EmbeddedDriver::with_jobs(actor, config, clock.clock(), executor).unwrap();
    if start {
        driver.run_turn(TurnBudget::default());
    }
    Rig { driver, log, refused, actor: spawn.address, life: spawn.lifecycle, clock, jobs }
}

fn rig(messages: usize) -> Rig {
    rig_with(Recorder::default(), limits(messages).into(), true)
}

impl Rig {
    fn turn(&mut self) -> TurnReport {
        self.driver.run_turn(TurnBudget::default())
    }

    fn seen(&mut self) -> Vec<u32> {
        let mut req = pin!(self.actor.request(None, Msg::Get));
        assert!(poll(req.as_mut()).is_pending());
        self.driver.run_turn(TurnBudget::default());
        match poll(req.as_mut()) {
            Poll::Ready(Ok(v)) => v,
            other => panic!("unexpected {:?}", other.map(|r| r.map(|_| ()))),
        }
    }
}

#[test]
fn admission_is_gated_bounded_and_returns_the_payload() {
    let mut r = rig_with(Recorder::default(), limits(2).into(), false);
    // Admitted before `on_start`, handled after it, in the same turn.
    r.actor.try_tell(Msg::Push(0)).unwrap();
    assert_eq!(r.turn().ordinary, 1);
    assert_eq!(r.actor.phase(), Phase::Running);
    r.actor.try_tell(Msg::Push(1)).unwrap();
    r.actor.try_tell(Msg::Push(2)).unwrap();
    let reject = r.actor.try_tell(Msg::Push(3)).unwrap_err();
    assert_eq!(reject.reason, RejectReason::Full);
    assert!(matches!(reject.message, Msg::Push(3)));
    assert_eq!(r.turn().ordinary, 2);
    assert_eq!(r.seen(), [0, 1, 2]);
    let d = r.actor.diagnostics();
    assert_eq!((d.admitted, d.rejected[RejectReason::Full as usize], d.mailbox_depth, d.mailbox_bytes), (4, 1, 0, 0));
}

#[test]
fn oversized_messages_and_custom_weights() {
    let config = MailboxConfig::from(MailboxLimits { max_message_bytes: 10, ..limits(4) })
        .with_weight(|m| if let Msg::Push(v) = m { *v as usize } else { 1 });
    let r = rig_with(Recorder::default(), config, true);
    assert_eq!(r.actor.try_tell(Msg::Push(11)).unwrap_err().reason, RejectReason::TooLarge);
    r.actor.try_tell(Msg::Push(10)).unwrap();
    assert_eq!(r.actor.diagnostics().mailbox_bytes, 10);
}

#[test]
fn permits_reserve_commit_and_release_on_drop() {
    let mut r = rig(1);
    let Poll::Ready(Ok(permit)) = poll(pin!(r.actor.reserve(64, None))) else { panic!() };
    assert_eq!(r.actor.try_tell(Msg::Push(0)).unwrap_err().reason, RejectReason::Full);
    assert_eq!(r.actor.diagnostics().reservations, 1);
    drop(permit);
    assert_eq!(r.actor.diagnostics().reservations, 0);
    let Poll::Ready(Ok(permit)) = poll(pin!(r.actor.reserve(64, None))) else { panic!() };
    permit.commit(Msg::Push(5)).unwrap();
    r.turn();
    assert_eq!(r.seen(), [5]);

    let Poll::Ready(Ok(small)) = poll(pin!(r.actor.reserve(1, None))) else { panic!() };
    assert_eq!(small.commit(Msg::Push(6)).unwrap_err().reason, RejectReason::TooLarge);
    let d = r.actor.diagnostics();
    assert_eq!((d.mailbox_bytes, d.reservations), (0, 0));

    let too_large = d.rejected[RejectReason::TooLarge as usize];
    assert!(matches!(poll(pin!(r.actor.reserve(usize::MAX, None))), Poll::Ready(Err(RejectReason::TooLarge))));
    assert_eq!(r.actor.diagnostics().rejected[RejectReason::TooLarge as usize], too_large + 1);
}

#[test]
fn waiters_are_fifo_and_immediate_sends_cannot_bypass_them() {
    let mut r = rig(1);
    r.actor.try_tell(Msg::Push(1)).unwrap();
    let mut first = pin!(r.actor.send(Msg::Push(2), None));
    let mut second = pin!(r.actor.send(Msg::Push(3), None));
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    r.turn();
    assert_eq!(r.actor.try_tell(Msg::Push(9)).unwrap_err().reason, RejectReason::Full);
    assert!(poll(second.as_mut()).is_pending(), "second waiter must not jump the first");
    assert!(poll(first.as_mut()).is_ready());
    r.turn();
    let mut third = pin!(r.actor.send(Msg::Push(4), None));
    assert!(poll(third.as_mut()).is_pending(), "a new send took the room the waiter was woken for");
    assert!(poll(second.as_mut()).is_ready());
    r.turn();
    assert!(poll(third.as_mut()).is_ready());
    r.turn();
    assert_eq!(r.seen(), [1, 2, 3, 4]);
}

#[test]
fn a_send_with_room_takes_it_at_once_and_counts_no_refusal() {
    let mut r = rig(1);
    assert!(matches!(poll(pin!(r.actor.send(Msg::Push(1), None))), Poll::Ready(Ok(_))));
    let mut second = pin!(r.actor.send(Msg::Push(2), None));
    assert!(poll(second.as_mut()).is_pending());
    r.turn();
    assert!(matches!(poll(second.as_mut()), Poll::Ready(Ok(_))));
    r.turn();
    let d = r.actor.diagnostics();
    assert_eq!((d.rejected, d.waiters, d.reservations), ([0; 6], 0, 0));
    // Admission is lazy: a send never polled takes nothing.
    drop(r.actor.send(Msg::Push(3), None));
    assert_eq!(r.seen(), [1, 2]);
    assert_eq!(r.actor.diagnostics().admitted, 3, "the two sends and the ask that read them");
}

#[test]
fn waiter_limit_deadline_and_cancellation_leave_nothing_behind() {
    let r = rig_with(Recorder::default(), MailboxLimits { waiters: 1, ..limits(1) }.into(), true);
    r.actor.try_tell(Msg::Push(1)).unwrap();
    let deadline = Some(Duration::from_secs(1));
    let mut waiting = pin!(r.actor.send(Msg::Push(2), deadline));
    assert!(poll(waiting.as_mut()).is_pending());
    let Poll::Ready(Err(limit)) = poll(pin!(r.actor.send(Msg::Push(3), None))) else { panic!() };
    assert_eq!(limit.reason, RejectReason::WaiterLimit);
    assert_eq!(r.clock.clock().pending_wakes(), 1);

    r.clock.advance(Duration::from_secs(1));
    let Poll::Ready(Err(expired)) = poll(waiting.as_mut()) else { panic!() };
    assert_eq!(expired.reason, RejectReason::Deadline);
    assert!(matches!(expired.message, Msg::Push(2)));

    {
        let mut dropped = pin!(r.actor.send(Msg::Push(4), Some(Duration::from_secs(9))));
        assert!(poll(dropped.as_mut()).is_pending());
    }
    let d = r.actor.diagnostics();
    assert_eq!((d.waiters, r.clock.clock().pending_wakes()), (0, 0));
}

#[test]
fn requests_distinguish_every_outcome() {
    let mut r = rig_with(Recorder::default(), MailboxLimits { requests: 2, ..limits(8) }.into(), true);

    let mut held = pin!(r.actor.request(Some(Duration::from_secs(5)), Msg::Hold));
    assert!(poll(held.as_mut()).is_pending());
    r.turn();
    r.clock.advance(Duration::from_secs(5));
    assert_eq!(poll(held.as_mut()), Poll::Ready(Err(RequestError::Deadline { admitted: true })));

    let mut second = pin!(r.actor.request(None, Msg::Hold));
    let mut third = pin!(r.actor.request::<()>(None, Msg::Hold));
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(poll(third.as_mut()), Poll::Ready(Err(RequestError::RequestLimit)));

    r.life.stop();
    r.turn();
    assert_eq!(poll(second.as_mut()), Poll::Ready(Err(RequestError::ReplyDropped { admitted: true })));
    let r = rig(4);
    r.life.stop();
    let closed = poll(pin!(r.actor.request::<()>(None, Msg::Hold)));
    assert_eq!(closed, Poll::Ready(Err(RequestError::NotAdmitted(RejectReason::Closed))));

    // A new waker moves the deadline's wake rather than adding one, and a request dropped unanswered takes it away.
    struct Other(AtomicUsize);
    impl std::task::Wake for Other {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (mut r, other) = (rig(4), Waker::from(Arc::new(Other(AtomicUsize::new(0)))));
    {
        let mut asked = pin!(r.actor.request(Some(Duration::from_secs(5)), Msg::Get));
        assert!(poll(asked.as_mut()).is_pending());
        assert!(asked.as_mut().poll(&mut Cx::from_waker(&other)).is_pending());
        assert_eq!(r.clock.clock().pending_wakes(), 1);
    }
    assert_eq!(r.clock.clock().pending_wakes(), 0);

    // On a manual clock an ask never waits on its thread: its first poll arms its one wake, and an answer takes it.
    {
        let mut asked = pin!(r.actor.request(Some(r.clock.clock().now() + Duration::from_secs(5)), Msg::Get));
        assert!(poll(asked.as_mut()).is_pending());
        assert_eq!(r.clock.clock().pending_wakes(), 1);
        r.turn();
        assert!(matches!(poll(asked.as_mut()), Poll::Ready(Ok(_))));
    }
    let d = r.actor.diagnostics();
    assert_eq!((r.clock.clock().pending_wakes(), d.requests), (0, 0));

    // A manual clock fires a wake only when it moves, so a deadline already past ends the request at its poll.
    let late = poll(pin!(r.actor.request(Some(r.clock.clock().now()), Msg::Hold)));
    assert_eq!(late, Poll::Ready(Err(RequestError::Deadline { admitted: true })));
    assert_eq!(r.clock.clock().pending_wakes(), 0);
}

/// A request whose deadline passes while it waits for room, then is admitted, ends at that poll: on a manual clock
/// nothing would fire a wake armed at a past time until the clock moves again.
#[test]
fn a_request_admitted_after_its_deadline_ends_at_once_on_a_manual_clock() {
    let mut r = rig(1);
    r.actor.try_tell(Msg::Push(1)).unwrap();
    let deadline = r.clock.clock().now() + Duration::from_secs(1);
    let mut late = pin!(r.actor.request(Some(deadline), Msg::Hold));
    assert!(poll(late.as_mut()).is_pending(), "waits for room");
    r.clock.advance(Duration::from_secs(2));
    r.turn();
    assert_eq!(poll(late.as_mut()), Poll::Ready(Err(RequestError::Deadline { admitted: true })));
    assert_eq!(r.clock.clock().pending_wakes(), 0);
}

#[test]
fn a_late_reply_is_handed_back() {
    struct Keeper(Option<ReplyTo<u32>>);
    impl Actor for Keeper {
        type Message = Option<ReplyTo<u32>>;
        type Error = ();
        fn on_message(&mut self, msg: Self::Message, _: &mut Context<'_, Self>) -> Result<(), ()> {
            match msg {
                Some(reply) => self.0 = Some(reply),
                None => assert_eq!(self.0.take().unwrap().reply(1), Err(1)),
            }
            Ok(())
        }
    }
    let clock = Clock::manual();
    let (mut driver, spawn) = EmbeddedDriver::new(Keeper(None), limits(4), clock.clock()).unwrap();
    driver.run_turn(TurnBudget::default());
    {
        let mut req = pin!(spawn.address.request(None, Some));
        assert!(poll(req.as_mut()).is_pending());
        driver.run_turn(TurnBudget::default());
    }
    spawn.address.try_tell(None).unwrap();
    assert!(!driver.run_turn(TurnBudget::default()).stopped);
}

#[test]
fn timers_fire_in_order_repeat_and_cancel() {
    let mut r = rig(8);
    r.actor.try_tell(Msg::Timer(2)).unwrap();
    r.actor.try_tell(Msg::Timer(1)).unwrap();
    assert_eq!(r.turn().next_timer, Some(Duration::from_secs(1)));
    assert_eq!(r.actor.diagnostics().timers, 2);
    r.clock.advance(Duration::from_secs(2));
    assert_eq!(r.turn().timers, 2);
    assert_eq!(r.seen(), [1, 2]);

    r.actor.try_tell(Msg::Every).unwrap();
    r.turn();
    for _ in 0..3 {
        r.clock.advance(Duration::from_secs(1));
        r.turn();
    }
    assert_eq!(r.seen(), [1, 2, 7, 7, 7]);
    r.actor.try_tell(Msg::CancelLast(TimerCancel::Cancelled)).unwrap();
    r.actor.try_tell(Msg::CancelLast(TimerCancel::AlreadyDispatched)).unwrap();
    r.turn();
    r.clock.advance(Duration::from_secs(5));
    assert_eq!((r.turn().timers, r.clock.clock().pending_wakes()), (0, 0));
    assert!(!r.turn().stopped);
}

#[test]
fn jobs_complete_and_a_panicking_one_ends_the_actor_as_panicked() {
    let mut r = rig_with(Recorder::default(), MailboxLimits { jobs: 1, ..limits(8) }.into(), true);
    r.actor.try_tell(Msg::Job(1)).unwrap();
    r.turn();
    assert_eq!((r.jobs.pending(), r.actor.diagnostics().jobs), (1, 1));
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().completions, 1);
    assert_eq!(r.seen(), [1]);

    for panics in [Msg::JobPanics, Msg::FuturePanics] {
        let mut r = rig_with(Recorder::default(), limits(8).into(), true);
        r.actor.try_tell(panics).unwrap();
        r.turn();
        r.jobs.run_next();
        assert!(r.turn().stopped);
        assert_eq!(r.life.exit().unwrap().reason, StopReason::Panicked);
    }
}

/// The pause a keyed actor loading its state needs: nothing queued behind the load is handled before its result,
/// the bound mailbox holds senders back, and a pause longer than the idle time does not drain the actor.
#[test]
fn a_paused_actor_handles_its_job_result_before_any_message_queued_meanwhile() {
    // Its idle time is past the pause, so the pause alone does not drain it.
    let mut r = rig_with(Recorder::default(), idle(3), true);
    r.actor.try_tell(Msg::PauseOn(1)).unwrap();
    r.turn();
    r.clock.advance(Duration::from_secs(60));
    assert!(!r.turn().stopped);
    for v in [2, 3, 4] {
        r.actor.try_tell(Msg::Push(v)).unwrap();
    }
    assert_eq!(r.actor.try_tell(Msg::Push(5)).unwrap_err().reason, RejectReason::Full);
    assert_eq!(r.turn().ordinary, 0);
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().completions, 1);
    assert_eq!(r.seen(), [1, 2, 3, 4]);

    // Paused mid-batch: the rest of the batch waits.
    [Msg::PauseOn(5), Msg::Push(6)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    let report = r.turn();
    assert_eq!((report.ordinary, report.runnable), (1, false));
    assert!(r.jobs.run_next());
    r.turn();
    assert_eq!(r.seen(), [1, 2, 3, 4, 5, 6]);
}

/// A pause holds everything but its own job: one change at a time, with no queue in the actor.
#[test]
fn a_paused_actor_holds_other_job_results_until_its_own_is_handled() {
    let mut r = rig(8);
    [Msg::Job(1), Msg::PauseOn(2)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().completions, 0, "another job's result waits for the pause");
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().completions, 2);
    assert_eq!(r.seen(), [2, 1]);

    // A held result whose handler pauses again: what is held behind it waits for that pause too.
    let mut r = rig(8);
    let again = Msg::JobThen(Box::new(Msg::PauseOn(4)));
    [again, Msg::Job(5), Msg::PauseOn(3)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    while r.jobs.run_next() {}
    assert_eq!(r.turn().completions, 2, "3, then the one that pauses on 4; 5 waits");
    assert_eq!(r.jobs.pending(), 1);
    assert!(r.jobs.run_next());
    assert_eq!(r.driver.run_until_idle(4), Ok(1));
    assert_eq!(r.seen(), [3, 4, 5]);

    // A held result keeps its job's slot until it is handled: the job limit bounds what a pause holds.
    let mut r = rig_with(Recorder::default(), MailboxLimits { jobs: 2, ..limits(8) }.into(), true);
    let refused = r.refused.clone();
    let timer = Msg::After(1, Box::new(Msg::TryJob(3)));
    [timer, Msg::Job(1), Msg::PauseOn(2)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    assert!(r.jobs.run_next());
    r.clock.advance(Duration::from_secs(1));
    assert_eq!(r.turn().timers, 1, "a timer reaches a paused actor");
    assert_eq!(*refused.lock().unwrap(), [(3, TaskAdmissionError::Full)]);
    assert!(r.jobs.run_next());
    r.turn();
    r.actor.try_tell(Msg::TryJob(4)).unwrap();
    r.turn();
    assert!(r.jobs.run_next());
    r.turn();
    assert_eq!((r.seen(), refused.lock().unwrap().len()), (vec![2, 1, 4], 1));

    // A timer pauses on a job whose result the pause already holds: that result is owed a turn, and goes first.
    let mut r = rig(8);
    let log = r.log.clone();
    let timer = Msg::After(1, Box::new(Msg::PauseOnLast));
    [timer, Msg::Job(1), Msg::PauseOn(2)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    assert!(r.jobs.run_next());
    r.turn();
    r.clock.advance(Duration::from_secs(1));
    assert_eq!(r.turn().timers, 1);
    // The settled flag is what makes it owed a turn; run_until_idle would run job 2 as well.
    assert_eq!(r.turn().completions, 1, "the held result it paused on is taken by the next turn");
    assert_eq!(*log.lock().unwrap(), [1]);
    assert!(r.jobs.run_next());
    r.turn();
    assert_eq!(r.seen(), [1, 2]);
}

#[test]
fn a_stop_or_drain_during_a_pause_counts_or_handles_what_was_held() {
    for end in ["stop", "drain", "deadline"] {
        let mut r = rig(8);
        let log = r.log.clone();
        [Msg::Job(1), Msg::PauseOn(2), Msg::Push(3)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
        r.turn();
        assert!(r.jobs.run_next());
        r.turn();
        match end {
            "stop" => r.life.stop(),
            "drain" => r.life.drain(None),
            _ => r.life.drain(Some(Duration::from_secs(3))),
        }
        if end != "stop" {
            assert!(!r.turn().stopped, "{end}: the drain waits for the pausing job");
        }
        match end {
            "drain" => assert!(r.jobs.run_next()),
            "deadline" => r.clock.advance(Duration::from_secs(3)),
            _ => {}
        }
        assert!(r.turn().stopped, "{end}");
        let exit = r.life.exit().unwrap();
        let expected = match end {
            "stop" => (StopReason::Stopped, 2, 1, vec![]),
            "drain" => (StopReason::Drained, 0, 0, vec![2, 1, 3]),
            _ => (StopReason::DrainDeadline, 2, 1, vec![]),
        };
        let got = (exit.reason.clone(), exit.discarded_messages(), exit.pending_jobs, log.lock().unwrap().clone());
        assert_eq!(got, expected, "{end}: the held result and the message are counted or handled");
        assert_eq!(r.driver.run_until_idle(4).map_err(drop), Ok(0), "{end}: nothing owed after");
    }
}

#[test]
fn a_pause_taken_in_on_start_is_part_of_the_start() {
    let stops = Arc::new(AtomicUsize::new(0));
    let actor = Recorder { load: Some(|| Msg::Done(1)), stops: stops.clone(), ..Default::default() };
    let mut r = rig_with(actor, limits(4).into(), true);
    let spawn = SpawnHandle { address: r.actor.clone(), lifecycle: r.life.clone() };
    assert!(matches!(spawn.wait_ready(Duration::ZERO), Err(StartError::TimedOut)));
    assert_eq!(r.actor.phase(), Phase::Starting);
    r.actor.try_tell(Msg::Push(2)).unwrap();
    assert!(r.jobs.run_next());
    r.turn();
    assert!(spawn.wait_ready(Duration::ZERO).is_ok());
    assert_eq!((r.actor.phase(), r.seen()), (Phase::Running, vec![1, 2]));

    // The load's handler fails: the start fails with its reason, and on_stop runs, as on_start returned Ok.
    let stops = Arc::new(AtomicUsize::new(0));
    let actor = Recorder { load: Some(|| Msg::Fail), stops: stops.clone(), ..Default::default() };
    let mut r = rig_with(actor, limits(4).into(), true);
    let spawn = SpawnHandle { address: r.actor.clone(), lifecycle: r.life.clone() };
    assert!(r.jobs.run_next());
    assert!(r.turn().stopped);
    let failed = StartError::Failed(StopReason::Failed("\"boom\"".into()));
    assert_eq!(spawn.wait_ready(Duration::ZERO).map(drop), Err(failed));
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    // A drain asked during the start's pause still admits what the load's handler sets up.
    let actor = Recorder { load: Some(|| Msg::Job(5)), ..Default::default() };
    let mut r = rig_with(actor, limits(4).into(), true);
    let log = r.log.clone();
    r.life.drain(None);
    assert!(!r.turn().stopped);
    assert!(r.jobs.run_next());
    assert!(!r.turn().stopped, "the job the load's handler submitted was admitted");
    assert!(r.jobs.run_next());
    assert!(r.turn().stopped);
    assert_eq!((r.life.exit().unwrap().reason, log.lock().unwrap().clone()), (StopReason::Drained, vec![5]));
}

#[test]
fn a_job_on_an_executor_of_its_own_shares_the_actors_limit_cancel_pause_and_drain() {
    let (other, theirs) = JobExecutor::manual(8);
    let actor = Recorder { other: Some(other), ..Default::default() };
    let mut r = rig_with(actor, MailboxLimits { jobs: 1, ..limits(8) }.into(), true);
    let (refused, log) = (r.refused.clone(), r.log.clone());
    [Msg::JobOther(1), Msg::TryJob(2)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    assert_eq!((theirs.pending(), r.jobs.pending()), (1, 0), "it runs on the executor it was given");
    assert_eq!(*refused.lock().unwrap(), [(2, TaskAdmissionError::Full)], "under the actor's one job limit");

    r.actor.try_tell(Msg::CancelLastJob).unwrap();
    r.turn();
    assert!(theirs.run_next());
    r.turn();
    assert_eq!((log.lock().unwrap().len(), r.actor.diagnostics().jobs), (0, 0), "a cancel suppresses its result");

    [Msg::PauseOnOther(3), Msg::Push(4)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    assert_eq!(r.turn().ordinary, 0, "a pause on it holds");
    assert!(theirs.run_next());
    r.turn();
    assert_eq!(r.seen(), [3, 4]);

    r.actor.try_tell(Msg::JobOther(5)).unwrap();
    r.turn();
    r.life.drain(None);
    assert!(!r.turn().stopped, "a drain waits for it");
    assert!(theirs.run_next());
    assert!(r.turn().stopped);
    assert_eq!((r.life.exit().unwrap().reason, log.lock().unwrap().clone()), (StopReason::Drained, vec![3, 4, 5]));
}

#[test]
fn a_job_its_executor_drops_ends_its_actor_and_a_cancelled_one_does_not() {
    let lost = StopReason::Failed("a job was dropped by its executor".into());
    let (other, lose) = losing();
    let mut r = rig_with(Recorder { other: Some(other), ..Default::default() }, limits(8).into(), true);
    [Msg::PauseOnOther(1), Msg::Push(2)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    lose();
    assert!(r.turn().stopped, "the pause on it does not hang");
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason.clone(), exit.pending_jobs, exit.discarded_messages()), (lost.clone(), 0, 1));
    assert_eq!(r.actor.diagnostics().jobs, 0);

    // Cancelled before its executor dropped it: the actor goes on, and the pause lifts.
    let (other, lose) = losing();
    let mut r = rig_with(Recorder { other: Some(other), ..Default::default() }, limits(8).into(), true);
    let cancel = Msg::After(1, Box::new(Msg::CancelLastJob));
    [cancel, Msg::PauseOnOther(1), Msg::Push(2)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    r.clock.advance(Duration::from_secs(1));
    r.turn();
    lose();
    assert!(!r.turn().stopped);
    assert_eq!((r.seen(), r.actor.diagnostics().jobs), (vec![2], 0));

    // Dropped as it is handed over, before the handler has its handle.
    let dropping = JobExecutor::custom(|job| {
        drop(job);
        Ok(())
    });
    let mut r = rig_with(Recorder { other: Some(dropping), ..Default::default() }, limits(8).into(), true);
    r.actor.try_tell(Msg::JobOther(1)).unwrap();
    r.turn();
    assert!(r.turn().stopped);
    assert_eq!((r.life.exit().unwrap().reason, r.actor.diagnostics().jobs), (lost, 0));
}

/// A source's end travels behind its items, through the mailbox: a cancel takes back nothing already sent.
#[test]
fn a_source_end_sent_before_its_cancel_still_arrives_and_nothing_after_it() {
    let mut r = rig(8);
    let tx = attached(&mut r, 8);
    r.actor.try_tell(Msg::CancelLastJob).unwrap();
    drop(tx);
    assert!(r.jobs.run_next(), "the source ends, its end queued behind the cancel");
    r.turn();
    assert_eq!((r.seen(), r.actor.diagnostics().jobs), (vec![99], 0));

    // Cancelled before it ran: it sends nothing, not even its end.
    let tx = attached(&mut r, 8);
    r.actor.try_tell(Msg::CancelLastJob).unwrap();
    r.turn();
    drop(tx);
    while r.jobs.run_next() {}
    r.turn();
    assert_eq!((r.seen(), r.actor.diagnostics().jobs), (vec![99], 0));
}

enum Parked {
    Put(u32),
    Ask(ReplyTo<Vec<u32>>),
    Open,
    /// Opens, and unstashes in `on_turn_end`.
    OpenLater,
    Close,
    /// Pauses on a job that answers `Put(2)`, then opens.
    PauseThenOpen,
    Fail,
}

/// Stashes what it is told while closed and replays it once opened. `again` is stashed a second time as it replays;
/// with `slow`, each `Put` it handles takes 10 ms.
#[derive(Default)]
struct Stasher {
    open: bool,
    release: bool,
    again: Option<u32>,
    slow: Option<ManualClock>,
    seen: Arc<std::sync::Mutex<Vec<u32>>>,
    refused: Arc<std::sync::Mutex<Vec<u32>>>,
}

impl Actor for Stasher {
    type Message = Parked;
    type Error = String;

    fn on_message(&mut self, msg: Parked, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Parked::Put(v) if self.open && self.again.take_if(|a| *a == v).is_none() => {
                self.slow.iter().for_each(|clock| clock.advance(Duration::from_millis(10)));
                self.seen.lock().unwrap().push(v)
            }
            Parked::Ask(reply) if self.open => drop(reply.reply(self.seen.lock().unwrap().clone())),
            Parked::Open => {
                self.open = true;
                cx.unstash_all();
            }
            Parked::OpenLater => (self.open, self.release) = (true, true),
            Parked::Close => self.open = false,
            Parked::PauseThenOpen => {
                let job = cx.submit_io(|| Parked::Put(2)).map_err(|e| format!("{e:?}"))?;
                cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
                self.open = true;
                cx.unstash_all();
            }
            Parked::Fail => return Err("boom".into()),
            held => {
                if let Err(Parked::Put(v)) = cx.stash(held) {
                    self.refused.lock().unwrap().push(v);
                }
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if std::mem::take(&mut self.release) {
            cx.unstash_all();
        }
        Ok(())
    }
}

#[test]
fn stashed_messages_replay_in_stash_order_before_newer_mailbox_messages_and_the_stash_is_bounded() {
    let actor = Stasher { again: Some(2), ..Default::default() };
    let (seen, refused) = (actor.seen.clone(), actor.refused.clone());
    let (mut driver, spawn) = EmbeddedDriver::new(actor, limits(3), Clock::manual().clock()).unwrap();
    let tell = |msg| {
        spawn.address.try_tell(msg).map_err(drop).expect("room");
    };
    driver.run_turn(TurnBudget::default());
    (1..=3).for_each(|v| tell(Parked::Put(v)));
    driver.run_turn(TurnBudget::default());
    assert_eq!(spawn.address.diagnostics().mailbox_depth, 0, "a stash is not the mailbox: its room is back");
    tell(Parked::Put(4));
    driver.run_turn(TurnBudget::default());
    assert_eq!(*refused.lock().unwrap(), [4], "the stash holds as many as the mailbox and hands the next one back");

    // Opened mid-batch: the rest of the batch waits for the replay, which keeps to the turn budget.
    [Parked::Open, Parked::Put(5), Parked::Put(6)].into_iter().for_each(tell);
    let report = driver.run_turn(TurnBudget::default());
    assert_eq!((report.ordinary, report.runnable), (1, true));
    let report = driver.run_turn(TurnBudget::messages(2));
    assert_eq!((report.ordinary, report.runnable), (2, true));
    assert_eq!(*seen.lock().unwrap(), [1]);
    assert_eq!(driver.run_until_idle(4), Ok(1));
    assert_eq!(*seen.lock().unwrap(), [1, 3, 5, 6], "2, stashed again as it replayed, went to the back");

    [Parked::Close, Parked::Put(7), Parked::Open].into_iter().for_each(tell);
    driver.run_until_idle(4).unwrap();
    assert_eq!(*seen.lock().unwrap(), [1, 3, 5, 6, 2, 7]);
}

#[test]
fn what_is_still_stashed_when_the_actor_ends_is_counted_and_its_asks_are_answered() {
    for end in ["stop", "fail", "drain", "drop"] {
        let (mut driver, spawn) = EmbeddedDriver::new(Stasher::default(), limits(8), Clock::manual().clock()).unwrap();
        driver.run_turn(TurnBudget::default());
        spawn.address.try_tell(Parked::Put(1)).map_err(drop).unwrap();
        let mut asked = pin!(spawn.address.request(None, Parked::Ask));
        assert!(poll(asked.as_mut()).is_pending());
        driver.run_turn(TurnBudget::default());
        assert!(poll(asked.as_mut()).is_pending(), "{end}: stashed, not answered");
        match end {
            "stop" => spawn.lifecycle.stop(),
            "fail" => {
                spawn.address.try_tell(Parked::Fail).map_err(drop).unwrap();
            }
            "drain" => spawn.lifecycle.drain(None),
            _ => {}
        }
        if end == "drop" {
            drop(driver);
        } else {
            assert!(driver.run_turn(TurnBudget::default()).stopped, "{end}: a drain does not wait for the stash");
        }
        assert_eq!(spawn.lifecycle.exit().unwrap().discarded_messages(), 2, "{end}");
        assert_eq!(poll(asked.as_mut()), Poll::Ready(Err(RequestError::ReplyDropped { admitted: true })), "{end}");
    }

    // Released, the ask is answered, and a drain waits for what was released, from on_turn_end too.
    let (mut driver, spawn) = EmbeddedDriver::new(Stasher::default(), limits(8), Clock::manual().clock()).unwrap();
    driver.run_turn(TurnBudget::default());
    let mut asked = pin!(spawn.address.request(None, Parked::Ask));
    assert!(poll(asked.as_mut()).is_pending());
    driver.run_turn(TurnBudget::default());
    spawn.address.try_tell(Parked::OpenLater).map_err(drop).unwrap();
    spawn.lifecycle.drain(None);
    assert!(!driver.run_turn(TurnBudget::messages(1)).stopped, "the drain waits for the replay");
    assert!(driver.run_turn(TurnBudget::default()).stopped);
    assert_eq!(poll(asked.as_mut()), Poll::Ready(Ok(Vec::new())));
    let exit = spawn.lifecycle.exit().unwrap();
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Drained, 0));
}

#[test]
fn a_paused_actor_replays_its_stash_only_once_the_pause_lifts() {
    let (executor, jobs) = JobExecutor::manual(8);
    let actor = Stasher::default();
    let seen = actor.seen.clone();
    let (mut driver, spawn) = EmbeddedDriver::with_jobs(actor, limits(8), Clock::manual().clock(), executor).unwrap();
    driver.run_turn(TurnBudget::default());
    spawn.address.try_tell(Parked::Put(1)).map_err(drop).unwrap();
    assert_eq!(driver.run_until_idle(4), Ok(1));
    assert_eq!(driver.run_until_idle(4), Ok(0), "nothing is owed for what is only stashed");
    spawn.address.try_tell(Parked::PauseThenOpen).map_err(drop).unwrap();
    let report = driver.run_turn(TurnBudget::default());
    assert_eq!((report.ordinary, report.runnable), (1, false), "released, but the pause holds the replay");
    assert_eq!(driver.run_turn(TurnBudget::default()).ordinary, 0);
    assert!(seen.lock().unwrap().is_empty());
    assert!(jobs.run_next());
    driver.run_until_idle(4).unwrap();
    assert_eq!(*seen.lock().unwrap(), [2, 1], "the job's result first, as a pause has it, then the replay");
}

#[test]
fn a_replay_shares_the_turn_budget_of_messages_and_time_with_the_batch_after_it() {
    let actor = Stasher::default();
    let seen = actor.seen.clone();
    let (mut driver, spawn) = EmbeddedDriver::new(actor, limits(16), Clock::manual().clock()).unwrap();
    let tell = |msg| {
        spawn.address.try_tell(msg).map_err(drop).expect("room");
    };
    driver.run_turn(TurnBudget::default());
    (1..=3).for_each(|v| tell(Parked::Put(v)));
    driver.run_turn(TurnBudget::default());
    [Parked::Open].into_iter().chain((10..=15).map(Parked::Put)).for_each(tell);
    assert_eq!(driver.run_turn(TurnBudget::default()).ordinary, 1);
    let report = driver.run_turn(TurnBudget::messages(4));
    assert_eq!((report.ordinary, report.runnable), (4, true));
    assert_eq!(*seen.lock().unwrap(), [1, 2, 3, 10]);
    driver.run_until_idle(4).unwrap();
    assert_eq!(*seen.lock().unwrap(), [1, 2, 3, 10, 11, 12, 13, 14, 15]);

    // As a batch, a replay hands out one message before the time can end the turn, and one that ends it leaves the
    // mailbox for the next turn.
    let shared = Clock::manual();
    let handle = shared.clock();
    let actor = Stasher { slow: Some(shared), ..Default::default() };
    let seen = actor.seen.clone();
    let (mut driver, spawn) = EmbeddedDriver::new(actor, limits(16), handle).unwrap();
    let tell = |msg| {
        spawn.address.try_tell(msg).map_err(drop).expect("room");
    };
    driver.run_turn(TurnBudget::default());
    (1..=2).for_each(|v| tell(Parked::Put(v)));
    driver.run_turn(TurnBudget::default());
    [Parked::Open, Parked::Put(4)].into_iter().for_each(tell);
    assert_eq!(driver.run_turn(TurnBudget::default()).ordinary, 1);
    let budget = TurnBudget { max_elapsed: Some(Duration::from_millis(5)), ..TurnBudget::default() };
    for expected in [&[1][..], &[1, 2], &[1, 2, 4]] {
        let report = driver.run_turn(budget);
        assert_eq!((report.ordinary, report.budget_overrun, report.runnable), (1, true, expected.len() < 3));
        assert_eq!(*seen.lock().unwrap(), expected);
    }
}

#[test]
fn the_stash_holds_as_many_bytes_as_the_mailbox_may() {
    let config = MailboxConfig::from(MailboxLimits { bytes: 10, max_message_bytes: 10, ..limits(8) })
        .with_weight(|m| if let Parked::Put(v) = m { *v as usize } else { 1 });
    let actor = Stasher::default();
    let (seen, refused) = (actor.seen.clone(), actor.refused.clone());
    let (mut driver, spawn) = EmbeddedDriver::new(actor, config, Clock::manual().clock()).unwrap();
    driver.run_turn(TurnBudget::default());
    let mut tell = |msg| {
        spawn.address.try_tell(msg).map_err(drop).expect("the mailbox has room for one");
        driver.run_turn(TurnBudget::default());
    };
    [4, 5, 2, 1].map(Parked::Put).into_iter().for_each(&mut tell);
    assert_eq!(*refused.lock().unwrap(), [2], "4 + 5 + 2 is past the mailbox's 10 bytes");
    // The replay runs before the close, and frees its bytes for the next stash.
    [Parked::Open, Parked::Close, Parked::Put(10), Parked::Open].into_iter().for_each(tell);
    driver.run_until_idle(4).unwrap();
    assert_eq!(*seen.lock().unwrap(), [4, 5, 1, 10]);
}

#[test]
fn an_on_batch_that_unstashes_before_it_takes_a_message_replays_and_keeps_running() {
    /// Stashes the first message, and unstashes at the start of every batch after it.
    #[derive(Default)]
    struct Eager(bool, Vec<u32>);
    impl Actor for Eager {
        type Message = u32;
        type Error = ();
        fn on_message(&mut self, v: u32, _: &mut Context<'_, Self>) -> Result<(), ()> {
            self.1.push(v);
            Ok(())
        }
        fn on_batch(&mut self, batch: &mut MailboxBatch<'_, u32>, cx: &mut Context<'_, Self>) -> Result<(), ()> {
            if self.0 {
                cx.unstash_all();
            }
            for v in batch.by_ref() {
                match std::mem::replace(&mut self.0, true) {
                    false => cx.stash(v).map_err(drop)?,
                    true => self.1.push(v),
                }
            }
            Ok(())
        }
        fn on_stop(&mut self, _: &StopReason) {
            assert_eq!(self.1, [1, 2], "the stashed one, then the one behind it");
        }
    }
    let (mut driver, spawn) = EmbeddedDriver::new(Eager::default(), limits(8), Clock::manual().clock()).unwrap();
    driver.run_turn(TurnBudget::default());
    spawn.address.try_tell(1u32).unwrap();
    driver.run_turn(TurnBudget::default());
    spawn.address.try_tell(2u32).unwrap();
    let report = driver.run_turn(TurnBudget::default());
    assert_eq!((report.ordinary, report.runnable, report.stopped), (0, true, false), "not a batch that took nothing");
    driver.run_until_idle(4).unwrap();
    spawn.lifecycle.stop();
    driver.run_turn(TurnBudget::default());
    assert_eq!(spawn.lifecycle.exit().unwrap().reason, StopReason::Stopped);
}

#[test]
fn a_replayed_message_keeps_its_trace_and_counts_as_activity() {
    let config = MailboxConfig::<Parked> {
        tracer: Some(Tracer::new(64, 0)),
        // Short, so a replay spread over turns crosses it.
        idle: Some(Duration::from_secs(1)),
        ..MailboxConfig::from(limits(8))
    };
    let tracer = config.tracer.clone().unwrap();
    let shared = Clock::manual();
    let handle = shared.clock();
    let (mut driver, spawn) = EmbeddedDriver::new(Stasher::default(), config, handle).unwrap();
    driver.run_turn(TurnBudget::default());
    let root = TraceContext::root();
    root.enter(|| spawn.address.try_tell(Parked::Put(1)).map_err(drop).unwrap());
    for v in 2..=3 {
        spawn.address.try_tell(Parked::Put(v)).map_err(drop).unwrap();
    }
    driver.run_turn(TurnBudget::default());
    spawn.address.try_tell(Parked::Open).map_err(drop).unwrap();
    driver.run_turn(TurnBudget::default());
    // Each replayed message is activity: past the idle time since the last message, the rest of the replay still runs.
    for _ in 0..3 {
        shared.advance(Duration::from_millis(600));
        driver.run_turn(TurnBudget::messages(1));
    }
    driver.run_until_idle(4).unwrap();
    assert_eq!(spawn.lifecycle.exit(), None, "no idle drain while a replay runs");
    let spans = tracer.trace(root.trace.get());
    assert_eq!(spans.len(), 2, "stashed, then replayed: {spans:?}");
    assert_eq!(spans[1].parent, spans[0].span, "the replay is caused by the handler that stashed it");
}

/// Tells itself another from each one, after the turn has taken its batch: the turn that handles it does not see it.
struct Spinner;

impl Actor for Spinner {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, (): (), cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.address().try_tell(()).map(drop).map_err(drop)
    }
}

#[test]
fn run_until_idle_handles_what_is_due_now_and_stops_there() {
    let mut r = rig(8);
    for m in [Msg::Timer(0), Msg::Timer(5), Msg::Job(1)] {
        r.actor.try_tell(m).unwrap();
    }
    // The messages; then the job they queued, run, whose result comes before the timer set for now. The later waits.
    assert_eq!(r.driver.run_until_idle(8), Ok(2));
    assert_eq!(r.jobs.pending(), 0);
    assert_eq!(r.driver.run_until_idle(8), Ok(0));
    r.clock.advance(Duration::from_secs(5));
    r.actor.try_tell(Msg::Push(6)).unwrap();
    assert_eq!(r.driver.run_until_idle(1), Ok(1), "one round is enough when it leaves nothing due");
    assert_eq!(r.seen(), [1, 0, 5, 6]);

    let clock = Clock::manual();
    let (mut driver, spinner) = EmbeddedDriver::new(Spinner, limits(2), clock.clock()).unwrap();
    spinner.address.try_tell(()).unwrap();
    assert_eq!(driver.run_until_idle(100), Err(NotIdle));
    assert_eq!(spinner.address.diagnostics().processed, 100, "one a turn");
}

enum Asked {
    Double(u32, ReplyTo<u32>),
    Done,
}

type Io = Box<dyn FnOnce() -> Asked + Send>;
type Fut = std::pin::Pin<Box<dyn Future<Output = Asked> + Send>>;

/// Doubles on a job that answers the caller. A refused job's work goes back to the front of its queue, and is
/// submitted again once a job ends.
#[derive(Default)]
struct Doubler {
    future: bool,
    io: VecDeque<Io>,
    futures: VecDeque<Fut>,
    refused: Arc<AtomicUsize>,
}

impl Actor for Doubler {
    type Message = Asked;
    type Error = ();

    fn on_message(&mut self, msg: Asked, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Asked::Double(v, reply) if self.future => self.futures.push_back(Box::pin(async move {
                let _ = reply.reply(v * 2);
                Asked::Done
            })),
            Asked::Double(v, reply) => self.io.push_back(Box::new(move || {
                let _ = reply.reply(v * 2);
                Asked::Done
            })),
            Asked::Done => {}
        }
        while let Some(work) = self.io.pop_front() {
            if let Err(refused) = cx.submit_io(work) {
                assert_eq!(refused.reason, TaskAdmissionError::Full);
                self.refused.fetch_add(1, Ordering::Relaxed);
                self.io.push_front(refused.work);
                break;
            }
        }
        while let Some(work) = self.futures.pop_front() {
            if let Err(refused) = cx.submit_future(work) {
                assert_eq!(refused.reason, TaskAdmissionError::Full);
                self.refused.fetch_add(1, Ordering::Relaxed);
                self.futures.push_front(refused.work);
                break;
            }
        }
        Ok(())
    }
}

#[test]
fn a_refused_job_hands_its_work_back_and_the_reply_it_holds_is_answered_once_it_runs() {
    // Refused by the mailbox's job limit, then by the executor's queue, which refuses a job already built around
    // the work: blocking work, then a future.
    for (jobs, queued, future) in [(1, 8, false), (8, 1, false), (8, 1, true)] {
        let (executor, _) = JobExecutor::manual(queued);
        let config = MailboxConfig::from(MailboxLimits { jobs, ..limits(8) });
        let refused = Arc::new(AtomicUsize::new(0));
        let actor = Doubler { future, refused: refused.clone(), ..Doubler::default() };
        let (mut driver, spawn) = EmbeddedDriver::with_jobs(actor, config, Clock::manual().clock(), executor).unwrap();
        let mut asks: Vec<_> =
            (1..=3).map(|v| Box::pin(spawn.address.request(None, move |r| Asked::Double(v, r)))).collect();
        asks.iter_mut().for_each(|ask| assert!(poll(ask.as_mut()).is_pending()));
        assert!(driver.run_until_idle(64).is_ok());
        let answers: Vec<_> = (asks.iter_mut())
            .map(|ask| match poll(ask.as_mut()) {
                Poll::Ready(Ok(v)) => v,
                other => panic!("jobs {jobs}, queued {queued}, future {future}: {:?}", other.map(|r| r.map(drop))),
            })
            .collect();
        assert_eq!(answers, [2, 4, 6], "jobs {jobs}, queued {queued}, future {future}");
        assert!(refused.load(Ordering::Relaxed) > 0, "jobs {jobs}, queued {queued}, future {future}: none was refused");
    }
}

fn panics() -> Msg {
    panic!("future panic")
}

fn attached(r: &mut Rig, messages: usize) -> tokio::sync::mpsc::Sender<u32> {
    let (tx, rx) = tokio::sync::mpsc::channel(messages);
    r.actor.try_tell(Msg::Attach(Recv(rx))).unwrap();
    r.turn();
    tx
}

#[test]
fn an_attached_source_feeds_the_actor_in_order_and_ends_with_none() {
    let mut r = rig(8);
    let tx = attached(&mut r, 8);
    assert_eq!((r.jobs.pending(), r.actor.diagnostics().jobs), (1, 1));
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().ordinary, 2);
    assert_eq!(r.seen(), [1, 2]);
    assert_eq!(r.jobs.pending(), 0, "waiting for the next item");

    tx.try_send(3).unwrap();
    assert_eq!(r.jobs.pending(), 1, "the item woke the source");
    drop(tx);
    assert!(r.jobs.run_next());
    r.turn();
    assert_eq!(r.seen(), [1, 2, 3, 99]);
    assert_eq!((r.jobs.pending(), r.actor.diagnostics().jobs), (0, 0));
    assert!(!r.turn().stopped);
}

#[test]
fn a_full_mailbox_pauses_the_source_and_loses_nothing() {
    let mut r = rig(2);
    let tx = attached(&mut r, 8);
    (1..=5).for_each(|v| tx.try_send(v).unwrap());
    assert!(r.jobs.run_next());
    assert_eq!(r.actor.diagnostics().mailbox_depth, 2);
    assert_eq!(r.jobs.pending(), 0, "parked on the mailbox, not spinning");
    assert_eq!(r.turn().ordinary, 2);
    assert!(r.jobs.run_next(), "room woke the source");
    r.turn();
    assert!(r.jobs.run_next());
    r.turn();
    assert_eq!(r.seen(), [1, 2, 3, 4, 5]);
}

#[test]
fn an_end_that_waits_for_room_is_sent_once() {
    let mut r = rig(1);
    let tx = attached(&mut r, 8);
    (1..=3).for_each(|v| tx.try_send(v).unwrap());
    drop(tx);
    for _ in 0..8 {
        r.jobs.run_next();
        r.turn();
    }
    assert_eq!(r.seen(), [1, 2, 3, 99]);
    assert_eq!((r.jobs.pending(), r.actor.diagnostics().jobs), (0, 0));
}

#[test]
fn a_detached_source_is_dropped_and_delivers_nothing_more() {
    let mut r = rig(8);
    let tx = attached(&mut r, 8);
    assert!(r.jobs.run_next());
    tx.try_send(1).unwrap();
    r.actor.try_tell(Msg::CancelLastJob).unwrap();
    r.turn();
    while r.jobs.run_next() {}
    r.turn();
    assert!(tx.is_closed(), "the source was dropped");
    assert_eq!((r.seen(), r.actor.diagnostics().jobs), (vec![], 0));
}

#[test]
fn a_drain_detaches_sources_and_finishes_drained() {
    let mut r = rig(8);
    let tx = attached(&mut r, 8);
    assert!(r.jobs.run_next());
    r.life.drain(None);
    assert!(!r.turn().stopped, "the source has not settled yet");
    assert!(r.jobs.run_next());
    assert!(tx.is_closed());
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason, exit.pending_jobs), (StopReason::Drained, 0));
}

#[test]
fn a_stop_drops_an_attached_source() {
    let mut r = rig(8);
    let tx = attached(&mut r, 8);
    assert!(r.jobs.run_next());
    r.life.stop();
    assert!(r.turn().stopped);
    assert_eq!(r.life.exit().unwrap().pending_jobs, 1);
    assert!(r.jobs.run_next());
    assert!(tx.is_closed());
}

#[test]
fn drain_processes_the_backlog_and_stop_discards_it() {
    let mut r = rig(8);
    (1..=3).for_each(|v| drop(r.actor.try_tell(Msg::Push(v))));
    r.life.drain(None);
    assert_eq!(r.actor.try_tell(Msg::Push(9)).unwrap_err().reason, RejectReason::Closed);
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Drained, 0));

    let mut r = rig(8);
    (1..=3).for_each(|v| drop(r.actor.try_tell(Msg::Push(v))));
    r.life.stop();
    r.turn();
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Stopped, 3));
    assert_eq!(r.actor.phase(), Phase::Stopped);

    // Timers that never fired are dropped either way, and the exit says how many.
    for drain in [true, false] {
        let mut r = rig(8);
        [Msg::Timer(1), Msg::Every].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
        r.turn();
        if drain {
            r.life.drain(None)
        } else {
            r.life.stop()
        }
        assert!(r.turn().stopped);
        let exit = r.life.exit().unwrap();
        assert_eq!((exit.pending_timers, exit.discarded_messages()), (2, 0), "drain: {drain}");
    }
}

#[test]
fn drain_waits_for_jobs_until_its_deadline() {
    let mut r = rig(8);
    r.actor.try_tell(Msg::Job(1)).unwrap();
    r.turn();
    r.life.drain(Some(Duration::from_secs(10)));
    r.life.drain(Some(Duration::from_secs(3)));
    r.life.drain(Some(Duration::from_secs(60)));
    assert!(!r.turn().stopped);
    r.clock.advance(Duration::from_secs(3));
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason, exit.pending_jobs), (StopReason::DrainDeadline, 1));
}

#[test]
fn an_actor_with_an_idle_time_drains_once_no_message_came_for_that_long() {
    let mut r = rig_with(Recorder::default(), idle(8), true);
    // A timer that fires and a job that settles keep nothing alive.
    [Msg::Timer(4), Msg::Job(1)].into_iter().for_each(|m| drop(r.actor.try_tell(m)));
    r.turn();
    r.clock.advance(Duration::from_secs(4));
    assert_eq!(r.turn().timers, 1);
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().completions, 1);

    // A message does: the idle time counts again from it.
    r.clock.advance(Duration::from_secs(5));
    r.actor.try_tell(Msg::Push(2)).unwrap();
    assert!(!r.turn().stopped);
    r.clock.advance(Duration::from_secs(9));
    assert!(!r.turn().stopped);
    // One queued once the idle time is up, before the turn that would drain, counts as well.
    r.clock.advance(Duration::from_secs(1));
    r.actor.try_tell(Msg::Push(3)).unwrap();
    r.turn();
    assert_eq!(r.actor.phase(), Phase::Running, "a message was queued");
    r.clock.advance(Duration::from_secs(10));
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Drained, 0));
    assert_eq!(r.actor.try_tell(Msg::Push(4)).unwrap_err().reason, RejectReason::Closed);
}

/// An attached source's items are messages, so each one counts against the idle time as a message does.
#[test]
fn an_attached_item_keeps_an_idle_actor_alive_as_a_message_does() {
    let mut r = rig_with(Recorder::default(), idle(8), true);
    let tx = attached(&mut r, 8);
    r.clock.advance(Duration::from_secs(9));
    tx.try_send(1).unwrap();
    assert!(r.jobs.run_next());
    assert_eq!(r.turn().ordinary, 1);
    r.clock.advance(Duration::from_secs(9));
    r.turn();
    assert_eq!(r.actor.phase(), Phase::Running, "18 s after the attach, 9 s after the item");
    r.clock.advance(Duration::from_secs(1));
    r.turn();
    assert_eq!(r.actor.phase(), Phase::Draining);
    assert!(!r.turn().stopped, "the drain detaches the source, which settles as a job");
    assert!(r.jobs.run_next());
    assert!(tx.is_closed());
    assert!(r.turn().stopped);
    assert_eq!(r.life.exit().unwrap().reason, StopReason::Drained);
}

#[test]
fn an_idle_drain_waits_for_a_job_no_longer_than_the_idle_time() {
    let mut r = rig_with(Recorder::default(), idle(8), true);
    r.actor.try_tell(Msg::Job(1)).unwrap();
    r.turn();
    r.clock.advance(Duration::from_secs(10));
    assert!(!r.turn().stopped);
    r.clock.advance(Duration::from_secs(10));
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason, exit.pending_jobs), (StopReason::DrainDeadline, 1));
}

/// A drain asked for before `on_start` ran begins after it: the actor's own setup is admitted, and the drain then
/// waits for the job that setup started.
#[test]
fn a_drain_before_the_start_admits_the_setup_and_then_drains() {
    let actor = Recorder { setup_on_start: true, ..Default::default() };
    let mut r = rig_with(actor, limits(4).into(), false);
    r.actor.try_tell(Msg::Push(3)).unwrap();
    r.life.drain(None);
    assert!(!r.turn().stopped, "the job started in on_start has not settled");
    assert!(r.jobs.run_next());
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason, exit.pending_jobs, exit.pending_timers), (StopReason::Drained, 0, 1));
}

#[test]
fn start_failure_never_opens_admission_or_calls_on_stop() {
    let stops = Arc::new(AtomicUsize::new(0));
    let actor = Recorder { fail_start: true, stops: stops.clone(), ..Default::default() };
    let mut r = rig_with(actor, limits(4).into(), true);
    assert_eq!(r.life.exit().unwrap().reason, StopReason::StartFailed("\"nope\"".into()));
    assert_eq!(r.actor.try_tell(Msg::Push(1)).unwrap_err().reason, RejectReason::Closed);
    assert_eq!(stops.load(Ordering::SeqCst), 0);
    assert!(r.turn().stopped);
}

#[test]
fn failures_panics_and_self_stop_end_the_actor_at_a_handler_boundary() {
    for (msg, expect) in [
        (Msg::Fail, StopReason::Failed("\"boom\"".into())),
        (Msg::Panic, StopReason::Panicked),
        (Msg::Stop, StopReason::Stopped),
    ] {
        let stops = Arc::new(AtomicUsize::new(0));
        let mut r = rig_with(Recorder { stops: stops.clone(), ..Default::default() }, limits(8).into(), true);
        r.actor.try_tell(msg).unwrap();
        r.actor.try_tell(Msg::Push(1)).unwrap();
        assert!(r.turn().stopped);
        let exit = r.life.exit().unwrap();
        assert_eq!((exit.reason.clone(), exit.discarded_messages()), (expect, 1));
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn panicking_debug_and_panicking_message_drop_are_contained() {
    struct Hostile;
    impl Actor for Hostile {
        type Message = ();
        type Error = LoudError;
        fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), LoudError> {
            Err(LoudError)
        }
    }
    let (mut driver, spawn) = EmbeddedDriver::new(Hostile, limits(4), Clock::manual().clock()).unwrap();
    driver.run_turn(TurnBudget::default());
    spawn.address.try_tell(()).unwrap();
    assert!(driver.run_turn(TurnBudget::default()).stopped);
    assert_eq!(spawn.lifecycle.exit().unwrap().reason, StopReason::Panicked);

    let mut r = rig(8);
    r.actor.try_tell(Msg::Stop).unwrap();
    r.actor.try_tell(Msg::Bomb(DropBomb)).unwrap();
    assert!(r.turn().stopped);
    let exit = r.life.exit().unwrap();
    assert_eq!((exit.reason.clone(), exit.discarded_messages()), (StopReason::Panicked, 1));
}

#[test]
fn dropping_the_driver_stops_the_actor_but_dropping_addresses_does_not() {
    let r = rig(4);
    drop(r.actor.clone());
    assert_eq!(r.life.phase(), Phase::Running);
    let Rig { driver, life, .. } = r;
    drop(driver);
    assert_eq!(life.exit().unwrap().reason, StopReason::DriverDropped);
}

#[test]
fn time_budgets_are_cooperative_and_keep_the_suffix_queued() {
    struct Slow(ManualClock, Vec<u32>);
    impl Actor for Slow {
        type Message = u32;
        type Error = ();
        fn on_message(&mut self, v: u32, _: &mut Context<'_, Self>) -> Result<(), ()> {
            self.0.advance(Duration::from_millis(10));
            self.1.push(v);
            Ok(())
        }
    }
    let shared = Clock::manual();
    let handle = shared.clock();
    let (mut driver, spawn) = EmbeddedDriver::new(Slow(shared, vec![]), limits(8), handle).unwrap();
    driver.run_turn(TurnBudget::default());
    (1..=3).for_each(|v| spawn.address.try_tell(v).map(drop).unwrap());
    let budget = TurnBudget { max_elapsed: Some(Duration::from_millis(5)), ..TurnBudget::default() };
    let report = driver.run_turn(budget);
    assert_eq!((report.ordinary, report.budget_overrun, report.runnable), (1, true, true));
    assert_eq!(spawn.address.diagnostics().mailbox_depth, 2);
    assert_eq!(driver.run_turn(TurnBudget::default()).ordinary, 2);
}

#[test]
fn children_follow_a_drained_parent() {
    struct Parent(Option<LifecycleHandle>);
    impl Actor for Parent {
        type Message = ();
        type Error = ();
        fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
            cx.adopt_child(self.0.take().unwrap()).map_err(drop)
        }
        fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
            Ok(())
        }
    }
    let clock = Clock::manual();
    let mut child = rig(4);
    let (mut parent, spawn) = EmbeddedDriver::new(Parent(Some(child.life.clone())), limits(4), clock.clock()).unwrap();
    parent.run_turn(TurnBudget::default());
    spawn.lifecycle.drain(None);
    assert!(!parent.run_turn(TurnBudget::default()).stopped);
    assert_eq!(child.life.phase(), Phase::Draining);
    child.turn();
    assert!(parent.run_turn(TurnBudget::default()).stopped);
    let exit = spawn.lifecycle.exit().unwrap();
    assert_eq!((exit.reason, exit.children_pending), (StopReason::Drained, 0));
}

#[test]
fn supervision_restarts_with_fresh_state_then_escalates() {
    let clock = Clock::manual();
    let backoff = Backoff { jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { max_restarts: 2, backoff, ..Default::default() };
    let (mut family, handle) = Supervised::new(Recorder::default, limits(8), clock.clock(), policy).unwrap();
    family.run_turn(TurnBudget::default());
    let first = handle.current().unwrap();
    first.try_tell(Msg::Push(1)).unwrap();
    first.try_tell(Msg::Fail).unwrap();
    let report = family.run_turn(TurnBudget::default());
    assert_eq!(report.next_restart, Some(Duration::from_millis(10)));
    assert!(handle.current().is_none());

    clock.advance(Duration::from_millis(10));
    assert!(family.run_turn(TurnBudget::default()).replaced);
    family.run_turn(TurnBudget::default());
    let second = handle.current().unwrap();
    assert_eq!((handle.generation(), handle.restarts()), (2, 1));
    assert_eq!(first.try_tell(Msg::Push(2)).unwrap_err().reason, RejectReason::Closed);

    second.try_tell(Msg::Fail).unwrap();
    family.run_turn(TurnBudget::default());
    clock.advance(Duration::from_millis(20));
    family.run_turn(TurnBudget::default());
    family.run_turn(TurnBudget::default());
    handle.current().unwrap().try_tell(Msg::Fail).unwrap();
    let report = family.run_turn(TurnBudget::default());
    assert!(report.escalated && report.terminal.is_some());
    assert_eq!(handle.lifecycle().phase(), Phase::Stopped);
}

#[test]
fn a_restart_burst_is_one_event_at_once_then_one_a_minute_with_its_count() {
    let clock = Clock::manual();
    let events = Arc::new(EventLog::new(clock.clock()));
    let config = MailboxConfig { events: Some(events.clone()), ..MailboxConfig::from(limits(8)) };
    let wait = Duration::from_millis(1);
    // Restarted a millisecond after each failure, a hundred times a minute, so a burst fits in one window.
    let backoff = Backoff { initial: wait, max: wait, jitter: 0.0 };
    let policy = RestartPolicy { max_restarts: 100, backoff, ..Default::default() };
    let (mut family, handle) = Supervised::new(Recorder::default, config, clock.clock(), policy).unwrap();
    family.run_turn(TurnBudget::default());
    let mut fail = || {
        handle.current().unwrap().try_tell(Msg::Fail).unwrap();
        family.run_turn(TurnBudget::default());
        clock.advance(wait);
        assert!(family.run_turn(TurnBudget::default()).replaced);
        family.run_turn(TurnBudget::default());
    };
    let told = |events: &EventLog| {
        events.since(0, usize::MAX).into_iter().map(|e| (e.seq, e.kind, e.count, e.detail)).collect::<Vec<_>>()
    };
    let boom = || r#""boom""#.to_owned();
    for _ in 0..5 {
        fail();
    }
    assert_eq!(told(&events), [(1, "actor_restarted", 1, boom())], "the first restart is told at once");
    let subject = &events.since(0, 1)[0].subject;
    assert!(subject.contains("Recorder"), "an unnamed family is its type and base: {subject}");

    clock.advance(Duration::from_secs(60));
    assert_eq!(told(&events)[1..], [(2, "actor_restarted", 4, boom())], "the rest are one event, with their count");
    clock.advance(Duration::from_secs(60));
    assert_eq!(told(&events).len(), 2, "a quiet minute tells nothing");
    fail();
    assert_eq!(
        told(&events)[2..],
        [(3, "actor_restarted", 1, boom())],
        "after a quiet minute the next is told at once"
    );
}

#[test]
fn a_family_that_gives_up_is_one_gave_up_event_with_its_last_failure() {
    let clock = Clock::manual();
    let events = Arc::new(EventLog::new(clock.clock()));
    let config = MailboxConfig { events: Some(events.clone()), ..MailboxConfig::from(limits(8)) };
    // No restart allowed: the first failure is the last.
    let backoff = Backoff { jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { max_restarts: 0, backoff, ..Default::default() };
    let (mut family, handle) = Supervised::new(Recorder::default, config, clock.clock(), policy).unwrap();
    family.run_turn(TurnBudget::default());
    handle.current().unwrap().try_tell(Msg::Fail).unwrap();
    assert!(family.run_turn(TurnBudget::default()).escalated);
    let told: Vec<_> = events.since(0, usize::MAX).into_iter().map(|e| (e.kind, e.detail)).collect();
    assert_eq!(told, [("gave_up", r#""boom""#.to_owned())]);
}

#[test]
fn an_event_log_keeps_the_newest_and_a_reader_resumes_after_its_position() {
    let log = EventLog::new(Clock::manual().clock());
    for n in 0..300 {
        log.record("acquired", format!("orders/{n}"), "");
    }
    let kept = log.since(0, usize::MAX);
    assert_eq!((kept.len(), kept[0].seq, kept[255].seq), (EVENTS, 45, 300));
    assert_eq!(kept[0].subject, "orders/44");
    let page = log.since(290, 5);
    assert_eq!(page.iter().map(|e| e.seq).collect::<Vec<_>>(), [291, 292, 293, 294, 295]);
    assert!(log.since(300, 5).is_empty());
}

#[test]
fn supervision_stop_interrupts_backoff_and_graceful_exits_do_not_restart() {
    let clock = Clock::manual();
    let (mut family, handle) =
        Supervised::new(Recorder::default, limits(8), clock.clock(), RestartPolicy::default()).unwrap();
    family.run_turn(TurnBudget::default());
    handle.current().unwrap().try_tell(Msg::Panic).unwrap();
    assert!(family.run_turn(TurnBudget::default()).next_restart.is_some());
    handle.lifecycle().stop();
    assert!(family.run_turn(TurnBudget::default()).terminal.is_some());

    let (mut family, handle) =
        Supervised::new(Recorder::default, limits(8), clock.clock(), RestartPolicy::default()).unwrap();
    family.run_turn(TurnBudget::default());
    handle.current().unwrap().try_tell(Msg::Stop).unwrap();
    let report = family.run_turn(TurnBudget::default());
    assert!(!report.escalated && report.terminal.unwrap().reason == StopReason::Stopped);
}

#[test]
fn timing_is_off_by_default_and_sampled_when_enabled() {
    let mut r = rig(8);
    r.actor.try_tell(Msg::Push(1)).unwrap();
    r.turn();
    assert!(r.actor.diagnostics().timings.is_none());

    let mut r = rig_with(Recorder::default(), MailboxConfig::from(limits(8)).with_timing(1), true);
    r.actor.try_tell(Msg::Push(1)).unwrap();
    r.clock.advance(Duration::from_millis(3));
    assert_eq!(r.actor.diagnostics().oldest_message_age, Some(Duration::from_millis(3)));
    r.turn();
    let t = r.actor.diagnostics().timings.unwrap();
    assert_eq!((t.mailbox_wait.count, t.handler.count), (1, 2));
    assert_eq!(t.mailbox_wait.percentile(1.0), Some(Duration::from_nanos(3_145_727)));
}

#[test]
fn sources_have_their_own_budget_and_do_not_count_as_waiters() {
    let limits = MailboxLimits { jobs: 1, sources: 3, waiters: 1, ..limits(1) };
    let mut r = rig_with(Recorder::default(), limits.into(), true);
    let txs: Vec<_> = (0..3).map(|_| attached(&mut r, 8)).collect();
    for (i, tx) in (0u32..).zip(&txs) {
        tx.try_send(i * 10 + 1).unwrap();
        tx.try_send(i * 10 + 2).unwrap();
    }
    for _ in 0..16 {
        while r.jobs.run_next() {}
        r.turn();
    }
    let mut seen = r.seen();
    seen.sort();
    assert_eq!(seen, [1, 2, 11, 12, 21, 22], "three sources parked on a one-slot mailbox, none refused");
    assert_eq!(r.actor.diagnostics().rejected[RejectReason::WaiterLimit as usize], 0);

    r.actor.try_tell(Msg::Job(7)).unwrap();
    r.turn();
    assert!(r.jobs.run_next(), "the job budget is untouched by the sources");
    r.turn();
    assert_eq!(r.seen().last(), Some(&7));

    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    r.actor.try_tell(Msg::Attach(Recv(rx))).unwrap();
    assert!(r.turn().stopped);
    assert_eq!(r.life.exit().unwrap().reason, StopReason::Failed("\"Full\"".into()));
}

#[test]
fn an_attached_item_over_the_size_limit_fails_the_actor_and_says_why() {
    let config = MailboxConfig::from(MailboxLimits { max_message_bytes: 10, ..limits(4) })
        .with_weight(|m| if let Msg::Push(v) = m { *v as usize } else { 1 });
    let mut r = rig_with(Recorder::default(), config, true);
    let tx = attached(&mut r, 8);
    tx.try_send(11).unwrap();
    assert!(r.jobs.run_next());
    assert!(r.turn().stopped);
    let reason = r.life.exit().unwrap().reason;
    assert_eq!(reason, StopReason::Failed("attached source: item refused (TooLarge)".into()));
}

/// A sink for the tests: takes items while it has `room`, and records flushes, the close and its own drop.
#[derive(Clone, Default)]
struct Wire(Arc<std::sync::Mutex<WireState>>);

#[derive(Default)]
struct WireState {
    room: usize,
    written: Vec<u32>,
    flushes: usize,
    closed: bool,
    dropped: bool,
    fail: bool,
    waker: Option<Waker>,
}

impl Wire {
    fn state(&self) -> std::sync::MutexGuard<'_, WireState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn open(&self, room: usize) {
        let waker = {
            let mut s = self.state();
            s.room += room;
            s.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct WireEnd(Wire);

impl Drop for WireEnd {
    fn drop(&mut self) {
        self.0.state().dropped = true;
    }
}

impl Sink<u32> for WireEnd {
    type Error = &'static str;

    fn poll_ready(self: std::pin::Pin<&mut Self>, cx: &mut Cx<'_>) -> Poll<Result<(), &'static str>> {
        let mut s = self.0.state();
        if s.fail {
            return Poll::Ready(Err("broken"));
        }
        if s.room > 0 {
            return Poll::Ready(Ok(()));
        }
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: u32) -> Result<(), &'static str> {
        let mut s = self.0.state();
        s.room -= 1;
        s.written.push(item);
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut Cx<'_>) -> Poll<Result<(), &'static str>> {
        self.0.state().flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, _: &mut Cx<'_>) -> Poll<Result<(), &'static str>> {
        self.0.state().closed = true;
        Poll::Ready(Ok(()))
    }
}

enum Out {
    Attach(WireEnd, usize),
    Write(Vec<u32>),
    Event(SinkEvent<&'static str>),
    DropSink,
    CancelSink,
}

/// Keeps what the sink refused in its own outbox and hands it over at the end of each turn.
#[derive(Default)]
struct Writer {
    tx: Option<SinkHandle<u32>>,
    outbox: VecDeque<u32>,
    events: Arc<std::sync::Mutex<Vec<SinkEvent<&'static str>>>>,
    turn_ends: Arc<AtomicUsize>,
    refusals: Arc<std::sync::Mutex<Vec<RejectReason>>>,
}

impl Actor for Writer {
    type Message = Out;
    type Error = String;

    fn on_message(&mut self, msg: Out, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Out::Attach(end, capacity) => {
                self.tx = Some(cx.attach_sink(end, capacity, Out::Event).map_err(|e| format!("{e:?}"))?)
            }
            Out::Write(items) => self.outbox.extend(items),
            Out::Event(event) => self.events.lock().unwrap().push(event),
            Out::DropSink => self.tx = None,
            Out::CancelSink => self.tx.as_ref().expect("a sink").cancel(),
        }
        Ok(())
    }

    fn on_turn_end(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.turn_ends.fetch_add(1, Ordering::SeqCst);
        let Some(tx) = &self.tx else { return Ok(()) };
        while let Some(item) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(item) {
                self.refusals.lock().unwrap().push(refused.reason);
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message);
                }
                break;
            }
        }
        Ok(())
    }
}

struct WriterRig {
    driver: EmbeddedDriver<Writer>,
    actor: ActorRef<Out>,
    life: LifecycleHandle,
    jobs: ManualJobs,
    wire: Wire,
    events: Arc<std::sync::Mutex<Vec<SinkEvent<&'static str>>>>,
    turn_ends: Arc<AtomicUsize>,
    refusals: Arc<std::sync::Mutex<Vec<RejectReason>>>,
}

fn writer(capacity: usize) -> WriterRig {
    let (writer, wire) = (Writer::default(), Wire::default());
    let (events, turn_ends, refusals) = (writer.events.clone(), writer.turn_ends.clone(), writer.refusals.clone());
    let (executor, jobs) = JobExecutor::manual(8);
    let (mut driver, spawn) = EmbeddedDriver::with_jobs(writer, limits(8), Clock::manual().clock(), executor).unwrap();
    spawn.address.try_tell(Out::Attach(WireEnd(wire.clone()), capacity)).unwrap();
    driver.run_turn(TurnBudget::default());
    WriterRig { driver, actor: spawn.address, life: spawn.lifecycle, jobs, wire, events, turn_ends, refusals }
}

impl WriterRig {
    fn turn(&mut self) -> TurnReport {
        self.driver.run_turn(TurnBudget::default())
    }

    fn settle(&mut self) {
        self.driver.run_until_idle(64).expect("settles");
    }

    fn events(&self) -> Vec<SinkEvent<&'static str>> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

#[test]
fn a_sink_takes_items_without_waiting_and_writes_them_in_order_flushing_when_dry() {
    let mut w = writer(2);
    w.actor.try_tell(Out::Write((1..=6).collect())).unwrap();
    w.turn();
    w.settle();
    assert!(w.wire.state().written.is_empty(), "the wire has no room yet");
    assert_eq!(w.events(), [SinkEvent::Ready], "one refusal, one Ready once the queue was half empty");

    w.wire.open(100);
    w.settle();
    let s = w.wire.state();
    assert_eq!(s.written, [1, 2, 3, 4, 5, 6]);
    assert!((1..6).contains(&s.flushes), "flushed when the queue ran dry, not per item: {}", s.flushes);
    drop(s);

    w.actor.try_tell(Out::DropSink).unwrap();
    w.settle();
    assert_eq!(w.events().last(), Some(&SinkEvent::Closed(Ok(()))));
    assert!(w.wire.state().closed && w.wire.state().dropped);
    assert_eq!(w.actor.diagnostics().jobs, 0);
}

#[test]
fn on_turn_end_runs_once_per_turn_that_handled_something() {
    let mut w = writer(8);
    let before = w.turn_ends.load(Ordering::SeqCst);
    for i in 0..3 {
        w.actor.try_tell(Out::Write(vec![i])).unwrap();
    }
    assert_eq!(w.turn().ordinary, 3);
    assert_eq!(w.turn_ends.load(Ordering::SeqCst), before + 1);
    w.turn();
    assert_eq!(w.turn_ends.load(Ordering::SeqCst), before + 1, "an idle turn calls nothing");
}

#[test]
fn a_drain_lets_the_actor_write_what_it_holds_before_the_sink_closes() {
    let mut w = writer(1);
    w.actor.try_tell(Out::Write((1..=5).collect())).unwrap();
    w.turn();
    w.life.drain(None);
    w.settle();
    assert!(w.life.exit().is_none(), "the actor still owes the sink four items");
    assert!(!w.wire.state().closed);

    w.wire.open(100);
    w.settle();
    assert_eq!(w.wire.state().written, [1, 2, 3, 4, 5]);
    assert!(w.wire.state().closed);
    assert_eq!(w.events().last(), Some(&SinkEvent::Closed(Ok(()))), "handled during the drain");
    let exit = w.life.exit().unwrap();
    assert_eq!((exit.reason, exit.pending_jobs), (StopReason::Drained, 0));
}

#[test]
fn a_stop_drops_the_sink_and_what_it_queued_without_an_event() {
    let mut w = writer(4);
    w.actor.try_tell(Out::Write(vec![1, 2])).unwrap();
    w.turn();
    w.life.stop();
    assert!(w.turn().stopped);
    assert_eq!(w.life.exit().unwrap().pending_jobs, 1);
    while w.jobs.run_next() {}
    let s = w.wire.state();
    assert!(s.dropped && !s.closed && s.written.is_empty());
    assert!(w.events().is_empty());
}

#[test]
fn a_failing_sink_closes_with_its_error_and_refuses_what_follows() {
    let mut w = writer(4);
    w.wire.state().fail = true;
    w.actor.try_tell(Out::Write(vec![1])).unwrap();
    w.turn();
    w.settle();
    assert_eq!(w.events(), [SinkEvent::Closed(Err("broken"))]);
    assert!(w.wire.state().dropped);
    w.actor.try_tell(Out::Write(vec![2])).unwrap();
    w.turn();
    assert_eq!(w.refusals.lock().unwrap().last(), Some(&RejectReason::Closed));
    assert_eq!((w.life.exit(), w.actor.diagnostics().jobs), (None, 0));
}

#[test]
fn a_cancelled_sink_is_dropped_with_what_it_queued_and_sends_nothing() {
    let mut w = writer(4);
    w.actor.try_tell(Out::Write(vec![1, 2])).unwrap();
    w.turn();
    w.actor.try_tell(Out::CancelSink).unwrap();
    w.settle();
    let s = w.wire.state();
    assert!(s.dropped && !s.closed && s.written.is_empty());
    drop(s);
    assert!(w.events().is_empty());
    assert_eq!((w.life.exit(), w.actor.diagnostics().jobs), (None, 0));
}

#[test]
fn a_sink_kept_busy_flushes_and_yields_its_thread_every_64_items() {
    let mut w = writer(256);
    w.wire.open(1000);
    w.actor.try_tell(Out::Write((0..150).collect())).unwrap();
    w.turn();
    assert!(w.jobs.run_next());
    let s = w.wire.state();
    assert_eq!((s.written.len(), s.flushes), (64, 1));
    drop(s);
    assert_eq!(w.jobs.pending(), 1, "it asked to be polled again");
    w.settle();
    assert_eq!(w.wire.state().written, (0..150).collect::<Vec<_>>());
}

struct Quiet;

impl Actor for Quiet {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

enum Watching {
    Watch(LifecycleHandle),
    CancelLast,
    Down(Exit),
    Fill,
}

struct Watcher {
    downs: Arc<std::sync::Mutex<Vec<ActorIncarnation>>>,
    watches: Vec<WatchHandle>,
    probe: Arc<()>,
}

impl Actor for Watcher {
    type Message = Watching;
    type Error = String;

    fn on_message(&mut self, msg: Watching, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Watching::Watch(target) => {
                let probe = self.probe.clone();
                let map = move |exit| {
                    drop(probe);
                    Watching::Down(exit)
                };
                self.watches.push(cx.watch(&target, map).map_err(|e| format!("{e:?}"))?);
            }
            Watching::CancelLast => self.watches.pop().expect("a watch").cancel(),
            Watching::Down(exit) => self.downs.lock().unwrap().push(exit.incarnation),
            Watching::Fill => {}
        }
        Ok(())
    }
}

#[test]
fn a_watcher_is_told_once_of_each_exit_past_a_full_mailbox_and_not_after_a_cancel() {
    let clock = Clock::manual();
    let target = || {
        let (mut driver, spawn) = EmbeddedDriver::new(Quiet, limits(4), clock.clock()).unwrap();
        driver.run_turn(TurnBudget::default());
        (driver, spawn.lifecycle)
    };
    let probe = Arc::new(());
    let watcher = || {
        let downs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let actor = Watcher { downs: downs.clone(), watches: Vec::new(), probe: probe.clone() };
        let (mut driver, spawn) = EmbeddedDriver::new(actor, limits(2), clock.clock()).unwrap();
        driver.run_turn(TurnBudget::default());
        (driver, spawn, downs)
    };
    let step = |driver: &mut EmbeddedDriver<Watcher>, to: &ActorRef<Watching>, msg: Watching| {
        to.try_tell(msg).unwrap();
        driver.run_until_idle(8).unwrap();
    };
    let ((mut a, a_life), (mut b, b_life), (mut c, c_life)) = (target(), target(), target());
    let (mut w, w_spawn, downs) = watcher();
    let (mut w2, w2_spawn, downs2) = watcher();
    let (to, to2) = (&w_spawn.address, &w2_spawn.address);

    // Any number of watches on one actor, from one watcher or several.
    step(&mut w, to, Watching::Watch(a_life.clone()));
    step(&mut w, to, Watching::Watch(a_life.clone()));
    step(&mut w2, to2, Watching::Watch(a_life.clone()));
    // A cancelled watch tells nothing.
    step(&mut w, to, Watching::Watch(b_life.clone()));
    step(&mut w, to, Watching::CancelLast);
    // An actor that has ended already is told at once.
    c_life.stop();
    c.run_turn(TurnBudget::default());
    step(&mut w, to, Watching::Watch(c_life.clone()));
    assert_eq!(*downs.lock().unwrap(), [c_life.incarnation()]);

    // The watcher's mailbox is full, and the downs still arrive: they are admitted like job results.
    to.try_tell(Watching::Fill).unwrap();
    to.try_tell(Watching::Fill).unwrap();
    assert!(to.try_tell(Watching::Fill).is_err());
    a_life.stop();
    a.run_turn(TurnBudget::default());
    b_life.stop();
    b.run_turn(TurnBudget::default());
    assert_eq!(w.run_until_idle(8), Ok(1));
    assert_eq!(w2.run_until_idle(8), Ok(1));
    assert_eq!(w.run_until_idle(8), Ok(0), "each is told once");
    let a_down = a_life.exit().unwrap().incarnation;
    assert_eq!(*downs.lock().unwrap(), [c_life.incarnation(), a_down, a_down]);
    assert_eq!(*downs2.lock().unwrap(), [a_down]);
    assert_eq!(Arc::strong_count(&probe), 3, "the watchers hold one each; the told and the cancelled watches none");

    // A watcher that drains is not held by a watch on an actor that goes on.
    let (_d, d_life) = target();
    step(&mut w, to, Watching::Watch(d_life.clone()));
    w_spawn.lifecycle.drain(None);
    w.run_until_idle(8).unwrap();
    assert_eq!(w_spawn.lifecycle.exit().map(|e| e.reason), Some(StopReason::Drained));
    w2_spawn.lifecycle.stop();
    w2.run_turn(TurnBudget::default());
    assert_eq!(Arc::strong_count(&probe), 1, "no watch outlives its watcher");
    d_life.stop();
    assert_eq!(downs.lock().unwrap().len(), 3);
}

enum Held {
    Watch(LifecycleHandle),
    Attach(Recv),
    Down(ActorIncarnation),
    Item,
}

#[derive(Clone, Copy)]
enum OnDown {
    Log,
    Stop,
    CancelTheRest,
}

/// Logs what each `cx.watch` and `cx.attach` returned, and each down.
struct Holder {
    log: Arc<std::sync::Mutex<Vec<String>>>,
    watches: Arc<std::sync::Mutex<Vec<WatchHandle>>>,
    on_down: OnDown,
}

impl Actor for Holder {
    type Message = Held;
    type Error = ();

    fn on_message(&mut self, msg: Held, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let said = match msg {
            Held::Watch(target) => match cx.watch(&target, |exit| Held::Down(exit.incarnation)) {
                Ok(watch) => {
                    self.watches.lock().unwrap().push(watch);
                    "watch".to_string()
                }
                Err(e) => format!("watch {e:?}"),
            },
            Held::Attach(source) => format!("attach {:?}", cx.attach(source, |_| Held::Item).map(drop)),
            Held::Down(of) => {
                match self.on_down {
                    OnDown::Log => {}
                    OnDown::Stop => cx.stop(),
                    OnDown::CancelTheRest => self.watches.lock().unwrap().iter().for_each(WatchHandle::cancel),
                }
                format!("down {of}")
            }
            Held::Item => "item".to_string(),
        };
        self.log.lock().unwrap().push(said);
        Ok(())
    }
}

#[test]
fn a_watch_holds_a_source_slot_until_handled_or_cancelled_and_nothing_follows_a_cancel_or_a_stop() {
    let clock = Clock::manual();
    let target = || {
        let (mut driver, spawn) = EmbeddedDriver::new(Quiet, limits(4), clock.clock()).unwrap();
        driver.run_turn(TurnBudget::default());
        (driver, spawn.lifecycle)
    };
    let end = |(driver, life): &mut (EmbeddedDriver<Quiet>, LifecycleHandle)| {
        life.stop();
        driver.run_turn(TurnBudget::default());
        format!("down {}", life.incarnation())
    };
    let holder = |on_down| {
        let (log, watches) = (Arc::default(), Arc::default());
        let actor = Holder { log: Arc::clone(&log), watches: Arc::clone(&watches), on_down };
        let config = MailboxLimits { sources: 2, ..limits(8) };
        let (executor, jobs) = JobExecutor::manual(8);
        let (mut driver, spawn) = EmbeddedDriver::with_jobs(actor, config, clock.clock(), executor).unwrap();
        driver.run_turn(TurnBudget::default());
        (driver, spawn, log, watches, jobs)
    };
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let (_tx2, rx2) = tokio::sync::mpsc::channel(1);
    let (mut a, mut b, c) = (target(), target(), target());

    // Two watches spend a budget of two, for watches and attached sources alike.
    let (mut h, spawn, log, watches, _jobs) = holder(OnDown::Log);
    for msg in [Held::Watch(a.1.clone()), Held::Watch(b.1.clone()), Held::Watch(c.1.clone()), Held::Attach(Recv(rx))] {
        spawn.address.try_tell(msg).unwrap();
    }
    h.run_until_idle(8).unwrap();
    assert_eq!(*log.lock().unwrap(), ["watch", "watch", "watch Full", "attach Err(Full)"]);

    // Both watched actors end; one watch is cancelled from outside before the watcher runs: nothing arrives for it.
    let a_down = end(&mut a);
    end(&mut b);
    watches.lock().unwrap()[1].cancel();
    assert_eq!(h.run_until_idle(8), Ok(1));
    assert_eq!(log.lock().unwrap()[4..], [a_down]);

    // The told and the cancelled watches gave their slots back.
    spawn.address.try_tell(Held::Watch(c.1.clone())).unwrap();
    spawn.address.try_tell(Held::Attach(Recv(rx2))).unwrap();
    h.run_until_idle(8).unwrap();
    assert_eq!(log.lock().unwrap()[5..], ["watch", "attach Ok(())"]);

    // Two downs due in one turn: the first stops the watcher, or cancels the other watch, and the other never arrives.
    for on_down in [OnDown::Stop, OnDown::CancelTheRest] {
        let (mut h, spawn, log, _, _jobs) = holder(on_down);
        let (mut d, mut e) = (target(), target());
        spawn.address.try_tell(Held::Watch(d.1.clone())).unwrap();
        spawn.address.try_tell(Held::Watch(e.1.clone())).unwrap();
        h.run_until_idle(8).unwrap();
        end(&mut d);
        end(&mut e);
        h.run_until_idle(8).unwrap();
        let log = log.lock().unwrap().clone();
        assert_eq!(log.len(), 3, "{log:?}");
        assert_eq!(spawn.lifecycle.exit().is_some(), matches!(on_down, OnDown::Stop));
    }
}
