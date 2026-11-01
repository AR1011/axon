use std::task::{Context, Poll, Waker};

use loom::future::block_on;
use loom::thread;

use super::*;

/// One slot, already taken; `notify` wakes its turn.
fn full(notify: Option<Waker>) -> ActorRef<u32> {
    let limits = MailboxLimits { messages: 1, ..Default::default() };
    let incarnation = ActorIncarnation { runtime: 0, slot: 0, generation: 0 };
    let mailbox = Mailbox::new(limits.into(), Clock::manual().clock(), incarnation, notify);
    mailbox.open();
    let address = ActorRef(mailbox);
    address.try_tell(0).expect("the one slot is free");
    address
}

/// The owner's turn: takes one message and gives its capacity back.
fn consume(address: &ActorRef<u32>) -> Option<u32> {
    let mut batch = VecDeque::new();
    address.0.pull(1, &mut batch);
    let entry = batch.pop_front()?;
    address.0.finish_batch(1, entry.bytes, &mut batch);
    Some(entry.msg)
}

fn check(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(model);
}

#[test]
fn a_sender_waiting_for_room_is_woken_by_the_release() {
    check(|| {
        let address = full(None);
        let sender = address.clone();
        let sent = thread::spawn(move || block_on(sender.send(1, None)).is_ok());
        assert_eq!(consume(&address), Some(0));
        assert!(sent.join().unwrap());
        assert_eq!((consume(&address), address.diagnostics().mailbox_depth), (Some(1), 0));
    });
}

#[test]
fn two_waiting_senders_are_both_admitted_in_turn() {
    check(|| {
        let address = full(None);
        let senders: Vec<_> = (1..=2)
            .map(|n| {
                let sender = address.clone();
                thread::spawn(move || block_on(sender.send(n, None)).is_ok())
            })
            .collect();
        let mut seen = 0;
        while seen < 3 {
            match consume(&address) {
                Some(_) => seen += 1,
                None => thread::yield_now(),
            }
        }
        assert!(senders.into_iter().all(|s| s.join().unwrap()));
    });
}

/// A send polled first while another waits queues behind it, however the release races it: it never takes the room
/// the waiter is woken for.
#[test]
fn a_new_send_never_takes_the_room_a_waiter_was_woken_for() {
    check(|| {
        let address = full(None);
        let mut first = address.send(1, None);
        assert!(std::pin::Pin::new(&mut first).poll(&mut Context::from_waker(Waker::noop())).is_pending());
        let consumer = {
            let address = address.clone();
            thread::spawn(move || consume(&address))
        };
        let sender = address.clone();
        let late = thread::spawn(move || block_on(sender.send(2, None)).is_ok());
        assert_eq!(consumer.join().unwrap(), Some(0));
        assert!(block_on(first).is_ok());
        let mut seen = Vec::new();
        while seen.len() < 2 {
            match consume(&address) {
                Some(n) => seen.push(n),
                None => thread::yield_now(),
            }
        }
        assert!(late.join().unwrap());
        assert_eq!(seen, [1, 2], "the new send went ahead of the waiter");
    });
}

#[test]
fn a_waiter_that_gives_up_hands_its_wake_to_the_next() {
    check(|| {
        let address = full(None);
        let (quitter, sender) = (address.clone(), address.clone());
        let quit = thread::spawn(move || {
            let mut reserve = std::pin::pin!(quitter.reserve(size_of::<u32>(), None));
            let polled = reserve.as_mut().poll(&mut Context::from_waker(Waker::noop()));
            matches!(polled, Poll::Ready(Ok(_)))
        });
        let sent = thread::spawn(move || block_on(sender.send(1, None)).is_ok());
        assert_eq!(consume(&address), Some(0));
        // A permit the quitter did get was dropped with it, so the slot comes back either way.
        quit.join().unwrap();
        assert!(sent.join().unwrap());
    });
}

#[test]
fn closing_wakes_a_waiting_sender_with_a_refusal() {
    check(|| {
        let address = full(None);
        let sender = address.clone();
        let sent = thread::spawn(move || block_on(sender.send(1, None)).map_err(|r| r.reason));
        address.0.close(Phase::Stopping);
        assert_eq!(sent.join().unwrap(), Err(RejectReason::Closed));
    });
}

/// The driver's turn while a drain may be pending, in the driver's order: flag, then pull.
/// Returns what it handled once it decides the drain is complete; nothing may be left then.
fn drain_turns(address: &ActorRef<u32>) -> usize {
    let (mb, mut handled) = (&address.0, 0);
    loop {
        let draining = mb.drain_requested.load(Ordering::Acquire);
        let mut batch = VecDeque::new();
        let remaining = mb.pull(8, &mut batch);
        let bytes = batch.iter().map(|e| e.bytes).sum();
        handled += batch.len();
        mb.release(batch.len(), bytes);
        if draining && remaining == 0 {
            assert_eq!(mb.discard().len(), 0, "drained with an accepted message still queued");
            return handled;
        }
        thread::yield_now();
    }
}

#[test]
fn a_drain_racing_a_tell_or_a_commit_handles_whatever_was_accepted() {
    for committed in [false, true] {
        check(move || {
            let address = full(None);
            assert_eq!(consume(&address), Some(0));
            let sender = address.clone();
            let sent = thread::spawn(move || match committed {
                false => sender.try_tell(1).is_ok(),
                true => match block_on(sender.reserve(size_of::<u32>(), None)) {
                    Ok(permit) => permit.commit(1).is_ok(),
                    Err(_) => false,
                },
            });
            let drainer = address.clone();
            let drained = thread::spawn(move || drainer.0.request_drain(None));
            let handled = drain_turns(&address);
            drained.join().unwrap();
            // A message accepted after the last turn would be the bug; one refused is the sender's to keep.
            let accepted = sent.join().unwrap() as usize;
            assert_eq!(handled + address.0.discard().len(), accepted);
            assert_eq!(handled, accepted, "accepted after the drain was judged complete");
        });
    }
}

/// A cell whose turn is queued, as `Cell::notify` wakes it: `pushed` is set when a wake claims its place.
fn queued_turn() -> (Arc<crate::runtime::Scheduled>, Arc<AtomicBool>, Waker) {
    let scheduled = Arc::new(crate::runtime::Scheduled::new());
    let pushed = Arc::new(AtomicBool::new(false));
    assert!(scheduled.claim(), "the turn about to run was queued");
    struct Push(Arc<crate::runtime::Scheduled>, Arc<AtomicBool>);
    impl std::task::Wake for Push {
        fn wake(self: Arc<Self>) {
            if self.0.claim() {
                self.1.store(true, Ordering::SeqCst);
            }
        }
    }
    let notify = Waker::from(Arc::new(Push(scheduled.clone(), pushed.clone())));
    (scheduled, pushed, notify)
}

/// The scheduler side of the same race, `Cell::run` against `request_drain`: the turn frees the
/// cell's place in the run queue and then reads the flag; the drain stores the flag and then claims
/// the place. With Release/Acquire both can read stale, nobody pushes the cell, and the actor
/// parks for ever with the drain pending.
#[test]
fn a_drain_flagged_as_a_turn_starts_is_seen_by_it_or_gets_a_turn() {
    check(|| {
        let (scheduled, pushed, notify) = queued_turn();
        let address = full(Some(notify));
        address.0.drain_requested.store(false, Ordering::SeqCst);
        let drainer = address.clone();
        let drain = thread::spawn(move || drainer.0.request_drain(None));
        scheduled.begin();
        let seen = address.0.drain_requested.load(Ordering::SeqCst);
        drain.join().unwrap();
        assert!(
            seen || pushed.load(Ordering::SeqCst),
            "drain flagged as a turn started: not seen by it and given no turn"
        );
    });
}

/// The same race for a job's result, `Cell::run` against `settle` on the job's thread: the job
/// leaves its result under `jobs`, flags `jobs_settled` and claims the cell's place; the turn frees
/// the place and reads the flag through `settled`, as the driver does. For an actor in `pause_until`
/// a result neither seen nor given a turn is a hang: no message can start the turn it waits for.
/// The flag was Release/Acquire until this failed; the `jobs` lock orders nothing here, the turn
/// reads the flag without it. A sink's `Ready` and a watch's down flag it through the same
/// `flag_settled`, so this holds them too.
#[test]
fn a_job_settling_as_a_turn_starts_is_seen_by_it_or_gets_a_turn() {
    check(|| {
        let (scheduled, pushed, notify) = queued_turn();
        let mb = full(Some(notify)).0;
        // Loom leaves an atomic's initial value out of the SeqCst order: without this store a SeqCst load may read it.
        mb.jobs_settled.store(false, Ordering::SeqCst);
        let cancel = Arc::new(crate::jobs::Cancel::default());
        let live = LiveJob { cancel: cancel.clone(), kind: JobKind::Job };
        mb.jobs.lock().get_or_insert_default().live.insert(1, live);
        mb.jobs_live.store(1, Ordering::Relaxed);
        let job = {
            let mb = mb.clone();
            thread::spawn(move || crate::context::settle(&mb, 1, &cancel, crate::context::Settle::Done(1), None))
        };
        scheduled.begin();
        let seen = mb.settled();
        let taken =
            if seen { mb.jobs.lock().as_mut().map_or(0, |jobs| std::mem::take(&mut jobs.done).len()) } else { 0 };
        job.join().unwrap();
        assert_eq!(taken, seen as usize, "flag seen before the result it was set for");
        assert!(
            seen || pushed.load(Ordering::SeqCst),
            "job settled as a turn started: not seen by it and given no turn"
        );
    });
}

/// The same race for a paused actor holding another job's result: the flag is up for that one already, so the turn
/// always looks, and takes only the result of the job it is paused on. That job settles on its thread as the turn
/// starts. The turn takes its result, or the flag is up again for the next turn and that turn is queued: held results
/// raise the flag only once, so a flag cleared after the look would leave the pause on for ever.
#[test]
fn a_pausing_job_settling_as_a_turn_starts_with_results_held_is_seen_or_gets_a_turn() {
    check(|| {
        let (scheduled, pushed, notify) = queued_turn();
        let mb = full(Some(notify)).0;
        let cancel = Arc::new(crate::jobs::Cancel::default());
        {
            let mut jobs = mb.jobs.lock();
            let jobs = jobs.get_or_insert_default();
            jobs.done.push_back((1, 10, None));
            jobs.live.insert(2, LiveJob { cancel: cancel.clone(), kind: JobKind::Job });
            jobs.pausing.push(2);
        }
        mb.jobs_live.store(1, Ordering::Relaxed);
        mb.paused.store(true, Ordering::Relaxed);
        mb.jobs_settled.store(true, Ordering::SeqCst);
        let job = {
            let mb = mb.clone();
            thread::spawn(move || crate::context::settle(&mb, 2, &cancel, crate::context::Settle::Done(20), None))
        };
        scheduled.begin();
        assert!(mb.settled(), "raised for the held result");
        let (_, _, lifted) = mb.take_settled();
        let taken = mb.jobs.lock().as_mut().and_then(|jobs| jobs.next_done(true)).map(|(_, msg, _)| msg);
        job.join().unwrap();
        assert!(!lifted, "the pause holds until the result is handled");
        assert!(
            taken == Some(20) || (pushed.load(Ordering::SeqCst) && mb.settled()),
            "a pausing job settled as a turn started: not taken by it, and no turn will take it"
        );
        assert_eq!(mb.jobs.lock().as_ref().map(|jobs| jobs.done.len()), Some(1 + usize::from(taken.is_none())));
    });
}
