//! Fault loops: one scenario is run again and again with the Nth user callback panicking, for
//! N = 1, 2, ... until a run finishes without the fuse firing. Whatever panics, no turn may
//! unwind, the actor must reach an exit, and every counter must come back to zero.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon::*;

mod common;
use common::{eventually, park_on, threadless};

struct Fuse {
    calls: AtomicUsize,
    at: usize,
}

impl Fuse {
    fn hit(&self) {
        if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.at && !std::thread::panicking() {
            panic!("fuse {}", self.at);
        }
    }
}

/// Runs `run` with the fuse at 1, 2, ... until a run makes fewer fused calls than that, which must take more than `min`.
fn every_fuse(min: usize, what: impl std::fmt::Display, run: impl Fn(usize) -> usize) {
    let mut at = 1;
    while run(at) >= at {
        at += 1;
    }
    assert!(at > min, "{what}: the scenario only made {at} fused calls");
}

enum Msg {
    Work(Bomb),
    Timer,
    Job,
    Sink,
    Fired(Bomb),
    Done(Bomb),
    Fail,
}

struct Bomb(Arc<Fuse>);

impl Drop for Bomb {
    fn drop(&mut self) {
        self.0.hit();
    }
}

struct Fused {
    fuse: Arc<Fuse>,
    entered: Arc<AtomicUsize>,
    tx: Option<SinkHandle<Bomb>>,
}

/// Every step of writing can panic, and so can dropping what it writes.
struct FusedSink(Arc<Fuse>);

impl Sink<Bomb> for FusedSink {
    type Error = ();

    fn poll_ready(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), ()>> {
        self.0.hit();
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, _item: Bomb) -> Result<(), ()> {
        self.0.hit();
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), ()>> {
        self.0.hit();
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), ()>> {
        self.0.hit();
        std::task::Poll::Ready(Ok(()))
    }
}

struct LoudError(Arc<Fuse>);

impl std::fmt::Debug for LoudError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.hit();
        f.write_str("loud")
    }
}

impl Actor for Fused {
    type Message = Msg;
    type Error = LoudError;

    fn on_start(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), LoudError> {
        self.fuse.hit();
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), LoudError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.fuse.hit();
        let fuse = self.fuse.clone();
        match msg {
            Msg::Timer => {
                let _ = cx.schedule_after(Duration::from_secs(3600), Msg::Fired(Bomb(fuse.clone())));
                let _ = cx.schedule_after(Duration::from_secs(1), Msg::Fired(Bomb(fuse)));
            }
            Msg::Job => drop(cx.submit_io(move || {
                fuse.hit();
                Msg::Done(Bomb(fuse))
            })),
            Msg::Sink => {
                let events = fuse.clone();
                let map = move |_| {
                    events.hit();
                    Msg::Done(Bomb(events.clone()))
                };
                self.tx = cx.attach_sink(FusedSink(fuse), 1, map).ok();
            }
            Msg::Fail => return Err(LoudError(fuse)),
            Msg::Work(_) | Msg::Fired(_) | Msg::Done(_) => {}
        }
        Ok(())
    }

    fn on_turn_end(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), LoudError> {
        self.fuse.hit();
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(Bomb(self.fuse.clone()));
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.fuse.hit();
    }

    fn label(msg: &Msg) -> &'static str {
        match msg {
            Msg::Work(bomb) | Msg::Fired(bomb) | Msg::Done(bomb) => {
                bomb.0.hit();
                "bomb"
            }
            _ => "plain",
        }
    }
}

/// Runs the scenario with the fuse set to `at` and returns how many fused calls it made.
fn scenario(at: usize, fail: bool) -> usize {
    let fuse = Arc::new(Fuse { calls: AtomicUsize::new(0), at });
    let entered = Arc::new(AtomicUsize::new(0));
    let weigher = fuse.clone();
    let config = MailboxConfig {
        timing_sample_every: 1,
        tracer: Some(Tracer::new(64, 0)),
        ..MailboxConfig::from(MailboxLimits { messages: 8, ..Default::default() })
    }
    .with_weight(move |_: &Msg| {
        weigher.hit();
        16
    });
    let clock = Clock::manual();
    let (executor, jobs) = JobExecutor::manual(8);
    let actor = Fused { fuse: fuse.clone(), entered: entered.clone(), tx: None };
    let (mut driver, spawn) = EmbeddedDriver::with_jobs(actor, config, clock.clock(), executor).unwrap();
    let (address, life) = (spawn.address, spawn.lifecycle);
    let mut admitted = 0;
    let mut tell = |msg: Msg| {
        // A panicking weight function unwinds into the sender, which is the sender's problem;
        // the mailbox must not have counted the message.
        if let Ok(Ok(_)) = catch_unwind(AssertUnwindSafe(|| address.try_tell(msg).map_err(drop))) {
            admitted += 1;
        }
    };

    TraceContext::root().enter(|| {
        driver.run_turn(TurnBudget::default());
        tell(Msg::Work(Bomb(fuse.clone())));
        tell(Msg::Timer);
        tell(Msg::Job);
        tell(Msg::Sink);
        tell(Msg::Work(Bomb(fuse.clone())));
        driver.run_turn(TurnBudget::default());
        clock.advance(Duration::from_secs(1));
        driver.run_turn(TurnBudget::default());
        jobs.run_next();
        driver.run_turn(TurnBudget::default());
        tell(Msg::Work(Bomb(fuse.clone())));
        if fail {
            tell(Msg::Fail);
        }
        tell(Msg::Work(Bomb(fuse.clone())));
        life.drain(None);
        for _ in 0..8 {
            jobs.run_next();
            driver.run_turn(TurnBudget::default());
        }
    });
    while jobs.run_next() {}
    driver.run_turn(TurnBudget::default());

    let exit = life.exit().unwrap_or_else(|| panic!("fuse {at}: no exit"));
    let d = address.diagnostics();
    assert_eq!(
        (d.mailbox_depth, d.mailbox_bytes, d.reservations, d.waiters, d.requests, d.jobs, d.timers),
        (0, 0, 0, 0, 0, 0, 0),
        "fuse {at}: {:?}",
        exit.reason
    );
    assert_eq!(d.admitted, admitted, "fuse {at}: a message was counted that its sender saw refused");
    assert_eq!(clock.clock().pending_wakes(), 0, "fuse {at}");
    let fired = fuse.calls.load(Ordering::SeqCst) >= at;
    if !fired {
        let expected = if fail { StopReason::Failed("loud".into()) } else { StopReason::Drained };
        assert_eq!((&exit.reason, exit.pending_timers), (&expected, 1), "the hour-long timer never fired");
    }
    assert!(exit.pending_timers <= 2, "fuse {at}: {} timers reported of 2 ever set", exit.pending_timers);
    // Every admitted message reached the handler or is reported as discarded. The one exception is the
    // message in hand when its label panicked: like one whose handler panicked, it counts as delivered.
    let in_hand = (exit.reason == StopReason::Panicked) as usize;
    let internal = entered.load(Ordering::SeqCst) + exit.discarded_messages() + in_hand;
    assert!(internal as u64 >= admitted, "fuse {at}: {internal} accounted of {admitted} admitted, {:?}", exit.reason);
    fuse.calls.load(Ordering::SeqCst)
}

#[test]
fn a_panic_at_any_callback_never_unwinds_a_turn_or_leaks_accounting() {
    for fail in [false, true] {
        every_fuse(25, format_args!("fail {fail}"), |at| scenario(at, fail));
    }
}

enum Seen {
    Watch(LifecycleHandle),
    Nop,
    Cancel,
    Down(#[allow(dead_code)] Bomb),
}

/// Watches one actor. Its handler, its stop, the watch's map and the map's captures dropped unused can all panic.
struct Watcher {
    fuse: Arc<Fuse>,
    watch: Option<WatchHandle>,
    downs: Arc<AtomicUsize>,
}

impl Actor for Watcher {
    type Message = Seen;
    type Error = ();

    fn on_message(&mut self, msg: Seen, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        self.fuse.hit();
        match msg {
            Seen::Watch(target) => {
                let bomb = Bomb(self.fuse.clone());
                let map = move |_: Exit| {
                    bomb.0.hit();
                    Seen::Down(bomb)
                };
                self.watch = cx.watch(&target, map).ok();
            }
            Seen::Cancel => self.watch.iter().for_each(WatchHandle::cancel),
            Seen::Down(_) => {
                self.downs.fetch_add(1, Ordering::SeqCst);
            }
            Seen::Nop => {}
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.fuse.hit();
    }
}

struct Watched(Arc<Fuse>);

impl Actor for Watched {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.0.hit();
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Order {
    WatcherFirst,
    WatchedFirst,
    /// The watched actor ends, and the watcher is stopped before it handles the down.
    WatcherStopsBeforeTheDown,
    Cancelled,
    AlreadyEnded,
    WatcherDrains,
}

/// Runs one stop order with the fuse set to `at`, and returns how many fused calls it made.
fn watch_scenario(order: Order, at: usize) -> usize {
    let fuse = Arc::new(Fuse { calls: AtomicUsize::new(0), at });
    let downs = Arc::new(AtomicUsize::new(0));
    let clock = Clock::manual();
    let (mut watched, target) =
        EmbeddedDriver::new(Watched(fuse.clone()), MailboxLimits::default(), clock.clock()).unwrap();
    let actor = Watcher { fuse: fuse.clone(), watch: None, downs: downs.clone() };
    let limits = MailboxLimits { messages: 1, ..Default::default() };
    let (mut watcher, spawn) = EmbeddedDriver::new(actor, limits, clock.clock()).unwrap();
    let (to, life) = (spawn.address, spawn.lifecycle);
    let turn = |driver: &mut EmbeddedDriver<Watcher>, msg: Option<Seen>| {
        if let Some(msg) = msg {
            let _ = to.try_tell(msg);
        }
        driver.run_turn(TurnBudget::default());
    };
    let end = |watched: &mut EmbeddedDriver<Watched>| {
        target.lifecycle.stop();
        watched.run_turn(TurnBudget::default());
    };
    watched.run_turn(TurnBudget::default());
    turn(&mut watcher, None);
    if order == Order::AlreadyEnded {
        end(&mut watched);
    }
    turn(&mut watcher, Some(Seen::Watch(target.lifecycle.clone())));
    // A full mailbox: the down must come in past it.
    let _ = to.try_tell(Seen::Nop);
    match order {
        Order::WatcherFirst => {
            life.stop();
            turn(&mut watcher, None);
            end(&mut watched);
        }
        Order::WatchedFirst => {
            end(&mut watched);
            turn(&mut watcher, None);
            life.stop();
            turn(&mut watcher, None);
        }
        Order::WatcherStopsBeforeTheDown | Order::AlreadyEnded => {
            end(&mut watched);
            life.stop();
            turn(&mut watcher, None);
        }
        Order::Cancelled => {
            turn(&mut watcher, None);
            turn(&mut watcher, Some(Seen::Cancel));
            end(&mut watched);
            turn(&mut watcher, None);
            life.stop();
            turn(&mut watcher, None);
        }
        Order::WatcherDrains => {
            life.drain(None);
            turn(&mut watcher, None);
            turn(&mut watcher, None);
            end(&mut watched);
        }
    }

    let exit = life.exit().unwrap_or_else(|| panic!("{order:?} fuse {at}: no exit"));
    assert!(target.lifecycle.exit().is_some(), "{order:?} fuse {at}: the watched actor did not end");
    let d = to.diagnostics();
    assert_eq!((d.mailbox_depth, d.jobs, d.timers), (0, 0, 0), "{order:?} fuse {at}: {:?}", exit.reason);
    assert_eq!(clock.clock().pending_wakes(), 0, "{order:?} fuse {at}");
    // The map, what it captured and the down it made are gone from both ends: only this test holds the fuse.
    assert_eq!(Arc::strong_count(&fuse), 1, "{order:?} fuse {at}: a watch outlived both actors");
    let told = downs.load(Ordering::SeqCst);
    let fired = fuse.calls.load(Ordering::SeqCst) >= at;
    let expected = usize::from(order == Order::WatchedFirst);
    assert!(told <= expected, "{order:?} fuse {at}: told {told} times");
    if !fired {
        assert_eq!(told, expected, "{order:?}: told {told} times");
    }
    fuse.calls.load(Ordering::SeqCst)
}

#[test]
fn no_watch_outlives_both_actors_or_tells_twice_in_any_stop_order_whatever_panics() {
    for order in [
        Order::WatcherFirst,
        Order::WatchedFirst,
        Order::WatcherStopsBeforeTheDown,
        Order::Cancelled,
        Order::AlreadyEnded,
        Order::WatcherDrains,
    ] {
        every_fuse(4, format_args!("{order:?}"), |at| watch_scenario(order, at));
    }
}

enum Knot {
    Link(usize, LifecycleHandle, #[allow(dead_code)] Bomb),
    Fail,
}

/// Links as told, and notes each link it made. Its handler, its stop and what a link message carried can all panic.
struct Knotted {
    me: usize,
    fuse: Arc<Fuse>,
    links: Arc<Mutex<Vec<(usize, usize)>>>,
    stops: Arc<AtomicUsize>,
}

impl Actor for Knotted {
    type Message = Knot;
    type Error = ();

    fn on_message(&mut self, msg: Knot, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        self.fuse.hit();
        match msg {
            Knot::Link(peer, to, _bomb) => {
                cx.link(&to);
                self.links.lock().unwrap().push((self.me, peer));
            }
            Knot::Fail => return Err(()),
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.stops.fetch_add(1, Ordering::SeqCst);
        self.fuse.hit();
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FirstEnd {
    Fails,
    Stops,
    Drains,
}

/// Links three actors in a chain, or a cycle, ends the first, then stops whoever is left, with the fuse set to `at`.
fn link_scenario(cycle: bool, first: FirstEnd, at: usize) -> usize {
    let fuse = Arc::new(Fuse { calls: AtomicUsize::new(0), at });
    let (links, stops) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicUsize::new(0)));
    let clock = Clock::manual();
    let (mut drivers, spawns): (Vec<_>, Vec<_>) = (0..3)
        .map(|me| {
            let actor = Knotted { me, fuse: fuse.clone(), links: links.clone(), stops: stops.clone() };
            EmbeddedDriver::new(actor, MailboxLimits::default(), clock.clock()).unwrap()
        })
        .unzip();
    let settle = |drivers: &mut Vec<EmbeddedDriver<Knotted>>| {
        for _ in 0..4 {
            for d in drivers.iter_mut() {
                d.run_turn(TurnBudget::default());
            }
        }
    };
    let link = |from: usize, to: usize| {
        let _ = spawns[from].address.try_tell(Knot::Link(to, spawns[to].lifecycle.clone(), Bomb(fuse.clone())));
    };
    // All started first: one stopped before its start runs no on_stop.
    settle(&mut drivers);
    link(1, 0);
    link(1, 2);
    if cycle {
        link(2, 0);
    }
    settle(&mut drivers);
    match first {
        FirstEnd::Fails => drop(spawns[0].address.try_tell(Knot::Fail)),
        FirstEnd::Stops => spawns[0].lifecycle.stop(),
        FirstEnd::Drains => spawns[0].lifecycle.drain(None),
    }
    settle(&mut drivers);

    let case = format!("cycle {cycle}, {first:?}, fuse {at}");
    let reasons: Vec<_> = spawns.iter().map(|s| s.lifecycle.exit().map(|e| e.reason)).collect();
    for &(a, b) in links.lock().unwrap().iter() {
        for (x, y) in [(a, b), (b, a)] {
            if reasons[x].as_ref().is_some_and(StopReason::is_failure) {
                assert!(reasons[y].is_some(), "{case}: {x} failed and {y}, linked to it, runs on: {reasons:?}");
            }
        }
    }
    if fuse.calls.load(Ordering::SeqCst) < at {
        let by = |i: usize| Some(StopReason::Linked(spawns[i].lifecycle.incarnation()));
        match first {
            // In the cycle the third is linked to the first too, so it hears of the failure from it.
            FirstEnd::Fails => assert_eq!(reasons[1..], [by(0), by(if cycle { 0 } else { 1 })], "{case}"),
            _ => assert_eq!(reasons[1..], [None, None], "{case}"),
        }
    }
    spawns.iter().for_each(|s| s.lifecycle.stop());
    settle(&mut drivers);

    for s in &spawns {
        assert!(s.lifecycle.exit().is_some(), "{case}: no exit");
        let d = s.address.diagnostics();
        assert_eq!((d.mailbox_depth, d.jobs, d.timers), (0, 0, 0), "{case}");
    }
    assert_eq!(stops.load(Ordering::SeqCst), 3, "{case}: each actor stops once");
    assert_eq!(clock.clock().pending_wakes(), 0, "{case}");
    assert_eq!(Arc::strong_count(&fuse), 1, "{case}: something outlived the actors");
    fuse.calls.load(Ordering::SeqCst)
}

#[test]
fn a_linked_failure_reaches_every_linked_actor_once_and_a_stop_none_whatever_panics() {
    for cycle in [false, true] {
        for first in [FirstEnd::Fails, FirstEnd::Stops, FirstEnd::Drains] {
            every_fuse(4, format_args!("cycle {cycle}, {first:?}"), |at| link_scenario(cycle, first, at));
        }
    }
}

enum Kept {
    Hold(Bomb),
    Open,
    Close,
    Fail,
}

/// Stashes each `Hold` while closed and replays them when opened. Its handler, its stop, a message's label and a
/// held message dropped, handled or discarded, can all panic.
struct Keeper {
    fuse: Arc<Fuse>,
    open: bool,
    handled: Arc<AtomicUsize>,
}

impl Actor for Keeper {
    type Message = Kept;
    type Error = LoudError;

    fn on_message(&mut self, msg: Kept, cx: &mut Context<'_, Self>) -> Result<(), LoudError> {
        self.fuse.hit();
        match msg {
            Kept::Hold(bomb) if self.open => {
                self.handled.fetch_add(1, Ordering::SeqCst);
                drop(bomb);
            }
            Kept::Hold(bomb) => drop(cx.stash(Kept::Hold(bomb))),
            Kept::Open => {
                self.open = true;
                cx.unstash_all();
            }
            Kept::Close => self.open = false,
            Kept::Fail => return Err(LoudError(self.fuse.clone())),
        }
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.fuse.hit();
    }

    fn label(msg: &Kept) -> &'static str {
        match msg {
            Kept::Hold(bomb) => {
                bomb.0.hit();
                "bomb"
            }
            _ => "plain",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum KeeperEnd {
    Stops,
    Drains,
    Fails,
    Dropped,
}

/// Stashes, replays part of it and stashes again, then ends the actor with some messages stashed and some released,
/// with the fuse set to `at`. Returns how many fused calls it made.
fn stash_scenario(end: KeeperEnd, at: usize) -> usize {
    let fuse = Arc::new(Fuse { calls: AtomicUsize::new(0), at });
    let handled = Arc::new(AtomicUsize::new(0));
    let config = MailboxConfig {
        timing_sample_every: 1,
        tracer: Some(Tracer::new(64, 0)),
        ..MailboxConfig::from(MailboxLimits { messages: 8, ..Default::default() })
    };
    let clock = Clock::manual();
    let actor = Keeper { fuse: fuse.clone(), open: false, handled: handled.clone() };
    let (mut driver, spawn) = EmbeddedDriver::new(actor, config, clock.clock()).unwrap();
    let (address, life) = (spawn.address, spawn.lifecycle);
    let mut holds = 0;
    let mut tell = |msg: Kept| {
        let hold = matches!(msg, Kept::Hold(_));
        if address.try_tell(msg).is_ok() {
            holds += usize::from(hold);
        }
    };
    let hold = |fuse: &Arc<Fuse>| Kept::Hold(Bomb(fuse.clone()));

    TraceContext::root().enter(|| {
        driver.run_turn(TurnBudget::default());
        (0..3).for_each(|_| tell(hold(&fuse)));
        driver.run_turn(TurnBudget::default());
        [Kept::Open, hold(&fuse), Kept::Close, hold(&fuse)].into_iter().for_each(&mut tell);
        driver.run_turn(TurnBudget::default());
        driver.run_turn(TurnBudget::messages(2));
        driver.run_turn(TurnBudget::default());
        (0..2).for_each(|_| tell(hold(&fuse)));
        driver.run_turn(TurnBudget::default());
        // Released and not yet replayed, with a close and a hold still in the mailbox behind them.
        [Kept::Open, Kept::Close, hold(&fuse)].into_iter().for_each(&mut tell);
        driver.run_turn(TurnBudget::default());
    });
    match end {
        KeeperEnd::Stops => life.stop(),
        KeeperEnd::Drains => life.drain(None),
        KeeperEnd::Fails => tell(Kept::Fail),
        KeeperEnd::Dropped => {}
    }
    if end == KeeperEnd::Dropped {
        drop(driver);
    } else {
        for _ in 0..4 {
            driver.run_turn(TurnBudget::default());
        }
    }

    let case = format!("{end:?} fuse {at}");
    let exit = life.exit().unwrap_or_else(|| panic!("{case}: no exit"));
    let d = address.diagnostics();
    assert_eq!((d.mailbox_depth, d.mailbox_bytes, d.timers), (0, 0, 0), "{case}: {:?}", exit.reason);
    assert_eq!(clock.clock().pending_wakes(), 0, "{case}");
    assert_eq!(Arc::strong_count(&fuse), 1, "{case}: a stashed message outlived the actor");
    // Every hold was handled or is counted as discarded, bar the one in hand when a handler or a label panicked.
    let in_hand = usize::from(exit.reason == StopReason::Panicked);
    let accounted = handled.load(Ordering::SeqCst) + exit.discarded_messages() + in_hand;
    assert!(accounted >= holds, "{case}: {accounted} accounted of {holds} holds, {:?}", exit.reason);
    if fuse.calls.load(Ordering::SeqCst) < at {
        let expected = match end {
            KeeperEnd::Stops => (StopReason::Stopped, 5),
            KeeperEnd::Drains => (StopReason::Drained, 1),
            KeeperEnd::Fails => (StopReason::Failed("loud".into()), 1),
            KeeperEnd::Dropped => (StopReason::DriverDropped, 5),
        };
        assert_eq!((exit.reason.clone(), exit.discarded_messages()), expected, "{case}");
        assert_eq!(
            handled.load(Ordering::SeqCst),
            4 + usize::from(end != KeeperEnd::Stops && end != KeeperEnd::Dropped) * 3
        );
    }
    fuse.calls.load(Ordering::SeqCst)
}

#[test]
fn a_stash_leaks_nothing_and_loses_nothing_uncounted_on_any_exit_whatever_panics() {
    for end in [KeeperEnd::Stops, KeeperEnd::Drains, KeeperEnd::Fails, KeeperEnd::Dropped] {
        every_fuse(20, format_args!("{end:?}"), |at| stash_scenario(end, at));
    }
}

struct Crasher;

impl Actor for Crasher {
    type Message = ();
    type Error = &'static str;

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Err("crash")
    }
}

/// Crashes the current generation and runs the family until the next one is up, or it has ended.
fn crash(
    family: &mut Supervised<Crasher, impl FnMut() -> Crasher>,
    handle: &SupervisionHandle<()>,
    clock: &ManualClock,
) -> SupervisionReport {
    handle.current().unwrap().try_tell(()).unwrap();
    let mut report = family.run_turn(TurnBudget::default());
    if let Some(after) = report.next_restart {
        clock.advance(after.saturating_sub(clock.clock().now()));
        report = family.run_turn(TurnBudget::default());
        family.run_turn(TurnBudget::default());
    }
    report
}

#[test]
fn a_supervisor_outlives_a_factory_and_a_policy_that_panic() {
    let clock = Clock::manual();
    let policy = || RestartPolicy { backoff: Backoff { jitter: 0.0, ..RestartPolicy::BACKOFF }, ..Default::default() };
    let never =
        Supervised::new(|| -> Crasher { panic!("no actor") }, MailboxLimits::default(), clock.clock(), policy());
    assert!(matches!(never.map(drop), Err(SpawnError::FactoryPanicked)));

    let mut built = 0;
    let factory = move || {
        built += 1;
        assert!(built != 3, "the third build panics");
        Crasher
    };
    let (mut family, handle) = Supervised::new(factory, MailboxLimits::default(), clock.clock(), policy()).unwrap();
    family.run_turn(TurnBudget::default());
    assert!(crash(&mut family, &handle, &clock).replaced);
    let report = crash(&mut family, &handle, &clock);
    assert!(report.escalated && report.terminal.is_some(), "a factory that panics on a restart ends the family");
    assert_eq!((handle.lifecycle().phase(), handle.current().is_none(), handle.restarts()), (Phase::Stopped, true, 1));
    assert_eq!(clock.clock().pending_wakes(), 0);

    let policy = RestartPolicy { should_restart: Arc::new(|_| panic!("no opinion")), ..policy() };
    let (mut family, handle) = Supervised::new(|| Crasher, MailboxLimits::default(), clock.clock(), policy).unwrap();
    family.run_turn(TurnBudget::default());
    assert!(crash(&mut family, &handle, &clock).escalated);
}

#[test]
fn the_restart_budget_counts_only_failures_inside_its_window() {
    let clock = Clock::manual();
    let backoff = Backoff { jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { max_restarts: 2, within: Duration::from_secs(60), backoff, ..Default::default() };
    let (mut family, handle) = Supervised::new(|| Crasher, MailboxLimits::default(), clock.clock(), policy).unwrap();
    family.run_turn(TurnBudget::default());
    for round in 0..5 {
        assert!(crash(&mut family, &handle, &clock).replaced, "round {round}");
        assert!(crash(&mut family, &handle, &clock).replaced, "round {round}");
        clock.advance(Duration::from_secs(61));
    }
    assert!(crash(&mut family, &handle, &clock).replaced);
    assert!(crash(&mut family, &handle, &clock).replaced);
    assert!(crash(&mut family, &handle, &clock).escalated, "a third failure inside the window");
    assert_eq!(handle.restarts(), 12);
}

/// A restart's and a redial's wait doubles per failure up to `max`, and the jitter only cuts it: no wait is longer than
/// `max`, so a caller can bound one.
#[test]
fn a_backoff_doubles_up_to_its_max_and_its_jitter_only_cuts() {
    // Steps a draw cannot hide: half of each may be cut away.
    let backoff = Backoff { initial: Duration::from_millis(100), max: Duration::from_millis(500), jitter: 0.5 };
    for _ in 0..1000 {
        for (failures, full) in [(1, 100), (2, 200), (3, 400), (4, 500), (40, 500)] {
            let (wait, full) = (backoff.delay(failures), Duration::from_millis(full));
            assert!(wait <= full && wait >= full / 2, "after {failures} failures: {wait:?}");
        }
    }
    assert_eq!(Backoff { jitter: 0.0, ..backoff }.delay(3), Duration::from_millis(400));
}

enum Ask {
    Timers,
    Jobs,
    Adopt(LifecycleHandle),
    WhileDraining,
    Tick(u8),
}

struct Limited(Arc<std::sync::Mutex<Vec<String>>>);

impl Actor for Limited {
    type Message = Ask;
    type Error = ();

    fn on_message(&mut self, msg: Ask, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let mut log = self.0.lock().unwrap();
        match msg {
            Ask::Timers => {
                for n in 0..3 {
                    let refused = cx.schedule_after(Duration::from_secs(1), Ask::Tick(n)).err();
                    log.push(format!(
                        "timer {n}: {:?}",
                        refused.map(|e| (e.reason, matches!(e.message, Some(Ask::Tick(2)))))
                    ));
                }
            }
            Ask::Jobs => {
                for op in [1, 1, 2, 3] {
                    log.push(format!("job {op}: {:?}", cx.submit_io(|| Ask::Tick(9)).err().map(|e| e.reason)));
                }
            }
            Ask::Adopt(child) => log.push(format!("child: {}", cx.adopt_child(child).is_ok())),
            Ask::WhileDraining => {
                cx.drain(None);
                let timer = cx.schedule_after(Duration::from_secs(1), Ask::Tick(0)).err().map(|e| e.reason);
                let every = cx.schedule_every(Duration::from_secs(1), || Ask::Tick(0)).err().map(|e| e.reason);
                log.push(format!(
                    "draining: {timer:?} {every:?} {:?}",
                    cx.submit_io(|| Ask::Tick(0)).err().map(|e| e.reason)
                ));
            }
            Ask::Tick(n) => log.push(format!("tick {n}")),
        }
        Ok(())
    }
}

#[test]
fn timer_job_and_child_limits_refuse_hand_the_payload_back_and_free_up_again() {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let limits = MailboxLimits { timers: 2, jobs: 2, children: 1, ..Default::default() };
    let clock = Clock::manual();
    let (executor, jobs) = JobExecutor::manual(8);
    let (mut driver, spawn) = EmbeddedDriver::with_jobs(Limited(log.clone()), limits, clock.clock(), executor).unwrap();
    let child = || EmbeddedDriver::new(Limited(Arc::default()), MailboxLimits::default(), clock.clock()).unwrap();
    let ((_first, a), (mut second, b), (_third, c)) = (child(), child(), child());
    driver.run_turn(TurnBudget::default());
    let step = |driver: &mut EmbeddedDriver<Limited>, msg| {
        spawn.address.try_tell(msg).ok().unwrap();
        driver.run_turn(TurnBudget::default());
        std::mem::take(&mut *log.lock().unwrap())
    };

    assert_eq!(step(&mut driver, Ask::Timers), ["timer 0: None", "timer 1: None", "timer 2: Some((Full, true))"]);
    assert_eq!(step(&mut driver, Ask::Jobs), ["job 1: None", "job 1: None", "job 2: Some(Full)", "job 3: Some(Full)"]);
    let d = spawn.address.diagnostics();
    assert_eq!((d.timers, d.jobs), (2, 2));

    clock.advance(Duration::from_secs(1));
    while jobs.run_next() {}
    driver.run_turn(TurnBudget::default());
    driver.run_turn(TurnBudget::default());
    let mut ticks = std::mem::take(&mut *log.lock().unwrap());
    ticks.sort();
    assert_eq!(ticks, ["tick 0", "tick 1", "tick 9", "tick 9"]);
    let d = spawn.address.diagnostics();
    assert_eq!((d.timers, d.jobs), (0, 0));
    assert_eq!(
        step(&mut driver, Ask::Timers)[2],
        "timer 2: Some((Full, true))",
        "the limit is per live timer, not per life"
    );

    assert_eq!(step(&mut driver, Ask::Adopt(a.lifecycle)), ["child: true"]);
    assert_eq!(step(&mut driver, Ask::Adopt(b.lifecycle.clone())), ["child: false"]);
    b.lifecycle.stop();
    second.run_turn(TurnBudget::default());
    assert_eq!(step(&mut driver, Ask::Adopt(c.lifecycle)), ["child: false"], "a live child still holds the slot");

    assert_eq!(step(&mut driver, Ask::WhileDraining), ["draining: Some(Draining) Some(Draining) Some(Draining)"]);
}

/// A source that never ends.
struct Silent;

impl Stream for Silent {
    type Item = u32;

    fn poll_next(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Option<u32>> {
        std::task::Poll::Pending
    }
}

enum Lossy {
    Start,
    Fail,
    Done,
}

struct Lost;

impl Actor for Lost {
    type Message = Lossy;
    type Error = &'static str;

    fn on_message(&mut self, msg: Lossy, cx: &mut Context<'_, Self>) -> Result<(), &'static str> {
        match msg {
            Lossy::Start => {
                cx.submit_io(|| Lossy::Done).map_err(|_| "refused")?;
                cx.attach(Silent, |_| Lossy::Done).map_err(|_| "refused")?;
            }
            Lossy::Fail => return Err("boom"),
            Lossy::Done => {}
        }
        Ok(())
    }
}

/// An executor that drops a job and a source it was given, unrun: on every exit each settles once, as cancelled when
/// the actor had cancelled it and as the actor's failure otherwise, and nothing waits for it.
#[test]
fn a_job_its_executor_loses_settles_once_on_any_exit_and_nothing_waits_for_it() {
    let lost = StopReason::Failed("a job was dropped by its executor".into());
    for end in ["none", "stop", "drain", "deadline", "fail"] {
        let clock = Clock::manual();
        let (executor, lose) = common::losing();
        let (mut driver, spawn) =
            EmbeddedDriver::with_jobs(Lost, MailboxLimits::default(), clock.clock(), executor).unwrap();
        spawn.address.try_tell(Lossy::Start).ok().unwrap();
        driver.run_turn(TurnBudget::default());
        assert_eq!(spawn.address.diagnostics().jobs, 2);
        match end {
            "stop" => spawn.lifecycle.stop(),
            "drain" => spawn.lifecycle.drain(None),
            "deadline" => {
                spawn.lifecycle.drain(Some(Duration::from_secs(1)));
                clock.advance(Duration::from_secs(1));
            }
            "fail" => drop(spawn.address.try_tell(Lossy::Fail).ok().unwrap()),
            _ => {}
        }
        driver.run_turn(TurnBudget::default());
        lose();
        assert_eq!(driver.run_until_idle(4).map_err(drop), Ok(0), "{end}: nothing is owed or waits");
        let exit = spawn.lifecycle.exit().unwrap_or_else(|| panic!("{end}: the actor ended"));
        let reason = match end {
            "stop" => StopReason::Stopped,
            "deadline" => StopReason::DrainDeadline,
            "fail" => StopReason::Failed("\"boom\"".into()),
            // The drain waits for the job, which is lost, not cancelled.
            _ => lost.clone(),
        };
        assert_eq!(exit.reason, reason, "{end}");
        assert_eq!(spawn.address.diagnostics().jobs, 0, "{end}: each settled once");
    }
}

struct Batcher {
    take: usize,
    panic_at: Option<u32>,
    taken: Arc<AtomicUsize>,
}

impl Actor for Batcher {
    type Message = u32;
    type Error = ();

    fn on_message(&mut self, _: u32, _: &mut Context<'_, Self>) -> Result<(), ()> {
        unreachable!("on_batch is overridden")
    }

    fn on_batch(&mut self, batch: &mut MailboxBatch<'_, u32>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        for msg in batch.by_ref().take(self.take) {
            assert!(Some(msg) != self.panic_at, "message {msg}");
            self.taken.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[test]
fn a_custom_on_batch_that_stalls_or_panics_ends_the_actor_with_every_message_accounted() {
    for (take, panic_at, reason) in
        [(0, None, StopReason::Failed("on_batch took no message".into())), (usize::MAX, Some(4), StopReason::Panicked)]
    {
        let taken = Arc::new(AtomicUsize::new(0));
        let actor = Batcher { take, panic_at, taken: taken.clone() };
        let (mut driver, spawn) =
            EmbeddedDriver::new(actor, MailboxLimits::default(), Clock::manual().clock()).unwrap();
        driver.run_turn(TurnBudget::default());
        (0..10).for_each(|n| drop(spawn.address.try_tell(n)));
        let turns = (0..5).take_while(|_| !driver.run_turn(TurnBudget::default()).stopped).count();
        assert!(turns < 5, "{reason:?}: the actor kept being handed the batch");
        let exit = spawn.lifecycle.exit().unwrap();
        let in_hand = panic_at.is_some() as usize;
        assert_eq!(exit.reason, reason);
        assert_eq!(taken.load(Ordering::SeqCst) + in_hand + exit.discarded_messages(), 10);
        let d = spawn.address.diagnostics();
        assert_eq!((d.mailbox_depth, d.mailbox_bytes), (0, 0));
    }
}

/// What a topic publishes: cloning it for each subscriber, and dropping it, can panic.
struct Pulse(Bomb);

impl Clone for Pulse {
    fn clone(&self) -> Self {
        self.0.0.hit();
        Pulse(Bomb(self.0.0.clone()))
    }
}

enum Heard {
    Pulse(#[allow(dead_code)] Pulse),
    Fail,
    Panic,
}

impl From<Pulse> for Heard {
    fn from(pulse: Pulse) -> Self {
        Heard::Pulse(pulse)
    }
}

struct Listener(Arc<Fuse>);

impl Actor for Listener {
    type Message = Heard;
    type Error = ();

    fn on_start(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.hit();
        Ok(())
    }

    fn on_message(&mut self, msg: Heard, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.hit();
        match msg {
            Heard::Pulse(_) => Ok(()),
            Heard::Fail => Err(()),
            Heard::Panic => panic!("listener"),
        }
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        self.0.hit();
    }
}

/// Subscribers that fail, panic, are stopped, and restart under supervision while pulses are published, with the fuse
/// set to `at`; returns how many fused calls it made.
fn topic_scenario(at: usize) -> usize {
    let fuse = Arc::new(Fuse { calls: AtomicUsize::new(0), at });
    let tokio = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
    tokio.block_on(async {
        let (system, _) = threadless();
        let topic = system.topic::<Heard>("pulses").unwrap();
        let limits = MailboxLimits { messages: 4, ..Default::default() };
        let spawn = || system.spawn(Listener(fuse.clone())).mailbox(limits).start().unwrap();
        let ends = [spawn(), spawn(), spawn()];
        let factory = fuse.clone();
        let backoff = Backoff { jitter: 0.0, ..RestartPolicy::BACKOFF };
        let policy = RestartPolicy { max_restarts: 1000, backoff, ..Default::default() };
        let family = system.supervise(move || Listener(factory.clone())).mailbox(limits).policy(policy);
        let family = family.start_supervised().unwrap();
        for end in &ends {
            topic.subscribe(end.clone()).unwrap();
        }
        topic.subscribe(family.clone()).unwrap();
        let publish = || {
            for _ in 0..2 {
                let pulse = Pulse(Bomb(fuse.clone()));
                let _ = catch_unwind(AssertUnwindSafe(|| topic.publish(pulse)));
            }
        };
        let settle = || tokio::time::sleep(Duration::from_millis(50));
        settle().await;
        publish();
        settle().await;
        let _ = ends[0].tell(Heard::Fail);
        let _ = ends[1].tell(Heard::Panic);
        ends[2].lifecycle.stop();
        let _ = family.tell(Heard::Fail);
        publish();
        settle().await;
        publish();
        for end in &ends {
            end.join().await;
        }
        let alive = family.lifecycle.phase() != Phase::Stopped;
        assert_eq!(topic.subscribers(), alive as usize, "fuse {at}: only the family is left, while it runs");
        family.lifecycle.stop();
        family.join().await;
        assert_eq!(topic.subscribers(), 0, "fuse {at}");
        drop(topic);
        assert!(system.topic::<()>("pulses").is_ok(), "fuse {at}: the topic outlived its handles and subscribers");
        system.shutdown().await.unwrap();
    });
    drop(tokio);
    assert_eq!(Arc::strong_count(&fuse), 1, "fuse {at}: a pulse or a subscriber outlived the system");
    fuse.calls.load(Ordering::SeqCst)
}

#[test]
fn a_topic_drops_subscribers_that_fail_panic_or_stop_keeps_a_restarting_family_and_leaks_nothing_whatever_panics() {
    every_fuse(20, "topic", topic_scenario);
}

/// Its drain waits on a job that never ends, as an actor's behind a disk that does not answer.
struct Stalled;

impl Actor for Stalled {
    type Message = ();
    type Error = ();

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        cx.submit_future(std::future::pending()).map(drop).map_err(drop)
    }

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

/// Hangs where it is told to, elsewhere uses its whole shutdown budget, and notes the budget its shutdown was given.
struct Slow {
    at: usize,
    hang_leaving: bool,
    hang_shutdown: bool,
    given: Arc<Mutex<Vec<(usize, Duration)>>>,
}

async fn hang_if(hang: bool) -> Result<(), String> {
    if hang {
        std::future::pending::<()>().await;
    }
    Ok(())
}

impl SystemExtension for Slow {
    fn name(&self) -> &str {
        "slow"
    }

    fn leaving(&self, _: &LeaveReason, _: Duration) -> ExtensionFuture {
        Box::pin(hang_if(self.hang_leaving))
    }

    fn shutdown(&self, budget: Duration) -> ExtensionFuture {
        self.given.lock().unwrap().push((self.at, budget));
        let hang = self.hang_shutdown;
        Box::pin(async move {
            hang_if(hang).await?;
            // Gives up at its budget in a task of its own, as the cluster's flush to a paused peer does.
            drop(tokio::spawn(tokio::time::timeout(budget, std::future::pending::<()>())).await);
            Ok(())
        })
    }
}

const EXTENSIONS: usize = 3;

/// With `stalled`, an actor whose drain never ends; the extensions at `leaving` and `shutdown` hang there.
fn shutdown_scenario(stalled: bool, leaving: Option<usize>, shutdown: Option<usize>) {
    let case = format!("stalled {stalled}, leaving hangs at {leaving:?}, shutdown at {shutdown:?}");
    let given = Arc::new(Mutex::new(Vec::new()));
    let tokio = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
    tokio.block_on(async {
        let (system, _) = threadless();
        for at in 0..EXTENSIONS {
            let (hang_leaving, hang_shutdown) = (leaving == Some(at), shutdown == Some(at));
            let slow = Slow { at, hang_leaving, hang_shutdown, given: given.clone() };
            system.attach_extension(Arc::new(slow)).unwrap();
        }
        if stalled {
            system.spawn(Stalled).start().unwrap().ready().await.unwrap();
        }
        let (began, timeout) = (tokio::time::Instant::now(), Duration::from_secs(5));
        let report = system.shutdown().await.unwrap();
        assert!(began.elapsed() <= timeout, "{case}: took {:?}", began.elapsed());
        let ends: Vec<_> = report.actors.iter().map(|e| e.reason.clone()).collect();
        assert_eq!(ends, if stalled { vec![StopReason::DrainDeadline] } else { vec![] }, "{case}");
        assert!(report.still_running.is_empty(), "{case}: an actor ended at its drain deadline is not running");
        let hung = (leaving.is_some() as usize, shutdown.is_some() as usize);
        assert_eq!((report.warnings.len(), report.failures.len()), hung, "{case}");
        // Whatever went before it, each extension gets its share of the eighth of the timeout kept from the actors.
        let given = given.lock().unwrap().clone();
        assert_eq!(given.iter().map(|g| g.0).collect::<Vec<_>>(), (0..EXTENSIONS).rev().collect::<Vec<_>>(), "{case}");
        assert!(given.iter().all(|g| g.1 >= timeout / 8 / EXTENSIONS as u32), "{case}: {given:?}");
    });
    drop(tokio);
    assert_eq!(Arc::strong_count(&given), 1, "{case}: an extension outlived the system");
}

#[test]
fn a_shutdown_step_that_hangs_leaves_each_extension_after_it_a_share_and_all_end_within_the_timeout() {
    for stalled in [true, false] {
        for leaving in [None, Some(0), Some(EXTENSIONS - 1)] {
            for shutdown in [None, Some(0), Some(EXTENSIONS - 1)] {
                shutdown_scenario(stalled, leaving, shutdown);
            }
        }
    }
}

/// How the actor an ask waits on ends it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AskEnd {
    Answered,
    Stop,
    Drain,
    Panic,
    Restart,
    Shutdown,
    Deadline,
    Dropped,
}

enum Asked {
    Hold(ReplyTo<u32>),
    Answer(ReplyTo<u32>),
    /// Answers what it holds, counting each answer handed back.
    Release,
    Panic,
    Fail,
}

struct Holds {
    held: Vec<ReplyTo<u32>>,
    late: Arc<AtomicUsize>,
}

impl Actor for Holds {
    type Message = Asked;
    type Error = &'static str;

    fn on_message(&mut self, msg: Asked, _: &mut Context<'_, Self>) -> Result<(), &'static str> {
        match msg {
            Asked::Hold(reply) => self.held.push(reply),
            Asked::Answer(reply) => drop(reply.reply(7)),
            Asked::Release => {
                let late = self.held.drain(..).map(|reply| reply.reply(1)).filter(Result::is_err);
                self.late.fetch_add(late.count(), Ordering::SeqCst);
            }
            Asked::Panic => panic!("asked to"),
            Asked::Fail => return Err("asked to"),
        }
        Ok(())
    }
}

/// A lone ask from a thread outside the pool, on the system clock, so it waits on its thread before it sleeps. With
/// `give_up`, it is dropped after its first poll, which found no answer.
#[expect(clippy::disallowed_methods, reason = "an asker off the workers is a thread of its own")]
fn ask_from_a_thread(
    address: ActorRef<Asked>,
    deadline: Duration,
    hold: bool,
    give_up: bool,
) -> std::thread::JoinHandle<Option<Result<u32, RequestError>>> {
    std::thread::spawn(move || {
        let build = if hold { Asked::Hold } else { Asked::Answer };
        let asked = address.request(Some(address.clock().now() + deadline), build);
        if !give_up {
            return Some(park_on(asked));
        }
        match std::future::Future::poll(
            std::pin::pin!(asked),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        ) {
            std::task::Poll::Ready(answer) => Some(answer),
            std::task::Poll::Pending => None,
        }
    })
}

/// Two asks of one actor, one from a thread of its own that waits there first and one from a tokio task, then the
/// actor ends as `end` says. Each ask ends as that end says, and nothing is left: no wake on the clock, no request
/// counted, every late answer handed back.
fn ask_scenario(end: AskEnd) {
    let tokio = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    tokio.block_on(async {
        let system = ActorSystem::builder().build().unwrap();
        let late = Arc::new(AtomicUsize::new(0));
        let factory = {
            let late = late.clone();
            move || Holds { held: Vec::new(), late: late.clone() }
        };
        let family = system.supervise(factory).start_supervised().unwrap();
        eventually("the first generation runs", async || family.family.current().is_some()).await;
        let address = family.family.current().unwrap();
        let deadline = if end == AskEnd::Deadline { Duration::from_millis(30) } else { Duration::from_secs(30) };
        let (hold, give_up) = (end != AskEnd::Answered, end == AskEnd::Dropped);
        let thread = ask_from_a_thread(address.clone(), deadline, hold, give_up);
        let task = {
            let build = if hold { Asked::Hold } else { Asked::Answer };
            let asked = address.request(Some(address.clock().now() + deadline), build);
            tokio::spawn(async move {
                match give_up {
                    true => tokio::time::timeout(Duration::from_millis(1), asked).await.ok(),
                    false => Some(asked.await),
                }
            })
        };
        eventually("both asks are handled", async || address.diagnostics().processed == 2).await;
        match end {
            AskEnd::Answered | AskEnd::Deadline | AskEnd::Dropped => {}
            AskEnd::Stop => address.lifecycle().stop(),
            AskEnd::Drain => address.lifecycle().drain(None),
            AskEnd::Panic => _ = address.try_tell(Asked::Panic),
            AskEnd::Restart => _ = address.try_tell(Asked::Fail),
            AskEnd::Shutdown => _ = system.shutdown().await.unwrap(),
        }
        eventually("the thread's ask ends", async || thread.is_finished()).await;
        let task = tokio::time::timeout(Duration::from_secs(5), task).await.expect("the task's ask ends");
        let answers = [thread.join().unwrap(), task.unwrap()];
        let expected = match end {
            AskEnd::Answered => Some(Ok(7)),
            AskEnd::Deadline => Some(Err(RequestError::Deadline { admitted: true })),
            AskEnd::Dropped => None,
            _ => Some(Err(RequestError::ReplyDropped { admitted: true })),
        };
        assert_eq!(answers, [expected.clone(), expected], "{end:?}");
        if matches!(end, AskEnd::Deadline | AskEnd::Dropped) {
            address.try_tell(Asked::Release).unwrap();
            eventually("both late answers come back", async || late.load(Ordering::SeqCst) == 2).await;
        }
        if end == AskEnd::Restart {
            eventually("the next generation runs", async || family.family.restarts() == 1).await;
        }
        let d = address.diagnostics();
        assert_eq!((d.requests, d.asks, d.ask_timeouts), (0, 2, 2 * u64::from(end == AskEnd::Deadline)), "{end:?}");
        eventually("no wake is left", async || system.clock().pending_wakes() == 0).await;
        if end != AskEnd::Shutdown {
            system.shutdown().await.unwrap();
        }
    });
}

#[test]
fn an_ask_that_waits_on_its_thread_ends_with_its_actor_on_every_exit_and_leaves_nothing() {
    for end in [
        AskEnd::Answered,
        AskEnd::Stop,
        AskEnd::Drain,
        AskEnd::Panic,
        AskEnd::Restart,
        AskEnd::Shutdown,
        AskEnd::Deadline,
        AskEnd::Dropped,
    ] {
        ask_scenario(end);
    }
}
