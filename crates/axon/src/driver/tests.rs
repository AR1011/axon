use super::*;
use crate::context::WatchHandle;
use crate::lifecycle::LinkHandle;
use crate::test_support::Idle;

/// 0 watches the target twice, 1 cancels its watches, 2 is a down.
struct Watcher(LifecycleHandle, Vec<WatchHandle>);

impl Actor for Watcher {
    type Message = u8;
    type Error = ();

    fn on_message(&mut self, msg: u8, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            0 => {
                for _ in 0..2 {
                    self.1.push(cx.watch(&self.0, |_| 2).expect("admitted"));
                }
            }
            1 => self.1.drain(..).for_each(|w| w.cancel()),
            _ => {}
        }
        Ok(())
    }
}

#[test]
fn a_watch_cancelled_or_ended_with_its_watcher_leaves_nothing_on_the_watched_actor() {
    let clock = Clock::manual();
    let (mut target, watched) = EmbeddedDriver::new(Idle, MailboxLimits::default(), clock.clock()).unwrap();
    target.run_turn(TurnBudget::default());
    let actor = Watcher(watched.lifecycle.clone(), Vec::new());
    let (mut watcher, spawn) = EmbeddedDriver::new(actor, MailboxLimits::default(), clock.clock()).unwrap();
    let registered = || watched.lifecycle.0.signal().watchers();
    let step = |watcher: &mut EmbeddedDriver<Watcher>, msg: u8| {
        spawn.address.try_tell(msg).unwrap();
        watcher.run_until_idle(4).unwrap();
    };

    step(&mut watcher, 0);
    assert_eq!(registered(), 2);
    step(&mut watcher, 1);
    assert_eq!(registered(), 0, "a cancel takes its watch back from the watched actor");
    step(&mut watcher, 0);
    spawn.lifecycle.stop();
    watcher.run_turn(TurnBudget::default());
    assert!(spawn.lifecycle.exit().is_some());
    assert_eq!(registered(), 0, "a watcher's end takes its watches back from the watched actor");
}

enum Op {
    Peer(LifecycleHandle),
    Link,
    Unlink,
    /// Unlinks by the handles its links returned.
    Release,
    Fail,
}

struct Linker(Vec<LifecycleHandle>, Vec<LinkHandle>);

impl Actor for Linker {
    type Message = Op;
    type Error = ();

    fn on_message(&mut self, op: Op, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match op {
            Op::Peer(peer) => self.0.push(peer),
            Op::Link => self.0.iter().for_each(|p| self.1.push(cx.link(p))),
            Op::Unlink => self.0.iter().for_each(|p| cx.unlink(p)),
            Op::Release => self.1.drain(..).for_each(|l| l.cancel()),
            Op::Fail => return Err(()),
        }
        Ok(())
    }
}

#[test]
fn a_link_is_registered_once_per_pair_and_leaves_nothing_once_unlinked_or_ended() {
    let clock = Clock::manual();
    let (mut drivers, spawns): (Vec<_>, Vec<_>) = (0..3)
        .map(|_| EmbeddedDriver::new(Linker(Vec::new(), Vec::new()), MailboxLimits::default(), clock.clock()))
        .map(Result::unwrap)
        .unzip();
    let life: Vec<_> = spawns.iter().map(|s| s.lifecycle.clone()).collect();
    let registered = || life.iter().map(|l| l.0.signal().watchers()).collect::<Vec<_>>();
    let mut step = |at: usize, op: Op| {
        spawns[at].address.try_tell(op).unwrap();
        for _ in 0..2 {
            drivers.iter_mut().for_each(|d| {
                d.run_turn(TurnBudget::default());
            });
        }
        registered()
    };
    // The middle one links to both ends, the first back to it.
    step(1, Op::Peer(life[0].clone()));
    step(1, Op::Peer(life[2].clone()));
    step(0, Op::Peer(life[1].clone()));

    assert_eq!(step(1, Op::Link), [1, 2, 1]);
    assert_eq!(step(1, Op::Link), [1, 2, 1], "linked again: still one link a pair");
    assert_eq!(step(0, Op::Link), [1, 2, 1], "linked from the other end: still one");
    assert_eq!(step(1, Op::Unlink), [0, 0, 0]);
    assert_eq!(step(1, Op::Link), [1, 2, 1]);
    assert_eq!(step(1, Op::Release), [0, 0, 0]);
    assert_eq!(step(1, Op::Link), [1, 2, 1]);
    assert_eq!(step(0, Op::Fail), [0, 0, 0], "an end takes its links off both actors");
    let exits: Vec<_> = life.iter().map(|l| l.exit().map(|e| e.reason)).collect();
    assert_eq!(exits[0], Some(StopReason::Failed("()".into())));
    assert_eq!(exits[1], Some(StopReason::Linked(life[0].incarnation())));
    assert_eq!(exits[2], Some(StopReason::Linked(life[1].incarnation())));
}

#[test]
#[expect(clippy::disallowed_methods, reason = "the race needs real threads")]
fn two_actors_linking_each_other_at_once_make_one_link_and_one_unlink_takes_it() {
    let clock = Clock::manual();
    for _ in 0..2000 {
        // The drivers kept, since dropping one ends its actor.
        let [(_da, a), (_db, b)] = [(); 2].map(|_| {
            EmbeddedDriver::new(Linker(Vec::new(), Vec::new()), MailboxLimits::default(), clock.clock()).unwrap()
        });
        let (a, b) = (a.lifecycle, b.lifecycle);
        let go = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            s.spawn(|| {
                go.wait();
                a.link(&b);
            });
            go.wait();
            b.link(&a);
        });
        assert_eq!([a.0.signal().watchers(), b.0.signal().watchers()], [1, 1]);
        a.unlink(&b);
        assert_eq!([a.0.signal().watchers(), b.0.signal().watchers()], [0, 0]);
        // Linked and unlinked with itself: nothing, and no other link found in its place.
        b.link(&a);
        a.link(&a);
        a.unlink(&a);
        assert_eq!([a.0.signal().watchers(), b.0.signal().watchers()], [1, 1]);
    }
}
