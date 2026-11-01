use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::clock::Clock;
use crate::types::{ActorIncarnation, Exit, Phase, StopReason};

pub(crate) trait Lifecycle: Send + Sync {
    fn incarnation(&self) -> ActorIncarnation;
    fn phase(&self) -> Phase;
    fn request_stop(&self, reason: StopReason);
    /// Stopped by a link: a family fails its current generation instead, for its policy to decide.
    fn fail(&self, reason: StopReason) {
        self.request_stop(reason);
    }
    fn request_drain(&self, deadline: Option<Duration>);
    fn signal(&self) -> &ExitSignal;
    fn clock(&self) -> &Clock;
}

/// Exit publication shared by blocking and async joiners.
#[derive(Default)]
pub(crate) struct ExitSignal {
    state: Mutex<SignalState>,
    changed: Condvar,
}

#[derive(Default)]
struct SignalState {
    // Boxed: written once, at the end, and every actor holds one.
    exit: Option<Box<Exit>>,
    publishing: bool,
    wakers: Vec<Waker>,
    watchers: Watchers,
}

// One watcher, the system's, which every actor has, inline and in the space an `Option` of it takes; more boxed.
enum Watchers {
    // Empty when it holds `Weak::new()`.
    One(Weak<dyn ExitWatcher>),
    #[expect(clippy::box_collection, reason = "a thin pointer, so the enum is no wider than `One`")]
    Many(Box<Vec<Weak<dyn ExitWatcher>>>),
}

impl Default for Watchers {
    fn default() -> Self {
        Watchers::One(Weak::<Nobody>::new())
    }
}

struct Nobody;

impl ExitWatcher for Nobody {
    fn exited(&self, _: &Exit) {}
}

/// Told of each exit it watches: the system of every actor's, a `cx.watch` of its one, a link of both its ends'.
pub(crate) trait ExitWatcher: Send + Sync {
    fn exited(&self, exit: &Exit);

    fn as_link(self: Arc<Self>) -> Option<Arc<Link>> {
        None
    }
}

impl ExitSignal {
    pub fn exit(&self) -> Option<Exit> {
        self.state.lock().exit.as_deref().cloned()
    }

    /// True from the start of publishing, while the watcher is told and `exit` is not yet visible.
    pub fn ended(&self) -> bool {
        let s = self.state.lock();
        s.publishing || s.exit.is_some()
    }

    pub fn publish(&self, exit: Exit) {
        {
            let mut s = self.state.lock();
            if s.publishing || s.exit.is_some() {
                return;
            }
            s.publishing = true;
        }
        let exit = Box::new(exit);
        // The watchers first, and the exit visible only after them, so whoever sees it also sees their effects.
        let wakers = loop {
            let watcher = {
                let mut s = self.state.lock();
                let next = match &mut s.watchers {
                    Watchers::One(w) => {
                        Some(std::mem::replace(w, Weak::<Nobody>::new())).filter(|w| w.strong_count() > 0)
                    }
                    // The first to come, the system's, first: it frees the name and records the exit before a
                    // `cx.watch` watcher can handle its message.
                    Watchers::Many(more) => (!more.is_empty()).then(|| more.swap_remove(0)),
                };
                match next {
                    Some(watcher) => watcher,
                    None => {
                        s.watchers = Watchers::default();
                        s.exit = Some(exit);
                        break std::mem::take(&mut s.wakers);
                    }
                }
            };
            tell(&watcher, &exit);
        };
        self.changed.notify_all();
        wake_all(wakers);
    }

    /// Tells `watcher` of the exit once, on the thread that publishes it, or here and now if it is already published.
    /// It is told before the exit is visible, so it must not wait for it.
    pub fn watch(&self, watcher: Weak<dyn ExitWatcher>) {
        let exit = {
            let mut s = self.state.lock();
            if s.exit.is_none() {
                match &mut s.watchers {
                    Watchers::One(w) if w.strong_count() == 0 => *w = watcher,
                    Watchers::One(w) => {
                        let first = std::mem::replace(w, Weak::<Nobody>::new());
                        s.watchers = Watchers::Many(Box::new(vec![first, watcher]));
                    }
                    Watchers::Many(more) => more.push(watcher),
                }
                return;
            }
            s.exit.as_deref().cloned()
        };
        tell(&watcher, &exit.expect("checked"));
    }

    /// Takes back a watcher that has not been told.
    pub fn unwatch(&self, watcher: &Weak<dyn ExitWatcher>) {
        match &mut self.state.lock().watchers {
            Watchers::One(w) if w.ptr_eq(watcher) => *w = Weak::<Nobody>::new(),
            Watchers::One(_) => {}
            Watchers::Many(more) => more.retain(|w| !w.ptr_eq(watcher)),
        }
    }

    /// The link between the actor whose signal this is and `other`, if there is one.
    fn find_link(&self, other: &Arc<dyn Lifecycle>) -> Option<Arc<Link>> {
        // Upgraded and dropped outside the lock.
        let watchers = match &self.state.lock().watchers {
            Watchers::One(w) => vec![w.clone()],
            Watchers::Many(more) => more.to_vec(),
        };
        let other = Arc::as_ptr(other);
        watchers
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|w| w.as_link())
            .find(|l| l.ends.iter().any(|e| std::ptr::addr_eq(e.as_ptr(), other)))
    }

    pub fn watchers(&self) -> usize {
        match &self.state.lock().watchers {
            Watchers::One(w) => usize::from(w.strong_count() > 0),
            Watchers::Many(more) => more.len(),
        }
    }

    /// Wakes joiners and generation waiters on a change that is not an exit: readiness, a new generation, a stop asked.
    pub fn touch(&self) {
        let wakers = std::mem::take(&mut self.state.lock().wakers);
        self.changed.notify_all();
        wake_all(wakers);
    }

    pub fn poll(&self, waker: &Waker) -> Option<Exit> {
        let mut s = self.state.lock();
        if s.exit.is_none() && !s.wakers.iter().any(|w| w.will_wake(waker)) {
            s.wakers.push(waker.clone());
        }
        s.exit.as_deref().cloned()
    }

    pub fn wait_until<T>(
        &self,
        clock: &Clock,
        timeout: Duration,
        mut check: impl FnMut(Option<&Exit>) -> Option<T>,
    ) -> Option<T> {
        debug_assert!(
            timeout.is_zero() || !clock.is_driven(),
            "a blocking wait on a threadless runtime, whose actors run only when this thread is free: await instead"
        );
        #[expect(clippy::disallowed_methods, reason = "a blocking wait on a thread, zero on a threadless runtime")]
        let deadline = Instant::now() + timeout;
        let mut s = self.state.lock();
        loop {
            if let Some(v) = check(s.exit.as_deref()) {
                return Some(v);
            }
            if self.changed.wait_until(&mut s, deadline).timed_out() {
                return check(s.exit.as_deref());
            }
        }
    }
}

/// A watcher's panic ends here, not in the actor that published or the one that watched.
fn tell(watcher: &Weak<dyn ExitWatcher>, exit: &Exit) {
    if let Some(watcher) = watcher.upgrade() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| watcher.exited(exit)));
    }
}

pub(crate) fn wake_all(wakers: impl IntoIterator<Item = Waker>) {
    for w in wakers {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.wake()));
    }
}

/// Controls and observes one actor (or one supervised family). Dropping it does nothing.
#[derive(Clone)]
pub struct LifecycleHandle(pub(crate) Arc<dyn Lifecycle>);

impl LifecycleHandle {
    pub fn incarnation(&self) -> ActorIncarnation {
        self.0.incarnation()
    }

    pub fn phase(&self) -> Phase {
        self.0.phase()
    }

    /// Closes admission now; the actor terminates at its next handler boundary.
    pub fn stop(&self) {
        self.0.request_stop(StopReason::Stopped);
    }

    /// Closes admission, processes the accepted backlog, then stops. Repeated calls can only
    /// tighten the deadline (absolute, on the actor's clock).
    pub fn drain(&self, deadline: Option<Duration>) {
        self.0.request_drain(deadline);
    }

    pub fn exit(&self) -> Option<Exit> {
        self.0.signal().exit()
    }

    /// Blocks up to `timeout` for the exit; `None` if it is still running.
    pub fn wait(&self, timeout: Duration) -> Option<Exit> {
        self.0.signal().wait_until(self.0.clock(), timeout, |e| e.cloned())
    }

    pub fn join(&self) -> Join {
        Join(self.clone())
    }

    pub(crate) fn watch(&self, watcher: Weak<dyn ExitWatcher>) {
        self.0.signal().watch(watcher);
    }

    pub(crate) fn unwatch<W: ExitWatcher + 'static>(&self, watcher: &Arc<W>) {
        self.0.signal().unwatch(&(Arc::downgrade(watcher) as Weak<dyn ExitWatcher>));
    }

    pub(crate) fn stop_with(&self, reason: StopReason) {
        self.0.request_stop(reason);
    }

    /// Links this and `other`, or finds them linked already. Linked to itself, nothing.
    pub(crate) fn link(&self, other: &LifecycleHandle) -> LinkHandle {
        if self.is(other) {
            return LinkHandle(Weak::new());
        }
        // ponytail: one lock for every find-or-insert in the process, so two actors linking each other at once make one
        // link; links are made in handlers, seldom. A lock per pair, taken in address order, if it ever shows.
        static LINKING: Mutex<()> = Mutex::new(());
        let _one = LINKING.lock();
        let link = self.find(other).unwrap_or_else(|| {
            let ends = [Arc::downgrade(&self.0), Arc::downgrade(&other.0)];
            let link = Arc::new(Link { ends, held: Mutex::default() });
            *link.held.lock() = Some(link.clone());
            for end in [self, other] {
                end.watch(Arc::downgrade(&link) as Weak<dyn ExitWatcher>);
            }
            link
        });
        LinkHandle(Arc::downgrade(&link))
    }

    pub(crate) fn unlink(&self, other: &LifecycleHandle) {
        if let Some(link) = self.find(other) {
            link.take();
        }
    }

    /// Searched from the end with fewer watchers, so a hub linking many fresh actors does not scan its own each time.
    fn find(&self, other: &LifecycleHandle) -> Option<Arc<Link>> {
        // Every link on a signal has its own actor as one end, so only the other end tells them apart.
        if self.is(other) {
            return None;
        }
        let (near, far) =
            if self.0.signal().watchers() <= other.0.signal().watchers() { (self, other) } else { (other, self) };
        near.0.signal().find_link(&far.0)
    }

    fn is(&self, other: &LifecycleHandle) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.0), Arc::as_ptr(&other.0))
    }
}

/// A `cx.link`, on both its actors' signals until either ends or it is unlinked.
pub(crate) struct Link {
    ends: [Weak<dyn Lifecycle>; 2],
    /// Itself, so it lives exactly as long as it is on the signals, which hold it weakly, as they hold every watcher.
    held: Mutex<Option<Arc<Link>>>,
}

impl Link {
    /// Takes the link off both signals, once; false if it was already. One ending lets its own publish drop it, so a
    /// hub failing does not scan its watchers once for each link.
    fn take(&self) -> bool {
        let Some(me) = self.held.lock().take() else { return false };
        let me = Arc::downgrade(&me) as Weak<dyn ExitWatcher>;
        for end in self.ends.iter().filter_map(Weak::upgrade).filter(|end| !end.signal().ended()) {
            end.signal().unwatch(&me);
        }
        true
    }
}

impl ExitWatcher for Link {
    fn exited(&self, exit: &Exit) {
        if !self.take() || !exit.reason.is_failure() {
            return;
        }
        for end in self.ends.iter().filter_map(Weak::upgrade).filter(|end| !end.signal().ended()) {
            end.fail(StopReason::Linked(exit.incarnation));
        }
    }

    fn as_link(self: Arc<Self>) -> Option<Arc<Link>> {
        Some(self)
    }
}

/// A link `cx.link` made. Dropping it does nothing.
#[derive(Clone, Debug)]
pub struct LinkHandle(Weak<Link>);

impl LinkHandle {
    /// Takes the link off both actors; nothing if either has ended or it was unlinked already.
    pub fn cancel(&self) {
        if let Some(link) = self.0.upgrade() {
            link.take();
        }
    }
}

impl std::fmt::Debug for LifecycleHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LifecycleHandle({}, {:?})", self.incarnation(), self.phase())
    }
}

pub struct Join(LifecycleHandle);

impl Future for Join {
    type Output = Exit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Exit> {
        match self.0.0.signal().poll(cx.waker()) {
            Some(exit) => Poll::Ready(exit),
            None => Poll::Pending,
        }
    }
}

/// An actor a [`Watched`] list holds.
#[cfg(feature = "system")]
pub(crate) trait Member {
    fn lifecycle(&self) -> &LifecycleHandle;
    /// What it is listed under, and what an exit names it by.
    fn id(&self) -> ActorIncarnation;
}

/// A router's members or a topic's subscribers, each watched while it is listed. The owner's `ExitWatcher`, `me`, takes
/// out one that ends through `exited`, before its exit is visible; what is still listed when the list drops is
/// unwatched, so no member is left with a watcher that is gone.
#[cfg(feature = "system")]
pub(crate) struct Watched<T: Member> {
    list: parking_lot::RwLock<Vec<T>>,
    me: Weak<dyn ExitWatcher>,
    max: usize,
}

#[cfg(feature = "system")]
impl<T: Member> Watched<T> {
    pub(crate) fn new(me: Weak<dyn ExitWatcher>, max: usize) -> Self {
        Self { list: Default::default(), me, max }
    }

    pub(crate) fn read(&self) -> parking_lot::RwLockReadGuard<'_, Vec<T>> {
        self.list.read()
    }

    /// Lists and watches `member`, unless it is listed already; false, and nothing listed, when `max` are. One that has
    /// ended already is taken out at once.
    pub(crate) fn add(&self, member: T) -> bool {
        let id = member.id();
        let lifecycle = {
            let mut list = self.list.write();
            if list.iter().any(|m| m.id() == id) {
                return true;
            }
            if list.len() >= self.max {
                return false;
            }
            let lifecycle = member.lifecycle().clone();
            list.push(member);
            lifecycle
        };
        // Outside the lock: an ended member's watcher is told here and now, and takes it.
        lifecycle.watch(self.me.clone());
        // A `remove` between the push and the watch found no watch to take back, so it is taken here; under the lock, as
        // `unwatch` takes every watch of `me`, so it cannot take the watch of an `add` that comes after.
        let list = self.list.write();
        if !list.iter().any(|m| m.id() == id) {
            lifecycle.0.signal().unwatch(&self.me);
        }
        true
    }

    /// Takes out the member listed under `id`, and its watch, for the caller to drop: its handle may hold the last of
    /// a mailbox and the messages in it.
    pub(crate) fn remove(&self, id: ActorIncarnation) -> Option<T> {
        let mut list = self.list.write();
        let gone = take(&mut list, id)?;
        // A member's own watch, which for a family is not on a generation's lifecycle; under the lock, so it cannot
        // take the watch of an `add` that comes after.
        gone.lifecycle().0.signal().unwatch(&self.me);
        Some(gone)
    }

    /// For the owner's `ExitWatcher`: takes out the member listed under `id`, its watch spent, and says whether the
    /// list is empty now.
    pub(crate) fn exited(&self, id: ActorIncarnation) -> bool {
        let mut list = self.list.write();
        let gone = take(&mut list, id);
        let empty = list.is_empty();
        drop(list);
        drop(gone);
        empty
    }
}

#[cfg(feature = "system")]
fn take<T: Member>(list: &mut Vec<T>, id: ActorIncarnation) -> Option<T> {
    let at = list.iter().position(|m| m.id() == id)?;
    // In order: a router's `members` keep theirs.
    Some(list.remove(at))
}

#[cfg(feature = "system")]
impl<T: Member> Drop for Watched<T> {
    fn drop(&mut self) {
        for member in self.list.get_mut().iter() {
            member.lifecycle().0.signal().unwatch(&self.me);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Watcher(Arc<ExitSignal>, AtomicUsize);

    impl ExitWatcher for Watcher {
        fn exited(&self, exit: &Exit) {
            // Whoever sees the exit must also see what its watcher did: a joiner polling now finds nothing yet.
            assert!(self.0.exit().is_none(), "the exit was visible before its watcher was told");
            self.1.fetch_add((exit.reason == StopReason::Stopped) as usize, Ordering::Relaxed);
        }
    }

    #[test]
    fn an_exit_is_told_once_whether_it_was_watched_before_or_after() {
        let exit = Exit::new(ActorIncarnation { runtime: 1, slot: 1, generation: 1 }, StopReason::Stopped);
        let signal = Arc::new(ExitSignal::default());
        let watcher = Arc::new(Watcher(signal.clone(), AtomicUsize::new(0)));
        let weak: Weak<dyn ExitWatcher> = Arc::downgrade(&watcher) as _;
        signal.watch(weak);
        assert_eq!(watcher.1.load(Ordering::Relaxed), 0);
        signal.publish(exit.clone());
        signal.publish(exit.clone());
        assert_eq!(watcher.1.load(Ordering::Relaxed), 1);

        // Watched after the exit is published: told at once.
        let late = Arc::new(Watcher(Arc::new(ExitSignal::default()), AtomicUsize::new(0)));
        let weak: Weak<dyn ExitWatcher> = Arc::downgrade(&late) as _;
        signal.watch(weak);
        assert_eq!(late.1.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn every_watcher_of_an_exit_is_told_once_before_it_is_visible() {
        let signal = Arc::new(ExitSignal::default());
        let watchers: Vec<_> = (0..3).map(|_| Arc::new(Watcher(signal.clone(), AtomicUsize::new(0)))).collect();
        for watcher in &watchers {
            signal.watch(Arc::downgrade(watcher) as Weak<dyn ExitWatcher>);
        }
        signal.unwatch(&(Arc::downgrade(&watchers[1]) as Weak<dyn ExitWatcher>));
        assert_eq!(signal.watchers(), 2);
        let exit = Exit::new(ActorIncarnation { runtime: 1, slot: 1, generation: 1 }, StopReason::Stopped);
        signal.publish(exit.clone());
        signal.publish(exit);
        assert_eq!(watchers.iter().map(|w| w.1.load(Ordering::Relaxed)).collect::<Vec<_>>(), [1, 0, 1]);
        assert_eq!(signal.watchers(), 0);
    }
}
