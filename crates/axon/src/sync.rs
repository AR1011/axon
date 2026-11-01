//! The mailbox's and the scheduler's locks, atomics and thread-locals: the real ones, or loom's under
//! `--cfg axon_loom`. Not `--cfg loom`, which takes `tokio::net` away from the crates above this one that the models
//! build with.

#[cfg(not(axon_loom))]
pub(crate) use parking_lot::{Condvar, Mutex, MutexGuard};
#[cfg(not(axon_loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, fence};
#[cfg(not(axon_loom))]
pub(crate) use std::thread_local;

#[cfg(axon_loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, fence};

#[cfg(axon_loom)]
#[derive(Default)]
pub(crate) struct Mutex<T>(loom::sync::Mutex<T>);

#[cfg(axon_loom)]
impl<T> Mutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(loom::sync::Mutex::new(value))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard { mutex: &self.0, guard: Some(self.0.lock().expect("loom does not poison")) }
    }

    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        Some(MutexGuard { mutex: &self.0, guard: Some(self.0.try_lock().ok()?) })
    }
}

/// parking_lot's shape: a condvar waits on `&mut` guard, so the guard is taken out for the wait and put back.
#[cfg(axon_loom)]
pub(crate) struct MutexGuard<'a, T> {
    mutex: &'a loom::sync::Mutex<T>,
    guard: Option<loom::sync::MutexGuard<'a, T>>,
}

#[cfg(axon_loom)]
impl<T> std::ops::Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref().expect("held outside a wait")
    }
}

#[cfg(axon_loom)]
impl<T> std::ops::DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.as_mut().expect("held outside a wait")
    }
}

#[cfg(axon_loom)]
#[derive(Default)]
pub(crate) struct Condvar(loom::sync::Condvar);

#[cfg(axon_loom)]
impl Condvar {
    pub(crate) fn new() -> Self {
        Self(loom::sync::Condvar::new())
    }

    pub(crate) fn notify_one(&self) {
        self.0.notify_one();
    }

    pub(crate) fn notify_all(&self) {
        self.0.notify_all();
    }

    pub(crate) fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
        let held = guard.guard.take().expect("held before a wait");
        guard.guard = Some(self.0.wait(held).expect("loom does not poison"));
    }

    /// Loom has no time, and its own timed wait never times out. So one that will: it gives the lock up, yields and
    /// takes it again. It times out once another thread has moved, at any later point the preemption bound allows.
    pub(crate) fn wait_for<T>(&self, guard: &mut MutexGuard<'_, T>, _: std::time::Duration) {
        drop(guard.guard.take());
        loom::thread::yield_now();
        guard.guard = Some(guard.mutex.lock().expect("loom does not poison"));
    }
}

/// std's `const` form, which loom's macro does not take. Loom runs every model thread on one OS thread, so std's
/// thread-locals would be shared by them.
#[cfg(axon_loom)]
macro_rules! loom_thread_local {
    ($($(#[$attr:meta])* static $name:ident: $t:ty = const { $init:expr };)*) => {
        loom::thread_local! { $($(#[$attr])* static $name: $t = $init;)* }
    };
}
#[cfg(axon_loom)]
pub(crate) use loom_thread_local as thread_local;

/// std's `LocalKey` conveniences, on loom's.
#[cfg(axon_loom)]
pub(crate) trait LocalCell<T> {
    fn get(&'static self) -> T;
    fn set(&'static self, value: T);
}

#[cfg(axon_loom)]
impl<T: Copy + 'static> LocalCell<T> for loom::thread::LocalKey<std::cell::Cell<T>> {
    fn get(&'static self) -> T {
        self.with(std::cell::Cell::get)
    }

    fn set(&'static self, value: T) {
        self.with(|c| c.set(value));
    }
}

#[cfg(axon_loom)]
pub(crate) trait LocalRefCell<T> {
    fn set(&'static self, value: T);
    fn take(&'static self) -> T;
    fn with_borrow<R>(&'static self, f: impl FnOnce(&T) -> R) -> R;
}

#[cfg(axon_loom)]
impl<T: Default + 'static> LocalRefCell<T> for loom::thread::LocalKey<std::cell::RefCell<T>> {
    fn set(&'static self, value: T) {
        self.with(|c| *c.borrow_mut() = value);
    }

    fn take(&'static self) -> T {
        self.with(|c| c.take())
    }

    fn with_borrow<R>(&'static self, f: impl FnOnce(&T) -> R) -> R {
        self.with(|c| f(&c.borrow()))
    }
}

/// std's name since 1.99 for loom's `fetch_update`.
#[cfg(axon_loom)]
pub(crate) trait TryUpdate {
    fn try_update(
        &self,
        set: std::sync::atomic::Ordering,
        fetch: std::sync::atomic::Ordering,
        f: impl FnMut(usize) -> Option<usize>,
    ) -> Result<usize, usize>;
}

#[cfg(axon_loom)]
impl TryUpdate for AtomicUsize {
    fn try_update(
        &self,
        set: std::sync::atomic::Ordering,
        fetch: std::sync::atomic::Ordering,
        f: impl FnMut(usize) -> Option<usize>,
    ) -> Result<usize, usize> {
        self.fetch_update(set, fetch, f)
    }
}
