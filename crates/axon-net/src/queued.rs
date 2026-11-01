//! Keeps a sender's frames in order when a submit may write the socket itself, past the writer task.

use std::sync::atomic::Ordering::Relaxed;

#[cfg(axon_loom)]
use loom::sync::atomic::AtomicUsize;
#[cfg(not(axon_loom))]
use std::sync::atomic::AtomicUsize;

/// Frames in the outbox or in the writer task's hands, not yet written. A submit may write the socket itself only at
/// zero, so it cannot pass a frame of its own caller that went through the outbox. Relaxed is enough: a caller's load
/// comes after its own increment, and the count falls back only once the frame is written or while the writer lock it
/// is written under is still held. Either one alone would do; the writer task keeps both.
pub(crate) struct Queued {
    count: AtomicUsize,
    /// A capped session: every frame goes through the writer task's bucket, none past it.
    paced: bool,
}

impl Queued {
    pub(crate) fn new(paced: bool) -> Self {
        Self { count: AtomicUsize::new(0), paced }
    }

    /// The writer lock, when the caller may write its frame itself: not paced, nothing queued, the lock free and the
    /// writer `ready`.
    pub(crate) fn write_now<G>(
        &self,
        try_lock: impl FnOnce() -> Option<G>,
        ready: impl FnOnce(&G) -> bool,
    ) -> Option<G> {
        if self.paced || self.count.load(Relaxed) != 0 {
            return None;
        }
        let w = try_lock()?;
        (self.count.load(Relaxed) == 0 && ready(&w)).then_some(w)
    }

    /// Counted before `send` puts the frame in the outbox, so the writer task's decrement cannot come first.
    pub(crate) fn queue(&self, send: impl FnOnce()) {
        self.count.fetch_add(1, Relaxed);
        send();
    }

    /// By the writer task, under the writer lock, once the `n` frames it took from the outbox are written.
    pub(crate) fn written(&self, n: usize) {
        self.count.fetch_sub(n, Relaxed);
    }
}

/// `RUSTFLAGS="--cfg axon_loom" cargo test -p axon-net --lib --release --target-dir target/loom queued::model`
#[cfg(test)] // apart from `axon_loom`, so clippy's allow-unwrap-in-tests sees a test module
#[cfg(axon_loom)]
mod model;
