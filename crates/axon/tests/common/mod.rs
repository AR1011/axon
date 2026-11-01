#![allow(dead_code, reason = "each axon test file uses part of it")]

use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use std::sync::Arc;

use axon::{ActorSystem, JobExecutor, RuntimeConfig, Stream, Threadless};

/// Every actor on one task of this tokio runtime, on its clock: what a simulator builds a node from. Actors run only
/// when the test awaits, so what a burst of tells queued can be read before any is handled.
pub fn threadless() -> (ActorSystem, Threadless) {
    let threadless = Threadless::default();
    let runtime = RuntimeConfig { threadless: Some(threadless.clone()), ..Default::default() };
    (ActorSystem::builder().runtime(runtime).build().unwrap(), threadless)
}

/// Sends and tells return on admission, before the actor handles what they carried: wait for what it did.
pub async fn eventually(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !done().await {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Polls `f` on this thread, parking it between polls: an asker off the workers and outside tokio.
pub fn park_on<F: Future>(f: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut f = pin!(f);
    loop {
        if let Poll::Ready(out) = f.as_mut().poll(&mut Context::from_waker(&waker)) {
            return out;
        }
        std::thread::park();
    }
}

/// An executor that keeps what it is given until `lose` drops it, unrun.
pub fn losing() -> (JobExecutor, impl Fn()) {
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let keep = held.clone();
    let executor = JobExecutor::custom(move |job| {
        keep.lock().unwrap().push(job);
        Ok(())
    });
    (executor, move || held.lock().unwrap().clear())
}

/// A source for the tests: items arrive from a channel, `None` when the sender is gone.
pub struct Recv(pub tokio::sync::mpsc::Receiver<u32>);

impl Stream for Recv {
    type Item = u32;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<u32>> {
        self.0.poll_recv(cx)
    }
}
