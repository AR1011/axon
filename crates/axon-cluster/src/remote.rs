use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use axon_net::{Backoff, SubmitReason};
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::engine::{Engine, LONGEST, Parts, Target};
use crate::wire::{REQUEST, SEND, TELL, WireProtocol};
use crate::{CallError, ClusterError, Placement, Retry, WeakCluster};

/// A typed reference to one endpoint on one boot of one node. It never retargets: after the
/// actor or the node is gone, calls fail and the holder resolves again.
pub struct RemoteRef<P> {
    engine: Arc<Engine>,
    target: Target,
    epoch: Option<u64>,
    protocol: PhantomData<fn() -> P>,
}

impl<P> Clone for RemoteRef<P> {
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone(), target: self.target.clone(), epoch: self.epoch, protocol: PhantomData }
    }
}

impl<P> std::fmt::Debug for RemoteRef<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteRef({}#{})", self.target.node, self.target.endpoint)
    }
}

impl<P: WireProtocol> RemoteRef<P> {
    /// `target` takes `P`: `Route::target` checked it.
    pub(crate) fn new(engine: Arc<Engine>, target: Target, epoch: Option<u64>) -> Self {
        Self { engine, target, epoch, protocol: PhantomData }
    }

    pub(crate) fn target(&self) -> &Target {
        &self.target
    }

    pub fn node(&self) -> &str {
        &self.target.node
    }

    pub fn node_incarnation(&self) -> u128 {
        self.target.incarnation
    }

    /// The ownership epoch of a strictly-owned keyed actor: a fencing token for its effects.
    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    /// Fire and forget: returns once the local outbox has the message.
    pub async fn tell(&self, message: P::Message) -> Result<(), CallError> {
        self.engine.call(&self.target, TELL, P::encode(message)?).await.map(drop)
    }

    /// Returns once the destination mailbox admitted the message. A full one refuses it at once with
    /// `Rejected(Full)` rather than waiting, which would hold back every later call to that node.
    pub async fn send(&self, message: P::Message) -> Result<(), CallError> {
        self.engine.call(&self.target, SEND, P::encode(message)?).await.map(drop)
    }

    pub async fn request(&self, message: P::Message) -> Result<P::Reply, CallError> {
        self.request_by(message, None).await
    }

    /// A request given `within` instead of `request_timeout`, which travels with it: the far node drops it unrun if it
    /// comes in past that deadline, as that node reads it, and counts it in `inbound_expired`. After the deadline the
    /// caller gets `Timeout`, and the handler's [`Reply::is_abandoned`](crate::Reply::is_abandoned) turns true.
    pub async fn request_within(&self, message: P::Message, within: Duration) -> Result<P::Reply, CallError> {
        self.request_by(message, Some(within)).await
    }

    /// Requests again after each error whose `retry()` is `Later`, waiting 10 ms, doubling to 100 ms, between tries,
    /// until `deadline`; then returns the last error. Any other error returns at once: after `Resolve` this reference
    /// cannot work, and a [`NamedRef`] or [`KeyedRef`] looks again. Each try is a
    /// [`request_within`](Self::request_within) of what is left, up to `request_timeout`, so the far node drops one that
    /// comes in past it. A request still out at `deadline` returns `Timeout`: it may have been handled. One not yet sent
    /// returns `NotSent(Deadline)`.
    pub async fn request_until(&self, message: P::Message, deadline: Instant) -> Result<P::Reply, CallError>
    where
        P::Message: Clone,
    {
        let message = &message;
        let again = |e: &CallError| e.retry() == Retry::Later;
        retry_until(deadline, again, || async move {
            let within = deadline.saturating_duration_since(Instant::now()).min(self.engine.cfg.request_timeout);
            self.request_within(message.clone(), within).await.map_err(Some)
        })
        .await
    }

    /// A reply larger than a frame, in parts: returns once the first part, or the end, has come, with the errors
    /// [`request`](Self::request) gives. Each wait for a part is bounded by `request_timeout`, after which the reader
    /// gets `Timeout` and the far side is stopped. The handler answers with [`Reply::send_parts`](crate::Reply::send_parts).
    pub async fn request_parts(&self, message: P::Message) -> Result<Parts<P>, CallError> {
        self.engine.call_parts(&self.target, P::encode(message)?, None).await
    }

    /// [`request_parts`](Self::request_parts) with the whole reply bounded by `within`, which travels as
    /// [`request_within`](Self::request_within)'s does.
    pub async fn request_parts_within(&self, message: P::Message, within: Duration) -> Result<Parts<P>, CallError> {
        self.engine.call_parts(&self.target, P::encode(message)?, Some(within)).await
    }

    /// A request larger than a frame: `message` as for [`request`](Self::request), then its body, the `parts` stream
    /// yields, each part at most the far session's `max_frame`. Parts are pulled only while the handler grants credit,
    /// so it holds at most 4 MiB and one part unread, and they go between the other frames of the session. Returns at
    /// the reply, which may come before the body ends: what is left is not sent. Each wait for credit is bounded by
    /// `request_timeout` (`Timeout`), and so is the reply after the handler last read a part, as the body's tail may
    /// still be on the wire when the stream ends; the stream's own waits are not bounded. An `Err` from the stream cuts
    /// the body and is returned, as is `NotSent(Oversized)` for a part too large; the handler reads `NoReply` either
    /// way. The handler takes the body with [`Reply::upload`](crate::Reply::upload). Nothing is resent: after
    /// `Timeout`, `OutcomeUnknown` or `NoReply`, make the stream again.
    pub async fn upload<S>(&self, message: P::Message, parts: S) -> Result<P::Reply, CallError>
    where
        S: axon::Stream<Item = Result<Bytes, CallError>>,
    {
        let body = self.engine.call_upload(&self.target, P::encode(message)?, parts).await?;
        P::decode_reply(body.ok_or(CallError::NoReply)?)
    }

    /// [`upload`](Self::upload) answered in parts, a two-way stream: the body goes from a task of its own while the
    /// reply is read, and is cut when the reply ends or the `Parts` is dropped. Returns at the first part of the reply,
    /// as [`request_parts`](Self::request_parts) does, within `request_timeout` of the call or of the handler's last
    /// read of the body; an `Err` from the stream ends the reply with it.
    pub async fn upload_parts<S>(&self, message: P::Message, parts: S) -> Result<Parts<P>, CallError>
    where
        S: axon::Stream<Item = Result<Bytes, CallError>> + Send + 'static,
    {
        self.engine.call_upload_parts(&self.target, P::encode(message)?, parts).await
    }

    async fn request_by(&self, message: P::Message, within: Option<Duration>) -> Result<P::Reply, CallError> {
        let body = self.engine.call_with(&self.target, REQUEST, P::encode(message)?, false, within).await?;
        P::decode_reply(body.ok_or(CallError::NoReply)?)
    }
}

/// Tries `attempt` until it answers, fails with an error `again` does not take, or the next try would start past
/// `deadline`, pausing `REFUSED` between tries; then returns the last error. `Err(None)` is an attempt that reached the
/// deadline before it sent anything: it returns the error before it, or `NotSent(Deadline)`.
async fn retry_until<T, E: From<CallError>, F: Future<Output = Result<T, Option<E>>>>(
    deadline: Instant,
    again: impl Fn(&E) -> bool,
    mut attempt: impl FnMut() -> F,
) -> Result<T, E> {
    let (mut failures, mut last) = (0, None);
    loop {
        let error = match attempt().await {
            Ok(answer) => return Ok(answer),
            Err(None) => return Err(last.unwrap_or_else(|| CallError::NotSent(SubmitReason::Deadline).into())),
            Err(Some(e)) if again(&e) => e,
            Err(Some(e)) => return Err(e),
        };
        failures += 1;
        let next = Instant::now() + REFUSED.delay(failures);
        if next >= deadline {
            return Err(error);
        }
        last = Some(error);
        sleep_until(next).await;
    }
}

/// A reference that follows a key: the one actor of a kind for that key, wherever it runs, from
/// [`Cluster::keyed`](crate::Cluster::keyed). The first call asks the owner with `get_or_spawn` and
/// keeps the [`RemoteRef`]; a call that fails with [`Retry::Resolve`] drops it, and the next call asks
/// again. Nothing is resent but by [`request_until`](Self::request_until). Clones share the kept reference.
pub struct KeyedRef<P>(Arc<Follow<P>>);

/// The [`KeyedRef`]s of one kind, kept by key so each key's owner is asked once, from
/// [`Cluster::keyed_refs`](crate::Cluster::keyed_refs). At most its bound are kept, in two halves: the keys got since
/// the young half last filled, and those got before that. When the young half fills it becomes the old one and the old
/// one goes, so the keys got last are always kept and nothing goes all at once. Clones share the table.
pub struct KeyedRefs<P>(Arc<Kept<P>>);

struct Kept<P> {
    cluster: WeakCluster,
    kind: String,
    placement: Placement,
    half: usize,
    /// The young half, then the old one.
    halves: Mutex<[BTreeMap<String, KeyedRef<P>>; 2]>,
}

impl<P: WireProtocol> KeyedRefs<P> {
    pub(crate) fn new(cluster: WeakCluster, kind: &str, placement: &Placement, max: usize) -> Self {
        let (kind, placement, half) = (kind.to_owned(), placement.clone(), (max / 2).max(1));
        Self(Arc::new(Kept { cluster, kind, placement, half, halves: Mutex::default() }))
    }

    /// The reference kept for `key`, or a new one kept from now on, which asks the owner on its first call. `args`
    /// serve only a new one's lookups: a key kept already keeps its first.
    pub fn get(&self, key: &str, args: Bytes) -> KeyedRef<P> {
        let kept = &self.0;
        let mut halves = kept.halves.lock();
        let [young, old] = &mut *halves;
        if let Some(found) = young.get(key) {
            return found.clone();
        }
        let (key, found) = old.remove_entry(key).unwrap_or_else(|| {
            (key.to_owned(), KeyedRef::new(kept.cluster.clone(), &kept.kind, key, args, &kept.placement))
        });
        young.insert(key, found.clone());
        if young.len() >= kept.half {
            let gone = std::mem::replace(old, std::mem::take(young));
            drop(halves);
            drop(gone);
        }
        found
    }
}

impl<P> KeyedRefs<P> {
    fn len(&self) -> usize {
        self.0.halves.lock().iter().map(BTreeMap::len).sum()
    }
}

impl<P> Clone for KeyedRefs<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P> std::fmt::Debug for KeyedRefs<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyedRefs({}, {} kept)", self.0.kind, self.len())
    }
}

/// A reference that follows a published name, from [`Cluster::named`](crate::Cluster::named): the first call
/// resolves the name and keeps the [`RemoteRef`]; a call that fails with [`Retry::Resolve`] drops it, and the next
/// call resolves again. The lookup is [`Cluster::resolve`](crate::Cluster::resolve), so a node that cannot be asked
/// fails it until that node is marked unreachable; [`among`](Self::among) asks fewer nodes. Nothing is resent but by
/// [`request_until`](Self::request_until). Clones share the kept reference.
pub struct NamedRef<P>(Arc<Follow<P>>);

struct Follow<P> {
    cluster: WeakCluster,
    lookup: Lookup,
    current: Mutex<Option<RemoteRef<P>>>,
}

enum Lookup {
    Key { kind: String, key: String, args: Bytes, placement: Placement },
    Name(String, Placement),
}

/// Between the tries of `request_until`.
const REFUSED: Backoff = Backoff { initial: Duration::from_millis(10), max: Duration::from_millis(100), jitter: 0.2 };

impl<P: WireProtocol> KeyedRef<P> {
    pub(crate) fn new(cluster: WeakCluster, kind: &str, key: &str, args: Bytes, placement: &Placement) -> Self {
        let (kind, key, placement) = (kind.to_owned(), key.to_owned(), placement.clone());
        Self(Follow::new(cluster, Lookup::Key { kind, key, args, placement }))
    }

    /// The node of the reference it keeps: `None` before its first call, and after a call failed with `Resolve`.
    /// It and [`epoch`](Self::epoch) are separate reads, so a clone that asks again in between can change it.
    pub fn node(&self) -> Option<Arc<str>> {
        self.0.current.lock().as_ref().map(|r| r.target.node.clone())
    }

    /// The epoch of the reference it keeps, when keyed actors are strictly owned.
    pub fn epoch(&self) -> Option<u64> {
        self.0.current.lock().as_ref().and_then(RemoteRef::epoch)
    }
}

impl<P: WireProtocol> NamedRef<P> {
    pub(crate) fn new(cluster: WeakCluster, name: &str, among: &Placement) -> Self {
        Self(Follow::new(cluster, Lookup::Name(name.to_owned(), among.clone())))
    }

    /// Looks the name up only on the nodes that carry every `required` label of `placement`, as
    /// [`Cluster::resolve_all_among`](crate::Cluster::resolve_all_among) does, each time it looks: a silent node it
    /// leaves out holds no call up. It keeps no reference found before. No I/O here.
    pub fn among(self, placement: &Placement) -> Self {
        let Lookup::Name(name, _) = &self.0.lookup else { unreachable!("a NamedRef looks up a name") };
        Self::new(self.0.cluster.clone(), name, placement)
    }
}

impl<P: WireProtocol> Follow<P> {
    fn new(cluster: WeakCluster, lookup: Lookup) -> Arc<Self> {
        Arc::new(Self { cluster, lookup, current: Mutex::new(None) })
    }

    async fn remote(&self) -> Result<RemoteRef<P>, ClusterError> {
        if let Some(remote) = self.current.lock().clone() {
            return Ok(remote);
        }
        let cluster = self.cluster.upgrade().ok_or(ClusterError::Closed)?;
        let remote = match &self.lookup {
            // `get_or_spawn` may have started the actor, but sent this call nothing.
            Lookup::Key { kind, key, args, placement } => {
                cluster.get_or_spawn(kind, key, args.clone(), placement).await.map_err(|e| match e {
                    ClusterError::Call(e) => e.of_lookup().into(),
                    e => e,
                })
            }
            Lookup::Name(name, among) => cluster.resolve_among(name, among).await,
        }?;
        *self.current.lock() = Some(remote.clone());
        Ok(remote)
    }

    /// The reference, found by `deadline`, and what is left of it; else `NotSent(Deadline)`.
    async fn remote_by(&self, deadline: Instant) -> Result<(RemoteRef<P>, Duration), ClusterError> {
        let remote = timeout_at(deadline, self.remote())
            .await
            .map_err(|_| ClusterError::Call(CallError::NotSent(SubmitReason::Deadline)))??;
        Ok((remote, deadline.saturating_duration_since(Instant::now())))
    }

    fn settle<T>(&self, used: &RemoteRef<P>, result: Result<T, CallError>) -> Result<T, ClusterError> {
        if let Err(e) = &result
            && e.retry() == Retry::Resolve
        {
            // Only the reference that failed: a clone may already have found the next one.
            let mut current = self.current.lock();
            if current.as_ref().is_some_and(|c| c.target == used.target) {
                *current = None;
            }
        }
        result.map_err(ClusterError::Call)
    }
}

// Two names for one following reference, so each says what it follows and prints as itself.
macro_rules! follows {
    ($($t:ident),*) => {$(
        impl<P> Clone for $t<P> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        impl<P> std::fmt::Debug for $t<P> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match &self.0.lookup {
                    Lookup::Key { kind, key, .. } => write!(f, "{}({kind}/{key})", stringify!($t)),
                    Lookup::Name(name, _) => write!(f, "{}({name})", stringify!($t)),
                }
            }
        }

        impl<P: WireProtocol> $t<P> {
            /// A far node never refuses a tell, but it says when the actor it went to is gone: the tells sent
            /// meanwhile go to dead letters, and the first after that fails with `Resolve`, so the call after it
            /// looks again.
            pub async fn tell(&self, message: P::Message) -> Result<(), ClusterError> {
                let remote = self.0.remote().await?;
                self.0.settle(&remote, remote.tell(message).await)
            }

            pub async fn send(&self, message: P::Message) -> Result<(), ClusterError> {
                let remote = self.0.remote().await?;
                self.0.settle(&remote, remote.send(message).await)
            }

            pub async fn request(&self, message: P::Message) -> Result<P::Reply, ClusterError> {
                let remote = self.0.remote().await?;
                self.0.settle(&remote, remote.request(message).await)
            }

            /// [`RemoteRef::request_within`], with `within` covering the lookup too, whose running out returns
            /// `NotSent(Deadline)`. Nothing is resent.
            pub async fn request_within(&self, message: P::Message, within: Duration) -> Result<P::Reply, ClusterError> {
                let (remote, left) = self.0.remote_by(Instant::now() + within.min(LONGEST)).await?;
                self.0.settle(&remote, remote.request_within(message, left).await)
            }

            /// [`RemoteRef::request_parts`], which settles on its first answer as `request` does.
            pub async fn request_parts(&self, message: P::Message) -> Result<Parts<P>, ClusterError> {
                let remote = self.0.remote().await?;
                self.0.settle(&remote, remote.request_parts(message).await)
            }

            /// [`RemoteRef::request_parts_within`], with `within` covering the lookup too, as
            /// [`request_within`](Self::request_within)'s does.
            pub async fn request_parts_within(&self, message: P::Message, within: Duration) -> Result<Parts<P>, ClusterError> {
                let (remote, left) = self.0.remote_by(Instant::now() + within.min(LONGEST)).await?;
                self.0.settle(&remote, remote.request_parts_within(message, left).await)
            }

            /// [`RemoteRef::upload`], which settles as `request` does. Nothing is resent.
            pub async fn upload<S>(&self, message: P::Message, parts: S) -> Result<P::Reply, ClusterError>
            where
                S: axon::Stream<Item = Result<Bytes, CallError>>,
            {
                let remote = self.0.remote().await?;
                self.0.settle(&remote, remote.upload(message, parts).await)
            }

            /// [`RemoteRef::upload_parts`], which settles on its first answer as `request` does.
            pub async fn upload_parts<S>(&self, message: P::Message, parts: S) -> Result<Parts<P>, ClusterError>
            where
                S: axon::Stream<Item = Result<Bytes, CallError>> + Send + 'static,
            {
                let remote = self.0.remote().await?;
                self.0.settle(&remote, remote.upload_parts(message, parts).await)
            }

            /// [`RemoteRef::request_until`], requesting again after `Resolve` too, which looks again first. Finding the
            /// target sends nothing, so its errors are tried again unless `Never`, and the last is returned as it came.
            /// A lookup still running at `deadline` returns the error before it, or `NotSent(Deadline)`.
            pub async fn request_until(&self, message: P::Message, deadline: Instant) -> Result<P::Reply, ClusterError>
            where
                P::Message: Clone,
            {
                let (follow, message) = (&self.0, &message);
                let again = |e: &ClusterError| matches!(e.retry(), Retry::Later | Retry::Resolve);
                retry_until(deadline, again, || async move {
                    let remote = timeout_at(deadline, follow.remote()).await.map_err(|_| None)?.map_err(Some)?;
                    let within = deadline.saturating_duration_since(Instant::now()).min(remote.engine.cfg.request_timeout);
                    follow.settle(&remote, remote.request_within(message.clone(), within).await).map_err(Some)
                })
                .await
            }
        }
    )*};
}
follows!(KeyedRef, NamedRef);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::disallowed_methods, reason = "the test runs real threads")]
    fn keyed_refs_keep_at_most_their_bound_under_concurrent_gets() {
        let refs =
            KeyedRefs::<crate::Unaddressed>::new(WeakCluster(std::sync::Weak::new()), "k", &Placement::default(), 8);
        std::thread::scope(|threads| {
            for t in 0..8u64 {
                let refs = refs.clone();
                threads.spawn(move || {
                    let mut at = t;
                    for _ in 0..2000 {
                        at = at.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
                        let _ = refs.get(&format!("key-{}", (at >> 33) % 100), Bytes::new());
                        assert!(refs.len() <= 8, "{refs:?}");
                    }
                });
            }
        });
        assert!((1..=8).contains(&refs.len()), "{refs:?}");
        // The key got last is always kept: the same reference comes back.
        let last = refs.get("last", Bytes::new());
        assert!(Arc::ptr_eq(&last.0, &refs.get("last", Bytes::new()).0));
    }
}
