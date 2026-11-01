use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{MappedRwLockReadGuard, RwLockReadGuard};

use crate::lifecycle::{ExitWatcher, LifecycleHandle, Member, Watched};
use crate::reply::ReplyTo;
use crate::system::ActorHandle;
use crate::trace::mix;
use crate::types::{ActorIncarnation, Enqueued, Exit, Reject, RejectReason, RequestError};

/// How `Router::tell`, `send` and `ask` choose a member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routing {
    RoundRobin,
    /// The fewest queued messages when the call is made; ties in turn.
    LeastLoaded,
}

/// Local actors of one message type behind one address. A plain value: clones share the members. A member that ends
/// is dropped before its exit is visible.
pub struct Router<M: Send + 'static>(Arc<Members<M>>);

struct Members<M: Send + 'static> {
    routing: Routing,
    turn: AtomicUsize,
    list: Watched<Routed<M>>,
}

struct Routed<M: Send + 'static> {
    /// What `member` weighs keys against.
    hash: u64,
    handle: ActorHandle<M>,
}

impl<M: Send + 'static> Member for Routed<M> {
    fn lifecycle(&self) -> &LifecycleHandle {
        &self.handle.lifecycle
    }

    fn id(&self) -> ActorIncarnation {
        self.handle.address.incarnation()
    }
}

impl<M: Send + 'static> Clone for Router<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: Send + 'static> std::fmt::Debug for Router<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Router({:?}, {} members)", self.0.routing, self.0.list.read().len())
    }
}

impl<M: Send + 'static> Router<M> {
    pub fn new(routing: Routing) -> Self {
        Self(Arc::new_cyclic(|me: &Weak<Members<M>>| Members {
            routing,
            turn: AtomicUsize::new(0),
            list: Watched::new(me.clone() as Weak<dyn ExitWatcher>, usize::MAX),
        }))
    }

    /// Adds a member; one that has ended already is dropped at once, and one already here is left as it is.
    pub fn add(&self, member: ActorHandle<M>) {
        self.0.list.add(Routed { hash: hash(&member.address.incarnation()), handle: member });
    }

    pub fn members(&self) -> Vec<ActorHandle<M>> {
        self.0.list.read().iter().map(|m| m.handle.clone()).collect()
    }

    /// The member for `key`: rendezvous, so a member leaving moves only its keys and one joining takes only those it wins.
    pub fn member(&self, key: &(impl Hash + ?Sized)) -> Option<ActorHandle<M>> {
        let key = hash(key);
        // ponytail: scores every member per call; a sorted ring if routers grow to thousands of members.
        self.0.list.read().iter().max_by_key(|m| mix(key ^ m.hash)).map(|m| m.handle.clone())
    }

    pub fn tell(&self, msg: impl Into<M>) -> Result<Enqueued, Reject<M>> {
        match self.pick() {
            Some(member) => member.tell(msg),
            None => Err(Reject { reason: RejectReason::NotReady, message: msg.into() }),
        }
    }

    /// Waits for the chosen member's room, as its handle's `send` does.
    pub async fn send(&self, msg: impl Into<M>) -> Result<Enqueued, Reject<M>> {
        match self.pick().map(|m| m.clone()) {
            Some(member) => member.send(msg).await,
            None => Err(Reject { reason: RejectReason::NotReady, message: msg.into() }),
        }
    }

    pub async fn ask<R, T: Into<M>>(&self, build: impl FnOnce(ReplyTo<R>) -> T) -> Result<R, RequestError> {
        match self.pick().map(|m| m.clone()) {
            Some(member) => member.ask(build).await,
            None => Err(RequestError::NotAdmitted(RejectReason::NotReady)),
        }
    }

    /// A read guard on the member list: clone the handle before any await.
    fn pick(&self) -> Option<MappedRwLockReadGuard<'_, ActorHandle<M>>> {
        let list = self.0.list.read();
        let n = list.len();
        if n == 0 {
            return None;
        }
        let turn = || self.0.turn.fetch_add(1, Ordering::Relaxed);
        // Passes over a member that is stopping or draining: it refuses everything until its exit drops it, and
        // least-loaded would favour it, as its queue only shrinks.
        let admits = |i: &usize| list[*i].handle.address.0.admission_phase().is_ok();
        let at = match self.0.routing {
            // A turn per member passed over, so the next one does not take its share too; other callers' turns can
            // skip the one that admits, hence the scan after.
            Routing::RoundRobin => (0..n).map(|_| turn() % n).find(admits).or_else(|| (0..n).find(admits)),
            // ponytail: a look at every member per message; sample two members if pools grow large.
            Routing::LeastLoaded => {
                let turn = turn();
                (0..n)
                    .map(|k| turn.wrapping_add(k) % n)
                    .filter(admits)
                    .min_by_key(|&i| list[i].handle.address.0.queued())
            }
        }
        .unwrap_or(0); // none admits: the member's own refusal, Closed
        Some(RwLockReadGuard::map(list, |list| &list[at].handle))
    }
}

impl<M: Send + 'static> ExitWatcher for Members<M> {
    fn exited(&self, exit: &Exit) {
        self.list.exited(exit.incarnation);
    }
}

fn hash(value: &(impl Hash + ?Sized)) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Idle, threadless_system};

    #[tokio::test]
    async fn a_member_is_watched_once_however_often_added_and_the_last_router_dropped_unwatches_it() {
        let system = threadless_system();
        let member = system.spawn(Idle).start().unwrap();
        let watchers = || member.lifecycle.0.signal().watchers();
        let base = watchers();
        let router = Router::new(Routing::RoundRobin);
        for _ in 0..3 {
            router.add(member.clone());
        }
        assert_eq!((router.members().len(), watchers()), (1, base + 1));
        let clone = router.clone();
        drop(router);
        assert_eq!(watchers(), base + 1, "a clone still holds the members");
        drop(clone);
        assert_eq!(watchers(), base);
        system.shutdown().await.unwrap();
    }
}
