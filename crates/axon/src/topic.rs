use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::dead::DeadLetters;
use crate::lifecycle::{ExitWatcher, LifecycleHandle, Member, Watched};
use crate::system::{ActorHandle, SupervisedHandle, SystemError};
use crate::types::{ActorIncarnation, Enqueued, Exit, Reject};

/// A system's topics by name, each an `Arc<Shared<M>>` of its own message type.
pub(crate) type Registry = Mutex<HashMap<Arc<str>, Arc<dyn Any + Send + Sync>>>;

/// What a topic tells: an actor, or a supervised family, which stays subscribed across its restarts.
pub enum Subscriber<M: Send + 'static> {
    Actor(ActorHandle<M>),
    Supervised(SupervisedHandle<M>),
}

impl<M: Send + 'static> From<ActorHandle<M>> for Subscriber<M> {
    fn from(handle: ActorHandle<M>) -> Self {
        Self::Actor(handle)
    }
}

impl<M: Send + 'static> From<SupervisedHandle<M>> for Subscriber<M> {
    fn from(handle: SupervisedHandle<M>) -> Self {
        Self::Supervised(handle)
    }
}

impl<M: Send + 'static> Subscriber<M> {
    fn lifecycle(&self) -> &LifecycleHandle {
        match self {
            Self::Actor(actor) => &actor.lifecycle,
            Self::Supervised(family) => &family.lifecycle,
        }
    }

    fn tell(&self, msg: M) -> Result<Enqueued, Reject<M>> {
        match self {
            Self::Actor(actor) => actor.tell(msg),
            Self::Supervised(family) => family.tell(msg),
        }
    }
}

/// A subscriber with its id, read once: a family's incarnation is behind its lock.
struct Subscribed<M: Send + 'static> {
    id: ActorIncarnation,
    subscriber: Subscriber<M>,
}

impl<M: Send + 'static> Member for Subscribed<M> {
    fn lifecycle(&self) -> &LifecycleHandle {
        self.subscriber.lifecycle()
    }

    fn id(&self) -> ActorIncarnation {
        self.id
    }
}

/// A named fan-out to local actors of one message type. A plain value: clones share the subscribers. A subscriber that
/// ends is dropped before its exit is visible.
pub struct Topic<M: Send + 'static>(Arc<Shared<M>>);

struct Shared<M: Send + 'static> {
    name: Arc<str>,
    registry: Weak<Registry>,
    dead: Arc<DeadLetters>,
    /// Handles to it; while there is one, or a subscriber, the registry keeps the topic.
    handles: AtomicUsize,
    list: Watched<Subscribed<M>>,
}

impl<M: Send + 'static> Clone for Topic<M> {
    fn clone(&self) -> Self {
        self.0.handles.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl<M: Send + 'static> std::fmt::Debug for Topic<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Topic({}, {} subscribers)", self.0.name, self.subscribers())
    }
}

impl<M: Send + 'static> Drop for Topic<M> {
    fn drop(&mut self) {
        if self.0.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.release();
        }
    }
}

/// A subscriber's id, its generation-0 incarnation: a family's generations share their slot, so this keys the family,
/// which keeps it across restarts and ends with it.
fn id(incarnation: ActorIncarnation) -> ActorIncarnation {
    ActorIncarnation { generation: 0, ..incarnation }
}

impl<M: Send + 'static> Topic<M> {
    /// `publish` tells each subscriber in turn, so this bounds its cost.
    pub const MAX_SUBSCRIBERS: usize = 4096;

    pub(crate) fn open(registry: &Arc<Registry>, dead: &Arc<DeadLetters>, name: &str) -> Result<Self, SystemError> {
        let mut topics = registry.lock();
        let topic = match topics.get(name) {
            Some(topic) => topic.clone(),
            None => {
                let name: Arc<str> = name.into();
                let topic = Arc::new_cyclic(|me: &Weak<Shared<M>>| Shared {
                    name: name.clone(),
                    registry: Arc::downgrade(registry),
                    dead: dead.clone(),
                    handles: AtomicUsize::new(0),
                    list: Watched::new(me.clone() as Weak<dyn ExitWatcher>, Self::MAX_SUBSCRIBERS),
                });
                topics.insert(name, topic.clone());
                topic
            }
        };
        let topic = topic.downcast::<Shared<M>>().map_err(|_| SystemError::NameTaken(name.to_string()))?;
        // Under the registry's lock, as `release` reads it there.
        topic.handles.fetch_add(1, Ordering::Relaxed);
        Ok(Self(topic))
    }

    /// Subscribes an actor or a supervised family; one already subscribed is left as it is, and one that has ended
    /// already is dropped at once.
    pub fn subscribe(&self, subscriber: impl Into<Subscriber<M>>) -> Result<(), SystemError> {
        let subscriber = subscriber.into();
        let id = id(subscriber.lifecycle().incarnation());
        if self.0.list.add(Subscribed { id, subscriber }) { Ok(()) } else { Err(SystemError::SubscriberLimit) }
    }

    /// Takes out the actor or family `lifecycle` is of; nothing if it is not subscribed.
    pub fn unsubscribe(&self, lifecycle: &LifecycleHandle) {
        drop(self.0.list.remove(id(lifecycle.incarnation())));
    }

    /// Tells every subscriber, never waiting: one that refuses misses it, recorded as a dead letter. Returns how many
    /// took it. Clones `msg` for each subscriber; its `Clone` and `Drop` run while the topic is held, so they must not
    /// subscribe or unsubscribe.
    pub fn publish<T: Into<M> + Clone>(&self, msg: T) -> usize {
        self.0.list.read().iter().map(|to| self.0.tell(&to.subscriber, msg.clone().into())).sum()
    }

    pub fn subscribers(&self) -> usize {
        self.0.list.read().len()
    }
}

impl<M: Send + 'static> Shared<M> {
    fn tell(&self, to: &Subscriber<M>, msg: M) -> usize {
        let Err(Reject { reason, .. }) = to.tell(msg) else { return 1 };
        self.dead.record_missed(reason, &self.name);
        0
    }

    /// Takes the topic out of the registry once no handle and no subscriber holds it.
    fn release(&self) {
        let Some(registry) = self.registry.upgrade() else { return };
        let mut topics = registry.lock();
        let held = self.handles.load(Ordering::Acquire) > 0 || !self.list.read().is_empty();
        // A handle's drop and a subscriber's exit can both release it: the first may take it out and a newer topic take
        // the name before the second looks here.
        let ours = topics.get(&self.name).is_some_and(|t| std::ptr::addr_eq(Arc::as_ptr(t), self));
        if !held && ours {
            topics.remove(&self.name);
        }
    }
}

impl<M: Send + 'static> ExitWatcher for Shared<M> {
    fn exited(&self, exit: &Exit) {
        if self.list.exited(id(exit.incarnation)) {
            self.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{Idle, threadless_system};

    #[tokio::test]
    async fn a_subscriber_is_watched_once_however_often_subscribed_and_unsubscribing_unwatches_it() {
        let system = threadless_system();
        let actor = system.spawn(Idle).start().unwrap();
        let family = system.supervise(|| Idle).start_supervised().unwrap();
        let watchers = || (actor.lifecycle.0.signal().watchers(), family.lifecycle.0.signal().watchers());
        let base = watchers();
        let topic = system.topic::<()>("idle").unwrap();
        for _ in 0..3 {
            topic.subscribe(actor.clone()).unwrap();
            topic.subscribe(family.clone()).unwrap();
        }
        assert_eq!((topic.subscribers(), watchers()), (2, (base.0 + 1, base.1 + 1)));
        for _ in 0..3 {
            topic.unsubscribe(&actor.lifecycle);
            topic.unsubscribe(&family.lifecycle);
            topic.subscribe(actor.clone()).unwrap();
            // A generation's lifecycle, as the family's actor has in its handler, takes out the family and its watch.
            topic.subscribe(family.clone()).unwrap();
            topic.unsubscribe(&family.family.current().unwrap().lifecycle());
        }
        topic.unsubscribe(&actor.lifecycle);
        assert_eq!((topic.subscribers(), watchers()), (0, base));
        system.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_system_dropped_with_subscribers_takes_its_topics_watches_back() {
        let system = threadless_system();
        let actor = system.spawn(Idle).start().unwrap();
        // The system's own watch, which outlives it as a watcher that is gone.
        let watchers = || actor.lifecycle.0.signal().watchers();
        let base = watchers();
        for name in ["a", "b"] {
            system.topic::<()>(name).unwrap().subscribe(actor.clone()).unwrap();
        }
        assert_eq!(watchers(), base + 2);
        drop(system);
        assert_eq!(watchers(), base);
    }

    #[tokio::test]
    #[expect(clippy::disallowed_methods, reason = "the race needs real threads")]
    async fn an_unsubscribe_racing_a_subscribe_leaves_no_watch_behind() {
        let system = threadless_system();
        let actor = system.spawn(Idle).start().unwrap();
        let base = actor.lifecycle.0.signal().watchers();
        let topic = system.topic::<()>("race").unwrap();
        let start = std::sync::Barrier::new(2);
        for _ in 0..20_000 {
            std::thread::scope(|s| {
                s.spawn(|| {
                    start.wait();
                    topic.subscribe(actor.clone()).unwrap();
                });
                start.wait();
                topic.unsubscribe(&actor.lifecycle);
            });
            // Before the next unsubscribe, which would take back a stray watch too.
            assert_eq!(actor.lifecycle.0.signal().watchers(), base + topic.subscribers());
            topic.unsubscribe(&actor.lifecycle);
        }
        system.shutdown().await.unwrap();
    }
}
