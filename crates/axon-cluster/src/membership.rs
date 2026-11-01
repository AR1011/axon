//! The local view of the cluster: the discovery snapshot plus this node's own reachability
//! observations. It is observer-local, never a consensus view.

use std::collections::{BTreeSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::task::{Context, Poll};

use axon::Stream;
use parking_lot::Mutex;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::WeakCluster;
use crate::discovery::{NodeRecord, Snapshot};
use crate::placement::{Candidates, Placement};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Member {
    pub record: NodeRecord,
    /// False once this node lost its session to the member and could not redial it.
    pub reachable: bool,
}

impl Member {
    /// Eligible as a routing or placement target.
    pub fn available(&self) -> bool {
        self.reachable && !self.record.draining()
    }
}

#[derive(Clone, Debug, Default)]
pub struct View {
    pub sequence: u64,
    pub revision: u64,
    /// Sorted by node id.
    pub members: Vec<Member>,
    /// Set while discovery is failing; the members are the last good snapshot.
    pub stale: Option<String>,
    /// Worked out by the owner lookups made in this view.
    pub(crate) candidates: Candidates,
}

impl View {
    pub fn get(&self, id: &str) -> Option<&Member> {
        self.members.binary_search_by(|m| m.record.id.as_str().cmp(id)).ok().map(|i| &self.members[i])
    }

    /// The available members that carry every `required` label of `placement`, in id order.
    pub(crate) fn admitted(&self, placement: &Placement) -> impl Iterator<Item = &Member> {
        self.members.iter().filter(|m| m.available() && placement.admits(&m.record))
    }

    fn admitted_ids(&self, placement: &Placement) -> impl Iterator<Item = Arc<str>> {
        self.admitted(placement).map(|m| m.record.id.as_str().into())
    }
}

/// The live set of the available nodes that carry every label a placement requires, from
/// [`Cluster::nodes`](crate::Cluster::nodes). Each call reads the view as it is then and sends nothing. It holds the
/// cluster weakly: once the cluster is gone the set is empty.
#[derive(Clone, Debug)]
pub struct Nodes {
    cluster: WeakCluster,
    placement: Placement,
}

/// A node that joined or left a [`Nodes`] set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeChange {
    Joined(Arc<str>),
    Left(Arc<str>),
}

impl Nodes {
    pub(crate) fn new(cluster: WeakCluster, placement: &Placement) -> Self {
        Self { cluster, placement: placement.clone() }
    }

    fn view(&self) -> Option<Arc<View>> {
        self.cluster.upgrade().map(|c| c.view())
    }

    /// The members, in id order.
    pub fn ids(&self) -> Vec<Arc<str>> {
        self.view().map(|view| view.admitted_ids(&self.placement).collect()).unwrap_or_default()
    }

    /// The members ranked for `key` by rendezvous hashing, first choice first: the same on every node with the same
    /// view, and a member leaving moves only the keys it led.
    pub fn order(&self, key: &str) -> Vec<Arc<str>> {
        let Some(view) = self.view() else { return Vec::new() };
        crate::placement::order(key, view.admitted(&self.placement).map(|m| m.record.id.as_str()))
            .into_iter()
            .map(Arc::from)
            .collect()
    }

    /// `Joined` for each member now, in id order, then each change as the view moves: `Left` for each node gone, then
    /// `Joined` for each new one. A subscriber that falls behind gets the difference to the view it resyncs to, so it
    /// always ends at the view's set. `None` once the cluster shuts down, and at once for a set made after that.
    pub fn changes(&self) -> NodeChanges {
        NodeChanges {
            events: self.cluster.upgrade().and_then(|c| c.0.engine.membership.subscribe_open()),
            placement: self.placement.clone(),
            reported: BTreeSet::new(),
            pending: VecDeque::new(),
        }
    }
}

/// The changes of a [`Nodes`] set, an `axon::Stream` for `cx.attach`. Holds the membership, never the cluster.
pub struct NodeChanges {
    events: Option<MembershipSubscription>,
    placement: Placement,
    reported: BTreeSet<Arc<str>>,
    /// One view's difference to what was reported, at most twice `max_nodes`.
    pending: VecDeque<NodeChange>,
}

impl NodeChanges {
    pub async fn next(&mut self) -> Option<NodeChange> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    fn diff(&mut self, view: &View) {
        let now: BTreeSet<Arc<str>> = view.admitted_ids(&self.placement).collect();
        let left = self.reported.difference(&now).cloned().map(NodeChange::Left);
        let joined = now.difference(&self.reported).cloned().map(NodeChange::Joined);
        self.pending.extend(left.chain(joined));
        self.reported = now;
    }
}

impl Stream for NodeChanges {
    type Item = NodeChange;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NodeChange>> {
        loop {
            if let Some(change) = self.pending.pop_front() {
                return Poll::Ready(Some(change));
            }
            let Some(events) = &mut self.events else { return Poll::Ready(None) };
            match std::task::ready!(Pin::new(events).poll_next(cx)) {
                Some(
                    MembershipEvent::Snapshot(view)
                    | MembershipEvent::Changes { view, .. }
                    | MembershipEvent::Resync(view),
                ) => self.diff(&view),
                // It carries no view, and the members stay as they were.
                Some(MembershipEvent::Unavailable(_)) => {}
                None => self.events = None,
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberChange {
    Joined(NodeRecord),
    /// Gone from discovery. Not proof that the process exited.
    Left(NodeRecord),
    Updated {
        previous: NodeRecord,
        current: NodeRecord,
    },
    /// Same node id, new boot.
    Replaced {
        previous: NodeRecord,
        current: NodeRecord,
    },
    Unreachable(String),
    Reachable(String),
}

#[derive(Clone, Debug)]
pub enum MembershipEvent {
    Snapshot(Arc<View>),
    Changes {
        view: Arc<View>,
        changes: Arc<[MemberChange]>,
    },
    /// The subscriber fell behind; intermediate changes are lost, never fabricated.
    Resync(Arc<View>),
    Unavailable(String),
}

#[derive(Default)]
pub(crate) struct Churn {
    pub joined: AtomicU64,
    pub left: AtomicU64,
    pub replaced: AtomicU64,
    pub unreachable: AtomicU64,
    pub discovery_errors: AtomicU64,
}

pub(crate) struct Membership {
    view: Mutex<Arc<View>>,
    /// Taken when the cluster closes, which ends every subscription.
    events: Mutex<Option<broadcast::Sender<MembershipEvent>>>,
    pub churn: Churn,
}

type Events = broadcast::Receiver<MembershipEvent>;
type Next = Pin<Box<dyn Future<Output = (Result<MembershipEvent, RecvError>, Events)> + Send + Sync>>;

/// Also an `axon::Stream`, so an actor can `cx.attach` it.
pub struct MembershipSubscription {
    first: Option<Arc<View>>,
    /// Owns the receiver, which it hands back with each event.
    next: Next,
    source: Arc<Membership>,
}

// ponytail: one allocation per event, which membership's rate affords.
fn receive(mut events: Events) -> Next {
    Box::pin(async move { (events.recv().await, events) })
}

impl MembershipSubscription {
    /// The first event is always a `Snapshot`. `None` once the cluster has shut down, even while handles to it remain.
    pub async fn next(&mut self) -> Option<MembershipEvent> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}

impl Stream for MembershipSubscription {
    type Item = MembershipEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<MembershipEvent>> {
        if let Some(view) = self.first.take() {
            return Poll::Ready(Some(MembershipEvent::Snapshot(view)));
        }
        let Poll::Ready((received, mut events)) = self.next.as_mut().poll(cx) else { return Poll::Pending };
        let event = match received {
            Ok(event) => Some(event),
            Err(RecvError::Lagged(_)) => {
                let view = self.source.view.lock();
                let sender = self.source.events.lock();
                sender.as_ref().map(|sender| {
                    events = sender.subscribe();
                    MembershipEvent::Resync(view.clone())
                })
            }
            Err(RecvError::Closed) => None,
        };
        self.next = receive(events);
        Poll::Ready(event)
    }
}

impl Membership {
    pub fn new(event_capacity: usize) -> Arc<Self> {
        let events = Mutex::new(Some(broadcast::Sender::new(event_capacity.max(1))));
        Arc::new(Self { view: Mutex::default(), events, churn: Churn::default() })
    }

    pub fn view(&self) -> Arc<View> {
        self.view.lock().clone()
    }

    /// Once closed: the last view, then the end.
    pub fn subscribe(self: &Arc<Self>) -> MembershipSubscription {
        self.subscribe_open().unwrap_or_else(|| MembershipSubscription {
            first: Some(self.view()),
            next: receive(broadcast::channel(1).1),
            source: self.clone(),
        })
    }

    /// `None` once closed. Snapshot and cursor are taken under one lock so no change slips between them.
    pub fn subscribe_open(self: &Arc<Self>) -> Option<MembershipSubscription> {
        let view = self.view.lock();
        let events = self.events.lock().as_ref()?.subscribe();
        Some(MembershipSubscription { first: Some(view.clone()), next: receive(events), source: self.clone() })
    }

    pub fn close(&self) {
        self.events.lock().take();
    }

    fn send(&self, event: MembershipEvent) {
        if let Some(events) = &*self.events.lock() {
            let _ = events.send(event);
        }
    }

    fn publish(&self, current: &mut Arc<View>, mut next: View, event: impl FnOnce(Arc<View>) -> MembershipEvent) {
        next.sequence = current.sequence + 1;
        *current = Arc::new(next);
        self.send(event(current.clone()));
    }

    /// `nodes` must be validated, sorted and free of duplicate ids.
    pub fn apply(&self, snapshot: Snapshot) -> Vec<MemberChange> {
        let mut current = self.view.lock();
        let mut changes = Vec::new();
        let mut members = Vec::with_capacity(snapshot.nodes.len());
        for record in snapshot.nodes {
            let previous = current.get(&record.id);
            let same_boot = previous.is_some_and(|p| p.record.incarnation == record.incarnation);
            match previous {
                None => changes.push(MemberChange::Joined(record.clone())),
                Some(p) if !same_boot => {
                    changes.push(MemberChange::Replaced { previous: p.record.clone(), current: record.clone() })
                }
                Some(p) if p.record != record => {
                    changes.push(MemberChange::Updated { previous: p.record.clone(), current: record.clone() })
                }
                Some(_) => {}
            }
            members.push(Member { reachable: !same_boot || previous.is_some_and(|p| p.reachable), record });
        }
        for gone in
            current.members.iter().filter(|m| members.binary_search_by(|n| n.record.id.cmp(&m.record.id)).is_err())
        {
            changes.push(MemberChange::Left(gone.record.clone()));
        }
        for change in &changes {
            match change {
                MemberChange::Joined(_) => self.churn.joined.fetch_add(1, Relaxed),
                MemberChange::Left(_) => self.churn.left.fetch_add(1, Relaxed),
                MemberChange::Replaced { .. } => self.churn.replaced.fetch_add(1, Relaxed),
                _ => 0,
            };
        }
        if !changes.is_empty() || current.stale.is_some() || current.revision != snapshot.revision {
            let next = View { revision: snapshot.revision, members, ..View::default() };
            let changes = changes.clone().into();
            self.publish(&mut current, next, |view| MembershipEvent::Changes { view, changes });
        }
        changes
    }

    pub fn set_reachable(&self, id: &str, incarnation: u128, reachable: bool) {
        let mut current = self.view.lock();
        let Some(member) = current.get(id) else { return };
        if member.reachable == reachable || member.record.incarnation != incarnation {
            return;
        }
        let mut next = View::clone(&current);
        next.members.iter_mut().find(|m| m.record.id == id).expect("member present").reachable = reachable;
        let change = if reachable {
            MemberChange::Reachable(id.to_owned())
        } else {
            self.churn.unreachable.fetch_add(1, Relaxed);
            MemberChange::Unreachable(id.to_owned())
        };
        self.publish(&mut current, next, |view| MembershipEvent::Changes { view, changes: vec![change].into() });
    }

    /// Keeps the last good members, marked stale. Repeats of the same error are not re-announced.
    pub fn unavailable(&self, mut error: String) {
        self.churn.discovery_errors.fetch_add(1, Relaxed);
        error.truncate(error.floor_char_boundary(1024));
        let mut current = self.view.lock();
        if current.stale.as_ref() == Some(&error) {
            return;
        }
        let mut next = View::clone(&current);
        next.stale = Some(error.clone());
        self.publish(&mut current, next, |_| MembershipEvent::Unavailable(error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_subscriber_that_falls_behind_resyncs_and_ends_when_the_membership_closes() {
        let membership = Membership::new(1);
        let mut events = membership.subscribe();
        for error in ["one", "two", "three"] {
            membership.unavailable(error.into());
        }
        assert!(matches!(events.next().await, Some(MembershipEvent::Snapshot(_))));
        let resync = events.next().await;
        assert!(matches!(resync, Some(MembershipEvent::Resync(view)) if view.stale.as_deref() == Some("three")));
        membership.unavailable("four".into());
        assert!(matches!(events.next().await, Some(MembershipEvent::Unavailable(e)) if e == "four"));
        membership.close();
        assert!(events.next().await.is_none());
    }
}
