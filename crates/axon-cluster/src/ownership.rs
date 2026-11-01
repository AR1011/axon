use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::discovery::{BoxFuture, Caught, ProviderInfo, caught, caught_some};
use crate::{Cluster, ClusterError, OwnershipError};

/// Taken off what a holder believes, and added to a restarted grantor's wait.
const MARGIN: Duration = Duration::from_millis(200);

/// How long a holder of a lease of `lease` believes after it sent the request that was granted, on clocks that drift
/// apart by up to `drift`. Paxos leases and quota grants both hold by it.
pub(crate) fn lease_hold(lease: Duration, drift: f64) -> Duration {
    lease.mul_f64(1.0 - drift).saturating_sub(MARGIN)
}

/// How long a restarted grantor refuses everything, on a clock that may run fast.
pub(crate) fn lease_wait(lease: Duration, drift: f64) -> Duration {
    lease.mul_f64(1.0 + drift) + MARGIN
}

/// Whose keys [`OwnershipAuthority::held_by`] lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Holder<'a> {
    /// The keys a process of this node holds, or held when it stopped renewing without releasing them, until another
    /// node takes them: what an eager kind restarts after the node is gone.
    Node(&'a str),
    /// Every key whose holder stopped renewing without releasing it, whichever node it was, until another node takes
    /// it: what a node restarts of a holder none of its peers saw go.
    Lapsed,
}

/// A lease service with fencing epochs, for keys that must never have two live owners. An
/// instance acts for one node. Exclusivity is only as strong as the effect sinks that check
/// the epoch.
pub trait OwnershipAuthority: Send + Sync + 'static {
    /// Takes the lease on `key`. The epoch grows with every grant of that key. A key this node holds
    /// may be answered with the epoch it holds, without extending it. Dropped before it answers, it leaves
    /// nothing held for the next call to be answered from: a grant that arrives after is given up.
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>>;

    /// Extends every lease this node holds from the moment it is called, and names the keys it could
    /// not extend: one whose extension was already under way counts as not extended. A key answered
    /// [`Lost`](OwnershipError::Lost) is no longer held by this node, and is named by every renewal
    /// until it is released or taken again; any other answer leaves it held as long as its last
    /// extension lasts. An error is the answer for every key.
    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>>;

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>>;

    /// Gives up whatever the authority holds for this node, after every lease was released: a clean leave.
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }

    fn describe(&self) -> ProviderInfo {
        ProviderInfo::of::<Self>()
    }

    /// The keys named `kind/key`, for each of `kinds`, that `holder` holds ([`Holder`]). `None` when the authority
    /// cannot tell, and then [`KindBuilder::eager`](crate::KindBuilder::eager) is refused.
    fn held_by(
        &self,
        holder: Holder<'_>,
        kinds: &[String],
    ) -> Option<BoxFuture<'_, Result<Vec<String>, OwnershipError>>> {
        let _ = (holder, kinds);
        None
    }

    /// How long a holder may act after it sent a renew that succeeded, if the authority bounds it.
    /// The cluster refuses a `lease_grace` beyond it, and takes 7/8 of it when none is set. A session quiet for longer
    /// is not trusted: the transport idle timeout is shortened to it.
    fn hold(&self) -> Option<Duration> {
        None
    }

    /// Whether a grant could reach this node now, as far as the authority knows without asking: `PaxosLeases` answers
    /// whether more than half its acceptors are neither unreachable in this node's view nor failing on their last
    /// request. A node that cannot grant refuses a key's start at once with `SpawnRefusal::Isolated`, so its callers
    /// try the key's next node, and is not handed a key moved to it. Cheap: asked on every keyed start.
    fn can_grant(&self) -> bool {
        true
    }

    /// Whether the authority still refuses every grant after its start, as a restarted Paxos acceptor does. A node
    /// whose authority warms is not ready ([`Cluster::warming`]). Cheap: a readiness probe asks it on every request.
    fn warming(&self) -> bool {
        false
    }

    /// Awaited once as the cluster starts, before it announces itself, for an authority that serves
    /// and calls over the cluster or reads what it stored. An error fails the start. Keep
    /// [`Cluster::downgrade`], not the cluster, which holds this authority.
    fn attach<'a>(&'a self, cluster: &'a Cluster) -> BoxFuture<'a, Result<(), ClusterError>> {
        let _ = cluster;
        Box::pin(async { Ok(()) })
    }
}

fn panicked() -> OwnershipError {
    OwnershipError::Unavailable("the authority panicked".into())
}

impl OwnershipAuthority for Caught<dyn OwnershipAuthority> {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        caught(panicked, || self.0.acquire(key))
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        caught(panicked, || self.0.renew())
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        caught(panicked, || self.0.release(key, epoch))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        let closing: BoxFuture<'_, Result<(), OwnershipError>> = Box::pin(async {
            self.0.close().await;
            Ok(())
        });
        let closing = caught(panicked, || closing);
        Box::pin(async { drop(closing.await) })
    }

    fn describe(&self) -> ProviderInfo {
        self.0.describe()
    }

    fn held_by(
        &self,
        holder: Holder<'_>,
        kinds: &[String],
    ) -> Option<BoxFuture<'_, Result<Vec<String>, OwnershipError>>> {
        caught_some(panicked, || self.0.held_by(holder, kinds))
    }

    fn hold(&self) -> Option<Duration> {
        self.0.hold()
    }

    fn can_grant(&self) -> bool {
        self.0.can_grant()
    }

    fn warming(&self) -> bool {
        self.0.warming()
    }

    fn attach<'a>(&'a self, cluster: &'a Cluster) -> BoxFuture<'a, Result<(), ClusterError>> {
        self.0.attach(cluster)
    }
}

#[derive(Default)]
struct Leases {
    owners: u64,
    /// key -> (epoch, holder); a released key keeps its epoch with holder 0.
    keys: BTreeMap<String, (u64, u64)>,
    lapsed: Vec<u64>,
}

/// In-process leases for tests and single-process clusters. Not a distributed quorum. Nothing expires by itself: a
/// holder that is gone keeps its keys until `expire` is called for its authority.
#[derive(Clone, Default)]
pub struct MemoryLeases(Arc<Mutex<Leases>>);

pub struct MemoryAuthority {
    leases: MemoryLeases,
    owner: u64,
}

impl MemoryLeases {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn authority(&self) -> MemoryAuthority {
        let mut leases = self.0.lock();
        leases.owners += 1;
        MemoryAuthority { leases: self.clone(), owner: leases.owners }
    }

    /// Expires everything `authority` holds, as a lapsed session would.
    pub fn expire(&self, authority: &MemoryAuthority) {
        let mut leases = self.0.lock();
        leases.keys.values_mut().filter(|(_, holder)| *holder == authority.owner).for_each(|k| k.1 = 0);
        leases.lapsed.push(authority.owner);
    }
}

impl OwnershipAuthority for MemoryAuthority {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        let mut leases = self.leases.0.lock();
        let lease = leases.keys.entry(key.to_owned()).or_default();
        let result = if lease.1 == 0 || lease.1 == self.owner {
            *lease = (lease.0 + 1, self.owner);
            Ok(lease.0)
        } else {
            Err(OwnershipError::Held(None))
        };
        Box::pin(std::future::ready(result))
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        let mut leases = self.leases.0.lock();
        let lapsed = leases.lapsed.iter().position(|o| *o == self.owner).map(|i| leases.lapsed.swap_remove(i));
        Box::pin(std::future::ready(lapsed.map_or(Ok(Vec::new()), |_| Err(OwnershipError::Lost))))
    }

    fn release(&self, key: &str, epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        if let Some(lease) = self.leases.0.lock().keys.get_mut(key).filter(|l| **l == (epoch, self.owner)) {
            lease.1 = 0;
        }
        Box::pin(std::future::ready(Ok(())))
    }

    fn describe(&self) -> ProviderInfo {
        let leases = self.leases.0.lock();
        let held = leases.keys.values().filter(|(_, holder)| *holder == self.owner).count();
        ProviderInfo::new("memory").stat("held", held).stat("keys", leases.keys.len()).stat("owners", leases.owners)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_holder_stops_before_and_a_restarted_grantor_waits_past_the_lease_on_any_clock() {
        let lease = Duration::from_secs(5);
        assert_eq!(lease_hold(lease, 0.01), Duration::from_millis(4750));
        assert_eq!(lease_wait(lease, 0.01), Duration::from_millis(5250));
    }
}
