//! Clustering for axon: discovery-driven membership with direct failure detection, typed
//! remote calls, remote spawn, keyed actors with rendezvous placement, and a control RPC.
//!
//! Membership is whatever discovery says, refined by what this node can reach. Nothing here
//! is consensus: keyed actors are unique per membership view, and strictly unique only with
//! an [`OwnershipAuthority`].
//!
//! Guide: [`axon::guide::cluster`].

#![deny(clippy::unwrap_used, clippy::iter_over_hash_type)]
mod cluster;
#[cfg(feature = "consul")]
mod consul;
mod discovery;
mod engine;
mod error;
mod membership;
mod ownership;
mod paxos;
mod placement;
mod quota;
mod remote;
mod seed;
mod spawn;
mod wire;

pub use cluster::{
    AccessPolicy, Cluster, ClusterConfig, ClusterStats, ControlRequest, EndpointBuilder, PendingReply, WeakCluster,
};
#[cfg(feature = "consul")]
pub use consul::{ConsulConfig, ConsulDiscovery, ConsulLeases};
pub use discovery::{
    BoxFuture, DRAINING, Discovery, KINDS, LEAVE_REASON, LIFECYCLE, MemoryDiscovery, NodeRecord, ProviderInfo,
    ProviderLink, Snapshot, StaticDiscovery,
};
pub use engine::{Action, Authorizer, EngineStats, Parts, PeerStats, REJECTION_NAMES, Reply, Tls, Upload};
pub use error::{CallError, ClusterError, DiscoveryError, OwnershipError, Rejection, Retry, SpawnRefusal};
pub use membership::{
    Member, MemberChange, MembershipEvent, MembershipSubscription, NodeChange, NodeChanges, Nodes, View,
};
pub use ownership::{Holder, MemoryAuthority, MemoryLeases, OwnershipAuthority};
pub use paxos::{PaxosConfig, PaxosLeases};
pub use placement::{Placement, Strategy, rendezvous};
pub use quota::{Denied, Permit, Permits, QuotaConfig, QuotaStats, Quotas, Rate};
pub use remote::{KeyedRef, KeyedRefs, NamedRef, RemoteRef};
pub use seed::{
    SeedConfig, SeedDiscovery, SeedLinkStats, SeedServer, SeedServerConfig, SeedServerStats, SeedStats, SeedTimings,
};
pub use spawn::{
    HELD_RETRY, Hosted, HostedKey, HostedKind, KEYED_ENDS, KEYED_STARTS, KeyWatch, KindBuilder, Owner, SpawnContext,
};
pub use wire::{ActorProtocol, Unaddressed, WireProtocol};

/// What the simulations share.
#[cfg(test)]
mod sim {
    use std::time::Duration;

    /// A process's clock: its own rate, and what it reads at real time zero.
    #[derive(Clone, Copy)]
    pub(crate) struct Clock {
        pub ppm: i64,
        pub base: Duration,
    }

    impl Clock {
        /// Real time at the clock's rate, from zero.
        pub fn elapsed(&self, real: Duration) -> Duration {
            let ns = real.as_nanos() as i128;
            Duration::from_nanos((ns + ns * self.ppm as i128 / 1_000_000) as u64)
        }

        pub fn now(&self, real: Duration) -> Duration {
            self.base + self.elapsed(real)
        }

        /// `now` in microseconds, for a clock whose base is a time of day.
        pub fn unix_us(&self, real: Duration) -> u64 {
            self.now(real).as_micros() as u64
        }
    }
}
