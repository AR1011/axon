use std::fmt;

/// A definite refusal from the destination node: the message was not delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Rejection {
    UnknownActor = 2,
    SchemaMismatch = 3,
    Full = 4,
    Forbidden = 5,
    InvalidMessage = 6,
    Stopped = 7,
}

impl Rejection {
    pub(crate) fn from_code(code: u8) -> Self {
        match code {
            2 => Self::UnknownActor,
            3 => Self::SchemaMismatch,
            4 => Self::Full,
            5 => Self::Forbidden,
            7 => Self::Stopped,
            _ => Self::InvalidMessage,
        }
    }
}

/// What to do after an error, from [`CallError::retry`] and [`ClusterError::retry`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retry {
    /// Not delivered; the same reference may work after a pause.
    Later,
    /// Not delivered; the reference or this node's view is out of date: resolve again, then retry.
    Resolve,
    /// It may have been handled: retry only a message that is safe to repeat.
    IfIdempotent,
    /// Retrying fails the same way.
    Never,
}

/// [`CallError::retry`] says whether to retry, resolve again or stop.
#[derive(Clone, Debug)]
pub enum CallError {
    Closed,
    /// Too many calls in flight on this node.
    Busy,
    /// The node is not in the membership view.
    NoRoute,
    /// The reference pins a node incarnation that is no longer the live one.
    StaleNode,
    /// No session and the last dial failed; retried in the background.
    Unreachable(axon_net::Error),
    /// Nothing was sent: the local outbox refused the frame, or the deadline passed while the session to the node
    /// was still being set up, or before the local mailbox had room.
    NotSent(axon_net::SubmitReason),
    Rejected(Rejection),
    /// Sent, and no answer arrived before the deadline: it may have been handled.
    Timeout,
    /// The session failed after the call was sent.
    OutcomeUnknown,
    /// The handler dropped the reply handle.
    NoReply,
    Codec(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryError {
    ReadOnly,
    Invalid(&'static str),
    /// A newer incarnation of the node is registered: until it is dropped, as after a wall step back and a restart.
    Stale,
    Capacity,
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnershipError {
    /// Another process holds the lease: of the node named, when the authority can tell (`PaxosLeases` does), which
    /// may be an earlier process of that node.
    Held(Option<String>),
    Lost,
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SpawnRefusal {
    UnknownKind = 1,
    Forbidden = 2,
    Capacity = 3,
    /// The receiving node does not compute itself as the key's owner, or under an authority as the first node of the
    /// order that the caller did not pass.
    NotOwner = 4,
    Leaving = 5,
    Failed = 6,
    /// The lease authority could not be used, as while restarted acceptors warm up, or the lease was lost.
    Ownership = 7,
    /// Another node holds the key's lease, or the key's actor here was still ending for the whole
    /// `operation_timeout`. When the authority names the holder the refusal comes at once and its detail is that
    /// node, which [`get_or_spawn`](crate::Cluster::get_or_spawn) asks next if it can reach it; otherwise the detail
    /// is empty: the lease was waited for, or the key here is on its way to its owner.
    Held = 8,
    /// This node is cut off, though another may not be: it reaches no majority of the lease authority
    /// ([`can_grant`](crate::OwnershipAuthority::can_grant)), or cannot confirm it is still a member (no heartbeat
    /// heard for `heartbeat_grace` and no peer lists it, as after a long pause, or at start while its seeds still warm
    /// up). Refused at once, and under an authority [`get_or_spawn`](crate::Cluster::get_or_spawn) asks the next node.
    Isolated = 9,
}

impl SpawnRefusal {
    pub(crate) fn from_code(code: u8) -> Self {
        use SpawnRefusal::*;
        [UnknownKind, Forbidden, Capacity, NotOwner, Leaving, Failed, Ownership, Held, Isolated]
            .into_iter()
            .find(|r| r.clone() as u8 == code)
            .unwrap_or(Failed)
    }
}

/// [`ClusterError::retry`] says whether to retry, resolve again or stop.
#[derive(Debug)]
pub enum ClusterError {
    Config(&'static str),
    Closed,
    NameTaken,
    NotFound,
    Ambiguous(usize),
    /// No running, reachable member satisfies the placement.
    NoCandidate,
    ProtocolMismatch,
    /// The node serves the name or the kind at `served`, not at the caller's `SCHEMA`, `wanted`. During a rolling
    /// upgrade another node may, or this one after its upgrade.
    SchemaMismatch {
        wanted: u16,
        served: Vec<u16>,
    },
    Spawn(SpawnRefusal, String),
    Control(String),
    Call(CallError),
    Discovery(DiscoveryError),
    Ownership(OwnershipError),
    Transport(axon_net::Error),
    System(axon::SystemError),
}

impl CallError {
    /// The error of a lookup made for a call: the lookup sends nothing to the actor it looks for, so one that timed
    /// out (`Deadline`) or lost its session or answer (`Closed`) did not send the call, which may be made again.
    pub(crate) fn of_lookup(self) -> Self {
        match self {
            Self::Timeout => Self::NotSent(axon_net::SubmitReason::Deadline),
            Self::OutcomeUnknown | Self::NoReply => Self::NotSent(axon_net::SubmitReason::Closed),
            error => error,
        }
    }

    pub fn retry(&self) -> Retry {
        use axon_net::SubmitReason as Submit;
        match self {
            Self::Busy
            | Self::NotSent(Submit::Full | Submit::Closed | Submit::Deadline)
            | Self::Rejected(Rejection::Full) => Retry::Later,
            Self::NoRoute
            | Self::StaleNode
            | Self::Unreachable(_)
            | Self::Rejected(Rejection::UnknownActor | Rejection::Stopped) => Retry::Resolve,
            Self::Timeout | Self::OutcomeUnknown | Self::NoReply => Retry::IfIdempotent,
            Self::Closed
            | Self::Codec(_)
            | Self::NotSent(Submit::Oversized)
            | Self::Rejected(Rejection::SchemaMismatch | Rejection::Forbidden | Rejection::InvalidMessage) => {
                Retry::Never
            }
        }
    }
}

impl ClusterError {
    pub fn retry(&self) -> Retry {
        use {SpawnRefusal as Refused, axon_net::Error as Net};
        match self {
            Self::Call(e) => e.retry(),
            Self::Spawn(Refused::Held | Refused::Capacity | Refused::Ownership | Refused::Isolated, _)
            | Self::Discovery(DiscoveryError::Unavailable(_) | DiscoveryError::Stale)
            | Self::Ownership(OwnershipError::Held(_) | OwnershipError::Unavailable(_))
            | Self::Transport(
                Net::Auth
                | Net::Negotiation(_)
                | Net::Frame(_)
                | Net::Timeout(_)
                | Net::Io(_)
                | Net::Closed
                | Net::PeerClosed,
            ) => Retry::Later,
            Self::Spawn(Refused::NotOwner | Refused::Leaving | Refused::UnknownKind, _)
            | Self::NotFound
            | Self::SchemaMismatch { .. }
            | Self::NoCandidate
            | Self::Ownership(OwnershipError::Lost) => Retry::Resolve,
            Self::Spawn(Refused::Forbidden | Refused::Failed, _)
            | Self::Transport(Net::Config(_))
            | Self::Config(_)
            | Self::Closed
            | Self::NameTaken
            | Self::Ambiguous(_)
            | Self::ProtocolMismatch
            | Self::Control(_)
            | Self::System(_)
            | Self::Discovery(DiscoveryError::ReadOnly | DiscoveryError::Invalid(_) | DiscoveryError::Capacity) => {
                Retry::Never
            }
        }
    }
}

macro_rules! display_as_debug {
    ($($t:ty),*) => {$(
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{self:?}")
            }
        }
        impl std::error::Error for $t {}
    )*};
}
display_as_debug!(CallError, DiscoveryError, OwnershipError, ClusterError);

macro_rules! from {
    ($($src:ty => $variant:ident),*) => {$(
        impl From<$src> for ClusterError {
            fn from(e: $src) -> Self {
                Self::$variant(e)
            }
        }
    )*};
}
from!(CallError => Call, DiscoveryError => Discovery, OwnershipError => Ownership, axon_net::Error => Transport, axon::SystemError => System);
