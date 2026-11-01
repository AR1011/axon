//! One authenticated, bounded, ordered, framed session between two processes, over TCP or TLS, the
//! [`Network`] in `Config::network`, or any stream handed to [`establish`].
//!
//! No routing, no replay, no retry: the unit is a [`Frame`], and admission only means the
//! local outbox accepted it. Plaintext sessions have no per-frame integrity; use TLS off-box.

#![deny(clippy::unwrap_used, clippy::iter_over_hash_type)]
mod config;
mod error;
pub mod frame;
mod handshake;
mod net;
mod queued;
mod session;
mod tcp;

/// [`Redial`]'s and the cluster's dial backoff, the same as a supervisor's restart backoff.
pub use axon::Backoff;
pub use tokio_rustls::rustls;

/// A session's traffic class, named in its hello: [`Config::lanes`] holds what each runs with.
pub use axon::Lane;
pub use config::{
    Authenticator, CertificateIdentity, Config, Identity, LaneConfig, Lanes, Limits, PeerContext, SharedSecret,
};
pub use error::{Error, FrameError, Negotiation, SubmitError, SubmitReason, Timeout};
pub use frame::{Codecs, Compression, Frame};
pub use handshake::{Role, VERSIONS};
pub use net::{
    AcceptLimits, ClientTls, Dial, DialError, HANDSHAKE_FAILURE_REASONS, Incoming, Io, Listener, ListenerStats,
    ListenerStatsReader, Network, Redial, ServerTls, connect,
};
#[doc(hidden)]
pub use net::{IpSlot, PerIp};
pub use session::{Peer, Received, Receiver, SUBMIT_REASONS, Sender, Session, SessionStats, SessionTimings, establish};
pub use tcp::{HostTcp, TcpHealth, host_tcp};
