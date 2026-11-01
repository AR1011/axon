use std::fmt;
use std::sync::Arc;

use tokio::time::{Instant, timeout_at};

use crate::Frame;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Negotiation {
    Magic,
    /// The two ranges of wire versions do not meet: `(lowest, highest)` this side and the peer speak.
    Version {
        ours: (u16, u16),
        theirs: (u16, u16),
    },
    Oversized,
    Malformed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    Oversized,
    Sequence,
    Codec,
    Reserved,
    Corrupt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Timeout {
    Connect,
    Handshake,
    Write,
    Idle,
    Shutdown,
}

/// Terminal session outcome. Authentication failures carry no detail on purpose.
#[derive(Clone, Debug)]
pub enum Error {
    Config(&'static str),
    Auth,
    Negotiation(Negotiation),
    Frame(FrameError),
    Timeout(Timeout),
    Io(Arc<std::io::Error>),
    Closed,
    PeerClosed,
}

impl Error {
    /// Bounded label set, usable as a telemetry dimension.
    pub fn label(&self) -> &'static str {
        match self {
            Error::Config(_) => "config",
            Error::Auth => "auth",
            Error::Negotiation(Negotiation::Magic) => "negotiation_magic",
            Error::Negotiation(Negotiation::Version { .. }) => "negotiation_version",
            Error::Negotiation(Negotiation::Oversized) => "negotiation_oversized",
            Error::Negotiation(Negotiation::Malformed) => "negotiation_malformed",
            Error::Frame(FrameError::Oversized) => "frame_oversized",
            Error::Frame(FrameError::Sequence) => "frame_sequence",
            Error::Frame(FrameError::Codec) => "frame_codec",
            Error::Frame(FrameError::Reserved) => "frame_reserved",
            Error::Frame(FrameError::Corrupt) => "frame_corrupt",
            Error::Timeout(Timeout::Connect) => "timeout_connect",
            Error::Timeout(Timeout::Handshake) => "timeout_handshake",
            Error::Timeout(Timeout::Write) => "timeout_write",
            Error::Timeout(Timeout::Idle) => "timeout_idle",
            Error::Timeout(Timeout::Shutdown) => "timeout_shutdown",
            Error::Io(_) => "io",
            Error::Closed => "closed",
            Error::PeerClosed => "peer_closed",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(why) => write!(f, "invalid configuration: {why}"),
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Negotiation(Negotiation::Version { ours: (a, b), theirs: (c, d) }) => {
                write!(f, "no common wire version: this node speaks {a}..={b}, the peer {c}..={d}")
            }
            other => f.write_str(other.label()),
        }
    }
}

impl std::error::Error for Error {}

/// `fut`, or `Timeout(kind)` once `deadline` passes.
pub(crate) async fn within<T, E: Into<Error>>(
    deadline: Instant,
    kind: Timeout,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, Error> {
    timeout_at(deadline, fut).await.map_err(|_| Error::Timeout(kind))?.map_err(Into::into)
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(Arc::new(e))
    }
}

impl From<FrameError> for Error {
    fn from(e: FrameError) -> Self {
        Error::Frame(e)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitReason {
    Full,
    Closed,
    Deadline,
    Oversized,
}

/// A definite local rejection: the frame was not admitted and is handed back.
#[derive(Debug)]
pub struct SubmitError {
    pub reason: SubmitReason,
    pub frame: Frame,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "frame not admitted: {:?}", self.reason)
    }
}

impl std::error::Error for SubmitError {}
