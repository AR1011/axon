use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::Semaphore;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

use crate::frame::{Codecs, Compression, HEADER_LEN};
use crate::{Error, Lane, Network};

/// The handshake nonce and incarnation bits: the system's randomness, or the seeded stream where a simulation
/// seeded the thread.
pub(crate) fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0; N];
    if axon::trace::simulated() {
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&axon::trace::random_u64().to_le_bytes()[..chunk.len()]);
        }
    } else {
        SystemRandom::new().fill(&mut bytes).expect("system randomness unavailable");
    }
    bytes
}

/// A node id plus a per-process incarnation, so a restarted node is a different peer.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Identity {
    pub node_id: String,
    pub incarnation: u128,
}

impl Identity {
    /// The incarnation is `unix millis << 64 | random`, so a later boot of a node compares greater.
    pub fn new(node_id: impl Into<String>) -> Self {
        let millis = axon::trace::unix_micros() / 1000;
        Self { node_id: node_id.into(), incarnation: (millis as u128) << 64 | u64::from_be_bytes(random()) as u128 }
    }
}

/// What an authenticator learns about the peer, after the peer's proof verified.
pub struct PeerContext<'a> {
    pub identity: &'a Identity,
    pub namespace: &'a str,
    pub remote: Option<SocketAddr>,
    /// The stream is TLS.
    pub secure: bool,
    /// The peer's verified certificate chain, leaf first.
    pub certificates: &'a [CertificateDer<'static>],
}

/// A `secret` or `authorize` that panics refuses the peer, as a failed authentication, wherever it is asked.
pub trait Authenticator: Send + Sync + 'static {
    /// Key for the mutual challenge–response. It never crosses the wire.
    fn secret(&self) -> &[u8] {
        &[]
    }

    /// Also asked outside any session, with this node's own identity, `remote: None` and its own certificates, when
    /// `Cluster::set_tls` checks that new certificates still pass as this node.
    fn authorize(&self, peer: &PeerContext<'_>) -> bool;
}

/// Proves possession of a cluster-wide secret. It does not bind the claimed node id: any
/// holder can claim any id. Use [`CertificateIdentity`] when that matters.
pub struct SharedSecret(Vec<u8>);

impl SharedSecret {
    pub const MIN_LEN: usize = 16;

    pub fn new(secret: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let secret = secret.into();
        if secret.len() < Self::MIN_LEN {
            return Err(Error::Config("shared secret shorter than 16 bytes"));
        }
        Ok(Self(secret))
    }
}

impl Authenticator for SharedSecret {
    fn secret(&self) -> &[u8] {
        &self.0
    }

    fn authorize(&self, _: &PeerContext<'_>) -> bool {
        true
    }
}

/// mTLS identity binding: the claimed node id must be a name the peer's verified leaf
/// certificate is valid for. Chain trust is the TLS verifier's job.
pub struct CertificateIdentity;

impl Authenticator for CertificateIdentity {
    fn authorize(&self, peer: &PeerContext<'_>) -> bool {
        use tokio_rustls::rustls::{client::verify_server_name, server::ParsedCertificate};
        let (Some(leaf), Ok(name)) = (peer.certificates.first(), ServerName::try_from(peer.identity.node_id.as_str()))
        else {
            return false;
        };
        peer.secure && ParsedCertificate::try_from(leaf).is_ok_and(|cert| verify_server_name(&cert, &name).is_ok())
    }
}

#[derive(Clone, Debug)]
pub struct Limits {
    /// Largest payload, before compression. The peer's value lowers ours for outbound frames.
    pub max_frame: usize,
    pub outbound_frames: usize,
    pub outbound_bytes: usize,
    pub inbound_frames: usize,
    /// Charged per delivered payload and held until the application drops the frame.
    pub inbound_bytes: usize,
    pub write_batch_frames: usize,
    pub write_batch_bytes: usize,
}

/// `Live`'s, and every session's before lanes.
impl Default for Limits {
    fn default() -> Self {
        Self::lane(1 << 20, 8 << 20)
    }
}

impl Limits {
    /// `max_frame`, and `bytes` each way: the outbox and what the application may hold of what came in.
    fn lane(max_frame: usize, bytes: usize) -> Self {
        Self {
            max_frame,
            outbound_frames: 256,
            outbound_bytes: bytes,
            inbound_frames: 256,
            inbound_bytes: bytes,
            write_batch_frames: 64,
            write_batch_bytes: 256 << 10,
        }
    }
}

/// What a session of one [`Lane`] runs with. Both ends read their own: the responder learns the lane from the hello.
#[derive(Clone, Debug)]
pub struct LaneConfig {
    pub limits: Limits,
    /// Preferred outbound codec; falls back to none if the peer does not accept it.
    pub compression: Compression,
    /// A cap on the bytes one session of the lane writes a second, pings and pongs apart: a bucket as deep as one
    /// write batch. `None`: no cap.
    pub max_bytes_per_sec: Option<u64>,
    /// `TCP_NOTSENT_LOWAT` on the lane's sockets, on Linux: the kernel holds at most this much unsent, so the rest
    /// waits in the bounded outbox. `None` leaves the kernel's default.
    pub notsent_lowat: Option<u32>,
}

impl LaneConfig {
    fn new(limits: Limits) -> Self {
        Self { limits, compression: Compression::None, max_bytes_per_sec: None, notsent_lowat: Some(128 << 10) }
    }
}

/// A [`LaneConfig`] for each lane. A peer with every lane up both ways holds at most 168 MiB of frames at the defaults.
#[derive(Clone, Debug)]
pub struct Lanes {
    /// 1 MiB frames, 2 MiB each way.
    pub control: LaneConfig,
    /// 1 MiB frames, 8 MiB each way.
    pub live: LaneConfig,
    /// 8 MiB frames, 32 MiB each way.
    pub bulk: LaneConfig,
}

impl Default for Lanes {
    fn default() -> Self {
        Self {
            control: LaneConfig::new(Limits::lane(1 << 20, 2 << 20)),
            live: LaneConfig::new(Limits::default()),
            bulk: LaneConfig::new(Limits::lane(8 << 20, 32 << 20)),
        }
    }
}

impl Lanes {
    pub fn get(&self, lane: Lane) -> &LaneConfig {
        match lane {
            Lane::Control => &self.control,
            Lane::Live => &self.live,
            Lane::Bulk => &self.bulk,
        }
    }

    pub fn get_mut(&mut self, lane: Lane) -> &mut LaneConfig {
        match lane {
            Lane::Control => &mut self.control,
            Lane::Live => &mut self.live,
            Lane::Bulk => &mut self.bulk,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub identity: Identity,
    /// Sessions only form between equal namespaces.
    pub namespace: String,
    pub auth: Arc<dyn Authenticator>,
    pub allow_plaintext: bool,
    /// Codecs this side will decode.
    pub accept: Codecs,
    pub zstd_level: i32,
    pub compression_threshold: usize,
    pub compression_min_savings: usize,
    pub lanes: Lanes,
    /// A dial's connect, TLS and authentication share it, as an accepted connection's wait for its turn, TLS and
    /// authentication do.
    pub handshake_timeout: Duration,
    /// How long a dial waits for its TCP connect, within `handshake_timeout`. A machine that answers no SYN by then is
    /// taken for gone, as one that refuses it; the handshake after the connect keeps the rest of its budget.
    pub connect_timeout: Duration,
    /// How long one write may make no progress before the session closes, so a slow link that keeps moving stays up.
    /// Over TLS the flush that ends a batch drains up to rustls's 64 KiB buffer within it: at the default 5 s, a TLS
    /// link must move 13 KB/s.
    pub write_timeout: Duration,
    /// Times the first and every Nth frame through queue, encode, write and decode. 0 disables.
    pub timing_sample_every: u32,
    pub keepalive_interval: Duration,
    /// Nothing received for this long closes the session. Pongs count.
    pub idle_timeout: Duration,
    /// Where dials and listeners get their streams. `None` is TCP; a simulator sets its own.
    pub network: Option<Arc<dyn Network>>,
    /// The wire versions this node speaks: [`VERSIONS`](crate::VERSIONS). A test sets another to act as a later
    /// build.
    #[doc(hidden)]
    pub versions: RangeInclusive<u16>,
}

impl Config {
    pub fn new(node_id: impl Into<String>, auth: impl Authenticator) -> Self {
        Self {
            identity: Identity::new(node_id),
            namespace: "default".into(),
            auth: Arc::new(auth),
            allow_plaintext: false,
            accept: Codecs::ALL,
            zstd_level: 1,
            compression_threshold: 512,
            compression_min_savings: 32,
            lanes: Lanes::default(),
            handshake_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            timing_sample_every: 128,
            keepalive_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            network: None,
            versions: crate::VERSIONS,
        }
    }

    /// Refuses a config no session could run with; `secure` says whether the session has TLS.
    pub fn validate(&self, secure: bool) -> Result<(), Error> {
        let check = |ok: bool, why| if ok { Ok(()) } else { Err(Error::Config(why)) };
        check(secure || self.allow_plaintext, "plaintext sessions require allow_plaintext")?;
        let name = |s: &str| axon::valid_text(s, axon::MAX_NAME);
        check(name(&self.identity.node_id), "node id must be 1..=256 bytes without control characters")?;
        check(name(&self.namespace), "namespace must be 1..=256 bytes without control characters")?;
        check(*self.versions.start() >= 2 && !self.versions.is_empty(), "versions must be a range from 2 up")?;
        for lane in Lane::ALL.map(|lane| self.lanes.get(lane)) {
            let l = &lane.limits;
            check((1..=1 << 30).contains(&l.max_frame), "max_frame must be 1..=1GiB")?;
            check(l.outbound_frames > 0 && l.inbound_frames > 0, "frame queues must be non-zero")?;
            check(l.write_batch_frames > 0 && l.write_batch_bytes > 0, "write batch must be non-zero")?;
            let permits = HEADER_LEN + l.max_frame..=Semaphore::MAX_PERMITS.min(u32::MAX as usize);
            check(permits.contains(&l.outbound_bytes), "outbound_bytes must hold one maximal frame")?;
            check(permits.contains(&l.inbound_bytes), "inbound_bytes must hold one maximal frame")?;
            check(lane.max_bytes_per_sec != Some(0), "max_bytes_per_sec must be non-zero")?;
            check(lane.notsent_lowat != Some(0), "notsent_lowat must be non-zero")?;
        }
        let timeouts = [self.handshake_timeout, self.connect_timeout, self.write_timeout, self.keepalive_interval];
        check(timeouts.iter().all(|t| !t.is_zero()), "timeouts must be non-zero")?;
        check(self.connect_timeout <= self.handshake_timeout, "connect_timeout must be within handshake_timeout")?;
        check(self.keepalive_interval < self.idle_timeout, "keepalive_interval must be below idle_timeout")
    }
}

#[cfg(test)]
mod tests {
    use axon::trace::{random_u64, simulate_seed};

    use super::random;

    #[test]
    fn random_draws_on_the_thread_s_seeded_stream_only_while_a_simulation_seeded_it() {
        simulate_seed(Some(7));
        let (one, two) = (random_u64(), random_u64());
        simulate_seed(Some(7));
        assert_eq!((random::<8>(), random_u64()), (one.to_le_bytes(), two));
        simulate_seed(Some(7));
        simulate_seed(None);
        let bytes = random::<8>();
        assert_eq!(random_u64(), one, "unseeded, random took nothing from the thread's stream: it is ring");
        assert_ne!(bytes, one.to_le_bytes());
    }
}
