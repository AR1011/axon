use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::{Instant, timeout_at};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::error::within;
use crate::handshake::{Link, Role};
use crate::session::{Halves, establish_with, shared_halves, tcp_halves};
use crate::{Backoff, Config, Error, Lane, Negotiation, Session, Timeout};

pub trait Io: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {}
pub type Incoming = mpsc::Receiver<io::Result<(Box<dyn Io>, SocketAddr)>>;
/// Named because clippy's type_complexity refuses the type spelled out, in the trait and in every impl.
pub type Dial = Pin<Box<dyn Future<Output = io::Result<Box<dyn Io>>> + Send>>;

/// Where sessions get their bytes. `Config::network: None` is TCP.
pub trait Network: Send + Sync + 'static {
    /// A stream to `to` for a session of `lane`, or what a TCP dial gives: refused, reset, or no answer before the
    /// caller's deadline. TCP dials a connection for each; another transport may carry the lanes on one.
    fn connect(&self, to: SocketAddr, lane: Lane) -> Dial;
    /// Accepts on `at` until the receiver is dropped, which frees the address. An Err is an accept error (EMFILE).
    fn bind(&self, at: SocketAddr) -> io::Result<(SocketAddr, Incoming)>;
}

#[derive(Clone)]
pub struct ClientTls {
    pub name: ServerName<'static>,
    pub config: Arc<ClientConfig>,
}

#[cfg(unix)]
fn fd(stream: &TcpStream) -> Option<i32> {
    Some(std::os::fd::AsRawFd::as_raw_fd(stream))
}

#[cfg(not(unix))]
fn fd(_: &TcpStream) -> Option<i32> {
    None
}

/// Dials a session of `lane`, optionally wraps it in TLS, and authenticates, all within one `handshake_timeout`.
pub async fn connect(addr: SocketAddr, lane: Lane, tls: Option<&ClientTls>, cfg: &Config) -> Result<Session, Error> {
    cfg.validate(tls.is_some())?;
    let now = Instant::now();
    let deadline = now + cfg.handshake_timeout;
    let connected_by = deadline.min(now + cfg.connect_timeout);
    let role = Role::Initiator(lane);
    let link = Link { role, secure: tls.is_some(), certificates: Vec::new(), remote: Some(addr), fd: None };
    let Some(network) = &cfg.network else {
        let stream = within(connected_by, Timeout::Connect, TcpStream::connect(addr)).await?;
        stream.set_nodelay(true)?;
        let initiated = initiate(Link { fd: fd(&stream), ..link }, stream, tls, cfg, tcp_halves);
        return within(deadline, Timeout::Handshake, initiated).await;
    };
    let stream = within(connected_by, Timeout::Connect, network.connect(addr, lane)).await?;
    within(deadline, Timeout::Handshake, initiate(link, stream, tls, cfg, shared_halves)).await
}

async fn initiate<S: Io>(
    mut link: Link,
    stream: S,
    tls: Option<&ClientTls>,
    cfg: &Config,
    plain: fn(S) -> Halves,
) -> Result<Session, Error> {
    let Some(tls) = tls else { return establish_with(stream, link, None, cfg, plain).await };
    let stream = TlsConnector::from(tls.config.clone()).connect(tls.name.clone(), stream).await?;
    link.certificates = stream.get_ref().1.peer_certificates().unwrap_or_default().to_vec();
    establish_with(stream, link, None, cfg, shared_halves).await
}

#[derive(Clone, Debug)]
pub struct AcceptLimits {
    /// Live sessions plus handshakes. At the cap the listener stops accepting.
    pub max_connections: usize,
    /// Handshakes at a time. The others wait their turn within `handshake_timeout`.
    pub max_handshakes: usize,
    /// Handshakes, running or waiting, from one address. More are turned away.
    pub max_handshakes_per_ip: usize,
}

impl Default for AcceptLimits {
    fn default() -> Self {
        // A peer opens a session on each of the three lanes.
        Self { max_connections: 3072, max_handshakes: 64, max_handshakes_per_ip: 16 }
    }
}

#[derive(Default)]
struct AcceptCounters {
    accepted: AtomicU64,
    accept_errors: AtomicU64,
    rejected_busy: AtomicU64,
    rejected_per_ip: AtomicU64,
    handshake_failures: [AtomicU64; 5],
}

/// `version`: the peer speaks no wire version this node does.
pub const HANDSHAKE_FAILURE_REASONS: [&str; 5] = ["auth", "negotiation", "timeout", "other", "version"];

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ListenerStats {
    pub accepted: u64,
    pub accept_errors: u64,
    pub rejected_busy: u64,
    pub rejected_per_ip: u64,
    /// Indexed like `HANDSHAKE_FAILURE_REASONS`.
    pub handshake_failures: [u64; 5],
}

pub struct Listener {
    local: SocketAddr,
    sessions: mpsc::Receiver<Session>,
    counters: Arc<AcceptCounters>,
    tls: Option<ServerTls>,
    task: tokio::task::JoinHandle<()>,
}

/// The certificates a listener serves. Handshakes that start after `set` use the new ones; sessions already up keep
/// theirs until they end.
#[derive(Clone)]
pub struct ServerTls(Arc<Mutex<Arc<ServerConfig>>>);

impl ServerTls {
    pub fn set(&self, config: Arc<ServerConfig>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = config;
    }

    fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.0.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }
}

/// How many places each address holds: the listener's handshakes here, and the console's connections in `axon-obs`.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct PerIp(Arc<Mutex<HashMap<IpAddr, usize>>>);

/// One of an address's places, given back when dropped.
#[doc(hidden)]
pub struct IpSlot(PerIp, IpAddr);

impl PerIp {
    /// `None` when `ip` already holds `max`.
    pub fn take(&self, ip: IpAddr, max: usize) -> Option<IpSlot> {
        let mut counts = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let n = counts.entry(ip).or_default();
        (*n < max).then(|| {
            *n += 1;
            IpSlot(self.clone(), ip)
        })
    }
}

impl Drop for IpSlot {
    fn drop(&mut self) {
        let mut counts = self.0.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = counts.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                counts.remove(&self.1);
            }
        }
    }
}

impl Listener {
    pub async fn bind(
        addr: SocketAddr,
        tls: Option<Arc<ServerConfig>>,
        cfg: Config,
        limits: AcceptLimits,
    ) -> Result<Self, Error> {
        cfg.validate(tls.is_some())?;
        let (local, source) = match &cfg.network {
            None => {
                let listener = TcpListener::bind(addr).await?;
                (listener.local_addr()?, Source::Tcp(listener))
            }
            Some(network) => {
                let (local, incoming) = network.bind(addr)?;
                (local, Source::Network(incoming))
            }
        };
        let (tx, sessions) = mpsc::channel(limits.max_handshakes.max(1));
        let counters = Arc::new(AcceptCounters::default());
        let tls = tls.map(|config| ServerTls(Arc::new(Mutex::new(config))));
        let task = tokio::spawn(accept_loop(source, tls.clone(), cfg, limits, tx, counters.clone()));
        Ok(Self { local, sessions, counters, tls, task })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// Outlives the listener, like `stats_reader`. None for a plaintext one.
    pub fn tls(&self) -> Option<ServerTls> {
        self.tls.clone()
    }

    /// The next authenticated session.
    pub async fn accept(&mut self) -> Option<Session> {
        self.sessions.recv().await
    }

    pub fn stats(&self) -> ListenerStats {
        self.stats_reader().get()
    }

    /// Outlives the listener, for whoever reports on it after handing it to an accept task.
    pub fn stats_reader(&self) -> ListenerStatsReader {
        ListenerStatsReader(self.counters.clone())
    }

    /// Stops accepting and returns once the port is free to bind again.
    pub async fn close(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

#[derive(Clone)]
pub struct ListenerStatsReader(Arc<AcceptCounters>);

impl ListenerStatsReader {
    pub fn get(&self) -> ListenerStats {
        let c = &self.0;
        ListenerStats {
            accepted: c.accepted.load(Relaxed),
            accept_errors: c.accept_errors.load(Relaxed),
            rejected_busy: c.rejected_busy.load(Relaxed),
            rejected_per_ip: c.rejected_per_ip.load(Relaxed),
            handshake_failures: std::array::from_fn(|i| c.handshake_failures[i].load(Relaxed)),
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Source {
    Tcp(TcpListener),
    Network(Incoming),
}

enum Accepted {
    Tcp(TcpStream),
    Io(Box<dyn Io>),
}

async fn accept_loop(
    mut source: Source,
    tls: Option<ServerTls>,
    cfg: Config,
    limits: AcceptLimits,
    sessions: mpsc::Sender<Session>,
    counters: Arc<AcceptCounters>,
) {
    let connections = Arc::new(Semaphore::new(limits.max_connections));
    let handshakes = Arc::new(Semaphore::new(limits.max_handshakes));
    let per_ip = PerIp::default();
    let cfg = Arc::new(cfg);
    loop {
        let Ok(connection) = connections.clone().acquire_owned().await else { return };
        let accepted = match &mut source {
            Source::Tcp(listener) => listener.accept().await.map(|(stream, at)| (Accepted::Tcp(stream), at)),
            Source::Network(incoming) => match incoming.recv().await {
                Some(accepted) => accepted.map(|(stream, at)| (Accepted::Io(stream), at)),
                None => return,
            },
        };
        let (stream, remote) = match accepted {
            Ok(accepted) => accepted,
            Err(_) => {
                // EMFILE and friends are transient; the loop must outlive them.
                counters.accept_errors.fetch_add(1, Relaxed);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Some(ip_slot) = per_ip.take(remote.ip(), limits.max_handshakes_per_ip) else {
            counters.rejected_per_ip.fetch_add(1, Relaxed);
            continue;
        };
        let tls = tls.as_ref().map(ServerTls::acceptor);
        let (cfg, sessions, counters) = (cfg.clone(), sessions.clone(), counters.clone());
        let handshakes = handshakes.clone();
        tokio::spawn(async move {
            // The wait for a permit comes out of the same budget, so the exchange does not get a fresh one.
            let deadline = Instant::now() + cfg.handshake_timeout;
            let Ok(Ok(handshake)) = timeout_at(deadline, handshakes.acquire_owned()).await else {
                counters.rejected_busy.fetch_add(1, Relaxed);
                return;
            };
            let link = Link {
                role: Role::Responder,
                secure: tls.is_some(),
                certificates: Vec::new(),
                remote: Some(remote),
                fd: None,
            };
            let established = async {
                match stream {
                    Accepted::Tcp(stream) => {
                        stream.set_nodelay(true)?;
                        let link = Link { fd: fd(&stream), ..link };
                        respond(link, stream, tls, connection, &cfg, tcp_halves).await
                    }
                    Accepted::Io(stream) => respond(link, stream, tls, connection, &cfg, shared_halves).await,
                }
            };
            let established = within(deadline, Timeout::Handshake, established).await;
            // Counted before the slots free, so whoever takes one next sees the count.
            match established {
                Ok(session) => {
                    counters.accepted.fetch_add(1, Relaxed);
                    drop((handshake, ip_slot));
                    drop(sessions.send(session).await);
                }
                Err(error) => {
                    let reason = match error {
                        Error::Auth => 0,
                        Error::Negotiation(Negotiation::Version { .. }) => 4,
                        Error::Negotiation(_) => 1,
                        Error::Timeout(_) => 2,
                        _ => 3,
                    };
                    counters.handshake_failures[reason].fetch_add(1, Relaxed);
                }
            }
        });
    }
}

async fn respond<S: Io>(
    mut link: Link,
    stream: S,
    tls: Option<TlsAcceptor>,
    connection: OwnedSemaphorePermit,
    cfg: &Config,
    plain: fn(S) -> Halves,
) -> Result<Session, Error> {
    let Some(tls) = tls else { return establish_with(stream, link, Some(connection), cfg, plain).await };
    let stream = tls.accept(stream).await?;
    link.certificates = stream.get_ref().1.peer_certificates().unwrap_or_default().to_vec();
    establish_with(stream, link, Some(connection), cfg, shared_halves).await
}

#[derive(Debug)]
pub enum DialError {
    /// The circuit is open: no attempt was made.
    Backoff {
        retry_in: Duration,
    },
    Failed(Error),
}

/// Reconnect policy for one peer's lane: jittered exponential backoff that doubles as a circuit
/// breaker. While a delay is pending, dials fail fast; after it, one probe goes through.
pub struct Redial {
    pub addr: SocketAddr,
    pub lane: Lane,
    pub tls: Option<ClientTls>,
    pub cfg: Config,
    pub backoff: Backoff,
    failures: u32,
    next_attempt: Instant,
}

impl Redial {
    pub fn new(addr: SocketAddr, lane: Lane, tls: Option<ClientTls>, cfg: Config, backoff: Backoff) -> Self {
        Self { addr, lane, tls, cfg, backoff, failures: 0, next_attempt: Instant::now() }
    }

    pub fn failures(&self) -> u32 {
        self.failures
    }

    pub async fn connect(&mut self) -> Result<Session, DialError> {
        let retry_in = self.next_attempt.saturating_duration_since(Instant::now());
        if !retry_in.is_zero() {
            return Err(DialError::Backoff { retry_in });
        }
        match connect(self.addr, self.lane, self.tls.as_ref(), &self.cfg).await {
            Ok(session) => {
                self.failures = 0;
                Ok(session)
            }
            Err(e) => {
                self.failures = self.failures.saturating_add(1);
                self.next_attempt = Instant::now() + self.backoff.delay(self.failures);
                Err(DialError::Failed(e))
            }
        }
    }
}
