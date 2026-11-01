use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axon::{ActorSystem, ExtensionFuture, SystemExtension, WeakActorSystem};
use axon_cluster::{Cluster, WeakCluster};
use axon_net::PerIp;
use axum::extract::{ConnectInfo, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use axum::serve::Listener;
use axum::{Extension, Router};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::time::Sleep;

use crate::Telemetry;

/// Connections a port holds at once; past them a connection is closed as it is accepted.
const CONNECTIONS: usize = 256;
/// Of those, one address holds this many, so one client cannot take them all; a trusted proxy has no share.
const CONNECTIONS_PER_CLIENT: usize = 64;
/// A request head and a sign-in's body must arrive within this, an idle connection waits no longer for its next
/// request, and an answer the client stops reading waits no longer for it to go on.
pub(crate) const DEADLINE: Duration = Duration::from_secs(10);
/// A connection buffers no more than this, so a port holds at most 4 MiB of heads; a longer head gets 431.
const HEAD_BYTES: usize = 16 * 1024;

/// Weak, so that a probe never keeps a dropped system alive: it reports it gone instead.
#[derive(Clone)]
struct Probe {
    system: WeakActorSystem,
    cluster: WeakCluster,
}

/// `/healthz` and `/readyz`. They take no credentials: the dashboard merges them outside its `authenticate` layer.
pub(crate) fn routes<S: Clone + Send + Sync + 'static>(system: &ActorSystem, cluster: &Cluster) -> Router<S> {
    let probe = Probe { system: system.downgrade(), cluster: cluster.downgrade() };
    Router::new().route("/healthz", get(live)).route("/readyz", get(ready)).with_state(probe)
}

/// 200 when every check is `ok`; any other value says what is wrong.
fn answer(checks: Vec<(&str, &str)>) -> Response {
    let ok = checks.iter().all(|(_, v)| *v == "ok");
    let status = if ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let checks: serde_json::Map<_, _> = checks.into_iter().map(|(k, v)| (k.to_owned(), v.into())).collect();
    let body = serde_json::json!({ "ok": ok, "checks": checks });
    (status, Json(body)).into_response()
}

/// Answering at all is the check: the port closes with the system, so a dropped one refuses the connection.
async fn live() -> Response {
    answer(vec![("system", "ok")])
}

async fn ready(State(probe): State<Probe>) -> Response {
    let system = match probe.system.upgrade() {
        None => "stopped",
        Some(system) if system.leave_reason().is_some() => "shutting down",
        Some(_) => "ok",
    };
    let (cluster, ownership, quotas) = match probe.cluster.upgrade() {
        None => ("stopped", "stopped", "stopped"),
        Some(c) => (
            if c.isolated() { "isolated" } else { "ok" },
            if c.warming() { "warming" } else { "ok" },
            // A replica that has not pulled the others' rows yet: a rolling restart waits for it.
            if c.quotas_starting() { "starting" } else { "ok" },
        ),
    };
    answer(vec![("system", system), ("cluster", cluster), ("ownership", ownership), ("quotas", quotas)])
}

struct Server {
    address: SocketAddr,
    stop: Notify,
    metrics: Mutex<Option<Telemetry>>,
    serving: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

async fn metrics(State(server): State<Arc<Server>>) -> Response {
    let Some(telemetry) = server.metrics.lock().clone() else { return StatusCode::NOT_FOUND.into_response() };
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], telemetry.exposition()).into_response()
}

impl SystemExtension for Server {
    fn name(&self) -> &str {
        "health"
    }

    /// Returns once the port is free to bind again: the serving task holds the listener until it ends.
    fn shutdown(&self, _budget: Duration) -> ExtensionFuture {
        self.close();
        let serving = self.serving.lock().take();
        Box::pin(async move {
            if let Some(serving) = serving {
                let _ = serving.await;
            }
            Ok(())
        })
    }

    fn close(&self) {
        self.stop.notify_one();
    }
}

/// The probes on a port of their own, for a node without a `Dashboard`, and `/metrics` for Prometheus on any node once
/// `serve_metrics` is called. It stops with the system, after the drain, so a draining node answers `/readyz` with 503
/// until then; once `shutdown` returns, the port is free to bind again. A reference; dropping it stops nothing.
#[derive(Clone)]
pub struct Health(Arc<Server>);

impl Health {
    pub async fn start(system: &ActorSystem, cluster: &Cluster, bind: SocketAddr) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind(bind).await.map_err(|e| format!("bind {bind}: {e}"))?;
        let address = listener.local_addr().map_err(|e| e.to_string())?;
        let server =
            Arc::new(Server { address, stop: Notify::new(), metrics: Mutex::default(), serving: Mutex::default() });
        system.attach_extension(server.clone()).map_err(|e| e.to_string())?;
        let metrics = Router::new().route("/metrics", get(metrics)).with_state(server.clone());
        let (app, stopping) = (routes::<()>(system, cluster).merge(metrics), server.clone());
        *server.serving.lock() =
            Some(tokio::spawn(serve(listener, app, async move { stopping.stop.notified().await }, Vec::new())));
        Ok(Self(server))
    }

    pub fn address(&self) -> SocketAddr {
        self.0.address
    }

    /// From now on `/metrics` answers with `telemetry`'s last sample in the Prometheus text format; until then, 404.
    pub fn serve_metrics(&self, telemetry: &Telemetry) {
        *self.0.metrics.lock() = Some(telemetry.clone());
    }
}

/// Serves `app` until `stop`, then closes the port and lets each open connection finish what it is answering.
/// `axum::serve` caps no connections and times neither heads nor writes, so a client that opens many, trickles its
/// head or never reads its answer would hold descriptors and memory without end.
pub(crate) async fn serve(
    mut listener: impl Listener<Addr = SocketAddr>,
    app: Router,
    stop: impl Future<Output = ()>,
    proxies: Vec<IpAddr>,
) {
    let (places, clients) = (Arc::new(Semaphore::new(CONNECTIONS)), PerIp::default());
    let (_stopping, stopped) = watch::channel(());
    let mut http = Builder::new(TokioExecutor::new()).http1_only();
    http.http1().timer(TokioTimer::new()).header_read_timeout(DEADLINE).max_buf_size(HEAD_BYTES);
    let mut stop = pin!(stop);
    loop {
        let (io, peer) = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut stop => return,
        };
        let client = peer.ip().to_canonical();
        let share = if proxies.contains(&client) { usize::MAX } else { CONNECTIONS_PER_CLIENT };
        let (Ok(place), Some(held)) = (places.clone().try_acquire_owned(), clients.take(client, share)) else {
            continue;
        };
        let service = TowerToHyperService::new(app.clone().layer(Extension(ConnectInfo(peer))));
        let (http, mut stopped) = (http.clone(), stopped.clone());
        tokio::spawn(async move {
            let mut connection = pin!(http.serve_connection(TokioIo::new(Stalls { io, stalled: None }), service));
            // Errs once `serve` returns and drops the sender.
            if tokio::select! { _ = connection.as_mut() => false, _ = stopped.changed() => true } {
                connection.as_mut().graceful_shutdown();
                let _ = connection.await;
            }
            drop((place, held));
        });
    }
}

/// Fails a connection whose writes make no progress for `DEADLINE`: hyper times only heads, so a client that asks
/// and never reads would otherwise hold its place for ever.
struct Stalls<T> {
    io: T,
    stalled: Option<Pin<Box<Sleep>>>,
}

impl<T: Unpin> Stalls<T> {
    fn progress<R>(
        &mut self,
        cx: &mut Context<'_>,
        write: impl FnOnce(Pin<&mut T>, &mut Context<'_>) -> Poll<io::Result<R>>,
    ) -> Poll<io::Result<R>> {
        let polled = write(Pin::new(&mut self.io), cx);
        if polled.is_ready() {
            self.stalled = None;
            return polled;
        }
        let stalled = self.stalled.get_or_insert_with(|| Box::pin(tokio::time::sleep(DEADLINE)));
        stalled.as_mut().poll(cx).map(|()| Err(io::ErrorKind::TimedOut.into()))
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Stalls<T> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Stalls<T> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.progress(cx, |io, cx| io.poll_write(cx, buf))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.progress(cx, |io, cx| io.poll_write_vectored(cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.progress(cx, AsyncWrite::poll_flush)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.progress(cx, AsyncWrite::poll_shutdown)
    }
}
