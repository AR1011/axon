//! Consul as a discovery provider (service + TTL check per node) and as an ownership
//! authority (one session per node, one KV lock per key, `LockIndex` as the fencing epoch).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use parking_lot::Mutex;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::discovery::{BoxFuture, Discovery, NodeRecord, ProviderInfo, Snapshot};
use crate::ownership::{Holder, OwnershipAuthority};
use crate::{Cluster, ClusterError, DiscoveryError, OwnershipError};

const NODE_ID: &str = "axon_node_id";
const INCARNATION: &str = "axon_incarnation";

#[derive(Clone, Debug)]
pub struct ConsulConfig {
    /// `http(s)://host:port` of the local agent.
    pub base_url: String,
    /// Service name for discovery, KV prefix for leases. `[A-Za-z0-9_-]`, at most 128 bytes.
    pub service: String,
    pub token: Option<String>,
    /// Health-check TTL (discovery) and session TTL (leases, where Consul enforces 10 s..=24 h).
    /// Keep `ClusterConfig::heartbeat_grace` well inside it.
    pub ttl: Duration,
    /// How long a failed node lingers as critical before Consul forgets it.
    pub deregister_after: Duration,
    /// How long Consul withholds a lock after its holder's session died.
    pub lock_delay: Duration,
    pub timeout: Duration,
    /// How long one blocking query for a membership change is left open.
    pub watch_wait: Duration,
    pub max_response_bytes: usize,
    /// Requests in flight at once; the rest wait their turn. An agent refuses a client that holds more than
    /// its `http_max_conns_per_client` (200) connections.
    pub max_in_flight: usize,
}

impl ConsulConfig {
    pub fn new(base_url: impl Into<String>, service: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            service: service.into(),
            token: None,
            ttl: Duration::from_secs(10),
            deregister_after: Duration::from_secs(60),
            lock_delay: Duration::from_secs(5),
            timeout: Duration::from_secs(5),
            watch_wait: Duration::from_secs(30),
            max_response_bytes: 4 << 20,
            max_in_flight: 64,
        }
    }
}

struct Client {
    http: reqwest::Client,
    cfg: ConsulConfig,
    in_flight: tokio::sync::Semaphore,
    requests: AtomicU64,
    failures: AtomicU64,
    /// The newest `X-Consul-Index` seen.
    index: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Client {
    fn new(mut cfg: ConsulConfig) -> Result<Self, &'static str> {
        let url = reqwest::Url::parse(&cfg.base_url).map_err(|_| "consul base url")?;
        let plain = matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none();
        let name = |s: &str| {
            (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        };
        if !plain || !name(&cfg.service) || cfg.ttl < Duration::from_secs(1) {
            return Err("consul base url, service name or ttl");
        }
        cfg.base_url = cfg.base_url.trim_end_matches('/').to_owned();
        let http = reqwest::Client::builder().timeout(cfg.timeout).redirect(reqwest::redirect::Policy::none()).build();
        let in_flight = tokio::sync::Semaphore::new(cfg.max_in_flight.max(1));
        Ok(Self {
            http: http.map_err(|_| "http client")?,
            cfg,
            in_flight,
            requests: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            index: AtomicU64::new(0),
            last_error: Mutex::new(None),
        })
    }

    fn describe(&self, kind: &str) -> ProviderInfo {
        let cfg = &self.cfg;
        ProviderInfo::new(kind)
            .setting("agent", &cfg.base_url)
            .setting("service", &cfg.service)
            .setting("token", if cfg.token.is_some() { "set" } else { "none" })
            .setting("ttl", format!("{:?}", cfg.ttl))
            .stat("requests", self.requests.load(Relaxed))
            .stat("failures", self.failures.load(Relaxed))
            .stat("index", self.index.load(Relaxed))
            .stat("last error", self.last_error.lock().as_deref().unwrap_or("none"))
    }

    /// Status, `X-Consul-Index`, and a body read against the size cap.
    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, u64, Vec<u8>), String> {
        self.send_within(method, path, body, self.cfg.timeout).await
    }

    async fn send_within(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        limit: Duration,
    ) -> Result<(StatusCode, u64, Vec<u8>), String> {
        let result = self.exchange(method, path, body, limit).await;
        self.requests.fetch_add(1, Relaxed);
        match &result {
            Ok((status, index, _)) if status.is_success() => drop(self.index.fetch_max(*index, Relaxed)),
            // A transaction whose check found the key moved on: it did what it should.
            Ok((StatusCode::CONFLICT, ..)) => {}
            Ok((status, ..)) => self.failed(format!("consul answered {status}")),
            Err(error) => self.failed(error.clone()),
        }
        result
    }

    fn failed(&self, error: String) {
        self.failures.fetch_add(1, Relaxed);
        *self.last_error.lock() = Some(error);
    }

    async fn exchange(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        limit: Duration,
    ) -> Result<(StatusCode, u64, Vec<u8>), String> {
        // A blocking query is one long-lived connection; it must not keep a heartbeat waiting for a turn.
        let _turn = match limit > self.cfg.timeout {
            true => None,
            false => Some(
                tokio::time::timeout(limit, self.in_flight.acquire())
                    .await
                    .map_err(|_| "timed out waiting for a request slot")?
                    .map_err(|_| "client closed")?,
            ),
        };
        let mut request = self.http.request(method, format!("{}{path}", self.cfg.base_url)).timeout(limit);
        if let Some(token) = &self.cfg.token {
            request = request.header("X-Consul-Token", token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let mut response = request.send().await.map_err(|e| e.without_url().to_string())?;
        let index = response.headers().get("X-Consul-Index").and_then(|v| v.to_str().ok()?.parse().ok()).unwrap_or(0);
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.without_url().to_string())? {
            if bytes.len() + chunk.len() > self.cfg.max_response_bytes {
                return Err("response too large".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok((response.status(), index, bytes))
    }

    async fn expect_ok(&self, method: Method, path: &str, body: Option<Value>) -> Result<Vec<u8>, String> {
        match self.send(method, path, body).await? {
            (status, _, bytes) if status.is_success() => Ok(bytes),
            (status, ..) => Err(format!("consul answered {status}")),
        }
    }

    /// The KV entries at `path`: none where Consul has no such key.
    async fn get_entries(&self, path: &str) -> Result<Vec<Value>, String> {
        match self.send(Method::GET, path, None).await? {
            (StatusCode::NOT_FOUND, ..) => Ok(Vec::new()),
            (status, _, body) if status.is_success() => {
                serde_json::from_slice(&body).map_err(|_| "undecodable answer from consul".into())
            }
            (status, ..) => Err(format!("consul answered {status}")),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<String> {
    let bytes = (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok());
    String::from_utf8(bytes.collect::<Option<Vec<u8>>>()?).ok()
}

fn base64(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let quad = |c: &[u8]| {
        let n = c.iter().enumerate().fold(0u32, |n, (i, b)| n | (*b as u32) << (16 - 8 * i));
        let len = c.len();
        (0..4).map(move |i| if i <= len { DIGITS[((n >> (18 - 6 * i)) & 63) as usize] as char } else { '=' })
    };
    bytes.chunks(3).flat_map(quad).collect()
}

fn seconds(d: Duration) -> String {
    format!("{}s", d.as_secs().max(1))
}

pub struct ConsulDiscovery(Client);

impl ConsulDiscovery {
    pub fn new(cfg: ConsulConfig) -> Result<Self, DiscoveryError> {
        Client::new(cfg).map(Self).map_err(DiscoveryError::Invalid)
    }

    fn service_id(&self, incarnation: u128) -> String {
        format!("{}:{incarnation:032x}", self.0.cfg.service)
    }

    /// Metadata keys are hex-encoded: Consul's key alphabet has no `.`.
    fn parse(entry: &Value) -> Option<NodeRecord> {
        let service = entry.get("Service")?;
        let meta = service.get("Meta")?.as_object()?;
        let text = |key: &str| meta.get(key)?.as_str();
        let address = service.get("Address")?.as_str()?.parse().ok()?;
        let port = u16::try_from(service.get("Port")?.as_u64()?).ok()?;
        let checks = entry.get("Checks").and_then(Value::as_array).into_iter().flatten();
        let owned = checks.filter(|c| c.get("ServiceID").and_then(Value::as_str).is_some_and(|id| !id.is_empty()));
        let indexes = owned.chain([service]).filter_map(|v| v.get("ModifyIndex")?.as_u64());
        let version = indexes.max().unwrap_or(0);
        let metadata = meta.iter().filter_map(|(k, v)| Some((unhex(k.strip_prefix("m_")?)?, v.as_str()?.to_owned())));
        Some(NodeRecord {
            id: text(NODE_ID)?.to_owned(),
            incarnation: u128::from_str_radix(text(INCARNATION)?, 16).ok()?,
            address: std::net::SocketAddr::new(address, port),
            metadata: metadata.collect(),
            version,
        })
    }

    async fn put(&self, path: &str, body: Option<Value>) -> Result<(), DiscoveryError> {
        self.0.expect_ok(Method::PUT, path, body).await.map(drop).map_err(DiscoveryError::Unavailable)
    }

    /// The passing members, held open by Consul until its index passes `after` or `wait` runs out.
    async fn members(&self, after: u64, wait: Duration) -> Result<Snapshot, DiscoveryError> {
        let cfg = &self.0.cfg;
        let path = match after {
            0 => format!("/v1/health/service/{}?passing=true", cfg.service),
            _ => format!("/v1/health/service/{}?passing=true&index={after}&wait={}", cfg.service, seconds(wait)),
        };
        // Consul adds up to a sixteenth of the wait as jitter.
        let limit = cfg.timeout + if after == 0 { Duration::ZERO } else { wait + wait / 8 };
        let (status, revision, body) =
            self.0.send_within(Method::GET, &path, None, limit).await.map_err(DiscoveryError::Unavailable)?;
        if !status.is_success() {
            return Err(DiscoveryError::Unavailable(format!("consul answered {status}")));
        }
        let entries: Vec<Value> =
            serde_json::from_slice(&body).map_err(|e| DiscoveryError::Unavailable(e.to_string()))?;
        // A restarted node can overlap its previous boot until that check expires: newest wins.
        let mut newest = BTreeMap::<String, NodeRecord>::new();
        for record in entries.iter().filter_map(Self::parse) {
            match newest.get(&record.id) {
                Some(current) if current.incarnation >= record.incarnation => {}
                _ => drop(newest.insert(record.id.clone(), record)),
            }
        }
        Ok(Snapshot { revision, nodes: newest.into_values().collect() })
    }
}

impl Discovery for ConsulDiscovery {
    fn snapshot(&self) -> BoxFuture<'_, Result<Snapshot, DiscoveryError>> {
        Box::pin(self.members(0, Duration::ZERO))
    }

    fn watch(&self, after: u64) -> Option<BoxFuture<'_, Result<Snapshot, DiscoveryError>>> {
        Some(Box::pin(self.members(after, self.0.cfg.watch_wait)))
    }

    fn register(&self, record: &NodeRecord) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let mut meta: BTreeMap<String, String> =
            record.metadata.iter().map(|(k, v)| (format!("m_{}", hex(k.as_bytes())), v.clone())).collect();
        meta.insert(NODE_ID.into(), record.id.clone());
        meta.insert(INCARNATION.into(), format!("{:032x}", record.incarnation));
        let cfg = &self.0.cfg;
        let body = json!({
            "ID": self.service_id(record.incarnation),
            "Name": cfg.service,
            "Address": record.address.ip().to_string(),
            "Port": record.address.port(),
            "Meta": meta,
            "Check": {
                "TTL": seconds(cfg.ttl),
                "Status": "passing",
                "DeregisterCriticalServiceAfter": seconds(cfg.deregister_after),
            },
        });
        let valid = record.validate();
        Box::pin(async move {
            valid?;
            self.put("/v1/agent/service/register", Some(body)).await
        })
    }

    fn unregister(&self, _id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let path = format!("/v1/agent/service/deregister/{}", self.service_id(incarnation));
        Box::pin(async move { self.put(&path, None).await })
    }

    fn heartbeat(&self, _id: &str, incarnation: u128) -> BoxFuture<'_, Result<(), DiscoveryError>> {
        let path = format!("/v1/agent/check/pass/service:{}", self.service_id(incarnation));
        Box::pin(async move { self.put(&path, None).await })
    }

    fn describe(&self) -> ProviderInfo {
        let cfg = &self.0.cfg;
        self.0
            .describe("consul")
            .setting("deregister after", format!("{:?}", cfg.deregister_after))
            .setting("watch wait", format!("{:?}", cfg.watch_wait))
    }
}

/// Leases for one node. Locks are released, never deleted, so a key's `LockIndex` only grows.
pub struct ConsulLeases {
    client: Client,
    holder: String,
    session: Mutex<Option<String>>,
    /// Callers that found no session wait for the one being created: a session each would leave all but the
    /// renewed one to lapse, and Consul would release the keys taken under them while their actors run.
    creating: tokio::sync::Mutex<()>,
    held: Mutex<Held>,
    /// Written as each acquire's flags, so that taking a key again under the same session moves its `ModifyIndex`
    /// and each lock names the acquire that took it; also stamps the keys left locked.
    acquires: AtomicU64,
    /// Whether the holder written into each key is the cluster's node id, which `held_by` looks for.
    named: AtomicBool,
}

impl ConsulLeases {
    /// `node_id` is written into each key it holds; it must be the cluster's node id for an eager kind, whose
    /// survivors look for it.
    pub fn new(cfg: ConsulConfig, node_id: impl Into<String>) -> Result<Self, OwnershipError> {
        let client = Client::new(cfg).map_err(|e| OwnershipError::Unavailable(e.into()))?;
        let (session, creating, held) = (Mutex::new(None), tokio::sync::Mutex::new(()), Mutex::default());
        let (named, acquires) = (AtomicBool::new(true), AtomicU64::new(0));
        Ok(Self { client, holder: node_id.into(), session, creating, held, acquires, named })
    }

    fn left_locked(&self, key: String) {
        let stamp = self.acquires.fetch_add(1, Relaxed) + 1;
        self.held.lock().loose.insert(key, stamp);
    }

    /// Unlocks `key` if the lock on it is this node's and names an acquire that neither holds it nor is under way,
    /// and then forgets it; a key held elsewhere, or not at all, is forgotten too. The unlock is checked against the
    /// `ModifyIndex` read, which an acquire moves, so it never unlocks a lock taken since.
    async fn unlock(&self, key: &str) -> Result<(), OwnershipError> {
        let Some(stamp) = self.held.lock().loose.get(key).copied() else { return Ok(()) };
        let path = self.key_path(key);
        let entry = self.client.get_entries(&path).await.map_err(OwnershipError::Unavailable)?.into_iter().next();
        let field = |name| entry.as_ref()?.get(name)?.as_u64();
        let locked = entry.as_ref().and_then(|e| e.get("Session")?.as_str()).map(str::to_owned);
        let ours = locked.is_some() && locked == *self.session.lock();
        let loose = ours && {
            let held = self.held.lock();
            !held.keys.contains_key(key) && !field("Flags").is_some_and(|flags| held.acquiring.contains(&flags))
        };
        if let (true, Some(session), Some(modified)) = (loose, locked, field("ModifyIndex")) {
            let stored = &path["/v1/kv/".len()..];
            let body = json!([
                {"KV": {"Verb": "check-index", "Key": stored, "Index": modified}},
                {"KV": {"Verb": "unlock", "Key": stored, "Session": session}},
            ]);
            match self.client.send(Method::PUT, "/v1/txn", Some(body)).await {
                Ok((status, ..)) if status.is_success() => {}
                // Taken again since it was read: looked at again on the next renewal.
                Ok((StatusCode::CONFLICT, ..)) => return Ok(()),
                Ok((status, ..)) => return Err(OwnershipError::Unavailable(format!("consul answered {status}"))),
                Err(error) => return Err(OwnershipError::Unavailable(error)),
            }
        }
        let mut held = self.held.lock();
        // Left locked again meanwhile, by an acquire that failed: looked at again.
        if held.loose.get(key) == Some(&stamp) {
            held.loose.remove(key);
        }
        Ok(())
    }

    async fn take(&self, path: &str, flags: u64) -> Result<(u64, String), OwnershipError> {
        let mut replaced = false;
        let session = loop {
            let session = self.session().await?;
            let lock = format!("{path}?acquire={session}&flags={flags}");
            let took = self.client.send(Method::PUT, &lock, Some(json!(self.holder))).await;
            let (status, _, body) = took.map_err(OwnershipError::Unavailable)?;
            // A session that lapsed while this node held nothing to renew is gone for good; start another.
            let lapsed = status == StatusCode::INTERNAL_SERVER_ERROR
                && String::from_utf8_lossy(&body).contains("invalid session");
            if status.is_success() && body == b"true" {
                break session;
            } else if status.is_success() {
                return Err(OwnershipError::Held(None));
            } else if lapsed && !replaced {
                replaced = true;
                self.forget_session(&session);
            } else {
                return Err(OwnershipError::Unavailable(format!("consul answered {status}")));
            }
        };
        let read = self.client.expect_ok(Method::GET, path, None).await.map_err(OwnershipError::Unavailable)?;
        let entries: Vec<Value> = serde_json::from_slice(&read).unwrap_or_default();
        let entry = entries.first().filter(|e| e.get("Session").and_then(Value::as_str) == Some(&session));
        let epoch = entry.and_then(|e| e.get("LockIndex")?.as_u64()).ok_or(OwnershipError::Lost)?;
        Ok((epoch, session))
    }

    /// Unless a new session has replaced it already.
    fn forget_session(&self, session: &str) {
        let mut cached = self.session.lock();
        if cached.as_deref() == Some(session) {
            *cached = None;
        }
    }

    fn key_path(&self, key: &str) -> String {
        let escape = |b: u8| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'/' => (b as char).to_string(),
            _ => format!("={b:02X}"),
        };
        format!("/v1/kv/{}/{}", self.client.cfg.service, key.bytes().map(escape).collect::<String>())
    }

    /// A key as `key_path` stored it.
    fn unescape(stored: &str) -> Option<String> {
        let mut parts = stored.split('=');
        let mut key = parts.next()?.as_bytes().to_vec();
        for part in parts {
            key.push(u8::from_str_radix(part.get(..2)?, 16).ok()?);
            key.extend(part.get(2..)?.as_bytes());
        }
        String::from_utf8(key).ok()
    }

    async fn session(&self) -> Result<String, OwnershipError> {
        if let Some(session) = self.session.lock().clone() {
            return Ok(session);
        }
        let _creating = self.creating.lock().await;
        if let Some(session) = self.session.lock().clone() {
            return Ok(session);
        }
        let cfg = &self.client.cfg;
        let body = json!({
            "Name": format!("{}:{}", cfg.service, self.holder),
            "TTL": seconds(cfg.ttl.max(Duration::from_secs(10))),
            "Behavior": "release",
            "LockDelay": seconds(cfg.lock_delay),
        });
        let created = self.client.expect_ok(Method::PUT, "/v1/session/create", Some(body)).await;
        let created: Value = serde_json::from_slice(&created.map_err(OwnershipError::Unavailable)?).unwrap_or_default();
        let id =
            created.get("ID").and_then(Value::as_str).ok_or(OwnershipError::Unavailable("no session id".into()))?;
        *self.session.lock() = Some(id.to_owned());
        Ok(id.to_owned())
    }
}

/// What this node holds of the keys, and what it may have left locked.
#[derive(Default)]
struct Held {
    /// The session each held key was taken under: a key under a session since replaced is gone.
    keys: BTreeMap<String, String>,
    /// The flags of the acquires under way, whose locks may yet be answered as taken.
    acquiring: BTreeSet<u64>,
    /// Keys released, or whose acquire failed after it may have taken the lock, each with a stamp: the lock may
    /// still be this node's with nothing running the key.
    loose: BTreeMap<String, u64>,
}

/// An acquire under way. Ended without an answer, dropped or failed, its key may be left locked.
struct Taking<'a> {
    leases: &'a ConsulLeases,
    /// `None` once the acquire held the key or was refused it.
    key: Option<String>,
    flags: u64,
}

impl Drop for Taking<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.leases.left_locked(key);
        }
        self.leases.held.lock().acquiring.remove(&self.flags);
    }
}

impl OwnershipAuthority for ConsulLeases {
    fn acquire(&self, key: &str) -> BoxFuture<'_, Result<u64, OwnershipError>> {
        let (path, key) = (self.key_path(key), key.to_owned());
        Box::pin(async move {
            let flags = self.acquires.fetch_add(1, Relaxed) + 1;
            self.held.lock().acquiring.insert(flags);
            let mut taking = Taking { leases: self, key: Some(key), flags };
            match self.take(&path, flags).await {
                Ok((epoch, session)) => {
                    let key = taking.key.take().expect("taken once");
                    self.held.lock().keys.insert(key, session);
                    Ok(epoch)
                }
                Err(held @ OwnershipError::Held(_)) => {
                    taking.key = None;
                    Err(held)
                }
                Err(error) => Err(error),
            }
        })
    }

    /// Every key hangs off the one session, so they are all renewed or all not, but for keys taken under a
    /// session that lapsed and was replaced since: Consul released those, and they are answered `Lost`. Keys left
    /// locked are unlocked alongside, within the renewal's own timeout: the renewals would keep them locked for good.
    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>> {
        Box::pin(async move {
            let Some(session) = self.session.lock().clone() else { return Ok(Vec::new()) };
            let path = format!("/v1/session/renew/{session}");
            let loose: Vec<String> = self.held.lock().loose.keys().cloned().collect();
            let unlocking = tokio::time::timeout(self.client.cfg.timeout, async {
                for key in &loose {
                    let _ = self.unlock(key).await;
                }
            });
            match tokio::join!(self.client.send(Method::PUT, &path, None), unlocking).0 {
                Ok((status, ..)) if status.is_success() => {
                    let mut held = self.held.lock();
                    let gone = held.keys.extract_if(.., |_, taken| *taken != session);
                    Ok(gone.map(|(key, _)| (key, OwnershipError::Lost)).collect())
                }
                Ok((StatusCode::NOT_FOUND, ..)) => {
                    self.forget_session(&session);
                    self.held.lock().keys.retain(|_, taken| *taken != session);
                    Err(OwnershipError::Lost)
                }
                Ok((status, ..)) => Err(OwnershipError::Unavailable(format!("consul answered {status}"))),
                Err(error) => Err(OwnershipError::Unavailable(error)),
            }
        })
    }

    /// A late release must not unlock the key taken again since, and a lost one must not leave it locked under the
    /// renewals: the key is left locked until `unlock` finds the lock gone or unlocks it.
    fn release(&self, key: &str, _epoch: u64) -> BoxFuture<'_, Result<(), OwnershipError>> {
        let key = key.to_owned();
        self.held.lock().keys.remove(&key);
        self.left_locked(key.clone());
        Box::pin(async move { self.unlock(&key).await })
    }

    /// The keys whose value names the node: a lapsed session releases a key but leaves its value, an explicit release
    /// empties it, and the next holder writes its own. For `Holder::Lapsed`, those with a value and no session. `None`
    /// when this node writes another name than its node id.
    // ponytail: locks are never deleted, so this reads every key each kind ever leased, and past `max_response_bytes`
    // (about 20k keys at 4 MiB) it fails: raise that, or keep a per-node index key, for kinds with more keys than that.
    fn held_by(
        &self,
        holder: Holder<'_>,
        kinds: &[String],
    ) -> Option<BoxFuture<'_, Result<Vec<String>, OwnershipError>>> {
        if !self.named.load(Relaxed) {
            return None;
        }
        let value = match holder {
            Holder::Node(node) => Some(base64(json!(node).to_string().as_bytes())),
            Holder::Lapsed => None,
        };
        let paths: Vec<String> =
            kinds.iter().map(|kind| format!("{}?recurse", self.key_path(&format!("{kind}/")))).collect();
        Some(Box::pin(async move {
            let prefix = format!("{}/", self.client.cfg.service);
            let mut keys = Vec::new();
            for path in paths {
                let entries = self.client.get_entries(&path).await.map_err(OwnershipError::Unavailable)?;
                let held = entries.iter().filter(|e| {
                    let v = e.get("Value").and_then(Value::as_str);
                    match &value {
                        Some(value) => v == Some(value.as_str()),
                        None => v.is_some_and(|v| !v.is_empty()) && e.get("Session").is_none_or(Value::is_null),
                    }
                });
                keys.extend(held.filter_map(|e| Self::unescape(e.get("Key")?.as_str()?.strip_prefix(&prefix)?)));
            }
            Ok(keys)
        }))
    }

    fn attach<'a>(&'a self, cluster: &'a Cluster) -> BoxFuture<'a, Result<(), ClusterError>> {
        self.named.store(cluster.identity().node_id == self.holder, Relaxed);
        Box::pin(async { Ok(()) })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            *self.held.lock() = Held::default();
            let Some(session) = self.session.lock().take() else { return };
            let _ = self.client.expect_ok(Method::PUT, &format!("/v1/session/destroy/{session}"), None).await;
        })
    }

    fn describe(&self) -> ProviderInfo {
        let mut info =
            self.client.describe("consul").setting("lock delay", format!("{:?}", self.client.cfg.lock_delay));
        info.stats.insert(0, ("session".into(), self.session.lock().clone().unwrap_or_else(|| "none".into())));
        info
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_holder_and_a_key_read_back_as_consul_stores_them() {
        let encoded = ["", "o", "on", "one", "\"one\""].map(|s| base64(s.as_bytes()));
        assert_eq!(encoded, ["", "bw==", "b24=", "b25l", "Im9uZSI="]);
        let leases = ConsulLeases::new(ConsulConfig::new("http://127.0.0.1:8500", "axon"), "one").unwrap();
        let stored = leases.key_path("eager/EUR USD=1");
        let stored = stored.strip_prefix("/v1/kv/axon/").unwrap();
        assert_eq!(stored, "eager/EUR=20USD=3D1");
        assert_eq!(ConsulLeases::unescape(stored).as_deref(), Some("eager/EUR USD=1"));
        assert_eq!(ConsulLeases::unescape("a=2"), None);
    }
}
