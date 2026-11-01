use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use axum::body::Body;
use axum::extract::{ConnectInfo, Request as HttpRequest, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Form, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::Instant;

use super::Inner;
use super::api::{App, Failure, alive, redact};
use crate::health::DEADLINE;
use crate::metrics::now_ms;

const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
                   font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; \
                   object-src 'none'";
const COOKIE: &str = "axon_session";
/// Why a request was refused a session, in the 401's `error`: the sign-in page explains each.
const SIGN_IN: &str = "sign in first";
const SIGNED_OUT: &str = "signed out: the console restarted or signed everyone out";
const EXPIRED: &str = "the session expired";
const ATTEMPTS: f64 = 5.0;
const REFILL: Duration = Duration::from_secs(20);
const LIMITED_CLIENTS: usize = 10_000;
/// The only body read is a sign-in form: a name of at most 64 bytes and a password.
const SIGN_IN_BYTES: usize = 4096;

/// An argon2id PHC string for [`User::hash`](super::User::hash): argon2's default cost (19 MiB, 2 passes) and a
/// random salt.
pub fn hash_password(password: &str) -> Result<String, String> {
    let mut salt = [0; 16];
    SystemRandom::new().fill(&mut salt).map_err(|_| "no randomness for the salt")?;
    let salt = SaltString::encode_b64(&salt).map_err(|e| e.to_string())?;
    Argon2::default().hash_password(password.as_bytes(), &salt).map(|h| h.to_string()).map_err(|e| e.to_string())
}

fn verify(hash: &str, password: &str) -> bool {
    PasswordHash::new(hash).is_ok_and(|hash| Argon2::default().verify_password(password.as_bytes(), &hash).is_ok())
}

pub(crate) fn signing_key(key: Option<Vec<u8>>) -> Result<hmac::Key, String> {
    match key {
        Some(key) if key.len() < 32 => Err("a session key needs 32 bytes or more".into()),
        Some(key) => Ok(hmac::Key::new(hmac::HMAC_SHA256, &key)),
        None => {
            hmac::Key::generate(hmac::HMAC_SHA256, &SystemRandom::new()).map_err(|_| "no randomness for a key".into())
        }
    }
}

/// A signed-in user, as the cookie says.
#[derive(Clone)]
pub(crate) struct Session {
    user: String,
    /// When it lapses unless used again, in Unix ms.
    pub(crate) deadline: u64,
    /// Changes when everyone is signed out.
    pub(crate) keys: watch::Receiver<hmac::Key>,
}

/// Sign-in attempts left, when that was counted, and whether running out was recorded.
pub(crate) type Bucket = (f64, Instant, bool);

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

impl Inner {
    /// Who is asking, and whether the browser reached us over TLS. A trusted proxy names the client: the nearest
    /// `X-Forwarded-For` address it does not vouch for.
    fn client(&self, headers: &HeaderMap, peer: IpAddr) -> (IpAddr, bool) {
        let tls = self.tls.is_some();
        let trusted = |ip: &IpAddr| self.cfg.trusted_proxies.contains(ip);
        if !trusted(&peer) {
            return (peer, tls);
        }
        let forwarded = headers.get_all("x-forwarded-for").iter().filter_map(|v| v.to_str().ok());
        let hops: Vec<IpAddr> =
            forwarded.flat_map(|v| v.split(',')).filter_map(|hop| hop.trim().parse().ok()).collect();
        let client = hops.iter().rev().find(|ip| !trusted(ip)).or(hops.first()).map_or(peer, |ip| ip.to_canonical());
        let proto = header_text(headers, "x-forwarded-proto").and_then(|p| p.split(',').next());
        (client, tls || proto.is_some_and(|p| p.trim().eq_ignore_ascii_case("https")))
    }

    /// A sign-in or sign-out must come from the console's own page: its `Origin`, or lacking one its `Referer`,
    /// names the host the browser asked for.
    fn same_origin(&self, headers: &HeaderMap, peer: IpAddr) -> bool {
        let forwarded = header_text(headers, "x-forwarded-host").filter(|_| self.cfg.trusted_proxies.contains(&peer));
        let host = forwarded.and_then(|h| h.split(',').next()).or(header_text(headers, header::HOST.as_str()));
        let from = header_text(headers, "origin").or(header_text(headers, "referer"));
        let authority = from
            .and_then(|from| from.strip_prefix("https://").or(from.strip_prefix("http://")))
            .and_then(|rest| rest.split(['/', '?', '#']).next());
        authority.zip(host).is_some_and(|(authority, host)| authority.eq_ignore_ascii_case(host.trim()))
    }

    fn deadline(&self, issued: u64, last: u64) -> u64 {
        let ms = |d: Duration| d.as_millis() as u64;
        (last + ms(self.cfg.session.idle)).min(issued + ms(self.cfg.session.max))
    }

    /// The signed-in user and when they signed in, if the cookie is valid under `key`; else why not, which the sign-in
    /// page says. `issued` and `last` may come from another dashboard's clock, so neither is held to this one's, nor
    /// to each other.
    fn session(&self, key: &hmac::Key, headers: &HeaderMap, now: u64) -> Result<(String, u64), &'static str> {
        let cookies = headers.get_all(header::COOKIE).iter().filter_map(|v| v.to_str().ok());
        let mut values =
            cookies.flat_map(|v| v.split(';')).filter_map(|c| c.trim().strip_prefix(COOKIE)?.strip_prefix('='));
        let mut why = SIGN_IN;
        let found = values.find_map(|value| {
            let (body, mac) = value.rsplit_once('.')?;
            if hmac::verify(key, body.as_bytes(), &B64.decode(mac).ok()?).is_err() {
                why = SIGNED_OUT;
                return None;
            }
            let mut fields = body.split('.');
            let user = String::from_utf8(B64.decode(fields.next()?).ok()?).ok()?;
            let (issued, last) = (fields.next()?.parse().ok()?, fields.next()?.parse().ok()?);
            let listed = self.cfg.users.iter().any(|u| u.name == user);
            let current = now < self.deadline(issued, last);
            if !current {
                why = EXPIRED;
            }
            (fields.next().is_none() && current && listed).then_some((user, issued))
        });
        found.ok_or(why)
    }

    fn cookie(&self, value: &str, lasts_ms: u64, secure: bool) -> HeaderValue {
        let (path, age, secure) = (&self.cfg.base_path, lasts_ms.div_ceil(1000), if secure { "; Secure" } else { "" });
        let cookie = format!("{COOKIE}={value}; Path={path}; Max-Age={age}; HttpOnly; SameSite=Strict{secure}");
        HeaderValue::from_str(&cookie).expect("base64, digits and a validated base path")
    }

    /// Takes one attempt from each bucket, or says how many seconds until each has one, and whether a bucket has
    /// run out since it last gave one: that refusal is worth an event, the ones after it are not.
    // ponytail: buckets per dashboard, so n dashboards behind a balancer allow n times the attempts; Quotas
    // (spec/quotas.md) can hold them cluster-wide. Every take also takes from a user bucket (one per listed user
    // and 64 for all other names), so the table grows by at most five clients per such bucket every 20 s.
    fn take(&self, keys: &[String]) -> Result<(), (u64, bool)> {
        let now = Instant::now();
        let level =
            |&(tokens, at, _): &Bucket| (tokens + (now - at).as_secs_f64() / REFILL.as_secs_f64()).min(ATTEMPTS);
        let mut buckets = self.attempts.lock();
        let left = |buckets: &BTreeMap<String, Bucket>, key: &String| buckets.get(key).map_or(ATTEMPTS, level);
        let wait = keys.iter().map(|key| ((1.0 - left(&buckets, key)).max(0.0) * REFILL.as_secs_f64()).ceil() as u64);
        let wait = wait.max().unwrap_or(0);
        if wait > 0 {
            let mut ran_out = false;
            for key in keys {
                if let Some(bucket) = buckets.get_mut(key)
                    && level(bucket) < 1.0
                    && !bucket.2
                {
                    bucket.2 = true;
                    ran_out = true;
                }
            }
            return Err((wait, ran_out));
        }
        let fresh = keys.iter().filter(|key| !buckets.contains_key(*key)).count();
        if buckets.len() + fresh > LIMITED_CLIENTS {
            buckets.retain(|_, bucket| level(bucket) < ATTEMPTS);
            if buckets.len() + fresh > LIMITED_CLIENTS {
                return Err((REFILL.as_secs(), false));
            }
        }
        for key in keys {
            let tokens = left(&buckets, key) - 1.0;
            buckets.insert(key.clone(), (tokens, now, false));
        }
        Ok(())
    }

    fn refund(&self, keys: &[String]) {
        let mut buckets = self.attempts.lock();
        for key in keys {
            if let Some((tokens, _, _)) = buckets.get_mut(key) {
                *tokens = (*tokens + 1.0).min(ATTEMPTS);
            }
        }
    }
}

/// What every answer carries, and the redaction of JSON ones.
pub(crate) async fn guard(
    State(weak): App,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: HttpRequest,
    next: Next,
) -> Response {
    let Ok(inner) = alive(&weak) else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    let (_, secure) = inner.client(request.headers(), peer.ip().to_canonical());
    let immutable = request.uri().path().contains("/assets/");
    let mut response = next.run(request).await;
    let json =
        response.headers().get(header::CONTENT_TYPE).is_some_and(|t| t.as_bytes().starts_with(b"application/json"));
    if inner.cfg.redact && json {
        let (mut parts, body) = response.into_parts();
        let Ok(body) = axum::body::to_bytes(body, usize::MAX).await else {
            return Failure::upstream("the reply stopped before its end".into()).into_response();
        };
        let body = match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(mut value) => {
                redact(&mut value, &inner.key.borrow().clone());
                value.to_string().into()
            }
            Err(_) => body,
        };
        parts.headers.remove(header::CONTENT_LENGTH);
        response = Response::from_parts(parts, Body::from(body));
    }
    let cache =
        if immutable && response.status().is_success() { "private, max-age=31536000, immutable" } else { "no-store" };
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
    headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    if secure {
        headers.insert(header::STRICT_TRANSPORT_SECURITY, HeaderValue::from_static("max-age=31536000"));
    }
    response
}

/// The API answers a signed-in user, and slides the session along with each request.
pub(crate) async fn authenticate(
    State(weak): App,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut request: HttpRequest,
    next: Next,
) -> Response {
    let Ok(inner) = alive(&weak) else { return StatusCode::SERVICE_UNAVAILABLE.into_response() };
    if inner.cfg.users.is_empty() {
        return next.run(request).await;
    }
    let now = now_ms();
    // One key for the check and the new cookie, so a sign-out in between cannot carry a session over.
    let mut keys = inner.key.subscribe();
    let key = keys.borrow_and_update().clone();
    let (user, issued) = match inner.session(&key, request.headers(), now) {
        Ok(session) => session,
        Err(why) => return Failure(StatusCode::UNAUTHORIZED, why.into()).into_response(),
    };
    let (_, secure) = inner.client(request.headers(), peer.ip().to_canonical());
    let deadline = inner.deadline(issued, now);
    let cookie = inner.cookie(&seal(&key, &user, issued, now), deadline.saturating_sub(now), secure);
    request.extensions_mut().insert(Session { user, deadline, keys });
    let mut response = next.run(request).await;
    response.headers_mut().insert(header::SET_COOKIE, cookie);
    response
}

/// `user.issued.last.mac`, the MAC an HMAC-SHA256 over the rest.
fn seal(key: &hmac::Key, user: &str, issued: u64, last: u64) -> String {
    let body = format!("{}.{issued}.{last}", B64.encode(user));
    format!("{body}.{}", B64.encode(hmac::sign(key, body.as_bytes()).as_ref()))
}

/// The sign-in form is the one body read: it must arrive within a head's deadline and size, so a client that
/// trickles it holds no place; checking the password after has no deadline.
pub(crate) async fn in_time(request: HttpRequest, next: Next) -> Response {
    let (parts, body) = request.into_parts();
    match tokio::time::timeout(DEADLINE, axum::body::to_bytes(body, SIGN_IN_BYTES)).await {
        Ok(Ok(body)) => next.run(HttpRequest::from_parts(parts, body.into())).await,
        Ok(Err(_)) => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}

#[derive(Deserialize)]
pub(crate) struct SignIn {
    user: String,
    password: String,
}

pub(crate) async fn login(
    State(weak): App,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<SignIn>,
) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    if inner.cfg.users.is_empty() {
        return Err(Failure(StatusCode::NOT_FOUND, "this console has no users".into()));
    }
    let peer = peer.ip().to_canonical();
    if !inner.same_origin(&headers, peer) {
        return Err(Failure(StatusCode::FORBIDDEN, "sign in from a page of this console".into()));
    }
    let _slot = inner.slot()?;
    let (client, secure) = inner.client(&headers, peer);
    let user = inner.cfg.users.iter().find(|u| u.name == form.user);
    // What a stranger types is neither a key nor an event: it may be a password typed in the wrong field. Other
    // names share 64 buckets by a keyed hash, so the table stays small and which names share one is secret: one
    // bucket shared by them all would tell, once spent, which names are users.
    let name = user.map_or("unknown user", |u| u.name.as_str());
    let keys = [
        format!("client {client}"),
        match user {
            Some(user) => format!("user {}", user.name),
            None => format!("unknown {}", hmac::sign(&inner.key.borrow(), form.user.as_bytes()).as_ref()[0] % 64),
        },
    ];
    let events = inner.cluster.events();
    if let Err((wait, ran_out)) = inner.take(&keys) {
        if ran_out {
            events.record("login_limited", "", format!("{name} from {client}"));
        }
        let mut refused = Failure(StatusCode::TOO_MANY_REQUESTS, "too many sign-in attempts".into()).into_response();
        refused.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(wait));
        return Ok(refused);
    }
    // An unknown user costs a verify too, so the time taken does not tell which names exist.
    let hash = user.or(inner.cfg.users.first()).map(|u| u.hash.clone()).unwrap_or_default();
    #[expect(
        clippy::disallowed_methods,
        reason = "argon2 takes tens of ms of CPU; the dashboard runs outside the World"
    )]
    let verified = tokio::task::spawn_blocking(move || verify(&hash, &form.password)).await.unwrap_or(false);
    if !(verified && user.is_some()) {
        events.record("login_failed", "", format!("{name} from {client}"));
        return Err(Failure(StatusCode::UNAUTHORIZED, "wrong user or password".into()));
    }
    inner.refund(&keys);
    events.record("login", "", format!("{name} from {client}"));
    let now = now_ms();
    let key = inner.key.borrow().clone();
    let cookie = inner.cookie(&seal(&key, &form.user, now, now), inner.deadline(now, now) - now, secure);
    Ok(([(header::SET_COOKIE, cookie)], Json(serde_json::json!({ "user": form.user }))).into_response())
}

/// Clears the cookie in this browser. A copy of it stays valid until it idles out.
pub(crate) async fn logout(
    State(weak): App,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let peer = peer.ip().to_canonical();
    if !inner.same_origin(&headers, peer) {
        return Err(Failure(StatusCode::FORBIDDEN, "sign out from a page of this console".into()));
    }
    let (client, secure) = inner.client(&headers, peer);
    let key = inner.key.borrow().clone();
    if let Ok((user, _)) = inner.session(&key, &headers, now_ms()) {
        inner.cluster.events().record("logout", "", format!("{user} from {client}"));
    }
    Ok((StatusCode::NO_CONTENT, [(header::SET_COOKIE, inner.cookie("", 0, secure))]).into_response())
}

pub(crate) async fn session(session: Option<Extension<Session>>) -> Response {
    Json(serde_json::json!({ "user": session.map(|Extension(s)| s.user) })).into_response()
}
