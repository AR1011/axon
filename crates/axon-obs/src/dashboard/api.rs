use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axon::ActorIncarnation;
use axon_cluster::{Owner, Placement};
use axum::body::Body;
use axum::extract::{Query, Request as HttpRequest, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use ring::hmac;
use serde::Serialize;
use serde_json::value::RawValue;
use tokio::time::{Instant, timeout};

use super::auth::Session;
use super::{ASSETS, ActorRow, Inner, SNAPSHOT_DEADLINE};
use crate::MetricsRequest;
use crate::metrics::{now_ms, query_metrics};
use crate::model::{ActorQuery, KeyPage, Request, boot};
use crate::provider::{Aggregation, Capabilities, EventQuery, HistoryQuery, HistoryResult, MAX_SERIES, Sum};
use crate::telemetry::fetch;
use crate::traces::{DeadLetterRecord, MAX_TRACE_LIMIT, SpanRecord, TraceQuery, TraceSummary, merge, parse_id};

/// Values under these keys may be `kind/key` names; `redact` keeps the kind.
const KEYED: [&str; 8] = ["name", "actor", "actor_name", "root", "root_actor_name", "target", "stream", "subject"];
const MAX_METRICS: usize = 32;
const SORT_KEYS: [&str; 15] = [
    "node_id",
    "name",
    "actor_type",
    "phase",
    "up",
    "since_restart",
    "restarts",
    "mailbox_depth",
    "mailbox_bytes",
    "processed",
    "admitted",
    "rejected",
    "oldest_message_age_ns",
    "jobs",
    "timers",
];

#[derive(Serialize)]
struct InventoryPage<'a> {
    actors: &'a [ActorRow],
    total: usize,
    offset: usize,
    limit: usize,
    truncated: bool,
    errors: &'a BTreeMap<String, String>,
    timestamp_ms: u64,
}

fn hide(text: &mut String) {
    let kept = crate::model::shown(text).len();
    if kept < text.len() {
        text.truncate(kept);
        text.push('…');
    }
}

/// `names` signs what stands in for a stream's name, so a stream keeps one name across nodes and replies.
pub(crate) fn redact(value: &mut serde_json::Value, names: &hmac::Key) {
    match value {
        serde_json::Value::Object(map) => {
            // A stream is `namespace` and `name`, where the name is the key.
            let stream = map.get("namespace").is_some_and(serde_json::Value::is_string);
            for (key, value) in map.iter_mut() {
                match value {
                    serde_json::Value::String(text) if stream && key == "name" => {
                        let mac = hmac::sign(names, text.as_bytes());
                        *text = mac.as_ref()[..4].iter().fold("…".into(), |text, b| format!("{text}{b:02x}"));
                    }
                    serde_json::Value::String(text) if KEYED.contains(&key.as_str()) => hide(text),
                    // `Cluster::hosted` lists each key apart from its kind, so all of it goes.
                    serde_json::Value::Object(hosted) if key == "hosted" => {
                        let keys = hosted.get_mut("keys").and_then(serde_json::Value::as_array_mut);
                        for listed in keys.into_iter().flatten() {
                            if let Some(serde_json::Value::String(text)) = listed.get_mut("key") {
                                *text = "…".into();
                            }
                        }
                    }
                    other => redact(other, names),
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(|item| redact(item, names)),
        _ => {}
    }
}

pub(crate) struct Failure(pub(crate) StatusCode, pub(crate) String);

impl Failure {
    fn bad(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self(StatusCode::SERVICE_UNAVAILABLE, message.into())
    }

    pub(crate) fn upstream(message: String) -> Self {
        Self(StatusCode::BAD_GATEWAY, message)
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "available": false, "error": self.1 }))).into_response()
    }
}

pub(crate) type App = State<Weak<Inner>>;
type Params = Query<HashMap<String, String>>;

pub(crate) fn alive(weak: &Weak<Inner>) -> Result<Arc<Inner>, Failure> {
    weak.upgrade().ok_or_else(|| Failure::unavailable("the system has stopped"))
}

fn json(body: impl Into<Body>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body.into()).into_response()
}

fn number<T: std::str::FromStr>(params: &HashMap<String, String>, name: &str, default: T) -> Result<T, Failure> {
    match params.get(name) {
        Some(text) => text.parse().map_err(|_| Failure::bad(format!("{name} is not a valid number"))),
        None => Ok(default),
    }
}

fn text(params: &HashMap<String, String>, name: &str) -> Option<String> {
    params.get(name).filter(|v| !v.is_empty()).cloned()
}

fn labels(params: &HashMap<String, String>) -> BTreeMap<String, String> {
    #[expect(clippy::disallowed_methods, reason = "the labels are collected into a BTreeMap, which orders them")]
    let labels = params.iter().filter_map(|(k, v)| Some((k.strip_prefix("label.")?.to_owned(), v.clone())));
    labels.collect()
}

fn actor_id(text: &str) -> Result<ActorIncarnation, Failure> {
    let mut parts = text.splitn(3, ':').map(str::parse::<u64>);
    match (parts.next(), parts.next(), parts.next()) {
        (Some(Ok(runtime)), Some(Ok(slot)), Some(Ok(generation))) => Ok(ActorIncarnation { runtime, slot, generation }),
        _ => Err(Failure::bad("an actor id is `runtime:slot:generation`")),
    }
}

fn actor_query(params: &HashMap<String, String>, redacted: bool) -> Result<ActorQuery, Failure> {
    Ok(ActorQuery {
        id: params.get("id").map(|id| actor_id(id)).transpose()?,
        after: params.get("after").map(|id| actor_id(id)).transpose()?,
        limit: 1,
        actor_type: text(params, "actor_type"),
        phase: text(params, "phase"),
        search: text(params, "q").filter(|q| q.len() <= 256),
        timings: params.get("timings").is_some_and(|t| t == "true"),
        redacted,
    })
}

pub(crate) async fn overview(State(weak): App) -> Result<Response, Failure> {
    Ok(json(alive(&weak)?.overview().await))
}

/// Ends when the session would, or when everyone is signed out, so the browser comes back with a valid one or not at
/// all.
pub(crate) async fn events(State(weak): App, session: Option<Extension<Session>>) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let slot = (inner.streams.clone().try_acquire_owned())
        .map_err(|_| Failure(StatusCode::TOO_MANY_REQUESTS, "live subscriber limit".into()))?;
    inner.overview().await;
    let mut frames = inner.frames.subscribe();
    frames.mark_changed();
    drop(inner);
    let lasts = |deadline: u64| Instant::now() + Duration::from_millis(deadline.saturating_sub(now_ms()));
    let (until, keys) = session.map(|Extension(s)| (lasts(s.deadline), s.keys)).unzip();
    // An empty frame is the shutdown signal.
    let stream = futures_util::stream::unfold((frames, keys, slot), move |(mut frames, mut keys, slot)| async move {
        let ended = async {
            match (until, keys.as_mut()) {
                (Some(until), Some(keys)) => drop(tokio::time::timeout_at(until, keys.changed()).await),
                _ => std::future::pending().await,
            }
        };
        tokio::select! {
            changed = frames.changed() => changed.ok()?,
            () = ended => return None,
        }
        let frame = frames.borrow_and_update().clone();
        (!frame.is_empty()).then(|| (Ok::<_, Infallible>(frame), (frames, keys, slot)))
    });
    let headers =
        [(header::CONTENT_TYPE, "text/event-stream"), (header::HeaderName::from_static("x-accel-buffering"), "no")];
    // Fused: the compression layer polls once more after the end, to finish its frame.
    Ok((headers, Body::from_stream(futures_util::StreamExt::fuse(stream))).into_response())
}

pub(crate) async fn actors(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let node = params.get("node_id").ok_or_else(|| Failure::bad("node_id is required"))?;
    let view = inner.cluster.view();
    let member =
        view.get(node).ok_or_else(|| Failure(StatusCode::NOT_FOUND, "node is not in the current membership".into()))?;
    if params.get("incarnation").is_some_and(|i| *i != boot(member.record.incarnation)) {
        return Err(Failure(StatusCode::CONFLICT, "this link is for an earlier start of the node".into()));
    }
    let mut query = actor_query(&params, inner.cfg.redact)?;
    query.limit = number(&params, "limit", 25)?;
    let page: Box<RawValue> = fetch(&inner.cluster, node, &Request::Actors(query)).await.map_err(Failure::upstream)?;
    Ok(Json(page).into_response())
}

/// Per-stream positions and recent stream events of every reachable node, fetched when asked for.
pub(crate) async fn streams(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let limit = number(&params, "limit", 200usize)?;
    let (done, errors) = inner
        .ask_reachable(move |cluster, node| async move {
            fetch::<Box<RawValue>>(&cluster, &node, &Request::Streams { limit }).await
        })
        .await;
    let nodes: BTreeMap<String, Box<RawValue>> = done.into_iter().collect();
    let body = serde_json::json!({ "nodes": nodes, "errors": errors });
    Ok(json(body.to_string()))
}

pub(crate) async fn inventory(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let (offset, limit) = (number(&params, "offset", 0usize)?, number(&params, "limit", 100usize)?);
    let sort = params.get("sort").map_or("node_id", String::as_str);
    if !(1..=1000).contains(&limit) || !SORT_KEYS.contains(&sort) {
        return Err(Failure::bad("limit must be 1..=1000, and sort must name a known column"));
    }
    let gathered = inner.gather(actor_query(&params, inner.cfg.redact)?, params.get("node_id")).await?;
    // Redacted, names sort by what they show, or the order would tell of the keys.
    let shown: fn(&str) -> &str = if inner.cfg.redact { crate::model::shown } else { |name| name };

    let count = |row: &ActorRow| match sort {
        "restarts" => row.restarts,
        "rejected" => row.diagnostics["rejected"].as_array().map_or(0, |r| r.iter().filter_map(|v| v.as_u64()).sum()),
        other => row.diagnostics[other].as_u64().unwrap_or(0),
    };
    // Since restart is blank for an actor that never restarted: those rows go last in either order.
    let blank = |row: &ActorRow| sort == "since_restart" && row.restarted_us.is_none();
    let mut order: Vec<&ActorRow> = gathered.rows.iter().collect();
    order.sort_by(|a, b| {
        let by = match sort {
            "node_id" => std::cmp::Ordering::Equal,
            "name" => a.name.as_deref().map(shown).cmp(&b.name.as_deref().map(shown)),
            "actor_type" => a.actor_type.cmp(&b.actor_type),
            "phase" => a.phase.cmp(&b.phase),
            // Ascending is the shortest time up first.
            "up" => b.started_us.cmp(&a.started_us),
            "since_restart" => b.restarted_us.cmp(&a.restarted_us),
            _ => count(a).cmp(&count(b)),
        };
        let by = if params.get("order").is_some_and(|o| o == "desc") { by.reverse() } else { by };
        blank(a).cmp(&blank(b)).then(by).then_with(|| (&a.node_id, a.incarnation).cmp(&(&b.node_id, b.incarnation)))
    });
    let page: Vec<ActorRow> = order.into_iter().skip(offset).take(limit).cloned().collect();
    let page = InventoryPage {
        actors: &page,
        total: gathered.rows.len(),
        offset,
        limit,
        truncated: gathered.truncated,
        errors: &gathered.errors,
        timestamp_ms: gathered.at_ms,
    };
    Ok(Json(page).into_response())
}

/// Where one keyed actor runs: the owner this node's view names, and what that owner answers a watch of the key.
pub(crate) async fn key(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    if inner.cfg.redact {
        return Err(Failure(StatusCode::FORBIDDEN, "this console hides keys".into()));
    }
    let _slot = inner.slot()?;
    let (Some(kind), Some(key)) = (text(&params, "kind"), text(&params, "key")) else {
        return Err(Failure::bad("kind and key are required"));
    };
    let placement = Placement { required: labels(&params), ..Placement::default() };
    // The watch places the key again when polled, so a running actor's node is the one it names.
    let (owner, running, epoch) = match inner.cluster.owner(&kind, &key, &placement) {
        None => (None, false, None),
        Some(owner) => {
            match timeout(SNAPSHOT_DEADLINE, inner.cluster.watch_key(&kind, &key, &placement).next()).await {
                Ok(Some(Owner::Running { node, epoch })) => (Some(node), true, epoch),
                Ok(Some(Owner::Stopped)) => (Some(owner), false, None),
                Ok(None) => return Err(Failure::upstream(format!("{owner} refuses to answer for {kind}"))),
                Err(_) => return Err(Failure::upstream(format!("{owner} did not answer in time"))),
            }
        }
    };
    Ok(Json(serde_json::json!({ "owner": owner.as_deref(), "running": running, "epoch": epoch })).into_response())
}

/// A page of the keys of one kind that start with `prefix`, from one node (`node_id`) or from every reachable node,
/// merged in key and node order. `after` and `after_node` name the last row of the page before.
pub(crate) async fn keys(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    if inner.cfg.redact {
        return Err(Failure(StatusCode::FORBIDDEN, "this console hides keys".into()));
    }
    let _slot = inner.slot()?;
    let kind = text(&params, "kind").ok_or_else(|| Failure::bad("kind is required"))?;
    let limit = number(&params, "limit", 100usize)?;
    if !(1..=1000).contains(&limit) {
        return Err(Failure::bad("limit must be 1..=1000"));
    }
    let after = text(&params, "after").map(|key| (key, params.get("after_node").cloned().unwrap_or_default()));
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    // From the last row's key, and one past the page: a node that also runs that key still fills the page after it.
    let from = after.as_ref().map(|(key, _)| key.clone());
    let request = Request::Keys { kind, prefix, from, limit: limit + 1 };
    let (done, errors) = match text(&params, "node_id") {
        Some(node) => match fetch::<KeyPage>(&inner.cluster, &node, &request).await {
            Ok(page) => (vec![(node, page)], BTreeMap::new()),
            Err(error) => (Vec::new(), BTreeMap::from([(node, error)])),
        },
        None => {
            inner
                .ask_reachable(move |cluster, node| {
                    let request = request.clone();
                    async move { fetch::<KeyPage>(&cluster, &node, &request).await }
                })
                .await
        }
    };
    let mut more = done.iter().any(|(_, page)| page.more);
    // A node that sent less than it holds, its reply cut to fit, has keys past its last row: the page ends there, or the
    // next one would start past them.
    let cut = (done.iter().filter(|(_, page)| page.more))
        .filter_map(|(node, page)| page.keys.last().map(|k| (k.key.clone(), node.clone())))
        .min();
    let rows =
        done.into_iter().flat_map(|(node, page)| page.keys.into_iter().map(move |k| (k.key, node.clone(), k.epoch)));
    let mut rows: Vec<_> = rows
        .filter(|(key, node, _)| after.as_ref().is_none_or(|(k, n)| (key, node) > (k, n)))
        .filter(|(key, node, _)| cut.as_ref().is_none_or(|(k, n)| (key, node) <= (k, n)))
        .collect();
    rows.sort_unstable();
    more |= rows.len() > limit;
    rows.truncate(limit);
    let keys: Vec<_> =
        rows.iter().map(|(key, node, epoch)| serde_json::json!({ "key": key, "node": node, "epoch": epoch })).collect();
    Ok(Json(serde_json::json!({ "keys": keys, "more": more, "errors": errors })).into_response())
}

/// One session's stage timings and request round trips, asked of the node that holds it.
pub(crate) async fn connection(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let (Some(node), Some(peer)) = (text(&params, "node_id"), text(&params, "peer")) else {
        return Err(Failure::bad("node_id and peer are required"));
    };
    let outgoing = match params.get("direction").map(String::as_str) {
        Some("out") => true,
        Some("in") => false,
        _ => return Err(Failure::bad("direction is in or out")),
    };
    let lane = params.get("lane").and_then(|name| axon::Lane::ALL.into_iter().find(|l| l.name() == name));
    let lane = lane.ok_or_else(|| Failure::bad("lane is control, live or bulk"))?;
    let gone = |what: &str| Failure(StatusCode::NOT_FOUND, format!("{what} is not there any more"));
    inner.cluster.view().get(&node).ok_or_else(|| gone("the node"))?;
    let request = Request::Connection { peer, outgoing, lane };
    let found: Option<Box<RawValue>> = fetch(&inner.cluster, &node, &request).await.map_err(Failure::upstream)?;
    Ok(Json(found.ok_or_else(|| gone("the session"))?).into_response())
}

#[derive(Serialize)]
struct Charts<'a> {
    source: &'a str,
    results: Vec<Charted<'a>>,
}

#[derive(Serialize)]
struct Charted<'a> {
    metric: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub(crate) async fn history(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let template = HistoryQuery {
        aggregation: match params.get("aggregation").map(String::as_str) {
            None | Some("last") => Aggregation::Last,
            Some("mean") => Aggregation::Mean,
            Some("min") => Aggregation::Min,
            Some("max") => Aggregation::Max,
            Some(_) => return Err(Failure::bad("aggregation is last, mean, min or max")),
        },
        node: text(&params, "node_id"),
        labels: labels(&params),
        max_points: number(&params, "max_points", 2000)?,
        max_series: number(&params, "max_series", 32)?,
        sum: match params.get("sum").map(String::as_str) {
            None => None,
            Some("node") => Some(Sum::Node),
            Some("cluster") => Some(Sum::Cluster),
            Some(_) => return Err(Failure::bad("sum is node or cluster")),
        },
        by: text(&params, "by").map(|by| by.split(',').map(str::to_owned).collect()).unwrap_or_default(),
        ..HistoryQuery::new(
            "",
            number(&params, "from_ms", 0)?,
            number(&params, "to_ms", 0)?,
            number(&params, "step_ms", 0)?,
        )
    };
    let metrics: Vec<&str> = params.get("metrics").map(|m| m.split(',').collect()).unwrap_or_default();
    if metrics.is_empty() || metrics.len() > MAX_METRICS {
        return Err(Failure::bad(format!("metrics names 1..={MAX_METRICS} metrics, comma separated")));
    }
    let queries: Vec<_> =
        metrics.iter().map(|m| HistoryQuery { metric: (*m).to_owned(), ..template.clone() }).collect();
    queries.iter().try_for_each(HistoryQuery::validate).map_err(Failure::bad)?;
    let node = inner.source(params.get("history")).await?;
    let answers: Vec<Result<Box<RawValue>, String>> = inner.ask_store(&node, &MetricsRequest::Queries(queries)).await?;
    let results: Vec<_> = (metrics.iter().zip(answers))
        .map(|(metric, answer)| match answer {
            Ok(result) => Charted { metric, result: Some(result), error: None },
            Err(error) => Charted { metric, result: None, error: Some(error) },
        })
        .collect();
    Ok(Json(Charts { source: &node, results }).into_response())
}

/// The series a metric has in a range, without values: what Explore offers to filter by.
pub(crate) async fn series(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let metric = text(&params, "metric").ok_or_else(|| Failure::bad("metric is required"))?;
    let to_ms = number(&params, "to_ms", now_ms())?;
    let from_ms = number(&params, "from_ms", to_ms.saturating_sub(3_600_000))?;
    let query = HistoryQuery {
        node: text(&params, "node_id"),
        max_series: MAX_SERIES,
        ..HistoryQuery::new(metric, from_ms, to_ms, 86_400_000)
    };
    query.validate().map_err(Failure::bad)?;
    let node = inner.source(params.get("history")).await?;
    let listed: HistoryResult = inner.ask_store(&node, &MetricsRequest::Series(query)).await?;
    let body = serde_json::json!({ "source": node, "series": listed.series, "truncated": listed.truncated });
    Ok(Json(body).into_response())
}

pub(crate) async fn timeline(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let query = EventQuery {
        from_ms: number(&params, "from_ms", 0)?,
        to_ms: number(&params, "to_ms", now_ms())?,
        limit: number(&params, "limit", 25)?,
        node: params.get("node").cloned().unwrap_or_default(),
        kind: params.get("kind").cloned().unwrap_or_default(),
    };
    query.validate().map_err(Failure::bad)?;
    let node = inner.source(params.get("history")).await?;
    let events: Box<RawValue> = inner.ask_store(&node, &MetricsRequest::Events(query)).await?;
    Ok(Json(events).into_response())
}

/// From the metrics store when it keeps traces; otherwise, and when it fails, from what the
/// nodes still hold.
pub(crate) async fn traces(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let wanted = TraceQuery {
        from_ms: number(&params, "from_ms", 0)?,
        to_ms: number(&params, "to_ms", now_ms())?,
        limit: number(&params, "limit", 100)?,
        failed: params.get("failed").is_some_and(|v| v == "true"),
        min_us: number(&params, "min_us", 0)?,
        search: params.get("q").filter(|q| !q.is_empty()).cloned(),
    };
    wanted.validate().map_err(Failure::bad)?;
    // Redacted, a search sees only what the reply shows: the store and the nodes are asked without it.
    let hidden = inner.cfg.redact && wanted.search.is_some();
    let query = match hidden {
        true => TraceQuery { search: None, limit: MAX_TRACE_LIMIT, ..wanted.clone() },
        false => wanted.clone(),
    };
    // A store that keeps no traces is not asked, and its refusal is no error.
    let stored = match inner.store_for(params.get("history"), |c| c.traces).await {
        Ok(Some(node)) => {
            query_metrics::<Vec<TraceSummary>>(&inner.cluster, &node, &MetricsRequest::Traces(query.clone()))
                .await
                .map_err(|error| Some(format!("{node}: {error}")))
        }
        Ok(None) => Err(None),
        Err(failure) => Err(Some(failure.1)),
    };
    let (mut traces, mut body) = match stored {
        Ok(traces) => (traces, serde_json::json!({ "source": "store", "errors": {} })),
        Err(store_error) => {
            let limit = MAX_TRACE_LIMIT;
            let (done, errors) = inner
                .ask_reachable(move |cluster, node| async move {
                    fetch::<Vec<TraceSummary>>(&cluster, &node, &Request::Traces { limit }).await
                })
                .await;
            let traces = query.apply(merge(done.into_iter().flat_map(|(_, part)| part)));
            (traces, serde_json::json!({ "source": "live", "store_error": store_error, "errors": errors }))
        }
    };
    if hidden {
        traces.iter_mut().for_each(|t| [&mut t.root, &mut t.root_actor_name].into_iter().for_each(hide));
        traces = wanted.apply(traces);
    }
    body["traces"] = serde_json::json!(traces);
    Ok(json(body.to_string()))
}

/// The newest dead letters the nodes still hold, and those the metrics store kept.
pub(crate) async fn dead_letters(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let limit = MAX_TRACE_LIMIT;
    let asked = (Request::DeadLetters { limit }, MetricsRequest::DeadLetters { limit });
    let (mut letters, errors): (Vec<DeadLetterRecord>, _) = nodes_and_store(&inner, &params, asked, |_| true).await;
    letters.sort_by(|a, b| (b.at_us, &a.node_id, b.seq).cmp(&(a.at_us, &b.node_id, a.seq)));
    letters.dedup_by(|a, b| (a.at_us, &a.node_id, a.seq) == (b.at_us, &b.node_id, b.seq));
    letters.truncate(limit);
    Ok(json(serde_json::json!({ "letters": letters, "errors": errors }).to_string()))
}

/// What every reachable node answers the first request, and what the history store answers the second, when there is
/// one that has what `needs` reads and it answers.
async fn nodes_and_store<T>(
    inner: &Inner,
    params: &HashMap<String, String>,
    (asked, stored): (Request, MetricsRequest),
    needs: fn(&Capabilities) -> bool,
) -> (Vec<T>, BTreeMap<String, String>)
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let (done, errors) = inner
        .ask_reachable(move |cluster, node| {
            let asked = asked.clone();
            async move { fetch::<Vec<T>>(&cluster, &node, &asked).await }
        })
        .await;
    let mut all: Vec<T> = done.into_iter().flat_map(|(_, part)| part).collect();
    if let Ok(Some(node)) = inner.store_for(params.get("history"), needs).await {
        all.extend(query_metrics::<Vec<T>>(&inner.cluster, &node, &stored).await.unwrap_or_default());
    }
    (all, errors)
}

/// Every span of one trace that a node or the store still has.
pub(crate) async fn trace(State(weak): App, Query(params): Params) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let _slot = inner.slot()?;
    let id = params.get("id").cloned().unwrap_or_default();
    parse_id(&id).map_err(Failure::bad)?;
    let asked = (Request::Trace { trace: id.clone() }, MetricsRequest::Trace { trace: id });
    let (mut spans, errors): (Vec<SpanRecord>, _) = nodes_and_store(&inner, &params, asked, |c| c.traces).await;
    spans.sort_by(|a, b| (a.start_us, &a.span).cmp(&(b.start_us, &b.span)));
    spans.dedup_by(|a, b| a.span == b.span);
    if spans.is_empty() {
        return Err(Failure(StatusCode::NOT_FOUND, "no node or store has this trace".into()));
    }
    Ok(json(serde_json::json!({ "spans": spans, "errors": errors }).to_string()))
}

pub(crate) async fn asset(State(weak): App, request: HttpRequest) -> Result<Response, Failure> {
    let inner = alive(&weak)?;
    let path = request.uri().path().trim_start_matches('/');
    if let Some(file) = ASSETS.get_file(path).filter(|_| path != "index.html") {
        let mime = match path.rsplit('.').next() {
            Some("js") => "text/javascript",
            Some("css") => "text/css",
            Some("svg") => "image/svg+xml",
            Some("woff2") => "font/woff2",
            Some("json") => "application/json",
            Some("png") => "image/png",
            Some("ico") => "image/x-icon",
            _ => "application/octet-stream",
        };
        return Ok(([(header::CONTENT_TYPE, mime)], file.contents()).into_response());
    }
    // The probes live at the root; under the base path the console's page would read as healthy for ever.
    let probe = matches!(path.trim_end_matches('/'), "healthz" | "readyz");
    let page = !probe && (path.rsplit('/').next().is_some_and(|last| !last.contains('.')) || path == "index.html");
    let index = ASSETS.get_file("index.html").and_then(|f| f.contents_utf8());
    match index.filter(|_| page && !path.starts_with("api/") && !path.starts_with("assets/")) {
        Some(index) => {
            let based = index.replacen("<base href=\"/\" />", &format!("<base href=\"{}\" />", inner.cfg.base_path), 1);
            Ok(([(header::CONTENT_TYPE, "text/html; charset=utf-8")], based).into_response())
        }
        None => Err(Failure(StatusCode::NOT_FOUND, "not found".into())),
    }
}
