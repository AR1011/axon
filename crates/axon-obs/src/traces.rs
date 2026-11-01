//! Spans as they leave a node, and the per-trace summaries the trace list is made of.

use std::collections::BTreeMap;

use axon::{Span, SpanKind};
use serde::{Deserialize, Serialize};

pub const MAX_TRACE_LIMIT: usize = 1000;

/// Ids are 16 hex digits: a `u64` does not survive JSON in a browser.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanRecord {
    pub trace: String,
    pub span: String,
    /// Empty on the first span of a trace.
    pub parent: String,
    /// `handle`, `call` or `stream`.
    pub kind: String,
    pub name: String,
    pub node_id: String,
    /// `runtime:slot:generation`, or empty.
    pub actor: String,
    /// The actor's registered name, if it had one and was still running when the span was read.
    #[serde(default)]
    pub actor_name: String,
    pub actor_type: String,
    pub peer: String,
    pub start_us: u64,
    pub wait_ns: u64,
    pub busy_ns: u64,
    pub failed: bool,
}

/// A message a node accepted and never handled, or a group of the same within a second.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub node_id: String,
    pub seq: u64,
    pub at_us: u64,
    pub reason: String,
    /// The message label, or `tell` for one another node sent.
    pub label: String,
    /// `runtime:slot:generation`, or empty.
    pub actor: String,
    pub actor_type: String,
    /// The actor's registered name, or the endpoint a remote message was for.
    pub target: String,
    /// The node that sent it, when another node did.
    pub peer: String,
    /// Empty when none of the messages were traced.
    pub trace: String,
    pub count: u64,
}

impl DeadLetterRecord {
    pub fn new(letter: &axon::DeadLetter, node_id: &str) -> Self {
        Self {
            node_id: node_id.into(),
            seq: letter.seq,
            at_us: letter.at_us,
            reason: letter.reason.into(),
            label: letter.label.into(),
            actor: letter.actor.map(|(id, _)| id.to_string()).unwrap_or_default(),
            actor_type: letter.actor.map(|(_, kind)| kind.to_owned()).unwrap_or_default(),
            target: letter.target.as_deref().unwrap_or_default().into(),
            peer: letter.peer.as_deref().unwrap_or_default().into(),
            trace: if letter.trace == 0 { String::new() } else { hex(letter.trace) },
            count: letter.count,
        }
    }
}

/// A node's settled dead letters after `after`, for the collector.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeadLetterPage {
    pub incarnation: String,
    /// The position to ask after next time.
    pub next: u64,
    pub letters: Vec<DeadLetterRecord>,
}

pub fn hex(id: u64) -> String {
    format!("{id:016x}")
}

pub fn parse_id(text: &str) -> Result<u64, String> {
    u64::from_str_radix(text, 16).ok().filter(|id| *id != 0 && text.len() == 16).ok_or("not a trace id".to_owned())
}

impl SpanRecord {
    pub fn new(span: &Span, node_id: &str, actor_name: &str) -> Self {
        Self {
            trace: hex(span.trace),
            span: hex(span.span),
            parent: if span.parent == 0 { String::new() } else { hex(span.parent) },
            kind: match span.kind {
                SpanKind::Handle => "handle",
                SpanKind::Call => "call",
                SpanKind::Stream => "stream",
            }
            .into(),
            name: span.name.to_string(),
            node_id: node_id.into(),
            actor: span.actor.map(|(id, _)| id.to_string()).unwrap_or_default(),
            actor_name: actor_name.into(),
            actor_type: span.actor.map(|(_, kind)| kind.to_owned()).unwrap_or_default(),
            peer: span.peer.as_deref().unwrap_or_default().into(),
            start_us: span.start_us,
            wait_ns: span.wait.as_nanos() as u64,
            busy_ns: span.busy.as_nanos() as u64,
            failed: span.failed,
        }
    }

    pub fn end_us(&self) -> u64 {
        self.start_us + self.busy_ns / 1000
    }
}

/// A node's spans after `after`, for the collector.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SpanPage {
    pub incarnation: String,
    /// The position to ask after next time.
    pub next: u64,
    pub spans: Vec<SpanRecord>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSummary {
    pub trace: String,
    pub start_us: u64,
    pub end_us: u64,
    pub spans: u64,
    pub failed: bool,
    pub nodes: Vec<String>,
    /// The earliest span without a parent, where whoever summarized had it.
    pub root: String,
    /// The root span's `kind`, `actor_name` and `peer`.
    #[serde(default)]
    pub root_kind: String,
    #[serde(default)]
    pub root_actor_name: String,
    #[serde(default)]
    pub root_peer: String,
    pub root_actor: String,
    pub root_node: String,
    #[serde(default)]
    pub(crate) root_start_us: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TraceQuery {
    pub from_ms: u64,
    pub to_ms: u64,
    pub limit: usize,
    #[serde(default)]
    pub failed: bool,
    /// Traces at least this long.
    #[serde(default)]
    pub min_us: u64,
    /// Matches the trace id, the root's name, actor and peer, and node ids.
    #[serde(default)]
    pub search: Option<String>,
}

impl TraceQuery {
    pub fn validate(&self) -> Result<(), String> {
        if self.from_ms > self.to_ms {
            return Err("from_ms is after to_ms".into());
        }
        if !(1..=MAX_TRACE_LIMIT).contains(&self.limit) {
            return Err(format!("limit must be 1..={MAX_TRACE_LIMIT}"));
        }
        if self.search.as_ref().is_some_and(|s| s.len() > 128) {
            return Err("search is longer than 128 bytes".into());
        }
        Ok(())
    }

    fn admits(&self, t: &TraceSummary) -> bool {
        let text = |needle: &str| {
            let has = |field: &str| field.to_lowercase().contains(needle);
            has(&t.trace)
                || has(&t.root)
                || has(&t.root_actor)
                || has(&t.root_actor_name)
                || has(&t.root_peer)
                || t.nodes.iter().any(|n| has(n))
        };
        (self.from_ms..=self.to_ms).contains(&(t.start_us / 1000))
            && (!self.failed || t.failed)
            && t.end_us - t.start_us >= self.min_us
            && self.search.as_deref().is_none_or(|s| text(&s.to_lowercase()))
    }

    /// Newest first.
    pub fn apply(&self, mut traces: Vec<TraceSummary>) -> Vec<TraceSummary> {
        traces.retain(|t| self.admits(t));
        traces.sort_by(|a, b| (b.start_us, &a.trace).cmp(&(a.start_us, &b.trace)));
        traces.truncate(self.limit);
        traces
    }
}

/// One summary per trace, by trace id. Summaries of the same trace, from nodes that each saw a part, add up.
pub fn summarize<'a>(spans: impl IntoIterator<Item = &'a SpanRecord>) -> Vec<TraceSummary> {
    merge(spans.into_iter().map(|s| {
        let rooted = s.parent.is_empty();
        let pick = |field: &String| if rooted { field.clone() } else { String::new() };
        TraceSummary {
            trace: s.trace.clone(),
            start_us: s.start_us,
            end_us: s.end_us(),
            spans: 1,
            failed: s.failed,
            nodes: vec![s.node_id.clone()],
            root: pick(&s.name),
            root_kind: pick(&s.kind),
            root_actor_name: pick(&s.actor_name),
            root_peer: pick(&s.peer),
            root_actor: pick(&s.actor_type),
            root_node: pick(&s.node_id),
            root_start_us: if rooted { s.start_us } else { u64::MAX },
        }
    }))
}

pub fn merge(parts: impl IntoIterator<Item = TraceSummary>) -> Vec<TraceSummary> {
    let mut traces: BTreeMap<String, TraceSummary> = BTreeMap::new();
    for part in parts {
        let Some(t) = traces.get_mut(&part.trace) else {
            traces.insert(part.trace.clone(), part);
            continue;
        };
        t.start_us = t.start_us.min(part.start_us);
        t.end_us = t.end_us.max(part.end_us);
        t.spans += part.spans;
        t.failed |= part.failed;
        t.nodes.extend(part.nodes);
        t.nodes.sort();
        t.nodes.dedup();
        if part.root_start_us < t.root_start_us {
            t.root_start_us = part.root_start_us;
            (t.root, t.root_kind, t.root_actor_name) = (part.root, part.root_kind, part.root_actor_name);
            (t.root_peer, t.root_actor, t.root_node) = (part.root_peer, part.root_actor, part.root_node);
        }
    }
    traces.into_values().collect()
}

#[test]
fn partial_summaries_add_up_and_the_query_filters_them() {
    let span = |trace: &str, parent: &str, node: &str, start_us: u64, busy_ns: u64, failed: bool| SpanRecord {
        trace: trace.into(),
        span: hex(start_us),
        parent: parent.into(),
        name: format!("at-{start_us}"),
        kind: "call".into(),
        peer: format!("{node}-peer"),
        node_id: node.into(),
        start_us,
        busy_ns,
        failed,
        ..Default::default()
    };
    let a = [span("t1", "", "a", 5_000, 2_000_000, false), span("t1", "", "a", 4_000, 0, false)];
    let b = [span("t1", "x", "b", 6_000, 9_000_000, true), span("t2", "", "b", 9_000, 1_000, false)];
    let all = merge(summarize(&a).into_iter().chain(summarize(&b)));
    let query = TraceQuery { to_ms: 10, limit: 10, ..Default::default() };
    let listed = query.apply(all.clone());
    assert_eq!(listed.iter().map(|t| &*t.trace).collect::<Vec<_>>(), ["t2", "t1"]);
    let t1 = &listed[1];
    assert_eq!((t1.start_us, t1.end_us, t1.spans, t1.failed), (4_000, 15_000, 3, true));
    assert_eq!((&*t1.root, &*t1.root_node, &t1.nodes), ("at-4000", "a", &vec!["a".to_owned(), "b".to_owned()]));
    assert_eq!((&*t1.root_kind, &*t1.root_peer), ("call", "a-peer"));

    let only = |q: TraceQuery| q.apply(all.clone()).into_iter().map(|t| t.trace).collect::<Vec<_>>();
    assert_eq!(only(TraceQuery { failed: true, ..query.clone() }), ["t1"]);
    assert_eq!(only(TraceQuery { min_us: 5_000, ..query.clone() }), ["t1"]);
    assert_eq!(only(TraceQuery { search: Some("AT-9".into()), ..query.clone() }), ["t2"]);
    assert_eq!(only(TraceQuery { search: Some("b-PEER".into()), ..query.clone() }), ["t2"]);
    assert_eq!(only(TraceQuery { from_ms: 5, ..query.clone() }), ["t2"]);
    assert_eq!(only(TraceQuery { limit: 1, ..query.clone() }), ["t2"]);
    assert!(TraceQuery { limit: 0, ..query }.validate().is_err());
    assert_eq!(parse_id(&hex(255)), Ok(255));
    assert!(parse_id("ff").is_err() && parse_id("000000000000000g").is_err());
}
