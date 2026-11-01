use std::borrow::Cow;
use std::collections::BTreeMap;

use axon::{ActorDiagnostics, ActorIncarnation, HistogramSnapshot, RuntimeStats, TypeTotals};
use axon_cluster::{ClusterStats, Hosted, Member};
use serde::{Deserialize, Serialize};

/// A node's incarnation as ids, cursors and pages write it.
pub(crate) fn boot(incarnation: u128) -> String {
    format!("{incarnation:032x}")
}

pub(crate) fn check(ok: bool, message: &str) -> Result<(), String> {
    if ok { Ok(()) } else { Err(message.to_owned()) }
}

pub(crate) fn range(name: &str, value: usize, min: usize, max: usize) -> Result<(), String> {
    if (min..=max).contains(&value) { Ok(()) } else { Err(format!("{name} must be {min}..={max}")) }
}

pub const TELEMETRY_SERVICE: &str = "axon.telemetry";

pub type Str = Cow<'static, str>;

/// A live series has a stored row at least this often: telemetry stores an unchanged series again within half of it,
/// and a history query holds a series' value into steps without a row for no longer than this.
pub const HELD_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Gauge(f64),
    Counter(u64),
    Histogram(HistogramSnapshot),
}

impl Value {
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Gauge(_) => "gauge",
            Value::Counter(_) => "counter",
            Value::Histogram(_) => "histogram",
        }
    }
}

/// One persisted observation. Counters and histograms are cumulative within a node
/// incarnation; a connection's series restart with each session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub name: Str,
    pub unit: Str,
    pub labels: BTreeMap<Str, String>,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SampleBatch {
    pub node_id: String,
    pub incarnation: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub incarnation: String,
    pub sequence: u64,
}

/// A collector refuses a `SamplePage` of another schema instead of misreading it. It starts at 2 because a collector
/// from before the field reads this first byte as the `bool` `reset`, and postcard refuses a 2 there.
pub const SAMPLES_SCHEMA: u8 = 2;

/// Travels as postcard, unlike the other replies: it is the bulk path and no browser reads it.
/// `reset`: the cursor belonged to another incarnation and was ignored. `gap`: batches
/// between the cursor and the first returned batch were evicted or dropped.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SamplePage {
    pub schema: u8,
    pub reset: bool,
    pub gap: bool,
    pub next: Cursor,
    pub batches: Vec<SampleBatch>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ActorQuery {
    /// Only this actor; the other filters still apply.
    #[serde(default)]
    pub id: Option<ActorIncarnation>,
    pub after: Option<ActorIncarnation>,
    pub limit: usize,
    pub actor_type: Option<String>,
    pub phase: Option<String>,
    /// Case-insensitive substring of the name or type.
    pub search: Option<String>,
    pub timings: bool,
    /// `search` sees only what redaction shows of a name: the `kind/` of a `kind/key`.
    #[serde(default)]
    pub redacted: bool,
}

/// What redaction leaves of a name: `kind/` of a `kind/key`, and all of any other.
pub(crate) fn shown(name: &str) -> &str {
    name.find('/').map_or(name, |slash| &name[..=slash])
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    Snapshot,
    Actors(ActorQuery),
    /// One session's `{ timings, request_rtt }`, or null when this node has no such session.
    Connection {
        peer: String,
        outgoing: bool,
        lane: axon::Lane,
    },
    Samples {
        after: Option<Cursor>,
    },
    /// Per-stream positions and recent stream events; empty when no stream node is attached.
    Streams {
        limit: usize,
    },
    /// A `SpanPage`: the spans recorded after position `after` of boot `incarnation`.
    Spans {
        incarnation: String,
        after: u64,
        limit: usize,
    },
    /// `Vec<TraceSummary>` of the newest traces in this node's span ring, from its own spans alone.
    Traces {
        limit: usize,
    },
    /// This node's `Vec<SpanRecord>` of one trace.
    Trace {
        trace: String,
    },
    /// This node's newest `Vec<DeadLetterRecord>`.
    DeadLetters {
        limit: usize,
    },
    /// A `DeadLetterPage`: the letters after position `after` of boot `incarnation` that no longer change.
    SettledDeadLetters {
        incarnation: String,
        after: u64,
        limit: usize,
    },
    /// An `EventPage`: the node's timeline events after position `after` of boot `incarnation`.
    Events {
        incarnation: String,
        after: u64,
        limit: usize,
    },
    /// A `KeyPage`: the keys of `kind` here that start with `prefix`, in key order from `from` on, `from` included. A
    /// node from before it answers `bad request: unknown variant`.
    Keys {
        kind: String,
        prefix: String,
        from: Option<String>,
        limit: usize,
    },
}

/// A node's keys of one kind, from `Request::Keys`. `more`: keys follow that the page left out.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KeyPage {
    pub keys: Vec<ListedKey>,
    pub more: bool,
}

/// A key and the lease epoch its actor started under; `None` without an ownership authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedKey {
    pub key: String,
    pub epoch: Option<u64>,
}

/// A node's events after `after`, for the collector.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EventPage {
    pub incarnation: String,
    /// The position to ask after next time.
    pub next: u64,
    /// Events of the same boot the node's ring let go of before they were asked for.
    pub missed: u64,
    pub events: Vec<crate::provider::ClusterEvent>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ActorPage {
    pub actors: Vec<ActorDiagnostics>,
    pub total: usize,
    pub next: Option<ActorIncarnation>,
    /// The scan budget ran out before `limit` matches were found; continue from `next`.
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ActorTypeSnapshot {
    pub actor_type: &'static str,
    pub live: u64,
    pub mailbox_depth: u64,
    pub mailbox_depth_max: u64,
    pub mailbox_bytes: u64,
    pub waiters: u64,
    pub requests: u64,
    pub oldest_message_age_ns: u64,
    /// Live plus exited actors.
    pub totals: TypeTotals,
}

#[derive(Clone, Debug, Serialize)]
pub struct Lifecycle {
    pub phase: &'static str,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessSnapshot {
    pub pid: u32,
    pub cpu_percent: Option<f32>,
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct HostSnapshot {
    pub cpu_percent: Option<f32>,
    pub cpus: usize,
    pub total_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub received_bytes: u64,
    pub transmitted_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Notice {
    pub code: &'static str,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SystemCounters {
    pub started: u64,
    pub stopped: u64,
    pub failed: u64,
    pub dead_letters: u64,
}

/// The live view of one node, per actor type: ask for single actors with `Request::Actors`.
#[derive(Clone, Debug, Serialize)]
pub struct NodeSnapshot {
    pub node_id: String,
    pub incarnation: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub uptime_ms: u64,
    pub lifecycle: Lifecycle,
    pub system: SystemCounters,
    pub runtime: RuntimeStats,
    /// Busy fraction per worker over the last interval; empty on the first sample.
    pub worker_utilisation: Vec<f64>,
    /// The same for each of `runtime.pools`, in its order.
    pub pool_utilisation: Vec<Vec<f64>>,
    pub actor_types: Vec<ActorTypeSnapshot>,
    pub actor_total: usize,
    /// Each peer's session timings and request round trips are left out: `Request::Connection` asks for one session's.
    #[serde(serialize_with = "without_timings")]
    pub cluster: ClusterStats,
    pub members: Vec<Member>,
    /// The kinds registered here with their keys counted, the first `snapshot_keys` keys here, and how many there are.
    pub hosted: Hosted,
    /// Present once a stream node is attached with `Telemetry::watch_streams`.
    pub streams: Option<axon_stream::RelayStats>,
    pub process: Option<ProcessSnapshot>,
    pub host: Option<HostSnapshot>,
    pub notices: Vec<Notice>,
}

fn without_timings<S: serde::Serializer>(stats: &ClusterStats, to: S) -> Result<S::Ok, S::Error> {
    let mut value = serde_json::to_value(stats).map_err(serde::ser::Error::custom)?;
    let peers = value.pointer_mut("/engine/peers").and_then(serde_json::Value::as_array_mut);
    for peer in peers.into_iter().flatten().filter_map(serde_json::Value::as_object_mut) {
        peer.remove("request_rtt");
        if let Some(session) = peer.get_mut("session").and_then(serde_json::Value::as_object_mut) {
            session.remove("timings");
        }
    }
    value.serialize(to)
}

#[test]
fn a_sample_page_carries_its_schema_and_histograms_sparse() {
    let mut h = axon::Histogram::default();
    for ns in [3, 5, 5] {
        h.record(std::time::Duration::from_nanos(ns));
    }
    let sample =
        Sample { name: "h".into(), unit: "ns".into(), labels: BTreeMap::new(), value: Value::Histogram((&h).into()) };
    let batch = SampleBatch {
        node_id: "n".into(),
        incarnation: "i".into(),
        sequence: 1,
        timestamp_ms: 0,
        samples: vec![sample],
    };
    let next = Cursor { incarnation: "i".into(), sequence: 1 };
    let page = SamplePage { schema: SAMPLES_SCHEMA, reset: false, gap: false, next, batches: vec![batch] };
    // A change here is a wire change: bump SAMPLES_SCHEMA.
    let bytes = postcard::to_stdvec(&page).expect("encodes");
    let histogram = [2, 3, 3, 1, 0, 2, 3, 13]; // Histogram variant, first, counts, count, sum_ns
    assert_eq!(
        bytes,
        [&[2, 0, 0, 1, 105, 1, 1, 1, 110, 1, 105, 1, 0, 1, 1, 104, 2, 110, 115, 0][..], &histogram].concat()
    );
    assert_eq!(postcard::from_bytes::<SamplePage>(&bytes).ok(), Some(page.clone()));
    assert_eq!(serde_json::from_slice::<SamplePage>(&serde_json::to_vec(&page).expect("encodes")).ok(), Some(page));
    // A collector from before the schema byte read it as the bool `reset`.
    assert!(postcard::from_bytes::<(bool, bool)>(&bytes).is_err());
    let past = r#"{"first":300,"counts":[1,1,1,1,1],"count":5,"sum_ns":5}"#;
    assert!(serde_json::from_str::<HistogramSnapshot>(past).is_err());
}
