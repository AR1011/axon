use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;

use axon::{Histogram, HistogramSnapshot};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, Serializer};

use crate::model::{HELD_MS, SampleBatch, Str, Value, check};
use crate::traces::{DeadLetterRecord, SpanRecord, TraceQuery, TraceSummary, hex, summarize};

pub type MetricsFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Writes are idempotent: a batch is identified by `(node_id, incarnation, sequence)` and an
/// event by `id`, and a retry carries identical content.
pub trait MetricsSink: Send + Sync + 'static {
    /// Names the sink in `MetricsHealth`.
    fn name(&self) -> &str {
        "sink"
    }

    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()>;
    /// The highest batch sequence under [`OWN_SEQUENCE`](crate::OWN_SEQUENCE) kept for that boot of the node,
    /// where the sink can tell. The collector resumes after it when it holds no position of its own or a
    /// write's outcome is unknown, so a sink that sums what it is given is not given the same batch twice.
    fn stored_through(&self, _node: &str, _incarnation: &str) -> MetricsFuture<'_, Option<u64>> {
        Box::pin(std::future::ready(Ok(None)))
    }
    fn write_events(&self, events: &[ClusterEvent]) -> MetricsFuture<'_, ()>;
    /// The same span may be written again; a sink that keeps spans keeps one per `(trace, span)`.
    fn write_spans(&self, _spans: &[SpanRecord]) -> MetricsFuture<'_, ()> {
        Box::pin(std::future::ready(Ok(())))
    }
    /// The same letter may be written again; a sink that keeps them keeps one per `(node_id, at_us, seq)`.
    fn write_dead_letters(&self, _letters: &[DeadLetterRecord]) -> MetricsFuture<'_, ()> {
        Box::pin(std::future::ready(Ok(())))
    }
    /// What the sink holds, where it can tell, shown in [`MetricsHealth::sink_usage`](crate::MetricsHealth). The
    /// collector works out the growth and when it fills. Asked whenever health is, so it must not wait on a write or a
    /// query.
    fn usage(&self) -> Option<SinkUsage> {
        None
    }
}

/// What a sink holds: against its bound, per table, and how soon it fills at the rate it grows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkUsage {
    /// 0 where the sink sets no bound of its own.
    pub max_bytes: u64,
    pub held_bytes: u64,
    pub held_batches: u64,
    /// Let go, oldest first, to stay under `max_bytes`.
    pub evicted_batches: u64,
    #[serde(default)]
    pub tables: Vec<TableUsage>,
    /// The room left: under `max_bytes`, or on the disks the database keeps its tables on.
    #[serde(default)]
    pub free_bytes: Option<u64>,
    /// Set by the collector: what the tables still filling gain per day, each over the span it holds, once one holds an
    /// hour.
    #[serde(default)]
    pub growth_bytes_per_day: Option<u64>,
    /// Set by the collector: `free_bytes` at that rate; `None` when retention levels every table off within them.
    #[serde(default)]
    pub full_in_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableUsage {
    pub name: String,
    pub rows: u64,
    /// On disk, or in memory.
    pub held_bytes: u64,
    /// Over `held_bytes`, the compression ratio.
    pub uncompressed_bytes: u64,
    pub parts: u64,
    /// The span it holds, Unix ms, no later than the store's clock.
    pub first_ms: Option<u64>,
    pub last_ms: Option<u64>,
    /// Its TTL; `None` where only a size bounds it.
    pub retention_days: Option<u32>,
}

impl SinkUsage {
    pub(crate) fn with_growth(mut self) -> Self {
        const DAY_MS: u128 = 86_400_000;
        // (rate, table), and a rate of 0 once a table spans all its TTL keeps.
        let rates: Vec<_> = (self.tables.iter())
            .filter_map(|t| {
                let span = t.last_ms?.checked_sub(t.first_ms?)? as u128;
                let kept = t.retention_days.map_or(u128::MAX, |d| d as u128 * DAY_MS);
                if span < DAY_MS / 24 {
                    return None;
                }
                Some((if span < kept { (t.held_bytes as u128 * DAY_MS / span) as u64 } else { 0 }, t))
            })
            .collect();
        let growth: u64 = rates.iter().map(|(rate, _)| rate).sum();
        // A table with a TTL levels off at its rate times its retention.
        let to_grow: Option<u64> = (rates.iter())
            .map(|(rate, t)| Some(rate.saturating_mul(t.retention_days? as u64).saturating_sub(t.held_bytes)))
            .sum();
        // ponytail: every table grows at its rate until free runs out, though one with a TTL may level off first; the
        // forecast errs early.
        self.growth_bytes_per_day = (!rates.is_empty()).then_some(growth);
        let fills = |free: &u64| growth > 0 && to_grow.is_none_or(|left| left > *free);
        let full_in = |free| u64::try_from(free as u128 * DAY_MS / growth as u128).unwrap_or(u64::MAX);
        self.full_in_ms = self.free_bytes.filter(fills).map(full_in);
        self
    }
}

pub trait MetricsQuery: Send + Sync + 'static {
    fn capabilities(&self) -> Capabilities;
    fn query(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult>;
    /// A page of charts, answered in order. A provider that can share the work between them overrides it.
    fn queries(&self, queries: &[HistoryQuery]) -> MetricsFuture<'_, Vec<Result<HistoryResult, String>>> {
        let running: Vec<_> = queries.iter().map(|q| self.query(q)).collect();
        Box::pin(async move { Ok(futures_util::future::join_all(running).await) })
    }
    /// The series of `query.metric` stored in its range, their headers without values (`count` 0); its step, its
    /// aggregation and its sum do not matter.
    fn series(&self, _query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        Box::pin(std::future::ready(Err("this provider lists no series".into())))
    }
    /// Newest first.
    fn events(&self, query: &EventQuery) -> MetricsFuture<'_, Vec<ClusterEvent>>;
    /// Newest first.
    fn traces(&self, _query: &TraceQuery) -> MetricsFuture<'_, Vec<TraceSummary>> {
        Box::pin(std::future::ready(Err(NO_TRACES.into())))
    }
    fn trace(&self, _trace: u64) -> MetricsFuture<'_, Vec<SpanRecord>> {
        Box::pin(std::future::ready(Err(NO_TRACES.into())))
    }
    /// Newest first.
    fn dead_letters(&self, _limit: usize) -> MetricsFuture<'_, Vec<DeadLetterRecord>> {
        Box::pin(std::future::ready(Err("this provider keeps no dead letters".into())))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub numeric_history: bool,
    pub histograms: bool,
    pub events: bool,
    #[serde(default)]
    pub traces: bool,
    pub max_points: usize,
    pub storage: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterEvent {
    pub id: String,
    pub time_ms: u64,
    pub node_id: String,
    pub kind: String,
    /// What it happened to: a keyed actor's `kind/key`, a supervised family, a stream; empty for a node's own.
    #[serde(default)]
    pub subject: String,
    pub detail: String,
}

/// The stored events between two times, newest first; `node` and `kind`, unless empty, must match exactly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventQuery {
    pub from_ms: u64,
    pub to_ms: u64,
    pub limit: usize,
    #[serde(default)]
    pub node: String,
    #[serde(default)]
    pub kind: String,
}

impl Default for EventQuery {
    /// Every event, up to [`MAX_EVENTS`].
    fn default() -> Self {
        Self { from_ms: 0, to_ms: u64::MAX, limit: MAX_EVENTS, node: String::new(), kind: String::new() }
    }
}

impl EventQuery {
    pub fn validate(&self) -> Result<(), String> {
        if self.to_ms < self.from_ms {
            return Err("to_ms must not precede from_ms".into());
        }
        if !(1..=MAX_EVENTS).contains(&self.limit) {
            return Err("limit must be 1..=1000".into());
        }
        if self.node.len() > 256 || self.kind.len() > 256 {
            Err("node and kind must be at most 256 bytes".into())
        } else {
            Ok(())
        }
    }

    pub fn matches(&self, event: &ClusterEvent) -> bool {
        let is = |filter: &str, value: &str| filter.is_empty() || filter == value;
        (self.from_ms..=self.to_ms).contains(&event.time_ms)
            && is(&self.node, &event.node_id)
            && is(&self.kind, &event.kind)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    #[default]
    Last,
    Mean,
    Min,
    Max,
}

pub const MAX_POINTS: usize = 10_000;
pub(crate) const NO_TRACES: &str = "this provider keeps no traces";
pub const MAX_SERIES: usize = 128;
pub const MAX_EVENTS: usize = 1000;
pub const MAX_BATCHES_PER_WRITE: usize = 64;

/// Series added up per step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sum {
    /// One series per node boot.
    Node,
    Cluster,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryQuery {
    pub metric: String,
    pub from_ms: u64,
    pub to_ms: u64,
    pub step_ms: u64,
    /// Histograms ignore this: a step always holds the last cumulative histogram in it.
    #[serde(default)]
    pub aggregation: Aggregation,
    #[serde(default)]
    pub node: Option<String>,
    /// A series matches when it carries every one of these.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// What the series read may hold: their known values in all, and how many.
    #[serde(default = "default_points")]
    pub max_points: usize,
    #[serde(default = "default_series")]
    pub max_series: usize,
    /// Answers the series read added up, keeping apart the values of the labels in `by`.
    #[serde(default)]
    pub sum: Option<Sum>,
    #[serde(default)]
    pub by: Vec<String>,
}

fn default_points() -> usize {
    2000
}

fn default_series() -> usize {
    32
}

impl HistoryQuery {
    pub fn new(metric: impl Into<String>, from_ms: u64, to_ms: u64, step_ms: u64) -> Self {
        Self {
            metric: metric.into(),
            from_ms,
            to_ms,
            step_ms,
            aggregation: Aggregation::Last,
            node: None,
            labels: BTreeMap::new(),
            max_points: default_points(),
            max_series: default_series(),
            sum: None,
            by: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        check((1..=128).contains(&self.metric.len()), "metric must be 1..=128 bytes")?;
        check(self.to_ms >= self.from_ms, "to_ms must not precede from_ms")?;
        check(self.to_ms - self.from_ms <= 366 * 86_400_000, "the range must not exceed 366 days")?;
        check((50..=86_400_000).contains(&self.step_ms), "step_ms must be 50..=86400000")?;
        check(self.steps() <= MAX_POINTS, "the range must hold at most 10000 steps")?;
        check((1..=MAX_POINTS).contains(&self.max_points), "max_points must be 1..=10000")?;
        check((1..=MAX_SERIES).contains(&self.max_series), "max_series must be 1..=128")?;
        check(self.node.as_ref().is_none_or(|n| n.len() <= 256), "node must be at most 256 bytes")?;
        check(self.labels.len() <= 8, "at most 8 label filters")?;
        check(self.labels.iter().all(|(k, v)| k.len() <= 64 && v.len() <= 256), "label filter too long")?;
        check(self.by.len() <= 8 && self.by.iter().all(|b| b.len() <= 64), "by names at most 8 labels")?;
        check(self.sum.is_some() || self.by.is_empty(), "by keeps labels apart in a sum")
    }

    /// The first step: `from_ms` in whole steps, so it may reach back before `from_ms` by less than one.
    pub(crate) fn start_ms(&self) -> u64 {
        self.from_ms / self.step_ms * self.step_ms
    }

    /// Where a provider reads from: the step before the first, which the first step's gain is over.
    pub fn read_from_ms(&self) -> u64 {
        self.start_ms().saturating_sub(self.step_ms)
    }

    pub(crate) fn steps(&self) -> usize {
        usize::try_from((self.to_ms.saturating_sub(self.start_ms())) / self.step_ms.max(1) + 1).unwrap_or(usize::MAX)
    }

    pub(crate) fn carried_by(&self, labels: &[(Str, String)]) -> bool {
        self.labels.iter().all(|(k, v)| labels.iter().any(|(sk, sv)| sk == k && sv == v))
    }
}

/// One step of a series: a gauge's value, or what a counter or a histogram gained over the step.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Reading {
    Number(f64),
    Histogram(HistogramSnapshot),
}

impl Serialize for Reading {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            // Counters and most gauges are whole: `1234`, not `1234.0`.
            Reading::Number(n) if n.fract() == 0.0 && n.abs() < 9e15 => s.serialize_i64(*n as i64),
            Reading::Number(n) => s.serialize_f64(*n),
            Reading::Histogram(h) => h.serialize(s),
        }
    }
}

/// A stored series, or a sum of them, over its answer's steps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Series {
    /// Empty in a sum over the cluster.
    pub node_id: String,
    pub incarnation: String,
    pub labels: BTreeMap<String, String>,
    pub kind: String,
    pub unit: String,
    /// How many stored series a sum added up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summed: Option<usize>,
    /// One per step; `None` where unknown: no value, or a gain whose step before has none or which went back, as
    /// across a restart.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Option<Reading>>,
}

/// Step `i` of every series starts at `from + i * step`, Unix ms.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HistoryResult {
    /// `from_ms` down to a whole step.
    pub from: u64,
    pub step: u64,
    pub count: usize,
    pub series: Vec<Series>,
    /// `max_points` or `max_series` left out the series after the last one here.
    pub truncated: bool,
    /// The steps before it no longer change, but for a node more than `HELD_MS` behind the newest one stored.
    pub settled: u64,
}

/// One aggregated step of one series, as a provider reads it back. `labels` is the canonical JSON object the series
/// was stored under.
pub struct Row<'a> {
    pub node_id: &'a str,
    pub incarnation: &'a str,
    pub labels: &'a str,
    pub kind: &'a str,
    pub unit: &'a str,
    pub time_ms: u64,
    /// The step's value under the query's aggregation; a histogram's count.
    pub value: f64,
    /// The last value in the step, whatever the aggregation: what the steps after it hold until the next row.
    pub held: f64,
    /// A histogram's last in the step.
    pub histogram: Option<&'a HistogramSnapshot>,
    /// The newest batch stored from this node incarnation, 0 when unknown.
    pub through: u64,
}

/// Steps rows ordered by `(node_id, incarnation, labels, kind, unit, time)` into the answer. A provider reads from
/// [`HistoryQuery::read_from_ms`], and the last row of each series from up to [`HELD_MS`] before that, which only
/// carries its value in. A series is stored when it changes, so a step without a row holds the value of the row
/// before it: for less than [`HELD_MS`] after that row's step, and not past `through`.
pub fn assemble<'a>(query: &HistoryQuery, rows: impl IntoIterator<Item = Row<'a>>) -> HistoryResult {
    let (from, step, count) = (query.start_ms(), query.step_ms, query.steps());
    // Slot `k` is the step at `from + (k - 1) * step`, slot 0 the one before the range.
    let slot = |t: u64| (t + step).checked_sub(from).map(|d| (d / step) as usize).filter(|&k| k <= count);
    type Slot<'a> = Option<(f64, Option<&'a HistogramSnapshot>)>;
    let hold =
        |at: &mut [Slot<'a>], (time_ms, held, histogram): (u64, f64, Option<&'a HistogramSnapshot>), before: u64| {
            let mut t = (time_ms + step).max(from.saturating_sub(step));
            while t < before.min(time_ms + HELD_MS) {
                if let Some(k) = slot(t) {
                    at[k] = Some((held, histogram));
                }
                t += step;
            }
        };
    let mut result = HistoryResult { from, step, count, ..Default::default() };
    let (mut room, mut newest) = (query.max_points, None::<u64>);
    let mut rows = rows.into_iter().peekable();
    while let Some(first) = rows.next() {
        let key = (first.node_id, first.incarnation, first.labels, first.kind, first.unit);
        let until = first.through.min(query.to_ms).saturating_add(1);
        newest = newest.max(Some(first.through));
        let mut at: Vec<Slot> = vec![None; count + 1];
        let mut last = None;
        let rest = std::iter::from_fn(|| rows.next_if(|r| (r.node_id, r.incarnation, r.labels, r.kind, r.unit) == key));
        for row in std::iter::once(first).chain(rest) {
            if let Some(last) = last {
                hold(&mut at, last, row.time_ms);
            }
            last = Some((row.time_ms, row.held, row.histogram));
            if let Some(k) = slot(row.time_ms) {
                at[k] = Some((row.value, row.histogram));
            }
        }
        if let Some(last) = last {
            hold(&mut at, last, until);
        }
        let value = |k: usize| at[k].map(|(v, _)| v);
        let values: Vec<_> = (1..=count)
            .map(|k| match key.3 {
                "histogram" => {
                    let (Some((_, Some(a))), Some((_, Some(b)))) = (at[k - 1], at[k]) else { return None };
                    Histogram::from(b).since(&a.into()).map(|gained| Reading::Histogram((&gained).into()))
                }
                "counter" => {
                    let (a, b) = (value(k - 1)?, value(k)?);
                    (b >= a).then_some(Reading::Number(b - a))
                }
                _ => value(k).map(Reading::Number),
            })
            .collect();
        let known = values.iter().flatten().count();
        if known == 0 {
            continue;
        }
        if result.series.len() == query.max_series || known > room {
            result.truncated = true;
            break;
        }
        room -= known;
        let (node_id, incarnation, labels, kind, unit) = key;
        result.series.push(Series {
            node_id: node_id.into(),
            incarnation: incarnation.into(),
            labels: serde_json::from_str(labels).unwrap_or_default(),
            kind: kind.into(),
            unit: unit.into(),
            summed: None,
            values,
        });
    }
    let edge = query.to_ms.saturating_add(1);
    result.settled = newest.map_or(edge, |t| t.min(edge)).saturating_sub(HELD_MS) / step * step;
    if let Some(sum) = query.sum {
        result.series = summed(std::mem::take(&mut result.series), sum, &query.by, count);
    }
    result
}

/// Each step's known values added up, per node boot or over the cluster, and per value of the labels in `by`. A sum
/// ends at the last step every node still reporting reached: past it, one yet to report would read as a drop.
fn summed(series: Vec<Series>, sum: Sum, by: &[String], count: usize) -> Vec<Series> {
    let mut groups = BTreeMap::<_, Vec<Series>>::new();
    for s in series {
        let labels: BTreeMap<_, _> =
            s.labels.iter().filter(|(k, _)| by.contains(k)).map(|(k, v)| (k.clone(), v.clone())).collect();
        let (node, boot) = match sum {
            Sum::Node => (s.node_id.clone(), s.incarnation.clone()),
            Sum::Cluster => Default::default(),
        };
        groups.entry((node, boot, labels, s.kind.clone(), s.unit.clone())).or_default().push(s);
    }
    let sums = groups.into_iter().map(|((node_id, incarnation, labels, kind, unit), members)| {
        let mut reached = BTreeMap::new();
        for m in &members {
            if let Some(last) = m.values.iter().rposition(Option::is_some) {
                let at = reached.entry(&m.node_id).or_insert(last);
                *at = last.max(*at);
            }
        }
        let newest = reached.values().max().copied().unwrap_or(0);
        let until = reached.values().copied().filter(|&r| r + 2 >= newest).min().unwrap_or(0);
        let values = (0..count)
            .map(|i| {
                let mut known = members.iter().filter_map(|m| m.values[i].as_ref()).filter(|_| i <= until).peekable();
                known.peek()?;
                Some(if kind == "histogram" {
                    let mut total = Histogram::default();
                    known.for_each(|r| {
                        if let Reading::Histogram(gained) = r {
                            total.merge(&gained.into())
                        }
                    });
                    Reading::Histogram((&total).into())
                } else {
                    Reading::Number(known.map(|r| if let Reading::Number(n) = r { *n } else { 0.0 }).sum())
                })
            })
            .collect();
        Series { node_id, incarnation, labels, kind, unit, summed: Some(members.len()), values }
    });
    sums.collect()
}

/// The indices under each distinct key, keys in the order they first come. A list, not a map: keys are few, and `==`
/// turns most away on their length.
pub(crate) fn groups<K: PartialEq>(keyed: impl IntoIterator<Item = (usize, K)>) -> Vec<(K, Vec<usize>)> {
    let mut groups: Vec<(K, Vec<usize>)> = Vec::new();
    for (i, key) in keyed {
        match groups.iter_mut().find(|(k, _)| *k == key) {
            Some((_, members)) => members.push(i),
            None => groups.push((key, vec![i])),
        }
    }
    groups
}

pub fn validate_batches(batches: &[SampleBatch]) -> Result<(), String> {
    check(batches.len() <= MAX_BATCHES_PER_WRITE, "at most 64 batches per write")?;
    for batch in batches {
        check((1..=256).contains(&batch.node_id.len()), "node_id must be 1..=256 bytes")?;
        check((1..=64).contains(&batch.incarnation.len()), "incarnation must be 1..=64 bytes")?;
        check(batch.sequence != 0, "sequence must not be 0")?;
        check(batch.samples.len() <= 16384, "at most 16384 samples per batch")?;
        for sample in &batch.samples {
            check((1..=128).contains(&sample.name.len()), "sample name must be 1..=128 bytes")?;
            check(sample.unit.len() <= 32, "sample unit must be at most 32 bytes")?;
            check(!matches!(sample.value, Value::Gauge(v) if !v.is_finite()), "gauge must be finite")?;
            check(sample.labels.len() <= 8, "at most 8 labels per sample")?;
            check(sample.labels.iter().all(|(k, v)| k.len() <= 64 && v.len() <= 256), "label too long")?;
        }
    }
    Ok(())
}

pub fn validate_events(events: &[ClusterEvent]) -> Result<(), String> {
    check(events.len() <= 1024, "at most 1024 events per write")?;
    for event in events {
        check((1..=512).contains(&event.id.len()), "event id must be 1..=512 bytes")?;
        check((1..=64).contains(&event.kind.len()), "event kind must be 1..=64 bytes")?;
        check(event.node_id.len() <= 256, "event node_id must be at most 256 bytes")?;
        check(event.subject.len() <= 1024, "event subject must be at most 1024 bytes")?;
        check(event.detail.len() <= 4096, "event detail must be at most 4096 bytes")?;
    }
    Ok(())
}

/// A batch as `MemoryMetrics` keeps it, `bytes` its size in memory.
struct Kept {
    node_id: String,
    incarnation: String,
    sequence: u64,
    timestamp_ms: u64,
    /// On the collector's clock, which a node's skew cannot stretch.
    stored_ms: u64,
    samples: Box<[KeptSample]>,
    bytes: usize,
}

/// Labels in a slice rather than a B-tree, whose one node takes over 500 B.
struct KeptSample {
    name: Str,
    unit: Str,
    labels: Box<[(Str, String)]>,
    value: Value,
}

/// What asking for `n` bytes takes from a malloc like glibc's: a word of header, in 16 B steps, 32 B at least. Counting
/// only what is asked for left the process 1.5 times the bound (`scale metrics`).
fn heap(n: usize) -> usize {
    if n == 0 { 0 } else { (n + 8).next_multiple_of(16).max(32) }
}

impl Kept {
    fn new(batch: &SampleBatch) -> Self {
        let text = |s: &Str| if let Cow::Owned(s) = s { heap(s.len()) } else { 0 };
        let mut bytes = size_of::<Self>() + size_of::<(String, String, u64)>();
        // the ids are kept twice: here and in `batch_ids`
        bytes += 2 * (heap(batch.node_id.len()) + heap(batch.incarnation.len()));
        bytes += heap(batch.samples.len() * size_of::<KeptSample>());
        let samples = batch.samples.iter().map(|sample| {
            if let Value::Histogram(h) = &sample.value {
                // one allocation: two reference counts, then `[first, count, sum_ns, the run of buckets..]`
                bytes += heap((5 + h.counts().len()) * size_of::<u64>());
            }
            let labels: Box<[_]> = sample.labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            bytes += text(&sample.name) + text(&sample.unit) + heap(size_of_val(&*labels));
            bytes += labels.iter().map(|(k, v)| text(k) + heap(v.len())).sum::<usize>();
            KeptSample { name: sample.name.clone(), unit: sample.unit.clone(), labels, value: sample.value.clone() }
        });
        let samples = samples.collect();
        let (node_id, incarnation) = (batch.node_id.clone(), batch.incarnation.clone());
        let (sequence, timestamp_ms, stored_ms) = (batch.sequence, batch.timestamp_ms, crate::metrics::now_ms());
        Kept { node_id, incarnation, sequence, timestamp_ms, stored_ms, samples, bytes }
    }
}

/// Serializes as the `BTreeMap` it was taken from.
struct Labels<'a>(&'a [(Str, String)]);

impl Serialize for Labels<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().map(|(k, v)| (k, v)))
    }
}

#[derive(Default)]
struct Stored {
    batches: VecDeque<Kept>,
    batch_ids: HashSet<(String, String, u64)>,
    held_bytes: usize,
    held_samples: u64,
    evicted_batches: u64,
    events: VecDeque<ClusterEvent>,
    event_ids: HashSet<String>,
    spans: VecDeque<SpanRecord>,
    span_ids: HashSet<(String, String)>,
    letters: VecDeque<DeadLetterRecord>,
}

const MEMORY_EVENTS: usize = 1 << 10;

const MEMORY_LETTERS: usize = 1 << 12;

const MEMORY_SPANS: usize = 1 << 16;

/// Bounded volatile history: for tests, and for a dashboard with no database behind it. Sample batches are bounded by
/// their size in memory; events, spans and dead letters by count.
pub struct MemoryMetrics {
    max_bytes: usize,
    stored: Mutex<Stored>,
}

impl MemoryMetrics {
    /// Holds at most `max_bytes` of sample batches. A bound below one batch holds nothing: each batch is let go as it
    /// comes, and `SinkUsage::evicted_batches` shows it.
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes, stored: Mutex::default() }
    }

    /// Every query in one pass over what it holds, under one lock, in order.
    fn answer(&self, queries: &[HistoryQuery]) -> Vec<Result<HistoryResult, String>> {
        // Per series, per step.
        type Steps<'a> =
            BTreeMap<(&'a str, &'a str, &'a [(Str, String)], &'static str, &'a str), BTreeMap<u64, Step<'a>>>;
        let checked: Vec<_> = queries.iter().map(HistoryQuery::validate).collect();
        let wanted =
            groups((queries.iter().enumerate()).filter(|(i, _)| checked[*i].is_ok()).map(|(i, q)| (i, &*q.metric)));
        let carries: Vec<_> = (queries.iter().zip(&checked))
            .map(|(query, checked)| checked.is_ok().then(|| query.read_from_ms().saturating_sub(HELD_MS)..=query.to_ms))
            .collect();
        let read = {
            let stored = self.stored.lock();
            let mut through = HashMap::new();
            let mut steps: Vec<Steps> = queries.iter().map(|_| BTreeMap::new()).collect();
            let mut reads = vec![false; queries.len()];
            for batch in &stored.batches {
                let mut from_node = false;
                for ((read, query), carry) in reads.iter_mut().zip(queries).zip(&carries) {
                    let node = carry.is_some() && query.node.as_ref().is_none_or(|n| *n == batch.node_id);
                    *read = node && carry.as_ref().is_some_and(|carry| carry.contains(&batch.timestamp_ms));
                    from_node |= node;
                }
                if from_node {
                    let at = through.entry((&*batch.node_id, &*batch.incarnation)).or_insert(0);
                    *at = batch.timestamp_ms.max(*at);
                }
                if !reads.contains(&true) {
                    continue;
                }
                for sample in &batch.samples {
                    let Some((_, asking)) = wanted.iter().find(|(metric, _)| **metric == *sample.name) else {
                        continue;
                    };
                    for &i in asking {
                        let query = &queries[i];
                        if !reads[i] || !query.carried_by(&sample.labels) {
                            continue;
                        }
                        let (value, histogram) = match &sample.value {
                            Value::Gauge(v) => (*v, None),
                            Value::Counter(v) => (*v as f64, None),
                            Value::Histogram(h) => (h.count() as f64, Some(h)),
                        };
                        let series =
                            (&*batch.node_id, &*batch.incarnation, &*sample.labels, sample.value.kind(), &*sample.unit);
                        let time = batch.timestamp_ms / query.step_ms * query.step_ms;
                        let at = (batch.timestamp_ms, batch.sequence);
                        let step = steps[i].entry(series).or_default().entry(time).or_insert(Step {
                            at,
                            last: value,
                            histogram: None,
                            sum: 0.0,
                            count: 0.0,
                            min: value,
                            max: value,
                        });
                        if at >= step.at {
                            (step.at, step.last, step.histogram) = (at, value, histogram);
                        }
                        (step.sum, step.count) = (step.sum + value, step.count + 1.0);
                        (step.min, step.max) = (step.min.min(value), step.max.max(value));
                    }
                }
            }
            // Owned before the lock goes, each series' header once; a histogram is a reference count.
            let owned = (queries.iter().zip(steps)).map(|(query, steps)| {
                let series = steps.into_iter().map(|((node_id, incarnation, labels, kind, unit), steps)| {
                    let through = through.get(&(node_id, incarnation)).copied().unwrap_or(0);
                    let labels = serde_json::to_string(&Labels(labels)).unwrap_or_default();
                    let steps = steps.into_iter().map(|(time_ms, step)| {
                        let value = match (query.aggregation, &step.histogram) {
                            (Aggregation::Last, _) | (_, Some(_)) => step.last,
                            (Aggregation::Mean, _) => step.sum / step.count,
                            (Aggregation::Min, _) => step.min,
                            (Aggregation::Max, _) => step.max,
                        };
                        (time_ms, value, step.last, step.histogram.cloned())
                    });
                    let header = (node_id.to_owned(), incarnation.to_owned(), labels, kind, unit.to_owned());
                    (header, through, steps.collect::<Vec<_>>())
                });
                series.collect::<Vec<_>>()
            });
            owned.collect::<Vec<_>>()
        };
        let answers = queries.iter().zip(checked).zip(read).map(|((query, checked), mut series)| {
            checked?;
            // In the order of the labels as JSON, which a store that keeps them so sorts by.
            series.sort_by(|(a, ..), (b, ..)| a.cmp(b));
            let rows = series.iter().flat_map(|((node_id, incarnation, labels, kind, unit), through, steps)| {
                steps.iter().map(move |(time_ms, value, held, histogram)| Row {
                    node_id,
                    incarnation,
                    labels,
                    kind,
                    unit,
                    time_ms: *time_ms,
                    value: *value,
                    held: *held,
                    histogram: histogram.as_ref(),
                    through: *through,
                })
            });
            Ok(assemble(query, rows))
        });
        answers.collect()
    }

    /// The series of a metric stored in the range, without values.
    fn list(&self, query: &HistoryQuery) -> Result<HistoryResult, String> {
        query.validate()?;
        let stored = self.stored.lock();
        let mut found = BTreeSet::new();
        let (range, node) = (query.from_ms..=query.to_ms, query.node.as_deref());
        for batch in
            (stored.batches.iter()).filter(|b| range.contains(&b.timestamp_ms) && node.is_none_or(|n| n == b.node_id))
        {
            for sample in batch.samples.iter().filter(|s| *s.name == query.metric && query.carried_by(&s.labels)) {
                found.insert((
                    &*batch.node_id,
                    &*batch.incarnation,
                    &*sample.labels,
                    sample.value.kind(),
                    &*sample.unit,
                ));
            }
        }
        let series = found.iter().take(query.max_series).map(|&(node_id, incarnation, labels, kind, unit)| Series {
            node_id: node_id.into(),
            incarnation: incarnation.into(),
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
            kind: kind.into(),
            unit: unit.into(),
            summed: None,
            values: Vec::new(),
        });
        let (from, step) = (query.start_ms(), query.step_ms);
        Ok(HistoryResult {
            from,
            step,
            series: series.collect(),
            truncated: found.len() > query.max_series,
            ..Default::default()
        })
    }
}

impl MetricsSink for MemoryMetrics {
    fn name(&self) -> &str {
        "memory"
    }

    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        let result = validate_batches(batches).map(|()| {
            let mut stored = self.stored.lock();
            for batch in batches {
                if stored.batch_ids.insert((batch.node_id.clone(), batch.incarnation.clone(), batch.sequence)) {
                    let kept = Kept::new(batch);
                    stored.held_bytes += kept.bytes;
                    stored.held_samples += kept.samples.len() as u64;
                    stored.batches.push_back(kept);
                }
            }
            while stored.held_bytes > self.max_bytes {
                let old = stored.batches.pop_front().expect("bytes are held by batches");
                stored.held_bytes -= old.bytes;
                stored.held_samples -= old.samples.len() as u64;
                stored.evicted_batches += 1;
                stored.batch_ids.remove(&(old.node_id, old.incarnation, old.sequence));
            }
        });
        Box::pin(std::future::ready(result))
    }

    fn write_events(&self, events: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        let result = validate_events(events).map(|()| {
            let mut stored = self.stored.lock();
            for event in events {
                if stored.event_ids.insert(event.id.clone()) {
                    stored.events.push_back(event.clone());
                }
            }
            while stored.events.len() > MEMORY_EVENTS {
                let old = stored.events.pop_front().expect("not empty");
                stored.event_ids.remove(&old.id);
            }
        });
        Box::pin(std::future::ready(result))
    }

    fn write_spans(&self, spans: &[SpanRecord]) -> MetricsFuture<'_, ()> {
        let mut stored = self.stored.lock();
        for span in spans {
            if stored.span_ids.insert((span.trace.clone(), span.span.clone())) {
                stored.spans.push_back(span.clone());
            }
        }
        while stored.spans.len() > MEMORY_SPANS {
            let old = stored.spans.pop_front().expect("not empty");
            stored.span_ids.remove(&(old.trace, old.span));
        }
        Box::pin(std::future::ready(Ok(())))
    }

    fn write_dead_letters(&self, letters: &[DeadLetterRecord]) -> MetricsFuture<'_, ()> {
        let mut stored = self.stored.lock();
        for letter in letters {
            if !stored.letters.contains(letter) {
                stored.letters.push_back(letter.clone());
            }
        }
        let excess = stored.letters.len().saturating_sub(MEMORY_LETTERS);
        stored.letters.drain(..excess);
        Box::pin(std::future::ready(Ok(())))
    }

    fn usage(&self) -> Option<SinkUsage> {
        let stored = self.stored.lock();
        let samples = TableUsage {
            name: "samples".into(),
            rows: stored.held_samples,
            held_bytes: stored.held_bytes as u64,
            uncompressed_bytes: stored.held_bytes as u64,
            // Stored in order, so the oldest is at the front.
            first_ms: stored.batches.front().map(|b| b.stored_ms),
            last_ms: stored.batches.back().map(|b| b.stored_ms),
            ..Default::default()
        };
        Some(SinkUsage {
            max_bytes: self.max_bytes as u64,
            held_bytes: stored.held_bytes as u64,
            held_batches: stored.batches.len() as u64,
            evicted_batches: stored.evicted_batches,
            tables: vec![samples],
            free_bytes: Some(self.max_bytes.saturating_sub(stored.held_bytes) as u64),
            ..Default::default()
        })
    }
}

struct Step<'a> {
    at: (u64, u64),
    last: f64,
    histogram: Option<&'a HistogramSnapshot>,
    sum: f64,
    count: f64,
    min: f64,
    max: f64,
}

impl MetricsQuery for MemoryMetrics {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            numeric_history: true,
            histograms: true,
            events: true,
            traces: true,
            max_points: MAX_POINTS,
            storage: "memory".into(),
        }
    }

    fn query(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        let answer = self.answer(std::slice::from_ref(query)).pop().expect("an answer per query");
        Box::pin(std::future::ready(answer))
    }

    fn queries(&self, queries: &[HistoryQuery]) -> MetricsFuture<'_, Vec<Result<HistoryResult, String>>> {
        Box::pin(std::future::ready(Ok(self.answer(queries))))
    }

    fn series(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        Box::pin(std::future::ready(self.list(query)))
    }

    fn events(&self, query: &EventQuery) -> MetricsFuture<'_, Vec<ClusterEvent>> {
        let result = query.validate().map(|()| {
            let stored = self.stored.lock();
            let mut events: Vec<_> = stored.events.iter().filter(|e| query.matches(e)).cloned().collect();
            events.sort_by(|a, b| (b.time_ms, &a.id).cmp(&(a.time_ms, &b.id)));
            events.truncate(query.limit);
            events
        });
        Box::pin(std::future::ready(result))
    }

    fn traces(&self, query: &TraceQuery) -> MetricsFuture<'_, Vec<TraceSummary>> {
        let result = query.validate().map(|()| query.apply(summarize(&self.stored.lock().spans)));
        Box::pin(std::future::ready(result))
    }

    fn dead_letters(&self, limit: usize) -> MetricsFuture<'_, Vec<DeadLetterRecord>> {
        let mut letters: Vec<_> = self.stored.lock().letters.iter().cloned().collect();
        letters.sort_by_key(|l| std::cmp::Reverse((l.at_us, l.seq)));
        letters.truncate(limit);
        Box::pin(std::future::ready(Ok(letters)))
    }

    fn trace(&self, trace: u64) -> MetricsFuture<'_, Vec<SpanRecord>> {
        let id = hex(trace);
        let spans = self.stored.lock().spans.iter().filter(|s| s.trace == id).cloned().collect();
        Box::pin(std::future::ready(Ok(spans)))
    }
}

#[test]
fn growth_is_what_each_table_still_filling_holds_over_its_span_and_retention_can_level_it_off() {
    const GB: u64 = 1 << 30;
    const DAY: u64 = 86_400_000;
    let table = |held_bytes, span_ms, retention_days| TableUsage {
        held_bytes,
        first_ms: Some(DAY),
        last_ms: Some(DAY + span_ms),
        retention_days,
        ..Default::default()
    };
    let usage = |tables, free| {
        let usage = SinkUsage { tables, free_bytes: Some(free), ..Default::default() }.with_growth();
        (usage.growth_bytes_per_day, usage.full_in_ms)
    };
    assert_eq!(usage(vec![table(GB, DAY, Some(30))], 100 * GB), (Some(GB), None), "30 days of 1 GB fit in 100 GB");
    assert_eq!(usage(vec![table(GB, DAY, Some(30))], 10 * GB), (Some(GB), Some(10 * DAY)), "they do not fit in 10");
    assert_eq!(usage(vec![table(5 * GB, DAY, Some(30))], 147 * GB), (Some(5 * GB), None), "145 GB more fit in 147");
    let forever = vec![table(GB, DAY, Some(30)), table(4 * GB, 4 * DAY, None)];
    assert_eq!(usage(forever, 100 * GB), (Some(2 * GB), Some(50 * DAY)), "a table kept for ever fills any disk");
    let level = vec![table(30 * GB, 30 * DAY, Some(30)), table(GB, DAY / 2, None)];
    assert_eq!(usage(level, 10 * GB), (Some(2 * GB), Some(5 * DAY)), "a table its TTL keeps full grows no more");
    assert_eq!(usage(vec![table(GB, 3_599_999, None)], GB), (None, None), "under an hour of history tells nothing");
    assert_eq!(usage(vec![table(0, DAY, None)], GB), (Some(0), None), "nothing grows, so nothing fills");
    assert_eq!(usage(vec![table(90, 30 * DAY, None)], 1 << 50), (Some(3), Some(u64::MAX)), "past what a u64 counts");
}

#[test]
fn usage_reads_across_a_collector_and_a_console_of_different_versions() {
    let before_tables = r#"{"max_bytes":9,"held_bytes":1,"held_batches":1,"evicted_batches":0}"#;
    let old: SinkUsage = serde_json::from_str(before_tables).expect("an older collector's answer reads");
    assert_eq!(old, SinkUsage { max_bytes: 9, held_bytes: 1, held_batches: 1, ..Default::default() });
    #[derive(Deserialize)]
    #[expect(dead_code, reason = "only whether it reads")]
    struct Older {
        max_bytes: u64,
        held_bytes: u64,
        held_batches: u64,
        evicted_batches: u64,
    }
    let new = SinkUsage { tables: vec![TableUsage::default()], full_in_ms: Some(1), ..old };
    let new = serde_json::to_string(&new).unwrap();
    serde_json::from_str::<Older>(&new).expect("and an older console reads a newer one");
}

#[cfg(test)]
fn row<'a>(node: &'a str, labels: &'a str, kind: &'a str, time_ms: u64, value: f64, through: u64) -> Row<'a> {
    let (incarnation, unit, histogram) = ("1", "u", None);
    Row { node_id: node, incarnation, labels, kind, unit, time_ms, value, held: value, histogram, through }
}

#[cfg(test)]
fn sparse(first: usize, counts: &[u64], count: u64, sum_ns: u64) -> HistogramSnapshot {
    let sparse = serde_json::json!({ "first": first, "counts": counts, "count": count, "sum_ns": sum_ns });
    serde_json::from_value(sparse).expect("a sparse histogram")
}

#[cfg(test)]
fn values(result: &HistoryResult) -> Vec<String> {
    result.series.iter().map(|s| serde_json::to_string(&s.values).expect("values serialize")).collect()
}

#[test]
fn a_counter_is_its_gain_per_step_from_the_step_before_the_range_and_a_gauge_its_value_held_until_through() {
    let query = HistoryQuery::new("m", 1000, 4000, 1000);
    let rows = [
        row("a", "{}", "counter", 0, 5.0, 4000),
        row("a", "{}", "counter", 1000, 7.0, 4000),
        row("a", "{}", "counter", 2000, 7.0, 4000),
        row("a", "{}", "counter", 4000, 9.0, 4000),
        row("a", "{}", "gauge", 1000, 0.5, 4000),
        row("a", "{}", "gauge", 2000, 2.0, 4000),
        row("b", "{}", "counter", 1000, 9.0, 3000),
        row("b", "{}", "counter", 2000, 3.0, 3000),
        row("b", "{}", "counter", 3000, 4.0, 3000),
    ];
    let result = assemble(&query, rows);
    assert_eq!((result.from, result.step, result.count, result.truncated), (1000, 1000, 4, false));
    // A step without a value before it, one the counter went back in, or one past what its node sent, is unknown.
    assert_eq!(values(&result), ["[2,0,0,2]", "[0.5,2,2,2]", "[null,null,1,null]"]);
    let json = serde_json::to_value(&result.series[0]).expect("a series serializes");
    assert!(json.get("summed").is_none() && json.get("metric").is_none());

    let room = HistoryQuery { max_points: 7, ..query.clone() };
    let cut = assemble(&room, [row("a", "{}", "gauge", 1000, 1.0, 4000), row("b", "{}", "gauge", 1000, 1.0, 4000)]);
    assert_eq!(
        (values(&cut), cut.truncated),
        (vec!["[1,1,1,1]".to_owned()], true),
        "a series fits whole or not at all"
    );

    let late = HistoryQuery::new("m", 100_000, 200_000, 1000);
    let settled = |through| assemble(&late, [row("a", "{}", "gauge", 150_000, 1.0, through)]).settled;
    assert_eq!((settled(180_500), settled(900_000)), (120_000, 140_000), "HELD_MS before the newest, or before to_ms");
}

#[test]
fn a_histogram_is_what_it_gained_per_step() {
    let h = sparse;
    let stored =
        [h(3, &[1, 0, 2], 3, 30), h(3, &[1, 0, 2, 0, 1], 4, 50), h(2, &[1, 1, 0, 2, 0, 1], 5, 51), h(2, &[1], 1, 1)];
    let rows = stored.iter().zip([0, 1000, 2000, 3000]).map(|(stored, t)| Row {
        histogram: Some(stored),
        ..row("a", "{}", "histogram", t, stored.count() as f64, 3000)
    });
    let result = assemble(&HistoryQuery::new("m", 1000, 4000, 1000), rows);
    let gained = |h: HistogramSnapshot| Some(Reading::Histogram(h));
    let expected = [gained(h(7, &[1], 1, 20)), gained(h(2, &[1], 1, 1)), None, None];
    assert_eq!(result.series[0].values, expected, "the reset at 3000 is unknown");
}

#[test]
fn a_sum_adds_each_known_step_per_node_boot_or_over_the_cluster_and_keeps_the_labels_named() {
    let answer = |sum, by: &[&str]| {
        let by = by.iter().map(|b| b.to_string()).collect();
        let query = HistoryQuery { sum: Some(sum), by, ..HistoryQuery::new("m", 1000, 4000, 1000) };
        let gauge = |node, labels, t, v, through| row(node, labels, "gauge", t, v, through);
        let mut rows = Vec::new();
        for t in [1000, 2000, 3000, 4000] {
            rows.push(gauge("a", r#"{"peer":"b","pool":"io"}"#, t, 1.0, 4000));
        }
        for t in [1000, 2000, 3000, 4000] {
            rows.push(gauge("a", r#"{"peer":"c"}"#, t, 2.0, 4000));
        }
        rows.extend([gauge("a", r#"{"peer":"d"}"#, 3000, 4.0, 4000), gauge("a", r#"{"peer":"d"}"#, 4000, 4.0, 4000)]);
        rows.extend([1000, 2000, 3000].map(|t| gauge("b", r#"{"peer":"a"}"#, t, 8.0, 3000)));
        rows.push(gauge("c", r#"{"peer":"a"}"#, 1000, 16.0, 1000));
        let result = assemble(&query, rows);
        let line = |s: &Series| (s.node_id.clone(), serde_json::to_string(&s.labels).unwrap_or_default(), s.summed);
        (result.series.iter().map(line).collect::<Vec<_>>(), values(&result))
    };
    let (node, values) = answer(Sum::Node, &[]);
    assert_eq!(node[..2], [("a".into(), "{}".into(), Some(3)), ("b".into(), "{}".into(), Some(1))]);
    assert_eq!(values[..2], ["[3,3,7,7]", "[8,8,8,null]"]);
    let (pools, values) = answer(Sum::Node, &["pool"]);
    assert_eq!(pools[..2], [("a".into(), "{}".into(), Some(2)), ("a".into(), r#"{"pool":"io"}"#.into(), Some(1))]);
    assert_eq!(values[..2], ["[2,2,6,6]", "[1,1,1,1]"]);
    // b stopped a step before a; c stopped long ago, so only b holds the line back.
    let (cluster, values) = answer(Sum::Cluster, &[]);
    assert_eq!((cluster, values), (vec![("".into(), "{}".into(), Some(5))], vec!["[27,11,15,null]".to_owned()]));
}

#[test]
fn a_histogram_sum_merges_the_gains() {
    let h = sparse;
    let stored = [
        (h(2, &[1], 1, 1), "a", 0),
        (h(2, &[1, 0, 3], 4, 10), "a", 1000),
        (h(6, &[1], 1, 1), "b", 0),
        (h(6, &[3], 3, 5), "b", 1000),
    ];
    let rows = stored.iter().map(|(stored, node, t)| Row {
        histogram: Some(stored),
        ..row(node, "{}", "histogram", *t, stored.count() as f64, 1000)
    });
    let query = HistoryQuery { sum: Some(Sum::Cluster), ..HistoryQuery::new("m", 1000, 4000, 1000) };
    let result = assemble(&query, rows);
    assert_eq!(result.series[0].values, [Some(Reading::Histogram(h(4, &[3, 0, 2], 5, 13))), None, None, None]);
}

#[test]
fn groups_keep_the_order_keys_first_come_in() {
    assert_eq!(groups([(0, "b"), (1, "a"), (2, "b")]), [("b", vec![0, 2]), ("a", vec![1])]);
}

#[test]
fn memory_lists_the_series_of_a_metric_in_range_without_values() {
    let store = MemoryMetrics::new(1 << 20);
    let batch = |node: &str, timestamp_ms, peer: &str| {
        let sample = |name: &'static str, labels: &[(&'static str, &str)]| crate::model::Sample {
            name: name.into(),
            unit: "u".into(),
            labels: labels.iter().map(|(k, v)| (Str::from(*k), v.to_string())).collect(),
            value: Value::Counter(1),
        };
        let samples = vec![sample("m", &[("peer", peer)]), sample("other", &[])];
        SampleBatch { node_id: node.into(), incarnation: "1".into(), sequence: 1, timestamp_ms, samples }
    };
    let written = store.write(&[batch("a", 1000, "b"), batch("b", 2000, "a"), batch("c", 9000, "a")]);
    futures_util::FutureExt::now_or_never(written).expect("ready").expect("stored");
    let listed = store.list(&HistoryQuery { max_series: 1, ..HistoryQuery::new("m", 0, 5000, 1000) }).expect("listed");
    assert_eq!((listed.series.len(), listed.truncated, listed.series[0].values.len()), (1, true, 0));
    assert_eq!((&*listed.series[0].node_id, &listed.series[0].labels["peer"]), ("a", &"b".to_owned()));
}
