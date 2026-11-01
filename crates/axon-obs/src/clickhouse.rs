use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use axon::{Histogram, HistogramSnapshot};
use parking_lot::Mutex;
use reqwest::Url;
use serde::Deserialize;
use tokio::time::Instant;

use crate::metrics::OWN_SEQUENCE;
use crate::model::{HELD_MS, SampleBatch, Value};
use crate::provider::*;
use crate::traces::{DeadLetterRecord, SpanRecord, TraceQuery, TraceSummary, hex};

const SPAN_COLUMNS: &str =
    "trace, span, parent, kind, name, node_id, actor, actor_name, actor_type, peer, start_us, wait_ns, busy_ns, failed";
const LETTER_COLUMNS: &str = "node_id, seq, at_us, reason, label, actor, actor_type, target, peer, trace, count";
const MAX_RESPONSE: usize = 16 << 20;
const USAGE_EVERY: Duration = Duration::from_secs(60);
const LIMITS: &str = "SETTINGS max_execution_time=2, max_rows_to_read=100000000, max_bytes_to_read=4294967296, \
                      max_memory_usage=134217728, output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow";

#[derive(Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: String,
    /// Raw samples, the one-minute rollup, events, spans, dead letters and positions are kept this long.
    pub retention_days: u32,
    /// The one-hour rollup is kept this long.
    pub rollup_retention_days: u32,
}

/// Rollups a query reads instead of raw samples once its step is a multiple of theirs, coarsest first.
const ROLLUPS: [(&str, u64); 2] = [("samples_1h", 3_600_000), ("samples_1m", 60_000)];

fn rollup_for(step_ms: u64) -> Option<(&'static str, u64)> {
    ROLLUPS.into_iter().find(|(_, step)| step_ms.is_multiple_of(*step))
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8123".into(),
            database: "axon_metrics".into(),
            username: "default".into(),
            password: String::new(),
            retention_days: 30,
            rollup_retention_days: 365,
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseProvider {
    http: reqwest::Client,
    url: Url,
    database: String,
    username: String,
    password: String,
    /// When usage was last asked of ClickHouse, and what it said.
    usage: Arc<Mutex<(Instant, Option<SinkUsage>)>>,
}

#[derive(Deserialize)]
struct StepRow {
    metric: String,
    node_id: String,
    incarnation: String,
    labels: String,
    kind: String,
    unit: String,
    time_ms: u64,
    value: f64,
    held: f64,
    /// A histogram's last in the step, `None` where its buckets do not fit axon's.
    #[serde(deserialize_with = "lenient")]
    histogram: Option<HistogramSnapshot>,
    through: u64,
}

impl StepRow {
    fn series(&self) -> (&str, &str, &str, &str, &str) {
        (&self.node_id, &self.incarnation, &self.labels, &self.kind, &self.unit)
    }
}

#[derive(Deserialize)]
struct SeriesRow {
    node_id: String,
    incarnation: String,
    labels: String,
    kind: String,
    unit: String,
}

fn lenient<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<HistogramSnapshot>, D::Error> {
    Ok(HistogramSnapshot::deserialize(d).ok())
}

impl ClickHouseProvider {
    /// Creates the database and tables when they are missing.
    pub async fn connect(cfg: ClickHouseConfig) -> Result<Self, String> {
        let url = Url::parse(&cfg.url).map_err(|e| format!("url: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("url must be http or https".into());
        }
        if !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
            return Err("url must not carry credentials, a query or a fragment".into());
        }
        let db = &cfg.database;
        if !(1..=64).contains(&db.len()) || !db.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err("database must be 1..=64 characters of [A-Za-z0-9_]".into());
        }
        if !(1..=3650).contains(&cfg.retention_days) {
            return Err("retention_days must be 1..=3650".into());
        }
        if !(1..=3650).contains(&cfg.rollup_retention_days) {
            return Err("rollup_retention_days must be 1..=3650".into());
        }
        let http = reqwest::Client::builder().timeout(Duration::from_secs(3)).build().map_err(|e| e.to_string())?;
        let days = cfg.retention_days;
        let usage = Arc::new(Mutex::new((Instant::now(), None)));
        let this = Self { http, url, database: cfg.database, username: cfg.username, password: cfg.password, usage };
        let db = &this.database;
        let mut statements = vec![
            format!("CREATE DATABASE IF NOT EXISTS `{db}`"),
            format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.samples (node_id String, incarnation String, sequence UInt64, \
                 timestamp_ms UInt64, metric LowCardinality(String), labels String, kind LowCardinality(String), \
                 unit LowCardinality(String), value Float64, sum_ns UInt64, buckets Array(UInt64)) \
                 ENGINE = ReplacingMergeTree \
                 ORDER BY (metric, node_id, incarnation, labels, kind, unit, timestamp_ms, sequence) \
                 TTL toDateTime(intDiv(timestamp_ms, 1000)) + INTERVAL {days} DAY \
                 SETTINGS non_replicated_deduplication_window = 1000"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.events (id String, time_ms UInt64, node_id String, \
                 kind LowCardinality(String), subject String, detail String) ENGINE = ReplacingMergeTree \
                 PARTITION BY intDiv(time_ms, 86400000) ORDER BY id \
                 TTL toDateTime(intDiv(time_ms, 1000)) + INTERVAL {days} DAY"
            ),
            format!("ALTER TABLE `{db}`.events ADD COLUMN IF NOT EXISTS subject String AFTER kind"),
            format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.spans (trace String, span String, parent String, \
                 kind LowCardinality(String), name String, node_id LowCardinality(String), actor String, actor_name String, \
                 actor_type LowCardinality(String), peer LowCardinality(String), start_us UInt64, wait_ns UInt64, \
                 busy_ns UInt64, failed Bool) ENGINE = ReplacingMergeTree \
                 PARTITION BY intDiv(start_us, 86400000000) ORDER BY (trace, span) \
                 TTL toDateTime(intDiv(start_us, 1000000)) + INTERVAL {days} DAY"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.dead_letters (node_id LowCardinality(String), seq UInt64, \
                 at_us UInt64, reason LowCardinality(String), label LowCardinality(String), actor String, \
                 actor_type LowCardinality(String), target String, peer LowCardinality(String), trace String, \
                 count UInt64) ENGINE = ReplacingMergeTree \
                 PARTITION BY intDiv(at_us, 86400000000) ORDER BY (at_us, node_id, seq) \
                 TTL toDateTime(intDiv(at_us, 1000000)) + INTERVAL {days} DAY"
            ),
        ];
        statements.push(format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.positions (node_id String, incarnation String, \
             sequence SimpleAggregateFunction(max, UInt64), at_ms SimpleAggregateFunction(max, UInt64)) \
             ENGINE = AggregatingMergeTree ORDER BY (node_id, incarnation) \
             TTL toDateTime(intDiv(at_ms, 1000)) + INTERVAL {days} DAY"
        ));
        statements.push(format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS `{db}`.positions_feed TO `{db}`.positions AS \
             SELECT node_id, incarnation, max(theirs) AS sequence, max(timestamp_ms) AS at_ms FROM \
             (SELECT node_id, incarnation, sequence AS theirs, timestamp_ms FROM `{db}`.samples \
             WHERE sequence < {OWN_SEQUENCE}) GROUP BY node_id, incarnation"
        ));
        let at = "Tuple(UInt64, UInt64)";
        for ((table, step), days) in ROLLUPS.into_iter().zip([cfg.rollup_retention_days, days]) {
            statements.push(format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.{table} (metric LowCardinality(String), node_id String, \
                 incarnation String, labels String, kind LowCardinality(String), unit LowCardinality(String), \
                 bucket_ms UInt64, last AggregateFunction(argMax, Float64, {at}), \
                 last_sum_ns AggregateFunction(argMax, UInt64, {at}), \
                 last_buckets AggregateFunction(argMax, Array(UInt64), {at}), \
                 low SimpleAggregateFunction(min, Float64), high SimpleAggregateFunction(max, Float64), \
                 total SimpleAggregateFunction(sum, Float64), points SimpleAggregateFunction(sum, UInt64)) \
                 ENGINE = AggregatingMergeTree \
                 ORDER BY (metric, node_id, incarnation, labels, kind, unit, bucket_ms) \
                 TTL toDateTime(intDiv(bucket_ms, 1000)) + INTERVAL {days} DAY \
                 SETTINGS non_replicated_deduplication_window = 1000"
            ));
            statements.push(format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS `{db}`.{table}_feed TO `{db}`.{table} AS \
                 SELECT metric, node_id, incarnation, labels, kind, unit, \
                 intDiv(timestamp_ms, {step}) * {step} AS bucket_ms, \
                 argMaxState(value, (timestamp_ms, sequence)) AS last, \
                 argMaxState(sum_ns, (timestamp_ms, sequence)) AS last_sum_ns, \
                 argMaxState(buckets, (timestamp_ms, sequence)) AS last_buckets, \
                 min(value) AS low, max(value) AS high, sum(value) AS total, count() AS points \
                 FROM `{db}`.samples GROUP BY metric, node_id, incarnation, labels, kind, unit, bucket_ms"
            ));
        }
        for ddl in statements {
            this.run(&ddl, &[], Vec::new()).await?;
        }
        // A retried insert is dropped before it reaches the rollups, whose sums would count it twice. A
        // table from before the rollups lacks the setting; one that may not be altered only loses that.
        let dedup = format!("ALTER TABLE `{db}`.samples MODIFY SETTING non_replicated_deduplication_window = 1000");
        let _ = this.run(&dedup, &[], Vec::new()).await;
        *this.usage.lock() = (Instant::now(), this.read_usage().await.ok());
        Ok(this)
    }

    /// From part metadata only, never the rows: a first or last point is its parts' TTL bound less the TTL.
    async fn read_usage(&self) -> Result<SinkUsage, String> {
        let db = [("db".to_owned(), self.database.clone())];
        let sql = format!(
            "SELECT name, row_count AS rows, raw_bytes AS uncompressed_bytes, disk_bytes AS held_bytes, \
             part_count AS parts, toUInt32OrZero(extract(engine_full, 'toIntervalDay[(]([0-9]+)[)]')) AS days, \
             nullIf(days, 0) AS retention_days, \
             if(first_s > 0 AND days > 0, least(first_s - days * 86400, toUnixTimestamp(now())) * 1000, NULL) AS first_ms, \
             if(last_s > 0 AND days > 0, least(last_s - days * 86400, toUnixTimestamp(now())) * 1000, NULL) AS last_ms \
             FROM system.tables LEFT JOIN (SELECT table AS name, sum(rows) AS row_count, \
             sum(data_uncompressed_bytes) AS raw_bytes, sum(bytes_on_disk) AS disk_bytes, count() AS part_count, \
             minIf(toUnixTimestamp(delete_ttl_info_min), toUnixTimestamp(delete_ttl_info_min) > 0) AS first_s, \
             max(toUnixTimestamp(delete_ttl_info_max)) AS last_s \
             FROM system.parts WHERE database = {{db:String}} AND active GROUP BY table) AS p USING (name) \
             WHERE database = {{db:String}} AND engine NOT LIKE '%View' ORDER BY name {LIMITS}"
        );
        let tables: Vec<TableUsage> = lines(&self.run(&sql, &db, Vec::new()).await?)?;
        let sql = format!(
            "SELECT minOrNull(free_space) AS free FROM system.disks WHERE name IN (SELECT arrayJoin(disks) \
             FROM system.storage_policies WHERE policy_name IN (SELECT storage_policy FROM system.tables \
             WHERE database = {{db:String}})) {LIMITS}"
        );
        #[derive(Deserialize)]
        struct Free {
            free: Option<u64>,
        }
        let free: Vec<Free> = lines(&self.run(&sql, &db, Vec::new()).await?)?;
        let held_bytes = tables.iter().map(|t| t.held_bytes).sum();
        Ok(SinkUsage { held_bytes, free_bytes: free.first().and_then(|f| f.free), tables, ..Default::default() })
    }

    /// Values travel as typed query parameters, never inside the statement. The statement is
    /// the body unless there is data to send (zstd-compressed), in which case it moves to the URL.
    async fn run(&self, sql: &str, params: &[(String, String)], body: Vec<u8>) -> Result<String, String> {
        let mut query: Vec<_> = params.iter().map(|(name, value)| (format!("param_{name}"), escape(value))).collect();
        let has_data = !body.is_empty();
        let body = if !has_data {
            sql.as_bytes().to_vec()
        } else {
            query.push(("query".to_owned(), sql.to_owned()));
            query.push(("deduplicate_blocks_in_dependent_materialized_views".to_owned(), "1".to_owned()));
            body
        };
        let mut request = self.http.post(self.url.clone()).query(&query).body(body);
        if has_data {
            request = request.header("Content-Encoding", "zstd");
        }
        let sent = request.basic_auth(&self.username, Some(&self.password)).send().await;
        let mut response = sent.map_err(|e| format!("ClickHouse: {}", e.without_url()))?;
        let mut text = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| format!("ClickHouse: {}", e.without_url()))? {
            if text.len() + chunk.len() > MAX_RESPONSE {
                return Err("ClickHouse: response exceeds 16 MiB".into());
            }
            text.extend_from_slice(&chunk);
        }
        let text = String::from_utf8_lossy(&text);
        if response.status().is_success() {
            Ok(text.into_owned())
        } else {
            Err(format!("ClickHouse {}: {}", response.status(), text.chars().take(1024).collect::<String>()))
        }
    }

    async fn insert(&self, table: &str, columns: &str, body: Vec<u8>) -> Result<(), String> {
        if body.is_empty() {
            return Ok(());
        }
        let sql = format!("INSERT INTO `{}`.{table} ({columns}) FORMAT RowBinary", self.database);
        let body = zstd::bulk::compress(&body, 1).map_err(|e| e.to_string())?;
        self.run(&sql, &[], body).await.map(drop)
    }
}

/// RowBinary: integers little-endian, a string or array behind its LEB128 length.
struct RowBinary(Vec<u8>);

impl RowBinary {
    fn len(&mut self, mut n: usize) {
        while n >= 0x80 {
            self.0.push(n as u8 | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    fn str(&mut self, text: &str) {
        self.len(text.len());
        self.0.extend_from_slice(text.as_bytes());
    }

    fn u64(&mut self, n: u64) {
        self.0.extend_from_slice(&n.to_le_bytes());
    }
}

fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out
}

#[derive(Deserialize)]
struct PositionRow {
    through: u64,
}

/// The node and label filters of a query, as SQL after a `WHERE`, its parameters added to `params`.
fn filtered(query: &HistoryQuery, params: &mut Vec<(String, String)>) -> String {
    let mut filter = String::new();
    if let Some(node) = &query.node {
        filter.push_str(" AND node_id = {node:String}");
        params.push(("node".to_owned(), node.clone()));
    }
    for (i, (key, wanted)) in query.labels.iter().enumerate() {
        let _ = write!(
            filter,
            " AND JSONHas(labels, {{k{i}:String}}) AND JSONExtractString(labels, {{k{i}:String}}) = {{v{i}:String}}"
        );
        params.push((format!("k{i}"), key.clone()));
        params.push((format!("v{i}"), wanted.clone()));
    }
    filter
}

/// The rows of the series read whole: past `max_points` rows in range, `LIMIT BY` may have cut the last of them, and
/// the series after it were not read.
fn whole<'r>(query: &HistoryQuery, rows: &'r [StepRow]) -> (&'r [StepRow], bool) {
    let from = query.read_from_ms();
    let Some(last) = rows.iter().rposition(|r| r.time_ms >= from) else { return (rows, false) };
    if rows.iter().filter(|r| r.time_ms >= from).count() <= query.max_points {
        return (rows, false);
    }
    let first = rows.iter().position(|r| r.series() == rows[last].series()).unwrap_or(last);
    (&rows[..first], true)
}

impl ClickHouseProvider {
    /// Valid queries that differ only in their metric, answered in order from one round trip.
    async fn stepped(&self, queries: &[HistoryQuery]) -> Result<Vec<HistoryResult>, String> {
        let query = &queries[0];
        let rollup = rollup_for(query.step_ms);
        let (table, time, last, sum_ns, buckets, [mean, min, max]) = match rollup {
            Some((table, _)) => (
                table,
                "bucket_ms",
                "argMaxMerge(last)",
                "argMaxMerge(last_sum_ns)",
                "argMaxMerge(last_buckets)",
                ["sum(total) / sum(points)", "min(low)", "max(high)"],
            ),
            None => (
                "samples",
                "timestamp_ms",
                "argMax(value, (timestamp_ms, sequence))",
                "argMax(sum_ns, (timestamp_ms, sequence))",
                "argMax(buckets, (timestamp_ms, sequence))",
                ["avg(value)", "min(value)", "max(value)"],
            ),
        };
        // Only a raw mean is skewed by a not yet merged duplicate, so only it pays for FINAL.
        let (value, dedup) = match query.aggregation {
            Aggregation::Last => (last.to_owned(), ""),
            Aggregation::Mean if rollup.is_none() => (format!("if(kind = 'histogram', {last}, {mean})"), " FINAL"),
            Aggregation::Mean => (format!("if(kind = 'histogram', {last}, {mean})"), ""),
            Aggregation::Min => (format!("if(kind = 'histogram', {last}, {min})"), ""),
            Aggregation::Max => (format!("if(kind = 'histogram', {last}, {max})"), ""),
        };
        let from_ms = query.read_from_ms();
        let mut params = vec![
            ("carry".to_owned(), from_ms.saturating_sub(HELD_MS).to_string()),
            ("from".to_owned(), from_ms.to_string()),
            ("to".to_owned(), query.to_ms.to_string()),
            ("step".to_owned(), query.step_ms.to_string()),
            ("limit".to_owned(), (query.max_points + 1).to_string()),
        ];
        let mut metrics = Vec::new();
        for (i, q) in queries.iter().enumerate() {
            metrics.push(format!("{{m{i}:String}}"));
            params.push((format!("m{i}"), q.metric.clone()));
        }
        let filter = format!(" AND metric IN ({}){}", metrics.join(", "), filtered(query, &mut params));
        // The steps in range, the last row of each series in the `HELD_MS` before them, and how far each node
        // has been stored, in one round trip; a histogram as axon's sparse one, the run of its nonzero buckets.
        let (db, series) = (&self.database, "metric, node_id, incarnation, labels, kind, unit");
        let sql = format!(
            "SELECT {series}, time_ms, v AS value, h AS held, \
             CAST((greatest(arrayFirstIndex(n -> n > 0, buckets) AS lo, 1) - 1, \
             if(lo = 0, [], arraySlice(buckets, lo, arrayLastIndex(n -> n > 0, buckets) - lo + 1)), \
             toUInt64(greatest(h, 0)), sum_ns), 'Tuple(first UInt64, counts Array(UInt64), count UInt64, sum_ns UInt64)') \
             AS histogram, through FROM (\
             (SELECT {series}, intDiv({time}, {{step:UInt64}}) * {{step:UInt64}} AS time_ms, {value} AS v, \
             {last} AS h, {sum_ns} AS sum_ns, {buckets} AS buckets FROM `{db}`.{table}{dedup} \
             WHERE {time} BETWEEN {{from:UInt64}} AND {{to:UInt64}}{filter} \
             GROUP BY {series}, time_ms ORDER BY {series}, time_ms LIMIT {{limit:UInt64}} BY metric) \
             UNION ALL \
             (SELECT {series}, intDiv(max({time}), {{step:UInt64}}) * {{step:UInt64}} AS time_ms, {last} AS v, \
             {last} AS h, {sum_ns} AS sum_ns, {buckets} AS buckets FROM `{db}`.{table} \
             WHERE {time} >= {{carry:UInt64}} AND {time} < {{from:UInt64}}{filter} \
             GROUP BY {series})) AS steps \
             LEFT JOIN (SELECT node_id, incarnation, max(at_ms) AS through FROM `{db}`.positions \
             WHERE at_ms >= {{carry:UInt64}} GROUP BY node_id, incarnation) AS stored USING (node_id, incarnation) \
             ORDER BY {series}, time_ms {LIMITS}"
        );
        let rows = lines::<StepRow>(&self.run(&sql, &params, Vec::new()).await?)?;
        let metrics: Vec<&[StepRow]> = rows.chunk_by(|a, b| a.metric == b.metric).collect();
        let mut results = vec![HistoryResult::default(); queries.len()];
        for (metric, asking) in groups(queries.iter().enumerate().map(|(i, q)| (i, &*q.metric))) {
            let rows = metrics.iter().find(|rows| rows[0].metric == metric).copied().unwrap_or_default();
            let (rows, cut) = whole(query, rows);
            let rows = rows.iter().map(|r| Row {
                node_id: &r.node_id,
                incarnation: &r.incarnation,
                labels: &r.labels,
                kind: &r.kind,
                unit: &r.unit,
                time_ms: r.time_ms,
                value: r.value,
                held: r.held,
                histogram: r.histogram.as_ref().filter(|_| r.kind == "histogram"),
                through: r.through,
            });
            let mut result = assemble(query, rows);
            result.truncated |= cut;
            for i in asking {
                results[i] = result.clone();
            }
        }
        Ok(results)
    }
}

fn lines<T: for<'de> Deserialize<'de>>(text: &str) -> Result<Vec<T>, String> {
    text.lines().map(|line| serde_json::from_str(line).map_err(|e| format!("ClickHouse row: {e}"))).collect()
}

impl MetricsSink for ClickHouseProvider {
    fn name(&self) -> &str {
        "clickhouse"
    }

    fn write(&self, batches: &[SampleBatch]) -> MetricsFuture<'_, ()> {
        let body = validate_batches(batches).map(|()| {
            let mut row = RowBinary(Vec::new());
            for batch in batches {
                for sample in &batch.samples {
                    let dense;
                    let (value, sum_ns, buckets): (f64, u64, &[u64]) = match &sample.value {
                        Value::Gauge(v) => (*v, 0, &[]),
                        Value::Counter(v) => (*v as f64, 0, &[]),
                        Value::Histogram(h) => {
                            dense = Histogram::from(h);
                            (h.count() as f64, h.sum_ns(), &dense.buckets)
                        }
                    };
                    row.str(&batch.node_id);
                    row.str(&batch.incarnation);
                    row.u64(batch.sequence);
                    row.u64(batch.timestamp_ms);
                    row.str(&sample.name);
                    row.str(&serde_json::to_string(&sample.labels).unwrap_or_default());
                    row.str(sample.value.kind());
                    row.str(&sample.unit);
                    row.0.extend_from_slice(&value.to_le_bytes());
                    row.u64(sum_ns);
                    row.len(buckets.len());
                    buckets.iter().for_each(|n| row.u64(*n));
                }
            }
            row.0
        });
        let columns =
            "node_id, incarnation, sequence, timestamp_ms, metric, labels, kind, unit, value, sum_ns, buckets";
        Box::pin(async move { self.insert("samples", columns, body?).await })
    }

    fn stored_through(&self, node: &str, incarnation: &str) -> MetricsFuture<'_, Option<u64>> {
        let params = [("node".to_owned(), node.to_owned()), ("incarnation".to_owned(), incarnation.to_owned())];
        Box::pin(async move {
            let sql = format!(
                "SELECT max(sequence) AS through FROM `{}`.positions \
                 WHERE node_id = {{node:String}} AND incarnation = {{incarnation:String}} HAVING count() > 0 {LIMITS}",
                self.database
            );
            let rows: Vec<PositionRow> = lines(&self.run(&sql, &params, Vec::new()).await?)?;
            Ok(rows.first().map(|row| row.through))
        })
    }

    fn write_events(&self, events: &[ClusterEvent]) -> MetricsFuture<'_, ()> {
        let body = validate_events(events).map(|()| {
            let mut row = RowBinary(Vec::new());
            for event in events {
                row.str(&event.id);
                row.u64(event.time_ms);
                row.str(&event.node_id);
                row.str(&event.kind);
                row.str(&event.subject);
                row.str(&event.detail);
            }
            row.0
        });
        Box::pin(async move { self.insert("events", "id, time_ms, node_id, kind, subject, detail", body?).await })
    }

    fn write_spans(&self, spans: &[SpanRecord]) -> MetricsFuture<'_, ()> {
        let mut row = RowBinary(Vec::new());
        for s in spans {
            let texts = [
                &s.trace,
                &s.span,
                &s.parent,
                &s.kind,
                &s.name,
                &s.node_id,
                &s.actor,
                &s.actor_name,
                &s.actor_type,
                &s.peer,
            ];
            for text in texts {
                row.str(text);
            }
            [s.start_us, s.wait_ns, s.busy_ns].into_iter().for_each(|n| row.u64(n));
            row.0.push(s.failed as u8);
        }
        Box::pin(async move { self.insert("spans", SPAN_COLUMNS, row.0).await })
    }

    fn write_dead_letters(&self, letters: &[DeadLetterRecord]) -> MetricsFuture<'_, ()> {
        let mut row = RowBinary(Vec::new());
        for l in letters {
            row.str(&l.node_id);
            [l.seq, l.at_us].into_iter().for_each(|n| row.u64(n));
            [&l.reason, &l.label, &l.actor, &l.actor_type, &l.target, &l.peer, &l.trace]
                .into_iter()
                .for_each(|text| row.str(text));
            row.u64(l.count);
        }
        Box::pin(async move { self.insert("dead_letters", LETTER_COLUMNS, row.0).await })
    }

    /// What ClickHouse said at most a minute ago; an older answer is asked again in the background, and this
    /// answers the one it has, the last good one when a read fails.
    fn usage(&self) -> Option<SinkUsage> {
        let mut usage = self.usage.lock();
        if usage.0.elapsed() >= USAGE_EVERY
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            usage.0 = Instant::now();
            let this = self.clone();
            runtime.spawn(async move {
                if let Ok(read) = this.read_usage().await {
                    this.usage.lock().1 = Some(read);
                }
            });
        }
        usage.1.clone()
    }
}

impl MetricsQuery for ClickHouseProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            numeric_history: true,
            histograms: true,
            events: true,
            traces: true,
            max_points: MAX_POINTS,
            storage: "clickhouse".into(),
        }
    }

    fn query(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        let query = query.clone();
        Box::pin(async move {
            query.validate()?;
            Ok(self.stepped(std::slice::from_ref(&query)).await?.pop().expect("a result per query"))
        })
    }

    /// Queries that differ only in their metric share one round trip.
    fn queries(&self, queries: &[HistoryQuery]) -> MetricsFuture<'_, Vec<Result<HistoryResult, String>>> {
        let mut answers: Vec<_> = queries.iter().map(|q| q.validate().map(|()| HistoryResult::default())).collect();
        let valid = queries.iter().enumerate().filter(|(i, _)| answers[*i].is_ok());
        let groups = groups(valid.map(|(i, query)| (i, HistoryQuery { metric: String::new(), ..query.clone() })));
        let asked: Vec<Vec<HistoryQuery>> =
            groups.iter().map(|(_, members)| members.iter().map(|&i| queries[i].clone()).collect()).collect();
        Box::pin(async move {
            let answered = futures_util::future::join_all(asked.iter().map(|group| self.stepped(group))).await;
            for ((_, members), answer) in groups.into_iter().zip(answered) {
                match answer {
                    Ok(results) => members.into_iter().zip(results).for_each(|(i, result)| answers[i] = Ok(result)),
                    Err(error) => members.into_iter().for_each(|i| answers[i] = Err(error.clone())),
                }
            }
            Ok(answers)
        })
    }

    fn series(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        let query = query.clone();
        Box::pin(async move {
            query.validate()?;
            let mut params = vec![
                ("metric".to_owned(), query.metric.clone()),
                ("from".to_owned(), (query.from_ms / 60_000 * 60_000).to_string()),
                ("to".to_owned(), query.to_ms.to_string()),
                ("limit".to_owned(), (query.max_series + 1).to_string()),
            ];
            let (db, filter) = (&self.database, filtered(&query, &mut params));
            let sql = format!(
                "SELECT DISTINCT node_id, incarnation, labels, kind, unit FROM `{db}`.samples_1m \
                 WHERE metric = {{metric:String}} AND bucket_ms BETWEEN {{from:UInt64}} AND {{to:UInt64}}{filter} \
                 ORDER BY node_id, incarnation, labels, kind, unit LIMIT {{limit:UInt64}} {LIMITS}"
            );
            let rows = lines::<SeriesRow>(&self.run(&sql, &params, Vec::new()).await?)?;
            let truncated = rows.len() > query.max_series;
            let series = rows.into_iter().take(query.max_series).map(|r| Series {
                labels: serde_json::from_str(&r.labels).unwrap_or_default(),
                node_id: r.node_id,
                incarnation: r.incarnation,
                kind: r.kind,
                unit: r.unit,
                summed: None,
                values: Vec::new(),
            });
            let series = series.collect();
            Ok(HistoryResult { from: query.start_ms(), step: query.step_ms, series, truncated, ..Default::default() })
        })
    }

    fn events(&self, query: &EventQuery) -> MetricsFuture<'_, Vec<ClusterEvent>> {
        let query = query.clone();
        Box::pin(async move {
            query.validate()?;
            let sql = format!(
                "SELECT id, time_ms, node_id, kind, subject, detail FROM `{}`.events FINAL \
                 WHERE time_ms BETWEEN {{from:UInt64}} AND {{to:UInt64}} \
                 AND ({{node:String}} = '' OR node_id = {{node:String}}) \
                 AND ({{kind:String}} = '' OR kind = {{kind:String}}) \
                 ORDER BY time_ms DESC, id LIMIT {{limit:UInt64}} {LIMITS}",
                self.database
            );
            let params = [
                ("from", query.from_ms.to_string()),
                ("to", query.to_ms.to_string()),
                ("limit", query.limit.to_string()),
                ("node", query.node),
                ("kind", query.kind),
            ];
            let params = params.map(|(name, value)| (name.to_owned(), value));
            lines(&self.run(&sql, &params, Vec::new()).await?)
        })
    }

    fn traces(&self, query: &TraceQuery) -> MetricsFuture<'_, Vec<TraceSummary>> {
        let query = query.clone();
        Box::pin(async move {
            query.validate()?;
            let sql = format!(
                "SELECT trace, min(start_us) AS first_us, max(start_us + intDiv(busy_ns, 1000)) AS last_us, \
                 count() AS n, max(failed) AS any_failed, arraySort(groupUniqArray(node_id)) AS nodes, \
                 argMinIf(name, start_us, parent = '') AS root, \
                 argMinIf(kind, start_us, parent = '') AS root_kind, \
                 argMinIf(actor_name, start_us, parent = '') AS root_actor_name, \
                 argMinIf(peer, start_us, parent = '') AS root_peer, \
                 argMinIf(actor_type, start_us, parent = '') AS root_actor, \
                 argMinIf(node_id, start_us, parent = '') AS root_node \
                 FROM `{}`.spans FINAL WHERE start_us BETWEEN {{from:UInt64}} AND {{to:UInt64}} GROUP BY trace \
                 HAVING (any_failed OR NOT {{failed:Bool}}) AND last_us - first_us >= {{min:UInt64}} \
                 AND ({{search:String}} = '' OR positionCaseInsensitiveUTF8(concat(trace, ' ', root, ' ', \
                 root_actor, ' ', root_actor_name, ' ', root_peer, ' ', arrayStringConcat(nodes, ' ')), {{search:String}}) > 0) \
                 ORDER BY first_us DESC, trace LIMIT {{limit:UInt64}} {LIMITS}",
                self.database
            );
            let params = [
                ("from", (query.from_ms * 1000).to_string()),
                ("to", (query.to_ms.saturating_mul(1000).saturating_add(999)).to_string()),
                ("failed", query.failed.to_string()),
                ("min", query.min_us.to_string()),
                ("search", query.search.unwrap_or_default()),
                ("limit", query.limit.to_string()),
            ];
            let params = params.map(|(name, value)| (name.to_owned(), value));
            let rows: Vec<TraceRow> = lines(&self.run(&sql, &params, Vec::new()).await?)?;
            let summary = |r: TraceRow| TraceSummary {
                root_start_us: if r.root_node.is_empty() { u64::MAX } else { r.first_us },
                trace: r.trace,
                start_us: r.first_us,
                end_us: r.last_us,
                spans: r.n,
                failed: r.any_failed,
                nodes: r.nodes,
                root: r.root,
                root_kind: r.root_kind,
                root_actor_name: r.root_actor_name,
                root_peer: r.root_peer,
                root_actor: r.root_actor,
                root_node: r.root_node,
            };
            Ok(rows.into_iter().map(summary).collect())
        })
    }

    fn dead_letters(&self, limit: usize) -> MetricsFuture<'_, Vec<DeadLetterRecord>> {
        Box::pin(async move {
            let sql = format!(
                "SELECT {LETTER_COLUMNS} FROM `{}`.dead_letters FINAL \
                 ORDER BY at_us DESC, node_id, seq DESC LIMIT {{limit:UInt64}} {LIMITS}",
                self.database
            );
            lines(&self.run(&sql, &[("limit".to_owned(), limit.to_string())], Vec::new()).await?)
        })
    }

    fn trace(&self, trace: u64) -> MetricsFuture<'_, Vec<SpanRecord>> {
        Box::pin(async move {
            let sql = format!(
                "SELECT {SPAN_COLUMNS} FROM `{}`.spans FINAL WHERE trace = {{trace:String}} \
                 ORDER BY start_us, span LIMIT 10000 {LIMITS}",
                self.database
            );
            lines(&self.run(&sql, &[("trace".to_owned(), hex(trace))], Vec::new()).await?)
        })
    }
}

#[derive(Deserialize)]
struct TraceRow {
    trace: String,
    first_us: u64,
    last_us: u64,
    n: u64,
    any_failed: bool,
    nodes: Vec<String>,
    root: String,
    root_kind: String,
    root_actor_name: String,
    root_peer: String,
    root_actor: String,
    root_node: String,
}

#[test]
fn a_step_reads_the_coarsest_rollup_that_divides_it() {
    let table = |step_ms| rollup_for(step_ms).map(|(table, _)| table);
    assert_eq!([table(59_000), table(90_000), table(1000)], [None; 3]);
    assert_eq!([table(60_000), table(4_200_000)], [Some("samples_1m"); 2]);
    assert_eq!([table(3_600_000), table(86_400_000)], [Some("samples_1h"); 2]);
}
