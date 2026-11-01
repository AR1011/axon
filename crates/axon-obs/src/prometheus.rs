//! A sample in the Prometheus text format, version 0.0.4, which every Prometheus reads.

use std::collections::BTreeMap;
use std::fmt::Write;

use axon::Histogram;

use crate::model::{Sample, Value};

/// A bound at every 16th bucket, at each power of four from 15 ns to 275 s: 18 exact bounds of the histogram's own
/// buckets, and `+Inf`.
const EVERY: usize = 16;

/// Its name, and whether its nanoseconds are given in seconds. A persisted actor's series are a family apart from its
/// type's, which already count it, so that a `sum` over a family counts each message once.
fn named(sample: &Sample) -> (String, bool) {
    let apart = if sample.labels.contains_key("actor") { "persisted_" } else { "" };
    let mut name = format!("axon_{apart}{}", sample.name.replace('.', "_"));
    let unit = match &*sample.unit {
        "nanoseconds" => "_seconds",
        "bytes" => "_bytes",
        "ratio" => "_ratio",
        _ => "",
    };
    if !name.ends_with(unit) {
        name.push_str(unit);
    }
    if matches!(sample.value, Value::Counter(_)) {
        name.push_str("_total");
    }
    (name, sample.unit == "nanoseconds")
}

/// Rust spells the infinities `inf`; Prometheus reads `+Inf`.
fn number(v: f64) -> String {
    match v {
        f64::INFINITY => "+Inf".into(),
        f64::NEG_INFINITY => "-Inf".into(),
        v => v.to_string(),
    }
}

/// `{a="x",b="y"}` with `extra` last, or nothing when there are none.
fn labels(sample: &Sample, extra: Option<(&str, &str)>) -> String {
    let mut out = String::new();
    for (key, value) in sample.labels.iter().map(|(k, v)| (&**k, v.as_str())).chain(extra) {
        out.push(if out.is_empty() { '{' } else { ',' });
        let _ = write!(out, "{key}=\"");
        for c in value.chars() {
            match c {
                '\\' => out.push_str(r"\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str(r"\n"),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    if !out.is_empty() {
        out.push('}');
    }
    out
}

fn histogram(out: &mut String, name: &str, sample: &Sample, h: &Histogram) {
    // `+Inf` and `_count` are the buckets' total, so they agree even where a snapshot's count ran ahead of them.
    let mut total = 0;
    for (bucket, n) in h.buckets.iter().enumerate() {
        total += n;
        if bucket % EVERY == EVERY - 1 && bucket < h.buckets.len() - 1 {
            let le = number(Histogram::upper_bound_ns(bucket) as f64 / 1e9);
            let _ = writeln!(out, "{name}_bucket{} {total}", labels(sample, Some(("le", &le))));
        }
    }
    let _ = writeln!(out, "{name}_bucket{} {total}", labels(sample, Some(("le", "+Inf"))));
    let _ = writeln!(out, "{name}_sum{} {}", labels(sample, None), number(h.sum_ns as f64 / 1e9));
    let _ = writeln!(out, "{name}_count{} {total}", labels(sample, None));
}

/// Families in name order, each once with its HELP and TYPE.
pub(crate) fn render(samples: &[Sample]) -> String {
    let mut families: BTreeMap<String, (bool, Vec<&Sample>)> = BTreeMap::new();
    for sample in samples {
        let (name, seconds) = named(sample);
        families.entry(name).or_insert((seconds, Vec::new())).1.push(sample);
    }
    let mut out = String::with_capacity(samples.len() * 128);
    for (name, (seconds, members)) in &families {
        let first = members[0];
        let unit = if *seconds { "seconds" } else { &*first.unit };
        let _ = writeln!(out, "# HELP {name} {}, in {unit}", first.name);
        let _ = writeln!(out, "# TYPE {name} {}", first.value.kind());
        for sample in members {
            let labels = labels(sample, None);
            match &sample.value {
                Value::Counter(n) if *seconds => {
                    let _ = writeln!(out, "{name}{labels} {}", number(*n as f64 / 1e9));
                }
                Value::Counter(n) => {
                    let _ = writeln!(out, "{name}{labels} {n}");
                }
                Value::Gauge(g) => {
                    let _ = writeln!(out, "{name}{labels} {}", number(if *seconds { g / 1e9 } else { *g }));
                }
                Value::Histogram(h) => histogram(&mut out, name, sample, &Histogram::from(h)),
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &'static str, unit: &'static str, labels: &[(&'static str, &str)], value: Value) -> Sample {
        let labels = labels.iter().map(|(k, v)| ((*k).into(), v.to_string())).collect();
        Sample { name: name.into(), unit: unit.into(), labels, value }
    }

    /// The format, pinned: names, units, seconds, escaping, the persisted family, and a histogram whose count ran ahead
    /// of its buckets still ending in a `+Inf` equal to its `_count`.
    #[test]
    fn a_sample_renders_as_this_text() {
        let mut h = Histogram { count: 9, sum_ns: 1000, ..Default::default() };
        (h.buckets[0], h.buckets[20]) = (2, 3);
        let samples = [
            sample("stream.retained", "bytes", &[], Value::Gauge(4096.0)),
            sample("runtime.worker.busy", "nanoseconds", &[("worker", "0")], Value::Counter(1_500_000_000)),
            sample("connection.rtt", "nanoseconds", &[("peer", "b"), ("direction", "out")], Value::Gauge(250_000.0)),
            sample("actor.processed", "messages", &[("actor", "pinned")], Value::Counter(7)),
            sample("actor.message.handler", "nanoseconds", &[("actor_type", "w"), ("message", "a \"b\"\\\nc")], {
                Value::Histogram((&h).into())
            }),
        ];
        let labels = r#"actor_type="w",message="a \"b\"\\\nc""#;
        let mut expected = String::from(
            "# HELP axon_actor_message_handler_seconds actor.message.handler, in seconds\n\
             # TYPE axon_actor_message_handler_seconds histogram\n",
        );
        for (le, n) in [
            ("0.000000015", 2),
            ("0.000000063", 5),
            ("0.000000255", 5),
            ("0.000001023", 5),
            ("0.000004095", 5),
            ("0.000016383", 5),
            ("0.000065535", 5),
            ("0.000262143", 5),
            ("0.001048575", 5),
            ("0.004194303", 5),
            ("0.016777215", 5),
            ("0.067108863", 5),
            ("0.268435455", 5),
            ("1.073741823", 5),
            ("4.294967295", 5),
            ("17.179869183", 5),
            ("68.719476735", 5),
            ("274.877906943", 5),
            ("+Inf", 5),
        ] {
            expected += &format!("axon_actor_message_handler_seconds_bucket{{{labels},le=\"{le}\"}} {n}\n");
        }
        expected += &format!(
            "axon_actor_message_handler_seconds_sum{{{labels}}} 0.000001\n\
             axon_actor_message_handler_seconds_count{{{labels}}} 5\n\
             # HELP axon_connection_rtt_seconds connection.rtt, in seconds\n\
             # TYPE axon_connection_rtt_seconds gauge\n\
             axon_connection_rtt_seconds{{direction=\"out\",peer=\"b\"}} 0.00025\n\
             # HELP axon_persisted_actor_processed_total actor.processed, in messages\n\
             # TYPE axon_persisted_actor_processed_total counter\n\
             axon_persisted_actor_processed_total{{actor=\"pinned\"}} 7\n\
             # HELP axon_runtime_worker_busy_seconds_total runtime.worker.busy, in seconds\n\
             # TYPE axon_runtime_worker_busy_seconds_total counter\n\
             axon_runtime_worker_busy_seconds_total{{worker=\"0\"}} 1.5\n\
             # HELP axon_stream_retained_bytes stream.retained, in bytes\n\
             # TYPE axon_stream_retained_bytes gauge\n\
             axon_stream_retained_bytes 4096\n"
        );
        assert_eq!(render(&samples), expected);
    }
}
