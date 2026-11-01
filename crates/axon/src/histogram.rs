use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use crate::clock::nanos;

/// Eight buckets to each power of two of nanoseconds, up to about 1030 s.
pub const BUCKETS: usize = 304;
pub const MAX_MESSAGE_LABELS: usize = 16;

/// Log-linear buckets of nanoseconds, HDR style: exact below 16 ns, then eight to each power of two, so a bucket's
/// upper bound is at most 12.5 % above any value in it. The last bucket is overflow, above about 1030 s.
///
/// It serializes sparse, as `first` and the `counts` of buckets `first..` up to the last nonzero one.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "Sparse<Vec<u64>>"))]
pub struct Histogram {
    pub buckets: [u64; BUCKETS],
    pub count: u64,
    pub sum_ns: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self { buckets: [0; BUCKETS], count: 0, sum_ns: 0 }
    }
}

impl Histogram {
    pub fn upper_bound_ns(bucket: usize) -> u64 {
        let shift = (bucket / 8).saturating_sub(1);
        if bucket >= BUCKETS - 1 { u64::MAX } else { (((bucket - shift * 8) as u64 + 1) << shift) - 1 }
    }

    pub fn record(&mut self, d: Duration) {
        let (bucket, ns) = bucket_of(d);
        self.buckets[bucket] += 1;
        self.count += 1;
        self.sum_ns = self.sum_ns.saturating_add(ns);
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += b;
        }
        self.count += other.count;
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
    }

    /// What was recorded since `earlier`, an earlier copy of the same histogram; `None` where a bucket or the count went
    /// down, as across a restart.
    pub fn since(&self, earlier: &Histogram) -> Option<Histogram> {
        let mut gained = Histogram {
            count: self.count.checked_sub(earlier.count)?,
            sum_ns: self.sum_ns.checked_sub(earlier.sum_ns)?,
            ..Default::default()
        };
        for ((g, now), then) in gained.buckets.iter_mut().zip(&self.buckets).zip(&earlier.buckets) {
            *g = now.checked_sub(*then)?;
        }
        Some(gained)
    }

    /// Upper bound of the bucket containing the `p` quantile (`0.0..=1.0`): never below the true value and at most
    /// 12.5 % above it, except in the overflow bucket, above about 1030 s, whose bound is `u64::MAX` ns.
    pub fn percentile(&self, p: f64) -> Option<Duration> {
        if self.count == 0 {
            return None;
        }
        let rank = ((self.count as f64) * p.clamp(0.0, 1.0)).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return Some(Duration::from_nanos(Self::upper_bound_ns(i)));
            }
        }
        None
    }
}

fn bucket_of(d: Duration) -> (usize, u64) {
    let ns = nanos(d);
    let shift = (63 - (ns | 1).leading_zeros()).saturating_sub(3);
    ((shift as usize * 8 + (ns >> shift) as usize).min(BUCKETS - 1), ns)
}

/// The buckets from the first to the last nonzero one; none when all are zero.
fn nonzero(counts: &[u64]) -> std::ops::Range<usize> {
    match (counts.iter().position(|&n| n > 0), counts.iter().rposition(|&n| n > 0)) {
        (Some(first), Some(last)) => first..last + 1,
        _ => 0..0,
    }
}

/// A `Histogram` as it is kept and sent rather than recorded into: the counts from its first to its last nonzero
/// bucket, so a snapshot of one that saw a few latencies is a few words, not all `BUCKETS`. It serializes as the
/// `Histogram` it stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "Sparse<Vec<u64>>"))]
pub struct HistogramSnapshot(std::sync::Arc<[u64]>);

impl Default for HistogramSnapshot {
    fn default() -> Self {
        Self::new(0, &[], 0, 0)
    }
}

impl HistogramSnapshot {
    /// One allocation, `[first, count, sum_ns, counts..]`, which a clone shares: telemetry clones every link's into
    /// its sample and keeps the ones it stored.
    fn new(first: usize, counts: &[u64], count: u64, sum_ns: u64) -> Self {
        let run = nonzero(counts);
        let first = if run.is_empty() { 0 } else { first + run.start };
        Self([first as u64, count, sum_ns].iter().chain(&counts[run]).copied().collect())
    }

    /// The bucket `counts()[0]` is.
    pub fn first(&self) -> usize {
        self.0[0] as usize
    }

    pub fn counts(&self) -> &[u64] {
        &self.0[3..]
    }

    pub fn count(&self) -> u64 {
        self.0[1]
    }

    pub fn sum_ns(&self) -> u64 {
        self.0[2]
    }
}

impl From<&Histogram> for HistogramSnapshot {
    fn from(h: &Histogram) -> Self {
        Self::new(0, &h.buckets, h.count, h.sum_ns)
    }
}

impl From<&HistogramSnapshot> for Histogram {
    fn from(s: &HistogramSnapshot) -> Self {
        let mut h = Histogram { count: s.count(), sum_ns: s.sum_ns(), ..Default::default() };
        h.buckets[s.first()..s.first() + s.counts().len()].copy_from_slice(s.counts());
        h
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
struct Sparse<C> {
    first: usize,
    counts: C,
    count: u64,
    sum_ns: u64,
}

#[cfg(feature = "serde")]
impl serde::Serialize for Histogram {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let run = nonzero(&self.buckets);
        Sparse { first: run.start, counts: &self.buckets[run], count: self.count, sum_ns: self.sum_ns }.serialize(s)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for HistogramSnapshot {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        Sparse { first: self.first(), counts: self.counts(), count: self.count(), sum_ns: self.sum_ns() }.serialize(s)
    }
}

#[cfg(feature = "serde")]
impl TryFrom<Sparse<Vec<u64>>> for HistogramSnapshot {
    type Error = &'static str;

    fn try_from(s: Sparse<Vec<u64>>) -> Result<Self, Self::Error> {
        if s.first.checked_add(s.counts.len()).is_none_or(|end| end > BUCKETS) {
            return Err("histogram buckets past axon::BUCKETS");
        }
        Ok(Self::new(s.first, &s.counts, s.count, s.sum_ns))
    }
}

#[cfg(feature = "serde")]
impl TryFrom<Sparse<Vec<u64>>> for Histogram {
    type Error = &'static str;

    fn try_from(s: Sparse<Vec<u64>>) -> Result<Self, Self::Error> {
        Ok(Histogram::from(&HistogramSnapshot::try_from(s)?))
    }
}

/// Lock-free recorder for shared paths. A snapshot taken during a record may be off by one.
pub struct AtomicHistogram {
    buckets: [AtomicU64; BUCKETS],
    sum_ns: AtomicU64,
    /// The lowest and highest bucket recorded into, so a snapshot reads only those between, not all `BUCKETS`:
    /// telemetry takes one of each of every link's six every second.
    lowest: AtomicUsize,
    highest: AtomicUsize,
}

impl Default for AtomicHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: AtomicU64::new(0),
            lowest: AtomicUsize::new(BUCKETS),
            highest: AtomicUsize::new(0),
        }
    }
}

impl AtomicHistogram {
    pub fn record(&self, d: Duration) {
        let (bucket, ns) = bucket_of(d);
        if bucket < self.lowest.load(Relaxed) {
            self.lowest.fetch_min(bucket, Relaxed);
        }
        if bucket > self.highest.load(Relaxed) {
            self.highest.fetch_max(bucket, Relaxed);
        }
        self.buckets[bucket].fetch_add(1, Relaxed);
        self.sum_ns.fetch_add(ns, Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let (first, highest) = (self.lowest.load(Relaxed), self.highest.load(Relaxed));
        // nothing recorded yet, or a first record's lowest not seen yet
        if first > highest {
            return HistogramSnapshot::default();
        }
        let run: Vec<u64> = self.buckets[first..=highest].iter().map(|b| b.load(Relaxed)).collect();
        HistogramSnapshot::new(first, &run, run.iter().sum(), self.sum_ns.load(Relaxed))
    }
}

/// Sampled count and handler time for one `Actor::label`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct MessageTimings {
    pub label: &'static str,
    pub handler: Histogram,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Timings {
    pub sample_every: u32,
    pub mailbox_wait: Histogram,
    pub handler: Histogram,
    pub turn: Histogram,
    pub timer_lag: Histogram,
    /// At most `MAX_MESSAGE_LABELS` labels per actor, the rest folded into `"other"`; timings merged across actors (a
    /// family's generations, a type's totals) may hold more.
    pub messages: Vec<MessageTimings>,
}

impl Timings {
    pub(crate) fn record_message(&mut self, label: &'static str, d: Duration) {
        let at = self.messages.iter().position(|m| m.label == label).unwrap_or_else(|| {
            let label = if self.messages.len() < MAX_MESSAGE_LABELS { label } else { "other" };
            self.messages.iter().position(|m| m.label == label).unwrap_or_else(|| {
                self.messages.push(MessageTimings { label, handler: Histogram::default() });
                self.messages.len() - 1
            })
        });
        self.messages[at].handler.record(d);
    }

    pub fn merge(&mut self, other: &Timings) {
        self.sample_every = self.sample_every.max(other.sample_every);
        self.mailbox_wait.merge(&other.mailbox_wait);
        self.handler.merge(&other.handler);
        self.turn.merge(&other.turn);
        self.timer_lag.merge(&other.timer_lag);
        for m in &other.messages {
            match self.messages.iter_mut().find(|mine| mine.label == m.label) {
                Some(mine) => mine.handler.merge(&m.handler),
                None => self.messages.push(m.clone()),
            }
        }
    }
}

/// First and every Nth observation.
pub(crate) fn sampled(every: u32, ordinal: u64) -> bool {
    every != 0 && (ordinal - 1).is_multiple_of(every as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_and_percentiles() {
        let mut h = Histogram::default();
        h.record(Duration::from_nanos(1));
        h.record(Duration::from_nanos(1000));
        h.record(Duration::from_nanos(1001));
        h.record(Duration::from_secs(5000));
        assert_eq!((h.buckets[1], h.buckets[63], h.buckets[BUCKETS - 1]), (1, 2, 1));
        assert_eq!(h.percentile(0.5), Some(Duration::from_nanos(1023)));
        assert_eq!(h.percentile(1.0), Some(Duration::from_nanos(u64::MAX)));
        assert!(sampled(4, 1) && !sampled(4, 2) && sampled(4, 5) && !sampled(0, 1));

        let shared = AtomicHistogram::default();
        shared.record(Duration::from_nanos(1001));
        let mut merged = Histogram::from(&shared.snapshot());
        merged.merge(&h);
        assert_eq!((merged.count, merged.buckets[63]), (5, 3));
        let gained = merged.since(&h).expect("merged holds all of h");
        assert_eq!((gained.count, gained.sum_ns, gained.buckets[63], gained.buckets[1]), (1, 1001, 1, 0));
        let mut restarted = h.clone();
        (restarted.buckets[1], restarted.count) = (0, 1);
        assert_eq!(merged.since(&restarted).map(|g| g.count), Some(4));
        assert_eq!(restarted.since(&h), None, "a bucket went down");

        let mut t = Timings::default();
        for i in 0..MAX_MESSAGE_LABELS + 3 {
            t.record_message(Box::leak(format!("m{i}").into_boxed_str()), Duration::from_nanos(1));
        }
        assert_eq!(t.messages.len(), MAX_MESSAGE_LABELS + 1);
        assert_eq!(t.messages.last().map(|m| (m.label, m.handler.count)), Some(("other", 3)));
    }

    #[test]
    fn a_snapshot_reads_and_keeps_only_the_run_recorded_into() {
        let shared = AtomicHistogram::default();
        assert_eq!(shared.snapshot(), HistogramSnapshot::default());
        [1000, 1001, 1100].into_iter().for_each(|ns| shared.record(Duration::from_nanos(ns)));
        // outside the buckets recorded into, so a snapshot that reads it has read past them
        shared.buckets[200].store(9, Relaxed);
        let snapshot = shared.snapshot();
        assert_eq!(
            (snapshot.first(), snapshot.counts(), snapshot.count(), snapshot.sum_ns()),
            (63, &[2, 1][..], 3, 3101)
        );
        let dense = Histogram::from(&snapshot);
        assert_eq!(
            (dense.buckets[63], dense.buckets[64], dense.percentile(1.0)),
            (2, 1, Some(Duration::from_nanos(1151)))
        );
        assert_eq!(HistogramSnapshot::from(&dense), snapshot);

        let edges = AtomicHistogram::default();
        [Duration::MAX, Duration::ZERO].into_iter().for_each(|d| edges.record(d));
        let all = edges.snapshot();
        assert_eq!((all.first(), all.counts().len(), all.count()), (0, BUCKETS, 2));
    }

    #[test]
    fn percentiles_of_known_distributions_are_at_most_an_eighth_above() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut unit = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 11) as f64 / (1u64 << 53) as f64
        };
        let uniform: Vec<u64> = (0..100_000).map(|_| 1 + (unit() * 1e6) as u64).collect();
        let log_uniform: Vec<u64> = (0..100_000).map(|_| 2f64.powf(unit() * 30.0) as u64).collect();
        let tail: Vec<u64> =
            (0..100_000).map(|_| if unit() < 0.9 { 40_000 + (unit() * 20_000.0) as u64 } else { 3e8 as u64 }).collect();
        for (name, mut values) in [("uniform", uniform), ("log-uniform", log_uniform), ("bimodal", tail)] {
            let mut h = Histogram::default();
            values.iter().for_each(|&ns| h.record(Duration::from_nanos(ns)));
            values.sort_unstable();
            for p in [0.0, 0.01, 0.1, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0] {
                let rank = ((values.len() as f64 * p).ceil() as usize).max(1);
                let exact = values[rank - 1];
                let got = h.percentile(p).map(|d| d.as_nanos() as u64);
                assert!(
                    got.is_some_and(|got| exact <= got && got <= exact + exact / 8),
                    "{name} p{p}: {got:?} for {exact}"
                );
            }
        }
    }

    #[test]
    fn a_bucket_holds_the_values_up_to_its_bound() {
        for i in 0..BUCKETS - 1 {
            let top = Histogram::upper_bound_ns(i);
            assert_eq!(
                (bucket_of(Duration::from_nanos(top)).0, bucket_of(Duration::from_nanos(top + 1)).0),
                (i, i + 1)
            );
        }
        // Pinned, and pinned the same in the console's model.test.ts.
        let bounds = [0, 7, 15, 16, 17, 100, 302, 303].map(Histogram::upper_bound_ns);
        assert_eq!(bounds, [0, 7, 15, 17, 19, 26_623, 1_030_792_151_039, u64::MAX]);
    }
}
