import type { Histogram, HistoryResult, NodeView, Series, SparseHistogram } from "../api";
import { duration } from "./format";

/** `axon::BUCKETS`: nanoseconds, exact below 16, then eight buckets to each power of two; the last is open. */
export const BUCKETS = 304;

/** `Histogram::upper_bound_ns`. */
export function boundNs(bucket: number): number {
  const shift = Math.max(0, Math.floor(bucket / 8) - 1);
  return bucket >= BUCKETS - 1 ? Infinity : (bucket - shift * 8 + 1) * 2 ** shift - 1;
}

/** A quantile as text: the open last bucket reads as more than the bound before it. */
export const boundText = (ns: number | null) => (ns === Infinity ? `> ${duration(boundNs(BUCKETS - 2))}` : duration(ns));

/** Whether a sparse histogram's run of buckets lies within `BUCKETS`, as `axon` checks one it reads. */
export const fits = (h: SparseHistogram) => Number.isInteger(h.first) && h.first >= 0 && Array.isArray(h.counts) && h.first + h.counts.length <= BUCKETS;

/**
 * All `BUCKETS` of a histogram axon sends sparse, as `first` and the `counts` of buckets `first..` up to the last
 * nonzero one, for the table or chart that reads them. A run past them leaves no buckets, so the histogram is invalid.
 */
export function expand(h: SparseHistogram): Histogram {
  const end = h.first + h.counts.length;
  const buckets = fits(h) ? [...Array<number>(h.first).fill(0), ...h.counts, ...Array<number>(BUCKETS - end).fill(0)] : [];
  return { buckets, count: h.count, sum_ns: h.sum_ns };
}

export function validHistogram(h: unknown): h is Histogram {
  const x = h as Histogram;
  return (
    !!x &&
    Array.isArray(x.buckets) &&
    x.buckets.length === BUCKETS &&
    x.buckets.every((n) => Number.isFinite(n) && n >= 0) &&
    Number.isFinite(x.count) &&
    Number.isFinite(x.sum_ns)
  );
}

/** The upper bound of the bucket that holds quantile `p`; a sparse histogram is read as it is, not expanded. */
export function quantile(h: Histogram | SparseHistogram, p: number): number | null {
  const [first, counts] = "buckets" in h ? [0, h.buckets] : [h.first, h.counts];
  const total = counts.reduce((a, b) => a + b, 0);
  if (!total) return null;
  const rank = Math.max(1, Math.ceil(total * p));
  let seen = 0;
  for (let i = 0; i < counts.length; i++) {
    seen += counts[i];
    if (seen >= rank) return boundNs(first + i);
  }
  return null;
}

export const mean = (h: { count: number; sum_ns: number }) => (h.count > 0 ? h.sum_ns / h.count : null);

/** Buckets `[first, end)` to draw: the nonzero ones with two either side, at least eight. */
export function drawn(counts: number[]): [number, number] {
  const [lowest, highest] = [counts.findIndex((n) => n > 0), counts.findLastIndex((n) => n > 0)];
  const first = Math.max(0, Math.min(lowest - 2, BUCKETS - 8));
  return [first, Math.min(BUCKETS, Math.max(first + 8, highest + 3))];
}

interface Observed {
  t: number;
  h: Histogram;
}

/** What one run recorded between two of its observations. */
export interface Interval {
  from: number;
  to: number;
  delta: SparseHistogram;
}

const bucket = (g: SparseHistogram, b: number) => g.counts[b - g.first] ?? 0;

/** Adds what `g` gained into `dense`, all `BUCKETS` of it. */
const addGain = (dense: number[], g: SparseHistogram) => g.counts.forEach((n, k) => (dense[g.first + k] += n));

/** Consecutive cumulative observations of one run with no reset between them. */
export function intervals(points: Observed[]): Interval[] {
  const sorted = points.filter((p) => validHistogram(p.h)).sort((a, b) => a.t - b.t);
  const out = [];
  for (let i = 1; i < sorted.length; i++) {
    const [a, b] = [sorted[i - 1], sorted[i]];
    if (b.t <= a.t) continue;
    const counts = b.h.buckets.map((n, k) => n - a.h.buckets[k]);
    if (counts.some((n) => n < 0) || b.h.count < a.h.count) continue;
    out.push({ from: a.t, to: b.t, delta: { first: 0, counts, count: b.h.count - a.h.count, sum_ns: b.h.sum_ns - a.h.sum_ns } });
  }
  return out;
}

/** A stored series' gains, each over the step before its own: a series is one node, boot and label set, or a sum of them. */
export const gained = (r: HistoryResult, s: Series): Interval[] =>
  (s.gains ?? []).flatMap((delta, i) => (delta ? [{ from: r.from + (i - 1) * r.step, to: r.from + i * r.step, delta }] : []));

/**
 * What was recorded inside the window, from complete intervals only. The mean is reported only
 * when its population is the one in the buckets.
 */
export function windowHistogram(spans: Interval[], from: number, to: number): Histogram | null {
  const inside = spans.filter((i) => i.from >= from && i.to <= to);
  if (!inside.length) return null;
  const buckets = Array<number>(BUCKETS).fill(0);
  for (const { delta } of inside) addGain(buckets, delta);
  const count = inside.reduce((sum, i) => sum + i.delta.count, 0);
  const matches = count === buckets.reduce((a, b) => a + b, 0);
  return { buckets, count, sum_ns: matches ? inside.reduce((sum, i) => sum + i.delta.sum_ns, 0) : NaN };
}

export interface Heat {
  columns: number[];
  /** Rows are buckets `first..first + buckets`. */
  first: number;
  buckets: number;
  /** `[column, row, samples per second]`; an unobserved cell is absent. */
  cells: [number, number, number][];
  max: number;
  /** Named rows in place of buckets, from `rowHeat`. */
  rows?: string[];
}

/** Sampled time in `on_message` per actor type and message label, added over the nodes: each type's labels largest first, the types busiest first. */
export function handlerTime(views: NodeView[]): Map<string, { label: string; ns: number }[]> {
  const sums = new Map<string, Map<string, number>>();
  for (const view of views)
    for (const type of view.snapshot?.actor_types ?? [])
      for (const m of type.totals.timings?.messages ?? []) {
        const labels = sums.get(type.actor_type) ?? new Map<string, number>();
        sums.set(type.actor_type, labels.set(m.label, (labels.get(m.label) ?? 0) + m.handler.sum_ns));
      }
  const total = (parts: { ns: number }[]) => parts.reduce((sum, p) => sum + p.ns, 0);
  const types = [...sums].map(([type, labels]) => [type, [...labels].map(([label, ns]) => ({ label, ns })).sort((a, b) => b.ns - a.ns)] as const);
  return new Map(types.sort((a, b) => total(b[1]) - total(a[1])));
}

/** A category × time grid: one row per named series, each cell the highest value observed in its column. */
export function rowHeat(rows: { name: string; points: [number, number][] }[], from: number, to: number, maxColumns = 120): Heat {
  const count = Math.max(1, Math.min(maxColumns, Math.ceil((to - from) / 1000)));
  const width = (to - from) / count;
  const highest = new Map<number, number>();
  rows.forEach((row, r) => {
    for (const [t, v] of row.points) {
      if (t < from || t >= to) continue;
      const at = Math.floor((t - from) / width) * rows.length + r;
      highest.set(at, Math.max(highest.get(at) ?? 0, v));
    }
  });
  const cells: Heat["cells"] = [...highest].map(([at, v]) => [Math.floor(at / rows.length), at % rows.length, v]);
  const columns = Array.from({ length: count }, (_, c) => from + c * width);
  return { columns, first: 0, buckets: rows.length, cells, max: cells.reduce((m, c) => Math.max(m, c[2]), 0), rows: rows.map((r) => r.name) };
}

/** Each run's intervals apart: runs at once add up, one run's own intervals share its time. */
export function heat(runs: Interval[][], from: number, to: number, maxColumns = 180): Heat {
  const spans = runs.map((r) => r.filter((i) => i.to > from && i.from < to));
  // The columns start at the first observation, as a line chart's axis does, not across a range history does not cover.
  const earliest = spans.flat().reduce((m, s) => Math.min(m, s.from), Infinity);
  if (earliest > from && earliest < to) from = earliest;
  const count = Math.max(1, Math.min(maxColumns, Math.ceil((to - from) / 1000)));
  const width = (to - from) / count;
  const columns = Array.from({ length: count }, (_, c) => from + c * width);
  const seen = Array<number>(BUCKETS).fill(0);
  for (const { delta } of spans.flat()) addGain(seen, delta);
  const [first, end] = drawn(seen);
  const size = end - first;
  // Each run's rate over the time it was observed in a column; the runs' rates add up.
  const rates = new Float64Array(count * size);
  const observed = new Uint8Array(count);
  const samples = new Float64Array(count * size);
  const seconds = new Float64Array(count);
  for (const mine of spans) {
    samples.fill(0);
    seconds.fill(0);
    for (const span of mine) {
      const firstColumn = Math.max(0, Math.floor((span.from - from) / width));
      const lastColumn = Math.min(count - 1, Math.floor((span.to - 1 - from) / width));
      for (let c = firstColumn; c <= lastColumn; c++) {
        const overlap = Math.min(span.to, columns[c] + width) - Math.max(span.from, columns[c]);
        if (overlap <= 0) continue;
        seconds[c] += overlap / 1000;
        const share = overlap / (span.to - span.from);
        for (let row = 0; row < size; row++) samples[c * size + row] += bucket(span.delta, first + row) * share;
      }
    }
    for (let c = 0; c < count; c++) {
      if (!seconds[c]) continue;
      observed[c] = 1;
      for (let row = 0; row < size; row++) rates[c * size + row] += samples[c * size + row] / seconds[c];
    }
  }
  const cells: Heat["cells"] = [];
  for (let c = 0; c < count; c++) if (observed[c]) for (let row = 0; row < size; row++) cells.push([c, row, rates[c * size + row]]);
  return { columns, first, buckets: size, cells, max: cells.reduce((m, c) => Math.max(m, c[2]), 0) };
}

/** A line per quantile `ps` of what the runs recorded in each interval, at the interval's end: stored series share their steps. */
export function quantileLines(spans: Interval[], ps: number[], from: number, to: number): { name: string; points: [number, number | null][] }[] {
  const ends = new Map<number, number[]>();
  for (const { to: end, delta } of spans) {
    if (end <= from || end > to) continue;
    const sum = ends.get(end) ?? Array<number>(BUCKETS).fill(0);
    addGain(sum, delta);
    ends.set(end, sum);
  }
  const times = [...ends.keys()].sort((a, b) => a - b);
  // ponytail: the open last bucket draws at the bound below it, not as a gap.
  const at = (t: number, p: number) => {
    const q = quantile({ buckets: ends.get(t)!, count: 0, sum_ns: 0 }, p);
    return q === Infinity ? boundNs(BUCKETS - 2) : q;
  };
  return ps.map((p) => ({ name: `p${Math.round(p * 1000) / 10}`, points: times.map((t) => [t, at(t, p)]) }));
}
