import type { HistoryResult, Series } from "../api";
import { lastKnown, stepRate } from "./history";
import type { P } from "./series";

export type Aggregate = "count" | "sum" | "max" | "rate";

/**
 * One cluster-wide line out of `series` of `r`: per step the nodes known, or their known values added, the highest,
 * or the gains added as a rate. The line ends at the last step every node still reporting reached: past it a node
 * yet to report would read as a drop. For a sum the server can make, ask it for one (`sum` on `useHistory`); this
 * cut is the same as its, `summed` in `provider.rs`.
 */
export function aggregate(r: HistoryResult, series: Series[], mode: Aggregate, max = 180): P[] {
  // Per node, not per series: a closed connection or an earlier incarnation is no node yet to report.
  const reached = new Map<string, number>();
  for (const s of series) reached.set(s.node_id, Math.max(reached.get(s.node_id) ?? -1, lastKnown(s)));
  const ends = [...reached.values()].filter((end) => end >= 0);
  if (!ends.length) return [];
  const newest = Math.max(...ends);
  const until = Math.min(...ends.filter((end) => end + 2 >= newest));
  const out: P[] = [];
  for (let i = 0; i <= until; i++) {
    const known = series.filter((s) => s.values[i] != null);
    const values = known.map((s) => s.values[i]!);
    const sum = values.reduce((a, b) => a + b, 0);
    const value =
      mode === "count" ? new Set(known.map((s) => s.node_id)).size || null
      : !values.length ? null
      : mode === "max" ? Math.max(...values)
      : mode === "rate" ? stepRate(sum, r.step)
      : sum;
    out.push([r.from + i * r.step, value]);
  }
  return out.slice(-max);
}
