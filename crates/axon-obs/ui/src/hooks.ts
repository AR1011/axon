import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { useSearchParams } from "react-router";
import type { HistoryAnswer, NodeView } from "./api";
import { accentColor, setTheme } from "./appearance";
import { getJson } from "./api";
import { ago, uptime } from "./model/format";
import { STALE_MS } from "./model/live";
import { loadHistory, type HistoryData } from "./model/history";
import { historyStep } from "./model/series";
import { useLive } from "./store";

let now = Date.now();
const listeners = new Set<() => void>();
let ticker: ReturnType<typeof setInterval> | undefined;

function onTick(listener: () => void) {
  listeners.add(listener);
  ticker ??= setInterval(() => {
    now = Date.now();
    listeners.forEach((l) => l());
  }, 1000);
  return () => {
    listeners.delete(listener);
    if (!listeners.size) {
      clearInterval(ticker);
      ticker = undefined;
    }
  };
}

/** One shared second-hand; only the components that show relative time subscribe to it. */
export const useNow = () => useSyncExternalStore(onTick, () => now);

/** A chart's time axis: the last `span` ms, up to the second-hand. */
export function useDomain(span: number): [number, number] {
  const now = useNow();
  return useMemo(() => [now - span, now], [now, span]);
}

/** A piece of view state kept in the query string, so every view can be shared and reloaded. */
export function useUrlState<T extends string = string>(key: string, fallback: NoInfer<T>, push = false): [T, (value: T | null) => void] {
  const [params, setParams] = useSearchParams();
  const value = (params.get(key) as T | null) ?? fallback;
  const set = useCallback(
    (next: T | null) =>
      setParams(
        () => {
          const copy = new URLSearchParams(window.location.search);
          if (next == null || next === fallback || next === "") copy.delete(key);
          else copy.set(key, next);
          return copy;
        },
        { replace: !push },
      ),
    [key, fallback, push, setParams],
  );
  return [value, set];
}

function useRoot<T>(read: () => T): T {
  return useSyncExternalStore((listener) => {
    const observer = new MutationObserver(listener);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme", "style"] });
    return () => observer.disconnect();
  }, read);
}

export function useTheme(): ["dark" | "light", () => void] {
  const theme = useRoot(() => (document.documentElement.dataset.theme === "light" ? "light" : "dark"));
  return [theme, () => setTheme(theme === "dark" ? "light" : "dark")];
}

/** The resolved accent, for canvases; it changes with the theme as well as with the setting. */
export const useAccent = () => useRoot(accentColor);

/** Since when the live view is old: the server stopped answering, the stream went back in time, or nothing new came. Null while it is fresh. */
export function useStaleSince(): number | null {
  const now = useNow();
  const { error, rollback, overview, receivedAt } = useLive();
  return overview && (error || rollback || now - receivedAt > STALE_MS) ? receivedAt : null;
}

export type Tone = "success" | "warning" | "danger" | "muted";

/** The tone of a node's or an actor's lifecycle phase. */
export const phaseTone = (phase: string): Tone => (/running|active/i.test(phase) ? "success" : /drain|stopping|leav/i.test(phase) ? "warning" : "muted");

export function nodeStatus(view: NodeView, at: number): { label: string; tone: Tone; stale: boolean } {
  if (view.leftAt) return view.leaveReason === null ? { label: `Lost ${ago(at - view.leftAt)}`, tone: "danger", stale: true } : { label: `Left ${ago(at - view.leftAt)}`, tone: "muted", stale: true };
  if (!view.snapshot || view.error) return { label: "Unavailable", tone: "danger", stale: true };
  if (at - view.snapshot.timestamp_ms > STALE_MS) return { label: "Stale", tone: "warning", stale: true };
  const phase = view.snapshot.lifecycle.phase;
  return { label: phase.charAt(0).toUpperCase() + phase.slice(1), tone: phaseTone(phase), stale: false };
}

export const RANGES = { "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "6h": 21_600_000, "24h": 86_400_000, "7d": 604_800_000 } as const;
export type Range = keyof typeof RANGES;
export const RANGE_OPTIONS = (Object.keys(RANGES) as Range[]).map((value) => ({ value, label: value }));

export interface HistoryRequest {
  metrics: string[];
  span: number;
  points: number;
  node?: string;
  labels?: Record<string, string>;
  maxPoints?: number;
  maxSeries?: number;
  /** How a step folds the samples in it; the server keeps the last by default. */
  aggregation?: "last" | "mean" | "min" | "max";
  /** Series added up per node boot or over the cluster, by the server; `by` keeps these labels' values apart. */
  sum?: "node" | "cluster";
  by?: string[];
  refetchMs?: number;
  enabled?: boolean;
}

/** Said beside a chart whose store holds less than its range, as a small ring at scale does, or a young cluster. */
export const coverage = (data: HistoryData | undefined, span: number) =>
  data?.covers != null && data.covers < span - 2 * data.step ? `History covers the last ${uptime(data.covers)}` : null;

/** A page of charts in one request. Off when no source is available; the last answer stays while refetching. */
export function useHistory(request: HistoryRequest) {
  const available = useLive((s) => s.overview?.history.available ?? false);
  const [source] = useUrlState("history", "");
  const client = useQueryClient();
  const step = historyStep(request.span, request.points);
  const labels = useMemo(() => Object.entries(request.labels ?? {}).map(([k, v]) => [`label.${k}`, v]), [request.labels]);
  const query = useQuery({
    queryKey: ["history", source, request.metrics, request.node, labels, request.span, step, request.maxPoints, request.maxSeries, request.aggregation, request.sum, request.by],
    enabled: available && request.enabled !== false && request.metrics.length > 0,
    // Not more often than a step ends.
    refetchInterval: Math.max(request.refetchMs ?? 30_000, step),
    placeholderData: keepPreviousData,
    queryFn: ({ signal, queryKey }) => {
      const ask = (from: number, to: number) =>
        getJson<HistoryAnswer>(
          "history",
          {
            metrics: request.metrics.join(","),
            node_id: request.node,
            from_ms: from,
            to_ms: to,
            step_ms: step,
            max_points: request.maxPoints ?? 10_000,
            max_series: request.maxSeries ?? 128,
            history: source,
            aggregation: request.aggregation,
            sum: request.sum,
            by: request.by?.join(","),
            ...Object.fromEntries(labels),
          },
          signal,
        );
      return loadHistory(ask, request.span, step, Date.now(), client.getQueryData<HistoryData>(queryKey));
    },
  });
  return { available, data: available ? query.data : undefined, error: query.error?.message ?? null, loading: query.isLoading };
}

export function useEscape(active: boolean, close: () => void) {
  useEffect(() => {
    if (!active) return;
    const onKey = (event: KeyboardEvent) => event.key === "Escape" && close();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [active, close]);
}

/** Follows `target` over `ms` so that a number which changes once a second glides between readings. */
export function useGlide(target: number | null, ms = 900): number | null {
  const [shown, setShown] = useState(target);
  const from = useRef(target);
  useEffect(() => {
    const start = from.current;
    if (target == null || start == null || matchMedia("(prefers-reduced-motion: reduce)").matches) {
      from.current = target;
      setShown(target);
      return;
    }
    let frame = 0;
    const began = performance.now();
    const step = (t: number) => {
      const k = Math.min(1, (t - began) / ms);
      from.current = start + (target - start) * k;
      setShown(from.current);
      if (k < 1) frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [target, ms]);
  return shown;
}
