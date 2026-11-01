import { create } from "zustand";
import type { NodeView, Overview } from "./api";
import { apiUrl, getJson, signedOut, validOverview } from "./api";
import { natural } from "./model/format";
import { clusterTotals, ConnSeries, departures, NodeBuffer, type Totals } from "./model/live";
import type { P } from "./model/series";

export type Mode = "connecting" | "stream" | "polling";

export interface Live {
  overview: Overview | null;
  /** Bumped on every accepted observation; cheap identity for memoising derived models. */
  version: number;
  receivedAt: number;
  mode: Mode;
  error: string | null;
  /** The source clock went backwards: shown, but marked stale. */
  rollback: boolean;
  /** Nodes that recently left, with the last thing they reported: usually what the operator is looking for. */
  departed: NodeView[];
}

export const useLive = create<Live>(() => ({ overview: null, version: 0, receivedAt: 0, mode: "connecting", error: null, rollback: false, departed: [] }));

/** Said in place of any empty state, or a count, until the first overview: until then the console knows nothing, not that there is nothing. */
export const pending = (s: Pick<Live, "overview" | "error">) => (s.overview ? null : (s.error ?? "Connecting"));

/** Dead letters since each node started: the one total the sidebar and the Dead Letters page show. */
export const deadLetters = (s: Pick<Live, "overview">) => s.overview?.nodes.reduce((total, n) => total + (n.snapshot?.system.dead_letters ?? 0), 0) ?? 0;

/** Nodes that answered with a snapshot. */
export const availableNodes = (s: Pick<Live, "overview">) => s.overview?.nodes.filter((n) => n.snapshot && !n.error).length ?? 0;

export const connections = new ConnSeries();
export const nodeBuffer = new NodeBuffer();
/** The browser-only cluster trend, shown when no history is retained. */
export const session: Record<keyof Totals, P[]> = { nodes: [], actors: [], connections: [], throughput: [], queued: [] };
export let totals: Totals = { nodes: 0, actors: null, connections: 0, throughput: null, queued: 0 };

function trend(overview: Overview) {
  totals = clusterTotals(overview, connections);
  if (overview.timestamp_ms < (session.nodes.at(-1)?.[0] ?? 0)) Object.values(session).forEach((points) => (points.length = 0));
  for (const key of Object.keys(session) as (keyof Totals)[]) {
    session[key].push([overview.timestamp_ms, totals[key]]);
    if (session[key].length > 180) session[key].shift();
  }
}

function accept(value: unknown, mode?: Mode) {
  if (!validOverview(value)) {
    useLive.setState({ error: "The dashboard sent an overview this console cannot read" });
    return;
  }
  value.nodes.sort((a, b) => natural(a.node.id, b.node.id));
  connections.append(value);
  nodeBuffer.append(value);
  trend(value);
  useLive.setState((s) => ({
    overview: value,
    departed: departures(s.departed, s.overview?.nodes ?? [], value),
    version: s.version + 1,
    receivedAt: Date.now(),
    mode: mode ?? s.mode,
    error: null,
    rollback: !!s.overview && value.timestamp_ms < s.overview.timestamp_ms,
  }));
}

const POLL_MS = 5000;

/**
 * Stream first, poll as the fallback. A hidden tab holds neither, so the dashboard stops asking the nodes once no one
 * looks. Returns the teardown.
 */
export function subscribe(): () => void {
  const abort = new AbortController();
  let receipts = 0;
  let lastStream = 0;
  let polling = false;
  let stream: EventSource | null = null;

  async function poll(mode?: Mode) {
    if (polling) return;
    polling = true;
    const seen = receipts;
    try {
      const value = await getJson<unknown>("overview", {}, abort.signal);
      // A poll that began before a newer stream observation must not roll the view back.
      if (seen === receipts) accept(value, mode);
    } catch (error) {
      if (!abort.signal.aborted && !signedOut.aborted) useLive.setState({ error: error instanceof TypeError ? "Console server unreachable" : error instanceof Error ? error.message : String(error) });
    } finally {
      polling = false;
    }
  }

  // The dashboard sends a new stream its first frame at once; the poll waits a period for it.
  function open() {
    const opened = new EventSource(apiUrl("events"), { withCredentials: true });
    lastStream = Date.now();
    opened.addEventListener("overview", (event) => {
      receipts++;
      lastStream = Date.now();
      try {
        accept(JSON.parse((event as MessageEvent).data), "stream");
      } catch {
        useLive.setState({ error: "The live stream sent malformed data" });
      }
    });
    opened.onerror = () => {
      useLive.setState({ mode: "polling" });
      void poll("polling");
    };
    stream = opened;
  }
  function shown() {
    if (document.hidden || signedOut.aborted) {
      stream?.close();
      stream = null;
    } else if (!stream) open();
  }
  signedOut.addEventListener("abort", shown, { signal: abort.signal });
  document.addEventListener("visibilitychange", shown, { signal: abort.signal });
  shown();
  const timer = setInterval(() => {
    if (!stream) return;
    if (stream.readyState !== EventSource.OPEN || Date.now() - lastStream > POLL_MS) {
      useLive.setState({ mode: "polling" });
      void poll("polling");
    }
  }, POLL_MS);

  return () => {
    abort.abort();
    stream?.close();
    clearInterval(timer);
  };
}
