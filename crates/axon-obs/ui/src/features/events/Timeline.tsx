import { useQuery } from "@tanstack/react-query";
import { useCallback, useMemo, type ReactNode } from "react";
import { Chart } from "../../charts/Chart";
import { lanesOption, type Mark } from "../../charts/options";
import type { ClusterEvent } from "../../api";
import { getJson } from "../../api";
import { Count, Dot, Empty, Stamp, STAMP } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { Identity } from "../../components/Identity";
import { Panel } from "../../components/Panel";
import { Search } from "../../components/Search";
import { Select } from "../../components/Select";
import { SortHead, useSort, type Accessors } from "../../components/sort";
import { RANGE_OPTIONS, RANGES, useNow, useUrlState, type Range } from "../../hooks";
import { ago } from "../../model/format";
import { useLive } from "../../store";
import { NodeName } from "../../components/Flag";

const FIELDS: Accessors<ClusterEvent> = { time: (e) => e.time_ms, event: (e) => e.kind, node: (e) => e.node_id, subject: (e) => e.subject, detail: (e) => e.detail };

/** Every kind a store can hold, so a kind filtered on the server can be swapped for another. */
const KINDS = [
  "joined", "left", "leaving", "updated", "restarted", "unreachable", "reachable", "resync", "discovery_unavailable",
  "acquired", "lease_lost", "handed_off", "handoff_failed", "eager_restart", "actor_restarted", "gave_up",
  "parent_lost", "reset", "orphaned", "parent_restored", "disk_failing", "disk_recovered",
  "isolated", "isolation_ended", "acceptor_warming", "acceptor_warm", "quota_mismatch", "tls_rotated", "authorizer_changed",
  "lane_down", "lane_up", "host_tcp_limits", "seed_timings",
  "login", "login_failed", "login_limited", "logout", "signed_out_everyone", "console_tls_rotated",
];

/** Trouble is amber, a departure is grey, and only an arrival or a recovery is green; `replaced` is what a store kept from before it was `restarted`. */
const tone = (kind: string): Mark["tone"] =>
  /fail|limited|unavailable|unreachable|gap|lost|resync|restart|reset|orphan|gave_up|^isolated|warming|mismatch|lane_down|tcp_limits|timings/.test(kind) ? "warning" : /left|leaving|replaced|removed|down|logout|signed_out|handed_off|rotated|changed/.test(kind) ? "muted" : "success";

/** Each node a lane, each event a mark on it; clicking one filters the table to its node and kind. */
function EventLanes({ events, domain, onPick }: { events: ClusterEvent[]; domain: [number, number]; onPick: (node: string, kind: string) => void }) {
  const lanes = useMemo(() => [...new Set(events.map((e) => e.node_id))].sort(), [events]);
  const marks = useMemo<Mark[]>(() => events.map((e) => ({ time_ms: e.time_ms, lane: e.node_id, kind: e.kind, tone: tone(e.kind), text: [e.kind.replaceAll("_", " "), e.subject, e.detail].filter(Boolean).join(" · ") })), [events]);
  const build = useCallback((theme: "dark" | "light") => lanesOption(marks, lanes, domain, theme), [marks, lanes, domain]);
  const pick = useCallback((data: unknown) => {
    const { value, kind } = data as { value: [number, string]; kind: string };
    onPick(value[1], kind);
  }, [onPick]);
  if (!events.length) return null;
  return (
    <div data-testid="event-lanes" className="shrink-0 border-b border-border" style={{ height: Math.min(40 + lanes.length * 24, 220) }}>
      <Chart build={build} label={`Events on ${lanes.length} node${lanes.length === 1 ? "" : "s"} over time`} onClick={pick} />
    </div>
  );
}

/** Long decimal or hex ids inside a sentence become chips; the raw digits never reach the text. */
function detailText(detail: string): ReactNode[] {
  return detail.split(/(\b\d{16,39}\b|\b[0-9a-f]{32}\b)/).map((part, i) => (i % 2 ? <Identity key={i} value={part} label="Process incarnation" /> : part));
}

/** The newest few events of one node on a single line, for pages whose subject is something else. */
export function RecentEvents({ node, onMore }: { node: string; onMore: () => void }) {
  const live = useLive((s) => s.overview?.events);
  const now = useNow();
  const recent = useMemo(() => (live ?? []).filter((e) => e.node_id === node).sort((a, b) => b.time_ms - a.time_ms).slice(0, 3), [live, node]);
  if (!recent.length) return null;
  return (
    <section aria-label="Recent events" className="flex shrink-0 items-center gap-4 overflow-hidden rounded-md border border-border bg-panel px-3 py-1.5 text-[11px]">
      <h2 className="shrink-0 text-[10px] font-normal text-muted">Recent events</h2>
      {recent.map((e) => (
        <span key={e.id} className="min-w-0 truncate capitalize" title={e.detail}><Dot tone={tone(e.kind)} />{e.kind.replaceAll("_", " ")}<span className="ml-1.5 text-muted normal-case">{ago(now - e.time_ms)}</span></span>
      ))}
      <button type="button" onClick={onMore} className="ml-auto shrink-0 text-[10px] text-muted hover:text-text">All events →</button>
    </section>
  );
}

/** `pageRange` is the owning page's range: the panel follows it and shows no range control of its own. */
export function Timeline({ node, compact, pageRange }: { node?: string; compact?: boolean; pageRange?: Range }) {
  const live = useLive((s) => s.overview?.events);
  const retainedEvents = useLive((s) => s.overview?.history.sources.some((x) => x.node === s.overview?.history.node) && s.overview.history.available);
  const [source] = useUrlState("history", "");
  const [chosen, setRange] = useUrlState<Range | "live">("er", "1h");
  const [kind, setKind] = useUrlState("ek", "");
  const [chosenNode, setNode] = useUrlState("en", "");
  const [q, setQ] = useUrlState("eq", "");
  const now = useNow();
  const range = retainedEvents && !compact ? (pageRange ?? chosen) : "live";
  // A page about one node filters to it; the events page lets the operator pick one.
  const on = node ?? chosenNode;
  const retained = useQuery({
    queryKey: ["timeline", source, range, on, kind],
    enabled: !compact && range !== "live" && !!retainedEvents,
    refetchInterval: 15_000,
    // Filtered on the server, so a busy node's events do not crowd the others out of the 1000.
    queryFn: ({ signal }) => getJson<ClusterEvent[]>("timeline", { from_ms: Date.now() - RANGES[range as Range], to_ms: Date.now() + 60_000, limit: 1000, history: source, node: on, kind }, signal),
  });
  const events = useMemo(() => {
    const all = (range !== "live" && retained.data) || live || [];
    // Without a store the page range still bounds the live events, so the list matches the tab's count.
    const since = pageRange && range === "live" ? now - RANGES[pageRange] : 0;
    return all.filter((e) => (!on || e.node_id === on) && e.time_ms >= since);
  }, [range, pageRange, retained.data, live, on, now]);
  const kinds = useMemo(() => [...new Set([...KINDS, ...(live ?? []).map((e) => e.kind), ...events.map((e) => e.kind)])].sort(), [live, events]);
  const nodes = useMemo(() => [...new Set([...(live ?? []), ...events].map((e) => e.node_id).concat(chosenNode ? [chosenNode] : []))].sort(), [live, events, chosenNode]);
  const shown = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return compact ? events : events.filter((e) => (!kind || e.kind === kind) && (!needle || `${e.node_id} ${e.kind} ${e.subject} ${e.detail}`.toLowerCase().includes(needle)));
  }, [events, kind, q, compact]);
  const domain = useMemo<[number, number]>(() => (range === "live" ? [Math.min(now - 60_000, ...shown.map((e) => e.time_ms)), now] : [now - RANGES[range as Range], now]), [range, shown, now]);
  const pick = useCallback((picked: string, pickedKind: string) => {
    if (!node) setNode(picked);
    setKind(pickedKind);
  }, [node, setNode, setKind]);
  const span = range === "live" ? 0 : RANGES[range as Range];
  const sort = useSort(shown, FIELDS, "e", "time", "desc");
  const limit = compact ? 5 : range === "live" ? 100 : 1000;
  const rows = sort.sorted.slice(0, limit);
  return (
    <Panel
      flat={compact}
      className="flex-1"
      title={compact ? undefined : <>Events <Count>{shown.length}</Count></>}
      controls={
        !compact && (
          <>
            <Search label="Search events" value={q} onChange={setQ} className="w-[180px] max-[700px]:w-[110px]" />
            {!node && <Select label="Event node" value={chosenNode} onChange={setNode} options={[{ value: "", label: "All nodes" }, ...nodes.map((n) => ({ value: n, label: n }))]} />}
            <Select label="Event kind" value={kind} onChange={setKind} options={[{ value: "", label: "All kinds" }, ...kinds.map((k) => ({ value: k, label: k.replaceAll("_", " ") }))]} />
            {retainedEvents && !pageRange && <Select label="Event range" value={range} onChange={setRange} options={[{ value: "live", label: "Live session" }, ...RANGE_OPTIONS]} />}
            <ExportButton dataset={{ name: "Events", count: shown.length, capped: range !== "live" && events.length >= 1000, filters: [["range", range === "live" ? "live session" : `last ${range}`], on && ["node", on], kind && ["kind", kind], q && ["search", q]], rows: () => sort.sorted }} />
          </>
        )
      }
      footer={<span>{rows.length} of {events.length} events{retained.error ? ` · ${retained.error.message}` : ""}</span>}
    >
      {!compact && <EventLanes events={shown} domain={domain} onPick={pick} />}
      {!rows.length ? (
        <Empty title={events.length ? "No events match the filters" : `No events${on ? ` for ${on}` : ""}${range === "live" ? " in this session" : ` in the last ${range}`}`} detail={events.length ? "Use a different kind or search." : "A node that disappeared without an event possibly failed."} />
      ) : (
        <div className="scroll-y flex-1 overflow-x-auto">
          <table className="min-w-[680px]">
            <thead>
              <tr>
                <SortHead sort={sort} field="time" label="Time" width={STAMP} />
                <SortHead sort={sort} field="event" label="Event" width="16%" />
                <SortHead sort={sort} field="node" label="Node" width="16%" />
                <SortHead sort={sort} field="subject" label="Subject" width="22%" />
                <SortHead sort={sort} field="detail" label="Detail" width="30%" />
              </tr>
            </thead>
            <tbody>
              {rows.map((event) => (
                <tr key={event.id}>
                  <td><Stamp ms={event.time_ms} span={span} /></td>
                  <td className="font-sans capitalize"><Dot tone={tone(event.kind)} />{event.kind.replaceAll("_", " ")}</td>
                  <td><NodeName id={event.node_id} className="underline decoration-border underline-offset-2 hover:decoration-current" /></td>
                  <td className="truncate" title={event.subject}>{event.subject}</td>
                  <td className="font-sans">{detailText(event.detail)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
