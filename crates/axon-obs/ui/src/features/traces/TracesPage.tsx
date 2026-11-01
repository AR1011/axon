import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router";
import type { TraceList, TraceSummary } from "../../api";
import { getJson } from "../../api";
import { Count, Dot, Empty, Stamp, STAMP } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { Info } from "../../components/Info";
import { Panel } from "../../components/Panel";
import { rowLink, rowNav } from "../../components/rowLink";
import { Search } from "../../components/Search";
import { Select } from "../../components/Select";
import { SortHead, useSort, type Accessors } from "../../components/sort";
import { RANGE_OPTIONS, RANGES, useUrlState, type Range } from "../../hooks";
import { duration } from "../../model/format";
import { traceParts } from "../../model/trace";
import { NodeName } from "../../components/Flag";
import { useLive } from "../../store";

const FIELDS: Accessors<TraceSummary> = {
  time: (t) => t.start_us,
  verb: (t) => traceParts(t).verb,
  target: (t) => traceParts(t).target,
  node: (t) => t.root_node,
  spans: (t) => t.spans,
  nodes: (t) => t.nodes.length,
  duration: (t) => t.end_us - t.start_us,
};

const SLOWER = [
  { value: "", label: "Any duration" },
  { value: "1000", label: "≥ 1 ms" },
  { value: "10000", label: "≥ 10 ms" },
  { value: "100000", label: "≥ 100 ms" },
  { value: "1000000", label: "≥ 1 s" },
];

export const tracePath = (trace: string) => `/traces/${trace}`;
const LIMIT = 500;

export function TracesPage() {
  const [source] = useUrlState("history", "");
  const [range, setRange] = useUrlState<Range>("range", "15m");
  const [q, setQ] = useUrlState("q", "");
  const [slower, setSlower] = useUrlState("slower", "");
  const [outcome, setOutcome] = useUrlState<"" | "failed">("outcome", "");
  const navigate = useNavigate();
  const span = RANGES[range] ?? RANGES["15m"];
  const list = useQuery({
    queryKey: ["traces", source, span, q, slower, outcome],
    // A longer range asks the store to scan more for rows that move less: 5 s at 15 minutes, a minute from 6 hours.
    refetchInterval: Math.min(60_000, Math.max(5_000, span / 180)),
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) =>
      getJson<TraceList>(
        "traces",
        {
          from_ms: Date.now() - span,
          to_ms: Date.now() + 60_000,
          limit: LIMIT,
          q: q.trim(),
          min_us: slower,
          failed: outcome === "failed" || null,
          history: source,
        },
        signal,
      ),
  });
  const traces = list.data?.traces ?? [];
  const sort = useSort(traces, FIELDS, "t", "time", "desc");
  const longest = Math.max(1, ...traces.map((t) => t.end_us - t.start_us));
  const unreachable = Object.keys(list.data?.errors ?? {});
  const nodes = useLive((s) => s.overview?.nodes.length ?? 0);
  const failed = list.error?.message ?? (!traces.length && list.data?.source === "live" && unreachable.length >= Math.max(1, nodes) ? `No answer from ${unreachable.join(", ")}.` : null);
  const capped = traces.length >= LIMIT;
  return (
    <>
      <h1 className="sr-only-h1">Traces</h1>
      <Panel
        className="flex-1"
        title={
          <>
            Traces <Count>{traces.length}</Count>
          </>
        }
        controls={
          <>
            <Search label="Search traces" value={q} onChange={setQ} className="w-[180px] max-[700px]:w-[110px]" />
            <Select
              label="Outcome"
              value={outcome}
              onChange={setOutcome}
              options={[
                { value: "", label: "Any outcome" },
                { value: "failed", label: "Failed" },
              ]}
            />
            <Select label="Minimum duration" value={slower} onChange={setSlower} options={SLOWER} />
            <Select label="Trace range" value={range} onChange={setRange} options={RANGE_OPTIONS} />
            <ExportButton dataset={{ name: "Traces", count: traces.length, capped, filters: [["range", `last ${range}`], q && ["search", q], slower && ["slower than", `${Number(slower) / 1000} ms`], outcome && ["outcome", outcome]], rows: () => sort.sorted }} />
          </>
        }
        footer={
          <>
            <span>
              {list.data?.source === "live" ? "From the nodes" : "From the metrics store"}
              {unreachable.length ? ` · no answer from ${unreachable.join(", ")}` : ""}
              {list.error ? ` · ${list.error.message}` : ""}
            </span>
            {capped && <span className="text-warning">{sort.field === "duration" ? `Slowest of the newest ${LIMIT} · set a minimum duration to search the whole range` : `Newest ${LIMIT} · use a shorter range or a filter`}</span>}
            <span className="ml-auto">
              <Info label="About this list">
                {list.data?.source === "live"
                  ? `No store answered (${list.data.store_error ?? "no store"}), so the list shows the spans that the nodes still keep.`
                  : "A trace shows when the collector stored its spans, a few seconds after the trace ran."}
              </Info>
            </span>
          </>
        }
      >
        {!traces.length && failed ? (
          <Empty title="Could not load traces" detail={failed} />
        ) : !traces.length ? (
          <Empty
            title={list.isPending ? "Wait for the traces" : q || slower || outcome ? "No traces match the filters" : `No traces in the last ${range}`}
            detail={q || slower || outcome ? "Use a longer range or fewer filters." : "A trace starts where the application calls TraceContext::root, or on one message in N when the system samples."}
          />
        ) : (
          <div className="scroll-y flex-1 overflow-x-auto">
            <table className="min-w-[720px]">
              <thead>
                <tr>
                  <SortHead sort={sort} field="time" label="Started" width={STAMP} />
                  <SortHead sort={sort} field="verb" label="Root" width="11%" />
                  <SortHead sort={sort} field="node" label="From" width="13%" />
                  <SortHead sort={sort} field="target" label="To" width="21%" />
                  <SortHead sort={sort} field="spans" label="Spans" width="7%" align="right" />
                  <SortHead sort={sort} field="nodes" label="Nodes" width="7%" align="right" />
                  <SortHead sort={sort} field="duration" label="Duration" width="24%" />
                </tr>
              </thead>
              <tbody {...rowNav}>
                {sort.sorted.map((t) => {
                  const took = t.end_us - t.start_us;
                  const { verb, target } = traceParts(t);
                  return (
                    <tr key={t.trace} {...rowLink(navigate, tracePath(t.trace))} className="cursor-pointer">
                      <td>
                        <Stamp ms={t.start_us / 1000} span={span} />
                      </td>
                      <td className="font-sans">
                        <Dot tone={t.failed ? "danger" : "success"} title={t.failed ? "A handler or a call failed" : "No failure recorded"} />
                        {t.root_node ? verb : <span className="text-muted" title="No node and no store holds the first span of this trace">not held</span>}
                      </td>
                      <td><NodeName id={t.root_node || t.nodes[0]} link={false} /></td>
                      <td className="truncate" title={target}>
                        {t.root_node ? target : ""}
                      </td>
                      <td className="text-right">{t.spans}</td>
                      <td className="text-right" title={t.nodes.join(", ")}>
                        {t.nodes.length}
                      </td>
                      <td>
                        <span className="flex items-center gap-2">
                          <span className="w-[62px] shrink-0 text-right">{duration(took * 1000)}</span>
                          <span className="h-1 flex-1 rounded-full bg-raised">
                            <span
                              className={`block h-1 rounded-full ${t.failed ? "bg-danger" : "bg-accent"}`}
                              style={{
                                width: `${Math.max(1, (took / longest) * 100)}%`,
                              }}
                            />
                          </span>
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  );
}
