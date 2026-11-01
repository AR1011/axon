import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { Link, useParams, useSearchParams } from "react-router";
import type { Actor, ActorPage, Histogram } from "../../api";
import { getJson } from "../../api";
import { COLORS } from "../../charts/options";
import { HeatPlot, HistogramPlot, LinePlot, PlotFrame } from "../../charts/Plots";
import { Badge, Empty, Facts, Since } from "../../components/atoms";
import { Identity } from "../../components/Identity";
import { Info } from "../../components/Info";
import { Segmented } from "../../components/Segmented";
import { phaseTone, useDomain, useNow, useUrlState } from "../../hooks";
import { bytes, count, duration, shortType, sum } from "../../model/format";
import { expand, heat, intervals, mean, quantile, validHistogram } from "../../model/histogram";
import { gaugePoints, ratePoints } from "../../model/series";
import { useLive } from "../../store";
import { NodeName } from "../../components/Flag";
import { nodePath } from "../select";
import { lettersPath } from "./Inventory";

const GAP = 15_000;
const SPANS = { "1m": 60_000, "5m": 300_000, "15m": 900_000 };
const TIMINGS = [["mailbox_wait", "Mailbox wait"], ["handler", "Handler duration"], ["turn", "Execution turn"]] as const;

interface Observation {
  t: number;
  actor: Actor;
}
/** Browser-only history of the actor on screen: bounded, and cleared when the identity changes. */
const observed = { key: "", samples: [] as Observation[] };

export function ActorDetail() {
  const { nodeId = "", actorId = "" } = useParams();
  const [search] = useSearchParams();
  const pinned = search.get("incarnation") ?? "";
  const now = useNow();
  const current = useLive((s) => s.overview?.nodes.find((n) => n.node.id === nodeId)?.node.incarnation);
  const [window_, setWindow] = useUrlState<keyof typeof SPANS>("span", "5m");
  const [view, setView] = useUrlState<"distributions" | "heatmaps">("timing", "distributions");
  const replaced = !!pinned && !!current && pinned !== current;

  const query = useQuery({
    queryKey: ["actor", nodeId, actorId, pinned],
    enabled: !replaced,
    refetchInterval: 2000,
    queryFn: async ({ signal }) => {
      const page = await getJson<ActorPage>("actors", { node_id: nodeId, incarnation: pinned, id: actorId, timings: true, limit: 1 }, signal);
      const key = `${nodeId}|${pinned}|${actorId}`;
      if (observed.key !== key) Object.assign(observed, { key, samples: [] });
      const actor = page.actors[0];
      if (actor) observed.samples = [...observed.samples, { t: Date.now(), actor }].slice(-600);
      return { actor, truncated: page.truncated, samples: observed.samples };
    },
  });

  const span = SPANS[window_];
  const samples = query.data?.samples;
  const charts = useMemo(() => {
    const all = samples ?? [];
    const pick = (read: (a: Actor) => number | null) => all.map((s) => ({ t: s.t, v: read(s.actor) }));
    const rejected = (a: Actor) => sum(a.diagnostics.rejected);
    return {
      rate: [
        { name: "Processed", points: ratePoints(pick((a) => a.diagnostics.processed), GAP) },
        { name: "Admitted", points: ratePoints(pick((a) => a.diagnostics.admitted), GAP) },
        { name: "Rejected", color: COLORS.rejected, points: ratePoints(pick(rejected), GAP) },
      ],
      depth: [{ name: "Queued messages", points: gaugePoints(pick((a) => a.diagnostics.mailbox_depth), GAP) }],
      memory: [{ name: "Accounted bytes", points: gaugePoints(pick((a) => a.diagnostics.mailbox_bytes), GAP) }],
    };
  }, [samples]);
  const domain = useDomain(span);

  const back = <><Link to="/actors" className="text-accent hover:underline">Back to actors</Link> · <Link to={`${nodePath(nodeId)}?view=events`} className="text-accent hover:underline">Events for {nodeId}</Link></>;
  if (replaced) return <Empty title="Actor belongs to a previous engine incarnation" detail="The node restarted after someone made this link. The page does not show data from the new engine under this link.">{back}</Empty>;
  if (query.error) return <Empty title="Actor unavailable" detail={query.error.message}>{back}</Empty>;
  const actor = query.data?.actor;
  if (!actor) return <Empty title={query.isLoading ? "Wait for the actor" : query.data?.truncated ? "Actor not found in the bounded inspection scan" : "Actor no longer present on this engine"}>{!query.isLoading && back}</Empty>;
  const d = actor.diagnostics;
  const letters = d.dead_letters;
  const timing = (key: (typeof TIMINGS)[number][0]): Histogram | null => {
    const h = d.timings && expand(d.timings[key]);
    return h && validHistogram(h) ? h : null;
  };

  return (
    <div className="scroll-y flex flex-1 flex-col tiles">
      <header className="flex shrink-0 items-center gap-2">
        <h1 className="mono truncate text-[14px] font-semibold tracking-[-0.25px]">
          {actor.name ?? actorId} <span className="font-sans text-[11px] font-normal text-muted">on <NodeName id={nodeId} className="hover:underline" /></span>
        </h1>
        <span className="ml-auto"><Badge tone={phaseTone(actor.phase)}>{actor.phase}</Badge></span>
      </header>
      <Facts
        className="shrink-0 rounded-md border border-border bg-panel"
        items={[
          ["Actor", <Identity key="a" value={actorId} label="Actor id" />],
          ["Incarnation", <Identity key="i" value={pinned || current || ""} label="Process incarnation" />],
          ["Type", <code key="t" className="mono">{shortType(actor.actor_type)}</code>],
          ["Up", <Since key="u" us={actor.started_us} label="Started" />],
          ...(actor.restarted_us != null ? [["Since restart", <Since key="s" us={actor.restarted_us} label="Restarted" />] as const] : []),
          ["Restarts", count(actor.restarts)],
          ["Jobs", count(d.jobs)],
          ["Timers", count(d.timers)],
          ["Dead letters", letters ? <Link to={lettersPath(nodeId, actor)} className="text-warning underline underline-offset-2">{count(letters)}</Link> : "0"],
        ] as const}
      />
      <div className="flex shrink-0 items-center gap-2 text-[10px]">
        <span className="text-accent">● Live session</span>
        <Info label="About the live session">Only this browser keeps the observations of each actor: at most 600 points. A reload removes them. Retained history is for each actor type.</Info>
        <span className="ml-auto"><Segmented label="Window" value={window_} onChange={setWindow} options={(Object.keys(SPANS) as (keyof typeof SPANS)[]).map((value) => ({ value, label: value }))} /></span>
      </div>
      <div className="grid shrink-0 grid-cols-3 tiles max-[1200px]:grid-cols-2 max-[700px]:grid-cols-1">
        <PlotFrame title="Message rate" footer={<><span>Admitted {count(d.admitted)}</span><span>Processed {count(d.processed)}</span><span>Rejected {count(sum(d.rejected))}</span></>}>
          <LinePlot lines={charts.rate} unit="/s" domain={domain} label="Processed, admitted and rejected messages per second from live observations" empty={{ title: "No second sample yet", detail: "A rate needs two observations from the time that this page is open. History does not keep data for each actor." }} />
        </PlotFrame>
        <PlotFrame title="Mailbox depth" footer={<><span>Queued {count(d.mailbox_depth)}</span><span>Oldest wait {duration(d.oldest_message_age_ns)}</span></>}>
          <LinePlot lines={charts.depth} unit="count" domain={domain} label="Queued messages from live observations" />
        </PlotFrame>
        <PlotFrame title="Mailbox memory" footer={<><span>Accounted {bytes(d.mailbox_bytes)}</span><span>Bounded mailbox</span></>}>
          <LinePlot lines={charts.memory} unit="bytes" domain={domain} label="Accounted mailbox bytes from live observations" />
        </PlotFrame>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <h2>Sampled timings</h2>
        <span className="text-[10px] text-muted">{view === "distributions" ? "Lifetime samples · independent populations" : "Interval samples · selected browser time window"}</span>
        <span className="ml-auto"><Segmented label="Timing view" value={view} onChange={setView} options={[{ value: "distributions", label: "Distributions" }, { value: "heatmaps", label: "Heatmaps" }]} /></span>
      </div>
      <div className="grid shrink-0 grid-cols-3 tiles max-[1200px]:grid-cols-2 max-[700px]:grid-cols-1">
        {TIMINGS.map(([key, title]) => (
          <PlotFrame key={key} title={title}>
            {view === "distributions" ? (
              <HistogramPlot h={timing(key)} label={`${title} distribution, lifetime samples`} empty={d.timings ? "No timing samples" : "Timing not enabled"} />
            ) : (
              <HeatPlot
                grid={heat([intervals((samples ?? []).flatMap((s) => (s.actor.diagnostics.timings ? [{ t: s.t, h: expand(s.actor.diagnostics.timings[key]) }] : [])))], now - span, now)}
                label={`${title} heatmap, interval samples`}
              />
            )}
          </PlotFrame>
        ))}
      </div>
      {!!d.timings?.messages.length && (
        <section className="shrink-0 overflow-hidden rounded-md border border-border bg-panel">
          <header className="flex h-[34px] items-center border-b border-border px-3"><h2>Handler time by message</h2></header>
          <table>
            <thead><tr>{["Message", "Samples", "Mean", "p95 ≤"].map((h) => <th key={h}><div className="flex h-[30px] items-center px-3 text-[10px] text-muted">{h}</div></th>)}</tr></thead>
            <tbody>
              {d.timings.messages.map((m) => (
                <tr key={m.label}>
                  <td>{m.label}</td>
                  <td>{count(m.handler.count)}</td>
                  <td>{duration(mean(m.handler))}</td>
                  <td>{duration(quantile(m.handler, 0.95))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
