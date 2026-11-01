import { useMemo, useState, type FormEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { getJson, type HistoryResult, type SeriesList } from "../../api";
import { HeatPlot, HistogramPlot, LinePlot, PlotFrame } from "../../charts/Plots";
import { Empty } from "../../components/atoms";
import { Combo, type Item } from "../../components/Combo";
import { HelpDialog, Part } from "../../components/HelpDialog";
import { Select } from "../../components/Select";
import { coverage, RANGE_OPTIONS, RANGES, useDomain, useHistory, useUrlState, type Range } from "../../hooks";
import { exploreLines, exploreUnit, findMetrics, highest, labelNames, labelPairs, METRICS, parseLabels, typing, withLabel } from "../../model/explore";
import { byUnit } from "../../model/format";
import { gained, heat, quantileLines, windowHistogram } from "../../model/histogram";
import { latest } from "../../model/series";
import { useLive } from "../../store";

type Aggregation = "last" | "mean" | "min" | "max";

const AGGREGATIONS: { value: Aggregation; label: string }[] = [
  { value: "last", label: "Last in step" },
  { value: "mean", label: "Mean in step" },
  { value: "min", label: "Min in step" },
  { value: "max", label: "Max in step" },
];

const metricItems = (typed: string): Item[] => findMetrics(typed).map((name) => ({ value: name, about: METRICS[name][1], tag: METRICS[name][0], group: name.split(".")[0] }));

/** A field's text, set again whenever the address changes it, as after Back. */
function useDraft(value: string) {
  const [draft, setDraft] = useState(value);
  const [shown, setShown] = useState(value);
  if (shown !== value) {
    setShown(value);
    setDraft(value);
  }
  return [draft, setDraft] as const;
}

/** Under the chart: a histogram's quantiles, distribution and heatmap over the range, then a chart split each other way the series can be; its button splits the chart above so. */
function Drilldown({ result, by, from, to, domain, onSplit }: { result: HistoryResult; by: string; from: number; to: number; domain: [number, number]; onSplit: (by: string) => void }) {
  const { series } = result;
  const unit = series[0] ? exploreUnit(series[0].kind, series[0].unit) : "count";
  // Apart from `domain`, which moves every second: a heatmap built again each time flickers.
  const histogram = useMemo(() => {
    if (series[0]?.kind !== "histogram") return null;
    const runs = series.map((s) => gained(result, s));
    return { quantiles: quantileLines(runs.flat(), [0.5, 0.9, 0.99], from, to), window: windowHistogram(runs.flat(), from, to), heat: heat(runs, from, to) };
  }, [result, series, from, to]);
  const splits = useMemo(
    () =>
      ["node", ...labelNames(series)]
        .filter((d) => d !== by)
        .map((d) => ({ by: d, ...exploreLines(result, d) }))
        .filter((split) => split.lines.length + split.hidden > 1),
    [result, series, by],
  );
  if (!histogram && !splits.length) return null;
  return (
    <div className="tiles grid shrink-0 auto-rows-[240px] grid-cols-3 max-[1200px]:grid-cols-2 max-[760px]:grid-cols-1">
      {histogram && (
        <>
          <PlotFrame title="Quantiles" info="For each step, the bucket limit below which half, nine in ten and 99 in 100 of the samples of the step fell, over all series.">
            <LinePlot lines={histogram.quantiles} unit="ns" domain={domain} label="Quantiles per step" />
          </PlotFrame>
          <PlotFrame title="Distribution" info="The samples that the nodes recorded over the range, from complete observed intervals. Resets and missing intervals add nothing.">
            <HistogramPlot h={histogram.window} label="Distribution over the range" empty="No samples in the range" />
          </PlotFrame>
          <PlotFrame title="Over time" info="The sample rate for each bucket and time column. A cell without observations is blank, not zero.">
            <HeatPlot grid={histogram.heat} label="Heatmap over the range" />
          </PlotFrame>
        </>
      )}
      {splits.map((split) => (
        <PlotFrame
          key={split.by}
          title={`By ${split.by}`}
          footer={
            <button type="button" onClick={() => onSplit(split.by)} className="hover:text-text">
              Split the chart by {split.by}
            </button>
          }
        >
          <LinePlot lines={split.lines} unit={unit} domain={domain} label={`By ${split.by}`} />
        </PlotFrame>
      ))}
    </div>
  );
}

export function ExploreHelp() {
  return (
    <HelpDialog button="What is this" title="What the explore page shows">
      <Part title="Any stored metric">
        Each metric that the collector stores, with the names of the persisted samples in the telemetry guide, over the range of the page. The chart shows a counter as its rate per second, a gauge as its value, and a histogram as the rate of its observations.
        A gauge that has no meaning as a sum, such as a ratio, a percentage or a duration, shows its highest series.
      </Part>
      <Part title="Added up or split">
        One line adds all series: each node, each boot, each label value. Split by node or by a label to get one line for each, the highest first, at most eight. Write labels as k=v, with commas between them, to keep only the series that have those labels. The field shows each pair that the series of the metric have.
      </Part>
      <Part title="Below the chart">
        There is a chart for each other split of the series, by node or by a label. Each chart has a button that applies its split to the chart above. A histogram adds the quantiles for each step, its distribution over the range, and a heatmap of it over time.
      </Part>
      <Part title="A chart is a link">
        The address keeps the metric, range, step aggregation, node, split and labels. Thus you can share or bookmark a chart as it is.
      </Part>
    </HelpDialog>
  );
}

export function ExplorePage() {
  const [metric, setMetric] = useUrlState("metric", "runtime.actors");
  const [range, setRange] = useUrlState<Range>("range", "1h");
  const [aggregation, setAggregation] = useUrlState<Aggregation>("agg", "last");
  const [node, setNode] = useUrlState("node", "");
  const [by, setBy] = useUrlState("by", "");
  const [labelText, setLabels] = useUrlState("labels", "");
  const [draft, setDraft] = useDraft(metric);
  const [labelDraft, setLabelDraft] = useDraft(labelText);
  const nodes = useLive((s) => s.overview?.nodes);
  const span = RANGES[range] ?? RANGES["1h"];
  const labels = useMemo(() => parseLabels(labelText), [labelText]);
  const history = useHistory({ metrics: [metric], span, points: 144, node: node || undefined, labels, aggregation, maxSeries: 128 });
  const result = history.data?.byMetric[metric];
  // The labels field offers every pair of the metric's series: with pairs kept, the store lists the series they leave
  // out too, over the range, once per metric, node and range, again only every five minutes.
  const [source] = useUrlState("history", "");
  const filtering = Object.keys(labels).length > 0;
  const offered = useQuery({
    queryKey: ["series", source, metric, node, span],
    enabled: history.available && !!metric && filtering,
    staleTime: 300_000,
    refetchInterval: 300_000,
    queryFn: ({ signal }) => getJson<SeriesList>("series", { metric, node_id: node || undefined, from_ms: Date.now() - span, history: source }, signal),
  });
  const listed = filtering ? offered.data?.series : result?.series;
  const pairs = useMemo(() => labelPairs(listed ?? [], labels), [listed, labels]);
  const { lines, hidden } = useMemo(() => exploreLines(result, by, metric), [result, by, metric]);
  const first = result?.series[0];
  const unit = first ? exploreUnit(first.kind, first.unit) : "count";
  const domain = useDomain(span);

  const splits = [{ value: "", label: first && highest(first.kind, first.unit) ? "Highest" : "Added up" }, { value: "node", label: "By node" }, ...labelNames(result?.series ?? []).map((l) => ({ value: l, label: `By ${l}` }))];
  if (!splits.some((s) => s.value === by)) splits.push({ value: by, label: `By ${by}` });
  const nodeOptions = [{ value: "", label: "All nodes" }, ...(nodes ?? []).map((n) => ({ value: n.node.id, label: n.node.id }))];
  if (node && !nodeOptions.some((n) => n.value === node)) nodeOptions.push({ value: node, label: node });

  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setMetric(draft.trim() || null);
    setLabels(labelDraft.trim() || null);
  };
  const note = [
    history.error ?? history.data?.errors.join(" · "),
    result?.truncated && `The limits of the store stopped the result at ${result.series.length} series: filter by node or labels`,
    hidden > 0 && `${hidden} more lines not drawn`,
    coverage(history.data, span),
  ].filter(Boolean);
  return (
    <>
      <h1 className="sr-only-h1">Explore</h1>
      <div className="scroll-y flex flex-1 flex-col gap-2.5">
        <form onSubmit={submit} className="flex shrink-0 flex-wrap items-center gap-2 bg-panel px-3 py-2">
          <Combo
            label="Search metrics"
            value={draft}
            current={metric}
            onChange={setDraft}
            items={metricItems}
            onPick={(name) => {
              setDraft(name);
              setMetric(name);
            }}
            select
            empty="No stored metric matches. Enter shows a chart of the name as you typed it."
            hint="finds names and descriptions"
            className="w-[320px]"
          />
          <Combo
            label="Labels, k=v, k=v"
            value={labelDraft}
            onChange={setLabelDraft}
            items={(typed) => {
              const word = typing(typed).toLowerCase();
              return pairs
                .filter((p) => p.pair.toLowerCase().includes(word))
                .map((p) => ({ value: p.pair, about: labels[p.key] && `in place of ${p.key}=${labels[p.key]}`, tag: `${p.series} series`, group: p.key }));
            }}
            onPick={(pair, typed) => {
              const text = withLabel(labelDraft, pair, typed);
              setLabelDraft(text);
              setLabels(text);
            }}
            empty={offered.isLoading ? "Wait for the labels…" : "The series of this metric have no other label. Enter keeps the labels as you typed them."}
            hint="from this metric's series"
            className="w-[220px]"
          />
          <button type="submit" className="h-7 rounded-md bg-accent px-3 text-[11px] font-[550] text-bg">Chart</button>
          <span className="ml-auto flex flex-wrap items-center gap-2">
            <Select label="Node" value={node} onChange={setNode} options={nodeOptions} />
            <Select label="Split" value={by} onChange={setBy} options={splits} />
            <Select label="Step aggregation" value={aggregation} onChange={setAggregation} options={AGGREGATIONS} />
            <Select label="Range" value={range} onChange={setRange} options={RANGE_OPTIONS} />
          </span>
        </form>
        <div className="flex h-[clamp(280px,50vh,460px)] shrink-0 flex-col [&>section]:flex-1">
          <PlotFrame
            title={metric}
            value={lines.length === 1 ? byUnit(unit, latest(lines[0].points)) : undefined}
            info="Stored history only: the samples of the collector, a few seconds older than the live pages."
            footer={
              <>
                <span>{result ? `${result.series.length} series · ${!first ? "no samples" : highest(first.kind, first.unit) ? "highest value" : first.kind === "gauge" ? "value" : "rate per second"} · ${aggregation} per step` : "Stored history"}</span>
                {note.length > 0 && <span className="text-warning">{note.join(" · ")}</span>}
              </>
            }
          >
            {!history.available ? (
              <Empty title="No stored history" detail="Explore reads the metrics store. Start a MetricsService." />
            ) : (
              <LinePlot
                lines={lines}
                unit={unit}
                domain={domain}
                label={`${metric} over the last ${range}`}
                empty={history.loading ? { title: "Wait…" } : { title: `No samples of ${metric}`, detail: "Compare the name with the telemetry guide, and examine the node and the labels." }}
              />
            )}
          </PlotFrame>
        </div>
        {history.available && result && <Drilldown result={result} by={by} from={history.data!.from} to={history.data!.to} domain={domain} onSplit={setBy} />}
      </div>
    </>
  );
}
