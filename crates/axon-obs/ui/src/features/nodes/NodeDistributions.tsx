import { useMemo } from "react";
import type { NodeView } from "../../api";
import { HeatPlot, HistogramPlot, PlotFrame } from "../../charts/Plots";
import { useNow } from "../../hooks";
import { gained, heat, windowHistogram } from "../../model/histogram";
import { DISTRIBUTIONS, useNodeHistory } from "./NodeCharts";

const METRICS = DISTRIBUTIONS.map((d) => d.metric);

export function NodeDistributions({ view, span }: { view: NodeView; span: number }) {
  const now = useNow();
  const incarnation = view.snapshot?.incarnation ?? view.node.incarnation;
  const history = useNodeHistory(view, span, METRICS);
  const [from, to] = [history.data?.from ?? now - span, history.data?.to ?? now];
  // Not again every second with `now`: a heatmap built again each time flickers.
  const families = useMemo(
    () =>
      DISTRIBUTIONS.map((f) => {
        const r = history.data?.byMetric[f.metric];
        const runs = (r?.series ?? []).filter((s) => s.incarnation === incarnation).map((s) => gained(r!, s));
        return { ...f, window: windowHistogram(runs.flat(), from, to), heat: heat(runs, from, to) };
      }),
    [history.data, incarnation, from, to],
  );
  return (
    <>
      {!history.available && <p className="shrink-0 text-[10px] text-warning">History unavailable · distributions over a window need retained histograms</p>}
      <div className="scroll-y grid flex-1 auto-rows-[minmax(260px,1fr)] grid-cols-2 tiles max-[900px]:grid-cols-1">
        {families.map((f) => (
          <PlotFrame key={f.metric} title={f.title} info="The samples that the node recorded in the selected window, from complete observed intervals. Resets and missing intervals add nothing.">
            <HistogramPlot h={f.window} label={`${f.title} distribution over the selected window`} empty="No timing samples" />
          </PlotFrame>
        ))}
        {families.map((f) => (
          <PlotFrame key={f.metric} title={`${f.title} over time`} info="The sample rate for each bucket and time column. A cell without observations is blank, not zero.">
            <HeatPlot grid={f.heat} label={`${f.title} heatmap over the selected window`} />
          </PlotFrame>
        ))}
      </div>
    </>
  );
}
