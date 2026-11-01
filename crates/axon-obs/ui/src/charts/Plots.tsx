import { useCallback, type ReactNode } from "react";
import type { Histogram } from "../api";
import { Empty } from "../components/atoms";
import { Info } from "../components/Info";
import { count, sum, type Unit } from "../model/format";
import { boundText, mean, quantile, type Heat } from "../model/histogram";
import { Chart } from "./Chart";
import { heatOption, histogramOption, lineOption, type Line } from "./options";

interface Frame {
  title: string;
  info?: ReactNode;
  value?: string;
  footer?: ReactNode;
  children: ReactNode;
}

export function PlotFrame({ title, info, value, footer, children }: Frame) {
  return (
    <section className="flex min-h-[220px] min-w-0 flex-col overflow-hidden rounded-md border border-border bg-panel">
      <header className="flex h-[34px] shrink-0 items-center gap-2 border-b border-border px-3">
        <h2 className="truncate">{title}</h2>
        {info && <Info label={`About ${title}`}>{info}</Info>}
        {value && <span className="mono ml-auto text-[14px]">{value}</span>}
      </header>
      <div className="flex min-h-0 flex-1 flex-col">{children}</div>
      {footer && <footer className="flex shrink-0 gap-4 border-t border-border px-3 py-1.5 text-[10px] text-muted">{footer}</footer>}
    </section>
  );
}

/** Every known sample is zero: a chart of that is a line on the axis, so callers can say it in words. */
export const flatZero = (lines: Line[]) => lines.some((l) => l.points.some((p) => p[1] !== null)) && lines.every((l) => l.points.every((p) => !p[1]));

export function LinePlot({ lines, unit, domain, label, empty }: { lines: Line[]; unit: Unit; domain: [number, number]; label: string; empty?: { title: string; detail?: string } }) {
  const build = useCallback((theme: "dark" | "light") => lineOption(lines, unit, domain, theme), [lines, unit, domain]);
  if (!lines.some((l) => l.points.filter((p) => p[1] !== null).length > 1)) return <Empty title={empty?.title ?? "No samples yet"} detail={empty?.detail ?? "A line needs two observations."} />;
  if (flatZero(lines)) return <Empty title="Flat at zero" detail="Every sample in this range is 0." />;
  return <Chart build={build} label={label} />;
}

export function Quantiles({ h }: { h: Histogram }) {
  const cells: [string, number | null][] = [["Mean", mean(h)], ["p50 ≤", quantile(h, 0.5)], ["p95 ≤", quantile(h, 0.95)], ["p99 ≤", quantile(h, 0.99)]];
  return (
    <dl className="grid shrink-0 grid-cols-4 border-b border-border">
      {cells.map(([name, value]) => (
        <div key={name} className="border-r border-border px-3 py-1.5 last:border-r-0">
          <dt className="text-[10px] text-muted">{name}</dt>
          <dd className="mono text-[11px]">{Number.isNaN(value) ? "Unavailable" : boundText(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

export function HistogramPlot({ h, label, empty }: { h: Histogram | null; label: string; empty: string }) {
  const build = useCallback((theme: "dark" | "light") => histogramOption(h!, theme), [h]);
  if (!h || !h.buckets.some((n) => n > 0)) return <Empty title={empty} />;
  return (
    <>
      <p className="shrink-0 px-3 pt-1.5 text-[10px] text-muted">{count(sum(h.buckets))} samples</p>
      <Quantiles h={h} />
      <div className="min-h-0 flex-1">
        <Chart build={build} label={label} />
      </div>
    </>
  );
}

export function HeatPlot({ grid, label, empty }: { grid: Heat; label: string; empty?: { title: string; detail?: string } }) {
  const build = useCallback((theme: "dark" | "light") => heatOption(grid, theme), [grid]);
  if (!grid.cells.length) return <Empty title={empty?.title ?? "No timing samples"} detail={empty?.detail ?? "Cells show when two observations are in the window."} />;
  return <Chart build={build} label={label} />;
}
