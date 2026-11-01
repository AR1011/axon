import { useQuery } from "@tanstack/react-query";
import { useMemo, useState, type CSSProperties, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { Link, useParams } from "react-router";
import type { SpanRecord, TraceDetail as Detail } from "../../api";
import { getJson } from "../../api";
import { Badge, Count, Empty, Field } from "../../components/atoms";
import { Identity } from "../../components/Identity";
import { Panel } from "../../components/Panel";
import { useUrlState } from "../../hooks";
import { clock, duration, shortType } from "../../model/format";
import { endedUs, layoutTrace, nodeHue, place, spanTitle } from "../../model/trace";
import { nodePath } from "../select";
import { Flag, NodeName } from "../../components/Flag";

const KIND = { handle: "Handled", call: "Call", stream: "Stream batch" } as const;

/** Nodes keep their colour across the waterfall. */
const nodeColour = (index: number) => `hsl(${nodeHue(index)} 52% 56%)`;

export function TraceDetail() {
  const { traceId = "" } = useParams();
  const [source] = useUrlState("history", "");
  const [selected, setSelected] = useUrlState("span", "");
  const found = useQuery<Detail, Error>({
    queryKey: ["trace", source, traceId],
    // Spans of a trace still running keep arriving; one that has been quiet for a minute is done.
    refetchInterval: ({ state }) => (!state.data || Math.max(0, ...(state.data?.spans ?? []).map(endedUs)) / 1000 > Date.now() - 60_000 ? 5_000 : false),
    retry: false,
    queryFn: ({ signal }) => getJson<Detail>("trace", { id: traceId, history: source }, signal),
  });
  const spans = found.data?.spans;
  const layout = useMemo(() => (spans?.length ? layoutTrace(spans) : null), [spans]);
  const nodes = useMemo(() => [...new Set((spans ?? []).map((s) => s.node_id))].sort(), [spans]);
  if (!layout) {
    return (
      <>
        <h1 className="sr-only-h1">Trace</h1>
        <Empty title={found.isPending ? "Wait for the trace" : "Trace not found"} detail={found.error?.message ?? "No node and no store still has its spans."}>
          <Link to="/traces" className="underline decoration-border underline-offset-2">
            All traces
          </Link>
        </Empty>
      </>
    );
  }
  const total = layout.endUs - layout.startUs;
  const failed = layout.rows.filter((r) => r.span.failed).length;
  const chosen = layout.rows.find((r) => r.span.span === selected)?.span;
  const unreachable = Object.keys(found.data?.errors ?? {});
  return (
    <>
      <h1 className="sr-only-h1">Trace {traceId}</h1>
      <section aria-label="Trace summary" className="flex shrink-0 flex-wrap items-center gap-x-6 gap-y-1 rounded-md border border-border bg-panel px-3 py-2 text-[11px]">
        <Link to="/traces" className="text-muted hover:text-text">
          ← Traces
        </Link>
        <Identity value={traceId} label="Trace id" />
        <dl className="contents">
          <Field label="Started">
            <time dateTime={new Date(layout.startUs / 1000).toISOString()}>{clock(layout.startUs / 1000, Infinity, true)}</time>
          </Field>
          <Field label="Duration">{duration(total * 1000)}</Field>
          <Field label="Spans">{layout.rows.length}</Field>
          <Field label="Nodes">
            <span className="flex flex-wrap items-center gap-x-3">
              {nodes.map((node, i) => (
                <Link key={node} to={nodePath(node)} className="flex items-center gap-1.5 hover:underline">
                  <span className="size-2 rounded-sm" style={{ background: nodeColour(i) }} />
                  <Flag node={node} />
                  {node}
                </Link>
              ))}
            </span>
          </Field>
        </dl>
        {failed > 0 && <Badge tone="danger">{failed} failed</Badge>}
      </section>
      <Panel
        className="flex-1"
        title={
          <>
            Spans <Count>{layout.rows.length}</Count>
          </>
        }
        footer={unreachable.length ? <span>No answer from {unreachable.join(", ")}</span> : undefined}
      >
        <div className="scroll-y flex-1 overflow-x-auto">
          <div className="min-w-[640px] sticky top-0 z-10 grid grid-cols-[minmax(220px,38%)_1fr_70px] items-end gap-3 border-b border-border bg-panel px-3 pt-1 pb-2 text-[11px]">
            <span>Whole trace</span>
            <span>
              <span className="mono relative block h-4 text-[10px] text-muted">
                {[0, 0.25, 0.5, 0.75, 1].map((f) => (
                  <span key={f} className="absolute" style={f === 1 ? { right: 0 } : { left: `${f * 100}%`, transform: f ? "translateX(-50%)" : undefined }}>
                    {f ? duration(total * f * 1000) : "0"}
                  </span>
                ))}
              </span>
              <span className="block h-2.5 rounded-sm bg-muted/50" />
            </span>
            <span className="mono text-right">{duration(total * 1000)}</span>
          </div>
          <ol aria-label="Spans" className="min-w-[640px]">
            {layout.rows.map(({ span, depth, orphan, gapUs, cause, guides }, i) => {
              const at = place(span, layout, gapUs);
              const before = layout.rows[i - 1]?.depth ?? depth;
              // A breath before each branch: where the tree steps back out, or a sibling starts a subtree of its own.
              const branch = i > 0 && (depth < before || (depth === before && (layout.rows[i + 1]?.depth ?? 0) > depth) || (depth === 1 && before === 0));
              const colour = nodeColour(nodes.indexOf(span.node_id));
              const open = span.span === selected;
              return (
                <li key={span.span}>
                  {branch && (
                    <span aria-hidden className="flex h-3 gap-1.5 px-3">
                      <Guides guides={guides} spacer />
                    </span>
                  )}
                  <button
                    type="button"
                    aria-expanded={open}
                    onClick={() => setSelected(open ? null : span.span)}
                    className={`grid h-7 w-full grid-cols-[minmax(220px,38%)_1fr_70px] items-center gap-3 px-3 text-left text-[11px] hover:bg-raised ${open ? "bg-raised" : ""}`}
                  >
                    <span className="flex min-w-0 items-center gap-1.5 self-stretch">
                      <Guides guides={guides} />
                      <span className="size-2 shrink-0 rounded-sm" style={{ background: colour }} />
                      <Flag node={span.node_id} />
                      <span className="shrink-0">{span.node_id}</span>
                      <span className={`truncate ${span.failed ? "text-danger" : "text-muted"}`}>{spanTitle(span)}</span>
                      {orphan && (
                        <span className="shrink-0 text-[10px] text-muted" title="No node recorded the span that caused this one, or no node keeps it now">
                          detached
                        </span>
                      )}
                    </span>
                    <span className="relative h-2.5 rounded-sm bg-bg">
                      {gapUs > 0 && (
                        <Segment left={at.gap} width={at.wait - at.gap} bar="top-1/2 border-t border-dashed border-muted" tip={<GapTip span={span} cause={cause} gapUs={gapUs} totalUs={total} />} />
                      )}
                      {span.wait_ns > 0 && (
                        <Segment left={at.wait} width={at.left - at.wait} bar="inset-y-2 rounded-sm opacity-35" style={{ background: colour }} tip={<WaitTip span={span} totalUs={total} />} />
                      )}
                      <Segment
                        left={at.left}
                        width={at.width}
                        bar="inset-y-2 min-w-[2px] rounded-sm"
                        style={{ background: span.failed ? "var(--danger)" : colour }}
                        tip={<BusyTip span={span} totalUs={total} offsetUs={span.start_us - layout.startUs} />}
                      />
                    </span>
                    <span className="mono text-right text-muted">{duration(span.busy_ns)}</span>
                  </button>
                  {open && chosen && <SpanFacts span={chosen} gapUs={gapUs} offsetUs={chosen.start_us - layout.startUs} />}
                </li>
              );
            })}
          </ol>
        </div>
      </Panel>
    </>
  );
}

/** One part of a span's bar. The hover target is taller and wider than the mark, which can be a hairline. */
function Segment({ left, width, bar, style, tip }: { left: number; width: number; bar: string; style?: CSSProperties; tip: ReactNode }) {
  const [at, setAt] = useState<CSSProperties | null>(null);
  return (
    <span
      className="absolute -inset-y-2 min-w-[6px]"
      style={{ left: `${left * 100}%`, width: `${width * 100}%` }}
      onMouseEnter={(e) => {
        const box = e.currentTarget.getBoundingClientRect();
        const x = Math.max(8, Math.min(window.innerWidth - 248, e.clientX - 120));
        setAt(box.top < 170 ? { left: x, top: box.bottom + 4 } : { left: x, bottom: window.innerHeight - box.top + 4 });
      }}
      onMouseLeave={() => setAt(null)}
    >
      <span className={`absolute inset-x-0 ${bar}`} style={style} />
      {at &&
        createPortal(
          <div role="tooltip" style={at} className="pointer-events-none fixed z-50 w-[240px] rounded-lg border border-border bg-panel px-3 py-2 font-sans text-[11px] leading-[1.6] text-text shadow-tip">
            {tip}
          </div>,
          document.body,
        )}
    </span>
  );
}

function Tip({ title, ns, totalUs, children }: { title: string; ns: number; totalUs: number; children: ReactNode }) {
  return (
    <>
      <span className="flex items-baseline gap-2">
        <span className="font-medium">{title}</span>
        <span className="mono ml-auto">{duration(ns)}</span>
        <span className="mono text-muted">{Math.round(ns / 10 / Math.max(1, totalUs)) || "<1"}%</span>
      </span>
      <span className="block text-muted">{children}</span>
    </>
  );
}

function GapTip({ span, cause, gapUs, totalUs }: { span: SpanRecord; cause?: SpanRecord; gapUs: number; totalUs: number }) {
  const wire = cause && cause.node_id !== span.node_id;
  return (
    <Tip title={wire ? "On the wire" : "Between spans"} ns={gapUs * 1000} totalUs={totalUs}>
      {wire
        ? `${cause.node_id} → ${span.node_id}, plus any clock skew.`
        : "Time before a timer or a job."}
    </Tip>
  );
}

function WaitTip({ span, totalUs }: { span: SpanRecord; totalUs: number }) {
  return (
    <Tip title="In mailbox" ns={span.wait_ns} totalUs={totalUs}>
      Queued for {span.actor_name || shortType(span.actor_type) || "the actor"}.
    </Tip>
  );
}

function BusyTip({ span, totalUs, offsetUs }: { span: SpanRecord; totalUs: number; offsetUs: number }) {
  const what =
    span.kind === "call" ? `${span.node_id} → ${span.peer}` : span.kind === "stream" ? (span.peer ? `${span.peer} → ${span.node_id}` : span.name) : `${span.name || "message"} · ${span.actor_name || shortType(span.actor_type)}`;
  return (
    <Tip title={span.kind === "call" ? (span.name === "request" ? "Until answered" : "Send") : span.kind === "stream" ? (span.peer ? "Batch taken" : "Publish") : "Handler time"} ns={span.busy_ns} totalUs={totalUs}>
      {what}
      {span.failed ? " · failed" : ""} · at +{duration(offsetUs * 1000)}
    </Tip>
  );
}

/** The tree's lines beside a row, or through the gap above it. */
function Guides({ guides, spacer = false }: { guides: boolean[]; spacer?: boolean }) {
  return guides.slice(-12).map((runsOn, i, shown) => {
    const own = i === shown.length - 1;
    return (
      <span key={i} aria-hidden className="relative w-5 shrink-0 self-stretch">
        {(runsOn || own) && <span className={`absolute top-0 left-1/2 border-l border-muted/60 ${runsOn || spacer ? "bottom-0" : "h-1/2"}`} />}
        {own && !spacer && <span className="absolute top-1/2 left-1/2 w-4 border-t border-muted/60" />}
      </span>
    );
  });
}

function SpanFacts({ span, gapUs, offsetUs }: { span: SpanRecord; gapUs: number; offsetUs: number }) {
  return (
    <dl className="flex flex-wrap gap-x-6 gap-y-1 bg-raised px-3 pt-1 pb-2 pl-[30px] text-[11px]">
      <Field label="Kind">{KIND[span.kind] ?? span.kind}</Field>
      <Field label={span.kind === "stream" ? "Stream" : span.kind === "call" ? "Call" : "Message"}>{span.name || "—"}</Field>
      <Field label="Node">
        <NodeName id={span.node_id} className="underline decoration-border underline-offset-2" />
      </Field>
      {span.actor && (
        <Field label="Actor">
          <Link to={`/actors/${encodeURIComponent(span.node_id)}/${encodeURIComponent(span.actor)}`} className="underline decoration-border underline-offset-2">
            {span.actor_name || shortType(span.actor_type)} {span.actor}
          </Link>
        </Field>
      )}
      {span.peer && (
        <Field label={span.kind === "call" ? "To" : "From"}>
          <NodeName id={span.peer} className="underline decoration-border underline-offset-2" />
        </Field>
      )}
      {gapUs > 0 && <Field label="Between spans">{duration(gapUs * 1000)}</Field>}
      <Field label="Starts at">+{duration(offsetUs * 1000)}</Field>
      {span.kind === "handle" && <Field label="In mailbox">{duration(span.wait_ns)}</Field>}
      <Field label={span.kind === "call" ? "Until answered" : "Took"}>{duration(span.busy_ns)}</Field>
      <Field label="Ends at">+{duration((endedUs(span) - span.start_us + offsetUs) * 1000)}</Field>
      <Field label="Outcome">{span.failed ? (span.kind === "call" ? "The call failed" : "The handler failed and the actor stopped") : "Completed"}</Field>
      <Field label="Span">{span.span}</Field>
    </dl>
  );
}
