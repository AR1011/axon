import { useCallback, useRef, useState } from "react";
import { Link } from "react-router";
import { createPortal } from "react-dom";
import type { HistorySource as Source } from "../api";
import { Badge, TONES } from "../components/atoms";
import { collected, collectorProblem } from "../model/attention";
import { Icon } from "../components/Icon";
import { useEscape, useNow, useStaleSince, useUrlState, type Tone } from "../hooks";
import { ago } from "../model/format";
import { useLive } from "../store";

export function StorageLogo({ storage, size = 14 }: { storage?: string; size?: number }) {
  if (storage === "clickhouse")
    return (
      <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" className="shrink-0">
        <path fill="#fc0" d="M2 3h2.4v18H2zM6.8 3h2.4v18H6.8zM11.6 3H14v18h-2.4zM16.4 3h2.4v18h-2.4zM21.2 9.8h2.4v4.4h-2.4z" />
      </svg>
    );
  return <Icon name="memory" size={size} className="text-muted" />;
}

const NAMES: Record<string, string> = { clickhouse: "ClickHouse", memory: "In-memory ring" };
export const storeName = (storage: string) => NAMES[storage] ?? storage;

function Row({ source, active, checked, onPick }: { source?: Source; active: boolean; checked: boolean; onPick?: () => void }) {
  const problem = collectorProblem(source?.collector ?? null);
  return (
    <button
      type="button"
      role="radio"
      aria-checked={checked}
      disabled={!onPick}
      onClick={onPick}
      className={`flex w-full items-start gap-2.5 rounded-md border px-2.5 py-2 text-left disabled:opacity-100 ${checked ? "border-accent bg-accent-soft" : "border-transparent hover:bg-panel"}`}
    >
      <span className="mt-0.5">{source ? <StorageLogo storage={source.storage} size={18} /> : <Icon name="refresh" size={18} />}</span>
      <span className="min-w-0 flex-1">
        <span className="flex items-center justify-between gap-2">
          <span className="truncate text-[11px] font-[550]">{source ? storeName(source.storage) : "Automatic"}</span>
          {source?.error ? <Badge tone="warning">Unavailable</Badge> : active ? <Badge tone="success">In use</Badge> : source ? <Badge tone="muted">Standby</Badge> : null}
        </span>
        {source ? (
          <>
            <span className="mono block truncate text-[10px] text-muted">
              {source.node} · {collected(source.collector)}
            </span>
            {problem && <span className="block text-[10px] text-warning">{problem}</span>}
            {source.error && <span className="block text-[10px] leading-[1.5] text-warning">{source.error}</span>}
          </>
        ) : (
          <span className="block text-[10px] text-muted">The most recent healthy source, with failover.</span>
        )}
      </span>
    </button>
  );
}

/** How far back retained history reaches: the oldest row the store in use holds. */
function horizon(using: Source | undefined): number | null {
  const firsts = Object.values(using?.collector?.sink_usage ?? {}).flatMap((u) => u.tables.flatMap((t) => (t.first_ms == null ? [] : [t.first_ms])));
  return firsts.length ? Math.min(...firsts) : null;
}

/**
 * The console's state in one chip: a dot, and how long ago the last overview came. It opens on the connection, how far
 * back history reaches and which metrics store answers it: automatic by default, the operator's choice otherwise.
 */
export function Status() {
  const now = useNow();
  const { mode, overview, receivedAt, error } = useLive();
  const stale = useStaleSince();
  const history = overview?.history;
  const [picked, setPicked] = useUrlState("history", "");
  const [at, setAt] = useState<{ right: number; top: number } | null>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => {
    setAt(null);
    trigger.current?.focus();
  }, []);
  useEscape(!!at, close);
  const focusOnOpen = useCallback((e: HTMLDivElement | null) => e?.focus(), []);
  let [label, tone, how]: [string, Tone, string] =
    mode === "stream" ? ["Connected", "success", "Live stream"] : mode === "polling" ? ["Polling", "success", "Polling: the live stream is unavailable"] : ["Connecting", "muted", "Connecting"];
  if (stale != null) [label, tone, how] = ["Stale view", "warning", error ? `${error} · the console tries again` : "No new overview · the console tries again"];
  else if (!overview) [label, tone, how] = ["Connecting", "muted", error ?? "The console server did not send its first overview yet"];
  const age = receivedAt ? ago(now - receivedAt).replace(" ago", "") : null;
  // No store answers: the charts over time and retained events are missing, which the chip says without being opened.
  const liveOnly = !!history && !history.available;
  const choice = history?.pinned ? "" : picked;
  const using = history?.sources.find((s) => s.node === (choice || history.node));
  const first = horizon(using);
  const fixed = !history || history.pinned || history.sources.length <= 1;
  return (
    <>
      <button
        ref={trigger}
        type="button"
        aria-haspopup="dialog"
        aria-expanded={!!at}
        aria-controls={at ? "console-status" : undefined}
        aria-label={`${age ? `${label}, updated ${age} ago` : label}${liveOnly ? ", live only" : ""}`}
        title={how}
        onClick={() => {
          if (at) return close();
          const box = trigger.current!.getBoundingClientRect();
          setAt({ right: Math.max(8, window.innerWidth - box.right), top: box.bottom + 4 });
        }}
        className={`flex h-[24px] shrink-0 items-center gap-1.5 rounded-full border border-border px-2 text-[10px] hover:bg-raised ${tone === "warning" ? "text-warning" : "text-muted"}`}
      >
        <span data-dot aria-hidden="true" className={`size-1.5 rounded-full bg-current ${TONES[tone]}`} />
        {label !== "Connected" && <span aria-hidden="true">{label}</span>}
        {age && <span aria-hidden="true" className="mono">{age}</span>}
        {liveOnly && <span aria-hidden="true" className="text-warning">live only</span>}
      </button>
      {at &&
        createPortal(
          <>
            <div className="fixed inset-0 z-40" onPointerDown={close} />
            <div
              id="console-status"
              role="dialog"
              aria-label="Console status"
              tabIndex={-1}
              ref={focusOnOpen}
              style={at}
              className="fixed z-50 w-[340px] max-w-[calc(100vw-16px)] rounded-lg border border-border bg-raised p-2 font-sans shadow-float outline-none"
            >
              <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 px-1 pb-2 text-[10px]">
                <dt className="text-muted">Connection</dt>
                <dd className={tone === "warning" ? "text-warning" : ""}>{how}</dd>
                <dt className="text-muted">Updated</dt>
                <dd className="mono">{receivedAt ? ago(now - receivedAt) : "Not yet"}</dd>
                <dt className="text-muted">History from</dt>
                <dd className="mono" title={first == null ? undefined : `The metrics store keeps history from ${new Date(first).toLocaleString()}`}>
                  {first == null ? "—" : `${ago(now - first)} · ${new Date(first).toLocaleString()}`}
                </dd>
              </dl>
              {history && (
                <div className="border-t border-border pt-2">
                  {(history.pinned || !history.sources.length) && (
                    <p className="px-1 pb-1.5 text-[10px] leading-[1.5] text-muted">
                      {history.pinned
                        ? "The configuration of this dashboard pins it to one history node."
                        : "No node announces a metrics store. Live telemetry works, but charts over time and retained events do not."}
                    </p>
                  )}
                  <div role="radiogroup" aria-label="History source" className="flex max-h-[50vh] flex-col gap-1 overflow-y-auto">
                    {!fixed && <Row active={false} checked={!choice} onPick={() => (setPicked(null), close())} />}
                    {history.sources.map((s) => (
                      <Row key={s.node} source={s} active={s.node === using?.node} checked={fixed ? s.node === using?.node : s.node === choice} onPick={fixed ? undefined : () => (setPicked(s.node), close())} />
                    ))}
                  </div>
                  {history.error && <p className="px-1 pt-1.5 text-[10px] leading-[1.5] text-warning">{history.error}</p>}
                </div>
              )}
              <Link to="/settings#stores" onClick={close} className="mt-1.5 block border-t border-border px-1 pt-1.5 text-[10px] text-muted hover:text-text">Capabilities and collector counters in Settings →</Link>
            </div>
          </>,
          document.body,
        )}
    </>
  );
}
