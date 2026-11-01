import type { ReactNode } from "react";
import { useNow, type Tone } from "../hooks";
import { ago, clock, uptime } from "../model/format";

export const TONES: Record<Tone, string> = { success: "text-success", warning: "text-warning", danger: "text-danger", muted: "text-muted" };

export function Dot({ tone = "muted", title }: { tone?: Tone; title?: string }) {
  return <span title={title} className={`mr-1.5 inline-block size-[5px] shrink-0 rounded-full bg-current align-middle ${TONES[tone]}`} />;
}

export function Badge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span className={`inline-flex items-center font-sans text-[10px] whitespace-nowrap ${TONES[tone]}`}>
      <span className="mr-1.5 size-1 rounded-full bg-current" />
      {children}
    </span>
  );
}

export function Empty({ title, detail, children }: { title: string; detail?: string; children?: ReactNode }) {
  return (
    <div className="flex min-h-[100px] flex-1 flex-col items-center justify-center gap-1 p-4 text-center">
      <span className="text-[20px] leading-none text-muted" aria-hidden="true">
        ◇
      </span>
      <p className="text-[11px]">{title}</p>
      {detail && <p className="max-w-[360px] text-[10px] leading-[1.6] text-muted">{detail}</p>}
      {children && <p className="mt-1 text-[11px]">{children}</p>}
    </div>
  );
}

export function Count({ children }: { children: ReactNode }) {
  return <span className="mono rounded-sm bg-raised px-[5px] py-px text-[10px] text-muted">{children}</span>;
}

export function Meter({ value, max, label, thin = false }: { value: number; max: number; label: string; thin?: boolean }) {
  const percent = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  return (
    <div role="meter" aria-label={label} aria-valuemin={0} aria-valuemax={max} aria-valuenow={value} aria-valuetext={label} className={`w-full overflow-hidden rounded-full bg-border ${thin ? "h-[3px]" : "h-1"}`}>
      <div className="h-full bg-accent" style={{ width: `${percent}%` }} />
    </div>
  );
}

/** Time since `us`, a node's `unix_micros`, on the shared second-hand; the title gives the moment itself. */
// ponytail: the browser's clock against the node's, clamped at 0, so skew shows as error; the snapshot's timestamp_ms could correct it if it matters.
export function Since({ us, label }: { us: number; label: string }) {
  return <span title={`${label} ${new Date(us / 1000).toLocaleString()}`}>{uptime(Math.max(0, useNow() - us / 1000))}</span>;
}

/** A detail page's strip of names and values. */
export function Facts({ items, className }: { items: readonly (readonly [string, ReactNode])[]; className: string }) {
  return (
    <dl className={`flex flex-wrap gap-x-6 gap-y-1 px-3 py-2 text-[10px] ${className}`}>
      {items.map(([name, value]) => (
        <div key={name} className="flex items-baseline gap-2"><dt className="text-muted">{name}</dt><dd className="mono">{value}</dd></div>
      ))}
    </dl>
  );
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 items-baseline justify-between gap-3 py-[3px]">
      <dt className="shrink-0 text-[10px] text-muted">{label}</dt>
      <dd className="mono min-w-0 truncate text-right text-[10px]">{children}</dd>
    </div>
  );
}

/** A table's time and its age below it, whole in a `STAMP` wide column. */
export const STAMP = "128px";
export function Stamp({ ms, span }: { ms: number; span: number }) {
  const now = useNow();
  return (
    <>
      <time className="block leading-tight" dateTime={new Date(ms).toISOString()} title={new Date(ms).toLocaleString()}>{clock(ms, span, true)}</time>
      <span className="block font-sans text-[10px] leading-tight text-muted">{ago(now - ms)}</span>
    </>
  );
}
