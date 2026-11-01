import { useEffect, useState } from "react";
import { useLocation } from "react-router";
import { ACCENTS, accentChoice, DEFAULT_ACCENT, setAccent, setTheme, themeChoice, type ThemeChoice } from "../../appearance";
import { Badge, Meter } from "../../components/atoms";
import { Icon } from "../../components/Icon";
import type { SinkUsage, TableUsage } from "../../api";
import { bytes, clock, count, UNKNOWN, uptime } from "../../model/format";
import { collected, collectorProblem } from "../../model/attention";
import { StorageLogo, storeName } from "../../shell/Status";
import { useLive } from "../../store";

const THEMES: { value: ThemeChoice; label: string; detail: string; faces: [string, string][] }[] = [
  { value: "system", label: "System", detail: "Follows the operating system", faces: [["#111213", "#202224"], ["#fafafa", "#e1e2e5"]] },
  { value: "dark", label: "Dark", detail: "Always dark", faces: [["#111213", "#202224"]] },
  { value: "light", label: "Light", detail: "Always light", faces: [["#fafafa", "#e1e2e5"]] },
];

function Section({ title, detail, action, children }: { title: string; detail: string; action?: React.ReactNode; children: React.ReactNode }) {
  return (
    <section className="rounded-md border border-border bg-panel">
      <header className="flex items-start gap-3 border-b border-border px-4 py-3">
        <div className="min-w-0 flex-1">
          <h2 className="text-[12px]">{title}</h2>
          <p className="mt-0.5 text-[11px] leading-[1.5] text-muted">{detail}</p>
        </div>
        {action}
      </header>
      <div className="p-4">{children}</div>
    </section>
  );
}

/** A thumbnail of the shell in one palette: sidebar, a panel, a line in the accent. */
function Face({ bg, line }: { bg: string; line: string }) {
  return (
    <span className="flex h-full flex-1 gap-1 p-1.5" style={{ background: bg }}>
      <span className="w-1/4 rounded-[2px]" style={{ background: line }} />
      <span className="flex flex-1 flex-col gap-1">
        <span className="h-1.5 w-2/3 rounded-full" style={{ background: line }} />
        <span className="flex-1 rounded-[2px] border" style={{ borderColor: line }} />
        <span className="h-1 w-1/2 rounded-full bg-accent" />
      </span>
    </span>
  );
}

/** What a sink holds and when it fills at the rate it grows, as facts for the store's card. */
function usageFacts(sink: string, u: SinkUsage, many: boolean): [string, string][] {
  const full = u.max_bytes && u.evicted_batches ? "now: the store removes the oldest" : u.full_in_ms != null ? `in ${uptime(u.full_in_ms)}` : u.growth_bytes_per_day != null ? "not at this rate" : UNKNOWN;
  const facts: [string, string][] = [
    ["held", u.max_bytes ? `${bytes(u.held_bytes)} of ${bytes(u.max_bytes)}` : bytes(u.held_bytes)],
    ["free", bytes(u.free_bytes)],
    ["growth per day", bytes(u.growth_bytes_per_day)],
    ["full", full],
  ];
  if (u.max_bytes) facts.push(["evicted batches", count(u.evicted_batches)]);
  return many ? facts.map(([k, v]) => [`${sink} ${k}`, v]) : facts;
}

function Tables({ sink, u }: { sink: string; u: SinkUsage }) {
  const span = ({ first_ms: a, last_ms: b }: TableUsage) => (a != null && b != null ? `${clock(a, b - a)} → ${clock(b, b - a)}` : UNKNOWN);
  return (
    <div role="group" aria-label={`Usage of ${sink}`} className="overflow-x-auto border-t border-border px-3 py-2">
      <table className="min-w-max table-auto text-[11px]">
        <thead>
          <tr className="text-left text-[10px] text-muted">{["table", "rows", "held", "uncompressed", "ratio", "parts", "first → last point", "kept"].map((h) => <th key={h} className="h-7 px-3 font-normal">{h}</th>)}</tr>
        </thead>
        <tbody>
          {u.tables.map((t) => (
            <tr key={t.name}>
              {[t.name, count(t.rows), bytes(t.held_bytes), bytes(t.uncompressed_bytes), t.held_bytes ? `${(t.uncompressed_bytes / t.held_bytes).toFixed(1)}×` : UNKNOWN, t.parts ? count(t.parts) : UNKNOWN, span(t), t.retention_days != null ? `${t.retention_days} d` : u.max_bytes ? "by size" : UNKNOWN].map((v, i) => <td key={i}>{v}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Stores() {
  const history = useLive((s) => s.overview?.history);
  if (!history?.sources.length) return <p className="text-[11px] text-muted">No node announces a metrics store. Live telemetry works, but charts over time and retained events do not.</p>;
  return (
    <div className="flex flex-col gap-3">
      {history.sources.map((s) => {
        const c = s.collector;
        const caps = s.capabilities;
        const problem = collectorProblem(c);
        const facts: [string, string][] = c
          ? [["cycles", count(c.cycles)], ["last cycle", `${c.last_cycle_duration_ms} ms`], ["stored batches", count(c.stored_batches)], ["stored spans", count(c.stored_spans)], ["rejected batches", count(c.rejected_batches)], ["collection gaps", count(c.collection_gaps)], ["source resets", count(c.source_resets)], ["write errors", count(c.write_errors)], ["dropped events", count(c.dropped_events)], ["pending events", count(c.pending_events)]]
          : [];
        const usage = Object.entries(c?.sink_usage ?? {});
        for (const [sink, u] of usage) facts.push(...usageFacts(sink, u, usage.length > 1));
        return (
          <div key={s.node} className="rounded-md border border-border">
            <div className="flex items-center gap-2 border-b border-border px-3 py-2">
              <StorageLogo storage={s.storage} size={16} />
              <span className="text-[11px] font-[550]">{storeName(s.storage)}</span>
              <span className="mono text-[10px] text-muted">{s.node} · {collected(c)}</span>
              <span className="ml-auto">{s.error ? <Badge tone="warning">Unavailable</Badge> : problem ? <Badge tone="warning">Data loss</Badge> : s.node === history.node ? <Badge tone="success">In use</Badge> : <Badge tone="muted">Standby</Badge>}</span>
            </div>
            <div className="grid grid-cols-[repeat(auto-fill,minmax(120px,1fr))] gap-x-3 gap-y-1.5 px-3 py-2">
              {caps && <div><div className="text-[10px] text-muted">answers</div><div className="text-[11px]">{[caps.numeric_history && "series", caps.histograms && "histograms", caps.events && "events"].filter(Boolean).join(" · ") || "nothing"}</div></div>}
              {caps && <div><div className="text-[10px] text-muted">points per query</div><div className="mono text-[11px]">{count(caps.max_points)}</div></div>}
              {facts.map(([k, v]) => (
                <div key={k}><div className="text-[10px] text-muted">{k}</div><div className="mono text-[11px]">{v}</div></div>
              ))}
            </div>
            {usage.map(([sink, u]) => <Tables key={sink} sink={sink} u={u} />)}
            {(s.error || (c && (Object.keys(c.sink_errors).length > 0 || Object.keys(c.node_errors).length > 0))) && (
              <ul className="border-t border-border px-3 py-2 text-[10px] leading-[1.6] text-warning">
                {s.error && <li>{s.error}</li>}
                {Object.entries(c?.sink_errors ?? {}).map(([k, v]) => <li key={k}><span className="mono">{k}</span> · {v}</li>)}
                {Object.entries(c?.node_errors ?? {}).map(([k, v]) => <li key={k}><span className="mono">{k}</span> · {v}</li>)}
              </ul>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function SettingsPage() {
  const [theme, setThemeState] = useState(themeChoice);
  const [accent, setAccentState] = useState(accentChoice);
  const hash = useLocation().hash.slice(1);
  useEffect(() => document.getElementById(hash)?.scrollIntoView(), [hash]);
  const pickTheme = (choice: ThemeChoice) => (setTheme(choice), setThemeState(choice));
  const pickAccent = (hex: string | null) => (setAccent(hex), setAccentState(accentChoice()));
  const preset = ACCENTS.some((a) => a.value === accent);
  const tile = (on: boolean) => `flex flex-col gap-2 rounded-md border p-2 text-left ${on ? "border-accent bg-accent-soft" : "border-border hover:bg-raised"}`;
  return (
    <div className="scroll-y flex-1">
      <div className="mx-auto flex w-full max-w-[720px] flex-col gap-3 py-2">
        <h1>Settings</h1>
        <Section title="Theme" detail="The palette of the whole console.">
          <div role="radiogroup" aria-label="Theme" className="grid grid-cols-3 gap-3 max-[600px]:grid-cols-1">
            {THEMES.map((t) => (
              <button key={t.value} type="button" role="radio" aria-checked={theme === t.value} aria-label={t.label} onClick={() => pickTheme(t.value)} className={tile(theme === t.value)}>
                <span className="flex h-[72px] w-full overflow-hidden rounded-sm border border-border">{t.faces.map(([bg, line]) => <Face key={bg} bg={bg} line={line} />)}</span>
                <span className="flex w-full items-center gap-2 px-0.5">
                  <span className="min-w-0 flex-1">
                    <span className="block text-[11px] font-[550]">{t.label}</span>
                    <span className="block truncate text-[10px] text-muted">{t.detail}</span>
                  </span>
                  {theme === t.value && <span className="text-accent"><Icon name="check" size={14} /></span>}
                </span>
              </button>
            ))}
          </div>
        </Section>
        <Section
          title="Accent colour"
          detail="Selection, live status, focus rings and the first series of every chart. Warnings and errors keep their own colours."
          action={<button type="button" disabled={accent === DEFAULT_ACCENT} onClick={() => pickAccent(null)} className="h-7 shrink-0 rounded-md border border-border px-2.5 text-[11px] text-muted hover:text-text">Reset</button>}
        >
          <div role="radiogroup" aria-label="Accent colour" className="grid grid-cols-9 gap-2 max-[700px]:grid-cols-3">
            {ACCENTS.map((a) => (
              <button key={a.value} type="button" role="radio" aria-checked={accent === a.value} aria-label={a.name} onClick={() => pickAccent(a.value)} className={`${tile(accent === a.value)} items-center !gap-1.5 !text-center`}>
                <span className="flex size-7 items-center justify-center rounded-full text-[#111]" style={{ background: a.value }}>{accent === a.value && <Icon name="check" size={14} />}</span>
                <span className="text-[10px]">{a.name}</span>
              </button>
            ))}
            <label className={`${tile(!preset)} relative cursor-pointer items-center !gap-1.5 !text-center`}>
              <span className="size-7 rounded-full border border-border" style={{ background: preset ? "conic-gradient(#f08c6c, #e0b252, #65cda0, #6aa9f0, #b58cf0, #f08c6c)" : accent }} />
              <span className={`text-[10px] ${preset ? "" : "mono"}`}>{preset ? "Custom" : accent}</span>
              <input type="color" aria-label="Custom accent colour" value={accent} onChange={(e) => pickAccent(e.target.value)} className="absolute inset-0 size-full cursor-pointer opacity-0" />
            </label>
          </div>
        </Section>
        <div id="stores" />
        <Section title="Metrics stores" detail="Each node that announces a store, what the store answers, and the counters of the collector. A store that loses data also shows on the attention tab of the overview.">
          <Stores />
        </Section>
        <Section title="Preview" detail="How the accent looks with this theme.">
          <div className="flex flex-wrap items-center gap-x-5 gap-y-3">
            <Badge tone="success">Running</Badge>
            <span className="rounded-md border border-accent bg-accent-soft px-2 py-1 text-[11px]">Selected row</span>
            <a href="#" onClick={(e) => e.preventDefault()} className="text-[11px] text-accent hover:underline">A link</a>
            <span className="w-32"><Meter value={62} max={100} label="Preview meter" /></span>
            <button type="button" className="h-7 rounded-md bg-accent px-3 text-[11px] font-[550] text-bg">Button</button>
          </div>
        </Section>
      </div>
    </div>
  );
}
