import { useRef } from "react";
import { Icon } from "./Icon";
import { useTip } from "./tip";

export interface Toggle<T extends string> {
  value: T;
  label: string;
  icon: string;
}


const FRAME = "inline-flex h-[28px] items-center gap-0.5 rounded-md border border-border bg-bg px-[3px]";
const CAPTION = "px-1 text-[10px] text-muted max-[1500px]:hidden";
const face = (on: boolean) =>
  `flex h-[24px] min-w-[24px] items-center justify-center rounded-sm focus-visible:outline-1 ${on ? "bg-[color-mix(in_srgb,var(--accent)_12%,transparent)] text-accent" : "text-muted hover:bg-raised hover:text-text"}`;

/** One of several. `caption` names the group in the toolbar when there is room for it. */
export function ToggleGroup<T extends string>({ label, caption, value, options, onChange }: { label: string; caption?: string; value: T; options: Toggle<T>[]; onChange: (value: T) => void }) {
  const group = useRef<HTMLDivElement>(null);
  const tip = useTip();
  const onKey = (e: React.KeyboardEvent) => {
    const at = options.findIndex((o) => o.value === value);
    const next = { ArrowLeft: at - 1, ArrowRight: at + 1, Home: 0, End: options.length - 1 }[e.key];
    if (next === undefined) return;
    e.preventDefault();
    const index = Math.max(0, Math.min(options.length - 1, next));
    onChange(options[index].value);
    group.current?.querySelectorAll("button")[index]?.focus();
  };
  return (
    <div ref={group} role="group" aria-label={label} onKeyDown={onKey} className={FRAME}>
      {caption && <span aria-hidden="true" className={CAPTION}>{caption}</span>}
      {options.map((option) => (
        <button key={option.value} type="button" {...tip.bind(`${label} · ${option.label}`)} aria-label={option.label} aria-pressed={option.value === value} onClick={() => onChange(option.value)} className={face(option.value === value)}>
          <Icon name={option.icon} size={14} />
        </button>
      ))}
      {tip.node}
    </div>
  );
}

/** On or off. */
export function IconToggle({ label, icon, pressed, onChange }: { label: string; icon: string; pressed: boolean; onChange: (pressed: boolean) => void }) {
  const tip = useTip();
  return (
    <span className={FRAME}>
      <button type="button" {...tip.bind(`${label} · ${pressed ? "on" : "off"}`)} aria-label={label} aria-pressed={pressed} onClick={() => onChange(!pressed)} className={face(pressed)}>
        <Icon name={icon} size={14} />
      </button>
      {tip.node}
    </span>
  );
}
