import type { ReactNode } from "react";

export function Tabs<T extends string>({ label, value, tabs, onChange, tall, trailing }: { label: string; value: T; tabs: { value: T; label: string; count?: number }[]; onChange: (value: T) => void; tall?: boolean; trailing?: ReactNode }) {
  return (
    <div className={`flex shrink-0 items-center border-b border-border px-3 ${tall ? "h-[42px] gap-5 overflow-x-auto" : "h-[34px] gap-[18px]"}`}>
      <div role="tablist" aria-label={label} className="flex h-full items-stretch gap-[inherit]">
        {tabs.map((tab) => (
          <button
            key={tab.value}
            type="button"
            role="tab"
            aria-selected={tab.value === value}
            onClick={() => onChange(tab.value)}
            className={`-mb-px border-b px-0.5 text-[11px] whitespace-nowrap capitalize ${tab.value === value ? "border-accent text-text" : "border-transparent text-muted hover:text-text"}`}
          >
            {tab.label}
            {tab.count != null && <span className="mono ml-1.5 text-[10px] text-muted">{tab.count}</span>}
          </button>
        ))}
      </div>
      {trailing && <div className="ml-auto flex items-center gap-2">{trailing}</div>}
    </div>
  );
}
