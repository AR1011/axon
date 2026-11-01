import { useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useEscape } from "../hooks";

/** An incarnation is `unix millis << 64 | random`: its top digits are always zero, so the chip shows the clock's low half and the random tail. */
const brief = (hex: string) => `${hex.slice(8, 12)}·${hex.slice(12, 16)}·${hex.slice(-4)}`;

/** A 16-39 digit decimal is a big integer: shown as 32 hex digits without ever passing through a JS number. */
function displayId(value: string): { short: string; full: string; hex: boolean } {
  if (/^\d{16,39}$/.test(value)) {
    const hex = BigInt(value).toString(16).padStart(32, "0");
    return { short: brief(hex), full: hex.match(/.{8}/g)!.join("–"), hex: true };
  }
  if (/^[0-9a-f]{32}$/.test(value)) return { short: brief(value), full: value.match(/.{8}/g)!.join("–"), hex: false };
  return { short: value.length > 24 ? `${value.slice(0, 9)}·${value.slice(-9)}` : value, full: value, hex: false };
}

function hue(value: string): number {
  let hash = 0;
  for (let i = 0; i < value.length; i++) hash = (hash * 31 + value.charCodeAt(i)) >>> 0;
  return hash % 360;
}

export function Identity({ value, label = "Identifier" }: { value: string; label?: string }) {
  const [at, setAt] = useState<{ left: number; top: number } | null>(null);
  const [copied, setCopied] = useState(false);
  const chip = useRef<HTMLButtonElement>(null);
  const shown = displayId(value);
  const close = () => {
    setAt(null);
    setCopied(false);
    chip.current?.focus();
  };
  useEscape(!!at, close);
  return (
    <>
      <button
        ref={(el) => {
          chip.current = el;
          // Inside a table the row is the tab stop; the chip stays clickable.
          if (el) el.tabIndex = el.closest("tbody") ? -1 : 0;
        }}
        type="button"
        aria-label={`${label} ${shown.short}`}
        onClick={(e) => {
          e.stopPropagation();
          const box = chip.current!.getBoundingClientRect();
          setAt({ left: Math.min(window.innerWidth - 330, box.left), top: box.bottom + 4 });
        }}
        className="mono inline-flex items-center gap-1 rounded-sm align-baseline text-[10px] text-muted hover:text-text"
      >
        <span className="size-[5px] rounded-[1px]" style={{ background: `hsl(${hue(value)} 55% 58%)` }} />
        {shown.short}
      </button>
      {at &&
        createPortal(
          <>
            <div className="fixed inset-0 z-40" onPointerDown={close} />
            <div role="dialog" aria-label={label} style={at} className="fixed z-50 w-[320px] rounded-lg border border-border bg-raised p-3 font-sans text-[11px] shadow-float">
              <p className="text-[10px] text-muted">{label}</p>
              <p className="mono mt-1 text-[11px] break-all">{shown.full}</p>
              {shown.hex && <p className="mt-1 text-[10px] text-muted">Hexadecimal form of the identifier below.</p>}
              <p className="mono mt-2 text-[10px] break-all text-muted">{value}</p>
              <button
                type="button"
                autoFocus
                className="mt-2 h-6 rounded-md border border-border px-2 text-[10px] hover:bg-panel"
                onClick={() => navigator.clipboard?.writeText(value).then(() => setCopied(true), () => setCopied(false))}
              >
                {copied ? "Copied" : "Copy original ID"}
              </button>
            </div>
          </>,
          document.body,
        )}
    </>
  );
}
