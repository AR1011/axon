import { useId, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

export function Info({ label = "More information", children }: { label?: string; children: ReactNode }) {
  const [at, setAt] = useState<{ left: number; top?: number; bottom?: number } | null>(null);
  const button = useRef<HTMLButtonElement>(null);
  const id = useId();
  const open = () => {
    const box = button.current!.getBoundingClientRect();
    const left = Math.max(8, Math.min(window.innerWidth - 308, box.left + box.width / 2 - 150));
    setAt(box.bottom + 140 > window.innerHeight ? { left, bottom: window.innerHeight - box.top + 6 } : { left, top: box.bottom + 6 });
  };
  const close = () => setAt(null);
  return (
    <>
      <button
        ref={button}
        type="button"
        aria-label={label}
        aria-describedby={at ? id : undefined}
        onMouseEnter={open}
        onMouseLeave={close}
        onFocus={open}
        onBlur={close}
        onClick={() => (at ? close() : open())}
        onKeyDown={(e) => e.key === "Escape" && close()}
        className="inline-flex size-[13px] shrink-0 cursor-help items-center justify-center rounded-full border border-border font-[Georgia,serif] text-[10px] leading-none text-muted hover:text-text focus-visible:outline-1"
      >
        i
      </button>
      {at &&
        createPortal(
          <div id={id} role="tooltip" style={at} className="pointer-events-none fixed z-50 w-[300px] rounded-lg border border-border bg-panel px-3 py-2 font-sans text-[11px] leading-[1.6] text-text shadow-tip">
            {children}
          </div>,
          document.body,
        )}
    </>
  );
}
