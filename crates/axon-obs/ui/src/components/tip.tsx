import { useState } from "react";
import { createPortal } from "react-dom";

/** An instant tooltip for an icon button: `bind(text)` on the button, `node` rendered once. */
export function useTip() {
  const [tip, setTip] = useState<{ text: string; left: number; top: number } | null>(null);
  const show = (text: string) => (e: React.SyntheticEvent<HTMLElement>) => {
    const box = e.currentTarget.getBoundingClientRect();
    setTip({ text, left: box.left + box.width / 2, top: box.bottom + 6 });
  };
  const hide = () => setTip(null);
  const bind = (text: string) => ({ onMouseEnter: show(text), onMouseLeave: hide, onFocus: show(text), onBlur: hide });
  const node =
    tip &&
    createPortal(
      <div role="presentation" style={tip.left > window.innerWidth - 130 ? { right: 8, top: tip.top } : { left: Math.max(8, tip.left - 90), top: tip.top }} className="pointer-events-none fixed z-50 rounded-md border border-border bg-raised px-2 py-1 font-sans text-[10px] whitespace-nowrap shadow-tip">
        {tip.text}
      </div>,
      document.body,
    );
  return { bind, node };
}
