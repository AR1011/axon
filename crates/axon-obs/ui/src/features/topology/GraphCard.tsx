import type { ReactNode } from "react";
import { CARD } from "../../model/topology";

/** A card on a graph: selected by click or key, dragged off the layout, lighting its edges on hover. */
export function GraphCard({ x, y, label, data, selected, stroke, strokeWidth, dash, onSelect, onHover, onDrag, children }: {
  x: number;
  y: number;
  label: string;
  data: Record<`data-${string}`, string>;
  selected: boolean;
  /** In place of the selection's outline. */
  stroke?: string;
  strokeWidth?: number;
  dash?: string;
  onSelect: () => void;
  onHover: (over: boolean) => void;
  onDrag: (e: React.PointerEvent<SVGGElement>) => void;
  children: ReactNode;
}) {
  return (
    <g
      role="button"
      tabIndex={-1}
      aria-pressed={selected}
      aria-label={label}
      data-no-pan
      {...data}
      transform={`translate(${x} ${y})`}
      onClick={onSelect}
      onPointerDown={onDrag}
      className="cursor-grab active:cursor-grabbing"
      onPointerEnter={() => onHover(true)}
      onPointerLeave={() => onHover(false)}
      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), onSelect())}
    >
      <rect width={CARD.w} height={CARD.h} rx="4" fill={selected ? "var(--accent-soft)" : "var(--panel-raised)"} stroke={stroke ?? (selected ? "var(--accent)" : "var(--border)")} strokeWidth={strokeWidth} strokeDasharray={dash} />
      {children}
    </g>
  );
}
