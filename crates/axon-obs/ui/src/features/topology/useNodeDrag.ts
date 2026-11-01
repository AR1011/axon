import { useCallback, useMemo, useRef, useState } from "react";
import type { Box } from "../../model/topology";

/**
 * Lets cards be dragged off the computed layout. A moved card keeps its place while the layout
 * changes under it; `reset` drops every move. `scale` is the camera zoom, so a card follows the pointer.
 */
export function useNodeDrag(layout: Map<string, Box>, scale: number) {
  const [moved, setMoved] = useState<Map<string, Box>>(new Map());
  const dragged = useRef(false);
  const positions = useMemo(() => new Map([...layout].map(([id, at]) => [id, moved.get(id) ?? at])), [layout, moved]);

  const onPointerDown = (id: string, e: React.PointerEvent<SVGGElement>) => {
    const from = positions.get(id);
    if (e.button !== 0 || !from) return;
    const element = e.currentTarget;
    const start = { x: e.clientX, y: e.clientY };
    dragged.current = false;
    // Captured at once: a flick that leaves the card before its first move must still end here.
    element.setPointerCapture(e.pointerId);
    const move = (m: PointerEvent) => {
      const [dx, dy] = [m.clientX - start.x, m.clientY - start.y];
      if (!dragged.current && Math.hypot(dx, dy) < 4) return;
      dragged.current = true;
      setMoved((all) => new Map(all).set(id, { x: from.x + dx / scale, y: from.y + dy / scale }));
    };
    const up = () => {
      element.removeEventListener("pointermove", move);
      element.removeEventListener("pointerup", up);
      element.removeEventListener("pointercancel", up);
    };
    element.addEventListener("pointermove", move);
    element.addEventListener("pointerup", up);
    element.addEventListener("pointercancel", up);
  };

  /** True once for the click that ends a drag, so it does not also select the card. */
  const wasDrag = useCallback(() => {
    const was = dragged.current;
    dragged.current = false;
    return was;
  }, []);
  const reset = useCallback(() => setMoved(new Map()), []);
  return { positions, onPointerDown, wasDrag, reset, arranged: [...moved.keys()].some((id) => layout.has(id)) };
}
