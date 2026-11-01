import { useCallback, useEffect, useRef, useState } from "react";
import { CARD, type Box } from "../../model/topology";

export interface Camera {
  x: number;
  y: number;
  k: number;
}

export const clampZoom = (k: number) => Math.max(0.08, Math.min(3, k));

/** `c` zoomed by `factor` about the point `(px, py)`, which stays where it is on screen. */
export function zoomAt(c: Camera, px: number, py: number, factor: number): Camera {
  const k = clampZoom(c.k * factor);
  return { k, x: px - ((px - c.x) / c.k) * k, y: py - ((py - c.y) / c.k) * k };
}

/** Refits only when `structure` changes or the container resizes; telemetry refreshes leave it alone. */
export function useCamera(structure: string, width: number, height: number) {
  const host = useRef<HTMLDivElement>(null);
  const [camera, setCamera] = useState<Camera>({ x: 0, y: 0, k: 1 });

  const fit = useCallback(() => {
    const box = host.current?.getBoundingClientRect();
    if (!box || !box.width || !width) return;
    // On a phone the whole graph would be too small to read: fit its width, no smaller than 60 %, from the top.
    const phone = matchMedia("(max-width: 700px)").matches;
    const k = phone ? Math.max(0.6, Math.min(1.15, box.width / width)) : clampZoom(Math.min(1.15, box.width / width, box.height / height));
    setCamera({ k, x: (box.width - width * k) / 2, y: phone ? 0 : (box.height - height * k) / 2 });
  }, [width, height]);

  const focus = useCallback((at: Box | undefined) => {
    const box = host.current?.getBoundingClientRect();
    if (!box || !at) return;
    setCamera({ k: 1.25, x: box.width / 2 - (at.x + CARD.w / 2) * 1.25, y: box.height / 2 - (at.y + CARD.h / 2) * 1.25 });
  }, []);

  const zoom = useCallback((factor: number, cx?: number, cy?: number) => {
    const box = host.current?.getBoundingClientRect();
    if (!box) return;
    const [px, py] = [cx ?? box.width / 2, cy ?? box.height / 2];
    setCamera((c) => zoomAt(c, px, py, factor));
  }, []);

  useEffect(fit, [fit, structure]);

  useEffect(() => {
    const element = host.current;
    if (!element) return;
    const observer = new ResizeObserver(fit);
    observer.observe(element);
    const wheel = (e: WheelEvent) => {
      e.preventDefault();
      const box = element.getBoundingClientRect();
      zoom(Math.exp(-e.deltaY * 0.0015), e.clientX - box.left, e.clientY - box.top);
    };
    element.addEventListener("wheel", wheel, { passive: false });
    return () => {
      observer.disconnect();
      element.removeEventListener("wheel", wheel);
    };
  }, [fit, zoom]);

  const onPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    if (e.button !== 0 || (e.target as Element).closest("[data-no-pan]")) return;
    const element = e.currentTarget;
    element.setPointerCapture(e.pointerId);
    element.style.cursor = "grabbing";
    const start = { x: e.clientX, y: e.clientY };
    let origin: Camera | null = null;
    const move = (m: PointerEvent) =>
      setCamera((c) => {
        origin ??= c;
        return { ...c, x: origin.x + m.clientX - start.x, y: origin.y + m.clientY - start.y };
      });
    const up = () => {
      element.style.cursor = "";
      element.removeEventListener("pointermove", move);
      element.removeEventListener("pointerup", up);
      element.removeEventListener("pointercancel", up);
    };
    element.addEventListener("pointermove", move);
    element.addEventListener("pointerup", up);
    element.addEventListener("pointercancel", up);
  };

  return { host, camera, fit, focus, zoom, onPointerDown };
}
