import type { NavigateFunction } from "react-router";
import { base } from "../api";

const NESTED = "a,button,input,[role=combobox]";

/** Whole-row navigation that leaves nested links and controls alone, and honours new-tab gestures. */
export function rowLink(navigate: NavigateFunction, href: string) {
  const open = (newTab: boolean) => (newTab ? window.open(`${base.replace(/\/$/, "")}${href}`, "_blank", "noopener") : navigate(href));
  return {
    "data-href": href,
    tabIndex: -1,
    onClick: (e: React.MouseEvent) => !(e.target as Element).closest(NESTED) && open(e.metaKey || e.ctrlKey),
    onAuxClick: (e: React.MouseEvent) => e.button === 1 && !(e.target as Element).closest(NESTED) && open(true),
    onKeyDown: (e: React.KeyboardEvent) => e.key === "Enter" && e.target === e.currentTarget && open(false),
  };
}

type Box = Element & HTMLOrSVGElement;

/**
 * Spread on a container: it is one tab stop that hands focus to its selected (or first) item, and `keys`, Home and End
 * move between the items `items` selects.
 */
function navWithin(items: string, keys: Record<string, number>) {
  return {
    tabIndex: 0,
    onFocus: (e: React.FocusEvent<Box>) => {
      // While an item holds focus the container is no tab stop, so Shift+Tab leaves it in one step.
      e.currentTarget.tabIndex = -1;
      if (e.target !== e.currentTarget || e.currentTarget.contains(e.relatedTarget)) return;
      (e.currentTarget.querySelector<HTMLElement>(`${items}[aria-selected=true],${items}[aria-pressed=true]`) ?? e.currentTarget.querySelector<HTMLElement>(items))?.focus();
    },
    onBlur: (e: React.FocusEvent<Box>) => {
      if (!e.currentTarget.contains(e.relatedTarget)) e.currentTarget.tabIndex = 0;
    },
    onKeyDown: (e: React.KeyboardEvent<Box>) => {
      if ((e.target as Element).closest("input,[role=combobox]") || e.metaKey || e.ctrlKey || e.altKey) return;
      const all = [...e.currentTarget.querySelectorAll<HTMLElement>(items)];
      const at = all.indexOf((e.target as Element).closest<HTMLElement>(items)!);
      const next = e.key === "Home" ? 0 : e.key === "End" ? all.length - 1 : keys[e.key] === undefined ? undefined : at + keys[e.key];
      if (next === undefined || !all.length) return;
      e.preventDefault();
      all[Math.max(0, Math.min(all.length - 1, next))].focus();
    },
  };
}

/** On a `<tbody>`: arrows and j/k move between rows. Left and Right stay with the page, which scrolls wide tables. */
export const rowNav = navWithin("tr[tabindex]", { ArrowDown: 1, j: 1, ArrowUp: -1, k: -1 });

/**
 * On a graph's `<svg>`: every arrow moves between its nodes and edges, in document order. A press on the background
 * pans, so it must not focus the graph, which would hand focus to an item.
 */
export const graphNav = {
  ...navWithin('[role="button"]', { ArrowDown: 1, ArrowRight: 1, ArrowUp: -1, ArrowLeft: -1 }),
  onMouseDown: (e: React.MouseEvent) => !(e.target as Element).closest('[role="button"]') && e.preventDefault(),
};
