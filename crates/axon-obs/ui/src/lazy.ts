import { lazy, type ComponentType } from "react";

/** A named export of a module fetched when first rendered, so the first load carries only the shell and the overview. */
export const lazyNamed = <K extends string>(load: () => Promise<Record<K, ComponentType>>, name: K) => lazy(() => load().then((m) => ({ default: m[name] })));
