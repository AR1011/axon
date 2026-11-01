import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { subscribe, useLive } from "./store";

class Source {
  static OPEN = 1;
  static made: Source[] = [];
  readyState = 0;
  closed = false;
  onerror: (() => void) | null = null;
  listeners: ((event: { data: string }) => void)[] = [];
  constructor() {
    Source.made.push(this);
  }
  addEventListener(_: string, listener: (event: { data: string }) => void) {
    this.listeners.push(listener);
  }
  close() {
    this.closed = true;
  }
}

const page = Object.assign(new EventTarget(), { hidden: false });
const fetched: string[] = [];
const overview = { timestamp_ms: 1, build: "dev", observer: "a", nodes: [], events: [], discovery_error: null, history: { sparse: { first: 2, counts: [1], count: 1, sum_ns: 5 } } };

beforeEach(() => {
  vi.useFakeTimers();
  Source.made = [];
  fetched.length = 0;
  page.hidden = false;
  vi.stubGlobal("document", page);
  vi.stubGlobal("EventSource", Source);
  vi.stubGlobal("fetch", async (url: string) => {
    fetched.push(url);
    return new Response(JSON.stringify(overview));
  });
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const show = (hidden: boolean) => {
  page.hidden = hidden;
  page.dispatchEvent(new Event("visibilitychange"));
};

test("the stream is the only request at load, and its frame keeps histograms sparse", () => {
  const stop = subscribe();
  expect(Source.made.length).toBe(1);
  expect(fetched).toEqual([]);
  Source.made[0].readyState = Source.OPEN;
  Source.made[0].listeners.forEach((l) => l({ data: JSON.stringify(overview) }));
  expect(useLive.getState().overview?.history).toEqual(overview.history);
  stop();
});

test("a hidden tab closes the stream and does not poll; shown again it opens a new one", async () => {
  const stop = subscribe();
  show(true);
  expect(Source.made[0].closed).toBe(true);
  await vi.advanceTimersByTimeAsync(20_000);
  expect(fetched).toEqual([]);
  show(false);
  expect(Source.made.length).toBe(2);
  expect(Source.made[1].closed).toBe(false);
  // Still not open a period later: the overview is polled.
  await vi.advanceTimersByTimeAsync(5_000);
  expect(fetched).toEqual(["/api/overview"]);
  stop();
  expect(Source.made[1].closed).toBe(true);
});
