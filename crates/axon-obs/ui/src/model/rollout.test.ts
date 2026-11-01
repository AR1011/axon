import { describe, expect, it } from "vitest";
import type { NodeView } from "../api";
import { acceptorRows, readiness, wireRange } from "./rollout";

function node(id: string, cluster: object = {}, metadata: Record<string, string> = {}, phase = "running"): NodeView {
  const snapshot = { lifecycle: { phase, reason: null }, cluster: { isolated: false, engine: { versions: [2, 2] }, ownership: null, ...cluster } };
  return { node: { id, incarnation: "1", address: "", metadata }, reachable: true, error: null, snapshot } as unknown as NodeView;
}

const paxos = (stats: [string, string][] = []) => ({ ownership: { kind: "paxos", settings: [["acceptors", "a, b, c"]], stats, links: [] } });

describe("rollout readiness", () => {
  it("names each failing readyz check, and nothing for a node without a snapshot or one that left", () => {
    expect(readiness(node("a"))).toEqual([]);
    expect(readiness(node("a", { isolated: true, warming: true, quotas_starting: true }, {}, "draining"))).toEqual(["shutting down", "isolated", "warming", "quotas starting"]);
    expect(readiness({ ...node("a"), snapshot: null })).toBeNull();
    expect(readiness({ ...node("a"), leftAt: 1 })).toBeNull();
    expect(readiness({ ...node("a"), reachable: false, error: "unreachable from the dashboard node" }), "its last snapshot is kept").toBeNull();
  });

  it("shows one wire version or the range, and nothing from an older node", () => {
    expect(wireRange(node("a").snapshot)).toBe("2");
    expect(wireRange(node("a", { engine: { versions: [2, 3] } }).snapshot)).toBe("2..3");
    expect(wireRange(node("a", { engine: {} }).snapshot)).toBe("");
  });

  it("lists every configured acceptor, with its figures or none when it has not reported", () => {
    const nodes = [node("a", paxos([["held", "0"], ["leases", "4"], ["warming", "true"], ["refused", "2"], ["clock behind", "1"]])), node("b", paxos([["held", "1"]])), node("c", paxos([["leases", "0"], ["warming", "false"]])), node("x", paxos())];
    expect(acceptorRows(nodes)).toEqual([
      { id: "a", stats: { held: "0", leases: "4", warming: "true", refused: "2", "clock behind": "1" }, state: "warming" },
      { id: "b", stats: null, state: "no report" },
      { id: "c", stats: { leases: "0", warming: "false" }, state: "active" },
    ]);
    expect(acceptorRows([node("a")])).toEqual([]);
    expect(acceptorRows([{ ...nodes[0], error: "unreachable from the dashboard node" }, nodes[1]])[0].stats, "its last report is kept").toBeNull();
  });
});
