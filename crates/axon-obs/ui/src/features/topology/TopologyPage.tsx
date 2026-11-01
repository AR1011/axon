import { Empty } from "../../components/atoms";
import { ConnectionTelemetry } from "../connections/ConnectionTelemetry";
import { Inspector } from "../nodes/Inspector";
import { useSelectedConnection, useSelectedNode } from "../select";
import { Topology } from "./Topology";

export function TopologyPage() {
  const [node] = useSelectedNode();
  const [connection] = useSelectedConnection(undefined, false);
  return (
    <>
      <h1 className="sr-only-h1">Topology</h1>
      <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_320px] max-[1100px]:grid-cols-1">
        <Topology fullPage />
        <div className="flex min-h-0 flex-col border-l border-border max-[1100px]:hidden [&>aside]:flex-1 [&>aside]:rounded-none [&>aside]:border-0">
          <Inspector view={node}>
            {connection ? <ConnectionTelemetry connection={connection} compact /> : <Empty title="No session selected" detail="Select an edge to see the traffic of that session." />}
          </Inspector>
        </div>
      </div>
    </>
  );
}
